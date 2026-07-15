//! Shared query-parameter helpers for protocol handlers.
//!
//! Port of `lib/protoparser/protoparserutil/extra_labels.go` and
//! `lib/protoparser/protoparserutil/timestamp.go` from upstream
//! VictoriaMetrics v1.146.0, plus common query-param lookup utilities.

use esm_http::{percent_decode, Request};

use crate::InsertError;

/// Parse a single extra_label query arg value into (name, value).
/// The value must be in the format "name=value".
fn parse_extra_label(value: &str) -> Result<(String, String), String> {
    let (name, val) = value.split_once('=').ok_or_else(|| {
        format!("`extra_label` query arg must have the format `name=value`; got {value:?}")
    })?;
    Ok((name.to_owned(), val.to_owned()))
}

/// Collects `extra_label=name=value` query args, prefixed by any
/// Pushgateway-style `.../metrics/job/<job>/<label>/<value>/...` labels
/// found in the request path.
///
/// Go: `protoparserutil.GetExtraLabels`. Every upstream ingestion handler
/// that accepts extra labels (influx, promremotewrite, prometheusimport,
/// vmimport, csvimport, ...) calls this same function, so Pushgateway path
/// labels are recognized uniformly here too, not just for
/// `/api/v1/import/prometheus`.
pub(crate) fn get_extra_labels(req: &Request<'_>) -> Result<Vec<(String, String)>, InsertError> {
    // Go's `req.URL.Path` is already percent-decoded by net/url; `req.path()`
    // here returns the raw (as-received) path, so it must be decoded before
    // searching for the Pushgateway marker.
    let decoded_path = percent_decode(req.path());
    let mut extra_labels =
        get_pushgateway_labels(&decoded_path).map_err(InsertError::bad_request)?;
    for (key, value) in req.query_params() {
        if key.as_ref() == "extra_label" {
            let (name, val) =
                parse_extra_label(value.as_ref()).map_err(InsertError::bad_request)?;
            extra_labels.push((name, val));
        }
    }
    Ok(extra_labels)
}

/// Extracts Pushgateway-compatible labels from a (decoded) request path,
/// per <https://github.com/prometheus/pushgateway#url>.
///
/// Go: `protoparserutil.getPushgatewayLabels`. `path` must already be
/// percent-decoded (see [`get_extra_labels`]); base64-encoded segments
/// (`name@base64/...`) are decoded here on top of that.
fn get_pushgateway_labels(path: &str) -> Result<Vec<(String, String)>, String> {
    let Some(n) = path.find("/metrics/job") else {
        return Ok(Vec::new());
    };
    let mut s = &path[n + "/metrics/".len()..];
    if !(s.starts_with("job/") || s.starts_with("job@base64/")) {
        return Ok(Vec::new());
    }

    let mut labels = Vec::new();
    while !s.is_empty() {
        let Some(slash) = s.find('/') else {
            return Err(format!("missing value for label {s:?}"));
        };
        let mut name = &s[..slash];
        s = &s[slash + 1..];
        let is_base64 = name.ends_with("@base64");
        if is_base64 {
            name = &name[..name.len() - "@base64".len()];
        }

        let value = match s.find('/') {
            None => {
                let value = s;
                s = "";
                value
            }
            Some(n) => {
                let (value, rest) = (&s[..n], &s[n + 1..]);
                s = rest;
                value
            }
        };

        let value = if is_base64 {
            let decoded = base64url_decode(value.trim_end_matches('=')).map_err(|err| {
                format!("cannot base64-decode value={value:?} for label={name:?}: {err}")
            })?;
            // Go's `string(data)` never validates UTF-8 (Go strings are raw
            // byte sequences); a lossy conversion is the closest match here
            // since our labels are `String`.
            String::from_utf8_lossy(&decoded).into_owned()
        } else {
            value.to_owned()
        };
        if value.is_empty() {
            // Skip labels with empty values.
            continue;
        }
        labels.push((name.to_owned(), value));
    }
    Ok(labels)
}

/// Decodes URL-safe, unpadded base64 (`base64.RawURLEncoding` in Go).
/// Hand-rolled to avoid pulling in a dependency for this one narrow use.
fn base64url_decode(s: &str) -> Result<Vec<u8>, String> {
    let bytes = s.as_bytes();
    if bytes.len() % 4 == 1 {
        return Err("illegal base64 data length".to_owned());
    }
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some(u32::from(c - b'A')),
            b'a'..=b'z' => Some(u32::from(c - b'a') + 26),
            b'0'..=b'9' => Some(u32::from(c - b'0') + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3 + 3);
    let mut buf: u32 = 0;
    let mut bits: u32 = 0;
    for &b in bytes {
        let v = val(b).ok_or_else(|| format!("invalid base64 character {:?}", b as char))?;
        buf = (buf << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Ok(out)
}

/// First occurrence of a query param, like Go url.Values.Get.
pub(crate) fn query_param(req: &Request<'_>, name: &str) -> Option<String> {
    for (key, value) in req.query_params() {
        if key.as_ref() == name {
            return Some(value.into_owned());
        }
    }
    None
}

/// Extracts the unix timestamp in milliseconds from the `timestamp` query
/// arg, or `0` if absent. Go: `protoparserutil.GetTimestamp`.
pub(crate) fn get_timestamp(req: &Request<'_>) -> Result<i64, InsertError> {
    match query_param(req, "timestamp") {
        None => Ok(0),
        Some(ts) => ts.parse::<i64>().map_err(|err| {
            InsertError::bad_request(format!("cannot parse `timestamp={ts}` query arg: {err}"))
        }),
    }
}

/// Appends extra labels to a metric name arena.
pub(crate) fn append_extra_labels(arena: &mut Vec<u8>, extra_labels: &[(String, String)]) {
    for (name, value) in extra_labels {
        esm_storage::marshal_metric_name_raw(arena, &[(name.as_bytes(), value.as_bytes())]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extra_label_requires_name_value_format() {
        // Valid format: "env=prod" → Ok
        let result = parse_extra_label("env=prod");
        assert!(result.is_ok());
        let (name, val) = result.unwrap();
        assert_eq!(name, "env");
        assert_eq!(val, "prod");

        // Invalid format: "envprod" (no '=') → Error with exact upstream message
        let result = parse_extra_label("envprod");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "`extra_label` query arg must have the format `name=value`; got \"envprod\""
        );

        // Edge case: value after '=' is empty, but format is valid
        let result = parse_extra_label("env=");
        assert!(result.is_ok());
        let (name, val) = result.unwrap();
        assert_eq!(name, "env");
        assert_eq!(val, "");

        // Edge case: name is empty, format is still valid
        let result = parse_extra_label("=prod");
        assert!(result.is_ok());
        let (name, val) = result.unwrap();
        assert_eq!(name, "");
        assert_eq!(val, "prod");

        // Multiple '=' signs: first one is the split point
        let result = parse_extra_label("key=value=extra");
        assert!(result.is_ok());
        let (name, val) = result.unwrap();
        assert_eq!(name, "key");
        assert_eq!(val, "value=extra");
    }

    // --- Pushgateway path labels: golden vectors ported from upstream
    // `lib/protoparser/protoparserutil/extra_labels_test.go`. ---

    fn labels_ok(path: &str) -> Vec<(String, String)> {
        get_pushgateway_labels(path)
            .unwrap_or_else(|err| panic!("unexpected error for {path:?}: {err}"))
    }

    #[test]
    fn pushgateway_labels_no_marker_returns_empty() {
        assert_eq!(labels_ok(""), vec![]);
        assert_eq!(labels_ok("/foo/bar"), vec![]);
        assert_eq!(labels_ok("/metrics/foo/bar"), vec![]);
        assert_eq!(labels_ok("/metrics/job"), vec![]);
        assert_eq!(labels_ok("/metrics/job@base64"), vec![]);
        assert_eq!(labels_ok("/metrics/job/"), vec![]);
    }

    #[test]
    fn pushgateway_labels_job_only() {
        assert_eq!(
            labels_ok("/metrics/job/foo"),
            vec![("job".to_owned(), "foo".to_owned())]
        );
        assert_eq!(
            labels_ok("/foo/metrics/job/foo"),
            vec![("job".to_owned(), "foo".to_owned())]
        );
        assert_eq!(
            labels_ok("/api/v1/import/prometheus/metrics/job/foo"),
            vec![("job".to_owned(), "foo".to_owned())]
        );
    }

    #[test]
    fn pushgateway_labels_job_base64() {
        assert_eq!(
            labels_ok("/foo/metrics/job@base64/Zm9v"),
            vec![("job".to_owned(), "foo".to_owned())]
        );
    }

    #[test]
    fn pushgateway_labels_extra_pairs() {
        assert_eq!(
            labels_ok("/foo/metrics/job/x/a/foo/aaa/bar"),
            vec![
                ("job".to_owned(), "x".to_owned()),
                ("a".to_owned(), "foo".to_owned()),
                ("aaa".to_owned(), "bar".to_owned()),
            ]
        );
        assert_eq!(
            labels_ok("/foo/metrics/job/x/a@base64/Zm9v"),
            vec![
                ("job".to_owned(), "x".to_owned()),
                ("a".to_owned(), "foo".to_owned()),
            ]
        );
    }

    #[test]
    fn pushgateway_labels_base64_with_slash_in_decoded_value() {
        assert_eq!(
            labels_ok(
                "/metrics/job/test/region@base64/YXotc291dGhlYXN0LTEtZjAxL3d6eS1hei1zb3V0aGVhc3QtMQ"
            ),
            vec![
                ("job".to_owned(), "test".to_owned()),
                (
                    "region".to_owned(),
                    "az-southeast-1-f01/wzy-az-southeast-1".to_owned()
                ),
            ]
        );
    }

    #[test]
    fn pushgateway_labels_base64_empty_value_is_skipped() {
        assert_eq!(
            labels_ok("/metrics/job/test/empty@base64/="),
            vec![("job".to_owned(), "test".to_owned())]
        );
    }

    #[test]
    fn pushgateway_labels_base64_decodes_padding_chars() {
        assert_eq!(
            labels_ok("/metrics/job/test/test@base64/PT0vPT0"),
            vec![
                ("job".to_owned(), "test".to_owned()),
                ("test".to_owned(), "==/==".to_owned()),
            ]
        );
    }

    #[test]
    fn pushgateway_labels_percent_decoded_unicode_value() {
        // The route/handler layer percent-decodes the path before calling
        // get_pushgateway_labels; simulate that here directly on decoded
        // input (percent_decode is exercised end-to-end in get_extra_labels).
        assert_eq!(
            labels_ok("/metrics/job/titan/name/Προμηθεύς"),
            vec![
                ("job".to_owned(), "titan".to_owned()),
                ("name".to_owned(), "Προμηθεύς".to_owned()),
            ]
        );
        assert_eq!(
            labels_ok("/metrics/job/titan/name@base64/zqDPgc6_zrzOt864zrXPjc-C"),
            vec![
                ("job".to_owned(), "titan".to_owned()),
                ("name".to_owned(), "Προμηθεύς".to_owned()),
            ]
        );
    }

    #[test]
    fn pushgateway_labels_missing_value_errors() {
        let err = get_pushgateway_labels("/metrics/job/foo/bar").unwrap_err();
        assert!(err.contains("missing value"), "unexpected error: {err}");
    }

    #[test]
    fn pushgateway_labels_invalid_base64_errors() {
        assert!(get_pushgateway_labels("/metrics/job@base64/#$%").is_err());
        assert!(get_pushgateway_labels("/metrics/job/foo/bar@base64/#$%").is_err());
    }

    #[test]
    fn base64url_decode_matches_go_raw_url_encoding() {
        assert_eq!(base64url_decode("Zm9v").unwrap(), b"foo");
        assert_eq!(base64url_decode("").unwrap(), b"");
        assert!(base64url_decode("#$%").is_err());
    }
}
