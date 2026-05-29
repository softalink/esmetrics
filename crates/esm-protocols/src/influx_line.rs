//! Influx line protocol parser (v1 + v2).
//!
//! Format:
//! ```text
//! measurement[,tag1=v1,tag2=v2 ...] field1=v1[,field2=v2 ...] [timestamp_ns]
//! ```
//!
//! - Multiple `field=value` pairs in one line emit one sample per field; the
//!   canonical metric name is `<measurement>_<field>` (matches VM behavior).
//! - Tags become labels.
//! - Field values: integers (`123i`), floats (`1.5`), booleans (`t`/`f`/
//!   `true`/`false`), or quoted strings. We round to nearest i64 for storage.
//! - Timestamp is nanoseconds by default (v1 default); a `?precision=` query
//!   param at the HTTP layer can shift this, handled by the caller.

#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_precision_loss)]

use std::borrow::Cow;

use thiserror::Error;

/// One parsed sample.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedSample {
    pub metric_name: Vec<u8>,
    pub timestamp_ms: i64,
    pub value: i64,
}

/// Parse a full Influx line-protocol payload.
///
/// `now_ms` is used for any line without an explicit timestamp.
/// `ns_per_unit` says how many nanoseconds each unit of the supplied
/// timestamp represents — pass `1` for nanoseconds (v1 default), `1_000_000`
/// for milliseconds, etc.
///
/// One parsed sample as `(key_range, timestamp_ms, value)`, where `key_range`
/// indexes the canonical metric-name bytes inside the caller's arena. Lets the
/// ingest path avoid a heap `Vec` per sample (the keys share one growable
/// arena; storage interns them by slice).
pub type KeyedSample = (std::ops::Range<usize>, i64, i64);

/// Streaming parse: append each sample's canonical metric-name bytes to `arena`
/// and push `(range, timestamp_ms, value)` to `out`. No per-sample allocation
/// (the arena and a small reused `scratch` buffer amortize), unlike [`parse`]
/// which returns an owned `Vec<u8>` key per sample.
///
/// # Errors
/// Returns [`ParseError`] on the first malformed line.
pub fn parse_into(
    input: &str,
    now_ms: i64,
    ns_per_unit: i64,
    arena: &mut Vec<u8>,
    out: &mut Vec<KeyedSample>,
) -> Result<(), ParseError> {
    let mut scratch = Vec::new();
    for (line_no, raw) in input.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        parse_line_into(line, now_ms, ns_per_unit, arena, &mut scratch, out)
            .map_err(|e| ParseError::Line { line_no, source: e })?;
    }
    Ok(())
}

/// # Errors
/// Returns [`ParseError`] on the first malformed line.
pub fn parse(input: &str, now_ms: i64, ns_per_unit: i64) -> Result<Vec<ParsedSample>, ParseError> {
    let mut out = Vec::new();
    for (line_no, raw) in input.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        parse_line(line, now_ms, ns_per_unit, &mut out)
            .map_err(|e| ParseError::Line { line_no, source: e })?;
    }
    Ok(out)
}

fn parse_line(
    line: &str,
    now_ms: i64,
    ns_per_unit: i64,
    out: &mut Vec<ParsedSample>,
) -> Result<(), LineError> {
    // Split into (key-section) (fields) [ts].
    let mut parts = split_top_level_spaces(line);
    let key_section = parts.next().ok_or(LineError::MissingKey)?;
    let fields_section = parts.next().ok_or(LineError::MissingFields)?;
    let ts_section = parts.next();
    if parts.next().is_some() {
        return Err(LineError::TooManyTokens);
    }

    // Parse `measurement,tag1=v1,tag2=v2`.
    let mut comma_parts = split_unescaped(key_section, ',');
    let measurement = comma_parts.next().ok_or(LineError::MissingMeasurement)?;
    let mut tags: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
    for tp in comma_parts {
        let Some(eq) = find_unescaped(tp, '=') else { return Err(LineError::BadTag) };
        let k = unescape_tag(&tp[..eq]);
        let v = unescape_tag(&tp[eq + 1..]);
        tags.push((k, v));
    }
    tags.sort_by(|a, b| a.0.cmp(&b.0));

    // Parse fields: `f1=v1,f2=v2`.
    let mut field_parts = split_unescaped(fields_section, ',');
    let mut field_pairs: Vec<(Cow<'_, str>, i64)> = Vec::new();
    for fp in field_parts.by_ref() {
        let Some(eq) = find_unescaped(fp, '=') else { return Err(LineError::BadField) };
        let k = unescape_tag(&fp[..eq]);
        let v = parse_field_value(&fp[eq + 1..])?;
        field_pairs.push((k, v));
    }
    if field_pairs.is_empty() {
        return Err(LineError::NoFields);
    }

    let timestamp_ms = match ts_section {
        Some(s) => {
            let ns: i64 = s.parse().map_err(|_| LineError::BadTimestamp(s.into()))?;
            ns * ns_per_unit / 1_000_000
        }
        None => now_ms,
    };

    // Build the canonical label suffix `{k="v",...}` once per line; the tag
    // set is identical across all of a line's fields, so the escape loop need
    // not run per field. Each field then just concatenates prefix + suffix.
    let mut suffix = Vec::new();
    if !tags.is_empty() {
        suffix.push(b'{');
        for (i, (k, v)) in tags.iter().enumerate() {
            if i > 0 {
                suffix.push(b',');
            }
            suffix.extend_from_slice(k.as_bytes());
            suffix.extend_from_slice(b"=\"");
            for c in v.chars() {
                match c {
                    '\\' => suffix.extend_from_slice(b"\\\\"),
                    '"' => suffix.extend_from_slice(b"\\\""),
                    '\n' => suffix.extend_from_slice(b"\\n"),
                    other => {
                        let mut buf = [0u8; 4];
                        suffix.extend_from_slice(other.encode_utf8(&mut buf).as_bytes());
                    }
                }
            }
            suffix.push(b'"');
        }
        suffix.push(b'}');
    }

    // Emit one sample per field: `measurement[_field]{suffix}`.
    for (field, value) in field_pairs {
        let with_field = field.as_ref() != "value";
        let cap = measurement.len() + usize::from(with_field) * (1 + field.len()) + suffix.len();
        let mut metric_name = Vec::with_capacity(cap);
        metric_name.extend_from_slice(measurement.as_bytes());
        if with_field {
            metric_name.push(b'_');
            metric_name.extend_from_slice(field.as_bytes());
        }
        metric_name.extend_from_slice(&suffix);
        out.push(ParsedSample { metric_name, timestamp_ms, value });
    }
    Ok(())
}

/// Arena-writing variant of [`parse_line`]. Produces byte-identical canonical
/// keys (see `parse_into_matches_parse` test) but appends them to `arena`
/// instead of allocating a `Vec` per sample. `scratch` is a reused buffer for
/// the per-line label suffix.
fn parse_line_into(
    line: &str,
    now_ms: i64,
    ns_per_unit: i64,
    arena: &mut Vec<u8>,
    scratch: &mut Vec<u8>,
    out: &mut Vec<KeyedSample>,
) -> Result<(), LineError> {
    let mut parts = split_top_level_spaces(line);
    let key_section = parts.next().ok_or(LineError::MissingKey)?;
    let fields_section = parts.next().ok_or(LineError::MissingFields)?;
    let ts_section = parts.next();
    if parts.next().is_some() {
        return Err(LineError::TooManyTokens);
    }

    let mut comma_parts = split_unescaped(key_section, ',');
    let measurement = comma_parts.next().ok_or(LineError::MissingMeasurement)?;
    let mut tags: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
    for tp in comma_parts {
        let Some(eq) = find_unescaped(tp, '=') else { return Err(LineError::BadTag) };
        tags.push((unescape_tag(&tp[..eq]), unescape_tag(&tp[eq + 1..])));
    }
    tags.sort_by(|a, b| a.0.cmp(&b.0));

    let mut field_parts = split_unescaped(fields_section, ',');
    let mut field_pairs: Vec<(Cow<'_, str>, i64)> = Vec::new();
    for fp in field_parts.by_ref() {
        let Some(eq) = find_unescaped(fp, '=') else { return Err(LineError::BadField) };
        field_pairs.push((unescape_tag(&fp[..eq]), parse_field_value(&fp[eq + 1..])?));
    }
    if field_pairs.is_empty() {
        return Err(LineError::NoFields);
    }

    let timestamp_ms = match ts_section {
        Some(s) => {
            let ns: i64 = s.parse().map_err(|_| LineError::BadTimestamp(s.into()))?;
            ns * ns_per_unit / 1_000_000
        }
        None => now_ms,
    };

    // Build the label suffix once per line into the reused scratch buffer.
    scratch.clear();
    if !tags.is_empty() {
        scratch.push(b'{');
        for (i, (k, v)) in tags.iter().enumerate() {
            if i > 0 {
                scratch.push(b',');
            }
            scratch.extend_from_slice(k.as_bytes());
            scratch.extend_from_slice(b"=\"");
            for c in v.chars() {
                match c {
                    '\\' => scratch.extend_from_slice(b"\\\\"),
                    '"' => scratch.extend_from_slice(b"\\\""),
                    '\n' => scratch.extend_from_slice(b"\\n"),
                    other => {
                        let mut buf = [0u8; 4];
                        scratch.extend_from_slice(other.encode_utf8(&mut buf).as_bytes());
                    }
                }
            }
            scratch.push(b'"');
        }
        scratch.push(b'}');
    }

    for (field, value) in &field_pairs {
        let with_field = field.as_ref() != "value";
        let start = arena.len();
        arena.extend_from_slice(measurement.as_bytes());
        if with_field {
            arena.push(b'_');
            arena.extend_from_slice(field.as_bytes());
        }
        arena.extend_from_slice(scratch);
        out.push((start..arena.len(), timestamp_ms, *value));
    }
    Ok(())
}

fn parse_field_value(raw: &str) -> Result<i64, LineError> {
    let raw = raw.trim();
    if raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2 {
        // String field — store length-of-string as proxy (Phase 2 MVP).
        return Ok(i64::try_from(raw.len()).unwrap_or(i64::MAX) - 2);
    }
    if let Some(rest) = raw.strip_suffix('i').or_else(|| raw.strip_suffix('u')) {
        return rest.parse().map_err(|_| LineError::BadValue(raw.into()));
    }
    if raw.eq_ignore_ascii_case("true") || raw.eq_ignore_ascii_case("t") {
        return Ok(1);
    }
    if raw.eq_ignore_ascii_case("false") || raw.eq_ignore_ascii_case("f") {
        return Ok(0);
    }
    let f: f64 = raw.parse().map_err(|_| LineError::BadValue(raw.into()))?;
    Ok(f as i64)
}

/// Split on `c`, respecting backslash escapes.
fn split_unescaped(s: &str, c: char) -> impl Iterator<Item = &str> {
    let mut out: Vec<&str> = Vec::new();
    let bytes = s.as_bytes();
    let mut start = 0;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            i += 2;
            continue;
        }
        if bytes[i] as char == c {
            out.push(&s[start..i]);
            start = i + 1;
            i += 1;
            continue;
        }
        i += 1;
    }
    out.push(&s[start..]);
    out.into_iter()
}

fn find_unescaped(s: &str, c: char) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            i += 2;
            continue;
        }
        if bytes[i] as char == c {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Split on whitespace at the top level (ignoring escaped spaces) into
/// up to three segments: key, fields, [timestamp].
fn split_top_level_spaces(s: &str) -> std::vec::IntoIter<&str> {
    let mut out: Vec<&str> = Vec::new();
    let bytes = s.as_bytes();
    let mut start = 0;
    let mut i = 0;
    let mut inside_quotes = false;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            i += 2;
            continue;
        }
        if bytes[i] == b'"' {
            inside_quotes = !inside_quotes;
        }
        if !inside_quotes && bytes[i] == b' ' {
            let segment = &s[start..i];
            if !segment.is_empty() {
                out.push(segment);
            }
            start = i + 1;
        }
        i += 1;
    }
    let tail = &s[start..];
    if !tail.is_empty() {
        out.push(tail);
    }
    out.into_iter()
}

/// Unescape a tag/field key or tag value. Borrows the input unchanged when it
/// contains no backslash (the overwhelmingly common case — zero allocation);
/// only escaped strings allocate.
fn unescape_tag(s: &str) -> Cow<'_, str> {
    if !s.as_bytes().contains(&b'\\') {
        return Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            out.push(bytes[i + 1] as char);
            i += 2;
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    Cow::Owned(out)
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("line {line_no}: {source}")]
    Line {
        line_no: usize,
        #[source]
        source: LineError,
    },
}

#[derive(Debug, Error)]
pub enum LineError {
    #[error("missing key section")]
    MissingKey,
    #[error("missing measurement name")]
    MissingMeasurement,
    #[error("missing fields section")]
    MissingFields,
    #[error("no field key=value pairs")]
    NoFields,
    #[error("malformed tag")]
    BadTag,
    #[error("malformed field")]
    BadField,
    #[error("invalid field value: {0:?}")]
    BadValue(String),
    #[error("invalid timestamp: {0:?}")]
    BadTimestamp(String),
    #[error("too many whitespace tokens")]
    TooManyTokens,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_into_matches_parse() {
        let input = "cpu,host=b,region=us usage_user=10i,usage_system=20i 1700000000000000000\n\
                     cpu,host=a value=5 1700000000000000000\n\
                     mem,host=a\\ x free=3i\n\
                     disk read=1i,write=2i";
        let normal = parse(input, 12345, 1).unwrap();
        let mut arena = Vec::new();
        let mut keyed = Vec::new();
        parse_into(input, 12345, 1, &mut arena, &mut keyed).unwrap();
        assert_eq!(normal.len(), keyed.len());
        for (ns, (range, ts, v)) in normal.iter().zip(keyed.iter()) {
            assert_eq!(&arena[range.clone()], ns.metric_name.as_slice(), "key mismatch");
            assert_eq!(*ts, ns.timestamp_ms);
            assert_eq!(*v, ns.value);
        }
    }

    #[test]
    fn parse_simple_line_no_tags() {
        let s = "cpu value=42 1700000000000000000\n";
        let out = parse(s, 0, 1).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name, b"cpu");
        assert_eq!(out[0].timestamp_ms, 1_700_000_000_000);
        assert_eq!(out[0].value, 42);
    }

    #[test]
    fn parse_with_tags() {
        let s = "cpu,host=server1,region=us value=10 1700000000000000000";
        let out = parse(s, 0, 1).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name, br#"cpu{host="server1",region="us"}"#);
    }

    #[test]
    fn parse_multiple_fields_become_multiple_samples() {
        let s = "weather,city=austin temperature=70,humidity=50 1700000000000000000";
        let out = parse(s, 0, 1).unwrap();
        assert_eq!(out.len(), 2);
        // Field name appended to measurement (skipped for "value").
        let names: Vec<&[u8]> = out.iter().map(|s| s.metric_name.as_slice()).collect();
        assert!(names.iter().any(|n| n.starts_with(b"weather_temperature")));
        assert!(names.iter().any(|n| n.starts_with(b"weather_humidity")));
    }

    #[test]
    fn parse_integer_suffix() {
        let s = "counters value=100i 1700000000000000000";
        let out = parse(s, 0, 1).unwrap();
        assert_eq!(out[0].value, 100);
    }

    #[test]
    fn parse_boolean() {
        let s = "flag value=true 1700000000000000000\nflag value=f 1700000060000000000";
        let out = parse(s, 0, 1).unwrap();
        assert_eq!(out[0].value, 1);
        assert_eq!(out[1].value, 0);
    }

    #[test]
    fn parse_missing_timestamp_uses_now() {
        let s = "cpu value=1";
        let out = parse(s, 12345, 1).unwrap();
        assert_eq!(out[0].timestamp_ms, 12345);
    }

    #[test]
    fn parse_blank_lines_and_comments_skipped() {
        let s = "# header\n\ncpu value=1 0\n";
        let out = parse(s, 0, 1).unwrap();
        assert_eq!(out.len(), 1);
    }
}
