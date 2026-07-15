//! Hand-rolled Prometheus JSON writers.
//!
//! Ports the quicktemplate-generated serializers from
//! `app/vmselect/prometheus/{util,query_response,query_range_response,
//! series_response,labels_response,label_values_response,export}.qtpl` and
//! `lib/httpserver/prometheus_error_response.qtpl`. The literal byte
//! sequences below were taken from the generated `.qtpl.go` files, so the
//! output matches the upstream byte-for-byte (modulo float digits, see
//! [`append_qt_float`]).
//!
//! No serde on this path: everything appends straight into the response
//! body buffer.

use esm_promql::QueryResult;
use esm_storage::metric_name::MetricName;
use std::io::Write;

/// Query stats rendered into the `"stats"` object of query responses.
pub(crate) struct Stats {
    /// Number of series fetched from storage
    /// (`promql.QueryStats.SeriesFetched` analog).
    pub series_fetched: u64,
    /// Wall-clock duration of `promql.Exec`, milliseconds.
    pub execution_time_msec: i64,
}

/// Port of quicktemplate `QWriter.F` (`{%f= v %}`), the formatter used for
/// every sample value and timestamp in the Prometheus JSON responses:
///
/// - integral values representable as `int64` are printed as integers
///   (`2` → `"2"`),
/// - everything else uses Go `strconv.AppendFloat(dst, f, 'f', -1, 64)`:
///   the shortest round-trip decimal in **fixed** notation — never
///   exponent form (`1e21` → `"1000000000000000000000"`).
///
/// Rust's `Display` for `f64` has exactly these slow-path semantics
/// (shortest round-trip digits, plain decimal notation), verified against
/// Go 1.26 output by the golden vectors in the tests below, so no external
/// float formatting crate is needed. NaN and ±Inf match Go `strconv`
/// output (`NaN`, `+Inf`, `-Inf`).
pub(crate) fn append_qt_float(dst: &mut Vec<u8>, f: f64) {
    if f.is_nan() {
        dst.extend_from_slice(b"NaN");
        return;
    }
    if f.is_infinite() {
        dst.extend_from_slice(if f > 0.0 { b"+Inf" } else { b"-Inf" });
        return;
    }
    // Fast integer path, matching Go `n := int(f); float64(n) == f`.
    // The range check keeps values just outside the int64 range (where Go's
    // float→int conversion produces an out-of-range sentinel that never
    // compares equal) on the slow path.
    const I64_MIN_F: f64 = -9_223_372_036_854_775_808.0; // -2^63, exact
    const I64_MAX_BOUND: f64 = 9_223_372_036_854_775_808.0; // 2^63, exact
    if (I64_MIN_F..I64_MAX_BOUND).contains(&f) {
        let n = f as i64;
        if n as f64 == f {
            let _ = write!(dst, "{n}");
            return;
        }
    }
    let _ = write!(dst, "{f}");
}

/// Writes a millisecond timestamp the way the templates do:
/// `{%f= float64(timestamp)/1e3 %}` — seconds with up to 3 decimals.
pub(crate) fn append_timestamp(dst: &mut Vec<u8>, timestamp_ms: i64) {
    append_qt_float(dst, timestamp_ms as f64 / 1e3);
}

pub(crate) fn append_i64(dst: &mut Vec<u8>, n: i64) {
    let _ = write!(dst, "{n}");
}

/// Port of quicktemplate `AppendJSONString(dst, s, true)` (`{%q=`/`{%qz=`):
/// escapes `\n \r \t \b \f " \\`, `<` → `<`, `'` → `'` and the
/// remaining control bytes as `\u00xx`; all other bytes (including
/// non-ASCII) pass through untouched.
pub(crate) fn append_json_string(dst: &mut Vec<u8>, s: &[u8]) {
    dst.push(b'"');
    for &c in s {
        match c {
            b'\n' => dst.extend_from_slice(b"\\n"),
            b'\r' => dst.extend_from_slice(b"\\r"),
            b'\t' => dst.extend_from_slice(b"\\t"),
            0x08 => dst.extend_from_slice(b"\\b"),
            0x0c => dst.extend_from_slice(b"\\f"),
            b'"' => dst.extend_from_slice(b"\\\""),
            b'\\' => dst.extend_from_slice(b"\\\\"),
            b'<' => dst.extend_from_slice(b"\\u003c"),
            b'\'' => dst.extend_from_slice(b"\\u0027"),
            c if c < 0x20 => {
                let _ = write!(dst, "\\u{c:04x}");
            }
            c => dst.push(c),
        }
    }
    dst.push(b'"');
}

/// Port of `metricNameObject` (util.qtpl):
/// `{"__name__":"...","k":"v",...}`. Tag order is preserved; callers sort
/// tags beforehand where the Go pipeline does.
pub(crate) fn append_metric_name_object(dst: &mut Vec<u8>, mn: &MetricName) {
    dst.push(b'{');
    if !mn.metric_group.is_empty() {
        dst.extend_from_slice(b"\"__name__\":");
        append_json_string(dst, &mn.metric_group);
        if !mn.tags.is_empty() {
            dst.push(b',');
        }
    }
    for (j, tag) in mn.tags.iter().enumerate() {
        if j > 0 {
            dst.push(b',');
        }
        append_json_string(dst, &tag.key);
        dst.push(b':');
        append_json_string(dst, &tag.value);
    }
    dst.push(b'}');
}

/// Port of `valuesWithTimestamps` (util.qtpl): `[[<ts>,"<v>"],...]`.
pub(crate) fn append_values_with_timestamps(dst: &mut Vec<u8>, values: &[f64], timestamps: &[i64]) {
    if values.is_empty() {
        dst.extend_from_slice(b"[]");
        return;
    }
    dst.push(b'[');
    for (i, &v) in values.iter().enumerate() {
        if i > 0 {
            dst.push(b',');
        }
        dst.push(b'[');
        append_timestamp(dst, timestamps[i]);
        dst.extend_from_slice(b",\"");
        append_qt_float(dst, v);
        dst.extend_from_slice(b"\"]");
    }
    dst.push(b']');
}

fn append_stats(dst: &mut Vec<u8>, stats: &Stats) {
    // seriesFetched is a quoted string for vmalert backwards compatibility;
    // the space after the colon is present in the generated Go template.
    dst.extend_from_slice(b"\"seriesFetched\": \"");
    let _ = write!(dst, "{}", stats.series_fetched);
    dst.extend_from_slice(b"\",\"executionTimeMsec\":");
    append_i64(dst, stats.execution_time_msec);
}

/// Port of `QueryRangeResponse` (query_range_response.qtpl).
pub(crate) fn write_query_range_response(dst: &mut Vec<u8>, rs: &[QueryResult], stats: &Stats) {
    dst.extend_from_slice(
        b"{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[",
    );
    for (i, r) in rs.iter().enumerate() {
        if i > 0 {
            dst.push(b',');
        }
        dst.extend_from_slice(b"{\"metric\":");
        append_metric_name_object(dst, &r.metric_name);
        dst.extend_from_slice(b",\"values\":");
        append_values_with_timestamps(dst, &r.values, &r.timestamps);
        dst.push(b'}');
    }
    dst.extend_from_slice(b"]},\"stats\":{");
    append_stats(dst, stats);
    dst.extend_from_slice(b"}}");
}

/// Port of `QueryResponse` (query_response.qtpl). The result type is always
/// `vector` in the single-node upstream.
pub(crate) fn write_query_response(dst: &mut Vec<u8>, rs: &[QueryResult], stats: &Stats) {
    dst.extend_from_slice(
        b"{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[",
    );
    for (i, r) in rs.iter().enumerate() {
        if i > 0 {
            dst.push(b',');
        }
        dst.extend_from_slice(b"{\"metric\":");
        append_metric_name_object(dst, &r.metric_name);
        dst.extend_from_slice(b",\"value\":[");
        append_timestamp(dst, r.timestamps[0]);
        dst.extend_from_slice(b",\"");
        append_qt_float(dst, r.values[0]);
        dst.extend_from_slice(b"\"]}");
    }
    dst.extend_from_slice(b"]},\"stats\":{");
    append_stats(dst, stats);
    dst.extend_from_slice(b"}}");
}

/// Port of `SeriesResponse` (series_response.qtpl).
pub(crate) fn write_series_response(dst: &mut Vec<u8>, metric_names: &[MetricName]) {
    dst.extend_from_slice(b"{\"status\":\"success\",\"data\":[");
    for (i, mn) in metric_names.iter().enumerate() {
        if i > 0 {
            dst.push(b',');
        }
        append_metric_name_object(dst, mn);
    }
    dst.extend_from_slice(b"]}");
}

/// Port of `LabelsResponse`/`LabelValuesResponse` (identical shape):
/// `{"status":"success","data":["a","b",...]}`.
pub(crate) fn write_string_list_response(dst: &mut Vec<u8>, items: &[String]) {
    dst.extend_from_slice(b"{\"status\":\"success\",\"data\":[");
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            dst.push(b',');
        }
        append_json_string(dst, item.as_bytes());
    }
    dst.extend_from_slice(b"]}");
}

/// Port of `convertValueToSpecialJSON` (export.qtpl): NaN → `null`,
/// ±Inf → `"Infinity"`/`"-Infinity"`, everything else via the standard
/// float formatter.
fn append_export_value(dst: &mut Vec<u8>, v: f64) {
    if v.is_nan() {
        dst.extend_from_slice(b"null");
    } else if v.is_infinite() {
        dst.extend_from_slice(if v > 0.0 {
            b"\"Infinity\"".as_slice()
        } else {
            b"\"-Infinity\"".as_slice()
        });
    } else {
        append_qt_float(dst, v);
    }
}

/// Port of `ExportJSONLine` (export.qtpl): one NDJSON line
/// `{"metric":{...},"values":[...],"timestamps":[...]}\n` with millisecond
/// integer timestamps. Empty blocks produce no output.
pub(crate) fn write_export_json_line(
    dst: &mut Vec<u8>,
    mn: &MetricName,
    values: &[f64],
    timestamps: &[i64],
) {
    if timestamps.is_empty() {
        return;
    }
    dst.extend_from_slice(b"{\"metric\":");
    append_metric_name_object(dst, mn);
    dst.extend_from_slice(b",\"values\":[");
    for (i, &v) in values.iter().enumerate() {
        if i > 0 {
            dst.push(b',');
        }
        append_export_value(dst, v);
    }
    dst.extend_from_slice(b"],\"timestamps\":[");
    for (i, &ts) in timestamps.iter().enumerate() {
        if i > 0 {
            dst.push(b',');
        }
        append_i64(dst, ts);
    }
    dst.extend_from_slice(b"]}\n");
}

/// Port of `ExportPromAPIHeader`.
pub(crate) fn write_export_prom_api_header(dst: &mut Vec<u8>) {
    dst.extend_from_slice(
        b"{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[",
    );
}

/// Port of `ExportPromAPILine`.
pub(crate) fn write_export_prom_api_line(
    dst: &mut Vec<u8>,
    mn: &MetricName,
    values: &[f64],
    timestamps: &[i64],
) {
    dst.extend_from_slice(b"{\"metric\":");
    append_metric_name_object(dst, mn);
    dst.extend_from_slice(b",\"values\":");
    append_values_with_timestamps(dst, values, timestamps);
    dst.push(b'}');
}

/// Port of `ExportPromAPIFooter` (without query tracing).
pub(crate) fn write_export_prom_api_footer(dst: &mut Vec<u8>) {
    dst.extend_from_slice(b"]}}");
}

/// Port of `escapePrometheusLabel` (export.qtpl): quotes the value and
/// escapes `\\`, `\n` and `"`.
fn append_prometheus_label_value(dst: &mut Vec<u8>, value: &[u8]) {
    dst.push(b'"');
    for &c in value {
        match c {
            b'\\' => dst.extend_from_slice(b"\\\\"),
            b'\n' => dst.extend_from_slice(b"\\n"),
            b'"' => dst.extend_from_slice(b"\\\""),
            c => dst.push(c),
        }
    }
    dst.push(b'"');
}

/// Port of `ExportPrometheusLine` (export.qtpl): text exposition rows
/// `name{k="v",...} <value> <ts_ms>\n`.
pub(crate) fn write_export_prometheus_line(
    dst: &mut Vec<u8>,
    mn: &MetricName,
    values: &[f64],
    timestamps: &[i64],
) {
    if timestamps.is_empty() {
        return;
    }
    let mut name = Vec::with_capacity(64);
    name.extend_from_slice(&mn.metric_group);
    if !mn.tags.is_empty() {
        name.push(b'{');
        for (i, tag) in mn.tags.iter().enumerate() {
            if i > 0 {
                name.push(b',');
            }
            name.extend_from_slice(&tag.key);
            name.push(b'=');
            append_prometheus_label_value(&mut name, &tag.value);
        }
        name.push(b'}');
    }
    for (i, &ts) in timestamps.iter().enumerate() {
        dst.extend_from_slice(&name);
        dst.push(b' ');
        append_qt_float(dst, values[i]);
        dst.push(b' ');
        append_i64(dst, ts);
        dst.push(b'\n');
    }
}

/// Port of `PrometheusErrorResponse`
/// (lib/httpserver/prometheus_error_response.qtpl):
/// `{"status":"error","errorType":"<statusCode>","error":"..."}`.
/// The upstream uses the numeric HTTP status code as the `errorType`.
pub(crate) fn write_prometheus_error_response(dst: &mut Vec<u8>, status_code: u16, msg: &str) {
    dst.extend_from_slice(b"{\"status\":\"error\",\"errorType\":\"");
    let _ = write!(dst, "{status_code}");
    dst.extend_from_slice(b"\",\"error\":");
    append_json_string(dst, msg.as_bytes());
    dst.push(b'}');
}

#[cfg(test)]
mod tests {
    use super::*;

    include!("float_golden.rs");

    fn fmt(f: f64) -> String {
        let mut dst = Vec::new();
        append_qt_float(&mut dst, f);
        String::from_utf8(dst).unwrap()
    }

    /// Golden vectors generated with Go 1.26 by replicating quicktemplate's
    /// `QWriter.F`: `n := int(f); if float64(n) == f { itoa } else
    /// { strconv.AppendFloat(dst, f, 'f', -1, 64) }`.
    #[test]
    fn qt_float_matches_go() {
        for &(bits, expected) in CASES {
            let f = f64::from_bits(bits);
            assert_eq!(fmt(f), expected, "bits=0x{bits:016x} value={f:e}");
        }
    }

    #[test]
    fn qt_float_specials() {
        assert_eq!(fmt(f64::NAN), "NaN");
        assert_eq!(fmt(f64::INFINITY), "+Inf");
        assert_eq!(fmt(f64::NEG_INFINITY), "-Inf");
        assert_eq!(fmt(-0.0), "0");
    }

    #[test]
    fn timestamps_format_as_seconds() {
        let case = |ts: i64, expected: &str| {
            let mut dst = Vec::new();
            append_timestamp(&mut dst, ts);
            assert_eq!(String::from_utf8(dst).unwrap(), expected, "ts={ts}");
        };
        case(1000, "1");
        case(1500, "1.5");
        case(1_577_836_800_000, "1577836800");
        case(1_577_836_800_123, "1577836800.123");
        case(1_577_836_800_001, "1577836800.001");
        case(999, "0.999");
        case(1, "0.001");
        case(0, "0");
    }

    #[test]
    fn json_string_escaping() {
        let esc = |s: &str| {
            let mut dst = Vec::new();
            append_json_string(&mut dst, s.as_bytes());
            String::from_utf8(dst).unwrap()
        };
        assert_eq!(esc("plain"), "\"plain\"");
        assert_eq!(esc("a\"b\\c"), "\"a\\\"b\\\\c\"");
        assert_eq!(esc("t\tn\nr\r"), "\"t\\tn\\nr\\r\"");
        assert_eq!(esc("<'>"), "\"\\u003c\\u0027>\"");
        assert_eq!(esc("\x01\x1f"), "\"\\u0001\\u001f\"");
        assert_eq!(esc("héllo"), "\"héllo\"");
    }

    #[test]
    fn metric_name_object_shapes() {
        let mut dst = Vec::new();
        let mn = MetricName::default();
        append_metric_name_object(&mut dst, &mn);
        assert_eq!(dst, b"{}");

        let mut mn = MetricName {
            metric_group: b"up".to_vec(),
            ..Default::default()
        };
        dst.clear();
        append_metric_name_object(&mut dst, &mn);
        assert_eq!(dst, b"{\"__name__\":\"up\"}");

        mn.add_tag("job", "esm");
        mn.add_tag("instance", "x:8428");
        dst.clear();
        append_metric_name_object(&mut dst, &mn);
        assert_eq!(
            String::from_utf8(dst.clone()).unwrap(),
            "{\"__name__\":\"up\",\"job\":\"esm\",\"instance\":\"x:8428\"}"
        );
    }

    #[test]
    fn export_json_line_special_values() {
        let mut mn = MetricName {
            metric_group: b"m".to_vec(),
            ..Default::default()
        };
        mn.add_tag("a", "b");
        let mut dst = Vec::new();
        write_export_json_line(
            &mut dst,
            &mn,
            &[1.5, f64::NAN, f64::INFINITY, f64::NEG_INFINITY],
            &[1000, 2000, 3000, 4000],
        );
        assert_eq!(
            String::from_utf8(dst).unwrap(),
            "{\"metric\":{\"__name__\":\"m\",\"a\":\"b\"},\"values\":[1.5,null,\"Infinity\",\"-Infinity\"],\"timestamps\":[1000,2000,3000,4000]}\n"
        );

        let mut dst = Vec::new();
        write_export_json_line(&mut dst, &mn, &[], &[]);
        assert!(dst.is_empty());
    }
}
