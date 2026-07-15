//! Unit tests for [`super`] (`opentsdbhttp`), split into their own file to
//! keep `opentsdbhttp.rs` under the project's 800-line-per-file guideline.
//! Wired up via `#[path]` from `opentsdbhttp.rs`, so this is still the
//! ordinary `opentsdbhttp::tests` child module with access to the parent's
//! private items (`SECOND_MASK`, `current_time_seconds`).

use super::*;

fn parse_ok(s: &str) -> Vec<Row> {
    let v: serde_json::Value = serde_json::from_str(s).unwrap();
    let mut rows = Rows::default();
    rows.unmarshal(&v, |msg| panic!("unexpected error for {s}: {msg}"));
    rows.rows().to_vec()
}

fn parse_invalid(s: &str) -> usize {
    let v: serde_json::Value = serde_json::from_str(s).unwrap();
    let mut rows = Rows::default();
    let mut errs = 0;
    rows.unmarshal(&v, |_| errs += 1);
    assert!(rows.rows().is_empty(), "expected no rows for {s}");
    errs
}

fn row(metric: &str, tags: &[(&str, &str)], value: f64, timestamp: i64) -> Row {
    Row {
        metric: metric.to_owned(),
        tags: tags
            .iter()
            .map(|&(k, v)| Tag {
                key: k.to_owned(),
                value: v.to_owned(),
            })
            .collect(),
        value,
        timestamp,
    }
}

#[test]
fn single_object_with_tags() {
    let got =
        parse_ok(r#"{"metric": "foobar", "timestamp": 789, "value": -123.456, "tags": {"a":"b"}}"#);
    assert_eq!(got, vec![row("foobar", &[("a", "b")], -123.456, 789)]);
}

#[test]
fn timestamp_as_string() {
    let got = parse_ok(
        r#"{"metric": "foobar", "timestamp": "1789", "value": -123.456, "tags": {"a":"b"}}"#,
    );
    assert_eq!(got, vec![row("foobar", &[("a", "b")], -123.456, 1789)]);
}

#[test]
fn timestamp_as_float_is_truncated() {
    let got = parse_ok(
        r#"{"metric": "foobar", "timestamp": 17.89, "value": -123.456, "tags": {"a":"b"}}"#,
    );
    assert_eq!(got, vec![row("foobar", &[("a", "b")], -123.456, 17)]);
}

#[test]
fn empty_tags_object_yields_no_tags() {
    let got = parse_ok(r#"{"metric": "foobar", "timestamp": 789, "value": -123.456, "tags": {}}"#);
    assert_eq!(got, vec![row("foobar", &[], -123.456, 789)]);
}

#[test]
fn missing_tags_yields_no_tags() {
    let got = parse_ok(r#"{"metric": "foobar", "timestamp": 789, "value": -123.456}"#);
    assert_eq!(got, vec![row("foobar", &[], -123.456, 789)]);
}

#[test]
fn empty_tag_key_or_value_is_skipped() {
    let got = parse_ok(
        r#"{"metric": "foobar", "timestamp": 123, "value": -123.456, "tags": {"a":"", "b":"c", "": "d"}}"#,
    );
    assert_eq!(got, vec![row("foobar", &[("b", "c")], -123.456, 123)]);
}

#[test]
fn value_as_string() {
    let got = parse_ok(
        r#"{"metric": "foobar", "timestamp": 789, "value": "-12.456", "tags": {"a":"b"}}"#,
    );
    assert_eq!(got, vec![row("foobar", &[("a", "b")], -12.456, 789)]);
}

#[test]
fn missing_timestamp_defaults_to_zero_sentinel() {
    // Rows::unmarshal leaves this as 0; `parse_stream`'s fixup fills it.
    let got = parse_ok(r#"{"metric": "foobar", "value": "-12.456", "tags": {"a":"b"}}"#);
    assert_eq!(got, vec![row("foobar", &[("a", "b")], -12.456, 0)]);
}

#[test]
fn multiple_tags_preserve_input_order() {
    let got = parse_ok(
        r#"{"metric": "foo", "value": 1, "timestamp": 2, "tags": {"bar":"baz", "x": "y"}}"#,
    );
    assert_eq!(got, vec![row("foo", &[("bar", "baz"), ("x", "y")], 1.0, 2)]);
}

#[test]
fn array_of_objects() {
    let got = parse_ok(
        r#"[{"metric": "foo", "value": "0.3", "timestamp": 2, "tags": {"a":"b"}},
{"metric": "bar.baz", "value": 0.34, "timestamp": 43, "tags": {"a":"b"}}]"#,
    );
    assert_eq!(
        got,
        vec![
            row("foo", &[("a", "b")], 0.3, 2),
            row("bar.baz", &[("a", "b")], 0.34, 43),
        ]
    );
}

#[test]
fn invalid_entry_in_array_is_skipped_others_kept() {
    let v: serde_json::Value = serde_json::from_str(
        r#"[{"metric": "foo", "value": 1, "timestamp": 2, "tags": {"a":"b"}},
{"metric": "bad", "value": "not-a-number"},
{"metric": "bar", "value": 3, "timestamp": 4}]"#,
    )
    .unwrap();
    let mut rows = Rows::default();
    let mut errs = 0;
    rows.unmarshal(&v, |_| errs += 1);
    assert_eq!(errs, 1, "expected exactly one skipped entry");
    assert_eq!(
        rows.rows().to_vec(),
        vec![row("foo", &[("a", "b")], 1.0, 2), row("bar", &[], 3.0, 4)]
    );
}

#[test]
fn reusable_across_calls() {
    let s = r#"{"metric": "m", "value": 1, "timestamp": 1, "tags": {"a":"b"}}"#;
    let v: serde_json::Value = serde_json::from_str(s).unwrap();
    let mut rows = Rows::default();
    rows.unmarshal(&v, |msg| panic!("unexpected error: {msg}"));
    assert_eq!(rows.rows().len(), 1);
    rows.unmarshal(&v, |msg| panic!("unexpected error: {msg}"));
    assert_eq!(rows.rows().len(), 1);
    rows.reset();
    assert!(rows.rows().is_empty());
}

#[test]
fn non_object_array_top_level_values_yield_zero_rows_without_error() {
    // Syntactically valid JSON, but not object/array-of-objects: this is
    // still logged via err_logger (matching Go's `invalidLines.Inc()`),
    // but it is not the request-level `Error::Unmarshal` failure that a
    // JSON *syntax* error would be — see the module doc's "Deviations"
    // section. `parse_stream`'s own test of this shape
    // (`parse_stream_non_object_array_top_level_is_not_an_error`)
    // confirms the callback still runs (with zero rows) rather than the
    // whole request failing.
    for s in ["1", "\"foo\"", "null"] {
        let v: serde_json::Value = serde_json::from_str(s).unwrap();
        let mut rows = Rows::default();
        let mut logged = 0;
        rows.unmarshal(&v, |_| logged += 1);
        assert!(rows.rows().is_empty(), "expected no rows for {s}");
        assert_eq!(logged, 1, "expected exactly one log call for {s}");
    }
}

#[test]
fn failures() {
    // Incomplete object.
    assert!(parse_invalid("{}") > 0);
    assert!(parse_invalid(r#"{"metric": "aaa"}"#) > 0);
    assert!(parse_invalid(r#"{"metric": "aaa", "timestamp": 1122}"#) > 0);
    assert!(parse_invalid(r#"{"metric": "aaa", "timestamp": "tststs"}"#) > 0);
    assert!(parse_invalid(r#"{"timestamp": 1122, "value": 33}"#) > 0);
    assert!(parse_invalid(r#"{"value": 33}"#) > 0);
    assert!(parse_invalid(r#"{"value": 33, "tags": {"fooo":"bar"}}"#) > 0);

    // Invalid value.
    assert!(parse_invalid(r#"{"metric": "aaa", "timestamp": 1122, "value": "0.0.0"}"#) > 0);

    // Invalid metric type/empty.
    assert!(
        parse_invalid(
            r#"{"metric": "", "timestamp": 1122, "value": 0.45, "tags": {"foo": "bar"}}"#
        ) > 0
    );
    assert!(
        parse_invalid(
            r#"{"metric": ["aaa"], "timestamp": 1122, "value": 0.45, "tags": {"foo": "bar"}}"#
        ) > 0
    );
    assert!(
        parse_invalid(
            r#"{"metric": {"aaa":1}, "timestamp": 1122, "value": 0.45, "tags": {"foo": "bar"}}"#
        ) > 0
    );
    assert!(
        parse_invalid(r#"{"metric": 1, "timestamp": 1122, "value": 0.45, "tags": {"foo": "bar"}}"#)
            > 0
    );

    // Invalid timestamp type.
    assert!(
        parse_invalid(
            r#"{"metric": "aaa", "timestamp": "foobar", "value": 0.45, "tags": {"foo": "bar"}}"#
        ) > 0
    );
    assert!(
        parse_invalid(
            r#"{"metric": "aaa", "timestamp": [1,2], "value": 0.45, "tags": {"foo": "bar"}}"#
        ) > 0
    );
    assert!(
        parse_invalid(
            r#"{"metric": "aaa", "timestamp": {"a":1}, "value": 0.45, "tags": {"foo": "bar"}}"#
        ) > 0
    );

    // Invalid value type.
    assert!(
        parse_invalid(
            r#"{"metric": "aaa", "timestamp": 1122, "value": [0,1], "tags": {"foo":"bar"}}"#
        ) > 0
    );
    assert!(
        parse_invalid(
            r#"{"metric": "aaa", "timestamp": 1122, "value": {"a":1}, "tags": {"foo":"bar"}}"#
        ) > 0
    );
    assert!(
        parse_invalid(
            r#"{"metric": "aaa", "timestamp": 1122, "value": "foobar", "tags": {"foo":"bar"}}"#
        ) > 0
    );

    // Invalid tags type.
    assert!(parse_invalid(r#"{"metric": "aaa", "timestamp": 1122, "value": 0.45, "tags": 1}"#) > 0);
    assert!(
        parse_invalid(r#"{"metric": "aaa", "timestamp": 1122, "value": 0.45, "tags": [1,2]}"#) > 0
    );
    assert!(
        parse_invalid(r#"{"metric": "aaa", "timestamp": 1122, "value": 0.45, "tags": "foo"}"#) > 0
    );

    // Invalid tag value type.
    assert!(
        parse_invalid(
            r#"{"metric": "aaa", "timestamp": 1122, "value": 0.45, "tags": {"foo": ["bar"]}}"#
        ) > 0
    );
    assert!(
        parse_invalid(
            r#"{"metric": "aaa", "timestamp": 1122, "value": 0.45, "tags": {"foo": {"bar":"baz"}}}"#
        ) > 0
    );
    assert!(
        parse_invalid(r#"{"metric": "aaa", "timestamp": 1122, "value": 0.45, "tags": {"foo": 1}}"#)
            > 0
    );
}

// -----------------------------------------------------------------
// Streaming parser tests.
// -----------------------------------------------------------------

#[derive(Debug, PartialEq)]
struct OwnedRow {
    metric: String,
    tags: Vec<(String, String)>,
    value: f64,
    timestamp: i64,
}

fn collect_rows(rows: &[Row], dst: &mut Vec<OwnedRow>) {
    for r in rows {
        dst.push(OwnedRow {
            metric: r.metric.clone(),
            tags: r
                .tags
                .iter()
                .map(|t| (t.key.clone(), t.value.clone()))
                .collect(),
            value: r.value,
            timestamp: r.timestamp,
        });
    }
}

fn owned_row(metric: &str, tags: &[(&str, &str)], value: f64, timestamp: i64) -> OwnedRow {
    OwnedRow {
        metric: metric.to_owned(),
        tags: tags
            .iter()
            .map(|&(k, v)| (k.to_owned(), v.to_owned()))
            .collect(),
        value,
        timestamp,
    }
}

#[test]
fn parse_stream_single_object() {
    let mut rows = Vec::new();
    parse_stream(
        r#"{"metric":"foo.bar","timestamp":1727879909,"value":123.456,"tags":{"tag1":"v1"}}"#
            .as_bytes(),
        "",
        |msg| panic!("{msg}"),
        |rs| {
            collect_rows(rs, &mut rows);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![owned_row(
            "foo.bar",
            &[("tag1", "v1")],
            123.456,
            1_727_879_909_000
        )]
    );
}

#[test]
fn parse_stream_array() {
    let mut rows = Vec::new();
    parse_stream(
        r#"[{"metric":"foo","timestamp":100,"value":1,"tags":{"a":"b"}},{"metric":"bar","timestamp":200,"value":2,"tags":{"a":"b"}}]"#.as_bytes(),
        "",
        |msg| panic!("{msg}"),
        |rs| {
            collect_rows(rs, &mut rows);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![
            owned_row("foo", &[("a", "b")], 1.0, 100_000),
            owned_row("bar", &[("a", "b")], 2.0, 200_000),
        ]
    );
}

#[test]
fn parse_stream_missing_timestamp_fills_now() {
    let before = current_time_seconds() * 1000;
    let mut rows = Vec::new();
    parse_stream(
        r#"{"metric":"foo","value":1,"tags":{"a":"b"}}"#.as_bytes(),
        "",
        |msg| panic!("{msg}"),
        |rs| {
            collect_rows(rs, &mut rows);
            Ok(())
        },
    )
    .unwrap();
    let after = current_time_seconds() * 1000;
    assert_eq!(rows.len(), 1);
    let ts = rows[0].timestamp;
    assert!(
        ts >= before && ts <= after,
        "timestamp {ts} not in [{before}, {after}]"
    );
}

#[test]
fn parse_stream_already_millisecond_timestamp_is_not_rescaled() {
    let ts_millis: i64 = 1_700_000_000_000;
    assert_ne!(ts_millis & SECOND_MASK, 0, "test fixture sanity check");
    let mut rows = Vec::new();
    parse_stream(
        format!(r#"{{"metric":"foo","timestamp":{ts_millis},"value":1,"tags":{{"a":"b"}}}}"#)
            .as_bytes(),
        "",
        |msg| panic!("{msg}"),
        |rs| {
            collect_rows(rs, &mut rows);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].timestamp, ts_millis);
}

#[test]
fn parse_stream_invalid_entries_are_skipped() {
    let mut rows = Vec::new();
    let mut errs = 0;
    parse_stream(
        r#"[{"metric":"foo","value":1,"timestamp":1},{"metric":"bad","value":"nope"},{"metric":"bar","value":2,"timestamp":2}]"#.as_bytes(),
        "",
        |_| errs += 1,
        |rs| {
            collect_rows(rs, &mut rows);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![
            owned_row("foo", &[], 1.0, 1_000),
            owned_row("bar", &[], 2.0, 2_000),
        ]
    );
    assert_eq!(errs, 1);
}

#[test]
fn parse_stream_malformed_json_syntax_is_a_request_error() {
    let err = parse_stream(b"{not json".as_slice(), "", |_| {}, |_| Ok(())).unwrap_err();
    assert!(
        matches!(err, Error::Unmarshal { .. }),
        "unexpected error: {err}"
    );
}

#[test]
fn parse_stream_non_object_array_top_level_is_not_an_error() {
    // Valid JSON, wrong shape: zero rows, no error (see module doc).
    let mut calls = 0;
    parse_stream(
        b"null".as_slice(),
        "",
        |_| {},
        |rs| {
            calls += 1;
            assert!(rs.is_empty());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(calls, 1);
}

#[test]
fn parse_stream_gzip_roundtrip() {
    use std::io::Write;
    let data =
        r#"{"metric":"foo.bar","timestamp":1727879909,"value":123.456,"tags":{"tag1":"v1"}}"#;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(data.as_bytes()).unwrap();
    let gzipped = encoder.finish().unwrap();

    let mut rows = Vec::new();
    parse_stream(
        &gzipped[..],
        "gzip",
        |msg| panic!("{msg}"),
        |rs| {
            collect_rows(rs, &mut rows);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![owned_row(
            "foo.bar",
            &[("tag1", "v1")],
            123.456,
            1_727_879_909_000
        )]
    );
}

#[test]
fn parse_stream_unsupported_encoding_errors() {
    let err = parse_stream(b"{}".as_slice(), "br", |_| {}, |_| Ok(())).unwrap_err();
    assert!(
        matches!(err, Error::UnsupportedEncoding(ref enc) if enc == "br"),
        "unexpected error: {err}"
    );
}

#[test]
fn parse_stream_callback_error_propagates() {
    let err = parse_stream(
        r#"{"metric":"foo","value":1,"timestamp":1}"#.as_bytes(),
        "",
        |_| {},
        |_| Err("boom".into()),
    )
    .unwrap_err();
    match err {
        Error::Callback(source) => assert_eq!(source.to_string(), "boom"),
        other => panic!("unexpected error: {other}"),
    }
}
