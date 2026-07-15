//! Integration tests for `esm_protoparser::csvimport`'s `pub` API
//! (`parse_column_descriptors`, `Rows::unmarshal[_detect_header]`,
//! `parse_stream`). Split out from `src/csvimport.rs` to keep that file
//! under the repo's file-size guideline; see its module doc.
//!
//! Ported from upstream VictoriaMetrics v1.146.0
//! `lib/protoparser/csvimport/{column_descriptor_test.go,parser_test.go}`.
//! Every case that used a `time:custom:<layout>` column upstream was adapted
//! to `rfc3339` instead (same date/time values, `T`-separated) since this
//! port doesn't support `custom:` (see `csvimport.rs`'s module doc).

use esm_protoparser::csvimport::{self, ColumnDescriptor, Row, TimeFormat};

fn cds(format: &str) -> Vec<ColumnDescriptor> {
    csvimport::parse_column_descriptors(format).unwrap()
}

/// `(metric, tags, value, timestamp)`, owned, for test assertions.
type OwnedRow = (String, Vec<(String, String)>, f64, i64);

// ---------------------------------------------------------------------------
// Column descriptor grammar.
// ---------------------------------------------------------------------------

#[test]
fn column_descriptors_success_golden_cases() {
    let cd_time = |fmt| ColumnDescriptor {
        parse_timestamp: Some(fmt),
        ..Default::default()
    };
    let cd_label = |name: &str| ColumnDescriptor {
        tag_name: name.to_owned(),
        ..Default::default()
    };
    let cd_metric = |name: &str| ColumnDescriptor {
        metric_name: name.to_owned(),
        ..Default::default()
    };

    assert_eq!(
        cds("1:time:unix_s,3:metric:temperature"),
        vec![
            cd_time(TimeFormat::UnixSeconds),
            ColumnDescriptor::default(),
            cd_metric("temperature"),
        ]
    );
    assert_eq!(
        cds("2:time:unix_ns,1:metric:temperature,3:label:city,4:label:country"),
        vec![
            cd_metric("temperature"),
            cd_time(TimeFormat::UnixNanos),
            cd_label("city"),
            cd_label("country"),
        ]
    );
    assert_eq!(
        cds("2:time:unix_ms,1:metric:temperature"),
        vec![cd_metric("temperature"), cd_time(TimeFormat::UnixMillis)]
    );
    assert_eq!(
        cds("2:time:rfc3339,1:metric:temperature"),
        vec![cd_metric("temperature"), cd_time(TimeFormat::Rfc3339)]
    );
}

#[test]
fn column_descriptors_failure_golden_cases() {
    let cases = [
        "",
        "1:time:unix_s", // missing metric column
        "1:label:aaa",   // missing metric column
        "foo:time:unix_s,bar:metric:temp",
        "0:metric:aaa",
        "-123:metric:aaa",
        "1:time:unix_s,2:time:rfc3339,3:metric:aaa", // duplicate time column
        "1:time:custom:2006,2:time:rfc3339,3:metric:aaa", // custom rejected + duplicate
        "1:time:foobar,2:metric:aaa",                // invalid time format
        "1:time:,2:metric:aaa",
        "1:time:sss:sss,2:metric:aaa",
        "2:label:,1:metric:aaa",   // empty label name
        "1:metric:",               // empty metric name
        "1:metric:aaa,2:aaaa:bbb", // unknown type
        "1:metric:a,1:metric:b",   // duplicate column number
        "70000:metric:aaa",        // column pos above the 64Ki cap
    ];
    for s in cases {
        assert!(
            csvimport::parse_column_descriptors(s).is_err(),
            "expected error for {s:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Row parsing.
// ---------------------------------------------------------------------------

fn unmarshal(format: &str, s: &str) -> Vec<OwnedRow> {
    let cds = cds(format);
    let mut rs = csvimport::Rows::default();
    rs.unmarshal(s, &cds, |msg| panic!("unexpected error for {s:?}: {msg}"));
    owned(rs.rows())
}

fn owned(rows: &[Row]) -> Vec<OwnedRow> {
    rows.iter()
        .map(|r| (r.metric.clone(), r.tags.clone(), r.value, r.timestamp))
        .collect()
}

fn unmarshal_err_count(format: &str, s: &str) -> usize {
    let cds = cds(format);
    let mut rs = csvimport::Rows::default();
    let mut errs = 0;
    rs.unmarshal(s, &cds, |_| errs += 1);
    assert!(rs.rows().is_empty(), "expected no rows for {s:?}");
    errs
}

#[test]
fn basic_row_and_tags() {
    assert_eq!(unmarshal("1:metric:foo", ""), vec![]);
    assert_eq!(
        unmarshal("1:metric:foo", "123"),
        vec![("foo".to_owned(), vec![], 123.0, 0)]
    );
    assert_eq!(
        unmarshal(
            "1:metric:foo,2:time:unix_s,3:label:foo,4:label:bar",
            "123,456,xxx,yy"
        ),
        vec![(
            "foo".to_owned(),
            vec![
                ("foo".to_owned(), "xxx".to_owned()),
                ("bar".to_owned(), "yy".to_owned())
            ],
            123.0,
            456_000,
        )]
    );
}

#[test]
fn multiple_metrics_share_tags_and_timestamp_quoted_fields() {
    // Adapted from upstream's `custom:` test: same date/time, `T`-separated
    // + `Z`, through `rfc3339` (see module doc).
    let rows = unmarshal(
        "2:metric:bar,1:metric:foo,3:label:foo,4:label:bar,5:time:rfc3339",
        r#""2.34",5.6,"foo"",bar","aa",2015-08-10T20:04:40.123Z"#,
    );
    let tags = vec![
        ("foo".to_owned(), "foo\",bar".to_owned()),
        ("bar".to_owned(), "aa".to_owned()),
    ];
    assert_eq!(
        rows,
        vec![
            ("foo".to_owned(), tags.clone(), 2.34, 1_439_237_080_123),
            ("bar".to_owned(), tags, 5.6, 1_439_237_080_123),
        ]
    );
}

#[test]
fn multiline_multi_metric_rows() {
    // Adapted from upstream's `custom:` test (bid/ask), same substitution.
    // Upstream's raw-string fixture also has leading/trailing whitespace-only
    // lines (gofmt indentation inside a backtick literal, plus a trailing
    // one before the closing backtick); its own test tolerates the logged
    // "missing columns" error those produce (column 1 is unused in this
    // format, so the *leading* per-row whitespace+quote noise lands there
    // and is ignored, but a whitespace-only *line* still yields zero usable
    // columns and is skipped as invalid) — this port does too, so a
    // non-panicking `err_logger` is used here instead of the shared,
    // strict `unmarshal()` helper.
    let cds = cds("2:label:symbol,3:time:rfc3339,4:metric:bid,5:metric:ask");
    let mut rs = csvimport::Rows::default();
    rs.unmarshal(
        "\n\t\t\t\"aaa\",\"AUDCAD\",\"2015-08-10T00:00:01.000Z\",0.9725,0.97273\n\
             \t\t\t\"aaa\",\"AUDCAD\",\"2015-08-10T00:00:02.000Z\",0.97253,0.97276\n\t\t\t",
        &cds,
        |_| {},
    );
    let rows = owned(rs.rows());
    let symbol = |v: &str| vec![("symbol".to_owned(), v.to_owned())];
    assert_eq!(
        rows,
        vec![
            (
                "bid".to_owned(),
                symbol("AUDCAD"),
                0.9725,
                1_439_164_801_000
            ),
            (
                "ask".to_owned(),
                symbol("AUDCAD"),
                0.97273,
                1_439_164_801_000
            ),
            (
                "bid".to_owned(),
                symbol("AUDCAD"),
                0.97253,
                1_439_164_802_000
            ),
            (
                "ask".to_owned(),
                symbol("AUDCAD"),
                0.97276,
                1_439_164_802_000
            ),
        ]
    );
}

#[test]
fn superfluous_and_empty_columns() {
    assert_eq!(
        unmarshal("1:metric:foo", "123,456,foo,bar"),
        vec![("foo".to_owned(), vec![], 123.0, 0)]
    );
    assert_eq!(
        unmarshal("2:metric:foo", "123,-45.6,foo,bar"),
        vec![("foo".to_owned(), vec![], -45.6, 0)]
    );
    // skip metrics with empty values
    assert_eq!(
        unmarshal(
            "1:metric:foo,2:metric:bar,3:metric:baz,4:metric:quux",
            "1,,,2"
        ),
        vec![
            ("foo".to_owned(), vec![], 1.0, 0),
            ("quux".to_owned(), vec![], 2.0, 0),
        ]
    );
    // last metric with an empty value (VM issue #4048)
    assert_eq!(
        unmarshal("1:metric:foo,2:metric:bar", "123,"),
        vec![("foo".to_owned(), vec![], 123.0, 0)]
    );
    // all metrics empty -> no row at all
    assert_eq!(
        unmarshal("1:metric:foo,2:metric:bar,3:label:xx", ",,abc"),
        vec![]
    );
    // labels with empty value are skipped, not errors
    assert_eq!(
        unmarshal(
            "1:metric:foo,2:label:bar,3:label:baz,4:label:xxx",
            "123,x,,"
        ),
        vec![(
            "foo".to_owned(),
            vec![("bar".to_owned(), "x".to_owned())],
            123.0,
            0
        )]
    );
    assert_eq!(
        unmarshal("1:metric:foo,2:label:bar,3:label:baz,4:label:xxx", "123,,,"),
        vec![("foo".to_owned(), vec![], 123.0, 0)]
    );
}

#[test]
fn column_gap_between_multiple_metrics_and_rfc3339_offset() {
    // VM issue #3540: a column gap (M40/M50 empty) plus an rfc3339 offset
    // (not `Z`).
    let rows = unmarshal(
        "1:label:mytest,2:time:rfc3339,3:metric:M10,4:metric:M20,5:metric:M30,6:metric:M40,7:metric:M50,8:metric:M60",
        "test,2022-12-25T16:57:12+01:00,10,20,30,,,60,70,80",
    );
    let tag = vec![("mytest".to_owned(), "test".to_owned())];
    assert_eq!(
        rows,
        vec![
            ("M10".to_owned(), tag.clone(), 10.0, 1_671_983_832_000),
            ("M20".to_owned(), tag.clone(), 20.0, 1_671_983_832_000),
            ("M30".to_owned(), tag.clone(), 30.0, 1_671_983_832_000),
            ("M60".to_owned(), tag, 60.0, 1_671_983_832_000),
        ]
    );
}

#[test]
fn rfc3339_rejects_invalid_calendar_dates_like_go() {
    // Go time.Parse(time.RFC3339) rejects all of these ("day out of range"
    // / "second out of range"; verified against go1.26). Before the
    // calendar-validation fix they silently converted to wrong-but-
    // plausible timestamps (e.g. Feb 30 -> Mar 1).
    let format = "1:time:rfc3339,2:metric:m";
    for ts in [
        "2020-02-30T00:00:00Z", // Feb 30 never exists
        "2023-02-29T00:00:00Z", // 2023 is not a leap year
        "2020-04-31T00:00:00Z", // April has 30 days
        "2020-01-01T23:59:60Z", // leap second: Go rejects it too
    ] {
        assert_eq!(
            unmarshal_err_count(format, &format!("{ts},1")),
            1,
            "{ts} must be rejected"
        );
    }
    // Positive controls: real calendar edge dates still parse.
    assert_eq!(
        unmarshal(format, "2024-02-29T00:00:00Z,1"), // 2024 IS a leap year
        vec![row("m", &[], 1.0, 1_709_164_800_000)]
    );
    assert_eq!(
        unmarshal(format, "2020-04-30T00:00:00Z,1"),
        vec![row("m", &[], 1.0, 1_588_204_800_000)]
    );
}

#[test]
fn invalid_rows_are_skipped() {
    assert_eq!(
        unmarshal_err_count("1:metric:foo,2:time:rfc3339", "234,foobar"),
        1
    );
    assert_eq!(
        unmarshal_err_count("1:metric:foo,2:time:unix_s", "234,foobar"),
        1
    );
    assert_eq!(
        unmarshal_err_count("1:metric:foo,2:time:unix_ms", "234,foobar"),
        1
    );
    assert_eq!(
        unmarshal_err_count("1:metric:foo,2:time:unix_ns", "234,foobar"),
        1
    );
    assert_eq!(unmarshal_err_count("1:metric:foo", "12foobar"), 1);
    assert_eq!(unmarshal_err_count("3:metric:aaa", "123,456"), 1); // missing columns
    assert_eq!(unmarshal_err_count("1:metric:foo,2:label:bar", "123"), 1); // missing columns
    assert_eq!(unmarshal_err_count("1:label:foo,2:metric:bar", "aaa"), 1); // missing columns
}

#[test]
fn reset_clears_rows_and_unmarshal_is_repeatable() {
    let cds = cds("1:metric:foo");
    let mut rs = csvimport::Rows::default();
    rs.unmarshal("123", &cds, |_| {});
    assert_eq!(rs.rows().len(), 1);
    rs.reset();
    assert!(rs.rows().is_empty());
    // Unmarshaling again on the same (now-empty) Rows must still work.
    rs.unmarshal("123\n456", &cds, |_| {});
    assert_eq!(rs.rows().len(), 2);
}

// ---------------------------------------------------------------------------
// Header autodetection.
// ---------------------------------------------------------------------------

fn unmarshal_detect_header(format: &str, s: &str) -> Vec<OwnedRow> {
    let cds = cds(format);
    let mut rs = csvimport::Rows::default();
    rs.unmarshal_detect_header(s, &cds, |msg| panic!("unexpected error for {s:?}: {msg}"));
    owned(rs.rows())
}

fn row(
    metric: &str,
    tags: &[(&str, &str)],
    value: f64,
    ts: i64,
) -> (String, Vec<(String, String)>, f64, i64) {
    (
        metric.to_owned(),
        tags.iter()
            .map(|&(k, v)| (k.to_owned(), v.to_owned()))
            .collect(),
        value,
        ts,
    )
}

#[test]
fn header_detection_non_numeric_and_numeric_first_rows() {
    // Non-numeric metric/timestamp/label header rows are skipped.
    assert_eq!(
        unmarshal_detect_header("1:metric:foo", "value\n123"),
        vec![row("foo", &[], 123.0, 0)]
    );
    assert_eq!(
        unmarshal_detect_header("1:metric:foo,2:time:unix_s", "value,timestamp\n123,456"),
        vec![row("foo", &[], 123.0, 456_000)]
    );
    assert_eq!(
        unmarshal_detect_header(
            "1:metric:foo,2:time:rfc3339",
            "value,timestamp\n10,2024-01-01T00:00:00Z"
        ),
        vec![row("foo", &[], 10.0, 1_704_067_200_000)]
    );
    assert_eq!(
        unmarshal_detect_header(
            "1:label:host,2:metric:cpu,3:time:unix_s",
            "host,value,timestamp\nmyhost,99.5,1000"
        ),
        vec![row("cpu", &[("host", "myhost")], 99.5, 1_000_000)]
    );
    assert_eq!(
        unmarshal_detect_header(
            "1:metric:bid,2:metric:ask,3:time:unix_s",
            "bid,ask,timestamp\n1.5,1.6,1000"
        ),
        vec![
            row("bid", &[], 1.5, 1_000_000),
            row("ask", &[], 1.6, 1_000_000)
        ]
    );
    // A numeric first row is data, not a header.
    assert_eq!(
        unmarshal_detect_header("1:metric:foo,2:time:unix_s", "123,456"),
        vec![row("foo", &[], 123.0, 456_000)]
    );
    // A numeric label ("404") is not a false-positive header trigger.
    assert_eq!(
        unmarshal_detect_header(
            "1:label:status,2:metric:count,3:time:unix_s",
            "404,100,1704067200"
        ),
        vec![row("count", &[("status", "404")], 100.0, 1_704_067_200_000)]
    );
    // Header only, no data rows.
    assert_eq!(
        unmarshal_detect_header("1:metric:foo,2:time:unix_s", "value,timestamp"),
        vec![]
    );
    // Text label columns alone don't trigger header detection.
    assert_eq!(
        unmarshal_detect_header(
            "1:label:host,2:metric:foo,3:time:unix_s",
            "myhost,42,1000\notherhost,99,2000"
        ),
        vec![
            row("foo", &[("host", "myhost")], 42.0, 1_000_000),
            row("foo", &[("host", "otherhost")], 99.0, 2_000_000),
        ]
    );
}

#[test]
fn header_detection_only_applies_to_the_very_first_row() {
    // A header is only ever recognized on the first row (mirroring
    // `parse_stream` only autodetecting on the first block); subsequent
    // non-numeric-looking rows are just invalid data rows, not headers.
    assert_eq!(
        unmarshal_detect_header(
            "1:metric:foo,2:time:unix_s",
            "value,timestamp\n10,100\n20,200\n30,300"
        ),
        vec![
            row("foo", &[], 10.0, 100_000),
            row("foo", &[], 20.0, 200_000),
            row("foo", &[], 30.0, 300_000),
        ]
    );
}

// ---------------------------------------------------------------------------
// Backward compatibility / export-import round trip.
// ---------------------------------------------------------------------------

#[test]
fn unmarshal_without_header_detection_is_unaffected_by_a_header_line() {
    let format = "1:label:env,2:metric:m,3:time:unix_s";
    assert_eq!(
        unmarshal(format, "prod,42,1000\nstaging,99,2000"),
        vec![
            row("m", &[("env", "prod")], 42.0, 1_000_000),
            row("m", &[("env", "staging")], 99.0, 2_000_000),
        ]
    );
}

#[test]
fn export_import_round_trip_with_header_detection() {
    let format = "1:label:host,2:metric:cpu,3:time:unix_s";
    let cds = cds(format);
    // Simulated `/api/v1/export/csv`-style output: header + data rows.
    let exported = [
        "host,value,timestamp",
        "server1,85.5,1704067200",
        "server2,92.3,1704067200",
        "server1,88.1,1704067260",
    ]
    .join("\n");

    let mut rs = csvimport::Rows::default();
    rs.unmarshal_detect_header(&exported, &cds, |msg| panic!("unexpected error: {msg}"));
    assert_eq!(
        owned(rs.rows()),
        vec![
            row("cpu", &[("host", "server1")], 85.5, 1_704_067_200_000),
            row("cpu", &[("host", "server2")], 92.3, 1_704_067_200_000),
            row("cpu", &[("host", "server1")], 88.1, 1_704_067_260_000),
        ]
    );

    // Without header detection, the header line is just an invalid data row
    // (non-numeric `value`/`timestamp`) that gets skipped; the 3 real data
    // rows still parse fine.
    rs.reset();
    let mut errs = 0;
    rs.unmarshal(&exported, &cds, |_| errs += 1);
    assert_eq!(rs.rows().len(), 3);
    assert_eq!(errs, 1);
}

// ---------------------------------------------------------------------------
// Streaming parser.
// ---------------------------------------------------------------------------

fn collect(cds: &[ColumnDescriptor], data: &[u8], encoding: &str) -> Vec<(String, f64, i64)> {
    let mut out = Vec::new();
    csvimport::parse_stream(
        data,
        encoding,
        cds,
        |msg| panic!("{msg}"),
        |rows| {
            for r in rows {
                out.push((r.metric.clone(), r.value, r.timestamp));
            }
            Ok(())
        },
    )
    .unwrap();
    out
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[test]
fn parse_stream_good_data_from_the_brief() {
    // The task brief's worked example.
    let cds = cds("1:label:device,2:metric:temperature,3:time:unix_s");
    let rows = collect(&cds, b"sensor-1,23.5,1447116400\n", "");
    assert_eq!(
        rows,
        vec![("temperature".to_owned(), 23.5, 1_447_116_400_000)]
    );
}

#[test]
fn parse_stream_missing_timestamp_defaults_to_now() {
    let cds = cds("1:metric:foo");
    let before = now_millis();
    let rows = collect(&cds, b"42\n", "");
    let after = now_millis();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "foo");
    assert!(rows[0].2 >= before && rows[0].2 <= after);
}

#[test]
fn parse_stream_header_autodetect_first_block_only() {
    let cds = cds("1:metric:foo,2:time:unix_s");
    let rows = collect(&cds, b"value,timestamp\n123,456\n", "");
    assert_eq!(rows, vec![("foo".to_owned(), 123.0, 456_000)]);
}

#[test]
fn parse_stream_gzip_body() {
    use std::io::Write;
    let cds = cds("1:label:device,2:metric:temperature,3:time:unix_s");
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(b"sensor-1,23.5,1447116400\n").unwrap();
    let gz = enc.finish().unwrap();
    assert_eq!(
        collect(&cds, &gz, "gzip"),
        vec![("temperature".to_owned(), 23.5, 1_447_116_400_000)]
    );
}

#[test]
fn parse_stream_quoted_field_with_comma() {
    let cds = cds("1:label:city,2:metric:temperature,3:time:unix_s");
    let rows = collect(&cds, b"\"Springfield, IL\",23.5,1447116400\n", "");
    assert_eq!(
        rows,
        vec![("temperature".to_owned(), 23.5, 1_447_116_400_000)]
    );
}

#[test]
fn parse_stream_too_long_line_errors() {
    let cds = cds("1:metric:foo");
    let data = vec![b'a'; csvimport::MAX_LINE_LEN + 1024];
    let err = csvimport::parse_stream(&data[..], "", &cds, |_| {}, |_| Ok(())).unwrap_err();
    assert!(
        matches!(err, csvimport::Error::TooLongLine { .. }),
        "unexpected error: {err}"
    );
}

#[test]
fn parse_stream_callback_error_propagates() {
    let cds = cds("1:metric:foo");
    let err = csvimport::parse_stream(b"1\n".as_slice(), "", &cds, |_| {}, |_| Err("boom".into()))
        .unwrap_err();
    match err {
        csvimport::Error::Callback(source) => assert_eq!(source.to_string(), "boom"),
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn parse_stream_invalid_lines_are_skipped_others_kept() {
    let cds = cds("1:metric:foo");
    let mut errs = 0;
    let mut out = Vec::new();
    csvimport::parse_stream(
        b"1\ngarbage-not-a-number\n2\n".as_slice(),
        "",
        &cds,
        |_| errs += 1,
        |rows| {
            for r in rows {
                out.push((r.metric.clone(), r.value));
            }
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(out, vec![("foo".to_owned(), 1.0), ("foo".to_owned(), 2.0)]);
    assert_eq!(errs, 1);
}
