//! Integration tests for `esm_protoparser::opentsdb`, ported from upstream
//! VictoriaMetrics v1.146.0 `lib/protoparser/opentsdb/parser_test.go`
//! (`TestRowsUnmarshalSuccess` / `TestRowsUnmarshalFailure`). These only
//! exercise the public API (`Rows::unmarshal`, `Row`, `Tag`), so they live
//! here rather than in `src/opentsdb.rs` to keep that file under the
//! 800-line guideline.

use esm_protoparser::opentsdb::{Row, Rows, Tag};

fn tag<'a>(key: &'a str, value: &'a str) -> Tag<'a> {
    Tag { key, value }
}

fn row<'a>(metric: &'a str, tags: Vec<Tag<'a>>, value: f64, timestamp: i64) -> Row<'a> {
    Row {
        metric,
        tags,
        value,
        timestamp,
    }
}

/// Asserts `s` parses to exactly `expected`, and that a second parse (row/tag
/// buffer reuse) and a `reset()` behave correctly too.
fn check_success(s: &str, expected: &[Row<'_>]) {
    let mut rows = Rows::default();
    rows.unmarshal(s, |_| {});
    assert_eq!(rows.rows(), expected, "unexpected rows for {s:?}");

    rows.unmarshal(s, |_| {});
    assert_eq!(
        rows.rows(),
        expected,
        "unexpected rows on second unmarshal for {s:?}"
    );

    rows.reset();
    assert!(
        rows.rows().is_empty(),
        "non-empty rows after reset for {s:?}"
    );
}

/// Asserts `s` produces zero rows (whether via an invalid line being skipped
/// and logged, or via blank lines that are silently skipped).
fn check_zero_rows(s: &str) {
    let mut rows = Rows::default();
    rows.unmarshal(s, |_| {});
    assert!(rows.rows().is_empty(), "expected zero rows for {s:?}");

    rows.unmarshal(s, |_| {});
    assert!(
        rows.rows().is_empty(),
        "expected zero rows on second unmarshal for {s:?}"
    );
}

// ---------------------------------------------------------------------------
// TestRowsUnmarshalFailure
// ---------------------------------------------------------------------------

#[test]
fn failure_missing_put_prefix() {
    check_zero_rows("xx");
}

#[test]
fn failure_missing_metric() {
    check_zero_rows("put  111 34");
}

#[test]
fn failure_missing_timestamp() {
    check_zero_rows("put aaa");
}

#[test]
fn failure_missing_value() {
    check_zero_rows("put aaa 1123");
}

#[test]
fn failure_invalid_timestamp() {
    check_zero_rows("put aaa timestamp");
    check_zero_rows("put foobar 3df4 -123456 a=b");
}

#[test]
fn failure_invalid_value() {
    check_zero_rows("put aaa 123 invalid-value");
    check_zero_rows("put foobar 789 -123foo456 a=b");
}

#[test]
fn failure_invalid_multiline() {
    check_zero_rows("put aaa\nbbb 123 34");
}

#[test]
fn failure_invalid_tag() {
    check_zero_rows("put aaa 123 4.5 foo");
}

// ---------------------------------------------------------------------------
// TestRowsUnmarshalSuccess
// ---------------------------------------------------------------------------

#[test]
fn empty_line() {
    check_success("", &[]);
    check_success("\r", &[]);
    check_success("\n\n", &[]);
    check_success("\n\r\n", &[]);
}

#[test]
fn single_line() {
    check_success(
        "put foobar 789 -123.456 a=b",
        &[row("foobar", vec![tag("a", "b")], -123.456, 789)],
    );
}

#[test]
fn empty_tag() {
    check_success(
        "put foobar 789 -123.456 a= b=c =d",
        &[row("foobar", vec![tag("b", "c")], -123.456, 789)],
    );
}

#[test]
fn missing_first_tag() {
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3290
    check_success("put aaa 123 43", &[row("aaa", vec![], 43.0, 123)]);
    check_success("put aaa 123 43 ", &[row("aaa", vec![], 43.0, 123)]);
}

#[test]
fn fractional_timestamp_supported_by_akumuli() {
    check_success(
        "put foobar 789.4 -123.456 a=b",
        &[row("foobar", vec![tag("a", "b")], -123.456, 789)],
    );
    check_success(
        "put foo.bar 789 123.456 a=b\n",
        &[row("foo.bar", vec![tag("a", "b")], 123.456, 789)],
    );
}

#[test]
fn tags() {
    check_success(
        "put foo 2 1 bar=baz",
        &[row("foo", vec![tag("bar", "baz")], 1.0, 2)],
    );
    check_success(
        "put foo 2 1 bar=baz x=y",
        &[row("foo", vec![tag("bar", "baz"), tag("x", "y")], 1.0, 2)],
    );
    check_success(
        "put foo 2 1 bar=baz=aaa x=y",
        &[row(
            "foo",
            vec![tag("bar", "baz=aaa"), tag("x", "y")],
            1.0,
            2,
        )],
    );
}

#[test]
fn multi_lines() {
    check_success(
        "put foo 2 0.3 a=b\nput bar.baz 43 0.34 a=b\n",
        &[
            row("foo", vec![tag("a", "b")], 0.3, 2),
            row("bar.baz", vec![tag("a", "b")], 0.34, 43),
        ],
    );
}

#[test]
fn multi_lines_with_invalid_line() {
    check_success(
        "put foo 2 0.3 a=b\naaa bbb\nput bar.baz 43 0.34 a=b\n",
        &[
            row("foo", vec![tag("a", "b")], 0.3, 2),
            row("bar.baz", vec![tag("a", "b")], 0.34, 43),
        ],
    );
}

#[test]
fn multi_spaces() {
    check_success(
        "put  foobar 789 -123.456 a=b",
        &[row("foobar", vec![tag("a", "b")], -123.456, 789)],
    );
    check_success(
        "put foobar  789 -123.456 a=b",
        &[row("foobar", vec![tag("a", "b")], -123.456, 789)],
    );
    check_success(
        "put foobar 789  -123.456 a=b",
        &[row("foobar", vec![tag("a", "b")], -123.456, 789)],
    );
    check_success(
        "put foobar 789 -123.456  a=b",
        &[row("foobar", vec![tag("a", "b")], -123.456, 789)],
    );
    check_success(
        "put foobar 789 -123.456 a=b  c=d",
        &[row(
            "foobar",
            vec![tag("a", "b"), tag("c", "d")],
            -123.456,
            789,
        )],
    );
}

#[test]
fn space_after_tags() {
    check_success(
        "put foobar 789 -123.456 a=b ",
        &[row("foobar", vec![tag("a", "b")], -123.456, 789)],
    );
}
