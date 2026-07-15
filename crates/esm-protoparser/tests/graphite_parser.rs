//! Integration tests for `esm_protoparser::graphite`, ported from upstream
//! VictoriaMetrics v1.146.0 `lib/protoparser/graphite/parser_test.go`
//! (`TestRowsUnmarshal_Success` / `TestRowsUnmarshal_Failure`). These only
//! exercise the public API (`Rows::unmarshal`, `Row`, `Tag`), so they live
//! here rather than in `src/graphite.rs` to keep that file under the
//! 800-line guideline.
//!
//! `TestUnmarshalMetricAndTags_Success`/`_Failure` are ported as unit tests
//! inside `src/graphite.rs` itself instead (they exercise `Row` directly,
//! not `Rows`). `TestRowsUnmarshal_SanitizeMetricNamesSuccess` is not ported:
//! `-graphite.sanitizeMetricName` is out of scope for this port (see the
//! "Deviations from the Go original" note in `src/graphite.rs`).

use esm_protoparser::graphite::{Row, Rows, Tag};

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

/// Asserts `s` produces zero rows AND that at least one error was logged.
fn check_invalid(s: &str) {
    let mut rows = Rows::default();
    let mut errs = 0usize;
    rows.unmarshal(s, |_| errs += 1);
    assert!(rows.rows().is_empty(), "expected zero rows for {s:?}");
    assert!(errs > 0, "expected an error to be logged for {s:?}");

    // Try again.
    let mut errs2 = 0usize;
    rows.unmarshal(s, |_| errs2 += 1);
    assert!(rows.rows().is_empty());
    assert!(errs2 > 0);
}

// ---------------------------------------------------------------------------
// TestRowsUnmarshal_Failure
// ---------------------------------------------------------------------------

#[test]
fn failure_missing_value() {
    check_invalid("aaa");
}

#[test]
fn failure_invalid_value() {
    check_invalid("aa bb");
}

#[test]
fn failure_invalid_timestamp() {
    check_invalid("aa 123 bar");
}

// ---------------------------------------------------------------------------
// TestRowsUnmarshal_Success
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
    check_success(" 123 455", &[row("123", vec![], 455.0, 0)]);
    check_success(
        "foobar -123.456 789",
        &[row("foobar", vec![], -123.456, 789)],
    );
    check_success(
        "foo.bar 123.456 789\n",
        &[row("foo.bar", vec![], 123.456, 789)],
    );
}

#[test]
fn whitespace_in_metric_tag_name_and_value() {
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3102
    check_success(
        "s a;ta g1=aaa1;tag2=bb b2;tag3 1 23",
        &[row(
            "s a",
            vec![tag("ta g1", "aaa1"), tag("tag2", "bb b2")],
            1.0,
            23,
        )],
    );
}

#[test]
fn missing_timestamp() {
    check_success("aaa 1123", &[row("aaa", vec![], 1123.0, 0)]);
    check_success("aaa 1123 -1", &[row("aaa", vec![], 1123.0, -1)]);
}

#[test]
fn timestamp_bigger_than_2_31() {
    check_success(
        "aaa 1123 429496729600",
        &[row("aaa", vec![], 1123.0, 429496729600)],
    );
}

#[test]
fn floating_point_timestamp() {
    // See https://github.com/graphite-project/carbon/blob/b0ba62a62d40a37950fed47a8f6ae6d0f02e6af5/lib/carbon/protocols.py#L197
    check_success("aaa 1123 4294.943", &[row("aaa", vec![], 1123.0, 4294)]);
}

#[test]
fn tags() {
    check_success(
        "foo;bar=baz 1 2",
        &[row("foo", vec![tag("bar", "baz")], 1.0, 2)],
    );
}

#[test]
fn empty_tags() {
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1100
    check_success("foo; 1", &[row("foo", vec![], 1.0, 0)]);
    check_success("foo; 1 2", &[row("foo", vec![], 1.0, 2)]);
}

#[test]
fn empty_tag_name_or_value() {
    check_success("foo;bar 1 2", &[row("foo", vec![], 1.0, 2)]);
    check_success(
        "foo;bar=baz;aa=;x=y;=z 1 2",
        &[row("foo", vec![tag("bar", "baz"), tag("x", "y")], 1.0, 2)],
    );
}

#[test]
fn multi_lines() {
    check_success(
        "foo 0.3 2\naaa 3\nbar.baz 0.34 43\n",
        &[
            row("foo", vec![], 0.3, 2),
            row("aaa", vec![], 3.0, 0),
            row("bar.baz", vec![], 0.34, 43),
        ],
    );
}

#[test]
fn multi_lines_with_invalid_line() {
    check_success(
        "foo 0.3 2\naaa\nbar.baz 0.34 43\n",
        &[row("foo", vec![], 0.3, 2), row("bar.baz", vec![], 0.34, 43)],
    );
}

#[test]
fn tab_as_separator() {
    // See https://github.com/grobian/carbon-c-relay/commit/f3ffe6cc2b52b07d14acbda649ad3fd6babdd528
    check_success(
        "foo.baz\t125.456\t1789\n",
        &[row("foo.baz", vec![], 125.456, 1789)],
    );
}

#[test]
fn tab_as_separator_with_tags() {
    check_success(
        "foo;baz=bar;bb=;y=x;=z\t1\t2",
        &[row("foo", vec![tag("baz", "bar"), tag("y", "x")], 1.0, 2)],
    );
}

#[test]
fn whitespace_after_timestamp() {
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1865
    check_success(
        "foo.baz 125 1789 \na 1.34 567\t  ",
        &[
            row("foo.baz", vec![], 125.0, 1789),
            row("a", vec![], 1.34, 567),
        ],
    );
}

#[test]
fn multiple_whitespaces_as_separators() {
    check_success(
        "foo.baz \t125  1789 \t\n",
        &[row("foo.baz", vec![], 125.0, 1789)],
    );
}
