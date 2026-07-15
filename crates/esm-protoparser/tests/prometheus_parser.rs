//! Integration tests for `esm_protoparser::prometheus`, ported from upstream
//! VictoriaMetrics v1.146.0 `lib/protoparser/prometheus/parser_test.go`
//! (`TestRowsUnmarshalSuccess` / `TestRowsUnmarshalFailure`). These only
//! exercise the public API (`Rows::unmarshal`, `Row`, `Tag`), so they live
//! here rather than in `src/prometheus.rs` to keep that file under the
//! 800-line guideline.
//!
//! Metadata-line parsing (`# HELP` / `# TYPE` -> `MetadataRows`) is out of
//! scope for this port, so `TestParseMetadataLine*` and
//! `TestUnmarshalWithMetadata` are not ported. `TestGetRowsDiff` and
//! `TestAreIdenticalSeriesFast` cover Go-only helper functions that aren't
//! part of this port's public surface, so they're not ported either.

use std::borrow::Cow;

use esm_protoparser::prometheus::{Row, Rows, Tag};

fn tag<'a>(key: &'a str, value: &'a str) -> Tag<'a> {
    Tag {
        key,
        value: Cow::Borrowed(value),
    }
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
/// buffer reuse) and a `reset()` behave correctly too - mirrors the Go
/// helper's re-parse + Reset checks.
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
/// and logged, or via blank/comment lines that are silently skipped).
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

/// Asserts `s` produces zero rows AND that at least one error was logged
/// (i.e. the line was genuinely invalid, not just blank/comment).
fn check_invalid(s: &str) {
    let mut rows = Rows::default();
    let mut errs = 0usize;
    rows.unmarshal(s, |_| errs += 1);
    assert!(rows.rows().is_empty(), "expected zero rows for {s:?}");
    assert!(errs > 0, "expected an error to be logged for {s:?}");
}

// ---------------------------------------------------------------------------
// TestRowsUnmarshalSuccess
// ---------------------------------------------------------------------------

#[test]
fn empty_line_or_comment() {
    check_success("", &[]);
    check_success("\r", &[]);
    check_success("\n\n", &[]);
    check_success("\n\r\n", &[]);
    check_success("\t  \t\n\r\n#foobar\n  # baz", &[]);
}

#[test]
fn single_line() {
    check_success("foobar 78.9", &[row("foobar", vec![], 78.9, 0)]);
    check_success(
        "foobar 123.456 789\n",
        &[row("foobar", vec![], 123.456, 789000)],
    );
    check_success(
        "foobar{} 123.456 789.4354\n",
        &[row("foobar", vec![], 123.456, 789435)],
    );
}

#[test]
fn comment_block_then_metric() {
    check_success(
        "#                                    _                                            _\n\
         #   ___ __ _ ___ ___  __ _ _ __   __| |_ __ __ _        _____  ___ __   ___  _ __| |_ ___ _ __\n\
         # TYPE cassandra_token_ownership_ratio gauge\n\
         cassandra_token_ownership_ratio 78.9",
        &[row("cassandra_token_ownership_ratio", vec![], 78.9, 0)],
    );
}

#[test]
fn hash_char_in_label_value() {
    check_success(
        r##"foo{bar="#1 az"} 24"##,
        &[row("foo", vec![tag("bar", "#1 az")], 24.0, 0)],
    );
}

#[test]
fn hash_char_in_label_name_and_value() {
    check_success(
        r##"foo{bar#2="#1 az"} 24 456"##,
        &[row("foo", vec![tag("bar#2", "#1 az")], 24.0, 456000)],
    );
}

#[test]
fn hash_char_in_metric_label_name_and_value() {
    check_success(
        r##"foo#qw{bar#2="#1 az"} 24 456 # foobar {baz="x"}"##,
        &[row("foo#qw", vec![tag("bar#2", "#1 az")], 24.0, 456000)],
    );
}

#[test]
fn incorrectly_escaped_backslash_real_world_case() {
    check_success(
        r#"mssql_sql_server_active_transactions_sec{loginname="domain\somelogin",env="develop"} 56"#,
        &[row(
            "mssql_sql_server_active_transactions_sec",
            vec![tag("loginname", "domain\\somelogin"), tag("env", "develop")],
            56.0,
            0,
        )],
    );
}

#[test]
fn exemplars_are_dropped_as_trailing_comments() {
    // See https://github.com/OpenObservability/OpenMetrics/blob/master/OpenMetrics.md#exemplars-1
    check_success(
        "foo_bucket{le=\"10\",a=\"#b\"} 17 # {trace_id=\"oHg5SJ#YRHA0\"} 9.8 1520879607.789\n\
              abc 123 456 # foobar\n\
              foo   344#bar",
        &[
            row("foo_bucket", vec![tag("le", "10"), tag("a", "#b")], 17.0, 0),
            row("abc", vec![], 123.0, 456000),
            row("foo", vec![], 344.0, 0),
        ],
    );
}

#[test]
fn infinity_word_openmetrics() {
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/924
    let inf = f64::INFINITY;
    check_success(
        "\n\
         \tfoo Infinity\n\
         \tbar +Infinity\n\
         \tbaz -infinity\n\
         \taaa +inf\n\
         \tbbb -INF\n\
         \tccc INF\n\
        ",
        &[
            row("foo", vec![], inf, 0),
            row("bar", vec![], inf, 0),
            row("baz", vec![], -inf, 0),
            row("aaa", vec![], inf, 0),
            row("bbb", vec![], -inf, 0),
            row("ccc", vec![], inf, 0),
        ],
    );
}

#[test]
fn timestamp_bigger_than_2_31_parsed_as_millis() {
    check_success(
        "aaa 1123 429496729600",
        &[row("aaa", vec![], 1123.0, 429496729600)],
    );
}

#[test]
fn floating_point_timestamp_openmetrics() {
    check_success(
        "aaa 1123 42949.567",
        &[row("aaa", vec![], 1123.0, 42949567)],
    );
}

#[test]
fn tags_basic() {
    check_success(
        r#"foo{bar="baz"} 1 2"#,
        &[row("foo", vec![tag("bar", "baz")], 1.0, 2000)],
    );
}

#[test]
fn utf8_quoted_tags() {
    check_success(
        r#"foo{"bar"="baz"} 1 2"#,
        &[row("foo", vec![tag("bar", "baz")], 1.0, 2000)],
    );
    check_success(
        r#"{"foo", "bar"="baz"} 1 2"#,
        &[row("foo", vec![tag("bar", "baz")], 1.0, 2000)],
    );
    check_success(
        r#"{"foo", "bar"="baf\"y"} 1 2"#,
        &[row("foo", vec![tag("bar", "baf\"y")], 1.0, 2000)],
    );
    check_success(
        r#"{bar="baz", "foo"} 1 2"#,
        &[row("foo", vec![tag("bar", "baz")], 1.0, 2000)],
    );
    check_success(r#"{"foo"} 1 2"#, &[row("foo", vec![], 1.0, 2000)]);
}

#[test]
fn utf8_special_characters() {
    check_success(
        "{\"温度{房间\"} 1 2",
        &[row("温度{房间", vec![], 1.0, 2000)],
    );
    // Divergence from upstream: Go unescapes quoted tag *keys* the same as
    // values (`\"` -> `"`), producing key `温度{房间="水电费`. Since this
    // port's `Tag::key` is a borrowed `&str` (not `Cow`), a quoted key is
    // never unescaped - the raw (still-escaped) slice is kept instead. See
    // the divergence note on `unmarshal_tags` in src/prometheus.rs.
    check_success(
        "{\"foo\", \"温度{房间=\\\"水电费\"=\"baz\"} 1 2",
        &[row(
            "foo",
            vec![tag("温度{房间=\\\"水电费", "baz")],
            1.0,
            2000,
        )],
    );
}

#[test]
fn escaped_tag_value_quote_and_backslash() {
    check_success(
        r#"foo{bar="b\"a\\z"} -1.2"#,
        &[row("foo", vec![tag("bar", "b\"a\\z")], -1.2, 0)],
    );
}

#[test]
fn empty_tag_values_and_keys() {
    check_success(
        r#"foo {bar="baz",aa="",x="y",="z"} 1 2"#,
        &[row(
            "foo",
            vec![tag("bar", "baz"), tag("aa", ""), tag("x", "y")],
            1.0,
            2000,
        )],
    );
}

#[test]
fn trailing_comma_after_tag() {
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/350
    check_success(
        r#"foo{bar="baz",} 1 2"#,
        &[row("foo", vec![tag("bar", "baz")], 1.0, 2000)],
    );
}

#[test]
fn multiple_lines() {
    check_success(
        "# foo\n # bar ba zzz\nfoo 0.3 2\naaa 3\nbar.baz 0.34 43\n",
        &[
            row("foo", vec![], 0.3, 2000),
            row("aaa", vec![], 3.0, 0),
            row("bar.baz", vec![], 0.34, 43000),
        ],
    );
}

#[test]
fn multiple_lines_with_invalid_line() {
    check_success(
        "\t foo\t {  } 0.3\t 2\naaa\n  bar.baz 0.34 43\n",
        &[
            row("foo", vec![], 0.3, 2000),
            row("bar.baz", vec![], 0.34, 43000),
        ],
    );
}

#[test]
fn spaces_around_tags() {
    check_success(
        "vm_accounting\t{   name=\"vminsertRows\", accountID = \"1\" , projectID=\t\"1\"   } 277779100",
        &[row(
            "vm_accounting",
            vec![
                tag("name", "vminsertRows"),
                tag("accountID", "1"),
                tag("projectID", "1"),
            ],
            277779100.0,
            0,
        )],
    );
}

// ---------------------------------------------------------------------------
// TestRowsUnmarshalFailure
//
// Go's test only asserts `len(rows.Rows) == 0`, which blank/comment lines
// also satisfy without logging any error. Split accordingly: blank/comment
// cases go through `check_zero_rows`; genuinely malformed lines (which must
// also invoke `err_logger`) go through `check_invalid`.
// ---------------------------------------------------------------------------

#[test]
fn failure_empty_lines_and_comments() {
    check_zero_rows("");
    check_zero_rows(" ");
    check_zero_rows("\t");
    check_zero_rows("\t  \r");
    check_zero_rows("\t\t  \n\n  # foobar");
    check_zero_rows("#foobar");
    check_zero_rows("#foobar\n");
}

#[test]
fn failure_invalid_tags() {
    check_invalid("a{");
    check_invalid("a { ");
    check_invalid("a {foo");
    check_invalid("a {foo} 3");
    check_invalid("a {foo  =");
    check_invalid(r#"a {foo  ="bar"#);
    check_invalid(r#"a {foo  ="b\ar"#);
    check_invalid(r#"a {foo  = "bar""#);
    check_invalid(r#"a {foo  ="bar","#);
    check_invalid(r#"a {foo  ="bar" , "#);
    check_invalid(r#"a {foo  ="bar" , baz } 2"#);
}

#[test]
fn failure_invalid_utf8_tags() {
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/4284
    check_invalid(r#"a{"__name__":"upsd_time_left_ns","host":"myhost", status_OB="true"} 12"#);
    check_invalid(r#"a{host:"myhost"} 12"#);
    check_invalid(r#"a{host:"myhost",foo="bar"} 12"#);
    check_invalid(r#"metric_"name"{"foo"="bar"}"#);
    check_invalid(r#""metric_name"{"name":"name}"#);
    check_invalid(r#"metric_"name{"name":"name"}"#);
    check_invalid(r#"metric{"foo":"bar"}"#);
    check_invalid(r#"{"foo":"bar", "metric"}"#);
}

#[test]
fn failure_empty_metric_name() {
    check_invalid(r#"{foo="bar"}"#);
    check_invalid(r#"{"a"="ok"} 1"#);
}

#[test]
fn failure_invalid_quotes_for_label_value() {
    check_invalid("{foo='bar'} 23");
    check_invalid("{foo=`bar`} 23");
}

#[test]
fn failure_missing_value() {
    check_invalid("aaa");
    check_invalid(" aaa");
    check_invalid(" aaa ");
    check_invalid(" aaa   \n");
    check_invalid(" aa{foo=\"bar\"}   \n");
}

#[test]
fn failure_invalid_value() {
    check_invalid("foo bar");
    check_invalid("foo bar 124");
}

#[test]
fn failure_invalid_timestamp() {
    check_invalid("foo 123 bar");
}

#[test]
fn failure_duplicate_metric_name() {
    check_invalid(r#"{"foo", "foo2", bar="baz"} 1 2"#);
    check_invalid(r#"foobar{"foo", bar="baz"} 1 2"#);
}

#[test]
fn failure_missing_closing_quote_on_key() {
    check_invalid(r#"{"a", "b = "c"}"#);
}
