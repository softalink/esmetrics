//! Port of `parser_test.go` (TestParseSuccess and TestParseError).

use esm_metricsql::parse;

/// Parses `s` and asserts that the serialized AST equals `s_expected`.
fn another(s: &str, s_expected: &str) {
    let e = parse(s).unwrap_or_else(|err| panic!("unexpected error when parsing {s}: {err}"));
    let res = e.to_string();
    assert_eq!(
        res, s_expected,
        "unexpected string constructed when parsing {s:?}"
    );
}

/// Asserts that `s` parses and round-trips to itself.
fn same(s: &str) {
    another(s, s);
}

/// Asserts that parsing `s` fails.
fn f(s: &str) {
    let e = parse(s);
    assert!(e.is_err(), "expecting non-nil error when parsing {s:?}");
}

#[test]
fn parse_metric_expr() {
    same("{}");
    same("{}[5m]");
    same("{}[5m:]");
    same("{}[:]");
    another("{}[: ]", "{}[:]");
    same("{}[:3s]");
    another("{}[: 3s ]", "{}[:3s]");
    same("{}[5m:3s]");
    another("{}[ 5m : 3s ]", "{}[5m:3s]");
    same("{} offset 5m");
    same("{} offset -5m");
    same("{}[5m] offset 10y");
    same("{}[5.3m:3.4s] offset 10y");
    same("{}[:3.4s] offset 10y");
    same("{}[:3.4s] offset -10y");
    same(r#"{Foo="bAR"}"#);
    same(r#"{foo="bar"}"#);
    another(r#"{"foo"="bar"}"#, r#"{foo="bar"}"#);
    another(r#"{"foo"="bAR"}"#, r#"{foo="bAR"}"#);
    another(r#"{"3foo"="bar"}"#, r#"{\3foo="bar"}"#);
    another(r#"{"'3foo'"="bar"}"#, r#"{\'3foo\'="bar"}"#);
    another(
        r#"{'温度{房间="水电费\xF3"}'="1"}[5m] offset 10m"#,
        r#"{温度\{房间\=\"水电费ó\"\}="1"}[5m] offset 10m"#,
    );
    same(r#"{foo="bar"}[5m]"#);
    another(r#"{"foo"="bar"}[5m]"#, r#"{foo="bar"}[5m]"#);
    same(r#"{foo="bar"}[5m:]"#);
    another(r#"{"foo"="bar"}[5m:]"#, r#"{foo="bar"}[5m:]"#);
    same(r#"{foo="bar"}[5m:3s]"#);
    another(r#"{"foo"="bar"}[5m:3s]"#, r#"{foo="bar"}[5m:3s]"#);
    same(r#"{foo="bar"} offset 13.4ms"#);
    another(
        r#"{"foo"="bar"} offset 13.4ms"#,
        r#"{foo="bar"} offset 13.4ms"#,
    );
    same(r#"{foo="bar"}[5w4h-3.4m13.4ms]"#);
    another(
        r#"{"foo"="bar"}[5w4h-3.4m13.4ms]"#,
        r#"{foo="bar"}[5w4h-3.4m13.4ms]"#,
    );
    same(r#"{foo="bar"} offset 10y"#);
    another(r#"{"foo"="bar"} offset 10y"#, r#"{foo="bar"} offset 10y"#);
    same(r#"{foo="bar"} offset -10y"#);
    another(r#"{"foo"="bar"} offset -10y"#, r#"{foo="bar"} offset -10y"#);
    same(r#"{foo="bar"}[5m] offset 10y"#);
    another(
        r#"{"foo"="bar"}[5m] offset 10y"#,
        r#"{foo="bar"}[5m] offset 10y"#,
    );
    same(r#"{foo="bar"}[5m:3s] offset 10y"#);
    another(
        r#"{"foo"="bar"}[5m:3s] offset 10y"#,
        r#"{foo="bar"}[5m:3s] offset 10y"#,
    );
    another(
        r#"{foo="bar"}[5m] oFFSEt 10y"#,
        r#"{foo="bar"}[5m] offset 10y"#,
    );
    another(
        r#"{"foo"="bar"}[5m] oFFSEt 10y"#,
        r#"{foo="bar"}[5m] offset 10y"#,
    );
    another(r#"{__name__="metric", a="1"}"#, r#"metric{a="1"}"#);
    another(r#"{"metric", a="1"}"#, r#"metric{a="1"}"#);
    another(r#"{"metric", "a"="1"}"#, r#"metric{a="1"}"#);
    same("METRIC");
    same("metric");
    another(r#"metric{"metric"}"#, "metric");
    another(r#"metric{__name__="metric"}"#, "metric");
    another(r#"{"metric"}"#, "metric");
    another(r#"{a="1",__name__="metric"}"#, r#"metric{a="1"}"#);
    another("metric{}", "metric");
    same("m_e:tri44:_c123");
    another(r#"{"m_e:tri44:_c123"}"#, "m_e:tri44:_c123");
    another("-metric", "0 - metric");
    another(r#"-{"metric"}"#, "0 - metric");
    same("metric offset 10h");
    another(r#"{"metric"} offset 10h"#, "metric offset 10h");
    same("metric[5m]");
    another(r#"{"metric"}[5m]"#, "metric[5m]");
    same("metric[5m:3s]");
    another(r#"{"metric"}[5m:3s]"#, "metric[5m:3s]");
    same("metric[5m] offset 10h");
    another(r#"{"metric"}[5m] offset 10h"#, "metric[5m] offset 10h");
    same("metric[5m:3s] offset 10h");
    another(
        r#"{"metric"}[5m:3s] offset 10h"#,
        "metric[5m:3s] offset 10h",
    );
    same("metric[5i:3i] offset 10i");
    another(r#"{"metric"}[5i:3i]"#, "metric[5i:3i]");
    same(r#"metric{foo="bar"}"#);
    another(r#"{"metric",foo="bar"}"#, r#"metric{foo="bar"}"#);
    another(r#"{"metric","foo"="bar"}"#, r#"metric{foo="bar"}"#);
    same(r#"metric{foo="bar"} offset 10h"#);
    another(
        r#"{"metric",foo="bar"} offset 10h"#,
        r#"metric{foo="bar"} offset 10h"#,
    );
    another(
        r#"{"metric","foo"="bar"} offset 10h"#,
        r#"metric{foo="bar"} offset 10h"#,
    );
    same(r#"metric{foo!="bar"}[2d]"#);
    another(r#"{"metric",foo!="bar"}[2d]"#, r#"metric{foo!="bar"}[2d]"#);
    another(
        r#"{"metric","foo"!="bar"}[2d]"#,
        r#"metric{foo!="bar"}[2d]"#,
    );
    same(r#"metric{foo="bar"}[2d] offset 10h"#);
    same(r#"metric{foo="bar",b="sdfsdf"}[2d:3h] offset 10h"#);
    same(r#"metric{foo="bar",b="sdfsdf"}[2d:3h] offset 10"#);
    same(r#"metric{foo="bar",b="sdfsdf"}[2d:3] offset 10h"#);
    same(r#"metric{foo="bar",b="sdfsdf"}[2:3h] offset 10h"#);
    same(r#"metric{foo="bar",b="sdfsdf"}[2.34:5.6] offset 3600.5"#);
    same(r#"metric{foo="bar",b="sdfsdf"}[234:56] offset -3600"#);
    another(
        r#"  metric  {  foo  = "bar"  }  [  2d ]   offset   10h  "#,
        r#"metric{foo="bar"}[2d] offset 10h"#,
    );
    another(
        r#"  {  "metric",  foo  = "bar"  }  [  2d ]   offset   10h  "#,
        r#"metric{foo="bar"}[2d] offset 10h"#,
    );
}

#[test]
fn parse_metric_expr_with_or() {
    same(r#"metric{foo="bar" or baz="a"}"#);
    another(
        r#"metric{"foo"="bar" or "baz"="a"}"#,
        r#"metric{foo="bar" or baz="a"}"#,
    );
    another(
        r#"metric{"foo"="bar" or baz="a"}"#,
        r#"metric{foo="bar" or baz="a"}"#,
    );
    another(
        r#"metric{foo="bar" or "baz"="a"}"#,
        r#"metric{foo="bar" or baz="a"}"#,
    );
    another(
        r#"{"metric", foo="bar" or "baz"="a"}"#,
        r#"metric{foo="bar" or baz="a"}"#,
    );
    same(r#"metric{foo="bar",x="y" or baz="a",z="q" or a="b"}"#);
    another(
        r#"{"metric", "foo"="bar","x"="y" or "baz"="a","z"="q" or "a"="b"}"#,
        r#"metric{foo="bar",x="y" or baz="a",z="q" or a="b"}"#,
    );
    another(
        r#"{"metric", foo="bar","x"="y" or baz="a","z"="q" or a="b"}"#,
        r#"metric{foo="bar",x="y" or baz="a",z="q" or a="b"}"#,
    );
    same(r#"{foo="bar",x="y" or baz="a",z="q" or a="b"}"#);
    another(
        r#"{"foo"="bar","x"="y" or "baz"="a","z"="q" or "a"="b"}"#,
        r#"{foo="bar",x="y" or baz="a",z="q" or a="b"}"#,
    );
    another(
        r#"{"foo"="bar",x="y" or "baz"="a",z="q" or a="b"}"#,
        r#"{foo="bar",x="y" or baz="a",z="q" or a="b"}"#,
    );
    another(
        r#"metric{foo="bar" OR baz="a"}"#,
        r#"metric{foo="bar" or baz="a"}"#,
    );
    another(r#"{foo="bar" OR baz="a"}"#, r#"{foo="bar" or baz="a"}"#);

    another(
        r#"{__name__="a",bar="baz" or __name__="a"}"#,
        r#"a{bar="baz"}"#,
    );
    another(
        r#"{__name__="a",bar="baz" or __name__="a" or __name__="a"}"#,
        r#"a{bar="baz"}"#,
    );
    another(
        r#"{__name__="a",bar="baz" or __name__="a",bar="abc"}"#,
        r#"a{bar="baz" or bar="abc"}"#,
    );
    another(
        r#"{__name__="a" or __name__="a",bar="abc",x!="y"}"#,
        r#"a{bar="abc",x!="y"}"#,
    );
    another(
        r#"{__name__="metric",a="1" or __name__="metric",b="2"}"#,
        r#"metric{a="1" or b="2"}"#,
    );
    another(
        r#"{"foo",a="1" or "bar",b="2"}"#,
        r#"{__name__="foo",a="1" or __name__="bar",b="2"}"#,
    );
    same(r#"{__name__="foo",a="1" or __name__="bar",b="2"}"#);
    same(r#"{__name__="foo",a="1" or __name__=~"bar",b="2"}"#);
}

#[test]
fn parse_at_modifier() {
    // See https://prometheus.io/docs/prometheus/latest/querying/basics/#modifier
    same("foo @ 123.45");
    same(r"foo\@ @ 123.45");
    another(r#"{foo=~"bar"} @ end()"#, r#"{foo=~"bar"} @ end()"#);
    same(r#"foo{bar="baz"} @ start()"#);
    same(r#"foo{bar="baz"}[5m] @ 12345"#);
    same(r#"foo{bar="baz"}[5m:4s] offset 5m @ (end() - 3.5m)"#);
    another(
        r#"foo{bar="baz"}[5m:4s] @ (end() - 3.5m) offset 2.4h"#,
        r#"foo{bar="baz"}[5m:4s] offset 2.4h @ (end() - 3.5m)"#,
    );
    another(
        "foo @ start() + (bar offset 3m @ end()) / baz OFFSET -5m",
        "(foo @ start()) + ((bar offset 3m @ end()) / (baz offset -5m))",
    );
    another(
        "sum(foo) @ start() + rate(bar @ (end() - 5m))",
        "(sum(foo) @ start()) + rate(bar @ (end() - 5m))",
    );
    another("time() @ (start())", "time() @ start()");
    another("time() @ (start()+(1+1))", "time() @ (start() + 2)");
    same("time() @ (end() - 10m)");
    another("a + b offset 5m @ 1235", "a + (b offset 5m @ 1235)");
    another("a + b @ 1235 offset 5m", "a + (b offset 5m @ 1235)");
}

#[test]
fn parse_metric_names_matching_keywords() {
    same("rate");
    same("RATE");
    same("by");
    same("BY");
    same("bool");
    same("BOOL");
    same("unless");
    same("UNLESS");
    same("Ignoring");
    same("with");
    same("WITH");
    same("With");
    same("with / by");
    same("offset");
    same("keep_metric_names");
    same("alias");
    same(r#"alias{foo="bar"}"#);
    same(r#"aLIas{alias="aa"}"#);
    same("or");
    another(r"al\ias", "alias");
}

#[test]
fn parse_idents_with_escape_chars() {
    same(r"foo\ bar");
    same(r#"foo\-bar\{{baz\+bar="aa"}"#);
    another(r#"\x2E\x2ef\oo{b\xEF\ar="aa"}"#, r#"\..foo{bïar="aa"}"#);
    same(r#"温度{房间="水电费"}[5m] offset 10m"#);
    another(
        r#"\温\度{\房\间="水电费"}[5m] offset 10m"#,
        r#"温度{房间="水电费"}[5m] offset 10m"#,
    );
    same(r"sum(fo\|o) by(b\|a,x)");
    another(r"sum(x) by (b\x7Ca)", r"sum(x) by(b\|a)");
}

#[test]
fn parse_duplicate_and_misc_filters() {
    // Duplicate filters.
    same(r#"foo{a="b",a="c",b="d"}"#);
    same(r#"{a="b",a="c",b="d"}"#);

    // Metric filters ending with comma.
    another(r#"m{foo="bar",}"#, r#"m{foo="bar"}"#);

    // String concat in tag value.
    another(r#"m{foo="bar" + "baz"}"#, r#"m{foo="barbaz"}"#);

    // Valid regexps.
    same(r#"foo{bar=~"x"}"#);
    same(r#"foo{bar=~"^x"}"#);
    same(r#"foo{bar=~"^x$"}"#);
    same(r#"foo{bar=~"^(a[bc]|d)$"}"#);
    same(r#"foo{bar!~"x"}"#);
    same(r#"foo{bar!~"^x"}"#);
    same(r#"foo{bar!~"^x$"}"#);
    same(r#"foo{bar!~"^(a[bc]|d)$"}"#);
}

#[test]
fn parse_string_expr() {
    same(r#""""#);
    same(r#""\n\t\r 12:{}[]()44""#);
    another("''", r#""""#);
    another("``", r#""""#);
    another("   `foo\"b'ar`  ", r#""foo\"b'ar""#);
    another(r#"  'foo\'bar"BAZ'  "#, r#""foo'bar\"BAZ""#);

    // String concat.
    another(r#""foo"+'bar'"#, r#""foobar""#);
    another(r#"("foo" + "bar")"#, r#""foobar""#);
    another(r#"(("foo")+"bar")+"baz""#, r#""foobarbaz""#);
}

#[test]
fn parse_number_expr() {
    same("1");
    same("123.");
    same("1_234");
    same("1_2_34.56_78_9");
    another("-123.", "-123");
    same("foo - 123.");
    same("12.e+4");
    same("12Ki");
    same("12Kib");
    same("12Mi");
    same("12Mb");
    same("12MB");
    same("(rate(foo)[5m] * 8) > 45Mi");
    same("(rate(foo)[5m] * 8) > 45mi");
    same("(rate(foo)[5m] * 8) > 45mI");
    same("(rate(foo)[5m] * 8) > 45Mib");
    same("1.23Gb");
    same("foo - 23M");
    another("-1.23Gb", "-1.23e+09");
    same("1.23");
    same("0.23");
    same("1.2e+45");
    same("1.2e-45");
    same("-1");
    same("-1.23");
    same("-0.23");
    same("-1.2e+45");
    same("-1.2e-45");
    same("-1.2e-45");
    same("12.5E34");
    another("-.2", "-0.2");
    another("-.2E-2", "-0.002");
    same("NaN");
    same("nan");
    same("NAN");
    same("nAN");
    same("Inf");
    same("INF");
    same("inf");
    another("+Inf", "Inf");
    same("-Inf");
    another("-inF", "-Inf");
    same("0x12");
    same("0x3b");
    another("-0x3b", "-59");
    another("+0X3B", "0X3B");
    same("0b1011");
    same("073");
    another("-0o12", "-10");
}

#[test]
fn parse_duration_expr() {
    same("1h");
    another("-1h", "0 - 1h");
    same("0.34h4m5s");
    same("0.34H4m5S");
    another("-0.34h4m5s", "0 - 0.34h4m5s");
    same("sum_over_time(m[1h]) / 1h");
    same("sum_over_time(m[3600]) / 3600");
}

#[test]
fn parse_binary_op_expr() {
    another("nan == nan", "NaN");
    another("nan ==bool nan", "1");
    another("nan !=bool nan", "0");
    another("nan !=bool 2", "1");
    another("2 !=bool nan", "1");
    another("nan >bool nan", "0");
    another("nan <bool nan", "0");
    another("1 ==bool nan", "0");
    another("NaN !=bool 1", "1");
    another("inf >=bool 2", "1");
    another("-1 >bool -inf", "1");
    another("-1 <bool -inf", "0");
    another("nan + 2 *3 * inf", "NaN");
    another("INF - Inf", "NaN");
    another("Inf + inf", "+Inf");
    another("1/0", "+Inf");
    another("0/0", "NaN");
    another("-m", "0 - m");
    same("m + ignoring() n[5m]");
    another("M + IGNORING () N[5m]", "M + ignoring() N[5m]");
    same("m + on(foo) n[5m]");
    another("m + ON (Foo) n[5m]", "m + on(Foo) n[5m]");
    same("m + ignoring(a,b) n[5m]");
    another("1 or 2", "1");
    another("1 or NaN", "1");
    another("NaN or 1", "1");
    another("(1 > 0) or 2", "1");
    another("(1 < 0) or 2", "2");
    another("(1 < 0) or (2 < 0)", "NaN");
    another("NaN or NaN", "NaN");
    another("1 and 2", "1");
    another("1 and (1 > 0)", "1");
    another("1 and (1 < 0)", "NaN");
    another("1 and NaN", "NaN");
    another("1 unless 2", "NaN");
    another("1 default 2", "1");
    another("1 default NaN", "1");
    another("NaN default 2", "2");
    another("1 > 2", "NaN");
    another("1 > bool 2", "0");
    another("3 >= 2", "3");
    another("3 <= bool 2", "0");
    another("1 + -2 - 3", "-4");
    another("1 / 0 + 2", "+Inf");
    another("2 + -1 / 0", "-Inf");
    another("(-1) ^ 0.5", "NaN");
    another("-1 ^ 0.5", "-1");
    another("512.5 - (1 + 3) * (2 ^ 2) ^ 3", "256.5");
    another("1 == bool 1 != bool 24 < bool 4 > bool -1", "1");
    another("1 == bOOl 1 != BOOL 24 < Bool 4 > booL -1", "1");
    another("m1+on(foo)group_left m2", "m1 + on(foo) group_left() m2");
    another("M1+ON(FOO)GROUP_left M2", "M1 + on(FOO) group_left() M2");
    same("m1 + on(foo) group_right() m2");
    same("m1 + on(foo,bar) group_right(x,y) m2");
    another(
        "m1 + on (foo, bar,) group_right (x, y,) m2",
        "m1 + on(foo,bar) group_right(x,y) m2",
    );
    same("m1 ==bool on(foo,bar) group_right(x,y) m2");
    same("a + on() group_left(*) b");
    same("a + on() group_right(*) b");
    same(r#"a + on() group_left(*) prefix "foo" b"#);
    another("a + group_left", "a + (group_left)");
    another("a + group_left / b", "a + (group_left / b)");
    same("a + on(x) (group_left)");
    same("a + on(x) group_left() (prefix)");
    another(
        r#"a + oN() gROUp_rigHt(*) PREfix "bar" b"#,
        r#"a + on() group_right(*) prefix "bar" b"#,
    );
    same(r#"a + on(a) group_left(x,y) prefix "foo" b"#);
    same(r#"a + on(a,b) group_right(z) prefix "bar" b"#);
    another(
        r#"5 - 1 + 3 * 2 ^ 2 ^ 3 - 2  OR Metric {Bar= "Baz", aaa!="bb",cc=~"dd" ,zz !~"ff" } "#,
        r#"770 or Metric{Bar="Baz",aaa!="bb",cc=~"dd",zz!~"ff"}"#,
    );

    same(r#""foo" + bar{x="y"}"#);
    same(r#"("foo"[3s] + bar{x="y"})[5m:3s] offset 10s"#);
    same(r#"("foo"[3s] + bar{x="y"})[5i:3i] offset 10i"#);
    another(r#"bar + "foo" offset 3s"#, r#"bar + ("foo" offset 3s)"#);
    another(r#"bar + "foo" offset 3i"#, r#"bar + ("foo" offset 3i)"#);
    another("1+2 if 2>3", "NaN");
    another("1+4 if 2<3", "5");
    another("2+6 default 3 if 2>3", "8");
    another("2+6 if 2>3 default NaN", "NaN");
    another("42 if 3>2 if 2+2<5", "42");
    another("42 if 3>2 if 2+2>=5", "NaN");
    another("1+2 ifnot 2>3", "3");
    another("1+4 ifnot 2<3", "NaN");
    another("2+6 default 3 ifnot 2>3", "8");
    another("2+6 ifnot 2>3 default NaN", "8");
    another("42 if 3>2 ifnot 2+2<5", "NaN");
    another("42 if 3>2 ifnot 2+2>=5", "42");
    another(r#""foo" + "bar""#, r#""foobar""#);
    another(r#""foo"=="bar""#, "NaN");
    another(r#""foo"=="foo""#, "1");
    another(r#""foo"!="bar""#, "1");
    another(r#""foo"+"bar"+"baz""#, r#""foobarbaz""#);
    another(r#""a">"b""#, "NaN");
    another(r#""a">bool"b""#, "0");
    another(r#""a"<"b""#, "1");
    another(r#""a">="b""#, "NaN");
    another(r#""a">=bool"b""#, "0");
    another(r#""a"<="b""#, "1");
    same(r#""a" - "b""#);
    another("x / a keep_metric_names", "(x / a) keep_metric_names");
    same("(a + b) keep_metric_names");
    another("((a) + (b)) keep_metric_names", "(a + b) keep_metric_names");
    another(
        "a + on(x) group_left(y) b offset 5m @ 1235 keep_metric_names",
        "(a + on(x) group_left(y) (b offset 5m @ 1235)) keep_metric_names",
    );
    another(
        "(a + on(x) group_left(y) b offset 5m keep_metric_names) @ 1235",
        "((a + on(x) group_left(y) (b offset 5m)) keep_metric_names) @ 1235",
    );
    another(
        "(a + on(x) group_left(y) b keep_metric_names) offset 5m @ 1235",
        "((a + on(x) group_left(y) b) keep_metric_names) offset 5m @ 1235",
    );
    another(
        "(a + on (x) group_left (y) b keep_metric_names) @ 1235 offset 5m",
        "((a + on(x) group_left(y) b) keep_metric_names) offset 5m @ 1235",
    );
    another(
        "rate(x) keep_metric_names + (abs(y) keep_metric_names) keep_metric_names",
        "(rate(x) keep_metric_names + (abs(y) keep_metric_names)) keep_metric_names",
    );
    same("a + (rate(b) keep_metric_names)");

    // Binary ops with reserved names.
    same("a + (on)");
    same("a + (on + c)");
    same("a + (GROUP_LEFT)");
    same("a + (bool)");
    another("a + (sum(1, 2))", "a + sum(1, 2)");
    same(r#"without + (ignoring{x="y"})"#);
    another("a + (GROUP_LEFT) / b", "a + (GROUP_LEFT / b)");
    same("by + without");
    same("group_left / (on)");
    another("group_left / (sum(1, 2))", "group_left / sum(1, 2)");
}

#[test]
fn parse_parens_expr() {
    another(
        "(-foo + ((bar) / (baz))) + ((23))",
        "((0 - foo) + (bar / baz)) + 23",
    );
    another(
        "(FOO + ((Bar) / (baZ))) + ((23))",
        "(FOO + (Bar / baZ)) + 23",
    );
    same("(foo, bar)");
    another("((foo, bar),(baz))", "((foo, bar), baz)");
    same("(foo, (bar, baz), ((x, y), (z, y), xx))");
    another("1+(foo, bar,)", "1 + (foo, bar)");
    another(
        "((avg(bar,baz)), (1+(2)+(3,4)+()))",
        "(avg(bar, baz), (3 + (3, 4)) + ())",
    );
    same("()");
}

#[test]
fn parse_func_expr() {
    same("sum()");
    another("sum(x,)", "sum(x)");
    another("-sum()-AVG_over_time()", "(0 - sum()) - AVG_over_time()");
    another("SUM()", "sum()");
    another("+SUM()", "sum()");
    another("++SUM()", "sum()");
    another("--SUM()", "0 - (0 - sum())");
    same("rate(http_server_request)");
    same("rate(http_server_request)[4s:5m] offset 10m");
    same("rate(http_server_request)[4i:5i] offset 10i");
    another("SUM(HttpServerRequest)", "sum(HttpServerRequest)");
    same("outliersk(job, foo)");
    same("outliersk(Job, Foo)");

    another(
        r#" SUM (bar) + rate  (  avg  (  ),sum(1 + (  2.5)) ,M[5m ]  , "ff"  )"#,
        r#"sum(bar) + rate(avg(), sum(3.5), M[5m], "ff")"#,
    );
    same("rate(foo[5m]) keep_metric_names");
    another(
        "log2(foo) KEEP_metric_names + 1 / increase(bar[5m]) keep_metric_names offset 1h @ 435",
        "log2(foo) keep_metric_names + (1 / (increase(bar[5m]) keep_metric_names offset 1h @ 435))",
    );

    // Embedded funcNames.
    same("rate(rate(m))");
    same("rate(rate(m[5m]))");
    same("rate(rate(m[5m])[1h:])");
    same("rate(rate(m[5m])[1h:3s])");

    // funcName with escape chars.
    another(r"r\a\te(m[5m])", "rate(m[5m])");
}

#[test]
fn parse_aggr_func_expr() {
    same("sum(http_server_request) by()");
    same("sum(http_server_request) by(job)");
    same("sum(http_server_request) without(job,foo)");
    another("sum(x,y,) without (a,b,)", "sum(x, y) without(a,b)");
    another("sum by () (xx)", "sum(xx) by()");
    another("sum by (s) (xx)[5s]", "(sum(xx) by(s))[5s]");
    another("SUM BY (ZZ, aa) (XX)", "sum(XX) by(ZZ,aa)");
    another("sum without (a, b) (xx,2+2)", "sum(xx, 4) without(a,b)");
    another("Sum WIthout (a, B) (XX,2+2)", "sum(XX, 4) without(a,B)");
    same("sum(a) or sum(b)");
    same("sum(a) by() or sum(b) without(x,y)");
    same("sum(a) + sum(b)");
    same("sum(x) * (1 + sum(a))");
    same("avg(x) limit 10");
    same("avg(x) without(z,b) limit 1");
    another("avg by(x) (z) limit 20", "avg(z) by(x) limit 20");
    // UTF-8 quoted label names.
    another(
        r#"sum({"metric name","label"="value"}) by ("cluster!one",instance)"#,
        r#"sum(metric\ name{label="value"}) by(cluster\!one,instance)"#,
    );
}

#[test]
fn parse_all_the_above() {
    another(
        r#"Sum(timestamp(M) * M{X=""}[5m] Offset 7m - 123, 35) BY (X, y) * LAG("Test")"#,
        r#"sum((timestamp(M) * (M{X=""}[5m] offset 7m)) - 123, 35) by(X,y) * LAG("Test")"#,
    );
    another(
        "# comment\n\t\tSum(Timestamp(M) * M{X=\"\"}[5m] Offset 7m - 123, 35) BY (X, y) # yet another comment\n\t\t* LAG(\"Test\")",
        r#"sum((Timestamp(M) * (M{X=""}[5m] offset 7m)) - 123, 35) by(X,y) * LAG("Test")"#,
    );
}

#[test]
fn parse_with_expr() {
    another("with () x", "x");
    another("with (x=1,) x", "1");
    another(
        "with (x = m offset 5h) x + x",
        "(m offset 5h) + (m offset 5h)",
    );
    another(
        "with (x = m offset 5i) x + x",
        "(m offset 5i) + (m offset 5i)",
    );
    another(r#"with (foo = bar{x="x"}) 1"#, "1");
    another(r#"with (foo = bar{x="x"}) "x""#, r#""x""#);
    another(r#"with (f="x") f"#, r#""x""#);
    another(r#"with (foo = bar{x="x"}) x{x="y"}"#, r#"x{x="y"}"#);
    another(r#"with (foo = bar{x="x"}) 1+1"#, "2");
    another(r#"with (foo = bar{x="x"}) time()"#, "time()");
    another(r#"with (foo = bar{x="x"}) sum(x)"#, "sum(x)");
    another(
        r#"with (foo = bar{x="x"}) baz{foo="bar"}"#,
        r#"baz{foo="bar"}"#,
    );
    another("with (foo = bar) baz", "baz");
    another(
        r#"with (foo = bar) foo + foo{a="b"}"#,
        r#"bar + bar{a="b"}"#,
    );
    another("with (foo = bar, bar=baz + f()) test", "test");
    another(
        r#"with (ct={job="test"}) a{ct} + ct() + ceil({ct="x"})"#,
        r#"(a{job="test"} + {job="test"}) + ceil({ct="x"})"#,
    );
    another(
        r#"with (ct={job="test", i="bar"}) ct + {ct, x="d"} + foo{ct, ct} + count(1)"#,
        r#"(({job="test",i="bar"} + {job="test",i="bar",x="d"}) + foo{job="test",i="bar"}) + count(1)"#,
    );
    another(
        r#"with (foo = bar) {__name__=~"foo"}"#,
        r#"{__name__=~"foo"}"#,
    );
    another(r#"with (foo = bar) foo{__name__="foo"}"#, "bar");
    another(
        r#"with (foo = bar) {__name__="foo", x="y"}"#,
        r#"bar{x="y"}"#,
    );
    another(
        r#"with (foo(bar) = {__name__!="bar"}) foo(x)"#,
        r#"{__name__!="bar"}"#,
    );
    another(r#"with (foo(bar) = bar{__name__="bar"}) foo(x)"#, "x");
    another(
        r"with (foo\-bar(baz) = baz + baz) foo\-bar((x,y))",
        "(x, y) + (x, y)",
    );
    another(
        r"with (foo\-bar(baz) = baz + baz) foo\-bar(x*y)",
        "(x * y) + (x * y)",
    );
    another(
        r"with (foo\-bar(baz) = baz + baz) foo\-bar(x\*y)",
        r"x\*y + x\*y",
    );
    another(
        r"with (foo\-bar(b\ az) = b\ az + b\ az) foo\-bar(x\*y)",
        r"x\*y + x\*y",
    );
}

#[test]
fn parse_with_expr_durations() {
    another("with (w=5m) w + m[w] offset w", "5m + (m[5m] offset 5m)");
    another(
        r#"with (f() = 5m + rate(m{x="a"}[5m:1h] offset 1h)) f()"#,
        r#"5m + rate(m{x="a"}[5m:1h] offset 1h)"#,
    );
    another(
        r#"with (f(w1, w2) = w1 + rate(m{x="a"}[w1:w2] offset w2)) f(5m, 1h)"#,
        r#"5m + rate(m{x="a"}[5m:1h] offset 1h)"#,
    );
    another("with (f(w) = m[w], f2(x) = f(x) / x) f2(5m)", "m[5m] / 5m");
    another(
        "with (f(w) = m[w:w], f2(x) = f(x) / x) f2(5i)",
        "m[5i:5i] / 5i",
    );
    another(
        "with (f(w,w1) = m[w:w1], f2(x) = f(x, 23.34) / x) f2(123.456)",
        "m[123.456:23.34] / 123.456",
    );
}

#[test]
fn parse_with_expr_or_filters() {
    another(
        r#"with (x={a="b"}) x{c="d" or q="w",r="t"}"#,
        r#"{a="b",c="d" or a="b",q="w",r="t"}"#,
    );
    another(
        r#"with (x={a="b"}) foo{x,bar="baz" or c="d",x}"#,
        r#"foo{a="b",bar="baz" or c="d",a="b"}"#,
    );
    another(
        r#"with (x={a="b"}) foo{x,bar="baz",x or c="d"}"#,
        r#"foo{a="b",bar="baz" or c="d"}"#,
    );
    another(
        r#"with (x={a="b"}) foo{bar="baz",x or c="d"}"#,
        r#"foo{bar="baz",a="b" or c="d"}"#,
    );
    another(
        r#"with (x={a="b",c="d"}) {bar="baz",x or x,c="d",x}"#,
        r#"{bar="baz",a="b",c="d" or a="b",c="d"}"#,
    );
    another(
        r#"with (x={a="b" or c="d"}) x / x{e="f"}"#,
        r#"{a="b" or c="d"} / {a="b",e="f" or c="d",e="f"}"#,
    );
}

#[test]
fn parse_with_expr_group_join_prefix() {
    another(
        r#"with (f(x)=a + on() group_left(a,b) prefix x b) f("bar")"#,
        r#"a + on() group_left(a,b) prefix "bar" b"#,
    );
    another(
        r#"with (f(x)=a + on() group_left(a,b) prefix x+"foo" b) f("bar")"#,
        r#"a + on() group_left(a,b) prefix "barfoo" b"#,
    );
    another(
        r#"with (f(x)=a + on() group_left(a,b) prefix "foo"+x b) f("bar")"#,
        r#"a + on() group_left(a,b) prefix "foobar" b"#,
    );
    another(
        r#"with (f(x)=a + on() group_left(a,b) prefix "foo"+x+"baz" b) f("bar")"#,
        r#"a + on() group_left(a,b) prefix "foobarbaz" b"#,
    );
}

#[test]
fn parse_with_expr_overrides_and_recursion() {
    // Override ttf with something new.
    another("with (ttf = a) ttf + b", "a + b");
    // Override ttf with ru.
    another(
        "with (ttf = ru(m, n)) ttf",
        "(clamp_min(n - clamp_min(m, 0), 0) / clamp_min(n, 0)) * 100",
    );

    // Verify withExpr recursion and forward references.
    another("with (x = x+y, y = x+x) y ^ 2", "((x + y) + (x + y)) ^ 2");
    another(
        "with (f1(x)=ceil(x), ceil(x)=f1(x)^2) f1(foobar)",
        "ceil(foobar)",
    );
    another(
        "with (f1(x)=ceil(x), ceil(x)=f1(x)^2) ceil(foobar)",
        "ceil(foobar) ^ 2",
    );
}

#[test]
fn parse_with_expr_funcs() {
    another("with (x() = y+1) x", "y + 1");
    another("with (x(foo) = foo+1) x(a)", "a + 1");
    another("with (x(a, b) = a + b) x(foo, bar)", "foo + bar");
    another("with (x(a, b) = a + b) x(foo, x(1, 2))", "foo + 3");
    another(
        "with (x(a) = sum(a) by (b)) x(xx) / x(y)",
        "sum(xx) by(b) / sum(y) by(b)",
    );
    another(
        "with (f(a,f,x)=clamp(x,f,a)) f(f(x,y,z),1,2)",
        "clamp(2, 1, clamp(z, y, x))",
    );
    another(
        r#"with (f(x)=1+sum(x)) f(foo{bar="baz"})"#,
        r#"1 + sum(foo{bar="baz"})"#,
    );
    another("with (a=foo, y=bar, f(a)= a+a+y) f(x)", "(x + x) + bar");
    another(
        r#"with (f(a, b) = m{a, b}) f({a="x", b="y"}, {c="d"})"#,
        r#"m{a="x",b="y",c="d"}"#,
    );
    another(
        r#"with (xx={a="x"}, f(a, b) = m{a, b}) f({xx, b="y"}, {c="d"})"#,
        r#"m{a="x",b="y",c="d"}"#,
    );
    another(r#"with (x() = {b="c"}) foo{x}"#, r#"foo{b="c"}"#);
    another(
        r#"with (f(x)=x{foo="bar"} offset 5m) f(m offset 10m)"#,
        r#"(m{foo="bar"} offset 10m) offset 5m"#,
    );
    another(
        r#"with (f(x)=x{foo="bar",bas="a"}[5m]) f(m[10m] offset 3s)"#,
        r#"(m{foo="bar",bas="a"}[10m] offset 3s)[5m]"#,
    );
    another(
        r#"with (f(x)=x{foo="bar"}[5m] offset 10m) f(m{x="y"})"#,
        r#"m{x="y",foo="bar"}[5m] offset 10m"#,
    );
    another(
        r#"with (f(x)=x{foo="bar"}[5m] offset 10m) f({x="y", foo="bar", foo="bar"})"#,
        r#"{x="y",foo="bar"}[5m] offset 10m"#,
    );
    another(
        r#"with (f(m, x)=m{x}[5m] offset 10m) f(foo, {})"#,
        "foo[5m] offset 10m",
    );
    another(
        r#"with (f(m, x)=m{x, bar="baz"}[5m] offset 10m) f(foo, {})"#,
        r#"foo{bar="baz"}[5m] offset 10m"#,
    );
    another(
        "with (f(x)=x[5m] offset 3s) f(foo[3m]+bar)",
        "(foo[3m] + bar)[5m] offset 3s",
    );
    another(
        "with (f(x)=x[5m:3s] oFFsEt 1.5m) f(sum(s) by (a,b))",
        "(sum(s) by(a,b))[5m:3s] offset 1.5m",
    );
    another(r#"with (x="a", y=x) y+"bc""#, r#""abc""#);
    another(
        r#"with (x="a", y="b"+x) "we"+y+"z"+count()"#,
        r#""webaz" + count()"#,
    );
    another(
        r#"with (f(x) = m{foo=x+"y", bar="y"+x, baz=x} + x) f("qwe")"#,
        r#"m{foo="qwey",bar="yqwe",baz="qwe"} + "qwe""#,
    );
    another("with (f(a)=a) f", "f");
    another(r"with (f\q(a)=a) f\q", "fq");
}

#[test]
fn parse_with_expr_aggr_func_modifiers() {
    another("with (f(x) = x, y = sum(m) by (f)) y", "sum(m) by(f)");
    another(
        "with (f(x) = x, y = sum(m) by (f) limit 20) y",
        "sum(m) by(f) limit 20",
    );
    another("with (f(x) = sum(m) by (x)) f(foo)", "sum(m) by(foo)");
    another(
        "with (f(x) = sum(m) by (x) limit 42) f(foo)",
        "sum(m) by(foo) limit 42",
    );
    another(
        "with (f(x) = sum(m) by (x)) f((foo, bar, foo))",
        "sum(m) by(foo,bar)",
    );
    another(
        "with (f(x) = sum(m) without (x,y)) f((a, b))",
        "sum(m) without(a,b,y)",
    );
    another(
        "with (f(x) = sum(m) without (y,x)) f((a, y))",
        "sum(m) without(y,a)",
    );
    another(
        "with (f(x,y) = a + on (x,y) group_left (y,bar) b) f(foo,())",
        "a + on(foo) group_left(bar) b",
    );
    another(
        "with (f(x,y) = a + on (x,y) group_left (y,bar) b) f((foo),())",
        "a + on(foo) group_left(bar) b",
    );
    another(
        "with (f(x,y) = a + on (x,y) group_left (y,bar) b) f((foo,xx),())",
        "a + on(foo,xx) group_left(bar) b",
    );

    // withExpr for group_left() / group_right() prefix.
    another(
        r#"with (f(x) = a+on() group_left() prefix x b) f("foo")"#,
        r#"a + on() group_left() prefix "foo" b"#,
    );
    another(
        r#"with (f(x) = a+on() group_left() prefix x+"bar" b) f("foo")"#,
        r#"a + on() group_left() prefix "foobar" b"#,
    );
    another(
        r#"with (f(x) = a+on() group_left() prefix "bar"+x b) f("foo")"#,
        r#"a + on() group_left() prefix "barfoo" b"#,
    );
    another(
        r#"with (f(x,y) = a+on() group_left() prefix y+x b) f("foo","bar")"#,
        r#"a + on() group_left() prefix "barfoo" b"#,
    );
}

#[test]
fn parse_nested_with_exprs() {
    another("with (f(x) = (with(x=y) x) + x) f(z)", "y + z");
    another(
        "with (x=foo) clamp_min(a, with (y=x) y)",
        "clamp_min(a, foo)",
    );
    another(
        "with (x=foo) a * x + (with (y=x) y) / y",
        "(a * foo) + (foo / y)",
    );
    another(
        "with (x = with (y = foo) y + x) x/x",
        "(foo + x) / (foo + x)",
    );
    another(
        "with (
		x = {foo=\"bar\"},
		q = m{x, y=\"1\"},
		f(x) =
			with (
				z(y) = x + y * q
			)
			z(foo) / changes(x)
	)
	f(a)",
        r#"(a + (foo * m{foo="bar",y="1"})) / changes(a)"#,
    );
}

#[test]
fn parse_complex_with_exprs() {
    another(
        "WITH (
		treshold = (0.9),
		commonFilters = {job=\"cacher\", instance=~\"1.2.3.4\"},
		hits = rate(cache{type=\"hit\", commonFilters}[5m]),
		miss = rate(cache{type=\"miss\", commonFilters}[5m]),
		sumByInstance(arg) = sum(arg) by (instance),
		hitRatio = sumByInstance(hits) / sumByInstance(hits + miss)
	)
	hitRatio < treshold",
        r#"(sum(rate(cache{type="hit",job="cacher",instance=~"1.2.3.4"}[5m])) by(instance) / sum(rate(cache{type="hit",job="cacher",instance=~"1.2.3.4"}[5m]) + rate(cache{type="miss",job="cacher",instance=~"1.2.3.4"}[5m])) by(instance)) < 0.9"#,
    );
    another(
        "WITH (
		x2(x) = x^2,
		f(x, y) = x2(x) + x*y + x2(y)
	)
	f(a, 3)
	",
        "((a ^ 2) + (a * 3)) + 9",
    );
    another(
        "WITH (
		x2(x) = x^2,
		f(x, y) = x2(x) + x*y + x2(y)
	)
	f(2, 3)
	",
        "19",
    );
    another(
        "WITH (
		commonFilters = {instance=\"foo\"},
		timeToFuckup(currv, maxv) = (maxv - currv) / rate(currv)
	)
	timeToFuckup(diskUsage{commonFilters}, maxDiskSize{commonFilters})",
        r#"(maxDiskSize{instance="foo"} - diskUsage{instance="foo"}) / rate(diskUsage{instance="foo"})"#,
    );
    another(
        "WITH (
	       commonFilters = {job=\"foo\", instance=\"bar\"},
	       sumRate(m, cf) = sum(rate(m{cf})) by (job, instance),
	       hitRate(hits, misses) = sumRate(hits, commonFilters) / (sumRate(hits, commonFilters) + sumRate(misses, commonFilters))
	   )
	   hitRate(cacheHits, cacheMisses)",
        r#"sum(rate(cacheHits{job="foo",instance="bar"})) by(job,instance) / (sum(rate(cacheHits{job="foo",instance="bar"})) by(job,instance) + sum(rate(cacheMisses{job="foo",instance="bar"})) by(job,instance))"#,
    );
    another(
        "with(y=123,z=5) union(with(y=3,f(x)=x*y) f(2) + f(3), with(x=5,y=2) x*y*z)",
        "union(15, 50)",
    );
    another(
        "with(sum=123,now=5) union(with(sum=3,f(x)=x*sum) f(2) + f(3), with(x=5,sum=2) x*sum*now)",
        "union(15, 50)",
    );
    another(
        "WITH(now = sum(rate(my_metric_total)), before = sum(rate(my_metric_total) offset 1h)) now/before*100",
        "(sum(rate(my_metric_total)) / sum(rate(my_metric_total) offset 1h)) * 100",
    );
    another("with (sum = x) sum", "x");
    another("with (clamp_min=x) clamp_min", "x");
    another("with (now=now(), sum=sum()) now", "now()");
    another("with (now=now(), sum=sum()) now()", "now()");
    another("with (now(a)=now()+a) now(1)", "now() + 1");
    another("with (rate(a,b)=a+b) rate(1,2)", "3");
    another("with (now=now(), sum=sum()) x", "x");
    another("with (rate(a) = b) c", "c");
    another(
        "rate(x) + with (rate(a,b)=a*b) rate(2,b)",
        "rate(x) + (2 * b)",
    );
    another("with (sum(a,b)=a+b) sum(c,d)", "c + d");
}

#[test]
fn parse_grafana_interval_vars() {
    // $__interval and $__rate_interval must be replaced with 1i.
    another(
        "rate(m[$__interval] offset $__interval) * $__interval",
        "rate(m offset 1i) * 1i",
    );
    another(
        "increase(m[$__rate_interval] offset -$__rate_interval) + -$__rate_interval",
        "increase(m offset -1i) + (0 - 1i)",
    );
    another("rate(m[$__rate_interval:5m])", "rate(m[:5m])");
    another("rate(m[$__interval:5m])", "rate(m[:5m])");
}

#[test]
fn parse_tsbs_queries() {
    // The TSBS benchmark queries must parse (not part of the Go test suite).
    another(
        "max(max_over_time(cpu_usage_user{hostname=~'host_1|host_2'}[1m])) by (__name__)",
        r#"max(max_over_time(cpu_usage_user{hostname=~"host_1|host_2"}[1m])) by(__name__)"#,
    );
    another(
        "avg(avg_over_time({__name__=~'cpu_(usage_user|usage_system)', hostname='host_1'}[1h])) by (__name__, hostname)",
        r#"avg(avg_over_time({__name__=~"cpu_(usage_user|usage_system)",hostname="host_1"}[1h])) by(__name__,hostname)"#,
    );
}

#[test]
fn parse_error_cases() {
    // An empty string.
    f("");
    f("  \t\x08\r\n  ");

    // Invalid metricExpr.
    f("{}[5M:]");
    f("foo[-55]");
    f("m[-5m]");
    f("{");
    f("foo{");
    f("foo{bar");
    f("foo{bar=");
    f(r#"foo{bar="baz""#);
    f(r#"foo{bar="baz",  "#);
    f(r#"foo{123="23"}"#);
    f("foo{foo}");
    f("foo{,}");
    f(r#"foo{,foo="bar"}"#);
    f("foo{foo=}");
    f(r#"foo{foo="ba}"#);
    // Test if two metric names are set.
    f(r#"{"foo", "a"="1", "bar"}"#);
    f(r#"{"foo", __name__="bar"}"#);
    f("foo{$");
    f("foo{a $");
    f(r#"foo{a="b",$"#);
    f(r#"foo{a="b"}$"#);
    f("[");
    f("[]");
    f("f[5m]$");
    f("[5m]");
    f("[5m] offset 4h");
    f("m[5m] offset $");
    f("m[5m] offset 5h $");
    f("m[]");
    f("m[-5m]");
    f("m[5m:");
    f("m[5m:-");
    f("m[5m:-1");
    f("m[5m:-1]");
    f("m[5m:-1s]");
    f("m[-5m:1s]");
    f("m[-5m:-1s]");
    f("m[:");
    f("m[:-");
    f("m[:-1]");
    f("m[:-1m]");
    f("m[-5]");
    f("m[[5m]]");
    f("m[foo]");
    f(r#"m["ff"]"#);
    f("m[10m");
    f("m[123");
    f(r#"m["ff"#);
    f("m[(f");
    f("fd}");
    f("]");
    f("m $");
    f("m{,}");
    f("m{x=y}");
    f("m{x=y/5}");
    f("m{x=y+5}");

    // Invalid 'or' filters.
    f("{or");
    f("a{or");
    f("{or x}");
    f(r#"{or x="y"}"#);
    f("{x or}");
    f("{x or,");
    f("{x or,}");
    f(r#"{x="y" or"#);
    f(r#"{x="y" or}"#);
    f(r#"{x="y" or z"#);
    f(r#"{x="y" or z="x""#);

    // keep_metric_names cannot be used with metric expressions.
    f("m keep_metric_names");

    // Invalid @ modifier.
    f("@");
    f("foo @");
    f("foo @ ! ");
    f("foo @ @");
    f("foo @ offset 5m");
    f("foo @ [5m]");
    f("foo offset @ 5m");
    f("foo @ 123 offset 5m @ 456");
    f("foo offset 5m @");

    // Invalid regexp.
    f(r#"foo{bar=~"x["}"#);
    f(r#"foo{bar=~"x("}"#);
    f(r#"foo{bar=~"x)"}"#);
    f(r#"foo{bar!~"x["}"#);
    f(r#"foo{bar!~"x("}"#);
    f(r#"foo{bar!~"x)"}"#);

    // invalid stringExpr.
    f("'");
    f("\"");
    f("`");
    f(r#""foo"#);
    f("'foo");
    f("`foo");
    f(r#""foo\"bar"#);
    f(r"'foo\'bar");
    f("`foo\\`bar");
    f(r#""" $"#);
    f(r#""foo" +"#);
    f(r#"n{"foo" + m"#);
    f(r#""foo" keep_metric_names"#);
    f(r#"keep_metric_names "foo""#);

    // Invalid numberExpr.
    f("1.2e");
    f("23e-");
    f("23E+");
    f(".");
    f("-1.2e");
    f("-23e-");
    f("-23E+");
    f("-.");
    f("-1$$");
    f("-$$");
    f("+$$");
    f("23 $$");
    f("1 keep_metric_names");
    f("keep_metric_names 1");

    // Invalid binaryOpExpr.
    f("+");
    f("1 +");
    f("3 unless");
    f("23 + on (foo)");
    f("m + on (,) m");
    f("3 * ignoring");
    f("m * on (");
    f("m * on (foo");
    f("m * on (foo,");
    f("m * on (foo,)");
    f("m * on (,foo)");
    f("m * on (,)");
    f("m == bool (bar) baz");
    f("m == bool () baz");
    f("m * by (baz) n");
    f("m + bool group_left m2");
    f("m + on () group_left (");
    f("m + on () group_left (,");
    f("m + on () group_left (,foo");
    f("m + on () group_left (foo,)");
    f("m + on () group_left (,foo)");
    f("m + on () group_left (foo)");
    f("m + on () group_right (foo) (m");
    f("m or ignoring () group_left () n");
    f("1 + bool 2");
    f("m % bool n");
    f("m * bool baz");
    f("M * BOoL BaZ");
    f("foo unless ignoring (bar) group_left xxx");
    f("foo or bool bar");
    f("foo == bool $$");
    f(r#""foo" + bar"#);
    f("(foo + ");
    f("a + on(*) b"); // star cannot be used inside on()
    f("a + ignoring(*) b"); // star cannot be used inside ignoring()
    f(r#"a + prefix "b" c"#); // missing group_left()/group_right()
    f(r#"a + on() prefix "b" c"#); // missing group_left()/group_right()
    f(r#"a + ignoring(foo) prefix "b" c"#); // missing group_left()/group_right()
    f("a + on() group_left(*,x) b"); // star cannot be mixed with other labels
    f("a + on() group_right(x,*) b"); // star cannot be mixed with other labels

    // Invalid parensExpr.
    f("(");
    f("($");
    f("(+");
    f("(1");
    f("(m+");
    f("1)");
    f("(,)");
    f("(1)$");
    f("(foo) keep_metric_names");

    // Invalid funcExpr.
    f("f $");
    f("f($)");
    f("f[");
    f("f()$");
    f("f(");
    f("f(foo");
    f("f(f,");
    f("f(,");
    f("f(,)");
    f("f(,foo)");
    f("f(,foo");
    f("f(foo,$");
    f("f() by (a)");
    f("f without (x) (y)");
    f("f() foo (a)");
    f("f bar (x) (b)");
    f("f bar (x)");
    f("keep_metric_names f()");
    f("f() abc");

    // Unknown functions.
    f("f()");
    f("f(x,)");
    f("f(http_server_request)");
    f("f(job, foo)");
    f("sum(a, b+f())");
    f("by(2)");
    f("BY(2)");
    f("or(2)");
    f("OR(2)");
    f("bool(2)");
    f("BOOL(2)");
    f(r#""foo" + bar()"#);
    f("a + (bool(1, 2))");
    f("group_left / (on(1, 2))");
    f("group_left / (f(1, 2))");

    // Invalid aggrFuncExpr.
    f("sum(");
    f("sum $");
    f("sum [");
    f("sum($)");
    f("sum()$");
    f("sum(foo) ba");
    f("sum(foo) ba()");
    f("sum(foo) by");
    f("sum(foo) without x");
    f("sum(foo) aaa");
    f("sum(foo) aaa x");
    f("sum() by $");
    f("sum() by (");
    f("sum() by ($");
    f("sum() by (a");
    f("sum() by (a $");
    f("sum() by (a ]");
    f("sum() by (a)$");
    f("sum() by (,");
    f("sum() by (a,$");
    f("sum() by (,)");
    f("sum() by (,a");
    f("sum() by (,a)");
    f("sum() on (b)");
    f("sum() bool");
    f("sum() group_left");
    f("sum() group_right(x)");
    f("sum ba");
    f("sum ba ()");
    f("sum by (");
    f("sum by (a");
    f("sum by (,");
    f("sum by (,)");
    f("sum by (,a");
    f("sum by (,a)");
    f("sum by (a)");
    f("sum by (a) (");
    f("sum by (a) [");
    f("sum by (a) {");
    f("sum by (a) (b");
    f("sum by (a) (b,");
    f("sum by (a) (,)");
    f("avg by (a) (,b)");
    f("sum by (x) (y) by (z)");
    f("sum(m) by (1)");
    f("sum(m) keep_metric_names"); // keep_metric_names cannot be used for aggregate functions
    f("sum(m) by(*)"); // star cannot be used in by()
    f("sum(m) without(*)"); // star cannot be used in without()

    // Invalid withExpr.
    f("with $");
    f("with a");
    f("with a=b c");
    f("with (");
    f("with (x=b)$");
    f("with ($");
    f("with (foo");
    f("with (foo $");
    f("with (x y");
    f("with (x =");
    f("with (x = $");
    f("with (x= y");
    f("with (x= y $");
    f("with (x= y)");
    f("with (x=(");
    f("with (x=[)");
    f("with (x=() x)");
    f("with(x)");
    f("with ($$)");
    f("with (x $$");
    f("with (x = $$)");
    f(r#"with (x = foo) bar{x}"#);
    f(r#"with (x = {foo="bar"}[5m]) bar{x}"#);
    f(r#"with (x = {foo="bar"} offset 5m) bar{x}"#);
    f("with (x = a, x = b) c");
    f("with (x(a, a) = b) c");
    f(r#"with (x=m{f="x"}) foo{x}"#);
    f("with (f()");
    f("with (a=b c=d) e");
    f("with (f(x)=x^2) m{x}");
    f("with (f(x)=ff()) m{x}");
    f("with (f(x");
    f("with (x=m) a{x} + b");
    f("with (x=m) b + a{x}");
    f("with (x=m) f(b, a{x})");
    f("with (x=m) sum(a{x})");
    f("with (x=m) (a{x})");
    f("with (f(a)=a) f(1, 2)");
    f(r#"with (f(x)=x{foo="bar"}) f(1)"#);
    f(r#"with (f(x)=x{foo="bar"}) f(m + n)"#);
    f("with (f = with");
    f("with (,)");
    f("with (1) 2");
    f("with (f(1)=2) 3");
    f("with (f(,)=x) x");
    f(r#"with (x(a) = {b="c"}) foo{x}"#);
    f(r#"with (f(x) = m{foo=xx}) f("qwe")"#);
    f("a + with(f(x)=x) f(1,2)");
    f(r#"with (f(x) = sum(m) by (x)) f({foo="bar"})"#);
    f(r#"with (f(x) = sum(m) by (x)) f((xx(), {foo="bar"}))"#);
    f("with (f(x) = m + on (x) n) f(xx())");
    f("with (f(x) = m + on (a) group_right (x) n) f(xx())");
    f("with (f(x) = m keep_metric_names)");
    f("with (now)");
    f("with (sum)");
    f("with (now=now()) now(1)");
    f("with (f())");
    f("with (sum(a,b)=a+b) sum(x)");
    f("with (rate()=foobar) rate(x)");
    f("{foo}");
    f("with (x={y}) x");

    // WITH template with {lf1 or lf2} isn't supported.
    f(r#"with (f(x) = m{x}) f({a="b" or c="d"})"#);
    f(r#"with (f(x) = m{x or y="z"}) f({a="b" or c="d"})"#);
    f(r#"with (f(__name__) = {__name__}) f({a="b" or c="d"})"#);

    // Invalid number of args at ct().
    f(r#"with (ct={job="test", i="bar"}) ct + {ct, x="d"} + foo{ct, ct} + ct(1)"#);

    // Unknown function ff().
    f("with (f(a,f,x)=ff(x,f,a)) f(f(x,y,z),1,2)");

    // Unknown function f().
    f(r#"with (x="a", y="b"+x) "we"+y+"z"+f()"#);
    f("with (x=foo) f(a, with (y=x) y)");
    f(r#"with (ct={job="test"}) a{ct} + ct() + f({ct="x"})"#);

    // Invalid withExpr with 'or' filters.
    f(r#"with (x={a="b" or c="d"}) {x}"#);
    f(r#"with (x={a="b" or c="d"}) x{d="e" or z="c"}"#);
    f(r#"with (x={a="b" or c="d"}) {x,d="e"}"#);
    f(r#"with (x={a="b" or c="d"}) {x,d="e" or z="c"}"#);
}
