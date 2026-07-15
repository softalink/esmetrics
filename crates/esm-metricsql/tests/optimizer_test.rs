//! Port of `optimizer_test.go` (TestPushdownBinaryOpFilters and TestOptimize).
//!
//! TestGetCommonLabelFilters lives in `src/optimizer.rs` since it exercises
//! a crate-private helper.

use esm_metricsql::{optimize, parse, pushdown_binary_op_filters, Expr};

// Port of TestPushdownBinaryOpFilters.
#[test]
fn pushdown_binary_op_filters_cases() {
    let f = |q: &str, filters: &str, result_expected: &str| {
        let e = parse(q).unwrap_or_else(|err| panic!("cannot parse {q}: {err}"));
        let s_orig = e.to_string();
        let filters_expr =
            parse(filters).unwrap_or_else(|err| panic!("cannot parse filters {filters}: {err}"));
        let Expr::Metric(me) = filters_expr else {
            panic!("filters={filters} must be a metrics expression");
        };
        assert!(
            me.label_filterss.len() <= 1,
            "filters={filters} mustn't contain 'or'"
        );
        let lfs = me.label_filterss.into_iter().next().unwrap_or_default();
        let result_expr = pushdown_binary_op_filters(&e, &lfs);
        let result = result_expr.to_string();
        assert_eq!(
            result, result_expected,
            "unexpected result for pushdown_binary_op_filters({q}, {filters})"
        );
        // Verify that the original e didn't change.
        assert_eq!(
            e.to_string(),
            s_orig,
            "the original expression has been changed"
        );
    };
    f("foo", "{}", "foo");
    f("foo", r#"{a="b"}"#, r#"foo{a="b"}"#);
    f(
        r#"foo + bar{x="y"}"#,
        r#"{c="d",a="b"}"#,
        r#"foo{a="b",c="d"} + bar{a="b",c="d",x="y"}"#,
    );
    f("sum(x)", r#"{a="b"}"#, "sum(x)");
    f("foo or bar", r#"{a="b"}"#, r#"foo{a="b"} or bar{a="b"}"#);
    f("foo or on(x) bar", r#"{a="b"}"#, "foo or on(x) bar");
    f(
        "foo == on(x) group_LEft bar",
        r#"{a="b"}"#,
        "foo == on(x) group_left() bar",
    );
    f(
        r#"foo{x="y"} > ignoRIng(x) group_left(abc) bar"#,
        r#"{a="b"}"#,
        r#"foo{a="b",x="y"} > ignoring(x) group_left(abc) bar{a="b"}"#,
    );
    f(
        r#"foo{x="y"} >bool ignoring(x) group_right(abc,def) bar"#,
        r#"{a="b"}"#,
        r#"foo{a="b",x="y"} >bool ignoring(x) group_right(abc,def) bar{a="b"}"#,
    );
    f(
        "foo * ignoring(x) bar",
        r#"{a="b"}"#,
        r#"foo{a="b"} * ignoring(x) bar{a="b"}"#,
    );
    f(
        r#"foo{f1!~"x"} UNLEss bar{f2=~"y.+"}"#,
        r#"{a="b",x=~"y"}"#,
        r#"foo{a="b",f1!~"x",x=~"y"} unless bar{a="b",f2=~"y.+",x=~"y"}"#,
    );
    f(
        "a / sum(x)",
        r#"{a="b",c=~"foo|bar"}"#,
        r#"a{a="b",c=~"foo|bar"} / sum(x)"#,
    );
    f(
        r#"round(rate(x[5m] offset -1h)) + 123 / {a="b"}"#,
        r#"{x!="y"}"#,
        r#"round(rate(x{x!="y"}[5m] offset -1h)) + (123 / {a="b",x!="y"})"#,
    );
    f(
        "scalar(foo)+bar",
        r#"{a="b"}"#,
        r#"scalar(foo) + bar{a="b"}"#,
    );
    f("vector(foo)", r#"{a="b"}"#, r#"vector(foo{a="b"})"#);
    f(
        r#"{a="b"} + on() group_left() {c="d"}"#,
        r#"{a="b"}"#,
        r#"{a="b"} + on() group_left() {c="d"}"#,
    );

    // Pushdown for 'or' filters.
    f(
        r#"foo{a="b" or c="d" or x="y",q="w"}"#,
        r#"{x="y"}"#,
        r#"foo{a="b",x="y" or c="d",x="y" or q="w",x="y"}"#,
    );
    f(
        r#"{a="b" or x="y",q="w"} + bar"#,
        r#"{x="y"}"#,
        r#"{a="b",x="y" or q="w",x="y"} + bar{x="y"}"#,
    );

    // Pushdown for label_set.
    f(
        r#"label_set(foo, "a", "b") + bar{baz="a"}"#,
        r#"{x="y"}"#,
        r#"label_set(foo{x="y"}, "a", "b") + bar{baz="a",x="y"}"#,
    );
    f(
        r#"label_set(foo, "a", "b", "x", "aa") + bar{baz="a"}"#,
        r#"{x="y"}"#,
        r#"label_set(foo, "a", "b", "x", "aa") + bar{baz="a",x="y"}"#,
    );
    f(
        r#"label_set(label_set(foo, "a", "b"), "c", "d") + bar"#,
        r#"{x="y"}"#,
        r#"label_set(label_set(foo{x="y"}, "a", "b"), "c", "d") + bar{x="y"}"#,
    );
}

// Port of TestOptimize.
#[test]
fn optimize_cases() {
    let f = |q: &str, q_optimized_expected: &str| {
        let e = parse(q).unwrap_or_else(|err| panic!("cannot parse {q}: {err}"));
        let s_orig = e.to_string();
        let e_optimized = optimize(&e);
        let q_optimized = e_optimized.to_string();
        assert_eq!(
            q_optimized, q_optimized_expected,
            "unexpected qOptimized for {q}"
        );
        // Make sure the original e didn't change after optimize().
        assert_eq!(
            e.to_string(),
            s_orig,
            "the original expression has been changed"
        );
    };
    f("foo", "foo");

    // Reserved words.
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/4422
    f("1 + (on)", "1 + (on)");
    f(
        r#"{a="b"} + (group_left)"#,
        r#"{a="b"} + (group_left{a="b"})"#,
    );
    f(
        r#"bool{a="b"} + (ignoring{c="d"})"#,
        r#"bool{a="b",c="d"} + (ignoring{a="b",c="d"})"#,
    );

    // Common binary expressions.
    f("a + b", "a + b");
    f(
        r#"foo{label1="value1"} == bar"#,
        r#"foo{label1="value1"} == bar{label1="value1"}"#,
    );
    f(
        r#"foo{label1="value1"} == bar{label2="value2"}"#,
        r#"foo{label1="value1",label2="value2"} == bar{label1="value1",label2="value2"}"#,
    );
    f(
        r#"foo + bar{b=~"a.*", a!="ss"}"#,
        r#"foo{a!="ss",b=~"a.*"} + bar{a!="ss",b=~"a.*"}"#,
    );
    f(r#"foo{bar="1"} / 234"#, r#"foo{bar="1"} / 234"#);
    f(
        r#"foo{bar="1"} / foo{bar="1"}"#,
        r#"foo{bar="1"} / foo{bar="1"}"#,
    );
    f(r#"123 + foo{bar!~"xx"}"#, r#"123 + foo{bar!~"xx"}"#);
    f(r#"foo or bar{x="y"}"#, r#"foo or bar{x="y"}"#);
    f(
        r#"foo{x="y"} * on() baz{a="b"}"#,
        r#"foo{x="y"} * on() baz{a="b"}"#,
    );
    f(
        r#"foo{x="y"} * on(a) baz{a="b"}"#,
        r#"foo{a="b",x="y"} * on(a) baz{a="b"}"#,
    );
    f(
        r#"foo{x="y"} * on(bar) baz{a="b"}"#,
        r#"foo{x="y"} * on(bar) baz{a="b"}"#,
    );
    f(
        r#"foo{x="y"} * on(x,a,bar) baz{a="b"}"#,
        r#"foo{a="b",x="y"} * on(x,a,bar) baz{a="b",x="y"}"#,
    );
    f(
        r#"foo{x="y"} * ignoring() baz{a="b"}"#,
        r#"foo{a="b",x="y"} * ignoring() baz{a="b",x="y"}"#,
    );
    f(
        r#"foo{x="y"} * ignoring(a) baz{a="b"}"#,
        r#"foo{x="y"} * ignoring(a) baz{a="b",x="y"}"#,
    );
    f(
        r#"foo{x="y"} * ignoring(bar) baz{a="b"}"#,
        r#"foo{a="b",x="y"} * ignoring(bar) baz{a="b",x="y"}"#,
    );
    f(
        r#"foo{x="y"} * ignoring(x,a,bar) baz{a="b"}"#,
        r#"foo{x="y"} * ignoring(x,a,bar) baz{a="b"}"#,
    );
    f(
        r#"foo{x="y"} * ignoring() group_left(foo,bar) baz{a="b"}"#,
        r#"foo{a="b",x="y"} * ignoring() group_left(foo,bar) baz{a="b",x="y"}"#,
    );
    f(
        r#"foo{x="y"} * on(a) group_left baz{a="b"}"#,
        r#"foo{a="b",x="y"} * on(a) group_left() baz{a="b"}"#,
    );
    f(
        r#"foo{x="y"} * on(a) group_right(x, y) baz{a="b"}"#,
        r#"foo{a="b",x="y"} * on(a) group_right(x,y) baz{a="b"}"#,
    );
    f(
        r#"histogram_quantile(foo, bar{baz=~"sdf"} + aa{baz=~"axx", aa="b"})"#,
        r#"histogram_quantile(foo, bar{aa="b",baz=~"axx",baz=~"sdf"} + aa{aa="b",baz=~"axx",baz=~"sdf"})"#,
    );
    f(
        r#"sum(foo, bar{baz=~"sdf"} + aa{baz=~"axx", aa="b"})"#,
        r#"sum(foo, bar{aa="b",baz=~"axx",baz=~"sdf"} + aa{aa="b",baz=~"axx",baz=~"sdf"})"#,
    );
    f(
        r#"foo AND bar{baz="aa"}"#,
        r#"foo{baz="aa"} and bar{baz="aa"}"#,
    );
    f(
        r#"{x="y",__name__="a"} + {a="b"}"#,
        r#"a{a="b",x="y"} + {a="b",x="y"}"#,
    );
    f(
        r#"{x="y",__name__=~"a|b"} + {a="b"}"#,
        r#"{__name__=~"a|b",a="b",x="y"} + {a="b",x="y"}"#,
    );
    f(
        r#"a{x="y",__name__=~"a|b"} + {a="b"}"#,
        r#"a{__name__=~"a|b",a="b",x="y"} + {a="b",x="y"}"#,
    );
    f(
        r#"{a="b"} + ({c="d"} * on() group_left() {e="f"})"#,
        r#"{a="b",c="d"} + ({c="d"} * on() group_left() {e="f"})"#,
    );
    f(
        r#"{a="b"} + ({c="d"} * on(a) group_left() {e="f"})"#,
        r#"{a="b",c="d"} + ({a="b",c="d"} * on(a) group_left() {a="b",e="f"})"#,
    );
    f(
        r#"{a="b"} + ({c="d"} * on(c) group_left() {e="f"})"#,
        r#"{a="b",c="d"} + ({c="d"} * on(c) group_left() {c="d",e="f"})"#,
    );
    f(
        r#"{a="b"} + ({c="d"} * on(e) group_left() {e="f"})"#,
        r#"{a="b",c="d",e="f"} + ({c="d",e="f"} * on(e) group_left() {e="f"})"#,
    );
    f(
        r#"{a="b"} + ({c="d"} * on(x) group_left() {e="f"})"#,
        r#"{a="b",c="d"} + ({c="d"} * on(x) group_left() {e="f"})"#,
    );
    f(
        r#"{a="b"} + ({c="d"} * on() group_right() {e="f"})"#,
        r#"{a="b",e="f"} + ({c="d"} * on() group_right() {e="f"})"#,
    );
    f(
        r#"{a="b"} + ({c="d"} * on(a) group_right() {e="f"})"#,
        r#"{a="b",e="f"} + ({a="b",c="d"} * on(a) group_right() {a="b",e="f"})"#,
    );
    f(
        r#"{a="b"} + ({c="d"} * on(c) group_right() {e="f"})"#,
        r#"{a="b",c="d",e="f"} + ({c="d"} * on(c) group_right() {c="d",e="f"})"#,
    );
    f(
        r#"{a="b"} + ({c="d"} * on(e) group_right() {e="f"})"#,
        r#"{a="b",e="f"} + ({c="d",e="f"} * on(e) group_right() {e="f"})"#,
    );
    f(
        r#"{a="b"} + ({c="d"} * on(x) group_right() {e="f"})"#,
        r#"{a="b",e="f"} + ({c="d"} * on(x) group_right() {e="f"})"#,
    );
    f(
        r#"{a="b" or c="d"} + ({c="d"} * on(x) group_right() {e="f"})"#,
        r#"{a="b",e="f" or c="d",e="f"} + ({c="d"} * on(x) group_right() {e="f"})"#,
    );
    f(
        r#"a + on(x) group_left(*) (prefix{x="a"})"#,
        r#"a{x="a"} + on(x) group_left(*) (prefix{x="a"})"#,
    );
    f(
        r#"a + on(x) group_right(*) prefix "foo_" b{x="a"}"#,
        r#"a{x="a"} + on(x) group_right(*) prefix "foo_" b{x="a"}"#,
    );
    f(
        r#"a{x="a"} + on(x) group_right(*) prefix "foo_" prefix"#,
        r#"a{x="a"} + on(x) group_right(*) prefix "foo_" (prefix{x="a"})"#,
    );
    f(
        r#"foo{a="a"} ifnot foo{b="b"}"#,
        r#"foo{a="a"} ifnot foo{a="a",b="b"}"#,
    );

    // Specially handled binary expressions.
    f(r#"foo{a="b"} or bar{x="y"}"#, r#"foo{a="b"} or bar{x="y"}"#);
    f(
        r#"(foo{a="b"} + bar{c="d"}) or (baz{x="y"} <= x{a="b"})"#,
        r#"(foo{a="b",c="d"} + bar{a="b",c="d"}) or (baz{a="b",x="y"} <= x{a="b",x="y"})"#,
    );
    f(
        r#"(foo{a="b"} + bar{c="d"}) or on(x) (baz{x="y"} <= x{a="b"})"#,
        r#"(foo{a="b",c="d"} + bar{a="b",c="d"}) or on(x) (baz{a="b",x="y"} <= x{a="b",x="y"})"#,
    );
    f(
        r#"foo + (bar or baz{a="b"})"#,
        r#"foo + (bar or baz{a="b"})"#,
    );
    f(
        r#"foo + (bar{a="b"} or baz{a="b"})"#,
        r#"foo{a="b"} + (bar{a="b"} or baz{a="b"})"#,
    );
    f(
        r#"foo + (bar{a="b",c="d"} or baz{a="b"})"#,
        r#"foo{a="b"} + (bar{a="b",c="d"} or baz{a="b"})"#,
    );
    f(
        r#"foo{a="b"} + (bar OR baz{x="y"})"#,
        r#"foo{a="b"} + (bar{a="b"} or baz{a="b",x="y"})"#,
    );
    f(
        r#"foo{a="b"} + (bar{x="y",z="456"} OR baz{x="y",z="123"})"#,
        r#"foo{a="b",x="y"} + (bar{a="b",x="y",z="456"} or baz{a="b",x="y",z="123"})"#,
    );
    f(
        r#"foo{a="b"} unless bar{c="d"}"#,
        r#"foo{a="b"} unless bar{a="b",c="d"}"#,
    );
    f(
        r#"foo{a="b"} unless on() bar{c="d"}"#,
        r#"foo{a="b"} unless on() bar{c="d"}"#,
    );
    f(
        r#"foo + (bar{x="y"} unless baz{a="b"})"#,
        r#"foo{x="y"} + (bar{x="y"} unless baz{a="b",x="y"})"#,
    );
    f(
        r#"foo + (bar{x="y"} unless on() baz{a="b"})"#,
        r#"foo + (bar{x="y"} unless on() baz{a="b"})"#,
    );
    f(
        r#"foo{a="b"} + (bar UNLESS baz{x="y"})"#,
        r#"foo{a="b"} + (bar{a="b"} unless baz{a="b",x="y"})"#,
    );
    f(
        r#"foo{a="b"} + (bar{x="y"} unLESS baz)"#,
        r#"foo{a="b",x="y"} + (bar{a="b",x="y"} unless baz{a="b",x="y"})"#,
    );

    // Aggregate funcs.
    f(
        r#"sum(foo{bar="baz"}) / a{b="c"}"#,
        r#"sum(foo{bar="baz"}) / a{b="c"}"#,
    );
    f(
        r#"sum(foo{bar="baz"}) by () / a{b="c"}"#,
        r#"sum(foo{bar="baz"}) by() / a{b="c"}"#,
    );
    f(
        r#"sum(foo{bar="baz"}) by (bar) / a{b="c"}"#,
        r#"sum(foo{bar="baz"}) by(bar) / a{b="c",bar="baz"}"#,
    );
    f(
        r#"sum(foo{bar="baz"}) by (b) / a{b="c"}"#,
        r#"sum(foo{b="c",bar="baz"}) by(b) / a{b="c"}"#,
    );
    f(
        r#"sum(foo{bar="baz"}) by (x) / a{b="c"}"#,
        r#"sum(foo{bar="baz"}) by(x) / a{b="c"}"#,
    );
    f(
        r#"sum(foo{bar="baz"}) by (bar,b) / a{b="c"}"#,
        r#"sum(foo{b="c",bar="baz"}) by(bar,b) / a{b="c",bar="baz"}"#,
    );
    f(
        r#"sum(foo{bar="baz"}) without () / a{b="c"}"#,
        r#"sum(foo{b="c",bar="baz"}) without() / a{b="c",bar="baz"}"#,
    );
    f(
        r#"sum(foo{bar="baz"}) without (bar) / a{b="c"}"#,
        r#"sum(foo{b="c",bar="baz"}) without(bar) / a{b="c"}"#,
    );
    f(
        r#"sum(foo{bar="baz"}) without (b) / a{b="c"}"#,
        r#"sum(foo{bar="baz"}) without(b) / a{b="c",bar="baz"}"#,
    );
    f(
        r#"sum(foo{bar="baz"}) without (x) / a{b="c"}"#,
        r#"sum(foo{b="c",bar="baz"}) without(x) / a{b="c",bar="baz"}"#,
    );
    f(
        r#"sum(foo{bar="baz"}) without (bar,b) / a{b="c"}"#,
        r#"sum(foo{bar="baz"}) without(bar,b) / a{b="c"}"#,
    );
    f(
        r#"sum(foo, bar) by (a) + baz{a="b"}"#,
        r#"sum(foo{a="b"}, bar{a="b"}) by(a) + baz{a="b"}"#,
    );
    f(
        r#"topk(3, foo) by (baz,x) + bar{baz="a"}"#,
        r#"topk(3, foo{baz="a"}) by(baz,x) + bar{baz="a"}"#,
    );
    f(
        r#"topk(a, foo) without (x,y) + bar{baz="a"}"#,
        r#"topk(a, foo{baz="a"}) without(x,y) + bar{baz="a"}"#,
    );
    f(
        r#"a{b="c"} + quantiles("foo", 0.1, 0.2, bar{x="y"}) by (b, x, y)"#,
        r#"a{b="c",x="y"} + quantiles("foo", 0.1, 0.2, bar{b="c",x="y"}) by(b,x,y)"#,
    );
    f(
        "sum(
					avg(foo{bar=\"one\"}) by (bar),
					avg(foo{bar=\"two\"}[1i]) by (bar)
				) by(bar)
				+ avg(foo{bar=\"three\"}) by(bar)",
        r#"sum(avg(foo{bar="one",bar="three"}) by(bar), avg(foo{bar="three",bar="two"}[1i]) by(bar)) by(bar) + avg(foo{bar="three"}) by(bar)"#,
    );
    f(
        "sum(
					foo{bar=\"one\"},
					avg(foo{bar=\"two\"}[1i]) by (bar)
				) by(bar)
				+ avg(foo{bar=\"three\"}) by(bar)",
        r#"sum(foo{bar="one",bar="three"}, avg(foo{bar="three",bar="two"}[1i]) by(bar)) by(bar) + avg(foo{bar="three"}) by(bar)"#,
    );
    f(
        r#"any(a{bar="x"}, b{bar="x",z="a"}) by (bar) + q{w="a"}"#,
        r#"any(a{bar="x"}, b{bar="x",z="a"}) by(bar) + q{bar="x",w="a"}"#,
    );

    // count_values.
    f(
        r#"count_values("foo", bar{a="b",c="d"}) by (a,x,y) + baz{foo="c",x="q",z="r"}"#,
        r#"count_values("foo", bar{a="b",c="d",x="q"}) by(a,x,y) + baz{a="b",foo="c",x="q",z="r"}"#,
    );
    f(
        r#"count_values("foo", bar{a="b",c="d"}) by (a) + baz{foo="c",x="q",z="r"}"#,
        r#"count_values("foo", bar{a="b",c="d"}) by(a) + baz{a="b",foo="c",x="q",z="r"}"#,
    );
    f(
        r#"count_values("foo", bar{a="b",c="d"}) + baz{foo="c",x="q",z="r"}"#,
        r#"count_values("foo", bar{a="b",c="d"}) + baz{foo="c",x="q",z="r"}"#,
    );

    // Transform funcs.
    f(
        r#"round(foo{bar="baz"}) + sqrt(a{z=~"c"})"#,
        r#"round(foo{bar="baz",z=~"c"}) + sqrt(a{bar="baz",z=~"c"})"#,
    );
    f(
        r#"foo{bar="baz"} + SQRT(a{z=~"c"})"#,
        r#"foo{bar="baz",z=~"c"} + SQRT(a{bar="baz",z=~"c"})"#,
    );
    f(r#"round({__name__="foo"}) + bar"#, "round(foo) + bar");
    f(
        r#"round({__name__=~"foo|bar"}) + baz"#,
        r#"round({__name__=~"foo|bar"}) + baz"#,
    );
    f(
        r#"round({__name__=~"foo|bar",a="b"}) + baz"#,
        r#"round({__name__=~"foo|bar",a="b"}) + baz{a="b"}"#,
    );
    f(
        r#"round({__name__=~"foo|bar",a="b"}) + sqrt(baz)"#,
        r#"round({__name__=~"foo|bar",a="b"}) + sqrt(baz{a="b"})"#,
    );
    f(
        r#"round(foo) + {__name__="bar",x="y"}"#,
        r#"round(foo{x="y"}) + bar{x="y"}"#,
    );
    f(
        r#"absent(foo{bar="baz"}) + sqrt(a{z=~"c"})"#,
        r#"absent(foo{bar="baz"}) + sqrt(a{z=~"c"})"#,
    );
    f(
        r#"ABSENT(foo{bar="baz"}) + sqrt(a{z=~"c"})"#,
        r#"ABSENT(foo{bar="baz"}) + sqrt(a{z=~"c"})"#,
    );
    f(
        r#"now() + foo{bar="baz"} + x{y="x"}"#,
        r#"(now() + foo{bar="baz",y="x"}) + x{bar="baz",y="x"}"#,
    );
    f(
        r#"limit_offset(5, 10, {x="y"}) if {a="b"}"#,
        r#"limit_offset(5, 10, {a="b",x="y"}) if {a="b",x="y"}"#,
    );
    f(
        r#"buckets_limit(aa, {x="y"}) if {a="b"}"#,
        r#"buckets_limit(aa, {a="b",x="y"}) if {a="b",x="y"}"#,
    );
    f(
        r#"histogram_quantiles("q", 0.1, 0.9, {x="y"}) - {a="b"}"#,
        r#"histogram_quantiles("q", 0.1, 0.9, {a="b",x="y"}) - {a="b",x="y"}"#,
    );
    f(
        r#"histogram_quantiles("q", 0.1, 0.9, sum(rate({x="y"}[5m])) by (le)) - {a="b"}"#,
        r#"histogram_quantiles("q", 0.1, 0.9, sum(rate({x="y"}[5m])) by(le)) - {a="b"}"#,
    );
    f(
        r#"histogram_quantiles("q", 0.1, 0.9, sum(rate({x="y"}[5m])) by (le,x)) - {a="b"}"#,
        r#"histogram_quantiles("q", 0.1, 0.9, sum(rate({x="y"}[5m])) by(le,x)) - {a="b",x="y"}"#,
    );
    f(
        r#"histogram_quantiles("q", 0.1, 0.9, sum(rate({x="y"}[5m])) by (le,x,a)) - {a="b"}"#,
        r#"histogram_quantiles("q", 0.1, 0.9, sum(rate({a="b",x="y"}[5m])) by(le,x,a)) - {a="b",x="y"}"#,
    );

    // vector.
    f(
        r#"vector(foo) + bar{a="b"}"#,
        r#"vector(foo{a="b"}) + bar{a="b"}"#,
    );
    f(
        r#"vector(foo{x="y"} + a) + bar{a="b"}"#,
        r#"vector(foo{a="b",x="y"} + a{a="b",x="y"}) + bar{a="b",x="y"}"#,
    );

    // labels_equal.
    f(
        r#"labels_equal(foo{x="y"}, "a", "b") + label_match(bar{q="w"}, "foo", "bar")"#,
        r#"labels_equal(foo{q="w",x="y"}, "a", "b") + label_match(bar{q="w",x="y"}, "foo", "bar")"#,
    );

    // label_set.
    f(
        r#"label_set(foo, "__name__", "bar") + x"#,
        r#"label_set(foo, "__name__", "bar") + x"#,
    );
    f(
        r#"label_set(foo{bar="baz"}, "xx", "y") + a{x="y"}"#,
        r#"label_set(foo{bar="baz",x="y"}, "xx", "y") + a{bar="baz",x="y",xx="y"}"#,
    );
    f(
        r#"label_set(foo{x="y"}, "q", "b", "x", "qwe") + label_set(bar{q="w"}, "x", "a", "q", "w")"#,
        r#"label_set(foo{x="y"}, "q", "b", "x", "qwe") + label_set(bar{q="w"}, "x", "a", "q", "w")"#,
    );
    f(
        r#"label_set(foo{a="b"}, "a", "qwe") + bar{a="x"}"#,
        r#"label_set(foo{a="b"}, "a", "qwe") + bar{a="qwe",a="x"}"#,
    );

    // alias.
    f(
        r#"alias(foo, "bar") + abc"#,
        r#"label_set(foo, "__name__", "bar") + abc"#,
    );
    f(
        r#"alias(foo, "bar") + abc{d="e"}"#,
        r#"label_set(foo{d="e"}, "__name__", "bar") + abc{d="e"}"#,
    );
    f(
        r#"alias(foo{x="y"}, "bar") + abc{d="e"}"#,
        r#"label_set(foo{d="e",x="y"}, "__name__", "bar") + abc{d="e",x="y"}"#,
    );

    // label_replace.
    f(
        r#"label_replace(foo, "a", "b", "c", "d") + bar{x="y"}"#,
        r#"label_replace(foo{x="y"}, "a", "b", "c", "d") + bar{x="y"}"#,
    );
    f(
        r#"label_replace(foo, "a", "b", "c", "d") + bar{a="y"}"#,
        r#"label_replace(foo, "a", "b", "c", "d") + bar{a="y"}"#,
    );
    f(
        r#"label_replace(foo{x="qwe"}, "a", "b", "c", "d") + bar{a="y"}"#,
        r#"label_replace(foo{x="qwe"}, "a", "b", "c", "d") + bar{a="y",x="qwe"}"#,
    );
    f(
        r#"label_replace(foo{x="qwe"}, "a", "b", "c", "d") + bar{x="y"}"#,
        r#"label_replace(foo{x="qwe",x="y"}, "a", "b", "c", "d") + bar{x="qwe",x="y"}"#,
    );
    f(
        r#"label_replace(foo{aa!="qwe"}, "a", "b", "c", "d") + bar{x="y"}"#,
        r#"label_replace(foo{aa!="qwe",x="y"}, "a", "b", "c", "d") + bar{aa!="qwe",x="y"}"#,
    );

    // label_join.
    f(
        r#"label_join(foo, "a", "b", "c") + bar{x="y"}"#,
        r#"label_join(foo{x="y"}, "a", "b", "c") + bar{x="y"}"#,
    );
    f(
        r#"label_join(foo, "a", "b", "c") + bar{a="y"}"#,
        r#"label_join(foo, "a", "b", "c") + bar{a="y"}"#,
    );
    f(
        r#"label_join(foo{a="qwe"}, "a", "b", "c") + bar{x="y"}"#,
        r#"label_join(foo{a="qwe",x="y"}, "a", "b", "c") + bar{x="y"}"#,
    );
    f(
        r#"label_join(foo{q="z"}, "a", "b", "c") + bar{a="y"}"#,
        r#"label_join(foo{q="z"}, "a", "b", "c") + bar{a="y",q="z"}"#,
    );
    f(
        r#"label_join(foo{q="z"}, "a", "b", "c") + bar{w="y"}"#,
        r#"label_join(foo{q="z",w="y"}, "a", "b", "c") + bar{q="z",w="y"}"#,
    );

    // label_map.
    f(
        r#"label_map(foo, "a", "x", "y") + bar{x="y"}"#,
        r#"label_map(foo{x="y"}, "a", "x", "y") + bar{x="y"}"#,
    );
    f(
        r#"label_map(foo{a="qwe",b="c"}, "a", "x", "y") + bar{a="rt",x="y"}"#,
        r#"label_map(foo{a="qwe",b="c",x="y"}, "a", "x", "y") + bar{a="rt",b="c",x="y"}"#,
    );

    // label_match.
    f(
        r#"label_match(foo, "a", "x", "y") + bar{x="y"}"#,
        r#"label_match(foo{x="y"}, "a", "x", "y") + bar{x="y"}"#,
    );
    f(
        r#"label_match(foo{a="qwe",b="c"}, "a", "x", "y") + bar{a="rt",x="y"}"#,
        r#"label_match(foo{a="qwe",b="c",x="y"}, "a", "x", "y") + bar{a="rt",b="c",x="y"}"#,
    );

    // label_mismatch.
    f(
        r#"label_mismatch(foo, "a", "x", "y") + bar{x="y"}"#,
        r#"label_mismatch(foo{x="y"}, "a", "x", "y") + bar{x="y"}"#,
    );
    f(
        r#"label_mismatch(foo{a="qwe",b="c"}, "a", "x", "y") + bar{a="rt",x="y"}"#,
        r#"label_mismatch(foo{a="qwe",b="c",x="y"}, "a", "x", "y") + bar{a="rt",b="c",x="y"}"#,
    );

    // label_transform.
    f(
        r#"label_transform(foo, "a", "x", "y") + bar{x="y"}"#,
        r#"label_transform(foo{x="y"}, "a", "x", "y") + bar{x="y"}"#,
    );
    f(
        r#"label_transform(foo{a="qwe",b="c"}, "a", "x", "y") + bar{a="rt",x="y"}"#,
        r#"label_transform(foo{a="qwe",b="c",x="y"}, "a", "x", "y") + bar{a="rt",b="c",x="y"}"#,
    );

    // label_copy.
    f(
        r#"label_copy(foo, "a", "b") + bar{x="y"}"#,
        r#"label_copy(foo{x="y"}, "a", "b") + bar{x="y"}"#,
    );
    f(
        r#"label_copy(foo, "a", "b", "c", "d") + bar{a="y",b="z"}"#,
        r#"label_copy(foo{a="y"}, "a", "b", "c", "d") + bar{a="y",b="z"}"#,
    );
    f(
        r#"label_copy(foo{q="w"}, "a", "b") + bar{a="y",b="z"}"#,
        r#"label_copy(foo{a="y",q="w"}, "a", "b") + bar{a="y",b="z",q="w"}"#,
    );
    f(
        r#"label_copy(foo{b="w"}, "a", "b") + bar{a="y",b="z"}"#,
        r#"label_copy(foo{a="y",b="w"}, "a", "b") + bar{a="y",b="z"}"#,
    );

    // label_del.
    f(
        r#"label_del(foo, "a", "b") + bar{x="y"}"#,
        r#"label_del(foo{x="y"}, "a", "b") + bar{x="y"}"#,
    );
    f(
        r#"label_del(foo{a="q",b="w",z="d"}, "a", "b") + bar{a="y",b="z",x="y"}"#,
        r#"label_del(foo{a="q",b="w",x="y",z="d"}, "a", "b") + bar{a="y",b="z",x="y",z="d"}"#,
    );

    // label_keep.
    f(
        r#"label_keep(foo, "a", "b") + bar{x="y"}"#,
        r#"label_keep(foo, "a", "b") + bar{x="y"}"#,
    );
    f(
        r#"label_keep(foo{a="q",c="d"}, "a", "b") + bar{x="y",b="z"}"#,
        r#"label_keep(foo{a="q",b="z",c="d"}, "a", "b") + bar{a="q",b="z",x="y"}"#,
    );

    // label_uppercase.
    f(
        r#"label_uppercase(foo, "a", "b") + bar{x="y"}"#,
        r#"label_uppercase(foo{x="y"}, "a", "b") + bar{x="y"}"#,
    );
    f(
        r#"label_uppercase(foo{a="q",b="w",z="d"}, "a", "b") + bar{a="y",b="z",x="y"}"#,
        r#"label_uppercase(foo{a="q",b="w",x="y",z="d"}, "a", "b") + bar{a="y",b="z",x="y",z="d"}"#,
    );

    // label_lowercase.
    f(
        r#"label_lowercase(foo, "a", "b") + bar{x="y"}"#,
        r#"label_lowercase(foo{x="y"}, "a", "b") + bar{x="y"}"#,
    );
    f(
        r#"label_lowercase(foo{a="q",b="w",z="d"}, "a", "b") + bar{a="y",b="z",x="y"}"#,
        r#"label_lowercase(foo{a="q",b="w",x="y",z="d"}, "a", "b") + bar{a="y",b="z",x="y",z="d"}"#,
    );

    // labels_equal.
    f(
        r#"labels_equal(foo, "a", "b") + bar{x="y"}"#,
        r#"labels_equal(foo{x="y"}, "a", "b") + bar{x="y"}"#,
    );
    f(
        r#"labels_equal(foo{a="q",b="w",z="d"}, "a", "b") + bar{a="y",b="z",x="y"}"#,
        r#"labels_equal(foo{a="q",b="w",x="y",z="d"}, "a", "b") + bar{a="y",b="z",x="y",z="d"}"#,
    );

    // label_graphite_group.
    f(
        r#"label_graphite_group(foo, 1, 2) + bar{x="y"}"#,
        r#"label_graphite_group(foo{x="y"}, 1, 2) + bar{x="y"}"#,
    );
    f(
        r#"label_graphite_group({a="b",__name__="qwe"}, 1, 2) + {__name__="abc",x="y"}"#,
        r#"label_graphite_group(qwe{a="b",x="y"}, 1, 2) + abc{a="b",x="y"}"#,
    );

    // Multilevel transform funcs.
    f("round(sqrt(foo)) + bar", "round(sqrt(foo)) + bar");
    f(
        r#"round(sqrt(foo)) + bar{b="a"}"#,
        r#"round(sqrt(foo{b="a"})) + bar{b="a"}"#,
    );
    f(
        r#"round(sqrt(foo{a="b"})) + bar{x="y"}"#,
        r#"round(sqrt(foo{a="b",x="y"})) + bar{a="b",x="y"}"#,
    );

    // Rollup funcs.
    f(
        r#"RATE(foo[5m]) / rate(baz{a="b"}) + increase(x{y="z"} offset 5i)"#,
        r#"(RATE(foo{a="b",y="z"}[5m]) / rate(baz{a="b",y="z"})) + increase(x{a="b",y="z"} offset 5i)"#,
    );
    f(
        r#"sum(rate(foo[5m])) / rate(baz{a="b"})"#,
        r#"sum(rate(foo[5m])) / rate(baz{a="b"})"#,
    );
    f(
        r#"sum(rate(foo[5m])) by (a) / rate(baz{a="b"})"#,
        r#"sum(rate(foo{a="b"}[5m])) by(a) / rate(baz{a="b"})"#,
    );
    f(
        r#"rate({__name__="foo"}) + rate({__name__="bar",x="y"}) - rate({__name__=~"baz"})"#,
        r#"(rate(foo{x="y"}) + rate(bar{x="y"})) - rate({__name__=~"baz",x="y"})"#,
    );
    f(
        r#"rate({__name__=~"foo|bar", x="y"}) + rate(baz)"#,
        r#"rate({__name__=~"foo|bar",x="y"}) + rate(baz{x="y"})"#,
    );
    f(
        r#"absent_over_time(foo{x="y"}[5m]) + bar{a="b"}"#,
        r#"absent_over_time(foo{x="y"}[5m]) + bar{a="b"}"#,
    );
    f(
        r#"{x="y"} + quantile_over_time(0.5, {a="b"})"#,
        r#"{a="b",x="y"} + quantile_over_time(0.5, {a="b",x="y"})"#,
    );
    f(
        r#"quantiles_over_time("quantile", 0.1, 0.9, foo{x="y"}[5m] offset 4h) + bar{a!="b"}"#,
        r#"quantiles_over_time("quantile", 0.1, 0.9, foo{a!="b",x="y"}[5m] offset 4h) + bar{a!="b",x="y"}"#,
    );

    // range_normalize.
    f(
        r#"range_normalize(foo{a="b",c="d"},bar{a="b",x="y"}) + baz{z="w"}"#,
        r#"range_normalize(foo{a="b",c="d",z="w"}, bar{a="b",x="y",z="w"}) + baz{a="b",z="w"}"#,
    );

    // union.
    f(
        r#"union(foo{a="b",c="d"},bar{a="b",x="y"}) + baz{z="w"}"#,
        r#"union(foo{a="b",c="d",z="w"}, bar{a="b",x="y",z="w"}) + baz{a="b",z="w"}"#,
    );
    f(
        r#"(foo{a="b",c="d"},bar{a="b",x="y"}) + baz{z="w"}"#,
        r#"(foo{a="b",c="d",z="w"}, bar{a="b",x="y",z="w"}) + baz{a="b",z="w"}"#,
    );

    // count_values_over_time.
    f(
        r#"count_values_over_time("a", foo{a="x",b="c"}[5m]) + bar{a="y",d="e"}"#,
        r#"count_values_over_time("a", foo{a="x",b="c",d="e"}[5m]) + bar{a="y",b="c",d="e"}"#,
    );

    // @ modifier.
    f(
        r#"foo @ end() + bar{baz="a"}"#,
        r#"(foo{baz="a"} @ end()) + bar{baz="a"}"#,
    );
    f(
        r#"sum(foo @ end()) + bar{baz="a"}"#,
        r#"sum(foo @ end()) + bar{baz="a"}"#,
    );
    f(
        r#"foo @ (bar{a="b"} + baz{x="y"})"#,
        r#"foo @ (bar{a="b",x="y"} + baz{a="b",x="y"})"#,
    );

    // Subqueries.
    f(
        r#"rate(avg_over_time(foo[5m:])) + bar{baz="a"}"#,
        r#"rate(avg_over_time(foo{baz="a"}[5m:])) + bar{baz="a"}"#,
    );
    f(
        r#"rate(sum(foo[5m:])) + bar{baz="a"}"#,
        r#"rate(sum(foo[5m:])) + bar{baz="a"}"#,
    );
    f(
        r#"rate(sum(foo[5m:]) by (baz)) + bar{baz="a"}"#,
        r#"rate(sum(foo{baz="a"}[5m:]) by(baz)) + bar{baz="a"}"#,
    );

    // Binary ops with constants or scalars.
    f(
        r#"100 * foo / bar{baz="a"}"#,
        r#"(100 * foo{baz="a"}) / bar{baz="a"}"#,
    );
    f(
        r#"foo * 100 / bar{baz="a"}"#,
        r#"(foo{baz="a"} * 100) / bar{baz="a"}"#,
    );
    f(
        r#"foo / bar{baz="a"} * 100"#,
        r#"(foo{baz="a"} / bar{baz="a"}) * 100"#,
    );
    f(
        r#"scalar(x) * foo / bar{baz="a"}"#,
        r#"(scalar(x) * foo{baz="a"}) / bar{baz="a"}"#,
    );
    f(
        r#"SCALAR(x) * foo / bar{baz="a"}"#,
        r#"(SCALAR(x) * foo{baz="a"}) / bar{baz="a"}"#,
    );
    f(
        r#"100 * on(foo) bar{baz="z"} + a"#,
        r#"(100 * on(foo) bar{baz="z"}) + a"#,
    );
}
