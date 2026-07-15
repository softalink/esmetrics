//! AST traversal and misc public helpers.
//!
//! Port of `utils.go`.

use crate::ast::{DurationExpr, Expr, ModifierExpr, StringExpr};
use crate::funcs::get_rollup_arg_idx;
use crate::parser::parse;
use crate::{ParseError, Result};

/// A node passed to the [`visit_all`] callback.
///
/// The Go `VisitAll` passes `Expr` interface values, which include
/// `*ModifierExpr`, `*DurationExpr` and `*StringExpr` nodes; this enum plays
/// the same role.
#[derive(Debug, Clone, Copy)]
pub enum VisitNode<'a> {
    /// A regular expression node.
    Expr(&'a Expr),
    /// A modifier such as `on(...)`, `by(...)` or `group_left(...)`.
    Modifier(&'a ModifierExpr),
    /// A duration (rollup window, step or offset).
    Duration(&'a DurationExpr),
    /// A string (the `group_left() prefix "..."` value).
    Str(&'a StringExpr),
}

impl VisitNode<'_> {
    /// Appends the string representation of the node to `dst`.
    pub fn append_string(&self, dst: &mut String) {
        match self {
            VisitNode::Expr(e) => e.append_string(dst),
            VisitNode::Modifier(me) => me.append_string(dst),
            VisitNode::Duration(de) => de.append_string(dst),
            VisitNode::Str(se) => se.append_string(dst),
        }
    }
}

/// Recursively calls `f` for all the children in `e`, leaf children first,
/// parents afterwards.
///
/// Port of Go `VisitAll` (which allows modifying nodes in `f`; this port is
/// read-only).
pub fn visit_all<'a>(e: &'a Expr, f: &mut dyn FnMut(VisitNode<'a>)) {
    match e {
        Expr::BinaryOp(be) => {
            visit_all(&be.left, f);
            visit_all(&be.right, f);
            f(VisitNode::Modifier(&be.group_modifier));
            f(VisitNode::Modifier(&be.join_modifier));
            if let Some(prefix) = &be.join_modifier_prefix {
                f(VisitNode::Str(prefix));
            }
        }
        Expr::Func(fe) => {
            for arg in &fe.args {
                visit_all(arg, f);
            }
        }
        Expr::Aggr(ae) => {
            for arg in &ae.args {
                visit_all(arg, f);
            }
            f(VisitNode::Modifier(&ae.modifier));
        }
        Expr::Rollup(re) => {
            visit_all(&re.expr, f);
            if let Some(window) = &re.window {
                f(VisitNode::Duration(window));
            }
            if let Some(step) = &re.step {
                f(VisitNode::Duration(step));
            }
            if let Some(offset) = &re.offset {
                f(VisitNode::Duration(offset));
            }
            if let Some(at) = &re.at {
                visit_all(at, f);
            }
        }
        _ => {}
    }
    f(VisitNode::Expr(e));
}

/// Expands WITH expressions inside `q` and returns the resulting PromQL
/// without WITH expressions.
///
/// Port of Go `ExpandWithExprs`.
pub fn expand_with_exprs(q: &str) -> Result<String> {
    let e = parse(q)?;
    Ok(e.to_string())
}

/// Returns true if `e` contains a tricky implicit conversion, which is
/// invalid most of the time, e.g. `rate(sum(foo))` or `rate(foo + bar)`.
///
/// See <https://docs.victoriametrics.com/victoriametrics/metricsql/#implicit-query-conversions>
///
/// Port of Go `IsLikelyInvalid`.
pub fn is_likely_invalid(e: &Expr) -> bool {
    let mut has_implicit_conversion = false;
    visit_all(e, &mut |node| {
        if has_implicit_conversion {
            return;
        }
        let VisitNode::Expr(Expr::Func(fe)) = node else {
            return;
        };
        if fe.name == "timestamp" {
            // In Prometheus, timestamp is a transform function on instant
            // vectors, but its behavior is closer to a rollup.
            // The upstream defines timestamp as a rollup function; to
            // remain consistent with Prometheus, is_likely_invalid doesn't
            // treat timestamp(sum(foo)) as an implicit conversion.
            // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/9527
            return;
        }

        let Some(idx) = get_rollup_arg_idx(fe) else {
            return;
        };
        if idx >= fe.args.len() {
            return;
        }
        match &fe.args[idx] {
            Expr::Rollup(re) => {
                if matches!(&*re.expr, Expr::Metric(_)) {
                    return;
                }
                if re.window.is_none() {
                    has_implicit_conversion = true;
                }
            }
            Expr::Metric(_) => {}
            _ => {
                has_implicit_conversion = true;
            }
        }
    });
    has_implicit_conversion
}

/// Returns true if `func_name` is a supported MetricsQL function.
///
/// Port of Go `IsSupportedFunction`.
pub fn is_supported_function(func_name: &str) -> bool {
    crate::funcs::is_rollup_func(func_name)
        || crate::funcs::is_transform_func(func_name)
        || crate::funcs::is_aggr_func(func_name)
}

/// Port of Go `checkSupportedFunctions`.
pub(crate) fn check_supported_functions(e: &Expr) -> Result<()> {
    let mut err: Option<ParseError> = None;
    visit_all(e, &mut |node| {
        if err.is_some() {
            return;
        }
        if let VisitNode::Expr(Expr::Func(fe)) = node {
            if !is_supported_function(&fe.name) {
                err = Some(ParseError::new(format!(
                    "unsupported function {:?}",
                    fe.name
                )));
            }
        }
    });
    match err {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Port of TestExpandWithExprsSuccess.
    #[test]
    fn expand_with_exprs_success() {
        let f = |q: &str, q_expected: &str| {
            for _ in 0..3 {
                let q_expanded = expand_with_exprs(q)
                    .unwrap_or_else(|err| panic!("unexpected error when expanding {q:?}: {err}"));
                assert_eq!(
                    q_expanded, q_expected,
                    "unexpected expanded expression for {q:?}"
                );
            }
        };
        f("1", "1");
        f("foobar", "foobar");
        f("with (x = 1) x+x", "2");
        f("with (f(x) = x*x) 3+f(2)+2", "9");
    }

    // Port of TestExpandWithExprsError.
    #[test]
    fn expand_with_exprs_error() {
        let f = |q: &str| {
            for _ in 0..3 {
                assert!(
                    expand_with_exprs(q).is_err(),
                    "expecting non-nil error when expanding {q:?}"
                );
            }
        };
        f("");
        f("  with (");
    }

    // Port of TestVisitAll.
    #[test]
    fn visit_all_order() {
        let f = |q: &str, s_expected: &str| {
            let expr = parse(q).unwrap_or_else(|err| panic!("unexpected error: {err}"));
            let mut buf = String::new();
            visit_all(&expr, &mut |node| {
                node.append_string(&mut buf);
                buf.push(',');
            });
            assert_eq!(buf, s_expected, "unexpected result for {q:?}");
        };
        f("123", "123,");
        f("1+2", "3,");
        f("1+a", "1,a,(),(),1 + a,");
        f(
            "avg(a<b+1, sum(x) by (y))",
            "a,b,1,(),(),b + 1,(),(),a < (b + 1),x,by(y),sum(x) by(y),(),avg(a < (b + 1), sum(x) by(y)),",
        );
        f(
            "a + on() group_left() prefix \"foo\" b",
            "a,b,on(),group_left(),\"foo\",a + on() group_left() prefix \"foo\" b,",
        );
        f("x[1s]", "x,1s,x[1s],");
        f(
            "x[1h:5m] offset 5s @ 10s",
            "x,1h,5m,5s,10s,x[1h:5m] offset 5s @ 10s,",
        );
    }

    // Port of TestIsLikelyInvalid.
    #[test]
    fn is_likely_invalid_cases() {
        let f = |q: &str, result_expected: bool| {
            let expr = parse(q).unwrap_or_else(|err| panic!("unexpected error: {err}"));
            let result = is_likely_invalid(&expr);
            assert_eq!(
                result, result_expected,
                "unexpected result for is_likely_invalid({q:?})"
            );
        };

        f("1", false);
        f(r#"foo{bar="baz"}"#, false);

        // This should be OK, since it is easy to reason about.
        f("rate(foo)", false);
        f("timestamp(foo)", false);
        f("foo[5m]", false);
        f("1 + foo[5m]", false);

        f("rate(foo[5s])", false);
        f(r#"rate(foo{bar=~"baz"}[5s])"#, false);
        f(r#"rate(foo{bar=~"baz"}[5s] offset 1h)"#, false);

        // Explicit subqueries are allowed.
        f("sum_over_time((up > 0)[5m:1s])", false);
        f("rate(sum(foo)[5m])", false);
        f("rate(sum(foo)[5m:3s])", false);

        // Implicit step in the subquery is OK.
        f("sum_over_time((up > 0)[5m])", false);

        // This is OK, since it is supported by Prometheus.
        f(r#"rate(foo{bar=~"baz"}[5m:1s])"#, false);
        f(r#"rate(foo{bar=~"baz"}[5m:1s] offset 1h)"#, false);
        f("timestamp(sum(foo))", false);

        f("sum(foo)", false);
        f("sum(rate(foo))", false);
        f("abs(foo)", false);
        f("sum(abs(foo))", false);

        // This isn't OK, since these queries work unexpectedly most of the
        // time.
        f("rate(sum(foo))", true);
        f("rate(abs(foo))", true);
        f("rate(1)", true);
        f("rate(foo + bar)", true);
        f("rate(rate(foo))", true);
        f(r#"1 + rate(label_set(foo, "bar", "baz"))"#, true);
        f("rate(sum(foo) offset 5m)", true);

        // Invalid number of args.
        f("quantile_over_time(foo)", false);
    }

    // Port of TestIsSupportedFunction.
    #[test]
    fn is_supported_function_cases() {
        let f = |s: &str, expected_result: bool| {
            assert_eq!(
                is_supported_function(s),
                expected_result,
                "unexpected result for is_supported_function({s:?})"
            );
        };

        // An empty function name is a synonym to union().
        f("", true);
        f("union", true);

        // Rollup functions.
        f("rate", true);
        f("RATE", true);
        f("Increase", true);

        // Transform functions.
        f("ceil", true);
        f("histogram_QUANTILe", true);

        // Aggregate functions.
        f("sum", true);
        f("aVG", true);

        // Unknown functions.
        f("foo", false);
        f("BAR", false);
    }
}
