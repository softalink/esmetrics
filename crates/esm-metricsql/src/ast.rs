//! MetricsQL AST types and their string serialization.
//!
//! Port of the type definitions and `AppendString` methods from `parser.go`.
//! The serialization format matches the Go implementation byte for byte —
//! it is used for cache keys and test parity.

use crate::lexer::{
    append_escaped_ident, has_escaped_chars, if_escaped_chars_append_quoted_ident, is_string_prefix,
};
use crate::regexutil::validate_regexp;
use crate::strutil::{append_quoted_string, format_float_go, go_unquote};
use crate::{binaryop, ParseError};
use std::fmt;
use std::sync::Arc;

/// A parsed MetricsQL expression.
///
/// Port of the Go `Expr` interface; the concrete node types are enum
/// variants. The [`Expr::Parens`] and [`Expr::With`] variants are internal:
/// they never appear in the output of [`crate::parse`].
#[derive(Debug, Clone)]
pub enum Expr {
    /// Metric with optional label filters, i.e. `foo{...}`.
    Metric(MetricExpr),
    /// Rollup expression, i.e. `foo[5m:3s] offset 5m @ 100`.
    Rollup(RollupExpr),
    /// Function call such as `rate(...)`.
    Func(FuncExpr),
    /// Aggregate function such as `sum(...) by (...)`.
    Aggr(AggrFuncExpr),
    /// Binary operation, i.e. `a + b`.
    BinaryOp(BinaryOpExpr),
    /// Number literal.
    Number(NumberExpr),
    /// String literal.
    String(StringExpr),
    /// Duration literal such as `5m`.
    Duration(DurationExpr),
    /// `(...)` — internal; removed by the parser after WITH expansion.
    Parens(ParensExpr),
    /// `WITH (...) expr` — internal; expanded away by the parser.
    With(WithExpr),
}

impl Expr {
    /// Appends the string representation of the expression to `dst`.
    ///
    /// Port of the Go `Expr.AppendString`.
    pub fn append_string(&self, dst: &mut String) {
        match self {
            Expr::Metric(me) => me.append_string(dst),
            Expr::Rollup(re) => re.append_string(dst),
            Expr::Func(fe) => fe.append_string(dst),
            Expr::Aggr(ae) => ae.append_string(dst),
            Expr::BinaryOp(be) => be.append_string(dst),
            Expr::Number(ne) => ne.append_string(dst),
            Expr::String(se) => se.append_string(dst),
            Expr::Duration(de) => de.append_string(dst),
            Expr::Parens(pe) => pe.append_string(dst),
            Expr::With(we) => we.append_string(dst),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::new();
        self.append_string(&mut s);
        f.write_str(&s)
    }
}

/// String expression. Port of Go `StringExpr`.
#[derive(Debug, Clone, Default)]
pub struct StringExpr {
    /// Unquoted value of the string expression.
    pub s: String,
    /// Composite string has non-empty tokens. They are converted into `s`
    /// during WITH expansion.
    pub(crate) tokens: Vec<String>,
}

impl StringExpr {
    pub(crate) fn from_string(s: impl Into<String>) -> StringExpr {
        StringExpr {
            s: s.into(),
            tokens: Vec::new(),
        }
    }

    /// Port of Go `StringExpr.AppendString`.
    pub fn append_string(&self, dst: &mut String) {
        if !self.tokens.is_empty() {
            for (i, token) in self.tokens.iter().enumerate() {
                if i > 0 {
                    dst.push('+');
                }
                dst.push_str(token);
            }
            return;
        }
        append_quoted_string(dst, &self.s);
    }
}

/// Number expression. Port of Go `NumberExpr`.
#[derive(Debug, Clone, Default)]
pub struct NumberExpr {
    /// The parsed number, i.e. `1.23`, `-234`, etc.
    pub n: f64,
    /// The original string representation of `n`; empty if the number was
    /// computed by constant folding.
    pub(crate) s: String,
}

impl NumberExpr {
    pub(crate) fn from_value(n: f64) -> NumberExpr {
        NumberExpr {
            n,
            s: String::new(),
        }
    }

    /// Port of Go `NumberExpr.AppendString`.
    pub fn append_string(&self, dst: &mut String) {
        if !self.s.is_empty() {
            dst.push_str(&self.s);
            return;
        }
        dst.push_str(&format_float_go(self.n));
    }
}

/// Duration expression such as `5m` or `-3h30m`. Port of Go `DurationExpr`.
#[derive(Debug, Clone)]
pub struct DurationExpr {
    /// String representation of the duration. It contains a valid duration
    /// if `needs_parsing` is false.
    pub(crate) s: String,
    /// Set when `s` refers to a WITH template that isn't expanded yet.
    pub(crate) needs_parsing: bool,
}

impl DurationExpr {
    /// Port of Go `newDurationExpr`: validates the duration string.
    pub(crate) fn new(s: impl Into<String>) -> Result<DurationExpr, ParseError> {
        let s = s.into();
        if let Err(err) = crate::lexer::duration_value(&s, 0) {
            return Err(ParseError::new(format!(
                "cannot parse duration {s:?}: {err}"
            )));
        }
        Ok(DurationExpr {
            s,
            needs_parsing: false,
        })
    }

    /// Port of Go `DurationExpr.AppendString`.
    pub fn append_string(&self, dst: &mut String) {
        dst.push_str(&self.s);
    }

    /// Returns the duration in milliseconds. Port of Go
    /// `DurationExpr.Duration`; panics if the duration isn't expanded yet.
    pub fn duration(&self, step: i64) -> i64 {
        assert!(
            !self.needs_parsing,
            "BUG: duration {:?} must be already parsed",
            self.s
        );
        crate::lexer::duration_value(&self.s, step)
            .unwrap_or_else(|err| panic!("BUG: cannot parse duration {:?}: {err}", self.s))
    }

    /// Returns a non-negative duration in milliseconds.
    /// Port of Go `DurationExpr.NonNegativeDuration`.
    pub fn non_negative_duration(&self, step: i64) -> Result<i64, ParseError> {
        let d = self.duration(step);
        if d < 0 {
            return Err(ParseError::new(format!(
                "unexpected negative duration {d}ms"
            )));
        }
        Ok(d)
    }
}

/// Appends an optional duration; a missing duration prints nothing,
/// matching Go's nil `*DurationExpr` behavior.
pub(crate) fn append_opt_duration(dst: &mut String, de: &Option<DurationExpr>) {
    if let Some(de) = de {
        de.append_string(dst);
    }
}

/// `(...)` expression. Port of the unexported Go `parensExpr`.
#[derive(Debug, Clone, Default)]
pub struct ParensExpr(pub Vec<Expr>);

impl ParensExpr {
    /// Port of Go `parensExpr.AppendString`.
    pub fn append_string(&self, dst: &mut String) {
        append_string_arg_list_expr(dst, &self.0);
    }
}

/// Appends `(arg1, arg2, ...)`. Port of Go `appendStringArgListExpr`.
pub(crate) fn append_string_arg_list_expr(dst: &mut String, args: &[Expr]) {
    dst.push('(');
    for (i, arg) in args.iter().enumerate() {
        if i > 0 {
            dst.push_str(", ");
        }
        arg.append_string(dst);
    }
    dst.push(')');
}

/// MetricsQL modifier such as `on (...)`, `by (...)`.
/// Port of Go `ModifierExpr`.
#[derive(Debug, Clone, Default)]
pub struct ModifierExpr {
    /// Modifier operation (`on`, `ignoring`, `by`, `without`,
    /// `group_left`, `group_right`); empty when absent.
    pub op: String,
    /// Modifier args from parens.
    pub args: Vec<String>,
}

impl ModifierExpr {
    /// Port of Go `ModifierExpr.AppendString`.
    pub fn append_string(&self, dst: &mut String) {
        dst.push_str(&self.op);
        dst.push('(');
        for (i, arg) in self.args.iter().enumerate() {
            if arg == "*" {
                dst.push('*');
            } else {
                append_escaped_ident(dst, arg);
            }
            if i + 1 < self.args.len() {
                dst.push(',');
            }
        }
        dst.push(')');
    }
}

/// Binary operation. Port of Go `BinaryOpExpr`.
#[derive(Debug, Clone)]
pub struct BinaryOpExpr {
    /// The operation itself, i.e. `+`, `-`, `*`, etc.
    pub op: String,
    /// Whether the `bool` modifier is present, e.g. `foo >bool bar`.
    pub bool_modifier: bool,
    /// Modifier such as `on` or `ignoring`.
    pub group_modifier: ModifierExpr,
    /// Modifier such as `group_left` or `group_right`.
    pub join_modifier: ModifierExpr,
    /// Optional prefix to add to labels from `group_left()`/`group_right()`
    /// lists: `group_left(foo,bar) prefix "abc"`.
    pub join_modifier_prefix: Option<StringExpr>,
    /// Whether the operation should keep metric names.
    pub keep_metric_names: bool,
    /// Left arg of the `left op right` expression.
    pub left: Box<Expr>,
    /// Right arg of the `left op right` expression.
    pub right: Box<Expr>,
}

impl BinaryOpExpr {
    /// Port of Go `BinaryOpExpr.AppendString`.
    pub fn append_string(&self, dst: &mut String) {
        if self.keep_metric_names {
            dst.push('(');
            self.append_string_no_keep_metric_names(dst);
            dst.push_str(") keep_metric_names");
        } else {
            self.append_string_no_keep_metric_names(dst);
        }
    }

    fn append_string_no_keep_metric_names(&self, dst: &mut String) {
        if self.need_left_parens() {
            append_arg_in_parens(dst, &self.left);
        } else {
            self.left.append_string(dst);
        }
        dst.push(' ');
        self.append_modifiers(dst);
        dst.push(' ');
        if self.need_right_parens() {
            append_arg_in_parens(dst, &self.right);
        } else {
            self.right.append_string(dst);
        }
    }

    fn need_left_parens(&self) -> bool {
        need_binary_op_arg_parens(&self.left)
    }

    fn need_right_parens(&self) -> bool {
        if need_binary_op_arg_parens(&self.right) {
            return true;
        }
        match &*self.right {
            Expr::Metric(me) => match me.get_metric_name() {
                Some(name) => is_reserved_binary_op_ident(name),
                None => false,
            },
            Expr::Func(fe) => {
                if is_reserved_binary_op_ident(&fe.name) {
                    return true;
                }
                fe.keep_metric_names || self.keep_metric_names
            }
            _ => false,
        }
    }

    fn append_modifiers(&self, dst: &mut String) {
        dst.push_str(&self.op);
        if self.bool_modifier {
            dst.push_str("bool");
        }
        if !self.group_modifier.op.is_empty() {
            dst.push(' ');
            self.group_modifier.append_string(dst);
        }
        if !self.join_modifier.op.is_empty() {
            dst.push(' ');
            self.join_modifier.append_string(dst);
            if let Some(prefix) = &self.join_modifier_prefix {
                dst.push_str(" prefix ");
                prefix.append_string(dst);
            }
        }
    }
}

/// Port of Go `needBinaryOpArgParens`.
fn need_binary_op_arg_parens(arg: &Expr) -> bool {
    match arg {
        Expr::BinaryOp(_) => true,
        Expr::Rollup(re) => {
            if let Expr::BinaryOp(be) = &*re.expr {
                if be.keep_metric_names {
                    return true;
                }
            }
            re.offset.is_some() || re.at.is_some()
        }
        _ => false,
    }
}

/// Port of Go `isReservedBinaryOpIdent`.
fn is_reserved_binary_op_ident(s: &str) -> bool {
    binaryop::is_binary_op_group_modifier(s)
        || binaryop::is_binary_op_join_modifier(s)
        || binaryop::is_binary_op_bool_modifier(s)
        || is_prefix_modifier(s)
}

/// Port of Go `isPrefixModifier`.
pub(crate) fn is_prefix_modifier(s: &str) -> bool {
    s.eq_ignore_ascii_case("prefix")
}

/// Port of Go `appendArgInParens`.
fn append_arg_in_parens(dst: &mut String, arg: &Expr) {
    dst.push('(');
    arg.append_string(dst);
    dst.push(')');
}

/// MetricsQL function such as `foo(...)`. Port of Go `FuncExpr`.
#[derive(Debug, Clone, Default)]
pub struct FuncExpr {
    /// Function name.
    pub name: String,
    /// Function args.
    pub args: Vec<Expr>,
    /// Whether the function should keep metric names.
    pub keep_metric_names: bool,
}

impl FuncExpr {
    /// Port of Go `FuncExpr.AppendString`.
    pub fn append_string(&self, dst: &mut String) {
        append_escaped_ident(dst, &self.name);
        append_string_arg_list_expr(dst, &self.args);
        if self.keep_metric_names {
            dst.push_str(" keep_metric_names");
        }
    }
}

/// Aggregate function such as `sum(...) by (...)`.
/// Port of Go `AggrFuncExpr`.
#[derive(Debug, Clone, Default)]
pub struct AggrFuncExpr {
    /// Function name in lowercase.
    pub name: String,
    /// Function args.
    pub args: Vec<Expr>,
    /// Optional `by (...)` / `without (...)` modifier.
    pub modifier: ModifierExpr,
    /// Optional limit for the number of output time series
    /// (MetricsQL extension): `sum(...) by (...) limit 10`.
    pub limit: i64,
}

impl AggrFuncExpr {
    /// Port of Go `AggrFuncExpr.AppendString`.
    pub fn append_string(&self, dst: &mut String) {
        append_escaped_ident(dst, &self.name);
        append_string_arg_list_expr(dst, &self.args);
        if !self.modifier.op.is_empty() {
            dst.push(' ');
            self.modifier.append_string(dst);
        }
        if self.limit > 0 {
            dst.push_str(" limit ");
            dst.push_str(&self.limit.to_string());
        }
    }
}

/// `WITH (...) expr` extension from MetricsQL.
/// Port of the unexported Go `withExpr`; internal to the parser.
#[derive(Debug, Clone)]
pub struct WithExpr {
    pub(crate) was: Vec<Arc<WithArgExpr>>,
    pub(crate) expr: Box<Expr>,
}

impl WithExpr {
    /// Port of Go `withExpr.AppendString`.
    pub fn append_string(&self, dst: &mut String) {
        dst.push_str("WITH (");
        for (i, wa) in self.was.iter().enumerate() {
            if i > 0 {
                dst.push_str(", ");
            }
            wa.append_string(dst);
        }
        dst.push_str(") ");
        self.expr.append_string(dst);
    }
}

/// A single entry inside `WITH (...)`.
/// Port of the unexported Go `withArgExpr`.
#[derive(Debug, Clone)]
pub struct WithArgExpr {
    pub(crate) name: String,
    pub(crate) args: Vec<String>,
    pub(crate) expr: Expr,
}

impl WithArgExpr {
    /// Port of Go `withArgExpr.AppendString`.
    pub fn append_string(&self, dst: &mut String) {
        append_escaped_ident(dst, &self.name);
        if !self.args.is_empty() {
            dst.push('(');
            for (i, arg) in self.args.iter().enumerate() {
                if i > 0 {
                    dst.push(',');
                }
                append_escaped_ident(dst, arg);
            }
            dst.push(')');
        }
        dst.push_str(" = ");
        self.expr.append_string(dst);
    }
}

/// MetricsQL expression with at least `offset` or `[...]` part.
/// Port of Go `RollupExpr`.
#[derive(Debug, Clone)]
pub struct RollupExpr {
    /// The expression for the rollup. Usually a [`MetricExpr`], but may be
    /// arbitrary when subqueries are used.
    pub expr: Box<Expr>,
    /// Optional window from square brackets: `http_requests_total[5m]`.
    pub window: Option<DurationExpr>,
    /// Optional offset: `foobar{baz="aa"} offset 5m`.
    pub offset: Option<DurationExpr>,
    /// Optional step from square brackets: `foobar[1h:3m]`.
    pub step: Option<DurationExpr>,
    /// If true, `foo[1h:]` is printed instead of `foo[1h]`.
    pub inherit_step: bool,
    /// Optional expression after the `@` modifier: `foo @ end()`.
    pub at: Option<Box<Expr>>,
}

impl RollupExpr {
    pub(crate) fn new(expr: Expr) -> RollupExpr {
        RollupExpr {
            expr: Box::new(expr),
            window: None,
            offset: None,
            step: None,
            inherit_step: false,
            at: None,
        }
    }

    /// Returns true if the rollup expr represents a subquery.
    /// Port of Go `RollupExpr.ForSubquery`.
    pub fn for_subquery(&self) -> bool {
        self.step.is_some() || self.inherit_step
    }

    /// Port of Go `RollupExpr.AppendString`.
    pub fn append_string(&self, dst: &mut String) {
        let need_parens = self.need_parens();
        if need_parens {
            dst.push('(');
        }
        self.expr.append_string(dst);
        if need_parens {
            dst.push(')');
        }
        self.append_modifiers(dst);
    }

    fn append_modifiers(&self, dst: &mut String) {
        if self.window.is_some() || self.inherit_step || self.step.is_some() {
            dst.push('[');
            append_opt_duration(dst, &self.window);
            if let Some(step) = &self.step {
                dst.push(':');
                step.append_string(dst);
            } else if self.inherit_step {
                dst.push(':');
            }
            dst.push(']');
        }
        if let Some(offset) = &self.offset {
            dst.push_str(" offset ");
            offset.append_string(dst);
        }
        if let Some(at) = &self.at {
            dst.push_str(" @ ");
            let need_at_parens = matches!(&**at, Expr::BinaryOp(_));
            if need_at_parens {
                dst.push('(');
            }
            at.append_string(dst);
            if need_at_parens {
                dst.push(')');
            }
        }
    }

    fn need_parens(&self) -> bool {
        match &*self.expr {
            Expr::Rollup(_) | Expr::BinaryOp(_) => true,
            Expr::Aggr(ae) => !ae.modifier.op.is_empty(),
            _ => false,
        }
    }
}

/// MetricsQL label filter such as `foo="bar"`. Port of Go `LabelFilter`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelFilter {
    /// Label name.
    pub label: String,
    /// Unquoted label value.
    pub value: String,
    /// Whether the filter is negative (`!=` or `!~`).
    pub is_negative: bool,
    /// Whether the filter is a regexp (`=~` or `!~`).
    pub is_regexp: bool,
}

impl LabelFilter {
    /// Port of Go `LabelFilter.AppendString`.
    pub fn append_string(&self, dst: &mut String) {
        append_escaped_ident(dst, &self.label);
        append_label_filter_op(dst, self.is_negative, self.is_regexp);
        append_quoted_string(dst, &self.value);
    }

    /// Port of Go `LabelFilter.isMetricNameFilter`.
    pub(crate) fn is_metric_name_filter(&self) -> bool {
        self.label == "__name__" && !self.is_negative && !self.is_regexp
    }
}

/// Port of Go `appendLabelFilterOp`.
fn append_label_filter_op(dst: &mut String, is_negative: bool, is_regexp: bool) {
    dst.push_str(match (is_negative, is_regexp) {
        (true, true) => "!~",
        (true, false) => "!=",
        (false, true) => "=~",
        (false, false) => "=",
    });
}

/// `foo <op> "bar"` filter expression prior to WITH expansion; the label may
/// contain a WITH template reference (`value` is `None` then).
///
/// Port of the unexported Go `labelFilterExpr`.
#[derive(Debug, Clone, Default)]
pub(crate) struct LabelFilterExpr {
    pub(crate) label: String,
    pub(crate) value: Option<StringExpr>,
    pub(crate) is_regexp: bool,
    pub(crate) is_negative: bool,
    pub(crate) is_possible_metric_name: bool,
}

impl LabelFilterExpr {
    /// Port of Go `labelFilterExpr.AppendString`.
    pub(crate) fn append_string(&self, dst: &mut String) {
        if_escaped_chars_append_quoted_ident(dst, &self.label);
        let Some(value) = &self.value else {
            return;
        };
        append_label_filter_op(dst, self.is_negative, self.is_regexp);
        if value.tokens.is_empty() {
            append_quoted_string(dst, &value.s);
            return;
        }
        for (i, token) in value.tokens.iter().enumerate() {
            if i > 0 {
                dst.push('+');
            }
            dst.push_str(token);
        }
    }

    /// Port of Go `labelFilterExpr.toLabelFilter`: converts an expanded
    /// filter expression into a [`LabelFilter`], validating regexps.
    pub(crate) fn to_label_filter(&self) -> Result<LabelFilter, ParseError> {
        let value = self
            .value
            .as_ref()
            .filter(|v| v.tokens.is_empty())
            .expect("BUG: lfe.value must be already expanded");
        let lf = LabelFilter {
            label: self.label.clone(),
            value: value.s.clone(),
            is_regexp: self.is_regexp,
            is_negative: self.is_negative,
        };
        if !lf.is_regexp {
            return Ok(lf);
        }
        // Verify the regexp.
        if let Err(err) = validate_regexp(&value.s) {
            return Err(ParseError::new(format!(
                "invalid regexp in {}={:?}: {err}",
                lf.label, lf.value
            )));
        }
        Ok(lf)
    }
}

/// MetricsQL metric with optional filters, i.e. `foo{...}`.
///
/// Curly braces may contain an or-delimited list of filter groups, e.g.
/// `x{job="foo",instance="bar" or job="x",instance="baz"}`.
///
/// Port of Go `MetricExpr`.
#[derive(Debug, Clone, Default)]
pub struct MetricExpr {
    /// Or-delimited groups of label filters from curly braces. The filter
    /// for the metric name (the `__name__` label) goes first in every group.
    ///
    /// Port of the Go `LabelFilterss` field.
    pub label_filterss: Vec<Vec<LabelFilter>>,
    /// Non-expanded label filters joined by the `or` operator; expanded into
    /// `label_filterss` during WITH expansion.
    ///
    /// Port of the unexported Go `labelFilterss` field.
    pub(crate) lfss_unexpanded: Vec<Vec<LabelFilterExpr>>,
}

impl MetricExpr {
    /// Creates a `MetricExpr` from expanded label filter groups.
    pub fn with_label_filterss(label_filterss: Vec<Vec<LabelFilter>>) -> MetricExpr {
        MetricExpr {
            label_filterss,
            lfss_unexpanded: Vec::new(),
        }
    }

    /// Port of Go `newMetricExpr`: a metric expression matching `name`.
    pub(crate) fn from_name(name: &str) -> MetricExpr {
        MetricExpr::with_label_filterss(vec![vec![LabelFilter {
            label: "__name__".to_string(),
            value: name.to_string(),
            is_negative: false,
            is_regexp: false,
        }]])
    }

    /// Port of Go `MetricExpr.AppendString`.
    pub fn append_string(&self, dst: &mut String) {
        if !self.lfss_unexpanded.is_empty() {
            append_label_filterss_unexpanded(dst, &self.lfss_unexpanded);
            return;
        }

        let lfss = &self.label_filterss;
        if lfss.is_empty() {
            dst.push_str("{}");
            return;
        }
        let mut offset = 0;
        if let Some(metric_name) = self.get_metric_name() {
            offset = 1;
            let metric_name = metric_name.to_string();
            append_escaped_ident(dst, &metric_name);
        }
        if self.is_only_metric_name() {
            return;
        }
        dst.push('{');
        for (i, lfs) in lfss.iter().enumerate() {
            let lfs = &lfs[offset.min(lfs.len())..];
            if lfs.is_empty() {
                continue;
            }
            append_label_filters(dst, lfs);
            if i + 1 < lfss.len() && lfss[i + 1].len() > offset {
                dst.push_str(" or ");
            }
        }
        dst.push('}');
    }

    /// Returns true if the metric expression equals `{}`.
    /// Port of Go `MetricExpr.IsEmpty`.
    pub fn is_empty(&self) -> bool {
        self.label_filterss.is_empty()
    }

    /// Port of Go `MetricExpr.isOnlyMetricName`.
    pub(crate) fn is_only_metric_name(&self) -> bool {
        if self.get_metric_name().is_none() {
            return false;
        }
        self.label_filterss.iter().all(|lfs| lfs.len() <= 1)
    }

    /// Returns the common metric name of all the or-delimited filter groups,
    /// if any. Port of Go `MetricExpr.getMetricName` (which returns `""`
    /// instead of `None`).
    pub fn get_metric_name(&self) -> Option<&str> {
        let lfss = &self.label_filterss;
        let first = lfss.first()?.first()?;
        if !first.is_metric_name_filter() {
            return None;
        }
        let metric_name = first.value.as_str();
        if metric_name.is_empty() {
            return None;
        }
        for lfs in &lfss[1..] {
            match lfs.first() {
                Some(lf) if lf.is_metric_name_filter() && lf.value == metric_name => {}
                _ => return None,
            }
        }
        Some(metric_name)
    }
}

/// Port of Go `appendLabelFilters`.
fn append_label_filters(dst: &mut String, lfs: &[LabelFilter]) {
    let Some((first, rest)) = lfs.split_first() else {
        return;
    };
    first.append_string(dst);
    for lf in rest {
        dst.push(',');
        lf.append_string(dst);
    }
}

/// Port of Go `appendLabelFilterss` (serialization of the unexpanded
/// or-delimited label filter groups).
fn append_label_filterss_unexpanded(dst: &mut String, lfss: &[Vec<LabelFilterExpr>]) {
    let mut offset = 0;
    let metric_name = get_metric_name_from_lfss(lfss).unwrap_or_default();
    let metric_name_has_escaped_chars = has_escaped_chars(&metric_name);

    if !metric_name.is_empty() {
        offset = 1;
        if !metric_name_has_escaped_chars {
            append_escaped_ident(dst, &metric_name);
        } else {
            dst.push('{');
            crate::lexer::append_quoted_ident(dst, &metric_name);
        }
    }
    if is_only_metric_name_in_lfss(lfss) {
        if metric_name_has_escaped_chars {
            dst.push('}');
        }
        return;
    }
    if !metric_name_has_escaped_chars {
        dst.push('{');
    } else {
        dst.push_str(", ");
    }
    for (i, lfs) in lfss.iter().enumerate() {
        let lfs = &lfs[offset.min(lfs.len())..];
        if lfs.is_empty() {
            continue;
        }
        for (j, lf) in lfs.iter().enumerate() {
            if j > 0 {
                dst.push(',');
            }
            lf.append_string(dst);
        }
        if i + 1 < lfss.len() && lfss[i + 1].len() > offset {
            dst.push_str(" or ");
        }
    }
    dst.push('}');
}

/// Port of Go `isOnlyMetricNameInLabelFilterss`.
fn is_only_metric_name_in_lfss(lfss: &[Vec<LabelFilterExpr>]) -> bool {
    if get_metric_name_from_lfss(lfss).is_none() {
        return false;
    }
    lfss.iter().all(|lfs| lfs.len() <= 1)
}

/// Port of Go `getMetricNameFromLabelFilterss`.
pub(crate) fn get_metric_name_from_lfss(lfss: &[Vec<LabelFilterExpr>]) -> Option<String> {
    let (first, rest) = lfss.split_first()?;
    let metric_name = must_get_metric_name(first)?;
    for lfs in rest {
        if must_get_metric_name(lfs).as_deref() != Some(metric_name.as_str()) {
            return None;
        }
    }
    Some(metric_name)
}

/// Port of Go `mustGetMetricName`; returns `None` where Go returns `""`.
fn must_get_metric_name(lfs: &[LabelFilterExpr]) -> Option<String> {
    let lf = lfs.first()?;
    let is_plain_name_filter = lf.label == "__name__"
        && !lf.is_regexp
        && !lf.is_negative
        && lf.value.as_ref().is_some_and(|v| v.tokens.len() == 1);
    if !is_plain_name_filter {
        if lf.is_possible_metric_name {
            return Some(lf.label.clone());
        }
        return None;
    }
    let token = &lf.value.as_ref().expect("checked above").tokens[0];
    let metric_name = extract_string_value(token)
        .unwrap_or_else(|err| panic!("BUG: cannot obtain metric name: {err}"));
    Some(metric_name)
}

/// Port of Go `extractStringValue`: unquotes a string literal token.
///
/// See <https://prometheus.io/docs/prometheus/latest/querying/basics/#string-literals>
pub(crate) fn extract_string_value(token: &str) -> Result<String, ParseError> {
    if !is_string_prefix(token) {
        return Err(ParseError::new(format!(
            "StringExpr must contain only string literals; got {token:?}"
        )));
    }
    if token.as_bytes()[0] == b'\'' {
        if token.len() < 2 || token.as_bytes()[token.len() - 1] != b'\'' {
            return Err(ParseError::new(format!(
                "string literal contains unexpected trailing char; got {token:?}"
            )));
        }
        let inner = &token[1..token.len() - 1];
        let transformed = inner.replace("\\'", "'").replace('"', "\\\"");
        let quoted = format!("\"{transformed}\"");
        return go_unquote(&quoted);
    }
    go_unquote(token)
}
