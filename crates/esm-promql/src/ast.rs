//! PromQL AST.
//!
//! Phase 3 MVP shape: vector selectors + literals. Binary expressions,
//! aggregations, function calls, and subqueries land in subsequent
//! sub-phases. Adding new variants here is the canonical way to grow the
//! language surface.

/// Top-level PromQL expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// `42`, `1.5e9`, `-3.14`
    NumberLiteral(f64),
    /// `"foo"`, `'bar'` — only legal as a function-call argument.
    StringLiteral(String),
    /// `inner[range:step]` — evaluates `inner` at each step over the
    /// range. The result is a range vector consumable by `rate` /
    /// `*_over_time` and friends.
    Subquery(Box<SubqueryExpr>),
    /// `up{job="prometheus"}`
    VectorSelector(VectorSelector),
    /// `lhs op rhs` — arithmetic / comparison / logical binary expressions.
    Binary(BinaryExpr),
    /// `+expr`, `-expr`
    Unary(UnaryOp, Box<Expr>),
    /// `(expr)` — preserved so unparsing is loss-free.
    Paren(Box<Expr>),
    /// `sum by (job) (rate(http_requests_total[5m]))` — `sum`, `avg`, `min`,
    /// `max`, `count`, `stddev`, `stdvar`, `topk`, `bottomk`, `group`.
    Aggregation(AggregationExpr),
    /// `rate(metric[5m])`, `abs(metric)`, `time()`, etc.
    FunctionCall(FunctionCall),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregationExpr {
    pub op: AggregationOp,
    /// The expression being aggregated (typically a vector selector or
    /// function call).
    pub arg: Box<Expr>,
    /// Optional grouping clause: `by (label1, label2)` keeps only those
    /// labels; `without (label1)` keeps everything except them.
    pub grouping: Option<GroupingClause>,
    /// For `topk` / `bottomk` / `quantile`, the leading numeric parameter.
    pub param: Option<Box<Expr>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupingClause {
    pub kind: GroupingKind,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupingKind {
    By,
    Without,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Stddev,
    Stdvar,
    Group,
    Topk,
    Bottomk,
    Quantile,
    CountValues,
}

impl AggregationOp {
    /// Try to parse a function-call identifier as an aggregation op.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "sum" => Self::Sum,
            "avg" => Self::Avg,
            "min" => Self::Min,
            "max" => Self::Max,
            "count" => Self::Count,
            "stddev" => Self::Stddev,
            "stdvar" => Self::Stdvar,
            "group" => Self::Group,
            "topk" => Self::Topk,
            "bottomk" => Self::Bottomk,
            "quantile" => Self::Quantile,
            "count_values" => Self::CountValues,
            _ => return None,
        })
    }

    /// `true` if this op takes a leading parameter:
    /// `topk(N, expr)`, `quantile(0.95, expr)`, `count_values("label", expr)`.
    #[must_use]
    pub const fn takes_param(self) -> bool {
        matches!(self, Self::Topk | Self::Bottomk | Self::Quantile | Self::CountValues)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubqueryExpr {
    pub inner: Box<Expr>,
    pub range_ms: i64,
    /// Step is optional; when absent, the engine's default step is used
    /// (typically the eval `step` of the outer range query).
    pub step_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryExpr {
    pub op: BinaryOp,
    pub lhs: Box<Expr>,
    pub rhs: Box<Expr>,
    /// `bool` modifier on comparison ops forces a numeric 0/1 result
    /// instead of vector filtering.
    pub return_bool: bool,
    /// Vector-matching modifier: `on(labels)` / `ignoring(labels)`
    /// optionally followed by `group_left(labels)` / `group_right(labels)`.
    pub matching: Option<VectorMatching>,
}

/// Modifiers that govern how two instant vectors pair up.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorMatching {
    pub kind: VectorMatchingKind,
    /// Labels named in the `on(...)` or `ignoring(...)` clause.
    pub labels: Vec<String>,
    pub group: Option<MatchingGroup>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMatchingKind {
    On,
    Ignoring,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchingGroup {
    pub side: GroupSide,
    /// Extra labels from the "many" side to include on the output series.
    pub include: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupSide {
    Left,
    Right,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Unless,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Pos,
    Neg,
}

impl BinaryOp {
    /// PromQL precedence (higher = binds tighter).
    #[must_use]
    pub const fn precedence(self) -> u8 {
        match self {
            Self::Or => 1,
            Self::And | Self::Unless => 2,
            Self::Eq | Self::Ne | Self::Lt | Self::Le | Self::Gt | Self::Ge => 3,
            Self::Add | Self::Sub => 4,
            Self::Mul | Self::Div | Self::Mod => 5,
            Self::Pow => 6,
        }
    }

    /// `^` is right-associative; everything else is left-associative.
    #[must_use]
    pub const fn right_associative(self) -> bool {
        matches!(self, Self::Pow)
    }

    #[must_use]
    pub const fn is_comparison(self) -> bool {
        matches!(self, Self::Eq | Self::Ne | Self::Lt | Self::Le | Self::Gt | Self::Ge)
    }
}

/// Instant vector selector. The optional `name` field carries the
/// metric-name sugar (`http_requests_total` is equivalent to
/// `{__name__="http_requests_total"}`); when both are present the matcher
/// list also contains a `__name__=` matcher.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorSelector {
    pub name: Option<String>,
    pub matchers: Vec<LabelMatcher>,
    /// Range duration in milliseconds, if `[duration]` followed the
    /// selector. `None` for instant vector, `Some(ms)` for range vector.
    pub range_ms: Option<i64>,
    /// `metric offset 5m` shifts the eval window back by this many
    /// milliseconds (negative values shift forward).
    pub offset_ms: Option<i64>,
    /// `metric @ <epoch_seconds>` pins evaluation to a fixed timestamp.
    pub at_timestamp_sec: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchOp {
    /// `name="value"`
    Equal,
    /// `name!="value"`
    NotEqual,
    /// `name=~"regex"`
    RegexMatch,
    /// `name!~"regex"`
    RegexNotMatch,
}

impl MatchOp {
    /// Render the operator the way it appears in PromQL source.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::RegexMatch => "=~",
            Self::RegexNotMatch => "!~",
        }
    }
}
