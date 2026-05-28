//! PromQL parser, planner, and executor.
//!
//! Target conformance: Prometheus's upstream `promqltest` corpus at 100%.
//! MetricsQL extensions are recognised by the lexer and rejected with a
//! "not yet implemented" diagnostic; full MetricsQL parity is planned for
//! a post-v1.0 release.
//!
//! Phase 3 MVP: lexer + parser for instant vector selectors with label
//! matchers (`=`, `!=`, `=~`, `!~`), plus numeric literals. Binary
//! expressions, aggregations, function calls, subqueries, `@`/offset
//! modifiers, and the full function library follow in later sub-phases.

pub mod ast;
pub mod evaluator;
pub mod lexer;
pub mod parser;

pub use ast::{
    AggregationExpr, AggregationOp, BinaryExpr, BinaryOp, Expr, FunctionCall, GroupSide,
    GroupingClause, GroupingKind, LabelMatcher, MatchOp, MatchingGroup, SubqueryExpr, UnaryOp,
    VectorMatching, VectorMatchingKind, VectorSelector,
};
pub use evaluator::{EvalContext, EvalError, InstantVectorElement, RangeVectorElement, Value};
