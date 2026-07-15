//! Rust port of the MetricsQL parser
//! ([github.com/VictoriaMetrics/metricsql](https://github.com/VictoriaMetrics/metricsql) v0.87.1).
//!
//! MetricsQL is backwards-compatible with PromQL. The main entry point is
//! [`parse`], which returns an [`Expr`] AST with all `WITH` templates
//! expanded, exactly like the Go `metricsql.Parse` function.
//!
//! Module layout mirrors the Go reference sources:
//! - `lexer` ← `lexer.go`
//! - `ast` + `parser` + `withexpand` ← `parser.go`
//! - `binaryop` ← `binary_op.go` + `binaryop/funcs.go`
//! - `funcs` ← `aggr.go`, `rollup.go`, `transform.go`
//! - `optimizer` ← `optimizer.go`
//! - `utils` ← `utils.go`
//!
//! Intentionally skipped: `regexp_cache.go` (caching belongs to a higher
//! layer) and `prettifier.go`.

mod ast;
mod binaryop;
mod funcs;
mod lexer;
mod optimizer;
mod parser;
mod regexutil;
mod strutil;
mod utils;
mod withexpand;

pub use ast::{
    AggrFuncExpr, BinaryOpExpr, DurationExpr, Expr, FuncExpr, LabelFilter, MetricExpr,
    ModifierExpr, NumberExpr, RollupExpr, StringExpr,
};
pub use binaryop::is_binary_op_cmp;
pub use funcs::{get_rollup_arg_idx, is_aggr_func, is_rollup_func, is_transform_func};
pub use lexer::{duration_value, positive_duration_value};
pub use optimizer::{optimize, pushdown_binary_op_filters, trim_filters_by_group_modifier};
pub use parser::parse;
pub use utils::{
    expand_with_exprs, is_likely_invalid, is_supported_function, visit_all, VisitNode,
};

use std::fmt;

/// Parse error returned by [`parse`] and friends.
///
/// Mirrors the error strings produced by the Go parser loosely; carries an
/// optional byte position into the source query when it is known.
#[derive(Debug, Clone)]
pub struct ParseError {
    msg: String,
    pos: Option<usize>,
}

impl ParseError {
    pub(crate) fn new(msg: impl Into<String>) -> Self {
        ParseError {
            msg: msg.into(),
            pos: None,
        }
    }

    pub(crate) fn with_pos(msg: impl Into<String>, pos: usize) -> Self {
        ParseError {
            msg: msg.into(),
            pos: Some(pos),
        }
    }

    /// Human-readable error message.
    pub fn message(&self) -> &str {
        &self.msg
    }

    /// Byte offset into the source query where the error occurred, if known.
    pub fn pos(&self) -> Option<usize> {
        self.pos
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.pos {
            Some(pos) => write!(f, "{} (at position {})", self.msg, pos),
            None => f.write_str(&self.msg),
        }
    }
}

impl std::error::Error for ParseError {}

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, ParseError>;
