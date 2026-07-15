//! Rust port of the upstream VictoriaMetrics v1.146.0 PromQL/MetricsQL query evaluator
//! (`app/vmselect/promql`), Stage 1.
//!
//! The evaluator runs against an abstract data source defined by the
//! [`MetricsProvider`] trait (see [`provider`]); storage integration comes in
//! Stage 2. Module layout mirrors the Go reference sources:
//! - `eval` ← `eval.go`
//! - `rollup` + `rollup_funcs` ← `rollup.go`
//! - `aggr` ← `aggr.go`
//! - `aggr_incremental` ← `aggr_incremental.go`
//! - `binary_op` ← `binary_op.go`
//! - `transform` ← `transform.go` (Stage-1 subset)
//! - `timeseries` ← `timeseries.go`
//! - `exec` + `parse_cache` ← `exec.go` + `parse_cache.go`
//! - `memory_limiter` ← `memory_limiter.go`
//!
//! - `rollup_result_cache` ← `rollup_result_cache.go` (in-memory)
//!
//! PORT-SKIP (Stage 2):
//! subqueries (`evalRollupFuncWithSubquery`), instant-rollup optimizations
//! (`evalInstantRollup`), binary-op common-filter pushdown, `aggr_over_time`
//! and the `rollup*` multi-config functions, query tracing and query stats.

pub mod aggr;
pub mod aggr_incremental;
pub mod binary_op;
pub mod eval;
pub mod eval_rollup;
pub mod exec;
pub mod memory_limiter;
pub mod parse_cache;
pub mod provider;
pub mod rollup;
pub mod rollup_funcs;
pub mod rollup_result_cache;
pub mod timeseries;
pub mod transform;
mod worker_pool;

pub use eval::EvalConfig;
pub use exec::{exec, QueryResult};
pub use provider::{Deadline, MetricsProvider, SearchQuery, Series};
pub use rollup_result_cache::reset_rollup_result_cache;
pub use timeseries::Timeseries;

use std::fmt;

/// Error returned by query evaluation.
///
/// The Go implementation returns wrapped `error` chains; Stage 1 keeps a
/// simple message-carrying error.
#[derive(Debug, Clone)]
pub struct Error {
    msg: String,
}

impl Error {
    pub fn new(msg: impl Into<String>) -> Self {
        Error { msg: msg.into() }
    }

    /// Human-readable error message.
    pub fn message(&self) -> &str {
        &self.msg
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for Error {}

impl From<esm_metricsql::ParseError> for Error {
    fn from(err: esm_metricsql::ParseError) -> Self {
        Error::new(err.to_string())
    }
}

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;
