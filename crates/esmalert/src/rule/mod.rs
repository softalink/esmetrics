//! Rule engine for esmalert: the `Querier` abstraction over the datasource,
//! the shared `RuleError`, and rule types (recording rules here; alerting
//! rules land in a later task).
//!
//! Port of `app/vmalert/rule/` — starting with `recording.go`.

mod alert;
mod alerting;
mod executor;
mod group;
mod labels;
mod recording;
mod snapshot;
mod timing;

// Real wiring into the CLI lands in a later task; these re-exports are
// unused from `main` until then (mirrors `esmalert::datasource`'s
// `#[allow(unused_imports)]` re-exports).
#[allow(unused_imports)]
pub use alert::{
    Alert, AlertState, ALERT_FOR_STATE_METRIC, ALERT_GROUP_LABEL, ALERT_METRIC, ALERT_NAME_LABEL,
};
#[allow(unused_imports)]
pub use alerting::AlertingRule;
#[allow(unused_imports)]
pub use executor::exec_concurrently;
#[allow(unused_imports)]
pub use group::{Group, GroupHandle, RuleKind};
#[allow(unused_imports)]
pub use labels::{labels_to_key, merge_labels};
#[allow(unused_imports)]
pub use recording::RecordingRule;
#[allow(unused_imports)]
pub use snapshot::{build_snapshot, AlertView, GroupSnapshot, RuleHealth, RuleView};
#[allow(unused_imports)]
pub use timing::{adjust_req_timestamp, get_resolve_duration, DEFAULT_EVAL_DELAY};

use std::fmt;

use crate::datasource::{DsError, QueryResult};

/// Read side of the datasource as the rule evaluator needs it. Implemented by
/// the real [`crate::datasource::Datasource`] and by test mocks.
///
/// Port of `datasource.Querier` (`datasource/datasource.go`), narrowed to the
/// single instant-`Query` call recording-rule evaluation makes.
pub trait Querier {
    fn query(&self, expr: &str, ts: i64) -> Result<QueryResult, DsError>;
}

impl Querier for crate::datasource::Datasource {
    fn query(&self, expr: &str, ts: i64) -> Result<QueryResult, DsError> {
        // Resolves to the inherent `Datasource::query`, not this trait method.
        crate::datasource::Datasource::query(self, expr, ts)
    }
}

/// Error returned by rule evaluation. Never constructed from a panic; always
/// carries a human-readable message with context. Mirrors
/// [`crate::datasource::DsError`]'s shape.
#[derive(Debug)]
pub struct RuleError {
    msg: String,
}

impl RuleError {
    pub(crate) fn new(msg: impl Into<String>) -> Self {
        RuleError { msg: msg.into() }
    }
}

impl fmt::Display for RuleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for RuleError {}
