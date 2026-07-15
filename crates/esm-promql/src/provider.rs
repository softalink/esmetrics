//! Abstract data-source boundary between the evaluator and storage.
//!
//! Mirrors what `eval.go` builds from a `metricsql.MetricExpr`
//! (`storage.NewSearchQuery` + `netstorage.ProcessSearchQuery`): the
//! evaluator asks for all raw samples of the series matching the given tag
//! filters on `[start..end]` and receives per-series sorted samples.
//!
//! Stage 2 implements [`MetricsProvider`] over `esm-storage`
//! (`ProcessSearchQuery` + block unpacking + merge + dedup); tests use an
//! in-memory fake.

use crate::{Error, Result};
use esm_metricsql::LabelFilter;
use esm_storage::metric_name::MetricName;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Query deadline. Port of the relevant subset of `searchutil.Deadline`.
///
/// A zero deadline means "no deadline". The Go implementation polls a coarse
/// 1s-cached clock; Stage 1 uses `SystemTime` directly — `exceeded()` is
/// called once per series, which is cheap enough.
#[derive(Debug, Clone, Copy, Default)]
pub struct Deadline {
    /// Unix timestamp in milliseconds; 0 means no deadline.
    deadline_ms: i64,
}

impl Deadline {
    /// A deadline `timeout` from now.
    pub fn from_timeout(timeout: Duration) -> Self {
        Deadline {
            deadline_ms: now_unix_ms() + timeout.as_millis() as i64,
        }
    }

    /// No deadline.
    pub fn none() -> Self {
        Deadline::default()
    }

    /// Returns true if the deadline is exceeded.
    pub fn exceeded(&self) -> bool {
        self.deadline_ms > 0 && now_unix_ms() > self.deadline_ms
    }

    /// The raw deadline as a unix timestamp in milliseconds (0 = none).
    /// Used by storage adapters that express deadlines in unix seconds.
    pub fn deadline_unix_ms(&self) -> i64 {
        self.deadline_ms
    }

    /// Returns an error if the deadline is exceeded.
    pub(crate) fn check(&self) -> Result<()> {
        if self.exceeded() {
            return Err(Error::new("the deadline for the query has been exceeded"));
        }
        Ok(())
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Search query passed to [`MetricsProvider::search`].
///
/// Mirror of `storage.SearchQuery` as built by `evalRollupFuncNoCache`
/// (eval.go): the time range is `[start..end]` in milliseconds (inclusive,
/// `start` already extended by the lookbehind window / silence interval),
/// `tag_filterss` is an or-delimited list of and-delimited label filter
/// groups, and `max_metrics` limits the number of matching series
/// (0 = no limit).
#[derive(Debug, Clone)]
pub struct SearchQuery {
    /// Minimum timestamp (`minTimestamp` in Go), milliseconds, inclusive.
    pub start: i64,
    /// Maximum timestamp (`ec.End` in Go), milliseconds, inclusive.
    pub end: i64,
    /// Or-delimited groups of and-delimited label filters. A series matches
    /// the query if it matches at least one group.
    pub tag_filterss: Vec<Vec<LabelFilter>>,
    /// The maximum number of series the query may match; 0 means no limit.
    pub max_metrics: usize,
}

/// A single raw series returned from [`MetricsProvider::search`].
///
/// Mirror of `netstorage.Result`: raw samples sorted by timestamp
/// (ascending, deduplicated), restricted to the query time range.
/// `values.len() == timestamps.len()`.
#[derive(Debug, Clone, Default)]
pub struct Series {
    pub metric_name: MetricName,
    pub timestamps: Arc<Vec<i64>>,
    pub values: Vec<f64>,
}

/// Abstract data source for the evaluator.
///
/// Stage-2 note: the Go pipeline streams blocks and unpacks them in parallel
/// workers (`netstorage.Results.RunParallel`); this trait returns fully
/// unpacked series instead, and the evaluator fans the per-series rollup work
/// out over its own worker threads. When the streaming pipeline lands, the
/// trait can grow a visitor-style API without changing evaluator semantics.
pub trait MetricsProvider: Send + Sync {
    /// Returns all series matching `sq` with their raw samples on
    /// `[sq.start..sq.end]`, sorted by timestamp.
    fn search(&self, sq: &SearchQuery, deadline: Deadline) -> Result<Vec<Series>>;
}

/// A provider with no data; useful for evaluating storage-independent
/// expressions (`time()`, arithmetic, ...).
#[derive(Debug, Clone, Copy, Default)]
pub struct EmptyProvider;

impl MetricsProvider for EmptyProvider {
    fn search(&self, _sq: &SearchQuery, _deadline: Deadline) -> Result<Vec<Series>> {
        Ok(Vec::new())
    }
}
