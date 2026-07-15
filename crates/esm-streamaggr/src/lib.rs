//! Streaming aggregation engine — a Rust port of
//! `github.com/VictoriaMetrics/lib/streamaggr` (VictoriaMetrics v1.146.0).
//!
//! Stream aggregation continuously aggregates incoming samples in memory,
//! grouped by an output label set, and flushes the aggregated series to a
//! push callback once per configured `interval`. It powers vmagent's
//! `-remoteWrite.streamAggr.config` and the single-node
//! `-streamAggr.config`.
//!
//! # Scope of this port
//!
//! Ported: the full config surface (`interval`, `by`/`without`,
//! `dedup_interval`, `staleness_interval`, `ignore_first_sample_interval`,
//! `keep_metric_names`, `ignore_old_samples`, `ignore_first_intervals`,
//! `flush_on_shutdown`, `no_align_flush_to_interval`, `drop_input_labels`,
//! `input_relabel_configs`, `output_relabel_configs`, `match`,
//! `enable_windows`), all 18 output functions, sample de-duplication, the
//! blue/green aggregation-window mode, the standalone [`Deduplicator`], and
//! the `esm_streamaggr_*` self-monitoring counters.
//!
//! Deliberately deferred (documented in `README.md`):
//! - The self-monitoring *histograms* and *gauges* (`vm_streamaggr_*` flush
//!   durations, sample lag, dedup state sizes) — `esm_common::metrics` is
//!   counters-only, so only the counter subset is exposed (see
//!   [`mod@metrics`]).
//! - The label-dictionary compressor (`promutil.LabelsCompressor`): the
//!   per-series key is an in-memory-only grouping key that never leaves the
//!   process, so this port uses a straightforward round-tripping label
//!   encoding instead (algorithmic fidelity, not byte-format fidelity — see
//!   the porting rules).

mod aggregator;
mod config;
mod dedup;
mod deduplicator;
mod godur;
mod histogram;
mod key;
mod metrics;
mod outputs;

#[cfg(test)]
mod tests;

pub use aggregator::Aggregators;
pub use config::Options;
pub use deduplicator::Deduplicator;
pub use esm_relabel::Label;

/// A single `(timestamp_ms, value)` sample. Mirrors `prompb.Sample`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Sample {
    /// Unix timestamp in milliseconds.
    pub timestamp: i64,
    /// Sample value.
    pub value: f64,
}

/// A labelled series with its samples. Mirrors `prompb.TimeSeries`.
#[derive(Clone, Debug, PartialEq)]
pub struct TimeSeries {
    /// Series labels (including `__name__`).
    pub labels: Vec<Label>,
    /// Samples for this series.
    pub samples: Vec<Sample>,
}

/// Callback invoked by the aggregator when it flushes aggregated series.
///
/// Must be cheap-to-clone and safe to call from the background flusher
/// thread and from `Push` callers concurrently.
pub type PushFunc = std::sync::Arc<dyn Fn(&[TimeSeries]) + Send + Sync>;

/// Error returned while loading a stream-aggregation config.
#[derive(Debug)]
pub struct Error {
    /// Human-readable failure description.
    pub msg: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for Error {}

impl Error {
    pub(crate) fn new(msg: impl Into<String>) -> Error {
        Error { msg: msg.into() }
    }
}
