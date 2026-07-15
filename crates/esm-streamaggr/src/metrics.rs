//! Self-monitoring counters for stream aggregation, exposed through
//! `esm_common::metrics` under the `esm_streamaggr_*` prefix (the `esm_`-vs-
//! `vm_` rename every other counter in this port uses).
//!
//! Only the counter subset of upstream's `vm_streamaggr_*` metrics is
//! exposed — `esm_common::metrics` is counters-only, so the histograms
//! (`flush_duration_seconds`, `samples_lag_seconds`) and gauges
//! (dedup/labels-compressor state sizes) have no counterpart here.

use esm_common::metrics::{get_or_create_counter, Counter};

/// Per-aggregator counters, keyed by the aggregator's `name`/`position`
/// labels. Each field is a process-global `&'static Counter`.
#[derive(Clone, Copy)]
pub(crate) struct AggrMetrics {
    pub(crate) matched_samples: &'static Counter,
    pub(crate) ignored_nan_samples: &'static Counter,
    pub(crate) ignored_old_samples: &'static Counter,
    pub(crate) output_samples: &'static Counter,
    pub(crate) flush_timeouts: &'static Counter,
    /// Shared across the aggregator's `total`/`increase`/`rate` outputs.
    pub(crate) counter_resets: &'static Counter,
    /// Incremented by the de-duplicator when a duplicate sample is dropped.
    pub(crate) dedup_dropped_samples: &'static Counter,
}

impl AggrMetrics {
    /// Builds the counter set for one aggregator. `labels` is the shared
    /// label string (e.g. `name="foo",position="1"`).
    pub(crate) fn new(labels: &str) -> AggrMetrics {
        AggrMetrics {
            matched_samples: get_or_create_counter(&format!(
                "esm_streamaggr_matched_samples_total{{{labels}}}"
            )),
            ignored_nan_samples: get_or_create_counter(&format!(
                "esm_streamaggr_ignored_samples_total{{reason=\"nan\",{labels}}}"
            )),
            ignored_old_samples: get_or_create_counter(&format!(
                "esm_streamaggr_ignored_samples_total{{reason=\"too_old\",{labels}}}"
            )),
            output_samples: get_or_create_counter(&format!(
                "esm_streamaggr_output_samples_total{{{labels}}}"
            )),
            flush_timeouts: get_or_create_counter(&format!(
                "esm_streamaggr_flush_timeouts_total{{{labels}}}"
            )),
            counter_resets: get_or_create_counter(&format!(
                "esm_streamaggr_counter_resets_total{{{labels}}}"
            )),
            dedup_dropped_samples: get_or_create_counter(&format!(
                "esm_streamaggr_dedup_dropped_samples_total{{{labels}}}"
            )),
        }
    }
}
