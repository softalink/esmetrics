//! Per-scrape auto-generated metrics (`up`, `scrape_duration_seconds`, ...).
//!
//! Port of `writeRequestCtx.addAutoMetrics`
//! (`lib/promscrape/scrapework.go:1010-1048`). The `sample_limit`/
//! `label_limit` conditional entries (`scrape_samples_limit`/
//! `scrape_labels_limit`) are wired in here (Task 7); the series-limiter
//! entries (`scrape_series_current`/`scrape_series_limit`/
//! `scrape_series_limit_samples_dropped`) are still deferred — see
//! [`append_auto_metrics`]'s doc.
//!
//! ## Label pipeline (upstream-faithful)
//!
//! Each auto-metric is built via `wc.addRows(cfg, dst, timestamp,
//! needRelabel=false)`, so it goes through the *same* target/external label
//! merge + [`finalize_labels`](super::scrapework::finalize_labels) pipeline
//! as a scraped row ([`super::scrapework::build_series`]), **except**:
//! - `metric_relabel` is never applied (`needRelabel` is `false`).
//! - The `isAutoMetric` `exported_` clash rename is never applied to the
//!   auto-metric's own name (that rename only fires when `needRelabel` is
//!   `true`, which auto-metrics never are).
//!
//! Net effect: an auto-metric like `up` carries `__name__=up` plus the
//! target labels (honor_labels-aware) plus the external labels
//! (honor_labels-aware, merged after finalize) — no `metric_relabel` rules
//! ever see or touch it.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use esm_protoparser::prompb::Sample;
use esm_relabel::Label;

use super::scrapework::{finalize_labels, merge_extra_labels, ScrapeConfigResolved, ScrapeResult};
use crate::series::OwnedSeries;

/// Number of auto-metrics emitted unconditionally, i.e. regardless of
/// config (everything except the `sample_limit`/`label_limit`-gated
/// entries added by [`append_auto_metrics`] below). Still the right
/// constant for tests that use a `sample_limit`/`label_limit`-disabled
/// config: those scrapes emit exactly this many auto-metric series.
pub(super) const AUTO_METRIC_COUNT: usize = 7;

/// Appends the auto-metrics to `result.series`, one sample each at
/// `scrape_timestamp_ms`, in upstream's `addAutoMetrics` emission order
/// (`scrapework.go:1019-1039`, series-limiter entries omitted — deferred,
/// see the module doc). Always appends the unconditional
/// [`AUTO_METRIC_COUNT`] entries, whether or not the scrape succeeded —
/// matching upstream's `processDataOneShot`, which calls `addAutoMetrics`
/// unconditionally after computing `am` (the `up == 0` failure path zeroes
/// `samplesScraped`/`samplesPostRelabeling`/`seriesAdded`/`responseSize`
/// *before* building `am`, rather than skipping the call). Additionally
/// appends `scrape_samples_limit` (value = `cfg.sample_limit`) when
/// `sample_limit > 0`, and `scrape_labels_limit` (value = `cfg.label_limit`)
/// when `label_limit > 0` — these advertise the *configured* limit every
/// scrape, independent of whether that scrape actually exceeded it
/// (`scrapework.go:1022-1026`/`1035-1037`).
pub(super) fn append_auto_metrics(
    result: &mut ScrapeResult,
    cfg: &ScrapeConfigResolved,
    series_added: usize,
    scrape_timestamp_ms: i64,
) {
    let up_value = if result.up { 1.0 } else { 0.0 };
    let mut entries: Vec<(&str, f64)> = Vec::with_capacity(AUTO_METRIC_COUNT + 2);
    entries.push(("scrape_duration_seconds", result.duration.as_secs_f64()));
    entries.push(("scrape_response_size_bytes", result.response_size as f64));
    if cfg.sample_limit > 0 {
        entries.push(("scrape_samples_limit", cfg.sample_limit as f64));
    }
    entries.push((
        "scrape_samples_post_metric_relabeling",
        result.samples_post_relabel as f64,
    ));
    entries.push(("scrape_samples_scraped", result.samples_scraped as f64));
    entries.push(("scrape_series_added", series_added as f64));
    if cfg.label_limit > 0 {
        entries.push(("scrape_labels_limit", cfg.label_limit as f64));
    }
    entries.push(("scrape_timeout_seconds", cfg.scrape_timeout.as_secs_f64()));
    entries.push(("up", up_value));

    result.series.reserve(entries.len());
    for (name, value) in entries {
        result
            .series
            .push(build_auto_series(name, value, cfg, scrape_timestamp_ms));
    }
}

/// Builds one auto-metric [`OwnedSeries`]: `__name__` -> merge
/// `target_labels` (honor_labels-aware) -> [`finalize_labels`] -> merge
/// `external_labels` (honor_labels-aware) -> one sample. See the module
/// doc for why `metric_relabel` is skipped.
fn build_auto_series(
    name: &str,
    value: f64,
    cfg: &ScrapeConfigResolved,
    scrape_timestamp_ms: i64,
) -> OwnedSeries {
    let mut labels = vec![Label {
        name: "__name__".to_string(),
        value: name.to_string(),
    }];
    merge_extra_labels(&mut labels, &cfg.target_labels, cfg.honor_labels);
    finalize_labels(&mut labels);
    merge_extra_labels(&mut labels, &cfg.external_labels, cfg.honor_labels);
    OwnedSeries {
        labels,
        samples: vec![Sample {
            value,
            timestamp: scrape_timestamp_ms,
        }],
    }
}

/// Order-independent identity hash of `series`'s label set (name+value
/// pairs, sorted by name then value so insertion order never matters).
/// Used by [`super::scrapework::Scraper`] to detect series that are new
/// since the previous scrape (`scrape_series_added`) and, in Task 6, series
/// that disappeared (staleness markers). Deliberately ignores sample
/// values/timestamps — series *identity* is its label set, not its data.
pub(super) fn series_identity_hash(series: &OwnedSeries) -> u64 {
    let mut sorted: Vec<&Label> = series.labels.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.value.cmp(&b.value)));
    let mut hasher = DefaultHasher::new();
    for l in sorted {
        l.name.hash(&mut hasher);
        l.value.hash(&mut hasher);
    }
    hasher.finish()
}
