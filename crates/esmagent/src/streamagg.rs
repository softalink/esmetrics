//! Global stream-aggregation stage (`-streamAggr.config`).
//!
//! Sits between the global relabel and the fan-out: incoming series are pushed
//! into an [`esm_streamaggr::Aggregators`], whose aggregated output is
//! forwarded to the downstream [`SeriesConsumer`] (the [`crate::fanout::Fanout`])
//! via the aggregator's push callback. Input series consumed by an aggregator
//! are dropped from the direct forward path unless `-streamAggr.keepInput` is
//! set. Ports the global stream-aggregation wiring in
//! `app/vmagent/remotewrite/remotewrite.go`.
//!
//! When `-streamAggr.config` is unset, [`build`] returns `None` and this stage
//! is absent entirely (the fan-out is the consumer directly) — zero cost.

use std::sync::Arc;

use esm_protoparser::prompb::Sample as PbSample;
use esm_streamaggr::{Aggregators, Options, PushFunc, Sample as AggSample, TimeSeries};

use crate::flags::Flags;
use crate::series::OwnedSeries;
use crate::sink::SeriesConsumer;

/// Converts esmagent's owned series into the stream-aggregator's input shape.
/// Shared with the per-URL aggregation stage in [`crate::rwctx`].
pub(crate) fn to_agg(series: &[OwnedSeries]) -> Vec<TimeSeries> {
    series
        .iter()
        .map(|s| TimeSeries {
            labels: s.labels.clone(),
            samples: s
                .samples
                .iter()
                .map(|x| AggSample {
                    timestamp: x.timestamp,
                    value: x.value,
                })
                .collect(),
        })
        .collect()
}

/// Converts the aggregator's output series back into esmagent's owned shape.
/// Shared with the per-URL aggregation stage in [`crate::rwctx`].
pub(crate) fn from_agg(tss: &[TimeSeries]) -> Vec<OwnedSeries> {
    tss.iter()
        .map(|ts| OwnedSeries {
            labels: ts.labels.clone(),
            samples: ts
                .samples
                .iter()
                .map(|x| PbSample {
                    value: x.value,
                    timestamp: x.timestamp,
                })
                .collect(),
        })
        .collect()
}

/// The built stream-aggregation stage: the consumer to install in front of
/// the fan-out, plus the shared [`Aggregators`] handle for explicit shutdown.
pub type StreamAggStage = (Arc<dyn SeriesConsumer>, Arc<Aggregators>);

/// The [`SeriesConsumer`] that aggregates then forwards. Wraps the downstream
/// fan-out consumer.
struct StreamAgg {
    downstream: Arc<dyn SeriesConsumer>,
    aggregators: Arc<Aggregators>,
    keep_input: bool,
}

impl SeriesConsumer for StreamAgg {
    fn push(&self, series: &[OwnedSeries]) {
        let tss = to_agg(series);
        // Aggregated output flows to `downstream` via the aggregators' push
        // callback (set in `build`); `match_idxs[i]` marks whether input
        // series `i` was consumed by any aggregator.
        let mut match_idxs = Vec::new();
        self.aggregators.push(&tss, &mut match_idxs);

        if self.keep_input {
            self.downstream.push(series);
            return;
        }
        // Forward only the series no aggregator consumed.
        let passthrough: Vec<OwnedSeries> = series
            .iter()
            .zip(match_idxs.iter())
            .filter(|(_, &m)| m == 0)
            .map(|(s, _)| s.clone())
            .collect();
        if !passthrough.is_empty() {
            self.downstream.push(&passthrough);
        }
    }
}

/// Builds the stream-aggregation stage if `-streamAggr.config` is set. Returns
/// the consumer wrapping `downstream` plus the shared [`Aggregators`] handle
/// (kept by the caller so it can be flushed/stopped explicitly at shutdown,
/// before the fan-out is torn down). Returns `Ok(None)` when the flag is unset.
pub fn build(
    flags: &Flags,
    downstream: Arc<dyn SeriesConsumer>,
) -> Result<Option<StreamAggStage>, String> {
    let Some(path) = &flags.stream_aggr_config else {
        return Ok(None);
    };
    let yaml = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read -streamAggr.config {path:?}: {e}"))?;

    // The aggregators' callback forwards aggregated output to the fan-out.
    let downstream_cb = Arc::clone(&downstream);
    let push_func: PushFunc = Arc::new(move |tss: &[TimeSeries]| {
        let owned = from_agg(tss);
        downstream_cb.push(&owned);
    });

    let opts = Options {
        dedup_interval_ms: flags.stream_aggr_dedup_interval.as_millis() as i64,
        drop_input_labels: flags.stream_aggr_drop_input_labels.clone(),
        keep_input: flags.stream_aggr_keep_input,
        ignore_old_samples: flags.stream_aggr_ignore_old_samples,
        ignore_first_intervals: flags.stream_aggr_ignore_first_intervals,
        flush_on_shutdown: flags.stream_aggr_flush_on_shutdown,
        enable_windows: flags.stream_aggr_enable_windows,
        ..Options::default()
    };
    let aggregators = Arc::new(
        Aggregators::load_from_data(&yaml, push_func, &opts)
            .map_err(|e| format!("invalid -streamAggr.config {path:?}: {}", e.msg))?,
    );

    let consumer = Arc::new(StreamAgg {
        downstream,
        aggregators: Arc::clone(&aggregators),
        keep_input: flags.stream_aggr_keep_input,
    }) as Arc<dyn SeriesConsumer>;
    Ok(Some((consumer, aggregators)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use esm_relabel::Label;
    use std::sync::Mutex;

    struct Cap(Mutex<Vec<OwnedSeries>>);
    impl SeriesConsumer for Cap {
        fn push(&self, s: &[OwnedSeries]) {
            self.0.lock().unwrap().extend_from_slice(s);
        }
    }

    fn series(name: &str, value: f64) -> OwnedSeries {
        OwnedSeries {
            labels: vec![Label {
                name: "__name__".into(),
                value: name.into(),
            }],
            samples: vec![PbSample {
                value,
                timestamp: 1000,
            }],
        }
    }

    #[test]
    fn build_returns_none_when_disabled() {
        let cap = Arc::new(Cap(Mutex::new(vec![]))) as Arc<dyn SeriesConsumer>;
        let flags = Flags::default();
        assert!(build(&flags, cap).unwrap().is_none());
    }

    #[test]
    fn passthrough_forwards_unmatched_and_drops_matched() {
        // Config matches only `keep_me`; aggregates it (dropping input),
        // and `pass_me` (unmatched) passes straight through.
        let dir = std::env::temp_dir();
        let cfg = dir.join("esmagent_streamagg_test.yml");
        std::fs::write(
            &cfg,
            "- interval: 1h\n  match: '{__name__=\"keep_me\"}'\n  outputs: [sum_samples]\n",
        )
        .unwrap();

        let cap = Arc::new(Cap(Mutex::new(vec![])));
        let downstream = Arc::clone(&cap) as Arc<dyn SeriesConsumer>;
        let flags = Flags {
            stream_aggr_config: Some(cfg.to_string_lossy().into_owned()),
            ..Flags::default()
        };
        let (consumer, aggregators) = build(&flags, downstream).unwrap().unwrap();

        consumer.push(&[series("keep_me", 5.0), series("pass_me", 9.0)]);
        let _ = std::fs::remove_file(&cfg);

        // Only the unmatched `pass_me` is forwarded immediately; `keep_me` is
        // held in the aggregator (1h interval, no flush yet).
        let got = cap.0.lock().unwrap();
        assert_eq!(got.len(), 1);
        assert!(got[0]
            .labels
            .iter()
            .any(|l| l.name == "__name__" && l.value == "pass_me"));
        drop(got);
        aggregators.must_stop();
    }
}
