//! The standalone [`Deduplicator`] — de-duplicates samples per series and
//! flushes the last sample per series once per interval. Ports
//! `deduplicator.go` (windowing removed). Used for
//! `-remoteWrite.dedup.minScrapeInterval`-style HA de-duplication independent
//! of full stream aggregation.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use esm_relabel::Label;

use crate::dedup::{DedupAggr, KeyedSample};
use crate::key::{decode_labels, encode_plain};
use crate::{PushFunc, Sample, TimeSeries};

struct Inner {
    da: DedupAggr,
    drop_labels: Vec<String>,
    push_func: PushFunc,
}

impl Inner {
    fn push(&self, tss: &[TimeSeries]) {
        let mut samples: Vec<KeyedSample> = Vec::new();
        for ts in tss {
            let labels: Vec<Label> = if !self.drop_labels.is_empty() {
                ts.labels
                    .iter()
                    .filter(|l| !self.drop_labels.iter().any(|d| d == &l.name))
                    .cloned()
                    .collect()
            } else {
                ts.labels.clone()
            };
            if labels.is_empty() {
                continue;
            }
            let mut labels = labels;
            labels.sort_by(|a, b| a.name.cmp(&b.name));
            let key = encode_plain(&labels);
            for s in &ts.samples {
                samples.push(KeyedSample {
                    key: key.clone(),
                    value: s.value,
                    timestamp: s.timestamp,
                });
            }
        }
        if !samples.is_empty() {
            self.da.push_samples(&samples, false);
        }
    }

    fn flush(&self) {
        self.da.flush(false, |samples| {
            let tss: Vec<TimeSeries> = samples
                .iter()
                .map(|ps| TimeSeries {
                    labels: decode_labels(&ps.key),
                    samples: vec![Sample {
                        value: ps.value,
                        timestamp: ps.timestamp,
                    }],
                })
                .collect();
            (self.push_func)(&tss);
        });
    }
}

/// De-duplicates samples per series, flushing once per `interval`.
pub struct Deduplicator {
    inner: Arc<Inner>,
    stop: Sender<()>,
    stopped: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Deduplicator {
    /// Creates a new deduplicator flushing de-duplicated samples to
    /// `push_func` once per `interval`. `drop_labels` are removed from every
    /// series before de-duplication (e.g. HA `replica` labels). Ports
    /// `NewDeduplicator`.
    pub fn new(push_func: PushFunc, interval: Duration, drop_labels: Vec<String>) -> Deduplicator {
        let dropped = esm_common::metrics::get_or_create_counter(
            "esm_streamaggr_dedup_dropped_samples_total{name=\"dedup_global\"}",
        );
        let inner = Arc::new(Inner {
            da: DedupAggr::new(dropped),
            drop_labels,
            push_func,
        });
        let (tx, rx) = std::sync::mpsc::channel();
        let stopped = Arc::new(AtomicBool::new(false));
        let inner_thread = Arc::clone(&inner);
        let handle = std::thread::Builder::new()
            .name("esm-streamaggr-dedup".to_string())
            .spawn(move || run_flusher(&inner_thread, &rx, interval))
            .expect("spawn deduplicator flusher");
        Deduplicator {
            inner,
            stop: tx,
            stopped,
            handle: Some(handle),
        }
    }

    /// Pushes `tss` for de-duplication. Ports `Deduplicator.Push`.
    pub fn push(&self, tss: &[TimeSeries]) {
        self.inner.push(tss);
    }

    /// Stops the flusher. Ports `MustStop`.
    pub fn must_stop(&mut self) {
        if self.stopped.swap(true, Ordering::SeqCst) {
            return;
        }
        let _ = self.stop.send(());
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for Deduplicator {
    fn drop(&mut self) {
        self.must_stop();
    }
}

fn run_flusher(inner: &Inner, rx: &Receiver<()>, interval: Duration) {
    loop {
        match rx.recv_timeout(interval) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => inner.flush(),
        }
    }
}
