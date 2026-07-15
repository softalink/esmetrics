//! Sample de-duplication. Ports `dedup.go`'s `dedupAggr` (the sharded
//! blue/green structure collapses to a single map here, since sharding and
//! windowing don't affect the de-duplicated result).

use std::collections::HashMap;
use std::sync::Mutex;

/// A sample tagged with its full encoded series key.
#[derive(Clone)]
pub(crate) struct KeyedSample {
    pub(crate) key: Vec<u8>,
    pub(crate) value: f64,
    pub(crate) timestamp: i64,
}

/// Prometheus staleness marker bit pattern (mirrors
/// `esm_common::decimal::STALE_NAN_BITS`; inlined to keep this crate's
/// dependency surface minimal).
const STALE_NAN_BITS: u64 = 0x7ff0000000000002;

fn is_stale_nan(v: f64) -> bool {
    v.to_bits() == STALE_NAN_BITS
}

/// Returns the deduplicated `(timestamp, value)` for two samples of the same
/// series. Ports `deduplicateSamples`.
pub(crate) fn deduplicate_samples(old_t: i64, new_t: i64, old_v: f64, new_v: f64) -> (i64, f64) {
    if new_t > old_t {
        return (new_t, new_v);
    }
    if new_t == old_t {
        // Same timestamp: prefer a non-stale value, then the larger value.
        if is_stale_nan(old_v) {
            return (new_t, new_v);
        }
        if new_v > old_v {
            return (new_t, new_v);
        }
    }
    (old_t, old_v)
}

/// De-duplicates the last sample per series over a dedup interval. Upstream
/// shards the state 128 ways for concurrency; here a single map per window
/// buffer suffices since access is already serialized. The `blue`/`green`
/// buffers implement the aggregation-window double-buffering (`enable_windows`)
/// — with windows disabled only `blue` is ever used.
pub(crate) struct DedupAggr {
    blue: Mutex<HashMap<Vec<u8>, (f64, i64)>>,
    green: Mutex<HashMap<Vec<u8>, (f64, i64)>>,
    /// Counts samples dropped by de-duplication (`vm_streamaggr_dedup_dropped_samples_total`).
    dropped: &'static esm_common::metrics::Counter,
}

impl DedupAggr {
    pub(crate) fn new(dropped: &'static esm_common::metrics::Counter) -> DedupAggr {
        DedupAggr {
            blue: Mutex::new(HashMap::new()),
            green: Mutex::new(HashMap::new()),
            dropped,
        }
    }

    fn state(&self, is_green: bool) -> &Mutex<HashMap<Vec<u8>, (f64, i64)>> {
        if is_green {
            &self.green
        } else {
            &self.blue
        }
    }

    /// Ports `dedupAggr.pushSamples`. `is_green` selects the window buffer.
    pub(crate) fn push_samples(&self, samples: &[KeyedSample], is_green: bool) {
        let mut m = self.state(is_green).lock().unwrap();
        for s in samples {
            match m.get_mut(&s.key) {
                Some(entry) => {
                    let (t, v) = deduplicate_samples(entry.1, s.timestamp, entry.0, s.value);
                    entry.0 = v;
                    entry.1 = t;
                    self.dropped.inc();
                }
                None => {
                    m.insert(s.key.clone(), (s.value, s.timestamp));
                }
            }
        }
    }

    /// Drains the accumulated de-duplicated samples from the `is_green` window
    /// buffer and passes them to `f`. Ports `dedupAggr.flush`.
    pub(crate) fn flush(&self, is_green: bool, mut f: impl FnMut(&[KeyedSample])) {
        let drained = {
            let mut m = self.state(is_green).lock().unwrap();
            std::mem::take(&mut *m)
        };
        if drained.is_empty() {
            return;
        }
        let samples: Vec<KeyedSample> = drained
            .into_iter()
            .map(|(key, (value, timestamp))| KeyedSample {
                key,
                value,
                timestamp,
            })
            .collect();
        f(&samples);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_timestamp_wins() {
        assert_eq!(deduplicate_samples(10, 20, 1.0, 2.0), (20, 2.0));
    }

    #[test]
    fn same_timestamp_prefers_larger_value() {
        assert_eq!(deduplicate_samples(10, 10, 1.0, 2.0), (10, 2.0));
        assert_eq!(deduplicate_samples(10, 10, 2.0, 1.0), (10, 2.0));
    }

    #[test]
    fn same_timestamp_prefers_non_stale() {
        let stale = f64::from_bits(STALE_NAN_BITS);
        assert_eq!(deduplicate_samples(10, 10, stale, 5.0), (10, 5.0));
    }

    #[test]
    fn older_timestamp_ignored() {
        assert_eq!(deduplicate_samples(20, 10, 1.0, 2.0), (20, 1.0));
    }
}
