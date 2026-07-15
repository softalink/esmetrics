//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/dedup.go.
//!
//! The dedup algorithm is implemented once, generic over the value type:
//! query-time dedup operates on `f64` values, merge-time dedup on decimal
//! `i64` values. The semantics match Go exactly: buckets are aligned to
//! interval boundaries, the last sample in each bucket wins, and among
//! samples with identical timestamps a non-StaleNaN value is preferred,
//! then the maximum value.

use esm_common::decimal;
use std::sync::atomic::{AtomicI64, Ordering};

static GLOBAL_DEDUP_INTERVAL: AtomicI64 = AtomicI64::new(0);

/// Sets the deduplication interval in milliseconds, which is applied to raw
/// samples during data ingestion and querying. De-duplication is disabled if
/// `dedup_interval_ms` is 0. This function must be called before initializing
/// the storage. Go: SetDedupInterval (takes a time.Duration).
pub fn set_dedup_interval(dedup_interval_ms: i64) {
    GLOBAL_DEDUP_INTERVAL.store(dedup_interval_ms, Ordering::Relaxed);
}

/// Returns the dedup interval in milliseconds set via [`set_dedup_interval`].
/// Go: GetDedupInterval.
pub fn get_dedup_interval() -> i64 {
    GLOBAL_DEDUP_INTERVAL.load(Ordering::Relaxed)
}

/// Go: isDedupEnabled.
#[allow(dead_code)] // used by the stage-2 merge path (exercised in tests)
pub(crate) fn is_dedup_enabled() -> bool {
    get_dedup_interval() > 0
}

/// Value types the dedup algorithm can operate on.
trait DedupValue: Copy + PartialOrd {
    fn is_stale_nan(self) -> bool;
}

impl DedupValue for f64 {
    fn is_stale_nan(self) -> bool {
        decimal::is_stale_nan(self)
    }
}

impl DedupValue for i64 {
    fn is_stale_nan(self) -> bool {
        decimal::is_stale_nan_int64(self)
    }
}

/// Removes samples from `src_timestamps`/`src_values` if they are closer to
/// each other than `dedup_interval` in milliseconds. The vectors are
/// truncated in place (Go returns the truncated shared-backing slices).
/// Go: DeduplicateSamples.
pub fn deduplicate_samples(
    src_timestamps: &mut Vec<i64>,
    src_values: &mut Vec<f64>,
    dedup_interval: i64,
) {
    let n = deduplicate_in_place(src_timestamps, src_values, dedup_interval);
    src_timestamps.truncate(n);
    src_values.truncate(n);
}

/// Merge-time variant of [`deduplicate_samples`] operating on decimal i64
/// values. Go: deduplicateSamplesDuringMerge.
pub fn deduplicate_samples_during_merge(
    src_timestamps: &mut Vec<i64>,
    src_values: &mut Vec<i64>,
    dedup_interval: i64,
) {
    let n = deduplicate_in_place(src_timestamps, src_values, dedup_interval);
    src_timestamps.truncate(n);
    src_values.truncate(n);
}

/// Slice-level merge-time dedup used by `Block::deduplicate_samples_during_merge`,
/// where only the tail of the block's sample arrays participates.
/// Returns the new number of valid samples at the start of the slices.
#[allow(dead_code)] // used by the stage-2 merge path (exercised in tests)
pub(crate) fn deduplicate_samples_during_merge_in_place(
    timestamps: &mut [i64],
    values: &mut [i64],
    dedup_interval: i64,
) -> usize {
    deduplicate_in_place(timestamps, values, dedup_interval)
}

/// The dedup algorithm shared by the f64 and i64 variants. Compacts the
/// deduplicated samples to the front of the slices and returns their count.
///
/// Port note: Go appends to `src[:0]` while iterating `src` (shared backing
/// array); this writes each kept sample to an index strictly smaller than any
/// index still to be read, so the in-place compaction below is equivalent.
fn deduplicate_in_place<T: DedupValue>(
    timestamps: &mut [i64],
    values: &mut [T],
    dedup_interval: i64,
) -> usize {
    if !needs_dedup(timestamps, dedup_interval) {
        // Fast path - nothing to deduplicate.
        return timestamps.len();
    }
    let src_len = timestamps.len();
    let mut ts_next = timestamps[0] + dedup_interval - 1;
    ts_next -= ts_next % dedup_interval;
    let mut dst_len = 0usize;
    for i in 1..src_len {
        let ts = timestamps[i];
        if ts <= ts_next {
            continue;
        }
        let (ts_prev, v_prev) = pick_sample_at(timestamps, values, i - 1);
        timestamps[dst_len] = ts_prev;
        values[dst_len] = v_prev;
        dst_len += 1;
        ts_next += dedup_interval;
        if ts_next < ts {
            ts_next = ts + dedup_interval - 1;
            ts_next -= ts_next % dedup_interval;
        }
    }
    let (ts_prev, v_prev) = pick_sample_at(timestamps, values, src_len - 1);
    timestamps[dst_len] = ts_prev;
    values[dst_len] = v_prev;
    dst_len + 1
}

/// Chooses the value for the sample at index `j`, walking backwards over
/// samples with the identical timestamp: always prefer a non-StaleNaN value
/// (https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10196), then
/// the maximum value
/// (https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3333).
fn pick_sample_at<T: DedupValue>(timestamps: &[i64], values: &[T], mut j: usize) -> (i64, T) {
    let ts_prev = timestamps[j];
    let mut v_prev = values[j];
    while j > 0 && timestamps[j - 1] == ts_prev {
        j -= 1;
        let v = values[j];
        if v.is_stale_nan() {
            continue;
        }
        if v_prev.is_stale_nan() {
            v_prev = v;
            continue;
        }
        if v > v_prev {
            v_prev = v;
        }
    }
    (ts_prev, v_prev)
}

/// Returns true if `timestamps` contain samples closer to each other than
/// `dedup_interval`. Go: needsDedup.
pub fn needs_dedup(timestamps: &[i64], dedup_interval: i64) -> bool {
    if timestamps.len() < 2 || dedup_interval <= 0 {
        return false;
    }
    let mut ts_next = timestamps[0] + dedup_interval - 1;
    ts_next -= ts_next % dedup_interval;
    for &ts in &timestamps[1..] {
        if ts <= ts_next {
            return true;
        }
        ts_next += dedup_interval;
        if ts_next < ts {
            ts_next = ts + dedup_interval - 1;
            ts_next -= ts_next % dedup_interval;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use esm_common::decimal::STALE_NAN;

    // Port of TestNeedsDedup.
    #[test]
    fn needs_dedup_table() {
        fn f(interval: i64, timestamps: &[i64], expected: bool) {
            assert_eq!(
                needs_dedup(timestamps, interval),
                expected,
                "needsDedup({timestamps:?}, {interval})"
            );
        }
        f(-1, &[], false);
        f(-1, &[1], false);
        f(0, &[1, 2], false);
        f(10, &[1], false);
        f(10, &[1, 2], true);
        f(10, &[9, 11], false);
        f(10, &[10, 11], false);
        f(10, &[0, 10, 11], false);
        f(10, &[9, 10], true);
        f(10, &[0, 10, 19], false);
        f(10, &[9, 19], false);
        f(10, &[0, 11, 19], true);
        f(10, &[0, 11, 20], true);
        f(10, &[0, 11, 21], false);
        f(10, &[0, 19], false);
        f(10, &[0, 30, 40], false);
        f(10, &[0, 31, 40], true);
        f(10, &[0, 31, 41], false);
        f(10, &[0, 31, 49], false);
    }

    fn equal_with_nans(a: &[f64], b: &[f64]) -> bool {
        a.len() == b.len()
            && a.iter()
                .zip(b)
                .all(|(&x, &y)| (decimal::is_stale_nan(x) && decimal::is_stale_nan(y)) || x == y)
    }

    // Port of TestDeduplicateSamplesWithIdenticalTimestamps.
    #[test]
    fn deduplicate_samples_with_identical_timestamps() {
        fn f(
            dedup_interval: i64,
            timestamps: &[i64],
            values: &[f64],
            timestamps_expected: &[i64],
            values_expected: &[f64],
        ) {
            let mut ts = timestamps.to_vec();
            let mut vs = values.to_vec();
            deduplicate_samples(&mut ts, &mut vs, dedup_interval);
            assert_eq!(
                ts, timestamps_expected,
                "invalid timestamps for {timestamps:?}"
            );
            assert!(
                equal_with_nans(&vs, values_expected),
                "invalid values for {timestamps:?}; got {vs:?}; want {values_expected:?}"
            );

            // Verify that the second call doesn't modify samples.
            let ts_before = ts.clone();
            let vs_before = vs.clone();
            deduplicate_samples(&mut ts, &mut vs, dedup_interval);
            assert_eq!(ts, ts_before, "second call modified timestamps");
            assert!(
                equal_with_nans(&vs, &vs_before),
                "second call modified values"
            );
        }
        f(1000, &[1000, 1000], &[2.0, 1.0], &[1000], &[2.0]);
        f(1000, &[1001, 1001], &[2.0, 1.0], &[1001], &[2.0]);
        f(
            1000,
            &[1000, 1001, 1001, 1001, 2001],
            &[1.0, 2.0, 5.0, 3.0, 0.0],
            &[1000, 1001, 2001],
            &[1.0, 5.0, 0.0],
        );

        // verify decimal.StaleNaN is NOT preferred on timestamp conflicts
        // see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10196
        f(1000, &[1000, 1000], &[2.0, STALE_NAN], &[1000], &[2.0]);
        f(1000, &[1000, 1000], &[STALE_NAN, 2.0], &[1000], &[2.0]);
        f(
            1000,
            &[1000, 1000, 1000],
            &[1.0, STALE_NAN, 2.0],
            &[1000],
            &[2.0],
        );
        // compare with Inf values
        f(
            1000,
            &[1000, 1000],
            &[f64::INFINITY, STALE_NAN],
            &[1000],
            &[f64::INFINITY],
        );
        f(
            1000,
            &[1000, 1000, 1000],
            &[f64::INFINITY, STALE_NAN, f64::NEG_INFINITY],
            &[1000],
            &[f64::INFINITY],
        );
        f(
            1000,
            &[1000, 1000, 2000, 2000],
            &[1.0, STALE_NAN, 2.0, 3.0],
            &[1000, 2000],
            &[1.0, 3.0],
        );
        f(
            1000,
            &[1000, 1000, 2000, 2000],
            &[STALE_NAN, STALE_NAN, 2.0, 3.0],
            &[1000, 2000],
            &[STALE_NAN, 3.0],
        );
        f(
            1000,
            &[1000, 1000, 1000, 2000, 2000],
            &[1.0, STALE_NAN, 6.0, 2.0, 3.0],
            &[1000, 2000],
            &[6.0, 3.0],
        );
    }

    // Port of TestDeduplicateSamplesDuringMergeWithIdenticalTimestamps.
    #[test]
    fn deduplicate_samples_during_merge_with_identical_timestamps() {
        fn f(
            dedup_interval: i64,
            timestamps: &[i64],
            values: &[i64],
            timestamps_expected: &[i64],
            values_expected: &[i64],
        ) {
            let mut ts = timestamps.to_vec();
            let mut vs = values.to_vec();
            deduplicate_samples_during_merge(&mut ts, &mut vs, dedup_interval);
            assert_eq!(
                ts, timestamps_expected,
                "invalid timestamps for {timestamps:?}"
            );
            assert_eq!(vs, values_expected, "invalid values for {timestamps:?}");

            // Verify that the second call doesn't modify samples.
            let ts_before = ts.clone();
            let vs_before = vs.clone();
            deduplicate_samples_during_merge(&mut ts, &mut vs, dedup_interval);
            assert_eq!(ts, ts_before, "second call modified timestamps");
            assert_eq!(vs, vs_before, "second call modified values");
        }
        f(1000, &[1000, 1000], &[2, 1], &[1000], &[2]);
        f(1000, &[1001, 1001], &[2, 1], &[1001], &[2]);
        f(
            1000,
            &[1000, 1001, 1001, 1001, 2001],
            &[1, 2, 5, 3, 0],
            &[1000, 1001, 2001],
            &[1, 5, 0],
        );

        // verify decimal.StaleNaN is NOT preferred on timestamp conflicts
        // see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10196
        let (stale_nan, _) = decimal::from_float(STALE_NAN);
        f(1000, &[1000, 1000], &[2, stale_nan], &[1000], &[2]);
        f(1000, &[1000, 1000], &[stale_nan, 2], &[1000], &[2]);
        f(1000, &[1000, 1000, 1000], &[1, stale_nan, 2], &[1000], &[2]);
        // compare with max values
        f(
            1000,
            &[1000, 1000],
            &[i64::MAX, stale_nan],
            &[1000],
            &[i64::MAX],
        );
        f(
            1000,
            &[1000, 1000, 1000],
            &[i64::MAX, stale_nan, i64::MAX],
            &[1000],
            &[i64::MAX],
        );
        f(
            1000,
            &[1000, 1000, 2000],
            &[1, stale_nan, 2],
            &[1000, 2000],
            &[1, 2],
        );
        f(
            1000,
            &[1000, 1000, 2000, 2000],
            &[1, stale_nan, 2, 3],
            &[1000, 2000],
            &[1, 3],
        );
        f(
            1000,
            &[1000, 1000, 1000, 2000, 2000],
            &[1, stale_nan, i64::MAX, 2, 3],
            &[1000, 2000],
            &[i64::MAX, 3],
        );
    }

    // Port of TestDeduplicateSamples.
    #[test]
    fn deduplicate_samples_table() {
        fn f(
            dedup_interval: i64,
            timestamps: &[i64],
            timestamps_expected: &[i64],
            values_expected: &[f64],
        ) {
            let mut ts = timestamps.to_vec();
            let mut vs: Vec<f64> = (0..timestamps.len()).map(|i| i as f64).collect();
            deduplicate_samples(&mut ts, &mut vs, dedup_interval);
            assert_eq!(
                ts, timestamps_expected,
                "invalid timestamps for {timestamps:?}"
            );
            assert_eq!(vs, values_expected, "invalid values for {timestamps:?}");

            // Verify that the second call doesn't modify samples.
            let ts_before = ts.clone();
            let vs_before = vs.clone();
            deduplicate_samples(&mut ts, &mut vs, dedup_interval);
            assert_eq!(ts, ts_before);
            assert_eq!(vs, vs_before);
        }
        f(1, &[], &[], &[]);
        f(1, &[123], &[123], &[0.0]);
        f(1, &[123, 456], &[123, 456], &[0.0, 1.0]);
        f(
            1,
            &[0, 0, 0, 1, 1, 2, 3, 3, 3, 4],
            &[0, 1, 2, 3, 4],
            &[2.0, 4.0, 5.0, 8.0, 9.0],
        );
        f(
            0,
            &[0, 0, 0, 1, 1, 2, 3, 3, 3, 4],
            &[0, 0, 0, 1, 1, 2, 3, 3, 3, 4],
            &[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
        );
        f(
            100,
            &[0, 100, 100, 101, 150, 180, 205, 300, 1000],
            &[0, 100, 180, 300, 1000],
            &[0.0, 2.0, 5.0, 7.0, 8.0],
        );
        f(
            10_000,
            &[
                10_000, 13_000, 21_000, 22_000, 30_000, 33_000, 39_000, 45_000,
            ],
            &[10_000, 13_000, 30_000, 39_000, 45_000],
            &[0.0, 1.0, 4.0, 6.0, 7.0],
        );
    }

    // Port of TestDeduplicateSamplesDuringMerge.
    #[test]
    fn deduplicate_samples_during_merge_table() {
        fn f(
            dedup_interval: i64,
            timestamps: &[i64],
            timestamps_expected: &[i64],
            values_expected: &[i64],
        ) {
            let mut ts = timestamps.to_vec();
            let mut vs: Vec<i64> = (0..timestamps.len() as i64).collect();
            deduplicate_samples_during_merge(&mut ts, &mut vs, dedup_interval);
            assert_eq!(
                ts, timestamps_expected,
                "invalid timestamps for {timestamps:?}"
            );
            assert_eq!(vs, values_expected, "invalid values for {timestamps:?}");

            // Verify that the second call doesn't modify samples.
            let ts_before = ts.clone();
            let vs_before = vs.clone();
            deduplicate_samples_during_merge(&mut ts, &mut vs, dedup_interval);
            assert_eq!(ts, ts_before);
            assert_eq!(vs, vs_before);
        }
        f(1, &[], &[], &[]);
        f(1, &[123], &[123], &[0]);
        f(1, &[123, 456], &[123, 456], &[0, 1]);
        f(
            1,
            &[0, 0, 0, 1, 1, 2, 3, 3, 3, 4],
            &[0, 1, 2, 3, 4],
            &[2, 4, 5, 8, 9],
        );
        f(
            100,
            &[0, 100, 100, 101, 150, 180, 200, 300, 1000],
            &[0, 100, 200, 300, 1000],
            &[0, 2, 6, 7, 8],
        );
        f(
            10_000,
            &[
                10_000, 13_000, 21_000, 22_000, 30_000, 33_000, 39_000, 45_000,
            ],
            &[10_000, 13_000, 30_000, 39_000, 45_000],
            &[0, 1, 4, 6, 7],
        );
    }

    // Port of TestDeduplicateSamples_KeepsFirstAndLast.
    #[test]
    fn deduplicate_samples_keeps_first_and_last() {
        fn f(
            dedup_interval: i64,
            timestamps: &[i64],
            values: &[f64],
            timestamps_expected: &[i64],
            values_expected: &[f64],
        ) {
            let mut ts = timestamps.to_vec();
            let mut vs = values.to_vec();
            deduplicate_samples(&mut ts, &mut vs, dedup_interval);

            assert!(!ts.is_empty(), "deduplication removed all samples");
            assert_eq!(ts[0], timestamps[0], "first timestamp lost");
            assert!(
                equal_with_nans(&vs[..1], &values[..1]),
                "first value lost; got {:?} want {:?}",
                vs[0],
                values[0]
            );
            assert_eq!(
                ts[ts.len() - 1],
                timestamps[timestamps.len() - 1],
                "last timestamp lost"
            );
            assert!(
                equal_with_nans(&vs[vs.len() - 1..], &values[values.len() - 1..]),
                "last value lost"
            );

            assert_eq!(ts, timestamps_expected, "unexpected timestamps after dedup");
            assert!(
                equal_with_nans(&vs, values_expected),
                "unexpected values after dedup; got {vs:?}; want {values_expected:?}"
            );
        }

        // duplicates around edges
        f(
            1000,
            &[0, 200, 400, 800, 1000, 1200, 1500, 2100, 2300, 2500, 2500],
            &[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            &[0, 1000, 1500, 2500],
            &[0.0, 4.0, 6.0, 10.0],
        );

        // heavy duplication in first and last intervals
        f(
            1000,
            &[
                0, 100, 200, 300, 700, 1000, 1600, 1700, 1800, 2300, 2400, 2500,
            ],
            &[
                10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0,
            ],
            &[0, 1000, 1800, 2500],
            &[10.0, 15.0, 18.0, 21.0],
        );

        // single sample case
        f(1000, &[1000], &[42.0], &[1000], &[42.0]);

        // two samples case (different intervals, nothing to drop)
        f(1000, &[0, 2000], &[1.0, 2.0], &[0, 2000], &[1.0, 2.0]);

        // many duplicates at start
        f(
            1000,
            &[0, 100, 200, 300, 400, 500, 1500, 2000],
            &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
            &[0, 500, 2000],
            &[1.0, 6.0, 8.0],
        );

        // many duplicates at end
        f(
            1000,
            &[0, 1000, 2000, 2100, 2200, 2300, 2400, 2500],
            &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
            &[0, 1000, 2000, 2500],
            &[1.0, 2.0, 3.0, 8.0],
        );
    }

    // Port of TestDeduplicateSamplesDuringMerge_KeepsFirstAndLast.
    #[test]
    fn deduplicate_samples_during_merge_keeps_first_and_last() {
        fn f(
            dedup_interval: i64,
            timestamps: &[i64],
            values: &[i64],
            timestamps_expected: &[i64],
            values_expected: &[i64],
        ) {
            let mut ts = timestamps.to_vec();
            let mut vs = values.to_vec();
            deduplicate_samples_during_merge(&mut ts, &mut vs, dedup_interval);

            assert!(!ts.is_empty(), "deduplication removed all samples");
            assert_eq!(
                (ts[0], vs[0]),
                (timestamps[0], values[0]),
                "first sample lost"
            );
            assert_eq!(
                (ts[ts.len() - 1], vs[vs.len() - 1]),
                (timestamps[timestamps.len() - 1], values[values.len() - 1]),
                "last sample lost"
            );

            assert_eq!(ts, timestamps_expected, "unexpected timestamps after dedup");
            assert_eq!(vs, values_expected, "unexpected values after dedup");
        }

        // duplicates around edges
        f(
            1000,
            &[0, 200, 400, 800, 1000, 1300, 1500, 2100, 2400, 2500, 2500],
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            &[0, 1000, 1500, 2500],
            &[0, 4, 6, 10],
        );

        // heavy duplication in first and last intervals
        f(
            1000,
            &[
                0, 100, 200, 300, 700, 1000, 1600, 1700, 1800, 2300, 2400, 2500,
            ],
            &[10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21],
            &[0, 1000, 1800, 2500],
            &[10, 15, 18, 21],
        );

        // single sample case
        f(1000, &[1000], &[42], &[1000], &[42]);

        // two samples case
        f(1000, &[0, 2000], &[1, 2], &[0, 2000], &[1, 2]);

        // many duplicates at start
        f(
            1000,
            &[0, 100, 200, 300, 400, 500, 1500, 2000],
            &[1, 2, 3, 4, 5, 6, 7, 8],
            &[0, 500, 2000],
            &[1, 6, 8],
        );

        // many duplicates at end
        f(
            1000,
            &[0, 1000, 2000, 2100, 2200, 2300, 2400, 2500],
            &[1, 2, 3, 4, 5, 6, 7, 8],
            &[0, 1000, 2000, 2500],
            &[1, 2, 3, 8],
        );
    }
}
