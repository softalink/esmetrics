//! Rollup core: `rollupConfig`, the `doInternal` window loop, scrape-interval
//! inference and the pre-functions. Port of the corresponding parts of
//! `rollup.go`.

use crate::rollup_funcs::{self, RollupFunc};
use crate::timeseries::validate_max_points_per_series;
use crate::{Error, Result};
use esm_metricsql::Expr;
use std::sync::Arc;

/// `-search.minStalenessInterval` analog; 0 by default (Stage 1 keeps it a
/// constant; make it configurable when the flag surface lands in esm-select).
pub const MIN_STALENESS_INTERVAL: i64 = 0;

/// Port of Go `maxSilenceInterval()`: the extra lookbehind fetched for
/// functions that need the sample preceding the window; 5m by default.
pub fn max_silence_interval() -> i64 {
    if MIN_STALENESS_INTERVAL > 0 {
        MIN_STALENESS_INTERVAL
    } else {
        5 * 60 * 1000
    }
}

/// Argument passed to every rollup function invocation.
/// Port of Go `rollupFuncArg`.
pub struct RollupFuncArg<'a> {
    /// The value preceding `values` if it fits the staleness interval.
    pub prev_value: f64,
    /// The timestamp for `prev_value`.
    pub prev_timestamp: i64,
    /// Values that fit the window ending at `curr_timestamp`. NaN-free by
    /// contract (stale markers and NaNs are dropped by the driver).
    pub values: &'a [f64],
    /// Timestamps for `values`.
    pub timestamps: &'a [i64],
    /// Real value preceding `values` regardless of the staleness interval,
    /// gated by `LookbackDelta`.
    pub real_prev_value: f64,
    /// Real value which goes after `values`.
    pub real_next_value: f64,
    /// Current timestamp for the rollup evaluation (window right edge).
    pub curr_timestamp: i64,
    /// Index of the currently evaluated point in the output grid.
    pub idx: usize,
    /// Time window for the rollup calculation.
    pub window: i64,
}

impl Default for RollupFuncArg<'_> {
    fn default() -> Self {
        RollupFuncArg {
            prev_value: f64::NAN,
            prev_timestamp: 0,
            values: &[],
            timestamps: &[],
            real_prev_value: f64::NAN,
            real_next_value: f64::NAN,
            curr_timestamp: 0,
            idx: 0,
            window: 0,
        }
    }
}

/// Pre-function applied once per raw series before windowing
/// (e.g. `removeCounterResets` for `rate`). Values may be mutated in place;
/// timestamps are read-only.
pub type PreFunc = Arc<dyn Fn(&mut [f64], &[i64]) + Send + Sync>;

/// One rollup configuration shared across all input series.
/// Port of Go `rollupConfig`.
#[derive(Clone)]
pub struct RollupConfig {
    /// This tag value must be added to the "rollup" tag if non-empty
    /// (`rollup_candlestick` etc.; unused in Stage 1).
    pub tag_value: String,
    pub func: RollupFunc,
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub window: i64,
    /// The maximum number of points which can be generated per series.
    pub max_points_per_series: usize,
    /// Whether the window may be adjusted to 2x the interval between data
    /// points (rate/deriv-style functions).
    pub may_adjust_window: bool,
    /// The shared output grid.
    pub timestamps: Arc<Vec<i64>>,
    /// Analog to `-query.lookback-delta` from Prometheus.
    pub lookback_delta: i64,
    /// Whether `default_rollup` is used.
    pub is_default_rollup: bool,
    /// The estimated number of samples scanned per func call (cost model
    /// only). Zero means the func scans all samples passed to it.
    pub samples_scanned_per_call: usize,
}

impl RollupConfig {
    /// Calculates rollups for the given timestamps and values, appends them
    /// to `dst_values` and returns `(dst_values, samples_scanned)`.
    ///
    /// It is expected that `timestamps` cover the time range
    /// `[start - window .. end]`. Port of Go `rollupConfig.Do`.
    pub fn exec(
        &self,
        dst_values: Vec<f64>,
        values: &[f64],
        timestamps: &[i64],
    ) -> (Vec<f64>, u64) {
        self.do_internal(dst_values, values, timestamps)
    }

    /// Port of Go `rollupConfig.doInternal` — the exact window loop.
    fn do_internal(
        &self,
        mut dst_values: Vec<f64>,
        values: &[f64],
        timestamps: &[i64],
    ) -> (Vec<f64>, u64) {
        // Sanity checks.
        assert!(
            self.step > 0,
            "BUG: Step must be bigger than 0; got {}",
            self.step
        );
        assert!(
            self.start <= self.end,
            "BUG: Start cannot exceed End; got {} vs {}",
            self.start,
            self.end
        );
        assert!(
            self.window >= 0,
            "BUG: Window must be non-negative; got {}",
            self.window
        );
        validate_max_points_per_series(self.start, self.end, self.step, self.max_points_per_series)
            .expect("BUG: this must be validated before the call to RollupConfig::exec");

        dst_values.reserve(self.timestamps.len());

        // Set max_prev_interval for subsequent prev_value calculations:
        // for instant queries use step directly; for range queries estimate
        // the scrape interval (0.6 quantile of the last 20 intervals) and
        // inflate it to tolerate jitter.
        let mut max_prev_interval = self.step;
        if self.start < self.end {
            let scrape_interval = get_scrape_interval(timestamps, self.step);
            max_prev_interval = get_max_prev_interval(scrape_interval);
        }
        if self.lookback_delta > 0 && max_prev_interval > self.lookback_delta {
            max_prev_interval = self.lookback_delta;
        }
        if MIN_STALENESS_INTERVAL > 0 && max_prev_interval < MIN_STALENESS_INTERVAL {
            max_prev_interval = MIN_STALENESS_INTERVAL;
        }
        let mut window = self.window;
        // Adjust the lookbehind window only if it isn't set explicitly,
        // e.g. rate(foo). If the user explicitly sets the lookbehind window,
        // e.g. rate(foo[1s]), do not adjust it.
        if window <= 0 {
            window = self.step;
            if self.may_adjust_window && window < max_prev_interval {
                window = max_prev_interval;
            }
            // Artificial window cannot exceed the explicit lookback_delta.
            if self.is_default_rollup && self.lookback_delta > 0 && window > self.lookback_delta {
                window = self.lookback_delta;
            }
        }
        let mut rfa = RollupFuncArg {
            window,
            ..Default::default()
        };

        let mut i = 0usize;
        let mut j = 0usize;
        let mut ni = 0usize;
        let mut nj = 0usize;
        let f = &self.func;
        let mut samples_scanned = values.len() as u64;
        let samples_scanned_per_call = self.samples_scanned_per_call as u64;
        for &t_end in self.timestamps.iter() {
            let t_start = t_end - window;
            ni = seek_first_timestamp_idx_after(&timestamps[i..], t_start, ni);
            i += ni;
            if j < i {
                j = i;
            }
            nj = seek_first_timestamp_idx_after(&timestamps[j..], t_end, nj);
            j += nj;

            rfa.prev_value = f64::NAN;
            rfa.prev_timestamp = t_start - max_prev_interval;
            if i < timestamps.len() && i > 0 && timestamps[i - 1] > rfa.prev_timestamp {
                rfa.prev_value = values[i - 1];
                rfa.prev_timestamp = timestamps[i - 1];
            }
            rfa.values = &values[i..j];
            rfa.timestamps = &timestamps[i..j];
            rfa.real_prev_value = f64::NAN;
            if i > 0 {
                let prev_value = values[i - 1];
                let prev_timestamp = timestamps[i - 1];
                // Set real_prev_value if lookback_delta == 0 or if the
                // distance between the datapoint in the previous interval and
                // the first datapoint in this interval doesn't exceed
                // lookback_delta.
                let curr_timestamp = if !rfa.timestamps.is_empty() {
                    rfa.timestamps[0]
                } else {
                    t_start
                };
                if self.lookback_delta == 0
                    || (curr_timestamp - prev_timestamp) < self.lookback_delta
                {
                    rfa.real_prev_value = prev_value;
                }
            }
            rfa.real_next_value = if j < values.len() {
                values[j]
            } else {
                f64::NAN
            };
            rfa.curr_timestamp = t_end;
            let value = f(&rfa);
            rfa.idx += 1;
            if samples_scanned_per_call > 0 {
                samples_scanned += samples_scanned_per_call;
            } else {
                samples_scanned += rfa.values.len() as u64;
            }
            dst_values.push(value);
        }

        (dst_values, samples_scanned)
    }
}

/// Port of Go `seekFirstTimestampIdxAfter`: returns the index of the first
/// timestamp in `timestamps` bigger than `seek_timestamp`, probing a ±2
/// neighborhood of the previous hit before scanning/binary searching.
pub(crate) fn seek_first_timestamp_idx_after(
    timestamps: &[i64],
    seek_timestamp: i64,
    n_hint: usize,
) -> usize {
    if timestamps.is_empty() || timestamps[0] > seek_timestamp {
        return 0;
    }
    let mut start_idx = n_hint.saturating_sub(2);
    if start_idx >= timestamps.len() {
        start_idx = timestamps.len() - 1;
    }
    let mut end_idx = (n_hint + 2).min(timestamps.len());
    let mut ts = timestamps;
    if start_idx > 0 && ts[start_idx] <= seek_timestamp {
        ts = &ts[start_idx..];
        end_idx -= start_idx;
    } else {
        start_idx = 0;
    }
    if end_idx < ts.len() && ts[end_idx] > seek_timestamp {
        ts = &ts[..end_idx];
    }
    if ts.len() < 16 {
        // Fast path: scan.
        for (idx, &timestamp) in ts.iter().enumerate() {
            if timestamp > seek_timestamp {
                return start_idx + idx;
            }
        }
        return start_idx + ts.len();
    }
    // Slow path: binary search for the first ts > seek_timestamp.
    start_idx + ts.partition_point(|&t| t <= seek_timestamp)
}

/// Port of Go `getScrapeInterval`: 0.6 quantile of the last ≤20 inter-sample
/// deltas; `default_interval` on failure.
pub(crate) fn get_scrape_interval(timestamps: &[i64], default_interval: i64) -> i64 {
    if timestamps.len() < 2 {
        return default_interval;
    }
    let mut ts_prev = timestamps[timestamps.len() - 1];
    let mut timestamps = &timestamps[..timestamps.len() - 1];
    if timestamps.len() > 20 {
        timestamps = &timestamps[timestamps.len() - 20..];
    }
    let mut intervals = Vec::with_capacity(timestamps.len());
    for &ts in timestamps.iter().rev() {
        intervals.push((ts_prev - ts) as f64);
        ts_prev = ts;
    }
    let scrape_interval = crate::aggr::quantile(0.6, &intervals) as i64;
    if scrape_interval <= 0 {
        return default_interval;
    }
    scrape_interval
}

/// Port of Go `getMaxPrevInterval`: inflates the scrape interval more for
/// smaller intervals in order to hide possible gaps under jitter.
pub(crate) fn get_max_prev_interval(scrape_interval: i64) -> i64 {
    if scrape_interval <= 2_000 {
        return scrape_interval + 4 * scrape_interval;
    }
    if scrape_interval <= 4_000 {
        return scrape_interval + 2 * scrape_interval;
    }
    if scrape_interval <= 8_000 {
        return scrape_interval + scrape_interval;
    }
    if scrape_interval <= 16_000 {
        return scrape_interval + scrape_interval / 2;
    }
    if scrape_interval <= 32_000 {
        return scrape_interval + scrape_interval / 4;
    }
    scrape_interval + scrape_interval / 8
}

/// Port of Go `removeCounterResets`: running-correction over counter values
/// with the partial-reset heuristic and the staleness-gap reset.
pub fn remove_counter_resets(values: &mut [f64], timestamps: &[i64], max_staleness_interval: i64) {
    if values.is_empty() {
        return;
    }
    let mut correction = 0f64;
    let mut prev_value = values[0];
    for i in 0..values.len() {
        let v = values[i];
        let d = v - prev_value;
        if d < 0.0 {
            if (-d * 8.0) < prev_value {
                // This is likely a partial counter reset.
                correction += prev_value - v;
            } else {
                correction += prev_value;
            }
        }
        if i > 0 && max_staleness_interval > 0 {
            let gap = timestamps[i] - timestamps[i - 1];
            if gap > max_staleness_interval {
                // Reset the correction if the gap between samples exceeds
                // the staleness interval.
                correction = 0.0;
                prev_value = v;
                continue;
            }
        }
        prev_value = v;
        values[i] = v + correction;
        // There could be a precision error in float operations.
        if i > 0 && values[i] < values[i - 1] {
            values[i] = values[i - 1];
        }
    }
}

/// Builds the pre-function and rollup configs for the given rollup function.
/// Port of Go `getRollupConfigs` (Stage-1 subset: no `rollup*` /
/// `aggr_over_time` multi-config families).
#[allow(clippy::too_many_arguments)]
pub fn get_rollup_configs(
    func_name: &str,
    rf: RollupFunc,
    _expr: &Expr,
    start: i64,
    end: i64,
    step: i64,
    max_points_per_series: usize,
    window: i64,
    lookback_delta: i64,
    shared_timestamps: Arc<Vec<i64>>,
) -> Result<(PreFunc, Vec<RollupConfig>)> {
    let func_name = func_name.to_ascii_lowercase();

    let mut staleness_interval = lookback_delta;
    if staleness_interval != 0 {
        // If the staleness interval is set, it should additionally account
        // for the [window] range.
        staleness_interval += window;
    }

    let mut pre_func: PreFunc = Arc::new(|_values: &mut [f64], _timestamps: &[i64]| {});
    if rollup_funcs::remove_counter_resets_for(&func_name) {
        pre_func = Arc::new(move |values: &mut [f64], timestamps: &[i64]| {
            remove_counter_resets(values, timestamps, staleness_interval);
        });
    }

    match func_name.as_str() {
        "rollup"
        | "rollup_rate"
        | "rollup_deriv"
        | "rollup_increase"
        | "rollup_delta"
        | "rollup_candlestick"
        | "rollup_scrape_interval"
        | "aggr_over_time" => {
            return Err(Error::new(format!(
                "{func_name} is not supported yet (Stage 2)"
            )));
        }
        _ => {}
    }

    let rc = RollupConfig {
        tag_value: String::new(),
        func: rf,
        start,
        end,
        step,
        window,
        max_points_per_series,
        may_adjust_window: rollup_funcs::can_adjust_window(&func_name),
        timestamps: shared_timestamps,
        lookback_delta,
        is_default_rollup: func_name == "default_rollup",
        samples_scanned_per_call: rollup_funcs::samples_scanned_per_call(&func_name),
    };
    Ok((pre_func, vec![rc]))
}

/// Drops Prometheus staleness markers from the series unless `func_name`
/// needs them. Port of Go `dropStaleNaNs`; returns possibly filtered
/// (values, timestamps).
pub fn drop_stale_nans(
    func_name: &str,
    values: Vec<f64>,
    timestamps: Arc<Vec<i64>>,
    no_stale_markers: bool,
) -> (Vec<f64>, Arc<Vec<i64>>) {
    if no_stale_markers || func_name == "default_rollup" || func_name == "stale_samples_over_time" {
        // Do not drop Prometheus staleness marks for default_rollup(), since
        // it uses them for Prometheus-style staleness detection. Same for
        // stale_samples_over_time(), which counts them.
        return (values, timestamps);
    }
    if !values.iter().any(|&v| esm_common::decimal::is_stale_nan(v)) {
        // Fast path: no staleness marks.
        return (values, timestamps);
    }
    // Slow path: drop staleness marks.
    let mut dst_values = Vec::with_capacity(values.len());
    let mut dst_timestamps = Vec::with_capacity(values.len());
    for (i, v) in values.into_iter().enumerate() {
        if esm_common::decimal::is_stale_nan(v) {
            continue;
        }
        dst_values.push(v);
        dst_timestamps.push(timestamps[i]);
    }
    (dst_values, Arc::new(dst_timestamps))
}

/// Port of Go `removeNanValues` (used before subquery rollups; Stage 2).
#[allow(dead_code)]
pub(crate) fn remove_nan_values(values: &[f64], timestamps: &[i64]) -> (Vec<f64>, Vec<i64>) {
    let mut dst_values = Vec::with_capacity(values.len());
    let mut dst_timestamps = Vec::with_capacity(values.len());
    for (i, &v) in values.iter().enumerate() {
        if v.is_nan() {
            continue;
        }
        dst_values.push(v);
        dst_timestamps.push(timestamps[i]);
    }
    (dst_values, dst_timestamps)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timeseries::get_timestamps;

    // Test data from rollup_test.go.
    const TEST_VALUES: [f64; 12] = [
        123.0, 34.0, 44.0, 21.0, 54.0, 34.0, 99.0, 12.0, 44.0, 32.0, 34.0, 34.0,
    ];
    const TEST_TIMESTAMPS: [i64; 12] = [5, 15, 24, 36, 49, 60, 78, 80, 97, 115, 120, 130];

    fn rc(func: &str, start: i64, end: i64, step: i64, window: i64) -> RollupConfig {
        rc_with_lookback(func, start, end, step, window, 0)
    }

    fn rc_with_lookback(
        func: &str,
        start: i64,
        end: i64,
        step: i64,
        window: i64,
        lookback_delta: i64,
    ) -> RollupConfig {
        let nrf = rollup_funcs::get_rollup_func(func).unwrap();
        let rf = nrf(&[crate::rollup_funcs::RollupArgValue::RollupExpr]).unwrap();
        RollupConfig {
            tag_value: String::new(),
            func: rf,
            start,
            end,
            step,
            window,
            max_points_per_series: 10_000,
            may_adjust_window: false,
            timestamps: get_timestamps(start, end, step, 10_000).unwrap(),
            lookback_delta,
            is_default_rollup: false,
            samples_scanned_per_call: 0,
        }
    }

    #[track_caller]
    fn check(rc: &RollupConfig, values_expected: &[f64], timestamps_expected: &[i64]) {
        let (values, samples_scanned) = rc.exec(Vec::new(), &TEST_VALUES, &TEST_TIMESTAMPS);
        assert!(samples_scanned > 0);
        assert_eq!(rc.timestamps.as_slice(), timestamps_expected);
        assert_eq!(values.len(), values_expected.len(), "got {values:?}");
        for (i, (&got, &want)) in values.iter().zip(values_expected).enumerate() {
            if want.is_nan() {
                assert!(got.is_nan(), "idx {i}: got {got}; want NaN; all={values:?}");
            } else {
                assert!(
                    (got - want).abs() <= 1e-14,
                    "idx {i}: got {got}; want {want}; all={values:?}"
                );
            }
        }
    }

    const NAN: f64 = f64::NAN;

    // Port of TestRollupNoWindowNoPoints.
    #[test]
    fn rollup_no_window_no_points() {
        check(
            &rc("first_over_time", 0, 4, 1, 0),
            &[NAN, NAN, NAN, NAN, NAN],
            &[0, 1, 2, 3, 4],
        );
        check(
            &rc("delta", 120, 148, 4, 0),
            &[2.0, 0.0, 0.0, 0.0, NAN, NAN, NAN, NAN],
            &[120, 124, 128, 132, 136, 140, 144, 148],
        );
    }

    // Port of TestRollupWindowNoPoints.
    #[test]
    fn rollup_window_no_points() {
        check(
            &rc("first_over_time", 0, 4, 1, 3),
            &[NAN, NAN, NAN, NAN, NAN],
            &[0, 1, 2, 3, 4],
        );
        check(
            &rc("first_over_time", 161, 191, 10, 3),
            &[NAN, NAN, NAN, NAN],
            &[161, 171, 181, 191],
        );
    }

    // Port of TestRollupNoWindowPartialPoints.
    #[test]
    fn rollup_no_window_partial_points() {
        check(
            &rc("first_over_time", 0, 25, 5, 0),
            &[NAN, 123.0, NAN, 34.0, NAN, 44.0],
            &[0, 5, 10, 15, 20, 25],
        );
        check(
            &rc("first_over_time", 100, 160, 20, 0),
            &[44.0, 32.0, 34.0, NAN],
            &[100, 120, 140, 160],
        );
        check(
            &rc("first_over_time", -50, 150, 50, 0),
            &[NAN, NAN, 123.0, 34.0, 32.0],
            &[-50, 0, 50, 100, 150],
        );
    }

    // Port of TestRollupWindowPartialPoints.
    #[test]
    fn rollup_window_partial_points() {
        check(
            &rc("last_over_time", 0, 20, 5, 8),
            &[NAN, 123.0, 123.0, 34.0, 34.0],
            &[0, 5, 10, 15, 20],
        );
        check(
            &rc("last_over_time", 100, 160, 20, 18),
            &[44.0, 34.0, 34.0, NAN],
            &[100, 120, 140, 160],
        );
        check(
            &rc("last_over_time", 0, 150, 50, 19),
            &[NAN, 54.0, 44.0, NAN],
            &[0, 50, 100, 150],
        );
    }

    // Port of TestRollupFuncsLookbackDelta.
    #[test]
    fn rollup_funcs_lookback_delta() {
        for lookback in [1, 7, 0] {
            check(
                &rc_with_lookback("first_over_time", 80, 140, 10, 0, lookback),
                &[99.0, NAN, 44.0, NAN, 32.0, 34.0, NAN],
                &[80, 90, 100, 110, 120, 130, 140],
            );
        }
    }

    // Port of TestRollupFuncsNoWindow (Stage-1 funcs).
    #[test]
    fn rollup_funcs_no_window() {
        check(
            &rc("first_over_time", 0, 160, 40, 0),
            &[NAN, 123.0, 54.0, 44.0, 34.0],
            &[0, 40, 80, 120, 160],
        );
        check(
            &rc("count_over_time", 0, 160, 40, 0),
            &[NAN, 4.0, 4.0, 3.0, 1.0],
            &[0, 40, 80, 120, 160],
        );
        check(
            &rc("min_over_time", 0, 160, 40, 0),
            &[NAN, 21.0, 12.0, 32.0, 34.0],
            &[0, 40, 80, 120, 160],
        );
        check(
            &rc("max_over_time", 0, 160, 40, 0),
            &[NAN, 123.0, 99.0, 44.0, 34.0],
            &[0, 40, 80, 120, 160],
        );
        check(
            &rc("sum_over_time", 0, 160, 40, 0),
            &[NAN, 222.0, 199.0, 110.0, 34.0],
            &[0, 40, 80, 120, 160],
        );
        check(
            &rc("delta", 0, 160, 40, 0),
            &[NAN, 21.0, -9.0, 22.0, 0.0],
            &[0, 40, 80, 120, 160],
        );
        check(
            &rc("delta_prometheus", 0, 160, 40, 0),
            &[NAN, -102.0, -42.0, -10.0, NAN],
            &[0, 40, 80, 120, 160],
        );
        check(
            &rc("idelta", 10, 130, 40, 0),
            &[123.0, 33.0, -87.0, 0.0],
            &[10, 50, 90, 130],
        );
        check(
            &rc("lag", 0, 160, 40, 0),
            &[NAN, 0.004, 0.0, 0.0, 0.03],
            &[0, 40, 80, 120, 160],
        );
    }

    // Port of the scrape-interval inflation table (getMaxPrevInterval).
    #[test]
    fn max_prev_interval_table() {
        assert_eq!(get_max_prev_interval(1_000), 5_000);
        assert_eq!(get_max_prev_interval(2_000), 10_000);
        assert_eq!(get_max_prev_interval(3_000), 9_000);
        assert_eq!(get_max_prev_interval(4_000), 12_000);
        assert_eq!(get_max_prev_interval(5_000), 10_000);
        assert_eq!(get_max_prev_interval(8_000), 16_000);
        assert_eq!(get_max_prev_interval(10_000), 15_000);
        assert_eq!(get_max_prev_interval(16_000), 24_000);
        assert_eq!(get_max_prev_interval(20_000), 25_000);
        assert_eq!(get_max_prev_interval(32_000), 40_000);
        assert_eq!(get_max_prev_interval(64_000), 72_000);
    }

    #[test]
    fn scrape_interval_inference() {
        // Regular 10ms interval.
        let ts: Vec<i64> = (0..30).map(|i| i * 10).collect();
        assert_eq!(get_scrape_interval(&ts, 55), 10);
        // Too few timestamps -> default.
        assert_eq!(get_scrape_interval(&[5], 55), 55);
        assert_eq!(get_scrape_interval(&[], 55), 55);
    }

    #[test]
    fn seek_first_timestamp() {
        let ts: Vec<i64> = (0..100).map(|i| i * 10).collect();
        for hint in [0usize, 3, 50, 99, 120] {
            assert_eq!(seek_first_timestamp_idx_after(&ts, -5, hint), 0);
            assert_eq!(seek_first_timestamp_idx_after(&ts, 0, hint), 1);
            assert_eq!(seek_first_timestamp_idx_after(&ts, 505, hint), 51);
            assert_eq!(seek_first_timestamp_idx_after(&ts, 990, hint), 100);
            assert_eq!(seek_first_timestamp_idx_after(&ts, 2000, hint), 100);
        }
    }

    // Port of TestRollupDelta (rollupDelta heuristics table).
    #[test]
    fn rollup_delta_heuristics() {
        fn f(
            prev_value: f64,
            real_prev_value: f64,
            real_next_value: f64,
            values: &[f64],
            result_expected: f64,
        ) {
            let rfa = RollupFuncArg {
                prev_value,
                real_prev_value,
                real_next_value,
                values,
                timestamps: &[],
                ..Default::default()
            };
            let result = crate::rollup_funcs::rollup_delta(&rfa);
            if result_expected.is_nan() {
                assert!(result.is_nan(), "got {result}");
            } else {
                assert_eq!(result, result_expected);
            }
        }
        f(NAN, NAN, NAN, &[], NAN);
        // Small initial value.
        f(NAN, NAN, NAN, &[1.0], 1.0);
        f(NAN, NAN, NAN, &[10.0], 0.0);
        f(NAN, NAN, NAN, &[100.0], 0.0);
        f(NAN, NAN, NAN, &[1.0, 2.0, 3.0], 3.0);
        f(1.0, NAN, NAN, &[1.0, 2.0, 3.0], 2.0);
        f(NAN, NAN, NAN, &[5.0, 6.0, 8.0], 8.0);
        f(2.0, NAN, NAN, &[5.0, 6.0, 8.0], 6.0);
        f(NAN, NAN, NAN, &[100.0, 100.0], 0.0);
        // Big initial value with zero delta after that.
        f(NAN, NAN, NAN, &[1000.0], 0.0);
        f(NAN, NAN, NAN, &[1000.0, 1000.0], 0.0);
        // Big initial value with small delta after that.
        f(NAN, NAN, NAN, &[1000.0, 1001.0, 1002.0], 2.0);
        // Non-NaN realPrevValue.
        f(NAN, 900.0, NAN, &[1000.0], 100.0);
        f(NAN, 1000.0, NAN, &[1000.0], 0.0);
        f(NAN, 1100.0, NAN, &[1000.0], -100.0);
        f(NAN, 900.0, NAN, &[1000.0, 1001.0, 1002.0], 102.0);
        // Small delta between realNextValue and values.
        f(NAN, NAN, 990.0, &[1000.0], 0.0);
        f(NAN, NAN, 1005.0, &[1000.0], 0.0);
        // Big delta between realNextValue and values.
        f(NAN, NAN, 800.0, &[1000.0], 1000.0);
        f(NAN, NAN, 1300.0, &[1000.0], 1000.0);
        // Empty values.
        f(1.0, NAN, NAN, &[], 0.0);
        f(100.0, NAN, NAN, &[], 0.0);
    }

    #[test]
    fn remove_counter_resets_works() {
        let mut values = vec![100.0, 101.0, 102.0, 103.0, 104.0, 42.0, 43.0, 44.0];
        let timestamps: Vec<i64> = (0..8).collect();
        remove_counter_resets(&mut values, &timestamps, 0);
        assert_eq!(
            values,
            vec![100.0, 101.0, 102.0, 103.0, 104.0, 146.0, 147.0, 148.0]
        );

        // Partial counter reset: d < 0 && -d*8 < prev.
        let mut values = vec![1000.0, 1010.0, 1009.0, 1020.0];
        let timestamps: Vec<i64> = (0..4).collect();
        remove_counter_resets(&mut values, &timestamps, 0);
        assert_eq!(values, vec![1000.0, 1010.0, 1010.0, 1021.0]);

        // Gap reset with staleness interval.
        let mut values = vec![100.0, 50.0];
        let timestamps: Vec<i64> = [0, 1000].to_vec();
        remove_counter_resets(&mut values, &timestamps, 10);
        assert_eq!(values, vec![100.0, 50.0]);
    }
}
