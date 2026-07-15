//! Rollup function registry and implementations (Stage-1 subset).
//! Port of the corresponding parts of `rollup.go`.

use crate::aggr::{quantile, stddev, stdvar};
use crate::rollup::RollupFuncArg;
use crate::timeseries::Timeseries;
use crate::{Error, Result};
use std::sync::Arc;

/// A rollup function: returns the rollup value for the given window.
/// `prev_value` may be NaN; `values` and `timestamps` may be empty.
pub type RollupFunc = Arc<dyn for<'a> Fn(&RollupFuncArg<'a>) -> f64 + Send + Sync>;

/// An argument to a rollup function call as evaluated by
/// `evalRollupFuncArgs`: either a materialized series (scalar args such as
/// the phi of `quantile_over_time`) or the rollup expression placeholder.
pub enum RollupArgValue {
    Series(Vec<Timeseries>),
    RollupExpr,
}

/// Constructor validating args and returning the rollup function.
/// Port of Go `newRollupFunc`.
pub type NewRollupFunc = fn(&[RollupArgValue]) -> Result<RollupFunc>;

/// Returns the constructor for the given rollup function name, if it is
/// implemented in Stage 1. Port of Go `getRollupFunc` over the
/// `rollupFuncs` table.
pub fn get_rollup_func(func_name: &str) -> Option<NewRollupFunc> {
    let name = func_name.to_ascii_lowercase();
    let nrf: NewRollupFunc = match name.as_str() {
        "absent_over_time" => |args| one_arg(args, Arc::new(rollup_absent)),
        "avg_over_time" => |args| one_arg(args, Arc::new(rollup_avg)),
        "changes" => |args| one_arg(args, Arc::new(rollup_changes)),
        "changes_prometheus" => |args| one_arg(args, Arc::new(rollup_changes_prometheus)),
        "count_over_time" => |args| one_arg(args, Arc::new(rollup_count)),
        "default_rollup" => |args| one_arg(args, Arc::new(rollup_default)),
        "delta" => |args| one_arg(args, Arc::new(rollup_delta)),
        "delta_prometheus" => |args| one_arg(args, Arc::new(rollup_delta_prometheus)),
        "deriv_fast" => |args| one_arg(args, Arc::new(rollup_deriv_fast)),
        "first_over_time" => |args| one_arg(args, Arc::new(rollup_first)),
        "idelta" => |args| one_arg(args, Arc::new(rollup_idelta)),
        // increase = rollupDelta + removeCounterResets preFunc.
        "increase" => |args| one_arg(args, Arc::new(rollup_delta)),
        "increase_prometheus" => |args| one_arg(args, Arc::new(rollup_delta_prometheus)),
        "increase_pure" => |args| one_arg(args, Arc::new(rollup_increase_pure)),
        // irate = rollupIderiv + removeCounterResets preFunc.
        "irate" => |args| one_arg(args, Arc::new(rollup_ideriv)),
        "ideriv" => |args| one_arg(args, Arc::new(rollup_ideriv)),
        "lag" => |args| one_arg(args, Arc::new(rollup_lag)),
        "last_over_time" => |args| one_arg(args, Arc::new(rollup_last)),
        "max_over_time" => |args| one_arg(args, Arc::new(rollup_max)),
        "median_over_time" => |args| one_arg(args, Arc::new(rollup_median)),
        "min_over_time" => |args| one_arg(args, Arc::new(rollup_min)),
        "present_over_time" => |args| one_arg(args, Arc::new(rollup_present)),
        "quantile_over_time" => new_rollup_quantile,
        "range_over_time" => |args| one_arg(args, Arc::new(rollup_range)),
        // rate = rollupDerivFast + removeCounterResets preFunc.
        "rate" => |args| one_arg(args, Arc::new(rollup_deriv_fast)),
        "rate_prometheus" => |args| one_arg(args, Arc::new(rollup_deriv_fast_prometheus)),
        "resets" => |args| one_arg(args, Arc::new(rollup_resets)),
        "stddev_over_time" => |args| one_arg(args, Arc::new(rollup_stddev)),
        "stdvar_over_time" => |args| one_arg(args, Arc::new(rollup_stdvar)),
        "sum_over_time" => |args| one_arg(args, Arc::new(rollup_sum)),
        "sum2_over_time" => |args| one_arg(args, Arc::new(rollup_sum2)),
        "tfirst_over_time" => |args| one_arg(args, Arc::new(rollup_tfirst)),
        // `timestamp` returns the timestamp of the last datapoint in the
        // window in order to properly handle offsets and unaligned samples.
        "timestamp" => |args| one_arg(args, Arc::new(rollup_tlast)),
        "timestamp_with_name" => |args| one_arg(args, Arc::new(rollup_tlast)),
        "tlast_over_time" => |args| one_arg(args, Arc::new(rollup_tlast)),
        "tmax_over_time" => |args| one_arg(args, Arc::new(rollup_tmax)),
        "tmin_over_time" => |args| one_arg(args, Arc::new(rollup_tmin)),
        _ => return None,
    };
    Some(nrf)
}

/// Whether `func_name` needs `removeCounterResets` applied over the input
/// samples first. Port of Go `rollupFuncsRemoveCounterResets`.
pub fn remove_counter_resets_for(func_name: &str) -> bool {
    matches!(
        func_name,
        "increase"
            | "increase_prometheus"
            | "increase_pure"
            | "irate"
            | "rate"
            | "rate_prometheus"
            | "rollup_increase"
            | "rollup_rate"
    )
}

/// Whether the lookbehind window may be adjusted for `func_name`.
/// Port of Go `rollupFuncsCanAdjustWindow`.
pub fn can_adjust_window(func_name: &str) -> bool {
    matches!(
        func_name,
        "default_rollup"
            | "deriv"
            | "deriv_fast"
            | "ideriv"
            | "irate"
            | "rate"
            | "rate_over_sum"
            | "rollup"
            | "rollup_candlestick"
            | "rollup_deriv"
            | "rollup_rate"
            | "rollup_scrape_interval"
            | "scrape_interval"
            | "timestamp"
    )
}

/// Whether `func_name` needs the silence interval (extra lookbehind fetch
/// for the sample preceding the window).
/// Port of Go `needSilenceIntervalForRollupFunc`.
pub fn need_silence_interval(func_name: &str) -> bool {
    matches!(
        func_name,
        "ascent_over_time"
            | "changes"
            | "decreases_over_time"
            | "default_rollup"
            | "delta"
            | "deriv_fast"
            | "descent_over_time"
            | "idelta"
            | "ideriv"
            | "increase"
            | "increase_pure"
            | "increases_over_time"
            | "integrate"
            | "irate"
            | "lag"
            | "lifetime"
            | "rate"
            | "resets"
            | "rollup"
            | "rollup_candlestick"
            | "rollup_delta"
            | "rollup_deriv"
            | "rollup_increase"
            | "rollup_rate"
            | "rollup_scrape_interval"
            | "scrape_interval"
            | "tlast_change_over_time"
    )
}

/// Functions that do not change the physical meaning of the input series and
/// therefore keep the metric name. Port of Go `rollupFuncsKeepMetricName`.
pub fn keep_metric_name(func_name: &str) -> bool {
    matches!(
        func_name,
        "avg_over_time"
            | "default_rollup"
            | "first_over_time"
            | "geomean_over_time"
            | "hoeffding_bound_lower"
            | "hoeffding_bound_upper"
            | "holt_winters"
            | "iqr_over_time"
            | "last_over_time"
            | "max_over_time"
            | "median_over_time"
            | "min_over_time"
            | "mode_over_time"
            | "predict_linear"
            | "quantile_over_time"
            | "quantiles_over_time"
            | "rollup"
            | "rollup_candlestick"
            | "timestamp_with_name"
    )
}

/// The estimated number of samples scanned per rollup call (cost model).
/// Port of Go `rollupFuncsSamplesScannedPerCall`; 0 means "all samples".
pub fn samples_scanned_per_call(func_name: &str) -> usize {
    match func_name {
        "absent_over_time"
        | "count_over_time"
        | "default_rollup"
        | "first_over_time"
        | "lag"
        | "last_over_time"
        | "present_over_time"
        | "tfirst_over_time"
        | "timestamp"
        | "timestamp_with_name"
        | "tlast_over_time" => 1,
        "delta"
        | "delta_prometheus"
        | "deriv_fast"
        | "idelta"
        | "ideriv"
        | "increase"
        | "increase_prometheus"
        | "increase_pure"
        | "irate"
        | "lifetime"
        | "rate"
        | "rate_prometheus"
        | "scrape_interval" => 2,
        _ => 0,
    }
}

fn expect_rollup_args_num(args: &[RollupArgValue], expected: usize) -> Result<()> {
    if args.len() == expected {
        return Ok(());
    }
    Err(Error::new(format!(
        "unexpected number of args; got {}; want {expected}",
        args.len()
    )))
}

fn one_arg(args: &[RollupArgValue], rf: RollupFunc) -> Result<RollupFunc> {
    expect_rollup_args_num(args, 1)?;
    Ok(rf)
}

fn get_scalar_arg(args: &[RollupArgValue], arg_num: usize) -> Result<Vec<f64>> {
    let Some(RollupArgValue::Series(tss)) = args.get(arg_num) else {
        return Err(Error::new(format!("arg #{} must be a scalar", arg_num + 1)));
    };
    if tss.len() != 1 {
        return Err(Error::new(format!("arg #{} must be a scalar", arg_num + 1)));
    }
    Ok(tss[0].values.clone())
}

// Note: rollup functions assume NaN-free windows (staleness marks and NaNs
// are stripped by the driver), except rollup_default which intentionally
// sees staleness marks.

pub(crate) fn rollup_default(rfa: &RollupFuncArg<'_>) -> f64 {
    let values = rfa.values;
    if values.is_empty() {
        // Do not take into account rfa.prev_value, since it may lead to
        // inconsistent results comparing to Prometheus on broken series.
        return f64::NAN;
    }
    // Intentionally do not skip the possible last Prometheus staleness mark.
    values[values.len() - 1]
}

pub(crate) fn rollup_last(rfa: &RollupFuncArg<'_>) -> f64 {
    rollup_default(rfa)
}

pub(crate) fn rollup_first(rfa: &RollupFuncArg<'_>) -> f64 {
    let values = rfa.values;
    if values.is_empty() {
        return f64::NAN;
    }
    values[0]
}

pub(crate) fn rollup_avg(rfa: &RollupFuncArg<'_>) -> f64 {
    let values = rfa.values;
    if values.is_empty() {
        return f64::NAN;
    }
    let sum: f64 = values.iter().sum();
    sum / values.len() as f64
}

pub(crate) fn rollup_min(rfa: &RollupFuncArg<'_>) -> f64 {
    let values = rfa.values;
    if values.is_empty() {
        return f64::NAN;
    }
    let mut min_value = values[0];
    for &v in values {
        if v < min_value {
            min_value = v;
        }
    }
    min_value
}

pub(crate) fn rollup_max(rfa: &RollupFuncArg<'_>) -> f64 {
    let values = rfa.values;
    if values.is_empty() {
        return f64::NAN;
    }
    let mut max_value = values[0];
    for &v in values {
        if v > max_value {
            max_value = v;
        }
    }
    max_value
}

pub(crate) fn rollup_median(rfa: &RollupFuncArg<'_>) -> f64 {
    quantile(0.5, rfa.values)
}

pub(crate) fn rollup_sum(rfa: &RollupFuncArg<'_>) -> f64 {
    let values = rfa.values;
    if values.is_empty() {
        return f64::NAN;
    }
    values.iter().sum()
}

pub(crate) fn rollup_sum2(rfa: &RollupFuncArg<'_>) -> f64 {
    let values = rfa.values;
    if values.is_empty() {
        return f64::NAN;
    }
    values.iter().map(|v| v * v).sum()
}

pub(crate) fn rollup_range(rfa: &RollupFuncArg<'_>) -> f64 {
    rollup_max(rfa) - rollup_min(rfa)
}

pub(crate) fn rollup_count(rfa: &RollupFuncArg<'_>) -> f64 {
    let values = rfa.values;
    if values.is_empty() {
        return f64::NAN;
    }
    values.len() as f64
}

pub(crate) fn rollup_absent(rfa: &RollupFuncArg<'_>) -> f64 {
    if rfa.values.is_empty() {
        return 1.0;
    }
    f64::NAN
}

pub(crate) fn rollup_present(rfa: &RollupFuncArg<'_>) -> f64 {
    if !rfa.values.is_empty() {
        return 1.0;
    }
    f64::NAN
}

pub(crate) fn rollup_stddev(rfa: &RollupFuncArg<'_>) -> f64 {
    stddev(rfa.values)
}

pub(crate) fn rollup_stdvar(rfa: &RollupFuncArg<'_>) -> f64 {
    stdvar(rfa.values)
}

pub(crate) fn rollup_tfirst(rfa: &RollupFuncArg<'_>) -> f64 {
    let timestamps = rfa.timestamps;
    if timestamps.is_empty() {
        return f64::NAN;
    }
    timestamps[0] as f64 / 1e3
}

pub(crate) fn rollup_tlast(rfa: &RollupFuncArg<'_>) -> f64 {
    let timestamps = rfa.timestamps;
    if timestamps.is_empty() {
        return f64::NAN;
    }
    timestamps[timestamps.len() - 1] as f64 / 1e3
}

pub(crate) fn rollup_tmin(rfa: &RollupFuncArg<'_>) -> f64 {
    let values = rfa.values;
    let timestamps = rfa.timestamps;
    if values.is_empty() {
        return f64::NAN;
    }
    let mut min_value = values[0];
    let mut min_timestamp = timestamps[0];
    for (i, &v) in values.iter().enumerate() {
        // Get the last timestamp for the minimum value as most users expect.
        if v <= min_value {
            min_value = v;
            min_timestamp = timestamps[i];
        }
    }
    min_timestamp as f64 / 1e3
}

pub(crate) fn rollup_tmax(rfa: &RollupFuncArg<'_>) -> f64 {
    let values = rfa.values;
    let timestamps = rfa.timestamps;
    if values.is_empty() {
        return f64::NAN;
    }
    let mut max_value = values[0];
    let mut max_timestamp = timestamps[0];
    for (i, &v) in values.iter().enumerate() {
        // Get the last timestamp for the maximum value as most users expect.
        if v >= max_value {
            max_value = v;
            max_timestamp = timestamps[i];
        }
    }
    max_timestamp as f64 / 1e3
}

pub(crate) fn rollup_lag(rfa: &RollupFuncArg<'_>) -> f64 {
    let timestamps = rfa.timestamps;
    if timestamps.is_empty() {
        if rfa.prev_value.is_nan() {
            return f64::NAN;
        }
        return (rfa.curr_timestamp - rfa.prev_timestamp) as f64 / 1e3;
    }
    (rfa.curr_timestamp - timestamps[timestamps.len() - 1]) as f64 / 1e3
}

/// Port of Go `rollupDelta` — upstream-style delta/increase heuristics
/// (deliberately different from Prometheus; see rollup.go comments).
pub(crate) fn rollup_delta(rfa: &RollupFuncArg<'_>) -> f64 {
    let mut values = rfa.values;
    let mut prev_value = rfa.prev_value;
    if prev_value.is_nan() {
        if values.is_empty() {
            return f64::NAN;
        }
        if !rfa.real_prev_value.is_nan() {
            // Assume that the value didn't change during the current gap.
            return values[values.len() - 1] - rfa.real_prev_value;
        }
        // Assume that the previous non-existing value was 0 only if the
        // first value doesn't exceed too much the delta with the next value.
        //
        // This should prevent from improper increase() results for os-level
        // counters which may start long before the first value appears in
        // the db.
        let mut d = 0f64;
        if values.len() > 1 {
            d = values[1] - values[0];
        } else if !rfa.real_next_value.is_nan() {
            d = rfa.real_next_value - values[0];
        }
        if values[0].abs() < 10.0 * (d.abs() + 1.0) {
            prev_value = 0.0;
        } else {
            prev_value = values[0];
            values = &values[1..];
        }
    }
    if values.is_empty() {
        // Assume that the value didn't change on the given interval.
        return 0.0;
    }
    values[values.len() - 1] - prev_value
}

pub(crate) fn rollup_delta_prometheus(rfa: &RollupFuncArg<'_>) -> f64 {
    let values = rfa.values;
    // Just return the difference between the last and the first sample like
    // Prometheus does.
    if values.len() < 2 {
        return f64::NAN;
    }
    values[values.len() - 1] - values[0]
}

pub(crate) fn rollup_increase_pure(rfa: &RollupFuncArg<'_>) -> f64 {
    let values = rfa.values;
    let mut prev_value = rfa.prev_value;
    if prev_value.is_nan() {
        if values.is_empty() {
            return f64::NAN;
        }
        // Assume the counter starts from 0.
        prev_value = 0.0;
        if !rfa.real_prev_value.is_nan() {
            // Assume that the value didn't change during the current gap.
            prev_value = rfa.real_prev_value;
        }
    }
    if values.is_empty() {
        // Assume the counter didn't change since prev_value.
        return 0.0;
    }
    values[values.len() - 1] - prev_value
}

pub(crate) fn rollup_idelta(rfa: &RollupFuncArg<'_>) -> f64 {
    let values = rfa.values;
    if values.is_empty() {
        if rfa.prev_value.is_nan() {
            return f64::NAN;
        }
        // Assume that the value didn't change on the given interval.
        return 0.0;
    }
    let last_value = values[values.len() - 1];
    let values = &values[..values.len() - 1];
    if values.is_empty() {
        let prev_value = rfa.prev_value;
        if prev_value.is_nan() {
            // Assume that the previous non-existing value was 0.
            return last_value;
        }
        return last_value - prev_value;
    }
    last_value - values[values.len() - 1]
}

/// Port of Go `rollupDerivFast` (upstream `rate`; NO Prometheus extrapolation).
pub(crate) fn rollup_deriv_fast(rfa: &RollupFuncArg<'_>) -> f64 {
    let values = rfa.values;
    let timestamps = rfa.timestamps;
    let mut prev_value = rfa.prev_value;
    let mut prev_timestamp = rfa.prev_timestamp;
    if prev_value.is_nan() {
        if values.is_empty() {
            return f64::NAN;
        }
        if values.len() == 1 {
            // It is impossible to determine the duration during which the
            // value changed from 0 to the current value; return NaN.
            return f64::NAN;
        }
        prev_value = values[0];
        prev_timestamp = timestamps[0];
    } else if values.is_empty() {
        // Assume that the value didn't change on the given interval.
        return 0.0;
    }
    let v_end = values[values.len() - 1];
    let t_end = timestamps[timestamps.len() - 1];
    let dv = v_end - prev_value;
    let dt = (t_end - prev_timestamp) as f64 / 1e3;
    dv / dt
}

pub(crate) fn rollup_deriv_fast_prometheus(rfa: &RollupFuncArg<'_>) -> f64 {
    let delta = rollup_delta_prometheus(rfa);
    if delta.is_nan() || rfa.window == 0 {
        return f64::NAN;
    }
    delta / (rfa.window as f64 / 1e3)
}

/// Port of Go `rollupIderiv` (upstream `irate`).
pub(crate) fn rollup_ideriv(rfa: &RollupFuncArg<'_>) -> f64 {
    let values = rfa.values;
    let timestamps = rfa.timestamps;
    if values.len() < 2 {
        if values.is_empty() {
            return f64::NAN;
        }
        if rfa.prev_value.is_nan() {
            return f64::NAN;
        }
        return (values[0] - rfa.prev_value) / ((timestamps[0] - rfa.prev_timestamp) as f64 / 1e3);
    }
    let v_end = values[values.len() - 1];
    let t_end = timestamps[timestamps.len() - 1];
    let values = &values[..values.len() - 1];
    let mut timestamps = &timestamps[..timestamps.len() - 1];
    // Skip data points with duplicate timestamps.
    while !timestamps.is_empty() && timestamps[timestamps.len() - 1] >= t_end {
        timestamps = &timestamps[..timestamps.len() - 1];
    }
    let t_start;
    let v_start;
    if timestamps.is_empty() {
        if rfa.prev_value.is_nan() {
            return 0.0;
        }
        t_start = rfa.prev_timestamp;
        v_start = rfa.prev_value;
    } else {
        t_start = timestamps[timestamps.len() - 1];
        v_start = values[timestamps.len() - 1];
    }
    let dv = v_end - v_start;
    let dt = t_end - t_start;
    dv / (dt as f64 / 1e3)
}

pub(crate) fn rollup_changes(rfa: &RollupFuncArg<'_>) -> f64 {
    let mut values = rfa.values;
    let mut prev_value = rfa.prev_value;
    let mut n = 0usize;
    if prev_value.is_nan() {
        if values.is_empty() {
            return f64::NAN;
        }
        if !rfa.real_prev_value.is_nan() {
            // Assume that the value didn't change during the current gap.
            prev_value = rfa.real_prev_value;
        } else {
            n += 1;
            prev_value = values[0];
            values = &values[1..];
        }
    }
    for &v in values {
        if v != prev_value {
            if (v - prev_value).abs() < 1e-12 * v.abs() {
                // This may be a precision error.
                continue;
            }
            n += 1;
            prev_value = v;
        }
    }
    n as f64
}

pub(crate) fn rollup_changes_prometheus(rfa: &RollupFuncArg<'_>) -> f64 {
    let values = rfa.values;
    // Do not take into account rfa.prev_value like Prometheus does.
    if values.is_empty() {
        return f64::NAN;
    }
    let mut prev_value = values[0];
    let mut n = 0usize;
    for &v in &values[1..] {
        if v != prev_value {
            if (v - prev_value).abs() < 1e-12 * v.abs() {
                // This may be a precision error.
                continue;
            }
            n += 1;
            prev_value = v;
        }
    }
    n as f64
}

pub(crate) fn rollup_resets(rfa: &RollupFuncArg<'_>) -> f64 {
    let mut values = rfa.values;
    if values.is_empty() {
        if rfa.prev_value.is_nan() {
            return f64::NAN;
        }
        return 0.0;
    }
    let mut prev_value = rfa.prev_value;
    if prev_value.is_nan() {
        prev_value = values[0];
        values = &values[1..];
    }
    if values.is_empty() {
        return 0.0;
    }
    let mut n = 0usize;
    for &v in values {
        if v < prev_value {
            if (v - prev_value).abs() < 1e-12 * v.abs() {
                // This may be a precision error.
                continue;
            }
            n += 1;
        }
        prev_value = v;
    }
    n as f64
}

fn new_rollup_quantile(args: &[RollupArgValue]) -> Result<RollupFunc> {
    expect_rollup_args_num(args, 2)?;
    let phis = get_scalar_arg(args, 0)?;
    Ok(Arc::new(move |rfa: &RollupFuncArg<'_>| {
        let phi = phis.get(rfa.idx).copied().unwrap_or(f64::NAN);
        quantile(phi, rfa.values)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test data from rollup_test.go.
    const TEST_VALUES: [f64; 12] = [
        123.0, 34.0, 44.0, 21.0, 54.0, 34.0, 99.0, 12.0, 44.0, 32.0, 34.0, 34.0,
    ];
    const TEST_TIMESTAMPS: [i64; 12] = [5, 15, 24, 36, 49, 60, 78, 80, 97, 115, 120, 130];

    // Port of testRollupFunc from rollup_test.go.
    #[track_caller]
    fn test_rollup_func(func_name: &str, args: &[RollupArgValue], v_expected: f64) {
        let nrf = get_rollup_func(func_name).expect("cannot obtain rollup func");
        let rf = nrf(args).expect("unexpected error");
        let mut values = TEST_VALUES.to_vec();
        let timestamps = TEST_TIMESTAMPS.to_vec();
        if remove_counter_resets_for(func_name) {
            crate::rollup::remove_counter_resets(&mut values, &timestamps, 0);
        }
        let rfa = RollupFuncArg {
            prev_value: f64::NAN,
            prev_timestamp: 0,
            values: &values,
            timestamps: &timestamps,
            window: timestamps[timestamps.len() - 1] - timestamps[0],
            ..Default::default()
        };
        for _ in 0..5 {
            let v = rf(&rfa);
            if v_expected.is_nan() {
                assert!(v.is_nan(), "unexpected value; got {v}; want NaN");
            } else if v_expected.is_infinite() {
                assert_eq!(v, v_expected, "unexpected value");
            } else {
                assert!(
                    (v - v_expected).abs() <= 1e-14,
                    "unexpected value; got {v}; want {v_expected}"
                );
            }
        }
    }

    fn re_arg() -> Vec<RollupArgValue> {
        vec![RollupArgValue::RollupExpr]
    }

    // Port of TestRollupNewRollupFuncSuccess (Stage-1 funcs).
    #[test]
    fn new_rollup_func_success() {
        let f = |func_name: &str, v_expected: f64| {
            test_rollup_func(func_name, &re_arg(), v_expected);
        };
        f("default_rollup", 34.0);
        f("changes", 11.0);
        f("changes_prometheus", 10.0);
        f("delta", 34.0);
        f("delta_prometheus", -89.0);
        f("deriv_fast", -712.0);
        f("idelta", 0.0);
        f("increase", 398.0);
        f("increase_prometheus", 275.0);
        f("increase_pure", 398.0);
        f("irate", 0.0);
        f("ideriv", 0.0);
        f("rate", 2200.0);
        f("rate_prometheus", 2200.0);
        f("resets", 5.0);
        f("range_over_time", 111.0);
        f("avg_over_time", 47.083333333333336);
        f("min_over_time", 12.0);
        f("max_over_time", 123.0);
        f("tmin_over_time", 0.08);
        f("tmax_over_time", 0.005);
        f("tfirst_over_time", 0.005);
        f("tlast_over_time", 0.13);
        f("sum_over_time", 565.0);
        f("sum2_over_time", 37951.0);
        f("count_over_time", 12.0);
        f("last_over_time", 34.0);
        f("first_over_time", 123.0);
        f("stddev_over_time", 30.752935722554287);
        f("stdvar_over_time", 945.7430555555555);
        f("absent_over_time", f64::NAN);
        f("present_over_time", 1.0);
        f("timestamp", 0.13);
        f("timestamp_with_name", 0.13);
        f("median_over_time", 34.0);
    }

    // Port of TestRollupQuantileOverTime.
    #[test]
    fn quantile_over_time_cases() {
        let f = |phi: f64, v_expected: f64| {
            let phis = Timeseries {
                metric_name: Default::default(),
                values: vec![phi; TEST_VALUES.len()],
                timestamps: Arc::new(TEST_TIMESTAMPS.to_vec()),
            };
            let args = vec![
                RollupArgValue::Series(vec![phis]),
                RollupArgValue::RollupExpr,
            ];
            test_rollup_func("quantile_over_time", &args, v_expected);
        };
        f(-123.0, f64::NEG_INFINITY);
        f(-0.5, f64::NEG_INFINITY);
        f(0.0, 12.0);
        f(0.1, 22.1);
        f(0.5, 34.0);
        f(0.9, 94.50000000000001);
        f(1.0, 123.0);
        f(234.0, f64::INFINITY);
        f(f64::NAN, f64::NAN);
    }

    // Port of TestRollupNewRollupFuncError.
    #[test]
    fn new_rollup_func_error() {
        assert!(get_rollup_func("non-existing-func").is_none());
        // Invalid number of args.
        for func_name in ["default_rollup", "avg_over_time", "quantile_over_time"] {
            let nrf = get_rollup_func(func_name).unwrap();
            assert!(
                nrf(&[]).is_err(),
                "expecting error for {func_name} with 0 args"
            );
        }
    }
}
