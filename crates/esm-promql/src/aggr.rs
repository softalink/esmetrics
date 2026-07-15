//! General (materialized) aggregate functions. Port of the Stage-1 subset of
//! `aggr.go`: grouping via `removeGroupTags` + sorted-marshal keys, plus the
//! sum/min/max/avg/count/group/sum2/geomean/stddev/stdvar/any reducers.

use crate::eval::EvalConfig;
use crate::timeseries::{metric_name_group_key, remove_empty_series, Timeseries};
use crate::{Error, Result};
use esm_metricsql::{AggrFuncExpr, ModifierExpr};
use esm_storage::metric_name::MetricName;
use std::collections::HashMap;

/// `-search.maxSeriesPerAggrFunc` analog.
pub const MAX_SERIES_PER_AGGR_FUNC: usize = 1_000_000;

pub struct AggrFuncArg<'a> {
    pub args: Vec<Vec<Timeseries>>,
    pub ae: &'a AggrFuncExpr,
    pub ec: &'a EvalConfig,
}

type AggrFn = fn(&mut AggrFuncArg<'_>) -> Result<Vec<Timeseries>>;

/// Returns the aggregate function for the given name (Stage-1 subset).
/// Port of Go `getAggrFunc` over the `aggrFuncs` table.
pub fn get_aggr_func(name: &str) -> Option<AggrFn> {
    let name = name.to_ascii_lowercase();
    let f: AggrFn = match name.as_str() {
        "any" => aggr_func_any,
        "avg" => |afa| new_aggr_func(afa, aggr_func_avg),
        "count" => |afa| new_aggr_func(afa, aggr_func_count),
        "geomean" => |afa| new_aggr_func(afa, aggr_func_geomean),
        "group" => |afa| new_aggr_func(afa, aggr_func_group),
        "max" => |afa| new_aggr_func(afa, aggr_func_max),
        "min" => |afa| new_aggr_func(afa, aggr_func_min),
        "stddev" => |afa| new_aggr_func(afa, aggr_func_stddev),
        "stdvar" => |afa| new_aggr_func(afa, aggr_func_stdvar),
        "sum" => |afa| new_aggr_func(afa, aggr_func_sum),
        "sum2" => |afa| new_aggr_func(afa, aggr_func_sum2),
        _ => return None,
    };
    Some(f)
}

/// Port of Go `newAggrFunc` + `getAggrTimeseries`.
fn new_aggr_func(afa: &mut AggrFuncArg<'_>, afe: fn(&mut [Timeseries])) -> Result<Vec<Timeseries>> {
    let tss = get_aggr_timeseries(std::mem::take(&mut afa.args))?;
    aggr_func_ext(
        |tss, _modifier| {
            afe(tss);
        },
        tss,
        &afa.ae.modifier,
        afa.ae.limit.max(0) as usize,
        false,
    )
}

fn get_aggr_timeseries(args: Vec<Vec<Timeseries>>) -> Result<Vec<Timeseries>> {
    if args.is_empty() {
        return Err(Error::new("expecting at least one arg"));
    }
    let mut args = args.into_iter();
    let mut tss = args.next().unwrap();
    for arg in args {
        tss.extend(arg);
    }
    Ok(tss)
}

/// Port of Go `removeGroupTags`: applies the `by (...)`/`without (...)`
/// modifier to the metric name.
pub fn remove_group_tags(metric_name: &mut MetricName, modifier: &ModifierExpr) {
    let group_op = modifier.op.to_ascii_lowercase();
    let args: Vec<&str> = modifier.args.iter().map(|s| s.as_str()).collect();
    match group_op.as_str() {
        "" | "by" => {
            metric_name.remove_tags_on(&args);
        }
        "without" => {
            metric_name.remove_tags_ignoring(&args);
            // Reset the metric group as Prometheus does on
            // `aggr(...) without (...)` calls.
            metric_name.reset_metric_group();
        }
        _ => panic!("BUG: unknown group modifier: {group_op:?}"),
    }
}

/// Port of Go `aggrFuncExt`: groups the series and applies the reducer to
/// each group. The reducer mutates `tss[0]` in place and the first series of
/// each group is kept.
pub fn aggr_func_ext(
    afe: impl Fn(&mut Vec<Timeseries>, &ModifierExpr),
    arg_orig: Vec<Timeseries>,
    modifier: &ModifierExpr,
    max_series: usize,
    keep_original: bool,
) -> Result<Vec<Timeseries>> {
    let m = aggr_prepare_series(arg_orig, modifier, max_series, keep_original);
    let mut rvs = Vec::with_capacity(m.len());
    for (_, mut tssl) in m {
        afe(&mut tssl, modifier);
        tssl.truncate(1);
        rvs.extend(tssl);
    }
    Ok(rvs)
}

/// Port of Go `aggrPrepareSeries`: removes empty series and groups the rest
/// by the modifier-adjusted metric name.
fn aggr_prepare_series(
    arg_orig: Vec<Timeseries>,
    modifier: &ModifierExpr,
    max_series: usize,
    keep_original: bool,
) -> HashMap<Vec<u8>, Vec<Timeseries>> {
    // Remove empty time series, since they are ignored by aggregate funcs.
    let arg = remove_empty_series(arg_orig);

    let mut m: HashMap<Vec<u8>, Vec<Timeseries>> = HashMap::new();
    for mut ts in arg {
        let key = if keep_original {
            let mut mn = ts.metric_name.clone();
            remove_group_tags(&mut mn, modifier);
            metric_name_group_key(&mut mn)
        } else {
            remove_group_tags(&mut ts.metric_name, modifier);
            metric_name_group_key(&mut ts.metric_name)
        };
        match m.get_mut(&key) {
            Some(tssl) => tssl.push(ts),
            None => {
                if max_series > 0 && m.len() >= max_series {
                    // Series limit reached after grouping; skip the rest.
                    continue;
                }
                m.insert(key, vec![ts]);
            }
        }
    }
    m
}

/// Port of Go `aggrFuncAny`.
fn aggr_func_any(afa: &mut AggrFuncArg<'_>) -> Result<Vec<Timeseries>> {
    let tss = get_aggr_timeseries(std::mem::take(&mut afa.args))?;
    let afe = |tss: &mut Vec<Timeseries>, _modifier: &ModifierExpr| {
        tss.truncate(1);
    };
    // Only a single time series per group must be returned.
    let limit = afa.ae.limit.clamp(0, 1) as usize;
    aggr_func_ext(afe, tss, &afa.ae.modifier, limit, true)
}

fn aggr_func_group(tss: &mut [Timeseries]) {
    for i in 0..tss[0].values.len() {
        let mut v = f64::NAN;
        for ts in tss.iter() {
            if ts.values[i].is_nan() {
                continue;
            }
            v = 1.0;
        }
        tss[0].values[i] = v;
    }
}

fn aggr_func_sum(tss: &mut [Timeseries]) {
    if tss.len() == 1 {
        // Fast path - nothing to sum.
        return;
    }
    for i in 0..tss[0].values.len() {
        let mut sum = 0f64;
        let mut count = 0usize;
        for ts in tss.iter() {
            let v = ts.values[i];
            if v.is_nan() {
                continue;
            }
            sum += v;
            count += 1;
        }
        if count == 0 {
            sum = f64::NAN;
        }
        tss[0].values[i] = sum;
    }
}

fn aggr_func_sum2(tss: &mut [Timeseries]) {
    for i in 0..tss[0].values.len() {
        let mut sum2 = 0f64;
        let mut count = 0usize;
        for ts in tss.iter() {
            let v = ts.values[i];
            if v.is_nan() {
                continue;
            }
            sum2 += v * v;
            count += 1;
        }
        if count == 0 {
            sum2 = f64::NAN;
        }
        tss[0].values[i] = sum2;
    }
}

fn aggr_func_geomean(tss: &mut [Timeseries]) {
    if tss.len() == 1 {
        // Fast path - nothing to geomean.
        return;
    }
    for i in 0..tss[0].values.len() {
        let mut p = 1f64;
        let mut count = 0usize;
        for ts in tss.iter() {
            let v = ts.values[i];
            if v.is_nan() {
                continue;
            }
            p *= v;
            count += 1;
        }
        if count == 0 {
            p = f64::NAN;
        }
        tss[0].values[i] = p.powf(1.0 / count as f64);
    }
}

fn aggr_func_min(tss: &mut [Timeseries]) {
    if tss.len() == 1 {
        // Fast path - nothing to min.
        return;
    }
    for i in 0..tss[0].values.len() {
        let mut min_v = tss[0].values[i];
        for ts in tss.iter() {
            if min_v.is_nan() || ts.values[i] < min_v {
                min_v = ts.values[i];
            }
        }
        tss[0].values[i] = min_v;
    }
}

fn aggr_func_max(tss: &mut [Timeseries]) {
    if tss.len() == 1 {
        // Fast path - nothing to max.
        return;
    }
    for i in 0..tss[0].values.len() {
        let mut max_v = tss[0].values[i];
        for ts in tss.iter() {
            if max_v.is_nan() || ts.values[i] > max_v {
                max_v = ts.values[i];
            }
        }
        tss[0].values[i] = max_v;
    }
}

fn aggr_func_avg(tss: &mut [Timeseries]) {
    if tss.len() == 1 {
        // Fast path - nothing to avg.
        return;
    }
    for i in 0..tss[0].values.len() {
        let mut sum = 0f64;
        let mut count = 0usize;
        for ts in tss.iter() {
            let v = ts.values[i];
            if v.is_nan() {
                continue;
            }
            count += 1;
            sum += v;
        }
        let mut avg = f64::NAN;
        if count > 0 {
            avg = sum / count as f64;
        }
        tss[0].values[i] = avg;
    }
}

fn aggr_func_count(tss: &mut [Timeseries]) {
    for i in 0..tss[0].values.len() {
        let mut count = 0usize;
        for ts in tss.iter() {
            if ts.values[i].is_nan() {
                continue;
            }
            count += 1;
        }
        let mut v = count as f64;
        if count == 0 {
            v = f64::NAN;
        }
        tss[0].values[i] = v;
    }
}

fn aggr_func_stddev(tss: &mut [Timeseries]) {
    if tss.len() == 1 {
        // Fast path - stddev over a single time series is zero.
        for v in tss[0].values.iter_mut() {
            if !v.is_nan() {
                *v = 0.0;
            }
        }
        return;
    }
    aggr_func_stdvar(tss);
    for v in tss[0].values.iter_mut() {
        *v = v.sqrt();
    }
}

fn aggr_func_stdvar(tss: &mut [Timeseries]) {
    if tss.len() == 1 {
        // Fast path - stdvar over a single time series is zero.
        for v in tss[0].values.iter_mut() {
            if !v.is_nan() {
                *v = 0.0;
            }
        }
        return;
    }
    for i in 0..tss[0].values.len() {
        // See `Rapid calculation methods` at
        // https://en.wikipedia.org/wiki/Standard_deviation
        let mut avg = 0f64;
        let mut count = 0f64;
        let mut q = 0f64;
        for ts in tss.iter() {
            let v = ts.values[i];
            if v.is_nan() {
                continue;
            }
            count += 1.0;
            let avg_new = avg + (v - avg) / count;
            q += (v - avg) * (v - avg_new);
            avg = avg_new;
        }
        if count == 0.0 {
            q = f64::NAN;
        }
        tss[0].values[i] = q / count;
    }
}

/// Port of Go `stddev` (rollup helper).
pub fn stddev(values: &[f64]) -> f64 {
    stdvar(values).sqrt()
}

/// Port of Go `stdvar` (rollup helper).
pub fn stdvar(values: &[f64]) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    if values.len() == 1 {
        // Fast path.
        return 0.0;
    }
    let mut avg = 0f64;
    let mut count = 0f64;
    let mut q = 0f64;
    for &v in values {
        if v.is_nan() {
            continue;
        }
        count += 1.0;
        let avg_new = avg + (v - avg) / count;
        q += (v - avg) * (v - avg_new);
        avg = avg_new;
    }
    if count == 0.0 {
        return f64::NAN;
    }
    q / count
}

/// Calculates the given `phi` quantile over `origin_values` without
/// modifying them, skipping NaNs. Port of Go `quantile`.
pub fn quantile(phi: f64, origin_values: &[f64]) -> f64 {
    let mut values: Vec<f64> = origin_values
        .iter()
        .copied()
        .filter(|v| !v.is_nan())
        .collect();
    values.sort_by(|a, b| a.partial_cmp(b).expect("NaNs are filtered out above"));
    quantile_sorted(phi, &values)
}

/// Calculates the given quantile over a sorted, NaN-free list of values.
/// The implementation mimics Prometheus for compatibility's sake.
/// Port of Go `quantileSorted`.
pub fn quantile_sorted(phi: f64, values: &[f64]) -> f64 {
    if values.is_empty() || phi.is_nan() {
        return f64::NAN;
    }
    if phi < 0.0 {
        return f64::NEG_INFINITY;
    }
    if phi > 1.0 {
        return f64::INFINITY;
    }
    let n = values.len() as f64;
    let rank = phi * (n - 1.0);
    let lower_index = rank.floor().max(0.0);
    let upper_index = (lower_index + 1.0).min(n - 1.0);
    let weight = rank - rank.floor();
    values[lower_index as usize] * (1.0 - weight) + values[upper_index as usize] * weight
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantile_matches_prometheus_interpolation() {
        let values = [1.0, 2.0, 3.0, 4.0];
        assert_eq!(quantile(0.0, &values), 1.0);
        assert_eq!(quantile(1.0, &values), 4.0);
        assert_eq!(quantile(0.5, &values), 2.5);
        assert!(quantile(0.5, &[]).is_nan());
        assert_eq!(quantile(-0.5, &values), f64::NEG_INFINITY);
        assert_eq!(quantile(1.5, &values), f64::INFINITY);
        // NaNs are skipped.
        assert_eq!(quantile(1.0, &[1.0, f64::NAN, 3.0]), 3.0);
    }

    #[test]
    fn stdvar_stddev_basic() {
        assert!(stdvar(&[]).is_nan());
        assert_eq!(stdvar(&[5.0]), 0.0);
        let v = stdvar(&[1.0, 2.0, 3.0, 4.0]);
        assert!((v - 1.25).abs() < 1e-15);
        assert!((stddev(&[1.0, 2.0, 3.0, 4.0]) - 1.25f64.sqrt()).abs() < 1e-15);
    }
}
