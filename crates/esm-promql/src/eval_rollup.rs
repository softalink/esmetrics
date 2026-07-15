//! Rollup evaluation drivers: window parsing, data fetch, memory gating and
//! the parallel per-series fan-out. Port of the `evalRollupFunc*` family
//! from `eval.go` (Stage-1 subset).

use crate::aggr_incremental::IncrementalAggrFuncContext;
use crate::eval::{eval_expr, eval_number, expr_str, EvalConfig};
use crate::memory_limiter::get_rollup_memory_limiter;
use crate::provider::{Deadline, MetricsProvider, SearchQuery, Series};
use crate::rollup::{
    drop_stale_nans, get_rollup_configs, max_silence_interval, PreFunc, RollupConfig,
};
use crate::rollup_funcs::{self, RollupFunc};
use crate::rollup_result_cache::{merge_series, rollup_result_cache};
use crate::timeseries::{get_timestamps, Timeseries};
use crate::transform::get_absent_timeseries;
use crate::{Error, Result};
use esm_metricsql::{DurationExpr, Expr, LabelFilter, MetricExpr, RollupExpr};
use esm_storage::metric_name::MetricName;
use std::sync::Arc;

#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_rollup_func(
    provider: &dyn MetricsProvider,
    ec: &EvalConfig,
    func_name: &str,
    rf: RollupFunc,
    expr: &Expr,
    re: &RollupExpr,
    iafc: Option<&IncrementalAggrFuncContext<'_>>,
) -> Result<Vec<Timeseries>> {
    let Some(at) = &re.at else {
        return eval_rollup_func_without_at(provider, ec, func_name, rf, expr, re, iafc);
    };
    let tss_at = eval_expr(provider, ec, at)
        .map_err(|err| Error::new(format!("cannot evaluate `@` modifier: {err}")))?;
    if tss_at.len() != 1 {
        return Err(Error::new(format!(
            "`@` modifier must return a single series; it returns {} series instead",
            tss_at.len()
        )));
    }
    let at_value = tss_at[0]
        .values
        .iter()
        .copied()
        .find(|v| !v.is_nan())
        .ok_or_else(|| Error::new("`@` modifier must return a non-NaN value"))?;
    let at_timestamp = (at_value * 1000.0) as i64;
    let mut ec_new = ec.copy_no_timestamps();
    ec_new.start = at_timestamp;
    ec_new.end = at_timestamp;
    let tss = eval_rollup_func_without_at(provider, &ec_new, func_name, rf, expr, re, iafc)?;
    // Expand the single-point series to the original time range.
    let timestamps = ec.shared_timestamps();
    Ok(tss
        .into_iter()
        .map(|ts| {
            let v = ts.values[0];
            Timeseries {
                metric_name: ts.metric_name,
                values: vec![v; timestamps.len()],
                timestamps: Arc::clone(&timestamps),
            }
        })
        .collect())
}

/// Port of Go `evalRollupFuncWithoutAt`.
#[allow(clippy::too_many_arguments)]
fn eval_rollup_func_without_at(
    provider: &dyn MetricsProvider,
    ec: &EvalConfig,
    func_name: &str,
    rf: RollupFunc,
    expr: &Expr,
    re: &RollupExpr,
    iafc: Option<&IncrementalAggrFuncContext<'_>>,
) -> Result<Vec<Timeseries>> {
    let mut ec_new = ec;
    let ec_offset;
    let mut offset: i64 = 0;
    if let Some(offset_expr) = &re.offset {
        offset = offset_expr.duration(ec.step);
        ec_offset = {
            let mut c = ec.copy_no_timestamps();
            c.start -= offset;
            c.end -= offset;
            c
        };
        ec_new = &ec_offset;
    }
    // PORT-SKIP: the `rollup_candlestick` -step offset (the function itself
    // is Stage 2).
    let mut rvs = match &*re.expr {
        Expr::Metric(me) => eval_rollup_func_with_metric_expr(
            provider,
            ec_new,
            func_name,
            rf,
            expr,
            me,
            iafc,
            re.window.as_ref(),
        )?,
        _ => {
            assert!(
                iafc.is_none(),
                "BUG: iafc must be None for rollup {func_name:?} over a subquery"
            );
            // PORT-SKIP (Stage 2): evalRollupFuncWithSubquery.
            return Err(Error::new(format!(
                "subqueries are not supported yet: {:?}",
                expr_str(&re.expr.as_ref().clone())
            )));
        }
    };
    if func_name == "absent_over_time" {
        rvs = aggregate_absent_over_time(ec_new, &re.expr, rvs);
    }
    if offset != 0 && !rvs.is_empty() {
        // Make a copy of timestamps, since they may be used in other values.
        let dst_timestamps: Vec<i64> = rvs[0].timestamps.iter().map(|ts| ts + offset).collect();
        let shared = Arc::new(dst_timestamps);
        for ts in rvs.iter_mut() {
            ts.timestamps = Arc::clone(&shared);
        }
    }
    Ok(rvs)
}

/// Port of Go `aggregateAbsentOverTime`: collapses tss to a single series
/// with 1 and NaN values.
fn aggregate_absent_over_time(
    ec: &EvalConfig,
    expr: &Expr,
    tss: Vec<Timeseries>,
) -> Vec<Timeseries> {
    let mut rvs = get_absent_timeseries(ec, expr);
    if tss.is_empty() {
        return rvs;
    }
    for i in 0..tss[0].values.len() {
        if tss.iter().any(|ts| ts.values[i].is_nan()) {
            rvs[0].values[i] = f64::NAN;
        }
    }
    rvs
}

/// Port of Go `getKeepMetricNames`.
fn get_keep_metric_names(expr: &Expr) -> bool {
    let mut expr = expr;
    if let Expr::Aggr(ae) = expr {
        // Extract rollupFunc(...) from aggrFunc(rollupFunc(...)) — the
        // incremental aggregation case.
        if ae.args.len() != 1 {
            return false;
        }
        expr = &ae.args[0];
    }
    match expr {
        Expr::Func(fe) => fe.keep_metric_names,
        _ => false,
    }
}

/// Port of Go `evalRollupFuncWithMetricExpr`, including the rollup result
/// cache lookup/merge/store around `eval_rollup_func_no_cache`.
#[allow(clippy::too_many_arguments)]
fn eval_rollup_func_with_metric_expr(
    provider: &dyn MetricsProvider,
    ec: &EvalConfig,
    func_name: &str,
    rf: RollupFunc,
    expr: &Expr,
    me: &MetricExpr,
    iafc: Option<&IncrementalAggrFuncContext<'_>>,
    window_expr: Option<&DurationExpr>,
) -> Result<Vec<Timeseries>> {
    let window = match window_expr {
        Some(we) => we.non_negative_duration(ec.step).map_err(|err| {
            Error::new(format!(
                "cannot parse lookbehind window in square brackets: {err}"
            ))
        })?,
        None => 0,
    };
    if me.is_empty() {
        return Ok(eval_number(ec, f64::NAN));
    }

    // PORT-SKIP (Stage 2): evalInstantRollup optimizations (+ their
    // instant-values cache) for ec.start == ec.end.
    let points_per_series = if ec.start == ec.end {
        1
    } else {
        1 + (ec.end - ec.start) / ec.step
    };
    let rf = &rf;
    let eval_with_config = |ec_arg: &EvalConfig| {
        eval_rollup_func_no_cache(
            provider,
            ec_arg,
            func_name,
            Arc::clone(rf),
            expr,
            me,
            iafc,
            window,
            points_per_series,
        )
    };
    if ec.start == ec.end || !ec.may_cache_results() {
        // Instant queries use the (not yet ported) instant-values cache in
        // Go, never the series cache; `nocache` disables caching entirely.
        return eval_with_config(ec);
    }

    // Search for cached results covering a prefix of [start .. end].
    let cache = rollup_result_cache();
    let (tss_cached, start) = cache.get_series(ec, expr, window);
    if start > ec.end {
        // The result is fully cached: return it directly, like Go
        // (rollupResultCacheFullHits). Fully cached queries report
        // seriesFetched=0, matching the upstream.
        return Ok(tss_cached);
    }

    // Fetch the missing tail, which isn't cached yet.
    let tss = if start != ec.start {
        let mut ec_new = ec.copy_no_timestamps();
        ec_new.start = start;
        eval_with_config(&ec_new)?
    } else {
        eval_with_config(ec)?
    };

    // Merge the cached results with the fetched additional results.
    let rvs = match merge_series(tss_cached, tss, start, ec) {
        Some(rvs) => rvs,
        // Cannot merge series — fall back to non-cached querying.
        None => eval_with_config(ec)?,
    };
    cache.put_series(ec, expr, window, &rvs);
    Ok(rvs)
}

/// Port of Go `searchutil.JoinTagFilterss`: cross-products the selector
/// filters with the enforced extra filters.
fn join_tag_filterss(src: &[Vec<LabelFilter>], etfs: &[Vec<LabelFilter>]) -> Vec<Vec<LabelFilter>> {
    if src.is_empty() {
        return etfs.to_vec();
    }
    if etfs.is_empty() {
        return src.to_vec();
    }
    let mut dst = Vec::with_capacity(src.len() * etfs.len());
    for tf in src {
        for etf in etfs {
            let mut tfs = tf.clone();
            tfs.extend(etf.iter().cloned());
            dst.push(tfs);
        }
    }
    dst
}

/// RAII guard for the rollup memory limiter reservation.
struct MemoryReservation {
    n: u64,
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        get_rollup_memory_limiter().put(self.n);
    }
}

/// Port of Go `evalRollupFuncNoCache`.
#[allow(clippy::too_many_arguments)]
fn eval_rollup_func_no_cache(
    provider: &dyn MetricsProvider,
    ec: &EvalConfig,
    func_name: &str,
    rf: RollupFunc,
    expr: &Expr,
    me: &MetricExpr,
    iafc: Option<&IncrementalAggrFuncContext<'_>>,
    window: i64,
    points_per_series: i64,
) -> Result<Vec<Timeseries>> {
    if window < 0 {
        return Ok(Vec::new());
    }
    // Obtain rollup configs before fetching data, so type errors are caught
    // earlier.
    let shared_timestamps = get_timestamps(ec.start, ec.end, ec.step, ec.max_points_per_series)?;
    let (pre_func, rcs) = get_rollup_configs(
        func_name,
        rf,
        expr,
        ec.start,
        ec.end,
        ec.step,
        ec.max_points_per_series,
        window,
        ec.lookback_delta,
        Arc::clone(&shared_timestamps),
    )?;

    // Fetch the result.
    let tfss = join_tag_filterss(&me.label_filterss, &ec.enforced_tag_filterss);
    let mut min_timestamp = ec.start;
    if rollup_funcs::need_silence_interval(func_name) {
        min_timestamp -= max_silence_interval();
    }
    if window > ec.step {
        min_timestamp -= window;
    } else {
        min_timestamp -= ec.step;
    }
    let sq = SearchQuery {
        start: min_timestamp,
        end: ec.end,
        tag_filterss: tfss,
        max_metrics: ec.max_series,
    };
    let series = provider.search(&sq, ec.deadline)?;
    if series.is_empty() {
        return Ok(Vec::new());
    }

    // Verify that the series fit available memory during the rollup.
    let rss_len = series.len();
    let mut timeseries_len = rss_len;
    if let Some(iafc) = iafc {
        // Incremental aggregates require holding only workers × groups
        // series in memory.
        timeseries_len = ec.workers();
        if !iafc.ae.modifier.op.is_empty() {
            if iafc.ae.limit > 0 {
                // There is an explicit limit on the number of output series.
                timeseries_len *= iafc.ae.limit as usize;
            } else {
                // Increase the estimate for non-empty group lists, since
                // each group can have its own set of series in memory.
                timeseries_len *= 1000;
            }
        }
        // The maximum number of output series is limited by rss_len.
        timeseries_len = timeseries_len.min(rss_len);
    }
    let rollup_points = mul_no_overflow(points_per_series, (timeseries_len * rcs.len()) as i64);
    let rollup_memory_size = sum_no_overflow(
        mul_no_overflow(timeseries_len as i64, 1000),
        mul_no_overflow(rollup_points, 16),
    );
    let rml = get_rollup_memory_limiter();
    if !rml.get(rollup_memory_size as u64) {
        return Err(Error::new(format!(
            "not enough memory for processing the query, which returns {rollup_points} data points \
             across {} time series with {points_per_series} points in each time series; \
             total available memory for concurrent requests: {} bytes; requested memory: {rollup_memory_size} bytes; \
             possible solutions are: reducing the number of matching time series; increasing `step` query arg (step={}s)",
            timeseries_len * rcs.len(),
            rml.max_size,
            ec.step as f64 / 1e3,
        )));
    }
    let _reservation = MemoryReservation {
        n: rollup_memory_size as u64,
    };

    // Evaluate the rollup.
    let keep_metric_names = get_keep_metric_names(expr);
    match iafc {
        Some(iafc) => eval_rollup_with_incremental_aggregate(
            ec,
            func_name,
            keep_metric_names,
            iafc,
            series,
            &rcs,
            pre_func,
            &shared_timestamps,
        ),
        None => eval_rollup_no_incremental_aggregate(
            ec,
            func_name,
            keep_metric_names,
            series,
            &rcs,
            pre_func,
            &shared_timestamps,
        ),
    }
}

/// Port of Go `evalRollupWithIncrementalAggregate`.
#[allow(clippy::too_many_arguments)]
fn eval_rollup_with_incremental_aggregate(
    ec: &EvalConfig,
    func_name: &str,
    keep_metric_names: bool,
    iafc: &IncrementalAggrFuncContext<'_>,
    series: Vec<Series>,
    rcs: &[RollupConfig],
    pre_func: PreFunc,
    shared_timestamps: &Arc<Vec<i64>>,
) -> Result<Vec<Timeseries>> {
    let no_stale_markers = ec.no_stale_markers;
    run_parallel(series, ec.workers(), ec.deadline, &|rs, worker_id, _out| {
        let Series {
            metric_name,
            timestamps,
            values,
        } = rs;
        let (mut values, timestamps) =
            drop_stale_nans(func_name, values, timestamps, no_stale_markers);
        pre_func(&mut values, &timestamps);
        for rc in rcs {
            let mut ts = do_rollup_for_timeseries(
                func_name,
                keep_metric_names,
                rc,
                &metric_name,
                &values,
                &timestamps,
                shared_timestamps,
            );
            iafc.update_timeseries(&mut ts, worker_id);
        }
        Ok(())
    })?;
    Ok(iafc.finalize_timeseries())
}

/// Port of Go `evalRollupNoIncrementalAggregate`.
#[allow(clippy::too_many_arguments)]
fn eval_rollup_no_incremental_aggregate(
    ec: &EvalConfig,
    func_name: &str,
    keep_metric_names: bool,
    series: Vec<Series>,
    rcs: &[RollupConfig],
    pre_func: PreFunc,
    shared_timestamps: &Arc<Vec<i64>>,
) -> Result<Vec<Timeseries>> {
    let no_stale_markers = ec.no_stale_markers;
    run_parallel(series, ec.workers(), ec.deadline, &|rs, _worker_id, out| {
        let Series {
            metric_name,
            timestamps,
            values,
        } = rs;
        let (mut values, timestamps) =
            drop_stale_nans(func_name, values, timestamps, no_stale_markers);
        pre_func(&mut values, &timestamps);
        for rc in rcs {
            let ts = do_rollup_for_timeseries(
                func_name,
                keep_metric_names,
                rc,
                &metric_name,
                &values,
                &timestamps,
                shared_timestamps,
            );
            out.push(ts);
        }
        Ok(())
    })
}

/// Fans the per-series work out over `min(max_workers, len)` stable
/// per-query worker slots executed on the persistent
/// [`crate::worker_pool`] (the Go netstorage `RunParallel` analog with
/// explicit worker ids). Series are distributed round-robin over the slots,
/// matching the Go worker distribution; the calling thread participates in
/// slot execution, so no per-evaluation threads are spawned.
fn run_parallel<F>(
    series: Vec<Series>,
    max_workers: usize,
    deadline: Deadline,
    f: &F,
) -> Result<Vec<Timeseries>>
where
    F: Fn(Series, usize, &mut Vec<Timeseries>) -> Result<()> + Sync,
{
    let workers = max_workers.max(1).min(series.len().max(1));
    if workers == 1 || series.len() == 1 {
        // Fast path.
        let mut out = Vec::new();
        for rs in series {
            deadline.check()?;
            f(rs, 0, &mut out)?;
        }
        return Ok(out);
    }

    // Distribute the series round-robin over per-slot queues.
    let mut queues: Vec<Vec<Series>> = (0..workers).map(|_| Vec::new()).collect();
    for (i, rs) in series.into_iter().enumerate() {
        queues[i % workers].push(rs);
    }
    // Each slot is claimed exactly once, so the mutexes are uncontended;
    // they only make the per-slot state shareable across pool threads.
    struct SlotState {
        input: Vec<Series>,
        output: Result<Vec<Timeseries>>,
    }
    let slots: Vec<parking_lot::Mutex<SlotState>> = queues
        .into_iter()
        .map(|input| {
            parking_lot::Mutex::new(SlotState {
                input,
                output: Ok(Vec::new()),
            })
        })
        .collect();
    crate::worker_pool::run_slots(workers, &|worker_id| {
        let mut slot = slots[worker_id].lock();
        let input = std::mem::take(&mut slot.input);
        let mut out = Vec::new();
        let mut result = Ok(());
        for rs in input {
            result = deadline.check().and_then(|()| f(rs, worker_id, &mut out));
            if result.is_err() {
                break;
            }
        }
        slot.output = result.map(|()| out);
    });
    let mut tss = Vec::new();
    for slot in slots {
        tss.extend(slot.into_inner().output?);
    }
    Ok(tss)
}

/// Port of Go `doRollupForTimeseries`.
fn do_rollup_for_timeseries(
    func_name: &str,
    keep_metric_names: bool,
    rc: &RollupConfig,
    mn_src: &MetricName,
    values_src: &[f64],
    timestamps_src: &[i64],
    shared_timestamps: &Arc<Vec<i64>>,
) -> Timeseries {
    let mut metric_name = mn_src.clone();
    if !rc.tag_value.is_empty() {
        metric_name.add_tag("rollup", &rc.tag_value);
    }
    if !keep_metric_names && !rollup_funcs::keep_metric_name(func_name) {
        metric_name.reset_metric_group();
    }
    let (values, _samples_scanned) = rc.exec(Vec::new(), values_src, timestamps_src);
    Timeseries {
        metric_name,
        values,
        timestamps: Arc::clone(shared_timestamps),
    }
}

fn mul_no_overflow(a: i64, b: i64) -> i64 {
    a.checked_mul(b).unwrap_or(i64::MAX)
}

fn sum_no_overflow(a: i64, b: i64) -> i64 {
    a.checked_add(b).unwrap_or(i64::MAX)
}
