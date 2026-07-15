//! Expression evaluation dispatch and `EvalConfig`.
//! Port of `eval.go` (Stage-1 subset); the rollup evaluation drivers live in
//! [`crate::eval_rollup`].

use crate::aggr::{get_aggr_func, AggrFuncArg};
use crate::aggr_incremental::{get_incremental_aggr_func_callbacks, IncrementalAggrFuncContext};
use crate::binary_op::eval_binary_op_series;
use crate::eval_rollup::eval_rollup_func;
use crate::provider::MetricsProvider;
use crate::rollup_funcs::{self, NewRollupFunc, RollupArgValue, RollupFunc};
use crate::timeseries::{get_timestamps, Timeseries};
use crate::transform::{get_transform_func, TransformFuncArg};
use crate::{Error, Result};
use esm_metricsql::{AggrFuncExpr, BinaryOpExpr, Expr, FuncExpr, LabelFilter, RollupExpr};
use esm_storage::metric_name::MetricName;
use std::sync::{Arc, OnceLock};

use crate::provider::Deadline;

/// The maximum number of rollup workers per query
/// (`netstorage.MaxWorkers()` analog: `min(cpus, 32)`).
/// Resolved via [`esm_common::query_workers`]: the
/// `-search.maxWorkersPerQuery` flag, else the `ESM_MAX_QUERY_WORKERS`
/// env var (debug/benchmarking knob), else the auto default.
pub fn default_max_workers() -> usize {
    esm_common::query_workers::max_workers()
}

/// Configuration required for query evaluation. Port of Go `EvalConfig`.
#[derive(Debug)]
pub struct EvalConfig {
    pub start: i64,
    pub end: i64,
    pub step: i64,

    /// The maximum number of series which can be scanned by the query;
    /// 0 means no limit.
    pub max_series: usize,
    /// The limit on the number of points which can be generated per series.
    pub max_points_per_series: usize,

    pub deadline: Deadline,

    /// Whether the response can be cached (rollup result cache; Stage 2).
    pub may_cache: bool,

    /// Analog to `-query.lookback-delta` from Prometheus (0 = auto).
    pub lookback_delta: i64,

    /// How many decimal digits after the point to leave in the response;
    /// values >= 100 disable rounding.
    pub round_digits: i32,

    /// Additional label filters ANDed into every selector.
    pub enforced_tag_filterss: Vec<Vec<LabelFilter>>,

    /// `-search.noStaleMarkers` analog.
    pub no_stale_markers: bool,

    /// Rollup fan-out workers; 0 means `min(cpus, 32)`.
    pub max_workers: usize,

    timestamps: OnceLock<Arc<Vec<i64>>>,
}

impl Default for EvalConfig {
    fn default() -> Self {
        EvalConfig {
            start: 0,
            end: 0,
            step: 300_000,
            max_series: 0,
            max_points_per_series: 30_000,
            deadline: Deadline::none(),
            may_cache: false,
            lookback_delta: 0,
            round_digits: 100,
            enforced_tag_filterss: Vec::new(),
            no_stale_markers: false,
            max_workers: 0,
            timestamps: OnceLock::new(),
        }
    }
}

impl EvalConfig {
    /// A config for the given time range with default limits.
    pub fn new(start: i64, end: i64, step: i64) -> EvalConfig {
        EvalConfig {
            start,
            end,
            step,
            ..Default::default()
        }
    }

    /// Port of Go `copyEvalConfig` — timestamps are not copied, they must be
    /// regenerated for the new time range.
    pub fn copy_no_timestamps(&self) -> EvalConfig {
        EvalConfig {
            start: self.start,
            end: self.end,
            step: self.step,
            max_series: self.max_series,
            max_points_per_series: self.max_points_per_series,
            deadline: self.deadline,
            may_cache: self.may_cache,
            lookback_delta: self.lookback_delta,
            round_digits: self.round_digits,
            enforced_tag_filterss: self.enforced_tag_filterss.clone(),
            no_stale_markers: self.no_stale_markers,
            max_workers: self.max_workers,
            timestamps: OnceLock::new(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.start > self.end {
            return Err(Error::new(format!(
                "BUG: start cannot exceed end; got {} vs {}",
                self.start, self.end
            )));
        }
        if self.step <= 0 {
            return Err(Error::new(format!(
                "BUG: step must be greater than 0; got {}",
                self.step
            )));
        }
        crate::timeseries::validate_max_points_per_series(
            self.start,
            self.end,
            self.step,
            self.max_points_per_series,
        )
    }

    /// Whether the rollup result cache may be used (Stage-2 seam).
    /// Port of Go `EvalConfig.mayCache`.
    pub fn may_cache_results(&self) -> bool {
        if !self.may_cache {
            return false;
        }
        if self.start == self.end {
            return true;
        }
        self.start % self.step == 0 && self.end % self.step == 0
    }

    /// The shared output timestamps grid.
    pub fn shared_timestamps(&self) -> Arc<Vec<i64>> {
        Arc::clone(self.timestamps.get_or_init(|| {
            get_timestamps(self.start, self.end, self.step, self.max_points_per_series)
                .expect("BUG: EvalConfig must be validated before evaluation")
        }))
    }

    pub(crate) fn workers(&self) -> usize {
        if self.max_workers > 0 {
            self.max_workers
        } else {
            default_max_workers()
        }
    }
}

/// Port of Go `AdjustStartEnd` + `alignStartEnd` (used by the query_range
/// handler; exported for esm-select).
pub fn adjust_start_end(start: i64, end: i64, step: i64) -> (i64, i64) {
    const MIN_TIMESERIES_POINTS_FOR_TIME_ROUNDING: i64 = 50;
    let points = (end - start) / step + 1;
    if points < MIN_TIMESERIES_POINTS_FOR_TIME_ROUNDING {
        return (start, end);
    }
    let (new_start, mut new_end) = align_start_end(start, end, step);
    let mut new_points = (new_end - new_start) / step + 1;
    while new_points > points {
        new_end -= step;
        new_points -= 1;
    }
    (new_start, new_end)
}

pub fn align_start_end(mut start: i64, mut end: i64, step: i64) -> (i64, i64) {
    // Round start to the nearest smaller value divisible by step.
    start -= start % step;
    // Round end to the nearest bigger value divisible by step.
    let adjust = end % step;
    if adjust > 0 {
        end += step - adjust;
    }
    (start, end)
}

/// Port of Go `evalExpr`/`evalExprInternal`: the whole evaluator dispatch.
pub fn eval_expr(
    provider: &dyn MetricsProvider,
    ec: &EvalConfig,
    e: &Expr,
) -> Result<Vec<Timeseries>> {
    match e {
        Expr::Metric(me) => {
            let re = RollupExpr {
                expr: Box::new(Expr::Metric(me.clone())),
                window: None,
                offset: None,
                step: None,
                inherit_step: false,
                at: None,
            };
            eval_rollup_func(
                provider,
                ec,
                "default_rollup",
                default_rollup_func()?,
                e,
                &re,
                None,
            )
            .map_err(|err| Error::new(format!("cannot evaluate {me:?}: {err}", me = expr_str(e))))
        }
        Expr::Rollup(re) => eval_rollup_func(
            provider,
            ec,
            "default_rollup",
            default_rollup_func()?,
            e,
            re,
            None,
        )
        .map_err(|err| Error::new(format!("cannot evaluate {:?}: {err}", expr_str(e)))),
        Expr::Func(fe) => {
            let Some(nrf) = rollup_funcs::get_rollup_func(&fe.name) else {
                return eval_transform_func(provider, ec, fe);
            };
            let (args, re) = eval_rollup_func_args(provider, ec, fe)?;
            let rf = nrf(&args).map_err(|err| {
                Error::new(format!("cannot evaluate args for {:?}: {err}", expr_str(e)))
            })?;
            eval_rollup_func(
                provider,
                ec,
                &fe.name.to_ascii_lowercase(),
                rf,
                e,
                &re,
                None,
            )
            .map_err(|err| Error::new(format!("cannot evaluate {:?}: {err}", expr_str(e))))
        }
        Expr::Aggr(ae) => eval_aggr_func(provider, ec, ae),
        Expr::BinaryOp(be) => eval_binary_op(provider, ec, be),
        Expr::Number(ne) => Ok(eval_number(ec, ne.n)),
        Expr::String(se) => Ok(eval_string(ec, &se.s)),
        Expr::Duration(de) => {
            let d = de.duration(ec.step);
            Ok(eval_number(ec, d as f64 / 1000.0))
        }
        _ => Err(Error::new(format!(
            "unexpected expression {:?}",
            expr_str(e)
        ))),
    }
}

pub(crate) fn expr_str(e: &Expr) -> String {
    let mut s = String::new();
    e.append_string(&mut s);
    s
}

fn default_rollup_func() -> Result<RollupFunc> {
    let nrf = rollup_funcs::get_rollup_func("default_rollup").expect("default_rollup must exist");
    nrf(&[RollupArgValue::RollupExpr])
}

/// Port of Go `evalTransformFunc`.
fn eval_transform_func(
    provider: &dyn MetricsProvider,
    ec: &EvalConfig,
    fe: &FuncExpr,
) -> Result<Vec<Timeseries>> {
    let Some(tf) = get_transform_func(&fe.name) else {
        return Err(Error::new(format!("unknown func {:?}", fe.name)));
    };
    // Go evaluates `union` args in parallel; Stage 1 keeps this sequential
    // (parallel seam for Stage 2).
    let args = eval_exprs_sequentially(provider, ec, &fe.args)?;
    let mut tfa = TransformFuncArg { ec, fe, args };
    tf(&mut tfa)
        .map_err(|err| Error::new(format!("cannot evaluate {:?}: {err}", expr_str_func(fe))))
}

fn expr_str_func(fe: &FuncExpr) -> String {
    let mut s = String::new();
    fe.append_string(&mut s);
    s
}

/// Port of Go `evalAggrFunc`, including the incremental-aggregation fast
/// path detection.
fn eval_aggr_func(
    provider: &dyn MetricsProvider,
    ec: &EvalConfig,
    ae: &AggrFuncExpr,
) -> Result<Vec<Timeseries>> {
    if let Some(callbacks) = get_incremental_aggr_func_callbacks(&ae.name) {
        if let Some((fe, nrf)) = try_get_arg_rollup_func_with_metric_expr(ae) {
            // The optimized path for calculating AggrFuncExpr over
            // rollupFunc over MetricExpr: saves RAM for aggregates over a
            // big number of series.
            let (args, re) = eval_rollup_func_args(provider, ec, &fe)?;
            let rf = nrf(&args).map_err(|err| {
                Error::new(format!("cannot evaluate args for aggregate func: {err}"))
            })?;
            let iafc = IncrementalAggrFuncContext::new(ae, callbacks, ec.workers());
            let expr = Expr::Aggr(ae.clone());
            return eval_rollup_func(
                provider,
                ec,
                &fe.name.to_ascii_lowercase(),
                rf,
                &expr,
                &re,
                Some(&iafc),
            );
        }
    }
    let args = eval_exprs_sequentially(provider, ec, &ae.args)?;
    let Some(af) = get_aggr_func(&ae.name) else {
        return Err(Error::new(format!("unknown func {:?}", ae.name)));
    };
    let mut afa = AggrFuncArg { args, ae, ec };
    af(&mut afa).map_err(|err| Error::new(format!("cannot evaluate aggregate: {err}")))
}

/// Port of Go `tryGetArgRollupFuncWithMetricExpr`: matches
/// `aggr(metricExpr)`, `aggr(metricExpr[d])`, `aggr(rollupFunc(metricExpr))`
/// and `aggr(rollupFunc(metricExpr[d]))` with a plain MetricExpr and no
/// subquery.
fn try_get_arg_rollup_func_with_metric_expr(
    ae: &AggrFuncExpr,
) -> Option<(FuncExpr, NewRollupFunc)> {
    if ae.args.len() != 1 {
        return None;
    }
    let e = &ae.args[0];

    let default_rollup_fe = |arg: Expr| -> Option<(FuncExpr, NewRollupFunc)> {
        let fe = FuncExpr {
            name: "default_rollup".to_string(),
            args: vec![arg],
            keep_metric_names: false,
        };
        let nrf = rollup_funcs::get_rollup_func(&fe.name)?;
        Some((fe, nrf))
    };

    match e {
        Expr::Metric(me) => {
            if me.is_empty() {
                return None;
            }
            default_rollup_fe(e.clone())
        }
        Expr::Rollup(re) => {
            let Expr::Metric(me) = &*re.expr else {
                return None;
            };
            if me.is_empty() || re.for_subquery() {
                return None;
            }
            default_rollup_fe(e.clone())
        }
        Expr::Func(fe) => {
            let nrf = rollup_funcs::get_rollup_func(&fe.name)?;
            let rollup_arg_idx = esm_metricsql::get_rollup_arg_idx(fe)?;
            if rollup_arg_idx >= fe.args.len() {
                // Incorrect number of args for the rollup func.
                return None;
            }
            match &fe.args[rollup_arg_idx] {
                Expr::Metric(me) => {
                    if me.is_empty() {
                        return None;
                    }
                    Some((fe.clone(), nrf))
                }
                Expr::Rollup(re) => {
                    let Expr::Metric(me) = &*re.expr else {
                        return None;
                    };
                    if me.is_empty() || re.for_subquery() {
                        return None;
                    }
                    Some((fe.clone(), nrf))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Port of Go `evalBinaryOp` + `execBinaryOpArgs` (without the Stage-2
/// common-label-filter pushdown).
fn eval_binary_op(
    provider: &dyn MetricsProvider,
    ec: &EvalConfig,
    be: &BinaryOpExpr,
) -> Result<Vec<Timeseries>> {
    if !crate::binary_op::is_supported_binary_op(&be.op) {
        return Err(Error::new(format!("unknown binary op {:?}", be.op)));
    }
    let op = be.op.to_ascii_lowercase();
    let (left, right) = match op.as_str() {
        // Fetch right-side series first for `and`/`if`, since it usually
        // contains a lower number of series.
        "and" | "if" => {
            let (right, left) = exec_binary_op_args(provider, ec, &be.right, &be.left, be)?;
            (left, right)
        }
        _ => exec_binary_op_args(provider, ec, &be.left, &be.right, be)?,
    };
    eval_binary_op_series(be, left, right)
        .map_err(|err| Error::new(format!("cannot evaluate binary op {:?}: {err}", be.op)))
}

fn can_pushdown_common_filters(be: &BinaryOpExpr) -> bool {
    match be.op.to_ascii_lowercase().as_str() {
        "or" | "default" => false,
        _ => !(is_aggr_func_without_grouping(&be.left) || is_aggr_func_without_grouping(&be.right)),
    }
}

fn is_aggr_func_without_grouping(e: &Expr) -> bool {
    match e {
        Expr::Aggr(afe) => afe.modifier.args.is_empty(),
        _ => false,
    }
}

fn exec_binary_op_args(
    provider: &dyn MetricsProvider,
    ec: &EvalConfig,
    expr_first: &Expr,
    expr_second: &Expr,
    be: &BinaryOpExpr,
) -> Result<(Vec<Timeseries>, Vec<Timeseries>)> {
    if !can_pushdown_common_filters(be) {
        // Go executes both sides in parallel here; Stage 1 is sequential.
        let tss_first = eval_expr(provider, ec, expr_first)?;
        let tss_second = eval_expr(provider, ec, expr_second)?;
        return Ok((tss_first, tss_second));
    }
    // PORT-SKIP (Stage 2): getCommonLabelFilters + PushdownBinaryOpFilters —
    // pushing common label filters from the first side into the second side
    // is a performance optimization only; correctness is identical.
    let tss_first = eval_expr(provider, ec, expr_first)?;
    if tss_first.is_empty() && !be.op.eq_ignore_ascii_case("or") {
        // Fast path: no sense in executing the second side when the first
        // side returns an empty result.
        return Ok((Vec::new(), Vec::new()));
    }
    let tss_second = eval_expr(provider, ec, expr_second)?;
    Ok((tss_first, tss_second))
}

fn eval_exprs_sequentially(
    provider: &dyn MetricsProvider,
    ec: &EvalConfig,
    es: &[Expr],
) -> Result<Vec<Vec<Timeseries>>> {
    let mut rvs = Vec::with_capacity(es.len());
    for e in es {
        rvs.push(eval_expr(provider, ec, e)?);
    }
    Ok(rvs)
}

/// Port of Go `evalRollupFuncArgs`.
fn eval_rollup_func_args(
    provider: &dyn MetricsProvider,
    ec: &EvalConfig,
    fe: &FuncExpr,
) -> Result<(Vec<RollupArgValue>, RollupExpr)> {
    let Some(rollup_arg_idx) = esm_metricsql::get_rollup_arg_idx(fe) else {
        return Err(Error::new(format!(
            "BUG: {:?} isn't a rollup func",
            fe.name
        )));
    };
    if fe.args.len() <= rollup_arg_idx {
        return Err(Error::new(format!(
            "expecting at least {} args to {:?}; got {} args",
            rollup_arg_idx + 1,
            fe.name,
            fe.args.len()
        )));
    }
    let mut args = Vec::with_capacity(fe.args.len());
    let mut re = None;
    for (i, arg) in fe.args.iter().enumerate() {
        if i == rollup_arg_idx {
            let r = get_rollup_expr_arg(arg);
            re = Some(r);
            args.push(RollupArgValue::RollupExpr);
            continue;
        }
        let ts = eval_expr(provider, ec, arg).map_err(|err| {
            Error::new(format!(
                "cannot evaluate arg #{} for {:?}: {err}",
                i + 1,
                fe.name
            ))
        })?;
        args.push(RollupArgValue::Series(ts));
    }
    Ok((args, re.expect("rollup_arg_idx is within fe.args")))
}

/// Port of Go `getRollupExprArg`.
fn get_rollup_expr_arg(arg: &Expr) -> RollupExpr {
    let re = match arg {
        Expr::Rollup(re) => re.clone(),
        _ => {
            // Wrap a non-rollup arg into a RollupExpr.
            return RollupExpr {
                expr: Box::new(arg.clone()),
                window: None,
                offset: None,
                step: None,
                inherit_step: false,
                at: None,
            };
        }
    };
    if !re.for_subquery() {
        // Return the standard rollup if it doesn't contain a subquery.
        return re;
    }
    let Expr::Metric(_) = &*re.expr else {
        // The arg contains a subquery.
        return re;
    };
    // Convert `me[w:step]` -> `default_rollup(me)[w:step]`.
    let mut re_new = re.clone();
    re_new.expr = Box::new(Expr::Func(FuncExpr {
        name: "default_rollup".to_string(),
        args: vec![Expr::Rollup(RollupExpr {
            expr: re.expr.clone(),
            window: None,
            offset: None,
            step: None,
            inherit_step: false,
            at: None,
        })],
        keep_metric_names: false,
    }));
    re_new
}

/// Port of Go `evalRollupFunc`: handles the `@` modifier, then delegates.
/// Port of Go `evalNumber`: a constant series over the shared grid.
pub fn eval_number(ec: &EvalConfig, n: f64) -> Vec<Timeseries> {
    let timestamps = ec.shared_timestamps();
    vec![Timeseries {
        metric_name: MetricName::default(),
        values: vec![n; timestamps.len()],
        timestamps,
    }]
}

/// Port of Go `evalString`.
pub fn eval_string(ec: &EvalConfig, s: &str) -> Vec<Timeseries> {
    let mut rv = eval_number(ec, f64::NAN);
    rv[0].metric_name.metric_group = s.as_bytes().to_vec();
    rv
}

/// Port of Go `evalTime`.
pub fn eval_time(ec: &EvalConfig) -> Vec<Timeseries> {
    let mut rv = eval_number(ec, f64::NAN);
    let timestamps = Arc::clone(&rv[0].timestamps);
    for (i, &ts) in timestamps.iter().enumerate() {
        rv[0].values[i] = ts as f64 / 1e3;
    }
    rv
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn align_start_end_works() {
        assert_eq!(align_start_end(101, 199, 100), (100, 200));
        assert_eq!(align_start_end(100, 200, 100), (100, 200));
    }

    #[test]
    fn adjust_start_end_preserves_point_count() {
        let (start, end) = (1000, 1000 + 99 * 7);
        let points = (end - start) / 7 + 1;
        let (s2, e2) = adjust_start_end(start, end, 7);
        assert_eq!((e2 - s2) / 7 + 1, points);
        assert_eq!(s2 % 7, 0);
    }
}
