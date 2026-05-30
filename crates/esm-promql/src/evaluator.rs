//! PromQL evaluator (Phase 3 MVP).
//!
//! Evaluates a parsed [`Expr`] against an [`esm_storage::Storage`] at a
//! single timestamp ("instant query"). Returns a [`Value`] that is either a
//! scalar number or an instant vector (one sample per matching series).
//!
//! Supported in this MVP:
//! - Numeric literals.
//! - Vector selectors (with all 4 label-matcher operators).
//! - Unary `+` / `-` on numbers (and broadcast over vectors).
//! - Binary arithmetic + comparison ops where one or both sides is a
//!   scalar; comparison ops without `bool` filter the vector to elements
//!   that satisfy the predicate.
//!
//! Deferred to subsequent sub-phases:
//! - Vector-vs-vector matching (`on`/`ignoring`/`group_left`/`group_right`).
//! - Function calls (`rate`, `sum`, `avg`, `histogram_quantile`, …).
//! - Aggregations (`sum by (...)`).
//! - Subqueries.
//! - Logical operators (`and` / `or` / `unless`) — surfaced today as
//!   [`EvalError::NotYetImplemented`].

// Pedantic clippy lints we accept noise from in this module:
#![allow(clippy::match_wildcard_for_single_variants)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::expect_used)]
#![allow(clippy::single_match_else)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::redundant_closure_for_method_calls)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::type_complexity)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::unnecessary_cast)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::map_unwrap_or)]

use std::collections::BTreeMap;

use esm_storage::timeseries::Tsid;
use esm_storage::{QueryStore, StorageError, StoredSample, TimeRange};
use thiserror::Error;

use crate::ast::{
    BinaryExpr, BinaryOp, Expr, GroupSide, MatchOp, UnaryOp, VectorMatching, VectorMatchingKind,
    VectorSelector,
};

/// One element of an instant vector: a labelled series and its single
/// sample at the eval timestamp.
#[derive(Debug, Clone)]
pub struct InstantVectorElement {
    /// Canonical metric identifier — for now we use the same raw bytes
    /// the storage layer stores. Real PromQL would carry a parsed
    /// label-set; that lands when `esm_storage` tracks labels first-class.
    pub metric_name: Vec<u8>,
    pub timestamp_ms: i64,
    pub value: f64,
}

/// The two value kinds PromQL can produce from an instant query.
#[derive(Debug, Clone)]
pub enum Value {
    Scalar(f64),
    InstantVector(Vec<InstantVectorElement>),
}

impl Value {
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Scalar(_) => "scalar",
            Self::InstantVector(_) => "vector",
        }
    }
}

/// Context passed to the evaluator. The Phase 3 MVP supports instant
/// queries; range queries are produced by calling the evaluator repeatedly
/// at each step.
#[derive(Debug, Clone, Copy)]
pub struct EvalContext {
    /// Evaluation timestamp (epoch millis).
    pub timestamp_ms: i64,
    /// How far back to look for the most recent sample. Matches
    /// Prometheus's default lookback delta of 5 minutes.
    pub lookback_ms: i64,
}

impl EvalContext {
    /// Default lookback delta (5 minutes) used by Prometheus.
    pub const DEFAULT_LOOKBACK_MS: i64 = 5 * 60 * 1000;
}

impl EvalContext {
    /// Construct an instant-query context at `timestamp_ms` with the
    /// Prometheus-default lookback.
    #[must_use]
    pub const fn instant(timestamp_ms: i64) -> Self {
        Self { timestamp_ms, lookback_ms: Self::DEFAULT_LOOKBACK_MS }
    }
}

/// One series in a range-query result: a metric identifier and a list of
/// (timestamp_ms, value) points across the query range.
#[derive(Debug, Clone)]
pub struct RangeVectorElement {
    pub metric_name: Vec<u8>,
    pub values: Vec<(i64, f64)>,
}

/// Run a range query: evaluate `expr` at each step from `start_ms` to
/// `end_ms` (inclusive) with `step_ms` between samples. Produces a matrix
/// of `(metric_name, [(timestamp, value), ...])` series.
///
/// # Errors
/// See [`EvalError`].
/// Largest `[range]` (in ms) over all selectors in `expr`, including subquery
/// ranges. Zero if the expression has no range vector.
fn max_selector_range(expr: &Expr) -> i64 {
    match expr {
        Expr::VectorSelector(s) => s.range_ms.unwrap_or(0),
        Expr::Paren(e) | Expr::Unary(_, e) => max_selector_range(e),
        Expr::Binary(b) => max_selector_range(&b.lhs).max(max_selector_range(&b.rhs)),
        Expr::Aggregation(a) => {
            let mut m = max_selector_range(&a.arg);
            if let Some(p) = &a.param {
                m = m.max(max_selector_range(p));
            }
            m
        }
        Expr::FunctionCall(fc) => fc.args.iter().map(max_selector_range).max().unwrap_or(0),
        Expr::Subquery(sq) => max_selector_range(&sq.inner).max(sq.range_ms),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => 0,
    }
}

/// Collect every vector selector in `expr` (for parallel cache warm-up).
fn collect_selectors<'a>(expr: &'a Expr, out: &mut Vec<&'a crate::ast::VectorSelector>) {
    match expr {
        Expr::VectorSelector(s) => out.push(s),
        Expr::Paren(e) | Expr::Unary(_, e) => collect_selectors(e, out),
        Expr::Binary(b) => {
            collect_selectors(&b.lhs, out);
            collect_selectors(&b.rhs, out);
        }
        Expr::Aggregation(a) => {
            collect_selectors(&a.arg, out);
            if let Some(p) = &a.param {
                collect_selectors(p, out);
            }
        }
        Expr::FunctionCall(fc) => {
            for a in &fc.args {
                collect_selectors(a, out);
            }
        }
        Expr::Subquery(sq) => collect_selectors(&sq.inner, out),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
    }
}

/// Per-query read cache wrapping a [`QueryStore`]. The first
/// `search_by_metric_name` for a series reads the whole preload window once and
/// memoizes it; later step queries are served from that buffer (filtered to the
/// requested sub-range). Metadata lookups delegate to the inner store. Lives
/// for a single `evaluate_range` call; single-threaded, hence `RefCell`.
struct RangeCache<'a, S: QueryStore> {
    inner: &'a S,
    lo: i64,
    hi: i64,
    series: std::cell::RefCell<std::collections::HashMap<Vec<u8>, Vec<StoredSample>>>,
    /// Total samples currently memoized; bounds per-query memory.
    cached_samples: std::cell::Cell<usize>,
    /// Memoized query-invariant metadata so per-step candidate resolution does
    /// not re-fan-out to every shard at each step.
    metric_index: std::cell::RefCell<std::collections::HashMap<Vec<u8>, Vec<Vec<u8>>>>,
    label_index: std::cell::RefCell<std::collections::HashMap<(Vec<u8>, Vec<u8>), Vec<Vec<u8>>>>,
    distinct: std::cell::RefCell<Option<Vec<Vec<u8>>>>,
}

/// Stop memoizing new series once this many samples are cached (~256 MB at
/// 16 bytes/sample). Series beyond the cap are served per-step from the inner
/// store — slower for those, but memory stays bounded.
const MAX_CACHED_SAMPLES: usize = 16_000_000;

impl<'a, S: QueryStore> RangeCache<'a, S> {
    fn new(inner: &'a S, lo: i64, hi: i64) -> Self {
        Self {
            inner,
            lo,
            hi,
            series: std::cell::RefCell::new(std::collections::HashMap::new()),
            cached_samples: std::cell::Cell::new(0),
            metric_index: std::cell::RefCell::new(std::collections::HashMap::new()),
            label_index: std::cell::RefCell::new(std::collections::HashMap::new()),
            distinct: std::cell::RefCell::new(None),
        }
    }
}

/// Extract the `[min, max]` sub-window from a timestamp-sorted slice. Binary
/// search bounds the work to O(log n + window) instead of scanning the whole
/// cached series on every step — the dominant cost for many-step / wide-range
/// queries (e.g. 720 steps over a 12 h window).
fn filter_range(samples: &[StoredSample], range: TimeRange) -> Vec<StoredSample> {
    let lo = samples.partition_point(|s| s.timestamp_ms < range.min_timestamp_ms);
    let hi = samples.partition_point(|s| s.timestamp_ms <= range.max_timestamp_ms);
    samples[lo..hi].to_vec()
}

impl<S: QueryStore> QueryStore for RangeCache<'_, S> {
    fn iter_metric_names(&self) -> Vec<(Vec<u8>, Tsid)> {
        self.inner.iter_metric_names()
    }

    fn search_by_metric_name(
        &self,
        metric_name: &[u8],
        range: TimeRange,
    ) -> Result<Vec<StoredSample>, StorageError> {
        // Cache hit: serve the requested sub-window from the memoized buffer.
        if let Some(all) = self.series.borrow().get(metric_name) {
            return Ok(filter_range(all, range));
        }
        // Miss and still under budget: read the whole window once and memoize.
        if self.cached_samples.get() < MAX_CACHED_SAMPLES {
            let full = self.inner.search_by_metric_name(
                metric_name,
                TimeRange { min_timestamp_ms: self.lo, max_timestamp_ms: self.hi },
            )?;
            self.cached_samples.set(self.cached_samples.get() + full.len());
            let out = filter_range(&full, range);
            self.series.borrow_mut().insert(metric_name.to_vec(), full);
            return Ok(out);
        }
        // Over budget: bypass the cache, fetch only the requested window.
        self.inner.search_by_metric_name(metric_name, range)
    }

    fn lookup_tsid(&self, metric_name: &[u8]) -> Option<Tsid> {
        self.inner.lookup_tsid(metric_name)
    }

    fn series_for_metric_name(&self, name_part: &[u8]) -> Vec<Vec<u8>> {
        if let Some(v) = self.metric_index.borrow().get(name_part) {
            return v.clone();
        }
        let v = self.inner.series_for_metric_name(name_part);
        self.metric_index.borrow_mut().insert(name_part.to_vec(), v.clone());
        v
    }

    fn series_for_label(&self, label_name: &[u8], label_value: &[u8]) -> Vec<Vec<u8>> {
        let key = (label_name.to_vec(), label_value.to_vec());
        if let Some(v) = self.label_index.borrow().get(&key) {
            return v.clone();
        }
        let v = self.inner.series_for_label(label_name, label_value);
        self.label_index.borrow_mut().insert(key, v.clone());
        v
    }

    fn distinct_metric_names(&self) -> Vec<Vec<u8>> {
        if let Some(v) = self.distinct.borrow().as_ref() {
            return v.clone();
        }
        let v = self.inner.distinct_metric_names();
        *self.distinct.borrow_mut() = Some(v.clone());
        v
    }
}

/// Map a `*_over_time` function name to its rollup kind, if supported.
fn over_time_kind(name: &str) -> Option<OverTimeKind> {
    Some(match name {
        "sum_over_time" => OverTimeKind::Sum,
        "avg_over_time" => OverTimeKind::Avg,
        "min_over_time" => OverTimeKind::Min,
        "max_over_time" => OverTimeKind::Max,
        "count_over_time" => OverTimeKind::Count,
        "stddev_over_time" => OverTimeKind::Stddev,
        "stdvar_over_time" => OverTimeKind::Stdvar,
        "last_over_time" => OverTimeKind::Last,
        "present_over_time" => OverTimeKind::Present,
        _ => return None,
    })
}

/// Candidate count at or above which `try_single_pass` switches from the
/// per-series parallel read to a per-part scan (opening each overlapping
/// part once); below it, selective queries seek straight to their few series.
///
/// Set from the `selective_scan_compare` microbench: the per-part scan is
/// faster (or, for candidates scattered one-per-shard, no worse) than the
/// per-series path from very low candidate counts up. A small threshold keeps
/// genuinely single-series lookups on the direct seek path while routing
/// multi-host selectors (single-groupby-*-8-1, cpu-max-all-8: 8–80 series)
/// through the scan, where per-series redundantly re-opens each shard's parts.
const WIDE_SCAN_THRESHOLD: usize = 8;

/// Single-pass fast path for `[agg(]rollup_over_time(selector[range])[)]`.
///
/// Resolves the candidate series once, reads each series' whole window once and
/// computes its per-step rollup in a single pass (binary-searched step
/// windows), all in parallel across series; then aggregates per step. Reuses
/// the shared [`reduce_over_time`]/[`reduce_simple_agg`]/[`group_key`] helpers
/// so the output is identical to [`evaluate_range_generic`] (verified by the
/// `fast_path_matches_generic` test). Returns `None` for any unsupported shape.
fn try_single_pass<S: QueryStore + Sync>(
    expr: &Expr,
    storage: &S,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Option<Result<Vec<RangeVectorElement>, EvalError>> {
    use rayon::prelude::*;

    // Match optional simple aggregation wrapping a `*_over_time` call.
    let (agg, fc): (Option<&crate::ast::AggregationExpr>, &crate::ast::FunctionCall) = match expr {
        Expr::Aggregation(a) => {
            if a.param.is_some() || reduce_simple_agg(a.op, &[]).is_none() {
                return None;
            }
            match a.arg.as_ref() {
                Expr::FunctionCall(fc) => (Some(a), fc),
                _ => return None,
            }
        }
        Expr::FunctionCall(fc) => (None, fc),
        _ => return None,
    };
    let kind = over_time_kind(&fc.name)?;
    if fc.args.len() != 1 {
        return None;
    }
    let sel = match &fc.args[0] {
        Expr::VectorSelector(s) => s,
        _ => return None,
    };
    let range_ms = sel.range_ms?;
    let mut bare = sel.clone();
    bare.range_ms = None;

    // Step timestamps (same enumeration as the generic loop).
    let mut steps = Vec::new();
    let mut t = start_ms;
    while t <= end_ms {
        steps.push(t);
        t = match t.checked_add(step_ms) {
            Some(n) => n,
            None => break,
        };
    }

    // Candidate series, resolved exactly once.
    let names: Vec<Vec<u8>> = candidate_series(storage, &bare)
        .into_iter()
        .filter(|n| matches_selector(n, &bare))
        .collect();
    let lo = start_ms.saturating_sub(range_ms);
    let window = TimeRange { min_timestamp_ms: lo, max_timestamp_ms: end_ms };

    // Roll one series' samples into a per-step value (`None` for an empty
    // window, matching the generic path), over the `StoredSample` slice — no
    // per-series `ts`/`vals` extraction.
    let roll = |samples: &[StoredSample]| -> Vec<Option<f64>> {
        steps
            .iter()
            .map(|&st| {
                // PromQL range `[d]` at `st` selects `(st-d, st]` — left-open,
                // matching VictoriaMetrics/Prometheus: a sample at exactly
                // `st-range_ms` is excluded.
                let wlo = samples.partition_point(|s| s.timestamp_ms <= st - range_ms);
                let whi = samples.partition_point(|s| s.timestamp_ms <= st);
                (wlo != whi).then(|| reduce_over_time_samples(kind, &samples[wlo..whi]))
            })
            .collect()
    };
    // Wide query (rollup over many series, e.g. double-groupby): a per-part scan
    // opens each part once instead of once per series. Selective queries keep
    // the per-series parallel read, which seeks straight to the few candidates.
    let per_series: Vec<(Vec<u8>, Vec<Option<f64>>)> = if names.len() >= WIDE_SCAN_THRESHOLD {
        let step_vals = match <S as QueryStore>::scan_series_map(storage, &names, window, roll) {
            Ok(v) => v,
            Err(e) => return Some(Err(EvalError::Storage(e.to_string()))),
        };
        names.into_iter().zip(step_vals).collect()
    } else {
        let r: Result<Vec<(Vec<u8>, Vec<Option<f64>>)>, EvalError> = names
            .into_par_iter()
            .map(|name| {
                let samples = storage
                    .search_by_metric_name(&name, window)
                    .map_err(|e| EvalError::Storage(e.to_string()))?;
                Ok((name, roll(&samples)))
            })
            .collect();
        match r {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        }
    };

    let result: Vec<RangeVectorElement> = match agg {
        None => {
            // No aggregation: one element per series; drop empty-window steps.
            let mut out: Vec<RangeVectorElement> = per_series
                .into_iter()
                .filter_map(|(name, sv)| {
                    let values: Vec<(i64, f64)> =
                        steps.iter().zip(sv).filter_map(|(&t, o)| o.map(|v| (t, v))).collect();
                    (!values.is_empty()).then_some(RangeVectorElement { metric_name: name, values })
                })
                .collect();
            // Match the generic path's BTreeMap-ordered output.
            out.sort_by(|a, b| a.metric_name.cmp(&b.metric_name));
            out
        }
        Some(a) => {
            // Group series by key once, then reduce each group across cores.
            // Members are visited in candidate (per_series) order, so the
            // floating-point reduction order matches the generic path exactly.
            let mut key_to_series: BTreeMap<Vec<u8>, Vec<usize>> = BTreeMap::new();
            for (i, (n, _)) in per_series.iter().enumerate() {
                key_to_series.entry(group_key(n, a.grouping.as_ref())).or_default().push(i);
            }
            let entries: Vec<(Vec<u8>, Vec<usize>)> = key_to_series.into_iter().collect();
            let mut out: Vec<RangeVectorElement> = entries
                .into_par_iter()
                .filter_map(|(key, members)| {
                    let mut values: Vec<(i64, f64)> = Vec::new();
                    for (si, &t) in steps.iter().enumerate() {
                        let mut vals: Vec<f64> = Vec::with_capacity(members.len());
                        for &m in &members {
                            if let Some(v) = per_series[m].1[si] {
                                vals.push(v);
                            }
                        }
                        if !vals.is_empty()
                            && let Some(av) = reduce_simple_agg(a.op, &vals)
                        {
                            values.push((t, av));
                        }
                    }
                    (!values.is_empty()).then_some(RangeVectorElement { metric_name: key, values })
                })
                .collect();
            out.sort_by(|x, y| x.metric_name.cmp(&y.metric_name));
            out
        }
    };
    Some(Ok(result))
}

pub fn evaluate_range(
    expr: &Expr,
    storage: &(impl QueryStore + Sync),
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Result<Vec<RangeVectorElement>, EvalError> {
    if step_ms <= 0 {
        return Err(EvalError::InvalidStep(step_ms));
    }
    if end_ms < start_ms {
        return Err(EvalError::InvalidRange { start: start_ms, end: end_ms });
    }
    // Fast path for `[agg(]rollup_over_time(selector[range])[ by/without ...])`:
    // resolve candidates once, read each series once, compute per-series step
    // rollups in parallel, then aggregate per step — reusing the exact shared
    // reduce helpers so results match the generic path. Falls back otherwise.
    if let Some(r) = try_single_pass(expr, storage, start_ms, end_ms, step_ms) {
        return r;
    }
    evaluate_range_generic(expr, storage, start_ms, end_ms, step_ms)
}

/// The generic per-step range evaluator (kept as the correctness reference and
/// fallback for shapes the fast path does not handle).
fn evaluate_range_generic(
    expr: &Expr,
    storage: &(impl QueryStore + Sync),
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Result<Vec<RangeVectorElement>, EvalError> {
    use rayon::prelude::*;

    // Read each touched series ONCE over the whole query window and serve every
    // step's sub-window from memory, instead of re-searching (and re-opening
    // parts for) the same series at every step. The preload lower bound covers
    // the largest `[range]` in the expression plus the instant-selector
    // lookback.
    let preload = max_selector_range(expr).max(EvalContext::DEFAULT_LOOKBACK_MS);
    let lo = start_ms.saturating_sub(preload);
    let cache = RangeCache::new(storage, lo, end_ms);

    // Warm the cache in parallel: resolve every selector's candidate series and
    // read their full windows concurrently (reads are pure; the inner store is
    // `Sync`). The serial evaluation below then runs entirely against the warm
    // cache — no rollup/aggregation math is duplicated, so results are
    // identical to the single-threaded path.
    let mut sels = Vec::new();
    collect_selectors(expr, &mut sels);
    let mut names: Vec<Vec<u8>> = Vec::new();
    for sel in sels {
        // Narrow to the *true* matches (not just the anchored superset) so a
        // selector like `{__name__=~..., host=~...}` warms only the series the
        // query needs, not every series of those metrics.
        names.extend(
            candidate_series(storage, sel).into_iter().filter(|n| matches_selector(n, sel)),
        );
    }
    names.sort_unstable();
    names.dedup();
    let window = TimeRange { min_timestamp_ms: lo, max_timestamp_ms: end_ms };
    let read: Vec<(Vec<u8>, Vec<StoredSample>)> = names
        .into_par_iter()
        .filter_map(|n| storage.search_by_metric_name(&n, window).ok().map(|s| (n, s)))
        .collect();
    {
        let mut series = cache.series.borrow_mut();
        let mut total = cache.cached_samples.get();
        for (n, s) in read {
            if total + s.len() > MAX_CACHED_SAMPLES {
                break;
            }
            total += s.len();
            series.insert(n, s);
        }
        cache.cached_samples.set(total);
    }

    let storage = &cache;
    let mut by_series: BTreeMap<Vec<u8>, Vec<(i64, f64)>> = BTreeMap::new();
    let mut t = start_ms;
    while t <= end_ms {
        let ctx = EvalContext::instant(t);
        match evaluate(expr, storage, ctx)? {
            Value::Scalar(n) => {
                by_series.entry(Vec::new()).or_default().push((t, n));
            }
            Value::InstantVector(elems) => {
                for e in elems {
                    by_series.entry(e.metric_name).or_default().push((t, e.value));
                }
            }
        }
        t = match t.checked_add(step_ms) {
            Some(next) => next,
            None => break,
        };
    }
    Ok(by_series
        .into_iter()
        .map(|(metric_name, values)| RangeVectorElement { metric_name, values })
        .collect())
}

/// Evaluate `expr` against `storage` in `ctx`.
///
/// # Errors
/// See [`EvalError`].
pub fn evaluate(
    expr: &Expr,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    match expr {
        Expr::NumberLiteral(n) => Ok(Value::Scalar(*n)),
        Expr::Paren(inner) => evaluate(inner, storage, ctx),
        Expr::Unary(op, inner) => {
            let v = evaluate(inner, storage, ctx)?;
            Ok(apply_unary(*op, v))
        }
        Expr::VectorSelector(sel) => {
            Ok(Value::InstantVector(evaluate_selector(sel, storage, ctx)?))
        }
        Expr::Binary(b) => evaluate_binary(b, storage, ctx),
        Expr::Aggregation(agg) => evaluate_aggregation(agg, storage, ctx),
        Expr::FunctionCall(fc) => evaluate_function(fc, storage, ctx),
        Expr::StringLiteral(_) => Err(EvalError::NotYetImplemented(
            "string literal in a non-function-call position".into(),
        )),
        Expr::Subquery(sq) => evaluate_subquery_as_instant(sq, storage, ctx),
    }
}

/// MVP subquery evaluation: walk the inner expression at every step over
/// the subquery range, then collapse to an instant vector whose value is
/// the most recent per-series sample. This is the minimum that lets
/// outer aggregators / functions consume the result without errors;
/// full range-vector behavior for rate / over_time on subqueries is a
/// follow-up.
fn evaluate_subquery_as_instant(
    sq: &crate::ast::SubqueryExpr,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    let step_ms = sq.step_ms.unwrap_or(60_000);
    if step_ms <= 0 || sq.range_ms <= 0 {
        return Err(EvalError::NotYetImplemented(
            "subquery with non-positive range or step".into(),
        ));
    }
    let mut by_series: BTreeMap<Vec<u8>, (i64, f64)> = BTreeMap::new();
    let start = ctx.timestamp_ms - sq.range_ms;
    let end = ctx.timestamp_ms;
    let mut t = start;
    while t <= end {
        let step_ctx = EvalContext::instant(t);
        match evaluate(&sq.inner, storage, step_ctx)? {
            Value::Scalar(n) => {
                by_series
                    .entry(Vec::new())
                    .and_modify(|e| {
                        if t > e.0 {
                            *e = (t, n);
                        }
                    })
                    .or_insert((t, n));
            }
            Value::InstantVector(elems) => {
                for e in elems {
                    by_series
                        .entry(e.metric_name.clone())
                        .and_modify(|x| {
                            if t > x.0 {
                                *x = (t, e.value);
                            }
                        })
                        .or_insert((t, e.value));
                }
            }
        }
        t = match t.checked_add(step_ms) {
            Some(next) => next,
            None => break,
        };
    }
    let out = by_series
        .into_iter()
        .map(|(name, (ts, v))| InstantVectorElement {
            metric_name: name,
            timestamp_ms: ts,
            value: v,
        })
        .collect();
    Ok(Value::InstantVector(out))
}

fn evaluate_aggregation(
    agg: &crate::ast::AggregationExpr,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    use crate::ast::{AggregationOp, GroupingKind};

    let arg = evaluate(&agg.arg, storage, ctx)?;
    let elems = match arg {
        Value::Scalar(n) => {
            // Aggregating a scalar yields a scalar in PromQL.
            return Ok(Value::Scalar(n));
        }
        Value::InstantVector(v) => v,
    };

    // Group elements by the retained labels.
    let mut groups: BTreeMap<Vec<u8>, Vec<InstantVectorElement>> = BTreeMap::new();
    for e in elems {
        let key = group_key(&e.metric_name, agg.grouping.as_ref());
        groups.entry(key).or_default().push(e);
    }

    let mut out: Vec<InstantVectorElement> = Vec::new();
    for (key, members) in groups {
        // Simple reductions go through the shared helper so the single-pass
        // fast path computes identically; complex ops stay inline below.
        if let Some(v) = {
            let vals: Vec<f64> = members.iter().map(|e| e.value).collect();
            reduce_simple_agg(agg.op, &vals)
        } {
            out.push(InstantVectorElement {
                metric_name: key,
                timestamp_ms: ctx.timestamp_ms,
                value: v,
            });
            continue;
        }
        let agg_value = match agg.op {
            AggregationOp::Sum => members.iter().map(|e| e.value).sum::<f64>(),
            AggregationOp::Avg => {
                let n = members.len() as f64;
                if n == 0.0 { f64::NAN } else { members.iter().map(|e| e.value).sum::<f64>() / n }
            }
            AggregationOp::Min => members.iter().map(|e| e.value).fold(f64::INFINITY, f64::min),
            AggregationOp::Max => members.iter().map(|e| e.value).fold(f64::NEG_INFINITY, f64::max),
            AggregationOp::Count => members.len() as f64,
            AggregationOp::Stddev => stddev(&members),
            AggregationOp::Stdvar => {
                let s = stddev(&members);
                s * s
            }
            AggregationOp::Group => 1.0,
            AggregationOp::CountValues => {
                // `count_values("label", expr)` groups elements by the *value*
                // of `expr` (rounded to a string) and emits one output series
                // per distinct value with the count, attaching a synthetic
                // label named by the leading string parameter.
                let label = match agg.param.as_deref() {
                    Some(Expr::StringLiteral(s)) => s.clone(),
                    _ => {
                        return Err(EvalError::NotYetImplemented(
                            "count_values requires a string-literal leading argument".into(),
                        ));
                    }
                };
                let mut buckets: std::collections::BTreeMap<String, u64> =
                    std::collections::BTreeMap::new();
                for e in &members {
                    let key = format_promql_value(e.value);
                    *buckets.entry(key).or_default() += 1;
                }
                for (val, count) in buckets {
                    let mut labels = parse_label_map(&key);
                    labels.remove("__name__");
                    labels.insert(label.clone(), val);
                    out.push(InstantVectorElement {
                        metric_name: build_metric_name(&labels),
                        timestamp_ms: ctx.timestamp_ms,
                        value: count as f64,
                    });
                }
                continue;
            }
            AggregationOp::Topk | AggregationOp::Bottomk => {
                let n_val = match agg.param.as_deref() {
                    Some(p) => evaluate(p, storage, ctx)?,
                    None => {
                        return Err(EvalError::NotYetImplemented(
                            "topk/bottomk without N parameter".into(),
                        ));
                    }
                };
                let n_f = match n_val {
                    Value::Scalar(v) => v,
                    Value::InstantVector(_) => {
                        return Err(EvalError::FunctionArgKind {
                            name: format!("{:?}", agg.op),
                            want: "scalar",
                        });
                    }
                };
                let take_n = if n_f.is_finite() && n_f > 0.0 { n_f as usize } else { 0 };
                let mut ranked = members.clone();
                ranked.sort_by(|a, b| {
                    let ord = a.value.partial_cmp(&b.value).unwrap_or(std::cmp::Ordering::Equal);
                    if matches!(agg.op, AggregationOp::Topk) { ord.reverse() } else { ord }
                });
                ranked.truncate(take_n);
                for e in ranked {
                    out.push(InstantVectorElement {
                        metric_name: e.metric_name,
                        timestamp_ms: ctx.timestamp_ms,
                        value: e.value,
                    });
                }
                continue;
            }
            AggregationOp::Quantile => {
                let p_val = match agg.param.as_deref() {
                    Some(p) => evaluate(p, storage, ctx)?,
                    None => {
                        return Err(EvalError::NotYetImplemented(
                            "quantile without parameter".into(),
                        ));
                    }
                };
                let p = match p_val {
                    Value::Scalar(v) => v,
                    Value::InstantVector(_) => {
                        return Err(EvalError::FunctionArgKind {
                            name: "quantile".into(),
                            want: "scalar",
                        });
                    }
                };
                let mut values: Vec<f64> = members.iter().map(|e| e.value).collect();
                values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                if values.is_empty() {
                    f64::NAN
                } else if !p.is_finite() || p < 0.0 {
                    f64::NEG_INFINITY
                } else if p > 1.0 {
                    f64::INFINITY
                } else {
                    let n = values.len();
                    let rank = p * (n as f64 - 1.0);
                    let lo = rank.floor() as usize;
                    let hi = rank.ceil() as usize;
                    let frac = rank - rank.floor();
                    values[lo] + frac * (values[hi] - values[lo])
                }
            }
        };
        // Timestamp: use eval timestamp.
        out.push(InstantVectorElement {
            metric_name: key,
            timestamp_ms: ctx.timestamp_ms,
            value: agg_value,
        });
    }
    // If `by` is empty and there were elements, PromQL collapses to a single
    // unlabelled element; our group_key naturally produces an empty key in
    // that case so groups already collapse correctly.
    let _ = GroupingKind::By;
    Ok(Value::InstantVector(out))
}

/// Reduce a group's values for the simple aggregation ops. Returns `None` for
/// ops that need element identity or parameters (`count_values`, `topk`,
/// `bottomk`, `quantile`) — those stay on the generic path. Shared so the
/// single-pass fast path aggregates identically.
fn reduce_simple_agg(op: crate::ast::AggregationOp, values: &[f64]) -> Option<f64> {
    use crate::ast::AggregationOp as A;
    Some(match op {
        A::Sum => values.iter().sum::<f64>(),
        A::Avg => {
            let n = values.len() as f64;
            if n == 0.0 { f64::NAN } else { values.iter().sum::<f64>() / n }
        }
        A::Min => values.iter().copied().fold(f64::INFINITY, f64::min),
        A::Max => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        A::Count => values.len() as f64,
        A::Stddev => stddev_of_floats(values),
        A::Stdvar => {
            let s = stddev_of_floats(values);
            s * s
        }
        A::Group => 1.0,
        _ => return None,
    })
}

fn stddev(members: &[InstantVectorElement]) -> f64 {
    let n = members.len() as f64;
    if n == 0.0 {
        return f64::NAN;
    }
    let mean = members.iter().map(|e| e.value).sum::<f64>() / n;
    let var = members.iter().map(|e| (e.value - mean).powi(2)).sum::<f64>() / n;
    var.sqrt()
}

/// Build the grouping key for an instant-vector element. The key is the
/// canonical metric-name byte-string with labels filtered per the
/// grouping clause.
fn group_key(metric_name: &[u8], grouping: Option<&crate::ast::GroupingClause>) -> Vec<u8> {
    use crate::ast::GroupingKind;

    // Parse `name{labels}` into name + labels.
    let s = match std::str::from_utf8(metric_name) {
        Ok(s) => s,
        Err(_) => return metric_name.to_vec(),
    };
    let (name, labels_str) = match s.find('{') {
        Some(i) => (&s[..i], s[i..].trim_start_matches('{').trim_end_matches('}')),
        None => (s, ""),
    };
    let mut labels: BTreeMap<&str, &str> = BTreeMap::new();
    // Expose the metric name as the `__name__` label so it can participate in
    // grouping. `by (__name__)` retains it (VM keeps it in the output; TSBS
    // relies on this); `without (...)` always drops it, matching VM/Prometheus.
    labels.insert("__name__", name);
    if !labels_str.is_empty() {
        for part in labels_str.split(',') {
            let Some(eq) = part.find('=') else { continue };
            let k = &part[..eq];
            let v = part[eq + 1..].trim_start_matches('"').trim_end_matches('"');
            labels.insert(k, v);
        }
    }
    // Apply grouping.
    let filtered: BTreeMap<&str, &str> = match grouping {
        None => BTreeMap::new(), // no `by`/`without` => single bucket
        Some(g) => match g.kind {
            GroupingKind::By => labels
                .iter()
                .filter(|(k, _)| g.labels.iter().any(|l| l == *k))
                .map(|(k, v)| (*k, *v))
                .collect(),
            GroupingKind::Without => labels
                .iter()
                .filter(|(k, _)| **k != "__name__" && !g.labels.iter().any(|l| l == *k))
                .map(|(k, v)| (*k, *v))
                .collect(),
        },
    };
    if filtered.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    out.push(b'{');
    for (i, (k, v)) in filtered.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(k.as_bytes());
        out.extend_from_slice(b"=\"");
        out.extend_from_slice(v.as_bytes());
        out.push(b'"');
    }
    out.push(b'}');
    out
}

fn evaluate_function(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    match fc.name.as_str() {
        "time" =>
        {
            #[allow(clippy::cast_precision_loss)]
            Ok(Value::Scalar((ctx.timestamp_ms as f64) / 1000.0))
        }
        "scalar" => {
            // `scalar(v)` returns the scalar value if `v` is a single-element
            // vector, else NaN.
            if fc.args.len() != 1 {
                return Err(EvalError::FunctionArity {
                    name: fc.name.clone(),
                    want: 1,
                    got: fc.args.len(),
                });
            }
            match evaluate(&fc.args[0], storage, ctx)? {
                Value::Scalar(n) => Ok(Value::Scalar(n)),
                Value::InstantVector(elems) => {
                    if elems.len() == 1 {
                        Ok(Value::Scalar(elems[0].value))
                    } else {
                        Ok(Value::Scalar(f64::NAN))
                    }
                }
            }
        }
        "vector" => {
            // `vector(s)` lifts a scalar to a single-element vector.
            if fc.args.len() != 1 {
                return Err(EvalError::FunctionArity {
                    name: fc.name.clone(),
                    want: 1,
                    got: fc.args.len(),
                });
            }
            match evaluate(&fc.args[0], storage, ctx)? {
                Value::Scalar(n) => Ok(Value::InstantVector(vec![InstantVectorElement {
                    metric_name: Vec::new(),
                    timestamp_ms: ctx.timestamp_ms,
                    value: n,
                }])),
                Value::InstantVector(_) => {
                    Err(EvalError::FunctionArgKind { name: fc.name.clone(), want: "scalar" })
                }
            }
        }
        "abs" => apply_to_vector_or_scalar(fc, storage, ctx, |x| x.abs()),
        "ceil" => apply_to_vector_or_scalar(fc, storage, ctx, f64::ceil),
        "floor" => apply_to_vector_or_scalar(fc, storage, ctx, f64::floor),
        "round" => apply_to_vector_or_scalar(fc, storage, ctx, f64::round),
        "sqrt" => apply_to_vector_or_scalar(fc, storage, ctx, f64::sqrt),
        "exp" => apply_to_vector_or_scalar(fc, storage, ctx, f64::exp),
        "ln" => apply_to_vector_or_scalar(fc, storage, ctx, f64::ln),
        "log2" => apply_to_vector_or_scalar(fc, storage, ctx, f64::log2),
        "log10" => apply_to_vector_or_scalar(fc, storage, ctx, f64::log10),
        "predict_linear" => evaluate_predict_linear(fc, storage, ctx),
        "deriv" => evaluate_deriv(fc, storage, ctx),
        "holt_winters" => evaluate_holt_winters(fc, storage, ctx),
        "rate" => evaluate_rate_like(fc, storage, ctx, RateKind::Rate),
        "increase" => evaluate_rate_like(fc, storage, ctx, RateKind::Increase),
        "irate" => evaluate_rate_like(fc, storage, ctx, RateKind::Irate),
        "delta" => evaluate_rate_like(fc, storage, ctx, RateKind::Delta),
        "clamp" => evaluate_clamp(fc, storage, ctx, ClampMode::Both),
        "clamp_min" => evaluate_clamp(fc, storage, ctx, ClampMode::Min),
        "clamp_max" => evaluate_clamp(fc, storage, ctx, ClampMode::Max),
        "timestamp" => evaluate_timestamp_fn(fc, storage, ctx),
        "sort" => evaluate_sort(fc, storage, ctx, /*desc=*/ false),
        "sort_desc" => evaluate_sort(fc, storage, ctx, /*desc=*/ true),
        "histogram_quantile" => evaluate_histogram_quantile(fc, storage, ctx),
        "histogram_count" => evaluate_histogram_sibling(fc, storage, ctx, "_count"),
        "histogram_sum" => evaluate_histogram_sibling(fc, storage, ctx, "_sum"),
        "histogram_avg" => evaluate_histogram_avg(fc, storage, ctx),
        "histogram_fraction" => evaluate_histogram_fraction(fc, storage, ctx),
        "histogram_stddev" => evaluate_histogram_stddev(fc, storage, ctx, /*variance=*/ false),
        "histogram_stdvar" => evaluate_histogram_stddev(fc, storage, ctx, /*variance=*/ true),
        "year" => apply_time_field(fc, storage, ctx, TimeField::Year),
        "month" => apply_time_field(fc, storage, ctx, TimeField::Month),
        "day_of_month" => apply_time_field(fc, storage, ctx, TimeField::DayOfMonth),
        "day_of_week" => apply_time_field(fc, storage, ctx, TimeField::DayOfWeek),
        "day_of_year" => apply_time_field(fc, storage, ctx, TimeField::DayOfYear),
        "days_in_month" => apply_time_field(fc, storage, ctx, TimeField::DaysInMonth),
        "hour" => apply_time_field(fc, storage, ctx, TimeField::Hour),
        "minute" => apply_time_field(fc, storage, ctx, TimeField::Minute),
        "label_replace" => evaluate_label_replace(fc, storage, ctx),
        "label_join" => evaluate_label_join(fc, storage, ctx),
        "changes" => evaluate_changes(fc, storage, ctx),
        "resets" => evaluate_resets(fc, storage, ctx),
        "absent" => evaluate_absent(fc, storage, ctx),
        "absent_over_time" => evaluate_absent_over_time(fc, storage, ctx),
        "sum_over_time" => evaluate_over_time(fc, storage, ctx, OverTimeKind::Sum),
        "avg_over_time" => evaluate_over_time(fc, storage, ctx, OverTimeKind::Avg),
        "min_over_time" => evaluate_over_time(fc, storage, ctx, OverTimeKind::Min),
        "max_over_time" => evaluate_over_time(fc, storage, ctx, OverTimeKind::Max),
        "count_over_time" => evaluate_over_time(fc, storage, ctx, OverTimeKind::Count),
        "stddev_over_time" => evaluate_over_time(fc, storage, ctx, OverTimeKind::Stddev),
        "stdvar_over_time" => evaluate_over_time(fc, storage, ctx, OverTimeKind::Stdvar),
        "last_over_time" => evaluate_over_time(fc, storage, ctx, OverTimeKind::Last),
        "present_over_time" => evaluate_over_time(fc, storage, ctx, OverTimeKind::Present),
        other => Err(EvalError::UnknownFunction(other.to_string())),
    }
}

#[derive(Debug, Clone, Copy)]
enum ClampMode {
    /// `clamp(v, lo, hi)` — clamp to `[lo, hi]`.
    Both,
    /// `clamp_min(v, lo)` — replace anything below `lo` with `lo`.
    Min,
    /// `clamp_max(v, hi)` — replace anything above `hi` with `hi`.
    Max,
}

fn evaluate_clamp(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
    mode: ClampMode,
) -> Result<Value, EvalError> {
    let expected = match mode {
        ClampMode::Both => 3,
        ClampMode::Min | ClampMode::Max => 2,
    };
    if fc.args.len() != expected {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: expected,
            got: fc.args.len(),
        });
    }
    let input = evaluate(&fc.args[0], storage, ctx)?;
    let lo = match mode {
        ClampMode::Both | ClampMode::Min => {
            Some(expect_scalar(&fc.name, evaluate(&fc.args[1], storage, ctx)?)?)
        }
        ClampMode::Max => None,
    };
    let hi = match mode {
        ClampMode::Both => Some(expect_scalar(&fc.name, evaluate(&fc.args[2], storage, ctx)?)?),
        ClampMode::Max => Some(expect_scalar(&fc.name, evaluate(&fc.args[1], storage, ctx)?)?),
        ClampMode::Min => None,
    };

    let apply = |v: f64| -> f64 {
        let v = if let Some(l) = lo { v.max(l) } else { v };
        if let Some(h) = hi { v.min(h) } else { v }
    };
    match input {
        Value::Scalar(n) => Ok(Value::Scalar(apply(n))),
        Value::InstantVector(mut elems) => {
            for e in &mut elems {
                e.value = apply(e.value);
            }
            Ok(Value::InstantVector(elems))
        }
    }
}

fn expect_scalar(name: &str, v: Value) -> Result<f64, EvalError> {
    match v {
        Value::Scalar(n) => Ok(n),
        Value::InstantVector(_) => {
            Err(EvalError::FunctionArgKind { name: name.to_string(), want: "scalar" })
        }
    }
}

fn evaluate_timestamp_fn(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    if fc.args.len() != 1 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 1,
            got: fc.args.len(),
        });
    }
    let v = evaluate(&fc.args[0], storage, ctx)?;
    match v {
        Value::Scalar(_) => Ok(Value::Scalar((ctx.timestamp_ms as f64) / 1000.0)),
        Value::InstantVector(elems) => {
            let mut out = Vec::with_capacity(elems.len());
            for e in elems {
                out.push(InstantVectorElement {
                    metric_name: strip_name(&e.metric_name),
                    timestamp_ms: e.timestamp_ms,
                    value: (e.timestamp_ms as f64) / 1000.0,
                });
            }
            Ok(Value::InstantVector(out))
        }
    }
}

/// `histogram_quantile(phi, vector)` — group input by every label except
/// `le`, sort the `le` buckets numerically, and compute the quantile via
/// linear interpolation. Matches Prometheus's standard implementation.
#[derive(Debug, Clone, Copy)]
enum TimeField {
    Year,
    Month,
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    DaysInMonth,
    Hour,
    Minute,
}

fn apply_time_field(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
    field: TimeField,
) -> Result<Value, EvalError> {
    // Optional argument: when omitted, use eval time as the timestamp source.
    let v = if fc.args.is_empty() {
        Value::InstantVector(vec![InstantVectorElement {
            metric_name: Vec::new(),
            timestamp_ms: ctx.timestamp_ms,
            value: (ctx.timestamp_ms as f64) / 1000.0,
        }])
    } else if fc.args.len() == 1 {
        evaluate(&fc.args[0], storage, ctx)?
    } else {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 1,
            got: fc.args.len(),
        });
    };

    let apply = |sec: f64| -> f64 {
        let secs = sec as i64;
        let (year, month_1based, day_1based, day_of_year_1based, days_in_month, hour, minute, dow) =
            civil_from_unix_secs(secs);
        match field {
            TimeField::Year => year as f64,
            TimeField::Month => month_1based as f64,
            TimeField::DayOfMonth => day_1based as f64,
            TimeField::DayOfWeek => dow as f64,
            TimeField::DayOfYear => day_of_year_1based as f64,
            TimeField::DaysInMonth => days_in_month as f64,
            TimeField::Hour => hour as f64,
            TimeField::Minute => minute as f64,
        }
    };

    match v {
        Value::Scalar(n) => Ok(Value::Scalar(apply(n))),
        Value::InstantVector(mut elems) => {
            for e in &mut elems {
                e.value = apply(e.value);
            }
            Ok(Value::InstantVector(elems))
        }
    }
}

/// Convert unix epoch seconds to civil UTC `(year, month, day, day_of_year,
/// days_in_month, hour, minute, day_of_week)` where `day_of_week` is 0..=6
/// with Sunday=0 (Prometheus convention).
fn civil_from_unix_secs(secs: i64) -> (i64, u8, u8, u16, u8, u8, u8, u8) {
    // Howard Hinnant's date algorithm (civil_from_days).
    let mut secs_of_day = secs.rem_euclid(86_400);
    let days_since_epoch = secs.div_euclid(86_400);
    let hour = (secs_of_day / 3600) as u8;
    secs_of_day %= 3600;
    let minute = (secs_of_day / 60) as u8;

    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z / 146_097 } else { (z - 146_096) / 146_097 };
    let doe = (z - era * 146_097) as i64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y_civil = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u8; // [1, 31]
    let m = (mp + if mp < 10 { 3 } else { -9 }) as u8; // [1, 12]
    let year = y_civil + i64::from(m <= 2);

    // Day of year (1-based).
    let day_of_year = day_of_year_for(year, m, d);
    let days_in_month = days_in_month_for(year, m);
    let dow = ((days_since_epoch + 4).rem_euclid(7)) as u8; // unix epoch = Thursday, +4 → Sunday=0

    (year, m, d, day_of_year, days_in_month, hour, minute, dow)
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month_for(year: i64, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(year) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

fn day_of_year_for(year: i64, month: u8, day: u8) -> u16 {
    let cum_normal = [0u16, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let cum_leap = [0u16, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335];
    let table = if is_leap(year) { &cum_leap } else { &cum_normal };
    table[(month - 1) as usize] + u16::from(day)
}

fn evaluate_label_replace(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    if fc.args.len() != 5 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 5,
            got: fc.args.len(),
        });
    }
    let dst = expect_string_literal(&fc.args[1], "label_replace dst label")?;
    let replacement = expect_string_literal(&fc.args[2], "label_replace replacement")?;
    let src = expect_string_literal(&fc.args[3], "label_replace src label")?;
    let pattern = expect_string_literal(&fc.args[4], "label_replace regex")?;
    let v = evaluate(&fc.args[0], storage, ctx)?;
    let Value::InstantVector(mut elems) = v else {
        return Err(EvalError::FunctionArgKind { name: fc.name.clone(), want: "vector" });
    };
    for e in &mut elems {
        let mut labels = parse_label_map(&e.metric_name);
        let actual = labels.get(&src).cloned().unwrap_or_default();
        // Anchored full match like Prometheus.
        if simple_full_regex_match(&actual, &pattern) {
            // PromQL supports $1, $2 backreferences; the minimal regex
            // implementation lacks capture groups. We approximate: when
            // the replacement contains no `$`, just use it; otherwise the
            // first capture group equals the full match (close enough for
            // common queries like `label_replace(v, "new", "$1", "old", "(.+)")`).
            let new_val = replacement.replace("$1", &actual);
            if new_val.is_empty() {
                labels.remove(&dst);
            } else {
                labels.insert(dst.clone(), new_val);
            }
            e.metric_name = build_metric_name(&labels);
        }
    }
    Ok(Value::InstantVector(elems))
}

fn evaluate_label_join(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    if fc.args.len() < 3 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 3,
            got: fc.args.len(),
        });
    }
    let dst = expect_string_literal(&fc.args[1], "label_join dst label")?;
    let separator = expect_string_literal(&fc.args[2], "label_join separator")?;
    let src_labels: Result<Vec<String>, EvalError> = fc.args[3..]
        .iter()
        .enumerate()
        .map(|(i, a)| expect_string_literal(a, &format!("label_join src #{}", i + 1)))
        .collect();
    let src_labels = src_labels?;
    let v = evaluate(&fc.args[0], storage, ctx)?;
    let Value::InstantVector(mut elems) = v else {
        return Err(EvalError::FunctionArgKind { name: fc.name.clone(), want: "vector" });
    };
    for e in &mut elems {
        let mut labels = parse_label_map(&e.metric_name);
        let joined = src_labels
            .iter()
            .map(|l| labels.get(l).cloned().unwrap_or_default())
            .collect::<Vec<_>>()
            .join(&separator);
        if joined.is_empty() {
            labels.remove(&dst);
        } else {
            labels.insert(dst.clone(), joined);
        }
        e.metric_name = build_metric_name(&labels);
    }
    Ok(Value::InstantVector(elems))
}

fn expect_string_literal(e: &Expr, what: &str) -> Result<String, EvalError> {
    match e {
        Expr::StringLiteral(s) => Ok(s.clone()),
        _ => Err(EvalError::NotYetImplemented(format!("non-literal {what} argument"))),
    }
}

fn simple_full_regex_match(s: &str, pattern: &str) -> bool {
    regex_full_match(s, pattern)
}

fn evaluate_changes(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    evaluate_over_time_count_like(fc, storage, ctx, |samples| {
        let mut changes = 0.0;
        for w in samples.windows(2) {
            if w[0].value != w[1].value {
                changes += 1.0;
            }
        }
        changes
    })
}

fn evaluate_resets(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    evaluate_over_time_count_like(fc, storage, ctx, |samples| {
        let mut resets = 0.0;
        for w in samples.windows(2) {
            if w[1].value < w[0].value {
                resets += 1.0;
            }
        }
        resets
    })
}

fn evaluate_over_time_count_like<F>(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
    accumulate: F,
) -> Result<Value, EvalError>
where
    F: Fn(&[esm_storage::StoredSample]) -> f64,
{
    if fc.args.len() != 1 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 1,
            got: fc.args.len(),
        });
    }
    let inner = match &fc.args[0] {
        Expr::VectorSelector(sel) => sel.clone(),
        _ => {
            return Err(EvalError::FunctionArgKind { name: fc.name.clone(), want: "range vector" });
        }
    };
    let Some(range_ms) = inner.range_ms else {
        return Err(EvalError::FunctionArgKind {
            name: fc.name.clone(),
            want: "range vector (metric[duration])",
        });
    };

    let mut bare = inner.clone();
    bare.range_ms = None;
    // Narrow to series matching the selector's metric-name constraint via the
    // name index, then apply the full matcher set. This is the hot path for
    // every `*_over_time` query, so avoiding the all-series scan matters.
    let candidate_names: Vec<Vec<u8>> = candidate_series(storage, &bare)
        .into_iter()
        .filter(|n| matches_selector(n, &bare))
        .collect();

    let window_start = ctx.timestamp_ms - range_ms + 1;
    let window_end = ctx.timestamp_ms;
    let mut out = Vec::new();
    for name in candidate_names {
        let mut samples = storage
            .search_by_metric_name(
                &name,
                TimeRange { min_timestamp_ms: window_start, max_timestamp_ms: window_end },
            )
            .map_err(|e| EvalError::Storage(e.to_string()))?;
        if samples.is_empty() {
            continue;
        }
        samples.sort_by_key(|s| s.timestamp_ms);
        let value = accumulate(&samples);
        out.push(InstantVectorElement { metric_name: name, timestamp_ms: ctx.timestamp_ms, value });
    }
    Ok(Value::InstantVector(out))
}

fn evaluate_absent(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    if fc.args.len() != 1 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 1,
            got: fc.args.len(),
        });
    }
    match evaluate(&fc.args[0], storage, ctx)? {
        Value::InstantVector(v) if v.is_empty() => {
            Ok(Value::InstantVector(vec![InstantVectorElement {
                metric_name: Vec::new(),
                timestamp_ms: ctx.timestamp_ms,
                value: 1.0,
            }]))
        }
        _ => Ok(Value::InstantVector(Vec::new())),
    }
}

fn evaluate_absent_over_time(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    if fc.args.len() != 1 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 1,
            got: fc.args.len(),
        });
    }
    let inner = match &fc.args[0] {
        Expr::VectorSelector(sel) => sel.clone(),
        _ => {
            return Err(EvalError::FunctionArgKind { name: fc.name.clone(), want: "range vector" });
        }
    };
    let range_ms = inner.range_ms.unwrap_or(EvalContext::DEFAULT_LOOKBACK_MS);
    let mut bare = inner.clone();
    bare.range_ms = None;
    let mut any_match = false;
    for (n, _) in storage.iter_metric_names() {
        if matches_selector(&n, &bare) {
            let samples = storage
                .search_by_metric_name(
                    &n,
                    TimeRange {
                        min_timestamp_ms: ctx.timestamp_ms - range_ms + 1,
                        max_timestamp_ms: ctx.timestamp_ms,
                    },
                )
                .map_err(|e| EvalError::Storage(e.to_string()))?;
            if !samples.is_empty() {
                any_match = true;
                break;
            }
        }
    }
    if any_match {
        Ok(Value::InstantVector(Vec::new()))
    } else {
        Ok(Value::InstantVector(vec![InstantVectorElement {
            metric_name: Vec::new(),
            timestamp_ms: ctx.timestamp_ms,
            value: 1.0,
        }]))
    }
}

fn format_promql_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 { "+Inf".to_string() } else { "-Inf".to_string() }
    } else {
        format!("{v}")
    }
}

fn evaluate_histogram_quantile(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    if fc.args.len() != 2 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 2,
            got: fc.args.len(),
        });
    }
    let phi = expect_scalar(&fc.name, evaluate(&fc.args[0], storage, ctx)?)?;
    let input = evaluate(&fc.args[1], storage, ctx)?;
    let elems = match input {
        Value::InstantVector(v) => v,
        Value::Scalar(_) => {
            return Err(EvalError::FunctionArgKind { name: fc.name.clone(), want: "vector" });
        }
    };

    // Group buckets by every label *except* `__name__` and `le`.
    let mut groups: std::collections::BTreeMap<Vec<u8>, Vec<(f64, f64)>> =
        std::collections::BTreeMap::new();
    for e in elems {
        let mut labels = parse_label_map(&e.metric_name);
        labels.remove("__name__");
        let le_str = labels.remove("le").unwrap_or_default();
        let le: f64 = match le_str.as_str() {
            "+Inf" | "Inf" => f64::INFINITY,
            "-Inf" => f64::NEG_INFINITY,
            other => other.parse().unwrap_or(f64::NAN),
        };
        let key = build_metric_name(&labels);
        groups.entry(key).or_default().push((le, e.value));
    }

    let mut out = Vec::with_capacity(groups.len());
    for (key, mut buckets) in groups {
        // Sort by upper bound; drop NaNs.
        buckets.retain(|(le, _)| !le.is_nan());
        buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        if buckets.is_empty() {
            continue;
        }
        let total = buckets.last().copied().map(|(_, c)| c).unwrap_or(0.0);
        let value = if total <= 0.0 || !phi.is_finite() {
            f64::NAN
        } else if phi < 0.0 {
            f64::NEG_INFINITY
        } else if phi > 1.0 {
            f64::INFINITY
        } else {
            let rank = phi * total;
            let mut prev_count = 0.0;
            let mut prev_le = 0.0;
            let mut quantile = f64::NAN;
            for (le, count) in &buckets {
                if *count >= rank {
                    if le.is_infinite() {
                        // Quantile lands in the +Inf bucket — use the
                        // previous finite boundary.
                        quantile = prev_le;
                    } else if prev_count == 0.0 {
                        quantile = le * (rank / count);
                    } else {
                        let lo = prev_le;
                        let hi = *le;
                        let frac = (rank - prev_count) / (count - prev_count);
                        quantile = lo + frac * (hi - lo);
                    }
                    break;
                }
                prev_count = *count;
                prev_le = *le;
            }
            quantile
        };
        out.push(InstantVectorElement { metric_name: key, timestamp_ms: ctx.timestamp_ms, value });
    }
    Ok(Value::InstantVector(out))
}

/// Classical-histogram interpretation of `histogram_count` and
/// `histogram_sum`: for each input series (which is presumed to be a
/// `_bucket{le="..."}` series), find the matching sibling series named
/// `<base><suffix>` (where `<base>` is the metric name with the
/// `_bucket` stripped) and return that series's most recent sample.
/// The classical histogram convention exposes `_count` and `_sum` as
/// separate sibling series; this function looks them up directly.
fn evaluate_histogram_sibling(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
    suffix: &str,
) -> Result<Value, EvalError> {
    if fc.args.len() != 1 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 1,
            got: fc.args.len(),
        });
    }
    let input = evaluate(&fc.args[0], storage, ctx)?;
    let elems = match input {
        Value::InstantVector(v) => v,
        Value::Scalar(_) => {
            return Err(EvalError::FunctionArgKind { name: fc.name.clone(), want: "vector" });
        }
    };

    // De-dup keys so we don't query the same sibling twice when the
    // input contains every bucket of the same histogram.
    let mut seen: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    for e in elems {
        let labels_all = parse_label_map(&e.metric_name);
        let mut labels = labels_all.clone();
        let name = labels.remove("__name__").unwrap_or_default();
        labels.remove("le");
        // Map `metric_bucket` (and `metric`) to `metric<suffix>`.
        let base = name.strip_suffix("_bucket").unwrap_or(&name);
        let sibling_name = format!("{base}{suffix}");
        labels.insert("__name__".to_string(), sibling_name.clone());
        let key = build_metric_name(&labels);
        if !seen.insert(key.clone()) {
            continue;
        }
        let range = esm_storage::TimeRange {
            min_timestamp_ms: ctx.timestamp_ms - ctx.lookback_ms,
            max_timestamp_ms: ctx.timestamp_ms,
        };
        let hits = storage
            .search_by_metric_name(&key, range)
            .map_err(|e| EvalError::Storage(e.to_string()))?;
        if let Some(latest) = hits.last() {
            out.push(InstantVectorElement {
                metric_name: key,
                timestamp_ms: latest.timestamp_ms,
                value: latest.value as f64,
            });
        }
    }
    Ok(Value::InstantVector(out))
}

/// `histogram_avg(v) = histogram_sum(v) / histogram_count(v)`.
fn evaluate_histogram_avg(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    let sums = match evaluate_histogram_sibling(fc, storage, ctx, "_sum")? {
        Value::InstantVector(v) => v,
        Value::Scalar(_) => return Ok(Value::InstantVector(Vec::new())),
    };
    let counts = match evaluate_histogram_sibling(fc, storage, ctx, "_count")? {
        Value::InstantVector(v) => v,
        Value::Scalar(_) => return Ok(Value::InstantVector(Vec::new())),
    };
    let count_by_key: std::collections::BTreeMap<Vec<u8>, f64> = counts
        .into_iter()
        .map(|e| (sibling_label_key(&e.metric_name, "_count"), e.value))
        .collect();
    let mut out = Vec::new();
    for s in sums {
        let key = sibling_label_key(&s.metric_name, "_sum");
        if let Some(c) = count_by_key.get(&key)
            && c.abs() > f64::EPSILON
        {
            out.push(InstantVectorElement {
                metric_name: s.metric_name,
                timestamp_ms: s.timestamp_ms,
                value: s.value / c,
            });
        }
    }
    Ok(Value::InstantVector(out))
}

/// Strip a `<suffix>` from the `__name__` label so two sibling series
/// (one `<base>_sum` and one `<base>_count`) compare equal.
fn sibling_label_key(metric_name: &[u8], suffix: &str) -> Vec<u8> {
    let mut labels = parse_label_map(metric_name);
    if let Some(name) = labels.get("__name__").cloned() {
        let trimmed = name.strip_suffix(suffix).unwrap_or(&name).to_string();
        labels.insert("__name__".to_string(), trimmed);
    }
    build_metric_name(&labels)
}

/// `histogram_fraction(lower, upper, v)` — fraction of observations in
/// `[lower, upper]` of a classical-histogram bucket series. Linear
/// interpolation between adjacent buckets, capped by `+Inf`.
#[allow(clippy::cast_precision_loss)]
fn evaluate_histogram_fraction(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    if fc.args.len() != 3 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 3,
            got: fc.args.len(),
        });
    }
    let lower = expect_scalar(&fc.name, evaluate(&fc.args[0], storage, ctx)?)?;
    let upper = expect_scalar(&fc.name, evaluate(&fc.args[1], storage, ctx)?)?;
    let input = evaluate(&fc.args[2], storage, ctx)?;
    let elems = match input {
        Value::InstantVector(v) => v,
        Value::Scalar(_) => {
            return Err(EvalError::FunctionArgKind { name: fc.name.clone(), want: "vector" });
        }
    };
    let mut groups: std::collections::BTreeMap<Vec<u8>, Vec<(f64, f64)>> =
        std::collections::BTreeMap::new();
    for e in elems {
        let mut labels = parse_label_map(&e.metric_name);
        labels.remove("__name__");
        let le_str = labels.remove("le").unwrap_or_default();
        let le: f64 = match le_str.as_str() {
            "+Inf" | "Inf" => f64::INFINITY,
            "-Inf" => f64::NEG_INFINITY,
            other => other.parse().unwrap_or(f64::NAN),
        };
        let key = build_metric_name(&labels);
        groups.entry(key).or_default().push((le, e.value));
    }
    let mut out = Vec::new();
    for (key, mut buckets) in groups {
        buckets.retain(|(le, _)| !le.is_nan());
        buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        if buckets.is_empty() {
            continue;
        }
        let total = buckets.last().copied().map(|(_, c)| c).unwrap_or(0.0);
        if total <= 0.0 {
            continue;
        }
        let cum_at = |x: f64| -> f64 {
            if x <= 0.0 {
                return 0.0;
            }
            if x.is_infinite() && x > 0.0 {
                return total;
            }
            let mut prev_le = 0.0_f64;
            let mut prev_count = 0.0_f64;
            for (le, count) in &buckets {
                if x <= *le {
                    if le.is_infinite() {
                        return *count;
                    }
                    if (*le - prev_le).abs() < f64::EPSILON {
                        return *count;
                    }
                    let frac = (x - prev_le) / (*le - prev_le);
                    return prev_count + frac * (count - prev_count);
                }
                prev_le = *le;
                prev_count = *count;
            }
            total
        };
        let value = (cum_at(upper) - cum_at(lower)) / total;
        out.push(InstantVectorElement { metric_name: key, timestamp_ms: ctx.timestamp_ms, value });
    }
    Ok(Value::InstantVector(out))
}

/// Approximate `histogram_stddev(v)` / `histogram_stdvar(v)` for
/// classical histograms — uses bucket midpoints weighted by bucket
/// counts. Exact computation requires the original observations, so the
/// result is an approximation that converges as bucket density grows.
#[allow(clippy::cast_precision_loss)]
fn evaluate_histogram_stddev(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
    variance_only: bool,
) -> Result<Value, EvalError> {
    if fc.args.len() != 1 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 1,
            got: fc.args.len(),
        });
    }
    let input = evaluate(&fc.args[0], storage, ctx)?;
    let elems = match input {
        Value::InstantVector(v) => v,
        Value::Scalar(_) => {
            return Err(EvalError::FunctionArgKind { name: fc.name.clone(), want: "vector" });
        }
    };
    let mut groups: std::collections::BTreeMap<Vec<u8>, Vec<(f64, f64)>> =
        std::collections::BTreeMap::new();
    for e in elems {
        let mut labels = parse_label_map(&e.metric_name);
        labels.remove("__name__");
        let le_str = labels.remove("le").unwrap_or_default();
        let le: f64 = match le_str.as_str() {
            "+Inf" | "Inf" => f64::INFINITY,
            "-Inf" => f64::NEG_INFINITY,
            other => other.parse().unwrap_or(f64::NAN),
        };
        let key = build_metric_name(&labels);
        groups.entry(key).or_default().push((le, e.value));
    }
    let mut out = Vec::new();
    for (key, mut buckets) in groups {
        buckets.retain(|(le, _)| !le.is_nan());
        buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        if buckets.is_empty() {
            continue;
        }
        let total = buckets.last().copied().map(|(_, c)| c).unwrap_or(0.0);
        if total <= 0.0 {
            continue;
        }
        // Convert cumulative bucket counts to per-bucket weights with
        // midpoints. The lowest bucket's "left edge" is 0 (Prometheus
        // convention for non-negative observations).
        let mut prev_le = 0.0_f64;
        let mut prev_count = 0.0_f64;
        let mut sum = 0.0_f64;
        let mut sum_sq = 0.0_f64;
        for (le, count) in &buckets {
            let w = count - prev_count;
            let midpoint = if le.is_infinite() { prev_le } else { f64::midpoint(prev_le, *le) };
            sum += w * midpoint;
            sum_sq += w * midpoint * midpoint;
            prev_le = *le;
            prev_count = *count;
        }
        let mean = sum / total;
        let var = (sum_sq / total) - (mean * mean);
        let var = var.max(0.0);
        let value = if variance_only { var } else { var.sqrt() };
        out.push(InstantVectorElement { metric_name: key, timestamp_ms: ctx.timestamp_ms, value });
    }
    Ok(Value::InstantVector(out))
}

fn evaluate_sort(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
    desc: bool,
) -> Result<Value, EvalError> {
    if fc.args.len() != 1 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 1,
            got: fc.args.len(),
        });
    }
    let v = evaluate(&fc.args[0], storage, ctx)?;
    match v {
        Value::Scalar(n) => Ok(Value::Scalar(n)),
        Value::InstantVector(mut elems) => {
            elems.sort_by(|a, b| {
                let ord = a.value.partial_cmp(&b.value).unwrap_or(std::cmp::Ordering::Equal);
                if desc { ord.reverse() } else { ord }
            });
            Ok(Value::InstantVector(elems))
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum OverTimeKind {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Stddev,
    Stdvar,
    Last,
    Present,
}

fn evaluate_over_time(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
    kind: OverTimeKind,
) -> Result<Value, EvalError> {
    if fc.args.len() != 1 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 1,
            got: fc.args.len(),
        });
    }
    let inner = match &fc.args[0] {
        Expr::VectorSelector(sel) => sel.clone(),
        _ => {
            return Err(EvalError::FunctionArgKind { name: fc.name.clone(), want: "range vector" });
        }
    };
    let Some(range_ms) = inner.range_ms else {
        return Err(EvalError::FunctionArgKind {
            name: fc.name.clone(),
            want: "range vector (metric[duration])",
        });
    };

    let mut bare = inner.clone();
    bare.range_ms = None;
    // Narrow to series matching the selector's metric-name constraint via the
    // name index, then apply the full matcher set. This is the hot path for
    // every `*_over_time` query, so avoiding the all-series scan matters.
    let candidate_names: Vec<Vec<u8>> = candidate_series(storage, &bare)
        .into_iter()
        .filter(|n| matches_selector(n, &bare))
        .collect();

    // PromQL range `[d]` selects `(t-d, t]` — left-open, matching VM/Prometheus.
    // Storage returns `[min,max]` inclusive, so shift the lower bound +1ms to
    // exclude a sample at exactly `t-range_ms`.
    let window_start = ctx.timestamp_ms - range_ms + 1;
    let window_end = ctx.timestamp_ms;
    let mut out: Vec<InstantVectorElement> = Vec::new();
    for name in candidate_names {
        let mut samples = storage
            .search_by_metric_name(
                &name,
                TimeRange { min_timestamp_ms: window_start, max_timestamp_ms: window_end },
            )
            .map_err(|e| EvalError::Storage(e.to_string()))?;
        if samples.is_empty() {
            continue;
        }
        samples.sort_by_key(|s| s.timestamp_ms);
        let values: Vec<f64> = samples.iter().map(|s| s.value as f64).collect();
        let value = reduce_over_time(kind, &values);
        // `*_over_time` retains the full series identity (unlike `rate`).
        out.push(InstantVectorElement { metric_name: name, timestamp_ms: ctx.timestamp_ms, value });
    }
    Ok(Value::InstantVector(out))
}

/// Reduce a non-empty window of sample values to a single `*_over_time` value.
/// Shared by the generic per-step path and the single-pass fast path so both
/// compute identically.
/// `reduce_over_time` over a `StoredSample` window, reading `value as f64`
/// inline so the caller needn't materialize a `Vec<f64>` per series. Equivalent
/// to `reduce_over_time(kind, &w.iter().map(|s| s.value as f64).collect())`.
fn reduce_over_time_samples(kind: OverTimeKind, w: &[StoredSample]) -> f64 {
    match kind {
        OverTimeKind::Sum => w.iter().map(|s| s.value as f64).sum::<f64>(),
        OverTimeKind::Avg => w.iter().map(|s| s.value as f64).sum::<f64>() / w.len() as f64,
        OverTimeKind::Min => w.iter().map(|s| s.value as f64).fold(f64::INFINITY, f64::min),
        OverTimeKind::Max => w.iter().map(|s| s.value as f64).fold(f64::NEG_INFINITY, f64::max),
        OverTimeKind::Count => w.len() as f64,
        OverTimeKind::Last => w.last().map_or(f64::NAN, |s| s.value as f64),
        OverTimeKind::Present => 1.0,
        // Two-pass reductions: fall back to a temp Vec (rare; not in the
        // TSBS heavy-aggregation shapes).
        OverTimeKind::Stddev | OverTimeKind::Stdvar => {
            let v: Vec<f64> = w.iter().map(|s| s.value as f64).collect();
            reduce_over_time(kind, &v)
        }
    }
}

fn reduce_over_time(kind: OverTimeKind, values: &[f64]) -> f64 {
    match kind {
        OverTimeKind::Sum => values.iter().sum::<f64>(),
        OverTimeKind::Avg => values.iter().sum::<f64>() / values.len() as f64,
        OverTimeKind::Min => values.iter().copied().fold(f64::INFINITY, f64::min),
        OverTimeKind::Max => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        OverTimeKind::Count => values.len() as f64,
        OverTimeKind::Stddev => stddev_of_floats(values),
        OverTimeKind::Stdvar => {
            let s = stddev_of_floats(values);
            s * s
        }
        OverTimeKind::Last => values.last().copied().unwrap_or(f64::NAN),
        OverTimeKind::Present => 1.0,
    }
}

fn stddev_of_floats(values: &[f64]) -> f64 {
    let n = values.len() as f64;
    if n == 0.0 {
        return f64::NAN;
    }
    let mean = values.iter().sum::<f64>() / n;
    let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
    var.sqrt()
}

#[derive(Debug, Clone, Copy)]
enum RateKind {
    /// Per-second average rate over the window (counter-aware: corrects
    /// for resets, accumulator-style).
    Rate,
    /// Total increase over the window (counter-aware). No /seconds.
    Increase,
    /// Instantaneous rate from the last two samples.
    Irate,
    /// Difference between first and last sample (gauge-style; no counter
    /// reset adjustment).
    Delta,
}

/// `predict_linear(v range-vector, t scalar)` — least-squares linear
/// regression of values on (timestamp - now) across the window. Projects
/// `t` seconds forward and returns the predicted value.
fn evaluate_predict_linear(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    if fc.args.len() != 2 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 2,
            got: fc.args.len(),
        });
    }
    let t = expect_scalar(&fc.name, evaluate(&fc.args[1], storage, ctx)?)?;
    apply_range_vector_per_series(&fc.args[0], &fc.name, storage, ctx, |series, _range_ms| {
        let (slope, intercept) = linear_regression(series, ctx.timestamp_ms)?;
        Some(intercept + slope * t)
    })
}

/// `deriv(v range-vector)` — slope of the regression line in per-second
/// units. Identical math to `predict_linear` but reports the slope rather
/// than a projected value.
fn evaluate_deriv(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    if fc.args.len() != 1 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 1,
            got: fc.args.len(),
        });
    }
    apply_range_vector_per_series(&fc.args[0], &fc.name, storage, ctx, |series, _range_ms| {
        let (slope, _) = linear_regression(series, ctx.timestamp_ms)?;
        Some(slope)
    })
}

/// `holt_winters(v range-vector, sf, tf)` — double-exponential smoothing.
/// `sf` ∈ (0, 1] is the smoothing factor, `tf` ∈ (0, 1] the trend factor.
/// Matches Prometheus's implementation.
fn evaluate_holt_winters(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    if fc.args.len() != 3 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 3,
            got: fc.args.len(),
        });
    }
    let sf = expect_scalar(&fc.name, evaluate(&fc.args[1], storage, ctx)?)?;
    let tf = expect_scalar(&fc.name, evaluate(&fc.args[2], storage, ctx)?)?;
    if !(0.0 < sf && sf <= 1.0 && 0.0 < tf && tf <= 1.0) {
        return Err(EvalError::FunctionArgKind {
            name: fc.name.clone(),
            want: "smoothing factors in (0, 1]",
        });
    }
    apply_range_vector_per_series(&fc.args[0], &fc.name, storage, ctx, |samples, _range| {
        if samples.len() < 2 {
            return None;
        }
        let v0 = samples[0].value as f64;
        let v1 = samples[1].value as f64;
        let mut s = v0;
        let mut b = v1 - v0;
        for w in samples.windows(2).skip(1) {
            let x = w[1].value as f64;
            let prev_s = s;
            s = sf * x + (1.0 - sf) * (s + b);
            b = tf * (s - prev_s) + (1.0 - tf) * b;
        }
        Some(s)
    })
}

fn linear_regression(samples: &[esm_storage::StoredSample], now_ms: i64) -> Option<(f64, f64)> {
    if samples.len() < 2 {
        return None;
    }
    // Convert timestamps to "seconds before now" so slope is per-second.
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xx = 0.0;
    let mut sum_xy = 0.0;
    let n = samples.len() as f64;
    for s in samples {
        let x = (s.timestamp_ms - now_ms) as f64 / 1000.0;
        let y = s.value as f64;
        sum_x += x;
        sum_y += y;
        sum_xx += x * x;
        sum_xy += x * y;
    }
    let denom = n * sum_xx - sum_x * sum_x;
    if denom.abs() < f64::EPSILON {
        return None;
    }
    let slope = (n * sum_xy - sum_x * sum_y) / denom;
    let intercept = (sum_y - slope * sum_x) / n;
    Some((slope, intercept))
}

/// Shared boilerplate that walks every series matching a range-vector arg
/// and applies a per-series callback returning the computed scalar.
fn apply_range_vector_per_series<F>(
    arg: &Expr,
    fn_name: &str,
    storage: &impl QueryStore,
    ctx: EvalContext,
    compute: F,
) -> Result<Value, EvalError>
where
    F: Fn(&[esm_storage::StoredSample], i64) -> Option<f64>,
{
    let inner = match arg {
        Expr::VectorSelector(sel) => sel.clone(),
        _ => {
            return Err(EvalError::FunctionArgKind {
                name: fn_name.to_string(),
                want: "range vector",
            });
        }
    };
    let Some(range_ms) = inner.range_ms else {
        return Err(EvalError::FunctionArgKind {
            name: fn_name.to_string(),
            want: "range vector (metric[duration])",
        });
    };
    let mut bare = inner.clone();
    bare.range_ms = None;
    let candidate_names: Vec<Vec<u8>> = if let Some(name) = &bare.name
        && bare.matchers.is_empty()
    {
        let mut out = Vec::new();
        for (n, _) in storage.iter_metric_names() {
            if n == name.as_bytes()
                || (n.starts_with(name.as_bytes()) && n.get(name.len()).copied() == Some(b'{'))
            {
                out.push(n);
            }
        }
        out
    } else {
        storage
            .iter_metric_names()
            .into_iter()
            .filter_map(|(n, _)| if matches_selector(&n, &bare) { Some(n) } else { None })
            .collect()
    };
    let mut out: Vec<InstantVectorElement> = Vec::new();
    for name in candidate_names {
        let mut samples = storage
            .search_by_metric_name(
                &name,
                TimeRange {
                    min_timestamp_ms: ctx.timestamp_ms - range_ms + 1,
                    max_timestamp_ms: ctx.timestamp_ms,
                },
            )
            .map_err(|e| EvalError::Storage(e.to_string()))?;
        if samples.is_empty() {
            continue;
        }
        samples.sort_by_key(|s| s.timestamp_ms);
        let Some(v) = compute(&samples, range_ms) else { continue };
        out.push(InstantVectorElement {
            metric_name: strip_name(&name),
            timestamp_ms: ctx.timestamp_ms,
            value: v,
        });
    }
    Ok(Value::InstantVector(out))
}

fn evaluate_rate_like(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
    kind: RateKind,
) -> Result<Value, EvalError> {
    if fc.args.len() != 1 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 1,
            got: fc.args.len(),
        });
    }
    let inner = match &fc.args[0] {
        Expr::VectorSelector(sel) => sel.clone(),
        _ => {
            return Err(EvalError::FunctionArgKind { name: fc.name.clone(), want: "range vector" });
        }
    };
    let Some(range_ms) = inner.range_ms else {
        return Err(EvalError::FunctionArgKind {
            name: fc.name.clone(),
            want: "range vector (metric[duration])",
        });
    };

    // Resolve series matching the selector (treat as an instant selector
    // for the purpose of identifying which TSIDs we want). Re-use the
    // existing instant-selector matcher with the range stripped.
    let mut bare = inner.clone();
    bare.range_ms = None;
    let candidate_names: Vec<Vec<u8>> = if let Some(name) = &bare.name
        && bare.matchers.is_empty()
    {
        // Scan-by-prefix: same logic as evaluate_selector's slow-path.
        let mut out = Vec::new();
        for (n, _) in storage.iter_metric_names() {
            if n == name.as_bytes()
                || (n.starts_with(name.as_bytes()) && n.get(name.len()).copied() == Some(b'{'))
            {
                out.push(n);
            }
        }
        out
    } else {
        storage
            .iter_metric_names()
            .into_iter()
            .filter_map(|(n, _)| if matches_selector(&n, &bare) { Some(n) } else { None })
            .collect()
    };

    let window_start = ctx.timestamp_ms - range_ms + 1;
    let window_end = ctx.timestamp_ms;
    let mut out: Vec<InstantVectorElement> = Vec::new();
    for name in candidate_names {
        let mut samples = storage
            .search_by_metric_name(
                &name,
                TimeRange { min_timestamp_ms: window_start, max_timestamp_ms: window_end },
            )
            .map_err(|e| EvalError::Storage(e.to_string()))?;
        samples.sort_by_key(|s| s.timestamp_ms);
        let Some(v) = compute_rate_like(&samples, range_ms, kind) else { continue };
        out.push(InstantVectorElement {
            metric_name: strip_name_for_rate_output(&name),
            timestamp_ms: ctx.timestamp_ms,
            value: v,
        });
    }
    Ok(Value::InstantVector(out))
}

/// `rate()`/`increase()` drop the `__name__` from the output because the
/// result is no longer the original counter — Prometheus does this so the
/// rate of `http_requests_total` becomes an unlabeled `{job="api",...}`
/// vector that can be aggregated.
fn strip_name_for_rate_output(metric_name: &[u8]) -> Vec<u8> {
    let s = match std::str::from_utf8(metric_name) {
        Ok(s) => s,
        Err(_) => return metric_name.to_vec(),
    };
    if let Some(i) = s.find('{') { metric_name[i..].to_vec() } else { Vec::new() }
}

fn compute_rate_like(
    samples: &[esm_storage::StoredSample],
    range_ms: i64,
    kind: RateKind,
) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    match kind {
        RateKind::Irate => {
            let last = samples[samples.len() - 1];
            let prev = samples[samples.len() - 2];
            let dt = (last.timestamp_ms - prev.timestamp_ms) as f64 / 1000.0;
            if dt <= 0.0 {
                return None;
            }
            let dv = (last.value - prev.value) as f64;
            // Handle counter reset: if last < prev, treat as a reset.
            let dv = if dv < 0.0 { last.value as f64 } else { dv };
            Some(dv / dt)
        }
        RateKind::Rate | RateKind::Increase => {
            // Sum of positive deltas (handles counter resets).
            let mut total: f64 = 0.0;
            for w in samples.windows(2) {
                let d = (w[1].value - w[0].value) as f64;
                if d >= 0.0 {
                    total += d;
                } else {
                    // Counter reset: assume counter wrapped to 0 then went up.
                    total += w[1].value as f64;
                }
            }
            // PromQL extrapolation: extend the observed delta to cover the
            // full requested window. Simple, approximate; full Prometheus
            // semantics use end-of-window offsets.
            let observed_ms = samples.last()?.timestamp_ms - samples.first()?.timestamp_ms;
            let extrapolated = if observed_ms > 0 {
                total * (range_ms as f64 / observed_ms as f64)
            } else {
                total
            };
            match kind {
                RateKind::Rate => Some(extrapolated / (range_ms as f64 / 1000.0)),
                RateKind::Increase => Some(extrapolated),
                _ => unreachable!(),
            }
        }
        RateKind::Delta => {
            let first = samples.first()?;
            let last = samples.last()?;
            let dv = (last.value - first.value) as f64;
            let observed_ms = last.timestamp_ms - first.timestamp_ms;
            if observed_ms > 0 {
                Some(dv * (range_ms as f64 / observed_ms as f64))
            } else {
                Some(dv)
            }
        }
    }
}

fn apply_to_vector_or_scalar(
    fc: &crate::ast::FunctionCall,
    storage: &impl QueryStore,
    ctx: EvalContext,
    f: fn(f64) -> f64,
) -> Result<Value, EvalError> {
    if fc.args.len() != 1 {
        return Err(EvalError::FunctionArity {
            name: fc.name.clone(),
            want: 1,
            got: fc.args.len(),
        });
    }
    let v = evaluate(&fc.args[0], storage, ctx)?;
    Ok(match v {
        Value::Scalar(n) => Value::Scalar(f(n)),
        Value::InstantVector(mut elems) => {
            for e in &mut elems {
                e.value = f(e.value);
            }
            Value::InstantVector(elems)
        }
    })
}

fn apply_unary(op: UnaryOp, v: Value) -> Value {
    let factor: f64 = match op {
        UnaryOp::Pos => 1.0,
        UnaryOp::Neg => -1.0,
    };
    match v {
        Value::Scalar(n) => Value::Scalar(factor * n),
        Value::InstantVector(mut elems) => {
            for e in &mut elems {
                e.value *= factor;
            }
            Value::InstantVector(elems)
        }
    }
}

fn evaluate_selector(
    sel: &VectorSelector,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Vec<InstantVectorElement>, EvalError> {
    // Range vectors aren't valid for instant queries — the executor will
    // accept them once function calls land (`rate(metric[5m])`).
    if sel.range_ms.is_some() {
        return Err(EvalError::RangeVectorInInstantPosition);
    }
    // Apply `@` and `offset` modifiers to derive the effective eval time.
    let mut effective_ms = match sel.at_timestamp_sec {
        Some(secs) => (secs * 1000.0) as i64,
        None => ctx.timestamp_ms,
    };
    if let Some(off) = sel.offset_ms {
        effective_ms = effective_ms.saturating_sub(off);
    }
    let range = TimeRange {
        min_timestamp_ms: effective_ms.saturating_sub(ctx.lookback_ms),
        max_timestamp_ms: effective_ms,
    };

    // Fast path: try a direct lookup using the canonical key only when the
    // selector specifies *every* known label of the target series. We can't
    // know that ahead of time without a label-aware index, so the fast path
    // is restricted to exact-name selectors with no `{}` clause — those can
    // only match series stored without any labels. Anything else uses the
    // scan below.
    if sel.matchers.is_empty()
        && let Some(name) = sel.name.as_deref()
        && storage.lookup_tsid(name.as_bytes()).is_some()
    {
        // Series stored under exactly this bare name (no labels). The scan
        // will also pick this up but the direct path avoids a full scan.
        let hits = storage
            .search_by_metric_name(name.as_bytes(), range)
            .map_err(|e| EvalError::Storage(e.to_string()))?;
        let direct = latest_per_series_at(name.as_bytes().to_vec(), hits, effective_ms);
        if !direct.is_empty() {
            // Also fall through to scan so that label-bearing variants of
            // the same `__name__` aren't missed when both forms exist.
            let mut out = direct;
            for (n, _) in storage.iter_metric_names() {
                if n.starts_with(name.as_bytes())
                    && n.get(name.len()).copied() == Some(b'{')
                    && matches_selector(&n, sel)
                {
                    let hits = storage
                        .search_by_metric_name(&n, range)
                        .map_err(|e| EvalError::Storage(e.to_string()))?;
                    out.extend(latest_per_series_at(n, hits, effective_ms));
                }
            }
            return Ok(out);
        }
    }

    // Slow path: scan all known metric names and post-filter via the
    // selector's matchers. Replaced by the indexdb scan in Phase 1D.
    let mut out: Vec<InstantVectorElement> = Vec::new();
    for (name, _tsid) in storage.iter_metric_names() {
        if matches_selector(&name, sel) {
            let hits = storage
                .search_by_metric_name(&name, range)
                .map_err(|e| EvalError::Storage(e.to_string()))?;
            out.extend(latest_per_series_at(name, hits, effective_ms));
        }
    }
    Ok(out)
}

fn latest_per_series_at(
    metric_name: Vec<u8>,
    mut hits: Vec<esm_storage::StoredSample>,
    upper_bound_ms: i64,
) -> Vec<InstantVectorElement> {
    if hits.is_empty() {
        return Vec::new();
    }
    hits.sort_by_key(|s| s.timestamp_ms);
    let last = hits.last().expect("checked non-empty");
    if last.timestamp_ms > upper_bound_ms {
        return Vec::new();
    }
    vec![InstantVectorElement {
        metric_name,
        timestamp_ms: last.timestamp_ms,
        // Phase 1B stores i64; floats land alongside the decimal codec.
        // Convert here so PromQL stays in f64 internally.
        #[allow(clippy::cast_precision_loss)]
        value: last.value as f64,
    }]
}

/// Build a canonical storage key for a selector that only uses `Equal`
/// matchers. Used to identify the legacy fast path; kept for the upcoming
/// label-aware indexdb work, hence the `#[allow]`.
#[allow(dead_code)]
fn canonical_storage_key(sel: &VectorSelector) -> Result<Vec<u8>, EvalError> {
    // Storage canonical key matches the text-exposition parser's output:
    // `metric_name{label1="v1",label2="v2"}` with labels sorted.
    let mut equal_matchers: BTreeMap<&str, &str> = BTreeMap::new();
    for m in &sel.matchers {
        if m.op == MatchOp::Equal {
            if m.name == "__name__" {
                continue;
            }
            equal_matchers.insert(&m.name, &m.value);
        }
    }
    let name = if let Some(n) = sel.name.as_deref() {
        n
    } else {
        // Anonymous selector: must have __name__ matcher with Equal op.
        sel.matchers
            .iter()
            .find(|m| m.name == "__name__" && m.op == MatchOp::Equal)
            .map(|m| m.value.as_str())
            .ok_or(EvalError::AnonymousSelectorWithoutName)?
    };
    let mut out = Vec::with_capacity(name.len() + 32);
    out.extend_from_slice(name.as_bytes());
    if !equal_matchers.is_empty() {
        out.push(b'{');
        for (i, (k, v)) in equal_matchers.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.extend_from_slice(k.as_bytes());
            out.extend_from_slice(b"=\"");
            for c in v.chars() {
                match c {
                    '\\' => out.extend_from_slice(b"\\\\"),
                    '"' => out.extend_from_slice(b"\\\""),
                    '\n' => out.extend_from_slice(b"\\n"),
                    other => {
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(other.encode_utf8(&mut buf).as_bytes());
                    }
                }
            }
            out.push(b'"');
        }
        out.push(b'}');
    }
    Ok(out)
}

/// Candidate series keys for a selector, narrowed via the metric-name index
/// when the selector pins `__name__` (a literal name or `__name__=~regex`).
/// Falls back to the full series list otherwise. The caller still applies
/// [`matches_selector`], so an over-broad candidate set stays correct — this
/// only ever *narrows*, never drops a real match.
/// If `pat` is a pure alternation of plain literals (`a|b|c`, or a single
/// literal, with no regex metacharacters), return the literals — so a
/// `label=~'a|b|c'` matcher can be resolved by exact index lookups instead of
/// a scan. Returns `None` for any pattern with regex structure, leaving the
/// caller to fall back to scanning + `matches_selector`. Sound because the
/// matcher's regex is anchored (`^(?:…)$`), so each literal alternative matches
/// exactly one value.
fn literal_alternation(pat: &str) -> Option<Vec<&str>> {
    const META: &[char] = &['.', '^', '$', '*', '+', '?', '(', ')', '[', ']', '{', '}', '\\'];
    if pat.is_empty() {
        return None;
    }
    let parts: Vec<&str> = pat.split('|').collect();
    if parts.iter().any(|p| p.is_empty() || p.contains(META)) {
        return None;
    }
    Some(parts)
}

fn candidate_series(storage: &impl QueryStore, sel: &VectorSelector) -> Vec<Vec<u8>> {
    use std::collections::HashSet;

    // Collect a posting list per anchorable constraint. Each list is a
    // *superset* of the true matches for that constraint, so intersecting them
    // never drops a real match; `matches_selector` (applied by the caller)
    // does the exact filtering afterwards.
    let mut sets: Vec<Vec<Vec<u8>>> = Vec::new();

    // Literal metric-name anchor: `sel.name` / `__name__="..."`.
    let literal = sel.name.as_deref().or_else(|| {
        sel.matchers
            .iter()
            .find(|m| m.name == "__name__" && m.op == MatchOp::Equal)
            .map(|m| m.value.as_str())
    });
    if let Some(name) = literal {
        sets.push(storage.series_for_metric_name(name.as_bytes()));
    }

    // Per-matcher anchors from the index:
    // - equality `label="v"` (non-empty) → that label's posting;
    // - `label=~'v1|v2|...'` where every alternative is a plain literal → the
    //   union of those exact postings (resolved via the index, not a scan).
    //   This is the common multi-host shape `hostname=~'host_a|host_b|...'`.
    // An empty equality value means "label absent or empty", which the index
    // can't represent, so skip it.
    for m in &sel.matchers {
        if m.name == "__name__" {
            continue;
        }
        match m.op {
            MatchOp::Equal if !m.value.is_empty() => {
                sets.push(storage.series_for_label(m.name.as_bytes(), m.value.as_bytes()));
            }
            MatchOp::RegexMatch => {
                if let Some(lits) = literal_alternation(&m.value) {
                    let mut out = Vec::new();
                    for lit in lits {
                        out.extend(storage.series_for_label(m.name.as_bytes(), lit.as_bytes()));
                    }
                    sets.push(out);
                }
            }
            _ => {}
        }
    }

    // A literal-alternation `__name__=~'a|b|...'` resolves via exact name
    // lookups too (no scan).
    if let Some(m) =
        sel.matchers.iter().find(|m| m.name == "__name__" && m.op == MatchOp::RegexMatch)
        && let Some(lits) = literal_alternation(&m.value)
    {
        let mut out = Vec::new();
        for lit in lits {
            out.extend(storage.series_for_metric_name(lit.as_bytes()));
        }
        sets.push(out);
    }

    // General `__name__=~regex` anchor: only when nothing cheaper anchors the
    // set. Resolving it scans every distinct metric name and unions all
    // matching metrics' full series lists — for `cpu_(...)` that's all 10
    // metrics × every host. When a cheaper anchor (equality / literal-
    // alternation label) is present its posting is already a valid superset
    // (the caller's `matches_selector` applies the regex exactly), so skip the
    // scan and avoid materializing thousands of names only to intersect away.
    if sets.is_empty()
        && let Some(m) =
            sel.matchers.iter().find(|m| m.name == "__name__" && m.op == MatchOp::RegexMatch)
    {
        let mut out = Vec::new();
        for dn in storage.distinct_metric_names() {
            if std::str::from_utf8(&dn).is_ok_and(|s| regex_full_match(s, &m.value)) {
                out.extend(storage.series_for_metric_name(&dn));
            }
        }
        sets.push(out);
    }

    if sets.is_empty() {
        // No anchorable constraint: full scan.
        return storage.iter_metric_names().into_iter().map(|(n, _)| n).collect();
    }
    if sets.len() == 1 {
        // Single anchor: no intersection needed — skip the HashSet build.
        return sets.pop().unwrap_or_default();
    }

    // Intersect, starting from the smallest posting list.
    sets.sort_by_key(Vec::len);
    let mut acc: HashSet<Vec<u8>> = sets[0].iter().cloned().collect();
    for s in &sets[1..] {
        let other: HashSet<&[u8]> = s.iter().map(Vec::as_slice).collect();
        acc.retain(|x| other.contains(x.as_slice()));
    }
    acc.into_iter().collect()
}

fn matches_selector(name: &[u8], sel: &VectorSelector) -> bool {
    let Ok(s) = std::str::from_utf8(name) else { return false };
    let (metric, labels_str) = split_metric_and_labels(s);
    if let Some(n) = sel.name.as_deref() {
        if metric != n {
            return false;
        }
    }
    let labels = parse_label_string(labels_str);
    for matcher in &sel.matchers {
        if matcher.name == "__name__" {
            if !match_op_check(metric, &matcher.value, matcher.op) {
                return false;
            }
            continue;
        }
        let actual = labels.get(matcher.name.as_str()).copied().unwrap_or("");
        if !match_op_check(actual, &matcher.value, matcher.op) {
            return false;
        }
    }
    true
}

fn split_metric_and_labels(s: &str) -> (&str, &str) {
    match s.find('{') {
        Some(brace) => {
            let metric = &s[..brace];
            let labels = s[brace..].trim_start_matches('{').trim_end_matches('}');
            (metric, labels)
        }
        None => (s, ""),
    }
}

fn parse_label_string(s: &str) -> BTreeMap<&str, &str> {
    let mut out: BTreeMap<&str, &str> = BTreeMap::new();
    if s.is_empty() {
        return out;
    }
    for part in s.split(',') {
        // `name="value"` — we trust the canonical form.
        let Some(eq) = part.find('=') else { continue };
        let name = &part[..eq];
        let raw = &part[eq + 1..];
        let value = raw.trim_start_matches('"').trim_end_matches('"');
        out.insert(name, value);
    }
    out
}

fn match_op_check(actual: &str, expected: &str, op: MatchOp) -> bool {
    match op {
        MatchOp::Equal => actual == expected,
        MatchOp::NotEqual => actual != expected,
        MatchOp::RegexMatch => regex_full_match(actual, expected),
        MatchOp::RegexNotMatch => !regex_full_match(actual, expected),
    }
}

/// Full RE2-style regex matcher with VM/Prometheus semantics: the pattern is
/// anchored as a full-string match (`^(?:pattern)$`). Compiled patterns are
/// cached per thread so a selector applied across many series compiles each
/// unique pattern once. An invalid pattern never matches (cached as `None`).
fn regex_full_match(actual: &str, pattern: &str) -> bool {
    use std::cell::RefCell;
    use std::collections::HashMap;
    thread_local! {
        static CACHE: RefCell<HashMap<String, Option<regex::Regex>>> = RefCell::new(HashMap::new());
    }
    CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        let compiled = cache
            .entry(pattern.to_string())
            .or_insert_with(|| regex::Regex::new(&format!("^(?:{pattern})$")).ok());
        compiled.as_ref().is_some_and(|re| re.is_match(actual))
    })
}

fn evaluate_binary(
    b: &BinaryExpr,
    storage: &impl QueryStore,
    ctx: EvalContext,
) -> Result<Value, EvalError> {
    let lhs = evaluate(&b.lhs, storage, ctx)?;
    let rhs = evaluate(&b.rhs, storage, ctx)?;
    apply_binary(b.op, lhs, rhs, b.return_bool, b.matching.as_ref())
}

fn apply_binary(
    op: BinaryOp,
    lhs: Value,
    rhs: Value,
    return_bool: bool,
    matching: Option<&VectorMatching>,
) -> Result<Value, EvalError> {
    // Logical operators only make sense between two instant vectors.
    if matches!(op, BinaryOp::And | BinaryOp::Or | BinaryOp::Unless) {
        return match (lhs, rhs) {
            (Value::InstantVector(a), Value::InstantVector(b)) => {
                Ok(logical_op(op, a, b, matching))
            }
            _ => Err(EvalError::NotYetImplemented(format!("logical op {op:?} between scalars"))),
        };
    }
    match (lhs, rhs) {
        (Value::Scalar(a), Value::Scalar(b)) => Ok(Value::Scalar(scalar_op(op, a, b, return_bool))),
        (Value::Scalar(scalar), Value::InstantVector(elems)) => {
            Ok(scalar_vector_op(op, scalar, elems, return_bool, /*scalar_is_lhs=*/ true))
        }
        (Value::InstantVector(elems), Value::Scalar(scalar)) => {
            Ok(scalar_vector_op(op, scalar, elems, return_bool, /*scalar_is_lhs=*/ false))
        }
        (Value::InstantVector(a), Value::InstantVector(b)) => {
            Ok(vector_vector_op(op, a, b, return_bool, matching))
        }
    }
}

/// PromQL logical set operations:
/// - `a and b` — emit lhs elements whose match key has a counterpart in rhs.
/// - `a or b`  — union: lhs elements + rhs elements whose match key isn't already in lhs.
/// - `a unless b` — emit lhs elements whose match key is *not* in rhs.
fn logical_op(
    op: BinaryOp,
    lhs: Vec<InstantVectorElement>,
    rhs: Vec<InstantVectorElement>,
    matching: Option<&VectorMatching>,
) -> Value {
    let lhs_parsed: Vec<(InstantVectorElement, Vec<u8>)> = lhs
        .into_iter()
        .map(|e| {
            let labels = parse_label_map(&e.metric_name);
            let k = match_key(&e.metric_name, &labels, matching);
            (e, k)
        })
        .collect();
    let rhs_parsed: Vec<(InstantVectorElement, Vec<u8>)> = rhs
        .into_iter()
        .map(|e| {
            let labels = parse_label_map(&e.metric_name);
            let k = match_key(&e.metric_name, &labels, matching);
            (e, k)
        })
        .collect();
    let rhs_keys: std::collections::BTreeSet<Vec<u8>> =
        rhs_parsed.iter().map(|(_, k)| k.clone()).collect();
    let mut out: Vec<InstantVectorElement> = Vec::new();
    match op {
        BinaryOp::And => {
            for (e, k) in lhs_parsed {
                if rhs_keys.contains(&k) {
                    out.push(e);
                }
            }
        }
        BinaryOp::Unless => {
            for (e, k) in lhs_parsed {
                if !rhs_keys.contains(&k) {
                    out.push(e);
                }
            }
        }
        BinaryOp::Or => {
            let lhs_keys: std::collections::BTreeSet<Vec<u8>> =
                lhs_parsed.iter().map(|(_, k)| k.clone()).collect();
            for (e, _) in lhs_parsed {
                out.push(e);
            }
            for (e, k) in rhs_parsed {
                if !lhs_keys.contains(&k) {
                    out.push(e);
                }
            }
        }
        _ => unreachable!("logical_op called with non-logical op"),
    }
    Value::InstantVector(out)
}

/// Pair two instant vectors. Without a matching modifier the default is
/// 1-to-1 over all labels except `__name__`. `on(labels)` restricts the
/// match key; `ignoring(labels)` removes specific labels from it.
/// `group_left`/`group_right` promote the join to 1-to-N / N-to-1.
fn vector_vector_op(
    op: BinaryOp,
    lhs: Vec<InstantVectorElement>,
    rhs: Vec<InstantVectorElement>,
    return_bool: bool,
    matching: Option<&VectorMatching>,
) -> Value {
    let is_filtering_comparison = op.is_comparison() && !return_bool;

    // Pre-extract every element's parsed labels once.
    let lhs_parsed: Vec<(Vec<u8>, std::collections::BTreeMap<String, String>, f64, i64)> = lhs
        .into_iter()
        .map(|e| {
            let labels = parse_label_map(&e.metric_name);
            (e.metric_name, labels, e.value, e.timestamp_ms)
        })
        .collect();
    let rhs_parsed: Vec<(Vec<u8>, std::collections::BTreeMap<String, String>, f64, i64)> = rhs
        .into_iter()
        .map(|e| {
            let labels = parse_label_map(&e.metric_name);
            (e.metric_name, labels, e.value, e.timestamp_ms)
        })
        .collect();

    let include_labels: Vec<String> =
        matching.and_then(|m| m.group.as_ref().map(|g| g.include.clone())).unwrap_or_default();
    let group_side = matching.and_then(|m| m.group.as_ref().map(|g| g.side));

    // Index rhs by match key. With group_left, we keep all rhs candidates
    // per key; with group_right or default 1:1, the rhs is the "one" side.
    let mut rhs_by_key: BTreeMap<Vec<u8>, Vec<usize>> = BTreeMap::new();
    for (i, (mn, labels, _, _)) in rhs_parsed.iter().enumerate() {
        let k = match_key(mn, labels, matching);
        rhs_by_key.entry(k).or_default().push(i);
    }

    let mut out: Vec<InstantVectorElement> = Vec::new();
    for (l_mn, l_labels, l_val, l_ts) in &lhs_parsed {
        let k = match_key(l_mn, l_labels, matching);
        let Some(r_indices) = rhs_by_key.get(&k) else { continue };

        let select_rhs: Vec<usize> = match group_side {
            // group_left: many lhs paired with one rhs (we're the "many" side
            // and rhs is the "one" — error if multiple rhs match).
            Some(GroupSide::Left) => {
                if r_indices.len() > 1 {
                    // PromQL would error; for the MVP we just take the first.
                    vec![r_indices[0]]
                } else {
                    r_indices.clone()
                }
            }
            // group_right: many rhs paired with one lhs. lhs is the "one" side.
            // Each rhs gets its own output row.
            Some(GroupSide::Right) => r_indices.clone(),
            // No grouping: strict 1:1.
            None => {
                if r_indices.len() > 1 {
                    continue; // Ambiguous — drop in MVP.
                }
                r_indices.clone()
            }
        };

        for r_idx in select_rhs {
            let (_r_mn, r_labels, r_val, _) = &rhs_parsed[r_idx];
            let value = scalar_op(op, *l_val, *r_val, return_bool);
            if is_filtering_comparison {
                if value.is_nan() {
                    continue;
                }
                // Filter mode: keep lhs's identity + original value.
                let metric_name = strip_name(l_mn);
                out.push(InstantVectorElement { metric_name, timestamp_ms: *l_ts, value: *l_val });
                continue;
            }

            // Construct output labels.
            let mut out_labels = match group_side {
                Some(GroupSide::Right) => {
                    // Many side is rhs; rhs's labels form the identity.
                    let mut m = r_labels.clone();
                    // Copy in any include labels from lhs.
                    for inc in &include_labels {
                        if let Some(v) = l_labels.get(inc) {
                            m.insert(inc.clone(), v.clone());
                        } else {
                            m.remove(inc);
                        }
                    }
                    m
                }
                Some(GroupSide::Left) => {
                    let mut m = l_labels.clone();
                    for inc in &include_labels {
                        if let Some(v) = r_labels.get(inc) {
                            m.insert(inc.clone(), v.clone());
                        } else {
                            m.remove(inc);
                        }
                    }
                    m
                }
                None => l_labels.clone(),
            };
            // Arithmetic between distinct metrics drops `__name__`.
            if !op.is_comparison() || !return_bool {
                out_labels.remove("__name__");
            }
            let metric_name = build_metric_name(&out_labels);
            out.push(InstantVectorElement { metric_name, timestamp_ms: *l_ts, value });
        }
    }
    Value::InstantVector(out)
}

fn parse_label_map(metric_name: &[u8]) -> std::collections::BTreeMap<String, String> {
    let s = match std::str::from_utf8(metric_name) {
        Ok(s) => s,
        Err(_) => return std::collections::BTreeMap::new(),
    };
    let (name, labels_str) = match s.find('{') {
        Some(i) => (&s[..i], s[i..].trim_start_matches('{').trim_end_matches('}')),
        None => (s, ""),
    };
    let mut out = std::collections::BTreeMap::new();
    if !name.is_empty() {
        out.insert("__name__".to_string(), name.to_string());
    }
    if !labels_str.is_empty() {
        for part in labels_str.split(',') {
            let Some(eq) = part.find('=') else { continue };
            let k = &part[..eq];
            let raw = &part[eq + 1..];
            let v = raw.trim_start_matches('"').trim_end_matches('"');
            out.insert(k.to_string(), v.to_string());
        }
    }
    out
}

fn match_key(
    metric_name: &[u8],
    labels: &std::collections::BTreeMap<String, String>,
    matching: Option<&VectorMatching>,
) -> Vec<u8> {
    use std::fmt::Write as _;
    let mut filtered: std::collections::BTreeMap<&str, &str> = std::collections::BTreeMap::new();
    match matching {
        Some(m) => match m.kind {
            VectorMatchingKind::On => {
                for lbl in &m.labels {
                    if let Some(v) = labels.get(lbl) {
                        filtered.insert(lbl.as_str(), v.as_str());
                    }
                }
            }
            VectorMatchingKind::Ignoring => {
                for (k, v) in labels {
                    if k == "__name__" {
                        continue;
                    }
                    if m.labels.iter().any(|l| l == k) {
                        continue;
                    }
                    filtered.insert(k.as_str(), v.as_str());
                }
            }
        },
        None => {
            // Default: all labels except `__name__`.
            for (k, v) in labels {
                if k == "__name__" {
                    continue;
                }
                filtered.insert(k.as_str(), v.as_str());
            }
        }
    }
    let _ = metric_name;
    let mut s = String::new();
    for (k, v) in filtered {
        let _ = write!(s, "{k}={v}|");
    }
    s.into_bytes()
}

fn build_metric_name(labels: &std::collections::BTreeMap<String, String>) -> Vec<u8> {
    let name = labels.get("__name__").cloned().unwrap_or_default();
    let mut out = Vec::new();
    out.extend_from_slice(name.as_bytes());
    let mut other: Vec<(&String, &String)> =
        labels.iter().filter(|(k, _)| k.as_str() != "__name__").collect();
    other.sort_by(|a, b| a.0.cmp(b.0));
    if !other.is_empty() {
        out.push(b'{');
        for (i, (k, v)) in other.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.extend_from_slice(k.as_bytes());
            out.extend_from_slice(b"=\"");
            for c in v.chars() {
                match c {
                    '\\' => out.extend_from_slice(b"\\\\"),
                    '"' => out.extend_from_slice(b"\\\""),
                    '\n' => out.extend_from_slice(b"\\n"),
                    other => {
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(other.encode_utf8(&mut buf).as_bytes());
                    }
                }
            }
            out.push(b'"');
        }
        out.push(b'}');
    }
    out
}

/// Build a comparison key from a stored metric name that ignores
/// `__name__`. This is what default PromQL vector matching uses: labels
/// match except the metric name itself.
fn labels_only_key(metric_name: &[u8]) -> Vec<u8> {
    let s = match std::str::from_utf8(metric_name) {
        Ok(s) => s,
        Err(_) => return metric_name.to_vec(),
    };
    if let Some(i) = s.find('{') { metric_name[i..].to_vec() } else { Vec::new() }
}

fn strip_name(metric_name: &[u8]) -> Vec<u8> {
    labels_only_key(metric_name)
}

#[allow(clippy::float_cmp)]
fn scalar_op(op: BinaryOp, a: f64, b: f64, return_bool: bool) -> f64 {
    match op {
        BinaryOp::Add => a + b,
        BinaryOp::Sub => a - b,
        BinaryOp::Mul => a * b,
        BinaryOp::Div => a / b,
        BinaryOp::Mod => a % b,
        BinaryOp::Pow => a.powf(b),
        BinaryOp::Eq => bool_or_nan(a == b, return_bool),
        BinaryOp::Ne => bool_or_nan(a != b, return_bool),
        BinaryOp::Lt => bool_or_nan(a < b, return_bool),
        BinaryOp::Le => bool_or_nan(a <= b, return_bool),
        BinaryOp::Gt => bool_or_nan(a > b, return_bool),
        BinaryOp::Ge => bool_or_nan(a >= b, return_bool),
        BinaryOp::And | BinaryOp::Or | BinaryOp::Unless => f64::NAN,
    }
}

fn bool_or_nan(b: bool, return_bool: bool) -> f64 {
    if return_bool {
        if b { 1.0 } else { 0.0 }
    } else if b {
        1.0
    } else {
        f64::NAN
    }
}

fn scalar_vector_op(
    op: BinaryOp,
    scalar: f64,
    mut elems: Vec<InstantVectorElement>,
    return_bool: bool,
    scalar_is_lhs: bool,
) -> Value {
    // Comparison without `bool` filters the vector to elements where the
    // predicate holds while keeping their original values; with `bool` we
    // produce 0/1 instead and keep every element. Arithmetic always replaces
    // the value with the result of the op.
    let is_filtering_comparison = op.is_comparison() && !return_bool;
    let mut out: Vec<InstantVectorElement> = Vec::with_capacity(elems.len());
    for e in elems.drain(..) {
        let (a, b) = if scalar_is_lhs { (scalar, e.value) } else { (e.value, scalar) };
        let result = scalar_op(op, a, b, return_bool);
        if is_filtering_comparison {
            // Filtering comparison: drop on false (NaN sentinel), preserve
            // the original sample value on pass.
            if result.is_nan() {
                continue;
            }
            out.push(e);
        } else {
            out.push(InstantVectorElement { value: result, ..e });
        }
    }
    Value::InstantVector(out)
}

#[derive(Debug, Error)]
pub enum EvalError {
    #[error("range vector in instant-query position")]
    RangeVectorInInstantPosition,
    #[error("anonymous vector selector requires an `__name__` matcher")]
    AnonymousSelectorWithoutName,
    #[error("not yet implemented: {0}")]
    NotYetImplemented(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("unknown function `{0}`")]
    UnknownFunction(String),
    #[error("function `{name}` expects {want} arguments, got {got}")]
    FunctionArity { name: String, want: usize, got: usize },
    #[error("function `{name}` expects a {want} argument")]
    FunctionArgKind { name: String, want: &'static str },
    #[error("range query step must be > 0; got {0}")]
    InvalidStep(i64),
    #[error("range query end {end} precedes start {start}")]
    InvalidRange { start: i64, end: i64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse;
    use esm_storage::{Sample, Storage};

    #[test]
    fn candidate_series_is_a_superset() {
        // candidate_series must return a *superset* of the true matches, since
        // the caller filters exactly with matches_selector. This guards the
        // index-anchor shortcuts (equality / literal-alternation) — a bug there
        // would silently drop series, which the fast-vs-generic test cannot
        // catch (both share candidate_series).
        let mut owned: Vec<(Vec<u8>, i64, i64)> = Vec::new();
        for metric in ["cpu_usage_user", "cpu_usage_system", "mem_used"] {
            for host in ["a", "b", "c", "d"] {
                owned.push((format!("{metric}{{hostname=\"{host}\"}}").into_bytes(), 1000, 1));
            }
        }
        let refs: Vec<(&[u8], i64, i64)> =
            owned.iter().map(|(n, t, v)| (n.as_slice(), *t, *v)).collect();
        let (s, _t) = open_storage(&refs);
        let all: Vec<Vec<u8>> = s.iter_metric_names().into_iter().map(|(n, _)| n).collect();

        let selectors = [
            "cpu_usage_user{hostname=\"a\"}",
            "{__name__=~\"cpu_(usage_user|usage_system)\", hostname=\"b\"}",
            "{__name__=~\"cpu_(usage_user|usage_system)\", hostname=~\"a|c\"}",
            "{__name__=~\"cpu_usage_user|mem_used\"}",
            "cpu_usage_user{hostname=~\"a|b|nonexistent\"}",
            "{__name__=~\"cpu_(usage_user|usage_system)\"}",
            "{hostname=~\"a|b\"}",
        ];
        for q in selectors {
            let expr = parse(q).unwrap();
            let Expr::VectorSelector(sel) = &expr else { panic!("not a selector: {q}") };
            let got: std::collections::HashSet<Vec<u8>> =
                candidate_series(&s, sel).into_iter().collect();
            let truth: Vec<&Vec<u8>> = all.iter().filter(|n| matches_selector(n, sel)).collect();
            for n in truth {
                assert!(
                    got.contains(n),
                    "candidate_series dropped a true match {:?} for {q}",
                    String::from_utf8_lossy(n)
                );
            }
        }
    }

    fn open_storage(samples: &[(&[u8], i64, i64)]) -> (Storage, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = Storage::open(tmp.path().join("d")).unwrap();
        let conv: Vec<Sample> = samples
            .iter()
            .map(|(n, t, v)| Sample { metric_name: n.to_vec(), timestamp_ms: *t, value: *v })
            .collect();
        s.ingest(&conv).unwrap();
        s.flush().unwrap();
        (s, tmp)
    }

    fn vlen(v: &Value) -> usize {
        match v {
            Value::InstantVector(e) => e.len(),
            _ => 0,
        }
    }

    #[test]
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    fn fast_path_matches_generic() {
        // Multi-metric, multi-host dataset over several timestamps so windows,
        // steps, grouping, and empty-window handling are all exercised.
        let mut owned: Vec<(Vec<u8>, i64, i64)> = Vec::new();
        for (mi, metric) in ["cpu_usage_user", "cpu_usage_system"].iter().enumerate() {
            for (hi, host) in ["a", "b", "c"].iter().enumerate() {
                for k in 0..10i64 {
                    // Leave a gap (skip k==4) to create empty windows for some steps.
                    if k == 4 {
                        continue;
                    }
                    let name = format!("{metric}{{hostname=\"{host}\"}}").into_bytes();
                    let ts = 1000 + k * 1000;
                    let v = (mi as i64 + 1) * 7 + k * 3 + hi as i64 * 2;
                    owned.push((name, ts, v));
                }
            }
        }
        let refs: Vec<(&[u8], i64, i64)> =
            owned.iter().map(|(n, t, v)| (n.as_slice(), *t, *v)).collect();
        let (s, _t) = open_storage(&refs);

        let queries = [
            "max_over_time(cpu_usage_user[3000ms])",
            "avg(avg_over_time(cpu_usage_user[5000ms])) by (__name__)",
            "max(max_over_time({__name__=~\"cpu_(usage_user|usage_system)\"}[4000ms])) by (__name__)",
            "sum(sum_over_time({__name__=~\"cpu_(usage_user|usage_system)\"}[3000ms])) by (__name__, hostname)",
            "max(max_over_time(cpu_usage_user{hostname=\"a\"}[2000ms])) by (__name__)",
            "min(min_over_time(cpu_usage_system[10000ms])) by (hostname)",
            "count(count_over_time(cpu_usage_user[5000ms])) by (__name__)",
            "avg(avg_over_time({__name__=~\"cpu_(usage_user|usage_system)\"}[4000ms])) without (hostname)",
        ];
        let (start, end, step) = (1000i64, 10_000i64, 1000i64);

        let norm = |mut v: Vec<RangeVectorElement>| -> Vec<(Vec<u8>, Vec<(i64, i64)>)> {
            v.sort_by(|a, b| a.metric_name.cmp(&b.metric_name));
            v.into_iter()
                .map(|e| {
                    let vals =
                        e.values.into_iter().map(|(t, x)| (t, (x * 1e6).round() as i64)).collect();
                    (e.metric_name, vals)
                })
                .collect()
        };

        for q in queries {
            let expr = parse(q).unwrap();
            let fast = try_single_pass(&expr, &s, start, end, step)
                .unwrap_or_else(|| panic!("fast path should handle: {q}"))
                .unwrap();
            let generic = evaluate_range_generic(&expr, &s, start, end, step).unwrap();
            assert_eq!(norm(fast), norm(generic), "fast vs generic mismatch for: {q}");
        }
    }

    #[test]
    fn over_time_range_window_is_left_open() {
        // PromQL `m[d]` at `t` selects the half-open interval `(t-d, t]`: a
        // sample at exactly `t-d` is excluded (matches VictoriaMetrics &
        // Prometheus). Regression test for the range-window boundary fix.
        let (s, _t) = open_storage(&[
            (b"m", 1000, 10),
            (b"m", 2000, 20),
            (b"m", 3000, 30),
            (b"m", 4000, 40),
            (b"m", 5000, 50),
        ]);
        let ctx = EvalContext::instant(5000);
        // Window (3000, 5000] => {4000, 5000}; the sample at t-d=3000 is excluded.
        let count = match evaluate(&parse("count_over_time(m[2000ms])").unwrap(), &s, ctx).unwrap()
        {
            Value::InstantVector(v) => v[0].value,
            other => panic!("expected instant vector, got {other:?}"),
        };
        assert!(
            (count - 2.0).abs() < 1e-9,
            "count_over_time must exclude the sample at t-range; got {count}"
        );
        let sum = match evaluate(&parse("sum_over_time(m[2000ms])").unwrap(), &s, ctx).unwrap() {
            Value::InstantVector(v) => v[0].value,
            other => panic!("expected instant vector, got {other:?}"),
        };
        assert!((sum - 90.0).abs() < 1e-9, "sum_over_time window is (t-range, t]; got {sum}");
    }

    #[test]
    fn selector_regex_and_bare_label_matching() {
        let (s, _t) = open_storage(&[
            (b"cpu_usage_user{hostname=\"h0\"}", 1000, 10),
            (b"cpu_usage_system{hostname=\"h0\"}", 1000, 20),
            (b"cpu_usage_user{hostname=\"h1\"}", 1000, 30),
        ]);
        let ctx = EvalContext::instant(1000);
        // Regex alternation on __name__ (TSBS single-groupby-5 / double-groupby / cpu-max-all).
        let a = evaluate(
            &parse("max_over_time({__name__=~\"cpu_(usage_user|usage_system)\"}[1h])").unwrap(),
            &s,
            ctx,
        )
        .unwrap();
        assert_eq!(vlen(&a), 3, "regex __name__ alternation should match all 3 series");
        // Regex alternation on a normal label (cpu-max-all-8 uses hostname=~h1|h2|...).
        let h = evaluate(
            &parse("max_over_time({__name__=\"cpu_usage_user\",hostname=~\"h0|h1\"}[1h])").unwrap(),
            &s,
            ctx,
        )
        .unwrap();
        assert_eq!(vlen(&h), 2, "regex hostname alternation should match both hosts");
        // Bare label-only selector.
        let b = evaluate(&parse("max_over_time({hostname=\"h0\"}[1h])").unwrap(), &s, ctx).unwrap();
        assert_eq!(vlen(&b), 2, "bare hostname selector should match both h0 metrics");
    }

    #[test]
    fn aggregation_by_name_groups_per_metric() {
        let (s, _t) = open_storage(&[
            (b"cpu_usage_user{hostname=\"h0\"}", 1000, 10),
            (b"cpu_usage_system{hostname=\"h0\"}", 1000, 20),
            (b"cpu_usage_user{hostname=\"h1\"}", 1000, 30),
        ]);
        let ctx = EvalContext::instant(1000);
        // `by (__name__)` must produce one group per distinct metric name and
        // retain __name__ in the output labels (VM semantics; TSBS relies on it).
        let v = evaluate(
            &parse("max(max_over_time({__name__=~\"cpu_(usage_user|usage_system)\"}[1h])) by (__name__)")
                .unwrap(),
            &s,
            ctx,
        )
        .unwrap();
        let Value::InstantVector(elems) = v else { panic!("want vector") };
        assert_eq!(elems.len(), 2, "expected one group per __name__");
        let names: std::collections::BTreeSet<_> =
            elems.iter().map(|e| String::from_utf8_lossy(&e.metric_name).to_string()).collect();
        assert!(
            names.iter().any(|n| n.contains("cpu_usage_user"))
                && names.iter().any(|n| n.contains("cpu_usage_system")),
            "output must retain __name__; got {names:?}"
        );
    }

    #[test]
    fn evaluate_number_literal() {
        let tmp = tempfile::tempdir().unwrap();
        let s = Storage::open(tmp.path().join("d")).unwrap();
        let e = parse("42").unwrap();
        let v = evaluate(&e, &s, EvalContext::instant(0)).unwrap();
        match v {
            Value::Scalar(n) => assert!((n - 42.0).abs() < 1e-9),
            other => panic!("expected scalar, got {other:?}"),
        }
    }

    #[test]
    fn evaluate_simple_selector() {
        let (storage, _t) = open_storage(&[(b"up", 1000, 1), (b"up", 2000, 1), (b"down", 1500, 0)]);
        let e = parse("up").unwrap();
        let v = evaluate(&e, &storage, EvalContext::instant(3000)).unwrap();
        match v {
            Value::InstantVector(elems) => {
                assert_eq!(elems.len(), 1);
                assert_eq!(elems[0].metric_name, b"up");
                assert!((elems[0].value - 1.0).abs() < 1e-9);
                assert_eq!(elems[0].timestamp_ms, 2000); // latest within lookback
            }
            other => panic!("expected vector, got {other:?}"),
        }
    }

    #[test]
    fn evaluate_outside_lookback_returns_empty() {
        let (storage, _t) = open_storage(&[(b"up", 1000, 1)]);
        let e = parse("up").unwrap();
        // Query far in the future, beyond the 5-minute lookback.
        let v = evaluate(&e, &storage, EvalContext::instant(1_000_000_000)).unwrap();
        match v {
            Value::InstantVector(elems) => assert!(elems.is_empty()),
            other => panic!("expected vector, got {other:?}"),
        }
    }

    #[test]
    fn evaluate_scalar_arith() {
        let tmp = tempfile::tempdir().unwrap();
        let s = Storage::open(tmp.path().join("d")).unwrap();
        let e = parse("2 + 3 * 4").unwrap();
        let v = evaluate(&e, &s, EvalContext::instant(0)).unwrap();
        match v {
            Value::Scalar(n) => assert!((n - 14.0).abs() < 1e-9),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn evaluate_scalar_times_vector() {
        let (storage, _t) = open_storage(&[(b"x", 1000, 10), (b"x", 2000, 20)]);
        let e = parse("x * 3").unwrap();
        let v = evaluate(&e, &storage, EvalContext::instant(3000)).unwrap();
        match v {
            Value::InstantVector(elems) => {
                assert_eq!(elems.len(), 1);
                assert!((elems[0].value - 60.0).abs() < 1e-9);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn evaluate_comparison_filters_vector() {
        let (storage, _t) = open_storage(&[(b"a", 1000, 5), (b"b", 1000, 15), (b"c", 1000, 25)]);
        // We expect series whose latest value is > 10 to survive with the
        // *original* value, not 1.0 (filtering comparison semantics).
        let e = parse("{__name__=~\".\"} > 10").unwrap();
        let v = evaluate(&e, &storage, EvalContext::instant(2000)).unwrap();
        match v {
            Value::InstantVector(elems) => {
                assert_eq!(elems.len(), 2);
                let mut values: Vec<i64> = elems.iter().map(|e| e.value as i64).collect();
                values.sort_unstable();
                assert_eq!(values, vec![15, 25]);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn evaluate_comparison_with_bool() {
        let (storage, _t) = open_storage(&[(b"a", 1000, 5)]);
        let e = parse("a == bool 5").unwrap();
        let v = evaluate(&e, &storage, EvalContext::instant(2000)).unwrap();
        match v {
            Value::InstantVector(elems) => {
                assert_eq!(elems.len(), 1);
                assert!((elems[0].value - 1.0).abs() < 1e-9);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn evaluate_unary_negate_vector() {
        let (storage, _t) = open_storage(&[(b"x", 1000, 7)]);
        let e = parse("-x").unwrap();
        let v = evaluate(&e, &storage, EvalContext::instant(2000)).unwrap();
        match v {
            Value::InstantVector(elems) => {
                assert!((elems[0].value - -7.0).abs() < 1e-9);
            }
            other => panic!("got {other:?}"),
        }
    }
}
