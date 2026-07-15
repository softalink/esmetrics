//! Incremental aggregation for the `aggr(rollup(metricExpr[d]))` fast path.
//! Port of `aggr_incremental.go`.
//!
//! Holds only O(workers × groups) accumulator series in memory instead of
//! materializing every rolled-up input series. Per-worker maps are
//! lock-free in Go (worker-private); the Rust port wraps each per-worker map
//! in an uncontended `Mutex` so the context can be shared across scoped
//! worker threads.

use crate::aggr::remove_group_tags;
use crate::timeseries::{metric_name_group_key, Timeseries};
use esm_metricsql::AggrFuncExpr;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

type UpdateFn = fn(&mut IncrementalAggrContext, &[f64]);
type MergeFn = fn(&mut IncrementalAggrContext, &IncrementalAggrContext);
type FinalizeFn = fn(&mut IncrementalAggrContext);

/// Callbacks for one incremental aggregate function.
/// Port of Go `incrementalAggrFuncCallbacks`.
pub struct IncrementalAggrFuncCallbacks {
    update: UpdateFn,
    merge: MergeFn,
    finalize: FinalizeFn,
    /// Whether to keep the original MetricName for every series.
    keep_original: bool,
}

/// Returns the callbacks for the given aggregate function name, if it
/// supports incremental calculation.
/// Port of Go `getIncrementalAggrFuncCallbacks`.
pub fn get_incremental_aggr_func_callbacks(
    name: &str,
) -> Option<&'static IncrementalAggrFuncCallbacks> {
    let name = name.to_ascii_lowercase();
    static SUM: IncrementalAggrFuncCallbacks = IncrementalAggrFuncCallbacks {
        update: update_aggr_sum,
        merge: merge_aggr_sum,
        finalize: finalize_aggr_common,
        keep_original: false,
    };
    static MIN: IncrementalAggrFuncCallbacks = IncrementalAggrFuncCallbacks {
        update: update_aggr_min,
        merge: merge_aggr_min,
        finalize: finalize_aggr_common,
        keep_original: false,
    };
    static MAX: IncrementalAggrFuncCallbacks = IncrementalAggrFuncCallbacks {
        update: update_aggr_max,
        merge: merge_aggr_max,
        finalize: finalize_aggr_common,
        keep_original: false,
    };
    static AVG: IncrementalAggrFuncCallbacks = IncrementalAggrFuncCallbacks {
        update: update_aggr_avg,
        merge: merge_aggr_avg,
        finalize: finalize_aggr_avg,
        keep_original: false,
    };
    static COUNT: IncrementalAggrFuncCallbacks = IncrementalAggrFuncCallbacks {
        update: update_aggr_count,
        merge: merge_aggr_count,
        finalize: finalize_aggr_count,
        keep_original: false,
    };
    static SUM2: IncrementalAggrFuncCallbacks = IncrementalAggrFuncCallbacks {
        update: update_aggr_sum2,
        merge: merge_aggr_sum2,
        finalize: finalize_aggr_common,
        keep_original: false,
    };
    static GEOMEAN: IncrementalAggrFuncCallbacks = IncrementalAggrFuncCallbacks {
        update: update_aggr_geomean,
        merge: merge_aggr_geomean,
        finalize: finalize_aggr_geomean,
        keep_original: false,
    };
    static ANY: IncrementalAggrFuncCallbacks = IncrementalAggrFuncCallbacks {
        update: update_aggr_any,
        merge: merge_aggr_any,
        finalize: finalize_aggr_common,
        keep_original: true,
    };
    static GROUP: IncrementalAggrFuncCallbacks = IncrementalAggrFuncCallbacks {
        update: update_aggr_count,
        merge: merge_aggr_count,
        finalize: finalize_aggr_group,
        keep_original: false,
    };
    match name.as_str() {
        "sum" => Some(&SUM),
        "min" => Some(&MIN),
        "max" => Some(&MAX),
        "avg" => Some(&AVG),
        "count" => Some(&COUNT),
        "sum2" => Some(&SUM2),
        "geomean" => Some(&GEOMEAN),
        "any" => Some(&ANY),
        "group" => Some(&GROUP),
        _ => None,
    }
}

/// Per-group accumulator. Port of Go `incrementalAggrContext`.
/// `ts.values` holds the accumulated values; `values` holds per-point
/// counts / auxiliary state (f64 to match Go exactly — counts can grow
/// beyond u32 during avg merges).
pub struct IncrementalAggrContext {
    pub ts: Timeseries,
    pub values: Vec<f64>,
}

type AggrContextMap = HashMap<Vec<u8>, IncrementalAggrContext>;

/// Port of Go `incrementalAggrFuncContext`.
pub struct IncrementalAggrFuncContext<'a> {
    pub ae: &'a AggrFuncExpr,
    by_worker_id: Vec<Mutex<AggrContextMap>>,
    callbacks: &'static IncrementalAggrFuncCallbacks,
}

impl<'a> IncrementalAggrFuncContext<'a> {
    /// Port of Go `newIncrementalAggrFuncContext`.
    pub fn new(
        ae: &'a AggrFuncExpr,
        callbacks: &'static IncrementalAggrFuncCallbacks,
        max_workers: usize,
    ) -> Self {
        let mut by_worker_id = Vec::with_capacity(max_workers);
        for _ in 0..max_workers.max(1) {
            by_worker_id.push(Mutex::new(HashMap::new()));
        }
        IncrementalAggrFuncContext {
            ae,
            by_worker_id,
            callbacks,
        }
    }

    /// Merges one rolled-up series into the worker-private accumulator map.
    /// Port of Go `incrementalAggrFuncContext.updateTimeseries`; called from
    /// worker threads with distinct `worker_id`s, so the mutexes are
    /// uncontended.
    pub fn update_timeseries(&self, ts_orig: &mut Timeseries, worker_id: usize) {
        let mut m = self.by_worker_id[worker_id].lock();

        let keep_original = self.callbacks.keep_original;
        // Compute the group key on a modified copy when the original
        // MetricName must be kept (the `any` aggregate).
        let key = if keep_original {
            let mut mn = ts_orig.metric_name.clone();
            remove_group_tags(&mut mn, &self.ae.modifier);
            metric_name_group_key(&mut mn)
        } else {
            remove_group_tags(&mut ts_orig.metric_name, &self.ae.modifier);
            metric_name_group_key(&mut ts_orig.metric_name)
        };
        if !m.contains_key(&key) {
            if self.ae.limit > 0 && m.len() >= self.ae.limit as usize {
                // Skip this series: the limit on the number of output series
                // has been reached.
                return;
            }
            let ts_aggr = Timeseries {
                metric_name: ts_orig.metric_name.clone(),
                values: vec![0f64; ts_orig.values.len()],
                timestamps: Arc::clone(&ts_orig.timestamps),
            };
            let iac = IncrementalAggrContext {
                ts: ts_aggr,
                values: vec![0f64; ts_orig.values.len()],
            };
            m.insert(key.clone(), iac);
        }
        let iac = m.get_mut(&key).expect("just inserted");
        (self.callbacks.update)(iac, &ts_orig.values);
    }

    /// Merges all per-worker maps and finalizes each group.
    /// Port of Go `incrementalAggrFuncContext.finalizeTimeseries`.
    pub fn finalize_timeseries(&self) -> Vec<Timeseries> {
        let mut m_global: AggrContextMap = HashMap::new();
        for worker_map in &self.by_worker_id {
            let m = std::mem::take(&mut *worker_map.lock());
            for (k, iac) in m {
                match m_global.get_mut(&k) {
                    Some(iac_global) => (self.callbacks.merge)(iac_global, &iac),
                    None => {
                        if self.ae.limit > 0 && m_global.len() >= self.ae.limit as usize {
                            // Skip this series: limit reached.
                            continue;
                        }
                        m_global.insert(k, iac);
                    }
                }
            }
        }
        let mut tss = Vec::with_capacity(m_global.len());
        for (_, mut iac) in m_global {
            (self.callbacks.finalize)(&mut iac);
            tss.push(iac.ts);
        }
        tss
    }
}

fn finalize_aggr_common(iac: &mut IncrementalAggrContext) {
    for (i, &count) in iac.values.iter().enumerate() {
        if count == 0.0 {
            iac.ts.values[i] = f64::NAN;
        }
    }
}

fn update_aggr_sum(iac: &mut IncrementalAggrContext, values: &[f64]) {
    for (i, &v) in values.iter().enumerate() {
        if v.is_nan() {
            continue;
        }
        if iac.values[i] == 0.0 {
            iac.ts.values[i] = v;
            iac.values[i] = 1.0;
            continue;
        }
        iac.ts.values[i] += v;
    }
}

fn merge_aggr_sum(dst: &mut IncrementalAggrContext, src: &IncrementalAggrContext) {
    for (i, &v) in src.ts.values.iter().enumerate() {
        if src.values[i] == 0.0 {
            continue;
        }
        if dst.values[i] == 0.0 {
            dst.ts.values[i] = v;
            dst.values[i] = 1.0;
            continue;
        }
        dst.ts.values[i] += v;
    }
}

fn update_aggr_min(iac: &mut IncrementalAggrContext, values: &[f64]) {
    for (i, &v) in values.iter().enumerate() {
        if v.is_nan() {
            continue;
        }
        if iac.values[i] == 0.0 {
            iac.ts.values[i] = v;
            iac.values[i] = 1.0;
            continue;
        }
        if v < iac.ts.values[i] {
            iac.ts.values[i] = v;
        }
    }
}

fn merge_aggr_min(dst: &mut IncrementalAggrContext, src: &IncrementalAggrContext) {
    for (i, &v) in src.ts.values.iter().enumerate() {
        if src.values[i] == 0.0 {
            continue;
        }
        if dst.values[i] == 0.0 {
            dst.ts.values[i] = v;
            dst.values[i] = 1.0;
            continue;
        }
        if v < dst.ts.values[i] {
            dst.ts.values[i] = v;
        }
    }
}

fn update_aggr_max(iac: &mut IncrementalAggrContext, values: &[f64]) {
    for (i, &v) in values.iter().enumerate() {
        if v.is_nan() {
            continue;
        }
        if iac.values[i] == 0.0 {
            iac.ts.values[i] = v;
            iac.values[i] = 1.0;
            continue;
        }
        if v > iac.ts.values[i] {
            iac.ts.values[i] = v;
        }
    }
}

fn merge_aggr_max(dst: &mut IncrementalAggrContext, src: &IncrementalAggrContext) {
    for (i, &v) in src.ts.values.iter().enumerate() {
        if src.values[i] == 0.0 {
            continue;
        }
        if dst.values[i] == 0.0 {
            dst.ts.values[i] = v;
            dst.values[i] = 1.0;
            continue;
        }
        if v > dst.ts.values[i] {
            dst.ts.values[i] = v;
        }
    }
}

fn update_aggr_avg(iac: &mut IncrementalAggrContext, values: &[f64]) {
    // Do not use `Rapid calculation methods`, since they are slower and have
    // no obvious benefits in increased precision.
    for (i, &v) in values.iter().enumerate() {
        if v.is_nan() {
            continue;
        }
        if iac.values[i] == 0.0 {
            iac.ts.values[i] = v;
            iac.values[i] = 1.0;
            continue;
        }
        iac.ts.values[i] += v;
        iac.values[i] += 1.0;
    }
}

fn merge_aggr_avg(dst: &mut IncrementalAggrContext, src: &IncrementalAggrContext) {
    for (i, &v) in src.ts.values.iter().enumerate() {
        if src.values[i] == 0.0 {
            continue;
        }
        if dst.values[i] == 0.0 {
            dst.ts.values[i] = v;
            dst.values[i] = src.values[i];
            continue;
        }
        dst.ts.values[i] += v;
        dst.values[i] += src.values[i];
    }
}

fn finalize_aggr_avg(iac: &mut IncrementalAggrContext) {
    for (i, &count) in iac.values.iter().enumerate() {
        if count == 0.0 {
            iac.ts.values[i] = f64::NAN;
            continue;
        }
        iac.ts.values[i] /= count;
    }
}

fn update_aggr_count(iac: &mut IncrementalAggrContext, values: &[f64]) {
    for (i, &v) in values.iter().enumerate() {
        if v.is_nan() {
            continue;
        }
        iac.ts.values[i] += 1.0;
    }
}

fn merge_aggr_count(dst: &mut IncrementalAggrContext, src: &IncrementalAggrContext) {
    for (i, &v) in src.ts.values.iter().enumerate() {
        dst.ts.values[i] += v;
    }
}

fn finalize_aggr_count(iac: &mut IncrementalAggrContext) {
    for v in iac.ts.values.iter_mut() {
        if *v == 0.0 {
            *v = f64::NAN;
        }
    }
}

fn finalize_aggr_group(iac: &mut IncrementalAggrContext) {
    for v in iac.ts.values.iter_mut() {
        if *v == 0.0 {
            *v = f64::NAN;
        } else {
            *v = 1.0;
        }
    }
}

fn update_aggr_sum2(iac: &mut IncrementalAggrContext, values: &[f64]) {
    for (i, &v) in values.iter().enumerate() {
        if v.is_nan() {
            continue;
        }
        if iac.values[i] == 0.0 {
            iac.ts.values[i] = v * v;
            iac.values[i] = 1.0;
            continue;
        }
        iac.ts.values[i] += v * v;
    }
}

fn merge_aggr_sum2(dst: &mut IncrementalAggrContext, src: &IncrementalAggrContext) {
    for (i, &v) in src.ts.values.iter().enumerate() {
        if src.values[i] == 0.0 {
            continue;
        }
        if dst.values[i] == 0.0 {
            dst.ts.values[i] = v;
            dst.values[i] = 1.0;
            continue;
        }
        dst.ts.values[i] += v;
    }
}

fn update_aggr_geomean(iac: &mut IncrementalAggrContext, values: &[f64]) {
    for (i, &v) in values.iter().enumerate() {
        if v.is_nan() {
            continue;
        }
        if iac.values[i] == 0.0 {
            iac.ts.values[i] = v;
            iac.values[i] = 1.0;
            continue;
        }
        iac.ts.values[i] *= v;
        iac.values[i] += 1.0;
    }
}

fn merge_aggr_geomean(dst: &mut IncrementalAggrContext, src: &IncrementalAggrContext) {
    for (i, &v) in src.ts.values.iter().enumerate() {
        if src.values[i] == 0.0 {
            continue;
        }
        if dst.values[i] == 0.0 {
            dst.ts.values[i] = v;
            dst.values[i] = src.values[i];
            continue;
        }
        dst.ts.values[i] *= v;
        dst.values[i] += src.values[i];
    }
}

fn finalize_aggr_geomean(iac: &mut IncrementalAggrContext) {
    for (i, &count) in iac.values.iter().enumerate() {
        if count == 0.0 {
            iac.ts.values[i] = f64::NAN;
            continue;
        }
        iac.ts.values[i] = iac.ts.values[i].powf(1.0 / count);
    }
}

fn update_aggr_any(iac: &mut IncrementalAggrContext, values: &[f64]) {
    if iac.values[0] > 0.0 {
        return;
    }
    for i in 0..values.len() {
        iac.values[i] = 1.0;
    }
    iac.ts.values.clear();
    iac.ts.values.extend_from_slice(values);
}

fn merge_aggr_any(dst: &mut IncrementalAggrContext, src: &IncrementalAggrContext) {
    if dst.values[0] > 0.0 {
        return;
    }
    dst.values[0] = src.values[0];
    dst.ts.values.clear();
    dst.ts.values.extend_from_slice(&src.ts.values);
}
