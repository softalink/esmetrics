//! Function name tables for aggregate, rollup and transform functions.
//!
//! Port of `aggr.go`, `rollup.go` and `transform.go`.

use crate::ast::FuncExpr;

/// Aggregate function names, from `aggr.go`.
static AGGR_FUNCS: &[&str] = &[
    "any",
    "avg",
    "bottomk",
    "bottomk_avg",
    "bottomk_max",
    "bottomk_median",
    "bottomk_last",
    "bottomk_min",
    "count",
    "count_values",
    "distinct",
    "geomean",
    "group",
    "histogram",
    "limitk",
    "mad",
    "max",
    "median",
    "min",
    "mode",
    "outliers_iqr",
    "outliers_mad",
    "outliersk",
    "quantile",
    "quantiles",
    "share",
    "stddev",
    "stdvar",
    "sum",
    "sum2",
    "topk",
    "topk_avg",
    "topk_max",
    "topk_median",
    "topk_last",
    "topk_min",
    "zscore",
];

/// Rollup function names, from `rollup.go`.
static ROLLUP_FUNCS: &[&str] = &[
    "absent_over_time",
    "aggr_over_time",
    "ascent_over_time",
    "avg_over_time",
    "changes",
    "changes_prometheus",
    "count_eq_over_time",
    "count_gt_over_time",
    "count_le_over_time",
    "count_ne_over_time",
    "count_over_time",
    "count_values_over_time",
    "decreases_over_time",
    "default_rollup",
    "delta",
    "delta_prometheus",
    "deriv",
    "deriv_fast",
    "descent_over_time",
    "distinct_over_time",
    "duration_over_time",
    "first_over_time",
    "geomean_over_time",
    "histogram_over_time",
    "hoeffding_bound_lower",
    "hoeffding_bound_upper",
    "holt_winters",
    "idelta",
    "ideriv",
    "increase",
    "increase_prometheus",
    "increase_pure",
    "increases_over_time",
    "integrate",
    "irate",
    "lag",
    "last_over_time",
    "lifetime",
    "mad_over_time",
    "max_over_time",
    "median_over_time",
    "min_over_time",
    "mode_over_time",
    "outlier_iqr_over_time",
    "predict_linear",
    "present_over_time",
    "quantile_over_time",
    "quantiles_over_time",
    "range_over_time",
    "rate",
    "rate_prometheus",
    "rate_over_sum",
    "resets",
    "rollup",
    "rollup_candlestick",
    "rollup_delta",
    "rollup_deriv",
    "rollup_increase",
    "rollup_rate",
    "rollup_scrape_interval",
    "scrape_interval",
    "share_gt_over_time",
    "share_le_over_time",
    "share_eq_over_time",
    "stale_samples_over_time",
    "stddev_over_time",
    "stdvar_over_time",
    "sum_eq_over_time",
    "sum_gt_over_time",
    "sum_le_over_time",
    "sum_over_time",
    "sum2_over_time",
    "tfirst_over_time",
    // `timestamp` must return the timestamp of the last datapoint in the
    // current window in order to properly handle offsets and timestamps
    // unaligned to the current step.
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/415
    "timestamp",
    "timestamp_with_name",
    "tlast_change_over_time",
    "tlast_over_time",
    "tmax_over_time",
    "tmin_over_time",
    "zscore_over_time",
];

/// Transform function names, from `transform.go`.
static TRANSFORM_FUNCS: &[&str] = &[
    "", // an empty func is a synonym to union
    "abs",
    "absent",
    "acos",
    "acosh",
    "asin",
    "asinh",
    "atan",
    "atanh",
    "bitmap_and",
    "bitmap_or",
    "bitmap_xor",
    "buckets_limit",
    "ceil",
    "clamp",
    "clamp_max",
    "clamp_min",
    "cos",
    "cosh",
    "day_of_month",
    "day_of_week",
    "day_of_year",
    "days_in_month",
    "deg",
    "drop_common_labels",
    "drop_empty_series",
    "end",
    "exp",
    "floor",
    "histogram_avg",
    "histogram_fraction",
    "histogram_quantile",
    "histogram_quantiles",
    "histogram_share",
    "histogram_stddev",
    "histogram_stdvar",
    "hour",
    "interpolate",
    "keep_last_value",
    "keep_next_value",
    "label_copy",
    "label_del",
    "label_graphite_group",
    "label_join",
    "label_keep",
    "label_lowercase",
    "label_map",
    "label_match",
    "label_mismatch",
    "label_move",
    "label_replace",
    "label_set",
    "label_transform",
    "label_uppercase",
    "label_value",
    "labels_equal",
    "limit_offset",
    "ln",
    "log2",
    "log10",
    "minute",
    "month",
    "now",
    "pi",
    "prometheus_buckets",
    "rad",
    "rand",
    "rand_exponential",
    "rand_normal",
    "range_avg",
    "range_first",
    "range_last",
    "range_linear_regression",
    "range_mad",
    "range_max",
    "range_min",
    "range_normalize",
    "range_quantile",
    "range_stddev",
    "range_stdvar",
    "range_sum",
    "range_trim_outliers",
    "range_trim_spikes",
    "range_trim_zscore",
    "range_zscore",
    "remove_resets",
    "round",
    "running_avg",
    "running_max",
    "running_min",
    "running_sum",
    "scalar",
    "sgn",
    "sin",
    "sinh",
    "smooth_exponential",
    "sort",
    "sort_by_label",
    "sort_by_label_desc",
    "sort_by_label_numeric",
    "sort_by_label_numeric_desc",
    "sort_desc",
    "sqrt",
    "start",
    "step",
    "tan",
    "tanh",
    "time",
    // "timestamp" has been moved to rollup funcs.
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/415
    "timezone_offset",
    "union",
    "vector",
    "year",
];

/// Returns whether `s` is a known aggregate function (case-insensitive).
///
/// Port of Go `IsAggrFunc`.
pub fn is_aggr_func(s: &str) -> bool {
    let s = s.to_ascii_lowercase();
    AGGR_FUNCS.contains(&s.as_str())
}

/// Port of Go `isAggrFuncModifier` (`by` / `without`).
pub(crate) fn is_aggr_func_modifier(s: &str) -> bool {
    matches!(s.to_ascii_lowercase().as_str(), "by" | "without")
}

/// Returns whether `func_name` is a known rollup function (case-insensitive).
///
/// Port of Go `IsRollupFunc`.
pub fn is_rollup_func(func_name: &str) -> bool {
    let s = func_name.to_ascii_lowercase();
    ROLLUP_FUNCS.contains(&s.as_str())
}

/// Returns whether `func_name` is a known transform function
/// (case-insensitive).
///
/// Port of Go `IsTransformFunc`.
pub fn is_transform_func(func_name: &str) -> bool {
    let s = func_name.to_ascii_lowercase();
    TRANSFORM_FUNCS.contains(&s.as_str())
}

/// Returns the argument index for the given `fe`, which accepts the rollup
/// argument. `None` is returned if `fe` isn't a rollup function.
///
/// Port of Go `GetRollupArgIdx` (which returns -1 instead of `None`).
pub fn get_rollup_arg_idx(fe: &FuncExpr) -> Option<usize> {
    let func_name = fe.name.to_ascii_lowercase();
    if !ROLLUP_FUNCS.contains(&func_name.as_str()) {
        return None;
    }
    match func_name.as_str() {
        "quantile_over_time"
        | "aggr_over_time"
        | "count_values_over_time"
        | "hoeffding_bound_lower"
        | "hoeffding_bound_upper" => Some(1),
        "quantiles_over_time" => fe.args.len().checked_sub(1),
        _ => Some(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Port of TestIsAggrFuncModifierSuccess/Error from aggr_test.go.
    #[test]
    fn aggr_func_modifier_cases() {
        for s in ["by", "BY", "without", "Without"] {
            assert!(
                is_aggr_func_modifier(s),
                "expecting valid funcModifier: {s:?}"
            );
        }
        for s in ["byfix", "on", "ignoring"] {
            assert!(
                !is_aggr_func_modifier(s),
                "unexpected valid funcModifier: {s:?}"
            );
        }
    }

    #[test]
    fn func_tables() {
        assert!(is_aggr_func("sum"));
        assert!(is_aggr_func("aVG"));
        assert!(is_rollup_func("rate"));
        assert!(is_rollup_func("RATE"));
        assert!(is_transform_func("ceil"));
        assert!(is_transform_func(""));
        assert!(!is_aggr_func("rate"));
        assert!(!is_rollup_func("sum"));
        assert!(!is_transform_func("foo"));
    }
}
