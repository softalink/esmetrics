//! Query optimizer: pushdown of common label filters into both sides of
//! binary operations.
//!
//! Port of `optimizer.go`.

use crate::ast::{AggrFuncExpr, BinaryOpExpr, Expr, LabelFilter};
use crate::funcs::{is_aggr_func, is_rollup_func, is_transform_func};
use crate::parser::parse;
use std::collections::HashSet;

/// Optimizes `e` in order to improve its performance.
///
/// It adds missing filters to `foo{filters1} op bar{filters2}`, converting
/// such queries to `foo{filters1, filters2} op bar{filters1, filters2}`.
/// See <https://utcc.utoronto.ca/~cks/space/blog/sysadmin/PrometheusLabelNonOptimization>
///
/// Port of Go `Optimize`.
pub fn optimize(e: &Expr) -> Expr {
    if !can_optimize(e) {
        return e.clone();
    }
    let mut e_copy = clone_expr(e);
    optimize_inplace(&mut e_copy);
    e_copy
}

/// Port of Go `canOptimize`.
fn can_optimize(e: &Expr) -> bool {
    match e {
        Expr::Rollup(re) => can_optimize(&re.expr) || re.at.as_deref().is_some_and(can_optimize),
        Expr::Func(fe) => fe.args.iter().any(can_optimize),
        Expr::Aggr(ae) => ae.args.iter().any(can_optimize),
        Expr::BinaryOp(_) => true,
        _ => false,
    }
}

/// Clones the given expression. Port of Go `Clone` (which re-parses the
/// serialized expression).
fn clone_expr(e: &Expr) -> Expr {
    let s = e.to_string();
    parse(&s).unwrap_or_else(|err| panic!("BUG: cannot parse the expression {s:?}: {err}"))
}

/// Port of Go `optimizeInplace`.
fn optimize_inplace(e: &mut Expr) {
    match e {
        Expr::Rollup(re) => {
            optimize_inplace(&mut re.expr);
            if let Some(at) = &mut re.at {
                optimize_inplace(at);
            }
        }
        Expr::Func(fe) => {
            for arg in &mut fe.args {
                optimize_inplace(arg);
            }
        }
        Expr::Aggr(ae) => {
            for arg in &mut ae.args {
                optimize_inplace(arg);
            }
        }
        Expr::BinaryOp(be) => {
            optimize_inplace(&mut be.left);
            optimize_inplace(&mut be.right);
            let lfs = get_common_label_filters_bin(be);
            pushdown_into_binary_op(&lfs, be);
        }
        _ => {}
    }
}

/// Port of Go `getCommonLabelFilters`.
pub(crate) fn get_common_label_filters(e: &Expr) -> Vec<LabelFilter> {
    match e {
        Expr::Metric(me) => get_common_label_filters_without_metric_name(&me.label_filterss),
        Expr::Rollup(re) => get_common_label_filters(&re.expr),
        Expr::Func(fe) => {
            let args = &fe.args;
            match fe.name.to_ascii_lowercase().as_str() {
                "label_set" => get_common_label_filters_for_label_set(args),
                "label_replace" | "label_join" | "label_map" | "label_match" | "label_mismatch"
                | "label_transform" => get_common_label_filters_for_label_replace(args),
                "label_copy" | "label_move" => get_common_label_filters_for_label_copy(args),
                "label_del" | "label_uppercase" | "label_lowercase" | "labels_equal" => {
                    get_common_label_filters_for_label_del(args)
                }
                "label_keep" => get_common_label_filters_for_label_keep(args),
                "count_values_over_time" => {
                    get_common_label_filters_for_count_values_over_time(args)
                }
                "range_normalize" | "union" | "" => intersect_label_filters_for_all_args(args),
                _ => match get_func_arg_for_optimization(&fe.name, args) {
                    None => Vec::new(),
                    Some(arg) => get_common_label_filters(arg),
                },
            }
        }
        Expr::Aggr(ae) => {
            let args = &ae.args;
            if ae.name.eq_ignore_ascii_case("count_values") {
                if args.len() != 2 {
                    return Vec::new();
                }
                let lfs = get_common_label_filters(&args[1]);
                let lfs = drop_label_filters_for_label_name(lfs, &args[0]);
                return trim_filters_by_aggr_modifier(lfs, ae);
            }
            if can_accept_multiple_args_for_aggr_func(&ae.name) {
                let lfs = intersect_label_filters_for_all_args(args);
                return trim_filters_by_aggr_modifier(lfs, ae);
            }
            match get_func_arg_for_optimization(&ae.name, args) {
                None => Vec::new(),
                Some(arg) => {
                    let lfs = get_common_label_filters(arg);
                    trim_filters_by_aggr_modifier(lfs, ae)
                }
            }
        }
        Expr::BinaryOp(be) => get_common_label_filters_bin(be),
        _ => Vec::new(),
    }
}

/// The `*BinaryOpExpr` case of Go `getCommonLabelFilters`.
fn get_common_label_filters_bin(be: &BinaryOpExpr) -> Vec<LabelFilter> {
    let lfs_left = get_common_label_filters(&be.left);
    let lfs_right = get_common_label_filters(&be.right);
    match be.op.to_ascii_lowercase().as_str() {
        "or" => {
            // {fCommon, f1} or {fCommon, f2} -> {fCommon}
            // {fCommon, f1} or on() {fCommon, f2} -> {}
            // {fCommon, f1} or on(fCommon) {fCommon, f2} -> {fCommon}
            // {fCommon, f1} or on(f1) {fCommon, f2} -> {}
            let lfs = intersect_label_filters(&lfs_left, &lfs_right);
            trim_filters_by_group_modifier(lfs, be)
        }
        "unless" => {
            // {f1} unless {f2} -> {f1}
            // {f1} unless on() {f2} -> {}
            // {f1} unless on(f1) {f2} -> {f1}
            trim_filters_by_group_modifier(lfs_left, be)
        }
        "ifnot" => {
            // Remove right from left, so filters in left can be pushed down
            // to right. {f1} ifnot `any` -> {f1}
            // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8435
            trim_filters_by_group_modifier(lfs_left, be)
        }
        _ => match be.join_modifier.op.to_ascii_lowercase().as_str() {
            "group_left" => {
                // {f1} * group_left() {f2} -> {f1, f2}
                // {f1} * on() group_left() {f2} -> {f1}
                // {f1} * on(f1) group_left() {f2} -> {f1}
                // {f1} * on(f2) group_left() {f2} -> {f1, f2}
                let lfs_right = trim_filters_by_group_modifier(lfs_right, be);
                union_label_filters(&lfs_left, &lfs_right)
            }
            "group_right" => {
                // {f1} * group_right() {f2} -> {f1, f2}
                // {f1} * on() group_right() {f2} -> {f2}
                // {f1} * on(f1) group_right() {f2} -> {f1, f2}
                let lfs_left = trim_filters_by_group_modifier(lfs_left, be);
                union_label_filters(&lfs_left, &lfs_right)
            }
            _ => {
                // {f1} * {f2} -> {f1, f2}
                // {f1} * on() {f2} -> {}
                // {f1} * on(f1) {f2} -> {f1}
                let lfs = union_label_filters(&lfs_left, &lfs_right);
                trim_filters_by_group_modifier(lfs, be)
            }
        },
    }
}

/// Port of Go `intersectLabelFiltersForAllArgs`.
fn intersect_label_filters_for_all_args(args: &[Expr]) -> Vec<LabelFilter> {
    let Some((first, rest)) = args.split_first() else {
        return Vec::new();
    };
    let mut lfs = get_common_label_filters(first);
    for arg in rest {
        let lfs_next = get_common_label_filters(arg);
        lfs = intersect_label_filters(&lfs, &lfs_next);
    }
    lfs
}

/// Port of Go `getCommonLabelFiltersForCountValuesOverTime`.
fn get_common_label_filters_for_count_values_over_time(args: &[Expr]) -> Vec<LabelFilter> {
    if args.len() != 2 {
        return Vec::new();
    }
    let lfs = get_common_label_filters(&args[1]);
    drop_label_filters_for_label_name(lfs, &args[0])
}

/// Port of Go `getCommonLabelFiltersForLabelKeep`.
fn get_common_label_filters_for_label_keep(args: &[Expr]) -> Vec<LabelFilter> {
    let Some((first, rest)) = args.split_first() else {
        return Vec::new();
    };
    let lfs = get_common_label_filters(first);
    keep_label_filters_for_label_names(lfs, rest)
}

/// Port of Go `getCommonLabelFiltersForLabelDel`.
fn get_common_label_filters_for_label_del(args: &[Expr]) -> Vec<LabelFilter> {
    let Some((first, rest)) = args.split_first() else {
        return Vec::new();
    };
    let lfs = get_common_label_filters(first);
    drop_label_filters_for_label_names(lfs, rest)
}

/// Port of Go `getCommonLabelFiltersForLabelCopy`.
fn get_common_label_filters_for_label_copy(args: &[Expr]) -> Vec<LabelFilter> {
    let Some((first, rest)) = args.split_first() else {
        return Vec::new();
    };
    let lfs = get_common_label_filters(first);
    let mut label_names: Vec<&Expr> = Vec::new();
    let mut i = 0;
    while i < rest.len() {
        if i + 1 >= rest.len() {
            return Vec::new();
        }
        label_names.push(&rest[i + 1]);
        i += 2;
    }
    drop_label_filters_for_label_name_refs(lfs, &label_names)
}

/// Port of Go `getCommonLabelFiltersForLabelReplace`.
fn get_common_label_filters_for_label_replace(args: &[Expr]) -> Vec<LabelFilter> {
    if args.len() < 2 {
        return Vec::new();
    }
    let lfs = get_common_label_filters(&args[0]);
    drop_label_filters_for_label_name(lfs, &args[1])
}

/// Port of Go `getCommonLabelFiltersForLabelSet`.
fn get_common_label_filters_for_label_set(args: &[Expr]) -> Vec<LabelFilter> {
    let Some((first, rest)) = args.split_first() else {
        return Vec::new();
    };
    let mut lfs = get_common_label_filters(first);
    let mut i = 0;
    while i < rest.len() {
        let label_name = &rest[i];
        if i + 1 >= rest.len() {
            return Vec::new();
        }
        let label_value = &rest[i + 1];

        let Expr::String(se_label_name) = label_name else {
            return Vec::new();
        };
        let Expr::String(se_label_value) = label_value else {
            return Vec::new();
        };

        if se_label_name.s != "__name__" {
            lfs = drop_label_filters_for_label_name(lfs, label_name);
            lfs.push(LabelFilter {
                label: se_label_name.s.clone(),
                value: se_label_value.s.clone(),
                is_negative: false,
                is_regexp: false,
            });
        }
        i += 2;
    }
    lfs
}

/// Port of Go `trimFiltersByAggrModifier`.
fn trim_filters_by_aggr_modifier(lfs: Vec<LabelFilter>, afe: &AggrFuncExpr) -> Vec<LabelFilter> {
    match afe.modifier.op.to_ascii_lowercase().as_str() {
        "by" => filter_label_filters_on(lfs, &afe.modifier.args),
        "without" => filter_label_filters_ignoring(lfs, &afe.modifier.args),
        _ => Vec::new(),
    }
}

/// Trims `lfs` by the given `be.group_modifier.op` (i.e. `on()` or
/// `ignoring()`):
/// - returns `lfs` as is if `be` doesn't contain a group modifier;
/// - returns only the filters specified in `on()`;
/// - drops the filters specified inside `ignoring()`.
///
/// Port of Go `TrimFiltersByGroupModifier`.
pub fn trim_filters_by_group_modifier(
    lfs: Vec<LabelFilter>,
    be: &BinaryOpExpr,
) -> Vec<LabelFilter> {
    match be.group_modifier.op.to_ascii_lowercase().as_str() {
        "on" => filter_label_filters_on(lfs, &be.group_modifier.args),
        "ignoring" => filter_label_filters_ignoring(lfs, &be.group_modifier.args),
        _ => lfs,
    }
}

/// Port of Go `getCommonLabelFiltersWithoutMetricName`.
fn get_common_label_filters_without_metric_name(lfss: &[Vec<LabelFilter>]) -> Vec<LabelFilter> {
    let Some((first, rest)) = lfss.split_first() else {
        return Vec::new();
    };
    let mut lfs_a = get_label_filters_without_metric_name(first);
    for lfs in rest {
        if lfs_a.is_empty() {
            return Vec::new();
        }
        let lfs_b = get_label_filters_without_metric_name(lfs);
        lfs_a = intersect_label_filters(&lfs_a, &lfs_b);
    }
    lfs_a
}

/// Port of Go `getLabelFiltersWithoutMetricName`.
fn get_label_filters_without_metric_name(lfs: &[LabelFilter]) -> Vec<LabelFilter> {
    lfs.iter()
        .filter(|lf| lf.label != "__name__")
        .cloned()
        .collect()
}

/// Pushes down the given `common_filters` to `e` if possible.
///
/// `e` must be a part of a binary operation - either left or right.
/// For example, if `e` contains `foo + sum(bar)` and
/// `common_filters={x="y"}`, the returned expression contains
/// `foo{x="y"} + sum(bar)`. The `{x="y"}` cannot be pushed down to
/// `sum(bar)`, since this may change binary operation results.
///
/// Port of Go `PushdownBinaryOpFilters`.
pub fn pushdown_binary_op_filters(e: &Expr, common_filters: &[LabelFilter]) -> Expr {
    if common_filters.is_empty() {
        // Fast path - nothing to push down.
        return e.clone();
    }
    let mut e_copy = clone_expr(e);
    pushdown_binary_op_filters_inplace(common_filters, &mut e_copy);
    e_copy
}

/// Port of Go `pushdownBinaryOpFiltersInplace`.
fn pushdown_binary_op_filters_inplace(lfs: &[LabelFilter], e: &mut Expr) {
    if lfs.is_empty() {
        return;
    }
    match e {
        Expr::Metric(me) => {
            for lfs_local in &mut me.label_filterss {
                let mut merged = union_label_filters(lfs_local, lfs);
                sort_label_filters(&mut merged);
                *lfs_local = merged;
            }
        }
        Expr::Rollup(re) => {
            pushdown_binary_op_filters_inplace(lfs, &mut re.expr);
        }
        Expr::Func(fe) => {
            let args = &mut fe.args;
            match fe.name.to_ascii_lowercase().as_str() {
                "label_set" => pushdown_label_filters_for_label_set(lfs, args),
                "label_replace" | "label_join" | "label_map" | "label_match" | "label_mismatch"
                | "label_transform" => {
                    pushdown_label_filters_for_label_replace(lfs, args);
                }
                "label_copy" | "label_move" => pushdown_label_filters_for_label_copy(lfs, args),
                "label_del" | "label_uppercase" | "label_lowercase" | "labels_equal" => {
                    pushdown_label_filters_for_label_del(lfs, args);
                }
                "label_keep" => pushdown_label_filters_for_label_keep(lfs, args),
                "count_values_over_time" => {
                    pushdown_label_filters_for_count_values_over_time(lfs, args);
                }
                "range_normalize" | "union" | "" => {
                    pushdown_label_filters_for_all_args(lfs, args);
                }
                _ => {
                    if let Some(idx) = get_func_arg_idx_for_optimization(&fe.name, args) {
                        if idx < args.len() {
                            pushdown_binary_op_filters_inplace(lfs, &mut args[idx]);
                        }
                    }
                }
            }
        }
        Expr::Aggr(ae) => {
            let lfs = trim_filters_by_aggr_modifier(lfs.to_vec(), ae);
            let args = &mut ae.args;
            if ae.name.eq_ignore_ascii_case("count_values") {
                if args.len() == 2 {
                    let (head, tail) = args.split_at_mut(1);
                    let lfs = drop_label_filters_for_label_name(lfs, &head[0]);
                    pushdown_binary_op_filters_inplace(&lfs, &mut tail[0]);
                }
            } else if can_accept_multiple_args_for_aggr_func(&ae.name) {
                pushdown_label_filters_for_all_args(&lfs, args);
            } else if let Some(idx) = get_func_arg_idx_for_optimization(&ae.name, args) {
                if idx < args.len() {
                    pushdown_binary_op_filters_inplace(&lfs, &mut args[idx]);
                }
            }
        }
        Expr::BinaryOp(be) => {
            pushdown_into_binary_op(lfs, be);
        }
        _ => {}
    }
}

/// The `*BinaryOpExpr` case of Go `pushdownBinaryOpFiltersInplace`.
fn pushdown_into_binary_op(lfs: &[LabelFilter], be: &mut BinaryOpExpr) {
    if lfs.is_empty() {
        return;
    }
    let lfs = trim_filters_by_group_modifier(lfs.to_vec(), be);
    pushdown_binary_op_filters_inplace(&lfs, &mut be.left);
    pushdown_binary_op_filters_inplace(&lfs, &mut be.right);
}

/// Port of Go `pushdownLabelFiltersForAllArgs`.
fn pushdown_label_filters_for_all_args(lfs: &[LabelFilter], args: &mut [Expr]) {
    for arg in args {
        pushdown_binary_op_filters_inplace(lfs, arg);
    }
}

/// Port of Go `pushdownLabelFiltersForCountValuesOverTime`.
fn pushdown_label_filters_for_count_values_over_time(lfs: &[LabelFilter], args: &mut [Expr]) {
    if args.len() != 2 {
        return;
    }
    let (head, tail) = args.split_at_mut(1);
    let lfs = drop_label_filters_for_label_name(lfs.to_vec(), &head[0]);
    pushdown_binary_op_filters_inplace(&lfs, &mut tail[0]);
}

/// Port of Go `pushdownLabelFiltersForLabelKeep`.
fn pushdown_label_filters_for_label_keep(lfs: &[LabelFilter], args: &mut [Expr]) {
    if args.is_empty() {
        return;
    }
    let (head, tail) = args.split_at_mut(1);
    let lfs = keep_label_filters_for_label_names(lfs.to_vec(), tail);
    pushdown_binary_op_filters_inplace(&lfs, &mut head[0]);
}

/// Port of Go `pushdownLabelFiltersForLabelDel`.
fn pushdown_label_filters_for_label_del(lfs: &[LabelFilter], args: &mut [Expr]) {
    if args.is_empty() {
        return;
    }
    let (head, tail) = args.split_at_mut(1);
    let lfs = drop_label_filters_for_label_names(lfs.to_vec(), tail);
    pushdown_binary_op_filters_inplace(&lfs, &mut head[0]);
}

/// Port of Go `pushdownLabelFiltersForLabelCopy`.
fn pushdown_label_filters_for_label_copy(lfs: &[LabelFilter], args: &mut [Expr]) {
    if args.is_empty() {
        return;
    }
    let (head, tail) = args.split_at_mut(1);
    let mut label_names: Vec<&Expr> = Vec::new();
    let mut i = 0;
    while i < tail.len() {
        if i + 1 >= tail.len() {
            return;
        }
        label_names.push(&tail[i + 1]);
        i += 2;
    }
    let lfs = drop_label_filters_for_label_name_refs(lfs.to_vec(), &label_names);
    pushdown_binary_op_filters_inplace(&lfs, &mut head[0]);
}

/// Port of Go `pushdownLabelFiltersForLabelReplace`.
fn pushdown_label_filters_for_label_replace(lfs: &[LabelFilter], args: &mut [Expr]) {
    if args.len() < 2 {
        return;
    }
    let (head, tail) = args.split_at_mut(1);
    let lfs = drop_label_filters_for_label_name(lfs.to_vec(), &tail[0]);
    pushdown_binary_op_filters_inplace(&lfs, &mut head[0]);
}

/// Port of Go `pushdownLabelFiltersForLabelSet`.
fn pushdown_label_filters_for_label_set(lfs: &[LabelFilter], args: &mut [Expr]) {
    if args.is_empty() {
        return;
    }
    let (head, tail) = args.split_at_mut(1);
    let mut label_names: Vec<&Expr> = Vec::new();
    let mut i = 0;
    while i < tail.len() {
        label_names.push(&tail[i]);
        i += 2;
    }
    let lfs = drop_label_filters_for_label_name_refs(lfs.to_vec(), &label_names);
    pushdown_binary_op_filters_inplace(&lfs, &mut head[0]);
}

/// Port of Go `intersectLabelFilters`.
fn intersect_label_filters(lfs_a: &[LabelFilter], lfs_b: &[LabelFilter]) -> Vec<LabelFilter> {
    if lfs_a.is_empty() || lfs_b.is_empty() {
        return Vec::new();
    }
    let m = get_label_filters_map(lfs_a);
    let mut lfs: Vec<LabelFilter> = Vec::new();
    for lf in lfs_b {
        let mut b = String::new();
        lf.append_string(&mut b);
        if m.contains(&b) {
            lfs.push(lf.clone());
        }
    }
    lfs
}

/// Port of Go `keepLabelFiltersForLabelNames`.
fn keep_label_filters_for_label_names(
    lfs: Vec<LabelFilter>,
    label_names: &[Expr],
) -> Vec<LabelFilter> {
    let mut m: HashSet<&str> = HashSet::with_capacity(label_names.len());
    for label_name in label_names {
        let Expr::String(se) = label_name else {
            return Vec::new();
        };
        m.insert(&se.s);
    }
    lfs.into_iter()
        .filter(|lf| m.contains(lf.label.as_str()))
        .collect()
}

/// Port of Go `dropLabelFiltersForLabelNames`.
fn drop_label_filters_for_label_names(
    mut lfs: Vec<LabelFilter>,
    label_names: &[Expr],
) -> Vec<LabelFilter> {
    for label_name in label_names {
        lfs = drop_label_filters_for_label_name(lfs, label_name);
    }
    lfs
}

/// Like [`drop_label_filters_for_label_names`] but for collected references.
fn drop_label_filters_for_label_name_refs(
    mut lfs: Vec<LabelFilter>,
    label_names: &[&Expr],
) -> Vec<LabelFilter> {
    for label_name in label_names {
        lfs = drop_label_filters_for_label_name(lfs, label_name);
    }
    lfs
}

/// Port of Go `dropLabelFiltersForLabelName`.
fn drop_label_filters_for_label_name(lfs: Vec<LabelFilter>, label_name: &Expr) -> Vec<LabelFilter> {
    let Expr::String(se) = label_name else {
        return Vec::new();
    };
    lfs.into_iter().filter(|lf| lf.label != se.s).collect()
}

/// Port of Go `unionLabelFilters`.
fn union_label_filters(lfs_a: &[LabelFilter], lfs_b: &[LabelFilter]) -> Vec<LabelFilter> {
    if lfs_a.is_empty() {
        return lfs_b.to_vec();
    }
    if lfs_b.is_empty() {
        return lfs_a.to_vec();
    }
    let m = get_label_filters_map(lfs_a);
    let mut lfs: Vec<LabelFilter> = lfs_a.to_vec();
    for lf in lfs_b {
        let mut b = String::new();
        lf.append_string(&mut b);
        if !m.contains(&b) {
            lfs.push(lf.clone());
        }
    }
    lfs
}

/// Port of Go `getLabelFiltersMap`.
fn get_label_filters_map(lfs: &[LabelFilter]) -> HashSet<String> {
    let mut m = HashSet::with_capacity(lfs.len());
    for lf in lfs {
        let mut b = String::new();
        lf.append_string(&mut b);
        m.insert(b);
    }
    m
}

/// Port of Go `sortLabelFilters`: sorts filters by (label, value), keeping a
/// leading `__name__` filter in place.
fn sort_label_filters(lfs: &mut [LabelFilter]) {
    let start = usize::from(lfs.first().is_some_and(LabelFilter::is_metric_name_filter));
    lfs[start..].sort_by(|a, b| a.label.cmp(&b.label).then_with(|| a.value.cmp(&b.value)));
}

/// Port of Go `filterLabelFiltersOn`.
fn filter_label_filters_on(lfs: Vec<LabelFilter>, args: &[String]) -> Vec<LabelFilter> {
    if args.is_empty() {
        return Vec::new();
    }
    let m: HashSet<&str> = args.iter().map(String::as_str).collect();
    lfs.into_iter()
        .filter(|lf| m.contains(lf.label.as_str()))
        .collect()
}

/// Port of Go `filterLabelFiltersIgnoring`.
fn filter_label_filters_ignoring(lfs: Vec<LabelFilter>, args: &[String]) -> Vec<LabelFilter> {
    if args.is_empty() {
        return lfs;
    }
    let m: HashSet<&str> = args.iter().map(String::as_str).collect();
    lfs.into_iter()
        .filter(|lf| !m.contains(lf.label.as_str()))
        .collect()
}

/// Port of Go `getFuncArgForOptimization`.
fn get_func_arg_for_optimization<'a>(func_name: &str, args: &'a [Expr]) -> Option<&'a Expr> {
    let idx = get_func_arg_idx_for_optimization(func_name, args)?;
    args.get(idx)
}

/// Port of Go `getFuncArgIdxForOptimization` (returns `None` instead of -1).
fn get_func_arg_idx_for_optimization(func_name: &str, args: &[Expr]) -> Option<usize> {
    let func_name = func_name.to_ascii_lowercase();
    if is_rollup_func(&func_name) {
        return get_rollup_arg_idx_for_optimization(&func_name, args);
    }
    if is_transform_func(&func_name) {
        return get_transform_arg_idx_for_optimization(&func_name, args);
    }
    if is_aggr_func(&func_name) {
        return get_aggr_arg_idx_for_optimization(&func_name, args);
    }
    None
}

/// Port of Go `getAggrArgIdxForOptimization`.
fn get_aggr_arg_idx_for_optimization(func_name: &str, args: &[Expr]) -> Option<usize> {
    match func_name {
        "bottomk" | "bottomk_avg" | "bottomk_max" | "bottomk_median" | "bottomk_last"
        | "bottomk_min" | "limitk" | "outliers_mad" | "outliersk" | "quantile" | "topk"
        | "topk_avg" | "topk_max" | "topk_median" | "topk_last" | "topk_min" => Some(1),
        "quantiles" => args.len().checked_sub(1),
        "count_values" => panic!("BUG: count_values must be already handled"),
        _ => {
            assert!(
                !can_accept_multiple_args_for_aggr_func(func_name),
                "BUG: {func_name} must be already handled"
            );
            Some(0)
        }
    }
}

/// Port of Go `canAcceptMultipleArgsForAggrFunc`.
fn can_accept_multiple_args_for_aggr_func(func_name: &str) -> bool {
    matches!(
        func_name.to_ascii_lowercase().as_str(),
        "any"
            | "avg"
            | "count"
            | "distinct"
            | "geomean"
            | "group"
            | "histogram"
            | "mad"
            | "max"
            | "median"
            | "min"
            | "mode"
            | "share"
            | "stddev"
            | "stdvar"
            | "sum"
            | "sum2"
            | "zscore"
    )
}

/// Port of Go `getRollupArgIdxForOptimization`.
/// This must be kept in sync with `get_rollup_arg_idx`.
fn get_rollup_arg_idx_for_optimization(func_name: &str, args: &[Expr]) -> Option<usize> {
    match func_name {
        "count_values_over_time" => {
            panic!("BUG: count_values_over_time must be already handled")
        }
        "absent_over_time" => None,
        "quantile_over_time"
        | "aggr_over_time"
        | "hoeffding_bound_lower"
        | "hoeffding_bound_upper" => Some(1),
        "quantiles_over_time" => args.len().checked_sub(1),
        _ => Some(0),
    }
}

/// Port of Go `getTransformArgIdxForOptimization`.
fn get_transform_arg_idx_for_optimization(func_name: &str, args: &[Expr]) -> Option<usize> {
    match func_name {
        "label_copy" | "label_del" | "label_join" | "label_keep" | "label_lowercase"
        | "label_map" | "label_match" | "label_mismatch" | "label_move" | "label_replace"
        | "label_set" | "label_transform" | "label_uppercase" | "labels_equal"
        | "range_normalize" | "" | "union" => {
            panic!("BUG: {func_name} must be already handled")
        }
        "drop_common_labels" => None,
        "absent" | "scalar" => None,
        "end" | "now" | "pi" | "ru" | "start" | "step" | "time" => None,
        "limit_offset" | "histogram_fraction" => Some(2),
        "buckets_limit"
        | "histogram_quantile"
        | "histogram_share"
        | "range_quantile"
        | "range_trim_outliers"
        | "range_trim_spikes"
        | "range_trim_zscore" => Some(1),
        "histogram_quantiles" => args.len().checked_sub(1),
        _ => Some(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::MetricExpr;

    // Port of TestGetCommonLabelFilters.
    #[test]
    fn get_common_label_filters_cases() {
        let f = |q: &str, result_expected: &str| {
            let e = parse(q).unwrap_or_else(|err| panic!("cannot parse {q}: {err}"));
            let lfs = get_common_label_filters(&e);
            let mut me = MetricExpr::default();
            if !lfs.is_empty() {
                me.label_filterss = vec![lfs];
            }
            let mut result = String::new();
            me.append_string(&mut result);
            assert_eq!(
                result, result_expected,
                "unexpected result for get_common_label_filters({q})"
            );
        };
        f("{}", "{}");
        f("foo", "{}");
        f(r#"{__name__="foo"}"#, "{}");
        f(r#"{__name__=~"bar"}"#, "{}");
        f(r#"{__name__=~"a|b",x="y"}"#, r#"{x="y"}"#);
        f(r#"foo{c!="d",a="b"}"#, r#"{c!="d",a="b"}"#);
        f("1+foo", "{}");
        f(r#"foo + bar{a="b"}"#, r#"{a="b"}"#);
        f(r#"foo + bar / baz{a="b"}"#, r#"{a="b"}"#);
        f(r#"foo{x!="y"} + bar / baz{a="b"}"#, r#"{x!="y",a="b"}"#);
        f(
            r#"foo{x!="y"} + bar{x=~"a|b",q!~"we|rt"} / baz{a="b"}"#,
            r#"{x!="y",x=~"a|b",q!~"we|rt",a="b"}"#,
        );
        f(r#"{a="b"} + on() {c="d"}"#, "{}");
        f(r#"{a="b"} + on() group_left() {c="d"}"#, r#"{a="b"}"#);
        f(r#"{a="b"} + on(a) group_left() {c="d"}"#, r#"{a="b"}"#);
        f(
            r#"{a="b"} + on(c) group_left() {c="d"}"#,
            r#"{a="b",c="d"}"#,
        );
        f(
            r#"{a="b"} + on(a,c) group_left() {c="d"}"#,
            r#"{a="b",c="d"}"#,
        );
        f(r#"{a="b"} + on(d) group_left() {c="d"}"#, r#"{a="b"}"#);
        f(r#"{a="b"} + on() group_right(s) {c="d"}"#, r#"{c="d"}"#);
        f(
            r#"{a="b"} + On(a) groUp_right() {c="d"}"#,
            r#"{a="b",c="d"}"#,
        );
        f(r#"{a="b"} + on(c) group_right() {c="d"}"#, r#"{c="d"}"#);
        f(
            r#"{a="b"} + on(a,c) group_right() {c="d"}"#,
            r#"{a="b",c="d"}"#,
        );
        f(r#"{a="b"} + on(d) group_right() {c="d"}"#, r#"{c="d"}"#);
        f(r#"{a="b"} or {c="d"}"#, "{}");
        f(r#"{a="b",x="y"} or {x="y",c="d"}"#, r#"{x="y"}"#);
        f(r#"{a="b",x="y"} Or on() {x="y",c="d"}"#, "{}");
        f(r#"{a="b",x="y"} Or on(a) {x="y",c="d"}"#, "{}");
        f(r#"{a="b",x="y"} Or on(x) {x="y",c="d"}"#, r#"{x="y"}"#);
        f(r#"{a="b",x="y"} Or oN(x,y) {x="y",c="d"}"#, r#"{x="y"}"#);
        f(r#"{a="b",x="y"} Or on(y) {x="y",c="d"}"#, "{}");
        f(
            r#"(foo{a="b"} + bar{c="d"}) or (baz{x="y"} <= x{a="b"})"#,
            r#"{a="b"}"#,
        );
        f(r#"{a="b"} unless {c="d"}"#, r#"{a="b"}"#);
        f(r#"{a="b"} unless on() {c="d"}"#, "{}");
        f(r#"{a="b"} unLess on(a) {c="d"}"#, r#"{a="b"}"#);
        f(r#"{a="b"} unLEss on(c) {c="d"}"#, "{}");
        f(r#"{a="b"} unless on(a,c) {c="d"}"#, r#"{a="b"}"#);
        f(r#"{a="b"} Unless on(x) {c="d"}"#, "{}");

        // Common filters for 'or' filters.
        f(r#"{a="b" or c="d",a="b"}"#, r#"{a="b"}"#);
        f(r#"{a="b",c="d" or c="d",a="b"}"#, r#"{c="d",a="b"}"#);
        f(
            r#"foo{x="y",a="b",c="d" or c="d",a="b"}"#,
            r#"{c="d",a="b"}"#,
        );
    }
}
