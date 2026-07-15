//! Binary operators over series. Port of `binary_op.go`.
//!
//! PORT-SKIP (Stage 2): the `q == (union)` / `q != (union)` special cases.

use crate::timeseries::{
    metric_name_group_key, remove_empty_series, set_tags, sort_series_by_metric_name,
    string_metric_tags, Timeseries,
};
use crate::{Error, Result};
use esm_metricsql::{is_binary_op_cmp, BinaryOpExpr};
use esm_storage::metric_name::MetricName;
use std::collections::HashMap;

/// Returns true if `op` is supported by [`eval_binary_op_series`].
pub fn is_supported_binary_op(op: &str) -> bool {
    matches!(
        op.to_ascii_lowercase().as_str(),
        "+" | "-"
            | "*"
            | "/"
            | "%"
            | "^"
            | "atan2"
            | "=="
            | "!="
            | ">"
            | "<"
            | ">="
            | "<="
            | "and"
            | "or"
            | "unless"
            | "if"
            | "ifnot"
            | "default"
    )
}

/// Applies the binary operator to the left/right series.
/// Port of the Go `binaryOpFuncs` dispatch.
pub fn eval_binary_op_series(
    be: &BinaryOpExpr,
    left: Vec<Timeseries>,
    right: Vec<Timeseries>,
) -> Result<Vec<Timeseries>> {
    let op = be.op.to_ascii_lowercase();
    match op.as_str() {
        "+" | "-" | "*" | "/" | "%" | "^" | "atan2" | "==" | "!=" | ">" | "<" | ">=" | "<=" => {
            binary_op_std(be, &op, left, right)
        }
        "and" => binary_op_and(be, left, right),
        "or" => binary_op_or(be, left, right),
        "unless" => binary_op_unless(be, left, right),
        "if" => binary_op_if(be, left, right),
        "ifnot" => binary_op_ifnot(be, left, right),
        "default" => binary_op_default(be, left, right),
        _ => Err(Error::new(format!("unknown binary op {:?}", be.op))),
    }
}

/// Scalar kernel for one point. Ports `binaryop.*` funcs plus the
/// `newBinaryOpCmpFunc`/`newBinaryOpArithFunc` wrappers.
fn eval_scalar_op(op: &str, left: f64, right: f64, is_bool: bool) -> f64 {
    match op {
        "+" => left + right,
        "-" => left - right,
        "*" => left * right,
        "/" => left / right,
        // Rust f64 `%` matches Go math.Mod semantics.
        "%" => left % right,
        "atan2" => left.atan2(right),
        "^" => {
            // Special case for NaN^any: math.Pow(NaN, 0) returns 1 in Go.
            if left.is_nan() {
                f64::NAN
            } else {
                left.powf(right)
            }
        }
        _ => {
            // Comparison ops. NaN comparison handling matches metricsql
            // binaryop funcs.
            let ok = match op {
                "==" => {
                    if left.is_nan() {
                        right.is_nan()
                    } else {
                        left == right
                    }
                }
                "!=" => {
                    if left.is_nan() {
                        !right.is_nan()
                    } else if right.is_nan() {
                        true
                    } else {
                        left != right
                    }
                }
                ">" => left > right,
                "<" => left < right,
                ">=" => left >= right,
                "<=" => left <= right,
                other => panic!("BUG: unexpected binary op {other:?}"),
            };
            if !is_bool {
                if ok {
                    left
                } else {
                    f64::NAN
                }
            } else if left.is_nan() {
                f64::NAN
            } else if ok {
                1.0
            } else {
                0.0
            }
        }
    }
}

/// Which side carries the result series.
#[derive(Clone, Copy, PartialEq)]
enum DstSide {
    Left,
    Right,
}

/// Port of Go `newBinaryOpFunc` + `adjustBinaryOpTags`.
fn binary_op_std(
    be: &BinaryOpExpr,
    op: &str,
    mut left: Vec<Timeseries>,
    mut right: Vec<Timeseries>,
) -> Result<Vec<Timeseries>> {
    let is_cmp = is_binary_op_cmp(op);
    if !is_cmp {
        // Do not remove empty series for comparison operations, since this
        // may lead to missing results for `(foo op bar) default N`.
        left = remove_empty_series(left);
        right = remove_empty_series(right);
    }
    if left.is_empty() || right.is_empty() {
        return Ok(Vec::new());
    }
    let is_bool = be.bool_modifier;

    // Fast paths: `scalar op vector` / `vector op scalar`.
    if be.group_modifier.op.is_empty() && be.join_modifier.op.is_empty() {
        if is_scalar(&left) {
            let scalar = std::mem::take(&mut left[0].values);
            for ts in right.iter_mut() {
                reset_metric_group_if_required(be, ts);
                for (j, v) in ts.values.iter_mut().enumerate() {
                    *v = eval_scalar_op(op, scalar[j], *v, is_bool);
                }
            }
            return Ok(right);
        }
        if is_scalar(&right) {
            let scalar = std::mem::take(&mut right[0].values);
            for ts in left.iter_mut() {
                reset_metric_group_if_required(be, ts);
                for (j, v) in ts.values.iter_mut().enumerate() {
                    *v = eval_scalar_op(op, *v, scalar[j], is_bool);
                }
            }
            return Ok(left);
        }
    }

    // Slow path: `vector op vector` or
    // `a op {on|ignoring} {group_left|group_right} b`.
    let (m_left, mut m_right) = create_timeseries_map_by_tag_set(be, left, right);
    let join_op = be.join_modifier.op.to_ascii_lowercase();
    let mut pairs: Vec<(Timeseries, Timeseries)> = Vec::new();
    let dst_side = if join_op == "group_right" {
        DstSide::Right
    } else {
        DstSide::Left
    };
    for (k, tss_left) in m_left {
        let Some(tss_right) = m_right.remove(&k) else {
            continue;
        };
        match join_op.as_str() {
            "group_left" => {
                group_join("right", be, &mut pairs, tss_left, tss_right, false)?;
            }
            "group_right" => {
                group_join("left", be, &mut pairs, tss_right, tss_left, true)?;
            }
            "" => {
                let mut ts_left = ensure_single_timeseries("left", be, tss_left)?;
                let ts_right = ensure_single_timeseries("right", be, tss_right)?;
                reset_metric_group_if_required(be, &mut ts_left);
                apply_group_modifier(be, &mut ts_left.metric_name);
                pairs.push((ts_left, ts_right));
            }
            _ => return Err(Error::new(format!("unexpected join modifier {join_op:?}"))),
        }
    }

    let mut rvs = Vec::with_capacity(pairs.len());
    for (mut ts_left, mut ts_right) in pairs {
        match dst_side {
            DstSide::Left => {
                for (j, v) in ts_left.values.iter_mut().enumerate() {
                    *v = eval_scalar_op(op, *v, ts_right.values[j], is_bool);
                }
                rvs.push(ts_left);
            }
            DstSide::Right => {
                for (j, v) in ts_right.values.iter_mut().enumerate() {
                    *v = eval_scalar_op(op, ts_left.values[j], *v, is_bool);
                }
                rvs.push(ts_right);
            }
        }
    }
    // Do not remove time series containing only NaNs, so
    // `(foo op bar) default N` works as expected.
    Ok(rvs)
}

/// Applies the `on(...)`/`ignoring(...)` modifier to `mn`
/// (default = `ignoring ()`). Also honors `keep_metric_names` +
/// `on(...)` by keeping `__name__`.
fn apply_group_modifier(be: &BinaryOpExpr, mn: &mut MetricName) {
    let group_op = if be.group_modifier.op.is_empty() {
        "ignoring".to_string()
    } else {
        be.group_modifier.op.to_ascii_lowercase()
    };
    let mut group_tags: Vec<&str> = be.group_modifier.args.iter().map(|s| s.as_str()).collect();
    if be.keep_metric_names && group_op == "on" {
        group_tags.push("__name__");
    }
    match group_op.as_str() {
        "on" => mn.remove_tags_on(&group_tags),
        "ignoring" => mn.remove_tags_ignoring(&group_tags),
        _ => panic!("BUG: unexpected binary op modifier {group_op:?}"),
    }
}

/// Port of Go `ensureSingleTimeseries`: merges non-overlapping duplicate
/// series into one or fails.
fn ensure_single_timeseries(
    side: &str,
    be: &BinaryOpExpr,
    mut tss: Vec<Timeseries>,
) -> Result<Timeseries> {
    assert!(!tss.is_empty(), "BUG: tss must contain at least one value");
    while tss.len() > 1 {
        let src = tss.pop().expect("len checked above");
        let (dst, src_ref) = (&mut tss[0], src);
        if !merge_non_overlapping_timeseries(dst, &src_ref) {
            let mut group_mod = String::new();
            be.group_modifier.append_string(&mut group_mod);
            return Err(Error::new(format!(
                "duplicate time series on the {side} side of {} {}: {} and {}",
                be.op,
                group_mod,
                string_metric_tags(&mut tss[0].metric_name.clone()),
                string_metric_tags(&mut src_ref.metric_name.clone()),
            )));
        }
    }
    Ok(tss.remove(0))
}

/// Port of Go `groupJoin`: copies join-modifier tags from the "one" side to
/// every "many" side series and pushes (many, one) pairs. `swapped` tells
/// whether the caller swapped sides (group_right), so the pairs are pushed
/// back in (left, right) order.
fn group_join(
    single_timeseries_side: &str,
    be: &BinaryOpExpr,
    pairs: &mut Vec<(Timeseries, Timeseries)>,
    tss_many: Vec<Timeseries>,
    tss_single: Vec<Timeseries>,
    swapped: bool,
) -> Result<()> {
    let join_tags = &be.join_modifier.args;
    let empty_tags: Vec<String> = Vec::new();
    let skip_tags: &[String] = if be.group_modifier.op.eq_ignore_ascii_case("on") {
        &be.group_modifier.args
    } else {
        &empty_tags
    };
    let join_prefix = be
        .join_modifier_prefix
        .as_ref()
        .map(|p| p.s.as_str())
        .unwrap_or("");

    let mut push = |many: Timeseries, single: Timeseries| {
        if swapped {
            pairs.push((single, many));
        } else {
            pairs.push((many, single));
        }
    };

    if tss_single.len() == 1 {
        // Easy case - the single side contains only one matching series.
        let single = &tss_single[0];
        for mut ts_many in tss_many {
            reset_metric_group_if_required(be, &mut ts_many);
            set_tags(
                &mut ts_many.metric_name,
                join_tags,
                join_prefix,
                skip_tags,
                &single.metric_name,
            );
            push(ts_many, single.clone());
        }
        return Ok(());
    }

    // Hard case - the single side contains multiple matching series.
    // Verify it doesn't result in duplicate MetricName values after adding
    // the missing tags.
    for mut ts_many in tss_many {
        reset_metric_group_if_required(be, &mut ts_many);
        let mut m: HashMap<Vec<u8>, (Timeseries, Timeseries)> = HashMap::new();
        for ts_single in &tss_single {
            let mut ts_copy = Timeseries::copy_from_shallow_timestamps(&ts_many);
            set_tags(
                &mut ts_copy.metric_name,
                join_tags,
                join_prefix,
                skip_tags,
                &ts_single.metric_name,
            );
            let key = metric_name_group_key(&mut ts_copy.metric_name);
            match m.get_mut(&key) {
                None => {
                    m.insert(key, (ts_copy, ts_single.clone()));
                }
                Some(pair) => {
                    // Try merging the accumulated single side with ts_single
                    // if they don't overlap.
                    let mut tmp = Timeseries::copy_from_shallow_timestamps(&pair.1);
                    if !merge_non_overlapping_timeseries(&mut tmp, ts_single) {
                        let mut group_mod = String::new();
                        be.group_modifier.append_string(&mut group_mod);
                        let mut join_mod = String::new();
                        be.join_modifier.append_string(&mut join_mod);
                        return Err(Error::new(format!(
                            "duplicate time series on the {single_timeseries_side} side of `{} {} {}`: {} and {}",
                            be.op,
                            group_mod,
                            join_mod,
                            string_metric_tags(&mut tmp.metric_name),
                            string_metric_tags(&mut ts_single.metric_name.clone()),
                        )));
                    }
                    pair.1 = tmp;
                }
            }
        }
        for (_, (many, single)) in m {
            push(many, single);
        }
    }
    Ok(())
}

/// Port of Go `mergeNonOverlappingTimeseries`.
pub(crate) fn merge_non_overlapping_timeseries(dst: &mut Timeseries, src: &Timeseries) -> bool {
    // Verify whether the time series can be merged.
    let mut overlaps = 0;
    for (i, v) in src.values.iter().enumerate() {
        if v.is_nan() {
            continue;
        }
        if !dst.values[i].is_nan() {
            overlaps += 1;
            // Allow up to two overlapping datapoints, which can appear due
            // to the staleness algorithm.
            if overlaps > 2 {
                return false;
            }
        }
    }
    // Do not merge time series with too small number of datapoints
    // (instant queries).
    if src.values.len() <= 2 && dst.values.len() <= 2 {
        return false;
    }
    // Time series can be merged. Merge them.
    for (i, v) in src.values.iter().enumerate() {
        if v.is_nan() {
            continue;
        }
        dst.values[i] = *v;
    }
    true
}

/// Port of Go `resetMetricGroupIfRequired`.
fn reset_metric_group_if_required(be: &BinaryOpExpr, ts: &mut Timeseries) {
    if is_binary_op_cmp(&be.op) && !be.bool_modifier {
        // Do not reset MetricGroup for non-boolean `compare` binary ops.
        return;
    }
    if be.keep_metric_names {
        return;
    }
    ts.metric_name.reset_metric_group();
}

type SeriesMap = HashMap<Vec<u8>, Vec<Timeseries>>;

/// Port of Go `createTimeseriesMapByTagSet`.
fn create_timeseries_map_by_tag_set(
    be: &BinaryOpExpr,
    left: Vec<Timeseries>,
    right: Vec<Timeseries>,
) -> (SeriesMap, SeriesMap) {
    let get_tags_map = |arg: Vec<Timeseries>| -> SeriesMap {
        let mut m: SeriesMap = HashMap::with_capacity(arg.len());
        for ts in arg {
            let mut mn = ts.metric_name.clone();
            if !be.keep_metric_names {
                mn.reset_metric_group();
            }
            let group_op = if be.group_modifier.op.is_empty() {
                "ignoring".to_string()
            } else {
                be.group_modifier.op.to_ascii_lowercase()
            };
            let group_tags: Vec<&str> = be.group_modifier.args.iter().map(|s| s.as_str()).collect();
            match group_op.as_str() {
                "on" => mn.remove_tags_on(&group_tags),
                "ignoring" => mn.remove_tags_ignoring(&group_tags),
                _ => panic!("BUG: unexpected binary op modifier {group_op:?}"),
            }
            let key = metric_name_group_key(&mut mn);
            m.entry(key).or_default().push(ts);
        }
        m
    };
    (get_tags_map(left), get_tags_map(right))
}

/// Port of Go `isScalar`.
pub(crate) fn is_scalar(arg: &[Timeseries]) -> bool {
    if arg.len() != 1 {
        return false;
    }
    let mn = &arg[0].metric_name;
    mn.metric_group.is_empty() && mn.tags.is_empty()
}

fn binary_op_if(
    be: &BinaryOpExpr,
    left: Vec<Timeseries>,
    right: Vec<Timeseries>,
) -> Result<Vec<Timeseries>> {
    let (m_left, m_right) = create_timeseries_map_by_tag_set(be, left, right);
    let mut rvs = Vec::new();
    for (k, mut tss_left) in m_left {
        let Some(tss_right) = series_by_key(&m_right, &k) else {
            continue;
        };
        add_right_nans_to_left(&mut tss_left, tss_right);
        rvs.extend(remove_empty_series(tss_left));
    }
    Ok(rvs)
}

fn binary_op_and(
    be: &BinaryOpExpr,
    left: Vec<Timeseries>,
    right: Vec<Timeseries>,
) -> Result<Vec<Timeseries>> {
    let (mut m_left, m_right) = create_timeseries_map_by_tag_set(be, left, right);
    let mut rvs = Vec::new();
    for (k, tss_right) in &m_right {
        let Some(mut tss_left) = m_left.remove(k) else {
            continue;
        };
        add_right_nans_to_left(&mut tss_left, tss_right);
        rvs.extend(remove_empty_series(tss_left));
    }
    Ok(rvs)
}

/// Port of Go `addRightNaNsToLeft` (without the trailing
/// `removeEmptySeries`, which the callers apply).
fn add_right_nans_to_left(tss_left: &mut [Timeseries], tss_right: &[Timeseries]) {
    for ts_left in tss_left.iter_mut() {
        for i in 0..ts_left.values.len() {
            let has_value = tss_right.iter().any(|ts| !ts.values[i].is_nan());
            if !has_value {
                ts_left.values[i] = f64::NAN;
            }
        }
    }
}

fn binary_op_default(
    be: &BinaryOpExpr,
    left: Vec<Timeseries>,
    right: Vec<Timeseries>,
) -> Result<Vec<Timeseries>> {
    let (m_left, m_right) = create_timeseries_map_by_tag_set(be, left, right);
    let mut rvs = Vec::new();
    if m_left.is_empty() {
        for (_, tss) in m_right {
            rvs.extend(tss);
        }
        return Ok(rvs);
    }
    for (k, mut tss_left) in m_left {
        if let Some(tss_right) = series_by_key(&m_right, &k) {
            fill_left_nans_with_right_values(&mut tss_left, tss_right);
        }
        rvs.extend(tss_left);
    }
    Ok(rvs)
}

fn binary_op_or(
    be: &BinaryOpExpr,
    left: Vec<Timeseries>,
    right: Vec<Timeseries>,
) -> Result<Vec<Timeseries>> {
    let (m_left, m_right) = create_timeseries_map_by_tag_set(be, left, right);
    let mut m_left: HashMap<Vec<u8>, Vec<Timeseries>> = m_left
        .into_iter()
        .map(|(k, tss)| (k, remove_empty_series(tss)))
        .collect();

    // Merge matching right-side groups into the left side; collect the
    // surviving (not consumed) right-side series.
    let mut right_extras: Vec<Timeseries> = Vec::new();
    for (k, mut tss_right) in m_right {
        match m_left.get_mut(&k) {
            None => right_extras.extend(tss_right),
            Some(tss_left) => {
                fill_left_nans_with_right_values_or_merge(tss_left, &mut tss_right);
                // tss_right might be filled with NaNs after the merge.
                right_extras.extend(remove_empty_series(tss_right));
            }
        }
    }
    // Sort the left-hand-side series by metric name as Prometheus does,
    // then append the sorted right-hand-side extras.
    let mut rvs: Vec<Timeseries> = m_left.into_values().flatten().collect();
    sort_series_by_metric_name(&mut rvs);
    sort_series_by_metric_name(&mut right_extras);
    rvs.extend(right_extras);
    Ok(rvs)
}

/// Port of Go `fillLeftNaNsWithRightValues`.
fn fill_left_nans_with_right_values(tss_left: &mut [Timeseries], tss_right: &[Timeseries]) {
    for ts_left in tss_left.iter_mut() {
        for (i, v) in ts_left.values.iter_mut().enumerate() {
            if !v.is_nan() {
                continue;
            }
            for ts_right in tss_right {
                let v_right = ts_right.values[i];
                if !v_right.is_nan() {
                    *v = v_right;
                    break;
                }
            }
        }
    }
}

/// Port of Go `fillLeftNaNsWithRightValuesOrMerge`: fills gaps in tss_left
/// with values from tss_right when the labels match, and NaN-outs the
/// consumed right values.
fn fill_left_nans_with_right_values_or_merge(
    tss_left: &mut [Timeseries],
    tss_right: &mut [Timeseries],
) {
    if is_scalar(tss_right) {
        // Fast path: a scalar right side can be merged with the left side
        // only when the left side is also a scalar.
        let can_be_merged = is_scalar(tss_left);
        for ts_left in tss_left.iter_mut() {
            for (i, v) in ts_left.values.iter_mut().enumerate() {
                let left_is_nan = v.is_nan();
                let value_right = tss_right[0].values[i];
                if left_is_nan && can_be_merged {
                    *v = value_right;
                }
                if !left_is_nan || can_be_merged {
                    tss_right[0].values[i] = f64::NAN;
                }
            }
        }
        return;
    }

    let name_right_keys: Vec<Vec<u8>> = tss_right
        .iter_mut()
        .map(|ts| metric_name_group_key(&mut ts.metric_name))
        .collect();
    for ts_left in tss_left.iter_mut() {
        let name_left = metric_name_group_key(&mut ts_left.metric_name);
        for (i, v) in ts_left.values.iter_mut().enumerate() {
            let left_is_nan = v.is_nan();
            for (r, ts_right) in tss_right.iter_mut().enumerate() {
                let can_be_merged = name_right_keys[r] == name_left;
                let value_right = ts_right.values[i];
                if left_is_nan && can_be_merged {
                    *v = value_right;
                }
                if !left_is_nan || can_be_merged {
                    ts_right.values[i] = f64::NAN;
                }
            }
        }
    }
}

fn binary_op_ifnot(
    be: &BinaryOpExpr,
    left: Vec<Timeseries>,
    right: Vec<Timeseries>,
) -> Result<Vec<Timeseries>> {
    let (m_left, m_right) = create_timeseries_map_by_tag_set(be, left, right);
    let mut rvs = Vec::new();
    for (k, mut tss_left) in m_left {
        let Some(tss_right) = series_by_key(&m_right, &k) else {
            rvs.extend(tss_left);
            continue;
        };
        add_left_nans_if_no_right_nans(&mut tss_left, tss_right);
        rvs.extend(remove_empty_series(tss_left));
    }
    Ok(rvs)
}

fn binary_op_unless(
    be: &BinaryOpExpr,
    left: Vec<Timeseries>,
    right: Vec<Timeseries>,
) -> Result<Vec<Timeseries>> {
    let (m_left, m_right) = create_timeseries_map_by_tag_set(be, left, right);
    let mut rvs = Vec::new();
    for (k, mut tss_left) in m_left {
        let Some(tss_right) = m_right.get(&k) else {
            rvs.extend(tss_left);
            continue;
        };
        add_left_nans_if_no_right_nans(&mut tss_left, tss_right);
        rvs.extend(remove_empty_series(tss_left));
    }
    Ok(rvs)
}

/// Port of Go `addLeftNaNsIfNoRightNaNs` (without the trailing
/// `removeEmptySeries`, which the callers apply).
fn add_left_nans_if_no_right_nans(tss_left: &mut [Timeseries], tss_right: &[Timeseries]) {
    for ts_left in tss_left.iter_mut() {
        for i in 0..ts_left.values.len() {
            if tss_right.iter().any(|ts| !ts.values[i].is_nan()) {
                ts_left.values[i] = f64::NAN;
            }
        }
    }
}

/// Port of Go `seriesByKey`: exact key match, or the single scalar group.
fn series_by_key<'a>(m: &'a SeriesMap, key: &[u8]) -> Option<&'a Vec<Timeseries>> {
    if let Some(tss) = m.get(key) {
        return Some(tss);
    }
    if m.len() != 1 {
        return None;
    }
    let tss = m.values().next().expect("len checked above");
    if is_scalar(tss) {
        return Some(tss);
    }
    None
}
