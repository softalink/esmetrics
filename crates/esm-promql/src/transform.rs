//! Transform functions (Stage-1 subset). Port of the corresponding parts of
//! `transform.go`. The registry is table-driven, so adding the remaining
//! functions is mechanical.

use crate::eval::{eval_number, eval_time, EvalConfig};
use crate::timeseries::{metric_name_group_key, Timeseries};
use crate::{Error, Result};
use esm_metricsql::{Expr, FuncExpr};
use std::collections::HashSet;

pub struct TransformFuncArg<'a> {
    pub ec: &'a EvalConfig,
    pub fe: &'a FuncExpr,
    pub args: Vec<Vec<Timeseries>>,
}

type TransformFunc = fn(&mut TransformFuncArg<'_>) -> Result<Vec<Timeseries>>;

/// Returns the transform function for the given name (Stage-1 subset).
/// Port of Go `getTransformFunc` over the `transformFuncs` table.
pub fn get_transform_func(name: &str) -> Option<TransformFunc> {
    let name = name.to_ascii_lowercase();
    let tf: TransformFunc = match name.as_str() {
        "" | "union" => transform_union,
        "abs" => |tfa| one_arg(tfa, f64::abs),
        "absent" => transform_absent,
        "ceil" => |tfa| one_arg(tfa, f64::ceil),
        "clamp" => transform_clamp,
        "clamp_max" => transform_clamp_max,
        "clamp_min" => transform_clamp_min,
        "end" => |tfa| zero_args(tfa, |tfa| tfa.ec.end as f64 / 1e3),
        "exp" => |tfa| one_arg(tfa, f64::exp),
        "floor" => |tfa| one_arg(tfa, f64::floor),
        "label_set" => transform_label_set,
        "ln" => |tfa| one_arg(tfa, f64::ln),
        "log2" => |tfa| one_arg(tfa, f64::log2),
        "log10" => |tfa| one_arg(tfa, f64::log10),
        "round" => transform_round,
        "scalar" => transform_scalar,
        "sgn" => transform_sgn,
        "sort" => |tfa| transform_sort_impl(tfa, false),
        "sort_desc" => |tfa| transform_sort_impl(tfa, true),
        "sqrt" => |tfa| one_arg(tfa, f64::sqrt),
        "start" => |tfa| zero_args(tfa, |tfa| tfa.ec.start as f64 / 1e3),
        "step" => |tfa| zero_args(tfa, |tfa| tfa.ec.step as f64 / 1e3),
        "time" => transform_time,
        "vector" => transform_vector,
        _ => return None,
    };
    Some(tf)
}

/// Port of Go `transformFuncsKeepMetricName` (replicated literally,
/// including the `range_sddev` typo, for bug-compatibility).
fn keep_metric_name(name: &str) -> bool {
    matches!(
        name,
        "ceil"
            | "clamp"
            | "clamp_max"
            | "clamp_min"
            | "floor"
            | "interpolate"
            | "keep_last_value"
            | "keep_next_value"
            | "range_avg"
            | "range_first"
            | "range_last"
            | "range_linear_regression"
            | "range_max"
            | "range_min"
            | "range_normalize"
            | "range_quantile"
            | "range_stdvar"
            | "range_sddev"
            | "round"
            | "running_avg"
            | "running_max"
            | "running_min"
            | "smooth_exponential"
    )
}

fn expect_transform_args_num(tfa: &TransformFuncArg<'_>, expected: usize) -> Result<()> {
    if tfa.args.len() == expected {
        return Ok(());
    }
    Err(Error::new(format!(
        "unexpected number of args; got {}; want {expected}",
        tfa.args.len()
    )))
}

/// Port of Go `doTransformValues`.
fn do_transform_values(
    mut arg: Vec<Timeseries>,
    tf: impl Fn(&mut [f64]),
    fe: &FuncExpr,
) -> Result<Vec<Timeseries>> {
    let name = fe.name.to_ascii_lowercase();
    let keep_metric_names = fe.keep_metric_names || keep_metric_name(&name);
    for ts in arg.iter_mut() {
        if !keep_metric_names {
            ts.metric_name.reset_metric_group();
        }
        tf(&mut ts.values);
    }
    Ok(arg)
}

/// Port of Go `newTransformFuncOneArg`.
fn one_arg(tfa: &mut TransformFuncArg<'_>, f: fn(f64) -> f64) -> Result<Vec<Timeseries>> {
    expect_transform_args_num(tfa, 1)?;
    let arg = std::mem::take(&mut tfa.args[0]);
    do_transform_values(
        arg,
        |values| {
            for v in values.iter_mut() {
                *v = f(*v);
            }
        },
        tfa.fe,
    )
}

/// Port of Go `newTransformFuncZeroArgs`.
fn zero_args(
    tfa: &mut TransformFuncArg<'_>,
    f: fn(&TransformFuncArg<'_>) -> f64,
) -> Result<Vec<Timeseries>> {
    expect_transform_args_num(tfa, 0)?;
    let v = f(tfa);
    Ok(eval_number(tfa.ec, v))
}

/// Port of Go `getScalar`.
fn get_scalar(tfa: &TransformFuncArg<'_>, arg_num: usize) -> Result<Vec<f64>> {
    let arg = tfa
        .args
        .get(arg_num)
        .ok_or_else(|| Error::new(format!("arg #{} must be a scalar", arg_num + 1)))?;
    if arg.len() != 1 {
        return Err(Error::new(format!("arg #{} must be a scalar", arg_num + 1)));
    }
    Ok(arg[0].values.clone())
}

/// Port of Go `getString`: a string arg is a single series with only NaN
/// values whose metric group carries the string.
fn get_string(tfa: &TransformFuncArg<'_>, arg_num: usize) -> Result<String> {
    let arg = tfa
        .args
        .get(arg_num)
        .ok_or_else(|| Error::new(format!("arg #{} must be a string", arg_num + 1)))?;
    if arg.len() != 1 {
        return Err(Error::new(format!("arg #{} must be a string", arg_num + 1)));
    }
    let ts = &arg[0];
    if ts.values.iter().any(|v| !v.is_nan()) {
        return Err(Error::new(format!("arg #{} must be a string", arg_num + 1)));
    }
    Ok(String::from_utf8_lossy(&ts.metric_name.metric_group).into_owned())
}

fn transform_absent(tfa: &mut TransformFuncArg<'_>) -> Result<Vec<Timeseries>> {
    expect_transform_args_num(tfa, 1)?;
    let tss = std::mem::take(&mut tfa.args[0]);
    let mut rvs = get_absent_timeseries(tfa.ec, &tfa.fe.args[0]);
    if tss.is_empty() {
        return Ok(rvs);
    }
    for i in 0..tss[0].values.len() {
        let is_absent = tss.iter().all(|ts| ts.values[i].is_nan());
        if !is_absent {
            rvs[0].values[i] = f64::NAN;
        }
    }
    Ok(rvs)
}

/// Port of Go `getAbsentTimeseries`: a constant-1 series carrying the plain
/// (non-regexp, non-negative) label filters from the arg metric expression.
pub(crate) fn get_absent_timeseries(ec: &EvalConfig, arg: &Expr) -> Vec<Timeseries> {
    // Copy tags from arg.
    let mut rvs = eval_number(ec, 1.0);
    let Expr::Metric(me) = arg else {
        return rvs;
    };
    if me.label_filterss.len() != 1 {
        return rvs;
    }
    for lf in &me.label_filterss[0] {
        if lf.label == "__name__" {
            continue;
        }
        if lf.is_regexp || lf.is_negative {
            continue;
        }
        rvs[0].metric_name.add_tag(&lf.label, &lf.value);
    }
    rvs
}

fn transform_clamp(tfa: &mut TransformFuncArg<'_>) -> Result<Vec<Timeseries>> {
    expect_transform_args_num(tfa, 3)?;
    let mins = get_scalar(tfa, 1)?;
    let maxs = get_scalar(tfa, 2)?;
    let arg = std::mem::take(&mut tfa.args[0]);
    do_transform_values(
        arg,
        |values| {
            for (i, v) in values.iter_mut().enumerate() {
                if *v > maxs[i] {
                    *v = maxs[i];
                } else if *v < mins[i] {
                    *v = mins[i];
                }
            }
        },
        tfa.fe,
    )
}

fn transform_clamp_max(tfa: &mut TransformFuncArg<'_>) -> Result<Vec<Timeseries>> {
    expect_transform_args_num(tfa, 2)?;
    let maxs = get_scalar(tfa, 1)?;
    let arg = std::mem::take(&mut tfa.args[0]);
    do_transform_values(
        arg,
        |values| {
            for (i, v) in values.iter_mut().enumerate() {
                if *v > maxs[i] {
                    *v = maxs[i];
                }
            }
        },
        tfa.fe,
    )
}

fn transform_clamp_min(tfa: &mut TransformFuncArg<'_>) -> Result<Vec<Timeseries>> {
    expect_transform_args_num(tfa, 2)?;
    let mins = get_scalar(tfa, 1)?;
    let arg = std::mem::take(&mut tfa.args[0]);
    do_transform_values(
        arg,
        |values| {
            for (i, v) in values.iter_mut().enumerate() {
                if *v < mins[i] {
                    *v = mins[i];
                }
            }
        },
        tfa.fe,
    )
}

/// Port of Go `transformRound`.
fn transform_round(tfa: &mut TransformFuncArg<'_>) -> Result<Vec<Timeseries>> {
    if tfa.args.len() != 1 && tfa.args.len() != 2 {
        return Err(Error::new(format!(
            "unexpected number of args: {}; want 1 or 2",
            tfa.args.len()
        )));
    }
    let nearest = if tfa.args.len() == 1 {
        let ts = eval_number(tfa.ec, 1.0);
        ts[0].values.clone()
    } else {
        get_scalar(tfa, 1)?
    };
    let arg = std::mem::take(&mut tfa.args[0]);
    do_transform_values(
        arg,
        |values| {
            let mut n_prev = 0f64;
            let mut p10 = 0f64;
            for (i, v) in values.iter_mut().enumerate() {
                let n = nearest[i];
                if n != n_prev {
                    n_prev = n;
                    let (_, e) = esm_common::decimal::from_float(n);
                    p10 = 10f64.powi(-e as i32);
                }
                let mut x = *v;
                x += 0.5 * n.copysign(x);
                x -= x % n;
                let (x_trunc, _) = modf(x * p10);
                *v = x_trunc / p10;
            }
        },
        tfa.fe,
    )
}

/// Port of Go `math.Modf`: returns (integer part, fractional part) with the
/// sign of the input.
fn modf(x: f64) -> (f64, f64) {
    let int_part = x.trunc();
    (int_part, x - int_part)
}

fn transform_sgn(tfa: &mut TransformFuncArg<'_>) -> Result<Vec<Timeseries>> {
    expect_transform_args_num(tfa, 1)?;
    let arg = std::mem::take(&mut tfa.args[0]);
    do_transform_values(
        arg,
        |values| {
            for v in values.iter_mut() {
                let mut sign = 0f64;
                if *v < 0.0 {
                    sign = -1.0;
                } else if *v > 0.0 {
                    sign = 1.0;
                }
                *v = sign;
            }
        },
        tfa.fe,
    )
}

/// Port of Go `transformScalar`.
fn transform_scalar(tfa: &mut TransformFuncArg<'_>) -> Result<Vec<Timeseries>> {
    expect_transform_args_num(tfa, 1)?;

    // Verify whether the arg is a string; then try converting it to a number.
    if let Expr::String(se) = &tfa.fe.args[0] {
        let n: f64 = se.s.parse().unwrap_or(f64::NAN);
        return Ok(eval_number(tfa.ec, n));
    }

    // The arg isn't a string. Extract the scalar from it.
    let mut arg = std::mem::take(&mut tfa.args[0]);
    if arg.len() != 1 {
        return Ok(eval_number(tfa.ec, f64::NAN));
    }
    arg[0].metric_name.reset();
    Ok(arg)
}

fn transform_time(tfa: &mut TransformFuncArg<'_>) -> Result<Vec<Timeseries>> {
    expect_transform_args_num(tfa, 0)?;
    Ok(eval_time(tfa.ec))
}

fn transform_vector(tfa: &mut TransformFuncArg<'_>) -> Result<Vec<Timeseries>> {
    expect_transform_args_num(tfa, 1)?;
    Ok(std::mem::take(&mut tfa.args[0]))
}

/// Port of Go `transformUnion`.
fn transform_union(tfa: &mut TransformFuncArg<'_>) -> Result<Vec<Timeseries>> {
    let args = std::mem::take(&mut tfa.args);
    if args.is_empty() {
        return Ok(eval_number(tfa.ec, f64::NAN));
    }

    if args.iter().all(|arg| crate::binary_op::is_scalar(arg)) {
        // Special case for `(v1, ..., vN)` where all args are scalars.
        return Ok(args.into_iter().map(|mut arg| arg.remove(0)).collect());
    }

    let mut rvs = Vec::with_capacity(args[0].len());
    let mut m: HashSet<Vec<u8>> = HashSet::new();
    for arg in args {
        for mut ts in arg {
            let key = metric_name_group_key(&mut ts.metric_name);
            if m.contains(&key) {
                continue;
            }
            m.insert(key);
            rvs.push(ts);
        }
    }
    Ok(rvs)
}

/// Port of Go `newTransformFuncSort`.
fn transform_sort_impl(tfa: &mut TransformFuncArg<'_>, is_desc: bool) -> Result<Vec<Timeseries>> {
    expect_transform_args_num(tfa, 1)?;
    let mut rvs = std::mem::take(&mut tfa.args[0]);
    rvs.sort_by(|a, b| {
        let a = &a.values;
        let b = &b.values;
        let mut n = a.len() as i64 - 1;
        while n >= 0 {
            let i = n as usize;
            if !a[i].is_nan() {
                if b[i].is_nan() {
                    return std::cmp::Ordering::Greater;
                }
                if a[i] != b[i] {
                    break;
                }
            } else if !b[i].is_nan() {
                return std::cmp::Ordering::Less;
            }
            n -= 1;
        }
        if n < 0 {
            return std::cmp::Ordering::Equal;
        }
        let i = n as usize;
        let ord = if is_desc {
            b[i].partial_cmp(&a[i])
        } else {
            a[i].partial_cmp(&b[i])
        };
        ord.unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(rvs)
}

/// Port of Go `transformLabelSet`.
fn transform_label_set(tfa: &mut TransformFuncArg<'_>) -> Result<Vec<Timeseries>> {
    if tfa.args.is_empty() {
        return Err(Error::new("not enough args; got 0; want at least 1"));
    }
    let (dst_labels, dst_values) = get_string_pairs(tfa, 1)?;
    let mut rvs = std::mem::take(&mut tfa.args[0]);
    for ts in rvs.iter_mut() {
        let mn = &mut ts.metric_name;
        for (dst_label, value) in dst_labels.iter().zip(dst_values.iter()) {
            if value.is_empty() {
                mn.remove_tag(dst_label);
            } else if dst_label == "__name__" {
                mn.metric_group.clear();
                mn.metric_group.extend_from_slice(value.as_bytes());
            } else {
                set_label_value(mn, dst_label, value);
            }
        }
    }
    Ok(rvs)
}

fn set_label_value(mn: &mut esm_storage::metric_name::MetricName, label: &str, value: &str) {
    for tag in &mut mn.tags {
        if tag.key == label.as_bytes() {
            tag.value.clear();
            tag.value.extend_from_slice(value.as_bytes());
            return;
        }
    }
    mn.add_tag(label, value);
}

/// Port of Go `getStringPairs` over args starting at `first_arg`.
fn get_string_pairs(
    tfa: &TransformFuncArg<'_>,
    first_arg: usize,
) -> Result<(Vec<String>, Vec<String>)> {
    let rest = tfa.args.len().saturating_sub(first_arg);
    if rest % 2 != 0 {
        return Err(Error::new(format!(
            "the number of string args must be even; got {rest}"
        )));
    }
    let mut keys = Vec::with_capacity(rest / 2);
    let mut values = Vec::with_capacity(rest / 2);
    let mut i = first_arg;
    while i < tfa.args.len() {
        keys.push(get_string(tfa, i)?);
        values.push(get_string(tfa, i + 1)?);
        i += 2;
    }
    Ok((keys, values))
}
