//! WITH-template expansion.
//!
//! Port of `expandWithExpr` and friends from `parser.go`, plus the default
//! WITH templates (`ru`, `ttf`, `range_median`, `alias`).

use crate::ast::{
    extract_string_value, DurationExpr, Expr, LabelFilter, LabelFilterExpr, MetricExpr, ParensExpr,
    StringExpr, WithArgExpr,
};
use crate::lexer::is_string_prefix;
use crate::parser::{check_duplicate_with_arg_names, must_parse_with_arg_expr};
use crate::{ParseError, Result};
use std::sync::{Arc, OnceLock};

/// Port of Go `getDefaultWithArgExprs`.
pub(crate) fn default_with_arg_exprs() -> &'static [Arc<WithArgExpr>] {
    static DEFAULT: OnceLock<Vec<Arc<WithArgExpr>>> = OnceLock::new();
    DEFAULT.get_or_init(|| {
        prepare_with_arg_exprs(&[
            // ru - resource utilization
            "ru(freev, maxv) = clamp_min(maxv - clamp_min(freev, 0), 0) / clamp_min(maxv, 0) * 100",
            // ttf - time to fuckup
            "ttf(freev) = smooth_exponential(
                clamp_max(clamp_max(-freev, 0) / clamp_max(deriv_fast(freev), 0), 365*24*3600),
                clamp_max(step()/300, 1)
            )",
            "range_median(q) = range_quantile(0.5, q)",
            r#"alias(q, name) = label_set(q, "__name__", name)"#,
        ])
    })
}

/// Port of Go `prepareWithArgExprs`.
fn prepare_with_arg_exprs(ss: &[&str]) -> Vec<Arc<WithArgExpr>> {
    let was: Vec<Arc<WithArgExpr>> = ss
        .iter()
        .map(|s| Arc::new(must_parse_with_arg_expr(s)))
        .collect();
    if let Err(err) = check_duplicate_with_arg_names(&was) {
        panic!("BUG: {err}");
    }
    was
}

/// Port of Go `getWithArgExpr`: scans `was` backwards, since later
/// expressions may override previously defined ones. Returns the index.
fn get_with_arg_expr(was: &[Arc<WithArgExpr>], name: &str) -> Option<usize> {
    was.iter().rposition(|wa| wa.name == name)
}

/// Port of Go `expandWithArgs`.
fn expand_with_args(was: &[Arc<WithArgExpr>], args: &[Expr]) -> Result<Vec<Expr>> {
    args.iter().map(|arg| expand_with_expr(was, arg)).collect()
}

/// Port of Go `expandWithExprExt`.
///
/// `wa_idx` is the index of the referenced template inside `was`; only the
/// templates defined before it stay in scope, plus the bound `args`.
/// `args: None` mirrors Go's `nil` args (a bare reference without a call).
fn expand_with_expr_ext(
    was: &[Arc<WithArgExpr>],
    wa_idx: usize,
    args: Option<Vec<Expr>>,
) -> Result<Expr> {
    let wa = &was[wa_idx];
    let args_len = args.as_ref().map_or(0, Vec::len);
    if wa.args.len() != args_len {
        match args {
            None => {
                // This is possible when a metric name clashes with a WITH
                // template name. Return a MetricExpr with the wa.name name.
                return Ok(Expr::Metric(MetricExpr::from_name(&wa.name)));
            }
            Some(args) => {
                return Err(ParseError::new(format!(
                    "invalid number of args for {:?}; got {}; want {}",
                    wa.name,
                    args.len(),
                    wa.args.len()
                )));
            }
        }
    }
    let mut was_new: Vec<Arc<WithArgExpr>> = Vec::with_capacity(wa_idx + args_len);
    was_new.extend_from_slice(&was[..wa_idx]);
    if let Some(args) = args {
        for (i, arg) in args.into_iter().enumerate() {
            was_new.push(Arc::new(WithArgExpr {
                name: wa.args[i].clone(),
                args: Vec::new(),
                expr: arg,
            }));
        }
    }
    expand_with_expr(&was_new, &wa.expr)
}

/// Port of Go `expandWithExpr`: recursively expands all the WITH template
/// references in `e`.
pub(crate) fn expand_with_expr(was: &[Arc<WithArgExpr>], e: &Expr) -> Result<Expr> {
    match e {
        Expr::BinaryOp(t) => {
            let left = expand_with_expr(was, &t.left)?;
            let right = expand_with_expr(was, &t.right)?;
            let group_modifier_args = expand_modifier_args(was, &t.group_modifier.args)?;
            let join_modifier_args = expand_modifier_args(was, &t.join_modifier.args)?;
            let join_modifier_prefix = match &t.join_modifier_prefix {
                None => None,
                Some(prefix) => {
                    let jmp = expand_with_expr(was, &Expr::String(prefix.clone()))?;
                    let Expr::String(se) = jmp else {
                        let mut jm = String::new();
                        t.join_modifier.append_string(&mut jm);
                        let mut got = String::new();
                        jmp.append_string(&mut got);
                        return Err(ParseError::new(format!(
                            "unexpected prefix for {jm}; want quoted string; got {got}"
                        )));
                    };
                    Some(se)
                }
            };
            if t.op == "+" {
                if let (Expr::String(lse), Expr::String(rse)) = (&left, &right) {
                    return Ok(Expr::String(StringExpr::from_string(format!(
                        "{}{}",
                        lse.s, rse.s
                    ))));
                }
            }
            let mut be = t.clone();
            be.left = Box::new(left);
            be.right = Box::new(right);
            be.group_modifier.args = group_modifier_args;
            be.join_modifier.args = join_modifier_args;
            be.join_modifier_prefix = join_modifier_prefix;
            Ok(Expr::Parens(ParensExpr(vec![Expr::BinaryOp(be)])))
        }
        Expr::Func(t) => {
            let args = expand_with_args(was, &t.args)?;
            if let Some(wa_idx) = get_with_arg_expr(was, &t.name) {
                return expand_with_expr_ext(was, wa_idx, Some(args));
            }
            let mut fe = t.clone();
            fe.args = args;
            Ok(Expr::Func(fe))
        }
        Expr::Aggr(t) => {
            let args = expand_with_args(was, &t.args)?;
            if let Some(wa_idx) = get_with_arg_expr(was, &t.name) {
                return expand_with_expr_ext(was, wa_idx, Some(args));
            }
            let modifier_args = expand_modifier_args(was, &t.modifier.args)?;
            let mut ae = t.clone();
            ae.args = args;
            ae.modifier.args = modifier_args;
            Ok(Expr::Aggr(ae))
        }
        Expr::Parens(t) => {
            let exprs = expand_with_args(was, &t.0)?;
            Ok(Expr::Parens(ParensExpr(exprs)))
        }
        Expr::String(t) => {
            if !t.s.is_empty() {
                // Already expanded.
                return Ok(Expr::String(t.clone()));
            }
            let mut b = String::new();
            for token in &t.tokens {
                if is_string_prefix(token) {
                    b.push_str(&extract_string_value(token)?);
                    continue;
                }
                let Some(wa_idx) = get_with_arg_expr(was, token) else {
                    return Err(ParseError::new(format!(
                        "missing {token:?} value inside StringExpr"
                    )));
                };
                let e_new = expand_with_expr_ext(was, wa_idx, None)?;
                let Expr::String(se_src) = &e_new else {
                    let mut got = String::new();
                    e_new.append_string(&mut got);
                    return Err(ParseError::new(format!(
                        "{token:?} must be string expression; got {got:?}"
                    )));
                };
                assert!(
                    se_src.tokens.is_empty(),
                    "BUG: seSrc.tokens must be empty; got {:?}",
                    se_src.tokens
                );
                b.push_str(&se_src.s);
            }
            Ok(Expr::String(StringExpr::from_string(b)))
        }
        Expr::Rollup(t) => {
            let e_new = expand_with_expr(was, &t.expr)?;
            let mut re = t.clone();
            re.expr = Box::new(e_new);
            re.window = expand_duration(was, &re.window).map_err(|err| {
                let mut s = String::new();
                re.expr.append_string(&mut s);
                ParseError::new(format!("cannot parse window for {s}: {err}"))
            })?;
            re.step = expand_duration(was, &re.step).map_err(|err| {
                let mut s = String::new();
                re.expr.append_string(&mut s);
                ParseError::new(format!("cannot parse step in {s}: {err}"))
            })?;
            re.offset = expand_duration(was, &re.offset).map_err(|err| {
                let mut s = String::new();
                re.expr.append_string(&mut s);
                ParseError::new(format!("cannot parse offset in {s}: {err}"))
            })?;
            if let Some(at) = &t.at {
                re.at = Some(Box::new(expand_with_expr(was, at)?));
            }
            Ok(Expr::Rollup(re))
        }
        Expr::With(t) => {
            let mut was_new: Vec<Arc<WithArgExpr>> = Vec::with_capacity(was.len() + t.was.len());
            was_new.extend_from_slice(was);
            was_new.extend_from_slice(&t.was);
            expand_with_expr(&was_new, &t.expr)
        }
        Expr::Metric(t) => expand_metric_expr(was, t),
        // NumberExpr, DurationExpr and already-expanded expressions.
        other => Ok(other.clone()),
    }
}

/// The `*MetricExpr` case of Go `expandWithExpr`.
fn expand_metric_expr(was: &[Arc<WithArgExpr>], t: &MetricExpr) -> Result<Expr> {
    if t.lfss_unexpanded.is_empty() {
        // Already expanded.
        return Ok(Expr::Metric(t.clone()));
    }
    let t_str = || {
        let mut s = String::new();
        t.append_string(&mut s);
        s
    };
    let mut me = MetricExpr::default();
    // Find out if all the or-subclauses that specify a metric name agree on
    // one. NB: cannot use a guard value because metric names can be any
    // string.
    let mut common_metric_name = String::new();
    let mut have_common_metric = true;
    for lfes in &t.lfss_unexpanded {
        let mut local_metric_name = String::new();
        let mut lfs_new: Vec<LabelFilter> = Vec::new();
        for lfe in lfes {
            let Some(lfe_value) = &lfe.value else {
                // Expand lfe.label into lfs_new.
                let Some(wa_idx) = get_with_arg_expr(was, &lfe.label) else {
                    // Check to see if this is a possible metric name: the
                    // label name was quoted but the value is nil.
                    if lfe.is_possible_metric_name {
                        local_metric_name = check_and_prepend_metric_name_filter(
                            &mut lfs_new,
                            &local_metric_name,
                            &lfe.label,
                        )?;
                        continue;
                    }
                    return Err(ParseError::new(format!(
                        "cannot find WITH template for {:?} inside {:?}",
                        lfe.label,
                        t_str()
                    )));
                };
                let e_new = expand_with_expr_ext(was, wa_idx, Some(Vec::new()))?;
                let wme = match &e_new {
                    Expr::Metric(wme) if wme.get_metric_name().is_none() => wme,
                    _ => {
                        let mut got = String::new();
                        e_new.append_string(&mut got);
                        return Err(ParseError::new(format!(
                            "WITH template {:?} inside {:?} must be {{...}}; got {got:?}",
                            lfe.label,
                            t_str()
                        )));
                    }
                };
                assert!(
                    wme.lfss_unexpanded.is_empty(),
                    "BUG: wme.lfss_unexpanded must be empty after WITH template expansion"
                );
                let lfss_src = &wme.label_filterss;
                if lfss_src.len() > 1 {
                    let mut got = String::new();
                    wme.append_string(&mut got);
                    return Err(ParseError::new(format!(
                        "WITH template {:?} at {:?} must be {{...}} without 'or'; got {got}",
                        lfe.label,
                        t_str()
                    )));
                }
                if let Some(lfs) = lfss_src.first() {
                    lfs_new.extend_from_slice(lfs);
                }
                continue;
            };
            // Convert lfe to a LabelFilter.
            let se = expand_with_expr(was, &Expr::String(lfe_value.clone()))?;
            let Expr::String(se) = se else {
                unreachable!("BUG: expanding a StringExpr always yields a StringExpr");
            };
            let lfe_new = LabelFilterExpr {
                label: lfe.label.clone(),
                value: Some(se),
                is_negative: lfe.is_negative,
                is_regexp: lfe.is_regexp,
                is_possible_metric_name: false,
            };
            let lf = lfe_new.to_label_filter()?;
            if lf.is_metric_name_filter() {
                local_metric_name = check_and_prepend_metric_name_filter(
                    &mut lfs_new,
                    &local_metric_name,
                    &lf.value,
                )?;
            } else {
                lfs_new.push(lf);
            }
        }
        if have_common_metric && !local_metric_name.is_empty() {
            if common_metric_name.is_empty() {
                common_metric_name = local_metric_name;
            } else if common_metric_name != local_metric_name {
                have_common_metric = false;
            }
        }
        let lfs_new = remove_duplicate_label_filters(lfs_new);
        me.label_filterss.push(lfs_new);
    }
    // If all the or-subclauses that specify a metric name agree on one,
    // prepend it to clauses where __name__ is missing entirely (including
    // regexps and negatives).
    if have_common_metric && !common_metric_name.is_empty() {
        for lfs in &mut me.label_filterss {
            let have_name_clause = lfs.iter().any(|lf| lf.label == "__name__");
            if !have_name_clause {
                prepend_metric_name_filter(lfs, &common_metric_name);
            }
        }
    }

    let t = me;
    let Some(metric_name) = t.get_metric_name().map(str::to_string) else {
        return Ok(Expr::Metric(t));
    };
    let Some(wa_idx) = get_with_arg_expr(was, &metric_name) else {
        return Ok(Expr::Metric(t));
    };
    let e_new = expand_with_expr_ext(was, wa_idx, None)?;
    let re = match &e_new {
        Expr::Rollup(re) => Some(re),
        _ => None,
    };
    let wme = match &e_new {
        Expr::Rollup(re) => match &*re.expr {
            Expr::Metric(wme) => Some(wme),
            _ => None,
        },
        Expr::Metric(wme) => Some(wme),
        _ => None,
    };
    let Some(wme) = wme else {
        if t.is_only_metric_name() {
            return Ok(e_new);
        }
        let mut got = String::new();
        e_new.append_string(&mut got);
        let mut t_s = String::new();
        t.append_string(&mut t_s);
        return Err(ParseError::new(format!(
            "cannot expand {t_s:?} to non-metric expression {got:?}"
        )));
    };
    assert!(
        wme.lfss_unexpanded.is_empty(),
        "BUG: wme.lfss_unexpanded must be empty after WITH templates expansion"
    );
    let lfss_src = &wme.label_filterss;
    let mut lfss_new: Vec<Vec<LabelFilter>> = Vec::new();
    if lfss_src.len() != 1 {
        // template_name{filters} where template_name is {... or ...}.
        if t.is_only_metric_name() {
            // {filters} is empty. Return {... or ...}.
            return Ok(e_new);
        }
        if t.label_filterss.len() != 1 {
            // {filters} contains {... or ...}. It cannot be merged with
            // {... or ...}.
            let mut got = String::new();
            wme.append_string(&mut got);
            return Err(ParseError::new(format!(
                "{metric_name:?} mustn't contain 'or' filters; got {got}"
            )));
        }
        // {filters} doesn't contain `or`. Merge it with {... or ...} into
        // {...,filters or ...,filters}.
        for lfs in lfss_src {
            let mut lfs_new = lfs.clone();
            lfs_new.extend_from_slice(&t.label_filterss[0][1..]);
            lfss_new.push(remove_duplicate_label_filters(lfs_new));
        }
    } else {
        // template_name{... or ...} where template_name is an ordinary
        // {filters} without 'or'. Merge it into {filters,... or filters,...}.
        for lfs in &t.label_filterss {
            let mut lfs_new = lfss_src[0].clone();
            lfs_new.extend_from_slice(&lfs[1..]);
            lfss_new.push(remove_duplicate_label_filters(lfs_new));
        }
    }
    let me = MetricExpr::with_label_filterss(lfss_new);
    match re {
        None => Ok(Expr::Metric(me)),
        Some(re) => {
            let mut re_new = re.clone();
            re_new.expr = Box::new(Expr::Metric(me));
            Ok(Expr::Rollup(re_new))
        }
    }
}

/// Port of Go `checkAndPrependMetricNameFilter`; returns the new metric name.
fn check_and_prepend_metric_name_filter(
    lfs: &mut Vec<LabelFilter>,
    metric_name: &str,
    new_metric_name: &str,
) -> Result<String> {
    if !metric_name.is_empty() && metric_name != new_metric_name {
        return Err(ParseError::new(format!(
            "parse error: metric name must not be set twice: {metric_name:?} or {new_metric_name:?}"
        )));
    }
    prepend_metric_name_filter(lfs, new_metric_name);
    Ok(new_metric_name.to_string())
}

/// Port of Go `prependMetricNameFilter`.
fn prepend_metric_name_filter(lfs: &mut Vec<LabelFilter>, metric_name: &str) {
    lfs.insert(
        0,
        LabelFilter {
            label: "__name__".to_string(),
            value: metric_name.to_string(),
            is_negative: false,
            is_regexp: false,
        },
    );
}

/// Port of Go `expandDuration`.
fn expand_duration(
    was: &[Arc<WithArgExpr>],
    d: &Option<DurationExpr>,
) -> Result<Option<DurationExpr>> {
    let Some(d) = d else {
        return Ok(None);
    };
    if !d.needs_parsing {
        return Ok(Some(d.clone()));
    }
    let Some(wa_idx) = get_with_arg_expr(was, &d.s) else {
        return Err(ParseError::new(format!(
            "cannot find WITH template for {:?}",
            d.s
        )));
    };
    let e = expand_with_expr_ext(was, wa_idx, Some(Vec::new()))?;
    match &e {
        Expr::Duration(t) => {
            assert!(
                !t.needs_parsing,
                "BUG: DurationExpr {:?} must be already parsed",
                t.s
            );
            Ok(Some(t.clone()))
        }
        Expr::Number(t) => {
            // Convert a number of seconds to a DurationExpr.
            Ok(Some(DurationExpr::new(t.s.clone())?))
        }
        _ => {
            let mut got = String::new();
            e.append_string(&mut got);
            Err(ParseError::new(format!(
                "unexpected value for WITH template {:?}; got {got}; want duration",
                d.s
            )))
        }
    }
}

/// Port of Go `expandModifierArgs`.
fn expand_modifier_args(was: &[Arc<WithArgExpr>], args: &[String]) -> Result<Vec<String>> {
    if args.is_empty() {
        return Ok(Vec::new());
    }
    let err_cannot_use = |expr: &Expr, arg: &str| {
        let mut got = String::new();
        expr.append_string(&mut got);
        ParseError::new(format!("cannot use {got:?} instead of {arg:?} in {args:?}"))
    };
    let mut dst_args: Vec<String> = Vec::with_capacity(args.len());
    for arg in args {
        let Some(wa_idx) = get_with_arg_expr(was, arg) else {
            // Leave the arg as is.
            dst_args.push(arg.clone());
            continue;
        };
        let wa = &was[wa_idx];
        if !wa.args.is_empty() {
            // Template funcs cannot be used inside modifier lists. Leave the
            // arg as is.
            dst_args.push(arg.clone());
            continue;
        }
        match &wa.expr {
            Expr::Metric(me) => {
                if !me.is_only_metric_name() {
                    return Err(err_cannot_use(&wa.expr, arg));
                }
                let metric_name = me.get_metric_name().expect("only metric name");
                dst_args.push(metric_name.to_string());
            }
            Expr::Parens(pe) => {
                for p_arg in &pe.0 {
                    let Expr::Metric(me) = p_arg else {
                        return Err(err_cannot_use(&wa.expr, arg));
                    };
                    if !me.is_only_metric_name() {
                        return Err(err_cannot_use(&wa.expr, arg));
                    }
                    let metric_name = me.get_metric_name().expect("only metric name");
                    dst_args.push(metric_name.to_string());
                }
            }
            _ => {
                return Err(err_cannot_use(&wa.expr, arg));
            }
        }
    }

    // Remove duplicate args from dst_args.
    let mut seen = std::collections::HashSet::with_capacity(dst_args.len());
    let filtered: Vec<String> = dst_args
        .into_iter()
        .filter(|arg| seen.insert(arg.clone()))
        .collect();
    Ok(filtered)
}

/// Port of Go `removeDuplicateLabelFilters`.
pub(crate) fn remove_duplicate_label_filters(lfs: Vec<LabelFilter>) -> Vec<LabelFilter> {
    let mut seen: std::collections::HashSet<String> =
        std::collections::HashSet::with_capacity(lfs.len());
    let mut lfs_new: Vec<LabelFilter> = Vec::with_capacity(lfs.len());
    for lf in lfs {
        let mut buf = String::new();
        lf.append_string(&mut buf);
        if seen.insert(buf) {
            lfs_new.push(lf);
        }
    }
    lfs_new
}
