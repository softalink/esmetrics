//! Query execution entry point. Port of `exec.go`.

use crate::eval::{eval_expr, EvalConfig};
use crate::parse_cache::parse_cache;
use crate::provider::MetricsProvider;
use crate::timeseries::{
    metric_name_group_key, remove_empty_series, sort_series_by_metric_name, string_metric_name,
    Timeseries,
};
use crate::{Error, Result};
use esm_metricsql::{is_binary_op_cmp, Expr};
use esm_storage::metric_name::MetricName;
use std::collections::HashSet;
use std::sync::Arc;

/// A single result series. Mirror of `netstorage.Result`.
#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    pub metric_name: MetricName,
    pub values: Vec<f64>,
    pub timestamps: Arc<Vec<i64>>,
}

/// Executes `q` for the given `ec` over `provider`. Port of Go `Exec`.
pub fn exec(provider: &dyn MetricsProvider, ec: &EvalConfig, q: &str) -> Result<Vec<QueryResult>> {
    ec.validate()?;

    let e = parse_promql_with_cache(q)?;
    let rv = eval_expr(provider, ec, &e)?;
    let may_sort = may_sort_results(&e);
    let mut result = timeseries_to_result(rv, may_sort)?;
    if ec.round_digits < 100 {
        for r in result.iter_mut() {
            for v in r.values.iter_mut() {
                *v = esm_common::decimal::round_to_decimal_digits(*v, ec.round_digits);
            }
        }
    }
    Ok(result)
}

/// Port of Go `maySortResults`.
fn may_sort_results(e: &Expr) -> bool {
    match e {
        Expr::Func(fe) => !matches!(
            fe.name.to_ascii_lowercase().as_str(),
            "sort"
                | "sort_desc"
                | "limit_offset"
                | "sort_by_label"
                | "sort_by_label_desc"
                | "sort_by_label_numeric"
                | "sort_by_label_numeric_desc"
        ),
        Expr::Aggr(ae) => !matches!(
            ae.name.to_ascii_lowercase().as_str(),
            "topk"
                | "bottomk"
                | "outliersk"
                | "topk_max"
                | "topk_min"
                | "topk_avg"
                | "topk_median"
                | "topk_last"
                | "bottomk_max"
                | "bottomk_min"
                | "bottomk_avg"
                | "bottomk_median"
                | "bottomk_last"
        ),
        Expr::BinaryOp(be) => !be.op.eq_ignore_ascii_case("or"),
        _ => true,
    }
}

/// Port of Go `timeseriesToResult`: removes empty series, optionally sorts,
/// verifies there are no duplicate output series.
pub fn timeseries_to_result(tss: Vec<Timeseries>, may_sort: bool) -> Result<Vec<QueryResult>> {
    let mut tss = remove_empty_series(tss);
    if may_sort {
        sort_series_by_metric_name(&mut tss);
    }

    let mut result = Vec::with_capacity(tss.len());
    let mut seen: HashSet<Vec<u8>> = HashSet::with_capacity(tss.len());
    for mut ts in tss {
        let key = metric_name_group_key(&mut ts.metric_name);
        if !seen.insert(key) {
            return Err(Error::new(format!(
                "duplicate output timeseries: {}",
                string_metric_name(&mut ts.metric_name)
            )));
        }
        result.push(QueryResult {
            metric_name: ts.metric_name,
            values: ts.values,
            timestamps: ts.timestamps,
        });
    }
    Ok(result)
}

/// Port of Go `parsePromQLWithCache`: parse + optimize + adjustCmpOps,
/// cached (both successes and errors).
pub fn parse_promql_with_cache(q: &str) -> Result<Arc<Expr>> {
    if let Some(cached) = parse_cache().get(q) {
        return cached;
    }
    let result = match esm_metricsql::parse(q) {
        Ok(e) => {
            let e = esm_metricsql::optimize(&e);
            let e = adjust_cmp_ops(e);
            Ok(Arc::new(e))
        }
        Err(err) => Err(Error::from(err)),
    };
    parse_cache().put(q, result.clone());
    result
}

/// Port of Go `adjustCmpOps`: converts `num cmpOp query` to
/// `query reverseCmpOp num` like Prometheus does.
fn adjust_cmp_ops(e: Expr) -> Expr {
    match e {
        Expr::BinaryOp(mut be) => {
            be.left = Box::new(adjust_cmp_ops(*be.left));
            be.right = Box::new(adjust_cmp_ops(*be.right));
            if is_binary_op_cmp(&be.op) && !is_number_expr(&be.right) && is_scalar_expr(&be.left) {
                std::mem::swap(&mut be.left, &mut be.right);
                be.op = get_reverse_cmp_op(&be.op).to_string();
            }
            Expr::BinaryOp(be)
        }
        Expr::Func(mut fe) => {
            fe.args = fe.args.into_iter().map(adjust_cmp_ops).collect();
            Expr::Func(fe)
        }
        Expr::Aggr(mut ae) => {
            ae.args = ae.args.into_iter().map(adjust_cmp_ops).collect();
            Expr::Aggr(ae)
        }
        Expr::Rollup(mut re) => {
            re.expr = Box::new(adjust_cmp_ops(*re.expr));
            if let Some(at) = re.at {
                re.at = Some(Box::new(adjust_cmp_ops(*at)));
            }
            Expr::Rollup(re)
        }
        other => other,
    }
}

fn is_number_expr(e: &Expr) -> bool {
    matches!(e, Expr::Number(_))
}

fn is_scalar_expr(e: &Expr) -> bool {
    if is_number_expr(e) {
        return true;
    }
    if let Expr::Func(fe) = e {
        // time() returns a scalar in PromQL.
        return fe.name.eq_ignore_ascii_case("time");
    }
    false
}

fn get_reverse_cmp_op(op: &str) -> &str {
    match op {
        ">" => "<",
        "<" => ">",
        ">=" => "<=",
        "<=" => ">=",
        // There is no need in changing `==` and `!=`.
        _ => op,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adjust_cmp_ops_swaps_scalar_left() {
        let e = esm_metricsql::parse("0.5 < foo").unwrap();
        let e = adjust_cmp_ops(e);
        let mut s = String::new();
        e.append_string(&mut s);
        assert_eq!(s, "foo > 0.5");

        let e = esm_metricsql::parse("time() >= bar").unwrap();
        let e = adjust_cmp_ops(e);
        let mut s = String::new();
        e.append_string(&mut s);
        assert_eq!(s, "bar <= time()");

        // No swap when the right side is a number.
        let e = esm_metricsql::parse("foo > 0.5").unwrap();
        let e = adjust_cmp_ops(e);
        let mut s = String::new();
        e.append_string(&mut s);
        assert_eq!(s, "foo > 0.5");
    }
}
