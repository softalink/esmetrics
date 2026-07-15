//! Query/vector and context-dependent builtin functions.
//!
//! Reference: `app/vmalert/templates/template.go` (upstream VictoriaMetrics
//! vmalert) — the vector helpers (`first`, `label`, `value`, `strvalue`,
//! `sortByLabel`), the `args` helper, and the datasource/URL-dependent
//! `query`/`externalURL`/`pathPrefix` closures normally substituted in by
//! `FuncsWithQuery`/`funcsWithExternalURL`. Here those three are wired
//! directly to an [`crate::EvalContext`] at registration time instead of
//! being swapped in later.

use std::collections::{BTreeMap, HashMap};

use crate::exec::FuncFn;
use crate::value::{Metric, Value};
use crate::{EvalContext, TemplateError};

/// Registers the 9 query/vector + context builtins into `m`. `query`,
/// `externalURL`, and `pathPrefix` close over data cloned from `ctx` so the
/// resulting [`FuncFn`]s satisfy the `'static` bound (see `crate::EvalContext`
/// docs for why `query_fn` is an `Arc`, not the `Box` a plain per-call
/// closure would need to clone out of a borrowed `&EvalContext`).
pub fn register_query_funcs(m: &mut HashMap<String, FuncFn>, ctx: &EvalContext) {
    let query_fn = ctx.query_fn.clone();
    m.insert(
        "query".to_string(),
        Box::new(move |args| {
            let q = one_str(args, "query")?;
            let metrics = query_fn(&q)?;
            Ok(Value::Vec(metrics))
        }),
    );

    m.insert(
        "first".to_string(),
        Box::new(|args| {
            let metrics = one_vec(args, "first")?;
            metrics
                .first()
                .cloned()
                .map(Value::Metric)
                .ok_or_else(|| TemplateError::new("first() called on vector with no elements"))
        }),
    );

    m.insert(
        "label".to_string(),
        Box::new(|args| {
            let (name, metric) = str_and_metric(args, "label")?;
            Ok(Value::Str(
                metric.labels.get(&name).cloned().unwrap_or_default(),
            ))
        }),
    );

    m.insert(
        "value".to_string(),
        Box::new(|args| Ok(Value::Float(one_metric(args, "value")?.value))),
    );

    m.insert(
        "strvalue".to_string(),
        Box::new(|args| {
            let metric = one_metric(args, "strvalue")?;
            Ok(Value::Str(
                metric.labels.get("__name__").cloned().unwrap_or_default(),
            ))
        }),
    );

    m.insert(
        "sortByLabel".to_string(),
        Box::new(|args| {
            let (name, mut metrics) = str_and_vec(args, "sortByLabel")?;
            // SliceStable in Go -> Rust's `sort_by` is also a stable sort.
            metrics.sort_by(|a, b| {
                a.labels
                    .get(&name)
                    .cloned()
                    .unwrap_or_default()
                    .cmp(&b.labels.get(&name).cloned().unwrap_or_default())
            });
            Ok(Value::Vec(metrics))
        }),
    );

    let external_url = ctx.external_url.clone();
    m.insert(
        "externalURL".to_string(),
        Box::new(move |_args| Ok(Value::Str(external_url.clone()))),
    );

    let path_prefix = ctx.path_prefix.clone();
    m.insert(
        "pathPrefix".to_string(),
        Box::new(move |_args| Ok(Value::Str(path_prefix.clone()))),
    );

    m.insert(
        "args".to_string(),
        Box::new(|args| {
            let map = args
                .iter()
                .enumerate()
                .map(|(i, v)| (format!("arg{i}"), v.clone()))
                .collect::<BTreeMap<_, _>>();
            Ok(Value::Map(map))
        }),
    );
}

fn one_str(args: &[Value], name: &str) -> Result<String, TemplateError> {
    match args {
        [a] => Ok(a.render_string()),
        _ => Err(TemplateError::new(format!(
            "{name}: expected 1 argument, got {}",
            args.len()
        ))),
    }
}

fn one_vec(args: &[Value], name: &str) -> Result<Vec<Metric>, TemplateError> {
    match args {
        [Value::Vec(v)] => Ok(v.clone()),
        [other] => Err(TemplateError::new(format!(
            "{name}: expected a vector argument, got {other:?}"
        ))),
        _ => Err(TemplateError::new(format!(
            "{name}: expected 1 argument, got {}",
            args.len()
        ))),
    }
}

fn one_metric(args: &[Value], name: &str) -> Result<Metric, TemplateError> {
    match args {
        [Value::Metric(m)] => Ok(m.clone()),
        [other] => Err(TemplateError::new(format!(
            "{name}: expected a metric argument, got {other:?}"
        ))),
        _ => Err(TemplateError::new(format!(
            "{name}: expected 1 argument, got {}",
            args.len()
        ))),
    }
}

fn str_and_metric(args: &[Value], name: &str) -> Result<(String, Metric), TemplateError> {
    match args {
        [a, Value::Metric(m)] => Ok((a.render_string(), m.clone())),
        [_, other] => Err(TemplateError::new(format!(
            "{name}: expected a metric argument, got {other:?}"
        ))),
        _ => Err(TemplateError::new(format!(
            "{name}: expected 2 arguments, got {}",
            args.len()
        ))),
    }
}

fn str_and_vec(args: &[Value], name: &str) -> Result<(String, Vec<Metric>), TemplateError> {
    match args {
        [a, Value::Vec(v)] => Ok((a.render_string(), v.clone())),
        [_, other] => Err(TemplateError::new(format!(
            "{name}: expected a vector argument, got {other:?}"
        ))),
        _ => Err(TemplateError::new(format!(
            "{name}: expected 2 arguments, got {}",
            args.len()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn m(pairs: &[(&str, &str)], v: f64) -> Metric {
        Metric {
            labels: pairs
                .iter()
                .map(|(k, x)| (k.to_string(), x.to_string()))
                .collect(),
            value: v,
            timestamp: 0,
        }
    }

    fn ctx_with_query(
        f: impl Fn(&str) -> Result<Vec<Metric>, TemplateError> + Send + Sync + 'static,
    ) -> EvalContext {
        EvalContext {
            external_url: "http://vm".to_string(),
            path_prefix: "/prefix".to_string(),
            query_fn: Arc::new(f),
        }
    }

    fn setup(ctx: &EvalContext) -> HashMap<String, FuncFn> {
        let mut map = HashMap::new();
        register_query_funcs(&mut map, ctx);
        map
    }

    fn call(map: &HashMap<String, FuncFn>, name: &str, args: &[Value]) -> Value {
        map.get(name)
            .unwrap_or_else(|| panic!("{name} not registered"))(args)
        .unwrap_or_else(|e| panic!("{name} call failed: {e}"))
    }

    fn call_err(map: &HashMap<String, FuncFn>, name: &str, args: &[Value]) -> TemplateError {
        map.get(name)
            .unwrap_or_else(|| panic!("{name} not registered"))(args)
        .expect_err("expected an error")
    }

    #[test]
    fn query_invokes_injected_query_fn() {
        let ctx = ctx_with_query(|q| {
            assert_eq!(q, "up == 0");
            Ok(vec![m(&[("instance", "h1")], 0.0)])
        });
        let map = setup(&ctx);
        let out = call(&map, "query", &[Value::Str("up == 0".to_string())]);
        assert_eq!(out, Value::Vec(vec![m(&[("instance", "h1")], 0.0)]));
    }

    #[test]
    fn first_returns_leading_element() {
        let ctx = ctx_with_query(|_| Ok(vec![]));
        let map = setup(&ctx);
        let vec = Value::Vec(vec![m(&[("a", "1")], 1.0), m(&[("a", "2")], 2.0)]);
        assert_eq!(
            call(&map, "first", &[vec]),
            Value::Metric(m(&[("a", "1")], 1.0))
        );
    }

    #[test]
    fn first_errors_on_empty_vector() {
        let ctx = ctx_with_query(|_| Ok(vec![]));
        let map = setup(&ctx);
        let err = call_err(&map, "first", &[Value::Vec(vec![])]);
        assert_eq!(err.msg, "first() called on vector with no elements");
    }

    #[test]
    fn label_value_and_strvalue_read_the_metric() {
        let ctx = ctx_with_query(|_| Ok(vec![]));
        let map = setup(&ctx);
        let metric = Value::Metric(m(&[("__name__", "up"), ("instance", "h1")], 3.5));
        assert_eq!(
            call(
                &map,
                "label",
                &[Value::Str("instance".into()), metric.clone()]
            ),
            Value::Str("h1".into())
        );
        assert_eq!(
            call(&map, "value", std::slice::from_ref(&metric)),
            Value::Float(3.5)
        );
        assert_eq!(call(&map, "strvalue", &[metric]), Value::Str("up".into()));
    }

    #[test]
    fn sort_by_label_is_stable_ascending() {
        let ctx = ctx_with_query(|_| Ok(vec![]));
        let map = setup(&ctx);
        let vec = Value::Vec(vec![m(&[("l", "b")], 1.0), m(&[("l", "a")], 2.0)]);
        assert_eq!(
            call(&map, "sortByLabel", &[Value::Str("l".into()), vec]),
            Value::Vec(vec![m(&[("l", "a")], 2.0), m(&[("l", "b")], 1.0)])
        );
    }

    #[test]
    fn external_url_and_path_prefix_read_from_ctx() {
        let ctx = ctx_with_query(|_| Ok(vec![]));
        let map = setup(&ctx);
        assert_eq!(
            call(&map, "externalURL", &[]),
            Value::Str("http://vm".into())
        );
        assert_eq!(call(&map, "pathPrefix", &[]), Value::Str("/prefix".into()));
    }

    #[test]
    fn args_builds_arg_n_keyed_map() {
        let ctx = ctx_with_query(|_| Ok(vec![]));
        let map = setup(&ctx);
        let out = call(&map, "args", &[Value::Str("x".into()), Value::Int(1)]);
        let expected: BTreeMap<String, Value> = [
            ("arg0".to_string(), Value::Str("x".into())),
            ("arg1".to_string(), Value::Int(1)),
        ]
        .into_iter()
        .collect();
        assert_eq!(out, Value::Map(expected));
    }

    #[test]
    fn wrong_arity_or_type_is_a_template_error_not_a_panic() {
        let ctx = ctx_with_query(|_| Ok(vec![]));
        let map = setup(&ctx);
        let err = call_err(&map, "value", &[]);
        assert!(err.msg.contains("value"), "got: {}", err.msg);
        let err = call_err(&map, "value", &[Value::Str("not-a-metric".into())]);
        assert!(err.msg.contains("value"), "got: {}", err.msg);
    }
}
