//! A faithful, IO-free subset of Go's `text/template` engine.
//!
//! This crate delivers the lexer, parser (AST), executor, the full builtin
//! `FuncMap` (string/format, humanize/time, query/vector + context), and the
//! public [`Template`] API used to parse, validate, and render templates
//! against injected datasource/context state (see [`EvalContext`]).

use std::collections::HashMap;
use std::sync::Arc;

pub mod ast;
pub mod exec;
pub mod funcs;
pub mod lexer;
pub mod parser;
pub mod value;

pub use ast::{Command, Node, Pipeline, Template};
pub use exec::{exec_nodes, FuncFn, Funcs};
pub use funcs::{
    register_builtin_funcs, register_humanize_funcs, register_query_funcs, register_string_funcs,
};
pub use lexer::{lex, Kw, Token};
pub use parser::parse_nodes;
pub use value::{Metric, Value};

/// Error type returned by template lexing, parsing, and execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateError {
    pub msg: String,
}

impl TemplateError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self { msg: msg.into() }
    }
}

impl std::fmt::Display for TemplateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for TemplateError {}

/// The injected datasource query callback: `query_fn(promql_or_metricsql) ->
/// matching metrics`. An `Arc`, not a `Box`: [`default_funcs`] only takes `&
/// EvalContext`, but the [`FuncFn`] closures it builds must be `'static`
/// (that's the implicit bound on `Box<dyn Fn(...) + Send + Sync>` with no
/// explicit lifetime — see `exec::FuncFn`). Since `dyn Fn` trait objects
/// can't be cloned out of a borrow, `query_fn` needs `Arc`'s shared-ownership
/// clone to hand `register_query_funcs` an owned, `'static` handle to call
/// through.
pub type QueryFn = Arc<dyn Fn(&str) -> Result<Vec<Metric>, TemplateError> + Send + Sync>;

/// Datasource/context state threaded into the query and URL builtins
/// (`query`, `externalURL`, `pathPrefix`) at [`default_funcs`] registration
/// time.
pub struct EvalContext {
    pub external_url: String,
    pub path_prefix: String,
    pub query_fn: QueryFn,
}

/// Assembles the full builtin `FuncMap`: 19 Go `text/template` builtins + 14
/// string/format + 10 humanize/time + 9 query/vector + context builtins, with
/// `query`, `externalURL`, and `pathPrefix` wired to `ctx`.
///
/// The Go `text/template` builtins are registered *first* so that a same-named
/// vmalert (or caller) entry registered afterwards overrides them — matching
/// Go, which merges the always-present builtins first and lets the custom
/// FuncMap win on collision.
pub fn default_funcs(ctx: &EvalContext) -> Funcs {
    let mut m: HashMap<String, FuncFn> = HashMap::new();
    register_builtin_funcs(&mut m);
    register_string_funcs(&mut m);
    register_humanize_funcs(&mut m);
    register_query_funcs(&mut m, ctx);
    Funcs(m)
}

/// A validation-only [`EvalContext`]: `query` returns a single empty metric
/// (matching upstream's `templateFuncs` stub, which returns non-empty output
/// so chained functions like `{{ query "x" | first | value }}` validate
/// successfully with no real datasource wired up yet); `externalURL`/
/// `pathPrefix` return the empty string.
fn stub_eval_context() -> EvalContext {
    EvalContext {
        external_url: String::new(),
        path_prefix: String::new(),
        query_fn: Arc::new(|_query: &str| {
            Ok(vec![Metric {
                labels: Default::default(),
                value: 0.0,
                timestamp: 0,
            }])
        }),
    }
}

impl Template {
    /// Lexes and parses `text` into a [`Template`], without executing it.
    pub fn parse(text: &str) -> Result<Template, TemplateError> {
        let toks = lex(text)?;
        let nodes = parse_nodes(&toks)?;
        Ok(Template {
            nodes,
            raw: text.to_string(),
        })
    }

    /// Parses `text`, then executes it once against `Value::Nil` with a
    /// stub `FuncMap` (see [`stub_eval_context`]) to catch parse and
    /// function-reference errors up front — matching upstream's
    /// `newTemplate` + `Execute(io.Discard, nil)` validation pass.
    pub fn parse_and_validate(text: &str) -> Result<Template, TemplateError> {
        let tmpl = Template::parse(text)?;
        let stub_funcs = default_funcs(&stub_eval_context());
        exec_nodes(&tmpl.nodes, &Value::Nil, &Value::Nil, &stub_funcs)?;
        Ok(tmpl)
    }

    /// Executes the template against `data`, using `funcs` as the FuncMap.
    ///
    /// `ctx` is accepted for API symmetry with [`default_funcs`] (which
    /// callers typically use to build `funcs`) but is not read directly
    /// here: any context state a builtin needs was already cloned into its
    /// closure when `funcs` was built.
    pub fn render(
        &self,
        data: &Value,
        funcs: &Funcs,
        _ctx: &EvalContext,
    ) -> Result<String, TemplateError> {
        exec_nodes(&self.nodes, data, data, funcs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_injection_and_render() {
        let ctx = EvalContext {
            external_url: "http://vm".into(),
            path_prefix: "".into(),
            query_fn: Arc::new(|q| {
                assert_eq!(q, "up == 0");
                Ok(vec![Metric {
                    labels: [("instance".to_string(), "h1".to_string())]
                        .into_iter()
                        .collect(),
                    value: 0.0,
                    timestamp: 0,
                }])
            }),
        };
        let funcs = default_funcs(&ctx);
        let tmpl =
            Template::parse(r#"{{ range query "up == 0" }}{{ .Labels.instance }} down{{ end }}"#)
                .unwrap();
        let out = tmpl.render(&Value::Nil, &funcs, &ctx).unwrap();
        assert_eq!(out, "h1 down");
    }

    #[test]
    fn validate_rejects_unknown_func() {
        let err = Template::parse_and_validate("{{ bogusFunc 1 }}").unwrap_err();
        assert!(err.msg.contains("bogusFunc") || err.msg.contains("function"));
    }

    #[test]
    fn validate_accepts_chained_query_builtins_against_the_stub() {
        // Mirrors the upstream `query()` stub returning `[]metric{{}}` so
        // that chained functions like `first`/`value` validate without a
        // real datasource.
        Template::parse_and_validate(r#"{{ query "up" | first | value }}"#).unwrap();
    }

    #[test]
    fn first_on_empty_vector_is_a_render_error() {
        let ctx = EvalContext {
            external_url: String::new(),
            path_prefix: String::new(),
            query_fn: Arc::new(|_q| Ok(vec![])),
        };
        let funcs = default_funcs(&ctx);
        let tmpl = Template::parse(r#"{{ query "x" | first }}"#).unwrap();
        let err = tmpl.render(&Value::Nil, &funcs, &ctx).unwrap_err();
        assert_eq!(err.msg, "first() called on vector with no elements");
    }

    #[test]
    fn default_funcs_registers_exactly_52_functions() {
        let ctx = stub_eval_context();
        let funcs = default_funcs(&ctx);
        assert_eq!(
            funcs.0.len(),
            52,
            "expected 19 gotemplate builtins + 14 string + 10 humanize + 9 query/context funcs"
        );
    }

    #[test]
    fn gotemplate_builtins_are_available_and_render() {
        let ctx = stub_eval_context();
        let funcs = default_funcs(&ctx);
        let cases = [
            (r#"{{ if eq 1 1 }}yes{{ end }}"#, "yes"),
            (r#"{{ printf "%.2f" 1.23456 }}"#, "1.23"),
            (r#"{{ if gt 5.0 1.0 }}big{{ end }}"#, "big"),
            (r#"{{ and 1 2 }}"#, "2"),
        ];
        for (tpl, want) in cases {
            let t = Template::parse(tpl).unwrap();
            assert_eq!(
                t.render(&Value::Nil, &funcs, &ctx).unwrap(),
                want,
                "tpl: {tpl}"
            );
        }
    }

    #[test]
    fn index_and_len_builtins_work_against_data() {
        let ctx = stub_eval_context();
        let funcs = default_funcs(&ctx);
        let mut m = std::collections::BTreeMap::new();
        m.insert("k".to_string(), Value::Str("v".into()));
        let data = Value::Map(
            [
                ("m".to_string(), Value::Map(m)),
                (
                    "s".to_string(),
                    Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
                ),
            ]
            .into_iter()
            .collect(),
        );
        let t = Template::parse(r#"{{ index .m "k" }}/{{ len .s }}"#).unwrap();
        assert_eq!(t.render(&data, &funcs, &ctx).unwrap(), "v/3");
    }

    #[test]
    fn humanize1024_builtin_renders_binary_prefix() {
        let ctx = stub_eval_context();
        let funcs = default_funcs(&ctx);
        let t = Template::parse(r#"{{ humanize1024 2048.0 }}"#).unwrap();
        assert_eq!(t.render(&Value::Nil, &funcs, &ctx).unwrap(), "2ki");
    }
}
