//! Executor: walks a parsed [`Node`] list against a data [`Value`] ("dot")
//! and produces the rendered output string.
//!
//! Reference: Go's `text/template/exec.go`. See the module-level docs in
//! `value.rs` for the data model and this crate's task brief for the exact
//! semantics this preserves (`missingkey=zero`, `if`/`with` dot-rebinding,
//! `range` iteration, `$var` lexical scope, and left-to-right pipelines).

use std::collections::HashMap;

use crate::ast::{Command, Node, Pipeline};
use crate::value::Value;
use crate::TemplateError;

/// A template-callable function: takes the already-evaluated argument list
/// (piped values are appended as the final argument) and returns a
/// [`Value`] or an execution error. Built-in functions (Tasks 5-7) and any
/// caller-supplied functions both use this shape.
pub type FuncFn = Box<dyn Fn(&[Value]) -> Result<Value, TemplateError> + Send + Sync>;

/// The function map available to a template execution, keyed by the name
/// used in `{{ name arg1 arg2 }}`.
#[derive(Default)]
pub struct Funcs(pub HashMap<String, FuncFn>);

/// A stack of lexical scopes for `$var` bindings. Entering `if`/`with`/each
/// `range` iteration pushes a frame; leaving it pops the frame. Lookup and
/// reassignment search from the innermost frame outward.
type Scopes = Vec<HashMap<String, Value>>;

/// Executes a parsed node list against `dot`, returning the rendered
/// output. `root` is the top-level data value that bare `$` resolves to
/// (which stays fixed even as `dot` is rebound by `range`/`with`).
pub fn exec_nodes(
    nodes: &[Node],
    dot: &Value,
    root: &Value,
    funcs: &Funcs,
) -> Result<String, TemplateError> {
    let mut scopes: Scopes = vec![HashMap::new()];
    exec_list(nodes, dot, root, funcs, &mut scopes)
}

fn exec_list(
    nodes: &[Node],
    dot: &Value,
    root: &Value,
    funcs: &Funcs,
    scopes: &mut Scopes,
) -> Result<String, TemplateError> {
    let mut out = String::new();
    for node in nodes {
        match node {
            Node::Text(text) => out.push_str(text),
            Node::Action(pipe) => {
                let v = eval_pipeline(pipe, dot, root, funcs, scopes)?;
                out.push_str(&v.render_string());
            }
            Node::Assign {
                name,
                declare,
                pipe,
            } => {
                let v = eval_pipeline(pipe, dot, root, funcs, scopes)?;
                if *declare {
                    scopes
                        .last_mut()
                        .expect("scope stack is never empty")
                        .insert(name.clone(), v);
                } else {
                    reassign_var(scopes, name, v)?;
                }
                // Go: variable declaration/assignment actions produce no
                // output.
            }
            Node::If { cond, body, els } => {
                let v = eval_pipeline(cond, dot, root, funcs, scopes)?;
                let branch = if v.truthy() { body } else { els };
                out.push_str(&exec_scoped(branch, dot, root, funcs, scopes)?);
            }
            Node::With { pipe, body, els } => {
                let v = eval_pipeline(pipe, dot, root, funcs, scopes)?;
                out.push_str(&if v.truthy() {
                    exec_scoped(body, &v, root, funcs, scopes)?
                } else {
                    exec_scoped(els, dot, root, funcs, scopes)?
                });
            }
            Node::Range {
                decl,
                pipe,
                body,
                els,
            } => {
                let v = eval_pipeline(pipe, dot, root, funcs, scopes)?;
                out.push_str(&exec_range(decl, &v, dot, root, funcs, scopes, body, els)?);
            }
        }
    }
    Ok(out)
}

/// Runs `nodes` in a freshly pushed scope frame with `dot` rebound,
/// popping the frame afterward regardless of outcome.
fn exec_scoped(
    nodes: &[Node],
    dot: &Value,
    root: &Value,
    funcs: &Funcs,
    scopes: &mut Scopes,
) -> Result<String, TemplateError> {
    scopes.push(HashMap::new());
    let result = exec_list(nodes, dot, root, funcs, scopes);
    scopes.pop();
    result
}

#[allow(clippy::too_many_arguments)]
fn exec_range(
    decl: &Option<(Option<String>, Option<String>)>,
    collection: &Value,
    dot: &Value,
    root: &Value,
    funcs: &Funcs,
    scopes: &mut Scopes,
    body: &[Node],
    els: &[Node],
) -> Result<String, TemplateError> {
    let items = range_items(collection)?;
    if items.is_empty() {
        return exec_scoped(els, dot, root, funcs, scopes);
    }
    let mut out = String::new();
    for (index, elem) in items {
        scopes.push(HashMap::new());
        if let Some((index_var, value_var)) = decl {
            let frame = scopes.last_mut().expect("just pushed");
            if let Some(name) = index_var {
                frame.insert(name.clone(), index.clone());
            }
            if let Some(name) = value_var {
                frame.insert(name.clone(), elem.clone());
            }
        }
        let result = exec_list(body, &elem, root, funcs, scopes);
        scopes.pop();
        out.push_str(&result?);
    }
    Ok(out)
}

/// Enumerates a range target as `(index, value)` pairs. `Nil` and empty
/// collections yield no items (the `range` else-branch runs); scalar,
/// non-iterable values are a hard error, matching Go's
/// `range can't iterate over ...`.
fn range_items(v: &Value) -> Result<Vec<(Value, Value)>, TemplateError> {
    match v {
        Value::Nil => Ok(Vec::new()),
        Value::Vec(items) => Ok(items
            .iter()
            .enumerate()
            .map(|(i, m)| (Value::Int(i as i64), Value::Metric(m.clone())))
            .collect()),
        Value::List(items) => Ok(items
            .iter()
            .enumerate()
            .map(|(i, item)| (Value::Int(i as i64), item.clone()))
            .collect()),
        // BTreeMap iterates in key order already, matching Go's
        // sorted-by-key map ranging in templates.
        Value::Map(m) => Ok(m
            .iter()
            .map(|(k, val)| (Value::Str(k.clone()), val.clone()))
            .collect()),
        other => Err(TemplateError::new(format!(
            "range can't iterate over {other:?}"
        ))),
    }
}

/// Evaluates a `|`-joined pipeline left to right, feeding each stage's
/// result as the final argument of the next `Call` stage.
fn eval_pipeline(
    pipe: &Pipeline,
    dot: &Value,
    root: &Value,
    funcs: &Funcs,
    scopes: &mut Scopes,
) -> Result<Value, TemplateError> {
    let mut prev: Option<Value> = None;
    for cmd in &pipe.cmds {
        prev = Some(eval_command(cmd, prev, dot, root, funcs, scopes)?);
    }
    prev.ok_or_else(|| TemplateError::new("empty pipeline"))
}

fn eval_command(
    cmd: &Command,
    piped: Option<Value>,
    dot: &Value,
    root: &Value,
    funcs: &Funcs,
    scopes: &mut Scopes,
) -> Result<Value, TemplateError> {
    match cmd {
        Command::Field(names) => {
            let mut v = dot.clone();
            for name in names {
                v = field_access(&v, name)?;
            }
            Ok(v)
        }
        Command::Var { name, fields } => {
            let mut v = lookup_var(scopes, name, root)?;
            for field in fields {
                v = field_access(&v, field)?;
            }
            Ok(v)
        }
        Command::Dot => Ok(dot.clone()),
        Command::Str(s) => Ok(Value::Str(s.clone())),
        Command::Num(n) => Ok(Value::Float(*n)),
        Command::Bool(b) => Ok(Value::Bool(*b)),
        Command::Nil => Ok(Value::Nil),
        Command::Call { name, args } => {
            let mut arg_vals = Vec::with_capacity(args.len() + 1);
            for arg in args {
                arg_vals.push(eval_pipeline(arg, dot, root, funcs, scopes)?);
            }
            if let Some(p) = piped {
                arg_vals.push(p);
            }
            let f = funcs
                .0
                .get(name)
                .ok_or_else(|| TemplateError::new(format!("function {name:?} not defined")))?;
            f(&arg_vals)
        }
    }
}

/// A single field hop (one segment of a `.A.B` chain), applying
/// `missingkey=zero`: an absent map key or unknown `Metric` field yields
/// `Value::Nil` rather than an error. `Nil` itself also chains to `Nil` so a
/// missing hop earlier in the chain doesn't turn a later `.C` into a hard
/// error.
fn field_access(v: &Value, name: &str) -> Result<Value, TemplateError> {
    match v {
        Value::Nil => Ok(Value::Nil),
        Value::Map(m) => Ok(m.get(name).cloned().unwrap_or(Value::Nil)),
        Value::Metric(m) => Ok(match name {
            "Value" => Value::Float(m.value),
            "Labels" => Value::Map(
                m.labels
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
                    .collect(),
            ),
            "Timestamp" => Value::Int(m.timestamp),
            _ => Value::Nil,
        }),
        other => Err(TemplateError::new(format!(
            "can't evaluate field {name:?} in type {other:?}"
        ))),
    }
}

/// Looks up a `$var` reference. The empty name (bare `$`) always resolves
/// to `root`; otherwise the scope stack is searched innermost-first.
fn lookup_var(scopes: &Scopes, name: &str, root: &Value) -> Result<Value, TemplateError> {
    if name.is_empty() {
        return Ok(root.clone());
    }
    for scope in scopes.iter().rev() {
        if let Some(v) = scope.get(name) {
            return Ok(v.clone());
        }
    }
    Err(TemplateError::new(format!("undefined variable {name:?}")))
}

/// Reassignment (`$x = pipe`, `declare = false`): updates the nearest
/// existing binding of `name`, searching innermost-first. Unlike `:=`, it
/// is an error if `name` isn't already bound anywhere on the stack.
fn reassign_var(scopes: &mut Scopes, name: &str, v: Value) -> Result<(), TemplateError> {
    for scope in scopes.iter_mut().rev() {
        if let Some(slot) = scope.get_mut(name) {
            *slot = v;
            return Ok(());
        }
    }
    Err(TemplateError::new(format!("undefined variable {name:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Metric;
    use std::collections::BTreeMap;

    fn render_str(tpl: &str, dot: &Value, funcs: &Funcs) -> String {
        let toks = crate::lexer::lex(tpl).unwrap();
        let nodes = crate::parser::parse_nodes(&toks).unwrap();
        exec_nodes(&nodes, dot, dot, funcs).unwrap()
    }

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

    #[test]
    fn renders_field_and_missingkey_zero() {
        let mut map = BTreeMap::new();
        let mut labels = BTreeMap::new();
        labels.insert("job".into(), Value::Str("api".into()));
        map.insert("Labels".into(), Value::Map(labels));
        map.insert("Value".into(), Value::Float(3.0));
        let dot = Value::Map(map);
        let funcs = Funcs(Default::default());

        let out = render_str(
            "{{ .Value }} {{ .Labels.job }} [{{ .Labels.missing }}]",
            &dot,
            &funcs,
        );
        assert_eq!(out, "3 api []"); // missing key -> empty, no error
    }

    #[test]
    fn range_over_vec_rebinds_dot() {
        let dot = Value::Map(
            [(
                "Xs".to_string(),
                Value::Vec(vec![m(&[("n", "a")], 1.0), m(&[("n", "b")], 2.0)]),
            )]
            .into_iter()
            .collect(),
        );
        let funcs = Funcs(Default::default());
        let out = render_str("{{ range .Xs }}{{ .Value }};{{ end }}", &dot, &funcs);
        assert_eq!(out, "1;2;");
    }

    #[test]
    fn if_else_uses_truthiness() {
        let dot = Value::Map(
            [("A".to_string(), Value::Bool(false))]
                .into_iter()
                .collect(),
        );
        let out = render_str(
            "{{ if .A }}yes{{ else }}no{{ end }}",
            &dot,
            &Funcs(Default::default()),
        );
        assert_eq!(out, "no");
    }

    #[test]
    fn standalone_assign_binds_and_renders_empty() {
        let dot = Value::Map(
            [("Value".to_string(), Value::Float(3.0))]
                .into_iter()
                .collect(),
        );
        let out = render_str(
            "{{ $x := .Value }}{{ $x }}",
            &dot,
            &Funcs(Default::default()),
        );
        assert_eq!(out, "3");
    }

    #[test]
    fn with_rebinds_dot_on_truthy_branch_only() {
        let mut inner = BTreeMap::new();
        inner.insert("Name".to_string(), Value::Str("nested".into()));
        let dot = Value::Map(
            [
                ("Inner".to_string(), Value::Map(inner)),
                ("Empty".to_string(), Value::Str(String::new())),
            ]
            .into_iter()
            .collect(),
        );
        let funcs = Funcs(Default::default());
        assert_eq!(
            render_str(
                "{{ with .Inner }}{{ .Name }}{{ else }}none{{ end }}",
                &dot,
                &funcs
            ),
            "nested"
        );
        assert_eq!(
            render_str(
                "{{ with .Empty }}{{ . }}{{ else }}none{{ end }}",
                &dot,
                &funcs
            ),
            "none"
        );
    }

    #[test]
    fn range_with_index_and_value_vars() {
        let dot = Value::Map(
            [(
                "Xs".to_string(),
                Value::List(vec![Value::Str("a".into()), Value::Str("b".into())]),
            )]
            .into_iter()
            .collect(),
        );
        let out = render_str(
            "{{ range $i, $v := .Xs }}{{ $i }}:{{ $v }};{{ end }}",
            &dot,
            &Funcs(Default::default()),
        );
        assert_eq!(out, "0:a;1:b;");
    }

    #[test]
    fn range_over_empty_uses_else() {
        let dot = Value::Map(
            [("Xs".to_string(), Value::List(vec![]))]
                .into_iter()
                .collect(),
        );
        let out = render_str(
            "{{ range .Xs }}x{{ else }}empty{{ end }}",
            &dot,
            &Funcs(Default::default()),
        );
        assert_eq!(out, "empty");
    }

    #[test]
    fn dollar_resolves_to_root_even_when_with_rebinds_dot() {
        let dot = Value::Str("outer".into());
        let out = render_str(
            r#"{{ with "inner" }}{{ . }}|{{ $ }}{{ end }}"#,
            &dot,
            &Funcs(Default::default()),
        );
        assert_eq!(out, "inner|outer");
    }

    #[test]
    fn var_field_chain_indexes_the_bound_map() {
        // Mirrors vmalert's default preamble: `$labels := .Labels`, then
        // annotations index `{{ $labels.instance }}`.
        let mut labels = BTreeMap::new();
        labels.insert("instance".into(), Value::Str("host-1".into()));
        let dot = Value::Map(
            [("Labels".to_string(), Value::Map(labels))]
                .into_iter()
                .collect(),
        );
        let out = render_str(
            "{{ $labels := .Labels }}{{ $labels.instance }}",
            &dot,
            &Funcs(Default::default()),
        );
        assert_eq!(out, "host-1");
    }

    #[test]
    fn var_field_chain_missing_key_renders_empty() {
        let labels: BTreeMap<String, Value> = BTreeMap::new();
        let dot = Value::Map(
            [("Labels".to_string(), Value::Map(labels))]
                .into_iter()
                .collect(),
        );
        let out = render_str(
            "[{{ $labels := .Labels }}{{ $labels.missing }}]",
            &dot,
            &Funcs(Default::default()),
        );
        assert_eq!(out, "[]");
    }

    #[test]
    fn dollar_root_field_chain_resolves_against_root() {
        let dot = Value::Map(
            [("Value".to_string(), Value::Float(3.0))]
                .into_iter()
                .collect(),
        );
        // `$.Value` inside a range still reaches the root map's Value, not
        // the rebound per-element dot.
        let ranged = Value::Map(
            [
                ("Value".to_string(), Value::Float(3.0)),
                ("Xs".to_string(), Value::List(vec![Value::Str("a".into())])),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(
            render_str("{{ $.Value }}", &dot, &Funcs(Default::default())),
            "3"
        );
        assert_eq!(
            render_str(
                "{{ range .Xs }}{{ $.Value }}{{ end }}",
                &ranged,
                &Funcs(Default::default())
            ),
            "3"
        );
    }

    #[test]
    fn undeclared_variable_reference_is_an_error() {
        let dot = Value::Nil;
        let toks = crate::lexer::lex("{{ $missing }}").unwrap();
        let nodes = crate::parser::parse_nodes(&toks).unwrap();
        let err = exec_nodes(&nodes, &dot, &dot, &Funcs(Default::default())).unwrap_err();
        assert!(err.msg.contains("undefined"), "got: {}", err.msg);
    }

    #[test]
    fn calls_a_registered_function_with_piped_value_as_last_arg() {
        let dot = Value::Map(
            [("Value".to_string(), Value::Float(2.0))]
                .into_iter()
                .collect(),
        );
        let mut funcs_map: HashMap<String, FuncFn> = HashMap::new();
        funcs_map.insert(
            "double".to_string(),
            Box::new(|args: &[Value]| match args {
                [Value::Float(f)] => Ok(Value::Float(f * 2.0)),
                _ => Err(TemplateError::new("double expects one float arg")),
            }),
        );
        let funcs = Funcs(funcs_map);
        let out = render_str("{{ .Value | double }}", &dot, &funcs);
        assert_eq!(out, "4");
    }
}
