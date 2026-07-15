//! AST types produced by [`crate::parser::parse_nodes`] for the Go
//! `text/template` subset (see `lexer` for the grammar this covers).

/// A single parsed template node.
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    /// Literal text emitted verbatim.
    Text(String),
    /// A `{{ pipeline }}` interpolation.
    Action(Pipeline),
    /// A standalone variable declaration or reassignment action:
    /// `{{ $x := pipe }}` (`declare = true`) or `{{ $x = pipe }}`
    /// (`declare = false`, Go reassignment). Renders as empty output; the
    /// executor binds `$x` to the pipeline's value. A single variable only.
    Assign {
        name: String,
        declare: bool,
        pipe: Pipeline,
    },
    /// `{{if cond}}body{{else}}els{{end}}` (`els` is empty without an `else`).
    If {
        cond: Pipeline,
        body: Vec<Node>,
        els: Vec<Node>,
    },
    /// `{{range [decl :=] pipe}}body{{else}}els{{end}}`.
    ///
    /// `decl` is `Some((index_var, value_var))` when the range declares
    /// loop variables: `$v := .Xs` yields `Some((None, Some("v")))`,
    /// `$i, $v := .Xs` yields `Some((Some("i"), Some("v")))`.
    Range {
        decl: Option<(Option<String>, Option<String>)>,
        pipe: Pipeline,
        body: Vec<Node>,
        els: Vec<Node>,
    },
    /// `{{with pipe}}body{{else}}els{{end}}`.
    With {
        pipe: Pipeline,
        body: Vec<Node>,
        els: Vec<Node>,
    },
}

/// A `|`-joined chain of commands; each stage after the first receives the
/// previous stage's value as an implicit final argument.
#[derive(Debug, Clone, PartialEq)]
pub struct Pipeline {
    pub cmds: Vec<Command>,
}

/// A single pipeline stage.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// A field chain, e.g. `.A.B` -> `Field(vec!["A", "B"])`.
    Field(Vec<String>),
    /// A `$name` variable reference, optionally followed by a field chain:
    /// bare `$x` -> `Var { name: "x", fields: vec![] }`; `$labels.instance`
    /// -> `Var { name: "labels", fields: vec!["instance"] }`; `$.Value`
    /// (root) -> `Var { name: "", fields: vec!["Value"] }`.
    Var {
        name: String,
        fields: Vec<String>,
    },
    /// The bare `.` (current value).
    Dot,
    Str(String),
    Num(f64),
    Bool(bool),
    Nil,
    /// A function call, e.g. `humanize .Value`. When chained after `|`, the
    /// previous stage's value is appended as the final argument.
    Call {
        name: String,
        args: Vec<Pipeline>,
    },
}

/// A parsed template: its node list plus the original source text.
#[derive(Debug, Clone, PartialEq)]
pub struct Template {
    pub(crate) nodes: Vec<Node>,
    pub(crate) raw: String,
}
