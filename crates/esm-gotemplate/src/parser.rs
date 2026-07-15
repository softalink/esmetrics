//! Recursive-descent parser: `&[lexer::Token]` -> `Vec<ast::Node>`.
//!
//! Covers the Go `text/template` subset in scope for this crate: text,
//! `{{ pipeline }}` interpolation, `if`/`else`/`end`, `range`/`else`/`end`,
//! `with`/`else`/`end`, `|`-joined pipelines, parenthesized sub-pipelines for
//! argument grouping, field chains (`.A.B`), `$var` references, `range`
//! variable declarations (`$v := .Xs`, `$i, $v := .Xs`), and literals.

use crate::ast::{Command, Node, Pipeline};
use crate::lexer::{Kw, Token};
use crate::TemplateError;

/// Parses a full token stream (as produced by [`crate::lexer::lex`]) into the
/// top-level node list.
pub fn parse_nodes(toks: &[Token]) -> Result<Vec<Node>, TemplateError> {
    let mut parser = Parser { toks, pos: 0 };
    match parser.parse_list()? {
        (nodes, Stop::Eof) => Ok(nodes),
        (_, Stop::End) => Err(TemplateError::new("unexpected {{end}}: no block is open")),
        (_, Stop::Else) => Err(TemplateError::new("unexpected {{else}}: no block is open")),
    }
}

/// Why [`Parser::parse_list`] stopped collecting nodes.
enum Stop {
    End,
    Else,
    Eof,
}

/// `range` loop-variable declaration: `(index_var, value_var)`. Matches
/// [`Node::Range`]'s `decl` field type.
type RangeDecl = Option<(Option<String>, Option<String>)>;

struct Parser<'a> {
    toks: &'a [Token],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&'a Token> {
        self.toks.get(self.pos)
    }

    fn advance(&mut self) -> Option<&'a Token> {
        let tok = self.toks.get(self.pos);
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    fn expect_right_delim(&mut self) -> Result<(), TemplateError> {
        match self.advance() {
            Some(Token::RightDelim { .. }) => Ok(()),
            other => Err(TemplateError::new(format!(
                "expected end of action, got {other:?}"
            ))),
        }
    }

    /// Parses text nodes and actions until a top-level `{{end}}`, `{{else}}`,
    /// or end of input.
    fn parse_list(&mut self) -> Result<(Vec<Node>, Stop), TemplateError> {
        let mut nodes = Vec::new();
        loop {
            match self.peek() {
                None => return Ok((nodes, Stop::Eof)),
                Some(Token::Text(text)) => {
                    nodes.push(Node::Text(text.clone()));
                    self.advance();
                }
                Some(Token::LeftDelim { .. }) => {
                    self.advance();
                    if let Some(stop) = self.try_parse_end_or_else()? {
                        return Ok((nodes, stop));
                    }
                    nodes.push(self.parse_action()?);
                }
                Some(other) => {
                    return Err(TemplateError::new(format!(
                        "unexpected token outside action: {other:?}"
                    )))
                }
            }
        }
    }

    /// After a `{{` has been consumed, checks for `end`/`else` and consumes
    /// the rest of that action (through `}}`) if found.
    fn try_parse_end_or_else(&mut self) -> Result<Option<Stop>, TemplateError> {
        let stop = match self.peek() {
            Some(Token::Keyword(Kw::End)) => Stop::End,
            Some(Token::Keyword(Kw::Else)) => Stop::Else,
            _ => return Ok(None),
        };
        self.advance();
        self.expect_right_delim()?;
        Ok(Some(stop))
    }

    /// Parses the body of a `{{`..action..`}}` that is not `end`/`else`:
    /// `if`/`range`/`with`, or a plain pipeline interpolation.
    fn parse_action(&mut self) -> Result<Node, TemplateError> {
        match self.peek() {
            Some(Token::Keyword(Kw::If)) => {
                self.advance();
                self.parse_if()
            }
            Some(Token::Keyword(Kw::Range)) => {
                self.advance();
                self.parse_range()
            }
            Some(Token::Keyword(Kw::With)) => {
                self.advance();
                self.parse_with()
            }
            Some(Token::Var(_)) => self.parse_var_action(),
            _ => Ok(Node::Action(self.parse_pipeline_top()?)),
        }
    }

    /// Parses an action that starts with `$var`. If the var is immediately
    /// followed by `:=` (declare) or `=` (reassign), it is a standalone
    /// variable-binding action; otherwise it is ordinary `$var` usage.
    ///
    /// Lookahead consumes no tokens until the shape is certain (same
    /// discipline as [`Parser::parse_range_decl`]). A multi-var standalone
    /// declaration (`$i, $v := ...`) is rejected — that form is range-only.
    fn parse_var_action(&mut self) -> Result<Node, TemplateError> {
        let Some(Token::Var(name)) = self.peek() else {
            unreachable!("parse_var_action entered without a leading Var token");
        };
        let name = name.clone();
        match self.toks.get(self.pos + 1) {
            Some(Token::Declare | Token::Assign) => {
                let declare = matches!(self.toks.get(self.pos + 1), Some(Token::Declare));
                self.pos += 2; // `$name` and `:=`/`=`
                let pipe = self.parse_pipeline_top()?;
                Ok(Node::Assign {
                    name,
                    declare,
                    pipe,
                })
            }
            Some(Token::Comma) => Err(TemplateError::new(
                "multi-variable declaration is only allowed in a range action",
            )),
            // Plain `$var` usage: parse as a normal pipeline.
            _ => Ok(Node::Action(self.parse_pipeline_top()?)),
        }
    }

    /// Parses `body{{else}}els{{end}}` or `body{{end}}` (`els` is empty
    /// without an `else`); the opening action's `}}` must already be
    /// consumed.
    fn parse_body_and_else(&mut self) -> Result<(Vec<Node>, Vec<Node>), TemplateError> {
        let (body, stop) = self.parse_list()?;
        match stop {
            Stop::End => Ok((body, Vec::new())),
            Stop::Else => match self.parse_list()? {
                (els, Stop::End) => Ok((body, els)),
                (_, Stop::Else) => Err(TemplateError::new("unexpected second {{else}}")),
                (_, Stop::Eof) => Err(TemplateError::new("unclosed block: missing {{end}}")),
            },
            Stop::Eof => Err(TemplateError::new("unclosed block: missing {{end}}")),
        }
    }

    fn parse_if(&mut self) -> Result<Node, TemplateError> {
        let cond = self.parse_pipeline_top()?;
        let (body, els) = self.parse_body_and_else()?;
        Ok(Node::If { cond, body, els })
    }

    fn parse_with(&mut self) -> Result<Node, TemplateError> {
        let pipe = self.parse_pipeline_top()?;
        let (body, els) = self.parse_body_and_else()?;
        Ok(Node::With { pipe, body, els })
    }

    fn parse_range(&mut self) -> Result<Node, TemplateError> {
        let decl = self.parse_range_decl()?;
        let pipe = self.parse_pipeline_top()?;
        let (body, els) = self.parse_body_and_else()?;
        Ok(Node::Range {
            decl,
            pipe,
            body,
            els,
        })
    }

    /// Parses the optional `$v :=` / `$i, $v :=` declaration prefix of a
    /// `range` action, via lookahead that only consumes tokens once it is
    /// certain a declaration (not a bare `$var` pipeline) is present.
    fn parse_range_decl(&mut self) -> Result<RangeDecl, TemplateError> {
        let Some(Token::Var(first)) = self.peek() else {
            return Ok(None);
        };
        let first = first.clone();
        match self.toks.get(self.pos + 1) {
            Some(Token::Declare) => {
                self.pos += 2; // `$v` `:=`
                Ok(Some((None, Some(first))))
            }
            Some(Token::Comma) => {
                let Some(Token::Var(second)) = self.toks.get(self.pos + 2) else {
                    return Err(TemplateError::new(
                        "expected a second '$var' after ',' in range declaration",
                    ));
                };
                let second = second.clone();
                if !matches!(self.toks.get(self.pos + 3), Some(Token::Declare)) {
                    return Err(TemplateError::new(
                        "expected ':=' after range variable declaration",
                    ));
                }
                self.pos += 4; // `$i` `,` `$v` `:=`
                Ok(Some((Some(first), Some(second))))
            }
            // Not a declaration: leave the `$var` token for the pipeline parse.
            _ => Ok(None),
        }
    }

    /// Parses a pipeline that ends the enclosing action, consuming its
    /// terminating `}}`.
    fn parse_pipeline_top(&mut self) -> Result<Pipeline, TemplateError> {
        let cmds = self.parse_cmd_list()?;
        self.expect_right_delim()?;
        Ok(Pipeline { cmds })
    }

    /// Parses a parenthesized sub-pipeline used to group a call argument;
    /// the opening `(` must already be consumed. Consumes the matching `)`.
    fn parse_pipeline_paren(&mut self) -> Result<Pipeline, TemplateError> {
        let cmds = self.parse_cmd_list()?;
        match self.advance() {
            Some(Token::RParen) => Ok(Pipeline { cmds }),
            other => Err(TemplateError::new(format!("expected ')', got {other:?}"))),
        }
    }

    fn parse_cmd_list(&mut self) -> Result<Vec<Command>, TemplateError> {
        let mut cmds = vec![self.parse_command()?];
        while matches!(self.peek(), Some(Token::Pipe)) {
            self.advance();
            cmds.push(self.parse_command()?);
        }
        Ok(cmds)
    }

    /// Parses one pipeline stage: a function call (identifier plus its
    /// arguments) or a bare operand (field chain, var, dot, or literal).
    fn parse_command(&mut self) -> Result<Command, TemplateError> {
        let Some(Token::Ident(name)) = self.peek() else {
            return self.parse_bare_operand();
        };
        let name = name.clone();
        self.advance();
        let mut args = Vec::new();
        while self.at_arg_start() {
            args.push(self.parse_arg()?);
        }
        Ok(Command::Call { name, args })
    }

    fn at_arg_start(&self) -> bool {
        matches!(
            self.peek(),
            Some(
                Token::Field(_)
                    | Token::Var(_)
                    | Token::Dot
                    | Token::String(_)
                    | Token::Number(_)
                    | Token::Bool(_)
                    | Token::Nil
                    | Token::Ident(_)
                    | Token::LParen
            )
        )
    }

    /// Parses one call argument: a parenthesized sub-pipeline, a zero-arg
    /// function reference, or a bare operand.
    fn parse_arg(&mut self) -> Result<Pipeline, TemplateError> {
        if matches!(self.peek(), Some(Token::LParen)) {
            self.advance();
            return self.parse_pipeline_paren();
        }
        if let Some(Token::Ident(name)) = self.peek() {
            let name = name.clone();
            self.advance();
            return Ok(Pipeline {
                cmds: vec![Command::Call {
                    name,
                    args: Vec::new(),
                }],
            });
        }
        Ok(Pipeline {
            cmds: vec![self.parse_bare_operand()?],
        })
    }

    /// Parses a single non-call operand: a field chain (consecutive `Field`
    /// tokens collapse into one chain), a `$var`, `.`, or a literal.
    fn parse_bare_operand(&mut self) -> Result<Command, TemplateError> {
        match self.advance() {
            Some(Token::Field(first)) => {
                let mut fields = vec![first.clone()];
                while let Some(Token::Field(next)) = self.peek() {
                    fields.push(next.clone());
                    self.advance();
                }
                Ok(Command::Field(fields))
            }
            Some(Token::Var(name)) => {
                let name = name.clone();
                let mut fields = Vec::new();
                while let Some(Token::Field(next)) = self.peek() {
                    fields.push(next.clone());
                    self.advance();
                }
                Ok(Command::Var { name, fields })
            }
            Some(Token::Dot) => Ok(Command::Dot),
            Some(Token::String(s)) => Ok(Command::Str(s.clone())),
            Some(Token::Number(n)) => Ok(Command::Num(*n)),
            Some(Token::Bool(b)) => Ok(Command::Bool(*b)),
            Some(Token::Nil) => Ok(Command::Nil),
            other => Err(TemplateError::new(format!(
                "unexpected token in pipeline: {other:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::lex;

    #[test]
    fn parses_if_else() {
        let toks = lex("{{if .A}}x{{else}}y{{end}}").unwrap();
        let nodes = parse_nodes(&toks).unwrap();
        match &nodes[0] {
            Node::If { body, els, .. } => {
                assert!(matches!(body[0], Node::Text(ref t) if t == "x"));
                assert!(matches!(els[0], Node::Text(ref t) if t == "y"));
            }
            _ => panic!("expected If"),
        }
    }

    #[test]
    fn parses_range_with_two_vars() {
        let toks = lex("{{range $i, $v := .Xs}}{{$v}}{{end}}").unwrap();
        let nodes = parse_nodes(&toks).unwrap();
        match &nodes[0] {
            Node::Range {
                decl: Some((Some(i), Some(v))),
                ..
            } => {
                assert_eq!(i, "i");
                assert_eq!(v, "v");
            }
            _ => panic!("expected Range with 2 vars"),
        }
    }

    #[test]
    fn parses_pipeline_of_calls() {
        let toks = lex("{{ humanize .Value | toUpper }}").unwrap();
        let nodes = parse_nodes(&toks).unwrap();
        match &nodes[0] {
            Node::Action(p) => {
                assert_eq!(p.cmds.len(), 2);
                assert!(matches!(&p.cmds[0], Command::Call{ name, .. } if name == "humanize"));
                assert!(matches!(&p.cmds[1], Command::Call{ name, .. } if name == "toUpper"));
            }
            _ => panic!("expected Action"),
        }
    }

    #[test]
    fn parses_range_with_single_var_and_no_else() {
        let toks = lex("{{range $v := .Xs}}{{$v}}{{end}}").unwrap();
        let nodes = parse_nodes(&toks).unwrap();
        match &nodes[0] {
            Node::Range { decl, els, .. } => {
                assert_eq!(decl, &Some((None, Some("v".to_string()))));
                assert!(els.is_empty());
            }
            _ => panic!("expected Range with 1 var"),
        }
    }

    #[test]
    fn parses_range_without_decl() {
        let toks = lex("{{range .Xs}}{{.}}{{end}}").unwrap();
        let nodes = parse_nodes(&toks).unwrap();
        match &nodes[0] {
            Node::Range { decl, pipe, .. } => {
                assert_eq!(decl, &None);
                assert_eq!(pipe.cmds, vec![Command::Field(vec!["Xs".to_string()])]);
            }
            _ => panic!("expected Range without decl"),
        }
    }

    #[test]
    fn parses_with_else() {
        let toks = lex("{{with .A}}x{{else}}y{{end}}").unwrap();
        let nodes = parse_nodes(&toks).unwrap();
        match &nodes[0] {
            Node::With { body, els, .. } => {
                assert!(matches!(body[0], Node::Text(ref t) if t == "x"));
                assert!(matches!(els[0], Node::Text(ref t) if t == "y"));
            }
            _ => panic!("expected With"),
        }
    }

    #[test]
    fn parses_field_chain_as_one_command() {
        let toks = lex("{{ .A.B }}").unwrap();
        let nodes = parse_nodes(&toks).unwrap();
        match &nodes[0] {
            Node::Action(p) => {
                assert_eq!(
                    p.cmds,
                    vec![Command::Field(vec!["A".to_string(), "B".to_string()])]
                );
            }
            _ => panic!("expected Action"),
        }
    }

    #[test]
    fn parses_parenthesized_sub_pipeline_argument() {
        let toks = lex("{{ printf (add 1 2) }}").unwrap();
        let nodes = parse_nodes(&toks).unwrap();
        match &nodes[0] {
            Node::Action(p) => match &p.cmds[0] {
                Command::Call { name, args } => {
                    assert_eq!(name, "printf");
                    assert_eq!(args.len(), 1);
                    assert_eq!(
                        args[0].cmds,
                        vec![Command::Call {
                            name: "add".to_string(),
                            args: vec![
                                Pipeline {
                                    cmds: vec![Command::Num(1.0)]
                                },
                                Pipeline {
                                    cmds: vec![Command::Num(2.0)]
                                },
                            ],
                        }]
                    );
                }
                other => panic!("expected Call, got {other:?}"),
            },
            _ => panic!("expected Action"),
        }
    }

    #[test]
    fn nested_if_inside_range() {
        let toks = lex("{{range .Xs}}{{if .A}}x{{end}}{{end}}").unwrap();
        let nodes = parse_nodes(&toks).unwrap();
        match &nodes[0] {
            Node::Range { body, els, .. } => {
                assert!(els.is_empty());
                assert!(matches!(body[0], Node::If { .. }));
            }
            _ => panic!("expected Range"),
        }
    }

    #[test]
    fn missing_end_is_an_error_not_a_panic() {
        let toks = lex("{{if .A}}x").unwrap();
        let err = parse_nodes(&toks).unwrap_err();
        assert!(err.msg.contains("missing"), "got: {}", err.msg);
    }

    #[test]
    fn stray_else_is_an_error() {
        let toks = lex("{{else}}").unwrap();
        let err = parse_nodes(&toks).unwrap_err();
        assert!(err.msg.contains("else"), "got: {}", err.msg);
    }

    #[test]
    fn stray_end_is_an_error() {
        let toks = lex("{{end}}").unwrap();
        let err = parse_nodes(&toks).unwrap_err();
        assert!(err.msg.contains("end"), "got: {}", err.msg);
    }

    #[test]
    fn parses_standalone_var_declaration() {
        let toks = lex("{{ $value := .Value }}").unwrap();
        let nodes = parse_nodes(&toks).unwrap();
        match &nodes[0] {
            Node::Assign {
                name,
                declare,
                pipe,
            } => {
                assert_eq!(name, "value");
                assert!(*declare);
                assert_eq!(pipe.cmds, vec![Command::Field(vec!["Value".to_string()])]);
            }
            other => panic!("expected Assign, got {other:?}"),
        }
    }

    #[test]
    fn plain_var_usage_is_not_a_declaration() {
        let toks = lex("{{ $value }}").unwrap();
        let nodes = parse_nodes(&toks).unwrap();
        match &nodes[0] {
            Node::Action(p) => {
                assert_eq!(
                    p.cmds,
                    vec![Command::Var {
                        name: "value".to_string(),
                        fields: vec![],
                    }]
                );
            }
            other => panic!("expected Action, got {other:?}"),
        }
    }

    #[test]
    fn parses_var_with_field_chain() {
        let toks = lex("{{ $labels.instance }}").unwrap();
        let nodes = parse_nodes(&toks).unwrap();
        match &nodes[0] {
            Node::Action(p) => {
                assert_eq!(
                    p.cmds,
                    vec![Command::Var {
                        name: "labels".to_string(),
                        fields: vec!["instance".to_string()],
                    }]
                );
            }
            other => panic!("expected Action, got {other:?}"),
        }
    }

    #[test]
    fn parses_dollar_root_with_field() {
        let toks = lex("{{ $.Value }}").unwrap();
        let nodes = parse_nodes(&toks).unwrap();
        match &nodes[0] {
            Node::Action(p) => {
                assert_eq!(
                    p.cmds,
                    vec![Command::Var {
                        name: String::new(),
                        fields: vec!["Value".to_string()],
                    }]
                );
            }
            other => panic!("expected Action, got {other:?}"),
        }
    }

    #[test]
    fn parses_var_declaration_with_pipeline() {
        let toks = lex("{{ $x := humanize .Value }}").unwrap();
        let nodes = parse_nodes(&toks).unwrap();
        match &nodes[0] {
            Node::Assign {
                name,
                declare,
                pipe,
            } => {
                assert_eq!(name, "x");
                assert!(*declare);
                assert_eq!(pipe.cmds.len(), 1);
                assert!(matches!(&pipe.cmds[0], Command::Call{ name, args }
                    if name == "humanize" && args.len() == 1));
            }
            other => panic!("expected Assign, got {other:?}"),
        }
    }

    #[test]
    fn parses_var_reassignment() {
        let toks = lex("{{ $x = .Value }}").unwrap();
        let nodes = parse_nodes(&toks).unwrap();
        match &nodes[0] {
            Node::Assign { name, declare, .. } => {
                assert_eq!(name, "x");
                assert!(!declare, "'=' is reassignment, not declaration");
            }
            other => panic!("expected Assign, got {other:?}"),
        }
    }

    #[test]
    fn standalone_multi_var_declaration_is_an_error() {
        let toks = lex("{{ $i, $v := .Xs }}").unwrap();
        let err = parse_nodes(&toks).unwrap_err();
        assert!(err.msg.contains("range"), "got: {}", err.msg);
    }

    #[test]
    fn malformed_range_decl_is_an_error() {
        // `$i,` with no second var and no `:=`.
        let toks = lex("{{range $i, .Xs}}{{end}}").unwrap();
        let err = parse_nodes(&toks).unwrap_err();
        assert!(err.msg.contains("range"), "got: {}", err.msg);
    }
}
