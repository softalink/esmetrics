//! MetricsQL recursive-descent parser.
//!
//! Port of the parsing half of `parser.go` (the AST types live in
//! [`crate::ast`], WITH-template expansion in [`crate::withexpand`]).

use crate::ast::{
    is_prefix_modifier, AggrFuncExpr, BinaryOpExpr, DurationExpr, Expr, FuncExpr, LabelFilterExpr,
    MetricExpr, ModifierExpr, NumberExpr, ParensExpr, RollupExpr, StringExpr, WithArgExpr,
    WithExpr,
};
use crate::binaryop::{
    binary_op_eval_number, binary_op_priority, is_binary_op, is_binary_op_bool_modifier,
    is_binary_op_cmp, is_binary_op_group_modifier, is_binary_op_join_modifier,
    is_binary_op_logical_set, is_right_associative_binary_op,
};
use crate::funcs::{is_aggr_func, is_aggr_func_modifier};
use crate::lexer::{
    is_eof, is_ident_prefix, is_inf_or_nan, is_offset, is_positive_duration,
    is_positive_number_prefix, is_string_prefix, parse_positive_number, unescape_ident, Lexer,
};
use crate::strutil::quote_string;
use crate::withexpand::{default_with_arg_exprs, expand_with_expr};
use crate::{ParseError, Result};
use std::sync::Arc;

/// Parses the MetricsQL query `s`.
///
/// All `WITH` expressions are expanded in the returned [`Expr`].
/// MetricsQL is backwards-compatible with PromQL.
///
/// Port of Go `Parse`.
pub fn parse(s: &str) -> Result<Expr> {
    let e = parse_internal(s)?;
    // Expand `WITH` expressions.
    let was = default_with_arg_exprs();
    let e = expand_with_expr(was, &e)
        .map_err(|err| ParseError::new(format!("cannot expand WITH expressions: {err}")))?;
    let e = remove_parens_expr(e);
    let e = simplify_constants(e);
    crate::utils::check_supported_functions(&e)?;
    Ok(e)
}

/// Port of Go `parseInternal`.
pub(crate) fn parse_internal(s: &str) -> Result<Expr> {
    let mut p = Parser { lex: Lexer::new(s) };
    p.lex
        .next()
        .map_err(|err| ParseError::new(format!("cannot find the first token: {err}")))?;
    let e = p
        .parse_expr()
        .map_err(|err| ParseError::new(format!("{err}; unparsed data: {:?}", p.lex.context())))?;
    if !is_eof(&p.lex.token) {
        return Err(ParseError::new(format!(
            "unparsed data left: {:?}",
            p.lex.context()
        )));
    }
    Ok(e)
}

/// Port of Go `mustParseWithArgExpr`.
pub(crate) fn must_parse_with_arg_expr(s: &str) -> WithArgExpr {
    let mut p = Parser { lex: Lexer::new(s) };
    p.lex
        .next()
        .unwrap_or_else(|err| panic!("BUG: cannot find the first token in {s:?}: {err}"));
    p.parse_with_arg_expr().unwrap_or_else(|err| {
        panic!(
            "BUG: cannot parse {s:?}: {err}; unparsed data: {:?}",
            p.lex.context()
        )
    })
}

/// Port of Go `checkDuplicateWithArgNames`.
pub(crate) fn check_duplicate_with_arg_names(was: &[Arc<WithArgExpr>]) -> Result<()> {
    let mut seen: std::collections::HashMap<&str, &Arc<WithArgExpr>> =
        std::collections::HashMap::with_capacity(was.len());
    for wa in was {
        if let Some(wa_old) = seen.get(wa.name.as_str()) {
            let mut old = String::new();
            wa_old.append_string(&mut old);
            let mut new = String::new();
            wa.append_string(&mut new);
            return Err(ParseError::new(format!(
                "duplicate `with` arg name for: {new}; previous one: {old}"
            )));
        }
        seen.insert(&wa.name, wa);
    }
    Ok(())
}

/// Port of Go `isWith`.
fn is_with(s: &str) -> bool {
    s.eq_ignore_ascii_case("with")
}

/// Port of Go `isKeepMetricNames`.
fn is_keep_metric_names(token: &str) -> bool {
    token.eq_ignore_ascii_case("keep_metric_names")
}

/// Port of Go `isQuotedString`.
fn is_quoted_string(s: &str) -> bool {
    is_string_prefix(s) && matches!(s.as_bytes().last(), Some(b'"' | b'\'' | b'`'))
}

/// Port of Go `isRollupStartToken`.
fn is_rollup_start_token(token: &str) -> bool {
    token == "[" || token == "@" || is_offset(token)
}

/// The MetricsQL parser.
///
/// Preconditions for all `parse_*` methods: `lex.token` points to the first
/// token to parse. Postconditions: `lex.token` points to the next token
/// after the parsed one.
pub(crate) struct Parser {
    lex: Lexer,
}

impl Parser {
    /// Port of Go `parser.parseWithExpr`: parses `WITH (withArgExpr...) expr`.
    fn parse_with_expr(&mut self) -> Result<Expr> {
        if !is_with(&self.lex.token) {
            return Err(ParseError::new(format!(
                "withExpr: unexpected token {:?}; want `WITH`",
                self.lex.token
            )));
        }
        self.lex.next()?;
        if self.lex.token != "(" {
            return Err(ParseError::new(format!(
                "withExpr: unexpected token {:?}; want \"(\"",
                self.lex.token
            )));
        }
        let mut was: Vec<Arc<WithArgExpr>> = Vec::new();
        loop {
            self.lex.next()?;
            if self.lex.token == ")" {
                break;
            }
            let wa = self.parse_with_arg_expr()?;
            was.push(Arc::new(wa));
            match self.lex.token.as_str() {
                "," => continue,
                ")" => break,
                _ => {
                    return Err(ParseError::new(format!(
                        "withExpr: unexpected token {:?}; want \",\", \")\"",
                        self.lex.token
                    )));
                }
            }
        }
        check_duplicate_with_arg_names(&was)?;
        self.lex.next()?;
        let e = self.parse_expr()?;
        Ok(Expr::With(WithExpr {
            was,
            expr: Box::new(e),
        }))
    }

    /// Port of Go `parser.parseWithArgExpr`.
    fn parse_with_arg_expr(&mut self) -> Result<WithArgExpr> {
        if !is_ident_prefix(&self.lex.token) {
            return Err(ParseError::new(format!(
                "withArgExpr: unexpected token {:?}; want \"ident\"",
                self.lex.token
            )));
        }
        let name = unescape_ident(&self.lex.token);
        self.lex.next()?;
        let mut args: Vec<String> = Vec::new();
        if self.lex.token == "(" {
            // Parse func args.
            args = self.parse_ident_list(false).map_err(|err| {
                ParseError::new(format!(
                    "withArgExpr: cannot parse args for {name:?}: {err}"
                ))
            })?;
            // Make sure all the args have different names.
            let mut seen = std::collections::HashSet::with_capacity(args.len());
            for arg in &args {
                if !seen.insert(arg.as_str()) {
                    return Err(ParseError::new(format!(
                        "withArgExpr: duplicate func arg found in {name:?}: {arg:?}"
                    )));
                }
            }
        }
        if self.lex.token != "=" {
            return Err(ParseError::new(format!(
                "withArgExpr: unexpected token {:?}; want \"=\"",
                self.lex.token
            )));
        }
        self.lex.next()?;
        let expr = self
            .parse_expr()
            .map_err(|err| ParseError::new(format!("withArgExpr: cannot parse {name:?}: {err}")))?;
        Ok(WithArgExpr { name, args, expr })
    }

    /// Port of Go `parser.parseExpr`.
    fn parse_expr(&mut self) -> Result<Expr> {
        let mut e = self.parse_single_expr()?;
        loop {
            if !is_binary_op(&self.lex.token) {
                return Ok(e);
            }

            let op = self.lex.token.to_ascii_lowercase();
            self.lex.next()?;
            let mut bool_modifier = false;
            let mut group_modifier = ModifierExpr::default();
            let mut join_modifier = ModifierExpr::default();
            let mut join_modifier_prefix = None;
            if is_binary_op_bool_modifier(&self.lex.token) {
                if !is_binary_op_cmp(&op) {
                    return Err(ParseError::new(format!(
                        "bool modifier cannot be applied to {op:?}"
                    )));
                }
                bool_modifier = true;
                self.lex.next()?;
            }
            if is_binary_op_group_modifier(&self.lex.token) {
                group_modifier = self.parse_modifier_expr(false)?;
                if is_binary_op_join_modifier(&self.lex.token) {
                    if is_binary_op_logical_set(&op) {
                        return Err(ParseError::new(format!(
                            "modifier {:?} cannot be applied to {op:?}",
                            self.lex.token
                        )));
                    }
                    join_modifier = self.parse_modifier_expr(true)?;
                    if is_prefix_modifier(&self.lex.token) {
                        let mut jm = String::new();
                        join_modifier.append_string(&mut jm);
                        self.lex.next().map_err(|err| {
                            ParseError::new(format!("cannot read prefix for {jm}: {err}"))
                        })?;
                        let se = self.parse_string_expr().map_err(|err| {
                            ParseError::new(format!("cannot parse prefix for {jm}: {err}"))
                        })?;
                        join_modifier_prefix = Some(se);
                    }
                }
            }
            let e2 = self.parse_single_expr()?;
            let mut be = BinaryOpExpr {
                op,
                bool_modifier,
                group_modifier,
                join_modifier,
                join_modifier_prefix,
                keep_metric_names: false,
                left: Box::new(e),
                right: Box::new(e2),
            };
            if is_keep_metric_names(&self.lex.token) {
                be.keep_metric_names = true;
                self.lex.next()?;
            }
            e = balance_binary_op(be);
        }
    }

    /// Port of Go `parser.parseSingleExpr` (non-binaryOp expressions).
    fn parse_single_expr(&mut self) -> Result<Expr> {
        if is_with(&self.lex.token) {
            let res = self.lex.next();
            let next_token = self.lex.token.clone();
            self.lex.prev();
            if res.is_ok() && next_token == "(" {
                return self.parse_with_expr();
            }
        }
        let e = self.parse_single_expr_without_rollup_suffix()?;
        if !is_rollup_start_token(&self.lex.token) {
            // There is no rollup expression.
            return Ok(e);
        }
        self.parse_rollup_expr(e)
    }

    /// Port of Go `parser.parseSingleExprWithoutRollupSuffix`.
    fn parse_single_expr_without_rollup_suffix(&mut self) -> Result<Expr> {
        if is_positive_duration(&self.lex.token) {
            return Ok(Expr::Duration(self.parse_positive_duration()?));
        }
        if is_string_prefix(&self.lex.token) {
            return Ok(Expr::String(self.parse_string_expr()?));
        }
        if is_positive_number_prefix(&self.lex.token) || is_inf_or_nan(&self.lex.token) {
            return Ok(Expr::Number(self.parse_positive_number_expr()?));
        }
        if is_ident_prefix(&self.lex.token) {
            return self.parse_ident_expr();
        }
        match self.lex.token.as_str() {
            "(" => Ok(Expr::Parens(self.parse_parens_expr()?)),
            "{" => Ok(Expr::Metric(self.parse_metric_expr()?)),
            "-" => {
                // Unary minus. Substitute `-expr` with `0 - expr`.
                self.lex.next()?;
                let e = self.parse_single_expr()?;
                Ok(Expr::BinaryOp(BinaryOpExpr {
                    op: "-".to_string(),
                    bool_modifier: false,
                    group_modifier: ModifierExpr::default(),
                    join_modifier: ModifierExpr::default(),
                    join_modifier_prefix: None,
                    keep_metric_names: false,
                    left: Box::new(Expr::Number(NumberExpr {
                        n: 0.0,
                        s: String::new(),
                    })),
                    right: Box::new(e),
                }))
            }
            "+" => {
                // Unary plus.
                self.lex.next()?;
                self.parse_single_expr()
            }
            _ => Err(ParseError::new(format!(
                "singleExpr: unexpected token {:?}; want \"(\", \"{{\", \"-\", \"+\"",
                self.lex.token
            ))),
        }
    }

    /// Port of Go `parser.parsePositiveNumberExpr`.
    fn parse_positive_number_expr(&mut self) -> Result<NumberExpr> {
        if !is_positive_number_prefix(&self.lex.token) && !is_inf_or_nan(&self.lex.token) {
            return Err(ParseError::new(format!(
                "positiveNumberExpr: unexpected token {:?}; want \"number\"",
                self.lex.token
            )));
        }
        let s = self.lex.token.clone();
        let n = parse_positive_number(&s).map_err(|err| {
            ParseError::new(format!("positivenumberExpr: cannot parse {s:?}: {err}"))
        })?;
        self.lex.next()?;
        Ok(NumberExpr { n, s })
    }

    /// Port of Go `parser.parseStringExpr`.
    fn parse_string_expr(&mut self) -> Result<StringExpr> {
        let mut se = StringExpr::default();
        loop {
            if is_string_prefix(&self.lex.token) || is_ident_prefix(&self.lex.token) {
                se.tokens.push(self.lex.token.clone());
            } else {
                return Err(ParseError::new(format!(
                    "StringExpr: unexpected token {:?}; want \"string\"",
                    self.lex.token
                )));
            }
            self.lex.next()?;
            if self.lex.token != "+" {
                return Ok(se);
            }

            // Composite StringExpr like `"s1" + "s2"`, `"s" + m()`,
            // `"s" + m{}` or `"s" + unknownToken`.
            self.lex.next()?;
            if is_string_prefix(&self.lex.token) {
                // "s1" + "s2"
                continue;
            }
            if !is_ident_prefix(&self.lex.token) {
                // "s" + unknownToken
                self.lex.prev();
                return Ok(se);
            }
            // Look after ident.
            self.lex.next()?;
            if self.lex.token == "(" || self.lex.token == "{" {
                // `"s" + m(` or `"s" + m{`
                self.lex.prev();
                self.lex.prev();
                return Ok(se);
            }
            // "s" + ident
            self.lex.prev();
        }
    }

    /// Port of Go `parser.parseParensExpr`.
    fn parse_parens_expr(&mut self) -> Result<ParensExpr> {
        if self.lex.token != "(" {
            return Err(ParseError::new(format!(
                "parensExpr: unexpected token {:?}; want \"(\"",
                self.lex.token
            )));
        }
        let mut exprs: Vec<Expr> = Vec::new();
        loop {
            self.lex.next()?;
            if self.lex.token == ")" {
                break;
            }
            let expr = self.parse_expr()?;
            exprs.push(expr);
            if self.lex.token == "," {
                continue;
            }
            if self.lex.token == ")" {
                break;
            }
            return Err(ParseError::new(format!(
                "parensExpr: unexpected token {:?}; want \",\" or \")\"",
                self.lex.token
            )));
        }
        self.lex.next()?;
        if exprs.len() == 1 {
            if let Expr::BinaryOp(be) = &mut exprs[0] {
                if is_keep_metric_names(&self.lex.token) {
                    self.lex.next()?;
                    be.keep_metric_names = true;
                }
            }
        }
        Ok(ParensExpr(exprs))
    }

    /// Port of Go `parser.parseAggrFuncExpr`.
    fn parse_aggr_func_expr(&mut self) -> Result<AggrFuncExpr> {
        if !is_aggr_func(&self.lex.token) {
            return Err(ParseError::new(format!(
                "AggrFuncExpr: unexpected token {:?}; want aggregate func",
                self.lex.token
            )));
        }

        let mut ae = AggrFuncExpr {
            name: unescape_ident(&self.lex.token).to_ascii_lowercase(),
            ..Default::default()
        };
        self.lex.next()?;
        if is_ident_prefix(&self.lex.token) {
            // Func modifier prefix, e.g. `sum by (...) (...)`.
            if !is_aggr_func_modifier(&self.lex.token) {
                return Err(ParseError::new(format!(
                    "AggrFuncExpr: unexpected token {:?}; want aggregate func modifier",
                    self.lex.token
                )));
            }
            ae.modifier = self.parse_modifier_expr(false)?;
        } else if self.lex.token != "(" {
            return Err(ParseError::new(format!(
                "AggrFuncExpr: unexpected token {:?}; want \"(\"",
                self.lex.token
            )));
        }

        ae.args = self.parse_arg_list_expr()?;

        // Verify whether a func suffix exists.
        if ae.modifier.op.is_empty() && is_aggr_func_modifier(&self.lex.token) {
            ae.modifier = self.parse_modifier_expr(false)?;
        }

        // Check for an optional limit.
        if self.lex.token.eq_ignore_ascii_case("limit") {
            self.lex.next()?;
            let limit: i64 = self.lex.token.parse().map_err(|err| {
                ParseError::new(format!("cannot parse limit {:?}: {err}", self.lex.token))
            })?;
            self.lex.next()?;
            ae.limit = limit;
        }
        Ok(ae)
    }

    /// Port of Go `parser.parseFuncExpr`.
    fn parse_func_expr(&mut self) -> Result<FuncExpr> {
        if !is_ident_prefix(&self.lex.token) {
            return Err(ParseError::new(format!(
                "FuncExpr: unexpected token {:?}; want \"ident\"",
                self.lex.token
            )));
        }

        let name = unescape_ident(&self.lex.token);
        self.lex.next()?;
        if self.lex.token != "(" {
            return Err(ParseError::new(format!(
                "FuncExpr; unexpected token {:?}; want \"(\"",
                self.lex.token
            )));
        }
        let args = self.parse_arg_list_expr()?;
        let mut fe = FuncExpr {
            name,
            args,
            keep_metric_names: false,
        };
        if is_keep_metric_names(&self.lex.token) {
            fe.keep_metric_names = true;
            self.lex.next()?;
        }
        Ok(fe)
    }

    /// Port of Go `parser.parseModifierExpr`.
    fn parse_modifier_expr(&mut self, allow_star: bool) -> Result<ModifierExpr> {
        if !is_ident_prefix(&self.lex.token) {
            return Err(ParseError::new(format!(
                "ModifierExpr: unexpected token {:?}; want \"ident\"",
                self.lex.token
            )));
        }

        let op = self.lex.token.to_ascii_lowercase();
        self.lex.next()?;
        if is_binary_op_join_modifier(&op) && self.lex.token != "(" {
            // The join modifier may miss the ident list.
            return Ok(ModifierExpr {
                op,
                args: Vec::new(),
            });
        }
        let args = self
            .parse_ident_list(allow_star)
            .map_err(|err| ParseError::new(format!("ModifierExpr: {err}")))?;
        Ok(ModifierExpr { op, args })
    }

    /// Port of Go `parser.parseIdentList`.
    fn parse_ident_list(&mut self, allow_star: bool) -> Result<Vec<String>> {
        if self.lex.token != "(" {
            return Err(ParseError::new(format!(
                "identList: unexpected token {:?}; want \"(\"",
                self.lex.token
            )));
        }
        self.lex.next()?;
        if allow_star && self.lex.token == "*" {
            self.lex.next()?;
            if self.lex.token != ")" {
                return Err(ParseError::new(format!(
                    "identList: unexpected token {:?} after \"*\"; want \")\"",
                    self.lex.token
                )));
            }
            self.lex.next()?;
            return Ok(vec!["*".to_string()]);
        }
        let mut idents: Vec<String> = Vec::new();
        loop {
            if self.lex.token == ")" {
                self.lex.next()?;
                return Ok(idents);
            }
            if is_quoted_string(&self.lex.token) {
                // The ident may be quoted according to the Prometheus UTF-8
                // proposal:
                // https://github.com/prometheus/proposals/blob/main/proposals/2023-08-21-utf8.md
                self.lex.token = self.lex.token[1..self.lex.token.len() - 1].to_string();
            }
            if !is_ident_prefix(&self.lex.token) {
                return Err(ParseError::new(format!(
                    "identList: unexpected token {:?}; want \"ident\"",
                    self.lex.token
                )));
            }
            idents.push(unescape_ident(&self.lex.token));
            self.lex.next()?;
            match self.lex.token.as_str() {
                "," => {
                    self.lex.next()?;
                }
                ")" => continue,
                _ => {
                    return Err(ParseError::new(format!(
                        "identList: unexpected token {:?}; want \",\", \")\"",
                        self.lex.token
                    )));
                }
            }
        }
    }

    /// Port of Go `parser.parseArgListExpr`.
    fn parse_arg_list_expr(&mut self) -> Result<Vec<Expr>> {
        if self.lex.token != "(" {
            return Err(ParseError::new(format!(
                "argList: unexpected token {:?}; want \"(\"",
                self.lex.token
            )));
        }
        let mut args: Vec<Expr> = Vec::new();
        loop {
            self.lex.next()?;
            if self.lex.token == ")" {
                break;
            }
            let expr = self.parse_expr()?;
            args.push(expr);
            match self.lex.token.as_str() {
                "," => continue,
                ")" => break,
                _ => {
                    return Err(ParseError::new(format!(
                        "argList: unexpected token {:?}; want \",\", \")\"",
                        self.lex.token
                    )));
                }
            }
        }
        self.lex.next()?;
        Ok(args)
    }

    /// Port of Go `parser.parseLabelFilterss`: parses or-delimited groups of
    /// label filters inside curly braces.
    fn parse_label_filterss(
        &mut self,
        mf: Option<&LabelFilterExpr>,
    ) -> Result<Vec<Vec<LabelFilterExpr>>> {
        if self.lex.token != "{" {
            return Err(ParseError::new(format!(
                "labelFilters: unexpected token {:?}; want \"{{\"",
                self.lex.token
            )));
        }
        self.lex.next()?;
        if self.lex.token == "}" {
            self.lex.next()?;
            if let Some(mf) = mf {
                return Ok(vec![vec![mf.clone()]]);
            }
            return Ok(Vec::new());
        }

        let mut lfess: Vec<Vec<LabelFilterExpr>> = Vec::new();
        loop {
            let lfes = self.parse_label_filters(mf)?;
            lfess.push(lfes);
            match self.lex.token.to_ascii_lowercase().as_str() {
                "}" => {
                    self.lex.next()?;
                    return Ok(lfess);
                }
                "or" => {
                    self.lex.next()?;
                }
                _ => {}
            }
        }
    }

    /// Port of Go `parser.parseLabelFilters`: parses a single or-group of
    /// label filters.
    fn parse_label_filters(
        &mut self,
        mf: Option<&LabelFilterExpr>,
    ) -> Result<Vec<LabelFilterExpr>> {
        let mut lfes: Vec<LabelFilterExpr> = Vec::new();
        if let Some(mf) = mf {
            lfes.push(mf.clone());
        }
        loop {
            let lfe = self.parse_label_filter_expr()?;
            lfes.push(lfe);
            match self.lex.token.to_ascii_lowercase().as_str() {
                "," => {
                    self.lex.next()?;
                    if self.lex.token == "}" {
                        return Ok(lfes);
                    }
                    continue;
                }
                "or" | "}" => return Ok(lfes),
                _ => {
                    return Err(ParseError::new(format!(
                        "labelFilters: unexpected token {:?}; want \",\", \"or\", \"}}\"",
                        self.lex.token
                    )));
                }
            }
        }
    }

    /// Port of Go `parser.parseLabelFilterExpr`.
    fn parse_label_filter_expr(&mut self) -> Result<LabelFilterExpr> {
        let mut is_possible_metric_name = false;
        if is_quoted_string(&self.lex.token) {
            // Strip the quotes. A quoted string could be a metric name:
            // {"metric_name"}.
            self.lex.token = self.lex.token[1..self.lex.token.len() - 1].to_string();
            is_possible_metric_name = true;
        } else if !is_ident_prefix(&self.lex.token) {
            return Err(ParseError::new(format!(
                "labelFilterExpr: unexpected token {:?}; want \"ident\"",
                self.lex.token
            )));
        }

        let mut lfe = LabelFilterExpr {
            label: unescape_ident(&self.lex.token),
            ..Default::default()
        };
        self.lex.next()?;

        match self.lex.token.to_ascii_lowercase().as_str() {
            "=" => {
                // Nothing to do.
            }
            "!=" => lfe.is_negative = true,
            "=~" => lfe.is_regexp = true,
            "!~" => {
                lfe.is_negative = true;
                lfe.is_regexp = true;
            }
            "," | "}" | "or" => {
                // An incomplete label filter `lf` such as `{lf}`,
                // `{lf,other="filter"}` or `{lf or other="filter"}`.
                // It must be substituted by a complete label filter during
                // WITH template expansion. A quoted label name with a nil
                // value may be a metric name per the Prometheus 3.0 UTF-8
                // quoted label names spec.
                lfe.is_possible_metric_name = is_possible_metric_name;
                return Ok(lfe);
            }
            _ => {
                return Err(ParseError::new(format!(
                    "labelFilterExpr: unexpected token {:?}; want \"=\", \"!=\", \"=~\", \"!~\", \",\", \"or\", \"}}\"",
                    self.lex.token
                )));
            }
        }

        self.lex.next()?;
        let se = self.parse_string_expr()?;
        lfe.value = Some(se);
        Ok(lfe)
    }

    /// Port of Go `parser.parseWindowAndStep`.
    fn parse_window_and_step(
        &mut self,
    ) -> Result<(Option<DurationExpr>, Option<DurationExpr>, bool)> {
        if self.lex.token != "[" {
            return Err(ParseError::new(format!(
                "windowAndStep: unexpected token {:?}; want \"[\"",
                self.lex.token
            )));
        }
        self.lex.next()?;
        let mut window = None;
        if !self.lex.token.starts_with(':') {
            if self.lex.token == "$__interval" {
                // Skip $__interval, since it must be treated as a missing
                // lookbehind window, e.g. rate(m[$__interval]) must be
                // equivalent to rate(m).
                self.lex.next()?;
            } else {
                window = Some(self.parse_positive_duration()?);
            }
        }
        let mut step = None;
        let mut inherit_step = false;
        if self.lex.token.starts_with(':') {
            // Parse the step.
            self.lex.token = self.lex.token[1..].to_string();
            if self.lex.token.is_empty() {
                self.lex.next()?;
                if self.lex.token == "]" {
                    inherit_step = true;
                }
            }
            if self.lex.token != "]" {
                step = Some(self.parse_positive_duration()?);
            }
        }
        if self.lex.token != "]" {
            return Err(ParseError::new(format!(
                "windowAndStep: unexpected token {:?}; want \"]\"",
                self.lex.token
            )));
        }
        self.lex.next()?;
        Ok((window, step, inherit_step))
    }

    /// Port of Go `parser.parseAtExpr`.
    fn parse_at_expr(&mut self) -> Result<Expr> {
        if self.lex.token != "@" {
            return Err(ParseError::new(format!(
                "unexpected token {:?}; want \"@\"",
                self.lex.token
            )));
        }
        self.lex.next()?;
        self.parse_single_expr_without_rollup_suffix()
            .map_err(|err| ParseError::new(format!("cannot parse `@` expresion: {err}")))
    }

    /// Port of Go `parser.parseOffset`.
    fn parse_offset(&mut self) -> Result<DurationExpr> {
        if !is_offset(&self.lex.token) {
            return Err(ParseError::new(format!(
                "offset: unexpected token {:?}; want \"offset\"",
                self.lex.token
            )));
        }
        self.lex.next()?;
        self.parse_duration()
    }

    /// Port of Go `parser.parseDuration`.
    fn parse_duration(&mut self) -> Result<DurationExpr> {
        let is_negative = self.lex.token == "-";
        if is_negative {
            self.lex.next()?;
        }
        let mut de = self.parse_positive_duration()?;
        if is_negative {
            de.s = format!("-{}", de.s);
        }
        Ok(de)
    }

    /// Port of Go `parser.parsePositiveDuration`.
    fn parse_positive_duration(&mut self) -> Result<DurationExpr> {
        let mut s = self.lex.token.clone();
        if is_ident_prefix(&s) {
            if let Some(n) = s.find(':') {
                let head = s[..n].to_string();
                let tail = s[n..].to_string();
                self.lex.push_back(&head, &tail);
                s = head;
            }
            self.lex.next()?;
            return Ok(DurationExpr {
                s,
                needs_parsing: true,
            });
        }
        if is_positive_duration(&s) {
            self.lex.next()?;
        } else {
            if !is_positive_number_prefix(&s) {
                return Err(ParseError::new(format!(
                    "duration: unexpected token {s:?}; want valid duration"
                )));
            }
            // Verify the duration in seconds without an explicit suffix.
            self.parse_positive_number_expr()
                .map_err(|err| ParseError::new(format!("duration: parse error: {err}")))?;
        }
        // Verify the duration value.
        if s == "$__interval" {
            s = "1i".to_string();
        }
        DurationExpr::new(s)
    }

    /// Port of Go `parser.parseIdentExpr`: parses expressions starting with
    /// an `ident` token.
    fn parse_ident_expr(&mut self) -> Result<Expr> {
        // Look into the next-next token in order to determine how to parse
        // the current expression.
        self.lex.next()?;
        if is_eof(&self.lex.token) || is_offset(&self.lex.token) {
            self.lex.prev();
            return Ok(Expr::Metric(self.parse_metric_expr()?));
        }
        if is_ident_prefix(&self.lex.token) {
            self.lex.prev();
            if is_aggr_func(&self.lex.token) {
                return Ok(Expr::Aggr(self.parse_aggr_func_expr()?));
            }
            return Ok(Expr::Metric(self.parse_metric_expr()?));
        }
        if is_binary_op(&self.lex.token) {
            self.lex.prev();
            return Ok(Expr::Metric(self.parse_metric_expr()?));
        }
        match self.lex.token.as_str() {
            "(" => {
                self.lex.prev();
                if is_aggr_func(&self.lex.token) {
                    return Ok(Expr::Aggr(self.parse_aggr_func_expr()?));
                }
                Ok(Expr::Func(self.parse_func_expr()?))
            }
            "{" | "[" | ")" | "," | "@" => {
                self.lex.prev();
                Ok(Expr::Metric(self.parse_metric_expr()?))
            }
            _ => Err(ParseError::new(format!(
                "identExpr: unexpected token {:?}; want \"(\", \"{{\", \"[\", \")\", \",\" or \"@\"",
                self.lex.token
            ))),
        }
    }

    /// Port of Go `parser.parseMetricExpr`.
    fn parse_metric_expr(&mut self) -> Result<MetricExpr> {
        let mut mf: Option<LabelFilterExpr> = None;
        let mut me = MetricExpr::default();
        if is_ident_prefix(&self.lex.token) {
            let quoted = quote_string(&unescape_ident(&self.lex.token));
            mf = Some(LabelFilterExpr {
                label: "__name__".to_string(),
                value: Some(StringExpr {
                    s: String::new(),
                    tokens: vec![quoted],
                }),
                ..Default::default()
            });
            self.lex.next()?;
            if self.lex.token != "{" {
                me.lfss_unexpanded.push(vec![mf.expect("just constructed")]);
                return Ok(me);
            }
        }
        let lfess = self.parse_label_filterss(mf.as_ref())?;
        me.lfss_unexpanded.extend(lfess);
        Ok(me)
    }

    /// Port of Go `parser.parseRollupExpr`.
    fn parse_rollup_expr(&mut self, arg: Expr) -> Result<Expr> {
        let mut re = RollupExpr::new(arg);
        if self.lex.token == "[" {
            let (window, step, inherit_step) = self.parse_window_and_step()?;
            re.window = window;
            re.step = step;
            re.inherit_step = inherit_step;
            if !is_offset(&self.lex.token) && self.lex.token != "@" {
                return Ok(Expr::Rollup(re));
            }
        }
        if self.lex.token == "@" {
            re.at = Some(Box::new(self.parse_at_expr()?));
        }
        if is_offset(&self.lex.token) {
            re.offset = Some(self.parse_offset()?);
        }
        if self.lex.token == "@" {
            if re.at.is_some() {
                return Err(ParseError::new("duplicate `@` token"));
            }
            re.at = Some(Box::new(self.parse_at_expr()?));
        }
        Ok(Expr::Rollup(re))
    }
}

/// Port of Go `balanceBinaryOp`: restores operator precedence for the
/// left-to-right parsed binary expression chain.
fn balance_binary_op(be: BinaryOpExpr) -> Expr {
    let lp = match &*be.left {
        Expr::BinaryOp(bel) => binary_op_priority(&bel.op),
        _ => return Expr::BinaryOp(be),
    };
    let rp = binary_op_priority(&be.op);
    if rp < lp || (rp == lp && !is_right_associative_binary_op(&be.op)) {
        return Expr::BinaryOp(be);
    }
    let mut be = be;
    let Expr::BinaryOp(mut bel) = *be.left else {
        unreachable!("checked above");
    };
    be.left = bel.right;
    bel.right = Box::new(balance_binary_op(be));
    Expr::BinaryOp(bel)
}

/// Port of Go `removeParensExpr`: removes `ParensExpr` for the `(Expr)` case
/// and converts multi-arg parens into a `union()`-like `FuncExpr` with an
/// empty name.
pub(crate) fn remove_parens_expr(e: Expr) -> Expr {
    match e {
        Expr::Rollup(mut re) => {
            re.expr = Box::new(remove_parens_expr(*re.expr));
            if let Some(at) = re.at {
                re.at = Some(Box::new(remove_parens_expr(*at)));
            }
            Expr::Rollup(re)
        }
        Expr::BinaryOp(mut be) => {
            be.left = Box::new(remove_parens_expr(*be.left));
            be.right = Box::new(remove_parens_expr(*be.right));
            Expr::BinaryOp(be)
        }
        Expr::Aggr(mut ae) => {
            ae.args = ae.args.into_iter().map(remove_parens_expr).collect();
            Expr::Aggr(ae)
        }
        Expr::Func(mut fe) => {
            fe.args = fe.args.into_iter().map(remove_parens_expr).collect();
            Expr::Func(fe)
        }
        Expr::Parens(pe) => {
            let mut args: Vec<Expr> = pe.0.into_iter().map(remove_parens_expr).collect();
            if args.len() == 1 {
                return args.remove(0);
            }
            // Treat parensExpr as a function with an empty name, i.e. union().
            Expr::Func(FuncExpr {
                name: String::new(),
                args,
                keep_metric_names: false,
            })
        }
        Expr::With(mut we) => {
            for wa in &mut we.was {
                let wa = Arc::make_mut(wa);
                wa.expr = remove_parens_expr(std::mem::replace(
                    &mut wa.expr,
                    Expr::Parens(ParensExpr(Vec::new())),
                ));
            }
            we.expr = Box::new(remove_parens_expr(*we.expr));
            Expr::With(we)
        }
        other => other,
    }
}

/// Port of Go `simplifyConstants`: folds constant sub-expressions.
pub(crate) fn simplify_constants(e: Expr) -> Expr {
    match e {
        Expr::With(_) => panic!("BUG: withExpr shouldn't be passed to simplify_constants"),
        Expr::Parens(_) => panic!("BUG: parensExpr shouldn't be passed to simplify_constants"),
        Expr::Rollup(mut re) => {
            re.expr = Box::new(simplify_constants(*re.expr));
            if let Some(at) = re.at {
                re.at = Some(Box::new(simplify_constants(*at)));
            }
            Expr::Rollup(re)
        }
        Expr::Aggr(mut ae) => {
            ae.args = ae.args.into_iter().map(simplify_constants).collect();
            Expr::Aggr(ae)
        }
        Expr::Func(mut fe) => {
            fe.args = fe.args.into_iter().map(simplify_constants).collect();
            Expr::Func(fe)
        }
        Expr::BinaryOp(be) => simplify_constants_in_binary_expr(be),
        other => other,
    }
}

/// Port of Go `simplifyConstantsInBinaryExpr`.
fn simplify_constants_in_binary_expr(mut be: BinaryOpExpr) -> Expr {
    be.left = Box::new(simplify_constants(*be.left));
    be.right = Box::new(simplify_constants(*be.right));

    if let (Expr::Number(lne), Expr::Number(rne)) = (&*be.left, &*be.right) {
        let n = binary_op_eval_number(&be.op, lne.n, rne.n, be.bool_modifier);
        return Expr::Number(NumberExpr::from_value(n));
    }

    // Check whether both operands are string literals.
    let (Expr::String(lse), Expr::String(rse)) = (&*be.left, &*be.right) else {
        return Expr::BinaryOp(be);
    };
    if be.op == "+" {
        // Convert "foo" + "bar" to "foobar".
        return Expr::String(StringExpr::from_string(format!("{}{}", lse.s, rse.s)));
    }
    if !is_binary_op_cmp(&be.op) {
        return Expr::BinaryOp(be);
    }
    // Perform string comparisons.
    let ok = match be.op.as_str() {
        "==" => lse.s == rse.s,
        "!=" => lse.s != rse.s,
        ">" => lse.s > rse.s,
        "<" => lse.s < rse.s,
        ">=" => lse.s >= rse.s,
        "<=" => lse.s <= rse.s,
        _ => unreachable!("BUG: unexpected comparison binaryOp: {:?}", be.op),
    };
    let mut n = if ok { 1.0 } else { 0.0 };
    if !be.bool_modifier && n == 0.0 {
        n = f64::NAN;
    }
    Expr::Number(NumberExpr::from_value(n))
}
