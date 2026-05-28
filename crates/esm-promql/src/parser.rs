//! PromQL parser (Phase 3 MVP subset).
//!
//! Accepts:
//! - Numeric literals: `42`, `1.5e9`.
//! - Vector selectors with metric-name sugar and label matchers:
//!   `up`, `up{}`, `up{job="prom"}`,
//!   `{__name__="up"}`, `metric{a=~"x.*",b!="y"}[5m]`.
//!
//! Rejects everything else with [`ParseError::Unsupported`].

use thiserror::Error;

use crate::ast::{
    AggregationExpr, AggregationOp, BinaryExpr, BinaryOp, Expr, FunctionCall, GroupSide,
    GroupingClause, GroupingKind, LabelMatcher, MatchOp, MatchingGroup, UnaryOp, VectorMatching,
    VectorMatchingKind, VectorSelector,
};
use crate::lexer::{LexError, Spanned, Token, tokenize};

/// Parse one PromQL expression from `src`.
///
/// # Errors
/// Returns [`ParseError`] on any tokenisation or parse failure, or if the
/// expression uses syntax outside the Phase 3 MVP subset.
pub fn parse(src: &str) -> Result<Expr, ParseError> {
    let tokens = tokenize(src)?;
    let mut p = Parser { tokens: &tokens, pos: 0 };
    let expr = p.parse_expr()?;
    if p.pos != tokens.len() {
        return Err(ParseError::TrailingTokens { at: p.peek_start() });
    }
    Ok(expr)
}

struct Parser<'a> {
    tokens: &'a [Spanned],
    pos: usize,
}

impl Parser<'_> {
    /// Top-level expression entry — accepts any complete PromQL expression.
    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_binary_expr(0)
    }

    /// Precedence-climbing binary-expression parser.
    fn parse_binary_expr(&mut self, min_prec: u8) -> Result<Expr, ParseError> {
        let mut lhs = self.parse_unary()?;
        while let Some((op, _)) = self.peek_binary_op() {
            let prec = op.precedence();
            if prec < min_prec {
                break;
            }
            self.advance();
            let mut return_bool = false;
            if op.is_comparison() && matches!(self.peek().map(|t| &t.token), Some(Token::KwBool)) {
                self.advance();
                return_bool = true;
            }
            let matching = self.parse_optional_vector_matching()?;
            let next_min = if op.right_associative() { prec } else { prec + 1 };
            let rhs = self.parse_binary_expr(next_min)?;
            lhs = Expr::Binary(BinaryExpr {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                return_bool,
                matching,
            });
        }
        Ok(lhs)
    }

    /// Parse an optional `on(labels)`/`ignoring(labels)` plus optional
    /// `group_left(includes)`/`group_right(includes)`. Returns None if no
    /// such modifier follows the binary operator.
    fn parse_optional_vector_matching(&mut self) -> Result<Option<VectorMatching>, ParseError> {
        let kind = match self.peek().map(|t| &t.token) {
            Some(Token::KwOn) => VectorMatchingKind::On,
            Some(Token::KwIgnoring) => VectorMatchingKind::Ignoring,
            _ => return Ok(None),
        };
        self.advance();
        let labels = self.parse_paren_label_list()?;
        let group = match self.peek().map(|t| &t.token) {
            Some(Token::KwGroupLeft) => {
                self.advance();
                let include = self.parse_paren_label_list_optional()?;
                Some(MatchingGroup { side: GroupSide::Left, include })
            }
            Some(Token::KwGroupRight) => {
                self.advance();
                let include = self.parse_paren_label_list_optional()?;
                Some(MatchingGroup { side: GroupSide::Right, include })
            }
            _ => None,
        };
        Ok(Some(VectorMatching { kind, labels, group }))
    }

    fn parse_paren_label_list(&mut self) -> Result<Vec<String>, ParseError> {
        self.expect(&Token::LParen)?;
        self.parse_label_list_until_rparen()
    }

    /// Like [`parse_paren_label_list`] but the `()` are optional — used after
    /// `group_left` / `group_right` which can stand alone.
    fn parse_paren_label_list_optional(&mut self) -> Result<Vec<String>, ParseError> {
        if matches!(self.peek().map(|t| &t.token), Some(Token::LParen)) {
            self.advance();
            self.parse_label_list_until_rparen()
        } else {
            Ok(Vec::new())
        }
    }

    fn parse_label_list_until_rparen(&mut self) -> Result<Vec<String>, ParseError> {
        let mut labels = Vec::new();
        loop {
            match self.peek().map(|t| &t.token) {
                Some(Token::Identifier(n)) => {
                    labels.push(n.clone());
                    self.advance();
                }
                Some(Token::RParen) => break,
                _ => {
                    return Err(ParseError::Expected {
                        what: "label name".into(),
                        at: self.peek_start(),
                    });
                }
            }
            match self.peek().map(|t| &t.token) {
                Some(Token::Comma) => {
                    self.advance();
                }
                Some(Token::RParen) => break,
                _ => {
                    return Err(ParseError::Expected {
                        what: "',' or ')'".into(),
                        at: self.peek_start(),
                    });
                }
            }
        }
        self.expect(&Token::RParen)?;
        Ok(labels)
    }

    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
        match self.peek().map(|t| &t.token) {
            Some(Token::Plus) => {
                self.advance();
                let inner = self.parse_unary()?;
                Ok(Expr::Unary(UnaryOp::Pos, Box::new(inner)))
            }
            Some(Token::Minus) => {
                self.advance();
                let inner = self.parse_unary()?;
                Ok(Expr::Unary(UnaryOp::Neg, Box::new(inner)))
            }
            _ => {
                let prim = self.parse_primary()?;
                self.maybe_wrap_subquery(prim)
            }
        }
    }

    /// `inner[range:step]` — subquery suffix. Only attached to non-selector
    /// expressions (vector selectors already use `[5m]` for their range).
    fn maybe_wrap_subquery(&mut self, expr: Expr) -> Result<Expr, ParseError> {
        // Skip the wrap if the next token isn't `[` or if the inner is a
        // bare vector selector (already handles its own range).
        if !matches!(self.peek().map(|t| &t.token), Some(Token::LBracket)) {
            return Ok(expr);
        }
        if matches!(expr, Expr::VectorSelector(_)) {
            return Ok(expr);
        }
        // Lookahead: subqueries require a `:` inside the brackets. If we
        // can't find one before the matching `]`, leave the expression
        // alone (no range form is valid here).
        if !self.contains_colon_before_rbracket() {
            return Ok(expr);
        }
        self.advance(); // `[`
        let range_ms = match self.peek().map(|t| &t.token) {
            Some(Token::DurationMs(ms)) => *ms,
            _ => return Err(ParseError::ExpectedDuration { at: self.peek_start() }),
        };
        self.advance();
        if !matches!(self.peek().map(|t| &t.token), Some(Token::Colon)) {
            return Err(ParseError::Expected { what: "':'".into(), at: self.peek_start() });
        }
        self.advance();
        let step_ms = match self.peek().map(|t| &t.token) {
            Some(Token::DurationMs(ms)) => {
                let v = *ms;
                self.advance();
                Some(v)
            }
            Some(Token::RBracket) => None,
            _ => return Err(ParseError::ExpectedDuration { at: self.peek_start() }),
        };
        self.expect(&Token::RBracket)?;
        Ok(Expr::Subquery(Box::new(crate::ast::SubqueryExpr {
            inner: Box::new(expr),
            range_ms,
            step_ms,
        })))
    }

    fn contains_colon_before_rbracket(&self) -> bool {
        // Cheap lookahead.
        let mut depth = 0i32;
        for tok in &self.tokens[self.pos..] {
            match tok.token {
                Token::LBracket => depth += 1,
                Token::RBracket => {
                    depth -= 1;
                    if depth <= 0 {
                        return false;
                    }
                }
                Token::Colon if depth == 1 => return true,
                _ => {}
            }
        }
        false
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        let tok = self.peek().ok_or(ParseError::UnexpectedEof)?;
        match &tok.token {
            Token::Number(n) => {
                let n = *n;
                self.advance();
                Ok(Expr::NumberLiteral(n))
            }
            Token::String(s) => {
                let s = s.clone();
                self.advance();
                Ok(Expr::StringLiteral(s))
            }
            Token::LParen => {
                self.advance();
                let inner = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(Expr::Paren(Box::new(inner)))
            }
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                // Function call or aggregation? `name (`...`)`.
                if matches!(self.peek().map(|t| &t.token), Some(Token::LParen)) {
                    if let Some(op) = AggregationOp::from_name(&name) {
                        return self.parse_aggregation(op);
                    }
                    return self.parse_function_call(name);
                }
                // Aggregations also support a `sum by (label) (expr)` form
                // where the grouping clause appears *before* the args.
                if let Some(op) = AggregationOp::from_name(&name)
                    && matches!(self.peek().map(|t| &t.token), Some(Token::KwBy | Token::KwWithout))
                {
                    return self.parse_aggregation_with_leading_grouping(op);
                }
                self.parse_vector_selector_with_name(Some(name))
            }
            Token::LBrace => self.parse_vector_selector_with_name(None),
            other => Err(ParseError::Unsupported { description: format!("token {other:?}") }),
        }
    }

    /// Map the next token to a `BinaryOp` if any. Returns `(op, return_bool)`
    /// where `return_bool` is always `false` here; the parser sets it true
    /// after consuming a trailing `bool` keyword on comparison ops.
    fn peek_binary_op(&self) -> Option<(BinaryOp, bool)> {
        Some(match self.peek()?.token {
            Token::Plus => (BinaryOp::Add, false),
            Token::Minus => (BinaryOp::Sub, false),
            Token::Star => (BinaryOp::Mul, false),
            Token::Slash => (BinaryOp::Div, false),
            Token::Percent => (BinaryOp::Mod, false),
            Token::Caret => (BinaryOp::Pow, false),
            Token::EqEq => (BinaryOp::Eq, false),
            Token::Neq => (BinaryOp::Ne, false),
            Token::Lt => (BinaryOp::Lt, false),
            Token::Le => (BinaryOp::Le, false),
            Token::Gt => (BinaryOp::Gt, false),
            Token::Ge => (BinaryOp::Ge, false),
            Token::KwAnd => (BinaryOp::And, false),
            Token::KwOr => (BinaryOp::Or, false),
            Token::KwUnless => (BinaryOp::Unless, false),
            _ => return None,
        })
    }

    fn parse_vector_selector_with_name(
        &mut self,
        name: Option<String>,
    ) -> Result<Expr, ParseError> {
        let mut matchers: Vec<LabelMatcher> = Vec::new();
        // If the next token is `{`, parse the matcher list.
        if matches!(self.peek().map(|t| &t.token), Some(Token::LBrace)) {
            self.advance();
            // Empty matcher list: `{}` is allowed.
            if !matches!(self.peek().map(|t| &t.token), Some(Token::RBrace)) {
                loop {
                    let m = self.parse_matcher()?;
                    matchers.push(m);
                    match self.peek().map(|t| &t.token) {
                        Some(Token::Comma) => {
                            self.advance();
                        }
                        Some(Token::RBrace) => break,
                        _ => {
                            return Err(ParseError::ExpectedCommaOrBrace { at: self.peek_start() });
                        }
                    }
                }
            }
            self.expect(&Token::RBrace)?;
        }

        // Optional range: `[5m]`.
        let mut range_ms: Option<i64> = None;
        if matches!(self.peek().map(|t| &t.token), Some(Token::LBracket)) {
            self.advance();
            let dur = match self.peek().map(|t| &t.token) {
                Some(Token::DurationMs(ms)) => *ms,
                _ => return Err(ParseError::ExpectedDuration { at: self.peek_start() }),
            };
            self.advance();
            self.expect(&Token::RBracket)?;
            range_ms = Some(dur);
        }

        // Optional `offset <duration>` and `@ <timestamp>` modifiers, in
        // either order (Prometheus allows both `metric offset 5m @ 1700`
        // and `metric @ 1700 offset 5m`).
        let mut offset_ms: Option<i64> = None;
        let mut at_timestamp_sec: Option<f64> = None;
        loop {
            match self.peek().map(|t| &t.token) {
                Some(Token::KwOffset) => {
                    self.advance();
                    let neg = matches!(self.peek().map(|t| &t.token), Some(Token::Minus));
                    if neg {
                        self.advance();
                    }
                    let dur = match self.peek().map(|t| &t.token) {
                        Some(Token::DurationMs(ms)) => *ms,
                        _ => return Err(ParseError::ExpectedDuration { at: self.peek_start() }),
                    };
                    self.advance();
                    offset_ms = Some(if neg { -dur } else { dur });
                }
                Some(Token::At) => {
                    self.advance();
                    let neg = matches!(self.peek().map(|t| &t.token), Some(Token::Minus));
                    if neg {
                        self.advance();
                    }
                    let n = match self.peek().map(|t| &t.token) {
                        Some(Token::Number(n)) => *n,
                        _ => {
                            return Err(ParseError::Expected {
                                what: "epoch-seconds number after @".into(),
                                at: self.peek_start(),
                            });
                        }
                    };
                    self.advance();
                    at_timestamp_sec = Some(if neg { -n } else { n });
                }
                _ => break,
            }
        }

        // A selector must have either a name or at least one matcher.
        if name.is_none() && matchers.is_empty() {
            return Err(ParseError::EmptySelector { at: self.peek_start() });
        }

        Ok(Expr::VectorSelector(VectorSelector {
            name,
            matchers,
            range_ms,
            offset_ms,
            at_timestamp_sec,
        }))
    }

    fn parse_function_call(&mut self, name: String) -> Result<Expr, ParseError> {
        self.expect(&Token::LParen)?;
        let mut args = Vec::new();
        if !matches!(self.peek().map(|t| &t.token), Some(Token::RParen)) {
            loop {
                let arg = self.parse_expr()?;
                args.push(arg);
                match self.peek().map(|t| &t.token) {
                    Some(Token::Comma) => {
                        self.advance();
                    }
                    Some(Token::RParen) => break,
                    _ => {
                        return Err(ParseError::Expected {
                            what: "',' or ')'".into(),
                            at: self.peek_start(),
                        });
                    }
                }
            }
        }
        self.expect(&Token::RParen)?;
        Ok(Expr::FunctionCall(FunctionCall { name, args }))
    }

    fn parse_aggregation(&mut self, op: AggregationOp) -> Result<Expr, ParseError> {
        // Two forms:
        //   sum (expr)
        //   sum (expr) by (lbl1, lbl2)
        //   sum (expr) without (lbl1, lbl2)
        self.expect(&Token::LParen)?;
        let param = if op.takes_param() {
            let p = self.parse_expr()?;
            self.expect(&Token::Comma)?;
            Some(Box::new(p))
        } else {
            None
        };
        let arg = self.parse_expr()?;
        self.expect(&Token::RParen)?;
        let grouping = self.parse_optional_grouping()?;
        Ok(Expr::Aggregation(AggregationExpr { op, arg: Box::new(arg), grouping, param }))
    }

    fn parse_aggregation_with_leading_grouping(
        &mut self,
        op: AggregationOp,
    ) -> Result<Expr, ParseError> {
        // `sum by (lbl) (expr)` / `sum without (lbl) (expr)`.
        let grouping = self.parse_optional_grouping()?;
        self.expect(&Token::LParen)?;
        let param = if op.takes_param() {
            let p = self.parse_expr()?;
            self.expect(&Token::Comma)?;
            Some(Box::new(p))
        } else {
            None
        };
        let arg = self.parse_expr()?;
        self.expect(&Token::RParen)?;
        Ok(Expr::Aggregation(AggregationExpr { op, arg: Box::new(arg), grouping, param }))
    }

    fn parse_optional_grouping(&mut self) -> Result<Option<GroupingClause>, ParseError> {
        let kind = match self.peek().map(|t| &t.token) {
            Some(Token::KwBy) => GroupingKind::By,
            Some(Token::KwWithout) => GroupingKind::Without,
            _ => return Ok(None),
        };
        self.advance();
        self.expect(&Token::LParen)?;
        let mut labels = Vec::new();
        loop {
            match self.peek().map(|t| &t.token) {
                Some(Token::Identifier(n)) => {
                    labels.push(n.clone());
                    self.advance();
                }
                Some(Token::RParen) => break,
                _ => {
                    return Err(ParseError::Expected {
                        what: "label name".into(),
                        at: self.peek_start(),
                    });
                }
            }
            match self.peek().map(|t| &t.token) {
                Some(Token::Comma) => {
                    self.advance();
                }
                Some(Token::RParen) => break,
                _ => {
                    return Err(ParseError::Expected {
                        what: "',' or ')'".into(),
                        at: self.peek_start(),
                    });
                }
            }
        }
        self.expect(&Token::RParen)?;
        Ok(Some(GroupingClause { kind, labels }))
    }

    fn parse_matcher(&mut self) -> Result<LabelMatcher, ParseError> {
        let name = match self.peek().map(|t| &t.token) {
            Some(Token::Identifier(n)) => n.clone(),
            _ => return Err(ParseError::ExpectedLabelName { at: self.peek_start() }),
        };
        self.advance();
        let op = match self.peek().map(|t| &t.token) {
            Some(Token::Eq) => MatchOp::Equal,
            Some(Token::Neq) => MatchOp::NotEqual,
            Some(Token::EqRegex) => MatchOp::RegexMatch,
            Some(Token::NeqRegex) => MatchOp::RegexNotMatch,
            _ => return Err(ParseError::ExpectedMatchOp { at: self.peek_start() }),
        };
        self.advance();
        let value = match self.peek().map(|t| &t.token) {
            Some(Token::String(s)) => s.clone(),
            _ => return Err(ParseError::ExpectedLabelValue { at: self.peek_start() }),
        };
        self.advance();
        Ok(LabelMatcher { name, op, value })
    }

    fn peek(&self) -> Option<&Spanned> {
        self.tokens.get(self.pos)
    }

    fn peek_start(&self) -> usize {
        self.tokens
            .get(self.pos)
            .map_or_else(|| self.tokens.last().map_or(0, |t| t.end), |t| t.start)
    }

    fn advance(&mut self) {
        self.pos += 1;
    }

    fn expect(&mut self, tok: &Token) -> Result<(), ParseError> {
        match self.peek() {
            Some(t) if std::mem::discriminant(&t.token) == std::mem::discriminant(tok) => {
                self.advance();
                Ok(())
            }
            _ => Err(ParseError::Expected { what: format!("{tok:?}"), at: self.peek_start() }),
        }
    }
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error(transparent)]
    Lex(#[from] LexError),
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("unsupported syntax: {description}")]
    Unsupported { description: String },
    #[error("trailing tokens after expression at offset {at}")]
    TrailingTokens { at: usize },
    #[error("expected label name at offset {at}")]
    ExpectedLabelName { at: usize },
    #[error("expected match operator (=, !=, =~, !~) at offset {at}")]
    ExpectedMatchOp { at: usize },
    #[error("expected label value (quoted string) at offset {at}")]
    ExpectedLabelValue { at: usize },
    #[error("expected ',' or '}}' between matchers at offset {at}")]
    ExpectedCommaOrBrace { at: usize },
    #[error("expected duration literal at offset {at}")]
    ExpectedDuration { at: usize },
    #[error("expected {what} at offset {at}")]
    Expected { what: String, at: usize },
    #[error("vector selector with no name or matchers at offset {at}")]
    EmptySelector { at: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_metric_name_only() {
        let e = parse("up").unwrap();
        assert_eq!(
            e,
            Expr::VectorSelector(VectorSelector {
                name: Some("up".into()),
                matchers: vec![],
                range_ms: None,
                offset_ms: None,
                at_timestamp_sec: None,
            })
        );
    }

    #[test]
    fn parse_metric_with_matchers() {
        let e = parse(r#"http_requests_total{job="prom",code=~"2.."}"#).unwrap();
        match e {
            Expr::VectorSelector(v) => {
                assert_eq!(v.name.as_deref(), Some("http_requests_total"));
                assert_eq!(v.matchers.len(), 2);
                assert_eq!(v.matchers[0].name, "job");
                assert_eq!(v.matchers[0].op, MatchOp::Equal);
                assert_eq!(v.matchers[0].value, "prom");
                assert_eq!(v.matchers[1].name, "code");
                assert_eq!(v.matchers[1].op, MatchOp::RegexMatch);
                assert_eq!(v.matchers[1].value, "2..");
            }
            other => panic!("expected vector selector, got {other:?}"),
        }
    }

    #[test]
    fn parse_anonymous_selector() {
        let e = parse(r#"{__name__="up"}"#).unwrap();
        match e {
            Expr::VectorSelector(v) => {
                assert!(v.name.is_none());
                assert_eq!(v.matchers.len(), 1);
                assert_eq!(v.matchers[0].name, "__name__");
            }
            other => panic!("expected vector selector, got {other:?}"),
        }
    }

    #[test]
    fn parse_range_vector() {
        let e = parse("metric[5m]").unwrap();
        match e {
            Expr::VectorSelector(v) => {
                assert_eq!(v.range_ms, Some(5 * 60 * 1000));
            }
            other => panic!("expected vector selector, got {other:?}"),
        }
    }

    #[test]
    fn parse_number_literal() {
        let e = parse("42").unwrap();
        match e {
            Expr::NumberLiteral(n) => assert!((n - 42.0).abs() < 1e-9),
            other => panic!("expected number, got {other:?}"),
        }
    }

    #[test]
    fn parse_empty_matcher_set() {
        let e = parse("up{}").unwrap();
        match e {
            Expr::VectorSelector(v) => {
                assert!(v.matchers.is_empty());
                assert_eq!(v.name.as_deref(), Some("up"));
            }
            other => panic!("expected vector selector, got {other:?}"),
        }
    }

    #[test]
    fn empty_anonymous_selector_rejected() {
        assert!(parse("{}").is_err());
    }

    #[test]
    fn parse_binary_arith() {
        let e = parse("a + b").unwrap();
        match e {
            Expr::Binary(b) => {
                assert_eq!(b.op, BinaryOp::Add);
                assert!(matches!(*b.lhs, Expr::VectorSelector(_)));
                assert!(matches!(*b.rhs, Expr::VectorSelector(_)));
            }
            other => panic!("expected binary expression, got {other:?}"),
        }
    }

    #[test]
    fn parse_precedence_mul_binds_tighter_than_add() {
        // `a + b * c` should parse as `a + (b * c)`.
        let e = parse("a + b * c").unwrap();
        match e {
            Expr::Binary(outer) => {
                assert_eq!(outer.op, BinaryOp::Add);
                match *outer.rhs {
                    Expr::Binary(inner) => assert_eq!(inner.op, BinaryOp::Mul),
                    other => panic!("expected nested * on rhs, got {other:?}"),
                }
            }
            other => panic!("expected binary, got {other:?}"),
        }
    }

    #[test]
    fn parse_pow_is_right_associative() {
        // `2 ^ 3 ^ 4` should parse as `2 ^ (3 ^ 4)`.
        let e = parse("2 ^ 3 ^ 4").unwrap();
        match e {
            Expr::Binary(outer) => {
                assert_eq!(outer.op, BinaryOp::Pow);
                match *outer.rhs {
                    Expr::Binary(inner) => assert_eq!(inner.op, BinaryOp::Pow),
                    other => panic!("expected nested ^ on rhs, got {other:?}"),
                }
            }
            other => panic!("expected binary, got {other:?}"),
        }
    }

    #[test]
    fn parse_unary_minus() {
        let e = parse("-1").unwrap();
        match e {
            Expr::Unary(UnaryOp::Neg, inner) => {
                assert!(matches!(*inner, Expr::NumberLiteral(_)));
            }
            other => panic!("expected unary, got {other:?}"),
        }
    }

    #[test]
    fn parse_parens_change_precedence() {
        let e = parse("(a + b) * c").unwrap();
        match e {
            Expr::Binary(outer) => {
                assert_eq!(outer.op, BinaryOp::Mul);
                match *outer.lhs {
                    Expr::Paren(inner) => match *inner {
                        Expr::Binary(b) => assert_eq!(b.op, BinaryOp::Add),
                        other => panic!("expected (+) inside parens, got {other:?}"),
                    },
                    other => panic!("expected paren on lhs, got {other:?}"),
                }
            }
            other => panic!("expected binary, got {other:?}"),
        }
    }

    #[test]
    fn parse_comparison_with_bool() {
        let e = parse("a == bool 5").unwrap();
        match e {
            Expr::Binary(b) => {
                assert_eq!(b.op, BinaryOp::Eq);
                assert!(b.return_bool);
            }
            other => panic!("expected binary, got {other:?}"),
        }
    }

    #[test]
    fn parse_logical_ops() {
        let _and = parse("a and b").unwrap();
        let _or = parse("a or b").unwrap();
        let _unless = parse("a unless b").unwrap();
    }
}
