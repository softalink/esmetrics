//! Lexer (tokenizer) for the Go `text/template` subset: text vs. `{{ }}`
//! actions, whitespace-trim markers (`{{-` / `-}}`), and in-action tokens.

use crate::TemplateError;

/// A single lexed token from a Go template.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Text(String),
    LeftDelim { trim: bool },
    RightDelim { trim: bool },
    Ident(String),
    Field(String),
    Var(String),
    Dot,
    String(String),
    Number(f64),
    Bool(bool),
    Nil,
    Pipe,
    Comma,
    LParen,
    RParen,
    Assign,
    Declare,
    Keyword(Kw),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kw {
    If,
    Else,
    End,
    Range,
    With,
}

const LEFT_DELIM: &str = "{{";
const RIGHT_DELIM: &str = "}}";

/// Lex a Go template subset into a flat token stream.
pub fn lex(input: &str) -> Result<Vec<Token>, TemplateError> {
    Lexer::new(input).run()
}

struct Lexer<'a> {
    input: &'a str,
    pos: usize,
    tokens: Vec<Token>,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            pos: 0,
            tokens: Vec::new(),
        }
    }

    fn rest(&self) -> &'a str {
        &self.input[self.pos..]
    }

    fn run(mut self) -> Result<Vec<Token>, TemplateError> {
        while self.pos < self.input.len() {
            match self.rest().find(LEFT_DELIM) {
                Some(0) => self.lex_action()?,
                Some(offset) => {
                    self.emit_text(offset);
                    self.lex_action()?;
                }
                None => {
                    self.emit_text(self.rest().len());
                }
            }
        }
        Ok(self.tokens)
    }

    /// Emit `len` bytes of `rest()` as `Text`, trimming trailing Go-whitespace
    /// when the following action opens with a `{{-` trim marker.
    fn emit_text(&mut self, len: usize) {
        let raw = &self.rest()[..len];
        self.pos += len;
        let text = if self.upcoming_left_trim() {
            trim_go_space_end(raw)
        } else {
            raw
        };
        if !text.is_empty() {
            self.tokens.push(Token::Text(text.to_string()));
        }
    }

    fn upcoming_left_trim(&self) -> bool {
        let rest = self.rest();
        rest.starts_with(LEFT_DELIM) && starts_left_trim(&rest[LEFT_DELIM.len()..])
    }

    fn lex_action(&mut self) -> Result<(), TemplateError> {
        self.pos += LEFT_DELIM.len();
        // `{{-` is a trim marker only when the dash is followed by Go
        // whitespace; otherwise the dash is action content (e.g. `{{-3}}`).
        let trim_left = starts_left_trim(self.rest());
        if trim_left {
            self.pos += 1;
        }
        self.tokens.push(Token::LeftDelim { trim: trim_left });

        loop {
            self.skip_go_space();
            if let Some(trim_right) = self.try_lex_right_delim()? {
                self.tokens.push(Token::RightDelim { trim: trim_right });
                return Ok(());
            }
            if self.rest().is_empty() {
                return Err(TemplateError::new("unclosed action"));
            }
            self.lex_action_token()?;
        }
    }

    /// Try to consume `-}}` or `}}`. `-}}` is a right-trim marker only when the
    /// dash is preceded by Go whitespace; otherwise the dash is action content.
    fn try_lex_right_delim(&mut self) -> Result<Option<bool>, TemplateError> {
        if self.rest().starts_with("-}}") && self.prev_is_go_space() {
            self.pos += 3;
            self.skip_go_space();
            return Ok(Some(true));
        }
        if self.rest().starts_with(RIGHT_DELIM) {
            self.pos += RIGHT_DELIM.len();
            return Ok(Some(false));
        }
        Ok(None)
    }

    fn prev_is_go_space(&self) -> bool {
        self.pos > 0 && is_go_space(self.input.as_bytes()[self.pos - 1])
    }

    fn skip_go_space(&mut self) {
        let bytes = self.input.as_bytes();
        while self.pos < bytes.len() && is_go_space(bytes[self.pos]) {
            self.pos += 1;
        }
    }

    fn lex_action_token(&mut self) -> Result<(), TemplateError> {
        let c = self.rest().chars().next().unwrap();
        match c {
            '|' => self.consume_char(Token::Pipe),
            ',' => self.consume_char(Token::Comma),
            '(' => self.consume_char(Token::LParen),
            ')' => self.consume_char(Token::RParen),
            '.' => self.lex_dot_or_field(),
            '$' => self.lex_variable(),
            '"' => self.lex_interpreted_string(),
            '`' => self.lex_raw_string(),
            ':' => self.lex_declare(),
            '=' => self.consume_char(Token::Assign),
            c if c.is_ascii_digit() || (c == '-' && self.next_is_digit_after_minus()) => {
                self.lex_number()
            }
            c if is_ident_start(c) => self.lex_ident_like(),
            other => Err(TemplateError::new(format!(
                "unexpected character {other:?} in action"
            ))),
        }
    }

    fn next_is_digit_after_minus(&self) -> bool {
        self.rest()[1..]
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_digit())
    }

    fn consume_char(&mut self, tok: Token) -> Result<(), TemplateError> {
        self.pos += 1;
        self.tokens.push(tok);
        Ok(())
    }

    fn lex_declare(&mut self) -> Result<(), TemplateError> {
        if self.rest().starts_with(":=") {
            self.pos += 2;
            self.tokens.push(Token::Declare);
            Ok(())
        } else {
            Err(TemplateError::new("expected ':=' after ':'"))
        }
    }

    fn lex_dot_or_field(&mut self) -> Result<(), TemplateError> {
        let name_len = ident_len(&self.rest()[1..]);
        if name_len == 0 {
            self.pos += 1;
            self.tokens.push(Token::Dot);
        } else {
            let name = self.rest()[1..1 + name_len].to_string();
            self.pos += 1 + name_len;
            self.tokens.push(Token::Field(name));
        }
        Ok(())
    }

    fn lex_variable(&mut self) -> Result<(), TemplateError> {
        let name_len = ident_len(&self.rest()[1..]);
        let name = self.rest()[1..1 + name_len].to_string();
        self.pos += 1 + name_len;
        self.tokens.push(Token::Var(name));
        Ok(())
    }

    fn lex_ident_like(&mut self) -> Result<(), TemplateError> {
        let len = ident_len(self.rest());
        let word = &self.rest()[..len];
        let token = match word {
            "true" => Token::Bool(true),
            "false" => Token::Bool(false),
            "nil" => Token::Nil,
            "if" => Token::Keyword(Kw::If),
            "else" => Token::Keyword(Kw::Else),
            "end" => Token::Keyword(Kw::End),
            "range" => Token::Keyword(Kw::Range),
            "with" => Token::Keyword(Kw::With),
            ident => Token::Ident(ident.to_string()),
        };
        self.pos += len;
        self.tokens.push(token);
        Ok(())
    }

    fn lex_number(&mut self) -> Result<(), TemplateError> {
        let rest = self.rest();
        let mut len = 0;
        let bytes = rest.as_bytes();
        if bytes[len] == b'-' {
            len += 1;
        }
        while len < bytes.len() && (bytes[len].is_ascii_digit() || bytes[len] == b'.') {
            len += 1;
        }
        let text = &rest[..len];
        let value: f64 = text
            .parse()
            .map_err(|_| TemplateError::new(format!("invalid number {text:?}")))?;
        self.pos += len;
        self.tokens.push(Token::Number(value));
        Ok(())
    }

    fn lex_interpreted_string(&mut self) -> Result<(), TemplateError> {
        let rest = self.rest();
        let mut chars = rest.char_indices().skip(1);
        let mut value = String::new();
        loop {
            let (idx, c) = chars
                .next()
                .ok_or_else(|| TemplateError::new("unterminated string"))?;
            match c {
                '"' => {
                    self.pos += idx + 1;
                    self.tokens.push(Token::String(value));
                    return Ok(());
                }
                '\\' => {
                    let (_, escaped) = chars
                        .next()
                        .ok_or_else(|| TemplateError::new("unterminated string escape"))?;
                    value.push(unescape(escaped));
                }
                c => value.push(c),
            }
        }
    }

    fn lex_raw_string(&mut self) -> Result<(), TemplateError> {
        let rest = self.rest();
        let end = rest[1..]
            .find('`')
            .ok_or_else(|| TemplateError::new("unterminated raw string"))?;
        let value = rest[1..1 + end].to_string();
        self.pos += end + 2;
        self.tokens.push(Token::String(value));
        Ok(())
    }
}

fn unescape(c: char) -> char {
    match c {
        'n' => '\n',
        't' => '\t',
        'r' => '\r',
        '\\' => '\\',
        '"' => '"',
        other => other,
    }
}

/// Go's `isSpace` set (`text/template/parse/lex.go`): only ` \t\r\n`, narrower
/// than Unicode whitespace — so `str::trim`/`is_whitespace` must not be used.
fn is_go_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\r' | b'\n')
}

/// Whether `after` (text following an opening `{{`) begins a left-trim marker.
fn starts_left_trim(after: &str) -> bool {
    let bytes = after.as_bytes();
    bytes.first() == Some(&b'-') && bytes.get(1).is_some_and(|b| is_go_space(*b))
}

fn trim_go_space_end(s: &str) -> &str {
    let bytes = s.as_bytes();
    let mut end = bytes.len();
    while end > 0 && is_go_space(bytes[end - 1]) {
        end -= 1;
    }
    &s[..end]
}

fn is_ident_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn is_ident_continue(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Length in bytes of the identifier at the start of `s` (may be 0).
fn ident_len(s: &str) -> usize {
    match s.chars().next() {
        Some(c) if is_ident_start(c) => s
            .char_indices()
            .find(|(_, c)| !is_ident_continue(*c))
            .map_or(s.len(), |(idx, _)| idx),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lexes_text_and_simple_action() {
        let toks = lex("value is {{ .Value }}!").unwrap();
        assert_eq!(toks[0], Token::Text("value is ".into()));
        assert!(matches!(toks[1], Token::LeftDelim { trim: false }));
        assert_eq!(toks[2], Token::Field("Value".into()));
        assert!(matches!(toks[3], Token::RightDelim { trim: false }));
        assert_eq!(toks[4], Token::Text("!".into()));
    }

    #[test]
    fn lexes_trim_markers_and_pipeline() {
        let toks = lex("{{- humanize .Value | toUpper -}}").unwrap();
        assert!(matches!(toks[0], Token::LeftDelim { trim: true }));
        assert_eq!(toks[1], Token::Ident("humanize".into()));
        assert_eq!(toks[2], Token::Field("Value".into()));
        assert_eq!(toks[3], Token::Pipe);
        assert_eq!(toks[4], Token::Ident("toUpper".into()));
        assert!(matches!(toks[5], Token::RightDelim { trim: true }));
    }

    #[test]
    fn lexes_var_declare_and_string() {
        let toks = lex(r#"{{ $x := "hi" }}"#).unwrap();
        assert_eq!(toks[1], Token::Var("x".into()));
        assert_eq!(toks[2], Token::Declare);
        assert_eq!(toks[3], Token::String("hi".into()));
    }

    #[test]
    fn dash_touching_delims_is_negative_number_not_trim() {
        // `{{-` is a trim marker only when followed by Go whitespace.
        let toks = lex("{{-3}}").unwrap();
        assert!(matches!(toks[0], Token::LeftDelim { trim: false }));
        assert_eq!(toks[1], Token::Number(-3.0));
        assert!(matches!(toks[2], Token::RightDelim { trim: false }));
    }

    #[test]
    fn dash_with_space_is_trim_marker() {
        // Dash flanked by whitespace on both sides is a genuine trim marker.
        let toks = lex("{{- 3 -}}").unwrap();
        assert!(matches!(toks[0], Token::LeftDelim { trim: true }));
        assert_eq!(toks[1], Token::Number(3.0));
        assert!(matches!(toks[2], Token::RightDelim { trim: true }));
    }

    #[test]
    fn lexes_comma_in_range_two_var_decl() {
        let toks = lex("{{range $i, $v := .Xs}}").unwrap();
        assert_eq!(toks[2], Token::Var("i".into()));
        assert_eq!(toks[3], Token::Comma);
        assert_eq!(toks[4], Token::Var("v".into()));
        assert_eq!(toks[5], Token::Declare);
    }

    #[test]
    fn trim_markers_trim_adjacent_text() {
        // Left trim eats trailing text whitespace; right trim eats leading.
        let toks = lex("a \t{{- .X -}}\n b").unwrap();
        assert_eq!(toks[0], Token::Text("a".into()));
        assert!(matches!(toks[1], Token::LeftDelim { trim: true }));
        assert_eq!(toks[2], Token::Field("X".into()));
        assert!(matches!(toks[3], Token::RightDelim { trim: true }));
        assert_eq!(toks[4], Token::Text("b".into()));
    }
}
