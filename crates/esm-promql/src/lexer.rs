//! PromQL lexer.
//!
//! Produces a flat token stream from a source string. Operates on `char`
//! to keep parsing straightforward; PromQL is ASCII-only at the token
//! level so there's no perf cost.

use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// `[a-zA-Z_:][a-zA-Z0-9_:]*` — metric names use `:`; label names do
    /// not, so the parser does the additional validation.
    Identifier(String),
    /// A f64-parseable numeric literal (integers, decimals, exponentials).
    Number(f64),
    /// A double-quoted string with PromQL escape sequences applied.
    String(String),
    /// A duration literal: `5m`, `30s`, `1h30m`. Stored as milliseconds.
    DurationMs(i64),
    /// `(`
    LParen,
    /// `)`
    RParen,
    /// `{`
    LBrace,
    /// `}`
    RBrace,
    /// `[`
    LBracket,
    /// `]`
    RBracket,
    /// `,`
    Comma,
    /// `=`
    Eq,
    /// `==`
    EqEq,
    /// `!=`
    Neq,
    /// `=~`
    EqRegex,
    /// `!~`
    NeqRegex,
    /// `+` `-` `*` `/` `%` `^`
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Caret,
    /// `<`, `<=`, `>`, `>=`
    Lt,
    Le,
    Gt,
    Ge,
    /// `@` — the `metric @ <timestamp>` modifier.
    At,
    /// `:` — used inside subquery `[range:step]`.
    Colon,
    /// `and`, `or`, `unless`, `by`, `without`, `on`, `ignoring`,
    /// `group_left`, `group_right`, `offset`, `bool`
    KwAnd,
    KwOr,
    KwUnless,
    KwBy,
    KwWithout,
    KwOn,
    KwIgnoring,
    KwGroupLeft,
    KwGroupRight,
    KwOffset,
    KwBool,
}

/// One token plus its source span (byte offsets).
#[derive(Debug, Clone, PartialEq)]
pub struct Spanned {
    pub token: Token,
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Error)]
pub enum LexError {
    #[error("unexpected character {ch:?} at offset {offset}")]
    UnexpectedChar { ch: char, offset: usize },
    #[error("unterminated string starting at offset {offset}")]
    UnterminatedString { offset: usize },
    #[error("invalid escape sequence at offset {offset}: \\{ch:?}")]
    InvalidEscape { offset: usize, ch: char },
    #[error("invalid number {raw:?} at offset {offset}")]
    InvalidNumber { offset: usize, raw: String },
    #[error("invalid duration {raw:?} at offset {offset}")]
    InvalidDuration { offset: usize, raw: String },
}

/// Tokenise `src`. Whitespace + comments are skipped.
///
/// # Errors
/// Returns [`LexError`] on the first malformed token.
#[allow(clippy::too_many_lines)]
pub fn tokenize(src: &str) -> Result<Vec<Spanned>, LexError> {
    let bytes = src.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();

    while i < bytes.len() {
        let c = bytes[i];
        // Whitespace
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        // Comments — `# ...` to end of line
        if c == b'#' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        let start = i;
        // Identifiers / keywords / durations.
        if c.is_ascii_alphabetic() || c == b'_' {
            let id_start = i;
            while i < bytes.len() {
                let b = bytes[i];
                if b.is_ascii_alphanumeric() || b == b'_' || b == b':' {
                    i += 1;
                } else {
                    break;
                }
            }
            let id = &src[id_start..i];
            // Keyword?
            let token = match id {
                "and" => Token::KwAnd,
                "or" => Token::KwOr,
                "unless" => Token::KwUnless,
                "by" => Token::KwBy,
                "without" => Token::KwWithout,
                "on" => Token::KwOn,
                "ignoring" => Token::KwIgnoring,
                "group_left" => Token::KwGroupLeft,
                "group_right" => Token::KwGroupRight,
                "offset" => Token::KwOffset,
                "bool" => Token::KwBool,
                other => Token::Identifier(other.to_string()),
            };
            out.push(Spanned { token, start, end: i });
            continue;
        }

        // Numbers. A leading `-` or `+` is handled by the parser (unary op).
        if c.is_ascii_digit() || (c == b'.' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit())
        {
            let n_start = i;
            // Consume the integer/float, then optionally a duration suffix.
            while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
                i += 1;
            }
            // Exponent (e+10, E-3)
            if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
                i += 1;
                if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
                    i += 1;
                }
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
            }
            // Duration suffix? A bare integer followed by `ms`/`s`/`m`/`h`/`d`/`w`/`y`.
            // Only attempt the duration path if there's a unit AND there's
            // no preceding decimal point (PromQL durations are integers).
            let raw = &src[n_start..i];
            let is_integer = !raw.contains('.') && !raw.contains('e') && !raw.contains('E');
            if is_integer
                && i < bytes.len()
                && (bytes[i] == b'm'
                    || bytes[i] == b's'
                    || bytes[i] == b'h'
                    || bytes[i] == b'd'
                    || bytes[i] == b'w'
                    || bytes[i] == b'y')
            {
                // Consume the unit run greedily so `1h30m` is one token.
                while i < bytes.len()
                    && (bytes[i].is_ascii_digit()
                        || matches!(bytes[i], b'm' | b's' | b'h' | b'd' | b'w' | b'y'))
                {
                    i += 1;
                }
                let raw_dur = &src[n_start..i];
                let ms = parse_duration_ms(raw_dur).ok_or_else(|| LexError::InvalidDuration {
                    offset: start,
                    raw: raw_dur.into(),
                })?;
                out.push(Spanned { token: Token::DurationMs(ms), start, end: i });
                continue;
            }
            let v: f64 = raw
                .parse()
                .map_err(|_| LexError::InvalidNumber { offset: start, raw: raw.into() })?;
            out.push(Spanned { token: Token::Number(v), start, end: i });
            continue;
        }

        // Strings: " or '
        if c == b'"' || c == b'\'' {
            let quote = c;
            i += 1;
            let mut s = String::new();
            loop {
                if i >= bytes.len() {
                    return Err(LexError::UnterminatedString { offset: start });
                }
                let b = bytes[i];
                if b == quote {
                    i += 1;
                    break;
                }
                if b == b'\\' {
                    i += 1;
                    if i >= bytes.len() {
                        return Err(LexError::UnterminatedString { offset: start });
                    }
                    let esc = bytes[i] as char;
                    match esc {
                        'n' => s.push('\n'),
                        't' => s.push('\t'),
                        'r' => s.push('\r'),
                        '\\' => s.push('\\'),
                        '"' => s.push('"'),
                        '\'' => s.push('\''),
                        other => return Err(LexError::InvalidEscape { offset: i, ch: other }),
                    }
                    i += 1;
                    continue;
                }
                // Pull one UTF-8 char.
                let ch = src[i..]
                    .chars()
                    .next()
                    .ok_or(LexError::UnterminatedString { offset: start })?;
                s.push(ch);
                i += ch.len_utf8();
            }
            out.push(Spanned { token: Token::String(s), start, end: i });
            continue;
        }

        // Single-/two-character operators.
        let (token, len) = match (c, bytes.get(i + 1).copied()) {
            (b'(', _) => (Token::LParen, 1),
            (b')', _) => (Token::RParen, 1),
            (b'{', _) => (Token::LBrace, 1),
            (b'}', _) => (Token::RBrace, 1),
            (b'[', _) => (Token::LBracket, 1),
            (b']', _) => (Token::RBracket, 1),
            (b',', _) => (Token::Comma, 1),
            (b'@', _) => (Token::At, 1),
            (b':', _) => (Token::Colon, 1),
            (b'+', _) => (Token::Plus, 1),
            (b'-', _) => (Token::Minus, 1),
            (b'*', _) => (Token::Star, 1),
            (b'/', _) => (Token::Slash, 1),
            (b'%', _) => (Token::Percent, 1),
            (b'^', _) => (Token::Caret, 1),
            (b'=', Some(b'=')) => (Token::EqEq, 2),
            (b'=', Some(b'~')) => (Token::EqRegex, 2),
            (b'=', _) => (Token::Eq, 1),
            (b'!', Some(b'=')) => (Token::Neq, 2),
            (b'!', Some(b'~')) => (Token::NeqRegex, 2),
            (b'<', Some(b'=')) => (Token::Le, 2),
            (b'<', _) => (Token::Lt, 1),
            (b'>', Some(b'=')) => (Token::Ge, 2),
            (b'>', _) => (Token::Gt, 1),
            (ch, _) => return Err(LexError::UnexpectedChar { ch: ch as char, offset: i }),
        };
        i += len;
        out.push(Spanned { token, start, end: i });
    }
    Ok(out)
}

fn parse_duration_ms(s: &str) -> Option<i64> {
    // Parse runs of `<int><unit>` and accumulate.
    let bytes = s.as_bytes();
    let mut total: i64 = 0;
    let mut i = 0;
    while i < bytes.len() {
        let num_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == num_start {
            return None;
        }
        let n: i64 = s[num_start..i].parse().ok()?;
        if i >= bytes.len() {
            return None;
        }
        // Recognise `ms` (two-char) before single-char units.
        let (unit_ms, advance): (i64, usize) = match (bytes[i], bytes.get(i + 1).copied()) {
            (b'm', Some(b's')) => (1, 2),
            (b's', _) => (1_000, 1),
            (b'm', _) => (60_000, 1),
            (b'h', _) => (3_600_000, 1),
            (b'd', _) => (86_400_000, 1),
            (b'w', _) => (7 * 86_400_000, 1),
            (b'y', _) => (365 * 86_400_000, 1),
            _ => return None,
        };
        total = total.checked_add(n.checked_mul(unit_ms)?)?;
        i += advance;
    }
    Some(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex(src: &str) -> Vec<Token> {
        tokenize(src).unwrap().into_iter().map(|s| s.token).collect()
    }

    #[test]
    fn identifiers_and_keywords() {
        let toks = lex("foo bar and or by");
        assert_eq!(
            toks,
            vec![
                Token::Identifier("foo".into()),
                Token::Identifier("bar".into()),
                Token::KwAnd,
                Token::KwOr,
                Token::KwBy,
            ]
        );
    }

    #[test]
    fn metric_name_with_colon() {
        let toks = lex("node:cpu:total");
        assert_eq!(toks, vec![Token::Identifier("node:cpu:total".into())]);
    }

    #[test]
    fn numbers() {
        let toks = lex("0 1 1.5 .25 1e3 2.5E-2");
        assert!(matches!(toks[0], Token::Number(n) if (n - 0.0).abs() < 1e-9));
        assert!(matches!(toks[1], Token::Number(n) if (n - 1.0).abs() < 1e-9));
        assert!(matches!(toks[2], Token::Number(n) if (n - 1.5).abs() < 1e-9));
        assert!(matches!(toks[3], Token::Number(n) if (n - 0.25).abs() < 1e-9));
        assert!(matches!(toks[4], Token::Number(n) if (n - 1000.0).abs() < 1e-9));
        assert!(matches!(toks[5], Token::Number(n) if (n - 0.025).abs() < 1e-3));
    }

    #[test]
    fn durations() {
        let toks = lex("5m 30s 1h30m 100ms");
        assert_eq!(
            toks,
            vec![
                Token::DurationMs(5 * 60 * 1000),
                Token::DurationMs(30 * 1000),
                Token::DurationMs(60 * 60 * 1000 + 30 * 60 * 1000),
                Token::DurationMs(100),
            ]
        );
    }

    #[test]
    fn strings() {
        let toks = lex(r#""hello" 'world' "esc\n\t\\\"" "#);
        assert_eq!(
            toks,
            vec![
                Token::String("hello".into()),
                Token::String("world".into()),
                Token::String("esc\n\t\\\"".into()),
            ]
        );
    }

    #[test]
    fn operators() {
        let toks = lex("= == != =~ !~ < <= > >= + - * / % ^");
        assert_eq!(
            toks,
            vec![
                Token::Eq,
                Token::EqEq,
                Token::Neq,
                Token::EqRegex,
                Token::NeqRegex,
                Token::Lt,
                Token::Le,
                Token::Gt,
                Token::Ge,
                Token::Plus,
                Token::Minus,
                Token::Star,
                Token::Slash,
                Token::Percent,
                Token::Caret,
            ]
        );
    }

    #[test]
    fn comments_skipped() {
        let toks = lex("foo # this is a comment\nbar");
        assert_eq!(toks, vec![Token::Identifier("foo".into()), Token::Identifier("bar".into())]);
    }

    #[test]
    fn punctuation() {
        let toks = lex("(){}[],");
        assert_eq!(
            toks,
            vec![
                Token::LParen,
                Token::RParen,
                Token::LBrace,
                Token::RBrace,
                Token::LBracket,
                Token::RBracket,
                Token::Comma,
            ]
        );
    }

    #[test]
    fn unterminated_string_errors() {
        assert!(tokenize("\"hello").is_err());
    }
}
