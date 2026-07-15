//! MetricsQL tokenizer and duration parsing.
//!
//! Port of `lexer.go`.

use crate::binaryop::scan_binary_op_prefix;
use crate::strutil::{is_print, parse_go_float, parse_go_int};
use crate::ParseError;
use std::fmt::Write as _;

/// Tokenizer over a MetricsQL query string.
///
/// Port of the Go `lexer` struct. `token` contains the currently parsed
/// token; an empty token means EOF.
pub(crate) struct Lexer {
    pub(crate) token: String,
    prev_tokens: Vec<String>,
    next_tokens: Vec<String>,
    s_orig: String,
    s_tail: String,
    err: Option<ParseError>,
}

impl Lexer {
    pub(crate) fn new(s: &str) -> Lexer {
        Lexer {
            token: String::new(),
            prev_tokens: Vec::new(),
            next_tokens: Vec::new(),
            s_orig: s.to_string(),
            s_tail: s.to_string(),
            err: None,
        }
    }

    /// Port of Go `lexer.Context`.
    pub(crate) fn context(&self) -> String {
        format!("{}{}", self.token, self.s_tail)
    }

    /// Port of Go `lexer.PushBack`: overrides the current token and pushes
    /// `s_head` back in front of the unparsed tail.
    pub(crate) fn push_back(&mut self, curr_token: &str, s_head: &str) {
        self.token = curr_token.to_string();
        let mut tail = String::with_capacity(s_head.len() + self.s_tail.len());
        tail.push_str(s_head);
        tail.push_str(&self.s_tail);
        self.s_tail = tail;
    }

    /// Advances to the next token. Port of Go `lexer.Next`.
    pub(crate) fn next(&mut self) -> Result<(), ParseError> {
        if let Some(err) = &self.err {
            return Err(err.clone());
        }
        // The current token stays in place when scanning fails, matching the
        // Go lexer (parse code may still call prev() after an error).
        self.prev_tokens.push(self.token.clone());
        if let Some(token) = self.next_tokens.pop() {
            self.token = token;
            return Ok(());
        }
        match self.next_inner() {
            Ok(token) => {
                self.token = token;
                Ok(())
            }
            Err(err) => {
                self.err = Some(err.clone());
                Err(err)
            }
        }
    }

    /// Rewinds to the previous token. Port of Go `lexer.Prev`.
    pub(crate) fn prev(&mut self) {
        let token = std::mem::take(&mut self.token);
        self.next_tokens.push(token);
        self.token = self.prev_tokens.pop().expect("BUG: no previous token");
    }

    /// Scans the next token from the tail. Port of Go `lexer.next`.
    fn next_inner(&mut self) -> Result<String, ParseError> {
        loop {
            // Skip whitespace.
            let ws = self
                .s_tail
                .bytes()
                .take_while(|&c| is_space_char(c))
                .count();
            if ws > 0 {
                self.s_tail = self.s_tail.split_off(ws);
            }
            if self.s_tail.is_empty() {
                return Ok(String::new());
            }

            let s = self.s_tail.as_str();
            match s.as_bytes()[0] {
                b'#' => {
                    // Skip comment till the end of line.
                    match s.find('\n') {
                        None => {
                            self.s_tail.clear();
                            return Ok(String::new());
                        }
                        Some(n) => {
                            self.s_tail = self.s_tail.split_off(n + 1);
                            continue;
                        }
                    }
                }
                b'{' | b'}' | b'[' | b']' | b'(' | b')' | b',' | b'@' => {
                    return Ok(self.take_token(1));
                }
                _ => {}
            }
            if is_ident_prefix(s) {
                let n = scan_ident(s).len();
                return Ok(self.take_token(n));
            }
            if is_string_prefix(s) {
                let n = scan_string(s)?;
                return Ok(self.take_token(n));
            }
            let n = scan_binary_op_prefix(s);
            if n > 0 {
                return Ok(self.take_token(n));
            }
            let n = scan_tag_filter_op_prefix(s);
            if n > 0 {
                return Ok(self.take_token(n));
            }
            let n = scan_duration(s);
            if n > 0 {
                return Ok(self.take_token(n as usize));
            }
            if is_positive_number_prefix(s) {
                let n = scan_positive_number(s)?.len();
                return Ok(self.take_token(n));
            }
            if let Some(rest) = s.strip_prefix("$__interval") {
                self.s_tail = rest.to_string();
                return Ok("$__interval".to_string());
            }
            if let Some(rest) = s.strip_prefix("$__rate_interval") {
                self.s_tail = rest.to_string();
                return Ok("$__interval".to_string());
            }
            let pos = self.s_orig.len() - self.s_tail.len();
            return Err(ParseError::with_pos(format!("cannot recognize {s:?}"), pos));
        }
    }

    /// Consumes `n` bytes from the tail and returns them as the token.
    fn take_token(&mut self, n: usize) -> String {
        let tail = self.s_tail.split_off(n);
        std::mem::replace(&mut self.s_tail, tail)
    }
}

/// Port of Go `isEOF`.
pub(crate) fn is_eof(s: &str) -> bool {
    s.is_empty()
}

/// Port of Go `scanString`: returns the byte length of the quoted string
/// token at the start of `s` (including quotes).
fn scan_string(s: &str) -> Result<usize, ParseError> {
    if s.len() < 2 {
        return Err(ParseError::new(format!(
            "cannot find end of string in {s:?}"
        )));
    }
    let b = s.as_bytes();
    let quote = b[0];
    let mut i = 1;
    loop {
        let n = match b[i..].iter().position(|&c| c == quote) {
            Some(n) => n,
            None => {
                return Err(ParseError::new(format!(
                    "cannot find closing quote {} for the string {:?}",
                    quote as char, s
                )));
            }
        };
        i += n;
        let mut bs = 0;
        while bs < i && b[i - bs - 1] == b'\\' {
            bs += 1;
        }
        if bs % 2 == 0 {
            return Ok(i + 1);
        }
        i += 1;
    }
}

/// Port of Go `parsePositiveNumber`: parses numbers with an optional
/// `k`/`Ki`/`M`/`Gi`/... multiplier suffix, special integer prefixes
/// (`0x`, `0o`, `0b`, leading zero octal) and `Inf`/`NaN`.
pub(crate) fn parse_positive_number(s: &str) -> Result<f64, ParseError> {
    if is_special_integer_prefix(s) {
        let n = parse_go_int(s)
            .ok_or_else(|| ParseError::new(format!("cannot parse integer number {s:?}")))?;
        return Ok(n as f64);
    }
    let s = s.to_ascii_lowercase();
    let (num, m) = strip_num_multiplier(&s);
    let v =
        parse_go_float(num).ok_or_else(|| ParseError::new(format!("cannot parse number {s:?}")))?;
    Ok(v * m)
}

/// Splits a lowercase numeric literal into (number, multiplier).
/// The suffix checks follow the same order as the Go `switch` statement.
fn strip_num_multiplier(s: &str) -> (&str, f64) {
    const KI: f64 = 1024.0;
    const K: f64 = 1000.0;
    let checks: &[(&str, f64)] = &[
        ("kib", KI),
        ("ki", KI),
        ("kb", K),
        ("k", K),
        ("mib", KI * KI),
        ("mi", KI * KI),
        ("mb", K * K),
        ("m", K * K),
        ("gib", KI * KI * KI),
        ("gi", KI * KI * KI),
        ("gb", K * K * K),
        ("g", K * K * K),
        ("tib", KI * KI * KI * KI),
        ("ti", KI * KI * KI * KI),
        ("tb", K * K * K * K),
        ("t", K * K * K * K),
    ];
    for (suffix, m) in checks {
        if let Some(num) = s.strip_suffix(suffix) {
            return (num, *m);
        }
    }
    (s, 1.0)
}

/// Port of Go `scanPositiveNumber`: returns the number token prefixing `s`.
pub(crate) fn scan_positive_number(s: &str) -> Result<&str, ParseError> {
    // Scan the integer part. It may be empty if a fractional part exists.
    let b = s.as_bytes();
    let mut i = 0;
    let (skip_chars, is_hex) = scan_special_integer_prefix(s);
    i += skip_chars;
    if is_hex {
        // Scan an integer hex number.
        while i < b.len() && b[i].is_ascii_hexdigit() {
            i += 1;
        }
        return Ok(&s[..i]);
    }
    while i < b.len() && is_decimal_char_or_underscore(b[i]) {
        i += 1;
    }

    if i == s.len() {
        if i == 0 {
            return Err(ParseError::new("number cannot be empty"));
        }
        return Ok(s);
    }
    let n = scan_num_multiplier(&s[i..]);
    if n > 0 {
        return Ok(&s[..i + n]);
    }
    if b[i] != b'.' && b[i] != b'e' && b[i] != b'E' {
        if i == 0 {
            return Err(ParseError::new("missing positive number"));
        }
        return Ok(&s[..i]);
    }

    if b[i] == b'.' {
        // Scan the fractional part. It cannot be empty.
        i += 1;
        let mut j = i;
        while j < b.len() && is_decimal_char_or_underscore(b[j]) {
            j += 1;
        }
        i = j;
        if i == s.len() {
            return Ok(s);
        }
    }
    let n = scan_num_multiplier(&s[i..]);
    if n > 0 {
        return Ok(&s[..i + n]);
    }

    if b[i] != b'e' && b[i] != b'E' {
        return Ok(&s[..i]);
    }
    i += 1;

    // Scan the exponent part.
    if i == s.len() {
        return Err(ParseError::new(format!("missing exponent part in {s:?}")));
    }
    if b[i] == b'-' || b[i] == b'+' {
        i += 1;
    }
    let mut j = i;
    while j < b.len() && b[j].is_ascii_digit() {
        j += 1;
    }
    if j == i {
        return Err(ParseError::new(format!("missing exponent part in {s:?}")));
    }
    Ok(&s[..j])
}

/// Port of Go `scanNumMultiplier`: returns the length of the
/// `k`/`Ki`/`Mb`/... multiplier suffix prefixing `s`.
pub(crate) fn scan_num_multiplier(s: &str) -> usize {
    let b = s.as_bytes();
    let pfx = |p: &str| b.len() >= p.len() && b[..p.len()].eq_ignore_ascii_case(p.as_bytes());
    if pfx("kib") || pfx("mib") || pfx("gib") || pfx("tib") {
        3
    } else if pfx("ki")
        || pfx("kb")
        || pfx("mi")
        || pfx("mb")
        || pfx("gi")
        || pfx("gb")
        || pfx("ti")
        || pfx("tb")
    {
        2
    } else if pfx("k") || pfx("m") || pfx("g") || pfx("t") {
        1
    } else {
        0
    }
}

/// Port of Go `scanIdent`: returns the ident prefixing `s`, including escape
/// sequences.
pub(crate) fn scan_ident(s: &str) -> &str {
    let mut i = 0;
    while i < s.len() {
        let r = s[i..].chars().next().expect("non-empty tail");
        let size = r.len_utf8();
        if (i == 0 && is_first_ident_char(r)) || (i > 0 && is_ident_char(r)) {
            i += size;
            continue;
        }
        if r != '\\' {
            break;
        }
        i += size;
        match decode_escape_sequence(&s[i..]) {
            None => {
                // Invalid escape sequence.
                i -= size;
                break;
            }
            Some((_, n)) => i += n,
        }
    }
    assert!(
        i > 0,
        "BUG: scanIdent couldn't find a single ident char; make sure is_ident_prefix is called before scan_ident"
    );
    &s[..i]
}

/// Port of Go `unescapeIdent`.
pub(crate) fn unescape_ident(s: &str) -> String {
    let Some(mut n) = s.find('\\') else {
        return s.to_string();
    };
    let mut dst = String::with_capacity(s.len());
    let mut s = s;
    loop {
        dst.push_str(&s[..n]);
        s = &s[n + 1..];
        match decode_escape_sequence(s) {
            None => {
                // Cannot decode the escape sequence. Put it in the output as is.
                dst.push('\\');
            }
            Some((r, size)) => {
                dst.push(r);
                s = &s[size..];
            }
        }
        match s.find('\\') {
            None => {
                dst.push_str(s);
                return dst;
            }
            Some(m) => n = m,
        }
    }
}

/// Port of Go `hasEscapedChars`: true if `s` contains chars that require
/// escaping when serialized as an ident.
pub(crate) fn has_escaped_chars(s: &str) -> bool {
    let mut first = true;
    for r in s.chars() {
        let ok = if first {
            is_first_ident_char(r)
        } else {
            is_ident_char(r)
        };
        if !ok {
            return true;
        }
        first = false;
    }
    false
}

/// Port of Go `appendQuotedIdent`.
pub(crate) fn append_quoted_ident(dst: &mut String, s: &str) {
    dst.push('"');
    for r in s.chars() {
        if r == '"' || r == '\\' {
            dst.push('\\');
        }
        dst.push(r);
    }
    dst.push('"');
}

/// Port of Go `appendEscapedIdent`.
pub(crate) fn append_escaped_ident(dst: &mut String, s: &str) {
    let mut first = true;
    for r in s.chars() {
        let ok = if first {
            is_first_ident_char(r)
        } else {
            is_ident_char(r)
        };
        if ok {
            dst.push(r);
        } else {
            append_escape_sequence(dst, r);
        }
        first = false;
    }
}

/// Port of Go `ifEscapedCharsAppendQuotedIdent`.
pub(crate) fn if_escaped_chars_append_quoted_ident(dst: &mut String, s: &str) {
    if has_escaped_chars(s) {
        append_quoted_ident(dst, s);
    } else {
        append_escaped_ident(dst, s);
    }
}

/// Port of Go `scanTagFilterOpPrefix`: length of `=`, `=~`, `!=` or `!~`
/// prefixing `s`; 0 if none.
fn scan_tag_filter_op_prefix(s: &str) -> usize {
    let b = s.as_bytes();
    if b.len() >= 2 && matches!(&b[..2], b"=~" | b"!~" | b"!=") {
        return 2;
    }
    if !b.is_empty() && b[0] == b'=' {
        return 1;
    }
    0
}

/// Port of Go `isInfOrNaN`.
pub(crate) fn is_inf_or_nan(s: &str) -> bool {
    if s.len() != 3 {
        return false;
    }
    s.eq_ignore_ascii_case("inf") || s.eq_ignore_ascii_case("nan")
}

/// Port of Go `isOffset`.
pub(crate) fn is_offset(s: &str) -> bool {
    s.eq_ignore_ascii_case("offset")
}

/// Port of Go `isStringPrefix`.
///
/// See <https://prometheus.io/docs/prometheus/latest/querying/basics/#string-literals>
pub(crate) fn is_string_prefix(s: &str) -> bool {
    matches!(s.as_bytes().first(), Some(b'"' | b'\'' | b'`'))
}

/// Port of Go `isPositiveNumberPrefix`.
pub(crate) fn is_positive_number_prefix(s: &str) -> bool {
    let b = s.as_bytes();
    if b.is_empty() {
        return false;
    }
    if b[0].is_ascii_digit() {
        return true;
    }
    // Check for .234 numbers.
    if b[0] != b'.' || b.len() < 2 {
        return false;
    }
    b[1].is_ascii_digit()
}

/// Port of Go `isSpecialIntegerPrefix`.
pub(crate) fn is_special_integer_prefix(s: &str) -> bool {
    scan_special_integer_prefix(s).0 > 0
}

/// Port of Go `scanSpecialIntegerPrefix`: returns `(skip_chars, is_hex)`.
fn scan_special_integer_prefix(s: &str) -> (usize, bool) {
    let b = s.as_bytes();
    if b.is_empty() || b[0] != b'0' {
        return (0, false);
    }
    let Some(&c) = b.get(1) else {
        return (0, false);
    };
    let c = c.to_ascii_lowercase();
    if c.is_ascii_digit() {
        // Octal number: 0123
        return (1, false);
    }
    if c == b'x' {
        // 0x prefix
        return (2, true);
    }
    if c == b'o' || c == b'b' {
        // 0o or 0b prefix
        return (2, false);
    }
    (0, false)
}

/// Port of Go `isPositiveDuration`.
pub(crate) fn is_positive_duration(s: &str) -> bool {
    if s == "$__interval" {
        return true;
    }
    scan_duration(s) == s.len() as isize
}

/// Returns a positive duration in milliseconds for the given `s` and `step`.
///
/// The duration in `s` may be combined, i.e. `2h5m` or `2h-5m`.
/// An error is returned if the duration in `s` is negative.
///
/// Port of Go `PositiveDurationValue`.
pub fn positive_duration_value(s: &str, step: i64) -> crate::Result<i64> {
    let d = duration_value(s, step)?;
    if d < 0 {
        return Err(ParseError::new(format!(
            "duration cannot be negative; got {s:?}"
        )));
    }
    Ok(d)
}

/// Returns the duration in milliseconds for the given `s` and `step`.
///
/// The duration in `s` may be combined, i.e. `2h5m`, `-2h5m` or `2h-5m`.
/// The returned duration value can be negative. Plain numbers are treated as
/// a number of seconds.
///
/// Port of Go `DurationValue`.
pub fn duration_value(s: &str, step: i64) -> crate::Result<i64> {
    if s.is_empty() {
        return Err(ParseError::new("duration cannot be empty"));
    }
    let last_char = *s.as_bytes().last().expect("non-empty");
    if last_char.is_ascii_digit() || last_char == b'.' {
        // Try parsing a floating-point duration.
        if let Some(d) = parse_go_float(s) {
            // Convert the duration to milliseconds.
            return Ok((d * 1000.0) as i64);
        }
    }
    let mut is_minus = false;
    let mut d = 0f64;
    let mut s = s;
    while !s.is_empty() {
        let n = scan_single_duration(s, true);
        if n <= 0 {
            return Err(ParseError::new(format!("cannot parse duration {s:?}")));
        }
        let ds = &s[..n as usize];
        s = &s[n as usize..];
        let mut d_local = parse_single_duration(ds, step)?;
        if is_minus && d_local > 0.0 {
            d_local = -d_local;
        }
        d += d_local;
        if d_local < 0.0 {
            is_minus = true;
        }
    }
    if d > i64::MAX as f64 {
        // Truncate too big durations.
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8447
        return Ok(i64::MAX);
    }
    if d < i64::MIN as f64 {
        // Truncate too small durations.
        return Ok(i64::MIN);
    }
    Ok(d as i64)
}

/// Port of Go `parseSingleDuration`: returns the duration in milliseconds.
fn parse_single_duration(s: &str, step: i64) -> crate::Result<f64> {
    if s == "$__interval" {
        return Ok(step as f64);
    }
    let s = s.to_ascii_lowercase();
    let mut num_part = &s[..s.len() - 1];
    // Strip the trailing "m" if the duration is in ms.
    num_part = num_part.strip_suffix('m').unwrap_or(num_part);
    let f = parse_go_float(num_part)
        .ok_or_else(|| ParseError::new(format!("cannot parse duration {s:?}")))?;
    let mp: f64 = match &s[num_part.len()..] {
        "ms" => 1.0,
        "s" => 1000.0,
        "m" => 60.0 * 1000.0,
        "h" => 60.0 * 60.0 * 1000.0,
        "d" => 24.0 * 60.0 * 60.0 * 1000.0,
        "w" => 7.0 * 24.0 * 60.0 * 60.0 * 1000.0,
        "y" => 365.0 * 24.0 * 60.0 * 60.0 * 1000.0,
        "i" => step as f64,
        _ => {
            return Err(ParseError::new(format!("invalid duration suffix in {s:?}")));
        }
    };
    Ok(mp * f)
}

/// Port of Go `scanDuration`: scans a duration, which must start with a
/// positive number, e.g. `123h`, `3h5m` or `3.4d-35.66s`.
/// Returns -1 if `s` doesn't start with a duration.
pub(crate) fn scan_duration(s: &str) -> isize {
    // The first part must be non-negative.
    let n = scan_single_duration(s, false);
    if n <= 0 {
        return -1;
    }
    let mut s = &s[n as usize..];
    let mut i = n;
    loop {
        // Other parts may be negative.
        let n = scan_single_duration(s, true);
        if n <= 0 {
            return i;
        }
        s = &s[n as usize..];
        i += n;
    }
}

/// Port of Go `scanSingleDuration`.
fn scan_single_duration(s: &str, can_be_negative: bool) -> isize {
    if s.is_empty() {
        return -1;
    }
    let b = s.as_bytes();
    let mut i = 0;
    if b[0] == b'-' && can_be_negative {
        i += 1;
    }
    if &s[i..] == "$__interval" {
        return (i + "$__interval".len()) as isize;
    }
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    if i == 0 || i == s.len() {
        return -1;
    }
    if b[i] == b'.' {
        let j = i;
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == j || i == s.len() {
            return -1;
        }
    }
    match b[i].to_ascii_lowercase() {
        b'm' => {
            if i + 1 < s.len() {
                match b[i + 1].to_ascii_lowercase() {
                    b's' => {
                        // Duration in ms.
                        return (i + 2) as isize;
                    }
                    b'i' | b'b' => {
                        // This is not a duration, but a Mi or MB suffix.
                        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3664
                        return -1;
                    }
                    _ => {}
                }
            }
            // Allow small m for duration in minutes. Big M means 1e6.
            // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3664
            if b[i] == b'm' {
                return (i + 1) as isize;
            }
            -1
        }
        b's' | b'h' | b'd' | b'w' | b'y' | b'i' => (i + 1) as isize,
        _ => -1,
    }
}

fn is_decimal_char_or_underscore(ch: u8) -> bool {
    ch.is_ascii_digit() || ch == b'_'
}

/// Port of Go `isIdentPrefix`.
pub(crate) fn is_ident_prefix(s: &str) -> bool {
    let Some(r) = s.chars().next() else {
        return false;
    };
    if r == '\\' {
        return decode_escape_sequence(&s[r.len_utf8()..]).is_some();
    }
    is_first_ident_char(r)
}

/// Port of Go `isFirstIdentChar`.
pub(crate) fn is_first_ident_char(r: char) -> bool {
    r.is_alphabetic() || r == '_' || r == ':'
}

/// Port of Go `isIdentChar`.
pub(crate) fn is_ident_char(r: char) -> bool {
    is_first_ident_char(r) || r.is_ascii_digit() || r == '.'
}

fn is_space_char(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

/// Port of Go `appendEscapeSequence`.
fn append_escape_sequence(dst: &mut String, r: char) {
    dst.push('\\');
    if is_print(r) {
        dst.push(r);
        return;
    }
    // Hex-encode non-printable chars.
    let v = u32::from(r);
    if v < 256 {
        let _ = write!(dst, "x{v:02x}");
    } else {
        // Like the Go original, only the lower 16 bits are encoded.
        let _ = write!(dst, "u{:04x}", v & 0xffff);
    }
}

/// Port of Go `decodeEscapeSequence`: decodes the escape sequence after a
/// backslash. Returns `None` when the sequence is invalid (Go returns
/// `utf8.RuneError` in that case).
fn decode_escape_sequence(s: &str) -> Option<(char, usize)> {
    let b = s.as_bytes();
    if b.first().is_some_and(|&c| c == b'x' || c == b'X') {
        if b.len() >= 3 {
            let h1 = (b[1] as char).to_digit(16);
            let h2 = (b[2] as char).to_digit(16);
            if let (Some(h1), Some(h2)) = (h1, h2) {
                let r = char::from_u32((h1 << 4) | h2)?;
                return Some((r, 3));
            }
        }
        return None;
    }
    if b.first().is_some_and(|&c| c == b'u' || c == b'U') {
        if b.len() >= 5 {
            let hs: Option<Vec<u32>> = (1..5).map(|i| (b[i] as char).to_digit(16)).collect();
            if let Some(hs) = hs {
                let v = (hs[0] << 12) | (hs[1] << 8) | (hs[2] << 4) | hs[3];
                let r = char::from_u32(v)?;
                return Some((r, 5));
            }
        }
        return None;
    }
    let r = s.chars().next()?;
    if is_print(r) {
        return Some((r, r.len_utf8()));
    }
    // Improperly escaped non-printable char.
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // Port of TestScanNumMultiplier.
    #[test]
    fn scan_num_multiplier_cases() {
        let f = |s: &str, len_expected: usize| {
            assert_eq!(
                scan_num_multiplier(s),
                len_expected,
                "unexpected len for scanNumMultiplier({s:?})"
            );
        };
        f("", 0);
        f("foo", 0);
        f("k", 1);
        f("KB", 2);
        f("Ki", 2);
        f("kiB", 3);
        f("M", 1);
        f("Mb", 2);
        f("mi", 2);
        f("MiB", 3);
        f("g", 1);
        f("GB", 2);
        f("GI", 2);
        f("GIB", 3);
        f("t", 1);
        f("tB", 2);
        f("tI", 2);
        f("tIb", 3);
        f("Gb   ", 2);
        f("tIb + 5", 3);
    }

    // Port of TestScanPositiveNumberSuccess.
    #[test]
    fn scan_positive_number_success() {
        let f = |s: &str, ns_expected: &str| {
            let ns = scan_positive_number(s)
                .unwrap_or_else(|e| panic!("unexpected error in scanPositiveNumber({s:?}): {e}"));
            assert_eq!(ns, ns_expected, "unexpected number scanned from {s:?}");
        };
        f("123", "123");
        f("123+5", "123");
        f("1.23 ", "1.23");
        f("12e5", "12e5");
        f("1.3E-3/5", "1.3E-3");
        f("234.", "234.");
        f("234. + foo", "234.");
        f("0xfe", "0xfe");
        f("0b0110", "0b0110");
        f("0O765", "0O765");
        f("0765", "0765");
        f("2k*34", "2k");
        f("2.3Kb / 43", "2.3Kb");
        f("3ki", "3ki");
        f("4.5Kib", "4.5Kib");
        f("2m", "2m");
        f("2.3Mb", "2.3Mb");
        f("3Mi", "3Mi");
        f("4.5mib", "4.5mib");
        f("2G", "2G");
        f("2.3gB", "2.3gB");
        f("3gI", "3gI");
        f("4.5GiB / foo", "4.5GiB");
        f("2T", "2T");
        f("2.3tb", "2.3tb");
        f("3tI", "3tI");
        f("4.5TIB   ", "4.5TIB");
        // Numbers with underscores - see https://github.com/golang/go/issues/28493
        f("1_2_334", "1_2_334");
        f("1_2.3_34_5", "1_2.3_34_5");
        f("1_2.3_34_5e8", "1_2.3_34_5e8");
    }

    // Port of TestScanPositiveNumberFailure.
    #[test]
    fn scan_positive_number_failure() {
        for s in ["", "foobar", "123e", "1233Ebc", "12.34E+abc", "12.34e-"] {
            assert!(
                scan_positive_number(s).is_err(),
                "expecting error in scanPositiveNumber({s:?})"
            );
        }
    }

    // Port of TestParsePositiveNumberSuccess.
    #[test]
    fn parse_positive_number_success() {
        let f = |s: &str, v_expected: f64| {
            let v = parse_positive_number(s)
                .unwrap_or_else(|e| panic!("unexpected error in parsePositiveNumber({s:?}): {e}"));
            if v.is_nan() {
                assert!(v_expected.is_nan(), "unexpected value for {s:?}: {v}");
            } else {
                assert_eq!(v, v_expected, "unexpected value for {s:?}");
            }
        };
        f("123", 123.0);
        f("1.23", 1.23);
        f("12e5", 12e5);
        f("1.3E-3", 1.3e-3);
        f("234.", 234.0);
        f("Inf", f64::INFINITY);
        f("NaN", f64::NAN);
        f("0xfe", 0xfe as f64);
        f("0b0110", 0b0110 as f64);
        f("0O765", 0o765 as f64);
        f("0765", 0o765 as f64);
        f("2k", 2.0 * 1000.0);
        f("2.3Kb", 2.3 * 1000.0);
        f("3ki", 3.0 * 1024.0);
        f("4.5Kib", 4.5 * 1024.0);
        f("2m", 2e6);
        f("2.3Mb", 2.3e6);
        f("3Mi", 3.0 * 1024.0 * 1024.0);
        f("4.5mib", 4.5 * 1024.0 * 1024.0);
        f("2G", 2e9);
        f("2.3gB", 2.3e9);
        f("3gI", 3.0 * 1024.0 * 1024.0 * 1024.0);
        f("4.5GiB", 4.5 * 1024.0 * 1024.0 * 1024.0);
        f("2T", 2e12);
        f("2.3tb", 2.3e12);
        f("3tI", 3.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0);
        f("4.5TIB", 4.5 * 1024.0 * 1024.0 * 1024.0 * 1024.0);
    }

    // Port of TestParsePositiveNumberFailure.
    #[test]
    fn parse_positive_number_failure() {
        for s in [
            "",
            "0xqwert",
            "foobar",
            "234.foobar",
            "123e",
            "1233Ebc",
            "12.34E+abc",
            "12.34e-",
            "12.weKB",
        ] {
            assert!(
                parse_positive_number(s).is_err(),
                "expecting error in parsePositiveNumber({s:?})"
            );
        }
    }

    // Port of TestIsSpecialIntegerPrefix.
    #[test]
    fn special_integer_prefix() {
        let f = |s: &str, expected: bool| {
            assert_eq!(
                is_special_integer_prefix(s),
                expected,
                "unexpected result for isSpecialIntegerPrefix({s:?})"
            );
        };
        f("", false);
        f("1", false);
        f("0", false);
        // Octal numbers.
        f("03", true);
        f("0o1", true);
        f("0O12", true);
        // Binary numbers.
        f("0b1110", true);
        f("0B0", true);
        // Hex numbers.
        f("0x1ffa", true);
        f("0X4", true);
    }

    // Port of TestUnescapeIdent.
    #[test]
    fn unescape_ident_cases() {
        let f = |s: &str, expected: &str| {
            assert_eq!(
                unescape_ident(s),
                expected,
                "unexpected result for unescapeIdent({s:?})"
            );
        };
        f("", "");
        f("a", "a");
        f("\\", r"\");
        f(r"\\", r"\");
        f(r"\foo\-bar", "foo-bar");
        f(r#"a\\\\b\"c\d"#, r#"a\\b"cd"#);
        f("foo.bar:baz_123", "foo.bar:baz_123");
        f(r"foo\ bar", "foo bar");
        f(r"\x21", "!");
        f(r"\X21", "!");
        f(r"\x7Dfoo\x2Fbar\-\xqw\x", "}foo/bar-\\xqw\\x");
        f(r"\п\р\и\в\е\т123", "привет123");
        f("123", "123");
        f(r"\123", "123");
        f(r"привет\-\foo", "привет-foo");
        f(r"\u0965", "\u{0965}");
        f(r"\U0965", "\u{0965}");
        f(r"\u202c", "\u{202c}");
        f(r"\U202ca", "\u{202c}a");
    }

    // Port of TestAppendEscapedIdent.
    #[test]
    fn append_escaped_ident_cases() {
        let f = |s: &str, expected: &str| {
            let mut dst = String::new();
            append_escaped_ident(&mut dst, s);
            assert_eq!(
                dst, expected,
                "unexpected result for appendEscapedIdent({s:?})"
            );
        };
        f("a", "a");
        f("a.b:c_23", "a.b:c_23");
        f("a b-cd+dd\\", r"a\ b\-cd\+dd\\");
        f("a\x1e\x20\x7e", r"a\x1e\ \~");
        f("\x2e\x2e", r"\..");
        f("123", r"\123");
        f("+43.6", r"\+43.6");
        f("привет123(a-b)", r"привет123\(a\-b\)");
        f("\u{0965}", "\\\u{0965}");
        f("\u{202c}", r"\u202c");
    }

    // Port of TestAppendQuotedIdent.
    #[test]
    fn append_quoted_ident_cases() {
        let f = |s: &str, expected: &str| {
            let mut dst = String::new();
            append_quoted_ident(&mut dst, s);
            assert_eq!(
                dst, expected,
                "unexpected result for appendQuotedIdent({s:?})"
            );
        };
        f("", r#""""#);
        f("a", r#""a""#);
        f("foo bar", r#""foo bar""#);
        f("foo\"bar", r#""foo\"bar""#);
        f(r"foo\bar", r#""foo\\bar""#);
        f(r#"a"b\c"d"#, r#""a\"b\\c\"d""#);
        f(r#"""#, r#""\"""#);
        f(r"\", r#""\\""#);
        f(r"\\", r#""\\\\""#);
        f(r#""quoted""#, r#""\"quoted\"""#);
    }

    // Port of TestScanIdent.
    #[test]
    fn scan_ident_cases() {
        let f = |s: &str, expected: &str| {
            assert_eq!(
                scan_ident(s),
                expected,
                "unexpected result for scanIdent({s:?})"
            );
        };
        f("a", "a");
        f("foo.bar:baz_123", "foo.bar:baz_123");
        f("a+b", "a");
        f("foo()", "foo");
        f(r"a\-b+c", r"a\-b");
        f(r"a\ b\\\ c\", r"a\ b\\\ c");
        f(r"\п\р\и\в\е\т123", r"\п\р\и\в\е\т123");
        f("привет123!foo", "привет123");
        f(r"\1fooЫ+bar", r"\1fooЫ");
        f(r"\u7834*аа", r"\u7834");
        f(r"\U7834*аа", r"\U7834");
        f(r"\x7834*аа", r"\x7834");
        f(r"\X7834*аа", r"\X7834");
        f(r"a\x+b", "a");
        f(r"a\x1+b", "a");
        f(r"a\x12+b", r"a\x12");
        f(r"a\u+b", "a");
        f(r"a\u1+b", "a");
        f(r"a\u12+b", "a");
        f(r"a\u123+b", "a");
        f(r"a\u1234+b", r"a\u1234");
        f("a\\\u{202c}", "a");
    }

    // Port of TestLexerNextPrev.
    #[test]
    fn lexer_next_prev() {
        let mut lex = Lexer::new("foo bar baz");
        assert_eq!(lex.token, "");
        lex.next().unwrap();
        assert_eq!(lex.token, "foo");

        // Rewind before the first item.
        lex.prev();
        assert_eq!(lex.token, "");
        lex.next().unwrap();
        assert_eq!(lex.token, "foo");
        lex.next().unwrap();
        assert_eq!(lex.token, "bar");

        // Rewind to the first item.
        lex.prev();
        assert_eq!(lex.token, "foo");
        lex.next().unwrap();
        assert_eq!(lex.token, "bar");
        lex.next().unwrap();
        assert_eq!(lex.token, "baz");

        // Go beyond the token stream.
        lex.next().unwrap();
        assert_eq!(lex.token, "");
        assert!(is_eof(&lex.token));
        lex.prev();
        assert_eq!(lex.token, "baz");

        // Go multiple times lex.next() beyond the token stream.
        lex.next().unwrap();
        assert_eq!(lex.token, "");
        assert!(is_eof(&lex.token));
        lex.next().unwrap();
        assert_eq!(lex.token, "");
        assert!(is_eof(&lex.token));
        lex.prev();
        assert_eq!(lex.token, "");
        assert!(is_eof(&lex.token));
    }

    fn test_lexer_success(s: &str, expected_tokens: &[&str]) {
        let mut lex = Lexer::new(s);
        let mut tokens: Vec<String> = Vec::new();
        loop {
            lex.next().unwrap();
            if is_eof(&lex.token) {
                break;
            }
            tokens.push(lex.token.clone());
        }
        assert_eq!(tokens, expected_tokens, "unexpected tokens for {s:?}");
    }

    // Port of TestLexerSuccess.
    #[test]
    fn lexer_success() {
        // An empty string.
        test_lexer_success("", &[]);
        // A string with whitespace.
        test_lexer_success("  \n\t\r ", &[]);
        // Just a metric name.
        test_lexer_success("metric", &["metric"]);
        // Metric name with spec chars.
        test_lexer_success(":foo.bar_", &[":foo.bar_"]);
        // Metric name with window.
        test_lexer_success("metric[5m]  ", &["metric", "[", "5m", "]"]);
        // Metric name with tag filters.
        test_lexer_success(
            r#"  metric:12.34{a="foo", b != "bar", c=~ "x.+y", d !~ "zzz"}"#,
            &[
                "metric:12.34",
                "{",
                "a",
                "=",
                r#""foo""#,
                ",",
                "b",
                "!=",
                r#""bar""#,
                ",",
                "c",
                "=~",
                r#""x.+y""#,
                ",",
                "d",
                "!~",
                r#""zzz""#,
                "}",
            ],
        );
        // Metric name with offset.
        test_lexer_success("   metric offset 10d   ", &["metric", "offset", "10d"]);
        // Func call.
        test_lexer_success(
            r#"sum  (  metric{x="y"  }  [5m] offset 10h)"#,
            &[
                "sum", "(", "metric", "{", "x", "=", r#""y""#, "}", "[", "5m", "]", "offset",
                "10h", ")",
            ],
        );
        // Binary op.
        test_lexer_success(
            "a+b or c % d and e unless f",
            &[
                "a", "+", "b", "or", "c", "%", "d", "and", "e", "unless", "f",
            ],
        );
        // Numbers.
        test_lexer_success(
            "3+1.2-.23+4.5e5-78e-6+1.24e+45-NaN+Inf",
            &[
                "3", "+", "1.2", "-", ".23", "+", "4.5e5", "-", "78e-6", "+", "1.24e+45", "-",
                "NaN", "+", "Inf",
            ],
        );
        test_lexer_success(
            "12.34 * 0X34 + 0b11 + 0O77",
            &["12.34", "*", "0X34", "+", "0b11", "+", "0O77"],
        );
        // Strings.
        test_lexer_success(
            "\"\"''``\"\\\\\"  '\\\\'  \"\\\"\" '\\''\"\\\\\\\"\\\\\"",
            &[
                r#""""#,
                "''",
                "``",
                r#""\\""#,
                r"'\\'",
                r#""\"""#,
                r"'\''",
                r#""\\\"\\""#,
            ],
        );
        // Various durations.
        test_lexer_success("m offset 123h", &["m", "offset", "123h"]);
        test_lexer_success(
            "m offset -1.23w-5h34.5m - 123",
            &["m", "offset", "-", "1.23w-5h34.5m", "-", "123"],
        );
        test_lexer_success("   `foo\\\\\\`бар`  ", &["`foo\\\\\\`бар`"]);
        test_lexer_success(
            "# comment # sdf\n\t\tfoobar # comment\n\t\tbaz\n\t\t# yet another comment",
            &["foobar", "baz"],
        );
    }

    fn test_lexer_error(s: &str) {
        let mut lex = Lexer::new(s);
        loop {
            if lex.next().is_err() {
                // Expected error.
                break;
            }
            assert!(!is_eof(&lex.token), "expecting error during parse of {s:?}");
        }
        // Try calling next() again. It must return an error.
        assert!(lex.next().is_err(), "expecting non-nil error");
    }

    // Port of TestLexerError.
    #[test]
    fn lexer_error() {
        // Invalid identifier.
        test_lexer_error(".foo");
        // Incomplete strings.
        test_lexer_error(r#""foobar"#);
        test_lexer_error("'");
        test_lexer_error("`");
        // Invalid numbers.
        test_lexer_error(".");
        test_lexer_error("12e");
        test_lexer_error("1.2e");
        test_lexer_error("1.2E+");
        test_lexer_error("1.2E-");
    }

    // Port of TestPositiveDurationSuccess.
    #[test]
    fn positive_duration_success() {
        let f = |s: &str, step: i64, d_expected: i64| {
            let d = positive_duration_value(s, step).unwrap();
            assert_eq!(d, d_expected, "unexpected duration for {s:?}");
        };
        // Integer durations.
        f("123ms", 42, 123);
        f("123s", 42, 123 * 1000);
        f("123m", 42, 123 * 60 * 1000);
        f("1h", 42, 3600 * 1000);
        f("2d", 42, 2 * 24 * 3600 * 1000);
        f("3w", 42, 3 * 7 * 24 * 3600 * 1000);
        f("4y", 42, 4 * 365 * 24 * 3600 * 1000);
        f("1i", 42 * 1000, 42 * 1000);
        f("3i", 42, 3 * 42);
        // Float durations.
        f("123.45ms", 42, 123);
        f("0.234s", 42, 234);
        f("1.5s", 42, 1500);
        f("1.5m", 42, 90 * 1000);
        f("1.2h", 42, (1.2 * 3600.0 * 1000.0) as i64);
        f("1.1d", 42, (1.1 * 24.0 * 3600.0 * 1000.0) as i64);
        f("1.1w", 42, (1.1 * 7.0 * 24.0 * 3600.0 * 1000.0) as i64);
        f("1.3y", 42, (1.3 * 365.0 * 24.0 * 3600.0 * 1000.0) as i64);
        f("0.1i", 12340, (0.1 * 12340.0) as i64);
        // Floating-point durations without suffix.
        f("123", 45, 123000);
        f("1.23", 45, 1230);
        f("0.56", 12, 560);
        f(".523e2", 21, 52300);
        // Duration suffixes in mixed case.
        f("1Ms", 45, 1);
        f("1mS", 45, 1);
        f("1H", 45, 3600 * 1000);
        f("1D", 45, 24 * 3600 * 1000);
        f("1Y", 45, 365 * 24 * 3600 * 1000);
        // Too big durations.
        f("10000000000y", 0, i64::MAX);
        f("922335359011637780i", 5 * 3600 * 1000, i64::MAX);
    }

    // Port of TestPositiveDurationError.
    #[test]
    fn positive_duration_error() {
        for s in [
            "",
            "foo",
            "m",
            "1.23mm",
            "123q",
            "-123s",
            "1.23.4434s",
            "1mi",
            "1mb",
            // Uppercase M isn't a duration, but a 1e6 multiplier.
            "1M",
        ] {
            assert!(
                positive_duration_value(s, 42).is_err(),
                "expecting error for duration {s:?}"
            );
        }
    }

    // Port of TestDurationSuccess.
    #[test]
    fn duration_success() {
        let f = |s: &str, step: i64, d_expected: i64| {
            let d = duration_value(s, step).unwrap();
            assert_eq!(d, d_expected, "unexpected duration for {s:?}");
        };
        // Integer durations.
        f("123ms", 42, 123);
        f("-123ms", 42, -123);
        f("4236579305ms", 42, 4236579305);
        f("123s", 42, 123 * 1000);
        f("-123s", 42, -123 * 1000);
        f("123m", 42, 123 * 60 * 1000);
        f("1h", 42, 3600 * 1000);
        f("2d", 42, 2 * 24 * 3600 * 1000);
        f("3w", 42, 3 * 7 * 24 * 3600 * 1000);
        f("4y", 42, 4 * 365 * 24 * 3600 * 1000);
        f("1i", 42 * 1000, 42 * 1000);
        f("3i", 42, 3 * 42);
        f("-3i", 42, -3 * 42);
        f("1m34s24ms", 42, 94024);
        f("1m-34s24ms", 42, 25976);
        f("-1m34s24ms", 42, -94024);
        f("-1m-34s24ms", 42, -94024);
        // Float durations.
        f("34.54ms", 42, 34);
        f("-34.34ms", 42, -34);
        f("0.234s", 42, 234);
        f("-0.234s", 42, -234);
        f("1.5s", 42, 1500);
        f("1.5m", 42, 90 * 1000);
        f("1.2h", 42, (1.2 * 3600.0 * 1000.0) as i64);
        f("1.1d", 42, (1.1 * 24.0 * 3600.0 * 1000.0) as i64);
        f("1.1w", 42, (1.1 * 7.0 * 24.0 * 3600.0 * 1000.0) as i64);
        f("1.3y", 42, (1.3 * 365.0 * 24.0 * 3600.0 * 1000.0) as i64);
        f(
            "-1.3y",
            42,
            -((1.3 * 365.0 * 24.0 * 3600.0 * 1000.0) as i64),
        );
        f("0.1i", 12340, (0.1 * 12340.0) as i64);
        f("1.5m3.4s2.4ms", 42, 93402);
        f("-1.5m3.4s2.4ms", 42, -93402);
        // Floating-point durations without suffix.
        f("123", 45, 123000);
        f("1.23", 45, 1230);
        f("-0.56", 12, -560);
        f("-.523e2", 21, -52300);
        // Duration suffixes in mixed case.
        f("-1Ms", 10, -1);
        f("-2.5mS", 10, -2);
        f("-1mS", 10, -1);
        f("-1H", 10, -3600 * 1000);
        f("-3.H", 10, -3 * 3600 * 1000);
        f("1D", 10, 24 * 3600 * 1000);
        f(
            "-.1Y",
            10,
            (-0.1f64 * 365.0 * 24.0 * 3600.0 * 1000.0) as i64,
        );
        // Too big durations.
        f("10000000000y", 0, i64::MAX);
        f("922335359011637780i", 5 * 3600 * 1000, i64::MAX);
        // Too small durations.
        f("-10000000000y", 0, i64::MIN);
        f("-922335359011637780i", 5 * 3600 * 1000, i64::MIN);
    }

    // Port of TestDurationError.
    #[test]
    fn duration_error() {
        for s in [
            "", "foo", "m", "1.23mm", "123q", "-123q", "-5.3mb", "-5.3mi",
            // M isn't a duration, but a 1e6 multiplier.
            "-5.3M",
        ] {
            assert!(
                duration_value(s, 42).is_err(),
                "expecting error for duration {s:?}"
            );
        }
    }
}
