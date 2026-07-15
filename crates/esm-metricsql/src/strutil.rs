//! Go-compatible string and number helpers.
//!
//! Ports the pieces of Go's `strconv` and `unicode` behavior that the
//! original parser (`parser.go`, `lexer.go`) relies on: `strconv.Quote`,
//! `strconv.Unquote`, `strconv.ParseFloat`, `strconv.ParseInt` (base 0) and
//! `strconv.AppendFloat(_, 'g', -1, 64)`.

use crate::ParseError;

/// Approximation of Go's `unicode.IsPrint`.
///
/// Treats letters, marks, numbers, punctuation, symbols and ASCII space as
/// printable. Control, whitespace (other than space), format and private-use
/// characters are not printable. Unassigned code points are treated as
/// printable, which is a slight deviation from Go.
pub(crate) fn is_print(c: char) -> bool {
    if c == ' ' {
        return true;
    }
    if c.is_control() || c.is_whitespace() || is_format_char(c) {
        return false;
    }
    // Private use areas are not printable in Go's unicode.IsPrint.
    !matches!(u32::from(c), 0xE000..=0xF8FF | 0xF0000..=0x10FFFD)
}

/// Unicode `Cf` (format) code points.
fn is_format_char(c: char) -> bool {
    matches!(
        u32::from(c),
        0xAD | 0x600..=0x605
            | 0x61C
            | 0x6DD
            | 0x70F
            | 0x890..=0x891
            | 0x8E2
            | 0x180E
            | 0x200B..=0x200F
            | 0x202A..=0x202E
            | 0x2060..=0x2064
            | 0x2066..=0x206F
            | 0xFEFF
            | 0xFFF9..=0xFFFB
            | 0x110BD
            | 0x110CD
            | 0x13430..=0x1343F
            | 0x1BCA0..=0x1BCA3
            | 0x1D173..=0x1D17A
            | 0xE0001
            | 0xE0020..=0xE007F
    )
}

/// Port of Go's `strconv.AppendQuote`: appends `s` quoted with double quotes.
pub(crate) fn append_quoted_string(dst: &mut String, s: &str) {
    use std::fmt::Write;
    dst.push('"');
    for c in s.chars() {
        match c {
            '"' | '\\' => {
                dst.push('\\');
                dst.push(c);
            }
            c if is_print(c) => dst.push(c),
            '\x07' => dst.push_str("\\a"),
            '\x08' => dst.push_str("\\b"),
            '\x0c' => dst.push_str("\\f"),
            '\n' => dst.push_str("\\n"),
            '\r' => dst.push_str("\\r"),
            '\t' => dst.push_str("\\t"),
            '\x0b' => dst.push_str("\\v"),
            c => {
                let v = u32::from(c);
                if v < 0x80 {
                    let _ = write!(dst, "\\x{v:02x}");
                } else if v < 0x10000 {
                    let _ = write!(dst, "\\u{v:04x}");
                } else {
                    let _ = write!(dst, "\\U{v:08x}");
                }
            }
        }
    }
    dst.push('"');
}

/// Returns `s` quoted with double quotes. See [`append_quoted_string`].
pub(crate) fn quote_string(s: &str) -> String {
    let mut dst = String::with_capacity(s.len() + 2);
    append_quoted_string(&mut dst, s);
    dst
}

/// Port of Go's `strconv.Unquote` for double-quoted and backquoted strings.
///
/// The token must include the surrounding quotes. Deviation from Go: `\xHH`
/// and octal escapes with values >= 0x80 decode to the corresponding Unicode
/// code point instead of a raw byte (Rust strings must stay valid UTF-8).
pub(crate) fn go_unquote(token: &str) -> Result<String, ParseError> {
    let err = || {
        ParseError::new(format!(
            "cannot parse string literal {token:?}: invalid syntax"
        ))
    };
    if token.len() < 2 {
        return Err(err());
    }
    let quote = token.as_bytes()[0];
    if token.as_bytes()[token.len() - 1] != quote {
        return Err(err());
    }
    let inner = &token[1..token.len() - 1];
    match quote {
        b'`' => {
            if inner.contains('`') {
                return Err(err());
            }
            // Go discards carriage returns inside raw strings.
            Ok(inner.replace('\r', ""))
        }
        b'"' => unquote_double(inner).map_err(|_| err()),
        _ => Err(err()),
    }
}

/// Unquotes the contents of a Go double-quoted string (without the quotes).
fn unquote_double(s: &str) -> Result<String, ()> {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\n' => return Err(()),
            '"' => return Err(()),
            '\\' => {
                let e = chars.next().ok_or(())?;
                match e {
                    'a' => out.push('\x07'),
                    'b' => out.push('\x08'),
                    'f' => out.push('\x0c'),
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    't' => out.push('\t'),
                    'v' => out.push('\x0b'),
                    '\\' => out.push('\\'),
                    '"' => out.push('"'),
                    'x' => out.push(read_hex_escape(&mut chars, 2)?),
                    'u' => out.push(read_hex_escape(&mut chars, 4)?),
                    'U' => out.push(read_hex_escape(&mut chars, 8)?),
                    '0'..='7' => {
                        let mut v = e as u32 - '0' as u32;
                        for _ in 0..2 {
                            let d = chars.next().ok_or(())?;
                            if !('0'..='7').contains(&d) {
                                return Err(());
                            }
                            v = v * 8 + (d as u32 - '0' as u32);
                        }
                        if v > 255 {
                            return Err(());
                        }
                        out.push(char::from_u32(v).ok_or(())?);
                    }
                    _ => return Err(()),
                }
            }
            c => out.push(c),
        }
    }
    Ok(out)
}

fn read_hex_escape(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    n: usize,
) -> Result<char, ()> {
    let mut v: u32 = 0;
    for _ in 0..n {
        let d = chars.next().ok_or(())?;
        let h = d.to_digit(16).ok_or(())?;
        v = v * 16 + h;
    }
    char::from_u32(v).ok_or(())
}

/// Port of Go's `strconv.AppendFloat(dst, v, 'g', -1, 64)`.
pub(crate) fn format_float_go(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    // Shortest round-trip digits via Rust's exponential formatting,
    // e.g. "2.565e2", "-1.23e9", "0e0".
    let s = format!("{v:e}");
    let epos = s.rfind('e').expect("exponent in {:e} output");
    let exp: i32 = s[epos + 1..].parse().expect("valid exponent");
    let mantissa = &s[..epos];
    let neg = mantissa.starts_with('-');
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let digits = digits.trim_end_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };

    let mut out = String::new();
    if neg {
        out.push('-');
    }
    // Same rule as Go's ftoa with shortest 'g' formatting: %e is used when
    // the decimal exponent is < -4 or >= 6 (e.g. Go prints 1e6 as "1e+06"
    // and -1.23e9 as "-1.23e+09", but 123456 stays "123456").
    if !(-4..6).contains(&exp) {
        // %e form: d.ddde±XX
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        if exp >= 0 {
            out.push('+');
        } else {
            out.push('-');
        }
        let ae = exp.unsigned_abs();
        if ae < 10 {
            out.push('0');
        }
        out.push_str(&ae.to_string());
    } else if exp >= 0 {
        // %f form with the decimal point inside or right after the digits.
        let ip = exp as usize + 1;
        if digits.len() > ip {
            out.push_str(&digits[..ip]);
            out.push('.');
            out.push_str(&digits[ip..]);
        } else {
            out.push_str(digits);
            for _ in 0..(ip - digits.len()) {
                out.push('0');
            }
        }
    } else {
        // %f form: 0.000ddd
        out.push_str("0.");
        for _ in 0..(-exp - 1) {
            out.push('0');
        }
        out.push_str(digits);
    }
    out
}

/// Port of Go's `strconv.ParseFloat(s, 64)` for decimal inputs
/// (hex floats are not supported; they never reach this code path).
///
/// Accepts underscores between digits, leading/trailing decimal dots and
/// case-insensitive `inf`/`infinity`/`nan` with an optional sign.
pub(crate) fn parse_go_float(s: &str) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    let (sign, rest) = match s.as_bytes()[0] {
        b'-' => (-1.0, &s[1..]),
        b'+' => (1.0, &s[1..]),
        _ => (1.0, s),
    };
    let low = rest.to_ascii_lowercase();
    if low == "inf" || low == "infinity" {
        return Some(sign * f64::INFINITY);
    }
    if low == "nan" {
        return Some(f64::NAN);
    }
    let cleaned = strip_underscores(rest)?;
    // Validate Go's decimal float grammar and normalize it for Rust's parser.
    let b = cleaned.as_bytes();
    let mut i = 0;
    let mut int_digits = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
        int_digits += 1;
    }
    let mut frac_digits = 0;
    if i < b.len() && b[i] == b'.' {
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
            frac_digits += 1;
        }
    }
    if int_digits + frac_digits == 0 {
        return None;
    }
    if i < b.len() {
        if b[i] != b'e' && b[i] != b'E' {
            return None;
        }
        i += 1;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        let mut exp_digits = 0;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
            exp_digits += 1;
        }
        if exp_digits == 0 || i != b.len() {
            return None;
        }
    }
    // Normalize forms like ".5", "12." and "12.e+4" for Rust's f64 parser.
    let mut norm = String::with_capacity(cleaned.len() + 2);
    if cleaned.starts_with('.') {
        norm.push('0');
    }
    for (idx, c) in cleaned.char_indices() {
        norm.push(c);
        if c == '.' {
            let next = cleaned.as_bytes().get(idx + 1);
            if !matches!(next, Some(d) if d.is_ascii_digit()) {
                norm.push('0');
            }
        }
    }
    norm.parse::<f64>().ok().map(|v| sign * v)
}

/// Removes underscores from a numeric literal, enforcing Go's rule that an
/// underscore must be surrounded by digits (hex digits are accepted so this
/// helper can also serve integer literals).
fn strip_underscores(s: &str) -> Option<String> {
    if !s.contains('_') {
        return Some(s.to_string());
    }
    let b = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    for (i, &c) in b.iter().enumerate() {
        if c == b'_' {
            let prev_ok = i > 0 && b[i - 1].is_ascii_hexdigit();
            let next_ok = i + 1 < b.len() && b[i + 1].is_ascii_hexdigit();
            if !prev_ok || !next_ok {
                return None;
            }
        } else {
            out.push(c as char);
        }
    }
    Some(out)
}

/// Port of Go's `strconv.ParseInt(s, 0, 64)`: base is auto-detected from the
/// `0x`/`0o`/`0b`/leading-zero prefix.
pub(crate) fn parse_go_int(s: &str) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let (neg, rest) = match s.as_bytes()[0] {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let cleaned = strip_underscores(rest)?;
    let lower = cleaned.to_ascii_lowercase();
    let (radix, digits) = if let Some(d) = lower.strip_prefix("0x") {
        (16, d)
    } else if let Some(d) = lower.strip_prefix("0o") {
        (8, d)
    } else if let Some(d) = lower.strip_prefix("0b") {
        (2, d)
    } else if lower.len() > 1 && lower.starts_with('0') {
        (8, &lower[1..])
    } else {
        (10, lower.as_str())
    };
    if digits.is_empty() {
        return None;
    }
    let mut v: i64 = 0;
    for c in digits.chars() {
        let d = c.to_digit(radix)? as i64;
        v = v.checked_mul(radix as i64)?.checked_add(d)?;
    }
    Some(if neg { -v } else { v })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_float_matches_go() {
        assert_eq!(format_float_go(f64::NAN), "NaN");
        assert_eq!(format_float_go(f64::INFINITY), "+Inf");
        assert_eq!(format_float_go(f64::NEG_INFINITY), "-Inf");
        assert_eq!(format_float_go(0.0), "0");
        assert_eq!(format_float_go(770.0), "770");
        assert_eq!(format_float_go(256.5), "256.5");
        assert_eq!(format_float_go(-4.0), "-4");
        assert_eq!(format_float_go(-0.2), "-0.2");
        assert_eq!(format_float_go(-0.002), "-0.002");
        assert_eq!(format_float_go(-59.0), "-59");
        assert_eq!(format_float_go(-1.23e9), "-1.23e+09");
        assert_eq!(format_float_go(1e21), "1e+21");
        assert_eq!(format_float_go(0.00002), "2e-05");
        assert_eq!(format_float_go(0.0002), "0.0002");
        assert_eq!(format_float_go(1234567.0), "1.234567e+06");
        assert_eq!(format_float_go(123456.0), "123456");
        assert_eq!(format_float_go(3.5), "3.5");
        assert_eq!(format_float_go(19.0), "19");
    }

    #[test]
    fn parse_go_float_grammar() {
        assert_eq!(parse_go_float("123"), Some(123.0));
        assert_eq!(parse_go_float("234."), Some(234.0));
        assert_eq!(parse_go_float(".2"), Some(0.2));
        assert_eq!(parse_go_float("-.523e2"), Some(-52.3));
        assert_eq!(parse_go_float("12.e+4"), Some(12e4));
        assert_eq!(parse_go_float("1_2.3_34_5e8"), Some(12.3345e8));
        assert_eq!(parse_go_float("inf"), Some(f64::INFINITY));
        assert!(parse_go_float("NaN").unwrap().is_nan());
        assert_eq!(parse_go_float(""), None);
        assert_eq!(parse_go_float("."), None);
        assert_eq!(parse_go_float("12e"), None);
        assert_eq!(parse_go_float("12.34e-"), None);
        assert_eq!(parse_go_float("234.foobar"), None);
        assert_eq!(parse_go_float("_1"), None);
    }

    #[test]
    fn parse_go_int_bases() {
        assert_eq!(parse_go_int("0xfe"), Some(0xfe));
        assert_eq!(parse_go_int("0b0110"), Some(6));
        assert_eq!(parse_go_int("0O765"), Some(0o765));
        assert_eq!(parse_go_int("0765"), Some(0o765));
        assert_eq!(parse_go_int("123"), Some(123));
        assert_eq!(parse_go_int("0xqwert"), None);
        assert_eq!(parse_go_int(""), None);
    }

    #[test]
    fn quote_unquote_roundtrip() {
        let mut s = String::new();
        append_quoted_string(&mut s, "foo'bar\"BAZ");
        assert_eq!(s, "\"foo'bar\\\"BAZ\"");
        assert_eq!(go_unquote(&s).unwrap(), "foo'bar\"BAZ");
        assert_eq!(go_unquote("`foo\"b'ar`").unwrap(), "foo\"b'ar");
        assert!(go_unquote("\"foo").is_err());
        assert_eq!(go_unquote("\"\\n\\t\\r\"").unwrap(), "\n\t\r");
    }
}
