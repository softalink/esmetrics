//! Influx line-protocol parser.
//!
//! Port of the upstream VictoriaMetrics v1.146.0 `lib/protoparser/influx/parser.go`.
//!
//! The parser is allocation-frugal: parsed rows hold `Cow::Borrowed` slices
//! into the input buffer on the hot path. Unescaping allocates only when a
//! `\` is actually present in the input (like the Go code, which appends to
//! a shared byte buffer only on the slow path).

use std::borrow::Cow;
use std::fmt;

/// Parse error returned by [`Rows::unmarshal`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError(String);

impl ParseError {
    fn new(msg: impl Into<String>) -> Self {
        ParseError(msg.into())
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ParseError {}

type Result<T> = std::result::Result<T, ParseError>;

/// Influx tag.
///
/// Values are `Cow::Borrowed` slices of the input buffer unless unescaping
/// was required.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Tag<'a> {
    pub key: Cow<'a, str>,
    pub value: Cow<'a, str>,
}

/// Influx field.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Field<'a> {
    pub key: Cow<'a, str>,
    pub value: f64,
}

/// A single influx row.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row<'a> {
    pub measurement: Cow<'a, str>,
    pub tags: Vec<Tag<'a>>,
    pub fields: Vec<Field<'a>>,
    pub timestamp: i64,
}

impl Row<'_> {
    fn reset(&mut self) {
        self.measurement = Cow::Borrowed("");
        self.tags.clear();
        self.fields.clear();
        self.timestamp = 0;
    }
}

/// Parsed influx rows.
///
/// Reusable across [`Rows::unmarshal`] calls on inputs sharing the lifetime
/// `'a`: row structs and their tag/field vectors keep their capacity across
/// calls, mirroring the Go `tagsPool`/`fieldsPool` reuse.
#[derive(Debug, Default)]
pub struct Rows<'a> {
    // Rows beyond `len` are retained for reuse (Go: `rs.Rows = rs.Rows[:n+1]`).
    rows: Vec<Row<'a>>,
    len: usize,
    ctx: UnmarshalContext,
}

#[derive(Debug, Default)]
struct UnmarshalContext {
    has_escape_chars: bool,
    has_quoted_fields: bool,
}

impl UnmarshalContext {
    fn reset(&mut self) {
        self.has_escape_chars = false;
        self.has_quoted_fields = false;
    }
}

impl<'a> Rows<'a> {
    /// Returns the parsed rows.
    pub fn rows(&self) -> &[Row<'a>] {
        &self.rows[..self.len]
    }

    /// Returns the parsed rows for in-place mutation (e.g. timestamp fixup).
    pub fn rows_mut(&mut self) -> &mut [Row<'a>] {
        &mut self.rows[..self.len]
    }

    /// Resets `self`.
    pub fn reset(&mut self) {
        self.len = 0;
        self.ctx.reset();
    }

    /// Unmarshals influx line protocol rows from `s`.
    ///
    /// See <https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/>
    ///
    /// If `skip_invalid_lines` is true, then all the invalid lines in `s` are
    /// ignored, the remaining lines are parsed and `Ok(())` is always
    /// returned. If false, then the first parse error is returned.
    pub fn unmarshal(&mut self, s: &'a str, skip_invalid_lines: bool) -> Result<()> {
        self.reset();
        self.unmarshal_inner(s, skip_invalid_lines)
    }

    fn unmarshal_inner(&mut self, mut s: &'a str, skip_invalid_lines: bool) -> Result<()> {
        self.ctx.has_escape_chars = s.as_bytes().contains(&b'\\');
        while !s.is_empty() {
            let n = match s.as_bytes().iter().position(|&b| b == b'\n') {
                Some(n) => n,
                // The last line.
                None => s.len(),
            };
            if let Err(err) = self.unmarshal_row(&s[..n]) {
                if !skip_invalid_lines {
                    return Err(ParseError(format!("incorrect influx line {s:?}: {err}")));
                }
                // The Go code logs the skipped line and increments the
                // `esm_rows_invalid_total{type="influx"}` counter here.
                // TODO: wire in logging/metrics once shared infrastructure exists.
            }
            if s.len() == n {
                return Ok(());
            }
            s = &s[n + 1..];
        }
        Ok(())
    }

    fn unmarshal_row(&mut self, s: &'a str) -> Result<()> {
        let s = s.strip_suffix('\r').unwrap_or(s);
        if s.is_empty() {
            // Skip empty line
            return Ok(());
        }
        if s.starts_with('#') {
            // Skip comment
            return Ok(());
        }

        if self.len < self.rows.len() {
            self.rows[self.len].reset();
        } else {
            self.rows.push(Row::default());
        }
        self.len += 1;
        let r = &mut self.rows[self.len - 1];
        if let Err(err) = unmarshal_single_row(r, s, &mut self.ctx) {
            self.len -= 1;
            return Err(err);
        }
        Ok(())
    }
}

fn unmarshal_single_row<'a>(r: &mut Row<'a>, s: &'a str, uc: &mut UnmarshalContext) -> Result<()> {
    let n = next_unescaped_char(s, b' ', uc.has_escape_chars)
        .ok_or_else(|| ParseError(format!("cannot find Whitespace I in {s:?}")))?;
    let mut measurement_tags = &s[..n];
    let mut s = strip_leading_whitespace(&s[n + 1..]);

    // Parse measurement and tags
    if let Some(n) = next_unescaped_char(measurement_tags, b',', uc.has_escape_chars) {
        unmarshal_tags(&mut r.tags, &measurement_tags[n + 1..], uc)?;
        measurement_tags = &measurement_tags[..n];
    }
    r.measurement = unescape_tag_value(measurement_tags, uc.has_escape_chars);
    // Allow empty r.measurement. In this case metric name is constructed directly from field keys.

    // Parse fields
    uc.has_quoted_fields = next_unescaped_char(s, b'"', uc.has_escape_chars).is_some();
    let n = match next_unquoted_char(s, b' ', uc) {
        None => {
            // No timestamp.
            return unmarshal_influx_fields(&mut r.fields, s, uc);
        }
        Some(n) => n,
    };
    if let Err(err) = unmarshal_influx_fields(&mut r.fields, &s[..n], uc) {
        if s[n + 1..].starts_with("HTTP/") {
            return Err(ParseError::new(TCP_HINT));
        }
        return Err(err);
    }
    s = strip_leading_whitespace(&s[n + 1..]);

    // The timestamp is optional in the InfluxDB line protocol.
    // Whitespace before it may still be present even when the timestamp itself is omitted.
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10049
    if !s.is_empty() {
        match parse_int64(s) {
            Ok(timestamp) => r.timestamp = timestamp,
            Err(err) => {
                if s.starts_with("HTTP/") {
                    return Err(ParseError::new(TCP_HINT));
                }
                return Err(ParseError(format!("cannot parse timestamp {s:?}: {err}")));
            }
        }
    }
    Ok(())
}

const TCP_HINT: &str = "please switch from tcp to http protocol for data ingestion; \
do not set `-influxListenAddr` command-line flag, since it is needed for tcp protocol only";

fn unmarshal_tag<'a>(s: &'a str, uc: &UnmarshalContext) -> Result<Tag<'a>> {
    let n = next_unescaped_char(s, b'=', uc.has_escape_chars)
        .ok_or_else(|| ParseError(format!("missing tag value for {s:?}")))?;
    Ok(Tag {
        key: unescape_tag_value(&s[..n], uc.has_escape_chars),
        value: unescape_tag_value(&s[n + 1..], uc.has_escape_chars),
    })
}

fn unmarshal_field<'a>(s: &'a str, uc: &UnmarshalContext) -> Result<Field<'a>> {
    let n = next_unescaped_char(s, b'=', uc.has_escape_chars)
        .ok_or_else(|| ParseError(format!("missing field value for {s:?}")))?;
    let key = unescape_tag_value(&s[..n], uc.has_escape_chars);
    if key.is_empty() {
        return Err(ParseError::new("field key cannot be empty"));
    }
    let value = parse_field_value(&s[n + 1..], uc)
        .map_err(|err| ParseError(format!("cannot parse field value for {key:?}: {err}")))?;
    Ok(Field { key, value })
}

fn unmarshal_tags<'a>(
    tags: &mut Vec<Tag<'a>>,
    mut s: &'a str,
    uc: &UnmarshalContext,
) -> Result<()> {
    loop {
        match next_unescaped_char(s, b',', uc.has_escape_chars) {
            None => {
                let tag = unmarshal_tag(s, uc)?;
                if !tag.key.is_empty() && !tag.value.is_empty() {
                    // Skip empty tag
                    tags.push(tag);
                }
                return Ok(());
            }
            Some(n) => {
                let tag = unmarshal_tag(&s[..n], uc)?;
                s = &s[n + 1..];
                if !tag.key.is_empty() && !tag.value.is_empty() {
                    // Skip empty tag
                    tags.push(tag);
                }
            }
        }
    }
}

fn unmarshal_influx_fields<'a>(
    fields: &mut Vec<Field<'a>>,
    mut s: &'a str,
    uc: &UnmarshalContext,
) -> Result<()> {
    loop {
        match next_unquoted_char(s, b',', uc) {
            None => {
                fields.push(unmarshal_field(s, uc)?);
                return Ok(());
            }
            Some(n) => {
                fields.push(unmarshal_field(&s[..n], uc)?);
                s = &s[n + 1..];
            }
        }
    }
}

fn unescape_tag_value<'a>(s: &'a str, has_escape_chars: bool) -> Cow<'a, str> {
    if !has_escape_chars {
        // Fast path - no escape chars.
        return Cow::Borrowed(s);
    }
    let mut b = s.as_bytes();
    let mut n = match b.iter().position(|&c| c == b'\\') {
        Some(n) => n,
        None => return Cow::Borrowed(s),
    };

    // Slow path. Remove escape chars.
    let mut buf: Vec<u8> = Vec::with_capacity(b.len());
    loop {
        buf.extend_from_slice(&b[..n]);
        b = &b[n + 1..];
        if b.is_empty() {
            buf.push(b'\\');
            break;
        }
        let ch = b[0];
        if ch != b' ' && ch != b',' && ch != b'=' && ch != b'\\' {
            buf.push(b'\\');
        }
        buf.push(ch);
        b = &b[1..];
        n = match b.iter().position(|&c| c == b'\\') {
            Some(n) => n,
            None => {
                buf.extend_from_slice(b);
                break;
            }
        };
    }
    // Only ASCII `\` bytes were removed from valid UTF-8, so the result is valid UTF-8.
    Cow::Owned(String::from_utf8(buf).expect("BUG: unescaped tag value must be valid UTF-8"))
}

fn parse_field_value(s: &str, uc: &UnmarshalContext) -> Result<f64> {
    if s.is_empty() {
        return Err(ParseError::new("field value cannot be empty"));
    }
    let b = s.as_bytes();
    if uc.has_quoted_fields && b[0] == b'"' {
        if b.len() < 2 || b[b.len() - 1] != b'"' {
            // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10067
            return Err(ParseError(format!(
                "missing closing quote for quoted field value {s}; \
this may be caused by a raw newline (`\\n`) inside the quoted field value"
            )));
        }
        // Try converting quoted string to number, since sometimes InfluxDB agents
        // send numbers as strings.
        return Ok(parse_best_effort(&s[1..s.len() - 1]));
    }
    let ch = b[b.len() - 1];
    if ch == b'i' {
        // Integer value
        let n = parse_int64(&s[..s.len() - 1])?;
        return Ok(n as f64);
    }
    if ch == b'u' {
        // Unsigned integer value
        let n = parse_uint64(&s[..s.len() - 1])?;
        return Ok(n as f64);
    }
    if matches!(s, "t" | "T" | "true" | "True" | "TRUE") {
        return Ok(1.0);
    }
    if matches!(s, "f" | "F" | "false" | "False" | "FALSE") {
        return Ok(0.0);
    }
    Ok(parse_best_effort(s))
}

fn next_unescaped_char(s: &str, ch: u8, has_escape_chars: bool) -> Option<usize> {
    let bytes = s.as_bytes();
    if !has_escape_chars {
        // Fast path: just search for ch in s, since s has no escape chars.
        return bytes.iter().position(|&b| b == ch);
    }

    let mut start = 0usize;
    loop {
        let w = &bytes[start..];
        let n = w.iter().position(|&b| b == ch)?;
        if n == 0 || w[n - 1] != b'\\' {
            return Some(start + n);
        }
        let mut slashes = 0usize;
        let mut i = n;
        while i > 0 && w[i - 1] == b'\\' {
            slashes += 1;
            i -= 1;
        }
        if slashes & 1 == 0 {
            return Some(start + n);
        }
        start += n + 1;
    }
}

fn next_unquoted_char(s: &str, ch: u8, uc: &UnmarshalContext) -> Option<usize> {
    if !uc.has_quoted_fields {
        return next_unescaped_char(s, ch, uc.has_escape_chars);
    }
    let mut start = 0usize;
    loop {
        let w = &s[start..];
        let n = next_unescaped_char(w, ch, uc.has_escape_chars)?;
        if !is_in_quote(&w[..n], uc.has_escape_chars) {
            return Some(start + n);
        }
        let w = &w[n + 1..];
        let m = next_unescaped_char(w, b'"', uc.has_escape_chars)?;
        start += n + 1 + m + 1;
    }
}

fn is_in_quote(mut s: &str, has_escape_chars: bool) -> bool {
    let mut is_quote = false;
    loop {
        let n = match next_unescaped_char(s, b'"', has_escape_chars) {
            Some(n) => n,
            None => return is_quote,
        };
        is_quote = !is_quote;
        s = &s[n + 1..];
    }
}

fn strip_leading_whitespace(mut s: &str) -> &str {
    while let Some(rest) = s.strip_prefix(' ') {
        s = rest;
    }
    s
}

// ---------------------------------------------------------------------------
// Fast number parsing, ported from github.com/valyala/fastjson/fastfloat.
// Fast path handles integers and simple decimals; falls back to
// `str::parse` for long mantissas / big exponents.
// ---------------------------------------------------------------------------

/// Parses an int64 from `s`. Port of `fastfloat.ParseInt64`.
pub(crate) fn parse_int64(s: &str) -> Result<i64> {
    let b = s.as_bytes();
    if b.is_empty() {
        return Err(ParseError::new("cannot parse int64 from empty string"));
    }
    let mut i = 0usize;
    let minus = b[0] == b'-';
    if minus {
        i += 1;
        if i >= b.len() {
            return Err(ParseError(format!("cannot parse int64 from {s:?}")));
        }
    }

    let mut d: i64 = 0;
    let j = i;
    while i < b.len() && b[i].is_ascii_digit() {
        d = d * 10 + i64::from(b[i] - b'0');
        i += 1;
        if i > 18 {
            // The integer part may be out of range for int64.
            // Fall back to slow parsing.
            return s
                .parse::<i64>()
                .map_err(|err| ParseError(format!("cannot parse int64 from {s:?}: {err}")));
        }
    }
    if i <= j {
        return Err(ParseError(format!("cannot parse int64 from {s:?}")));
    }
    if i < b.len() {
        // Unparsed tail left.
        return Err(ParseError(format!(
            "unparsed tail left after parsing int64 from {s:?}: {:?}",
            &s[i..]
        )));
    }
    Ok(if minus { -d } else { d })
}

/// Parses a uint64 from `s`. Port of `fastfloat.ParseUint64`.
pub(crate) fn parse_uint64(s: &str) -> Result<u64> {
    let b = s.as_bytes();
    if b.is_empty() {
        return Err(ParseError::new("cannot parse uint64 from empty string"));
    }
    let mut i = 0usize;
    let mut d: u64 = 0;
    let j = i;
    while i < b.len() && b[i].is_ascii_digit() {
        d = d * 10 + u64::from(b[i] - b'0');
        i += 1;
        if i > 18 {
            // The integer part may be out of range for uint64.
            // Fall back to slow parsing.
            return s
                .parse::<u64>()
                .map_err(|err| ParseError(format!("cannot parse uint64 from {s:?}: {err}")));
        }
    }
    if i <= j {
        return Err(ParseError(format!("cannot parse uint64 from {s:?}")));
    }
    if i < b.len() {
        // Unparsed tail left.
        return Err(ParseError(format!(
            "unparsed tail left after parsing uint64 from {s:?}: {:?}",
            &s[i..]
        )));
    }
    Ok(d)
}

// Exact powers of 10 for fast mantissa scaling.
const F64_POW10: [f64; 17] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
];

/// Parses a floating-point number from `s`, returning 0 if it cannot be
/// parsed. Port of `fastfloat.ParseBestEffort`.
pub(crate) fn parse_best_effort(s: &str) -> f64 {
    let b = s.as_bytes();
    if b.is_empty() {
        return 0.0;
    }
    let mut i = 0usize;
    let minus = b[0] == b'-';
    if minus {
        i += 1;
        if i >= b.len() {
            return 0.0;
        }
    }

    // The integer part might be elided.
    if b[i] == b'.' && (i + 1 >= b.len() || !b[i + 1].is_ascii_digit()) {
        return 0.0;
    }

    let mut d: u64 = 0;
    let j = i;
    while i < b.len() && b[i].is_ascii_digit() {
        d = d * 10 + u64::from(b[i] - b'0');
        i += 1;
        if i > 18 {
            // The integer part may be out of range for u64.
            // Fall back to slow parsing.
            return s.parse::<f64>().unwrap_or(0.0);
        }
    }
    if i <= j && b[i] != b'.' {
        let mut rest = &s[i..];
        if let Some(stripped) = rest.strip_prefix('+') {
            rest = stripped;
        }
        // "infinity" is needed for OpenMetrics support.
        if rest.eq_ignore_ascii_case("inf") || rest.eq_ignore_ascii_case("infinity") {
            return if minus {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            };
        }
        if rest.eq_ignore_ascii_case("nan") {
            return f64::NAN;
        }
        return 0.0;
    }
    let mut f = d as f64;
    if i >= b.len() {
        // Fast path - just integer.
        return if minus { -f } else { f };
    }

    if b[i] == b'.' {
        // Parse fractional part.
        i += 1;
        if i >= b.len() {
            // The fractional part may be elided.
            // NOTE: the Go original does not apply the minus sign here;
            // mirrored for bug-compatibility.
            return f;
        }
        let k = i;
        while i < b.len() && b[i].is_ascii_digit() {
            d = d * 10 + u64::from(b[i] - b'0');
            i += 1;
            if i - j >= F64_POW10.len() {
                // The mantissa is out of range. Fall back to standard parsing.
                return s.parse::<f64>().unwrap_or(0.0);
            }
        }
        if i < k {
            return 0.0;
        }
        // Convert the entire mantissa to a float at once to avoid rounding errors.
        f = d as f64 / F64_POW10[i - k];
        if i >= b.len() {
            // Fast path - parsed fractional number.
            return if minus { -f } else { f };
        }
    }
    if b[i] == b'e' || b[i] == b'E' {
        // Parse exponent part.
        i += 1;
        if i >= b.len() {
            return 0.0;
        }
        let mut exp_minus = false;
        if b[i] == b'+' || b[i] == b'-' {
            exp_minus = b[i] == b'-';
            i += 1;
            if i >= b.len() {
                return 0.0;
            }
        }
        let mut exp: i32 = 0;
        let jj = i;
        while i < b.len() && b[i].is_ascii_digit() {
            exp = exp * 10 + i32::from(b[i] - b'0');
            i += 1;
            if exp > 300 {
                // The exponent may be too big for f64.
                // Fall back to standard parsing.
                return s.parse::<f64>().unwrap_or(0.0);
            }
        }
        if i <= jj {
            return 0.0;
        }
        if exp_minus {
            exp = -exp;
        }
        f *= 10f64.powi(exp);
        if i >= b.len() {
            return if minus { -f } else { f };
        }
    }
    0.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uc(has_escape_chars: bool, has_quoted_fields: bool) -> UnmarshalContext {
        UnmarshalContext {
            has_escape_chars,
            has_quoted_fields,
        }
    }

    #[test]
    fn test_next_unquoted_char() {
        fn f(s: &str, ch: u8, has_escape_chars: bool, n_expected: Option<usize>) {
            let uc = uc(has_escape_chars, true);
            let n = next_unquoted_char(s, ch, &uc);
            assert_eq!(
                n, n_expected,
                "unexpected n for next_unquoted_char({s:?}, {:?}, {has_escape_chars})",
                ch as char
            );
        }

        f("", b' ', true, None);
        f("", b' ', false, None);
        f(r#""""#, b' ', true, None);
        f(r#""""#, b' ', false, None);
        f(r#""foo bar\" " baz"#, b' ', true, Some(12));
        f(r#""foo bar\" " baz"#, b' ', false, Some(10));
    }

    #[test]
    fn test_next_unescaped_char() {
        fn f(s: &str, ch: u8, has_escape_chars: bool, n_expected: Option<usize>) {
            let n = next_unescaped_char(s, ch, has_escape_chars);
            assert_eq!(
                n, n_expected,
                "unexpected n for next_unescaped_char({s:?}, {:?}, {has_escape_chars})",
                ch as char
            );
        }

        f("", b' ', false, None);
        f("", b' ', true, None);
        f(" ", b' ', false, Some(0));
        f(" ", b' ', true, Some(0));
        f("x y", b' ', false, Some(1));
        f("x y", b' ', true, Some(1));
        f(r"x\  y", b' ', false, Some(2));
        f(r"x\  y", b' ', true, Some(3));
        f(r"\\,", b',', false, Some(2));
        f(r"\\,", b',', true, Some(2));
        f(r"\\\=", b'=', false, Some(3));
        f(r"\\\=", b'=', true, None);
        f(r"\\\=aa", b'=', false, Some(3));
        f(r"\\\=aa", b'=', true, None);
        f(r"\\\=a=a", b'=', false, Some(3));
        f(r"\\\=a=a", b'=', true, Some(5));
        f(r"a\", b' ', false, None);
        f(r"a\", b' ', true, None);
    }

    #[test]
    fn test_unescape_tag_value() {
        fn f(s: &str, expected: &str) {
            let ss = unescape_tag_value(s, true);
            assert_eq!(ss.as_ref(), expected, "unexpected value for {s:?}");
        }

        f("", "");
        f("x", "x");
        f("foobar", "foobar");
        f("привет", "привет");
        f(r"\a\b\cd", r"\a\b\cd");
        f(r"\", r"\");
        f(r"foo\", r"foo\");
        f(r"\,foo\\\=\ bar", r",foo\= bar");
    }

    #[test]
    fn test_rows_unmarshal_failure() {
        fn f(s: &str) {
            let mut rows = Rows::default();
            assert!(
                rows.unmarshal(s, false).is_err(),
                "expecting non-nil error for {s:?}"
            );
            assert!(
                rows.rows().is_empty(),
                "expecting zero rows for {s:?}; got {:?}",
                rows.rows()
            );

            // Try again
            assert!(
                rows.unmarshal(s, false).is_err(),
                "expecting non-nil error on the second attempt for {s:?}"
            );
            assert!(rows.rows().is_empty());
        }

        // No fields
        f("foo");
        f("foo,bar=baz 1234");

        // Missing tag value
        f("foo,bar");
        f("foo,bar baz");
        f("foo,bar=123, 123");

        // Missing field value
        f("foo bar");
        f("foo bar=");
        f("foo bar=,baz=23 123");
        f("foo bar=1, 123");
        f(r#"foo bar=" 123"#);
        f(r#"foo bar="123"#);
        f(r#"foo bar=",123"#);
        f(r#"foo bar=a"", 123"#);

        // Missing field name
        f("foo =123");
        f("foo =123\nbar");

        // Invalid timestamp
        f("foo bar=123 baz");

        // Invalid field value
        f("foo bar=1abci");
        f("foo bar=-2abci");
        f("foo bar=3abcu");

        // HTTP request line
        f("GET /foo HTTP/1.1");
        f("GET /foo?bar=baz HTTP/1.0");
    }

    #[test]
    fn test_parse_field_value_missing_closing_quote_with_raw_newline_hint() {
        let uc = uc(false, true);

        // Simulate the truncated value that happens
        // after line splitting on raw newline
        let input = "\"hello";

        let err = parse_field_value(input, &uc).expect_err("expected error for missing quote");
        assert!(
            err.to_string()
                .contains("this may be caused by a raw newline"),
            "unexpected error message: {err}"
        );
    }

    fn tag(key: &'static str, value: &'static str) -> Tag<'static> {
        Tag {
            key: Cow::Borrowed(key),
            value: Cow::Borrowed(value),
        }
    }

    fn field(key: &'static str, value: f64) -> Field<'static> {
        Field {
            key: Cow::Borrowed(key),
            value,
        }
    }

    fn row(
        measurement: &'static str,
        tags: Vec<Tag<'static>>,
        fields: Vec<Field<'static>>,
        timestamp: i64,
    ) -> Row<'static> {
        Row {
            measurement: Cow::Borrowed(measurement),
            tags,
            fields,
            timestamp,
        }
    }

    #[test]
    fn test_rows_unmarshal_success() {
        fn f(s: &str, rows_expected: &[Row<'_>]) {
            let mut rows = Rows::default();
            rows.unmarshal(s, true).unwrap();
            assert_eq!(rows.rows(), rows_expected, "unexpected rows for {s:?}");

            // Try unmarshaling again
            rows.unmarshal(s, true).unwrap();
            assert_eq!(
                rows.rows(),
                rows_expected,
                "unexpected rows on the second unmarshal attempt for {s:?}"
            );

            rows.reset();
            assert!(
                rows.rows().is_empty(),
                "non-empty rows after reset: {:?}",
                rows.rows()
            );
        }

        // Empty line
        f("", &[]);
        f("\n\n", &[]);
        f("\n\r\n", &[]);

        // Comment
        f("\n# foobar\n", &[]);
        f("#foobar baz", &[]);
        f("#foobar baz\n#sss", &[]);

        // Missing measurement
        f(" baz=123", &[row("", vec![], vec![field("baz", 123.0)], 0)]);
        f(
            ",foo=bar baz=123",
            &[row(
                "",
                vec![tag("foo", "bar")],
                vec![field("baz", 123.0)],
                0,
            )],
        );

        // Minimal line without tags and timestamp
        f(
            "foo bar=123",
            &[row("foo", vec![], vec![field("bar", 123.0)], 0)],
        );
        // Excess whitespace after final field. Issue #10049
        f(
            "foo bar=123   ",
            &[row("foo", vec![], vec![field("bar", 123.0)], 0)],
        );
        f(
            "# comment\nfoo bar=123\r\n#comment2 sdsf dsf",
            &[row("foo", vec![], vec![field("bar", 123.0)], 0)],
        );
        f(
            "foo bar=123\n",
            &[row("foo", vec![], vec![field("bar", 123.0)], 0)],
        );

        // Line without tags and with a timestamp.
        f(
            "foo bar=123.45 -345",
            &[row("foo", vec![], vec![field("bar", 123.45)], -345)],
        );

        // Line with a single tag
        f(
            "foo,tag1=xyz bar=123",
            &[row(
                "foo",
                vec![tag("tag1", "xyz")],
                vec![field("bar", 123.0)],
                0,
            )],
        );

        // Line with multiple tags
        f(
            "foo,tag1=xyz,tag2=43as bar=123",
            &[row(
                "foo",
                vec![tag("tag1", "xyz"), tag("tag2", "43as")],
                vec![field("bar", 123.0)],
                0,
            )],
        );

        // Line with empty tag values
        f(
            "foo,tag1=xyz,tagN=,tag2=43as,=xxx bar=123",
            &[row(
                "foo",
                vec![tag("tag1", "xyz"), tag("tag2", "43as")],
                vec![field("bar", 123.0)],
                0,
            )],
        );

        // Line with multiple tags, multiple fields and timestamp
        f(
            r#"system,host=ip-172-16-10-144 uptime_format="3 days, 21:01",quoted_float="-1.23",quoted_int="123" 1557761040000000000"#,
            &[row(
                "system",
                vec![tag("host", "ip-172-16-10-144")],
                vec![
                    field("uptime_format", 0.0),
                    field("quoted_float", -1.23),
                    field("quoted_int", 123.0),
                ],
                1557761040000000000,
            )],
        );
        f(
            r#"foo,tag1=xyz,tag2=43as bar=-123e4,x=True,y=-45i,z=f,aa="f,= \"a",bb=23u 48934"#,
            &[row(
                "foo",
                vec![tag("tag1", "xyz"), tag("tag2", "43as")],
                vec![
                    field("bar", -123e4),
                    field("x", 1.0),
                    field("y", -45.0),
                    field("z", 0.0),
                    field("aa", 0.0),
                    field("bb", 23.0),
                ],
                48934,
            )],
        );

        // Escape chars
        f(
            r"fo\,bar\=b\ az,x\=\ b=\\a\,\=\q\  \\\a\ b\=\,=4.34",
            &[row(
                r"fo,bar=b az",
                vec![tag(r"x= b", r"\a,=\q ")],
                vec![field(r"\\a b=,", 4.34)],
                0,
            )],
        );
        // Test case from https://community.librenms.org/t/integration-with-victoriametrics/9689
        f(
            "ports,foo=a,bar=et\\ +\\ V,baz=ype INDISCARDS=245333676,OUTDISCARDS=1798680",
            &[row(
                "ports",
                vec![tag("foo", "a"), tag("bar", "et + V"), tag("baz", "ype")],
                vec![
                    field("INDISCARDS", 245333676.0),
                    field("OUTDISCARDS", 1798680.0),
                ],
                0,
            )],
        );

        // Multiple lines
        f(
            "foo,tag=xyz field=1.23 48934\nbar x=-1i\n\n",
            &[
                row(
                    "foo",
                    vec![tag("tag", "xyz")],
                    vec![field("field", 1.23)],
                    48934,
                ),
                row("bar", vec![], vec![field("x", -1.0)], 0),
            ],
        );

        // Multiple lines with invalid line in the middle.
        f(
            "foo,tag=xyz field=1.23 48934\ninvalid line\nbar x=-1i\n\n",
            &[
                row(
                    "foo",
                    vec![tag("tag", "xyz")],
                    vec![field("field", 1.23)],
                    48934,
                ),
                row("bar", vec![], vec![field("x", -1.0)], 0),
            ],
        );

        // No newline after the second line.
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/82
        f(
            "foo,tag=xyz field=1.23 48934\nbar x=-1i",
            &[
                row(
                    "foo",
                    vec![tag("tag", "xyz")],
                    vec![field("field", 1.23)],
                    48934,
                ),
                row("bar", vec![], vec![field("x", -1.0)], 0),
            ],
        );

        // Superfluous whitespace between tags, fields and timestamps.
        f(
            "cpu_utilization,host=mnsbook-pro.local value=119.8 1607222595591",
            &[row(
                "cpu_utilization",
                vec![tag("host", "mnsbook-pro.local")],
                vec![field("value", 119.8)],
                1607222595591,
            )],
        );
        f(
            "cpu_utilization,host=mnsbook-pro.local   value=119.8   1607222595591",
            &[row(
                "cpu_utilization",
                vec![tag("host", "mnsbook-pro.local")],
                vec![field("value", 119.8)],
                1607222595591,
            )],
        );

        f(
            "x,y=z,g=p:\\ \\ 5432\\,\\ gp\\ mon\\ [lol]\\ con10\\ cmd5\\ SELECT f=1",
            &[row(
                "x",
                vec![
                    tag("y", "z"),
                    tag("g", "p:  5432, gp mon [lol] con10 cmd5 SELECT"),
                ],
                vec![field("f", 1.0)],
                0,
            )],
        );
    }
}
