//! Prometheus exposition-text parser.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `lib/protoparser/prometheus/parser.go` (the `Rows.UnmarshalWithErrLogger`
//! decode path only; `UnmarshalWithMetadata`'s `MetadataRows` half is out of
//! scope).
//!
//! Like [`crate::influx`], parsed rows hold `&'a str` / `Cow<'a, str>` slices
//! into the input buffer on the hot path; unescaping a tag value allocates
//! only when the value actually contains a `\`.
//!
//! The streaming entry point ([`parse_stream`]) lives in the sibling
//! [`crate::prometheus_stream`] module (kept separate to stay under the
//! file-size guideline) and is re-exported here so callers use
//! `esm_protoparser::prometheus::parse_stream` like every other symbol in
//! this parser.

use std::borrow::Cow;
use std::fmt;

pub use crate::prometheus_stream::{parse_stream, CallbackResult, Error};

/// Parse error for a single line. Never surfaced publicly: [`Rows::unmarshal`]
/// only passes the formatted message to `err_logger` and skips the line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ParseError(String);

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

/// A Prometheus tag (label).
///
/// `key` is always a borrowed slice of the input: unlike tag values, keys are
/// never unescaped (see divergence note on [`unmarshal_tags`]).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Tag<'a> {
    pub key: &'a str,
    pub value: Cow<'a, str>,
}

/// A single Prometheus exposition-text row.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row<'a> {
    pub metric: &'a str,
    pub tags: Vec<Tag<'a>>,
    pub value: f64,
    pub timestamp: i64,
}

impl Row<'_> {
    fn reset(&mut self) {
        self.metric = "";
        self.tags.clear();
        self.value = 0.0;
        self.timestamp = 0;
    }
}

/// Parsed Prometheus rows.
///
/// Reusable across [`Rows::unmarshal`] calls on inputs sharing the lifetime
/// `'a`: row structs and their tag vectors keep their capacity across calls,
/// mirroring the Go `tagsPool` reuse.
#[derive(Debug, Default)]
pub struct Rows<'a> {
    // Rows beyond `len` are retained for reuse (Go: `rs.Rows = rs.Rows[:n+1]`).
    rows: Vec<Row<'a>>,
    len: usize,
}

impl<'a> Rows<'a> {
    /// Returns the parsed rows.
    pub fn rows(&self) -> &[Row<'a>] {
        &self.rows[..self.len]
    }

    /// Returns the parsed rows, mutably (used by [`crate::prometheus_stream`]
    /// to fill in default timestamps after unmarshaling).
    pub fn rows_mut(&mut self) -> &mut [Row<'a>] {
        &mut self.rows[..self.len]
    }

    /// Resets `self`.
    pub fn reset(&mut self) {
        self.len = 0;
    }

    /// Unmarshals Prometheus exposition-text rows from `s`.
    ///
    /// See <https://github.com/prometheus/docs/blob/master/docs/instrumenting/exposition_formats.md#line-format>
    ///
    /// Invalid lines are skipped; the formatted error is passed to
    /// `err_logger` for each one (Go: `UnmarshalWithErrLogger`'s `errLogger`).
    /// Comment lines (`#...`) and blank lines are skipped silently, with no
    /// callback.
    pub fn unmarshal(&mut self, s: &'a str, mut err_logger: impl FnMut(&str)) {
        self.reset();
        let has_escape_chars = s.as_bytes().contains(&b'\\');
        let mut rest = s;
        while !rest.is_empty() {
            match rest.as_bytes().iter().position(|&b| b == b'\n') {
                None => {
                    self.unmarshal_row(rest, has_escape_chars, &mut err_logger);
                    break;
                }
                Some(n) => {
                    self.unmarshal_row(&rest[..n], has_escape_chars, &mut err_logger);
                    rest = &rest[n + 1..];
                }
            }
        }
    }

    fn unmarshal_row(
        &mut self,
        s: &'a str,
        has_escape_chars: bool,
        err_logger: &mut impl FnMut(&str),
    ) {
        let s = s.strip_suffix('\r').unwrap_or(s);
        let s = skip_leading_whitespace(s);
        if s.is_empty() {
            // Skip empty line.
            return;
        }
        if s.starts_with('#') {
            // Skip comment.
            return;
        }

        if self.len < self.rows.len() {
            self.rows[self.len].reset();
        } else {
            self.rows.push(Row::default());
        }
        self.len += 1;
        let idx = self.len - 1;
        if let Err(err) = unmarshal_single_row(&mut self.rows[idx], s, has_escape_chars) {
            self.len -= 1;
            err_logger(&format!("cannot unmarshal Prometheus line {s:?}: {err}"));
        }
    }
}

fn unmarshal_single_row<'a>(r: &mut Row<'a>, s: &'a str, has_escape_chars: bool) -> Result<()> {
    let mut s = skip_leading_whitespace(s);
    if let Some(n) = s.find('{') {
        r.metric = skip_trailing_whitespace(&s[..n]);
        s = unmarshal_tags(r, &s[n + 1..], has_escape_chars)?;
    } else {
        let n = next_whitespace(s).ok_or_else(|| ParseError::new("missing value"))?;
        r.metric = &s[..n];
        s = &s[n + 1..];
    }
    if r.metric.is_empty() {
        return Err(ParseError::new("metric cannot be empty"));
    }
    let s = skip_leading_whitespace(s);
    let s = skip_trailing_comment(s);
    if s.is_empty() {
        return Err(ParseError::new("value cannot be empty"));
    }
    match next_whitespace(s) {
        None => {
            r.value = parse_prom_float(s)
                .map_err(|err| ParseError(format!("cannot parse value {s:?}: {err}")))?;
            Ok(())
        }
        Some(n) => {
            r.value = parse_prom_float(&s[..n])
                .map_err(|err| ParseError(format!("cannot parse value {:?}: {err}", &s[..n])))?;
            let rest = skip_leading_whitespace(&s[n + 1..]);
            if rest.is_empty() {
                // There is no timestamp - just whitespace after the value.
                return Ok(());
            }
            let rest = skip_trailing_whitespace(rest);
            let ts = parse_prom_float(rest)
                .map_err(|err| ParseError(format!("cannot parse timestamp {rest:?}: {err}")))?;
            // Timestamps within `[-2^31, 2^31)` look like OpenMetrics Unix
            // seconds; convert to milliseconds. Otherwise the value is
            // already milliseconds. See
            // https://github.com/OpenObservability/OpenMetrics/blob/master/specification/OpenMetrics.md#timestamps
            let ts = if (-2_147_483_648.0..2_147_483_648.0).contains(&ts) {
                ts * 1000.0
            } else {
                ts
            };
            r.timestamp = ts as i64;
            Ok(())
        }
    }
}

/// Parses the tag list starting just after the opening `{`, up to and
/// including the closing `}`. Returns the remainder of the line after `}`.
///
/// Supports both classic `key="value"` tags and the OpenMetrics/Prometheus
/// UTF-8 quoted syntax (`"key"="value"`, bare `{"metric_name"}`). Port of Go
/// `(*Row).unmarshalTags`.
///
/// Divergence: Go unescapes quoted tag *keys* the same way it unescapes
/// values (via `unescapeValue`). Since [`Tag::key`] here is a borrowed
/// `&'a str` (not `Cow`), a quoted key containing `\"`/`\\`/`\n` is kept
/// verbatim (unescaped) instead of being unescaped like Go does. This only
/// affects the rare UTF-8-quoted-label-with-escapes case; plain keys and all
/// tag values are unaffected and match Go exactly.
fn unmarshal_tags<'a>(r: &mut Row<'a>, mut s: &'a str, has_escape_chars: bool) -> Result<&'a str> {
    loop {
        s = skip_leading_whitespace(s);
        let n = match s.find('"') {
            None => {
                if let Some(rest) = s.strip_prefix('}') {
                    return Ok(rest);
                }
                return Err(ParseError(format!("missing value for tag {s:?}")));
            }
            Some(n) => n,
        };

        let key: &'a str;
        if n == 0 {
            // Quoted key, or a bare quoted metric name: {"metric"} / {"metric", ...}.
            let (raw_key, rest) = extract_quoted(s, has_escape_chars)?;
            s = skip_leading_whitespace(rest);
            if !s.is_empty() {
                let c = s.as_bytes()[0];
                if c == b',' || c == b'}' {
                    if !r.metric.is_empty() {
                        return Err(ParseError(format!(
                            "metric name {:?} already set, duplicate metric name {raw_key:?}",
                            r.metric
                        )));
                    }
                    r.metric = raw_key;
                    if s.len() > 1 && c == b',' {
                        s = &s[1..];
                    }
                    continue;
                } else if c != b'=' {
                    return Err(ParseError(format!(
                        "missing value for quoted tag {raw_key:?}"
                    )));
                }
                s = skip_leading_whitespace(&s[1..]);
            }
            key = raw_key;
        } else {
            let key_part = skip_trailing_whitespace(&s[..n]);
            if !key_part.ends_with('=') {
                return Err(ParseError(format!("missing value for unquoted tag {s:?}")));
            }
            key = skip_trailing_whitespace(&key_part[..key_part.len() - 1]);
            s = &s[n..];
        }

        let (value, rest) = extract_quoted(s, has_escape_chars)?;
        s = rest;
        if !key.is_empty() {
            // Allow empty values - see
            // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/453
            r.tags.push(Tag {
                key,
                value: unescape_value(value),
            });
        }
        s = skip_leading_whitespace(s);
        if let Some(rest) = s.strip_prefix('}') {
            return Ok(rest);
        }
        if !s.starts_with(',') {
            return Err(ParseError(format!(
                "missing comma after tag {key}={value:?}"
            )));
        }
        s = &s[1..];
    }
}

/// Extracts a `"..."` quoted string starting at `s[0]` (which must be `"`).
/// Returns the raw (not unescaped) inner slice and the remainder after the
/// closing quote. Port of Go `unmarshalQuotedString`.
fn extract_quoted(s: &str, has_escape_chars: bool) -> Result<(&str, &str)> {
    if !s.starts_with('"') {
        return Err(ParseError(format!(
            "missing starting double quote in string: {s:?}"
        )));
    }
    if !has_escape_chars {
        // Fast path: no backslashes anywhere in the input, so the first `"`
        // must be the closing quote.
        return match s[1..].find('"') {
            None => Err(ParseError(format!(
                "missing closing double quote in string: {s:?}"
            ))),
            Some(n) => Ok((&s[1..n + 1], &s[n + 2..])),
        };
    }
    match find_closing_quote(s) {
        None => Err(ParseError(format!(
            "missing closing double quote in string: {s:?}"
        ))),
        Some(n) => Ok((&s[1..n], &s[n + 1..])),
    }
}

/// Finds the byte offset of the closing quote for `s`, which must start with
/// `"`. Handles quotes escaped with an odd number of preceding backslashes.
/// Port of Go `findClosingQuote`.
fn find_closing_quote(s: &str) -> Option<usize> {
    if !s.starts_with('"') {
        return None;
    }
    let mut off = 1usize;
    let mut rest = &s[1..];
    loop {
        let n = rest.find('"')?;
        if prev_backslashes_count(&rest[..n]) % 2 == 0 {
            return Some(off + n);
        }
        off += n + 1;
        rest = &rest[n + 1..];
    }
}

/// Counts the number of consecutive `\` bytes at the end of `s`.
/// Port of Go `prevBackslashesCount`.
fn prev_backslashes_count(s: &str) -> usize {
    s.as_bytes()
        .iter()
        .rev()
        .take_while(|&&b| b == b'\\')
        .count()
}

/// Unescapes `\\`, `\"`, and `\n` in a tag value. Any other backslash escape
/// (e.g. `\a`) is kept verbatim, including a trailing lone `\`. Port of Go
/// `unescapeValue`.
fn unescape_value(s: &str) -> Cow<'_, str> {
    let bytes = s.as_bytes();
    let Some(mut n) = bytes.iter().position(|&b| b == b'\\') else {
        // Fast path - nothing to unescape.
        return Cow::Borrowed(s);
    };

    let mut b: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut rest = bytes;
    loop {
        b.extend_from_slice(&rest[..n]);
        rest = &rest[n + 1..];
        if rest.is_empty() {
            b.push(b'\\');
            break;
        }
        match rest[0] {
            b'\\' => b.push(b'\\'),
            b'"' => b.push(b'"'),
            b'n' => b.push(b'\n'),
            other => {
                b.push(b'\\');
                b.push(other);
            }
        }
        rest = &rest[1..];
        match rest.iter().position(|&c| c == b'\\') {
            Some(next_n) => n = next_n,
            None => {
                b.extend_from_slice(rest);
                break;
            }
        }
    }
    // Only ASCII `\` bytes were removed/replaced with other ASCII bytes from
    // valid UTF-8, so the result is valid UTF-8.
    Cow::Owned(String::from_utf8(b).expect("BUG: unescaped tag value must be valid UTF-8"))
}

fn skip_leading_whitespace(mut s: &str) -> &str {
    // Prometheus treats ' ' and '\t' as whitespace. See
    // https://github.com/prometheus/docs/blob/master/docs/instrumenting/exposition_formats.md#line-format
    while let Some(rest) = s.strip_prefix([' ', '\t']) {
        s = rest;
    }
    s
}

fn skip_trailing_whitespace(mut s: &str) -> &str {
    while let Some(rest) = s.strip_suffix([' ', '\t']) {
        s = rest;
    }
    s
}

fn skip_trailing_comment(s: &str) -> &str {
    match s.as_bytes().iter().position(|&b| b == b'#') {
        None => s,
        Some(n) => &s[..n],
    }
}

/// Within a line, tokens can be separated by any number of blanks and/or
/// tabs. Returns the offset of the first ` ` or `\t`, whichever comes first.
fn next_whitespace(s: &str) -> Option<usize> {
    s.as_bytes().iter().position(|&b| b == b' ' || b == b'\t')
}

// ---------------------------------------------------------------------------
// Strict float parsing, ported from github.com/valyala/fastjson/fastfloat's
// `Parse` (not `ParseBestEffort`: unlike the influx parser's field values,
// Prometheus sample values and timestamps use the strict variant, which
// returns an error instead of silently falling back to 0 on trailing/invalid
// input).
// ---------------------------------------------------------------------------

const F64_POW10: [f64; 17] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
];

/// Parses a floating-point number from `s`. Port of `fastfloat.Parse`.
///
/// Divergence from plain `str::parse::<f64>`: a leading `+` is accepted only
/// before `inf`/`infinity`/`nan` (case-insensitive), not before ordinary
/// digits (`+5` is rejected, matching upstream); hex float literals
/// (`0x1p3`) are not supported, matching upstream. Also mirrored verbatim: a
/// value with an elided fractional part after a trailing `.` (e.g. `-5.`)
/// does not get the minus sign re-applied - this is a pre-existing upstream
/// quirk in both `Parse` and `ParseBestEffort`, preserved here for fidelity.
fn parse_prom_float(s: &str) -> Result<f64> {
    let b = s.as_bytes();
    if b.is_empty() {
        return Err(ParseError::new("cannot parse float64 from empty string"));
    }
    let mut i = 0usize;
    let minus = b[0] == b'-';
    if minus {
        i += 1;
        if i >= b.len() {
            return Err(ParseError(format!("cannot parse float64 from {s:?}")));
        }
    }

    // The integer part might be elided, e.g. `.5`.
    if b[i] == b'.' && (i + 1 >= b.len() || !b[i + 1].is_ascii_digit()) {
        return Err(ParseError(format!(
            "missing integer and fractional part in {s:?}"
        )));
    }

    let mut d: u64 = 0;
    let j = i;
    while i < b.len() && b[i].is_ascii_digit() {
        d = d * 10 + u64::from(b[i] - b'0');
        i += 1;
        if i > 18 {
            // The integer part may be out of range for u64. Fall back to
            // standard parsing.
            return s
                .parse::<f64>()
                .map_err(|err| ParseError(format!("cannot parse float64 from {s:?}: {err}")));
        }
    }
    if i <= j && b[i] != b'.' {
        let mut tail = &s[i..];
        if let Some(stripped) = tail.strip_prefix('+') {
            tail = stripped;
        }
        // "infinity" is needed for OpenMetrics support.
        if tail.eq_ignore_ascii_case("inf") || tail.eq_ignore_ascii_case("infinity") {
            return Ok(if minus {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            });
        }
        if tail.eq_ignore_ascii_case("nan") {
            return Ok(f64::NAN);
        }
        return Err(ParseError(format!(
            "unparsed tail left after parsing float64 from {s:?}: {tail:?}"
        )));
    }
    let mut f = d as f64;
    if i >= b.len() {
        // Fast path - just an integer.
        return Ok(if minus { -f } else { f });
    }

    if b[i] == b'.' {
        i += 1;
        if i >= b.len() {
            // The fractional part may be elided. NOTE: no minus sign applied
            // here - see the divergence note on this function.
            return Ok(f);
        }
        let k = i;
        while i < b.len() && b[i].is_ascii_digit() {
            d = d * 10 + u64::from(b[i] - b'0');
            i += 1;
            if i - j >= F64_POW10.len() {
                return s
                    .parse::<f64>()
                    .map_err(|err| ParseError(format!("cannot parse mantissa in {s:?}: {err}")));
            }
        }
        if i < k {
            return Err(ParseError(format!("cannot find mantissa in {s:?}")));
        }
        // Convert the entire mantissa to a float at once to avoid rounding
        // errors.
        f = d as f64 / F64_POW10[i - k];
        if i >= b.len() {
            return Ok(if minus { -f } else { f });
        }
    }
    if b[i] == b'e' || b[i] == b'E' {
        i += 1;
        if i >= b.len() {
            return Err(ParseError(format!("cannot parse exponent in {s:?}")));
        }
        let mut exp_minus = false;
        if b[i] == b'+' || b[i] == b'-' {
            exp_minus = b[i] == b'-';
            i += 1;
            if i >= b.len() {
                return Err(ParseError(format!("cannot parse exponent in {s:?}")));
            }
        }
        let mut exp: i32 = 0;
        let jj = i;
        while i < b.len() && b[i].is_ascii_digit() {
            exp = exp * 10 + i32::from(b[i] - b'0');
            i += 1;
            if exp > 300 {
                return s
                    .parse::<f64>()
                    .map_err(|err| ParseError(format!("cannot parse exponent in {s:?}: {err}")));
            }
        }
        if i <= jj {
            return Err(ParseError(format!("cannot parse exponent in {s:?}")));
        }
        if exp_minus {
            exp = -exp;
        }
        f *= 10f64.powi(exp);
        if i >= b.len() {
            return Ok(if minus { -f } else { f });
        }
    }
    Err(ParseError(format!("cannot parse float64 from {s:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prev_backslashes_count() {
        fn f(s: &str, expected: usize) {
            assert_eq!(prev_backslashes_count(s), expected, "for {s:?}");
        }
        f("", 0);
        f("foo", 0);
        f(r"\", 1);
        f(r"\\", 2);
        f(r"\\\", 3);
        f(r"\\\a", 0);
        f(r"foo\bar", 0);
        f(r"foo\\", 2);
        f(r"\\foo\", 1);
        f(r"\\foo\\\\", 4);
    }

    #[test]
    fn test_find_closing_quote() {
        fn f(s: &str, expected: Option<usize>) {
            assert_eq!(find_closing_quote(s), expected, "for {s:?}");
        }
        f("", None);
        f("x", None);
        f(r#"""#, None);
        f(r#""""#, Some(1));
        f(r#"foobar""#, None);
        f(r#""foo""#, Some(4));
        f(r#""\"""#, Some(3));
        f(r#""\\""#, Some(3));
        f(r#""\""#, None);
        f(r#""foo\"bar\"baz""#, Some(14));
    }

    #[test]
    fn test_unescape_value() {
        fn f(s: &str, expected: &str) {
            assert_eq!(unescape_value(s).as_ref(), expected, "for {s:?}");
        }
        f("", "");
        f("f", "f");
        f("foobar", "foobar");
        f("\\\"\\n\\t", "\"\n\\t");
        f(r"foo\bar", "foo\\bar");
        f(r"foo\", "foo\\");
    }

    #[test]
    fn test_parse_prom_float() {
        fn ok(s: &str, expected: f64) {
            let v = parse_prom_float(s)
                .unwrap_or_else(|err| panic!("unexpected error for {s:?}: {err}"));
            if expected.is_nan() {
                assert!(v.is_nan(), "expected NaN for {s:?}, got {v}");
            } else {
                assert_eq!(v, expected, "for {s:?}");
            }
        }
        fn err(s: &str) {
            assert!(parse_prom_float(s).is_err(), "expected error for {s:?}");
        }

        ok("0", 0.0);
        ok("123", 123.0);
        ok("-123", -123.0);
        ok("123.456", 123.456);
        ok("-123.456", -123.456);
        ok(".5", 0.5);
        ok("1e5", 100000.0);
        ok("1.5e-3", 0.0015);
        ok("-1.5E+3", -1500.0);
        ok("NaN", f64::NAN);
        ok("nan", f64::NAN);
        ok("Inf", f64::INFINITY);
        ok("+Inf", f64::INFINITY);
        ok("-Inf", f64::NEG_INFINITY);
        ok("+Infinity", f64::INFINITY);
        ok("-infinity", f64::NEG_INFINITY);

        err("");
        err("+5");
        err("5a");
        err(".");
        err(".x");
        err("nansf");
        err("0x1p3");
    }

    fn tag<'a>(key: &'a str, value: &'a str) -> Tag<'a> {
        Tag {
            key,
            value: Cow::Borrowed(value),
        }
    }

    fn row<'a>(metric: &'a str, tags: Vec<Tag<'a>>, value: f64, timestamp: i64) -> Row<'a> {
        Row {
            metric,
            tags,
            value,
            timestamp,
        }
    }

    fn unmarshal_ok(s: &str, expected: &[Row<'_>]) {
        let mut rows = Rows::default();
        rows.unmarshal(s, |msg| panic!("unexpected error for {s:?}: {msg}"));
        assert_eq!(rows.rows(), expected, "for {s:?}");

        // Reused Rows should behave identically on a second parse.
        rows.unmarshal(s, |msg| panic!("unexpected error for {s:?}: {msg}"));
        assert_eq!(rows.rows(), expected, "for {s:?} (second pass)");

        rows.reset();
        assert!(rows.rows().is_empty());
    }

    fn unmarshal_invalid(s: &str) {
        let mut rows = Rows::default();
        let mut errs = 0;
        rows.unmarshal(s, |_| errs += 1);
        assert!(rows.rows().is_empty(), "expected no rows for {s:?}");
        assert!(errs > 0, "expected an error to be logged for {s:?}");
    }

    #[test]
    fn test_unmarshal_plain() {
        unmarshal_ok("foo 123", &[row("foo", vec![], 123.0, 0)]);
    }

    #[test]
    fn test_unmarshal_tags_and_timestamp() {
        unmarshal_ok(
            r#"foo{bar="baz",x="y"} 1 1727879909390"#,
            &[row(
                "foo",
                vec![tag("bar", "baz"), tag("x", "y")],
                1.0,
                1727879909390,
            )],
        );
    }

    #[test]
    fn test_unmarshal_escaped_tag_value() {
        unmarshal_ok(
            r#"foo{bar="a\"b\\c\nd"} 1"#,
            &[row("foo", vec![tag("bar", "a\"b\\c\nd")], 1.0, 0)],
        );
    }

    #[test]
    fn test_unmarshal_special_values() {
        let mut rows = Rows::default();
        rows.unmarshal("foo NaN", |msg| panic!("unexpected error: {msg}"));
        assert!(rows.rows()[0].value.is_nan());

        unmarshal_ok("foo +Inf", &[row("foo", vec![], f64::INFINITY, 0)]);
        unmarshal_ok("foo -Inf", &[row("foo", vec![], f64::NEG_INFINITY, 0)]);
        unmarshal_ok("foo 1.5e3", &[row("foo", vec![], 1500.0, 0)]);
    }

    #[test]
    fn test_unmarshal_missing_timestamp_defaults_zero() {
        unmarshal_ok("foo 123   ", &[row("foo", vec![], 123.0, 0)]);
    }

    #[test]
    fn test_unmarshal_invalid_line_skipped() {
        unmarshal_invalid(r#"foo{unclosed 1"#);
    }

    #[test]
    fn test_unmarshal_empty_metric_name_skipped() {
        unmarshal_invalid(r#"{foo="bar"} 1"#);
    }

    #[test]
    fn test_unmarshal_comments_skipped() {
        unmarshal_ok(
            "# HELP foo help text\n# TYPE foo counter\nfoo 1\n",
            &[row("foo", vec![], 1.0, 0)],
        );
    }

    #[test]
    fn test_unmarshal_exemplar_suffix_ignored() {
        // OpenMetrics exemplar suffix after the value/timestamp is treated
        // like any other trailing comment and dropped.
        unmarshal_ok(
            r#"foo_bucket{le="10"} 17 # {trace_id="abc"} 9.8 1520879607.789"#,
            &[row("foo_bucket", vec![tag("le", "10")], 17.0, 0)],
        );
    }

    #[test]
    fn test_unmarshal_timestamp_seconds_converted_to_millis() {
        unmarshal_ok("foo 1 2", &[row("foo", vec![], 1.0, 2000)]);
    }

    #[test]
    fn test_unmarshal_timestamp_already_millis_passthrough() {
        // >= 2^31 is treated as already-milliseconds, not multiplied.
        unmarshal_ok(
            "aaa 1123 429496729600",
            &[row("aaa", vec![], 1123.0, 429496729600)],
        );
    }
}
