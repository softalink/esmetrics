//! Graphite plaintext protocol parser.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `lib/protoparser/graphite/parser.go`.
//!
//! See <https://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-plaintext-protocol>.
//! Lines look like `metric.path;tag1=v1;tag2=v2 value timestamp`, with tags
//! and the timestamp both optional. Fields may be separated by a run of
//! spaces and/or tabs (see `graphiteSeparators` in the Go source, citing
//! <https://github.com/grobian/carbon-c-relay/commit/f3ffe6cc2b52b07d14acbda649ad3fd6babdd528>).
//!
//! Like [`crate::prometheus`], parsed rows hold `&'a str` slices into the
//! input buffer - graphite tags are never escaped, so no `Cow` is needed
//! anywhere in this module (unlike influx/prometheus tag values).
//!
//! The streaming entry point ([`parse_stream`]) lives in the sibling
//! [`crate::graphite_stream`] module (kept separate to stay under the
//! file-size guideline) and is re-exported here.
//!
//! # Deviations from the Go original
//!
//! - `-graphite.sanitizeMetricName` (a CLI flag, off by default, that runs
//!   metric/tag-key names through a regexp-based sanitizer) is not ported.
//!   It is disabled by default and porting it would require introducing
//!   global mutable flag state plus a regex dependency into a pure parser
//!   module; that is out of scope for this parser-only task. `TestRowsUnmarshal_SanitizeMetricNamesSuccess`
//!   is accordingly not ported either.
//! - As with the other parsers in this crate, `Rows::unmarshal` takes an
//!   `err_logger` callback even though the real Go `Unmarshal(s string)` has
//!   no such parameter (it calls the package-global `logger.Errorf` and a
//!   `vm_rows_invalid_total` counter internally); this mirrors the same
//!   deviation already made for `crate::vmimport` and `crate::csvimport`.

use std::fmt;

pub use crate::graphite_stream::{parse_stream, CallbackResult, Error};

/// Parse error for a single line.
///
/// `Rows::unmarshal` only passes the formatted message to `err_logger` and
/// skips the line, so this type is never surfaced there; it is `pub` solely
/// because [`Row::unmarshal_metric_and_tags`] (mirroring the public Go
/// method of the same name) returns it directly.
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

/// A graphite tag (from the `;key=value` syntax). Never escaped, unlike
/// influx/prometheus tag values.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Tag<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

impl<'a> Tag<'a> {
    /// Port of Go `(*Tag).unmarshal`. A tag without `=` gets an empty value
    /// rather than being rejected (see
    /// <https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1100>).
    fn unmarshal(s: &'a str) -> Self {
        match s.as_bytes().iter().position(|&b| b == b'=') {
            None => Tag { key: s, value: "" },
            Some(n) => Tag {
                key: &s[..n],
                value: &s[n + 1..],
            },
        }
    }
}

/// A single graphite row.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row<'a> {
    pub metric: &'a str,
    pub tags: Vec<Tag<'a>>,
    pub value: f64,
    pub timestamp: i64,
}

impl<'a> Row<'a> {
    fn reset(&mut self) {
        self.metric = "";
        self.tags.clear();
        self.value = 0.0;
        self.timestamp = 0;
    }

    /// Unmarshals metric and optional (semicolon-separated) tags from `s`.
    /// Port of Go `(*Row).UnmarshalMetricAndTags`, exposed publicly since
    /// upstream's `app/vmselect/graphite` tags API calls it directly.
    ///
    /// Only `metric`/`tags` are touched, matching Go (which leaves other
    /// `Row` fields alone); callers that need a clean row should `Row::default()`
    /// it first.
    pub fn unmarshal_metric_and_tags(&mut self, s: &'a str) -> Result<()> {
        match s.as_bytes().iter().position(|&b| b == b';') {
            None => {
                // No tags.
                self.metric = s;
            }
            Some(n) => {
                self.metric = &s[..n];
                self.tags.clear();
                unmarshal_tags(&mut self.tags, &s[n + 1..]);
            }
        }
        if self.metric.is_empty() {
            return Err(ParseError::new("metric cannot be empty"));
        }
        Ok(())
    }
}

/// Parsed graphite rows.
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

    /// Returns the parsed rows, mutably (used by [`crate::graphite_stream`]
    /// to fill in default/sentinel timestamps after unmarshaling).
    pub fn rows_mut(&mut self) -> &mut [Row<'a>] {
        &mut self.rows[..self.len]
    }

    /// Resets `self`.
    pub fn reset(&mut self) {
        self.len = 0;
    }

    /// Unmarshals graphite plaintext protocol rows from `s`.
    ///
    /// See <https://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-plaintext-protocol>
    ///
    /// Invalid lines are skipped; the formatted error is passed to
    /// `err_logger` for each one.
    pub fn unmarshal(&mut self, s: &'a str, mut err_logger: impl FnMut(&str)) {
        self.reset();
        let mut rest = s;
        while !rest.is_empty() {
            match rest.as_bytes().iter().position(|&b| b == b'\n') {
                None => {
                    self.unmarshal_row(rest, &mut err_logger);
                    break;
                }
                Some(n) => {
                    self.unmarshal_row(&rest[..n], &mut err_logger);
                    rest = &rest[n + 1..];
                }
            }
        }
    }

    fn unmarshal_row(&mut self, s: &'a str, err_logger: &mut impl FnMut(&str)) {
        let s = s.strip_suffix('\r').unwrap_or(s);
        let s = strip_leading_whitespace(s);
        if s.is_empty() {
            // Skip empty line.
            return;
        }

        if self.len < self.rows.len() {
            self.rows[self.len].reset();
        } else {
            self.rows.push(Row::default());
        }
        self.len += 1;
        let idx = self.len - 1;
        if let Err(err) = unmarshal_single_row(&mut self.rows[idx], s) {
            self.len -= 1;
            err_logger(&format!("cannot unmarshal Graphite line {s:?}: {err}"));
        }
    }
}

fn unmarshal_single_row<'a>(r: &mut Row<'a>, s_orig: &'a str) -> Result<()> {
    r.reset();
    let s = strip_trailing_whitespace(s_orig);
    let n = last_separator(s).ok_or_else(|| {
        ParseError(format!(
            "cannot find separator between value and timestamp in {s:?}"
        ))
    })?;
    let timestamp_str_full = &s[n + 1..];
    let s = strip_trailing_whitespace(&s[..n]);

    let (metric_and_tags, value_str, timestamp_str) = match last_separator(s) {
        None => {
            // Missing timestamp.
            (strip_leading_whitespace(s), timestamp_str_full, "")
        }
        Some(n) => (
            strip_leading_whitespace(&s[..n]),
            &s[n + 1..],
            timestamp_str_full,
        ),
    };
    let metric_and_tags = strip_trailing_whitespace(metric_and_tags);
    r.unmarshal_metric_and_tags(metric_and_tags).map_err(|err| {
        ParseError(format!(
            "cannot parse metric and tags from {metric_and_tags:?}: {err}; original line: {s_orig:?}"
        ))
    })?;

    if !timestamp_str.is_empty() {
        let ts = crate::fastfloat::parse(timestamp_str).map_err(|err| {
            ParseError(format!(
                "cannot unmarshal timestamp from {timestamp_str:?}: {err}; original line: {s_orig:?}"
            ))
        })?;
        r.timestamp = ts as i64;
    }
    let v = crate::fastfloat::parse(value_str).map_err(|err| {
        ParseError(format!(
            "cannot unmarshal metric value from {value_str:?}: {err}; original line: {s_orig:?}"
        ))
    })?;
    r.value = v;
    Ok(())
}

fn unmarshal_tags<'a>(tags: &mut Vec<Tag<'a>>, mut s: &'a str) {
    loop {
        match s.as_bytes().iter().position(|&b| b == b';') {
            None => {
                let tag = Tag::unmarshal(s);
                if !tag.key.is_empty() && !tag.value.is_empty() {
                    tags.push(tag);
                }
                return;
            }
            Some(n) => {
                let tag = Tag::unmarshal(&s[..n]);
                s = &s[n + 1..];
                if !tag.key.is_empty() && !tag.value.is_empty() {
                    tags.push(tag);
                }
            }
        }
    }
}

/// Graphite text line protocol may use a space or a tab as a field
/// separator. Port of the `graphiteSeparators` search helpers.
fn last_separator(s: &str) -> Option<usize> {
    s.as_bytes().iter().rposition(|&b| b == b' ' || b == b'\t')
}

fn strip_trailing_whitespace(mut s: &str) -> &str {
    while let Some(rest) = s.strip_suffix([' ', '\t']) {
        s = rest;
    }
    s
}

fn strip_leading_whitespace(mut s: &str) -> &str {
    while let Some(rest) = s.strip_prefix([' ', '\t']) {
        s = rest;
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag<'a>(key: &'a str, value: &'a str) -> Tag<'a> {
        Tag { key, value }
    }

    #[test]
    fn unmarshal_metric_and_tags_failure() {
        let mut r = Row::default();
        assert!(r.unmarshal_metric_and_tags("").is_err());
        let mut r = Row::default();
        assert!(r.unmarshal_metric_and_tags(";foo=bar").is_err());
    }

    #[test]
    fn unmarshal_metric_and_tags_success() {
        fn f(s: &str, metric: &str, tags: Vec<Tag<'_>>) {
            let mut r = Row::default();
            r.unmarshal_metric_and_tags(s).unwrap();
            assert_eq!(r.metric, metric, "for {s:?}");
            assert_eq!(r.tags, tags, "for {s:?}");
        }

        f(" ", " ", vec![]);
        f("foo ;bar=baz", "foo ", vec![tag("bar", "baz")]);
        f("f oo;bar=baz", "f oo", vec![tag("bar", "baz")]);
        f("foo;bar=baz   ", "foo", vec![tag("bar", "baz   ")]);
        f("foo;bar= baz", "foo", vec![tag("bar", " baz")]);
        f("foo;bar=b az", "foo", vec![tag("bar", "b az")]);
        f("foo;b ar=baz", "foo", vec![tag("b ar", "baz")]);
        f("foo", "foo", vec![]);
        f(
            "foo;bar=123;baz=aa=bb",
            "foo",
            vec![tag("bar", "123"), tag("baz", "aa=bb")],
        );
    }

    #[test]
    fn strip_whitespace_helpers() {
        assert_eq!(strip_trailing_whitespace("foo  \t "), "foo");
        assert_eq!(strip_leading_whitespace(" \t foo"), "foo");
        assert_eq!(strip_trailing_whitespace(""), "");
        assert_eq!(strip_leading_whitespace(""), "");
    }

    #[test]
    fn last_separator_finds_space_or_tab() {
        assert_eq!(last_separator("foo bar"), Some(3));
        assert_eq!(last_separator("foo\tbar"), Some(3));
        assert_eq!(last_separator("foobar"), None);
    }
}
