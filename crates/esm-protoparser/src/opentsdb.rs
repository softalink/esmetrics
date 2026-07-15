//! OpenTSDB telnet `put` protocol parser.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `lib/protoparser/opentsdb/parser.go`.
//!
//! See <http://opentsdb.net/docs/build/html/api_telnet/put.html>. Lines look
//! like `put <metric> <timestamp> <value> <tag1=v1> <tag2=v2> ...`. Tags are
//! space-separated, unlike graphite's semicolon syntax.
//!
//! Like [`crate::graphite`], parsed rows hold `&'a str` slices into the
//! input buffer - opentsdb tags/values are never escaped, so no `Cow` is
//! needed.
//!
//! The streaming entry point ([`parse_stream`]) lives in the sibling
//! [`crate::opentsdb_stream`] module (kept separate to stay under the
//! file-size guideline) and is re-exported here.
//!
//! # Deviations from the Go original
//!
//! - As with the other parsers in this crate, `Rows::unmarshal` takes an
//!   `err_logger` callback even though the real Go `Unmarshal(s string)` has
//!   no such parameter (it calls the package-global `logger.Errorf` and a
//!   `vm_rows_invalid_total` counter internally); this mirrors the same
//!   deviation already made for `crate::vmimport` and `crate::graphite`.
//! - Per upstream (see
//!   <https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3290>), a
//!   `put` line with zero tags is accepted even though the OpenTSDB spec
//!   requires at least one tag pair - ported verbatim, not tightened.

use std::fmt;

pub use crate::opentsdb_stream::{parse_stream, CallbackResult, Error};

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

/// An OpenTSDB tag (from the space-separated `key=value` syntax).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Tag<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

impl<'a> Tag<'a> {
    /// Port of Go `(*Tag).unmarshal`. Unlike graphite, a tag without `=` is
    /// rejected outright.
    fn unmarshal(s: &'a str) -> Result<Self> {
        match s.as_bytes().iter().position(|&b| b == b'=') {
            None => Err(ParseError(format!("missing tag value for {s:?}"))),
            Some(n) => Ok(Tag {
                key: &s[..n],
                value: &s[n + 1..],
            }),
        }
    }
}

/// A single OpenTSDB row.
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

/// Parsed OpenTSDB rows.
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

    /// Returns the parsed rows, mutably (used by [`crate::opentsdb_stream`]
    /// to fill in default/sentinel timestamps after unmarshaling).
    pub fn rows_mut(&mut self) -> &mut [Row<'a>] {
        &mut self.rows[..self.len]
    }

    /// Resets `self`.
    pub fn reset(&mut self) {
        self.len = 0;
    }

    /// Unmarshals OpenTSDB `put` rows from `s`.
    ///
    /// See <http://opentsdb.net/docs/build/html/api_telnet/put.html>
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
            err_logger(&format!("cannot unmarshal OpenTSDB line {s:?}: {err}"));
        }
    }
}

fn unmarshal_single_row<'a>(r: &mut Row<'a>, s: &'a str) -> Result<()> {
    r.reset();
    let s = trim_leading_spaces(s);
    let s = s
        .strip_prefix("put ")
        .ok_or_else(|| ParseError(format!("missing `put ` prefix in {s:?}")))?;
    let s = trim_leading_spaces(s);

    let n = s
        .as_bytes()
        .iter()
        .position(|&b| b == b' ')
        .ok_or_else(|| {
            ParseError(format!(
                "cannot find whitespace between metric and timestamp in {s:?}"
            ))
        })?;
    r.metric = &s[..n];
    if r.metric.is_empty() {
        return Err(ParseError::new("metric cannot be empty"));
    }

    let tail = trim_leading_spaces(&s[n + 1..]);
    let n = tail
        .as_bytes()
        .iter()
        .position(|&b| b == b' ')
        .ok_or_else(|| {
            ParseError(format!(
                "cannot find whitespace between timestamp and value in {tail:?}"
            ))
        })?;
    let timestamp = crate::fastfloat::parse(&tail[..n]).map_err(|err| {
        ParseError(format!(
            "cannot parse timestamp from {:?}: {err}",
            &tail[..n]
        ))
    })?;
    r.timestamp = timestamp as i64;

    let tail = trim_leading_spaces(&tail[n + 1..]);
    let (value_str, tags_str) = match tail.as_bytes().iter().position(|&b| b == b' ') {
        None => {
            // Missing tags. Accepted even though OpenTSDB forbids it
            // (see the "Deviations" note in the module doc comment).
            (tail, "")
        }
        Some(n) => (&tail[..n], &tail[n + 1..]),
    };
    let v = crate::fastfloat::parse(value_str)
        .map_err(|err| ParseError(format!("cannot parse value from {value_str:?}: {err}")))?;
    r.value = v;

    unmarshal_tags(&mut r.tags, tags_str)
        .map_err(|err| ParseError(format!("cannot unmarshal tags in {s:?}: {err}")))?;
    Ok(())
}

fn unmarshal_tags<'a>(tags: &mut Vec<Tag<'a>>, mut s: &'a str) -> Result<()> {
    loop {
        s = trim_leading_spaces(s);
        if s.is_empty() {
            return Ok(());
        }
        match s.as_bytes().iter().position(|&b| b == b' ') {
            None => {
                // The last tag found.
                let tag = Tag::unmarshal(s)?;
                if !tag.key.is_empty() && !tag.value.is_empty() {
                    tags.push(tag);
                }
                return Ok(());
            }
            Some(n) => {
                let tag = Tag::unmarshal(&s[..n])?;
                s = &s[n + 1..];
                if !tag.key.is_empty() && !tag.value.is_empty() {
                    tags.push(tag);
                }
            }
        }
    }
}

fn trim_leading_spaces(mut s: &str) -> &str {
    while let Some(rest) = s.strip_prefix(' ') {
        s = rest;
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_unmarshal_requires_equals() {
        assert!(Tag::unmarshal("foo").is_err());
        assert_eq!(
            Tag::unmarshal("a=b").unwrap(),
            Tag {
                key: "a",
                value: "b"
            }
        );
        assert_eq!(
            Tag::unmarshal("a=b=c").unwrap(),
            Tag {
                key: "a",
                value: "b=c"
            }
        );
    }

    #[test]
    fn trim_leading_spaces_strips_only_spaces() {
        assert_eq!(trim_leading_spaces("  foo"), "foo");
        assert_eq!(trim_leading_spaces("\tfoo"), "\tfoo");
        assert_eq!(trim_leading_spaces(""), "");
    }
}
