//! Prometheus text exposition format parser.
//!
//! Parses the line-oriented format produced by the `/metrics` endpoint of
//! every Prometheus-compatible exporter. Reference:
//! <https://prometheus.io/docs/instrumenting/exposition_formats/>.
//!
//! Grammar (informal):
//!
//! ```text
//! # HELP <metric_name> <description>      ;; optional, ignored
//! # TYPE <metric_name> <type>             ;; optional, ignored
//! <metric_name>[{<label_set>}] <value> [<timestamp_ms>]
//! ```
//!
//! - Lines starting with `#` that are not `HELP` or `TYPE` are ignored.
//! - Blank lines are ignored.
//! - `value` is parsed as `f64`; we convert to `i64` here by rounding to
//!   the nearest integer (the precise float-to-int64 decimal codec lands
//!   alongside the lossy precision modes in a later phase).
//! - `timestamp_ms` is optional; when absent we use the supplied `now_ms`.
//!
//! Output is a flat `Vec<ParsedSample>` ready to feed into
//! `esm_storage::Storage::ingest`.

use thiserror::Error;

/// One sample parsed out of a text exposition document.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedSample {
    /// Canonical metric identifier: bytes of `metric_name{labels-sorted}`.
    pub metric_name: Vec<u8>,
    /// Sample timestamp in epoch milliseconds.
    pub timestamp_ms: i64,
    /// Sample value, rounded to nearest int64. (Lossless float storage lands
    /// with the decimal-scaling codec in Phase 1B.x.)
    pub value: i64,
}

/// Parse `input` as a Prometheus text exposition document. `now_ms` is
/// used for samples without an explicit timestamp.
///
/// # Errors
/// Returns [`ParseError`] on the first malformed line.
pub fn parse(input: &str, now_ms: i64) -> Result<Vec<ParsedSample>, ParseError> {
    let mut out = Vec::new();
    for (line_no, raw_line) in input.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('#') {
            // Comment / HELP / TYPE — ignored.
            continue;
        }
        let sample =
            parse_sample_line(line, now_ms).map_err(|e| ParseError::Line { line_no, source: e })?;
        out.push(sample);
    }
    Ok(out)
}

fn parse_sample_line(line: &str, now_ms: i64) -> Result<ParsedSample, LineError> {
    // Split off the metric_name and the optional label-set.
    let (name, after_name) = read_metric_name(line)?;
    let after_name = after_name.trim_start();

    let (labels_canonical, after_labels) = if let Some(rest) = after_name.strip_prefix('{') {
        let (labels, rest) = parse_label_set(rest)?;
        (Some(labels), rest)
    } else {
        (None, after_name)
    };
    let after_labels = after_labels.trim();

    // Now: value [timestamp]
    let mut tokens = after_labels.split_ascii_whitespace();
    let value_str = tokens.next().ok_or(LineError::MissingValue)?;
    let ts_str = tokens.next();
    if tokens.next().is_some() {
        return Err(LineError::TooManyTokens);
    }

    let value_f64: f64 = value_str.parse().map_err(|_| LineError::BadValue(value_str.into()))?;
    // Round-half-to-even is the default; for Phase 2 MVP we use truncation
    // toward zero matching Rust's `as i64`. Lossless precision lands later.
    #[allow(clippy::cast_possible_truncation)]
    let value = value_f64 as i64;

    let timestamp_ms = match ts_str {
        Some(s) => s.parse().map_err(|_| LineError::BadTimestamp(s.into()))?,
        None => now_ms,
    };

    let mut metric_name =
        Vec::with_capacity(name.len() + labels_canonical.as_deref().map_or(0, str::len) + 2);
    metric_name.extend_from_slice(name.as_bytes());
    if let Some(labels) = labels_canonical {
        metric_name.push(b'{');
        metric_name.extend_from_slice(labels.as_bytes());
        metric_name.push(b'}');
    }

    Ok(ParsedSample { metric_name, timestamp_ms, value })
}

fn read_metric_name(line: &str) -> Result<(&str, &str), LineError> {
    let end = line.find(|c: char| !is_metric_name_char(c)).unwrap_or(line.len());
    if end == 0 {
        return Err(LineError::MissingName);
    }
    Ok(line.split_at(end))
}

fn is_metric_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == ':'
}

/// Parse a label-set starting *after* the `{`. Returns the canonical form
/// `name1="value1",name2="value2"` (sorted by name) and the tail past the
/// closing `}`.
fn parse_label_set(s: &str) -> Result<(String, &str), LineError> {
    let mut rest = s;
    let mut pairs: Vec<(String, String)> = Vec::new();
    loop {
        rest = rest.trim_start();
        if let Some(after) = rest.strip_prefix('}') {
            // Optionally consume a trailing comma — actually `}` is the end.
            rest = after;
            break;
        }
        if rest.is_empty() {
            return Err(LineError::UnclosedLabelSet);
        }
        let (name, after_name) = read_label_name(rest)?;
        let after_name = after_name.trim_start();
        let after_eq = after_name.strip_prefix('=').ok_or(LineError::ExpectedEquals)?.trim_start();
        let (value, after_value) = parse_quoted_string(after_eq)?;
        pairs.push((name.to_string(), value));
        let after_value = after_value.trim_start();
        if let Some(after) = after_value.strip_prefix(',') {
            rest = after;
            continue;
        }
        if let Some(after) = after_value.strip_prefix('}') {
            rest = after;
            break;
        }
        return Err(LineError::ExpectedCommaOrBrace);
    }
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let mut canonical = String::new();
    for (i, (k, v)) in pairs.iter().enumerate() {
        if i > 0 {
            canonical.push(',');
        }
        canonical.push_str(k);
        canonical.push_str("=\"");
        canonical.push_str(&escape_label_value(v));
        canonical.push('"');
    }
    Ok((canonical, rest))
}

fn read_label_name(s: &str) -> Result<(&str, &str), LineError> {
    let end = s.find(|c: char| !is_label_name_char(c)).unwrap_or(s.len());
    if end == 0 {
        return Err(LineError::MissingLabelName);
    }
    Ok(s.split_at(end))
}

fn is_label_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Parse a Prometheus-quoted string starting at `"`. Returns the decoded
/// value and the remainder past the closing quote.
fn parse_quoted_string(s: &str) -> Result<(String, &str), LineError> {
    let bytes = s.as_bytes();
    if bytes.first() != Some(&b'"') {
        return Err(LineError::ExpectedQuote);
    }
    let mut out = String::new();
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                return Ok((out, &s[i + 1..]));
            }
            b'\\' => {
                i += 1;
                if i >= bytes.len() {
                    return Err(LineError::TrailingEscape);
                }
                match bytes[i] {
                    b'n' => out.push('\n'),
                    b'\\' => out.push('\\'),
                    b'"' => out.push('"'),
                    other => out.push(other as char),
                }
                i += 1;
            }
            b if b < 0x80 => {
                out.push(b as char);
                i += 1;
            }
            _ => {
                // UTF-8 multibyte — find the char and push it.
                let c = s[i..].chars().next().ok_or(LineError::TrailingEscape)?;
                out.push(c);
                i += c.len_utf8();
            }
        }
    }
    Err(LineError::UnterminatedString)
}

fn escape_label_value(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("line {line_no}: {source}")]
    Line {
        line_no: usize,
        #[source]
        source: LineError,
    },
}

#[derive(Debug, Error)]
pub enum LineError {
    #[error("missing metric name")]
    MissingName,
    #[error("missing label name inside label set")]
    MissingLabelName,
    #[error("missing value")]
    MissingValue,
    #[error("expected '='")]
    ExpectedEquals,
    #[error("expected ',' or '}}' between labels")]
    ExpectedCommaOrBrace,
    #[error("expected '\"' to start a label value")]
    ExpectedQuote,
    #[error("unterminated quoted string")]
    UnterminatedString,
    #[error("trailing escape character")]
    TrailingEscape,
    #[error("unclosed label set: missing '}}'")]
    UnclosedLabelSet,
    #[error("too many whitespace-separated tokens")]
    TooManyTokens,
    #[error("invalid value: {0:?}")]
    BadValue(String),
    #[error("invalid timestamp: {0:?}")]
    BadTimestamp(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_no_labels() {
        let input = "up 1 1700000000000\n";
        let samples = parse(input, 0).unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].metric_name, b"up");
        assert_eq!(samples[0].value, 1);
        assert_eq!(samples[0].timestamp_ms, 1_700_000_000_000);
    }

    #[test]
    fn parse_with_labels_canonicalises() {
        let input = r#"http_requests_total{path="/foo",code="200"} 42 1700000000000"#;
        let samples = parse(input, 0).unwrap();
        assert_eq!(samples.len(), 1);
        // Labels emitted in sorted order.
        assert_eq!(samples[0].metric_name, br#"http_requests_total{code="200",path="/foo"}"#);
    }

    #[test]
    fn parse_missing_timestamp_uses_now() {
        let input = "metric 5\n";
        let samples = parse(input, 12345).unwrap();
        assert_eq!(samples[0].timestamp_ms, 12345);
    }

    #[test]
    fn parse_value_is_truncated_to_i64() {
        let input = "metric 1.9 0\n";
        let samples = parse(input, 0).unwrap();
        assert_eq!(samples[0].value, 1);
    }

    #[test]
    fn parse_ignores_comments_and_blanks() {
        let input = "# HELP up Up.\n# TYPE up gauge\n\nup 1\n";
        let samples = parse(input, 0).unwrap();
        assert_eq!(samples.len(), 1);
    }

    #[test]
    fn parse_escape_sequences_in_label_values() {
        let input = r#"m{l="a\"b\nc\\d"} 1 0"#;
        let samples = parse(input, 0).unwrap();
        // The canonical form re-escapes special chars.
        assert_eq!(samples[0].metric_name, br#"m{l="a\"b\nc\\d"}"#);
    }

    #[test]
    fn parse_multiple_metrics_in_one_document() {
        let input = "a 1 100\nb{x=\"y\"} 2 200\n";
        let samples = parse(input, 0).unwrap();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].metric_name, b"a");
        assert_eq!(samples[1].metric_name, br#"b{x="y"}"#);
    }

    #[test]
    fn parse_bad_value_errors() {
        let input = "metric notanumber 0\n";
        let r = parse(input, 0);
        assert!(r.is_err());
    }

    #[test]
    fn parse_bad_timestamp_errors() {
        let input = "metric 1 not_a_ts\n";
        let r = parse(input, 0);
        assert!(r.is_err());
    }

    #[test]
    fn parse_unterminated_label_set_errors() {
        let input = "metric{l=\"v 1 0";
        let r = parse(input, 0);
        assert!(r.is_err());
    }
}
