//! Graphite plaintext protocol parser.
//!
//! Format: `metric.path value [timestamp]` per line, whitespace-separated.
//! Dots in the metric path become `_` in the canonical name (Graphite
//! convention is hierarchical; flattening lets us reuse the existing
//! storage key format).

#![allow(clippy::cast_possible_truncation)]

use thiserror::Error;

/// One parsed sample.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedSample {
    pub metric_name: Vec<u8>,
    pub timestamp_ms: i64,
    pub value: i64,
}

/// Parse a Graphite plaintext payload. `now_ms` is the fallback timestamp
/// when a line omits it (`-1` is the Graphite-native sentinel for "now").
///
/// # Errors
/// Returns [`ParseError`] on the first malformed line.
pub fn parse(input: &str, now_ms: i64) -> Result<Vec<ParsedSample>, ParseError> {
    let mut out = Vec::new();
    for (line_no, raw) in input.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        parse_line(line, now_ms, &mut out).map_err(|e| ParseError::Line { line_no, source: e })?;
    }
    Ok(out)
}

fn parse_line(line: &str, now_ms: i64, out: &mut Vec<ParsedSample>) -> Result<(), LineError> {
    let mut tokens = line.split_ascii_whitespace();
    let path = tokens.next().ok_or(LineError::MissingPath)?;
    let value_str = tokens.next().ok_or(LineError::MissingValue)?;
    let ts_str = tokens.next();
    if tokens.next().is_some() {
        return Err(LineError::TooManyTokens);
    }

    let value_f: f64 = value_str.parse().map_err(|_| LineError::BadValue(value_str.into()))?;
    let value = value_f as i64;

    let timestamp_ms = match ts_str {
        None | Some("-1") => now_ms,
        Some(s) => {
            let sec: i64 = s.parse().map_err(|_| LineError::BadTimestamp(s.into()))?;
            sec * 1000
        }
    };

    // Graphite paths are dot-separated. We replace `.` with `_` so the
    // canonical name remains a single token; the original path is kept in
    // a `path=` label so it round-trips through PromQL queries.
    let mut metric_name = Vec::new();
    metric_name.extend_from_slice(path.replace('.', "_").as_bytes());
    metric_name.extend_from_slice(b"{path=\"");
    metric_name.extend_from_slice(path.as_bytes());
    metric_name.extend_from_slice(b"\"}");
    out.push(ParsedSample { metric_name, timestamp_ms, value });
    Ok(())
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
    #[error("missing metric path")]
    MissingPath,
    #[error("missing value")]
    MissingValue,
    #[error("too many tokens")]
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
    fn parse_basic() {
        let s = "servers.web1.cpu 42 1700000000\n";
        let out = parse(s, 0).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name, br#"servers_web1_cpu{path="servers.web1.cpu"}"#);
        assert_eq!(out[0].timestamp_ms, 1_700_000_000_000);
        assert_eq!(out[0].value, 42);
    }

    #[test]
    fn parse_negative_one_uses_now() {
        let out = parse("m 1 -1", 999).unwrap();
        assert_eq!(out[0].timestamp_ms, 999);
    }

    #[test]
    fn parse_missing_timestamp_uses_now() {
        let out = parse("m 1", 12345).unwrap();
        assert_eq!(out[0].timestamp_ms, 12345);
    }

    #[test]
    fn parse_blank_lines_skipped() {
        let out = parse("\n# comment\nm 1 0\n", 0).unwrap();
        assert_eq!(out.len(), 1);
    }
}
