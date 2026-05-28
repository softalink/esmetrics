//! OpenTSDB ingest parsers — telnet and HTTP JSON forms.
//!
//! Telnet line: `put <metric> <timestamp> <value> <tagk1>=<tagv1> ...`
//! HTTP JSON: one object (or array of objects) with
//! `{"metric": ..., "timestamp": ..., "value": ..., "tags": {...}}`.

#![allow(clippy::cast_possible_truncation)]

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedSample {
    pub metric_name: Vec<u8>,
    pub timestamp_ms: i64,
    pub value: i64,
}

/// Parse OpenTSDB telnet/plaintext `put` lines.
///
/// # Errors
/// Returns [`ParseError`] on the first malformed line.
pub fn parse_telnet(input: &str) -> Result<Vec<ParsedSample>, ParseError> {
    let mut out = Vec::new();
    for (line_no, raw) in input.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        parse_put_line(line, &mut out).map_err(|message| ParseError::Line { line_no, message })?;
    }
    Ok(out)
}

fn parse_put_line(line: &str, out: &mut Vec<ParsedSample>) -> Result<(), String> {
    let mut iter = line.split_ascii_whitespace();
    let head = iter.next().ok_or("empty line")?;
    if head != "put" {
        return Err(format!("expected 'put', got {head:?}"));
    }
    let metric = iter.next().ok_or("missing metric")?;
    let ts_str = iter.next().ok_or("missing timestamp")?;
    let val_str = iter.next().ok_or("missing value")?;
    let ts: i64 = ts_str.parse().map_err(|_| format!("bad timestamp {ts_str:?}"))?;
    let value_f: f64 = val_str.parse().map_err(|_| format!("bad value {val_str:?}"))?;
    let value = value_f as i64;

    // Tags follow.
    let mut tags: Vec<(String, String)> = Vec::new();
    for tag in iter {
        let Some(eq) = tag.find('=') else { return Err(format!("bad tag {tag:?}")) };
        tags.push((tag[..eq].to_string(), tag[eq + 1..].to_string()));
    }
    tags.sort_by(|a, b| a.0.cmp(&b.0));

    let timestamp_ms = if ts > 9_999_999_999 { ts } else { ts * 1000 };
    let mut metric_name = Vec::new();
    metric_name.extend_from_slice(metric.as_bytes());
    if !tags.is_empty() {
        metric_name.push(b'{');
        for (i, (k, v)) in tags.iter().enumerate() {
            if i > 0 {
                metric_name.push(b',');
            }
            metric_name.extend_from_slice(k.as_bytes());
            metric_name.extend_from_slice(b"=\"");
            metric_name.extend_from_slice(v.as_bytes());
            metric_name.push(b'"');
        }
        metric_name.push(b'}');
    }
    out.push(ParsedSample { metric_name, timestamp_ms, value });
    Ok(())
}

/// Parse the HTTP JSON form. Accepts either one object or an array.
///
/// # Errors
/// Returns [`ParseError`] on malformed JSON or shape.
pub fn parse_http_json(input: &str) -> Result<Vec<ParsedSample>, ParseError> {
    let v: serde_json::Value = serde_json::from_str(input)
        .map_err(|e| ParseError::Line { line_no: 0, message: e.to_string() })?;
    let rows: Vec<Row> = if v.is_array() {
        serde_json::from_value(v)
            .map_err(|e| ParseError::Line { line_no: 0, message: e.to_string() })?
    } else {
        let row: Row = serde_json::from_value(v)
            .map_err(|e| ParseError::Line { line_no: 0, message: e.to_string() })?;
        vec![row]
    };
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let mut tags: Vec<(String, String)> = r.tags.into_iter().collect();
        tags.sort_by(|a, b| a.0.cmp(&b.0));
        let ts_ms = if r.timestamp > 9_999_999_999 { r.timestamp } else { r.timestamp * 1000 };
        let mut metric_name = Vec::new();
        metric_name.extend_from_slice(r.metric.as_bytes());
        if !tags.is_empty() {
            metric_name.push(b'{');
            for (i, (k, v)) in tags.iter().enumerate() {
                if i > 0 {
                    metric_name.push(b',');
                }
                metric_name.extend_from_slice(k.as_bytes());
                metric_name.extend_from_slice(b"=\"");
                metric_name.extend_from_slice(v.as_bytes());
                metric_name.push(b'"');
            }
            metric_name.push(b'}');
        }
        out.push(ParsedSample { metric_name, timestamp_ms: ts_ms, value: r.value as i64 });
    }
    Ok(out)
}

#[derive(Debug, Deserialize)]
struct Row {
    metric: String,
    timestamp: i64,
    value: f64,
    #[serde(default)]
    tags: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("line {line_no}: {message}")]
    Line { line_no: usize, message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_telnet_simple() {
        let s = "put cpu 1700000000 42 host=a region=us\n";
        let out = parse_telnet(s).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name, br#"cpu{host="a",region="us"}"#);
        assert_eq!(out[0].timestamp_ms, 1_700_000_000_000);
        assert_eq!(out[0].value, 42);
    }

    #[test]
    fn parse_http_json_array() {
        let s = r#"[{"metric":"cpu","timestamp":1700000000,"value":42,"tags":{"host":"a"}}]"#;
        let out = parse_http_json(s).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name, br#"cpu{host="a"}"#);
    }
}
