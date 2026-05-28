//! JSON line import.
//!
//! Each line is a JSON object shaped like VM's `/api/v1/import` payload:
//!
//! ```json
//! {
//!   "metric": {"__name__": "up", "job": "prom"},
//!   "values": [1.0, 1.0, 0.0],
//!   "timestamps": [1700000000000, 1700000060000, 1700000120000]
//! }
//! ```

#![allow(clippy::cast_possible_truncation)]

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedSample {
    pub metric_name: Vec<u8>,
    pub timestamp_ms: i64,
    pub value: i64,
}

#[derive(Debug, Deserialize)]
struct Row {
    metric: std::collections::BTreeMap<String, String>,
    values: Vec<f64>,
    timestamps: Vec<i64>,
}

/// Parse a multi-line JSON document; one JSON object per line.
///
/// # Errors
/// Returns [`ParseError`] on the first malformed line or shape mismatch.
pub fn parse(input: &str) -> Result<Vec<ParsedSample>, ParseError> {
    let mut out = Vec::new();
    for (line_no, raw) in input.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let row: Row = serde_json::from_str(line)
            .map_err(|e| ParseError::Line { line_no, message: e.to_string() })?;
        if row.values.len() != row.timestamps.len() {
            return Err(ParseError::Line {
                line_no,
                message: format!(
                    "values.len()={} != timestamps.len()={}",
                    row.values.len(),
                    row.timestamps.len()
                ),
            });
        }
        let metric_name = build_metric_name(&row.metric);
        for (v, ts) in row.values.iter().zip(row.timestamps.iter()) {
            out.push(ParsedSample {
                metric_name: metric_name.clone(),
                timestamp_ms: *ts,
                value: *v as i64,
            });
        }
    }
    Ok(out)
}

fn build_metric_name(metric: &std::collections::BTreeMap<String, String>) -> Vec<u8> {
    let name = metric.get("__name__").cloned().unwrap_or_default();
    let mut out = Vec::new();
    out.extend_from_slice(name.as_bytes());
    let mut other: Vec<(&String, &String)> =
        metric.iter().filter(|(k, _)| k.as_str() != "__name__").collect();
    other.sort_by(|a, b| a.0.cmp(b.0));
    if !other.is_empty() {
        out.push(b'{');
        for (i, (k, v)) in other.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.extend_from_slice(k.as_bytes());
            out.extend_from_slice(b"=\"");
            for c in v.chars() {
                match c {
                    '\\' => out.extend_from_slice(b"\\\\"),
                    '"' => out.extend_from_slice(b"\\\""),
                    '\n' => out.extend_from_slice(b"\\n"),
                    other => {
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(other.encode_utf8(&mut buf).as_bytes());
                    }
                }
            }
            out.push(b'"');
        }
        out.push(b'}');
    }
    out
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
    fn parse_one_row_multiple_samples() {
        let s = r#"{"metric":{"__name__":"up","job":"prom"},"values":[1,1,0],"timestamps":[100,200,300]}"#;
        let out = parse(s).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].metric_name, br#"up{job="prom"}"#);
        assert_eq!(out[0].timestamp_ms, 100);
        assert_eq!(out[1].timestamp_ms, 200);
        assert_eq!(out[2].value, 0);
    }

    #[test]
    fn parse_multiple_rows() {
        let s = "{\"metric\":{\"__name__\":\"a\"},\"values\":[1],\"timestamps\":[100]}\n\
                 {\"metric\":{\"__name__\":\"b\"},\"values\":[2],\"timestamps\":[200]}";
        let out = parse(s).unwrap();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn parse_length_mismatch_errors() {
        let s = r#"{"metric":{"__name__":"a"},"values":[1,2],"timestamps":[100]}"#;
        assert!(parse(s).is_err());
    }
}
