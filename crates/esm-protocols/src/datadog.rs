//! DataDog Series API ingest parser.
//!
//! Request shape:
//! ```json
//! {
//!   "series": [
//!     { "metric": "system.cpu.idle",
//!       "points": [[1700000000, 42], [1700000060, 43]],
//!       "tags": ["host:server1", "env:prod"] }
//!   ]
//! }
//! ```
//! Tags are `"k:v"` strings; we split on `:` to produce label key/value pairs.

#![allow(clippy::cast_possible_truncation)]

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedSample {
    pub metric_name: Vec<u8>,
    pub timestamp_ms: i64,
    pub value: i64,
}

/// Parse a DataDog `/api/v1/series` request body.
///
/// # Errors
/// Returns [`ParseError`] on malformed JSON.
pub fn parse(input: &str) -> Result<Vec<ParsedSample>, ParseError> {
    let req: Request = serde_json::from_str(input).map_err(|e| ParseError::Body(e.to_string()))?;
    let mut out: Vec<ParsedSample> = Vec::new();
    for s in req.series {
        let mut tags: Vec<(String, String)> = Vec::new();
        for tag in &s.tags {
            if let Some((k, v)) = tag.split_once(':') {
                tags.push((k.to_string(), v.to_string()));
            } else {
                tags.push((tag.clone(), String::new()));
            }
        }
        tags.sort_by(|a, b| a.0.cmp(&b.0));

        for [ts, v] in &s.points {
            // DataDog timestamps are floating-point unix-seconds.
            let timestamp_ms = (ts * 1000.0) as i64;
            let value = *v as i64;
            let mut metric_name = Vec::new();
            metric_name.extend_from_slice(s.metric.as_bytes());
            if !tags.is_empty() {
                metric_name.push(b'{');
                for (i, (k, val)) in tags.iter().enumerate() {
                    if i > 0 {
                        metric_name.push(b',');
                    }
                    metric_name.extend_from_slice(k.as_bytes());
                    metric_name.extend_from_slice(b"=\"");
                    metric_name.extend_from_slice(val.as_bytes());
                    metric_name.push(b'"');
                }
                metric_name.push(b'}');
            }
            out.push(ParsedSample { metric_name, timestamp_ms, value });
        }
    }
    Ok(out)
}

#[derive(Debug, Deserialize)]
struct Request {
    #[serde(default)]
    series: Vec<Series>,
}

#[derive(Debug, Deserialize)]
struct Series {
    metric: String,
    #[serde(default)]
    points: Vec<[f64; 2]>,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("body: {0}")]
    Body(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic() {
        let s = r#"{"series":[{"metric":"system.cpu.idle","points":[[1700000000,42]],"tags":["host:a","env:prod"]}]}"#;
        let out = parse(s).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name, br#"system.cpu.idle{env="prod",host="a"}"#);
        assert_eq!(out[0].timestamp_ms, 1_700_000_000_000);
        assert_eq!(out[0].value, 42);
    }
}
