//! NewRelic Metric API ingest.
//!
//! Accepts the NewRelic JSON envelope:
//! ```json
//! [{
//!   "common": { "timestamp": 1700000000, "attributes": {"host": "a"} },
//!   "metrics": [
//!     {"name": "cpu", "type": "gauge", "value": 42, "timestamp": 1700000001,
//!      "attributes": {"region": "us"}}
//!   ]
//! }]
//! ```
//! Timestamps are seconds-precision per the public spec; we widen to ms.
//! `attributes` from `common` are merged into each metric, with per-metric
//! attributes winning on conflict.

#![allow(clippy::cast_possible_truncation)]

use std::collections::BTreeMap;

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedSample {
    pub metric_name: Vec<u8>,
    pub timestamp_ms: i64,
    pub value: i64,
}

#[derive(Debug, Deserialize)]
struct Envelope {
    #[serde(default)]
    common: Common,
    #[serde(default)]
    metrics: Vec<MetricEntry>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Common {
    timestamp: Option<i64>,
    attributes: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct MetricEntry {
    name: String,
    #[serde(default)]
    timestamp: Option<i64>,
    #[serde(default)]
    value: Option<serde_json::Value>,
    #[serde(default)]
    attributes: BTreeMap<String, serde_json::Value>,
}

/// Parse a NewRelic Metric API payload.
///
/// # Errors
/// Returns [`ParseError::Json`] if the body is not valid JSON in the expected
/// envelope shape.
pub fn parse(body: &[u8]) -> Result<Vec<ParsedSample>, ParseError> {
    let envelopes: Vec<Envelope> = serde_json::from_slice(body)?;
    let mut out = Vec::new();
    for env in envelopes {
        let common_ts_ms = env.common.timestamp.map(|s| s.saturating_mul(1000));
        for metric in env.metrics {
            let Some(v) = json_to_i64(metric.value.as_ref()) else {
                continue;
            };
            let timestamp_ms =
                metric.timestamp.map(|s| s.saturating_mul(1000)).or(common_ts_ms).unwrap_or(0);
            let mut tags: BTreeMap<String, String> = BTreeMap::new();
            for (k, val) in &env.common.attributes {
                if let Some(s) = json_to_string(val) {
                    tags.insert(k.clone(), s);
                }
            }
            for (k, val) in &metric.attributes {
                if let Some(s) = json_to_string(val) {
                    tags.insert(k.clone(), s);
                }
            }
            let mut metric_name = Vec::new();
            metric_name.extend_from_slice(metric.name.as_bytes());
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
            out.push(ParsedSample { metric_name, timestamp_ms, value: v });
        }
    }
    Ok(out)
}

fn json_to_i64(v: Option<&serde_json::Value>) -> Option<i64> {
    match v? {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(i)
            } else {
                n.as_f64().map(|f| f as i64)
            }
        }
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn json_to_string(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_common_attributes() {
        let body = br#"[{
            "common": {"timestamp": 1700000000, "attributes": {"host": "a"}},
            "metrics": [
                {"name": "cpu", "value": 42, "timestamp": 1700000001,
                 "attributes": {"region": "us"}}
            ]
        }]"#;
        let out = parse(body).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name, br#"cpu{host="a",region="us"}"#);
        assert_eq!(out[0].timestamp_ms, 1_700_000_001_000);
        assert_eq!(out[0].value, 42);
    }

    #[test]
    fn common_timestamp_used_when_metric_omits_it() {
        let body = br#"[{
            "common": {"timestamp": 1700000000, "attributes": {}},
            "metrics": [{"name": "cpu", "value": 7}]
        }]"#;
        let out = parse(body).unwrap();
        assert_eq!(out[0].timestamp_ms, 1_700_000_000_000);
    }
}
