//! Prometheus remote-write parser.
//!
//! Wire format: snappy-compressed protobuf `prometheus.WriteRequest` (see
//! <https://github.com/prometheus/prometheus/blob/main/prompb/remote.proto>).
//! Both Prometheus servers and vmagent emit this format when writing to a
//! `/api/v1/write` endpoint with `Content-Encoding: snappy`.
//!
//! For Phase 2.x we decode just the fields EsMetrics needs (label list +
//! samples) and skip everything else.

#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use prost::Message;
use thiserror::Error;

/// Decode a snappy-compressed Prometheus remote-write request.
///
/// # Errors
/// Returns [`RemoteWriteError`] on snappy failure, protobuf decode failure,
/// or any decoded label/sample being malformed.
pub fn parse_snappy(body: &[u8]) -> Result<Vec<ParsedTimeSeries>, RemoteWriteError> {
    let mut decoder = snap::raw::Decoder::new();
    let decompressed =
        decoder.decompress_vec(body).map_err(|e| RemoteWriteError::Snappy(e.to_string()))?;
    parse_proto(&decompressed)
}

/// Decode an already-decompressed protobuf payload.
///
/// # Errors
/// See [`RemoteWriteError`].
pub fn parse_proto(payload: &[u8]) -> Result<Vec<ParsedTimeSeries>, RemoteWriteError> {
    let req = WriteRequest::decode(payload).map_err(RemoteWriteError::Decode)?;
    let mut out = Vec::with_capacity(req.timeseries.len());
    for ts in req.timeseries {
        let mut labels: Vec<(String, String)> = Vec::with_capacity(ts.labels.len());
        let mut metric_name: Option<String> = None;
        for lbl in ts.labels {
            if lbl.name == "__name__" {
                metric_name = Some(lbl.value);
            } else {
                labels.push((lbl.name, lbl.value));
            }
        }
        let metric_name = metric_name.ok_or(RemoteWriteError::MissingMetricName)?;
        labels.sort_by(|a, b| a.0.cmp(&b.0));
        let samples: Vec<ParsedSample> = ts
            .samples
            .into_iter()
            .map(|s| ParsedSample { value: s.value, timestamp_ms: s.timestamp })
            .collect();
        out.push(ParsedTimeSeries { metric_name, labels, samples });
    }
    Ok(out)
}

/// One parsed series from a remote-write request.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedTimeSeries {
    pub metric_name: String,
    /// Labels excluding `__name__`, sorted by name.
    pub labels: Vec<(String, String)>,
    pub samples: Vec<ParsedSample>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ParsedSample {
    pub value: f64,
    pub timestamp_ms: i64,
}

impl ParsedTimeSeries {
    /// Build the canonical storage key that matches the text-exposition
    /// parser's output: `metric_name{label1="v1",label2="v2"}` with labels
    /// sorted.
    #[must_use]
    pub fn canonical_storage_key(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            self.metric_name.len()
                + self.labels.iter().map(|(k, v)| k.len() + v.len() + 4).sum::<usize>()
                + 2,
        );
        out.extend_from_slice(self.metric_name.as_bytes());
        if !self.labels.is_empty() {
            out.push(b'{');
            for (i, (k, v)) in self.labels.iter().enumerate() {
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
}

#[derive(Debug, Error)]
pub enum RemoteWriteError {
    #[error("snappy decompression failed: {0}")]
    Snappy(String),
    #[error("protobuf decode failed: {0}")]
    Decode(prost::DecodeError),
    #[error("time series is missing __name__ label")]
    MissingMetricName,
}

// ---------------------------------------------------------------------------
// Protobuf messages — schema lifted from `prometheus/prompb`. Hand-coded so
// no build.rs is needed.

#[derive(Clone, PartialEq, Message)]
struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    timeseries: Vec<TimeSeries>,
    // `metadata` field at tag 3 is intentionally omitted; we don't consume it.
}

#[derive(Clone, PartialEq, Message)]
struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    samples: Vec<Sample>,
    // `exemplars` (tag 3), `histograms` (tag 4) omitted.
}

#[derive(Clone, PartialEq, Message)]
struct Label {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Clone, PartialEq, Message)]
struct Sample {
    #[prost(double, tag = "1")]
    value: f64,
    #[prost(int64, tag = "2")]
    timestamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(req: &WriteRequest) -> Vec<u8> {
        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();
        buf
    }

    #[test]
    fn roundtrip_single_series() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    Label { name: "__name__".into(), value: "up".into() },
                    Label { name: "job".into(), value: "prom".into() },
                ],
                samples: vec![Sample { value: 1.0, timestamp: 1_700_000_000_000 }],
            }],
        };
        let bytes = encode(&req);
        let parsed = parse_proto(&bytes).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].metric_name, "up");
        assert_eq!(parsed[0].labels, vec![("job".to_string(), "prom".to_string())]);
        assert_eq!(parsed[0].samples.len(), 1);
        assert_eq!(parsed[0].samples[0].timestamp_ms, 1_700_000_000_000);
        assert!((parsed[0].samples[0].value - 1.0).abs() < 1e-9);
    }

    #[test]
    fn snappy_roundtrip() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label { name: "__name__".into(), value: "x".into() }],
                samples: vec![Sample { value: 42.0, timestamp: 1 }],
            }],
        };
        let raw = encode(&req);
        let mut encoder = snap::raw::Encoder::new();
        let compressed = encoder.compress_vec(&raw).unwrap();
        let parsed = parse_snappy(&compressed).unwrap();
        assert_eq!(parsed[0].metric_name, "x");
        assert_eq!(parsed[0].samples[0].timestamp_ms, 1);
    }

    #[test]
    fn missing_name_rejected() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label { name: "x".into(), value: "y".into() }],
                samples: vec![Sample { value: 0.0, timestamp: 0 }],
            }],
        };
        let bytes = encode(&req);
        assert!(matches!(parse_proto(&bytes), Err(RemoteWriteError::MissingMetricName)));
    }

    #[test]
    fn canonical_storage_key_matches_text_format() {
        let ts = ParsedTimeSeries {
            metric_name: "http_requests_total".to_string(),
            labels: vec![
                ("code".to_string(), "200".to_string()),
                ("job".to_string(), "api".to_string()),
            ],
            samples: vec![],
        };
        let key = ts.canonical_storage_key();
        assert_eq!(key, br#"http_requests_total{code="200",job="api"}"#);
    }
}
