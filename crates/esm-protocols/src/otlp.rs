//! OpenTelemetry metrics ingest (OTLP protobuf).
//!
//! Decodes the subset of `opentelemetry.proto.collector.metrics.v1`
//! actually needed to land metric samples: gauge + sum data points only.
//! Histograms and exponential histograms are skipped — they need a
//! storage-layer mapping that we don't yet have.
//!
//! Resource and scope attributes are flattened into per-sample labels;
//! data-point attributes win on conflict.

#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_precision_loss)]

use std::collections::BTreeMap;

use prost::Message;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedSample {
    pub metric_name: Vec<u8>,
    pub timestamp_ms: i64,
    pub value: i64,
}

/// Decode an OTLP `ExportMetricsServiceRequest` payload.
///
/// # Errors
/// Returns [`OtlpError::Decode`] on a malformed protobuf payload.
pub fn parse(body: &[u8]) -> Result<Vec<ParsedSample>, OtlpError> {
    let req = ExportMetricsServiceRequest::decode(body).map_err(OtlpError::Decode)?;
    let mut out = Vec::new();
    for rm in req.resource_metrics {
        let resource_attrs =
            rm.resource.map(|r| extract_string_attrs(&r.attributes)).unwrap_or_default();
        for sm in rm.scope_metrics {
            let scope_attrs =
                sm.scope.map(|s| extract_string_attrs(&s.attributes)).unwrap_or_default();
            for metric in sm.metrics {
                let points = collect_points(&metric);
                let name = metric.name;
                for dp in points {
                    let value = dp_value(&dp);
                    let timestamp_ms = (dp.time_unix_nano / 1_000_000) as i64;
                    let mut attrs = resource_attrs.clone();
                    for (k, v) in &scope_attrs {
                        attrs.insert(k.clone(), v.clone());
                    }
                    for (k, v) in extract_string_attrs(&dp.attributes) {
                        attrs.insert(k, v);
                    }
                    let metric_name = build_storage_key(&name, &attrs);
                    out.push(ParsedSample { metric_name, timestamp_ms, value });
                }
            }
        }
    }
    Ok(out)
}

fn collect_points(metric: &Metric) -> Vec<NumberDataPoint> {
    if let Some(g) = &metric.gauge {
        return g.data_points.clone();
    }
    if let Some(s) = &metric.sum {
        return s.data_points.clone();
    }
    Vec::new()
}

fn dp_value(dp: &NumberDataPoint) -> i64 {
    if dp.as_int != 0 {
        return dp.as_int;
    }
    if dp.as_double != 0.0 {
        return dp.as_double as i64;
    }
    0
}

fn extract_string_attrs(attrs: &[KeyValue]) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for kv in attrs {
        if let Some(v) = &kv.value
            && let Some(s) = &v.string_value
        {
            out.insert(kv.key.clone(), s.clone());
        }
    }
    out
}

fn build_storage_key(name: &str, attrs: &BTreeMap<String, String>) -> Vec<u8> {
    let mut out = Vec::with_capacity(name.len() + 16);
    out.extend_from_slice(name.as_bytes());
    if !attrs.is_empty() {
        out.push(b'{');
        for (i, (k, v)) in attrs.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.extend_from_slice(k.as_bytes());
            out.extend_from_slice(b"=\"");
            out.extend_from_slice(v.as_bytes());
            out.push(b'"');
        }
        out.push(b'}');
    }
    out
}

#[derive(Debug, Error)]
pub enum OtlpError {
    #[error("protobuf decode: {0}")]
    Decode(#[from] prost::DecodeError),
}

// ---------------------------------------------------------------------------
// Hand-rolled subset of the OTLP metrics schema. Tags + types match the
// upstream `.proto` definitions; fields we don't need are intentionally
// omitted (prost ignores unknown tags).

#[derive(Clone, PartialEq, Message)]
struct ExportMetricsServiceRequest {
    #[prost(message, repeated, tag = "1")]
    resource_metrics: Vec<ResourceMetrics>,
}

#[derive(Clone, PartialEq, Message)]
struct ResourceMetrics {
    #[prost(message, optional, tag = "1")]
    resource: Option<Resource>,
    #[prost(message, repeated, tag = "2")]
    scope_metrics: Vec<ScopeMetrics>,
}

#[derive(Clone, PartialEq, Message)]
struct Resource {
    #[prost(message, repeated, tag = "1")]
    attributes: Vec<KeyValue>,
}

#[derive(Clone, PartialEq, Message)]
struct ScopeMetrics {
    #[prost(message, optional, tag = "1")]
    scope: Option<InstrumentationScope>,
    #[prost(message, repeated, tag = "2")]
    metrics: Vec<Metric>,
}

#[derive(Clone, PartialEq, Message)]
struct InstrumentationScope {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(message, repeated, tag = "3")]
    attributes: Vec<KeyValue>,
}

#[derive(Clone, PartialEq, Message)]
struct Metric {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(message, optional, tag = "5")]
    gauge: Option<Gauge>,
    #[prost(message, optional, tag = "7")]
    sum: Option<Sum>,
}

#[derive(Clone, PartialEq, Message)]
struct Gauge {
    #[prost(message, repeated, tag = "1")]
    data_points: Vec<NumberDataPoint>,
}

#[derive(Clone, PartialEq, Message)]
struct Sum {
    #[prost(message, repeated, tag = "1")]
    data_points: Vec<NumberDataPoint>,
}

#[derive(Clone, PartialEq, Message)]
struct NumberDataPoint {
    #[prost(message, repeated, tag = "7")]
    attributes: Vec<KeyValue>,
    #[prost(fixed64, tag = "3")]
    time_unix_nano: u64,
    #[prost(double, tag = "4")]
    as_double: f64,
    #[prost(sfixed64, tag = "6")]
    as_int: i64,
}

#[derive(Clone, PartialEq, Message)]
struct KeyValue {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(message, optional, tag = "2")]
    value: Option<AnyValue>,
}

#[derive(Clone, PartialEq, Message)]
struct AnyValue {
    #[prost(string, optional, tag = "1")]
    string_value: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_gauge_with_attrs() {
        let req = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "host".into(),
                        value: Some(AnyValue { string_value: Some("a".into()) }),
                    }],
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "cpu".into(),
                        gauge: Some(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![KeyValue {
                                    key: "region".into(),
                                    value: Some(AnyValue { string_value: Some("us".into()) }),
                                }],
                                time_unix_nano: 1_700_000_000_000_000_000,
                                as_double: 0.0,
                                as_int: 42,
                            }],
                        }),
                        sum: None,
                    }],
                }],
            }],
        };
        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();
        let out = parse(&buf).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name, br#"cpu{host="a",region="us"}"#);
        assert_eq!(out[0].timestamp_ms, 1_700_000_000_000);
        assert_eq!(out[0].value, 42);
    }
}
