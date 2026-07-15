//! OTLP (OpenTelemetry Protocol) metrics protobuf decode.
//!
//! Port of the *decode* direction of upstream VictoriaMetrics v1.146.0
//! `lib/protoparser/opentelemetry/pb/pb.go` (marshal direction is
//! vmagent-only and is skipped), plus the `AnyValue` array/`KeyValueList`
//! JSON-rendering helpers from `pb/pb_json.go`.
//!
//! ## Deviation from upstream
//!
//! `pb.go` decodes straight into flattened Prometheus labels via a
//! `MetricPusher` callback during the wire walk — there is no intermediate
//! AST. This port instead produces an owned struct tree mirroring the OTLP
//! protobuf messages by name (`ExportMetricsServiceRequest`,
//! `ResourceMetrics`, `Metric`, `NumberDataPoint`, ...). Flattening the tree
//! into Prometheus samples/labels (upstream's `decoderContext` +
//! `histogramDataPointContext`/`exponentialHistogramDataPointContext`/
//! `summaryDataPointContext` `pushSamples` methods) is implemented in
//! [`convert`] — the downstream conversion task the note above referred to.

mod convert;
pub mod pb;
mod sanitize;

pub use convert::{parse_stream, Error, Row, MAX_OTLP_REQUEST_SIZE};
