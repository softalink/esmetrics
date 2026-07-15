//! Integration tests: esm-http server + InsertHandlers over a mock sink,
//! for the OTLP metrics `/opentelemetry/v1/metrics` (+
//! `/opentelemetry/api/v1/push`) path.
//!
//! Payloads are built with the same tiny protobuf wire-writer pattern used
//! by `esm-protoparser/tests/otel_pb.rs` (Task 11) — no protobuf dependency,
//! just enough encoders for the message shapes these tests exercise.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use esm_insert::{InsertHandlers, MetricRow, RowSink};
use esm_storage::MetricName;

// --- tiny protobuf wire-writer test helpers (no protobuf dependency) ---

fn append_varint(dst: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            dst.push(byte);
            break;
        }
        dst.push(byte | 0x80);
    }
}

fn append_tag(dst: &mut Vec<u8>, field_num: u32, wire_type: u8) {
    append_varint(dst, (u64::from(field_num) << 3) | u64::from(wire_type));
}

fn append_bytes_field(dst: &mut Vec<u8>, field_num: u32, data: &[u8]) {
    append_tag(dst, field_num, 2);
    append_varint(dst, data.len() as u64);
    dst.extend_from_slice(data);
}

fn append_fixed64_field(dst: &mut Vec<u8>, field_num: u32, bits: u64) {
    append_tag(dst, field_num, 1);
    dst.extend_from_slice(&bits.to_le_bytes());
}

fn append_double_field(dst: &mut Vec<u8>, field_num: u32, v: f64) {
    append_fixed64_field(dst, field_num, v.to_bits());
}

fn append_packed_fixed64s_field(dst: &mut Vec<u8>, field_num: u32, values: &[u64]) {
    let mut payload = Vec::with_capacity(values.len() * 8);
    for v in values {
        payload.extend_from_slice(&v.to_le_bytes());
    }
    append_bytes_field(dst, field_num, &payload);
}

fn append_packed_doubles_field(dst: &mut Vec<u8>, field_num: u32, values: &[f64]) {
    append_packed_fixed64s_field(
        dst,
        field_num,
        &values.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
    );
}

fn encode_any_value_string(s: &str) -> Vec<u8> {
    let mut dst = Vec::new();
    append_bytes_field(&mut dst, 1, s.as_bytes());
    dst
}

fn encode_key_value(key: &str, any_value: &[u8]) -> Vec<u8> {
    let mut dst = Vec::new();
    append_bytes_field(&mut dst, 1, key.as_bytes());
    append_bytes_field(&mut dst, 2, any_value);
    dst
}

fn encode_number_data_point_double(attrs: &[Vec<u8>], ts: u64, value: f64) -> Vec<u8> {
    let mut dst = Vec::new();
    for a in attrs {
        append_bytes_field(&mut dst, 7, a);
    }
    append_fixed64_field(&mut dst, 3, ts);
    append_double_field(&mut dst, 4, value);
    dst
}

fn encode_gauge(data_points: &[Vec<u8>]) -> Vec<u8> {
    let mut dst = Vec::new();
    for dp in data_points {
        append_bytes_field(&mut dst, 1, dp);
    }
    dst
}

#[allow(clippy::too_many_arguments)]
fn encode_histogram_data_point(
    attrs: &[Vec<u8>],
    ts: u64,
    count: u64,
    sum: Option<f64>,
    bucket_counts: &[u64],
    explicit_bounds: &[f64],
) -> Vec<u8> {
    let mut dst = Vec::new();
    for a in attrs {
        append_bytes_field(&mut dst, 9, a);
    }
    append_fixed64_field(&mut dst, 3, ts);
    append_fixed64_field(&mut dst, 4, count);
    if let Some(sum) = sum {
        append_double_field(&mut dst, 5, sum);
    }
    append_packed_fixed64s_field(&mut dst, 6, bucket_counts);
    append_packed_doubles_field(&mut dst, 7, explicit_bounds);
    dst
}

fn encode_histogram(data_points: &[Vec<u8>]) -> Vec<u8> {
    let mut dst = Vec::new();
    for dp in data_points {
        append_bytes_field(&mut dst, 1, dp);
    }
    dst
}

struct MetricDataField {
    field_num: u32,
    bytes: Vec<u8>,
}

fn encode_metric(name: &str, data: MetricDataField) -> Vec<u8> {
    let mut dst = Vec::new();
    append_bytes_field(&mut dst, 1, name.as_bytes());
    append_bytes_field(&mut dst, data.field_num, &data.bytes);
    dst
}

fn encode_scope_metrics(metrics: &[Vec<u8>]) -> Vec<u8> {
    let mut dst = Vec::new();
    for m in metrics {
        append_bytes_field(&mut dst, 2, m);
    }
    dst
}

fn encode_resource(attrs: &[Vec<u8>]) -> Vec<u8> {
    let mut dst = Vec::new();
    for a in attrs {
        append_bytes_field(&mut dst, 1, a);
    }
    dst
}

fn encode_resource_metrics(resource: Option<&[u8]>, scope_metrics: &[u8]) -> Vec<u8> {
    let mut dst = Vec::new();
    if let Some(resource) = resource {
        append_bytes_field(&mut dst, 1, resource);
    }
    append_bytes_field(&mut dst, 2, scope_metrics);
    dst
}

fn encode_export_request(resource_metrics: &[u8]) -> Vec<u8> {
    let mut dst = Vec::new();
    append_bytes_field(&mut dst, 1, resource_metrics);
    dst
}

// --- test server plumbing (same pattern as prometheusimport_write.rs) ---

/// Owned copy of an ingested row.
#[derive(Debug, Clone, PartialEq)]
struct GotRow {
    metric_group: String,
    tags: Vec<(String, String)>,
    timestamp: i64,
    value: f64,
}

#[derive(Default)]
struct MockSink {
    rows: Mutex<Vec<GotRow>>,
}

impl MockSink {
    fn take_rows(&self) -> Vec<GotRow> {
        std::mem::take(&mut self.rows.lock().unwrap())
    }
}

impl RowSink for MockSink {
    fn add_rows(&self, rows: &[MetricRow<'_>]) -> Result<(), String> {
        let mut got = self.rows.lock().unwrap();
        for row in rows {
            let mut mn = MetricName::default();
            mn.unmarshal_raw(row.metric_name_raw)
                .map_err(|err| format!("cannot unmarshal metric name: {err}"))?;
            got.push(GotRow {
                metric_group: String::from_utf8(mn.metric_group.clone()).unwrap(),
                tags: mn
                    .tags
                    .iter()
                    .map(|t| {
                        (
                            String::from_utf8(t.key.clone()).unwrap(),
                            String::from_utf8(t.value.clone()).unwrap(),
                        )
                    })
                    .collect(),
                timestamp: row.timestamp,
                value: row.value,
            });
        }
        Ok(())
    }
}

struct TestServer {
    server: esm_http::Server,
    sink: Arc<MockSink>,
}

fn start_default_server() -> TestServer {
    let sink = Arc::new(MockSink::default());
    let handlers = InsertHandlers::new(Arc::clone(&sink));
    let server = esm_http::Server::bind("127.0.0.1:0").unwrap();
    let handlers = Arc::new(handlers);
    server.serve(Arc::new(move |req, w| {
        if !handlers.handle(req, w) {
            w.write_status(404);
        }
    }));
    TestServer { server, sink }
}

struct Response {
    status: u16,
    body: Vec<u8>,
}

/// Minimal HTTP client: one request per connection (`Connection: close`).
fn post(addr: std::net::SocketAddr, path: &str, extra_headers: &str, body: &[u8]) -> Response {
    let mut stream = TcpStream::connect(addr).unwrap();
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\
         Content-Length: {}\r\n{extra_headers}\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).unwrap();
    stream.write_all(body).unwrap();

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response must contain header terminator");
    let headers = String::from_utf8(raw[..split].to_vec()).unwrap();
    let status: u16 = headers
        .split(' ')
        .nth(1)
        .expect("status line")
        .parse()
        .unwrap();
    Response {
        status,
        body: raw[split + 4..].to_vec(),
    }
}

fn tags(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|&(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

/// Builds a minimal `ExportMetricsServiceRequest` with one resource
/// (`job=vm`), no scope, and a single Gauge metric with one int data point.
fn gauge_request_bytes(
    metric_name: &str,
    label: (&str, &str),
    ts_nanos: u64,
    value: f64,
) -> Vec<u8> {
    let attr = encode_key_value(label.0, &encode_any_value_string(label.1));
    let dp = encode_number_data_point_double(&[attr], ts_nanos, value);
    let metric = encode_metric(
        metric_name,
        MetricDataField {
            field_num: 5, // Gauge
            bytes: encode_gauge(&[dp]),
        },
    );
    let scope_metrics = encode_scope_metrics(&[metric]);
    let resource = encode_resource(&[encode_key_value("job", &encode_any_value_string("vm"))]);
    let resource_metrics = encode_resource_metrics(Some(&resource), &scope_metrics);
    encode_export_request(&resource_metrics)
}

#[test]
fn identity_body_returns_200_and_converts_gauge_row() {
    let ts = start_default_server();
    let body = gauge_request_bytes("my-gauge", ("label1", "value1"), 15_000_000_000, 15.0);
    let resp = post(
        ts.server.local_addr(),
        "/opentelemetry/v1/metrics",
        "Content-Type: application/x-protobuf\r\n",
        &body,
    );
    assert_eq!(resp.status, 200);
    assert!(resp.body.is_empty(), "200 success must have an empty body");

    let rows = ts.sink.take_rows();
    assert_eq!(
        rows,
        vec![GotRow {
            metric_group: "my-gauge".to_owned(),
            tags: tags(&[("job", "vm"), ("label1", "value1")]),
            timestamp: 15000,
            value: 15.0,
        }]
    );
}

#[test]
fn gzip_body_is_decoded() {
    let ts = start_default_server();
    let body = gauge_request_bytes("cpu", ("host", "h1"), 1_000_000_000, 42.0);
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(&body).unwrap();
    let gz = enc.finish().unwrap();

    let resp = post(
        ts.server.local_addr(),
        "/opentelemetry/v1/metrics",
        "Content-Encoding: gzip\r\n",
        &gz,
    );
    assert_eq!(resp.status, 200);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "cpu");
    assert_eq!(rows[0].tags, tags(&[("job", "vm"), ("host", "h1")]));
    assert_eq!(rows[0].timestamp, 1000);
    assert_eq!(rows[0].value, 42.0);
}

#[test]
fn garbage_body_returns_400() {
    let ts = start_default_server();
    let resp = post(
        ts.server.local_addr(),
        "/opentelemetry/v1/metrics",
        "",
        b"\xff\xff\xff not a protobuf message",
    );
    assert_eq!(resp.status, 400);
    assert!(ts.sink.take_rows().is_empty());
}

#[test]
fn json_content_type_returns_400_with_upstream_message() {
    let ts = start_default_server();
    let resp = post(
        ts.server.local_addr(),
        "/opentelemetry/v1/metrics",
        "Content-Type: application/json\r\n",
        b"{}",
    );
    assert_eq!(resp.status, 400);
    let body = String::from_utf8(resp.body).unwrap();
    assert!(
        body.contains("json encoding isn't supported for opentelemetry format"),
        "unexpected error body: {body:?}"
    );
    assert!(ts.sink.take_rows().is_empty());
}

#[test]
fn legacy_push_path_is_routed() {
    let ts = start_default_server();
    let body = gauge_request_bytes("m", ("k", "v"), 1_000_000_000, 1.0);
    let resp = post(
        ts.server.local_addr(),
        "/opentelemetry/api/v1/push",
        "",
        &body,
    );
    assert_eq!(resp.status, 200);
    assert_eq!(ts.sink.take_rows().len(), 1);
}

/// End-to-end histogram conversion: cumulative `_bucket{le=...}` rows plus
/// `_count`/`_sum`, matching `esm_protoparser::opentelemetry::convert`'s
/// unit-tested bucket cumulation logic — exercised here through the full
/// HTTP + gzip decode + conversion + sink pipeline.
#[test]
fn histogram_end_to_end_cumulates_buckets_through_http() {
    let ts = start_default_server();
    let dp = encode_histogram_data_point(
        &[],
        30_000_000_000,
        15,
        Some(30.0),
        &[0, 5, 10, 0, 0],
        &[0.1, 0.5, 1.0, 5.0],
    );
    let metric = encode_metric(
        "my-histogram",
        MetricDataField {
            field_num: 9, // Histogram
            bytes: encode_histogram(&[dp]),
        },
    );
    let scope_metrics = encode_scope_metrics(&[metric]);
    let resource_metrics = encode_resource_metrics(None, &scope_metrics);
    let body = encode_export_request(&resource_metrics);

    let resp = post(
        ts.server.local_addr(),
        "/opentelemetry/v1/metrics",
        "",
        &body,
    );
    assert_eq!(resp.status, 200);

    let rows = ts.sink.take_rows();
    let by_metric =
        |m: &str| -> Vec<&GotRow> { rows.iter().filter(|r| r.metric_group == m).collect() };

    assert_eq!(by_metric("my-histogram_count").len(), 1);
    assert_eq!(by_metric("my-histogram_count")[0].value, 15.0);
    assert_eq!(by_metric("my-histogram_sum")[0].value, 30.0);

    let buckets = by_metric("my-histogram_bucket");
    assert_eq!(buckets.len(), 5);
    let bucket_le = |le: &str| -> f64 {
        buckets
            .iter()
            .find(|r| r.tags.iter().any(|(k, v)| k == "le" && v == le))
            .unwrap_or_else(|| panic!("no bucket with le={le}"))
            .value
    };
    assert_eq!(bucket_le("0.1"), 0.0);
    assert_eq!(bucket_le("0.5"), 5.0);
    assert_eq!(bucket_le("1"), 15.0);
    assert_eq!(bucket_le("5"), 15.0);
    assert_eq!(bucket_le("+Inf"), 15.0);
}
