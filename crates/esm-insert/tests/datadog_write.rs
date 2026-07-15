//! Integration tests: esm-http server + `InsertHandlers` over a mock sink,
//! for the DataDog `/datadog/api/v1/series`, `/datadog/api/v2/series`
//! ingest paths and the fixed-response agent stub endpoints.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use esm_insert::{InsertHandlers, MetricRow, RowSink};
use esm_storage::MetricName;

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
    content_type: Option<String>,
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
    let content_type = headers
        .lines()
        .find_map(|line| line.strip_prefix("Content-Type: ").map(str::to_owned));
    Response {
        status,
        body: raw[split + 4..].to_vec(),
        content_type,
    }
}

fn tags(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|&(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

// ---------------------------------------------------------------------------
// /datadog/api/v1/series
// ---------------------------------------------------------------------------

#[test]
fn v1_series_upstream_fixture_returns_202_and_status_ok_body() {
    let ts = start_default_server();
    // Fixture ported verbatim from
    // `lib/protoparser/datadogv1/parser_test.go`'s
    // `TestRequestUnmarshalSuccess`.
    let body = br#"
{
  "series": [
    {
      "host": "test.example.com",
      "interval": 20,
      "metric": "system.load.1",
      "device": "/dev/sda",
      "points": [[
        1575317847,
        0.5
      ]],
      "tags": [
        "environment:test"
      ],
      "type": "rate"
    }
  ]
}
"#;
    let resp = post(ts.server.local_addr(), "/datadog/api/v1/series", "", body);
    assert_eq!(resp.status, 202);
    assert_eq!(resp.content_type.as_deref(), Some("application/json"));
    assert_eq!(resp.body, b"{\"status\":\"ok\"}");

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "system.load.1");
    assert_eq!(
        rows[0].tags,
        tags(&[
            ("host", "test.example.com"),
            ("device", "/dev/sda"),
            ("environment", "test"),
        ])
    );
    assert_eq!(rows[0].timestamp, 1_575_317_847_000);
    assert_eq!(rows[0].value, 0.5);
}

#[test]
fn v1_series_sanitizes_metric_name() {
    let ts = start_default_server();
    let body = br#"{"series":[{"metric":"my!!metric.name","points":[[100,1.0]]}]}"#;
    let resp = post(ts.server.local_addr(), "/datadog/api/v1/series", "", body);
    assert_eq!(resp.status, 202);
    let rows = ts.sink.take_rows();
    assert_eq!(rows[0].metric_group, "my_metric.name");
}

#[test]
fn v1_series_gzip_body_is_decoded() {
    let ts = start_default_server();
    let body = br#"{"series":[{"metric":"cpu","host":"h1","points":[[100,3.5]]}]}"#;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(body).unwrap();
    let gz = enc.finish().unwrap();

    let resp = post(
        ts.server.local_addr(),
        "/datadog/api/v1/series",
        "Content-Encoding: gzip\r\n",
        &gz,
    );
    assert_eq!(resp.status, 202);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "cpu");
    assert_eq!(rows[0].tags, tags(&[("host", "h1")]));
    assert_eq!(rows[0].value, 3.5);
}

#[test]
fn v1_series_invalid_json_returns_400() {
    let ts = start_default_server();
    let resp = post(
        ts.server.local_addr(),
        "/datadog/api/v1/series",
        "",
        b"not json at all",
    );
    assert_eq!(resp.status, 400);
}

#[test]
fn v1_series_trailing_slash_is_trimmed() {
    let ts = start_default_server();
    let body = br#"{"series":[{"metric":"m","points":[[100,1.0]]}]}"#;
    let resp = post(ts.server.local_addr(), "/datadog/api/v1/series/", "", body);
    assert_eq!(resp.status, 202, "trailing slash must still route");
    assert_eq!(ts.sink.take_rows().len(), 1);
}

// ---------------------------------------------------------------------------
// /datadog/api/v2/series
// ---------------------------------------------------------------------------

#[test]
fn v2_series_upstream_json_fixture_returns_202_and_status_ok_body() {
    let ts = start_default_server();
    // Fixture ported verbatim from
    // `lib/protoparser/datadogv2/parser_test.go`'s
    // `TestRequestUnmarshalJSONSuccess`.
    let body = br#"
{
  "series": [
    {
      "metric": "system.load.1",
      "type": 0,
      "points": [
        {
          "timestamp": 1636629071,
          "value": 0.7
        }
      ],
      "resources": [
        {
          "name": "dummyhost",
          "type": "host"
        }
      ],
      "source_type_name": "kubernetes",
      "tags": ["environment:test"]
    }
  ]
}
"#;
    let resp = post(ts.server.local_addr(), "/datadog/api/v2/series", "", body);
    assert_eq!(resp.status, 202);
    assert_eq!(resp.content_type.as_deref(), Some("application/json"));
    assert_eq!(resp.body, b"{\"status\":\"ok\"}");

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "system.load.1");
    assert_eq!(
        rows[0].tags,
        tags(&[
            ("host", "dummyhost"),
            ("environment", "test"),
            ("source_type_name", "kubernetes"),
        ])
    );
    assert_eq!(rows[0].timestamp, 1_636_629_071_000);
    assert_eq!(rows[0].value, 0.7);
}

/// Minimal protobuf encoder helpers (LEB128 varint + tag/len-delim framing),
/// mirroring `crates/esm-protoparser/src/datadog/tests.rs`'s.
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
fn append_len_delim(dst: &mut Vec<u8>, field_num: u32, bytes: &[u8]) {
    append_tag(dst, field_num, 2);
    append_varint(dst, bytes.len() as u64);
    dst.extend_from_slice(bytes);
}

#[test]
fn v2_series_protobuf_body_is_accepted_with_content_type_header() {
    let ts = start_default_server();

    // Point{value=1 double, timestamp=2 int64}.
    let mut point = Vec::new();
    append_tag(&mut point, 1, 1);
    point.extend_from_slice(&2.5f64.to_bits().to_le_bytes());
    append_tag(&mut point, 2, 0);
    append_varint(&mut point, 100);

    // Resource{type=1, name=2}.
    let mut resource = Vec::new();
    append_len_delim(&mut resource, 1, b"host");
    append_len_delim(&mut resource, 2, b"h1");

    // Series{resources=1, metric=2, tags=3, points=4}.
    let mut series = Vec::new();
    append_len_delim(&mut series, 1, &resource);
    append_len_delim(&mut series, 2, b"proto.metric");
    append_len_delim(&mut series, 3, b"env:prod");
    append_len_delim(&mut series, 4, &point);

    // Request{series=1}.
    let mut body = Vec::new();
    append_len_delim(&mut body, 1, &series);

    let resp = post(
        ts.server.local_addr(),
        "/datadog/api/v2/series",
        "Content-Type: application/x-protobuf\r\n",
        &body,
    );
    assert_eq!(resp.status, 202);
    assert_eq!(resp.body, b"{\"status\":\"ok\"}");

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "proto.metric");
    assert_eq!(rows[0].tags, tags(&[("host", "h1"), ("env", "prod")]));
    assert_eq!(rows[0].timestamp, 100_000);
    assert_eq!(rows[0].value, 2.5);
}

#[test]
fn v2_series_without_protobuf_content_type_falls_back_to_json() {
    let ts = start_default_server();
    let body = br#"{"series":[{"metric":"m","points":[{"timestamp":1,"value":1.0}]}]}"#;
    // Content-Type absent entirely: Go's default switch arm is JSON.
    let resp = post(ts.server.local_addr(), "/datadog/api/v2/series", "", body);
    assert_eq!(resp.status, 202);
    assert_eq!(ts.sink.take_rows().len(), 1);
}

#[test]
fn v2_series_gzip_body_is_decoded() {
    let ts = start_default_server();
    let body = br#"{"series":[{"metric":"cpu","points":[{"timestamp":100,"value":9.5}]}]}"#;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(body).unwrap();
    let gz = enc.finish().unwrap();

    let resp = post(
        ts.server.local_addr(),
        "/datadog/api/v2/series",
        "Content-Encoding: gzip\r\n",
        &gz,
    );
    assert_eq!(resp.status, 202);
    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "cpu");
    assert_eq!(rows[0].value, 9.5);
}

#[test]
fn v2_series_invalid_json_returns_400() {
    let ts = start_default_server();
    let resp = post(
        ts.server.local_addr(),
        "/datadog/api/v2/series",
        "",
        b"not json at all",
    );
    assert_eq!(resp.status, 400);
}

// ---------------------------------------------------------------------------
// Fixed-response agent stub endpoints.
// Exact statuses/bodies copied from `app/vminsert/main.go` (not guessed —
// see `crates/esm-insert/src/datadog.rs`'s module doc for the file:line
// citations).
// ---------------------------------------------------------------------------

#[test]
fn validate_stub_returns_200_and_valid_true() {
    let ts = start_default_server();
    let resp = post(ts.server.local_addr(), "/datadog/api/v1/validate", "", b"");
    assert_eq!(resp.status, 200);
    assert_eq!(resp.content_type.as_deref(), Some("application/json"));
    assert_eq!(resp.body, b"{\"valid\":true}");
}

#[test]
fn check_run_stub_returns_202_and_status_ok() {
    let ts = start_default_server();
    let resp = post(ts.server.local_addr(), "/datadog/api/v1/check_run", "", b"");
    assert_eq!(resp.status, 202);
    assert_eq!(resp.body, b"{\"status\":\"ok\"}");
}

#[test]
fn intake_stub_returns_200_and_empty_json_object() {
    let ts = start_default_server();
    let resp = post(ts.server.local_addr(), "/datadog/intake", "", b"");
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, b"{}");
}

#[test]
fn metadata_stub_returns_200_and_empty_json_object() {
    // Note: 200, not 201 — `app/vminsert/main.go`'s `/datadog/api/v1/metadata`
    // case never calls `w.WriteHeader`, so Go's implicit default (200)
    // applies. The plan's guess of 201 for this endpoint was wrong; verified
    // by reading main.go directly (see the module doc for datadog.rs).
    let ts = start_default_server();
    let resp = post(ts.server.local_addr(), "/datadog/api/v1/metadata", "", b"");
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, b"{}");
}

#[test]
fn stub_endpoints_trailing_slash_is_trimmed() {
    let ts = start_default_server();
    let resp = post(ts.server.local_addr(), "/datadog/intake/", "", b"");
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, b"{}");
}

#[test]
fn unknown_datadog_path_is_not_matched() {
    let ts = start_default_server();
    let resp = post(ts.server.local_addr(), "/datadog/nope", "", b"");
    assert_eq!(
        resp.status, 404,
        "unmatched paths fall through to the caller's 404"
    );
}
