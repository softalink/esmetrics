//! Integration tests: esm-http server + InsertHandlers over a mock sink,
//! for the Prometheus remote-write `/api/v1/write` path.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

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

/// Sink collecting decoded rows; can be gated to block inside `add_rows`.
#[derive(Default)]
struct MockSink {
    rows: Mutex<Vec<GotRow>>,
    blocked: Mutex<bool>,
    unblocked: Condvar,
    in_add_rows: AtomicUsize,
}

impl MockSink {
    fn take_rows(&self) -> Vec<GotRow> {
        std::mem::take(&mut self.rows.lock().unwrap())
    }
}

impl RowSink for MockSink {
    fn add_rows(&self, rows: &[MetricRow<'_>]) -> Result<(), String> {
        self.in_add_rows.fetch_add(1, Ordering::SeqCst);
        let mut blocked = self.blocked.lock().unwrap();
        while *blocked {
            blocked = self.unblocked.wait(blocked).unwrap();
        }
        drop(blocked);

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

struct FailSink;

impl RowSink for FailSink {
    fn add_rows(&self, _rows: &[MetricRow<'_>]) -> Result<(), String> {
        Err("storage full".to_owned())
    }
}

struct TestServer {
    server: esm_http::Server,
    sink: Arc<MockSink>,
}

fn start_server(handlers: InsertHandlers<Arc<MockSink>>, sink: Arc<MockSink>) -> TestServer {
    let server = esm_http::Server::bind("127.0.0.1:0").unwrap();
    let handlers = Arc::new(handlers);
    server.serve(Arc::new(move |req, w| {
        if !handlers.handle(req, w) {
            w.write_status(404);
        }
    }));
    TestServer { server, sink }
}

fn start_default_server() -> TestServer {
    let sink = Arc::new(MockSink::default());
    start_server(InsertHandlers::new(Arc::clone(&sink)), sink)
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

// --- tiny protobuf wire-writer test helpers (no protobuf dependency) ---
// Same approach as `esm_protoparser::prompb`'s tests.

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

fn append_double_field(dst: &mut Vec<u8>, field_num: u32, v: f64) {
    append_tag(dst, field_num, 1);
    dst.extend_from_slice(&v.to_le_bytes());
}

fn append_varint_field(dst: &mut Vec<u8>, field_num: u32, v: i64) {
    append_tag(dst, field_num, 0);
    append_varint(dst, v as u64);
}

fn encode_label(name: &[u8], value: &[u8]) -> Vec<u8> {
    let mut dst = Vec::new();
    append_bytes_field(&mut dst, 1, name);
    append_bytes_field(&mut dst, 2, value);
    dst
}

fn encode_sample(value: f64, timestamp: i64) -> Vec<u8> {
    let mut dst = Vec::new();
    append_double_field(&mut dst, 1, value);
    append_varint_field(&mut dst, 2, timestamp);
    dst
}

fn encode_time_series(labels: &[(&[u8], &[u8])], samples: &[(f64, i64)]) -> Vec<u8> {
    let mut dst = Vec::new();
    for (name, value) in labels {
        append_bytes_field(&mut dst, 1, &encode_label(name, value));
    }
    for (value, ts) in samples {
        append_bytes_field(&mut dst, 2, &encode_sample(*value, *ts));
    }
    dst
}

fn encode_write_request(timeseries: &[Vec<u8>]) -> Vec<u8> {
    let mut dst = Vec::new();
    for ts in timeseries {
        append_bytes_field(&mut dst, 1, ts);
    }
    dst
}

fn snappy_encode(data: &[u8]) -> Vec<u8> {
    let mut encoder = snap::raw::Encoder::new();
    encoder.compress_vec(data).unwrap()
}

fn tags(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|&(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

#[test]
fn write_snappy_body_returns_204_and_converts_rows() {
    let ts = start_default_server();
    let series = encode_time_series(
        &[(b"__name__", b"cpu_usage"), (b"host", b"h1")],
        &[(58.1, 1451606400000), (60.0, 1451606401000)],
    );
    let body = snappy_encode(&encode_write_request(&[series]));

    let resp = post(ts.server.local_addr(), "/api/v1/write", "", &body);
    assert_eq!(resp.status, 204);
    assert!(resp.body.is_empty(), "204 must have an empty body");

    let rows = ts.sink.take_rows();
    assert_eq!(
        rows,
        vec![
            GotRow {
                metric_group: "cpu_usage".to_owned(),
                tags: tags(&[("host", "h1")]),
                timestamp: 1451606400000,
                value: 58.1,
            },
            GotRow {
                metric_group: "cpu_usage".to_owned(),
                tags: tags(&[("host", "h1")]),
                timestamp: 1451606401000,
                value: 60.0,
            },
        ]
    );
}

#[test]
fn garbage_body_returns_400() {
    let ts = start_default_server();
    let resp = post(
        ts.server.local_addr(),
        "/api/v1/write",
        "",
        b"this is not a valid snappy or zstd frame",
    );
    assert_eq!(resp.status, 400);
    assert!(
        !resp.body.is_empty(),
        "400 body should carry an error message"
    );
}

#[test]
fn blocked_sink_error_returns_503_with_message() {
    // FailSink's add_rows always errors, which the parse callback surfaces
    // as Error::Callback -> 503, mirroring Go InsertCtx.FlushBufs failures.
    let sink = FailSink;
    let handlers = InsertHandlers::new(sink);
    let server = esm_http::Server::bind("127.0.0.1:0").unwrap();
    let handlers = Arc::new(handlers);
    server.serve(Arc::new(move |req, w| {
        if !handlers.handle(req, w) {
            w.write_status(404);
        }
    }));

    let series = encode_time_series(&[(b"__name__", b"m")], &[(1.0, 1)]);
    let body = snappy_encode(&encode_write_request(&[series]));
    let resp = post(server.local_addr(), "/api/v1/write", "", &body);
    assert_eq!(resp.status, 503);
    let body = String::from_utf8(resp.body).unwrap();
    assert!(body.contains("storage full"), "unexpected 503 body: {body}");
}

#[test]
fn all_remote_write_paths_are_routed() {
    let ts = start_default_server();
    let series = encode_time_series(&[(b"__name__", b"m")], &[(1.0, 5000)]);
    let body = snappy_encode(&encode_write_request(&[series]));
    for path in [
        "/api/v1/write",
        "/prometheus/api/v1/write",
        "/api/v1/push",
        "/prometheus/api/v1/push",
    ] {
        let resp = post(ts.server.local_addr(), path, "", &body);
        assert_eq!(resp.status, 204, "unexpected status for {path}");
    }
    assert_eq!(ts.sink.take_rows().len(), 4);
}
