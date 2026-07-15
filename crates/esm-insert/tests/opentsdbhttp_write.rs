//! Integration tests: esm-http server + `OpentsdbHttpHandlers` over a mock
//! sink, for the OpenTSDB HTTP `/api/put` path.
//!
//! Unlike `vmimport_write.rs`/`opentelemetry_write.rs` (which exercise
//! `InsertHandlers`, the main `-httpListenAddr` router),
//! `OpentsdbHttpHandlers` is the standalone handler meant for a *second*,
//! dedicated `esm_http::Server` bound to `-opentsdbHTTPListenAddr` — see
//! `crates/esm-insert/src/opentsdbhttp.rs`'s module doc.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use esm_insert::opentsdbhttp::OpentsdbHttpHandlers;
use esm_insert::{MetricRow, RowSink};
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
    let handlers = Arc::new(OpentsdbHttpHandlers::new(Arc::clone(&sink)));
    let server = esm_http::Server::bind("127.0.0.1:0").unwrap();
    server.serve(Arc::new(move |req, w| {
        handlers.handle(req, w);
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

#[test]
fn single_object_returns_204_and_converts_row() {
    let ts = start_default_server();
    let body = br#"{"metric":"sys.mem","timestamp":1751700000,"value":7,"tags":{"host":"h1"}}"#;
    let resp = post(ts.server.local_addr(), "/api/put", "", body);
    assert_eq!(resp.status, 204);
    assert!(resp.body.is_empty(), "204 must have an empty body");

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "sys.mem");
    assert_eq!(rows[0].tags, tags(&[("host", "h1")]));
    assert_eq!(rows[0].timestamp, 1_751_700_000_000);
    assert_eq!(rows[0].value, 7.0);
}

#[test]
fn array_of_objects_converts_every_row() {
    let ts = start_default_server();
    let body = br#"[{"metric":"foo","timestamp":100,"value":1,"tags":{"a":"b"}},
{"metric":"bar","timestamp":200,"value":2,"tags":{"a":"b"}}]"#;
    let resp = post(ts.server.local_addr(), "/api/put", "", body);
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].metric_group, "foo");
    assert_eq!(rows[0].timestamp, 100_000);
    assert_eq!(rows[1].metric_group, "bar");
    assert_eq!(rows[1].timestamp, 200_000);
}

#[test]
fn opentsdb_api_put_alias_path_is_matched() {
    let ts = start_default_server();
    let body = br#"{"metric":"m","timestamp":1,"value":1}"#;
    let resp = post(ts.server.local_addr(), "/opentsdb/api/put", "", body);
    assert_eq!(resp.status, 204);
    assert_eq!(ts.sink.take_rows().len(), 1);
}

#[test]
fn gzip_body_is_decoded() {
    let ts = start_default_server();
    let body = br#"{"metric":"cpu","timestamp":1000,"value":3.5,"tags":{"host":"h1"}}"#;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(body).unwrap();
    let gz = enc.finish().unwrap();

    let resp = post(
        ts.server.local_addr(),
        "/api/put",
        "Content-Encoding: gzip\r\n",
        &gz,
    );
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "cpu");
    assert_eq!(rows[0].tags, tags(&[("host", "h1")]));
    assert_eq!(rows[0].timestamp, 1_000_000);
    assert_eq!(rows[0].value, 3.5);
}

#[test]
fn missing_timestamp_defaults_to_now() {
    let ts = start_default_server();
    let before_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let body = br#"{"metric":"cpu","value":1}"#;
    let resp = post(ts.server.local_addr(), "/api/put", "", body);
    assert_eq!(resp.status, 204);
    let after_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    // Go's fixup fills seconds-granularity "now", so allow a few seconds of
    // slack on both sides of the request window.
    assert!(
        rows[0].timestamp >= before_ms - 2000 && rows[0].timestamp <= after_ms + 2000,
        "timestamp {} not within request window [{before_ms}, {after_ms}]",
        rows[0].timestamp
    );
}

#[test]
fn fractional_seconds_timestamp_is_truncated_and_rescaled() {
    let ts = start_default_server();
    // 17.89 seconds -> truncated to 17 (int64 cast) -> rescaled to ms since
    // it has no SECOND_MASK bits set.
    let body = br#"{"metric":"cpu","timestamp":17.89,"value":1}"#;
    let resp = post(ts.server.local_addr(), "/api/put", "", body);
    assert_eq!(resp.status, 204);
    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].timestamp, 17_000);
}

#[test]
fn millisecond_timestamp_is_not_rescaled() {
    let ts = start_default_server();
    let body = br#"{"metric":"cpu","timestamp":1700000000000,"value":1}"#;
    let resp = post(ts.server.local_addr(), "/api/put", "", body);
    assert_eq!(resp.status, 204);
    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].timestamp, 1_700_000_000_000);
}

#[test]
fn string_value_and_string_timestamp_are_coerced() {
    let ts = start_default_server();
    let body = br#"{"metric":"cpu","timestamp":"1789","value":"-12.456"}"#;
    let resp = post(ts.server.local_addr(), "/api/put", "", body);
    assert_eq!(resp.status, 204);
    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].timestamp, 1_789_000);
    assert_eq!(rows[0].value, -12.456);
}

#[test]
fn missing_metric_row_is_skipped_others_still_ingested() {
    let ts = start_default_server();
    let body = br#"[{"timestamp":1,"value":1},{"metric":"ok","timestamp":2,"value":2}]"#;
    let resp = post(ts.server.local_addr(), "/api/put", "", body);
    assert_eq!(
        resp.status, 204,
        "the whole batch still succeeds; the bad entry is just skipped"
    );
    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "ok");
}

#[test]
fn malformed_json_syntax_returns_400() {
    let ts = start_default_server();
    let resp = post(ts.server.local_addr(), "/api/put", "", b"{not json");
    assert_eq!(resp.status, 400);
    assert!(ts.sink.take_rows().is_empty());
}

#[test]
fn unmatched_path_returns_400_not_404() {
    // The dedicated OpenTSDB HTTP server has no other routes to fall through
    // to; an unmatched path gets the same 400 as any other InsertHandler
    // error, not a 404 — see the handler's module doc.
    let ts = start_default_server();
    let resp = post(ts.server.local_addr(), "/nonexistent", "", b"{}");
    assert_eq!(resp.status, 400);
}

#[test]
fn extra_label_query_arg_is_appended() {
    let ts = start_default_server();
    let body = br#"{"metric":"cpu","timestamp":1,"value":1,"tags":{"host":"h1"}}"#;
    let resp = post(
        ts.server.local_addr(),
        "/api/put?extra_label=env=prod",
        "",
        body,
    );
    assert_eq!(resp.status, 204);
    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].tags,
        tags(&[("host", "h1"), ("env", "prod")]),
        "extra_label must be appended after row tags"
    );
}
