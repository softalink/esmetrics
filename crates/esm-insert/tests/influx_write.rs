//! Integration tests: esm-http server + InsertHandlers over a mock sink.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

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

/// Sink collecting decoded rows; can be gated to block inside `add_rows`
/// for the concurrency-limiter test.
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

    fn set_blocked(&self, blocked: bool) {
        *self.blocked.lock().unwrap() = blocked;
        self.unblocked.notify_all();
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
    headers: String,
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
        headers,
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
fn write_plain_lines_returns_204_and_converts_rows() {
    let ts = start_default_server();
    let body = "cpu,hostname=host_0,region=eu-west-1 usage_user=58.1,usage_system=2.2 1451606400000000000\n\
                mem,hostname=host_0 used=1024i 1451606401000000000\n";
    let resp = post(
        ts.server.local_addr(),
        "/write?db=benchmark",
        "",
        body.as_bytes(),
    );
    assert_eq!(resp.status, 204);
    assert!(resp.body.is_empty(), "204 must have an empty body");
    assert!(
        resp.headers.contains("X-Influxdb-Version: 1.8.0"),
        "missing influx version header in {:?}",
        resp.headers
    );

    let rows = ts.sink.take_rows();
    assert_eq!(
        rows,
        vec![
            GotRow {
                metric_group: "cpu_usage_user".to_owned(),
                tags: tags(&[
                    ("hostname", "host_0"),
                    ("region", "eu-west-1"),
                    ("db", "benchmark"),
                ]),
                timestamp: 1451606400000,
                value: 58.1,
            },
            GotRow {
                metric_group: "cpu_usage_system".to_owned(),
                tags: tags(&[
                    ("hostname", "host_0"),
                    ("region", "eu-west-1"),
                    ("db", "benchmark"),
                ]),
                timestamp: 1451606400000,
                value: 2.2,
            },
            GotRow {
                metric_group: "mem_used".to_owned(),
                tags: tags(&[("hostname", "host_0"), ("db", "benchmark")]),
                timestamp: 1451606401000,
                value: 1024.0,
            },
        ]
    );
}

#[test]
fn write_gzip_body_is_decoded() {
    let ts = start_default_server();
    let body = "cpu,hostname=host_1 usage_user=42 1451606400000000000\n";
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(body.as_bytes()).unwrap();
    let gz = enc.finish().unwrap();

    let resp = post(
        ts.server.local_addr(),
        "/write?db=benchmark",
        "Content-Encoding: gzip\r\n",
        &gz,
    );
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "cpu_usage_user");
    assert_eq!(
        rows[0].tags,
        tags(&[("hostname", "host_1"), ("db", "benchmark")])
    );
    assert_eq!(rows[0].timestamp, 1451606400000);
    assert_eq!(rows[0].value, 42.0);
}

#[test]
fn write_10k_line_batch() {
    let ts = start_default_server();
    let base_ts_ns: i64 = 1451606400000000000;
    let mut body = String::with_capacity(1 << 20);
    for i in 0..10_000i64 {
        body.push_str(&format!(
            "cpu,hostname=host_{},region=eu-central-1 usage_user={},usage_system={} {}\n",
            i % 100,
            i,
            i * 2,
            base_ts_ns + i * 1_000_000,
        ));
    }
    let resp = post(
        ts.server.local_addr(),
        "/write?db=benchmark",
        "",
        body.as_bytes(),
    );
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 20_000, "2 fields per line x 10k lines");
    // Spot-check first and last data points.
    assert_eq!(rows[0].metric_group, "cpu_usage_user");
    assert_eq!(rows[0].timestamp, 1451606400000);
    assert_eq!(rows[0].value, 0.0);
    let last = &rows[19_999];
    assert_eq!(last.metric_group, "cpu_usage_system");
    assert_eq!(last.tags[0], ("hostname".to_owned(), "host_99".to_owned()));
    assert_eq!(last.timestamp, 1451606400000 + 9_999);
    assert_eq!(last.value, 19_998.0);
    // Every row carries the db label.
    assert!(rows
        .iter()
        .all(|r| r.tags.last() == Some(&("db".to_owned(), "benchmark".to_owned()))));
}

#[test]
fn malformed_lines_are_skipped_in_stream_mode() {
    let ts = start_default_server();
    let body = "cpu,hostname=h0 usage=1 1451606400000000000\n\
                this line is invalid\n\
                cpu,hostname=h1 usage=2 1451606401000000000\n";
    let resp = post(ts.server.local_addr(), "/write", "", body.as_bytes());
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].value, 1.0);
    assert_eq!(rows[1].value, 2.0);
}

#[test]
fn precision_param_scales_timestamps_to_millis() {
    let ts = start_default_server();
    let resp = post(
        ts.server.local_addr(),
        "/write?precision=s",
        "",
        b"m f=1 5\n",
    );
    assert_eq!(resp.status, 204);
    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].timestamp, 5_000);
}

#[test]
fn string_and_bool_fields_follow_parser_semantics() {
    // v1.146.0: bools become 0/1; quoted strings are converted to numbers
    // best-effort (non-numeric strings become 0) — no rows are dropped.
    let ts = start_default_server();
    let body = "m up=true,down=F,note=\"hello\",qnum=\"12.5\" 1451606400000000000\n";
    let resp = post(ts.server.local_addr(), "/write", "", body.as_bytes());
    assert_eq!(resp.status, 204);
    let rows = ts.sink.take_rows();
    let values: Vec<(String, f64)> = rows
        .iter()
        .map(|r| (r.metric_group.clone(), r.value))
        .collect();
    assert_eq!(
        values,
        vec![
            ("m_up".to_owned(), 1.0),
            ("m_down".to_owned(), 0.0),
            ("m_note".to_owned(), 0.0),
            ("m_qnum".to_owned(), 12.5),
        ]
    );
}

#[test]
fn all_influx_write_paths_are_routed() {
    let ts = start_default_server();
    for path in [
        "/write",
        "/api/v2/write",
        "/influx/write",
        "/influx/api/v2/write",
    ] {
        let resp = post(ts.server.local_addr(), path, "", b"m f=1 5000\n");
        assert_eq!(resp.status, 204, "unexpected status for {path}");
    }
    assert_eq!(ts.sink.take_rows().len(), 4);

    let resp = post(ts.server.local_addr(), "/notwrite", "", b"m f=1 5000\n");
    assert_eq!(resp.status, 404);
}

#[test]
fn doubled_slash_write_path_is_normalized_like_upstream() {
    // Go: `path := strings.ReplaceAll(r.URL.Path, "//", "/")` in vminsert
    // main.go before route matching, so "//write" reaches the influx handler.
    let ts = start_default_server();
    let resp = post(ts.server.local_addr(), "//write", "", b"m f=1 5000\n");
    assert_eq!(resp.status, 204);
    assert_eq!(ts.sink.take_rows().len(), 1);
}

#[test]
fn concurrency_limit_overflow_returns_503() {
    let sink = Arc::new(MockSink::default());
    let handlers = InsertHandlers::with_limits(Arc::clone(&sink), 1, Duration::from_millis(50));
    let ts = start_server(handlers, sink);
    let addr = ts.server.local_addr();

    // First request blocks inside the sink while holding the only slot.
    ts.sink.set_blocked(true);
    let first = std::thread::spawn(move || post(addr, "/write", "", b"m f=1 5000\n"));
    while ts.sink.in_add_rows.load(Ordering::SeqCst) == 0 {
        std::thread::sleep(Duration::from_millis(1));
    }

    // Second request cannot get a slot within the queue duration -> 503.
    let resp = post(addr, "/write", "", b"m f=2 5000\n");
    assert_eq!(resp.status, 503);
    let body = String::from_utf8(resp.body).unwrap();
    assert!(
        body.contains("concurrent insert requests are executed"),
        "unexpected 503 body: {body}"
    );

    // Unblock; the first request completes normally.
    ts.sink.set_blocked(false);
    let first_resp = first.join().unwrap();
    assert_eq!(first_resp.status, 204);
    assert_eq!(ts.sink.take_rows().len(), 1);
}
