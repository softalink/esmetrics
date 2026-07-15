//! Integration tests: esm-http server + InsertHandlers over a mock sink,
//! for the Prometheus exposition-text `/api/v1/import/prometheus` path.

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

/// Sink collecting decoded rows.
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

#[test]
fn plain_body_returns_204_and_converts_rows() {
    let ts = start_default_server();
    let body = "foo{location=\"us-midwest1\"} 81 1727879909390\n\
                bar{location=\"us-midwest2\",env=\"prod\"} 82 1727879909391\n";
    let resp = post(
        ts.server.local_addr(),
        "/api/v1/import/prometheus",
        "",
        body.as_bytes(),
    );
    assert_eq!(resp.status, 204);
    assert!(resp.body.is_empty(), "204 must have an empty body");

    let rows = ts.sink.take_rows();
    assert_eq!(
        rows,
        vec![
            GotRow {
                metric_group: "foo".to_owned(),
                tags: tags(&[("location", "us-midwest1")]),
                timestamp: 1727879909390,
                value: 81.0,
            },
            GotRow {
                metric_group: "bar".to_owned(),
                tags: tags(&[("location", "us-midwest2"), ("env", "prod")]),
                timestamp: 1727879909391,
                value: 82.0,
            },
        ]
    );
}

#[test]
fn gzip_body_is_decoded() {
    let ts = start_default_server();
    let body = "cpu{host=\"h1\"} 42 1727879909390\n";
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(body.as_bytes()).unwrap();
    let gz = enc.finish().unwrap();

    let resp = post(
        ts.server.local_addr(),
        "/api/v1/import/prometheus",
        "Content-Encoding: gzip\r\n",
        &gz,
    );
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "cpu");
    assert_eq!(rows[0].tags, tags(&[("host", "h1")]));
    assert_eq!(rows[0].timestamp, 1727879909390);
    assert_eq!(rows[0].value, 42.0);
}

#[test]
fn pushgateway_job_path_variant_returns_200_and_adds_job_label() {
    let ts = start_default_server();
    let body = "cpu 5\n";
    let resp = post(
        ts.server.local_addr(),
        "/api/v1/import/prometheus/metrics/job/backup/instance/host1",
        "",
        body.as_bytes(),
    );
    // Go: main.go returns 200 (not 204) specifically for Pushgateway-style
    // requests. See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3636
    assert_eq!(resp.status, 200);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "cpu");
    assert_eq!(
        rows[0].tags,
        tags(&[("job", "backup"), ("instance", "host1")])
    );
    assert_eq!(rows[0].value, 5.0);
}

#[test]
fn invalid_lines_skipped_request_still_204() {
    let ts = start_default_server();
    let body = "foo 1 1727879909390\n\
                {missing_metric=\"x\"} 2\n\
                bar 3 1727879909391\n";
    let resp = post(
        ts.server.local_addr(),
        "/api/v1/import/prometheus",
        "",
        body.as_bytes(),
    );
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].metric_group, "foo");
    assert_eq!(rows[0].timestamp, 1727879909390);
    assert_eq!(rows[1].metric_group, "bar");
    assert_eq!(rows[1].timestamp, 1727879909391);
}

#[test]
fn timestamp_query_arg_sets_default_timestamp() {
    let ts = start_default_server();
    let resp = post(
        ts.server.local_addr(),
        "/api/v1/import/prometheus?timestamp=1700000000000",
        "",
        b"foo 1\n",
    );
    assert_eq!(resp.status, 204);
    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].timestamp, 1_700_000_000_000);
}

#[test]
fn extra_label_query_arg_is_appended_after_pushgateway_labels() {
    let ts = start_default_server();
    let resp = post(
        ts.server.local_addr(),
        "/api/v1/import/prometheus/metrics/job/backup?extra_label=env=prod",
        "",
        b"cpu 1\n",
    );
    assert_eq!(resp.status, 200);
    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].tags, tags(&[("job", "backup"), ("env", "prod")]));
}

#[test]
fn all_prometheus_import_prefixes_are_routed() {
    let ts = start_default_server();
    for path in [
        "/api/v1/import/prometheus",
        "/prometheus/api/v1/import/prometheus",
    ] {
        let resp = post(ts.server.local_addr(), path, "", b"m 1\n");
        assert_eq!(resp.status, 204, "unexpected status for {path}");
    }
    assert_eq!(ts.sink.take_rows().len(), 2);

    let resp = post(ts.server.local_addr(), "/notaroute", "", b"m 1\n");
    assert_eq!(resp.status, 404);
}

#[test]
fn doubled_slash_path_is_normalized_like_upstream() {
    // Go normalizes the path before all vminsert routing:
    // `path := strings.ReplaceAll(r.URL.Path, "//", "/")` in main.go. A
    // doubled leading slash must still route, still return the Pushgateway
    // 200, and still yield the job label.
    let ts = start_default_server();
    let resp = post(
        ts.server.local_addr(),
        "//api/v1/import/prometheus/metrics/job/x",
        "",
        b"cpu 5\n",
    );
    assert_eq!(resp.status, 200);
    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].tags, tags(&[("job", "x")]));
}
