//! Integration tests: esm-http server + InsertHandlers over a mock sink,
//! for the CSV import `/api/v1/import/csv` path.

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

/// The task brief's worked example: `format=1:label:device,2:metric:temperature,3:time:unix_s`.
const BRIEF_FORMAT: &str = "1:label:device,2:metric:temperature,3:time:unix_s";
const BRIEF_PATH: &str =
    "/api/v1/import/csv?format=1%3Alabel%3Adevice%2C2%3Ametric%3Atemperature%2C3%3Atime%3Aunix_s";

#[test]
fn happy_path_from_the_brief_returns_204_and_converts_rows() {
    let ts = start_default_server();
    let body = "sensor-1,23.5,1447116400\n";
    let resp = post(ts.server.local_addr(), BRIEF_PATH, "", body.as_bytes());
    assert_eq!(resp.status, 204);
    assert!(resp.body.is_empty(), "204 must have an empty body");

    let rows = ts.sink.take_rows();
    assert_eq!(
        rows,
        vec![GotRow {
            metric_group: "temperature".to_owned(),
            tags: tags(&[("device", "sensor-1")]),
            timestamp: 1_447_116_400_000,
            value: 23.5,
        }]
    );
}

#[test]
fn gzip_body_is_decoded() {
    let ts = start_default_server();
    let body = "sensor-2,18.0,1447116500\n";
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(body.as_bytes()).unwrap();
    let gz = enc.finish().unwrap();

    let path = format!("/api/v1/import/csv?format={}", urlencode(BRIEF_FORMAT));
    let resp = post(
        ts.server.local_addr(),
        &path,
        "Content-Encoding: gzip\r\n",
        &gz,
    );
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "temperature");
    assert_eq!(rows[0].tags, tags(&[("device", "sensor-2")]));
    assert_eq!(rows[0].timestamp, 1_447_116_500_000);
    assert_eq!(rows[0].value, 18.0);
}

#[test]
fn missing_format_query_arg_is_400() {
    let ts = start_default_server();
    let resp = post(ts.server.local_addr(), "/api/v1/import/csv", "", b"1,2,3\n");
    assert_eq!(resp.status, 400);
    assert!(!resp.body.is_empty(), "400 must carry an error message");
    assert!(ts.sink.take_rows().is_empty());
}

#[test]
fn invalid_format_query_arg_is_400() {
    let ts = start_default_server();
    // No `metric` column in the format -> ParseColumnDescriptors error.
    let resp = post(
        ts.server.local_addr(),
        "/api/v1/import/csv?format=1%3Alabel%3Afoo",
        "",
        b"bar\n",
    );
    assert_eq!(resp.status, 400);
    assert!(ts.sink.take_rows().is_empty());
}

#[test]
fn header_row_is_autodetected() {
    let ts = start_default_server();
    let path = format!("/api/v1/import/csv?format={}", urlencode(BRIEF_FORMAT));
    let body = "device,value,timestamp\nsensor-1,23.5,1447116400\n";
    let resp = post(ts.server.local_addr(), &path, "", body.as_bytes());
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(
        rows.len(),
        1,
        "the non-numeric header row must be skipped, not ingested"
    );
    assert_eq!(rows[0].metric_group, "temperature");
    assert_eq!(rows[0].tags, tags(&[("device", "sensor-1")]));
    assert_eq!(rows[0].timestamp, 1_447_116_400_000);
}

#[test]
fn quoted_field_with_comma_is_parsed_as_one_column() {
    let ts = start_default_server();
    let path = format!("/api/v1/import/csv?format={}", urlencode(BRIEF_FORMAT));
    let body = "\"Springfield, IL\",23.5,1447116400\n";
    let resp = post(ts.server.local_addr(), &path, "", body.as_bytes());
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].tags, tags(&[("device", "Springfield, IL")]));
    assert_eq!(rows[0].value, 23.5);
}

#[test]
fn missing_timestamp_column_defaults_to_now() {
    let ts = start_default_server();
    let path = "/api/v1/import/csv?format=1%3Ametric%3Afoo";
    let before = now_millis();
    let resp = post(ts.server.local_addr(), path, "", b"42\n");
    let after = now_millis();
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "foo");
    assert_eq!(rows[0].value, 42.0);
    assert!(
        rows[0].timestamp >= before && rows[0].timestamp <= after,
        "expected timestamp in [{before}, {after}], got {}",
        rows[0].timestamp
    );
}

#[test]
fn extra_label_query_arg_is_appended_after_row_labels() {
    let ts = start_default_server();
    let path = format!(
        "/api/v1/import/csv?format={}&extra_label=env=prod",
        urlencode(BRIEF_FORMAT)
    );
    let resp = post(
        ts.server.local_addr(),
        &path,
        "",
        b"sensor-1,23.5,1447116400\n",
    );
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].tags,
        tags(&[("device", "sensor-1"), ("env", "prod")])
    );
}

#[test]
fn multiple_metric_columns_share_tags_and_timestamp() {
    let ts = start_default_server();
    let path = "/api/v1/import/csv?format=1%3Alabel%3Asymbol%2C2%3Ametric%3Abid%2C3%3Ametric%3Aask%2C4%3Atime%3Aunix_s";
    let resp = post(
        ts.server.local_addr(),
        path,
        "",
        b"AUDCAD,0.9725,0.97273,1000\n",
    );
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].metric_group, "bid");
    assert_eq!(rows[0].tags, tags(&[("symbol", "AUDCAD")]));
    assert_eq!(rows[0].value, 0.9725);
    assert_eq!(rows[0].timestamp, 1_000_000);
    assert_eq!(rows[1].metric_group, "ask");
    assert_eq!(rows[1].tags, tags(&[("symbol", "AUDCAD")]));
    assert_eq!(rows[1].value, 0.97273);
    assert_eq!(rows[1].timestamp, 1_000_000);
}

#[test]
fn invalid_rows_are_skipped_others_kept() {
    let ts = start_default_server();
    let path = "/api/v1/import/csv?format=1%3Ametric%3Afoo";
    let body = "1\ngarbage-not-a-number\n2\n";
    let resp = post(ts.server.local_addr(), path, "", body.as_bytes());
    assert_eq!(
        resp.status, 204,
        "the whole batch still succeeds; the bad line is skipped"
    );

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].value, 1.0);
    assert_eq!(rows[1].value, 2.0);
}

#[test]
fn both_csv_import_routes_are_matched() {
    let ts = start_default_server();
    for path in ["/api/v1/import/csv", "/prometheus/api/v1/import/csv"] {
        let full_path = format!("{path}?format={}", urlencode(BRIEF_FORMAT));
        let resp = post(ts.server.local_addr(), &full_path, "", b"sensor-1,1.0,1\n");
        assert_eq!(resp.status, 204, "unexpected status for {path}");
    }
    assert_eq!(ts.sink.take_rows().len(), 2);
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Minimal percent-encoder for the small set of characters this test file's
/// `format=` query values use (`:`, `,`, `=`).
fn urlencode(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            ':' => "%3A".to_owned(),
            ',' => "%2C".to_owned(),
            '=' => "%3D".to_owned(),
            other => other.to_string(),
        })
        .collect()
}
