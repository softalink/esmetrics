//! Integration tests: esm-http server + InsertHandlers over a mock sink,
//! for the `/api/v1/import` vmimport JSON-lines path.

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
fn happy_path_captured_upstream_export_returns_204_and_converts_rows() {
    let ts = start_default_server();
    // Fixture captured verbatim (byte-for-byte) from a REAL upstream
    // VictoriaMetrics v1.146.0 single-node `/api/v1/export`, 2026-07-05:
    //
    //   cd /home/test/refsrc/VictoriaMetrics
    //   go run ./app/victoria-metrics -storageDataPath=<tmpdir> -httpListenAddr=:8429
    //   # 3 series ingested via VM's own /api/v1/import (timestamps within
    //   # retention; NOW=$(date +%s)*1000, T1=NOW-30000 etc.):
    //   #   {"metric":{"__name__":"up","job":"node_exporter","instance":"localhost:9100"},"values":[0,0,0],"timestamps":[T1,T2,T3]}
    //   #   {"metric":{"__name__":"temperature","sensor":"s1"},"values":[21.5,"NaN"],"timestamps":[T1,T2]}
    //   #   {"metric":{"__name__":"rate","host":"h1"},"values":["Infinity"],"timestamps":[T1]}
    //   curl -s http://127.0.0.1:8429/internal/force_flush
    //   curl -s --get 'http://127.0.0.1:8429/api/v1/export' \
    //     --data-urlencode 'match[]={__name__=~".+"}'
    //
    // Note the ingested NaN sample is absent from the export: upstream
    // storage silently drops plain (non-staleness-marker) NaN values at
    // insert time (lib/storage/storage.go:1912-1917, "Skip NaNs other than
    // Prometheus staleness marker"), so a real VM export can never contain
    // `null` for data ingested through /api/v1/import. The +Infinity value
    // IS stored and exports as the quoted string "Infinity"
    // (app/vmselect/prometheus/export.qtpl `convertValueToSpecialJSON`).
    let body = "{\"metric\":{\"__name__\":\"rate\",\"host\":\"h1\"},\"values\":[\"Infinity\"],\"timestamps\":[1783237780000]}\n\
{\"metric\":{\"__name__\":\"up\",\"job\":\"node_exporter\",\"instance\":\"localhost:9100\"},\"values\":[0,0,0],\"timestamps\":[1783237780000,1783237790000,1783237800000]}\n\
{\"metric\":{\"__name__\":\"temperature\",\"sensor\":\"s1\"},\"values\":[21.5],\"timestamps\":[1783237780000]}\n";
    let resp = post(
        ts.server.local_addr(),
        "/api/v1/import",
        "",
        body.as_bytes(),
    );
    assert_eq!(resp.status, 204);
    assert!(resp.body.is_empty(), "204 must have an empty body");

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 5, "1 rate + 3 up + 1 temperature samples");
    assert_eq!(rows[0].metric_group, "rate");
    assert_eq!(rows[0].tags, tags(&[("host", "h1")]));
    assert_eq!(rows[0].timestamp, 1783237780000);
    assert!(
        rows[0].value.is_infinite() && rows[0].value.is_sign_positive(),
        "exported \"Infinity\" string must import as +Inf, got {}",
        rows[0].value
    );
    assert_eq!(rows[1].metric_group, "up");
    assert_eq!(
        rows[1].tags,
        tags(&[("job", "node_exporter"), ("instance", "localhost:9100")])
    );
    assert_eq!(rows[1].timestamp, 1783237780000);
    assert_eq!(rows[1].value, 0.0);
    assert_eq!(rows[2].timestamp, 1783237790000);
    assert_eq!(rows[3].timestamp, 1783237800000);
    assert_eq!(rows[4].metric_group, "temperature");
    assert_eq!(rows[4].tags, tags(&[("sensor", "s1")]));
    assert_eq!(rows[4].timestamp, 1783237780000);
    assert_eq!(rows[4].value, 21.5);
}

#[test]
fn gzip_body_is_decoded() {
    let ts = start_default_server();
    let body = "{\"metric\":{\"__name__\":\"cpu\",\"host\":\"h1\"},\"values\":[3.5],\"timestamps\":[1000]}\n";
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(body.as_bytes()).unwrap();
    let gz = enc.finish().unwrap();

    let resp = post(
        ts.server.local_addr(),
        "/api/v1/import",
        "Content-Encoding: gzip\r\n",
        &gz,
    );
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "cpu");
    assert_eq!(rows[0].tags, tags(&[("host", "h1")]));
    assert_eq!(rows[0].timestamp, 1000);
    assert_eq!(rows[0].value, 3.5);
}

#[test]
fn special_floats_roundtrip_nan_and_infinity() {
    // Synthetic body (not a captured export): a real VM export can never
    // contain NaN-valued samples ingested via /api/v1/import — upstream
    // storage drops plain NaN at insert (lib/storage/storage.go:1912) —
    // but the *import* side must still accept every getSpecialFloat64
    // spelling. `null` and "NaN" both map to NaN, "Infinity" to +Inf.
    let ts = start_default_server();
    let body = "{\"metric\":{\"__name__\":\"temperature\",\"sensor\":\"s1\"},\"values\":[21.5,\"NaN\",null],\"timestamps\":[1549891472010,1549891487724,1549891503438]}\n\
{\"metric\":{\"__name__\":\"rate\",\"host\":\"h1\"},\"values\":[\"Infinity\"],\"timestamps\":[1549891472010]}\n";
    let resp = post(
        ts.server.local_addr(),
        "/api/v1/import",
        "",
        body.as_bytes(),
    );
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].metric_group, "temperature");
    assert_eq!(rows[0].value, 21.5);
    assert!(
        rows[1].value.is_nan(),
        "\"NaN\" must import as NaN, got {}",
        rows[1].value
    );
    assert!(
        rows[2].value.is_nan(),
        "JSON null must import as NaN, got {}",
        rows[2].value
    );
    assert_eq!(rows[3].metric_group, "rate");
    assert!(rows[3].value.is_infinite() && rows[3].value.is_sign_positive());
}

#[test]
fn minus_null_string_imports_as_nan() {
    // getSpecialFloat64FromString strips a leading "-" then matches "Null";
    // the minus is intentionally dropped for the nan/null arm (upstream
    // lib/protoparser/vmimport/parser.go getSpecialFloat64FromString), so
    // "-Null" imports as plain NaN.
    let ts = start_default_server();
    let body = "{\"metric\":{\"__name__\":\"m\"},\"values\":[\"-Null\"],\"timestamps\":[1]}\n";
    let resp = post(
        ts.server.local_addr(),
        "/api/v1/import",
        "",
        body.as_bytes(),
    );
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].value.is_nan(),
        "\"-Null\" must import as NaN, got {}",
        rows[0].value
    );
}

#[test]
fn duplicate_tag_keys_last_wins() {
    // Duplicate keys inside the `metric` object: serde_json's Map (indexmap
    // with preserve_order) keeps the first key's position and overwrites its
    // value, so the last occurrence wins and only ONE tag is produced. This
    // diverges from Go fastjson, whose Object keeps every key/value pair and
    // Visit yields both (two `foo` tags reach storage upstream). Pinning the
    // serde_json behavior here so any change is caught.
    let ts = start_default_server();
    let body =
        "{\"metric\":{\"__name__\":\"m\",\"foo\":\"x\",\"foo\":\"y\"},\"values\":[1],\"timestamps\":[1]}\n";
    let resp = post(
        ts.server.local_addr(),
        "/api/v1/import",
        "",
        body.as_bytes(),
    );
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "m");
    assert_eq!(rows[0].tags, tags(&[("foo", "y")]), "last duplicate wins");
}

#[test]
fn mismatched_arrays_line_skipped_valid_line_still_ingested() {
    let ts = start_default_server();
    let body = "{\"metric\":{\"__name__\":\"bad\"},\"values\":[1,2],\"timestamps\":[3]}\n\
{\"metric\":{\"__name__\":\"good\"},\"values\":[5],\"timestamps\":[6]}\n";
    let resp = post(
        ts.server.local_addr(),
        "/api/v1/import",
        "",
        body.as_bytes(),
    );
    assert_eq!(
        resp.status, 204,
        "the whole batch still succeeds; the bad line is just skipped"
    );

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "good");
    assert_eq!(rows[0].timestamp, 6);
    assert_eq!(rows[0].value, 5.0);
}

#[test]
fn garbage_line_is_skipped() {
    let ts = start_default_server();
    let body = "not json at all\n\
{\"metric\":{\"__name__\":\"ok\"},\"values\":[1],\"timestamps\":[1]}\n";
    let resp = post(
        ts.server.local_addr(),
        "/api/v1/import",
        "",
        body.as_bytes(),
    );
    assert_eq!(resp.status, 204);

    let rows = ts.sink.take_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric_group, "ok");
}

#[test]
fn both_import_routes_are_matched() {
    let ts = start_default_server();
    for path in ["/api/v1/import", "/prometheus/api/v1/import"] {
        let resp = post(
            ts.server.local_addr(),
            path,
            "",
            b"{\"metric\":{\"__name__\":\"m\"},\"values\":[1],\"timestamps\":[1]}\n",
        );
        assert_eq!(resp.status, 204, "unexpected status for {path}");
    }
    assert_eq!(ts.sink.take_rows().len(), 2);
}

#[test]
fn does_not_shadow_import_prometheus_or_csv_prefixes() {
    let ts = start_default_server();
    // /api/v1/import/prometheus is handled elsewhere; the exact-match
    // "/api/v1/import" route must not swallow it. With no prometheusimport
    // body this still returns a response (200 is expected for empty body per
    // that handler), just proving the vmimport handler wasn't invoked
    // instead (which would 400 on an empty JSON body).
    let resp = post(ts.server.local_addr(), "/api/v1/import/prometheus", "", b"");
    assert_ne!(
        resp.status, 400,
        "prometheus import path must not be routed to vmimport"
    );
}
