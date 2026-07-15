//! Integration test: start the server on an ephemeral port, hit the skeleton
//! endpoints over a plain TcpStream, then stop it.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use esmetrics::flags::Flags;

fn test_flags() -> Flags {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "esmetrics-server-test-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    Flags {
        http_listen_addr: "127.0.0.1:0".to_string(),
        storage_data_path: dir.to_string_lossy().into_owned(),
        ..Flags::default()
    }
}

/// Sends `GET <target>` with `Connection: close` and returns
/// (status line, body). Dechunks the body when the response uses
/// `Transfer-Encoding: chunked` (e.g. `/api/v1/export`'s streamed output).
fn http_get(addr: SocketAddr, target: &str) -> (String, String) {
    let mut stream = TcpStream::connect(addr).expect("connect failed");
    write!(
        stream,
        "GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .expect("write failed");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read failed");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed response: {response:?}"));
    let status_line = head.lines().next().unwrap_or_default().to_string();
    let chunked = head
        .lines()
        .any(|l| l.eq_ignore_ascii_case("transfer-encoding: chunked"));
    let body = if chunked {
        dechunk(body)
    } else {
        body.to_string()
    };
    (status_line, body)
}

/// Like [`http_get`], but also returns the raw header block so tests can
/// check headers `http_get` doesn't expose (e.g. `Location`,
/// `Content-Type`, `Cache-Control`).
fn http_get_with_headers(addr: SocketAddr, target: &str) -> (String, String, String) {
    let mut stream = TcpStream::connect(addr).expect("connect failed");
    write!(
        stream,
        "GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .expect("write failed");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read failed");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed response: {response:?}"));
    let status_line = head.lines().next().unwrap_or_default().to_string();
    let chunked = head
        .lines()
        .any(|l| l.eq_ignore_ascii_case("transfer-encoding: chunked"));
    let body = if chunked {
        dechunk(body)
    } else {
        body.to_string()
    };
    (status_line, head.to_string(), body)
}

/// Sends `POST <target>` with `Connection: close` and the given body, and
/// returns (status line, body).
fn http_post(addr: SocketAddr, target: &str, body: &[u8]) -> (String, String) {
    let mut stream = TcpStream::connect(addr).expect("connect failed");
    write!(
        stream,
        "POST {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Length: {}\r\n\r\n",
        body.len()
    )
    .expect("write failed");
    stream.write_all(body).expect("write body failed");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read failed");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed response: {response:?}"));
    let status_line = head.lines().next().unwrap_or_default().to_string();
    (status_line, body.to_string())
}

/// Decodes an HTTP/1.1 chunked-transfer-encoded body.
fn dechunk(mut body: &str) -> String {
    let mut out = String::new();
    while let Some((size_line, rest)) = body.split_once("\r\n") {
        let size = usize::from_str_radix(size_line.trim(), 16).expect("chunk size");
        if size == 0 {
            break;
        }
        out.push_str(&rest[..size]);
        body = &rest[size + 2..]; // skip chunk data + trailing CRLF
    }
    out
}

/// Ingests a couple of influx rows via `/write`, then checks that `/metrics`
/// reports at least that many rows for
/// `esm_rows_inserted_total{type="influx"}`. The counter is process-global
/// (shared across every test in this binary), so this only proves the
/// wiring works end to end — it asserts a lower bound, not an exact value.
#[test]
fn metrics_endpoint_reports_influx_rows_inserted() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let addr = server.local_addr();

    let (status, _) = http_post(
        addr,
        "/write",
        b"e2e.metrics.influx,host=h1 value=1 100\ne2e.metrics.influx,host=h1 value=2 200\n",
    );
    assert_eq!(status, "HTTP/1.1 204 No Content");

    let (status, body) = http_get(addr, "/metrics");
    assert_eq!(status, "HTTP/1.1 200 OK");

    let value: u64 = body
        .lines()
        .find_map(|line| line.strip_prefix("esm_rows_inserted_total{type=\"influx\"} "))
        .unwrap_or_else(|| panic!("counter line missing from /metrics body: {body:?}"))
        .trim()
        .parse()
        .expect("counter value must be a valid u64");
    assert!(
        value >= 2,
        "expected at least 2 influx rows inserted, got {value}"
    );

    server.stop();
}

#[test]
fn health_endpoint_returns_200_ok() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let (status, body) = http_get(server.local_addr(), "/health");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "OK");
    server.stop();
}

#[test]
fn root_returns_short_text() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let (status, body) = http_get(server.local_addr(), "/");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(body.contains("EsMetrics"), "body: {body:?}");
    server.stop();
}

#[test]
fn unknown_path_returns_esm_style_404() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let (status, body) = http_get(server.local_addr(), "/no/such/path");
    assert_eq!(status, "HTTP/1.1 404 Not Found");
    assert_eq!(body, "unsupported path requested: \"/no/such/path\"\n");
    server.stop();
}

#[test]
fn go_style_listen_addr_binds_all_interfaces() {
    let flags = Flags {
        http_listen_addr: ":0".to_string(),
        ..Flags::default()
    };
    let server = esmetrics::run(&flags).expect("run failed with Go-style addr");
    assert_ne!(server.local_addr().port(), 0);
    let addr = SocketAddr::from(([127, 0, 0, 1], server.local_addr().port()));
    let (status, _) = http_get(addr, "/health");
    assert_eq!(status, "HTTP/1.1 200 OK");
    server.stop();
}

#[test]
fn stop_shuts_the_listener_down() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let addr = server.local_addr();
    server.stop();
    assert!(
        TcpStream::connect(addr).is_err(),
        "listener still accepting after stop"
    );
}

#[test]
fn snapshot_endpoints_roundtrip() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let addr = server.local_addr();

    let (status, body) = http_get(addr, "/snapshot/create");
    assert_eq!(status, "HTTP/1.1 200 OK");
    let name = body
        .strip_prefix("{\"status\":\"ok\",\"snapshot\":\"")
        .and_then(|s| s.strip_suffix("\"}"))
        .unwrap_or_else(|| panic!("unexpected create body: {body:?}"))
        .to_string();

    let (status, body) = http_get(addr, "/snapshot/list");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(
        body,
        format!("{{\"status\":\"ok\",\"snapshots\":[\"{name}\"]}}")
    );

    let (status, body) = http_get(addr, &format!("/snapshot/delete?snapshot={name}"));
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "{\"status\":\"ok\"}");

    // Deleting again → error JSON with 500.
    let (status, body) = http_get(addr, &format!("/snapshot/delete?snapshot={name}"));
    assert_eq!(status, "HTTP/1.1 500 Internal Server Error");
    assert!(body.starts_with("{\"status\":\"error\""), "body: {body:?}");

    // create + delete_all
    http_get(addr, "/snapshot/create");
    http_get(addr, "/snapshot/create");
    let (status, body) = http_get(addr, "/snapshot/delete_all");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "{\"status\":\"ok\"}");
    let (_, body) = http_get(addr, "/snapshot/list");
    assert_eq!(body, "{\"status\":\"ok\",\"snapshots\":[]}");

    server.stop();
}

#[test]
fn prometheus_admin_snapshot_alias() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let (status, body) = http_get(server.local_addr(), "/api/v1/admin/tsdb/snapshot");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(
        body.starts_with("{\"status\":\"success\",\"data\":{\"name\":\""),
        "body: {body:?}"
    );
    server.stop();
}

/// Forces newly ingested rows visible to search, then polls
/// `GET <target>` until `body_contains` matches or `timeout` elapses.
/// TCP/UDP ingestion lands on a background thread, so both the flush and
/// the query need to be retried until the write has actually landed.
///
/// `/internal/force_flush` alone makes new rows AND their tag-index
/// entries searchable (Storage::force_flush flushes the data table and
/// each partition indexDB; verified deterministically — see
/// esm-storage/tests/storage_test.rs `flush_only_makes_new_series_searchable`).
/// No `/internal/force_merge` is needed. If an ingest-then-export test
/// misses data, check the sample TIMESTAMPS first: export's default `end`
/// is the request-time clock, so samples stamped at/after "now" are
/// correctly out of range, and `/api/v1/query` additionally applies the
/// upstream `-search.latencyOffset` (30s) to its default evaluation time.
fn wait_for_body(addr: SocketAddr, target: &str, body_contains: &str, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    loop {
        http_get(addr, "/internal/force_flush");
        let (_, body) = http_get(addr, target);
        if body.contains(body_contains) || Instant::now() >= deadline {
            return body;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn now_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[test]
fn graphite_flag_wires_a_tcp_udp_listener_that_reaches_storage() {
    let mut flags = test_flags();
    flags.graphite_listen_addr = "127.0.0.1:0".to_string();
    let server = esmetrics::run(&flags).expect("run failed");
    let graphite_addr = server.graphite_addr().expect("graphite server enabled");
    assert!(server.opentsdb_addr().is_none());

    let ts = now_unix_secs();
    let mut stream = TcpStream::connect(graphite_addr).expect("graphite tcp connect");
    writeln!(stream, "e2e.graphite.tcp;env=test 11 {ts}").unwrap();
    drop(stream);

    let udp = UdpSocket::bind("127.0.0.1:0").unwrap();
    udp.send_to(
        format!("e2e.graphite.udp 22 {ts}\n").as_bytes(),
        graphite_addr,
    )
    .unwrap();

    let body = wait_for_body(
        server.local_addr(),
        "/api/v1/export?match%5B%5D=%7B__name__%3D%22e2e.graphite.tcp%22%7D",
        "e2e.graphite.tcp",
        Duration::from_secs(5),
    );
    assert!(body.contains("\"env\":\"test\""), "body: {body}");
    assert!(body.contains("\"values\":[11]"), "body: {body}");

    let body = wait_for_body(
        server.local_addr(),
        "/api/v1/export?match%5B%5D=%7B__name__%3D%22e2e.graphite.udp%22%7D",
        "e2e.graphite.udp",
        Duration::from_secs(5),
    );
    assert!(body.contains("\"values\":[22]"), "body: {body}");

    server.stop();
}

#[test]
fn opentsdb_flag_wires_a_tcp_udp_telnet_listener_that_reaches_storage() {
    let mut flags = test_flags();
    flags.opentsdb_listen_addr = "127.0.0.1:0".to_string();
    let server = esmetrics::run(&flags).expect("run failed");
    let opentsdb_addr = server.opentsdb_addr().expect("opentsdb server enabled");
    assert!(server.graphite_addr().is_none());

    let ts = now_unix_secs();
    let mut stream = TcpStream::connect(opentsdb_addr).expect("opentsdb tcp connect");
    writeln!(stream, "put e2e.opentsdb.tcp {ts} 33 host=h1").unwrap();
    drop(stream);

    let udp = UdpSocket::bind("127.0.0.1:0").unwrap();
    udp.send_to(
        format!("put e2e.opentsdb.udp {ts} 44 host=h2\n").as_bytes(),
        opentsdb_addr,
    )
    .unwrap();

    let body = wait_for_body(
        server.local_addr(),
        "/api/v1/export?match%5B%5D=%7B__name__%3D%22e2e.opentsdb.tcp%22%7D",
        "e2e.opentsdb.tcp",
        Duration::from_secs(5),
    );
    assert!(body.contains("\"host\":\"h1\""), "body: {body}");
    assert!(body.contains("\"values\":[33]"), "body: {body}");

    let body = wait_for_body(
        server.local_addr(),
        "/api/v1/export?match%5B%5D=%7B__name__%3D%22e2e.opentsdb.udp%22%7D",
        "e2e.opentsdb.udp",
        Duration::from_secs(5),
    );
    assert!(body.contains("\"values\":[44]"), "body: {body}");

    server.stop();
}

#[test]
fn opentsdb_http_flag_wires_a_dedicated_listener_that_reaches_storage() {
    let mut flags = test_flags();
    flags.opentsdb_http_listen_addr = "127.0.0.1:0".to_string();
    let server = esmetrics::run(&flags).expect("run failed");
    let opentsdbhttp_addr = server
        .opentsdb_http_addr()
        .expect("opentsdbhttp server enabled");
    assert!(server.graphite_addr().is_none());
    assert!(server.opentsdb_addr().is_none());
    // The dedicated listener is a genuinely separate port from the main
    // HTTP server, not just an alias.
    assert_ne!(opentsdbhttp_addr.port(), server.local_addr().port());

    let ts = now_unix_secs();
    let (status, body) = http_post(
        opentsdbhttp_addr,
        "/api/put",
        format!(
            "{{\"metric\":\"e2e.opentsdbhttp.put\",\"timestamp\":{ts},\"value\":55,\"tags\":{{\"host\":\"h1\"}}}}"
        )
        .as_bytes(),
    );
    assert_eq!(status, "HTTP/1.1 204 No Content", "body: {body}");

    let body = wait_for_body(
        server.local_addr(),
        "/api/v1/export?match%5B%5D=%7B__name__%3D%22e2e.opentsdbhttp.put%22%7D",
        "e2e.opentsdbhttp.put",
        Duration::from_secs(5),
    );
    assert!(body.contains("\"host\":\"h1\""), "body: {body}");
    assert!(body.contains("\"values\":[55]"), "body: {body}");

    server.stop();
}

#[test]
fn opentsdb_http_alias_path_also_matched() {
    let mut flags = test_flags();
    flags.opentsdb_http_listen_addr = "127.0.0.1:0".to_string();
    let server = esmetrics::run(&flags).expect("run failed");
    let opentsdbhttp_addr = server
        .opentsdb_http_addr()
        .expect("opentsdbhttp server enabled");

    let ts = now_unix_secs();
    let (status, body) = http_post(
        opentsdbhttp_addr,
        "/opentsdb/api/put",
        format!("{{\"metric\":\"e2e.opentsdbhttp.alias\",\"timestamp\":{ts},\"value\":66}}")
            .as_bytes(),
    );
    assert_eq!(status, "HTTP/1.1 204 No Content", "body: {body}");

    server.stop();
}

#[test]
fn main_http_port_does_not_serve_api_put() {
    // Upstream's main vminsert `RequestHandler` has no `/api/put` case at
    // all (verified against `app/vminsert/main.go`); `/api/put` is only
    // ever served on the dedicated `-opentsdbHTTPListenAddr` listener. Even
    // with that flag enabled, the *main* port must still 404 on `/api/put`.
    let mut flags = test_flags();
    flags.opentsdb_http_listen_addr = "127.0.0.1:0".to_string();
    let server = esmetrics::run(&flags).expect("run failed");

    let (status, body) = http_post(server.local_addr(), "/api/put", b"{}");
    assert_eq!(status, "HTTP/1.1 404 Not Found", "body: {body}");

    server.stop();
}

#[test]
fn opentsdb_http_disabled_by_default() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    assert!(server.opentsdb_http_addr().is_none());
    server.stop();
}

#[test]
fn snapshot_auth_key_enforced() {
    let mut flags = test_flags();
    flags.snapshot_auth_key = "sekret".to_string();
    let server = esmetrics::run(&flags).expect("run failed");
    let addr = server.local_addr();

    let (status, _) = http_get(addr, "/snapshot/list");
    assert_eq!(status, "HTTP/1.1 401 Unauthorized");
    let (status, _) = http_get(addr, "/snapshot/list?authKey=wrong");
    assert_eq!(status, "HTTP/1.1 401 Unauthorized");
    let (status, _) = http_get(addr, "/snapshot/list?authKey=sekret");
    assert_eq!(status, "HTTP/1.1 200 OK");

    server.stop();
}

// The router matches Go's `r.URL.Path` semantics: paths are percent-decoded
// before routing (%2F becomes '/'), and invalid escapes are a 400 at the
// HTTP-parse layer like Go's net/http.
#[test]
fn router_matches_percent_decoded_paths() {
    let flags = test_flags();
    let server = esmetrics::run(&flags).expect("run failed");

    let (status, body) = http_get(server.local_addr(), "/he%61lth");
    assert_eq!(status, "HTTP/1.1 200 OK", "body: {body}");
    assert_eq!(body, "OK");

    let (status, _) = http_get(server.local_addr(), "/bad%zzpath");
    assert_eq!(status, "HTTP/1.1 400 Bad Request");

    server.stop();
}

// -- vmui (vendored VictoriaMetrics web UI) -------------------------------

#[test]
fn esmui_index_serves_html_with_esmetrics_marker() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let (status, head, body) = http_get_with_headers(server.local_addr(), "/esmui/");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(
        head.to_ascii_lowercase()
            .contains("content-type: text/html"),
        "head: {head:?}"
    );
    // vmui's built index.html always references its own (rebranded) <title>.
    assert!(body.contains("EsMetrics"), "body: {body:?}");
    server.stop();
}

#[test]
fn esmui_without_trailing_slash_redirects() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let (status, head, _) = http_get_with_headers(server.local_addr(), "/esmui");
    assert_eq!(status, "HTTP/1.1 302 Found");
    assert!(
        head.to_ascii_lowercase().contains("location: esmui/"),
        "head: {head:?}"
    );
    server.stop();
}

#[test]
fn esmui_hashed_asset_serves_with_expected_content_type_and_cache_control() {
    let server = esmetrics::run(&test_flags()).expect("run failed");

    // Find a real hashed JS asset path by reading it out of the vendored
    // index.html, rather than hardcoding a hash that will drift on rebuild.
    let (_, index_body) = http_get(server.local_addr(), "/esmui/");
    let js_path = index_body
        .match_indices("assets/")
        .find_map(|(i, _)| {
            let rest = &index_body[i..];
            let end = rest.find('"')?;
            let candidate = &rest[..end];
            candidate.ends_with(".js").then(|| candidate.to_string())
        })
        .unwrap_or_else(|| panic!("no assets/*.js reference found in index.html: {index_body:?}"));

    let (status, head, body) =
        http_get_with_headers(server.local_addr(), &format!("/esmui/{js_path}"));
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(!body.is_empty());
    let head_lower = head.to_ascii_lowercase();
    assert!(
        head_lower.contains("content-type: text/javascript"),
        "head: {head:?}"
    );
    assert!(
        head_lower.contains("cache-control: max-age=3600"),
        "head: {head:?}"
    );

    server.stop();
}

#[test]
fn esmui_unknown_subpath_falls_back_to_index_html() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let (status, head, body) =
        http_get_with_headers(server.local_addr(), "/esmui/some/client-routed/path");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(
        head.to_ascii_lowercase()
            .contains("content-type: text/html"),
        "head: {head:?}"
    );
    assert!(body.contains("EsMetrics"), "body: {body:?}");
    server.stop();
}

#[test]
fn esmui_index_html_is_no_cache() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let (_, head, _) = http_get_with_headers(server.local_addr(), "/esmui/");
    assert!(
        head.to_ascii_lowercase()
            .contains("cache-control: no-cache"),
        "head: {head:?}"
    );
    server.stop();
}

#[test]
fn root_mentions_esmui() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let (status, body) = http_get(server.local_addr(), "/");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(body.contains("/esmui/"), "body: {body:?}");
    server.stop();
}

// vmui computes its API base as `<origin>/prometheus` and fetches
// `/prometheus/vmui/config.json` etc.; upstream serves the whole vmui
// tree under the /prometheus cluster-compat prefix too (vmselect's
// RequestHandler strips it before routing).
#[test]
fn esmui_tree_is_also_served_under_prometheus_prefix() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let addr = server.local_addr();

    let (status, head, body) = http_get_with_headers(addr, "/prometheus/esmui/config.json");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(
        head.to_ascii_lowercase()
            .contains("content-type: application/json"),
        "head: {head:?}"
    );
    assert!(body.contains("license"), "body: {body:?}");

    // A prefixed static asset (resolved out of the served index.html, no
    // hardcoded hash).
    let (_, index_body) = http_get(addr, "/prometheus/esmui/");
    let js_path = index_body
        .match_indices("assets/")
        .find_map(|(i, _)| {
            let rest = &index_body[i..];
            let end = rest.find('"')?;
            let candidate = &rest[..end];
            candidate.ends_with(".js").then(|| candidate.to_string())
        })
        .unwrap_or_else(|| panic!("no assets/*.js reference found in index.html: {index_body:?}"));
    let (status, head, body) = http_get_with_headers(addr, &format!("/prometheus/esmui/{js_path}"));
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(!body.is_empty());
    assert!(
        head.to_ascii_lowercase()
            .contains("content-type: text/javascript"),
        "head: {head:?}"
    );

    // Prefixed no-trailing-slash form redirects too (relative Location,
    // so the browser resolves it under /prometheus/). The legacy /vmui
    // form lands on the branded path.
    let (status, head, _) = http_get_with_headers(addr, "/prometheus/vmui");
    assert_eq!(status, "HTTP/1.1 302 Found");
    assert!(
        head.to_ascii_lowercase().contains("location: esmui/"),
        "head: {head:?}"
    );

    server.stop();
}

// Rebrand compatibility: the legacy /vmui tree 302-redirects to /esmui,
// including the three URLs the vendored app fetches with a hardcoded
// /vmui/ segment (fetch follows redirects).
#[test]
fn legacy_vmui_tree_redirects_to_esmui() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let addr = server.local_addr();

    // Page URL: /vmui → esmui/ (query preserved).
    let (status, head, _) = http_get_with_headers(addr, "/vmui?g0.expr=up");
    assert_eq!(status, "HTTP/1.1 302 Found");
    assert!(
        head.to_ascii_lowercase()
            .contains("location: esmui/?g0.expr=up"),
        "head: {head:?}"
    );

    // Subtree: /vmui/<rest> → ../esmui/<rest>; following it manually must
    // serve the real content.
    let (status, head, _) = http_get_with_headers(addr, "/vmui/timezone");
    assert_eq!(status, "HTTP/1.1 302 Found");
    assert!(
        head.to_ascii_lowercase()
            .contains("location: ../esmui/timezone"),
        "head: {head:?}"
    );
    let (status, body) = http_get(addr, "/esmui/timezone");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "{\"timezone\": \"UTC\"}");

    // Prefixed subtree resolves under /prometheus/ via the relative
    // Location: /prometheus/vmui/config.json → /prometheus/esmui/config.json.
    let (status, head, _) = http_get_with_headers(addr, "/prometheus/vmui/config.json");
    assert_eq!(status, "HTTP/1.1 302 Found");
    assert!(
        head.to_ascii_lowercase()
            .contains("location: ../esmui/config.json"),
        "head: {head:?}"
    );

    server.stop();
}

// Upstream app/vmselect/vmui.go handleVMUICustomDashboards with
// -vmui.customDashboardsPath unset: exact body match.
#[test]
fn esmui_custom_dashboards_returns_upstream_default_body() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let addr = server.local_addr();

    for target in [
        "/esmui/custom-dashboards",
        "/prometheus/esmui/custom-dashboards",
    ] {
        let (status, head, body) = http_get_with_headers(addr, target);
        assert_eq!(status, "HTTP/1.1 200 OK", "target: {target}");
        assert!(
            head.to_ascii_lowercase()
                .contains("content-type: application/json"),
            "target: {target}, head: {head:?}"
        );
        assert_eq!(body, r#"{"dashboardsSettings": []}"#, "target: {target}");
    }

    server.stop();
}

// Upstream app/vmselect/vmui.go handleVMUITimezone with
// -vmui.defaultTimezone unset: time.LoadLocation("") is UTC, so the
// upstream default body is {"timezone": "UTC"}. Upstream also wraps the
// handler in httpserver.EnableCORS (wildcard CORS headers).
#[test]
fn esmui_timezone_returns_upstream_default_body_with_cors() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let addr = server.local_addr();

    for target in ["/esmui/timezone", "/prometheus/esmui/timezone"] {
        let (status, head, body) = http_get_with_headers(addr, target);
        assert_eq!(status, "HTTP/1.1 200 OK", "target: {target}");
        let head_lower = head.to_ascii_lowercase();
        assert!(
            head_lower.contains("content-type: application/json"),
            "target: {target}, head: {head:?}"
        );
        assert!(
            head_lower.contains("access-control-allow-origin: *"),
            "target: {target}, head: {head:?}"
        );
        assert_eq!(body, r#"{"timezone": "UTC"}"#, "target: {target}");
    }

    server.stop();
}

// Upstream main.go: `/graph` redirects like `/vmui` and `/graph/...` is
// rewritten to `/vmui/...` (used by Grafana's Prometheus datasource).
#[test]
fn graph_alias_redirects_and_rewrites_to_esmui() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let addr = server.local_addr();

    let (status, head, _) = http_get_with_headers(addr, "/graph");
    assert_eq!(status, "HTTP/1.1 302 Found");
    assert!(
        head.to_ascii_lowercase().contains("location: graph/"),
        "head: {head:?}"
    );

    // Query string survives the redirect (upstream appends r.Form).
    let (status, head, _) = http_get_with_headers(addr, "/graph?g0.expr=up");
    assert_eq!(status, "HTTP/1.1 302 Found");
    assert!(
        head.to_ascii_lowercase()
            .contains("location: graph/?g0.expr=up"),
        "head: {head:?}"
    );

    let (status, head, body) = http_get_with_headers(addr, "/graph/");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(
        head.to_ascii_lowercase()
            .contains("content-type: text/html"),
        "head: {head:?}"
    );
    assert!(body.contains("EsMetrics"), "body: {body:?}");

    // Prefixed form too.
    let (status, _, _) = http_get_with_headers(addr, "/prometheus/graph/");
    assert_eq!(status, "HTTP/1.1 200 OK");

    server.stop();
}

// Upstream collapses doubled slashes before UI routing; a doubled-slash
// asset URL must serve the asset, not fall back to index.html.
#[test]
fn esmui_doubled_slash_asset_path_still_serves_the_asset() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let addr = server.local_addr();

    // Discover a real hashed asset path from the served index.html.
    let (_, index) = http_get(addr, "/esmui/");
    let js = index
        .split('"')
        .find(|s| s.starts_with("./assets/") && s.ends_with(".js"))
        .expect("index.html must reference a JS bundle")
        .trim_start_matches("./")
        .to_string();

    let (status, head_body) = http_get(addr, &format!("/esmui//{js}"));
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(
        !head_body.contains("<!doctype html>") && !head_body.contains("<!DOCTYPE html>"),
        "doubled-slash asset URL fell back to index.html"
    );

    server.stop();
}

/// End-to-end single-node stream aggregation (`-streamAggr.config`): a
/// `sum_samples` aggregation over a 1s interval matching every series.
/// Ingesting two samples of `foo` (influx measurement+field → `foo_value`)
/// must store the aggregated `foo_value:1s_sum_samples`, whose first flush
/// carries the interval sum (3); and — since `keepInput` is off — must NOT
/// store the raw `foo_value`.
#[test]
fn stream_aggregation_writes_aggregated_series() {
    use std::time::{Duration, Instant};

    let mut flags = test_flags();
    let cfg = std::env::temp_dir().join(format!("esmetrics-streamaggr-{}.yml", std::process::id()));
    std::fs::write(
        &cfg,
        "- interval: 1s\n  staleness_interval: 1h\n  outputs: [sum_samples]\n",
    )
    .expect("write streamAggr config");
    flags.stream_aggr_config = Some(cfg.to_string_lossy().into_owned());

    let server = esmetrics::run(&flags).expect("run failed");
    let addr = server.local_addr();

    let (status, _) = http_post(addr, "/write", b"foo value=1\nfoo value=2\n");
    assert_eq!(status, "HTTP/1.1 204 No Content");

    // Poll a range query until the aggregated series appears with its
    // first-interval sum (3). `sum_samples` resets each interval, so an
    // instant query would return a later 0; a range query catches the 3.
    let deadline = Instant::now() + Duration::from_secs(12);
    let mut found = false;
    let mut last = String::new();
    while Instant::now() < deadline {
        let (status, body) = http_get(addr, "/api/v1/export?match[]=foo_value:1s_sum_samples");
        last = body.clone();
        if status == "HTTP/1.1 200 OK"
            && body.contains("foo_value:1s_sum_samples")
            && body.contains("3")
        {
            found = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    if !found {
        let (_, names) = http_get(addr, "/api/v1/label/__name__/values");
        panic!(
            "aggregated series never stored.\n  last export: {last}\n  __name__ values: {names}"
        );
    }

    // The raw input must have been consumed by the aggregator (keepInput off).
    let (_, raw_body) = http_get(addr, "/api/v1/export?match[]=foo_value");
    assert!(
        !raw_body.contains("\"__name__\":\"foo_value\""),
        "raw input `foo_value` should not be stored when aggregated: {raw_body}"
    );

    server.stop();
    let _ = std::fs::remove_file(&cfg);
}
