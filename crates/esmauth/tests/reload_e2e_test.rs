//! End-to-end tests for the esmauth binary's config hot-reload and its
//! proxying in front of *real* backends.
//!
//! Mirrors `esmauth/tests/proxy_test.rs`'s style (mock-backend + temp
//! config-file pattern) and `esmetrics/tests/server_test.rs`'s raw-TCP HTTP
//! client helpers, but this file is a separate integration-test binary, so
//! the small set of helpers it needs are duplicated locally rather than
//! shared.
//!
//! Test 1 (`hot_reload_...`) drives `esmauth::run`'s `/-/reload` endpoint:
//! rewrite `-auth.config` on disk, reload, and observe the next request
//! routing to the new backend; then prove a syntactically broken config is
//! rejected and the last-good config keeps serving.
//!
//! Test 2 (`two_backend_proxy_...`) starts two real, separate `esmetrics`
//! instances (no shared storage) and proves data ingested through esmauth's
//! proxy for one is queryable back through esmauth's proxy. The upstream
//! `/api/v1/write` endpoint in this codebase is Prometheus remote write
//! (protobuf/snappy) — not influx line protocol — so this test proxies the
//! plain-text influx endpoint (`/write`) instead; that is the write path
//! that actually accepts line-protocol bodies, and it exercises the same
//! url_map + least_loaded routing the brief calls for. See the docstring on
//! that test for the full robustness argument (deterministic round-robin
//! alternation across sequential requests, so both backends are guaranteed
//! to receive writes without any timing assumptions).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use esm_http::{Request, ResponseWriter, Server as MockServer};
use esmauth::flags::Flags;

// -- config / flags plumbing (mirrors proxy_test.rs) ------------------------

fn write_config(yaml: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "esmauth-reload-e2e-test-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("create temp config dir");
    let path = dir.join("auth.yml");
    std::fs::write(&path, yaml).expect("write auth config");
    path
}

fn test_flags(config_path: &std::path::Path) -> Flags {
    Flags {
        auth_config: config_path.to_string_lossy().into_owned(),
        http_listen_addr: "127.0.0.1:0".to_string(),
        ..Flags::default()
    }
}

/// Starts esmauth against `yaml`, returning both the running app and the
/// config file path so the test can rewrite it in place and trigger reload.
fn start_esmauth(yaml: &str) -> (esmauth::App, PathBuf) {
    let path = write_config(yaml);
    let app = esmauth::run(&test_flags(&path)).expect("esmauth run failed");
    (app, path)
}

// -- mock backends (mirrors proxy_test.rs) ----------------------------------

struct MockBackend {
    server: MockServer,
    hits: Arc<AtomicUsize>,
}

impl MockBackend {
    fn url(&self) -> String {
        format!("http://{}", self.server.local_addr())
    }

    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }

    fn stop(self) {
        self.server.stop();
    }
}

fn mock_backend(body: &'static str) -> MockBackend {
    let hits = Arc::new(AtomicUsize::new(0));
    let server = MockServer::bind("127.0.0.1:0").expect("mock backend bind failed");
    let handler_hits = Arc::clone(&hits);
    server.serve(Arc::new(
        move |_req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            handler_hits.fetch_add(1, Ordering::SeqCst);
            w.set_status(200);
            w.write_body(body.as_bytes());
        },
    ));
    MockBackend { server, hits }
}

// -- raw HTTP client helpers (mirrors proxy_test.rs / server_test.rs) -------

const CLIENT_READ_TIMEOUT: Duration = Duration::from_secs(10);

fn send_request(
    addr: SocketAddr,
    method: &str,
    target: &str,
    auth: Option<&str>,
    body: Option<&[u8]>,
) -> (String, String) {
    let mut stream = TcpStream::connect(addr).expect("connect failed");
    stream
        .set_read_timeout(Some(CLIENT_READ_TIMEOUT))
        .expect("set_read_timeout failed");
    let mut req = format!("{method} {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    if let Some(value) = auth {
        req.push_str("Authorization: ");
        req.push_str(value);
        req.push_str("\r\n");
    }
    if let Some(b) = body {
        req.push_str(&format!("Content-Length: {}\r\n\r\n", b.len()));
        stream.write_all(req.as_bytes()).expect("write failed");
        stream.write_all(b).expect("write body failed");
    } else {
        req.push_str("\r\n");
        stream.write_all(req.as_bytes()).expect("write failed");
    }
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read failed (or timed out)");
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

fn dechunk(mut body: &str) -> String {
    let mut out = String::new();
    while let Some((size_line, rest)) = body.split_once("\r\n") {
        let size = usize::from_str_radix(size_line.trim(), 16).expect("chunk size");
        if size == 0 {
            break;
        }
        out.push_str(&rest[..size]);
        body = &rest[size + 2..];
    }
    out
}

fn http_get(addr: SocketAddr, target: &str) -> (String, String) {
    send_request(addr, "GET", target, None, None)
}

fn http_get_auth(addr: SocketAddr, target: &str, auth: &str) -> (String, String) {
    send_request(addr, "GET", target, Some(auth), None)
}

fn http_post_auth(addr: SocketAddr, target: &str, auth: &str, body: &[u8]) -> (String, String) {
    send_request(addr, "POST", target, Some(auth), Some(body))
}

/// Polls `check` until it returns `true` or `timeout` elapses; returns
/// whether it succeeded. Bounds every wait in this file — a routing/reload
/// bug fails the test fast instead of hanging the suite.
fn wait_until(timeout: Duration, mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if check() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
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

// -- tests -------------------------------------------------------------------

/// Drives `/-/reload` end to end:
/// 1. esmauth starts pointed at backend A; a request routes to A.
/// 2. The config file is rewritten (in place, same path) to point at B;
///    `/-/reload` is called; the *next* request routes to B.
/// 3. The config file is then rewritten to syntactically invalid YAML;
///    `/-/reload` is called again and must fail (500, secret-free error);
///    a following request still routes to B — the last-good config is kept,
///    matching `Reloader::reload`'s "keep the previous config on failure"
///    contract in `esmauth/src/lib.rs`.
#[test]
fn hot_reload_switches_backend_then_rejects_broken_config() {
    let backend_a = mock_backend("from-A");
    let backend_b = mock_backend("from-B");

    let yaml_a = format!(
        r#"users:
- name: alice
  bearer_token: t1
  url_prefix: "{}"
"#,
        backend_a.url()
    );
    let (app, config_path) = start_esmauth(&yaml_a);
    let addr = app.local_addr();

    // 1. Routes to A.
    let (status, body) = http_get_auth(addr, "/x", "Bearer t1");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "from-A");
    assert_eq!(backend_a.hits(), 1);
    assert_eq!(backend_b.hits(), 0);

    // 2. Rewrite the config file to point at B, then reload via HTTP.
    let yaml_b = format!(
        r#"users:
- name: alice
  bearer_token: t1
  url_prefix: "{}"
"#,
        backend_b.url()
    );
    std::fs::write(&config_path, &yaml_b).expect("rewrite config to backend B");

    let (status, body) = http_get(addr, "/-/reload");
    assert_eq!(status, "HTTP/1.1 200 OK", "reload body: {body}");
    assert_eq!(body, "OK");

    let (status, body) = http_get_auth(addr, "/x", "Bearer t1");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "from-B");
    assert_eq!(backend_a.hits(), 1, "A must not be hit again after reload");
    assert_eq!(backend_b.hits(), 1);

    // 3. Rewrite the config file to syntactically broken YAML; reload must
    // fail and the previous (B) configuration must be kept.
    std::fs::write(&config_path, "users: [this is not valid: yaml\n").expect("write broken yaml");

    let (status, body) = http_get(addr, "/-/reload");
    assert_eq!(status, "HTTP/1.1 500 Internal Server Error", "body: {body}");
    assert!(
        body.contains("cannot reload -auth.config"),
        "error body should explain the reload failure: {body:?}"
    );
    // The secret-free contract: a broken-YAML error is a parse error, never
    // a token; nothing from the (nonexistent) tokens could leak here, but
    // assert the shape is the generic reload-failure message, not a panic
    // or an empty body.
    assert!(!body.is_empty());

    let (status, body) = http_get_auth(addr, "/x", "Bearer t1");
    assert_eq!(
        status, "HTTP/1.1 200 OK",
        "requests must keep working on the last-good config"
    );
    assert_eq!(body, "from-B", "B (the last-good config) must still serve");
    assert_eq!(backend_a.hits(), 1);
    assert_eq!(backend_b.hits(), 2);

    app.stop();
    backend_a.stop();
    backend_b.stop();
}

/// Starts two independent, real `esmetrics` instances (separate storage
/// dirs, no shared state) behind one esmauth. A write-token user's
/// `url_map` routes `/write` to both instances (`least_loaded`, the
/// default policy); a read-token user's `url_prefix` routes to the same two
/// instances *in the same order*, so it shares the write route's cached
/// `BackendPool` (`esmauth`'s `AuthState::pool_for` keys the pool cache by
/// the URL list) and therefore its round-robin counter.
///
/// Robustness argument for "both backends receive data, and a query through
/// the proxy sees it" without depending on timing:
/// - `BackendPool::select` for `least_loaded` (see `esm-auth/src/balance.rs`)
///   increments a round-robin counter on every call and picks the backend at
///   `(n + i) % len` with zero in-flight requests. Because this test issues
///   writes *sequentially* (each request completes, releasing its backend,
///   before the next starts), every backend has zero in-flight load at
///   selection time, so the counter deterministically alternates:
///   backend 0, 1, 0, 1, ... An even number of sequential writes guarantees
///   an exactly equal split across both real backends.
/// - Both backends are force-flushed directly (bypassing esmauth, using the
///   addresses this test already knows) before querying, so newly ingested
///   rows and their tag-index entries are searchable on both.
/// - The query goes back through esmauth using the read-token user, whose
///   pool is the *same cached pool* as the write route (identical URL list
///   → identical cache key), so it continues the same deterministic
///   alternation — but since both backends now hold the ingested series,
///   either choice returns data. No retry-until-lucky is needed; this is
///   the "write enough points that both get some" strategy from the task
///   brief, made deterministic instead of probabilistic.
#[test]
fn two_backend_proxy_ingests_via_write_and_reads_back_via_query() {
    let esmetrics_a = esmetrics::run(&esmetrics_test_flags()).expect("esmetrics A run failed");
    let esmetrics_b = esmetrics::run(&esmetrics_test_flags()).expect("esmetrics B run failed");
    let addr_a = esmetrics_a.local_addr();
    let addr_b = esmetrics_b.local_addr();
    let url_a = format!("http://{addr_a}");
    let url_b = format!("http://{addr_b}");

    let yaml = format!(
        r#"users:
- name: writer
  bearer_token: wtok
  url_map:
  - src_paths: ["/write"]
    url_prefix:
    - "{url_a}"
    - "{url_b}"
- name: reader
  bearer_token: rtok
  url_prefix:
  - "{url_a}"
  - "{url_b}"
"#
    );
    let (app, _config_path) = start_esmauth(&yaml);
    let addr = app.local_addr();

    // Ingest: N sequential single-line influx writes (no explicit
    // timestamp — the server fills "now", see
    // esm-protoparser::stream::test_parse_stream_fills_missing_timestamp_with_now).
    // N is even so the deterministic least_loaded alternation (see the test
    // doc above) gives each backend exactly N/2 writes.
    //
    // Influx name mapping is `{measurement}_{field_key}` (see
    // esm-insert::influx's module doc), so `MEASUREMENT,tags value=1` is
    // stored under the metric name `{MEASUREMENT}_value`.
    const MEASUREMENT: &str = "e2e_two_backend_write";
    const METRIC: &str = "e2e_two_backend_write_value";
    const N: usize = 20;
    let query_start = now_unix_secs() - 60;
    for i in 0..N {
        let line = format!("{MEASUREMENT},run=t{i} value=1\n");
        let (status, body) = http_post_auth(addr, "/write", "Bearer wtok", line.as_bytes());
        assert_eq!(status, "HTTP/1.1 204 No Content", "write #{i} body: {body}");
    }

    // Both real backends must have actually received rows (proves the
    // url_map route + least_loaded spread both backends, not just one).
    for (name, addr) in [("A", addr_a), ("B", addr_b)] {
        let (status, body) = http_get(addr, "/metrics");
        assert_eq!(status, "HTTP/1.1 200 OK");
        let value: u64 = body
            .lines()
            .find_map(|l| l.strip_prefix("esm_rows_inserted_total{type=\"influx\"} "))
            .unwrap_or_else(|| panic!("counter missing from backend {name} /metrics: {body:?}"))
            .trim()
            .parse()
            .expect("counter value must be a valid u64");
        assert!(
            value >= 1,
            "backend {name} received no writes (value={value})"
        );
    }

    // Force-flush both backends directly so the newly ingested rows (and
    // their tag-index entries) are searchable.
    http_get(addr_a, "/internal/force_flush");
    http_get(addr_b, "/internal/force_flush");

    // Query back through esmauth with the read-token user. Bounded poll
    // (defensive only — force_flush makes data synchronously searchable,
    // see server_test.rs's wait_for_body doc comment) rather than a single
    // shot, so a transient hiccup can't flake the suite.
    let query_end = now_unix_secs() + 60;
    let target =
        format!("/api/v1/query_range?query={METRIC}&start={query_start}&end={query_end}&step=30");
    let mut last_body = String::new();
    let found = wait_until(Duration::from_secs(5), || {
        let (status, body) = http_get_auth(addr, &target, "Bearer rtok");
        assert_eq!(status, "HTTP/1.1 200 OK", "query body: {body}");
        let ok = body.contains(&format!("\"__name__\":\"{METRIC}\""));
        last_body = body;
        ok
    });
    assert!(
        found,
        "ingested series not returned via the proxy query: {last_body:?}"
    );

    app.stop();
    esmetrics_a.stop();
    esmetrics_b.stop();
}

fn esmetrics_test_flags() -> esmetrics::flags::Flags {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "esmauth-reload-e2e-esmetrics-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    esmetrics::flags::Flags {
        http_listen_addr: "127.0.0.1:0".to_string(),
        storage_data_path: dir.to_string_lossy().into_owned(),
        ..esmetrics::flags::Flags::default()
    }
}
