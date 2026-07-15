//! End-to-end integration tests for the esmauth binary: start the real
//! `esmauth::run` server (bound to `127.0.0.1:0`) in front of one or more
//! mock backends (tiny in-test `esm_http::Server`s), then drive it over raw
//! `TcpStream`s exactly like `esmetrics/tests/server_test.rs` does for the
//! main binary.
//!
//! Mock backends record which of them served each request (an
//! `AtomicUsize` hit counter per backend) so distribution/failover/retry
//! behavior can be asserted directly, instead of trusting log lines.
//!
//! Timing-sensitive tests (load-balancing spread, per-user concurrency
//! queueing) use a `Gate`: a mock backend blocks on it after recording its
//! hit, so the test controls exactly when an in-flight request completes.
//! Every gate wait is bounded by `MAX_GATE_WAIT` so a routing/limiter bug
//! fails the test fast instead of hanging the suite; every poll loop below
//! is likewise bounded by an explicit deadline.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use esm_http::{Request, ResponseWriter, Server as MockServer};
use esmauth::flags::Flags;

// -- config / flags plumbing --------------------------------------------

/// Writes `yaml` to a fresh temp file (a unique dir per test, mirroring
/// `esmetrics/tests/server_test.rs`'s `test_flags`), and returns its path.
fn write_config(yaml: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "esmauth-proxy-test-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("create temp config dir");
    let path = dir.join("auth.yml");
    std::fs::write(&path, yaml).expect("write auth config");
    path
}

fn test_flags(config_path: &Path) -> Flags {
    Flags {
        auth_config: config_path.to_string_lossy().into_owned(),
        http_listen_addr: "127.0.0.1:0".to_string(),
        ..Flags::default()
    }
}

/// Starts esmauth against `yaml` with default flags (except the listen
/// address and config path).
fn start_esmauth(yaml: &str) -> esmauth::App {
    let path = write_config(yaml);
    esmauth::run(&test_flags(&path)).expect("esmauth run failed")
}

/// Starts esmauth against `yaml`, letting the caller tweak flags (e.g. a
/// tiny `-maxQueueDuration` for the concurrency-limit test) before boot.
fn start_esmauth_with(yaml: &str, mutate: impl FnOnce(&mut Flags)) -> esmauth::App {
    let path = write_config(yaml);
    let mut flags = test_flags(&path);
    mutate(&mut flags);
    esmauth::run(&flags).expect("esmauth run failed")
}

// -- mock backends --------------------------------------------------------

/// Bounds every `Gate::wait` — no test can hang forever on a mock backend.
const MAX_GATE_WAIT: Duration = Duration::from_secs(5);

/// A one-shot, many-waiters gate: mock backend handlers block in `wait`
/// until the test calls `release` (or `MAX_GATE_WAIT` elapses), so the test
/// controls exactly when in-flight requests complete.
struct Gate {
    ready: Mutex<bool>,
    cv: Condvar,
}

impl Gate {
    fn new() -> Arc<Gate> {
        Arc::new(Gate {
            ready: Mutex::new(false),
            cv: Condvar::new(),
        })
    }

    fn wait(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut ready = self.ready.lock().unwrap();
        while !*ready {
            let now = Instant::now();
            if now >= deadline {
                return;
            }
            let (guard, _) = self.cv.wait_timeout(ready, deadline - now).unwrap();
            ready = guard;
        }
    }

    fn release(&self) {
        *self.ready.lock().unwrap() = true;
        self.cv.notify_all();
    }
}

/// A mock upstream backend: an `esm_http::Server` on an ephemeral port that
/// records a hit for every request it serves.
struct MockBackend {
    server: MockServer,
    hits: Arc<AtomicUsize>,
}

impl MockBackend {
    fn addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr())
    }

    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }

    fn stop(self) {
        self.server.stop();
    }
}

/// Starts a mock backend that always answers `status`/`body` immediately.
fn mock_backend(status: u16, body: &'static str) -> MockBackend {
    let hits = Arc::new(AtomicUsize::new(0));
    let server = MockServer::bind("127.0.0.1:0").expect("mock backend bind failed");
    let handler_hits = Arc::clone(&hits);
    server.serve(Arc::new(
        move |_req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            handler_hits.fetch_add(1, Ordering::SeqCst);
            w.set_status(status);
            w.write_body(body.as_bytes());
        },
    ));
    MockBackend { server, hits }
}

/// Starts a mock backend that records a hit, then blocks on `gate` (bounded
/// by `MAX_GATE_WAIT`) before answering `status`/`body`.
fn gated_mock_backend(status: u16, body: &'static str, gate: Arc<Gate>) -> MockBackend {
    let hits = Arc::new(AtomicUsize::new(0));
    let server = MockServer::bind("127.0.0.1:0").expect("mock backend bind failed");
    let handler_hits = Arc::clone(&hits);
    server.serve(Arc::new(
        move |_req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            handler_hits.fetch_add(1, Ordering::SeqCst);
            gate.wait(MAX_GATE_WAIT);
            w.set_status(status);
            w.write_body(body.as_bytes());
        },
    ));
    MockBackend { server, hits }
}

/// A `http://host:port` that nothing is listening on: binds an ephemeral
/// port, then immediately drops the listener so a connect attempt gets a
/// fast, deterministic "connection refused" instead of a hang.
fn dead_backend_url() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind dead addr");
    let addr = listener.local_addr().expect("dead addr local_addr");
    drop(listener);
    format!("http://{addr}")
}

// -- raw HTTP client helpers (mirrors esmetrics/tests/server_test.rs) -----

/// Bounds every client read; a server bug that fails to close the
/// connection fails the test instead of hanging it.
const CLIENT_READ_TIMEOUT: Duration = Duration::from_secs(10);

fn send_request(addr: SocketAddr, target: &str, auth: Option<&str>) -> (String, String) {
    let mut stream = TcpStream::connect(addr).expect("connect failed");
    stream
        .set_read_timeout(Some(CLIENT_READ_TIMEOUT))
        .expect("set_read_timeout failed");
    let mut req = format!("GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    if let Some(value) = auth {
        req.push_str("Authorization: ");
        req.push_str(value);
        req.push_str("\r\n");
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).expect("write failed");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read failed (or timed out)");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed response: {response:?}"));
    let status_line = head.lines().next().unwrap_or_default().to_string();
    // Proxied (successful) responses go through `stream_response`, which
    // always re-frames the backend body as `Transfer-Encoding: chunked` (see
    // esm-auth::proxy); dechunk it like esmetrics/tests/server_test.rs does.
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

/// Decodes an HTTP/1.1 chunked-transfer-encoded body (mirrors
/// `esmetrics/tests/server_test.rs`'s helper of the same name).
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

/// Sends a POST with a raw body and `Authorization` header, returning the
/// status line and (dechunked) body. Used by the URL-redaction test, which
/// needs a body larger than `-maxRequestBodySizeToRetry` so the request is
/// non-retryable and the proxy answers with a target-naming 503.
fn http_post_body(addr: SocketAddr, target: &str, auth: &str, body: &[u8]) -> (String, String) {
    let mut stream = TcpStream::connect(addr).expect("connect failed");
    stream
        .set_read_timeout(Some(CLIENT_READ_TIMEOUT))
        .expect("set_read_timeout failed");
    let head = format!(
        "POST {target} HTTP/1.1\r\nHost: localhost\r\nAuthorization: {auth}\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .expect("write head failed");
    stream.write_all(body).expect("write body failed");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read failed (or timed out)");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed response: {response:?}"));
    let status_line = head.lines().next().unwrap_or_default().to_string();
    (status_line, body.to_string())
}

fn http_get(addr: SocketAddr, target: &str) -> (String, String) {
    send_request(addr, target, None)
}

fn http_get_auth(addr: SocketAddr, target: &str, auth: &str) -> (String, String) {
    send_request(addr, target, Some(auth))
}

/// Standard base64 (RFC 4648, `=` padding) — used only to build `Basic` auth
/// headers for the test client; independent of (and not exercising) the
/// production encoder in `esm-auth::auth`.
fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        let n = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);
        out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[((n >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(n & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// Polls `check` until it returns `true` or `timeout` elapses; returns
/// whether it succeeded. Used to wait for hit counters to reach an expected
/// value without a fixed sleep.
fn wait_until(timeout: Duration, mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if check() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

// -- tests ------------------------------------------------------------------

#[test]
fn unauthenticated_request_gets_401_when_no_unauthorized_user() {
    let backend = mock_backend(200, "ok");
    let yaml = format!(
        r#"users:
- name: alice
  bearer_token: t1
  url_prefix: "{}"
"#,
        backend.url()
    );
    let app = start_esmauth(&yaml);
    let addr = app.local_addr();

    let (status, body) = http_get(addr, "/anything");
    assert_eq!(status, "HTTP/1.1 401 Unauthorized");
    assert_eq!(body, "missing 'Authorization' request header\n");
    assert_eq!(backend.hits(), 0);

    app.stop();
    backend.stop();
}

#[test]
fn bearer_token_routes_to_configured_backend() {
    let backend = mock_backend(200, "backend-body");
    let yaml = format!(
        r#"users:
- name: alice
  bearer_token: t1
  url_prefix: "{}"
"#,
        backend.url()
    );
    let app = start_esmauth(&yaml);
    let addr = app.local_addr();

    let (status, body) = http_get_auth(addr, "/x", "Bearer t1");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "backend-body");
    assert_eq!(backend.hits(), 1);

    app.stop();
    backend.stop();
}

#[test]
fn basic_auth_routes_to_configured_backend() {
    let backend = mock_backend(200, "basic-ok");
    let yaml = format!(
        r#"users:
- name: alice
  username: alice
  password: s3cr3t
  url_prefix: "{}"
"#,
        backend.url()
    );
    let app = start_esmauth(&yaml);
    let addr = app.local_addr();

    let auth = format!("Basic {}", base64_encode(b"alice:s3cr3t"));
    let (status, body) = http_get_auth(addr, "/y", &auth);
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "basic-ok");
    assert_eq!(backend.hits(), 1);

    app.stop();
    backend.stop();
}

#[test]
fn wrong_token_gets_401() {
    let backend = mock_backend(200, "ok");
    let yaml = format!(
        r#"users:
- name: alice
  bearer_token: correct
  url_prefix: "{}"
"#,
        backend.url()
    );
    let app = start_esmauth(&yaml);
    let addr = app.local_addr();

    let (status, body) = http_get_auth(addr, "/x", "Bearer wrong");
    assert_eq!(status, "HTTP/1.1 401 Unauthorized");
    assert_eq!(body, "Unauthorized\n");
    assert_eq!(backend.hits(), 0);

    app.stop();
    backend.stop();
}

#[test]
fn url_map_routes_write_and_query_to_different_backends() {
    let write_backend = mock_backend(200, "write-ok");
    let query_backend = mock_backend(200, "query-ok");
    let yaml = format!(
        r#"users:
- name: alice
  bearer_token: t1
  url_map:
  - src_paths: ["/api/v1/write"]
    url_prefix: "{}"
  - src_paths: ["/api/v1/query"]
    url_prefix: "{}"
"#,
        write_backend.url(),
        query_backend.url(),
    );
    let app = start_esmauth(&yaml);
    let addr = app.local_addr();

    let (status, body) = http_get_auth(addr, "/api/v1/write", "Bearer t1");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "write-ok");
    assert_eq!(write_backend.hits(), 1);
    assert_eq!(query_backend.hits(), 0);

    let (status, body) = http_get_auth(addr, "/api/v1/query", "Bearer t1");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "query-ok");
    assert_eq!(write_backend.hits(), 1);
    assert_eq!(query_backend.hits(), 1);

    app.stop();
    write_backend.stop();
    query_backend.stop();
}

#[test]
fn least_loaded_spreads_across_two_backends() {
    let gate = Gate::new();
    let backend_a = gated_mock_backend(200, "a", Arc::clone(&gate));
    let backend_b = gated_mock_backend(200, "b", Arc::clone(&gate));
    let yaml = format!(
        r#"users:
- name: alice
  bearer_token: t1
  url_prefix:
  - "{}"
  - "{}"
"#,
        backend_a.url(),
        backend_b.url(),
    );
    let app = start_esmauth(&yaml);
    let addr = app.local_addr();

    const N: usize = 4;
    let clients: Vec<_> = (0..N)
        .map(|_| thread::spawn(move || http_get_auth(addr, "/spread", "Bearer t1")))
        .collect();

    // Wait until every request has reached a backend and parked on the
    // gate — bounded so a routing bug fails fast rather than hanging.
    let reached = wait_until(Duration::from_secs(5), || {
        backend_a.hits() + backend_b.hits() >= N
    });
    assert!(
        reached,
        "not all {N} requests reached a backend in time (a={}, b={})",
        backend_a.hits(),
        backend_b.hits()
    );
    assert!(backend_a.hits() > 0, "backend a was never selected");
    assert!(backend_b.hits() > 0, "backend b was never selected");

    gate.release();
    for client in clients {
        let (status, _) = client.join().expect("client thread panicked");
        assert_eq!(status, "HTTP/1.1 200 OK");
    }

    app.stop();
    backend_a.stop();
    backend_b.stop();
}

#[test]
fn first_available_fails_over_when_first_backend_down() {
    let healthy = mock_backend(200, "healthy-ok");
    let yaml = format!(
        r#"users:
- name: alice
  bearer_token: t1
  url_prefix:
  - "{}"
  - "{}"
  load_balancing_policy: first_available
"#,
        dead_backend_url(),
        healthy.url(),
    );
    let app = start_esmauth(&yaml);
    let addr = app.local_addr();

    let (status, body) = http_get_auth(addr, "/z", "Bearer t1");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "healthy-ok");
    assert_eq!(healthy.hits(), 1);

    app.stop();
    healthy.stop();
}

#[test]
fn retries_5xx_to_healthy_backend() {
    let failing = mock_backend(503, "fail");
    let healthy = mock_backend(200, "healthy-ok");
    let yaml = format!(
        r#"users:
- name: alice
  bearer_token: t1
  url_prefix:
  - "{}"
  - "{}"
  load_balancing_policy: first_available
  retry_status_codes: [503]
"#,
        failing.url(),
        healthy.url(),
    );
    let app = start_esmauth(&yaml);
    let addr = app.local_addr();

    let (status, body) = http_get_auth(addr, "/w", "Bearer t1");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "healthy-ok");
    assert_eq!(failing.hits(), 1);
    assert_eq!(healthy.hits(), 1);

    app.stop();
    failing.stop();
    healthy.stop();
}

#[test]
fn per_user_concurrency_limit_queues_then_429_on_timeout() {
    let gate = Gate::new();
    let backend = gated_mock_backend(200, "slow-ok", Arc::clone(&gate));
    let yaml = format!(
        r#"users:
- name: alice
  bearer_token: t1
  url_prefix: "{}"
"#,
        backend.url()
    );
    let app = start_esmauth_with(&yaml, |flags| {
        flags.max_concurrent_per_user_requests = 1;
        flags.max_queue_duration = Duration::from_millis(150);
    });
    let addr = app.local_addr();

    let first = thread::spawn(move || http_get_auth(addr, "/slow", "Bearer t1"));

    // Wait for the first request to occupy the user's one concurrency slot
    // (it has reached the backend and is parked on the gate) before firing
    // the second — bounded so a stuck first request fails fast.
    let occupied = wait_until(Duration::from_secs(5), || backend.hits() >= 1);
    assert!(occupied, "first request never reached the backend");

    let (status, body) = http_get_auth(addr, "/slow", "Bearer t1");
    assert_eq!(status, "HTTP/1.1 429 Too Many Requests");
    assert_eq!(body, "too many concurrent requests\n");

    gate.release();
    let (status, body) = first.join().expect("first request thread panicked");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "slow-ok");

    app.stop();
    backend.stop();
}

#[test]
fn metrics_endpoint_exposes_esmauth_counters() {
    let backend = mock_backend(200, "ok");
    let yaml = format!(
        r#"users:
- name: proxy_test_metrics_user
  bearer_token: t1
  url_prefix: "{}"
"#,
        backend.url()
    );
    let app = start_esmauth(&yaml);
    let addr = app.local_addr();

    // A valid request bumps esmauth_user_requests_total{username="..."}.
    let (status, _) = http_get_auth(addr, "/m", "Bearer t1");
    assert_eq!(status, "HTTP/1.1 200 OK");

    // An invalid token with no unauthorized_user configured bumps
    // esmauth_http_request_errors_total{reason="invalid_auth_token"}.
    let (status, _) = http_get_auth(addr, "/m", "Bearer wrong");
    assert_eq!(status, "HTTP/1.1 401 Unauthorized");

    let (status, body) = http_get(addr, "/metrics");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(
        body.contains(r#"esmauth_user_requests_total{username="proxy_test_metrics_user"}"#),
        "body: {body}"
    );
    assert!(
        body.contains(r#"esmauth_http_request_errors_total{reason="invalid_auth_token"}"#),
        "body: {body}"
    );

    app.stop();
    backend.stop();
}

#[test]
fn reload_auth_key_gates_reload_endpoint() {
    let backend = mock_backend(200, "ok");
    let yaml = format!(
        r#"users:
- name: alice
  bearer_token: t1
  url_prefix: "{}"
"#,
        backend.url()
    );
    let app = start_esmauth_with(&yaml, |flags| {
        flags.reload_auth_key = "rsecret".to_string();
    });
    let addr = app.local_addr();

    // No key -> 403 and the reload is NOT performed.
    let (status, body) = http_get(addr, "/-/reload");
    assert_eq!(status, "HTTP/1.1 403 Forbidden");
    assert_eq!(body, "The provided authKey doesn't match -reloadAuthKey\n");

    // Wrong key -> still 403.
    let (status, _) = http_get(addr, "/-/reload?authKey=nope");
    assert_eq!(status, "HTTP/1.1 403 Forbidden");

    // Correct key -> 200 and the reload actually happens.
    let (status, body) = http_get(addr, "/-/reload?authKey=rsecret");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "OK");

    app.stop();
    backend.stop();
}

#[test]
fn metrics_auth_key_gates_metrics_endpoint() {
    let backend = mock_backend(200, "ok");
    let yaml = format!(
        r#"users:
- name: alice
  bearer_token: t1
  url_prefix: "{}"
"#,
        backend.url()
    );
    let app = start_esmauth_with(&yaml, |flags| {
        flags.metrics_auth_key = "msecret".to_string();
    });
    let addr = app.local_addr();

    let (status, body) = http_get(addr, "/metrics");
    assert_eq!(status, "HTTP/1.1 403 Forbidden");
    assert_eq!(body, "The provided authKey doesn't match -metricsAuthKey\n");
    assert!(
        !body.contains("esmauth_"),
        "metrics leaked despite 403: {body}"
    );

    let (status, body) = http_get(addr, "/metrics?authKey=msecret");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(
        body.contains("esmauth_"),
        "metrics missing after auth: {body}"
    );

    app.stop();
    backend.stop();
}

#[test]
fn empty_auth_keys_leave_endpoints_open() {
    let backend = mock_backend(200, "ok");
    let yaml = format!(
        r#"users:
- name: alice
  bearer_token: t1
  url_prefix: "{}"
"#,
        backend.url()
    );
    // Default flags: both auth keys empty.
    let app = start_esmauth(&yaml);
    let addr = app.local_addr();

    let (status, _) = http_get(addr, "/metrics");
    assert_eq!(status, "HTTP/1.1 200 OK");
    let (status, body) = http_get(addr, "/-/reload");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "OK");

    app.stop();
    backend.stop();
}

#[test]
fn backend_credentials_are_redacted_from_client_error_body() {
    // A dead backend whose url_prefix embeds credentials. A non-retryable
    // (large) body forces the target-naming 503 path.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind dead addr");
    let dead_addr = listener.local_addr().expect("dead addr local_addr");
    drop(listener);
    let yaml = format!(
        r#"users:
- name: alice
  bearer_token: t1
  url_prefix: "http://u:p@{dead_addr}"
"#
    );
    let app = start_esmauth(&yaml);
    let addr = app.local_addr();

    // Body > 16 KiB (-maxRequestBodySizeToRetry) => non-retryable => 503 names
    // the target.
    let big = vec![b'x'; 32 * 1024];
    let (status, body) = http_post_body(addr, "/api/v1/write", "Bearer t1", &big);
    assert_eq!(status, "HTTP/1.1 503 Service Unavailable");
    assert!(!body.contains("u:p@"), "creds leaked to client: {body}");
    assert!(!body.contains("u:p"), "creds leaked to client: {body}");
    // The host is still identified so the error stays actionable.
    assert!(
        body.contains(&dead_addr.to_string()),
        "error body should still name the backend host: {body}"
    );

    app.stop();
}
