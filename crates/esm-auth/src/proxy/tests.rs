//! Unit tests for esm-auth::proxy — split into a child module
//! (per the T7 brief) since proxy.rs neared the 800-line file-size guideline.

use super::*;
use crate::config::LoadBalancingPolicy;
use std::io::Write as _;
use std::net::TcpStream;
use std::time::Duration;

// -- test harness: a tiny backend server + a front server wired to Proxy --

fn spawn_backend_capturing(
    handler: impl Fn(&mut Request<'_>, &mut ResponseWriter<'_>) + Send + Sync + 'static,
) -> (esm_http::Server, String) {
    let config = esm_http::ServerConfig {
        capture_all_headers: true,
        ..Default::default()
    };
    let server = esm_http::Server::bind_with_config("127.0.0.1:0", config).expect("bind backend");
    let addr = server.local_addr();
    server.serve(Arc::new(handler));
    (server, format!("http://{addr}"))
}

fn spawn_backend(status: u16, body: &str) -> (esm_http::Server, String) {
    let body = body.to_string();
    spawn_backend_capturing(move |_req, w| {
        w.set_status(status);
        w.write_body(body.as_bytes());
    })
}

fn spawn_front(user: UserInfo, policy: LoadBalancingPolicy) -> (esm_http::Server, String) {
    spawn_front_with_retry_cap(user, policy, super::MAX_RETRY_BODY_SIZE)
}

fn spawn_front_with_retry_cap(
    user: UserInfo,
    policy: LoadBalancingPolicy,
    max_retry_body_size: usize,
) -> (esm_http::Server, String) {
    let proxy = Arc::new(Proxy::new(
        reqwest::blocking::Client::new(),
        max_retry_body_size,
    ));
    let user = Arc::new(user);
    let config = esm_http::ServerConfig {
        capture_all_headers: true,
        ..Default::default()
    };
    let server = esm_http::Server::bind_with_config("127.0.0.1:0", config).expect("bind front");
    let addr = server.local_addr();
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let pool_for = |prefixes: &[String]| -> Arc<BackendPool> {
                Arc::new(BackendPool::new(prefixes, policy, Duration::from_secs(30)))
            };
            proxy.proxy(&user, &pool_for, req, w);
        },
    ));
    (server, format!("127.0.0.1:{}", addr.port()))
}

/// Sends a raw HTTP/1.1 request and reads the raw response until the
/// server closes the connection (every test request below sends
/// `Connection: close`, so this always terminates).
fn raw_request(addr: &str, request: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.write_all(request.as_bytes()).expect("write request");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    String::from_utf8_lossy(&buf).into_owned()
}

#[test]
fn proxies_get_and_streams_response() {
    let (_backend, backend_addr) = spawn_backend(200, "hello");
    let user = UserInfo {
        url_prefix: Some(vec![backend_addr]),
        ..Default::default()
    };
    let (_front, front_addr) = spawn_front(user, LoadBalancingPolicy::FirstAvailable);

    let resp = raw_request(
        &front_addr,
        "GET /x HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );

    assert!(resp.starts_with("HTTP/1.1 200"), "resp: {resp}");
    assert!(resp.contains("hello"), "resp: {resp}");
}

#[test]
fn retries_on_configured_status_code_to_second_backend() {
    let (_backend1, addr1) = spawn_backend(500, "fail");
    let (_backend2, addr2) = spawn_backend(200, "ok-from-2");
    let user = UserInfo {
        url_prefix: Some(vec![addr1, addr2]),
        retry_status_codes: vec![500],
        ..Default::default()
    };
    let (_front, front_addr) = spawn_front(user, LoadBalancingPolicy::FirstAvailable);

    let resp = raw_request(
        &front_addr,
        "GET /x HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );

    assert!(resp.starts_with("HTTP/1.1 200"), "resp: {resp}");
    assert!(resp.contains("ok-from-2"), "resp: {resp}");
}

#[test]
fn retryable_body_at_exact_cap_boundary_still_retries_to_second_backend() {
    // Boundary check for `read_buffered_body`: a body of EXACTLY
    // MAX_RETRY_BODY_SIZE (16384) bytes must still be classified retryable
    // (`bufferedBody.canRetry`'s `len(bb.buf) <= maxRetrySize`, main.go:857,
    // is an inclusive `<=`). If the cap check were off by one (`<` instead
    // of `<=`), this request would get a 503 from backend1 instead of
    // successfully retrying to backend2.
    let (_backend1, addr1) = spawn_backend(500, "fail");
    let (_backend2, addr2) = spawn_backend(200, "ok-from-2");
    let user = UserInfo {
        url_prefix: Some(vec![addr1, addr2]),
        retry_status_codes: vec![500],
        ..Default::default()
    };
    let (_front, front_addr) = spawn_front(user, LoadBalancingPolicy::FirstAvailable);

    let body = "a".repeat(super::MAX_RETRY_BODY_SIZE);
    let request = format!(
        "POST /x HTTP/1.1\r\nHost: t\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let resp = raw_request(&front_addr, &request);

    assert!(resp.starts_with("HTTP/1.1 200"), "resp: {resp}");
    assert!(resp.contains("ok-from-2"), "resp: {resp}");
}

#[test]
fn oversized_body_with_retry_status_code_does_not_retry_to_healthy_second_backend() {
    // Regression test for the `RetryStatus` + `!can_retry` path: a body one
    // byte over MAX_RETRY_BODY_SIZE must get a 503 directly from the first
    // (unhealthy-response) backend rather than being retried against a
    // perfectly healthy second backend, since the client body can't be
    // replayed. This also exercises the upstream asymmetry ported in
    // `Proxy::proxy` (main.go:522-540): the backend must NOT be marked
    // broken in this specific branch (unlike the analogous connect-error
    // branch), though that isn't observable from the client-facing
    // response alone.
    let (_backend1, addr1) = spawn_backend(500, "fail");
    let (_backend2, addr2) = spawn_backend(200, "should-not-be-reached");
    let user = UserInfo {
        url_prefix: Some(vec![addr1, addr2]),
        retry_status_codes: vec![500],
        ..Default::default()
    };
    let (_front, front_addr) = spawn_front(user, LoadBalancingPolicy::FirstAvailable);

    let body = "a".repeat(super::MAX_RETRY_BODY_SIZE + 1);
    let request = format!(
        "POST /x HTTP/1.1\r\nHost: t\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let resp = raw_request(&front_addr, &request);

    assert!(resp.starts_with("HTTP/1.1 503"), "resp: {resp}");
    assert!(!resp.contains("should-not-be-reached"), "resp: {resp}");
}

#[test]
fn configured_max_request_body_size_to_retry_gates_retryability() {
    // Proves `-maxRequestBodySizeToRetry` (wired via `Proxy::new`'s
    // `max_retry_body_size` param) actually has an effect: with a small
    // configured cap N, a body of exactly N bytes is still retried at the
    // second backend, while a body of N+1 bytes is not (503 straight from
    // the first, unhealthy backend) — proving a non-default value changes
    // behavior, rather than the proxy silently using a hardcoded constant.
    const N: usize = 64;

    // <= N: retried to the healthy second backend.
    {
        let (_backend1, addr1) = spawn_backend(500, "fail");
        let (_backend2, addr2) = spawn_backend(200, "ok-from-2");
        let user = UserInfo {
            url_prefix: Some(vec![addr1, addr2]),
            retry_status_codes: vec![500],
            ..Default::default()
        };
        let (_front, front_addr) =
            spawn_front_with_retry_cap(user, LoadBalancingPolicy::FirstAvailable, N);

        let body = "a".repeat(N);
        let request = format!(
            "POST /x HTTP/1.1\r\nHost: t\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let resp = raw_request(&front_addr, &request);
        assert!(resp.starts_with("HTTP/1.1 200"), "resp: {resp}");
        assert!(resp.contains("ok-from-2"), "resp: {resp}");
    }

    // N+1: not retried; 503 straight from the first backend, second backend
    // never contacted.
    {
        let (_backend1, addr1) = spawn_backend(500, "fail");
        let (_backend2, addr2) = spawn_backend(200, "should-not-be-reached");
        let user = UserInfo {
            url_prefix: Some(vec![addr1, addr2]),
            retry_status_codes: vec![500],
            ..Default::default()
        };
        let (_front, front_addr) =
            spawn_front_with_retry_cap(user, LoadBalancingPolicy::FirstAvailable, N);

        let body = "a".repeat(N + 1);
        let request = format!(
            "POST /x HTTP/1.1\r\nHost: t\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let resp = raw_request(&front_addr, &request);
        assert!(resp.starts_with("HTTP/1.1 503"), "resp: {resp}");
        assert!(!resp.contains("should-not-be-reached"), "resp: {resp}");
    }
}

#[test]
fn non_retryable_large_body_gets_503_on_backend_error() {
    // A guaranteed-closed port: bind then immediately drop the
    // listener, so a subsequent connect fails with connection-refused.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let dead_addr = listener.local_addr().expect("addr");
    drop(listener);

    let user = UserInfo {
        url_prefix: Some(vec![format!("http://{dead_addr}")]),
        ..Default::default()
    };
    let (_front, front_addr) = spawn_front(user, LoadBalancingPolicy::FirstAvailable);

    let body = "a".repeat(20_000);
    let request = format!(
        "POST /x HTTP/1.1\r\nHost: t\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let resp = raw_request(&front_addr, &request);

    assert!(resp.starts_with("HTTP/1.1 503"), "resp: {resp}");
}

#[test]
fn sets_esmauth_user_agent() {
    let (_backend, addr) = spawn_backend_capturing(|req, w| {
        let ua = req
            .all_headers()
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case("user-agent"))
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        w.set_status(200);
        w.write_body(format!("ua={ua}").as_bytes());
    });
    let user = UserInfo {
        url_prefix: Some(vec![addr]),
        ..Default::default()
    };
    let (_front, front_addr) = spawn_front(user, LoadBalancingPolicy::FirstAvailable);

    let resp = raw_request(
        &front_addr,
        "GET /x HTTP/1.1\r\nHost: t\r\nUser-Agent: custom-client\r\nConnection: close\r\n\r\n",
    );

    assert!(resp.contains("ua=esmauth"), "resp: {resp}");
}

#[test]
fn applies_configured_request_and_response_headers() {
    let (_backend, addr) = spawn_backend_capturing(|req, w| {
        let injected = req
            .all_headers()
            .iter()
            .any(|(n, v)| n.eq_ignore_ascii_case("x-injected") && v == "yes");
        w.set_status(200);
        w.write_body(if injected {
            b"got-injected"
        } else {
            b"missing-injected"
        });
    });
    let user = UserInfo {
        url_prefix: Some(vec![addr]),
        headers: vec![("X-Injected".to_string(), "yes".to_string())],
        response_headers: vec![("X-From-Config".to_string(), "resp".to_string())],
        ..Default::default()
    };
    let (_front, front_addr) = spawn_front(user, LoadBalancingPolicy::FirstAvailable);

    let resp = raw_request(
        &front_addr,
        "GET /x HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );

    assert!(resp.contains("got-injected"), "resp: {resp}");
    assert!(resp.contains("X-From-Config: resp"), "resp: {resp}");
}

#[test]
fn strips_hop_by_hop_headers() {
    let (_backend, addr) = spawn_backend_capturing(|req, w| {
        let names: Vec<String> = req
            .all_headers()
            .iter()
            .map(|(n, _)| n.to_ascii_lowercase())
            .collect();
        let mut report = String::new();
        for n in [
            "connection",
            "keep-alive",
            "x-should-strip",
            "authorization",
            "x-custom",
        ] {
            report.push_str(&format!("{n}={}\n", names.iter().any(|h| h == n)));
        }
        let auth = req
            .all_headers()
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case("authorization"))
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        report.push_str(&format!("auth-value={auth}\n"));
        w.set_status(200);
        w.write_body(report.as_bytes());
    });
    let user = UserInfo {
        url_prefix: Some(vec![addr]),
        ..Default::default()
    };
    let (_front, front_addr) = spawn_front(user, LoadBalancingPolicy::FirstAvailable);

    let request = "GET /x HTTP/1.1\r\n\
        Host: t\r\n\
        Connection: close, X-Should-Strip\r\n\
        Keep-Alive: timeout=5\r\n\
        X-Should-Strip: yes\r\n\
        X-Custom: keep\r\n\
        Authorization: Bearer abc\r\n\
        \r\n";
    let resp = raw_request(&front_addr, request);

    assert!(resp.contains("connection=false"), "resp: {resp}");
    assert!(resp.contains("keep-alive=false"), "resp: {resp}");
    assert!(resp.contains("x-should-strip=false"), "resp: {resp}");
    // Non-hop-by-hop custom headers must still pass through unchanged.
    assert!(resp.contains("x-custom=true"), "resp: {resp}");
    // Authorization is NOT hop-by-hop upstream and must be forwarded.
    assert!(resp.contains("authorization=true"), "resp: {resp}");
    assert!(resp.contains("auth-value=Bearer abc"), "resp: {resp}");
}

#[test]
fn strips_spoofable_x_forwarded_for_header() {
    // Security: a client-supplied X-Forwarded-For must NOT reach the backend
    // (it would let any client spoof the trusted client-IP a backend uses
    // for allowlisting/logging). This proxy strips it; authoritative
    // peer-derived XFF is a documented follow-up pending esm-http peer
    // exposure.
    let (_backend, addr) = spawn_backend_capturing(|req, w| {
        let has_xff = req
            .all_headers()
            .iter()
            .any(|(n, _)| n.eq_ignore_ascii_case("x-forwarded-for"));
        let has_custom = req
            .all_headers()
            .iter()
            .any(|(n, v)| n.eq_ignore_ascii_case("x-custom") && v == "keep");
        w.set_status(200);
        w.write_body(format!("xff={has_xff} custom={has_custom}").as_bytes());
    });
    let user = UserInfo {
        url_prefix: Some(vec![addr]),
        ..Default::default()
    };
    let (_front, front_addr) = spawn_front(user, LoadBalancingPolicy::FirstAvailable);

    let request = "GET /x HTTP/1.1\r\n\
        Host: t\r\n\
        X-Forwarded-For: 1.2.3.4\r\n\
        X-Custom: keep\r\n\
        Connection: close\r\n\
        \r\n";
    let resp = raw_request(&front_addr, request);

    // The spoofed XFF is dropped, but unrelated custom headers still pass.
    assert!(resp.contains("xff=false"), "resp: {resp}");
    assert!(resp.contains("custom=true"), "resp: {resp}");
}

#[test]
fn oversized_body_gets_413_and_bounds_memory() {
    // A body over MAX_REQUEST_BODY_SIZE (32 MiB) must be rejected with 413
    // and never fully buffered, so a hostile/oversized request can't exhaust
    // memory. The 413 is decided before any backend attempt, so the backend
    // (a live one here) is never contacted for the oversized body.
    let (_backend, addr) = spawn_backend(200, "should-not-be-reached");
    let user = UserInfo {
        url_prefix: Some(vec![addr]),
        ..Default::default()
    };
    let (_front, front_addr) = spawn_front(user, LoadBalancingPolicy::FirstAvailable);

    // One byte over the 32 MiB ceiling. Stream the body in blocks instead of
    // materializing a single 32 MiB request string, to keep the test's own
    // footprint down (the point is that the *server* stays bounded).
    let body_len = super::MAX_REQUEST_BODY_SIZE + 1;
    let mut stream = TcpStream::connect(&front_addr).expect("connect");
    let head = format!(
        "POST /x HTTP/1.1\r\nHost: t\r\nConnection: close\r\nContent-Length: {body_len}\r\n\r\n"
    );
    stream.write_all(head.as_bytes()).expect("write head");
    let block = vec![b'a'; 64 * 1024];
    let mut written = 0usize;
    while written < body_len {
        let n = (body_len - written).min(block.len());
        // The server may close the socket (after sending 413) before we
        // finish writing the whole oversized body — that's expected, so a
        // write error here is not a test failure.
        if stream.write_all(&block[..n]).is_err() {
            break;
        }
        written += n;
    }
    let _ = stream.flush();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    let resp = String::from_utf8_lossy(&buf);

    assert!(resp.starts_with("HTTP/1.1 413"), "resp: {resp}");
    assert!(!resp.contains("should-not-be-reached"), "resp: {resp}");
}

// -- unit-level coverage for the smaller helpers --

#[test]
fn sanitize_request_headers_strips_forwarded_and_hop_by_hop_keeps_rest() {
    let headers = vec![
        ("Host".to_string(), "client".to_string()),
        ("Connection".to_string(), "keep-alive".to_string()),
        ("X-Forwarded-For".to_string(), "1.2.3.4".to_string()),
        ("X-Forwarded-Host".to_string(), "evil".to_string()),
        ("X-Forwarded-Proto".to_string(), "https".to_string()),
        ("Authorization".to_string(), "Bearer t".to_string()),
        ("X-Custom".to_string(), "keep".to_string()),
    ];
    let out = sanitize_request_headers(&headers);
    let has = |name: &str| out.iter().any(|(n, _)| n.eq_ignore_ascii_case(name));

    assert!(!has("host"), "Host must be excluded from forwarded set");
    assert!(!has("connection"), "hop-by-hop Connection must be stripped");
    assert!(!has("x-forwarded-for"), "spoofable XFF must be stripped");
    assert!(!has("x-forwarded-host"), "spoofable XFH must be stripped");
    assert!(!has("x-forwarded-proto"), "spoofable XFP must be stripped");
    // Authorization and unrelated custom headers survive.
    assert!(has("authorization"));
    assert!(has("x-custom"));
}

#[test]
fn user_label_prefers_name_then_username_then_hashed_bearer() {
    let named = UserInfo {
        name: Some("svc".to_string()),
        username: Some("alice".to_string()),
        ..Default::default()
    };
    assert_eq!(user_label(&named), "svc");

    let with_username = UserInfo {
        username: Some("alice".to_string()),
        bearer_token: Some("tok".to_string()),
        ..Default::default()
    };
    assert_eq!(user_label(&with_username), "alice");

    let bearer_only = UserInfo {
        bearer_token: Some("s3cr3t-bearer-value".to_string()),
        ..Default::default()
    };
    let label = user_label(&bearer_only);
    assert!(label.starts_with("bearer_token:hash:"), "label: {label}");
    assert!(
        !label.contains("s3cr3t-bearer-value"),
        "raw token must not leak into the label: {label}"
    );

    assert_eq!(user_label(&UserInfo::default()), "");
}

#[test]
fn apply_header_config_deletes_on_empty_value_and_sets_on_nonempty() {
    let mut headers = vec![
        ("X-Keep".to_string(), "1".to_string()),
        ("X-Drop".to_string(), "old".to_string()),
    ];
    apply_header_config(
        &mut headers,
        &[
            ("X-Drop".to_string(), "".to_string()),
            ("X-New".to_string(), "v".to_string()),
        ],
    );
    assert_eq!(
        headers,
        vec![
            ("X-Keep".to_string(), "1".to_string()),
            ("X-New".to_string(), "v".to_string()),
        ]
    );
}

#[test]
fn resolve_host_header_keeps_original_when_configured() {
    let user = UserInfo {
        keep_original_host: Some(true),
        ..Default::default()
    };
    assert_eq!(
        resolve_host_header(&user, &[], "client-host"),
        Some("client-host".to_string())
    );
}

#[test]
fn resolve_host_header_uses_configured_host_header() {
    let user = UserInfo::default();
    let headers = [("Host".to_string(), "configured".to_string())];
    assert_eq!(
        resolve_host_header(&user, &headers, "client-host"),
        Some("configured".to_string())
    );
}

#[test]
fn resolve_host_header_defaults_to_none() {
    let user = UserInfo::default();
    assert_eq!(resolve_host_header(&user, &[], "client-host"), None);
}

#[test]
fn redact_url_userinfo_strips_credentials_but_keeps_host() {
    // user:pass@ is removed; scheme, host, port, and path survive.
    assert_eq!(
        redact_url_userinfo("http://user:pass@backend:8480/api/v1/write"),
        "http://backend:8480/api/v1/write"
    );
    // Userinfo without a password.
    assert_eq!(
        redact_url_userinfo("http://user@backend:8480/x"),
        "http://backend:8480/x"
    );
    // A password containing a ':' is still fully stripped.
    assert_eq!(
        redact_url_userinfo("https://u:p:with:colons@h:9/p?q=1"),
        "https://h:9/p?q=1"
    );
}

#[test]
fn redact_url_userinfo_leaves_credential_free_urls_unchanged() {
    assert_eq!(
        redact_url_userinfo("http://backend:8480/api"),
        "http://backend:8480/api"
    );
    // A '@' in the path (not the authority) must not be treated as userinfo.
    assert_eq!(
        redact_url_userinfo("http://backend:8480/path@notcreds"),
        "http://backend:8480/path@notcreds"
    );
    // Non-URL input is returned verbatim.
    assert_eq!(redact_url_userinfo("not a url"), "not a url");
}
