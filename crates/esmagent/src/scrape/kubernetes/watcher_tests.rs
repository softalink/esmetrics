//! Stub-server tests for [`super`] — split out of `watcher.rs` per this
//! crate's `#[path]`-sibling convention (see `scrapework_tests.rs`) to keep
//! both files under the repo's 800-line cap.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_http::{Request, ResponseWriter, Server};

use super::*;
use crate::scrape::config::KubernetesSdConfig;
use crate::scrape::kubernetes::client::{resolve_api_config, InClusterPaths};
use crate::scrape::kubernetes::registry::BuildCtx;

/// Polls `check` until it returns `true` or `timeout` elapses. Bounds every
/// wait in this file so a wiring bug fails the test fast instead of hanging
/// the suite (mirrors `tests/e2e.rs`'s `wait_until`).
fn wait_until(timeout: Duration, mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if check() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Canned responses for a stub Kubernetes API server: `list_body` answers
/// every non-watch request (LIST), `watch_lines` are streamed (one per
/// line, newline-joined) in response to every watch request (`watch=1` in
/// the query string), then the response body ends.
struct K8sStubScript {
    list_body: String,
    watch_lines: Vec<String>,
}

/// A running stub Kubernetes API server. `requests` records every request's
/// `path?query` string in arrival order, so a test can assert the LIST/WATCH
/// call sequence the watcher made.
struct K8sStub {
    server: Server,
    requests: Arc<Mutex<Vec<String>>>,
}

impl K8sStub {
    fn base_url(&self) -> String {
        format!("http://{}", self.server.local_addr())
    }

    /// Snapshot of the recorded request URLs so far.
    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }

    fn stop(&self) {
        self.server.stop();
    }
}

/// True for a recorded request URL that is a WATCH (carries `watch=1`).
fn is_watch(url: &str) -> bool {
    url.contains("watch=1")
}

/// Starts a stub k8s API server that answers every request with `script`,
/// routing on whether the query string carries `watch=1` (mirrors
/// `ApiConfig::watch_url`'s query shape) and recording every request URL.
fn start_k8s_stub(script: K8sStubScript) -> K8sStub {
    let server = Server::bind("127.0.0.1:0").expect("bind k8s stub");
    let list_body = script.list_body;
    // Each script entry must become exactly one physical line on the wire
    // (real k8s watch events are single-line JSON) — collapse any embedded
    // newlines from the source's multi-line raw-string formatting before
    // joining, so `BufReader::read_line` in the watcher sees one JSON
    // object per line rather than a JSON object split across two lines.
    let watch_body: String = script
        .watch_lines
        .iter()
        .map(|line| format!("{}\n", line.replace('\n', " ")))
        .collect();

    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_for_handler = Arc::clone(&requests);

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            requests_for_handler
                .lock()
                .unwrap()
                .push(format!("{}?{}", req.path(), req.query()));
            if req.query().contains("watch=1") {
                w.write_body(watch_body.as_bytes());
            } else {
                w.write_json(200, &list_body);
            }
        },
    ));

    K8sStub { server, requests }
}

/// Resolves an [`ApiConfig`] pointed at `base_url` with no auth/TLS — the
/// watcher's `role`/`namespace`/`selectors` are passed to [`start`]
/// separately, so the config's own `role` field here is irrelevant.
fn api_config_for_test(base_url: &str) -> ApiConfig {
    let cfg = KubernetesSdConfig {
        api_server: Some(base_url.to_string()),
        ..KubernetesSdConfig::default()
    };
    resolve_api_config(&cfg, &InClusterPaths::default()).unwrap()
}

#[test]
fn watcher_lists_then_applies_watch_events() {
    let stub = start_k8s_stub(K8sStubScript {
        list_body: r#"{"metadata":{"resourceVersion":"10"},"items":[
            {"metadata":{"name":"a","namespace":"d"},"spec":{"containers":[{"name":"c"}]},
             "status":{"phase":"Running","podIP":"10.0.0.1"}}]}"#
            .into(),
        // one MODIFIED adding pod b, one DELETED removing a, then the stream closes.
        watch_lines: vec![
            r#"{"type":"ADDED","object":{"metadata":{"name":"b","namespace":"d"},
             "spec":{"containers":[{"name":"c"}]},"status":{"phase":"Running","podIP":"10.0.0.2"}}}"#
                .into(),
            r#"{"type":"DELETED","object":{"metadata":{"name":"a","namespace":"d"}}}"#.into(),
        ],
    });
    let api = Arc::new(api_config_for_test(&stub.base_url()));
    let mut w = start(api, "pod".into(), Some("d".into()), vec![]);

    assert!(
        wait_until(Duration::from_secs(5), || {
            let g = w.target_groups(&BuildCtx::detached());
            g.len() == 1 && g[0].targets == vec!["10.0.0.2".to_string()]
        }),
        "watcher never converged to the post-watch-event state"
    );

    w.stop();
    stub.stop();
}

#[test]
fn watcher_starts_with_an_empty_cache_before_the_first_list_completes() {
    // The cache starts empty; target_groups() must not panic or block
    // before the background thread's first LIST finishes.
    let stub = start_k8s_stub(K8sStubScript {
        list_body: r#"{"metadata":{"resourceVersion":"1"},"items":[]}"#.into(),
        watch_lines: vec![],
    });
    let api = Arc::new(api_config_for_test(&stub.base_url()));
    let mut w = start(api, "pod".into(), None, vec![]);

    // Never panics, even if called immediately.
    let _ = w.target_groups(&BuildCtx::detached());

    assert!(wait_until(Duration::from_secs(5), || w
        .target_groups(&BuildCtx::detached())
        .is_empty()));

    w.stop();
    stub.stop();
}

#[test]
fn watcher_resumes_watch_from_resource_version_without_re_listing() {
    // After a watch stream ends cleanly, the watcher must RESUME the watch
    // from the tracked resourceVersion (upstream `reloadObjects` returns the
    // cached rv without a LIST), not fall back to a fresh LIST. The stub
    // records every request URL; we assert the sequence is
    // LIST -> WATCH -> WATCH... with exactly ONE list at the front.
    let stub = start_k8s_stub(K8sStubScript {
        list_body: r#"{"metadata":{"resourceVersion":"10"},"items":[
            {"metadata":{"name":"a","namespace":"d"},"spec":{"containers":[{"name":"c"}]},
             "status":{"phase":"Running","podIP":"10.0.0.1"}}]}"#
            .into(),
        // No events; the stub just closes the stream each time, so the watcher
        // re-watches from rv=10 repeatedly.
        watch_lines: vec![],
    });
    let api = Arc::new(api_config_for_test(&stub.base_url()));
    let mut w = start(api, "pod".into(), Some("d".into()), vec![]);

    // Wait until we've observed at least two WATCH requests (proving a
    // re-watch happened after the first stream closed).
    assert!(
        wait_until(Duration::from_secs(5), || {
            stub.requests().iter().filter(|u| is_watch(u)).count() >= 2
        }),
        "watcher never re-watched after the first stream closed"
    );

    w.stop();
    stub.stop();

    let reqs = stub.requests();
    assert!(!reqs.is_empty(), "no requests recorded");
    // First request is the initial LIST.
    assert!(
        !is_watch(&reqs[0]) && reqs[0].contains("limit=1000"),
        "first request should be the initial LIST, got {:?}",
        reqs[0]
    );
    // Every subsequent request is a WATCH — NO re-LIST between the watches.
    for (i, url) in reqs.iter().enumerate().skip(1) {
        assert!(
            is_watch(url),
            "request #{i} should be a WATCH (resume, not re-LIST), got {url:?}"
        );
        // And each resumes from the tracked resourceVersion (10 — the
        // fixture's objects carry none, so latest_rv stays at the listed rv).
        assert!(
            url.contains("resourceVersion=10"),
            "watch #{i} should resume from resourceVersion=10, got {url:?}"
        );
    }
    // Exactly one LIST total.
    assert_eq!(
        reqs.iter().filter(|u| !is_watch(u)).count(),
        1,
        "expected exactly one LIST, got requests: {reqs:?}"
    );
}

#[test]
fn watcher_re_lists_promptly_after_an_error_410_watch_event() {
    // An in-band ERROR event with code 410 is k8s's documented "resourceVersion
    // too old" signal: the watcher must end the watch, clear the rv, and
    // re-LIST promptly (upstream `readObjectUpdateStream`) — NOT resume the
    // watch. We prove this by observing a SECOND LIST request (a plain clean
    // close would resume-watch and never re-LIST, keeping the list count at 1).
    let stub = start_k8s_stub(K8sStubScript {
        list_body: r#"{"metadata":{"resourceVersion":"10"},"items":[
            {"metadata":{"name":"a","namespace":"d"},"spec":{"containers":[{"name":"c"}]},
             "status":{"phase":"Running","podIP":"10.0.0.1"}}]}"#
            .into(),
        // A Status error event with code 410, then the stream closes.
        watch_lines: vec![
            r#"{"type":"ERROR","object":{"kind":"Status","apiVersion":"v1",
             "status":"Failure","message":"too old resource version","reason":"Expired","code":410}}"#
                .into(),
        ],
    });
    let api = Arc::new(api_config_for_test(&stub.base_url()));
    let mut w = start(api, "pod".into(), Some("d".into()), vec![]);

    assert!(
        wait_until(Duration::from_secs(5), || {
            stub.requests().iter().filter(|u| !is_watch(u)).count() >= 2
        }),
        "watcher never re-LISTed after the ERROR-410 watch event"
    );

    w.stop();
    stub.stop();
}

#[test]
fn repeated_410_gone_watch_loop_is_throttled_not_flooded() {
    // A pathological server that answers LIST 200 but immediately 410s
    // every WATCH must not flood LIST+WATCH requests unbounded — the
    // `consecutive_gone` floor in `run()` must kick in after the first
    // repeat. Without it this loop would issue thousands of requests per
    // second on loopback; with it, request count stays low over a short
    // window and `stop()` still returns promptly.
    let requests = Arc::new(Mutex::new(0usize));
    let requests_for_handler = Arc::clone(&requests);
    let server = Server::bind("127.0.0.1:0").expect("bind always-410 stub");
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            *requests_for_handler.lock().unwrap() += 1;
            if req.query().contains("watch=1") {
                w.write_status(410);
            } else {
                w.write_json(200, r#"{"metadata":{"resourceVersion":"1"},"items":[]}"#);
            }
        },
    ));
    let base_url = format!("http://{}", server.local_addr());
    let api = Arc::new(api_config_for_test(&base_url));
    let mut w = start(api, "pod".into(), None, vec![]);

    // Let the pathological loop run for a while; the floor delay caps how
    // many LIST+WATCH pairs can happen in this window.
    std::thread::sleep(Duration::from_millis(700));
    let count = *requests.lock().unwrap();

    let started = Instant::now();
    w.stop();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "stop() took {:?} while a repeated-410 loop was active",
        started.elapsed()
    );
    server.stop();

    assert!(
        count < 50,
        "expected the repeated-410 loop to be throttled, got {count} requests in 700ms"
    );
}

#[test]
fn stop_does_not_hang_while_backing_off_after_a_list_error() {
    // A server that answers every request with a 500 forces the watcher
    // into its list-retry backoff loop; `stop()` must still return
    // promptly (bounded by STOP_POLL_INTERVAL, not the full backoff).
    let server = Server::bind("127.0.0.1:0").expect("bind failing stub");
    server.serve(Arc::new(
        move |_req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            w.write_status(500);
        },
    ));
    let base_url = format!("http://{}", server.local_addr());
    let api = Arc::new(api_config_for_test(&base_url));
    let mut w = start(api, "pod".into(), None, vec![]);

    // Give the background thread a moment to hit the first failure and
    // enter its backoff wait.
    std::thread::sleep(Duration::from_millis(100));

    let started = Instant::now();
    w.stop();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "stop() took {:?}, expected it to return promptly",
        started.elapsed()
    );

    server.stop();
}

#[test]
fn v1beta1_fallback_rewrites_discovery_and_networking_paths() {
    assert_eq!(
        apply_v1beta1_fallback(
            "https://a/apis/discovery.k8s.io/v1/endpointslices?x",
            "endpointslice",
            true
        ),
        "https://a/apis/discovery.k8s.io/v1beta1/endpointslices?x"
    );
    assert_eq!(
        apply_v1beta1_fallback(
            "https://a/apis/networking.k8s.io/v1/ingresses",
            "ingress",
            true
        ),
        "https://a/apis/networking.k8s.io/v1beta1/ingresses"
    );
    // flag off -> untouched
    assert_eq!(
        apply_v1beta1_fallback(
            "https://a/apis/discovery.k8s.io/v1/endpointslices",
            "endpointslice",
            false
        ),
        "https://a/apis/discovery.k8s.io/v1/endpointslices"
    );
}
