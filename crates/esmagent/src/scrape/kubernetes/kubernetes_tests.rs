//! Tests for [`super::KubernetesDiscovery`] — split out of `mod.rs` per this
//! crate's `#[path]`-sibling convention (see `watcher_tests.rs`) to keep
//! both files under the repo's 800-line cap.
//!
//! The stub k8s API server here ([`start_k8s_stub_multi`]) is a small,
//! deliberately narrower duplicate of `watcher_tests.rs`'s `K8sStub` (LIST +
//! immediately-closing WATCH, no request recording) rather than a shared
//! helper — see the task brief's note that a small duplicated stub is
//! acceptable when sharing across sibling test modules would require making
//! that helper `pub`. It routes each LIST to a body by URL-path substring so
//! one stub can serve several kinds at once (endpoints + pods + services),
//! which `start_node_stub` also reuses for its single-kind case.

use std::sync::Arc;
use std::time::{Duration, Instant};

use esm_http::{Request, ResponseWriter, Server};

use super::*;
use crate::scrape::config::KubernetesSdConfig;

/// Polls `check` until it returns `true` or `timeout` elapses. Mirrors
/// `watcher_tests::wait_until`/`manager_tests::wait_until`.
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

/// A stub k8s API server. Every WATCH gets an immediately-closing empty
/// stream (the watcher just resumes/re-watches, no events needed); every LIST
/// is answered with the first `routes` body whose path substring the request
/// path contains, or an empty item list if none matches (so a dependency
/// watcher for an unmodeled kind still lists cleanly instead of error-spinning).
struct K8sStub {
    server: Server,
}

impl K8sStub {
    fn base_url(&self) -> String {
        format!("http://{}", self.server.local_addr())
    }

    fn stop(&self) {
        self.server.stop();
    }
}

/// An empty LIST body for any kind the stub isn't given a route for.
const EMPTY_LIST_BODY: &str = r#"{"metadata":{"resourceVersion":"1"},"items":[]}"#;

/// Starts a stub k8s API server that routes each LIST to a body by URL-path
/// substring — e.g. `("/namespaces/d/endpoints", EPS_BODY)`. One stub can thus
/// serve several kinds at once (endpoints + pods + services) for the
/// dependency-watcher join tests.
fn start_k8s_stub_multi(routes: Vec<(&'static str, &'static str)>) -> K8sStub {
    let routes: Vec<(String, String)> = routes
        .into_iter()
        .map(|(p, b)| (p.to_string(), b.to_string()))
        .collect();
    let server = Server::bind("127.0.0.1:0").expect("bind k8s stub");

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            if req.query().contains("watch=1") {
                w.write_body(b"");
                return;
            }
            let path = req.path();
            let body = routes
                .iter()
                .find(|(p, _)| path.contains(p.as_str()))
                .map(|(_, b)| b.as_str())
                .unwrap_or(EMPTY_LIST_BODY);
            w.write_json(200, body);
        },
    ));

    K8sStub { server }
}

/// Single-`node`-object stub — a `start_k8s_stub_multi` with one route that
/// matches every LIST (the node role's cluster-wide `/nodes` path).
fn start_node_stub() -> K8sStub {
    start_k8s_stub_multi(vec![(
        "/nodes",
        r#"{"metadata":{"resourceVersion":"1"},"items":[
            {"metadata":{"name":"n1"},"spec":{"providerID":"aws:///i-1"},
             "status":{"addresses":[{"type":"InternalIP","address":"10.0.0.5"}],
                       "daemonEndpoints":{"kubeletEndpoint":{"port":10250}}}}]}"#,
    )])
}

/// Builds a `KubernetesSdConfig` with `role` set and every other field at
/// its default (no `api_server`, no auth, no tls, no namespace restriction).
fn k8s_cfg_role(role: &str) -> KubernetesSdConfig {
    KubernetesSdConfig {
        role: role.to_string(),
        ..KubernetesSdConfig::default()
    }
}

#[test]
fn kubernetes_discovery_polls_targets_from_stub() {
    let stub = start_node_stub();
    let mut cfg = k8s_cfg_role("node");
    cfg.api_server = Some(stub.base_url());

    let mut d = KubernetesDiscovery::new(&cfg).unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || !d.poll().is_empty()),
        "KubernetesDiscovery never surfaced the stub's node target"
    );

    stub.stop();
}

/// Endpoints LIST: one ready address in subset, targeting pod `p1`, sharing
/// its name with service `svc1`. Mirrors the `endpoints.rs` unit fixture.
const EPS_LIST_BODY: &str = r#"{"metadata":{"resourceVersion":"1"},"items":[
    {"metadata":{"name":"svc1","namespace":"d"},
     "subsets":[{"addresses":[{"ip":"10.0.0.1",
             "targetRef":{"kind":"Pod","name":"p1","namespace":"d"}}],
         "ports":[{"name":"http","port":8080,"protocol":"TCP"}]}]}]}"#;
const POD_LIST_BODY: &str = r#"{"metadata":{"resourceVersion":"1"},"items":[
    {"metadata":{"name":"p1","namespace":"d","uid":"u1"},
     "spec":{"nodeName":"n1","containers":[{"name":"c","image":"i",
         "ports":[{"name":"http","containerPort":8080,"protocol":"TCP"}]}]},
     "status":{"phase":"Running","podIP":"10.0.0.1","hostIP":"10.0.0.100",
         "conditions":[{"type":"Ready","status":"True"}]}}]}"#;
const SVC_LIST_BODY: &str = r#"{"metadata":{"resourceVersion":"1"},"items":[
    {"metadata":{"name":"svc1","namespace":"d","labels":{"app":"a"}},
     "spec":{"type":"ClusterIP","clusterIP":"10.96.0.1",
         "ports":[{"name":"http","protocol":"TCP","port":8080}]}}]}"#;

#[test]
fn endpoints_discovery_joins_pod_and_service_from_dep_watchers() {
    // Proves the endpoints role's dependency watchers (pod + service, started
    // by `dependency_roles`) populate the shared registry live, so the
    // endpoints builder joins BOTH the service name and the targetRef pod name
    // onto its target group — end to end through `KubernetesDiscovery::new`.
    let stub = start_k8s_stub_multi(vec![
        ("/namespaces/d/endpoints", EPS_LIST_BODY),
        ("/namespaces/d/pods", POD_LIST_BODY),
        ("/namespaces/d/services", SVC_LIST_BODY),
    ]);
    let mut cfg = k8s_cfg_role("endpoints");
    cfg.api_server = Some(stub.base_url());
    cfg.namespaces.names = vec!["d".into()];

    let mut d = KubernetesDiscovery::new(&cfg).unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || {
            d.poll().iter().any(|g| {
                g.labels
                    .get("__meta_kubernetes_service_name")
                    .map(String::as_str)
                    == Some("svc1")
                    && g.labels
                        .get("__meta_kubernetes_pod_name")
                        .map(String::as_str)
                        == Some("p1")
            })
        }),
        "endpoints discovery never joined pod+service from its dependency watchers"
    );

    // Stop the watchers (primary + every dependency) *while the stub is still
    // serving*, so each blocked watch read is answered promptly and the thread
    // observes the stop flag on its next loop turn — rather than blocking on a
    // dead socket until `WATCH_HTTP_TIMEOUT`. Then stop the stub.
    drop(d);
    stub.stop();
}

#[test]
fn new_fails_when_api_server_and_kubeconfig_file_are_both_set() {
    // Regression guard for the fail-fast path documented on `new()`: a
    // genuinely bad config (here, the auth-resolution rejection already
    // covered by `client::resolve_api_config`'s own tests) must propagate as
    // an `Err` from `KubernetesDiscovery::new`, not panic or silently start
    // no watchers.
    let mut cfg = k8s_cfg_role("pod");
    cfg.api_server = Some("https://k8s:6443".into());
    cfg.kubeconfig_file = Some("/x".into());

    let result = KubernetesDiscovery::new(&cfg);
    let err = match result {
        Ok(_) => panic!("expected an Err for api_server + kubeconfig_file both set"),
        Err(e) => e,
    };
    assert!(err.msg.contains("kubeconfig_file"), "{}", err.msg);
}

/// A namespace-file path that is guaranteed not to exist, for the
/// read-failure branch. Under a temp dir so it never collides with a real
/// in-cluster mount (and never depends on the real hardcoded path).
fn missing_namespace_file() -> String {
    let dir = tempfile::tempdir().unwrap();
    dir.path()
        .join("does-not-exist")
        .to_string_lossy()
        .into_owned()
    // `dir` drops here, removing the temp dir — the returned path is now
    // guaranteed absent, which is exactly what the read-failure branch wants.
}

#[test]
fn resolve_namespaces_node_role_is_always_single_cluster_wide() {
    let mut cfg = k8s_cfg_role("node");
    cfg.namespaces.names = vec!["ignored".to_string()];
    // Even with own_namespace set, node ignores namespaces entirely, so the
    // namespace file is never read — pass the real default path to prove it
    // isn't consulted.
    cfg.namespaces.own_namespace = true;
    assert_eq!(
        resolve_namespaces(&cfg, &missing_namespace_file()),
        vec![None]
    );
}

#[test]
fn resolve_namespaces_empty_namespaces_is_single_cluster_wide() {
    let cfg = k8s_cfg_role("pod");
    assert_eq!(
        resolve_namespaces(&cfg, &missing_namespace_file()),
        vec![None]
    );
}

#[test]
fn resolve_namespaces_names_become_one_watcher_each() {
    let mut cfg = k8s_cfg_role("pod");
    cfg.namespaces.names = vec!["a".to_string(), "b".to_string()];
    assert_eq!(
        resolve_namespaces(&cfg, &missing_namespace_file()),
        vec![Some("a".to_string()), Some("b".to_string())]
    );
}

#[test]
fn resolve_namespaces_own_namespace_reads_the_namespace_file() {
    // The success path (previously dead): own_namespace=true + a real,
    // readable namespace file → the resolved namespaces include its trimmed
    // contents.
    let dir = tempfile::tempdir().unwrap();
    let ns_file = dir.path().join("namespace");
    std::fs::write(&ns_file, "team-a\n").unwrap();

    let mut cfg = k8s_cfg_role("pod");
    cfg.namespaces.own_namespace = true;
    assert_eq!(
        resolve_namespaces(&cfg, &ns_file.to_string_lossy()),
        vec![Some("team-a".to_string())]
    );
}

#[test]
fn resolve_namespaces_own_namespace_reads_file_then_appends_names() {
    // own_namespace's file contents come first, then the explicit `names` —
    // one watcher each, in that order.
    let dir = tempfile::tempdir().unwrap();
    let ns_file = dir.path().join("namespace");
    std::fs::write(&ns_file, "own-ns\n").unwrap();

    let mut cfg = k8s_cfg_role("pod");
    cfg.namespaces.own_namespace = true;
    cfg.namespaces.names = vec!["extra".to_string()];
    assert_eq!(
        resolve_namespaces(&cfg, &ns_file.to_string_lossy()),
        vec![Some("own-ns".to_string()), Some("extra".to_string())]
    );
}

#[test]
fn resolve_namespaces_own_namespace_read_failure_is_skipped_not_fatal() {
    // `own_namespace` set but the namespace file (an overridable path here,
    // guaranteed absent) can't be read: must not panic, and (with no `names`
    // either) must fall back to a single cluster-wide watcher rather than an
    // empty `Vec` (zero watchers would silently discover nothing forever).
    let mut cfg = k8s_cfg_role("pod");
    cfg.namespaces.own_namespace = true;
    assert_eq!(
        resolve_namespaces(&cfg, &missing_namespace_file()),
        vec![None]
    );
}
