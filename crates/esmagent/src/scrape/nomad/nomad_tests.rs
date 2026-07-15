//! Stub-server tests for [`super::NomadDiscovery`] — split out per this
//! crate's `#[path]`-sibling convention to keep `mod.rs` under the 800-line
//! cap.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_http::{Request, ResponseWriter, Server};

use super::*;
use crate::scrape::config::NomadSdConfig;

/// Polls `check` until it returns `true` or `timeout` elapses. Bounds every
/// wait so a wiring bug fails fast instead of hanging the suite.
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

/// A running stub Nomad agent. `requests` records every request's
/// `path?query` in arrival order.
struct NomadStub {
    server: Server,
    requests: Arc<Mutex<Vec<String>>>,
}

impl NomadStub {
    fn addr(&self) -> String {
        self.server.local_addr().to_string()
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }

    fn stop(&self) {
        self.server.stop();
    }
}

/// Starts a stub Nomad agent serving `/v1/services` (two services `web` and
/// `db`) and `/v1/service/<name>` (one registration each). A request for any
/// other service path yields `[]`.
fn start_nomad_stub() -> NomadStub {
    let server = Server::bind("127.0.0.1:0").expect("bind nomad stub");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_for_handler = Arc::clone(&requests);

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let path = req.path().to_string();
            requests_for_handler
                .lock()
                .unwrap()
                .push(format!("{}?{}", path, req.query()));

            if path == "/v1/services" {
                w.write_json(
                    200,
                    r#"[{"Namespace":"default","Services":[
                        {"ServiceName":"web","Tags":["prod"]},
                        {"ServiceName":"db","Tags":["prod"]}
                    ]}]"#,
                );
            } else if path == "/v1/service/web" {
                w.write_json(
                    200,
                    r#"[{"ID":"web-1","ServiceName":"web","Namespace":"default",
                        "NodeID":"node-a","Datacenter":"dc1","JobID":"web-job",
                        "AllocID":"alloc-1","Tags":["prod"],
                        "Address":"1.2.3.4","Port":8080}]"#,
                );
            } else if path == "/v1/service/db" {
                w.write_json(
                    200,
                    r#"[{"ID":"db-1","ServiceName":"db","Namespace":"default",
                        "NodeID":"node-b","Datacenter":"dc1","JobID":"db-job",
                        "AllocID":"alloc-2","Tags":["prod"],
                        "Address":"5.6.7.8","Port":5432}]"#,
                );
            } else {
                w.write_json(200, "[]");
            }
        },
    ));

    NomadStub { server, requests }
}

/// The stub lists two services; both must surface as targets with the
/// correct `__address__` and `__meta_nomad_service`.
#[test]
fn discovers_both_service_targets() {
    let stub = start_nomad_stub();
    let cfg = NomadSdConfig {
        server: stub.addr(),
        refresh_interval: Duration::from_millis(50),
        ..NomadSdConfig::default()
    };

    let mut d = NomadDiscovery::new(&cfg, "job").expect("new");

    let both = wait_until(Duration::from_secs(5), || {
        let g = d.poll();
        let has = |name: &str| {
            g.iter()
                .any(|t| t.labels.get("__meta_nomad_service").map(String::as_str) == Some(name))
        };
        has("web") && has("db")
    });
    assert!(
        both,
        "both service targets never discovered; requests={:?}",
        stub.requests()
    );

    let groups = d.poll();
    let web = groups
        .iter()
        .find(|g| g.labels.get("__meta_nomad_service").map(String::as_str) == Some("web"))
        .expect("web group");
    assert_eq!(web.targets, vec!["1.2.3.4:8080".to_string()]);
    assert_eq!(web.labels["__meta_nomad_service_id"], "web-1");
    assert_eq!(web.labels["__meta_nomad_dc"], "dc1");
    assert_eq!(web.source, "job/nomad/web");

    let db = groups
        .iter()
        .find(|g| g.labels.get("__meta_nomad_service").map(String::as_str) == Some("db"))
        .expect("db group");
    assert_eq!(db.targets, vec!["5.6.7.8:5432".to_string()]);
    assert_eq!(db.source, "job/nomad/db");

    // Clean stop: dropping the discovery joins the refresh thread promptly.
    drop(d);
    stub.stop();
}

/// [`build_query_args`] honors `allow_stale` (default-on) plus namespace and
/// region, and the refresh loop actually sends those args to the stub.
#[test]
fn sends_stale_namespace_region_query_args() {
    let stub = start_nomad_stub();
    let cfg = NomadSdConfig {
        server: stub.addr(),
        namespace: Some("prod".into()),
        region: Some("eu".into()),
        refresh_interval: Duration::from_millis(50),
        ..NomadSdConfig::default()
    };

    let d = NomadDiscovery::new(&cfg, "job").expect("new");
    let listed = wait_until(Duration::from_secs(5), || {
        stub.requests()
            .iter()
            .any(|r| r.starts_with("/v1/services?"))
    });
    assert!(
        listed,
        "services never listed; requests={:?}",
        stub.requests()
    );

    let list_req = stub
        .requests()
        .into_iter()
        .find(|r| r.starts_with("/v1/services?"))
        .unwrap();
    assert!(list_req.contains("stale="), "{list_req}");
    assert!(list_req.contains("namespace=prod"), "{list_req}");
    assert!(list_req.contains("region=eu"), "{list_req}");

    drop(d);
    stub.stop();
}

/// `resolve_region` upstream-parity fallback: config wins; absent/empty falls
/// back to `NOMAD_REGION`; both absent defaults to `global`. All cases run in
/// one test — sequentially set/removed — so this doesn't race other tests
/// over the shared process env var.
#[test]
fn resolve_region_prefers_config_then_env_then_global() {
    std::env::set_var("NOMAD_REGION", "env-region-should-be-ignored");
    assert_eq!(resolve_region(Some("cfg-region")), "cfg-region");

    std::env::set_var("NOMAD_REGION", "env-region-esmagent-nomad-test");
    assert_eq!(resolve_region(None), "env-region-esmagent-nomad-test");
    assert_eq!(resolve_region(Some("")), "env-region-esmagent-nomad-test");

    std::env::remove_var("NOMAD_REGION");
    assert_eq!(resolve_region(None), "global");
}

/// `resolve_namespace` upstream-parity fallback: config wins; absent/empty
/// falls back to `NOMAD_NAMESPACE` (empty when unset too).
#[test]
fn resolve_namespace_prefers_config_then_env_then_empty() {
    std::env::set_var("NOMAD_NAMESPACE", "env-ns-should-be-ignored");
    assert_eq!(resolve_namespace(Some("cfg-ns")), "cfg-ns");

    std::env::set_var("NOMAD_NAMESPACE", "env-ns-esmagent-nomad-test");
    assert_eq!(resolve_namespace(None), "env-ns-esmagent-nomad-test");
    assert_eq!(resolve_namespace(Some("")), "env-ns-esmagent-nomad-test");

    std::env::remove_var("NOMAD_NAMESPACE");
    assert_eq!(resolve_namespace(None), "");
}
