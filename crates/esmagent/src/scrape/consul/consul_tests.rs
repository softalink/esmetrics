//! Stub-server tests for [`super::ConsulDiscovery`] — split out per this
//! crate's `#[path]`-sibling convention to keep `mod.rs` under the 800-line
//! cap.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_http::{Request, ResponseWriter, Server};

use super::*;
use crate::scrape::config::ConsulSdConfig;

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

/// A running stub Consul agent. `requests` records every request's
/// `path?query` in arrival order.
struct ConsulStub {
    server: Server,
    requests: Arc<Mutex<Vec<String>>>,
}

impl ConsulStub {
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

/// Starts a stub Consul agent serving `/v1/agent/self` (datacenter `dc1`),
/// `/v1/catalog/services` (services `web` and `db`, both tagged `prod`), and
/// `/v1/health/service/web` (one node). A request for any other
/// health-service path yields `[]`.
fn start_consul_stub() -> ConsulStub {
    let server = Server::bind("127.0.0.1:0").expect("bind consul stub");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_for_handler = Arc::clone(&requests);

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let path = req.path().to_string();
            requests_for_handler
                .lock()
                .unwrap()
                .push(format!("{}?{}", path, req.query()));

            if path == "/v1/agent/self" {
                w.write_json(200, r#"{"Config":{"Datacenter":"dc1"}}"#);
            } else if path == "/v1/catalog/services" {
                w.write_json(200, r#"{"web":["prod"],"db":["prod"]}"#);
            } else if path == "/v1/health/service/web" {
                w.write_json(
                    200,
                    r#"[{"Service":{"ID":"web-1","Service":"web","Address":"1.2.3.4","Port":8080,
                        "Tags":["prod"]},
                        "Node":{"Address":"9.9.9.9","Datacenter":"dc1","Node":"node-a"},
                        "Checks":[{"CheckID":"serviceCheck","Status":"passing"}]}]"#,
                );
            } else {
                w.write_json(200, "[]");
            }
        },
    ));

    ConsulStub { server, requests }
}

/// `services: [web]` allowlist keeps `web` and filters `db` out; the stub
/// must yield exactly the `web` target with its `__meta_consul_service` and
/// `__address__`, and never fetch `db`'s nodes.
#[test]
fn discovers_web_target_and_filters_db() {
    let stub = start_consul_stub();
    let cfg = ConsulSdConfig {
        server: stub.addr(),
        services: vec!["web".to_string()],
        tags: vec!["prod".to_string()],
        refresh_interval: Duration::from_millis(50),
        ..ConsulSdConfig::default()
    };

    let mut d = ConsulDiscovery::new(&cfg, "job").expect("new");

    let found = wait_until(Duration::from_secs(5), || {
        d.poll()
            .iter()
            .any(|g| g.labels.get("__meta_consul_service").map(String::as_str) == Some("web"))
    });
    assert!(
        found,
        "web target never discovered; requests={:?}",
        stub.requests()
    );

    let groups = d.poll();
    let web = groups
        .iter()
        .find(|g| g.labels.get("__meta_consul_service").map(String::as_str) == Some("web"))
        .expect("web group");
    assert_eq!(web.targets, vec!["1.2.3.4:8080".to_string()]);
    assert_eq!(web.labels["__meta_consul_health"], "passing");
    assert_eq!(web.source, "job/consul/dc1/web");

    // db was filtered out by the services allowlist: no db group, and its
    // node endpoint was never hit.
    assert!(groups
        .iter()
        .all(|g| g.labels.get("__meta_consul_service").map(String::as_str) != Some("db")));
    assert!(
        !stub
            .requests()
            .iter()
            .any(|r| r.starts_with("/v1/health/service/db")),
        "db nodes should never be fetched; requests={:?}",
        stub.requests()
    );

    // Clean stop: dropping the discovery joins the refresh thread promptly.
    drop(d);
    stub.stop();
}

/// [`resolve_namespace`] upstream-parity fallback (`api.go` lines 99-101):
/// a config namespace always wins over the env var; an absent/empty config
/// namespace falls back to `CONSUL_NAMESPACE` (empty when that's unset too).
/// All three cases run in one test — sequentially set/removed — so this
/// doesn't race other tests over the shared `CONSUL_NAMESPACE` process env
/// var.
#[test]
fn resolve_namespace_prefers_config_then_env_then_empty() {
    // Config namespace present: env is ignored even when set.
    std::env::set_var("CONSUL_NAMESPACE", "env-ns-should-be-ignored");
    assert_eq!(resolve_namespace(Some("cfg-ns")), "cfg-ns");

    // Config namespace absent, env set: env value is used.
    std::env::set_var("CONSUL_NAMESPACE", "env-ns-value-esmagent-consul-test");
    assert_eq!(resolve_namespace(None), "env-ns-value-esmagent-consul-test");

    // Config namespace empty string, env set: still falls back to env.
    assert_eq!(
        resolve_namespace(Some("")),
        "env-ns-value-esmagent-consul-test"
    );

    // Both absent: empty string.
    std::env::remove_var("CONSUL_NAMESPACE");
    assert_eq!(resolve_namespace(None), "");
}
