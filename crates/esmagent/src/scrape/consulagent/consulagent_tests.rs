//! Stub-server tests for [`super::ConsulagentDiscovery`] — split out per this
//! crate's `#[path]`-sibling convention to keep `mod.rs` under the 800-line
//! cap.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_http::{Request, ResponseWriter, Server};

use super::*;
use crate::scrape::config::ConsulagentSdConfig;

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
struct ConsulagentStub {
    server: Server,
    requests: Arc<Mutex<Vec<String>>>,
}

impl ConsulagentStub {
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

/// Starts a stub local Consul agent serving `/v1/agent/self` (datacenter
/// `dc1`, node `node-a`, member `9.9.9.9`), `/v1/agent/services` (services
/// `web` in `dc1` and `db` in `dc1`), and
/// `/v1/agent/health/service/name/web` (one node). A request for any other
/// health path yields `[]`.
fn start_consulagent_stub() -> ConsulagentStub {
    let server = Server::bind("127.0.0.1:0").expect("bind consulagent stub");
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
                w.write_json(
                    200,
                    r#"{"Member":{"Addr":"9.9.9.9"},
                        "Config":{"Datacenter":"dc1","NodeName":"node-a"},
                        "Meta":{"instance_type":"t2.medium"}}"#,
                );
            } else if path == "/v1/agent/services" {
                w.write_json(
                    200,
                    r#"{"web-1":{"ID":"web-1","Service":"web","Datacenter":"dc1"},
                        "db-1":{"ID":"db-1","Service":"db","Datacenter":"dc1"}}"#,
                );
            } else if path == "/v1/agent/health/service/name/web" {
                w.write_json(
                    200,
                    r#"[{"Service":{"ID":"web-1","Service":"web","Address":"1.2.3.4","Port":8080,
                        "Tags":["prod"]},
                        "Checks":[{"CheckID":"serviceCheck","Status":"passing"}]}]"#,
                );
            } else {
                w.write_json(200, "[]");
            }
        },
    ));

    ConsulagentStub { server, requests }
}

/// `services: [web]` allowlist keeps `web` and filters `db` out; the stub must
/// yield exactly the `web` target with its `__meta_consulagent_service` and
/// `__address__`, using the local-agent endpoints, and never fetch `db`'s
/// nodes.
#[test]
fn discovers_web_target_and_filters_db() {
    let stub = start_consulagent_stub();
    let cfg = ConsulagentSdConfig {
        server: stub.addr(),
        services: vec!["web".to_string()],
        refresh_interval: Duration::from_millis(50),
        ..ConsulagentSdConfig::default()
    };

    let mut d = ConsulagentDiscovery::new(&cfg, "job").expect("new");

    let found = wait_until(Duration::from_secs(5), || {
        d.poll().iter().any(|g| {
            g.labels
                .get("__meta_consulagent_service")
                .map(String::as_str)
                == Some("web")
        })
    });
    assert!(
        found,
        "web target never discovered; requests={:?}",
        stub.requests()
    );

    let groups = d.poll();
    let web = groups
        .iter()
        .find(|g| {
            g.labels
                .get("__meta_consulagent_service")
                .map(String::as_str)
                == Some("web")
        })
        .expect("web group");
    assert_eq!(web.targets, vec!["1.2.3.4:8080".to_string()]);
    assert_eq!(web.labels["__meta_consulagent_health"], "passing");
    assert_eq!(web.labels["__meta_consulagent_dc"], "dc1");
    assert_eq!(web.labels["__meta_consulagent_node"], "node-a");
    assert_eq!(web.labels["__meta_consulagent_address"], "9.9.9.9");
    assert_eq!(web.source, "job/consulagent/dc1/web");

    // db was filtered out by the services allowlist: no db group, and its
    // node endpoint was never hit.
    assert!(groups.iter().all(|g| {
        g.labels
            .get("__meta_consulagent_service")
            .map(String::as_str)
            != Some("db")
    }));
    assert!(
        !stub
            .requests()
            .iter()
            .any(|r| r.starts_with("/v1/agent/health/service/name/db")),
        "db nodes should never be fetched; requests={:?}",
        stub.requests()
    );

    // Clean stop: dropping the discovery joins the refresh thread promptly.
    drop(d);
    stub.stop();
}

/// A service registered in a different datacenter than the resolved agent
/// datacenter is skipped entirely (matching upstream `getServiceNames`'s
/// `service.Datacenter != cw.watchDatacenter` filter).
#[test]
fn skips_service_in_other_datacenter() {
    let server = Server::bind("127.0.0.1:0").expect("bind stub");
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let path = req.path().to_string();
            if path == "/v1/agent/self" {
                w.write_json(
                    200,
                    r#"{"Member":{"Addr":"9.9.9.9"},"Config":{"Datacenter":"dc1","NodeName":"n"}}"#,
                );
            } else if path == "/v1/agent/services" {
                // `other` service lives in dc2 -> must be skipped.
                w.write_json(
                    200,
                    r#"{"o-1":{"ID":"o-1","Service":"other","Datacenter":"dc2"}}"#,
                );
            } else {
                w.write_json(200, "[]");
            }
        },
    ));
    let cfg = ConsulagentSdConfig {
        server: server.local_addr().to_string(),
        refresh_interval: Duration::from_millis(50),
        ..ConsulagentSdConfig::default()
    };
    let mut d = ConsulagentDiscovery::new(&cfg, "job").expect("new");

    // Give the refresh loop time to run a few iterations, then assert nothing
    // was discovered (the only service is in the wrong datacenter).
    std::thread::sleep(Duration::from_millis(300));
    assert!(d.poll().is_empty(), "no target should be discovered");

    drop(d);
    server.stop();
}

/// [`resolve_namespace`] fallback: a config namespace always wins over the env
/// var; an absent/empty config namespace falls back to `CONSUL_NAMESPACE`
/// (empty when that's unset too). All cases run in one test — sequentially
/// set/removed — so this doesn't race other tests over the shared
/// `CONSUL_NAMESPACE` process env var.
#[test]
fn resolve_namespace_prefers_config_then_env_then_empty() {
    std::env::set_var("CONSUL_NAMESPACE", "env-ns-should-be-ignored");
    assert_eq!(resolve_namespace(Some("cfg-ns")), "cfg-ns");

    std::env::set_var("CONSUL_NAMESPACE", "env-ns-value-esmagent-consulagent-test");
    assert_eq!(
        resolve_namespace(None),
        "env-ns-value-esmagent-consulagent-test"
    );
    assert_eq!(
        resolve_namespace(Some("")),
        "env-ns-value-esmagent-consulagent-test"
    );

    std::env::remove_var("CONSUL_NAMESPACE");
    assert_eq!(resolve_namespace(None), "");
}

/// The services query carries `filter` and `ns` (alphabetical, `url.Values`
/// order) when set, and is a bare `?` when neither is set.
#[test]
fn services_query_shape() {
    assert_eq!(build_services_query("", ""), "?");
    assert_eq!(build_services_query("ns1", ""), "?ns=ns1");
    assert_eq!(
        build_services_query("ns1", "Service==web"),
        "?filter=Service%3D%3Dweb&ns=ns1"
    );
}
