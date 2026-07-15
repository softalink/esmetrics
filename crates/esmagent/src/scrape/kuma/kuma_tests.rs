//! Stub-server tests for [`super::KumaDiscovery`] — split out per this crate's
//! `#[path]`-sibling convention to keep `mod.rs` under the 800-line cap.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_http::{Method, Request, ResponseWriter, Server};

use super::*;
use crate::scrape::config::KumaSdConfig;

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

/// A running stub Kuma control plane. `bodies` records the POST body of every
/// request to the MADS path, in arrival order.
struct KumaStub {
    server: Server,
    bodies: Arc<Mutex<Vec<String>>>,
}

impl KumaStub {
    /// The `http://host:port` base URL a config's `server` should point at.
    fn url(&self) -> String {
        format!("http://{}", self.server.local_addr())
    }

    fn bodies(&self) -> Vec<String> {
        self.bodies.lock().unwrap().clone()
    }

    fn stop(&self) {
        self.server.stop();
    }
}

/// The two-assignment `DiscoveryResponse` fixture from upstream `api_test.go`.
const DISCOVERY_JSON: &str = r#"{
    "version_info":"5dc9a5dd-2091-4426-a886-dfdc24fc99d7",
    "resources":[
       { "@type":"type.googleapis.com/kuma.observability.v1.MonitoringAssignment",
         "mesh":"default","service":"redis","labels":{"test":"test1"},
         "targets":[{"name":"redis","scheme":"http","address":"127.0.0.1:5670",
           "metrics_path":"/metrics","labels":{"kuma_io_protocol":"tcp"}}] },
       { "@type":"type.googleapis.com/kuma.observability.v1.MonitoringAssignment",
         "mesh":"default","service":"app","labels":{"test":"test2"},
         "targets":[{"name":"app","scheme":"https","address":"127.0.0.1:5671",
           "metrics_path":"/metrics/abc","labels":{"kuma_io_protocol":"http"}}] }
    ],
    "type_url":"type.googleapis.com/kuma.observability.v1.MonitoringAssignment",
    "nonce":"foobar"
 }"#;

const MADS_PATH: &str = "/v3/discovery:monitoringassignments";

/// Starts a stub Kuma control plane. A `POST` to the MADS path records its
/// body and returns [`DISCOVERY_JSON`]; any other request yields `{}`.
fn start_kuma_stub() -> KumaStub {
    let server = Server::bind("127.0.0.1:0").expect("bind kuma stub");
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let bodies_for_handler = Arc::clone(&bodies);

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let is_mads = req.method() == Method::Post && req.path() == MADS_PATH;
            if is_mads {
                let mut buf = Vec::new();
                let _ = req.read_body_to(&mut buf, 1 << 20);
                bodies_for_handler
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buf).into_owned());
                w.write_json(200, DISCOVERY_JSON);
            } else {
                w.write_json(200, "{}");
            }
        },
    ));

    KumaStub { server, bodies }
}

/// The stub returns two assignments; each must surface as its own
/// single-target group with the correct `__address__` and `__meta_kuma_*`
/// labels, and the POST body must carry the faithful `DiscoveryRequest` shape.
#[test]
fn discovers_targets_and_posts_discovery_request() {
    let stub = start_kuma_stub();
    let cfg = KumaSdConfig {
        server: stub.url(),
        client_id: "my-client".to_string(),
        refresh_interval: Duration::from_millis(50),
        ..KumaSdConfig::default()
    };

    let mut d = KumaDiscovery::new(&cfg, "job").expect("new");

    let found = wait_until(Duration::from_secs(5), || d.poll().len() == 2);
    assert!(
        found,
        "two targets never discovered; bodies={:?}",
        stub.bodies()
    );

    let mut groups = d.poll();
    groups.sort_by(|a, b| a.targets.cmp(&b.targets));
    assert_eq!(groups.len(), 2);

    let redis = &groups[0];
    assert_eq!(redis.targets, vec!["127.0.0.1:5670".to_string()]);
    assert_eq!(redis.labels["__meta_kuma_service"], "redis");
    assert_eq!(redis.labels["__meta_kuma_mesh"], "default");
    assert_eq!(redis.labels["__meta_kuma_dataplane"], "redis");
    assert_eq!(redis.labels["__meta_kuma_label_kuma_io_protocol"], "tcp");
    assert_eq!(redis.labels["__meta_kuma_label_test"], "test1");
    assert_eq!(redis.labels["__scheme__"], "http");
    assert_eq!(redis.labels["__metrics_path__"], "/metrics");
    assert_eq!(redis.labels["instance"], "redis");
    assert!(!redis.labels.contains_key("__address__"));
    assert_eq!(redis.source, "job/kuma");

    let app = &groups[1];
    assert_eq!(app.targets, vec!["127.0.0.1:5671".to_string()]);
    assert_eq!(app.labels["__meta_kuma_service"], "app");
    assert_eq!(app.labels["__scheme__"], "https");
    assert_eq!(app.labels["__metrics_path__"], "/metrics/abc");

    // The POST body must carry the DiscoveryRequest shape with node.id and
    // the MonitoringAssignment type_url.
    let body = stub.bodies().into_iter().next().expect("a MADS POST body");
    let parsed: serde_json::Value = serde_json::from_str(&body).expect("body is JSON");
    assert_eq!(
        parsed["node"]["id"].as_str(),
        Some("my-client"),
        "body={body}"
    );
    assert_eq!(
        parsed["type_url"].as_str(),
        Some("type.googleapis.com/kuma.observability.v1.MonitoringAssignment"),
        "body={body}"
    );
    assert!(parsed.get("version_info").is_some());
    assert!(parsed.get("response_nonce").is_some());

    // Clean stop: dropping the discovery joins the refresh thread promptly.
    drop(d);
    stub.stop();
}

/// A programmatically-built config with a missing/bad `server` fails `new()`
/// (upstream `newAPIConfig` contract), rather than spawning a doomed refresh
/// thread.
#[test]
fn new_rejects_bad_config() {
    let empty = KumaSdConfig {
        server: String::new(),
        ..KumaSdConfig::default()
    };
    assert!(KumaDiscovery::new(&empty, "job").is_err());

    let bad = KumaSdConfig {
        server: ":".to_string(),
        ..KumaSdConfig::default()
    };
    assert!(KumaDiscovery::new(&bad, "job").is_err());
}
