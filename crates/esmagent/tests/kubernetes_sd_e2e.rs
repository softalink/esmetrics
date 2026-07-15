//! End-to-end test for esmagent's Kubernetes service discovery
//! (`kubernetes_sd_configs`): drives the real [`esmagent::run`] pipeline
//! (k8s watcher -> `KubernetesDiscovery` -> the SAME target-relabel ->
//! scrape -> global-relabel -> `Fanout` -> `RemoteWriteCtx` forwarding path
//! `tests/scrape_e2e.rs` exercises for static targets) against a stub k8s
//! API server, a stub `/metrics` target, and a stub remote-write
//! destination. Mirrors `tests/scrape_e2e.rs`'s harness style (in-process
//! mock servers, bounded polling for every capture, snappy/protobuf decode
//! of the captured remote-write bodies, no sleep-only assertions) and the
//! k8s stub-server pattern from
//! `src/scrape/kubernetes/watcher_tests.rs::start_k8s_stub` (that module is
//! an inline `#[cfg(test)]` sibling of `watcher.rs`, not reachable from an
//! integration-test binary, so a small stand-alone stub is rebuilt here).
//!
//! Scenario:
//! 1. A stub `/metrics` target serves two Prometheus exposition series.
//! 2. A stub k8s API server answers the `node` role's LIST request with one
//!    `Node` object whose `InternalIP` address + `kubeletEndpoint.port`
//!    resolve to the `/metrics` stub's `host:port` (matching
//!    `roles::node::node_target_groups`'s address-selection + `__address__`
//!    construction). Its WATCH request answers with an empty body (a clean
//!    EOF the watcher resumes-watch from, per
//!    `watcher_resumes_watch_from_resource_version_without_re_listing`) —
//!    the initial LIST alone is enough to produce the target.
//! 3. A stub remote-write destination captures every `/api/v1/write` body
//!    it receives (answering `204`).
//! 4. `esmagent::run` starts against a one-job `-promscrape.config`: a
//!    `kubernetes_sd_configs: [{role: node, api_server: <k8s stub>}]`
//!    entry, plus a `relabel_configs` rule that copies
//!    `__meta_kubernetes_node_name` into a plain `k8s_node` label — proving
//!    k8s SD's `__meta_kubernetes_*` labels reach target-relabel, not just
//!    that a target gets scraped at all.
//! 5. The destination stub is polled until it has received the two scraped
//!    series and the `up=1` auto-metric, all carrying `job`/`instance` (the
//!    node's name, per `node_target_groups`'s `instance` label) plus the
//!    relabel-derived `k8s_node` label.
//! 6. `GET /api/v1/targets` on `app.local_addr()` confirms the discovered
//!    target is reported `health: "up"`.
//! 7. `app.stop()` + the k8s/metrics/destination stubs are stopped; temp
//!    dirs cleaned up.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_http::{Request, ResponseWriter, Server};

use esmagent::flags::Flags;

/// Polls `check` until it returns `true` or `timeout` elapses. Bounds every
/// wait in this file so a wiring bug fails the test fast instead of hanging
/// the suite (duplicated from `tests/scrape_e2e.rs`'s `wait_until` — private
/// to its own module, same rationale as this crate's other per-file test
/// helper duplication).
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

/// Serves a fixed Prometheus exposition body on `/metrics`; any other path
/// gets 404. Mirrors `tests/scrape_e2e.rs`'s `start_metrics_stub`.
fn start_metrics_stub(body: &'static str) -> Server {
    let server = Server::bind("127.0.0.1:0").expect("bind metrics stub");
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            if req.path() == "/metrics" {
                w.set_content_type("text/plain; version=0.0.4");
                w.write_body(body.as_bytes());
            } else {
                w.write_status(404);
            }
        },
    ));
    server
}

/// A remote-write destination stub: captures every request body it
/// accepts and always answers `204`. Mirrors `tests/scrape_e2e.rs`'s
/// `DestStub`.
struct DestStub {
    server: Server,
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
}

fn start_dest_stub() -> DestStub {
    let server = Server::bind("127.0.0.1:0").expect("bind destination stub");
    let bodies: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let bodies_for_handler = Arc::clone(&bodies);
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let mut body = Vec::new();
            req.read_body_to(&mut body, 1 << 20).ok();
            bodies_for_handler.lock().unwrap().push(body);
            w.write_status(204);
        },
    ));
    DestStub { server, bodies }
}

/// A stub Kubernetes API server: every request whose query string carries
/// `watch=1` gets an empty 200 body (a clean EOF — the watcher resumes the
/// watch from the tracked resourceVersion rather than erroring); every other
/// request (the LIST) gets `list_body` as a JSON `200`. Mirrors
/// `src/scrape/kubernetes/watcher_tests.rs::start_k8s_stub`, narrowed to
/// this test's single-script needs (no request-sequence recording).
fn start_k8s_stub(list_body: String) -> Server {
    let server = Server::bind("127.0.0.1:0").expect("bind k8s stub");
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            if req.query().contains("watch=1") {
                w.write_body(&[]);
            } else {
                w.write_json(200, &list_body);
            }
        },
    ));
    server
}

/// A multi-resource stub Kubernetes API server for the `endpoints` role,
/// which fans out several dependency watchers (its own `endpoints` LIST plus
/// `pods`/`services` in the same namespace and cluster-wide `nodes` for
/// `attach_metadata: {node: true}`). Each `routes` entry maps a resource-path
/// suffix (`/endpoints`, `/pods`, `/services`, `/nodes` — the tail of
/// `client::resource_path`) to that resource's LIST body. `watch=1` requests
/// get a clean empty EOF (as in [`start_k8s_stub`]); a LIST whose path matches
/// no route gets an empty-items list so an unexpected watcher can't hang.
fn start_k8s_multipath_stub(routes: Vec<(&'static str, String)>) -> Server {
    let server = Server::bind("127.0.0.1:0").expect("bind k8s multipath stub");
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            if req.query().contains("watch=1") {
                w.write_body(&[]);
                return;
            }
            let path = req.path();
            match routes.iter().find(|(suffix, _)| path.ends_with(suffix)) {
                Some((_, body)) => w.write_json(200, body),
                None => w.write_json(200, r#"{"metadata":{"resourceVersion":"1"},"items":[]}"#),
            }
        },
    ));
    server
}

/// One decoded remote-write sample: its metric name (from `__name__`), its
/// full label set, and its value. Mirrors `tests/scrape_e2e.rs`'s
/// `DecodedSample`.
struct DecodedSample {
    name: String,
    labels: HashMap<String, String>,
    value: f64,
}

/// Snappy-decompresses and protobuf-decodes every captured body, flattening
/// every `(series, sample)` pair across all of them into one `Vec`.
fn decode_bodies(bodies: &[Vec<u8>]) -> Vec<DecodedSample> {
    let mut out = Vec::new();
    for body in bodies {
        let raw = snap::raw::Decoder::new()
            .decompress_vec(body)
            .expect("snappy decompress captured body");
        let wr = esm_protoparser::prompb::unmarshal_write_request(&raw)
            .expect("decode captured write request");
        for ts in &wr.timeseries {
            let labels: HashMap<String, String> = ts
                .labels
                .iter()
                .map(|l| {
                    (
                        String::from_utf8_lossy(l.name).into_owned(),
                        String::from_utf8_lossy(l.value).into_owned(),
                    )
                })
                .collect();
            let name = labels.get("__name__").cloned().unwrap_or_default();
            for s in &ts.samples {
                out.push(DecodedSample {
                    name: name.clone(),
                    labels: labels.clone(),
                    value: s.value,
                });
            }
        }
    }
    out
}

/// Whether `samples` contains an entry named `name` whose value satisfies
/// `value_ok` and whose labels contain every `(key, value)` pair in
/// `want_labels`.
fn has_sample(
    samples: &[DecodedSample],
    name: &str,
    value_ok: impl Fn(f64) -> bool,
    want_labels: &[(&str, &str)],
) -> bool {
    samples.iter().any(|s| {
        s.name == name
            && value_ok(s.value)
            && want_labels
                .iter()
                .all(|(k, v)| s.labels.get(*k).map(|got| got == v).unwrap_or(false))
    })
}

/// Minimal raw HTTP/1.1 GET client, mirroring `tests/scrape_e2e.rs`'s
/// `http_get` (private to that module, so duplicated here).
fn http_get(addr: SocketAddr, target: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect to esmagent");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set_read_timeout");
    let req = format!("GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).expect("write request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed response: {response:?}"));
    let status_line = head.lines().next().unwrap_or_default();
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (status, body.to_string())
}

const JOB_NAME: &str = "esmagent_e2e_k8s_sd";
const NODE_NAME: &str = "e2e-node";

#[test]
fn kubernetes_sd_forwards_series_and_reports_target_up() {
    let dest = start_dest_stub();
    let dest_addr = dest.server.local_addr();

    let metrics_server = start_metrics_stub("foo_metric{code=\"200\"} 5\nbar_metric 7\n");
    let metrics_addr = metrics_server.local_addr();

    // One `node` object whose InternalIP + kubeletEndpoint.port resolve to
    // the `/metrics` stub's address, matching `roles::node::node_target_groups`
    // (`__address__` = join_host_port(InternalIP, kubeletEndpoint.port)).
    let list_body = format!(
        r#"{{"metadata":{{"resourceVersion":"1"}},"items":[
            {{"metadata":{{"name":"{NODE_NAME}"}},
             "spec":{{}},
             "status":{{"addresses":[{{"type":"InternalIP","address":"{ip}"}}],
                        "daemonEndpoints":{{"kubeletEndpoint":{{"port":{port}}}}}}}}}]}}"#,
        ip = metrics_addr.ip(),
        port = metrics_addr.port(),
    );
    let k8s_server = start_k8s_stub(list_body);
    let k8s_addr = k8s_server.local_addr();

    let tmp = tempfile::tempdir().expect("tmp data path");
    let scrape_cfg_path = tmp.path().join("scrape.yml");
    // `scrape_interval`/`scrape_timeout` short (as in `scrape_e2e.rs`) so
    // the test doesn't need multi-second waits. The `relabel_configs` rule
    // copies `__meta_kubernetes_node_name` into `k8s_node` — proving the
    // k8s SD `__meta_kubernetes_*` labels reach target-relabel, not just
    // that some target got scraped.
    std::fs::write(
        &scrape_cfg_path,
        format!(
            "scrape_configs:\n\
             \x20 - job_name: {JOB_NAME}\n\
             \x20   scrape_interval: 1s\n\
             \x20   scrape_timeout: 500ms\n\
             \x20   kubernetes_sd_configs:\n\
             \x20     - role: node\n\
             \x20       api_server: 'http://{k8s_addr}'\n\
             \x20   relabel_configs:\n\
             \x20     - source_labels: [__meta_kubernetes_node_name]\n\
             \x20       target_label: k8s_node\n"
        ),
    )
    .expect("write scrape config");

    let flags = Flags {
        remote_write_urls: vec![format!("http://{dest_addr}/api/v1/write")],
        remote_write_tmp_data_path: tmp.path().to_string_lossy().to_string(),
        remote_write_max_block_size: 1,
        http_listen_addr: "127.0.0.1:0".to_string(),
        promscrape_config: Some(scrape_cfg_path.to_string_lossy().to_string()),
        ..Flags::default()
    };

    let app = esmagent::run(&flags).expect("esmagent::run should succeed");
    let agent_addr = app.local_addr();

    // `node_target_groups` sets `instance` to the node's name (it overrides
    // the address-based default before target-relabel even runs — see
    // `assemble_labels`/`build_targets` in `src/scrape/target.rs`).
    let target_labels: &[(&str, &str)] = &[
        ("job", JOB_NAME),
        ("instance", NODE_NAME),
        ("k8s_node", NODE_NAME),
    ];

    // --- The destination receives the scraped series and up=1, all
    // carrying job/instance + the relabel-derived k8s_node label. ---
    assert!(
        wait_until(Duration::from_secs(15), || {
            let samples = decode_bodies(&dest.bodies.lock().unwrap());
            has_sample(&samples, "foo_metric", |v| v == 5.0, target_labels)
                && has_sample(&samples, "bar_metric", |v| v == 7.0, target_labels)
                && has_sample(&samples, "up", |v| v == 1.0, target_labels)
        }),
        "destination never received the k8s-discovered scraped series + up=1"
    );

    // --- `/api/v1/targets` reports the discovered target as up. ---
    let metrics_addr_str = metrics_addr.to_string();
    assert!(
        wait_until(Duration::from_secs(10), || {
            let (status, body) = http_get(agent_addr, "/api/v1/targets");
            if status != 200 {
                return false;
            }
            let json: serde_json::Value =
                serde_json::from_str(&body).expect("targets response must be valid JSON");
            json["data"]["activeTargets"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|t| {
                    t["scrapeUrl"]
                        .as_str()
                        .is_some_and(|u| u.contains(&metrics_addr_str))
                        && t["health"] == "up"
                })
        }),
        "/api/v1/targets never reported the k8s-discovered target as up"
    );

    app.stop();
    drop(metrics_server);
    k8s_server.stop();
    dest.server.stop();
    let _ = std::fs::remove_dir_all(tmp.path());
}

const EPS_JOB_NAME: &str = "esmagent_e2e_k8s_sd_endpoints";
const EPS_NAMESPACE: &str = "d";
const SVC_NAME: &str = "svc1";
const EPS_NODE_NAME: &str = "n1";
const EPS_NODE_REGION: &str = "r1";

/// The `endpoints` role exercises the Phase B cross-role object registry:
/// its target group joins the same-named `Service` and the `targetRef` `Pod`,
/// and `attach_metadata: {node: true}` joins the `Pod`'s `Node` labels. This
/// drives the full pipeline (four dependency LISTs -> shared registry ->
/// endpoints builder join -> target-relabel -> scrape -> forward) and asserts
/// two *joined* labels reach target-relabel: `__meta_kubernetes_service_name`
/// (proving the service join) and `__meta_kubernetes_node_label_region`
/// (proving the attach_metadata node join actually resolved the node object,
/// not just the unconditional `node_name` label).
#[test]
fn kubernetes_sd_endpoints_role_joins_service_and_node_and_forwards() {
    let dest = start_dest_stub();
    let dest_addr = dest.server.local_addr();

    let metrics_server = start_metrics_stub("foo_metric{code=\"200\"} 5\nbar_metric 7\n");
    let metrics_addr = metrics_server.local_addr();

    // The endpoints address ip:port resolves to the /metrics stub, so the
    // built `__address__` (join_host_port(ea.ip, epp.port)) is scrapeable.
    let ip = metrics_addr.ip().to_string();
    let port = metrics_addr.port();

    let endpoints_body = format!(
        r#"{{"metadata":{{"resourceVersion":"1"}},"items":[
            {{"metadata":{{"name":"{SVC_NAME}","namespace":"{EPS_NAMESPACE}"}},
             "subsets":[{{"addresses":[{{"ip":"{ip}",
                 "targetRef":{{"kind":"Pod","name":"p1","namespace":"{EPS_NAMESPACE}"}}}}],
                 "ports":[{{"name":"http","port":{port},"protocol":"TCP"}}]}}]}}]}}"#,
    );
    // Pod p1 has a container port equal to the endpoint port, so it matches
    // (no skipped-port fanout) and produces exactly one target. Its nodeName
    // resolves to the node LIST for the attach_metadata node join.
    let pods_body = format!(
        r#"{{"metadata":{{"resourceVersion":"1"}},"items":[
            {{"metadata":{{"name":"p1","namespace":"{EPS_NAMESPACE}"}},
             "spec":{{"nodeName":"{EPS_NODE_NAME}","containers":[{{"name":"c","image":"i",
                 "ports":[{{"name":"http","containerPort":{port},"protocol":"TCP"}}]}}]}},
             "status":{{"phase":"Running","podIP":"10.0.0.1",
                 "conditions":[{{"type":"Ready","status":"True"}}]}}}}]}}"#,
    );
    let services_body = format!(
        r#"{{"metadata":{{"resourceVersion":"1"}},"items":[
            {{"metadata":{{"name":"{SVC_NAME}","namespace":"{EPS_NAMESPACE}","labels":{{"app":"a"}}}},
             "spec":{{"type":"ClusterIP","clusterIP":"10.96.0.1",
                 "ports":[{{"name":"http","protocol":"TCP","port":{port}}}]}}}}]}}"#,
    );
    let nodes_body = format!(
        r#"{{"metadata":{{"resourceVersion":"1"}},"items":[
            {{"metadata":{{"name":"{EPS_NODE_NAME}","labels":{{"region":"{EPS_NODE_REGION}"}}}},
             "status":{{"addresses":[{{"type":"InternalIP","address":"10.0.0.100"}}]}}}}]}}"#,
    );

    let k8s_server = start_k8s_multipath_stub(vec![
        ("/endpoints", endpoints_body),
        ("/pods", pods_body),
        ("/services", services_body),
        ("/nodes", nodes_body),
    ]);
    let k8s_addr = k8s_server.local_addr();

    let tmp = tempfile::tempdir().expect("tmp data path");
    let scrape_cfg_path = tmp.path().join("scrape.yml");
    // role: endpoints, scoped to namespace `d`, with attach_metadata node.
    // Two relabel rules copy JOINED meta labels into plain labels: the
    // service name and a node label (which only exists if the node object
    // was resolved through the registry).
    std::fs::write(
        &scrape_cfg_path,
        format!(
            "scrape_configs:\n\
             \x20 - job_name: {EPS_JOB_NAME}\n\
             \x20   scrape_interval: 1s\n\
             \x20   scrape_timeout: 500ms\n\
             \x20   kubernetes_sd_configs:\n\
             \x20     - role: endpoints\n\
             \x20       api_server: 'http://{k8s_addr}'\n\
             \x20       namespaces:\n\
             \x20         names: [{EPS_NAMESPACE}]\n\
             \x20       attach_metadata:\n\
             \x20         node: true\n\
             \x20   relabel_configs:\n\
             \x20     - source_labels: [__meta_kubernetes_service_name]\n\
             \x20       target_label: svc\n\
             \x20     - source_labels: [__meta_kubernetes_node_label_region]\n\
             \x20       target_label: node_region\n"
        ),
    )
    .expect("write scrape config");

    let flags = Flags {
        remote_write_urls: vec![format!("http://{dest_addr}/api/v1/write")],
        remote_write_tmp_data_path: tmp.path().to_string_lossy().to_string(),
        remote_write_max_block_size: 1,
        http_listen_addr: "127.0.0.1:0".to_string(),
        promscrape_config: Some(scrape_cfg_path.to_string_lossy().to_string()),
        ..Flags::default()
    };

    let app = esmagent::run(&flags).expect("esmagent::run should succeed");
    let agent_addr = app.local_addr();

    // The forwarded series carry job + the service-join label `svc` + the
    // node-join label `node_region`. The dependency caches (pod/service/node)
    // populate asynchronously, so the join lands on a later reconcile pass —
    // hence the bounded poll.
    let target_labels: &[(&str, &str)] = &[
        ("job", EPS_JOB_NAME),
        ("svc", SVC_NAME),
        ("node_region", EPS_NODE_REGION),
    ];
    assert!(
        wait_until(Duration::from_secs(20), || {
            let samples = decode_bodies(&dest.bodies.lock().unwrap());
            has_sample(&samples, "foo_metric", |v| v == 5.0, target_labels)
                && has_sample(&samples, "bar_metric", |v| v == 7.0, target_labels)
                && has_sample(&samples, "up", |v| v == 1.0, target_labels)
        }),
        "destination never received the endpoints-discovered series carrying the \
         service- and node-join labels"
    );

    // `/api/v1/targets` reports the discovered target as up.
    let metrics_addr_str = metrics_addr.to_string();
    assert!(
        wait_until(Duration::from_secs(10), || {
            let (status, body) = http_get(agent_addr, "/api/v1/targets");
            if status != 200 {
                return false;
            }
            let json: serde_json::Value =
                serde_json::from_str(&body).expect("targets response must be valid JSON");
            json["data"]["activeTargets"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|t| {
                    t["scrapeUrl"]
                        .as_str()
                        .is_some_and(|u| u.contains(&metrics_addr_str))
                        && t["health"] == "up"
                })
        }),
        "/api/v1/targets never reported the endpoints-discovered target as up"
    );

    app.stop();
    drop(metrics_server);
    k8s_server.stop();
    dest.server.stop();
    let _ = std::fs::remove_dir_all(tmp.path());
}
