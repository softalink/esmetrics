//! Stub-server tests for [`super::GceDiscovery`] — split out per this crate's
//! `#[path]`-sibling convention to keep `mod.rs` under the 800-line cap. One
//! in-process stub serves the metadata token / project-id / zone endpoints,
//! the `zones.list`, and a paginated `instances.list` (page 1 carries a
//! `nextPageToken`, page 2 does not), so the tests exercise both credential
//! modes (static bearer token and metadata token) and pagination.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_http::{Request, ResponseWriter, Server};

use super::*;

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

/// A running stub GCE endpoint. `requests` records every request's
/// `path?query` in arrival order (so a test can prove page 2 / the token /
/// the zones list were fetched).
struct GceStub {
    server: Server,
    requests: Arc<Mutex<Vec<String>>>,
}

impl GceStub {
    fn base(&self) -> String {
        format!("http://{}", self.server.local_addr())
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }

    fn stop(&self) {
        self.server.stop();
    }
}

const INSTANCES_PAGE1: &str = r#"{
  "items": [
    {
      "id": "1",
      "name": "inst-1",
      "status": "RUNNING",
      "machineType": "mt/f1-micro",
      "zone": "z/us-east1-b",
      "networkInterfaces": [
        {"name": "nic0", "network": "net/default", "subnetwork": "sn/a", "networkIP": "10.0.0.1"}
      ],
      "tags": {"items": ["web", "prod"]},
      "labels": {"env": "play"}
    }
  ],
  "nextPageToken": "tok2"
}"#;

const INSTANCES_PAGE2: &str = r#"{
  "items": [
    {
      "id": "2",
      "name": "inst-2",
      "status": "RUNNING",
      "networkInterfaces": [
        {"name": "nic0", "networkIP": "10.0.0.2"}
      ]
    }
  ]
}"#;

/// Starts a stub GCE endpoint dispatching on the request path:
/// - `.../token` -> a metadata access token,
/// - `.../project/project-id` -> a project id,
/// - `.../instance/zone` -> a `projects/N/zones/Z` string,
/// - `.../instances` -> paginated instances (page 2 when `pageToken` is set),
/// - `.../zones` -> a one-zone `zones.list`.
fn start_gce_stub() -> GceStub {
    let server = Server::bind("127.0.0.1:0").expect("bind gce stub");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_for_handler = Arc::clone(&requests);

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let path = req.path().to_string();
            let query = req.query().to_string();
            requests_for_handler
                .lock()
                .unwrap()
                .push(format!("{path}?{query}"));

            if path.ends_with("/token") {
                w.write_json(
                    200,
                    r#"{"access_token":"meta-tok","expires_in":3600,"token_type":"Bearer"}"#,
                );
            } else if path.ends_with("/instances") {
                if query.contains("pageToken=") {
                    w.write_json(200, INSTANCES_PAGE2);
                } else {
                    w.write_json(200, INSTANCES_PAGE1);
                }
            } else if path.ends_with("/zones") {
                w.write_json(200, r#"{"items":[{"name":"us-east1-b"}]}"#);
            } else if path.ends_with("/project/project-id") {
                w.write_status(200);
                w.write_body(b"proj-detected");
            } else if path.ends_with("/instance/zone") {
                w.write_status(200);
                w.write_body(b"projects/12345/zones/us-east1-b");
            } else {
                w.write_status(404);
            }
        },
    ));

    GceStub { server, requests }
}

fn base_cfg(stub: &GceStub) -> GceSdConfig {
    GceSdConfig {
        endpoint: Some(stub.base()),
        refresh_interval: Duration::from_millis(50),
        ..GceSdConfig::default()
    }
}

/// Waits until `d` has discovered instance targets with ids `1` and `2`,
/// proving `instances.list` pagination was followed across both pages.
fn discovers_both_instances(d: &mut GceDiscovery, stub: &GceStub) {
    let found = wait_until(Duration::from_secs(5), || {
        let ids: std::collections::BTreeSet<String> = d
            .poll()
            .iter()
            .filter_map(|g| g.labels.get("__meta_gce_instance_id").cloned())
            .collect();
        ids.contains("1") && ids.contains("2")
    });
    assert!(
        found,
        "both gce instances never discovered; requests={:?}",
        stub.requests()
    );
    assert!(
        stub.requests().iter().any(|r| r.contains("pageToken=")),
        "page 2 should have been fetched; requests={:?}",
        stub.requests()
    );
}

/// Static bearer token + explicit project + explicit zone: the stub must yield
/// both paginated instances with their `__meta_gce_*` labels and `__address__`
/// (first interface's private IP + configured port), then stop cleanly.
#[test]
fn discovers_gce_instances_with_static_token() {
    let stub = start_gce_stub();
    let cfg = GceSdConfig {
        project: "proj".to_string(),
        zones: vec!["us-east1-b".to_string()],
        port: 9100,
        bearer_token: Some("static-tok".to_string()),
        ..base_cfg(&stub)
    };

    let mut d = GceDiscovery::new(&cfg, "job").expect("new");
    discovers_both_instances(&mut d, &stub);

    let groups = d.poll();
    let inst1 = groups
        .iter()
        .find(|g| g.labels.get("__meta_gce_instance_id").map(String::as_str) == Some("1"))
        .expect("instance 1");
    assert_eq!(inst1.targets, vec!["10.0.0.1:9100".to_string()]);
    assert_eq!(inst1.labels["__meta_gce_instance_name"], "inst-1");
    assert_eq!(inst1.labels["__meta_gce_instance_status"], "RUNNING");
    assert_eq!(inst1.labels["__meta_gce_private_ip"], "10.0.0.1");
    assert_eq!(inst1.labels["__meta_gce_project"], "proj");
    assert_eq!(inst1.labels["__meta_gce_network"], "net/default");
    assert_eq!(inst1.labels["__meta_gce_subnetwork"], "sn/a");
    assert_eq!(inst1.labels["__meta_gce_interface_ipv4_nic0"], "10.0.0.1");
    assert_eq!(inst1.labels["__meta_gce_tags"], ",web,prod,");
    assert_eq!(inst1.labels["__meta_gce_label_env"], "play");
    assert_eq!(inst1.source, "job/gce/us-east1-b");
    // __address__ is a target, not a label.
    assert!(!inst1.labels.contains_key("__address__"));

    let inst2 = groups
        .iter()
        .find(|g| g.labels.get("__meta_gce_instance_id").map(String::as_str) == Some("2"))
        .expect("instance 2");
    assert_eq!(inst2.targets, vec!["10.0.0.2:9100".to_string()]);

    // A static token means the metadata token endpoint is never hit.
    assert!(
        !stub.requests().iter().any(|r| r.contains("/token")),
        "static-token mode must not call the metadata token endpoint; requests={:?}",
        stub.requests()
    );

    drop(d);
    stub.stop();
}

/// No bearer token: the client must fetch the metadata-server access token
/// (hitting `.../token`) and still discover instances.
#[test]
fn discovers_gce_instances_with_metadata_token() {
    let stub = start_gce_stub();
    let cfg = GceSdConfig {
        project: "proj".to_string(),
        zones: vec!["us-east1-b".to_string()],
        metadata_url: Some(stub.base()),
        ..base_cfg(&stub)
    };

    let mut d = GceDiscovery::new(&cfg, "job").expect("new");
    discovers_both_instances(&mut d, &stub);

    assert!(
        stub.requests().iter().any(|r| r.contains("/token")),
        "metadata-token mode must call the token endpoint; requests={:?}",
        stub.requests()
    );

    drop(d);
    stub.stop();
}

/// `zone: '*'` must resolve zones via `zones.list` (hitting `.../zones`) then
/// list instances for each returned zone.
#[test]
fn wildcard_zone_lists_all_zones() {
    let stub = start_gce_stub();
    let cfg = GceSdConfig {
        project: "proj".to_string(),
        zones: vec!["*".to_string()],
        bearer_token: Some("static-tok".to_string()),
        ..base_cfg(&stub)
    };

    let mut d = GceDiscovery::new(&cfg, "job").expect("new");
    let found = wait_until(Duration::from_secs(5), || {
        d.poll()
            .iter()
            .any(|g| g.labels.get("__meta_gce_instance_id").map(String::as_str) == Some("1"))
    });
    assert!(
        found,
        "wildcard-zone discovery never produced targets; requests={:?}",
        stub.requests()
    );
    assert!(
        stub.requests()
            .iter()
            .any(|r| r.split('?').next() == Some("/projects/proj/zones")),
        "wildcard zone must call zones.list; requests={:?}",
        stub.requests()
    );

    drop(d);
    stub.stop();
}

/// A set `credentials_file` (service-account JSON key) is DEFERRED and must
/// fail `new()` with a clear message.
#[test]
fn credentials_file_is_rejected_as_deferred() {
    let cfg = GceSdConfig {
        project: "proj".to_string(),
        zones: vec!["us-east1-b".to_string()],
        credentials_file: Some("/etc/gcp/key.json".to_string()),
        ..GceSdConfig::default()
    };
    let err = match GceDiscovery::new(&cfg, "job") {
        Ok(_) => panic!("credentials_file must be rejected"),
        Err(e) => e,
    };
    assert!(err.msg.contains("credentials_file"), "{}", err.msg);
    assert!(err.msg.contains("deferred"), "{}", err.msg);
}
