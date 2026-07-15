//! Stub-server tests for [`super::OpenstackDiscovery`] — split out per this
//! crate's `#[path]`-sibling convention to keep `mod.rs` under the 800-line
//! cap. One in-process stub serves the Keystone `POST /v3/auth/tokens` (a
//! `201` with an `X-Subject-Token` header + a catalog pointing Nova at the
//! stub) and the paginated Nova `servers/detail` / `os-hypervisors/detail`
//! GETs, so the tests exercise password auth, catalog resolution, pagination,
//! both roles, and a clean stop.

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

/// A running stub OpenStack endpoint. `requests` records every request's
/// `METHOD path?query` in arrival order.
struct OsStub {
    server: Server,
    requests: Arc<Mutex<Vec<String>>>,
}

impl OsStub {
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

/// Starts a stub OpenStack endpoint dispatching on the request path:
/// - `POST .../auth/tokens` -> `201` + `X-Subject-Token` + a catalog whose
///   compute endpoint points back at this stub,
/// - `.../servers/detail` -> paginated servers (page 2 when `marker` is set),
/// - `.../os-hypervisors/detail` -> two hypervisors, unpaginated.
fn start_os_stub() -> OsStub {
    let server = Server::bind("127.0.0.1:0").expect("bind openstack stub");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_for_handler = Arc::clone(&requests);
    let base = format!("http://{}", server.local_addr());

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let method = req.method();
            let path = req.path().to_string();
            let query = req.query().to_string();
            requests_for_handler
                .lock()
                .unwrap()
                .push(format!("{method:?} {path}?{query}"));

            if path.ends_with("/auth/tokens") {
                let catalog = format!(
                    r#"{{"token":{{"expires_at":"2999-01-01T00:00:00Z","catalog":[
                        {{"type":"compute","name":"nova","endpoints":[
                            {{"interface":"public","region_id":"RegionOne","url":"{base}"}}
                        ]}}
                    ]}}}}"#
                );
                w.set_header("X-Subject-Token", "subject-token-xyz");
                w.write_json(201, &catalog);
            } else if path.ends_with("/servers/detail") {
                if query.contains("marker=") {
                    w.write_json(200, SERVERS_PAGE2);
                } else {
                    let page1 = SERVERS_PAGE1
                        .replace("{NEXT}", &format!("{base}/servers/detail?marker=s2"));
                    w.write_json(200, &page1);
                }
            } else if path.ends_with("/os-hypervisors/detail") {
                w.write_json(200, HYPERVISORS_JSON);
            } else {
                w.write_status(404);
            }
        },
    ));

    OsStub { server, requests }
}

const SERVERS_PAGE1: &str = r#"{
  "servers": [
    {
      "id": "s1",
      "name": "server-1",
      "status": "ACTIVE",
      "tenant_id": "tenant-a",
      "user_id": "user-a",
      "flavor": {"id": "2"},
      "addresses": {
        "private": [
          {"version": 4, "addr": "10.0.0.1", "OS-EXT-IPS:type": "fixed"},
          {"version": 4, "addr": "1.2.3.4", "OS-EXT-IPS:type": "floating"}
        ]
      }
    }
  ],
  "servers_links": [{"href": "{NEXT}", "rel": "next"}]
}"#;

const SERVERS_PAGE2: &str = r#"{
  "servers": [
    {
      "id": "s2",
      "name": "server-2",
      "status": "ACTIVE",
      "tenant_id": "tenant-a",
      "user_id": "user-a",
      "flavor": {"id": "2"},
      "addresses": {
        "private": [
          {"version": 4, "addr": "10.0.0.2", "OS-EXT-IPS:type": "fixed"}
        ]
      }
    }
  ]
}"#;

const HYPERVISORS_JSON: &str = r#"{
  "hypervisors": [
    {"host_ip": "1.1.1.1", "id": 1, "hypervisor_hostname": "hv-1", "status": "enabled", "state": "up", "hypervisor_type": "QEMU"},
    {"host_ip": "1.1.1.2", "id": 2, "hypervisor_hostname": "hv-2", "status": "enabled", "state": "up", "hypervisor_type": "QEMU"}
  ]
}"#;

fn password_cfg(stub: &OsStub, role: &str) -> OpenstackSdConfig {
    OpenstackSdConfig {
        identity_endpoint: format!("{}/v3", stub.base()),
        username: "u".into(),
        password: Some("p".into()),
        domain_name: "default".into(),
        role: role.into(),
        port: 9100,
        refresh_interval: Duration::from_millis(50),
        ..OpenstackSdConfig::default()
    }
}

/// Password auth + `role: instance`: the stub must authenticate, resolve the
/// compute endpoint from the catalog, follow `servers_links` pagination, and
/// yield both servers with their `__meta_openstack_*` labels + `__address__`,
/// then stop cleanly.
#[test]
fn discovers_openstack_instances_with_password_auth() {
    let stub = start_os_stub();
    let cfg = password_cfg(&stub, ROLE_INSTANCE);

    let mut d = OpenstackDiscovery::new(&cfg, "job").expect("new");
    let found = wait_until(Duration::from_secs(5), || {
        let ids: std::collections::BTreeSet<String> = d
            .poll()
            .iter()
            .filter_map(|g| g.labels.get("__meta_openstack_instance_id").cloned())
            .collect();
        ids.contains("s1") && ids.contains("s2")
    });
    assert!(
        found,
        "both instances never discovered; requests={:?}",
        stub.requests()
    );

    let groups = d.poll();
    let s1 = groups
        .iter()
        .find(|g| {
            g.labels
                .get("__meta_openstack_instance_id")
                .map(String::as_str)
                == Some("s1")
        })
        .expect("instance s1");
    assert_eq!(s1.targets, vec!["10.0.0.1:9100".to_string()]);
    assert_eq!(s1.labels["__meta_openstack_instance_name"], "server-1");
    assert_eq!(s1.labels["__meta_openstack_instance_status"], "ACTIVE");
    assert_eq!(s1.labels["__meta_openstack_instance_flavor"], "2");
    assert_eq!(s1.labels["__meta_openstack_private_ip"], "10.0.0.1");
    assert_eq!(s1.labels["__meta_openstack_public_ip"], "1.2.3.4");
    assert_eq!(s1.labels["__meta_openstack_address_pool"], "private");
    assert_eq!(s1.labels["__meta_openstack_project_id"], "tenant-a");
    assert_eq!(s1.labels["__meta_openstack_user_id"], "user-a");
    assert_eq!(s1.source, "job/openstack");
    assert!(!s1.labels.contains_key("__address__"));

    let s2 = groups
        .iter()
        .find(|g| {
            g.labels
                .get("__meta_openstack_instance_id")
                .map(String::as_str)
                == Some("s2")
        })
        .expect("instance s2");
    assert_eq!(s2.targets, vec!["10.0.0.2:9100".to_string()]);

    // The auth endpoint and page 2 must both have been hit.
    assert!(
        stub.requests().iter().any(|r| r.contains("/auth/tokens")),
        "auth endpoint must be called; requests={:?}",
        stub.requests()
    );
    assert!(
        stub.requests().iter().any(|r| r.contains("marker=")),
        "page 2 must be fetched; requests={:?}",
        stub.requests()
    );

    drop(d);
    stub.stop();
}

/// `role: hypervisor`: the stub must yield both hypervisors with their
/// `__meta_openstack_hypervisor_*` labels + `__address__`.
#[test]
fn discovers_openstack_hypervisors() {
    let stub = start_os_stub();
    let cfg = password_cfg(&stub, ROLE_HYPERVISOR);

    let mut d = OpenstackDiscovery::new(&cfg, "job").expect("new");
    let found = wait_until(Duration::from_secs(5), || {
        let ids: std::collections::BTreeSet<String> = d
            .poll()
            .iter()
            .filter_map(|g| g.labels.get("__meta_openstack_hypervisor_id").cloned())
            .collect();
        ids.contains("1") && ids.contains("2")
    });
    assert!(
        found,
        "both hypervisors never discovered; requests={:?}",
        stub.requests()
    );

    let groups = d.poll();
    let hv1 = groups
        .iter()
        .find(|g| {
            g.labels
                .get("__meta_openstack_hypervisor_id")
                .map(String::as_str)
                == Some("1")
        })
        .expect("hypervisor 1");
    assert_eq!(hv1.targets, vec!["1.1.1.1:9100".to_string()]);
    assert_eq!(hv1.labels["__meta_openstack_hypervisor_hostname"], "hv-1");
    assert_eq!(hv1.labels["__meta_openstack_hypervisor_type"], "QEMU");
    assert_eq!(hv1.labels["__meta_openstack_hypervisor_state"], "up");
    assert_eq!(hv1.labels["__meta_openstack_hypervisor_status"], "enabled");
    assert_eq!(hv1.labels["__meta_openstack_hypervisor_host_ip"], "1.1.1.1");

    drop(d);
    stub.stop();
}

/// A `v2.0` identity endpoint is rejected at `new()`.
#[test]
fn v2_identity_endpoint_is_rejected() {
    let cfg = OpenstackSdConfig {
        identity_endpoint: "http://example.com/v2.0".into(),
        username: "u".into(),
        password: Some("p".into()),
        domain_name: "default".into(),
        role: ROLE_INSTANCE.into(),
        ..OpenstackSdConfig::default()
    };
    let err = OpenstackDiscovery::new(&cfg, "job")
        .err()
        .expect("must reject v2.0");
    assert!(err.msg.contains("v2.0"), "{}", err.msg);
}

/// Missing auth (no password, no application credential) fails at `new()`.
#[test]
fn missing_auth_is_rejected() {
    let cfg = OpenstackSdConfig {
        identity_endpoint: "http://example.com/v3".into(),
        role: ROLE_INSTANCE.into(),
        ..OpenstackSdConfig::default()
    };
    let err = OpenstackDiscovery::new(&cfg, "job")
        .err()
        .expect("must reject missing auth");
    assert!(
        err.msg
            .contains("password and application credentials are missing"),
        "{}",
        err.msg
    );
}

/// An unknown `role` fails at `new()`.
#[test]
fn unknown_role_is_rejected() {
    let cfg = OpenstackSdConfig {
        identity_endpoint: "http://example.com/v3".into(),
        username: "u".into(),
        password: Some("p".into()),
        domain_name: "default".into(),
        role: "bogus".into(),
        ..OpenstackSdConfig::default()
    };
    let err = OpenstackDiscovery::new(&cfg, "job")
        .err()
        .expect("must reject role");
    assert!(err.msg.contains("unexpected role"), "{}", err.msg);
}
