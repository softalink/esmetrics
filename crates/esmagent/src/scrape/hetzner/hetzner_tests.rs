//! Stub-server tests for [`super::HetznerDiscovery`] — split out per this
//! crate's `#[path]`-sibling convention to keep `mod.rs` under the 800-line
//! cap. Cover BOTH roles: `hcloud` (Bearer auth, paginated `/v1/servers` +
//! `/v1/networks` — a two-page servers response must surface BOTH servers'
//! targets) and `robot` (HTTP Basic auth, single `/server`).

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_http::{Request, ResponseWriter, Server, ServerConfig};

use super::*;
use crate::client::AuthConfig;

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

/// One recorded request: `path?query` plus its captured `Authorization`
/// header (if any).
type RecordedRequest = (String, Option<String>);

/// A running stub Hetzner API. `requests` records every request's `path?query`
/// (with the captured `Authorization` header) in arrival order.
struct HetznerStub {
    server: Server,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
}

impl HetznerStub {
    fn addr(&self) -> String {
        self.server.local_addr().to_string()
    }

    fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn auth_header_for(&self, needle: &str) -> Option<String> {
        self.requests()
            .into_iter()
            .find(|(path, _)| path.contains(needle))
            .and_then(|(_, auth)| auth)
    }

    fn stop(&self) {
        self.server.stop();
    }
}

fn record(requests: &Arc<Mutex<Vec<RecordedRequest>>>, req: &Request<'_>) {
    let auth = req
        .all_headers()
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
        .map(|(_, value)| value.clone());
    requests
        .lock()
        .unwrap()
        .push((format!("{}?{}", req.path(), req.query()), auth));
}

/// hcloud stub: `/v1/networks` returns one network (`mynet` id 4711, no next
/// page); `/v1/servers` is two-paged — page 1 = server 1 (`1.1.1.1`) with
/// `next_page: 2`, page 2 = server 2 (`2.2.2.2`) with no next page.
fn start_hcloud_stub() -> HetznerStub {
    let config = ServerConfig {
        capture_all_headers: true,
        ..ServerConfig::default()
    };
    let server = Server::bind_with_config("127.0.0.1:0", config).expect("bind hcloud stub");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_for_handler = Arc::clone(&requests);

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            record(&requests_for_handler, req);
            let path = req.path().to_string();
            let query = req.query().to_string();

            if path.contains("networks") {
                w.write_json(
                    200,
                    r#"{"meta":{"pagination":{"next_page":null}},
                        "networks":[{"id":4711,"name":"mynet"}]}"#,
                );
            } else if query.contains("page=2") {
                w.write_json(
                    200,
                    r#"{"meta":{"pagination":{"next_page":null}},
                        "servers":[{"id":2,"name":"srv-2","status":"running",
                          "public_net":{"ipv4":{"ip":"2.2.2.2"}},
                          "server_type":{"cores":2,"cpu_type":"shared","memory":4,"disk":40,"name":"cx21"},
                          "datacenter":{"name":"fsn1-dc8"},
                          "location":{"name":"fsn1","network_zone":"eu-central"}}]}"#,
                );
            } else {
                w.write_json(
                    200,
                    r#"{"meta":{"pagination":{"next_page":2}},
                        "servers":[{"id":1,"name":"srv-1","status":"running",
                          "public_net":{"ipv4":{"ip":"1.1.1.1"},"ipv6":{"ip":"2001:db8::/64"}},
                          "private_net":[{"network":4711,"ip":"10.0.0.5"}],
                          "server_type":{"cores":1,"cpu_type":"shared","memory":1,"disk":25,"name":"cx11"},
                          "datacenter":{"name":"fsn1-dc8"},
                          "location":{"name":"fsn1","network_zone":"eu-central"}}]}"#,
                );
            }
        },
    ));

    HetznerStub { server, requests }
}

/// robot stub: `/server` returns a two-element array.
fn start_robot_stub() -> HetznerStub {
    let config = ServerConfig {
        capture_all_headers: true,
        ..ServerConfig::default()
    };
    let server = Server::bind_with_config("127.0.0.1:0", config).expect("bind robot stub");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_for_handler = Arc::clone(&requests);

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            record(&requests_for_handler, req);
            w.write_json(
                200,
                r#"[
                  {"server":{"server_ip":"123.123.123.123","server_number":321,
                             "server_name":"server1","product":"DS 3000","dc":"NBG1-DC1",
                             "status":"ready","cancelled":false,
                             "subnet":[{"ip":"2a01:4f8:111:4221::","mask":"64"}]}},
                  {"server":{"server_ip":"123.123.123.124","server_number":421,
                             "server_name":"server2","product":"X5","dc":"FSN1-DC10",
                             "status":"ready","cancelled":false,"subnet":null}}
                ]"#,
            );
        },
    ));

    HetznerStub { server, requests }
}

/// hcloud: a two-page servers response must yield BOTH servers' targets
/// (proving pagination), with the first server's private-ipv4 join against the
/// networks list and its `publicIPv4:port` `__address__`. Bearer auth applied.
/// Clean stop.
#[test]
fn hcloud_discovers_both_pages() {
    let stub = start_hcloud_stub();
    let cfg = HetznerSdConfig {
        role: ROLE_HCLOUD.into(),
        server: stub.addr(),
        port: 9100,
        auth: AuthConfig {
            bearer: Some("tok".into()),
            ..AuthConfig::default()
        },
        refresh_interval: Duration::from_millis(50),
        ..HetznerSdConfig::default()
    };

    let mut d = HetznerDiscovery::new(&cfg, "job").expect("new");

    let found = wait_until(Duration::from_secs(5), || {
        let ids: std::collections::BTreeSet<String> = d
            .poll()
            .iter()
            .filter_map(|g| g.labels.get("__meta_hetzner_server_id").cloned())
            .collect();
        ids.contains("1") && ids.contains("2")
    });
    assert!(
        found,
        "both servers never discovered; requests={:?}",
        stub.requests()
    );

    let groups = d.poll();
    assert_eq!(groups.len(), 2, "groups={groups:?}");

    let srv1 = groups
        .iter()
        .find(|g| g.labels.get("__meta_hetzner_server_id").map(String::as_str) == Some("1"))
        .expect("server 1");
    assert_eq!(srv1.targets, vec!["1.1.1.1:9100".to_string()]);
    assert_eq!(srv1.labels["__meta_hetzner_role"], "hcloud");
    assert_eq!(srv1.labels["__meta_hetzner_public_ipv4"], "1.1.1.1");
    assert_eq!(
        srv1.labels["__meta_hetzner_public_ipv6_network"],
        "2001:db8::/64"
    );
    assert_eq!(
        srv1.labels["__meta_hetzner_hcloud_private_ipv4_mynet"],
        "10.0.0.5"
    );
    assert_eq!(srv1.labels["__meta_hetzner_hcloud_server_type"], "cx11");
    assert_eq!(srv1.source, "job/hetzner");

    let srv2 = groups
        .iter()
        .find(|g| g.labels.get("__meta_hetzner_server_id").map(String::as_str) == Some("2"))
        .expect("server 2");
    assert_eq!(srv2.targets, vec!["2.2.2.2:9100".to_string()]);

    // Page 2 must actually have been fetched, and Bearer auth applied.
    assert!(
        stub.requests().iter().any(|(p, _)| p.contains("page=2")),
        "page 2 should have been fetched; requests={:?}",
        stub.requests()
    );
    assert_eq!(
        stub.auth_header_for("servers").as_deref(),
        Some("Bearer tok")
    );

    drop(d);
    stub.stop();
}

/// robot: a `/server` response yields both dedicated servers with their
/// `__meta_hetzner_robot_*` labels and HTTP Basic auth
/// (`ZDoxMjM=` == base64("d:123") — here `dXNlcjpwYXNz` == base64("user:pass")).
#[test]
fn robot_discovers_servers_with_basic_auth() {
    let stub = start_robot_stub();
    let cfg = HetznerSdConfig {
        role: ROLE_ROBOT.into(),
        server: stub.addr(),
        port: 9100,
        auth: AuthConfig {
            basic: Some(("user".into(), "pass".into())),
            ..AuthConfig::default()
        },
        refresh_interval: Duration::from_millis(50),
        ..HetznerSdConfig::default()
    };

    let mut d = HetznerDiscovery::new(&cfg, "job").expect("new");

    let found = wait_until(Duration::from_secs(5), || {
        let ids: std::collections::BTreeSet<String> = d
            .poll()
            .iter()
            .filter_map(|g| g.labels.get("__meta_hetzner_server_id").cloned())
            .collect();
        ids.contains("321") && ids.contains("421")
    });
    assert!(
        found,
        "robot servers never discovered; requests={:?}",
        stub.requests()
    );

    let groups = d.poll();
    assert_eq!(groups.len(), 2);
    let srv1 = groups
        .iter()
        .find(|g| g.labels.get("__meta_hetzner_server_id").map(String::as_str) == Some("321"))
        .expect("server 321");
    assert_eq!(srv1.targets, vec!["123.123.123.123:9100".to_string()]);
    assert_eq!(srv1.labels["__meta_hetzner_role"], "robot");
    assert_eq!(srv1.labels["__meta_hetzner_robot_product"], "DS 3000");
    assert_eq!(srv1.labels["__meta_hetzner_datacenter"], "nbg1-dc1");
    assert_eq!(
        srv1.labels["__meta_hetzner_public_ipv6_network"],
        "2a01:4f8:111:4221::/64"
    );

    // HTTP Basic auth applied: base64("user:pass") == "dXNlcjpwYXNz".
    assert_eq!(
        stub.auth_header_for("server").as_deref(),
        Some("Basic dXNlcjpwYXNz")
    );

    drop(d);
    stub.stop();
}
