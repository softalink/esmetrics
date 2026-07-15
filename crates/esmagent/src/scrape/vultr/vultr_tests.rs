//! Stub-server tests for [`super::VultrDiscovery`] — split out per this crate's
//! `#[path]`-sibling convention to keep `mod.rs` under the 800-line cap. The
//! key assertion is that cursor pagination is followed: a two-page
//! `/v2/instances` response must surface BOTH instances' targets.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_http::{Request, ResponseWriter, Server, ServerConfig};

use super::*;
use crate::client::AuthConfig;
use crate::scrape::config::VultrSdConfig;

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

/// A running stub Vultr API. `requests` records every request's `path?query` in
/// arrival order (so the test can prove page 2 was fetched with the cursor).
struct VultrStub {
    server: Server,
    requests: Arc<Mutex<Vec<String>>>,
}

impl VultrStub {
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

/// Page 1: instance id `i-1` (`main_ip 1.1.1.1`) plus a `meta.links.next`
/// cursor token. Page 2 (requested with `cursor=<token>`): instance id `i-2`
/// (`main_ip 2.2.2.2`) with an empty `next` (end of pagination). Routing keys
/// on the presence of the `cursor` query param.
fn start_vultr_stub() -> VultrStub {
    let server = Server::bind("127.0.0.1:0").expect("bind vultr stub");
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

            if query.contains("cursor=") {
                // Page 2: last instance, empty next cursor.
                w.write_json(
                    200,
                    r#"{"instances":[
                        {"id":"i-2","main_ip":"2.2.2.2","os":"Ubuntu","ram":2048,
                         "disk":50,"vcpu_count":2,"region":"ewr","plan":"vc2-2c-2gb",
                         "server_status":"ok","os_id":1743}
                    ],"meta":{"links":{"next":""}}}"#,
                );
            } else {
                // Page 1: first instance + next cursor token.
                w.write_json(
                    200,
                    r#"{"instances":[
                        {"id":"i-1","main_ip":"1.1.1.1","os":"Ubuntu","ram":1024,
                         "disk":25,"vcpu_count":1,"region":"sgp","plan":"vc2-1c-1gb",
                         "server_status":"ok","os_id":1743,
                         "features":["ipv6"],"tags":["web","prod"]}
                    ],"meta":{"links":{"next":"cursor-token-page-2"}}}"#,
                );
            }
        },
    ));

    VultrStub { server, requests }
}

/// A two-page instances response must yield BOTH instances' targets — proving
/// the client follows `meta.links.next` as a `cursor` — with the first
/// instance's comma-wrapped tags and its `main_ip:port` `__address__`. Clean
/// stop.
#[test]
fn discovers_both_pages_of_instances() {
    let stub = start_vultr_stub();
    let cfg = VultrSdConfig {
        server: stub.addr(),
        port: 9100,
        auth: AuthConfig {
            bearer: Some("tok".to_string()),
            ..AuthConfig::default()
        },
        refresh_interval: Duration::from_millis(50),
        ..VultrSdConfig::default()
    };

    let mut d = VultrDiscovery::new(&cfg, "job").expect("new");

    // Bounded-poll until BOTH instance targets are present (proves pagination).
    let found = wait_until(Duration::from_secs(5), || {
        let groups = d.poll();
        let ids: std::collections::BTreeSet<String> = groups
            .iter()
            .filter_map(|g| g.labels.get("__meta_vultr_instance_id").cloned())
            .collect();
        ids.contains("i-1") && ids.contains("i-2")
    });
    assert!(
        found,
        "both instances never discovered; requests={:?}",
        stub.requests()
    );

    let groups = d.poll();
    assert_eq!(groups.len(), 2, "groups={groups:?}");

    let i1 = groups
        .iter()
        .find(|g| g.labels.get("__meta_vultr_instance_id").map(String::as_str) == Some("i-1"))
        .expect("instance i-1");
    assert_eq!(i1.targets, vec!["1.1.1.1:9100".to_string()]);
    assert_eq!(i1.labels["__meta_vultr_instance_main_ip"], "1.1.1.1");
    assert_eq!(i1.labels["__meta_vultr_instance_region"], "sgp");
    assert_eq!(i1.labels["__meta_vultr_instance_plan"], "vc2-1c-1gb");
    assert_eq!(i1.labels["__meta_vultr_instance_ram_mb"], "1024");
    assert_eq!(i1.labels["__meta_vultr_instance_features"], ",ipv6,");
    assert_eq!(i1.labels["__meta_vultr_instance_tags"], ",web,prod,");
    assert_eq!(i1.source, "job/vultr");

    let i2 = groups
        .iter()
        .find(|g| g.labels.get("__meta_vultr_instance_id").map(String::as_str) == Some("i-2"))
        .expect("instance i-2");
    assert_eq!(i2.targets, vec!["2.2.2.2:9100".to_string()]);

    // Page 2 must actually have been fetched with a cursor.
    assert!(
        stub.requests().iter().any(|r| r.contains("cursor=")),
        "page 2 should have been fetched with a cursor; requests={:?}",
        stub.requests()
    );

    // Clean stop: dropping the discovery joins the refresh thread promptly.
    drop(d);
    stub.stop();
}

/// A stub that captures the `Authorization` header of the first request and
/// returns an empty instances list. Used to prove the client applies bearer
/// auth.
struct AuthStub {
    server: Server,
    auth_header: Arc<Mutex<Option<String>>>,
}

fn start_auth_stub() -> AuthStub {
    let config = ServerConfig {
        capture_all_headers: true,
        ..ServerConfig::default()
    };
    let server = Server::bind_with_config("127.0.0.1:0", config).expect("bind auth stub");
    let auth_header = Arc::new(Mutex::new(None));
    let auth_for_handler = Arc::clone(&auth_header);

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let value = req
                .all_headers()
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
                .map(|(_, value)| value.clone());
            *auth_for_handler.lock().unwrap() = value;
            w.write_json(200, r#"{"instances":[],"meta":{"links":{"next":""}}}"#);
        },
    ));

    AuthStub {
        server,
        auth_header,
    }
}

/// A bearer-configured Vultr SD must send an `Authorization: Bearer <token>`
/// header — not go out unauthenticated.
#[test]
fn bearer_auth_sends_bearer_authorization_header() {
    let stub = start_auth_stub();
    let cfg = VultrSdConfig {
        server: stub.server.local_addr().to_string(),
        port: 9100,
        auth: AuthConfig {
            bearer: Some("vultr-secret".to_string()),
            ..AuthConfig::default()
        },
        refresh_interval: Duration::from_millis(50),
        ..VultrSdConfig::default()
    };

    let d = VultrDiscovery::new(&cfg, "job").expect("new");

    let got = wait_until(Duration::from_secs(5), || {
        stub.auth_header.lock().unwrap().is_some()
    });
    assert!(got, "no request reached the stub");

    let header = stub.auth_header.lock().unwrap().clone().unwrap();
    assert_eq!(header, "Bearer vultr-secret", "auth header={header:?}");

    drop(d);
    stub.server.stop();
}
