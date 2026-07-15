//! Stub-server tests for [`super::DigitaloceanDiscovery`] — split out per this
//! crate's `#[path]`-sibling convention to keep `mod.rs` under the 800-line
//! cap. The key assertion is that pagination is followed: a two-page
//! `/v2/droplets` response must surface BOTH droplets' targets.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_http::{Request, ResponseWriter, Server, ServerConfig};

use super::*;
use crate::client::AuthConfig;
use crate::scrape::config::DigitaloceanSdConfig;

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

/// A running stub DigitalOcean API. `requests` records every request's
/// `path?query` in arrival order (so the test can prove page 2 was fetched).
struct DoStub {
    server: Server,
    requests: Arc<Mutex<Vec<String>>>,
}

impl DoStub {
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

/// Page 1: droplet id 1 (`public 1.1.1.1`) plus a `links.pages.next` pointing
/// at page 2. Page 2: droplet id 2 (`public 2.2.2.2`) with no `next` (end of
/// pagination). The `next` URL's host is `api.digitalocean.com`; the client
/// must reduce it to its request URI and re-issue against the stub's base URL,
/// so only `page=2` in the query routes page 2 here.
fn start_do_stub() -> DoStub {
    let server = Server::bind("127.0.0.1:0").expect("bind digitalocean stub");
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

            if query.contains("page=2") {
                // Page 2: last droplet, no next link.
                w.write_json(
                    200,
                    r#"{"droplets":[
                        {"id":2,"name":"drop-2","status":"active",
                         "image":{"name":"Ubuntu","slug":"ubuntu-22"},
                         "size_slug":"s-1","region":{"slug":"nyc3"},
                         "networks":{"v4":[{"ip_address":"2.2.2.2","type":"public"}]}}
                    ]}"#,
                );
            } else {
                // Page 1: first droplet + next link to page 2.
                w.write_json(
                    200,
                    r#"{"droplets":[
                        {"id":1,"name":"drop-1","status":"active",
                         "image":{"name":"Ubuntu","slug":"ubuntu-22"},
                         "size_slug":"s-1","region":{"slug":"nyc3"},
                         "tags":["web","prod"],
                         "networks":{"v4":[
                            {"ip_address":"1.1.1.1","type":"public"},
                            {"ip_address":"10.0.0.1","type":"private"}]}}
                    ],
                    "links":{"pages":{
                        "last":"https://api.digitalocean.com/v2/droplets?page=2&per_page=1",
                        "next":"https://api.digitalocean.com/v2/droplets?page=2&per_page=1"}}}"#,
                );
            }
        },
    ));

    DoStub { server, requests }
}

/// A two-page droplets response must yield BOTH droplets' targets — proving
/// the client follows `links.pages.next` — with the first droplet's
/// comma-wrapped tags and its `publicIPv4:port` `__address__`. Clean stop.
#[test]
fn discovers_both_pages_of_droplets() {
    let stub = start_do_stub();
    let cfg = DigitaloceanSdConfig {
        server: stub.addr(),
        port: 9100,
        auth: AuthConfig {
            bearer: Some("tok".to_string()),
            ..AuthConfig::default()
        },
        refresh_interval: Duration::from_millis(50),
        ..DigitaloceanSdConfig::default()
    };

    let mut d = DigitaloceanDiscovery::new(&cfg, "job").expect("new");

    // Bounded-poll until BOTH droplet targets are present (proves pagination).
    let found = wait_until(Duration::from_secs(5), || {
        let groups = d.poll();
        let ids: std::collections::BTreeSet<String> = groups
            .iter()
            .filter_map(|g| g.labels.get("__meta_digitalocean_droplet_id").cloned())
            .collect();
        ids.contains("1") && ids.contains("2")
    });
    assert!(
        found,
        "both droplets never discovered; requests={:?}",
        stub.requests()
    );

    let groups = d.poll();
    assert_eq!(groups.len(), 2, "groups={groups:?}");

    let drop1 = groups
        .iter()
        .find(|g| {
            g.labels
                .get("__meta_digitalocean_droplet_id")
                .map(String::as_str)
                == Some("1")
        })
        .expect("droplet 1");
    assert_eq!(drop1.targets, vec!["1.1.1.1:9100".to_string()]);
    assert_eq!(drop1.labels["__meta_digitalocean_private_ipv4"], "10.0.0.1");
    assert_eq!(drop1.labels["__meta_digitalocean_public_ipv4"], "1.1.1.1");
    assert_eq!(drop1.labels["__meta_digitalocean_region"], "nyc3");
    assert_eq!(drop1.labels["__meta_digitalocean_size"], "s-1");
    assert_eq!(drop1.labels["__meta_digitalocean_status"], "active");
    assert_eq!(drop1.labels["__meta_digitalocean_image"], "ubuntu-22");
    assert_eq!(drop1.labels["__meta_digitalocean_tags"], ",web,prod,");
    assert_eq!(drop1.source, "job/digitalocean");

    let drop2 = groups
        .iter()
        .find(|g| {
            g.labels
                .get("__meta_digitalocean_droplet_id")
                .map(String::as_str)
                == Some("2")
        })
        .expect("droplet 2");
    assert_eq!(drop2.targets, vec!["2.2.2.2:9100".to_string()]);

    // Page 2 must actually have been fetched.
    assert!(
        stub.requests().iter().any(|r| r.contains("page=2")),
        "page 2 should have been fetched; requests={:?}",
        stub.requests()
    );

    // Clean stop: dropping the discovery joins the refresh thread promptly.
    drop(d);
    stub.stop();
}

/// A stub that captures the `Authorization` header of the first request and
/// returns an empty droplets list. Used to prove the client applies auth.
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
            w.write_json(200, r#"{"droplets":[]}"#);
        },
    ));

    AuthStub {
        server,
        auth_header,
    }
}

/// A `basic_auth`-configured DigitalOcean SD must send an
/// `Authorization: Basic <base64(user:pass)>` header — not go out
/// unauthenticated. `ZG8tdXNlcjpkby1wYXNz` is `base64("do-user:do-pass")`.
#[test]
fn basic_auth_sends_basic_authorization_header() {
    let stub = start_auth_stub();
    let cfg = DigitaloceanSdConfig {
        server: stub.server.local_addr().to_string(),
        port: 9100,
        auth: AuthConfig {
            basic: Some(("do-user".to_string(), "do-pass".to_string())),
            ..AuthConfig::default()
        },
        refresh_interval: Duration::from_millis(50),
        ..DigitaloceanSdConfig::default()
    };

    let d = DigitaloceanDiscovery::new(&cfg, "job").expect("new");

    let got = wait_until(Duration::from_secs(5), || {
        stub.auth_header.lock().unwrap().is_some()
    });
    assert!(got, "no request reached the stub");

    let header = stub.auth_header.lock().unwrap().clone().unwrap();
    assert_eq!(
        header, "Basic ZG8tdXNlcjpkby1wYXNz",
        "auth header={header:?}"
    );

    drop(d);
    stub.server.stop();
}
