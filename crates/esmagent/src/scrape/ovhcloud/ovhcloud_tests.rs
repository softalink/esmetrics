//! Stub-server tests for [`super::OvhcloudDiscovery`] — split out per this
//! crate's `#[path]`-sibling convention to keep `mod.rs` under the 800-line
//! cap. They mirror upstream `mock_server_test.go`: a stub serves `/auth/time`,
//! the service listing, and each instance's detail + `/ips`, and the discovery
//! must surface the instance's `__meta_ovhcloud_*` labels and bare-IP
//! `__address__`, then stop cleanly.

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

/// A running stub OVH API. `requests` records every request's path in arrival
/// order (so a test can prove `/auth/time` and the detail GETs happened).
struct OvhStub {
    server: Server,
    requests: Arc<Mutex<Vec<String>>>,
}

impl OvhStub {
    fn url(&self) -> String {
        format!("http://{}", self.server.local_addr())
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }

    fn stop(&self) {
        self.server.stop();
    }
}

/// Starts a stub serving the VPS discovery flow for one VPS.
fn start_vps_stub() -> OvhStub {
    let server = Server::bind("127.0.0.1:0").expect("bind ovhcloud vps stub");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_for_handler = Arc::clone(&requests);

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let path = req.path().to_string();
            requests_for_handler.lock().unwrap().push(path.clone());
            match path.as_str() {
                "/auth/time" => w.write_json(200, "1700000000"),
                "/vps" => w.write_json(200, r#"["vps-000e0e00.vps.ovh.ca"]"#),
                "/vps/vps-000e0e00.vps.ovh.ca" => w.write_json(
                    200,
                    r#"{
                        "model": {
                            "name": "vps-starter-1-2-20",
                            "offer": "VPS vps2020-starter-1-2-20",
                            "maximumAdditionnalIp": 16,
                            "version": "2019v1",
                            "datacenter": [],
                            "vcore": 1,
                            "memory": 2048,
                            "disk": 20
                        },
                        "netbootMode": "local",
                        "cluster": "",
                        "name": "vps-000e0e00.vps.ovh.ca",
                        "displayName": "vps-000e0e00.vps.ovh.ca",
                        "vcore": 1,
                        "zone": "Region OpenStack: os-syd2",
                        "memoryLimit": 2048,
                        "offerType": "ssd",
                        "state": "running"
                    }"#,
                ),
                "/vps/vps-000e0e00.vps.ovh.ca/ips" => {
                    w.write_json(200, r#"["139.99.154.158","2402:1f00:8100:401::bb6"]"#)
                }
                _ => w.write_json(400, "\"bad path\""),
            }
        },
    ));

    OvhStub { server, requests }
}

/// Starts a stub serving the dedicated-server discovery flow for one server.
fn start_dedicated_stub() -> OvhStub {
    let server = Server::bind("127.0.0.1:0").expect("bind ovhcloud dedicated stub");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_for_handler = Arc::clone(&requests);

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let path = req.path().to_string();
            requests_for_handler.lock().unwrap().push(path.clone());
            match path.as_str() {
                "/auth/time" => w.write_json(200, "1700000000"),
                "/dedicated/server" => w.write_json(200, r#"["ns0000000.ip-00-00-000.eu"]"#),
                "/dedicated/server/ns0000000.ip-00-00-000.eu" => w.write_json(
                    200,
                    r#"{
                        "name": "ns0000000.ip-00-00-000.eu",
                        "datacenter": "gra2",
                        "linkSpeed": 1000,
                        "reverse": "ns0000000.ip-00-00-000.eu",
                        "serverId": 1000000,
                        "rack": "G000A00",
                        "supportLevel": "pro",
                        "commercialRange": "RISE-3",
                        "state": "ok",
                        "os": "centos7_64",
                        "noIntervention": false
                    }"#,
                ),
                "/dedicated/server/ns0000000.ip-00-00-000.eu/ips" => {
                    w.write_json(200, r#"["2001:40d0:302:8874::/64","50.75.126.113/32"]"#)
                }
                _ => w.write_json(400, "\"bad path\""),
            }
        },
    ));

    OvhStub { server, requests }
}

fn vps_cfg(url: String) -> OvhcloudSdConfig {
    OvhcloudSdConfig {
        endpoint: DEFAULT_ENDPOINT.to_string(),
        application_key: "app".into(),
        application_secret: "secret".into(),
        consumer_key: "consumer".into(),
        service: SERVICE_VPS.into(),
        refresh_interval: Duration::from_millis(50),
        api_url_override: url,
    }
}

/// The VPS flow (`/auth/time` → `/vps` → detail → `/ips`) must surface the
/// VPS's `__meta_ovhcloud_vps_*` labels and its v4 bare-IP `__address__`, then
/// stop cleanly.
#[test]
fn discovers_vps_target() {
    let stub = start_vps_stub();
    let cfg = vps_cfg(stub.url());

    let mut d = OvhcloudDiscovery::new(&cfg, "job").expect("new");

    let found = wait_until(Duration::from_secs(5), || {
        d.poll()
            .iter()
            .any(|g| g.labels.contains_key("__meta_ovhcloud_vps_name"))
    });
    assert!(
        found,
        "vps never discovered; requests={:?}",
        stub.requests()
    );

    let groups = d.poll();
    assert_eq!(groups.len(), 1, "groups={groups:?}");
    let g = &groups[0];
    assert_eq!(g.targets, vec!["139.99.154.158".to_string()]);
    assert_eq!(g.source, "job/ovhcloud");
    assert_eq!(g.labels["instance"], "vps-000e0e00.vps.ovh.ca");
    assert_eq!(
        g.labels["__meta_ovhcloud_vps_offer"],
        "VPS vps2020-starter-1-2-20"
    );
    assert_eq!(g.labels["__meta_ovhcloud_vps_ipv4"], "139.99.154.158");
    assert_eq!(
        g.labels["__meta_ovhcloud_vps_ipv6"],
        "2402:1f00:8100:401::bb6"
    );

    // The server-clock sync must have happened.
    assert!(
        stub.requests().iter().any(|r| r == "/auth/time"),
        "requests={:?}",
        stub.requests()
    );

    drop(d);
    stub.stop();
}

/// The dedicated-server flow must surface the server's
/// `__meta_ovhcloud_dedicated_server_*` labels and its `/32` v4 `__address__`
/// (the `/64` v6 prefix is dropped), then stop cleanly.
#[test]
fn discovers_dedicated_server_target() {
    let stub = start_dedicated_stub();
    let cfg = OvhcloudSdConfig {
        service: SERVICE_DEDICATED_SERVER.into(),
        ..vps_cfg(stub.url())
    };

    let mut d = OvhcloudDiscovery::new(&cfg, "job").expect("new");

    let found = wait_until(Duration::from_secs(5), || {
        d.poll().iter().any(|g| {
            g.labels
                .contains_key("__meta_ovhcloud_dedicated_server_name")
        })
    });
    assert!(
        found,
        "dedicated server never discovered; requests={:?}",
        stub.requests()
    );

    let groups = d.poll();
    assert_eq!(groups.len(), 1, "groups={groups:?}");
    let g = &groups[0];
    assert_eq!(g.targets, vec!["50.75.126.113".to_string()]);
    assert_eq!(g.labels["instance"], "ns0000000.ip-00-00-000.eu");
    assert_eq!(g.labels["__meta_ovhcloud_dedicated_server_state"], "ok");
    assert_eq!(
        g.labels["__meta_ovhcloud_dedicated_server_ipv4"],
        "50.75.126.113"
    );
    assert_eq!(g.labels["__meta_ovhcloud_dedicated_server_ipv6"], "");

    drop(d);
    stub.stop();
}

/// An unknown endpoint fails `new()` fast (bad config, not down-at-startup).
#[test]
fn unknown_endpoint_fails_new() {
    let cfg = OvhcloudSdConfig {
        endpoint: "does-not-exist".into(),
        api_url_override: String::new(),
        service: SERVICE_VPS.into(),
        ..OvhcloudSdConfig::default()
    };
    assert!(OvhcloudDiscovery::new(&cfg, "job").is_err());
}
