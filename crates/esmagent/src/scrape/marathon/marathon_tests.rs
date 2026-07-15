//! Stub-server tests for [`super::MarathonDiscovery`] — split out per this
//! crate's `#[path]`-sibling convention to keep `mod.rs` under the 800-line
//! cap.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_http::{Request, ResponseWriter, Server, ServerConfig};

use super::*;
use crate::client::AuthConfig;
use crate::scrape::config::MarathonSdConfig;

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

/// A running stub Marathon server serving `/v2/apps` with one app + one task.
struct MarathonStub {
    server: Server,
    requests: Arc<Mutex<Vec<String>>>,
}

impl MarathonStub {
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

const APPS_BODY: &str = r#"{"apps":[{
    "id":"/web",
    "tasks":[{"id":"web.task-1","host":"host-a","ports":[31000],
              "ipAddresses":[{"ipAddress":"10.0.0.5","protocol":"IPv4"}]}],
    "labels":{"team":"backend"},
    "container":{"docker":{"image":"registry/web:1"},
                 "portMappings":[{"labels":{"scrape":"yes"},"containerPort":8080,"hostPort":0}]},
    "networks":[{"mode":"container/bridge"}],
    "requirePorts":false
}]}"#;

fn start_marathon_stub() -> MarathonStub {
    let server = Server::bind("127.0.0.1:0").expect("bind marathon stub");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_for_handler = Arc::clone(&requests);

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let path = req.path().to_string();
            requests_for_handler
                .lock()
                .unwrap()
                .push(format!("{}?{}", path, req.query()));
            if path == "/v2/apps/" {
                w.write_json(200, APPS_BODY);
            } else {
                w.write_json(404, r#"{"message":"not found"}"#);
            }
        },
    ));

    MarathonStub { server, requests }
}

/// The stub serves one app/task; it must surface as a target with the correct
/// `__address__` and `__meta_marathon_*` labels, and the query must carry the
/// `embed=apps.tasks` arg. Clean stop.
#[test]
fn discovers_app_task_target() {
    let stub = start_marathon_stub();
    let cfg = MarathonSdConfig {
        servers: vec![stub.addr()],
        auth: AuthConfig {
            bearer: Some("tok".to_string()),
            ..AuthConfig::default()
        },
        refresh_interval: Duration::from_millis(50),
        ..MarathonSdConfig::default()
    };

    let mut d = MarathonDiscovery::new(&cfg, "job").expect("new");

    let found = wait_until(Duration::from_secs(5), || {
        d.poll()
            .iter()
            .any(|g| g.labels.get("__meta_marathon_app").map(String::as_str) == Some("/web"))
    });
    assert!(
        found,
        "app target never discovered; requests={:?}",
        stub.requests()
    );

    let groups = d.poll();
    assert_eq!(groups.len(), 1, "{groups:#?}");
    let g = &groups[0];
    // container/bridge is NOT container mode -> host networking uses task host.
    assert_eq!(g.targets, vec!["host-a:31000".to_string()]);
    assert_eq!(g.labels["__meta_marathon_task"], "web.task-1");
    assert_eq!(g.labels["__meta_marathon_image"], "registry/web:1");
    assert_eq!(g.labels["__meta_marathon_app_label_team"], "backend");
    assert_eq!(g.labels["__meta_marathon_port_mapping_label_scrape"], "yes");
    assert_eq!(g.labels["__meta_marathon_port_index"], "0");
    assert_eq!(g.source, "job/marathon");

    // The embed arg must have been sent.
    assert!(
        stub.requests()
            .iter()
            .any(|r| r.contains("embed=apps.tasks")),
        "embed arg missing; requests={:?}",
        stub.requests()
    );

    // Clean stop: dropping the discovery joins the refresh thread promptly.
    drop(d);
    stub.stop();
}

/// A `bearer_token`-configured Marathon SD must send an
/// `Authorization: Bearer <token>` header.
#[test]
fn bearer_auth_sends_authorization_header() {
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
            w.write_json(200, r#"{"apps":[]}"#);
        },
    ));

    let cfg = MarathonSdConfig {
        servers: vec![server.local_addr().to_string()],
        auth: AuthConfig {
            bearer: Some("marathon-secret".to_string()),
            ..AuthConfig::default()
        },
        refresh_interval: Duration::from_millis(50),
        ..MarathonSdConfig::default()
    };

    let d = MarathonDiscovery::new(&cfg, "job").expect("new");
    let got = wait_until(Duration::from_secs(5), || {
        auth_header.lock().unwrap().is_some()
    });
    assert!(got, "no request reached the stub");
    assert_eq!(
        auth_header.lock().unwrap().clone().unwrap(),
        "Bearer marathon-secret"
    );

    drop(d);
    server.stop();
}
