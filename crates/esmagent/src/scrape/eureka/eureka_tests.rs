//! Stub-server tests for [`super::EurekaDiscovery`] — split out per this
//! crate's `#[path]`-sibling convention to keep `mod.rs` under the 800-line
//! cap. The key assertion is that a `GET /apps` XML response surfaces one
//! target per enabled instance with the expected `__meta_eureka_*` labels, and
//! that dropping the discovery joins the refresh thread promptly (clean stop).

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_http::{Request, ResponseWriter, Server, ServerConfig};

use super::*;
use crate::client::AuthConfig;
use crate::scrape::config::EurekaSdConfig;

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

/// A running stub Eureka API. `requests` records every request's `path` in
/// arrival order (so the test can prove `/apps` was fetched).
struct EurekaStub {
    server: Server,
    requests: Arc<Mutex<Vec<String>>>,
}

impl EurekaStub {
    fn addr(&self) -> String {
        self.server.local_addr().to_string()
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

const APPS_XML: &str = r#"<applications>
  <application>
    <name>HELLO-NETFLIX-OSS</name>
    <instance>
      <hostName>98de25ebef42</hostName>
      <app>HELLO-NETFLIX-OSS</app>
      <ipAddr>10.10.0.3</ipAddr>
      <status>UP</status>
      <port enabled="true">8080</port>
      <securePort enabled="false">443</securePort>
      <countryId>1</countryId>
      <dataCenterInfo class="com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo">
        <name>MyOwn</name>
      </dataCenterInfo>
      <metadata><zone>us-east-1a</zone></metadata>
      <homePageUrl>http://98de25ebef42:8080/</homePageUrl>
      <vipAddress>HELLO-NETFLIX-OSS</vipAddress>
      <instanceId>inst-1</instanceId>
    </instance>
  </application>
</applications>"#;

/// Serves the `APPS_XML` fixture for any `GET /apps`.
fn start_eureka_stub() -> EurekaStub {
    let server = Server::bind("127.0.0.1:0").expect("bind eureka stub");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_for_handler = Arc::clone(&requests);

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            requests_for_handler
                .lock()
                .unwrap()
                .push(req.path().to_string());
            w.set_content_type("application/xml");
            w.write_body(APPS_XML.as_bytes());
        },
    ));

    EurekaStub { server, requests }
}

/// A `/apps` XML response must yield one target with the enabled instance's
/// `hostName:port` `__address__` and its `__meta_eureka_*` labels (including a
/// metadata-derived label). Clean stop.
#[test]
fn discovers_instance_from_apps() {
    let stub = start_eureka_stub();
    let cfg = EurekaSdConfig {
        server: stub.addr(),
        auth: AuthConfig {
            bearer: Some("tok".to_string()),
            ..AuthConfig::default()
        },
        refresh_interval: Duration::from_millis(50),
        ..EurekaSdConfig::default()
    };

    let mut d = EurekaDiscovery::new(&cfg, "job").expect("new");

    let found = wait_until(Duration::from_secs(5), || !d.poll().is_empty());
    assert!(
        found,
        "instance never discovered; requests={:?}",
        stub.requests()
    );

    let groups = d.poll();
    assert_eq!(groups.len(), 1, "groups={groups:?}");
    let g = &groups[0];
    assert_eq!(g.targets, vec!["98de25ebef42:8080".to_string()]);
    assert_eq!(g.labels["__meta_eureka_app_name"], "HELLO-NETFLIX-OSS");
    assert_eq!(g.labels["__meta_eureka_app_instance_ip_addr"], "10.10.0.3");
    assert_eq!(g.labels["__meta_eureka_app_instance_port"], "8080");
    assert_eq!(g.labels["__meta_eureka_app_instance_port_enabled"], "true");
    assert_eq!(g.labels["__meta_eureka_app_instance_secure_port"], "443");
    assert_eq!(
        g.labels["__meta_eureka_app_instance_datacenterinfo_name"],
        "MyOwn"
    );
    assert_eq!(
        g.labels["__meta_eureka_app_instance_metadata_zone"],
        "us-east-1a"
    );
    assert_eq!(g.labels["instance"], "inst-1");
    assert_eq!(g.source, "job/eureka");

    // `/apps` must actually have been fetched.
    assert!(
        stub.requests().iter().any(|r| r == "/apps"),
        "/apps should have been fetched; requests={:?}",
        stub.requests()
    );

    // Clean stop: dropping the discovery joins the refresh thread promptly.
    drop(d);
    stub.server.stop();
}

/// A stub that captures the `Authorization` header of the first request and
/// returns an empty `<applications/>` body. Used to prove the client applies
/// basic auth.
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
            w.set_content_type("application/xml");
            w.write_body(b"<applications/>");
        },
    ));

    AuthStub {
        server,
        auth_header,
    }
}

/// A `basic_auth`-configured Eureka SD must send an
/// `Authorization: Basic <base64(user:pass)>` header. `ZXUtdXNlcjpldS1wYXNz`
/// is `base64("eu-user:eu-pass")`.
#[test]
fn basic_auth_sends_basic_authorization_header() {
    let stub = start_auth_stub();
    let cfg = EurekaSdConfig {
        server: stub.server.local_addr().to_string(),
        auth: AuthConfig {
            basic: Some(("eu-user".to_string(), "eu-pass".to_string())),
            ..AuthConfig::default()
        },
        refresh_interval: Duration::from_millis(50),
        ..EurekaSdConfig::default()
    };

    let d = EurekaDiscovery::new(&cfg, "job").expect("new");

    let got = wait_until(Duration::from_secs(5), || {
        stub.auth_header.lock().unwrap().is_some()
    });
    assert!(got, "no request reached the stub");

    let header = stub.auth_header.lock().unwrap().clone().unwrap();
    assert_eq!(
        header, "Basic ZXUtdXNlcjpldS1wYXNz",
        "auth header={header:?}"
    );

    drop(d);
    stub.server.stop();
}
