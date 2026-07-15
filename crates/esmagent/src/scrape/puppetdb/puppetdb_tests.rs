//! Stub-server tests for [`super::PuppetdbDiscovery`] — split out per this
//! crate's `#[path]`-sibling convention to keep `mod.rs` under the 800-line
//! cap.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_http::{Method, Request, ResponseWriter, Server};

use super::*;
use crate::scrape::config::PuppetdbSdConfig;

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

/// A running stub PuppetDB server. `bodies` records the POST body of every
/// request to `/pdb/query/v4`, in arrival order.
struct PuppetdbStub {
    server: Server,
    bodies: Arc<Mutex<Vec<String>>>,
}

impl PuppetdbStub {
    /// The `http://host:port` base URL a config's `url` should point at.
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

/// The single-resource fixture from upstream `puppetdb_test.go`, trimmed to
/// the fields this discovery surfaces plus one list-valued parameter.
const RESOURCE_JSON: &str = r#"[
   {
      "certname": "edinburgh.example.com",
      "environment": "prod",
      "exported": false,
      "file": "/etc/puppetlabs/init.pp",
      "parameters": { "docroot": "/var/www/html", "options": ["Indexes","FollowSymLinks"] },
      "resource": "49af83866dc5a1518968b68e58a25319107afe11",
      "tags": ["apache", "vhost"],
      "title": "default-ssl",
      "type": "Apache::Vhost"
   }
]"#;

/// Starts a stub PuppetDB server. A `POST /pdb/query/v4` records its body and
/// returns [`RESOURCE_JSON`]; any other request yields `[]`.
fn start_puppetdb_stub() -> PuppetdbStub {
    let server = Server::bind("127.0.0.1:0").expect("bind puppetdb stub");
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let bodies_for_handler = Arc::clone(&bodies);

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let is_query = req.method() == Method::Post && req.path() == "/pdb/query/v4";
            if is_query {
                let mut buf = Vec::new();
                let _ = req.read_body_to(&mut buf, 1 << 20);
                bodies_for_handler
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buf).into_owned());
                w.write_json(200, RESOURCE_JSON);
            } else {
                w.write_json(200, "[]");
            }
        },
    ));

    PuppetdbStub { server, bodies }
}

const PQL: &str = r#"resources { type = "Class" and title = "Prometheus::Node_exporter" }"#;

/// The stub returns one resource; it must surface as a target with the correct
/// `__address__` (certname:port) and base `__meta_puppetdb_*` labels, and the
/// query POST body must carry the configured PQL.
#[test]
fn discovers_resource_target_and_posts_query() {
    let stub = start_puppetdb_stub();
    let cfg = PuppetdbSdConfig {
        url: stub.url(),
        query: PQL.to_string(),
        port: 9100,
        refresh_interval: Duration::from_millis(50),
        ..PuppetdbSdConfig::default()
    };

    let mut d = PuppetdbDiscovery::new(&cfg, "job").expect("new");

    let found = wait_until(Duration::from_secs(5), || {
        d.poll().iter().any(|t| {
            t.labels.get("__meta_puppetdb_certname").map(String::as_str)
                == Some("edinburgh.example.com")
        })
    });
    assert!(
        found,
        "resource target never discovered; bodies={:?}",
        stub.bodies()
    );

    let groups = d.poll();
    assert_eq!(groups.len(), 1);
    let g = &groups[0];
    assert_eq!(g.targets, vec!["edinburgh.example.com:9100".to_string()]);
    assert_eq!(g.labels["__meta_puppetdb_query"], PQL);
    assert_eq!(g.labels["__meta_puppetdb_title"], "default-ssl");
    assert_eq!(g.labels["__meta_puppetdb_exported"], "false");
    assert_eq!(g.labels["__meta_puppetdb_tags"], ",apache,vhost,");
    assert_eq!(g.source, "job/puppetdb");
    // include_parameters defaults off -> no parameter labels.
    assert!(g
        .labels
        .keys()
        .all(|k| !k.starts_with("__meta_puppetdb_parameter_")));

    // The POST body must be the JSON-wrapped PQL query.
    let body = stub.bodies().into_iter().next().expect("a query POST body");
    let parsed: serde_json::Value = serde_json::from_str(&body).expect("body is JSON");
    assert_eq!(parsed["query"].as_str(), Some(PQL), "body={body}");

    // Clean stop: dropping the discovery joins the refresh thread promptly.
    drop(d);
    stub.stop();
}

/// With `include_parameters: true`, end-to-end discovery surfaces the
/// sanitized `__meta_puppetdb_parameter_*` labels (string + comma-joined list).
#[test]
fn include_parameters_surfaces_parameter_labels() {
    let stub = start_puppetdb_stub();
    let cfg = PuppetdbSdConfig {
        url: stub.url(),
        query: PQL.to_string(),
        include_parameters: true,
        refresh_interval: Duration::from_millis(50),
        ..PuppetdbSdConfig::default()
    };

    let mut d = PuppetdbDiscovery::new(&cfg, "job").expect("new");

    let found = wait_until(Duration::from_secs(5), || {
        d.poll()
            .iter()
            .any(|t| t.labels.contains_key("__meta_puppetdb_parameter_docroot"))
    });
    assert!(
        found,
        "parameter labels never surfaced; bodies={:?}",
        stub.bodies()
    );

    let groups = d.poll();
    let g = &groups[0];
    assert_eq!(
        g.labels["__meta_puppetdb_parameter_docroot"],
        "/var/www/html"
    );
    assert_eq!(
        g.labels["__meta_puppetdb_parameter_options"],
        "Indexes,FollowSymLinks"
    );
    // default port 80 when unset.
    assert_eq!(g.targets, vec!["edinburgh.example.com:80".to_string()]);

    drop(d);
    stub.stop();
}

/// A programmatically-built config with a missing/bad `url` or `query` fails
/// `new()` (upstream `newAPIConfig` contract), rather than spawning a doomed
/// refresh thread.
#[test]
fn new_rejects_bad_config() {
    let bad_url = PuppetdbSdConfig {
        url: String::new(),
        query: PQL.to_string(),
        ..PuppetdbSdConfig::default()
    };
    assert!(PuppetdbDiscovery::new(&bad_url, "job").is_err());

    let bad_query = PuppetdbSdConfig {
        url: "https://puppetdb.example.com".into(),
        query: String::new(),
        ..PuppetdbSdConfig::default()
    };
    assert!(PuppetdbDiscovery::new(&bad_query, "job").is_err());
}
