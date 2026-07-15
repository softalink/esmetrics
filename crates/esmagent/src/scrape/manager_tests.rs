//! Tests for [`super::ScrapeManager`] — split out of `manager.rs` to keep
//! that file under the repo's 800-line cap. Still `mod tests` from
//! `manager.rs`'s point of view (see the `#[path]` attribute there), so
//! `super::*` below is `manager`'s items.

use super::*;
use crate::client::AuthConfig;
use crate::scrape::config::{
    Ec2SdConfig, GlobalConfig, KubernetesSdConfig, OvhcloudSdConfig, ScrapeConfig,
    ScrapeConfigFile, StaticConfig,
};
use crate::scrape::kubernetes::oauth2::OAuth2Config;
use crate::series::OwnedSeries;
use esm_http::{Request, ResponseWriter, Server};
use std::net::TcpListener;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

/// Captures every [`OwnedSeries`] pushed to it, for assertions. Mirrors
/// `crate::sink::tests::Cap`.
struct Cap(StdMutex<Vec<OwnedSeries>>);

impl Cap {
    fn new() -> Arc<Cap> {
        Arc::new(Cap(StdMutex::new(Vec::new())))
    }

    fn snapshot(&self) -> Vec<OwnedSeries> {
        self.0.lock().unwrap().clone()
    }
}

impl SeriesConsumer for Cap {
    fn push(&self, series: &[OwnedSeries]) {
        self.0.lock().unwrap().extend_from_slice(series);
    }
}

/// Serves a fixed Prometheus exposition-format `body` on `/metrics`; any
/// other path gets 404. Mirrors `scrapework_tests::start_metrics_stub`.
fn start_metrics_stub(body: &'static str) -> (String, Server) {
    let server = Server::bind("127.0.0.1:0").expect("bind stub server");
    let addr = server.local_addr().to_string();
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            if req.path() == "/metrics" {
                w.set_content_type("text/plain");
                w.write_body(body.as_bytes());
            } else {
                w.write_status(404);
            }
        },
    ));
    (addr, server)
}

/// A `host:port` that refuses connections immediately: bind an ephemeral
/// port, then drop the listener so nothing is behind it. Used to exercise a
/// genuinely unreachable target without relying on `scrape_timeout`
/// (connection refused fails fast, unlike a black-holed address).
fn dead_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind throwaway listener");
    let addr = listener.local_addr().expect("local addr").to_string();
    drop(listener);
    addr
}

/// Polls `check` until it returns `true` or `timeout` elapses. Mirrors
/// `crate::client::tests::wait_until`.
fn wait_until(mut check: impl FnMut() -> bool, timeout: Duration) -> bool {
    let start = Instant::now();
    loop {
        if check() {
            return true;
        }
        if start.elapsed() > timeout {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn static_job(job_name: &str, targets: &[&str]) -> ScrapeConfig {
    ScrapeConfig {
        job_name: job_name.to_string(),
        scheme: "http".to_string(),
        metrics_path: "/metrics".to_string(),
        honor_timestamps: true,
        static_configs: vec![StaticConfig {
            targets: targets.iter().map(|s| s.to_string()).collect(),
            labels: Default::default(),
        }],
        ..Default::default()
    }
}

fn cfg_with_jobs(jobs: Vec<ScrapeConfig>) -> ScrapeConfigFile {
    ScrapeConfigFile {
        global: GlobalConfig::default(),
        scrape_configs: jobs,
    }
}

fn find_series<'a>(series: &'a [OwnedSeries], name: &str) -> Option<&'a OwnedSeries> {
    series.iter().find(|s| {
        s.labels
            .iter()
            .any(|l| l.name == "__name__" && l.value == name)
    })
}

/// A stub k8s API server that answers every LIST with one `node` object and
/// every WATCH with an immediately-closing empty stream. Deliberately a
/// small local duplicate of `kubernetes::kubernetes_tests::start_node_stub`
/// rather than a shared helper (that one is private to a sibling module
/// tree) — see this task's brief.
fn start_k8s_node_stub() -> Server {
    let server = Server::bind("127.0.0.1:0").expect("bind k8s node stub");
    let list_body = r#"{"metadata":{"resourceVersion":"1"},"items":[
        {"metadata":{"name":"n1"},"spec":{"providerID":"aws:///i-1"},
         "status":{"addresses":[{"type":"InternalIP","address":"10.0.0.5"}],
                   "daemonEndpoints":{"kubeletEndpoint":{"port":10250}}}}]}"#
        .to_string();

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            if req.query().contains("watch=1") {
                w.write_body(b"");
            } else {
                w.write_json(200, &list_body);
            }
        },
    ));

    server
}

#[test]
fn build_providers_includes_kubernetes_sd() {
    let stub = start_k8s_node_stub();
    let base_url = format!("http://{}", stub.local_addr());

    let sc = ScrapeConfig {
        job_name: "k8s".to_string(),
        kubernetes_sd_configs: vec![KubernetesSdConfig {
            role: "node".to_string(),
            api_server: Some(base_url),
            ..KubernetesSdConfig::default()
        }],
        ..Default::default()
    };

    let mut providers = build_providers(&sc).expect("build_providers should succeed");
    assert_eq!(providers.len(), 1);
    assert!(
        wait_until(|| !providers[0].poll().is_empty(), Duration::from_secs(5)),
        "kubernetes_sd provider never surfaced the stub's node target"
    );

    stub.stop();
}

#[test]
fn reconcile_starts_worker_and_reports_active_up_target() {
    let (addr, server) = start_metrics_stub("up 1\nfoo_metric 42\n");
    let cap = Cap::new();
    let cfg = cfg_with_jobs(vec![static_job("node", &[addr.as_str()])]);
    let deps = ManagerDeps {
        global_relabel: None,
        consumer: cap.clone() as Arc<dyn SeriesConsumer>,
        suppress_scrape_errors: false,
    };

    let mut mgr = ScrapeManager::start(cfg, deps).expect("manager should start");
    mgr.reconcile_once(); // test seam: explicit second pass is a no-op diff, proving idempotency

    let scraped = wait_until(
        || {
            let snap = mgr.targets_snapshot();
            snap.active.len() == 1 && snap.active[0].health == Health::Up
        },
        Duration::from_secs(5),
    );
    assert!(scraped, "target never became Up in targets_snapshot()");

    let snap = mgr.targets_snapshot();
    assert_eq!(snap.active[0].scrape_pool, "node");
    assert_eq!(snap.active[0].scrape_url, format!("http://{addr}/metrics"));
    assert!(snap.dropped.is_empty());

    let pushed_up = wait_until(
        || find_series(&cap.snapshot(), "up").is_some(),
        Duration::from_secs(5),
    );
    assert!(pushed_up, "consumer never received the scraped `up` series");
    let captured = cap.snapshot();
    let up_series = find_series(&captured, "up").unwrap();
    assert_eq!(up_series.samples[0].value, 1.0);
    assert!(find_series(&captured, "foo_metric").is_some());

    mgr.stop();
    server.stop();
}

#[test]
fn reload_removing_job_stops_worker_and_flushes_stale_markers() {
    let (addr, server) = start_metrics_stub("up 1\n");
    let cap = Cap::new();
    let cfg = cfg_with_jobs(vec![static_job("node", &[addr.as_str()])]);
    let deps = ManagerDeps {
        global_relabel: None,
        consumer: cap.clone() as Arc<dyn SeriesConsumer>,
        suppress_scrape_errors: false,
    };

    let mut mgr = ScrapeManager::start(cfg, deps).expect("manager should start");

    let scraped = wait_until(
        || {
            let snap = mgr.targets_snapshot();
            !snap.active.is_empty() && snap.active[0].health == Health::Up
        },
        Duration::from_secs(5),
    );
    assert!(scraped, "target never became Up before reload");

    // Reload with the job removed: the worker must stop and its final
    // stale-marker flush must land in the consumer BEFORE reload() returns
    // (stop_job -> stop_worker joins the worker thread synchronously).
    let empty_cfg = cfg_with_jobs(vec![]);
    mgr.reload(empty_cfg).expect("reload should succeed");

    let snap = mgr.targets_snapshot();
    assert!(
        snap.active.is_empty(),
        "removed job's target must leave the active snapshot"
    );

    let series = cap.snapshot();
    let stale = series.iter().flat_map(|s| s.samples.iter()).any(|sample| {
        sample.value.is_nan() && sample.value.to_bits() == esm_common::decimal::STALE_NAN.to_bits()
    });
    assert!(stale, "no STALE_NAN marker was pushed on job removal");

    mgr.stop();
    server.stop();
}

#[test]
fn per_target_failure_is_isolated_from_a_healthy_sibling() {
    let (good_addr, server) = start_metrics_stub("up 1\n");
    let bad_addr = dead_addr();
    let cap = Cap::new();
    let cfg = cfg_with_jobs(vec![static_job(
        "mixed",
        &[good_addr.as_str(), bad_addr.as_str()],
    )]);
    let deps = ManagerDeps {
        global_relabel: None,
        consumer: cap.clone() as Arc<dyn SeriesConsumer>,
        suppress_scrape_errors: false,
    };

    let mut mgr = ScrapeManager::start(cfg, deps).expect("manager should start");

    let both_reported = wait_until(
        || {
            let snap = mgr.targets_snapshot();
            snap.active.len() == 2 && snap.active.iter().all(|t| t.health != Health::Unknown)
        },
        Duration::from_secs(5),
    );
    assert!(
        both_reported,
        "both targets never reported a terminal health"
    );

    let snap = mgr.targets_snapshot();
    let good = snap
        .active
        .iter()
        .find(|t| t.scrape_url.contains(&good_addr))
        .expect("good target missing from snapshot");
    let bad = snap
        .active
        .iter()
        .find(|t| t.scrape_url.contains(&bad_addr))
        .expect("bad target missing from snapshot");
    assert_eq!(
        good.health,
        Health::Up,
        "healthy sibling must stay Up despite the dead target"
    );
    assert_eq!(bad.health, Health::Down);
    assert!(
        bad.last_error.is_some(),
        "the dead target must report a last_error"
    );

    // The manager must still be fully functional: another reconcile pass
    // and snapshot read must work cleanly after a target failure.
    mgr.reconcile_once();
    assert_eq!(mgr.targets_snapshot().active.len(), 2);

    mgr.stop();
    server.stop();
}

#[test]
fn suppress_scrape_errors_flag_is_threaded_and_down_target_still_reports() {
    // Proves `-promscrape.suppressScrapeErrors` is threaded end-to-end
    // (ManagerDeps -> worker) and that suppressing the failure LOG does not
    // change the recorded failure STATE: a dead target still reports Down +
    // last_error in the snapshot. (We don't assert on log output here — log
    // capture is awkward — the point is the flag is wired, not dead.)
    let bad_addr = dead_addr();
    let cap = Cap::new();
    let cfg = cfg_with_jobs(vec![static_job("down", &[bad_addr.as_str()])]);
    let deps = ManagerDeps {
        global_relabel: None,
        consumer: cap.clone() as Arc<dyn SeriesConsumer>,
        suppress_scrape_errors: true,
    };

    let mgr = ScrapeManager::start(cfg, deps).expect("manager should start");

    let reported = wait_until(
        || {
            let snap = mgr.targets_snapshot();
            snap.active.len() == 1 && snap.active[0].health == Health::Down
        },
        Duration::from_secs(5),
    );
    assert!(
        reported,
        "dead target never reported Down with suppression on"
    );

    let snap = mgr.targets_snapshot();
    assert_eq!(snap.active[0].health, Health::Down);
    assert!(
        snap.active[0].last_error.is_some(),
        "last_error must still be recorded even when the failure log is suppressed"
    );

    mgr.stop();
}

// --- job_checksum secret sensitivity ------------------------------------
//
// `job_checksum` hashes the (redacting) `Debug` string plus the *real* secret
// values (`hash_secrets`). These tests prove that a change to a secret value
// alone flips the checksum — the property `ScrapeManager::reload` relies on to
// rebuild a job whose only change is a rotated/fixed credential — while an
// identical config (same secret) still hashes equal (stability). Coverage
// spans a shared-`AuthConfig` secret (bearer token), a provider-specific
// scalar secret (EC2 `secret_key`), a non-`Option` `String` secret
// (OVHcloud `application_secret`), and a nested redacting secret
// (k8s `oauth2.client_secret`).

#[test]
fn job_checksum_reflects_auth_bearer_change() {
    let g = GlobalConfig::default();
    let mut a = static_job("j", &["127.0.0.1:9000"]);
    a.auth = AuthConfig {
        bearer: Some("token-a".to_string()),
        ..Default::default()
    };

    // Identical config (same secret) => identical checksum (stability).
    assert_eq!(job_checksum(&a, &g), job_checksum(&a.clone(), &g));

    // Secret-only change => different checksum.
    let mut b = a.clone();
    b.auth = AuthConfig {
        bearer: Some("token-b".to_string()),
        ..Default::default()
    };
    assert_ne!(job_checksum(&a, &g), job_checksum(&b, &g));
}

#[test]
fn job_checksum_reflects_ec2_secret_key_change() {
    let g = GlobalConfig::default();
    let with_secret = |secret: &str| {
        let mut sc = static_job("ec2", &[]);
        sc.ec2_sd_configs = vec![Ec2SdConfig {
            region: "us-east-1".to_string(),
            access_key: "AKIAEXAMPLE".to_string(),
            secret_key: Some(secret.to_string()),
            ..Ec2SdConfig::default()
        }];
        sc
    };

    assert_eq!(
        job_checksum(&with_secret("s1"), &g),
        job_checksum(&with_secret("s1"), &g)
    );
    // `secret_key` is `<redacted>` in `Debug`; only `hash_secrets` catches this.
    assert_ne!(
        job_checksum(&with_secret("s1"), &g),
        job_checksum(&with_secret("s2"), &g)
    );
}

#[test]
fn job_checksum_reflects_ovhcloud_application_secret_change() {
    let g = GlobalConfig::default();
    let with_secret = |secret: &str| {
        let mut sc = static_job("ovh", &[]);
        sc.ovhcloud_sd_configs = vec![OvhcloudSdConfig {
            application_key: "app-key".to_string(),
            application_secret: secret.to_string(),
            consumer_key: "consumer".to_string(),
            ..OvhcloudSdConfig::default()
        }];
        sc
    };

    assert_ne!(
        job_checksum(&with_secret("secret-a"), &g),
        job_checksum(&with_secret("secret-b"), &g)
    );
}

#[test]
fn job_checksum_reflects_k8s_oauth2_client_secret_change() {
    let g = GlobalConfig::default();
    let with_secret = |secret: &str| {
        let mut sc = static_job("k8s", &[]);
        sc.kubernetes_sd_configs = vec![KubernetesSdConfig {
            role: "pod".to_string(),
            api_server: Some("http://127.0.0.1:1".to_string()),
            oauth2: Some(OAuth2Config {
                client_id: "client".to_string(),
                client_secret: Some(secret.to_string()),
                token_url: "http://127.0.0.1/token".to_string(),
                ..OAuth2Config::default()
            }),
            ..KubernetesSdConfig::default()
        }];
        sc
    };

    assert_eq!(
        job_checksum(&with_secret("cs1"), &g),
        job_checksum(&with_secret("cs1"), &g)
    );
    // `client_secret` is `<redacted>` in `OAuth2Config`'s hand-written `Debug`.
    assert_ne!(
        job_checksum(&with_secret("cs1"), &g),
        job_checksum(&with_secret("cs2"), &g)
    );
}

#[test]
fn reload_rebuilds_job_when_only_a_secret_changes() {
    // End-to-end proof through the real reload path: a job whose ONLY change
    // is a rotated bearer token must be rebuilt (its stored checksum updates),
    // not skipped by reload's `unchanged => continue` fast path.
    let cap = Cap::new();
    let mut job_a = static_job("node", &[dead_addr().as_str()]);
    job_a.auth = AuthConfig {
        bearer: Some("secret-a".to_string()),
        ..Default::default()
    };
    let deps = ManagerDeps {
        global_relabel: None,
        consumer: cap.clone() as Arc<dyn SeriesConsumer>,
        suppress_scrape_errors: true,
    };

    let mut mgr = ScrapeManager::start(cfg_with_jobs(vec![job_a.clone()]), deps)
        .expect("manager should start");
    let checksum_before = mgr.jobs.lock().unwrap().get("node").unwrap().checksum;

    let mut job_b = job_a.clone();
    job_b.auth = AuthConfig {
        bearer: Some("secret-b".to_string()),
        ..Default::default()
    };
    mgr.reload(cfg_with_jobs(vec![job_b]))
        .expect("reload should succeed");
    let checksum_after = mgr.jobs.lock().unwrap().get("node").unwrap().checksum;

    assert_ne!(
        checksum_before, checksum_after,
        "a secret-only change must rebuild the job (new checksum stored), not be skipped as unchanged"
    );

    mgr.stop();
}
