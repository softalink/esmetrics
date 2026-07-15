//! Tests for `esmagent`'s top-level `lib.rs` items (`run`/`run_dry`/
//! `run_scrape_config_dry`/`App`/`request_handler`/...) — split out to keep
//! `lib.rs` under the repo's 800-line cap. Still `mod tests` from `lib.rs`'s
//! point of view (see the `#[path]` attribute there), so `super::*` below is
//! the crate root's items.

use super::*;
use std::io::Write as _;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "esmagent-lib-test-{}-{}-{}",
        std::process::id(),
        name,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn write_file(dir: &Path, name: &str, contents: &str) -> String {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).expect("create relabel file");
    f.write_all(contents.as_bytes())
        .expect("write relabel file");
    path.to_string_lossy().to_string()
}

fn base_flags(url: &str) -> Flags {
    Flags {
        remote_write_urls: vec![url.to_string()],
        ..Flags::default()
    }
}

#[test]
fn run_dry_requires_at_least_one_remote_write_url() {
    let err = run_dry(&Flags::default()).unwrap_err();
    assert!(err.contains("-remoteWrite.url"), "{err}");
}

#[test]
fn dryrun_rejects_bad_relabel_config() {
    let dir = temp_dir("bad-relabel");
    let bad_path = write_file(
        &dir,
        "bad.yml",
        "- source_labels: [__name__]\n  regex: \".*\"\n  action: not_a_real_action\n",
    );
    let mut flags = base_flags("http://example.invalid/api/v1/write");
    flags.remote_write_relabel_config = bad_path;
    let err = run_dry(&flags).unwrap_err();
    assert!(err.contains("-remoteWrite.relabelConfig"), "{err}");

    let good_path = write_file(
        &dir,
        "good.yml",
        "- source_labels: [__name__]\n  regex: \"temp_.*\"\n  action: drop\n",
    );
    let mut flags = base_flags("http://example.invalid/api/v1/write");
    flags.remote_write_relabel_config = good_path;
    assert!(run_dry(&flags).is_ok());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn dryrun_rejects_bad_per_destination_url_relabel_config() {
    let dir = temp_dir("bad-url-relabel");
    let bad_path = write_file(
        &dir,
        "bad.yml",
        "- action: hashmod\n  source_labels: [__name__]\n",
    );
    let mut flags = base_flags("http://example.invalid/api/v1/write");
    flags.remote_write_url_relabel_configs = vec![bad_path];
    let err = run_dry(&flags).unwrap_err();
    assert!(err.contains("-remoteWrite.urlRelabelConfig"), "{err}");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn dryrun_validates_scrape_config() {
    let dir = temp_dir("scrape-dryrun");

    // A valid scrape config: run_dry succeeds.
    let good_path = write_file(
        &dir,
        "good.yml",
        "scrape_configs:\n  - job_name: node\n    static_configs:\n      - targets: ['h1:9100']\n",
    );
    let mut flags = base_flags("http://example.invalid/api/v1/write");
    flags.promscrape_config = Some(good_path.clone());
    assert!(run_dry(&flags).is_ok());
    // -promscrape.config.dryRun validates independent of -remoteWrite.url.
    assert!(run_scrape_config_dry(&flags).is_ok());

    // A scrape config referencing an unsupported (deferred) cloud-SD key:
    // run_dry fails.
    let cloud_sd_path = write_file(
        &dir,
        "cloud-sd.yml",
        "scrape_configs:\n  - job_name: k\n    azure_sd_configs: [{}]\n",
    );
    let mut flags = base_flags("http://example.invalid/api/v1/write");
    flags.promscrape_config = Some(cloud_sd_path);
    let err = run_dry(&flags).unwrap_err();
    assert!(err.contains("-promscrape.config"), "{err}");

    // A scrape config with a duplicate job_name: run_dry fails.
    let dup_path = write_file(
        &dir,
        "dup.yml",
        "scrape_configs:\n  - job_name: a\n    static_configs: [{targets: [x]}]\n  - job_name: a\n    static_configs: [{targets: [y]}]\n",
    );
    let mut flags = base_flags("http://example.invalid/api/v1/write");
    flags.promscrape_config = Some(dup_path);
    let err = run_dry(&flags).unwrap_err();
    assert!(err.contains("-promscrape.config"), "{err}");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn scrape_config_dry_run_requires_the_flag_to_be_set() {
    let err = run_scrape_config_dry(&Flags::default()).unwrap_err();
    assert!(err.contains("-promscrape.config"), "{err}");
}

#[test]
fn queue_dir_name_is_sanitized_and_indexed() {
    let name = queue_dir_name("http://a:9090/api/v1/write", 3);
    assert!(name.starts_with("3-"));
    assert!(!name.contains(':'));
    assert!(!name.contains('/'));
}

#[test]
fn drop_dangling_queues_removes_unexpected_subdirs_only() {
    let dir = temp_dir("dangling");
    let flags = Flags {
        remote_write_urls: vec!["http://a".to_string()],
        remote_write_tmp_data_path: dir.to_string_lossy().to_string(),
        ..Flags::default()
    };
    let keep = dir.join(queue_dir_name("http://a", 0));
    let drop_me = dir.join("1-http___stale");
    std::fs::create_dir_all(&keep).unwrap();
    std::fs::create_dir_all(&drop_me).unwrap();

    drop_dangling_queues(&flags);

    assert!(keep.exists(), "configured destination's queue must survive");
    assert!(!drop_me.exists(), "leftover queue must be removed");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn drop_dangling_queues_is_a_noop_when_tmp_data_path_is_missing() {
    let flags = Flags {
        remote_write_urls: vec!["http://a".to_string()],
        remote_write_tmp_data_path: "/nonexistent/esmagent/path/for/tests".to_string(),
        ..Flags::default()
    };
    // Must not panic.
    drop_dangling_queues(&flags);
}

#[test]
fn resolve_auth_reads_password_and_bearer_token_from_file() {
    let dir = temp_dir("auth-files");
    let pw_path = write_file(&dir, "pw", "s3cr3t\n");
    let tok_path = write_file(&dir, "tok", "tok-value\n");
    let flags = RemoteWriteAuthFlags {
        username: vec!["alice".to_string()],
        password_file: vec![pw_path],
        bearer_token_file: vec![tok_path],
        ..RemoteWriteAuthFlags::default()
    };
    let auth = resolve_auth(&flags, 0).unwrap();
    assert_eq!(
        auth.basic,
        Some(("alice".to_string(), "s3cr3t".to_string()))
    );
    assert_eq!(auth.bearer, Some("tok-value".to_string()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn resolve_auth_errors_on_unreadable_password_file() {
    let flags = RemoteWriteAuthFlags {
        password_file: vec!["/nonexistent/esmagent/secret/file".to_string()],
        ..RemoteWriteAuthFlags::default()
    };
    let err = resolve_auth(&flags, 0).unwrap_err();
    assert!(err.contains("passwordFile"), "{err}");
    // Never echoes a would-be secret value; only the path.
    assert!(err.contains("/nonexistent/esmagent/secret/file"), "{err}");
}

#[test]
fn run_rejects_config_with_no_destinations() {
    let err = match run(&Flags::default()) {
        Err(e) => e,
        Ok(_) => panic!("expected an error for a config with no -remoteWrite.url"),
    };
    assert!(err.contains("-remoteWrite.url"), "{err}");
}

#[test]
fn run_starts_serves_and_stops_cleanly() {
    // Stub remote-write endpoint so RemoteWriteCtx's worker pool has
    // somewhere to (successfully) deliver to; not strictly required for
    // this test (nothing is pushed), but keeps `App::stop`'s drain from
    // ever blocking on a dead endpoint.
    let stub = Server::bind("127.0.0.1:0").expect("bind stub");
    stub.serve(Arc::new(
        |_req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            w.write_status(204);
        },
    ));
    let stub_addr = stub.local_addr();

    let dir = temp_dir("run-e2e");
    let flags = Flags {
        remote_write_urls: vec![format!("http://{stub_addr}/api/v1/write")],
        remote_write_tmp_data_path: dir.to_string_lossy().to_string(),
        http_listen_addr: "127.0.0.1:0".to_string(),
        ..Flags::default()
    };

    let app = run(&flags).expect("run should succeed");
    let addr = app.local_addr();

    let (status, body) = http_get(addr, "/-/healthy");
    assert_eq!(status, 200);
    assert_eq!(body, "OK");

    let (status, _body) = http_get(addr, "/metrics");
    assert_eq!(status, 200);

    app.stop();
    stub.stop();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn metrics_endpoint_requires_auth_key_when_set() {
    let dir = temp_dir("metrics-auth");
    let flags = Flags {
        remote_write_urls: vec!["http://example.invalid/api/v1/write".to_string()],
        remote_write_tmp_data_path: dir.to_string_lossy().to_string(),
        http_listen_addr: "127.0.0.1:0".to_string(),
        metrics_auth_key: "secret".to_string(),
        ..Flags::default()
    };
    let app = run(&flags).expect("run should succeed");
    let addr = app.local_addr();

    let (status, _) = http_get(addr, "/metrics");
    assert_eq!(status, 401);
    let (status, _) = http_get(addr, "/metrics?authKey=secret");
    assert_eq!(status, 200);

    app.stop();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn targets_route_returns_404_when_scrape_engine_disabled() {
    let dir = temp_dir("targets-route-disabled");
    let flags = Flags {
        remote_write_urls: vec!["http://example.invalid/api/v1/write".to_string()],
        remote_write_tmp_data_path: dir.to_string_lossy().to_string(),
        http_listen_addr: "127.0.0.1:0".to_string(),
        ..Flags::default()
    };
    let app = run(&flags).expect("run should succeed");
    let addr = app.local_addr();

    let (status, _) = http_get(addr, "/api/v1/targets");
    assert_eq!(status, 404);

    app.stop();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn targets_route_serves_scraped_targets_and_becomes_healthy() {
    // Stub remote-write endpoint (forwarding destination for both pushed
    // and scraped series).
    let rw_stub = Server::bind("127.0.0.1:0").expect("bind rw stub");
    rw_stub.serve(Arc::new(
        |_req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            w.write_status(204);
        },
    ));
    let rw_stub_addr = rw_stub.local_addr();

    // Stub scrape target: a minimal Prometheus exposition body, served
    // at every path (the scrape engine requests `/metrics`).
    let scrape_stub = Server::bind("127.0.0.1:0").expect("bind scrape stub");
    scrape_stub.serve(Arc::new(
        |_req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            w.set_content_type("text/plain; version=0.0.4");
            w.write_body(b"up_test 1\n");
        },
    ));
    let scrape_stub_addr = scrape_stub.local_addr();

    let dir = temp_dir("targets-route-enabled");
    // No `scrape_interval`/`scrape_timeout` override needed: a worker
    // scrapes immediately on start (see `scrape::manager::worker`'s module
    // doc), so the target reaches its first result well before the
    // (default 60s) interval's first tick — and a shorter interval here
    // would need a shorter `scrape_timeout` too, or trip the
    // timeout-exceeds-interval validation error.
    let scrape_cfg_path = write_file(
        &dir,
        "scrape.yml",
        &format!(
            "scrape_configs:\n  - job_name: e2e\n    static_configs:\n      \
             - targets: ['{scrape_stub_addr}']\n"
        ),
    );

    let flags = Flags {
        remote_write_urls: vec![format!("http://{rw_stub_addr}/api/v1/write")],
        remote_write_tmp_data_path: dir.to_string_lossy().to_string(),
        http_listen_addr: "127.0.0.1:0".to_string(),
        promscrape_config: Some(scrape_cfg_path),
        ..Flags::default()
    };

    let app = run(&flags).expect("run should succeed");
    let addr = app.local_addr();

    // Poll briefly: the target is upserted (Health::Unknown) synchronously
    // during `run`, then its worker scrapes immediately, so it should
    // reach `up` well within this window.
    let mut healthy = false;
    for _ in 0..100 {
        let (status, body) = http_get(addr, "/api/v1/targets");
        assert_eq!(status, 200);
        let json: serde_json::Value =
            serde_json::from_str(&body).expect("targets response must be valid JSON");
        let active = json["data"]["activeTargets"]
            .as_array()
            .expect("activeTargets must be an array");
        if active.iter().any(|t| {
            t["scrapeUrl"]
                .as_str()
                .is_some_and(|u| u.contains(&scrape_stub_addr.to_string()))
                && t["health"] == "up"
        }) {
            healthy = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        healthy,
        "expected the scraped target to become healthy in /api/v1/targets"
    );

    // `state=active` filter: droppedTargets stays empty.
    let (status, body) = http_get(addr, "/api/v1/targets?state=active");
    assert_eq!(status, 200);
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["data"]["droppedTargets"].as_array().unwrap().len(), 0);

    app.stop();
    rw_stub.stop();
    scrape_stub.stop();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Minimal raw-HTTP/1.1 client, mirroring `esmalert::web::api::tests`'s
/// `send_request` helper so this in-process test doesn't need a full
/// HTTP client dependency.
fn http_get(addr: std::net::SocketAddr, target: &str) -> (u16, String) {
    use std::io::Read;
    use std::net::TcpStream;
    let mut stream = TcpStream::connect(addr).expect("connect failed");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set_read_timeout failed");
    let req = format!("GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).expect("write failed");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read failed (or timed out)");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed response: {response:?}"));
    let status_line = head.lines().next().unwrap_or_default();
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (status, body.to_string())
}
