//! Stub-server tests for [`super::YandexcloudDiscovery`] — split out per this
//! crate's `#[path]`-sibling convention to keep `mod.rs` under the 800-line cap.
//! One in-process stub serves `/endpoints`, both credential modes (the IAM
//! token exchange and the metadata token), the resource-manager
//! organizations/clouds/folders enumeration, and a paginated `instances.list`
//! (page 1 carries a `nextPageToken`, page 2 does not).

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

/// A running stub Yandex Cloud endpoint. `requests` records every request's
/// `path?query` in arrival order (so a test can prove the token/enumeration/
/// page-2 requests were made).
struct YandexStub {
    server: Server,
    requests: Arc<Mutex<Vec<String>>>,
}

impl YandexStub {
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

const INSTANCES_PAGE1: &str = r#"{
  "instances": [
    {
      "id": "i1",
      "name": "inst-1",
      "fqdn": "inst-1.ru-central1.internal",
      "status": "RUNNING",
      "folderId": "folder-1",
      "platformId": "s2.micro",
      "resources": {"cores": "2", "coreFraction": "100", "memory": "4294967296"},
      "networkInterfaces": [
        {"index": "0", "primaryV4Address": {"address": "10.0.0.1",
          "oneToOneNat": {"address": "1.2.3.4"}}}
      ],
      "labels": {"env": "prod"}
    }
  ],
  "nextPageToken": "tok2"
}"#;

const INSTANCES_PAGE2: &str = r#"{
  "instances": [
    {
      "id": "i2",
      "name": "inst-2",
      "fqdn": "inst-2.ru-central1.internal",
      "status": "RUNNING",
      "folderId": "folder-1",
      "networkInterfaces": [
        {"index": "0", "primaryV4Address": {"address": "10.0.0.2"}}
      ]
    }
  ]
}"#;

/// Starts a stub Yandex Cloud endpoint dispatching on the request path. The
/// `/endpoints` response maps all four service ids back at this stub's address.
fn start_yandex_stub() -> YandexStub {
    let server = Server::bind("127.0.0.1:0").expect("bind yandexcloud stub");
    let addr = server.local_addr().to_string();
    let endpoints_json = format!(
        r#"{{"endpoints":[
          {{"id":"iam","address":"{addr}"}},
          {{"id":"compute","address":"{addr}"}},
          {{"id":"resource-manager","address":"{addr}"}},
          {{"id":"organization-manager","address":"{addr}"}}
        ]}}"#
    );
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

            if path.contains("/endpoints") {
                w.write_json(200, &endpoints_json);
            } else if path.contains("/iam/v1/tokens") {
                w.write_json(
                    200,
                    r#"{"iamToken":"iam-tok","expiresAt":"2999-01-01T00:00:00Z"}"#,
                );
            } else if path.ends_with("/token") {
                w.write_json(
                    200,
                    r#"{"access_token":"meta-tok","expires_in":3600,"token_type":"Bearer"}"#,
                );
            } else if path.contains("/organizations") {
                w.write_json(200, r#"{"organizations":[{"id":"org-1"}]}"#);
            } else if path.contains("/clouds") {
                w.write_json(200, r#"{"clouds":[{"id":"cloud-1"}]}"#);
            } else if path.contains("/folders") {
                w.write_json(200, r#"{"folders":[{"id":"folder-1"}]}"#);
            } else if path.contains("/instances") {
                if query.contains("pageToken=") {
                    w.write_json(200, INSTANCES_PAGE2);
                } else {
                    w.write_json(200, INSTANCES_PAGE1);
                }
            } else {
                w.write_status(404);
            }
        },
    ));

    YandexStub { server, requests }
}

fn base_cfg(stub: &YandexStub) -> YandexcloudSdConfig {
    YandexcloudSdConfig {
        service: "compute".to_string(),
        api_endpoint: Some(stub.base()),
        refresh_interval: Duration::from_millis(50),
        ..YandexcloudSdConfig::default()
    }
}

/// Waits until `d` has discovered instance targets with ids `i1` and `i2`,
/// proving `instances.list` pagination was followed across both pages.
fn discovers_both_instances(d: &mut YandexcloudDiscovery, stub: &YandexStub) {
    let found = wait_until(Duration::from_secs(5), || {
        let ids: std::collections::BTreeSet<String> = d
            .poll()
            .iter()
            .filter_map(|g| g.labels.get("__meta_yandexcloud_instance_id").cloned())
            .collect();
        ids.contains("i1") && ids.contains("i2")
    });
    assert!(
        found,
        "both yandexcloud instances never discovered; requests={:?}",
        stub.requests()
    );
    assert!(
        stub.requests().iter().any(|r| r.contains("pageToken=")),
        "page 2 should have been fetched; requests={:?}",
        stub.requests()
    );
}

/// OAuth token + full org -> cloud -> folder enumeration: the stub must exchange
/// the OAuth token, enumerate folders, and yield both paginated instances with
/// their `__meta_yandexcloud_*` labels and `__address__` (the FQDN), then stop
/// cleanly.
#[test]
fn discovers_instances_with_oauth_token_and_enumeration() {
    let stub = start_yandex_stub();
    let cfg = YandexcloudSdConfig {
        yandex_passport_oauth_token: Some("oauth-tok".to_string()),
        ..base_cfg(&stub)
    };

    let mut d = YandexcloudDiscovery::new(&cfg, "job").expect("new");
    discovers_both_instances(&mut d, &stub);

    let groups = d.poll();
    let inst1 = groups
        .iter()
        .find(|g| {
            g.labels
                .get("__meta_yandexcloud_instance_id")
                .map(String::as_str)
                == Some("i1")
        })
        .expect("instance i1");
    assert_eq!(
        inst1.targets,
        vec!["inst-1.ru-central1.internal".to_string()]
    );
    assert_eq!(inst1.labels["__meta_yandexcloud_instance_name"], "inst-1");
    assert_eq!(
        inst1.labels["__meta_yandexcloud_instance_status"],
        "RUNNING"
    );
    assert_eq!(inst1.labels["__meta_yandexcloud_folder_id"], "folder-1");
    assert_eq!(
        inst1.labels["__meta_yandexcloud_instance_private_ip_0"],
        "10.0.0.1"
    );
    assert_eq!(
        inst1.labels["__meta_yandexcloud_instance_public_ip_0"],
        "1.2.3.4"
    );
    assert_eq!(
        inst1.labels["__meta_yandexcloud_instance_label_env"],
        "prod"
    );
    assert_eq!(inst1.source, "job/yandexcloud");
    assert!(!inst1.labels.contains_key("__address__"));

    // The OAuth exchange endpoint (and enumeration) must have been hit; the
    // metadata token endpoint must NOT.
    let reqs = stub.requests();
    assert!(
        reqs.iter().any(|r| r.contains("/iam/v1/tokens")),
        "reqs={reqs:?}"
    );
    assert!(
        reqs.iter().any(|r| r.contains("/organizations")),
        "reqs={reqs:?}"
    );
    assert!(reqs.iter().any(|r| r.contains("/clouds")), "reqs={reqs:?}");
    assert!(reqs.iter().any(|r| r.contains("/folders")), "reqs={reqs:?}");
    assert!(
        !reqs.iter().any(|r| r.ends_with("/token?")),
        "oauth mode must not call the metadata token endpoint; reqs={reqs:?}"
    );

    drop(d);
    stub.stop();
}

/// Metadata token + configured `folder_ids`: the stub must fetch the metadata
/// IAM token and list instances directly for the configured folder (skipping
/// enumeration).
#[test]
fn discovers_instances_with_metadata_token_and_folder_ids() {
    let stub = start_yandex_stub();
    let cfg = YandexcloudSdConfig {
        metadata_url: Some(stub.base()),
        folder_ids: vec!["folder-1".to_string()],
        ..base_cfg(&stub)
    };

    let mut d = YandexcloudDiscovery::new(&cfg, "job").expect("new");
    discovers_both_instances(&mut d, &stub);

    let reqs = stub.requests();
    assert!(
        reqs.iter()
            .any(|r| r.contains("/service-accounts/default/token")),
        "metadata-token mode must call the metadata token endpoint; reqs={reqs:?}"
    );
    // Configured folder_ids skip the enumeration entirely.
    assert!(
        !reqs.iter().any(|r| r.contains("/organizations")),
        "configured folder_ids must skip organization enumeration; reqs={reqs:?}"
    );

    drop(d);
    stub.stop();
}

/// A set `service_account_key_file` (SA authorized-key JSON) is DEFERRED and
/// must fail `new()` with a clear message.
#[test]
fn service_account_key_file_is_rejected_as_deferred() {
    let cfg = YandexcloudSdConfig {
        service: "compute".to_string(),
        service_account_key_file: Some("/etc/yc/key.json".to_string()),
        ..YandexcloudSdConfig::default()
    };
    let err = match YandexcloudDiscovery::new(&cfg, "job") {
        Ok(_) => panic!("service_account_key_file must be rejected"),
        Err(e) => e,
    };
    assert!(err.msg.contains("service_account_key_file"), "{}", err.msg);
    assert!(err.msg.contains("deferred"), "{}", err.msg);
}

/// A `service` other than `compute` must fail `new()`.
#[test]
fn non_compute_service_is_rejected() {
    let cfg = YandexcloudSdConfig {
        service: "storage".to_string(),
        ..YandexcloudSdConfig::default()
    };
    let err = match YandexcloudDiscovery::new(&cfg, "job") {
        Ok(_) => panic!("non-compute service must be rejected"),
        Err(e) => e,
    };
    assert!(err.msg.contains("compute"), "{}", err.msg);
}
