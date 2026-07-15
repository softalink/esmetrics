//! Stub-server tests for [`super::AzureDiscovery`] — split out per this
//! crate's `#[path]`-sibling convention to keep `mod.rs` under the 800-line
//! cap. One in-process stub serves the token endpoint (both the `OAuth`
//! `.../oauth2/token` POST and the `ManagedIdentity` IMDS GET), a paginated VM
//! list (`Microsoft.Compute/virtualMachines`, page 1 carries a `nextLink`,
//! page 2 does not), an empty scale-set list, and the per-VM NIC GET — so the
//! tests exercise both auth methods, `nextLink` pagination, and the NIC join.

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

/// A running stub Azure endpoint. `requests` records every request's
/// `path?query` in arrival order (so a test can prove page 2 / the token were
/// fetched).
struct AzureStub {
    server: Server,
    requests: Arc<Mutex<Vec<String>>>,
}

impl AzureStub {
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

fn vm_json(name: &str, nic: &str, os_type: &str, next_link: &str) -> String {
    let next = if next_link.is_empty() {
        "\"\"".to_string()
    } else {
        format!("\"{next_link}\"")
    };
    format!(
        r#"{{
  "value": [
    {{
      "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/{name}",
      "name": "{name}",
      "type": "Microsoft.Compute/virtualMachines",
      "location": "eastus",
      "tags": {{"env": "prod"}},
      "properties": {{
        "osProfile": {{"computerName": "comp-{name}"}},
        "storageProfile": {{"osDisk": {{"osType": "{os_type}"}}}},
        "hardwareProfile": {{"vmSize": "Standard_D1"}},
        "networkProfile": {{"networkInterfaces": [
          {{"id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/networkInterfaces/{nic}", "properties": {{"primary": true}}}}
        ]}}
      }}
    }}
  ],
  "nextLink": {next}
}}"#
    )
}

fn nic_json(private_ip: &str) -> String {
    format!(
        r#"{{
  "name": "test-nic",
  "properties": {{
    "primary": true,
    "ipConfigurations": [
      {{"properties": {{
        "privateIPAddress": "{private_ip}",
        "publicIPAddress": {{"properties": {{"ipAddress": "20.30.40.50"}}}},
        "primary": true
      }}}}
    ]
  }}
}}"#
    )
}

/// Starts a stub Azure endpoint dispatching on the request path:
/// - `.../oauth2/token` -> an access token (serves both OAuth POST and IMDS GET),
/// - `.../networkInterfaces/<nic>` -> a NIC with a private/public IP,
/// - `.../virtualMachineScaleSets` -> an empty list,
/// - `.../virtualMachines` -> paginated VMs (page 2 when `marker=2` is set).
fn start_azure_stub() -> AzureStub {
    let server = Server::bind("127.0.0.1:0").expect("bind azure stub");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_for_handler = Arc::clone(&requests);
    let addr = server.local_addr();
    let base = format!("http://{addr}");

    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let path = req.path().to_string();
            let query = req.query().to_string();
            requests_for_handler
                .lock()
                .unwrap()
                .push(format!("{path}?{query}"));

            if path.ends_with("/oauth2/token") {
                w.write_json(
                    200,
                    r#"{"access_token":"azure-tok","expires_in":"3600","token_type":"Bearer"}"#,
                );
            } else if path.contains("/networkInterfaces/") {
                let ip = if path.contains("nic-2") {
                    "10.0.0.2"
                } else {
                    "10.0.0.1"
                };
                w.write_json(200, &nic_json(ip));
            } else if path.contains("/virtualMachineScaleSets") {
                w.write_json(200, r#"{"value":[],"nextLink":""}"#);
            } else if path.contains("/virtualMachines") {
                if query.contains("marker=2") {
                    w.write_json(200, &vm_json("vm-2", "nic-2", "Windows", ""));
                } else {
                    let next = format!(
                        "{base}/subscriptions/sub/providers/Microsoft.Compute/virtualMachines?api-version=2022-03-01&marker=2"
                    );
                    w.write_json(200, &vm_json("vm-1", "nic-1", "Linux", &next));
                }
            } else {
                w.write_status(404);
            }
        },
    ));

    AzureStub { server, requests }
}

fn oauth_cfg(stub: &AzureStub) -> AzureSdConfig {
    AzureSdConfig {
        subscription_id: "sub".into(),
        tenant_id: "tenant".into(),
        client_id: "cid".into(),
        client_secret: Some("secret".into()),
        port: 9100,
        resource_manager_endpoint: Some(stub.base()),
        active_directory_endpoint: Some(stub.base()),
        refresh_interval: Duration::from_millis(50),
        ..AzureSdConfig::default()
    }
}

/// Waits until `d` has discovered both VM targets (`vm-1`, `vm-2`), proving
/// the VM list `nextLink` pagination was followed across both pages.
fn discovers_both_vms(d: &mut AzureDiscovery, stub: &AzureStub) {
    let found = wait_until(Duration::from_secs(5), || {
        let names: std::collections::BTreeSet<String> = d
            .poll()
            .iter()
            .filter_map(|g| g.labels.get("__meta_azure_machine_name").cloned())
            .collect();
        names.contains("vm-1") && names.contains("vm-2")
    });
    assert!(
        found,
        "both azure vms never discovered; requests={:?}",
        stub.requests()
    );
    assert!(
        stub.requests().iter().any(|r| r.contains("marker=2")),
        "page 2 (nextLink) should have been fetched; requests={:?}",
        stub.requests()
    );
}

/// OAuth auth: the stub must yield both paginated VMs with their
/// `__meta_azure_*` labels and `__address__` (private IP from the NIC join +
/// configured port), then stop cleanly.
#[test]
fn discovers_azure_vms_with_oauth() {
    let stub = start_azure_stub();
    let cfg = oauth_cfg(&stub);

    let mut d = AzureDiscovery::new(&cfg, "job").expect("new");
    discovers_both_vms(&mut d, &stub);

    let groups = d.poll();
    let vm1 = groups
        .iter()
        .find(|g| {
            g.labels
                .get("__meta_azure_machine_name")
                .map(String::as_str)
                == Some("vm-1")
        })
        .expect("vm-1");
    // NIC join: private IP came from the networkInterfaces GET.
    assert_eq!(vm1.targets, vec!["10.0.0.1:9100".to_string()]);
    assert_eq!(vm1.labels["__meta_azure_machine_private_ip"], "10.0.0.1");
    assert_eq!(vm1.labels["__meta_azure_machine_public_ip"], "20.30.40.50");
    assert_eq!(vm1.labels["__meta_azure_subscription_id"], "sub");
    assert_eq!(vm1.labels["__meta_azure_tenant_id"], "tenant");
    assert_eq!(vm1.labels["__meta_azure_machine_os_type"], "Linux");
    assert_eq!(vm1.labels["__meta_azure_machine_resource_group"], "rg");
    assert_eq!(
        vm1.labels["__meta_azure_machine_computer_name"],
        "comp-vm-1"
    );
    assert_eq!(vm1.labels["__meta_azure_machine_tag_env"], "prod");
    assert_eq!(vm1.source, "job/azure");
    // __address__ is a target, not a label.
    assert!(!vm1.labels.contains_key("__address__"));

    let vm2 = groups
        .iter()
        .find(|g| {
            g.labels
                .get("__meta_azure_machine_name")
                .map(String::as_str)
                == Some("vm-2")
        })
        .expect("vm-2");
    assert_eq!(vm2.targets, vec!["10.0.0.2:9100".to_string()]);

    // OAuth mode must POST the token to the AD `.../oauth2/token` endpoint.
    assert!(
        stub.requests()
            .iter()
            .any(|r| r.contains("/tenant/oauth2/token")),
        "oauth mode must hit the tenant token endpoint; requests={:?}",
        stub.requests()
    );

    drop(d);
    stub.stop();
}

/// ManagedIdentity auth: the client must fetch the IMDS token (hitting
/// `/metadata/identity/oauth2/token`) and still discover VMs.
#[test]
fn discovers_azure_vms_with_managed_identity() {
    let stub = start_azure_stub();
    let cfg = AzureSdConfig {
        subscription_id: "sub".into(),
        authentication_method: "ManagedIdentity".into(),
        port: 9100,
        resource_manager_endpoint: Some(stub.base()),
        imds_endpoint: Some(stub.base()),
        refresh_interval: Duration::from_millis(50),
        ..AzureSdConfig::default()
    };

    let mut d = AzureDiscovery::new(&cfg, "job").expect("new");
    let found = wait_until(Duration::from_secs(5), || {
        d.poll().iter().any(|g| {
            g.labels
                .get("__meta_azure_machine_name")
                .map(String::as_str)
                == Some("vm-1")
        })
    });
    assert!(
        found,
        "managed-identity discovery never produced targets; requests={:?}",
        stub.requests()
    );
    assert!(
        stub.requests()
            .iter()
            .any(|r| r.starts_with("/metadata/identity/oauth2/token")),
        "managed-identity mode must call the IMDS token endpoint; requests={:?}",
        stub.requests()
    );

    drop(d);
    stub.stop();
}

/// A bad `authentication_method` must fail `new()` (defense-in-depth beyond
/// the parse-time `build_azure_sd_config` check).
#[test]
fn bad_authentication_method_fails_new() {
    let cfg = AzureSdConfig {
        subscription_id: "sub".into(),
        authentication_method: "Kerberos".into(),
        ..AzureSdConfig::default()
    };
    let err = match AzureDiscovery::new(&cfg, "job") {
        Ok(_) => panic!("bad authentication_method must be rejected"),
        Err(e) => e,
    };
    assert!(err.msg.contains("authentication_method"), "{}", err.msg);
}
