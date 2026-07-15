//! Yandex Cloud compute-instance JSON structs (parsed with `serde_json`) and
//! the `__meta_yandexcloud_*` label builder.
//!
//! Port of `lib/promscrape/discovery/yandexcloud/instance.go`
//! (`instance`/`resources`/`networkInterface`/... structs + `addInstanceLabels`)
//! and `yandexcloud.go`'s `instancesPage`, reshaped for this crate's
//! [`TargetGroup`]: one group per instance whose single `__address__` (the
//! instance FQDN) is carried in `targets` and whose `__meta_yandexcloud_*` set
//! is `labels` — mirroring `scrape::gce::labels`.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::sanitize_label_name;

/// `instances.list` response page. Port of `yandexcloud.go`'s `instancesPage`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct InstancesPage {
    pub instances: Vec<Instance>,
    #[serde(rename = "nextPageToken")]
    pub next_page_token: String,
}

/// Port of `yandexcloud.go`'s `instance`, narrowed to the fields
/// `addInstanceLabels` reads.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Instance {
    pub id: String,
    pub name: String,
    pub fqdn: String,
    pub status: String,
    #[serde(rename = "folderId")]
    pub folder_id: String,
    #[serde(rename = "platformId")]
    pub platform_id: String,
    pub resources: Resources,
    #[serde(rename = "networkInterfaces")]
    pub network_interfaces: Vec<NetworkInterface>,
    pub labels: BTreeMap<String, String>,
}

/// Port of `yandexcloud.go`'s `resources`. Every field is a JSON string in the
/// Compute API (e.g. `"cores": "2"`), so they map to `String`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Resources {
    pub cores: String,
    #[serde(rename = "coreFraction")]
    pub core_fraction: String,
    pub memory: String,
}

/// Port of `yandexcloud.go`'s `networkInterface`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct NetworkInterface {
    pub index: String,
    #[serde(rename = "macAddress")]
    pub mac_address: String,
    #[serde(rename = "subnetId")]
    pub subnet_id: String,
    #[serde(rename = "primaryV4Address")]
    pub primary_v4_address: PrimaryV4Address,
}

/// Port of `yandexcloud.go`'s `primaryV4Address`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct PrimaryV4Address {
    pub address: String,
    #[serde(rename = "oneToOneNat")]
    pub one_to_one_nat: OneToOneNat,
    #[serde(rename = "dnsRecords")]
    pub dns_records: Vec<DnsRecord>,
}

/// Port of `yandexcloud.go`'s `oneToOneNat`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct OneToOneNat {
    pub address: String,
    #[serde(rename = "ipVersion")]
    pub ip_version: String,
    #[serde(rename = "dnsRecords")]
    pub dns_records: Vec<DnsRecord>,
}

/// Port of `yandexcloud.go`'s `dnsRecord` (only `fqdn` is read into labels).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct DnsRecord {
    pub fqdn: String,
    #[serde(rename = "dnsZoneId")]
    pub dns_zone_id: String,
    pub ttl: String,
    pub ptr: bool,
}

/// Parses an `instances.list` JSON response page. Port of the `json.Unmarshal`
/// into `instancesPage` in `getInstances`.
pub fn parse_instances_page(data: &[u8]) -> Result<InstancesPage, String> {
    serde_json::from_slice(data).map_err(|e| format!("cannot unmarshal instancesPage: {e}"))
}

/// Builds one [`TargetGroup`] per instance, mirroring `addInstanceLabels`.
/// `__address__` (the instance FQDN) is carried in the group's `targets`; every
/// `__meta_yandexcloud_*` label goes in `labels`. `source` is threaded through
/// unchanged. Unlike GCE/DigitalOcean, no `port` is appended — upstream sets
/// `__address__` to the bare FQDN.
pub fn add_instance_labels(instances: &[Instance], source: &str) -> Vec<TargetGroup> {
    instances
        .iter()
        .map(|server| build_group(server, source))
        .collect()
}

fn build_group(server: &Instance, source: &str) -> TargetGroup {
    let mut m: BTreeMap<String, String> = BTreeMap::new();
    m.insert(
        "__meta_yandexcloud_instance_name".into(),
        server.name.clone(),
    );
    m.insert(
        "__meta_yandexcloud_instance_fqdn".into(),
        server.fqdn.clone(),
    );
    m.insert("__meta_yandexcloud_instance_id".into(), server.id.clone());
    m.insert(
        "__meta_yandexcloud_instance_status".into(),
        server.status.clone(),
    );
    m.insert(
        "__meta_yandexcloud_instance_platform_id".into(),
        server.platform_id.clone(),
    );
    m.insert(
        "__meta_yandexcloud_instance_resources_cores".into(),
        server.resources.cores.clone(),
    );
    m.insert(
        "__meta_yandexcloud_instance_resources_core_fraction".into(),
        server.resources.core_fraction.clone(),
    );
    m.insert(
        "__meta_yandexcloud_instance_resources_memory".into(),
        server.resources.memory.clone(),
    );
    m.insert(
        "__meta_yandexcloud_folder_id".into(),
        server.folder_id.clone(),
    );
    for (k, v) in &server.labels {
        m.insert(
            sanitize_label_name(&format!("__meta_yandexcloud_instance_label_{k}")),
            v.clone(),
        );
    }
    for ni in &server.network_interfaces {
        m.insert(
            format!("__meta_yandexcloud_instance_private_ip_{}", ni.index),
            ni.primary_v4_address.address.clone(),
        );
        if !ni.primary_v4_address.one_to_one_nat.address.is_empty() {
            m.insert(
                format!("__meta_yandexcloud_instance_public_ip_{}", ni.index),
                ni.primary_v4_address.one_to_one_nat.address.clone(),
            );
        }
        for (j, rec) in ni.primary_v4_address.dns_records.iter().enumerate() {
            m.insert(
                format!("__meta_yandexcloud_instance_private_dns_{j}"),
                rec.fqdn.clone(),
            );
        }
        for (j, rec) in ni
            .primary_v4_address
            .one_to_one_nat
            .dns_records
            .iter()
            .enumerate()
        {
            m.insert(
                format!("__meta_yandexcloud_instance_public_dns_{j}"),
                rec.fqdn.clone(),
            );
        }
    }
    TargetGroup {
        targets: vec![server.fqdn.clone()],
        labels: m,
        source: source.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds the one-interface `server-1` fixture from upstream
    /// `instance_test.go`.
    fn base_instance() -> Instance {
        Instance {
            name: "server-1".into(),
            id: "test".into(),
            fqdn: "server-1.ru-central1.internal".into(),
            folder_id: "test".into(),
            status: "RUNNING".into(),
            platform_id: "s2.micro".into(),
            resources: Resources {
                cores: "2".into(),
                core_fraction: "20".into(),
                memory: "4".into(),
            },
            network_interfaces: vec![NetworkInterface {
                index: "0".into(),
                primary_v4_address: PrimaryV4Address {
                    address: "192.168.1.1".into(),
                    ..PrimaryV4Address::default()
                },
                ..NetworkInterface::default()
            }],
            labels: BTreeMap::new(),
        }
    }

    fn only(instances: &[Instance]) -> TargetGroup {
        let mut g = add_instance_labels(instances, "src");
        assert_eq!(g.len(), 1);
        g.remove(0)
    }

    /// `TestAddInstanceLabels` case 1 (one server, private IP only). Validates
    /// the exact upstream label vector + `__address__`.
    #[test]
    fn one_server_private_ip_matches_upstream_vector() {
        let g = only(&[base_instance()]);
        assert_eq!(g.targets, vec!["server-1.ru-central1.internal".to_string()]);
        assert!(!g.labels.contains_key("__address__"));
        let l = &g.labels;
        assert_eq!(l["__meta_yandexcloud_instance_name"], "server-1");
        assert_eq!(
            l["__meta_yandexcloud_instance_fqdn"],
            "server-1.ru-central1.internal"
        );
        assert_eq!(l["__meta_yandexcloud_instance_id"], "test");
        assert_eq!(l["__meta_yandexcloud_instance_status"], "RUNNING");
        assert_eq!(l["__meta_yandexcloud_instance_platform_id"], "s2.micro");
        assert_eq!(l["__meta_yandexcloud_instance_resources_cores"], "2");
        assert_eq!(
            l["__meta_yandexcloud_instance_resources_core_fraction"],
            "20"
        );
        assert_eq!(l["__meta_yandexcloud_instance_resources_memory"], "4");
        assert_eq!(l["__meta_yandexcloud_folder_id"], "test");
        assert_eq!(l["__meta_yandexcloud_instance_private_ip_0"], "192.168.1.1");
        // Exactly the upstream set: 9 fixed + 1 private-ip = 10 labels.
        assert_eq!(l.len(), 10, "labels={l:?}");
    }

    /// `TestAddInstanceLabels` case 2 (public IP via one-to-one NAT).
    #[test]
    fn public_ip_via_one_to_one_nat() {
        let mut inst = base_instance();
        inst.network_interfaces[0]
            .primary_v4_address
            .one_to_one_nat
            .address = "1.1.1.1".into();
        let g = only(&[inst]);
        assert_eq!(
            g.labels["__meta_yandexcloud_instance_private_ip_0"],
            "192.168.1.1"
        );
        assert_eq!(
            g.labels["__meta_yandexcloud_instance_public_ip_0"],
            "1.1.1.1"
        );
    }

    /// `TestAddInstanceLabels` case 3 (private + public DNS records).
    #[test]
    fn private_and_public_dns_records() {
        let mut inst = base_instance();
        let addr = &mut inst.network_interfaces[0].primary_v4_address;
        addr.one_to_one_nat.address = "1.1.1.1".into();
        addr.one_to_one_nat.dns_records = vec![DnsRecord {
            fqdn: "server-1.example.com".into(),
            ..DnsRecord::default()
        }];
        addr.dns_records = vec![DnsRecord {
            fqdn: "server-1.example.local".into(),
            ..DnsRecord::default()
        }];
        let g = only(&[inst]);
        let l = &g.labels;
        assert_eq!(l["__meta_yandexcloud_instance_private_ip_0"], "192.168.1.1");
        assert_eq!(l["__meta_yandexcloud_instance_public_ip_0"], "1.1.1.1");
        assert_eq!(
            l["__meta_yandexcloud_instance_private_dns_0"],
            "server-1.example.local"
        );
        assert_eq!(
            l["__meta_yandexcloud_instance_public_dns_0"],
            "server-1.example.com"
        );
    }

    /// Empty input yields no groups (upstream `f(nil, nil)`).
    #[test]
    fn empty_input_yields_no_groups() {
        assert!(add_instance_labels(&[], "src").is_empty());
    }

    /// Instance labels are sanitized and prefixed.
    #[test]
    fn instance_labels_are_sanitized_and_prefixed() {
        let mut inst = base_instance();
        inst.labels.insert("env-name".into(), "prod".into());
        let g = only(&[inst]);
        assert_eq!(
            g.labels["__meta_yandexcloud_instance_label_env_name"],
            "prod"
        );
    }

    /// Parses an `instancesPage` with a `nextPageToken`.
    #[test]
    fn parses_instances_page_with_pagination_token() {
        let data = r#"{
          "instances": [
            {"id":"i1","name":"n1","fqdn":"n1.internal","status":"RUNNING","folderId":"f1",
             "platformId":"s2.micro",
             "resources":{"cores":"2","coreFraction":"100","memory":"4294967296"},
             "networkInterfaces":[{"index":"0","primaryV4Address":{"address":"10.0.0.1"}}],
             "labels":{"team":"core"}}
          ],
          "nextPageToken": "tok2"
        }"#;
        let page = parse_instances_page(data.as_bytes()).unwrap();
        assert_eq!(page.next_page_token, "tok2");
        assert_eq!(page.instances.len(), 1);
        let i = &page.instances[0];
        assert_eq!(i.id, "i1");
        assert_eq!(i.fqdn, "n1.internal");
        assert_eq!(i.resources.memory, "4294967296");
        assert_eq!(
            i.network_interfaces[0].primary_v4_address.address,
            "10.0.0.1"
        );
        assert_eq!(i.labels["team"], "core");
    }
}
