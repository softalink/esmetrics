//! GCE Compute API JSON response structs (parsed with `serde_json`) and the
//! `__meta_gce_*` label builder.
//!
//! Port of `lib/promscrape/discovery/gce/instance.go` (`Instance`/
//! `NetworkInterface`/... structs + `appendTargetLabels`) and `zone.go`
//! (`ZoneList`), reshaped for this crate's [`TargetGroup`]: one group per
//! instance, whose single `__address__` (the instance's first-interface
//! private IP + configured port) is carried in `targets` and whose
//! `__meta_gce_*` set is `labels` — mirroring `scrape::ec2::labels`.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::sanitize_label_name;

/// `instances.list` response. Port of `instance.go`'s `InstanceList`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct InstanceList {
    pub items: Vec<Instance>,
    #[serde(rename = "nextPageToken")]
    pub next_page_token: String,
}

/// Port of `instance.go`'s `Instance`, narrowed to the fields
/// `appendTargetLabels` reads. `id` is a JSON string in the Compute API
/// (e.g. `"7897352091592122"`), so it maps to `String`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Instance {
    pub id: String,
    pub name: String,
    pub status: String,
    #[serde(rename = "machineType")]
    pub machine_type: String,
    pub zone: String,
    #[serde(rename = "networkInterfaces")]
    pub network_interfaces: Vec<NetworkInterface>,
    pub tags: TagList,
    pub metadata: MetadataList,
    /// `labels` is a plain JSON object (`{"env":"play"}`), unlike EC2's
    /// key/value list — so it deserializes straight into a map.
    pub labels: BTreeMap<String, String>,
}

/// Port of `instance.go`'s `NetworkInterface`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct NetworkInterface {
    pub name: String,
    pub network: String,
    pub subnetwork: String,
    #[serde(rename = "networkIP")]
    pub network_ip: String,
    #[serde(rename = "ipv6Address")]
    pub ipv6_address: String,
    #[serde(rename = "accessConfigs")]
    pub access_configs: Vec<AccessConfig>,
    #[serde(rename = "ipv6AccessConfigs")]
    pub ipv6_access_configs: Vec<Ipv6AccessConfig>,
}

/// Port of `instance.go`'s `AccessConfig`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct AccessConfig {
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(rename = "natIP")]
    pub nat_ip: String,
}

/// Port of `instance.go`'s `Ipv6AccessConfig`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Ipv6AccessConfig {
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(rename = "externalIpv6")]
    pub external_ipv6: String,
}

/// Port of `instance.go`'s `TagList`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct TagList {
    pub items: Vec<String>,
}

/// Port of `instance.go`'s `MetadataList`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct MetadataList {
    pub items: Vec<MetadataEntry>,
}

/// Port of `instance.go`'s `MetadataEntry`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct MetadataEntry {
    pub key: String,
    pub value: String,
}

/// `zones.list` response. Port of `zone.go`'s `ZoneList`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ZoneList {
    pub items: Vec<ZoneItem>,
    #[serde(rename = "nextPageToken")]
    pub next_page_token: String,
}

/// Port of `zone.go`'s `Zone`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ZoneItem {
    pub name: String,
}

/// Parses an `instances.list` JSON response. Port of `parseInstanceList`.
pub fn parse_instance_list(data: &[u8]) -> Result<InstanceList, String> {
    serde_json::from_slice(data).map_err(|e| format!("cannot unmarshal InstanceList: {e}"))
}

/// Parses a `zones.list` JSON response. Port of `parseZoneList`.
pub fn parse_zone_list(data: &[u8]) -> Result<ZoneList, String> {
    serde_json::from_slice(data).map_err(|e| format!("cannot unmarshal ZoneList: {e}"))
}

/// Builds the [`TargetGroup`] for one GCE instance, mirroring
/// `appendTargetLabels`. Returns `None` for an instance with no network
/// interfaces (upstream skips it — there is no address to scrape).
/// `__address__` (first interface's `networkIP` + `port`) is carried in the
/// group's `targets`; every `__meta_gce_*` label goes in `labels`. `source`
/// is threaded through unchanged.
pub fn append_target_labels(
    inst: &Instance,
    project: &str,
    tag_separator: &str,
    port: u16,
    source: String,
) -> Option<TargetGroup> {
    let iface = inst.network_interfaces.first()?;
    let address = join_host_port(&iface.network_ip, port);

    let mut m: BTreeMap<String, String> = BTreeMap::new();
    m.insert("__meta_gce_instance_id".into(), inst.id.clone());
    m.insert("__meta_gce_instance_status".into(), inst.status.clone());
    m.insert("__meta_gce_instance_name".into(), inst.name.clone());
    m.insert("__meta_gce_machine_type".into(), inst.machine_type.clone());
    m.insert("__meta_gce_network".into(), iface.network.clone());
    m.insert("__meta_gce_private_ip".into(), iface.network_ip.clone());
    m.insert("__meta_gce_project".into(), project.to_string());
    m.insert("__meta_gce_subnetwork".into(), iface.subnetwork.clone());
    m.insert("__meta_gce_zone".into(), inst.zone.clone());

    for ni in &inst.network_interfaces {
        m.insert(
            sanitize_label_name(&format!("__meta_gce_interface_ipv4_{}", ni.name)),
            ni.network_ip.clone(),
        );
    }

    if !inst.tags.items.is_empty() {
        // Surround the list with the separator so relabel regexes don't have
        // to consider element position.
        m.insert(
            "__meta_gce_tags".into(),
            format!(
                "{sep}{}{sep}",
                inst.tags.items.join(tag_separator),
                sep = tag_separator
            ),
        );
    }

    for item in &inst.metadata.items {
        m.insert(
            sanitize_label_name(&format!("__meta_gce_metadata_{}", item.key)),
            item.value.clone(),
        );
    }

    for (name, value) in &inst.labels {
        m.insert(
            sanitize_label_name(&format!("__meta_gce_label_{name}")),
            value.clone(),
        );
    }

    if let Some(ac) = iface.access_configs.first() {
        if ac.type_ == "ONE_TO_ONE_NAT" {
            m.insert("__meta_gce_public_ip".into(), ac.nat_ip.clone());
        }
    }
    // GCE supports ULA as well as native IPv6.
    if let Some(ac) = iface.ipv6_access_configs.first() {
        if ac.type_ == "DIRECT_IPV6" {
            m.insert("__meta_gce_public_ipv6".into(), ac.external_ipv6.clone());
        }
    }
    if !iface.ipv6_address.is_empty() {
        m.insert(
            "__meta_gce_internal_ipv6".into(),
            iface.ipv6_address.clone(),
        );
    }

    Some(TargetGroup {
        targets: vec![address],
        labels: m,
        source,
    })
}

/// `host:port`, bracketing `host` when it is an IPv6 address (contains a
/// `:`). Port of `discoveryutil.JoinHostPort`.
fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `instances.list` fixture from upstream `instance_test.go`
    /// (`TestParseInstanceListSuccess`), kept in the exact shape upstream
    /// asserts its `appendTargetLabels` output against.
    const INSTANCES_JSON: &str = r#"
{
  "id": "projects/victoriametrics-test/zones/us-east1-b/instances",
  "items": [
    {
      "id": "7897352091592122",
      "creationTimestamp": "2020-02-16T07:10:14.357-08:00",
      "name": "play-1m-1-vmagent",
      "tags": {
        "items": ["play", "play-1m-1", "vmagent"],
        "fingerprint": "O44NvJ36CCo="
      },
      "machineType": "https://www.googleapis.com/compute/v1/projects/victoriametrics-test/zones/us-east1-b/machineTypes/f1-micro",
      "status": "RUNNING",
      "zone": "https://www.googleapis.com/compute/v1/projects/victoriametrics-test/zones/us-east1-b",
      "networkInterfaces": [
        {
          "network": "https://www.googleapis.com/compute/v1/projects/victoriametrics-test/global/networks/default",
          "subnetwork": "https://www.googleapis.com/compute/v1/projects/victoriametrics-test/regions/us-east1/subnetworks/play-1m-1-snw",
          "networkIP": "10.11.2.7",
          "name": "nic0",
          "fingerprint": "O4eNOfaplJ4=",
          "kind": "compute#networkInterface"
        }
      ],
      "metadata": {
        "fingerprint": "BAFZwTyaAxQ=",
        "items": [
          { "key": "gce-container-declaration", "value": "foobar" }
        ],
        "kind": "compute#metadata"
      },
      "labels": {
        "goog-dm": "play-deployment",
        "cluster_num": "1",
        "cluster_retention": "1m",
        "env": "play",
        "type": "vmagent"
      },
      "labelFingerprint": "-CXeRXMQiVc=",
      "kind": "compute#instance"
    }
  ]
}
"#;

    #[test]
    fn parses_instance_list_fields() {
        let il = parse_instance_list(INSTANCES_JSON.as_bytes()).unwrap();
        assert_eq!(il.next_page_token, "");
        assert_eq!(il.items.len(), 1);
        let inst = &il.items[0];
        assert_eq!(inst.id, "7897352091592122");
        assert_eq!(inst.name, "play-1m-1-vmagent");
        assert_eq!(inst.status, "RUNNING");
        assert_eq!(inst.network_interfaces[0].network_ip, "10.11.2.7");
        assert_eq!(inst.network_interfaces[0].name, "nic0");
        assert_eq!(inst.tags.items, vec!["play", "play-1m-1", "vmagent"]);
        assert_eq!(inst.metadata.items[0].key, "gce-container-declaration");
        assert_eq!(inst.metadata.items[0].value, "foobar");
        assert_eq!(inst.labels["env"], "play");
    }

    /// The label set + `__address__` must match the expected output from
    /// upstream `instance_test.go` (`appendTargetLabels(nil, "proj-1", ",",
    /// 80)`).
    #[test]
    fn builds_meta_gce_labels_matching_upstream_vector() {
        let il = parse_instance_list(INSTANCES_JSON.as_bytes()).unwrap();
        let inst = &il.items[0];

        let g = append_target_labels(inst, "proj-1", ",", 80, "src".into())
            .expect("instance has a network interface");

        // __address__ is the target, first interface's private IP + port.
        assert_eq!(g.targets, vec!["10.11.2.7:80".to_string()]);
        assert!(!g.labels.contains_key("__address__"));

        let l = &g.labels;
        assert_eq!(l["__meta_gce_instance_id"], "7897352091592122");
        assert_eq!(l["__meta_gce_instance_name"], "play-1m-1-vmagent");
        assert_eq!(l["__meta_gce_instance_status"], "RUNNING");
        assert_eq!(l["__meta_gce_interface_ipv4_nic0"], "10.11.2.7");
        assert_eq!(l["__meta_gce_label_cluster_num"], "1");
        assert_eq!(l["__meta_gce_label_cluster_retention"], "1m");
        assert_eq!(l["__meta_gce_label_env"], "play");
        assert_eq!(l["__meta_gce_label_goog_dm"], "play-deployment");
        assert_eq!(l["__meta_gce_label_type"], "vmagent");
        assert_eq!(
            l["__meta_gce_machine_type"],
            "https://www.googleapis.com/compute/v1/projects/victoriametrics-test/zones/us-east1-b/machineTypes/f1-micro"
        );
        assert_eq!(l["__meta_gce_metadata_gce_container_declaration"], "foobar");
        assert_eq!(
            l["__meta_gce_network"],
            "https://www.googleapis.com/compute/v1/projects/victoriametrics-test/global/networks/default"
        );
        assert_eq!(l["__meta_gce_private_ip"], "10.11.2.7");
        assert_eq!(l["__meta_gce_project"], "proj-1");
        assert_eq!(
            l["__meta_gce_subnetwork"],
            "https://www.googleapis.com/compute/v1/projects/victoriametrics-test/regions/us-east1/subnetworks/play-1m-1-snw"
        );
        assert_eq!(l["__meta_gce_tags"], ",play,play-1m-1,vmagent,");
        assert_eq!(
            l["__meta_gce_zone"],
            "https://www.googleapis.com/compute/v1/projects/victoriametrics-test/zones/us-east1-b"
        );
        // No accessConfigs / ipv6 in the fixture -> those conditionals unset.
        assert!(!l.contains_key("__meta_gce_public_ip"));
        assert!(!l.contains_key("__meta_gce_public_ipv6"));
        assert!(!l.contains_key("__meta_gce_internal_ipv6"));
        // Exactly the upstream set (9 fixed + 1 interface + 1 metadata + 5
        // labels + tags = 17 labels).
        assert_eq!(l.len(), 17, "labels={l:?}");
    }

    #[test]
    fn public_and_internal_ipv6_conditionals() {
        let inst = Instance {
            id: "1".into(),
            network_interfaces: vec![NetworkInterface {
                name: "nic0".into(),
                network_ip: "10.0.0.5".into(),
                ipv6_address: "fd20::5".into(),
                access_configs: vec![AccessConfig {
                    type_: "ONE_TO_ONE_NAT".into(),
                    nat_ip: "34.1.2.3".into(),
                }],
                ipv6_access_configs: vec![Ipv6AccessConfig {
                    type_: "DIRECT_IPV6".into(),
                    external_ipv6: "2600::1".into(),
                }],
                ..NetworkInterface::default()
            }],
            ..Instance::default()
        };
        let g = append_target_labels(&inst, "p", ",", 9100, "s".into()).unwrap();
        assert_eq!(g.targets, vec!["10.0.0.5:9100".to_string()]);
        assert_eq!(g.labels["__meta_gce_public_ip"], "34.1.2.3");
        assert_eq!(g.labels["__meta_gce_public_ipv6"], "2600::1");
        assert_eq!(g.labels["__meta_gce_internal_ipv6"], "fd20::5");
    }

    #[test]
    fn skips_instance_without_network_interface() {
        let inst = Instance::default();
        assert!(append_target_labels(&inst, "p", ",", 80, "s".into()).is_none());
    }

    #[test]
    fn custom_tag_separator_wraps_tags() {
        let inst = Instance {
            id: "1".into(),
            network_interfaces: vec![NetworkInterface {
                name: "nic0".into(),
                network_ip: "10.0.0.5".into(),
                ..NetworkInterface::default()
            }],
            tags: TagList {
                items: vec!["a".into(), "b".into()],
            },
            ..Instance::default()
        };
        let g = append_target_labels(&inst, "p", "|", 80, "s".into()).unwrap();
        assert_eq!(g.labels["__meta_gce_tags"], "|a|b|");
    }

    #[test]
    fn parses_zone_list_with_pagination_token() {
        let data =
            r#"{"items":[{"name":"us-east1-b"},{"name":"us-east1-c"}],"nextPageToken":"tok2"}"#;
        let zl = parse_zone_list(data.as_bytes()).unwrap();
        assert_eq!(zl.next_page_token, "tok2");
        let names: Vec<&str> = zl.items.iter().map(|z| z.name.as_str()).collect();
        assert_eq!(names, vec!["us-east1-b", "us-east1-c"]);
    }

    #[test]
    fn ipv4_host_not_bracketed_ipv6_is() {
        assert_eq!(join_host_port("10.0.0.1", 80), "10.0.0.1:80");
        assert_eq!(join_host_port("fd20::5", 80), "[fd20::5]:80");
    }
}
