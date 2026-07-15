//! OVHcloud VPS serde structs and their `__meta_ovhcloud_vps_*` label builder.
//!
//! Port of `lib/promscrape/discovery/ovhcloud/vps.go`'s `vpsModel` /
//! `virtualPrivateServer` structs and `getVPSLabels`, reshaped for this crate's
//! [`TargetGroup`] the same way as [`super::dedicated_server`]: the VPS's
//! default IP is the group's single bare-IP `__address__` target, and the
//! `instance` + `__meta_ovhcloud_vps_*` set becomes the group's `labels`.

use std::collections::BTreeMap;
use std::net::IpAddr;

use serde::Deserialize;

use super::common::{default_ip, split_ipv4_ipv6};
use crate::scrape::discovery::TargetGroup;

/// The `model` block of a [`VirtualPrivateServer`]. Port of `vps.go`'s
/// `vpsModel`. Note the OVH API's `maximumAdditionnalIp` spelling (upstream
/// keeps the API's typo).
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct VpsModel {
    #[serde(rename = "maximumAdditionnalIp")]
    pub maximum_additional_ip: i64,
    pub offer: String,
    pub datacenter: Vec<String>,
    pub vcore: i64,
    pub version: String,
    pub name: String,
    pub disk: i64,
    pub memory: i64,
}

/// One OVHcloud VPS. Port of `vps.go`'s `virtualPrivateServer` (the
/// `/vps/{serviceName}` detail). `ips` is fetched separately from `.../ips`
/// and attached by the client (`#[serde(skip)]`). `#[serde(default)]` tolerates
/// the response fields this port doesn't read.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct VirtualPrivateServer {
    pub zone: String,
    pub model: VpsModel,
    #[serde(rename = "displayName")]
    pub display_name: String,
    pub cluster: String,
    pub state: String,
    pub name: String,
    #[serde(rename = "netbootMode")]
    pub netboot_mode: String,
    #[serde(rename = "memoryLimit")]
    pub memory_limit: i64,
    #[serde(rename = "offerType")]
    pub offer_type: String,
    pub vcore: i64,

    /// IPs fetched separately from the `.../ips` endpoint (not in the detail
    /// JSON).
    #[serde(skip)]
    pub ips: Vec<IpAddr>,
}

/// Renders a `[]string` the way Go's `fmt.Sprintf("%+v", ...)` does: elements
/// space-separated inside brackets (`[]` when empty, `[a b]` for two). Upstream
/// stores `datacenter` this way in `__meta_ovhcloud_vps_datacenter`.
fn format_datacenter(dc: &[String]) -> String {
    format!("[{}]", dc.join(" "))
}

/// Builds a [`TargetGroup`] per VPS. Port of `getVPSLabels`. `__address__` is
/// the bare default IP (in `targets`); `instance` and every
/// `__meta_ovhcloud_vps_*` label go in `labels`.
pub fn append_vps_target_labels(
    servers: &[VirtualPrivateServer],
    source: &str,
) -> Vec<TargetGroup> {
    let mut groups = Vec::with_capacity(servers.len());
    for server in servers {
        let (ipv4, ipv6) = split_ipv4_ipv6(&server.ips);
        let address = default_ip(&ipv4, &ipv6);

        let mut m: BTreeMap<String, String> = BTreeMap::new();
        m.insert("instance".into(), server.name.clone());
        m.insert(
            "__meta_ovhcloud_vps_offer".into(),
            server.model.offer.clone(),
        );
        m.insert(
            "__meta_ovhcloud_vps_datacenter".into(),
            format_datacenter(&server.model.datacenter),
        );
        m.insert(
            "__meta_ovhcloud_vps_model_vcore".into(),
            server.model.vcore.to_string(),
        );
        m.insert(
            "__meta_ovhcloud_vps_maximum_additional_ip".into(),
            server.model.maximum_additional_ip.to_string(),
        );
        m.insert(
            "__meta_ovhcloud_vps_version".into(),
            server.model.version.clone(),
        );
        m.insert(
            "__meta_ovhcloud_vps_model_name".into(),
            server.model.name.clone(),
        );
        m.insert(
            "__meta_ovhcloud_vps_disk".into(),
            server.model.disk.to_string(),
        );
        m.insert(
            "__meta_ovhcloud_vps_memory".into(),
            server.model.memory.to_string(),
        );
        m.insert("__meta_ovhcloud_vps_zone".into(), server.zone.clone());
        m.insert(
            "__meta_ovhcloud_vps_display_name".into(),
            server.display_name.clone(),
        );
        m.insert("__meta_ovhcloud_vps_cluster".into(), server.cluster.clone());
        m.insert("__meta_ovhcloud_vps_state".into(), server.state.clone());
        m.insert("__meta_ovhcloud_vps_name".into(), server.name.clone());
        m.insert(
            "__meta_ovhcloud_vps_netboot_mode".into(),
            server.netboot_mode.clone(),
        );
        m.insert(
            "__meta_ovhcloud_vps_memory_limit".into(),
            server.memory_limit.to_string(),
        );
        m.insert(
            "__meta_ovhcloud_vps_offer_type".into(),
            server.offer_type.clone(),
        );
        m.insert("__meta_ovhcloud_vps_vcore".into(), server.vcore.to_string());
        m.insert("__meta_ovhcloud_vps_ipv4".into(), ipv4);
        m.insert("__meta_ovhcloud_vps_ipv6".into(), ipv6);

        groups.push(TargetGroup {
            targets: vec![address],
            labels: m,
            source: source.to_string(),
        });
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::super::common::parse_ip_list;
    use super::*;

    /// Port of `vps.go`'s detail fixture (`mockVpsDetail`).
    fn mock_detail() -> &'static str {
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
        }"#
    }

    /// Port of upstream `Test_getVpsLabels`: the detail + `/ips` (bare v4 + v6)
    /// must produce exactly the expected `__meta_ovhcloud_vps_*` set,
    /// `instance`, and the v4 `__address__`.
    #[test]
    fn vps_labels_match_upstream() {
        let mut server: VirtualPrivateServer = serde_json::from_str(mock_detail()).unwrap();
        server.ips = parse_ip_list(&[
            "139.99.154.158".to_string(),
            "2402:1f00:8100:401::bb6".to_string(),
        ])
        .unwrap();

        let groups = append_vps_target_labels(&[server], "job/ovhcloud");
        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert_eq!(g.targets, vec!["139.99.154.158".to_string()]);
        assert_eq!(g.source, "job/ovhcloud");
        let l = &g.labels;
        assert_eq!(l["instance"], "vps-000e0e00.vps.ovh.ca");
        assert_eq!(l["__meta_ovhcloud_vps_offer"], "VPS vps2020-starter-1-2-20");
        assert_eq!(l["__meta_ovhcloud_vps_datacenter"], "[]");
        assert_eq!(l["__meta_ovhcloud_vps_model_vcore"], "1");
        assert_eq!(l["__meta_ovhcloud_vps_maximum_additional_ip"], "16");
        assert_eq!(l["__meta_ovhcloud_vps_version"], "2019v1");
        assert_eq!(l["__meta_ovhcloud_vps_model_name"], "vps-starter-1-2-20");
        assert_eq!(l["__meta_ovhcloud_vps_disk"], "20");
        assert_eq!(l["__meta_ovhcloud_vps_memory"], "2048");
        assert_eq!(l["__meta_ovhcloud_vps_zone"], "Region OpenStack: os-syd2");
        assert_eq!(
            l["__meta_ovhcloud_vps_display_name"],
            "vps-000e0e00.vps.ovh.ca"
        );
        assert_eq!(l["__meta_ovhcloud_vps_cluster"], "");
        assert_eq!(l["__meta_ovhcloud_vps_state"], "running");
        assert_eq!(l["__meta_ovhcloud_vps_name"], "vps-000e0e00.vps.ovh.ca");
        assert_eq!(l["__meta_ovhcloud_vps_netboot_mode"], "local");
        assert_eq!(l["__meta_ovhcloud_vps_memory_limit"], "2048");
        assert_eq!(l["__meta_ovhcloud_vps_offer_type"], "ssd");
        assert_eq!(l["__meta_ovhcloud_vps_vcore"], "1");
        assert_eq!(l["__meta_ovhcloud_vps_ipv4"], "139.99.154.158");
        assert_eq!(l["__meta_ovhcloud_vps_ipv6"], "2402:1f00:8100:401::bb6");
        assert!(!l.contains_key("__address__"));
    }

    /// A populated `datacenter` renders Go-style `[a b]` (space-separated, no
    /// quotes).
    #[test]
    fn datacenter_formats_like_go_percent_plus_v() {
        assert_eq!(format_datacenter(&[]), "[]");
        assert_eq!(
            format_datacenter(&["gra".to_string(), "rbx".to_string()]),
            "[gra rbx]"
        );
    }
}
