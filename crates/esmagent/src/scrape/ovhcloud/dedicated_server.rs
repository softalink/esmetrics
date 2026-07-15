//! OVHcloud dedicated-server serde struct and its `__meta_ovhcloud_dedicated_server_*`
//! label builder.
//!
//! Port of `lib/promscrape/discovery/ovhcloud/dedicated_server.go`'s
//! `dedicatedServer` struct and `getDedicatedServerLabels`, reshaped for this
//! crate's [`TargetGroup`] (one group per server: the server's default IP is
//! the group's single `__address__` target, and the `instance` +
//! `__meta_ovhcloud_dedicated_server_*` set becomes the group's `labels`).
//!
//! Upstream puts `__address__` into the returned label set because a Prometheus
//! label set *is* the target; this crate carries the address separately in
//! `targets`, so [`append_dedicated_server_target_labels`] puts it there and
//! leaves it out of `labels` — mirroring `scrape::digitalocean::labels`.
//! Unlike most SD providers, OVHcloud sets no port: `__address__` is the bare
//! default IP (upstream `m.Add("__address__", defaultIP)`).

use std::collections::BTreeMap;
use std::net::IpAddr;

use serde::Deserialize;

use super::common::{default_ip, split_ipv4_ipv6};
use crate::scrape::discovery::TargetGroup;

/// One OVHcloud dedicated server. Port of `dedicated_server.go`'s
/// `dedicatedServer` (the `/dedicated/server/{serviceName}` detail). `ips` is
/// not a JSON field of the detail — it is fetched separately from the
/// `.../ips` endpoint and attached by the client, so it is `#[serde(skip)]`.
/// `#[serde(default)]` tolerates the many detail fields this port doesn't read.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct DedicatedServer {
    pub state: String,
    #[serde(rename = "commercialRange")]
    pub commercial_range: String,
    #[serde(rename = "linkSpeed")]
    pub link_speed: i64,
    pub rack: String,
    #[serde(rename = "noIntervention")]
    pub no_intervention: bool,
    pub os: String,
    #[serde(rename = "supportLevel")]
    pub support_level: String,
    #[serde(rename = "serverId")]
    pub server_id: i64,
    pub reverse: String,
    pub datacenter: String,
    pub name: String,

    /// IPs fetched separately from the `.../ips` endpoint (not in the detail
    /// JSON).
    #[serde(skip)]
    pub ips: Vec<IpAddr>,
}

/// Builds a [`TargetGroup`] per dedicated server. Port of
/// `getDedicatedServerLabels`. `__address__` is the bare default IP (carried in
/// `targets`); `instance` and every `__meta_ovhcloud_dedicated_server_*` label
/// go in `labels`. `source` is threaded through unchanged so the reconcile diff
/// stays stable across refreshes.
pub fn append_dedicated_server_target_labels(
    servers: &[DedicatedServer],
    source: &str,
) -> Vec<TargetGroup> {
    let mut groups = Vec::with_capacity(servers.len());
    for server in servers {
        let (ipv4, ipv6) = split_ipv4_ipv6(&server.ips);
        let address = default_ip(&ipv4, &ipv6);

        let mut m: BTreeMap<String, String> = BTreeMap::new();
        m.insert("instance".into(), server.name.clone());
        m.insert(
            "__meta_ovhcloud_dedicated_server_state".into(),
            server.state.clone(),
        );
        m.insert(
            "__meta_ovhcloud_dedicated_server_commercial_range".into(),
            server.commercial_range.clone(),
        );
        m.insert(
            "__meta_ovhcloud_dedicated_server_link_speed".into(),
            server.link_speed.to_string(),
        );
        m.insert(
            "__meta_ovhcloud_dedicated_server_rack".into(),
            server.rack.clone(),
        );
        m.insert(
            "__meta_ovhcloud_dedicated_server_no_intervention".into(),
            server.no_intervention.to_string(),
        );
        m.insert(
            "__meta_ovhcloud_dedicated_server_os".into(),
            server.os.clone(),
        );
        m.insert(
            "__meta_ovhcloud_dedicated_server_support_level".into(),
            server.support_level.clone(),
        );
        m.insert(
            "__meta_ovhcloud_dedicated_server_server_id".into(),
            server.server_id.to_string(),
        );
        m.insert(
            "__meta_ovhcloud_dedicated_server_reverse".into(),
            server.reverse.clone(),
        );
        m.insert(
            "__meta_ovhcloud_dedicated_server_datacenter".into(),
            server.datacenter.clone(),
        );
        m.insert(
            "__meta_ovhcloud_dedicated_server_name".into(),
            server.name.clone(),
        );
        m.insert("__meta_ovhcloud_dedicated_server_ipv4".into(), ipv4);
        m.insert("__meta_ovhcloud_dedicated_server_ipv6".into(), ipv6);

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

    /// Port of `dedicated_server.go`'s detail fixture (`mockDedicatedServerDetail`).
    fn mock_detail() -> &'static str {
        r#"{
            "name": "ns0000000.ip-00-00-000.eu",
            "datacenter": "gra2",
            "linkSpeed": 1000,
            "reverse": "ns0000000.ip-00-00-000.eu",
            "serverId": 1000000,
            "rack": "G000A00",
            "supportLevel": "pro",
            "commercialRange": "RISE-3",
            "state": "ok",
            "os": "centos7_64",
            "noIntervention": false
        }"#
    }

    /// Port of upstream `Test_getDedicatedServerLabels`: the detail + `/ips`
    /// (`.../64` v6 prefix dropped, `.../32` v4 kept) must produce exactly the
    /// expected `__meta_ovhcloud_dedicated_server_*` set, `instance`, and the
    /// bare-IP `__address__`.
    #[test]
    fn dedicated_server_labels_match_upstream() {
        let mut server: DedicatedServer = serde_json::from_str(mock_detail()).unwrap();
        server.ips = parse_ip_list(&[
            "2001:40d0:302:8874::/64".to_string(),
            "50.75.126.113/32".to_string(),
        ])
        .unwrap();

        let groups = append_dedicated_server_target_labels(&[server], "job/ovhcloud");
        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert_eq!(g.targets, vec!["50.75.126.113".to_string()]);
        assert_eq!(g.source, "job/ovhcloud");
        let l = &g.labels;
        assert_eq!(l["instance"], "ns0000000.ip-00-00-000.eu");
        assert_eq!(l["__meta_ovhcloud_dedicated_server_state"], "ok");
        assert_eq!(
            l["__meta_ovhcloud_dedicated_server_commercial_range"],
            "RISE-3"
        );
        assert_eq!(l["__meta_ovhcloud_dedicated_server_link_speed"], "1000");
        assert_eq!(l["__meta_ovhcloud_dedicated_server_rack"], "G000A00");
        assert_eq!(
            l["__meta_ovhcloud_dedicated_server_no_intervention"],
            "false"
        );
        assert_eq!(l["__meta_ovhcloud_dedicated_server_os"], "centos7_64");
        assert_eq!(l["__meta_ovhcloud_dedicated_server_support_level"], "pro");
        assert_eq!(l["__meta_ovhcloud_dedicated_server_server_id"], "1000000");
        assert_eq!(
            l["__meta_ovhcloud_dedicated_server_reverse"],
            "ns0000000.ip-00-00-000.eu"
        );
        assert_eq!(l["__meta_ovhcloud_dedicated_server_datacenter"], "gra2");
        assert_eq!(
            l["__meta_ovhcloud_dedicated_server_name"],
            "ns0000000.ip-00-00-000.eu"
        );
        assert_eq!(l["__meta_ovhcloud_dedicated_server_ipv4"], "50.75.126.113");
        assert_eq!(l["__meta_ovhcloud_dedicated_server_ipv6"], "");
        // __address__ is the target, not a label.
        assert!(!l.contains_key("__address__"));
    }
}
