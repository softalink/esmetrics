//! Hetzner Cloud (`role: hcloud`) server/network serde structs, the paginated
//! list-response parsers, and the `__meta_hetzner_*` / `__meta_hetzner_hcloud_*`
//! label builder ([`append_hcloud_target_labels`]).
//!
//! Port of `lib/promscrape/discovery/hetzner/hcloud.go` (v1.146.0):
//! `HCloudServer`/`HCloudNetwork` and their nested structs,
//! `parseHCloudServerList`/`parseHCloudNetworksList` (both drive
//! `meta.pagination.next_page` cursoring in [`super::client`]), and
//! `appendHCloudTargetLabels`, reshaped for this crate's [`TargetGroup`] (one
//! group per server: the server's `__address__` is the group's single target,
//! the `__meta_hetzner_*` set becomes the group's `labels`).
//!
//! Upstream includes `__address__` in the returned label set because a
//! Prometheus label set *is* the target; this crate's [`TargetGroup`] carries
//! the address separately in `targets`, so [`append_hcloud_target_labels`] puts
//! it there and leaves it out of `labels` — mirroring `scrape::digitalocean`.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::labels::{join_host_port, parse_cidr_network};
use crate::scrape::config::ScrapeError;
use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::sanitize_label_name;

/// `/v1/servers` list response. Port of `HCloudServerList`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct HCloudServerList {
    pub meta: HCloudMeta,
    pub servers: Vec<HCloudServer>,
}

/// `/v1/networks` list response. Port of `HCloudNetworksList`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct HCloudNetworksList {
    pub meta: HCloudMeta,
    pub networks: Vec<HCloudNetwork>,
}

/// Hetzner Cloud pagination envelope. Port of `HCloudMeta`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct HCloudMeta {
    pub pagination: HCloudPagination,
}

/// The `meta.pagination` block. Port of `HCloudPagination`. `next_page` is 0
/// (JSON `null`/absent → serde default) on the last page.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct HCloudPagination {
    #[serde(default, deserialize_with = "super::labels::null_default")]
    pub next_page: i64,
}

/// One Hetzner Cloud network. Port of `HCloudNetwork` (`id`+`name` only).
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct HCloudNetwork {
    pub id: i64,
    pub name: String,
}

/// One Hetzner Cloud server. Port of `HCloudServer` (`/v1/servers` array
/// element). `#[serde(default)]` tolerates the many response fields this port
/// doesn't read.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct HCloudServer {
    pub id: i64,
    pub name: String,
    pub status: String,
    pub public_net: HCloudPublicNet,
    pub private_net: Vec<HCloudPrivateNet>,
    pub server_type: HCloudServerType,
    pub datacenter: HCloudDatacenter,
    pub location: HCloudLocation,
    pub image: Option<HCloudImage>,
    pub labels: BTreeMap<String, String>,
}

/// The `server_type` block of a [`HCloudServer`]. Port of `HCloudServerType`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct HCloudServerType {
    pub name: String,
    pub cores: i64,
    pub cpu_type: String,
    pub memory: f32,
    pub disk: i64,
}

/// The `datacenter` block of a [`HCloudServer`], narrowed to `name`. Port of
/// `HCloudDatacenter`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct HCloudDatacenter {
    pub name: String,
}

/// The top-level `location` block of a [`HCloudServer`]. Port of
/// `HCloudLocation`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct HCloudLocation {
    pub name: String,
    pub network_zone: String,
}

/// The `public_net` block of a [`HCloudServer`]. Port of `HCloudPublicNet`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct HCloudPublicNet {
    pub ipv4: HCloudIPv4,
    pub ipv6: HCloudIPv6,
}

/// The `public_net.ipv4` block. Port of `HCloudIPv4`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct HCloudIPv4 {
    pub ip: String,
}

/// The `public_net.ipv6` block. Port of `HCloudIPv6`. `ip` is a CIDR
/// (e.g. `2001:db8::/64`).
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct HCloudIPv6 {
    pub ip: String,
}

/// One `private_net` entry of a [`HCloudServer`]. Port of `HCloudPrivateNet`
/// (`network` is the network id; renamed since `network`/`id` differ).
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct HCloudPrivateNet {
    #[serde(rename = "network")]
    pub id: i64,
    pub ip: String,
}

/// The `image` block of a [`HCloudServer`]. Port of `HCloudImage`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct HCloudImage {
    pub name: String,
    pub description: String,
    pub os_flavor: String,
    pub os_version: String,
}

/// Parses a `/v1/servers` response body into its servers and the
/// `meta.pagination.next_page` cursor. Port of `parseHCloudServerList`.
pub fn parse_hcloud_server_list(data: &[u8]) -> Result<(Vec<HCloudServer>, i64), ScrapeError> {
    let resp: HCloudServerList = serde_json::from_slice(data).map_err(|e| ScrapeError {
        msg: format!("cannot unmarshal HCloudServerList: {e}"),
    })?;
    Ok((resp.servers, resp.meta.pagination.next_page))
}

/// Parses a `/v1/networks` response body into its networks and the
/// `meta.pagination.next_page` cursor. Port of `parseHCloudNetworksList`.
pub fn parse_hcloud_networks_list(data: &[u8]) -> Result<(Vec<HCloudNetwork>, i64), ScrapeError> {
    let resp: HCloudNetworksList = serde_json::from_slice(data).map_err(|e| ScrapeError {
        msg: format!("cannot unmarshal HCloudNetworksList: {e}"),
    })?;
    Ok((resp.networks, resp.meta.pagination.next_page))
}

/// Builds one [`TargetGroup`] per hcloud server, mirroring
/// `getHCloudServerLabels` + `appendHCloudTargetLabels`. `__address__` is
/// `publicIPv4:default_port` (carried in the group's `targets`); every
/// `__meta_hetzner_*`/`__meta_hetzner_hcloud_*` label goes in `labels`.
/// `source` is threaded through unchanged so the reconcile diff stays stable
/// across refreshes.
pub fn append_hcloud_target_labels(
    servers: &[HCloudServer],
    networks: &[HCloudNetwork],
    default_port: u16,
    source: &str,
) -> Vec<TargetGroup> {
    servers
        .iter()
        .map(|server| hcloud_target_group(server, networks, default_port, source))
        .collect()
}

fn hcloud_target_group(
    server: &HCloudServer,
    networks: &[HCloudNetwork],
    default_port: u16,
    source: &str,
) -> TargetGroup {
    let address = join_host_port(&server.public_net.ipv4.ip, default_port);

    let mut m: BTreeMap<String, String> = BTreeMap::new();
    m.insert("__meta_hetzner_role".into(), "hcloud".into());
    m.insert("__meta_hetzner_server_id".into(), server.id.to_string());
    m.insert("__meta_hetzner_server_name".into(), server.name.clone());
    // Note: Hetzner is removing the datacenter field from the Hetzner Cloud API
    // after 2026-07-01; this label returns an empty value after that date.
    m.insert(
        "__meta_hetzner_datacenter".into(),
        server.datacenter.name.clone(),
    );
    m.insert(
        "__meta_hetzner_public_ipv4".into(),
        server.public_net.ipv4.ip.clone(),
    );
    if let Some(network) = parse_cidr_network(&server.public_net.ipv6.ip) {
        m.insert("__meta_hetzner_public_ipv6_network".into(), network);
    }
    m.insert("__meta_hetzner_server_status".into(), server.status.clone());

    m.insert(
        "__meta_hetzner_hcloud_location".into(),
        server.location.name.clone(),
    );
    m.insert(
        "__meta_hetzner_hcloud_location_network_zone".into(),
        server.location.network_zone.clone(),
    );
    // Deprecated aliases of the two labels above.
    m.insert(
        "__meta_hetzner_hcloud_datacenter_location".into(),
        server.location.name.clone(),
    );
    m.insert(
        "__meta_hetzner_hcloud_datacenter_location_network_zone".into(),
        server.location.network_zone.clone(),
    );
    m.insert(
        "__meta_hetzner_hcloud_server_type".into(),
        server.server_type.name.clone(),
    );
    m.insert(
        "__meta_hetzner_hcloud_cpu_cores".into(),
        server.server_type.cores.to_string(),
    );
    m.insert(
        "__meta_hetzner_hcloud_cpu_type".into(),
        server.server_type.cpu_type.clone(),
    );
    m.insert(
        "__meta_hetzner_hcloud_memory_size_gb".into(),
        (server.server_type.memory as i64).to_string(),
    );
    m.insert(
        "__meta_hetzner_hcloud_disk_size_gb".into(),
        server.server_type.disk.to_string(),
    );

    if let Some(image) = &server.image {
        m.insert(
            "__meta_hetzner_hcloud_image_name".into(),
            image.name.clone(),
        );
        m.insert(
            "__meta_hetzner_hcloud_image_description".into(),
            image.description.clone(),
        );
        m.insert(
            "__meta_hetzner_hcloud_image_os_version".into(),
            image.os_version.clone(),
        );
        m.insert(
            "__meta_hetzner_hcloud_image_os_flavor".into(),
            image.os_flavor.clone(),
        );
    }

    for private_net in &server.private_net {
        for network in networks {
            if private_net.id == network.id {
                let key = sanitize_label_name(&format!(
                    "__meta_hetzner_hcloud_private_ipv4_{}",
                    network.name
                ));
                m.insert(key, private_net.ip.clone());
            }
        }
    }

    for (key, value) in &server.labels {
        let present = sanitize_label_name(&format!("__meta_hetzner_hcloud_labelpresent_{key}"));
        m.insert(present, "true".into());
        let label = sanitize_label_name(&format!("__meta_hetzner_hcloud_label_{key}"));
        m.insert(label, value.clone());
    }

    TargetGroup {
        targets: vec![address],
        labels: m,
        source: source.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Port of upstream `hcloud_test.go::TestParseHCloudNetworksList`: `mynet`
    /// (id 4711) plus `next_page == 4`.
    #[test]
    fn parse_networks_list_matches_upstream() {
        let data = br#"{
            "meta": {"pagination": {"last_page": 4, "next_page": 4, "page": 3}},
            "networks": [{"id": 4711, "name": "mynet", "ip_range": "10.0.0.0/16"}]
        }"#;
        let (nets, next_page) = parse_hcloud_networks_list(data).unwrap();
        assert_eq!(
            nets,
            vec![HCloudNetwork {
                id: 4711,
                name: "mynet".into()
            }]
        );
        assert_eq!(next_page, 4);
    }

    /// Port of upstream `hcloud_test.go::TestParseHCloudServerListResponse`:
    /// one fully-populated server parses to the expected struct + `next_page`,
    /// and `appendHCloudTargetLabels` yields exactly the expected
    /// `__meta_hetzner_*` set and `publicIPv4:port` `__address__`.
    #[test]
    fn parse_server_list_and_labels_match_upstream() {
        let data = br#"{
            "meta": {"pagination": {"next_page": 4, "page": 3}},
            "servers": [{
              "id": 42, "name": "my-resource", "status": "running",
              "datacenter": {"name": "fsn1-dc8"},
              "image": {"description": "Ubuntu 20.04 Standard 64 bit",
                        "name": "ubuntu-20.04", "os_flavor": "ubuntu", "os_version": "20.04"},
              "private_net": [{"ip": "10.0.0.2", "network": 4711}],
              "public_net": {"ipv4": {"ip": "1.2.3.4"}, "ipv6": {"ip": "2001:db8::/64"}},
              "server_type": {"cores": 1, "cpu_type": "shared", "disk": 25,
                              "memory": 1, "name": "cx11"},
              "location": {"name": "fsn1", "network_zone": "eu-central"}
            }]
        }"#;
        let (servers, next_page) = parse_hcloud_server_list(data).unwrap();
        assert_eq!(next_page, 4);
        assert_eq!(servers.len(), 1);
        let s = &servers[0];
        assert_eq!(s.id, 42);
        assert_eq!(s.name, "my-resource");
        assert_eq!(s.public_net.ipv4.ip, "1.2.3.4");
        assert_eq!(s.public_net.ipv6.ip, "2001:db8::/64");
        assert_eq!(
            s.private_net,
            vec![HCloudPrivateNet {
                id: 4711,
                ip: "10.0.0.2".into()
            }]
        );
        assert_eq!(s.server_type.memory, 1.0);
        assert_eq!(s.datacenter.name, "fsn1-dc8");
        assert_eq!(s.location.name, "fsn1");

        let networks = vec![HCloudNetwork {
            id: 4711,
            name: "mynet".into(),
        }];
        let groups = append_hcloud_target_labels(&servers, &networks, 123, "job/hetzner");
        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert_eq!(g.targets, vec!["1.2.3.4:123".to_string()]);
        assert_eq!(g.source, "job/hetzner");
        let l = &g.labels;
        assert_eq!(l["__meta_hetzner_role"], "hcloud");
        assert_eq!(l["__meta_hetzner_server_id"], "42");
        assert_eq!(l["__meta_hetzner_server_name"], "my-resource");
        assert_eq!(l["__meta_hetzner_server_status"], "running");
        assert_eq!(l["__meta_hetzner_public_ipv4"], "1.2.3.4");
        assert_eq!(l["__meta_hetzner_public_ipv6_network"], "2001:db8::/64");
        assert_eq!(l["__meta_hetzner_datacenter"], "fsn1-dc8");
        assert_eq!(l["__meta_hetzner_hcloud_image_name"], "ubuntu-20.04");
        assert_eq!(
            l["__meta_hetzner_hcloud_image_description"],
            "Ubuntu 20.04 Standard 64 bit"
        );
        assert_eq!(l["__meta_hetzner_hcloud_image_os_flavor"], "ubuntu");
        assert_eq!(l["__meta_hetzner_hcloud_image_os_version"], "20.04");
        assert_eq!(l["__meta_hetzner_hcloud_location"], "fsn1");
        assert_eq!(
            l["__meta_hetzner_hcloud_location_network_zone"],
            "eu-central"
        );
        assert_eq!(l["__meta_hetzner_hcloud_datacenter_location"], "fsn1");
        assert_eq!(
            l["__meta_hetzner_hcloud_datacenter_location_network_zone"],
            "eu-central"
        );
        assert_eq!(l["__meta_hetzner_hcloud_server_type"], "cx11");
        assert_eq!(l["__meta_hetzner_hcloud_cpu_cores"], "1");
        assert_eq!(l["__meta_hetzner_hcloud_cpu_type"], "shared");
        assert_eq!(l["__meta_hetzner_hcloud_memory_size_gb"], "1");
        assert_eq!(l["__meta_hetzner_hcloud_disk_size_gb"], "25");
        assert_eq!(l["__meta_hetzner_hcloud_private_ipv4_mynet"], "10.0.0.2");
        // __address__ is the target, not a label.
        assert!(!l.contains_key("__address__"));
    }

    /// A server whose `image` is absent omits the four `image_*` labels
    /// (upstream `if server.Image != nil`), and a `labels` map produces
    /// `labelpresent_`/`label_` pairs with sanitized keys.
    #[test]
    fn no_image_and_custom_labels() {
        let mut server = HCloudServer {
            id: 7,
            public_net: HCloudPublicNet {
                ipv4: HCloudIPv4 {
                    ip: "5.6.7.8".into(),
                },
                ipv6: HCloudIPv6::default(),
            },
            ..HCloudServer::default()
        };
        server.labels.insert("my.team".into(), "core".into());

        let groups = append_hcloud_target_labels(&[server], &[], 80, "s");
        let l = &groups[0].labels;
        assert!(!l.contains_key("__meta_hetzner_hcloud_image_name"));
        // ipv6 empty -> no public_ipv6_network label.
        assert!(!l.contains_key("__meta_hetzner_public_ipv6_network"));
        // dotted label key is sanitized to `_`.
        assert_eq!(l["__meta_hetzner_hcloud_labelpresent_my_team"], "true");
        assert_eq!(l["__meta_hetzner_hcloud_label_my_team"], "core");
    }
}
