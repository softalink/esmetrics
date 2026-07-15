//! Docker container serde structs, their parser, and the
//! `__meta_docker_*` target-label builder ([`add_containers_labels`]).
//!
//! Port of `lib/promscrape/discovery/docker/container.go`'s `container`
//! structs, `parseContainers`, and `addContainersLabels` (container/network/
//! port fan-out, host-networking handling, shared-network `container:<id>`
//! link following, and `match_first_network`), reshaped for this crate's
//! [`TargetGroup`] (the `__address__` is carried in `targets`, every other
//! `__meta_docker_*` key in `labels` — mirroring `scrape::digitalocean`).

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::Deserialize;

use crate::scrape::config::ScrapeError;
use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::sanitize_label_name;

/// One Docker container (`GET /containers/json` array element). Port of
/// `container.go`'s `container`. Docker's JSON is PascalCase; fields are
/// renamed to idiomatic Rust names. `#[serde(default)]` tolerates the many
/// response fields this port ignores.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Container {
    #[serde(rename = "Id")]
    pub id: String,
    #[serde(rename = "Names")]
    pub names: Vec<String>,
    #[serde(rename = "Labels")]
    pub labels: BTreeMap<String, String>,
    #[serde(rename = "Ports")]
    pub ports: Vec<ContainerPort>,
    #[serde(rename = "HostConfig")]
    pub host_config: ContainerHostConfig,
    #[serde(rename = "NetworkSettings")]
    pub network_settings: ContainerNetworkSettings,
}

/// A published/exposed port of a [`Container`]. Port of `containerPort`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ContainerPort {
    #[serde(rename = "IP")]
    pub ip: String,
    #[serde(rename = "PrivatePort")]
    pub private_port: u16,
    #[serde(rename = "PublicPort")]
    pub public_port: u16,
    #[serde(rename = "Type")]
    pub kind: String,
}

/// The `HostConfig` block of a [`Container`]. Port of `containerHostConfig`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ContainerHostConfig {
    #[serde(rename = "NetworkMode")]
    pub network_mode: String,
}

/// The `NetworkSettings` block of a [`Container`]. Port of
/// `containerNetworkSettings`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ContainerNetworkSettings {
    #[serde(rename = "Networks")]
    pub networks: BTreeMap<String, ContainerNetwork>,
}

/// One attached network of a [`Container`]. Port of `containerNetwork`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ContainerNetwork {
    #[serde(rename = "IPAddress")]
    pub ip_address: String,
    #[serde(rename = "NetworkID")]
    pub network_id: String,
}

/// Parses a `GET /containers/json` response body. Port of `parseContainers`.
pub fn parse_containers(data: &[u8]) -> Result<Vec<Container>, ScrapeError> {
    serde_json::from_slice(data).map_err(|e| ScrapeError {
        msg: format!("cannot parse containers: {e}"),
    })
}

/// Builds a [`TargetGroup`] for every container/network/port combination,
/// mirroring `addContainersLabels`. `network_labels` is the ID-keyed map from
/// [`super::network::network_labels_by_id`]; `default_port` is used for the
/// fallback address when a container exposes no TCP port;
/// `host_networking_host` is the `__address__` for `network_mode: host`
/// containers with no TCP port.
pub fn add_containers_labels(
    containers: &[Container],
    network_labels: &BTreeMap<String, BTreeMap<String, String>>,
    default_port: u16,
    host_networking_host: &str,
    match_first_network: bool,
    source: &str,
) -> Vec<TargetGroup> {
    let idx_by_id: HashMap<&str, usize> = containers
        .iter()
        .enumerate()
        .map(|(i, c)| (c.id.as_str(), i))
        .collect();

    let mut groups = Vec::new();
    for c in containers {
        if c.names.is_empty() {
            continue;
        }

        let networks = resolve_networks(c, &idx_by_id, containers);
        let entries: Vec<(&String, &ContainerNetwork)> =
            if match_first_network && networks.len() > 1 {
                // BTreeMap iterates in ascending key order, so the first entry
                // is the lowest network name — matching upstream's
                // `sort.Strings(keys); keys[0]`.
                networks.iter().take(1).collect()
            } else {
                networks.iter().collect()
            };

        for (_name, n) in entries {
            let mut added = false;
            for p in &c.ports {
                if p.kind != "tcp" {
                    continue;
                }
                let mut m = BTreeMap::new();
                m.insert("__meta_docker_network_ip".to_string(), n.ip_address.clone());
                m.insert(
                    "__meta_docker_port_private".to_string(),
                    p.private_port.to_string(),
                );
                if p.public_port > 0 {
                    m.insert(
                        "__meta_docker_port_public".to_string(),
                        p.public_port.to_string(),
                    );
                    m.insert("__meta_docker_port_public_ip".to_string(), p.ip.clone());
                }
                add_common_labels(&mut m, c, network_labels.get(&n.network_id));
                groups.push(TargetGroup {
                    targets: vec![join_host_port(&n.ip_address, p.private_port)],
                    labels: m,
                    source: source.to_string(),
                });
                added = true;
            }
            if !added {
                // No exposed TCP port: fall back to a single target.
                let address = if c.host_config.network_mode == "host" {
                    host_networking_host.to_string()
                } else {
                    join_host_port(&n.ip_address, default_port)
                };
                let mut m = BTreeMap::new();
                m.insert("__meta_docker_network_ip".to_string(), n.ip_address.clone());
                add_common_labels(&mut m, c, network_labels.get(&n.network_id));
                groups.push(TargetGroup {
                    targets: vec![address],
                    labels: m,
                    source: source.to_string(),
                });
            }
        }
    }
    groups
}

/// Adds `__meta_docker_container_*` labels and joins the network's
/// `__meta_docker_network_*` labels (which overwrite any colliding key, as
/// upstream's `AddFrom` + `RemoveDuplicates` do). Port of `addCommonLabels`.
fn add_common_labels(
    m: &mut BTreeMap<String, String>,
    c: &Container,
    network_labels: Option<&BTreeMap<String, String>>,
) {
    m.insert("__meta_docker_container_id".to_string(), c.id.clone());
    m.insert(
        "__meta_docker_container_name".to_string(),
        c.names.first().cloned().unwrap_or_default(),
    );
    m.insert(
        "__meta_docker_container_network_mode".to_string(),
        c.host_config.network_mode.clone(),
    );
    for (k, v) in &c.labels {
        m.insert(
            sanitize_label_name(&format!("__meta_docker_container_label_{k}")),
            v.clone(),
        );
    }
    if let Some(nl) = network_labels {
        for (k, v) in nl {
            m.insert(k.clone(), v.clone());
        }
    }
}

/// Resolves the networks to fan out over: the container's own attached
/// networks, or — when it has none — the networks of the container it shares a
/// namespace with (`network_mode: container:<id>`), following the chain. Port
/// of the shared-network lookup in `addContainersLabels`.
fn resolve_networks<'a>(
    c: &'a Container,
    idx_by_id: &HashMap<&str, usize>,
    containers: &'a [Container],
) -> &'a BTreeMap<String, ContainerNetwork> {
    let mut networks = &c.network_settings.networks;
    let mut network_mode = c.host_config.network_mode.as_str();
    if networks.is_empty() {
        // Guard against a cyclic `container:<id>` link chain (A -> B -> A): a
        // crafted Docker response could otherwise spin this loop forever. Once
        // a container id repeats, the chain can never resolve to a non-empty
        // network set, so break and treat it as unresolved (empty), the same
        // outcome as a missing link.
        let mut visited: HashSet<&str> = HashSet::new();
        visited.insert(c.id.as_str());
        while let Some(cid) = try_get_linked_container_id(network_mode) {
            if !visited.insert(cid) {
                break;
            }
            let Some(&i) = idx_by_id.get(cid) else {
                break;
            };
            let tmp = &containers[i];
            networks = &tmp.network_settings.networks;
            network_mode = tmp.host_config.network_mode.as_str();
            if !networks.is_empty() {
                break;
            }
        }
    }
    networks
}

/// Parses `container:<id>` network modes, returning `<id>`. Port of
/// `tryGetLinkedContainerID`.
fn try_get_linked_container_id(network_mode: &str) -> Option<&str> {
    network_mode
        .strip_prefix("container:")
        .filter(|id| !id.is_empty())
}

/// `host:port`, bracketing `host` when it is an IPv6 address (contains a
/// `:`). Local copy of `discoveryutil.JoinHostPort` (matching
/// `scrape::digitalocean`).
fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
#[path = "labels_tests.rs"]
mod tests;
