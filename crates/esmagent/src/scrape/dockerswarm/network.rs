//! Docker Swarm network serde struct, its parser, and the
//! `__meta_dockerswarm_network_*` label map keyed by network ID, plus the
//! two address helpers ([`join_host_port`], [`parse_cidr_ip`]) shared by the
//! services/tasks/nodes label builders.
//!
//! Port of `lib/promscrape/discovery/dockerswarm/network.go`'s `network`
//! struct, `parseNetworks`, and `getNetworkLabelsByNetworkID`. The label map
//! is joined onto each service/task target by network ID.

use std::collections::BTreeMap;
use std::net::IpAddr;

use serde::Deserialize;

use crate::scrape::config::ScrapeError;
use crate::scrape::kubernetes::labels::sanitize_label_name;

/// One Docker Swarm network (`GET /networks` array element). Port of
/// `network.go`'s `network`. Docker's JSON is PascalCase; fields are renamed
/// to idiomatic Rust names, and `#[serde(default)]` tolerates the many
/// response fields this port ignores. `Id` also appears (as `Network.ID`)
/// inside a task's `NetworksAttachments`, where only the ID is consulted.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Network {
    #[serde(rename = "ID", alias = "Id")]
    pub id: String,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Scope")]
    pub scope: String,
    #[serde(rename = "Internal")]
    pub internal: bool,
    #[serde(rename = "Ingress")]
    pub ingress: bool,
    #[serde(rename = "Labels")]
    pub labels: BTreeMap<String, String>,
}

/// Parses a `GET /networks` response body. Port of `parseNetworks`.
pub fn parse_networks(data: &[u8]) -> Result<Vec<Network>, ScrapeError> {
    serde_json::from_slice(data).map_err(|e| ScrapeError {
        msg: format!("cannot parse networks: {e}"),
    })
}

/// Builds the `__meta_dockerswarm_network_*` label set for every network,
/// keyed by network ID. Port of `getNetworkLabelsByNetworkID`.
pub fn network_labels_by_id(networks: &[Network]) -> BTreeMap<String, BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for network in networks {
        let mut m: BTreeMap<String, String> = BTreeMap::new();
        m.insert(
            "__meta_dockerswarm_network_id".to_string(),
            network.id.clone(),
        );
        m.insert(
            "__meta_dockerswarm_network_name".to_string(),
            network.name.clone(),
        );
        m.insert(
            "__meta_dockerswarm_network_internal".to_string(),
            network.internal.to_string(),
        );
        m.insert(
            "__meta_dockerswarm_network_ingress".to_string(),
            network.ingress.to_string(),
        );
        m.insert(
            "__meta_dockerswarm_network_scope".to_string(),
            network.scope.clone(),
        );
        for (k, v) in &network.labels {
            m.insert(
                sanitize_label_name(&format!("__meta_dockerswarm_network_label_{k}")),
                v.clone(),
            );
        }
        out.insert(network.id.clone(), m);
    }
    out
}

/// `host:port`, bracketing `host` when it is an IPv6 address (contains a
/// `:`). Local copy of `discoveryutil.JoinHostPort` (matching
/// `scrape::docker`).
pub(crate) fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Extracts the host IP from a CIDR string (`"10.0.0.3/24"` -> `"10.0.0.3"`),
/// normalizing it via [`IpAddr`]'s `Display`. Port of the `net.ParseCIDR`
/// (`ip.String()`) usage in `services.go`/`tasks.go`: the returned IP is the
/// full address, not the masked network. Returns `None` on an unparseable
/// address (upstream logs and skips such entries).
pub(crate) fn parse_cidr_ip(addr: &str) -> Option<String> {
    let ip_part = addr.split('/').next()?;
    ip_part.parse::<IpAddr>().ok().map(|ip| ip.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Port of upstream `network_test.go::TestParseNetworks` (two networks).
    #[test]
    fn parse_networks_extracts_two() {
        let data = br#"[
  { "Name": "ingress", "Id": "qs0hog6ldlei9ct11pr3c77v1", "Scope": "swarm",
    "Internal": false, "Ingress": true, "Labels": { "key1": "value1" } },
  { "Name": "host", "Id": "317f0384d7e5", "Scope": "local",
    "Internal": false, "Ingress": false, "Labels": { "key": "value" } }
]"#;
        let networks = parse_networks(data).unwrap();
        assert_eq!(networks.len(), 2);
        assert_eq!(networks[0].id, "qs0hog6ldlei9ct11pr3c77v1");
        assert_eq!(networks[0].name, "ingress");
        assert!(networks[0].ingress);
        assert_eq!(networks[0].scope, "swarm");
        assert_eq!(networks[0].labels["key1"], "value1");
        assert_eq!(networks[1].id, "317f0384d7e5");
        assert_eq!(networks[1].scope, "local");
    }

    /// Port of upstream `network_test.go::TestAddNetworkLabels` (ingress
    /// network -> exact `__meta_dockerswarm_network_*` set).
    #[test]
    fn network_labels_match_upstream() {
        let networks = vec![Network {
            id: "qs0hog6ldlei9ct11pr3c77v1".into(),
            ingress: true,
            scope: "swarm".into(),
            name: "ingress".into(),
            internal: false,
            labels: BTreeMap::from([("key1".to_string(), "value1".to_string())]),
        }];
        let by_id = network_labels_by_id(&networks);
        let m = &by_id["qs0hog6ldlei9ct11pr3c77v1"];
        let expected = BTreeMap::from([
            (
                "__meta_dockerswarm_network_id".to_string(),
                "qs0hog6ldlei9ct11pr3c77v1".to_string(),
            ),
            (
                "__meta_dockerswarm_network_ingress".to_string(),
                "true".to_string(),
            ),
            (
                "__meta_dockerswarm_network_internal".to_string(),
                "false".to_string(),
            ),
            (
                "__meta_dockerswarm_network_label_key1".to_string(),
                "value1".to_string(),
            ),
            (
                "__meta_dockerswarm_network_name".to_string(),
                "ingress".to_string(),
            ),
            (
                "__meta_dockerswarm_network_scope".to_string(),
                "swarm".to_string(),
            ),
        ]);
        assert_eq!(m, &expected);
    }

    #[test]
    fn parse_cidr_ip_extracts_host_ip() {
        assert_eq!(parse_cidr_ip("10.0.0.3/24").as_deref(), Some("10.0.0.3"));
        assert_eq!(
            parse_cidr_ip("10.10.15.15/24").as_deref(),
            Some("10.10.15.15")
        );
        assert_eq!(parse_cidr_ip("not-an-ip").as_deref(), None);
    }

    #[test]
    fn join_host_port_brackets_ipv6() {
        assert_eq!(join_host_port("10.0.0.3", 8081), "10.0.0.3:8081");
        assert_eq!(join_host_port("fe80::1", 80), "[fe80::1]:80");
    }
}
