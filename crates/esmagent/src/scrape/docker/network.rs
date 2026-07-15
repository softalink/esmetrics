//! Docker network serde struct, its parser, and the
//! `__meta_docker_network_*` label map keyed by network ID.
//!
//! Port of `lib/promscrape/discovery/docker/network.go`'s `network` struct,
//! `parseNetworks`, and `getNetworkLabelsByNetworkID`. The label map is joined
//! onto each container's target by network ID in
//! [`super::labels::add_containers_labels`].

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scrape::config::ScrapeError;
use crate::scrape::kubernetes::labels::sanitize_label_name;

/// One Docker network (`GET /networks` array element). Port of
/// `network.go`'s `network`. Field names use `#[serde(rename)]` to match the
/// PascalCase Docker API JSON while keeping idiomatic Rust field names;
/// `#[serde(default)]` tolerates the many response fields this port ignores.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Network {
    #[serde(rename = "Id")]
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

/// Builds the `__meta_docker_network_*` label set for every network, keyed by
/// network ID. Port of `getNetworkLabelsByNetworkID`.
pub fn network_labels_by_id(networks: &[Network]) -> BTreeMap<String, BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for network in networks {
        let mut m: BTreeMap<String, String> = BTreeMap::new();
        m.insert("__meta_docker_network_id".to_string(), network.id.clone());
        m.insert(
            "__meta_docker_network_name".to_string(),
            network.name.clone(),
        );
        m.insert(
            "__meta_docker_network_internal".to_string(),
            network.internal.to_string(),
        );
        m.insert(
            "__meta_docker_network_ingress".to_string(),
            network.ingress.to_string(),
        );
        m.insert(
            "__meta_docker_network_scope".to_string(),
            network.scope.clone(),
        );
        for (k, v) in &network.labels {
            m.insert(
                sanitize_label_name(&format!("__meta_docker_network_label_{k}")),
                v.clone(),
            );
        }
        out.insert(network.id.clone(), m);
    }
    out
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
    /// network -> exact `__meta_docker_network_*` set).
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
        assert_eq!(m["__meta_docker_network_id"], "qs0hog6ldlei9ct11pr3c77v1");
        assert_eq!(m["__meta_docker_network_ingress"], "true");
        assert_eq!(m["__meta_docker_network_internal"], "false");
        assert_eq!(m["__meta_docker_network_label_key1"], "value1");
        assert_eq!(m["__meta_docker_network_name"], "ingress");
        assert_eq!(m["__meta_docker_network_scope"], "swarm");
    }
}
