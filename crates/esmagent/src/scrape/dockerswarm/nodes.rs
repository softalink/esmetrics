//! Docker Swarm node serde structs, their parser, and the
//! `__meta_dockerswarm_node_*` target-label builder ([`add_node_labels`]).
//!
//! Port of `lib/promscrape/discovery/dockerswarm/nodes.go`'s `node` structs,
//! `parseNodes`, and `addNodeLabels`. Each returned map carries `__address__`
//! (the node's `Status.Addr` joined with the config `port`) plus the
//! `__meta_dockerswarm_node_*` set; the refresh path splits `__address__` out
//! into a [`super::TargetGroup`]'s `targets`.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scrape::config::ScrapeError;
use crate::scrape::kubernetes::labels::sanitize_label_name;

use super::network::join_host_port;

/// One Docker Swarm node (`GET /nodes` array element). Port of `nodes.go`'s
/// `node`. `#[serde(default)]` tolerates the many ignored response fields.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Node {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "Spec")]
    pub spec: NodeSpec,
    #[serde(rename = "Description")]
    pub description: NodeDescription,
    #[serde(rename = "Status")]
    pub status: NodeStatus,
    #[serde(rename = "ManagerStatus")]
    pub manager_status: NodeManagerStatus,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct NodeSpec {
    #[serde(rename = "Labels")]
    pub labels: BTreeMap<String, String>,
    #[serde(rename = "Role")]
    pub role: String,
    #[serde(rename = "Availability")]
    pub availability: String,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct NodeDescription {
    #[serde(rename = "Hostname")]
    pub hostname: String,
    #[serde(rename = "Platform")]
    pub platform: NodePlatform,
    #[serde(rename = "Engine")]
    pub engine: NodeEngine,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct NodePlatform {
    #[serde(rename = "Architecture")]
    pub architecture: String,
    #[serde(rename = "OS")]
    pub os: String,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct NodeEngine {
    #[serde(rename = "EngineVersion")]
    pub engine_version: String,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct NodeStatus {
    #[serde(rename = "State")]
    pub state: String,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Addr")]
    pub addr: String,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct NodeManagerStatus {
    #[serde(rename = "Leader")]
    pub leader: bool,
    #[serde(rename = "Reachability")]
    pub reachability: String,
    #[serde(rename = "Addr")]
    pub addr: String,
}

/// Parses a `GET /nodes` response body. Port of `parseNodes`.
pub fn parse_nodes(data: &[u8]) -> Result<Vec<Node>, ScrapeError> {
    serde_json::from_slice(data).map_err(|e| ScrapeError {
        msg: format!("cannot parse nodes: {e}"),
    })
}

/// Builds one `__meta_dockerswarm_node_*` label map (including `__address__`)
/// per node. Port of `addNodeLabels`.
pub fn add_node_labels(nodes: &[Node], port: u16) -> Vec<BTreeMap<String, String>> {
    let mut ms = Vec::new();
    for node in nodes {
        let mut m: BTreeMap<String, String> = BTreeMap::new();
        m.insert(
            "__address__".to_string(),
            join_host_port(&node.status.addr, port),
        );
        m.insert(
            "__meta_dockerswarm_node_address".to_string(),
            node.status.addr.clone(),
        );
        m.insert(
            "__meta_dockerswarm_node_availability".to_string(),
            node.spec.availability.clone(),
        );
        m.insert(
            "__meta_dockerswarm_node_engine_version".to_string(),
            node.description.engine.engine_version.clone(),
        );
        m.insert(
            "__meta_dockerswarm_node_hostname".to_string(),
            node.description.hostname.clone(),
        );
        m.insert("__meta_dockerswarm_node_id".to_string(), node.id.clone());
        m.insert(
            "__meta_dockerswarm_node_manager_address".to_string(),
            node.manager_status.addr.clone(),
        );
        m.insert(
            "__meta_dockerswarm_node_manager_leader".to_string(),
            node.manager_status.leader.to_string(),
        );
        m.insert(
            "__meta_dockerswarm_node_manager_reachability".to_string(),
            node.manager_status.reachability.clone(),
        );
        m.insert(
            "__meta_dockerswarm_node_platform_architecture".to_string(),
            node.description.platform.architecture.clone(),
        );
        m.insert(
            "__meta_dockerswarm_node_platform_os".to_string(),
            node.description.platform.os.clone(),
        );
        m.insert(
            "__meta_dockerswarm_node_role".to_string(),
            node.spec.role.clone(),
        );
        m.insert(
            "__meta_dockerswarm_node_status".to_string(),
            node.status.state.clone(),
        );
        for (k, v) in &node.spec.labels {
            m.insert(
                sanitize_label_name(&format!("__meta_dockerswarm_node_label_{k}")),
                v.clone(),
            );
        }
        ms.push(m);
    }
    ms
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_node() -> Node {
        Node {
            id: "qauwmifceyvqs0sipvzu8oslu".into(),
            spec: NodeSpec {
                role: "manager".into(),
                availability: "active".into(),
                ..NodeSpec::default()
            },
            status: NodeStatus {
                state: "ready".into(),
                addr: "172.31.40.97".into(),
                ..NodeStatus::default()
            },
            description: NodeDescription {
                hostname: "ip-172-31-40-97".into(),
                platform: NodePlatform {
                    architecture: "x86_64".into(),
                    os: "linux".into(),
                },
                engine: NodeEngine {
                    engine_version: "19.03.11".into(),
                },
            },
            manager_status: NodeManagerStatus::default(),
        }
    }

    /// Port of upstream `nodes_test.go::TestParseNodes`.
    #[test]
    fn parse_nodes_extracts_one() {
        let data = br#"[
  { "ID": "qauwmifceyvqs0sipvzu8oslu",
    "Spec": { "Role": "manager", "Availability": "active" },
    "Description": { "Hostname": "ip-172-31-40-97",
      "Platform": { "Architecture": "x86_64", "OS": "linux" },
      "Engine": { "EngineVersion": "19.03.11" } },
    "Status": { "State": "ready", "Addr": "172.31.40.97" } }
]"#;
        let nodes = parse_nodes(data).unwrap();
        assert_eq!(nodes, vec![sample_node()]);
    }

    /// Port of upstream `nodes_test.go::TestAddNodeLabels`.
    #[test]
    fn add_node_labels_matches_upstream() {
        let ms = add_node_labels(&[sample_node()], 9100);
        assert_eq!(ms.len(), 1);
        let expected = BTreeMap::from([
            ("__address__", "172.31.40.97:9100"),
            ("__meta_dockerswarm_node_address", "172.31.40.97"),
            ("__meta_dockerswarm_node_availability", "active"),
            ("__meta_dockerswarm_node_engine_version", "19.03.11"),
            ("__meta_dockerswarm_node_hostname", "ip-172-31-40-97"),
            ("__meta_dockerswarm_node_manager_address", ""),
            ("__meta_dockerswarm_node_manager_leader", "false"),
            ("__meta_dockerswarm_node_manager_reachability", ""),
            ("__meta_dockerswarm_node_id", "qauwmifceyvqs0sipvzu8oslu"),
            ("__meta_dockerswarm_node_platform_architecture", "x86_64"),
            ("__meta_dockerswarm_node_platform_os", "linux"),
            ("__meta_dockerswarm_node_role", "manager"),
            ("__meta_dockerswarm_node_status", "ready"),
        ]);
        let got: BTreeMap<&str, &str> = ms[0]
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(got, expected);
    }
}
