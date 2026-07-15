//! Consul Agent `ServiceNode`/`Agent` serde structs, the
//! `__meta_consulagent_*` label builder ([`append_target_labels`]), and
//! check-status aggregation ([`aggregated_status`]).
//!
//! Port of `lib/promscrape/discovery/consulagent/service_node.go`
//! (`getServiceNodesLabels`/`appendTargetLabels`) plus the `consul` package's
//! `ServiceNode`/`Service`/`Node`/`Check`/`Agent`/`AggregatedStatus` structs it
//! reuses, and `discoveryutil.AddTagsToLabels` / `JoinHostPort` /
//! `SanitizeLabelName` (`lib/promscrape/discoveryutil/util.go`), reshaped for
//! this crate's [`TargetGroup`] shape (one group per service node: the node's
//! `__address__` is the group's single target, and the `__meta_consulagent_*`
//! set becomes the group's `labels`).
//!
//! ## Key differences from the plain Consul port (`scrape::consul::labels`)
//!
//! - The label prefix is `__meta_consulagent_` (not `__meta_consul_`).
//! - `__meta_consulagent_address`/`_dc`/`_node` come from the *local agent*
//!   (`Agent.Member.Addr` / `Agent.Config.Datacenter` / `Agent.Config.NodeName`)
//!   rather than the per-node `Node` block, and the service-address fallback
//!   host is the agent member address (not the node address).
//! - There is NO `__meta_consulagent_partition` label (upstream omits it).
//! - `_metadata_*` comes from `Agent.Meta` (not the node meta), and
//!   `_tagged_address_*` is joined from BOTH `Node.TaggedAddresses` (plain
//!   string values) AND `Service.TaggedAddresses` (an `{address, port}` object
//!   rendered as `address:port`).
//!
//! Upstream includes `__address__` in the returned label set because a
//! Prometheus label set *is* the target; this crate's [`TargetGroup`] carries
//! the address separately in `targets`, so [`append_target_labels`] puts it
//! there and leaves it out of `labels` — `target::build_targets` seeds
//! `__address__` from the target string and overlays `labels`, so the
//! resulting relabel input is identical.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::sanitize_label_name;

/// One Consul service node. Port of the `consul` package's `ServiceNode`
/// (`/v1/agent/health/service/name/<svc>` array element). Consul serializes
/// fields in PascalCase.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ServiceNode {
    pub service: Service,
    pub node: Node,
    pub checks: Vec<Check>,
}

/// The `Service` block of a [`ServiceNode`] (also the value shape of
/// `/v1/agent/services`). `ID` keeps Consul's exact capitalization (PascalCase
/// would render it `Id`). `datacenter` is used to filter `/v1/agent/services`
/// entries to the watched datacenter.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct Service {
    #[serde(rename = "ID")]
    pub id: String,
    pub service: String,
    pub address: String,
    pub namespace: String,
    pub port: i64,
    pub tags: Vec<String>,
    pub meta: BTreeMap<String, String>,
    pub tagged_addresses: BTreeMap<String, ServiceTaggedAddress>,
    pub datacenter: String,
}

/// A `Service.TaggedAddresses` entry: an `{address, port}` object. Consul
/// serializes these inner fields in lowercase (unlike the PascalCase outer
/// fields), so this struct uses the default (lowercase) field names.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ServiceTaggedAddress {
    pub address: String,
    pub port: i64,
}

/// The `Node` block of a [`ServiceNode`].
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct Node {
    pub tagged_addresses: BTreeMap<String, String>,
}

/// One health check of a [`ServiceNode`]. `CheckID` keeps Consul's exact
/// capitalization.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct Check {
    #[serde(rename = "CheckID")]
    pub check_id: String,
    pub status: String,
}

/// The local Consul agent, from `/v1/agent/self`. Port of the `consul`
/// package's `Agent`/`AgentConfig`/`AgentMember`, narrowed to the fields this
/// port reads for labels: the member address, the datacenter/node name, and
/// the agent metadata map.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct Agent {
    pub config: AgentConfig,
    pub member: AgentMember,
    pub meta: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct AgentConfig {
    pub datacenter: String,
    pub node_name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct AgentMember {
    pub addr: String,
}

/// Parses a `/v1/agent/self` response body into an [`Agent`]. Port of the
/// `consul` package's `ParseAgent`.
pub fn parse_agent(data: &[u8]) -> Result<Agent, String> {
    serde_json::from_slice(data).map_err(|e| format!("cannot unmarshal agent info: {e}"))
}

/// Parses a `/v1/agent/health/service/name/<svc>` response body into a list of
/// [`ServiceNode`]. Port of the `consul` package's `ParseServiceNodes`.
pub fn parse_service_nodes(data: &[u8]) -> Result<Vec<ServiceNode>, String> {
    serde_json::from_slice(data).map_err(|e| format!("cannot unmarshal ServiceNodes: {e}"))
}

/// Builds the [`TargetGroup`] for one service node, mirroring
/// `appendTargetLabels`. `__address__` is `Service.Address:Service.Port`
/// (falling back to `Agent.Member.Addr:Service.Port` when the service address
/// is empty) and is carried in the group's `targets`; every
/// `__meta_consulagent_*` label goes in `labels`. `source` is threaded through
/// unchanged so the reconcile diff stays stable across refreshes.
pub fn append_target_labels(
    sn: &ServiceNode,
    service_name: &str,
    tag_separator: &str,
    agent: &Agent,
    source: String,
) -> TargetGroup {
    let host = if sn.service.address.is_empty() {
        &agent.member.addr
    } else {
        &sn.service.address
    };
    let address = join_host_port(host, sn.service.port);

    let mut m: BTreeMap<String, String> = BTreeMap::new();
    m.insert(
        "__meta_consulagent_address".into(),
        agent.member.addr.clone(),
    );
    m.insert(
        "__meta_consulagent_dc".into(),
        agent.config.datacenter.clone(),
    );
    m.insert(
        "__meta_consulagent_health".into(),
        aggregated_status(&sn.checks),
    );
    m.insert(
        "__meta_consulagent_namespace".into(),
        sn.service.namespace.clone(),
    );
    m.insert(
        "__meta_consulagent_node".into(),
        agent.config.node_name.clone(),
    );
    m.insert(
        "__meta_consulagent_service".into(),
        service_name.to_string(),
    );
    m.insert(
        "__meta_consulagent_service_address".into(),
        sn.service.address.clone(),
    );
    m.insert(
        "__meta_consulagent_service_id".into(),
        sn.service.id.clone(),
    );
    m.insert(
        "__meta_consulagent_service_port".into(),
        sn.service.port.to_string(),
    );

    add_tags_to_labels(
        &mut m,
        &sn.service.tags,
        "__meta_consulagent_",
        tag_separator,
    );

    for (k, v) in &agent.meta {
        m.insert(
            sanitize_label_name(&format!("__meta_consulagent_metadata_{k}")),
            v.clone(),
        );
    }
    for (k, v) in &sn.service.meta {
        m.insert(
            sanitize_label_name(&format!("__meta_consulagent_service_metadata_{k}")),
            v.clone(),
        );
    }
    for (k, v) in &sn.node.tagged_addresses {
        m.insert(
            sanitize_label_name(&format!("__meta_consulagent_tagged_address_{k}")),
            v.clone(),
        );
    }
    for (k, v) in &sn.service.tagged_addresses {
        m.insert(
            sanitize_label_name(&format!("__meta_consulagent_tagged_address_{k}")),
            format!("{}:{}", v.address, v.port),
        );
    }

    TargetGroup {
        targets: vec![address],
        labels: m,
        source,
    }
}

/// Port of `discoveryutil.AddTagsToLabels`: adds `<prefix>tags` (the tag
/// list joined and surrounded by `tag_separator`) plus, per tag,
/// `<prefix>tag_<k>`=`<v>` and `<prefix>tagpresent_<k>`=`true` (a `k=v` tag
/// splits on the first `=`; a bare tag has an empty value). Both per-tag
/// names are sanitized.
fn add_tags_to_labels(
    m: &mut BTreeMap<String, String>,
    tags: &[String],
    prefix: &str,
    tag_separator: &str,
) {
    m.insert(
        format!("{prefix}tags"),
        format!("{tag_separator}{}{tag_separator}", tags.join(tag_separator)),
    );
    for tag in tags {
        let (k, v) = match tag.find('=') {
            Some(n) => (&tag[..n], &tag[n + 1..]),
            None => (tag.as_str(), ""),
        };
        m.insert(
            sanitize_label_name(&format!("{prefix}tag_{k}")),
            v.to_string(),
        );
        m.insert(
            sanitize_label_name(&format!("{prefix}tagpresent_{k}")),
            "true".to_string(),
        );
    }
}

/// `host:port`, bracketing `host` when it is an IPv6 address (contains a
/// `:`). Port of `discoveryutil.JoinHostPort`.
fn join_host_port(host: &str, port: i64) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Aggregated health of a service node's checks, mirroring the `consul`
/// package's `AggregatedStatus`: a `_node_maintenance` /
/// `_service_maintenance:*` check ID means `maintenance`; otherwise the worst
/// of critical > warning > passing wins; an unrecognized status string yields
/// `""`.
pub fn aggregated_status(checks: &[Check]) -> String {
    let mut warning = false;
    let mut critical = false;
    let mut maintenance = false;
    for check in checks {
        let id = &check.check_id;
        if id == "_node_maintenance" || id.starts_with("_service_maintenance:") {
            maintenance = true;
            continue;
        }
        match check.status.as_str() {
            "passing" => {}
            "warning" => warning = true,
            "critical" => critical = true,
            _ => return String::new(),
        }
    }
    if maintenance {
        "maintenance".to_string()
    } else if critical {
        "critical".to_string()
    } else if warning {
        "warning".to_string()
    } else {
        "passing".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(id: &str, status: &str) -> Check {
        Check {
            check_id: id.to_string(),
            status: status.to_string(),
        }
    }

    /// Parses the exact `service_node_test.go` fixture (service `redis` with
    /// tags, tagged addresses, meta) plus its agent fixture, and asserts the
    /// full `__meta_consulagent_*` map from that test's `expectedLabelss`.
    #[test]
    fn parses_upstream_fixture_and_builds_expected_labels() {
        let data = br#"
[
  {
    "Service": {
      "ID": "redis",
      "Service": "redis",
      "Tags": ["primary","foo=bar"],
      "Address": "10.1.10.12",
      "TaggedAddresses": {
        "lan": {"address": "10.1.10.12", "port": 8000},
        "wan": {"address": "198.18.1.2", "port": 80}
      },
      "Meta": {"redis_version": "4.0"},
      "Port": 8000,
      "Namespace": "ns-dev",
      "Partition": "part-foobar"
    },
    "Checks": [
      {"CheckID": "service:redis", "Status": "passing"},
      {"CheckID": "serfHealth", "Status": "passing"}
    ]
  }
]
"#;
        let sns = parse_service_nodes(data).unwrap();
        assert_eq!(sns.len(), 1);

        let agent = parse_agent(
            br#"{"Member":{"Addr":"10.1.10.12"},
                 "Config":{"Datacenter":"dc1","NodeName":"foobar"},
                 "Meta":{"instance_type":"t2.medium"}}"#,
        )
        .unwrap();

        let g = append_target_labels(&sns[0], "redis", ",", &agent, "src".into());
        let l = &g.labels;

        assert_eq!(g.targets, vec!["10.1.10.12:8000".to_string()]);
        assert_eq!(l["__meta_consulagent_address"], "10.1.10.12");
        assert_eq!(l["__meta_consulagent_dc"], "dc1");
        assert_eq!(l["__meta_consulagent_health"], "passing");
        assert_eq!(l["__meta_consulagent_metadata_instance_type"], "t2.medium");
        assert_eq!(l["__meta_consulagent_namespace"], "ns-dev");
        assert_eq!(l["__meta_consulagent_node"], "foobar");
        assert_eq!(l["__meta_consulagent_service"], "redis");
        assert_eq!(l["__meta_consulagent_service_address"], "10.1.10.12");
        assert_eq!(l["__meta_consulagent_service_id"], "redis");
        assert_eq!(
            l["__meta_consulagent_service_metadata_redis_version"],
            "4.0"
        );
        assert_eq!(l["__meta_consulagent_service_port"], "8000");
        assert_eq!(
            l["__meta_consulagent_tagged_address_lan"],
            "10.1.10.12:8000"
        );
        assert_eq!(l["__meta_consulagent_tagged_address_wan"], "198.18.1.2:80");
        assert_eq!(l["__meta_consulagent_tag_foo"], "bar");
        assert_eq!(l["__meta_consulagent_tag_primary"], "");
        assert_eq!(l["__meta_consulagent_tagpresent_foo"], "true");
        assert_eq!(l["__meta_consulagent_tagpresent_primary"], "true");
        assert_eq!(l["__meta_consulagent_tags"], ",primary,foo=bar,");

        // No partition label (upstream consulagent omits it), and __address__
        // is the target, not a label.
        assert!(!l.contains_key("__meta_consulagent_partition"));
        assert!(!l.contains_key("__address__"));
    }

    #[test]
    fn address_falls_back_to_agent_member_addr() {
        let sn = ServiceNode {
            service: Service {
                address: String::new(),
                port: 8080,
                ..Service::default()
            },
            ..ServiceNode::default()
        };
        let agent = Agent {
            member: AgentMember {
                addr: "9.9.9.9".into(),
            },
            ..Agent::default()
        };
        let g = append_target_labels(&sn, "web", ",", &agent, "src".into());
        assert_eq!(g.targets, vec!["9.9.9.9:8080".to_string()]);
    }

    #[test]
    fn ipv6_host_is_bracketed() {
        assert_eq!(join_host_port("::1", 80), "[::1]:80");
        assert_eq!(join_host_port("10.0.0.1", 80), "10.0.0.1:80");
    }

    #[test]
    fn metadata_label_names_are_sanitized() {
        let sn = ServiceNode::default();
        let agent = Agent {
            meta: [("app.kubernetes.io/name".to_string(), "web".to_string())].into(),
            ..Agent::default()
        };
        let g = append_target_labels(&sn, "web", ",", &agent, "src".into());
        assert_eq!(
            g.labels["__meta_consulagent_metadata_app_kubernetes_io_name"],
            "web"
        );
    }

    #[test]
    fn node_tagged_addresses_are_plain_strings() {
        let sn = ServiceNode {
            node: Node {
                tagged_addresses: [("lan".to_string(), "10.0.0.1".to_string())].into(),
            },
            ..ServiceNode::default()
        };
        let g = append_target_labels(&sn, "web", ",", &Agent::default(), "src".into());
        assert_eq!(
            g.labels["__meta_consulagent_tagged_address_lan"],
            "10.0.0.1"
        );
    }

    #[test]
    fn aggregated_status_precedence() {
        assert_eq!(aggregated_status(&[check("c", "passing")]), "passing");
        assert_eq!(
            aggregated_status(&[check("c", "passing"), check("c2", "warning")]),
            "warning"
        );
        assert_eq!(
            aggregated_status(&[check("c", "warning"), check("c2", "critical")]),
            "critical"
        );
        assert_eq!(
            aggregated_status(&[check("_node_maintenance", ""), check("c", "critical")]),
            "maintenance"
        );
        assert_eq!(
            aggregated_status(&[check("_service_maintenance:web", ""), check("c", "passing")]),
            "maintenance"
        );
        assert_eq!(aggregated_status(&[check("c", "bogus")]), "");
        assert_eq!(aggregated_status(&[]), "passing");
    }
}
