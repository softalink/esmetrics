//! Consul `ServiceNode` serde structs, the `__meta_consul_*` label builder
//! ([`append_target_labels`]), and check-status aggregation
//! ([`aggregated_status`]).
//!
//! Port of `lib/promscrape/discovery/consul/service_node.go`
//! (`ServiceNode`/`Service`/`Node`/`Check`, `appendTargetLabels`,
//! `AggregatedStatus`) plus `discoveryutil.AddTagsToLabels` /
//! `JoinHostPort` / `SanitizeLabelName`
//! (`lib/promscrape/discoveryutil/util.go`), reshaped for this crate's
//! [`TargetGroup`] shape (one group per service node: the node's
//! `__address__` is the group's single target, and the `__meta_consul_*`
//! set becomes the group's `labels`).
//!
//! Upstream includes `__address__` in the returned label set because a
//! Prometheus label set *is* the target; this crate's [`TargetGroup`]
//! carries the address separately in `targets`, so [`append_target_labels`]
//! puts it there and leaves it out of `labels` — `target::build_targets`
//! seeds `__address__` from the target string and overlays `labels`, so the
//! resulting relabel input is identical.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::sanitize_label_name;

/// One Consul service node. Port of `service_node.go`'s `ServiceNode`
/// (`/v1/health/service/<svc>` array element). Consul serializes fields in
/// PascalCase.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ServiceNode {
    pub service: Service,
    pub node: Node,
    pub checks: Vec<Check>,
}

/// The `Service` block of a [`ServiceNode`]. `ID` keeps Consul's exact
/// capitalization (PascalCase would render it `Id`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct Service {
    #[serde(rename = "ID")]
    pub id: String,
    pub service: String,
    pub address: String,
    pub namespace: String,
    pub partition: String,
    pub port: i64,
    pub tags: Vec<String>,
    pub meta: BTreeMap<String, String>,
    pub datacenter: String,
}

/// The `Node` block of a [`ServiceNode`].
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct Node {
    pub address: String,
    pub datacenter: String,
    pub node: String,
    pub meta: BTreeMap<String, String>,
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

/// Parses a `/v1/health/service/<svc>` response body into a list of
/// [`ServiceNode`]. Port of `ParseServiceNodes`.
pub fn parse_service_nodes(data: &[u8]) -> Result<Vec<ServiceNode>, String> {
    serde_json::from_slice(data).map_err(|e| format!("cannot unmarshal ServiceNodes: {e}"))
}

/// Builds the [`TargetGroup`] for one service node, mirroring
/// `appendTargetLabels`. `__address__` is `Service.Address:Service.Port`
/// (falling back to `Node.Address:Service.Port` when the service address is
/// empty) and is carried in the group's `targets`; every `__meta_consul_*`
/// label goes in `labels`. `source` is threaded through unchanged so the
/// reconcile diff stays stable across refreshes.
pub fn append_target_labels(
    sn: &ServiceNode,
    service_name: &str,
    tag_separator: &str,
    source: String,
) -> TargetGroup {
    let host = if sn.service.address.is_empty() {
        &sn.node.address
    } else {
        &sn.service.address
    };
    let address = join_host_port(host, sn.service.port);

    let mut m: BTreeMap<String, String> = BTreeMap::new();
    m.insert("__meta_consul_address".into(), sn.node.address.clone());
    m.insert("__meta_consul_dc".into(), sn.node.datacenter.clone());
    m.insert("__meta_consul_health".into(), aggregated_status(&sn.checks));
    m.insert(
        "__meta_consul_namespace".into(),
        sn.service.namespace.clone(),
    );
    m.insert(
        "__meta_consul_partition".into(),
        sn.service.partition.clone(),
    );
    m.insert("__meta_consul_node".into(), sn.node.node.clone());
    m.insert("__meta_consul_service".into(), service_name.to_string());
    m.insert(
        "__meta_consul_service_address".into(),
        sn.service.address.clone(),
    );
    m.insert("__meta_consul_service_id".into(), sn.service.id.clone());
    m.insert(
        "__meta_consul_service_port".into(),
        sn.service.port.to_string(),
    );

    add_tags_to_labels(&mut m, &sn.service.tags, "__meta_consul_", tag_separator);

    for (k, v) in &sn.node.meta {
        m.insert(
            sanitize_label_name(&format!("__meta_consul_metadata_{k}")),
            v.clone(),
        );
    }
    for (k, v) in &sn.service.meta {
        m.insert(
            sanitize_label_name(&format!("__meta_consul_service_metadata_{k}")),
            v.clone(),
        );
    }
    for (k, v) in &sn.node.tagged_addresses {
        m.insert(
            sanitize_label_name(&format!("__meta_consul_tagged_address_{k}")),
            v.clone(),
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

/// Aggregated health of a service node's checks, mirroring
/// `AggregatedStatus` (copied from Consul's `HealthChecks.AggregatedStatus`):
/// a `_node_maintenance` / `_service_maintenance:*` check ID means
/// `maintenance`; otherwise the worst of critical > warning > passing wins;
/// an unrecognized status string yields `""`.
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
        // `passing` needs no flag: it's the default result (upstream's
        // `passing` and no-checks arms both return "passing"); the match
        // exists to reject an unrecognized status with "".
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

    #[test]
    fn address_prefers_service_address() {
        let sn = ServiceNode {
            service: Service {
                address: "1.2.3.4".into(),
                port: 8080,
                ..Service::default()
            },
            node: Node {
                address: "9.9.9.9".into(),
                ..Node::default()
            },
            checks: vec![],
        };
        let g = append_target_labels(&sn, "web", ",", "src".into());
        assert_eq!(g.targets, vec!["1.2.3.4:8080".to_string()]);
    }

    #[test]
    fn address_falls_back_to_node_address() {
        let sn = ServiceNode {
            service: Service {
                address: String::new(),
                port: 8080,
                ..Service::default()
            },
            node: Node {
                address: "9.9.9.9".into(),
                ..Node::default()
            },
            checks: vec![],
        };
        let g = append_target_labels(&sn, "web", ",", "src".into());
        assert_eq!(g.targets, vec!["9.9.9.9:8080".to_string()]);
    }

    #[test]
    fn ipv6_host_is_bracketed() {
        assert_eq!(join_host_port("::1", 80), "[::1]:80");
        assert_eq!(join_host_port("10.0.0.1", 80), "10.0.0.1:80");
    }

    #[test]
    fn core_meta_labels_and_tag_and_metadata() {
        let sn = ServiceNode {
            service: Service {
                id: "web-1".into(),
                service: "web".into(),
                address: "1.2.3.4".into(),
                namespace: "ns".into(),
                partition: "part".into(),
                port: 8080,
                tags: vec!["prod".into(), "team=infra".into()],
                meta: [("version".to_string(), "1".to_string())].into(),
                datacenter: "dc1".into(),
            },
            node: Node {
                address: "9.9.9.9".into(),
                datacenter: "dc1".into(),
                node: "node-a".into(),
                meta: [("rack".to_string(), "r1".to_string())].into(),
                tagged_addresses: [("lan".to_string(), "10.0.0.1".to_string())].into(),
            },
            checks: vec![check("serviceCheck", "passing")],
        };
        let g = append_target_labels(&sn, "web", ",", "job/consul/dc1/web".into());
        let l = &g.labels;
        assert_eq!(l["__meta_consul_address"], "9.9.9.9");
        assert_eq!(l["__meta_consul_dc"], "dc1");
        assert_eq!(l["__meta_consul_health"], "passing");
        assert_eq!(l["__meta_consul_namespace"], "ns");
        assert_eq!(l["__meta_consul_partition"], "part");
        assert_eq!(l["__meta_consul_node"], "node-a");
        assert_eq!(l["__meta_consul_service"], "web");
        assert_eq!(l["__meta_consul_service_address"], "1.2.3.4");
        assert_eq!(l["__meta_consul_service_id"], "web-1");
        assert_eq!(l["__meta_consul_service_port"], "8080");
        // AddTagsToLabels: separator-wrapped joined list + per-tag labels.
        assert_eq!(l["__meta_consul_tags"], ",prod,team=infra,");
        assert_eq!(l["__meta_consul_tag_prod"], "");
        assert_eq!(l["__meta_consul_tagpresent_prod"], "true");
        assert_eq!(l["__meta_consul_tag_team"], "infra");
        assert_eq!(l["__meta_consul_tagpresent_team"], "true");
        // Node/service meta + tagged address, sanitized.
        assert_eq!(l["__meta_consul_metadata_rack"], "r1");
        assert_eq!(l["__meta_consul_service_metadata_version"], "1");
        assert_eq!(l["__meta_consul_tagged_address_lan"], "10.0.0.1");
        // __address__ is the target, not a label.
        assert!(!l.contains_key("__address__"));
        assert_eq!(g.source, "job/consul/dc1/web");
    }

    #[test]
    fn metadata_label_names_are_sanitized() {
        let sn = ServiceNode {
            node: Node {
                meta: [("app.kubernetes.io/name".to_string(), "web".to_string())].into(),
                ..Node::default()
            },
            ..ServiceNode::default()
        };
        let g = append_target_labels(&sn, "web", ",", "src".into());
        assert_eq!(
            g.labels["__meta_consul_metadata_app_kubernetes_io_name"],
            "web"
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
        // Unknown status string -> "".
        assert_eq!(aggregated_status(&[check("c", "bogus")]), "");
        // No checks -> passing (upstream's default arm).
        assert_eq!(aggregated_status(&[]), "passing");
    }
}
