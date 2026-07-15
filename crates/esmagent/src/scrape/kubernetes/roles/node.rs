//! `node` role target-group builder.
//!
//! Ports upstream vmagent's `getTargetLabels`/`getNodeAddr`
//! (`lib/promscrape/discoveryutils/kubernetes/node.go`).

use super::join_host_port;
use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::{register_labels_and_annotations, sanitize_label_name};
use crate::scrape::kubernetes::object::Node;
use crate::scrape::kubernetes::registry::BuildCtx;

/// Address types tried in order to pick the node's address, matching
/// upstream `getNodeAddr`. The first type with a non-empty address wins.
const ADDRESS_TYPE_PRIORITY: [&str; 6] = [
    "InternalIP",
    "InternalDNS",
    "ExternalIP",
    "ExternalDNS",
    "LegacyHostIP",
    "Hostname",
];

/// Builds the 0-or-1 [`TargetGroup`]s for a single node.
///
/// A node with no usable address (none of the priority address types
/// present) is skipped entirely — no group is emitted for it.
///
/// `_ctx` is accepted only for signature uniformity across role builders —
/// the upstream `node` role has no `attach_metadata` joins.
pub fn node_target_groups(n: &Node, _ctx: &BuildCtx) -> Vec<TargetGroup> {
    let Some(addr) = pick_node_addr(n) else {
        return Vec::new();
    };

    let kubelet_port = n.status.daemon_endpoints.kubelet_endpoint.port;
    let target = join_host_port(&addr, kubelet_port);

    let mut labels = std::collections::BTreeMap::new();
    labels.insert("instance".to_string(), n.metadata.name.clone());
    labels.insert(
        "__meta_kubernetes_node_name".to_string(),
        n.metadata.name.clone(),
    );
    labels.insert(
        "__meta_kubernetes_node_provider_id".to_string(),
        n.spec.provider_id.clone(),
    );

    let mut seen_types = std::collections::BTreeSet::new();
    for a in &n.status.addresses {
        if seen_types.insert(a.address_type.clone()) {
            labels.insert(
                sanitize_label_name(&format!(
                    "__meta_kubernetes_node_address_{}",
                    a.address_type
                )),
                a.address.clone(),
            );
        }
    }

    register_labels_and_annotations("__meta_kubernetes_node", &n.metadata, &mut labels);

    let source = format!(
        "kubernetes_sd/node/{}/{}",
        n.metadata.namespace, n.metadata.name
    );

    vec![TargetGroup {
        targets: vec![target],
        labels,
        source,
    }]
}

/// Picks the node's address by trying [`ADDRESS_TYPE_PRIORITY`] in order and
/// returning the first non-empty address found for that type.
fn pick_node_addr(n: &Node) -> Option<String> {
    for want_type in ADDRESS_TYPE_PRIORITY {
        for a in &n.status.addresses {
            if a.address_type == want_type && !a.address.is_empty() {
                return Some(a.address.clone());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use crate::scrape::kubernetes::registry::BuildCtx;

    #[test]
    fn node_target_group_addresses_and_labels() {
        let j = br#"{"metadata":{"name":"n1","labels":{"kubernetes.io/hostname":"n1"}},
        "spec":{"providerID":"aws:///i-123"},
        "status":{"addresses":[{"type":"InternalIP","address":"10.0.0.5"},
                               {"type":"Hostname","address":"n1.local"}],
                  "daemonEndpoints":{"kubeletEndpoint":{"port":10250}}}}"#;
        let (objs, rv) = crate::scrape::kubernetes::object::parse_list(
            "node",
            format!(
                "{{\"metadata\":{{\"resourceVersion\":\"7\"}},\"items\":[{}]}}",
                std::str::from_utf8(j).unwrap()
            )
            .as_bytes(),
        )
        .unwrap();
        assert_eq!(rv, "7");
        let g = objs[0].target_groups(&BuildCtx::detached());
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].targets, vec!["10.0.0.5:10250".to_string()]);
        assert_eq!(g[0].labels["instance"], "n1");
        assert_eq!(g[0].labels["__meta_kubernetes_node_name"], "n1");
        assert_eq!(
            g[0].labels["__meta_kubernetes_node_provider_id"],
            "aws:///i-123"
        );
        assert_eq!(
            g[0].labels["__meta_kubernetes_node_address_InternalIP"],
            "10.0.0.5"
        );
        assert_eq!(
            g[0].labels["__meta_kubernetes_node_address_Hostname"],
            "n1.local"
        );
        assert_eq!(
            g[0].labels["__meta_kubernetes_node_label_kubernetes_io_hostname"],
            "n1"
        );
        assert!(!g[0].labels.contains_key("__address__")); // address is the target, not a label
    }

    #[test]
    fn node_with_only_legacy_host_ip_uses_it_as_the_address() {
        // LegacyHostIP is a deprecated node address type, but upstream
        // `getNodeAddr` still tries it (between ExternalDNS and Hostname)
        // before falling back to Hostname.
        let j = br#"{"metadata":{"name":"n3"},
        "status":{"addresses":[{"type":"LegacyHostIP","address":"192.168.1.1"}],
                  "daemonEndpoints":{"kubeletEndpoint":{"port":10250}}}}"#;
        let (objs, _) = crate::scrape::kubernetes::object::parse_list(
            "node",
            format!("{{\"items\":[{}]}}", std::str::from_utf8(j).unwrap()).as_bytes(),
        )
        .unwrap();
        let g = objs[0].target_groups(&BuildCtx::detached());
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].targets, vec!["192.168.1.1:10250".to_string()]);
        assert_eq!(
            g[0].labels["__meta_kubernetes_node_address_LegacyHostIP"],
            "192.168.1.1"
        );
    }

    #[test]
    fn node_without_address_is_skipped() {
        let (objs, _) = crate::scrape::kubernetes::object::parse_list(
            "node",
            br#"{"items":[{"metadata":{"name":"n2"},"status":{}}]}"#,
        )
        .unwrap();
        assert!(objs[0].target_groups(&BuildCtx::detached()).is_empty());
    }
}
