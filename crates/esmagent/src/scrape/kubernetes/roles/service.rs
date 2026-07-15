//! `service` role target-group builder.
//!
//! Ports upstream vmagent's `getTargetLabels`
//! (`lib/promscrape/discoveryutils/kubernetes/service.go`).

use std::collections::BTreeMap;

use super::join_host_port;
use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::register_labels_and_annotations;
use crate::scrape::kubernetes::object::{K8sObject, Service, ServicePort};
use crate::scrape::kubernetes::registry::BuildCtx;

/// Builds the [`TargetGroup`]s for a single service: one per declared port.
///
/// A service with no ports yields no groups (there is nothing to scrape).
pub fn service_target_groups(s: &Service, ctx: &BuildCtx) -> Vec<TargetGroup> {
    s.spec
        .ports
        .iter()
        .map(|port| build_target_group(s, ctx, port))
        .collect()
}

/// Builds a single [`TargetGroup`] for one (service, port) pair.
fn build_target_group(s: &Service, ctx: &BuildCtx, port: &ServicePort) -> TargetGroup {
    let host = format!("{}.{}.svc", s.metadata.name, s.metadata.namespace);
    let target = join_host_port(&host, port.port);

    let mut labels = BTreeMap::new();
    labels.insert(
        "__meta_kubernetes_service_port_name".to_string(),
        port.name.clone(),
    );
    labels.insert(
        "__meta_kubernetes_service_port_number".to_string(),
        port.port.to_string(),
    );
    labels.insert(
        "__meta_kubernetes_service_port_protocol".to_string(),
        port.protocol.clone(),
    );

    append_common_labels(s, ctx, &mut labels);

    let source = format!(
        "kubernetes_sd/service/{}/{}",
        s.metadata.namespace, s.metadata.name
    );

    TargetGroup {
        targets: vec![target],
        labels,
        source,
    }
}

/// Ports upstream `Service.appendCommonLabels`: the service's own common
/// labels plus the `attach_metadata` namespace join (upstream inserts the
/// namespace join just before the service's own label/annotation block).
pub(crate) fn append_common_labels(
    s: &Service,
    ctx: &BuildCtx,
    labels: &mut BTreeMap<String, String>,
) {
    labels.insert(
        "__meta_kubernetes_namespace".to_string(),
        s.metadata.namespace.clone(),
    );
    labels.insert(
        "__meta_kubernetes_service_name".to_string(),
        s.metadata.name.clone(),
    );
    labels.insert(
        "__meta_kubernetes_service_type".to_string(),
        s.spec.type_.clone(),
    );
    if s.spec.type_ == "ExternalName" {
        labels.insert(
            "__meta_kubernetes_service_external_name".to_string(),
            s.spec.external_name.clone(),
        );
    } else {
        labels.insert(
            "__meta_kubernetes_service_cluster_ip".to_string(),
            s.spec.cluster_ip.clone(),
        );
    }

    if ctx.attach_namespace_metadata {
        if let Some(o) = ctx
            .registry
            .get_object("namespace", "", &s.metadata.namespace)
        {
            if let K8sObject::Namespace(ns) = &*o {
                register_labels_and_annotations(
                    "__meta_kubernetes_namespace",
                    &ns.metadata,
                    labels,
                );
            }
        }
    }

    register_labels_and_annotations("__meta_kubernetes_service", &s.metadata, labels);
}

#[cfg(test)]
mod tests {
    use crate::scrape::kubernetes::registry::BuildCtx;

    #[test]
    fn service_fans_out_ports_with_cluster_ip() {
        let j = br#"{"items":[{"metadata":{"name":"api","namespace":"prod"},
            "spec":{"type":"ClusterIP","clusterIP":"10.96.0.1",
              "ports":[{"name":"http","protocol":"TCP","port":80}]}}]}"#;
        let (objs, _) = crate::scrape::kubernetes::object::parse_list("service", j).unwrap();
        let g = objs[0].target_groups(&BuildCtx::detached());
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].targets, vec!["api.prod.svc:80".to_string()]);
        assert_eq!(g[0].labels["__meta_kubernetes_service_name"], "api");
        assert_eq!(
            g[0].labels["__meta_kubernetes_service_cluster_ip"],
            "10.96.0.1"
        );
        assert_eq!(g[0].labels["__meta_kubernetes_service_port_number"], "80");
        assert!(!g[0]
            .labels
            .contains_key("__meta_kubernetes_service_external_name"));
    }

    #[test]
    fn external_name_service_uses_external_name_label() {
        let j = br#"{"items":[{"metadata":{"name":"ext","namespace":"d"},
            "spec":{"type":"ExternalName","externalName":"example.com",
              "ports":[{"name":"h","protocol":"TCP","port":443}]}}]}"#;
        let (objs, _) = crate::scrape::kubernetes::object::parse_list("service", j).unwrap();
        let g = objs[0].target_groups(&BuildCtx::detached());
        assert_eq!(
            g[0].labels["__meta_kubernetes_service_external_name"],
            "example.com"
        );
        assert!(!g[0]
            .labels
            .contains_key("__meta_kubernetes_service_cluster_ip"));
    }

    #[test]
    fn service_attach_namespace_metadata_joins_namespace_labels() {
        use crate::scrape::kubernetes::registry::ObjectRegistry;
        let (svcs, _) = crate::scrape::kubernetes::object::parse_list(
            "service",
            br#"{"items":[{"metadata":{"name":"api","namespace":"d"},
                "spec":{"type":"ClusterIP","clusterIP":"10.96.0.1",
                  "ports":[{"name":"http","protocol":"TCP","port":80}]}}]}"#,
        )
        .unwrap();
        let (nss, _) = crate::scrape::kubernetes::object::parse_list(
            "namespace",
            br#"{"items":[{"metadata":{"name":"d","labels":{"team":"a"}}}]}"#,
        )
        .unwrap();
        let mut m = std::collections::HashMap::new();
        for o in nss {
            m.insert(o.key(), std::sync::Arc::new(o));
        }
        let mut reg = ObjectRegistry::default();
        reg.register(
            "namespace",
            None,
            std::sync::Arc::new(std::sync::Mutex::new(m)),
        );
        let ctx = BuildCtx {
            registry: &reg,
            attach_node_metadata: false,
            attach_namespace_metadata: true,
        };
        let g = svcs[0].target_groups(&ctx);
        assert_eq!(g[0].labels["__meta_kubernetes_namespace_label_team"], "a");
        // without the flag the joined label is absent
        let g2 = svcs[0].target_groups(&BuildCtx::detached());
        assert!(!g2[0]
            .labels
            .contains_key("__meta_kubernetes_namespace_label_team"));
    }
}
