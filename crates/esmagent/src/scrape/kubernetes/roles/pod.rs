//! `pod` role target-group builder.
//!
//! Ports upstream vmagent's `getTargetLabels`/`appendPodLabels`/
//! `appendPodLabelsInternal`/`appendCommonLabels`/`appendContainerLabels`
//! (`lib/promscrape/discoveryutils/kubernetes/pod.go`).

use std::collections::BTreeMap;

use super::join_host_port;
use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::register_labels_and_annotations;
use crate::scrape::kubernetes::object::{
    Container, ContainerPort, ContainerStatus, K8sObject, Pod,
};
use crate::scrape::kubernetes::registry::BuildCtx;

/// Builds the [`TargetGroup`]s for a single pod: one per
/// (container, port) pair, or one portless group per container with no
/// declared ports.
///
/// The whole pod is skipped (empty `Vec`) if it has no pod IP yet, or if
/// it has already finished running (`phase` `Succeeded`/`Failed`).
pub fn pod_target_groups(p: &Pod, ctx: &BuildCtx) -> Vec<TargetGroup> {
    if p.status.pod_ip.is_empty() || matches!(p.status.phase.as_str(), "Succeeded" | "Failed") {
        return Vec::new();
    }

    let mut groups = Vec::new();
    for (containers, statuses, is_init) in [
        (&p.spec.containers, &p.status.container_statuses, false),
        (
            &p.spec.init_containers,
            &p.status.init_container_statuses,
            true,
        ),
    ] {
        for container in containers {
            append_container_groups(p, ctx, container, statuses, is_init, &mut groups);
        }
    }
    groups
}

/// Appends the target group(s) for one container, skipping it entirely if
/// its matching status reports it as already terminated.
fn append_container_groups(
    p: &Pod,
    ctx: &BuildCtx,
    container: &Container,
    statuses: &[ContainerStatus],
    is_init: bool,
    groups: &mut Vec<TargetGroup>,
) {
    let status = statuses.iter().find(|s| s.name == container.name);
    if status.is_some_and(|s| s.state.terminated.is_some()) {
        return;
    }
    let container_id = status.map(|s| s.container_id.as_str()).unwrap_or("");

    if container.ports.is_empty() {
        groups.push(build_target_group(
            p,
            ctx,
            container,
            None,
            is_init,
            container_id,
        ));
        return;
    }
    for port in &container.ports {
        groups.push(build_target_group(
            p,
            ctx,
            container,
            Some(port),
            is_init,
            container_id,
        ));
    }
}

/// Builds a single [`TargetGroup`] for one (container, port-or-none) pair.
fn build_target_group(
    p: &Pod,
    ctx: &BuildCtx,
    container: &Container,
    port: Option<&ContainerPort>,
    is_init: bool,
    container_id: &str,
) -> TargetGroup {
    let target = match port {
        Some(cp) => join_host_port(&p.status.pod_ip, cp.container_port),
        None => escape_ipv6(&p.status.pod_ip),
    };

    let mut labels = BTreeMap::new();
    // Upstream `appendPodLabelsInternal` only adds the container ID label when
    // it's non-empty (a container with no matching ContainerStatus yet, e.g.
    // ContainerCreating, has no ID and gets no label rather than an empty one).
    if !container_id.is_empty() {
        labels.insert(
            "__meta_kubernetes_pod_container_id".to_string(),
            container_id.to_string(),
        );
    }
    append_common_labels(p, ctx, &mut labels);
    append_container_labels(container, port, is_init, &mut labels);

    let source = format!(
        "kubernetes_sd/pod/{}/{}",
        p.metadata.namespace, p.metadata.name
    );

    TargetGroup {
        targets: vec![target],
        labels,
        source,
    }
}

/// Ports upstream `Pod.appendCommonLabels`: the pod's own common labels plus
/// the `attach_metadata` node/namespace joins that precede them (upstream
/// call order — the key sets don't collide, so order only mirrors upstream).
pub(crate) fn append_common_labels(p: &Pod, ctx: &BuildCtx, labels: &mut BTreeMap<String, String>) {
    if ctx.attach_node_metadata {
        labels.insert(
            "__meta_kubernetes_node_name".to_string(),
            p.spec.node_name.clone(),
        );
        if let Some(o) = ctx
            .registry
            .get_object("node", &p.metadata.namespace, &p.spec.node_name)
        {
            if let K8sObject::Node(n) = &*o {
                register_labels_and_annotations("__meta_kubernetes_node", &n.metadata, labels);
            }
        }
    }
    if ctx.attach_namespace_metadata {
        if let Some(o) = ctx
            .registry
            .get_object("namespace", "", &p.metadata.namespace)
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

    labels.insert(
        "__meta_kubernetes_namespace".to_string(),
        p.metadata.namespace.clone(),
    );
    labels.insert(
        "__meta_kubernetes_pod_name".to_string(),
        p.metadata.name.clone(),
    );
    labels.insert(
        "__meta_kubernetes_pod_ip".to_string(),
        p.status.pod_ip.clone(),
    );
    labels.insert(
        "__meta_kubernetes_pod_ready".to_string(),
        get_pod_ready_status(p),
    );
    labels.insert(
        "__meta_kubernetes_pod_phase".to_string(),
        p.status.phase.clone(),
    );
    labels.insert(
        "__meta_kubernetes_pod_node_name".to_string(),
        p.spec.node_name.clone(),
    );
    labels.insert(
        "__meta_kubernetes_pod_host_ip".to_string(),
        p.status.host_ip.clone(),
    );
    labels.insert(
        "__meta_kubernetes_pod_uid".to_string(),
        p.metadata.uid.clone(),
    );

    // Upstream `appendCommonLabels` gates each controller label independently
    // on its value being non-empty.
    if let Some(owner) = p.metadata.owner_references.iter().find(|o| o.controller) {
        if !owner.kind.is_empty() {
            labels.insert(
                "__meta_kubernetes_pod_controller_kind".to_string(),
                owner.kind.clone(),
            );
        }
        if !owner.name.is_empty() {
            labels.insert(
                "__meta_kubernetes_pod_controller_name".to_string(),
                owner.name.clone(),
            );
        }
    }

    register_labels_and_annotations("__meta_kubernetes_pod", &p.metadata, labels);
}

/// Ports upstream `Pod.appendContainerLabels`: the per-container image/name/
/// init labels plus the port name/number/protocol block when a port is given.
pub(crate) fn append_container_labels(
    container: &Container,
    port: Option<&ContainerPort>,
    is_init: bool,
    labels: &mut BTreeMap<String, String>,
) {
    labels.insert(
        "__meta_kubernetes_pod_container_image".to_string(),
        container.image.clone(),
    );
    labels.insert(
        "__meta_kubernetes_pod_container_name".to_string(),
        container.name.clone(),
    );
    labels.insert(
        "__meta_kubernetes_pod_container_init".to_string(),
        is_init.to_string(),
    );

    if let Some(cp) = port {
        labels.insert(
            "__meta_kubernetes_pod_container_port_name".to_string(),
            cp.name.clone(),
        );
        labels.insert(
            "__meta_kubernetes_pod_container_port_number".to_string(),
            cp.container_port.to_string(),
        );
        labels.insert(
            "__meta_kubernetes_pod_container_port_protocol".to_string(),
            cp.protocol.clone(),
        );
    }
}

/// Finds the `Ready` condition and lowercases its `status`
/// (`"True"`/`"False"`/`"Unknown"` -> `"true"`/`"false"`/`"unknown"`).
/// Returns `"unknown"` if no `Ready` condition is present.
fn get_pod_ready_status(p: &Pod) -> String {
    p.status
        .conditions
        .iter()
        .find(|c| c.condition_type == "Ready")
        .map(|c| c.status.to_lowercase())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Wraps an IPv6 host in `[...]` for use as a bare (portless) target,
/// matching the bracketing half of [`join_host_port`]'s IPv6 handling.
fn escape_ipv6(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::scrape::kubernetes::registry::BuildCtx;

    #[test]
    fn pod_fans_out_ports_and_skips_finished() {
        let running = br#"{"metadata":{"name":"web","namespace":"prod","uid":"u1",
            "ownerReferences":[{"name":"web-rs","kind":"ReplicaSet","controller":true}]},
          "spec":{"nodeName":"n1","containers":[{"name":"c","image":"img:1",
            "ports":[{"name":"http","containerPort":8080,"protocol":"TCP"},
                     {"name":"metrics","containerPort":9100,"protocol":"TCP"}]}]},
          "status":{"phase":"Running","podIP":"10.1.2.3","hostIP":"10.0.0.5",
            "conditions":[{"type":"Ready","status":"True"}]}}"#;
        let (objs, _) = crate::scrape::kubernetes::object::parse_list(
            "pod",
            format!("{{\"items\":[{}]}}", std::str::from_utf8(running).unwrap()).as_bytes(),
        )
        .unwrap();
        let g = objs[0].target_groups(&BuildCtx::detached());
        assert_eq!(g.len(), 2); // one per container port
        assert_eq!(g[0].targets, vec!["10.1.2.3:8080".to_string()]);
        assert_eq!(g[0].labels["__meta_kubernetes_namespace"], "prod");
        assert_eq!(g[0].labels["__meta_kubernetes_pod_ready"], "true");
        // Value-asserts the `hostIP` rename: a `hostIP`->`hostIp` regression
        // would leave this empty and fail here.
        assert_eq!(g[0].labels["__meta_kubernetes_pod_host_ip"], "10.0.0.5");
        assert_eq!(
            g[0].labels["__meta_kubernetes_pod_container_port_number"],
            "8080"
        );
        assert_eq!(
            g[0].labels["__meta_kubernetes_pod_controller_kind"],
            "ReplicaSet"
        );
        assert_eq!(g[0].labels["__meta_kubernetes_pod_container_init"], "false");

        let done = br#"{"items":[{"metadata":{"name":"job","namespace":"prod"},
            "status":{"phase":"Succeeded","podIP":"10.1.2.9"}}]}"#;
        let (objs2, _) = crate::scrape::kubernetes::object::parse_list("pod", done).unwrap();
        assert!(objs2[0].target_groups(&BuildCtx::detached()).is_empty());
    }

    #[test]
    fn pod_portless_container_yields_one_target() {
        let j = br#"{"items":[{"metadata":{"name":"p","namespace":"d"},
            "spec":{"containers":[{"name":"c","image":"i"}]},
            "status":{"phase":"Running","podIP":"10.1.2.3"}}]}"#;
        let (objs, _) = crate::scrape::kubernetes::object::parse_list("pod", j).unwrap();
        let g = objs[0].target_groups(&BuildCtx::detached());
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].targets, vec!["10.1.2.3".to_string()]);
    }

    #[test]
    fn pod_skips_terminated_container_but_keeps_running_one() {
        let j = br#"{"items":[{"metadata":{"name":"p","namespace":"d"},
            "spec":{"containers":[
                {"name":"done","image":"i1"},
                {"name":"live","image":"i2"}]},
            "status":{"phase":"Running","podIP":"10.1.2.3",
                "containerStatuses":[
                    {"name":"done","containerID":"docker://a","state":{"terminated":{"exitCode":0}}},
                    {"name":"live","containerID":"docker://b","state":{}}]}}]}"#;
        let (objs, _) = crate::scrape::kubernetes::object::parse_list("pod", j).unwrap();
        let g = objs[0].target_groups(&BuildCtx::detached());
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].labels["__meta_kubernetes_pod_container_name"], "live");
        assert_eq!(
            g[0].labels["__meta_kubernetes_pod_container_id"],
            "docker://b"
        );
    }

    #[test]
    fn pod_container_without_matching_status_omits_container_id() {
        // A container that has no entry in containerStatuses yet (e.g. still
        // ContainerCreating) must NOT get an empty container_id label.
        let j = br#"{"items":[{"metadata":{"name":"p","namespace":"d"},
            "spec":{"containers":[{"name":"c","image":"i"}]},
            "status":{"phase":"Running","podIP":"10.1.2.3"}}]}"#;
        let (objs, _) = crate::scrape::kubernetes::object::parse_list("pod", j).unwrap();
        let g = objs[0].target_groups(&BuildCtx::detached());
        assert_eq!(g.len(), 1);
        assert!(!g[0]
            .labels
            .contains_key("__meta_kubernetes_pod_container_id"));
    }

    #[test]
    fn pod_init_container_is_flagged_and_fanned_out_separately() {
        let j = br#"{"items":[{"metadata":{"name":"p","namespace":"d"},
            "spec":{
                "containers":[{"name":"main","image":"i1"}],
                "initContainers":[{"name":"setup","image":"i2"}]},
            "status":{"phase":"Running","podIP":"10.1.2.3",
                "initContainerStatuses":[
                    {"name":"setup","containerID":"docker://s","state":{}}]}}]}"#;
        let (objs, _) = crate::scrape::kubernetes::object::parse_list("pod", j).unwrap();
        let g = objs[0].target_groups(&BuildCtx::detached());
        assert_eq!(g.len(), 2);
        let main = g
            .iter()
            .find(|tg| tg.labels["__meta_kubernetes_pod_container_name"] == "main")
            .unwrap();
        assert_eq!(main.labels["__meta_kubernetes_pod_container_init"], "false");
        let init = g
            .iter()
            .find(|tg| tg.labels["__meta_kubernetes_pod_container_name"] == "setup")
            .unwrap();
        assert_eq!(init.labels["__meta_kubernetes_pod_container_init"], "true");
        assert_eq!(
            init.labels["__meta_kubernetes_pod_container_id"],
            "docker://s"
        );
    }

    #[test]
    fn pod_ready_status_is_unknown_without_ready_condition() {
        let j = br#"{"items":[{"metadata":{"name":"p","namespace":"d"},
            "spec":{"containers":[{"name":"c","image":"i"}]},
            "status":{"phase":"Pending","podIP":"10.1.2.3"}}]}"#;
        let (objs, _) = crate::scrape::kubernetes::object::parse_list("pod", j).unwrap();
        let g = objs[0].target_groups(&BuildCtx::detached());
        assert_eq!(g[0].labels["__meta_kubernetes_pod_ready"], "unknown");
    }

    #[test]
    fn pod_without_controller_omits_controller_labels() {
        let j = br#"{"items":[{"metadata":{"name":"p","namespace":"d"},
            "spec":{"containers":[{"name":"c","image":"i"}]},
            "status":{"phase":"Running","podIP":"10.1.2.3"}}]}"#;
        let (objs, _) = crate::scrape::kubernetes::object::parse_list("pod", j).unwrap();
        let g = objs[0].target_groups(&BuildCtx::detached());
        assert!(!g[0]
            .labels
            .contains_key("__meta_kubernetes_pod_controller_kind"));
        assert!(!g[0]
            .labels
            .contains_key("__meta_kubernetes_pod_controller_name"));
        assert!(!g[0].labels.contains_key("__address__"));
    }

    #[test]
    fn pod_attach_metadata_joins_node_and_namespace_labels() {
        use crate::scrape::kubernetes::registry::{BuildCtx, ObjectRegistry};
        let (pods, _) = crate::scrape::kubernetes::object::parse_list(
            "pod",
            br#"{"items":[{"metadata":{"name":"p","namespace":"d"},
                "spec":{"nodeName":"n1","containers":[{"name":"c","image":"i"}]},
                "status":{"phase":"Running","podIP":"10.1.2.3"}}]}"#,
        )
        .unwrap();
        let mut reg = ObjectRegistry::default();
        // node n1 with a label; namespace d with an annotation
        let (nodes, _) = crate::scrape::kubernetes::object::parse_list(
            "node",
            br#"{"items":[{"metadata":{"name":"n1","labels":{"zone":"z1"}}}]}"#,
        )
        .unwrap();
        let (nss, _) = crate::scrape::kubernetes::object::parse_list(
            "namespace",
            br#"{"items":[{"metadata":{"name":"d","annotations":{"owner":"t"}}}]}"#,
        )
        .unwrap();
        let mk = |objs: Vec<crate::scrape::kubernetes::object::K8sObject>| {
            let mut m = std::collections::HashMap::new();
            for o in objs {
                m.insert(o.key(), std::sync::Arc::new(o));
            }
            std::sync::Arc::new(std::sync::Mutex::new(m))
        };
        reg.register("node", None, mk(nodes));
        reg.register("namespace", None, mk(nss));
        let ctx = BuildCtx {
            registry: &reg,
            attach_node_metadata: true,
            attach_namespace_metadata: true,
        };
        let g = pods[0].target_groups(&ctx);
        assert_eq!(g[0].labels["__meta_kubernetes_node_name"], "n1");
        assert_eq!(g[0].labels["__meta_kubernetes_node_label_zone"], "z1");
        assert_eq!(
            g[0].labels["__meta_kubernetes_node_labelpresent_zone"],
            "true"
        );
        assert_eq!(
            g[0].labels["__meta_kubernetes_namespace_annotation_owner"],
            "t"
        );
        // without the flags, none of these labels appear
        let g2 = pods[0].target_groups(&BuildCtx::detached());
        assert!(!g2[0]
            .labels
            .contains_key("__meta_kubernetes_node_label_zone"));
        assert!(!g2[0].labels.contains_key("__meta_kubernetes_node_name"));
    }
}
