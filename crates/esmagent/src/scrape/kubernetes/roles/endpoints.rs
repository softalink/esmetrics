//! `endpoints` role target-group builder.
//!
//! Ports upstream vmagent's `Endpoints.getTargetLabels`/
//! `appendEndpointLabelsForAddresses`/`getEndpointLabelsForAddressAndPort`/
//! `getEndpointLabels` (`lib/promscrape/discovery/kubernetes/endpoints.go`)
//! plus `Pod.appendEndpointLabels` (`pod.go`).
//!
//! The `endpoints` role joins across three kinds via the shared
//! [`ObjectRegistry`](crate::scrape::kubernetes::registry::ObjectRegistry):
//! the `Endpoints` object itself, the `Service` that shares its name, and the
//! `Pod` referenced by each address's `targetRef`. Upstream stores the
//! `__address__` inside its label map; this port keeps the address in
//! [`TargetGroup::targets`] instead (matching the other role builders), so the
//! label maps here never carry an `__address__` entry.
//!
//! Upstream builds labels in an ordered list and calls `RemoveDuplicates`
//! (keep-last) at the end of each group. This port accumulates into a
//! `BTreeMap`, whose `insert` is likewise keep-last, so replicating upstream's
//! append *order* reproduces upstream's final label set exactly.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::join_host_port;
use super::pod::{append_common_labels as append_pod_common_labels, append_container_labels};
use super::service::append_common_labels as append_service_common_labels;
use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::register_labels_and_annotations;
use crate::scrape::kubernetes::object::{
    Container, EndpointAddress, EndpointPort, Endpoints, K8sObject, ObjectMeta, Pod, Service,
};
use crate::scrape::kubernetes::registry::BuildCtx;

/// The shared, immutable context for one `Endpoints` build: the surrounding
/// [`BuildCtx`], the object being built, the joined `Service` (if any), and
/// the common `source` string. Bundling these keeps the recursive helper
/// signatures small.
struct EndpointsCtx<'a> {
    build: &'a BuildCtx<'a>,
    eps: &'a Endpoints,
    svc: Option<&'a Service>,
    source: &'a str,
}

/// Per-pod accumulator: the pod object plus the container ports already
/// emitted through a matching endpoint port. Keyed by pod key in a
/// [`BTreeMap`] so the skipped-ports fanout is deterministic (upstream keys by
/// pod pointer, whose map order is nondeterministic).
type PodPortsSeen = BTreeMap<String, (Arc<K8sObject>, Vec<i64>)>;

/// Builds the [`TargetGroup`]s for a single `Endpoints` object: one per
/// subset address × port (ready then not-ready), followed by one extra group
/// per skipped container port on every pod referenced by a `targetRef`.
pub fn endpoints_target_groups(eps: &Endpoints, ctx: &BuildCtx) -> Vec<TargetGroup> {
    let source = format!(
        "kubernetes_sd/endpoints/{}/{}",
        eps.metadata.namespace, eps.metadata.name
    );

    // Endpoints and their service share a name, so the service lookup keys on
    // the endpoints' own namespace/name.
    let svc_obj = ctx
        .registry
        .get_object("service", &eps.metadata.namespace, &eps.metadata.name);
    let svc = svc_obj.as_deref().and_then(|o| match o {
        K8sObject::Service(s) => Some(s),
        _ => None,
    });

    let ec = EndpointsCtx {
        build: ctx,
        eps,
        svc,
        source: &source,
    };

    let mut pod_ports_seen: PodPortsSeen = BTreeMap::new();
    let mut groups = Vec::new();

    for ess in &eps.subsets {
        for epp in &ess.ports {
            append_endpoint_labels_for_addresses(
                &ec,
                &mut groups,
                &mut pod_ports_seen,
                &ess.addresses,
                epp,
                "true",
            );
            append_endpoint_labels_for_addresses(
                &ec,
                &mut groups,
                &mut pod_ports_seen,
                &ess.not_ready_addresses,
                epp,
                "false",
            );
        }
    }

    // See https://kubernetes.io/docs/reference/labels-annotations-taints/#endpoints-kubernetes-io-over-capacity
    match eps
        .metadata
        .annotations
        .get("endpoints.kubernetes.io/over-capacity")
        .map(String::as_str)
    {
        Some("truncated") => log::warn!(
            "the number of targets for \"role: endpoints\" {:?} exceeds 1000 and has been truncated; please use \"role: endpointslice\" instead",
            eps.metadata.key()
        ),
        Some("warning") => log::warn!(
            "the number of targets for \"role: endpoints\" {:?} exceeds 1000 and will be truncated in the next k8s releases; please use \"role: endpointslice\" instead",
            eps.metadata.key()
        ),
        _ => {}
    }

    // Append labels for skipped container ports on every seen pod.
    for (pod_arc, seen) in pod_ports_seen.values() {
        let K8sObject::Pod(p) = &**pod_arc else {
            continue;
        };
        for c in &p.spec.containers {
            append_pod_metadata(&ec, &mut groups, p, c, seen, false);
        }
        for c in &p.spec.init_containers {
            // Native sidecars only: a plain init container is never scraped.
            // https://kubernetes.io/blog/2023/08/25/native-sidecar-containers/
            if c.restart_policy != "Always" {
                continue;
            }
            append_pod_metadata(&ec, &mut groups, p, c, seen, true);
        }
    }

    groups
}

/// Ports `appendEndpointLabelsForAddresses`: emits one group per address in
/// `eas`, resolving the address's `targetRef` pod through the registry.
fn append_endpoint_labels_for_addresses(
    ec: &EndpointsCtx,
    groups: &mut Vec<TargetGroup>,
    pod_ports_seen: &mut PodPortsSeen,
    eas: &[EndpointAddress],
    epp: &EndpointPort,
    ready: &str,
) {
    for ea in eas {
        let pod_arc = if ea.target_ref.name.is_empty() {
            None
        } else {
            ec.build
                .registry
                .get_object("pod", &ea.target_ref.namespace, &ea.target_ref.name)
        };
        let labels =
            get_endpoint_labels_for_address_and_port(ec, pod_ports_seen, ea, epp, pod_arc, ready);
        groups.push(TargetGroup {
            targets: vec![join_host_port(&ea.ip, epp.port)],
            labels,
            source: ec.source.to_string(),
        });
    }
}

/// Ports `getEndpointLabelsForAddressAndPort`: the base endpoint labels, the
/// service join, the endpoints meta, and (for a `Pod` target) the pod common
/// labels plus the matching container-port labels. Records the pod and its
/// matched ports in `pod_ports_seen` for the later skipped-ports fanout.
fn get_endpoint_labels_for_address_and_port(
    ec: &EndpointsCtx,
    pod_ports_seen: &mut PodPortsSeen,
    ea: &EndpointAddress,
    epp: &EndpointPort,
    pod_arc: Option<Arc<K8sObject>>,
    ready: &str,
) -> BTreeMap<String, String> {
    let mut labels = get_endpoint_labels(&ec.eps.metadata, ea, epp, ready);
    if let Some(s) = ec.svc {
        append_service_common_labels(s, ec.build, &mut labels);
    }
    // See https://github.com/prometheus/prometheus/issues/10284
    register_labels_and_annotations("__meta_kubernetes_endpoints", &ec.eps.metadata, &mut labels);

    if ea.target_ref.kind != "Pod" {
        return labels;
    }
    let Some(pod_arc) = pod_arc else {
        return labels;
    };
    let K8sObject::Pod(p) = &*pod_arc else {
        return labels;
    };

    append_pod_common_labels(p, ec.build, &mut labels);
    // Always record the pod targetRef, even if no container port matches.
    // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/2134
    let entry = pod_ports_seen
        .entry(p.metadata.key())
        .or_insert_with(|| (Arc::clone(&pod_arc), Vec::new()));

    for c in &p.spec.containers {
        match_container_port(c, epp, false, &mut entry.1, &mut labels);
    }
    for c in &p.spec.init_containers {
        if c.restart_policy != "Always" {
            continue;
        }
        match_container_port(c, epp, true, &mut entry.1, &mut labels);
    }

    labels
}

/// Finds the first container port equal to `epp.port`; on a match records the
/// port in `seen` and appends the container-port labels. Mirrors upstream's
/// inner-loop `break` (stop at the first matching port) while the caller keeps
/// scanning later containers.
fn match_container_port(
    c: &Container,
    epp: &EndpointPort,
    is_init: bool,
    seen: &mut Vec<i64>,
    labels: &mut BTreeMap<String, String>,
) {
    for cp in &c.ports {
        if cp.container_port == epp.port {
            seen.push(cp.container_port);
            append_container_labels(c, Some(cp), is_init, labels);
            break;
        }
    }
}

/// Ports `getEndpointLabels`: the base per-address/per-port label block.
fn get_endpoint_labels(
    om: &ObjectMeta,
    ea: &EndpointAddress,
    epp: &EndpointPort,
    ready: &str,
) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert(
        "__meta_kubernetes_namespace".to_string(),
        om.namespace.clone(),
    );
    m.insert(
        "__meta_kubernetes_endpoints_name".to_string(),
        om.name.clone(),
    );
    m.insert(
        "__meta_kubernetes_endpoint_ready".to_string(),
        ready.to_string(),
    );
    m.insert(
        "__meta_kubernetes_endpoint_port_name".to_string(),
        epp.name.clone(),
    );
    m.insert(
        "__meta_kubernetes_endpoint_port_protocol".to_string(),
        epp.protocol.clone(),
    );
    if !ea.target_ref.kind.is_empty() {
        m.insert(
            "__meta_kubernetes_endpoint_address_target_kind".to_string(),
            ea.target_ref.kind.clone(),
        );
        m.insert(
            "__meta_kubernetes_endpoint_address_target_name".to_string(),
            ea.target_ref.name.clone(),
        );
    }
    if !ea.node_name.is_empty() {
        m.insert(
            "__meta_kubernetes_endpoint_node_name".to_string(),
            ea.node_name.clone(),
        );
    }
    if !ea.hostname.is_empty() {
        m.insert(
            "__meta_kubernetes_endpoint_hostname".to_string(),
            ea.hostname.clone(),
        );
    }
    m
}

/// Ports the `appendPodMetadata` closure from `getTargetLabels`: for one
/// container, emits a group for every container port that was *not* already
/// matched by an endpoint port. These carry the pod, container, endpoint and
/// service labels but no `endpoint_ready`/`endpoint_port_*` block.
fn append_pod_metadata(
    ec: &EndpointsCtx,
    groups: &mut Vec<TargetGroup>,
    p: &Pod,
    c: &Container,
    seen: &[i64],
    is_init: bool,
) {
    for cp in &c.ports {
        if seen.contains(&cp.container_port) {
            continue;
        }
        let mut labels = BTreeMap::new();
        append_pod_common_labels(p, ec.build, &mut labels);
        append_container_labels(c, Some(cp), is_init, &mut labels);
        // Prometheus sets endpoints_name/target labels for all endpoints, even
        // ports not matching a service port.
        // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/4154
        append_endpoint_labels(&mut labels, ec.eps, p);
        if let Some(s) = ec.svc {
            append_service_common_labels(s, ec.build, &mut labels);
        }
        groups.push(TargetGroup {
            targets: vec![join_host_port(&p.status.pod_ip, cp.container_port)],
            labels,
            source: ec.source.to_string(),
        });
    }
}

/// Ports `Pod.appendEndpointLabels` (`pod.go`): the pod-side endpoints
/// identity labels attached to skipped-port fanout groups.
fn append_endpoint_labels(labels: &mut BTreeMap<String, String>, eps: &Endpoints, p: &Pod) {
    labels.insert(
        "__meta_kubernetes_endpoints_name".to_string(),
        eps.metadata.name.clone(),
    );
    labels.insert(
        "__meta_kubernetes_endpoint_address_target_kind".to_string(),
        "Pod".to_string(),
    );
    labels.insert(
        "__meta_kubernetes_endpoint_address_target_name".to_string(),
        p.metadata.name.clone(),
    );
    register_labels_and_annotations("__meta_kubernetes_endpoints", &eps.metadata, labels);
}

#[cfg(test)]
mod tests {
    use crate::scrape::kubernetes::object::parse_list;
    use crate::scrape::kubernetes::registry::{BuildCtx, ObjectRegistry};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn reg_with(entries: &[(&str, &[u8])]) -> ObjectRegistry {
        let mut reg = ObjectRegistry::default();
        for (role, json) in entries {
            let (objs, _) = parse_list(role, json).unwrap();
            let mut m = HashMap::new();
            for o in objs {
                m.insert(o.key(), Arc::new(o));
            }
            reg.register(role, None, Arc::new(Mutex::new(m)));
        }
        reg
    }

    const POD_LIST: &[u8] = br#"{"items":[{"metadata":{"name":"p1","namespace":"d","uid":"u1"},
        "spec":{"nodeName":"n1","containers":[{"name":"c","image":"i",
            "ports":[{"name":"http","containerPort":8080,"protocol":"TCP"},
                     {"name":"side","containerPort":9090,"protocol":"TCP"}]}]},
        "status":{"phase":"Running","podIP":"10.0.0.1","hostIP":"10.0.0.100",
            "conditions":[{"type":"Ready","status":"True"}]}}]}"#;
    const SVC_LIST: &[u8] = br#"{"items":[{"metadata":{"name":"svc1","namespace":"d",
        "labels":{"app":"a"}},
        "spec":{"type":"ClusterIP","clusterIP":"10.96.0.1",
            "ports":[{"name":"http","protocol":"TCP","port":8080}]}}]}"#;
    const EPS_LIST: &[u8] = br#"{"items":[{"metadata":{"name":"svc1","namespace":"d"},
        "subsets":[{"addresses":[{"ip":"10.0.0.1",
                "targetRef":{"kind":"Pod","name":"p1","namespace":"d"}}],
            "notReadyAddresses":[{"ip":"10.0.0.9"}],
            "ports":[{"name":"http","port":8080,"protocol":"TCP"}]}]}]}"#;

    #[test]
    fn endpoints_join_service_and_pod_and_fan_out_skipped_ports() {
        let reg = reg_with(&[("pod", POD_LIST), ("service", SVC_LIST)]);
        let ctx = BuildCtx {
            registry: &reg,
            attach_node_metadata: false,
            attach_namespace_metadata: false,
        };
        let (objs, _) = parse_list("endpoints", EPS_LIST).unwrap();
        let g = objs[0].target_groups(&ctx);
        // 1 ready + 1 not-ready + 1 skipped-port (9090) on the seen pod
        assert_eq!(g.len(), 3);

        let ready = g
            .iter()
            .find(|x| x.targets == vec!["10.0.0.1:8080".to_string()])
            .unwrap();
        assert_eq!(ready.labels["__meta_kubernetes_endpoint_ready"], "true");
        assert_eq!(ready.labels["__meta_kubernetes_endpoints_name"], "svc1");
        // service join
        assert_eq!(ready.labels["__meta_kubernetes_service_name"], "svc1");
        assert_eq!(ready.labels["__meta_kubernetes_service_label_app"], "a");
        // pod join (targetRef) incl. matched container port labels
        assert_eq!(ready.labels["__meta_kubernetes_pod_name"], "p1");
        assert_eq!(
            ready.labels["__meta_kubernetes_pod_container_port_number"],
            "8080"
        );
        assert_eq!(
            ready.labels["__meta_kubernetes_endpoint_address_target_kind"],
            "Pod"
        );
        assert_eq!(ready.source, "kubernetes_sd/endpoints/d/svc1");

        let notready = g
            .iter()
            .find(|x| x.targets == vec!["10.0.0.9:8080".to_string()])
            .unwrap();
        assert_eq!(notready.labels["__meta_kubernetes_endpoint_ready"], "false");
        assert!(!notready.labels.contains_key("__meta_kubernetes_pod_name"));

        // the pod's 9090 port was not exposed via the endpoints object -> extra target
        let skipped = g
            .iter()
            .find(|x| x.targets == vec!["10.0.0.1:9090".to_string()])
            .unwrap();
        assert_eq!(
            skipped.labels["__meta_kubernetes_pod_container_port_number"],
            "9090"
        );
        assert_eq!(skipped.labels["__meta_kubernetes_endpoints_name"], "svc1");
        assert_eq!(
            skipped.labels["__meta_kubernetes_endpoint_address_target_kind"],
            "Pod"
        );
        assert_eq!(skipped.labels["__meta_kubernetes_service_name"], "svc1");
        // skipped-port groups carry no endpoint_ready label
        assert!(!skipped
            .labels
            .contains_key("__meta_kubernetes_endpoint_ready"));
    }

    #[test]
    fn endpoints_without_registry_still_emit_address_targets() {
        let (objs, _) = parse_list("endpoints", EPS_LIST).unwrap();
        let g = objs[0].target_groups(&BuildCtx::detached());
        assert_eq!(g.len(), 2); // no pod/service in registry -> no joins, no fanout
        assert!(g
            .iter()
            .all(|x| !x.labels.contains_key("__meta_kubernetes_service_name")));
    }

    #[test]
    fn endpoints_init_sidecar_requires_restart_policy_always() {
        let pod_with_init: &[u8] = br#"{"items":[{"metadata":{"name":"p1","namespace":"d"},
            "spec":{"containers":[{"name":"c","image":"i",
                "ports":[{"name":"http","containerPort":8080,"protocol":"TCP"}]}],
                "initContainers":[
                  {"name":"sc","image":"i2","restartPolicy":"Always",
                   "ports":[{"name":"m","containerPort":7070,"protocol":"TCP"}]},
                  {"name":"plain-init","image":"i3",
                   "ports":[{"name":"x","containerPort":6060,"protocol":"TCP"}]}]},
            "status":{"phase":"Running","podIP":"10.0.0.1"}}]}"#;
        let reg = reg_with(&[("pod", pod_with_init)]);
        let ctx = BuildCtx {
            registry: &reg,
            attach_node_metadata: false,
            attach_namespace_metadata: false,
        };
        let (objs, _) = parse_list("endpoints", EPS_LIST).unwrap();
        let g = objs[0].target_groups(&ctx);
        // sidecar's 7070 fans out; plain init's 6060 must NOT
        assert!(g
            .iter()
            .any(|x| x.targets == vec!["10.0.0.1:7070".to_string()]));
        assert!(!g
            .iter()
            .any(|x| x.targets == vec!["10.0.0.1:6060".to_string()]));
    }
}
