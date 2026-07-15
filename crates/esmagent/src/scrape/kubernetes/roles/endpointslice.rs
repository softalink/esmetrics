//! `endpointslice` role target-group builder.
//!
//! Ports upstream vmagent's `EndpointSlice.getTargetLabels`/
//! `getEndpointSliceLabelsForAddressAndPort`/`getEndpointSliceLabels`
//! (`lib/promscrape/discovery/kubernetes/endpointslice.go`) plus
//! `Pod.appendEndpointSliceLabels` (`pod.go`).
//!
//! Like the `endpoints` role, the `endpointslice` role joins across three
//! kinds through the shared
//! [`ObjectRegistry`](crate::scrape::kubernetes::registry::ObjectRegistry):
//! the `EndpointSlice` object itself, the `Service` it belongs to, and the
//! `Pod` referenced by each endpoint's `targetRef`. Two deliberate
//! asymmetries versus the `endpoints` role:
//!
//! 1. The service is resolved by the `kubernetes.io/service-name` label value,
//!    not by the slice's own name (an empty label misses, yielding no join).
//! 2. The pod is looked up once per endpoint (before the port loop), and in
//!    the address-path container match a native-sidecar init container is not
//!    required to declare `restartPolicy: Always` (upstream's slice matching
//!    loop has no such guard, unlike `endpoints.go`). The *skipped-ports*
//!    fanout does still require it.
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
use crate::scrape::kubernetes::labels::{register_labels_and_annotations, sanitize_label_name};
use crate::scrape::kubernetes::object::{
    Container, Endpoint, EndpointPort, EndpointSlice, K8sObject, ObjectMeta, Pod, Service,
};
use crate::scrape::kubernetes::registry::BuildCtx;

/// The shared, immutable context for one `EndpointSlice` build: the
/// surrounding [`BuildCtx`], the object being built, the joined `Service` (if
/// any), and the common `source` string.
struct EndpointSliceCtx<'a> {
    build: &'a BuildCtx<'a>,
    eps: &'a EndpointSlice,
    svc: Option<&'a Service>,
    source: &'a str,
}

/// Per-pod accumulator: the pod object plus the container ports already
/// emitted through a matching endpoint port. Keyed by pod key in a
/// [`BTreeMap`] so the skipped-ports fanout is deterministic (upstream keys by
/// pod pointer, whose map order is nondeterministic).
type PodPortsSeen = BTreeMap<String, (Arc<K8sObject>, Vec<i64>)>;

/// Builds the [`TargetGroup`]s for a single `EndpointSlice` object: one per
/// endpoint address × port, followed by one extra group per skipped container
/// port on every pod referenced by a `targetRef`.
pub fn endpointslice_target_groups(eps: &EndpointSlice, ctx: &BuildCtx) -> Vec<TargetGroup> {
    let source = format!(
        "kubernetes_sd/endpointslice/{}/{}",
        eps.metadata.namespace, eps.metadata.name
    );

    // The associated service name is stored in the kubernetes.io/service-name
    // label; an empty value misses and yields no service join.
    let svc_name = eps
        .metadata
        .labels
        .get("kubernetes.io/service-name")
        .map(String::as_str)
        .unwrap_or("");
    let svc_obj = ctx
        .registry
        .get_object("service", &eps.metadata.namespace, svc_name);
    let svc = svc_obj.as_deref().and_then(|o| match o {
        K8sObject::Service(s) => Some(s),
        _ => None,
    });

    let ec = EndpointSliceCtx {
        build: ctx,
        eps,
        svc,
        source: &source,
    };

    let mut pod_ports_seen: PodPortsSeen = BTreeMap::new();
    let mut groups = Vec::new();

    for ess in &eps.endpoints {
        // The pod is resolved once per endpoint, before the port loop.
        let pod_arc = if ess.target_ref.name.is_empty() {
            None
        } else {
            ctx.registry
                .get_object("pod", &ess.target_ref.namespace, &ess.target_ref.name)
        };
        for epp in &eps.ports {
            for addr in &ess.addresses {
                let labels = get_endpointslice_labels_for_address_and_port(
                    &ec,
                    &mut pod_ports_seen,
                    ess,
                    epp,
                    pod_arc.as_ref(),
                );
                groups.push(TargetGroup {
                    targets: vec![join_host_port(addr, epp.port)],
                    labels,
                    source: source.clone(),
                });
            }
        }
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

/// Ports `getEndpointSliceLabelsForAddressAndPort`: the base slice labels, the
/// service join, the endpointslice meta, and (for a `Pod` target) the pod
/// common labels plus the matching container-port labels. Records the pod and
/// its matched ports in `pod_ports_seen` for the later skipped-ports fanout.
fn get_endpointslice_labels_for_address_and_port(
    ec: &EndpointSliceCtx,
    pod_ports_seen: &mut PodPortsSeen,
    ess: &Endpoint,
    epp: &EndpointPort,
    pod_arc: Option<&Arc<K8sObject>>,
) -> BTreeMap<String, String> {
    let mut labels = get_endpointslice_labels(&ec.eps.metadata, &ec.eps.address_type, ess, epp);
    if let Some(s) = ec.svc {
        append_service_common_labels(s, ec.build, &mut labels);
    }
    // See https://github.com/prometheus/prometheus/issues/10284
    register_labels_and_annotations(
        "__meta_kubernetes_endpointslice",
        &ec.eps.metadata,
        &mut labels,
    );

    if ess.target_ref.kind != "Pod" {
        return labels;
    }
    let Some(pod_arc) = pod_arc else {
        return labels;
    };
    let K8sObject::Pod(p) = &**pod_arc else {
        return labels;
    };

    // Always record the pod targetRef, even if no container port matches, and
    // before appending the pod's common labels (upstream records first).
    // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/2134
    let entry = pod_ports_seen
        .entry(p.metadata.key())
        .or_insert_with(|| (Arc::clone(pod_arc), Vec::new()));

    append_pod_common_labels(p, ec.build, &mut labels);
    // Unlike endpoints.go, the slice matching loop applies no restart-policy
    // guard to init containers.
    for c in &p.spec.containers {
        match_container_port(c, epp, false, &mut entry.1, &mut labels);
    }
    for c in &p.spec.init_containers {
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

/// Ports `getEndpointSliceLabels`: the base per-address/per-port label block.
fn get_endpointslice_labels(
    om: &ObjectMeta,
    address_type: &str,
    ea: &Endpoint,
    epp: &EndpointPort,
) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert(
        "__meta_kubernetes_namespace".to_string(),
        om.namespace.clone(),
    );
    m.insert(
        "__meta_kubernetes_endpointslice_name".to_string(),
        om.name.clone(),
    );
    m.insert(
        "__meta_kubernetes_endpointslice_address_type".to_string(),
        address_type.to_string(),
    );
    m.insert(
        "__meta_kubernetes_endpointslice_endpoint_conditions_ready".to_string(),
        ea.conditions.ready.to_string(),
    );
    m.insert(
        "__meta_kubernetes_endpointslice_endpoint_conditions_serving".to_string(),
        ea.conditions.serving.to_string(),
    );
    m.insert(
        "__meta_kubernetes_endpointslice_endpoint_conditions_terminating".to_string(),
        ea.conditions.terminating.to_string(),
    );
    m.insert(
        "__meta_kubernetes_endpointslice_port_name".to_string(),
        epp.name.clone(),
    );
    m.insert(
        "__meta_kubernetes_endpointslice_port_protocol".to_string(),
        epp.protocol.clone(),
    );
    m.insert(
        "__meta_kubernetes_endpointslice_port".to_string(),
        epp.port.to_string(),
    );
    if !epp.app_protocol.is_empty() {
        m.insert(
            "__meta_kubernetes_endpointslice_port_app_protocol".to_string(),
            epp.app_protocol.clone(),
        );
    }
    if !ea.target_ref.kind.is_empty() {
        m.insert(
            "__meta_kubernetes_endpointslice_address_target_kind".to_string(),
            ea.target_ref.kind.clone(),
        );
        m.insert(
            "__meta_kubernetes_endpointslice_address_target_name".to_string(),
            ea.target_ref.name.clone(),
        );
    }
    if !ea.hostname.is_empty() {
        m.insert(
            "__meta_kubernetes_endpointslice_endpoint_hostname".to_string(),
            ea.hostname.clone(),
        );
    }
    if !ea.node_name.is_empty() {
        m.insert(
            "__meta_kubernetes_endpointslice_endpoint_node_name".to_string(),
            ea.node_name.clone(),
        );
    }
    for (k, v) in &ea.topology {
        m.insert(
            sanitize_label_name(&format!(
                "__meta_kubernetes_endpointslice_endpoint_topology_{k}"
            )),
            v.clone(),
        );
        m.insert(
            sanitize_label_name(&format!(
                "__meta_kubernetes_endpointslice_endpoint_topology_present_{k}"
            )),
            "true".to_string(),
        );
    }
    m
}

/// Ports the `appendPodMetadata` closure from `getTargetLabels`: for one
/// container, emits a group for every container port that was *not* already
/// matched by an endpoint port. These carry the pod, container, endpointslice
/// and service labels but no endpoint conditions / port block.
fn append_pod_metadata(
    ec: &EndpointSliceCtx,
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
        // Prometheus sets endpointslice_name and namespace labels for all
        // endpoints, even ports not matching a service port.
        // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/4154
        append_endpointslice_labels(&mut labels, ec.eps, p);
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

/// Ports `Pod.appendEndpointSliceLabels` (`pod.go`): the pod-side
/// endpointslice identity labels attached to skipped-port fanout groups.
fn append_endpointslice_labels(
    labels: &mut BTreeMap<String, String>,
    eps: &EndpointSlice,
    p: &Pod,
) {
    labels.insert(
        "__meta_kubernetes_endpointslice_name".to_string(),
        eps.metadata.name.clone(),
    );
    labels.insert(
        "__meta_kubernetes_endpointslice_address_target_kind".to_string(),
        "Pod".to_string(),
    );
    labels.insert(
        "__meta_kubernetes_endpointslice_address_target_name".to_string(),
        p.metadata.name.clone(),
    );
    labels.insert(
        "__meta_kubernetes_endpointslice_address_type".to_string(),
        eps.address_type.clone(),
    );
    register_labels_and_annotations("__meta_kubernetes_endpointslice", &eps.metadata, labels);
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

    const POD_LIST: &[u8] = br#"{"items":[{"metadata":{"name":"p2","namespace":"d"},
        "spec":{"containers":[{"name":"c","image":"i",
            "ports":[{"name":"web","containerPort":80,"protocol":"TCP"},
                     {"name":"extra","containerPort":81,"protocol":"TCP"}]}]},
        "status":{"phase":"Running","podIP":"10.0.0.3"}}]}"#;
    const SVC_LIST: &[u8] = br#"{"items":[{"metadata":{"name":"svc1","namespace":"d"},
        "spec":{"type":"ClusterIP","clusterIP":"10.96.0.2",
            "ports":[{"name":"web","protocol":"TCP","port":80}]}}]}"#;
    const SLICE_LIST: &[u8] = br#"{"items":[{"metadata":{"name":"svc1-abc","namespace":"d",
        "labels":{"kubernetes.io/service-name":"svc1"}},
        "addressType":"IPv4",
        "endpoints":[{"addresses":["10.0.0.3"],
            "conditions":{"ready":true,"serving":true,"terminating":false},
            "nodeName":"n2","topology":{"kubernetes.io/hostname":"n2"},
            "targetRef":{"kind":"Pod","name":"p2","namespace":"d"}}],
        "ports":[{"name":"web","port":80,"protocol":"TCP","appProtocol":"http"}]}]}"#;

    #[test]
    fn endpointslice_joins_and_fans_out() {
        let reg = reg_with(&[("pod", POD_LIST), ("service", SVC_LIST)]);
        let ctx = BuildCtx {
            registry: &reg,
            attach_node_metadata: false,
            attach_namespace_metadata: false,
        };
        let (objs, _) = parse_list("endpointslice", SLICE_LIST).unwrap();
        let g = objs[0].target_groups(&ctx);
        assert_eq!(g.len(), 2); // 1 address target + 1 skipped-port (81)

        let main = g
            .iter()
            .find(|x| x.targets == vec!["10.0.0.3:80".to_string()])
            .unwrap();
        assert_eq!(
            main.labels["__meta_kubernetes_endpointslice_name"],
            "svc1-abc"
        );
        assert_eq!(
            main.labels["__meta_kubernetes_endpointslice_address_type"],
            "IPv4"
        );
        assert_eq!(
            main.labels["__meta_kubernetes_endpointslice_endpoint_conditions_ready"],
            "true"
        );
        assert_eq!(
            main.labels["__meta_kubernetes_endpointslice_endpoint_conditions_terminating"],
            "false"
        );
        assert_eq!(main.labels["__meta_kubernetes_endpointslice_port"], "80");
        assert_eq!(
            main.labels["__meta_kubernetes_endpointslice_port_app_protocol"],
            "http"
        );
        assert_eq!(
            main.labels["__meta_kubernetes_endpointslice_endpoint_topology_kubernetes_io_hostname"],
            "n2"
        );
        assert_eq!(
            main.labels
                ["__meta_kubernetes_endpointslice_endpoint_topology_present_kubernetes_io_hostname"],
            "true"
        );
        // service-name label join
        assert_eq!(main.labels["__meta_kubernetes_service_name"], "svc1");
        // pod join
        assert_eq!(main.labels["__meta_kubernetes_pod_name"], "p2");
        assert_eq!(
            main.labels["__meta_kubernetes_pod_container_port_number"],
            "80"
        );
        assert_eq!(main.source, "kubernetes_sd/endpointslice/d/svc1-abc");

        let skipped = g
            .iter()
            .find(|x| x.targets == vec!["10.0.0.3:81".to_string()])
            .unwrap();
        assert_eq!(
            skipped.labels["__meta_kubernetes_endpointslice_address_target_kind"],
            "Pod"
        );
        assert_eq!(
            skipped.labels["__meta_kubernetes_endpointslice_address_type"],
            "IPv4"
        );
    }

    #[test]
    fn endpointslice_without_service_name_label_skips_service_join() {
        let no_label: &[u8] = br#"{"items":[{"metadata":{"name":"s-x","namespace":"d"},
            "addressType":"IPv4",
            "endpoints":[{"addresses":["10.0.0.5"],"conditions":{"ready":true}}],
            "ports":[{"name":"w","port":80,"protocol":"TCP"}]}]}"#;
        let reg = reg_with(&[("service", SVC_LIST)]);
        let ctx = BuildCtx {
            registry: &reg,
            attach_node_metadata: false,
            attach_namespace_metadata: false,
        };
        let (objs, _) = parse_list("endpointslice", no_label).unwrap();
        let g = objs[0].target_groups(&ctx);
        assert_eq!(g.len(), 1);
        assert!(!g[0].labels.contains_key("__meta_kubernetes_service_name"));
        // no target_ref -> no target kind labels
        assert!(!g[0]
            .labels
            .contains_key("__meta_kubernetes_endpointslice_address_target_kind"));
    }
}
