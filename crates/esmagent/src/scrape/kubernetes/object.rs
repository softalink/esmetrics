//! Kubernetes API object serde structs.
//!
//! Mirrors the subset of upstream vmagent's `common_types.go` needed by
//! service discovery: `ObjectMeta`, `OwnerReference`, `ListMeta`, and the
//! watch-stream envelope `WatchEvent` (`api_watcher.go`).
//!
//! All structs derive `Default` and use `#[serde(default)]` so that fields
//! absent from a given API response (e.g. a partial object in a watch
//! event) simply take their zero value instead of failing to deserialize.
//! `rename_all = "camelCase"` maps Rust's `snake_case` field names to the
//! Kubernetes API's camelCase JSON field names (e.g. `resourceVersion`,
//! `ownerReferences`).

use std::collections::BTreeMap;

use serde::Deserialize;

use super::registry::BuildCtx;
use super::roles;
use crate::scrape::discovery::TargetGroup;

/// Kubernetes object metadata (`metadata` field of any k8s API object).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ObjectMeta {
    pub name: String,
    pub namespace: String,
    pub uid: String,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub owner_references: Vec<OwnerReference>,
}

impl ObjectMeta {
    /// The object's unique key within its kind: `<namespace>/<name>`.
    pub fn key(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
}

/// A single entry in `metadata.ownerReferences`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct OwnerReference {
    pub name: String,
    pub kind: String,
    pub controller: bool,
}

/// Kubernetes list metadata (`metadata` field of a k8s API list response).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ListMeta {
    pub resource_version: String,
}

/// A single event from a Kubernetes watch stream.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct WatchEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub object: serde_json::Value,
}

/// A Kubernetes `Node` object (the `metadata`/`spec`/`status` subset needed
/// for the `node` role's service discovery labels).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Node {
    pub metadata: ObjectMeta,
    pub spec: NodeSpec,
    pub status: NodeStatus,
}

/// `Node.spec` subset needed for SD.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NodeSpec {
    #[serde(rename = "providerID")]
    pub provider_id: String,
}

/// `Node.status` subset needed for SD.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NodeStatus {
    pub addresses: Vec<NodeAddress>,
    pub daemon_endpoints: NodeDaemonEndpoints,
}

/// One entry in `Node.status.addresses`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NodeAddress {
    #[serde(rename = "type")]
    pub address_type: String,
    pub address: String,
}

/// `Node.status.daemonEndpoints`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NodeDaemonEndpoints {
    pub kubelet_endpoint: DaemonEndpoint,
}

/// A single named endpoint (currently only used for the kubelet endpoint).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct DaemonEndpoint {
    pub port: i64,
}

/// A Kubernetes API list response for [`Node`] objects:
/// `{metadata: {resourceVersion}, items: [...]}`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NodeList {
    pub metadata: ListMeta,
    pub items: Vec<Node>,
}

/// A Kubernetes `Pod` object (the `metadata`/`spec`/`status` subset needed
/// for the `pod` role's service discovery labels).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Pod {
    pub metadata: ObjectMeta,
    pub spec: PodSpec,
    pub status: PodStatus,
}

/// `Pod.spec` subset needed for SD.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PodSpec {
    pub node_name: String,
    pub containers: Vec<Container>,
    pub init_containers: Vec<Container>,
}

/// `Pod.status` subset needed for SD.
///
/// `podIP`/`hostIP` need explicit renames: k8s capitalizes `IP`, so the
/// default `rename_all = "camelCase"` mapping (`podIp`/`hostIp`) would miss
/// the field entirely and silently leave it empty.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PodStatus {
    pub phase: String,
    #[serde(rename = "podIP")]
    pub pod_ip: String,
    #[serde(rename = "hostIP")]
    pub host_ip: String,
    pub conditions: Vec<PodCondition>,
    pub container_statuses: Vec<ContainerStatus>,
    pub init_container_statuses: Vec<ContainerStatus>,
}

/// A single container spec entry (`Pod.spec.containers`/`initContainers`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Container {
    pub name: String,
    pub image: String,
    pub ports: Vec<ContainerPort>,
    pub restart_policy: String,
}

/// One entry in `Container.ports`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ContainerPort {
    pub name: String,
    pub container_port: i64,
    pub protocol: String,
}

/// One entry in `Pod.status.conditions`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PodCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
}

/// One entry in `Pod.status.containerStatuses`/`initContainerStatuses`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ContainerStatus {
    pub name: String,
    #[serde(rename = "containerID")]
    pub container_id: String,
    pub state: ContainerState,
}

/// `ContainerStatus.state`; only whether `terminated` is present matters for
/// SD (a container is skipped once it has finished running).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ContainerState {
    pub terminated: Option<serde_json::Value>,
}

/// A Kubernetes API list response for [`Pod`] objects:
/// `{metadata: {resourceVersion}, items: [...]}`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PodList {
    pub metadata: ListMeta,
    pub items: Vec<Pod>,
}

/// A Kubernetes `Service` object (the `metadata`/`spec` subset needed for
/// the `service` role's service discovery labels).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Service {
    pub metadata: ObjectMeta,
    pub spec: ServiceSpec,
}

/// `Service.spec` subset needed for SD.
///
/// k8s capitalizes `IP`, so the default `rename_all = "camelCase"` mapping
/// (`clusterIp`) would miss the field entirely; `type` is a Rust keyword and
/// needs an explicit rename regardless of casing.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ServiceSpec {
    #[serde(rename = "clusterIP")]
    pub cluster_ip: String,
    pub external_name: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub ports: Vec<ServicePort>,
}

/// One entry in `Service.spec.ports`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ServicePort {
    pub name: String,
    pub protocol: String,
    pub port: i64,
}

/// A Kubernetes API list response for [`Service`] objects:
/// `{metadata: {resourceVersion}, items: [...]}`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ServiceList {
    pub metadata: ListMeta,
    pub items: Vec<Service>,
}

/// A Kubernetes `Ingress` object (the `metadata`/`spec` subset needed for
/// the `ingress` role's service discovery labels).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Ingress {
    pub metadata: ObjectMeta,
    pub spec: IngressSpec,
}

/// `Ingress.spec` subset needed for SD.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct IngressSpec {
    pub tls: Vec<IngressTLS>,
    pub rules: Vec<IngressRule>,
    pub ingress_class_name: String,
}

/// One entry in `Ingress.spec.tls`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct IngressTLS {
    pub hosts: Vec<String>,
}

/// One entry in `Ingress.spec.rules`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct IngressRule {
    pub host: String,
    pub http: IngressHTTP,
}

/// `IngressRule.http`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct IngressHTTP {
    pub paths: Vec<IngressPath>,
}

/// One entry in `IngressHTTP.paths`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct IngressPath {
    pub path: String,
}

/// A Kubernetes API list response for [`Ingress`] objects:
/// `{metadata: {resourceVersion}, items: [...]}`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct IngressList {
    pub metadata: ListMeta,
    pub items: Vec<Ingress>,
}

/// A Kubernetes `Namespace` object. Namespaces are metadata-only for SD
/// purposes: upstream also parses `spec`/`status`, but only `metadata` is
/// ever used (for label enrichment of other resources), so those fields are
/// intentionally omitted here.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Namespace {
    pub metadata: ObjectMeta,
}

/// A Kubernetes API list response for [`Namespace`] objects:
/// `{metadata: {resourceVersion}, items: [...]}`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NamespaceList {
    pub metadata: ListMeta,
    pub items: Vec<Namespace>,
}

/// A reference to another Kubernetes object (e.g. `EndpointAddress.targetRef`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ObjectReference {
    pub kind: String,
    pub name: String,
    pub namespace: String,
}

/// A Kubernetes `Endpoints` object (the `metadata`/`subsets` subset needed
/// for the `endpoints` role's service discovery labels).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Endpoints {
    pub metadata: ObjectMeta,
    pub subsets: Vec<EndpointSubset>,
}

/// One entry in `Endpoints.subsets`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct EndpointSubset {
    pub addresses: Vec<EndpointAddress>,
    pub not_ready_addresses: Vec<EndpointAddress>,
    pub ports: Vec<EndpointPort>,
}

/// One entry in `EndpointSubset.addresses`/`notReadyAddresses`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct EndpointAddress {
    pub hostname: String,
    pub ip: String,
    pub node_name: String,
    pub target_ref: ObjectReference,
}

/// One entry in `EndpointSubset.ports` / `EndpointSlice.ports`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct EndpointPort {
    pub app_protocol: String,
    pub name: String,
    pub port: i64,
    pub protocol: String,
}

/// A Kubernetes API list response for [`Endpoints`] objects:
/// `{metadata: {resourceVersion}, items: [...]}`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct EndpointsList {
    pub metadata: ListMeta,
    pub items: Vec<Endpoints>,
}

/// A Kubernetes `EndpointSlice` object (the `metadata`/`endpoints`/`ports`
/// subset needed for the `endpointslice` role's service discovery labels).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct EndpointSlice {
    pub metadata: ObjectMeta,
    pub endpoints: Vec<Endpoint>,
    pub address_type: String,
    pub ports: Vec<EndpointPort>,
}

/// One entry in `EndpointSlice.endpoints`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Endpoint {
    pub addresses: Vec<String>,
    pub conditions: EndpointConditions,
    pub hostname: String,
    pub target_ref: ObjectReference,
    pub topology: BTreeMap<String, String>,
    pub node_name: String,
}

/// `Endpoint.conditions`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct EndpointConditions {
    pub ready: bool,
    pub serving: bool,
    pub terminating: bool,
}

/// A Kubernetes API list response for [`EndpointSlice`] objects:
/// `{metadata: {resourceVersion}, items: [...]}`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct EndpointSliceList {
    pub metadata: ListMeta,
    pub items: Vec<EndpointSlice>,
}

/// A parsed Kubernetes object of any SD-supported role, tagged by kind.
#[derive(Debug, Clone)]
pub enum K8sObject {
    Node(Node),
    Pod(Pod),
    Service(Service),
    Ingress(Ingress),
    Namespace(Namespace),
    Endpoints(Endpoints),
    EndpointSlice(EndpointSlice),
}

impl K8sObject {
    /// The object's unique key within its kind: `<namespace>/<name>`.
    pub fn key(&self) -> String {
        match self {
            K8sObject::Node(n) => n.metadata.key(),
            K8sObject::Pod(p) => p.metadata.key(),
            K8sObject::Service(s) => s.metadata.key(),
            K8sObject::Ingress(ig) => ig.metadata.key(),
            K8sObject::Namespace(ns) => ns.metadata.key(),
            K8sObject::Endpoints(eps) => eps.metadata.key(),
            K8sObject::EndpointSlice(eps) => eps.metadata.key(),
        }
    }

    /// Builds the [`TargetGroup`]s this object contributes to scraping, by
    /// dispatching to the matching role builder. Returns an empty `Vec` if
    /// the object has no usable scrape target (e.g. a node with no address).
    pub fn target_groups(&self, ctx: &BuildCtx) -> Vec<TargetGroup> {
        match self {
            K8sObject::Node(n) => roles::node::node_target_groups(n, ctx),
            K8sObject::Pod(p) => roles::pod::pod_target_groups(p, ctx),
            K8sObject::Service(s) => roles::service::service_target_groups(s, ctx),
            K8sObject::Ingress(ig) => roles::ingress::ingress_target_groups(ig, ctx),
            // Namespaces are metadata-only; upstream `namespace.go`'s
            // `getTargetLabels` always returns nil.
            K8sObject::Namespace(_) => Vec::new(),
            K8sObject::Endpoints(eps) => roles::endpoints::endpoints_target_groups(eps, ctx),
            K8sObject::EndpointSlice(eps) => {
                roles::endpointslice::endpointslice_target_groups(eps, ctx)
            }
        }
    }
}

/// Parses a single object of `role`'s type (e.g. a watch event's `object`
/// payload). Returns `Err` for an unknown role.
pub fn parse_object(role: &str, data: &[u8]) -> Result<K8sObject, String> {
    match role {
        "node" => {
            let n: Node = serde_json::from_slice(data).map_err(|e| e.to_string())?;
            Ok(K8sObject::Node(n))
        }
        "pod" => {
            let p: Pod = serde_json::from_slice(data).map_err(|e| e.to_string())?;
            Ok(K8sObject::Pod(p))
        }
        "service" => {
            let s: Service = serde_json::from_slice(data).map_err(|e| e.to_string())?;
            Ok(K8sObject::Service(s))
        }
        "ingress" => {
            let ig: Ingress = serde_json::from_slice(data).map_err(|e| e.to_string())?;
            Ok(K8sObject::Ingress(ig))
        }
        "namespace" => {
            let ns: Namespace = serde_json::from_slice(data).map_err(|e| e.to_string())?;
            Ok(K8sObject::Namespace(ns))
        }
        "endpoints" => {
            let eps: Endpoints = serde_json::from_slice(data).map_err(|e| e.to_string())?;
            Ok(K8sObject::Endpoints(eps))
        }
        "endpointslice" => {
            let eps: EndpointSlice = serde_json::from_slice(data).map_err(|e| e.to_string())?;
            Ok(K8sObject::EndpointSlice(eps))
        }
        other => Err(format!("unsupported role: {other}")),
    }
}

/// Parses a Kubernetes API list response of `role`'s type
/// (`{metadata: {resourceVersion}, items: [...]}`), returning the parsed
/// objects plus the list's `resourceVersion`. Returns `Err` for an unknown
/// role.
pub fn parse_list(role: &str, data: &[u8]) -> Result<(Vec<K8sObject>, String), String> {
    match role {
        "node" => {
            let list: NodeList = serde_json::from_slice(data).map_err(|e| e.to_string())?;
            let resource_version = list.metadata.resource_version;
            let objects = list.items.into_iter().map(K8sObject::Node).collect();
            Ok((objects, resource_version))
        }
        "pod" => {
            let list: PodList = serde_json::from_slice(data).map_err(|e| e.to_string())?;
            let resource_version = list.metadata.resource_version;
            let objects = list.items.into_iter().map(K8sObject::Pod).collect();
            Ok((objects, resource_version))
        }
        "service" => {
            let list: ServiceList = serde_json::from_slice(data).map_err(|e| e.to_string())?;
            let resource_version = list.metadata.resource_version;
            let objects = list.items.into_iter().map(K8sObject::Service).collect();
            Ok((objects, resource_version))
        }
        "ingress" => {
            let list: IngressList = serde_json::from_slice(data).map_err(|e| e.to_string())?;
            let resource_version = list.metadata.resource_version;
            let objects = list.items.into_iter().map(K8sObject::Ingress).collect();
            Ok((objects, resource_version))
        }
        "namespace" => {
            let list: NamespaceList = serde_json::from_slice(data).map_err(|e| e.to_string())?;
            let resource_version = list.metadata.resource_version;
            let objects = list.items.into_iter().map(K8sObject::Namespace).collect();
            Ok((objects, resource_version))
        }
        "endpoints" => {
            let list: EndpointsList = serde_json::from_slice(data).map_err(|e| e.to_string())?;
            let resource_version = list.metadata.resource_version;
            let objects = list.items.into_iter().map(K8sObject::Endpoints).collect();
            Ok((objects, resource_version))
        }
        "endpointslice" => {
            let list: EndpointSliceList =
                serde_json::from_slice(data).map_err(|e| e.to_string())?;
            let resource_version = list.metadata.resource_version;
            let objects = list
                .items
                .into_iter()
                .map(K8sObject::EndpointSlice)
                .collect();
            Ok((objects, resource_version))
        }
        other => Err(format!("unsupported role: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scrape::kubernetes::registry::BuildCtx;

    #[test]
    fn parses_object_meta_camelcase_and_key() {
        let j = r#"{"name":"p1","namespace":"ns","uid":"u1","labels":{"a":"b"},
            "ownerReferences":[{"name":"rs1","kind":"ReplicaSet","controller":true}]}"#;
        let m: ObjectMeta = serde_json::from_str(j).unwrap();
        assert_eq!(m.key(), "ns/p1");
        assert_eq!(m.labels["a"], "b");
        assert_eq!(m.owner_references[0].kind, "ReplicaSet");
        assert!(m.owner_references[0].controller);
    }

    #[test]
    fn parses_watch_event_type_rename() {
        let e: WatchEvent = serde_json::from_str(r#"{"type":"ADDED","object":{"x":1}}"#).unwrap();
        assert_eq!(e.event_type, "ADDED");
        assert_eq!(e.object["x"], 1);
    }

    #[test]
    fn parses_endpoints_list() {
        let j = br#"{"metadata":{"resourceVersion":"5"},"items":[
            {"metadata":{"name":"svc1","namespace":"d"},
             "subsets":[{"addresses":[{"ip":"10.0.0.1","nodeName":"n1",
                            "targetRef":{"kind":"Pod","name":"p1","namespace":"d"}}],
                         "notReadyAddresses":[{"ip":"10.0.0.2"}],
                         "ports":[{"name":"http","port":8080,"protocol":"TCP"}]}]}]}"#;
        let (objs, rv) = parse_list("endpoints", j).unwrap();
        assert_eq!(rv, "5");
        let K8sObject::Endpoints(eps) = &objs[0] else {
            panic!("wrong variant")
        };
        assert_eq!(eps.metadata.key(), "d/svc1");
        assert_eq!(eps.subsets[0].addresses[0].ip, "10.0.0.1");
        assert_eq!(eps.subsets[0].addresses[0].target_ref.name, "p1");
        assert_eq!(eps.subsets[0].not_ready_addresses[0].ip, "10.0.0.2");
        assert_eq!(eps.subsets[0].ports[0].port, 8080);
        // With a detached ctx (no registry) the endpoints builder still emits
        // one group per address: the ready address plus the not-ready one.
        assert_eq!(objs[0].target_groups(&BuildCtx::detached()).len(), 2);
    }

    #[test]
    fn parses_endpointslice_list() {
        let j = br#"{"metadata":{"resourceVersion":"9"},"items":[
            {"metadata":{"name":"svc1-abc","namespace":"d",
                "labels":{"kubernetes.io/service-name":"svc1"}},
             "addressType":"IPv4",
             "endpoints":[{"addresses":["10.0.0.3"],
                "conditions":{"ready":true,"serving":true,"terminating":false},
                "nodeName":"n2","topology":{"kubernetes.io/hostname":"n2"},
                "targetRef":{"kind":"Pod","name":"p2","namespace":"d"}}],
             "ports":[{"name":"web","port":80,"protocol":"TCP","appProtocol":"http"}]}]}"#;
        let (objs, rv) = parse_list("endpointslice", j).unwrap();
        assert_eq!(rv, "9");
        let K8sObject::EndpointSlice(eps) = &objs[0] else {
            panic!("wrong variant")
        };
        assert_eq!(eps.address_type, "IPv4");
        assert!(eps.endpoints[0].conditions.ready);
        assert_eq!(eps.endpoints[0].topology["kubernetes.io/hostname"], "n2");
        assert_eq!(eps.ports[0].app_protocol, "http");
    }

    #[test]
    fn parses_namespace_list_with_slash_key() {
        let j = br#"{"items":[{"metadata":{"name":"prod","labels":{"team":"a"}}}]}"#;
        let (objs, _) = parse_list("namespace", j).unwrap();
        assert_eq!(objs[0].key(), "/prod");
        assert!(objs[0].target_groups(&BuildCtx::detached()).is_empty());
    }

    #[test]
    fn parses_container_restart_policy() {
        let j = br#"{"name":"sidecar","restartPolicy":"Always"}"#;
        let c: Container = serde_json::from_slice(j).unwrap();
        assert_eq!(c.restart_policy, "Always");
    }
}
