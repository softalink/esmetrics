# esmagent Kubernetes SD Phase B Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the Kubernetes SD Phase B gap: `endpoints` + `endpointslice` roles, the shared cross-role object registry, and `attach_metadata` node/namespace label joins.

**Architecture:** Phase A gave each `(role, namespace)` its own `Watcher` with a private cache and self-contained `K8sObject::target_groups()`. Phase B adds an `ObjectRegistry` per `KubernetesDiscovery` (the analog of upstream `groupWatcher`'s `getObjectByRoleLocked`): every watcher's cache — primary role plus dependency roles (`pod`/`service` for endpoints roles, `node`/`namespace` for `attach_metadata`) — is registered in it, and target-group building takes a `BuildCtx { registry, attach_node_metadata, attach_namespace_metadata }` so builders can join labels across roles. Only the primary role's watchers contribute target groups in `poll()`; dependency watchers only feed the registry.

**Tech Stack:** Rust (edition 2021), `reqwest::blocking`, `serde`/`serde_json`, existing `scrape::kubernetes` module (Phase A), no tokio.

## Porting Convention (read before every task)

Faithful port of `/home/test/refsrc/VictoriaMetrics/lib/promscrape/discovery/kubernetes/` @ v1.146.0 — chiefly `endpoints.go`, `endpointslice.go`, `namespace.go`, `pod.go` (`appendCommonLabels` attach branches), `service.go` (`appendCommonLabels` attach branch), `ingress.go` (attach branch), `api_watcher.go` (`getObjectByRoleLocked`, `startWatchersForRole` dependency rules, `useDiscoveryV1Beta1`). When a task cites a `.go` file, read and translate faithfully.

Upstream duplicate-label semantics: `promutil.Labels.RemoveDuplicates()` keeps the **last** occurrence of a duplicated name. Our `BTreeMap<String,String>` insert-overwrite already keeps the last write — so **replicate upstream's append call order** and the map does the rest.

## Global Constraints

- **No tokio** — `reqwest::blocking` + std threads.
- Faithful to upstream v1.146.0 for the six roles (`node`, `pod`, `service`, `ingress`, `endpoints`, `endpointslice`).
- Files ≤ 800 lines; `cargo fmt` clean; `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets` clean; `cargo build -p esmagent --target x86_64-pc-windows-gnu` compiles.
- Never panic in a watch thread, `poll()`, or a role builder — lookups that miss return `None` and the label join is simply skipped.
- The registry lock is never held across an HTTP call; builders receive `Arc<K8sObject>` clones.
- `TargetGroup { targets, labels, source }`: builders put the address in `targets`, never an `__address__` label.
- Out of scope (unchanged from Phase A, do NOT implement): `kubeconfig_file`, OAuth2, `proxy_url`, the global `-promscrape.kubernetes.attachNodeMetadataAll`/`attachNamespaceMetadataAll` flags, cross-config `groupWatcher` dedup (each `kubernetes_sd_configs` entry gets its own watchers — document this deviation).
- Commit style `<type>: <description>`, no attribution trailers.
- After the final task: `cargo test --workspace` green, push, watch GitHub Actions (Windows tests run only in CI), fix failures.

---

## Task 1: Namespace/Endpoints/EndpointSlice object structs + parse arms

**Files:**
- Modify: `crates/esmagent/src/scrape/kubernetes/object.rs`
- Test: inline in `object.rs`

**Interfaces:**
- Consumes: existing `ObjectMeta`, `ListMeta`, `K8sObject`.
- Produces (all `#[derive(Debug, Clone, Default, Deserialize)]` `#[serde(default, rename_all = "camelCase")]` unless noted):
  - `pub struct Namespace { pub metadata: ObjectMeta }` + `NamespaceList { metadata: ListMeta, items: Vec<Namespace> }` (upstream also parses `spec`/`status` but only `metadata` is ever used for label enrichment — keep metadata only, note it in the doc comment).
  - `pub struct ObjectReference { pub kind: String, pub name: String, pub namespace: String }`
  - `pub struct Endpoints { pub metadata: ObjectMeta, pub subsets: Vec<EndpointSubset> }` + `EndpointsList`
  - `pub struct EndpointSubset { pub addresses: Vec<EndpointAddress>, pub not_ready_addresses: Vec<EndpointAddress>, pub ports: Vec<EndpointPort> }`
  - `pub struct EndpointAddress { pub hostname: String, pub ip: String, pub node_name: String, pub target_ref: ObjectReference }`
  - `pub struct EndpointPort { pub app_protocol: String, pub name: String, pub port: i64, pub protocol: String }`
  - `pub struct EndpointSlice { pub metadata: ObjectMeta, pub endpoints: Vec<Endpoint>, pub address_type: String, pub ports: Vec<EndpointPort> }` + `EndpointSliceList`
  - `pub struct Endpoint { pub addresses: Vec<String>, pub conditions: EndpointConditions, pub hostname: String, pub target_ref: ObjectReference, pub topology: BTreeMap<String, String>, pub node_name: String }`
  - `pub struct EndpointConditions { pub ready: bool, pub serving: bool, pub terminating: bool }`
  - `Container` gains `pub restart_policy: String` (upstream `Container.RestartPolicy`, used for native-sidecar init containers).
  - `K8sObject` gains variants `Namespace(Namespace)`, `Endpoints(Endpoints)`, `EndpointSlice(EndpointSlice)`; `key()` dispatches to `metadata.key()` for all three (a Namespace's `metadata.namespace` is empty, so `key()` = `"/<name>"`, matching upstream `namespace.go`'s `"/" + name`).
  - `parse_object`/`parse_list` gain `"namespace"`, `"endpoints"`, `"endpointslice"` arms.
  - `K8sObject::target_groups` gets arms for the three new variants returning `Vec::new()` **in this task only** (real builders land in Tasks 3–4 for endpoints/endpointslice; `Namespace` permanently returns empty — upstream `namespace.go`'s `getTargetLabels` returns nil, namespaces are metadata-only).

**Reference:** `endpoints.go` (struct block), `endpointslice.go` (struct block), `namespace.go`, `pod.go` (`Container.RestartPolicy`). JSON casing: `notReadyAddresses`, `nodeName`, `targetRef`, `appProtocol`, `addressType` — all handled by `rename_all = "camelCase"`; `ip` is lowercase in JSON and maps cleanly.

- [ ] **Step 1: Write the failing tests** (append to `object.rs` tests)

```rust
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
    let K8sObject::Endpoints(eps) = &objs[0] else { panic!("wrong variant") };
    assert_eq!(eps.metadata.key(), "d/svc1");
    assert_eq!(eps.subsets[0].addresses[0].ip, "10.0.0.1");
    assert_eq!(eps.subsets[0].addresses[0].target_ref.name, "p1");
    assert_eq!(eps.subsets[0].not_ready_addresses[0].ip, "10.0.0.2");
    assert_eq!(eps.subsets[0].ports[0].port, 8080);
    assert!(objs[0].target_groups(&crate::scrape::kubernetes::registry::BuildCtx::detached()).is_empty());
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
    let K8sObject::EndpointSlice(eps) = &objs[0] else { panic!("wrong variant") };
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
    assert!(objs[0].target_groups(&crate::scrape::kubernetes::registry::BuildCtx::detached()).is_empty());
}

#[test]
fn parses_container_restart_policy() {
    let j = br#"{"name":"sidecar","restartPolicy":"Always"}"#;
    let c: Container = serde_json::from_slice(j).unwrap();
    assert_eq!(c.restart_policy, "Always");
}
```

Note: `BuildCtx::detached()` doesn't exist until Task 2. For THIS task, write the two `target_groups` assertions using the Phase A no-arg signature (`objs[0].target_groups()`); Task 2 mechanically updates every call site including these.

- [ ] **Step 2: Run to verify it fails** — `cargo test -p esmagent scrape::kubernetes::object` → FAIL (structs absent).
- [ ] **Step 3: Implement** the structs, variants, and parse arms following the existing pattern in `object.rs` exactly (each `parse_list` arm: deserialize the `*List`, take `metadata.resource_version`, map items into the variant).
- [ ] **Step 4: Run** — PASS; `RUSTFLAGS="-D warnings" cargo clippy -p esmagent --all-targets` clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent k8s SD endpoints/endpointslice/namespace object structs"`

---

## Task 2: ObjectRegistry + BuildCtx + attach_metadata joins for existing roles

**Files:**
- Create: `crates/esmagent/src/scrape/kubernetes/registry.rs`
- Modify: `crates/esmagent/src/scrape/kubernetes/mod.rs` (`pub mod registry;`, `KubernetesDiscovery` restructure)
- Modify: `crates/esmagent/src/scrape/kubernetes/object.rs` (`target_groups(&self, ctx: &BuildCtx)`)
- Modify: `crates/esmagent/src/scrape/kubernetes/roles/{node,pod,service,ingress}.rs` (ctx param; attach joins; helper extraction)
- Modify: `crates/esmagent/src/scrape/kubernetes/watcher.rs` (`target_groups(ctx)`, `cache()` accessor)
- Test: inline in `registry.rs` + updated role tests in `pod.rs`/`service.rs`/`ingress.rs`

**Interfaces:**
- Produces (`registry.rs`):

```rust
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use super::object::K8sObject;

/// Shared object cache view across every watcher a single
/// `KubernetesDiscovery` owns — the analog of upstream `groupWatcher`'s
/// `getObjectByRoleLocked` (`api_watcher.go`). Entries are registered once
/// at discovery construction; lookups scan entries matching role +
/// namespace scope. Never held across I/O: `get_object` locks one cache
/// just long enough to clone the `Arc`.
#[derive(Default)]
pub struct ObjectRegistry {
    entries: Vec<RegistryEntry>,
}

struct RegistryEntry {
    role: String,
    /// The watcher's namespace scope: `None` = cluster-wide.
    namespace: Option<String>,
    cache: Arc<Mutex<HashMap<String, Arc<K8sObject>>>>,
}

impl ObjectRegistry {
    pub fn register(
        &mut self,
        role: &str,
        namespace: Option<&str>,
        cache: Arc<Mutex<HashMap<String, Arc<K8sObject>>>>,
    ) {
        self.entries.push(RegistryEntry {
            role: role.to_string(),
            namespace: namespace.map(|s| s.to_string()),
            cache,
        });
    }

    /// Upstream `getObjectByRoleLocked`: `node`/`namespace` lookups force an
    /// empty namespace (cluster-scoped kinds, key `"/<name>"`); a namespaced
    /// entry only matches when its scope equals the requested namespace (a
    /// cluster-wide entry matches any).
    pub fn get_object(&self, role: &str, namespace: &str, name: &str) -> Option<Arc<K8sObject>> {
        let namespace = if role == "node" || role == "namespace" { "" } else { namespace };
        let key = format!("{namespace}/{name}");
        for e in &self.entries {
            if e.role != role {
                continue;
            }
            if !namespace.is_empty() {
                if let Some(scope) = &e.namespace {
                    if scope != namespace {
                        continue;
                    }
                }
            }
            if let Some(obj) = e.cache.lock().unwrap().get(&key) {
                return Some(Arc::clone(obj));
            }
        }
        None
    }
}

/// Everything a role builder needs beyond the object itself.
pub struct BuildCtx<'a> {
    pub registry: &'a ObjectRegistry,
    pub attach_node_metadata: bool,
    pub attach_namespace_metadata: bool,
}

impl BuildCtx<'_> {
    /// A ctx with an empty registry and no attach flags — Phase A behavior.
    /// Used by tests and any caller without cross-role state.
    pub fn detached() -> BuildCtx<'static> {
        static EMPTY: std::sync::OnceLock<ObjectRegistry> = std::sync::OnceLock::new();
        BuildCtx {
            registry: EMPTY.get_or_init(ObjectRegistry::default),
            attach_node_metadata: false,
            attach_namespace_metadata: false,
        }
    }
}
```

- `object.rs`: `pub fn target_groups(&self, ctx: &BuildCtx) -> Vec<TargetGroup>` — dispatch passes `ctx` to every role builder.
- `roles/pod.rs`:
  - `pub fn pod_target_groups(p: &Pod, ctx: &BuildCtx) -> Vec<TargetGroup>`
  - `pub(crate) fn append_common_labels(p: &Pod, ctx: &BuildCtx, labels: &mut BTreeMap<String, String>)` — extraction of the existing common-label block (namespace, pod_name/ip/ready/phase/node_name/host_ip/uid, controller kind/name, `register_labels_and_annotations("__meta_kubernetes_pod", ...)`) **plus** the two attach branches, ported from `pod.go` `appendCommonLabels`:

```rust
if ctx.attach_node_metadata {
    labels.insert(
        "__meta_kubernetes_node_name".to_string(),
        p.spec.node_name.clone(),
    );
    if let Some(o) = ctx.registry.get_object("node", &p.metadata.namespace, &p.spec.node_name) {
        if let K8sObject::Node(n) = &*o {
            register_labels_and_annotations("__meta_kubernetes_node", &n.metadata, labels);
        }
    }
}
if ctx.attach_namespace_metadata {
    if let Some(o) = ctx.registry.get_object("namespace", "", &p.metadata.namespace) {
        if let K8sObject::Namespace(ns) = &*o {
            register_labels_and_annotations("__meta_kubernetes_namespace", &ns.metadata, labels);
        }
    }
}
```

  - `pub(crate) fn append_container_labels(container: &Container, port: Option<&ContainerPort>, is_init: bool, labels: &mut BTreeMap<String, String>)` — extraction of the container image/name/init + port name/number/protocol block (`pod.go` `appendContainerLabels`).
  - `build_target_group` refactors to call both helpers (behavior for Phase A inputs unchanged: same keys, same values — the container-ID label stays where it is, gated on non-empty).
- `roles/service.rs`: `pub fn service_target_groups(s: &Service, ctx: &BuildCtx)`; `pub(crate) fn append_common_labels(s: &Service, ctx: &BuildCtx, labels: &mut BTreeMap<String, String>)` — extraction of namespace/service_name/service_type/cluster_ip-or-external_name + `register_labels_and_annotations("__meta_kubernetes_service", ...)`, plus the namespace attach branch (`service.go` lines 101–107, same shape as pod's).
- `roles/ingress.rs`: `ingress_target_groups(ig, ctx)`; namespace attach branch before `register_labels_and_annotations("__meta_kubernetes_ingress", ...)` (`ingress.go` lines 143–150).
- `roles/node.rs`: `node_target_groups(n, ctx)` — ctx accepted for signature uniformity, unused (`_ctx`); upstream node role has no attach joins.
- `watcher.rs`: `pub fn target_groups(&self, ctx: &BuildCtx) -> Vec<TargetGroup>`; new `pub fn cache(&self) -> Arc<Mutex<HashMap<String, Arc<K8sObject>>>>` (clone of the Arc, for registry registration).
- `mod.rs`: `KubernetesDiscovery` becomes:

```rust
pub struct KubernetesDiscovery {
    primary: Vec<Watcher>,
    /// Dependency-role watchers (attach_metadata / endpoints deps): feed the
    /// registry, never contribute target groups directly.
    deps: Vec<Watcher>,
    registry: ObjectRegistry,
    attach_node_metadata: bool,
    attach_namespace_metadata: bool,
}
```

  `new()`: resolve `(attach_node_metadata, attach_namespace_metadata)` from `cfg.attach_metadata` (absent → both false; the upstream global `-promscrape.kubernetes.attachNodeMetadataAll` flags are not ported). Start primary watchers as today, register each cache under `(cfg.role, namespace)`. Then start dependency watchers per `dependency_roles` (below) — in this task that's only the attach rules for `pod`/`service`/`ingress`; Task 6 extends it for endpoints roles. Every watcher (primary + dep) is passed `cfg.selectors` filtered to **that watcher's** role (upstream `joinSelectors` filters per role).

```rust
/// Upstream `startWatchersForRole`'s dependency rules. `true` = cluster-wide
/// (single `None`-namespace watcher); `false` = same namespace expansion as
/// the primary role.
fn dependency_roles(role: &str, attach_node: bool, attach_ns: bool) -> Vec<(&'static str, bool)> {
    let mut deps = Vec::new();
    if role == "endpoints" || role == "endpointslice" {
        deps.push(("pod", false));
        deps.push(("service", false));
    }
    if attach_node && matches!(role, "pod" | "endpoints" | "endpointslice") {
        deps.push(("node", true));
    }
    if attach_ns && matches!(role, "pod" | "service" | "endpoints" | "endpointslice" | "ingress") {
        deps.push(("namespace", true));
    }
    deps
}
```

  `poll()`: build a `BuildCtx` from `&self.registry` + the flags; flat_map over `self.primary` only.

**Reference:** `api_watcher.go` `getObjectByRoleLocked` (lines 438–458), `startWatchersForRole` (lines 460–502), `pod.go` `appendCommonLabels`/`appendContainerLabels`, `service.go` `appendCommonLabels`, `ingress.go` `getTargetLabels` attach branch. Ordering: attach labels are inserted **before** the pod's own labels in upstream's call order, but since key sets don't collide (different prefixes) and the map keeps last-write, replicating upstream's order inside each helper is sufficient.

- [ ] **Step 1: Write the failing tests**

In `registry.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::scrape::kubernetes::object::parse_list;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn cache_with(role: &str, list_json: &[u8]) -> Arc<Mutex<HashMap<String, Arc<K8sObject>>>> {
        let (objs, _) = parse_list(role, list_json).unwrap();
        let mut m = HashMap::new();
        for o in objs {
            m.insert(o.key(), Arc::new(o));
        }
        Arc::new(Mutex::new(m))
    }

    #[test]
    fn get_object_scopes_by_role_and_namespace() {
        let mut reg = ObjectRegistry::default();
        reg.register(
            "pod",
            Some("d"),
            cache_with("pod", br#"{"items":[{"metadata":{"name":"p1","namespace":"d"},
                "spec":{"containers":[{"name":"c"}]},
                "status":{"phase":"Running","podIP":"10.0.0.1"}}]}"#),
        );
        reg.register(
            "node",
            None,
            cache_with("node", br#"{"items":[{"metadata":{"name":"n1"}}]}"#),
        );
        assert!(reg.get_object("pod", "d", "p1").is_some());
        assert!(reg.get_object("pod", "other", "p1").is_none());
        assert!(reg.get_object("service", "d", "p1").is_none());
        // node lookup ignores the requested namespace (cluster-scoped key "/n1")
        assert!(reg.get_object("node", "d", "n1").is_some());
    }
}
```

In `roles/pod.rs` tests (add):

```rust
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
    assert_eq!(g[0].labels["__meta_kubernetes_node_labelpresent_zone"], "true");
    assert_eq!(g[0].labels["__meta_kubernetes_namespace_annotation_owner"], "t");
    // without the flags, none of these labels appear
    let g2 = pods[0].target_groups(&BuildCtx::detached());
    assert!(!g2[0].labels.contains_key("__meta_kubernetes_node_label_zone"));
    assert!(!g2[0].labels.contains_key("__meta_kubernetes_node_name"));
}
```

In `roles/service.rs` tests (add): a service in namespace `d` + a registered namespace `d` with a label, `attach_namespace_metadata: true` → `__meta_kubernetes_namespace_label_<x>` present; `BuildCtx::detached()` → absent. In `roles/ingress.rs` tests: same shape for ingress.

- [ ] **Step 2: Run to verify it fails** — `cargo test -p esmagent scrape::kubernetes` → FAIL (registry module absent; signature mismatches).
- [ ] **Step 3: Implement** `registry.rs`, the signature change through `object.rs`/`roles/*`/`watcher.rs`/`mod.rs`, the helper extractions, the attach branches, and the `KubernetesDiscovery` restructure + `dependency_roles`. Mechanically update every existing call site/test to pass `&BuildCtx::detached()` where no registry exists. `kubernetes_tests.rs`/`watcher_tests.rs` compile again with the ctx arg.
- [ ] **Step 4: Run** — `cargo test -p esmagent` PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent k8s SD shared object registry + attach_metadata joins"`

---

## Task 3: endpoints role builder

**Files:**
- Create: `crates/esmagent/src/scrape/kubernetes/roles/endpoints.rs`
- Modify: `roles/mod.rs` (`pub mod endpoints;`), `object.rs` (`K8sObject::Endpoints` arm dispatches to it)
- Test: inline in `endpoints.rs`

**Interfaces:**
- Consumes: `object::{Endpoints, EndpointAddress, EndpointPort, Pod, Service, Container, ContainerPort, K8sObject}`, `registry::BuildCtx`, `roles::pod::{append_common_labels as append_pod_common_labels, append_container_labels}`, `roles::service::append_common_labels as append_service_common_labels`, `roles::join_host_port`.
- Produces: `pub fn endpoints_target_groups(eps: &Endpoints, ctx: &BuildCtx) -> Vec<TargetGroup>`.

**Reference:** `endpoints.go` `getTargetLabels`/`appendEndpointLabelsForAddresses`/`getEndpointLabelsForAddressAndPort`/`getEndpointLabels`, `pod.go` `appendEndpointLabels`. Semantics to port exactly:

1. `svc` = registry `("service", eps.namespace, eps.name)` (endpoints and their service share the name).
2. For each subset × port: addresses with `ready="true"`, then `notReadyAddresses` with `ready="false"`. Per address:
   - target = `join_host_port(&ea.ip, epp.port)`; labels: `__meta_kubernetes_namespace`, `__meta_kubernetes_endpoints_name`, `__meta_kubernetes_endpoint_ready`, `__meta_kubernetes_endpoint_port_name`, `__meta_kubernetes_endpoint_port_protocol`; `endpoint_address_target_kind`/`_name` when `target_ref.kind` non-empty; `endpoint_node_name`/`endpoint_hostname` when non-empty.
   - then `append_service_common_labels` (if svc found), then `register_labels_and_annotations("__meta_kubernetes_endpoints", &eps.metadata, ...)`.
   - if `target_ref.kind == "Pod"` and the pod is in the registry (`("pod", target_ref.namespace, target_ref.name)`): `append_pod_common_labels`; record the pod in `pod_ports_seen: BTreeMap<String, (Arc<K8sObject>, Vec<i64>)>` (keyed by pod key — BTreeMap for deterministic fanout order; upstream keys by pointer); for each container in `spec.containers` whose some port == `epp.port` → record port + `append_container_labels(c, Some(cp), false, ...)` (inner-loop break only, keep scanning later containers — upstream does); same for `spec.init_containers` but **only when `restart_policy == "Always"`** (native sidecars), `is_init = true`.
3. Over-capacity annotation: `eps.metadata.annotations.get("endpoints.kubernetes.io/over-capacity")` — `"truncated"`/`"warning"` → `log::warn!` with upstream's message shape.
4. Skipped-ports fanout: for each seen pod (deterministic BTreeMap order), for `spec.containers` (`is_init=false`) and `spec.init_containers` with `restart_policy == "Always"` (`is_init=true`): every container port NOT in that pod's seen list emits one extra group: target = `join_host_port(&pod.status.pod_ip, cp.container_port)`; labels = pod common + container labels + (`__meta_kubernetes_endpoints_name` = eps name, `__meta_kubernetes_endpoint_address_target_kind` = `"Pod"`, `__meta_kubernetes_endpoint_address_target_name` = pod name, register endpoints meta) + service common (if svc).
5. Every group: `source = "kubernetes_sd/endpoints/<ns>/<name>"`.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::scrape::kubernetes::object::{parse_list, K8sObject};
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
        let ctx = BuildCtx { registry: &reg, attach_node_metadata: false, attach_namespace_metadata: false };
        let (objs, _) = parse_list("endpoints", EPS_LIST).unwrap();
        let g = objs[0].target_groups(&ctx);
        // 1 ready + 1 not-ready + 1 skipped-port (9090) on the seen pod
        assert_eq!(g.len(), 3);

        let ready = g.iter().find(|x| x.targets == vec!["10.0.0.1:8080".to_string()]).unwrap();
        assert_eq!(ready.labels["__meta_kubernetes_endpoint_ready"], "true");
        assert_eq!(ready.labels["__meta_kubernetes_endpoints_name"], "svc1");
        // service join
        assert_eq!(ready.labels["__meta_kubernetes_service_name"], "svc1");
        assert_eq!(ready.labels["__meta_kubernetes_service_label_app"], "a");
        // pod join (targetRef) incl. matched container port labels
        assert_eq!(ready.labels["__meta_kubernetes_pod_name"], "p1");
        assert_eq!(ready.labels["__meta_kubernetes_pod_container_port_number"], "8080");
        assert_eq!(ready.labels["__meta_kubernetes_endpoint_address_target_kind"], "Pod");
        assert_eq!(ready.source, "kubernetes_sd/endpoints/d/svc1");

        let notready = g.iter().find(|x| x.targets == vec!["10.0.0.9:8080".to_string()]).unwrap();
        assert_eq!(notready.labels["__meta_kubernetes_endpoint_ready"], "false");
        assert!(!notready.labels.contains_key("__meta_kubernetes_pod_name"));

        // the pod's 9090 port was not exposed via the endpoints object -> extra target
        let skipped = g.iter().find(|x| x.targets == vec!["10.0.0.1:9090".to_string()]).unwrap();
        assert_eq!(skipped.labels["__meta_kubernetes_pod_container_port_number"], "9090");
        assert_eq!(skipped.labels["__meta_kubernetes_endpoints_name"], "svc1");
        assert_eq!(skipped.labels["__meta_kubernetes_endpoint_address_target_kind"], "Pod");
        assert_eq!(skipped.labels["__meta_kubernetes_service_name"], "svc1");
        // skipped-port groups carry no endpoint_ready label
        assert!(!skipped.labels.contains_key("__meta_kubernetes_endpoint_ready"));
    }

    #[test]
    fn endpoints_without_registry_still_emit_address_targets() {
        let (objs, _) = parse_list("endpoints", EPS_LIST).unwrap();
        let g = objs[0].target_groups(&BuildCtx::detached());
        assert_eq!(g.len(), 2); // no pod/service in registry -> no joins, no fanout
        assert!(g.iter().all(|x| !x.labels.contains_key("__meta_kubernetes_service_name")));
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
        let ctx = BuildCtx { registry: &reg, attach_node_metadata: false, attach_namespace_metadata: false };
        let (objs, _) = parse_list("endpoints", EPS_LIST).unwrap();
        let g = objs[0].target_groups(&ctx);
        // sidecar's 7070 fans out; plain init's 6060 must NOT
        assert!(g.iter().any(|x| x.targets == vec!["10.0.0.1:7070".to_string()]));
        assert!(!g.iter().any(|x| x.targets == vec!["10.0.0.1:6060".to_string()]));
    }
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p esmagent scrape::kubernetes::roles::endpoints` → FAIL.
- [ ] **Step 3: Implement** per the Reference semantics above. Keep the file ≤ 800 lines. `K8sObject::Endpoints` arm in `object.rs` now calls `roles::endpoints::endpoints_target_groups(eps, ctx)`.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent k8s SD endpoints role"`

---

## Task 4: endpointslice role builder

**Files:**
- Create: `crates/esmagent/src/scrape/kubernetes/roles/endpointslice.rs`
- Modify: `roles/mod.rs`, `object.rs` (`K8sObject::EndpointSlice` arm)
- Test: inline in `endpointslice.rs`

**Interfaces:**
- Consumes: same helper set as Task 3 plus `labels::sanitize_label_name`.
- Produces: `pub fn endpointslice_target_groups(eps: &EndpointSlice, ctx: &BuildCtx) -> Vec<TargetGroup>`.

**Reference:** `endpointslice.go` `getTargetLabels`/`getEndpointSliceLabelsForAddressAndPort`/`getEndpointSliceLabels`, `pod.go` `appendEndpointSliceLabels`. Semantics:

1. `svc` = registry `("service", eps.namespace, metadata.labels["kubernetes.io/service-name"])` (empty label → lookup misses, no join).
2. For each endpoint `ess`: pod = registry `("pod", ess.target_ref.namespace, ess.target_ref.name)` (looked up once per endpoint, before the port loop). For each port × each address string:
   - target = `join_host_port(addr, epp.port)`; labels: `__meta_kubernetes_namespace`, `__meta_kubernetes_endpointslice_name`, `__meta_kubernetes_endpointslice_address_type`, `__meta_kubernetes_endpointslice_endpoint_conditions_ready`/`_serving`/`_terminating` (bool → `"true"`/`"false"`), `__meta_kubernetes_endpointslice_port_name`, `_port_protocol`, `_port` (the number as string); `_port_app_protocol` when non-empty; `_address_target_kind`/`_address_target_name` when kind non-empty; `_endpoint_hostname`/`_endpoint_node_name` when non-empty; per topology pair: `sanitize_label_name("__meta_kubernetes_endpointslice_endpoint_topology_" + k)` = v and `..._topology_present_" + k` = `"true"`.
   - then service common (if svc), then `register_labels_and_annotations("__meta_kubernetes_endpointslice", &eps.metadata, ...)`.
   - if `target_ref.kind == "Pod"` and pod found: record in `pod_ports_seen` **before** the container match (upstream does), `append_pod_common_labels`, then container port match over `spec.containers` (`is_init=false`) and `spec.init_containers` (`is_init=true`, **no** restart-policy check here — upstream's slice-path matching loop has none, unlike endpoints.go's).
3. Skipped-ports fanout: identical shape to Task 3's, but init containers DO require `restart_policy == "Always"` here, and the per-group extra labels come from `appendEndpointSliceLabels`: `__meta_kubernetes_endpointslice_name`, `_address_target_kind` = `"Pod"`, `_address_target_name` = pod name, `_address_type` = eps.address_type, + register endpointslice meta; then service common.
4. `source = "kubernetes_sd/endpointslice/<ns>/<name>"`.

- [ ] **Step 1: Write the failing tests**

```rust
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
        let ctx = BuildCtx { registry: &reg, attach_node_metadata: false, attach_namespace_metadata: false };
        let (objs, _) = parse_list("endpointslice", SLICE_LIST).unwrap();
        let g = objs[0].target_groups(&ctx);
        assert_eq!(g.len(), 2); // 1 address target + 1 skipped-port (81)

        let main = g.iter().find(|x| x.targets == vec!["10.0.0.3:80".to_string()]).unwrap();
        assert_eq!(main.labels["__meta_kubernetes_endpointslice_name"], "svc1-abc");
        assert_eq!(main.labels["__meta_kubernetes_endpointslice_address_type"], "IPv4");
        assert_eq!(main.labels["__meta_kubernetes_endpointslice_endpoint_conditions_ready"], "true");
        assert_eq!(main.labels["__meta_kubernetes_endpointslice_endpoint_conditions_terminating"], "false");
        assert_eq!(main.labels["__meta_kubernetes_endpointslice_port"], "80");
        assert_eq!(main.labels["__meta_kubernetes_endpointslice_port_app_protocol"], "http");
        assert_eq!(main.labels["__meta_kubernetes_endpointslice_endpoint_topology_kubernetes_io_hostname"], "n2");
        assert_eq!(main.labels["__meta_kubernetes_endpointslice_endpoint_topology_present_kubernetes_io_hostname"], "true");
        // service-name label join
        assert_eq!(main.labels["__meta_kubernetes_service_name"], "svc1");
        // pod join
        assert_eq!(main.labels["__meta_kubernetes_pod_name"], "p2");
        assert_eq!(main.labels["__meta_kubernetes_pod_container_port_number"], "80");
        assert_eq!(main.source, "kubernetes_sd/endpointslice/d/svc1-abc");

        let skipped = g.iter().find(|x| x.targets == vec!["10.0.0.3:81".to_string()]).unwrap();
        assert_eq!(skipped.labels["__meta_kubernetes_endpointslice_address_target_kind"], "Pod");
        assert_eq!(skipped.labels["__meta_kubernetes_endpointslice_address_type"], "IPv4");
    }

    #[test]
    fn endpointslice_without_service_name_label_skips_service_join() {
        let no_label: &[u8] = br#"{"items":[{"metadata":{"name":"s-x","namespace":"d"},
            "addressType":"IPv4",
            "endpoints":[{"addresses":["10.0.0.5"],"conditions":{"ready":true}}],
            "ports":[{"name":"w","port":80,"protocol":"TCP"}]}]}"#;
        let reg = reg_with(&[("service", SVC_LIST)]);
        let ctx = BuildCtx { registry: &reg, attach_node_metadata: false, attach_namespace_metadata: false };
        let (objs, _) = parse_list("endpointslice", no_label).unwrap();
        let g = objs[0].target_groups(&ctx);
        assert_eq!(g.len(), 1);
        assert!(!g[0].labels.contains_key("__meta_kubernetes_service_name"));
        // no target_ref -> no target kind labels
        assert!(!g[0].labels.contains_key("__meta_kubernetes_endpointslice_address_target_kind"));
    }
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** per the Reference semantics.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent k8s SD endpointslice role"`

---

## Task 5: API paths for endpoints/endpointslices/namespaces + discovery v1beta1 fallback

**Files:**
- Modify: `crates/esmagent/src/scrape/kubernetes/client.rs` (role → URL path arms)
- Modify: `crates/esmagent/src/scrape/kubernetes/watcher.rs` (generalize the v1beta1 fallback)
- Test: inline in both

**Interfaces:**
- `client.rs`'s role→path mapping (find the existing match over roles inside `list_url`/`watch_url` or its helper) gains: `endpoints` → `/api/v1[/namespaces/<ns>]/endpoints`; `endpointslice` → `/apis/discovery.k8s.io/v1[/namespaces/<ns>]/endpointslices`; `namespace` → `/api/v1/namespaces` (cluster-scoped: the namespace param is ignored, like `node`).
- `watcher.rs`: `apply_v1beta1_fallback(url: &str, role: &str, use_v1beta1: bool) -> String` — for `ingress` replaces `/networking.k8s.io/v1/`→`/networking.k8s.io/v1beta1/`, for `endpointslice` replaces `/discovery.k8s.io/v1/`→`/discovery.k8s.io/v1beta1/` (upstream `useDiscoveryV1Beta1`, `api_watcher.go` lines 510–533). `get_with_v1beta1_fallback`'s 404-flip condition becomes `matches!(ctx.role.as_str(), "ingress" | "endpointslice")`.

**Reference:** `api_watcher.go` `getAPIPath` (lines 1015–1030), `getObjectTypeByRole`, `doRequest`'s v1beta1 substitutions.

- [ ] **Step 1: Write the failing tests**

In `client.rs` tests (mirror the existing `list_and_watch_urls_include_namespace_and_selectors` helpers):

```rust
#[test]
fn endpoints_endpointslice_namespace_urls() {
    let ac = api_config_for_test("https://api:6443");
    let lu = ac.list_url("endpoints", Some("prod"), &[], None);
    assert!(lu.starts_with("https://api:6443/api/v1/namespaces/prod/endpoints?"));
    let lu2 = ac.list_url("endpointslice", Some("prod"), &[], None);
    assert!(lu2.starts_with("https://api:6443/apis/discovery.k8s.io/v1/namespaces/prod/endpointslices?"));
    let lu3 = ac.list_url("endpointslice", None, &[], None);
    assert!(lu3.starts_with("https://api:6443/apis/discovery.k8s.io/v1/endpointslices?"));
    let lu4 = ac.list_url("namespace", None, &[], None);
    assert!(lu4.starts_with("https://api:6443/api/v1/namespaces?"));
    let wu = ac.watch_url("endpointslice", Some("d"), &[], "7", 60);
    assert!(wu.starts_with("https://api:6443/apis/discovery.k8s.io/v1/namespaces/d/endpointslices?"));
    assert!(wu.contains("watch=1"));
}
```

In `watcher.rs` tests (unit, no server needed):

```rust
#[test]
fn v1beta1_fallback_rewrites_discovery_and_networking_paths() {
    assert_eq!(
        apply_v1beta1_fallback("https://a/apis/discovery.k8s.io/v1/endpointslices?x", "endpointslice", true),
        "https://a/apis/discovery.k8s.io/v1beta1/endpointslices?x"
    );
    assert_eq!(
        apply_v1beta1_fallback("https://a/apis/networking.k8s.io/v1/ingresses", "ingress", true),
        "https://a/apis/networking.k8s.io/v1beta1/ingresses"
    );
    // flag off -> untouched
    assert_eq!(
        apply_v1beta1_fallback("https://a/apis/discovery.k8s.io/v1/endpointslices", "endpointslice", false),
        "https://a/apis/discovery.k8s.io/v1/endpointslices"
    );
}
```

(Adjust the existing `apply_v1beta1_fallback` call sites/tests for the new `role` parameter.)

- [ ] **Step 2: Run to verify it fails** — FAIL (unknown roles in path builder; signature).
- [ ] **Step 3: Implement**.
- [ ] **Step 4: Run** — `cargo test -p esmagent scrape::kubernetes` PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent k8s SD endpoints/endpointslice/namespace API paths + discovery v1beta1 fallback"`

---

## Task 6: config acceptance + discovery wiring for endpoints roles

**Files:**
- Modify: `crates/esmagent/src/scrape/config.rs` (`validate_kubernetes_sd_config`, role alias normalization in `build_kubernetes_sd_config`, module doc)
- Modify: `crates/esmagent/src/scrape/kubernetes/mod.rs` (module doc note; `dependency_roles` already handles endpoints — verify the wiring end to end)
- Test: inline in `config.rs` + `kubernetes_tests.rs`

**Interfaces:**
- `build_kubernetes_sd_config` normalizes `role: endpointslices` → `"endpointslice"` (upstream `SDConfig.role()`, kept for VictoriaMetrics-operator compat).
- `validate_kubernetes_sd_config` accepts `pod|node|service|ingress|endpoints|endpointslice`; the unknown-role error becomes upstream's: `unexpected role: <r>; must be one of node, pod, service, endpoints, endpointslice or ingress`. The Phase-B-deferral arm is deleted. `kubeconfig_file` rejections unchanged.

**Reference:** `api.go` `newAPIConfig` role switch, `kubernetes.go` `role()`.

- [ ] **Step 1: Write the failing tests**

In `config.rs` (replace the old `rejects_deferred_and_bad_k8s_roles` Phase-B assertion):

```rust
#[test]
fn accepts_endpoints_roles_and_normalizes_alias() {
    let y = "scrape_configs:\n  - job_name: j\n    kubernetes_sd_configs: [{role: endpoints}, {role: endpointslices}]\n";
    let c = parse_scrape_config(y).unwrap();
    assert_eq!(c.scrape_configs[0].kubernetes_sd_configs[0].role, "endpoints");
    assert_eq!(c.scrape_configs[0].kubernetes_sd_configs[1].role, "endpointslice");
    validate(&c).unwrap();
    let bad = "scrape_configs:\n  - job_name: j\n    kubernetes_sd_configs: [{role: bogus}]\n";
    let err = validate(&parse_scrape_config(bad).unwrap()).unwrap_err();
    assert!(err.msg.contains("endpointslice"));
}
```

In `kubernetes_tests.rs` (stub-based, mirroring the existing `kubernetes_discovery_polls_targets_from_stub` — the stub must now serve three LIST paths; extend the existing stub helper so its script maps a path substring to a body, e.g. `("/endpoints?", EPS_BODY)`, `("/pods?", POD_BODY)`, `("/services?", SVC_BODY)`, default watch = hang/close):

```rust
#[test]
fn endpoints_discovery_joins_pod_and_service_from_dep_watchers() {
    // stub serves: endpoints list (one address targeting pod p1), pods list (p1), services list (svc1)
    let stub = start_k8s_stub_multi(vec![
        ("/api/v1/namespaces/d/endpoints", EPS_LIST_BODY),
        ("/api/v1/namespaces/d/pods", POD_LIST_BODY),
        ("/api/v1/namespaces/d/services", SVC_LIST_BODY),
    ]);
    let mut cfg = k8s_cfg_role("endpoints");
    cfg.api_server = Some(stub.base_url());
    cfg.namespaces.names = vec!["d".into()];
    let mut d = KubernetesDiscovery::new(&cfg).unwrap();
    assert!(wait_until(Duration::from_secs(5), || {
        d.poll().iter().any(|g| {
            g.labels.get("__meta_kubernetes_service_name").map(String::as_str) == Some("svc1")
                && g.labels.get("__meta_kubernetes_pod_name").map(String::as_str) == Some("p1")
        })
    }));
    stub.stop();
}
```

(Reuse Task 3's JSON fixtures for the three bodies; exact stub-helper shape follows whatever `watcher_tests.rs`'s existing stub provides — extend it, don't fork it.)

- [ ] **Step 2: Run to verify it fails** — FAIL (roles still rejected).
- [ ] **Step 3: Implement** the config change + any wiring gap the stub test exposes (dependency watchers for endpoints were added in Task 2's `dependency_roles`; this test proves them live).
- [ ] **Step 4: Run** — `cargo test -p esmagent` PASS; clippy clean; `cargo build -p esmagent --target x86_64-pc-windows-gnu`.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent accept kubernetes_sd endpoints/endpointslice roles"`

---

## Task 7: e2e + docs + memory

**Files:**
- Modify: `crates/esmagent/tests/kubernetes_sd_e2e.rs` (add an endpoints-role + attach_metadata scenario)
- Modify: `crates/esmagent/README.md` (k8s SD section: Phase B now covered; prune the limitations list)
- Modify: `docs/PORTING.md` (row 78 + the vmagent scope note: k8s SD Phase B shipped; remaining k8s gaps = `kubeconfig_file`, OAuth2, `proxy_url`, global attach-all flags, cross-config watcher dedup)
- Modify: `crates/esmagent/src/scrape/config.rs` module doc (Phase B note)
- Test: `cargo test -p esmagent --test kubernetes_sd_e2e`

**Interfaces:** none new.

- [ ] **Step 1: Write the failing e2e test** — extend the existing harness: k8s stub serving an `endpoints` LIST whose address targets a live stub `/metrics` server (plus `pods`/`services` LISTs so the joins land), `-promscrape.config` with `kubernetes_sd_configs: [{role: endpoints, api_server: <stub>, namespaces: {names: [d]}, attach_metadata: {node: true}}]` + a node LIST, relabel `__meta_kubernetes_service_name` → an asserted label, `-remoteWrite.url` → capture; bounded-poll until the destination receives `up==1` carrying the service-derived label; `/api/v1/targets` shows the target; clean stop.
- [ ] **Step 2: Run to verify it fails** — only if a wiring gap exists; if it passes immediately, verify the assertions actually exercise the joins (flip a label name to prove it can fail, then restore).
- [ ] **Step 3: Docs** — honest updates in all three files: Phase B (endpoints/endpointslice roles, shared registry, attach_metadata node+namespace joins) shipped; still out: `kubeconfig_file`, OAuth2 scrape auth, `proxy_url`, `-promscrape.kubernetes.attachNodeMetadataAll`/`attachNamespaceMetadataAll`, upstream's cross-config groupWatcher dedup (per-config watchers here), `-promscrape.kubernetes.apiServerTimeout` (fixed 60s watch timeout).
- [ ] **Step 4: Run** — `cargo test --workspace` green; `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets`; `cargo fmt --check`; windows-gnu compile.
- [ ] **Step 5: Commit** — `git commit -m "test: esmagent k8s SD endpoints e2e; docs: Phase B coverage"`

---

## Final verification (after Task 7, before merge)

- [ ] `cargo test --workspace` green on Linux; push and watch GitHub Actions until green on BOTH platforms (memory: `check-ci-after-push`).
- [ ] Whole-branch review (subagent, most capable model) — focus: registry lock never held across I/O; no panics in builders on missing registry objects; endpoints/endpointslice label sets byte-match upstream (spot-check against `endpoints_test.go`/`endpointslice_test.go` expectations); the endpoints-vs-endpointslice init-container `RestartPolicy` asymmetry preserved; dep watchers stop cleanly (no `stop()` hang with 5+ watchers); `BuildCtx::detached()` call sites can't mask a missing registry in production paths (only `poll()` builds groups, and it always passes the real registry).
- [ ] No esmetrics ingest/query hot-path impact → no benchmark re-validation.
- [ ] Update memory: `esmagent-k8s-sd-port.md` (Phase B shipped; remaining gaps list) + MEMORY.md hook line.
