# esmagent Kubernetes SD Phase A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `kubernetes_sd_configs` (roles `pod`/`node`/`service`/`ingress`) to esmagent's scrape engine via a no-tokio blocking Kubernetes list+watch client feeding the existing `Discovery` trait.

**Architecture:** New submodule `crates/esmagent/src/scrape/kubernetes/`. A background thread per `(role, namespace)` maintains an in-memory object cache (LIST then streamed WATCH long-poll); `poll()` builds `TargetGroup`s from the cache snapshot — cheap, non-blocking, identical contract to the existing SD providers. No change to the `Discovery` trait, `TargetGroup`, the manager reconcile loop, or the CLI.

**Tech Stack:** Rust (edition 2021, rust-version 1.85), `reqwest::blocking`, `serde`/`serde_json`, `serde_yaml_ng`, the existing `scrape::discovery::{Discovery, TargetGroup}` + `client::{AuthConfig, TlsConfig}`.

## Porting Convention (read before every task)

Faithful port of `/home/test/refsrc/VictoriaMetrics/lib/promscrape/discovery/kubernetes/` @ v1.146.0. When a task cites `<file>.go`, read and translate faithfully. Spec: `docs/superpowers/specs/2026-07-12-esmagent-k8s-sd-phase-a-design.md`.

Reference the existing scrape code the new module mirrors: `crates/esmagent/src/scrape/discovery.rs` (the `Discovery` trait, `TargetGroup`, `build_http_client`, the http_sd stub-server test), `crates/esmagent/src/client.rs` (`AuthConfig`/`TlsConfig`, `build_client`'s reqwest TLS wiring, the `stop: Arc<AtomicBool>` + `JoinHandle` worker pattern), `crates/esmagent/src/scrape/manager.rs` (`build_providers`/`build_job`), `crates/esmagent/src/scrape/config.rs` (`ScrapeConfig`, `reject_unsupported_keys`, the `#[serde(flatten)] extra` raw-parse pattern).

## Global Constraints

- **No tokio** — `reqwest::blocking` + std threads; the watch is a blocking streamed long-poll read.
- Faithful to upstream v1.146.0 k8s SD behavior for the 4 roles.
- Files ≤ 800 lines; `cargo fmt` clean; `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets` clean; `cargo build -p esmagent --target x86_64-pc-windows-gnu` compiles.
- Never log secrets (bearer token, basic password, client key); usernames only.
- Never panic in a watch thread, `poll()`, or the reconcile path — I/O/decode errors log + retry (re-LIST / bounded backoff) or return last-good/empty.
- `TargetGroup { targets: Vec<String>, labels: BTreeMap<String,String>, source: String }`: a role builder emits one `TargetGroup` per (object, target-address) with `targets = vec![address]` and `labels =` all `__meta_kubernetes_*`/`instance` labels **except** `__address__` (the manager's `build_targets` sets `__address__` from the target string).
- `esm_relabel::Label`/relabel are unchanged. Commit style `<type>: <description>`, no attribution trailers.
- After the final task: `cargo test --workspace` green, push, watch GitHub Actions (Windows tests run only in CI), fix failures.

---

## Task 1: k8s object structs + label helpers

**Files:**
- Create: `crates/esmagent/src/scrape/kubernetes/mod.rs` (module scaffold: `pub mod object; pub mod labels;` — later tasks add more)
- Create: `crates/esmagent/src/scrape/kubernetes/object.rs`
- Create: `crates/esmagent/src/scrape/kubernetes/labels.rs`
- Modify: `crates/esmagent/src/scrape/mod.rs` (add `pub mod kubernetes;`)
- Test: inline in `object.rs` + `labels.rs`

**Interfaces:**
- Consumes: nothing (leaf module).
- Produces:
  - `object.rs`: `pub struct ObjectMeta { pub name: String, pub namespace: String, pub uid: String, pub labels: BTreeMap<String,String>, pub annotations: BTreeMap<String,String>, pub owner_references: Vec<OwnerReference> }`; `pub struct OwnerReference { pub name: String, pub kind: String, pub controller: bool }`. `ObjectMeta::key(&self) -> String` = `format!("{}/{}", namespace, name)`. `pub struct ListMeta { pub resource_version: String }`. `pub struct WatchEvent { pub event_type: String, pub object: serde_json::Value }` (serde rename `type`→`event_type`, `object`). All `#[derive(Debug, Clone, Default, Deserialize)]` with `#[serde(default, rename_all = "camelCase")]` so absent JSON fields default and k8s camelCase (`resourceVersion`, `ownerReferences`) maps.
  - `labels.rs`: `pub fn sanitize_label_name(name: &str) -> String` (every char not in `[a-zA-Z0-9_]` → `_`); `pub fn register_labels_and_annotations(prefix: &str, meta: &ObjectMeta, out: &mut BTreeMap<String,String>)`.

**Reference:** `common_types.go` (`ObjectMeta`, `ListMeta`, `registerLabelsAndAnnotations`, `OwnerReference`), `api_watcher.go` (`WatchEvent`), `discoveryutil/util.go` (`SanitizeLabelName` = replace `[^a-zA-Z0-9_]` with `_`). Note upstream sanitizes the WHOLE constructed name `<prefix>_label_<labelname>`, so the prefix+separators (already valid) pass through and only invalid chars in `<labelname>` become `_`.

- [ ] **Step 1: Write the failing tests**

```rust
// in labels.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::scrape::kubernetes::object::ObjectMeta;

    #[test]
    fn sanitizes_invalid_label_chars() {
        assert_eq!(sanitize_label_name("app.kubernetes.io/name"), "app_kubernetes_io_name");
        assert_eq!(sanitize_label_name("plain_ok9"), "plain_ok9");
    }

    #[test]
    fn registers_labels_annotations_and_present_markers() {
        let mut meta = ObjectMeta::default();
        meta.labels.insert("app.kubernetes.io/name".into(), "web".into());
        meta.annotations.insert("prometheus.io/scrape".into(), "true".into());
        let mut out = std::collections::BTreeMap::new();
        register_labels_and_annotations("__meta_kubernetes_pod", &meta, &mut out);
        assert_eq!(out["__meta_kubernetes_pod_label_app_kubernetes_io_name"], "web");
        assert_eq!(out["__meta_kubernetes_pod_labelpresent_app_kubernetes_io_name"], "true");
        assert_eq!(out["__meta_kubernetes_pod_annotation_prometheus_io_scrape"], "true");
        assert_eq!(out["__meta_kubernetes_pod_annotationpresent_prometheus_io_scrape"], "true");
    }
}
```
```rust
// in object.rs
#[cfg(test)]
mod tests {
    use super::*;

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
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p esmagent scrape::kubernetes` → FAIL (module absent).

- [ ] **Step 3: Implement** `object.rs` + `labels.rs`.

```rust
// labels.rs
use std::collections::BTreeMap;
use super::object::ObjectMeta;

pub fn sanitize_label_name(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}

pub fn register_labels_and_annotations(
    prefix: &str,
    meta: &ObjectMeta,
    out: &mut BTreeMap<String, String>,
) {
    for (name, value) in &meta.labels {
        out.insert(sanitize_label_name(&format!("{prefix}_label_{name}")), value.clone());
        out.insert(sanitize_label_name(&format!("{prefix}_labelpresent_{name}")), "true".into());
    }
    for (name, value) in &meta.annotations {
        out.insert(sanitize_label_name(&format!("{prefix}_annotation_{name}")), value.clone());
        out.insert(sanitize_label_name(&format!("{prefix}_annotationpresent_{name}")), "true".into());
    }
}
```
```rust
// object.rs — structs per the Interfaces block. Example shape:
use std::collections::BTreeMap;
use serde::Deserialize;

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
    pub fn key(&self) -> String { format!("{}/{}", self.namespace, self.name) }
}
// OwnerReference { name, kind, controller }, ListMeta { resource_version },
// WatchEvent { #[serde(rename="type")] event_type: String, object: serde_json::Value }
// — all #[derive(Debug, Clone, Default, Deserialize)] #[serde(default, rename_all="camelCase")].
```

- [ ] **Step 4: Run** — `cargo test -p esmagent scrape::kubernetes` PASS; `RUSTFLAGS="-D warnings" cargo clippy -p esmagent --all-targets`.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent k8s SD object structs + label helpers"`

---

## Task 2: kubernetes_sd_configs config parse + validate

**Files:**
- Modify: `crates/esmagent/src/scrape/config.rs` (remove `"kubernetes_sd_configs"` from `CLOUD_SD_KEYS`; add typed field + struct + validation)
- Test: inline in `config.rs`

**Interfaces:**
- Consumes: `client::{AuthConfig, TlsConfig}` (already imported in config.rs).
- Produces:
  - `pub struct KubernetesSdConfig { pub role: String, pub api_server: Option<String>, pub kubeconfig_file: Option<String>, pub namespaces: K8sNamespaces, pub selectors: Vec<K8sSelector>, pub attach_metadata: Option<K8sAttachMetadata>, pub auth: AuthConfig, pub tls: TlsConfig }`
  - `pub struct K8sNamespaces { pub own_namespace: bool, pub names: Vec<String> }`
  - `pub struct K8sSelector { pub role: String, pub label: Option<String>, pub field: Option<String> }`
  - `pub struct K8sAttachMetadata { pub node: bool, pub namespace: bool }`
  - New field on `ScrapeConfig`: `pub kubernetes_sd_configs: Vec<KubernetesSdConfig>`.
- `parse_scrape_config` populates it; `validate` checks each entry (see Reference).

**Reference:** `kubernetes.go` (`SDConfig` fields, `role()` — note `endpointslices`→`endpointslice` alias), `api.go` `newAPIConfig` (role must be one of node/pod/service/endpoints/endpointslice/ingress; `api_server`+`kubeconfig_file` together is an error). Phase A validation: `role` ∈ {pod,node,service,ingress} OK; `endpoints`/`endpointslice`/`endpointslices` → `"unsupported (deferred to Phase B): role <r>"`; any other role → `"unexpected role: <r>; must be one of pod, node, service, ingress"`; `kubeconfig_file` set → `"unsupported (deferred): kubeconfig_file"`; `api_server` and `kubeconfig_file` both set → the upstream mutual-exclusion error. Parse via the same raw-YAML→typed pattern config.rs already uses for relabel/auth (the `RawScrapeConfig`/`#[serde(flatten)] extra` struct + `into_auth_config`); reuse `into_auth_config`/`into_tls` helpers for the inline auth/tls (grep how `HttpSdConfig` parses its `auth`/`tls`).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn parses_and_validates_kubernetes_sd() {
    let y = r#"
scrape_configs:
  - job_name: k8s
    kubernetes_sd_configs:
      - role: pod
        namespaces: { names: [default, kube-system] }
        selectors:
          - { role: pod, label: "app=web" }
"#;
    let c = parse_scrape_config(y).unwrap();
    let k = &c.scrape_configs[0].kubernetes_sd_configs[0];
    assert_eq!(k.role, "pod");
    assert_eq!(k.namespaces.names, vec!["default".to_string(), "kube-system".to_string()]);
    assert_eq!(k.selectors[0].label.as_deref(), Some("app=web"));
    validate(&c).unwrap();
}

#[test]
fn rejects_deferred_and_bad_k8s_roles() {
    let ep = "scrape_configs:\n  - job_name: j\n    kubernetes_sd_configs: [{role: endpoints}]\n";
    assert!(validate(&parse_scrape_config(ep).unwrap()).unwrap_err().msg.contains("Phase B"));
    let bad = "scrape_configs:\n  - job_name: j\n    kubernetes_sd_configs: [{role: bogus}]\n";
    assert!(validate(&parse_scrape_config(bad).unwrap()).is_err());
    let kc = "scrape_configs:\n  - job_name: j\n    kubernetes_sd_configs: [{role: pod, kubeconfig_file: /x}]\n";
    assert!(validate(&parse_scrape_config(kc).unwrap()).unwrap_err().msg.contains("kubeconfig_file"));
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p esmagent scrape::config` → FAIL.
- [ ] **Step 3: Implement** the struct(s), the `RawKubernetesSdConfig` raw-parse (mirroring `HttpSdConfig`'s auth/tls handling), wire into `ScrapeConfig`/`parse_scrape_config`, add the per-entry checks to `validate`, and remove `"kubernetes_sd_configs"` from `CLOUD_SD_KEYS`. Keep the other cloud-SD keys rejected.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent kubernetes_sd_configs parse + validate"`

---

## Task 3: node role builder (+ the object enum & TargetGroup-per-target pattern)

**Files:**
- Create: `crates/esmagent/src/scrape/kubernetes/roles/mod.rs` (`pub mod node;` — later tasks add pod/service/ingress)
- Create: `crates/esmagent/src/scrape/kubernetes/roles/node.rs`
- Modify: `crates/esmagent/src/scrape/kubernetes/object.rs` (add `Node`/`NodeList` structs + the `K8sObject` enum + `parse_list`/`parse_object`)
- Modify: `crates/esmagent/src/scrape/kubernetes/mod.rs` (`pub mod roles;`)
- Test: inline in `node.rs`

**Interfaces:**
- Consumes: `object::{ObjectMeta, ListMeta, WatchEvent}`, `labels::register_labels_and_annotations`, `scrape::discovery::TargetGroup`.
- Produces (`object.rs`):
  - `pub enum K8sObject { Node(Node), Pod(Pod), Service(Service), Ingress(Ingress) }` (Pod/Service/Ingress variants added by Tasks 4-6; add `Node` now and the others as unit-carrying variants when their structs land — for Task 3 the enum has just `Node`).
  - `impl K8sObject { pub fn key(&self) -> String; pub fn target_groups(&self) -> Vec<TargetGroup> }` (dispatches to the role builder).
  - `pub fn parse_object(role: &str, data: &[u8]) -> Result<K8sObject, String>` and `pub fn parse_list(role: &str, data: &[u8]) -> Result<(Vec<K8sObject>, String), String>` (returns objects + `metadata.resourceVersion`). `role` picks the variant; an unknown role → `Err`.
  - `pub struct Node { pub metadata: ObjectMeta, pub spec: NodeSpec, pub status: NodeStatus }` with `NodeSpec { provider_id: String }`, `NodeStatus { addresses: Vec<NodeAddress>, daemon_endpoints: NodeDaemonEndpoints }`, `NodeAddress { address_type: String (serde rename "type"), address: String }`, `NodeDaemonEndpoints { kubelet_endpoint: DaemonEndpoint }`, `DaemonEndpoint { port: i64 }` — all `#[serde(default, rename_all="camelCase")]`.
- Produces (`node.rs`): `pub fn node_target_groups(n: &Node) -> Vec<TargetGroup>` (0 or 1 group).

**Reference:** `node.go` `getTargetLabels`/`getNodeAddr`. Address = first of InternalIP→InternalDNS→ExternalIP→ExternalDNS→Hostname (port them all; Hostname last), else the node is skipped (no group). `__address__` (the target string) = `JoinHostPort(addr, kubeletPort)` (IPv6 host → `[addr]:port`). Labels: `instance` = node name; `__meta_kubernetes_node_name`; `__meta_kubernetes_node_provider_id`; `__meta_kubernetes_node_address_<Type>` = address for each distinct address Type (`sanitize_label_name` the whole key); + `register_labels_and_annotations("__meta_kubernetes_node", meta)`. `source = "kubernetes_sd/node/<namespace>/<name>"` (nodes have empty namespace → `kubernetes_sd/node//<name>`).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn node_target_group_addresses_and_labels() {
    let j = br#"{"metadata":{"name":"n1","labels":{"kubernetes.io/hostname":"n1"}},
        "spec":{"providerID":"aws:///i-123"},
        "status":{"addresses":[{"type":"InternalIP","address":"10.0.0.5"},
                               {"type":"Hostname","address":"n1.local"}],
                  "daemonEndpoints":{"kubeletEndpoint":{"port":10250}}}}"#;
    let (objs, rv) = crate::scrape::kubernetes::object::parse_list(
        "node", format!("{{\"metadata\":{{\"resourceVersion\":\"7\"}},\"items\":[{}]}}",
            std::str::from_utf8(j).unwrap()).as_bytes()).unwrap();
    assert_eq!(rv, "7");
    let g = objs[0].target_groups();
    assert_eq!(g.len(), 1);
    assert_eq!(g[0].targets, vec!["10.0.0.5:10250".to_string()]);
    assert_eq!(g[0].labels["instance"], "n1");
    assert_eq!(g[0].labels["__meta_kubernetes_node_name"], "n1");
    assert_eq!(g[0].labels["__meta_kubernetes_node_provider_id"], "aws:///i-123");
    assert_eq!(g[0].labels["__meta_kubernetes_node_address_InternalIP"], "10.0.0.5");
    assert_eq!(g[0].labels["__meta_kubernetes_node_address_Hostname"], "n1.local");
    assert_eq!(g[0].labels["__meta_kubernetes_node_label_kubernetes_io_hostname"], "n1");
    assert!(!g[0].labels.contains_key("__address__")); // address is the target, not a label
}

#[test]
fn node_without_address_is_skipped() {
    let (objs, _) = crate::scrape::kubernetes::object::parse_list(
        "node", br#"{"items":[{"metadata":{"name":"n2"},"status":{}}]}"#).unwrap();
    assert!(objs[0].target_groups().is_empty());
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** the `Node` structs, the `K8sObject` enum + `parse_object`/`parse_list` + `key`/`target_groups`, and `node_target_groups`. Provide a small `join_host_port(host, port)` helper in `roles/mod.rs` (IPv6 host detection = contains `:` and no `]` → wrap in `[...]`; reused by pod/service later).
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent k8s SD node role"`

---

## Task 4: pod role builder

**Files:**
- Create: `crates/esmagent/src/scrape/kubernetes/roles/pod.rs`
- Modify: `object.rs` (add `Pod`/`PodList` structs + `K8sObject::Pod` variant + `parse_*` role arm), `roles/mod.rs` (`pub mod pod;`)
- Test: inline in `pod.rs`

**Interfaces:**
- Produces (`object.rs`): `pub struct Pod { pub metadata: ObjectMeta, pub spec: PodSpec, pub status: PodStatus }`; `PodSpec { node_name: String, containers: Vec<Container>, init_containers: Vec<Container> }`; `PodStatus { phase: String, pod_ip: String, host_ip: String, conditions: Vec<PodCondition>, container_statuses: Vec<ContainerStatus>, init_container_statuses: Vec<ContainerStatus> }`; `Container { name: String, image: String, ports: Vec<ContainerPort> }`; `ContainerPort { name: String, container_port: i64, protocol: String }`; `PodCondition { condition_type: String (rename "type"), status: String }`; `ContainerStatus { name: String, container_id: String, state: ContainerState }`; `ContainerState { terminated: Option<serde_json::Value> }` (presence = terminated). All `#[serde(default, rename_all="camelCase")]`.
- Produces (`pod.rs`): `pub fn pod_target_groups(p: &Pod) -> Vec<TargetGroup>`.

**Reference:** `pod.go` `getTargetLabels`/`appendPodLabels`/`appendPodLabelsInternal`. Skip pod if `pod_ip` empty OR phase ∈ {Succeeded,Failed}. For each container (regular then init, `_container_init` = "false"/"true"): skip if its `ContainerStatus.state.terminated` is present; emit one target per `ports` entry, and one portless target if `ports` is empty. `__address__` (target) = portless: `pod_ip` (IPv6-escaped); with port: `JoinHostPort(pod_ip, container_port)`. Labels: `__meta_kubernetes_namespace`; `pod_name`, `pod_ip`, `pod_ready` (from conditions: the `Ready` condition's status lowercased → "true"/"false"/"unknown"; helper `get_pod_ready_status`), `pod_phase`, `pod_node_name`, `pod_host_ip`, `pod_uid`; `pod_container_name`, `_container_id` (from matching ContainerStatus), `_container_image`, `_container_init`; when a port: `_container_port_name`, `_container_port_number` (the int as string), `_container_port_protocol`; controller: from `owner_references` where `controller==true` → `pod_controller_kind`/`_controller_name`; + `register_labels_and_annotations("__meta_kubernetes_pod", meta)`. **Do NOT** add node/namespace attach_metadata labels (Phase B). `source = "kubernetes_sd/pod/<ns>/<name>"`.

- [ ] **Step 1: Write the failing test**

```rust
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
        "pod", format!("{{\"items\":[{}]}}", std::str::from_utf8(running).unwrap()).as_bytes()).unwrap();
    let g = objs[0].target_groups();
    assert_eq!(g.len(), 2); // one per container port
    assert_eq!(g[0].targets, vec!["10.1.2.3:8080".to_string()]);
    assert_eq!(g[0].labels["__meta_kubernetes_namespace"], "prod");
    assert_eq!(g[0].labels["__meta_kubernetes_pod_ready"], "true");
    assert_eq!(g[0].labels["__meta_kubernetes_pod_container_port_number"], "8080");
    assert_eq!(g[0].labels["__meta_kubernetes_pod_controller_kind"], "ReplicaSet");
    assert_eq!(g[0].labels["__meta_kubernetes_pod_container_init"], "false");

    let done = br#"{"items":[{"metadata":{"name":"job","namespace":"prod"},
        "status":{"phase":"Succeeded","podIP":"10.1.2.9"}}]}"#;
    let (objs2, _) = crate::scrape::kubernetes::object::parse_list("pod", done).unwrap();
    assert!(objs2[0].target_groups().is_empty());
}

#[test]
fn pod_portless_container_yields_one_target() {
    let j = br#"{"items":[{"metadata":{"name":"p","namespace":"d"},
        "spec":{"containers":[{"name":"c","image":"i"}]},
        "status":{"phase":"Running","podIP":"10.1.2.3"}}]}"#;
    let (objs, _) = crate::scrape::kubernetes::object::parse_list("pod", j).unwrap();
    let g = objs[0].target_groups();
    assert_eq!(g.len(), 1);
    assert_eq!(g[0].targets, vec!["10.1.2.3".to_string()]);
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** the pod structs + `pod_target_groups` + the `get_pod_ready_status` helper + the `K8sObject::Pod` arm.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent k8s SD pod role"`

---

## Task 5: service role builder

**Files:**
- Create: `crates/esmagent/src/scrape/kubernetes/roles/service.rs`
- Modify: `object.rs` (add `Service`/`ServiceList` + variant + role arm), `roles/mod.rs` (`pub mod service;`)
- Test: inline

**Interfaces:**
- Produces (`object.rs`): `pub struct Service { pub metadata: ObjectMeta, pub spec: ServiceSpec }`; `ServiceSpec { cluster_ip: String, external_name: String, type_: String (rename "type"), ports: Vec<ServicePort> }`; `ServicePort { name: String, protocol: String, port: i64 }`. `#[serde(default, rename_all="camelCase")]`.
- Produces (`service.rs`): `pub fn service_target_groups(s: &Service) -> Vec<TargetGroup>` — one per service port.

**Reference:** `service.go` `getTargetLabels`. `__address__` = `JoinHostPort("<name>.<namespace>.svc", port)`. Labels: `__meta_kubernetes_namespace`; `service_name`, `service_type`, `service_cluster_ip` (added when `type != "ExternalName"`), `service_external_name` (added when `type == "ExternalName"`); per port: `service_port_name`, `_port_number`, `_port_protocol`; + `register_labels_and_annotations("__meta_kubernetes_service", meta)`. `source = "kubernetes_sd/service/<ns>/<name>"`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn service_fans_out_ports_with_cluster_ip() {
    let j = br#"{"items":[{"metadata":{"name":"api","namespace":"prod"},
        "spec":{"type":"ClusterIP","clusterIP":"10.96.0.1",
          "ports":[{"name":"http","protocol":"TCP","port":80}]}}]}"#;
    let (objs, _) = crate::scrape::kubernetes::object::parse_list("service", j).unwrap();
    let g = objs[0].target_groups();
    assert_eq!(g.len(), 1);
    assert_eq!(g[0].targets, vec!["api.prod.svc:80".to_string()]);
    assert_eq!(g[0].labels["__meta_kubernetes_service_name"], "api");
    assert_eq!(g[0].labels["__meta_kubernetes_service_cluster_ip"], "10.96.0.1");
    assert_eq!(g[0].labels["__meta_kubernetes_service_port_number"], "80");
    assert!(!g[0].labels.contains_key("__meta_kubernetes_service_external_name"));
}

#[test]
fn external_name_service_uses_external_name_label() {
    let j = br#"{"items":[{"metadata":{"name":"ext","namespace":"d"},
        "spec":{"type":"ExternalName","externalName":"example.com",
          "ports":[{"name":"h","protocol":"TCP","port":443}]}}]}"#;
    let (objs, _) = crate::scrape::kubernetes::object::parse_list("service", j).unwrap();
    let g = objs[0].target_groups();
    assert_eq!(g[0].labels["__meta_kubernetes_service_external_name"], "example.com");
    assert!(!g[0].labels.contains_key("__meta_kubernetes_service_cluster_ip"));
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement**.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent k8s SD service role"`

---

## Task 6: ingress role builder

**Files:**
- Create: `crates/esmagent/src/scrape/kubernetes/roles/ingress.rs`
- Modify: `object.rs` (add `Ingress`/`IngressList` + variant + role arm), `roles/mod.rs` (`pub mod ingress;`)
- Test: inline

**Interfaces:**
- Produces (`object.rs`): `pub struct Ingress { pub metadata: ObjectMeta, pub spec: IngressSpec }`; `IngressSpec { tls: Vec<IngressTLS>, rules: Vec<IngressRule>, ingress_class_name: String }`; `IngressTLS { hosts: Vec<String> }`; `IngressRule { host: String, http: IngressHTTP }`; `IngressHTTP { paths: Vec<IngressPath> }`; `IngressPath { path: String }`. `#[serde(default, rename_all="camelCase")]`.
- Produces (`ingress.rs`): `pub fn ingress_target_groups(ig: &Ingress) -> Vec<TargetGroup>` — one per (rule host × path).

**Reference:** `ingress.go` `getTargetLabels`/`getSchemeForHost`/`matchesHostPattern`/`getIngressRulePaths`. For each rule: scheme = `https` if the host matches any TLS host pattern (exact match, or wildcard `*.suffix` where dropping the first host label equals the pattern's suffix) else `http`; paths = each `http.paths[].path` (empty → `/`), or `["/"]` if no paths. Per (host,path): `__address__` (target) = host; labels `__meta_kubernetes_namespace`; `ingress_name`, `ingress_scheme`, `ingress_host`, `ingress_path`, `ingress_class_name`; + `register_labels_and_annotations("__meta_kubernetes_ingress", meta)`. `source = "kubernetes_sd/ingress/<ns>/<name>"`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn ingress_fans_out_hosts_paths_and_tls_scheme() {
    let j = br#"{"items":[{"metadata":{"name":"web","namespace":"prod"},
        "spec":{"ingressClassName":"nginx",
          "tls":[{"hosts":["secure.example.com"]}],
          "rules":[
            {"host":"secure.example.com","http":{"paths":[{"path":"/a"},{"path":"/b"}]}},
            {"host":"plain.example.com","http":{"paths":[]}}]}}]}"#;
    let (objs, _) = crate::scrape::kubernetes::object::parse_list("ingress", j).unwrap();
    let g = objs[0].target_groups();
    assert_eq!(g.len(), 3); // /a,/b on secure + / on plain
    let secure_a = g.iter().find(|x| x.labels["__meta_kubernetes_ingress_path"] == "/a").unwrap();
    assert_eq!(secure_a.targets, vec!["secure.example.com".to_string()]);
    assert_eq!(secure_a.labels["__meta_kubernetes_ingress_scheme"], "https");
    assert_eq!(secure_a.labels["__meta_kubernetes_ingress_class_name"], "nginx");
    let plain = g.iter().find(|x| x.labels["__meta_kubernetes_ingress_host"] == "plain.example.com").unwrap();
    assert_eq!(plain.labels["__meta_kubernetes_ingress_scheme"], "http");
    assert_eq!(plain.labels["__meta_kubernetes_ingress_path"], "/");
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** incl. the `matches_host_pattern` wildcard helper.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent k8s SD ingress role"`

---

## Task 7: API client + auth + LIST/WATCH request builders

**Files:**
- Create: `crates/esmagent/src/scrape/kubernetes/client.rs`
- Modify: `crates/esmagent/src/scrape/kubernetes/mod.rs` (`pub mod client;`)
- Test: inline in `client.rs`

**Interfaces:**
- Consumes: `config::KubernetesSdConfig`, `client::{AuthConfig, TlsConfig}`, `scrape::config::ScrapeError`.
- Produces:
  - `pub struct ApiConfig { pub api_server: String, http: reqwest::blocking::Client, bearer_token_file: Option<String>, bearer_token: Option<String>, basic: Option<(String,String)> }`
  - `pub struct InClusterPaths { pub host_env: String, pub port_env: String, pub ca_file: String, pub token_file: String }` with `Default` = the real k8s values (`KUBERNETES_SERVICE_HOST`, `KUBERNETES_SERVICE_PORT`, `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`, `.../token`) — a param so tests can point at temp files/env.
  - `pub fn resolve_api_config(cfg: &KubernetesSdConfig, paths: &InClusterPaths) -> Result<ApiConfig, ScrapeError>`
  - `impl ApiConfig { pub fn list_url(&self, role: &str, namespace: Option<&str>, selectors: &[K8sSelector], cont: Option<&str>) -> String; pub fn watch_url(&self, role: &str, namespace: Option<&str>, selectors: &[K8sSelector], resource_version: &str, timeout_secs: u64) -> String; pub fn get(&self, url: &str, timeout: Duration) -> Result<reqwest::blocking::Response, ScrapeError> }` (`get` applies the auth header, re-reading `bearer_token_file` per call).

**Reference:** `api.go` `newAPIConfig` (auth resolution), `api_watcher.go` path/query building. Auth: explicit `api_server` → the config's inline `auth`/`tls`; empty `api_server` → in-cluster (env host/port → `https://host:port`, CA file, token file); `kubeconfig_file` set → `ScrapeError "unsupported (deferred): kubeconfig_file"`; both `api_server`+`kubeconfig_file` → error. Normalize api_server (add scheme, strip trailing `/`). Build the reqwest client mirroring `crate::client::build_client` (ca_file→`add_root_certificate`, cert+key→`Identity::from_pem`, insecure→`danger_accept_invalid_certs`; in-cluster CA file becomes `tls.ca_file`). `get()` sets `Accept: application/json` and the bearer/basic auth, **re-reading `bearer_token_file` each call** (token rotation) — an unreadable token file → `ScrapeError` (never log the token). URL paths: node `/api/v1/nodes`; pod `/api/v1[/namespaces/<ns>]/pods`; service `.../services`; ingress `/apis/networking.k8s.io/v1[/namespaces/<ns>]/ingresses`. LIST query: `resourceVersion=0&resourceVersionMatch=NotOlderThan` + `labelSelector`/`fieldSelector` (URL-encoded, joined from `selectors` whose `.role == role`) + `&continue=<cont>` when paginating. WATCH query: `watch=1&allowWatchBookmarks=true&timeoutSeconds=<n>&resourceVersion=<rv>` + the same selectors. (The v1→v1beta1 ingress fallback is handled in the watcher on a 404 — Task 8 — not here.)

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn in_cluster_resolution_from_overridable_paths() {
    let dir = tempfile::tempdir().unwrap();
    let ca = dir.path().join("ca.crt");
    // a valid self-signed PEM fixture string is fine; reqwest only needs it to parse
    std::fs::write(&ca, TEST_CA_PEM).unwrap();
    let tok = dir.path().join("token");
    std::fs::write(&tok, "tok-123\n").unwrap();
    // point env at a fake apiserver host/port via custom env var names
    std::env::set_var("ESM_TEST_K8S_HOST", "10.0.0.1");
    std::env::set_var("ESM_TEST_K8S_PORT", "6443");
    let paths = InClusterPaths {
        host_env: "ESM_TEST_K8S_HOST".into(), port_env: "ESM_TEST_K8S_PORT".into(),
        ca_file: ca.to_string_lossy().into(), token_file: tok.to_string_lossy().into(),
    };
    let cfg = k8s_cfg_role("pod"); // helper building a KubernetesSdConfig{role:"pod",..default}
    let ac = resolve_api_config(&cfg, &paths).unwrap();
    assert_eq!(ac.api_server, "https://10.0.0.1:6443");
}

#[test]
fn list_and_watch_urls_include_namespace_and_selectors() {
    let ac = api_config_for_test("https://api:6443"); // explicit api_server, no auth
    let sel = vec![K8sSelector{role:"pod".into(), label:Some("app=web".into()), field:None}];
    let lu = ac.list_url("pod", Some("prod"), &sel, None);
    assert!(lu.starts_with("https://api:6443/api/v1/namespaces/prod/pods?"));
    assert!(lu.contains("resourceVersion=0"));
    assert!(lu.contains("labelSelector=app%3Dweb"));
    let wu = ac.watch_url("pod", None, &sel, "42", 300);
    assert!(wu.starts_with("https://api:6443/api/v1/pods?"));
    assert!(wu.contains("watch=1") && wu.contains("resourceVersion=42") && wu.contains("timeoutSeconds=300"));
}

#[test]
fn kubeconfig_file_is_rejected() {
    let mut cfg = k8s_cfg_role("pod");
    cfg.kubeconfig_file = Some("/x".into());
    assert!(resolve_api_config(&cfg, &InClusterPaths::default()).unwrap_err().msg.contains("kubeconfig_file"));
}
```

(Provide `TEST_CA_PEM` = any parseable PEM cert constant, `k8s_cfg_role`, `api_config_for_test` helpers in the test module.)

- [ ] **Step 2: Run to verify it fails** — FAIL. (Add `tempfile` dev-dep — already present.)
- [ ] **Step 3: Implement**. URL-encode selector values (a tiny percent-encoder for the query is fine; or reuse an existing dep — grep the workspace for `urlencoding`/`percent-encoding` before hand-rolling).
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent k8s SD API client + auth"`

---

## Task 8: watch cache + watcher thread

**Files:**
- Create: `crates/esmagent/src/scrape/kubernetes/watcher.rs`
- Modify: `mod.rs` (`pub mod watcher;`)
- Test: inline in `watcher.rs` (in-process stub API server)

**Interfaces:**
- Consumes: `client::ApiConfig`, `object::{parse_list, parse_object, WatchEvent, K8sObject}`, `config::K8sSelector`, `discovery::TargetGroup`.
- Produces:
  - `pub struct Watcher { cache: Arc<Mutex<HashMap<String, Arc<K8sObject>>>>, stop: Arc<AtomicBool>, handle: Option<JoinHandle<()>> }`
  - `pub fn start(api: Arc<ApiConfig>, role: String, namespace: Option<String>, selectors: Vec<K8sSelector>) -> Watcher` — spawns the watch thread (initial LIST → cache; then WATCH loop applying events; re-LIST on 410/EOF/error with bounded, stop-aware backoff).
  - `impl Watcher { pub fn target_groups(&self) -> Vec<TargetGroup>; pub fn stop(&mut self) }` (`stop` sets the flag + joins; also called on `Drop`). `target_groups` locks briefly to clone the `Arc<K8sObject>` values, unlocks, then builds groups.
- The watch thread NEVER panics: all I/O/decode errors log + drive re-LIST or `wait_or_stop`-style backoff.

**Reference:** `api_watcher.go` `reloadObjects`/`watchForUpdates`. LIST once (with pagination via `continue`) → replace cache + capture `resourceVersion`; WATCH from that rv, applying `ADDED`/`MODIFIED` (parse the event `object` for this role via `parse_object`, insert by `key()`), `DELETED` (remove by key), `BOOKMARK` (advance rv only). 410 → drop rv + re-LIST (no sleep). EOF/timeout → re-LIST + re-WATCH. Read the streamed body line-by-line via `BufReader::read_line` (each line one `WatchEvent` JSON). Ingress v1→v1beta1 fallback: on a 404 from the `networking.k8s.io/v1` LIST for the ingress role, retry the URL with `/v1beta1/` (mirror upstream's `useNetworkingV1Beta1`; a per-Watcher `AtomicBool` sticky flag). Mirror the stop/backoff cadence of `crate::client`'s `wait_or_stop`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn watcher_lists_then_applies_watch_events() {
    // Stub API server: GET .../pods?...watch=1... streams events; the LIST returns one pod.
    // Mirror the http_sd stub-server pattern (esm_http::Server or a raw TcpListener thread).
    let stub = start_k8s_stub(K8sStubScript {
        list_body: r#"{"metadata":{"resourceVersion":"10"},"items":[
            {"metadata":{"name":"a","namespace":"d"},"spec":{"containers":[{"name":"c"}]},
             "status":{"phase":"Running","podIP":"10.0.0.1"}}]}"#.into(),
        // one MODIFIED adding pod b, one DELETED removing a, then the stream closes
        watch_lines: vec![
          r#"{"type":"ADDED","object":{"metadata":{"name":"b","namespace":"d"},
             "spec":{"containers":[{"name":"c"}]},"status":{"phase":"Running","podIP":"10.0.0.2"}}}"#.into(),
          r#"{"type":"DELETED","object":{"metadata":{"name":"a","namespace":"d"}}}"#.into(),
        ],
    });
    let api = Arc::new(api_config_for_test(&stub.base_url()));
    let mut w = start(api, "pod".into(), Some("d".into()), vec![]);
    // bounded poll: wait until the cache reflects {b} (a removed, b added)
    assert!(wait_until(Duration::from_secs(5), || {
        let g = w.target_groups();
        g.len() == 1 && g[0].targets == vec!["10.0.0.2".to_string()]
    }));
    w.stop();
    stub.stop();
}
```

(Provide `start_k8s_stub`/`K8sStubScript`/`wait_until`/`api_config_for_test` in the test module; `wait_until` mirrors `tests/e2e.rs`.)

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** the watcher. Keep the file ≤ 800 lines; if the stub-server test helper is large, put it in a `#[path]` sibling `watcher_tests.rs` (mirror how `scrapework_tests.rs` is wired).
- [ ] **Step 4: Run** — PASS (run `cargo test -p esmagent scrape::kubernetes::watcher` a few times — no flakes/hangs); clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent k8s SD watch cache + watcher"`

---

## Task 9: KubernetesDiscovery + manager wiring

**Files:**
- Modify: `crates/esmagent/src/scrape/kubernetes/mod.rs` (add `KubernetesDiscovery`)
- Modify: `crates/esmagent/src/scrape/manager.rs` (`build_providers` → `Result`; add k8s providers; thread through `build_job`)
- Test: inline in `mod.rs` (integration against the Task-8 stub) + a manager unit test

**Interfaces:**
- Consumes: `config::KubernetesSdConfig`, `client::{resolve_api_config, ApiConfig, InClusterPaths}`, `watcher::{Watcher, start}`, `discovery::{Discovery, TargetGroup}`.
- Produces:
  - `pub struct KubernetesDiscovery { watchers: Vec<Watcher> }`
  - `pub fn new(cfg: &KubernetesSdConfig) -> Result<KubernetesDiscovery, ScrapeError>` — `resolve_api_config(cfg, &InClusterPaths::default())`, wrap in `Arc<ApiConfig>`, then start one `Watcher` per namespace (from `cfg.namespaces`: empty → one cluster-wide `None` namespace; `own_namespace` → read the in-cluster namespace file `/var/run/secrets/kubernetes.io/serviceaccount/namespace`, best-effort; `names` → one watcher each). `node` role → always a single cluster-wide watcher (ignore namespaces). Filter `selectors` to those whose `.role == cfg.role`.
  - `impl Discovery for KubernetesDiscovery { fn poll(&mut self) -> Vec<TargetGroup> { self.watchers.iter().flat_map(|w| w.target_groups()).collect() } }`
- `manager.rs`: `build_providers(sc) -> Result<Vec<Box<dyn Discovery>>, ScrapeError>` (append one `KubernetesDiscovery` per `sc.kubernetes_sd_configs`); `build_job` propagates the `Result`.

**Reference:** the spec's data-flow section; `manager.rs` `build_providers`/`build_job`. A k8s config with an unreachable API server must NOT crash `new()` at the URL-build/auth-resolve stage that succeeds offline — resolution only fails on genuinely bad config (bad CA file, missing in-cluster env when `api_server` empty). The watch thread tolerates an unreachable server at runtime (empty targets until first successful LIST).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn kubernetes_discovery_polls_targets_from_stub() {
    let stub = start_k8s_stub(/* one node in LIST, empty watch */);
    let mut cfg = k8s_cfg_role("node");
    cfg.api_server = Some(stub.base_url());
    let mut d = KubernetesDiscovery::new(&cfg).unwrap();
    assert!(wait_until(Duration::from_secs(5), || !d.poll().is_empty()));
    stub.stop();
}
```
```rust
// manager.rs test
#[test]
fn build_providers_includes_kubernetes_sd() {
    // a ScrapeConfig with one kubernetes_sd_configs{role:node, api_server:<stub>}
    // build_providers(&sc) returns Ok with a provider that polls the stub's node.
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** `KubernetesDiscovery` + the `build_providers`/`build_job` `Result` change. Update all `build_providers(...)` call sites.
- [ ] **Step 4: Run** — `cargo test -p esmagent` green; clippy clean; windows-gnu compile.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent k8s SD discovery + manager wiring"`

---

## Task 10: e2e + docs

**Files:**
- Create: `crates/esmagent/tests/kubernetes_sd_e2e.rs`
- Modify: `crates/esmagent/README.md` (k8s SD section + Phase A/B limitations), `docs/PORTING.md` (extend the `lib/promscrape` row / limitations to note k8s SD Phase A), `crates/esmagent/src/scrape/config.rs` module doc (k8s SD now supported)
- Test: `crates/esmagent/tests/kubernetes_sd_e2e.rs`

**Interfaces:** none new — drives esmagent's scrape+forward with a `kubernetes_sd_configs` job.

**Reference:** `crates/esmagent/tests/scrape_e2e.rs` (the harness) + the Task-8 k8s stub server.

- [ ] **Step 1: Write the failing test** — full scrape→forward via k8s SD:
  1. Start a k8s stub API server whose `node` (or `pod`) LIST returns one object whose `__address__` points at a stub `/metrics` server.
  2. Start a stub `/metrics` server + a destination `/api/v1/write` capture.
  3. `esmagent::run(&Flags)` with a `-promscrape.config` containing a `kubernetes_sd_configs: [{role: node, api_server: <stub>}]` job (+ relabel to map `__meta_kubernetes_node_name`→a label you assert) and `-remoteWrite.url` → the destination.
  4. Bounded-poll until the destination receives the scraped `up==1` series carrying the discovered `__meta_kubernetes_*`-derived label; `GET /api/v1/targets` shows the target up.
  5. `app.stop()` clean.
- [ ] **Step 2: Run to verify it fails** — `cargo test -p esmagent --test kubernetes_sd_e2e` → FAIL.
- [ ] **Step 3: Implement** any wiring gaps + the docs (honest: k8s SD Phase A = pod/node/service/ingress via in-cluster/api_server auth, list+watch; endpoints/endpointslice + attach_metadata joins + kubeconfig + OAuth2 + proxy_url deferred to Phase B/later).
- [ ] **Step 4: Run** — `cargo test -p esmagent --test kubernetes_sd_e2e` PASS; full-workspace `cargo test --workspace`, `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets`, `cargo fmt --check`, windows-gnu compile.
- [ ] **Step 5: Commit** — `git commit -m "test: esmagent k8s SD e2e; docs: kubernetes_sd usage"`

---

## Final verification (after Task 10, before merge)

- [ ] `cargo test --workspace` green on Linux; push and confirm Windows CI green (watch threads + streamed reads are platform-sensitive).
- [ ] windows-gnu cross-compile check passes; `rustup update stable` + workspace clippy before pushing (toolchain drift).
- [ ] Whole-branch code review (subagent-driven final review, most capable model) — focus: no panics in watch threads / `poll()` / reconcile; the watch loop's 410/EOF/backoff paths are stop-aware and never hang `stop()`; no credential logging (bearer token re-read, never logged); the cache lock is never held across a blocking HTTP call; role label sets match upstream; `build_providers` `Result` change didn't break the existing SD providers or the reconcile loop; `endpoints`/`endpointslice`/`kubeconfig_file` correctly rejected.
- [ ] No esmetrics ingest/query hot-path impact (new SD provider only; scrapework/forwarding unchanged) → no benchmark re-validation.
- [ ] Update memory: add an `esmagent-k8s-sd` note (Phase A shipped; Phase B = endpoints/endpointslice + shared cache + attach_metadata) + MEMORY.md.
