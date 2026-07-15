# esmagent Kubernetes Service Discovery — Phase A Design

**Status:** approved (brainstorm) — ready for implementation planning
**Date:** 2026-07-12
**Component:** `esmagent` scrape engine (vmagent `lib/promscrape/discovery/kubernetes` port)
**Upstream ref:** `/home/test/refsrc/VictoriaMetrics/lib/promscrape/discovery/kubernetes/` @ v1.146.0

## Goal

Add `kubernetes_sd_configs` support to esmagent's scrape engine: a hand-rolled,
**no-tokio** blocking Kubernetes API list+watch client feeding the existing
`scrape::discovery::Discovery` trait. This is the highest-impact deferred
scrape-engine item (Kubernetes is the dominant vmagent deployment mode).

**Phase A (this spec):** the API/watch client + auth + config parse + the four
**self-contained** roles — `pod`, `node`, `service`, `ingress` — wired into the
`Discovery` trait, plus `namespaces` + `selectors` filtering.

**Phase B (later cycle, NOT this spec):** the shared cross-resource object cache
+ the `endpoints` and `endpointslice` roles + `attach_metadata` joins (node
labels on pods, namespace metadata). Phase B is called out at each boundary
below so Phase A leaves clean seams.

## Scope decisions (locked in brainstorm)

- **Roles (all 6 eventually; Phase A = 4):** Phase A ships `pod`, `node`,
  `service`, `ingress`. `endpoints`/`endpointslice` are Phase B (they need a
  shared multi-resource cache to join pods+services).
- **Watch model:** **list + watch, cached behind `poll()`.** A background thread
  per `(role, namespace)` maintains an in-memory object cache; `poll()` (called
  by the reconcile loop on its cadence) builds targets from the cache snapshot —
  cheap, non-blocking. No change to the `Discovery` trait or the reconcile loop.
- **Auth:** **in-cluster** (service-account token + cluster CA, auto-discovered)
  **+ explicit `api_server`** (with the config's own inline auth/TLS). **No
  kubeconfig parser** in Phase A (`kubeconfig_file:` → "unsupported (deferred)"
  error).
- **Filtering:** `namespaces` (own-namespace / list), `selectors` (label/field
  selectors pushed to the API server), `attach_metadata` **parsed** in Phase A
  but the pod→node / namespace joins it drives are **Phase B**.

## Architecture

New submodule `crates/esmagent/src/scrape/kubernetes/`, plugging into the
existing `Discovery` trait — the manager, reconcile loop, and `TargetGroup` type
are unchanged.

```
scrape/kubernetes/
├── mod.rs        — KubernetesSdConfig (parsed), KubernetesDiscovery (impl Discovery)
├── client.rs     — ApiConfig: resolve api_server + auth + TLS; build blocking reqwest client; LIST + WATCH request builders
├── watcher.rs    — per-(role,namespace) watch thread: LIST → chunked WATCH long-poll → in-memory object cache
├── object.rs     — minimal serde structs: Pod, Node, Service, Ingress, ObjectMeta, WatchEvent, List metadata
├── labels.rs     — register_labels_and_annotations + SanitizeLabelName helpers
└── roles/
    ├── pod.rs      — Pod cache → Vec<TargetGroup>
    ├── node.rs     — Node cache → Vec<TargetGroup>
    ├── service.rs  — Service cache → Vec<TargetGroup>
    └── ingress.rs  — Ingress cache → Vec<TargetGroup>
```

### Data flow

1. `KubernetesDiscovery::new(cfg)` resolves `ApiConfig` (auth/TLS/api_server),
   then starts one `Watcher` thread per `(role, namespace)` instance (a single
   cluster-wide watcher when `namespaces` is empty; `node` is always
   cluster-scoped regardless of `namespaces`).
2. Each watcher: initial **LIST** (populate cache + capture
   `metadata.resourceVersion`) → **WATCH** long-poll, applying streamed events to
   the cache. On watch expiry / 410-Gone / EOF it re-LISTs.
3. `poll()` (reconcile-loop cadence) briefly locks the cache to clone the
   `Arc<Object>` handles, releases the lock, and runs the role's target-builder
   → `Vec<TargetGroup>`. Non-blocking; identical contract to the existing
   `StaticDiscovery`/`FileSdDiscovery`/`HttpSdDiscovery`.
4. A `stop: Arc<AtomicBool>` + join on drop stops the watch threads on
   shutdown/reload (mirrors the forwarding-tier `client.rs` worker pattern).

### Reuse

- `scrape::discovery::{Discovery, TargetGroup}` — unchanged trait, new impl.
- `reqwest::blocking` + the forwarding tier's TLS/auth idioms (`client.rs`):
  `add_root_certificate(Certificate::from_pem(ca))` (required to trust the
  cluster CA in-cluster), `Identity::from_pem(cert+key)`,
  `danger_accept_invalid_certs(insecure_skip_verify)`.
- `serde_json` (already a dep) for object/event decode; `serde_yaml_ng` for the
  config struct.
- The manager/reconcile loop, `ScrapeManager`, and the CLI wiring are unchanged
  (a `kubernetes_sd_configs`-bearing job just produces another `Discovery`
  provider).

## The API client & auth (`client.rs`)

`ApiConfig::resolve(cfg) -> Result<ApiConfig, ScrapeError>`, matching upstream
`api.go`:

- **Explicit `api_server:`** → use the config's inline auth
  (`bearer_token`/`bearer_token_file`, `basic_auth {username,password/_file}`,
  `authorization {type,credentials/_file}`) + `tls_config`
  (`ca_file`/`cert_file`/`key_file`/`insecure_skip_verify`/`server_name`). If
  `kubeconfig_file:` is ALSO set → error (upstream rejects the combination; we
  additionally have no kubeconfig parser).
- **Empty `api_server:`** → **in-cluster**: read `KUBERNETES_SERVICE_HOST` /
  `KUBERNETES_SERVICE_PORT` → `https://host:port`; a missing env var produces
  the upstream-worded error ("... it must be defined when running in k8s;
  probably `kubernetes_sd_config->api_server` is missing?"). CA from
  `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`; bearer token from
  `/var/run/secrets/kubernetes.io/serviceaccount/token`.
- **`kubeconfig_file:` set (Phase A)** → "unsupported (deferred): kubeconfig_file"
  error.
- **Normalize** `api_server`: prepend scheme (`https` if TLS configured else
  `http`) when it lacks `://`; strip trailing `/`.

**TLS** (reqwest-blocking): `ca_file` → `add_root_certificate`; `cert_file` +
`key_file` → `Identity::from_pem`; `insecure_skip_verify` →
`danger_accept_invalid_certs`. **`server_name`/SNI override is parsed but NOT
applied** — the same documented reqwest-blocking limitation as
esmauth/esmalert/the forwarding tier.

**Token refresh (correctness):** projected service-account tokens rotate
(~hourly), so a `bearer_token_file` (in-cluster token included) is **re-read per
request** (the file is tiny) rather than cached at startup — a long-running pod
must not pin an expiring token. Static `bearer_token`/basic values are read once.

**Requests:** one blocking client; **per-request timeout** via
`RequestBuilder::timeout` (LIST short, WATCH long = `timeoutSeconds` + margin);
`Accept: application/json`; auth header applied per request.

- **LIST:** `GET <path>?resourceVersion=0&resourceVersionMatch=NotOlderThan`
  + `labelSelector=`/`fieldSelector=` from `selectors` for that role
  + pagination (`limit=`/`continue=`). `resourceVersion=0` reduces control-plane
  load (upstream). Response = a typed `List<T>` carrying items +
  `metadata.resourceVersion` (+ `continue` token for pagination).
- **WATCH:** `GET <path>?watch=1&allowWatchBookmarks=true&timeoutSeconds=N&resourceVersion=X`;
  the response body is streamed through a `BufReader`, one `WatchEvent` JSON per
  line (`ADDED`/`MODIFIED`/`DELETED`/`BOOKMARK`). **410 Gone** → clear
  `resourceVersion` and re-LIST immediately (no sleep, per k8s docs); EOF /
  request timeout → re-LIST + re-WATCH; transport error → bounded, stop-flag-
  aware backoff (mirrors `client.rs` `wait_or_stop`).

**Path builder** per role:
- `node`: `/api/v1/nodes` (cluster-scoped; `namespaces` ignored).
- `pod`: `/api/v1/pods` or `/api/v1/namespaces/<ns>/pods`.
- `service`: `/api/v1/services` or `/api/v1/namespaces/<ns>/services`.
- `ingress`: `/apis/networking.k8s.io/v1/ingresses` (or `.../namespaces/<ns>/…`),
  with a **v1beta1 fallback** on 404 (old clusters), matching upstream's
  `useNetworkingV1Beta1` behavior.

## Watch cache (`watcher.rs`)

- One `Watcher` per `(role, namespace)` instance owns
  `Arc<Mutex<HashMap<ObjectKey, Arc<Object>>>>` keyed by `"<namespace>/<name>"`.
- The watch thread applies events: `ADDED`/`MODIFIED` → insert/replace,
  `DELETED` → remove, `BOOKMARK` → advance `resourceVersion` only.
- `poll()` locks briefly to clone the `Arc<Object>` handles, releases, then runs
  the role builder outside the lock. One `TargetGroup` per object;
  `source = "kubernetes_sd/<role>/<namespace>/<name>"`.
- `stop` flag + thread join on drop; never panics in the watch thread (I/O /
  decode errors are logged and drive a re-LIST or bounded backoff, never a
  crash).

## Role label sets

Shared helper `register_labels_and_annotations(prefix, meta, out)`: for each
object label/annotation adds `<prefix>_label_<sanitized>`,
`<prefix>_labelpresent_<sanitized>="true"`, `<prefix>_annotation_<sanitized>`,
`<prefix>_annotationpresent_<sanitized>="true"`; `SanitizeLabelName` maps every
char outside `[a-zA-Z0-9_]` to `_` (matches upstream `discoveryutil.SanitizeLabelName`).

Faithful to upstream `node.go`/`pod.go`/`service.go`/`ingress.go`:

**node** — `__address__` = node address : kubelet port (from the node's
addresses, preferring InternalIP; port is the kubelet port). Labels:
`__meta_kubernetes_node_name`, `__meta_kubernetes_node_provider_id`,
`__meta_kubernetes_node_address_<Type>` (e.g. `_InternalIP`, `_Hostname`), + the
node label/annotation set (`__meta_kubernetes_node_*`).

**pod** — one target per container port; a container with no declared ports
still yields one portless target. Pods in `Succeeded`/`Failed` phase are skipped.
`__address__` = `podIP:containerPort` (or `podIP` portless). Labels:
`__meta_kubernetes_namespace`; `__meta_kubernetes_pod_name`, `_pod_ip`,
`_pod_ready`, `_pod_phase`, `_pod_node_name`, `_pod_host_ip`, `_pod_uid`;
`__meta_kubernetes_pod_container_name`, `_container_id`, `_container_image`,
`_container_init`, `_container_port_name`, `_container_port_number`,
`_container_port_protocol`; `__meta_kubernetes_pod_controller_kind`,
`_controller_name`; + the pod label/annotation set. **`attach_metadata:{node}`
(node labels on the pod target) is Phase B** — Phase A parses the option but does
not perform the node-cache join.

**service** — one target per service port. `__address__` =
`<service>.<namespace>.svc:port`. Labels: `__meta_kubernetes_namespace`;
`__meta_kubernetes_service_name`, `_service_type`, `_service_cluster_ip` (or
`_service_external_name` for `ExternalName` services);
`__meta_kubernetes_service_port_name`, `_port_number`, `_port_protocol`; + the
service label/annotation set.

**ingress** — one target per rule host × path. `__address__` = host; scheme is
`https` when the host is covered by the ingress's TLS config, else `http`. Empty
path → `/`. Labels: `__meta_kubernetes_namespace`;
`__meta_kubernetes_ingress_name`, `_ingress_scheme`, `_ingress_host`,
`_ingress_path`, `_ingress_class_name`; + the ingress label/annotation set.

## Config parse (`config.rs` change)

`kubernetes_sd_configs` moves OUT of the reject-unknown-keys list (currently at
`config.rs:56`) into a typed field on `ScrapeConfig`:
`kubernetes_sd_configs: Vec<KubernetesSdConfig>`. `KubernetesSdConfig`:

```
role: String            // pod|node|service|ingress (endpoints/endpointslice -> "unsupported (deferred, Phase B)")
api_server: Option<String>
kubeconfig_file: Option<String>   // set -> "unsupported (deferred)" error
namespaces: { own_namespace: bool, names: Vec<String> }
selectors: Vec<{ role: String, label: Option<String>, field: Option<String> }>
attach_metadata: Option<{ node: bool, namespace: bool }>   // parsed; joins are Phase B
// inline auth/tls (reuse the AuthConfig/TlsConfig already used by http_sd_configs):
authorization / basic_auth / bearer_token / bearer_token_file / tls_config
```

Validation (at parse, matching upstream `newAPIConfig`): `role` must be one of
the 4 supported (a valid-but-Phase-B `endpoints`/`endpointslice` gives a
"deferred (Phase B)" message; anything else the upstream "unexpected role"
error); `api_server` + `kubeconfig_file` together → error;
`kubeconfig_file` set → deferred error.

The other cloud-SD keys stay rejected.

## Error handling

- Parse/validate errors surface at `-promscrape.config` load (fail fast, before
  serving), secret-free.
- Runtime watch errors (API unreachable, 5xx, decode failure, watch expiry) are
  **logged and retried** (re-LIST / bounded backoff); a broken watcher yields its
  last-good cache via `poll()` (empty until the first successful LIST) — a down
  API server degrades to stale/empty targets, never a crash or a blocked
  reconcile loop.
- Never log the bearer token / basic password / client key. API URLs and k8s
  error bodies (which don't carry our credentials) may appear in logs.
- Never panic in a watch thread or `poll()`.

## Testing

- **Role builders** (`roles/*.rs`): pure `Vec<Object> → Vec<TargetGroup>`
  functions, unit-tested against upstream's JSON fixtures in
  `discovery/kubernetes/testdata/` (and hand-built objects), asserting exact
  `__address__` + `__meta_kubernetes_*` label sets per role, the pod
  port-fan-out + Succeeded/Failed skip, the ingress host×path fan-out + TLS
  scheme, the ExternalName service branch.
- **labels.rs**: `SanitizeLabelName` + the label/annotation/present fan-out.
- **client.rs**: auth resolution (in-cluster env + files via a temp dir and
  overridable paths; explicit api_server with inline auth), api_server
  normalization, the LIST/WATCH URL builders (selectors → labelSelector/
  fieldSelector, namespace paths, ingress v1→v1beta1 fallback).
- **watcher.rs**: an in-process stub API server (mirror the http_sd /
  scrapework stub-server pattern) serving a canned LIST then a WATCH event
  stream; assert the cache reflects ADDED/MODIFIED/DELETED, that `poll()`
  returns the built targets, and that a 410 on the watch triggers a re-LIST.
- **config.rs**: `kubernetes_sd_configs` parses into the typed struct; an
  invalid role, `endpoints`/`endpointslice` (Phase B), and `kubeconfig_file`
  each produce the expected error; a valid pod/node/service/ingress config
  validates.
- Full-workspace gate at the end (`cargo test --workspace`, `clippy -D warnings`,
  `fmt --check`, windows-gnu compile). No esmetrics ingest/query hot-path impact
  (new provider only) → no benchmark re-validation.

## Global constraints

- Faithful to upstream v1.146.0 k8s SD behavior for the 4 roles.
- **No tokio** — `reqwest::blocking` + std threads; the watch is a blocking
  streamed long-poll read.
- Files ≤ 800 lines; `cargo fmt` clean; `RUSTFLAGS="-D warnings" cargo clippy
  --workspace --all-targets` clean; windows-gnu cross-compile check.
- Never log secrets; usernames only.
- Commit style `<type>: <description>`, no attribution trailers.
- After push, watch GitHub Actions + fix failures (Windows tests run only in
  CI).

## Explicitly deferred (documented in `crates/esmagent/README.md`)

- **Phase B:** `endpoints` + `endpointslice` roles; the shared cross-resource
  object cache; `attach_metadata` pod→node / namespace-metadata joins.
- kubeconfig-file auth; OAuth2; `proxy_url`; SNI/`server_name` wiring.
- The `-promscrape.kubernetesSDCheckInterval` flag as a distinct knob (Phase A
  refreshes on the reconcile cadence + the watch stream; watch `timeoutSeconds`
  is a fixed sane default).
