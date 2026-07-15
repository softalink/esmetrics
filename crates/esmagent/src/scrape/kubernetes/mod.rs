//! Kubernetes service discovery (`kubernetes_sd_configs`) support.
//!
//! k8s object serde structs ([`object`]), the `__meta_kubernetes_*`
//! label/annotation helpers ([`labels`]), the per-role `TargetGroup`
//! builders ([`roles`]), the API client + auth + LIST/WATCH URL builders
//! ([`client`]), and the watch cache/thread ([`watcher`]) are the
//! foundation this module's [`KubernetesDiscovery`] sits on top of.
//!
//! [`KubernetesDiscovery`] is the [`super::discovery::Discovery`]
//! implementation `scrape::manager` polls: it owns one [`watcher::Watcher`]
//! per namespace (or a single cluster-wide watcher — see
//! [`KubernetesDiscovery::new`]'s doc for the exact expansion rule) and
//! flattens their cached target groups on every [`Discovery::poll`] call.
//! Building a `KubernetesDiscovery` can fail — auth resolution
//! ([`client::resolve_api_config`]) reads local token/CA files and can hit a
//! genuinely bad config (unreadable CA file, missing in-cluster env when
//! `api_server` is empty) — so `scrape::manager::build_providers` and
//! everything above it (`build_job`, `ScrapeManager::start`) propagate that
//! as a startup-time `Err` (fail fast), matching this crate's existing
//! `Result`-returning discovery/job-build helpers. A reachable-at-startup
//! but unreachable-at-runtime API server does NOT fail `new()` — the watch
//! thread tolerates that on its own (see `watcher`'s module doc), and
//! `poll()` simply returns no target groups from that watcher until its
//! first successful LIST.

use std::sync::Arc;

use client::{resolve_api_config, InClusterPaths};
use registry::{BuildCtx, ObjectRegistry};
use watcher::Watcher;

use super::config::{K8sSelector, KubernetesSdConfig, ScrapeError};
use super::discovery::{Discovery, TargetGroup};

pub mod client;
pub mod kubeconfig;
pub mod labels;
pub mod oauth2;
pub mod object;
pub mod registry;
pub mod roles;
pub mod watcher;

/// [`Discovery`] over one `kubernetes_sd_config` entry: one
/// [`watcher::Watcher`] per resolved namespace for the primary role (or a
/// single cluster-wide watcher), flattened on every [`poll`](Discovery::poll).
///
/// When `attach_metadata` is set, or the primary role is one of the endpoints
/// roles (`endpoints`/`endpointslice`, which join their targetRef pods and
/// same-named services), additional *dependency* watchers are started so the
/// primary role's builders can join in other kinds' labels via the shared
/// [`ObjectRegistry`]. Dependency watchers feed the registry but never
/// contribute target groups directly.
pub struct KubernetesDiscovery {
    primary: Vec<Watcher>,
    /// Dependency-role watchers (attach_metadata / endpoints deps): feed the
    /// registry, never contribute target groups directly. Retained solely to
    /// keep their background watch threads (and thus the caches the registry
    /// holds `Arc`s into) alive for this discovery's lifetime.
    #[allow(dead_code)]
    deps: Vec<Watcher>,
    registry: ObjectRegistry,
    attach_node_metadata: bool,
    attach_namespace_metadata: bool,
}

impl KubernetesDiscovery {
    /// Resolves `cfg`'s auth into an [`client::ApiConfig`] (see
    /// [`client::resolve_api_config`]), then starts one [`watcher::Watcher`] per namespace this config
    /// resolves to:
    ///
    /// - `cfg.role == "node"`: always a single cluster-wide watcher
    ///   (namespace `None`), ignoring `cfg.namespaces` entirely (`node` is a
    ///   cluster-scoped resource).
    /// - otherwise: the namespace list starts empty; if
    ///   `cfg.namespaces.own_namespace` is set, the in-cluster namespace file
    ///   ([`InClusterPaths::namespace_file`]) is read best-effort — a read
    ///   failure is logged and that source is skipped, it does NOT fail `new()` (this
    ///   config field is meaningless outside a real pod, and failing startup
    ///   over it would make every non-in-cluster deployment need to leave it
    ///   unset); then every `cfg.namespaces.names` entry is appended. If the
    ///   resulting list is empty, a single cluster-wide watcher is started
    ///   (matches upstream: no namespace restriction watches everything);
    ///   otherwise one watcher per named namespace.
    ///
    /// `cfg.selectors` is filtered down to entries whose `.role == cfg.role`
    /// before being passed to every watcher (a selector for another role in
    /// the same `kubernetes_sd_config` doesn't apply here).
    ///
    /// Never blocks on/talks to the Kubernetes API server itself — auth
    /// resolution only builds the HTTP client and reads local token/CA
    /// files/env vars (see [`client::resolve_api_config`]'s doc); each
    /// watcher's LIST/WATCH network calls happen on its own background
    /// thread after this returns.
    pub fn new(cfg: &KubernetesSdConfig) -> Result<KubernetesDiscovery, ScrapeError> {
        let paths = InClusterPaths::default();
        let api = Arc::new(resolve_api_config(cfg, &paths)?);

        // `attach_metadata` absent → both flags false. The upstream global
        // `-promscrape.kubernetes.attachNodeMetadataAll` /
        // `attachNamespaceMetadataAll` flags are resolved into `attach_metadata`
        // upstream at wiring time (see
        // `scrape::wiring::apply_kubernetes_attach_metadata_defaults`), so by the
        // time discovery runs, `None` here genuinely means "attach nothing".
        let (attach_node_metadata, attach_namespace_metadata) = match &cfg.attach_metadata {
            Some(am) => (am.node, am.namespace),
            None => (false, false),
        };

        let mut registry = ObjectRegistry::default();

        // Primary watchers: one per namespace the primary role resolves to.
        let primary: Vec<Watcher> = resolve_namespaces(cfg, &paths.namespace_file)
            .into_iter()
            .map(|ns| {
                let w = start_watcher(&api, cfg, &cfg.role, ns);
                registry.register(&cfg.role, w.0.as_deref(), w.1.cache());
                w.1
            })
            .collect();

        // Dependency watchers per `dependency_roles`. Each is passed
        // `cfg.selectors` filtered to *its* role (upstream `joinSelectors`
        // filters per role). Cluster-wide deps (node/namespace) use a single
        // `None` watcher; namespaced deps (pod/service) use the same namespace
        // expansion as the primary role.
        let mut deps = Vec::new();
        for (dep_role, cluster_wide) in
            dependency_roles(&cfg.role, attach_node_metadata, attach_namespace_metadata)
        {
            let namespaces = if cluster_wide {
                vec![None]
            } else {
                resolve_namespaces(cfg, &paths.namespace_file)
            };
            for ns in namespaces {
                let w = start_watcher(&api, cfg, dep_role, ns);
                registry.register(dep_role, w.0.as_deref(), w.1.cache());
                deps.push(w.1);
            }
        }

        Ok(KubernetesDiscovery {
            primary,
            deps,
            registry,
            attach_node_metadata,
            attach_namespace_metadata,
        })
    }
}

/// Starts one watcher for `role`/`namespace`, passing `cfg.selectors`
/// filtered to `role`. Returns the namespace alongside the watcher so the
/// caller can register the watcher's cache under `(role, namespace)`.
fn start_watcher(
    api: &Arc<client::ApiConfig>,
    cfg: &KubernetesSdConfig,
    role: &str,
    namespace: Option<String>,
) -> (Option<String>, Watcher) {
    let selectors: Vec<K8sSelector> = cfg
        .selectors
        .iter()
        .filter(|s| s.role == role)
        .cloned()
        .collect();
    let w = watcher::start(
        Arc::clone(api),
        role.to_string(),
        namespace.clone(),
        selectors,
    );
    (namespace, w)
}

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
    if attach_ns
        && matches!(
            role,
            "pod" | "service" | "endpoints" | "endpointslice" | "ingress"
        )
    {
        deps.push(("namespace", true));
    }
    deps
}

impl Discovery for KubernetesDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        let ctx = BuildCtx {
            registry: &self.registry,
            attach_node_metadata: self.attach_node_metadata,
            attach_namespace_metadata: self.attach_namespace_metadata,
        };
        self.primary
            .iter()
            .flat_map(|w| w.target_groups(&ctx))
            .collect()
    }
}

/// The `Vec<Option<namespace>>` [`KubernetesDiscovery::new`] starts one
/// watcher per entry for — see its doc for the exact rule. `namespace_file`
/// is the projected in-cluster namespace path to read (best-effort) when
/// `cfg.namespaces.own_namespace` is set; it's a parameter (rather than a
/// hardcoded const) so a unit test can point it at a temp file, mirroring
/// [`InClusterPaths`]'s `ca_file`/`token_file` overridability.
fn resolve_namespaces(cfg: &KubernetesSdConfig, namespace_file: &str) -> Vec<Option<String>> {
    if cfg.role == "node" {
        return vec![None];
    }

    let mut names: Vec<String> = Vec::new();
    if cfg.namespaces.own_namespace {
        match std::fs::read_to_string(namespace_file) {
            Ok(contents) => names.push(contents.trim().to_string()),
            Err(e) => log::warn!(
                "esmagent kubernetes_sd (role {:?}): namespaces.own_namespace is set but \
                 {namespace_file:?} could not be read ({e}); skipping it",
                cfg.role
            ),
        }
    }
    names.extend(cfg.namespaces.names.iter().cloned());

    if names.is_empty() {
        vec![None]
    } else {
        names.into_iter().map(Some).collect()
    }
}

#[cfg(test)]
#[path = "kubernetes_tests.rs"]
mod tests;
