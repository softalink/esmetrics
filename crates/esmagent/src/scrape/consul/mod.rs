//! Consul service discovery (`consul_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/consul` (v1.146.0): the [`client`] does
//! auth/datacenter/service queries, [`labels`] holds the `ServiceNode`
//! structs + `__meta_consul_*` label builder, and [`ConsulDiscovery`] (this
//! file) is the [`super::discovery::Discovery`] the scrape manager polls.
//!
//! ## Refresh model (deliberate deviation from upstream)
//!
//! Upstream uses Consul *blocking queries* (`?index=&wait=`, long-poll on
//! `X-Consul-Index`) with one background goroutine per service. This port
//! instead re-lists on a fixed interval (`-promscrape.consulSDCheckInterval`,
//! default 30s), mirroring the k8s watcher's single-background-thread +
//! `Mutex`-snapshot + `stop`/`Drop` shape (see `scrape::kubernetes::watcher`)
//! rather than http_sd's inline-fetch — a Consul refresh issues several
//! sequential HTTP calls (datacenter + service list + one per service) and
//! must not block the reconcile loop. There is no blocking-query index
//! long-poll and no `-promscrape.consul.waitTime`; each refresh is a plain
//! poll. `allow_stale` IS honored (it adds `&stale` to every query, matching
//! upstream's default-on behavior); the omitted long-poll is the only
//! functional gap.
//!
//! ## Startup robustness
//!
//! [`ConsulDiscovery::new`] fails only on genuinely bad config (unreadable
//! token file, bad TLS material, conflicting auth). A Consul server that is
//! down at startup does NOT fail `new()`: datacenter resolution and the
//! first service listing happen on the background thread (retried at the
//! refresh cadence), and [`Discovery::poll`] returns an empty list until the
//! first successful refresh — matching the k8s SD robustness choice.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{new_consul_api, ConsulApi};

use super::config::{RawAuthFields, RawBasicAuth, ScrapeError};
use super::discovery::{Discovery, TargetGroup};
use crate::client::{AuthConfig, TlsConfig};
use labels::append_target_labels;

pub mod client;
pub mod labels;

/// Default `consul_sd_config` refresh interval, matching
/// `-promscrape.consulSDCheckInterval`'s default.
/// `scrape::wiring::apply_consul_sd_check_interval` overrides it from the
/// flag; `scrape::config::build_consul_sd_config` seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Local `consul_sd_config` shape. Port of `discovery/consul.SDConfig`'s
/// fields this port drives discovery from. Built via
/// `scrape::config::build_consul_sd_config` from its `RawConsulSdConfig`,
/// mirroring `HttpSdConfig`'s inline `basic_auth`/`bearer_token`/`tls_config`
/// handling. `refresh_interval` is not a YAML field (upstream reads it from
/// `-promscrape.consulSDCheckInterval`); it defaults to
/// [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time.
///
/// Defined here (rather than alongside the other SD configs in
/// `scrape::config`) and re-exported from there so the parse layer and every
/// existing `scrape::config::ConsulSdConfig` reference keep working while
/// keeping `config.rs` under the repo's 800-line file cap.
#[derive(Debug, Clone, PartialEq)]
pub struct ConsulSdConfig {
    pub server: String,
    pub token: Option<String>,
    pub datacenter: String,
    pub namespace: Option<String>,
    pub partition: Option<String>,
    pub scheme: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub services: Vec<String>,
    pub tags: Vec<String>,
    pub node_meta: BTreeMap<String, String>,
    pub tag_separator: Option<String>,
    /// `None` is treated as `true` at query time (upstream sends `&stale` by
    /// default) — see [`build_query_args`].
    pub allow_stale: Option<bool>,
    pub filter: String,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
    pub refresh_interval: Duration,
}

impl Default for ConsulSdConfig {
    fn default() -> Self {
        ConsulSdConfig {
            server: String::new(),
            token: None,
            datacenter: String::new(),
            namespace: None,
            partition: None,
            scheme: None,
            username: None,
            password: None,
            services: Vec::new(),
            tags: Vec::new(),
            node_meta: BTreeMap::new(),
            tag_separator: None,
            allow_stale: None,
            filter: String::new(),
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

/// Undefaulted `consul_sd_config` list-entry shape. Inline
/// `basic_auth`/`bearer_token` need [`RawAuthFields`] conversion, same as
/// `RawHttpSdConfig`. Lives here (not in `scrape::config`) alongside
/// [`ConsulSdConfig`] and [`build_consul_sd_config`], keeping `config.rs`
/// under the repo's 800-line cap; `scrape::config` imports it for
/// `RawScrapeConfig`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawConsulSdConfig {
    server: String,
    token: Option<String>,
    datacenter: String,
    namespace: Option<String>,
    partition: Option<String>,
    scheme: Option<String>,
    username: Option<String>,
    password: Option<String>,
    services: Vec<String>,
    tags: Vec<String>,
    node_meta: BTreeMap<String, String>,
    tag_separator: Option<String>,
    allow_stale: Option<bool>,
    filter: String,
    basic_auth: Option<RawBasicAuth>,
    bearer_token: Option<String>,
    tls_config: Option<TlsConfig>,
}

/// Builds a [`ConsulSdConfig`] from its raw form. `refresh_interval` is seeded
/// to the flag default and overridden by
/// `scrape::wiring::apply_consul_sd_check_interval`.
pub(crate) fn build_consul_sd_config(raw: RawConsulSdConfig) -> ConsulSdConfig {
    let auth_fields = RawAuthFields {
        basic_auth: raw.basic_auth,
        bearer_token: raw.bearer_token,
    };
    ConsulSdConfig {
        server: raw.server,
        token: raw.token,
        datacenter: raw.datacenter,
        namespace: raw.namespace,
        partition: raw.partition,
        scheme: raw.scheme,
        username: raw.username,
        password: raw.password,
        services: raw.services,
        tags: raw.tags,
        node_meta: raw.node_meta,
        tag_separator: raw.tag_separator,
        allow_stale: raw.allow_stale,
        filter: raw.filter,
        auth: auth_fields.into_auth_config(),
        tls: raw.tls_config.unwrap_or_default(),
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
    }
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes, so a [`ConsulDiscovery::stop`]/`Drop` is observed promptly
/// rather than after a full refresh interval. Local copy of the k8s
/// watcher's constant of the same purpose.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `consul_sd_config` entry. A background thread
/// refreshes the target-group snapshot on `refresh_interval`; [`poll`]
/// clones the current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct ConsulDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime, bundled so helpers
/// take one reference. The query-shaping fields mirror `watch.go`'s
/// `newConsulWatcher` inputs.
struct RefreshCtx {
    api: ConsulApi,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    job: String,
    /// Datacenter from config; empty means "resolve via `/v1/agent/self`".
    configured_datacenter: String,
    namespace: String,
    partition: String,
    node_meta: BTreeMap<String, String>,
    tags: Vec<String>,
    filter: String,
    /// `None` or `Some(true)` -> send `&stale` (upstream default-on).
    allow_stale: Option<bool>,
    watch_services: Vec<String>,
    refresh_interval: Duration,
}

impl ConsulDiscovery {
    /// Builds the Consul API client (failing only on bad config — see the
    /// module doc) and spawns the background refresh thread. The snapshot
    /// starts empty and is populated by the thread's first successful
    /// refresh.
    pub fn new(cfg: &ConsulSdConfig, job: &str) -> Result<ConsulDiscovery, ScrapeError> {
        let api = new_consul_api(cfg)?;
        let snapshot = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let ctx = RefreshCtx {
            api,
            snapshot: Arc::clone(&snapshot),
            stop: Arc::clone(&stop),
            job: job.to_string(),
            configured_datacenter: cfg.datacenter.clone(),
            namespace: resolve_namespace(cfg.namespace.as_deref()),
            partition: cfg.partition.clone().unwrap_or_default(),
            node_meta: cfg.node_meta.clone(),
            tags: cfg.tags.clone(),
            filter: cfg.filter.clone(),
            allow_stale: cfg.allow_stale,
            watch_services: cfg.services.clone(),
            refresh_interval: cfg.refresh_interval,
        };

        let handle = thread::spawn(move || run(&ctx));

        Ok(ConsulDiscovery {
            snapshot,
            stop,
            handle: Some(handle),
        })
    }

    /// Signals the refresh thread to stop and joins it. Idempotent; also run
    /// by `Drop`.
    fn stop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ConsulDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for ConsulDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: resolve the datacenter (once), then
/// re-list on `refresh_interval`. A datacenter-resolution or service-list
/// failure is logged and retried at the same cadence, keeping the previous
/// snapshot (so a transiently-down Consul never wipes discovered targets).
fn run(ctx: &RefreshCtx) {
    let mut datacenter = ctx.configured_datacenter.clone();

    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        if datacenter.is_empty() {
            match ctx.api.get_datacenter() {
                Ok(dc) => datacenter = dc,
                Err(e) => {
                    log::warn!(
                        "esmagent consul_sd ({}): cannot resolve datacenter: {e}",
                        ctx.job
                    );
                    if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
                        return;
                    }
                    continue;
                }
            }
        }

        let query = build_query_args(ctx, &datacenter);
        match refresh(ctx, &query, &datacenter) {
            Ok(groups) => *ctx.snapshot.lock().unwrap() = groups,
            Err(e) => log::warn!(
                "esmagent consul_sd ({}): refresh failed, keeping last-good targets: {e}",
                ctx.job
            ),
        }

        if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
            return;
        }
    }
}

/// One full refresh: list service names, filter them (by the `services`
/// allowlist and `tags`), then fetch each kept service's nodes and build a
/// [`TargetGroup`] per node. A single service's node-fetch failure is logged
/// and skipped (the other services still contribute); only a failure to list
/// the service names propagates as `Err` (keeping the previous snapshot).
fn refresh(
    ctx: &RefreshCtx,
    query: &QueryArgs,
    datacenter: &str,
) -> Result<Vec<TargetGroup>, ScrapeError> {
    let names = ctx.api.list_service_names(&query.service_names)?;
    let mut groups = Vec::new();
    for (service, tags) in names {
        if !should_collect_service_by_name(&ctx.watch_services, &service) {
            continue;
        }
        if !should_collect_service_by_tags(&ctx.tags, &tags) {
            continue;
        }
        let nodes = match ctx.api.get_service_nodes(&service, &query.service_nodes) {
            Ok(n) => n,
            Err(e) => {
                log::warn!(
                    "esmagent consul_sd ({}): cannot fetch nodes for service {service:?}: {e}",
                    ctx.job
                );
                continue;
            }
        };
        let source = format!("{}/consul/{datacenter}/{service}", ctx.job);
        for sn in &nodes {
            groups.push(append_target_labels(
                sn,
                &service,
                &ctx.api.tag_separator,
                source.clone(),
            ));
        }
    }
    Ok(groups)
}

/// The two query-arg strings (`?...`) sent to `/v1/catalog/services` and
/// `/v1/health/service/<svc>`. Built by [`build_query_args`].
struct QueryArgs {
    service_names: String,
    service_nodes: String,
}

/// Builds the service-names and service-nodes query strings, mirroring
/// `newConsulWatcher`: a shared base (`?dc=`, `&stale`, `&ns=`,
/// `&partition=`, `&node-meta=`), then `&tag=` per tag for the nodes query
/// and `&filter=` for the names query.
fn build_query_args(ctx: &RefreshCtx, datacenter: &str) -> QueryArgs {
    let mut base = format!("?dc={}", query_escape(datacenter));
    if ctx.allow_stale.unwrap_or(true) {
        base.push_str("&stale");
    }
    if !ctx.namespace.is_empty() {
        base.push_str(&format!("&ns={}", query_escape(&ctx.namespace)));
    }
    if !ctx.partition.is_empty() {
        base.push_str(&format!("&partition={}", query_escape(&ctx.partition)));
    }
    for (k, v) in &ctx.node_meta {
        base.push_str(&format!("&node-meta={}", query_escape(&format!("{k}:{v}"))));
    }

    let mut service_nodes = base.clone();
    for tag in &ctx.tags {
        service_nodes.push_str(&format!("&tag={}", query_escape(tag)));
    }

    let mut service_names = base;
    if !ctx.filter.is_empty() {
        service_names.push_str(&format!("&filter={}", query_escape(&ctx.filter)));
    }

    QueryArgs {
        service_names,
        service_nodes,
    }
}

/// Port of `ShouldCollectServiceByName`: keep every service when the
/// allowlist is empty, else keep only those matching (case-insensitively) an
/// allowlist entry.
fn should_collect_service_by_name(filter_services: &[String], service_name: &str) -> bool {
    filter_services.is_empty()
        || filter_services
            .iter()
            .any(|s| s.eq_ignore_ascii_case(service_name))
}

/// Port of `shouldCollectServiceByTags`: keep a service only when every
/// configured tag is present in its tag list.
fn should_collect_service_by_tags(filter_tags: &[String], tags: &[String]) -> bool {
    filter_tags.iter().all(|ft| tags.contains(ft))
}

/// Resolves the Consul `namespace` used in queries, matching upstream
/// parity with `lib/promscrape/discovery/consul/api.go` (v1.146.0) lines
/// 99-101: `sdc.Namespace` wins; when it's empty, fall back to the
/// `CONSUL_NAMESPACE` environment variable (empty when unset). Mirrors
/// `client::resolve_token`'s config-then-env precedence for
/// `CONSUL_HTTP_TOKEN`.
fn resolve_namespace(cfg_namespace: Option<&str>) -> String {
    match cfg_namespace {
        Some(ns) if !ns.is_empty() => ns.to_string(),
        _ => std::env::var("CONSUL_NAMESPACE").unwrap_or_default(),
    }
}

/// Go `url.QueryEscape`-equivalent: unreserved (`A-Za-z0-9-_.~`) pass
/// through, space becomes `+`, everything else is `%XX` (UTF-8 byte-wise).
fn query_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`]. Returns
/// `true` if `stop` was observed. Local copy of the k8s watcher's helper.
fn wait_or_stop(stop: &AtomicBool, dur: Duration) -> bool {
    let mut remaining = dur;
    while remaining > Duration::ZERO {
        if stop.load(Ordering::SeqCst) {
            return true;
        }
        let step = remaining.min(STOP_POLL_INTERVAL);
        thread::sleep(step);
        remaining = remaining.saturating_sub(step);
    }
    stop.load(Ordering::SeqCst)
}

#[cfg(test)]
#[path = "consul_tests.rs"]
mod tests;
