//! Consul Agent service discovery (`consulagent_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/consulagent` (v1.146.0): the [`client`]
//! does auth/agent-info/service queries against the LOCAL Consul agent,
//! [`labels`] holds the `ServiceNode`/`Agent` structs + the
//! `__meta_consulagent_*` label builder, and [`ConsulagentDiscovery`] (this
//! file) is the [`super::discovery::Discovery`] the scrape manager polls.
//!
//! ## Consul Agent vs. Consul (the key difference)
//!
//! Unlike `scrape::consul` (which queries the cluster catalog/health APIs),
//! this provider queries the local agent's own endpoints: `/v1/agent/self`
//! for the agent (datacenter/node/member-address/metadata), `/v1/agent/services`
//! for the registered service list, and `/v1/agent/health/service/name/<svc>`
//! for each service's nodes. The label prefix is `__meta_consulagent_`, the
//! address/dc/node/metadata labels come from the local agent, and there is no
//! `partition`/`tags`/`node_meta`/`allow_stale`/`dc` query support (upstream's
//! consulagent `SDConfig` has none of those).
//!
//! ## Refresh model (deliberate deviation from upstream)
//!
//! Upstream runs one background goroutine per service polling on
//! `-promscrape.consulagentSDCheckInterval` (default 30s). This port instead
//! re-lists everything on that same interval from a single background thread,
//! mirroring the Consul port's single-thread + `Mutex`-snapshot +
//! `stop`/`Drop` shape.
//!
//! ## Startup robustness
//!
//! [`ConsulagentDiscovery::new`] fails only on genuinely bad config (unreadable
//! token file, bad TLS material, conflicting auth). An agent that is down at
//! startup does NOT fail `new()`: agent-info resolution and the first service
//! listing happen on the background thread (retried at the refresh cadence),
//! and [`Discovery::poll`] returns an empty list until the first successful
//! refresh.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{new_consulagent_api, ConsulagentApi};

use super::config::{RawAuthFields, RawBasicAuth, ScrapeError};
use super::discovery::{Discovery, TargetGroup};
use crate::client::{AuthConfig, TlsConfig};
use labels::{append_target_labels, Agent};

pub mod client;
pub mod labels;

/// Default `consulagent_sd_config` refresh interval, matching
/// `-promscrape.consulagentSDCheckInterval`'s default.
/// `scrape::wiring::apply_consulagent_sd_check_interval` overrides it from the
/// flag; `scrape::config::build_consulagent_sd_config` seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Local `consulagent_sd_config` shape. Port of `discovery/consulagent.SDConfig`.
/// Built via `scrape::config::build_consulagent_sd_config` from its
/// `RawConsulagentSdConfig`. `refresh_interval` is not a YAML field (upstream
/// reads it from `-promscrape.consulagentSDCheckInterval`); it defaults to
/// [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`super::consul::ConsulSdConfig`], to keep `config.rs` under the
/// repo's 800-line file cap.
#[derive(Debug, Clone, PartialEq)]
pub struct ConsulagentSdConfig {
    pub server: String,
    pub token: Option<String>,
    pub datacenter: String,
    pub namespace: Option<String>,
    pub scheme: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub services: Vec<String>,
    pub tag_separator: Option<String>,
    pub filter: String,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
    pub refresh_interval: Duration,
}

impl Default for ConsulagentSdConfig {
    fn default() -> Self {
        ConsulagentSdConfig {
            server: String::new(),
            token: None,
            datacenter: String::new(),
            namespace: None,
            scheme: None,
            username: None,
            password: None,
            services: Vec::new(),
            tag_separator: None,
            filter: String::new(),
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

/// Undefaulted `consulagent_sd_config` list-entry shape. Inline
/// `basic_auth`/`bearer_token` need [`RawAuthFields`] conversion, same as
/// `RawConsulSdConfig`. Lives here (not in `scrape::config`) alongside
/// [`ConsulagentSdConfig`] and [`build_consulagent_sd_config`];
/// `scrape::config` imports it for `RawScrapeConfig`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawConsulagentSdConfig {
    server: String,
    token: Option<String>,
    datacenter: String,
    namespace: Option<String>,
    scheme: Option<String>,
    username: Option<String>,
    password: Option<String>,
    services: Vec<String>,
    tag_separator: Option<String>,
    filter: String,
    basic_auth: Option<RawBasicAuth>,
    bearer_token: Option<String>,
    tls_config: Option<TlsConfig>,
}

/// Builds a [`ConsulagentSdConfig`] from its raw form. `refresh_interval` is
/// seeded to the flag default and overridden by
/// `scrape::wiring::apply_consulagent_sd_check_interval`.
pub(crate) fn build_consulagent_sd_config(raw: RawConsulagentSdConfig) -> ConsulagentSdConfig {
    let auth_fields = RawAuthFields {
        basic_auth: raw.basic_auth,
        bearer_token: raw.bearer_token,
    };
    ConsulagentSdConfig {
        server: raw.server,
        token: raw.token,
        datacenter: raw.datacenter,
        namespace: raw.namespace,
        scheme: raw.scheme,
        username: raw.username,
        password: raw.password,
        services: raw.services,
        tag_separator: raw.tag_separator,
        filter: raw.filter,
        auth: auth_fields.into_auth_config(),
        tls: raw.tls_config.unwrap_or_default(),
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
    }
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes, so a [`ConsulagentDiscovery::stop`]/`Drop` is observed promptly
/// rather than after a full refresh interval.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `consulagent_sd_config` entry. A background thread
/// refreshes the target-group snapshot on `refresh_interval`; [`poll`] clones
/// the current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct ConsulagentDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime, bundled so helpers
/// take one reference.
struct RefreshCtx {
    api: ConsulagentApi,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    job: String,
    /// Datacenter from config; empty means "resolve via `/v1/agent/self`".
    configured_datacenter: String,
    namespace: String,
    filter: String,
    watch_services: Vec<String>,
    refresh_interval: Duration,
}

impl ConsulagentDiscovery {
    /// Builds the Consul Agent API client (failing only on bad config — see
    /// the module doc) and spawns the background refresh thread. The snapshot
    /// starts empty and is populated by the thread's first successful refresh.
    pub fn new(cfg: &ConsulagentSdConfig, job: &str) -> Result<ConsulagentDiscovery, ScrapeError> {
        let api = new_consulagent_api(cfg)?;
        let snapshot = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let ctx = RefreshCtx {
            api,
            snapshot: Arc::clone(&snapshot),
            stop: Arc::clone(&stop),
            job: job.to_string(),
            configured_datacenter: cfg.datacenter.clone(),
            namespace: resolve_namespace(cfg.namespace.as_deref()),
            filter: cfg.filter.clone(),
            watch_services: cfg.services.clone(),
            refresh_interval: cfg.refresh_interval,
        };

        let handle = thread::spawn(move || run(&ctx));

        Ok(ConsulagentDiscovery {
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

impl Drop for ConsulagentDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for ConsulagentDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: on each tick fetch the local agent (which
/// carries the datacenter used for filtering plus the member-address /
/// node-name / metadata used in labels), then re-list. An agent-fetch or
/// service-list failure is logged and retried at the same cadence, keeping the
/// previous snapshot (so a transiently-down agent never wipes discovered
/// targets).
fn run(ctx: &RefreshCtx) {
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        match ctx.api.get_agent() {
            Ok(agent) => {
                let datacenter = if ctx.configured_datacenter.is_empty() {
                    agent.config.datacenter.clone()
                } else {
                    ctx.configured_datacenter.clone()
                };
                match refresh(ctx, &agent, &datacenter) {
                    Ok(groups) => *ctx.snapshot.lock().unwrap() = groups,
                    Err(e) => log::warn!(
                        "esmagent consulagent_sd ({}): refresh failed, keeping last-good targets: {e}",
                        ctx.job
                    ),
                }
            }
            Err(e) => log::warn!(
                "esmagent consulagent_sd ({}): cannot obtain agent info: {e}",
                ctx.job
            ),
        }

        if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
            return;
        }
    }
}

/// One full refresh: list the agent's services, filter them (by matching
/// datacenter and the `services` allowlist), then fetch each distinct kept
/// service's nodes and build a [`TargetGroup`] per node. A single service's
/// node-fetch failure is logged and skipped (the other services still
/// contribute); only a failure to list the services propagates as `Err`
/// (keeping the previous snapshot).
fn refresh(
    ctx: &RefreshCtx,
    agent: &Agent,
    datacenter: &str,
) -> Result<Vec<TargetGroup>, ScrapeError> {
    let query = build_services_query(&ctx.namespace, &ctx.filter);
    let services = ctx.api.list_agent_services(&query)?;

    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut groups = Vec::new();
    for svc in services.values() {
        let service_name = &svc.service;
        if svc.datacenter != datacenter {
            continue;
        }
        if !should_collect_service_by_name(&ctx.watch_services, service_name) {
            continue;
        }
        // `/v1/agent/services` returns one entry per registered instance, so
        // the same service name can appear multiple times; each name maps to a
        // single `/v1/agent/health/service/name/<svc>` query (which returns all
        // of that service's nodes), mirroring upstream's one-watcher-per-name.
        if !seen.insert(service_name.clone()) {
            continue;
        }
        let nodes = match ctx.api.get_agent_service_nodes(service_name) {
            Ok(n) => n,
            Err(e) => {
                log::warn!(
                    "esmagent consulagent_sd ({}): cannot fetch nodes for service {service_name:?}: {e}",
                    ctx.job
                );
                continue;
            }
        };
        let source = format!("{}/consulagent/{datacenter}/{service_name}", ctx.job);
        for sn in &nodes {
            groups.push(append_target_labels(
                sn,
                service_name,
                &ctx.api.tag_separator,
                agent,
                source.clone(),
            ));
        }
    }
    Ok(groups)
}

/// Builds the `/v1/agent/services` query string, mirroring
/// `newConsulAgentWatcher`'s `url.Values`: `&ns=` then `&filter=`, each only
/// when non-empty. Always begins with `?` (upstream sends `"?" + qv.Encode()`,
/// which is `"?"` when both are empty). Keys are emitted in `url.Values.Encode`
/// alphabetical order (`filter` before `ns`).
fn build_services_query(namespace: &str, filter: &str) -> String {
    let mut parts: Vec<String> = Vec::new();
    if !filter.is_empty() {
        parts.push(format!("filter={}", query_escape(filter)));
    }
    if !namespace.is_empty() {
        parts.push(format!("ns={}", query_escape(namespace)));
    }
    format!("?{}", parts.join("&"))
}

/// Port of `consul.ShouldCollectServiceByName`: keep every service when the
/// allowlist is empty, else keep only those matching (case-insensitively) an
/// allowlist entry.
fn should_collect_service_by_name(filter_services: &[String], service_name: &str) -> bool {
    filter_services.is_empty()
        || filter_services
            .iter()
            .any(|s| s.eq_ignore_ascii_case(service_name))
}

/// Resolves the Consul `namespace` used in queries: `sdc.Namespace` wins;
/// when empty, fall back to the `CONSUL_NAMESPACE` environment variable
/// (empty when unset), matching `api.go`'s
/// `namespace = os.Getenv("CONSUL_NAMESPACE")` fallback.
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
/// `true` if `stop` was observed.
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
#[path = "consulagent_tests.rs"]
mod tests;
