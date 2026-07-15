//! HashiCorp Nomad service discovery (`nomad_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/nomad` (v1.146.0): the [`client`] does
//! auth/server normalization and the service queries, [`labels`] holds the
//! `Service` structs + `__meta_nomad_*` label builder, and [`NomadDiscovery`]
//! (this file) is the [`super::discovery::Discovery`] the scrape manager
//! polls.
//!
//! ## Refresh model (deliberate deviation from upstream)
//!
//! Upstream uses Nomad *blocking queries* (`?index=&wait=`, long-poll on
//! `X-Nomad-Index`) with one background goroutine per service. This port
//! instead re-lists on a fixed interval (`-promscrape.nomadSDCheckInterval`,
//! default 30s), mirroring the Consul/EC2/DigitalOcean ports'
//! single-background-thread + `Mutex`-snapshot + `stop`/`Drop` shape rather
//! than http_sd's inline-fetch — a Nomad refresh issues several sequential
//! HTTP calls (service list + one per service) and must not block the
//! reconcile loop. There is no blocking-query index long-poll and no
//! `-promscrape.nomad.waitTime`; each refresh is a plain poll. `allow_stale`
//! IS honored (it adds `&stale` to every query, matching upstream's
//! default-on behavior); the omitted long-poll is the only functional gap.
//!
//! ## Startup robustness
//!
//! [`NomadDiscovery::new`] fails only on genuinely bad config (bad TLS
//! material, conflicting auth). A Nomad server that is down at startup does
//! NOT fail `new()`: the first service listing happens on the background
//! thread (retried at the refresh cadence), and [`Discovery::poll`] returns
//! an empty list until the first successful refresh — matching the
//! Consul/EC2/DigitalOcean robustness choice.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{new_nomad_api, NomadApi};
use labels::append_target_labels;

use super::config::{RawAuthFields, RawBasicAuth, ScrapeError};
use super::discovery::{Discovery, TargetGroup};
use crate::client::{AuthConfig, TlsConfig};

pub mod client;
pub mod labels;

/// Default `nomad_sd_config` refresh interval, matching
/// `-promscrape.nomadSDCheckInterval`'s default (upstream
/// `nomad.SDCheckInterval` = `30*time.Second`).
/// `scrape::wiring::apply_nomad_sd_check_interval` overrides it from the flag;
/// `build_nomad_sd_config` seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Local `nomad_sd_config` shape. Port of `discovery/nomad.SDConfig`'s
/// supported fields. Built via [`build_nomad_sd_config`] from its
/// [`RawNomadSdConfig`]. `refresh_interval` is not a YAML field (upstream
/// reads it from `-promscrape.nomadSDCheckInterval`); it defaults to
/// [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`super::consul::ConsulSdConfig`] / [`super::ec2::Ec2SdConfig`] /
/// [`super::digitalocean::DigitaloceanSdConfig`], to keep `config.rs` under
/// the repo's 800-line file cap.
///
/// `Debug` is hand-written to redact the bearer token / basic password held
/// in `auth` (mirrors the DigitalOcean/EC2 secret redaction).
#[derive(Clone, PartialEq)]
pub struct NomadSdConfig {
    pub server: String,
    pub namespace: Option<String>,
    pub region: Option<String>,
    pub tag_separator: Option<String>,
    /// `None` is treated as `true` at query time (upstream sends `&stale` by
    /// default) — see [`build_query_args`].
    pub allow_stale: Option<bool>,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
    pub refresh_interval: Duration,
}

impl Default for NomadSdConfig {
    fn default() -> Self {
        NomadSdConfig {
            server: String::new(),
            namespace: None,
            region: None,
            tag_separator: None,
            allow_stale: None,
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

impl std::fmt::Debug for NomadSdConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NomadSdConfig")
            .field("server", &self.server)
            .field("namespace", &self.namespace)
            .field("region", &self.region)
            .field("tag_separator", &self.tag_separator)
            .field("allow_stale", &self.allow_stale)
            .field("auth", &"<redacted>")
            .field("tls", &self.tls)
            .field("refresh_interval", &self.refresh_interval)
            .finish()
    }
}

/// Undefaulted `nomad_sd_config` list-entry shape. `bearer_token` holds a
/// secret, so `Debug` is hand-written to redact it (this struct is reachable
/// from `scrape::config`'s `RawScrapeConfig`'s derived `Debug`). Inline
/// `basic_auth`/`bearer_token` need [`RawAuthFields`] conversion, same as the
/// other providers. Lives here (not in `scrape::config`) alongside
/// [`NomadSdConfig`] and [`build_nomad_sd_config`], keeping `config.rs` under
/// the repo's 800-line cap; `scrape::config` imports it for `RawScrapeConfig`.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawNomadSdConfig {
    server: String,
    namespace: Option<String>,
    region: Option<String>,
    tag_separator: Option<String>,
    allow_stale: Option<bool>,
    basic_auth: Option<RawBasicAuth>,
    bearer_token: Option<String>,
    tls_config: Option<TlsConfig>,
}

impl std::fmt::Debug for RawNomadSdConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawNomadSdConfig")
            .field("server", &self.server)
            .field("namespace", &self.namespace)
            .field("region", &self.region)
            .field("tag_separator", &self.tag_separator)
            .field("allow_stale", &self.allow_stale)
            .field(
                "basic_auth",
                &self.basic_auth.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "<redacted>"),
            )
            .field("tls_config", &self.tls_config)
            .finish()
    }
}

/// Builds a [`NomadSdConfig`] from its raw form. `refresh_interval` is seeded
/// to the flag default and overridden by
/// `scrape::wiring::apply_nomad_sd_check_interval`.
pub(crate) fn build_nomad_sd_config(raw: RawNomadSdConfig) -> NomadSdConfig {
    let auth_fields = RawAuthFields {
        basic_auth: raw.basic_auth,
        bearer_token: raw.bearer_token,
    };
    NomadSdConfig {
        server: raw.server,
        namespace: raw.namespace,
        region: raw.region,
        tag_separator: raw.tag_separator,
        allow_stale: raw.allow_stale,
        auth: auth_fields.into_auth_config(),
        tls: raw.tls_config.unwrap_or_default(),
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
    }
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes, so a [`NomadDiscovery::stop`]/`Drop` is observed promptly
/// rather than after a full refresh interval.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `nomad_sd_config` entry. A background thread
/// refreshes the target-group snapshot on `refresh_interval`; [`poll`] clones
/// the current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct NomadDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime, bundled so helpers
/// take one reference. The query-shaping fields mirror `newNomadWatcher`'s
/// inputs.
struct RefreshCtx {
    api: NomadApi,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    job: String,
    /// Resolved namespace (config, else `NOMAD_NAMESPACE`, else empty).
    namespace: String,
    /// Resolved region (config, else `NOMAD_REGION`, else `global`).
    region: String,
    /// `None` or `Some(true)` -> send `&stale` (upstream default-on).
    allow_stale: Option<bool>,
    refresh_interval: Duration,
}

impl NomadDiscovery {
    /// Builds the Nomad API client (failing only on bad config — see the
    /// module doc) and spawns the background refresh thread. The snapshot
    /// starts empty and is populated by the thread's first successful
    /// refresh.
    pub fn new(cfg: &NomadSdConfig, job: &str) -> Result<NomadDiscovery, ScrapeError> {
        let api = new_nomad_api(cfg)?;
        let snapshot = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let ctx = RefreshCtx {
            api,
            snapshot: Arc::clone(&snapshot),
            stop: Arc::clone(&stop),
            job: job.to_string(),
            namespace: resolve_namespace(cfg.namespace.as_deref()),
            region: resolve_region(cfg.region.as_deref()),
            allow_stale: cfg.allow_stale,
            refresh_interval: cfg.refresh_interval,
        };

        let handle = thread::spawn(move || run(&ctx));

        Ok(NomadDiscovery {
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

impl Drop for NomadDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for NomadDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: re-list on `refresh_interval`. A
/// service-list failure is logged and retried at the same cadence, keeping
/// the previous snapshot (so a transiently-down Nomad never wipes discovered
/// targets).
fn run(ctx: &RefreshCtx) {
    let query = build_query_args(ctx);
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        match refresh(ctx, &query) {
            Ok(groups) => *ctx.snapshot.lock().unwrap() = groups,
            Err(e) => log::warn!(
                "esmagent nomad_sd ({}): refresh failed, keeping last-good targets: {e}",
                ctx.job
            ),
        }

        if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
            return;
        }
    }
}

/// One full refresh: list service names, then fetch each service's
/// registrations and build a [`TargetGroup`] per registration. A single
/// service's fetch failure is logged and skipped (the other services still
/// contribute); only a failure to list the service names propagates as `Err`
/// (keeping the previous snapshot).
fn refresh(ctx: &RefreshCtx, query: &str) -> Result<Vec<TargetGroup>, ScrapeError> {
    let names = ctx.api.list_service_names(query)?;
    let mut groups = Vec::new();
    for service in names {
        let regs = match ctx.api.get_service(&service, query) {
            Ok(r) => r,
            Err(e) => {
                log::warn!(
                    "esmagent nomad_sd ({}): cannot fetch registrations for service {service:?}: {e}",
                    ctx.job
                );
                continue;
            }
        };
        let source = format!("{}/nomad/{service}", ctx.job);
        for reg in &regs {
            groups.push(append_target_labels(
                reg,
                &ctx.api.tag_separator,
                source.clone(),
            ));
        }
    }
    Ok(groups)
}

/// Builds the `?...` query string sent to `/v1/services` and
/// `/v1/service/<name>`, mirroring `newNomadWatcher`: `&stale` (when
/// `allow_stale` is unset or true), `&namespace=`, `&region=`.
fn build_query_args(ctx: &RefreshCtx) -> String {
    let mut parts: Vec<String> = Vec::new();
    if ctx.allow_stale.unwrap_or(true) {
        parts.push("stale=".to_string());
    }
    if !ctx.namespace.is_empty() {
        parts.push(format!("namespace={}", query_escape(&ctx.namespace)));
    }
    if !ctx.region.is_empty() {
        parts.push(format!("region={}", query_escape(&ctx.region)));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("?{}", parts.join("&"))
    }
}

/// Resolves the Nomad `namespace` used in queries, matching `api.go`:
/// `sdc.Namespace` wins; when it's empty, fall back to the `NOMAD_NAMESPACE`
/// environment variable (empty when unset).
fn resolve_namespace(cfg_namespace: Option<&str>) -> String {
    match cfg_namespace {
        Some(ns) if !ns.is_empty() => ns.to_string(),
        _ => std::env::var("NOMAD_NAMESPACE").unwrap_or_default(),
    }
}

/// Resolves the Nomad `region` used in queries, matching `api.go`:
/// `sdc.Region` wins; when empty, fall back to `NOMAD_REGION`; when that is
/// empty too, default to `global`.
fn resolve_region(cfg_region: Option<&str>) -> String {
    if let Some(r) = cfg_region.filter(|r| !r.is_empty()) {
        return r.to_string();
    }
    match std::env::var("NOMAD_REGION") {
        Ok(r) if !r.is_empty() => r,
        _ => "global".to_string(),
    }
}

/// Go `url.QueryEscape`-equivalent: unreserved (`A-Za-z0-9-_.~`) pass
/// through, space becomes `+`, everything else is `%XX` (UTF-8 byte-wise).
/// Local copy of the Consul port's helper.
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
/// `true` if `stop` was observed. Local copy of the Consul port's helper.
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
#[path = "nomad_tests.rs"]
mod tests;
