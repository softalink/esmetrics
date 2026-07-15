//! OVHcloud service discovery (`ovhcloud_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/ovhcloud` (v1.146.0). The provider covers
//! both OVH services: [`client`] resolves the region endpoint's base URL,
//! performs the OVH request signing (`/auth/time` clock sync + per-request
//! `X-Ovh-Signature`), and issues the list + detail GETs;
//! [`dedicated_server`] / [`vps`] hold the per-service structs and the
//! `__meta_ovhcloud_*` label builders; [`common`] holds the shared IP parsing;
//! and [`OvhcloudDiscovery`] (this file) is the [`super::discovery::Discovery`]
//! the scrape manager polls.
//!
//! ## Refresh model
//!
//! Mirrors `scrape::digitalocean`/`scrape::hetzner`: a single background thread
//! re-lists instances on a fixed interval (`-promscrape.ovhcloudSDCheckInterval`,
//! default 30s — upstream `ovhcloud.SDCheckInterval`'s `30*time.Second`),
//! publishing the target-group snapshot behind a `Mutex`;
//! [`OvhcloudDiscovery::poll`] clones it. [`wait_or_stop`] observes a
//! `stop`/`Drop` promptly rather than after a full interval.
//!
//! ## Startup robustness
//!
//! [`OvhcloudDiscovery::new`] fails only on genuinely bad config (unknown
//! `endpoint`; `service` is validated earlier at parse time in
//! [`build_ovhcloud_sd_config`]). An OVH API unreachable at startup does NOT
//! fail `new()`: the first listing happens on the background thread (retried at
//! the refresh cadence), and [`Discovery::poll`] returns an empty list until
//! the first successful refresh — matching the digitalocean/hetzner choice.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{new_ovhcloud_api, OvhcloudApi};
use dedicated_server::append_dedicated_server_target_labels;
use vps::append_vps_target_labels;

use super::config::ScrapeError;
use super::discovery::{Discovery, TargetGroup};

pub mod client;
pub mod common;
pub mod dedicated_server;
pub mod vps;

/// The `service: vps` discriminant.
pub const SERVICE_VPS: &str = "vps";
/// The `service: dedicated_server` discriminant.
pub const SERVICE_DEDICATED_SERVER: &str = "dedicated_server";
/// The default `endpoint` when unset — upstream `newAPIConfig` defaults it to
/// `ovh-eu`.
pub const DEFAULT_ENDPOINT: &str = "ovh-eu";

/// Default `ovhcloud_sd_config` refresh interval, matching
/// `-promscrape.ovhcloudSDCheckInterval`'s default
/// (`ovhcloud.SDCheckInterval` = `30*time.Second`).
/// `scrape::wiring::apply_ovhcloud_sd_check_interval` overrides it from the
/// flag; [`build_ovhcloud_sd_config`] seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Local `ovhcloud_sd_config` shape. Port of `discovery/ovhcloud.SDConfig`'s
/// supported fields. Built via [`build_ovhcloud_sd_config`] from its
/// [`RawOvhcloudSdConfig`]. `refresh_interval` is not a YAML field (upstream
/// reads it from `-promscrape.ovhcloudSDCheckInterval`); it defaults to
/// [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time.
/// `api_url_override` is likewise not a YAML field — it is empty in production
/// (so [`client`] uses the region endpoint's URL) and set only by tests to
/// point at a stub, mirroring `HetznerSdConfig::server`.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`super::hetzner::HetznerSdConfig`], to keep `config.rs` under the
/// repo's 800-line file cap.
///
/// `Debug` is hand-written to redact the `application_secret` and
/// `consumer_key`.
#[derive(Clone, PartialEq)]
pub struct OvhcloudSdConfig {
    pub endpoint: String,
    pub application_key: String,
    pub application_secret: String,
    pub consumer_key: String,
    pub service: String,
    pub refresh_interval: Duration,
    pub api_url_override: String,
}

impl Default for OvhcloudSdConfig {
    fn default() -> Self {
        OvhcloudSdConfig {
            endpoint: DEFAULT_ENDPOINT.to_string(),
            application_key: String::new(),
            application_secret: String::new(),
            consumer_key: String::new(),
            service: String::new(),
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
            api_url_override: String::new(),
        }
    }
}

impl fmt::Debug for OvhcloudSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OvhcloudSdConfig")
            .field("endpoint", &self.endpoint)
            .field("application_key", &self.application_key)
            .field("application_secret", &"<redacted>")
            .field("consumer_key", &"<redacted>")
            .field("service", &self.service)
            .field("refresh_interval", &self.refresh_interval)
            .finish()
    }
}

/// Undefaulted `ovhcloud_sd_config` list-entry shape. `application_secret` /
/// `consumer_key` hold secrets, so `Debug` is hand-written to redact them (this
/// struct is reachable from `scrape::config`'s `RawScrapeConfig`'s derived
/// `Debug`). Lives here (not in `scrape::config`) alongside [`OvhcloudSdConfig`]
/// and [`build_ovhcloud_sd_config`], keeping `config.rs` under the repo's
/// 800-line cap.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawOvhcloudSdConfig {
    endpoint: Option<String>,
    application_key: String,
    application_secret: String,
    consumer_key: String,
    service: String,
}

impl fmt::Debug for RawOvhcloudSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawOvhcloudSdConfig")
            .field("endpoint", &self.endpoint)
            .field("application_key", &self.application_key)
            .field("application_secret", &"<redacted>")
            .field("consumer_key", &"<redacted>")
            .field("service", &self.service)
            .finish()
    }
}

/// Builds an [`OvhcloudSdConfig`] from its raw form. `service` and `endpoint`
/// are validated at parse time (upstream validates `endpoint` in `newAPIConfig`
/// and `service` in `GetLabels`) so a misconfigured `ovhcloud_sd_config` fails
/// at config parse rather than at discovery time. `endpoint` defaults to
/// [`DEFAULT_ENDPOINT`]; `refresh_interval` is seeded to the flag default and
/// overridden by `scrape::wiring::apply_ovhcloud_sd_check_interval`.
pub(crate) fn build_ovhcloud_sd_config(
    raw: RawOvhcloudSdConfig,
) -> Result<OvhcloudSdConfig, ScrapeError> {
    if raw.service != SERVICE_VPS && raw.service != SERVICE_DEDICATED_SERVER {
        return Err(ScrapeError::new(format!(
            "unexpected service={:?}; only `dedicated_server` and `vps` are supported",
            raw.service
        )));
    }
    let endpoint = raw
        .endpoint
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
    if client::endpoint_base_url(&endpoint).is_none() {
        return Err(ScrapeError::new(format!(
            "unsupported `endpoint` for ovhcloud sd: {endpoint}"
        )));
    }
    Ok(OvhcloudSdConfig {
        endpoint,
        application_key: raw.application_key,
        application_secret: raw.application_secret,
        consumer_key: raw.consumer_key,
        service: raw.service,
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
        api_url_override: String::new(),
    })
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes. Local copy of the digitalocean/hetzner constant of the same purpose.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `ovhcloud_sd_config` entry. A background thread
/// refreshes the target-group snapshot on `refresh_interval`; [`poll`] clones
/// the current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct OvhcloudDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime.
struct RefreshCtx {
    api: OvhcloudApi,
    service: String,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    job: String,
    refresh_interval: Duration,
}

impl OvhcloudDiscovery {
    /// Builds the OVHcloud API client (failing only on bad config — see the
    /// module doc) and spawns the background refresh thread. The snapshot
    /// starts empty and is populated by the thread's first successful refresh.
    pub fn new(cfg: &OvhcloudSdConfig, job: &str) -> Result<OvhcloudDiscovery, ScrapeError> {
        let api = new_ovhcloud_api(cfg)?;
        let snapshot = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let ctx = RefreshCtx {
            api,
            service: cfg.service.clone(),
            snapshot: Arc::clone(&snapshot),
            stop: Arc::clone(&stop),
            job: job.to_string(),
            refresh_interval: cfg.refresh_interval,
        };

        let handle = thread::spawn(move || run(&ctx));

        Ok(OvhcloudDiscovery {
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

impl Drop for OvhcloudDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for OvhcloudDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: re-list instances on `refresh_interval`. A
/// list failure is logged and retried at the same cadence, keeping the previous
/// snapshot (so a transiently-unreachable OVH API never wipes discovered
/// targets).
fn run(ctx: &RefreshCtx) {
    let source = format!("{}/ovhcloud", ctx.job);
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        match list_target_groups(ctx, &source) {
            Ok(groups) => *ctx.snapshot.lock().unwrap() = groups,
            Err(e) => log::warn!(
                "esmagent ovhcloud_sd ({}): refresh failed, keeping last-good targets: {e}",
                ctx.job
            ),
        }

        if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
            return;
        }
    }
}

/// Lists instances for the configured service and builds the target groups. A
/// per-instance detail failure is logged and that instance is skipped (matching
/// upstream's `logger.Errorf(...); continue`), so one bad instance doesn't drop
/// the rest. `service` was validated by [`build_ovhcloud_sd_config`], so the
/// fallthrough is unreachable in practice; it is reported as an error (never a
/// panic) to match the port's no-panic contract.
fn list_target_groups(ctx: &RefreshCtx, source: &str) -> Result<Vec<TargetGroup>, ScrapeError> {
    match ctx.service.as_str() {
        SERVICE_VPS => {
            let names = ctx.api.list_vps()?;
            let mut servers = Vec::with_capacity(names.len());
            for name in &names {
                match ctx.api.get_vps_details(name) {
                    Ok(v) => servers.push(v),
                    Err(e) => log::error!("ovhcloud vps details for {name:?} failed: {e}"),
                }
            }
            Ok(append_vps_target_labels(&servers, source))
        }
        SERVICE_DEDICATED_SERVER => {
            let names = ctx.api.list_dedicated_servers()?;
            let mut servers = Vec::with_capacity(names.len());
            for name in &names {
                match ctx.api.get_dedicated_server_details(name) {
                    Ok(s) => servers.push(s),
                    Err(e) => {
                        log::error!("ovhcloud dedicated_server details for {name:?} failed: {e}")
                    }
                }
            }
            Ok(append_dedicated_server_target_labels(&servers, source))
        }
        other => Err(ScrapeError::new(format!(
            "unexpected service={other:?}; only `dedicated_server` and `vps` are supported"
        ))),
    }
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`]. Returns
/// `true` if `stop` was observed. Local copy of the digitalocean/hetzner helper.
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
#[path = "ovhcloud_tests.rs"]
mod tests;
