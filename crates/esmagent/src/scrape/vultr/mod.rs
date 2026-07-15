//! Vultr service discovery (`vultr_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/vultr` (v1.146.0): [`client`] resolves
//! the endpoint + bearer-token auth and issues the cursor-paginated
//! `/v2/instances` listing, [`labels`] holds the instance structs + the
//! `__meta_vultr_*` label builder, and [`VultrDiscovery`] (this file) is the
//! [`super::discovery::Discovery`] the scrape manager polls.
//!
//! ## Refresh model
//!
//! Mirrors `scrape::digitalocean`/`scrape::hetzner`: a single background thread
//! re-lists instances on a fixed interval
//! (`-promscrape.vultrSDCheckInterval`), publishing the target-group snapshot
//! behind a `Mutex`; [`VultrDiscovery::poll`] clones it. [`wait_or_stop`]
//! observes a `stop`/`Drop` promptly rather than after a full interval.
//!
//! ## Startup robustness
//!
//! [`VultrDiscovery::new`] fails only on genuinely bad config (missing
//! `bearer_token`, bad TLS material). A Vultr API that is unreachable at
//! startup does NOT fail `new()`: the first listing happens on the background
//! thread (retried at the refresh cadence), and [`Discovery::poll`] returns an
//! empty list until the first successful refresh — matching the
//! digitalocean/hetzner robustness choice.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{new_vultr_api, VultrApi};
use labels::append_target_labels;

use super::config::ScrapeError;
use super::discovery::{Discovery, TargetGroup};
use crate::client::{AuthConfig, TlsConfig};

pub mod client;
pub mod labels;

/// Default `vultr_sd_config` refresh interval: 30s (matches upstream
/// `vultr.SDCheckInterval`'s `30*time.Second`).
/// `scrape::wiring::apply_vultr_sd_check_interval` overrides it from the flag;
/// `scrape::vultr::build_vultr_sd_config` seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Default `port` for the discovered target's `__address__` when a config
/// doesn't set one — matches upstream `vultr.newAPIConfig`'s `port = 80`.
pub const DEFAULT_PORT: u16 = 80;

/// Local `vultr_sd_config` shape. Port of `discovery/vultr.SDConfig`'s
/// supported fields (Vultr's API filter query params — `label`/`main_ip`/
/// `region`/`firewall_group_id`/`hostname` — are not ported; see the crate
/// PORTING notes). Built via `scrape::vultr::build_vultr_sd_config` from its
/// `RawVultrSdConfig`. `refresh_interval` is not a YAML field (upstream reads it
/// from `-promscrape.vultrSDCheckInterval`); it defaults to
/// [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`super::digitalocean::DigitaloceanSdConfig`], to keep `config.rs`
/// under the repo's 800-line file cap.
///
/// `Debug` is hand-written to redact the bearer token held in `auth`.
#[derive(Clone, PartialEq)]
pub struct VultrSdConfig {
    /// Endpoint override. Not part of upstream `vultr.SDConfig` (upstream
    /// hardcodes `https://api.vultr.com`); this is an esmagent test-enabling
    /// extension, exposed as an optional `server` YAML field. Defaults to
    /// empty, which [`client::normalize_server`] resolves to the real Vultr
    /// API; tests set it to point at a stub instead (same pattern as EC2's
    /// `endpoint` / GCE's `endpoint` / Azure's `resource_manager_endpoint`).
    pub server: String,
    pub port: u16,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
    pub refresh_interval: Duration,
}

impl Default for VultrSdConfig {
    fn default() -> Self {
        VultrSdConfig {
            server: String::new(),
            port: DEFAULT_PORT,
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

impl fmt::Debug for VultrSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VultrSdConfig")
            .field("server", &self.server)
            .field("port", &self.port)
            .field("auth", &"<redacted>")
            .field("tls", &self.tls)
            .field("refresh_interval", &self.refresh_interval)
            .finish()
    }
}

/// Undefaulted `vultr_sd_config` list-entry shape. `bearer_token` holds a
/// secret, so `Debug` is hand-written to redact it (this struct is reachable
/// from `scrape::config`'s `RawScrapeConfig`'s derived `Debug`). Lives here
/// (not in `scrape::config`) alongside [`VultrSdConfig`] and
/// [`build_vultr_sd_config`], keeping `config.rs` under the repo's 800-line cap.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawVultrSdConfig {
    /// See [`VultrSdConfig::server`] — an esmagent-only endpoint override not
    /// present in upstream `vultr.SDConfig`.
    server: String,
    port: Option<u16>,
    bearer_token: Option<String>,
    tls_config: Option<TlsConfig>,
}

impl fmt::Debug for RawVultrSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawVultrSdConfig")
            .field("server", &self.server)
            .field("port", &self.port)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "<redacted>"),
            )
            .field("tls_config", &self.tls_config)
            .finish()
    }
}

/// Builds a [`VultrSdConfig`] from its raw form. `port` defaults to
/// [`DEFAULT_PORT`] (`vultr.newAPIConfig`); `refresh_interval` is seeded to the
/// flag default and overridden by
/// `scrape::wiring::apply_vultr_sd_check_interval`.
pub(crate) fn build_vultr_sd_config(raw: RawVultrSdConfig) -> VultrSdConfig {
    VultrSdConfig {
        server: raw.server,
        port: raw.port.unwrap_or(DEFAULT_PORT),
        auth: AuthConfig {
            basic: None,
            bearer: raw.bearer_token.filter(|s| !s.is_empty()),
        },
        tls: raw.tls_config.unwrap_or_default(),
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
    }
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes. Local copy of the digitalocean/ec2/k8s constant of the same
/// purpose.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `vultr_sd_config` entry. A background thread
/// refreshes the target-group snapshot on `refresh_interval`; [`poll`] clones
/// the current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct VultrDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime.
struct RefreshCtx {
    api: VultrApi,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    job: String,
    port: u16,
    refresh_interval: Duration,
}

impl VultrDiscovery {
    /// Builds the Vultr API client (failing only on bad config — see the module
    /// doc) and spawns the background refresh thread. The snapshot starts empty
    /// and is populated by the thread's first successful refresh.
    pub fn new(cfg: &VultrSdConfig, job: &str) -> Result<VultrDiscovery, ScrapeError> {
        let api = new_vultr_api(cfg)?;
        let snapshot = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let ctx = RefreshCtx {
            api,
            snapshot: Arc::clone(&snapshot),
            stop: Arc::clone(&stop),
            job: job.to_string(),
            port: cfg.port,
            refresh_interval: cfg.refresh_interval,
        };

        let handle = thread::spawn(move || run(&ctx));

        Ok(VultrDiscovery {
            snapshot,
            stop,
            handle: Some(handle),
        })
    }

    /// Signals the refresh thread to stop and joins it. Idempotent; also run by
    /// `Drop`.
    fn stop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for VultrDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for VultrDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: re-list instances on `refresh_interval`. A
/// list failure is logged and retried at the same cadence, keeping the previous
/// snapshot (so a transiently-unreachable Vultr API never wipes discovered
/// targets).
fn run(ctx: &RefreshCtx) {
    let source = format!("{}/vultr", ctx.job);
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        match ctx.api.list_instances() {
            Ok(instances) => {
                let groups = append_target_labels(&instances, ctx.port, &source);
                *ctx.snapshot.lock().unwrap() = groups;
            }
            Err(e) => log::warn!(
                "esmagent vultr_sd ({}): refresh failed, keeping last-good targets: {e}",
                ctx.job
            ),
        }

        if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
            return;
        }
    }
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`]. Returns
/// `true` if `stop` was observed. Local copy of the digitalocean/ec2/k8s
/// helper.
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
#[path = "vultr_tests.rs"]
mod tests;
