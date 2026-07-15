//! Hetzner service discovery (`hetzner_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/hetzner` (v1.146.0). The provider covers
//! both Hetzner roles: [`client`] resolves the per-role endpoint + auth and
//! issues the listing calls ([`hcloud`]: paginated `/v1/servers` + `/v1/networks`
//! with Bearer auth; [`robot`]: `/server` with HTTP Basic auth), [`hcloud`] /
//! [`robot`] hold the server structs + the `__meta_hetzner_*` label builders,
//! and [`HetznerDiscovery`] (this file) is the [`super::discovery::Discovery`]
//! the scrape manager polls.
//!
//! ## Refresh model
//!
//! Mirrors `scrape::digitalocean`/`scrape::ec2`: a single background thread
//! re-lists servers on a fixed interval (`-promscrape.hetznerSDCheckInterval`,
//! default 60s — upstream `hetzner.SDCheckInterval`'s `time.Minute`),
//! publishing the target-group snapshot behind a `Mutex`;
//! [`HetznerDiscovery::poll`] clones it. [`wait_or_stop`] observes a
//! `stop`/`Drop` promptly rather than after a full interval.
//!
//! ## Startup robustness
//!
//! [`HetznerDiscovery::new`] fails only on genuinely bad config (unknown
//! `role`, missing required auth, bad TLS material — upstream *panics* on an
//! unexpected role in `GetLabels`; this port returns an error instead). A
//! Hetzner API that is unreachable at startup does NOT fail `new()`: the first
//! listing happens on the background thread (retried at the refresh cadence),
//! and [`Discovery::poll`] returns an empty list until the first successful
//! refresh — matching the digitalocean/ec2 robustness choice.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{new_hetzner_api, HetznerApi};
use hcloud::append_hcloud_target_labels;
use robot::append_robot_target_labels;

use super::config::{RawAuthFields, RawBasicAuth, ScrapeError};
use super::discovery::{Discovery, TargetGroup};
use crate::client::{AuthConfig, TlsConfig};

pub mod client;
pub mod hcloud;
pub mod labels;
pub mod robot;

/// The `role: hcloud` discriminant (Hetzner Cloud API).
pub const ROLE_HCLOUD: &str = "hcloud";
/// The `role: robot` discriminant (Hetzner Robot API).
pub const ROLE_ROBOT: &str = "robot";

/// Default `hetzner_sd_config` refresh interval, matching
/// `-promscrape.hetznerSDCheckInterval`'s default
/// (`hetzner.SDCheckInterval` = `time.Minute`).
/// `scrape::wiring::apply_hetzner_sd_check_interval` overrides it from the
/// flag; [`build_hetzner_sd_config`] seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Default `port` for the discovered target's `__address__` when a config
/// doesn't set one — matches upstream `hetzner.newAPIConfig`'s `port = 80`.
pub const DEFAULT_PORT: u16 = 80;

/// Local `hetzner_sd_config` shape. Port of `discovery/hetzner.SDConfig`'s
/// supported fields. Built via [`build_hetzner_sd_config`] from its
/// [`RawHetznerSdConfig`]. `refresh_interval` is not a YAML field (upstream
/// reads it from `-promscrape.hetznerSDCheckInterval`); it defaults to
/// [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time. `server` is
/// likewise not a YAML field — it is empty in production (so [`client`] uses
/// the role's Hetzner endpoint) and set only by tests to point at a stub.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`super::digitalocean::DigitaloceanSdConfig`], to keep `config.rs`
/// under the repo's 800-line file cap.
///
/// `Debug` is hand-written to redact the bearer token / basic password held in
/// `auth` (mirrors `DigitaloceanSdConfig`'s secret redaction).
#[derive(Clone, PartialEq)]
pub struct HetznerSdConfig {
    pub role: String,
    pub server: String,
    pub port: u16,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
    pub refresh_interval: Duration,
}

impl Default for HetznerSdConfig {
    fn default() -> Self {
        HetznerSdConfig {
            role: String::new(),
            server: String::new(),
            port: DEFAULT_PORT,
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

impl fmt::Debug for HetznerSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HetznerSdConfig")
            .field("role", &self.role)
            .field("server", &self.server)
            .field("port", &self.port)
            .field("auth", &"<redacted>")
            .field("tls", &self.tls)
            .field("refresh_interval", &self.refresh_interval)
            .finish()
    }
}

/// Undefaulted `hetzner_sd_config` list-entry shape. `basic_auth`/`bearer_token`
/// hold secrets, so `Debug` is hand-written to redact them (this struct is
/// reachable from `scrape::config`'s `RawScrapeConfig`'s derived `Debug`).
/// Inline `basic_auth`/`bearer_token` need [`RawAuthFields`] conversion, same
/// as `RawDigitaloceanSdConfig`. Lives here (not in `scrape::config`) alongside
/// [`HetznerSdConfig`] and [`build_hetzner_sd_config`], keeping `config.rs`
/// under the repo's 800-line cap.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawHetznerSdConfig {
    role: String,
    port: Option<u16>,
    basic_auth: Option<RawBasicAuth>,
    bearer_token: Option<String>,
    tls_config: Option<TlsConfig>,
}

impl fmt::Debug for RawHetznerSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawHetznerSdConfig")
            .field("role", &self.role)
            .field("port", &self.port)
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

/// Builds a [`HetznerSdConfig`] from its raw form. `role` is validated at parse
/// time (upstream `newAPIConfig` errors on an unexpected role) — an unknown
/// role is rejected here so a misconfigured `hetzner_sd_config` fails at
/// config parse rather than at discovery time. `port` defaults to
/// [`DEFAULT_PORT`]; `refresh_interval` is seeded to the flag default and
/// overridden by `scrape::wiring::apply_hetzner_sd_check_interval`.
pub(crate) fn build_hetzner_sd_config(
    raw: RawHetznerSdConfig,
) -> Result<HetznerSdConfig, ScrapeError> {
    if raw.role != ROLE_HCLOUD && raw.role != ROLE_ROBOT {
        return Err(ScrapeError::new(format!(
            "unexpected role={:?}; must be one of `robot` or `hcloud`",
            raw.role
        )));
    }
    let auth_fields = RawAuthFields {
        basic_auth: raw.basic_auth,
        bearer_token: raw.bearer_token,
    };
    Ok(HetznerSdConfig {
        role: raw.role,
        server: String::new(),
        port: raw.port.unwrap_or(DEFAULT_PORT),
        auth: auth_fields.into_auth_config(),
        tls: raw.tls_config.unwrap_or_default(),
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
    })
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes. Local copy of the digitalocean/ec2/k8s constant of the same purpose.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `hetzner_sd_config` entry. A background thread
/// refreshes the target-group snapshot on `refresh_interval`; [`poll`] clones
/// the current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct HetznerDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime.
struct RefreshCtx {
    api: HetznerApi,
    role: String,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    job: String,
    port: u16,
    refresh_interval: Duration,
}

impl HetznerDiscovery {
    /// Builds the Hetzner API client (failing only on bad config — see the
    /// module doc) and spawns the background refresh thread. The snapshot
    /// starts empty and is populated by the thread's first successful refresh.
    pub fn new(cfg: &HetznerSdConfig, job: &str) -> Result<HetznerDiscovery, ScrapeError> {
        let api = new_hetzner_api(cfg)?;
        let snapshot = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let ctx = RefreshCtx {
            api,
            role: cfg.role.clone(),
            snapshot: Arc::clone(&snapshot),
            stop: Arc::clone(&stop),
            job: job.to_string(),
            port: cfg.port,
            refresh_interval: cfg.refresh_interval,
        };

        let handle = thread::spawn(move || run(&ctx));

        Ok(HetznerDiscovery {
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

impl Drop for HetznerDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for HetznerDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: re-list servers on `refresh_interval`. A
/// list failure is logged and retried at the same cadence, keeping the previous
/// snapshot (so a transiently-unreachable Hetzner API never wipes discovered
/// targets).
fn run(ctx: &RefreshCtx) {
    let source = format!("{}/hetzner", ctx.job);
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        match list_target_groups(ctx, &source) {
            Ok(groups) => *ctx.snapshot.lock().unwrap() = groups,
            Err(e) => log::warn!(
                "esmagent hetzner_sd ({}): refresh failed, keeping last-good targets: {e}",
                ctx.job
            ),
        }

        if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
            return;
        }
    }
}

/// Lists servers for the configured role and builds the target groups.
/// `role` was validated by [`build_hetzner_sd_config`] and [`new_hetzner_api`],
/// so the fallthrough is unreachable in practice; it is reported as an error
/// (never a panic) to match the port's no-panic contract.
fn list_target_groups(ctx: &RefreshCtx, source: &str) -> Result<Vec<TargetGroup>, ScrapeError> {
    match ctx.role.as_str() {
        ROLE_HCLOUD => {
            let networks = ctx.api.list_hcloud_networks()?;
            let servers = ctx.api.list_hcloud_servers()?;
            Ok(append_hcloud_target_labels(
                &servers, &networks, ctx.port, source,
            ))
        }
        ROLE_ROBOT => {
            let servers = ctx.api.list_robot_servers()?;
            Ok(append_robot_target_labels(&servers, ctx.port, source))
        }
        other => Err(ScrapeError {
            msg: format!("unexpected role={other:?}; must be one of `robot` or `hcloud`"),
        }),
    }
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`]. Returns
/// `true` if `stop` was observed. Local copy of the digitalocean/ec2/k8s helper.
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
#[path = "hetzner_tests.rs"]
mod tests;
