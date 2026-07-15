//! DigitalOcean service discovery (`digitalocean_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/digitalocean` (v1.146.0): [`client`]
//! resolves the endpoint + bearer-token auth and issues the paginated
//! `/v2/droplets` listing, [`labels`] holds the droplet structs + the
//! `__meta_digitalocean_*` label builder, and [`DigitaloceanDiscovery`] (this
//! file) is the [`super::discovery::Discovery`] the scrape manager polls.
//!
//! ## Refresh model
//!
//! Mirrors `scrape::consul`/`scrape::ec2`: a single background thread re-lists
//! droplets on a fixed interval (`-promscrape.digitaloceanSDCheckInterval`,
//! default 60s — upstream `digitalocean.SDCheckInterval`'s `time.Minute`),
//! publishing the target-group snapshot behind a `Mutex`;
//! [`DigitaloceanDiscovery::poll`] clones it. [`wait_or_stop`] observes a
//! `stop`/`Drop` promptly rather than after a full interval.
//!
//! ## Startup robustness
//!
//! [`DigitaloceanDiscovery::new`] fails only on genuinely bad config (bad TLS
//! material). A DigitalOcean API that is unreachable at startup does NOT fail
//! `new()`: the first listing happens on the background thread (retried at the
//! refresh cadence), and [`Discovery::poll`] returns an empty list until the
//! first successful refresh — matching the k8s/consul/ec2 robustness choice.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{new_digitalocean_api, DigitaloceanApi};
use labels::append_target_labels;

use super::config::{RawAuthFields, RawBasicAuth, ScrapeError};
use super::discovery::{Discovery, TargetGroup};
use crate::client::{AuthConfig, TlsConfig};

pub mod client;
pub mod labels;

/// Default `digitalocean_sd_config` refresh interval, matching
/// `-promscrape.digitaloceanSDCheckInterval`'s default
/// (`digitalocean.SDCheckInterval` = `time.Minute`).
/// `scrape::wiring::apply_digitalocean_sd_check_interval` overrides it from the
/// flag; `scrape::config::build_digitalocean_sd_config` seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Default `port` for the discovered target's `__address__` when a config
/// doesn't set one — matches upstream `digitalocean.newAPIConfig`'s
/// `cfg.port = 80`.
pub const DEFAULT_PORT: u16 = 80;

/// Local `digitalocean_sd_config` shape. Port of
/// `discovery/digitalocean.SDConfig`'s supported fields. Built via
/// `scrape::config::build_digitalocean_sd_config` from its
/// `RawDigitaloceanSdConfig`. `refresh_interval` is not a YAML field (upstream
/// reads it from `-promscrape.digitaloceanSDCheckInterval`); it defaults to
/// [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`super::consul::ConsulSdConfig`] / [`super::ec2::Ec2SdConfig`],
/// to keep `config.rs` under the repo's 800-line file cap.
///
/// `Debug` is hand-written to redact the bearer token held in `auth` (mirrors
/// `Ec2SdConfig`'s secret redaction).
#[derive(Clone, PartialEq)]
pub struct DigitaloceanSdConfig {
    pub server: String,
    pub port: u16,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
    pub refresh_interval: Duration,
}

impl Default for DigitaloceanSdConfig {
    fn default() -> Self {
        DigitaloceanSdConfig {
            server: String::new(),
            port: DEFAULT_PORT,
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

impl fmt::Debug for DigitaloceanSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DigitaloceanSdConfig")
            .field("server", &self.server)
            .field("port", &self.port)
            .field("auth", &"<redacted>")
            .field("tls", &self.tls)
            .field("refresh_interval", &self.refresh_interval)
            .finish()
    }
}

/// Undefaulted `digitalocean_sd_config` list-entry shape. `bearer_token` holds
/// a secret, so `Debug` is hand-written to redact it (this struct is reachable
/// from `scrape::config`'s `RawScrapeConfig`'s derived `Debug`). Inline
/// `basic_auth`/`bearer_token` need [`RawAuthFields`] conversion, same as
/// `RawHttpSdConfig`. Lives here (not in `scrape::config`) alongside
/// [`DigitaloceanSdConfig`] and [`build_digitalocean_sd_config`], keeping
/// `config.rs` under the repo's 800-line cap.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawDigitaloceanSdConfig {
    server: String,
    port: Option<u16>,
    basic_auth: Option<RawBasicAuth>,
    bearer_token: Option<String>,
    tls_config: Option<TlsConfig>,
}

impl fmt::Debug for RawDigitaloceanSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawDigitaloceanSdConfig")
            .field("server", &self.server)
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

/// Builds a [`DigitaloceanSdConfig`] from its raw form. `port` defaults to
/// [`DEFAULT_PORT`] (`digitalocean.newAPIConfig`); `refresh_interval` is
/// seeded to the flag default and overridden by
/// `scrape::wiring::apply_digitalocean_sd_check_interval`.
pub(crate) fn build_digitalocean_sd_config(raw: RawDigitaloceanSdConfig) -> DigitaloceanSdConfig {
    let auth_fields = RawAuthFields {
        basic_auth: raw.basic_auth,
        bearer_token: raw.bearer_token,
    };
    DigitaloceanSdConfig {
        server: raw.server,
        port: raw.port.unwrap_or(DEFAULT_PORT),
        auth: auth_fields.into_auth_config(),
        tls: raw.tls_config.unwrap_or_default(),
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
    }
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes. Local copy of the consul/ec2/k8s constant of the same purpose.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `digitalocean_sd_config` entry. A background thread
/// refreshes the target-group snapshot on `refresh_interval`; [`poll`] clones
/// the current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct DigitaloceanDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime.
struct RefreshCtx {
    api: DigitaloceanApi,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    job: String,
    port: u16,
    refresh_interval: Duration,
}

impl DigitaloceanDiscovery {
    /// Builds the DigitalOcean API client (failing only on bad config — see
    /// the module doc) and spawns the background refresh thread. The snapshot
    /// starts empty and is populated by the thread's first successful refresh.
    pub fn new(
        cfg: &DigitaloceanSdConfig,
        job: &str,
    ) -> Result<DigitaloceanDiscovery, ScrapeError> {
        let api = new_digitalocean_api(cfg)?;
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

        Ok(DigitaloceanDiscovery {
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

impl Drop for DigitaloceanDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for DigitaloceanDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: re-list droplets on `refresh_interval`. A
/// list failure is logged and retried at the same cadence, keeping the
/// previous snapshot (so a transiently-unreachable DigitalOcean API never
/// wipes discovered targets).
fn run(ctx: &RefreshCtx) {
    let source = format!("{}/digitalocean", ctx.job);
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        match ctx.api.list_droplets() {
            Ok(droplets) => {
                let groups = append_target_labels(&droplets, ctx.port, &source);
                *ctx.snapshot.lock().unwrap() = groups;
            }
            Err(e) => log::warn!(
                "esmagent digitalocean_sd ({}): refresh failed, keeping last-good targets: {e}",
                ctx.job
            ),
        }

        if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
            return;
        }
    }
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`]. Returns
/// `true` if `stop` was observed. Local copy of the consul/ec2/k8s helper.
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
#[path = "digitalocean_tests.rs"]
mod tests;
