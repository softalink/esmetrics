//! Marathon (Mesosphere) service discovery (`marathon_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/marathon` (v1.146.0): the [`client`] does
//! per-server auth/scheme normalization and the `/v2/apps` query, [`labels`]
//! holds the `App`/`Task`/`PortMapping`/`PortDefinition` structs + the
//! `__meta_marathon_*` label builder, and [`MarathonDiscovery`] (this file) is
//! the [`super::discovery::Discovery`] the scrape manager polls.
//!
//! ## Refresh model
//!
//! Mirrors `scrape::digitalocean`/`scrape::nomad`: a single background thread
//! re-lists apps on a fixed interval (`-promscrape.marathonSDCheckInterval`,
//! default 30s — upstream `marathon.SDCheckInterval`'s `30*time.Second`),
//! publishing the target-group snapshot behind a `Mutex`;
//! [`MarathonDiscovery::poll`] clones it. [`wait_or_stop`] observes a
//! `stop`/`Drop` promptly rather than after a full interval.
//!
//! ## Startup robustness
//!
//! [`MarathonDiscovery::new`] fails only on genuinely bad config (bad TLS
//! material). A Marathon server that is unreachable at startup does NOT fail
//! `new()`: the first listing happens on the background thread (retried at the
//! refresh cadence), and [`Discovery::poll`] returns an empty list until the
//! first successful refresh — matching the digitalocean/nomad robustness
//! choice.
//!
//! ## Auth deviation from the task brief (faithful to upstream v1.146.0)
//!
//! The task brief describes `auth_token`/`auth_token_file` →
//! `Authorization: token=<t>`. Upstream v1.146.0's `marathon.SDConfig` has no
//! such field; auth is entirely `promauth.HTTPClientConfig` (bearer / basic /
//! TLS). This port matches upstream: no `auth_token`, bearer-else-basic auth.
//! See [`client`]'s module doc.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{new_marathon_api, MarathonApi};
use labels::append_apps_labels;

use super::config::{RawAuthFields, RawBasicAuth, ScrapeError};
use super::discovery::{Discovery, TargetGroup};
use crate::client::{AuthConfig, TlsConfig};

pub mod client;
pub mod labels;

/// Default `marathon_sd_config` refresh interval, matching
/// `-promscrape.marathonSDCheckInterval`'s default (upstream
/// `marathon.SDCheckInterval` = `30*time.Second`).
/// `scrape::wiring::apply_marathon_sd_check_interval` overrides it from the
/// flag; [`build_marathon_sd_config`] seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Local `marathon_sd_config` shape. Port of `discovery/marathon.SDConfig`'s
/// supported fields (`servers` + `HTTPClientConfig` bearer/basic/TLS auth).
/// Built via [`build_marathon_sd_config`] from its [`RawMarathonSdConfig`].
/// `refresh_interval` is not a YAML field (upstream reads it from
/// `-promscrape.marathonSDCheckInterval`); it defaults to
/// [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`super::nomad::NomadSdConfig`] /
/// [`super::digitalocean::DigitaloceanSdConfig`], to keep `config.rs` under the
/// repo's 800-line file cap.
///
/// `Debug` is hand-written to redact the bearer token / basic password held in
/// `auth` (mirrors the DigitalOcean/Nomad secret redaction).
#[derive(Clone, PartialEq)]
pub struct MarathonSdConfig {
    pub servers: Vec<String>,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
    pub refresh_interval: Duration,
}

impl Default for MarathonSdConfig {
    fn default() -> Self {
        MarathonSdConfig {
            servers: Vec::new(),
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

impl fmt::Debug for MarathonSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MarathonSdConfig")
            .field("servers", &self.servers)
            .field("auth", &"<redacted>")
            .field("tls", &self.tls)
            .field("refresh_interval", &self.refresh_interval)
            .finish()
    }
}

/// Undefaulted `marathon_sd_config` list-entry shape. `bearer_token` holds a
/// secret, so `Debug` is hand-written to redact it (this struct is reachable
/// from `scrape::config`'s `RawScrapeConfig`'s derived `Debug`). Inline
/// `basic_auth`/`bearer_token` need [`RawAuthFields`] conversion, same as the
/// other providers. Lives here (not in `scrape::config`) alongside
/// [`MarathonSdConfig`] and [`build_marathon_sd_config`], keeping `config.rs`
/// under the repo's 800-line cap.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawMarathonSdConfig {
    servers: Vec<String>,
    basic_auth: Option<RawBasicAuth>,
    bearer_token: Option<String>,
    tls_config: Option<TlsConfig>,
}

impl fmt::Debug for RawMarathonSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawMarathonSdConfig")
            .field("servers", &self.servers)
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

/// Builds a [`MarathonSdConfig`] from its raw form. `refresh_interval` is
/// seeded to the flag default and overridden by
/// `scrape::wiring::apply_marathon_sd_check_interval`.
pub(crate) fn build_marathon_sd_config(raw: RawMarathonSdConfig) -> MarathonSdConfig {
    let auth_fields = RawAuthFields {
        basic_auth: raw.basic_auth,
        bearer_token: raw.bearer_token,
    };
    MarathonSdConfig {
        servers: raw.servers,
        auth: auth_fields.into_auth_config(),
        tls: raw.tls_config.unwrap_or_default(),
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
    }
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes. Local copy of the digitalocean/nomad constant of the same
/// purpose.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `marathon_sd_config` entry. A background thread
/// refreshes the target-group snapshot on `refresh_interval`; [`poll`] clones
/// the current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct MarathonDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime.
struct RefreshCtx {
    api: MarathonApi,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    job: String,
    refresh_interval: Duration,
}

impl MarathonDiscovery {
    /// Builds the Marathon API client (failing only on bad config — see the
    /// module doc) and spawns the background refresh thread. The snapshot
    /// starts empty and is populated by the thread's first successful refresh.
    pub fn new(cfg: &MarathonSdConfig, job: &str) -> Result<MarathonDiscovery, ScrapeError> {
        let api = new_marathon_api(cfg)?;
        let snapshot = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let ctx = RefreshCtx {
            api,
            snapshot: Arc::clone(&snapshot),
            stop: Arc::clone(&stop),
            job: job.to_string(),
            refresh_interval: cfg.refresh_interval,
        };

        let handle = thread::spawn(move || run(&ctx));

        Ok(MarathonDiscovery {
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

impl Drop for MarathonDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for MarathonDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: re-list apps on `refresh_interval`. A list
/// failure is logged and retried at the same cadence, keeping the previous
/// snapshot (so a transiently-unreachable Marathon never wipes discovered
/// targets).
fn run(ctx: &RefreshCtx) {
    let source = format!("{}/marathon", ctx.job);
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        match ctx.api.get_apps() {
            Ok(apps) => {
                let groups = append_apps_labels(&apps, &source);
                *ctx.snapshot.lock().unwrap() = groups;
            }
            Err(e) => log::warn!(
                "esmagent marathon_sd ({}): refresh failed, keeping last-good targets: {e}",
                ctx.job
            ),
        }

        if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
            return;
        }
    }
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`]. Returns
/// `true` if `stop` was observed. Local copy of the digitalocean/nomad helper.
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
#[path = "marathon_tests.rs"]
mod tests;
