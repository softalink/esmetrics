//! Eureka (Netflix) service discovery (`eureka_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/eureka` (v1.146.0): [`client`] resolves
//! the server + bearer/basic auth and issues the single `GET <server>/apps`
//! listing, [`labels`] holds the XML response structs + the `__meta_eureka_*`
//! label builder, and [`EurekaDiscovery`] (this file) is the
//! [`super::discovery::Discovery`] the scrape manager polls.
//!
//! ## Refresh model
//!
//! Mirrors `scrape::digitalocean`/`scrape::ec2`: a single background thread
//! re-lists applications on a fixed interval
//! (`-promscrape.eurekaSDCheckInterval`, default 30s — upstream
//! `eureka.SDCheckInterval`'s `30*time.Second`), publishing the target-group
//! snapshot behind a `Mutex`; [`EurekaDiscovery::poll`] clones it.
//! [`wait_or_stop`] observes a `stop`/`Drop` promptly rather than after a full
//! interval.
//!
//! ## Startup robustness
//!
//! [`EurekaDiscovery::new`] fails only on genuinely bad config (bad TLS
//! material). A Eureka server that is unreachable at startup does NOT fail
//! `new()`: the first listing happens on the background thread (retried at the
//! refresh cadence), and [`Discovery::poll`] returns an empty list until the
//! first successful refresh — matching the digitalocean/ec2 robustness choice.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{new_eureka_api, EurekaApi};
use labels::append_target_labels;

use super::config::{RawAuthFields, RawBasicAuth, ScrapeError};
use super::discovery::{Discovery, TargetGroup};
use crate::client::{AuthConfig, TlsConfig};

pub mod client;
pub mod labels;

/// Default `eureka_sd_config` refresh interval, matching
/// `-promscrape.eurekaSDCheckInterval`'s default (upstream
/// `eureka.SDCheckInterval` = `30*time.Second`).
/// `scrape::wiring::apply_eureka_sd_check_interval` overrides it from the flag;
/// [`build_eureka_sd_config`] seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Local `eureka_sd_config` shape. Port of `discovery/eureka.SDConfig`'s
/// supported fields (`server` + basic/bearer/tls auth). Built via
/// [`build_eureka_sd_config`] from its [`RawEurekaSdConfig`].
/// `refresh_interval` is not a YAML field (upstream reads it from
/// `-promscrape.eurekaSDCheckInterval`); it defaults to
/// [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`super::digitalocean::DigitaloceanSdConfig`] /
/// [`super::ec2::Ec2SdConfig`], to keep `config.rs` under the repo's 800-line
/// cap.
///
/// `Debug` is hand-written to redact the bearer token / basic password held in
/// `auth` (mirrors the DigitalOcean/EC2 secret redaction).
#[derive(Clone, PartialEq)]
pub struct EurekaSdConfig {
    pub server: String,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
    pub refresh_interval: Duration,
}

impl Default for EurekaSdConfig {
    fn default() -> Self {
        EurekaSdConfig {
            server: String::new(),
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

impl fmt::Debug for EurekaSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EurekaSdConfig")
            .field("server", &self.server)
            .field("auth", &"<redacted>")
            .field("tls", &self.tls)
            .field("refresh_interval", &self.refresh_interval)
            .finish()
    }
}

/// Undefaulted `eureka_sd_config` list-entry shape. `bearer_token` holds a
/// secret, so `Debug` is hand-written to redact it (this struct is reachable
/// from `scrape::config`'s `RawScrapeConfig`'s derived `Debug`). Inline
/// `basic_auth`/`bearer_token` need [`RawAuthFields`] conversion, same as the
/// other providers. Lives here (not in `scrape::config`) alongside
/// [`EurekaSdConfig`] and [`build_eureka_sd_config`], keeping `config.rs` under
/// the repo's 800-line cap.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawEurekaSdConfig {
    server: String,
    basic_auth: Option<RawBasicAuth>,
    bearer_token: Option<String>,
    tls_config: Option<TlsConfig>,
}

impl fmt::Debug for RawEurekaSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawEurekaSdConfig")
            .field("server", &self.server)
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

/// Builds an [`EurekaSdConfig`] from its raw form. `refresh_interval` is seeded
/// to the flag default and overridden by
/// `scrape::wiring::apply_eureka_sd_check_interval`.
pub(crate) fn build_eureka_sd_config(raw: RawEurekaSdConfig) -> EurekaSdConfig {
    let auth_fields = RawAuthFields {
        basic_auth: raw.basic_auth,
        bearer_token: raw.bearer_token,
    };
    EurekaSdConfig {
        server: raw.server,
        auth: auth_fields.into_auth_config(),
        tls: raw.tls_config.unwrap_or_default(),
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
    }
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes. Local copy of the digitalocean/ec2 constant of the same purpose.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `eureka_sd_config` entry. A background thread
/// refreshes the target-group snapshot on `refresh_interval`; [`poll`] clones
/// the current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct EurekaDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime.
struct RefreshCtx {
    api: EurekaApi,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    source: String,
    refresh_interval: Duration,
}

impl EurekaDiscovery {
    /// Builds the Eureka API client (failing only on bad config — see the
    /// module doc) and spawns the background refresh thread. The snapshot
    /// starts empty and is populated by the thread's first successful refresh.
    pub fn new(cfg: &EurekaSdConfig, job: &str) -> Result<EurekaDiscovery, ScrapeError> {
        let api = new_eureka_api(cfg)?;
        let snapshot = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let ctx = RefreshCtx {
            api,
            snapshot: Arc::clone(&snapshot),
            stop: Arc::clone(&stop),
            source: format!("{job}/eureka"),
            refresh_interval: cfg.refresh_interval,
        };

        let handle = thread::spawn(move || run(&ctx));

        Ok(EurekaDiscovery {
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

impl Drop for EurekaDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for EurekaDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: re-list applications on `refresh_interval`.
/// A list failure is logged and retried at the same cadence, keeping the
/// previous snapshot (so a transiently-unreachable Eureka server never wipes
/// discovered targets).
fn run(ctx: &RefreshCtx) {
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        match ctx.api.list_applications() {
            Ok(apps) => {
                let groups = append_target_labels(&apps, &ctx.source);
                *ctx.snapshot.lock().unwrap() = groups;
            }
            Err(e) => log::warn!(
                "esmagent eureka_sd ({}): refresh failed, keeping last-good targets: {e}",
                ctx.source
            ),
        }

        if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
            return;
        }
    }
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`]. Returns
/// `true` if `stop` was observed. Local copy of the digitalocean/ec2 helper.
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
#[path = "eureka_tests.rs"]
mod tests;
