//! PuppetDB service discovery (`puppetdb_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/puppetdb` (v1.146.0): [`client`]
//! validates the endpoint + bearer/basic auth and issues the single
//! `POST /pdb/query/v4` PQL query, [`labels`] holds the `Resource` struct +
//! the `__meta_puppetdb_*` label builder, and [`PuppetdbDiscovery`] (this
//! file) is the [`super::discovery::Discovery`] the scrape manager polls.
//!
//! ## Refresh model
//!
//! Mirrors `scrape::digitalocean`/`scrape::nomad`: a single background thread
//! re-runs the query on a fixed interval
//! (`-promscrape.puppetdbSDCheckInterval`, default 30s — upstream
//! `puppetdb.SDCheckInterval`'s `30*time.Second`), publishing the target-group
//! snapshot behind a `Mutex`; [`PuppetdbDiscovery::poll`] clones it.
//! [`wait_or_stop`] observes a `stop`/`Drop` promptly rather than after a full
//! interval.
//!
//! ## Startup robustness
//!
//! [`PuppetdbDiscovery::new`] fails only on genuinely bad config (missing/bad
//! `url`, missing `query`, bad TLS material — see [`client::new_puppetdb_api`]).
//! A PuppetDB server that is unreachable at startup does NOT fail `new()`: the
//! first query happens on the background thread (retried at the refresh
//! cadence), and [`Discovery::poll`] returns an empty list until the first
//! successful refresh — matching the DigitalOcean/Nomad robustness choice.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{new_puppetdb_api, PuppetdbApi};
use labels::append_target_labels;

use super::config::{RawAuthFields, RawBasicAuth, ScrapeError};
use super::discovery::{Discovery, TargetGroup};
use crate::client::{AuthConfig, TlsConfig};

pub mod client;
pub mod labels;

/// Default `puppetdb_sd_config` refresh interval, matching
/// `-promscrape.puppetdbSDCheckInterval`'s default
/// (`puppetdb.SDCheckInterval` = `30*time.Second`).
/// `scrape::wiring::apply_puppetdb_sd_check_interval` overrides it from the
/// flag; [`build_puppetdb_sd_config`] seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Default `port` for the discovered target's `__address__` when a config
/// doesn't set one — matches upstream `puppetdb.newAPIConfig`'s `port = 80`.
pub const DEFAULT_PORT: u16 = 80;

/// Local `puppetdb_sd_config` shape. Port of `discovery/puppetdb.SDConfig`'s
/// supported fields. Built via [`build_puppetdb_sd_config`] from its
/// [`RawPuppetdbSdConfig`]. `refresh_interval` is not a YAML field (upstream
/// reads it from `-promscrape.puppetdbSDCheckInterval`); it defaults to
/// [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`super::digitalocean::DigitaloceanSdConfig`] /
/// [`super::nomad::NomadSdConfig`], to keep `config.rs` under the repo's
/// 800-line file cap.
///
/// `Debug` is hand-written to redact the bearer token / basic password held in
/// `auth` (mirrors the DigitalOcean/Nomad secret redaction).
#[derive(Clone, PartialEq)]
pub struct PuppetdbSdConfig {
    pub url: String,
    pub query: String,
    pub include_parameters: bool,
    pub port: u16,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
    pub refresh_interval: Duration,
}

impl Default for PuppetdbSdConfig {
    fn default() -> Self {
        PuppetdbSdConfig {
            url: String::new(),
            query: String::new(),
            include_parameters: false,
            port: DEFAULT_PORT,
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

impl fmt::Debug for PuppetdbSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PuppetdbSdConfig")
            .field("url", &self.url)
            .field("query", &self.query)
            .field("include_parameters", &self.include_parameters)
            .field("port", &self.port)
            .field("auth", &"<redacted>")
            .field("tls", &self.tls)
            .field("refresh_interval", &self.refresh_interval)
            .finish()
    }
}

/// Undefaulted `puppetdb_sd_config` list-entry shape. `bearer_token` holds a
/// secret, so `Debug` is hand-written to redact it (this struct is reachable
/// from `scrape::config`'s `RawScrapeConfig`'s derived `Debug`). Inline
/// `basic_auth`/`bearer_token` need [`RawAuthFields`] conversion, same as the
/// other providers. Lives here (not in `scrape::config`) alongside
/// [`PuppetdbSdConfig`] and [`build_puppetdb_sd_config`], keeping `config.rs`
/// under the repo's 800-line cap.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawPuppetdbSdConfig {
    url: String,
    query: String,
    include_parameters: bool,
    port: Option<u16>,
    basic_auth: Option<RawBasicAuth>,
    bearer_token: Option<String>,
    tls_config: Option<TlsConfig>,
}

impl fmt::Debug for RawPuppetdbSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawPuppetdbSdConfig")
            .field("url", &self.url)
            .field("query", &self.query)
            .field("include_parameters", &self.include_parameters)
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

/// Builds a [`PuppetdbSdConfig`] from its raw form, rejecting a missing `url`
/// or `query` at config-parse time (upstream's two required fields). `port`
/// defaults to [`DEFAULT_PORT`]; `refresh_interval` is seeded to the flag
/// default and overridden by
/// `scrape::wiring::apply_puppetdb_sd_check_interval`. Full URL scheme/host
/// validation is deferred to [`client::new_puppetdb_api`] (at discovery
/// construction), matching upstream's `newAPIConfig`.
pub(crate) fn build_puppetdb_sd_config(
    raw: RawPuppetdbSdConfig,
) -> Result<PuppetdbSdConfig, ScrapeError> {
    if raw.url.is_empty() {
        return Err(ScrapeError::new(
            "puppetdb_sd_config: `url` is required".to_string(),
        ));
    }
    if raw.query.is_empty() {
        return Err(ScrapeError::new(
            "puppetdb_sd_config: `query` is required".to_string(),
        ));
    }
    let auth_fields = RawAuthFields {
        basic_auth: raw.basic_auth,
        bearer_token: raw.bearer_token,
    };
    Ok(PuppetdbSdConfig {
        url: raw.url,
        query: raw.query,
        include_parameters: raw.include_parameters,
        port: raw.port.unwrap_or(DEFAULT_PORT),
        auth: auth_fields.into_auth_config(),
        tls: raw.tls_config.unwrap_or_default(),
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
    })
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes. Local copy of the digitalocean/nomad constant of the same
/// purpose.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `puppetdb_sd_config` entry. A background thread
/// re-runs the query on `refresh_interval`; [`poll`] clones the current
/// snapshot.
///
/// [`poll`]: Discovery::poll
pub struct PuppetdbDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime.
struct RefreshCtx {
    api: PuppetdbApi,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    job: String,
    query: String,
    include_parameters: bool,
    port: u16,
    refresh_interval: Duration,
}

impl PuppetdbDiscovery {
    /// Builds the PuppetDB API client (failing only on bad config — see the
    /// module doc) and spawns the background refresh thread. The snapshot
    /// starts empty and is populated by the thread's first successful refresh.
    pub fn new(cfg: &PuppetdbSdConfig, job: &str) -> Result<PuppetdbDiscovery, ScrapeError> {
        let api = new_puppetdb_api(cfg)?;
        let snapshot = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let ctx = RefreshCtx {
            api,
            snapshot: Arc::clone(&snapshot),
            stop: Arc::clone(&stop),
            job: job.to_string(),
            query: cfg.query.clone(),
            include_parameters: cfg.include_parameters,
            port: cfg.port,
            refresh_interval: cfg.refresh_interval,
        };

        let handle = thread::spawn(move || run(&ctx));

        Ok(PuppetdbDiscovery {
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

impl Drop for PuppetdbDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for PuppetdbDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: re-run the query on `refresh_interval`. A
/// query failure is logged and retried at the same cadence, keeping the
/// previous snapshot (so a transiently-unreachable PuppetDB never wipes
/// discovered targets).
fn run(ctx: &RefreshCtx) {
    let source = format!("{}/puppetdb", ctx.job);
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        match ctx.api.get_resources(&ctx.query) {
            Ok(resources) => {
                let groups: Vec<TargetGroup> = resources
                    .iter()
                    .map(|res| {
                        append_target_labels(
                            res,
                            &ctx.query,
                            ctx.include_parameters,
                            ctx.port,
                            source.clone(),
                        )
                    })
                    .collect();
                *ctx.snapshot.lock().unwrap() = groups;
            }
            Err(e) => log::warn!(
                "esmagent puppetdb_sd ({}): refresh failed, keeping last-good targets: {e}",
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
#[path = "puppetdb_tests.rs"]
mod tests;
