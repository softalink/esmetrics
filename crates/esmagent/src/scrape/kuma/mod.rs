//! Kuma service-mesh service discovery (`kuma_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/kuma` (v1.146.0): [`client`] derives the
//! MADS request URL from `server` + bearer/basic/TLS auth and issues the
//! single `POST <server>/v3/discovery:monitoringassignments`, [`labels`] holds
//! the `DiscoveryResponse`/`MonitoringAssignment`/`Target` structs + the
//! `__meta_kuma_*` label builder, and [`KumaDiscovery`] (this file) is the
//! [`super::discovery::Discovery`] the scrape manager polls.
//!
//! ## Refresh model
//!
//! Mirrors `scrape::puppetdb`/`scrape::digitalocean`: a single background
//! thread re-runs the MADS fetch on a fixed interval
//! (`-promscrape.kumaSDCheckInterval`, default 30s — upstream
//! `kuma.SDCheckInterval`'s `30*time.Second`), publishing the target-group
//! snapshot behind a `Mutex`; [`KumaDiscovery::poll`] clones it.
//! [`wait_or_stop`] observes a `stop`/`Drop` promptly rather than after a full
//! interval.
//!
//! ## Startup robustness
//!
//! [`KumaDiscovery::new`] fails only on genuinely bad config (missing/bad
//! `server`, bad TLS material — see [`client::new_kuma_api`]). A Kuma control
//! plane that is unreachable at startup does NOT fail `new()`: the first fetch
//! happens on the background thread (retried at the refresh cadence), and
//! [`Discovery::poll`] returns an empty list until the first successful
//! refresh — matching the PuppetDB/DigitalOcean robustness choice.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{new_kuma_api, KumaApi};

use super::config::{RawAuthFields, RawBasicAuth, ScrapeError};
use super::discovery::{Discovery, TargetGroup};
use crate::client::{AuthConfig, TlsConfig};

pub mod client;
pub mod labels;

/// Default `kuma_sd_config` refresh interval, matching
/// `-promscrape.kumaSDCheckInterval`'s default (`kuma.SDCheckInterval` =
/// `30*time.Second`). `scrape::wiring::apply_kuma_sd_check_interval` overrides
/// it from the flag; [`build_kuma_sd_config`] seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Local `kuma_sd_config` shape. Port of `discovery/kuma.SDConfig`'s supported
/// fields (`server`, `client_id`, and the inline `HTTPClientConfig` bearer/
/// basic/TLS). Built via [`build_kuma_sd_config`] from its
/// [`RawKumaSdConfig`]. `refresh_interval` is not a YAML field (upstream reads
/// it from `-promscrape.kumaSDCheckInterval`); it defaults to
/// [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`super::puppetdb::PuppetdbSdConfig`], to keep `config.rs` under
/// the repo's 800-line file cap.
///
/// `Debug` is hand-written to redact the bearer token / basic password held in
/// `auth` (mirrors the PuppetDB/DigitalOcean secret redaction).
#[derive(Clone, PartialEq)]
pub struct KumaSdConfig {
    pub server: String,
    pub client_id: String,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
    pub refresh_interval: Duration,
}

impl Default for KumaSdConfig {
    fn default() -> Self {
        KumaSdConfig {
            server: String::new(),
            client_id: String::new(),
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

impl fmt::Debug for KumaSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KumaSdConfig")
            .field("server", &self.server)
            .field("client_id", &self.client_id)
            .field("auth", &"<redacted>")
            .field("tls", &self.tls)
            .field("refresh_interval", &self.refresh_interval)
            .finish()
    }
}

/// Undefaulted `kuma_sd_config` list-entry shape. `bearer_token` holds a
/// secret, so `Debug` is hand-written to redact it (this struct is reachable
/// from `scrape::config`'s `RawScrapeConfig`'s derived `Debug`). Inline
/// `basic_auth`/`bearer_token` need [`RawAuthFields`] conversion, same as the
/// other providers. Lives here (not in `scrape::config`) alongside
/// [`KumaSdConfig`] and [`build_kuma_sd_config`], keeping `config.rs` under the
/// repo's 800-line cap.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawKumaSdConfig {
    server: String,
    client_id: String,
    basic_auth: Option<RawBasicAuth>,
    bearer_token: Option<String>,
    tls_config: Option<TlsConfig>,
}

impl fmt::Debug for RawKumaSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawKumaSdConfig")
            .field("server", &self.server)
            .field("client_id", &self.client_id)
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

/// Builds a [`KumaSdConfig`] from its raw form, rejecting a missing `server`
/// at config-parse time (upstream's sole required field). `refresh_interval`
/// is seeded to the flag default and overridden by
/// `scrape::wiring::apply_kuma_sd_check_interval`. Full URL scheme/host
/// derivation is deferred to [`client::new_kuma_api`] (at discovery
/// construction), matching upstream's `newAPIConfig`.
pub(crate) fn build_kuma_sd_config(raw: RawKumaSdConfig) -> Result<KumaSdConfig, ScrapeError> {
    if raw.server.is_empty() {
        return Err(ScrapeError::new(
            "kuma_sd_config: `server` is required".to_string(),
        ));
    }
    let auth_fields = RawAuthFields {
        basic_auth: raw.basic_auth,
        bearer_token: raw.bearer_token,
    };
    Ok(KumaSdConfig {
        server: raw.server,
        client_id: raw.client_id,
        auth: auth_fields.into_auth_config(),
        tls: raw.tls_config.unwrap_or_default(),
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
    })
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes. Local copy of the puppetdb/digitalocean constant.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `kuma_sd_config` entry. A background thread re-runs
/// the MADS fetch on `refresh_interval`; [`poll`] clones the current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct KumaDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime.
struct RefreshCtx {
    api: KumaApi,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    source: String,
    refresh_interval: Duration,
}

impl KumaDiscovery {
    /// Builds the Kuma API client (failing only on bad config — see the module
    /// doc) and spawns the background refresh thread. The snapshot starts
    /// empty and is populated by the thread's first successful refresh.
    pub fn new(cfg: &KumaSdConfig, job: &str) -> Result<KumaDiscovery, ScrapeError> {
        let api = new_kuma_api(cfg)?;
        let snapshot = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let ctx = RefreshCtx {
            api,
            snapshot: Arc::clone(&snapshot),
            stop: Arc::clone(&stop),
            source: format!("{job}/kuma"),
            refresh_interval: cfg.refresh_interval,
        };

        let handle = thread::spawn(move || run(&ctx));

        Ok(KumaDiscovery {
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

impl Drop for KumaDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for KumaDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: re-run the MADS fetch on
/// `refresh_interval`. A fetch failure is logged and retried at the same
/// cadence, keeping the previous snapshot (so a transiently-unreachable Kuma
/// control plane never wipes discovered targets). Each assignment target
/// becomes its own single-target [`TargetGroup`] (mirroring upstream's flat
/// per-target label sets); they share the same `source`, which is allowed —
/// see [`TargetGroup::source`].
fn run(ctx: &RefreshCtx) {
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        match ctx.api.fetch_targets() {
            Ok(targets) => {
                let groups: Vec<TargetGroup> = targets
                    .into_iter()
                    .map(|t| TargetGroup {
                        targets: vec![t.address],
                        labels: t.labels,
                        source: ctx.source.clone(),
                    })
                    .collect();
                *ctx.snapshot.lock().unwrap() = groups;
            }
            Err(e) => log::warn!(
                "esmagent kuma_sd ({}): refresh failed, keeping last-good targets: {e}",
                ctx.source
            ),
        }

        if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
            return;
        }
    }
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`]. Returns
/// `true` if `stop` was observed. Local copy of the puppetdb/digitalocean
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
#[path = "kuma_tests.rs"]
mod tests;
