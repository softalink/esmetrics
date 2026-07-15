//! Yandex Cloud service discovery (`yandexcloud_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/yandexcloud` (v1.146.0): [`client`]
//! resolves auth (a static `yandex_passport_oauth_token` exchanged for an IAM
//! token, or the compute metadata-server IAM token), resolves the service
//! endpoints (`GET <api_endpoint>/endpoints`), and issues the paginated
//! resource-manager (organizations/clouds/folders) + compute (instances) GETs.
//! [`labels`] holds the instance structs + `__meta_yandexcloud_*` label
//! builder, and [`YandexcloudDiscovery`] (this file) is the
//! [`super::discovery::Discovery`] the scrape manager polls.
//!
//! ## Refresh model
//!
//! Mirrors `scrape::gce`: a single background thread re-lists instances on a
//! fixed interval (`-promscrape.yandexcloudSDCheckInterval`, default 30s —
//! upstream `yandexcloud.SDCheckInterval`'s `30*time.Second`), publishing the
//! target-group snapshot behind a `Mutex`; [`YandexcloudDiscovery::poll`] clones
//! it. [`wait_or_stop`] observes a `stop`/`Drop` promptly rather than after a
//! full interval. The service endpoints are resolved once at thread start
//! (retried at the refresh cadence on failure) — matching upstream
//! `newAPIConfig`, which resolves them once.
//!
//! ## Auth scope (SCOPED subset)
//!
//! Supported: a static `yandex_passport_oauth_token` (exchanged for an IAM token
//! at `<iam>/iam/v1/tokens`), else the compute **metadata-server IAM token**
//! (`GET http://169.254.169.254/computeMetadata/v1/instance/service-accounts/
//! default/token` with `Metadata-Flavor: Google`, cached to its `expires_in`).
//! The IAM token is cached until shortly before it expires.
//!
//! **Deferred** (rejected at build time): the service-account authorized-key
//! JSON (the JWT -> IAM-exchange flow). A set `service_account_key_file` is a
//! build-time error, the same reject-early convention GCE uses for
//! `credentials_file`.
//!
//! ## Startup robustness
//!
//! [`YandexcloudDiscovery::new`] fails only on a set `service_account_key_file`
//! (DEFERRED), a `service` other than `compute`, or a genuinely bad HTTP-client
//! build. Yandex Cloud / metadata being unreachable at startup does NOT fail
//! `new()`: the token fetch, endpoint resolution, and the first listing happen
//! on the background thread (retried at the refresh cadence), and
//! [`Discovery::poll`] returns an empty list until the first successful refresh
//! — matching the GCE/EC2/consul robustness choice.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{new_yandexcloud_api, ServiceEndpoints, YandexcloudApi};
use labels::add_instance_labels;

use super::config::ScrapeError;
use super::discovery::{Discovery, TargetGroup};
use crate::client::TlsConfig;

pub mod client;
pub mod labels;

/// Default `yandexcloud_sd_config` refresh interval, matching
/// `-promscrape.yandexcloudSDCheckInterval`'s default
/// (`yandexcloud.SDCheckInterval` = `30*time.Second`).
/// `scrape::wiring::apply_yandexcloud_sd_check_interval` overrides it from the
/// flag; [`build_yandexcloud_sd_config`] seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// The only `service` upstream supports.
const SUPPORTED_SERVICE: &str = "compute";

/// Local `yandexcloud_sd_config` shape. Port of
/// `discovery/yandexcloud.SDConfig`'s supported fields, plus the esmagent-only
/// `metadata_url` override (for tests, mirroring GCE's `metadata_url`) and
/// `service_account_key_file` (DEFERRED). Built via
/// [`build_yandexcloud_sd_config`] from its [`RawYandexcloudSdConfig`].
/// `refresh_interval` is not a YAML field (upstream reads it from
/// `-promscrape.yandexcloudSDCheckInterval`); it defaults to
/// [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`super::gce::GceSdConfig`], to keep `config.rs` under the repo's
/// 800-line file cap.
///
/// `Debug` is hand-written to redact `yandex_passport_oauth_token`.
#[derive(Clone, PartialEq)]
pub struct YandexcloudSdConfig {
    pub service: String,
    /// Static Passport OAuth token, exchanged for an IAM token. Wins over the
    /// metadata-server token when set.
    pub yandex_passport_oauth_token: Option<String>,
    /// API endpoint override (defaults to `https://api.cloud.yandex.net`) — a
    /// test can point it at a stub serving `/endpoints`.
    pub api_endpoint: Option<String>,
    /// If non-empty, list instances directly for these folders (skip the
    /// org -> cloud -> folder enumeration). Port of `FolderIDs`.
    pub folder_ids: Vec<String>,
    pub tls: TlsConfig,
    /// Compute metadata-server base override (defaults to
    /// `http://169.254.169.254`) — lets a test point the metadata IAM-token
    /// lookup at a stub (esmagent extension).
    pub metadata_url: Option<String>,
    /// Service-account authorized-key JSON file — DEFERRED. A non-empty value
    /// is rejected at build time (see [`YandexcloudDiscovery::new`] and
    /// [`build_yandexcloud_sd_config`]).
    pub service_account_key_file: Option<String>,
    pub refresh_interval: Duration,
}

impl Default for YandexcloudSdConfig {
    fn default() -> Self {
        YandexcloudSdConfig {
            service: String::new(),
            yandex_passport_oauth_token: None,
            api_endpoint: None,
            folder_ids: Vec::new(),
            tls: TlsConfig::default(),
            metadata_url: None,
            service_account_key_file: None,
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

impl fmt::Debug for YandexcloudSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("YandexcloudSdConfig")
            .field("service", &self.service)
            .field(
                "yandex_passport_oauth_token",
                &self
                    .yandex_passport_oauth_token
                    .as_ref()
                    .map(|_| "<redacted>"),
            )
            .field("api_endpoint", &self.api_endpoint)
            .field("folder_ids", &self.folder_ids)
            .field("tls", &self.tls)
            .field("metadata_url", &self.metadata_url)
            .field("service_account_key_file", &self.service_account_key_file)
            .field("refresh_interval", &self.refresh_interval)
            .finish()
    }
}

/// Undefaulted `yandexcloud_sd_config` list-entry shape.
/// `yandex_passport_oauth_token` holds a secret, so `Debug` is hand-written to
/// redact it (this struct is reachable from `scrape::config`'s
/// `RawScrapeConfig`'s derived `Debug`). Lives here (not in `scrape::config`)
/// alongside [`YandexcloudSdConfig`] and [`build_yandexcloud_sd_config`],
/// keeping `config.rs` under the repo's 800-line cap.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawYandexcloudSdConfig {
    service: String,
    yandex_passport_oauth_token: Option<String>,
    api_endpoint: Option<String>,
    folder_ids: Vec<String>,
    tls_config: Option<TlsConfig>,
    metadata_url: Option<String>,
    service_account_key_file: Option<String>,
}

impl fmt::Debug for RawYandexcloudSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawYandexcloudSdConfig")
            .field("service", &self.service)
            .field(
                "yandex_passport_oauth_token",
                &self
                    .yandex_passport_oauth_token
                    .as_ref()
                    .map(|_| "<redacted>"),
            )
            .field("api_endpoint", &self.api_endpoint)
            .field("folder_ids", &self.folder_ids)
            .field("tls_config", &self.tls_config)
            .field("metadata_url", &self.metadata_url)
            .field("service_account_key_file", &self.service_account_key_file)
            .finish()
    }
}

/// Builds a [`YandexcloudSdConfig`] from its raw form. Fails at build (parse)
/// time when `service_account_key_file` is set (the JWT -> IAM exchange is
/// DEFERRED) or when `service` is anything other than `compute` (the only
/// service upstream supports). `refresh_interval` is seeded to the flag default
/// and overridden by `scrape::wiring::apply_yandexcloud_sd_check_interval`.
pub(crate) fn build_yandexcloud_sd_config(
    raw: RawYandexcloudSdConfig,
) -> Result<YandexcloudSdConfig, ScrapeError> {
    if raw
        .service_account_key_file
        .as_deref()
        .is_some_and(|c| !c.is_empty())
    {
        return Err(ScrapeError::new(
            "yandexcloud_sd_config: unsupported (deferred): service-account key \
             (service_account_key_file)",
        ));
    }
    if raw.service != SUPPORTED_SERVICE {
        return Err(ScrapeError::new(format!(
            "yandexcloud_sd_config: unexpected service={:?}; only `compute` is supported",
            raw.service
        )));
    }
    Ok(YandexcloudSdConfig {
        service: raw.service,
        yandex_passport_oauth_token: raw.yandex_passport_oauth_token,
        api_endpoint: raw.api_endpoint,
        folder_ids: raw.folder_ids,
        tls: raw.tls_config.unwrap_or_default(),
        metadata_url: raw.metadata_url,
        service_account_key_file: raw.service_account_key_file,
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
    })
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes. Local copy of the GCE/consul constant of the same purpose.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `yandexcloud_sd_config` entry. A background thread
/// refreshes the target-group snapshot on `refresh_interval`; [`poll`] clones
/// the current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct YandexcloudDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime.
struct RefreshCtx {
    api: YandexcloudApi,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    job: String,
    /// Folder ids from config; empty means "enumerate via org -> cloud ->
    /// folder".
    folder_ids: Vec<String>,
    refresh_interval: Duration,
}

impl YandexcloudDiscovery {
    /// Builds the Yandex Cloud API client (failing on a set
    /// `service_account_key_file` — DEFERRED — a non-`compute` `service`, or a
    /// bad HTTP-client build; see the module doc) and spawns the background
    /// refresh thread. The snapshot starts empty and is populated by the
    /// thread's first successful refresh.
    pub fn new(cfg: &YandexcloudSdConfig, job: &str) -> Result<YandexcloudDiscovery, ScrapeError> {
        if cfg
            .service_account_key_file
            .as_deref()
            .is_some_and(|c| !c.is_empty())
        {
            return Err(ScrapeError::new(
                "yandexcloud_sd_config: unsupported (deferred): service-account key \
                 (service_account_key_file)",
            ));
        }
        if cfg.service != SUPPORTED_SERVICE {
            return Err(ScrapeError::new(format!(
                "yandexcloud_sd_config: unexpected service={:?}; only `compute` is supported",
                cfg.service
            )));
        }
        let api = new_yandexcloud_api(cfg)?;
        let snapshot = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let ctx = RefreshCtx {
            api,
            snapshot: Arc::clone(&snapshot),
            stop: Arc::clone(&stop),
            job: job.to_string(),
            folder_ids: cfg.folder_ids.clone(),
            refresh_interval: cfg.refresh_interval,
        };

        let handle = thread::spawn(move || run(&ctx));

        Ok(YandexcloudDiscovery {
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

impl Drop for YandexcloudDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for YandexcloudDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: resolve the service endpoints once (retried
/// at the refresh cadence on failure), then re-list instances on
/// `refresh_interval`. A list failure is logged and retried at the same cadence,
/// keeping the previous snapshot (so a transiently-unreachable Yandex Cloud API
/// never wipes discovered targets).
fn run(ctx: &RefreshCtx) {
    let eps = loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }
        match ctx.api.resolve_service_endpoints() {
            Ok(e) => break e,
            Err(e) => {
                log::warn!(
                    "esmagent yandexcloud_sd ({}): cannot resolve service endpoints: {e}",
                    ctx.job
                );
                if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
                    return;
                }
            }
        }
    };

    let source = format!("{}/yandexcloud", ctx.job);
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        match refresh(ctx, &eps, &source) {
            Ok(groups) => *ctx.snapshot.lock().unwrap() = groups,
            Err(e) => log::warn!(
                "esmagent yandexcloud_sd ({}): refresh failed, keeping last-good targets: {e}",
                ctx.job
            ),
        }

        if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
            return;
        }
    }
}

/// One full refresh: resolve the folder id list (config, else
/// org -> cloud -> folder enumeration), list instances per folder, and build a
/// [`TargetGroup`] per instance. Port of `getInstancesLabels`.
fn refresh(
    ctx: &RefreshCtx,
    eps: &ServiceEndpoints,
    source: &str,
) -> Result<Vec<TargetGroup>, ScrapeError> {
    let folder_ids = if ctx.folder_ids.is_empty() {
        let orgs = ctx.api.list_organizations(eps)?;
        let clouds = ctx.api.list_clouds(eps, &orgs)?;
        ctx.api.list_folders(eps, &clouds)?
    } else {
        ctx.folder_ids.clone()
    };

    let mut instances = Vec::new();
    for folder_id in &folder_ids {
        instances.extend(ctx.api.list_instances(eps, folder_id)?);
    }
    Ok(add_instance_labels(&instances, source))
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`]. Returns
/// `true` if `stop` was observed. Local copy of the GCE/consul helper.
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
#[path = "yandexcloud_tests.rs"]
mod tests;
