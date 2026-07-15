//! Azure service discovery (`azure_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/azure` (v1.146.0): [`client`] resolves
//! auth (an `OAuth` client-credentials token from the Active Directory
//! endpoint, or a `ManagedIdentity` token from the Azure IMDS), selects the
//! cloud-environment ARM/AD endpoints, and issues the paginated VM / VMSS-VM
//! `Microsoft.Compute/virtualMachines` GETs + per-VM NIC resolution;
//! [`labels`] holds the response structs + `__meta_azure_*` label builder; and
//! [`AzureDiscovery`] (this file) is the [`super::discovery::Discovery`] the
//! scrape manager polls.
//!
//! ## Refresh model
//!
//! Mirrors `scrape::gce`/`scrape::ec2`: a single background thread re-lists VMs
//! on a fixed interval (`-promscrape.azureSDCheckInterval`, default 60s —
//! upstream `azure.SDCheckInterval`'s `time.Minute`), publishing the
//! target-group snapshot behind a `Mutex`; [`AzureDiscovery::poll`] clones it.
//! [`wait_or_stop`] observes a `stop`/`Drop` promptly rather than after a full
//! interval.
//!
//! ## Auth scope
//!
//! Both upstream `authentication_method`s are supported: `OAuth`
//! (`client_id`/`client_secret`/`tenant_id` -> a bearer token POSTed to
//! `<AD>/<tenant>/oauth2/token` with `resource=<ARM>`) and `ManagedIdentity`
//! (a token GET to the Azure IMDS with `Metadata: true`). The token is cached
//! until shortly before expiry. Cloud environments AzureCloud/AzurePublicCloud,
//! AzureChinaCloud, AzureGermanCloud, and AzureUSGovernment are supported
//! (`AzureStackCloud`'s file-based endpoints are not — override the endpoints
//! directly instead). The NIC-resolution worker pool is done sequentially
//! here (a deliberate simplification of upstream's `AvailableCPUs()*10`
//! goroutines).
//!
//! ## Startup robustness
//!
//! [`AzureDiscovery::new`] fails only on bad config (unknown `environment`,
//! unsupported `authentication_method`, or missing `OAuth` credentials — the
//! same checks `build_azure_sd_config` applies at parse time) or a genuinely
//! bad HTTP-client build. Azure being unreachable at startup does NOT fail
//! `new()`: the token fetch and the first VM listing happen on the background
//! thread (retried at the refresh cadence), and [`Discovery::poll`] returns an
//! empty list until the first successful refresh — matching the GCE/EC2
//! robustness choice.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{new_azure_api, AzureApi};
use labels::append_machine_labels;

use super::config::ScrapeError;
use super::discovery::{Discovery, TargetGroup};

pub mod client;
pub mod labels;

/// Default `azure_sd_config` refresh interval, matching
/// `-promscrape.azureSDCheckInterval`'s default (`azure.SDCheckInterval` =
/// `time.Minute`). `scrape::wiring::apply_azure_sd_check_interval` overrides it
/// from the flag; [`build_azure_sd_config`] seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Default `port` for the discovered target's `__address__` when a config
/// doesn't set one — matches upstream `azure.newAPIConfig`'s `port := 80`.
const DEFAULT_PORT: u16 = 80;

/// Local `azure_sd_config` shape. Port of `discovery/azure.SDConfig`'s
/// supported fields, plus esmagent-specific endpoint overrides (for tests, so
/// a stub can serve the ARM / AD / IMDS endpoints). Built via
/// [`build_azure_sd_config`] from its [`RawAzureSdConfig`]. `refresh_interval`
/// is not a YAML field (upstream reads it from `-promscrape.azureSDCheckInterval`);
/// it defaults to [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`super::gce::GceSdConfig`] / [`super::ec2::Ec2SdConfig`], to keep
/// `config.rs` under the repo's 800-line file cap.
///
/// `Debug` is hand-written to redact `client_secret`.
#[derive(Clone, PartialEq)]
pub struct AzureSdConfig {
    /// Cloud environment name (default `AzureCloud`). See
    /// [`client`]'s `cloud_env_by_name` for supported values.
    pub environment: String,
    /// `OAuth` (default) or `ManagedIdentity`.
    pub authentication_method: String,
    pub subscription_id: String,
    pub tenant_id: String,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub resource_group: String,
    pub port: u16,
    /// ARM (Resource Manager) base override (defaults to the environment's
    /// endpoint) — lets a test point at a stub.
    pub resource_manager_endpoint: Option<String>,
    /// Active Directory base override for `OAuth` token requests (defaults to
    /// the environment's endpoint) — lets a test point at a stub.
    pub active_directory_endpoint: Option<String>,
    /// Azure IMDS base override for `ManagedIdentity` token requests (defaults
    /// to `http://169.254.169.254`) — lets a test point at a stub.
    pub imds_endpoint: Option<String>,
    pub refresh_interval: Duration,
}

impl Default for AzureSdConfig {
    fn default() -> Self {
        AzureSdConfig {
            environment: String::new(),
            authentication_method: String::new(),
            subscription_id: String::new(),
            tenant_id: String::new(),
            client_id: String::new(),
            client_secret: None,
            resource_group: String::new(),
            port: DEFAULT_PORT,
            resource_manager_endpoint: None,
            active_directory_endpoint: None,
            imds_endpoint: None,
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

impl std::fmt::Debug for AzureSdConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureSdConfig")
            .field("environment", &self.environment)
            .field("authentication_method", &self.authentication_method)
            .field("subscription_id", &self.subscription_id)
            .field("tenant_id", &self.tenant_id)
            .field("client_id", &self.client_id)
            .field(
                "client_secret",
                &self.client_secret.as_ref().map(|_| "<redacted>"),
            )
            .field("resource_group", &self.resource_group)
            .field("port", &self.port)
            .field("resource_manager_endpoint", &self.resource_manager_endpoint)
            .field("active_directory_endpoint", &self.active_directory_endpoint)
            .field("imds_endpoint", &self.imds_endpoint)
            .field("refresh_interval", &self.refresh_interval)
            .finish()
    }
}

/// Undefaulted `azure_sd_config` list-entry shape. `client_secret` holds a
/// secret, so `Debug` is hand-written to redact it (this struct is reachable
/// from `scrape::config`'s `RawScrapeConfig`'s derived `Debug`). Lives here
/// (not in `scrape::config`) alongside [`AzureSdConfig`] and
/// [`build_azure_sd_config`], keeping `config.rs` under the repo's 800-line
/// cap; `scrape::config` imports it for `RawScrapeConfig`.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawAzureSdConfig {
    environment: String,
    authentication_method: String,
    subscription_id: String,
    tenant_id: String,
    client_id: String,
    client_secret: Option<String>,
    resource_group: String,
    port: Option<u16>,
    resource_manager_endpoint: Option<String>,
    active_directory_endpoint: Option<String>,
    imds_endpoint: Option<String>,
}

impl std::fmt::Debug for RawAzureSdConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawAzureSdConfig")
            .field("environment", &self.environment)
            .field("authentication_method", &self.authentication_method)
            .field("subscription_id", &self.subscription_id)
            .field("tenant_id", &self.tenant_id)
            .field("client_id", &self.client_id)
            .field(
                "client_secret",
                &self.client_secret.as_ref().map(|_| "<redacted>"),
            )
            .field("resource_group", &self.resource_group)
            .field("port", &self.port)
            .finish()
    }
}

/// Builds an [`AzureSdConfig`] from its raw form, validating at parse time
/// (reject-early, matching the other providers): `subscription_id` required;
/// `authentication_method` must be `OAuth`/`ManagedIdentity` (empty defaults to
/// `OAuth`); `OAuth` additionally requires `tenant_id`/`client_id`/
/// `client_secret`. `port` defaults to 80; `refresh_interval` is seeded to the
/// flag default and overridden by
/// `scrape::wiring::apply_azure_sd_check_interval`.
pub(crate) fn build_azure_sd_config(raw: RawAzureSdConfig) -> Result<AzureSdConfig, ScrapeError> {
    if raw.subscription_id.is_empty() {
        return Err(ScrapeError::new(
            "azure_sd_config: missing `subscription_id`",
        ));
    }
    let method = raw.authentication_method.to_lowercase();
    match method.as_str() {
        "" | "oauth" => {
            let secret_set = raw.client_secret.as_deref().is_some_and(|s| !s.is_empty());
            if raw.tenant_id.is_empty() || raw.client_id.is_empty() || !secret_set {
                return Err(ScrapeError::new(
                    "azure_sd_config: `authentication_method: OAuth` requires `tenant_id`, \
                     `client_id`, and `client_secret`",
                ));
            }
        }
        "managedidentity" => {}
        other => {
            return Err(ScrapeError::new(format!(
                "azure_sd_config: unsupported `authentication_method: {other:?}`; only \
                 `OAuth` and `ManagedIdentity` are supported"
            )));
        }
    }
    Ok(AzureSdConfig {
        environment: raw.environment,
        authentication_method: raw.authentication_method,
        subscription_id: raw.subscription_id,
        tenant_id: raw.tenant_id,
        client_id: raw.client_id,
        client_secret: raw.client_secret,
        resource_group: raw.resource_group,
        port: raw.port.unwrap_or(DEFAULT_PORT),
        resource_manager_endpoint: raw.resource_manager_endpoint,
        active_directory_endpoint: raw.active_directory_endpoint,
        imds_endpoint: raw.imds_endpoint,
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
    })
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes. Local copy of the GCE/EC2/consul constant of the same purpose.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `azure_sd_config` entry. A background thread
/// refreshes the target-group snapshot on `refresh_interval`; [`poll`] clones
/// the current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct AzureDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime.
struct RefreshCtx {
    api: AzureApi,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    job: String,
    subscription_id: String,
    tenant_id: String,
    port: u16,
    refresh_interval: Duration,
}

impl AzureDiscovery {
    /// Builds the Azure ARM client (failing on bad config — see the module doc
    /// — or a bad HTTP-client build) and spawns the background refresh thread.
    /// The snapshot starts empty and is populated by the thread's first
    /// successful refresh.
    pub fn new(cfg: &AzureSdConfig, job: &str) -> Result<AzureDiscovery, ScrapeError> {
        let api = new_azure_api(cfg)?;
        let snapshot = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let ctx = RefreshCtx {
            api,
            snapshot: Arc::clone(&snapshot),
            stop: Arc::clone(&stop),
            job: job.to_string(),
            subscription_id: cfg.subscription_id.clone(),
            tenant_id: cfg.tenant_id.clone(),
            port: cfg.port,
            refresh_interval: cfg.refresh_interval,
        };

        let handle = thread::spawn(move || run(&ctx));

        Ok(AzureDiscovery {
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

impl Drop for AzureDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for AzureDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: re-list VMs on `refresh_interval`. A list
/// failure is logged and retried at the same cadence, keeping the previous
/// snapshot (so a transiently-unreachable ARM API never wipes discovered
/// targets).
fn run(ctx: &RefreshCtx) {
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }
        match refresh(ctx) {
            Ok(groups) => *ctx.snapshot.lock().unwrap() = groups,
            Err(e) => log::warn!(
                "esmagent azure_sd ({}): refresh failed, keeping last-good targets: {e}",
                ctx.job
            ),
        }
        if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
            return;
        }
    }
}

/// One full refresh: list the VMs (+ VMSS VMs, + NIC IPs) and build a
/// [`TargetGroup`] per VM private IP.
fn refresh(ctx: &RefreshCtx) -> Result<Vec<TargetGroup>, ScrapeError> {
    let vms = ctx.api.get_virtual_machines()?;
    let source = format!("{}/azure", ctx.job);
    let mut groups = Vec::new();
    for vm in &vms {
        groups.extend(append_machine_labels(
            vm,
            &ctx.subscription_id,
            &ctx.tenant_id,
            ctx.port,
            &source,
        ));
    }
    Ok(groups)
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`]. Returns
/// `true` if `stop` was observed. Local copy of the GCE/EC2/consul helper.
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
mod build_tests {
    use super::*;

    #[test]
    fn oauth_requires_credentials() {
        let mut raw = RawAzureSdConfig {
            subscription_id: "sub".into(),
            ..RawAzureSdConfig::default()
        };
        // Missing tenant/client/secret -> rejected.
        assert!(build_azure_sd_config(RawAzureSdConfig {
            subscription_id: "sub".into(),
            ..RawAzureSdConfig::default()
        })
        .is_err());
        raw.tenant_id = "t".into();
        raw.client_id = "c".into();
        raw.client_secret = Some("s".into());
        let cfg = build_azure_sd_config(raw).unwrap();
        assert_eq!(cfg.subscription_id, "sub");
        assert_eq!(cfg.port, 80);
        assert_eq!(cfg.refresh_interval, DEFAULT_REFRESH_INTERVAL);
    }

    #[test]
    fn managed_identity_needs_no_oauth_credentials() {
        let cfg = build_azure_sd_config(RawAzureSdConfig {
            subscription_id: "sub".into(),
            authentication_method: "ManagedIdentity".into(),
            ..RawAzureSdConfig::default()
        })
        .unwrap();
        assert_eq!(cfg.authentication_method, "ManagedIdentity");
    }

    #[test]
    fn missing_subscription_id_is_rejected() {
        assert!(build_azure_sd_config(RawAzureSdConfig::default())
            .unwrap_err()
            .msg
            .contains("subscription_id"));
    }

    #[test]
    fn bad_authentication_method_is_rejected() {
        let err = build_azure_sd_config(RawAzureSdConfig {
            subscription_id: "sub".into(),
            authentication_method: "Kerberos".into(),
            ..RawAzureSdConfig::default()
        })
        .unwrap_err();
        assert!(err.msg.contains("authentication_method"), "{}", err.msg);
    }
}

#[cfg(test)]
#[path = "azure_tests.rs"]
mod tests;
