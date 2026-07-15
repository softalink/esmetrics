//! OpenStack service discovery (`openstack_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/openstack` (v1.146.0). The provider covers
//! both roles: [`auth`] builds the Keystone v3 auth-request body (password AND
//! application-credential methods) and parses the service catalog, [`client`]
//! obtains + caches the Keystone token, resolves the Nova/compute endpoint, and
//! issues the paginated Nova GETs, [`instance`] / [`hypervisor`] hold the
//! per-role response structs + `__meta_openstack_*` label builders, and
//! [`OpenstackDiscovery`] (this file) is the [`super::discovery::Discovery`] the
//! scrape manager polls.
//!
//! ## Refresh model
//!
//! Mirrors `scrape::gce`/`scrape::hetzner`: a single background thread re-lists
//! on a fixed interval (`-promscrape.openstackSDCheckInterval`, default 30s —
//! upstream `openstack.SDCheckInterval`'s `30*time.Second`), publishing the
//! target-group snapshot behind a `Mutex`; [`OpenstackDiscovery::poll`] clones
//! it. [`wait_or_stop`] observes a `stop`/`Drop` promptly rather than after a
//! full interval.
//!
//! ## Auth methods
//!
//! Supported: Keystone v3 **password** auth (with `domain_id`/`domain_name`
//! scoping) and **application-credential** auth (id+secret; or name+secret with
//! user/domain). When `identity_endpoint` is unset, credentials fall back to
//! the standard `OS_*` environment variables. **Deferred:** the legacy `v2.0`
//! identity endpoint is rejected at build time (upstream also rejects it).
//!
//! ## Startup robustness
//!
//! [`OpenstackDiscovery::new`] fails only on genuinely bad config (unknown
//! `role`, missing/invalid auth fields, unsupported `v2.0` endpoint, bad TLS
//! material). An OpenStack API unreachable at startup does NOT fail `new()`: the
//! first token fetch + listing happen on the background thread (retried at the
//! refresh cadence), and [`Discovery::poll`] returns an empty list until the
//! first successful refresh — matching the gce/hetzner choice.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{new_openstack_api, OpenstackApi};
use hypervisor::add_hypervisor_labels;
use instance::add_instance_labels;

use super::config::ScrapeError;
use super::discovery::{Discovery, TargetGroup};
use crate::client::TlsConfig;

pub mod auth;
pub mod client;
pub mod hypervisor;
pub mod instance;

/// The `role: instance` discriminant (Nova servers).
pub const ROLE_INSTANCE: &str = "instance";
/// The `role: hypervisor` discriminant (Nova hypervisors).
pub const ROLE_HYPERVISOR: &str = "hypervisor";

/// Default `openstack_sd_config` refresh interval, matching
/// `-promscrape.openstackSDCheckInterval`'s default
/// (`openstack.SDCheckInterval` = `30*time.Second`).
/// `scrape::wiring::apply_openstack_sd_check_interval` overrides it from the
/// flag; [`build_openstack_sd_config`] seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Default `port` for a discovered target's `__address__` when a config doesn't
/// set one — matches upstream `openstack.newAPIConfig`'s `port = 80`.
pub const DEFAULT_PORT: u16 = 80;

/// Local `openstack_sd_config` shape. Port of `discovery/openstack.SDConfig`'s
/// supported fields. Built via [`build_openstack_sd_config`] from its
/// [`RawOpenstackSdConfig`]. `refresh_interval` is not a YAML field (upstream
/// reads it from `-promscrape.openstackSDCheckInterval`); it defaults to
/// [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`super::ovhcloud::OvhcloudSdConfig`], to keep `config.rs` under
/// the repo's 800-line file cap.
///
/// `Debug` is hand-written to redact `password` and
/// `application_credential_secret`.
#[derive(Clone, PartialEq)]
pub struct OpenstackSdConfig {
    pub identity_endpoint: String,
    pub username: String,
    pub userid: String,
    pub password: Option<String>,
    pub project_name: String,
    pub project_id: String,
    pub domain_name: String,
    pub domain_id: String,
    pub application_credential_name: String,
    pub application_credential_id: String,
    pub application_credential_secret: Option<String>,
    pub role: String,
    pub region: String,
    pub port: u16,
    pub all_tenants: bool,
    pub availability: String,
    pub tls: TlsConfig,
    pub refresh_interval: Duration,
}

impl Default for OpenstackSdConfig {
    fn default() -> Self {
        OpenstackSdConfig {
            identity_endpoint: String::new(),
            username: String::new(),
            userid: String::new(),
            password: None,
            project_name: String::new(),
            project_id: String::new(),
            domain_name: String::new(),
            domain_id: String::new(),
            application_credential_name: String::new(),
            application_credential_id: String::new(),
            application_credential_secret: None,
            role: String::new(),
            region: String::new(),
            port: DEFAULT_PORT,
            all_tenants: false,
            availability: String::new(),
            tls: TlsConfig::default(),
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

impl fmt::Debug for OpenstackSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenstackSdConfig")
            .field("identity_endpoint", &self.identity_endpoint)
            .field("username", &self.username)
            .field("userid", &self.userid)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("project_name", &self.project_name)
            .field("project_id", &self.project_id)
            .field("domain_name", &self.domain_name)
            .field("domain_id", &self.domain_id)
            .field(
                "application_credential_name",
                &self.application_credential_name,
            )
            .field("application_credential_id", &self.application_credential_id)
            .field(
                "application_credential_secret",
                &self
                    .application_credential_secret
                    .as_ref()
                    .map(|_| "<redacted>"),
            )
            .field("role", &self.role)
            .field("region", &self.region)
            .field("port", &self.port)
            .field("all_tenants", &self.all_tenants)
            .field("availability", &self.availability)
            .field("tls", &self.tls)
            .field("refresh_interval", &self.refresh_interval)
            .finish()
    }
}

/// Undefaulted `openstack_sd_config` list-entry shape. `password` /
/// `application_credential_secret` hold secrets, so `Debug` is hand-written to
/// redact them (this struct is reachable from `scrape::config`'s
/// `RawScrapeConfig`'s derived `Debug`). Lives here (not in `scrape::config`)
/// alongside [`OpenstackSdConfig`] and [`build_openstack_sd_config`], keeping
/// `config.rs` under the repo's 800-line cap.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawOpenstackSdConfig {
    identity_endpoint: String,
    username: String,
    userid: String,
    password: Option<String>,
    project_name: String,
    project_id: String,
    domain_name: String,
    domain_id: String,
    application_credential_name: String,
    application_credential_id: String,
    application_credential_secret: Option<String>,
    role: String,
    region: String,
    port: Option<u16>,
    all_tenants: bool,
    availability: String,
    tls_config: Option<TlsConfig>,
}

impl fmt::Debug for RawOpenstackSdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawOpenstackSdConfig")
            .field("identity_endpoint", &self.identity_endpoint)
            .field("username", &self.username)
            .field("userid", &self.userid)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("project_name", &self.project_name)
            .field("project_id", &self.project_id)
            .field("domain_name", &self.domain_name)
            .field("domain_id", &self.domain_id)
            .field(
                "application_credential_name",
                &self.application_credential_name,
            )
            .field("application_credential_id", &self.application_credential_id)
            .field(
                "application_credential_secret",
                &self
                    .application_credential_secret
                    .as_ref()
                    .map(|_| "<redacted>"),
            )
            .field("role", &self.role)
            .field("region", &self.region)
            .field("port", &self.port)
            .field("all_tenants", &self.all_tenants)
            .field("availability", &self.availability)
            .field("tls_config", &self.tls_config)
            .finish()
    }
}

/// Builds an [`OpenstackSdConfig`] from its raw form. `role` is validated at
/// parse time (upstream `GetLabels` errors on an unexpected role) so a
/// misconfigured `openstack_sd_config` fails at config parse rather than at
/// discovery time. `port` defaults to [`DEFAULT_PORT`]; `refresh_interval` is
/// seeded to the flag default and overridden by
/// `scrape::wiring::apply_openstack_sd_check_interval`.
pub(crate) fn build_openstack_sd_config(
    raw: RawOpenstackSdConfig,
) -> Result<OpenstackSdConfig, ScrapeError> {
    if raw.role != ROLE_INSTANCE && raw.role != ROLE_HYPERVISOR {
        return Err(ScrapeError::new(format!(
            "unexpected role={:?}; must be one of `instance` or `hypervisor`",
            raw.role
        )));
    }
    Ok(OpenstackSdConfig {
        identity_endpoint: raw.identity_endpoint,
        username: raw.username,
        userid: raw.userid,
        password: raw.password,
        project_name: raw.project_name,
        project_id: raw.project_id,
        domain_name: raw.domain_name,
        domain_id: raw.domain_id,
        application_credential_name: raw.application_credential_name,
        application_credential_id: raw.application_credential_id,
        application_credential_secret: raw.application_credential_secret,
        role: raw.role,
        region: raw.region,
        // Upstream `newAPIConfig` coerces `port == 0` to 80, so treat both an
        // absent key and an explicit `0` as the default.
        port: raw.port.filter(|&p| p != 0).unwrap_or(DEFAULT_PORT),
        all_tenants: raw.all_tenants,
        availability: raw.availability,
        tls: raw.tls_config.unwrap_or_default(),
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
    })
}

/// `host:port`, bracketing `host` when it is an IPv6 address (contains a `:`).
/// Port of `discoveryutil.JoinHostPort`; shared by the instance/hypervisor
/// label builders.
pub(super) fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes. Local copy of the gce/hetzner constant of the same purpose.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `openstack_sd_config` entry. A background thread
/// refreshes the target-group snapshot on `refresh_interval`; [`poll`] clones
/// the current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct OpenstackDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime.
struct RefreshCtx {
    api: OpenstackApi,
    role: String,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    job: String,
    refresh_interval: Duration,
}

impl OpenstackDiscovery {
    /// Builds the OpenStack API client (failing only on bad config — see the
    /// module doc) and spawns the background refresh thread. The snapshot
    /// starts empty and is populated by the thread's first successful refresh.
    pub fn new(cfg: &OpenstackSdConfig, job: &str) -> Result<OpenstackDiscovery, ScrapeError> {
        if cfg.role != ROLE_INSTANCE && cfg.role != ROLE_HYPERVISOR {
            return Err(ScrapeError::new(format!(
                "unexpected role={:?}; must be one of `instance` or `hypervisor`",
                cfg.role
            )));
        }
        let api = new_openstack_api(cfg)?;
        let snapshot = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let ctx = RefreshCtx {
            api,
            role: cfg.role.clone(),
            snapshot: Arc::clone(&snapshot),
            stop: Arc::clone(&stop),
            job: job.to_string(),
            refresh_interval: cfg.refresh_interval,
        };

        let handle = thread::spawn(move || run(&ctx));

        Ok(OpenstackDiscovery {
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

impl Drop for OpenstackDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for OpenstackDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: re-list on `refresh_interval`. A list
/// failure is logged and retried at the same cadence, keeping the previous
/// snapshot (so a transiently-unreachable OpenStack API never wipes discovered
/// targets).
fn run(ctx: &RefreshCtx) {
    let source = format!("{}/openstack", ctx.job);
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        match list_target_groups(ctx, &source) {
            Ok(groups) => *ctx.snapshot.lock().unwrap() = groups,
            Err(e) => log::warn!(
                "esmagent openstack_sd ({}): refresh failed, keeping last-good targets: {e}",
                ctx.job
            ),
        }

        if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
            return;
        }
    }
}

/// Lists targets for the configured role. `role` was validated by
/// [`build_openstack_sd_config`] / [`OpenstackDiscovery::new`], so the
/// fallthrough is unreachable in practice; it is reported as an error (never a
/// panic) to match the port's no-panic contract.
fn list_target_groups(ctx: &RefreshCtx, source: &str) -> Result<Vec<TargetGroup>, ScrapeError> {
    match ctx.role.as_str() {
        ROLE_INSTANCE => {
            let servers = ctx.api.get_servers()?;
            Ok(add_instance_labels(&servers, ctx.api.port(), source))
        }
        ROLE_HYPERVISOR => {
            let hvs = ctx.api.get_hypervisors()?;
            Ok(add_hypervisor_labels(&hvs, ctx.api.port(), source))
        }
        other => Err(ScrapeError::new(format!(
            "unexpected role={other:?}; must be one of `instance` or `hypervisor`"
        ))),
    }
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`]. Returns
/// `true` if `stop` was observed. Local copy of the gce/hetzner helper.
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
#[path = "openstack_tests.rs"]
mod tests;
