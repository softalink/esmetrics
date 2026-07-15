//! Docker Swarm service discovery (`dockerswarm_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/dockerswarm` (v1.146.0), across three
//! roles (`services`/`tasks`/`nodes`). The Unix-socket HTTP/1.1 transport is
//! REUSED from the docker provider ([`crate::scrape::docker::transport`]);
//! [`client`] resolves the `host` into a transport and issues the per-role
//! endpoint fetches, [`network`]/[`nodes`]/[`services`]/[`tasks`] hold the
//! serde structs and the `__meta_dockerswarm_*` label builders, and
//! [`DockerswarmDiscovery`] (this file) is the [`Discovery`] the scrape
//! manager polls.
//!
//! ## Refresh model
//!
//! Mirrors `scrape::docker`: a single background thread re-lists the role's
//! endpoints on a fixed interval (`-promscrape.dockerswarmSDCheckInterval`,
//! default 30s — upstream `dockerswarm.SDCheckInterval`'s `30*time.Second`),
//! publishing the target-group snapshot behind a `Mutex`; [`poll`] clones it.
//! [`wait_or_stop`] observes a `stop`/`Drop` promptly rather than after a full
//! interval.
//!
//! ## Startup robustness
//!
//! [`DockerswarmDiscovery::new`] fails only on genuinely bad config
//! (missing/invalid `host`, invalid `role`, bad TLS material). A Docker Swarm
//! manager that is unreachable at startup does NOT fail `new()`: the first
//! listing happens on the background thread, and [`Discovery::poll`] returns
//! an empty list until the first successful refresh — matching the
//! docker/k8s/consul robustness choice.
//!
//! [`poll`]: Discovery::poll

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{new_dockerswarm_api, DockerswarmApi};
use network::network_labels_by_id;
use nodes::add_node_labels;
use services::{add_services_labels, add_services_labels_for_task};
use tasks::add_tasks_labels;

use super::config::{RawAuthFields, RawBasicAuth, ScrapeError};
use super::discovery::{Discovery, TargetGroup};
use crate::client::{AuthConfig, TlsConfig};

pub mod client;
pub mod network;
pub mod nodes;
pub mod services;
pub mod tasks;

/// Default `dockerswarm_sd_config` refresh interval, matching
/// `-promscrape.dockerswarmSDCheckInterval`'s default
/// (`dockerswarm.SDCheckInterval` = `30*time.Second`).
/// `scrape::wiring::apply_dockerswarm_sd_check_interval` overrides it from the
/// flag; [`build_dockerswarm_sd_config`] seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Default target `port`, matching upstream `dockerswarm.newAPIConfig`'s
/// `cfg.port = 80` fallback when `Port` is unset (`0`).
pub const DEFAULT_PORT: u16 = 80;

/// The discovery role. Port of `SDConfig.Role`'s `tasks`/`services`/`nodes`
/// switch in `dockerswarm.go`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Services,
    Tasks,
    Nodes,
}

impl Role {
    /// Parses a role string, rejecting anything other than the three upstream
    /// roles (matching `GetLabels`'s `default:` error).
    pub fn parse(s: &str) -> Result<Role, ScrapeError> {
        match s {
            "services" => Ok(Role::Services),
            "tasks" => Ok(Role::Tasks),
            "nodes" => Ok(Role::Nodes),
            other => Err(ScrapeError {
                msg: format!(
                    "unexpected dockerswarm role={other:?}; must be one of `tasks`, `services` or `nodes`"
                ),
            }),
        }
    }
}

/// One `filters` entry (`dockerswarm.go`'s `Filter`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DockerswarmFilter {
    pub name: String,
    pub values: Vec<String>,
}

/// Local `dockerswarm_sd_config` shape. Port of
/// `discovery/dockerswarm.SDConfig`'s supported fields, built via
/// [`build_dockerswarm_sd_config`] from [`RawDockerswarmSdConfig`].
/// `refresh_interval` is not a YAML field (upstream reads it from
/// `-promscrape.dockerswarmSDCheckInterval`); it defaults to
/// [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`crate::scrape::docker::DockerSdConfig`], to keep `config.rs`
/// under the repo's 800-line cap. `Debug` is hand-written to redact the auth
/// secret held in `auth`.
#[derive(Clone, PartialEq)]
pub struct DockerswarmSdConfig {
    pub host: String,
    pub role: String,
    pub port: u16,
    pub filters: Vec<DockerswarmFilter>,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
    pub refresh_interval: Duration,
}

impl Default for DockerswarmSdConfig {
    fn default() -> Self {
        DockerswarmSdConfig {
            host: String::new(),
            role: String::new(),
            port: DEFAULT_PORT,
            filters: Vec::new(),
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

impl std::fmt::Debug for DockerswarmSdConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DockerswarmSdConfig")
            .field("host", &self.host)
            .field("role", &self.role)
            .field("port", &self.port)
            .field("filters", &self.filters)
            .field("auth", &"<redacted>")
            .field("tls", &self.tls)
            .field("refresh_interval", &self.refresh_interval)
            .finish()
    }
}

/// Undefaulted `dockerswarm_sd_config` list-entry shape. `bearer_token` holds
/// a secret, so `Debug` is hand-written to redact it (this struct is reachable
/// from `scrape::config`'s `RawScrapeConfig`'s derived `Debug`). Lives here
/// (not in `scrape::config`) alongside [`DockerswarmSdConfig`] and
/// [`build_dockerswarm_sd_config`], keeping `config.rs` under the 800-line cap.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawDockerswarmSdConfig {
    host: String,
    role: String,
    port: Option<u16>,
    filters: Vec<RawDockerswarmFilter>,
    basic_auth: Option<RawBasicAuth>,
    bearer_token: Option<String>,
    tls_config: Option<TlsConfig>,
}

/// Undefaulted `filters:` list entry.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawDockerswarmFilter {
    name: String,
    values: Vec<String>,
}

impl std::fmt::Debug for RawDockerswarmSdConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawDockerswarmSdConfig")
            .field("host", &self.host)
            .field("role", &self.role)
            .field("port", &self.port)
            .field("filters", &self.filters)
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

/// Builds a [`DockerswarmSdConfig`] from its raw form. `port` defaults to
/// [`DEFAULT_PORT`]; `refresh_interval` is seeded to the flag default and
/// overridden by `scrape::wiring::apply_dockerswarm_sd_check_interval`. The
/// `role` string is validated later (in [`DockerswarmDiscovery::new`]), not
/// here, matching upstream's parse-then-fetch-time role check.
pub(crate) fn build_dockerswarm_sd_config(raw: RawDockerswarmSdConfig) -> DockerswarmSdConfig {
    let auth_fields = RawAuthFields {
        basic_auth: raw.basic_auth,
        bearer_token: raw.bearer_token,
    };
    DockerswarmSdConfig {
        host: raw.host,
        role: raw.role,
        port: raw.port.unwrap_or(DEFAULT_PORT),
        filters: raw
            .filters
            .into_iter()
            .map(|f| DockerswarmFilter {
                name: f.name,
                values: f.values,
            })
            .collect(),
        auth: auth_fields.into_auth_config(),
        tls: raw.tls_config.unwrap_or_default(),
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
    }
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes. Local copy of the docker/consul constant.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `dockerswarm_sd_config` entry. A background thread
/// refreshes the target-group snapshot on `refresh_interval`; [`poll`] clones
/// the current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct DockerswarmDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime.
struct RefreshCtx {
    api: DockerswarmApi,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    job: String,
    port: u16,
    refresh_interval: Duration,
}

impl DockerswarmDiscovery {
    /// Builds the Docker Swarm API client (failing only on bad config — see
    /// the module doc) and spawns the background refresh thread. The snapshot
    /// starts empty and is populated by the thread's first successful refresh.
    pub fn new(cfg: &DockerswarmSdConfig, job: &str) -> Result<DockerswarmDiscovery, ScrapeError> {
        let api = new_dockerswarm_api(cfg)?;
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

        Ok(DockerswarmDiscovery {
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

impl Drop for DockerswarmDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for DockerswarmDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: re-list the role's endpoints on
/// `refresh_interval`. A fetch failure is logged and retried at the same
/// cadence, keeping the previous snapshot (so a transiently-unreachable
/// manager never wipes discovered targets).
fn run(ctx: &RefreshCtx) {
    let source = format!("{}/dockerswarm", ctx.job);
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        match refresh(ctx, &source) {
            Ok(groups) => *ctx.snapshot.lock().unwrap() = groups,
            Err(e) => log::warn!(
                "esmagent dockerswarm_sd ({}): refresh failed, keeping last-good targets: {e}",
                ctx.job
            ),
        }

        if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
            return;
        }
    }
}

/// One refresh: fetch the role's endpoints, build the role's label maps, and
/// convert them into target groups. Port of `GetLabels`'s per-role dispatch.
fn refresh(ctx: &RefreshCtx, source: &str) -> Result<Vec<TargetGroup>, ScrapeError> {
    let maps = match ctx.api.role() {
        Role::Nodes => {
            let nodes = ctx.api.get_nodes()?;
            add_node_labels(&nodes, ctx.port)
        }
        Role::Services => {
            let services = ctx.api.get_services()?;
            let networks = ctx.api.get_networks()?;
            let network_labels = network_labels_by_id(&networks);
            add_services_labels(&services, &network_labels, ctx.port)
        }
        Role::Tasks => {
            let tasks = ctx.api.get_tasks()?;
            let services = ctx.api.get_services()?;
            let networks = ctx.api.get_networks()?;
            let network_labels = network_labels_by_id(&networks);
            let service_labels = add_services_labels_for_task(&services);
            let nodes = ctx.api.get_nodes()?;
            let node_labels = add_node_labels(&nodes, ctx.port);
            add_tasks_labels(
                &tasks,
                &node_labels,
                &service_labels,
                &network_labels,
                &services,
                ctx.port,
            )
        }
    };
    Ok(maps_to_target_groups(maps, source))
}

/// Splits each label map's `__address__` into a single-target
/// [`TargetGroup`], leaving the remaining `__meta_dockerswarm_*` keys as its
/// `labels` (mirroring `scrape::docker`'s target shape).
fn maps_to_target_groups(maps: Vec<BTreeMap<String, String>>, source: &str) -> Vec<TargetGroup> {
    maps.into_iter()
        .map(|mut m| {
            let address = m.remove("__address__").unwrap_or_default();
            TargetGroup {
                targets: vec![address],
                labels: m,
                source: source.to_string(),
            }
        })
        .collect()
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`]. Returns
/// `true` if `stop` was observed. Local copy of the docker helper.
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
#[path = "dockerswarm_tests.rs"]
mod tests;
