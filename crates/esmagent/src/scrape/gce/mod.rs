//! Google Compute Engine service discovery (`gce_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/gce` (v1.146.0): [`client`] resolves auth
//! (a static bearer token, or the GCE metadata-server access token) and issues
//! the paginated `zones.list` / `instances.list` Compute API GETs, [`labels`]
//! holds the response structs + `__meta_gce_*` label builder, and
//! [`GceDiscovery`] (this file) is the [`super::discovery::Discovery`] the
//! scrape manager polls.
//!
//! ## Refresh model
//!
//! Mirrors `scrape::ec2`: a single background thread re-lists instances on a
//! fixed interval (`-promscrape.gceSDCheckInterval`, default 60s — upstream
//! `gce.SDCheckInterval`'s `time.Minute`), publishing the target-group
//! snapshot behind a `Mutex`; [`GceDiscovery::poll`] clones it.
//! [`wait_or_stop`] observes a `stop`/`Drop` promptly rather than after a full
//! interval. The project and zone list are resolved once at thread start
//! (retried at the refresh cadence on failure) — matching upstream
//! `newAPIConfig`, which resolves them once.
//!
//! ## Auth scope (SCOPED subset)
//!
//! Supported: a static `bearer_token` from config (wins if set), else the GCE
//! **metadata-server access token** (`GET
//! .../instance/service-accounts/default/token` with `Metadata-Flavor:
//! Google`, cached to its `expires_in`). Project/zone come from config, else
//! auto-detect via the metadata server (`.../project/project-id`,
//! `.../instance/zone`); `zone` may be a single value, a list, or `*` (list
//! all zones for the project).
//!
//! **Deferred** (rejected at build time): a service-account JSON key file
//! (`credentials_file` / `GOOGLE_APPLICATION_CREDENTIALS`, the RS256-JWT ->
//! token-exchange flow). A set `credentials_file` is a build-time error, the
//! same reject-early convention EC2 uses for `role_arn`.
//!
//! ## Startup robustness
//!
//! [`GceDiscovery::new`] fails only on a set `credentials_file` (DEFERRED) or a
//! genuinely bad HTTP-client build. GCE / metadata being unreachable at
//! startup does NOT fail `new()`: the token fetch, project/zone resolution,
//! and the first `instances.list` happen on the background thread (retried at
//! the refresh cadence), and [`Discovery::poll`] returns an empty list until
//! the first successful refresh — matching the EC2/consul robustness choice.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{new_gce_api, GceApi};
use labels::append_target_labels;

use super::config::ScrapeError;
use super::discovery::{Discovery, TargetGroup};

pub mod client;
pub mod labels;

/// Default `gce_sd_config` refresh interval, matching
/// `-promscrape.gceSDCheckInterval`'s default (`gce.SDCheckInterval` =
/// `time.Minute`). `scrape::wiring::apply_gce_sd_check_interval` overrides it
/// from the flag; [`build_gce_sd_config`] seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Default `port` for the discovered target's `__address__` when a config
/// doesn't set one — matches upstream `gce.newAPIConfig`'s `port := 80`.
const DEFAULT_PORT: u16 = 80;

/// Default `tag_separator`, matching upstream `gce.newAPIConfig`'s `","`.
const DEFAULT_TAG_SEPARATOR: &str = ",";

/// Local `gce_sd_config` shape. Port of `discovery/gce.SDConfig`'s supported
/// fields, plus the esmagent-specific `bearer_token` (static auth),
/// `credentials_file` (DEFERRED), and `endpoint`/`metadata_url` overrides (for
/// tests, mirroring EC2's `endpoint`). Built via [`build_gce_sd_config`] from
/// its [`RawGceSdConfig`]. `refresh_interval` is not a YAML field (upstream
/// reads it from `-promscrape.gceSDCheckInterval`); it defaults to
/// [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`super::ec2::Ec2SdConfig`] / [`super::nomad::NomadSdConfig`], to
/// keep `config.rs` under the repo's 800-line file cap.
///
/// `Debug` is hand-written to redact `bearer_token` (mirrors the EC2/Nomad
/// secret redaction).
#[derive(Clone, PartialEq)]
pub struct GceSdConfig {
    pub project: String,
    /// `zone` normalized to a list. Empty means "auto-detect the current
    /// zone"; `["*"]` means "all zones for the project".
    pub zones: Vec<String>,
    pub filter: String,
    pub port: u16,
    pub tag_separator: String,
    /// Static Compute API bearer token (esmagent extension). Wins over the
    /// metadata-server token when set.
    pub bearer_token: Option<String>,
    /// Service-account JSON key file — DEFERRED. A non-empty value is rejected
    /// at build time (see [`GceDiscovery::new`] and [`build_gce_sd_config`]).
    pub credentials_file: Option<String>,
    /// Compute API base override (defaults to
    /// `https://compute.googleapis.com/compute/v1`) — like EC2's `endpoint`,
    /// lets a test point at a stub.
    pub endpoint: Option<String>,
    /// Metadata-server base override (defaults to
    /// `http://metadata.google.internal/computeMetadata/v1`) — lets a test
    /// point the token/project/zone lookups at a stub.
    pub metadata_url: Option<String>,
    pub refresh_interval: Duration,
}

impl Default for GceSdConfig {
    fn default() -> Self {
        GceSdConfig {
            project: String::new(),
            zones: Vec::new(),
            filter: String::new(),
            port: DEFAULT_PORT,
            tag_separator: DEFAULT_TAG_SEPARATOR.to_string(),
            bearer_token: None,
            credentials_file: None,
            endpoint: None,
            metadata_url: None,
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

impl std::fmt::Debug for GceSdConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GceSdConfig")
            .field("project", &self.project)
            .field("zones", &self.zones)
            .field("filter", &self.filter)
            .field("port", &self.port)
            .field("tag_separator", &self.tag_separator)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "<redacted>"),
            )
            .field("credentials_file", &self.credentials_file)
            .field("endpoint", &self.endpoint)
            .field("metadata_url", &self.metadata_url)
            .field("refresh_interval", &self.refresh_interval)
            .finish()
    }
}

/// A YAML `zone:` that is either a single string or a list of strings — port
/// of upstream `gce.ZoneYAML`'s `UnmarshalYAML`.
#[derive(Deserialize)]
#[serde(untagged)]
enum RawZone {
    One(String),
    Many(Vec<String>),
}

impl RawZone {
    fn into_vec(self) -> Vec<String> {
        match self {
            RawZone::One(z) => vec![z],
            RawZone::Many(zs) => zs,
        }
    }
}

/// Undefaulted `gce_sd_config` list-entry shape. `bearer_token` holds a
/// secret, so `Debug` is hand-written to redact it (this struct is reachable
/// from `scrape::config`'s `RawScrapeConfig`'s derived `Debug`). Lives here
/// (not in `scrape::config`) alongside [`GceSdConfig`] and
/// [`build_gce_sd_config`], keeping `config.rs` under the repo's 800-line cap;
/// `scrape::config` imports it for `RawScrapeConfig`.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawGceSdConfig {
    project: String,
    zone: Option<RawZone>,
    filter: String,
    port: Option<u16>,
    tag_separator: Option<String>,
    bearer_token: Option<String>,
    credentials_file: Option<String>,
    endpoint: Option<String>,
    metadata_url: Option<String>,
}

impl std::fmt::Debug for RawGceSdConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawGceSdConfig")
            .field("project", &self.project)
            .field("filter", &self.filter)
            .field("port", &self.port)
            .field("tag_separator", &self.tag_separator)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "<redacted>"),
            )
            .field("credentials_file", &self.credentials_file)
            .field("endpoint", &self.endpoint)
            .field("metadata_url", &self.metadata_url)
            .finish()
    }
}

/// Builds a [`GceSdConfig`] from its raw form. Fails at build (parse) time when
/// `credentials_file` is set — the service-account JSON key file is DEFERRED
/// (see this module's doc) — matching upstream's reject-early convention for
/// unsupported cloud-SD features. `port` defaults to 80 and `tag_separator` to
/// `,` (`gce.newAPIConfig`); `refresh_interval` is seeded to the flag default
/// and overridden by `scrape::wiring::apply_gce_sd_check_interval`.
pub(crate) fn build_gce_sd_config(raw: RawGceSdConfig) -> Result<GceSdConfig, ScrapeError> {
    if raw
        .credentials_file
        .as_deref()
        .is_some_and(|c| !c.is_empty())
    {
        return Err(ScrapeError::new(
            "gce_sd_config: unsupported (deferred): service-account key file (credentials_file)",
        ));
    }
    Ok(GceSdConfig {
        project: raw.project,
        zones: raw.zone.map(RawZone::into_vec).unwrap_or_default(),
        filter: raw.filter,
        port: raw.port.unwrap_or(DEFAULT_PORT),
        tag_separator: raw
            .tag_separator
            .unwrap_or_else(|| DEFAULT_TAG_SEPARATOR.to_string()),
        bearer_token: raw.bearer_token,
        credentials_file: raw.credentials_file,
        endpoint: raw.endpoint,
        metadata_url: raw.metadata_url,
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
    })
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes. Local copy of the EC2/consul constant of the same purpose.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `gce_sd_config` entry. A background thread refreshes
/// the target-group snapshot on `refresh_interval`; [`poll`] clones the
/// current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct GceDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime.
struct RefreshCtx {
    api: GceApi,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    job: String,
    /// Project from config; empty means "resolve via metadata".
    configured_project: String,
    /// Zones from config; empty = auto-detect current zone; `["*"]` = all
    /// zones for the project.
    configured_zones: Vec<String>,
    filter: String,
    tag_separator: String,
    port: u16,
    refresh_interval: Duration,
}

impl GceDiscovery {
    /// Builds the GCE API client (failing on a set `credentials_file` —
    /// DEFERRED — or a bad HTTP-client build; see the module doc) and spawns
    /// the background refresh thread. The snapshot starts empty and is
    /// populated by the thread's first successful refresh.
    pub fn new(cfg: &GceSdConfig, job: &str) -> Result<GceDiscovery, ScrapeError> {
        if cfg
            .credentials_file
            .as_deref()
            .is_some_and(|c| !c.is_empty())
        {
            return Err(ScrapeError {
                msg: "gce_sd_config: unsupported (deferred): service-account key file \
                      (credentials_file)"
                    .to_string(),
            });
        }
        let api = new_gce_api(cfg)?;
        let snapshot = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let ctx = RefreshCtx {
            api,
            snapshot: Arc::clone(&snapshot),
            stop: Arc::clone(&stop),
            job: job.to_string(),
            configured_project: cfg.project.clone(),
            configured_zones: cfg.zones.clone(),
            filter: cfg.filter.clone(),
            tag_separator: cfg.tag_separator.clone(),
            port: cfg.port,
            refresh_interval: cfg.refresh_interval,
        };

        let handle = thread::spawn(move || run(&ctx));

        Ok(GceDiscovery {
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

impl Drop for GceDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for GceDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: resolve the project + zone list once (via
/// config or the metadata server, retried at the refresh cadence on failure),
/// then re-list instances on `refresh_interval`. A list failure is logged and
/// retried at the same cadence, keeping the previous snapshot (so a
/// transiently-unreachable Compute API never wipes discovered targets).
fn run(ctx: &RefreshCtx) {
    let (project, zones) = loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }
        match resolve_project_and_zones(ctx) {
            Ok(pz) => break pz,
            Err(e) => {
                log::warn!(
                    "esmagent gce_sd ({}): cannot resolve project/zones: {e}",
                    ctx.job
                );
                if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
                    return;
                }
            }
        }
    };

    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        match refresh(ctx, &project, &zones) {
            Ok(groups) => *ctx.snapshot.lock().unwrap() = groups,
            Err(e) => log::warn!(
                "esmagent gce_sd ({}): refresh failed, keeping last-good targets: {e}",
                ctx.job
            ),
        }

        if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
            return;
        }
    }
}

/// Resolves the project (config, else metadata) and the zone list (config,
/// else auto-detect the current zone; `["*"]` -> list all zones). Port of
/// `newAPIConfig`'s project/zone resolution.
fn resolve_project_and_zones(ctx: &RefreshCtx) -> Result<(String, Vec<String>), ScrapeError> {
    let project = if ctx.configured_project.is_empty() {
        ctx.api.get_current_project()?
    } else {
        ctx.configured_project.clone()
    };
    let zones = if ctx.configured_zones.is_empty() {
        vec![ctx.api.get_current_zone()?]
    } else if ctx.configured_zones.len() == 1 && ctx.configured_zones[0] == "*" {
        ctx.api.list_zones(&project)?
    } else {
        ctx.configured_zones.clone()
    };
    Ok((project, zones))
}

/// One full refresh: page through `instances.list` for each zone and build a
/// [`TargetGroup`] per instance that has a network interface. A single zone's
/// list failure is logged and skipped (the other zones still contribute),
/// mirroring upstream `getInstances`. Only when *every* zone failed (and there
/// was at least one) does this return `Err`, so the caller keeps the previous
/// snapshot rather than wiping targets on a total-but-transient outage.
fn refresh(
    ctx: &RefreshCtx,
    project: &str,
    zones: &[String],
) -> Result<Vec<TargetGroup>, ScrapeError> {
    let mut groups = Vec::new();
    let mut any_ok = false;
    let mut last_err: Option<ScrapeError> = None;
    for zone in zones {
        let insts = match ctx.api.list_instances(project, zone, &ctx.filter) {
            Ok(i) => i,
            Err(e) => {
                log::warn!(
                    "esmagent gce_sd ({}): cannot collect instances from zone {zone:?}: {e}",
                    ctx.job
                );
                last_err = Some(e);
                continue;
            }
        };
        any_ok = true;
        let source = format!("{}/gce/{zone}", ctx.job);
        for inst in &insts {
            if let Some(g) =
                append_target_labels(inst, project, &ctx.tag_separator, ctx.port, source.clone())
            {
                groups.push(g);
            }
        }
    }
    if !any_ok {
        if let Some(e) = last_err {
            return Err(e);
        }
    }
    Ok(groups)
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`]. Returns
/// `true` if `stop` was observed. Local copy of the EC2/consul helper.
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
#[path = "gce_tests.rs"]
mod tests;
