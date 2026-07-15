//! AWS EC2 service discovery (`ec2_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/ec2` (v1.146.0): [`client`] resolves
//! credentials + region and issues SigV4-signed `DescribeInstances` /
//! `DescribeAvailabilityZones` queries, [`sigv4`] is the AWS Signature V4
//! algorithm, [`labels`] holds the response structs + `__meta_ec2_*` label
//! builder, and [`Ec2Discovery`] (this file) is the
//! [`super::discovery::Discovery`] the scrape manager polls.
//!
//! ## Refresh model
//!
//! Mirrors `scrape::consul`: a single background thread re-lists instances on
//! a fixed interval (`-promscrape.ec2SDCheckInterval`, default 60s — upstream
//! `ec2.SDCheckInterval`'s `time.Minute`), publishing the target-group
//! snapshot behind a `Mutex`; [`Ec2Discovery::poll`] clones it.
//! [`wait_or_stop`] observes a `stop`/`Drop` promptly rather than after a
//! full interval. The AZ-name -> AZ-id map is loaded once (best-effort, like
//! upstream `getAZMap`, which caches even an empty map on error).
//!
//! ## Credential chain (SCOPED subset)
//!
//! Supported, resolved in this order (first that yields keys wins): static
//! `access_key`/`secret_key` (+ optional `session_token`) from the config;
//! `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (+ optional
//! `AWS_SESSION_TOKEN`) from the environment; the IMDSv2 instance role
//! (token PUT -> role list -> role credentials, cached until near
//! expiration). **Deferred** (rejected/not implemented): STS
//! `AssumeRole`/`role_arn` (a set `role_arn` is a build-time error), the
//! web-identity token file (`AWS_WEB_IDENTITY_TOKEN_FILE`), and the shared
//! `~/.aws` config/credentials files.
//!
//! ## Startup robustness
//!
//! [`Ec2Discovery::new`] fails only on a set `role_arn` or a genuinely bad
//! HTTP-client build. AWS/IMDS being unreachable at startup does NOT fail
//! `new()`: region resolution, credential fetch, and the first
//! `DescribeInstances` happen on the background thread (retried at the
//! refresh cadence), and [`Discovery::poll`] returns an empty list until the
//! first successful refresh — matching the k8s/consul robustness choice.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use client::{filters_query_string, new_ec2_api, Ec2Api};
use labels::{append_target_labels, build_az_map, parse_instances_response};

use super::config::ScrapeError;
use super::discovery::{Discovery, TargetGroup};

pub mod client;
pub mod labels;
pub mod sigv4;

/// Default `ec2_sd_config` refresh interval, matching
/// `-promscrape.ec2SDCheckInterval`'s default (`ec2.SDCheckInterval` =
/// `time.Minute`). `scrape::wiring::apply_ec2_sd_check_interval` overrides it
/// from the flag; `scrape::config::build_ec2_sd_config` seeds it at parse
/// time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Default `port` for the discovered target's `__address__` when a config
/// doesn't set one — matches upstream `ec2.newAPIConfig`'s `port := 80`.
const DEFAULT_PORT: u16 = 80;

/// One EC2 filter (`filters` / `az_filters` entry). Port of `awsapi.Filter`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct Ec2Filter {
    pub name: String,
    pub values: Vec<String>,
}

/// Local `ec2_sd_config` shape. Port of `discovery/ec2.SDConfig`'s supported
/// fields. Built via `scrape::config::build_ec2_sd_config` from its
/// `RawEc2SdConfig`. `refresh_interval` is not a YAML field (upstream reads
/// it from `-promscrape.ec2SDCheckInterval`); it defaults to
/// [`DEFAULT_REFRESH_INTERVAL`] and is overridden at wiring time.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`super::consul::ConsulSdConfig`], to keep `config.rs` under the
/// repo's 800-line file cap.
///
/// `Debug` is hand-written to redact `secret_key`/`session_token` (mirrors
/// `OAuth2Config`'s redaction). `access_key` is an identifier, not a secret,
/// so it is shown.
#[derive(Clone, PartialEq)]
pub struct Ec2SdConfig {
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key: String,
    pub secret_key: Option<String>,
    pub session_token: Option<String>,
    /// STS `AssumeRole` ARN — DEFERRED. A non-empty value is rejected at
    /// build time (see [`Ec2Discovery::new`] and `build_ec2_sd_config`).
    pub role_arn: Option<String>,
    pub port: u16,
    pub filters: Vec<Ec2Filter>,
    pub az_filters: Vec<Ec2Filter>,
    pub refresh_interval: Duration,
}

impl Default for Ec2SdConfig {
    fn default() -> Self {
        Ec2SdConfig {
            region: String::new(),
            endpoint: None,
            access_key: String::new(),
            secret_key: None,
            session_token: None,
            role_arn: None,
            port: DEFAULT_PORT,
            filters: Vec::new(),
            az_filters: Vec::new(),
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

impl std::fmt::Debug for Ec2SdConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ec2SdConfig")
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("access_key", &self.access_key)
            .field(
                "secret_key",
                &self.secret_key.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .field("role_arn", &self.role_arn)
            .field("port", &self.port)
            .field("filters", &self.filters)
            .field("az_filters", &self.az_filters)
            .field("refresh_interval", &self.refresh_interval)
            .finish()
    }
}

/// Undefaulted `ec2_sd_config` list-entry shape. `secret_key`/`session_token`
/// hold secrets, so `Debug` is hand-written to redact them (this struct
/// derives nothing that would print them, but `scrape::config`'s
/// `RawScrapeConfig` derives `Debug` and contains a `Vec<RawEc2SdConfig>`).
/// Lives here (not in `scrape::config`) alongside [`Ec2SdConfig`] and
/// [`build_ec2_sd_config`], keeping `config.rs` under the repo's 800-line cap.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawEc2SdConfig {
    region: String,
    endpoint: Option<String>,
    access_key: String,
    secret_key: Option<String>,
    session_token: Option<String>,
    role_arn: Option<String>,
    port: Option<u16>,
    filters: Vec<Ec2Filter>,
    az_filters: Vec<Ec2Filter>,
}

impl std::fmt::Debug for RawEc2SdConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawEc2SdConfig")
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("access_key", &self.access_key)
            .field(
                "secret_key",
                &self.secret_key.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .field("role_arn", &self.role_arn)
            .field("port", &self.port)
            .field("filters", &self.filters)
            .field("az_filters", &self.az_filters)
            .finish()
    }
}

/// Builds an [`Ec2SdConfig`] from its raw form. Fails at build (parse) time
/// when `role_arn` is set — STS `AssumeRole` is DEFERRED (see this module's
/// doc) — matching upstream's reject-early convention for unsupported
/// cloud-SD features. `port` defaults to 80 (`ec2.newAPIConfig`);
/// `refresh_interval` is seeded to the flag default and overridden by
/// `scrape::wiring::apply_ec2_sd_check_interval`.
pub(crate) fn build_ec2_sd_config(raw: RawEc2SdConfig) -> Result<Ec2SdConfig, ScrapeError> {
    if raw.role_arn.as_deref().is_some_and(|r| !r.is_empty()) {
        return Err(ScrapeError::new(
            "ec2_sd_config: unsupported (deferred): role_arn",
        ));
    }
    Ok(Ec2SdConfig {
        region: raw.region,
        endpoint: raw.endpoint,
        access_key: raw.access_key,
        secret_key: raw.secret_key,
        session_token: raw.session_token,
        role_arn: raw.role_arn,
        port: raw.port.unwrap_or(80),
        filters: raw.filters,
        az_filters: raw.az_filters,
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
    })
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes. Local copy of the consul/k8s constant of the same purpose.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `ec2_sd_config` entry. A background thread
/// refreshes the target-group snapshot on `refresh_interval`; [`poll`]
/// clones the current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct Ec2Discovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime.
struct RefreshCtx {
    api: Ec2Api,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    job: String,
    /// Region from config; empty means "resolve via IMDS".
    configured_region: String,
    port: u16,
    filters: Vec<Ec2Filter>,
    az_filters: Vec<Ec2Filter>,
    refresh_interval: Duration,
}

impl Ec2Discovery {
    /// Builds the EC2 API client (failing on a set `role_arn` — DEFERRED — or
    /// a bad HTTP-client build; see the module doc) and spawns the background
    /// refresh thread. The snapshot starts empty and is populated by the
    /// thread's first successful refresh.
    pub fn new(cfg: &Ec2SdConfig, job: &str) -> Result<Ec2Discovery, ScrapeError> {
        if cfg.role_arn.as_deref().is_some_and(|r| !r.is_empty()) {
            return Err(ScrapeError {
                msg: "ec2_sd_config: unsupported (deferred): role_arn".to_string(),
            });
        }
        let api = new_ec2_api(cfg)?;
        let snapshot = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let ctx = RefreshCtx {
            api,
            snapshot: Arc::clone(&snapshot),
            stop: Arc::clone(&stop),
            job: job.to_string(),
            configured_region: cfg.region.clone(),
            port: cfg.port,
            filters: cfg.filters.clone(),
            az_filters: cfg.az_filters.clone(),
            refresh_interval: cfg.refresh_interval,
        };

        let handle = thread::spawn(move || run(&ctx));

        Ok(Ec2Discovery {
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

impl Drop for Ec2Discovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for Ec2Discovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: resolve the region (once, via config or
/// IMDS), load the AZ map (once, best-effort), then re-list instances on
/// `refresh_interval`. A region-resolution or list failure is logged and
/// retried at the same cadence, keeping the previous snapshot (so a
/// transiently-unreachable EC2 API never wipes discovered targets).
fn run(ctx: &RefreshCtx) {
    let mut region = ctx.configured_region.clone();
    if region.is_empty() {
        // Match upstream getDefaultRegion: AWS_REGION env before IMDS.
        region = std::env::var("AWS_REGION").unwrap_or_default();
    }
    let mut az_map: BTreeMap<String, String> = BTreeMap::new();
    let mut az_loaded = false;

    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        if region.is_empty() {
            match ctx.api.resolve_region_via_imds() {
                Ok(r) => region = r,
                Err(e) => {
                    log::warn!("esmagent ec2_sd ({}): cannot resolve region: {e}", ctx.job);
                    if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
                        return;
                    }
                    continue;
                }
            }
        }

        if !az_loaded {
            az_map = load_az_map(ctx, &region);
            az_loaded = true;
        }

        match refresh(ctx, &region, &az_map) {
            Ok(groups) => *ctx.snapshot.lock().unwrap() = groups,
            Err(e) => log::warn!(
                "esmagent ec2_sd ({}): refresh failed, keeping last-good targets: {e}",
                ctx.job
            ),
        }

        if wait_or_stop(&ctx.stop, ctx.refresh_interval) {
            return;
        }
    }
}

/// Loads the AZ-name -> AZ-id map via `DescribeAvailabilityZones`. Best
/// effort: on any error it logs and returns an empty map, so
/// `__meta_ec2_availability_zone_id` is simply unset rather than failing
/// discovery — mirrors upstream `getAZMap`.
fn load_az_map(ctx: &RefreshCtx, region: &str) -> BTreeMap<String, String> {
    let az_filters = filters_query_string(&ctx.az_filters);
    match ctx.api.describe_availability_zones(region, &az_filters) {
        Ok(data) => match labels::parse_availability_zones_response(&data) {
            Ok(azr) => build_az_map(&azr),
            Err(e) => {
                log::warn!(
                    "esmagent ec2_sd ({}): cannot parse availability zones, \
                     __meta_ec2_availability_zone_id will be unset: {e}",
                    ctx.job
                );
                BTreeMap::new()
            }
        },
        Err(e) => {
            log::warn!(
                "esmagent ec2_sd ({}): cannot load availability zones, \
                 __meta_ec2_availability_zone_id will be unset: {e}",
                ctx.job
            );
            BTreeMap::new()
        }
    }
}

/// One full refresh: page through `DescribeInstances` (following
/// `<nextToken>`) and build a [`TargetGroup`] per instance that has a private
/// IP. Any error (request, parse) propagates as `Err` so the caller keeps the
/// previous snapshot.
fn refresh(
    ctx: &RefreshCtx,
    region: &str,
    az_map: &BTreeMap<String, String>,
) -> Result<Vec<TargetGroup>, ScrapeError> {
    let filters_qs = filters_query_string(&ctx.filters);
    let source = format!("{}/ec2/{region}", ctx.job);
    let mut groups = Vec::new();
    let mut next_token = String::new();
    loop {
        let data = ctx
            .api
            .describe_instances(region, &filters_qs, &next_token)?;
        let resp = parse_instances_response(&data).map_err(|msg| ScrapeError {
            msg: format!("cannot parse DescribeInstances response: {msg}"),
        })?;
        for reservation in &resp.reservation_set.items {
            for inst in &reservation.instance_set.items {
                if let Some(g) = append_target_labels(
                    inst,
                    &reservation.owner_id,
                    region,
                    ctx.port,
                    az_map,
                    source.clone(),
                ) {
                    groups.push(g);
                }
            }
        }
        if resp.next_token.is_empty() {
            return Ok(groups);
        }
        next_token = resp.next_token;
    }
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`]. Returns
/// `true` if `stop` was observed. Local copy of the consul/k8s helper.
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
#[path = "ec2_tests.rs"]
mod tests;
