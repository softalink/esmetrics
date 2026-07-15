//! DNS service discovery (`dns_sd_configs`) support.
//!
//! Port of `lib/promscrape/discovery/dns` (v1.146.0): [`labels`] holds the
//! `__meta_dns_*` label builders, [`wire`] is a hand-rolled synchronous DNS
//! client for SRV/MX, [`resolve`] dispatches resolution per record type, and
//! [`DnsDiscovery`] (this file) is the [`super::discovery::Discovery`] the
//! scrape manager polls.
//!
//! ## Resolution strategy (no tokio, no async DNS crate)
//!
//! This crate is `reqwest::blocking` + std threads, so DNS resolution uses
//! only `std::net`:
//!
//! - **A / AAAA**: `std::net::ToSocketAddrs` (getaddrinfo via the OS resolver)
//!   — resolve `(name, 0)`, keep the IPv4 (A) or IPv6 (AAAA) addrs. Matches
//!   Go's `LookupIPAddr` closely enough and needs no nameserver discovery.
//! - **SRV / MX**: getaddrinfo can't answer these, so [`wire`] issues raw DNS
//!   queries over `std::net::UdpSocket` (falling back to TCP on a truncated
//!   response) to a nameserver read from `/etc/resolv.conf` on unix (or the
//!   config's explicit `nameserver` override, used by tests). Rather than pull
//!   `hickory-proto` — which, even with `default-features = false`, drags in
//!   `async-trait`, the `futures-*` stack, `enum-as-inner`, and the full
//!   ICU/`idna` tree — the ~1-question / SRV+MX-answer subset is hand-rolled
//!   in [`wire`] (with DNS name decompression), keeping the dependency set
//!   unchanged. On non-unix with no override, SRV/MX resolution has no
//!   nameserver: it logs a warning and yields no targets (the windows-gnu
//!   build still compiles; Windows CI exercises the override).
//!
//! ## Refresh model
//!
//! Mirrors `scrape::digitalocean`: a single background thread re-resolves on a
//! fixed interval (`-promscrape.dnsSDCheckInterval`, default 30s — upstream
//! `dns.SDCheckInterval`'s `30*time.Second`), publishing the target-group
//! snapshot behind a `Mutex`; [`DnsDiscovery::poll`] clones it. [`wait_or_stop`]
//! observes a `stop`/`Drop` promptly rather than after a full interval, and
//! each SRV/MX query is read-timeout-bounded so the thread and its `Drop` stay
//! responsive.
//!
//! ## Startup robustness
//!
//! [`DnsDiscovery::new`] fails only on genuinely bad config (empty `names`, a
//! bad `type`, or a missing `port` for A/AAAA — the same checks
//! [`build_dns_sd_config`] applies at parse time). A DNS server that is
//! unreachable at startup does NOT fail `new()`: resolution happens on the
//! background thread, and a refresh where **every** name fails keeps the
//! last-good snapshot rather than wiping it (see [`resolve::resolve_target_groups`]).
//!
//! ## Port defaulting (faithful to upstream)
//!
//! A/AAAA **require** a `port` (upstream `getAAddrLabels` errors when unset).
//! MX defaults the port to `25` when unset (upstream `getMXAddrLabels`:
//! `port := 25; if sdc.Port != nil { port = *sdc.Port }`) — see
//! [`DnsSdConfig::mx_port`]. SRV ignores `port` entirely (each SRV record
//! carries its own).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use super::config::ScrapeError;
use super::discovery::{Discovery, TargetGroup};

pub mod labels;
pub mod resolve;
pub mod wire;

/// Default `dns_sd_config` refresh interval, matching
/// `-promscrape.dnsSDCheckInterval`'s default (`dns.SDCheckInterval` =
/// `30*time.Second`). `scrape::wiring::apply_dns_sd_check_interval` overrides
/// it from the flag; [`build_dns_sd_config`] seeds it at parse time.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Default MX target port when a `dns_sd_config` of `type: MX` omits `port`.
/// Mirrors upstream `getMXAddrLabels`'s `port := 25`.
const DEFAULT_MX_PORT: u16 = 25;

/// The DNS record type a `dns_sd_config` queries. Port of `dns.SDConfig.Type`
/// (`SRV` default, plus `A`/`AAAA`/`MX`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DnsRecordType {
    /// `SRV` (the default). Each record carries its own target host + port.
    #[default]
    Srv,
    /// `A`: IPv4 address records (resolved via getaddrinfo).
    A,
    /// `AAAA`: IPv6 address records (resolved via getaddrinfo).
    Aaaa,
    /// `MX`: mail-exchange records.
    Mx,
}

impl DnsRecordType {
    /// Whether this type requires a configured `port`. Only A/AAAA do
    /// (upstream `getAAddrLabels` errors when unset); MX defaults to `25` and
    /// SRV carries its own port per record.
    fn requires_port(self) -> bool {
        matches!(self, DnsRecordType::A | DnsRecordType::Aaaa)
    }
}

/// Local `dns_sd_config` shape. Port of `discovery/dns.SDConfig`'s supported
/// fields, built via [`build_dns_sd_config`] from its [`RawDnsSdConfig`].
///
/// `nameserver` is NOT a YAML field (upstream has none); it defaults to
/// `None` (discover from `/etc/resolv.conf`) and exists so tests can point
/// SRV/MX queries at an in-process stub DNS server. `refresh_interval` is
/// likewise not a YAML field — upstream reads it from
/// `-promscrape.dnsSDCheckInterval` — and is overridden at wiring time.
///
/// Defined here (rather than in `scrape::config`) and re-exported from there,
/// mirroring [`super::digitalocean::DigitaloceanSdConfig`], to keep
/// `config.rs` under the repo's 800-line cap. No secrets, so `Debug` is
/// derived (no redaction needed).
#[derive(Debug, Clone, PartialEq)]
pub struct DnsSdConfig {
    pub names: Vec<String>,
    pub record_type: DnsRecordType,
    pub port: Option<u16>,
    pub refresh_interval: Duration,
    /// Explicit nameserver override for SRV/MX (`host` or `host:port`); `None`
    /// means discover from `/etc/resolv.conf`. Not a YAML field.
    pub nameserver: Option<String>,
}

impl Default for DnsSdConfig {
    fn default() -> Self {
        DnsSdConfig {
            names: Vec::new(),
            record_type: DnsRecordType::Srv,
            port: None,
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
            nameserver: None,
        }
    }
}

impl DnsSdConfig {
    /// The configured port, or `0` when unset (only reachable for SRV, which
    /// ignores it — A/AAAA are validated to carry a port and MX uses
    /// [`mx_port`]).
    ///
    /// [`mx_port`]: DnsSdConfig::mx_port
    fn port_or_default(&self) -> u16 {
        self.port.unwrap_or(0)
    }

    /// The MX target port: the configured port, or upstream's default of `25`
    /// when unset. Mirrors `getMXAddrLabels`'s
    /// `port := 25; if sdc.Port != nil { port = *sdc.Port }`.
    fn mx_port(&self) -> u16 {
        self.port.unwrap_or(DEFAULT_MX_PORT)
    }

    /// Validates the config the way upstream `GetLabels` does before
    /// resolving: non-empty `names`, and a `port` present for A/AAAA.
    /// (Type validity is guaranteed by the [`DnsRecordType`] enum; MX defaults
    /// its port to 25, and SRV carries its own port per record.)
    fn validate(&self) -> Result<(), ScrapeError> {
        if self.names.is_empty() {
            return Err(ScrapeError::new(
                "`names` cannot be empty in `dns_sd_config`",
            ));
        }
        if self.record_type.requires_port() && self.port.is_none() {
            return Err(ScrapeError::new(format!(
                "missing `port` in `dns_sd_config` for `type: {:?}`",
                self.record_type
            )));
        }
        Ok(())
    }
}

/// Undefaulted `dns_sd_config` list-entry shape for `serde_yaml_ng`. No
/// secrets, so `Debug` is derived. Lives here (not in `scrape::config`)
/// alongside [`DnsSdConfig`] and [`build_dns_sd_config`], keeping `config.rs`
/// under the repo's 800-line cap.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(crate) struct RawDnsSdConfig {
    names: Vec<String>,
    #[serde(rename = "type")]
    record_type: Option<String>,
    port: Option<u16>,
}

/// Builds a [`DnsSdConfig`] from its raw form, validating at parse time the
/// same conditions [`DnsSdConfig::validate`] enforces plus the `type` string
/// itself. Port of `dns.SDConfig.GetLabels`'s up-front checks: rejects empty
/// `names`, an unrecognized `type`, and a missing `port` for A/AAAA (MX
/// defaults its port to 25; SRV carries its own).
pub(crate) fn build_dns_sd_config(raw: RawDnsSdConfig) -> Result<DnsSdConfig, ScrapeError> {
    let record_type = parse_record_type(raw.record_type.as_deref())?;
    let cfg = DnsSdConfig {
        names: raw.names,
        record_type,
        port: raw.port,
        refresh_interval: DEFAULT_REFRESH_INTERVAL,
        nameserver: None,
    };
    cfg.validate()?;
    Ok(cfg)
}

/// Parses the `type` string (case-insensitive; empty/unset defaults to SRV).
/// Port of `GetLabels`'s `strings.ToUpper(typ)` switch.
fn parse_record_type(raw: Option<&str>) -> Result<DnsRecordType, ScrapeError> {
    let typ = raw.unwrap_or("").trim();
    if typ.is_empty() {
        return Ok(DnsRecordType::Srv);
    }
    match typ.to_ascii_uppercase().as_str() {
        "SRV" => Ok(DnsRecordType::Srv),
        "A" => Ok(DnsRecordType::A),
        "AAAA" => Ok(DnsRecordType::Aaaa),
        "MX" => Ok(DnsRecordType::Mx),
        other => Err(ScrapeError::new(format!(
            "unexpected `type` in `dns_sd_config`: {other:?}; supported values: SRV, A, AAAA, MX"
        ))),
    }
}

/// How often [`wait_or_stop`] re-checks the stop flag while waiting between
/// refreshes. Local copy of the digitalocean/consul/ec2 constant.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`Discovery`] over one `dns_sd_config` entry. A background thread
/// re-resolves the target-group snapshot on `refresh_interval`; [`poll`]
/// clones the current snapshot.
///
/// [`poll`]: Discovery::poll
pub struct DnsDiscovery {
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Everything the refresh thread needs for its lifetime.
struct RefreshCtx {
    cfg: DnsSdConfig,
    snapshot: Arc<Mutex<Vec<TargetGroup>>>,
    stop: Arc<AtomicBool>,
    source: String,
}

impl DnsDiscovery {
    /// Validates `cfg` (failing only on bad config — see the module doc) and
    /// spawns the background refresh thread. The snapshot starts empty and is
    /// populated by the thread's first successful resolution.
    pub fn new(cfg: &DnsSdConfig, job: &str) -> Result<DnsDiscovery, ScrapeError> {
        cfg.validate()?;
        let snapshot = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let ctx = RefreshCtx {
            cfg: cfg.clone(),
            snapshot: Arc::clone(&snapshot),
            stop: Arc::clone(&stop),
            source: format!("{job}/dns"),
        };

        let handle = thread::Builder::new()
            .name("esmagent-dns-sd".to_string())
            .spawn(move || run(&ctx))
            .map_err(|e| ScrapeError::new(format!("cannot spawn esmagent dns_sd thread: {e}")))?;

        Ok(DnsDiscovery {
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

impl Drop for DnsDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Discovery for DnsDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.snapshot.lock().unwrap().clone()
    }
}

/// The refresh thread's whole life: re-resolve on `refresh_interval`. A
/// total-failure refresh (every name failed) is logged and retried at the
/// same cadence, keeping the previous snapshot; a partial success replaces it.
fn run(ctx: &RefreshCtx) {
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }
        match resolve::resolve_target_groups(&ctx.cfg, &ctx.source) {
            Ok(groups) => *ctx.snapshot.lock().unwrap() = groups,
            Err(e) => log::warn!(
                "esmagent dns_sd ({}): refresh failed, keeping last-good targets: {e}",
                ctx.source
            ),
        }
        if wait_or_stop(&ctx.stop, ctx.cfg.refresh_interval) {
            return;
        }
    }
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`]. Returns
/// `true` if `stop` was observed. Local copy of the digitalocean helper.
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
#[path = "dns_tests.rs"]
mod tests;
