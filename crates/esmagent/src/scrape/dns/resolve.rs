//! Resolution: turns a validated [`DnsSdConfig`] into [`TargetGroup`]s.
//!
//! Two strategies, matching the record type (see the module doc in [`super`]):
//!
//! - **A / AAAA** go through [`std::net::ToSocketAddrs`] (getaddrinfo via the
//!   OS resolver) — resolve `(name, 0)`, keep the IPv4 addrs (A) or IPv6 addrs
//!   (AAAA). Cross-platform, no nameserver discovery needed. Close port of
//!   Go's `LookupIPAddr` + the `To4()` family filter in `getAAddrLabels`.
//! - **SRV / MX** need real DNS queries getaddrinfo can't do, so they go
//!   through the hand-rolled [`super::wire`] client, pointed at a nameserver
//!   discovered from `/etc/resolv.conf` (unix) or the config's explicit
//!   `nameserver` override (used by tests to target an in-process stub).
//!
//! ## Failure semantics
//!
//! Per-name lookup failures are logged and skipped, matching upstream's
//! per-name `continue`. [`resolve_target_groups`] returns `Ok` with whatever
//! resolved (possibly empty) as long as *at least one* name resolved, or the
//! config has no names to resolve; it returns `Err` only when **every** name
//! failed (so the caller keeps its last-good snapshot rather than wiping it on
//! a transient total DNS outage — see [`super::DnsDiscovery`]). On non-unix
//! with no `nameserver` override, SRV/MX resolution has no nameserver and all
//! names fail this way (logged), which keeps the build compiling and the
//! Windows CI path exercising the override instead.

use std::net::ToSocketAddrs;
use std::time::Duration;

use super::labels::{append_addr_labels, append_mx_labels};
use super::wire::{self, QTYPE_MX, QTYPE_SRV};
use super::{DnsRecordType, DnsSdConfig};
use crate::scrape::config::ScrapeError;
use crate::scrape::discovery::TargetGroup;

/// Per-query socket read timeout for SRV/MX (bounds the refresh thread + its
/// `Drop`). A/AAAA go through getaddrinfo, which the OS bounds itself.
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Resolves every name in `cfg` into target groups. See the module doc for
/// the failure semantics behind the `Result`.
pub fn resolve_target_groups(
    cfg: &DnsSdConfig,
    source: &str,
) -> Result<Vec<TargetGroup>, ScrapeError> {
    if cfg.names.is_empty() {
        return Ok(Vec::new());
    }
    let nameserver = match cfg.record_type {
        DnsRecordType::Srv | DnsRecordType::Mx => discover_nameserver(cfg),
        DnsRecordType::A | DnsRecordType::Aaaa => None,
    };

    let mut groups = Vec::new();
    let mut successes = 0usize;
    for name in &cfg.names {
        match resolve_one(cfg, name, nameserver.as_deref(), source) {
            Ok(mut g) => {
                successes += 1;
                groups.append(&mut g);
            }
            Err(e) => log::warn!(
                "esmagent dns_sd: {:?} lookup for {name:?} failed: {e}",
                cfg.record_type
            ),
        }
    }

    if successes == 0 {
        return Err(ScrapeError::new(format!(
            "all {} dns_sd lookups failed",
            cfg.names.len()
        )));
    }
    Ok(groups)
}

/// Resolves a single name. A/AAAA use getaddrinfo; SRV/MX use the wire client
/// (erroring when no nameserver is available).
fn resolve_one(
    cfg: &DnsSdConfig,
    name: &str,
    nameserver: Option<&str>,
    source: &str,
) -> Result<Vec<TargetGroup>, String> {
    match cfg.record_type {
        DnsRecordType::A => resolve_ip(name, cfg.port_or_default(), true, source),
        DnsRecordType::Aaaa => resolve_ip(name, cfg.port_or_default(), false, source),
        DnsRecordType::Srv => resolve_srv(name, nameserver, source),
        DnsRecordType::Mx => resolve_mx(name, cfg.mx_port(), nameserver, source),
    }
}

/// A/AAAA via getaddrinfo: resolve `(name, 0)` and keep addrs of the wanted
/// family. Port of `getAAddrLabels`'s `LookupIPAddr` + `To4()` filter.
fn resolve_ip(
    name: &str,
    port: u16,
    want_ipv4: bool,
    source: &str,
) -> Result<Vec<TargetGroup>, String> {
    let addrs = (name, 0u16).to_socket_addrs().map_err(|e| e.to_string())?;
    let mut groups = Vec::new();
    for addr in addrs {
        let ip = addr.ip();
        if ip.is_ipv4() != want_ipv4 {
            continue;
        }
        groups.push(append_addr_labels(name, &ip.to_string(), port, source));
    }
    Ok(groups)
}

/// SRV via the wire client. Each answer becomes a group whose address is
/// `target:record_port` (the SRV record carries its own port).
fn resolve_srv(
    name: &str,
    nameserver: Option<&str>,
    source: &str,
) -> Result<Vec<TargetGroup>, String> {
    let ns = nameserver.ok_or_else(|| "no nameserver available for SRV lookup".to_string())?;
    let records = wire::query(ns, name, QTYPE_SRV, QUERY_TIMEOUT).map_err(|e| e.to_string())?;
    Ok(records
        .into_iter()
        .map(|r| append_addr_labels(name, &r.target, r.port, source))
        .collect())
}

/// MX via the wire client. Each answer becomes a group whose address is
/// `exchange:port` (the MX record carries no port, so `port` comes from the
/// config or defaults to 25 — see [`super::DnsSdConfig::mx_port`]).
fn resolve_mx(
    name: &str,
    port: u16,
    nameserver: Option<&str>,
    source: &str,
) -> Result<Vec<TargetGroup>, String> {
    let ns = nameserver.ok_or_else(|| "no nameserver available for MX lookup".to_string())?;
    let records = wire::query(ns, name, QTYPE_MX, QUERY_TIMEOUT).map_err(|e| e.to_string())?;
    Ok(records
        .into_iter()
        .map(|r| append_mx_labels(name, &r.target, port, source))
        .collect())
}

/// The nameserver to send SRV/MX queries to: the config's explicit override
/// (tests / advanced setups) if set, else the first `nameserver` line of
/// `/etc/resolv.conf` on unix. On non-unix with no override this is `None`
/// (logged by the caller) — see the module doc.
fn discover_nameserver(cfg: &DnsSdConfig) -> Option<String> {
    if let Some(ns) = &cfg.nameserver {
        return Some(ns.clone());
    }
    resolv_conf_nameserver()
}

/// First `nameserver <addr>` entry in `/etc/resolv.conf`. Returns `None` on
/// any read failure or on platforms without the file (e.g. Windows), where
/// SRV/MX degrade to "no targets + a logged warning" unless an override is
/// set — the compiling, Windows-safe path required by the task.
#[cfg(unix)]
fn resolv_conf_nameserver() -> Option<String> {
    let contents = std::fs::read_to_string("/etc/resolv.conf").ok()?;
    parse_resolv_conf(&contents)
}

#[cfg(not(unix))]
fn resolv_conf_nameserver() -> Option<String> {
    log::warn!(
        "esmagent dns_sd: SRV/MX nameserver discovery from resolv.conf is unavailable on this \
         platform; set a nameserver override to enable SRV/MX discovery"
    );
    None
}

/// Extracts the first `nameserver <addr>` directive. Comments (`#`/`;`) and
/// blank lines are ignored. Shared by the unix path and the unit test.
#[cfg_attr(not(any(unix, test)), allow(dead_code))]
fn parse_resolv_conf(contents: &str) -> Option<String> {
    for line in contents.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        let mut parts = line.split_whitespace();
        if parts.next() == Some("nameserver") {
            if let Some(addr) = parts.next() {
                return Some(addr.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_resolv_conf_takes_first_nameserver() {
        let contents =
            "# a comment\n; another\nsearch example.com\nnameserver 8.8.8.8\nnameserver 8.8.4.4\n";
        assert_eq!(parse_resolv_conf(contents).as_deref(), Some("8.8.8.8"));
    }

    #[test]
    fn parse_resolv_conf_none_when_absent() {
        assert!(parse_resolv_conf("search example.com\noptions ndots:1\n").is_none());
    }

    #[test]
    fn resolve_a_record_for_localhost() {
        // localhost resolves to 127.0.0.1 on essentially every CI host.
        let groups = resolve_ip("localhost", 9100, true, "s").expect("localhost A lookup");
        assert!(
            groups
                .iter()
                .any(|g| g.targets == vec!["127.0.0.1:9100".to_string()]),
            "expected 127.0.0.1:9100 among {groups:?}"
        );
        for g in &groups {
            assert_eq!(g.labels[super::super::labels::META_DNS_NAME], "localhost");
        }
    }
}
