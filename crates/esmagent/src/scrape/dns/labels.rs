//! The `__meta_dns_*` label builders and the `host:port` helper.
//!
//! Port of `lib/promscrape/discovery/dns/dns.go`'s `appendAddrLabels`
//! (shared by SRV *and* A/AAAA — see below) and `appendMXLabels`, reshaped
//! for this crate's [`TargetGroup`] (one group per resolved record: the
//! record's `__address__` is the group's single `targets` entry and the
//! `__meta_dns_*` set becomes the group's `labels`, mirroring
//! `scrape::digitalocean::labels`).
//!
//! Upstream includes `__address__` in the returned label set because a
//! Prometheus label set *is* the target; this crate carries the address in
//! `TargetGroup::targets` instead, so the builders put it there and leave it
//! out of `labels`.
//!
//! Note the `__meta_dns_srv_record_*` labels are attached to A/AAAA records
//! too, not just SRV — upstream's `getAAddrLabels` calls the very same
//! `appendAddrLabels` as `getSRVAddrLabels`, so an A/AAAA target carries
//! `__meta_dns_srv_record_target` = the resolved IP and
//! `__meta_dns_srv_record_port` = the configured port. This port replicates
//! that faithfully.

use std::collections::BTreeMap;

use crate::scrape::discovery::TargetGroup;

/// The always-present queried-name label.
pub const META_DNS_NAME: &str = "__meta_dns_name";
/// SRV/A/AAAA record target label (the SRV target host, or the resolved IP).
pub const META_DNS_SRV_RECORD_TARGET: &str = "__meta_dns_srv_record_target";
/// SRV/A/AAAA record port label.
pub const META_DNS_SRV_RECORD_PORT: &str = "__meta_dns_srv_record_port";
/// MX record exchange (target) label.
pub const META_DNS_MX_RECORD_TARGET: &str = "__meta_dns_mx_record_target";

/// Builds one [`TargetGroup`] for a SRV or A/AAAA record. Port of
/// `appendAddrLabels`: `__address__` = `target:port` (in `targets`),
/// `__meta_dns_name` = the queried name, plus the SRV target/port labels.
pub fn append_addr_labels(name: &str, target: &str, port: u16, source: &str) -> TargetGroup {
    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    labels.insert(META_DNS_NAME.to_string(), name.to_string());
    labels.insert(META_DNS_SRV_RECORD_TARGET.to_string(), target.to_string());
    labels.insert(META_DNS_SRV_RECORD_PORT.to_string(), port.to_string());
    TargetGroup {
        targets: vec![join_host_port(target, port)],
        labels,
        source: source.to_string(),
    }
}

/// Builds one [`TargetGroup`] for an MX record. Port of `appendMXLabels`:
/// `__address__` = `target:port` (in `targets`), `__meta_dns_name` = the
/// queried name, `__meta_dns_mx_record_target` = the MX exchange host. Unlike
/// [`append_addr_labels`], MX carries no `__meta_dns_srv_record_*` labels.
pub fn append_mx_labels(name: &str, target: &str, port: u16, source: &str) -> TargetGroup {
    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    labels.insert(META_DNS_NAME.to_string(), name.to_string());
    labels.insert(META_DNS_MX_RECORD_TARGET.to_string(), target.to_string());
    TargetGroup {
        targets: vec![join_host_port(target, port)],
        labels,
        source: source.to_string(),
    }
}

/// `host:port`, bracketing `host` when it is an IPv6 address (contains a
/// `:`). Port of `discoveryutil.JoinHostPort` (local copy, matching
/// `scrape::digitalocean`/`scrape::consul`).
pub fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn srv_labels_match_upstream() {
        let g = append_addr_labels("_sip._tcp.example.com", "sip.example.com", 5060, "job/dns");
        assert_eq!(g.targets, vec!["sip.example.com:5060".to_string()]);
        assert_eq!(g.source, "job/dns");
        assert_eq!(g.labels[META_DNS_NAME], "_sip._tcp.example.com");
        assert_eq!(g.labels[META_DNS_SRV_RECORD_TARGET], "sip.example.com");
        assert_eq!(g.labels[META_DNS_SRV_RECORD_PORT], "5060");
        assert!(!g.labels.contains_key("__address__"));
    }

    #[test]
    fn a_record_labels_carry_srv_labels_too() {
        // Upstream getAAddrLabels reuses appendAddrLabels, so an A record's
        // resolved IP shows up under __meta_dns_srv_record_target.
        let g = append_addr_labels("host.example.com", "10.0.0.1", 9100, "s");
        assert_eq!(g.targets, vec!["10.0.0.1:9100".to_string()]);
        assert_eq!(g.labels[META_DNS_SRV_RECORD_TARGET], "10.0.0.1");
        assert_eq!(g.labels[META_DNS_SRV_RECORD_PORT], "9100");
    }

    #[test]
    fn aaaa_record_address_is_bracketed() {
        let g = append_addr_labels("host.example.com", "::1", 9100, "s");
        assert_eq!(g.targets, vec!["[::1]:9100".to_string()]);
    }

    #[test]
    fn mx_labels_match_upstream() {
        let g = append_mx_labels("example.com", "mail.example.com", 25, "s");
        assert_eq!(g.targets, vec!["mail.example.com:25".to_string()]);
        assert_eq!(g.labels[META_DNS_NAME], "example.com");
        assert_eq!(g.labels[META_DNS_MX_RECORD_TARGET], "mail.example.com");
        assert!(!g.labels.contains_key(META_DNS_SRV_RECORD_TARGET));
    }

    #[test]
    fn join_host_port_brackets_ipv6_only() {
        assert_eq!(join_host_port("10.0.0.1", 80), "10.0.0.1:80");
        assert_eq!(join_host_port("::1", 80), "[::1]:80");
        assert_eq!(join_host_port("host", 80), "host:80");
    }
}
