//! Shared OVHcloud helpers: IP-list parsing and ipv4/ipv6 selection.
//!
//! Port of `lib/promscrape/discovery/ovhcloud/common.go`'s `parseIPList` plus
//! the per-service `for _, ip := range server.IPs { ... }` ipv4/ipv6 selection
//! repeated verbatim in `dedicated_server.go` and `vps.go`. Both services fetch
//! a `/ips` array separately from the instance detail, parse it here, then
//! pick a default `__address__` from it — so the logic lives in one place.

use std::net::IpAddr;

use crate::scrape::config::ScrapeError;

/// Parses the OVH `/ips` response (a JSON array of addresses and/or CIDR
/// prefixes) into concrete [`IpAddr`]s. Port of `common.go`'s `parseIPList`:
///
/// - A bare address is kept as-is.
/// - A CIDR prefix is kept only when its mask is exactly `/32` (the address is
///   used); any other prefix length is skipped — this is how upstream drops the
///   `.../64` IPv6 prefixes OVH returns for dedicated servers while keeping the
///   `.../32` IPv4 address.
/// - Unspecified (all-zero) addresses are dropped.
///
/// Errors when the list yields no usable address (matching upstream), so the
/// caller can skip that instance.
pub fn parse_ip_list(ip_list: &[String]) -> Result<Vec<IpAddr>, ScrapeError> {
    let mut addrs = Vec::new();
    for ip in ip_list {
        let addr = match ip.parse::<IpAddr>() {
            Ok(a) => a,
            Err(_) => match parse_prefix(ip) {
                // Upstream keeps only /32 prefixes; anything else is skipped.
                Some((a, 32)) => a,
                Some(_) => continue,
                None => {
                    return Err(ScrapeError::new(format!(
                        "could not parse IP addresses: {ip}"
                    )));
                }
            },
        };
        if !addr.is_unspecified() {
            addrs.push(addr);
        }
    }
    if addrs.is_empty() {
        return Err(ScrapeError::new(format!(
            "could not parse IP addresses from ip list: {ip_list:?}"
        )));
    }
    Ok(addrs)
}

/// Parses a `addr/bits` CIDR string into its address and mask length, or `None`
/// when either part is malformed. Std has no CIDR parser, so this mirrors
/// `netip.ParsePrefix`'s split just enough for [`parse_ip_list`].
fn parse_prefix(s: &str) -> Option<(IpAddr, u8)> {
    let (addr, bits) = s.split_once('/')?;
    let addr = addr.parse::<IpAddr>().ok()?;
    let bits = bits.parse::<u8>().ok()?;
    Some((addr, bits))
}

/// Selects the `(ipv4, ipv6)` string pair from parsed addresses, taking the
/// last of each family (matching upstream's overwrite-in-loop). Empty when a
/// family is absent.
pub fn split_ipv4_ipv6(ips: &[IpAddr]) -> (String, String) {
    let mut ipv4 = String::new();
    let mut ipv6 = String::new();
    for ip in ips {
        match ip {
            IpAddr::V4(_) => ipv4 = ip.to_string(),
            IpAddr::V6(_) => ipv6 = ip.to_string(),
        }
    }
    (ipv4, ipv6)
}

/// The default `__address__` for an instance: its IPv4 if present, else its
/// IPv6. Port of the `defaultIP := ipv4; if defaultIP == "" { defaultIP = ipv6 }`
/// idiom shared by both services.
pub fn default_ip(ipv4: &str, ipv6: &str) -> String {
    if ipv4.is_empty() {
        ipv6.to_string()
    } else {
        ipv4.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Dedicated-server `/ips` case: a `.../64` IPv6 prefix is dropped and the
    /// `.../32` IPv4 prefix's address is kept. Mirrors upstream's
    /// `dedicated_server_test.go` fixture (ipv6 ends up empty).
    #[test]
    fn keeps_slash32_drops_other_prefixes() {
        let ips = parse_ip_list(&[
            "2001:40d0:302:8874::/64".to_string(),
            "50.75.126.113/32".to_string(),
        ])
        .unwrap();
        assert_eq!(ips.len(), 1);
        let (v4, v6) = split_ipv4_ipv6(&ips);
        assert_eq!(v4, "50.75.126.113");
        assert_eq!(v6, "");
        assert_eq!(default_ip(&v4, &v6), "50.75.126.113");
    }

    /// VPS `/ips` case: bare v4 + v6 addresses are both kept; default is the v4.
    #[test]
    fn keeps_bare_v4_and_v6() {
        let ips = parse_ip_list(&[
            "139.99.154.158".to_string(),
            "2402:1f00:8100:401::bb6".to_string(),
        ])
        .unwrap();
        let (v4, v6) = split_ipv4_ipv6(&ips);
        assert_eq!(v4, "139.99.154.158");
        assert_eq!(v6, "2402:1f00:8100:401::bb6");
        assert_eq!(default_ip(&v4, &v6), "139.99.154.158");
    }

    /// v6-only yields the v6 as the default address.
    #[test]
    fn v6_only_defaults_to_v6() {
        let ips = parse_ip_list(&["2402:1f00::1".to_string()]).unwrap();
        let (v4, v6) = split_ipv4_ipv6(&ips);
        assert_eq!(v4, "");
        assert_eq!(default_ip(&v4, &v6), "2402:1f00::1");
    }

    /// An empty / all-unusable list errors so the instance is skipped.
    #[test]
    fn empty_list_errors() {
        assert!(parse_ip_list(&[]).is_err());
        assert!(parse_ip_list(&["0.0.0.0".to_string()]).is_err());
        assert!(parse_ip_list(&["not-an-ip".to_string()]).is_err());
    }
}
