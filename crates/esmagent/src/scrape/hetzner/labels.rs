//! Label helpers shared by the Hetzner Cloud ([`super::hcloud`]) and Robot
//! ([`super::robot`]) label builders: `host:port` joining and CIDR-network
//! masking.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use serde::Deserialize;

/// Deserializes a value that may be explicitly `null` (not just absent) into
/// its [`Default`]. `#[serde(default)]` alone only covers an *absent* field;
/// the Hetzner API sends explicit JSON `null` both for `meta.pagination.
/// next_page` on the last page and for a robot server's `subnet`, so those
/// fields also need this. Pair with `#[serde(default, deserialize_with = ...)]`.
pub fn null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: Default + Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
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

/// Parses a CIDR string (`ip/prefix`) and returns the masked network in
/// canonical `network/prefix` form, or `None` when the input is not a valid
/// CIDR. Port of Go's `net.ParseCIDR(s)` followed by `n.String()`: the host
/// bits below the prefix are zeroed (so `2001:db8::1/64` → `2001:db8::/64`),
/// and — like upstream `appendHCloudTargetLabels` — an unparseable value is
/// skipped by the caller.
pub fn parse_cidr_network(s: &str) -> Option<String> {
    let (ip_str, prefix_str) = s.split_once('/')?;
    let prefix: u8 = prefix_str.parse().ok()?;
    match ip_str.parse::<IpAddr>().ok()? {
        IpAddr::V4(v4) => {
            if prefix > 32 {
                return None;
            }
            let bits = u32::from(v4);
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            Some(format!("{}/{prefix}", Ipv4Addr::from(bits & mask)))
        }
        IpAddr::V6(v6) => {
            if prefix > 128 {
                return None;
            }
            let bits = u128::from(v6);
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            Some(format!("{}/{prefix}", Ipv6Addr::from(bits & mask)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipv6_host_is_bracketed() {
        assert_eq!(join_host_port("2001:db8::1", 80), "[2001:db8::1]:80");
        assert_eq!(join_host_port("1.2.3.4", 9100), "1.2.3.4:9100");
    }

    #[test]
    fn cidr_network_masks_host_bits() {
        // Already-masked network is returned unchanged (compressed form).
        assert_eq!(
            parse_cidr_network("2001:db8::/64").as_deref(),
            Some("2001:db8::/64")
        );
        // Host bits below the prefix are zeroed.
        assert_eq!(
            parse_cidr_network("2001:db8::1/64").as_deref(),
            Some("2001:db8::/64")
        );
        assert_eq!(
            parse_cidr_network("10.1.2.3/24").as_deref(),
            Some("10.1.2.0/24")
        );
    }

    #[test]
    fn invalid_cidr_is_none() {
        assert_eq!(parse_cidr_network(""), None);
        assert_eq!(parse_cidr_network("not-a-cidr"), None);
        assert_eq!(parse_cidr_network("2001:db8::"), None);
        assert_eq!(parse_cidr_network("2001:db8::/200"), None);
    }
}
