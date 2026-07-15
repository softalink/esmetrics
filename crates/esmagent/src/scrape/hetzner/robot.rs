//! Hetzner Robot (`role: robot`) server serde structs, the list-response
//! parser, and the `__meta_hetzner_*` / `__meta_hetzner_robot_*` label builder
//! ([`append_robot_target_labels`]).
//!
//! Port of `lib/promscrape/discovery/hetzner/robot.go` (v1.146.0):
//! `RobotServerEntry`/`RobotServer`/`RobotSubnet`, `parseRobotServers` (the
//! `/server` response is a top-level array of `{ "server": {...} }` wrappers),
//! and `appendRobotTargetLabels`, reshaped for this crate's [`TargetGroup`]
//! (one group per server; `__address__` in `targets`, `__meta_hetzner_*` in
//! `labels`) — mirroring [`super::hcloud`].

use std::collections::BTreeMap;
use std::net::IpAddr;

use serde::Deserialize;

use super::labels::join_host_port;
use crate::scrape::config::ScrapeError;
use crate::scrape::discovery::TargetGroup;

/// One `/server` array element: a `{ "server": {...} }` wrapper. Port of
/// `RobotServerEntry`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct RobotServerEntry {
    pub server: RobotServer,
}

/// One Hetzner Robot dedicated server. Port of `RobotServer`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct RobotServer {
    pub server_ip: String,
    #[serde(rename = "server_ipv6_net")]
    pub server_ipv6: String,
    pub server_number: i64,
    pub server_name: String,
    pub dc: String,
    pub status: String,
    pub product: String,
    #[serde(rename = "cancelled")]
    pub canceled: bool,
    /// `null` in the response deserializes to an empty vec (explicit `null`
    /// needs [`super::labels::null_default`], not just `#[serde(default)]`).
    #[serde(default, deserialize_with = "super::labels::null_default")]
    pub subnet: Vec<RobotSubnet>,
}

/// One `subnet` entry of a [`RobotServer`]. Port of `RobotSubnet`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct RobotSubnet {
    pub ip: String,
    pub mask: String,
}

/// Parses a `/server` response body (top-level array of wrappers) into the
/// inner servers. Port of `parseRobotServers`. `subnet: null` tolerated by the
/// field's `#[serde(default)]`.
pub fn parse_robot_servers(data: &[u8]) -> Result<Vec<RobotServer>, ScrapeError> {
    let entries: Vec<RobotServerEntry> = serde_json::from_slice(data).map_err(|e| ScrapeError {
        msg: format!("cannot unmarshal RobotServer list: {e}"),
    })?;
    Ok(entries.into_iter().map(|e| e.server).collect())
}

/// Builds one [`TargetGroup`] per robot server, mirroring
/// `getRobotServerLabels` + `appendRobotTargetLabels`. `__address__` is
/// `serverIP:default_port` (in `targets`); every `__meta_hetzner_*` /
/// `__meta_hetzner_robot_*` label goes in `labels`.
pub fn append_robot_target_labels(
    servers: &[RobotServer],
    default_port: u16,
    source: &str,
) -> Vec<TargetGroup> {
    servers
        .iter()
        .map(|server| robot_target_group(server, default_port, source))
        .collect()
}

fn robot_target_group(server: &RobotServer, default_port: u16, source: &str) -> TargetGroup {
    let address = join_host_port(&server.server_ip, default_port);

    let mut m: BTreeMap<String, String> = BTreeMap::new();
    m.insert("__meta_hetzner_role".into(), "robot".into());
    m.insert(
        "__meta_hetzner_server_id".into(),
        server.server_number.to_string(),
    );
    m.insert(
        "__meta_hetzner_server_name".into(),
        server.server_name.clone(),
    );
    m.insert("__meta_hetzner_datacenter".into(), server.dc.to_lowercase());
    m.insert(
        "__meta_hetzner_robot_datacenter".into(),
        server.dc.to_lowercase(),
    );
    m.insert(
        "__meta_hetzner_public_ipv4".into(),
        server.server_ip.clone(),
    );

    // First non-IPv4 subnet becomes the public IPv6 network (upstream breaks
    // after the first match). Go's `net.ParseIP(...).To4() == nil` is true both
    // for real IPv6 addresses and for unparseable inputs, so a parse failure is
    // treated as "not IPv4" too.
    for subnet in &server.subnet {
        let is_ipv4 = subnet
            .ip
            .parse::<IpAddr>()
            .map(|ip| ip.is_ipv4())
            .unwrap_or(false);
        if !is_ipv4 {
            m.insert(
                "__meta_hetzner_public_ipv6_network".into(),
                format!("{}/{}", subnet.ip, subnet.mask),
            );
            break;
        }
    }

    m.insert("__meta_hetzner_server_status".into(), server.status.clone());
    m.insert(
        "__meta_hetzner_robot_product".into(),
        server.product.clone(),
    );
    m.insert(
        "__meta_hetzner_robot_cancelled".into(),
        server.canceled.to_string(),
    );

    TargetGroup {
        targets: vec![address],
        labels: m,
        source: source.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Port of upstream `robot_test.go::TestParseRobotServerListResponse`: two
    /// servers parse (one with an IPv6 subnet, one with `subnet: null`), and
    /// `appendRobotTargetLabels` for the first yields exactly the expected
    /// `__meta_hetzner_*` set (dc lowercased, ipv6 subnet concatenated as
    /// `ip/mask`) and the `serverIP:port` `__address__`.
    #[test]
    fn parse_and_labels_match_upstream() {
        let data = br#"[
            {"server": {"server_ip": "123.123.123.123", "server_ipv6_net": "2a01:f48:111:4221::",
                        "server_number": 321, "server_name": "server1", "product": "DS 3000",
                        "dc": "NBG1-DC1", "status": "ready", "cancelled": false,
                        "subnet": [{"ip": "2a01:4f8:111:4221::", "mask": "64"}]}},
            {"server": {"server_ip": "123.123.123.124", "server_ipv6_net": "2a01:f48:111:4221::",
                        "server_number": 421, "server_name": "server2", "product": "X5",
                        "dc": "FSN1-DC10", "status": "ready", "cancelled": false, "subnet": null}}
        ]"#;
        let servers = parse_robot_servers(data).unwrap();
        assert_eq!(servers.len(), 2);
        assert_eq!(
            servers[0],
            RobotServer {
                server_ip: "123.123.123.123".into(),
                server_ipv6: "2a01:f48:111:4221::".into(),
                server_number: 321,
                server_name: "server1".into(),
                dc: "NBG1-DC1".into(),
                status: "ready".into(),
                product: "DS 3000".into(),
                canceled: false,
                subnet: vec![RobotSubnet {
                    ip: "2a01:4f8:111:4221::".into(),
                    mask: "64".into()
                }],
            }
        );
        assert!(servers[1].subnet.is_empty());

        let groups = append_robot_target_labels(&servers[..1], 123, "job/hetzner");
        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert_eq!(g.targets, vec!["123.123.123.123:123".to_string()]);
        let l = &g.labels;
        assert_eq!(l["__meta_hetzner_role"], "robot");
        assert_eq!(l["__meta_hetzner_server_id"], "321");
        assert_eq!(l["__meta_hetzner_server_name"], "server1");
        assert_eq!(l["__meta_hetzner_server_status"], "ready");
        assert_eq!(l["__meta_hetzner_public_ipv4"], "123.123.123.123");
        assert_eq!(
            l["__meta_hetzner_public_ipv6_network"],
            "2a01:4f8:111:4221::/64"
        );
        assert_eq!(l["__meta_hetzner_datacenter"], "nbg1-dc1");
        assert_eq!(l["__meta_hetzner_robot_datacenter"], "nbg1-dc1");
        assert_eq!(l["__meta_hetzner_robot_product"], "DS 3000");
        assert_eq!(l["__meta_hetzner_robot_cancelled"], "false");
        assert!(!l.contains_key("__address__"));
    }

    /// A server with no subnets omits the ipv6 label entirely.
    #[test]
    fn no_subnet_omits_ipv6_label() {
        let server = RobotServer {
            server_ip: "1.2.3.4".into(),
            server_number: 1,
            dc: "hel1-dc2".into(),
            ..RobotServer::default()
        };
        let groups = append_robot_target_labels(&[server], 80, "s");
        let l = &groups[0].labels;
        assert!(!l.contains_key("__meta_hetzner_public_ipv6_network"));
        assert_eq!(l["__meta_hetzner_datacenter"], "hel1-dc2");
    }
}
