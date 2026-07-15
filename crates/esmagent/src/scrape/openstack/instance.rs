//! Nova `servers/detail` JSON response structs (parsed with `serde_json`) and
//! the `__meta_openstack_*` label builder for `role: instance`.
//!
//! Port of `lib/promscrape/discovery/openstack/instance.go` (`serversDetail`/
//! `server`/`serverAddress`/`serverFlavor` structs + `addInstanceLabels`),
//! reshaped for this crate's [`TargetGroup`]: one group per server *address*
//! (a non-floating IP in a pool), whose single `__address__` (that IP +
//! configured port) is carried in `targets` and whose `__meta_openstack_*` set
//! is `labels` — mirroring `scrape::gce::labels`.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::sanitize_label_name;

use super::join_host_port;

/// `servers/detail` response. Port of `instance.go`'s `serversDetail`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ServersDetail {
    pub servers: Vec<Server>,
    #[serde(rename = "servers_links")]
    pub links: Vec<Link>,
}

/// Port of `instance.go`'s `link` (a paginated-next pointer).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Link {
    pub href: String,
    pub rel: String,
}

/// Port of `instance.go`'s `server`, narrowed to the fields
/// `addInstanceLabels` reads. `addresses` is a JSON object keyed by pool name;
/// deserializing into a [`BTreeMap`] gives the deterministic alphabetical pool
/// order upstream sorts for explicitly.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Server {
    pub id: String,
    #[serde(rename = "tenant_id")]
    pub tenant_id: String,
    #[serde(rename = "user_id")]
    pub user_id: String,
    pub name: String,
    #[serde(rename = "hostid")]
    pub host_id: String,
    pub status: String,
    pub addresses: BTreeMap<String, Vec<ServerAddress>>,
    pub metadata: BTreeMap<String, String>,
    pub flavor: ServerFlavor,
}

/// Port of `instance.go`'s `serverAddress`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ServerAddress {
    #[serde(rename = "addr")]
    pub address: String,
    pub version: i64,
    #[serde(rename = "OS-EXT-IPS:type")]
    pub type_: String,
}

/// Port of `instance.go`'s `serverFlavor`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ServerFlavor {
    pub id: String,
}

/// Parses a `servers/detail` JSON response. Port of `parseServersDetail`.
pub fn parse_servers_detail(data: &[u8]) -> Result<ServersDetail, String> {
    serde_json::from_slice(data).map_err(|e| format!("cannot parse serversDetail: {e}"))
}

/// Builds one [`TargetGroup`] per discovered server address, mirroring
/// `addInstanceLabels`. For each server, pools are traversed in alphabetical
/// order (via the [`BTreeMap`]); within a pool the single `floating` address
/// (if any) becomes `__meta_openstack_public_ip`, and every non-floating,
/// non-empty address yields a target whose `__address__` (that IP + `port`) is
/// carried in `targets`. `source` is threaded through unchanged.
pub fn add_instance_labels(servers: &[Server], port: u16, source: &str) -> Vec<TargetGroup> {
    let mut ms = Vec::new();
    for server in servers {
        let mut common: BTreeMap<String, String> = BTreeMap::new();
        common.insert("__meta_openstack_instance_id".into(), server.id.clone());
        common.insert(
            "__meta_openstack_instance_status".into(),
            server.status.clone(),
        );
        common.insert("__meta_openstack_instance_name".into(), server.name.clone());
        common.insert(
            "__meta_openstack_project_id".into(),
            server.tenant_id.clone(),
        );
        common.insert("__meta_openstack_user_id".into(), server.user_id.clone());
        common.insert(
            "__meta_openstack_instance_flavor".into(),
            server.flavor.id.clone(),
        );
        for (k, v) in &server.metadata {
            common.insert(
                sanitize_label_name(&format!("__meta_openstack_tag_{k}")),
                v.clone(),
            );
        }

        // `addresses` is a BTreeMap, so iteration is already in alphabetical
        // pool order (upstream sorts `sortedPools` for the same reason).
        for (pool, addresses) in &server.addresses {
            if addresses.is_empty() {
                continue;
            }
            // At most one floating IP per pool.
            let public_ip = addresses
                .iter()
                .find(|ip| ip.type_ == "floating")
                .map(|ip| ip.address.clone());
            for ip in addresses {
                if ip.address.is_empty() || ip.type_ == "floating" {
                    continue;
                }
                let mut m = common.clone();
                m.insert("__meta_openstack_address_pool".into(), pool.clone());
                m.insert("__meta_openstack_private_ip".into(), ip.address.clone());
                if let Some(public) = &public_ip {
                    m.insert("__meta_openstack_public_ip".into(), public.clone());
                }
                ms.push(TargetGroup {
                    targets: vec![join_host_port(&ip.address, port)],
                    labels: m,
                    source: source.to_string(),
                });
            }
        }
    }
    ms
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `servers/detail` fixture from upstream `instance_test.go`
    /// (`TestParseServersDetail`).
    const SERVERS_JSON: &str = r#"{
   "servers":[
      {
         "id":"c9f68076-01a3-489a-aebe-8b773c71e7f3",
         "name":"test10",
         "status":"ACTIVE",
         "tenant_id":"d34be4e44f9c444eab9a5ec7b953951f",
         "user_id":"e55737f142ac42f18093037760656bd7",
         "metadata":{},
         "hostId":"e26db8db23736877aa92ebbbe11743b2a2a3b107aada00a8a0cf474b",
         "flavor":{ "id":"1" },
         "addresses":{
            "test":[
               { "version":4, "addr":"192.168.222.15", "OS-EXT-IPS:type":"fixed" },
               { "version":4, "addr":"10.20.20.69", "OS-EXT-IPS:type":"floating" }
            ]
         }
      }
   ]
}"#;

    #[test]
    fn parses_servers_detail_fields() {
        let sd = parse_servers_detail(SERVERS_JSON.as_bytes()).unwrap();
        assert_eq!(sd.servers.len(), 1);
        let s = &sd.servers[0];
        assert_eq!(s.id, "c9f68076-01a3-489a-aebe-8b773c71e7f3");
        assert_eq!(s.tenant_id, "d34be4e44f9c444eab9a5ec7b953951f");
        assert_eq!(s.user_id, "e55737f142ac42f18093037760656bd7");
        assert_eq!(s.name, "test10");
        assert_eq!(s.status, "ACTIVE");
        assert_eq!(s.flavor.id, "1");
        let test_pool = &s.addresses["test"];
        assert_eq!(test_pool.len(), 2);
        assert_eq!(test_pool[0].address, "192.168.222.15");
        assert_eq!(test_pool[0].type_, "fixed");
        assert_eq!(test_pool[1].type_, "floating");
    }

    fn one_fixed_server() -> Server {
        let mut addresses = BTreeMap::new();
        addresses.insert(
            "test".to_string(),
            vec![ServerAddress {
                address: "192.168.0.1".into(),
                version: 4,
                type_: "fixed".into(),
            }],
        );
        Server {
            id: "10".into(),
            status: "enabled".into(),
            name: "server-1".into(),
            host_id: "some-host-id".into(),
            tenant_id: "some-tenant-id".into(),
            user_id: "some-user-id".into(),
            flavor: ServerFlavor { id: "5".into() },
            addresses,
            metadata: BTreeMap::new(),
        }
    }

    /// One server, one fixed address — matches upstream `TestAddInstanceLabels`
    /// first vector (port 9100).
    #[test]
    fn builds_instance_labels_single_fixed() {
        let g = add_instance_labels(&[one_fixed_server()], 9100, "src");
        assert_eq!(g.len(), 1);
        let t = &g[0];
        assert_eq!(t.targets, vec!["192.168.0.1:9100".to_string()]);
        assert!(!t.labels.contains_key("__address__"));
        let l = &t.labels;
        assert_eq!(l["__meta_openstack_address_pool"], "test");
        assert_eq!(l["__meta_openstack_instance_flavor"], "5");
        assert_eq!(l["__meta_openstack_instance_id"], "10");
        assert_eq!(l["__meta_openstack_instance_name"], "server-1");
        assert_eq!(l["__meta_openstack_instance_status"], "enabled");
        assert_eq!(l["__meta_openstack_private_ip"], "192.168.0.1");
        assert_eq!(l["__meta_openstack_project_id"], "some-tenant-id");
        assert_eq!(l["__meta_openstack_user_id"], "some-user-id");
        // No floating IP in this pool -> no public_ip.
        assert!(!l.contains_key("__meta_openstack_public_ip"));
        assert_eq!(l.len(), 8, "labels={l:?}");
        assert_eq!(t.source, "src");
    }

    /// Two pools, one with a floating IP — matches upstream
    /// `TestAddInstanceLabels` second vector: alphabetical pool order
    /// (`internal` before `test`) and `public_ip` only on the pool that has a
    /// floating address.
    #[test]
    fn builds_instance_labels_with_public_ip_and_pool_order() {
        let mut addresses = BTreeMap::new();
        addresses.insert(
            "test".to_string(),
            vec![
                ServerAddress {
                    address: "192.168.0.1".into(),
                    version: 4,
                    type_: "fixed".into(),
                },
                ServerAddress {
                    address: "1.5.5.5".into(),
                    version: 4,
                    type_: "floating".into(),
                },
            ],
        );
        addresses.insert(
            "internal".to_string(),
            vec![ServerAddress {
                address: "10.10.0.1".into(),
                version: 4,
                type_: "fixed".into(),
            }],
        );
        let server = Server {
            id: "10".into(),
            status: "enabled".into(),
            name: "server-2".into(),
            host_id: "some-host-id".into(),
            tenant_id: "some-tenant-id".into(),
            user_id: "some-user-id".into(),
            flavor: ServerFlavor { id: "5".into() },
            addresses,
            metadata: BTreeMap::new(),
        };
        let g = add_instance_labels(&[server], 9100, "src");
        assert_eq!(g.len(), 2);
        // internal pool comes first (alphabetical), and has no public_ip.
        assert_eq!(g[0].targets, vec!["10.10.0.1:9100".to_string()]);
        assert_eq!(g[0].labels["__meta_openstack_address_pool"], "internal");
        assert_eq!(g[0].labels["__meta_openstack_private_ip"], "10.10.0.1");
        assert!(!g[0].labels.contains_key("__meta_openstack_public_ip"));
        // test pool second, carries the floating IP as public_ip.
        assert_eq!(g[1].targets, vec!["192.168.0.1:9100".to_string()]);
        assert_eq!(g[1].labels["__meta_openstack_address_pool"], "test");
        assert_eq!(g[1].labels["__meta_openstack_private_ip"], "192.168.0.1");
        assert_eq!(g[1].labels["__meta_openstack_public_ip"], "1.5.5.5");
    }

    #[test]
    fn metadata_becomes_sanitized_tag_labels() {
        let mut s = one_fixed_server();
        s.metadata.insert("some.key".into(), "v".into());
        let g = add_instance_labels(&[s], 80, "src");
        assert_eq!(g[0].labels["__meta_openstack_tag_some_key"], "v");
    }
}
