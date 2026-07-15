//! Label-vector tests ported from upstream
//! `lib/promscrape/discovery/docker/container_test.go`, validated exactly
//! against its `__meta_docker_*` + `__address__` expectations.

use super::*;
use crate::scrape::docker::network::{network_labels_by_id, parse_networks};

/// The 3-network fixture (host/none/bridge) from `TestAddContainerLabels`.
const NETWORKS_JSON: &[u8] = br#"[
  { "Name": "host", "Id": "6a1989488dcb", "Scope": "local", "Driver": "host",
    "Internal": false, "Ingress": false, "Labels": {} },
  { "Name": "none", "Id": "c9668d06973d", "Scope": "local", "Driver": "null",
    "Internal": false, "Ingress": false, "Labels": {} },
  { "Name": "bridge", "Id": "1dd8d1a8bef59943345c7231d7ce8268333ff5a8c5b3c94881e6b4742b447634",
    "Scope": "local", "Driver": "bridge", "Internal": false, "Ingress": false, "Labels": {} }
]"#;

fn bridge_labels() -> BTreeMap<String, BTreeMap<String, String>> {
    network_labels_by_id(&parse_networks(NETWORKS_JSON).unwrap())
}

fn group_for<'a>(groups: &'a [TargetGroup], addr: &str) -> &'a TargetGroup {
    groups
        .iter()
        .find(|g| g.targets == vec![addr.to_string()])
        .unwrap_or_else(|| panic!("no group with address {addr}"))
}

fn crow_server(network_mode: &str, network_key: &str, ports: Vec<ContainerPort>) -> Container {
    Container {
        id: "90bc3b31aa13da5c0b11af2e228d54b38428a84e25d4e249ae9e9c95e51a0700".into(),
        names: vec!["/crow-server".into()],
        labels: BTreeMap::from([
            (
                "com.docker.compose.config-hash".to_string(),
                "c9f0bd5bb31921f94cff367d819a30a0cc08d4399080897a6c5cd74b983156ec".to_string(),
            ),
            (
                "com.docker.compose.service".to_string(),
                "crow-server".to_string(),
            ),
        ]),
        ports,
        host_config: ContainerHostConfig {
            network_mode: network_mode.into(),
        },
        network_settings: ContainerNetworkSettings {
            networks: BTreeMap::from([(
                network_key.to_string(),
                ContainerNetwork {
                    ip_address: "172.17.0.2".into(),
                    network_id: "1dd8d1a8bef59943345c7231d7ce8268333ff5a8c5b3c94881e6b4742b447634"
                        .into(),
                },
            )]),
        },
    }
}

/// Port of `TestParseContainers`: two containers parse with their ports and
/// single bridge network.
#[test]
fn parse_containers_extracts_ports_and_networks() {
    let data = br#"[
  { "Id": "90bc3b31", "Names": ["/crow-server"],
    "Ports": [{ "IP": "0.0.0.0", "PrivatePort": 8080, "PublicPort": 18081, "Type": "tcp" }],
    "Labels": { "com.docker.compose.project": "crowserver" },
    "HostConfig": { "NetworkMode": "bridge" },
    "NetworkSettings": { "Networks": { "bridge": {
      "NetworkID": "1dd8d1a8", "IPAddress": "172.17.0.2" } } } }
]"#;
    let containers = parse_containers(data).unwrap();
    assert_eq!(containers.len(), 1);
    let c = &containers[0];
    assert_eq!(c.id, "90bc3b31");
    assert_eq!(c.names, vec!["/crow-server"]);
    assert_eq!(c.ports[0].private_port, 8080);
    assert_eq!(c.ports[0].public_port, 18081);
    assert_eq!(c.ports[0].kind, "tcp");
    assert_eq!(c.host_config.network_mode, "bridge");
    let n = &c.network_settings.networks["bridge"];
    assert_eq!(n.ip_address, "172.17.0.2");
    assert_eq!(n.network_id, "1dd8d1a8");
}

/// Port of `TestAddContainerLabels`, case 1 (NetworkMode != host, no ports):
/// fallback address uses the default port; network labels come from the
/// bridge network via NetworkID.
#[test]
fn container_no_ports_bridge_uses_default_port() {
    let c = crow_server("bridge", "host", vec![]);
    let groups = add_containers_labels(&[c], &bridge_labels(), 8012, "foobar", false, "s");
    let g = group_for(&groups, "172.17.0.2:8012");
    let l = &g.labels;
    assert_eq!(
        l["__meta_docker_container_id"],
        "90bc3b31aa13da5c0b11af2e228d54b38428a84e25d4e249ae9e9c95e51a0700"
    );
    assert_eq!(l["__meta_docker_container_name"], "/crow-server");
    assert_eq!(l["__meta_docker_container_network_mode"], "bridge");
    assert_eq!(
        l["__meta_docker_container_label_com_docker_compose_service"],
        "crow-server"
    );
    assert_eq!(l["__meta_docker_network_ip"], "172.17.0.2");
    assert_eq!(l["__meta_docker_network_name"], "bridge");
    assert_eq!(l["__meta_docker_network_scope"], "local");
    assert_eq!(l["__meta_docker_network_ingress"], "false");
    assert_eq!(l["__meta_docker_network_internal"], "false");
    assert!(!l.contains_key("__address__"));
}

/// Port of `TestAddContainerLabels`, case 2 (NetworkMode == host, no ports):
/// the fallback address is the host-networking host.
#[test]
fn container_host_network_mode_uses_host_networking_host() {
    let c = crow_server("host", "host", vec![]);
    let groups = add_containers_labels(&[c], &bridge_labels(), 8012, "foobar", false, "s");
    let g = group_for(&groups, "foobar");
    assert_eq!(g.labels["__meta_docker_container_network_mode"], "host");
    assert_eq!(g.labels["__meta_docker_network_name"], "bridge");
}

/// Port of `TestAddContainerLabels`, case 3 (a published TCP port): address is
/// `ip:privatePort` and the public-port labels appear.
#[test]
fn container_with_published_port() {
    let c = crow_server(
        "bridge",
        "bridge",
        vec![ContainerPort {
            ip: "0.0.0.0".into(),
            private_port: 8080,
            public_port: 18081,
            kind: "tcp".into(),
        }],
    );
    let groups = add_containers_labels(&[c], &bridge_labels(), 8012, "foobar", false, "s");
    let g = group_for(&groups, "172.17.0.2:8080");
    let l = &g.labels;
    assert_eq!(l["__meta_docker_port_private"], "8080");
    assert_eq!(l["__meta_docker_port_public"], "18081");
    assert_eq!(l["__meta_docker_port_public_ip"], "0.0.0.0");
    assert_eq!(l["__meta_docker_network_name"], "bridge");
}

const MULTI_NETWORKS_JSON: &[u8] = br#"[
  { "Name": "dockersd_private", "Id": "e804771e55254a360fdb70dfdd78d3610fdde231b14ef2f837a00ac1eeb9e601",
    "Scope": "local", "Internal": false, "Ingress": false, "Labels": {} },
  { "Name": "dockersd_private1", "Id": "bfcf66a6b64f7d518f009e34290dc3f3c66a08164257ad1afc3bd31d75f656e8",
    "Scope": "local", "Internal": false, "Ingress": false, "Labels": {} }
]"#;

const MULTI_CONTAINER_JSON: &[u8] = br#"[
  { "Id": "f84b2a0cfaa58d9e70b0657e2b3c6f44f0e973de4163a871299b4acf127b224f",
    "Names": ["/dockersd_multi_networks"],
    "Ports": [{ "PrivatePort": 3306, "Type": "tcp" }, { "PrivatePort": 33060, "Type": "tcp" }],
    "Labels": { "com.docker.compose.service": "mysql" },
    "HostConfig": { "NetworkMode": "dockersd_private_none" },
    "NetworkSettings": { "Networks": {
      "dockersd_private": { "NetworkID": "e804771e55254a360fdb70dfdd78d3610fdde231b14ef2f837a00ac1eeb9e601", "IPAddress": "172.20.0.3" },
      "dockersd_private1": { "NetworkID": "bfcf66a6b64f7d518f009e34290dc3f3c66a08164257ad1afc3bd31d75f656e8", "IPAddress": "172.21.0.3" }
    } } }
]"#;

/// Port of `TestDockerMultiNetworkLabels`: `match_first_network = false`
/// yields a target per network per TCP port (4 total); `= true` keeps only the
/// lowest-named network (`dockersd_private`), dropping the `dockersd_private1`
/// targets (2 total).
#[test]
fn multi_network_match_first_network_toggle() {
    let networks = network_labels_by_id(&parse_networks(MULTI_NETWORKS_JSON).unwrap());
    let containers = parse_containers(MULTI_CONTAINER_JSON).unwrap();

    let all = add_containers_labels(&containers, &networks, 80, "localhost", false, "s");
    assert_eq!(all.len(), 4);
    for addr in [
        "172.20.0.3:3306",
        "172.20.0.3:33060",
        "172.21.0.3:3306",
        "172.21.0.3:33060",
    ] {
        let g = group_for(&all, addr);
        assert_eq!(
            g.labels["__meta_docker_container_network_mode"],
            "dockersd_private_none"
        );
    }
    assert_eq!(
        group_for(&all, "172.21.0.3:3306").labels["__meta_docker_network_name"],
        "dockersd_private1"
    );

    let first = add_containers_labels(&containers, &networks, 80, "localhost", true, "s");
    assert_eq!(first.len(), 2);
    assert!(first
        .iter()
        .all(|g| g.labels["__meta_docker_network_name"] == "dockersd_private"));
}

/// Port of `TestDockerLinkedNetworkSettings`: a container with
/// `NetworkMode: container:<id>` and no networks of its own inherits the
/// linked container's networks, keeping its OWN id/name/network_mode.
#[test]
fn linked_container_inherits_networks() {
    let network_json = br#"[
      { "Name": "dockersd_private", "Id": "e804771e55254a360fdb70dfdd78d3610fdde231b14ef2f837a00ac1eeb9e601",
        "Scope": "local", "Internal": false, "Ingress": false, "Labels": {} },
      { "Name": "dockersd_private1", "Id": "bfcf66a6b64f7d518f009e34290dc3f3c66a08164257ad1afc3bd31d75f656e8",
        "Scope": "local", "Internal": false, "Ingress": false, "Labels": {} }
    ]"#;
    let container_json = br#"[
      { "Id": "f9ade4b83199d6f83020b7c0bfd1e8281b19dbf9e6cef2cf89bc45c8f8d20fe8",
        "Names": ["/dockersd_mysql"],
        "Ports": [{ "PrivatePort": 3306, "Type": "tcp" }],
        "Labels": {},
        "HostConfig": { "NetworkMode": "dockersd_private" },
        "NetworkSettings": { "Networks": {
          "dockersd_private": { "NetworkID": "e804771e55254a360fdb70dfdd78d3610fdde231b14ef2f837a00ac1eeb9e601", "IPAddress": "172.20.0.2" }
        } } },
      { "Id": "59bf76e8816af98856b90dd619c91027145ca501043b1c51756d03b085882e06",
        "Names": ["/dockersd_mysql_exporter"],
        "Ports": [{ "PrivatePort": 9104, "Type": "tcp" }],
        "Labels": {},
        "HostConfig": { "NetworkMode": "container:f9ade4b83199d6f83020b7c0bfd1e8281b19dbf9e6cef2cf89bc45c8f8d20fe8" },
        "NetworkSettings": { "Networks": {} } }
    ]"#;
    let networks = network_labels_by_id(&parse_networks(network_json).unwrap());
    let containers = parse_containers(container_json).unwrap();
    let groups = add_containers_labels(&containers, &networks, 80, "localhost", false, "s");

    // The exporter target inherits the linked container's IP but keeps its own
    // id/name/network_mode.
    let g = group_for(&groups, "172.20.0.2:9104");
    assert_eq!(
        g.labels["__meta_docker_container_id"],
        "59bf76e8816af98856b90dd619c91027145ca501043b1c51756d03b085882e06"
    );
    assert_eq!(
        g.labels["__meta_docker_container_name"],
        "/dockersd_mysql_exporter"
    );
    assert_eq!(
        g.labels["__meta_docker_container_network_mode"],
        "container:f9ade4b83199d6f83020b7c0bfd1e8281b19dbf9e6cef2cf89bc45c8f8d20fe8"
    );
    assert_eq!(g.labels["__meta_docker_network_name"], "dockersd_private");
}

/// Hardening: two containers whose `container:<id>` network modes reference
/// each other (A -> B -> A), neither with any network of its own. Without a
/// cycle guard, `resolve_networks` would spin forever. It must instead
/// TERMINATE and yield a sane (here empty-network -> no target) result.
#[test]
fn cyclic_container_links_terminate() {
    let container_json = br#"[
      { "Id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "Names": ["/a"],
        "Ports": [{ "PrivatePort": 3306, "Type": "tcp" }],
        "Labels": {},
        "HostConfig": { "NetworkMode": "container:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" },
        "NetworkSettings": { "Networks": {} } },
      { "Id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "Names": ["/b"],
        "Ports": [{ "PrivatePort": 9104, "Type": "tcp" }],
        "Labels": {},
        "HostConfig": { "NetworkMode": "container:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
        "NetworkSettings": { "Networks": {} } }
    ]"#;
    let containers = parse_containers(container_json).unwrap();
    let empty: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    // The key assertion is simply that this call RETURNS (does not hang).
    let groups = add_containers_labels(&containers, &empty, 80, "localhost", false, "s");
    // Both containers resolve to no network, so no targets are produced.
    assert!(groups.is_empty());
}
