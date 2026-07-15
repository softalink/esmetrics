//! Docker Swarm task serde structs, their parser, and the
//! `__meta_dockerswarm_task_*`/`_container_*` target-label builder
//! ([`add_tasks_labels`]) with its node/service/network cross-endpoint joins.
//!
//! Port of `lib/promscrape/discovery/dockerswarm/tasks.go`'s `task` structs,
//! `parseTasks`, `addTasksLabels`, and the `addLabels` join helper. The
//! `services` join uses [`super::services::add_services_labels_for_task`]; the
//! `nodes` join uses [`super::nodes::add_node_labels`]; the `networks` join
//! uses [`super::network::network_labels_by_id`].

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scrape::config::ScrapeError;
use crate::scrape::kubernetes::labels::sanitize_label_name;

use super::network::{join_host_port, parse_cidr_ip, Network};
use super::services::{PortConfig, Service};

/// One Docker Swarm task (`GET /tasks` array element). Port of `tasks.go`'s
/// `task`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Task {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "ServiceID")]
    pub service_id: String,
    #[serde(rename = "NodeID")]
    pub node_id: String,
    #[serde(rename = "DesiredState")]
    pub desired_state: String,
    #[serde(rename = "NetworksAttachments")]
    pub networks_attachments: Vec<NetworkAttachment>,
    #[serde(rename = "Status")]
    pub status: TaskStatus,
    #[serde(rename = "Spec")]
    pub spec: TaskSpec,
    #[serde(rename = "Slot")]
    pub slot: i64,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct NetworkAttachment {
    #[serde(rename = "Addresses")]
    pub addresses: Vec<String>,
    #[serde(rename = "Network")]
    pub network: Network,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct TaskStatus {
    #[serde(rename = "State")]
    pub state: String,
    #[serde(rename = "ContainerStatus")]
    pub container_status: ContainerStatus,
    #[serde(rename = "PortStatus")]
    pub port_status: PortStatus,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ContainerStatus {
    #[serde(rename = "ContainerID")]
    pub container_id: String,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct PortStatus {
    #[serde(rename = "Ports")]
    pub ports: Vec<PortConfig>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct TaskSpec {
    #[serde(rename = "ContainerSpec")]
    pub container_spec: TaskContainerSpec,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct TaskContainerSpec {
    #[serde(rename = "Labels")]
    pub labels: BTreeMap<String, String>,
}

/// Parses a `GET /tasks` response body. Port of `parseTasks`.
pub fn parse_tasks(data: &[u8]) -> Result<Vec<Task>, ScrapeError> {
    serde_json::from_slice(data).map_err(|e| ScrapeError {
        msg: format!("cannot parse tasks: {e}"),
    })
}

/// Builds the `tasks`-role target label maps, fanning each task out over its
/// container-status ports and its network attachments and joining the node,
/// service, and network labels. Port of `addTasksLabels`.
///
/// `nodes_labels`/`services_labels` are the already-built label maps from
/// [`super::nodes::add_node_labels`] /
/// [`super::services::add_services_labels_for_task`]; `networks_labels` the
/// ID-keyed map from [`super::network::network_labels_by_id`]; `services` the
/// raw services (for the per-service published-port lookup); `port` the config
/// default port used when a network attachment has no matching service port.
pub fn add_tasks_labels(
    tasks: &[Task],
    nodes_labels: &[BTreeMap<String, String>],
    services_labels: &[BTreeMap<String, String>],
    networks_labels: &BTreeMap<String, BTreeMap<String, String>>,
    services: &[Service],
    port: u16,
) -> Vec<BTreeMap<String, String>> {
    let mut ms = Vec::new();
    for task in tasks {
        let mut common: BTreeMap<String, String> = BTreeMap::new();
        common.insert("__meta_dockerswarm_task_id".to_string(), task.id.clone());
        common.insert(
            "__meta_dockerswarm_task_container_id".to_string(),
            task.status.container_status.container_id.clone(),
        );
        common.insert(
            "__meta_dockerswarm_task_desired_state".to_string(),
            task.desired_state.clone(),
        );
        common.insert(
            "__meta_dockerswarm_task_slot".to_string(),
            task.slot.to_string(),
        );
        common.insert(
            "__meta_dockerswarm_task_state".to_string(),
            task.status.state.clone(),
        );
        for (k, v) in &task.spec.container_spec.labels {
            common.insert(
                sanitize_label_name(&format!("__meta_dockerswarm_container_label_{k}")),
                v.clone(),
            );
        }

        let svc_ports: &[PortConfig] = services
            .iter()
            .find(|s| s.id == task.service_id)
            .map(|s| s.endpoint.ports.as_slice())
            .unwrap_or(&[]);

        add_labels(
            &mut common,
            services_labels,
            "__meta_dockerswarm_service_id",
            &task.service_id,
        );
        add_labels(
            &mut common,
            nodes_labels,
            "__meta_dockerswarm_node_id",
            &task.node_id,
        );

        for p in &task.status.port_status.ports {
            if p.protocol != "tcp" {
                continue;
            }
            let mut m = common.clone();
            let node_addr = common
                .get("__meta_dockerswarm_node_address")
                .map(String::as_str)
                .unwrap_or("");
            m.insert(
                "__address__".to_string(),
                join_host_port(node_addr, p.published_port),
            );
            m.insert(
                "__meta_dockerswarm_task_port_publish_mode".to_string(),
                p.publish_mode.clone(),
            );
            ms.push(m);
        }

        for na in &task.networks_attachments {
            let net_labels = networks_labels.get(&na.network.id);
            for address in &na.addresses {
                let Some(ip) = parse_cidr_ip(address) else {
                    log::warn!(
                        "esmagent dockerswarm_sd: cannot parse task network attachment address {address:?} as CIDR"
                    );
                    continue;
                };
                let mut added = false;
                for ep in svc_ports {
                    if ep.protocol != "tcp" {
                        continue;
                    }
                    let mut m = common.clone();
                    merge_opt(&mut m, net_labels);
                    m.insert(
                        "__address__".to_string(),
                        join_host_port(&ip, ep.published_port),
                    );
                    m.insert(
                        "__meta_dockerswarm_task_port_publish_mode".to_string(),
                        ep.publish_mode.clone(),
                    );
                    ms.push(m);
                    added = true;
                }
                if !added {
                    let mut m = common.clone();
                    merge_opt(&mut m, net_labels);
                    m.insert("__address__".to_string(), join_host_port(&ip, port));
                    ms.push(m);
                }
            }
        }
    }
    ms
}

/// Copies every label of the first `src` map whose `key` equals `value` into
/// `dst`. Port of `tasks.go`'s `addLabels`.
fn add_labels(
    dst: &mut BTreeMap<String, String>,
    src: &[BTreeMap<String, String>],
    key: &str,
    value: &str,
) {
    for m in src {
        if m.get(key).map(String::as_str) != Some(value) {
            continue;
        }
        for (k, v) in m {
            dst.insert(k.clone(), v.clone());
        }
        return;
    }
}

/// Copies every entry of `src` into `dst` (last-write-wins) when present.
fn merge_opt(dst: &mut BTreeMap<String, String>, src: Option<&BTreeMap<String, String>>) {
    if let Some(m) = src {
        for (k, v) in m {
            dst.insert(k.clone(), v.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scrape::dockerswarm::services::{
        ServiceEndpoint, ServiceSpec, ServiceSpecMode, TaskTemplate, VirtualIp,
    };

    /// Port of upstream `tasks_test.go::TestParseTasks`.
    #[test]
    fn parse_tasks_extracts_one() {
        let data = br#"[
  { "ID": "t4rdm7j2y9yctbrksiwvsgpu5",
    "Spec": { "ContainerSpec": { "Labels": { "label1": "value1" } } },
    "ServiceID": "t91nf284wzle1ya09lqvyjgnq",
    "Slot": 1,
    "NodeID": "qauwmifceyvqs0sipvzu8oslu",
    "Status": { "State": "running",
      "ContainerStatus": { "ContainerID": "33034b69f6fa5f808098208752fd1fe4e0e1ca86311988cea6a73b998cdc62e8" },
      "PortStatus": {} },
    "DesiredState": "running" }
]"#;
        let tasks = parse_tasks(data).unwrap();
        assert_eq!(tasks.len(), 1);
        let t = &tasks[0];
        assert_eq!(t.id, "t4rdm7j2y9yctbrksiwvsgpu5");
        assert_eq!(t.service_id, "t91nf284wzle1ya09lqvyjgnq");
        assert_eq!(t.node_id, "qauwmifceyvqs0sipvzu8oslu");
        assert_eq!(t.slot, 1);
        assert_eq!(t.desired_state, "running");
        assert_eq!(t.status.state, "running");
        assert_eq!(
            t.status.container_status.container_id,
            "33034b69f6fa5f808098208752fd1fe4e0e1ca86311988cea6a73b998cdc62e8"
        );
        assert_eq!(t.spec.container_spec.labels["label1"], "value1");
    }

    fn node_labels_vector() -> Vec<BTreeMap<String, String>> {
        vec![to_owned([
            ("__address__", "172.31.40.97:9100"),
            ("__meta_dockerswarm_node_address", "172.31.40.97"),
            ("__meta_dockerswarm_node_availability", "active"),
            ("__meta_dockerswarm_node_engine_version", "19.03.11"),
            ("__meta_dockerswarm_node_hostname", "ip-172-31-40-97"),
            ("__meta_dockerswarm_node_id", "qauwmifceyvqs0sipvzu8oslu"),
            ("__meta_dockerswarm_node_platform_architecture", "x86_64"),
            ("__meta_dockerswarm_node_platform_os", "linux"),
            ("__meta_dockerswarm_node_role", "manager"),
            ("__meta_dockerswarm_node_status", "ready"),
        ])]
    }

    fn to_owned<const N: usize>(pairs: [(&str, &str); N]) -> BTreeMap<String, String> {
        pairs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn as_str_map(m: &BTreeMap<String, String>) -> BTreeMap<&str, &str> {
        m.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()
    }

    /// Port of `tasks_test.go::TestAddTasksLabels`, case 1: one task with node
    /// + service labels via a container-status port.
    #[test]
    fn add_tasks_labels_with_node_and_service() {
        let tasks = vec![Task {
            id: "t4rdm7j2y9yctbrksiwvsgpu5".into(),
            service_id: "t91nf284wzle1ya09lqvyjgnq".into(),
            node_id: "qauwmifceyvqs0sipvzu8oslu".into(),
            desired_state: "running".into(),
            slot: 1,
            status: TaskStatus {
                state: "running".into(),
                container_status: ContainerStatus {
                    container_id:
                        "33034b69f6fa5f808098208752fd1fe4e0e1ca86311988cea6a73b998cdc62e8".into(),
                },
                port_status: PortStatus {
                    ports: vec![PortConfig {
                        publish_mode: "ingress".into(),
                        name: "redis".into(),
                        protocol: "tcp".into(),
                        published_port: 6379,
                    }],
                },
            },
            ..Task::default()
        }];
        let svc_labels = vec![
            to_owned([
                ("__meta_dockerswarm_service_id", "t91nf284wzle1ya09lqvyjgnq"),
                ("__meta_dockerswarm_service_name", "real_service_name"),
                ("__meta_dockerswarm_service_mode", "real_service_mode"),
            ]),
            to_owned([
                ("__meta_dockerswarm_service_id", "fake_service_id"),
                ("__meta_dockerswarm_service_name", "fake_service_name"),
                ("__meta_dockerswarm_service_mode", "fake_service_mode"),
            ]),
        ];
        let ms = add_tasks_labels(
            &tasks,
            &node_labels_vector(),
            &svc_labels,
            &BTreeMap::new(),
            &[],
            9100,
        );
        assert_eq!(ms.len(), 1);
        let expected = to_owned([
            ("__address__", "172.31.40.97:6379"),
            ("__meta_dockerswarm_node_address", "172.31.40.97"),
            ("__meta_dockerswarm_node_availability", "active"),
            ("__meta_dockerswarm_node_engine_version", "19.03.11"),
            ("__meta_dockerswarm_node_hostname", "ip-172-31-40-97"),
            ("__meta_dockerswarm_node_id", "qauwmifceyvqs0sipvzu8oslu"),
            ("__meta_dockerswarm_node_platform_architecture", "x86_64"),
            ("__meta_dockerswarm_node_platform_os", "linux"),
            ("__meta_dockerswarm_node_role", "manager"),
            ("__meta_dockerswarm_node_status", "ready"),
            (
                "__meta_dockerswarm_task_container_id",
                "33034b69f6fa5f808098208752fd1fe4e0e1ca86311988cea6a73b998cdc62e8",
            ),
            ("__meta_dockerswarm_task_desired_state", "running"),
            ("__meta_dockerswarm_task_id", "t4rdm7j2y9yctbrksiwvsgpu5"),
            ("__meta_dockerswarm_task_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_task_slot", "1"),
            ("__meta_dockerswarm_task_state", "running"),
            ("__meta_dockerswarm_service_id", "t91nf284wzle1ya09lqvyjgnq"),
            ("__meta_dockerswarm_service_name", "real_service_name"),
            ("__meta_dockerswarm_service_mode", "real_service_mode"),
        ]);
        assert_eq!(as_str_map(&ms[0]), as_str_map(&expected));
    }

    /// Port of `tasks_test.go::TestAddTasksLabels`, case 2: one task with node,
    /// network, and service labels via a network attachment + service port.
    #[test]
    fn add_tasks_labels_with_node_network_and_service() {
        let tasks = vec![Task {
            id: "t4rdm7j2y9yctbrksiwvsgpu5".into(),
            service_id: "tgsci5gd31aai3jyudv98pqxf".into(),
            node_id: "qauwmifceyvqs0sipvzu8oslu".into(),
            desired_state: "running".into(),
            slot: 1,
            networks_attachments: vec![NetworkAttachment {
                network: Network {
                    id: "qs0hog6ldlei9ct11pr3c77v1".into(),
                    ..Network::default()
                },
                addresses: vec!["10.10.15.15/24".into()],
            }],
            status: TaskStatus {
                state: "running".into(),
                container_status: ContainerStatus {
                    container_id:
                        "33034b69f6fa5f808098208752fd1fe4e0e1ca86311988cea6a73b998cdc62e8".into(),
                },
                port_status: PortStatus::default(),
            },
            ..Task::default()
        }];
        let networks_labels = BTreeMap::from([(
            "qs0hog6ldlei9ct11pr3c77v1".to_string(),
            to_owned([
                ("__meta_dockerswarm_network_id", "qs0hog6ldlei9ct11pr3c77v1"),
                ("__meta_dockerswarm_network_ingress", "true"),
                ("__meta_dockerswarm_network_internal", "false"),
                ("__meta_dockerswarm_network_label_key1", "value1"),
                ("__meta_dockerswarm_network_name", "ingress"),
                ("__meta_dockerswarm_network_scope", "swarm"),
            ]),
        )]);
        let svc_labels = vec![
            to_owned([
                ("__meta_dockerswarm_service_id", "tgsci5gd31aai3jyudv98pqxf"),
                ("__meta_dockerswarm_service_name", "redis2"),
                ("__meta_dockerswarm_service_mode", "replicated"),
            ]),
            to_owned([
                ("__meta_dockerswarm_service_id", "fake_service_id"),
                ("__meta_dockerswarm_service_name", "fake_service_name"),
                ("__meta_dockerswarm_service_mode", "fake_service_mode"),
            ]),
        ];
        let services = vec![Service {
            id: "tgsci5gd31aai3jyudv98pqxf".into(),
            spec: ServiceSpec {
                labels: BTreeMap::new(),
                name: "redis2".into(),
                task_template: TaskTemplate::default(),
                mode: ServiceSpecMode {
                    replicated: Some(serde_json::json!({})),
                    global: None,
                },
            },
            endpoint: ServiceEndpoint {
                ports: vec![PortConfig {
                    protocol: "tcp".into(),
                    name: "redis".into(),
                    publish_mode: "ingress".into(),
                    published_port: 6379,
                }],
                virtual_ips: vec![VirtualIp {
                    network_id: "qs0hog6ldlei9ct11pr3c77v1".into(),
                    addr: "10.0.0.3/24".into(),
                }],
            },
            ..Service::default()
        }];
        let ms = add_tasks_labels(
            &tasks,
            &node_labels_vector(),
            &svc_labels,
            &networks_labels,
            &services,
            9100,
        );
        assert_eq!(ms.len(), 1);
        let expected = to_owned([
            ("__address__", "10.10.15.15:6379"),
            ("__meta_dockerswarm_network_id", "qs0hog6ldlei9ct11pr3c77v1"),
            ("__meta_dockerswarm_network_ingress", "true"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_label_key1", "value1"),
            ("__meta_dockerswarm_network_name", "ingress"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_node_address", "172.31.40.97"),
            ("__meta_dockerswarm_node_availability", "active"),
            ("__meta_dockerswarm_node_engine_version", "19.03.11"),
            ("__meta_dockerswarm_node_hostname", "ip-172-31-40-97"),
            ("__meta_dockerswarm_node_id", "qauwmifceyvqs0sipvzu8oslu"),
            ("__meta_dockerswarm_node_platform_architecture", "x86_64"),
            ("__meta_dockerswarm_node_platform_os", "linux"),
            ("__meta_dockerswarm_node_role", "manager"),
            ("__meta_dockerswarm_node_status", "ready"),
            (
                "__meta_dockerswarm_task_container_id",
                "33034b69f6fa5f808098208752fd1fe4e0e1ca86311988cea6a73b998cdc62e8",
            ),
            ("__meta_dockerswarm_task_desired_state", "running"),
            ("__meta_dockerswarm_task_id", "t4rdm7j2y9yctbrksiwvsgpu5"),
            ("__meta_dockerswarm_task_port_publish_mode", "ingress"),
            ("__meta_dockerswarm_task_slot", "1"),
            ("__meta_dockerswarm_task_state", "running"),
            ("__meta_dockerswarm_service_id", "tgsci5gd31aai3jyudv98pqxf"),
            ("__meta_dockerswarm_service_name", "redis2"),
            ("__meta_dockerswarm_service_mode", "replicated"),
        ]);
        assert_eq!(as_str_map(&ms[0]), as_str_map(&expected));
    }
}
