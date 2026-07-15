//! Docker Swarm service serde structs, their parser, and the two
//! `__meta_dockerswarm_service_*` label builders: [`add_services_labels`] for
//! the `services` role (with the network-label join and per-port fan-out) and
//! [`add_services_labels_for_task`] for the `tasks` role's service join.
//!
//! Port of `lib/promscrape/discovery/dockerswarm/services.go`'s `service`
//! structs, `parseServicesResponse`, `getServiceMode`, `addServicesLabels`,
//! and (from `tasks.go`) `addServicesLabelsForTask`.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scrape::config::ScrapeError;
use crate::scrape::kubernetes::labels::sanitize_label_name;

use super::network::{join_host_port, parse_cidr_ip};

/// One Docker Swarm service (`GET /services` array element). Port of
/// `services.go`'s `service`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Service {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "Spec")]
    pub spec: ServiceSpec,
    #[serde(rename = "UpdateStatus")]
    pub update_status: ServiceUpdateStatus,
    #[serde(rename = "Endpoint")]
    pub endpoint: ServiceEndpoint,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ServiceSpec {
    #[serde(rename = "Labels")]
    pub labels: BTreeMap<String, String>,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "TaskTemplate")]
    pub task_template: TaskTemplate,
    #[serde(rename = "Mode")]
    pub mode: ServiceSpecMode,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct TaskTemplate {
    #[serde(rename = "ContainerSpec")]
    pub container_spec: ContainerSpec,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ContainerSpec {
    #[serde(rename = "Hostname")]
    pub hostname: String,
    #[serde(rename = "Image")]
    pub image: String,
}

/// `Spec.Mode` — exactly one of `Global`/`Replicated` is present. Modeled as
/// arbitrary JSON so any (possibly empty) object counts as "present", matching
/// `getServiceMode`'s `!= nil` checks.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ServiceSpecMode {
    #[serde(rename = "Global")]
    pub global: Option<serde_json::Value>,
    #[serde(rename = "Replicated")]
    pub replicated: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ServiceUpdateStatus {
    #[serde(rename = "State")]
    pub state: String,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ServiceEndpoint {
    #[serde(rename = "Ports")]
    pub ports: Vec<PortConfig>,
    #[serde(rename = "VirtualIPs")]
    pub virtual_ips: Vec<VirtualIp>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct VirtualIp {
    #[serde(rename = "NetworkID")]
    pub network_id: String,
    #[serde(rename = "Addr")]
    pub addr: String,
}

/// A published port (shared by services and tasks). Port of `portConfig`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct PortConfig {
    #[serde(rename = "Protocol")]
    pub protocol: String,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "PublishMode")]
    pub publish_mode: String,
    #[serde(rename = "PublishedPort")]
    pub published_port: u16,
}

/// Parses a `GET /services` response body. Port of `parseServicesResponse`.
pub fn parse_services(data: &[u8]) -> Result<Vec<Service>, ScrapeError> {
    serde_json::from_slice(data).map_err(|e| ScrapeError {
        msg: format!("cannot parse services: {e}"),
    })
}

/// `"global"`, `"replicated"`, or `""`. Port of `getServiceMode`.
pub fn get_service_mode(svc: &Service) -> &'static str {
    if svc.spec.mode.global.is_some() {
        "global"
    } else if svc.spec.mode.replicated.is_some() {
        "replicated"
    } else {
        ""
    }
}

/// Builds the `services`-role target label maps: one per (virtual IP, tcp
/// port) — or one per virtual IP when the service exposes no tcp port — with
/// the common service labels and the joined network labels. Port of
/// `addServicesLabels`.
pub fn add_services_labels(
    services: &[Service],
    networks_labels: &BTreeMap<String, BTreeMap<String, String>>,
    port: u16,
) -> Vec<BTreeMap<String, String>> {
    let mut ms = Vec::new();
    for service in services {
        let common = service_common_labels(service);
        for vip in &service.endpoint.virtual_ips {
            // Skip services without a virtual address (usually host services).
            if vip.addr.is_empty() {
                continue;
            }
            let Some(ip) = parse_cidr_ip(&vip.addr) else {
                log::warn!(
                    "esmagent dockerswarm_sd: cannot parse {:?} as CIDR for service label",
                    vip.addr
                );
                continue;
            };
            let net_labels = networks_labels.get(&vip.network_id);
            let mut added = false;
            for ep in &service.endpoint.ports {
                if ep.protocol != "tcp" {
                    continue;
                }
                let mut m: BTreeMap<String, String> = BTreeMap::new();
                m.insert(
                    "__address__".to_string(),
                    join_host_port(&ip, ep.published_port),
                );
                m.insert(
                    "__meta_dockerswarm_service_endpoint_port_name".to_string(),
                    ep.name.clone(),
                );
                m.insert(
                    "__meta_dockerswarm_service_endpoint_port_publish_mode".to_string(),
                    ep.publish_mode.clone(),
                );
                merge(&mut m, &common);
                merge_opt(&mut m, net_labels);
                ms.push(m);
                added = true;
            }
            if !added {
                let mut m: BTreeMap<String, String> = BTreeMap::new();
                m.insert("__address__".to_string(), join_host_port(&ip, port));
                merge(&mut m, &common);
                merge_opt(&mut m, net_labels);
                ms.push(m);
            }
        }
    }
    ms
}

/// The service labels shared by every target of a service (`addServicesLabels`
/// `commonLabels`).
fn service_common_labels(service: &Service) -> BTreeMap<String, String> {
    let mut m: BTreeMap<String, String> = BTreeMap::new();
    m.insert(
        "__meta_dockerswarm_service_id".to_string(),
        service.id.clone(),
    );
    m.insert(
        "__meta_dockerswarm_service_name".to_string(),
        service.spec.name.clone(),
    );
    m.insert(
        "__meta_dockerswarm_service_mode".to_string(),
        get_service_mode(service).to_string(),
    );
    m.insert(
        "__meta_dockerswarm_service_task_container_hostname".to_string(),
        service.spec.task_template.container_spec.hostname.clone(),
    );
    m.insert(
        "__meta_dockerswarm_service_task_container_image".to_string(),
        service.spec.task_template.container_spec.image.clone(),
    );
    m.insert(
        "__meta_dockerswarm_service_updating_status".to_string(),
        service.update_status.state.clone(),
    );
    for (k, v) in &service.spec.labels {
        m.insert(
            sanitize_label_name(&format!("__meta_dockerswarm_service_label_{k}")),
            v.clone(),
        );
    }
    m
}

/// Builds the per-service label map used by the `tasks` role to join a task to
/// its service (only id/name/mode + service labels). Port of
/// `addServicesLabelsForTask`.
pub fn add_services_labels_for_task(services: &[Service]) -> Vec<BTreeMap<String, String>> {
    let mut ms = Vec::new();
    for svc in services {
        let mut m: BTreeMap<String, String> = BTreeMap::new();
        m.insert("__meta_dockerswarm_service_id".to_string(), svc.id.clone());
        m.insert(
            "__meta_dockerswarm_service_name".to_string(),
            svc.spec.name.clone(),
        );
        m.insert(
            "__meta_dockerswarm_service_mode".to_string(),
            get_service_mode(svc).to_string(),
        );
        for (k, v) in &svc.spec.labels {
            m.insert(
                sanitize_label_name(&format!("__meta_dockerswarm_service_label_{k}")),
                v.clone(),
            );
        }
        ms.push(m);
    }
    ms
}

/// Copies every entry of `src` into `dst` (last-write-wins, mirroring
/// upstream's `AddFrom` + `RemoveDuplicates`).
fn merge(dst: &mut BTreeMap<String, String>, src: &BTreeMap<String, String>) {
    for (k, v) in src {
        dst.insert(k.clone(), v.clone());
    }
}

/// [`merge`] for an optional source map (a missing network-label join is a
/// no-op).
fn merge_opt(dst: &mut BTreeMap<String, String>, src: Option<&BTreeMap<String, String>>) {
    if let Some(m) = src {
        merge(dst, m);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Port of upstream `services_test.go::TestParseServicesResponse`.
    #[test]
    fn parse_services_extracts_one() {
        let data = br#"[
  { "ID": "tgsci5gd31aai3jyudv98pqxf",
    "Spec": { "Name": "redis2", "Labels": {},
      "TaskTemplate": { "ContainerSpec": {
        "Image": "redis:3.0.6@sha256:6a692a76c2081888b589e26e6ec835743119fe453d67ecf03df7de5b73d69842" } },
      "Mode": { "Replicated": {} } },
    "Endpoint": {
      "Ports": [ { "Protocol": "tcp", "TargetPort": 6379, "PublishedPort": 8081, "PublishMode": "ingress" } ],
      "VirtualIPs": [ { "NetworkID": "qs0hog6ldlei9ct11pr3c77v1", "Addr": "10.0.0.3/24" } ] } }
]"#;
        let services = parse_services(data).unwrap();
        assert_eq!(services.len(), 1);
        let s = &services[0];
        assert_eq!(s.id, "tgsci5gd31aai3jyudv98pqxf");
        assert_eq!(s.spec.name, "redis2");
        assert_eq!(get_service_mode(s), "replicated");
        assert_eq!(s.endpoint.ports[0].published_port, 8081);
        assert_eq!(s.endpoint.ports[0].protocol, "tcp");
        assert_eq!(s.endpoint.virtual_ips[0].addr, "10.0.0.3/24");
        assert_eq!(
            s.endpoint.virtual_ips[0].network_id,
            "qs0hog6ldlei9ct11pr3c77v1"
        );
    }

    fn ingress_network_labels() -> BTreeMap<String, BTreeMap<String, String>> {
        BTreeMap::from([(
            "qs0hog6ldlei9ct11pr3c77v1".to_string(),
            BTreeMap::from([
                (
                    "__meta_dockerswarm_network_id".to_string(),
                    "qs0hog6ldlei9ct11pr3c77v1".to_string(),
                ),
                (
                    "__meta_dockerswarm_network_ingress".to_string(),
                    "true".to_string(),
                ),
                (
                    "__meta_dockerswarm_network_internal".to_string(),
                    "false".to_string(),
                ),
                (
                    "__meta_dockerswarm_network_label_key1".to_string(),
                    "value1".to_string(),
                ),
                (
                    "__meta_dockerswarm_network_name".to_string(),
                    "ingress".to_string(),
                ),
                (
                    "__meta_dockerswarm_network_scope".to_string(),
                    "swarm".to_string(),
                ),
            ]),
        )])
    }

    /// Port of upstream `services_test.go::TestAddServicesLabels`.
    #[test]
    fn add_services_labels_matches_upstream() {
        let services = vec![Service {
            id: "tgsci5gd31aai3jyudv98pqxf".into(),
            spec: ServiceSpec {
                labels: BTreeMap::new(),
                name: "redis2".into(),
                task_template: TaskTemplate {
                    container_spec: ContainerSpec {
                        hostname: "node1".into(),
                        image: "redis:3.0.6@sha256:6a692a76c2081888b589e26e6ec835743119fe453d67ecf03df7de5b73d69842".into(),
                    },
                },
                mode: ServiceSpecMode {
                    replicated: Some(serde_json::json!({})),
                    global: None,
                },
            },
            update_status: ServiceUpdateStatus::default(),
            endpoint: ServiceEndpoint {
                ports: vec![PortConfig {
                    protocol: "tcp".into(),
                    name: "redis".into(),
                    publish_mode: "ingress".into(),
                    published_port: 0,
                }],
                virtual_ips: vec![VirtualIp {
                    network_id: "qs0hog6ldlei9ct11pr3c77v1".into(),
                    addr: "10.0.0.3/24".into(),
                }],
            },
        }];
        let ms = add_services_labels(&services, &ingress_network_labels(), 9100);
        assert_eq!(ms.len(), 1);
        let expected = BTreeMap::from([
            ("__address__", "10.0.0.3:0"),
            ("__meta_dockerswarm_network_id", "qs0hog6ldlei9ct11pr3c77v1"),
            ("__meta_dockerswarm_network_ingress", "true"),
            ("__meta_dockerswarm_network_internal", "false"),
            ("__meta_dockerswarm_network_label_key1", "value1"),
            ("__meta_dockerswarm_network_name", "ingress"),
            ("__meta_dockerswarm_network_scope", "swarm"),
            ("__meta_dockerswarm_service_endpoint_port_name", "redis"),
            (
                "__meta_dockerswarm_service_endpoint_port_publish_mode",
                "ingress",
            ),
            ("__meta_dockerswarm_service_id", "tgsci5gd31aai3jyudv98pqxf"),
            ("__meta_dockerswarm_service_mode", "replicated"),
            ("__meta_dockerswarm_service_name", "redis2"),
            ("__meta_dockerswarm_service_task_container_hostname", "node1"),
            (
                "__meta_dockerswarm_service_task_container_image",
                "redis:3.0.6@sha256:6a692a76c2081888b589e26e6ec835743119fe453d67ecf03df7de5b73d69842",
            ),
            ("__meta_dockerswarm_service_updating_status", ""),
        ]);
        let got: BTreeMap<&str, &str> = ms[0]
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(got, expected);
    }
}
