//! Marathon `App`/`Task`/`PortMapping`/`PortDefinition` serde structs, the
//! `/v2/apps` response parser ([`parse_app_list`]), and the
//! `__meta_marathon_*` label builder ([`append_apps_labels`] /
//! [`append_app_target_labels`]).
//!
//! Port of `lib/promscrape/discovery/marathon/apps.go` (`AppList`/`app`/
//! `task`/`container`/`portMapping`/`portDefinition`/`network`) and
//! `marathon.go`'s `getAppsLabels`/`getAppLabels`/`targetEndpoint`/
//! `extractPortMapping` (v1.146.0), reshaped for this crate's [`TargetGroup`]
//! shape.
//!
//! Upstream builds one label set *per task*, then iterates that task's ports
//! adding (dedup-dependent) `__address__` labels to the same set. This crate's
//! [`TargetGroup`] carries the address in `targets` (separate from `labels`),
//! so [`append_app_target_labels`] emits **one [`TargetGroup`] per (task,
//! port)** — each with the single `__address__` in `targets` and that port's
//! `__meta_marathon_task`/`__meta_marathon_port_index`/port-label set in
//! `labels`. For the common one-port-per-task case this is identical to
//! upstream's output (see the `marathon_test.go`-derived unit test).

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer};

use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::sanitize_label_name;

/// Deserialize helper mirroring Go's JSON handling of `null` for slices/maps/
/// nested objects: a JSON `null` (and a missing key) both yield `T::default()`
/// rather than erroring. Marathon sends `null` for absent arrays and a `null`
/// `container` (see `app_test.go`/`marathon_test.go` fixtures).
fn null_default<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

/// Port of `apps.go`'s `AppList`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct AppList {
    #[serde(deserialize_with = "null_default")]
    pub apps: Vec<App>,
}

/// Port of `apps.go`'s `app`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct App {
    pub id: String,
    #[serde(deserialize_with = "null_default")]
    pub tasks: Vec<Task>,
    #[serde(deserialize_with = "null_default")]
    pub labels: BTreeMap<String, String>,
    #[serde(deserialize_with = "null_default")]
    pub container: Container,
    #[serde(deserialize_with = "null_default")]
    pub port_definitions: Vec<PortDefinition>,
    #[serde(deserialize_with = "null_default")]
    pub networks: Vec<Network>,
    pub require_ports: bool,
}

/// Port of `apps.go`'s `task`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Task {
    pub id: String,
    pub host: String,
    #[serde(deserialize_with = "null_default")]
    pub ports: Vec<u32>,
    #[serde(deserialize_with = "null_default")]
    pub ip_addresses: Vec<IpAddr>,
}

/// Port of `apps.go`'s `ipAddr`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct IpAddr {
    pub ip_address: String,
    pub protocol: String,
}

/// Port of `apps.go`'s `container`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Container {
    #[serde(deserialize_with = "null_default")]
    pub docker: DockerContainer,
    #[serde(deserialize_with = "null_default")]
    pub port_mappings: Vec<PortMapping>,
}

/// Port of `apps.go`'s `dockerContainer`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct DockerContainer {
    pub image: String,
    #[serde(deserialize_with = "null_default")]
    pub port_mappings: Vec<PortMapping>,
}

/// Port of `apps.go`'s `portMapping`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PortMapping {
    #[serde(deserialize_with = "null_default")]
    pub labels: BTreeMap<String, String>,
    pub container_port: u32,
    pub host_port: u32,
    pub service_port: u32,
}

/// Port of `apps.go`'s `portDefinition`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PortDefinition {
    #[serde(deserialize_with = "null_default")]
    pub labels: BTreeMap<String, String>,
    pub port: u32,
}

/// Port of `apps.go`'s `network`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Network {
    pub name: String,
    pub mode: String,
}

impl App {
    /// Port of `apps.go`'s `(app).isContainerNet`: the app's first network is
    /// in `container` mode.
    fn is_container_net(&self) -> bool {
        self.networks.first().map(|n| n.mode.as_str()) == Some("container")
    }
}

/// Parses a `/v2/apps` response body into an [`AppList`]. Port of
/// `GetAppsList`'s `json.Unmarshal`.
pub fn parse_app_list(data: &[u8]) -> Result<AppList, String> {
    serde_json::from_slice(data).map_err(|e| format!("cannot unmarshal AppList: {e}"))
}

/// Builds the [`TargetGroup`]s for every app. Port of `getAppsLabels`.
pub fn append_apps_labels(apps: &AppList, source: &str) -> Vec<TargetGroup> {
    let mut groups = Vec::new();
    for app in &apps.apps {
        groups.extend(append_app_target_labels(app, source));
    }
    groups
}

/// The `__meta_marathon_port_mapping_label_` prefix (used for both
/// `container.portMappings` and the legacy `container.docker.portMappings`).
const PORT_MAPPING_LABEL_PREFIX: &str = "__meta_marathon_port_mapping_label_";
/// The `__meta_marathon_port_definition_label_` prefix.
const PORT_DEFINITION_LABEL_PREFIX: &str = "__meta_marathon_port_definition_label_";

/// Builds one [`TargetGroup`] per (task, port) for a single app, mirroring
/// `getAppLabels`. The `__address__` goes in the group's `targets`; the
/// `__meta_marathon_*` set goes in `labels`. `source` is threaded through
/// unchanged so the reconcile diff stays stable across refreshes.
pub fn append_app_target_labels(app: &App, source: &str) -> Vec<TargetGroup> {
    let container_net = app.is_container_net();

    // Base labels shared by every target of this app.
    let mut base: BTreeMap<String, String> = BTreeMap::new();
    base.insert("__meta_marathon_app".into(), app.id.clone());
    base.insert(
        "__meta_marathon_image".into(),
        app.container.docker.image.clone(),
    );
    for (ln, lv) in &app.labels {
        base.insert(
            format!("__meta_marathon_app_label_{}", sanitize_label_name(ln)),
            lv.clone(),
        );
    }

    // Determine the port list, the per-port Marathon labels, and the label
    // prefix, matching `getAppLabels`'s switch on where ports come from.
    let (mut ports, port_labels, prefix) = resolve_ports(app, container_net);

    let mut groups = Vec::new();
    for task in &app.tasks {
        // Host-networked apps expose ports only on the task; adopt them when we
        // gathered none above (upstream mutates `ports` here, so the value
        // carries to later tasks — replicated).
        if ports.is_empty() && !task.ports.is_empty() {
            ports = task.ports.clone();
        }

        for (i, &raw_port) in ports.iter().enumerate() {
            // A zero port is auto-generated by Mesos and must be read back from
            // the task's `ports` array.
            let mut port = raw_port;
            if port == 0 && task.ports.len() == ports.len() {
                port = task.ports[i];
            }

            let address = target_endpoint(task, port, container_net);
            let mut labels = base.clone();
            labels.insert("__meta_marathon_task".into(), task.id.clone());
            labels.insert("__meta_marathon_port_index".into(), i.to_string());
            if !port_labels.is_empty() {
                for (ln, lv) in &port_labels[i] {
                    labels.insert(format!("{prefix}{}", sanitize_label_name(ln)), lv.clone());
                }
            }

            groups.push(TargetGroup {
                targets: vec![address],
                labels,
                source: source.to_string(),
            });
        }
    }

    groups
}

/// Resolves `(ports, per-port labels, label prefix)` for an app, mirroring the
/// `switch` in `getAppLabels`. Returns empty vecs + an empty prefix when the
/// app defines no ports (host networking with task-only `ports`).
fn resolve_ports(
    app: &App,
    container_net: bool,
) -> (Vec<u32>, Vec<BTreeMap<String, String>>, &'static str) {
    if !app.container.port_mappings.is_empty() {
        let (ports, labels) = extract_port_mapping(&app.container.port_mappings, container_net);
        (ports, labels, PORT_MAPPING_LABEL_PREFIX)
    } else if !app.container.docker.port_mappings.is_empty() {
        let (ports, labels) =
            extract_port_mapping(&app.container.docker.port_mappings, container_net);
        (ports, labels, PORT_MAPPING_LABEL_PREFIX)
    } else if !app.port_definitions.is_empty() {
        let mut ports = vec![0u32; app.port_definitions.len()];
        let mut labels = Vec::with_capacity(app.port_definitions.len());
        for (i, pd) in app.port_definitions.iter().enumerate() {
            labels.push(pd.labels.clone());
            // When requirePorts is false this is the servicePort, not the
            // listen port, and must be read from the task instead.
            if app.require_ports {
                ports[i] = pd.port;
            }
        }
        (ports, labels, PORT_DEFINITION_LABEL_PREFIX)
    } else {
        (Vec::new(), Vec::new(), "")
    }
}

/// Port of `extractPortMapping`: the per-mapping labels plus, per mapping, the
/// container port (container networking) or the host port (otherwise).
fn extract_port_mapping(
    port_mappings: &[PortMapping],
    container_net: bool,
) -> (Vec<u32>, Vec<BTreeMap<String, String>>) {
    let mut ports = Vec::with_capacity(port_mappings.len());
    let mut labels = Vec::with_capacity(port_mappings.len());
    for pm in port_mappings {
        labels.push(pm.labels.clone());
        ports.push(if container_net {
            pm.container_port
        } else {
            pm.host_port
        });
    }
    (ports, labels)
}

/// Port of `targetEndpoint`: `host:port` where `host` is the task's first
/// container IP under container networking, else the task host. IPv6 hosts are
/// bracketed (`net.JoinHostPort` semantics).
fn target_endpoint(task: &Task, port: u32, container_net: bool) -> String {
    let host = if container_net && !task.ip_addresses.is_empty() {
        task.ip_addresses[0].ip_address.as_str()
    } else {
        task.host.as_str()
    };
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
#[path = "labels_tests.rs"]
mod tests;
