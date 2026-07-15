//! Nomad `Service` serde structs, the service-list parser
//! ([`parse_services`] / [`parse_service_names`]), and the `__meta_nomad_*`
//! label builder ([`append_target_labels`]).
//!
//! Port of `lib/promscrape/discovery/nomad/service.go` (v1.146.0)
//! (`ServiceList`/`service`/`Service`, `parseServices`, `appendTargetLabels`)
//! plus `discoveryutil.AddTagsToLabels` / `JoinHostPort`
//! (`lib/promscrape/discoveryutil/util.go`), reshaped for this crate's
//! [`TargetGroup`] shape (one group per Nomad service registration: the
//! registration's `__address__` is the group's single target, and the
//! `__meta_nomad_*` set becomes the group's `labels`).
//!
//! Upstream includes `__address__` in the returned label set because a
//! Prometheus label set *is* the target; this crate's [`TargetGroup`]
//! carries the address separately in `targets`, so [`append_target_labels`]
//! puts it there and leaves it out of `labels` — `target::build_targets`
//! seeds `__address__` from the target string and overlays `labels`, so the
//! resulting relabel input is identical.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::sanitize_label_name;

/// One entry of the `/v1/services` listing. Port of `service.go`'s
/// `ServiceList` (`{ "Namespace": ..., "Services": [{ "ServiceName": ...,
/// "Tags": [...] }] }`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ServiceList {
    #[serde(rename = "Namespace")]
    pub namespace: String,
    #[serde(rename = "Services")]
    pub services: Vec<ServiceStub>,
}

/// The `Services` element of a [`ServiceList`]. Port of `service.go`'s
/// (unexported) `service` struct.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ServiceStub {
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "Tags")]
    pub tags: Vec<String>,
}

/// One Nomad service registration returned by `/v1/service/<name>`. Port of
/// `service.go`'s `Service`. Nomad serializes these fields with mixed
/// capitalization (`ID`, `NodeID`, `JobID`, `AllocID`), so each `rename` is
/// spelled out rather than using a blanket `rename_all`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Service {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "Namespace")]
    pub namespace: String,
    #[serde(rename = "NodeID")]
    pub node_id: String,
    #[serde(rename = "Datacenter")]
    pub datacenter: String,
    #[serde(rename = "JobID")]
    pub job_id: String,
    #[serde(rename = "AllocID")]
    pub alloc_id: String,
    #[serde(rename = "Tags")]
    pub tags: Vec<String>,
    #[serde(rename = "Address")]
    pub address: String,
    #[serde(rename = "Port")]
    pub port: i64,
}

/// Parses a `/v1/service/<name>` response body into a list of [`Service`].
/// Port of `parseServices`.
pub fn parse_services(data: &[u8]) -> Result<Vec<Service>, String> {
    serde_json::from_slice(data).map_err(|e| format!("cannot unmarshal Services: {e}"))
}

/// Parses a `/v1/services` response body into the flat list of service names
/// (across every namespace's [`ServiceList`]). Port of
/// `getBlockingServiceNames`'s unmarshal + flatten.
pub fn parse_service_names(data: &[u8]) -> Result<Vec<String>, String> {
    let lists: Vec<ServiceList> =
        serde_json::from_slice(data).map_err(|e| format!("cannot unmarshal ServiceList: {e}"))?;
    let mut names = Vec::new();
    for list in lists {
        for s in list.services {
            names.push(s.service_name);
        }
    }
    Ok(names)
}

/// Builds the [`TargetGroup`] for one Nomad service registration, mirroring
/// `appendTargetLabels`. `__address__` is `Address:Port` and is carried in
/// the group's `targets`; every `__meta_nomad_*` label goes in `labels`.
/// `source` is threaded through unchanged so the reconcile diff stays stable
/// across refreshes.
pub fn append_target_labels(svc: &Service, tag_separator: &str, source: String) -> TargetGroup {
    let address = join_host_port(&svc.address, svc.port);

    let mut m: BTreeMap<String, String> = BTreeMap::new();
    m.insert("__meta_nomad_address".into(), svc.address.clone());
    m.insert("__meta_nomad_dc".into(), svc.datacenter.clone());
    m.insert("__meta_nomad_namespace".into(), svc.namespace.clone());
    m.insert("__meta_nomad_node_id".into(), svc.node_id.clone());
    m.insert("__meta_nomad_service".into(), svc.service_name.clone());
    m.insert("__meta_nomad_service_address".into(), svc.address.clone());
    m.insert("__meta_nomad_service_alloc_id".into(), svc.alloc_id.clone());
    m.insert("__meta_nomad_service_id".into(), svc.id.clone());
    m.insert("__meta_nomad_service_job_id".into(), svc.job_id.clone());
    m.insert("__meta_nomad_service_port".into(), svc.port.to_string());

    add_tags_to_labels(&mut m, &svc.tags, "__meta_nomad_", tag_separator);

    TargetGroup {
        targets: vec![address],
        labels: m,
        source,
    }
}

/// Port of `discoveryutil.AddTagsToLabels`: adds `<prefix>tags` (the tag
/// list joined and surrounded by `tag_separator`) plus, per tag,
/// `<prefix>tag_<k>`=`<v>` and `<prefix>tagpresent_<k>`=`true` (a `k=v` tag
/// splits on the first `=`; a bare tag has an empty value). Both per-tag
/// names are sanitized. Identical to the Consul port's helper.
fn add_tags_to_labels(
    m: &mut BTreeMap<String, String>,
    tags: &[String],
    prefix: &str,
    tag_separator: &str,
) {
    m.insert(
        format!("{prefix}tags"),
        format!("{tag_separator}{}{tag_separator}", tags.join(tag_separator)),
    );
    for tag in tags {
        let (k, v) = match tag.find('=') {
            Some(n) => (&tag[..n], &tag[n + 1..]),
            None => (tag.as_str(), ""),
        };
        m.insert(
            sanitize_label_name(&format!("{prefix}tag_{k}")),
            v.to_string(),
        );
        m.insert(
            sanitize_label_name(&format!("{prefix}tagpresent_{k}")),
            "true".to_string(),
        );
    }
}

/// `host:port`, bracketing `host` when it is an IPv6 address (contains a
/// `:`). Port of `discoveryutil.JoinHostPort`.
fn join_host_port(host: &str, port: i64) -> String {
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
    fn parse_services_failure() {
        for s in ["", "[1,23]", r#"{"items":[{"metadata":1}]}"#] {
            assert!(
                parse_services(s.as_bytes()).is_err(),
                "expected err for {s:?}"
            );
        }
    }

    /// The exact fixture and expected label set from upstream
    /// `service_test.go`'s `TestParseServiceNodesSuccess`.
    #[test]
    fn parse_service_and_append_labels_matches_upstream() {
        let data = br#"
        [
            {
                "ID": "_nomad-task-1a321d90-79b5-681f-e6fa-8a43c8ec6b69-web-doggo-web-http",
                "ServiceName": "doggo-web",
                "Namespace": "default",
                "NodeID": "9e02c85b-db59-45f1-ddee-40d0317bd33d",
                "Datacenter": "dc1",
                "JobID": "doggo",
                "AllocID": "1a321d90-79b5-681f-e6fa-8a43c8ec6b69",
                "Tags": ["doggo", "web"],
                "Address": "192.168.29.76",
                "Port": 23761,
                "CreateIndex": 402,
                "ModifyIndex": 402
            }
        ]"#;
        let svcs = parse_services(data).unwrap();
        assert_eq!(svcs.len(), 1);
        let g = append_target_labels(&svcs[0], ",", "job/nomad/doggo-web".into());

        // __address__ is the target, not a label.
        assert_eq!(g.targets, vec!["192.168.29.76:23761".to_string()]);
        assert!(!g.labels.contains_key("__address__"));
        assert_eq!(g.source, "job/nomad/doggo-web");

        let l = &g.labels;
        assert_eq!(l["__meta_nomad_dc"], "dc1");
        assert_eq!(
            l["__meta_nomad_node_id"],
            "9e02c85b-db59-45f1-ddee-40d0317bd33d"
        );
        assert_eq!(l["__meta_nomad_address"], "192.168.29.76");
        assert_eq!(l["__meta_nomad_namespace"], "default");
        assert_eq!(l["__meta_nomad_service"], "doggo-web");
        assert_eq!(l["__meta_nomad_service_address"], "192.168.29.76");
        assert_eq!(
            l["__meta_nomad_service_alloc_id"],
            "1a321d90-79b5-681f-e6fa-8a43c8ec6b69"
        );
        assert_eq!(
            l["__meta_nomad_service_id"],
            "_nomad-task-1a321d90-79b5-681f-e6fa-8a43c8ec6b69-web-doggo-web-http"
        );
        assert_eq!(l["__meta_nomad_service_job_id"], "doggo");
        assert_eq!(l["__meta_nomad_service_port"], "23761");
        assert_eq!(l["__meta_nomad_tag_doggo"], "");
        assert_eq!(l["__meta_nomad_tag_web"], "");
        assert_eq!(l["__meta_nomad_tagpresent_doggo"], "true");
        assert_eq!(l["__meta_nomad_tagpresent_web"], "true");
        assert_eq!(l["__meta_nomad_tags"], ",doggo,web,");
    }

    #[test]
    fn parse_service_names_flattens_lists() {
        let data = br#"
        [
            {"Namespace":"default","Services":[
                {"ServiceName":"web","Tags":["prod"]},
                {"ServiceName":"db","Tags":[]}
            ]}
        ]"#;
        let mut names = parse_service_names(data).unwrap();
        names.sort();
        assert_eq!(names, vec!["db".to_string(), "web".to_string()]);
    }

    #[test]
    fn ipv6_host_is_bracketed() {
        assert_eq!(join_host_port("::1", 80), "[::1]:80");
        assert_eq!(join_host_port("10.0.0.1", 80), "10.0.0.1:80");
    }

    #[test]
    fn tag_name_is_sanitized() {
        let svc = Service {
            tags: vec!["app.kubernetes.io/name=web".into()],
            ..Service::default()
        };
        let g = append_target_labels(&svc, ",", "src".into());
        assert_eq!(g.labels["__meta_nomad_tag_app_kubernetes_io_name"], "web");
        assert_eq!(
            g.labels["__meta_nomad_tagpresent_app_kubernetes_io_name"],
            "true"
        );
    }
}
