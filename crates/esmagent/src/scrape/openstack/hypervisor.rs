//! Nova `os-hypervisors/detail` JSON response structs (parsed with
//! `serde_json`) and the `__meta_openstack_*` label builder for
//! `role: hypervisor`.
//!
//! Port of `lib/promscrape/discovery/openstack/hypervisor.go`
//! (`hypervisorDetail`/`hypervisor` structs + `addHypervisorLabels`), reshaped
//! for this crate's [`TargetGroup`]: one group per hypervisor, whose single
//! `__address__` (its `host_ip` + configured port) is carried in `targets` and
//! whose `__meta_openstack_hypervisor_*` set is `labels`.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scrape::discovery::TargetGroup;

use super::join_host_port;

/// `os-hypervisors/detail` response. Port of `hypervisor.go`'s
/// `hypervisorDetail`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct HypervisorDetail {
    pub hypervisors: Vec<Hypervisor>,
    #[serde(rename = "hypervisors_links")]
    pub links: Vec<HypervisorLink>,
}

/// Port of `hypervisor.go`'s `hypervisorLink` (a paginated-next pointer).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct HypervisorLink {
    pub href: String,
    pub rel: String,
}

/// Port of `hypervisor.go`'s `hypervisor`, narrowed to the fields
/// `addHypervisorLabels` reads. `id` is a JSON integer in the Nova API.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Hypervisor {
    #[serde(rename = "host_ip")]
    pub host_ip: String,
    pub id: i64,
    #[serde(rename = "hypervisor_hostname")]
    pub hostname: String,
    pub status: String,
    pub state: String,
    #[serde(rename = "hypervisor_type")]
    pub type_: String,
}

/// Parses an `os-hypervisors/detail` JSON response. Port of
/// `parseHypervisorDetail`.
pub fn parse_hypervisor_detail(data: &[u8]) -> Result<HypervisorDetail, String> {
    serde_json::from_slice(data).map_err(|e| format!("cannot parse hypervisorDetail: {e}"))
}

/// Builds one [`TargetGroup`] per hypervisor, mirroring `addHypervisorLabels`.
/// `__address__` (the hypervisor's `host_ip` + `port`) is carried in `targets`;
/// every `__meta_openstack_hypervisor_*` label goes in `labels`. `source` is
/// threaded through unchanged.
pub fn add_hypervisor_labels(hvs: &[Hypervisor], port: u16, source: &str) -> Vec<TargetGroup> {
    let mut ms = Vec::new();
    for hv in hvs {
        let mut m: BTreeMap<String, String> = BTreeMap::new();
        m.insert("__meta_openstack_hypervisor_type".into(), hv.type_.clone());
        m.insert(
            "__meta_openstack_hypervisor_status".into(),
            hv.status.clone(),
        );
        m.insert(
            "__meta_openstack_hypervisor_hostname".into(),
            hv.hostname.clone(),
        );
        m.insert("__meta_openstack_hypervisor_state".into(), hv.state.clone());
        m.insert(
            "__meta_openstack_hypervisor_host_ip".into(),
            hv.host_ip.clone(),
        );
        m.insert("__meta_openstack_hypervisor_id".into(), hv.id.to_string());
        ms.push(TargetGroup {
            targets: vec![join_host_port(&hv.host_ip, port)],
            labels: m,
            source: source.to_string(),
        });
    }
    ms
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `os-hypervisors/detail` fixture from upstream `hypervisor_test.go`
    /// (`TestParseHypervisorDetail_Success`).
    const HV_JSON: &str = r#"{
    "hypervisors": [
        {
            "cpu_info": { "arch": "x86_64" },
            "status": "enabled",
            "state": "up",
            "host_ip": "1.1.1.1",
            "hypervisor_hostname": "host1",
            "hypervisor_type": "fake",
            "hypervisor_version": 1000,
            "id": 2,
            "vcpus": 2
        }
    ]}"#;

    #[test]
    fn parses_hypervisor_detail_fields() {
        let d = parse_hypervisor_detail(HV_JSON.as_bytes()).unwrap();
        assert_eq!(d.hypervisors.len(), 1);
        let h = &d.hypervisors[0];
        assert_eq!(h.host_ip, "1.1.1.1");
        assert_eq!(h.id, 2);
        assert_eq!(h.hostname, "host1");
        assert_eq!(h.status, "enabled");
        assert_eq!(h.state, "up");
        assert_eq!(h.type_, "fake");
    }

    #[test]
    fn parse_hypervisor_detail_rejects_bad_data() {
        assert!(parse_hypervisor_detail(b"{ff}").is_err());
    }

    /// Matches upstream `TestAddHypervisorLabels` (port 9100).
    #[test]
    fn builds_hypervisor_labels_matching_upstream_vector() {
        let hv = Hypervisor {
            type_: "fake".into(),
            id: 5,
            state: "enabled".into(),
            status: "up".into(),
            hostname: "fakehost".into(),
            host_ip: "1.2.2.2".into(),
        };
        let g = add_hypervisor_labels(&[hv], 9100, "src");
        assert_eq!(g.len(), 1);
        let t = &g[0];
        assert_eq!(t.targets, vec!["1.2.2.2:9100".to_string()]);
        assert!(!t.labels.contains_key("__address__"));
        let l = &t.labels;
        assert_eq!(l["__meta_openstack_hypervisor_host_ip"], "1.2.2.2");
        assert_eq!(l["__meta_openstack_hypervisor_hostname"], "fakehost");
        assert_eq!(l["__meta_openstack_hypervisor_id"], "5");
        assert_eq!(l["__meta_openstack_hypervisor_state"], "enabled");
        assert_eq!(l["__meta_openstack_hypervisor_status"], "up");
        assert_eq!(l["__meta_openstack_hypervisor_type"], "fake");
        assert_eq!(l.len(), 6, "labels={l:?}");
        assert_eq!(t.source, "src");
    }
}
