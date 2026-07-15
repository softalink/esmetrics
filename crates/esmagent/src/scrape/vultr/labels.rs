//! Vultr instance serde structs, the pagination-response parser, and the
//! `__meta_vultr_*` label builder ([`append_target_labels`]).
//!
//! Port of `lib/promscrape/discovery/vultr/instance.go`'s `Instance` /
//! `ListInstanceResponse` / `Meta` / `Links` structs and `vultr.go`'s
//! `getInstanceLabels`, reshaped for this crate's [`TargetGroup`] shape (one
//! group per instance: the instance's `__address__` is the group's single
//! target, and the `__meta_vultr_*` set becomes the group's `labels`).
//!
//! Upstream includes `__address__` in the returned label set because a
//! Prometheus label set *is* the target; this crate's [`TargetGroup`] carries
//! the address separately in `targets`, so [`append_target_labels`] puts it
//! there and leaves it out of `labels` — mirroring `scrape::digitalocean`.
//!
//! Like DigitalOcean (and unlike EC2/Consul), Vultr's feature/tag values go
//! into *comma-wrapped label values* (`,a,b,`), not into per-tag label *keys*,
//! so no `sanitize_label_name` is needed — every `__meta_vultr_*` key is a
//! fixed literal. Unlike DigitalOcean, no instance is ever skipped: every
//! instance yields exactly one target group.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scrape::config::ScrapeError;
use crate::scrape::discovery::TargetGroup;

/// One Vultr instance (VPS). Port of `instance.go`'s `Instance`
/// (`/v2/instances` array element). `#[serde(default)]` tolerates the many
/// response fields this port doesn't read.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Instance {
    pub id: String,
    pub os: String,
    pub ram: i64,
    pub disk: i64,
    pub main_ip: String,
    pub vcpu_count: i64,
    pub region: String,
    pub server_status: String,
    pub allowed_bandwidth: i64,
    pub v6_main_ip: String,
    pub hostname: String,
    pub label: String,
    pub internal_ip: String,
    pub os_id: i64,
    pub features: Vec<String>,
    pub plan: String,
    pub tags: Vec<String>,
}

/// `/v2/instances` list response. Port of `ListInstanceResponse`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ListInstanceResponse {
    pub instances: Vec<Instance>,
    pub meta: Meta,
}

/// The `meta` block of a [`ListInstanceResponse`]. Port of `Meta`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Meta {
    pub links: Links,
}

/// The `meta.links` block: the pagination cursor. Port of `Links`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Links {
    pub next: String,
}

/// Parses a `/v2/instances` response body. Port of the `json.Unmarshal` in
/// `getInstances`.
pub fn parse_api_response(data: &[u8]) -> Result<ListInstanceResponse, ScrapeError> {
    serde_json::from_slice(data).map_err(|e| ScrapeError {
        msg: format!("cannot unmarshal vultr ListInstanceResponse: {e}"),
    })
}

/// Builds one [`TargetGroup`] per instance, mirroring `getInstanceLabels`.
/// `__address__` is `main_ip:default_port` (carried in the group's `targets`);
/// every `__meta_vultr_*` label goes in `labels`. `source` is threaded through
/// unchanged so the reconcile diff stays stable across refreshes. No instance
/// is skipped (upstream adds every instance unconditionally).
pub fn append_target_labels(
    instances: &[Instance],
    default_port: u16,
    source: &str,
) -> Vec<TargetGroup> {
    let mut groups = Vec::with_capacity(instances.len());
    for instance in instances {
        let address = join_host_port(&instance.main_ip, default_port);

        let mut m: BTreeMap<String, String> = BTreeMap::new();
        m.insert(
            "__meta_vultr_instance_allowed_bandwidth_gb".into(),
            instance.allowed_bandwidth.to_string(),
        );
        m.insert(
            "__meta_vultr_instance_disk_gb".into(),
            instance.disk.to_string(),
        );
        m.insert(
            "__meta_vultr_instance_hostname".into(),
            instance.hostname.clone(),
        );
        m.insert("__meta_vultr_instance_id".into(), instance.id.clone());
        m.insert(
            "__meta_vultr_instance_internal_ip".into(),
            instance.internal_ip.clone(),
        );
        m.insert("__meta_vultr_instance_label".into(), instance.label.clone());
        m.insert(
            "__meta_vultr_instance_main_ip".into(),
            instance.main_ip.clone(),
        );
        m.insert(
            "__meta_vultr_instance_main_ipv6".into(),
            instance.v6_main_ip.clone(),
        );
        m.insert("__meta_vultr_instance_os".into(), instance.os.clone());
        m.insert(
            "__meta_vultr_instance_os_id".into(),
            instance.os_id.to_string(),
        );
        m.insert("__meta_vultr_instance_plan".into(), instance.plan.clone());
        m.insert(
            "__meta_vultr_instance_region".into(),
            instance.region.clone(),
        );
        m.insert(
            "__meta_vultr_instance_ram_mb".into(),
            instance.ram.to_string(),
        );
        m.insert(
            "__meta_vultr_instance_server_status".into(),
            instance.server_status.clone(),
        );
        m.insert(
            "__meta_vultr_instance_vcpu_count".into(),
            instance.vcpu_count.to_string(),
        );
        if !instance.features.is_empty() {
            m.insert(
                "__meta_vultr_instance_features".into(),
                join_strings(&instance.features),
            );
        }
        if !instance.tags.is_empty() {
            m.insert(
                "__meta_vultr_instance_tags".into(),
                join_strings(&instance.tags),
            );
        }

        groups.push(TargetGroup {
            targets: vec![address],
            labels: m,
            source: source.to_string(),
        });
    }
    groups
}

/// Port of `vultr.go`'s `joinStrings`: wraps a comma-joined list in leading and
/// trailing commas (`,a,b,`) so relabeling can match `,value,` exactly.
fn join_strings(a: &[String]) -> String {
    format!(",{},", a.join(","))
}

/// `host:port`, bracketing `host` when it is an IPv6 address (contains a
/// `:`). Port of `discoveryutil.JoinHostPort` (local copy, matching
/// `scrape::digitalocean::labels`).
fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Port of upstream `vultr_test.go::TestGetInstanceLabels`: one
    /// fully-populated instance must produce exactly the expected
    /// `__meta_vultr_*` set (features/tags comma-wrapped) and the
    /// `main_ip:port` `__address__`.
    #[test]
    fn get_instance_labels_matches_upstream() {
        let instance = Instance {
            id: "fake-id-07f7-4b68-88ac-fake-id".into(),
            os: "Ubuntu 22.04 x64".into(),
            ram: 1024,
            disk: 25,
            main_ip: "64.176.84.27".into(),
            vcpu_count: 1,
            region: "sgp".into(),
            plan: "vc2-1c-1gb".into(),
            allowed_bandwidth: 1,
            server_status: "installingbooting".into(),
            v6_main_ip: "2002:18f0:4100:263a:5300:07ff:fdd7:691c".into(),
            label: "vultr-sd".into(),
            internal_ip: String::new(),
            hostname: "vultr-sd".into(),
            tags: vec!["mock tags".into()],
            os_id: 1743,
            features: vec!["ipv6".into()],
        };

        let groups = append_target_labels(&[instance], 8080, "job/vultr");
        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert_eq!(g.targets, vec!["64.176.84.27:8080".to_string()]);
        assert_eq!(g.source, "job/vultr");
        let l = &g.labels;
        assert_eq!(
            l["__meta_vultr_instance_id"],
            "fake-id-07f7-4b68-88ac-fake-id"
        );
        assert_eq!(l["__meta_vultr_instance_label"], "vultr-sd");
        assert_eq!(l["__meta_vultr_instance_os"], "Ubuntu 22.04 x64");
        assert_eq!(l["__meta_vultr_instance_os_id"], "1743");
        assert_eq!(l["__meta_vultr_instance_region"], "sgp");
        assert_eq!(l["__meta_vultr_instance_plan"], "vc2-1c-1gb");
        assert_eq!(l["__meta_vultr_instance_main_ip"], "64.176.84.27");
        assert_eq!(l["__meta_vultr_instance_internal_ip"], "");
        assert_eq!(
            l["__meta_vultr_instance_main_ipv6"],
            "2002:18f0:4100:263a:5300:07ff:fdd7:691c"
        );
        assert_eq!(l["__meta_vultr_instance_hostname"], "vultr-sd");
        assert_eq!(
            l["__meta_vultr_instance_server_status"],
            "installingbooting"
        );
        assert_eq!(l["__meta_vultr_instance_vcpu_count"], "1");
        assert_eq!(l["__meta_vultr_instance_ram_mb"], "1024");
        assert_eq!(l["__meta_vultr_instance_allowed_bandwidth_gb"], "1");
        assert_eq!(l["__meta_vultr_instance_disk_gb"], "25");
        assert_eq!(l["__meta_vultr_instance_features"], ",ipv6,");
        assert_eq!(l["__meta_vultr_instance_tags"], ",mock tags,");
        // __address__ is the target, not a label.
        assert!(!l.contains_key("__address__"));
    }

    /// Absent features/tags omit their labels entirely (upstream only adds them
    /// when the slice is non-empty); every other label is still present.
    #[test]
    fn empty_features_and_tags_omit_labels() {
        let instance = Instance {
            id: "i-2".into(),
            main_ip: "1.2.3.4".into(),
            ..Instance::default()
        };
        let groups = append_target_labels(&[instance], 80, "s");
        assert_eq!(groups.len(), 1);
        let l = &groups[0].labels;
        assert!(!l.contains_key("__meta_vultr_instance_features"));
        assert!(!l.contains_key("__meta_vultr_instance_tags"));
        assert_eq!(l["__meta_vultr_instance_internal_ip"], "");
        assert_eq!(l["__meta_vultr_instance_ram_mb"], "0");
    }

    /// An IPv6 `main_ip` is bracketed in `__address__` but left bare in the
    /// `__meta_vultr_instance_main_ip` label.
    #[test]
    fn ipv6_main_ip_is_bracketed_in_address() {
        let instance = Instance {
            id: "i-3".into(),
            main_ip: "2001:db8::1".into(),
            ..Instance::default()
        };
        let groups = append_target_labels(&[instance], 9100, "s");
        assert_eq!(groups[0].targets, vec!["[2001:db8::1]:9100".to_string()]);
        assert_eq!(
            groups[0].labels["__meta_vultr_instance_main_ip"],
            "2001:db8::1"
        );
    }

    /// A two-instance `/v2/instances` body plus the `meta.links.next` cursor
    /// parse out of a real Vultr response shape.
    #[test]
    fn parse_api_response_extracts_instances_and_cursor() {
        let data = br#"
{
  "instances": [
    {
      "id": "abc",
      "main_ip": "64.176.84.27",
      "os": "Ubuntu 22.04 x64",
      "ram": 1024,
      "disk": 25,
      "vcpu_count": 1,
      "region": "sgp",
      "plan": "vc2-1c-1gb",
      "os_id": 1743,
      "features": ["ipv6"],
      "tags": ["web"]
    }
  ],
  "meta": {
    "total": 2,
    "links": { "next": "next-cursor-token", "prev": "" }
  }
}"#;
        let resp = parse_api_response(data).unwrap();
        assert_eq!(resp.instances.len(), 1);
        let i = &resp.instances[0];
        assert_eq!(i.id, "abc");
        assert_eq!(i.main_ip, "64.176.84.27");
        assert_eq!(i.ram, 1024);
        assert_eq!(i.os_id, 1743);
        assert_eq!(i.features, vec!["ipv6"]);
        assert_eq!(i.tags, vec!["web"]);
        assert_eq!(resp.meta.links.next, "next-cursor-token");
    }

    /// An empty `meta.links.next` deserializes to an empty string (end of
    /// pagination); a missing `meta` block defaults the same way.
    #[test]
    fn empty_or_missing_cursor_defaults_to_empty() {
        let with_empty =
            parse_api_response(br#"{"instances":[],"meta":{"links":{"next":""}}}"#).unwrap();
        assert_eq!(with_empty.meta.links.next, "");
        let missing = parse_api_response(br#"{"instances":[]}"#).unwrap();
        assert_eq!(missing.meta.links.next, "");
    }
}
