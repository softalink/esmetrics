//! Kuma MADS (`DiscoveryResponse`/`MonitoringAssignment`/`Target`) serde
//! structs and the `__meta_kuma_*` label builder ([`parse_targets_labels`]).
//!
//! Port of `lib/promscrape/discovery/kuma/api.go` (v1.146.0) — the
//! `discoveryResponse`/`resource`/`target` structs, `parseTargetsLabels`,
//! `getTargetsLabels`, and `addLabels` — reshaped for this crate's
//! [`TargetGroup`] shape (one target per assignment target; the target's
//! `__address__` is carried in `targets` rather than as a label).
//!
//! Upstream's per-target label set is (`getTargetsLabels`):
//! `instance` (= target name), `__address__`, `__scheme__`,
//! `__metrics_path__`, `__meta_kuma_dataplane` (= target name),
//! `__meta_kuma_mesh`, `__meta_kuma_service`, plus `__meta_kuma_label_<k>`
//! (sanitized key) for every assignment-level label then every target-level
//! label (the target-level value wins on a key collision — upstream's
//! `addLabels(r.Labels)` then `addLabels(t.Labels)` + `RemoveDuplicates`,
//! which keeps the last-added value; a [`BTreeMap`] insert has the same
//! last-wins semantics). `__address__` is the only one this port carries
//! outside `labels` (in [`KumaTarget::address`]), matching
//! `scrape::puppetdb`/`scrape::nomad`.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scrape::kubernetes::labels::sanitize_label_name;

/// One discovered Kuma target: the `__address__` (carried here rather than as
/// a label, mirroring the other providers) plus its `instance`/`__scheme__`/
/// `__metrics_path__`/`__meta_kuma_*` label set.
#[derive(Debug, Clone, PartialEq)]
pub struct KumaTarget {
    pub address: String,
    pub labels: BTreeMap<String, String>,
}

/// Parses a MADS `DiscoveryResponse` body into the flat per-target label sets
/// plus the response's `version_info` and `nonce`. Port of
/// `parseTargetsLabels` + `getTargetsLabels`.
pub fn parse_targets_labels(data: &[u8]) -> Result<(Vec<KumaTarget>, String, String), String> {
    let resp: DiscoveryResponse = serde_json::from_slice(data)
        .map_err(|e| format!("cannot unmarshal Kuma discovery response: {e}"))?;
    let mut targets = Vec::new();
    for r in &resp.resources {
        for t in &r.targets {
            targets.push(build_target(r, t));
        }
    }
    Ok((targets, resp.version_info, resp.nonce))
}

/// Builds one [`KumaTarget`] from an assignment `r` and one of its targets `t`,
/// mirroring `getTargetsLabels`'s per-target `promutils.Labels`.
fn build_target(r: &Resource, t: &Target) -> KumaTarget {
    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    labels.insert("instance".into(), t.name.clone());
    labels.insert("__scheme__".into(), t.scheme.clone());
    labels.insert("__metrics_path__".into(), t.metrics_path.clone());
    labels.insert("__meta_kuma_dataplane".into(), t.name.clone());
    labels.insert("__meta_kuma_mesh".into(), r.mesh.clone());
    labels.insert("__meta_kuma_service".into(), r.service.clone());

    // Assignment-level labels first, then target-level labels (the latter wins
    // on a key collision — matches upstream's ordered addLabels calls).
    add_labels(&r.labels, &mut labels);
    add_labels(&t.labels, &mut labels);

    KumaTarget {
        address: t.address.clone(),
        labels,
    }
}

/// Flattens `src` into `__meta_kuma_label_<sanitized-key>` entries. Port of
/// `addLabels`.
fn add_labels(src: &BTreeMap<String, String>, dst: &mut BTreeMap<String, String>) {
    for (k, v) in src {
        let name = format!("__meta_kuma_label_{}", sanitize_label_name(k));
        dst.insert(name, v.clone());
    }
}

/// MADS `DiscoveryResponse`. Port of `api.go`'s `discoveryResponse`.
/// `#[serde(default)]` tolerates the `type_url` field this port doesn't read.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct DiscoveryResponse {
    version_info: String,
    resources: Vec<Resource>,
    nonce: String,
}

/// One `MonitoringAssignment` resource. Port of `api.go`'s `resource`.
/// `#[serde(default)]` tolerates the `@type` field this port doesn't read.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Resource {
    mesh: String,
    service: String,
    targets: Vec<Target>,
    labels: BTreeMap<String, String>,
}

/// One assignment target. Port of `api.go`'s `target`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Target {
    name: String,
    scheme: String,
    address: String,
    metrics_path: String,
    labels: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact `DiscoveryResponse` fixture from upstream `api_test.go`'s
    /// `TestParseTargetsLabels`.
    const JSON: &[u8] = br#"{
    "version_info":"5dc9a5dd-2091-4426-a886-dfdc24fc99d7",
    "resources":[
       {
          "@type":"type.googleapis.com/kuma.observability.v1.MonitoringAssignment",
          "mesh":"default",
          "service":"redis",
          "labels":{ "test":"test1" },
          "targets":[
             {
                "name":"redis",
                "scheme":"http",
                "address":"127.0.0.1:5670",
                "metrics_path":"/metrics",
                "labels":{ "kuma_io_protocol":"tcp" }
             }
          ]
       },
       {
          "@type":"type.googleapis.com/kuma.observability.v1.MonitoringAssignment",
          "mesh":"default",
          "service":"app",
          "labels":{ "test":"test2" },
          "targets":[
             {
                "name":"app",
                "scheme":"https",
                "address":"127.0.0.1:5671",
                "metrics_path":"/metrics/abc",
                "labels":{ "kuma_io_protocol":"http" }
             }
          ]
       }
    ],
    "type_url":"type.googleapis.com/kuma.observability.v1.MonitoringAssignment",
    "nonce": "foobar"
 }"#;

    fn expected(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// The parsed per-target label sets, `version_info`, and `nonce` must
    /// match upstream `TestParseTargetsLabels` EXACTLY (with `__address__`
    /// carried as the target address rather than a label).
    #[test]
    fn parse_targets_labels_matches_upstream() {
        let (targets, version_info, nonce) = parse_targets_labels(JSON).unwrap();
        assert_eq!(targets.len(), 2);

        assert_eq!(targets[0].address, "127.0.0.1:5670");
        assert_eq!(
            targets[0].labels,
            expected(&[
                ("instance", "redis"),
                ("__scheme__", "http"),
                ("__metrics_path__", "/metrics"),
                ("__meta_kuma_dataplane", "redis"),
                ("__meta_kuma_mesh", "default"),
                ("__meta_kuma_service", "redis"),
                ("__meta_kuma_label_kuma_io_protocol", "tcp"),
                ("__meta_kuma_label_test", "test1"),
            ])
        );

        assert_eq!(targets[1].address, "127.0.0.1:5671");
        assert_eq!(
            targets[1].labels,
            expected(&[
                ("instance", "app"),
                ("__scheme__", "https"),
                ("__metrics_path__", "/metrics/abc"),
                ("__meta_kuma_dataplane", "app"),
                ("__meta_kuma_mesh", "default"),
                ("__meta_kuma_service", "app"),
                ("__meta_kuma_label_kuma_io_protocol", "http"),
                ("__meta_kuma_label_test", "test2"),
            ])
        );

        // __address__ is carried as the target address, never as a label.
        assert!(targets
            .iter()
            .all(|t| !t.labels.contains_key("__address__")));

        assert_eq!(version_info, "5dc9a5dd-2091-4426-a886-dfdc24fc99d7");
        assert_eq!(nonce, "foobar");
    }

    /// A non-ASCII-safe assignment label key is sanitized to
    /// `[a-zA-Z0-9_]`; a target-level label overrides an assignment-level one
    /// on the same (post-sanitize) key.
    #[test]
    fn label_keys_are_sanitized_and_target_labels_win() {
        let json = br#"{
            "version_info":"v","nonce":"n",
            "resources":[{"mesh":"m","service":"s","labels":{"kuma.io/zone":"z","dup":"from-resource"},
              "targets":[{"name":"t","scheme":"http","address":"h:1","metrics_path":"/m",
                "labels":{"dup":"from-target"}}]}]
        }"#;
        let (targets, _v, _n) = parse_targets_labels(json).unwrap();
        assert_eq!(targets.len(), 1);
        let l = &targets[0].labels;
        assert_eq!(l["__meta_kuma_label_kuma_io_zone"], "z");
        assert_eq!(l["__meta_kuma_label_dup"], "from-target");
    }

    #[test]
    fn parse_rejects_bad_json() {
        assert!(parse_targets_labels(b"not json").is_err());
    }

    #[test]
    fn empty_resources_yield_no_targets() {
        let (targets, v, n) =
            parse_targets_labels(br#"{"version_info":"v","nonce":"n","resources":[]}"#).unwrap();
        assert!(targets.is_empty());
        assert_eq!(v, "v");
        assert_eq!(n, "n");
    }
}
