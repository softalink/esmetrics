//! PuppetDB `Resource` serde struct, the resource-array parser
//! ([`parse_resources`]), and the `__meta_puppetdb_*` label builder
//! ([`append_target_labels`]).
//!
//! Port of `lib/promscrape/discovery/puppetdb/resource.go` (v1.146.0)
//! (`resource`/`parameters` structs, `getResourceLabels`, and
//! `parameters.addToLabels`), reshaped for this crate's [`TargetGroup`] shape
//! (one group per PuppetDB resource: the resource's `__address__` is the
//! group's single target, and the `__meta_puppetdb_*` set becomes the group's
//! `labels`).
//!
//! Upstream includes `__address__` in the returned label set because a
//! Prometheus label set *is* the target; this crate's [`TargetGroup`] carries
//! the address separately in `targets`, so [`append_target_labels`] puts it
//! there and leaves it out of `labels` — mirroring `scrape::nomad::labels`.
//!
//! `__address__` is `JoinHostPort(resource.certname, cfg.port)` (upstream
//! `getResourceLabels`); `port` defaults to 80.
//!
//! ## Number formatting parity
//!
//! Upstream unmarshals PuppetDB JSON into `map[string]any`, so *every* JSON
//! number arrives as a Go `float64` and is stringified via
//! `strconv.FormatFloat(v, 'g', -1, 64)`. This port formats every
//! [`serde_json::Number`] the same way — as an `f64` via Rust's shortest
//! round-tripping `{}` Display — so `22` -> `"22"`, `0` -> `"0"`,
//! `3.141592653589793` -> `"3.141592653589793"`. The only divergence is that
//! Go's `'g'` verb switches to exponent notation for very large/small
//! magnitudes while Rust's `{}` never does; no PuppetDB parameter in practice
//! hits that range.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::{Map, Number, Value};

use crate::scrape::discovery::TargetGroup;
use crate::scrape::kubernetes::labels::sanitize_label_name;

/// The `,`-wrapping separator upstream uses for tag lists and list-valued
/// parameters. Port of `resource.go`'s `separator`.
const SEPARATOR: &str = ",";

/// One PuppetDB resource returned by `POST /pdb/query/v4`. Port of
/// `resource.go`'s `resource` struct. `#[serde(default)]` tolerates the extra
/// response fields this port doesn't read (e.g. `line`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Resource {
    pub certname: String,
    pub resource: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub title: String,
    pub exported: bool,
    pub tags: Vec<String>,
    pub file: String,
    pub environment: String,
    pub parameters: Map<String, Value>,
}

/// Parses a `POST /pdb/query/v4` response body into a list of [`Resource`].
/// Port of `getResourceList`'s `json.Unmarshal` into `[]resource`.
pub fn parse_resources(data: &[u8]) -> Result<Vec<Resource>, String> {
    serde_json::from_slice(data).map_err(|e| format!("cannot unmarshal PuppetDB resources: {e}"))
}

/// Builds the [`TargetGroup`] for one PuppetDB resource, mirroring
/// `getResourceLabels`. `__address__` is `certname:port` and is carried in the
/// group's `targets`; every `__meta_puppetdb_*` label goes in `labels`.
/// `query` becomes `__meta_puppetdb_query`. `include_parameters` gates the
/// `__meta_puppetdb_parameter_*` set (off by default — it can leak secrets).
/// `source` is threaded through unchanged so the reconcile diff stays stable
/// across refreshes.
pub fn append_target_labels(
    res: &Resource,
    query: &str,
    include_parameters: bool,
    port: u16,
    source: String,
) -> TargetGroup {
    let address = join_host_port(&res.certname, port);

    let mut m: BTreeMap<String, String> = BTreeMap::new();
    m.insert("__meta_puppetdb_certname".into(), res.certname.clone());
    m.insert(
        "__meta_puppetdb_environment".into(),
        res.environment.clone(),
    );
    m.insert("__meta_puppetdb_exported".into(), res.exported.to_string());
    m.insert("__meta_puppetdb_file".into(), res.file.clone());
    m.insert("__meta_puppetdb_query".into(), query.to_string());
    m.insert("__meta_puppetdb_resource".into(), res.resource.clone());
    m.insert("__meta_puppetdb_title".into(), res.title.clone());
    m.insert("__meta_puppetdb_type".into(), res.type_.clone());

    if !res.tags.is_empty() {
        m.insert(
            "__meta_puppetdb_tags".into(),
            format!("{SEPARATOR}{}{SEPARATOR}", res.tags.join(SEPARATOR)),
        );
    }

    // Parameters are off by default: enabling them can expose secrets held in
    // a resource's parameters. Port of `getResourceLabels`'s guarded call.
    if include_parameters {
        add_params_to_labels(&res.parameters, "__meta_puppetdb_parameter_", &mut m);
    }

    TargetGroup {
        targets: vec![address],
        labels: m,
        source,
    }
}

/// Port of `parameters.addToLabels`: flattens a resource's `parameters` map
/// into `<key_prefix><sanitized-key>` labels.
///
/// - string -> value; bool -> `true`/`false`; number -> [`format_number`]
/// - array -> each element stringified (string/bool/number; other element
///   types contribute an empty string, matching Go's zero-value fall-through)
///   and joined with `,`; an *empty* array is skipped entirely
/// - object -> recursed with `key_prefix + sanitize(key + "_")`
/// - a resulting empty string is skipped (matches upstream's
///   `if labelValue == "" { continue }`)
fn add_params_to_labels(
    params: &Map<String, Value>,
    key_prefix: &str,
    m: &mut BTreeMap<String, String>,
) {
    for (k, v) in params {
        let label_value = match v {
            Value::String(s) => s.clone(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => format_number(n),
            Value::Array(arr) => {
                if arr.is_empty() {
                    continue;
                }
                let values: Vec<String> = arr.iter().map(stringify_array_element).collect();
                values.join(SEPARATOR)
            }
            Value::Object(obj) => {
                let sub_prefix = format!("{key_prefix}{}", sanitize_label_name(&format!("{k}_")));
                add_params_to_labels(obj, &sub_prefix, m);
                continue;
            }
            Value::Null => continue,
        };
        if label_value.is_empty() {
            continue;
        }
        m.insert(
            format!("{key_prefix}{}", sanitize_label_name(k)),
            label_value,
        );
    }
}

/// Stringifies one element of a list-valued parameter, mirroring the inner
/// switch of `parameters.addToLabels`'s `[]any` case: string/bool/number map
/// to their scalar form, and any other element type yields an empty string
/// (Go leaves `values[i]` at its zero value for unmatched types — the
/// `[]string` inner case is dead under `encoding/json`, which decodes JSON
/// arrays into `[]any`).
fn stringify_array_element(el: &Value) -> String {
    match el {
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => format_number(n),
        _ => String::new(),
    }
}

/// Stringifies a JSON number the way upstream does — as a Go `float64` via
/// `strconv.FormatFloat(v, 'g', -1, 64)`. See the module doc's "Number
/// formatting parity" note.
fn format_number(n: &Number) -> String {
    match n.as_f64() {
        Some(f) => format!("{f}"),
        None => n.to_string(),
    }
}

/// `host:port`, bracketing `host` when it is an IPv6 address (contains a
/// `:`). Port of `discoveryutil.JoinHostPort`.
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

    /// The exact fixture from upstream `puppetdb_test.go`'s
    /// `TestSDConfig_GetLabels` (itself lifted from Prometheus).
    const JSON_RESPONSE: &[u8] = br#"[
       {
          "certname": "edinburgh.example.com",
          "environment": "prod",
          "exported": false,
          "file": "/etc/puppetlabs/code/environments/prod/modules/upstream/apache/manifests/init.pp",
          "line": 384,
          "parameters": {
             "access_log": true,
             "access_log_file": "ssl_access_log",
             "additional_includes": [ ],
             "directoryindex": "",
             "docroot": "/var/www/html",
             "ensure": "absent",
             "options": [ "Indexes", "FollowSymLinks", "MultiViews" ],
             "php_flags": { },
             "labels": { "alias": "edinburgh" },
             "scriptaliases": [ { "alias": "/cgi-bin", "path": "/var/www/cgi-bin" } ],
             "port": 22,
             "pi": 3.141592653589793,
             "buckets": [ 0, 2, 5 ],
             "coordinates": [ 60.13464726551357, -2.0513768021728893 ]
          },
          "resource": "49af83866dc5a1518968b68e58a25319107afe11",
          "tags": [
             "roles::hypervisor", "apache", "apache::vhost", "class",
             "default-ssl", "profile_hypervisor", "vhost", "profile_apache",
             "hypervisor", "__node_regexp__edinburgh", "roles", "node"
          ],
          "title": "default-ssl",
          "type": "Apache::Vhost"
       }
    ]"#;

    fn one_resource() -> Resource {
        let mut r = parse_resources(JSON_RESPONSE).unwrap();
        assert_eq!(r.len(), 1);
        r.remove(0)
    }

    /// With `include_parameters: true`, the full `__meta_puppetdb_*` label set
    /// must match upstream `TestSDConfig_GetLabels` EXACTLY, including the
    /// sanitized `__meta_puppetdb_parameter_*` entries, list-valued params
    /// comma-joined, nested `labels.alias`, and the `,`-wrapped tags — while
    /// empty/unrepresentable params (empty array, empty string, `{}`, an array
    /// of objects) are dropped.
    #[test]
    fn append_labels_with_parameters_matches_upstream() {
        let res = one_resource();
        let g = append_target_labels(&res, "vhosts", true, 9100, "job/puppetdb".into());

        // __address__ is the target, not a label.
        assert_eq!(g.targets, vec!["edinburgh.example.com:9100".to_string()]);
        assert!(!g.labels.contains_key("__address__"));
        assert_eq!(g.source, "job/puppetdb");

        let l = &g.labels;
        assert_eq!(l["__meta_puppetdb_query"], "vhosts");
        assert_eq!(l["__meta_puppetdb_certname"], "edinburgh.example.com");
        assert_eq!(l["__meta_puppetdb_environment"], "prod");
        assert_eq!(l["__meta_puppetdb_exported"], "false");
        assert_eq!(
            l["__meta_puppetdb_file"],
            "/etc/puppetlabs/code/environments/prod/modules/upstream/apache/manifests/init.pp"
        );
        assert_eq!(l["__meta_puppetdb_parameter_access_log"], "true");
        assert_eq!(
            l["__meta_puppetdb_parameter_access_log_file"],
            "ssl_access_log"
        );
        assert_eq!(l["__meta_puppetdb_parameter_buckets"], "0,2,5");
        assert_eq!(
            l["__meta_puppetdb_parameter_coordinates"],
            "60.13464726551357,-2.0513768021728893"
        );
        assert_eq!(l["__meta_puppetdb_parameter_docroot"], "/var/www/html");
        assert_eq!(l["__meta_puppetdb_parameter_ensure"], "absent");
        assert_eq!(l["__meta_puppetdb_parameter_labels_alias"], "edinburgh");
        assert_eq!(
            l["__meta_puppetdb_parameter_options"],
            "Indexes,FollowSymLinks,MultiViews"
        );
        assert_eq!(l["__meta_puppetdb_parameter_pi"], "3.141592653589793");
        assert_eq!(l["__meta_puppetdb_parameter_port"], "22");
        assert_eq!(
            l["__meta_puppetdb_resource"],
            "49af83866dc5a1518968b68e58a25319107afe11"
        );
        assert_eq!(
            l["__meta_puppetdb_tags"],
            ",roles::hypervisor,apache,apache::vhost,class,default-ssl,profile_hypervisor,vhost,profile_apache,hypervisor,__node_regexp__edinburgh,roles,node,"
        );
        assert_eq!(l["__meta_puppetdb_title"], "default-ssl");
        assert_eq!(l["__meta_puppetdb_type"], "Apache::Vhost");

        // Empty / unrepresentable parameters are dropped, matching upstream.
        assert!(!l.contains_key("__meta_puppetdb_parameter_additional_includes"));
        assert!(!l.contains_key("__meta_puppetdb_parameter_directoryindex"));
        assert!(!l.contains_key("__meta_puppetdb_parameter_php_flags"));
        assert!(!l.contains_key("__meta_puppetdb_parameter_scriptaliases"));

        // The emitted label set must equal EXACTLY these 19 keys (upstream's
        // expected map, minus __address__ which this crate carries as a
        // target rather than a label).
        let expected_keys = [
            "__meta_puppetdb_query",
            "__meta_puppetdb_certname",
            "__meta_puppetdb_environment",
            "__meta_puppetdb_exported",
            "__meta_puppetdb_file",
            "__meta_puppetdb_parameter_access_log",
            "__meta_puppetdb_parameter_access_log_file",
            "__meta_puppetdb_parameter_buckets",
            "__meta_puppetdb_parameter_coordinates",
            "__meta_puppetdb_parameter_docroot",
            "__meta_puppetdb_parameter_ensure",
            "__meta_puppetdb_parameter_labels_alias",
            "__meta_puppetdb_parameter_options",
            "__meta_puppetdb_parameter_pi",
            "__meta_puppetdb_parameter_port",
            "__meta_puppetdb_resource",
            "__meta_puppetdb_tags",
            "__meta_puppetdb_title",
            "__meta_puppetdb_type",
        ];
        let want: std::collections::BTreeSet<&str> = expected_keys.iter().copied().collect();
        assert_eq!(want.len(), 19);
        let got: std::collections::BTreeSet<&str> = l.keys().map(String::as_str).collect();
        assert_eq!(got, want, "label key set mismatch");
    }

    /// Without `include_parameters`, only the base `__meta_puppetdb_*` set (no
    /// `__meta_puppetdb_parameter_*`) is emitted; the base labels and the
    /// `,`-wrapped tags are still present.
    #[test]
    fn append_labels_without_parameters_omits_parameter_labels() {
        let res = one_resource();
        let g = append_target_labels(&res, "vhosts", false, 80, "src".into());

        assert_eq!(g.targets, vec!["edinburgh.example.com:80".to_string()]);
        let l = &g.labels;
        assert_eq!(l["__meta_puppetdb_certname"], "edinburgh.example.com");
        assert_eq!(l["__meta_puppetdb_query"], "vhosts");
        assert_eq!(l["__meta_puppetdb_exported"], "false");
        assert_eq!(l["__meta_puppetdb_title"], "default-ssl");
        assert!(l.contains_key("__meta_puppetdb_tags"));
        assert!(
            l.keys()
                .all(|k| !k.starts_with("__meta_puppetdb_parameter_")),
            "no parameter labels expected, got: {:?}",
            l.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn parse_resources_rejects_bad_json() {
        for s in ["", "{}", "[1,2]"] {
            assert!(
                parse_resources(s.as_bytes()).is_err(),
                "expected err for {s:?}"
            );
        }
    }

    #[test]
    fn ipv6_certname_is_bracketed() {
        assert_eq!(join_host_port("::1", 80), "[::1]:80");
        assert_eq!(join_host_port("host.example", 9100), "host.example:9100");
    }

    #[test]
    fn empty_tags_omit_the_tags_label() {
        let res = Resource {
            certname: "h".into(),
            ..Resource::default()
        };
        let g = append_target_labels(&res, "q", true, 80, "s".into());
        assert!(!g.labels.contains_key("__meta_puppetdb_tags"));
    }
}
