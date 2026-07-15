//! Target relabeling + scrape URL: turns discovered [`TargetGroup`]s into
//! scrape [`Target`]s (or [`DroppedTarget`]s, for targets that
//! `target_relabel` drops).
//!
//! Port of the target-relabel slice of `lib/promscrape/config.go`'s
//! `getScrapeWork`/`mergeLabels` (~line 1204) plus
//! `lib/promrelabel/scrape_url.go`'s `GetScrapeURL`. The scrape loop, HTTP
//! fetch, manager, and CLI wiring are later tasks — see the module doc in
//! `scrape/mod.rs`.
//!
//! ## Deliberate divergences from upstream (task-brief-driven)
//!
//! - **`instance` default source.** Upstream defaults the `instance` label
//!   to `GetScrapeURL`'s second return value (`__address__` with a default
//!   port `:80`/`:443` appended when missing —
//!   `promrelabel.GetScrapeURL`/`addMissingPort`). This port defaults
//!   `instance` to the post-relabel `__address__` label's raw value
//!   instead (per the task brief), so no default port is appended to
//!   `instance` here.
//! - **`__scrape_interval__`/`__scrape_timeout__` synthetic labels.**
//!   Upstream's `mergeLabels` always adds these two labels (as the
//!   resolved-against-global duration strings, even when the scrape config
//!   doesn't override them). This port only adds them when
//!   `ScrapeConfig::scrape_interval`/`scrape_timeout` is `Some` (per the
//!   task brief) — `ScrapeConfig` doesn't carry the resolved-against-global
//!   value at this layer (that resolution is `config::validate`'s job, one
//!   layer up), so there is nothing to format when the field is `None`.
//! - **Missing scrape URL.** Upstream drops a target whose computed scrape
//!   URL is empty (`targetDropReasonMissingScrapeURL`) — i.e. `__address__`
//!   was relabeled away to an empty string. This port mirrors that: such a
//!   target becomes a [`DroppedTarget`] rather than being silently
//!   discarded, even though the task brief's tests don't exercise this
//!   path directly.
//! - **`url.Values.Encode()` percent-encoding.** This port uses
//!   [`url::form_urlencoded`] (percent-encodes to the WHATWG
//!   `application/x-www-form-urlencoded` profile) rather than Go's
//!   `net/url` encoder. Both sort by key and preserve per-key value order;
//!   they can differ on the exact percent-encoding of a handful of
//!   characters (e.g. Go escapes space as `+`, matching WHATWG's `+` for
//!   form encoding too), but no test in this task's scope exercises an
//!   edge-case character, so the difference is unexercised.

use std::collections::BTreeMap;
use std::time::Duration;

use esm_relabel::{Label, ParsedConfigs};

use super::config::ScrapeConfig;
use super::discovery::TargetGroup;

/// A target ready to scrape: its computed URL, its public per-sample labels
/// (`__*` stripped), and its pre-relabel labels (kept for `/targets`
/// reporting).
#[derive(Debug, Clone, PartialEq)]
pub struct Target {
    pub scrape_url: String,
    pub labels: Vec<Label>,
    pub discovered_labels: Vec<Label>,
}

/// A target that `target_relabel` (or missing-scrape-URL handling) dropped.
/// Only the pre-relabel labels are kept — matches upstream's
/// `droppedTargetsMap`, used for `/targets` reporting.
#[derive(Debug, Clone, PartialEq)]
pub struct DroppedTarget {
    pub discovered_labels: Vec<Label>,
}

/// Returns the value of the label named `name`, or `None` if absent.
/// Local to this module because `esm_relabel::label::get_label_value` is
/// crate-private to `esm-relabel`.
fn get_label<'a>(labels: &'a [Label], name: &str) -> Option<&'a str> {
    labels
        .iter()
        .find(|l| l.name == name)
        .map(|l| l.value.as_str())
}

/// Sets `name` to `value`, overwriting an existing entry in place (so a
/// later overlay — e.g. `group.labels` — wins over an earlier synthetic
/// label of the same name) rather than appending a duplicate.
fn set_label(labels: &mut Vec<Label>, name: &str, value: &str) {
    match labels.iter_mut().find(|l| l.name == name) {
        Some(l) => l.value = value.to_string(),
        None => labels.push(Label {
            name: name.to_string(),
            value: value.to_string(),
        }),
    }
}

/// Formats a [`Duration`] as a Prometheus-duration-grammar string
/// (`esm_metricsql::duration_value`'s input format) — whole seconds as
/// `<n>s`, otherwise milliseconds as `<n>ms`. `ScrapeConfig` only carries a
/// parsed `Duration`, not the original config string, so this is a
/// reformat rather than a round-trip of the user's exact spelling.
fn format_duration(d: Duration) -> String {
    let millis = d.as_millis();
    if millis % 1000 == 0 {
        format!("{}s", millis / 1000)
    } else {
        format!("{millis}ms")
    }
}

/// Assembles the pre-relabel synthetic + group label set for one target
/// address. Port of `mergeLabels` (`config.go:1364`), narrowed per the task
/// brief — see the module doc for the `__scrape_interval__`/
/// `__scrape_timeout__` divergence. `group.labels` is applied last, so it
/// overrides any synthetic label of the same name (matches upstream's
/// `AddFrom` + `RemoveDuplicates`, which keeps the last-added value).
fn assemble_labels(sc: &ScrapeConfig, address: &str, group: &TargetGroup) -> Vec<Label> {
    let mut labels = Vec::new();
    set_label(&mut labels, "__address__", address);
    set_label(&mut labels, "__scheme__", &sc.scheme);
    set_label(&mut labels, "__metrics_path__", &sc.metrics_path);
    for (k, values) in &sc.params {
        if let Some(first) = values.first() {
            set_label(&mut labels, &format!("__param_{k}"), first);
        }
    }
    set_label(&mut labels, "job", &sc.job_name);
    if let Some(d) = sc.scrape_interval {
        set_label(&mut labels, "__scrape_interval__", &format_duration(d));
    }
    if let Some(d) = sc.scrape_timeout {
        set_label(&mut labels, "__scrape_timeout__", &format_duration(d));
    }
    for (k, v) in &group.labels {
        set_label(&mut labels, k, v);
    }
    labels
}

/// Turns every address in every group into a [`Target`] or
/// [`DroppedTarget`]. `target_relabel` is `sc.relabel_configs`, already
/// parsed — a later task/caller wires that conversion; this function just
/// applies whatever `ParsedConfigs` it's handed.
pub fn build_targets(
    sc: &ScrapeConfig,
    target_relabel: &ParsedConfigs,
    groups: &[TargetGroup],
) -> (Vec<Target>, Vec<DroppedTarget>) {
    let mut active = Vec::new();
    let mut dropped = Vec::new();

    for group in groups {
        for address in &group.targets {
            let discovered_labels = assemble_labels(sc, address, group);
            let mut labels = discovered_labels.clone();

            if !target_relabel.apply(&mut labels) {
                dropped.push(DroppedTarget { discovered_labels });
                continue;
            }

            if get_label(&labels, "instance").is_none() {
                if let Some(addr) = get_label(&labels, "__address__") {
                    let addr = addr.to_string();
                    set_label(&mut labels, "instance", &addr);
                }
            }

            let Some(scrape_url) = get_scrape_url(&labels, &sc.params) else {
                // Upstream drops a target whose computed scrape URL is
                // empty (missing/relabeled-away __address__) rather than
                // erroring — see the module doc.
                dropped.push(DroppedTarget { discovered_labels });
                continue;
            };

            let public_labels = labels
                .into_iter()
                .filter(|l| !l.name.starts_with("__"))
                .collect();

            active.push(Target {
                scrape_url,
                labels: public_labels,
                discovered_labels,
            });
        }
    }

    (active, dropped)
}

/// Collects `__param_*` labels (post-relabel) into `{name: [value, ...]}`,
/// merging in any secondary values (`extra_params[name][1:]`) from the
/// scrape config's own `params:` block. Port of `getParamsFromLabels`
/// (`scrape_url.go:64`).
fn get_params_from_labels(
    labels: &[Label],
    extra_params: &BTreeMap<String, Vec<String>>,
) -> BTreeMap<String, Vec<String>> {
    let mut params: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for l in labels {
        let Some(name) = l.name.strip_prefix("__param_") else {
            continue;
        };
        let mut values = vec![l.value.clone()];
        if let Some(extra) = extra_params.get(name) {
            if extra.len() > 1 {
                values.extend_from_slice(&extra[1..]);
            }
        }
        params.insert(name.to_string(), values);
    }
    params
}

/// Encodes `params` the way `url.Values.Encode()` does: keys in sorted
/// order (guaranteed by the `BTreeMap`), each key's values in their
/// original order, percent-encoded and joined with `&`.
fn encode_params(params: &BTreeMap<String, Vec<String>>) -> String {
    let mut ser = url::form_urlencoded::Serializer::new(String::new());
    for (name, values) in params {
        for value in values {
            ser.append_pair(name, value);
        }
    }
    ser.finish()
}

/// Computes the scrape URL from a target's (post-relabel) labels, or
/// `None` if `__address__` is absent/empty. Port of
/// `promrelabel.GetScrapeURL` (`scrape_url.go:12`), minus its second return
/// value (the port-padded address used for the upstream `instance`
/// default) — see the module doc for why this port doesn't need it.
pub fn get_scrape_url(
    labels: &[Label],
    extra_params: &BTreeMap<String, Vec<String>>,
) -> Option<String> {
    let mut scheme = get_label(labels, "__scheme__").unwrap_or("");
    if scheme.is_empty() {
        scheme = "http";
    }
    let mut metrics_path = get_label(labels, "__metrics_path__").unwrap_or("");
    if metrics_path.is_empty() {
        metrics_path = "/metrics";
    }
    let mut address = get_label(labels, "__address__").unwrap_or("");
    if address.is_empty() {
        return None;
    }

    // Usability extension to Prometheus behavior: extract optional scheme
    // and metrics_path from __address__.
    if let Some(rest) = address.strip_prefix("http://") {
        scheme = "http";
        address = rest;
    } else if let Some(rest) = address.strip_prefix("https://") {
        scheme = "https";
        address = rest;
    }
    let mut metrics_path = metrics_path.to_string();
    if let Some(idx) = address.find('/') {
        metrics_path = address[idx..].to_string();
        address = &address[..idx];
    }
    if !metrics_path.starts_with('/') {
        metrics_path = format!("/{metrics_path}");
    }

    let params = get_params_from_labels(labels, extra_params);
    let params_str = encode_params(&params);
    let optional_question = if params.is_empty() {
        ""
    } else if metrics_path.contains('?') {
        "&"
    } else {
        "?"
    };

    Some(format!(
        "{scheme}://{address}{metrics_path}{optional_question}{params_str}"
    ))
}

#[cfg(test)]
mod tests {
    use super::super::config::StaticConfig;
    use super::super::discovery::static_target_groups;
    use super::*;

    fn ls(pairs: &[(&str, &str)]) -> Vec<Label> {
        pairs
            .iter()
            .map(|(n, v)| Label {
                name: n.to_string(),
                value: v.to_string(),
            })
            .collect()
    }

    fn test_scrape_config(job_name: &str, targets: &[&str]) -> ScrapeConfig {
        ScrapeConfig {
            job_name: job_name.to_string(),
            scheme: "http".to_string(),
            metrics_path: "/metrics".to_string(),
            honor_timestamps: true,
            static_configs: vec![StaticConfig {
                targets: targets.iter().map(|s| s.to_string()).collect(),
                labels: BTreeMap::new(),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn builds_scrape_url_and_labels() {
        let sc = test_scrape_config("node", &["h1:9100"]);
        let rel = ParsedConfigs::parse("[]").unwrap(); // no target relabel
        let groups = static_target_groups(&sc.static_configs, "node");
        let (active, dropped) = build_targets(&sc, &rel, &groups);
        assert!(dropped.is_empty());
        assert_eq!(active[0].scrape_url, "http://h1:9100/metrics");
        assert!(active[0]
            .labels
            .iter()
            .any(|l| l.name == "job" && l.value == "node"));
        assert!(active[0]
            .labels
            .iter()
            .any(|l| l.name == "instance" && l.value == "h1:9100"));
        assert!(active[0].labels.iter().all(|l| !l.name.starts_with("__"))); // __* stripped
    }

    #[test]
    fn get_scrape_url_extracts_scheme_and_path_from_address() {
        assert_eq!(
            get_scrape_url(
                &ls(&[("__address__", "https://h1:9100/m")]),
                &Default::default()
            )
            .as_deref(),
            Some("https://h1:9100/m")
        );
    }

    #[test]
    fn relabel_drop_yields_dropped_target() {
        let sc = test_scrape_config("node", &["h1:9100"]);
        let rel = ParsedConfigs::parse(
            "- source_labels: [__address__]\n  regex: 'h1:.*'\n  action: drop\n",
        )
        .unwrap();
        let (active, dropped) =
            build_targets(&sc, &rel, &static_target_groups(&sc.static_configs, "node"));
        assert!(active.is_empty());
        assert_eq!(dropped.len(), 1);
    }

    #[test]
    fn get_scrape_url_returns_none_for_missing_address() {
        assert_eq!(get_scrape_url(&ls(&[]), &Default::default()), None);
    }

    #[test]
    fn get_scrape_url_includes_params_from_labels_and_extra_params() {
        let labels = ls(&[
            ("__scheme__", "http"),
            ("__metrics_path__", "/metrics"),
            ("__address__", "h1:9100"),
            ("__param_foo", "bar"),
        ]);
        let mut extra: BTreeMap<String, Vec<String>> = BTreeMap::new();
        extra.insert(
            "foo".to_string(),
            vec!["bar".to_string(), "baz".to_string()],
        );
        let url = get_scrape_url(&labels, &extra).unwrap();
        assert_eq!(url, "http://h1:9100/metrics?foo=bar&foo=baz");
    }

    #[test]
    fn group_labels_override_synthetic_labels_and_job_defaults() {
        let mut sc = test_scrape_config("node", &["h1:9100"]);
        sc.static_configs[0]
            .labels
            .insert("job".to_string(), "custom-job".to_string());
        let rel = ParsedConfigs::parse("[]").unwrap();
        let groups = static_target_groups(&sc.static_configs, "node");
        let (active, dropped) = build_targets(&sc, &rel, &groups);
        assert!(dropped.is_empty());
        assert!(active[0]
            .labels
            .iter()
            .any(|l| l.name == "job" && l.value == "custom-job"));
    }

    #[test]
    fn explicit_instance_label_is_not_overridden() {
        let mut sc = test_scrape_config("node", &["h1:9100"]);
        sc.static_configs[0]
            .labels
            .insert("instance".to_string(), "custom-instance".to_string());
        let rel = ParsedConfigs::parse("[]").unwrap();
        let groups = static_target_groups(&sc.static_configs, "node");
        let (active, _dropped) = build_targets(&sc, &rel, &groups);
        assert!(active[0]
            .labels
            .iter()
            .any(|l| l.name == "instance" && l.value == "custom-instance"));
    }

    #[test]
    fn discovered_labels_keep_dunder_labels_and_survive_drop() {
        let sc = test_scrape_config("node", &["h1:9100"]);
        let rel = ParsedConfigs::parse(
            "- source_labels: [__address__]\n  regex: 'h1:.*'\n  action: drop\n",
        )
        .unwrap();
        let (_active, dropped) =
            build_targets(&sc, &rel, &static_target_groups(&sc.static_configs, "node"));
        assert!(dropped[0]
            .discovered_labels
            .iter()
            .any(|l| l.name == "__address__" && l.value == "h1:9100"));
    }

    #[test]
    fn scrape_interval_and_timeout_labels_are_set_only_when_present() {
        let mut sc = test_scrape_config("node", &["h1:9100"]);
        sc.scrape_interval = Some(Duration::from_secs(15));
        let rel = ParsedConfigs::parse("[]").unwrap();
        let groups = static_target_groups(&sc.static_configs, "node");
        let (active, _dropped) = build_targets(&sc, &rel, &groups);
        assert!(active[0]
            .discovered_labels
            .iter()
            .any(|l| l.name == "__scrape_interval__" && l.value == "15s"));
        assert!(!active[0]
            .discovered_labels
            .iter()
            .any(|l| l.name == "__scrape_timeout__"));
    }
}
