//! Label helpers for rule evaluation: building a series' label set from a
//! queried metric plus the rule's own labels, and computing a stable dedup
//! key over a label set.
//!
//! Port of the label-handling in `app/vmalert/rule/recording.go`'s
//! `toTimeSeries` (`__name__` overlay + extra-label overlay) and
//! `app/vmalert/rule/utils.go`'s `newTimeSeries`/`stringifyLabels` (label sort
//! + dedup key).

use std::collections::BTreeMap;

const NAME_LABEL: &str = "__name__";

/// Builds a series' label set from `base` (the queried metric's labels)
/// overlaid with `extra` (the rule's configured labels), and sets `__name__`
/// to `name` (the record name).
///
/// Faithful port of `RecordingRule.toTimeSeries` (`recording.go:285-320`)
/// label handling, applied as an in-place merge over the working label set so
/// that (like upstream mutating `m.Labels`) `__name__` and earlier rule labels
/// are visible to later ones. For each rule label `(k, v)`:
/// - if `v` is empty, delete label `k` from the result and skip — an empty
///   value means "remove it to preserve relabeling compatibility (#10766),
///   otherwise ignore (#9984)"; an empty label is never added;
/// - else if a label `k` already exists with value equal to `v`, do nothing;
/// - else if a label `k` exists with a differing value, rename the existing
///   one to `exported_<k>` (preserving the original) and add `(k, v)`;
/// - else add `(k, v)`.
///
/// The result is sorted by label name to match upstream `newTimeSeries`
/// (`utils.go:16`, which calls `SortLabels`), giving a deterministic order
/// that [`labels_to_key`] can rely on.
pub fn merge_labels(
    base: &[(String, String)],
    extra: &BTreeMap<String, String>,
    name: &str,
) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::with_capacity(base.len() + extra.len() + 1);

    // Set `__name__` to the record name: replace an existing one, else append.
    let mut name_set = false;
    for (k, v) in base {
        if k == NAME_LABEL {
            out.push((k.clone(), name.to_string()));
            name_set = true;
        } else {
            out.push((k.clone(), v.clone()));
        }
    }
    if !name_set {
        out.push((NAME_LABEL.to_string(), name.to_string()));
    }

    // Overlay rule labels against the current working set (upstream mutates
    // `m.Labels` in place; iterating a `BTreeMap` gives a deterministic order).
    for (k, v) in extra {
        if v.is_empty() {
            // Empty value: drop the label entirely, never add an empty one.
            out.retain(|(ek, _)| ek != k);
            continue;
        }
        match out.iter_mut().find(|(ek, _)| ek == k) {
            Some(existing) if existing.1 == *v => {
                // Identical to an existing label — no duplicate to add.
            }
            Some(existing) => {
                // Conflict: preserve the original under an `exported_` prefix,
                // then add the rule label below.
                existing.0 = format!("exported_{k}");
                out.push((k.clone(), v.clone()));
            }
            None => out.push((k.clone(), v.clone())),
        }
    }

    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Computes a stable dedup key for a label set: `name=value` pairs sorted by
/// name and joined with `,`.
///
/// Port of `stringifyLabels` (`recording.go:272-283`), which runs on
/// already-sorted labels; this sorts defensively so the key is independent of
/// input order.
pub fn labels_to_key(labels: &[(String, String)]) -> String {
    let mut pairs: Vec<&(String, String)> = labels.iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));

    let mut key = String::new();
    for (i, (name, value)) in pairs.iter().enumerate() {
        if i != 0 {
            key.push(',');
        }
        key.push_str(name);
        key.push('=');
        key.push_str(value);
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn merge_sets_name_when_absent_and_sorts() {
        let base = vec![("instance".to_string(), "h1".to_string())];
        let out = merge_labels(&base, &BTreeMap::new(), "job:up");
        assert_eq!(
            out,
            vec![
                ("__name__".to_string(), "job:up".to_string()),
                ("instance".to_string(), "h1".to_string()),
            ]
        );
    }

    #[test]
    fn merge_replaces_existing_name() {
        let base = vec![
            ("__name__".to_string(), "up".to_string()),
            ("instance".to_string(), "h1".to_string()),
        ];
        let out = merge_labels(&base, &BTreeMap::new(), "job:up");
        assert!(out.contains(&("__name__".to_string(), "job:up".to_string())));
        assert!(!out.iter().any(|(_, v)| v == "up"));
    }

    #[test]
    fn merge_conflict_preserves_original_as_exported() {
        // Rule label conflicts with a queried label of a different value:
        // the original is preserved under `exported_<name>` and the rule
        // label is present (upstream #10766/#9984 behavior).
        let base = vec![("team".to_string(), "queried".to_string())];
        let out = merge_labels(&base, &map(&[("team", "a")]), "r");
        assert!(out.contains(&("team".to_string(), "a".to_string())));
        assert!(out.contains(&("exported_team".to_string(), "queried".to_string())));
        // The original name no longer carries the queried value.
        assert!(!out.iter().any(|(k, v)| k == "team" && v == "queried"));
    }

    #[test]
    fn merge_equal_value_does_not_duplicate() {
        // Rule label equal to an existing queried label -> single label.
        let base = vec![("team".to_string(), "a".to_string())];
        let out = merge_labels(&base, &map(&[("team", "a")]), "r");
        let team: Vec<_> = out.iter().filter(|(k, _)| k == "team").collect();
        assert_eq!(team.len(), 1);
        assert_eq!(team[0].1, "a");
        assert!(!out.iter().any(|(k, _)| k == "exported_team"));
    }

    #[test]
    fn merge_no_conflict_adds_rule_label() {
        // Rule label with no queried counterpart is simply added.
        let base = vec![("instance".to_string(), "h1".to_string())];
        let out = merge_labels(&base, &map(&[("team", "a")]), "r");
        assert!(out.contains(&("team".to_string(), "a".to_string())));
        assert!(out.contains(&("instance".to_string(), "h1".to_string())));
    }

    #[test]
    fn merge_empty_value_deletes_existing_and_adds_nothing() {
        // Empty rule value deletes an existing queried label and never adds
        // an empty one.
        let base = vec![
            ("team".to_string(), "queried".to_string()),
            ("instance".to_string(), "h1".to_string()),
        ];
        let out = merge_labels(&base, &map(&[("team", "")]), "r");
        assert!(!out.iter().any(|(k, _)| k == "team"));
        assert!(!out.iter().any(|(_, v)| v.is_empty()));
        // Unrelated labels are untouched.
        assert!(out.contains(&("instance".to_string(), "h1".to_string())));
    }

    #[test]
    fn key_is_sorted_and_stable_regardless_of_order() {
        let a = vec![
            ("b".to_string(), "2".to_string()),
            ("a".to_string(), "1".to_string()),
        ];
        let b = vec![
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ];
        assert_eq!(labels_to_key(&a), "a=1,b=2");
        assert_eq!(labels_to_key(&a), labels_to_key(&b));
    }
}
