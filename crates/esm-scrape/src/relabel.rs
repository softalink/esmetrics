//! Prometheus-compatible relabel engine.
//!
//! Implements the full Prometheus relabel action set so that vmagent-style
//! scrape configs can be lifted into esm-agent unmodified.
//!
//! Reference: <https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config>

use std::collections::BTreeMap;

use regex::Regex;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct RelabelRule {
    pub source_labels: Vec<String>,
    pub separator: String,
    pub target_label: Option<String>,
    pub regex: Regex,
    pub modulus: u64,
    pub replacement: String,
    pub action: Action,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Replace,
    Keep,
    Drop,
    HashMod,
    LabelMap,
    LabelDrop,
    LabelKeep,
    Lowercase,
    Uppercase,
    KeepEqual,
    DropEqual,
}

impl Action {
    /// Parse a YAML/string action name into the enum.
    ///
    /// # Errors
    /// Returns [`RelabelError::UnknownAction`] for unknown names.
    pub fn from_name(s: &str) -> Result<Self, RelabelError> {
        Ok(match s {
            "replace" => Self::Replace,
            "keep" => Self::Keep,
            "drop" => Self::Drop,
            "hashmod" => Self::HashMod,
            "labelmap" => Self::LabelMap,
            "labeldrop" => Self::LabelDrop,
            "labelkeep" => Self::LabelKeep,
            "lowercase" => Self::Lowercase,
            "uppercase" => Self::Uppercase,
            "keepequal" => Self::KeepEqual,
            "dropequal" => Self::DropEqual,
            other => return Err(RelabelError::UnknownAction(other.to_string())),
        })
    }
}

/// Apply a chain of relabel rules to a label-set. Returns `None` when a
/// `keep`/`drop` action removes the target series entirely.
#[must_use]
pub fn apply(
    rules: &[RelabelRule],
    mut labels: BTreeMap<String, String>,
) -> Option<BTreeMap<String, String>> {
    for rule in rules {
        let joined = rule
            .source_labels
            .iter()
            .map(|l| labels.get(l).cloned().unwrap_or_default())
            .collect::<Vec<_>>()
            .join(&rule.separator);
        match rule.action {
            Action::Replace => {
                if let Some(captures) = rule.regex.captures(&joined)
                    && let Some(target) = &rule.target_label
                {
                    let replaced = expand_template(&rule.replacement, &captures);
                    let resolved_target = expand_template(target, &captures);
                    if replaced.is_empty() {
                        labels.remove(&resolved_target);
                    } else {
                        labels.insert(resolved_target, replaced);
                    }
                }
            }
            Action::Keep => {
                if !rule.regex.is_match(&joined) {
                    return None;
                }
            }
            Action::Drop => {
                if rule.regex.is_match(&joined) {
                    return None;
                }
            }
            Action::HashMod => {
                if let Some(target) = &rule.target_label {
                    let h = fnv1a64(joined.as_bytes());
                    let m = if rule.modulus == 0 { 1 } else { rule.modulus };
                    labels.insert(target.clone(), (h % m).to_string());
                }
            }
            Action::LabelMap => {
                let mut to_insert: Vec<(String, String)> = Vec::new();
                for (k, v) in &labels {
                    if let Some(c) = rule.regex.captures(k) {
                        let new_name = expand_template(&rule.replacement, &c);
                        to_insert.push((new_name, v.clone()));
                    }
                }
                for (k, v) in to_insert {
                    labels.insert(k, v);
                }
            }
            Action::LabelDrop => {
                let to_drop: Vec<String> =
                    labels.keys().filter(|k| rule.regex.is_match(k)).cloned().collect();
                for k in to_drop {
                    labels.remove(&k);
                }
            }
            Action::LabelKeep => {
                let to_drop: Vec<String> =
                    labels.keys().filter(|k| !rule.regex.is_match(k)).cloned().collect();
                for k in to_drop {
                    labels.remove(&k);
                }
            }
            Action::Lowercase => {
                if let Some(target) = &rule.target_label {
                    labels.insert(target.clone(), joined.to_lowercase());
                }
            }
            Action::Uppercase => {
                if let Some(target) = &rule.target_label {
                    labels.insert(target.clone(), joined.to_uppercase());
                }
            }
            Action::KeepEqual => {
                if let Some(target) = &rule.target_label
                    && labels.get(target).map(String::as_str) != Some(joined.as_str())
                {
                    return None;
                }
            }
            Action::DropEqual => {
                if let Some(target) = &rule.target_label
                    && labels.get(target).map(String::as_str) == Some(joined.as_str())
                {
                    return None;
                }
            }
        }
    }
    Some(labels)
}

fn expand_template(template: &str, captures: &regex::Captures<'_>) -> String {
    // Prometheus uses `$1`, `$2`, ... for capture references. The `regex`
    // crate's `expand` produces exactly that semantics.
    let mut out = String::new();
    captures.expand(template, &mut out);
    out
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

#[derive(Debug, Error)]
pub enum RelabelError {
    #[error("unknown relabel action: {0:?}")]
    UnknownAction(String),
    #[error("invalid regex {pattern:?}: {source}")]
    BadRegex {
        pattern: String,
        #[source]
        source: regex::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(action: Action) -> RelabelRule {
        RelabelRule {
            source_labels: vec![],
            separator: ";".into(),
            target_label: None,
            regex: Regex::new(".*").unwrap(),
            modulus: 0,
            replacement: "$1".into(),
            action,
        }
    }

    #[test]
    fn replace_basic() {
        let mut r = rule(Action::Replace);
        r.source_labels = vec!["a".into()];
        r.target_label = Some("b".into());
        r.regex = Regex::new("(.+)").unwrap();
        r.replacement = "v_$1".into();
        let mut labels = BTreeMap::new();
        labels.insert("a".to_string(), "1".to_string());
        let out = apply(&[r], labels).unwrap();
        assert_eq!(out.get("b").map(String::as_str), Some("v_1"));
    }

    #[test]
    fn keep_match_passes() {
        let mut r = rule(Action::Keep);
        r.source_labels = vec!["job".into()];
        r.regex = Regex::new("api").unwrap();
        let mut labels = BTreeMap::new();
        labels.insert("job".to_string(), "api".to_string());
        assert!(apply(&[r], labels).is_some());
    }

    #[test]
    fn keep_no_match_drops() {
        let mut r = rule(Action::Keep);
        r.source_labels = vec!["job".into()];
        r.regex = Regex::new("api").unwrap();
        let mut labels = BTreeMap::new();
        labels.insert("job".to_string(), "node".to_string());
        assert!(apply(&[r], labels).is_none());
    }

    #[test]
    fn drop_match_drops() {
        let mut r = rule(Action::Drop);
        r.source_labels = vec!["job".into()];
        r.regex = Regex::new("api").unwrap();
        let mut labels = BTreeMap::new();
        labels.insert("job".to_string(), "api".to_string());
        assert!(apply(&[r], labels).is_none());
    }

    #[test]
    fn labelmap_renames() {
        let mut r = rule(Action::LabelMap);
        r.regex = Regex::new("^old_(.+)$").unwrap();
        r.replacement = "new_$1".into();
        let mut labels = BTreeMap::new();
        labels.insert("old_foo".to_string(), "v".to_string());
        let out = apply(&[r], labels).unwrap();
        assert_eq!(out.get("new_foo").map(String::as_str), Some("v"));
        // Original is preserved per Prometheus semantics; labelmap does not
        // delete the source.
        assert!(out.contains_key("old_foo"));
    }

    #[test]
    fn labeldrop_removes_matching() {
        let mut r = rule(Action::LabelDrop);
        r.regex = Regex::new("^_tmp_.*$").unwrap();
        let mut labels = BTreeMap::new();
        labels.insert("_tmp_a".to_string(), "x".to_string());
        labels.insert("keep_me".to_string(), "y".to_string());
        let out = apply(&[r], labels).unwrap();
        assert!(!out.contains_key("_tmp_a"));
        assert!(out.contains_key("keep_me"));
    }

    #[test]
    fn hashmod_bucketizes() {
        let mut r = rule(Action::HashMod);
        r.source_labels = vec!["x".into()];
        r.modulus = 3;
        r.target_label = Some("bucket".into());
        let mut labels = BTreeMap::new();
        labels.insert("x".to_string(), "foo".to_string());
        let out = apply(&[r], labels).unwrap();
        let b: u64 = out.get("bucket").unwrap().parse().unwrap();
        assert!(b < 3);
    }

    #[test]
    fn keepequal_filters() {
        let mut r = rule(Action::KeepEqual);
        r.source_labels = vec!["a".into()];
        r.target_label = Some("b".into());
        let mut labels = BTreeMap::new();
        labels.insert("a".to_string(), "x".to_string());
        labels.insert("b".to_string(), "y".to_string());
        // a != b → dropped.
        assert!(apply(&[r.clone()], labels.clone()).is_none());
        labels.insert("b".to_string(), "x".to_string());
        assert!(apply(&[r], labels).is_some());
    }
}
