//! Group/rule config validation: structural checks, MetricsQL expression
//! validation, and Go-template validation for labels/annotations.
//!
//! Port of `Group.Validate` / `Rule.Validate`
//! (`app/vmalert/config/config.go:78-124`, `:218-226`). Rule-type dispatch
//! (upstream supports per-group `graphite`/`vlogs` expression validators via
//! `-rule.defaultRuleType`) is out of scope: MetricsQL is the only supported
//! expression language here, so every rule's `expr` is validated via
//! [`esm_metricsql::parse`].

use std::collections::BTreeMap;
use std::time::Duration;

use super::types::{Group, Rule};
use super::{Config, ConfigError};
use crate::templating::validate_template;

/// Validates every group in `c` via [`validate_group`]. A global
/// `(name, file)` duplicate-group check is out of scope: this crate's config
/// model has no per-group `file` field yet, so that check is single-file
/// scoped upstream anyway and adds no value here (Task 9 brief).
pub fn validate_config(c: &Config) -> Result<(), ConfigError> {
    for g in &c.groups {
        validate_group(g)?;
    }
    Ok(())
}

/// Validates a single group and its rules. Port of `Group.Validate`
/// (`config.go:78-124`).
pub fn validate_group(g: &Group) -> Result<(), ConfigError> {
    if g.name.is_empty() {
        return Err(ConfigError::new("group name must be set"));
    }

    // `interval`/`eval_offset` are already non-negative `Option<Duration>`
    // (Task 8 rejects negative durations at parse time: see
    // `types::deserialize_opt_duration`), so there's no separate "interval
    // shouldn't be lower than 0" check to perform here — the type makes
    // that state unrepresentable. A `None` interval/eval_offset behaves as
    // `0`, mirroring upstream's nil-safe `(*promutil.Duration)(nil).Duration()`.
    let interval = g.interval.unwrap_or(Duration::ZERO);
    let eval_offset = g.eval_offset.unwrap_or(Duration::ZERO);
    if eval_offset > interval {
        return Err(ConfigError::new(format!(
            "the abs value of eval_offset should be smaller than interval; now eval_offset: {eval_offset:?}, interval: {interval:?}"
        )));
    }
    if g.eval_offset.is_some() && g.eval_delay.is_some() {
        return Err(ConfigError::new(
            "eval_offset cannot be used with eval_delay",
        ));
    }
    if let Some(limit) = g.limit {
        if limit < 0 {
            return Err(ConfigError::new(format!(
                "invalid limit {limit}, shouldn't be less than 0"
            )));
        }
    }
    if g.concurrency < 0 {
        return Err(ConfigError::new(format!(
            "invalid concurrency {}, shouldn't be less than 0",
            g.concurrency
        )));
    }

    let mut seen_rules = std::collections::BTreeSet::new();
    for r in &g.rules {
        let rule_name = rule_name(r);
        let identity = rule_identity(r);
        if !seen_rules.insert(identity) {
            return Err(ConfigError::new(format!(
                "rule {rule_name:?} is a duplicate in group"
            )));
        }

        validate_rule(r)
            .map_err(|e| ConfigError::new(format!("invalid rule {rule_name:?}: {e}")))?;
        validate_expr(&r.expr).map_err(|e| {
            ConfigError::new(format!("invalid expression for rule {rule_name:?}: {e}"))
        })?;
        validate_template_map(&r.annotations).map_err(|e| {
            ConfigError::new(format!("invalid annotations for rule {rule_name:?}: {e}"))
        })?;
        validate_template_map(&r.labels)
            .map_err(|e| ConfigError::new(format!("invalid labels for rule {rule_name:?}: {e}")))?;
    }
    Ok(())
}

/// The rule's `record` or `alert` name, matching upstream's `Rule.Name()`
/// (`config.go:169-174`): `record` takes priority, falling back to `alert`.
fn rule_name(r: &Rule) -> &str {
    match &r.record {
        Some(name) if !name.is_empty() => name,
        _ => r.alert.as_deref().unwrap_or(""),
    }
}

/// Rule identity used for duplicate-rule detection. Mirrors upstream's
/// `HashRule` (`config.go:198`), which hashes: `Expr`, then a
/// recording/alerting discriminator plus the `Record`/`Alert` name, then the
/// sorted `(key, value)` label pairs. Two rules that differ only in `expr`
/// therefore have distinct identities (upstream allows both); two rules equal
/// in expr + name + labels collide (a duplicate). `r.labels` is a `BTreeMap`,
/// so iteration is already sorted by key.
pub(crate) fn rule_identity(r: &Rule) -> String {
    let mut s = String::new();
    s.push_str(&r.expr);
    if r.record.as_deref().is_some_and(|v| !v.is_empty()) {
        s.push_str("\x00recording\x00");
        s.push_str(r.record.as_deref().unwrap_or(""));
    } else {
        s.push_str("\x00alerting\x00");
        s.push_str(r.alert.as_deref().unwrap_or(""));
    }
    for (k, v) in &r.labels {
        s.push('\x00');
        s.push_str(k);
        s.push('\x00');
        s.push_str(v);
    }
    s
}

/// Stable identity hash for a rule, over the same [`rule_identity`] byte
/// sequence (expr, then recording/alerting discriminator + name, then
/// sorted label pairs) — FNV-1a 64-bit, the same algorithm
/// `rule::alert::hash_labels` uses (duplicated per this crate's established
/// per-module convention documented on that function). Mirrors upstream's
/// `HashRule` (`config.go:198-215`): two rules are the "same rule" across a
/// reload only if this hash matches, which is exactly what `expr`/labels
/// changing should NOT preserve.
///
/// Used by `manager::build_rule` to give every built `RuleKind` a stable
/// `id`, which `rule::group::Group::apply_update` matches alerting rules on
/// (instead of name alone) to decide whether a hot-reloaded rule keeps its
/// live alert state.
pub(crate) fn rule_identity_hash(r: &Rule) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET_BASIS;
    for b in rule_identity(r).as_bytes() {
        h = (h ^ u64::from(*b)).wrapping_mul(PRIME);
    }
    h
}

/// Structural rule checks. Port of `Rule.Validate` (`config.go:218-226`),
/// minus the `XXX`/`checkOverflow` unknown-field check (already enforced at
/// parse time by `#[serde(deny_unknown_fields)]`).
fn validate_rule(r: &Rule) -> Result<(), ConfigError> {
    let has_record = r.record.as_deref().is_some_and(|s| !s.is_empty());
    let has_alert = r.alert.as_deref().is_some_and(|s| !s.is_empty());
    if has_record == has_alert {
        return Err(ConfigError::new("either `record` or `alert` must be set"));
    }
    if r.expr.is_empty() {
        return Err(ConfigError::new("expression can't be empty"));
    }
    if r.labels.contains_key("__name__") {
        return Err(ConfigError::new("invalid rule label __name__"));
    }
    Ok(())
}

/// Validates `expr` as MetricsQL. A parse error is turned into a plain
/// `String` here; the caller (`validate_group`) wraps it with rule context.
fn validate_expr(expr: &str) -> Result<(), String> {
    esm_metricsql::parse(expr)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Validates every value in a label/annotation map as a Go template, with the
/// vmalert variable preamble ([`crate::templating::TPL_HEADERS`]) prepended so
/// `{{ $value }}` / `{{ $labels.instance }}` and friends resolve.
///
/// The template engine does not support Go's time/duration method-call
/// syntax (`.Sub`/`.Add`/`.UnixMilli` on time values, deferred out of
/// `esm_gotemplate`'s scope). Such templates will fail here with a plain
/// parse/validate error; that's the intended behavior for the deferred
/// feature, not a bug to work around.
fn validate_template_map(m: &BTreeMap<String, String>) -> Result<(), String> {
    for (key, value) in m {
        validate_template(value).map_err(|e| format!("{key}: {e}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::types::Rule;

    fn alerting_rule(alert: &str, expr: &str) -> Rule {
        Rule {
            alert: Some(alert.to_string()),
            expr: expr.to_string(),
            ..Default::default()
        }
    }

    fn group_with_rules(rules: Vec<Rule>) -> Group {
        Group {
            name: "g".into(),
            rules,
            ..Default::default()
        }
    }

    #[test]
    fn accepts_a_valid_group() {
        let g = group_with_rules(vec![Rule {
            alert: Some("HighLoad".into()),
            expr: "node_load1 > 5".into(),
            labels: [("severity".to_string(), "page".to_string())]
                .into_iter()
                .collect(),
            annotations: [("summary".to_string(), "load is high".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        }]);
        validate_group(&g).unwrap();
    }

    #[test]
    fn accepts_annotation_using_alert_template_variables() {
        // Realistic annotation: the vmalert preamble (`TPL_HEADERS`) declares
        // `$value`/`$labels`, so this now validates (previously it failed as
        // "undefined variable"). See FIX A.
        let g = group_with_rules(vec![Rule {
            alert: Some("a".into()),
            expr: "up".into(),
            annotations: [(
                "summary".to_string(),
                "value is {{ $value }} on {{ $labels.instance }}".to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        }]);
        validate_group(&g).unwrap();
    }

    #[test]
    fn accepts_label_using_alert_template_variables() {
        let g = group_with_rules(vec![Rule {
            alert: Some("a".into()),
            expr: "up".into(),
            labels: [("host".to_string(), "{{ $labels.instance }}".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        }]);
        validate_group(&g).unwrap();
    }

    #[test]
    fn rejects_record_and_alert_both_set() {
        let g = Group {
            name: "g".into(),
            rules: vec![Rule {
                record: Some("r".into()),
                alert: Some("a".into()),
                expr: "x".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let e = validate_group(&g).unwrap_err();
        assert!(e.to_string().contains("record") && e.to_string().contains("alert"));
    }

    #[test]
    fn rejects_bad_metricsql_expr() {
        let g = Group {
            name: "g".into(),
            rules: vec![Rule {
                alert: Some("a".into()),
                expr: "sum(".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(validate_group(&g).is_err());
    }

    #[test]
    fn rejects_bad_template_annotation() {
        let g = Group {
            name: "g".into(),
            rules: vec![Rule {
                alert: Some("a".into()),
                expr: "up".into(),
                annotations: [("summary".to_string(), "{{ nope }".to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(validate_group(&g).is_err());
    }

    #[test]
    fn rejects_bad_template_label() {
        let g = group_with_rules(vec![Rule {
            alert: Some("a".into()),
            expr: "up".into(),
            labels: [("env".to_string(), "{{ nope }".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        }]);
        assert!(validate_group(&g).is_err());
    }

    #[test]
    fn rejects_eval_offset_larger_than_interval() {
        let mut g = group_with_rules(vec![]);
        g.interval = Some(Duration::from_secs(60));
        g.eval_offset = Some(Duration::from_secs(120));
        let e = validate_group(&g).unwrap_err();
        assert!(e.to_string().contains("eval_offset"));
    }

    #[test]
    fn accepts_eval_offset_equal_to_interval() {
        let mut g = group_with_rules(vec![]);
        g.interval = Some(Duration::from_secs(60));
        g.eval_offset = Some(Duration::from_secs(60));
        validate_group(&g).unwrap();
    }

    #[test]
    fn rejects_eval_offset_combined_with_eval_delay() {
        let mut g = group_with_rules(vec![]);
        g.interval = Some(Duration::from_secs(60));
        g.eval_offset = Some(Duration::from_secs(30));
        g.eval_delay = Some(Duration::from_secs(10));
        let e = validate_group(&g).unwrap_err();
        assert!(e.to_string().contains("eval_offset") && e.to_string().contains("eval_delay"));
    }

    #[test]
    fn rejects_negative_limit() {
        let mut g = group_with_rules(vec![]);
        g.limit = Some(-1);
        assert!(validate_group(&g).is_err());
    }

    #[test]
    fn rejects_negative_concurrency() {
        let mut g = group_with_rules(vec![]);
        g.concurrency = -1;
        assert!(validate_group(&g).is_err());
    }

    #[test]
    fn rejects_dunder_name_rule_label() {
        let g = group_with_rules(vec![Rule {
            alert: Some("a".into()),
            expr: "up".into(),
            labels: [("__name__".to_string(), "x".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        }]);
        let e = validate_group(&g).unwrap_err();
        assert!(e.to_string().contains("__name__"));
    }

    #[test]
    fn rejects_duplicate_rule_in_group() {
        // Two truly identical rules (same alert name + expr + labels) collide.
        let g = group_with_rules(vec![alerting_rule("Dup", "up"), alerting_rule("Dup", "up")]);
        let e = validate_group(&g).unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("duplicate"));
        // The user-facing message names the rule and must not leak the
        // internal NUL-delimited identity string.
        assert!(msg.contains("Dup"));
        assert!(!msg.contains('\0'));
    }

    #[test]
    fn same_name_and_labels_but_different_expr_is_not_a_duplicate() {
        // FIX B: upstream `HashRule` folds `expr` into rule identity, so two
        // rules with the same alert name (and labels) but different `expr` are
        // distinct — both must be allowed.
        let g = group_with_rules(vec![
            alerting_rule("Dup", "up"),
            alerting_rule("Dup", "up == 0"),
        ]);
        validate_group(&g).unwrap();
    }

    #[test]
    fn same_expr_but_recording_vs_alerting_is_not_a_duplicate() {
        // The recording/alerting discriminator is part of the identity too.
        let g = group_with_rules(vec![
            Rule {
                record: Some("x".into()),
                expr: "up".into(),
                ..Default::default()
            },
            alerting_rule("x", "up"),
        ]);
        validate_group(&g).unwrap();
    }

    #[test]
    fn rejects_empty_group_name() {
        let g = group_with_rules(vec![]);
        let mut g = g;
        g.name = String::new();
        let e = validate_group(&g).unwrap_err();
        assert!(e.to_string().contains("name"));
    }

    #[test]
    fn rejects_empty_expr() {
        let g = group_with_rules(vec![alerting_rule("a", "")]);
        assert!(validate_group(&g).is_err());
    }

    #[test]
    fn rule_identity_hash_stable_and_sensitive_to_expr() {
        let a = alerting_rule("A", "up");
        let b = alerting_rule("A", "up");
        let c = alerting_rule("A", "up > 1");
        assert_eq!(
            rule_identity_hash(&a),
            rule_identity_hash(&b),
            "identical rules must hash to the same id"
        );
        assert_ne!(
            rule_identity_hash(&a),
            rule_identity_hash(&c),
            "a changed expr must change the identity hash"
        );
    }

    #[test]
    fn rule_identity_hash_sensitive_to_labels_not_annotations() {
        let mut with_label = alerting_rule("A", "up");
        with_label.labels = [("env".to_string(), "prod".to_string())]
            .into_iter()
            .collect();
        let base = alerting_rule("A", "up");
        assert_ne!(
            rule_identity_hash(&base),
            rule_identity_hash(&with_label),
            "a changed label set must change the identity hash"
        );

        let mut with_annotation = alerting_rule("A", "up");
        with_annotation.annotations = [("summary".to_string(), "x".to_string())]
            .into_iter()
            .collect();
        assert_eq!(
            rule_identity_hash(&base),
            rule_identity_hash(&with_annotation),
            "annotations aren't part of rule identity (matches upstream's HashRule)"
        );
    }

    #[test]
    fn validate_config_runs_validate_group_for_each_group() {
        let good = group_with_rules(vec![alerting_rule("a", "up")]);
        let mut bad = group_with_rules(vec![]);
        bad.name = String::new();
        let c = Config {
            groups: vec![good, bad],
        };
        assert!(validate_config(&c).is_err());
    }
}
