//! Rust port of the Prometheus relabel engine
//! ([github.com/VictoriaMetrics/lib/promrelabel](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/master/lib/promrelabel)).
//!
//! This crate covers `relabel_configs` YAML parsing (defaults,
//! `source_labels`/`regex` scalar-or-list handling, action-specific field
//! validation), the anchored regex wrapper used by every relabel action, the
//! apply engine for every relabel action (including `graphite`), the `if:`
//! selector ([`IfExpression`]), and the [`ParsedConfigs`] public API that
//! ties parsing and applying together.

mod apply;
mod config;
mod graphite;
mod if_expr;
mod label;
mod regex;

pub use apply::apply_one;
pub use config::{parse_relabel_configs, Action, RelabelConfig};
pub use if_expr::IfExpression;
pub use label::Label;
pub use regex::AnchoredRegex;

use std::fmt;

/// Error returned when a `relabel_configs` YAML document, or one of the
/// regexes it contains, fails to parse.
#[derive(Debug, Clone)]
pub struct RelabelError {
    pub msg: String,
}

impl fmt::Display for RelabelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for RelabelError {}

/// A parsed and validated `relabel_configs` YAML document, ready to apply to
/// label sets. Ports `parsedRelabelConfigs`/`ParseRelabelConfigsData`
/// (`lib/promrelabel/relabel.go`), minus the multi-series batching that
/// upstream does for a whole scrape target at once — this crate applies one
/// series' labels at a time (see the task brief for this crate's scope).
pub struct ParsedConfigs {
    /// Each entry pairs a compiled [`RelabelConfig`] with its compiled
    /// `if:` selector, if any.
    cfgs: Vec<(RelabelConfig, Option<IfExpression>)>,
}

impl ParsedConfigs {
    /// Parses `yaml` into fully-validated, `if:`-compiled relabel configs.
    ///
    /// Ports `config.go`'s per-action structural validation (rejecting
    /// e.g. `keep_if_equal` with fewer than 2 `source_labels`, or
    /// `action=hashmod` with no `modulus`) via
    /// [`config::validate_relabel_config`], and compiles each raw `if:`
    /// string into an [`IfExpression`].
    pub fn parse(yaml: &str) -> Result<ParsedConfigs, RelabelError> {
        Self::from_raw_configs(parse_relabel_configs(yaml)?)
    }

    /// Validates and compiles an already-parsed `Vec<RelabelConfig>` (e.g. a
    /// scrape config's `relabel_configs`/`metric_relabel_configs`, parsed
    /// once at config-load time via [`parse_relabel_configs`] and stored as
    /// a plain `Vec<RelabelConfig>`) into a [`ParsedConfigs`], without a
    /// round trip back through YAML. Shares the same validation + `if:`
    /// compilation as [`ParsedConfigs::parse`] — see its doc.
    pub fn from_raw_configs(raw_cfgs: Vec<RelabelConfig>) -> Result<ParsedConfigs, RelabelError> {
        let mut cfgs = Vec::with_capacity(raw_cfgs.len());
        for cfg in raw_cfgs {
            config::validate_relabel_config(&cfg)?;
            let if_expr = match &cfg.if_expr {
                Some(selectors) => Some(IfExpression::parse_list(selectors)?),
                None => None,
            };
            cfgs.push((cfg, if_expr));
        }
        Ok(ParsedConfigs { cfgs })
    }

    /// Applies every config to `labels`, in order, mutating it in place.
    ///
    /// Returns `false` as soon as any `keep`/`drop`-family action drops the
    /// series (later configs are not applied, matching upstream's early
    /// `return nil` from `applyRelabelConfigs`).
    ///
    /// `if:` gating (ports `parsedRelabelConfig.apply`'s
    /// `!prc.If.Match(src)` branch, relabel.go:165-172): a config whose
    /// `if:` selector does not match the current labels is skipped —
    /// EXCEPT for `action: keep`, where an `if:` mismatch drops the series.
    /// This mirrors upstream exactly: `action: keep` gated by `if:` (often
    /// with no `source_labels`/`regex` at all) is vmagent's idiom for "keep
    /// only series matching this selector, drop everything else", so a
    /// non-match must drop rather than no-op.
    pub fn apply(&self, labels: &mut Vec<Label>) -> bool {
        for (cfg, if_expr) in &self.cfgs {
            if let Some(ie) = if_expr {
                if !ie.matches(labels) {
                    if cfg.action == Action::Keep {
                        return false;
                    }
                    continue;
                }
            }
            if !apply_one(cfg, labels) {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Vec<Label> {
        pairs
            .iter()
            .map(|(n, v)| Label {
                name: n.to_string(),
                value: v.to_string(),
            })
            .collect()
    }

    fn get_label_value<'a>(labels: &'a [Label], name: &str) -> &'a str {
        labels
            .iter()
            .find(|l| l.name == name)
            .map(|l| l.value.as_str())
            .unwrap_or("")
    }

    #[test]
    fn if_gates_the_rule() {
        // rule only applies when {__name__="up"}; a "down" series is untouched
        let p = ParsedConfigs::parse(
            r#"
- if: '{__name__="up"}'
  source_labels: [__name__]
  target_label: matched
  replacement: "yes"
  action: replace
"#,
        )
        .unwrap();
        let mut up = labels(&[("__name__", "up")]);
        assert!(p.apply(&mut up));
        assert_eq!(get_label_value(&up, "matched"), "yes");
        let mut down = labels(&[("__name__", "down")]);
        assert!(p.apply(&mut down));
        assert_eq!(get_label_value(&down, "matched"), ""); // rule skipped
    }

    #[test]
    fn full_config_drop_and_relabel_pipeline() {
        let p = ParsedConfigs::parse(
            r#"
- source_labels: [__name__]
  regex: "temp_.*"
  action: drop
- source_labels: [instance]
  target_label: host
  regex: "([^:]+):.*"
  replacement: "$1"
  action: replace
"#,
        )
        .unwrap();
        assert!(!p.apply(&mut labels(&[("__name__", "temp_x")]))); // dropped
        let mut l = labels(&[("__name__", "up"), ("instance", "h1:9090")]);
        assert!(p.apply(&mut l));
        assert_eq!(get_label_value(&l, "host"), "h1");
    }

    #[test]
    fn graphite_action_through_the_full_pipeline() {
        // Derived from lib/promrelabel/relabel_test.go's "graphite-match" case.
        let p = ParsedConfigs::parse(
            r#"
- action: graphite
  match: "foo.*.baz"
  labels:
    job: "${1}-zz"
"#,
        )
        .unwrap();
        let mut l = labels(&[("__name__", "foo.bar.baz")]);
        assert!(p.apply(&mut l));
        assert_eq!(get_label_value(&l, "job"), "bar-zz");

        // Mismatch leaves labels untouched (graphite-mismatch case).
        let mut l2 = labels(&[("__name__", "foo.bar.bazz")]);
        assert!(p.apply(&mut l2));
        assert_eq!(get_label_value(&l2, "job"), "");
    }

    #[test]
    fn if_or_group_matches_either_branch() {
        let p = ParsedConfigs::parse(
            r#"
- if: '{env="prod" or env="staging"}'
  source_labels: [env]
  target_label: gated
  replacement: "yes"
  action: replace
"#,
        )
        .unwrap();
        let mut prod = labels(&[("env", "prod")]);
        assert!(p.apply(&mut prod));
        assert_eq!(get_label_value(&prod, "gated"), "yes");
        let mut staging = labels(&[("env", "staging")]);
        assert!(p.apply(&mut staging));
        assert_eq!(get_label_value(&staging, "gated"), "yes");
        let mut dev = labels(&[("env", "dev")]);
        assert!(p.apply(&mut dev));
        assert_eq!(get_label_value(&dev, "gated"), "");
    }

    #[test]
    fn if_as_list_ors_across_selectors() {
        // `if:` given as a YAML list matches when ANY selector matches.
        let p = ParsedConfigs::parse(
            r#"
- if:
  - '{env="prod"}'
  - '{env="staging"}'
  source_labels: [env]
  target_label: gated
  replacement: "yes"
  action: replace
"#,
        )
        .unwrap();
        let mut prod = labels(&[("env", "prod")]);
        assert!(p.apply(&mut prod));
        assert_eq!(get_label_value(&prod, "gated"), "yes");
        let mut staging = labels(&[("env", "staging")]);
        assert!(p.apply(&mut staging));
        assert_eq!(get_label_value(&staging, "gated"), "yes");
        let mut dev = labels(&[("env", "dev")]);
        assert!(p.apply(&mut dev));
        assert_eq!(get_label_value(&dev, "gated"), ""); // no selector matched
    }

    #[test]
    fn default_equivalent_regex_with_default_replacement_copies_source() {
        // `regex: ".*"` has no capture group, but Go treats it as the default
        // regex `^(.*)$`, so the default `$1` replacement copies the source
        // value into the target label rather than emptying it.
        let p = ParsedConfigs::parse(
            r#"
- source_labels: [foo]
  target_label: bar
  regex: ".*"
"#,
        )
        .unwrap();
        let mut l = labels(&[("foo", "hello")]);
        assert!(p.apply(&mut l));
        assert_eq!(get_label_value(&l, "bar"), "hello");
    }

    #[test]
    fn keep_if_equal_with_one_source_label_is_rejected_at_parse() {
        let result = ParsedConfigs::parse(
            r#"
- action: keep_if_equal
  source_labels: [foo]
"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn if_mismatch_on_keep_action_drops_the_series() {
        // Upstream idiom: `action: keep` gated purely by `if:` (no
        // source_labels/regex) — an `if:` mismatch must drop, not skip.
        let p = ParsedConfigs::parse(
            r#"
- if: '{env="prod"}'
  action: keep
"#,
        )
        .unwrap();
        assert!(p.apply(&mut labels(&[("env", "prod")])));
        assert!(!p.apply(&mut labels(&[("env", "dev")])));
    }
}
