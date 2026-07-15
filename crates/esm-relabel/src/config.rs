//! `relabel_configs` YAML parsing, ported from `lib/promrelabel/config.go`.
//!
//! Scope: defaulting + the `source_labels`/`regex` scalar-or-list handling
//! (`MultiLineRegex` in the Go source) + anchored regex compilation +
//! action-specific field validation. `if:` accepts a scalar or a list of
//! selectors (kept as raw `Option<Vec<String>>` strings here); its
//! compilation into an [`crate::IfExpression`] and the apply engine are built
//! on top of this in the [`crate::ParsedConfigs`] layer.

use crate::regex::AnchoredRegex;
use crate::RelabelError;
use serde::Deserialize;

const DEFAULT_REGEX: &str = "(.*)";
const DEFAULT_SEPARATOR: &str = ";";
const DEFAULT_REPLACEMENT: &str = "$1";

/// The relabel action to apply. YAML values match `lib/promrelabel/config.go`
/// exactly, including the two actions (`keepequal`/`dropequal`) that break
/// from the otherwise-consistent snake_case naming.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    #[default]
    Replace,
    ReplaceAll,
    Keep,
    Drop,
    KeepEqual,
    DropEqual,
    KeepIfEqual,
    DropIfEqual,
    KeepIfContains,
    DropIfContains,
    KeepMetrics,
    DropMetrics,
    Labelmap,
    LabelmapAll,
    Labeldrop,
    Labelkeep,
    Hashmod,
    Lowercase,
    Uppercase,
    Graphite,
}

impl Action {
    /// Maps a YAML action string to an [`Action`]. Matching is case-insensitive,
    /// porting Go's `action := strings.ToLower(rc.Action)` (config.go:216).
    fn from_yaml(s: &str) -> Result<Action, RelabelError> {
        let action = match s.to_ascii_lowercase().as_str() {
            "replace" => Action::Replace,
            "replace_all" => Action::ReplaceAll,
            "keep" => Action::Keep,
            "drop" => Action::Drop,
            "keepequal" => Action::KeepEqual,
            "dropequal" => Action::DropEqual,
            "keep_if_equal" => Action::KeepIfEqual,
            "drop_if_equal" => Action::DropIfEqual,
            "keep_if_contains" => Action::KeepIfContains,
            "drop_if_contains" => Action::DropIfContains,
            "keep_metrics" => Action::KeepMetrics,
            "drop_metrics" => Action::DropMetrics,
            "labelmap" => Action::Labelmap,
            "labelmap_all" => Action::LabelmapAll,
            "labeldrop" => Action::Labeldrop,
            "labelkeep" => Action::Labelkeep,
            "hashmod" => Action::Hashmod,
            "lowercase" => Action::Lowercase,
            "uppercase" => Action::Uppercase,
            "graphite" => Action::Graphite,
            other => {
                return Err(RelabelError {
                    msg: format!("unknown `action` {other:?}"),
                })
            }
        };
        Ok(action)
    }
}

/// A single compiled `relabel_config` entry with all defaults applied.
#[derive(Debug, Clone)]
pub struct RelabelConfig {
    pub source_labels: Vec<String>,
    pub separator: String,
    pub target_label: String,
    pub regex: AnchoredRegex,
    pub modulus: u64,
    pub replacement: String,
    pub action: Action,
    /// The `if:` selectors, each a metricsql series selector. Multiple
    /// selectors OR together (`if:` accepts a scalar or a YAML list, ported
    /// from `if_expression.go`'s `unmarshalFromInterface`).
    pub if_expr: Option<Vec<String>>,
    /// `match` — the `*`-glob template for `action: graphite`.
    pub graphite_match: Option<String>,
    /// `labels` — the `target_label -> replace_template` map for
    /// `action: graphite`.
    pub graphite_labels: std::collections::BTreeMap<String, String>,
}

/// Accepts either a scalar or a sequence in YAML, mirroring the `any` handling
/// in Go's `MultiLineRegex.UnmarshalYAML` / plain `source_labels` decoding.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StringOrSeq {
    Single(String),
    Seq(Vec<String>),
}

/// Raw, undefaulted shape of a single YAML `relabel_config` list entry.
#[derive(Debug, Default, Deserialize)]
struct RawRelabelConfig {
    #[serde(
        default,
        rename = "if",
        deserialize_with = "deserialize_opt_string_or_seq"
    )]
    if_expr: Option<Vec<String>>,
    #[serde(default)]
    action: Option<String>,
    #[serde(default, deserialize_with = "deserialize_string_or_seq")]
    source_labels: Vec<String>,
    #[serde(default)]
    separator: Option<String>,
    #[serde(default)]
    target_label: String,
    #[serde(default, deserialize_with = "deserialize_joined_string_or_seq")]
    regex: Option<String>,
    #[serde(default)]
    modulus: u64,
    #[serde(default)]
    replacement: Option<String>,
    #[serde(default, rename = "match")]
    graphite_match: Option<String>,
    #[serde(default)]
    labels: std::collections::BTreeMap<String, String>,
}

fn deserialize_string_or_seq<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<StringOrSeq> = Option::deserialize(deserializer)?;
    Ok(match opt {
        None => Vec::new(),
        Some(StringOrSeq::Single(s)) => vec![s],
        Some(StringOrSeq::Seq(v)) => v,
    })
}

/// Accepts a scalar or a sequence for `if:`, mirroring Go's
/// `IfExpression.unmarshalFromInterface` (string OR `[]any`). Returns `None`
/// only when the key is absent; a present scalar becomes a single-element
/// list.
fn deserialize_opt_string_or_seq<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<StringOrSeq> = Option::deserialize(deserializer)?;
    Ok(match opt {
        None => None,
        Some(StringOrSeq::Single(s)) => Some(vec![s]),
        Some(StringOrSeq::Seq(v)) => Some(v),
    })
}

/// Ports `MultiLineRegex`: a YAML sequence is joined with `|` into a single pattern.
fn deserialize_joined_string_or_seq<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<StringOrSeq> = Option::deserialize(deserializer)?;
    Ok(match opt {
        None => None,
        Some(StringOrSeq::Single(s)) => Some(s),
        Some(StringOrSeq::Seq(v)) => Some(v.join("|")),
    })
}

/// Parses a `relabel_configs` YAML document (a list of relabel config
/// entries) into fully-defaulted `RelabelConfig`s.
pub fn parse_relabel_configs(yaml: &str) -> Result<Vec<RelabelConfig>, RelabelError> {
    let raws: Vec<RawRelabelConfig> = serde_yaml_ng::from_str(yaml).map_err(|e| RelabelError {
        msg: format!("cannot parse relabel_configs: {e}"),
    })?;
    raws.into_iter().map(into_relabel_config).collect()
}

fn into_relabel_config(raw: RawRelabelConfig) -> Result<RelabelConfig, RelabelError> {
    let separator = raw
        .separator
        .unwrap_or_else(|| DEFAULT_SEPARATOR.to_string());
    let replacement = raw
        .replacement
        .unwrap_or_else(|| DEFAULT_REPLACEMENT.to_string());
    let action = match raw.action {
        Some(s) => Action::from_yaml(&s)?,
        None => Action::default(),
    };

    // `keep_metrics`/`drop_metrics` require a non-empty `regex` (unless gated
    // by `if:`) and forbid `source_labels`, ported from the `switch action`
    // block in Go's `parseRelabelConfig` (config.go:363-380). This has to be
    // decided from the RAW config, before `regex` is defaulted to `(.*)` —
    // once defaulted, "was the regex empty?" is no longer decidable.
    if matches!(action, Action::KeepMetrics | Action::DropMetrics) {
        let action_name = if action == Action::KeepMetrics {
            "keep_metrics"
        } else {
            "drop_metrics"
        };
        let regex_is_empty = raw.regex.as_deref().unwrap_or("").is_empty();
        if regex_is_empty && raw.if_expr.is_none() {
            return Err(RelabelError {
                msg: format!("`regex` must be non-empty for `action={action_name}`"),
            });
        }
        if !raw.source_labels.is_empty() {
            return Err(RelabelError {
                msg: format!("`source_labels` must be empty for `action={action_name}`"),
            });
        }
    }

    let regex_pattern = raw.regex.unwrap_or_else(|| DEFAULT_REGEX.to_string());
    let regex = compile_relabel_regex(&regex_pattern)?;

    Ok(RelabelConfig {
        source_labels: raw.source_labels,
        separator,
        target_label: raw.target_label,
        regex,
        modulus: raw.modulus,
        replacement,
        action,
        if_expr: raw.if_expr,
        graphite_match: raw.graphite_match,
        graphite_labels: raw.labels,
    })
}

/// Compiles a relabel-config `regex`, normalizing any default-equivalent
/// pattern (e.g. `.*`, `(.*)`, `(?s:.*)`) to the canonical default `(.*)` so
/// its anchored form always exposes capture group 1 spanning the whole
/// string. This mirrors Go's `parseRelabelConfig`, which maps any
/// `isDefaultRegex` pattern to `defaultRegexForRelabelConfig` (`^(.*)$`) —
/// without it, an explicit no-capture-group default like `regex: ".*"` paired
/// with the default `$1` replacement would expand to an empty target label
/// instead of copying the source value. The user's original pattern string is
/// preserved for the unanchored `replace_all`/`labelmap_all` recompile path.
fn compile_relabel_regex(pattern: &str) -> Result<AnchoredRegex, RelabelError> {
    if is_default_regex(pattern) {
        let mut re = AnchoredRegex::compile(DEFAULT_REGEX)?;
        re.original = pattern.to_string();
        Ok(re)
    } else {
        AnchoredRegex::compile(pattern)
    }
}

/// Ports Go's `isDefaultRegex` (config.go:434): a regex is default-equivalent
/// iff it simplifies to a match-everything pattern with no literal prefix.
fn is_default_regex(expr: &str) -> bool {
    let (prefix, suffix) = esm_common::regexutil::simplify_prom_regex(expr);
    prefix.is_empty() && suffix == "(?s:.*)"
}

/// Cheap structural validation for action-specific field requirements,
/// ported from the `switch action` block in Go's `parseRelabelConfig`
/// (config.go:260-402). Only checks that are decidable from the fully
/// defaulted [`RelabelConfig`] are ported here (e.g. "is `source_labels`
/// non-empty", not "was `regex` present in the YAML before defaulting").
pub(crate) fn validate_relabel_config(cfg: &RelabelConfig) -> Result<(), RelabelError> {
    let has_source_labels = !cfg.source_labels.is_empty();
    let has_target_label = !cfg.target_label.is_empty();
    match cfg.action {
        Action::Replace => {
            if !has_target_label {
                return err("missing `target_label` for `action=replace`");
            }
        }
        Action::ReplaceAll => {
            if !has_source_labels {
                return err("missing `source_labels` for `action=replace_all`");
            }
            if !has_target_label {
                return err("missing `target_label` for `action=replace_all`");
            }
        }
        Action::KeepIfContains | Action::DropIfContains => {
            if !has_target_label {
                return err(
                    "`target_label` must be set for `action=keep_if_contains`/`drop_if_contains`",
                );
            }
            if !has_source_labels {
                return err(
                    "`source_labels` must contain at least a single entry for `action=keep_if_contains`/`drop_if_contains`",
                );
            }
        }
        Action::KeepIfEqual | Action::DropIfEqual => {
            if cfg.source_labels.len() < 2 {
                return err(
                    "`source_labels` must contain at least two entries for `action=keep_if_equal`/`drop_if_equal`",
                );
            }
            if has_target_label {
                return err(
                    "`target_label` cannot be used for `action=keep_if_equal`/`drop_if_equal`",
                );
            }
        }
        Action::KeepEqual | Action::DropEqual => {
            if !has_target_label {
                return err("missing `target_label` for `action=keepequal`/`dropequal`");
            }
        }
        Action::Keep | Action::Drop => {
            if !has_source_labels && cfg.if_expr.is_none() {
                return err("missing `source_labels` for `action=keep`/`drop`");
            }
        }
        Action::Hashmod => {
            if !has_source_labels {
                return err("missing `source_labels` for `action=hashmod`");
            }
            if !has_target_label {
                return err("missing `target_label` for `action=hashmod`");
            }
            if cfg.modulus < 1 {
                return err("unexpected `modulus` for `action=hashmod`: must be greater than 0");
            }
        }
        Action::Uppercase | Action::Lowercase => {
            if !has_source_labels {
                return err("missing `source_labels` for `action=uppercase`/`action=lowercase`");
            }
            if !has_target_label {
                return err("missing `target_label` for `action=uppercase`/`action=lowercase`");
            }
        }
        Action::Graphite => {
            if cfg.graphite_match.is_none() {
                return err("missing `match` for `action=graphite`");
            }
            if cfg.graphite_labels.is_empty() {
                return err("missing `labels` for `action=graphite`");
            }
            if has_source_labels {
                return err("`source_labels` cannot be used with `action=graphite`");
            }
            if has_target_label {
                return err("`target_label` cannot be used with `action=graphite`");
            }
        }
        Action::KeepMetrics
        | Action::DropMetrics
        | Action::Labelmap
        | Action::LabelmapAll
        | Action::Labeldrop
        | Action::Labelkeep => {}
    }
    if !matches!(cfg.action, Action::Graphite) {
        if cfg.graphite_match.is_some() {
            return err("`match` config cannot be applied to this action; it is applied only to `action=graphite`");
        }
        if !cfg.graphite_labels.is_empty() {
            return err("`labels` config cannot be applied to this action; it is applied only to `action=graphite`");
        }
    }
    Ok(())
}

fn err(msg: &str) -> Result<(), RelabelError> {
    Err(RelabelError {
        msg: msg.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_relabel_config() {
        let y = r#"
- source_labels: [__name__, job]
  separator: "/"
  regex: "(.+)/(.+)"
  target_label: combined
  replacement: "$1-$2"
  action: replace
"#;
        let cfgs = parse_relabel_configs(y).unwrap();
        assert_eq!(cfgs.len(), 1);
        assert_eq!(
            cfgs[0].source_labels,
            vec!["__name__".to_string(), "job".to_string()]
        );
        assert_eq!(cfgs[0].separator, "/");
        assert!(matches!(cfgs[0].action, Action::Replace));
        assert_eq!(cfgs[0].target_label, "combined");
    }

    #[test]
    fn action_matching_is_case_insensitive() {
        let keep = parse_relabel_configs("- action: Keep\n  source_labels: [x]\n").unwrap();
        assert_eq!(keep[0].action, Action::Keep);
        let drop = parse_relabel_configs("- action: DROP\n  source_labels: [x]\n").unwrap();
        assert_eq!(drop[0].action, Action::Drop);
    }

    #[test]
    fn regex_list_is_joined_with_pipe() {
        let cfgs = parse_relabel_configs("- regex: [foo, bar]\n  target_label: x\n").unwrap();
        assert_eq!(cfgs[0].regex.original, "foo|bar");
    }

    #[test]
    fn if_accepts_a_scalar() {
        let cfgs = parse_relabel_configs("- if: '{env=\"prod\"}'\n  action: keep\n").unwrap();
        assert_eq!(cfgs[0].if_expr.as_deref().unwrap(), &[r#"{env="prod"}"#]);
    }

    #[test]
    fn if_accepts_a_yaml_list() {
        let cfgs = parse_relabel_configs(
            "- if:\n  - '{env=\"prod\"}'\n  - '{env=\"staging\"}'\n  action: keep\n",
        )
        .unwrap();
        assert_eq!(
            cfgs[0].if_expr.as_deref().unwrap(),
            &[
                r#"{env="prod"}"#.to_string(),
                r#"{env="staging"}"#.to_string()
            ]
        );
    }

    #[test]
    fn keep_metrics_requires_non_empty_regex() {
        assert!(parse_relabel_configs("- action: keep_metrics\n").is_err());
        assert!(parse_relabel_configs("- action: drop_metrics\n").is_err());
    }

    #[test]
    fn keep_metrics_with_regex_is_ok() {
        assert!(parse_relabel_configs("- action: keep_metrics\n  regex: 'foo.*'\n").is_ok());
    }

    #[test]
    fn keep_metrics_rejects_source_labels() {
        assert!(parse_relabel_configs(
            "- action: keep_metrics\n  regex: 'foo.*'\n  source_labels: [__name__]\n"
        )
        .is_err());
    }

    #[test]
    fn empty_regex_default_equivalent_gets_capture_group() {
        // An explicit `.*` (no capture group) is normalized to the canonical
        // default `(.*)` so its anchored form exposes group 1.
        let cfgs = parse_relabel_configs("- regex: '.*'\n  target_label: x\n").unwrap();
        assert_eq!(cfgs[0].regex.original, ".*");
        // group 1 spans the whole string, so `$1` expands to the source.
        assert_eq!(cfgs[0].regex.replace_all("hello", "$1"), "hello");
    }

    #[test]
    fn defaults_applied() {
        let cfgs =
            parse_relabel_configs("- action: uppercase\n  source_labels: [x]\n  target_label: x\n")
                .unwrap();
        assert_eq!(cfgs[0].separator, ";");
        assert_eq!(cfgs[0].replacement, "$1");
        assert_eq!(cfgs[0].regex.original, "(.*)");
    }
}
