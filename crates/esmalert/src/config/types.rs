use std::collections::BTreeMap;
use std::time::Duration;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

/// Top-level rule-group config file. Port of the `cfgFile` shape in
/// `config.go:301-305` (the `groups:` document root).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub groups: Vec<Group>,
}

// `Serialize` on `Group`/`Rule`/`Header` below is Task 9's addition, used
// only by `Group::checksum` (`checksum.rs`) to produce a stable canonical
// serialization to hash. It doesn't need to round-trip byte-for-byte with
// upstream's YAML shape — only to be deterministic and content-sensitive.

/// Port of `Group` (`config.go:25-55`). Only the parse-relevant fields are
/// included; `File`/`Checksum` (computed, not parsed) are out of scope for
/// this task.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Group {
    pub name: String,
    #[serde(rename = "type")]
    pub r#type: Option<String>,
    #[serde(deserialize_with = "deserialize_opt_duration")]
    pub interval: Option<Duration>,
    #[serde(deserialize_with = "deserialize_opt_duration")]
    pub eval_offset: Option<Duration>,
    #[serde(deserialize_with = "deserialize_opt_duration")]
    pub eval_delay: Option<Duration>,
    pub limit: Option<i64>,
    pub concurrency: i64,
    pub labels: BTreeMap<String, String>,
    pub params: BTreeMap<String, Vec<String>>,
    pub headers: Vec<Header>,
    pub notifier_headers: Vec<Header>,
    pub eval_alignment: Option<bool>,
    pub debug: bool,
    pub rules: Vec<Rule>,
}

/// Port of `Rule` (`config.go:133-150`). `record`/`alert` xor-validation and
/// expression checking are Task 9's job; this is parse-only.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Rule {
    pub record: Option<String>,
    pub alert: Option<String>,
    pub expr: String,
    #[serde(rename = "for", deserialize_with = "deserialize_opt_duration")]
    pub r#for: Option<Duration>,
    #[serde(deserialize_with = "deserialize_opt_duration")]
    pub keep_firing_for: Option<Duration>,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub debug: Option<bool>,
    /// Parse-only for now (upstream `config.go:146`); may be wired into rule
    /// state-tracking later. Accepted here so real vmalert rule files that set
    /// it don't get rejected by `deny_unknown_fields`.
    pub update_entries_limit: Option<i64>,
}

/// An HTTP header, parsed from a `"Key: Value"` YAML string. Port of
/// `Header.UnmarshalYAML` (`types.go:122-138`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Header {
    pub key: String,
    pub value: String,
}

impl<'de> Deserialize<'de> for Header {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        // Match upstream: an empty string yields a zero-value Header rather
        // than erroring (types.go:128-130). Only a non-empty string missing
        // the ':' separator is an error.
        if s.is_empty() {
            return Ok(Header::default());
        }
        let n = s.find(':').ok_or_else(|| {
            D::Error::custom(format!(
                "missing ':' in header {s:?}; expecting \"key: value\" format"
            ))
        })?;
        Ok(Header {
            key: s[..n].trim().to_string(),
            value: s[n + 1..].trim().to_string(),
        })
    }
}

/// Parses a Go duration string (`config.go`'s `*promutil.Duration` fields,
/// e.g. `interval: 30s`) via the metricsql duration grammar, matching how
/// vmalert itself parses these — not Go stdlib `time.ParseDuration`.
fn deserialize_opt_duration<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let ms = esm_metricsql::duration_value(&s, 0).map_err(D::Error::custom)?;
    // Reject negatives at parse rather than wrapping into a huge Duration.
    // Deliberate divergence from upstream (which stores signed `time.Duration`
    // and validates eval_offset via `.Abs()`): a negative duration is
    // semantically meaningless here, and keeping the field non-negative lets
    // Task 9's `|eval_offset| < interval` check work on plain Durations.
    if ms < 0 {
        return Err(D::Error::custom(format!(
            "duration must be non-negative, got {s}"
        )));
    }
    Ok(Some(Duration::from_millis(ms as u64)))
}

#[cfg(test)]
mod tests {
    use super::super::parse_config_str;
    use std::time::Duration;

    #[test]
    fn parses_minimal_group() {
        let y = r#"
groups:
  - name: g1
    interval: 30s
    rules:
      - alert: HighLoad
        expr: node_load1 > 5
        for: 2m
        labels: { severity: page }
        annotations: { summary: "load is {{ $value }}" }
"#;
        let c = parse_config_str(y).unwrap();
        assert_eq!(c.groups.len(), 1);
        assert_eq!(c.groups[0].name, "g1");
        assert_eq!(c.groups[0].rules[0].alert.as_deref(), Some("HighLoad"));
        assert_eq!(c.groups[0].rules[0].expr, "node_load1 > 5");
    }

    #[test]
    fn rejects_unknown_field() {
        let y = "groups:\n  - name: g1\n    bogus: 1\n    rules: []\n";
        assert!(parse_config_str(y).is_err());
    }

    #[test]
    fn parses_interval_duration() {
        let y = "groups:\n  - name: g1\n    interval: 30s\n    rules: []\n";
        let c = parse_config_str(y).unwrap();
        assert_eq!(c.groups[0].interval, Some(Duration::from_secs(30)));
    }

    #[test]
    fn rejects_unknown_field_in_rule() {
        let y = "groups:\n  - name: g1\n    rules:\n      - alert: A\n        expr: up\n        bogus: 1\n";
        assert!(parse_config_str(y).is_err());
    }

    #[test]
    fn parses_update_entries_limit() {
        let y = "groups:\n  - name: g1\n    rules:\n      - record: r\n        expr: up\n        update_entries_limit: 42\n";
        let c = parse_config_str(y).unwrap();
        assert_eq!(c.groups[0].rules[0].update_entries_limit, Some(42));
    }

    #[test]
    fn empty_header_string_is_zero_value() {
        let y = "groups:\n  - name: g1\n    headers:\n    - \"\"\n    rules: []\n";
        let c = parse_config_str(y).unwrap();
        assert_eq!(c.groups[0].headers.len(), 1);
        assert_eq!(c.groups[0].headers[0].key, "");
        assert_eq!(c.groups[0].headers[0].value, "");
    }

    #[test]
    fn rejects_negative_duration() {
        let y = "groups:\n  - name: g1\n    eval_offset: -5m\n    rules: []\n";
        assert!(parse_config_str(y).is_err());
    }

    #[test]
    fn parses_header_key_value() {
        let y = "groups:\n  - name: g1\n    headers:\n    - \"X-Foo: bar\"\n    rules: []\n";
        let c = parse_config_str(y).unwrap();
        assert_eq!(c.groups[0].headers.len(), 1);
        assert_eq!(c.groups[0].headers[0].key, "X-Foo");
        assert_eq!(c.groups[0].headers[0].value, "bar");
    }
}
