//! YAML schema for `esmalert-tool` unit test files.
//!
//! Port of VictoriaMetrics `vmalert-tool`'s `unitTestFile` / `testGroup` /
//! `alertTestCase` / `metricsqlTestCase` structs
//! (`app/vmalert-tool/unittest/unittest.go:477-508`, `alerting.go`,
//! `input.go`, `recording.go`).
//!
//! Unlike the esmalert rule-config schema, the test-file schema is lenient
//! about unknown fields (upstream applies no strict overflow check here), so
//! none of these structs use `#[serde(deny_unknown_fields)]`.

// Scaffold stage: these fields are populated by `serde` deserialization but
// not yet read by application code — the input parser, harness, and runner
// that consume them land in later tasks.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Deserializer};

use crate::ToolError;

/// Top-level unit test file (`unitTestFile` in upstream).
#[derive(Debug, Deserialize)]
pub struct UnitTestFile {
    #[serde(default)]
    pub rule_files: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_opt_duration")]
    pub evaluation_interval: Option<Duration>,
    #[serde(default)]
    pub group_eval_order: Vec<String>,
    #[serde(default)]
    pub tests: Vec<TestGroup>,
}

/// A group of input series and test cases (`testGroup` in upstream).
#[derive(Debug, Deserialize)]
pub struct TestGroup {
    #[serde(default, deserialize_with = "deserialize_opt_duration")]
    pub interval: Option<Duration>,
    #[serde(default)]
    pub input_series: Vec<Series>,
    #[serde(default)]
    pub alert_rule_test: Vec<AlertTestCase>,
    #[serde(default)]
    pub metricsql_expr_test: Vec<MetricsqlTestCase>,
    #[serde(default)]
    pub external_labels: BTreeMap<String, String>,
    pub name: String,
}

/// A single input series (`series` in upstream).
#[derive(Debug, Deserialize)]
pub struct Series {
    pub series: String,
    pub values: String,
}

/// An `alert_rule_test` case (`alertTestCase` in upstream).
#[derive(Debug, Deserialize)]
pub struct AlertTestCase {
    #[serde(deserialize_with = "deserialize_duration")]
    pub eval_time: Duration,
    pub groupname: String,
    pub alertname: String,
    #[serde(default)]
    pub exp_alerts: Vec<ExpAlert>,
}

/// An expected alert (`expAlert` in upstream).
#[derive(Debug, Deserialize)]
pub struct ExpAlert {
    #[serde(default)]
    pub exp_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub exp_annotations: BTreeMap<String, String>,
}

/// A `metricsql_expr_test` case (`metricsqlTestCase` in upstream).
#[derive(Debug, Deserialize)]
pub struct MetricsqlTestCase {
    pub expr: String,
    #[serde(deserialize_with = "deserialize_duration")]
    pub eval_time: Duration,
    #[serde(default)]
    pub exp_samples: Vec<ExpSample>,
}

/// An expected sample (`expSample` in upstream).
#[derive(Debug, Deserialize)]
pub struct ExpSample {
    pub labels: String,
    pub value: f64,
}

/// Parses a Go duration string into milliseconds via the metricsql duration
/// grammar (same approach as `esmalert::config::types::deserialize_opt_duration`),
/// rejecting negative durations.
fn duration_from_str<E: serde::de::Error>(s: &str) -> Result<Duration, E> {
    let ms = esm_metricsql::duration_value(s, 0).map_err(E::custom)?;
    if ms < 0 {
        return Err(E::custom(format!("duration must be non-negative, got {s}")));
    }
    Ok(Duration::from_millis(ms as u64))
}

/// Deserializes a required Go duration string field (e.g. `eval_time`).
fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    duration_from_str(&s)
}

/// Deserializes an optional Go duration string field (upstream uses a
/// `*promutil.Duration` pointer for these; we model absence as `None`).
fn deserialize_opt_duration<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        None => Ok(None),
        Some(s) => duration_from_str(&s).map(Some),
    }
}

/// Parses a `esmalert-tool` unit test file from its YAML text.
pub fn parse_test_file(yaml: &str) -> Result<UnitTestFile, ToolError> {
    serde_yaml_ng::from_str(yaml)
        .map_err(|e| ToolError::new(format!("failed to parse test file: {e}")))
}

#[cfg(test)]
mod tests {
    use super::parse_test_file;
    use std::time::Duration;

    #[test]
    fn parses_a_unittest_file() {
        let y = r#"
rule_files:
  - alerts.yml
evaluation_interval: 1m
tests:
  - interval: 1m
    name: t1
    input_series:
      - series: 'up{job="x"}'
        values: '0 0 0 1 1'
    alert_rule_test:
      - eval_time: 4m
        groupname: g
        alertname: InstanceDown
        exp_alerts:
          - exp_labels: { severity: page, job: x }
            exp_annotations: { summary: "down" }
    metricsql_expr_test:
      - expr: up
        eval_time: 4m
        exp_samples:
          - labels: 'up{job="x"}'
            value: 1
"#;
        let f = parse_test_file(y).unwrap();
        assert_eq!(f.rule_files, vec!["alerts.yml".to_string()]);
        assert_eq!(f.evaluation_interval, Some(Duration::from_secs(60)));
        assert_eq!(f.tests[0].input_series[0].series, r#"up{job="x"}"#);
        assert_eq!(
            f.tests[0].alert_rule_test[0].eval_time,
            Duration::from_secs(240)
        );
        assert_eq!(
            f.tests[0].alert_rule_test[0].exp_alerts[0].exp_labels["severity"],
            "page"
        );
        assert_eq!(f.tests[0].metricsql_expr_test[0].exp_samples[0].value, 1.0);
    }

    #[test]
    fn rejects_negative_duration() {
        // Mirrors esmalert::config::types::rejects_negative_duration: a
        // negative Go duration must error at parse, not wrap into a huge
        // Duration.
        let y = r#"
tests:
  - name: t1
    alert_rule_test:
      - eval_time: -5m
        groupname: g
        alertname: A
"#;
        assert!(parse_test_file(y).is_err());
    }
}
