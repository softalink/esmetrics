//! Startup alert-state restore: recovers a `for:` alert's `ActiveAt`
//! progress from the datasource by reading back the `ALERTS_FOR_STATE`
//! series a previous run wrote (see `rule::alert::alert_for_time_series`).
//!
//! Port of `AlertingRule.restore` (`app/vmalert/rule/alerting.go:793-821`),
//! narrowed for this crate:
//! - no `For < 1` / `alerts.is_empty()` early-return gates — a group's
//!   rules haven't been evaluated yet when restore runs at startup, so
//!   `alerts` starts empty; this port seeds a `Pending` [`Alert`] for an
//!   unmatched series rather than only updating pre-existing ones (upstream
//!   assumes `exec` already ran once and skips unmatched series);
//! - no `Restored`/per-rule-label filter beyond `alertname`/`alertgroup` —
//!   this port's [`Alert`] doesn't carry a `Restored` flag (see its doc
//!   comment in `rule::alert`), and additional label filters are out of
//!   scope for this task.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::rule::{
    Alert, AlertState, AlertingRule, Querier, RuleError, ALERT_FOR_STATE_METRIC, ALERT_GROUP_LABEL,
    ALERT_NAME_LABEL,
};

const NAME_LABEL: &str = "__name__";

/// Escapes `\` and `"` so `v` can be embedded in a MetricsQL string-literal
/// label matcher (`label="value"`).
fn escape_label_value(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Builds the `last_over_time(ALERTS_FOR_STATE{...}[Ns])` restore query.
/// Port of the query built at `alerting.go:806-816`, narrowed to the
/// `alertname`/`alertgroup` filters (see module doc) and `last_over_time`
/// in place of upstream's current `default_rollup` (matching this task's
/// brief).
///
/// The `alertgroup` matcher is dropped when `disable_alertgroup_label` is
/// set, so the filter matches the identity labels `build_alert_labels`
/// actually wrote (which also omit `alertgroup` under the same flag). If
/// they disagreed — matcher present but label absent, or vice versa —
/// restore would silently match nothing.
fn build_restore_query(
    rule_name: &str,
    group_name: &str,
    disable_alertgroup_label: bool,
    lookback: Duration,
) -> String {
    let mut filter = format!("{ALERT_NAME_LABEL}=\"{}\"", escape_label_value(rule_name));
    if !disable_alertgroup_label && !group_name.is_empty() {
        filter.push_str(&format!(
            ",{ALERT_GROUP_LABEL}=\"{}\"",
            escape_label_value(group_name)
        ));
    }
    format!(
        "last_over_time({ALERT_FOR_STATE_METRIC}{{{filter}}}[{}s])",
        lookback.as_secs()
    )
}

impl AlertingRule {
    /// Restores each alert's `for:` progress (`active_at`) from the
    /// `ALERTS_FOR_STATE` series a previous run wrote, by querying `q` at
    /// `ts - 1s` (avoids reading data written by the current run, matching
    /// upstream's rationale at `alerting.go:818-819`) and matching results
    /// back to alerts by label-set identity hash ([`AlertingRule::alert_hash`],
    /// the same identity `exec` computes).
    ///
    /// A series matching an alert already present in `self.alerts`
    /// overwrites that alert's `active_at`; an unmatched series seeds a new
    /// `Pending` alert (see module doc for why this port seeds rather than
    /// only updating, unlike upstream). The series value is unix **whole
    /// seconds** (`alerting.go`'s `time.Unix(int64(value), 0)`); since this
    /// crate's `active_at` is unix millis, the value is multiplied by 1000.
    pub fn restore(
        &mut self,
        q: &dyn Querier,
        ts: i64,
        lookback: Duration,
    ) -> Result<(), RuleError> {
        let expr = build_restore_query(
            &self.name,
            &self.group_name,
            self.disable_alertgroup_label,
            lookback,
        );
        let query_ts = ts - 1000;
        let res = q.query(&expr, query_ts).map_err(|e| {
            RuleError::new(format!("failed to execute restore query {expr:?}: {e}"))
        })?;

        for m in &res.data {
            let labels: BTreeMap<String, String> = m
                .labels
                .iter()
                .filter(|(k, _)| k != NAME_LABEL)
                .cloned()
                .collect();
            let value = m.values.first().copied().unwrap_or(0.0);
            let active_at = (value as i64) * 1000;
            let id = AlertingRule::alert_hash(&labels);

            match self.alerts.get_mut(&id) {
                Some(a) => {
                    a.active_at = active_at;
                }
                None => {
                    self.alerts.insert(
                        id,
                        Alert {
                            state: AlertState::Pending,
                            active_at,
                            labels,
                            ..Default::default()
                        },
                    );
                }
            }
            log::info!("alert {:?} restored to state at {active_at}", self.name);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::{DsError, Metric, QueryResult};

    struct RestoreQ;
    impl Querier for RestoreQ {
        fn query(&self, expr: &str, _ts: i64) -> Result<QueryResult, DsError> {
            assert!(expr.contains("ALERTS_FOR_STATE"));
            assert!(expr.contains("last_over_time"));
            Ok(QueryResult {
                data: vec![Metric {
                    labels: vec![
                        ("alertname".into(), "A".into()),
                        ("instance".into(), "h1".into()),
                    ],
                    timestamps: vec![0],
                    values: vec![1_700_000_000.0],
                }],
                is_partial: None,
            })
        }
    }

    #[test]
    fn restores_active_at_from_for_state() {
        let mut ar = AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            ..Default::default()
        };
        ar.restore(&RestoreQ, 1_700_000_100_000, Duration::from_secs(3600))
            .unwrap();
        let a = ar.alerts.values().next().expect("restored alert");
        // Series value is unix whole seconds; active_at is unix millis, so
        // the restored value must be the series value * 1000 (NOT the raw
        // seconds value re-used as millis).
        assert_eq!(a.active_at, 1_700_000_000 * 1000);
    }

    struct TsCheckQ {
        called: std::cell::Cell<bool>,
    }
    impl Querier for TsCheckQ {
        fn query(&self, _expr: &str, ts: i64) -> Result<QueryResult, DsError> {
            self.called.set(true);
            assert_eq!(ts, 1_700_000_099_000);
            Ok(QueryResult {
                data: vec![],
                is_partial: None,
            })
        }
    }

    #[test]
    fn queries_at_ts_minus_one_second() {
        let mut ar = AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            ..Default::default()
        };
        let q = TsCheckQ {
            called: std::cell::Cell::new(false),
        };
        ar.restore(&q, 1_700_000_100_000, Duration::from_secs(3600))
            .unwrap();
        assert!(q.called.get(), "restore never issued a query");
    }

    struct GroupFilterQ {
        called: std::cell::Cell<bool>,
    }
    impl Querier for GroupFilterQ {
        fn query(&self, expr: &str, _ts: i64) -> Result<QueryResult, DsError> {
            self.called.set(true);
            assert!(expr.contains(r#"alertname="A""#));
            assert!(expr.contains(r#"alertgroup="g\"1""#));
            assert!(expr.contains("[3600s]"));
            Ok(QueryResult {
                data: vec![],
                is_partial: None,
            })
        }
    }

    #[test]
    fn query_includes_alertgroup_filter_and_escapes_quotes() {
        let mut ar = AlertingRule {
            name: "A".into(),
            group_name: "g\"1".into(),
            expr: "up".into(),
            ..Default::default()
        };
        let q = GroupFilterQ {
            called: std::cell::Cell::new(false),
        };
        ar.restore(&q, 0, Duration::from_secs(3600)).unwrap();
        assert!(q.called.get(), "restore never issued a query");
    }

    struct NoGroupFilterQ {
        called: std::cell::Cell<bool>,
    }
    impl Querier for NoGroupFilterQ {
        fn query(&self, expr: &str, _ts: i64) -> Result<QueryResult, DsError> {
            self.called.set(true);
            assert!(expr.contains(r#"alertname="A""#));
            assert!(
                !expr.contains("alertgroup"),
                "restore query must omit the alertgroup matcher when disabled: {expr}"
            );
            Ok(QueryResult {
                data: vec![],
                is_partial: None,
            })
        }
    }

    #[test]
    fn query_omits_alertgroup_filter_when_label_disabled() {
        let mut ar = AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            disable_alertgroup_label: true,
            ..Default::default()
        };
        let q = NoGroupFilterQ {
            called: std::cell::Cell::new(false),
        };
        ar.restore(&q, 0, Duration::from_secs(3600)).unwrap();
        assert!(q.called.get(), "restore never issued a query");
    }

    struct SameLabelsQ;
    impl Querier for SameLabelsQ {
        fn query(&self, _expr: &str, _ts: i64) -> Result<QueryResult, DsError> {
            Ok(QueryResult {
                data: vec![Metric {
                    labels: vec![
                        ("alertname".into(), "A".into()),
                        ("alertgroup".into(), "g".into()),
                    ],
                    timestamps: vec![0],
                    values: vec![42.0],
                }],
                is_partial: None,
            })
        }
    }

    #[test]
    fn updates_existing_alert_active_at_instead_of_duplicating() {
        let mut labels = BTreeMap::new();
        labels.insert("alertname".to_string(), "A".to_string());
        labels.insert("alertgroup".to_string(), "g".to_string());
        let id = AlertingRule::alert_hash(&labels);

        let mut ar = AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            ..Default::default()
        };
        ar.alerts.insert(
            id,
            Alert {
                state: AlertState::Pending,
                active_at: 123,
                labels: labels.clone(),
                ..Default::default()
            },
        );

        ar.restore(&SameLabelsQ, 0, Duration::from_secs(3600))
            .unwrap();

        assert_eq!(ar.alerts.len(), 1);
        assert_eq!(ar.alerts.get(&id).unwrap().active_at, 42_000);
    }
}
