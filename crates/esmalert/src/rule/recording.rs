//! Recording-rule evaluation. Port of `app/vmalert/rule/recording.go:1-58`
//! (`RecordingRule` + `exec`) and the label/series construction in
//! `toTimeSeries` (`:285-320`) / `newTimeSeries` (`utils.go:16`).

use std::collections::BTreeMap;
use std::collections::HashSet;

use crate::datasource::Metric;
use crate::series::{Sample, Series};

use super::labels::{labels_to_key, merge_labels};
use super::{Querier, RuleError};

/// Upstream `errDuplicate` message (`rule/rule.go:40`), embedded verbatim so
/// the duplicate-series error matches vmalert.
const ERR_DUPLICATE: &str = "result contains metrics with the same labelset during evaluation. See https://docs.victoriametrics.com/victoriametrics/vmalert/#series-with-the-same-labelset for details";

/// A rule that evaluates an expression and emits its result as a set of time
/// series (to be persisted via remote-write).
///
/// Port of `RecordingRule` (`recording.go:25-45`), narrowed to the fields this
/// port needs; VictoriaMetrics-only bookkeeping (metrics, group/file IDs,
/// `ruleState`) is out of scope here.
#[derive(Debug, Default)]
pub struct RecordingRule {
    /// Stable rule identity (see `config::rule_identity_hash`), stored so a
    /// rule's live [`super::RuleView`] can carry the same id the manager
    /// keys on. Recording rules don't use it for state preservation (they
    /// carry no live state), but exposing it keeps the JSON-API rule view
    /// symmetric with alerting rules.
    pub id: u64,
    pub name: String,
    pub expr: String,
    pub labels: BTreeMap<String, String>,
    /// Health of this rule's most recent `exec`, published in the live
    /// [`super::GroupSnapshot`]. `Unknown` until the first evaluation.
    pub health: super::RuleHealth,
    /// The most recent `exec` error's message, if the last evaluation
    /// failed (`None` once it succeeds). Recorded by `Group::eval_once`.
    pub last_error: Option<String>,
}

impl RecordingRule {
    /// Evaluates the rule's expression at `ts` via `q`, returning one owned
    /// [`Series`] per result metric.
    ///
    /// Port of `RecordingRule.exec` (`recording.go:187-244`): query at `ts`;
    /// error if `limit > 0 && num_series > limit`; build each series
    /// (`__name__` = record name, rule labels overlaid); error on a duplicate
    /// label set. Stale-series emission (upstream's `lastEvaluation` diff) is
    /// not ported here.
    pub fn exec(&mut self, q: &dyn Querier, ts: i64, limit: i64) -> Result<Vec<Series>, RuleError> {
        let res = q
            .query(&self.expr, ts)
            .map_err(|e| RuleError::new(format!("failed to execute query {:?}: {e}", self.expr)))?;

        let num_series = res.data.len() as i64;
        if limit > 0 && num_series > limit {
            return Err(RuleError::new(format!(
                "exec exceeded limit of {limit} with {num_series} series"
            )));
        }

        let mut seen: HashSet<String> = HashSet::with_capacity(res.data.len());
        let mut out: Vec<Series> = Vec::with_capacity(res.data.len());
        for m in &res.data {
            let series = to_time_series(m, &self.name, &self.labels);
            let key = labels_to_key(&series.labels);
            if !seen.insert(key.clone()) {
                return Err(RuleError::new(format!(
                    "original metric {:?}; resulting labels {key:?}: {ERR_DUPLICATE}",
                    m.labels
                )));
            }
            out.push(series);
        }
        Ok(out)
    }
}

/// Builds an owned [`Series`] from a queried metric: labels merged via
/// [`merge_labels`] (`__name__` = `name`, rule labels overlaid), and one sample
/// per `(value, timestamp)` pair.
///
/// Port of `toTimeSeries` (`recording.go:285-320`) + `newTimeSeries`
/// (`utils.go:16-29`). Timestamps from the datasource are unix seconds; upstream
/// `newTimeSeries` converts them to millis (`time.Unix(ts,0).UnixNano()/1e6`),
/// mirrored here as `t * 1000`. Recording rules query instant, so each metric
/// carries exactly one sample; if a metric carries several, all are emitted
/// (faithful to `newTimeSeries`, which builds one sample per value).
fn to_time_series(m: &Metric, name: &str, rule_labels: &BTreeMap<String, String>) -> Series {
    let labels = merge_labels(&m.labels, rule_labels, name);
    let samples = m
        .values
        .iter()
        .zip(m.timestamps.iter())
        .map(|(&value, &ts_secs)| Sample {
            value,
            timestamp: ts_secs.saturating_mul(1000),
        })
        .collect();
    Series { labels, samples }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::{DsError, QueryResult};

    struct MockQ(Vec<Metric>);
    impl Querier for MockQ {
        fn query(&self, _: &str, _: i64) -> Result<QueryResult, DsError> {
            Ok(QueryResult {
                data: self.0.clone(),
                is_partial: None,
            })
        }
    }

    #[test]
    fn recording_sets_name_and_overlays_labels() {
        let mut rr = RecordingRule {
            name: "job:up".into(),
            expr: "up".into(),
            labels: [("team".to_string(), "a".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let q = MockQ(vec![Metric {
            labels: vec![("instance".into(), "h1".into())],
            timestamps: vec![0],
            values: vec![1.0],
        }]);
        let tss = rr.exec(&q, 0, 0).unwrap();
        assert!(tss[0]
            .labels
            .contains(&("__name__".to_string(), "job:up".to_string())));
        assert!(tss[0]
            .labels
            .contains(&("team".to_string(), "a".to_string())));
    }

    #[test]
    fn recording_enforces_limit() {
        let mut rr = RecordingRule {
            name: "r".into(),
            expr: "up".into(),
            ..Default::default()
        };
        let q = MockQ(vec![
            Metric {
                labels: vec![("i".into(), "1".into())],
                timestamps: vec![0],
                values: vec![1.0],
            },
            Metric {
                labels: vec![("i".into(), "2".into())],
                timestamps: vec![0],
                values: vec![1.0],
            },
        ]);
        assert!(rr.exec(&q, 0, 1).is_err());
    }

    #[test]
    fn recording_detects_duplicate_series() {
        // Two metrics whose only distinguishing label (`i`) is deleted by an
        // empty rule-label value -> identical resulting label sets.
        let mut rr = RecordingRule {
            name: "r".into(),
            expr: "up".into(),
            labels: [("i".to_string(), String::new())].into_iter().collect(),
            ..Default::default()
        };
        let q = MockQ(vec![
            Metric {
                labels: vec![("i".into(), "1".into()), ("keep".into(), "x".into())],
                timestamps: vec![0],
                values: vec![1.0],
            },
            Metric {
                labels: vec![("i".into(), "2".into()), ("keep".into(), "x".into())],
                timestamps: vec![0],
                values: vec![2.0],
            },
        ]);
        let err = rr.exec(&q, 0, 0).unwrap_err();
        assert!(
            err.to_string().contains("same labelset"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn recording_converts_timestamp_seconds_to_millis() {
        let mut rr = RecordingRule {
            name: "r".into(),
            expr: "up".into(),
            ..Default::default()
        };
        let q = MockQ(vec![Metric {
            labels: vec![("i".into(), "1".into())],
            timestamps: vec![1000],
            values: vec![5.0],
        }]);
        let tss = rr.exec(&q, 0, 0).unwrap();
        assert_eq!(tss[0].samples.len(), 1);
        assert_eq!(tss[0].samples[0].value, 5.0);
        assert_eq!(tss[0].samples[0].timestamp, 1_000_000);
    }
}
