//! Alerting-rule evaluation: the `Pending`/`Firing`/`Inactive` state machine
//! and `ALERTS`/`ALERTS_FOR_STATE` series construction.
//!
//! Port of `app/vmalert/rule/alerting.go`'s `AlertingRule.exec` (the state
//! machine, `:438-599`) and `alertsToSend` (`:862-890`). Per-alert label/
//! annotation computation and series conversion live in `alert.rs`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use esm_gotemplate::{EvalContext, Funcs};

use super::alert::{
    alert_for_time_series, alert_time_series, build_alert_labels, build_tpl_data,
    firing_alert_stale_series, hash_labels, pending_alert_stale_series, render_map, Alert,
    AlertState, RESOLVED_RETENTION_MS,
};
use super::{Querier, RuleError};
use crate::series::Series;

/// Upstream `errDuplicate` message (`rule/rule.go:40`), embedded verbatim —
/// same wording as `recording.rs`'s copy, since both rule kinds report the
/// identical duplicate-labelset condition.
const ERR_DUPLICATE: &str = "result contains metrics with the same labelset during evaluation. See https://docs.victoriametrics.com/victoriametrics/vmalert/#series-with-the-same-labelset for details";

/// An alerting rule: an expression plus thresholds/labels/annotations, and
/// the live `alerts` map its evaluations maintain. Port of `AlertingRule`
/// (`alerting.go:28-54`), narrowed to the fields this port needs;
/// VictoriaMetrics-only bookkeeping (metrics, group/rule IDs, `ruleState`,
/// health/last-error tracking) is out of scope here — a later task (the
/// group executor) owns per-rule health reporting.
#[derive(Debug, Default)]
pub struct AlertingRule {
    /// Stable rule identity, matching upstream's `HashRule`/`Rule.ID()`: a
    /// hash over `expr` + recording/alerting discriminator + name + sorted
    /// labels (see `config::rule_identity_hash`, computed once when this
    /// rule is built from parsed config). Used by
    /// `rule::group::Group::apply_update` to match alerting rules across a
    /// hot-reload by identity rather than by name alone, so a rule whose
    /// `expr`/labels changed starts fresh instead of inheriting a
    /// no-longer-applicable rule's live alert state.
    pub id: u64,
    pub name: String,
    pub group_name: String,
    pub expr: String,
    pub r#for: Duration,
    pub keep_firing_for: Duration,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    /// `-disableAlertgroupLabel`: when set, the `alertgroup` label is
    /// suppressed on every `ALERTS`/`ALERTS_FOR_STATE` series and notifier
    /// alert this rule produces (see `build_alert_labels`), and the
    /// remote-read restore query drops its `alertgroup` matcher to stay
    /// consistent (see `AlertingRule::restore`). Mirrors upstream vmalert's
    /// flag of the same name.
    pub disable_alertgroup_label: bool,
    pub alerts: HashMap<u64, Alert>,
    /// Health of this rule's most recent `exec`, published in the live
    /// [`super::GroupSnapshot`]. `Unknown` until the first evaluation.
    pub health: super::RuleHealth,
    /// The most recent `exec` error's message, if the last evaluation
    /// failed (`None` once it succeeds). Recorded by `Group::eval_once`.
    pub last_error: Option<String>,
}

/// One query result's computed identity/annotations, built before `exec`
/// mutates `self.alerts` — mirrors upstream computing `expandedLabels`/
/// `expandedAnnotations` up front (`alerting.go:474-499`) so a slow
/// (potential sub-query-driven) template render can't interleave with state
/// mutation.
struct PendingAlert {
    id: u64,
    labels: BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
    value: f64,
}

impl AlertingRule {
    /// Evaluates the rule's expression at `ts` (unix millis) via `q`,
    /// advances the `Pending`/`Firing`/`Inactive` state machine, and
    /// returns the `ALERTS`/`ALERTS_FOR_STATE` series for every
    /// currently-active (non-`Inactive`) alert.
    ///
    /// Port of `AlertingRule.exec` (`alerting.go:438-599`), minus the
    /// `limit` (active/pending count cap) and per-execution state/metrics
    /// bookkeeping upstream also does there — out of scope for this task.
    pub fn exec(
        &mut self,
        q: &dyn Querier,
        ts: i64,
        funcs: &Funcs,
        ctx: &EvalContext,
    ) -> Result<Vec<Series>, RuleError> {
        let res = q
            .query(&self.expr, ts)
            .map_err(|e| RuleError::new(format!("failed to execute query {:?}: {e}", self.expr)))?;
        let is_partial = res.is_partial.unwrap_or(false);

        let pending = self.expand_results(&res.data, is_partial, ts, funcs, ctx);

        // Evict alerts resolved long enough ago (`resolvedRetention`,
        // `alerting.go:434-436,504-510`).
        self.alerts.retain(|_, a| {
            !(a.state == AlertState::Inactive
                && a.resolved_at
                    .is_some_and(|r| ts - r > RESOLVED_RETENTION_MS))
        });

        let updated = self.apply_updates(pending, ts)?;
        // Stale (StaleNaN) `ALERTS`/`ALERTS_FOR_STATE` series for alerts that
        // transitioned this round come first, then the live series for
        // still-active alerts — matching upstream's append order
        // (`alerting.go:546-598`).
        let mut tss = self.sweep_and_promote(&updated, ts);
        for a in self.alerts.values() {
            if a.state == AlertState::Inactive {
                continue;
            }
            tss.push(alert_time_series(a, ts));
            tss.push(alert_for_time_series(a, ts));
        }
        Ok(tss)
    }

    /// Computes each result's alert identity (hash of processed labels) and
    /// rendered annotations. Port of the per-series loop building
    /// `expandedLabels`/`expandedAnnotations` (`alerting.go:474-499`),
    /// including the "use the existing alert's `ActiveAt` while it's still
    /// active" rule (`:483-490`).
    fn expand_results(
        &self,
        data: &[crate::datasource::Metric],
        is_partial: bool,
        ts: i64,
        funcs: &Funcs,
        ctx: &EvalContext,
    ) -> Vec<PendingAlert> {
        let mut out = Vec::with_capacity(data.len());
        for m in data {
            let value = m.values.first().copied().unwrap_or(0.0);
            let built = build_alert_labels(
                m,
                &self.labels,
                &self.name,
                &self.group_name,
                &self.expr,
                self.disable_alertgroup_label,
                funcs,
                ctx,
            );
            let id = hash_labels(&built.processed);
            let active_at = match self.alerts.get(&id) {
                Some(a) if a.state != AlertState::Inactive => a.active_at,
                _ => ts,
            };
            let data = build_tpl_data(
                value,
                "prometheus",
                &built.origin,
                &self.expr,
                id,
                0,
                active_at,
                self.r#for.as_secs_f64(),
                is_partial,
                ctx,
            );
            let annotations = render_map(&self.annotations, &data, funcs, ctx);
            out.push(PendingAlert {
                id,
                labels: built.processed,
                annotations,
                value,
            });
        }
        out
    }

    /// Creates/updates `self.alerts` from this round's results, returning
    /// the set of alert IDs seen (used by [`Self::sweep_and_promote`] to
    /// find absent alerts). Port of `alerting.go:512-543`; a repeated ID
    /// within the same round is a duplicate-labelset error (`:516-521`),
    /// matching `RecordingRule::exec`'s identical check.
    fn apply_updates(
        &mut self,
        pending: Vec<PendingAlert>,
        ts: i64,
    ) -> Result<HashSet<u64>, RuleError> {
        let mut updated = HashSet::with_capacity(pending.len());
        for p in pending {
            if !updated.insert(p.id) {
                return Err(RuleError::new(format!(
                    "labels {:?}: {ERR_DUPLICATE}",
                    p.labels
                )));
            }
            match self.alerts.get_mut(&p.id) {
                Some(a) => {
                    if a.state == AlertState::Inactive {
                        // An alert can sit Inactive for `resolvedRetention`;
                        // seeing its series again reactivates it.
                        a.state = AlertState::Pending;
                        a.active_at = ts;
                    }
                    a.value = p.value;
                    a.annotations = p.annotations;
                    a.keep_firing_since = None;
                }
                None => {
                    self.alerts.insert(
                        p.id,
                        Alert {
                            state: AlertState::Pending,
                            active_at: ts,
                            keep_firing_since: None,
                            resolved_at: None,
                            value: p.value,
                            labels: p.labels,
                            annotations: p.annotations,
                        },
                    );
                }
            }
        }
        Ok(updated)
    }

    /// Resolves alerts absent this round (`Pending` -> deleted; `Firing` ->
    /// `Inactive`, unless `keep_firing_for` is still counting down) and
    /// promotes `Pending` alerts that have been active `>= for` to `Firing`.
    /// Returns the StaleNaN `ALERTS`/`ALERTS_FOR_STATE` series that terminate
    /// each transitioned alert's live series in remote storage (so the
    /// remote-read restore path won't resurrect a resolved alert). Port of
    /// `alerting.go:546-591`, including its three stale-series appends.
    fn sweep_and_promote(&mut self, updated: &HashSet<u64>, ts: i64) -> Vec<Series> {
        let for_ms = i64::try_from(self.r#for.as_millis()).unwrap_or(i64::MAX);
        let keep_firing_for_ms =
            i64::try_from(self.keep_firing_for.as_millis()).unwrap_or(i64::MAX);

        let mut stale = Vec::new();
        self.alerts.retain(|id, a| {
            if !updated.contains(id) {
                match a.state {
                    AlertState::Pending => {
                        // Pending -> deleted: also terminate its
                        // `ALERTS_FOR_STATE` (`alerting.go:553`).
                        stale.extend(pending_alert_stale_series(&a.labels, ts, true));
                        return false;
                    }
                    AlertState::Firing => {
                        if keep_firing_for_ms > 0 && a.keep_firing_since.is_none() {
                            a.keep_firing_since = Some(ts);
                        }
                        let elapsed = a.keep_firing_since.map(|s| ts - s).unwrap_or(i64::MAX);
                        if elapsed >= keep_firing_for_ms {
                            a.state = AlertState::Inactive;
                            a.resolved_at = Some(ts);
                            // Firing -> Inactive (`alerting.go:573`).
                            stale.extend(firing_alert_stale_series(&a.labels, ts));
                        }
                    }
                    AlertState::Inactive => {}
                }
            }
            if a.state == AlertState::Pending && ts - a.active_at >= for_ms {
                a.state = AlertState::Firing;
                // Pending -> Firing with For>0: the pending `ALERTS` series is
                // stale, but no `ALERTS_FOR_STATE` was written while pending
                // (`alerting.go:586-589`).
                if for_ms > 0 {
                    stale.extend(pending_alert_stale_series(&a.labels, ts, false));
                }
            }
            true
        });
        stale
    }

    /// Selects alerts that should be sent to a notifier.
    ///
    /// Divergence from upstream: `alertsToSend` (`alerting.go:862-890`) also
    /// throttles repeat notifications for `Firing`/`Inactive` alerts using
    /// `LastSent`/`End` timestamps stored on the alert, resending only once
    /// `resend_delay` has elapsed since the last send. This port's [`Alert`]
    /// deliberately doesn't carry `LastSent`/`End` (see its doc comment) —
    /// that bookkeeping belongs to the notifier-dispatch loop (a later
    /// task), which can track "last sent per alert ID" externally across
    /// evaluation cycles. Given that, this method returns every currently
    /// non-`Pending` alert on every call; `ts`, `resolve_delay`, and
    /// `resend_delay` are accepted to match the intended call site (an
    /// executor with external `LastSent` tracking could reintroduce
    /// throttling around this) but aren't consulted here.
    pub fn alerts_to_send(
        &self,
        ts: i64,
        resolve_delay: Duration,
        resend_delay: Duration,
    ) -> Vec<Alert> {
        let _ = (ts, resolve_delay, resend_delay);
        self.alerts
            .values()
            .filter(|a| a.state != AlertState::Pending)
            .cloned()
            .collect()
    }

    /// Computes an alert's identity hash for a label set, exposing
    /// [`hash_labels`] (used by [`Self::expand_results`]) to sibling
    /// modules — specifically `remoteread::restore`, which must key
    /// restored `ALERTS_FOR_STATE` series into `self.alerts` with the same
    /// identity `exec` uses.
    pub(crate) fn alert_hash(labels: &BTreeMap<String, String>) -> u64 {
        hash_labels(labels)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::{DsError, Metric, QueryResult};
    use esm_gotemplate::default_funcs;
    use std::sync::Arc;

    fn one_sample(v: f64) -> Metric {
        Metric {
            labels: vec![("instance".into(), "h1".into())],
            timestamps: vec![0],
            values: vec![v],
        }
    }

    struct Scripted {
        present: bool,
    }
    impl Querier for Scripted {
        fn query(&self, _: &str, _: i64) -> Result<QueryResult, DsError> {
            Ok(QueryResult {
                data: if self.present {
                    vec![one_sample(1.0)]
                } else {
                    vec![]
                },
                is_partial: None,
            })
        }
    }

    struct FixedMetric(Metric);
    impl Querier for FixedMetric {
        fn query(&self, _: &str, _: i64) -> Result<QueryResult, DsError> {
            Ok(QueryResult {
                data: vec![self.0.clone()],
                is_partial: None,
            })
        }
    }

    fn test_ctx() -> EvalContext {
        EvalContext {
            external_url: "".into(),
            path_prefix: "".into(),
            query_fn: Arc::new(|_| Ok(vec![])),
        }
    }

    #[test]
    fn pending_then_firing_after_for() {
        let ctx = test_ctx();
        let funcs = default_funcs(&ctx);
        let mut ar = AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            r#for: Duration::from_secs(120),
            ..Default::default()
        };
        // t=0: becomes Pending
        ar.exec(&Scripted { present: true }, 0, &funcs, &ctx)
            .unwrap();
        assert!(matches!(
            ar.alerts.values().next().unwrap().state,
            AlertState::Pending
        ));
        // t=130s: for elapsed -> Firing
        ar.exec(&Scripted { present: true }, 130_000, &funcs, &ctx)
            .unwrap();
        assert!(matches!(
            ar.alerts.values().next().unwrap().state,
            AlertState::Firing
        ));
    }

    #[test]
    fn alertgroup_label_present_by_default() {
        let ctx = test_ctx();
        let funcs = default_funcs(&ctx);
        let mut ar = AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            r#for: Duration::from_secs(0),
            ..Default::default()
        };
        let tss = ar
            .exec(&Scripted { present: true }, 0, &funcs, &ctx)
            .unwrap();
        // The active alert's identity labels carry `alertgroup`.
        let a = ar.alerts.values().next().unwrap();
        assert_eq!(a.labels.get("alertgroup").map(String::as_str), Some("g"));
        // ...and so do the emitted ALERTS/ALERTS_FOR_STATE series.
        assert!(
            !tss.is_empty(),
            "expected emitted series for a firing alert"
        );
        for ts in &tss {
            assert!(
                ts.labels.iter().any(|(k, v)| k == "alertgroup" && v == "g"),
                "series missing alertgroup label: {:?}",
                ts.labels
            );
        }
    }

    #[test]
    fn alertgroup_label_suppressed_when_disabled() {
        let ctx = test_ctx();
        let funcs = default_funcs(&ctx);
        let mut ar = AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            r#for: Duration::from_secs(0),
            disable_alertgroup_label: true,
            ..Default::default()
        };
        let tss = ar
            .exec(&Scripted { present: true }, 0, &funcs, &ctx)
            .unwrap();
        // No `alertgroup` on the alert identity labels...
        let a = ar.alerts.values().next().unwrap();
        assert!(
            !a.labels.contains_key("alertgroup"),
            "alertgroup label must be suppressed: {:?}",
            a.labels
        );
        // ...nor on any emitted series.
        assert!(
            !tss.is_empty(),
            "expected emitted series for a firing alert"
        );
        for ts in &tss {
            assert!(
                !ts.labels.iter().any(|(k, _)| k == "alertgroup"),
                "series must omit alertgroup label: {:?}",
                ts.labels
            );
        }
    }

    #[test]
    fn resolves_when_sample_absent() {
        let ctx = test_ctx();
        let funcs = default_funcs(&ctx);
        let mut ar = AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            r#for: Duration::from_secs(0),
            ..Default::default()
        };
        ar.exec(&Scripted { present: true }, 0, &funcs, &ctx)
            .unwrap(); // firing (for=0)
        ar.exec(&Scripted { present: false }, 60_000, &funcs, &ctx)
            .unwrap(); // sample gone -> inactive
        let a = ar.alerts.values().next().unwrap();
        assert!(matches!(a.state, AlertState::Inactive));
    }

    #[test]
    fn keep_firing_for_holds_firing_state_during_absence() {
        let ctx = test_ctx();
        let funcs = default_funcs(&ctx);
        let mut ar = AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            r#for: Duration::from_secs(0),
            keep_firing_for: Duration::from_secs(120),
            ..Default::default()
        };
        ar.exec(&Scripted { present: true }, 0, &funcs, &ctx)
            .unwrap();
        assert!(matches!(
            ar.alerts.values().next().unwrap().state,
            AlertState::Firing
        ));

        // Absent at t=60s: only 60s since first absence, keep_firing_for
        // (120s) not yet elapsed -> stays Firing.
        ar.exec(&Scripted { present: false }, 60_000, &funcs, &ctx)
            .unwrap();
        assert!(matches!(
            ar.alerts.values().next().unwrap().state,
            AlertState::Firing
        ));

        // Absent at t=200s: 140s since first absence (t=60s) >=
        // keep_firing_for -> Inactive.
        ar.exec(&Scripted { present: false }, 200_000, &funcs, &ctx)
            .unwrap();
        assert!(matches!(
            ar.alerts.values().next().unwrap().state,
            AlertState::Inactive
        ));
    }

    #[test]
    fn alerts_for_state_value_is_active_at_unix_seconds() {
        let ctx = test_ctx();
        let funcs = default_funcs(&ctx);
        let mut ar = AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            r#for: Duration::from_secs(0),
            ..Default::default()
        };
        // Non-round-second `active_at` (5_123 ms): the ALERTS_FOR_STATE
        // value must truncate to WHOLE unix seconds (upstream
        // `float64(a.ActiveAt.Unix())`), i.e. 5.0 — never 5.123.
        let tss = ar
            .exec(&Scripted { present: true }, 5_123, &funcs, &ctx)
            .unwrap();
        let for_state = tss
            .iter()
            .find(|s| {
                s.labels
                    .iter()
                    .any(|(k, v)| k == "__name__" && v == "ALERTS_FOR_STATE")
            })
            .expect("ALERTS_FOR_STATE series present");
        assert_eq!(for_state.samples[0].value, 5.0);
        assert_eq!(for_state.samples[0].timestamp, 5_123);

        let alerts = tss
            .iter()
            .find(|s| {
                s.labels
                    .iter()
                    .any(|(k, v)| k == "__name__" && v == "ALERTS")
            })
            .expect("ALERTS series present");
        assert!(alerts
            .labels
            .iter()
            .any(|(k, v)| k == "alertstate" && v == "firing"));
        assert_eq!(alerts.samples[0].value, 1.0);
    }

    #[test]
    fn annotation_renders_value_and_labels_through_preamble() {
        let ctx = test_ctx();
        let funcs = default_funcs(&ctx);
        let mut ar = AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            r#for: Duration::from_secs(0),
            annotations: [(
                "summary".to_string(),
                "value={{ $value }} x={{ $labels.x }}".to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let q = FixedMetric(Metric {
            labels: vec![("x".into(), "42".into())],
            timestamps: vec![0],
            values: vec![3.0],
        });
        ar.exec(&q, 0, &funcs, &ctx).unwrap();
        let a = ar.alerts.values().next().unwrap();
        assert_eq!(a.annotations.get("summary").unwrap(), "value=3 x=42");
    }

    // Finds the (single) series in `tss` whose `__name__` == `name`.
    fn find_series<'a>(tss: &'a [Series], name: &str) -> Option<&'a Series> {
        tss.iter()
            .find(|s| s.labels.iter().any(|(k, v)| k == "__name__" && v == name))
    }

    #[test]
    fn pending_to_resolved_emits_stale_alerts_and_for_state() {
        use esm_common::decimal::is_stale_nan;
        let ctx = test_ctx();
        let funcs = default_funcs(&ctx);
        let mut ar = AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            r#for: Duration::from_secs(120),
            ..Default::default()
        };
        // t=0: Pending.
        ar.exec(&Scripted { present: true }, 0, &funcs, &ctx)
            .unwrap();
        // t=60s: sample absent while still Pending -> deleted; emits StaleNaN
        // ALERTS{alertstate="pending"} AND ALERTS_FOR_STATE.
        let tss = ar
            .exec(&Scripted { present: false }, 60_000, &funcs, &ctx)
            .unwrap();

        let alerts = find_series(&tss, "ALERTS").expect("stale ALERTS series present");
        assert!(
            alerts
                .labels
                .iter()
                .any(|(k, v)| k == "alertstate" && v == "pending"),
            "stale ALERTS must carry alertstate=pending: {:?}",
            alerts.labels
        );
        assert!(
            alerts
                .labels
                .iter()
                .any(|(k, v)| k == "alertname" && v == "A"),
            "stale ALERTS must retain identity labels: {:?}",
            alerts.labels
        );
        assert!(
            is_stale_nan(alerts.samples[0].value),
            "stale ALERTS value must be StaleNaN"
        );
        assert_eq!(alerts.samples[0].timestamp, 60_000);

        let for_state =
            find_series(&tss, "ALERTS_FOR_STATE").expect("stale ALERTS_FOR_STATE present");
        assert!(
            is_stale_nan(for_state.samples[0].value),
            "stale ALERTS_FOR_STATE value must be StaleNaN"
        );

        // The alert is fully gone (deleted, not merely Inactive).
        assert!(ar.alerts.is_empty());
    }

    #[test]
    fn pending_to_firing_emits_stale_pending_alerts_only() {
        use esm_common::decimal::is_stale_nan;
        let ctx = test_ctx();
        let funcs = default_funcs(&ctx);
        let mut ar = AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            r#for: Duration::from_secs(120),
            ..Default::default()
        };
        // t=0: Pending.
        ar.exec(&Scripted { present: true }, 0, &funcs, &ctx)
            .unwrap();
        // t=130s: For elapsed, still present -> promoted to Firing. The prior
        // pending ALERTS series is terminated with StaleNaN, but there is NO
        // stale ALERTS_FOR_STATE (none was written while pending).
        let tss = ar
            .exec(&Scripted { present: true }, 130_000, &funcs, &ctx)
            .unwrap();

        // Exactly one stale (StaleNaN) ALERTS with alertstate=pending exists,
        // alongside the live firing ALERTS series.
        let stale_pending: Vec<_> = tss
            .iter()
            .filter(|s| {
                s.labels
                    .iter()
                    .any(|(k, v)| k == "__name__" && v == "ALERTS")
                    && s.labels
                        .iter()
                        .any(|(k, v)| k == "alertstate" && v == "pending")
            })
            .collect();
        assert_eq!(stale_pending.len(), 1, "one stale pending ALERTS expected");
        assert!(is_stale_nan(stale_pending[0].samples[0].value));

        // No StaleNaN ALERTS_FOR_STATE on a pending->firing promotion.
        let stale_for_state = tss.iter().any(|s| {
            s.labels
                .iter()
                .any(|(k, v)| k == "__name__" && v == "ALERTS_FOR_STATE")
                && is_stale_nan(s.samples[0].value)
        });
        assert!(
            !stale_for_state,
            "no stale ALERTS_FOR_STATE on pending->firing"
        );
    }

    #[test]
    fn firing_to_inactive_emits_stale_firing_series() {
        use esm_common::decimal::is_stale_nan;
        let ctx = test_ctx();
        let funcs = default_funcs(&ctx);
        let mut ar = AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            r#for: Duration::from_secs(0),
            keep_firing_for: Duration::from_secs(120),
            ..Default::default()
        };
        ar.exec(&Scripted { present: true }, 0, &funcs, &ctx)
            .unwrap(); // Firing (for=0)
        ar.exec(&Scripted { present: false }, 60_000, &funcs, &ctx)
            .unwrap(); // absent, keep_firing not yet elapsed -> stays Firing
                       // t=200s: 140s since first absence >= keep_firing_for -> Inactive;
                       // emits StaleNaN ALERTS{alertstate="firing"} + ALERTS_FOR_STATE.
        let tss = ar
            .exec(&Scripted { present: false }, 200_000, &funcs, &ctx)
            .unwrap();

        let alerts = find_series(&tss, "ALERTS").expect("stale ALERTS series present");
        assert!(
            alerts
                .labels
                .iter()
                .any(|(k, v)| k == "alertstate" && v == "firing"),
            "stale ALERTS must carry alertstate=firing: {:?}",
            alerts.labels
        );
        assert!(is_stale_nan(alerts.samples[0].value));

        let for_state =
            find_series(&tss, "ALERTS_FOR_STATE").expect("stale ALERTS_FOR_STATE present");
        assert!(is_stale_nan(for_state.samples[0].value));
    }

    #[test]
    fn alerts_to_send_excludes_pending() {
        let ctx = test_ctx();
        let funcs = default_funcs(&ctx);
        let mut ar = AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            r#for: Duration::from_secs(120),
            ..Default::default()
        };
        ar.exec(&Scripted { present: true }, 0, &funcs, &ctx)
            .unwrap();
        assert!(ar
            .alerts_to_send(0, Duration::from_secs(60), Duration::from_secs(60))
            .is_empty());

        ar.exec(&Scripted { present: true }, 130_000, &funcs, &ctx)
            .unwrap();
        let sent = ar.alerts_to_send(130_000, Duration::from_secs(60), Duration::from_secs(60));
        assert_eq!(sent.len(), 1);
        assert!(matches!(sent[0].state, AlertState::Firing));
    }
}
