//! Live, read-only snapshot of a running group's rule health and active
//! alerts, published by the group's evaluation thread after every
//! `eval_once` and read back by the manager for the JSON API (Task 18).
//!
//! The group thread OWNS its rules (they're moved into it by
//! [`super::group::Group::start`]), so the only sound way to expose their
//! live state outside that thread is for the thread to rebuild this
//! plain-data snapshot into a shared `Arc<Mutex<GroupSnapshot>>` after each
//! evaluation — never to move the rules back out. [`build_snapshot`]
//! performs that rebuild from the group's `&[RuleKind]`.

use std::collections::BTreeMap;

use serde::Serialize;

use super::group::RuleKind;

/// Health of a rule's most recent evaluation. `Unknown` before the first
/// eval; `Ok`/`Err` reflect the last `exec` outcome. Port of the
/// health-status field on `rule.ApiRule` (`rule/rule.go`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RuleHealth {
    #[default]
    Unknown,
    Ok,
    Err,
}

/// One rule's plain-data view for the JSON API (Task 18). Port of the
/// rule-facing subset of `rule.ApiRule` (`rule/rule.go`).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RuleView {
    /// Stable identity hash — see `config::rule_identity_hash`.
    pub id: u64,
    pub name: String,
    pub record: Option<String>,
    pub alert: Option<String>,
    pub expr: String,
    pub health: RuleHealth,
    pub last_error: Option<String>,
}

/// One active (or recently resolved) alert's plain-data view for the JSON
/// API (Task 18). Port of the alert-facing subset of `notifier.ApiAlert`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AlertView {
    pub group: String,
    pub alertname: String,
    pub state: String,
    pub active_at: i64,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub value: f64,
}

/// The full live snapshot a running group publishes: every rule's current
/// health plus every currently-tracked alert. Rebuilt wholesale after each
/// evaluation; cheap to clone out under the lock.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct GroupSnapshot {
    pub rules: Vec<RuleView>,
    pub alerts: Vec<AlertView>,
}

/// Rebuilds a [`GroupSnapshot`] from a group's live rules: one [`RuleView`]
/// per rule (with its current health/last_error), plus one [`AlertView`]
/// per entry in each alerting rule's `alerts` map.
pub fn build_snapshot(rules: &[RuleKind]) -> GroupSnapshot {
    let mut rule_views = Vec::with_capacity(rules.len());
    let mut alert_views = Vec::new();
    for r in rules {
        match r {
            RuleKind::Recording(rr) => rule_views.push(RuleView {
                id: rr.id,
                name: rr.name.clone(),
                record: Some(rr.name.clone()),
                alert: None,
                expr: rr.expr.clone(),
                health: rr.health,
                last_error: rr.last_error.clone(),
            }),
            RuleKind::Alerting(ar) => {
                rule_views.push(RuleView {
                    id: ar.id,
                    name: ar.name.clone(),
                    record: None,
                    alert: Some(ar.name.clone()),
                    expr: ar.expr.clone(),
                    health: ar.health,
                    last_error: ar.last_error.clone(),
                });
                for a in ar.alerts.values() {
                    alert_views.push(AlertView {
                        group: ar.group_name.clone(),
                        alertname: ar.name.clone(),
                        state: a.state.as_str().to_string(),
                        active_at: a.active_at,
                        labels: a.labels.clone(),
                        annotations: a.annotations.clone(),
                        value: a.value,
                    });
                }
            }
        }
    }
    GroupSnapshot {
        rules: rule_views,
        alerts: alert_views,
    }
}
