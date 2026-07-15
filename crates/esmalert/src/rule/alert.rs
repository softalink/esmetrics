//! The `Alert` entity, its identity/label computation, and the `ALERTS` /
//! `ALERTS_FOR_STATE` series it's converted to.
//!
//! Port of `app/vmalert/notifier/alert.go` (`Alert`, `AlertState`,
//! `AlertTplData`) and the label/series-building pieces of
//! `app/vmalert/rule/alerting.go` (`labelSet`, `toLabels`, `hash`,
//! `alertToTimeSeries`/`alertForToTimeSeries`). The state-machine loop that
//! drives these (`AlertingRule::exec`) lives in `alerting.rs`.

use std::collections::BTreeMap;

use esm_common::decimal::STALE_NAN;
use esm_gotemplate::{EvalContext, Funcs, Template, Value};

use crate::datasource::Metric;
use crate::series::{Sample, Series};
use crate::templating::TPL_HEADERS;

/// Metric name for the series reflecting the alert state. Port of
/// `alertMetricName` (`alerting.go:686`).
pub const ALERT_METRIC: &str = "ALERTS";
/// Metric name for the series reflecting the moment an alert became active.
/// Port of `alertForStateMetricName` (`alerting.go:688`).
pub const ALERT_FOR_STATE_METRIC: &str = "ALERTS_FOR_STATE";
/// Label naming the alert. Port of `alertNameLabel` (`alerting.go:691`).
pub const ALERT_NAME_LABEL: &str = "alertname";
/// Label naming the alert's parent group. Port of `alertGroupNameLabel`
/// (`alerting.go:697`); upstream can disable this via
/// `-disableAlertgroupLabel`, not wired in this port (always added when
/// `group_name` is non-empty).
pub const ALERT_GROUP_LABEL: &str = "alertgroup";
/// Label naming the alert's current state. Port of `alertStateLabel`
/// (`alerting.go:693`).
pub const ALERT_STATE_LABEL: &str = "alertstate";

const NAME_LABEL: &str = "__name__";

/// Duration an alert is kept in `Inactive` state (and re-sent/queryable)
/// after resolving, before eviction. Port of `resolvedRetention`
/// (`alerting.go:436`), expressed in millis to match this crate's `ts`
/// convention (unix millis; see `datasource::client::Datasource::query`).
pub(crate) const RESOLVED_RETENTION_MS: i64 = 15 * 60 * 1000;

/// The current state of an [`Alert`]. Port of `notifier.AlertState`
/// (`alert.go:56-68`); `Inactive` is the zero value both here and upstream
/// (Go's `iota` starts `StateInactive` at 0).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AlertState {
    #[default]
    Inactive,
    Pending,
    Firing,
}

impl AlertState {
    /// Port of `AlertState.String` (`alert.go:70-79`).
    pub fn as_str(&self) -> &'static str {
        match self {
            AlertState::Inactive => "inactive",
            AlertState::Pending => "pending",
            AlertState::Firing => "firing",
        }
    }
}

/// A single active (or recently resolved) alert instance. Port of
/// `notifier.Alert` (`alert.go:17-54`), narrowed to the fields the state
/// machine and `ALERTS`/`ALERTS_FOR_STATE` series construction need.
///
/// Not ported: `GroupID`/`Name`/`Type`/`Expr`/`ID`/`Restored`/`For` (static
/// per-rule metadata the caller already has via `AlertingRule`, not
/// per-alert state) and `Start`/`End`/`LastSent` (notifier resend-throttle
/// bookkeeping — see [`super::alerting::AlertingRule::alerts_to_send`]'s doc
/// comment for why that's deferred rather than tracked here).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Alert {
    pub state: AlertState,
    /// Unix millis when the alert most recently transitioned into
    /// `Pending` (i.e. became newly active). Port of `Alert.ActiveAt`.
    pub active_at: i64,
    /// Unix millis when a `Firing` alert's underlying series first went
    /// absent, started only when `keep_firing_for > 0`. Port of
    /// `Alert.KeepFiringSince` (`time.Time{}` zero value == `None` here).
    pub keep_firing_since: Option<i64>,
    /// Unix millis when the alert transitioned `Firing`/`Pending` ->
    /// `Inactive`. Port of `Alert.ResolvedAt`.
    pub resolved_at: Option<i64>,
    pub value: f64,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
}

/// FNV-1a 64-bit hash of a label set, skipping `__name__`. Port of `hash`
/// (`alerting.go:641-660`): `labels` is a `BTreeMap`, so iteration is
/// already name-sorted, matching upstream's explicit `sort.Strings(keys)`.
/// This is the alert's identity key (`alerts: HashMap<u64, Alert>`).
pub(crate) fn hash_labels(labels: &BTreeMap<String, String>) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut h = OFFSET_BASIS;
    let step = |b: u8, h: &mut u64| *h = (*h ^ u64::from(b)).wrapping_mul(PRIME);
    for (k, v) in labels {
        if k == NAME_LABEL {
            continue;
        }
        for b in k.as_bytes() {
            step(*b, &mut h);
        }
        for b in v.as_bytes() {
            step(*b, &mut h);
        }
        step(0xFF, &mut h);
    }
    h
}

/// The origin (all queried labels, `__name__` included) and processed
/// (`__name__` dropped, rule labels overlaid) label sets built from one
/// query result. Port of `labelSet` (`alerting.go:296-308`).
struct LabelSet {
    origin: BTreeMap<String, String>,
    processed: BTreeMap<String, String>,
}

impl LabelSet {
    fn from_metric(m: &Metric) -> Self {
        let mut origin = BTreeMap::new();
        let mut processed = BTreeMap::new();
        for (k, v) in &m.labels {
            origin.insert(k.clone(), v.clone());
            if k != NAME_LABEL {
                processed.insert(k.clone(), v.clone());
            }
        }
        LabelSet { origin, processed }
    }

    /// Port of `labelSet.add` (`alerting.go:310-333`): an empty `v` removes
    /// `k` from `processed` (relabeling-compatibility; never adds an empty
    /// label). Otherwise `processed[k] = v`; if `origin` disagrees with `v`,
    /// the original is preserved under `exported_<k>` in `processed`.
    fn add(&mut self, k: &str, v: &str) {
        if v.is_empty() {
            self.processed.remove(k);
            return;
        }
        self.processed.insert(k.to_string(), v.to_string());
        match self.origin.get(k) {
            None => {
                self.origin.insert(k.to_string(), v.to_string());
            }
            Some(ov) if ov != v => {
                self.processed.insert(format!("exported_{k}"), ov.clone());
            }
            Some(_) => {}
        }
    }
}

/// Result of [`build_alert_labels`]: `origin` feeds the `Labels` template
/// variable for annotation rendering; `processed` is the alert's identity
/// (hashed) and persisted label set.
pub(crate) struct BuiltLabels {
    pub origin: BTreeMap<String, String>,
    pub processed: BTreeMap<String, String>,
}

/// Builds an alert's origin/processed label sets from one query result.
/// Port of `AlertingRule.toLabels` (`alerting.go:337-369`): rule labels are
/// rendered as templates (with only `Value`/`Labels`/`Expr` populated —
/// upstream's comment at `alerting.go:352` explains the restriction: alert
/// identity/`ActiveAt` aren't known until *after* labels exist, and allowing
/// broader templating here risks cardinality blowups), then overlaid via
/// [`LabelSet::add`]; `alertname`/`alertgroup` are added last. When
/// `disable_alertgroup_label` is set (from `-disableAlertgroupLabel`), the
/// `alertgroup` label is suppressed everywhere — matching upstream vmalert's
/// `-disableAlertgroupLabel`, which drops the label from every alert it
/// builds. The remote-read restore query must gate its `alertgroup` matcher
/// on the same flag (see `remoteread::build_restore_query`), else the
/// restore filter and these identity labels disagree and restore matches
/// nothing.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_alert_labels(
    m: &Metric,
    rule_labels: &BTreeMap<String, String>,
    rule_name: &str,
    group_name: &str,
    expr: &str,
    disable_alertgroup_label: bool,
    funcs: &Funcs,
    ctx: &EvalContext,
) -> BuiltLabels {
    let mut ls = LabelSet::from_metric(m);
    let value = m.values.first().copied().unwrap_or(0.0);
    let data = build_tpl_data(value, "", &ls.origin, expr, 0, 0, 0, 0.0, false, ctx);
    let rendered = render_map(rule_labels, &data, funcs, ctx);
    for (k, v) in &rendered {
        ls.add(k, v);
    }
    if !rule_name.is_empty() {
        ls.add(ALERT_NAME_LABEL, rule_name);
    }
    if !disable_alertgroup_label && !group_name.is_empty() {
        ls.add(ALERT_GROUP_LABEL, group_name);
    }
    BuiltLabels {
        origin: ls.origin,
        processed: ls.processed,
    }
}

/// Renders every value in `templates` as a Go-template ([`TPL_HEADERS`]
/// preamble prepended, matching `esmalert::templating::validate_template`)
/// against `data`. Port of `templateAnnotations` (`alert.go:148-178`):
/// - a value with no `{{`/`}}` is passed through literally (skips parsing);
/// - a parse or render error does **not** abort the batch — matching
///   upstream, the failing value becomes the error's message and every
///   other key still renders (`alert.go:170-175`).
pub(crate) fn render_map(
    templates: &BTreeMap<String, String>,
    data: &Value,
    funcs: &Funcs,
    ctx: &EvalContext,
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (key, text) in templates {
        if !text.contains("{{") || !text.contains("}}") {
            out.insert(key.clone(), text.clone());
            continue;
        }
        let rendered = Template::parse(&format!("{TPL_HEADERS}{text}"))
            .and_then(|tmpl| tmpl.render(data, funcs, ctx))
            .unwrap_or_else(|e| e.to_string());
        out.insert(key.clone(), rendered);
    }
    out
}

/// Builds the `Value::Map` rendered against for annotation/label templates,
/// providing every field [`TPL_HEADERS`]' preamble declares. Port of
/// `notifier.AlertTplData` (`alert.go:81-92`) plus the two globals
/// (`externalLabels`, `externalURL`) upstream's `tplData` wrapper adds
/// (`alert.go:180-184`).
///
/// Representation choices, documented since our engine has no `Time`/
/// `Duration` type or method dispatch (deferred; see
/// `esm-gotemplate`'s `DECISION on Task 6b`):
/// - `active_at_ms` (unix millis, this crate's `ts` convention) renders as
///   an RFC3339 UTC string (`Value::Str`) — the closest scalar analog to
///   Go's `time.Time` default (`%v`) formatting available without a real
///   `Time` type; plain `{{ $activeAt }}` interpolation reads sensibly,
///   though `.Format`/`.Add`-style method calls remain unsupported.
/// - `for_secs` (the rule's `for:` duration, in seconds) renders as a plain
///   number (`Value::Float`) rather than Go's `2m0s`-style `Duration.String`
///   — again the simplest faithful scalar given no method dispatch.
/// - `alert_id`/`group_id` are `u64` upstream; cast to `i64` for
///   [`Value::Int`] (this crate's only integer variant) — a reinterpret of
///   the bit pattern, fine for opaque display, not for arithmetic.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_tpl_data(
    value: f64,
    alert_type: &str,
    labels: &BTreeMap<String, String>,
    expr: &str,
    alert_id: u64,
    group_id: u64,
    active_at_ms: i64,
    for_secs: f64,
    is_partial: bool,
    ctx: &EvalContext,
) -> Value {
    let labels_map = labels
        .iter()
        .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
        .collect();
    let mut m = BTreeMap::new();
    m.insert("Value".to_string(), Value::Float(value));
    m.insert("Type".to_string(), Value::Str(alert_type.to_string()));
    m.insert("Labels".to_string(), Value::Map(labels_map));
    m.insert("Expr".to_string(), Value::Str(expr.to_string()));
    m.insert("ExternalLabels".to_string(), Value::Map(BTreeMap::new()));
    m.insert(
        "ExternalURL".to_string(),
        Value::Str(ctx.external_url.clone()),
    );
    m.insert("AlertID".to_string(), Value::Int(alert_id as i64));
    m.insert("GroupID".to_string(), Value::Int(group_id as i64));
    m.insert(
        "ActiveAt".to_string(),
        Value::Str(format_active_at(active_at_ms)),
    );
    m.insert("For".to_string(), Value::Float(for_secs));
    m.insert("IsPartial".to_string(), Value::Bool(is_partial));
    Value::Map(m)
}

/// Formats a unix-millis timestamp as RFC3339 seconds-precision UTC.
/// Duplicated (not shared) from the equivalent algorithm in
/// `datasource::client::rfc3339_millis` — that function is private to its
/// module and this repo's established convention (see
/// `esm-gotemplate::value`'s `format_float_go_g` doc comment) is to
/// duplicate small already-verified helpers per module rather than add
/// cross-module plumbing for one function.
fn format_active_at(ms: i64) -> String {
    let unix_secs = ms.div_euclid(1000).max(0) as u64;
    let (y, mo, d, h, mi, s) = civil_from_unix(unix_secs);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

fn civil_from_unix(unix_secs: u64) -> (i64, u64, u64, u64, u64, u64) {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, d, h, m, s)
}

/// Converts an active alert to its `ALERTS` series: the alert's labels plus
/// `__name__=ALERTS` and `alertstate=<state>`, value `1`, at `ts`. Port of
/// `alertToTimeSeries` (`alerting.go:708-724`).
pub(crate) fn alert_time_series(a: &Alert, ts: i64) -> Series {
    let mut labels: Vec<(String, String)> = a
        .labels
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    labels.push((NAME_LABEL.to_string(), ALERT_METRIC.to_string()));
    match labels.iter_mut().find(|(k, _)| k == ALERT_STATE_LABEL) {
        Some(existing) => existing.1 = a.state.as_str().to_string(),
        None => labels.push((ALERT_STATE_LABEL.to_string(), a.state.as_str().to_string())),
    }
    labels.sort_by(|x, y| x.0.cmp(&y.0));
    Series {
        labels,
        samples: vec![Sample {
            value: 1.0,
            timestamp: ts,
        }],
    }
}

/// Converts an active alert to its `ALERTS_FOR_STATE` series: the alert's
/// labels plus `__name__=ALERTS_FOR_STATE`, value = `active_at` as unix
/// **whole seconds** (this crate's `active_at` is unix millis; upstream's
/// `float64(a.ActiveAt.Unix())` truncates to whole seconds — `time.Unix()`
/// drops the sub-second part — so we `div_euclid(1000)` *before* the float
/// cast, matching `format_active_at` above; a plain `/ 1000.0` would leak
/// fractional seconds), at `ts`. Port of `alertForToTimeSeries`
/// (`alerting.go:728-739`). This value is what remote-read restore (a later
/// task) reads back to recover `ActiveAt` across restarts, so it must
/// byte-match upstream's seconds convention.
pub(crate) fn alert_for_time_series(a: &Alert, ts: i64) -> Series {
    let mut labels: Vec<(String, String)> = a
        .labels
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    labels.push((NAME_LABEL.to_string(), ALERT_FOR_STATE_METRIC.to_string()));
    labels.sort_by(|x, y| x.0.cmp(&y.0));
    Series {
        labels,
        samples: vec![Sample {
            value: a.active_at.div_euclid(1000) as f64,
            timestamp: ts,
        }],
    }
}

/// Builds a single StaleNaN-valued series from an alert's label set plus a
/// `__name__` and (for `ALERTS`) an `alertstate`. Shared by
/// [`pending_alert_stale_series`]/[`firing_alert_stale_series`]; matches the
/// label assembly of [`alert_time_series`]/[`alert_for_time_series`] but with
/// `decimal.StaleNaN` as the value, so the series carries the *same* identity
/// as the live one it terminates in remote storage.
fn stale_series(
    labels: &BTreeMap<String, String>,
    name: &str,
    state: Option<&str>,
    ts: i64,
) -> Series {
    let mut out: Vec<(String, String)> =
        labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    out.push((NAME_LABEL.to_string(), name.to_string()));
    if let Some(s) = state {
        out.push((ALERT_STATE_LABEL.to_string(), s.to_string()));
    }
    out.sort_by(|x, y| x.0.cmp(&y.0));
    Series {
        labels: out,
        samples: vec![Sample {
            value: STALE_NAN,
            timestamp: ts,
        }],
    }
}

/// StaleNaN `ALERTS`/`ALERTS_FOR_STATE` series for an alert leaving `Pending`
/// — either resolved (absent, `include_for_state = true`) or promoted to
/// `Firing` (`include_for_state = false`, since no `ALERTS_FOR_STATE` was ever
/// written for a still-pending alert). Port of `pendingAlertStaleTimeSeries`
/// (`alerting.go:741-765`).
pub(crate) fn pending_alert_stale_series(
    labels: &BTreeMap<String, String>,
    ts: i64,
    include_for_state: bool,
) -> Vec<Series> {
    let mut result = vec![stale_series(
        labels,
        ALERT_METRIC,
        Some(AlertState::Pending.as_str()),
        ts,
    )];
    if include_for_state {
        result.push(stale_series(labels, ALERT_FOR_STATE_METRIC, None, ts));
    }
    result
}

/// StaleNaN `ALERTS`+`ALERTS_FOR_STATE` series for an alert leaving `Firing`
/// for `Inactive` (its `keep_firing_for` window elapsed while absent). Port of
/// `firingAlertStaleTimeSeries` (`alerting.go:767-790`).
pub(crate) fn firing_alert_stale_series(labels: &BTreeMap<String, String>, ts: i64) -> Vec<Series> {
    vec![
        stale_series(labels, ALERT_METRIC, Some(AlertState::Firing.as_str()), ts),
        stale_series(labels, ALERT_FOR_STATE_METRIC, None, ts),
    ]
}
