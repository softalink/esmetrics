//! Group-set lifecycle: builds runtime rule groups from parsed config,
//! starts their background evaluation loops, and hot-reloads a changed
//! config in place while preserving live alert state.
//!
//! Port of `app/vmalert/manager.go` (`manager.start`/`update`/`close`),
//! narrowed to the group-set lifecycle; VictoriaMetrics-only bookkeeping
//! (numeric group/rule/alert API lookups by hash, replay) is out of scope —
//! `groups_snapshot`/`alerts_snapshot` instead expose plain-data views for
//! Task 18's JSON API to build those lookups from.
//!
//! Two `Group` types are in play here, aliased explicitly to keep them
//! apart: [`CfgGroup`] (`config::Group`, parsed YAML) and [`RtGroup`]
//! (`rule::Group`, the runtime type `rule::Group::start` spawns an
//! evaluation thread for). [`build_runtime_group`] converts the former into
//! the latter.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use esm_gotemplate::{default_funcs, EvalContext, Funcs, QueryFn};
use serde::Serialize;

use crate::config::{self, Config, Group as CfgGroup, Rule as CfgRule};
use crate::notifier::Notifiers;
use crate::remotewrite::RwClient;
use crate::rule::{
    AlertView, AlertingRule, Group as RtGroup, GroupHandle, Querier, RecordingRule, RuleHealth,
    RuleKind, RuleView,
};

/// Error returned by [`Manager::start`]/[`Manager::reload`]. Never
/// constructed from a panic; always carries a human-readable message.
/// Mirrors [`crate::config::ConfigError`]'s shape.
#[derive(Debug)]
pub struct MgrError {
    msg: String,
}

impl MgrError {
    fn new(msg: impl Into<String>) -> Self {
        MgrError { msg: msg.into() }
    }
}

impl fmt::Display for MgrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for MgrError {}

/// Shared collaborators every group's evaluation loop needs, bundled so
/// [`Manager::start`]/[`Manager::reload`] can hand each group's
/// `RtGroup::start` call its `Arc`-shared datasource/remote-write/notifier
/// handles plus a freshly built `Funcs`/`EvalContext` pair.
pub struct ManagerDeps {
    /// The datasource query path. `Arc` (not owned/`Box`) because every
    /// group's background thread needs its own `'static` handle to the same
    /// underlying client: `RtGroup::start` takes `Arc<dyn Querier + Send +
    /// Sync>`, and cheap `Arc::clone`s share one real `Datasource` (or test
    /// mock) across as many group threads as the config has groups.
    pub querier: Arc<dyn Querier + Send + Sync>,
    /// Startup alert-state restore source (a remote-read-capable `Querier`)
    /// plus the `-remoteRead.lookback` window; `None` disables restore
    /// entirely (every group starts with no prior alert state).
    pub restore: Option<(Arc<dyn Querier + Send + Sync>, Duration)>,
    pub rw: Option<Arc<RwClient>>,
    pub notifiers: Option<Arc<Notifiers>>,
    pub external_url: String,
    pub path_prefix: String,
    /// The `esm_gotemplate` datasource query callback threaded into every
    /// group's `EvalContext`. Kept here rather than a pre-built
    /// `EvalContext`/`Funcs` pair because neither type is `Clone` — see
    /// [`ManagerDeps::build_funcs_ctx`], which builds a fresh pair per group
    /// from this callback.
    pub query_fn: QueryFn,
    /// `-rule.resendDelay` equivalent: no per-group config field exists for
    /// this (it's a global flag upstream too), so every group this
    /// `Manager` starts gets the same value.
    pub resend_delay: Duration,
    /// `-rule.maxResolveDuration` equivalent.
    pub max_resolve_duration: Option<Duration>,
    /// `-evaluationInterval` equivalent: the interval a group uses when its
    /// config omits `interval:`.
    pub default_eval_interval: Duration,
    /// `-group.maxStartDelay` equivalent, passed through to `RtGroup::start`.
    pub max_start_delay: Duration,
    /// `-disableAlertgroupLabel`: threaded into every alerting rule built by
    /// this `Manager` (see [`build_rule`]) so the `alertgroup` label is
    /// suppressed consistently across all groups.
    pub disable_alertgroup_label: bool,
}

impl ManagerDeps {
    /// Builds a fresh `Funcs`/`EvalContext` pair from this `ManagerDeps`.
    /// Called once per group started/reloaded — neither `Funcs` nor
    /// `EvalContext` is `Clone` (see their doc comments in `esm_gotemplate`),
    /// so every group's background thread needs its own pair built from the
    /// shared, `Clone`-able pieces (`external_url`/`path_prefix`/`query_fn`).
    fn build_funcs_ctx(&self) -> (Funcs, EvalContext) {
        let ctx = EvalContext {
            external_url: self.external_url.clone(),
            path_prefix: self.path_prefix.clone(),
            query_fn: Arc::clone(&self.query_fn),
        };
        let funcs = default_funcs(&ctx);
        (funcs, ctx)
    }
}

/// One group's plain-data view for the JSON API (Task 18). Port of the
/// group-facing subset of `rule.ApiGroup` (`rule/rule.go`). `RuleView`/
/// `AlertView`/`RuleHealth` are defined in [`crate::rule`] (they're built by
/// the group's evaluation thread into its live snapshot) and re-used here.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GroupView {
    pub name: String,
    pub checksum: String,
    pub rules: Vec<RuleView>,
}

/// One group's live state as tracked by [`Manager`]: the config-shape rule
/// views and checksum captured at build time (start/reload), plus the
/// handle to its background evaluation thread.
///
/// This deliberately isn't a literal `(RtGroup, GroupHandle)` pair:
/// `RtGroup::start` takes `self` by value and moves the runtime group into
/// its spawned thread, so the `RtGroup` that's actually running is never
/// available to read back from outside that thread. The thread instead
/// republishes a live [`crate::rule::GroupSnapshot`] after every
/// evaluation, read back here via `handle.snapshot()`. The build-time
/// `rules`/`checksum` kept here give a group's config shape (name/expr/id,
/// stable regardless of evaluation timing) that
/// [`Manager::groups_snapshot`] overlays the live health/last_error onto.
struct RunningGroup {
    checksum: String,
    rules: Vec<RuleView>,
    handle: GroupHandle,
}

/// Controls the set of running rule groups: builds them from parsed config,
/// diffs an incoming [`Config`] against what's running on [`Manager::reload`]
/// (by group name -> checksum), and exposes read-only snapshots for the
/// JSON API (Task 18).
///
/// Port of `manager` (`app/vmalert/manager.go`), narrowed to the group-set
/// lifecycle (`start`/`update`/`close`); VictoriaMetrics-only bookkeeping
/// (per-group/rule/alert numeric API lookups, replay) is out of scope.
///
/// Keyed by group **name** (not upstream's `GetID()` group hash): this
/// crate's config model has no per-group `file` field yet (see
/// `config::validate_config`'s doc comment), so name is already the natural
/// single-file-scoped key. A config with two groups sharing a name is
/// rejected by `start`/`reload` rather than silently letting one overwrite
/// the other's running thread.
pub struct Manager {
    groups: HashMap<String, RunningGroup>,
    deps: ManagerDeps,
}

impl Manager {
    /// Builds and starts one runtime group per `cfg`'s groups. Rejects a
    /// config with two groups sharing the same name up front, before
    /// starting any thread — the manager has no way to decide which of two
    /// same-named groups should "win", and starting both then discarding
    /// one's `GroupHandle` on insert would leak its background thread.
    pub fn start(cfg: Config, deps: ManagerDeps) -> Result<Manager, MgrError> {
        reject_duplicate_names(&cfg)?;

        let mut groups = HashMap::with_capacity(cfg.groups.len());
        for cfg_group in &cfg.groups {
            let (runtime_group, rules) = build_runtime_group(cfg_group, &deps);
            let checksum = runtime_group.checksum.clone();
            let handle = start_runtime_group(runtime_group, &deps);
            groups.insert(
                cfg_group.name.clone(),
                RunningGroup {
                    checksum,
                    rules,
                    handle,
                },
            );
        }
        Ok(Manager { groups, deps })
    }

    /// Diffs `cfg` against the currently-running groups by name -> checksum:
    /// - a name present in both with an unchanged checksum is left
    ///   untouched;
    /// - a name present in both with a changed checksum gets the rebuilt
    ///   runtime group sent on its `GroupHandle::update_tx`, which the
    ///   group's own loop applies via `Group::apply_update` (preserving
    ///   live alert state for rules whose identity is unchanged — see
    ///   `config::rule_identity_hash`);
    /// - a name no longer present is stopped (`GroupHandle::stop`) and
    ///   dropped;
    /// - a name not previously present is built and started.
    ///
    /// Port of `manager.update` (`manager.go:113-172`), minus its
    /// restore-on-reload pass (this crate's `Group::start` only restores
    /// once, at initial start — see its doc comment) and its
    /// recording/alerting-rule-requires-rw/notifier precondition check (out
    /// of scope: this crate has no CLI wiring yet to make "is rw/notifier
    /// configured" a meaningful global precondition, and `ManagerDeps`'s
    /// test constructor deliberately allows both disabled).
    pub fn reload(&mut self, cfg: Config) -> Result<(), MgrError> {
        reject_duplicate_names(&cfg)?;
        let mut incoming: HashMap<String, &CfgGroup> = HashMap::with_capacity(cfg.groups.len());
        for g in &cfg.groups {
            incoming.insert(g.name.clone(), g);
        }

        let removed: Vec<String> = self
            .groups
            .keys()
            .filter(|name| !incoming.contains_key(name.as_str()))
            .cloned()
            .collect();
        for name in removed {
            if let Some(rg) = self.groups.remove(&name) {
                rg.handle.stop();
            }
        }

        for (name, running) in self.groups.iter_mut() {
            let Some(cfg_group) = incoming.remove(name.as_str()) else {
                continue;
            };
            let new_checksum = cfg_group.checksum();
            if new_checksum == running.checksum {
                continue;
            }
            let (runtime_group, rules) = build_runtime_group(cfg_group, &self.deps);
            running.checksum = new_checksum;
            running.rules = rules;
            let _ = running.handle.update_tx.send(runtime_group);
        }

        // Whatever's left in `incoming` wasn't previously running: new
        // groups to build and start.
        for (name, cfg_group) in incoming {
            let (runtime_group, rules) = build_runtime_group(cfg_group, &self.deps);
            let checksum = runtime_group.checksum.clone();
            let handle = start_runtime_group(runtime_group, &self.deps);
            self.groups.insert(
                name,
                RunningGroup {
                    checksum,
                    rules,
                    handle,
                },
            );
        }

        Ok(())
    }

    /// Read-only snapshot of every running group's name/checksum/rules, for
    /// the JSON API (Task 18).
    ///
    /// Each rule's config shape (id/name/record/alert/expr) comes from the
    /// build-time `RuleView` captured at start/reload — stable regardless of
    /// evaluation timing — with the **live** `health`/`last_error` overlaid
    /// from the group thread's most recent published snapshot
    /// (`handle.snapshot()`), matched by rule `id`. Before a group's first
    /// evaluation completes the live snapshot is empty, so those rules read
    /// as `RuleHealth::Unknown` (the build-time default) until it does.
    pub fn groups_snapshot(&self) -> Vec<GroupView> {
        self.groups
            .iter()
            .map(|(name, rg)| {
                let live = rg.handle.snapshot();
                let mut rules = rg.rules.clone();
                for rv in &mut rules {
                    if let Some(live_rule) = live.rules.iter().find(|lr| lr.id == rv.id) {
                        rv.health = live_rule.health;
                        rv.last_error = live_rule.last_error.clone();
                    }
                }
                GroupView {
                    name: name.clone(),
                    checksum: rg.checksum.clone(),
                    rules,
                }
            })
            .collect()
    }

    /// Read-only snapshot of every currently-active alert across all running
    /// groups, for the JSON API (Task 18). Read live from each group
    /// thread's most recently published snapshot (`handle.snapshot()`).
    /// A group that hasn't completed its first evaluation contributes no
    /// alerts yet.
    pub fn alerts_snapshot(&self) -> Vec<AlertView> {
        self.groups
            .values()
            .flat_map(|rg| rg.handle.snapshot().alerts)
            .collect()
    }

    /// Stops and joins every running group's background thread.
    pub fn shutdown(self) {
        for (_, rg) in self.groups {
            rg.handle.stop();
        }
    }
}

/// Rejects a config with two groups sharing the same name, before any
/// group is built/started from it.
fn reject_duplicate_names(cfg: &Config) -> Result<(), MgrError> {
    let mut seen = HashSet::with_capacity(cfg.groups.len());
    for g in &cfg.groups {
        if !seen.insert(g.name.as_str()) {
            return Err(MgrError::new(format!(
                "duplicate group name {:?} in config",
                g.name
            )));
        }
    }
    Ok(())
}

/// Converts one parsed `CfgGroup` into a runtime `RtGroup` (every rule built
/// into a `RuleKind`, each carrying a stable identity `id` — see
/// `config::rule_identity_hash`) plus the plain-data `RuleView`s describing
/// them. Returns both because `RtGroup::start` consumes (moves) the runtime
/// group into its evaluation thread, while the `RuleView`s must stay
/// readable from `Manager` afterward (see [`RunningGroup`]'s doc comment).
fn build_runtime_group(cfg_group: &CfgGroup, deps: &ManagerDeps) -> (RtGroup, Vec<RuleView>) {
    let mut rules = Vec::with_capacity(cfg_group.rules.len());
    let mut views = Vec::with_capacity(cfg_group.rules.len());
    for r in &cfg_group.rules {
        let (kind, view) = build_rule(r, &cfg_group.name, deps.disable_alertgroup_label);
        rules.push(kind);
        views.push(view);
    }

    // Upstream: `if g.Concurrency < 1 { g.Concurrency = 1 }` (`group.go:145-146`).
    let concurrency = usize::try_from(cfg_group.concurrency).unwrap_or(1).max(1);

    let group = RtGroup {
        name: cfg_group.name.clone(),
        interval: cfg_group.interval.unwrap_or(deps.default_eval_interval),
        concurrency,
        eval_offset: cfg_group.eval_offset,
        eval_delay: cfg_group.eval_delay,
        // Upstream's config field defaults to "aligned" (`true`) when
        // unset; `RtGroup::eval_alignment`'s own derived `Default` is
        // `false` (documented on that field) — resolved here rather than
        // relying on `RtGroup::default()`.
        eval_alignment: cfg_group.eval_alignment.unwrap_or(true),
        limit: cfg_group.limit.unwrap_or(0),
        rules,
        checksum: cfg_group.checksum(),
        labels: cfg_group.labels.clone(),
        resend_delay: deps.resend_delay,
        max_resolve_duration: deps.max_resolve_duration,
    };
    (group, views)
}

/// Builds a runtime `rule::group::Group` from parsed config for callers that
/// evaluate it synchronously via `Group::eval_once` themselves — instead of
/// starting a `Manager`-owned background thread — e.g. `esmalert-tool`'s
/// offline unit-test runner (`crates/esmalert-tool/src/runner.rs`).
///
/// Unlike [`build_runtime_group`] (`Manager`'s own path, which has no
/// external-labels wiring — no CLI flag exists for it yet), this also
/// overlays `extra_labels` onto every rule's labels before building it,
/// mirroring upstream `rule.NewGroup`'s `labels` parameter
/// (`app/vmalert/rule/group.go:118,166-179`): `extra_labels`, then
/// `cfg_group.labels`, then each rule's own `labels`, each later stage
/// overriding the earlier on a key conflict (`mergeLabels`,
/// `group.go:104-115`). Doesn't return `Vec<RuleView>` (the `Manager`-only
/// build-time snapshot): this caller drives the group directly and has no
/// use for it.
pub fn build_group_for_eval(
    cfg_group: &CfgGroup,
    extra_labels: &BTreeMap<String, String>,
    disable_alertgroup_label: bool,
    default_eval_interval: Duration,
    resend_delay: Duration,
    max_resolve_duration: Option<Duration>,
) -> RtGroup {
    let mut base_labels = extra_labels.clone();
    base_labels.extend(cfg_group.labels.clone());

    let mut rules = Vec::with_capacity(cfg_group.rules.len());
    for r in &cfg_group.rules {
        let (kind, _view) = if base_labels.is_empty() {
            build_rule(r, &cfg_group.name, disable_alertgroup_label)
        } else {
            let mut merged = base_labels.clone();
            merged.extend(r.labels.clone());
            let effective = CfgRule {
                labels: merged,
                ..r.clone()
            };
            build_rule(&effective, &cfg_group.name, disable_alertgroup_label)
        };
        rules.push(kind);
    }

    // Upstream: `if g.Concurrency < 1 { g.Concurrency = 1 }` (`group.go:145-146`).
    let concurrency = usize::try_from(cfg_group.concurrency).unwrap_or(1).max(1);

    RtGroup {
        name: cfg_group.name.clone(),
        interval: cfg_group.interval.unwrap_or(default_eval_interval),
        concurrency,
        eval_offset: cfg_group.eval_offset,
        eval_delay: cfg_group.eval_delay,
        eval_alignment: cfg_group.eval_alignment.unwrap_or(true),
        limit: cfg_group.limit.unwrap_or(0),
        rules,
        checksum: cfg_group.checksum(),
        labels: cfg_group.labels.clone(),
        resend_delay,
        max_resolve_duration,
    }
}

/// Builds one `RuleKind` (+ its `RuleView`) from a parsed `CfgRule`. `id` is
/// `config::rule_identity_hash(r)` — the same expr/kind/name/labels identity
/// upstream's `HashRule` computes — stored on `AlertingRule` so
/// `rule::group::Group::apply_update` can match alerting rules by identity
/// (not name) across a hot-reload.
fn build_rule(
    r: &CfgRule,
    group_name: &str,
    disable_alertgroup_label: bool,
) -> (RuleKind, RuleView) {
    let id = config::rule_identity_hash(r);
    let is_recording = r.record.as_deref().is_some_and(|s| !s.is_empty());

    if is_recording {
        let name = r.record.clone().unwrap_or_default();
        let kind = RuleKind::Recording(RecordingRule {
            id,
            name: name.clone(),
            expr: r.expr.clone(),
            labels: r.labels.clone(),
            health: RuleHealth::Unknown,
            last_error: None,
        });
        let view = RuleView {
            id,
            name: name.clone(),
            record: Some(name),
            alert: None,
            expr: r.expr.clone(),
            health: RuleHealth::Unknown,
            last_error: None,
        };
        (kind, view)
    } else {
        let name = r.alert.clone().unwrap_or_default();
        let kind = RuleKind::Alerting(AlertingRule {
            id,
            name: name.clone(),
            group_name: group_name.to_string(),
            expr: r.expr.clone(),
            r#for: r.r#for.unwrap_or_default(),
            keep_firing_for: r.keep_firing_for.unwrap_or_default(),
            labels: r.labels.clone(),
            annotations: r.annotations.clone(),
            disable_alertgroup_label,
            alerts: HashMap::new(),
            health: RuleHealth::Unknown,
            last_error: None,
        });
        let view = RuleView {
            id,
            name: name.clone(),
            record: None,
            alert: Some(name),
            expr: r.expr.clone(),
            health: RuleHealth::Unknown,
            last_error: None,
        };
        (kind, view)
    }
}

/// Spawns one runtime group's background evaluation loop via
/// `RtGroup::start`, threading through `deps`'s shared collaborators: a
/// fresh `Funcs`/`EvalContext` pair for this group (see
/// [`ManagerDeps::build_funcs_ctx`]) and `Arc::clone`s of the
/// querier/remote-write/notifiers/restore handles so every group's thread
/// shares the same underlying clients without deep-copying them.
fn start_runtime_group(group: RtGroup, deps: &ManagerDeps) -> GroupHandle {
    let (funcs, ctx) = deps.build_funcs_ctx();
    group.start(
        Arc::clone(&deps.querier),
        funcs,
        ctx,
        deps.rw.clone(),
        deps.notifiers.clone(),
        deps.external_url.clone(),
        deps.restore.clone(),
        deps.max_start_delay,
    )
}

/// Test-only constructor, `pub(crate)` so other in-crate test modules (e.g.
/// `web::api`'s Task 18 tests) can build a `Manager` without duplicating
/// this wiring. See the doc comment on the (former) inline impl for the
/// rationale of each disabled/zeroed field.
#[cfg(test)]
impl ManagerDeps {
    /// A caller-supplied mock `Querier`, remote-write and notifiers disabled
    /// (`None`), and a zero `max_start_delay` so a started group's
    /// background thread doesn't sit idle during a test (matches
    /// `rule::group`'s own `start_and_stop_does_not_hang` test convention).
    pub(crate) fn for_test(querier: Arc<dyn Querier + Send + Sync>) -> Self {
        ManagerDeps {
            querier,
            restore: None,
            rw: None,
            notifiers: None,
            external_url: "http://vm".to_string(),
            path_prefix: String::new(),
            query_fn: Arc::new(|_| Ok(vec![])),
            resend_delay: Duration::ZERO,
            max_resolve_duration: None,
            default_eval_interval: Duration::from_secs(3600),
            max_start_delay: Duration::ZERO,
            disable_alertgroup_label: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse_config_str;
    use crate::datasource::{DsError, QueryResult};

    struct EmptyQuerier;
    impl Querier for EmptyQuerier {
        fn query(&self, _expr: &str, _ts: i64) -> Result<QueryResult, DsError> {
            Ok(QueryResult {
                data: vec![],
                is_partial: None,
            })
        }
    }

    #[test]
    fn build_rule_threads_disable_alertgroup_label_into_alerting_rule() {
        let cfg_rule = CfgRule {
            alert: Some("A".to_string()),
            expr: "up == 0".to_string(),
            ..CfgRule::default()
        };

        let (enabled_kind, _) = build_rule(&cfg_rule, "g", false);
        match enabled_kind {
            RuleKind::Alerting(ar) => assert!(!ar.disable_alertgroup_label),
            other => panic!("expected an alerting rule, got {other:?}"),
        }

        let (disabled_kind, _) = build_rule(&cfg_rule, "g", true);
        match disabled_kind {
            RuleKind::Alerting(ar) => assert!(ar.disable_alertgroup_label),
            other => panic!("expected an alerting rule, got {other:?}"),
        }
    }

    fn two_group_config(g1_expr: &str, g2_expr: &str) -> Config {
        // NOTE: built with explicit `\n  ` sequences rather than Rust's
        // backslash-newline literal continuation — the latter strips all
        // leading whitespace from the continued line, which would destroy
        // this YAML's (significant) indentation.
        let yaml = format!(
            "groups:\n  - name: g1\n    interval: 3600s\n    rules:\n      - record: r1\n        expr: {g1_expr}\n  - name: g2\n    interval: 3600s\n    rules:\n      - record: r2\n        expr: {g2_expr}\n"
        );
        parse_config_str(&yaml).expect("parse test config")
    }

    #[test]
    fn reload_swaps_changed_group_preserving_others() {
        let deps = ManagerDeps::for_test(Arc::new(EmptyQuerier));
        let mut mgr = Manager::start(two_group_config("up", "down"), deps).expect("manager start");

        let before = mgr.groups_snapshot();
        assert_eq!(before.len(), 2);
        let g1_before = before.iter().find(|g| g.name == "g1").unwrap();
        let g2_before = before.iter().find(|g| g.name == "g2").unwrap();

        // g1's expr changes (-> different checksum); g2 is byte-identical.
        mgr.reload(two_group_config("up > 1", "down"))
            .expect("reload");

        let after = mgr.groups_snapshot();
        assert_eq!(after.len(), 2, "both groups must still be present");
        let g1_after = after.iter().find(|g| g.name == "g1").unwrap();
        let g2_after = after.iter().find(|g| g.name == "g2").unwrap();

        assert_ne!(
            g1_before.checksum, g1_after.checksum,
            "g1's checksum should change after its expr changed"
        );
        assert_eq!(
            g2_before.checksum, g2_after.checksum,
            "g2's checksum should be unchanged (untouched by the reload)"
        );
        assert_eq!(g1_after.rules[0].expr, "up > 1");
        assert_eq!(g2_after.rules[0].expr, "down");

        mgr.shutdown();
    }

    #[test]
    fn reload_stops_removed_group_and_starts_added_group() {
        let deps = ManagerDeps::for_test(Arc::new(EmptyQuerier));
        let mut mgr = Manager::start(two_group_config("up", "down"), deps).expect("manager start");

        let yaml = "groups:\n  - name: g2\n    interval: 3600s\n    rules:\n      - record: r2\n        expr: down\n  - name: g3\n    interval: 3600s\n    rules:\n      - record: r3\n        expr: sideways\n";
        mgr.reload(parse_config_str(yaml).unwrap()).expect("reload");

        let names: HashSet<String> = mgr.groups_snapshot().into_iter().map(|g| g.name).collect();
        assert_eq!(
            names,
            HashSet::from(["g2".to_string(), "g3".to_string()]),
            "g1 should be stopped/dropped, g2 kept, g3 newly started"
        );

        mgr.shutdown();
    }

    #[test]
    fn groups_snapshot_reports_rule_view_fields() {
        let deps = ManagerDeps::for_test(Arc::new(EmptyQuerier));
        let yaml = "groups:\n  - name: g1\n    interval: 3600s\n    rules:\n      - alert: HighLoad\n        expr: node_load1 > 5\n";
        let mgr = Manager::start(parse_config_str(yaml).unwrap(), deps).expect("manager start");

        let snap = mgr.groups_snapshot();
        let g1 = snap.iter().find(|g| g.name == "g1").unwrap();
        assert_eq!(g1.rules.len(), 1);
        assert_eq!(g1.rules[0].alert.as_deref(), Some("HighLoad"));
        assert_eq!(g1.rules[0].record, None);
        assert_eq!(g1.rules[0].expr, "node_load1 > 5");

        mgr.shutdown();
    }

    #[test]
    fn build_group_for_eval_applies_label_priority_rule_over_group_over_external() {
        let yaml = "groups:\n  - name: g1\n    interval: 30s\n    labels:\n      env: group_env\n      dc: group_dc\n    rules:\n      - alert: A\n        expr: up\n        labels:\n          env: rule_env\n";
        let cfg = parse_config_str(yaml).unwrap();

        let mut extra = BTreeMap::new();
        extra.insert("env".to_string(), "external_env".to_string());
        extra.insert("region".to_string(), "external_region".to_string());

        let group = build_group_for_eval(
            &cfg.groups[0],
            &extra,
            false,
            Duration::from_secs(3600),
            Duration::ZERO,
            None,
        );

        assert_eq!(group.name, "g1");
        assert_eq!(group.interval, Duration::from_secs(30));
        match &group.rules[0] {
            RuleKind::Alerting(ar) => {
                // Rule's own label wins over the group label...
                assert_eq!(ar.labels.get("env").map(String::as_str), Some("rule_env"));
                // ...group label wins over external (dc has no rule override)...
                assert_eq!(ar.labels.get("dc").map(String::as_str), Some("group_dc"));
                // ...and an external-only label passes through untouched.
                assert_eq!(
                    ar.labels.get("region").map(String::as_str),
                    Some("external_region")
                );
            }
            other => panic!("expected an alerting rule, got {other:?}"),
        }
    }

    #[test]
    fn build_group_for_eval_defaults_interval_when_unset() {
        let cfg = parse_config_str(
            "groups:\n  - name: g1\n    rules:\n      - record: r\n        expr: up\n",
        )
        .unwrap();
        let group = build_group_for_eval(
            &cfg.groups[0],
            &BTreeMap::new(),
            false,
            Duration::from_secs(3600),
            Duration::ZERO,
            None,
        );
        assert_eq!(group.interval, Duration::from_secs(3600));
    }

    #[test]
    fn start_rejects_duplicate_group_names() {
        let deps = ManagerDeps::for_test(Arc::new(EmptyQuerier));
        let yaml = "groups:\n  - name: g1\n    rules:\n      - record: r1\n        expr: up\n  - name: g1\n    rules:\n      - record: r2\n        expr: down\n";
        match Manager::start(parse_config_str(yaml).unwrap(), deps) {
            Err(e) => assert!(e.to_string().contains("duplicate")),
            Ok(_) => panic!("expected duplicate group name to be rejected"),
        }
    }

    /// A `Querier` that always returns one present sample, so a `for: 0`
    /// alerting rule fires on its first evaluation.
    struct PresentQuerier;
    impl Querier for PresentQuerier {
        fn query(&self, _expr: &str, _ts: i64) -> Result<QueryResult, DsError> {
            Ok(QueryResult {
                data: vec![crate::datasource::Metric {
                    labels: vec![("instance".into(), "h1".into())],
                    timestamps: vec![0],
                    values: vec![1.0],
                }],
                is_partial: None,
            })
        }
    }

    #[test]
    fn alerts_snapshot_reflects_live_firing_alert_from_running_group() {
        // Interval is long, but `max_start_delay=0` + immediate-first-eval
        // means the started group fires its `for: 0` alert almost at once.
        // Poll the manager's live `alerts_snapshot()` with a short bounded
        // wait (no fixed sleep) so this stays deterministic without racing.
        let deps = ManagerDeps::for_test(Arc::new(PresentQuerier));
        let yaml = "groups:\n  - name: g1\n    interval: 3600s\n    rules:\n      - alert: A\n        expr: up\n";
        let mgr = Manager::start(parse_config_str(yaml).unwrap(), deps).expect("manager start");

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let alerts = loop {
            let alerts = mgr.alerts_snapshot();
            if !alerts.is_empty() || std::time::Instant::now() >= deadline {
                break alerts;
            }
            std::thread::yield_now();
        };

        assert_eq!(
            alerts.len(),
            1,
            "the running group's firing alert must surface"
        );
        assert_eq!(alerts[0].alertname, "A");
        assert_eq!(alerts[0].group, "g1");
        assert_eq!(alerts[0].state, "firing");
        assert_eq!(
            alerts[0].labels.get("instance").map(String::as_str),
            Some("h1")
        );

        // And the group's rule view shows a healthy (successfully evaluated) rule.
        let groups = mgr.groups_snapshot();
        let g1 = groups.iter().find(|g| g.name == "g1").unwrap();
        assert_eq!(g1.rules[0].health, RuleHealth::Ok);

        mgr.shutdown();
    }
}
