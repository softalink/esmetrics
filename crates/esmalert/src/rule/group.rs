//! `Group`: a set of rules sharing one evaluation interval, and the
//! synchronous per-tick evaluation (`eval_once`) plus the background
//! interval-loop thread (`start`) around it.
//!
//! Port of `app/vmalert/rule/group.go`'s `Group` type, `Start` (`:346-420`),
//! `restore` (`:225-246`), and `updateWith` (`:252-291`) — narrowed to what
//! this crate's rule evaluator needs; VictoriaMetrics-only bookkeeping
//! (metrics, group/file IDs, replay, `-rule.stripFilePath`) is out of scope.
//! The pure timestamp/duration math `Start`'s loop relies on
//! (`adjustReqTimestamp`/`getEvalDelay`, `getResolveDuration`,
//! `delayBeforeStart`) lives in [`super::timing`], split out to keep this
//! file under this crate's file-size convention.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use esm_gotemplate::{EvalContext, Funcs};

use super::executor::exec_concurrently;
use super::snapshot::{build_snapshot, GroupSnapshot};
use super::timing::{adjust_req_timestamp, delay_before_start, get_resolve_duration, now_ms};
use super::{Alert, AlertingRule, Querier, RecordingRule, RuleError, RuleHealth};
use crate::notifier::Notifiers;
use crate::remotewrite::RwClient;
use crate::series::Series;

/// One rule in a [`Group`]: either evaluates to a set of time series
/// (recording) or drives the `Pending`/`Firing`/`Inactive` alert state
/// machine (alerting). Port of the `Rule` interface (`rule.go`), collapsed
/// into a closed enum since this crate has exactly two rule kinds.
#[derive(Debug)]
pub enum RuleKind {
    Alerting(AlertingRule),
    Recording(RecordingRule),
}

impl RuleKind {
    /// Dispatches to the underlying rule's `exec`, matching each variant's
    /// own signature (`limit` is unused by `AlertingRule::exec` — upstream's
    /// per-rule result-count limit doesn't apply to alerting rules in this
    /// port; see `AlertingRule::exec`'s doc comment).
    pub(crate) fn exec(
        &mut self,
        q: &dyn Querier,
        ts: i64,
        funcs: &Funcs,
        ctx: &EvalContext,
        limit: i64,
    ) -> Result<Vec<Series>, RuleError> {
        match self {
            RuleKind::Alerting(ar) => ar.exec(q, ts, funcs, ctx),
            RuleKind::Recording(rr) => rr.exec(q, ts, limit),
        }
    }

    pub(crate) fn name(&self) -> &str {
        match self {
            RuleKind::Alerting(ar) => &ar.name,
            RuleKind::Recording(rr) => &rr.name,
        }
    }

    /// Records the outcome of this rule's most recent `exec` onto the rule
    /// itself (for the live [`super::GroupSnapshot`]): success clears the
    /// error and sets health `Ok`; failure stores the message and sets
    /// health `Err`. Called by [`Group::eval_once`] for every rule after
    /// [`exec_concurrently`] returns.
    fn record_eval(&mut self, result: &Result<Vec<Series>, RuleError>) {
        let (health, last_error) = match result {
            Ok(_) => (RuleHealth::Ok, None),
            Err(e) => (RuleHealth::Err, Some(e.to_string())),
        };
        match self {
            RuleKind::Alerting(ar) => {
                ar.health = health;
                ar.last_error = last_error;
            }
            RuleKind::Recording(rr) => {
                rr.health = health;
                rr.last_error = last_error;
            }
        }
    }
}

/// A named set of rules sharing one evaluation `interval` and concurrency
/// budget. Port of `Group` (`group.go:46-77`), narrowed to the
/// evaluation-relevant fields; VictoriaMetrics-only bookkeeping (`id`,
/// metrics, `File`/`Type`, params/headers passed to the datasource
/// separately) is out of scope for this task.
#[derive(Debug, Default)]
pub struct Group {
    pub name: String,
    pub interval: Duration,
    pub concurrency: usize,
    pub eval_offset: Option<Duration>,
    pub eval_delay: Option<Duration>,
    /// Whether the evaluation timestamp is truncated down to the nearest
    /// `interval` boundary. Upstream's equivalent (`Group.evalAlignment`,
    /// `*bool`) defaults to "aligned" (`true`) when unset in config; this
    /// plain `bool`'s derived `Default` is `false` — a caller building a
    /// `Group` from parsed config (a later task) must resolve config's
    /// `Option<bool>` to `true` when absent to match upstream, rather than
    /// relying on this type's own default.
    pub eval_alignment: bool,
    pub limit: i64,
    pub rules: Vec<RuleKind>,
    pub checksum: String,
    pub labels: BTreeMap<String, String>,
    /// Port of upstream's `-rule.resendDelay` global flag (default `0`,
    /// `group.go:33`): minimum time before resending an already-sent alert.
    /// A plain field here (rather than a global) since this crate has no
    /// flag-parsing wired into the rule engine yet.
    pub resend_delay: Duration,
    /// Port of upstream's `-rule.maxResolveDuration` global flag (default
    /// unset, `group.go:31-32`): `None`/`Some(Duration::ZERO)` both mean
    /// "unset" (matching upstream's `maxDuration > 0` guard).
    pub max_resolve_duration: Option<Duration>,
}

impl Group {
    /// Runs every rule once at `ts`: executes them all (via
    /// [`exec_concurrently`], respecting `self.concurrency`), pushes every
    /// successfully-produced series to `rw` (if given), and — for alerting
    /// rules — sends the resulting `alerts_to_send` batch to `notifiers`
    /// (if given). A rule execution error is logged and that rule's round
    /// is simply skipped; this never aborts the other rules and never
    /// panics.
    ///
    /// Returns every rule-evaluation error encountered this tick (in
    /// arbitrary order), so a caller that needs to *observe* failures can —
    /// mirroring upstream `Group.ExecOnce`, which returns a `chan error`
    /// (`group.go:652-662`). The live daemon ([`Group::start`]'s loop)
    /// ignores the returned `Vec`: it has already logged each error and must
    /// keep evaluating on the next tick. The `esmalert-tool` unittest runner,
    /// by contrast, treats any returned error as a hard test-group failure
    /// (matching upstream `unittest.go:381-388`). An empty `Vec` means every
    /// rule evaluated cleanly.
    ///
    /// `ts` is expected to already be the *adjusted* evaluation timestamp —
    /// see [`adjust_req_timestamp`], which the caller (this method isn't
    /// the caller; [`Group::start`]'s tick loop is) applies before invoking
    /// this method. Keeping that adjustment out of `eval_once` itself is
    /// what makes it a pure, directly-testable function of its arguments.
    ///
    /// Port of the `eval` closure inside `Group.Start` (`group.go:388-411`)
    /// plus `executor.exec` (`:769-816`), collapsed into one synchronous
    /// method. `resolveDuration` (upstream computes it once per tick,
    /// outside `exec`) is instead computed here, internally, via
    /// [`get_resolve_duration`] from `self.interval`/`self.resend_delay`/
    /// `self.max_resolve_duration` — `eval_once`'s signature has no
    /// parameter for it, per this task's fixed interface.
    ///
    /// endsAt wiring: upstream's notifier `send` step mutates each `Alert`'s
    /// persisted `End` field (`rule/alerting.go:879-885`) to `now +
    /// resolveDuration` for a still-firing alert before sending. This
    /// port's [`Alert`] has no separate `End` field (see its doc comment) —
    /// only `resolved_at`, which also drives eviction (`RESOLVED_RETENTION_MS`)
    /// — so mutating `self`'s persisted alerts here would corrupt that.
    /// Instead, the already-cloned `Vec<Alert>` [`AlertingRule::alerts_to_send`]
    /// returns has its `resolved_at` overwritten in place (a mutation of the
    /// *outgoing* batch only, never of `self`) for every alert that isn't
    /// already actually resolved, so the notifier — which writes `endsAt`
    /// exactly when `resolved_at.is_some()` — gets an auto-expiry deadline
    /// for still-firing alerts too.
    #[allow(clippy::too_many_arguments)]
    pub fn eval_once(
        &mut self,
        q: &(dyn Querier + Sync),
        ts: i64,
        funcs: &Funcs,
        ctx: &EvalContext,
        rw: Option<&RwClient>,
        notifiers: Option<&Notifiers>,
        external_url: &str,
    ) -> Vec<RuleError> {
        if self.rules.is_empty() {
            return Vec::new();
        }

        let resolve_duration =
            get_resolve_duration(self.interval, self.resend_delay, self.max_resolve_duration);
        let resolve_ms = i64::try_from(resolve_duration.as_millis()).unwrap_or(i64::MAX);

        let results = exec_concurrently(
            &mut self.rules,
            q,
            ts,
            self.concurrency,
            funcs,
            ctx,
            self.limit,
        );

        let mut eval_errors = Vec::new();
        for (idx, result) in results {
            // Record every rule's health/last_error onto the rule itself so
            // the live snapshot reflects it — the error is no longer merely
            // logged and discarded.
            self.rules[idx].record_eval(&result);
            let series = match result {
                Ok(series) => series,
                Err(e) => {
                    log::warn!(
                        "group {:?}: rule {:?} failed to execute: {e}",
                        self.name,
                        self.rules[idx].name()
                    );
                    // Surface the error to callers that need it (the unittest
                    // runner) while the daemon path still logs and continues.
                    eval_errors.push(e);
                    continue;
                }
            };

            if let Some(rw) = rw {
                for s in series {
                    rw.push(s);
                }
            }

            let RuleKind::Alerting(ar) = &self.rules[idx] else {
                continue;
            };
            let mut alerts = ar.alerts_to_send(ts, resolve_duration, self.resend_delay);
            if alerts.is_empty() {
                continue;
            }
            for a in &mut alerts {
                if a.resolved_at.is_none() {
                    a.resolved_at = Some(ts.saturating_add(resolve_ms));
                }
            }
            if let Some(notifiers) = notifiers {
                for (target_idx, err) in notifiers.send(&alerts, external_url) {
                    log::warn!(
                        "group {:?}: rule {:?}: notifier target {target_idx} failed: {err}",
                        self.name,
                        ar.name
                    );
                }
            }
        }

        eval_errors
    }

    /// Applies an updated `Group` definition in place, preserving live
    /// alert state for rules that still exist. Port of `updateWith`
    /// (`group.go:252-291`).
    ///
    /// Alerting rules are matched across the old/new definitions by their
    /// stable `id` (`AlertingRule::id`, computed by `manager::build_rule`
    /// via `config::rule_identity_hash` — the same expr/kind/name/labels
    /// identity upstream's `HashRule`/`Rule.ID()` computes), not by name
    /// alone. A rule whose `expr` or `labels` changed therefore gets a new
    /// id and correctly starts fresh rather than inheriting a
    /// no-longer-applicable rule's alert state; a rule that's unchanged (or
    /// changed only in a non-identity field, e.g. `annotations`) keeps its
    /// id and its live alerts carry over. Recording rules carry no live
    /// state, so they're simply replaced.
    fn apply_update(&mut self, new_group: Group) {
        let mut old_alerts: HashMap<u64, HashMap<u64, Alert>> = HashMap::new();
        for r in self.rules.drain(..) {
            if let RuleKind::Alerting(ar) = r {
                old_alerts.insert(ar.id, ar.alerts);
            }
        }

        let mut new_rules = new_group.rules;
        for r in &mut new_rules {
            if let RuleKind::Alerting(ar) = r {
                if let Some(alerts) = old_alerts.remove(&ar.id) {
                    ar.alerts = alerts;
                }
            }
        }

        self.name = new_group.name;
        self.interval = new_group.interval;
        self.concurrency = new_group.concurrency;
        self.eval_offset = new_group.eval_offset;
        self.eval_delay = new_group.eval_delay;
        self.eval_alignment = new_group.eval_alignment;
        self.limit = new_group.limit;
        self.checksum = new_group.checksum;
        self.labels = new_group.labels;
        self.resend_delay = new_group.resend_delay;
        self.max_resolve_duration = new_group.max_resolve_duration;
        self.rules = new_rules;
    }

    /// Builds a live [`GroupSnapshot`] of this group's current rule health
    /// and active alerts. Exposed so callers driving [`Group::eval_once`]
    /// directly (tests, and the background loop in [`Group::start`]) can
    /// read the group's state without moving its rules out.
    ///
    /// Not called by `main`'s wiring (Task 19): the JSON API instead reads
    /// the *published* snapshot via [`GroupHandle::snapshot`], since the
    /// running `Group` itself has been moved into its evaluation thread by
    /// then (see `manager::RunningGroup`'s doc comment).
    #[allow(dead_code)]
    pub fn snapshot(&self) -> GroupSnapshot {
        build_snapshot(&self.rules)
    }

    /// Spawns the group's interval-evaluation loop on a background thread
    /// and returns a [`GroupHandle`] to control it. Port of `Group.Start`
    /// (`group.go:346-420`): a deterministic (never `rand`) start-delay
    /// sleep, then — once, before the first regular tick — an alert-state
    /// [`AlertingRule::restore`] pass if `restore` is given, then a loop
    /// that evaluates once per `self.interval` while also watching for a
    /// stop signal or a live update, applied via [`Group::apply_update`].
    ///
    /// Divergences from upstream, all deliberate simplifications given this
    /// task's "test `eval_once`, not wall-clock timing" scope:
    /// - no missed-tick drift correction (upstream's `offset`/`missed`
    ///   arithmetic in the `<-t.C` branch, `group.go:472-491`): each pass
    ///   through the loop waits up to one full `self.interval` from when it
    ///   last returned, so a slow evaluation delays the next tick's start
    ///   rather than the loop trying to catch up;
    /// - `eval_offset`'s effect on the *first* tick's phase (upstream's
    ///   `delayBeforeStart` offset-aligned branch, `group.go:509-520`) isn't
    ///   ported — [`delay_before_start`] always uses the name-hash spread,
    ///   regardless of `eval_offset`.
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        mut self,
        q: Arc<dyn Querier + Send + Sync>,
        funcs: Funcs,
        ctx: EvalContext,
        rw: Option<Arc<RwClient>>,
        notifiers: Option<Arc<Notifiers>>,
        external_url: String,
        restore: Option<(Arc<dyn Querier + Send + Sync>, Duration)>,
        max_start_delay: Duration,
    ) -> GroupHandle {
        let (update_tx, update_rx) = crossbeam_channel::unbounded::<Group>();
        let (stop_tx, stop_rx) = crossbeam_channel::bounded::<()>(1);

        // Live snapshot the loop republishes after every evaluation; the
        // manager reads it through the returned `GroupHandle` (the group
        // thread owns the rules, so this shared, after-each-eval-rebuilt
        // plain-data copy is how their state is exposed outside the thread).
        let live = Arc::new(Mutex::new(GroupSnapshot::default()));
        let live_for_thread = Arc::clone(&live);

        let join = thread::spawn(move || {
            // A single tick: adjust the wall-clock timestamp, evaluate every
            // rule once, and republish the live snapshot. Takes `&mut Group`
            // (rather than capturing `self`) so it can be called both for
            // the immediate first evaluation and inside the steady-state
            // loop without a borrow conflict; captures the per-run inputs
            // (`q`/`funcs`/... and `live_for_thread`) by reference.
            let run_eval = |g: &mut Group| {
                let ts = adjust_req_timestamp(
                    now_ms(),
                    g.interval,
                    g.eval_offset,
                    g.eval_delay,
                    g.eval_alignment,
                );
                g.eval_once(
                    q.as_ref(),
                    ts,
                    &funcs,
                    &ctx,
                    rw.as_deref(),
                    notifiers.as_deref(),
                    &external_url,
                );
                if let Ok(mut guard) = live_for_thread.lock() {
                    *guard = build_snapshot(&g.rules);
                }
            };

            let start_delay = delay_before_start(&self.name, self.interval, max_start_delay);
            if matches!(
                wait_or_apply_updates(
                    &mut self,
                    &stop_rx,
                    &update_rx,
                    &live_for_thread,
                    start_delay
                ),
                WaitOutcome::Stop
            ) {
                return;
            }

            // Restore alert state before the first evaluation, so that
            // first eval sees restored `active_at` progress (upstream runs
            // restore right after creating the ticker and before the
            // steady-state loop, `group.go:427-439`).
            if let Some((rq, lookback)) = &restore {
                let restore_ts = now_ms();
                for r in self.rules.iter_mut() {
                    if let RuleKind::Alerting(ar) = r {
                        if ar.r#for.is_zero() {
                            continue;
                        }
                        if let Err(e) = ar.restore(rq.as_ref(), restore_ts, *lookback) {
                            log::warn!(
                                "group {:?}: restore failed for rule {:?}: {e}",
                                self.name,
                                ar.name
                            );
                        }
                    }
                }
            }

            // Immediate first evaluation, before entering the interval
            // loop — matching upstream, which evaluates once right away and
            // only then waits out each `interval` (`group.go:437` +
            // `:441-491`). Without this, an hour-interval group would sit
            // idle for a full extra hour after every start/reload.
            run_eval(&mut self);

            loop {
                let interval = self.interval;
                match wait_or_apply_updates(
                    &mut self,
                    &stop_rx,
                    &update_rx,
                    &live_for_thread,
                    interval,
                ) {
                    WaitOutcome::Stop => return,
                    WaitOutcome::Elapsed => run_eval(&mut self),
                }
            }
        });

        GroupHandle {
            update_tx,
            stop_tx,
            join,
            live,
        }
    }
}

/// Outcome of [`wait_or_apply_updates`]'s bounded wait.
enum WaitOutcome {
    /// A stop signal arrived; the caller must return without evaluating.
    Stop,
    /// The requested duration elapsed with no stop signal (updates, if
    /// any arrived during the wait, were already applied in place).
    Elapsed,
}

/// Waits up to `timeout`, applying any `Group` sent on `update_rx` in place
/// via [`Group::apply_update`] as soon as it arrives (without resetting the
/// remaining wait — matching upstream's `randSleep`/main-loop `select`,
/// which keeps waiting on the *same* timer after handling an update,
/// `group.go:363-376` and `:457-467`), and returning early if `stop_rx`
/// fires.
///
/// After each applied update the live snapshot in `live` is rebuilt and
/// republished immediately (same build-then-lock-briefly pattern
/// [`Group::start`]'s `run_eval` uses; a poisoned lock is tolerated, never
/// panicked on). Without this, a hot-reload's new rule set — and, crucially,
/// the *removal* of alerts for rules that no longer exist — wouldn't reach
/// snapshot readers until the group's next scheduled tick, up to a full
/// `interval` later.
fn wait_or_apply_updates(
    group: &mut Group,
    stop_rx: &Receiver<()>,
    update_rx: &Receiver<Group>,
    live: &Mutex<GroupSnapshot>,
    timeout: Duration,
) -> WaitOutcome {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return WaitOutcome::Elapsed;
        }
        crossbeam_channel::select! {
            recv(stop_rx) -> _ => return WaitOutcome::Stop,
            recv(update_rx) -> msg => {
                if let Ok(ng) = msg {
                    group.apply_update(ng);
                    let snapshot = build_snapshot(&group.rules);
                    if let Ok(mut guard) = live.lock() {
                        *guard = snapshot;
                    }
                }
            }
            default(remaining) => return WaitOutcome::Elapsed,
        }
    }
}

/// Handle to a running [`Group::start`] background thread.
///
/// Send an updated [`Group`] on `update_tx` to hot-reload it in place
/// (preserving live alert state, see [`Group::apply_update`]); call
/// [`GroupHandle::stop`] to shut the loop down and join its thread.
pub struct GroupHandle {
    pub update_tx: Sender<Group>,
    pub stop_tx: Sender<()>,
    join: JoinHandle<()>,
    /// The live snapshot the loop republishes after every evaluation; read
    /// via [`GroupHandle::snapshot`].
    live: Arc<Mutex<GroupSnapshot>>,
}

impl GroupHandle {
    /// Returns the group's most recently published live snapshot (rule
    /// health + active alerts). Empty ([`GroupSnapshot::default`]) until the
    /// loop's first evaluation completes. Never panics: a poisoned lock
    /// yields an empty snapshot rather than unwinding.
    pub fn snapshot(&self) -> GroupSnapshot {
        self.live
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Signals the loop to stop and joins its thread. Never panics even if
    /// the loop thread already exited on its own (the send/join errors are
    /// ignored — a closed stop channel or an already-finished thread both
    /// mean there's nothing left to stop).
    pub fn stop(self) {
        let _ = self.stop_tx.send(());
        let _ = self.join.join();
    }
}

#[cfg(test)]
#[path = "group_tests.rs"]
mod tests;
