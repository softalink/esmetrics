//! Runner core for `esmalert-tool`: builds runtime rule groups from a parsed
//! test file's `rule_files`, drives an eval loop against a [`Harness`], and
//! checks `alert_rule_test` and `metricsql_expr_test` assertions.
//!
//! Port of VictoriaMetrics `app/vmalert-tool/unittest/unittest.go:316-474`
//! (`testGroup.test`, the eval loop, `ExecOnce`/`DebugFlush`),
//! `unittest/alerting.go` (the labels+annotations set-match comparison), and
//! `unittest/recording.go` (`checkMetricsqlCase`'s sample comparison).
//! Deliberate scope narrowing vs. upstream is documented inline below.

// Scaffold stage: not wired into `main()` yet (a later task) — its `pub`
// items are only used from this module's own tests until then, mirroring
// `schema.rs`/`input.rs`/`harness.rs`'s `#![allow(dead_code)]` convention.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use esm_gotemplate::{default_funcs, EvalContext};

use esmalert::config::{load_config, validate_config};
use esmalert::datasource::{AuthConfig, Datasource, TlsConfig, DEFAULT_QUERY_TIMEOUT};
use esmalert::remotewrite::{RwClient, RwConfig, DEFAULT_SEND_TIMEOUT};
use esmalert::rule::{AlertState, Group as RtGroup, RuleKind};

use crate::harness::Harness;
use crate::input::expand_series;
use crate::recording::check_metricsql_expr_test_case;
use crate::schema::{AlertTestCase, TestGroup};
use crate::ToolError;

/// Fixed base timestamp every `input_series` sample offset and
/// `alert_rule_test`/`metricsql_expr_test` `eval_time` offset is computed
/// relative to. Port of upstream's `testStartTime = time.Unix(0, 0).UTC()`
/// (`unittest/unittest.go:48`) — unix epoch, expressed in millis (this
/// crate's timestamp convention).
const TEST_START_MS: i64 = 0;

/// A placeholder `external_url`, threaded into every group's `EvalContext`
/// (used by `$externalURL` template interpolation) and into `eval_once`'s
/// notifier-facing `external_url` argument. No notifier is ever configured
/// here (`run_test_group` always passes `None`), so this value is never
/// actually sent anywhere — it exists only because `eval_once`'s signature
/// requires one.
const EXTERNAL_URL: &str = "http://esmalert-tool.local";

/// The outcome of running one [`TestGroup`]'s test cases: a human-readable
/// diff string per failed `alert_rule_test` case. Empty means every case
/// passed.
#[derive(Debug, Default)]
pub struct GroupResult {
    pub diffs: Vec<String>,
}

/// Runs one [`TestGroup`]'s `input_series` + `alert_rule_test` +
/// `metricsql_expr_test` cases against rule groups loaded from
/// `rule_files`, using `h` as the real storage + query backend.
///
/// Port of `testGroup.test` (`unittest/unittest.go:316-474`) plus
/// `checkMetricsqlCase` (`unittest/recording.go:29-96`) for the
/// `metricsql_expr_test` assertion.
///
/// Groups from `rule_files` are evaluated in `group_eval_order` order: a
/// group's sort key is its index in `group_eval_order` (or `0` if absent,
/// matching Go's map zero-value default for `groupOrderMap[name]` when
/// `name` isn't a key — port of upstream's `sort.Slice` call,
/// `unittest.go:358-361`). Groups tied on the same key keep their original
/// `rule_files` document order (`slice::sort_by_key` is stable) — a
/// deliberate divergence from upstream's `sort.Slice`, which isn't
/// documented as stable; this only matters when `group_eval_order` omits
/// more than one group name, an edge case upstream's own behavior is
/// unspecified for. Duplicate names within `group_eval_order` aren't
/// rejected here (upstream errors earlier, in `ruleUnitTest`, before this
/// function's equivalent is ever called) — a duplicate's key resolves to its
/// *first* occurrence's index.
///
/// Divergence from upstream, deliberate given this task's fixed interface
/// (no `disableAlertgroupLabel` parameter exists on this signature):
/// - `alertgroup`/`disableAlertgroupLabel` is always `false` — no CLI flag
///   wiring exists yet for it in this tool;
/// - sample value comparison uses a small epsilon instead of upstream's
///   exact float equality (see [`crate::recording`]).
///
/// `metricsql_expr_test` cases are asserted in a post-loop pass, matching
/// upstream's `checkMetricsqlCase` call after the eval loop finishes
/// (`unittest.go:472`): each `expr` is an instant query at its *exact*
/// `eval_time` offset (`TEST_START_MS + eval_time`), independent of the
/// eval loop's tick alignment (see [`check_metricsql_expr_test_case`]).
///
/// Two distinct interval quantities, matching upstream (`unittest.go:322-325,376`):
/// - the **input cadence** (`tg.interval.unwrap_or(eval_interval)`) is used
///   *only* for the `input_series` sample timestamps (upstream's
///   `writeInputSeries`, `unittest.go:325`);
/// - the **eval step** (`eval_interval`, the file-level `evaluation_interval`)
///   is the eval loop's tick spacing *and* the width of the half-open
///   window each `alert_rule_test.eval_time` is floored into (upstream's
///   `for ts := ...; ts = ts.Add(evalInterval)` loop and its
///   `alertEvalTimes` window match, `unittest.go:376,394-398`).
pub fn run_test_group(
    h: &Harness,
    rule_files: &[String],
    eval_interval: Duration,
    global_external_labels: &BTreeMap<String, String>,
    group_eval_order: &[String],
    tg: &TestGroup,
) -> Result<GroupResult, ToolError> {
    // Input sample cadence: `tg.interval` overrides the file-level default,
    // but this affects ONLY the ingested series' timestamps — never the eval
    // loop's tick spacing (see the doc comment above).
    let input_cadence = tg.interval.unwrap_or(eval_interval);

    // Eval loop tick spacing / window width: always the file-level
    // `evaluation_interval`, regardless of `tg.interval`.
    let eval_step_ms = duration_ms(eval_interval);
    if eval_step_ms <= 0 {
        return Err(ToolError::new("evaluation_interval must be positive"));
    }

    ingest_input_series(h, tg, input_cadence)?;

    let ds = Datasource::new(
        h.base_url(),
        AuthConfig::default(),
        TlsConfig::default(),
        BTreeMap::new(),
        Vec::new(),
        eval_interval,
        DEFAULT_QUERY_TIMEOUT,
    )
    .map_err(|e| ToolError::new(format!("failed to build datasource: {e}")))?;

    let mut merged_external_labels = global_external_labels.clone();
    merged_external_labels.extend(tg.external_labels.clone()); // tg wins, per task brief.

    let mut groups = build_groups(rule_files, eval_interval, &merged_external_labels)?;
    groups.sort_by_key(|g| eval_order_key(group_eval_order, &g.name));

    let rw = RwClient::start(RwConfig {
        url: h.base_url().to_string(),
        flush_interval: Duration::from_secs(3600), // Never fires on its own; every flush below is explicit.
        max_batch_size: 1000,
        max_queue_size: 100_000,
        concurrency: 1,
        send_timeout: DEFAULT_SEND_TIMEOUT,
        auth: AuthConfig::default(),
        tls: TlsConfig::default(),
        headers: vec![],
    })
    .map_err(|e| ToolError::new(format!("failed to start remote-write client: {e}")))?;

    let ctx = EvalContext {
        external_url: EXTERNAL_URL.to_string(),
        path_prefix: String::new(),
        query_fn: Arc::new(|_| Ok(vec![])),
    };
    let funcs = default_funcs(&ctx);

    // Distinct `alert_rule_test` eval times, sorted ascending, walked by a
    // single monotonic index so each is asserted exactly once — at the tick
    // whose half-open window `[ts_offset, ts_offset + eval_step)` contains
    // it. Port of upstream's `alertEvalTimes`/`evalIndex` machinery
    // (`unittest.go:339-356,394-398`).
    let mut alert_eval_times: Vec<i64> = tg
        .alert_rule_test
        .iter()
        .map(|at| duration_ms(at.eval_time))
        .collect();
    alert_eval_times.sort_unstable();
    alert_eval_times.dedup();
    let mut eval_index = 0usize;

    let max_eval_ms = max_eval_time_ms(tg);
    let mut diffs = Vec::new();
    let mut ts = TEST_START_MS;
    while ts <= TEST_START_MS + max_eval_ms {
        for g in &mut groups {
            let eval_errors = g.eval_once(&ds, ts, &funcs, &ctx, Some(&rw), None, EXTERNAL_URL);
            // Upstream aborts the whole test group on ANY rule-eval error,
            // recording a "failed to exec group" check error and reporting
            // the group FAILED (`unittest.go:381-388`) — checked *before* the
            // post-eval flush, matching upstream's `return` ahead of
            // `DebugFlush`. esmalert's live daemon logs+continues on such an
            // error; the unittest runner must instead treat it as a hard
            // failure so an erroring expression can never report SUCCESS.
            if let Some(err) = eval_errors.into_iter().next() {
                diffs.push(format!(
                    "\nfailed to exec group: {:?}, time: {ts:?}, err: {err}",
                    g.name
                ));
                rw.shutdown();
                return Ok(GroupResult { diffs });
            }
            // Recording/alert series just written must be visible before the
            // next group (or the next tick) reads them back.
            let _ = rw.flush_now();
            h.flush()?;
        }

        // Assert every distinct eval time that falls in this tick's window.
        // Upstream's break condition, transliterated: stop once the next
        // eval time is behind this tick (already passed) or in a future
        // window; otherwise floor it to this tick and assert it.
        let ts_offset = ts - TEST_START_MS;
        while eval_index < alert_eval_times.len() {
            let et = alert_eval_times[eval_index];
            if ts_offset > et || et >= ts_offset + eval_step_ms {
                break;
            }
            for at in &tg.alert_rule_test {
                if duration_ms(at.eval_time) == et {
                    check_alert_test_case(&groups, &tg.name, at, &mut diffs);
                }
            }
            eval_index += 1;
        }

        ts += eval_step_ms;
    }

    // Post-loop pass for `metricsql_expr_test` cases, matching upstream's
    // `checkMetricsqlCase` call after the eval loop (`unittest.go:472`):
    // all input is ingested and every recording result has been written
    // back and flushed by now, so each `expr` can be queried at its exact
    // `eval_time` offset regardless of tick alignment.
    for mt in &tg.metricsql_expr_test {
        let ts_ms = TEST_START_MS + duration_ms(mt.eval_time);
        check_metricsql_expr_test_case(&ds, &tg.name, mt, ts_ms, &mut diffs);
    }

    rw.shutdown();
    Ok(GroupResult { diffs })
}

/// Expands and ingests every `tg.input_series` entry via [`expand_series`],
/// then flushes so the samples are visible to the eval loop's first tick.
/// A no-op (no ingest/flush call at all) when `tg` has no input series.
fn ingest_input_series(
    h: &Harness,
    tg: &TestGroup,
    input_cadence: Duration,
) -> Result<(), ToolError> {
    let mut samples = Vec::new();
    for s in &tg.input_series {
        samples.extend(expand_series(
            &s.series,
            &s.values,
            input_cadence,
            TEST_START_MS,
        )?);
    }
    if samples.is_empty() {
        return Ok(());
    }
    h.ingest(&samples)?;
    h.flush()
}

/// Loads and validates `rule_files`, then converts every parsed config group
/// into a runtime [`RtGroup`] via `esmalert::build_group_for_eval`, in
/// file/document order.
///
/// Errors with `found no rule group in <rule_files>` when the files yield
/// no groups, matching upstream (`unittest.go:207-208`) — even a pure
/// `metricsql_expr_test` group must reference `rule_files` containing at
/// least one group.
fn build_groups(
    rule_files: &[String],
    default_eval_interval: Duration,
    extra_labels: &BTreeMap<String, String>,
) -> Result<Vec<RtGroup>, ToolError> {
    let cfg = load_config(rule_files)
        .map_err(|e| ToolError::new(format!("failed to parse `rule_files`: {e}")))?;
    validate_config(&cfg).map_err(|e| ToolError::new(format!("invalid rule config: {e}")))?;
    if cfg.groups.is_empty() {
        return Err(ToolError::new(format!(
            "found no rule group in {rule_files:?}"
        )));
    }

    Ok(cfg
        .groups
        .iter()
        .map(|g| {
            esmalert::build_group_for_eval(
                g,
                extra_labels,
                false, // disable_alertgroup_label: no CLI wiring for this flag yet.
                default_eval_interval,
                Duration::ZERO,
                None,
            )
        })
        .collect())
}

/// The sort key `run_test_group` sorts runtime groups by: `name`'s index in
/// `group_eval_order`, or `0` if `name` isn't listed (Go map zero-value
/// default — see the divergence note on [`run_test_group`]'s doc comment).
fn eval_order_key(group_eval_order: &[String], name: &str) -> usize {
    group_eval_order.iter().position(|n| n == name).unwrap_or(0)
}

/// The max `eval_time` across both `alert_rule_test` and `metricsql_expr_test`
/// cases, in millis. Port of `testGroup.maxEvalTime` (`unittest.go:494-508`).
fn max_eval_time_ms(tg: &TestGroup) -> i64 {
    let alert_max = tg
        .alert_rule_test
        .iter()
        .map(|at| duration_ms(at.eval_time))
        .max()
        .unwrap_or(0);
    let expr_max = tg
        .metricsql_expr_test
        .iter()
        .map(|et| duration_ms(et.eval_time))
        .max()
        .unwrap_or(0);
    alert_max.max(expr_max)
}

fn duration_ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// Checks one [`AlertTestCase`] against the current state of `groups`:
/// finds the case's named group and named `AlertingRule`, collects its
/// `Firing`-state alerts, and set-matches (order-insensitive) their
/// labels+annotations against `at.exp_alerts`. Appends a human-readable diff
/// to `diffs` on any mismatch (missing, extra, or differing alert).
///
/// Port of the per-`evalIndex` comparison in `unittest.go:399-466`, and
/// `unittest/alerting.go`'s `labelsAndAnnotations` sort-then-`DeepEqual`
/// comparison — expressed here as a sorted-`Vec` equality, which is
/// equivalent set-match semantics.
fn check_alert_test_case(
    groups: &[RtGroup],
    test_group_name: &str,
    at: &AlertTestCase,
    diffs: &mut Vec<String>,
) {
    let mut got: Vec<(BTreeMap<String, String>, BTreeMap<String, String>)> = Vec::new();
    if let Some(g) = groups.iter().find(|g| g.name == at.groupname) {
        for r in &g.rules {
            let RuleKind::Alerting(ar) = r else {
                continue;
            };
            if ar.name != at.alertname {
                continue;
            }
            for a in ar.alerts.values() {
                if a.state != AlertState::Firing {
                    continue;
                }
                got.push((a.labels.clone(), a.annotations.clone()));
            }
        }
    }
    got.sort();

    let mut exp: Vec<(BTreeMap<String, String>, BTreeMap<String, String>)> = at
        .exp_alerts
        .iter()
        .map(|e| {
            let mut labels = e.exp_labels.clone();
            // Matches upstream: `alertgroup`/`alertname` are added as
            // additional expected labels (unittest.go:440-445) — the caller
            // doesn't repeat them in every `exp_labels` block.
            labels.insert("alertgroup".to_string(), at.groupname.clone());
            labels.insert("alertname".to_string(), at.alertname.clone());
            (labels, e.exp_annotations.clone())
        })
        .collect();
    exp.sort();

    if exp != got {
        diffs.push(format!(
            "testGroupName: {test_group_name}, groupname: {}, alertname: {}, time: {:?}\n    exp: {exp:#?}\n    got: {got:#?}",
            at.groupname, at.alertname, at.eval_time
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parse_test_file;
    use std::collections::BTreeMap as Map;

    #[test]
    fn eval_order_key_matches_upstream_go_map_zero_value_default() {
        let order = vec!["b".to_string(), "c".to_string()];
        assert_eq!(eval_order_key(&order, "b"), 0);
        assert_eq!(eval_order_key(&order, "c"), 1);
        // Unlisted names default to key 0 -- the Go zero value for
        // `groupOrderMap[name]` when `name` isn't a map key -- so an
        // unlisted group sorts alongside whichever listed group is first in
        // `group_eval_order`, not strictly "after" every listed group. This
        // is upstream's literal (if surprising) behavior; see the
        // divergence note on `run_test_group`'s doc comment.
        assert_eq!(eval_order_key(&order, "a"), 0);
        assert_eq!(eval_order_key(&[], "anything"), 0);
    }

    #[test]
    fn sort_by_eval_order_key_reorders_and_keeps_ties_stable() {
        let order = vec!["b".to_string(), "c".to_string()];
        let mut names = vec!["c", "a", "b", "z"];
        names.sort_by_key(|n| eval_order_key(&order, n));
        // "a" and "z" both default to key 0 (tied with "b", also key 0) and
        // must keep their original relative order among themselves and
        // relative to "b": [a, b, z] as they appeared, then "c" (key 1) last.
        assert_eq!(names, vec!["a", "b", "z", "c"]);
    }

    /// Writes a temp rule file with one alerting rule (`InstanceDown`,
    /// `expr: up == 0`, `for: 2m`, `labels: {severity: page}`,
    /// `annotations: {summary: "down"}`) and returns its path.
    fn write_rule_file(dir: &std::path::Path) -> String {
        let path = dir.join("alerts.yml");
        std::fs::write(
            &path,
            "groups:\n  \
             - name: g\n    \
               rules:\n      \
               - alert: InstanceDown\n        \
                 expr: up == 0\n        \
                 for: 2m\n        \
                 labels:\n          \
                   severity: page\n        \
                 annotations:\n          \
                   summary: \"down\"\n",
        )
        .expect("write temp rule file");
        path.to_string_lossy().into_owned()
    }

    fn test_group_yaml(exp_severity: &str) -> String {
        format!(
            "name: t1\n\
             interval: 1m\n\
             input_series:\n  \
               - series: 'up{{job=\"x\"}}'\n    \
                 values: '0 0 0 0 0'\n\
             alert_rule_test:\n  \
               - eval_time: 4m\n    \
                 groupname: g\n    \
                 alertname: InstanceDown\n    \
                 exp_alerts:\n      \
                   - exp_labels: {{ severity: {exp_severity}, job: x }}\n        \
                     exp_annotations: {{ summary: \"down\" }}\n"
        )
    }

    fn parse_one_test_group(yaml: &str) -> TestGroup {
        // `parse_test_file` parses a whole `UnitTestFile`; wrap the single
        // `TestGroup` fixture in a minimal `tests:` document so we can reuse
        // it (per the task brief: build the `TestGroup` via
        // `schema::parse_test_file`).
        let wrapped = format!("tests:\n  - {}", yaml.replace('\n', "\n    "));
        let mut f = parse_test_file(&wrapped).expect("parse wrapped test group");
        f.tests.remove(0)
    }

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "esmalert-tool-runner-test-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn alert_fires_after_for_and_matches_expectation() {
        let dir = temp_dir("pass");
        let rule_file = write_rule_file(&dir);
        let tg = parse_one_test_group(&test_group_yaml("page"));

        let h = Harness::start().expect("start harness");
        let result = run_test_group(
            &h,
            &[rule_file],
            Duration::from_secs(60),
            &Map::new(),
            &[],
            &tg,
        )
        .expect("run_test_group should not error");

        assert!(
            result.diffs.is_empty(),
            "expected no diffs, got: {:#?}",
            result.diffs
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn wrong_expected_label_produces_a_diff() {
        let dir = temp_dir("fail");
        let rule_file = write_rule_file(&dir);
        // Wrong expected severity: the rule always sets `severity: page`.
        let tg = parse_one_test_group(&test_group_yaml("critical"));

        let h = Harness::start().expect("start harness");
        let result = run_test_group(
            &h,
            &[rule_file],
            Duration::from_secs(60),
            &Map::new(),
            &[],
            &tg,
        )
        .expect("run_test_group should not error");

        assert!(
            !result.diffs.is_empty(),
            "expected a diff for the mismatched expected label"
        );
        assert!(result.diffs[0].contains("InstanceDown"));

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Writes a temp rule file with an immediately-firing alert
    /// (`InstanceDown`, `expr: up == 0`, `for: 0`) so a firing alert exists
    /// at every tick where `up == 0` — used to exercise the non-tick-aligned
    /// `eval_time` window match without a `for:` warm-up confusing the state.
    fn write_immediate_rule_file(dir: &std::path::Path) -> String {
        let path = dir.join("alerts.yml");
        std::fs::write(
            &path,
            "groups:\n  \
             - name: g\n    \
               rules:\n      \
               - alert: InstanceDown\n        \
                 expr: up == 0\n        \
                 for: 0s\n        \
                 labels:\n          \
                   severity: page\n",
        )
        .expect("write temp rule file");
        path.to_string_lossy().into_owned()
    }

    /// A test group whose `alert_rule_test.eval_time` is `90s` — deliberately
    /// NOT a multiple of the 1m eval step, so it only ever gets asserted via
    /// the half-open-window floor-to-tick match (`[60s, 120s)` → tick 60s).
    fn non_tick_aligned_test_group_yaml(exp_severity: &str) -> String {
        format!(
            "name: t1\n\
             interval: 1m\n\
             input_series:\n  \
               - series: 'up{{job=\"x\"}}'\n    \
                 values: '0 0 0'\n\
             alert_rule_test:\n  \
               - eval_time: 90s\n    \
                 groupname: g\n    \
                 alertname: InstanceDown\n    \
                 exp_alerts:\n      \
                   - exp_labels: {{ severity: {exp_severity}, job: x }}\n"
        )
    }

    #[test]
    fn non_tick_aligned_eval_time_is_evaluated_not_skipped() {
        // Regression for the exact-equality bug: `eval_time: 90s` with a 1m
        // eval step matches NO tick exactly (ticks land at 0/60/120s). Under
        // the correct half-open-window match it floors to the 60s tick and
        // IS asserted; under the old exact-equality check it was silently
        // skipped, so `diffs` came back empty (a false pass).
        //
        // Correct-expectation path: the firing alert at tick 60s carries
        // `severity: page`, so a matching expectation yields no diff — the
        // assertion ran and passed.
        let dir = temp_dir("window-pass");
        let rule_file = write_immediate_rule_file(&dir);
        let tg = parse_one_test_group(&non_tick_aligned_test_group_yaml("page"));

        let h = Harness::start().expect("start harness");
        let result = run_test_group(
            &h,
            &[rule_file],
            Duration::from_secs(60),
            &Map::new(),
            &[],
            &tg,
        )
        .expect("run_test_group should not error");
        assert!(
            result.diffs.is_empty(),
            "the 90s case should be evaluated at the 60s tick and pass, got: {:#?}",
            result.diffs
        );
        std::fs::remove_dir_all(&dir).ok();

        // Wrong-expectation path: the SAME 90s case with a mismatched
        // expected label must now produce a diff. Under the old exact-match
        // code this would be skipped and wrongly report empty diffs.
        let dir = temp_dir("window-fail");
        let rule_file = write_immediate_rule_file(&dir);
        let tg = parse_one_test_group(&non_tick_aligned_test_group_yaml("critical"));

        let h = Harness::start().expect("start harness");
        let result = run_test_group(
            &h,
            &[rule_file],
            Duration::from_secs(60),
            &Map::new(),
            &[],
            &tg,
        )
        .expect("run_test_group should not error");
        assert!(
            !result.diffs.is_empty(),
            "the non-tick-aligned 90s assertion must actually run and flag the mismatch"
        );
        assert!(result.diffs[0].contains("InstanceDown"));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Writes a temp rule file with one minimal recording rule
    /// (`record: r`, `expr: up`) so `build_groups` finds ≥1 group — a
    /// `metricsql_expr_test` file must still reference `rule_files` with at
    /// least one group, matching upstream (`unittest.go:207-208`). The rule
    /// itself is irrelevant to the `up` expr assertion below.
    fn write_recording_rule_file(dir: &std::path::Path) -> String {
        let path = dir.join("rules.yml");
        std::fs::write(
            &path,
            "groups:\n  \
             - name: g\n    \
               rules:\n      \
               - record: r\n        \
                 expr: up\n",
        )
        .expect("write temp rule file");
        path.to_string_lossy().into_owned()
    }

    /// A `metricsql_expr_test` group: `up{job="x"}` = "0 1 1" at 1m, and one
    /// `metricsql_expr_test` asserting `up` at `eval_time: 2m` against an
    /// expected sample of `exp_value`.
    fn metricsql_expr_test_group_yaml(exp_value: &str) -> String {
        format!(
            "name: t1\n\
             interval: 1m\n\
             input_series:\n  \
               - series: 'up{{job=\"x\"}}'\n    \
                 values: '0 1 1'\n\
             metricsql_expr_test:\n  \
               - expr: up\n    \
                 eval_time: 2m\n    \
                 exp_samples:\n      \
                   - labels: 'up{{job=\"x\"}}'\n        \
                     value: {exp_value}\n"
        )
    }

    #[test]
    fn metricsql_expr_test_matches_samples() {
        // Positive case: `up{job="x"}` is `1` at the exact 2m eval_time
        // (input series "0 1 1" at 1m cadence -> samples at 0m=0, 1m=1,
        // 2m=1), and the case expects exactly that. A minimal recording-rule
        // file satisfies `build_groups`' ≥1-group requirement; its rule is
        // irrelevant to the `up` assertion.
        let dir = temp_dir("expr-pass");
        let rule_file = write_recording_rule_file(&dir);
        let tg = parse_one_test_group(&metricsql_expr_test_group_yaml("1"));
        let h = Harness::start().expect("start harness");
        let result = run_test_group(
            &h,
            &[rule_file],
            Duration::from_secs(60),
            &Map::new(),
            &[],
            &tg,
        )
        .expect("run_test_group should not error");
        assert!(
            result.diffs.is_empty(),
            "expected no diffs, got: {:#?}",
            result.diffs
        );
        std::fs::remove_dir_all(&dir).ok();

        // Negative case: same series/expr, but the expected value (5) does
        // not match the actual value (1) -> must produce a diff.
        let dir = temp_dir("expr-fail");
        let rule_file = write_recording_rule_file(&dir);
        let tg = parse_one_test_group(&metricsql_expr_test_group_yaml("5"));
        let h = Harness::start().expect("start harness");
        let result = run_test_group(
            &h,
            &[rule_file],
            Duration::from_secs(60),
            &Map::new(),
            &[],
            &tg,
        )
        .expect("run_test_group should not error");
        assert!(
            !result.diffs.is_empty(),
            "expected a diff for the mismatched expected value"
        );
        assert!(result.diffs[0].contains("up"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn metricsql_expr_test_queries_at_exact_eval_time_not_floored_tick() {
        // Regression guard for the post-loop exact-`eval_time` query (vs. the
        // earlier in-loop window-floored approach). Input cadence (30s, via
        // `interval`) is FINER than the eval step (60s), so samples land at
        // 0/30/60/90/120s while the eval loop only ticks at 0/60/120s.
        //
        // Values "10 20 30 40 50" at 30s cadence => 0s=10, 30s=20, 60s=30,
        // 90s=40, 120s=50. The expr test's `eval_time: 90s` is NOT a tick
        // multiple. The exact-time instant query at 90s resolves to the
        // sample at exactly 90s (value 40). The old window-floored approach
        // would have queried at the 60s tick instead (value 30) — so
        // asserting 40 fails under the old code and passes only under the
        // exact-time query.
        let dir = temp_dir("expr-exact");
        let rule_file = write_recording_rule_file(&dir);
        let yaml = "name: t1\n\
             interval: 30s\n\
             input_series:\n  \
               - series: 'up{job=\"x\"}'\n    \
                 values: '10 20 30 40 50'\n\
             metricsql_expr_test:\n  \
               - expr: up\n    \
                 eval_time: 90s\n    \
                 exp_samples:\n      \
                   - labels: 'up{job=\"x\"}'\n        \
                     value: 40\n";
        let tg = parse_one_test_group(yaml);

        let h = Harness::start().expect("start harness");
        let result = run_test_group(
            &h,
            &[rule_file],
            Duration::from_secs(60),
            &Map::new(),
            &[],
            &tg,
        )
        .expect("run_test_group should not error");
        assert!(
            result.diffs.is_empty(),
            "expected the exact-90s query to yield value 40, got diffs: {:#?}",
            result.diffs
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Writes a temp rule file with one recording rule whose expression
    /// PARSES as valid MetricsQL (so `build_groups`' config validation via
    /// `esm_metricsql::parse` passes) but ERRORS at query-execution time:
    /// `label_replace` is a registered MetricsQL function name the parser
    /// accepts, yet the query engine backing the harness (`esm-promql`)
    /// doesn't implement it, so evaluating it yields an
    /// `unknown func "label_replace"` query error. That's exactly the
    /// eval-time failure the runner must treat as a hard test-group failure.
    fn write_erroring_rule_file(dir: &std::path::Path) -> String {
        let path = dir.join("erroring.yml");
        std::fs::write(
            &path,
            "groups:\n  \
             - name: g\n    \
               rules:\n      \
               - record: r\n        \
                 expr: label_replace(up, \"x\", \"y\", \"job\", \"z\")\n",
        )
        .expect("write temp rule file");
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn rule_eval_error_makes_the_test_group_fail() {
        // Regression: a rule whose expression errors at query time must make
        // the unittest report FAILED. esmalert's live daemon logs+continues
        // on such an error; the unittest runner must instead treat ANY
        // rule-eval error as a hard test-group failure and abort the group
        // (upstream unittest.go:381-388). Before this fix `run_test_group`
        // discarded the eval error and could report SUCCESS (empty diffs) or
        // a misleading missing-alert diff — a false pass.
        //
        // The group's only assertion is a `metricsql_expr_test` that would
        // otherwise pass; the exec error must pre-empt it (upstream returns
        // before the `checkMetricsqlCase` pass) and fail the group instead.
        let dir = temp_dir("exec-error");
        let rule_file = write_erroring_rule_file(&dir);
        let yaml = "name: t1\n\
             interval: 1m\n\
             input_series:\n  \
               - series: 'up{job=\"x\"}'\n    \
                 values: '0 1 1'\n\
             metricsql_expr_test:\n  \
               - expr: up\n    \
                 eval_time: 2m\n    \
                 exp_samples:\n      \
                   - labels: 'up{job=\"x\"}'\n        \
                     value: 1\n";
        let tg = parse_one_test_group(yaml);

        let h = Harness::start().expect("start harness");
        let result = run_test_group(
            &h,
            &[rule_file],
            Duration::from_secs(60),
            &Map::new(),
            &[],
            &tg,
        )
        .expect("run_test_group should not itself return an error");
        assert!(
            !result.diffs.is_empty(),
            "a rule that errors at eval time must make the group FAILED"
        );
        assert!(
            result.diffs[0].contains("failed to exec group"),
            "the failure must be reported as an exec error, got: {:#?}",
            result.diffs
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn zero_rule_groups_is_an_error() {
        // Upstream errors `found no rule group in <RuleFiles>` when the rule
        // files yield no groups (`unittest.go:207-208`) — even for an
        // expr-only test. An empty `rule_files` slice yields zero groups and
        // must surface that error rather than silently running.
        let tg = parse_one_test_group(&metricsql_expr_test_group_yaml("1"));
        let h = Harness::start().expect("start harness");
        let err = run_test_group(&h, &[], Duration::from_secs(60), &Map::new(), &[], &tg)
            .expect_err("empty rule_files must be an error");
        assert!(
            err.msg.contains("found no rule group"),
            "unexpected error message: {}",
            err.msg
        );
    }
}
