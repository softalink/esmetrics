//! `esmalert-tool` library: offline unit-tester for esmalert rule files.
//! Port of VictoriaMetrics `vmalert-tool`.
//!
//! Lib-ified (mirrors `esmalert`'s `lib.rs`/`main.rs` split) so integration
//! tests can call [`run_unittest`] in-process, without spawning a process.
//! `main.rs` stays a thin argv-parse -> dispatch -> exit-code shell; the rest
//! of the crate — the test-file schema (`schema.rs`), the in-process
//! esmetrics harness (`harness.rs`), the input-series parser (`input.rs`),
//! the eval-loop runner (`runner.rs`), the `metricsql_expr_test` comparison
//! (`recording.rs`), and the failure reporting (`report.rs`) — lives here.

mod harness;
mod input;
mod recording;
mod report;
mod runner;
mod schema;

use std::collections::BTreeMap;
use std::fs;
use std::time::Duration;

use harness::Harness;
use runner::run_test_group;
use schema::parse_test_file;

/// Generic error type shared across `esmalert-tool` modules.
#[derive(Debug)]
pub struct ToolError {
    pub msg: String,
}

impl std::fmt::Display for ToolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for ToolError {}

impl ToolError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self { msg: msg.into() }
    }
}

/// Default `evaluation_interval` when a test file doesn't set one, matching
/// upstream (`unittest.go:190-192`).
const DEFAULT_EVALUATION_INTERVAL: Duration = Duration::from_secs(60);

/// Runs the `unittest` subcommand for every file in `files`: for each one,
/// [`schema::parse_test_file`]s it, then runs every [`schema::TestGroup`] in
/// it via [`run_test_group`] against a fresh [`Harness`] started *per group*,
/// so no samples leak between groups. This matches upstream, where
/// `(tg *testGroup).test` opens with `setUp()` and `defer tearDown()`
/// (`unittest.go:314-320`): each group gets freshly-`Init`'d storage and its
/// data dir removed afterwards. (The once-per-run temp-dir/`Init` at
/// `unittest.go:104-117` is process-level setup, *not* the per-group storage
/// lifecycle.) Prints
/// `SUCCESS`/`FAILED` per file (mirroring `unittest.go:148-152`) and the
/// per-group failure diffs via [`report::format_group_failures`].
///
/// Returns `Ok(true)` iff every test in every file passed, `Ok(false)` if at
/// least one `alert_rule_test`/`metricsql_expr_test` assertion diffed but
/// every file was otherwise readable/parseable/runnable. A file-level *hard*
/// error — unreadable file, malformed YAML, an invalid/missing `rule_files`
/// entry, or a `Harness` that fails to start — aborts the whole run and
/// returns `Err` immediately rather than being folded into `Ok(false)`: this
/// mirrors upstream's `logger.Fatalf` treatment of the equivalent
/// prerequisite failures (`ReadFromFS`, `yaml.UnmarshalStrict`, `vmalertconfig.Parse`
/// all `Fatalf`/hard-return before any assertion ever runs), and keeps this
/// function's `bool` result meaning exactly "did every assertion pass"
/// rather than conflating it with "did every file even run".
///
/// `-external.label`, upstream's flag for file-level external labels
/// (`unittest.go:58,128-138`), isn't in this task's CLI scope (see the task
/// report) — an empty map is passed as `run_test_group`'s
/// `global_external_labels` argument instead. A `TestGroup`'s own
/// `external_labels` field (deprecated upstream, `unittest.go:254-257`) is
/// unaffected and still applies via `run_test_group`.
pub fn run_unittest(files: &[String]) -> Result<bool, ToolError> {
    let mut all_passed = true;
    for file in files {
        println!("\nUnit Testing: {file}");

        let content = fs::read_to_string(file)
            .map_err(|e| ToolError::new(format!("failed to read test file {file:?}: {e}")))?;
        let uf = parse_test_file(&content)?;

        let eval_interval = match uf.evaluation_interval {
            Some(d) => d,
            None => {
                println!("evaluation_interval set to 1m by default");
                DEFAULT_EVALUATION_INTERVAL
            }
        };

        let external_labels: BTreeMap<String, String> = BTreeMap::new();

        let mut file_passed = true;
        for tg in &uf.tests {
            // Fresh storage per group, matching upstream's per-group
            // `setUp`/`tearDown` (see this fn's doc): every group starts its
            // `input_series` at unix-epoch 0, so a shared engine would collide
            // timestamps across groups and leak samples. The prior iteration's
            // `Harness` was dropped at its scope end, stopping that server and
            // removing its temp dir before this one starts.
            let h = Harness::start()?;
            let result = run_test_group(
                &h,
                &uf.rule_files,
                eval_interval,
                &external_labels,
                &uf.group_eval_order,
                tg,
            )?;
            if !result.diffs.is_empty() {
                file_passed = false;
                println!(
                    "{}",
                    report::format_group_failures(file, &tg.name, &result.diffs)
                );
            }
        }

        if file_passed {
            println!("SUCCESS");
        } else {
            println!("FAILED");
            all_passed = false;
        }
    }
    Ok(all_passed)
}

#[cfg(test)]
mod tests {
    use super::run_unittest;
    use std::path::{Path, PathBuf};

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "esmalert-tool-lib-test-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    /// Writes a temp rule file with one immediately-firing alert
    /// (`InstanceDown`, `expr: up == 0`, `for: 0s`) and returns its path.
    fn write_rule_file(dir: &Path) -> String {
        let path = dir.join("alerts.yml");
        std::fs::write(
            &path,
            r#"groups:
  - name: g
    rules:
      - alert: InstanceDown
        expr: up == 0
        for: 0s
        labels:
          severity: page
"#,
        )
        .expect("write temp rule file");
        path.to_string_lossy().into_owned()
    }

    /// Writes a temp unittest file referencing `rule_file`, expecting
    /// `InstanceDown` to fire with `severity: exp_severity` at `eval_time:
    /// 0s`. The rule always sets `severity: page`, so `exp_severity: "page"`
    /// passes and any other value fails.
    fn write_unittest_file(dir: &Path, rule_file: &str, exp_severity: &str) -> String {
        let path = dir.join("unittest.yml");
        let yaml = format!(
            r#"rule_files:
  - {rule_file}
evaluation_interval: 1m
tests:
  - name: t1
    interval: 1m
    input_series:
      - series: 'up{{job="x"}}'
        values: '0 0 0'
    alert_rule_test:
      - eval_time: 0s
        groupname: g
        alertname: InstanceDown
        exp_alerts:
          - exp_labels: {{ severity: {exp_severity}, job: x }}
"#
        );
        std::fs::write(&path, yaml).expect("write temp unittest file");
        path.to_string_lossy().into_owned()
    }

    /// Writes a minimal rule file with one recording rule so
    /// `build_groups` finds >=1 group (upstream requires it even for a pure
    /// `metricsql_expr_test` file). The rule is irrelevant to the `m` query
    /// the isolation test below asserts on.
    fn write_min_recording_rule_file(dir: &Path) -> String {
        let path = dir.join("rules.yml");
        std::fs::write(
            &path,
            "groups:\n  \
             - name: g\n    \
               rules:\n      \
               - record: r\n        \
                 expr: m\n",
        )
        .expect("write temp rule file");
        path.to_string_lossy().into_owned()
    }

    /// Writes a two-group unittest file where BOTH groups reuse the same
    /// metric name `m` with different label sets and values (mirroring
    /// upstream `testdata/test1.yaml`, which reuses `test`/`foo` across
    /// groups). Each group's `metricsql_expr_test` queries `m` at `eval_time:
    /// 0s` and expects to see ONLY its own series. If storage is shared across
    /// groups (the bug), the second group also observes the first group's
    /// leaked `m{g="1"}` sample -> an extra series -> a diff -> FAILED.
    fn write_two_group_isolation_file(dir: &Path, rule_file: &str) -> String {
        let path = dir.join("isolation.yml");
        let yaml = format!(
            r#"rule_files:
  - {rule_file}
evaluation_interval: 1m
tests:
  - name: g1
    interval: 1m
    input_series:
      - series: 'm{{g="1"}}'
        values: '111 111 111'
    metricsql_expr_test:
      - expr: m
        eval_time: 0s
        exp_samples:
          - labels: 'm{{g="1"}}'
            value: 111
  - name: g2
    interval: 1m
    input_series:
      - series: 'm{{g="2"}}'
        values: '222 222 222'
    metricsql_expr_test:
      - expr: m
        eval_time: 0s
        exp_samples:
          - labels: 'm{{g="2"}}'
            value: 222
"#
        );
        std::fs::write(&path, yaml).expect("write temp unittest file");
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn run_unittest_isolates_storage_between_test_groups() {
        // Each group in the file reuses metric `m` but writes a distinct
        // series/value and expects to see only its own. This passes iff each
        // group runs against clean storage; if one storage engine is shared
        // across the whole file, group 2 sees group 1's leaked `m{g="1"}`
        // sample and FAILS.
        let dir = temp_dir("isolation");
        let rule_file = write_min_recording_rule_file(&dir);
        let test_file = write_two_group_isolation_file(&dir, &rule_file);
        let passed = run_unittest(&[test_file]).expect("run_unittest should not error");
        assert!(
            passed,
            "expected per-group storage isolation: group 2 must not observe \
             group 1's samples"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn run_unittest_returns_true_for_passing_file_false_for_failing() {
        // Passing case: the expectation matches what the rule always sets.
        let dir = temp_dir("pass");
        let rule_file = write_rule_file(&dir);
        let test_file = write_unittest_file(&dir, &rule_file, "page");
        let passed = run_unittest(&[test_file]).expect("run_unittest should not error");
        assert!(passed, "expected the matching-expectation file to pass");
        std::fs::remove_dir_all(&dir).ok();

        // Failing case: same rule, but the expected severity is wrong.
        let dir = temp_dir("fail");
        let rule_file = write_rule_file(&dir);
        let test_file = write_unittest_file(&dir, &rule_file, "critical");
        let passed = run_unittest(&[test_file]).expect("run_unittest should not error");
        assert!(!passed, "expected the mismatched-expectation file to fail");
        std::fs::remove_dir_all(&dir).ok();

        // Nonexistent file: a hard I/O error, not a test-assertion failure --
        // must surface as `Err`, not `Ok(false)`.
        let err = run_unittest(&["/no/such/path/does-not-exist.yml".to_string()])
            .expect_err("a nonexistent file must be a hard error");
        assert!(
            err.msg.contains("does-not-exist.yml"),
            "unexpected error message: {}",
            err.msg
        );
    }
}
