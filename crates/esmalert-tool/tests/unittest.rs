//! Integration test driving `esmalert_tool::run_unittest` against real
//! fixture files under `tests/fixtures/`.
//!
//! ## Path resolution
//!
//! Each fixture test file's `rule_files:` entries (e.g.
//! `tests/fixtures/rules/alerts.yml`) are paths relative to the *process*
//! working directory, not to the test file's own directory -- `run_test_group`
//! resolves them via `esmalert::config::load_config`, which globs each entry
//! against the CWD. Cargo guarantees that `cargo test`/`cargo run` set the
//! test binary's working directory to the package root (the directory
//! holding this crate's `Cargo.toml`, i.e. `CARGO_MANIFEST_DIR`), regardless
//! of the directory `cargo` itself is invoked from. That makes
//! crate-root-relative
//! `rule_files:` paths robust for every supported invocation
//! (`cargo test`, `cargo test -p esmalert-tool`, `cargo test --workspace`,
//! from any CWD) without needing to rewrite fixture content or copy files
//! into a temp dir at runtime -- so this test locates the checked-in
//! `tests/fixtures/{pass,fail}` files via `CARGO_MANIFEST_DIR` (for glob
//! enumeration, which does need an absolute base) but passes them to
//! `run_unittest` unmodified, letting their relative `rule_files:` resolve
//! against the guaranteed CWD.
//!
//! Rule files referenced via `rule_files:` live in their own
//! `tests/fixtures/rules/` directory, separate from `pass/`/`fail/` -- a
//! rule-group YAML file (`groups: ...`) happens to also parse as a
//! (degenerate, no-op) `UnitTestFile` under this crate's lenient test-file
//! schema, since it has no `tests:` key, so keeping it out of the `*.yml`
//! glob below avoids vacuously "passing" a file that isn't actually a test.
//!
//! Fixture coverage (ported from `app/vmalert-tool/unittest/*_test.go`
//! scenarios):
//! - `pass/alerting.yml`: an alerting rule's `for:` crossing Pending ->
//!   Firing, plus a test-group `external_labels` entry.
//! - `pass/recording.yml`: a recording rule's result asserted via
//!   `metricsql_expr_test`, with a `_` gap in one input series.
//! - `pass/expr.yml`: a pure `metricsql_expr_test` (no `alert_rule_test`),
//!   with a `stale` point in its input series.
//! - `fail/wrong_alert.yml`: same rule + scenario as `pass/alerting.yml`,
//!   but with a wrong expected label -- must fail.

use std::path::{Path, PathBuf};

/// Root of this crate (`CARGO_MANIFEST_DIR`), resolved at compile time.
/// Used only to enumerate fixture files; the paths handed to
/// `run_unittest` themselves stay crate-root-relative (see module doc).
fn fixtures_dir(subdir: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(subdir)
}

/// Every `*.yml` file directly under `tests/fixtures/<subdir>`, expressed as
/// a crate-root-relative path string (e.g. `tests/fixtures/pass/alerting.yml`)
/// -- exactly the form `run_unittest` (via `run_test_group`'s
/// `esmalert::config::load_config`) expects to resolve against the CWD.
fn fixture_files(subdir: &str) -> Vec<String> {
    let dir = fixtures_dir(subdir);
    let mut files: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read_dir {dir:?}: {e}"))
        .filter_map(|entry| {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("yml") {
                return None;
            }
            Some(format!(
                "tests/fixtures/{subdir}/{}",
                entry.file_name().to_string_lossy()
            ))
        })
        .collect();
    files.sort();
    files
}

#[test]
fn passing_fixtures_pass_and_failing_fixtures_fail() {
    let pass_files = fixture_files("pass");
    assert!(
        !pass_files.is_empty(),
        "expected at least one tests/fixtures/pass/*.yml fixture"
    );
    for file in &pass_files {
        // Each fixture is driven through its own `run_unittest` call: a
        // hard file/parse error in one file would abort a batch call before
        // later files ever ran, so testing them individually keeps every
        // fixture's result independent and the failure message localized.
        let result = esmalert_tool::run_unittest(std::slice::from_ref(file));
        match result {
            Ok(passed) => assert!(passed, "expected {file} to pass all its assertions"),
            Err(e) => panic!("expected {file} to run without a hard error, got: {e}"),
        }
    }

    let fail_files = fixture_files("fail");
    assert!(
        !fail_files.is_empty(),
        "expected at least one tests/fixtures/fail/*.yml fixture"
    );
    for file in &fail_files {
        let result = esmalert_tool::run_unittest(std::slice::from_ref(file));
        match result {
            Ok(passed) => assert!(!passed, "expected {file} to fail at least one assertion"),
            Err(e) => panic!("expected {file} to run without a hard error, got: {e}"),
        }
    }
}
