# esmalert-tool (vmalert-tool port) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port VictoriaMetrics `vmalert-tool` to a standalone Rust `esmalert-tool` binary whose `unittest` subcommand runs offline unit tests of alerting + recording rules (the `promtool test rules` analog), so existing vmalert-tool test files run unchanged.

**Architecture:** A new bin crate `esmalert-tool` that stands up a **real in-process esmetrics server** via the existing `esmetrics::run(&flags)` entry (temp storage dir, ephemeral port), ingests the test file's `input_series` via remote-write, builds rule groups with esmalert's config + rule engine (reused as a library), drives `Group::eval_once` at successive timestamps, and asserts firing alerts + MetricsQL instant-query results against the test file's expectations.

**Tech Stack:** Rust (edition 2021, rust-version 1.85), `esmetrics` (in-process server via `run()`), `esmalert` (config + rule engine, as a lib), `esm-protoparser` (remote-write encode), `esm-metricsql` (selector + duration parse), `serde_yaml_ng`, `reqwest` (blocking).

## Porting Convention (read before every task)

Faithful port. For each task the authoritative behavioral source is the cited upstream file at `/home/test/refsrc/VictoriaMetrics/app/vmalert-tool/` (pinned at v1.146.0). The plan gives you the exact Rust interfaces to produce, real failing tests, and the subtle semantics. When the plan says "port `<file>:<lines>`", read that Go code and translate it faithfully. Reuse in-repo crates; do not reimplement query evaluation.

Reference existing ports for idiom: `crates/esmalert` (config parser `config::{load_config, validate_config, parse_config_str}`, rule engine `rule::{RuleKind, group::Group, alerting::AlertingRule}`, `series::Series`), `crates/esmetrics/tests/server_test.rs` (how `esmetrics::run(&flags)` is used in-process: `let app = esmetrics::run(&flags)?; let addr = app.local_addr(); ... app.stop();`), `crates/esm-protoparser::encode_and_compress` (remote-write encode), `crates/esmalert/src/datasource` (the `Datasource` query client).

## Global Constraints

- Files ≤ 800 lines; extract modules when a file grows unwieldy.
- `cargo fmt` clean; `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets` clean; windows-gnu cross-check must compile.
- No tokio. Sync stack.
- Faithful to upstream v1.146.0 test-file semantics — existing vmalert-tool unittest files run unchanged.
- New workspace member `crates/esmalert-tool` added to root `Cargo.toml` `members`.
- Never panic on bad input (bad file/YAML/rule → reported error + non-zero exit).
- Commit style `<type>: <description>`, no attribution trailers.
- After push, watch the GitHub Actions run and fix failures (Windows tests run only in CI).

---

## Task 1: crate scaffold + test-file schema

**Files:**
- Create: `crates/esmalert-tool/Cargo.toml`, `crates/esmalert-tool/src/main.rs` (temporary `fn main(){}`), `crates/esmalert-tool/src/schema.rs`
- Modify: root `Cargo.toml` `[workspace] members`
- Test: inline in `schema.rs`

**Interfaces:**
- Produces (`schema.rs`):
  - `pub struct UnitTestFile { pub rule_files: Vec<String>, pub evaluation_interval: Option<Duration>, pub group_eval_order: Vec<String>, pub tests: Vec<TestGroup> }`
  - `pub struct TestGroup { pub interval: Option<Duration>, pub input_series: Vec<Series>, pub alert_rule_test: Vec<AlertTestCase>, pub metricsql_expr_test: Vec<MetricsqlTestCase>, pub external_labels: BTreeMap<String,String>, pub name: String }`
  - `pub struct Series { pub series: String, pub values: String }`
  - `pub struct AlertTestCase { pub eval_time: Duration, pub groupname: String, pub alertname: String, pub exp_alerts: Vec<ExpAlert> }`
  - `pub struct ExpAlert { pub exp_labels: BTreeMap<String,String>, pub exp_annotations: BTreeMap<String,String> }`
  - `pub struct MetricsqlTestCase { pub expr: String, pub eval_time: Duration, pub exp_samples: Vec<ExpSample> }`
  - `pub struct ExpSample { pub labels: String, pub value: f64 }`
  - `pub fn parse_test_file(yaml: &str) -> Result<UnitTestFile, ToolError>` and `pub struct ToolError { pub msg: String }` (Display + Error).

**Reference:** `unittest/unittest.go:477-508` (`unitTestFile`/`testGroup`/`alertTestCase`/`metricsqlTestCase` yaml tags). Durations are Go duration strings → parse with a serde helper calling `esm_metricsql::duration_value(s,0)` (ms) → `std::time::Duration::from_millis(ms as u64)` (reject negative like the esmalert config layer does). Unknown-field handling: upstream is lenient (no strict overflow check on the test file) — do NOT `deny_unknown_fields`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn parses_a_unittest_file() {
    let y = r#"
rule_files:
  - alerts.yml
evaluation_interval: 1m
tests:
  - interval: 1m
    name: t1
    input_series:
      - series: 'up{job="x"}'
        values: '0 0 0 1 1'
    alert_rule_test:
      - eval_time: 4m
        groupname: g
        alertname: InstanceDown
        exp_alerts:
          - exp_labels: { severity: page, job: x }
            exp_annotations: { summary: "down" }
    metricsql_expr_test:
      - expr: up
        eval_time: 4m
        exp_samples:
          - labels: 'up{job="x"}'
            value: 1
"#;
    let f = parse_test_file(y).unwrap();
    assert_eq!(f.rule_files, vec!["alerts.yml".to_string()]);
    assert_eq!(f.evaluation_interval, Some(Duration::from_secs(60)));
    assert_eq!(f.tests[0].input_series[0].series, r#"up{job="x"}"#);
    assert_eq!(f.tests[0].alert_rule_test[0].eval_time, Duration::from_secs(240));
    assert_eq!(f.tests[0].alert_rule_test[0].exp_alerts[0].exp_labels["severity"], "page");
    assert_eq!(f.tests[0].metricsql_expr_test[0].exp_samples[0].value, 1.0);
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p esmalert-tool schema` → FAIL.
- [ ] **Step 3: Implement** the crate + structs + parse. Deps: `serde`, `serde_yaml_ng`, `esm-metricsql` (workspace). Binary crate; `main.rs` temporary.
- [ ] **Step 4: Run** — PASS; `RUSTFLAGS="-D warnings" cargo clippy -p esmalert-tool --all-targets`.
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: esmalert-tool test-file schema"`

---

## Task 2: input_series value parser

**Files:**
- Create: `crates/esmalert-tool/src/input.rs`
- Test: inline in `input.rs`

**Interfaces:**
- Produces:
  - `pub enum SeqValue { Value(f64), Gap, Stale }` (a gap = no sample; stale = VM staleness marker)
  - `pub fn parse_input_value(input: &str) -> Result<Vec<SeqValue>, ToolError>` (port of upstream `parseInputValue`)
  - `pub struct InputSample { pub labels: Vec<(String,String)>, pub timestamp_ms: i64, pub value: f64 }`
  - `pub fn expand_series(series_selector: &str, values: &str, interval: Duration, start_ms: i64) -> Result<Vec<InputSample>, ToolError>` — parse the `metric{labels}` selector via `esm_metricsql` to get `__name__` + labels, parse the value sequence, and emit one `InputSample` per non-gap point at `start_ms + i*interval_ms`; `Stale` emits a sample whose value is VM's stale NaN (reuse the repo's StaleNaN constant — `grep -rn "StaleNaN\|stale_nan\|staleNaN\|0x7ff0000000000002" crates/` to find it).

**Reference:** `unittest/input.go:94-196` (`parseInputValue` — the `numReg` regex, `a+bxN`/`a-bxN` expansion, `_` gap, `stale` marker, `inf`/`nan`) and the selector parsing (`metricsql.ParseMetricExpr`-equivalent — use `esm_metricsql`'s expression parser and pull the metric-name + label filters). `stale` cannot combine with operators (upstream errors: "stale metric doesn't support operations").

- [ ] **Step 1: Write the failing test** — from `unittest/input_test.go`.

```rust
#[test]
fn parses_expanding_and_gaps_and_stale() {
    use SeqValue::*;
    let got = parse_input_value("1+1x3").unwrap();
    assert_eq!(got, vec![Value(1.0), Value(2.0), Value(3.0), Value(4.0)]);
    let got = parse_input_value("1 _ 3").unwrap();
    assert_eq!(got, vec![Value(1.0), Gap, Value(3.0)]);
    let got = parse_input_value("5-1x2").unwrap();
    assert_eq!(got, vec![Value(5.0), Value(4.0), Value(3.0)]);
    let got = parse_input_value("stale").unwrap();
    assert_eq!(got, vec![Stale]);
    assert!(parse_input_value("1+stalex2").is_err()); // stale + op -> error
}

#[test]
fn expands_series_to_samples() {
    let s = expand_series(r#"up{job="x"}"#, "0 1", Duration::from_secs(60), 1_000_000).unwrap();
    assert_eq!(s.len(), 2);
    assert!(s[0].labels.iter().any(|(k,v)| k=="__name__" && v=="up"));
    assert!(s[0].labels.iter().any(|(k,v)| k=="job" && v=="x"));
    assert_eq!(s[0].timestamp_ms, 1_000_000);
    assert_eq!(s[1].timestamp_ms, 1_000_000 + 60_000);
    assert_eq!(s[1].value, 1.0);
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** the parser + expansion. Add `regex` (workspace) for the value tokenizer.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmalert-tool input_series parser"`

---

## Task 3: in-process harness (server + ingest + query)

**Files:**
- Create: `crates/esmalert-tool/src/harness.rs`
- Test: inline in `harness.rs`

**Interfaces:**
- Consumes: `esmetrics::run`, `esmetrics::Flags` (grep `crates/esmetrics/src/flags.rs` for the `Flags` fields and how `server_test.rs::test_flags()` builds them — storage dir + `http_listen_addr: "127.0.0.1:0"`), `esm_protoparser::encode_and_compress`, `esmalert::series::Series`, `esm_protoparser::prompb`, `input::InputSample`.
- Produces:
  - `pub struct Harness { /* App handle, base_url: String, temp_dir: PathBuf */ }`
  - `impl Harness { pub fn start() -> Result<Harness, ToolError>; pub fn base_url(&self) -> &str; pub fn ingest(&self, samples: &[InputSample]) -> Result<(), ToolError>; pub fn flush(&self) -> Result<(), ToolError>; }`
  - `Drop for Harness` → `app.stop()` + remove the temp storage dir.
- `start()` builds `Flags` with a unique temp dir (use a name derived from a passed-in counter or the process id + a monotonically-incremented atomic — NOT `Math.random`/time) and `http_listen_addr="127.0.0.1:0"`, calls `esmetrics::run(&flags)?`, records `base_url = format!("http://{}", app.local_addr())`.
- `ingest()` converts `InputSample`s → `esmalert::series::Series` (one series per unique label set, samples sorted by ts) → borrowed `prompb::TimeSeries` → `encode_and_compress` → HTTP POST `{base_url}/api/v1/write` with `Content-Encoding: snappy`, `Content-Type: application/x-protobuf` (reuse the RwClient's request shape; a blocking `reqwest` POST is fine). `flush()` → POST `{base_url}/internal/force_flush` if that endpoint exists (grep `crates/esm-http`/`esm-storage` for a flush endpoint; upstream uses `vmstorage.DebugFlush()` — find the esmetrics analog, e.g. `/internal/force_flush` or `/internal/force_merge`; if none exists, note it and rely on the storage being queryable without an explicit flush).

**Reference:** `crates/esmetrics/tests/server_test.rs` (the `esmetrics::run` + `local_addr` + `stop` lifecycle and `test_flags()`), `unittest/input.go:35-57` (`httpWrite`/`writeInputSeries` — upstream POSTs the series to the datasource `/write`; here we use remote-write to `/api/v1/write`).

- [ ] **Step 1: Write the failing test** — round-trip ingest→query through a real in-process server.

```rust
#[test]
fn ingest_then_query_roundtrips_through_in_process_server() {
    let h = Harness::start().unwrap();
    let samples = vec![
        InputSample { labels: vec![("__name__".into(),"up".into()),("job".into(),"x".into())], timestamp_ms: 1_700_000_000_000, value: 1.0 },
    ];
    h.ingest(&samples).unwrap();
    h.flush().unwrap();
    // query via an esmalert Datasource pointed at the in-process server
    let ds = esmalert::datasource::Datasource::new(
        h.base_url(), Default::default(), Default::default(),
        Default::default(), vec![], std::time::Duration::from_secs(60),
        esmalert::datasource::DEFAULT_QUERY_TIMEOUT,
    ).unwrap();
    let res = ds.query("up", 1_700_000_000_000).unwrap();
    assert!(res.data.iter().any(|m| m.values.iter().any(|v| *v == 1.0)));
}
```

(Confirm the exact `Datasource::new` argument list against `crates/esmalert/src/datasource/client.rs` — it may differ; adjust the call. If `AuthConfig`/`TlsConfig` don't impl `Default`, construct empty ones explicitly.)

- [ ] **Step 2: Run to verify it fails** — FAIL. Add `esmetrics`, `esmalert`, `esm-protoparser` deps.
- [ ] **Step 3: Implement** the harness.
- [ ] **Step 4: Run** — PASS; clippy clean. (This test spins a real server — keep it deterministic; poll the query with a short bounded retry if the first query races storage visibility.)
- [ ] **Step 5: Commit** — `git commit -m "feat: esmalert-tool in-process esmetrics harness"`

---

## Task 4: runner core — build groups + eval loop + alert_rule_test

**Files:**
- Create: `crates/esmalert-tool/src/runner.rs`
- Modify: `crates/esmalert/src/lib.rs` and/or `crates/esmalert/src/rule/*`, `crates/esmalert/src/manager.rs` — targeted `pub`/`pub(crate)`→`pub` bumps so esmalert-tool can: build a runtime `rule::group::Group` from a config `Group` + a `Datasource` + funcs/ctx, call `Group::eval_once`, and read each `AlertingRule`'s firing `alerts`. (Grep esmalert for the existing `build_runtime_group`/`build_rule` in `manager.rs`; expose a reusable builder OR expose the pieces the tool needs. Prefer exposing a single `pub fn build_group_for_eval(cfg: &config::Group, deps: ...) -> rule::group::Group` in esmalert so the tool does not duplicate the conversion.)
- Test: inline in `runner.rs`

**Interfaces:**
- Consumes: `schema::{TestGroup, AlertTestCase, ExpAlert}`, `input::expand_series`, `harness::Harness`, esmalert config + the newly-exposed group builder + `Group::eval_once` + `AlertingRule` alerts, `esm_gotemplate` funcs/ctx.
- Produces:
  - `pub struct GroupResult { pub diffs: Vec<String> }`
  - `pub fn run_test_group(h: &Harness, rule_files: &[String], eval_interval: Duration, global_external_labels: &BTreeMap<String,String>, tg: &TestGroup) -> Result<GroupResult, ToolError>` — does: ingest `tg.input_series`; flush; build rule groups from `rule_files` (load_config + validate_config), ordered per `group_eval_order`; run the eval loop from `test_start` stepping `eval_interval` (tg.interval overrides) up to `max(eval_time)`; at each `ts`, `eval_once(...Some(rw)/None notifier...)` per group + flush; at each `alert_rule_test.eval_time`, read the named group→named `AlertingRule`'s firing alerts and set-match labels+annotations vs `exp_alerts`, appending a human-readable diff per mismatch.
  - A fixed `test_start` (upstream uses a fixed base like `time.Unix(0,0)` or a constant — read `unittest.go` for `testStartTime`; use the same base so eval_time offsets line up).

**Reference:** `unittest/unittest.go:360-475` (the eval loop, `ExecOnce`, `DebugFlush`, alert-assertion set-matching) and `unittest/alerting.go` (alert comparison). Firing alerts come from the `AlertingRule.alerts` map filtered to `state == Firing` (match upstream — it compares alerts in firing state at eval_time). rw = the ingest path back into storage (recording results + ALERTS); notifier = None.

- [ ] **Step 1: Write the failing test** — a full alerting scenario end to end.

```rust
#[test]
fn alert_fires_after_for_and_matches_expectation() {
    // Write a temp rule file with an alerting rule: alert InstanceDown, expr up==0, for 2m,
    // labels {severity: page}, annotations {summary: "down"}.
    // Build a TestGroup: input_series up{job="x"} = "0 0 0 0 0" over 1m interval;
    // alert_rule_test at eval_time 4m expects InstanceDown firing with those labels/annotations.
    // run_test_group -> diffs empty (pass).
    // Then a NEGATIVE case: expectation with wrong severity -> diffs non-empty.
}
```

Flesh this out fully: write the temp rule file via `std::fs`, construct the `TestGroup` from parsed YAML (reuse `schema::parse_test_file`), call `run_test_group`, assert `result.diffs.is_empty()` for the passing case and non-empty for the mismatched case. No blank stubs.

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** the esmalert lib-surface bumps + the runner group-build + eval loop + alert assertions.
- [ ] **Step 4: Run** — `cargo test -p esmalert-tool runner` PASS; `cargo test -p esmalert` still green (visibility changes don't break it); clippy workspace clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmalert-tool runner eval loop and alert_rule_test"`

---

## Task 5: runner — metricsql_expr_test

**Files:**
- Modify: `crates/esmalert-tool/src/runner.rs`
- Test: inline in `runner.rs`

**Interfaces:**
- Produces: extend `run_test_group` to also, at each `metricsql_expr_test.eval_time`, run `expr` as an instant query against the group's `Datasource` (`ds.query(expr, ts)`), and set-match returned samples (labels rendered as `metric{k="v"}` + value within `1e-9` epsilon) against `exp_samples`, appending diffs. Add `fn labels_to_selector_string(labels: &[(String,String)]) -> String` producing the canonical `name{k="v",...}` form for comparison with `ExpSample.labels` (parse `ExpSample.labels` via `esm_metricsql` and compare label sets, so formatting differences don't cause false diffs).

**Reference:** `unittest/unittest.go` (`metricsql_expr_test` handling) + `unittest/recording.go` (sample comparison). Compare by label-set equality + value epsilon, not string equality, to match upstream's tolerance.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn metricsql_expr_test_matches_samples() {
    // input_series up{job="x"} = "0 1 1" over 1m; metricsql_expr_test expr `up` at eval_time 2m
    // expects sample up{job="x"} value 1. run_test_group -> diffs empty.
    // Negative: expected value 5 -> diffs non-empty.
}
```

Flesh out fully (temp rule file may be empty/absent for a pure expr test — upstream allows expr tests without rules; ensure `run_test_group` handles zero rule groups). No blank stubs.

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** the expr-test assertion path.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmalert-tool metricsql_expr_test assertions"`

---

## Task 6: CLI + report + exit code

**Files:**
- Create: `crates/esmalert-tool/src/report.rs`
- Modify: `crates/esmalert-tool/src/main.rs`
- Test: inline in `main.rs` (a `run_unittest(files) -> (bool, String)` helper is the testable seam) + `report.rs`

**Interfaces:**
- Produces:
  - `pub fn run_unittest(files: &[String]) -> Result<bool, ToolError>` — for each file: `parse_test_file`, start a `Harness`, run each `TestGroup` via `run_test_group`, collect diffs; print per-file/per-group failures via `report`; returns `true` iff every test in every file passed. (One Harness per file is fine; or one per group — pick per isolation needs and note it. Upstream uses fresh storage per file.)
  - `main()` — parse args: `esmalert-tool unittest <files...>`; call `run_unittest`; exit `0` if all passed else `1`; unknown subcommand / no files → usage error to stderr + exit `2`. Mirror esmalert's arg-parsing idiom (grep `crates/esmalert/src/flags.rs`).
  - `report::format_group_failures(file: &str, group: &str, diffs: &[String]) -> String` — upstream-style human-readable block.

**Reference:** `app/vmalert-tool/main.go:1-80` (subcommand dispatch, exit codes) and `unittest.go`'s failure printing.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn run_unittest_returns_true_for_passing_file_false_for_failing() {
    // Write a temp dir with alerts.yml + a passing test file; run_unittest -> Ok(true).
    // Write a failing test file (wrong exp_alerts); run_unittest -> Ok(false).
    // A nonexistent file -> Err.
}
```

Flesh out fully with temp files. No blank stubs.

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** CLI + report + `run_unittest`. `cargo build -p esmalert-tool` produces the `esmalert-tool` binary.
- [ ] **Step 4: Run** — PASS; `cargo build -p esmalert-tool`; clippy workspace clean; `cargo fmt`.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmalert-tool unittest CLI and reporting"`

---

## Task 7: integration fixtures + docs

**Files:**
- Create: `crates/esmalert-tool/tests/fixtures/` (`.yml` test files + their rule files), `crates/esmalert-tool/tests/unittest.rs`
- Create: `crates/esmalert-tool/README.md`
- Modify: `README.md` (top-level — add esmalert-tool to the esmalert section), `docs/PORTING.md` (add an `app/vmalert-tool` row)
- Test: `crates/esmalert-tool/tests/unittest.rs`

**Interfaces:** none new — drives `esmalert_tool::run_unittest` (needs a `lib.rs` exposing it; if the crate is bin-only, add a small `lib.rs` mirroring the esmalert bin/lib split so the integration test can call `run_unittest`).

**Reference:** `app/vmalert-tool/unittest/*_test.go` fixtures — port representative passing + intentionally-failing `.yml` files covering: an alerting rule with `for:` crossing Pending→Firing, a recording rule feeding a second rule/expr, a `metricsql_expr_test`, `_` gaps + `stale`, and `external_labels`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn passing_fixtures_pass_and_failing_fixtures_fail() {
    // for each fixture under tests/fixtures/pass/*.yml -> run_unittest -> Ok(true)
    // for each under tests/fixtures/fail/*.yml -> run_unittest -> Ok(false)
}
```

Write the fixture `.yml` files (real rule files + test files) so the test is meaningful. Include at least: `pass/alerting.yml`, `pass/recording.yml`, `pass/expr.yml`, `fail/wrong_alert.yml`.

- [ ] **Step 2: Run to verify it fails** — `cargo test -p esmalert-tool --test unittest` → FAIL.
- [ ] **Step 3: Implement** fixtures + any wiring gaps they surface; write the README sections (honest: note this stands up a real in-process esmetrics server per run; deferred = nothing beyond `unittest`, which is upstream's only subcommand). Add the PORTING.md row.
- [ ] **Step 4: Run** — `cargo test -p esmalert-tool --test unittest` PASS; full-workspace `cargo test --workspace`, `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets`, `cargo fmt --check`, windows-gnu check.
- [ ] **Step 5: Commit** — `git commit -m "test: esmalert-tool integration fixtures; docs: esmalert-tool usage"`

---

## Final verification (after Task 7, before merge)

- [ ] `cargo test --workspace` green on Linux; push and confirm Windows CI green (watch the GitHub Actions run; fix failures — the in-process server + storage temp dirs must work on Windows).
- [ ] windows-gnu cross-compile check passes.
- [ ] Whole-branch code review (subagent-driven final review) — address Critical/Important findings; pay attention to temp-dir cleanup (no leaked storage dirs), server teardown (no leaked threads/ports across many test files), and that esmalert visibility bumps didn't over-expose internals.
- [ ] No benchmark impact expected (offline tool, separate binary) — no re-validation needed.
- [ ] Update memory: extend the `esmalert-port` memory (or a short `esmalert-tool` note) + MEMORY.md index.
