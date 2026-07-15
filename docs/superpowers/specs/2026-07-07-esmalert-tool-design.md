# esmalert-tool (vmalert-tool port) — Design Spec

**Status:** Approved design, pending implementation plan
**Date:** 2026-07-07
**Upstream:** VictoriaMetrics `app/vmalert-tool` @ v1.146.0 (see `UPSTREAM`)
**Depends on:** the esmalert port (`docs/superpowers/specs/2026-07-07-esmalert-design.md`) — reuses its config + rule engine as a library.

## Goal

Port VictoriaMetrics `vmalert-tool` to Rust as a standalone `esmalert-tool` binary whose `unittest` subcommand runs offline unit tests of alerting + recording rules — the analog of `promtool test rules` — so existing vmalert-tool test files run unchanged.

## Scope

**In scope (full test-file parity)**
- `unittest` subcommand: parse one or more YAML test files, run every test group, report per-test failures, exit 0 (all pass) / 1 (any fail).
- Test-file schema field-for-field with upstream: `rule_files`, `evaluation_interval`, `group_eval_order`, `tests[]` (`interval`, `input_series[]`, `alert_rule_test[]`, `metricsql_expr_test[]`, `external_labels`, `name`).
- `input_series` Prometheus notation: literals, `inf`/`nan`, expanding `a+bxN` / `a-bxN`, `_` gaps, `stale` markers.
- Both assertion types: `alert_rule_test` (firing alerts at eval times) and `metricsql_expr_test` (bare MetricsQL instant-query results at eval times).
- Real in-process esmetrics query engine for evaluation.

**Out of scope**
- Any subcommand other than `unittest` (upstream vmalert-tool only has `unittest`).
- Bundling into the esmalert daemon (kept a separate binary to keep storage/select deps out of the daemon).

## Architecture

`esmalert-tool` is a **separate bin crate**. Its `unittest` runner stands up a **real in-process esmetrics stack** — mirroring upstream, which assembles `vmstorage` + `vminsert` + `vmselect` behind an `httptest` server — so rule MetricsQL is evaluated with true esmetrics query semantics (rollups, staleness, lookbehind), not a mock.

### Crate layout

**`crates/esmalert-tool`** (new bin):
- `main.rs` — CLI: `esmalert-tool unittest <files...>`; exit code; mirrors esmalert's arg/flag idiom.
- `schema.rs` — serde structs for the YAML test-file format.
- `input.rs` — `input_series` value + selector parser (faithful port of upstream `parseInputValue`).
- `harness.rs` — stands up the in-process esmetrics server (temp `esm-storage` dir + `esm-insert`/`wiring::StorageSink` + `esm-select`/`wiring::StorageProvider` over `esm-http` on `127.0.0.1:0`), exposing ingest + a base URL; tears down + cleans the temp dir on drop.
- `runner.rs` — per-group ingest → build rules → eval loop → assert → collect diffs.
- `report.rs` — human-readable failure formatting.

### Library dependencies (reused, not reimplemented)
- **`esmalert`** — `load_config`/`validate_config`, `RuleKind`/`Group` build, `Group::eval_once`, and read access to each `AlertingRule`'s live `alerts` map for assertions. Targeted `pub(crate)`→`pub` visibility bumps to esmalert's `lib.rs` surface where needed (a small, deliberate change).
- **`esm-gotemplate`** — transitively, annotation rendering during eval.
- **In-process esmetrics stack** — `esm-storage`, `esm-insert`, `esm-select`, `esm-http`, and the `esmetrics` crate's `wiring` (`StorageSink`/`StorageProvider`). Reuses the assembly `crates/esmetrics/tests/server_test.rs` already performs. Primary approach: extract that assembly into a reusable `pub fn` in the esmetrics crate (and have `server_test.rs` call it too, so the harness stays single-sourced); replicate it locally in `harness.rs` only as a fallback if a clean extraction proves impractical.
- **`esm-metricsql`** — parsing the `input_series` metric selector + durations (`duration_value`), consistent with the config layer.

### Flow per invocation
Start the in-process server → for each test file, for each test group: ingest `input_series`; build rule groups from `rule_files`; point an esmalert `Datasource` at the local server; run the eval loop; assert `alert_rule_test` + `metricsql_expr_test` → tear down → aggregate → exit 0/1 with printed diffs.

## Component: test-file schema (`schema.rs`)

Serde structs matching `app/vmalert-tool/unittest/unittest.go`'s `unitTestFile`:
- `UnitTestFile { rule_files: Vec<String>, evaluation_interval: Option<Duration>, group_eval_order: Vec<String>, tests: Vec<TestGroup> }`
- `TestGroup { interval: Option<Duration>, input_series: Vec<Series>, alert_rule_test: Vec<AlertTestCase>, metricsql_expr_test: Vec<MetricsqlTestCase>, external_labels: BTreeMap<String,String>, name: String }`
- `AlertTestCase { eval_time: Duration, groupname: String, alertname: String, exp_alerts: Vec<ExpAlert> }`
- `ExpAlert { exp_labels: BTreeMap<String,String>, exp_annotations: BTreeMap<String,String> }`
- `MetricsqlTestCase { expr: String, eval_time: Duration, exp_samples: Vec<ExpSample> }`
- `ExpSample { labels: String, value: f64 }`
- `Series { series: String, values: String }`

Durations parse via `esm_metricsql::duration_value` (the VM grammar the esmalert config layer already uses). Strict-vs-lenient unknown-field handling matches upstream (upstream is lenient here — do not `deny_unknown_fields` unless upstream does).

## Component: `input_series` parser (`input.rs`)

Faithful port of upstream `parseInputValue` (`unittest/input.go`). Given a `values` string and an `interval`, produce a sequence of `(timestamp, value)` samples starting at `test_start`:
- literals `1 2 3`, floats, `inf`/`-inf`/`nan`;
- expanding `a+bxN` → `a, a+b, …, a+N·b` (N+1 points); `a-bxN` → `a, a-b, …`;
- `_` → a gap (no sample at that step);
- `stale` → a VM staleness marker (reuse the repo's existing StaleNaN constant from the OTLP/import port); `stale` cannot combine with operators;
- combined forms like `1+1x5 _ -4 3+20x1`.

The metric selector `metric{k="v",...}` is parsed with `esm_metricsql`'s parser to extract `__name__` + labels. Each series' samples (skipping gaps; StaleNaN for `stale`) are ingested into the in-process storage via the `StorageSink`/remote-write path. Test cases ported from upstream `input_test.go` so the notation matches exactly.

## Component: runner (`runner.rs`)

Per test group:
1. Ingest `input_series`; flush storage.
2. Build rule groups from `rule_files` via esmalert `load_config` + `validate_config` → runtime `Group`s pointed at the local `Datasource`. `group_eval_order` orders group evaluation (recording rules feeding later rules/exprs).
3. **Eval loop:** from `test_start`, step by `evaluation_interval` (group `interval` overrides) up to `max(eval_time across both test kinds)`; at each `ts`, `Group::eval_once(q, ts, funcs, ctx, Some(rw), None, external_url)` per group in order — `rw` writes recording results + `ALERTS`/`ALERTS_FOR_STATE` back to storage; notifier is `None`. Flush after each group.
4. **Assert at each eval_time:**
   - `alert_rule_test` → read the named group's named `AlertingRule`'s **firing** alerts (live `alerts` map) and set-match each alert's labels+annotations against `exp_alerts` (order-insensitive, like upstream). Report missing / extra / mismatched.
   - `metricsql_expr_test` → run `expr` as an instant query at `ts` against the `Datasource`; set-match returned samples (labels + value within a small float epsilon) against `exp_samples`.
5. Collect all diffs for the group.

## Component: CLI & report (`main.rs`, `report.rs`)

- `esmalert-tool unittest <files...>` — parse each file (bad file → reported error), run its groups, print per-test failures upstream-style (group/alert/expr, expected vs got). Exit 0 iff every test in every file passes, else 1. Mirror esmalert's arg-parsing idiom.
- Errors are per-file with context; a rule-eval error fails that group's test carrying the error; never panic.

## Error handling

- File not found / YAML parse / rule `validate_config` errors → reported per file with context, contribute to a non-zero exit, never panic.
- Rule-eval error during the loop → the group's test fails with that error.
- Temp storage dir cleaned up on exit including on failure (RAII drop on the harness).

## Testing

- **Integration:** port upstream `unittest_test.go` fixtures — real `.yml` files exercising alerting, recording, and expr tests, both passing and intentionally-failing — run the tool against each and assert the pass/fail outcome and that a known-bad fixture yields the expected diff.
- **Unit:** `input.rs` (from `input_test.go` — every notation form), `schema.rs` (parse + duration handling).
- **Harness:** one test proving the in-process server ingests a series and answers an instant query (sanity of the reused assembly).
- Coverage 80%+; both-platform (Linux + Windows CI); `cargo fmt` / `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets` clean; windows-gnu compiles.

## Global constraints

- Files ≤ 800 lines; extract modules when a file grows unwieldy.
- `cargo fmt` clean; `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets` clean; windows-gnu cross-check compiles.
- No tokio; sync stack (esm-http server + blocking reqwest / in-process calls).
- Never log secrets (none expected — offline tool).
- Faithful to upstream v1.146.0 test-file semantics so existing vmalert-tool unittest files run unchanged.
- New workspace member added to root `Cargo.toml` `members`.
- Commit style `<type>: <description>`, no attribution trailers.
- After push, watch the GitHub Actions run and fix failures (Windows tests run only in CI).
