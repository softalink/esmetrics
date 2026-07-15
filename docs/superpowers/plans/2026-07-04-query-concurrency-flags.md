# Query Concurrency Flags Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add upstream-parity `-search.maxConcurrentRequests` and `-search.maxWorkersPerQuery` CLI flags so operators can bound query-phase CPU, wiring them into the existing concurrency limiter, the promql rollup workers, and the storage unpack pool.

**Architecture:** A single process-wide resolved "max workers per query" value lives in `esm-common::query_workers` (new module); `esm-promql` rollup workers and `esm-storage` unpack fan-out both read it, and the `esmetrics` binary sets it from the flag at startup. `-search.maxConcurrentRequests` flows through the existing (but previously unreachable) `SelectConfig`/`ConcurrencyLimiter` in `esm-select`.

**Tech Stack:** Rust workspace (`crates/*`), hand-rolled Go-style flag parser in `crates/esmetrics/src/flags.rs`, `std::sync::OnceLock` statics, cargo test / clippy / fmt, `cargo xwin` for the Windows MSVC build, TSBS monitored benchmark harness (`benchmarks/bench-monitored.{sh,ps1}`).

**Spec:** `docs/superpowers/specs/2026-07-04-query-concurrency-flags-design.md`

## Global Constraints

- Flag names, semantics, and help text mirror upstream VictoriaMetrics v1.146.0 (`-search.maxConcurrentRequests`, `-search.maxWorkersPerQuery`); "See also" sentences referencing flags the port does not define are replaced by a reference to the sibling flag.
- Defaults unchanged: an unflagged run must behave exactly as today. Auto values: `min(2×cpus, 16)` concurrent requests, `min(cpus, 32)` workers per query.
- Precedence for workers per query: CLI flag > `ESM_MAX_QUERY_WORKERS` env var (kept as a debug/benchmarking knob) > auto.
- Query responses must remain byte-identical regardless of cap settings.
- No OS-level ceilings, no thread priorities, no new env vars, no changes to ingest/merge parallelism.
- Every commit: `cargo fmt` clean, `cargo clippy -- -D warnings` clean, workspace tests green. Commit format `<type>: <description>`, no attribution footer.

---

### Task 1: `esm-common::query_workers` — the shared per-query worker cap

**Files:**
- Create: `crates/esm-common/src/query_workers.rs`
- Modify: `crates/esm-common/src/lib.rs` (add `pub mod query_workers;` to the module list after `pub mod memory;`)

**Interfaces:**
- Consumes: nothing (leaf module; `std` only).
- Produces (used by Tasks 2, 3, 4, 5):
  - `pub fn set_max_workers(n: usize)` — installs the flag value; call before the first query.
  - `pub fn max_workers() -> usize` — the resolved cap (≥ 1).
  - `pub fn auto_max_workers(cpus: usize) -> usize` — the `min(cpus, 32)` formula (for usage-text display).

- [ ] **Step 1: Write the failing test**

Create `crates/esm-common/src/query_workers.rs` containing only the test module for the pure resolver (the functions don't exist yet):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_prefers_env_over_auto() {
        // Parsable positive env wins.
        assert_eq!(resolve_max_workers(Some("3"), 8), 3);
        // Unset, unparsable, or zero env falls back to auto.
        assert_eq!(resolve_max_workers(None, 8), 8);
        assert_eq!(resolve_max_workers(Some("abc"), 8), 8);
        assert_eq!(resolve_max_workers(Some("0"), 8), 8);
        assert_eq!(resolve_max_workers(Some("-2"), 8), 8);
    }

    #[test]
    fn auto_is_cpus_capped_at_32() {
        assert_eq!(auto_max_workers(1), 1);
        assert_eq!(auto_max_workers(8), 8);
        assert_eq!(auto_max_workers(48), 32);
        // Degenerate cpus=0 still yields a usable value.
        assert_eq!(auto_max_workers(0), 1);
    }

    #[test]
    fn set_wins_over_everything() {
        // Runs in the same process as the other tests but is the only test
        // touching the static, so the OnceLock observation is deterministic.
        set_max_workers(5);
        assert_eq!(max_workers(), 5);
        // A second set is ignored: the value may already have been observed.
        set_max_workers(7);
        assert_eq!(max_workers(), 5);
    }
}
```

Add `pub mod query_workers;` to `crates/esm-common/src/lib.rs` after the `pub mod memory;` line.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p esm-common query_workers -- --nocapture`
Expected: COMPILE ERROR — `resolve_max_workers`, `auto_max_workers`, `set_max_workers`, `max_workers` not found.

- [ ] **Step 3: Write the implementation**

Prepend to `crates/esm-common/src/query_workers.rs` (above the test module):

```rust
//! Process-wide resolved `-search.maxWorkersPerQuery` value: the maximum
//! number of CPU cores a single query may use. Read by the promql rollup
//! workers (esm-promql) and the storage unpack fan-out (esm-storage), so a
//! single setting caps both layers.
//!
//! Resolution precedence: explicit flag value via [`set_max_workers`] >
//! `ESM_MAX_QUERY_WORKERS` env var (debug/benchmarking knob) > auto
//! `min(cpus, 32)` (the upstream `netstorage.MaxWorkers()` analog).

use std::sync::OnceLock;

static MAX_WORKERS: OnceLock<usize> = OnceLock::new();

/// Installs the `-search.maxWorkersPerQuery` flag value. Must be called
/// before the first query is served; later calls are ignored because the
/// resolved value may already have been observed.
pub fn set_max_workers(n: usize) {
    let _ = MAX_WORKERS.set(n.max(1));
}

/// The resolved per-query worker cap (always ≥ 1).
pub fn max_workers() -> usize {
    *MAX_WORKERS.get_or_init(|| {
        resolve_max_workers(
            std::env::var("ESM_MAX_QUERY_WORKERS").ok().as_deref(),
            std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(1),
        )
    })
}

/// `min(cpus, 32)`: the auto default (upstream `netstorage.MaxWorkers()`).
pub fn auto_max_workers(cpus: usize) -> usize {
    cpus.clamp(1, 32)
}

/// Pure resolution used by [`max_workers`]: parsable positive env value,
/// else auto.
fn resolve_max_workers(env: Option<&str>, cpus: usize) -> usize {
    if let Some(n) = env
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n >= 1)
    {
        return n;
    }
    auto_max_workers(cpus)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p esm-common query_workers`
Expected: `test result: ok. 3 passed`

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt -p esm-common && cargo clippy -p esm-common -- -D warnings
git add crates/esm-common/src/query_workers.rs crates/esm-common/src/lib.rs
git commit -m "feat: shared per-query worker cap in esm-common::query_workers"
```

---

### Task 2: `esm-promql` rollup workers read the shared cap

**Files:**
- Modify: `crates/esm-promql/src/eval.rs:20-38` (the `default_max_workers` function)
- Test: existing `cargo test -p esm-promql` suite (behavior is unchanged; the resolution logic itself is covered by Task 1's tests)

**Interfaces:**
- Consumes: `esm_common::query_workers::max_workers()` (Task 1).
- Produces: `pub fn default_max_workers() -> usize` — unchanged signature; existing callers in esm-promql keep working.

- [ ] **Step 1: Replace the private OnceLock with the shared value**

In `crates/esm-promql/src/eval.rs`, replace the whole function (currently lines 20-38, beginning `/// The maximum number of rollup workers per query` and ending with the closing brace after `.min(32)`) with:

```rust
/// The maximum number of rollup workers per query
/// (`netstorage.MaxWorkers()` analog: `min(cpus, 32)`).
/// Resolved via [`esm_common::query_workers`]: the
/// `-search.maxWorkersPerQuery` flag, else the `ESM_MAX_QUERY_WORKERS`
/// env var (debug/benchmarking knob), else the auto default.
pub fn default_max_workers() -> usize {
    esm_common::query_workers::max_workers()
}
```

If `use std::sync::OnceLock;` (or the `OnceLock` import path used at the top of `eval.rs`) is now unused, delete it — `cargo clippy -- -D warnings` in Step 3 catches this.

- [ ] **Step 2: Run the crate tests**

Run: `cargo test -p esm-promql`
Expected: all existing tests PASS (same resolution semantics as before: env > `min(cpus, 32)`).

- [ ] **Step 3: Lint and commit**

```bash
cargo fmt -p esm-promql && cargo clippy -p esm-promql -- -D warnings
git add crates/esm-promql/src/eval.rs
git commit -m "refactor: resolve promql rollup workers via esm-common query_workers"
```

---

### Task 3: `esm-storage` unpack fan-out honors the per-query cap

**Files:**
- Modify: `crates/esm-storage/src/parallel_search.rs:499-529` (the parallel branch of `unpack_series_parallel`) plus a new `plan_unpack` function and its unit tests
- Create: `crates/esm-storage/tests/parallel_search_cap_test.rs`

**Interfaces:**
- Consumes: `esm_common::query_workers::{max_workers, set_max_workers}` (Task 1).
- Produces: no API change — `Storage::search_series_parallel` signature is untouched; a single query's unpack now claims at most `max_workers()` threads (calling thread included), matching Go `RunParallel(workers)`.

- [ ] **Step 1: Write the failing unit tests for the planning function**

Append to `crates/esm-storage/src/parallel_search.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::plan_unpack;

    #[test]
    fn plan_unpack_respects_per_query_worker_cap() {
        // Cap 3 on an 8-worker pool: the calling thread plus at most
        // 2 helpers.
        let (_, helpers) = plan_unpack(1000, 8, 3);
        assert_eq!(helpers, 2);
        // Cap 1: no helpers — the query runs on the calling thread only.
        let (_, helpers) = plan_unpack(1000, 8, 1);
        assert_eq!(helpers, 0);
        // Uncapped (cap >= pool): full fan-out, caller + pool workers.
        let (batch, helpers) = plan_unpack(1000, 8, 32);
        assert_eq!(batch, 8);
        assert_eq!(helpers, 7);
    }

    #[test]
    fn plan_unpack_never_requests_more_helpers_than_batches() {
        // 4 series at batch 1: the caller plus at most 3 helpers.
        let (batch, helpers) = plan_unpack(4, 8, 8);
        assert_eq!(batch, 1);
        assert_eq!(helpers, 3);
        // Degenerate empty job (guarded by the caller) must not underflow.
        let (_, helpers) = plan_unpack(0, 8, 8);
        assert_eq!(helpers, 0);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p esm-storage --lib parallel_search::tests`
Expected: COMPILE ERROR — `plan_unpack` not found.

- [ ] **Step 3: Implement the cap**

In `crates/esm-storage/src/parallel_search.rs`, add above `unpack_series_parallel`:

```rust
/// Batch size and helper-worker count for a job of `total` series when a
/// single query may use at most `max_workers` threads (calling thread
/// included) of a `pool_workers`-thread pool. Go: `RunParallel(workers)`
/// with `workers = -search.maxWorkersPerQuery`.
fn plan_unpack(total: usize, pool_workers: usize, max_workers: usize) -> (usize, usize) {
    let workers = pool_workers.min(max_workers).max(1);
    let batch = (total / (workers * 4)).clamp(1, MAX_BATCH_SIZE);
    // The calling thread claims batches too, so the job needs at most
    // `workers - 1` helpers, and never more than the remaining batches.
    let helpers = (workers - 1).min(total.div_ceil(batch).saturating_sub(1));
    (batch, helpers)
}
```

Then in `unpack_series_parallel`, make three changes:

1. Replace the fast-path condition (line ~499)

```rust
    if pool.workers <= 1 || total <= MIN_PARALLEL_SERIES {
```

with

```rust
    let max_workers = esm_common::query_workers::max_workers();
    if pool.workers <= 1 || max_workers <= 1 || total <= MIN_PARALLEL_SERIES {
```

2. Replace the batch computation (line ~513)

```rust
    let batch = (total / (pool.workers * 4)).clamp(1, MAX_BATCH_SIZE);
```

with

```rust
    let (batch, helpers) = plan_unpack(total, pool.workers, max_workers);
```

3. Replace the helpers computation (lines ~525-527)

```rust
    // The calling thread claims batches too, so it needs at most enough
    // helpers to cover the remaining batches.
    let helpers = pool.workers.min(total.div_ceil(batch) - 1);
```

with nothing (delete it — `helpers` now comes from `plan_unpack`; keep the
`if helpers > 0 { pool.submit(&job, helpers); }` lines that follow).

Note the uncapped case now submits `pool.workers - 1` helpers instead of
`pool.workers` — correct, because the calling thread participates, and
harmless either way per the over-submission note on `UnpackPool::submit`.

- [ ] **Step 4: Run the unit tests**

Run: `cargo test -p esm-storage --lib parallel_search::tests`
Expected: `2 passed`

- [ ] **Step 5: Write the capped-equivalence integration test**

Create `crates/esm-storage/tests/parallel_search_cap_test.rs`. Integration
test files are separate processes, so setting the process-wide cap here
cannot leak into other test binaries. Reuse the fixture style of the
sibling `parallel_search_test.rs`:

```rust
//! With `-search.maxWorkersPerQuery`-style cap installed, the parallel
//! unpack must produce results identical to the serial path (spec §4:
//! results identical across cap values).

use std::path::PathBuf;

use esm_storage::{
    marshal_metric_name_raw, MetricRow, OpenOptions, SeriesBlock, Storage, TagFilters, TimeRange,
    NO_DEADLINE,
};

const MSEC_PER_MINUTE: i64 = 60 * 1000;
const RETENTION_365D_MSECS: i64 = 365 * 24 * 3600 * 1000;

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "esm-storage-parallel-search-cap-test-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn make_row(metric: &str, host: usize, timestamp: i64, value: f64) -> MetricRow {
    let mut raw = Vec::new();
    let host_tag = format!("host-{host}");
    let labels: Vec<(&[u8], &[u8])> = vec![
        (b"__name__", metric.as_bytes()),
        (b"host", host_tag.as_bytes()),
    ];
    marshal_metric_name_raw(&mut raw, &labels);
    MetricRow {
        metric_name_raw: raw,
        timestamp,
        value,
    }
}

#[test]
fn capped_parallel_unpack_matches_serial_path() {
    // Cap the whole process to 2 workers per query BEFORE any search runs.
    esm_common::query_workers::set_max_workers(2);
    assert_eq!(esm_common::query_workers::max_workers(), 2);

    let dir = test_dir("equivalence");
    let storage = Storage::must_open(
        &dir,
        OpenOptions {
            retention_msecs: RETENTION_365D_MSECS,
            ..Default::default()
        },
    );
    let base_ts = now_ms() - 200 * MSEC_PER_MINUTE;

    // 40 series x 60 samples across two flushed parts so the parallel path
    // (total > MIN_PARALLEL_SERIES) actually engages under the cap.
    for part in 0..2 {
        let mut rows = Vec::new();
        for host in 0..40 {
            for i in 0..30 {
                let ts = base_ts + (part * 30 + i) * MSEC_PER_MINUTE;
                rows.push(make_row("cap_metric", host, ts, (host * 1000 + i as usize) as f64));
            }
        }
        storage.add_rows(&rows).expect("add_rows must succeed");
        storage.force_flush();
    }

    let mut tfs = TagFilters::new();
    tfs.add(&[], b"cap_metric", false, false).unwrap();
    let tr = TimeRange {
        min_timestamp: base_ts,
        max_timestamp: base_ts + 200 * MSEC_PER_MINUTE,
    };

    // Serial reference via Search::next_series.
    let mut search = storage
        .search(&[tfs.clone()], tr, 100_000, NO_DEADLINE)
        .expect("search must succeed");
    let mut serial: Vec<(String, Vec<i64>, Vec<f64>)> = Vec::new();
    let mut sb = SeriesBlock::default();
    while search.next_series(&mut sb).expect("next_series must succeed") {
        serial.push((sb.metric_name.to_string(), sb.timestamps.clone(), sb.values.clone()));
    }
    serial.sort_by(|a, b| a.0.cmp(&b.0));
    drop(search);

    // Capped parallel path.
    let mut parallel: Vec<(String, Vec<i64>, Vec<f64>)> = storage
        .search_series_parallel(&[tfs], tr, 100_000, NO_DEADLINE)
        .expect("search_series_parallel must succeed")
        .into_iter()
        .map(|sb| (sb.metric_name.to_string(), sb.timestamps, sb.values))
        .collect();
    parallel.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(serial.len(), 40, "fixture must produce 40 series");
    assert_eq!(serial, parallel, "capped parallel unpack must equal serial");

    storage.must_close();
    let _ = std::fs::remove_dir_all(&dir);
}
```

Note: if `esm-common` is not already a dev-dependency of `esm-storage`, no
change is needed — it is a regular dependency (`crates/esm-storage/Cargo.toml`
line 11), so `esm_common::` paths resolve in integration tests. If
`add_rows`/`force_flush` names differ, mirror whatever
`crates/esm-storage/tests/parallel_search_test.rs` `fill_storage` uses —
that file is the source of truth for the ingestion fixture API.

- [ ] **Step 6: Run the integration test**

Run: `cargo test -p esm-storage --test parallel_search_cap_test`
Expected: `1 passed`

Also run the pre-existing equivalence suite (uncapped process — separate binary):

Run: `cargo test -p esm-storage --test parallel_search_test`
Expected: all PASS (uncapped default path unchanged)

- [ ] **Step 7: Lint and commit**

```bash
cargo fmt -p esm-storage && cargo clippy -p esm-storage -- -D warnings
git add crates/esm-storage/src/parallel_search.rs crates/esm-storage/tests/parallel_search_cap_test.rs
git commit -m "feat: cap per-query unpack fan-out at query_workers::max_workers"
```

---

### Task 4: the two CLI flags

**Files:**
- Modify: `crates/esm-select/src/lib.rs:91-96` (make `default_max_concurrent_requests` pub)
- Modify: `crates/esmetrics/src/flags.rs` (FLAG_DEFS, `Flags`, parser match, `set_flag`, `usage`, tests)

**Interfaces:**
- Consumes: `esm_select::default_max_concurrent_requests()` (made pub here), `esm_common::query_workers::auto_max_workers()` (Task 1).
- Produces (used by Task 5):
  - `Flags.search_max_concurrent_requests: usize` (0 = auto)
  - `Flags.search_max_workers_per_query: usize` (0 = auto)

- [ ] **Step 1: Make the esm-select default formula pub**

In `crates/esm-select/src/lib.rs`, change

```rust
/// `getDefaultMaxConcurrentRequests` from app/victoria-metrics/main.go.
fn default_max_concurrent_requests() -> usize {
```

to

```rust
/// `getDefaultMaxConcurrentRequests` from app/victoria-metrics/main.go:
/// `min(2 × cpus, 16)`. Pub so the binary's usage text can display the
/// computed default the way Go's flag package does.
pub fn default_max_concurrent_requests() -> usize {
```

- [ ] **Step 2: Write the failing flag tests**

In `crates/esmetrics/src/flags.rs`, extend the existing `tests` module:

In `defaults_when_no_args`, add before the closing brace:

```rust
        assert_eq!(flags.search_max_concurrent_requests, 0);
        assert_eq!(flags.search_max_workers_per_query, 0);
```

In `parses_every_defined_flag`, add `"-search.maxConcurrentRequests=4",` and
`"-search.maxWorkersPerQuery=2",` to the argument array, and these asserts:

```rust
        assert_eq!(flags.search_max_concurrent_requests, 4);
        assert_eq!(flags.search_max_workers_per_query, 2);
```

In `invalid_numeric_and_level_values_are_errors`, add:

```rust
        assert!(parse_flags(&["-search.maxConcurrentRequests=abc"]).is_err());
        assert!(parse_flags(&["-search.maxConcurrentRequests=-1"]).is_err());
        assert!(parse_flags(&["-search.maxWorkersPerQuery=1.5"]).is_err());
        assert!(parse_flags(&["-search.maxWorkersPerQuery=-3"]).is_err());
```

Add a new test:

```rust
    #[test]
    fn search_flags_usage_shows_computed_defaults() {
        let usage = usage();
        let cpus = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1);
        // Computed, unquoted defaults — Go flag-package style.
        assert!(
            usage.contains(&format!(
                "-search.maxConcurrentRequests\n    \tThe maximum number of concurrent search requests"
            )),
            "{usage}"
        );
        assert!(
            usage.contains(&format!(
                "(default {})",
                esm_select::default_max_concurrent_requests()
            )),
            "{usage}"
        );
        assert!(
            usage.contains(&format!(
                "(default {})",
                esm_common::query_workers::auto_max_workers(cpus)
            )),
            "{usage}"
        );
    }
```

- [ ] **Step 3: Run to verify failure**

Run: `cargo test -p esmetrics --lib flags`
Expected: COMPILE ERROR — `search_max_concurrent_requests` field not found.

- [ ] **Step 4: Implement the flags**

All edits in `crates/esmetrics/src/flags.rs`:

1. In `FLAG_DEFS`, insert before the `("version", ...)` entry (empty default
strings — `usage()` computes them):

```rust
    (
        "search.maxConcurrentRequests",
        "",
        "The maximum number of concurrent search requests. It shouldn't be high, \
         since a single request can saturate all the CPU cores, while many \
         concurrently executed requests may require high amounts of memory. \
         See also -search.maxWorkersPerQuery",
    ),
    (
        "search.maxWorkersPerQuery",
        "",
        "The maximum number of CPU cores a single query can use. The default value \
         should work good for most cases. The flag can be set to lower values for \
         improving performance of big number of concurrently executed queries. \
         The flag can be set to bigger values for improving performance of heavy \
         queries, which scan big number of time series (>10K) and/or big number \
         of samples (>100M). There is no sense in setting this flag to values \
         bigger than the number of CPU cores available on the system",
    ),
```

(Upstream's "See also -search.maxQueueDuration and -search.maxMemoryPerQuery"
references flags the port doesn't define; the sibling-flag reference replaces
it, per the spec's global constraints.)

2. In `struct Flags`, after `pub memory_allowed_bytes: i64,`:

```rust
    /// `-search.maxConcurrentRequests`; 0 → auto `min(2 × cpus, 16)`.
    pub search_max_concurrent_requests: usize,
    /// `-search.maxWorkersPerQuery`; 0 → auto (env override or `min(cpus, 32)`).
    pub search_max_workers_per_query: usize,
```

3. In `impl Default for Flags`, after `memory_allowed_bytes: 0,`:

```rust
            search_max_concurrent_requests: 0,
            search_max_workers_per_query: 0,
```

4. In `parse`, extend the value-taking match arm list:

```rust
            "httpListenAddr"
            | "storageDataPath"
            | "retentionPeriod"
            | "loggerLevel"
            | "memory.allowedPercent"
            | "memory.allowedBytes"
            | "search.maxConcurrentRequests"
            | "search.maxWorkersPerQuery" => {
```

5. In `set_flag`, before the `_ => unreachable!` arm:

```rust
        "search.maxConcurrentRequests" => {
            flags.search_max_concurrent_requests = value.parse().map_err(|_| {
                format!("invalid value {value:?} for flag -search.maxConcurrentRequests")
            })?;
        }
        "search.maxWorkersPerQuery" => {
            flags.search_max_workers_per_query = value.parse().map_err(|_| {
                format!("invalid value {value:?} for flag -search.maxWorkersPerQuery")
            })?;
        }
```

6. In `usage()`, replace the loop body

```rust
    for (name, default, help) in FLAG_DEFS {
        s.push_str("  -");
        s.push_str(name);
        s.push_str("\n    \t");
        s.push_str(help);
        if !default.is_empty() {
            s.push_str(&format!(" (default {default:?})"));
        }
        s.push('\n');
    }
```

with

```rust
    let cpus = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    for (name, default, help) in FLAG_DEFS {
        s.push_str("  -");
        s.push_str(name);
        s.push_str("\n    \t");
        s.push_str(help);
        // The search.* int flags display their computed auto default,
        // unquoted, the way Go's flag package prints int defaults.
        match *name {
            "search.maxConcurrentRequests" => s.push_str(&format!(
                " (default {})",
                esm_select::default_max_concurrent_requests()
            )),
            "search.maxWorkersPerQuery" => s.push_str(&format!(
                " (default {})",
                esm_common::query_workers::auto_max_workers(cpus)
            )),
            _ if !default.is_empty() => s.push_str(&format!(" (default {default:?})")),
            _ => {}
        }
        s.push('\n');
    }
```

- [ ] **Step 5: Run the flag tests**

Run: `cargo test -p esmetrics --lib flags`
Expected: all PASS, including `unknown_flag_lists_all_flags` (it iterates
FLAG_DEFS, so the new flags are asserted in the error text automatically).

- [ ] **Step 6: Lint and commit**

```bash
cargo fmt -p esmetrics -p esm-select && cargo clippy -p esmetrics -p esm-select -- -D warnings
git add crates/esmetrics/src/flags.rs crates/esm-select/src/lib.rs
git commit -m "feat: -search.maxConcurrentRequests / -search.maxWorkersPerQuery flags"
```

---

### Task 5: wire the flags into the server

**Files:**
- Modify: `crates/esmetrics/src/lib.rs:12-14` (imports), `:57-71` (`run`), tests module at end of file

**Interfaces:**
- Consumes: `Flags.search_max_concurrent_requests`, `Flags.search_max_workers_per_query` (Task 4); `SelectConfig`, `SelectHandlers::with_config` (existing, `esm-select`); `esm_common::query_workers::set_max_workers` (Task 1).
- Produces: `fn select_config(flags: &Flags) -> SelectConfig` (crate-private helper, unit-tested).

- [ ] **Step 1: Write the failing test**

Append to the `tests` module in `crates/esmetrics/src/lib.rs`:

```rust
    #[test]
    fn select_config_maps_flags() {
        use crate::flags::Flags;

        // 0 stays 0: esm-select resolves it to min(2 × cpus, 16) itself.
        let auto = super::select_config(&Flags::default());
        assert_eq!(auto.max_concurrent_requests, 0);

        let capped = super::select_config(&Flags {
            search_max_concurrent_requests: 2,
            ..Flags::default()
        });
        assert_eq!(capped.max_concurrent_requests, 2);
        // Everything else keeps the upstream defaults.
        assert_eq!(capped.max_queue_duration_ms, 10_000);
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p esmetrics --lib select_config_maps_flags`
Expected: COMPILE ERROR — `select_config` not found.

- [ ] **Step 3: Implement the wiring**

In `crates/esmetrics/src/lib.rs`:

1. Change the import (line 14) from

```rust
use esm_select::SelectHandlers;
```

to

```rust
use esm_select::{SelectConfig, SelectHandlers};
```

2. Add above `run`:

```rust
/// Builds the select config from the command-line flags
/// (upstream flag → vmselect config).
fn select_config(flags: &Flags) -> SelectConfig {
    SelectConfig {
        max_concurrent_requests: flags.search_max_concurrent_requests,
        ..SelectConfig::default()
    }
}
```

3. In `run`, install the worker cap before storage opens (first statement of
the function), so no query path can observe the default first:

```rust
pub fn run(flags: &Flags) -> io::Result<App> {
    if flags.search_max_workers_per_query > 0 {
        esm_common::query_workers::set_max_workers(flags.search_max_workers_per_query);
    }
```

4. Replace the `SelectHandlers::new` call (lines 69-71):

```rust
    let select = SelectHandlers::new(StorageProvider {
        storage: Arc::clone(&storage),
    });
```

with

```rust
    let select = SelectHandlers::with_config(
        StorageProvider {
            storage: Arc::clone(&storage),
        },
        select_config(flags),
    );
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p esmetrics`
Expected: all PASS (unit + integration; integration tests start the server
in-process with default flags, exercising the unchanged-defaults path).

- [ ] **Step 5: Manual smoke check**

```bash
cargo build --release
./target/release/esmetrics --help | grep -A2 "search.max"
```

Expected: both flags listed with upstream help text and computed numeric
defaults (e.g. `(default 8)` / `(default 4)` on the 4-core host).

```bash
./target/release/esmetrics -search.maxWorkersPerQuery=abc; echo "exit=$?"
```

Expected: `invalid value "abc" for flag -search.maxWorkersPerQuery` and a
non-zero exit.

- [ ] **Step 6: Lint and commit**

```bash
cargo fmt -p esmetrics && cargo clippy -p esmetrics -- -D warnings
git add crates/esmetrics/src/lib.rs
git commit -m "feat: wire query concurrency flags into the server"
```

---

### Task 6: operator documentation

**Files:**
- Modify: `README.md` (after the "Building from source" section's closing paragraph, before "## Repository layout")

**Interfaces:** none (docs only).

- [ ] **Step 1: Add the headroom section**

Insert into `README.md` between the "Building from source" section and
"## Repository layout":

```markdown
## Limiting query CPU usage

When co-locating EsMetrics with other workloads, bound the query engine the
same way as upstream VictoriaMetrics:

```
esmetrics -search.maxConcurrentRequests=2 -search.maxWorkersPerQuery=3
```

Aggregate query CPU is bounded by roughly
`maxConcurrentRequests × maxWorkersPerQuery` cores (e.g. `2 × 3` on an
8-core host keeps ~2 cores free under full query load). Defaults —
`min(2 × cpus, 16)` concurrent requests, `min(cpus, 32)` workers per
query — use the whole machine, matching upstream. The caps only trade
latency for headroom: query CPU-seconds stay flat (the query path is
work-conserving; see `benchmarks/results/README.md`), and results are
byte-identical at any setting.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: operator guide for query CPU limiting flags"
```

---

### Task 7: full-workspace verification and release builds

**Files:** none (verification only).

- [ ] **Step 1: Full test suite**

Run: `cargo fmt --check && cargo clippy --workspace -- -D warnings && cargo test --workspace`
Expected: clean fmt, no clippy warnings, all tests PASS.

- [ ] **Step 2: Release builds, both platforms**

```bash
cargo build --release
cargo xwin build --release --target x86_64-pc-windows-msvc
```

Expected: both succeed; note the two binary paths for Task 8:
`target/release/esmetrics`, `target/x86_64-pc-windows-msvc/release/esmetrics.exe`.

- [ ] **Step 3: Commit anything outstanding**

Only if fmt/clippy fixes were needed in Step 1:

```bash
git add -u && git commit -m "chore: fmt/clippy fixes from workspace verification"
```

---

### Task 8: benchmark validation on both platforms

**Files:**
- Modify: `benchmarks/bench-monitored.ps1:6-8` (accept extra server args)
- Modify: `benchmarks/results/README.md` (append validation note)

Spec §5 success criteria: capped peak ≤ ~650% on the 8-CPU Windows host
(≤ ~300% with 2×2 caps on the 4-core Linux host), query CPU-seconds ≈
uncapped, responses byte-identical, uncapped round matches the committed
baseline. Windows always runs on **agent-6.home** with the **MSVC** build
(`powershell -ExecutionPolicy Bypass` required on that host).

- [ ] **Step 1: Let the PS1 harness pass extra server args**

In `benchmarks/bench-monitored.ps1`, change the param block:

```powershell
param(
    [Parameter(Mandatory = $true)][string]$Label,
    [Parameter(Mandatory = $true)][string]$ServerExe,
    [string[]]$ExtraArgs = @()
)
```

and the server start:

```powershell
$server = Start-Process -FilePath $ServerExe -ArgumentList (@(
    "-storageDataPath=$Storage", "-retentionPeriod=100y", "-httpListenAddr=:$Port"
) + $ExtraArgs) -RedirectStandardOutput "$Results\server-out.log" -RedirectStandardError "$Results\server-err.log" -PassThru -NoNewWindow
```

- [ ] **Step 2: Linux rounds (4-core host)**

```bash
cd benchmarks
./bench-monitored.sh rust-capfree ../target/release/esmetrics
./bench-monitored.sh rust-capped ../target/release/esmetrics \
    -search.maxConcurrentRequests=2 -search.maxWorkersPerQuery=2
./analyze-resources.py results/rust-capfree results/rust-capped
```

Expected: `rust-capfree` query phase within noise of the committed baseline
(peak ~376%, cpuSec ~78); `rust-capped` query peak ≤ ~300% with cpuSec in
the same ~78 range (longer wall time is expected and fine).

- [ ] **Step 3: Windows rounds (agent-6.home, MSVC)**

```bash
scp target/x86_64-pc-windows-msvc/release/esmetrics.exe \
    test@agent-6.home:C:/bench/esmetrics-msvc-capflags.exe
scp benchmarks/bench-monitored.ps1 test@agent-6.home:C:/bench/bench-monitored.ps1
ssh test@agent-6.home "powershell -ExecutionPolicy Bypass -File C:\bench\bench-monitored.ps1 -Label monrust-capfree -ServerExe C:\bench\esmetrics-msvc-capflags.exe"
ssh test@agent-6.home "powershell -ExecutionPolicy Bypass -File C:\bench\bench-monitored.ps1 -Label monrust-capped -ServerExe C:\bench\esmetrics-msvc-capflags.exe -ExtraArgs '-search.maxConcurrentRequests=2','-search.maxWorkersPerQuery=3'"
scp -r "test@agent-6.home:C:/bench/results-monrust-cap*" /tmp/cap-validation/
benchmarks/analyze-resources.py /tmp/cap-validation/results-monrust-capfree /tmp/cap-validation/results-monrust-capped
```

Expected: `capfree` within noise of the committed baseline (query peak
~786%, cpuSec ~135, lifetime peak RSS ~535 MiB); `capped` query peak ≤
~650% with cpuSec ≈ 135.

- [ ] **Step 4: Response byte-identity under caps (Linux, cheap)**

```bash
S=/tmp/capcheck; rm -rf $S-a $S-b
./target/release/esmetrics -storageDataPath=$S-a -retentionPeriod=100y -httpListenAddr=:8428 & PID_A=$!
sleep 1
/home/test/refsrc/bin/tsbs_load_victoriametrics --file=/home/test/refsrc/tsbs-data/cpu-only-100h-1d.lp --urls=http://127.0.0.1:8428/write --workers=4 --batch-size=10000
sleep 5; curl -s http://127.0.0.1:8428/internal/force_flush
/home/test/refsrc/bin/tsbs_run_queries_victoriametrics --file=/home/test/refsrc/tsbs-data/queries-double-groupby-all.dat --workers=1 --max-queries=100 --urls=http://127.0.0.1:8428 --print-responses > /tmp/resp-uncapped.txt
kill $PID_A; wait $PID_A 2>/dev/null

./target/release/esmetrics -storageDataPath=$S-b -retentionPeriod=100y -httpListenAddr=:8428 -search.maxConcurrentRequests=2 -search.maxWorkersPerQuery=2 & PID_B=$!
sleep 1
/home/test/refsrc/bin/tsbs_load_victoriametrics --file=/home/test/refsrc/tsbs-data/cpu-only-100h-1d.lp --urls=http://127.0.0.1:8428/write --workers=4 --batch-size=10000
sleep 5; curl -s http://127.0.0.1:8428/internal/force_flush
/home/test/refsrc/bin/tsbs_run_queries_victoriametrics --file=/home/test/refsrc/tsbs-data/queries-double-groupby-all.dat --workers=1 --max-queries=100 --urls=http://127.0.0.1:8428 --print-responses > /tmp/resp-capped.txt
kill $PID_B; wait $PID_B 2>/dev/null

diff <(sed 's/"executionTimeMsec":[0-9.]*//g' /tmp/resp-uncapped.txt) \
     <(sed 's/"executionTimeMsec":[0-9.]*//g' /tmp/resp-capped.txt) && echo IDENTICAL
```

Expected: `IDENTICAL` (only the timing stat may differ, as in the June
correctness check).

- [ ] **Step 5: Record and commit**

Append to `benchmarks/results/README.md` under the "Resource usage" section
a short "### Bounding query CPU" note with the measured capped numbers from
Steps 2-3 (fill in the actual values), e.g.:

```markdown
### Bounding query CPU (`-search.maxConcurrentRequests` / `-search.maxWorkersPerQuery`)

Validation rounds (2026-07-04, same regimen): with `2 × 3` caps on the
8-CPU Windows host, query-phase peak CPU drops 786% → <measured>% with
CPU-seconds ≈ <measured> (unchanged — the query path is work-conserving,
so caps trade latency for headroom, not throughput). With `2 × 2` on the
4-core Linux host: 376% → <measured>%. Responses byte-identical under all
cap settings; uncapped rounds match the tables above.
```

```bash
git add benchmarks/bench-monitored.ps1 benchmarks/results/README.md
git commit -m "test: validate query CPU caps on Linux and Windows (agent-6, MSVC)"
```

---

## Self-Review

- **Spec coverage:** §1 flags → Task 4; §2 plumbing → Task 5; §3 both-layer cap → Tasks 1-3 (single shared setting consolidates the spec's two setters); §4 tests → Tasks 1, 3, 4, 5 (the spec's "high-water-mark" concurrency assertion is realized as the `plan_unpack` claim-count unit tests plus the capped-equivalence integration test — the claim arithmetic is what bounds concurrency, and it is exactly unit-testable); §5 validation → Task 8; README headroom docs → Task 6; success criteria 1-5 → Tasks 4/5 (help text), 7 (defaults/tests), 8 (measured caps + byte-identity).
- **Placeholder scan:** Task 8 Step 5 `<measured>` values are intentionally filled at execution time from the just-measured rounds; every other step carries complete code/commands.
- **Type consistency:** `set_max_workers(usize)`, `max_workers() -> usize`, `auto_max_workers(usize) -> usize` used identically in Tasks 1-5; `plan_unpack(usize, usize, usize) -> (usize, usize)` defined and consumed only in Task 3; `Flags.search_max_concurrent_requests`/`search_max_workers_per_query: usize` named identically in Tasks 4 and 5; `select_config(&Flags) -> SelectConfig` defined and tested in Task 5.
```
