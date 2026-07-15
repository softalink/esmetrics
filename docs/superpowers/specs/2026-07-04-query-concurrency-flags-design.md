# Query concurrency flags: `-search.maxConcurrentRequests` / `-search.maxWorkersPerQuery`

**Date:** 2026-07-04
**Status:** approved

## Problem

The esmetrics binary exposes no query-concurrency controls: `--help` lists six
flags, none of which bound query parallelism. Upstream VictoriaMetrics
v1.146.0 exposes `-search.maxConcurrentRequests` and
`-search.maxWorkersPerQuery` for exactly this purpose. An operator co-locating
esmetrics with other workloads today cannot stop the query path from
saturating every core (measured: 786% peak on the 8-logical-CPU Windows
benchmark host, 376% on the 4-core Linux host — machine saturation on both).

The internals already exist but are unreachable or incomplete:

- `esm-select` has a `ConcurrencyLimiter` and a `SelectConfig` whose
  `max_concurrent_requests` (0 = auto `min(2×cpus, 16)`) and
  `max_queue_duration_ms` (10s) mirror upstream — but the server main calls
  `SelectHandlers::new`, which always uses defaults.
- `esm-promql`'s rollup worker count (`default_max_workers()`, auto
  `min(cpus, 32)`) is overridable only via the undocumented
  `ESM_MAX_QUERY_WORKERS` env var.
- `esm-storage/src/parallel_search.rs` fans each query's block unpacking
  across a process-wide `available_parallelism`-sized pool with **no
  per-query cap** — the Rust counterpart of Go's netstorage unpack workers,
  which upstream's `-search.maxWorkersPerQuery` does cap (via
  `RunParallel(workers)`).

## Evidence (2026-07-04, agent-6.home, MSVC build, monitored TSBS regimen)

Capping `ESM_MAX_QUERY_WORKERS` (query phase, 4 TSBS client workers):

| workers | peak CPU | avg CPU | CPU-seconds | wall |
|---|---|---|---|---|
| 8 (default) | 786% | 730% | 135.2 | 18.5s |
| 6 | 779% | 736% | 136.2 | 18.5s |
| 4 | 778% | 727% | 133.3 | 18.3s |
| 1 | 691% | 643% | 125.1 | 19.4s |

Two findings:

1. **No busy-waiting.** Total CPU-seconds are flat-to-slightly-lower as
   parallelism drops; wall time barely moves. The high peak is genuine,
   work-conserving computation (Rust keeps cores busier than Go's scheduler;
   it also finishes the workload 2.5× sooner with 60% less total CPU).
2. **The rollup-worker knob alone cannot deliver headroom.** Even `workers=1`
   peaks at 691% because the storage unpack pool fans out per query
   independently of it. A faithful `-search.maxWorkersPerQuery` must cap both
   layers.

## Design

Upstream-parity CLI flags, wired to the existing internals. No OS-level
ceilings, no thread priorities, no new env vars, no change to ingest/merge
parallelism.

### 1. Flags (`crates/esmetrics/src/flags.rs`)

Add to `FLAG_DEFS`, `Flags`, `set_flag`, and the parser's known-name match:

- `-search.maxConcurrentRequests` (int). 0 = auto `min(2×cpus, 16)`.
  Upstream help text verbatim. Usage output shows the computed auto value as
  the default, as Go's flag package does.
- `-search.maxWorkersPerQuery` (int). 0 = auto `min(cpus, 32)`. Upstream help
  text verbatim; computed default shown likewise.

Both reject non-integer and negative values with Go-style error messages.

### 2. Plumbing (`crates/esmetrics/src/lib.rs`)

Build a `SelectConfig` from `Flags` (only the two new fields deviate from
`SelectConfig::default()`) and switch `SelectHandlers::new` →
`SelectHandlers::with_config`. At startup, when
`-search.maxWorkersPerQuery` > 0, call the new eval setter (below) before the
HTTP server starts serving.

### 3. Per-query worker cap in both layers

- **`esm-promql/src/eval.rs`** — `default_max_workers()` gains a companion
  setter (`set_default_max_workers(n)`) that initializes the `OnceLock`.
  Precedence: CLI flag > `ESM_MAX_QUERY_WORKERS` (retained as a
  benchmarking/debug knob) > auto `min(cpus, 32)`.
- **`esm-storage/src/parallel_search.rs`** — each query's unpack claims at
  most `max_workers_per_query` workers from the shared pool (calling thread
  counts as one, matching Go `RunParallel(workers)` semantics). The pool
  itself stays `available_parallelism`-sized so concurrent queries share it;
  the cap limits a single query's claim, not the pool. The cap value flows
  from the same resolved setting as the eval layer.

Aggregate query CPU is then bounded by roughly
`maxConcurrentRequests × maxWorkersPerQuery` cores — the same operator
playbook as upstream (e.g. `2 × 3` on the 8-CPU host reserves ~2 cores of
guaranteed slack).

### 4. Tests

- `flags.rs`: extend the existing parse-table tests (both flags, all
  syntaxes, invalid values, usage text lists them with computed defaults).
- `esmetrics`: `SelectConfig`-from-`Flags` unit test.
- `esm-promql`: setter/env/auto precedence test (serialized around the
  `OnceLock` as existing env-dependent tests are).
- `esm-storage`: with cap `N`, a query never has more than `N` workers
  active concurrently (atomic high-water-mark assertion in a test unpack);
  results are byte-identical across cap values 1, 2, unbounded.

### 5. Validation (documented in `benchmarks/results/README.md`)

Rebuild Linux + MSVC (`cargo xwin`). On agent-6.home, run the monitored
regimen with `-search.maxConcurrentRequests=2 -search.maxWorkersPerQuery=3`:

- query-phase peak CPU ≤ ~650% (≥ ~2 cores headroom on 8),
- query CPU-seconds ≈ 135 (work-conserving, no throughput cliff),
- TSBS query responses byte-identical to an uncapped run,
- one uncapped round matches the committed baseline (no default-path
  regression).

Same smoke check on Linux (`-search.maxConcurrentRequests=2
-search.maxWorkersPerQuery=2` on the 4-core host, expect ≤ ~300% peak).

## Success criteria

1. `--help` lists both flags with upstream-matching text and computed
   defaults; unknown-flag error text includes them.
2. Defaults unchanged: an unflagged run behaves exactly as today (auto
   values, benchmark numbers within noise of the committed baseline).
3. With caps set, measured peak query CPU respects
   `maxConcurrentRequests × maxWorkersPerQuery` within measurement noise,
   on both platforms.
4. Query responses remain byte-identical to upstream regardless of cap
   settings.
5. All existing tests pass; new tests cover the four bullets in §4.
