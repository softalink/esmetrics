# EsMetrics performance/correctness improvement plan (from TSBS gaps)

Derived from [`tsbs-comparison.md`](./tsbs-comparison.md). EsMetrics trails
VM on every dimension; all gaps trace to a handful of storage-engine root
causes. Ordered by impact ÷ effort. Each item has a verification gate.

## P1 — Incremental flush + in-memory query visibility  ✅ DONE (2026-05-29)
**Fixed:** ingest RAM, persistence blocker, the scale cap, the superlinear
flush, and "queries can't see unflushed data".
- ✅ Flush trigger: auto-flush once `pending` crosses ~1M samples
  (`FLUSH_THRESHOLD_SAMPLES`), so parts stream to disk during ingest.
- ✅ `search_by_tsid` now merges the in-memory `pending` buffer with on-disk
  parts via a timestamp-keyed map (pending wins on ties) — queries see fresh
  data with no forced flush.
- ✅ **Root-caused & fixed the flush superlinearity:** `compress_level_for`
  used zstd **level 22** for ≥8192-item blocks (a port error — VM caps at 5).
  Level 22 on a full block ran ~198 ms; fixed to match VM's `getCompressLevel`.

**Results (scale=1000, 25.92M rows):**
| metric | before | after |
|---|---|---|
| peak RSS | 5.5 GB | **1.58 GB** (now < VM's 2.3 GB) |
| persistence | none | **100 MB on disk** |
| query @ scale-1000 w/o flush | impossible (66 s flush) | **instant, 1000 series** |
| flush micro (100×20k pts) | 47.6 s | **31 ms** (~1500×) |
| ingest (now persisting) | 354K rows/s* | 127K rows/s |

\* The old 354K never wrote to disk. 127K is honest persisting throughput;
still 5× behind VM's 643K — remaining gap is merge write-amplification
(259 flushes → 7 parts) + the unindexed read path → **P3/P5 + merger tuning.**

**Merger tuning ✅ DONE (2026-05-29):** replaced "merge the 4 smallest on
every flush" with **size-tiered compaction** (bucket parts by `floor(log2)`,
compact a tier once it holds ≥4 parts, ≤8 per merge). Cut write-amplification
→ persisting ingest **127K → 182K rows/s (+43%)**, RSS ~1.9 GB, 9 parts.

**Remaining ingest gap (182K vs VM 643K, ~3.5×):** now dominated by the global
`Arc<Mutex<Storage>>` — each inline flush freezes all ingest workers. Closing
it needs background/concurrent flush → **P4 (concurrency)**, not the merger.

## P2 — Query correctness  ✅ DONE (2026-05-29)
**Fixed:** all 7 failing types resolved → **4/11 → 11/11 correct** (series
counts match VM; values exact for max, ~1% for avg_over_time window rounding).
The root causes were *not* a missing index (the evaluator already scans all
series with `matches_selector`); they were two specific bugs:
- **Regex matcher** was a hand-rolled micro-engine (only literals, `.`, `.*`)
  → `{__name__=~'cpu_(a|b)'}` matched nothing. Replaced with anchored RE2
  (`regex` crate, `^(?:pat)$`), thread-local cached so a selector compiles each
  pattern once across all series.
- **`by (__name__)` grouping** collapsed everything into one empty group:
  `group_key` parsed the metric name out but never exposed `__name__` as a
  groupable label. Now `__name__` is a groupable label (`by` retains it —
  VM/TSBS rely on this; `without` drops it).
- **Verify:** ✅ all 11 TSBS types match VM series counts; cpu-max-all values
  identical, double-groupby-all within ~1% (avg window-boundary nuance noted).
- Note: resolution is still O(all series) per query (no inverted index yet) —
  a *perf* follow-up that overlaps with P3, not a correctness gap.

## P3 — Query read-path efficiency  ✅ PARTIAL (2026-05-29)
**Was:** `search_by_tsid` did a `read_dir` + opened every part on every call.
- ✅ Added an in-memory `parts_index` (per-part min/max timestamp), maintained
  incrementally on flush/merge/retention. Queries iterate it and **prune parts
  by time range before opening** — no per-query `read_dir`. Also rewrote
  `enforce_retention` to use it (no longer re-reads every block).
- **Result (med latency @ 8 workers, vs original baseline):**
  single-groupby-1-1-1 52.7→7.6 ms, -1-1-12 612→89 ms, double-groupby-1
  103→16 ms — ~**6.9×** on time-pruned queries (combined with P2/P4).
- **Still pending:** resolution is still O(all series) per query
  (`iter_metric_names` + `matches_selector` scan). A true **inverted label
  index** (label→postings) is the next lever for regex/multi-host queries;
  per-part TSID→block-offset caching would cut repeated part opens further.

## P4 — Concurrency  *(query side ✅ DONE 2026-05-29; ingest side pending)*
**Was:** global `Arc<Mutex<Storage>>` serialized all work.
- ✅ Switched esm-single to `Arc<RwLock<Storage>>`; read-only query handlers
  (`promql_instant`/`promql_range`) now take a shared **read** lock (the
  optional `flush=true` is the only path needing the write lock).
- **Result (single-groupby-1-8-1 qps):** was flat 1288→1494→1440 across
  1/8/16 workers; now **1228→6643→7966 (~6.5× scaling)**, vs VM's ~10×.
- **Still pending — ingest concurrency:** ingest takes the write lock, so the
  8 loader workers still serialize. Closing the ~3.5× ingest gap needs
  **sharded ingest** (per-shard locks / lock-free write buffer) so appends
  run in parallel — a larger change tracked separately.

## P5 / sharded ingest — concurrency on the write path  ✅ DONE (2026-05-29)
**Was:** ingest took the single global write lock, serializing all 8 loader
workers (~1 core of work while VM uses many).
- ✅ Added `ShardedStorage`: N independent `Storage` shards (each its own
  subdir + lock), series routed by a stable FNV-1a hash of the metric name.
  esm-single uses `min(cores, 16)` shards. Ingest partitions a batch to shards;
  concurrent callers hit different shards in parallel.
- ✅ Kept the evaluator agnostic via a new `QueryStore` trait implemented by
  both `Storage` and `ShardedStorage` (queries route point-reads to the owning
  shard, fan out whole-store scans). No TSID-coherence work needed — the
  evaluator resolves purely by metric name.
- **Result:** persisting ingest **182K → 296K rows/s (+63%)** at scale=1000,
  RSS still ~1.65 GB; gap to VM's 643K narrowed ~3.5× → ~2.2×. All query
  correctness preserved (1000-series fan-out; regex + `by(__name__)` intact).
- **Remaining headroom:** per-batch sample clone into shard buckets and
  influx parsing are the next ingest costs.

## Metric-name index — narrow the query candidate scan  ✅ DONE (2026-05-29)
**Was:** the evaluator resolved selectors by scanning *every* series and
running `matches_selector` on each — O(all series) per query.
- ✅ Added a `metric-name part → series-keys` index in `Storage` (maintained
  incrementally; rebuilt on open), exposed via `QueryStore::series_for_metric_name`
  + `distinct_metric_names` (Storage + fan-out for ShardedStorage).
- ✅ The `*_over_time` hot path now narrows candidates by the selector's
  `__name__` constraint (literal or `=~regex`) via the index, then applies
  `matches_selector`. Correctness preserved (only ever narrows).
- **Result (scale-1000, 10k series, med @ 8 workers):** single-groupby-1-1-1
  172→71 ms (**2.4×**), single-groupby-5-1-1 617→388 ms (**1.6×**). Broad
  aggregations (double-groupby across all hosts) are unaffected — now
  data-read-bound.
- **Next lever:** a full inverted **label** index (e.g. host) + per-series
  block-offset caching would speed the broad/data-bound aggregations.

## Range-query step reuse (per-query read cache)  ✅ DONE (2026-05-29)
**Was:** `evaluate_range` re-evaluated the expression at every step, so each
series was re-searched (and its parts re-opened) once *per step* — ~72×
redundant for a 3-day/1h `query_range`.
- ✅ Added `RangeCache`, a per-query `QueryStore` wrapper: the first
  `search_by_metric_name` for a series reads the whole `[start−maxlookback, end]`
  window once and memoizes it; every step's sub-window is served from that
  buffer. Metadata lookups delegate. Preload bound derived from the expression's
  largest `[range]`.
- **Result (scale-1000, 10k series, med @ 8 workers):** double-groupby-1
  3514→**911 ms (3.9×)**; single-groupby-1-1-1 71→54 ms; single-groupby-5-1-1
  388→278 ms. Correctness preserved (1000 host-groups; range tests green).
- **Tradeoff:** a broad range query buffers its touched series in memory.

## RangeCache memory cap  ✅ DONE (2026-05-29)
- ✅ `RangeCache` now caps memoized data at 16M samples (~256 MB); series
  beyond the cap bypass the cache and are served per-step. Correctness-neutral
  graceful degradation — bounded memory for pathologically broad range queries.

## Inverted label index  ✅ DONE (2026-05-29)
**Was:** after the name index, host-filtered selectors still scanned every
series of that metric (e.g. 1000 `cpu_usage_user` series to find 1 host).
- ✅ Added a `(label_name, label_value) → series-keys` index in `Storage`
  (incremental + rebuilt on open), exposed via `QueryStore::series_for_label`
  (ShardedStorage fans out). `candidate_series` now **intersects** the
  name-posting with each equality-label posting (smallest-first), so
  host-anchored queries resolve to ~the matching series directly.
  Correctness-preserving (each posting is a superset; `matches_selector` still
  applies). Empty-value `=""` matchers are not anchored (can't index absence).
- **Result (scale-1000, med @ 8 workers):** single-groupby-1-1-1
  **54→12 ms (4.5×)**, single-groupby-5-1-1 **278→63 ms (4.4×)**.
  double-groupby (no equality host filter) unchanged.
- **Cumulative from session start:** single-groupby-1-1-1 **172→12 ms (14×)**,
  single-groupby-5-1-1 **617→63 ms (10×)**, double-groupby-1 **3514→942 ms (3.7×)**.

## Final gate
Re-run the full TSBS suite at scale=1000 and regenerate
[`tsbs-comparison.md`](./tsbs-comparison.md) with after-numbers.

**Sequencing:** P1 first (unblocks honest large-scale measurement), then P2
(correctness), then P3/P4 (latency+concurrency), then P5. P2 and P3 are
largely independent and can proceed in parallel.
