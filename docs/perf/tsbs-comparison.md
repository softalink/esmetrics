# TSBS benchmark: EsMetrics vs VictoriaMetrics v1.144.0

**Date:** 2026-05-29 · **Tool:** [TSBS](https://github.com/timescale/tsbs) ·
**Workload:** `cpu-only` (devops), 10 fields/row, 10s interval ·
**Host:** 22 cores, 62 GB RAM, NVMe.

Both databases were driven by the **same** TSBS VictoriaMetrics driver
(EsMetrics is wire-compatible): InfluxDB line protocol to `/write` for
ingest, `GET /api/v1/query_range` for queries. VM ran in Docker
(`v1.144.0`) with `-retentionPeriod=100y` so it accepts the dataset
(default 1-month retention silently rejects the 2024-dated data — a trap
that inflates VM's apparent ingest rate and empties its query results if
not corrected).

## Current standings — after the perf/correctness work (2026-05-29)

The improvements in [`improvement-plan.md`](./improvement-plan.md) (incremental
flush, zstd-level fix, size-tiered compaction, RwLock concurrency, regex +
`by(__name__)` correctness, metric-name + label indexes, range-query cache,
sharded ingest) changed the picture qualitatively. EsMetrics now runs the
**full scale-1000 (10k-series) benchmark** — which was *impossible* in the
original run (the flush blocker forced a drop to scale-10).

**Capability + correctness:**

| Dimension | VictoriaMetrics | EsMetrics (now) | EsMetrics (original) |
|---|---|---|---|
| Runs full scale-1000 | yes | **yes** | no (flush blocker → scale-10) |
| Query correctness | 11/11 | **11/11** ✅ | 4/11 |
| Ingest (persisting) | 645K rows/s | **292K rows/s** (2.2×) | didn't persist |
| Ingest peak RAM | 2.09 GB | **1.67 GB** (less) | 5.5 GB |
| Query concurrency | scales | scales (RwLock) | flat |

**Query latency** (scale-1000, 10k series, median @ 8 workers; all 11 types
now return series counts matching VM):

| query type | VM | EsMetrics | ratio |
|---|---|---|---|
| single-groupby-1-1-1 | 0.65 ms | 14.7 ms | 23× |
| single-groupby-1-1-12 | 1.00 ms | 149 ms | 149× |
| single-groupby-1-8-1 | 1.06 ms | 75.9 ms | 72× |
| single-groupby-5-1-1 | 1.07 ms | 70.2 ms | 66× |
| single-groupby-5-1-12 | 2.28 ms | 767 ms | 336× |
| single-groupby-5-8-1 | 1.84 ms | 390 ms | 211× |
| double-groupby-1 | 61 ms | 940 ms | 15× |
| double-groupby-5 | 356 ms | 9.0 s | 25× |
| double-groupby-all | 681 ms | 23.7 s | 35× |
| cpu-max-all-1 | 1.16 ms | 38 ms | 33× |
| cpu-max-all-8 | 4.18 ms | 191 ms | 46× |

**Read of the results:** correctness and capability are now at parity (11/11,
full scale, concurrent, persists, lower RAM); ingest is within ~2.2×. **Query
latency remains 15–336× behind VM** — VM has a mature, columnar, deeply
optimized query engine (block-level indexes, vectorized rollups, parallel
per-series execution). EsMetrics' generic per-step evaluator, while now
correct and far faster than it was, is the dominant remaining gap. The worst
cases are wide time-range (`*-12`, 12 h) and all-host aggregations
(`double-groupby-all`), which are data-volume/compute bound — the next levers
are vectorized rollup evaluation and parallelizing query execution across
series/shards.

---

## TL;DR (original run — historical baseline)

The first run (below) found EsMetrics behind on **every** dimension; it is
kept for context. The deltas above show what the improvement work changed.

| Dimension | VictoriaMetrics | EsMetrics | Gap |
|---|---|---|---|
| Ingest throughput | ~643K rows/s | ~354K rows/s | **1.8× slower** |
| Ingest peak RAM | 2.3 GB | 5.5 GB | **2.4× more** |
| Persistence at scale | streams to disk | **cannot** (see flush) | blocker |
| Query correctness | 11/11 types correct | **4/11 correct** | 7 wrong |
| Query latency (correct types) | baseline | **13–672× slower** | large |
| Query concurrency scaling | ~10× (1→8 workers) | **~1× (flat)** | no scaling |

---

## 1. Ingest (load) — scale=1000 hosts / 3 days = 25.92M rows / 259M points

3 iterations, fresh state each, 8 loader workers, batch 10000.

| | rows/sec (median) | peak RSS | on-disk after load |
|---|---|---|---|
| VictoriaMetrics | **642,255** | 2.31 GB | 112 MB |
| EsMetrics | **354,085** | 5.51 GB | none (all in RAM) |

EsMetrics ingests ~55% as fast and holds the entire dataset in memory
(5.5 GB) because it has no incremental flush — see §3.

## 2. Query latency & correctness — scale=10 hosts / 3 days

Reduced to scale=10 because EsMetrics cannot flush larger sets in
reasonable time (§3). **Identical data on both sides**, so the comparison
is fair. 11 benchmarkable query types (TSBS's VM target declares
`high-cpu`, `lastpoint`, `groupby-orderby-limit` "not supported in PromQL"
— excluded for both). 400 queries × 3 iters, median latency @ 8 workers:

| query type | VM med | ESM med | ESM/VM | correct? |
|---|---|---|---|---|
| single-groupby-1-1-1 | 0.56 ms | 52.7 ms | 94× | ✅ |
| single-groupby-1-1-12 | 0.91 ms | 611.6 ms | **672×** | ✅ |
| single-groupby-1-8-1 | 0.39 ms | 5.1 ms | 13× | ✅ |
| double-groupby-1 | 1.57 ms | 102.7 ms | 65× | ✅ |
| single-groupby-5-1-1 | 0.99 ms | 24.6 ms | — | ❌ empty |
| single-groupby-5-1-12 | 2.20 ms | 288.3 ms | — | ❌ empty |
| single-groupby-5-8-1 | 0.48 ms | 24.3 ms | — | ❌ empty |
| double-groupby-5 | 5.93 ms | 5.39 ms | — | ❌ empty |
| double-groupby-all | 10.83 ms | 5.44 ms | — | ❌ empty |
| cpu-max-all-1 | 1.65 ms | 4.26 ms | — | ❌ partial |
| cpu-max-all-8 | 7.97 ms | 4.11 ms | — | ❌ partial |

⚠️ For the ❌ rows EsMetrics' latency looks competitive **only because it
returns empty/partial results** — it is doing less work, not faster work.
The valid latency comparison is the 4 ✅ rows: **13–672× slower**.

Concurrency: VM qps scales ~10× from 1→8 workers; EsMetrics stays flat
(~1300→~1500) — it does not scale with load.

---

## 3. Root causes (code-level)

**Storage engine is MVP-level** (`crates/esm-storage/src/storage.rs` — its
own comments say incremental flush / indexdb are deferred to a later phase):

1. **No incremental/background flush.** All samples buffer in an in-memory
   `BTreeMap<Tsid, Vec<(ts,value)>>` (`pending`) and persist only on explicit
   `flush()`/`shutdown()`. → 5.5 GB RAM for the full set; nothing on disk.
2. **Flush is severely superlinear in series length.** Measured (one-shot
   flush): 100 series × 25,920 pts = 66 s; scale=100 didn't finish in 120 s;
   the full 1000-host set never flushed in 46 min. This is the hard cap that
   forced query tests down to scale=10.
3. **Queries can't see unflushed data** — `search` reads on-disk parts only,
   never the in-memory buffer. Data is invisible until (slowly) flushed.
4. **No inverted label index.** Storage indexes only `name_to_tsid`
   (exact metric-name key → TSID). Selectors resolve by exact name, so:
   - `{__name__=~'cpu_(...)'}` (regex on name) → **0 series** (single-groupby-5-\*, double-groupby-5/-all).
   - `{hostname='host_0'}` (bare, no metric name) → **1 of 10 series** (cpu-max-all-\*).
5. **`search_by_tsid` re-`read_dir`s and re-opens every part directory on
   every call** (no part/block cache) → O(series × parts) I/O per query; the
   12-hour-range query (single-groupby-1-1-12) hits 612 ms.
6. **Global `Arc<Mutex<Storage>>`** in `apps/esm-single` serializes all
   ingest + query work → zero concurrency scaling.

## 4. Methodology notes / artifacts
- Bench harness + scripts: `../tsbs-bench/` (not committed). Raw results:
  `../tsbs-bench/results2/` (load), `../tsbs-bench/qresults/` (query).
- A genuine VM parity fix was made along the way: added the standard
  `/api/v1/query_range` route to `esm-single` (it only had the non-standard
  `/api/v1/promql_range`).
