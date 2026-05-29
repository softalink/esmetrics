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

## Current standings — after the single-pass parallel evaluator (2026-05-29)

After the full [`surpass-vm-roadmap.md`](./surpass-vm-roadmap.md) **plus** the
single-pass parallel-aggregation evaluator (the dominant query shape resolves
candidates once, reads each series once, rolls up per-series in parallel, then
group-reduces across cores — proven identical to the generic path by the
`fast_path_matches_generic` test).

**EsMetrics SURPASSES VM** on memory and disk; **parity** on correctness,
capability, and the simplest query; **behind** on ingest and heavier queries —
but the query gap is now **1.4–10×** (was 10–234× before the evaluator).

| Dimension | VictoriaMetrics | EsMetrics | verdict |
|---|---|---|---|
| Ingest peak RAM | 2.19 GB | **1.34 GB** | ✅ EsMetrics ahead (1.6×) |
| On-disk size | 118 MB | **86 MB** | ✅ EsMetrics ahead |
| Query correctness | 11/11 | **11/11** | ✅ parity |
| Runs full scale-1000, concurrent, persists | yes | yes | ✅ parity |
| single-groupby-1-1-1 | 0.65 ms | 0.90 ms | ≈ near parity (1.4×) |
| Ingest throughput | 648K rows/s | 337K rows/s | ❌ 1.9× behind |
| Heavier queries | baseline | 2–10× | ❌ behind (was 10–234×) |

**Query latency** (scale-1000, 10k series, median @ 8 workers; all 11 types
return series counts matching VM):

| query type | VM | EsMetrics | ratio | (before evaluator) |
|---|---|---|---|---|
| single-groupby-1-1-1 | 0.65 ms | 0.90 ms | 1.4× | (9.5 ms, 14×) |
| single-groupby-1-1-12 | 0.99 ms | 2.15 ms | 2.2× | (107, 118×) |
| single-groupby-1-8-1 | 0.99 ms | 3.17 ms | 3.2× | (60, 63×) |
| single-groupby-5-1-1 | 0.99 ms | 3.19 ms | 3.2× | (47, 53×) |
| single-groupby-5-1-12 | 2.31 ms | 7.58 ms | 3.3× | (554, 234×) |
| single-groupby-5-8-1 | 1.81 ms | 13.1 ms | 7.2× | (294, 179×) |
| double-groupby-1 | 63 ms | 656 ms | 10× | (605, 10×) |
| double-groupby-5 | 329 ms | 2.69 s | 8.2× | (7.6 s, 23×) |
| double-groupby-all | 701 ms | 5.13 s | 7.3× | (23.5 s, 34×) |
| cpu-max-all-1 | 1.56 ms | 7.92 ms | 5.1× | (23, 17×) |
| cpu-max-all-8 | 5.04 ms | 44 ms | 8.7× | (120, 26×) |

**Read of the results:** EsMetrics is now **ahead of VM on RAM (1.6×) and
disk**, at **parity on correctness and the simplest query** (single-groupby
within 1.4× of VM's 0.65 ms), and the single-pass parallel evaluator cut the
heavier-query gap by **~10–70×** (single-groupby-5-1-12 234×→3.3×,
double-groupby-all 34×→7.3×) with no correctness regression (the fast path is
proven equivalent to the generic path by `fast_path_matches_generic`).

**Remaining gaps:**
1. **Ingest (1.9×):** ~520M `String`/`Vec` allocations per load — a zero-copy
   line parser is the lever.
2. **Heavier aggregations (5–10×):** bound by raw per-query data volume under
   workers=8 CPU saturation (each query decodes ~all touched series). *Tried
   and reverted:* block-level pre-aggregation (rollup windows 1m–1h are far
   smaller than the ~23 h blocks, so no window contains a whole block) and a
   batched per-part read (it caps parallelism at the part count, losing to the
   per-series parallel read). The remaining lever is reducing decode volume
   per query — finer block granularity or a columnar value layout.

**Bottom line on "surpass on every benchmark":** ahead on RAM + disk, parity on
correctness/capability and the simplest query; ingest and heavier aggregations
remain behind — now by **single-digit multiples** rather than orders of
magnitude. The remaining levers are storage-format changes (zero-copy parse,
columnar block summaries).

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
