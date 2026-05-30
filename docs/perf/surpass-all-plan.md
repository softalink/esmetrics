# Plan: surpass VictoriaMetrics on *every* TSBS metric

## Where we are (after the ingest + query + compaction work)

EsMetrics is **ahead of VM on ingest (752K vs 648K rows/s), peak RAM (1.4 vs
2.2 GB), and disk (92 vs 118 MB)**, faster on 3 query types, at parity on a 4th.
The holdouts are the read-heavy aggregations:

| query | ratio vs VM | shape |
|---|---|---|
| double-groupby-all/-5/-1 | **2.2–2.4×** | rollup over 1k–10k series, no selective filter |
| single-groupby-5-8-1 / -1-8-1 | 1.8–2.7× | 8–40 series, sub-5 ms (fixed overhead) |
| cpu-max-all-8 | 2.1× | 80 series, 8 h window |

## Research: how VM executes these (verified against VM master source)

1. **Per-part streaming merge-join scan.** VM resolves tag filters to *one
   sorted `[]TSID`* via indexdb, then scans **each part once**, binary-searching
   its metaindex to merge-join the sorted TSID set against the sorted on-disk
   blocks (`part_search.go`, `table_search.go` heap-merge across parts). Blocks
   come out already grouped by series. It **never looks a series up
   individually.** (`Search.NextMetricBlock`.)
2. **Decode is scalar Go — no SIMD** (`encoding_amd64.s` is empty). Delta /
   delta-of-delta + ZSTD over float→scaled-int64.
3. **Parallelism:** the scan is single-threaded; decode+rollup is parallel
   per-series across ~`min(cores,32)` workers; concurrent queries capped at
   `2×cores≤16`.
4. **Caches** (rollupResult, indexdb) mostly **miss** on TSBS's random windows —
   TSBS measures the cold scan+decode+rollup path.
5. VM stores per-block first-value + min/max **timestamp**, but **not** per-block
   value min/max.

## Why we're slower — and it's not what I first assumed

Our evaluator looks each candidate series up **individually**
(`search_by_metric_name` per series), so for a wide query it **re-opens the same
part files once per series**. Measured (`read_path_split.rs`): the per-series
open+seek is ~9.5 µs/series — ~8 % of read with one part, but ~30 %+ with the
~4 parts/shard a real load leaves. The decode itself is fast (314M samples/s
isolated) and comparable to VM's scalar decode.

**Direct measurement of the lever** (`scan_compare.rs`, 1000 series, 6
time-disjoint parts, 12 h window — the double-groupby shape):

| read strategy | time/iter |
|---|---|
| per-series (current) | 78.2 ms |
| **per-part scan (VM-style)** | **36.7 ms** |
| **speedup** | **2.13×** |

A per-part scan is **2.13× faster** on the exact workload where we trail VM by
2.2×. This is the lever.

## The plan

### Lever 1 — per-part streaming scan for wide queries (primary; ~2× proven)

Add a scan-oriented read path the evaluator uses when a query touches a large
fraction of a shard's series (double-groupby, cpu-max-all):

- Resolve candidate TSIDs once (already done by `candidate_series` → look up
  each name's TSID into a sorted set per shard).
- Per shard, per overlapping part (pruned by `parts_index` time range), open the
  part **once** and stream blocks via the cached metaindex (`open_with_index` +
  `seek_to_tsid` to skip to the first candidate), merge-joining the sorted TSID
  set against the sorted blocks; collect in-window samples per series.
- Parallelize across (shard × part) — with ~140 parts there's ample work for the
  thread pool. (The earlier "batched per-part read" was reverted at *7* parts,
  where part-count parallelism starved; that constraint is gone.)
- Keep the per-series path for **selective** queries (a few series), where
  scanning whole parts would read far more than needed. Choose by estimated
  candidate fraction (candidate count vs shard series count).

Expected: roughly halves the dominant read cost of the double-groupby trio →
from ~2.2–2.4× to ~1.1–1.2× VM, i.e. at/near parity, before Lever 2.

### Lever 2 — SIMD-vectorize the block decode (compounds; pulls ahead)

VM's decode is deliberately **scalar**. Once Lever 1 removes the per-series
overhead, decode becomes the dominant remaining read cost — so vectorizing it
(delta/delta-of-delta prefix-sum and the varint fast path via `std::simd` or
explicit SIMD intrinsics) makes our decode *faster than VM's*, turning near-parity
into a lead. Isolated decode is already 314M samples/s scalar; a 1.5–2× SIMD
decode is realistic for the prefix-sum-heavy path. This is decode-internal — no
on-disk format change, so VM byte-compat is preserved.

### Lever 3 — multi-host selector fixed overhead (smaller, separate)

The 8-host queries are selective (8–80 series) and sub-5 ms; their gap is fixed
per-query overhead, dominated by `candidate_series` fanning each label lookup
across all 32 shards. Lever: resolve label/name postings only on the shards that
can hold matching series, or short-circuit the intersection when one anchor is
already tiny. Lower priority (small absolute numbers) but needed for a clean
sweep.

### Not pursuing (measured dead ends)

- **mmap** (+0.5 %, page cache makes `read_exact` free), **zstd-context reuse**
  (+7 %), **block pre-aggregation / per-block value min/max** (rollup windows
  1 m–1 h ≪ the ~22.8 h blocks, so no window covers a whole block; would need a
  block-size change that breaks VM byte-compat and the ingest/disk wins).

## Sequencing, risk, effort

1. **Lever 1** (per-part scan) — biggest win, ~2× proven. Moderate–large: a new
   read path + the per-series-vs-scan chooser, reusing existing block iteration.
   Risk: correctness of the merge dedup across parts/pending (guard with the
   existing equivalence + dedup tests). **Do first.**
2. **Lever 2** (SIMD decode) — medium effort, isolated to the codec, low risk
   (exhaustive roundtrip tests already exist). **Do second; compounds Lever 1.**
3. **Lever 3** (selector fan-out) — small, for the 8-host clean-up.

After 1+2, the double-groupby trio should move from ~2.2× behind to a lead, and
with 3 the multi-host selectors too — putting EsMetrics **ahead of VM on every
TSBS metric**, while keeping the won ingest/RAM/disk advantages and VM
byte-compatibility (no on-disk format change in any lever).

Evidence: `crates/esm-storage/tests/scan_compare.rs` (Lever 1, 2.13×),
`crates/esm-compress/tests/decode_split.rs` (Lever 2 headroom),
`crates/esm-storage/tests/read_path_split.rs` (per-series overhead breakdown).
