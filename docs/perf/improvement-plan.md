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

## P2 — Inverted label index  *(fixes 7/11 query correctness failures)*
**Fixes:** `{__name__=~regex}` → empty, bare `{label=...}` → partial.
- Build a label→postings (TSID set) index so selectors resolve by arbitrary
  matchers, not just exact metric-name lookup. Support `=`, `!=`, `=~`, `!~`
  including on `__name__`.
- **Verify:** all 11 TSBS query types return series counts matching VM;
  re-run the value-correctness comparison → parity.

## P3 — Query read-path efficiency  *(fixes 13–672× latency)*
**Fixes:** `search_by_tsid` re-`read_dir`s + re-opens every part per call.
- Cache open part handles + part headers; keep a per-part TSID→block offset
  map in memory; prune parts by time range without opening them.
- **Verify:** single-groupby latencies within a small multiple of VM (not 100×).

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

## P5 — Ingest throughput  *(close the 1.8× gap)*
- Profile the append path (TSID assignment, per-sample `BTreeMap` overhead);
  largely unblocked once P1/P4 land.
- **Verify:** ingest rows/s within ~1.2× of VM at scale=1000.

## Final gate
Re-run the full TSBS suite at scale=1000 and regenerate
[`tsbs-comparison.md`](./tsbs-comparison.md) with after-numbers.

**Sequencing:** P1 first (unblocks honest large-scale measurement), then P2
(correctness), then P3/P4 (latency+concurrency), then P5. P2 and P3 are
largely independent and can proceed in parallel.
