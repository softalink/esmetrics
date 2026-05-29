# Roadmap: EsMetrics → surpass VictoriaMetrics v1.144.0

Goal: every TSBS benchmark row shows EsMetrics ≤ VM. Honest bar: ingest, RAM,
disk, and wide-range queries are realistic to surpass; big aggregations are a
real fight (VM is parallel too); VM's sub-millisecond trivial queries are the
hardest (fixed-overhead floor) — parity likely, surpass uncertain.

Baseline (scale-1000, median @ 8 workers): see `tsbs-comparison.md`.

## Phase 1 — Ingest: pass 645K rows/s
Root cause: per-sample allocation + parse + map-insert overhead.
- 1.1 Zero-copy Influx line parsing (borrowed slices, no per-field String).
- 1.2 Series interning: metric-name → u64 id once; fast hasher.
- 1.3 Remove per-sample clone in `ShardedStorage::ingest` (route by index).
- 1.4 Replace per-sample `BTreeMap` pending insert with columnar append.
- Gate: ingest > 645K rows/s, RAM ≤ VM.

## Phase 2 — Single-pass range evaluation
Root cause: `evaluate_range` re-runs the expression per step.
- 2.1 Bucket each series' samples into all step-windows in one pass.
- 2.2 Resolve candidates + parse labels once per query, not per step.
- Gate: `*-12`/range queries drop ~step-count×.

## Phase 3 — Parallel + vectorized query execution
Root cause: one query = one core, point-by-point.
- 3.1 Parallel per-series / per-shard execution (rayon).
- 3.2 Vectorized rollups over decoded blocks.
- 3.3 Block-level summaries (min/max/sum/count) at flush to skip decode.
- Gate: double-groupby-all → < VM's 0.68 s.

## Phase 4 — Lean hot path for trivial queries
Root cause: VM answers single-series in ~0.65 ms (fixed overhead).
- 4.1 Parsed-query (AST) cache.
- 4.2 Trim hot path (label re-parse, buffer sizing, JSON, lock scope).
- 4.3 Optional rollup-result cache.
- Gate: single-groupby median < VM. (Hardest; parity likely.)

## Phase 5 — Hold leads (RAM, disk) + columnar storage
- 5.1 Keep memory bounded as parallelism grows.
- 5.2 Match/beat VM per-type encodings (Gorilla floats, delta-of-delta ts).
- 5.3 mmap'd parts + block cache.

## Process
Commit/push to `main` after each phase (sole-maintainer, no PR), keeping
`fmt`/`clippy -D warnings`/tests green. Re-run the TSBS comparison after each
phase; update `tsbs-comparison.md`. Done when every row is EsMetrics ≤ VM.

## Status (2026-05-29, after running the roadmap)
- Phase 1 (ingest): 292K → 341K rows/s (+17%). Gate (>645K) NOT met — residual
  cost is allocation churn; needs a zero-copy parser (deferred, larger change).
- Phase 2 (range eval): binary-search sub-window + memoized metadata.
  single-groupby-5-1-12 767→554ms, etc. Single-pass rewrite folded into Phase 3.
- Phase 3 (parallel): parallel cache warm-up (reads). double-groupby-1
  940→605ms, cpu-max-all-8 191→120ms. All-host aggregations remain serial-
  aggregation-bound; parallel aggregation deferred (high-risk rewrite).
- Phase 4 (lean hot path): single-anchor micro-opt. Gate (sub-ms single-groupby)
  NOT met — needs the single-pass fast-path; flagged "uncertain to surpass".
- Phase 5 (RAM/disk): **GATE MET** — EsMetrics ahead on RAM (1.68 vs 2.06 GB)
  and disk (86 vs 122 MB). Columnar/mmap left as optional enhancement (lead
  already held).

**Outcome:** surpasses VM on RAM + disk; parity on correctness + capability;
still behind on ingest (1.87×) and query latency (10–234×). The two remaining
gaps need large, carefully-validated engine rewrites (zero-copy parser;
single-pass parallel-aggregation evaluator + columnar storage), intentionally
not rushed in autonomous mode to preserve the 11/11 correctness parity.
