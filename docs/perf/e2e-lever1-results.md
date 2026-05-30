# End-to-end TSBS: EsMetrics (Lever 1, `c5118e5`) vs VictoriaMetrics v1.144.0

Run 2026-05-30 on this 22-core / 62 GB host. Identical data both sides:
TSBS `cpu-only` scale-1000 (10k series, 3-day, 10s step = 25.92M rows /
259.2M metric points). EsMetrics = committed Lever-1 build `c5118e5`
(`esm-single`, auto-selected 32 shards + background compaction). VM =
`victoriametrics/victoria-metrics:v1.144.0` in a fresh container on :18431
(oracle on :18430 left untouched). TSBS loader/runner: `--workers=8`,
`--max-queries=200`. EsMetrics flushed before queries.

**Caveat:** a second Claude session + a qemu VM were consuming CPU during
this run (host load avg ~4). This depresses the CPU-bound EsMetrics ingest
more than VM. Numbers below are real but a quiet-host re-run is advised
before quoting ingest as final. See "Ingest discrepancy" below.

## Query latency (workers=8, 200 queries, median ms)

| query | VM med | ESM med | ESM/VM |
|-------|-------:|--------:|-------:|
| single-groupby-1-1-1   | 0.68 | 0.59 | **0.87** |
| single-groupby-1-1-12  | 0.90 | 0.90 | 1.00 |
| single-groupby-1-8-1   | 0.75 | 1.66 | 2.21 |
| single-groupby-5-1-1   | 0.86 | 0.96 | 1.12 |
| single-groupby-5-1-12  | 2.18 | 2.14 | **0.98** |
| single-groupby-5-8-1   | 1.77 | 4.83 | 2.73 |
| double-groupby-1       | 59.61 | 79.69 | 1.34 |
| double-groupby-5       | 348.37 | 394.48 | 1.13 |
| double-groupby-all     | 701.73 | 870.50 | 1.24 |
| cpu-max-all-1          | 1.69 | 2.34 | 1.38 |
| cpu-max-all-8          | 5.49 | 11.78 | 2.15 |

Lever 1's target — the **double-groupby trio** — is the closest EsMetrics
gets to VM on the heavy aggregations: **1.13–1.34×** (was ~2.2× before
Lever 1 per prior notes). The per-part-scan read path is confirmed 4.56×
faster than per-series in the `scan_compare` release microbench
(199.2ms → 43.7ms, identical output). Selective/8-host selectors
(single-groupby-*-8-1, cpu-max-all-8) remain 2–2.7× — Lever 3 territory,
untouched here.

## Resources

| metric | VM | ESM | winner |
|--------|---:|----:|:------:|
| ingest @8w (rows/s)  | 644,699 | 331,236 | VM 1.95× |
| ingest @16w (rows/s) | 655,027 | 470,735 | VM 1.39× |
| peak RSS @8w run     | 2.09 GB | 1.78 GB | **ESM** |
| peak RSS @16w run    | 3.47 GB | 2.07 GB | **ESM** |
| on-disk size         | ~121 MB | ~89 MB  | **ESM** |

ESM keeps its **disk** (~0.74×) and **peak-RSS** (~0.6–0.85×) leads. VM
wins **ingest throughput** in this run (see discrepancy note).

### Quiet-host ingest re-run (confirms the above)

Re-ran ingest at host load ~1.3 (vs ~4 originally), 3 iterations at 16
workers + a worker sweep. Results were tight and reproducible — the
contention caveat did **not** hide an ESM ingest win:

| workers | VM rows/s | ESM rows/s | ESM/VM |
|--------:|----------:|-----------:|-------:|
|  8 | 649,090 | 332,565 | 0.51× |
| 16 | 646,737 (mean of 3) | 473,353 (mean of 3) | 0.73× |
| 22 | 646,015 | 512,118 | 0.79× |
| 32 | 642,581 | 548,706 | 0.85× |

VM is flat at ~645K regardless of workers (saturates early); ESM scales
with workers (333K→549K from 8→32w) but **does not reach VM** even at
32w on this 22-core box. ESM peak RSS during ingest is *higher* than VM
at ≥16w (2.99–3.69 GB vs 2.3–2.4 GB) — the load-phase pending buffers;
ESM's RSS lead is a *query/steady-state* property, not an ingest-phase
one. ESM on-disk stays ~93 MB vs VM ~131 MB.

**Conclusion: the memory's "ingest 750K, surpasses VM" claim does not
reproduce.** Measured ESM ingest is 0.73× VM at the canonical 16w and
never exceeds 0.85× VM. This was contention-independent (confirmed at
load ~1.3). The 750K figure was most likely a stale-binary/measurement
artifact from the prior session.

## Correctness — ALL 11 QUERY TYPES NOW MATCH VM (after fixes)

All 11 TSBS query types are now **byte-identical to VM v1.144.0** on a
fresh scale-1000 load (compare_json: single-groupby ×6, double-groupby ×3,
cpu-max-all ×2 — every value point matches). This was 2/11 at first
measurement; two pre-existing PromQL-eval bugs (neither from Lever 1) were
found by diffing values and have since been fixed:

1. **Step-grid alignment (single-groupby ×6). ✅ FIXED** (VM
   `AdjustStartEnd` ported into the `promql_range` HTTP handler): range
   queries with ≥50 points now round start/end to step multiples exactly
   as VM does, so the evaluated grid matches. Was: ESM used the raw
   `start + k·step`, VM snapped to `ts % step == 0`.
2. **Range-window boundary (double-groupby ×3, + count/rate/absent). ✅
   FIXED** (`4319088` for `*_over_time`, plus the remaining sites): the
   range selector window is now left-open `(t-d, t]` (VM/Prometheus
   semantics) instead of closed `[t-d, t]`, dropping the one extra
   boundary sample that shifted `avg_over_time` by ~0.1%.
3. **cpu-max-all (2 types)** matched VM throughout (`max` is insensitive
   to either effect).

**Methodology note:** the earlier "11/11 correctness parity" claim only
counted *series-count* parity; it never compared sample values (the TSBS
VM driver's `--print-responses` prints timing, not bodies). Diffing actual
values surfaced these two latent eval bugs, both now fixed and verified by
direct value comparison vs VM (all 11 types MATCH). Note the step
alignment matches VM specifically; VM's alignment is a cache optimization
that Prometheus itself does not perform, so this is VM-parity by choice.

## Repro

Scripts in `../tsbs-bench/`: `run-e2e-lever1.sh` (8w full),
`run-e2e-followup.sh` (16w ingest), `run-correctness3.sh` /
`run-correctness5.sh` (value capture) + `compare_json.py` /
`classify_diff.py`. Oracle VM on :18430 is never touched.
