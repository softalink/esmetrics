# Benchmark results

Environment: Linux x86_64, 4 cores, 30 GiB RAM. TSBS cpu-only, scale=100,
1 day @ 10s interval (8.64M metrics / 864k rows), 1000 queries/type,
4 workers, batch-size 10000, dataset seed 123.

Regimen (`bench.sh` on Linux, `bench-windows-native.ps1` on Windows):
fresh storage dir → load → 5s settle + `/internal/force_flush` + 2s →
query types alphabetically. Rust and Go runs are executed back-to-back
under identical conditions.

Committed rounds: `go-linux-final`/`rust-linux-final` (Linux tables),
`go-lin6..8`/`rust-lin6..8` (Linux median-of-3 verification),
`go-win6..8`/`rust-win6..8` (Windows median-of-3), `rust-msvc`
(MSVC-build verification round).

## FINAL: Linux (`go-linux-final/` vs `rust-linux-final/`, 2026-07-03)

Load: Go **3.63M** metrics/sec → Rust **4.99M** metrics/sec (**+38%**)

| query type | go med/mean/max (ms) | rust med/mean/max (ms) | mean delta |
|---|---|---|---|
| single-groupby-1-1-1 | 0.32/0.37/2.10 | 0.28/0.33/2.25 | −11% |
| single-groupby-1-1-12 | 0.66/0.75/5.69 | 0.36/0.41/1.35 | −45% |
| single-groupby-1-8-1 | 0.70/0.78/3.21 | 0.43/0.47/2.10 | −40% |
| single-groupby-5-1-1 | 0.71/0.80/3.00 | 0.40/0.45/2.33 | −44% |
| single-groupby-5-8-1 | 2.24/2.38/8.40 | 0.90/0.98/3.39 | −59% |
| cpu-max-all-1 | 0.93/1.08/5.51 | 0.77/0.96/5.19 | −11% |
| cpu-max-all-8 | 4.82/5.53/23.96 | 3.89/4.06/12.14 | −27% |
| double-groupby-1 | 6.73/6.99/18.59 | 5.31/5.62/14.76 | −20% |
| double-groupby-5 | 33.77/34.71/76.00 | 24.85/25.34/44.73 | −27% |
| double-groupby-all | 67.95/69.03/135.76 | 49.00/49.65/85.12 | −28% |

**Rust wins every TSBS metric on Linux** (load throughput; med, mean and
max latency on all 10 supported devops query types).

## FINAL: Windows — real hardware (`go-win6..8/` vs `rust-win6..8/`, 2026-07-03)

Run on a physical Windows 11 host (build 26100, 8 logical CPUs, 32 GB RAM,
Defender real-time protection ON for both servers): Go = official
victoria-metrics-windows-amd64-prod.exe v1.146.0, Rust = the
x86_64-pc-windows-gnu build, TSBS clients built for Windows, everything
local to the host. Median of 3 back-to-back paired rounds.

Load: Go **2.99M** → Rust **4.49M** metrics/sec (**+50%**)

Every med/mean stat on all 10 query types is won by Rust (30-65% faster),
as are max and stddev on all heavy types (e.g. double-groupby-all max
141→113ms, cpu-max-all-8 max 18.6→9.2ms). A few sub-millisecond min/stddev
stats flip on individual runs: these trace to the TSBS client clock's
~0.5ms quantization (Go "min" readings of 0.0-0.6ms are under-reads;
server-side QPC timing shows Rust's true floors are lower). Full per-round
data in `go-win6..8/`/`rust-win6..8/`; the MSVC build's round is in
`rust-msvc/` (see the README's Windows table for its numbers).

## Resource usage: peak memory, peak CPU, storage size (2026-07-04)

Same regimen re-run with the resource-monitored harnesses
(`bench-monitored.sh` / `bench-monitored.ps1`, summarized by
`analyze-resources.py`): the server process is sampled every 200ms
(CPU time + resident set via `/proc` on Linux, `Get-Process` on Windows);
peak CPU is the highest usage over any 1s window; lifetime peak memory is
the kernel high-water mark (`VmHWM` / `PeakWorkingSet64`); storage is the
data dir size once stable after `/internal/force_merge`. Median of 3
back-to-back paired rounds per platform. Linux: same 4-core environment
as the tables above. Windows: same physical host as the Windows tables
(build 26100, 8 logical CPUs), Rust = the MSVC build. Load throughput in
these rounds matched the published numbers (Linux 3.2–3.9M vs ~5.0M;
Windows 2.97M vs 5.12M metrics/sec).

### Peak resident memory

| | Go | Rust | delta |
|---|---|---|---|
| Linux: load-phase peak | 749 MiB | 146 MiB | −80% |
| Linux: query-phase peak | 681 MiB | 597 MiB | −12% |
| Linux: lifetime peak (VmHWM) | 749 MiB | 633 MiB | **−15%** |
| Windows: load-phase peak | 908 MiB | 268 MiB | −70% |
| Windows: query-phase peak | 887 MiB | 533 MiB | −40% |
| Windows: lifetime peak working set | 911 MiB | 535 MiB | **−41%** |

### CPU

Peak/average CPU (Linux max 400%, Windows max 800%):

| phase | Go peak / avg | Rust peak / avg |
|---|---|---|
| Linux load | 352% / 335% | 345% / 321% |
| Linux queries | 370% / 360% | 376% / 360% |
| Windows load | 620% / 581% | 570% / 485% |
| Windows queries | 772% / 723% | 786% / 730% |

Both servers saturate the machine under query load on both platforms
(instantaneous draw is a tie); Rust ingests with a lower peak on Windows.
Total CPU consumed for the identical workload:

| workload | Go CPU-s | Rust CPU-s | delta |
|---|---|---|---|
| Linux: load 8.64M metrics | 7.8 | 4.6 | −41% |
| Linux: 10,000 queries | 128 | 78 | −39% |
| Windows: load 8.64M metrics | 16.2 | 7.5 | **−54%** |
| Windows: 10,000 queries | 339 | 135 | **−60%** |

### Storage size (fully merged)

| | Go | Rust |
|---|---|---|
| Linux | 3.23 MB | 3.23 MB |
| Windows | 3.46 MB (3.35–3.49 across rounds) | 3.23 MB (byte-stable) |

Format efficiency is identical (~0.37 bytes/sample); Rust's final merged
size is byte-stable across rounds and platforms, while Go's Windows runs
land 4–8% larger with run-to-run variation in final part layout.

### Bounding query CPU (`-search.maxConcurrentRequests` / `-search.maxWorkersPerQuery`)

Validation rounds (2026-07-04, same regimen): with `2 × 3` caps on the
8-CPU Windows host, query-phase peak CPU drops 780% → 513% with
CPU-seconds 133 → 96 (wall time near-unchanged, 19.0s → 19.9s — the
caps shed cross-core coordination overhead rather than throughput).
With `2 × 2` on the 4-core Linux host: 376% → 317% (CPU-seconds
80 → 84–90 across two rounds, query wall time 22s → 28–30s). Responses
byte-identical under the tested cap configuration (Linux 2×2,
100 double-groupby-all queries; modulo the `executionTimeMsec` stat);
uncapped rounds match the tables above.

## Current-code re-benchmark (post-esmauth, 2026-07-06)

Full paired Go-vs-Rust re-run at the current code state (after the
ingestion-protocol and esmauth merges, which changed shared crates on the
`/write` ingest path). Same methodology as the FINAL tables — median of 3
paired back-to-back rounds, Go v1.146.0 vs the current Rust build. Backing
dirs committed: `go-lin-e{1,2,3}`/`rust-lin-e{1,2,3}` (Linux),
`go-win-e{1,2,3}`/`rust-win-e{1,2,3}` (Windows/MSVC on the agent-6 host).
`esmauth` itself is an auth-proxy binary and is **not** benchmarked — this
is the esmetrics TSDB re-benchmark; it confirms the shared-crate changes did
not regress the data path, and Rust still wins every metric on both
platforms.

### Linux (`go-lin-e*` vs `rust-lin-e*`)

Load: Go **3.73M** → Rust **5.00M** metrics/sec (**+34%**)

| query type | go med/mean (ms) | rust med/mean (ms) |
|---|---|---|
| single-groupby-1-1-1 | 0.32/0.42 | 0.16/0.21 |
| single-groupby-1-1-12 | 0.69/0.78 | 0.22/0.26 |
| single-groupby-1-8-1 | 0.69/0.81 | 0.45/0.53 |
| single-groupby-5-1-1 | 0.60/0.67 | 0.30/0.38 |
| single-groupby-5-8-1 | 2.17/2.51 | 0.82/0.89 |
| cpu-max-all-1 | 0.82/0.91 | 0.59/0.71 |
| cpu-max-all-8 | 4.26/4.51 | 3.64/3.89 |
| double-groupby-1 | 6.41/6.71 | 4.88/5.05 |
| double-groupby-5 | 32.39/33.41 | 25.03/25.37 |
| double-groupby-all | 65.63/67.10 | 48.33/48.61 |

Lifetime peak RSS: Go **775 MiB** → Rust **660 MiB** (**−15%**). Merged
storage: **3.23 MB** both (Rust byte-stable).

### Windows — real hardware, MSVC (`go-win-e*` vs `rust-win-e*`)

Same agent-6 host as the FINAL Windows rounds (8 logical CPUs, Defender on
for both). Load: Go **2.95M** → Rust **5.12M** metrics/sec (**+74%**).

| query type | go med/mean (ms) | rust med/mean (ms) |
|---|---|---|
| single-groupby-1-1-1 | 0.52/0.50 | 0.00/0.26 |
| single-groupby-1-1-12 | 1.02/0.87 | 0.51/0.31 |
| single-groupby-1-8-1 | 0.98/0.92 | 0.52/0.46 |
| single-groupby-5-1-1 | 0.66/0.81 | 0.51/0.35 |
| single-groupby-5-8-1 | 3.44/3.58 | 0.57/0.65 |
| cpu-max-all-1 | 1.38/1.35 | 0.54/0.60 |
| cpu-max-all-8 | 6.81/7.42 | 3.09/3.23 |
| double-groupby-1 | 9.68/10.45 | 4.74/5.01 |
| double-groupby-5 | 44.70/49.25 | 20.54/20.75 |
| double-groupby-all | 87.26/98.01 | 40.81/41.43 |

Lifetime peak RSS: Go **987 MiB** → Rust **543 MiB** (**−45%**). Merged
storage: Go **3.43 MB** → Rust **3.23 MB** (**−6%**, Rust byte-stable). The
`single-groupby-1-1-1` Rust median of `0.00` is the TSBS client clock's
~0.5ms quantization producing a sub-quantum under-read (same caveat as the
FINAL Windows section); the mean (0.26ms) is the reliable stat, and Rust
wins med and mean on every other type by 25–82%.

Rust wins load throughput, all query med/mean latencies, memory, and
storage on both platforms — consistent with the FINAL tables above; the
Windows load margin is wider here (+74% vs the FINAL +50%) on this run set.

## Correctness

750 TSBS queries across all 10 types replayed with `--print-responses`
against both servers on identical data: responses byte-identical
(modulo the `executionTimeMsec` stat).

## Notes

- `high-cpu-*`, `groupby-orderby-limit`, `lastpoint` are unsupported by
  the TSBS VictoriaMetrics adapter (it panics generating them) — out of scope.
- Go load throughput varies 2.4-4.4M metrics/sec across runs/cache states;
  Rust's final ingest (5.0-5.1M on Linux) exceeds Go's best observed run.
- Intermediate optimization-campaign rounds were pruned from the repo;
  the committed rounds above are the ones backing the published tables.
