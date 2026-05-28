# Performance documentation

Benchmark methodology, baseline numbers, and regression history. EsMetrics has
a hard performance-parity requirement vs VictoriaMetrics v1.144.0; this is
where we keep the receipts.

Planned files (populated during Phase 0.5–0.6 onward):
- `methodology.md` — how parity benchmarks are run; hardware pinning; reproducibility.
- `baselines/` — JSON files capturing baseline measurements per platform per commit.
- `vs-upstream.md` — head-to-head results vs VictoriaMetrics v1.144.0, updated nightly.
- `regression-log.md` — when, where, and why perf regressed; how it was fixed.
