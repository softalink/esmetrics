# Upstream sync process and log

EsMetrics is a semantic port, not a fork: upstream changes are adopted by
re-porting them, guided by the tooling below. The current baselines live in
[`UPSTREAM`](../UPSTREAM) at the repo root.

## Versioning

EsMetrics follows the upstream semver: port version `1.146.x` corresponds to
upstream VictoriaMetrics `v1.146.0` (the workspace `version` in `Cargo.toml`).
A sync to upstream `v1.NNN.0` bumps the port to `1.NNN.0`; port-only fixes
between syncs bump the patch level.

## Per-release sync loop

1. **Generate the triage report**
   ```
   scripts/upstream-diff.sh [vX.Y.Z]        # defaults to latest release tag
   ```
   This fetches upstream into `.upstream/` (git-ignored), diffs the ported
   scope (`VM_SCOPE` in `UPSTREAM`) since the recorded baseline, and writes
   `docs/sync/<old>..<new>.md` with each changed Go file mapped to the Rust
   files that port it (via `scripts/upstream-map.py`, which reads the
   provenance citations embedded in every ported module).
2. **Triage each changed file** into one of:
   - **adopt** — bug/correctness fix in ported code: port it, test first;
   - **adapt** — performance or structural change: evaluate against our
     implementation (which may already differ or be faster) and port the
     idea if it wins on our benchmarks;
   - **skip** — out of ported scope, or superseded by our design; record why.
3. **Tests first**: when the upstream change carries `*_test.go` changes,
   port the test before the fix and watch it fail.
4. **Behavioral tripwire**: run the differential harness against the *new*
   upstream release binary — byte-identical responses expected on the TSBS
   replay set; divergence means upstream changed observable behavior and the
   triage list knows where to look.
5. **Benchmark gate**: `benchmarks/bench.sh` (Linux) and
   `benchmarks/bench-windows-native.ps1` (Windows) back-to-back against the
   new upstream release; the port must keep winning all metrics.
6. **Close out**: record decisions in the log below, bump the version to the
   upstream semver, update `UPSTREAM` (tags + commits), and commit the
   report from `docs/sync/`.

## Cadence

Default to syncing on upstream **LTS releases** (lowest churn; correctness
fixes get backported there). Batch latest-line syncs quarterly if needed.

## Sync log

| date | upstream | port version | adopted | adapted | skipped | notes |
|---|---|---|---|---|---|---|
| 2026-07-03 | VM v1.146.0 + metricsql v0.87.1 | 1.146.0 | — | — | — | initial port baseline |
| 2026-07-03 | metricsql v0.87.2 | 1.146.0 | 1 | — | — | drop bogus `range` transform-func entry (funcs.rs) |
