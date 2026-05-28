# Phase 0 — Foundation — Report

**Closed:** 2026-05-28
**Reference pin:** VictoriaMetrics v1.144.0
**Status:** All Phase 0 sub-tasks complete. Workspace builds and lints green
locally and cross-compiles for all three target families.

---

## What shipped

### Repository foundation (0.1)
- Renamed `victoria-metrics-claude/` → `esmetrics/`.
- Initialised Git on `main` (no commits per owner directive — see
  [progress-log](./progress-log.md)).
- Apache 2.0 `LICENSE`, `NOTICE` with VictoriaMetrics attribution and
  trademark disclaimer, `CREDITS.md` enumerating reference projects.
- Toolchain pins: `rust-toolchain.toml` (1.95.0 stable, all six CI targets),
  `rustfmt.toml`, `clippy.toml`, `deny.toml`, `.editorconfig`, `.gitignore`,
  `.cargo/config.toml` (with `xtask` alias).

### Cargo workspace + crate skeletons (0.2)
- Workspace root `Cargo.toml` declares 19 members:
  - 12 library crates: `esm-platform`, `esm-common`, `esm-compress`,
    `esm-storage`, `esm-promql`, `esm-protocols`, `esm-scrape`,
    `esm-discovery`, `esm-alerting`, `esm-net`, `esm-objstore`, `esm-vmui`.
  - 6 app skeletons: `esm-single`, `esm-agent`, `esm-alert`, `esm-auth`,
    `esm-backup`, `esm-ctl`.
  - `xtask` workspace tooling.
  - `conformance-harness` (added in 0.6).
- Workspace-wide lint suite (rust + clippy pedantic with curated allow-list).
- Release / dev / bench / release-with-debug profiles defined.

### `esm-platform` core abstractions (0.3)
- Implemented and tested on Linux:
  - `mmap::{Mmap, MmapMut}` — memmap2-backed, with documented Windows
    defer-unlink contract and SAFETY justifications per ADR-001 #10.
  - `durability::{fsync_file, fsync_dir}` — Unix fsync + dir-fsync; Windows
    file-only with documented no-op for dirs.
  - `atomic_rename::rename` — wraps `std::fs::rename` with documented
    cross-platform semantics.
  - `file_lock::FileLock` — `flock(2)` / `LockFileEx` via the `fs2` crate.
  - `signal::{wait_for_shutdown, wait_for_reload}` — Tokio signal handlers,
    typed `ShutdownSignal`, Unix SIGTERM/SIGINT/SIGHUP + Windows ctrl_c.
  - `paths::canonical_data_path` — non-resolving canonicalisation +
    Windows-forbidden-character validation.
  - `proc::set_open_file_limit` — `setrlimit(RLIMIT_NOFILE)` on Unix; no-op
    on Windows.
- 15 unit tests passing on Linux. Each module cross-compiles for
  `aarch64-apple-darwin` and `x86_64-pc-windows-msvc`.

### xtask tooling (0.4)
- `cargo xtask <subcommand>` driver with `fmt`, `lint`, `test`, `bench`,
  `perf record|compare`, `fixtures regenerate|push|pull` subcommands.
- `fmt`, `lint`, `test` dispatch to real cargo invocations; the remainder
  print a clear "not yet implemented" stub and exit successfully.
- Released through `.cargo/config.toml` alias.

### CI workflows (0.5)
- `.github/workflows/ci.yml` — `fmt`, `clippy`, `cargo-deny`, plus a test
  matrix over `linux-x86_64-gnu`, `macos-aarch64`, `macos-x86_64`,
  `windows-x86_64-msvc`. Cancels superseded runs via concurrency group.
- `.github/workflows/bench.yml` — parity bench placeholder (real benches
  land in Phase 1; CI structure exercised early).
- `.github/workflows/nightly.yml` — `cargo-audit`, `cargo-deny`, vs-upstream
  perf compare placeholder, one-hour fuzz smoke placeholder.
- `linux-x86_64-musl` and `linux-aarch64-gnu` left as TODO comments; they
  light up once the owner-provided self-hosted runners are online.

### Conformance harness skeleton (0.6)
- `conformance/harness/` workspace member building the
  `conformance-harness` binary.
- Scenario YAML schema + parser via `serde_yaml`.
- `list`, `check`, `dry-run` subcommands fully working.
- `run` subcommand surfaces a clear "not yet implemented (Phase 1+)" error.
- One trivial `smoke.yaml` scenario in `conformance/scenarios/`.
- Empty `fixtures.lock.json` with schema version + VM tag pin.
- `conformance/README.md` documenting usage and Phase 0 vs Phase 1 scope.

### Docs + state files (0.7)
- `docs/architecture/`, `docs/format/`, `docs/ops/`, `docs/perf/`,
  `docs/state/` directories each with a README explaining intent.
- `docs/state/{progress-log, backlog, blockers, decisions}.md` populated
  with Phase 0 kickoff entry, Phase 0 task list, empty-blockers state,
  and four ADRs (ADR-001 captures all 18 owner-locked decisions).
- Top-level `README.md` with status, component table, links to state files.

---

## Verification

Local on Linux x86_64-gnu:
- `cargo build --workspace` — green (3.93s clean, 0.08s incremental).
- `cargo test --workspace` — 15 tests passing, 0 failures (all in
  `esm-platform`; other crates intentionally have no tests yet).
- `cargo xtask lint` (= `cargo clippy --workspace --all-targets --
  -D warnings`) — green.
- `cargo xtask fmt --check` — green.
- `cargo deny check` — not run locally (cargo-deny not installed); CI
  exercises it.

Cross-compile sanity:
- `cargo check --target aarch64-apple-darwin --workspace` — green.
- `cargo check --target x86_64-pc-windows-msvc --workspace` — green.

CI-side verification is gated on the repository being pushed to GitHub
(deferred per ADR-001 #18 → "public from day one" but pending an actual
repo URL from the owner).

---

## Known limitations leaving Phase 0

1. **No commits yet.** Per owner directive, the first commit lands after
   the first round of *all* phases (0–9) is complete. State preserved on
   the working tree only.
2. **CI matrix incomplete locally.** The `linux-x86_64-musl` and
   `linux-aarch64-gnu` matrix slots are commented placeholders awaiting
   self-hosted runners.
3. **Conformance `run` not yet functional.** Returns a clear error; real
   docker orchestration + diff engine land in Phase 1.
4. **`serde_yaml` is deprecated upstream.** Tracked in
   [decisions.md](./decisions.md) (consider switching to `serde_yaml_ng`
   in Phase 1).
5. **`cargo-deny` not run locally.** Config validated by structure only;
   first real check happens in CI.
6. **vmui not yet downloaded.** `esm-vmui` crate exists with a build-script
   stub; actual fetch + checksum verification lands in Phase 4 when
   `esm-single` first serves the UI.
7. **No external dependencies pulled by feature flag yet.** Each crate
   pulls only what its current scope needs; further deps land per-crate
   as real implementations begin.

---

## What's next (Phase 1 — Storage engine)

Per [PLAN.md §9 Phase 1](../../PLAN.md), the next phase is the highest-risk
in the project. Sub-phases 1A → 1E land in order:

| Sub-phase | Deliverable | Est. weeks |
|---|---|---|
| 1A | Mergeset (inverted index): readers + writers + merger, byte-exact format | 6–8 |
| 1B | Time-series codecs: Gorilla XOR, delta-of-delta, block zstd | 3–4 |
| 1C | Time-series part format (timestamps/values/index/metaindex) | 4–5 |
| 1D | IndexDB + TSID assignment | 3–4 |
| 1E | Storage engine integration + Phase 1 conformance gate | 2–3 |

Backlog is rewritten to Phase 1 sub-tasks in
[docs/state/backlog.md](./backlog.md) once Phase 1 begins.

---

## Sign-off

All Phase 0 quality gates met. Recommend proceeding to Phase 1 unless the
owner has revisions to plan or scope.
