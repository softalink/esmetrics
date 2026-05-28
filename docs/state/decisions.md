# Architectural Decision Records (ADRs)

One entry per non-trivial decision. Each ADR records: date, status, context,
decision, consequences. Append-only; supersession is captured by linking
forward from the older record.

---

## ADR-001 — Initial locked decisions (Phase 0 kickoff)
**Date:** 2026-05-28
**Status:** Accepted
**Source:** [PLAN.md §17.1](../../PLAN.md)

The following decisions were reached with the project owner during the
Phase 0 planning conversation and form the binding baseline for all subsequent
work:

| # | Decision | Rationale (brief) |
|---|---|---|
| 1 | **Scope: single-node parity** (esm-single + esm-agent + esm-alert + esm-auth + esm-backup) | Bounded, useful, achievable. Cluster mode is post-v1.0. |
| 2 | **Drop-in compatibility** (on-disk + wire format match VM v1.144.0) | Highest user value; enables zero-downtime migration. |
| 3 | **PromQL first, MetricsQL deferred** | Reduces Phase 3 surface area; covers majority of users. |
| 4 | **Read VM Go source freely as reference**; Rust written from scratch | Apache 2.0 permits; faster than clean-room. |
| 5 | **Reuse upstream vmui unchanged** | It speaks the HTTP API; no port needed. |
| 6 | **Match-or-beat VM v1.144.0 performance** (hard requirement) | Bench gate in CI; nightly comparison vs upstream. |
| 7 | **Operating mode: 24/7 autonomous** with daily checkpoints | Per [PLAN.md §16](../../PLAN.md). |
| 8 | **VM reference pin: v1.144.0** (released 2026-05-22) | Conformance fixtures regenerated against this tag; bumps are gated. |
| 9 | **MSRV: current Rust stable − 2 minor versions** | Covers ~6 months of distros; pinned via `rust-toolchain.toml`. |
| 10 | **Pragmatic `unsafe` policy** — allowed in hot paths with `// SAFETY:` docs; Miri in CI | Realistic path to matching VM perf. |
| 11 | **Windows read I/O: mmap on all platforms** (defer-unlink coordination handles Windows quirks) | Matches VM behaviour closely; perf parity. |
| 12 | **Backup: bidirectional drop-in** with vmbackup / vmrestore | Same migration story as on-disk format. |
| 13 | **SD backends for v1.0: Tier 1 only** (static + file_sd); others land in v1.x behind feature gates | Bounded Phase 5 scope. |
| 14 | **zstd implementation: `zstd` crate bound to facebook/zstd C lib**, pinned to VM v1.144.0's version | Byte-identical part output. |
| 15 | **CLI flag naming: compat shim** accepting both `-storageDataPath` and `--storage-data-path` | Lower migration friction. |
| 16 | **v1.0 distribution: GitHub releases only** (signed tarballs + sha256 + cosign attestations). Others deferred to v1.x. | Narrow v1.0 surface. |
| 17 | **Code signing: owner-provided Apple Developer ID + Windows EV cert** | Best install UX; CI signs via GHA secrets. |
| 18 | **Repository visibility: public on GitHub from day one** | OSS communication, transparency, inviting community feedback. |

**Consequences:** This baseline shapes every phase. Changes to any of the
above require a new ADR explicitly superseding the relevant entry.

---

## ADR-002 — Allow `clippy::doc_markdown` at workspace level
**Date:** 2026-05-28
**Status:** Accepted

**Context:** The `clippy::doc_markdown` lint (pedantic group) requires every
CamelCase token in doc comments to be backticked, including product names like
`VictoriaMetrics`, `EsMetrics`, `MetricsQL`, `OpenMetrics`, `OpenTelemetry`,
`OpenTSDB`, etc. Doc comments touch nearly every file in EsMetrics; chasing
this lint becomes a constant maintenance tax with no clarity gain.

**Decision:** Add `doc_markdown = "allow"` to `[workspace.lints.clippy]` in
the root `Cargo.toml`. Authors backtick deliberate code references in doc
comments by hand; the lint no longer fights product-name spellings.

**Consequences:** Faster authoring; doc comments stay readable as English
prose. Manual backticking still expected for actual code identifiers
(`MmapMut`, `Storage::open`, etc.) and crate names.

---

## ADR-003 — Cargo workspace `resolver = "3"`
**Date:** 2026-05-28
**Status:** Accepted

**Context:** Cargo resolver v3 is the default for `edition = "2024"` and the
1.95.0 toolchain. Setting it explicitly documents intent and ensures the
workspace continues to use the v3 algorithm if the MSRV slips past the
default change.

**Decision:** Workspace root `Cargo.toml` declares `resolver = "3"`.

**Consequences:** Feature unification matches the v3 algorithm (per-target
feature resolution). No backwards-compat concerns for a fresh workspace.

---

## ADR-005 — Cross-compile checks defer to CI once C deps land

**Date:** 2026-05-28
**Status:** Accepted

**Context:** Before Phase 1B introduced `zstd-sys`, `cargo check --target
aarch64-apple-darwin` and `cargo check --target x86_64-pc-windows-msvc`
both completed on the Linux dev host. After zstd, both require a
target-native C toolchain (clang for darwin, MSVC's `lib.exe` for
windows-msvc, MinGW for windows-gnu) — none of which is installed on the
linux-x86_64 dev box and none of which is a sensible requirement for an
individual contributor.

**Decision:** Treat cross-compile verification as a CI-only check. Local
`cargo check` is sufficient validation on the contributor's host
platform; the per-platform native build runs in GitHub Actions
(see `.github/workflows/ci.yml`).

**Consequences:** Faster local iteration; no contributor needs MinGW or
the Apple SDK. The CI matrix is the only place where every target is
tested, which is fine for an open-source project with public CI.

---

## ADR-004 — Drop nightly-only rustfmt options
**Date:** 2026-05-28
**Status:** Accepted

**Context:** `imports_granularity = "Crate"` and
`group_imports = "StdExternalCrate"` produce warnings on stable rustfmt
because they're nightly-only. They were originally in `rustfmt.toml` to
enforce a canonical import layout.

**Decision:** Remove them from `rustfmt.toml`. Re-add if/when CI runs nightly
fmt as a separate workflow. Inline imports still get sorted via
`reorder_imports = true`.

**Consequences:** Import grouping is no longer enforced. Authors organise
imports manually; review catches inconsistencies. Cost is small in a
small-import-footprint codebase.
