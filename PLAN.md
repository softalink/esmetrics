# EsMetrics — Migration Plan

A cross-platform (Linux, macOS, Windows) Rust rewrite of the single-node VictoriaMetrics suite, aiming for byte-level on-disk compatibility, wire-level protocol compatibility, and performance parity-or-better with upstream.

This document is the authoritative migration plan. Subsequent design docs (per-phase) live under `docs/` and link back here.

---

## 1. Executive summary

**Goal.** Produce a Rust software suite, **EsMetrics**, that is a drop-in replacement for the **single-node** components of VictoriaMetrics on Linux, macOS, and Windows, with performance matching or exceeding upstream.

**Scope (single-node parity).**
- `esm-single` ← vmsingle
- `esm-agent` ← vmagent
- `esm-alert` ← vmalert
- `esm-auth` ← vmauth
- `esm-backup` / `esm-restore` ← vmbackup / vmrestore
- `esm-ctl` ← vmctl (deferred to late phases)
- **vmui reused as-is** — it talks only to the HTTP API, so no port needed.

**Out of scope for v1.**
- Cluster mode (`vminsert` / `vmselect` / `vmstorage` split). Architecture will leave hooks for it but no code.
- Enterprise-only features (downsampling, multitenancy auth, advanced retention filters).
- MetricsQL extensions beyond standard PromQL. Parser will accept them and return a clear "not yet implemented" error; planned for v1.x post-GA.

**Success criteria (binary).**
1. A VictoriaMetrics v1.144.0 data directory can be opened by `esm-single` and queried, returning byte-identical results for `/api/v1/query{,_range}` and `/api/v1/series` on a defined test corpus.
2. Data written by `esm-single` can be read by VictoriaMetrics v1.144.0 (round-trip).
3. Prometheus's upstream `promqltest` corpus passes against `esm-single`.
4. CI matrix green on `{linux-x86_64-gnu, linux-x86_64-musl, linux-aarch64-gnu, macos-aarch64, macos-x86_64, windows-x86_64-msvc}`.
5. Ingest throughput, query latency p50/p95/p99, and RAM-per-active-series **match or improve on** VM v1.144.0 measured on identical hardware via the project benchmark suite.

**Non-goals.**
- We are not building a "better PromQL" or a new TSDB design. Compatibility comes first.
- We do not add new query semantics, on-disk formats, or wire protocols. We mirror what VM does until v1 ships.

---

## 2. Locked decisions (from kickoff)

| Decision | Choice | Rationale |
|---|---|---|
| Scope tier | Single-node parity | Bounded, useful, achievable. Cluster is a future phase. |
| Compatibility | Drop-in (on-disk + wire) | Highest user value; enables zero-downtime migration. |
| Query language | PromQL first, MetricsQL deferred | Reduces phase-3 surface area; covers majority of users. |
| Source-reading policy | Read VM Go freely as reference; write Rust from scratch | Apache 2.0 permits; faster than clean-room; legal hygiene preserved. |
| vmui | Reuse upstream React build as-is | It speaks our HTTP API; no port needed. |
| Performance target | Match or beat VM v1.144.0 on identical hardware | Hard requirement. Bench gate in CI. |
| Operating mode | 24/7 autonomous execution | Pacing rules in §16. |

**Reference pin.** Upstream VictoriaMetrics is pinned to tag **`v1.144.0`** (released 2026-05-22). All conformance fixtures are generated against this tag. Bumping the pin is a deliberate, gated milestone, not a drive-by.

---

## 3. License & legal hygiene

- VictoriaMetrics is **Apache 2.0**. EsMetrics will ship **Apache 2.0** as well.
- `LICENSE` and `NOTICE` files preserve VM's attribution.
- `README.md` and `CREDITS.md` explicitly state that EsMetrics is an independent Rust reimplementation, references VictoriaMetrics design and source as a reference under Apache 2.0, and shares no original source code.
- **No copy-paste.** No machine translation of Go source files into Rust. Algorithmic understanding is fine; bit-for-bit format compatibility is fine; lifting source verbatim is not.
- Third-party crate licenses must be Apache-2.0, MIT, BSD-3, or compatible. A `cargo-deny` policy enforces this in CI.
- Trademarks: "VictoriaMetrics" is a trademark of VictoriaMetrics Inc. We do not call EsMetrics "a VictoriaMetrics fork" or use their marks beyond factual attribution.

---

## 4. Repository layout

```
esmetrics/                              # repo root (renamed from victoria-metrics-claude/ at Phase 0 kickoff)
├── PLAN.md                             # this file
├── README.md
├── LICENSE                             # Apache 2.0
├── NOTICE                              # attributions incl. VictoriaMetrics
├── CREDITS.md
├── rust-toolchain.toml                 # pinned stable channel
├── Cargo.toml                          # workspace root
├── deny.toml                           # cargo-deny config
├── .github/workflows/                  # CI definitions
├── docs/
│   ├── architecture/                   # per-subsystem design docs
│   │   ├── storage-engine.md
│   │   ├── promql-engine.md
│   │   ├── ingest-protocols.md
│   │   ├── scrape-agent.md
│   │   ├── alerting.md
│   │   ├── auth-proxy.md
│   │   └── backup-restore.md
│   ├── ops/                            # operator-facing docs (per binary)
│   ├── perf/                           # benchmark methodology + results
│   └── format/                         # on-disk format reverse-engineering notes
├── crates/
│   ├── esm-platform/                   # xplat: mmap, fsync, locks, signals, paths
│   ├── esm-common/                     # config, logging, errors, metrics-about-self
│   ├── esm-compress/                   # gorilla, delta-of-delta, zstd/snap/lz4 wrappers
│   ├── esm-storage/                    # mergeset, indexdb, parts, merger, retention
│   ├── esm-promql/                     # parser, planner, executor, functions
│   ├── esm-protocols/                  # promremote, influx, graphite, opentsdb, otlp, datadog, native
│   ├── esm-scrape/                     # scraper, relabeling, persistent queue
│   ├── esm-discovery/                  # SD backends (each feature-gated)
│   ├── esm-alerting/                   # rule eval, alertmanager client
│   ├── esm-net/                        # axum/hyper servers, TLS, common middleware
│   ├── esm-objstore/                   # S3/GCS/Azure via `object_store`
│   └── esm-vmui/                       # embeds upstream vmui static assets
├── apps/
│   ├── esm-single/
│   ├── esm-agent/
│   ├── esm-alert/
│   ├── esm-auth/
│   ├── esm-backup/                     # backup + restore subcommands
│   └── esm-ctl/                        # deferred
├── conformance/
│   ├── fixtures/                       # data dirs produced by upstream VM, checked in via Git LFS
│   ├── format-tests/                   # byte-level format round-trip tests
│   ├── promql-tests/                   # vendored Prometheus promqltest cases + VM cases
│   ├── wire-tests/                     # captured-traffic replay
│   └── harness/                        # docker-compose + driver binaries
├── benches/                            # criterion benchmarks (parity-vs-VM)
└── xtask/                              # dev tooling (test orchestration, perf, releases)
```

**Crate dependency direction** (no cycles): `apps/*` → `esm-net`, `esm-storage`, `esm-promql`, `esm-protocols`, `esm-scrape`, `esm-alerting`, `esm-objstore` → `esm-compress`, `esm-discovery` → `esm-common` → `esm-platform`. `esm-vmui` is leaf.

---

## 5. Toolchain & dependencies

### 5.1 Rust toolchain
- Channel: **stable**. MSRV pinned at `current stable - 2` (covers ~6 months of distros).
- `rust-toolchain.toml` pins the exact version; bump is a deliberate PR.
- Edition 2024.
- `clippy` + `rustfmt` enforced in CI. Lint profile in `clippy.toml`; deny `clippy::pedantic` minus a curated allow-list.

### 5.2 Core dependencies (intent locked; specific versions chosen at Phase 0)

| Concern | Crate | Rationale |
|---|---|---|
| Async runtime | `tokio` (multi-thread) | Industry standard; needed for axum. |
| HTTP server | `axum` + `hyper` | Same hyper used by reqwest; minimal stack. |
| HTTP client | `reqwest` (no default features; native-tls or rustls per platform) | For alertmanager, remote-write, SD backends. |
| TLS | `rustls` primarily; `native-tls` opt-in | Pure-Rust, FIPS-friendly path; native for Windows enterprise stores. |
| Protobuf | `prost` + `prost-build` | For Prom remote-write, OTLP. |
| Serialization | `serde`, `serde_json`, `serde_yaml`, `toml` | Standard. |
| Compression | `zstd` (binding to upstream C lib via `zstd-safe`), `snap`, `lz4_flex` | VM uses these; we match. |
| Memory mapping | `memmap2` | Maintained xplat mmap. |
| File locks | `fs2` or `fd-lock` (final choice in Phase 0) | Data-dir exclusivity. |
| Parallelism | `rayon`, `crossbeam` | Storage merges, query fan-out. |
| Bit-packing / SIMD | `std::simd` (portable_simd) + targeted `arch::x86_64`/`arch::aarch64` intrinsics behind cfg | Match VM's hand-tuned compression perf. |
| Object storage | `object_store` | S3/GCS/Azure abstraction; battle-tested via DataFusion/Delta. |
| Tracing/logging | `tracing` + `tracing-subscriber` | Structured logs; field names mirror VM's keys. |
| Metrics-about-self | `metrics` + `metrics-exporter-prometheus` | Self-monitoring at `/metrics`. |
| CLI parsing | `clap` (derive) | Standard. Flag names mirror VM's exactly. |
| Benchmarks | `criterion` + custom xtask harness for parity | Per-PR perf gate. |
| Fuzzing | `cargo-fuzz` (`libFuzzer`) + `arbitrary` | Parsers (PromQL, ingest formats). |
| Property testing | `proptest` | Storage format round-trips. |

### 5.3 Build profile policy
- `release` profile: `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, `debug = 1` (line tables only).
- PGO build target for release artifacts: `cargo-pgo` in xtask, two-stage build with a representative workload corpus.
- `release-with-debug` profile retains full debug info for perf investigation.

### 5.4 Forbidden patterns (lint-enforced)
- `unwrap()` and `expect()` outside tests, except behind a `// SAFETY: invariant ...` justification or in `main()`.
- `std::process::exit` outside `main()`.
- `println!`/`eprintln!` outside `xtask/` and binaries' CLI parsing (use `tracing`).
- Blocking I/O on the async runtime (custom clippy lint or runtime-detection in debug builds).
- `async fn` for storage hot paths (sync + threadpool via rayon/`tokio::task::spawn_blocking`).

---

## 6. Cross-platform strategy

All platform-specific code lives in `esm-platform`. The rest of the workspace consumes platform-neutral traits.

### 6.1 `esm-platform` abstractions
- `Mmap` / `MmapMut`: thin wrapper over `memmap2` with documented Windows semantics (handles must outlive mappings; no unlink-while-mapped; `MAP_POPULATE` is Linux-only and opportunistic).
- `Durability::fsync_file` / `Durability::fsync_dir`: `fsync(2)` on Unix; `FlushFileBuffers` on Windows; directory fsync is no-op on Windows (documented).
- `AtomicRename::rename`: `std::fs::rename` (which already handles `MoveFileEx`+`MOVEFILE_REPLACE_EXISTING` on Windows in modern Rust).
- `FileLock::try_acquire_exclusive` / `acquire_exclusive`: `flock(2)` on Unix; `LockFileEx` on Windows. Single source of truth for data-dir locking.
- `signal::shutdown_stream()`: yields on SIGTERM/SIGINT (Unix) or Ctrl-C/Ctrl-Break/service-stop (Windows). Returns the signal name for logging.
- `signal::reload_stream()`: SIGHUP on Unix; Windows uses a configurable mechanism (named pipe or service control code; default off).
- `paths::canonical_data_path`: handles Windows long-path prefix `\\?\` when needed; rejects paths containing characters that VM tolerates on Linux but Windows refuses (`:`, `*`, `?`, `<`, `>`, `|`, `"`).
- `proc::set_open_file_limit`: best-effort `setrlimit(RLIMIT_NOFILE)` on Unix; no-op on Windows. Documented required value.

### 6.2 Per-platform packaging
- **Linux**: static-musl binaries for distro-independent deploys; deb/rpm via `cargo-deb`/`cargo-generate-rpm`. systemd unit shipped as a separate package.
- **macOS**: universal binaries (`lipo`-joined x86_64 + aarch64). Homebrew formula. Code signing + notarization for Developer ID releases.
- **Windows**: MSVC builds. MSI installer via WiX or `cargo-wix`. Windows Service integration via the `windows-service` crate; service registration handled by installer.

### 6.3 Cross-platform CI matrix (Phase 0 deliverable)
| Target | Runner | Tests | Bench |
|---|---|---|---|
| linux-x86_64-gnu | ubuntu-latest | full | yes |
| linux-x86_64-musl | ubuntu-latest container | full | no |
| linux-aarch64-gnu | linux-arm64 runner | full | yes |
| macos-aarch64 | macos-14 | full | yes |
| macos-x86_64 | macos-13 | full | no |
| windows-x86_64-msvc | windows-latest | full | yes |

"Full tests" includes conformance harness (launches upstream VM in a sidecar via Docker on linux+windows; via colima on macos).

---

## 7. Performance strategy

Matching or beating VM perf is a hard requirement. This is the riskiest commitment in the plan and shapes many design choices.

### 7.1 Where VM gets its perf
1. Hand-tuned bit-packing for timestamps + values (Gorilla XOR for floats, delta-of-delta + variable-bit for timestamps).
2. Lock-free or coarse-grained locking in hot ingest paths; arena-allocated row batches.
3. Aggressive mmap-based reads; OS page cache does the heavy lifting.
4. Background merger sized to physical core count, not goroutine spam.
5. Custom string-interning for label names.
6. PromQL evaluation operates on column-oriented `Series → []Sample` blocks, not point-at-a-time.

### 7.2 How we match in Rust
- **SIMD-accelerated codecs.** `esm-compress` exposes scalar reference impls and SIMD-accelerated variants gated by `cfg(target_feature)`. Targets: AVX2 + AVX-512 on x86_64; NEON on aarch64. Dispatch via runtime feature detection at first call.
- **Arena allocation.** Per-request and per-batch arenas via `bumpalo`. No `Vec::with_capacity` guessing on hot paths.
- **Buffer pools.** Custom thread-local pools for the small set of buffer sizes the storage engine actually allocates. Returned-to-pool on drop.
- **Sync I/O on storage hot paths.** Async only at the network edge. Storage merges run on `rayon` with thread count = physical cores; ingest write path uses dedicated worker threads, not Tokio executors.
- **`tokio-uring` (Linux) opt-in.** Feature-gated optimization for high-fanout remote-write fanout in `esm-agent`. Falls back to standard I/O when unavailable. Never the default path.
- **mmap-only read path.** `MmapMut` for new parts being merged, then converted to read-only mmap after publication.
- **Label interning.** Global concurrent string interner (`arc-swap` + `dashmap`) returning `Symbol(u32)`; comparisons and hashing operate on the symbol, not the string.
- **PGO + LTO release builds.** `cargo pgo` integrated into release pipeline. Representative workload corpus checked into `benches/pgo-corpus/`.
- **Zero-copy parsing.** Ingest parsers take `&[u8]` and produce label-set + value refs into the source buffer. Validators run before any allocation.

### 7.3 Perf gate in CI
- `benches/parity/` contains a benchmark suite mirroring VM's `lib/storage` benchmarks and a synthetic Prometheus scrape simulator.
- Every PR runs the parity bench on the linux-x86_64 + linux-aarch64 + windows-x86_64 + macos-aarch64 runners. Results are compared against a rolling baseline checked into `benches/baselines/`.
- A regression of >3% on any benchmark on any platform fails the build.
- Comparison vs upstream VM runs nightly (not per-PR; too slow). Threshold: any bench >5% slower than VM v1.144.0 on the same hardware blocks the next release.

### 7.4 Perf debugging tooling (built in Phase 0)
- `xtask perf record` — runs a workload under `perf`/`samply` and produces a flamegraph.
- `xtask perf compare <bench> --against vm` — runs same workload against VM and EsMetrics, side-by-side.
- `xtask perf budget` — verifies a candidate build against the rolling baselines.

---

## 8. Conformance harness

This is the single most important piece of infrastructure for "drop-in compatibility." Built in Phase 0; lives forever.

### 8.1 What it does
1. Launches upstream VictoriaMetrics v1.144.0 (Docker `victoriametrics/victoria-metrics:v1.144.0`) as a sidecar.
2. Launches `esm-single` from the workspace under test.
3. Drives both with identical sequences of HTTP requests (ingest + query) from a scenario DSL.
4. Diffs:
   - **HTTP responses** byte-for-byte where format is canonical (e.g., protobuf-encoded).
   - **HTTP responses** semantically where format is JSON (PromQL results are unordered sets; compare as sets).
   - **On-disk parts** byte-for-byte after triggering compaction.
   - **Internal `/metrics` parity** for a documented subset of metric names (e.g., `vm_rows_inserted_total` ↔ `esm_rows_inserted_total` semantically equivalent).
5. Round-trip tests:
   - VM writes → shutdown → esm reads (the "drop-in" test).
   - esm writes → shutdown → VM reads (the "reverse drop-in" test).

### 8.2 Scenario DSL
A YAML-based DSL (parsed by `conformance/harness`) defines reproducible scenarios:
```yaml
name: prom-remote-write-basic
ingest:
  - protocol: prom-remote-write
    data: fixtures/prom-rw-1k-series.bin
queries:
  - path: /api/v1/query
    params: { query: "up", time: "2026-05-28T00:00:00Z" }
    compare: semantic_set
  - path: /api/v1/query_range
    params: { query: "rate(http_requests_total[5m])", start: ..., end: ..., step: "15s" }
    compare: semantic_set_with_tolerance
    tolerance: { value: 1e-9 }
on_disk:
  compare_after: compact
  paths:
    - data/indexdb
    - data/data
```

### 8.3 Fixture management (no Git LFS)
Fixtures are **reproducible artifacts**, not irreducible inputs — they can always be regenerated by running VM at the pinned tag against the scenario script. We exploit this to keep the Git repo lean.

- **Repo stores text only:** scenario YAML definitions live in `conformance/scenarios/*.yaml`. No binary fixtures in Git.
- **Lock manifest:** `conformance/fixtures.lock.json` maps each scenario → `(vm_tag, scenario_hash, expected_fixture_sha256, byte_size)`. Diff-friendly. Updated automatically when scenarios change or the VM pin is bumped.
- **Local cache:** harness writes generated fixtures to `target/conformance-cache/<scenario-hash>/` and reuses across runs. Cache survives `cargo clean` (lives outside `target/debug` and `target/release`).
- **CI cache:** GitHub Actions `actions/cache@v4` keyed on `${{ vm_tag }}-${{ scenarios_hash }}`. First CI run on a new pin is slow (regenerates everything from VM Docker images); subsequent runs hit cache.
- **Optional shared cache (off by default):** `xtask fixtures push <s3-bucket>` uploads to owner-controlled object storage; `xtask fixtures pull` downloads with sha256 verification before falling back to regeneration. Activated via env var, never required.
- **Regeneration command:** `xtask fixtures regenerate [--scenario <name>] [--all]` rebuilds fixtures, updates `fixtures.lock.json`, surfaces any sha256 deltas in a single review-friendly diff.
- **CI behavior on cache miss:** budget 30 minutes for full regeneration against VM Docker. If a CI run blows that budget, increase the runner timeout rather than smuggling binaries into the repo.

### 8.4 Failure modes & isolation
- Harness runs each scenario in a fresh tempdir + fresh container.
- On mismatch, the harness saves both VM's and esm's responses + on-disk diff to `target/conformance-failures/<scenario>/`.
- Scenarios are independent; one failure does not cascade.

---

## 9. Phase plan

Each phase ends with a runnable binary and passes its quality gates before we move on. Phases are sequential; intra-phase work is parallel where dependencies allow. Time estimates are FTE-engineer ranges and exist to set expectations, not commitments.

### Phase 0 — Foundation (2–3 weeks)

**Deliverable:** workspace skeleton with hello-world binaries, full CI matrix green, conformance harness scaffold that can launch upstream VM and drive it, perf-bench scaffold with one trivial parity benchmark.

**Sub-deliverables:**
- `Cargo.toml` workspace + all empty crates with minimal `lib.rs`
- `apps/esm-single/main.rs` that starts an HTTP server, returns `204` on `/api/v1/write`, `[]` on `/api/v1/query`
- `esm-platform` core abstractions implemented and unit-tested on all three OSes
- `.github/workflows/ci.yml` running clippy + rustfmt + tests on the full matrix
- `.github/workflows/bench.yml` running parity benchmarks on PR
- `conformance/harness` binary that launches VM in Docker, ingests a tiny dataset, and confirms HTTP roundtrip
- `xtask` with `fmt`, `lint`, `test`, `bench`, `perf record`, `fixtures regenerate` subcommands
- `deny.toml` + `cargo-deny check` in CI
- `LICENSE`, `NOTICE`, `CREDITS.md`, initial `README.md`

**Verification gates:**
- All CI matrix targets green.
- Conformance harness can produce a `PASS` for the trivial scenario.
- `xtask bench` runs cleanly on linux+macos+windows.

**Risks:** Windows CI flakiness around Docker (use Linux containers on WSL2 backend; if too unstable, skip harness on Windows CI initially and document).

---

### Phase 1 — Storage engine port ⚠️ HIGHEST RISK (4–6 months)

This phase is the crux of drop-in compat and consumes more time than any other phase. It is decomposed into independently-verifiable sub-phases.

#### 1A. Inverted index (mergeset) — 6–8 weeks
**Goal:** byte-exact reimplementation of VM's `lib/mergeset` package.

**What mergeset is:** an LSM-like sorted-string KV store optimized for VM's index entries (e.g., `metric_name → TSID`, `tag_pair → set<TSID>`). Parts are immutable, compressed (zstd at part level), with two-level (metaindex + index) addressing.

**Sub-tasks:**
1. Reverse-engineer + document the part on-disk format. Output: `docs/format/mergeset-part.md` with field-by-field layout.
2. Implement reader: `MergesetReader::open(path)` returning iterators over sorted entries.
3. Verify with fixture: VM writes a known set of entries → esm reads same entries.
4. Implement writer: produces parts byte-identical to VM's writer for a given input order.
5. Verify with fixture: esm writes same set of entries → VM reads back identical entries.
6. Implement in-memory part (the staging buffer before flush).
7. Implement merger: VM's level scheme, retention-aware, concurrent merges bounded by core count.
8. Verify: after a sequence of writes + merges, esm's parts are byte-identical to VM's.

**Quality gates:**
- 100% of mergeset format-tests pass.
- Round-trip tests (VM↔esm) pass on a fixture of ≥10M entries.
- Mergeset write throughput within 5% of VM at the parity benchmark.

#### 1B. Time-series compression codecs — 3–4 weeks
**Goal:** identical encoded bytes to VM's `lib/encoding` for timestamps and values.

**Codecs to implement:**
- Timestamps: delta-of-delta + variable-bit packing (VM's `marshalInt64Array` family).
- Values (floats): Gorilla XOR + variable-bit length.
- Values (constant detection): "all same" short-encoding.
- Block-level zstd wrapper.

**Sub-tasks:**
1. Reverse-engineer + document each codec. Output: `docs/format/timeseries-codecs.md`.
2. Scalar reference implementation, tested against fixture vectors generated by VM.
3. SIMD-accelerated variant gated by `cfg(target_feature)`, falling back to scalar.
4. Property tests (`proptest`) for round-trip correctness on random inputs.
5. Fuzz the decoder against arbitrary byte inputs.

**Quality gates:**
- Encoded bytes match VM's bytes on a corpus of 10K real-world traces.
- Decoder accepts every byte sequence VM's decoder accepts (differential fuzzing).
- Codec throughput within parity vs VM on the bench suite.

#### 1C. Part format (time series data) — 4–5 weeks
**Goal:** VM's per-day part directory format with `timestamps.bin`, `values.bin`, `index.bin`, `metaindex.bin`.

**Sub-tasks:**
1. Document the layout: `docs/format/timeseries-part.md`.
2. Implement reader.
3. Implement writer.
4. Block-level addressing (each part contains many small blocks, indexed by metaindex).
5. Round-trip verification with VM fixtures.

#### 1D. IndexDB + TSID assignment — 3–4 weeks
**Goal:** VM's `lib/storage/index_db.go` semantics: TSID generation, `metricID → MetricName` lookup, `tag_pair → TSIDs`, date-sharded indices, generationTSID rotation.

**Sub-tasks:**
1. Document indexdb structure: `docs/format/indexdb.md`.
2. Implement TSID assigner (deterministic given the same input order, matching VM's algorithm).
3. Implement lookup APIs.
4. Implement generation rotation (used for retention enforcement).
5. Round-trip verification.

#### 1E. Storage engine integration — 2–3 weeks
**Goal:** glue 1A–1D into a single `Storage` API used by everything else.

**APIs to expose:**
- `Storage::open(path: &Path, config: StorageConfig) -> Result<Storage>`
- `Storage::ingest(rows: &[Row]) -> Result<()>`
- `Storage::search_metric_names(filters: &[TagFilter], time_range: TimeRange) -> impl Iterator`
- `Storage::search_data(metric_ids: &[u64], time_range: TimeRange) -> impl Iterator<Item=Block>`
- `Storage::register_signal_handlers(...)`
- `Storage::flush()`, `Storage::shutdown()`

**Quality gates:**
- Full Phase 1 conformance harness scenario passes: ingest 100M points via VM, shutdown, open with esm, read back identical.
- Reverse direction also passes.
- Ingest throughput ≥ VM v1.144.0 on the parity benchmark.

**Risks:**
- VM's format has undocumented quirks; we may discover edge cases late. Mitigation: differential fuzzing from day one of each sub-phase.
- Performance shortfall on a specific codec. Mitigation: SIMD work scheduled into 1B explicitly; PGO build profile available.

---

### Phase 2 — Ingest protocols (6–8 weeks)

**Deliverable:** `esm-single` accepts all VM-supported ingest protocols with wire-compatible behavior.

**Protocols (in priority order):**
1. Prometheus remote-write (`/api/v1/write`) — protobuf + snappy. **First priority.**
2. Native VM protocol (`/api/v1/import/native`) — used by vmagent → vmsingle.
3. JSON line protocol (`/api/v1/import`).
4. CSV (`/api/v1/import/csv`).
5. Influx line v1 + v2 (`/write`, `/api/v2/write`).
6. Graphite (TCP plaintext + HTTP).
7. OpenTSDB (telnet + HTTP).
8. DataDog (`/api/v1/series`).
9. NewRelic (`/infra/v2/metrics/events/bulk`).
10. OpenTelemetry metrics (`/opentelemetry/v1/metrics`).

**Per protocol:**
- Zero-copy parser in `esm-protocols`.
- Conformance test: capture a real client's request (using `tcpdump` against VM in the harness), replay against esm, verify identical persisted result.
- Error responses match VM's status codes and bodies for a documented set of error conditions.

**Quality gates:**
- All ten protocols handle their conformance scenario.
- Ingest throughput per protocol within 5% of VM.

**Risks:**
- OTLP and DataDog evolve; pin to the version VM v1.144.0 supports.

---

### Phase 3 — PromQL engine (3–4 months)

**Deliverable:** PromQL parser + planner + executor, exposed via VM's HTTP query API (`/api/v1/query`, `/api/v1/query_range`, `/api/v1/series`, `/api/v1/labels`, `/api/v1/label/<name>/values`, `/api/v1/status/*`).

**Sub-phases:**

#### 3A. Lexer + parser — 3 weeks
- Hand-written or `nom`-based PromQL parser producing an AST.
- AST matches Prometheus's expression types (vector, matrix, scalar, string).
- Recognize MetricsQL extensions at the lexer level and reject with a clear "MetricsQL extension `<name>` is not yet implemented" error (not a parse failure).

**Quality gates:** parse 100% of Prometheus's `promqltest` corpus inputs.

#### 3B. Planner — 2 weeks
- Lowers AST to a physical plan tree (selector, eval-step, aggregator, function-call, binop).
- Series-selection pushdown to the storage layer.
- No optimizer in v1 beyond constant folding + selector merging.

#### 3C. Executor — 6–8 weeks
- Column-oriented block evaluation.
- All standard PromQL functions: `rate`, `irate`, `increase`, `delta`, `idelta`, `deriv`, `predict_linear`, `histogram_quantile`, `holt_winters`, `clamp`, `clamp_min`, `clamp_max`, `absent`, `absent_over_time`, `changes`, `resets`, `count_values`, `quantile`, `topk`, `bottomk`, `sort`, `sort_desc`, `time`, `timestamp`, `vector`, `scalar`, `year`, `month`, `day_of_month`, `day_of_week`, `day_of_year`, `days_in_month`, `hour`, `minute`, plus all the basic aggregators (sum, avg, min, max, count, group, stddev, stdvar) with grouping (`by`, `without`).
- Binary ops with vector matching (`on`, `ignoring`, `group_left`, `group_right`).
- Subqueries.

**Quality gates:** Prometheus's `promqltest` corpus passes at 100%. The VM-flavored subset of MetricsQL tests that are pure PromQL also pass.

#### 3D. Query HTTP API — 2 weeks
- All endpoints listed above with VM-identical request/response shapes.
- Streaming results where VM streams (large `/api/v1/series` responses).

#### 3E. Performance tuning — 2–4 weeks (overlaps later phases)
- Parallel evaluation across series.
- Block-level vectorized math.
- Query result caching (matches VM's behavior).
- Benchmark gate: query latency p50/p95/p99 ≤ VM's.

**Risks:**
- PromQL has subtle edge cases (NaN propagation, lookback delta handling, stale markers) that take time to get right. Mitigation: `promqltest` corpus is exhaustive.

---

### Phase 4 — esm-single integration (3–4 weeks)

**Deliverable:** production-ready single binary with the full VM `vmsingle` CLI surface.

**Sub-tasks:**
- Wire Phase 1 + 2 + 3 into the binary.
- CLI flags matching VM's exactly (`-storageDataPath`, `-retentionPeriod`, `-httpListenAddr`, `-search.maxQueryDuration`, `-memory.allowedPercent`, ~80 flags total).
- Self-monitoring `/metrics` endpoint with metric names mirroring VM's.
- Embed vmui static assets from upstream (`crates/esm-vmui` build script downloads + verifies sha256 of the VM v1.144.0 vmui artifact at build time, with offline fallback).
- systemd unit, launchd plist, Windows Service entry.
- Graceful shutdown (drain pending writes, flush WAL, fsync).
- Config file support (YAML, matching any VM YAML configs).

**Quality gates:**
- A Grafana dashboard pointed at upstream VM works identically when pointed at `esm-single`.
- End-to-end perf benchmark (Prometheus scrape simulator → esm-single → Grafana query) matches or beats VM.
- All ~80 CLI flags accepted; unknown flags produce VM-identical error messages.

---

### Phase 5 — esm-agent (3–4 months)

**Deliverable:** vmagent-equivalent: scraping, relabeling, remote-write fanout, persistent disk queue.

**Sub-phases:**

#### 5A. Scraper core — 3 weeks
- HTTP scraping with Prometheus-compatible exposition format parsing (text + OpenMetrics).
- Per-target scrape scheduling with jitter.
- TLS, basic auth, bearer tokens, OAuth2.

#### 5B. Relabeling engine — 3 weeks
- Full Prometheus relabel rule set: `replace`, `keep`, `drop`, `keepequal`, `dropequal`, `hashmod`, `labelmap`, `labeldrop`, `labelkeep`, `lowercase`, `uppercase`, `keep_if_equal`, `drop_if_equal`.
- Regex engine: `regex` crate (RE2-compatible, like Prometheus uses).
- Conformance: vmagent's relabel test corpus passes against esm-agent.

#### 5C. Persistent queue — 3 weeks
- Disk-backed queue for remote-write retry buffering, matching vmagent's `persistentqueue` layout for drop-in upgrades.
- Compression, fsync policy, bounded size with overflow handling.

#### 5D. Service discovery — 4–6 weeks
- Backends in priority order: `static_configs`, `file_sd_configs`, `kubernetes_sd_configs`, `consul_sd_configs`, `ec2_sd_configs`, `gce_sd_configs`, `azure_sd_configs`, `dns_sd_configs`, `dockerswarm_sd_configs`, `nomad_sd_configs`, `http_sd_configs`, `kuma_sd_configs`.
- Each backend feature-gated (`features = ["sd-kubernetes", "sd-ec2", ...]`) so users can build minimal binaries.
- Conformance: each backend tested against a docker-compose'd target environment in CI.

#### 5E. Remote-write fanout — 2 weeks
- Multiple targets with per-target queueing.
- Streaming aggregations (vmagent's `stream aggregation` mode).
- Authorization headers, custom headers per target.

**Quality gates:**
- esm-agent → esm-single (or VM) full pipeline test passes.
- Same set of targets scraped + relabeled produces identical remote-write output to vmagent.

---

### Phase 6 — esm-alert (1.5–2 months)

**Deliverable:** vmalert-equivalent rule evaluator.

**Sub-tasks:**
- Rule file loader (YAML, Prometheus-compatible).
- Recording rule evaluation against `esm-single` or any PromQL-speaking backend, write-back via remote-write.
- Alerting rule evaluation with `for:` / `keep_firing_for:` state tracking, deduped against Alertmanager v2 API.
- Alertmanager client (HA-aware: multiple AM URLs, round-robin).
- `/api/v1/rules`, `/api/v1/alerts` HTTP API matching Prometheus/vmalert.
- State persistence across restarts (vmalert's `-remoteRead.url` mechanism).

**Quality gates:**
- vmalert's example rule files load identically.
- Firing alerts arrive at a fake Alertmanager with identical payloads.

---

### Phase 7 — esm-auth (3–4 weeks)

**Deliverable:** vmauth-equivalent reverse proxy.

**Sub-tasks:**
- YAML config schema identical to vmauth's `auth.yml`.
- Per-user URL maps with prefix routing.
- Bearer token + basic auth.
- IP filters (allow/deny lists).
- Per-route retry policy + backend failover.
- Request/response header manipulation.
- TLS termination (rustls).

**Quality gates:** vmauth config dropped into esm-auth routes traffic identically.

---

### Phase 8 — esm-backup / esm-restore (3–4 weeks)

**Deliverable:** snapshot-based incremental backup/restore to S3/GCS/Azure.

**Sub-tasks:**
- Snapshot mechanism in `esm-storage` (`Storage::create_snapshot()`): hard-link parts into a snapshot directory atomically.
- Incremental backup: diff against previous backup manifest; upload only new/changed parts.
- Restore: download manifest + parts, place into target data dir.
- Object store backends via `object_store` crate.
- Lifecycle: snapshot deletion when older than `-retention`.

**Quality gates:**
- vmbackup backup of a VM data dir restored via esm-restore opens cleanly in esm-single.
- esm-backup of an esm-single data dir restored via vmrestore opens cleanly in VM.

---

### Phase 9 — Hardening, perf finalization, release (ongoing, gated on real-world usage)

**Deliverable:** v1.0.0 release candidate.

**Sub-tasks:**
- Fuzz all parsers continuously (set up `oss-fuzz` style continuous fuzzing).
- Soak test: run esm-single under a synthetic Prometheus load for 30 days, monitor for memory growth, file handle leaks, perf degradation.
- Memory profiling under sustained load (`heaptrack`, `valgrind massif`).
- Windows service hardening: event log integration, graceful upgrade.
- macOS notarization pipeline.
- Documentation: full operator docs, migration guide from VM, troubleshooting runbook.
- Release artifacts: `.tar.gz`, `.deb`, `.rpm`, Homebrew formula, MSI, OCI images.
- Security review: dependency audit, secrets-in-logs review, attack surface review of HTTP endpoints.

**Quality gates for v1.0.0:**
- All conformance scenarios pass.
- Performance parity benchmarks: every metric within ±5% of VM v1.144.0 or better.
- 30-day soak shows no regression.
- Zero `cargo-audit` advisories at critical/high severity.

---

## 10. Quality gates summary

| Level | Trigger | Gate |
|---|---|---|
| Pre-commit hook (optional) | Local commit | `cargo fmt --check`, `cargo clippy -- -D warnings` on changed crates |
| PR | Every push | Full CI matrix; conformance harness on linux+windows+macos; bench parity check (±3% vs rolling baseline) |
| Nightly | Cron | Full conformance suite; bench vs upstream VM (±5%); fuzz one-hour smoke; cargo-audit |
| Phase end | Tag `phase-N` | All phase-specific quality gates listed in §9 |
| Release candidate | Tag `v*-rcN` | 7-day soak on representative workload |
| Release | Tag `v*` | All RC checks + 30-day soak + signed artifacts |

---

## 11. CI/CD strategy

### 11.1 Workflows
- `ci.yml`: triggered on PR + push to main. Runs fmt, clippy, test, conformance (per platform), bench parity check.
- `bench.yml`: triggered on PR. Runs subset of benchmarks; full nightly.
- `nightly.yml`: cron at 02:00 UTC. Full benches vs VM, full fuzz smoke, cargo-audit, cargo-deny, license audit.
- `release.yml`: triggered on `v*` tag. Builds release artifacts for all targets, signs, publishes to GitHub releases + OCI registry + Homebrew tap.

### 11.2 Artifact caching
- `sccache` for compilation cache (S3 backend on self-hosted runners; GHA cache on GitHub-hosted).
- Cargo registry cache.
- Bench baseline checked into `benches/baselines/<target>/<commit>.json`; rolling window of 30 commits retained.

### 11.3 Self-hosted runners
- linux-aarch64: 1 self-hosted runner (Ampere or similar).
- macOS-aarch64: GitHub-hosted (`macos-14`) is sufficient.
- Bench reproducibility requires pinned CPU governor on self-hosted runners (`performance` mode, fixed frequency).

---

## 12. Release strategy

### 12.1 Versioning
- Semver. `0.x` during phases 0–8. `1.0.0` after phase 9 RC gates pass.
- Each phase ends with a `0.N.0` tag (Phase 1 → `0.1.0`, Phase 2 → `0.2.0`, etc.).

### 12.2 Channels
- `main` branch: always green.
- `release/0.x` branches for backports.
- Pre-1.0: no API stability promise.
- Post-1.0: storage format stable; HTTP API stable; CLI flags stable. Breaking changes require major version bump.

### 12.3 Artifacts

**v1.0 (required):**
- GitHub Releases:
  - Linux: `esmetrics-v*-linux-x86_64-gnu.tar.gz`, `esmetrics-v*-linux-x86_64-musl.tar.gz`, `esmetrics-v*-linux-aarch64-gnu.tar.gz`
  - macOS: `esmetrics-v*-macos-universal.tar.gz` (lipo'd x86_64 + aarch64), code-signed + notarized via owner-provided Apple Developer ID
  - Windows: `esmetrics-v*-windows-x86_64.zip` + `esmetrics-v*-windows-x86_64-setup.exe`, code-signed via owner-provided EV cert
  - Per-artifact `sha256sum.txt` + `cosign` signatures + SLSA L3 provenance attestations
- Public release notes auto-generated from CHANGELOG.md.

**v1.x (deferred from v1.0):**
- OCI images (multi-arch `docker.io/esmetrics/*` + `ghcr.io/...`)
- Linux package repos (apt + yum)
- Homebrew tap (`esmetrics/esmetrics`)
- Chocolatey (Windows)
- MSI distribution beyond GitHub releases

These can ship in v1.x without breaking changes; v1.0 keeps the surface narrow.

---

## 13. Documentation strategy

- `docs/architecture/*` — per-subsystem design (updated as we build).
- `docs/format/*` — on-disk format reverse-engineering notes (the canonical EsMetrics format spec).
- `docs/ops/*` — operator-facing per-binary docs (CLI flags, config, deployment, troubleshooting).
- `docs/perf/*` — benchmark methodology, results, regression history.
- `docs/migration-from-vm.md` — step-by-step guide for VM users to switch.
- `CHANGELOG.md` — keep-a-changelog format, updated per release.
- Public docs site: mdBook output of `docs/` published via GitHub Pages.

---

## 14. Risk register

Top risks ordered by impact × probability. Lower-impact risks tracked inside per-phase plans.

| # | Risk | Impact | Likelihood | Mitigation |
|---|---|---|---|---|
| 1 | Storage format has undocumented quirks discovered late | High | High | Differential fuzzing from Phase 1A day 1; allocate buffer time at end of Phase 1. |
| 2 | Performance shortfall on a specific codec or hot path | High | Medium | SIMD work scheduled into Phase 1B; PGO build profile; perf budget gate from Phase 0. |
| 3 | VM v1.144.0 has implementation bugs we'd faithfully replicate | Medium | Medium | Track upstream bug reports; document deliberate divergences in `docs/compat-deltas.md`. |
| 4 | Tokio + sync-storage interop causes subtle deadlocks under load | High | Low | Strict separation enforced; soak test in Phase 9. |
| 5 | Windows file-locking semantics break some workflow | Medium | Medium | Phase 0 platform abstractions tested on Windows from day 1. |
| 6 | Conformance harness Docker dependency is unreliable on macOS / Windows CI | Medium | Medium | Document fallback: harness can run against a remote VM instance for those platforms. |
| 7 | License/trademark concerns escalate | Low | Low | Apache 2.0 attribution + factual references only; no marks usage. |
| 8 | Upstream VM bumps a major version mid-project | Medium | High | Pin policy: stay on v1.144.0 until phase 4 ships; consider re-pin then. |
| 9 | Solo-engineer + agent cadence cannot sustain 9–18 month timeline | High | High | Acknowledged; pacing and checkpoints in §16; pause/extend triggers documented. |
| 10 | Dependency churn (tokio 2.0, axum major bump) breaks builds | Low | Medium | MSRV policy; lock-file checked in; quarterly dep-bump cadence. |

---

## 15. Glossary

- **VM** — VictoriaMetrics, the upstream Go project.
- **EsMetrics** — this Rust reimplementation.
- **Mergeset** — VM's LSM-like sorted-string store used for the inverted index.
- **Part** — an immutable directory of files representing a chunk of stored data; merged by background mergers.
- **TSID** — Time Series ID; an integer identifier assigned to each unique label set.
- **IndexDB** — VM's index that maps metric names + tag pairs to TSIDs.
- **Gorilla XOR** — Facebook's float compression scheme used by VM for sample values.
- **Delta-of-delta** — timestamp compression encoding successive differences.
- **vmui** — VM's React frontend; we embed it unchanged.
- **Conformance harness** — the test infrastructure that runs VM and EsMetrics side-by-side and diffs outputs.
- **Parity bench** — benchmark suite measuring EsMetrics against upstream VM on identical workloads.

---

## 16. Autonomous operation mode

Operating assumption: 24/7 autonomous execution under the project owner's direction.

### 16.0 Standing directive — do not stop until parity

The owner has repeatedly affirmed: **drive `docs/state/backlog.md` to all-`[x]` autonomously**. Do not pause between rounds to ask "should I continue?". On any session that lands in this workspace:

1. Read this section and `docs/state/backlog.md`.
2. Pick the first open `[ ]` item.
3. Implement, validate (`cargo clippy --workspace --all-targets -- -D warnings` + `cargo test --workspace` must be clean), update `backlog.md` + `progress-log.md`.
4. Repeat from step 2.

Stop only when:
- Every backlog item is `[x]`, **or**
- An item literally requires operator action that cannot be simulated (creating a public GitHub repo + push access, running a 30-day soak in real wall-clock time, paying for a code-signing certificate).

Docker (`victoriametrics/victoria-metrics:v1.144.0`) is pre-authorised as the byte-format / query-shape oracle. The conformance harness drives it (see `conformance/HOW-TO-RUN.md`). Spin it up unprompted when an item is gated on a VM oracle.

**The no-commits constraint from the kickoff session still applies until parity is reached**; then propose a commit chain summarising the work.

### 16.1 Cadence
- **Working iteration:** the agent picks the next task from the current phase's backlog (tracked in `docs/state/backlog.md`), implements it end-to-end, verifies it against the relevant gates, and commits.
- **Daily checkpoint:** at the end of each agent day (24h since last checkpoint, or when context approaches limits), write a one-screen progress entry to `docs/state/progress-log.md` summarizing what shipped, what's blocked, and what's next.
- **Phase checkpoint:** at each phase end, tag the commit, generate a phase report under `docs/state/phase-N-report.md`, and pause for owner review before starting the next phase.

### 16.2 Hard stop conditions (require human input before proceeding)
- A design decision with ≥2 viable options where the choice has non-trivial downstream impact (e.g., choosing TLS library, runtime, on-disk layout deviation).
- A discovered compatibility blocker that would force divergence from VM's behavior.
- A perf regression > 10% that cannot be resolved within one working iteration.
- A dependency or license question.
- Any change to the scope, success criteria, or locked decisions in §2.
- A discovered bug in upstream VM that would propagate into EsMetrics.

### 16.3 Soft pause triggers (log + continue, surface at next checkpoint)
- A failing flaky test on one CI platform.
- A perf regression between 3% and 10%.
- A backport candidate from upstream VM.

### 16.4 State tracking
- `docs/state/backlog.md` — ordered list of next tasks within the current phase.
- `docs/state/progress-log.md` — append-only daily checkpoint log.
- `docs/state/blockers.md` — current open blockers, each with: discovered date, summary, options considered, requested decision.
- `docs/state/decisions.md` — ADR-style record of every architectural decision with date and rationale.

### 16.5 Communication
- Owner-facing summary: `docs/state/progress-log.md` is the source of truth. Each entry under 200 words.
- Issue tracker: GitHub Issues for items requiring owner attention; the agent files them and links from `blockers.md`.

---

## 17. Open items

### 17.1 Resolved (locked-in, no further action needed)

| Item | Decision | Date |
|---|---|---|
| Repository directory name | Rename `victoria-metrics-claude/` → `esmetrics/` at Phase 0 first action | 2026-05-28 |
| Remote / push URL | Defer; local-only for now | 2026-05-28 |
| Self-hosted perf runner | Owner-provided | 2026-05-28 |
| Fixture storage | No Git LFS — scenario-only in Git, fixtures regenerated on demand + cached (see §8.3) | 2026-05-28 |
| Final brand name | "EsMetrics" | 2026-05-28 |
| MSRV policy | Current Rust stable − 2 minor versions; bumped on a deliberate PR | 2026-05-28 |
| `unsafe` Rust policy | Pragmatic: allowed in hot paths with `// SAFETY:` docs; Miri in CI | 2026-05-28 |
| Windows read I/O | mmap on all platforms (Linux, macOS, Windows); defer-unlink coordination handles Windows quirks | 2026-05-28 |
| Backup format compat | Bidirectional drop-in: vmrestore can restore esm-backup output, esm-restore can restore vmbackup output | 2026-05-28 |
| SD backends for v1.0 | Tier 1: `static_configs` + `file_sd_configs` only; others deferred to v1.x | 2026-05-28 |
| zstd implementation | `zstd` crate (bindings to facebook/zstd C lib), pinned to the version VM v1.144.0 vendors; byte-identical part output | 2026-05-28 |
| CLI flag naming | Compat shim: accept both VM-style `-storageDataPath` AND Rust-idiomatic `--storage-data-path` | 2026-05-28 |
| v1.0 distribution channels | GitHub releases only (signed tarballs + sha256 + cosign attestations). OCI images, deb/rpm, Homebrew, MSI deferred to v1.x | 2026-05-28 |
| Code signing | Owner provides Apple Developer ID + Windows EV cert; CI signs via GitHub Actions secrets; macOS notarization in pipeline | 2026-05-28 |
| Repository visibility | Public on GitHub from day one (Phase 0 push). Apache 2.0 license, public progress logs, public decisions | 2026-05-28 |

### 17.2 Open — still needed

1. **Phase 0 kickoff timing.** All blocking questions resolved; ready to start whenever you give the go.

---

## 18. Document control

- **Owner:** project owner.
- **Maintainer:** the EsMetrics agent.
- **Source of truth:** this file, committed to repo root.
- **Update policy:** any change to §1–4 requires owner approval; §5 onward can be revised by the maintainer with a logged ADR.
- **Initial revision:** 2026-05-28.
- **Reference pin:** VictoriaMetrics v1.144.0.
