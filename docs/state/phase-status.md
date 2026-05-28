# Phase status (1 – 9)

A single document replaces the per-phase `phase-N-report.md` series. The
authoritative source for live state is
[`backlog.md`](./backlog.md); this file gives the narrative.

Phase 0 has its own dedicated [`phase-0-report.md`](./phase-0-report.md)
because the foundation decisions are durable.

---

## Phase 1 — Storage engine

**Status:** MVP+ shipped, depth work pending.

### Done
- 1A.1–1A.6 mergeset format spec, types, codec, file I/O.
- 1A.7 size-based background merger (`maybe_merge_small_parts`).
- 1B lossless `marshal_int64_array` (all 6 marshal types).
- 1C time-series part format + read/write roundtrip
  (block_stream_writer / block_stream_reader).
- 1D persistent metric_name ↔ TSID sidecar (`index.json`, schema_v1).
- 1E open / ingest / flush / search / shutdown.
- **TSID-keyed scan** (`next_block_header` + `seek_to_tsid`); 2.5x
  PromQL latency win on the bench.
- **Retention** by whole-part drop.
- **Snapshot** via hardlink with `/snapshot/{create,list,delete}` HTTP
  endpoints and a background snapshot-retention sweeper.

### Pending
- 1A.8 level-based merge scheme (current heuristic is size-based only).
- 1D.next mergeset-backed indexdb (label-pair → TSIDs, metric_id → name).
- 1B.lossy precision_bits mode in `marshal_int64_array`.
- SIMD codecs (AVX2 + NEON behind cfg gates).
- Native VM binary import — `Block.UnmarshalPortable` for Gorilla-style
  encoded ts+v arrays. Deferred.
- Differential fuzzing vs VM and byte-identity tests — gated on the
  conformance harness's Docker driver maturing.

---

## Phase 2 — Ingest protocols

**Status:** Broad coverage; native VM binary deferred.

Implemented: Prometheus text exposition, Prom remote-write (snappy +
protobuf), Influx line v1/v2, Graphite plaintext + HTTP, OpenTSDB telnet
+ HTTP, DataDog Series API, JSON line, CSV import, NewRelic Metric API,
OTLP protobuf (gauge + sum data points only).

Pending: `/api/v1/import/native` (VM binary block stream — see
phase 1's note on `Block.UnmarshalPortable`); OTLP histograms (gated on
storage-layer histogram representation).

---

## Phase 3 — PromQL

**Status:** Broad surface; promqltest corpus pass pending.

Implemented: lexer, precedence-climbing parser with `on/ignoring/
group_left/group_right` vector matching, `@` modifier, `offset`
modifier, subqueries (instant flattening), 11 aggregations including
`topk`/`bottomk`/`quantile`/`count_values`, 40+ functions including the
rate / over_time family, `histogram_quantile` with linear interpolation,
`predict_linear` / `deriv` / `holt_winters`, full label-manipulation
(`label_replace`, `label_join`), time-of-day family, `changes` /
`resets` / `absent` / `absent_over_time`.

Pending: full Prometheus `promqltest` corpus pass — needed for a "PromQL
parity" claim. Range-vector semantics on subquery output for
rate/over_time is also a follow-up. Histogram family beyond
`histogram_quantile` is gated on native-histogram storage.

End-to-end criterion bench at
`crates/esm-promql/benches/promql_eval.rs` — selector / sum / sum by /
rate / topk on 50 series × 200 samples, 1.5–1.8ms each after the A6
TSID-keyed scan.

---

## Phase 4 — esm-single

**Status:** Production-shaped MVP.

HTTP routes implemented include every Prometheus/VM ingest endpoint
listed under Phase 2, plus the read-side: `/api/v1/query`,
`/api/v1/promql`, `/api/v1/promql_range`, `/api/v1/series`,
`/api/v1/labels`, `/api/v1/label/:name/values`,
`/api/v1/status/{buildinfo,runtimeinfo,flags,tsdb}`,
`/api/v1/targets`, `/snapshot/{create,list,delete}`, `/vmui`, `/health`,
`/metrics`.

VM-style CLI flag compat shim (`parse_cli_with_vm_compat`) accepts
single-dash camelCase flags. Flag surface includes
`-retentionPeriod`, `-maxInsertRequestSize`,
`-search.maxQueryDuration`, `-search.maxSeries`, `-loggerLevel`,
`-selfScrapeInterval`, `-tlsCertFile/-tlsKeyFile`,
`-httpAuth.password`. TLS termination and the search limits are
accepted with a startup-warning compat shim per ADR-001 #15.

Graceful shutdown: SIGTERM → axum graceful → flush + fsync → exit.

Pending: full ~80 VM flag surface (currently common subset only),
real TLS termination, search-limit enforcement, request-timeout
middleware.

---

## Phase 5 — esm-agent

**Status:** MVP+.

Scrape loop, file-per-entry disk queue at `--queue-dir` with retry on
next tick, vmagent-style `promscrape.config` YAML loader, `file_sd`
service discovery with a built-in glob, `static_configs` as before.

Pending: kubernetes/consul/EC2/GCE/Azure/DNS/HTTP service discovery
(Tier-1 commitment was static + file only). Multiple remote-write
targets per agent. vmagent persistent-queue byte-layout compat.

---

## Phase 6 — esm-alert

**Status:** MVP+ with embedded PromQL.

YAML rule loading, threshold evaluator with `for:` /
`keep_firing_for:` state machine, Alertmanager v2 POST.
`--local-data-path` mode bypasses HTTP and routes through esm-promql
directly.

Pending: recording rules + on-disk state persistence. Multiple
datasource fallback. UI for current alert state.

---

## Phase 7 — esm-auth

**Status:** MVP.

vmauth-compatible YAML (`users[*]` with bearer or basic, `url_map`
with src_path regex, IP filters skeleton). Single upstream-URL CLI
mode for trivial deployments.

Pending: TLS termination, retry policy, header rewriting beyond the
canonical Authorization passthrough.

---

## Phase 8 — esm-backup

**Status:** Bidirectional vmbackup compat at marker-file level.

`backup` writes the data tree unchanged + `backup_metadata.ignore` +
`backup_complete.ignore` markers, so the directory is recognisable to a
VM-side `vmrestore`. `restore` reads either our `MANIFEST.json` form
or a tree-only vmbackup directory, drops a `restore-in-progress`
marker during the copy.

`backup --prev <previous-backup>` does incremental dedup via hardlinks
against the previous manifest.

`snapshot` subcommand calls `/snapshot/create` on a running
esm-single, then backs the snapshot directory up.

Pending: SHA-256 content checksum in the manifest. S3 / GCS / Azure
backends via `object_store`.

---

## Phase 9 — Quality & release

**Status:** Scaffolded; release-blocker items pending operator action.

### Done
- CI: `fmt`, `clippy --workspace --all-targets -- -D warnings`,
  `cargo-deny`, multi-OS test matrix (linux x86_64-gnu + x86_64-musl,
  macOS x86_64 + aarch64, Windows MSVC).
- aarch64-unknown-linux-gnu **cross-build** job (test execution still
  needs a self-hosted arm runner).
- Nightly Miri job for esm-promql + esm-compress.
- `cargo-audit` and `cargo-deny` (full) in the nightly workflow.
- Property tests (proptest) for the PromQL parser and every ingest
  parser — don't-panic on arbitrary bytes + well-formed inputs parse.
- Conformance harness `run` subcommand: spawns upstream
  `victoriametrics/victoria-metrics:<tag>` via docker + esm-single
  binary, replays ingest steps against both, diffs query responses.
  Operator-driven; the GitHub Actions integration lives in the
  nightly workflow but is conditional on a Docker host.
- `esm-ctl` MVP (`inspect`, `export`).
- Migration guide at [`docs/migration-from-vm.md`](../migration-from-vm.md).
- Service integration templates at `packaging/`.
- Replaced deprecated `serde_yaml` with `serde_yaml_ng`.

### Pending (gated on operator)
- Owner-provided Apple Developer ID + Windows EV cert plumbing.
- cosign + SLSA L3 provenance for release artefacts.
- PGO build profile + reproducible build pipeline.
- Public GitHub remote (`ADR-001 #18`).
- 30-day soak test under synthetic load.
- nightly `cargo fuzz` smoke run wired to actual cargo-fuzz targets
  (placeholder workflow exists).

---

## What "full parity" means today

| Surface             | Status                                                            |
|---------------------|-------------------------------------------------------------------|
| Wire protocols      | All major ingest formats except native VM binary                  |
| PromQL              | Full functional surface; promqltest corpus pass pending           |
| HTTP read API       | Full Prometheus/VM compatible set                                 |
| HTTP write API      | All listed above                                                  |
| CLI flag surface    | Common ~25 vmsingle flags; full ~80 incremental                   |
| Storage perf        | 2.5× win on PromQL after A6; mergeset indexdb still pending       |
| Disk format         | EsMetrics-native; **not** byte-compatible with VM parts           |
| Backup format       | vmbackup marker-compatible; vmrestore should accept our output    |
| Service integration | systemd, launchd, Windows SCM templates shipped                   |
| Conformance         | Harness drives both sides; corpus is currently one smoke scenario |

The remaining gap to **byte-level format parity** with upstream VM is
the largest single item — it requires implementing VM's mergeset
indexdb and `Block.UnmarshalPortable` for parts. Until then, EsMetrics
is a **functionally compatible** alternative for greenfield
deployments but cannot read existing VM data directories in-place.
