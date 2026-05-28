# Backlog

Ordered list of remaining work toward full VictoriaMetrics v1.144.0 parity.
Items at the top are unblocked and claimable. Regenerated to reflect the
post-MVP state of the workspace.

## Done (MVP-level across every phase)

- ✅ Phase 0 — Foundation (8/8 sub-tasks)
- ✅ Phase 1A.1–1A.6 — Mergeset format spec, types, codec, file I/O
- ✅ Phase 1B (lossless) — `marshal_int64_array` with all 6 marshal-types
- ✅ Phase 1C — Time-series part format + read/write roundtrip
- ✅ Phase 1D (MVP) — Persistent `index.json` sidecar for metric_name↔TSID
- ✅ Phase 1E (MVP) — Storage open/ingest/flush/search/shutdown
- ✅ Phase 2 (partial) — Text exposition + Prometheus remote-write (snappy+protobuf)
- ✅ Phase 3 (MVP+) — PromQL: lexer, parser w/ full precedence, 11 aggregations,
  32 functions, vector binops, range queries
- ✅ Phase 4 (MVP) — esm-single with Prometheus-compatible HTTP API
- ✅ Phase 5 (MVP) — esm-agent scrape+forward
- ✅ Phase 6 (MVP) — esm-alert threshold rules + Alertmanager v2
- ✅ Phase 7 (MVP) — esm-auth bearer-token reverse proxy
- ✅ Phase 8 (MVP) — esm-backup local snapshot + restore
- ✅ Phase 9 (partial) — CI workflows scaffolded

## Priority-ordered remaining work

### Tier A — Required for credible single-node parity

- [x] **A1. Background merger** ✅ (size-based heuristic in `maybe_merge_small_parts`;
      level scheme still pending)
- [x] **A2. PromQL `on`/`ignoring`/`group_left`/`group_right` vector matching** ✅
- [x] **A3. `histogram_quantile()` with linear interpolation** ✅
- [x] **A4. PromQL `@` modifier and `offset`** ✅
- [x] **A5. PromQL subqueries** ✅ (instant flattening; range-vector
      semantics on subquery output for rate/over_time is a follow-up)
- [x] **A6. Storage TSID-keyed scan** ✅
      (`next_block_header`/`read_data_block_for` skip payload decompression
      on non-matching blocks; `seek_to_tsid` binary-searches the metaindex.
      Bench: 4.0–4.3ms → 1.5–1.8ms = ~2.5x on every PromQL op)
- [x] **A7 (part 1). End-to-end PromQL bench** ✅
      (`crates/esm-promql/benches/promql_eval.rs`: selector, sum, sum by,
      rate, topk on 50 series × 200 samples). VM head-to-head still
      pending — gated by the conformance harness.

### Tier B — Filling in protocol breadth

- [x] **B1. Influx line protocol v1 + v2** ✅
- [x] **B2. Graphite plaintext + HTTP** ✅
- [x] **B3. OpenTSDB telnet + HTTP** ✅
- [x] **B4. DataDog series API** ✅
- [x] **B5. NewRelic events API** ✅
- [x] **B6. OpenTelemetry metrics (OTLP)** ✅ (gauge + sum only;
      histograms deferred)
- [x] **B7. Native VM protocol (`/api/v1/import/native`)** ✅
      (full VM portable block decoder + escape-decoded
      `MetricName.Marshal` form. Round-trip unit test confirms a
      VM-shape payload produces the expected `metric{k="v"}` series.)
- [x] **B8. JSON line import (`/api/v1/import`)** ✅
- [x] **B9. CSV import** ✅

### Tier C — Operator-grade vmagent / vmalert / vmauth

- [x] **C1. Prometheus relabel engine wired into esm-agent** ✅
      (`metric_relabel_configs` YAML compiled into the existing
      esm-scrape engine; per-target relabel chain applied after scrape
      before forward. All 11 actions available via the engine.)
- [x] **C2. Persistent disk queue in esm-agent** ✅ (file-per-entry queue;
      vmagent `persistentqueue` byte-layout compat deferred)
- [x] **C3 (Tier 1). SD backends**: `static_configs` + `file_sd_configs` ✅
      (tiny built-in glob; YAML and JSON entries). kubernetes/consul/cloud
      SD backends remain post-v1.0 per the original commitment.
- [x] **C4. Local PromQL evaluator in esm-alert** ✅ (`--local-data-path`
      opens the data dir directly and routes through esm-promql)
- [x] **C5. esm-alert state machine + disk persistence + recording rules** ✅
      (`for:` / `keep_firing_for:`; `--state-file` persists across
      restarts via atomic write+rename; `recording_rules` per group
      eval → text exposition → `--remote-write-url`.)
- [x] **C6 (subset). vmauth-compatible `auth.yml`** ✅
      (multi-user with Basic + Bearer, url_map regex routing, proper
      CIDR IP filters for v4 + v6). Retry policy + TLS termination
      remain post-MVP.

### Tier D — PromQL function library completion

- [x] **D1. Time-of-day functions** ✅ (`year`, `month`, `day_of_month`,
      `day_of_week`, `day_of_year`, `days_in_month`, `hour`, `minute`)
- [x] **D2. Prediction** — `predict_linear`, `deriv`, `holt_winters` ✅
- [x] **D3. Label manipulation** ✅ (`label_replace`, `label_join`;
      `label_lower` / `label_upper` are MetricsQL extensions, not core)
- [x] **D4. Histogram functions beyond `histogram_quantile`** ✅
      (classical-histogram interpretation: `histogram_sum`,
      `histogram_count`, `histogram_avg`, `histogram_fraction`,
      `histogram_stddev`, `histogram_stdvar`. Each function looks up
      the sibling `_sum` / `_count` / `_bucket{le=…}` series by name.
      Pure native-histogram-sample storage is deferred — it requires
      a new sample type cutting across every layer and isn't required
      for v1.0 parity since classical histograms are still the
      dominant deployment.)
- [x] **D5. `changes`, `resets`, `absent`, `absent_over_time`** ✅
- [x] **D6. `count_values` aggregator** ✅
- [x] **D7. `vector` / `scalar`** ✅
- [x] **D8 (mini-corpus). 15-case PromQL regression suite** ✅
      (`crates/esm-promql/tests/promql_corpus.rs`; covers selector,
      scalar arith, sum/min/max/avg/count, topk, abs, vector, bool
      comparison, filter, label_replace). Full upstream `promqltest`
      corpus remains a follow-up.

### Tier E — Storage depth (vs MVP)

- [x] **E1. Binary indexdb (mergeset-light)** ✅
      (sorted binary `index.bin` with magic `ESMIDX01`, LE u64 metric_id +
      LE u32 name_len + raw name; replaces the JSON sidecar. Atomic
      write+rename, fsync-dir. Legacy `index.json` is auto-migrated on
      first save. Two storage unit tests:
      `binary_index_roundtrip_after_reopen` and
      `legacy_json_index_is_migrated`. The full mergeset (multi-part
      with on-disk merge across generations + tag_pair posting lists) is
      a post-v1.0 follow-up that doesn't change this format.)
- [x] **E2. Lossy precision modes** for `marshal_int64_array` ✅
      (precision_bits in 1..63 honored via per-value magnitude-bit
      masking; quantized values flow through the standard delta/zstd
      pipeline). Byte-level VM `marshalInt64NearestDelta` parity needs
      VM's trailing-zero state machine — separate sub-task.
- [x] **E3 partial. SIMD-gated `compute_deltas`** ✅
      (AVX2 path on x86_64 + NEON path on aarch64 + scalar fallback;
      runtime feature detection on x86_64. Correctness verified by
      the existing codec property tests. Bench shows no measurable
      win because zstd decompression dominates — the SIMD wiring is
      kept for future codec hot paths.)
- [x] **E4. Retention enforcement** ✅ (conservative whole-part drop;
      partial-part rewriting deferred)
- [x] **E5. Snapshot mechanism** ✅ (hard-link parts +
      `/snapshot/create|list|delete` HTTP endpoints)
- [x] **E6. Codec self-inverse + differential fuzz vs VM** ✅
      (`crates/esm-compress/tests/codec_properties.rs`: 200 proptest
      cases prove lossless round-trip + lossy idempotence.
      `crates/esm-protocols/tests/vm_differential_codec.rs`: 3 cases ×
      32 random samples are encoded via `native_vm::encode`, POSTed
      to a live VM v1.144.0 container, and read back via
      `/api/v1/export` with byte-perfect equality.)
- [x] **E7. Byte-identity tests against VM-produced fixtures** ✅
      (real VM v1.144.0 container exports `/api/v1/export/native`;
      committed fixture at
      `crates/esm-protocols/tests/fixtures/vm-native-http-requests.bin`
      decodes via `native_vm::parse` with values intact. Decoder now
      honors VM's per-block decimal `scale` field.)

### Tier F — esm-single completeness

- [x] **F1. CLI-flag compat shim** ✅ (camelCase↔kebab-case translation in
      `parse_cli_with_vm_compat`)
- [x] **F2. Common VM CLI flag surface** ✅ (`-retentionPeriod`,
      `-maxInsertRequestSize`, `-search.maxQueryDuration`,
      `-search.maxSeries`, `-http.maxGracefulShutdownDuration`,
      `-loggerLevel`, `-selfScrapeInterval`, `-tlsCertFile/-tlsKeyFile`,
      `-httpAuth.password` accepted. Some are no-op compat with a
      startup warning; full ~80-flag surface is incremental.)
- [x] **F3. `/api/v1/series`, `/api/v1/labels`, `/api/v1/label/<n>/values`,
      `/api/v1/status/{buildinfo,runtimeinfo,flags,tsdb}`,
      `/api/v1/targets`** ✅
- [x] **F4. Self-monitoring `/metrics`** ✅ (Prometheus text exposition;
      VM-style metric names is a polish item)
- [x] **F5. vmui placeholder embedding** ✅ (esm_vmui::asset + esm-single
      `/vmui` routes serve a one-page landing screen; real upstream vmui
      bundle download via build.rs is the follow-up)
- [x] **F6. systemd unit + launchd plist + Windows service installer** ✅
      (`packaging/{systemd,launchd,windows}/`)
- [x] **F7-agent. `promscrape.config` YAML support in esm-agent** ✅
      (esm-single is vmsingle-style flag-only by design)
- [x] **F8. Graceful shutdown drain** ✅ (axum graceful + explicit
      flush+fsync on shutdown)

### Tier G — Backup (bidirectional vmbackup compat)

- [x] **G1. vmbackup directory format** ✅ (it's the snapshot tree
      verbatim + `backup_metadata.ignore` + `backup_complete.ignore`)
- [x] **G2. esm-backup writes vmbackup-format markers** ✅
- [x] **G3. esm-backup restores from a vmbackup directory** ✅
      (manifest-driven if `MANIFEST.json` present, else tree-walk that
      skips marker files; drops a `restore-in-progress` marker during
      the copy)
- [x] **G4 scaffold. ObjectStore trait + local backend** ✅
      (`esm-objstore::{ObjectStore,LocalFsStore,open_target}` with a
      `file://` URL scheme; `s3://` / `gs://` / `azure://` return
      `UnsupportedScheme` until the cloud backends land. Trait shape
      mirrors the upstream `object_store` crate so a future drop-in
      stays bounded.)
- [x] **G5. Incremental backups (manifest-driven) + sha256 verify** ✅
      (hardlink dedup against `--prev` manifest; sha256 computed at
      backup time and verified on restore)
- [x] **G6. Snapshot deletion when older than retention** ✅
      (`--snapshot-retention-secs`, mtime-based sweeper)

### Tier H — Cross-cutting / quality / release

- [x] **H1. Replaced deprecated `serde_yaml` → `serde_yaml_ng`** ✅
- [x] **H2. Miri job in CI** ✅ (esm-promql + esm-compress; esm-storage
      uses mmap+file_lock and is intentionally out of scope)
- [x] **H3. CI matrix musl + aarch64-gnu** ✅ (linux-x86_64-musl row in
      test matrix; aarch64-unknown-linux-gnu cross-build job. Native
      arm64 test execution still needs a self-hosted runner)
- [x] **H4. Conformance harness `run` subcommand** ✅
      (spawns upstream `victoriametrics/victoria-metrics:v1.144.0` via
      docker + esm-single binary, replays ingest steps against both,
      diffs query responses with `semantic_set` or `exact_text`.
      **5 scenarios passing against a live VM container today**:
      smoke, sum_aggregation, series_metadata, labels_endpoint,
      binary_arithmetic.)
- [x] **H5. Round-trip test: VM-writes → esm-reads** ✅
      (integration test in `esm-protocols::tests::native_vm_roundtrip`:
      builds a VM-shape payload via `MarshalPortable` +
      `MetricName.Marshal`, decodes through `native_vm::parse`,
      ingests via esm-storage, and queries back. 5-sample / 2-series
      corpus — wider corpora are a CI volume bump.)
- [x] **H6. Round-trip test: esm-writes → VM-reads** ✅
      (`crates/esm-protocols/tests/vm_writeback.rs`: encodes via
      `native_vm::encode`, POSTs to a live VM container, queries back
      via `/api/v1/export`, and asserts every sample value survives.
      Skipped when `VM_URL` is unset.)
- [x] **H7. PGO build profile** ✅ (`release-pgo-generate` +
      `release-pgo-use` Cargo profiles, runbook at
      `docs/build-pgo.md`). Reproducible-build pipeline still pending.
- [x] **H8. Apple Developer ID + Windows EV cert plumbing in
      release.yml** ✅ (workflow scaffold gated on
      `secrets.APPLE_DEVELOPER_ID_CERT` and `secrets.WINDOWS_EV_CERT`;
      activates once the owner uploads the certs).
- [x] **H9. `cosign` sign-blob + SLSA build provenance** ✅
      (keyless cosign on every archive plus
      `actions/attest-build-provenance` in `release.yml`).
- [x] **H10. Parser property tests** ✅ (proptest cases for PromQL +
      10 ingest parsers — don't-panic on arbitrary bytes; well-formed
      queries parse). cargo-fuzz nightly job is the follow-up.
- [x] **H11. Soak test rig** ✅ (`cargo xtask soak --url <esm-single>
      --duration-secs N --series N --writes-per-sec N
      --queries-per-sec N`. Validated locally: 30s × 100 series ×
      5 000 writes/s + 5 queries/s = 147 500 writes, 150 queries,
      zero errors. The 30-day wall-clock run is operator-managed per
      `docs/soak-test.md`.)
- [x] **H12. `esm-ctl` MVP** ✅ (inspect + export subcommands;
      VM-direction migrators still pending)
- [x] **H13. Public GitHub publish — ready for operator push** ✅
      (git initialised on `main`, `.gitignore` / LICENSE / NOTICE /
      CREDITS / README / CI workflows all staged. Operator runs the
      4-step recipe in `docs/publish-to-github.md`.)
- [x] **H14. Phase 1–9 status report** ✅ (consolidated into
      `docs/state/phase-status.md`; one document beats nine for
      ergonomics and stays in sync with the backlog)
- [x] **H15. `docs/migration-from-vm.md`** ✅

## Completed phases

- ✅ **Phase 0 — Foundation.** See [`phase-0-report.md`](./phase-0-report.md).
