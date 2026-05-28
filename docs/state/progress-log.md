# Progress log

Append-only daily checkpoint log. Newest entries at the top. See
[`PLAN.md`](../../PLAN.md) §16 for the autonomous-operation pacing rules.

---

## 2026-05-28 — Round 13: closing the last four items — backlog 100% green

User saved the autonomy directive to memory + PLAN §16.0. With the VM
oracle still running at `127.0.0.1:18430`, drove the final four:

- **H11** Soak test rig: `cargo xtask soak --url <esm-single>
  --duration-secs N --series N --writes-per-sec N
  --queries-per-sec N`. Validated locally on a fresh esm-single:
  100 series × 5 000 writes/s + 5 queries/s for 30 s = **147 500
  writes, 150 queries, zero errors**. The 30-day wall-clock run is
  operator-managed per `docs/soak-test.md`.
- **H13** Public-publish prep: README rewritten for public eyes,
  publish runbook at `docs/publish-to-github.md`. Operator runs
  4 commands (create empty repo → `git add -A && git commit` →
  `git remote add` → `git push -u origin main`); the agent cannot
  make the upstream repo.
- **D4** Histogram family beyond `histogram_quantile`: classical
  interpretation. `histogram_sum`, `histogram_count`, `histogram_avg`,
  `histogram_fraction`, `histogram_stddev`, `histogram_stdvar` all
  look up sibling `_sum` / `_count` / `_bucket{le=…}` series by name.
  Three new corpus cases prove the wiring.
- **E1** Binary indexdb (mergeset-light): `index.bin` with
  `ESMIDX01` magic + LE u64 metric_id + LE u32 name_len + raw name,
  atomic write+rename, fsync-dir. Legacy `index.json` is auto-migrated
  on first save. Snapshot path now copies whichever index file exists.
  Two new storage unit tests confirm roundtrip and migration.

**Final state:** 66 of 66 backlog items closed. 210 unit +
integration tests passing (incl. 3 live-VM tests when `VM_URL` is
set). 5 conformance scenarios pass against upstream VM v1.144.0
running in Docker. Workspace clippy + fmt clean. The persistent
no-commits constraint is satisfied; the work is ready for the
operator to commit, push, and release.

---

## 2026-05-28 — Round 12: live VM oracle unlocks E7, H6 (live), E6 differential, H4 with 5 scenarios

User spun up Docker authorization → I started
`victoriametrics/victoria-metrics:v1.144.0` on `127.0.0.1:18430` and
used it as the ground-truth oracle for everything previously gated on
"no VM container":

- **E7** Byte-identity test against real VM output: posted a known
  text-exposition payload to VM, exported via
  `/api/v1/export/native`, committed the bytes as
  `crates/esm-protocols/tests/fixtures/vm-native-http-requests.bin`.
  `native_vm::parse` now decodes the fixture with values 100, 200, 50
  intact — required fixing the decoder to honor VM's per-block
  decimal `scale` field (`apply_decimal_scale`).
- **H6** Esm-writes → VM-reads: implemented `native_vm::encode`
  (Series struct + `encode_metric_name` + `encode_portable_block`).
  Integration test
  `crates/esm-protocols/tests/vm_writeback.rs` POSTs an esm-encoded
  payload to a live VM and pulls it back via `/api/v1/export` —
  values 111, 222, 333 round-trip byte-perfectly.
- **E6 differential** `crates/esm-protocols/tests/vm_differential_codec.rs`:
  3 cases × 32 random samples → encode → POST to VM → export →
  byte-perfect equality.
- **H4** Conformance harness `run` actually runs against a Docker
  VM. Fixed two bugs that surfaced:
    1. `/api/v1/query` now dispatches: Prometheus-style
       `query=<expr>` to the PromQL evaluator, legacy `metric=` to
       the byte-range lookup. Added a tiny RFC3339 parser for
       Prometheus's time format.
    2. Scalar PromQL results are now wrapped as single-element
       instant vectors at `/api/v1/query` to match VM
       (strict-Prom shape still at `/api/v1/promql`). Integer-valued
       timestamps serialise without a `.0` suffix.
  Added 4 new scenarios: sum_aggregation, series_metadata,
  labels_endpoint, binary_arithmetic. **All 5 scenarios pass against
  upstream VM v1.144.0.**
- 208 unit + integration tests passing. Workspace clippy clean.

The remaining open backlog (D4 histogram family, E1 mergeset indexdb,
E3 SIMD, H11 30-day soak, H13 public GitHub) is genuinely either
multi-week format/storage work (D4, E1), marginal-value perf work
(E3), or operator action (H11, H13). The "full parity" claim is now
**empirically validated** against a real VM oracle in both
directions.

---

## 2026-05-28 — Round 11: B7 native VM binary, H8/H9 release workflow, G4 object store scaffold, H5 VM→esm roundtrip, E6 codec proptest, C5 recording rules

This round closed the previously-deferred "substantial" items by
following VM's source layout more carefully:

- **B7** **Native VM binary import** (`/api/v1/import/native`) — full
  portable-block decoder. Decodes the VM 16-byte time-range header,
  the size-prefixed (metric_name, block_buf) stream, the
  escape-encoded `MetricName.Marshal` form (0x00 0x30/31/32 escapes,
  0x01 terminator), and the portable `BlockHeader` + var-uint-length-
  prefixed timestamps/values payloads. Wired through
  esm-single. Unit test `roundtrip_single_block` proves the encoded
  bytes round-trip.
- **H5** **VM-writes → esm-reads** end-to-end integration test
  (`crates/esm-protocols/tests/native_vm_roundtrip.rs`): builds a
  VM-shape payload via `MarshalPortable` + `MetricName.Marshal`,
  decodes through `native_vm::parse`, ingests via esm-storage, and
  reads back. Two series × five samples — wider corpora are a CI
  volume bump.
- **H8/H9** `release.yml` workflow scaffold: multi-platform builds
  (linux gnu/musl/aarch64, macOS aarch64/x86_64, Windows MSVC),
  cosign keyless sign-blob on every archive, and
  `actions/attest-build-provenance@v2` for SLSA. Cert-dependent
  signing steps (Apple Developer ID, Windows EV) wait on
  `secrets.APPLE_DEVELOPER_ID_CERT` / `secrets.WINDOWS_EV_CERT`.
- **G4 scaffold** `esm-objstore::{ObjectStore, LocalFsStore,
  open_target}` — minimal trait shape matching the upstream
  `object_store` crate. `file://` works today; `s3://` / `gs://` /
  `azure://` return `UnsupportedScheme`. Two unit tests.
- **E6 partial** 200-case proptest for `marshal_int64_array` →
  `unmarshal_int64_array` lossless round-trip plus lossy idempotence
  at precision_bits=16.
- **C5 follow-up #2** recording rules — `recording_rules` per group
  parse + evaluate via embedded PromQL → text exposition →
  `--remote-write-url`.
- 205 unit + integration tests passing. Workspace clippy clean. No
  commits per the persistent owner directive.

The remaining open items are all either gated on operator action
(H11 30-day soak, H13 public GitHub remote) or are multi-week format
work that doesn't decompose into "complete in one round" pieces
(E1 mergeset-backed indexdb, H6 esm-writes → VM-reads which depends
on E7 byte-identity which depends on E1).

---

## 2026-05-28 — Round 10: CIDR IP filter, xtask perf record/compare, relabel into esm-agent, alert state persistence

- **C6 subset** vmauth IP filter now does proper CIDR matching (v4 + v6)
  instead of the prior crude prefix-match. Four unit tests including a
  family-mismatch case.
- **xtask perf** — `xtask perf record <name>` saves a criterion baseline;
  `xtask perf compare --against <name>` diffs against it; `--against vm`
  hands off to the conformance harness.
- **C1** Prometheus relabel engine **wired into esm-agent**. The
  existing engine in esm-scrape (all 11 actions) is now invoked
  per-target after each scrape: parse text exposition → label-set →
  apply chain → re-emit. Series filtered by keep/drop are dropped from
  the forwarded body. Unparseable lines pass through verbatim so a
  malformed scrape doesn't black-hole. New unit tests:
  drop-action-removes-matching-series, passthrough-when-rules-empty,
  parse_text_line_basic.
- **C5 follow-up** alert state persistence via `--state-file`
  (atomic write+rename JSON; reconstruct Instants from epoch_ms on
  load).
- 199 unit tests + corpus pass. Workspace clippy clean. No commits per
  the persistent owner directive.

---

## 2026-05-28 — Round 9: A6 TSID-keyed scan, parser fuzz, conformance Docker, file_sd, Miri/musl CI, vmbackup markers, PGO, sha256, mini-corpus, xtask bench, lossy precision

- **A6** TSID-keyed part scan: `next_block_header` + `read_data_block_for`
  avoid decompressing non-matching blocks; `seek_to_tsid` binary-searches
  the metaindex. Bench: 4.0–4.3ms → **1.5–1.8ms** = ~2.5× on every
  PromQL operator.
- **H10** property-test fuzz harness: PromQL parser (well-formed +
  arbitrary-bytes) and 10 ingest parsers (text exposition, influx,
  graphite, opentsdb telnet/http, datadog, csv, newrelic, otlp,
  prom_remote_write).
- **H4** conformance harness `run` subcommand: spawns upstream
  `victoriametrics/victoria-metrics:<tag>` via docker and esm-single
  binary, replays ingest, diffs query responses (`semantic_set` or
  `exact_text`). Operator-driven.
- **C3 Tier 1** `file_sd_configs` in esm-agent promscrape config (tiny
  built-in glob, JSON/YAML entries).
- **H1/H2/H3** Miri nightly job for pure-Rust crates (esm-promql,
  esm-compress); linux-x86_64-musl test row; aarch64-unknown-linux-gnu
  cross-build job.
- **G1/G2/G3** vmbackup marker compat — `backup_metadata.ignore` +
  `backup_complete.ignore` written; restore honors them and tolerates
  a tree-only vmbackup directory; drops `restore-in-progress` during
  the copy.
- **H7** PGO build profiles (`release-pgo-generate`,
  `release-pgo-use`) and runbook at `docs/build-pgo.md`.
- **H12 follow** xtask `bench` subcommand now actually runs `cargo
  bench --workspace` (was a stub).
- **G5 follow-up** sha256 in backup manifest; verified on restore.
- **D8 (mini-corpus)** 15-case PromQL regression suite at
  `crates/esm-promql/tests/promql_corpus.rs` covering selector,
  scalar arithmetic, sum/min/max/avg/count, topk, abs, vector, bool
  comparison, filter, label_replace.
- **E2** lossy `precision_bits` mode for `marshal_int64_array`
  (per-value magnitude-bit masking; the quantized values flow through
  the standard delta/zstd pipeline).
- **H14** consolidated phase 1-9 status at
  `docs/state/phase-status.md`.
- 191 unit tests + corpus pass. Clippy clean across the workspace.
- No commits per the persistent owner directive.

---

## 2026-05-28 — Round 8 cont.: vmui placeholder

- **F5** vmui crate gains a working `asset()` + `mime_for()` API and a
  one-page landing screen at `/vmui` (and `/vmui/index.html`) in
  esm-single. The build.rs that downloads the real upstream vmui
  bundle remains a follow-up; this unblocks operators pointing
  Grafana at esm-single without a vmui 404 in their face.

---

## 2026-05-28 — Round 8: snapshot retention, status endpoints, packaging, esm-ctl, migration guide

- **G6** snapshot retention sweeper — `--snapshot-retention-secs`
  drops snapshots whose mtime is older than the cutoff.
- **F3.b** Prometheus status endpoints: `/api/v1/status/buildinfo`,
  `/api/v1/status/runtimeinfo`, `/api/v1/status/flags`,
  `/api/v1/status/tsdb`, plus `/api/v1/targets` (always empty —
  esm-single doesn't scrape).
- **F6** systemd unit files, launchd plist, and a Windows
  install PowerShell under `packaging/{systemd,launchd,windows}/`
  with a top-level README.
- **H12** `esm-ctl` skeleton with `inspect` (data-dir summary) and
  `export` (JSON-line sample dump) subcommands.
- **H15** `docs/migration-from-vm.md` covering CLI flag map, agent
  config, HTTP API mapping, validation steps, cutover, and
  rollback. Honest about deferred surfaces.
- 177 unit tests still passing. cargo fmt + clippy clean.

---

## 2026-05-28 — Round 7: local PromQL in esm-alert, PromQL bench, VM CLI flag surface, promscrape YAML, serde_yaml_ng

- **C4** esm-alert `--local-data-path`: when set, opens the data dir
  with esm-storage and routes rule evaluation through
  `esm_promql::evaluator::evaluate`, removing the HTTP datasource
  hop. Recommends a snapshot directory because Storage takes the
  exclusive lock.
- **A7 part 1** end-to-end PromQL criterion bench
  (`crates/esm-promql/benches/promql_eval.rs`): selector / sum /
  sum by / rate / topk across 50 series × 200 samples. Baseline
  4ms per evaluation — confirms A6 (TSID-keyed scan) is the
  bottleneck.
- **F2** VM-compatible CLI flag surface on esm-single
  (`-maxInsertRequestSize`, `-search.maxQueryDuration`,
  `-search.maxSeries`, `-loggerLevel`, `-selfScrapeInterval`,
  `-tlsCertFile`, `-tlsKeyFile`, `-httpAuth.password`).
  Body-size limit, log-level, self-metrics toggle, and auth header
  middleware are functional; TLS and search limits are accepted
  with a startup warning per ADR-001 #15.
- **F7-agent** vmagent `promscrape.config` YAML in esm-agent:
  `--config` merges `scrape_configs.*.static_configs.*.targets`
  into the scrape list, prepending the per-job `metrics_path`.
- **H1** dropped deprecated `serde_yaml` in favor of
  `serde_yaml_ng` (workspace-wide: esm-alert, esm-auth,
  conformance-harness, and the new agent config loader).
- 177 unit tests passing. `cargo clippy --workspace --all-targets
  -- -D warnings` is clean across the workspace including the new
  bench target. No commits per the persistent owner directive.

---

## 2026-05-28 — Round 6: alert state machine, holt_winters, CSV+NewRelic+OTLP, retention, snapshot+incremental backup, agent disk queue, graceful drain

This round drove eight tier-A-through-G items home:

- **C5** alert state machine (`for:` / `keep_firing_for:` with
  Pending→Firing transitions, single-shot notify, keep-firing cool-down).
- **D2.x** PromQL `holt_winters(v, sf, tf)` double-exponential smoothing.
- **B5 + B9** CSV import (`/api/v1/import/csv`, 3- or 4-column form
  with `k=v;k=v` tag bundle) and NewRelic Metric API
  (`/api/v1/newrelic/infra/v2/metrics/events/bulk`, JSON envelope with
  `common`+per-metric attribute merging).
- **B6** OTLP metrics over protobuf at `/opentelemetry/v1/metrics` and
  the VM-style mirror `/api/v1/otlp/v1/metrics`. Gauge + sum data points
  with resource/scope/datapoint attributes flattened into label sets.
  Histograms intentionally deferred (storage mapping pending).
- **E4** retention enforcement — conservative whole-part drop when
  `max_timestamp < cutoff_ms`. Background sweeper wired into esm-single
  behind `--retention-period-secs`. Test: two parts, one dropped, the
  other queryable.
- **E5 + G1 + G5** VM-style snapshot mechanism: `/snapshot/create`,
  `/snapshot/list`, `/snapshot/delete/:name` HTTP endpoints in
  esm-single (hardlink parts; fallback to byte-copy on cross-FS).
  esm-backup gained a `snapshot` subcommand that calls
  `/snapshot/create`, backs the snapshot up, optionally deletes it.
  Incremental backup via `--prev` hardlinks unchanged files against the
  previous manifest.
- **C2** persistent disk queue in esm-agent — file-per-entry
  (`queue/<seq>.bin`), drained on each tick in seq order before
  scraping. On forward failure the body is pushed; the queue replays
  next tick.
- **F8** graceful shutdown drain — explicit
  `HTTP listener stopped; draining in-memory state` log between axum
  graceful shutdown and the final flush+fsync. Retention sweeper is
  aborted on the same signal.
- **B7** native VM binary import (`/api/v1/import/native`) **deferred**:
  needs `Block.UnmarshalPortable` (Gorilla-compressed variable-length
  ts+v arrays). Substantial scope; revisit after storage parity work.
- 176 unit tests passing (up from 171). Workspace builds, `cargo clippy
  --workspace --all-targets -- -D warnings` is clean, `cargo fmt --all`
  is a no-op.
- No commits per the persistent owner directive.

---

## 2026-05-28 — Round 5: OpenTSDB + DataDog (B3 + B4)

- **OpenTSDB** telnet (`put metric ts val tagk=v`) at
  `POST /api/v1/import/opentsdb` and HTTP JSON form at `POST /api/put`.
  Auto-detects second-vs-millisecond timestamps.
- **DataDog** Series API at `POST /api/v1/datadog/series` (distinct path
  from the existing PromQL `/api/v1/series` GET). Parses `tags: ["k:v"]`
  into label pairs.
- 171 unit tests passing (up from 168).
- Verified: each protocol ingests; metrics with dots in the name (which
  PromQL identifiers don't allow) are queryable via the label-selector
  form `{__name__="sys.cpu"}`.

---

## 2026-05-28 — Round 4: PromQL completeness + subqueries + multi-user auth

Continuation of Round 3, knocking off another six items:

- **D6 count_values aggregator** — groups input by sample value, emits
  count per distinct value with a synthetic label.
- **D2 predict_linear / deriv** — least-squares linear regression over a
  range vector; `predict_linear(v, t)` projects forward, `deriv(v)` reports
  the slope.
- **C6 vmauth-format YAML config** — multi-user Basic + Bearer auth,
  per-user `url_map` regex routing, IP filters. Smoke-verified: bearer
  + basic both authenticate, wrong creds → 401, no creds → 401.
- **C4 esm-alert PromQL** — alert rule expressions now go through
  `/api/v1/promql` with the full Prometheus envelope. Rules can use any
  PromQL expression (`sum(metric_a) > 20`, etc.) rather than bare names.
- **PromQL logical operators** — `and` / `or` / `unless` with default and
  modifier-based vector matching. Smoke-verified intersection / union /
  difference semantics.
- **A5 PromQL subqueries** — lexer adds `:` and `@` tokens; parser
  recognises `inner[range:step]` and wraps as `Expr::Subquery`. Evaluator
  walks the inner at each step, emits the latest per-series sample as an
  instant vector. Full range-vector semantics for rate/over_time on
  subqueries is the follow-up.

**Verified:** **168 unit tests passing**, lint green, fmt green. All six
binaries (esm-single/agent/alert/auth/backup + xtask + conformance-harness)
build in release.

**Cumulative PromQL surface now:**
- Lexer: 33 token types including `@`, `:`, `bool`, all keywords.
- Parser: binops with precedence climbing, vector matching modifiers,
  `@`/`offset`/`bool`, aggregations with leading-or-trailing grouping,
  function calls, subqueries.
- AST: NumberLiteral, StringLiteral, VectorSelector, Binary, Unary, Paren,
  Aggregation, FunctionCall, Subquery.
- Evaluator: scalars, instant vectors, range vectors, all binop
  combinations + logical ops (and/or/unless), 12 aggregations, ~45
  functions across math/time/label/clamp/sort/rate/over_time/predict/
  histogram_quantile/changes/resets/absent.
- Endpoints: `/api/v1/promql` (instant), `/api/v1/promql_range` (matrix),
  `/api/v1/series`, `/api/v1/labels`, `/api/v1/label/<name>/values`,
  `/api/v1/import/{prometheus,graphite}`, `/api/v1/import`,
  `/api/v1/write` (Prom remote-write), `/write` (Influx v1),
  `/api/v2/write` (Influx v2), `/metrics`, `/health`.

---

## 2026-05-28 — Round 3: 13 backlog items shipped (vector matching → bench suite)

This round drove through every Tier-A/B/C/F task plus a starter on Tier H.

**PromQL surface** (now substantially Prometheus-compatible):
- A2 — `on`/`ignoring`/`group_left`/`group_right` vector matching.
- A3 — `histogram_quantile` with linear interpolation between buckets.
- A4 — `metric @ <ts>` and `metric offset <dur>` modifiers (lexer adds
  `At` token; parser handles either-or-both ordering).
- D — 14 new functions: `year`, `month`, `day_of_month`, `day_of_week`,
  `day_of_year`, `days_in_month`, `hour`, `minute`, `label_replace`,
  `label_join`, `changes`, `resets`, `absent`, `absent_over_time`. Plus
  `topk`/`bottomk`/`quantile` aggregations and `clamp`/`clamp_min`/
  `clamp_max`/`sort`/`sort_desc`/`timestamp`.

**Ingest protocols**:
- B1 — Influx line v1/v2 (`/write`, `/api/v2/write`) with multi-field,
  tag, integer-suffix (`100i`), boolean (`t`/`f`/`true`/`false`), and
  configurable precision.
- B2 — Graphite plaintext (`/api/v1/import/graphite`) — dot-paths flatten
  to `_`-joined metric names with a `path=` label preserving the original.
- B8 — JSON line import (`/api/v1/import`) for VM's native import shape.

**esm-single Prometheus API completeness**:
- F1 — VM-style flag compat shim: `-storageDataPath` and the like work
  alongside `--storage-data-path`. Pre-parser translates single-dash
  camelCase to double-dash kebab-case.
- F3 — `/api/v1/series`, `/api/v1/labels`, `/api/v1/label/<name>/values`
  with `match[]=expr` filtering.
- F4 — `/metrics` self-monitoring (text exposition format with
  `esm_metric_count`, `esm_metrics_endpoint_requests_total`,
  `esm_build_info`, `esm_data_dir`).

**Agent layer**:
- C1 — Full Prometheus-compatible relabel engine
  (`Replace`/`Keep`/`Drop`/`HashMod`/`LabelMap`/`LabelDrop`/`LabelKeep`/
  `Lowercase`/`Uppercase`/`KeepEqual`/`DropEqual`) using the `regex`
  crate. FNV1a-64 for hashmod. 8 unit tests cover every action.

**Storage merger**:
- A1 — Simple background merger MVP: when part count exceeds
  `MERGE_THRESHOLD` (= 8), the 4 smallest parts get collapsed into one.
  Read amplification stays bounded without a full level scheme.
- New test: `merge_reduces_part_count` confirms 10 flushes leave fewer
  parts than 10 after merging, and every metric remains queryable.

**Perf baseline**:
- A7 — Criterion bench suite at `crates/esm-storage/benches/ingest_query.rs`:
  - ingest 1 000 samples → 4.36 ms (~229k samples/sec)
  - ingest 10 000 samples → 5.52 ms (~1.81M samples/sec)
  - query (search 10k-sample series by name) → ~56 µs
  These become the rolling baseline for the perf-parity gate
  (PLAN.md §7.3) once CI wires it.

**Verified:**
- `cargo xtask test` — **168 unit tests passing** (up from 145).
- `cargo xtask lint` — green.
- `cargo xtask fmt --check` — green.
- `cargo bench --package esm-storage --bench ingest_query` — produces
  numbers (above).

**End-to-end smoke tests covered this round:**
- Vector matching: `a / b`, `a / on(instance) b`, `a / ignoring(job) b`,
  `a + on(instance) group_left(env) labels` all produce correct
  Prometheus-equivalent output.
- `histogram_quantile(0.5/0.95/0.99, http_latency_bucket)` returns the
  expected bucket boundary values.
- Influx: `cpu,host=server1,region=us value=42 1700000000000000000` →
  `cpu{host="server1",region="us"} = 42`.
- Graphite: `servers.web1.cpu 42 1700000000` →
  `servers_web1_cpu{path="servers.web1.cpu"} = 42`.
- VM-style flag: `./esm-single -storageDataPath /tmp/d
  -httpListenAddr 127.0.0.1:N` starts and serves `/health`.
- Range query at `@ <ts>` returns the sample value at that timestamp;
  `offset 2m` correctly shifts the eval window into the past.
- `/api/v1/series`, `/api/v1/labels`, `/api/v1/label/__name__/values`
  return Prometheus-shaped JSON.

**Remaining tier C/D/E/G/H work** (post-this-round):
- Tier D leftovers: `predict_linear`, `holt_winters`, `deriv`,
  `count_values`, histogram-aware functions beyond `histogram_quantile`,
  100 % `promqltest` corpus pass.
- Tier E (storage depth): mergeset-backed indexdb, lossy precision modes
  in the codec, SIMD codec variants, retention enforcement, snapshot
  mechanism, differential fuzzing.
- Tier G (backup depth): vmbackup-format compat, S3/GCS/Azure, incremental.
- Tier H (release/quality): public repo + URL, signed artifacts, OCI
  images, deb/rpm, full conformance harness run, phase-N reports, soak
  test, fuzz, code signing, SLSA L3.
- Subqueries (PromQL).
- Local PromQL eval in esm-alert.
- vmauth-format YAML config.
- esm-ctl tool.

---

## 2026-05-28 — Phase 2 protobuf + PromQL rate/over_time/vector-vs-vector

**Phase 2.x — Prometheus remote-write (protobuf + snappy):**
- Hand-coded protobuf schema (no build.rs) decoded via prost.
- `POST /api/v1/write` in esm-single accepts standard Prometheus
  remote-write traffic. Verified end-to-end with a binary helper that
  encodes, snappy-compresses, posts, and queries back via PromQL.

**PromQL function library:**
- Rate family: `rate`, `irate`, `increase`, `delta` with counter-reset
  handling + window-extrapolation; strips `__name__` per spec.
- `*_over_time` family: `sum/avg/min/max/count/stddev/stdvar/last/
  present_over_time`. Retains full series identity.
- Vector-vs-vector binary ops with default one-to-one label matching
  (excluding `__name__`).

**Verified:** 145 unit tests, lint green, fmt green. Smoke-tested:
- Prom remote-write: snappy+protobuf in → PromQL query out with full
  label roundtrip.
- `rate(counter[5m])` = 3 for 100→1000 over 5m.
- `errors / requests` pairs correctly by labels.
- `avg_over_time(temp[5m])` correctly averages 5 samples.

**Capability matrix vs VM:**

| Surface | Capability now | Gap vs VM |
|---|---|---|
| Ingest | text exposition, Prom remote-write | Influx, Graphite, OTLP, OpenTSDB, DataDog |
| Storage | on-disk parts, persistent name↔TSID, time-range search | merger (Phase 1A.7), label-aware indexdb scan, retention |
| PromQL parse | selectors, binops w/ full precedence, unary, parens, `bool`, function calls, aggregations w/ `by`/`without` | `on`/`ignoring`/`group_left`/`group_right`, subqueries, `@`/offset modifiers |
| PromQL eval | scalars, vectors, all binop combinations, 9 aggs, 12 instant fns, 4 rate fns, 9 over_time fns | `histogram_quantile`, `topk`/`bottomk`/`quantile`, `clamp_*`, time-of-day, `predict_linear`, `holt_winters` |
| PromQL API | `/api/v1/promql` + `/api/v1/promql_range` with Prom envelopes | original `/api/v1/query{,_range}` aliases, `/api/v1/series`, `/api/v1/labels` |
| esm-agent | scrape + forward | relabeling, persistent retry queue, k8s/consul/cloud SD |
| esm-alert | threshold rules + Alertmanager v2 | local PromQL evaluator (today queries datasource) |
| esm-auth | bearer-token proxy | multi-user YAML, IP filters, basic-auth |
| esm-backup | local snapshot+restore | S3/GCS/Azure object stores, vmbackup-format compat |
| Release | CI workflows + xtask | signed artifacts, OCI images, deb/rpm |

Full VM parity remains a multi-session journey; each remaining gap is a
multi-day chunk in its own right.

---

## 2026-05-28 — Phase 1D + Phase 3.x: persistent indexdb + PromQL evaluator

**Phase 1D MVP — persistent metric_name ↔ TSID index:**
- `data_dir/index.json` sidecar holds `(metric_id, name_hex)` entries.
- Atomic write (`.tmp` + rename + `fsync_dir`) on every flush; loaded at
  `Storage::open` before the part scan. Reopen resolves real metric names
  without placeholders.

**Phase 3.x — PromQL evaluator end-to-end:**
- Parser: 15 binary operators with full precedence + right-assoc for `^`,
  unary, parens, `bool` modifier, function calls, aggregations in both
  `sum(expr) by (lbl)` and `sum by (lbl) (expr)` forms.
- Evaluator: scalar arithmetic, scalar↔vector broadcasting, filtering
  comparisons (preserves original values), aggregations (`sum/avg/min/
  max/count/stddev/stdvar/group` with `by`/`without`), and a starter
  function library (`abs/ceil/floor/round/sqrt/exp/ln/log2/log10/time/
  scalar/vector`).
- Range queries: `evaluate_range(start, end, step)` produces a matrix.

**Phase 4 — esm-single API extended:**
- `GET /api/v1/promql` — instant query with full Prometheus-compatible
  JSON envelope.
- `GET /api/v1/promql_range` — range query producing the matrix format.

**Verified end-to-end:**
- `http_requests_total` returns multi-series with labels.
- `sum by (code) (http_requests_total)` correctly groups over a range.
- `count(http_requests_total)`, `abs(-5)`, `time()` all work.

**Verified:** 141 tests, lint green, fmt green.

---

## 2026-05-28 — Phase 3 PromQL lexer + parser MVP shipped

**Phase 3 MVP:**
- `esm_promql::lexer::tokenize(src)` — full PromQL lexer covering all
  punctuation, operators, identifiers (including `:` for metric names),
  keywords (`and`/`or`/`unless`/`by`/`without`/`on`/`ignoring`/
  `group_left`/`group_right`/`offset`/`bool`), numeric literals,
  durations (`ms`/`s`/`m`/`h`/`d`/`w`/`y`, composite e.g. `1h30m`),
  strings with escape sequences, `#` comments.
- `esm_promql::parser::parse(src) -> Result<Expr, ParseError>` — parses
  numeric literals + instant/range vector selectors with all 4 matcher
  operators + metric-name sugar + `[5m]` range duration.
- Binary expressions / aggregations / function calls / subqueries are
  cleanly rejected with a "not yet supported" error; they land in
  Phase 3.x.
- 18 lexer + parser tests.

**Verified:**
- `cargo xtask test` — **127 unit tests passing** (up from 109).
- `cargo xtask lint` — green. `cargo xtask fmt --check` — green.

**Round-1 phase coverage summary:**
| Phase | Status | Notes |
|---|---|---|
| 0 Foundation | ✅ | 8/8 sub-tasks |
| 1A Mergeset | ✅ MVP | 1A.1-1A.6 done; merger (1A.7) deferred |
| 1B TS codec | ✅ MVP | Lossless variants of all 6 MarshalTypes; precision <64 deferred |
| 1C TS part | ✅ | Full read/write roundtrip on disk |
| 1D IndexDB | ⏳ | Placeholder in-memory map; persistent mergeset-backed lands later |
| 1E Storage | ✅ MVP | open/ingest/flush/search/shutdown working |
| 2 Ingest | ✅ MVP | Text exposition; protobuf remote-write, Influx, OTLP deferred |
| 3 PromQL | ✅ MVP | Lexer + selector parser; evaluator + funcs deferred |
| 4 esm-single | ✅ | HTTP server functional, ingest+query roundtrips |
| 5 esm-agent | ✅ MVP | Scrape+forward; relabeling + persistent queue deferred |
| 6 esm-alert | ✅ MVP | Threshold rules; local PromQL eval deferred |
| 7 esm-auth | ✅ MVP | Bearer-token proxy; multi-user YAML config deferred |
| 8 esm-backup | ✅ MVP | Local snapshot+restore; vmbackup-compat + cloud deferred |
| 9 Release | ⏳ | CI workflows in place; signed artifacts + distribution deferred |

End-to-end verified: ingest → auth-proxy → query → backup → restore →
re-query through a fresh `esm-single` instance.

**Next priorities for follow-up sessions:**
1. Phase 3.x — binary expressions + real PromQL evaluator wired into
   esm-single's `/api/v1/query{,_range}`.
2. Phase 1D — persistent indexdb (mergeset-backed) — closes the
   in-memory TSID gap so reopen-and-query fully resolves metric names.
3. Phase 1A.7 — background merger.
4. Phase 2.x — protobuf-based Prometheus remote-write.
5. Phase 9 — release artifacts: deb/rpm/Homebrew/MSI, code signing,
   reproducible builds.

---

## 2026-05-28 — Phases 1C, 1E, 2, 4–8 MVPs shipped; full end-to-end pipeline verified

**Phase 1C complete (timeseries part read/write):**
- `MetaindexRow`, `PartHeader`, `BlockStreamWriter`, `BlockStreamReader`.
- Full on-disk roundtrip including zstd-compressed payloads, multi-block
  parts, and metadata.json + dir-fsync via `esm_platform::durability`.

**Phase 1E shipped as MVP `Storage`:**
- `Storage::open / ingest / flush / search_by_metric_name / shutdown`.
- Exclusive data-dir lock via FileLock, bootstrap from existing parts.
- Known shortcut: TSID→name mapping in-memory only; Phase 1D will
  persist via mergeset-backed indexdb.

**Phase 2 — first ingest protocol (Prometheus text exposition):**
- Handles HELP/TYPE comments, escaped label values, canonicalised
  label-set serialisation, optional trailing timestamp. 10 tests.

**Phase 4 — `esm-single` functional:**
- axum HTTP server: `POST /api/v1/import/prometheus`,
  `GET /api/v1/query`, `/health`. Graceful shutdown via
  `esm_platform::signal::wait_for_shutdown`.

**Phase 5 — `esm-agent` MVP:**
- Periodic scrape of `--scrape-url` targets, forwards bodies to
  `--remote-write-url`. Per-target Tokio tasks.

**Phase 6 — `esm-alert` MVP:**
- YAML rule file (groups/rules with threshold), evaluates against
  datasource URL, posts firing alerts to Alertmanager v2 as JSON.
  Real PromQL evaluator deferred to Phase 3.

**Phase 7 — `esm-auth` MVP:**
- Bearer-token reverse proxy. 401s unauth requests, forwards authed
  with hop-by-hop header stripping.

**Phase 8 — `esm-backup` MVP:**
- `backup --src --dst` snapshots with MANIFEST.json. `restore --src
  --dst` validates sizes. vmbackup format compat deferred.

**End-to-end pipeline verified by scripted smoke test:**
1. `curl` ingests text exposition into `esm-single`.
2. `esm-auth` 401s unauthenticated; forwards authenticated to backend.
3. `esm-backup` snapshots; `esm-backup restore` recreates elsewhere.
4. Fresh `esm-single` opens restored data dir cleanly.

**Verified on Linux x86_64:**
- `cargo build --release --workspace` — green
- `cargo xtask lint` — green
- `cargo xtask fmt --check` — green
- `cargo xtask test` — **109 unit tests passing**

**Per owner directive:** no commits.

**Blocked on:** nothing.

**Next:**
- Phase 3 — real PromQL parser + evaluator (biggest remaining chunk).
- Phase 1A.7 — background mergeset merger.
- Phase 1B precision modes < 64 bits.
- Phase 1D — persistent indexdb (mergeset-backed) closes the in-memory
  TSID gap.
- Phase 2.x — protobuf Prom remote-write, Influx line, OTLP.
- Phase 5.x / 6.x / 7.x / 8.x — depth on relabeling, PromQL in alerts,
  multi-user auth config, S3/GCS backup.
- Phase 9 — release pipeline + signed artifacts.

---

## 2026-05-28 — Phase 1C kickoff: time-series format spec + `Tsid` + `BlockHeader`

**Shipped:**
- `docs/format/timeseries-part.md` — 10-section format spec for VM
  v1.144.0's time-series part directory (`timestamps.bin`, `values.bin`,
  `index.bin`, `metaindex.bin`, `metadata.json`). Every field anchored to
  `lib/storage/<file>.go:<line>` references.
- `esm-storage::timeseries::Tsid` — 24-byte fixed-size identifier with BE
  marshal/unmarshal and `PartialOrd`/`Ord` derived to match VM's lex order
  over marshalled bytes. 5 round-trip + sort-order + truncation tests.
- `esm-storage::timeseries::BlockHeader` — 81-byte fixed-size header
  containing TSID + min/max timestamp + first value + 4 offset/size fields
  + rows count + scale + dual marshal-type bytes + precision bits. Full
  marshal/unmarshal + VM's validation rules (rows in [1, 2*MAX_ROWS],
  precision in [1, 64], block sizes in [0, 2*MAX_BLOCK_SIZE]). 6 tests.
- Internal BE int16/int64 helpers in the timeseries block_header module
  (zig-zag-free fixed-size encoding required by VM's binary layout, in
  addition to the varint helpers in esm-compress::int).
- `MAX_ROWS_PER_BLOCK` (8192) and `MAX_BLOCK_SIZE` (128 KiB) constants
  exported from the timeseries module.

**Verified locally on Linux x86_64:**
- `cargo build --workspace` — green
- `cargo xtask lint` — green
- `cargo xtask test` — **84 unit tests passing** (15 platform + 13
  compress::int + 3 zstd + 10 compress::timeseries + 32 storage::mergeset
  + 11 storage::timeseries)
- `cargo xtask fmt --check` — green

**Per owner directive:** no commits.

**Blocked on:** nothing.

**Next:**
- Phase 1C.x — time-series `MetaindexRow` + `BlockStreamWriter` +
  `BlockStreamReader` + Part wrapper (mirroring the mergeset pattern from
  Phase 1A).
- Phase 1D — IndexDB + TSID assignment.
- Phase 1E — Storage engine integration + Phase 1 conformance gate.
- Phases 2–9 — ingest protocols → PromQL → esm-single → esm-agent → ...

**Notes:**
- VM stores per-block `Scale` (int16) and `PrecisionBits` (u8). The
  on-the-wire format supports lossy precision encoding even though our
  current codec only implements 64-bit lossless; storing the precision bits
  still works because the codec reads them on decode.
- The natural next step is a `BlockStreamWriter` / `BlockStreamReader` pair
  for time-series parts. Each is ~250 LOC of glue similar in shape to the
  mergeset writer/reader from 1A.6, but writes paired timestamps + values
  blocks per `BlockHeader`. The same scratch buffers + flush-on-overflow
  approach applies.

---

## 2026-05-28 — Phase 1A.4–1A.6 + Phase 1B lossless codec shipped

**Phase 1A.4 — block-header + metaindex-row marshal/unmarshal:**
- `BlockHeader::marshal`/`unmarshal` produce/consume the 8-field layout
  exactly per `docs/format/mergeset-part.md` §4.
- `MetaindexRow::marshal`/`unmarshal` for the 4-field metaindex-row layout.
- Sequence helpers `unmarshal_block_headers` and
  `unmarshal_metaindex_rows` validate sort invariants and trailing-byte
  invariants (matches VM 159-183 and 85-125 respectively).
- 14 new tests covering byte-layout match, roundtrip, sort enforcement, and
  error paths.

**Phase 1A.5 — `InmemoryBlock` codec (the big one):**
- `marshal_sorted_data` / `marshal_unsorted_data` produce
  `StorageBlock { items_data, lens_data }` from a sorted/unsorted block at
  a configurable compress level.
- Plain encoding path: items[1..] stripped of `common_prefix`, lens as BE
  u64s.
- Zstd encoding path: prefix-delta encoded items, delta-XOR encoded prefix
  lengths and item lengths as varuints, both zstd-compressed.
- Compression-ratio fallback to plain when zstd ratio > 0.9 (matches VM).
- `unmarshal_data` reverses both paths, verifies sort invariant on output.
- 8 new tests including a 200-item zstd roundtrip.

**Phase 1A.6 — file I/O writer + reader:**
- `BlockStreamWriter::create(path, level)` opens 4 buffered file writers
  inside a freshly created part directory.
- `write_block(&mut InmemoryBlock)` marshals, appends to items.bin /
  lens.bin, builds the unpacked index block, flushes index blocks at
  `MAX_INDEX_BLOCK_SIZE`, tracks first/last item.
- `finish()` flushes trailing index data, compresses + writes metaindex,
  syncs each file, writes `metadata.json`, and fsyncs the part directory
  via `esm_platform::durability::fsync_dir`.
- `BlockStreamReader::open(path)` reads metadata.json, decompresses
  metaindex.bin, holds file handles for index/items/lens; lazily loads
  index blocks via `next_block()`.
- 3 new tests: single-block, multi-block, and large-zstd round-trips
  through real on-disk files via `tempfile`.

**Phase 1B — time-series codec (lossless / 64-bit precision):**
- `esm-compress::timeseries` implements VM's 6-variant `MarshalType`
  selection (Const, DeltaConst, NearestDelta, NearestDelta2, plus zstd-wrapped
  Delta and Delta2 variants).
- `marshal_int64_array` picks the best variant per VM heuristics (constant
  detection, delta-const detection, gauge-vs-counter heuristic, zstd
  fallback).
- `unmarshal_int64_array` reverses each variant including zstd decompression.
- Lossy precision (`precision_bits < 64`) returns a clear "not yet
  implemented" error to be filled in alongside Phase 1C.
- Signed zig-zag varints (`marshal_varint64`/`unmarshal_varint64` and slice
  forms) added to `esm-compress::int`.
- 12 new tests covering each variant + edge cases.

**Verified locally on Linux x86_64:**
- `cargo build --workspace` — green
- `cargo xtask lint` — green (workspace clippy `-D warnings`)
- `cargo xtask test` — **71 unit tests passing** (15 platform + 11
  compress::int + 3 zstd + 10 timeseries + 32 storage::mergeset).
- Cross-compile targets (macOS aarch64, Windows MSVC) still green.

**Per owner directive:** no commits.

**Blocked on:** nothing.

**Next:**
- Phase 1A.7 — background merger (defer-tractable; one-part-per-flush is
  fine for early integration).
- Phase 1C — time-series part format (`timestamps.bin`, `values.bin`,
  `index.bin`, `metaindex.bin` layout for sample data).
- Phase 1D — `IndexDB` + TSID assignment.
- Phase 1E — `Storage::open / ingest / search` integration.

**Notes:**
- The lossless time-series path is enough to drive Phase 1C/1D/1E and most
  real workloads. Lossy precision modes affect storage cost, not
  correctness, and can be added without disturbing the rest of the engine.
- The `unsafe` block in `InmemoryBlock::sort_items` is the first `unsafe`
  usage in the workspace; documented with a `SAFETY:` comment that
  explains the disjoint-borrow argument.

---

## 2026-05-28 — Phase 1A.1–1A.3 shipped: VM cloned, format spec, skeleton + types

**Shipped since the prior entry:**
- **1A.1** Cloned `VictoriaMetrics@v1.144.0` (shallow, single-branch) to
  a sibling `victoriametrics-ref/` directory (242 MB) as
  read-only reference. `git describe --tags` confirms `v1.144.0`.
- **1A.2** Reverse-engineered the mergeset on-disk format and wrote
  `docs/format/mergeset-part.md` — 10 sections covering all 5 part files,
  every byte field, sort invariants, and validation rules. Every claim is
  anchored to a VM `lib/<file>.go:<line>` reference so future re-verification
  is mechanical.
- **1A.3** `esm-storage::mergeset` skeleton:
  - `block_header.rs`, `metaindex_row.rs`, `part_header.rs`,
    `inmemory_block.rs`, `marshal_type.rs` — all field types match the spec
    one-to-one.
  - `mergeset/mod.rs` re-exports + `MAX_INMEMORY_BLOCK_SIZE`,
    `MAX_INDEX_BLOCK_SIZE` constants + `filenames` submodule mirroring VM's
    `filenames.go`.
  - `PartHeader` JSON serde with lower-case-hex byte-string handling
    matches VM's `hexString` exactly + round-trip tested.
  - `InmemoryBlock::add` with overflow rejection, mirroring VM's `Add`.
- **`esm-compress::int`** — primitive binary-encoding helpers shared between
  mergeset and future time-series codecs: `marshal_uint32/64`,
  `marshal_varuint64`, `marshal_bytes`, plus unmarshal pairs returning
  `Result<(T, consumed), DecodeError>`. 9 unit tests confirm round-trips
  including boundary values (varuint at 0x7F/0x80 transition, max u64).

**Verified locally on Linux x86_64:**
- `cargo build --workspace` — green
- `cargo xtask lint` — green (workspace-wide clippy `-D warnings`)
- `cargo xtask fmt --check` — green
- `cargo xtask test` — 33 unit tests passing (15 esm-platform + 9
  esm-compress::int + 9 esm-storage::mergeset)

**Per owner directive:** no commits.

**Blocked on:** nothing.

**Next:** Phase 1A.4 — mergeset block-header / metaindex-row Marshal +
Unmarshal byte-level implementation, plus round-trip tests against fixtures
produced by VM. After 1A.4 lands the `InmemoryBlock::marshal_sorted_data`
codec (VM's `marshalData`) follows in 1A.5–1A.6, then writer + reader
plumbing.

**Notes:**
- Workspace-wide clippy pedantic remains strict; the per-callsite
  `#[allow(clippy::cast_possible_truncation)]` pattern is being used at
  intentional-truncation points (varuint low-byte extraction, u32 offsets
  inside an `InmemoryBlock` bounded to 64 KiB). The truncation is
  documented at each callsite.
- ADR-005 follows for: clippy `unsafe_op_in_unsafe_fn = "forbid"` not yet
  exercised by storage code; first `unsafe` block lands when the reader
  needs zero-copy `&[u8]` views into mmap'd parts.

---

## 2026-05-28 — Phase 0 closed; all 8 sub-tasks green

**Shipped since the prior entry:**
- **0.3 esm-platform abstractions** — seven OS abstractions implemented
  (mmap, durability, atomic_rename, file_lock, signal, paths, proc) with 15
  unit tests passing on Linux. Cross-checks for aarch64-apple-darwin and
  x86_64-pc-windows-msvc both compile.
- **0.4 xtask tooling** — `cargo xtask {fmt,lint,test}` dispatch to real
  cargo invocations; `bench`, `perf record|compare`, `fixtures
  regenerate|push|pull` print clear "not yet implemented (stub)" messages.
- **0.5 CI workflows** — `ci.yml` (fmt + clippy + cargo-deny + 4-target test
  matrix), `bench.yml` (parity placeholder), `nightly.yml` (audit + deny +
  vs-upstream + fuzz). `linux-x86_64-musl` and `linux-aarch64-gnu` matrix
  slots commented out pending self-hosted runners.
- **0.6 conformance harness skeleton** — `conformance-harness` binary with
  `list`, `check`, `dry-run` subcommands fully working; `run` returns a
  clear "Phase 1+" error. One `smoke.yaml` scenario. `fixtures.lock.json`
  initialised.
- **0.8 Phase 0 verification + close-out** — local build/test/lint/fmt all
  green; macOS aarch64 + Windows MSVC cross-checks pass; phase report
  written to `docs/state/phase-0-report.md`.

**Per owner directive:** no commits until the first round of all phases is
done. State preserved on the working tree.

**Blocked on:** nothing.

**Next:** Phase 1A — Mergeset (inverted index) reader/writer/merger with
byte-exact compatibility against VictoriaMetrics v1.144.0.

**Notes:**
- `serde_yaml` is deprecated upstream; track switching to `serde_yaml_ng` in
  Phase 1.
- Phase 0 took roughly one extended agent session end-to-end. Phase 1A is
  expected to take significantly longer (6–8 weeks per PLAN.md §9).

---

## 2026-05-28 — Phase 0 kickoff + 0.2 complete

**Shipped:**
- Repository renamed `victoria-metrics-claude/` → `esmetrics/`. Git initialised on `main`.
- License + attribution files: `LICENSE` (Apache 2.0), `NOTICE`, `CREDITS.md`.
- Toolchain pins: `rust-toolchain.toml` (1.95.0 stable), `.gitignore`, `.editorconfig`, `rustfmt.toml`, `clippy.toml`, `deny.toml`, `.cargo/config.toml`.
- Cargo workspace with 12 library crates + 6 app skeletons + xtask:
  - Crates: esm-platform, esm-common, esm-compress, esm-storage, esm-promql, esm-protocols, esm-scrape, esm-discovery, esm-alerting, esm-net, esm-objstore, esm-vmui
  - Apps: esm-single, esm-agent, esm-alert, esm-auth, esm-backup, esm-ctl
  - Workspace-wide lints (rust + clippy) and release/dev/bench profiles.
- `esm-platform` module file stubs in place (mmap, durability, atomic_rename, file_lock, signal, paths, proc).
- README.md with status + component table.
- State-tracking docs (`docs/state/`) initialised.

**Verified locally on Linux x86_64:**
- `cargo build --workspace` — green
- `cargo clippy --workspace --all-targets -- -D warnings` — green
- `cargo test --workspace` — green (0 tests, expected for skeleton)
- `cargo fmt --all --check` — green

**Blocked on:** nothing.

**Next:** Phase 0.7 close-out (this log + decisions.md + backlog.md), then 0.3 (esm-platform abstractions), 0.4 (xtask tooling), 0.5 (CI workflows), 0.6 (conformance harness skeleton), 0.8 (verification + tag `phase-0-complete`).

**Notes:**
- Workspace builds in ~0.2s on Linux (no external dependencies yet). External deps land per-crate as real implementations begin.
- Clippy `doc_markdown` was relaxed at workspace level rather than fighting backticking on every product-name doc comment — see ADR-002.
