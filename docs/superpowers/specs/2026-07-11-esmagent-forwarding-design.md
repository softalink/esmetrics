# esmagent (vmagent port) — Forwarding Tier + Relabel Engine — Design Spec

**Status:** Approved design, pending implementation plan
**Date:** 2026-07-11
**Upstream:** VictoriaMetrics `app/vmagent` + `lib/promrelabel` + `lib/persistentqueue` @ v1.146.0 (see `UPSTREAM`)
**Scope note:** This is **Phase 0 + Phase 1** of the vmagent port. The scrape engine (`lib/promscrape`) is **Phase 2**, a separate later spec/plan/build cycle.

## Goal

Port VictoriaMetrics `vmagent`'s **remote-write forwarding tier** to Rust as a standalone `esmagent` binary that receives metrics via all the push protocols esmetrics already supports, relabels them, and forwards them to N remote-write destinations with per-destination durable disk buffering and retry — plus a reusable **`esm-relabel`** promrelabel engine that both this tier and the future scrape tier depend on.

## Scope

**In scope**
- **`esm-relabel`** crate: parse `relabel_configs` YAML → compiled configs → apply to a label set. All 20 VM relabel actions + `if` expressions + the `graphite` action.
- **`esmagent`** binary — the forwarding tier:
  - Input: reuse `esm-insert`'s full protocol router + HTTP handlers via a `ForwardingSink` (`esm-insert::RowSink`) — accepts all push protocols (remote-write, influx, graphite, opentsdb, opentsdbhttp, datadog v1/v2/sketches, otlp, prometheusimport, csvimport, native, newrelic).
  - Global relabel → fan-out to N `-remoteWrite.url` destinations, each with per-URL relabel + pendingseries batching + persistent queue + remote-write client (retry/backoff, auth/TLS).
  - Multi-destination fan-out (the core vmagent model).
  - Persistent queue: **faithful behavior** (in-mem + disk overflow, durable FIFO across restarts, `-remoteWrite.maxDiskUsagePerURL` cap, drop-oldest when full) — not byte-identical to VM's chunk-file format.

**Out of scope (deferred to later cycles / documented)**
- The scrape engine (`lib/promscrape`) — Phase 2.
- All service discovery (Phase 2 will start with static/file/http SD).
- Stream aggregation (`app/vmagent/remotewrite/streamaggr.go`).
- `-remoteWrite.oauth2.*` flag families; multitenancy; the blocking (non-drop) `-remoteWrite.disableOnDiskQueue` backpressure mode (documented follow-up).

## Architecture

`esmagent` is a standalone binary: it receives (push) and forwards to N backends with durable buffering — the faithful vmagent relay. Sync stack: hand-rolled `esm-http` server + blocking `reqwest`, no tokio.

### Crate layout

**`crates/esm-relabel`** (new lib, Phase 0) — the promrelabel engine, zero vmagent knowledge.
- `config.rs` — `RelabelConfig` struct + `Action` enum + YAML parse.
- `regex.rs` — anchor (`^(?:…)$`) + compile (Rust `regex`, RE2-compatible).
- `apply.rs` — the per-action in-place transform + drop signaling.
- `if_expr.rs` — the `if:` metric-selector gate (via `esm-metricsql` label filters).
- `graphite.rs` — the `graphite` action (match + labels template).
- Public API: `ParsedConfigs::parse(yaml) -> Result<ParsedConfigs>` and `parsed.apply(&mut labels, labels_offset) -> bool` (false = series dropped).

**`crates/esmagent`** (new bin, Phase 1) — the forwarding tier.
- `sink.rs` — `ForwardingSink` (impl `esm_insert::RowSink`): convert rows → series, apply global relabel, hand to fan-out.
- `fanout.rs` — owns `Vec<RemoteWriteCtx>`; global-relabel-once then dispatch.
- `rwctx.rs` — `RemoteWriteCtx` (one per destination): per-URL relabel + pendingseries + queue + client, as a self-contained unit.
- `pendingseries.rs` — batch series → snappy `WriteRequest` blocks (via `esm-protoparser::prompb_encode`).
- `queue.rs` — `PersistentQueue`: durable FIFO of blocks (in-mem ring + disk spill, size cap, drop-oldest, restart replay).
- `client.rs` — per-destination remote-write client + worker pool + retry/backoff.
- `flags.rs` + `main.rs` — CLI + wiring + lifecycle; reuses the esm-insert router + esm-http server for input.

### Reuse map
- `esm-protoparser::prompb_encode` (esmalert Task 1) — encode snappy `WriteRequest` blocks.
- `esm-insert` (protocol router + all handlers + the `RowSink` trait: `fn add_rows(&self, rows: &[MetricRow<'_>]) -> Result<(), String>`) — input reception; `esmagent` supplies a `ForwardingSink` where esmetrics supplies `StorageSink`.
- `esm-http` server + blocking `reqwest` + `AuthConfig`/`TlsConfig` (from esmalert's datasource/notifier) — HTTP server + remote-write clients.
- `esm-metricsql` — the `if:` selector parse.
- `esm-common::metrics` — esm_ counters.

### Data flow
`push (any protocol) → esm-insert handler → ForwardingSink::add_rows → global relabel → fanout → for each RemoteWriteCtx: per-URL relabel → PendingSeries (batch → snappy block) → PersistentQueue (in-mem + disk) → Client workers (POST with retry) → destination`.

## Component: `esm-relabel` (promrelabel engine)

Faithful to `lib/promrelabel`. **Config model** (Prometheus-compatible superset):
- `RelabelConfig { source_labels: Vec<String>, separator: Option<String> (default ";"), target_label: Option<String>, regex: Option<CompiledRegex> (default "(.*)"), modulus: Option<u64>, replacement: Option<String> (default "$1"), action: Action, if_expr: Option<IfExpression> }`.
- `Action` (20 variants): `Replace`, `ReplaceAll`, `Keep`, `Drop`, `KeepEqual`, `DropEqual`, `KeepIfEqual`, `DropIfEqual`, `KeepIfContains`, `DropIfContains`, `KeepMetrics`, `DropMetrics`, `Labelmap`, `LabelmapAll`, `Labeldrop`, `Labelkeep`, `Hashmod`, `Lowercase`, `Uppercase`, `Graphite`.
- **`if` expressions**: VM extension — a metric selector (`{__name__=~"…", label="…"}`) gating whether the rule applies; parsed via `esm-metricsql` label filters.
- **`graphite` action**: VM extension mapping graphite-style names to labels (match + labels template).

**Regex**: VM anchors as `^(?:<regex>)$`, RE2 semantics; Rust `regex` matches. Default `(.*)`, `$1`-style replacement (Go `regexp` `$1` == Rust `regex` `$1`). Compile once at parse time.

**Public API**:
```rust
let parsed = ParsedConfigs::parse(yaml)?;   // parse + compile once
let survives = parsed.apply(&mut labels, offset);  // in-place; false = drop series
```
`apply` runs rules in order; `keep`/`drop`/`*_if_*`/`*_metrics` can signal drop. `__name__` participates like any label.

**Testing**: exhaustive — each of the 20 actions against `lib/promrelabel/relabel_test.go`-derived vectors; regex anchoring; `if`-gating; `separator`/`modulus`/`replacement` defaults; multi-rule ordering.

## Component: forwarding tier (`esmagent`)

**`ForwardingSink`** (impl `esm_insert::RowSink`): `add_rows(rows)` → convert `MetricRow`s to an owned series form → apply the global relabel (`-remoteWrite.relabelConfig`) once → drop killed series → hand survivors to the fanout.

**Fanout** — `Vec<RemoteWriteCtx>`, one per `-remoteWrite.url`. Each ctx is independent:
1. **Per-URL relabel** (`-remoteWrite.urlRelabelConfig[i]`, optional) on a per-destination copy.
2. **`PendingSeries`** — accumulate series → marshal into snappy `WriteRequest` blocks when a block fills (`-remoteWrite.maxBlockSize`) or on `-remoteWrite.flushInterval`.
3. **`PersistentQueue`** — durable FIFO of blocks: in-mem ring (fast path) spilling to disk files under `-remoteWrite.tmpDataPath/<per-url-dir>` when the consumer lags; survives restart (replay queued blocks on start); size-capped by `-remoteWrite.maxDiskUsagePerURL` with drop-oldest when full. Faithful behavior, not VM's exact chunk format.
4. **`Client` workers** — `-remoteWrite.queues` threads/dest read blocks + POST (`Content-Encoding: snappy`, `Content-Type: application/x-protobuf`, `X-Prometheus-Remote-Write-Version: 0.1.0`) with exponential-backoff retry (`retryMinInterval` 1s doubling to `retryMaxInterval` 1m): retry on 5xx/429/transport (block stays queued); drop on non-retryable 4xx (logged + counted). Per-destination `AuthConfig`/`TlsConfig`.

**Lifecycle/durability**: on shutdown, workers drain, `PendingSeries` flushes to queue, queue persists. On start, each ctx reopens its disk queue and resumes. A `dropDanglingQueues`-equivalent cleans queue dirs for no-longer-configured destinations.

**Backpressure**: queue-full + `-remoteWrite.disableOnDiskQueue` off (default) → drop-oldest, non-blocking ingestion (faithful default). Blocking mode is a documented follow-up.

## CLI & config

Mirrors esmauth/esmalert flag idiom (`-flag=value` / `-flag value`, repeatable arrays, `*File` secrets + redaction). In-scope flags:
- **Forwarding**: `-remoteWrite.url` (repeatable), `-remoteWrite.tmpDataPath`, `-remoteWrite.maxDiskUsagePerURL`, `-remoteWrite.queues`, `-remoteWrite.maxBlockSize`, `-remoteWrite.flushInterval`, `-remoteWrite.retryMinInterval`, `-remoteWrite.retryMaxInterval`, `-remoteWrite.disableOnDiskQueue`, `-remoteWrite.showURL`.
- **Relabel**: `-remoteWrite.relabelConfig` (global), `-remoteWrite.urlRelabelConfig` (per-destination, repeatable).
- **Per-destination auth/TLS**: basicAuth (user/pass + `*File`), bearerToken (+`File`), TLS (`caFile`/`certFile`/`keyFile`/`serverName`/`insecureSkipVerify`).
- **Server/input**: `-httpListenAddr` (esm-http server hosting the esm-insert push endpoints), `-metrics.authKey`, `-httpReadTimeout`.
- **Deferred (documented)**: `-promscrape.*` + all SD, stream-aggregation flags, `-remoteWrite.oauth2.*`, multitenancy.

Relabel configs validated at startup (bad config → fail fast, non-zero exit). ≥1 destination required.

## Error handling

- Per-destination failure isolation: retry with backoff; block stays queued across retries; only non-retryable 4xx or full-queue drop-oldest discards data, both logged + counted.
- esm_ counters: rows received, blocks sent/dropped/retried per destination, queue size/bytes per destination.
- Relabel/config parse errors at startup → fail fast. Per-request input parse errors → the protocol's error to the client (reuse esm-insert paths).
- Never panic in a worker loop. Never log credentials.

## Testing

- **`esm-relabel`**: exhaustive unit suite — 20 actions + `if`-gating + regex anchoring vs `relabel_test.go` vectors.
- **Forwarding unit**: `PendingSeries` block-fill/flush; `PersistentQueue` durability (enqueue → drop → reopen → dequeue same blocks), size-cap drop-oldest, FIFO order; `Client` retry/backoff + 4xx-drop-vs-5xx-retry against a stub server; fanout applies global-then-per-URL relabel.
- **E2E**: `esmagent` end-to-end — POST remote-write (+ one other protocol, e.g. influx) to its input endpoint with two destination stub servers; assert both receive the relabeled series; kill a destination mid-stream → its blocks queue to disk → replay on return; restart `esmagent` → queued blocks survive. Deterministic (stubs + bounded polling, no sleep-only waits).
- **Both-platform** (Linux + Windows CI — the disk-queue file handling is platform-sensitive); 80%+ coverage.

## Global constraints

- Files ≤ 800 lines; extract modules when a file grows unwieldy.
- `cargo fmt` clean; `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets` clean; windows-gnu cross-check compiles. (Note: CI's stable toolchain auto-updates — match it locally with `rustup update stable` before pushing to catch new deny-by-default clippy lints.)
- No tokio; sync stack (esm-http server + blocking reqwest + std threads).
- Never log secrets/tokens; usernames-only in logs/metric labels.
- Faithful to upstream v1.146.0 semantics; remote-write wire format + relabel behavior preserved (existing vmagent relabel configs work unchanged).
- New workspace members `crates/esm-relabel` + `crates/esmagent` added to root `Cargo.toml`.
- Commit style `<type>: <description>`, no attribution trailers.
- After push, watch the GitHub Actions run and fix failures (Windows tests run only in CI).
