# vminsert Ingestion-Protocol Gap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port the remaining upstream vminsert ingestion protocols — Prometheus remote-write, Prometheus text import, vmimport, CSV import, OpenTelemetry (OTLP), Graphite, OpenTSDB (telnet + HTTP), and Datadog (v1/v2 series) — so esmetrics accepts the same write traffic as upstream single-node VictoriaMetrics v1.146.0.

**Architecture:** Every protocol follows the proven influx pipeline: a zero-copy parser in `esm-protoparser` (port of `lib/protoparser/<format>`), a converter/handler in `esm-insert` (port of `app/vminsert/<format>/request_handler.go`) that marshals rows through `esm_storage::marshal_metric_name_raw` into the `RowSink` seam, and a route added to `InsertHandlers::handle`. Shared infrastructure (content-encoding plumbing, whole-body decompression, protobuf wire reader) is built once in Phase 0/1 and reused by every later phase. TCP/UDP listeners for Graphite/OpenTSDB are a new `ingestserver` module in `esm-insert`.

**Tech Stack:** Rust 2021 (workspace), `flate2` (gzip/deflate), `zstd` (workspace dep), new `snap = "1"` workspace dep (snappy block format), `serde_json` (workspace dep, replaces upstream `fastjson`), hand-rolled protobuf wire reader (mirrors upstream `easyproto`; no prost/protoc).

## Global Constraints

- Upstream baseline is **VictoriaMetrics v1.146.0** (`UPSTREAM`: commit `4d9901fbf42c518ecc6d467d65efea9c7d842bbd`); local checkout at `/home/test/refsrc/VictoriaMetrics`.
- Command-line flags are **fixed at upstream defaults** (same convention as the influx port): `-maxInsertRequestSize=32MiB`, `-import.maxLineLen=10MB`, `-opentelemetry.usePrometheusNaming=false`, `-sortLabels=false`, relabeling/streamaggr/rate-limiting **not ported** (all default-off upstream).
- Port unit tests alongside each module (`*_test.go` → `#[cfg(test)]`), per `docs/PORTING.md` rule 2.
- Every crate must build warning-free on `x86_64-unknown-linux-gnu` and `x86_64-pc-windows-gnu` (`cargo check --target ...`), per `docs/PORTING.md` rule 3.
- `unsafe` only where measured, wrapped and documented (rule 4).
- Files ≤ 800 lines; split modules the way upstream splits files.
- `cargo fmt` + `cargo clippy -- -D warnings` clean before every commit.
- Commit format `<type>: <description>` (feat/fix/test/docs/chore); no attribution trailers.
- On-disk/wire compatibility with Go clients is the acceptance bar for each protocol: real client payloads (listed per task) must ingest byte-identically to upstream's label/metric-group mapping.
- Multi-tenancy, `/prometheus`-prefixed alias paths ARE ported (they are just extra `match` arms); `vmproto` handshake (`lib/protoparser/protoparserutil/vmproto_handshake.go`) is NOT (vmagent-specific).
- Out of scope (record in PORTING.md at the end): `native` import (no `/api/v1/export/native` on the select side yet), `newrelic`, `zabbixconnector`, `datadogsketches`, OTLP firehose, prometheus metadata storage (`lib/prommetadata`), Datadog proxy flags.

## File Structure

```
crates/esm-http/src/request.rs          # MODIFY: ContentEncoding enum + accessor
crates/esm-protoparser/src/
├── lib.rs                              # MODIFY: declare new modules
├── util.rs                             # NEW (Phase 0): read_uncompressed_data, uncompressed_reader, snappy block
├── wire.rs                             # NEW (Phase 1): protobuf wire-format reader (varint/tag/len-delim/f64)
├── prompb.rs                           # NEW (Phase 1): WriteRequest/TimeSeries/Label/Sample unmarshal
├── promremotewrite.rs                  # NEW (Phase 1): snappy/zstd body decode → WriteRequest
├── prometheus.rs                       # NEW (Phase 2): exposition-text Rows parser
├── vmimport.rs                         # NEW (Phase 2): JSON-lines parser
├── csvimport.rs                        # NEW (Phase 2): CSV parser + ColumnDescriptor
├── opentelemetry/
│   ├── mod.rs                          # NEW (Phase 3)
│   ├── pb.rs                           # NEW (Phase 3): OTLP metrics message decode
│   └── sanitize.rs                     # NEW (Phase 3): label/name sanitizing
├── graphite.rs                         # NEW (Phase 4): plaintext parser
├── opentsdb.rs                         # NEW (Phase 4): telnet put parser
└── opentsdbhttp.rs                     # NEW (Phase 4): JSON /api/put parser
crates/esm-insert/src/
├── lib.rs                              # MODIFY each phase: routes in InsertHandlers::handle
├── common.rs                           # NEW (Phase 0): extra_labels + shared query-param helpers
├── promremotewrite.rs                  # NEW (Phase 1)
├── prometheusimport.rs                 # NEW (Phase 2)
├── vmimport.rs                         # NEW (Phase 2)
├── csvimport.rs                        # NEW (Phase 2)
├── opentelemetry.rs                    # NEW (Phase 3)
├── graphite.rs                         # NEW (Phase 4): row conversion shared by TCP/UDP/HTTP
├── opentsdb.rs                         # NEW (Phase 4)
├── opentsdbhttp.rs                     # NEW (Phase 4)
├── ingestserver.rs                     # NEW (Phase 4): TCP/UDP listeners
├── datadog.rs                          # NEW (Phase 5): v1+v2 handlers + stub endpoints
└── tests/<proto>_write.rs              # NEW per phase, modeled on tests/influx_write.rs
crates/esmetrics/src/flags.rs           # MODIFY (Phase 4): -graphiteListenAddr, -opentsdbListenAddr, -opentsdbHTTPListenAddr
crates/esmetrics/src/main.rs            # MODIFY (Phase 4): start/stop ingest servers
docs/PORTING.md, UPSTREAM               # MODIFY at each phase end: scope rows + VM_SCOPE
```

Estimated ported volume: ~9–10k lines of Rust incl. tests (upstream sources total ~6.5k Go excl. tests). Each phase is independently shippable.

---

# Phase 0 — Shared decompression & header plumbing

## Task 1: `ContentEncoding` in esm-http

Remote-write needs `Content-Encoding: zstd|snappy`; import endpoints accept `gzip|zstd|deflate`. The head parser currently collapses everything to `gzip_body: bool`.

**Files:**
- Modify: `crates/esm-http/src/request.rs` (Head struct ~line 45, parse_head ~line 108, Request accessors ~line 474)

**Interfaces:**
- Produces: `esm_http::ContentEncoding` (re-export from `lib.rs`) and `Request::content_encoding(&self) -> ContentEncoding`. `Request::is_gzipped()` is kept, now derived.

```rust
/// Body `Content-Encoding`. Values mirror what upstream
/// `protoparserutil.GetUncompressedReader` accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ContentEncoding {
    #[default]
    Identity,
    Gzip,
    Zstd,
    Deflate,
    Snappy,
    /// Any other value; handlers answer 400 like upstream's
    /// "unsupported contentType" error.
    Unsupported,
}

impl ContentEncoding {
    /// The upstream string form, for esm-protoparser's `&str`-typed APIs.
    pub fn as_str(self) -> &'static str {
        match self {
            ContentEncoding::Identity => "",
            ContentEncoding::Gzip => "gzip",
            ContentEncoding::Zstd => "zstd",
            ContentEncoding::Deflate => "deflate",
            ContentEncoding::Snappy => "snappy",
            ContentEncoding::Unsupported => "unsupported",
        }
    }
}
```

- [ ] **Step 1: Write failing tests** in the existing `#[cfg(test)]` module of `request.rs`:

```rust
#[test]
fn content_encoding_is_parsed() {
    for (hdr, want) in [
        ("gzip", ContentEncoding::Gzip),
        ("x-gzip", ContentEncoding::Gzip),
        ("ZSTD", ContentEncoding::Zstd),
        ("deflate", ContentEncoding::Deflate),
        ("snappy", ContentEncoding::Snappy),
        ("br", ContentEncoding::Unsupported),
    ] {
        let raw = format!("POST /w HTTP/1.1\r\nContent-Encoding: {hdr}\r\n\r\n");
        let head = parse_head(raw.as_bytes()).unwrap();
        assert_eq!(head.content_encoding, want, "header {hdr:?}");
    }
    let head = parse_head(b"POST /w HTTP/1.1\r\n\r\n").unwrap();
    assert_eq!(head.content_encoding, ContentEncoding::Identity);
}
```

- [ ] **Step 2:** `cargo test -p esm-http content_encoding` — expect FAIL (no such field/enum).
- [ ] **Step 3: Implement.** Replace `gzip_body: bool` in `Head` with `content_encoding: ContentEncoding`; in `parse_head`'s `content-encoding` arm match the six cases (case-insensitive, `"" | "none" | "identity"` → `Identity`, unknown → `Unsupported`). `Request::is_gzipped()` becomes `self.head.content_encoding == ContentEncoding::Gzip`; add `Request::content_encoding()`. Re-export `ContentEncoding` from `lib.rs`. Fix the one other use of `gzip_body` (grep for it).
- [ ] **Step 4:** `cargo test -p esm-http && cargo test -p esm-insert` — PASS (influx path must be unaffected).
- [ ] **Step 5:** `cargo fmt && cargo clippy -p esm-http -- -D warnings`
- [ ] **Step 6: Commit** — `feat: parse Content-Encoding into an enum in esm-http`

## Task 2: `esm-protoparser::util` — whole-body + streaming decompression

Port of `lib/protoparser/protoparserutil/compress_reader.go` (305 lines). Upstream semantics: `snappy` and `zstd` bodies are read in full then block-decoded with a size limit; `gzip`/`deflate`/identity are streamed.

**Files:**
- Create: `crates/esm-protoparser/src/util.rs`
- Modify: `crates/esm-protoparser/src/lib.rs` (`pub mod util;`), `crates/esm-protoparser/Cargo.toml` (+ `snap`, `zstd` workspace deps), root `Cargo.toml` (+ `snap = "1"` to `[workspace.dependencies]`)

**Interfaces:**
- Consumes: nothing new.
- Produces (used by every later handler task):

```rust
pub const MAX_INSERT_REQUEST_SIZE: usize = 32 * 1024 * 1024; // -maxInsertRequestSize default

#[derive(Debug)]
pub enum UtilError {
    Io(std::io::Error),
    UnsupportedEncoding(String),
    TooBig { limit: usize },
    Decompress(String),
    Callback(Box<dyn std::error::Error + Send + Sync>),
}
// impl Display + std::error::Error like stream::Error

/// Go: protoparserutil.ReadUncompressedData. Reads the whole body,
/// decompresses per `encoding` ("", "none", "identity", "gzip", "zstd",
/// "deflate", "snappy"), enforces `max_data_size` on the *decompressed*
/// size, then hands the bytes to `callback`.
pub fn read_uncompressed_data<R: std::io::Read>(
    r: R,
    encoding: &str,
    max_data_size: usize,
    callback: impl FnOnce(&[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
) -> Result<(), UtilError>;

/// Go: protoparserutil.GetUncompressedReader, for line-streaming parsers.
/// `snappy` is not supported here (upstream buffers it; callers use
/// read_uncompressed_data instead).
pub fn uncompressed_reader<'a, R: std::io::Read + 'a>(
    r: R,
    encoding: &str,
) -> Result<Box<dyn std::io::Read + 'a>, UtilError>;

/// Go: lib/encoding/snappy.Decode — block-format snappy with a
/// decompressed-size cap checked *before* allocating.
pub fn snappy_block_decode(src: &[u8], max_size: usize) -> Result<Vec<u8>, UtilError>;

/// Go: encoding.DecompressZSTDLimited.
pub fn zstd_decode(src: &[u8], max_size: usize) -> Result<Vec<u8>, UtilError>;
```

- [ ] **Step 1: Write failing tests** in `util.rs` (`#[cfg(test)]`), ported from `compress_reader_test.go`: one round-trip per encoding (compress a 1KiB payload with `flate2::write::GzEncoder`, `flate2::write::ZlibEncoder`, `zstd::encode_all`, `snap::raw::Encoder`, plus identity), asserting `read_uncompressed_data` yields the original bytes; plus `too_big_decompressed_data_is_rejected` (compress 2KiB, pass `max_data_size: 1024`, expect `UtilError::TooBig`), `unsupported_encoding_is_rejected` (`"br"` → `UnsupportedEncoding`), and `snappy_size_cap_checked_before_alloc` (hand-craft a snappy header claiming a huge decoded length via `snap::raw::decompress_len`, expect `TooBig`, no OOM).
- [ ] **Step 2:** `cargo test -p esm-protoparser util` — FAIL (module missing).
- [ ] **Step 3: Implement** `util.rs`. Mapping: `GetUncompressedReader` match → `uncompressed_reader` (`flate2::read::GzDecoder`, `flate2::read::ZlibDecoder`, `zstd::stream::read::Decoder`); `ReadUncompressedData` → read-to-Vec with `max_data_size + 1` cap then dispatch; snappy path uses `snap::raw::decompress_len` for the pre-alloc cap then `snap::raw::Decoder::decompress`. No pooling (thread-per-connection; same call made in the influx port).
- [ ] **Step 4:** `cargo test -p esm-protoparser` — PASS.
- [ ] **Step 5:** `cargo fmt && cargo clippy -p esm-protoparser -- -D warnings && cargo check -p esm-protoparser --target x86_64-pc-windows-gnu`
- [ ] **Step 6: Commit** — `feat: port protoparserutil decompression helpers to esm-protoparser`

## Task 3: shared handler helpers in `esm-insert::common`

Port `lib/protoparser/protoparserutil/extra_labels.go` + the query-param scan currently inlined in `influx.rs:52-73`, so the 8 upcoming handlers don't copy-paste it.

**Files:**
- Create: `crates/esm-insert/src/common.rs`
- Modify: `crates/esm-insert/src/lib.rs` (`mod common;`), `crates/esm-insert/src/influx.rs` (use the helper)

**Interfaces:**
- Produces:

```rust
/// Go: protoparserutil.GetExtraLabels. Collects `extra_label=name=value`
/// query args; error message must match upstream:
/// "`extra_label` query arg must have the format `name=value`; got {arg:?}"
pub(crate) fn get_extra_labels(req: &Request<'_>) -> Result<Vec<(String, String)>, InsertError>;

/// First occurrence of a query param, like Go url.Values.Get.
pub(crate) fn query_param(req: &Request<'_>, name: &str) -> Option<String>;

/// Appends extra labels to an arena with marshal_metric_name_raw —
/// the tail step every converter shares.
pub(crate) fn append_extra_labels(arena: &mut Vec<u8>, extra_labels: &[(String, String)]);
```

- [ ] **Step 1: Write failing tests** in `common.rs`: `extra_label_requires_name_value_format` (build a `Request` via the header-bytes trick used in esm-http tests, or test the pure parsing core `parse_extra_label(&str)` directly — prefer the latter: extract `fn parse_extra_label(value: &str) -> Result<(String, String), String>` and test `"env=prod"` → ok, `"envprod"` → the exact upstream error string).
- [ ] **Step 2:** `cargo test -p esm-insert common` — FAIL.
- [ ] **Step 3: Implement**; refactor `influx::insert_handler_for_http` to call `get_extra_labels` + `query_param` (behavior identical — its existing tests are the regression net).
- [ ] **Step 4:** `cargo test -p esm-insert` — PASS (all pre-existing influx tests green).
- [ ] **Step 5:** `cargo fmt && cargo clippy -p esm-insert -- -D warnings`
- [ ] **Step 6: Commit** — `refactor: extract shared extra_label/query helpers in esm-insert`

---

# Phase 1 — Prometheus remote-write (`/api/v1/write`)

The single highest-value protocol: makes esmetrics a drop-in remote-write target for Prometheus, Grafana Agent/Alloy, and vmagent.

## Task 4: protobuf wire reader + `prompb`

Port `lib/prompb/write_request_unmarshaler.go` (707 lines) with a minimal wire-format reader replacing `easyproto` (~150 lines: varint, tag, length-delimited, fixed64).

**Files:**
- Create: `crates/esm-protoparser/src/wire.rs`, `crates/esm-protoparser/src/prompb.rs`
- Modify: `crates/esm-protoparser/src/lib.rs`

**Interfaces:**
- Produces (Phase 3 reuses `wire`):

```rust
// wire.rs — pub(crate)
pub(crate) struct WireReader<'a> { /* src: &'a [u8], pos: usize */ }
impl<'a> WireReader<'a> {
    pub(crate) fn new(src: &'a [u8]) -> Self;
    pub(crate) fn is_eof(&self) -> bool;
    /// Returns (field_number, wire_type); wire_type in {0,1,2,5}.
    pub(crate) fn read_tag(&mut self) -> Result<(u32, u8), WireError>;
    pub(crate) fn read_varint(&mut self) -> Result<u64, WireError>;
    pub(crate) fn read_len_delim(&mut self) -> Result<&'a [u8], WireError>;
    pub(crate) fn read_double(&mut self) -> Result<f64, WireError>;   // wire type 1
    pub(crate) fn skip(&mut self, wire_type: u8) -> Result<(), WireError>;
}
pub(crate) fn zigzag_decode(v: u64) -> i64; // sint64 (unused by prompb; OTLP needs it — verify per-field before use)

// prompb.rs — labels/values borrow the decompressed body buffer.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Label<'a> { pub name: &'a [u8], pub value: &'a [u8] }
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample { pub value: f64, pub timestamp: i64 }
#[derive(Debug, Default)]
pub struct TimeSeries<'a> { pub labels: Vec<Label<'a>>, pub samples: Vec<Sample> }
#[derive(Debug, Default)]
pub struct WriteRequest<'a> { pub timeseries: Vec<TimeSeries<'a>> }

/// Go: WriteRequestUnmarshaler.UnmarshalProtobuf. Field map:
/// WriteRequest{1: repeated TimeSeries, 3: repeated MetricMetadata(skipped)}
/// TimeSeries{1: repeated Label, 2: repeated Sample, others skipped}
/// Label{1: name bytes, 2: value bytes} Sample{1: double value, 2: varint(int64) timestamp}
/// Unknown fields are skipped by wire type, like easyproto.
pub fn unmarshal_write_request<'a>(src: &'a [u8]) -> Result<WriteRequest<'a>, WireError>;
```

Note: sample timestamps are proto3 `int64` (plain varint, two's-complement via `as i64`), NOT zigzag. Labels stay `&[u8]` — they feed `marshal_metric_name_raw` directly; no UTF-8 validation (Go does none either).

- [ ] **Step 1: Write failing tests** in `prompb.rs`, ported from `write_request_unmarshaler_test.go`. Build test payloads with a tiny `#[cfg(test)]` writer (append_tag/append_varint/append_bytes helpers, ~30 lines) rather than a protobuf dep. Cases: `empty_write_request`, `single_series_single_sample` (labels `__name__=foo`, `job=x`; sample 42.5 @ 1727879909390 — assert exact structs), `multiple_series_reuse`, `unknown_fields_are_skipped` (inject field 9/wiretype 2 and field 3 metadata; parse must succeed), `truncated_input_errors`, `negative_timestamp_roundtrips` (timestamp `-1` encodes as 10-byte varint).
- [ ] **Step 2:** `cargo test -p esm-protoparser prompb` — FAIL.
- [ ] **Step 3: Implement** `wire.rs` then `prompb.rs` (function mapping: `UnmarshalProtobuf` → `unmarshal_write_request`, `unmarshalTimeSeries` → private `unmarshal_time_series`, label/sample loops inline). Buffers grow-and-reuse is unnecessary (no pooling); allocate Vecs per request.
- [ ] **Step 4:** `cargo test -p esm-protoparser` — PASS.
- [ ] **Step 5:** `cargo fmt && cargo clippy -p esm-protoparser -- -D warnings`
- [ ] **Step 6: Commit** — `feat: port prompb WriteRequest unmarshaling with a minimal wire reader`

## Task 5: remote-write stream decode

Port `lib/protoparser/promremotewrite/stream/streamparser.go` (111 lines): body → (snappy | zstd with mutual fallback) → `WriteRequest`.

**Files:**
- Create: `crates/esm-protoparser/src/promremotewrite.rs`
- Modify: `crates/esm-protoparser/src/lib.rs`

**Interfaces:**
- Consumes: `util::{read_uncompressed_data is NOT used here — the body is read raw}`, `util::{snappy_block_decode, zstd_decode, MAX_INSERT_REQUEST_SIZE}`, `prompb::unmarshal_write_request`.
- Produces:

```rust
/// Go: stream.Parse. `encoding` is the Content-Encoding header value:
/// "zstd" tries zstd-then-snappy; anything else tries snappy-then-zstd
/// (vmagent persistent-queue compatibility, see upstream #5301).
/// The callback must not hold the series after returning.
pub fn parse<R: std::io::Read>(
    r: R,
    encoding: &str,
    callback: impl FnOnce(&[prompb::TimeSeries<'_>]) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
) -> Result<(), Error>;   // Error enum mirroring stream::Error (Io / Decompress / TooBig / Callback)
```

- [ ] **Step 1: Write failing tests**: `parses_snappy_body` (encode a one-series WriteRequest with the test writer from Task 4, `snap::raw::Encoder`, no encoding header → callback sees the series), `parses_zstd_body_with_zstd_encoding`, `snappy_body_with_zstd_header_falls_back` (the #5301 case), `decompressed_size_limit_enforced`, `garbage_body_errors`.
- [ ] **Step 2:** `cargo test -p esm-protoparser promremotewrite` — FAIL.
- [ ] **Step 3: Implement** (read body to Vec capped at `MAX_INSERT_REQUEST_SIZE`, decode with fallback exactly as `parseRequestBody`, keep the original error from the *first* attempted codec like upstream, then unmarshal + callback).
- [ ] **Step 4:** `cargo test -p esm-protoparser` — PASS.
- [ ] **Step 5:** `cargo fmt && cargo clippy -p esm-protoparser -- -D warnings`
- [ ] **Step 6: Commit** — `feat: port prometheus remote-write stream decoding`

## Task 6: remote-write insert handler + routes + e2e

Port `app/vminsert/promremotewrite/request_handler.go` (85 lines) and register routes.

**Files:**
- Create: `crates/esm-insert/src/promremotewrite.rs`, `crates/esm-insert/tests/promremotewrite_write.rs`
- Modify: `crates/esm-insert/src/lib.rs` (module + routes)

**Interfaces:**
- Consumes: `esm_protoparser::promremotewrite::parse`, `common::{get_extra_labels, append_extra_labels}`, `ConcurrencyLimiter`, `RowSink`, `esm_storage::marshal_metric_name_raw`, `Request::content_encoding()`.
- Produces: `pub(crate) fn insert_handler<S: RowSink>(sink: &S, limiter: &ConcurrencyLimiter, req: &mut Request<'_>) -> Result<(), InsertError>`.

Conversion (Go `insertRows`): for each `TimeSeries`, marshal labels in input order — the `__name__` label becomes the trailing empty-key metric-group pair `(b"", value)`, all other labels as `(name, value)` pairs; append extra labels; emit one `MetricRow` per sample sharing the same `metric_name_raw` slice. Reuse the influx `ConvertCtx` arena pattern (thread-local, entries + `recycle_rows`) — copy the pattern into this module, do not generalize it yet (first duplication; extract on the second if it bites).

Routes in `InsertHandlers::handle`:

```rust
"/api/v1/write" | "/prometheus/api/v1/write" | "/api/v1/push" | "/prometheus/api/v1/push" => {
    match promremotewrite::insert_handler(&self.sink, &self.limiter, req) {
        Ok(()) => w.write_status(204),
        Err(err) => { /* same error-writing block as influx */ }
    }
    true
}
```

- [ ] **Step 1: Write failing unit tests** in `promremotewrite.rs` for the converter (same shape as `influx.rs` tests: `CollectSink` + `MetricName::unmarshal_raw` assertions): `name_label_becomes_metric_group`, `label_order_is_preserved`, `one_row_per_sample`, `extra_labels_appended_last`, `series_without_name_label_still_ingests` (metric group empty — upstream does not reject), `raw_encoding_matches_marshal_metric_name_raw`.
- [ ] **Step 2:** `cargo test -p esm-insert promremotewrite` — FAIL.
- [ ] **Step 3: Implement** converter + handler + routes.
- [ ] **Step 4:** `cargo test -p esm-insert` — PASS.
- [ ] **Step 5: Write the integration test** `tests/promremotewrite_write.rs` modeled line-for-line on `tests/influx_write.rs` (esm-http `Server::bind("127.0.0.1:0")` + `MockSink`): POST a snappy-encoded WriteRequest to `/api/v1/write`, assert 204 + decoded rows; POST garbage, assert 400; POST with blocked sink returning error, assert 503 body. Run: `cargo test -p esm-insert --test promremotewrite_write` — PASS.
- [ ] **Step 6:** `cargo fmt && cargo clippy -p esm-insert -- -D warnings && cargo check -p esm-insert --target x86_64-pc-windows-gnu`
- [ ] **Step 7: Phase gate.** Real-client smoke: `cargo run -p esmetrics` locally, point a stock Prometheus (or `promtool tsdb` remote-write bench, or vmagent) at `http://127.0.0.1:8428/api/v1/write`, verify series arrive via `/api/v1/query`. Record the command used in the commit body.
- [ ] **Step 8: Update docs**: PORTING.md scope table row (`app/vminsert/promremotewrite | esm-insert::promremotewrite | done`), UPSTREAM `VM_SCOPE` += `lib/prompb lib/protoparser/promremotewrite lib/protoparser/protoparserutil app/vminsert/promremotewrite`.
- [ ] **Step 9: Commit** — `feat: accept prometheus remote-write on /api/v1/write`

---

# Phase 2 — Text imports: prometheus exposition, vmimport, CSV

## Task 7: prometheus exposition-text parser

Port `lib/protoparser/prometheus/parser.go` (959 lines; skip the metadata half — `UnmarshalWithMetadata`'s metadata rows are dropped, matching "prommetadata out of scope").

**Files:**
- Create: `crates/esm-protoparser/src/prometheus.rs`
- Modify: `crates/esm-protoparser/src/lib.rs`

**Interfaces:**
- Produces (mirrors the influx parser shape):

```rust
#[derive(Debug, Default, PartialEq)]
pub struct Row<'a> {
    pub metric: &'a str,
    pub tags: Vec<Tag<'a>>,          // Tag { key: &'a str, value: Cow<'a, str> } — value unescapes \\ \" \n
    pub value: f64,
    pub timestamp: i64,              // 0 = absent (caller fills default)
}
#[derive(Debug, Default)]
pub struct Rows<'a> { /* rows, tags pool */ }
impl<'a> Rows<'a> {
    /// Invalid lines are skipped with the error passed to `err_logger`
    /// (Go: unmarshal with errLogger). Comment/blank lines skipped.
    pub fn unmarshal(&mut self, s: &'a str, err_logger: impl FnMut(&str));
    pub fn rows(&self) -> &[Row<'a>];
}
```

- [ ] **Step 1: Write failing tests** ported from `parser_test.go` (upstream has extensive cases — port at minimum): plain `foo 123`, tags `foo{bar="baz",x="y"} 1 1727879909390`, escaped tag values (`\"`, `\\`, `\n`), `NaN`/`+Inf`/`-Inf` values, exponent values, missing timestamp → 0, trailing whitespace, invalid lines skipped (`foo{unclosed 1`), empty metric name skipped, `# HELP`/`# TYPE` comments skipped, exemplar suffix `# {...}` after value ignored (OpenMetrics tolerance — check upstream `parser.go` behavior at v1.146.0 and match it exactly).
- [ ] **Step 2:** `cargo test -p esm-protoparser prometheus` — FAIL.
- [ ] **Step 3: Implement** (function mapping: `Rows.Unmarshal`→`unmarshal`, `unmarshalRow`, `unmarshalTags`, `prevBackslashesCount`/`unescapeValue` helpers; zero-copy `&'a str` slices, `Cow::Owned` only when a tag value contains a backslash).
- [ ] **Step 4:** `cargo test -p esm-protoparser` — PASS.
- [ ] **Step 5:** `cargo fmt && cargo clippy -p esm-protoparser -- -D warnings`
- [ ] **Step 6: Commit** — `feat: port prometheus exposition-text parser`

## Task 8: `/api/v1/import/prometheus` handler

Port `lib/protoparser/prometheus/stream/streamparser.go` + `app/vminsert/prometheusimport/request_handler.go` (80 lines), including the pushgateway-style `/api/v1/import/prometheus/metrics/job/...` path→labels mapping (`app/vminsert/main.go:146-162` + `protoparserutil` path parsing).

**Files:**
- Create: `crates/esm-insert/src/prometheusimport.rs`, `crates/esm-insert/tests/prometheusimport_write.rs`
- Modify: `crates/esm-insert/src/lib.rs` (routes: prefix-match `path.starts_with("/api/v1/import/prometheus")` and `/prometheus/`-prefixed twin), `crates/esm-protoparser/src/prometheus.rs` (+ `pub mod stream`-style `parse_stream` fn or a sibling `prometheus_stream.rs` if the file nears 800 lines)

**Interfaces:**
- Consumes: `prometheus::Rows`, `util::uncompressed_reader`, `read_lines_block` (make the existing `stream::read_lines_block` `pub(crate)`-shared or move it into `util.rs` — move it: it is protoparserutil upstream anyway; update `stream.rs` imports).
- Produces: `esm_protoparser::prometheus::parse_stream<R: Read>(r, encoding: &str, default_timestamp: i64, err_logger, callback: FnMut(&[Row<'_>]) -> CallbackResult) -> Result<(), Error>` and `esm_insert` handler `prometheusimport::insert_handler(sink, limiter, req, path_suffix: &str)`.

Handler details: query args `timestamp` (fallback default timestamp, ms) + `extra_label`; rows with `timestamp == 0` get the default (or current time when no arg — Go `protoparserutil.GetTimestamp`); path suffix `metrics/job/<job>/<label>/<value>/...` pairs become extra labels prepended before `extra_label` args (port `protoparserutil`'s pushgateway path parsing — it lives in `app/vminsert/main.go` `getPushgatewayLabels` at v1.146.0; verify location before porting).

- [ ] **Step 1: Write failing converter tests** in `prometheusimport.rs`: `metric_name_becomes_group_tags_preserved`, `default_timestamp_applied_to_zero_rows`, `pushgateway_path_labels` (`metrics/job/backup/instance/host1` → labels `job=backup`, `instance=host1`), `invalid_lines_skipped_request_still_204`.
- [ ] **Step 2:** `cargo test -p esm-insert prometheusimport` — FAIL.
- [ ] **Step 3: Implement** stream fn + handler + routes.
- [ ] **Step 4:** `cargo test -p esm-insert && cargo test -p esm-protoparser` — PASS.
- [ ] **Step 5: Integration test** `tests/prometheusimport_write.rs`: POST plain text body, gzip body (`Content-Encoding: gzip`), and the job-path variant; assert decoded rows. PASS.
- [ ] **Step 6:** `cargo fmt && cargo clippy -- -D warnings` (workspace)
- [ ] **Step 7: Commit** — `feat: accept prometheus text import on /api/v1/import/prometheus`

## Task 9: vmimport (`/api/v1/import`)

Port `lib/protoparser/vmimport/parser.go` (245 lines, JSON-lines `{"metric":{...},"values":[...],"timestamps":[...]}`) + `stream/` + `app/vminsert/vmimport/request_handler.go` (104 lines). Use `serde_json::Value`? No — port the shape with a typed struct + `serde_json::from_str`, but preserve upstream's special float handling (`"Infinity"`, `"-Infinity"`, `"NaN"` as strings, and numbers) via a custom `Deserialize` or a post-pass on `serde_json::Value`. Decision: parse each line to `serde_json::Value` (fastjson analogue), then walk it exactly like `Row.unmarshal` — simplest faithful mapping of `getSpecialFloat64`.

**Files:**
- Create: `crates/esm-protoparser/src/vmimport.rs`, `crates/esm-insert/src/vmimport.rs`, `crates/esm-insert/tests/vmimport_write.rs`
- Modify: both `lib.rs` (module + route `"/api/v1/import" | "/prometheus/api/v1/import"`), `crates/esm-protoparser/Cargo.toml` (+ `serde_json` workspace dep)

**Interfaces:**
- Produces: `vmimport::Row { tags: Vec<(Vec<u8>, Vec<u8>)>, values: Vec<f64>, timestamps: Vec<i64> }` (owned — JSON parsing allocates anyway), `Rows::unmarshal(&mut self, s: &str, err_logger)` skipping invalid lines, `parse_stream(r, encoding, err_logger, callback)`, handler `vmimport::insert_handler`. The `__name__` tag maps to the metric group; `values.len() != timestamps.len()` is an invalid row (skipped, logged).

- [ ] **Step 1: Write failing parser tests** ported from `parser_test.go`: happy path with 2 tags/3 samples, `"values":["Infinity",1.23,"NaN"]` special floats, mismatched values/timestamps lengths skipped, missing `metric` object skipped, empty line skipped.
- [ ] **Step 2:** `cargo test -p esm-protoparser vmimport` — FAIL.
- [ ] **Step 3: Implement** parser + stream + handler (converter: tags in input order, `__name__` → trailing group pair, one MetricRow per (value, timestamp) pair, extra labels).
- [ ] **Step 4:** `cargo test -p esm-protoparser && cargo test -p esm-insert` — PASS.
- [ ] **Step 5: Integration test** with a body exported by upstream: run real VM (`/api/v1/export`) once, paste 3 captured lines as the fixture. PASS.
- [ ] **Step 6:** fmt/clippy. **Commit** — `feat: accept vmimport JSON lines on /api/v1/import`

## Task 10: CSV import (`/api/v1/import/csv`)

Port `lib/protoparser/csvimport/` (524 lines: `parser.go`, `column_descriptor.go`, `scanner.go`, `streamparser.go`) + `app/vminsert/csvimport/request_handler.go` (59 lines).

**Files:**
- Create: `crates/esm-protoparser/src/csvimport.rs` (parser + scanner + ColumnDescriptor in one file; ~500 lines Rust, under the cap), `crates/esm-insert/src/csvimport.rs`, `crates/esm-insert/tests/csvimport_write.rs`
- Modify: both `lib.rs` (route `"/api/v1/import/csv" | "/prometheus/api/v1/import/csv"`)

**Interfaces:**
- Produces: `csvimport::ColumnDescriptor` + `pub fn parse_column_descriptors(format: &str) -> Result<Vec<ColumnDescriptor>, String>` (the `format=` query arg grammar `<pos>:<type>:<name>`, types `metric|label|time:<fmt>` with time formats `unix_s|unix_ms|unix_ns|rfc3339`), `Rows::unmarshal_detect_header`, `parse_stream`, handler. RFC3339 parsing: no chrono — port upstream's approach; upstream uses `time.Parse(time.RFC3339, ...)`. Implement a ~40-line RFC3339→unix-ms function with tests (repo precedent: esm-backup has `timeutil.rs` — check it first and reuse if it already parses RFC3339).

- [ ] **Step 1: Write failing tests** ported from `column_descriptor_test.go` + `parser_test.go`: descriptor grammar (valid specs, duplicate positions rejected, missing metric column rejected), row parse `GOOD: "sensor-1,23.5,1447116400"` with format `1:label:device,2:metric:temperature,3:time:unix_s`, header-row autodetect, quoted fields with commas, empty value cells skipped.
- [ ] **Step 2:** `cargo test -p esm-protoparser csvimport` — FAIL.
- [ ] **Step 3: Implement** parser; then handler (format arg required → 400 without it; metric column name becomes metric group; labels from label columns; `extra_label` appended).
- [ ] **Step 4:** tests PASS. **Step 5:** integration test (plain + gzip). **Step 6:** fmt/clippy both crates + windows-gnu check.
- [ ] **Step 7: Update docs** (PORTING.md rows for prometheusimport/vmimport/csvimport; UPSTREAM `VM_SCOPE` += `lib/protoparser/prometheus lib/protoparser/vmimport lib/protoparser/csvimport app/vminsert/prometheusimport app/vminsert/vmimport app/vminsert/csvimport`).
- [ ] **Step 8: Commit** — `feat: accept CSV import on /api/v1/import/csv`

---

# Phase 3 — OpenTelemetry (OTLP/HTTP)

## Task 11: OTLP protobuf decode

Port `lib/protoparser/opentelemetry/pb/pb.go` (1812 lines — decode direction only; the marshal direction is vmagent-only, skip it) reusing `wire.rs`. Messages: `ExportMetricsServiceRequest → ResourceMetrics → ScopeMetrics → Metric → {Gauge, Sum, Histogram, Summary, ExponentialHistogram} → NumberDataPoint/HistogramDataPoint/SummaryDataPoint/ExponentialHistogramDataPoint`, `KeyValue/AnyValue` attributes, `Resource`.

**Files:**
- Create: `crates/esm-protoparser/src/opentelemetry/mod.rs`, `crates/esm-protoparser/src/opentelemetry/pb.rs`
- Modify: `crates/esm-protoparser/src/lib.rs`, `wire.rs` (widen helpers to `pub(crate)` as needed; add `read_fixed64`/`read_sfixed64` if Histogram bounds need them — check `pb.go` field wire types while porting)

**Interfaces:**
- Produces: owned structs mirroring `pb.go` names (`ExportMetricsServiceRequest::unmarshal(src: &[u8]) -> Result<Self, WireError>`; `AnyValue` rendered to `String` exactly like upstream `AnyValue.FormatString` — bool/int/double/string/bytes(base64)/array/kvlist). Timestamps are `time_unix_nano: u64`; values `f64`/`i64` per `NumberDataPoint` oneof (field 4 double / field 6 sfixed64 — **verify against pb.go**, do not trust this line).
- JSON ingestion (`pb_json.go`) is skipped: upstream single-node only accepts protobuf OTLP bodies unless `Content-Type: application/json`— **check `stream/streamparser.go` at v1.146.0**: if it supports JSON, add a follow-up task; note the finding in the commit body either way.

- [ ] **Step 1: Write failing tests**: build payloads with the test wire-writer — `gauge_double_datapoint`, `sum_with_attributes` (two KeyValue attrs incl. an int value formatted as `"42"`), `histogram_buckets_and_bounds`, `summary_quantiles`, `resource_attributes_decoded`, `unknown_fields_skipped`.
- [ ] **Step 2:** FAIL. **Step 3: Implement** decode (one `unmarshal` per message, systematic field-number match, `skip` on unknowns). **Step 4:** PASS. **Step 5:** fmt/clippy.
- [ ] **Step 6: Commit** — `feat: port OTLP metrics protobuf decoding`

## Task 12: OTLP conversion + `/opentelemetry/v1/metrics` handler

Port `lib/protoparser/opentelemetry/stream/streamparser.go` (255) + `sanitize.go` (204, flags fixed: `-opentelemetry.usePrometheusNaming=false`) + `app/vminsert/opentelemetry/request_handler.go` (95).

**Files:**
- Create: `crates/esm-protoparser/src/opentelemetry/sanitize.rs`, `crates/esm-insert/src/opentelemetry.rs`, `crates/esm-insert/tests/opentelemetry_write.rs`
- Modify: `crates/esm-insert/src/lib.rs` (routes `"/opentelemetry/api/v1/push" | "/opentelemetry/v1/metrics"`)

**Interfaces:**
- Consumes: `opentelemetry::pb::*`, `util::read_uncompressed_data` (OTLP bodies are gzip-or-identity), `common` helpers.
- Produces: `opentelemetry::parse_stream(r, encoding, callback: FnMut(&[Row]) -> ...)` where `Row` is the flattened `{metric_name, labels, value, timestamp_ms}` — port upstream's conversions exactly: Gauge/Sum → plain samples; monotonic cumulative Sum keeps name; Histogram → `<name>_bucket{le=...}` cumulative counts + `_sum` + `_count`; Summary → `<name>{quantile=...}` + `_sum` + `_count`; resource attributes → labels per upstream rules; scope/`target_info` handling **as implemented in streamparser.go** (verify; do not improvise).

- [ ] **Step 1: Failing tests** ported from `stream/streamparser_test.go` (upstream has golden conversions — port ≥6 cases incl. histogram bucket cumulation and staleness/NaN flags handling if present).
- [ ] **Step 2:** FAIL. **Step 3: Implement** conversion + handler + routes (response on success: 200 with empty body? — upstream writes `ExportMetricsServiceResponse`; **check request_handler.go** and match, likely `w.write_status(200)` with protobuf empty response bytes).
- [ ] **Step 4:** PASS. **Step 5:** integration test: gzip + identity protobuf bodies → rows; garbage → 400. **Step 6:** fmt/clippy + windows-gnu check.
- [ ] **Step 7: Update docs** (PORTING.md + VM_SCOPE += `lib/protoparser/opentelemetry app/vminsert/opentelemetry`). Real-client smoke: `opentelemetry-collector` with otlphttp exporter pointed at the server, or `telemetrygen metrics --otlp-http --otlp-endpoint 127.0.0.1:8428 --otlp-http-url-path /opentelemetry/v1/metrics --otlp-insecure`; record in commit body.
- [ ] **Step 8: Commit** — `feat: accept OTLP metrics on /opentelemetry/v1/metrics`

---

# Phase 4 — Graphite & OpenTSDB (TCP/UDP/HTTP)

## Task 13: graphite + opentsdb telnet parsers

Port `lib/protoparser/graphite/parser.go` (269) and `lib/protoparser/opentsdb/parser.go` (216). Both are line formats: graphite `metric.path;tag1=v1 123.45 1727879909` (tags optional, `-1` timestamp = now); opentsdb `put <metric> <ts> <value> <tag>=<v> ...`.

**Files:**
- Create: `crates/esm-protoparser/src/graphite.rs`, `crates/esm-protoparser/src/opentsdb.rs`
- Modify: `crates/esm-protoparser/src/lib.rs`

**Interfaces:** `Row<'a>/Rows<'a>` + `unmarshal(&mut self, s: &'a str)` (invalid lines skipped) mirroring the influx/prometheus parser shape; plus `parse_stream` for each (reusing `read_lines_block` from `util.rs`; graphite timestamps in seconds → ms, `detect_timestamp`-style rules per upstream `streamparser.go`).

- [ ] **Step 1: Failing tests** ported from `parser_test.go` of each: graphite with/without tags, semicolon tag syntax, whitespace trimming, `-1`/absent timestamp → 0, float and exponent values; opentsdb `put` keyword required, tags parsed, invalid lines skipped.
- [ ] **Step 2:** FAIL. **Step 3: Implement.** **Step 4:** PASS. **Step 5:** fmt/clippy.
- [ ] **Step 6: Commit** — `feat: port graphite and opentsdb telnet parsers`

## Task 14: ingest servers (TCP/UDP) + binary flags

Port `lib/ingestserver/graphite/server.go` + `lib/ingestserver/opentsdb/server.go` (thread-per-connection TCP accept loop + one UDP reader thread each; opentsdb's TCP listener multiplexes telnet and HTTP upstream — port telnet-only here, HTTP is Task 15's separate listener, and note the deviation in PORTING.md).

**Files:**
- Create: `crates/esm-insert/src/ingestserver.rs`
- Modify: `crates/esm-insert/src/lib.rs`, `crates/esmetrics/src/flags.rs` (+ `-graphiteListenAddr`, `-opentsdbListenAddr`, `-opentsdbHTTPListenAddr`, empty default = disabled), `crates/esmetrics/src/main.rs` + `wiring.rs` (start servers when flags set, stop on shutdown signal — follow the existing esm-http Server start/stop pattern in `main.rs`)

**Interfaces:**
- Produces:

```rust
pub struct IngestServer { /* join handles + shutdown flag */ }
/// Serves graphite plaintext on tcp+udp at `addr`. Each line batch goes
/// through the same converter as the HTTP path.
pub fn serve_graphite<S: RowSink + 'static>(addr: &str, sink: Arc<S>) -> io::Result<IngestServer>;
pub fn serve_opentsdb_telnet<S: RowSink + 'static>(addr: &str, sink: Arc<S>) -> io::Result<IngestServer>;
impl IngestServer { pub fn stop(self); }  // closes listener, joins threads
```

plus `esm-insert/src/graphite.rs` converter `insert_rows(sink, rows: &[graphite::Row]) -> Result<(), String>` (metric → group, tags as labels) shared by TCP/UDP; same for opentsdb.

- [ ] **Step 1: Failing tests** in `ingestserver.rs`: bind `127.0.0.1:0`, connect with `TcpStream`, write two graphite lines, assert MockSink rows (poll with timeout, pattern exists in `tests/influx_write.rs`); UDP equivalent with `UdpSocket::send_to`; `stop()` joins cleanly.
- [ ] **Step 2:** FAIL. **Step 3: Implement** servers + converters. **Step 4:** PASS.
- [ ] **Step 5:** wire flags in esmetrics: flags default empty → no listener; manual smoke: `echo "foo.bar 1 $(date +%s)" | nc 127.0.0.1 2003` against a locally run binary, verify via `/api/v1/query`.
- [ ] **Step 6:** fmt/clippy + windows-gnu check (UDP/TCP code must compile on windows).
- [ ] **Step 7: Commit** — `feat: graphite and opentsdb TCP/UDP ingestion listeners`

## Task 15: OpenTSDB HTTP (`/api/put`)

Port `lib/protoparser/opentsdbhttp/` (219: JSON single-object-or-array of `{metric, timestamp, value, tags}`) + `app/vminsert/opentsdbhttp/request_handler.go` (67). Runs on the dedicated `-opentsdbHTTPListenAddr` esm-http Server instance (upstream parity), reusing `InsertHandlers`-style routing: `"/api/put" | "/opentsdb/api/put"`.

**Files:**
- Create: `crates/esm-protoparser/src/opentsdbhttp.rs`, `crates/esm-insert/src/opentsdbhttp.rs`, `crates/esm-insert/tests/opentsdbhttp_write.rs`
- Modify: `crates/esm-insert/src/lib.rs`, `crates/esmetrics/src/main.rs` (second esm-http Server on the flag'd port with a handler that only routes opentsdbhttp)

- [ ] **Step 1: Failing parser tests** from `parser_test.go`: single object, array, integer + float timestamps (seconds vs ms detection per upstream), string values coerced?, missing tags object, invalid entries rejected.
- [ ] **Step 2:** FAIL. **Step 3: Implement** (serde_json::Value walk, like vmimport). **Step 4:** PASS. **Step 5:** integration test (POST both shapes + gzip). **Step 6:** fmt/clippy.
- [ ] **Step 7: Update docs** (PORTING.md rows + VM_SCOPE += `lib/protoparser/graphite lib/protoparser/opentsdb lib/protoparser/opentsdbhttp lib/ingestserver app/vminsert/graphite app/vminsert/opentsdb app/vminsert/opentsdbhttp`).
- [ ] **Step 8: Commit** — `feat: opentsdb HTTP /api/put listener`

---

# Phase 5 — Datadog v1/v2 series

## Task 16: datadog parsers + handlers + agent stub endpoints

Port `lib/protoparser/datadogutil` (57), `datadogv1` (98), `datadogv2` (314, JSON only — protobuf v2 bodies are rejected with 400 like upstream when `Content-Type: application/x-protobuf`; **verify upstream v1.146.0 behavior** — if it supports protobuf v2, reuse `wire.rs`) + `app/vminsert/datadogv1|datadogv2/request_handler.go` + the fixed-response endpoints from `app/vminsert/main.go:302-331`.

**Files:**
- Create: `crates/esm-protoparser/src/datadog.rs` (v1+v2+util in one module), `crates/esm-insert/src/datadog.rs`, `crates/esm-insert/tests/datadog_write.rs`
- Modify: `crates/esm-insert/src/lib.rs`

Routes (all under `handle`): `/datadog/api/v1/series` (v1 insert), `/datadog/api/v2/series` (v2 insert), and stubs answering exactly like upstream main.go: `/datadog/api/v1/validate` → 200 `{"valid":true}`, `/datadog/api/v1/check_run` → 202 `{"status":"ok"}`, `/datadog/intake` → 200 `{}`, `/datadog/api/v1/metadata` → 201 `{}` (**copy status codes from main.go, don't trust this line**).

**Interfaces:** `datadog::v1::Request { series: Vec<Series> }` / `datadog::v2::Request` parsed via serde_json::Value walk; converter maps `metric` → group, `tags: ["k:v", ...]` split on first `:` (datadogutil.SplitTag), `host`/`device` → labels, v2 `resources` → labels; sanitize metric names per `datadogutil` rules (fixed flag defaults).

- [ ] **Step 1: Failing tests** ported from both `parser_test.go`s + converter tests (`tag_without_colon_gets_no_value` etc. per datadogutil tests).
- [ ] **Step 2:** FAIL. **Step 3: Implement.** **Step 4:** PASS. **Step 5:** integration test incl. one real `datadog-agent` captured payload fixture if available; otherwise the upstream test fixture bodies. Stub endpoints asserted for exact status+body.
- [ ] **Step 6:** fmt/clippy + windows-gnu check.
- [ ] **Step 7: Final docs pass**: PORTING.md scope rows for datadog; UPSTREAM VM_SCOPE += `lib/protoparser/datadogutil lib/protoparser/datadogv1 lib/protoparser/datadogv2 app/vminsert/datadogv1 app/vminsert/datadogv2`; PORTING.md "out of scope" line updated to remove the ported protocols and keep `native/newrelic/zabbixconnector/datadogsketches/firehose/prommetadata`.
- [ ] **Step 8: Commit** — `feat: accept datadog v1/v2 series ingestion`

---

# Final gate (after Phase 5)

- [ ] `cargo test --workspace` green; `cargo clippy --workspace -- -D warnings`; `cargo check --workspace --target x86_64-pc-windows-gnu`.
- [ ] Windows spot-check on agent-6.home per `docs/PORTING.md` conventions (MSVC build; benchmark conventions in memory).
- [ ] `scripts/upstream-diff.sh` runs clean against the updated `VM_SCOPE`.
- [ ] Update README feature matrix (supported ingestion protocols list).

## Self-Review Notes

- Spec coverage: remote-write (T4–6), prometheus text (T7–8), vmimport (T9), csv (T10), OTLP (T11–12), graphite (T13–14), opentsdb telnet+HTTP (T13–15), datadog (T16). `native` explicitly descoped with rationale (Global Constraints). ✓
- Marked-uncertain upstream details (OTLP JSON support, OTLP success response, datadog v2 protobuf, stub status codes, pushgateway label helper location) are flagged inline with "verify against upstream" — implementers must read the referenced Go file before coding that line; guesses are not to be trusted. ✓
- Type consistency: all handlers use `insert_handler(sink, limiter, req) -> Result<(), InsertError>`; parsers use `Row<'a>/Rows<'a> + parse_stream(r, encoding, ...)`; `ContentEncoding::as_str()` bridges esm-http → esm-protoparser everywhere. ✓
