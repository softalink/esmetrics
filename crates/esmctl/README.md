# esmctl

A Rust port of `app/vmctl` — the EsMetrics/VictoriaMetrics command-line
migration tool. This port implements the **`vm-native`** (VM↔EsMetrics native
streaming), **`opentsdb`**, **`remote-read`** (Prometheus remote-read, both the
`SAMPLES` and `STREAMED_XOR_CHUNKS` response types), and **`influx`** migration
modes, the **`verify-block`** validator, and the shared JSON-import destination
(`vm.Importer`). A Ctrl-C / SIGTERM handler cancels an in-progress migration
cleanly (aborting retry waits and stopping workers between requests), and each
importer-backed mode prints an end-of-run stats summary (idle/import time,
total samples, samples/s, bytes, bytes/s, request and retry counts) — the
`vmctl_importer_*` counters have no scrape endpoint in a one-shot CLI, so they
are surfaced as this summary instead.

### Out of scope: `prometheus` / `thanos` / `mimir`

These three modes are **not** ported. They all read Prometheus **TSDB blocks**
directly, which requires a full on-disk block reader that esmetrics does not
have (and that is orthogonal to the HTTP-based migration paths above):

- **`prometheus`** opens a snapshot directory via Prometheus's
  `tsdb.DBReadOnly` and iterates every block's index + chunk files. Porting it
  means porting Prometheus's `tsdb/index` reader (symbol table, postings lists,
  series records, label indices), the `tsdb/chunks` segment reader, and the
  block/meta.json/tombstones layout — several thousand lines of storage-format
  parsing. The Gorilla **XOR chunk decoder is already ported** (`chunkenc.rs`,
  for remote-read `STREAMED_XOR_CHUNKS`) and would be reused, so the concrete
  blocker is specifically the *index/block* on-disk format, not chunk decoding.
- **`thanos`** additionally reads blocks from object storage and decodes
  Thanos's downsampled *aggregated* chunks (`thanos/aggr_chunk.go`).
- **`mimir`** additionally reads Grafana Mimir's object-storage block layout.

Because on-disk/byte-format compatibility is an explicit non-goal of this port,
building a Prometheus-block reader purely to serve these three migration
sources is not justified; users with Prometheus/Thanos/Mimir data should
migrate via the `remote-read` mode instead.

## Usage

```
esmctl vm-native \
  --vm-native-src-addr http://source:8428 \
  --vm-native-dst-addr http://dest:8428 \
  --vm-native-filter-time-start 2024-01-01T00:00:00Z \
  [--vm-native-filter-time-end now] \
  [--vm-native-filter-match '{__name__!=""}'] \
  [--vm-native-step-interval month] \
  [--vm-concurrency 2] \
  [--vm-extra-label datacenter=eu] \
  [-s]
```

Run `esmctl --help` for the full flag list.

## What is ported (`vm-native`)

- Streaming native export → import between two endpoints, fully faithful to
  `vm_native.go`: metric-name exploration (`/api/v1/label/__name__/values`),
  per-metric request planning, and the whole-range single-request mode
  (`--vm-native-disable-per-metric-migration`).
- Time-range splitting by `month`/`week`/`day`/`hour`/`minute`
  (`stepper.SplitDateRange`, with month ranges aligned to the 1st) and the
  `--vm-native-filter-time-reverse` ordering.
- Time parsing (`vmctlutil.ParseTime`): RFC3339, fixed-length calendar
  prefixes, unix timestamps, and `now`-relative durations.
- Per-metric MetricsQL match construction (`buildMatchWithFilter`) via
  `esm-metricsql`.
- Exponential-backoff retries (`backoff.Backoff`), concurrent import workers
  (`--vm-concurrency`), source/destination basic-auth / bearer-token /
  `^^`-separated custom headers, `--vm-extra-label` injection on the import
  path, the intercluster tenant discovery mode (`--vm-intercluster`), and the
  binary-protocol toggle (`--vm-native-disable-binary-protocol`).

The streaming path uses `reqwest`'s blocking client: the export response body
is handed directly to the import request body, so data is never fully
buffered in memory. Verified end-to-end against mock source/destination
servers.

## What is ported (`opentsdb`)

Faithful to `app/vmctl/opentsdb/*` + `app/vmctl/opentsdb.go`:

- Metric discovery (`/api/suggest`), series lookup (`/api/search/lookup`), and
  data retrieval (`/api/query`) with the OpenTSDB aggregation-policy query
  format.
- Retention parsing (`convertRetention`/`convertDuration`): the
  `agg-aggtime-agg2:rowlen:ttl` grammar with the OpenTSDB/Java duration units
  (`y`/`w`/`d`/`h`/`m`/`s`/`ms`) and the query-range-splitting heuristic.
- Prometheus data-model normalization (`modifyData`): metric/label-name
  sanitization (`SanitizeMetricName`/`SanitizeLabelName`), optional
  lowercasing, and dropping `__`-prefixed tags.
- Concurrent per-metric query workers feeding the shared importer.

## What is ported (`remote-read`)

Faithful to `app/vmctl/remoteread/remoteread.go` + the `remoteReadProcessor`:

- Builds a `prompb.ReadRequest` (a single `Query` with `RE` label matchers),
  snappy-compresses it, POSTs to `/api/v1/read`, and snappy-decodes +
  protobuf-decodes the `prompb.ReadResponse` — via a small self-contained
  protobuf codec (`src/proto.rs`), no external protobuf dependency.
- Time-range splitting (`stepper`), concurrent range workers feeding the
  importer, `RE` matchers from `--remote-read-filter-label`/`-label-value`
  (default `__name__=~.*`), basic-auth + `^^`-separated headers,
  `--remote-read-disable-path-append`, and the request timeout.
- Both **SAMPLES** and **STREAMED_XOR_CHUNKS** (`--remote-read-use-stream`)
  response types are supported. Stream mode ports Prometheus's XOR (Gorilla)
  chunk decoder (`tsdb/chunkenc`) and the chunked-frame reader
  (`storage/remote`, uvarint + crc32c framing) — see `src/chunkenc.rs`
  (verified by an in-crate encode/decode round-trip over every delta-of-delta
  bucket, plus an end-to-end streamed migration test).

Verified end-to-end against a mock remote-read source (snappy protobuf) +
import destination.

## What is ported (`verify-block`)

`esmctl verify-block <path> [--gunzip]` validates a native-format export block
file (ports the `verify-block` command + the native stream framing from
`lib/protoparser/native/stream`). It reads the time-range header, then each
`(metricName, block)` frame, fully unmarshaling every block via
`esm-storage`'s real `MetricName::unmarshal` + `Block::unmarshal_portable` /
`unmarshal_data` — so a corrupt block fails exactly as it would on import — and
reports the verified block count.

## What is ported (`influx`)

Ports `app/vmctl/influx/*` + `app/vmctl/influx.go`, reimplementing the slice of
the InfluxDB HTTP `/query` API that vmctl uses (rather than depending on the
`influxdata/influxdb/client/v2` Go library):

- Schema exploration: `show field keys` (skipping non-numeric fields),
  `show tag keys`, and `show series [filter] [where time…]`, combined into the
  concrete `(measurement, field, tags)` series to import.
- Series-key unmarshaling with full InfluxDB escape handling
  (`\,`/`\=`/`\ `), per-series `select "field" from "measurement" where
  "tag"::tag='value' …` queries, RFC3339 timestamp parsing, and the
  measurement/field-separator + `__name__` (prometheus-mode) + `db`-label name
  construction.
- Concurrent series workers feeding the shared importer.

**Deviation:** queries are issued non-chunked and the full JSON response is
read into memory (upstream streams chunked NDJSON purely to bound memory);
results are identical. Verified end-to-end against a mock InfluxDB + import
destination.

## Shared destination importer (`vm.Importer`)

Ports `vm/vm.go` + `vm/timeseries.go`: a `/health` ping, a concurrent worker
pool that batches series by sample count and POSTs newline-delimited VM import
JSON to `/api/v1/import` (splitting series over 10K samples across lines),
value rounding (`--vm-significant-figures`/`--vm-round-digits` via
`esm_common::decimal`), backoff retries with bad-request fast-fail, and
`--vm-extra-label` injection. Verified end-to-end against a mock OpenTSDB
source + import destination.

## Deferred / out of scope

- **`prometheus` / `thanos` / `mimir` modes** — these read Prometheus TSDB
  blocks (via `github.com/prometheus/prometheus/tsdb`) directly. Porting them
  means porting Prometheus's on-disk storage engine (block/chunk/index
  readers), which is a separate large undertaking outside this project's
  scope. They exit with a clear "not yet ported" message.
- **TLS `server_name` (SNI override)** — the `-*-server-name` flags are
  accepted but not applied: the reqwest-blocking client has no SNI-override
  hook (the same documented limitation as esmauth/esmalert/esmagent). Custom
  CA files, client cert/key pairs, and `insecure-skip-verify` **are** honored.
- Progress bars are replaced by `log` lines.
