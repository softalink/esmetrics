# Porting Matrix

> This port is branded **EsMetrics** by **Softalink LLC**. "VictoriaMetrics" below
> always refers to the upstream Go project being ported, not to this repo's product.

Reference source: VictoriaMetrics **v1.146.0** (checkout at `/home/test/refsrc/VictoriaMetrics`,
not committed to this repo). TSBS reference at `/home/test/refsrc/tsbs`.

Goal: the Rust single-node server must beat Go VictoriaMetrics v1.146.0 on **all**
TSBS metrics (ingest throughput; query latency mean/median/p95/p99 for every
supported devops query type) on Linux and Windows.

## TSBS surface (what must be fast and correct)

- **Load**: `tsbs_load_victoriametrics` POSTs Influx line protocol to `/write`.
- **Queries** (`tsbs_generate_queries` → `tsbs_run_queries_victoriametrics`, devops):
  - `single-groupby-{1,5}-{1,8}-{1,12}`: `max(max_over_time(cpu_X{hostname=~'a|b'}[1m])) by (__name__)`
  - `double-groupby-{1,5,all}`: `avg(avg_over_time({__name__=~'cpu_(...)'}[1h])) by (__name__, hostname)`
  - `cpu-max-all-{1,8}`: `max(max_over_time(cpu_X{...}[1h])) by (__name__)`
  - `high-cpu-{all,1}`: subqueries with comparison filter
  - Sent to `/api/v1/query_range` with `start`, `end`, `step`.
  - `groupby-orderby-limit`, `lastpoint` are unsupported by the TSBS VictoriaMetrics adapter → out of scope.

## Package mapping

| Go package (v1.146.0) | Rust crate | Status | Notes |
|---|---|---|---|
| lib/bytesutil | esm-common | done | fast string/byte helpers, interning |
| lib/fasttime | esm-common | done | coarse unix-time clock |
| lib/decimal | esm-common | done | decimal <-> (value,exponent) conversion for values compression |
| lib/uint64set | esm-common | done | compact u64 set for TSID sets |
| lib/regexutil | esm-common | done | regex simplification/matcher specialization |
| lib/fs, lib/filestream | esm-common | done | fsync-safe file IO, streaming readers/writers, mmap |
| lib/memory | esm-common | done | allowed-memory detection (cgroup on Linux, GlobalMemoryStatusEx on Windows) |
| lib/logger | esm-common | done | leveled logging (thin wrapper over `log`) |
| github.com/VictoriaMetrics/metrics | esm-common::metrics | done | counters only (process-global name->Counter registry + Prometheus text `write_prometheus`); no gauges/histograms/summaries, no process metrics, no `HELP`/`TYPE` lines, no push client; exposed metric names use the `esm_` prefix (renamed from upstream `vm_`, e.g. `esm_rows_inserted_total`) |
| lib/encoding | esm-encoding | done | varint/zigzag, delta, delta2, values enc (decimal+zstd), timestamps enc |
| lib/blockcache, lib/workingsetcache, lib/lrucache | esm-mergeset/esm-storage | done |
| lib/httpserver (subset) | esm-http | done | hand-rolled sync HTTP/1.1 server | sized caches for index/data blocks |
| lib/mergeset | esm-mergeset | done | LSM for inverted index: inmemory/file parts, bg merges |
| lib/storage | esm-storage | done | TSID, MetricName, index_db, raw rows, partitions, parts, blocks, search, tag filters, dedup, retention |
| lib/storage snapshots + /snapshot/* API | esm-storage / esmetrics | done | hard-link snapshots; no symlink indirection (Windows) |
| lib/protoparser/influx | esm-protoparser | done | streaming Influx line protocol parser |
| lib/protoparser/prometheus | esm-protoparser | done | Prometheus exposition-text parser (decode path only; OpenMetrics metadata out of scope) |
| app/vminsert/prometheusimport | esm-insert::prometheusimport | done | /api/v1/import/prometheus (+ Pushgateway `/metrics/job/...` path labels, 200 vs 204) |
| lib/protoparser/vmimport | esm-protoparser | done | `/api/v1/import` JSON-lines decode + stream parser |
| app/vminsert/vmimport | esm-insert::vmimport | done | /api/v1/import (+ /prometheus/... alias) |
| lib/protoparser/csvimport | esm-protoparser | done | column descriptor grammar, scanner, row parser, stream parser; `time:custom:<Go layout>` narrowed out (rfc3339/unix_s/unix_ms/unix_ns only) |
| app/vminsert/csvimport | esm-insert::csvimport | done | /api/v1/import/csv (+ /prometheus/... alias); `format` query arg required |
| github.com/VictoriaMetrics/metricsql v0.87.1 | esm-metricsql | done | lexer, parser, AST, optimizer |
| app/vmselect/promql | esm-promql | done (stage-1 fn set + result cache) | eval, rollup fns, aggr fns, binary ops, memory limiter |
| app/vmselect (netstorage, prometheus API) | esm-select | done | /api/v1/query_range, query, series, labels, export |
| app/vminsert | esm-insert | done | /write handler, relabel skipped (not needed by TSBS); `vm_rows_inserted_total{type=...}` ported per protocol handler (exposed as `esm_rows_inserted_total`), `vm_rows_per_insert`/metadata histograms skipped |
| lib/prompb | esm-protoparser | done | decode-only WriteRequest/TimeSeries/Label/Sample; no native histograms, no metadata |
| lib/protoparser/promremotewrite | esm-protoparser | done | remote-write stream decode: snappy/zstd fallback + protobuf unmarshal |
| lib/protoparser/protoparserutil | esm-insert | done | `GetExtraLabels` (as `common::get_extra_labels`) |
| app/vminsert/promremotewrite | esm-insert::promremotewrite | done | /api/v1/write, /api/v1/push (+ /prometheus/... aliases); relabel and metric-metadata skipped |
| lib/protoparser/opentelemetry | esm-protoparser | done | OTLP metrics protobuf decode-to-AST (`pb`) + AST-to-Prometheus-row conversion (`convert`, ports `pb.go`'s `decoderContext`/`pushSamples` logic — the actual conversion, not `stream/streamparser.go`, which only sanitizes names); Gauge/Sum/Histogram/ExponentialHistogram(`vmrange` buckets)/Summary; resource+scope label promotion; `sanitize.go`'s `usePrometheusNaming=false` path only (identity; true-branch unported, see module doc) |
| app/vminsert/opentelemetry | esm-insert::opentelemetry | done | /opentelemetry/v1/metrics, /opentelemetry/api/v1/push; 200-empty-body success (not a protobuf response); JSON bodies rejected (AWS Firehose JSON envelope out of scope); relabel and metric-metadata skipped |
| lib/protoparser/graphite | esm-protoparser | done | Graphite plaintext line parser + stream parser; `-graphite.sanitizeMetricName` out of scope |
| lib/protoparser/opentsdb | esm-protoparser | done | OpenTSDB telnet `put` line parser + stream parser |
| lib/ingestserver/graphite, lib/ingestserver/opentsdb (telnet) | esm-insert::ingestserver | done | thread-per-connection TCP accept loop + single UDP reader thread per protocol (upstream uses a per-CPU UDP worker pool — see module doc); no proxy-protocol support; `-opentsdbListenAddr` here is telnet-only, upstream multiplexes telnet+HTTP on the same TCP listener; `vm_ingestserver_requests_total{type=...,name="write",net="tcp"|"udp"}` ported (exposed as `esm_ingestserver_requests_total`), `vm_ingestserver_request_errors_total` skipped |
| app/vminsert/graphite, app/vminsert/opentsdb | esm-insert::graphite, esm-insert::opentsdb | done | metric→`MetricRow` conversion shared by TCP/UDP; metric-group label is marshaled *first* here (matching `ctx.AddLabel("", r.Metric)` preceding the tag loop in both Go handlers), unlike influx's group-last layout; no relabeling, no extra labels (telnet/UDP carry no query string) |
| lib/protoparser/opentsdbhttp | esm-protoparser | done | OpenTSDB HTTP `/api/put` JSON parser (object or array of objects) + stream parser; `serde_json::Value` walk like vmimport; two failure tiers ported faithfully (top-level JSON syntax error = request-level `Error::Unmarshal`, valid-JSON-but-wrong-shape = logged + zero rows, no error) |
| app/vminsert/opentsdbhttp, lib/ingestserver/opentsdbhttp | esm-insert::opentsdbhttp, esmetrics | done | dedicated `-opentsdbHTTPListenAddr` `esm_http::Server` (second listener, not a route on the main port — confirmed absent from `app/vminsert/main.go`'s `RequestHandler`); serves `/api/put` + `/opentsdb/api/put` only; 204 success, 400 for any other path (no 404 fallback, matching upstream's dedicated-server `newRequestHandler`); metric-group label first then tags then extra labels last (matches Go `insertRows`'s `ctx.AddLabel` call order); gzip/deflate/zstd/snappy accepted via the shared `util::read_uncompressed_data`; `vm_rows_inserted_total{type="opentsdbhttp"}` ported (exposed as `esm_rows_inserted_total`) |
| lib/protoparser/datadogutil | esm-protoparser | done | `SplitTag` (tag without a value -> `"no_label_value"`), `SanitizeName` (regex-based, `-datadog.sanitizeMetricName` default `true` applied unconditionally, unlike `-graphite.sanitizeMetricName`'s unported default-`false`), `-datadog.maxInsertRequestSize` (64 MiB, distinct from the shared 32 MiB default) |
| lib/protoparser/datadogv1, lib/protoparser/datadogv2 | esm-protoparser::datadog (`v1`/`v2` submodules) | done | `/api/v1/series` JSON + `/api/v2/series` JSON **and** protobuf (verified against `datadogv2/stream/streamparser.go`: `Content-Type: application/x-protobuf` decodes via `crate::wire::WireReader`, anything else defaults to JSON); strict all-or-nothing shape validation (unlike opentsdbhttp/vmimport's lenient per-entry skip), matching Go `encoding/json`; missing/non-positive point timestamps filled with current unix time; reset-before-parse (`&mut self` API) ported to satisfy `TestRequestUnmarshalMissingHost` |
| app/vminsert/datadogv1, app/vminsert/datadogv2 | esm-insert::datadog | done | `/datadog/api/v1/series`, `/datadog/api/v2/series` on the main port; metric-group label first, then v1 `host`/`device` (v2: `resources` as `type=name` labels), then tags (`SplitTag`, a tag literally named `host` renamed to `exported_host`), then v2 `source_type_name`, then extra labels last; v1 timestamp seconds->ms via `pt[0]*1000` truncation, v2 via `timestamp*1000`; success is 202 + `{"status":"ok"}` (not 204); DataDog agent stub endpoints (`/datadog/api/v1/validate` 200 `{"valid":true}`, `/datadog/api/v1/check_run` 202 `{"status":"ok"}`, `/datadog/intake` 200 `{}`, `/datadog/api/v1/metadata` 200 `{}` — verified byte-for-byte against `app/vminsert/main.go`, not guessed) and the `/datadog/`-prefixed trailing-slash trim also live here; `vm_rows_inserted_total{type="datadogv1"|"datadogv2"}` ported (exposed as `esm_rows_inserted_total`) |
| app/victoria-metrics | esmetrics (bin) | done | flags, http server, lifecycle, graphite/opentsdb TCP+UDP ingest listener wiring, dedicated OpenTSDB HTTP `/api/put` listener wiring, `/metrics` endpoint (`esm_common::metrics::write_prometheus`) |
| lib/backup + app/vmbackup | esm-backup / esbackup | done | fs/s3/gcs/azblob via object_store; no -maxBytesPerSecond, no bandwidth metrics |
| app/vmrestore | esm-backup / esrestore | done | direct-write restore (upstream -skipFilePreallocation mode) |
| app/vmui (packages/vmui) | esmetrics (`src/esmui.rs`, `assets/esmui/`) | vendored, not ported | built vmui dist (v1.146.0, Apache-2.0, see NOTICE), rebranded to EsMetrics UI (product name, logo, footer links/copyright, and the API-base path regex recognizing `/esmui`) via a source patch applied before building — see `assets/esmui/PATCHES.md` — copied in as static files and served at the EsMetrics-branded `/esmui/` (legacy `/vmui...` 302-redirects there) (also under the `/prometheus` cluster-compat prefix — vmui fetches `<origin>/prometheus/...` — and via the `/graph` alias upstream keeps for Grafana Prometheus-datasource links); `build.rs` embeds them via a generated `include_bytes!` table. `/vmui/custom-dashboards` and `/vmui/timezone` answer with upstream's default-flag responses (`{"dashboardsSettings": []}`, `{"timezone": "UTC"}` — Go `time.LoadLocation("")` is UTC); the `-vmui.customDashboardsPath` / `-vmui.defaultTimezone` flags themselves are not ported |
| app/vmauth/{main,auth_config,target_url}.go | esm-auth | done | config parsing/validation, `http_auth` token map + Basic/Bearer extraction, target-URL routing (`url_prefix`/`url_map`, `drop_src_path_prefix_parts`, `merge_query_args`), load balancing (`least_loaded`/`first_available`) + backend health (lazy "broken until" recovery instead of a per-backend TCP-dial health-check goroutine), full-header-capture streaming proxy (request/response passthrough, 3xx not followed, 5xx retry via `retry_status_codes`), per-user + global concurrency limiting, SIGHUP/`-configCheckInterval`/`/-/reload` hot-reload (last-good config kept on a failed reload) |
| app/vmauth (binary) | esmauth | done | flag parsing (Go-`flag`-style), signal handling, binary wiring around esm-auth; `-reloadAuthKey`/`-metricsAuthKey` gate `/-/reload` and `/metrics` (empty = open, matching upstream `httpserver.CheckAuthFlag`); `-readTimeout` (default 30s) sets a per-read idle timeout that closes slow-loris connections |
| app/vmalert + app/vmalert/{rule,datasource,notifier,remotewrite,remoteread,config,templates} | esmalert + esm-gotemplate | done | alerting + recording rules with alert-state persistence (remote-write of `ALERTS`/`ALERTS_FOR_STATE`, remote-read restore of `for:` progress on startup); faithful Go-`text/template` subset engine (`esm-gotemplate`, 32-function FuncMap) for annotation/label rendering; Prometheus datasource client, Alertmanager `/api/v2/alerts` notifier, YAML rule-group config with strict validation + hot-reload; read-only JSON API (`/api/v1/rules`, `/api/v1/alerts`). Deferred: replay/backfill, notifier service discovery, full HTML web UI (JSON API only), oauth2 flags, Graphite/VictoriaLogs datasources; Go-template time/duration method calls (`.Add`/`.Sub`/`.UnixMilli`) unsupported (validation error). See `crates/esmalert/README.md` for the full limitations list |
| app/vmalert-tool | esmalert-tool | done | offline unit-test runner for `esmalert`/`vmalert` rule files (`promtool test rules` analog): parses a YAML test file's `rule_files`/`input_series`/`alert_rule_test`/`metricsql_expr_test`, stands up a real in-process `esmetrics` engine per file, drives `esmalert`'s real rule-evaluation loop against synthetic input, and asserts alerting-rule and MetricsQL-expression results (label/annotation set-match, epsilon value comparison). Only the `unittest` subcommand exists (upstream has no others). Deferred: the top-level `-external.label` CLI flag (test-group `external_labels` works); `group_eval_order` duplicate-name validation. See `crates/esmalert-tool/README.md` for the full limitations list |
| app/vmagent (forwarding tier) + lib/promrelabel + lib/persistentqueue | esmagent + esm-relabel | done | accepts data via every `esm-insert` push protocol, applies a global relabel config, fans out to N `-remoteWrite.url` destinations each with its own optional per-URL relabel config, `PendingSeries` block batching, and a durable `PersistentQueue` (disk-backed FIFO, size-capped drop-oldest) feeding a retrying HTTP `Client` worker pool per destination (5xx/429/transport -> retry with backoff, other 4xx -> drop). See `crates/esmagent/README.md` for the full limitations list |
| app/vmctl (all HTTP-based modes + vm.Importer) | esmctl (bin) | done (vm-native + opentsdb + remote-read + influx + verify-block) | migration CLI. **`vm-native`** mode (VM↔VM / EsMetrics↔EsMetrics native export→import streaming): metric-name exploration, `stepper.SplitDateRange` (month/week/day/hour/minute), `vmctlutil.ParseTime` (RFC3339/calendar/unix/relative), `buildMatchWithFilter` (per-metric MetricsQL match via esm-metricsql), `backoff.Backoff`, concurrent workers, src/dst basic/bearer/`^^`-headers auth, `--vm-extra-label` injection, intercluster tenant discovery, binary-protocol toggle; streams the export response body directly into the import request body via reqwest-blocking (no full buffering). **`opentsdb`** mode: metric/series discovery (`/api/suggest`, `/api/search/lookup`), retention parsing (`convertRetention`/`convertDuration` — the `agg-aggtime-agg2:rowlen:ttl` grammar + y/w/d/h/m/s/ms units + query-range splitting), `/api/query` retrieval, and `modifyData` Prometheus-data-model normalization (`SanitizeMetricName`/`SanitizeLabelName`, lowercase, `__`-tag drop). **`remote-read`** mode (SAMPLES): builds a snappy-compressed `prompb.ReadRequest` (RE label matchers), POSTs to `/api/v1/read`, snappy+protobuf-decodes the `ReadResponse` via a self-contained protobuf codec (`src/proto.rs`, no external protobuf dep), splits the range (`stepper`), and streams series to the importer; STREAMED_XOR_CHUNKS (`--remote-read-use-stream`) also supported — ports Prometheus's XOR/Gorilla chunk decoder + chunked-frame (uvarint+crc32c) reader (`src/chunkenc.rs`). **`verify-block`**: validates a native export block file — reads the time-range header + each `(metricName, block)` frame and fully unmarshals every block via esm-storage's `MetricName::unmarshal`+`Block::unmarshal_portable`/`unmarshal_data` (a corrupt block fails as on import), reporting the block count; `--gunzip` deferred. **`influx`** mode: reimplements the slice of InfluxDB's HTTP `/query` API vmctl uses (`show field keys` skipping non-numeric fields, `show tag keys`, `show series [filter]`, then per-series `select "field" from "measurement" where "tag"::tag='value'`) instead of the `influxdata/influxdb/client/v2` Go lib; full InfluxDB series-key escape handling, RFC3339 timestamp parsing, measurement/field-separator + prometheus-mode `__name__` + `db`-label name construction, concurrent series workers; queries issued non-chunked (full-response read — a memory-only deviation from upstream's chunked NDJSON). **`vm.Importer`** (shared JSON `/api/v1/import` destination): `/health` ping, concurrent batching worker pool, newline-delimited VM import JSON (10K-sample line splitting), value rounding via `esm_common::decimal`, backoff with bad-request fast-fail. Both modes verified end-to-end against mock endpoints. Deferred (see `crates/esmctl/README.md`): the `prometheus`/`thanos`/`mimir` modes (read Prometheus TSDB blocks — porting means porting Prometheus's storage engine, out of scope), TLS client certs (only `insecure-skip-verify` wired), `--vm-rate-limit`, `--vm-compress` gzip, progress bars, and the `vmctl_*` self-metrics |
| lib/streamaggr (+ valyala/histogram, VictoriaMetrics/metrics histogram) | esm-streamaggr | done (library + wired into esmagent global + per-URL `-remoteWrite.streamAggr.config`, and single-node esmetrics `-streamAggr.config`) | streaming aggregation engine: strict YAML `Config`/`Options` parse+validate (all of `interval`/`by`/`without`/`dedup_interval`/`staleness_interval`/`ignore_first_sample_interval`/`keep_metric_names`/`ignore_old_samples`/`ignore_first_intervals`/`flush_on_shutdown`/`no_align_flush_to_interval`/`drop_input_labels`/`input_relabel_configs`/`output_relabel_configs`/`match`), ALL 18 outputs (`avg`/`count_samples`/`count_series`/`histogram_bucket`/`increase`/`increase_prometheus`/`last`/`max`/`min`/`quantiles(phi…)`/`rate_avg`/`rate_sum`/`stddev`/`stdvar`/`sum_samples`/`sum_samples_total`/`total`/`total_prometheus`/`unique_samples`) with `histogram_bucket` porting VM's `vmrange` bucketing and `quantiles` porting `valyala/histogram.Fast` (incl. deterministic >1000-sample reservoir), sample de-duplication + the standalone `Deduplicator`, output-name suffixing, `by`/`without` grouping, input/output relabeling, aligned/unaligned flushing, and a background flusher thread. Go's `time.ParseDuration` ported faithfully (`godur.rs`) so `1d`/bare durations reject as upstream. Deferred: `enable_windows` (experimental blue/green windowing — accepted but no-op), the `vm_streamaggr_*` self-monitoring metrics, and byte-format fidelity of the label-dictionary compressor (an in-memory-only round-tripping key encoding replaces `promutil.LabelsCompressor`). See `crates/esm-streamaggr/README.md` |
| lib/promscrape (core: static/file/http/kubernetes/consul/ec2/gce/azure/digitalocean/hetzner/nomad/marathon/vultr/eureka/yandexcloud/ovhcloud SD; scrape loop; JSON /api/v1/targets) | esmagent::scrape | done (full promscrape SD surface ported — no cloud SD deferred) | `-promscrape.config` YAML parse + validate; `static_configs`/`file_sd_configs`/`http_sd_configs`/`kubernetes_sd_configs` discovery; target-relabel (`relabel_configs`) then per-target scrape workers on the job's `scrape_interval`; `metric_relabel_configs`, `honor_labels`/`honor_timestamps`, `sample_limit`/`label_limit`, `max_scrape_size`; auto-metrics (`up`, `scrape_duration_seconds`, ...); cross-scrape + target-removal staleness (`STALE_NAN` markers); scraped series flow through the same global-relabel -> `Fanout` pipeline pushed data uses; JSON `GET /api/v1/targets` (`?state=active\|dropped`); SIGHUP/`-promscrape.configCheckInterval` reload. Kubernetes SD Phase A+B: `pod`/`node`/`service`/`ingress`/`endpoints`/`endpointslice` roles, in-cluster (service-account token + cluster CA), explicit `api_server` auth (inline bearer/basic + TLS), `kubeconfig_file` auth (`current-context` cluster server + TLS from file paths or inline base64 `*-data`, user token/token-file/basic, cluster `proxy-url`), or OAuth2 client-credentials auth (`oauth2:` block: `client_id`, `client_secret`/`client_secret_file`, `scopes`, `token_url`, `endpoint_params`, token-endpoint `tls_config`/`proxy_url`; token cached until shortly before `expires_in`, attached as a bearer token), a list+watch client with an in-memory per-role cache (resumes the watch from `resourceVersion`, re-lists on `410`), `namespaces`/`selectors` filtering, per-role `__meta_kubernetes_*` labels, a shared cross-role object cache (endpoints/endpointslice join their `Service` + `targetRef` `Pod`), and `attach_metadata: {node, namespace}` label-joining. The standalone `proxy_url` SDConfig field and the global `-promscrape.kubernetes.attachNodeMetadataAll`/`attachNamespaceMetadataAll` flags (default `attach_metadata` per config, overridden per-config) are supported. Consul SD (`consul_sd_configs`: `services`/`tags` filtering, datacenter auto-resolution, token/basic/TLS auth, full `__meta_consul_*` labels, interval re-list rather than blocking-query long-poll) is supported. Consul Agent SD (`consulagent_sd_configs`: like Consul SD but against the LOCAL agent — `/v1/agent/self` + `/v1/agent/services` + `/v1/agent/health/service/name/<svc>`, `__meta_consulagent_*` labels with agent-derived address/dc/node/metadata, `services`/`filter`/`namespace`/`tag_separator`, no partition/tags/node_meta/allow_stale, interval re-list from `-promscrape.consulagentSDCheckInterval` default 30s) is supported. EC2 SD (`ec2_sd_configs`: `region`/`endpoint`/`filters`/`port`, SigV4-signed paginated `DescribeInstances` + best-effort `DescribeAvailabilityZones`, full `__meta_ec2_*` labels, interval re-list from `-promscrape.ec2SDCheckInterval` default 60s; credential chain scoped to static config/env keys + IMDSv2 instance role, with STS `role_arn`/web-identity/shared-`~/.aws`-file modes deferred and `role_arn` rejected at build time) is supported. GCE SD (`gce_sd_configs`: `project`/`zone` (single/list/`*`) config or metadata auto-detect, `filter`/`port`/`tag_separator`, paginated `instances.list` per zone following `nextPageToken`, full `__meta_gce_*` labels, interval re-list from `-promscrape.gceSDCheckInterval` default 60s; auth scoped to a static `bearer_token` or the GCE metadata-server access token, with the service-account JSON key file (`credentials_file`) deferred and rejected at build time) is supported. DigitalOcean SD (`digitalocean_sd_configs`: `server`/`bearer_token`/`tls_config`/`port`, paginated `/v2/droplets` following `links.pages.next`, full `__meta_digitalocean_*` labels, interval re-list from `-promscrape.digitaloceanSDCheckInterval` default 60s) is supported. Hetzner SD (`hetzner_sd_configs`: `role` (`hcloud`/`robot`), `bearer_token` (hcloud) / `basic_auth` (robot) / `tls_config` / `port` default 80; `hcloud` role Bearer-auth paginated `/v1/servers` + `/v1/networks` against `https://api.hetzner.cloud` with the `__meta_hetzner_*` + `__meta_hetzner_hcloud_*` labels, `robot` role HTTP-Basic `/server` against `https://robot-ws.your-server.de` with the `__meta_hetzner_*` + `__meta_hetzner_robot_*` labels, interval re-list from `-promscrape.hetznerSDCheckInterval` default 60s; unknown `role` rejected at config parse) is supported. Nomad SD (`nomad_sd_configs`: `server`/`namespace`/`region`/`tag_separator`/`allow_stale`, `NOMAD_TOKEN`-or-inline-`bearer_token` bearer auth + `basic_auth`/`tls_config`, `/v1/services` then per-service `/v1/service/<name>` listing, full `__meta_nomad_*` labels, interval re-list from `-promscrape.nomadSDCheckInterval` default 30s) is supported. Marathon SD (`marathon_sd_configs`: `servers` list tried in order, `bearer_token`/`basic_auth`/`tls_config` auth, `/v2/apps/?embed=apps.tasks` listing, one target per app task per port with the full `__meta_marathon_*` labels, interval re-list from `-promscrape.marathonSDCheckInterval` default 30s; upstream's `auth_token`/`Authorization: token=` header does not exist in v1.146.0 so is not ported, and random-server pick is replaced by ordered failover) is supported. Vultr SD (`vultr_sd_configs`: `server`/`bearer_token`/`tls_config`/`port` default 80, cursor-paginated `/v2/instances?per_page=100` following the opaque `meta.links.next` cursor, full `__meta_vultr_instance_*` labels, `__address__` = instance `main_ip` + `port`, interval re-list from `-promscrape.vultrSDCheckInterval` default 30s (matches upstream's `vultr.SDCheckInterval`); `server` is an esmagent-only endpoint-override field not in upstream's Vultr `SDConfig`, and the API-side `label`/`main_ip`/`region`/`firewall_group_id`/`hostname` filter params are not ported) is supported. PuppetDB SD (`puppetdb_sd_configs`: required `url`/`query` (PQL), `include_parameters`/`port` (default 80), `bearer_token`/`basic_auth`/`tls_config` auth, single `POST <url>/pdb/query/v4` with a `{"query": ...}` body, one target per resource (`__address__` = `certname` + `port`), full `__meta_puppetdb_*` labels incl. `include_parameters`-gated sanitized `_parameter_<k>`, interval re-run from `-promscrape.puppetdbSDCheckInterval` default 30s) is supported. Kuma SD (`kuma_sd_configs`: required `server` (MADS endpoint; `http://` assumed when scheme-less; base path/query preserved before the `/v3/discovery:monitoringassignments` suffix), `client_id` (default hostname else `vmagent`), `bearer_token`/`basic_auth`/`tls_config` auth, single `POST` of an xDS `DiscoveryRequest` (poll-based: empty `version_info`/`nonce` each refresh, full response taken), one target per assignment target (`__address__` = target address) with `instance`/`__scheme__`/`__metrics_path__` and the `__meta_kuma_*` labels (`_dataplane`/`_mesh`/`_service` + sanitized `_label_<k>`), interval re-run from `-promscrape.kumaSDCheckInterval` default 30s) is supported. Eureka SD (`eureka_sd_configs`: `server` (default `localhost:8080/eureka/v2`), `bearer_token`/`basic_auth`/`tls_config` auth, single `GET <server>/apps` with `Accept: application/xml`, one target per registered instance (`__address__` = instance `hostName` + `<port>` else 80), full `__meta_eureka_*` labels incl. per-metadata sanitized `_metadata_<k>`/`_datacenterinfo_metadata_<k>` and the `instance`=instance-id override, interval re-list from `-promscrape.eurekaSDCheckInterval` default 30s) is supported. Yandex Cloud SD (`yandexcloud_sd_configs`: `service` (must be `compute`), `yandex_passport_oauth_token`/`api_endpoint`/`folder_ids`/`tls_config`, per-service endpoint resolution from `GET <api_endpoint>/endpoints`, `folder_ids` or organizations->clouds->folders (resource-manager) enumeration, paginated `/compute/v1/instances` per folder following `nextPageToken`, one target per instance (`__address__` = instance FQDN, no port), full `__meta_yandexcloud_*` labels, interval re-list from `-promscrape.yandexcloudSDCheckInterval` default 30s; auth scoped to a static OAuth token exchanged for an IAM token or the compute metadata-server IAM token, with the service-account authorized-key JSON (`service_account_key_file`, JWT->IAM exchange) deferred and rejected at build time and upstream's disabled EC2 IMDSv1 fallback not ported) is supported; DNS SD (`dns_sd_configs`: `names` list, `type` (`SRV` default / `A` / `AAAA` / `MX`), `port` (required for A/AAAA, defaults to 25 for MX, ignored for SRV — matching upstream); A/AAAA via the OS resolver (getaddrinfo), SRV/MX via a built-in synchronous DNS client (UDP with a per-query random transaction id and a source-address check on the response, falling back to TCP on truncation) pointed at the first `/etc/resolv.conf` nameserver on unix (no nameserver on non-unix without an override -> no SRV/MX targets); `__meta_dns_name`/`__meta_dns_srv_record_target`/`__meta_dns_srv_record_port` (SRV+A/AAAA) or `__meta_dns_mx_record_target` (MX), interval re-resolve from `-promscrape.dnsSDCheckInterval` default 30s) is supported; Docker SD (`docker_sd_configs`: required `host` (`unix://<path>` via a hand-rolled synchronous HTTP/1.1 chunked-decoding client over `UnixStream` — Unix-only, reused by the Dockerswarm provider; `tcp://host:port` mapped to `http://host:port`; or `http(s)://…` via `reqwest` with `basic_auth`/`bearer_token`/`tls_config`), `port` default 80, `filters`, `host_networking_host` default `localhost`, `match_first_network` default `true`; unversioned `/networks` + `/containers/json` fetches, one target per container network × exposed TCP port (fallback target when none; `host_networking_host` for `network_mode: host`), full `__meta_docker_*` labels incl. sanitized container/network `_label_<k>` and the network-ID-joined network labels, `container:<id>` linked-network inheritance, interval re-list from `-promscrape.dockerSDCheckInterval` default 30s) is supported; Docker Swarm SD (`dockerswarm_sd_configs`: required `host` (same schemes as Docker SD, REUSING the Docker provider's Unix-socket HTTP/1.1 transport) + required `role` (`services`/`tasks`/`nodes`), `port` default 80, `filters` (applied only to the role's own endpoint), full `__meta_dockerswarm_*` labels per role — `nodes` from `/nodes`; `services` from `/services`+`/networks` with the network-ID-joined labels; `tasks` from `/tasks`+`/services`+`/nodes`+`/networks` with the node/service/network joins — interval re-list from `-promscrape.dockerswarmSDCheckInterval` default 30s; invalid `role`/`host` rejected when the job is built) is supported — with this the ENTIRE non-Kubernetes promscrape SD surface is ported and no SD key is deferred; the HTML `/targets` page, `scrape_config_files`, `series_limit`, and OAuth2 auth for per-job `scrape`/`http_sd` configs are not ported; remaining k8s gaps are the OAuth2 token-request `headers` field + credential-style autodetect (body-only here), kubeconfig `exec`/impersonation auth, upstream's cross-config `groupWatcher` dedup (per-config watchers here), and `-promscrape.kubernetes.apiServerTimeout` (fixed 60s watch timeout). See `crates/esmagent/README.md`'s "Scraping limitations" and "Kubernetes SD limitations" for the full list |

Out of scope (not exercised by TSBS single-node): native import, newrelic,
zabbixconnector, datadogsketches, OTLP firehose, prommetadata, clustering,
Kubernetes SD's own remaining auth/tuning gaps (see the vmagent scope note
below), vmgateway (enterprise-only — no open-source Go source available),
vmbackupmanager (enterprise-only), downsampling, logs products, and vmctl's
Prometheus-TSDB-block-reading modes (`prometheus`/`thanos`/`mimir` — the
`vm-native` mode IS ported as `esmctl`, see the table above). Also out of scope on
the query/observability surface: the Influx TCP/UDP line-protocol listener
(`lib/ingestserver/influx`, `-influxListenAddr` — Influx-over-HTTP `/write`
**is** ported), the Graphite render query language (`app/vmselect/graphiteql`,
`/render`), query-stats (`app/vmselect/querystats`,
`/api/v1/status/top_queries`), metric-names usage stats (`app/vmselect/stats`),
the self-monitoring self-scraper (`app/victoria-metrics/self_scraper.go`), and
the rest of the `/api/v1/status/*` family (`tsdb`, `active_queries`) — only
`/api/v1/status/buildinfo` is served. (CSV
import, graphite/opentsdb telnet ingestion, OpenTSDB HTTP `/api/put`,
DataDog `/api/v1/series` + `/api/v2/series` ingestion, and the vmagent
scrape + remote-write forwarding tier are all ported — see the table above
— even though TSBS itself doesn't exercise them. vmui's built assets are
vendored and served, per the table above, though its Go-side app logic was
not ported.)

### vmauth scope note

`app/vmauth` (now ported as `esm-auth`/`esmauth`, see the table above) is
**not** exercised by TSBS; the port covers the open-source auth-proxy
subset needed to front esmetrics with multiple backends. The following
upstream vmauth pieces are explicitly out of scope:

- JWT/OIDC authentication
- DNS-based backend discovery (`discoverBackendAddrsIfNeeded`)
- Backend-TLS tuning (custom TLS configs per backend)
- `ip_filters` (allow/deny lists)
- `src_query_args`/`src_headers` matching in `url_map` (config parsing
  rejects a `url_map` entry that sets either, rather than silently ignoring
  it — see `esm-auth/src/config.rs`)
- Authoritative `X-Forwarded-For`: this port strips any client-supplied
  `X-Forwarded-For` rather than overwriting it with the real peer address
  (upstream does the latter; see `esm-auth/src/proxy.rs`)
- Upstream's "stream large request bodies straight through to the backend"
  design is superseded here: this port always fully buffers the request body
  (up to a hard 32 MiB ceiling, which answers `413` beyond it) rather than
  streaming it — see the "Full in-memory body buffering" deviation in
  `esm-auth/src/proxy.rs`. `-maxRequestBodySizeToRetry` still exists and is
  honored, but it now controls only the smaller *retry-buffer* threshold
  (whether a body can be replayed at another backend on failure), not
  whether the body is streamed.

Security hardening carried by this port (from the T11 adversarial review):

- `-reloadAuthKey` / `-metricsAuthKey` gate `/-/reload` and `/metrics` with a
  `?authKey=<value>` query param (mirroring esmetrics' `-snapshotAuthKey` and
  upstream vmauth's `httpserver.CheckAuthFlag`). Empty (the default) leaves the
  endpoint open. Gating `/-/reload` prevents an unauthenticated caller from
  forcing repeated config re-reads that wipe backend circuit-breaker state and
  reset per-user concurrency limiters.
- `-readTimeout` (default 30s) is applied as a per-read **idle** timeout
  (`SO_RCVTIMEO`, reset on every successful read) so a slow-loris that trickles
  header bytes is dropped, while a steadily progressing large upload is not cut.
  Zero disables it.
- Backend `url_prefix` userinfo (`http://user:pass@backend`) is redacted from
  **client-facing** 5xx error bodies (`esm-auth/src/proxy.rs`
  `redact_url_userinfo`) so embedded backend credentials are never disclosed to
  clients; the host is preserved so the error still names the failing backend.

Metric mapping: upstream's `vmauth_*` counters are exposed as `esmauth_*`
(same `esm_`-prefix convention as every other renamed counter in this
port), e.g. `vmauth_user_requests_total` -> `esmauth_user_requests_total`,
`vmauth_http_request_errors_total` -> `esmauth_http_request_errors_total`,
`vmauth_concurrent_requests_limit_reached_total` ->
`esmauth_concurrent_requests_limit_reached_total`.

### vmagent scope note

`app/vmagent` (now ported as `esmagent` + `esm-relabel`, see the table above)
is **not** exercised by TSBS; the port covers upstream vmagent's
**remote-write forwarding tier** (receive via any push protocol, relabel,
durably queue per destination, remote-write to N backends) AND its
**scrape engine** (`lib/promscrape`: static/`file_sd`/`http_sd`/Kubernetes/
Consul SD discovery, target-relabel, the per-target scrape loop,
metric-relabel, auto-metrics, staleness, JSON `/api/v1/targets`) — scraped
series flow through the same relabel -> fan-out -> durable-queue pipeline
pushed data uses. The following upstream vmagent pieces are explicitly out of
scope or deferred:

- **Service discovery is complete** — every provider in
  `lib/promscrape/discovery/*` is ported and no SD key is deferred:
  `static_configs`/`file_sd_configs`/`http_sd_configs`/
  `kubernetes_sd_configs`/`consul_sd_configs`/`consulagent_sd_configs`/
  `ec2_sd_configs`/
  `gce_sd_configs`/`azure_sd_configs`/`digitalocean_sd_configs`/
  `hetzner_sd_configs`/`nomad_sd_configs`/`marathon_sd_configs`/
  `vultr_sd_configs`/`puppetdb_sd_configs`/`kuma_sd_configs`/
  `eureka_sd_configs`/
  `yandexcloud_sd_configs`/`ovhcloud_sd_configs`/`openstack_sd_configs`/
  `dns_sd_configs`/`docker_sd_configs`/`dockerswarm_sd_configs` all parse into
  typed configs. A genuinely-unknown SD key still fails at parse time (as an
  unknown field) rather than being silently ignored.
- **Azure SD** (done): `azure_sd_configs` with `subscription_id`,
  `authentication_method` (`OAuth` — the default, `client_id`/`client_secret`/
  `tenant_id` -> AD-endpoint bearer token — or `ManagedIdentity` — an Azure
  IMDS token with a short timeout), `environment` (AzureCloud/AzurePublicCloud,
  AzureChinaCloud, AzureGermanCloud, AzureUSGovernment; selects the AD + ARM
  endpoints), `resource_group`, and `port`. Paginated VM + VMSS-VM listing
  (`Microsoft.Compute/virtualMachines`, follows `nextLink`) then per-VM primary
  NIC resolution for the private/public IP, one target per VM private IP
  (`__address__` = private IP + `port`), and the full `__meta_azure_*` label
  set (`_subscription_id`/`_machine_id`/`_machine_name`/`_machine_location`/
  `_machine_private_ip`, conditional `_tenant_id`/`_machine_resource_group`/
  `_machine_os_type`/`_machine_computer_name`/`_machine_public_ip`/
  `_machine_scale_set`/`_machine_size`, sanitized `_machine_tag_<k>`). Token
  cached until shortly before expiry. Refresh interval from
  `-promscrape.azureSDCheckInterval` (default 60s). **Deviations:** NIC
  resolution is sequential (not upstream's worker pool); `AzureStackCloud`'s
  file-based endpoints are not ported. A bad `authentication_method` and, for
  `OAuth`, missing `tenant_id`/`client_id`/`client_secret` are rejected at
  build time.
- **GCE SD** (done): `gce_sd_configs` with `project` (config or metadata
  auto-detect), `zone` (a single value, a list, or `*` = all zones for the
  project), `filter`, `port`, `tag_separator`, and a SCOPED auth chain — a
  static `bearer_token` (config), else the GCE metadata-server access token
  (`.../instance/service-accounts/default/token` with `Metadata-Flavor:
  Google`, cached to `expires_in`). Paginated `instances.list` per zone
  (follows `nextPageToken`), one target per instance's first network interface
  (`__address__` = `networkIP` + `port`), and the full `__meta_gce_*` label
  set (`_instance_id`/`_instance_name`/`_instance_status`, `_machine_type`,
  `_network`/`_private_ip`/`_subnetwork`, `_project`, `_zone`, per-interface
  `_interface_ipv4_<name>`, comma-wrapped `_tags`, sanitized
  `_metadata_<k>`/`_label_<k>`, conditional `_public_ip`/`_public_ipv6`/
  `_internal_ipv6`). Refresh interval from `-promscrape.gceSDCheckInterval`
  (default 60s). **Deferred:** the service-account JSON key file
  (`credentials_file`/`GOOGLE_APPLICATION_CREDENTIALS`, RS256-JWT ->
  token-exchange) — rejected at build time.
- **Yandex Cloud SD** (done): `yandexcloud_sd_configs` with `service` (must be
  `compute`), `yandex_passport_oauth_token`, `api_endpoint` (default
  `https://api.cloud.yandex.net`), `folder_ids`, `tls_config`, and a SCOPED auth
  chain — a static `yandex_passport_oauth_token` exchanged for an IAM token at
  `<iam>/iam/v1/tokens`, else the compute metadata-server IAM token
  (`.../instance/service-accounts/default/token` with `Metadata-Flavor: Google`,
  cached to `expires_in`; IAM token cached until shortly before expiry).
  Resolves per-service endpoints from `GET <api_endpoint>/endpoints`, then lists
  instances for the configured `folder_ids` or enumerates organizations ->
  clouds -> folders (resource-manager API) and lists `/compute/v1/instances` per
  folder (follows `nextPageToken`). One target per instance, `__address__` = the
  instance FQDN (no port), full `__meta_yandexcloud_*` label set
  (`_instance_name`/`_instance_fqdn`/`_instance_id`/`_instance_status`/
  `_instance_platform_id`, `_instance_resources_cores`/`_core_fraction`/
  `_memory`, `_folder_id`, sanitized `_instance_label_<k>`, per-interface
  `_instance_private_ip_<index>`/conditional `_instance_public_ip_<index>`,
  `_instance_private_dns_<n>`/`_instance_public_dns_<n>`). Refresh interval from
  `-promscrape.yandexcloudSDCheckInterval` (default 30s). **Deferred:** the
  service-account authorized-key JSON (`service_account_key_file`, JWT -> IAM
  exchange) — rejected at build time; upstream's disabled EC2 IMDSv1 credential
  fallback is not ported.
- **OVHcloud SD** (done): `ovhcloud_sd_configs` with `service` (`vps` or
  `dedicated_server`), `application_key`/`application_secret`/`consumer_key`,
  and `endpoint` (one of the seven OVH regions `ovh-eu`/`ovh-ca`/`ovh-us`/
  `kimsufi-eu`/`kimsufi-ca`/`soyoustart-eu`/`soyoustart-ca`, default `ovh-eu`).
  Every request is signed with the OVH scheme: an `X-Ovh-Signature` header of
  `"$1$" + sha1(application_secret+consumer_key+GET+<url>++<timestamp>)` (the
  fields `+`-joined with an empty body), the timestamp corrected by the server
  clock learned from `/auth/time`. Lists instances (`GET /vps` or
  `GET /dedicated/server`), fetches each instance's detail plus its `.../ips`,
  and parses the IP list (a `/32` CIDR's address is kept, other prefixes are
  dropped). One target per instance, `__address__` = the default IP (IPv4 else
  IPv6, no port), an `instance` label of the instance name, and the full
  `__meta_ovhcloud_vps_*` / `__meta_ovhcloud_dedicated_server_*` label set.
  Refresh interval from `-promscrape.ovhcloudSDCheckInterval` (default 30s).
  `service` and `endpoint` are validated at config parse time. **Deferred:**
  upstream's inline `HTTPClientConfig`/`proxy_url` knobs are not ported.
- **OpenStack SD** (done): `openstack_sd_configs` with `role` (`instance` or
  `hypervisor`), `identity_endpoint` (Keystone v3 base), `region`, `port`
  (default 80), `all_tenants`, `availability` (default `public`), `tls_config`,
  and the auth fields (`username`/`userid`/`password`, `project_*`/`domain_*`,
  `application_credential_id`/`_name`/`_secret`). Authenticates against Keystone
  v3 (`POST <identity_endpoint>/auth/tokens` with a body built to match
  upstream `buildAuthRequestBody` byte-for-byte — password auth scoped by
  `project`/`domain`, or all three application-credential variants), reads the
  `X-Subject-Token` header and parses the service catalog for the compute
  endpoint matching `region`+`availability`, then paginated-lists Nova
  `servers/detail` (`instance`) or `os-hypervisors/detail` (`hypervisor`). Token
  + compute URL cached until the token's `expires_at` (converted to a monotonic
  deadline), with a 401-triggered re-auth as a safety net. `instance`: one
  target per server address (per pool, `__address__` = fixed IP + `port`, a
  pool's floating IP -> `__meta_openstack_public_ip`) with
  `__meta_openstack_instance_*`/`_project_id`/`_user_id`/`_address_pool`/
  `_private_ip` + sanitized `_tag_<k>` metadata labels; `hypervisor`: one target
  per hypervisor (`__address__` = `host_ip` + `port`) with the
  `__meta_openstack_hypervisor_*` labels. When `identity_endpoint` is unset,
  credentials fall back to the `OS_*` env vars. `role` is validated at config
  parse time. Refresh interval from `-promscrape.openstackSDCheckInterval`
  (default 30s). **Deferred:** the legacy `v2.0` identity endpoint is rejected
  at build time; upstream's inline `HTTPClientConfig`/`proxy_url` knobs are not
  ported.
- **Nomad SD** (done): `nomad_sd_configs` with `server` (config, `NOMAD_ADDR`,
  else `localhost:4646`), `namespace` (config or `NOMAD_NAMESPACE`), `region`
  (config, `NOMAD_REGION`, else `global`), `tag_separator`, `allow_stale`,
  token (`NOMAD_TOKEN` env or inline `bearer_token`, sent as
  `Authorization: Bearer` per upstream — not Nomad's `X-Nomad-Token`) /
  inline `basic_auth` / `tls_config` auth. Lists `/v1/services`, then fetches
  each service's registrations from `/v1/service/<name>`, one target per
  registration (`__address__` = `Address` + `Port`), and the full
  `__meta_nomad_*` label set (`_service`/`_service_id`/`_service_address`/
  `_service_port`/`_service_alloc_id`/`_service_job_id`, `_address`, `_dc`,
  `_namespace`, `_node_id`, per-tag `_tag_<k>`/`_tagpresent_<k>` +
  comma-wrapped `_tags`). Refresh interval from
  `-promscrape.nomadSDCheckInterval` (default 30s). **Deviation:** interval
  re-list, not Nomad blocking-query long-poll (`?index=&wait=`) — so no
  `-promscrape.nomad.waitTime` analog; `allow_stale` is honored.
- **Marathon SD** (done): `marathon_sd_configs` with `servers` (list, tried in
  order), `bearer_token` (sent as `Authorization: Bearer`) / `basic_auth` /
  `tls_config` auth. Queries `/v2/apps/?embed=apps.tasks` and emits one target
  per app task per port (`__address__` = task host — or, under container
  networking, the task's first container IP — + the selected port, taken from
  `container.portMappings`, the legacy `container.docker.portMappings`, or
  `portDefinitions`, falling back to the task's own `ports`), with the full
  `__meta_marathon_*` label set (`_app`, `_image`, `_task`, `_port_index`,
  sanitized `_app_label_<k>`, and per-port sanitized `_port_mapping_label_<k>`
  / `_port_definition_label_<k>`). Refresh interval from
  `-promscrape.marathonSDCheckInterval` (default 30s). **Deviations:** upstream
  v1.146.0's `marathon.SDConfig` has NO `auth_token` field and sends no
  Marathon-specific `Authorization: token=` header (auth is entirely
  HTTPClientConfig bearer/basic/TLS) — matched here, so `auth_token` is not
  accepted; upstream picks one random server per refresh with no failover,
  whereas this port tries each `server` in order (strictly more robust,
  identical for a single-server config); and for a task exposing **multiple
  ports** this port emits one target per (task, port), whereas upstream's
  duplicate-key `Labels` + `RemoveDuplicates` pipeline collapses such a task to
  a single target (its `__address__` taken from the first port but the
  surviving `__meta_marathon_port_index`/port-labels from the last). The
  common one-port-per-task case is identical; multi-port tasks yield more
  targets here (each port scraped independently — arguably the more useful
  behavior, and free of upstream's first-address/last-labels mismatch).
- **DigitalOcean SD** (done): `digitalocean_sd_configs` with `server`
  (default `https://api.digitalocean.com`), `bearer_token` auth, `tls_config`,
  and `port`. Paginated `/v2/droplets` listing (follows the `links.pages.next`
  cursor), one target per droplet with an IPv4 network (`__address__` = the
  droplet's public IPv4 + `port`), and the full `__meta_digitalocean_*` label
  set (`_droplet_id`/`_droplet_name`, `_image`/`_image_name`,
  `_private_ipv4`/`_public_ipv4`/`_public_ipv6`, `_region`, `_size`,
  `_status`, `_vpc`, comma-wrapped `_features`/`_tags`). Refresh interval from
  `-promscrape.digitaloceanSDCheckInterval` (default 60s).
- **Docker SD** (done): `docker_sd_configs` with `host` (required —
  `unix://<path>`, `tcp://host:port` mapped to `http://host:port`, or
  `http(s)://…`), `port` (default 80), `filters`, `host_networking_host`
  (default `localhost`), `match_first_network` (default `true`), and
  `basic_auth`/`bearer_token`/`tls_config` auth on the HTTP arm. Fetches
  `/networks` + `/containers/json` (unversioned paths) and emits one target per
  container network × exposed TCP port (fallback target when a container has no
  TCP port; `host_networking_host` for `network_mode: host`), with the full
  `__meta_docker_*` label set (`_container_id`/`_name`/`_network_mode`,
  sanitized `_container_label_<k>`, `_network_ip`,
  `_port_private`/`_port_public`/`_port_public_ip`, and the joined
  `_network_id`/`_name`/`_scope`/`_internal`/`_ingress`/sanitized
  `_network_label_<k>`); `match_first_network` keeps only the lowest-named
  network per container, and `network_mode: container:<id>` inherits the linked
  container's networks. Unix-socket hosts use a hand-rolled synchronous
  HTTP/1.1 client (chunked-transfer-decoding) over `std::os::unix::net::
  UnixStream`, gated to Unix platforms (a `unix://` host errors at fetch time
  on Windows; the client is shared with the forthcoming Dockerswarm provider);
  `tcp`/`http(s)` hosts use `reqwest`. Refresh interval from
  `-promscrape.dockerSDCheckInterval` (default 30s).
- **Vultr SD** (done): `vultr_sd_configs` with `server`
  (default `https://api.vultr.com`), `bearer_token` (Vultr API key, required)
  auth, `tls_config`, and `port` (default 80). Cursor-paginated
  `/v2/instances?per_page=100` listing (follows the opaque `meta.links.next`
  cursor until empty), one target per instance (`__address__` = the instance's
  `main_ip` + `port`), and the full `__meta_vultr_instance_*` label set
  (`_id`/`_label`/`_os`/`_os_id`/`_region`/`_plan`/`_main_ip`/`_internal_ip`/
  `_main_ipv6`/`_hostname`/`_server_status`/`_vcpu_count`/`_ram_mb`/
  `_allowed_bandwidth_gb`/`_disk_gb`, comma-wrapped `_features`/`_tags`).
  Refresh interval from `-promscrape.vultrSDCheckInterval` (default 30s,
  matching upstream `vultr.SDCheckInterval`). **Deviations:** `server` is an
  esmagent-only endpoint-override field not present in upstream's Vultr
  `SDConfig` (which hardcodes the endpoint) — it defaults to the real API and
  exists to let tests point at a stub, same as the EC2/GCE/Azure endpoint
  overrides; Vultr's API-side filter query params (`label`/`main_ip`/`region`/
  `firewall_group_id`/`hostname`) are not ported — filter via
  `relabel_configs`.
- **PuppetDB SD** (done): `puppetdb_sd_configs` with `url` (required,
  `http`/`https` with a host), `query` (required PQL), `include_parameters`
  (default `false`), `port` (default 80), and `bearer_token`/`basic_auth`/
  `tls_config` auth. Single `POST <url>/pdb/query/v4` with a JSON
  `{"query": "<pql>"}` body, one target per returned resource (`__address__` =
  the resource's `certname` + `port`), and the full `__meta_puppetdb_*` label
  set (`_query`/`_certname`/`_environment`/`_exported`/`_file`/`_resource`/
  `_title`/`_type`, comma-wrapped `_tags`, and — only when `include_parameters`
  is `true` — the sanitized `_parameter_<k>` set: list-valued params
  comma-joined, nested objects flattened as `_parameter_<obj>_<k>`, bool/number
  params stringified, empty/unrepresentable params dropped). Refresh interval
  from `-promscrape.puppetdbSDCheckInterval` (default 30s, matching upstream
  `puppetdb.SDCheckInterval`). **Deviations:** interval re-run rather than
  upstream's `discoveryutil` client polling (same shape as the other ports);
  `proxy_url` is not ported; JSON numbers are formatted via Rust's shortest
  round-tripping `{}` (matching Go's `FormatFloat 'g'` except Go switches to
  exponent notation for extreme magnitudes, which no PuppetDB parameter hits).
- **Kuma SD** (done): `kuma_sd_configs` with `server` (required MADS endpoint;
  `http://` assumed when scheme-less; any base path and query are preserved
  before the `/v3/discovery:monitoringassignments` suffix — ported exactly from
  `getAPIServerPath`), `client_id` (default: OS hostname, else `vmagent`), and
  `bearer_token`/`basic_auth`/`tls_config` auth. A single `POST` of an xDS
  `DiscoveryRequest` JSON body (`node.id` = `client_id`, the
  `MonitoringAssignment` `type_url`) parses the `DiscoveryResponse` into one
  target per assignment target (`__address__` = the target's address) with
  `instance` (= target name), `__scheme__`, `__metrics_path__`, and the
  `__meta_kuma_*` set (`_dataplane` = target name, `_mesh`, `_service`, and a
  sanitized `_label_<k>` per assignment- then target-level label — target wins
  on a key collision). Refresh interval from `-promscrape.kumaSDCheckInterval`
  (default 30s, matching upstream `kuma.SDCheckInterval`). **Deviations:**
  poll-based — an empty `version_info`/`nonce` is sent each refresh and the full
  response is taken (upstream keeps the xDS ACK cursor and honors `304`), a
  faithful request-body shape without the incremental optimization; the OS
  hostname for the default `client_id` is read from `$HOSTNAME`/`$COMPUTERNAME`
  (portable, no `libc`/unsafe) rather than `os.Hostname()`; `proxy_url` is not
  ported.
- **Eureka SD** (done): `eureka_sd_configs` with `server`
  (default `localhost:8080/eureka/v2`; scheme prepended when absent — `https`
  if `tls_config` set, else `http`), and `bearer_token`/`basic_auth`/
  `tls_config` auth. Single `GET <server>/apps` with `Accept: application/xml`,
  the XML response parsed via `quick-xml` serde, one target per registered
  instance (`__address__` = instance `hostName` + its `<port>`, falling back to
  `80`), and the full `__meta_eureka_*` label set (`_app_name`,
  `_app_instance_id`/`_hostname`/`_ip_addr`/`_vip_address`/`_secure_vip_address`/
  `_status`/`_country_id`/`_homepage_url`/`_statuspage_url`/`_healthcheck_url`,
  the `_port`/`_port_enabled` and `_secure_port`/`_secure_port_enabled` pairs
  when present, `_datacenterinfo_name` + sanitized `_datacenterinfo_metadata_<k>`
  when a datacenter is set, and a sanitized `_metadata_<k>` per instance-metadata
  entry), plus the `instance` label overridden with the Eureka instance id.
  Refresh interval from `-promscrape.eurekaSDCheckInterval` (default 30s,
  matching upstream `eureka.SDCheckInterval`). **Deviations:** interval re-list
  rather than upstream's `discoveryutil` client polling (same shape as the other
  ports); `Accept: application/xml` is sent explicitly (upstream relies on the
  server defaulting to XML) so the server returns the XML this port parses;
  `proxy_url` is not ported.
- **Hetzner SD** (done): `hetzner_sd_configs` with `role` (`hcloud` or
  `robot`), `bearer_token` (required for `hcloud`) / `basic_auth` (required for
  `robot`), `tls_config`, and `port` (default 80). For `role: hcloud`,
  Bearer-auth paginated `/v1/servers` + `/v1/networks` against
  `https://api.hetzner.cloud` (follows `meta.pagination.next_page`), one target
  per server (`__address__` = the server's public IPv4 + `port`), the common
  `__meta_hetzner_*` set plus the `__meta_hetzner_hcloud_*` set (location /
  network-zone / server-type / cpu / memory / disk / image, sanitized
  per-network `_private_ipv4_<name>` and per-label `_label_<k>`/
  `_labelpresent_<k>`). For `role: robot`, HTTP-Basic `/server` against
  `https://robot-ws.your-server.de`, one target per dedicated server, the
  common set plus `__meta_hetzner_robot_*` (`_datacenter`/`_product`/
  `_cancelled`). Refresh interval from `-promscrape.hetznerSDCheckInterval`
  (default 60s). **Deviation:** an unknown `role` is rejected at config-parse
  time (upstream panics in `GetLabels`); this port returns an error instead.
- **EC2 SD** (done): `ec2_sd_configs` with `region` (config, `AWS_REGION`, or
  IMDS), `endpoint`, `filters`/`az_filters`, `port`, and a SCOPED credential
  chain — static `access_key`/`secret_key` (+ `session_token`), the
  `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (+ `AWS_SESSION_TOKEN`)
  environment, or the IMDSv2 instance role (cached until near expiration,
  short-timeout-bounded). AWS Signature V4-signed `DescribeInstances`
  (paginated) + best-effort `DescribeAvailabilityZones`, the full
  `__meta_ec2_*` label set. Refresh interval from
  `-promscrape.ec2SDCheckInterval` (default 60s). **Deferred:** STS
  `role_arn`/`AssumeRole` (rejected at build time), web-identity token file
  (`AWS_WEB_IDENTITY_TOKEN_FILE`), and shared `~/.aws` config/credentials
  files (`profile`). Query API version `2016-11-15`, matching upstream.
- **Consul SD** (done): `consul_sd_configs` with `server`/`scheme`/
  `datacenter` (resolved via `/v1/agent/self` when unset), `services`/`tags`
  filtering, enterprise `namespace` (config or `CONSUL_NAMESPACE` env
  fallback)/`partition`, `node_meta`, `filter`, `tag_separator`,
  `allow_stale`, token (config or `CONSUL_HTTP_TOKEN[_FILE]`) /
  `username`+`password` / inline
  `basic_auth`/`bearer_token` / `tls_config` auth, and the full
  `__meta_consul_*` label set. Refresh interval from
  `-promscrape.consulSDCheckInterval` (default 30s). **Deviation:** interval
  re-list, not Consul blocking-query long-poll (`?index=&wait=`) — so no
  `-promscrape.consul.waitTime` analog; `allow_stale` is honored.
- **Consul Agent SD** (done): `consulagent_sd_configs` — like Consul SD but
  queries the LOCAL agent (`/v1/agent/self`, `/v1/agent/services`,
  `/v1/agent/health/service/name/<svc>`) rather than the cluster
  catalog/health APIs, with the `__meta_consulagent_*` label prefix and the
  agent-derived `_address`/`_dc`/`_node`/`_metadata_*` labels. `server`/
  `scheme`/`datacenter` (resolved via `/v1/agent/self` when unset),
  `services` allowlist, `filter`, `namespace` (config or `CONSUL_NAMESPACE`
  env), `tag_separator`, and the same token/basic/`tls_config` auth as Consul
  SD are honored; there is no `partition`/`tags`/`node_meta`/`allow_stale`
  (upstream's consulagent SDConfig has none). Refresh interval from
  `-promscrape.consulagentSDCheckInterval` (default 30s). **Deviation:**
  interval re-list from a single background thread, not one long-poll
  goroutine per service.
- **Kubernetes SD** (Phase A+B done): `pod`/`node`/`service`/`ingress`/
  `endpoints`/`endpointslice` roles, in-cluster, explicit `api_server`, or
  `kubeconfig_file` auth (with the kubeconfig cluster `proxy-url`), the
  standalone `proxy_url` SDConfig field (explicit-`api_server`/in-cluster
  clients), list+watch with an in-memory per-role cache,
  `namespaces`/`selectors` filtering, per-role `__meta_kubernetes_*`
  labels, a shared cross-role object cache (endpoints/endpointslice join
  their `Service` + `targetRef` `Pod`), `attach_metadata: {node,
  namespace}` label-joining, and the global
  `-promscrape.kubernetes.attachNodeMetadataAll`/`attachNamespaceMetadataAll`
  flags (default `attach_metadata` for every config, overridden per-config).
  OAuth2 client-credentials auth (`oauth2:` block — `client_id`,
  `client_secret`/`client_secret_file`, `scopes`, `token_url`,
  `endpoint_params`, token-endpoint `tls_config`/`proxy_url`) is supported: a
  token is fetched via the client-credentials grant, cached until shortly
  before `expires_in`, and attached as a bearer token. Remaining k8s gaps
  (deferred): the OAuth2 token-request `headers` field and credential-style
  autodetect (body-only `AuthStyleInParams` here), kubeconfig
  `exec`/impersonation auth, upstream's cross-config `groupWatcher` dedup
  (this port starts one watcher per `kubernetes_sd_config`), and
  `-promscrape.kubernetes.apiServerTimeout` (the watch `timeoutSeconds` is a
  fixed 60s).
- The HTML `/targets` page (JSON `/api/v1/targets` only), `scrape_config_files`
  (external scrape-config includes), `series_limit` (and the global
  `-promscrape.maxLabelNameLen`/`maxLabelValueLen` flags), per-target
  interval/timeout override via a relabel-rewritten
  `__scrape_interval__`/`__scrape_timeout__` label, and OAuth2 auth for
  per-job `scrape`/`http_sd` configs (OAuth2 *is* supported for
  `kubernetes_sd_configs`) — see `crates/esmagent/README.md`'s "Scraping
  limitations" for the full, itemized list
- Stream aggregation (`lib/streamaggr`): ported as the `esm-streamaggr`
  crate AND wired into esmagent as a GLOBAL `-streamAggr.config` stage
  (applied after global relabel, before fan-out; aggregated output forwarded
  to every destination, matched input dropped unless `-streamAggr.keepInput`).
  Flags: `-streamAggr.config`/`keepInput`/`dedupInterval`/`dropInputLabels`/
  `ignoreOldSamples`/`ignoreFirstIntervals`/`flushOnShutdown`. Deferred:
  PER-URL `-remoteWrite.streamAggr.config` (per-destination aggregation inside
  each rwctx) and the single-node esmetrics `-streamAggr.config`
- `-remoteWrite.oauth2.*` auth flags
- Multitenancy (`-remoteWrite.multitenantURL`, per-tenant routing)
- The blocking (non-drop) backpressure mode — this port only implements the
  default drop-oldest `-remoteWrite.maxDiskUsagePerURL` behavior, not
  upstream's alternative "block the ingest path until the queue drains"
  mode
- `-remoteWrite.sendTimeout` is not a flag yet — every destination's HTTP
  client uses a fixed 30s request timeout (`REMOTE_WRITE_SEND_TIMEOUT` in
  `esmagent/src/lib.rs`)
- `tlsServerName` (SNI override) is parsed but not applied — same
  `reqwest`-blocking-client limitation as `esmalert`/`esmauth`

`PersistentQueue`'s on-disk block format is a faithful **behavior** port of
`lib/persistentqueue` (durable FIFO, in-memory + disk, size-capped
drop-oldest), not its exact chunk/metadata file layout — the queue has no
external reader, so the format is a private implementation detail (see
`esmagent/src/queue.rs`'s module doc). Queue durability is process-crash-safe
(every push is fsync'd + atomically renamed before it returns); power-loss
durability additionally needs the directory-fsync step in
`flush_to_disk`/`close`, which is best-effort and silently skipped on
Windows. `-remoteWrite.maxDiskUsagePerURL=0` (the default) means unlimited,
matching upstream — `PersistentQueue::open` normalizes `0` to `u64::MAX`
internally so the size-cap eviction logic never treats a caller's "no cap"
as a literal zero-byte cap.

## Porting rules

1. **On-disk format compatibility is a non-goal**; algorithmic fidelity is the goal.
   Where the Go upstream trades CPU for GC pressure, the Rust port may restructure for zero-copy.
2. Port unit tests alongside each module (`*_test.go` → Rust `#[cfg(test)]`).
3. Every crate must build warning-free on `x86_64-unknown-linux-gnu` and
   `x86_64-pc-windows-gnu` (`cargo check --target`). Windows release
   binaries are built for `x86_64-pc-windows-msvc` via `cargo xwin`
   (faster codegen than MinGW); the gnu target remains the lightweight
   compile check.
4. Unsafe code only where measured; keep it wrapped and documented.

## Phase plan

1. **Foundations** — esm-common, esm-encoding (parallel-friendly, few deps).
2. **mergeset** — needs foundations.
3. **storage** — the big one; split: types → index_db → parts/blocks → table/partition → search.
4. **ingest** — influx parser + insert API (can proceed in parallel with 3 once types exist).
5. **query** — metricsql parser (independent, can start any time) → promql engine → select API.
6. **server** — binary wiring, Windows build.
7. **bench** — TSBS runs vs the Go upstream, optimize until all metrics win on Linux, then Windows.
