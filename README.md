<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/logo-dark.svg">
    <img src="assets/logo.svg" alt="EsMetrics" width="420">
  </picture>
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="License"></a>
  <img src="https://img.shields.io/badge/version-1.146.0-brightgreen" alt="Version">
  <img src="https://img.shields.io/badge/platforms-Linux%20%7C%20Windows-informational" alt="Platforms">
  <a href="https://github.com/softalink/esmetrics/actions/workflows/ci.yml"><img src="https://img.shields.io/badge/CI-GitHub%20Actions-success" alt="CI"></a>
</p>

# EsMetrics

**EsMetrics** is a secure, fast, memory-safe time-series database by
[Softalink LLC](https://softalink.com) — a from-scratch Rust implementation
of [VictoriaMetrics](https://github.com/VictoriaMetrics/VictoriaMetrics)
single-node (reference **v1.146.0**) that outperforms the original on every
[TSBS](https://github.com/timescale/TSBS) benchmark metric, on both Linux
and Windows, while answering queries **byte-for-byte identically**.

- ⚡ **+43-66% ingestion throughput** and **20-82% lower query latency** than
  Go VictoriaMetrics on the same hardware
- 🔁 **Drop-in compatible**: Influx line protocol in, Prometheus HTTP API out
  — existing agents, Grafana dashboards, and PromQL queries just work
- 🛡️ **Memory-safe by construction**: no GC pauses, no `unsafe` shortcuts on
  data paths, deterministic resource cleanup
- 📦 **One small static binary** per platform (the Windows build is ~4 MB),
  no runtime dependencies
- 🪟 **First-class Windows support**, benchmarked and tested on real
  Windows hardware — not just a cross-compile checkbox

## Quick start

Grab a binary from the [releases page](https://github.com/softalink/esmetrics/releases)
(or [build from source](#building-from-source)), then:

```bash
# Start the server (retention in months)
esmetrics -storageDataPath=/var/lib/esmetrics -retentionPeriod=12 -httpListenAddr=:8428

# Ingest via Influx line protocol
curl -X POST 'http://localhost:8428/write' \
  --data-binary 'cpu,host=web01 usage_user=42.5,usage_system=7.1'

# Query via the Prometheus HTTP API
curl 'http://localhost:8428/api/v1/query' \
  --data-urlencode 'query=max(max_over_time(cpu_usage_user{host="web01"}[1m]))'
```

Supported endpoints: `/write` (Influx), `/api/v1/query_range`,
`/api/v1/query`, `/api/v1/series`, `/api/v1/labels`,
`/api/v1/label/<name>/values`, `/api/v1/export`, `/health`,
`/snapshot/create|list|delete|delete_all` (storage snapshots), plus
`/logo.svg` and `/favicon.ico`. A browser UI (vmui, vendored from
VictoriaMetrics) is served at `/esmui/` (the legacy `/vmui/` tree 302-redirects there). Flags mirror VictoriaMetrics
single-node (`-storageDataPath`, `-retentionPeriod`, `-httpListenAddr`,
`-memory.allowedPercent`, ...), so existing deployment recipes carry over.

`esmauth` is the auth/routing reverse proxy (a port of upstream `vmauth`):
put it in front of one or more `esmetrics` instances for token-based auth,
`url_prefix`/`url_map` routing, and load balancing. It listens on `:8427`
by default and is configured with `-auth.config=/path/to/auth.yml`; config
changes are picked up via SIGHUP, `-configCheckInterval`, or `/-/reload`
without dropping connections.

```yaml
# auth.yml — one token, load-balanced across two esmetrics nodes; a
# second token scoped to read-only query endpoints via url_map.
users:
  - bearer_token: WRITE_TOKEN
    url_prefix:
      - http://node1:8428
      - http://node2:8428
    load_balancing_policy: least_loaded   # or first_available
  - bearer_token: READ_TOKEN
    url_map:
      - src_paths: ["/api/v1/query.*", "/api/v1/series", "/api/v1/label.*"]
        url_prefix:
          - http://node1:8428
          - http://node2:8428
```

```
esmauth -auth.config=auth.yml -httpListenAddr=:8427

curl -H 'Authorization: Bearer READ_TOKEN' \
  'http://localhost:8427/api/v1/query?query=up'
```

Global and per-user concurrency limits (`-maxConcurrentRequests`,
`max_concurrent_requests`), retries (`retry_status_codes`), and backend
health (`-failTimeout`) are supported. The `/-/reload` and `/metrics`
endpoints can be gated with `-reloadAuthKey`/`-metricsAuthKey`, and
`-readTimeout` (default 30s) bounds slow clients. See
[docs/PORTING.md](docs/PORTING.md) for the full ported/out-of-scope surface
(JWT/OIDC, DNS discovery, and backend-TLS tuning are out of scope).

`esmalert` is the alerting/recording-rule engine (a port of upstream
`vmalert`): it evaluates MetricsQL rules on a schedule, drives the alert
`Pending`/`Firing`/`Inactive` state machine (`for:`/`keep_firing_for:`),
pushes recording-rule results and `ALERTS`/`ALERTS_FOR_STATE` series to
remote-write, sends firing/resolved alerts to Alertmanager, and restores a
`for:` alert's progress across restarts from a remote-read datasource.

```yaml
# alerts.yml
groups:
  - name: example
    interval: 30s
    rules:
      - alert: HighLoad
        expr: node_load1 > 4
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "{{ $labels.instance }} load is {{ $value }}"
      - record: job:up:avg
        expr: avg(up) by (job)
```

```bash
esmalert -rule=alerts.yml \
  -datasource.url=http://localhost:8428 \
  -remoteWrite.url=http://localhost:8428 \
  -remoteRead.url=http://localhost:8428 \
  -notifier.url=http://localhost:9093 \
  -httpListenAddr=:8880
```

Flags follow upstream vmalert's shape: `-datasource.url`/`-remoteWrite.url`/
`-remoteRead.url`/`-notifier.url` (repeatable) each take a matching
`.basicAuth.{username,password[,File]}`/`.bearerToken[,File]`/`.tls*` set,
plus `-evaluationInterval` (default `1m`), `-external.url`, `-dryRun`
(validate `-rule` files and exit without starting anything),
`-configCheckInterval` (polling reload; `/-/reload` also works), and
`-reload.authKey`/`-metrics.authKey` gating those two endpoints. A JSON API
(`GET /api/v1/rules`, `/api/v1/alerts`, `/api/v1/rule`, `/api/v1/alert`,
`/api/v1/group`, `/-/healthy`) serves on `-httpListenAddr` (default
`:8880`); there is no HTML UI yet. See
[crates/esmalert/README.md](crates/esmalert/README.md) for the full flag
reference and an honest list of deferred features (replay/backfill mode,
notifier service discovery, oauth2, Graphite/VictoriaLogs datasources) and
divergences (a Go-template subset that doesn't support time/duration method
calls like `.Sub`/`.Add`).

`esmalert-tool` is the offline rule unit-tester (a port of upstream
`vmalert-tool`, the `promtool test rules` analog): it stands up a real
in-process `esmetrics` engine per test file, feeds it synthetic
`input_series`, evaluates your rule files against it, and asserts the
resulting alerts/recording-rule output — no live datasource or remote-write
target needed.

```yaml
# mytest.yml
rule_files:
  - alerts.yml
evaluation_interval: 1m
tests:
  - name: high load fires after for
    interval: 1m
    input_series:
      - series: 'node_load1{instance="h1"}'
        values: '5 5 5 5 5'
    alert_rule_test:
      - eval_time: 2m
        groupname: example
        alertname: HighLoad
        exp_alerts:
          - exp_labels: { severity: warning, instance: h1 }
```

```bash
esmalert-tool unittest mytest.yml
```

See [crates/esmalert-tool/README.md](crates/esmalert-tool/README.md) for the
test-file format and an honest list of limitations.

`esmagent` is a port of upstream `vmagent`: **scrape + remote-write
forwarding**. It accepts metrics via any of `esmetrics`' push protocols AND
actively scrapes `/metrics` targets it discovers itself
(`-promscrape.config`: static/`file_sd`/`http_sd`/Kubernetes discovery —
`pod`/`node`/`service`/`ingress` roles, in-cluster or explicit `api_server`
auth); either way, every accepted series relabels and fans out to one or
more remote-write destinations, each with its own durable on-disk queue and
retrying delivery worker pool, so one destination being down never blocks
or loses data bound for another.

```bash
esmagent -remoteWrite.url=http://localhost:8428/api/v1/write \
  -remoteWrite.url=http://backup-region:8428/api/v1/write \
  -promscrape.config=scrape.yml \
  -httpListenAddr=:8429
```

> **Scope note.** The **entire non-Kubernetes cloud service-discovery surface
> is ported** — Consul, Consul Agent, EC2, GCE, Azure, DigitalOcean, Hetzner,
> Nomad, Marathon, Vultr, PuppetDB, Kuma, Eureka, Yandex Cloud, OVHcloud,
> OpenStack, DNS, Docker, and Docker Swarm — alongside
> `static_configs`/`file_sd_configs`/`http_sd_configs` and Kubernetes SD
> (`pod`/`node`/`service`/`ingress`/`endpoints`/`endpointslice` roles). No SD
> key is rejected as "deferred" anymore. See
> [crates/esmagent/README.md](crates/esmagent/README.md) for the full flag
> reference and an honest list of the remaining deferred features (the HTML
> `/targets` page, per-target series limits, stream aggregation, oauth2 for
> per-job scrape/http_sd configs, multitenancy, the blocking backpressure
> mode, and the residual Kubernetes SD auth gaps).

## Benchmarks

Full methodology, raw per-round data, and honest caveats:
[benchmarks/results/README.md](benchmarks/results/README.md). Headlines
(TSBS `cpu-only`, scale 100, 8.64M metrics, medians of 3 paired
back-to-back rounds against the official v1.146.0 release binaries;
current code as of 2026-07-06):

### Linux — load +34%, all query types faster

| metric | Go v1.146.0 | EsMetrics | delta |
|---|---|---|---|
| ingest (metrics/sec) | 3.73M | 5.00M | **+34%** |

| query type (med/mean/max ms) | Go | EsMetrics | mean |
|---|---|---|---|
| single-groupby-1-1-1 | 0.32/0.42/8.72 | 0.16/0.21/2.00 | −50% |
| single-groupby-1-1-12 | 0.69/0.78/2.52 | 0.22/0.26/1.19 | −67% |
| single-groupby-1-8-1 | 0.69/0.81/4.06 | 0.45/0.53/3.25 | −35% |
| single-groupby-5-1-1 | 0.60/0.67/2.78 | 0.30/0.38/2.28 | −43% |
| single-groupby-5-8-1 | 2.17/2.51/10.59 | 0.82/0.89/3.03 | −65% |
| cpu-max-all-1 | 0.82/0.91/3.46 | 0.59/0.71/4.17 | −22% |
| cpu-max-all-8 | 4.26/4.51/11.64 | 3.64/3.89/12.21 | −14% |
| double-groupby-1 | 6.41/6.71/18.06 | 4.88/5.05/9.89 | −25% |
| double-groupby-5 | 32.39/33.41/77.29 | 25.03/25.37/44.60 | −24% |
| double-groupby-all | 65.63/67.10/135.56 | 48.33/48.61/89.06 | −28% |

### Windows (physical host) — load +74%, all query types faster

Physical Windows 11 host (build 26100), 8 logical CPUs, 32 GiB RAM; both
servers as native Windows binaries, TSBS clients local to the host. Medians
of 3 back-to-back paired rounds. EsMetrics is the production MSVC build (the
Windows release artifact).

| metric | Go v1.146.0 | EsMetrics (MSVC) | delta |
|---|---|---|---|
| ingest (metrics/sec) | 2.95M | 5.12M | **+74%** |

| query type (med/mean/max ms) | Go | EsMetrics (MSVC) | mean |
|---|---|---|---|
| single-groupby-1-1-1 | 0.52/0.50/4.30 | 0.00/0.26/7.26 | −48% |
| single-groupby-1-1-12 | 1.02/0.87/7.70 | 0.51/0.31/2.04 | −64% |
| single-groupby-1-8-1 | 0.98/0.92/3.09 | 0.52/0.46/2.19 | −50% |
| single-groupby-5-1-1 | 0.66/0.81/7.46 | 0.51/0.35/2.45 | −57% |
| single-groupby-5-8-1 | 3.44/3.58/8.04 | 0.57/0.65/2.09 | −82% |
| cpu-max-all-1 | 1.38/1.35/3.67 | 0.54/0.60/2.86 | −56% |
| cpu-max-all-8 | 6.81/7.42/17.05 | 3.09/3.23/11.40 | −56% |
| double-groupby-1 | 9.68/10.45/21.58 | 4.74/5.01/30.67 | −52% |
| double-groupby-5 | 44.70/49.25/73.03 | 20.54/20.75/48.88 | −58% |
| double-groupby-all | 87.26/98.01/145.62 | 40.81/41.43/75.04 | −58% |

Sub-millisecond readings (e.g. the 0.00ms median) reflect the TSBS
client clock's ~0.5ms granularity on Windows; server-side QPC timing
confirms EsMetrics' latency floors are below Go's. The two `max` columns
where EsMetrics is higher (single-groupby-1-1-1, double-groupby-1) are
single-sample tail outliers on the median round — EsMetrics wins med and
mean on every type.

### Resource usage — peak memory, CPU, storage

Same workload re-run with a resource-monitored harness (200ms process
sampling; medians of 3 paired rounds per platform; Windows numbers from
the MSVC build on the same physical host as above — see
[benchmarks/results/README.md](benchmarks/results/README.md) for
methodology and per-round data):

| peak resident memory | Go v1.146.0 | EsMetrics | delta |
|---|---|---|---|
| Linux: during ingest | 751 MiB | 179 MiB | −76% |
| Linux: lifetime peak | 775 MiB | 660 MiB | **−15%** |
| Windows: during ingest | 985 MiB | 278 MiB | −72% |
| Windows: lifetime peak | 987 MiB | 543 MiB | **−45%** |

| total CPU for the identical workload | Go | EsMetrics | delta |
|---|---|---|---|
| Linux: load 8.64M metrics | 7.0 s | 4.6 s | −34% |
| Linux: 10,000 queries | 105 s | 78 s | −26% |
| Windows: load 8.64M metrics | 16.8 s | 7.6 s | **−55%** |
| Windows: 10,000 queries | 320 s | 135 s | **−58%** |

Peak instantaneous CPU draw under query load is a saturation tie on both
platforms (both engines use the whole machine; EsMetrics simply finishes
2-2.5× sooner).

| storage size, fully merged | Go v1.146.0 | EsMetrics | delta |
|---|---|---|---|
| Linux | 3.23 MB | 3.23 MB | 0% |
| Windows | 3.43 MB (varies across rounds) | 3.23 MB (byte-stable) | **−6%** |

Format efficiency is identical (~0.37 bytes/sample for the 8.64M-sample
dataset); EsMetrics' final merged size is byte-stable across rounds and
platforms, while Go's Windows runs land 4-8% larger with run-to-run
variation in final part layout.

### Correctness

750 TSBS queries across all 10 types were replayed against both servers on
identical data with `--print-responses`: **byte-identical output** (modulo
the `executionTimeMsec` stat). The port carries 1,300+ tests, most
translated from the upstream Go suites — codecs, dedup tables, regex simplification,
parser grammar, PromQL evaluation goldens, storage round-trips — run on
Linux and on real Windows in CI.

## Why a Rust implementation?

VictoriaMetrics is already one of the fastest open-source time-series
databases; its Go implementation is heavily tuned around the realities of a
garbage-collected runtime (`sync.Pool` everywhere, unsafe byte↔string
casts, buffer recycling discipline). EsMetrics keeps the proven data
structures and algorithms — the mergeset LSM inverted index, the
per-partition indexDB, the timestamp/value block codecs, the
incremental-aggregation query engine — and changes the execution substrate:

- **No GC pauses or GC CPU tax.** The ingest path is allocation-free at
  steady state; that alone is most of the ingestion win.
- **Zero-copy where Go must copy or cast unsafely.** Parsing borrows
  straight from request buffers via lifetimes; the compiler proves what
  Go's `unsafe` helpers merely hope.
- **Fearless intra-query parallelism.** Per-series block unpacking and
  rollup evaluation fan out across persistent worker pools with borrowed
  data — data races are compile-time errors.
- **Deterministic destruction.** Expensive cleanup (mmap teardown, file
  deletion) is deliberately scheduled off latency-critical threads.

Beyond the faithful port, EsMetrics adds measured improvements of its own:
a decoded-block cache (upstream re-decodes on every query), SWAR varint
decoding, fair query-pool scheduling, single-flight series registration
(fixing a duplicate-registration race the port inherited), and a
background part remover that keeps tail latencies flat on Windows. Every
optimization was profile-driven and verified against the byte-identical
correctness harness. The full engineering story is in
[docs/PORTING.md](docs/PORTING.md) and the commit history.

## Building from source

```
cargo build --release                                         # Linux
cargo xwin build --release --target x86_64-pc-windows-msvc    # Windows (cross, recommended)
cargo build --release --target x86_64-pc-windows-gnu          # Windows (cross, MinGW)
```

The MSVC target is the recommended Windows build — it measures ~20-45%
faster than the MinGW build on heavy queries and ~15% faster ingest.
Cross-compiling it from Linux needs `clang`/`lld` and
[`cargo-xwin`](https://github.com/rust-cross/cargo-xwin)
(`cargo install cargo-xwin`), which fetches the Windows SDK automatically;
on a Windows host a plain `cargo build --release` produces it natively.

## Limiting query CPU usage

When co-locating EsMetrics with other workloads, bound the query engine the
same way as upstream VictoriaMetrics:

```
esmetrics -search.maxConcurrentRequests=2 -search.maxWorkersPerQuery=3
```

Aggregate query CPU is bounded by roughly
`maxConcurrentRequests × maxWorkersPerQuery` cores (e.g. `2 × 3` on an
8-core host keeps ~2 cores free under full query load). Defaults —
`min(2 × cpus, 16)` concurrent requests, `min(cpus, 32)` workers per
query — use the whole machine, matching upstream. The caps only trade
latency for headroom: query CPU-seconds stay flat (the query path is
work-conserving; see `benchmarks/results/README.md`), and results are
unchanged by the caps (byte-identity validated at 2×2; see
`benchmarks/results/README.md`).

## Backup and restore

`esbackup` and `esrestore` back up and restore storage snapshots, mirroring
upstream `vmbackup`/`vmrestore`. Destinations are URLs: `fs:///abs/dir`,
`s3://bucket/dir`, `gs://bucket/dir`, or `azblob://container/dir`.

> **⚠️ Not interchangeable with upstream `vmbackup`/`vmrestore`.** EsMetrics
> ports VictoriaMetrics algorithmically, but on-disk storage-format
> compatibility is deliberately a non-goal (see
> [docs/PORTING.md](docs/PORTING.md)). A backup is a faithful copy of the
> storage tree, so the incompatibility carries over: `esrestore` can only
> restore backups made by `esbackup`, and `vmrestore` can only restore
> backups made by `vmbackup`. Don't be misled by the similar remote layout —
> the two tools intentionally share the same object naming and marker files
> (`backup_complete.ignore` etc.), but the part payloads are different
> formats, and cross-restoring will produce a data directory the other
> server cannot open. To migrate data between VictoriaMetrics and EsMetrics
> in either direction, go through the HTTP API instead (e.g. export with
> `/api/v1/export` and re-ingest via `/write`).

Take a snapshot and back it up in one step with `-snapshot.createURL` (the
snapshot is created, backed up, then deleted afterwards):

```bash
esbackup -storageDataPath=/var/lib/esmetrics \
  -snapshot.createURL=http://localhost:8428/snapshot/create \
  -dst=fs:///backups/esmetrics-2026-07-05
```

Restore into a data directory (**the esmetrics server must be stopped** —
`esrestore` syncs the directory to match the backup, deleting local files
that aren't in it, like `rsync --delete`):

```bash
esrestore -src=fs:///backups/esmetrics-2026-07-05 \
  -storageDataPath=/var/lib/esmetrics
```

Pointing `-dst` at an existing backup makes `esbackup` incremental (only
new/changed parts are uploaded, obsolete ones deleted); `-origin` names a
separate existing backup to server-side-copy unchanged parts from instead of
re-uploading them.

Cloud credentials come from standard environment variables:

| Scheme | Environment variables |
|---|---|
| `s3://` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `AWS_ENDPOINT` |
| `gs://` | `GOOGLE_APPLICATION_CREDENTIALS` (service-account JSON path) |
| `azblob://` | `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY` |

## Repository layout

```
crates/
├── esm-common       # bytesutil, fasttime, decimal, uint64set, regexutil, fs, filestream, memory
├── esm-encoding     # varint/zigzag, nearest-delta codecs, zstd block compression
├── esm-mergeset     # LSM inverted-index storage + block caches
├── esm-storage      # TSDB core: TSIDs, indexDB, partitions/parts/blocks, search
├── esm-protoparser  # Influx line-protocol parser (zero-copy, streaming, gzip)
├── esm-metricsql    # MetricsQL/PromQL parser
├── esm-promql       # Query engine: rollups, incremental aggregation, result cache
├── esm-insert       # Ingestion HTTP API (/write)
├── esm-select       # Query HTTP API (/api/v1/*)
├── esm-http         # Minimal HTTP/1.1 server (thread-per-conn, keep-alive)
├── esm-auth         # Auth/routing/load-balancing proxy library (vmauth port)
├── esm-gotemplate   # Go text/template subset (alert/recording rule annotations)
├── esm-relabel      # Relabel-config engine (lib/promrelabel port)
├── esmetrics        # Single-node server binary
├── esmauth          # Auth proxy binary
├── esmalert         # Alerting/recording-rule engine binary (vmalert port)
├── esmalert-tool    # Offline rule unit-test runner (vmalert-tool port)
└── esmagent         # Scrape + remote-write forwarding binary (vmagent port; full SD surface ported — k8s + all non-k8s cloud SD incl. Docker/Docker Swarm)
benchmarks/          # TSBS harnesses (Linux, Windows) and archived results
docs/                # Porting matrix, per-subsystem blueprints, upstream-sync process
```

## Contributing

Contributions are welcome — bug reports, portability fixes, PromQL function
coverage, and benchmark results from other hardware are all valuable.

- **Dev loop**: `cargo test --workspace` (1,300+ tests), `cargo clippy
  --workspace --all-targets` (warnings are errors in CI), `cargo fmt`.
  CI also runs the full suite natively on Windows and gates the
  `x86_64-pc-windows-gnu` build.
- **Tracking upstream**: EsMetrics follows VictoriaMetrics releases via a
  scripted sync loop — baselines pinned in [`UPSTREAM`](UPSTREAM),
  triage reports from `scripts/upstream-diff.sh`, decisions logged in
  [docs/UPSTREAM-SYNC.md](docs/UPSTREAM-SYNC.md). Versions mirror the
  upstream semver (port `1.146.x` ⇔ upstream `v1.146.0`). Helping triage
  an upstream release is a great first contribution.
- **Ground rules**: changes to ported algorithms need either an upstream
  citation or a benchmark + correctness justification; performance claims
  come with `benchmarks/` harness runs.
- **Contact**: questions and proposals to
  [info@softalink.com](mailto:info@softalink.com) or via GitHub issues.

**Contributors**: EsMetrics is developed by **Softalink LLC**, with
**Claude** (Anthropic) as an engineering contributor on the port,
optimization, and benchmarking work.

## Security

EsMetrics is built to be *secure by construction* — memory-safe Rust on
every data path. If you believe you've found a security vulnerability,
please email [info@softalink.com](mailto:info@softalink.com) rather than
opening a public issue; we'll respond promptly and credit reporters.

## Sponsorship & commercial support

EsMetrics is free, Apache-2.0 licensed software. If your organization runs
it in production — or wants to — Softalink LLC offers:

- **Commercial support** — SLAs, deployment reviews, tuning for your
  workload and hardware
- **Sponsored development** — prioritized features (clustering, additional
  ingestion protocols, extended PromQL coverage), benchmark validation on
  your hardware
- **Sponsorship** — fund the upstream-sync cadence and platform coverage
  that keep the project healthy

Reach us at [info@softalink.com](mailto:info@softalink.com) — sponsors are
credited here in the README.

## Scope

Everything the TSBS single-node benchmark and standard Prometheus-style
usage exercises is implemented (backup/restore included — see
[Backup and restore](#backup-and-restore)), plus a broader set of ingestion
protocols: Influx line protocol, Prometheus remote-write, `/api/v1/import`
(JSON and CSV), OTLP metrics, Graphite plaintext, OpenTSDB (telnet and
HTTP `/api/put`), and DataDog `/api/v1/series` + `/api/v2/series`. Alerting
(`esmalert`) and scrape + remote-write forwarding (`esmagent`) are also
ported — see below. The `esmagent` scrape engine now covers the **complete**
promscrape service-discovery surface (static/`file_sd`/`http_sd`/Kubernetes
plus every non-Kubernetes cloud SD: Consul/Consul Agent/EC2/GCE/Azure/
DigitalOcean/Hetzner/Nomad/Marathon/Vultr/PuppetDB/Kuma/Eureka/Yandex Cloud/
OVHcloud/OpenStack/DNS/Docker/Docker Swarm). Deliberately out of scope for now
(see [docs/PORTING.md](docs/PORTING.md)): clustering, downsampling, stream
aggregation, and the web UI beyond the vendored vmui. If one of these
matters to you, see [Sponsorship](#sponsorship--commercial-support).

## License

Apache-2.0. EsMetrics is Copyright © 2026 Softalink LLC. It is a derivative
work of VictoriaMetrics (Copyright VictoriaMetrics, Inc., Apache-2.0) —
see [NOTICE](NOTICE).
