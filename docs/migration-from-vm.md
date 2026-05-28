# Migrating from VictoriaMetrics v1.144.0 to EsMetrics

This guide walks through swapping a running VictoriaMetrics deployment
for EsMetrics. The intended outcome is *no observable change to
downstream consumers* — same wire protocols, same PromQL, same HTTP
surface. Disk-format compatibility is described under "Data" below.

> **State.** EsMetrics is pre-1.0. Treat this guide as a release-day
> deployment plan once we hit v1.0; before then it documents what
> already works.

---

## 0. Read this first

EsMetrics aims to be a drop-in for `vmsingle`, `vmagent`, `vmalert`,
`vmauth`, `vmbackup`, and `vmctl`. The CLI flag surface (Sect. 1) and
the HTTP API surface (Sect. 4) cover the common subset. **Anywhere this
guide says "deferred"**, that surface is either a no-op compat shim with
a startup warning, or simply not implemented — check
[`docs/state/backlog.md`](state/backlog.md) for the live state.

Do not swap in production until you have:

1. A reversible deployment plan (Sect. 6).
2. A read-only canary running side-by-side with vmsingle for at least
   one full retention cycle.
3. The conformance harness (Sect. 5) passing on your real query
   workload.

## 1. Map vmsingle → esm-single

| VM flag                          | EsMetrics flag                        | Notes                                                              |
|----------------------------------|---------------------------------------|--------------------------------------------------------------------|
| `-storageDataPath`               | `--storage-data-path`                 | Both forms accepted via the compat shim                            |
| `-httpListenAddr`                | `--http-listen-addr`                  | Same default `127.0.0.1:8428`                                      |
| `-retentionPeriod`               | `--retention-period-secs`             | EsMetrics takes plain seconds; VM accepts `30d`, `12mo`, etc.      |
| `-maxInsertRequestSize`          | `--max-insert-request-size`           | Default 64 MiB matches VM                                          |
| `-search.maxQueryDuration`       | `--search-max-query-duration-secs`    | **Accepted, not yet enforced**                                     |
| `-search.maxSeries`              | `--search-max-series`                 | **Accepted, not yet enforced**                                     |
| `-http.maxGracefulShutdownDuration` | `--http-request-timeout-secs`     | Different semantics — see Sect. 2                                  |
| `-loggerLevel`                   | `--logger-level`                      | trace / debug / info / warn / error                                |
| `-selfScrapeInterval=0`          | `--disable-self-metrics`              | Disables `/metrics`                                                |
| `-tlsCertFile`                   | `--tls-cert-file`                     | **Accepted, TLS termination not yet implemented**                  |
| `-tlsKeyFile`                    | `--tls-key-file`                      | See above                                                          |
| `-httpAuth.password`             | `--http-auth-header`                  | Pass the full `Authorization` header value                         |
| `-snapshotsDir`                  | _implicit_                            | Snapshots live at `<data-dir>/snapshots/` and have no separate flag |

The compat shim in `parse_cli_with_vm_compat()` translates single-dash
camelCase flags into the kebab-case equivalents at parse time, so
`-storageDataPath /data` works unchanged.

## 2. Map vmagent → esm-agent

`esm-agent` accepts a vmagent-style `promscrape.config` via `--config`:

```yaml
scrape_configs:
  - job_name: node
    metrics_path: /metrics
    static_configs:
      - targets:
          - host-a:9100
          - host-b:9100
```

Remote-write goes to `--remote-write-url`, which must accept
`POST /api/v1/import/prometheus` (esm-single does). Failed forwards are
buffered to `--queue-dir` and replayed on the next tick.

**Not yet implemented:** kubernetes/consul/EC2/GCE/Azure/DNS/HTTP
service discovery (`static`+`file` only), relabel actions beyond the 11
documented in the engine, multiple remote-write targets per agent.

## 3. Map vmalert → esm-alert

The rule-file schema is Prometheus / vmalert compatible. By default
esm-alert queries an upstream over HTTP. Setting `--local-data-path`
makes it evaluate via the embedded PromQL evaluator instead — useful on
the same host as esm-single, but only against a snapshot directory
because the data dir takes an exclusive lock.

`for:` and `keep_firing_for:` semantics match Prometheus.
Recording-rule output is not yet persisted.

## 4. HTTP API surface

Implemented endpoints:

| Path                                        | Method | Notes                                |
|---------------------------------------------|--------|--------------------------------------|
| `/api/v1/query`                             | GET    | esm's lightweight metric query       |
| `/api/v1/promql`                            | GET    | Instant PromQL                       |
| `/api/v1/promql_range`                      | GET    | Range PromQL                         |
| `/api/v1/series`                            | GET    | Prometheus-compatible                |
| `/api/v1/labels`                            | GET    | Prometheus-compatible                |
| `/api/v1/label/:name/values`                | GET    | Prometheus-compatible                |
| `/api/v1/status/{buildinfo,runtimeinfo,flags,tsdb}` | GET | Prometheus-compatible      |
| `/api/v1/targets`                           | GET    | Always returns `activeTargets: []` (esm-single doesn't scrape) |
| `/api/v1/import/prometheus`                 | POST   | Text exposition                      |
| `/api/v1/write`                             | POST   | Prom remote-write (snappy + protobuf)|
| `/write` / `/api/v2/write`                  | POST   | Influx line v1 / v2                  |
| `/api/v1/import/graphite`                   | POST   | Plaintext graphite                   |
| `/api/v1/import`                            | POST   | JSON line                            |
| `/api/put` / `/api/v1/import/opentsdb`      | POST   | OpenTSDB HTTP / telnet               |
| `/api/v1/datadog/series`                    | POST   | DataDog Series API                   |
| `/api/v1/import/csv`                        | POST   | CSV (3 or 4 columns)                 |
| `/api/v1/newrelic/infra/v2/metrics/events/bulk` | POST | NewRelic Metric API              |
| `/opentelemetry/v1/metrics`                 | POST   | OTLP protobuf (gauge + sum only)     |
| `/snapshot/{create,list,delete/:name}`      | varies | VM-compatible snapshot endpoints     |

Not yet implemented: `/api/v1/import/native` (VM's binary format —
needs `Block.UnmarshalPortable` work).

## 5. Verify before the cutover

1. Spin up esm-single alongside vmsingle on the same host. Point a
   subset of agents at it. Confirm `/metrics` reports the expected
   series count.
2. Use the [conformance harness](../conformance/README.md) to compare
   PromQL output between the two for your real dashboards.
3. Run `esm-ctl inspect --storage-data-path=…` against the new data dir
   to confirm series count tracks ingest rate.

## 6. Cutover

The recommended flow is:

1. **Snapshot.** While vmsingle is still serving, take a vmbackup
   snapshot. EsMetrics cannot import vmbackup output yet (G2/G3 in the
   backlog); the snapshot exists for rollback only.
2. **Drain agents.** Update vmagent remote-write to point at
   esm-single. Confirm ingest catches up.
3. **Drain queries.** Move read traffic to esm-single. Watch error
   rates and p99.
4. **Decommission vmsingle.** Stop the process, archive its data dir.

### Rolling back

Until G2/G3 land, rolling back means starting vmsingle against the
snapshot taken in step 1 and re-pointing agents. Any samples ingested
into esm-single during the cutover window are lost on rollback —
budget your test window with this in mind.

## 7. Operational changes you should know about

- **Single binary per app.** vmsingle/vmagent/vmalert/vmauth/vmbackup
  ship as separate binaries; esm-* mirrors this.
- **Same lock semantics.** Both refuse to start if another process
  holds the data-dir lock.
- **Disk-format compatibility.** **Not yet bidirectional.** EsMetrics
  reads its own parts only. VM-direction round-trip lands with E6/E7
  (differential fuzzing + byte-identity tests).
- **Cross-platform.** EsMetrics ships Linux, macOS, and Windows
  binaries — VM is Linux-first.

## 8. Reporting incompatibilities

Open an issue with:

1. The exact PromQL or ingest payload that diverged.
2. VM's response.
3. EsMetrics' response.
4. The data dir state (`esm-ctl inspect`).

The conformance harness scenarios under `conformance/scenarios/` are
the right place to encode reproducers.
