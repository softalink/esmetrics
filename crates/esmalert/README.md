# esmalert

A from-scratch Rust port of upstream VictoriaMetrics
[`vmalert`](https://docs.victoriametrics.com/victoriametrics/vmalert/):
evaluates MetricsQL recording/alerting rules on a schedule, drives the
`Pending` -> `Firing` -> `Inactive` alert state machine (`for:`/
`keep_firing_for:`), pushes recording-rule results and `ALERTS`/
`ALERTS_FOR_STATE` series to remote-write, sends firing/resolved alerts to
Alertmanager, and restores a `for:` alert's progress across restarts by
reading its `ALERTS_FOR_STATE` history back from a remote-read datasource.

## Build

```bash
cargo build --release -p esmalert
```

## Usage

```yaml
# alerts.yml
groups:
  - name: example
    interval: 30s        # default: -evaluationInterval
    concurrency: 1        # rules within a group evaluated with this much parallelism
    rules:
      - alert: HighLoad
        expr: node_load1 > 4
        for: 2m
        keep_firing_for: 1m
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
  -remoteRead.lookback=1h \
  -notifier.url=http://localhost:9093 \
  -evaluationInterval=30s \
  -external.url=http://esmalert.example.com:8880 \
  -httpListenAddr=:8880
```

`-rule` is repeatable and accepts a glob. `-dryRun` loads and validates every
`-rule` file (structural checks, MetricsQL parsing, Go-template validation of
labels/annotations) and exits without starting anything or touching the
network — use it in CI to lint rule files.

### Flags

| Flag | Default | Notes |
|---|---|---|
| `-rule=<path-or-glob>` | (required) | Repeatable |
| `-datasource.url=<url>` | (required) | Queried for rule evaluation |
| `-remoteWrite.url=<url>` | unset | Recording-rule results + `ALERTS`/`ALERTS_FOR_STATE`; unset means recording rules have nowhere to push (rejected at load if any are configured) |
| `-remoteWrite.flushInterval` | `5s` | |
| `-remoteWrite.maxBatchSize` | `1000` | |
| `-remoteWrite.maxQueueSize` | `100000` | |
| `-remoteWrite.concurrency` | `1` | |
| `-remoteRead.url=<url>` | unset | Startup alert-state restore source; unset disables restore |
| `-remoteRead.lookback` | `1h` | |
| `-notifier.url=<url>` | unset | Alertmanager v2 target; repeatable; unset means alerting rules have nowhere to send (rejected at load if any are configured) |
| `-evaluationInterval` | `1m` | Per-group default (a group's own `interval:` overrides it) |
| `-external.url` | derived from `-httpListenAddr` | `generatorURL` sent to Alertmanager |
| `-configCheckInterval` | `0` (disabled) | Polling reload of `-rule`; `POST /-/reload` always works regardless |
| `-group.maxStartDelay` | `0` | Caps the deterministic per-group startup-delay spread |
| `-httpListenAddr` | `:8880` | |
| `-httpReadTimeout` | `30s` | |
| `-reload.authKey` / `-metrics.authKey` | unset (open) | Gate `POST /-/reload` / `GET /metrics` |
| `-dryRun` | `false` | Validate `-rule` and exit |
| `-disableAlertgroupLabel` | `false` | Accepted, not yet wired (see Limitations) |
| `-external.alert.source` | unset | Accepted, not yet wired (see Limitations) |

`-datasource.*`/`-remoteWrite.*`/`-remoteRead.*`/`-notifier.*` each also take
a `.basicAuth.username`/`.basicAuth.password[File]`/`.bearerToken[File]`/
`.tlsCAFile`/`.tlsCertFile`/`.tlsKeyFile`/`.tlsServerName`/
`.tlsInsecureSkipVerify` set. Run `esmalert -help` for the full text.

### HTTP API

Served on `-httpListenAddr`:

- `GET /api/v1/rules`, `GET /api/v1/alerts` — every group's rules / every
  active alert, as JSON
- `GET /api/v1/rule?...`, `GET /api/v1/alert?...`, `GET /api/v1/group?...` —
  single-object lookup by name/query param (not upstream's numeric IDs; see
  Limitations)
- `GET /api/v1/notifiers` — always an empty list (see Limitations)
- `GET /-/healthy`, `GET /metrics`, `POST /-/reload`

There is no HTML web UI (upstream vmalert's `/vmalert` pages) — only this
JSON API.

## Limitations

This port covers the evaluation/alerting/remote-write/notifier/restore/
hot-reload core exercised end to end (see `tests/e2e.rs`). The following are
deliberately out of scope or incomplete; documented here rather than left to
be discovered:

**Not implemented:**
- Replay/backfill mode (upstream `-remoteRead.url` + `vmalert -replay*`)
- Notifier service discovery (`-notifier.config`, DNS/Consul/Kubernetes SD)
  — only static `-notifier.url` targets
- A full HTML web UI — only the JSON API (`/api/v1/rules`, `/api/v1/alerts`,
  etc.) above
- oauth2 auth flag families for datasource/remote-write/remote-read/notifier
- Graphite and VictoriaLogs datasource types — Prometheus HTTP API
  (`/api/v1/query`) only

Upstream's offline rule unit-test runner (`vmalert-tool`) is a separate
crate, `esmalert-tool` — see
[crates/esmalert-tool/README.md](../esmalert-tool/README.md).

**Go-template subset:** annotations/labels are rendered with a from-scratch
Go `text/template` subset (`esm-gotemplate`) covering variable interpolation,
the full alert `FuncMap` (32 functions: `humanize*`, `reReplaceAll`,
`toTime`, `parseDuration`, `query`, ...), and simple patterns like
`{{ $value }}`, `{{ $labels.x }}`, `{{ humanize $value }}`. It does **not**
support Go's time/duration **method-call** syntax — a template using
`.Sub`/`.Add`/`.UnixMilli`/`.Unix` on a time or duration value (e.g.
`{{ (now | toTime).Sub $activeAt }}`, a common dashboard-link annotation
pattern) fails rule validation with a clear error rather than silently
misrendering.

**Config:** a negative duration (e.g. `eval_offset: -30s`) is rejected at
parse time. Upstream instead takes its absolute value; this port treats that
as a negligible, deliberate divergence rather than a compatibility gap worth
preserving.

**Inert flags:** `-external.alert.source` is parsed but not wired —
`generatorURL` sent to Alertmanager is always `-external.url` verbatim, since
`Alert` doesn't yet carry a per-alert source-link field.
`-disableAlertgroupLabel` is parsed but not wired — the `alertgroup` label is
always added.

**TLS:** `tlsServerName` (SNI override) is accepted but not applied —
`reqwest`'s blocking client has no SNI-override knob independent of the
request URL's host.

**Group-level `headers:`/`params:`/`notifier_headers:`** in a rule YAML file
are parsed but not yet applied to the datasource/notifier requests that
group issues.

**Single-object API:** `/api/v1/group?group=<name>` looks up a group by name
rather than upstream's numeric `group_id`. `/api/v1/rule?group=<name>&rule_id=<id>`
looks its group up by name, then the rule within it by `rule_id` (the rule's
stable identity hash — see `config::rule_identity_hash` — not upstream's
per-process rule ID, though the same integer shape).
`/api/v1/alert?group=<name>&alert=<alertname>` looks up both by name/string
rather than upstream's numeric `group_id`/`alert_id` pair. `/api/v1/notifiers`
always returns an empty list.

**Remote-write:** a batch that fails to send (network error or non-2xx) is
logged and dropped, not retried — the next evaluation re-pushes fresh state,
so a transient outage only loses that one tick's samples rather than
blocking or growing an unbounded retry queue.

**Alert resend:** firing/resolved alerts are re-sent to Alertmanager on
every evaluation (Alertmanager de-duplicates on its side); there is no
`resendDelay`-based client-side throttle yet.
