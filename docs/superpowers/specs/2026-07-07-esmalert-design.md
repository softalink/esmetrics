# esmalert (vmalert port) — Design Spec

**Status:** Approved design, pending implementation plan
**Date:** 2026-07-07
**Upstream:** VictoriaMetrics `app/vmalert` @ v1.146.0 (see `UPSTREAM`)
**Precedent:** follows the esmauth port (`docs/superpowers/specs/2026-07-06-esmauth-design.md`) — sync stack, standalone binary, subagent-driven execution.

## Goal

Port VictoriaMetrics `vmalert` to Rust as a standalone `esmalert` daemon that evaluates **alerting and recording rules** against esmetrics, sends alerts to Alertmanager, and **persists alert state** across restarts — running existing vmalert rule files unchanged.

## Scope

**In scope**
- YAML rule-group config (alerting + recording rules) with faithful validation and hot-reload.
- Rule engine: alerting-rule state machine (`for`, `keep_firing_for`, resolvedRetention) + recording rules (limit, dedup).
- A faithful Go-`text/template` **subset** engine for annotation/label templating, with vmalert's 32-function FuncMap.
- Datasource read path (Prometheus-type instant/range queries) against esmetrics.
- Notifier: Alertmanager HTTP push (static config).
- State persistence: remote-write of `ALERTS`/`ALERTS_FOR_STATE`/recording results; startup restore of `for:` progress via an instant query.
- Minimal read-only JSON API (`/api/v1/rules`, `/api/v1/alerts`, and cheap single-object lookups), `/metrics`, `/-/healthy`, `/-/reload`.
- Core operational + auth/TLS CLI flags.

**Deferred (explicitly out, additive later)**
- `vmalert-tool` (offline rule unit-tester) — separate follow-up project; reuses this engine.
- Replay/backfill mode.
- Notifier service discovery (`-notifier.config`).
- Full HTML web UI (`web.qtpl`) — JSON API only.
- OAuth2 auth flag families; Graphite / VictoriaLogs datasource types; niche tuning knobs (`datasource.roundDigits`, `showURL`, etc.).

## Architecture

esmalert is a **separate process** that talks HTTP to esmetrics as its datasource and remote-write target — exactly as vmalert talks to a Prometheus-compatible backend. No in-process coupling to the TSDB. Stack: hand-rolled `esm-http` server + blocking `reqwest` client, **no tokio** (the proven Option-1 stack from esmauth).

### Crate layout

Two new workspace crates + two small shared-crate touches.

**`crates/esm-gotemplate`** (new lib) — Go-`text/template` subset engine, zero vmalert knowledge.
`lexer` → `parser` (AST) → `exec` (executor) + `funcs` (the 32-function FuncMap). IO-free: the datasource-backed functions are injected via a `QueryFn` callback so the engine is unit-testable without a server.

**`crates/esmalert`** (new bin) — the daemon, one crate, modules mirroring upstream:
- `config/` — YAML parse + validation + per-group checksum (hot-reload diffing)
- `rule/` — `AlertingRule`, `RecordingRule`, `Group` loop, concurrency executor
- `datasource/` — blocking `reqwest` client → esmetrics `/api/v1/query{,_range}`
- `notifier/` — blocking `reqwest` Alertmanager client (static config)
- `remotewrite/` — buffered prompb push (background flush thread)
- `remoteread/` — startup state restore (instant query wiring)
- `web/` — esm-http handlers (JSON API, metrics, health, reload)
- `manager.rs` + `main.rs` — group lifecycle, hot-reload, CLI

**Shared-crate touches**
- `esm-protoparser` — add the prompb **WriteRequest encode** side (protobuf + snappy via `snap`); currently decode-only. The one substantive shared addition.
- `esm-common` — reuse existing `metrics` (esm_ counters) and HTTP helpers; no new abstractions unless a concrete need appears.
- Reuse `esm-metricsql` for rule `expr` validation (expressions are MetricsQL, already ported).

## Component: `esm-gotemplate`

**Pipeline:** `lexer → parser → AST → executor`, faithful to Go `text/template`, scoped to the subset alerting rules use.

Annotation/label rendering goes through `text/template` (upstream's `current`/`replacement` are `*textTpl.Template`); `html/template` appears only for the `safeHtml` return type and a nil-data validation pass. Therefore **Go's contextual HTML auto-escaping engine is NOT needed** — only the functions.

**Lexer** — text vs. `{{ }}` action delimiters, including whitespace-trim markers `{{-` / `-}}`.

**Parser** — AST of the subset:
- Text nodes; `{{ pipeline }}` interpolation.
- Actions: `if`/`else`/`end`, `range`/`else`/`end`, `with`/`else`/`end`.
- Pipelines: command chains joined by `|`; each command a function call or field/var access.
- Field access `.Value`, `.Labels.foo`, chained `.A.B`; variable decl/use `$x := …`, `$x`; special `$` (root) and `.` (cursor).
- Literals: string (interpreted + raw backtick), number, bool, `nil`.
- Parenthesized sub-pipelines for argument grouping.

**Executor** — walks AST against a data value:
- `enum Value { Nil, Bool, Int, Float, Str, Metric, Vec(Vec<Metric>), Map(...), List(...) }`.
- Annotation data is upstream's `{ Value, Labels(map), Expr, AlertID, GroupID, ActiveAt, For, … }` modeled as a `Map`.
- **`missingkey=zero`**: a missing map key renders as the zero value, never an error (matches upstream `Option("missingkey=zero")`).
- Dot re-binding in `range`/`with`; `$var` lexical scoping with save/restore on block entry/exit; whitespace trimming for `{{- -}}`.
- `range` over vectors (common case: `range query "…"`), maps, slices.

**FuncMap (32 functions)** — verified against `templates/template.go` (`templateFuncs()` + `FuncsWithQuery()`), deduplicated.
- *String/format (14):* `toUpper`, `toLower`, `title`, `crlfEscape`, `quotesEscape`, `jsonEscape`, `htmlEscape`, `stripPort`, `stripDomain`, `match`, `reReplaceAll` (Rust `regex`, RE2-compatible), `pathEscape`, `queryEscape`, `safeHtml`.
- *Humanize/time (9):* `humanize`, `humanizeDuration`, `humanizePercentage`, `humanizeTimestamp`, `toTime`, `formatTime`, `parseDuration`, `parseDurationTime`, `now` — `%.4g` paths reuse the repo's existing Go-`'g'`-format port.
- *Query/vector (6):* `query`, `first`, `label`, `value`, `strvalue`, `sortByLabel` — operate on `Metric`/`Vec`.
- *Context (3):* `externalURL`, `pathPrefix`, `args`.
- `query` and the context funcs injected via `QueryFn` + `EvalContext` at execute time; parse-time validation uses upstream's stub `query` (returns one empty metric) so chained-function validation passes.

**Public API**
```rust
let tmpl = Template::parse(text)?;               // parse once, reuse
let out  = tmpl.render(&data, &funcs, &ctx)?;    // per-evaluation render
```

## Component: config (`esmalert`)

Parses YAML files/globs into `Config { groups: Vec<Group> }`, field-for-field with upstream.

- **Group:** `name` (required), `type`, `interval`, `eval_offset`, `eval_delay`, `limit`, `concurrency`, `labels`, `params`, `headers`, `notifier_headers`, `eval_alignment`, `debug`, `rules`.
- **Rule:** `record` **xor** `alert`, `expr` (required), `for`, `keep_firing_for`, `labels`, `annotations`, `debug`.
- **Validation (ported 1:1):** group name set; `interval ≥ 0`; `|eval_offset| < interval`; `eval_offset` not combined with `eval_delay`; `limit ≥ 0`; `concurrency ≥ 0`; duplicate-rule detection within a group; `record`-xor-`alert`; non-empty `expr`; reject `__name__` as a rule label; **strict YAML** (unknown fields rejected).
- **Expr validation** via `esm-metricsql`.
- **Template validation:** each annotation/label parsed through `esm-gotemplate` at load time (stub `query`).
- **Checksum** per group (stable serialization → hash) drives hot-reload diffing.

## Component: rule engine (`esmalert`)

- **`RecordingRule::exec(ts)`** → datasource `Query(ts)` → enforce `limit` (error if series > limit) → dedup (duplicate label set = error) → attach group labels → `Vec<prompb::TimeSeries>` for remote-write.
- **`AlertingRule`** holds `alerts: HashMap<hash, Alert>`; per-eval faithful state machine:
  - sample present, Inactive-past-retention or new → **Pending**, `ActiveAt = ts`, reset `KeepFiringSince`.
  - Pending and `ts − ActiveAt ≥ for` → **Firing**.
  - sample absent → Pending drops; Firing → **Inactive** unless `keep_firing_for` still holds (`KeepFiringSince`).
  - Inactive alerts retained `resolvedRetention`, then evicted.
  - Annotations/labels rendered per-eval via `esm-gotemplate`; produces `notifier::Alert`s + `ALERTS`/`ALERTS_FOR_STATE` series.
- **`Group`** — interval loop: random start delay (`-group.maxStartDelay`) to spread evals; `eval_offset`/`eval_delay`/`eval_alignment` timestamp adjustment; per-group **concurrency** executor; `restore()` on first start; hot-reload update channel swapping rules while preserving live alert state.
- **Executor:** `exec_concurrently(rules, ts, concurrency)` — bounded worker pool (thread-based, sync stack); `concurrency = 1` runs sequentially.

## Component: datasource / notifier / state persistence

**Clarification:** vmalert "remote-read" for restore is **not** the Prometheus remote-read protobuf protocol — it is an ordinary instant query for `ALERTS_FOR_STATE`. Only the prompb **WriteRequest encode** side is needed; no ReadRequest/ReadResponse decoder.

**`datasource/` (read path, Prometheus-type only):**
- `Query(expr, ts)` → GET `<url>/api/v1/query` with `query`, `time` (RFC3339), `step`; `QueryRange(expr, from, to)` → `/api/v1/query_range` with `start`/`end`/`step`.
- Parse Prometheus JSON envelope (`vector`/`matrix`/`scalar`) → `Result { data: Vec<Metric> }`, `Metric { labels, timestamps, values }`.
- Per-group `params`, `headers`, auth (basic/bearer). Blocking `reqwest`.

**`notifier/` (Alertmanager push):**
- Static list of AM base URLs, optional `PathPrefix`, `notifier_headers`, per-target timeout, auth. **No SD.**
- `Send(alerts)` → POST `<url>[/prefix]/api/v2/alerts`, JSON array of `{ labels, annotations, startsAt, endsAt, generatorURL }`. Fan-out; firing re-sent every `resendDelay`; resolved sent with `endsAt`.

**`remotewrite/` (state & recording write path):**
- `Client`: bounded in-memory queue + background flush thread. `Push(TimeSeries)` non-blocking, drops with warning when full (`maxQueueSize`); flusher batches to `maxBatchSize` on `flushInterval` ticker with per-instance jitter.
- Each batch → `prompb::WriteRequest` → **snappy** → POST `<url>/api/v1/write`. Requires the new `esm-protoparser` encoder.

**`remoteread/` (state restore):**
- On a group's first start, each alerting rule issues `last_over_time(ALERTS_FOR_STATE{alertname="…",alertgroup="…",<labels>}[lookback])` at `ts − 1s` via a datasource client pointed at `-remoteRead.url`. Returned value = original `ActiveAt` unix ts, restoring `for:` progress. Thin wiring; no new protocol.

**Faithfulness:** `ALERTS`/`ALERTS_FOR_STATE` names, `alertname`/`alertgroup`/`alertstate` labels, and the `ts − 1s` restore offset preserved exactly — state stays compatible with real vmalert/VictoriaMetrics.

## Component: web JSON API (`esmalert`)

Faithful subset, both bare and `/vmalert`-prefixed paths:
- **`GET /api/v1/rules`** — all groups + rules (id, name, expr, for, labels, annotations, state, health, lastError, last-eval samples).
- **`GET /api/v1/alerts`** — active alerts (state, activeAt, labels, annotations, value).
- **`GET /api/v1/rule|alert|group|notifiers`** — single-object lookups (same data model).
- **`GET /metrics`** (esm_ counters), **`GET /-/healthy`**, **`POST /-/reload`**.
- Deferred: HTML web UI.
- **Security (from esmauth):** `/-/reload` and `/metrics` gated by optional `-reload.authKey` / `-metrics.authKey`; read timeout on the esm-http server; never log datasource/remote-write credentials.

## CLI & lifecycle (`main.rs` + `manager.rs`)

**Manager** owns the group set: parse config → build groups → start each group's loop thread → on `-configCheckInterval` or `/-/reload`, re-parse, diff by checksum, hot-swap changed groups preserving live alert state (faithful `updateWith`).

**Flag scope (honest cut — full upstream surface is large):**
- **In (core operational):** `-rule` (globs), `-datasource.url`, `-remoteWrite.url`, `-remoteRead.url`, `-notifier.url`, `-evaluationInterval`, `-remoteRead.lookback`, `-remoteWrite.{flushInterval,maxBatchSize,maxQueueSize,concurrency}`, `-external.url`, `-external.alert.source`, `-configCheckInterval`, `-group.maxStartDelay`, `-httpListenAddr`, `-dryRun`, `-disableAlertgroupLabel`.
- **In (auth/TLS):** basicAuth (user/pass + `*File`), bearerToken (+`File`), TLS (`caFile`/`certFile`/`keyFile`/`serverName`/`insecureSkipVerify`) for datasource, remoteWrite, remoteRead.
- **Deferred (documented):** oauth2 families; Graphite/vlogs datasource types; `-notifier.config` SD; replay flags; `datasource.roundDigits`/`showURL` and niche knobs.

## Error handling

- Config load: fail fast with file+group+rule context (upstream wrapping preserved); `-dryRun` validates all rules (incl. template parse), exits non-zero on any error.
- Eval errors **per-rule, non-fatal**: recorded in rule `health`/`lastError`, surfaced via `/api/v1/rules` + esm_ error counters; loop continues. Datasource/notifier/remote-write transport errors logged + counted, never panic the loop.
- Template render errors on annotations degrade gracefully: alert still fires; failed annotation carries the error text.

## Testing strategy

- **`esm-gotemplate`:** exhaustive unit suite (highest-risk crate) — each of 32 funcs + executor semantics (missingkey=zero, `$var` scope, whitespace trim, nested range/with, pipelines); Rust output diffed against captured Go `text/template` output where practical.
- **config:** parse/validate table tests from upstream `config_test.go` fixtures (valid + every rejection path); strict-unknown-field cases.
- **rule:** state-machine tests (Inactive↔Pending↔Firing, `for`, `keep_firing_for`, resolvedRetention) with a **mock datasource** returning scripted results at successive timestamps (ported from `alerting_test.go`/`group_test.go`); recording-rule limit/dedup.
- **datasource/notifier/remotewrite:** stub in-process esm-http server asserting exact paths, params, JSON/protobuf bodies, snappy framing; remote-write encode round-trips through the new `esm-protoparser` encoder.
- **e2e:** one integration test — esmalert vs. a real esmetrics instance: load a rule file, drive a threshold-crossing series, assert an alert reaches a stub Alertmanager and `ALERTS_FOR_STATE` is written; restart, assert `for:` restores. Mirrors esmauth e2e.
- **Coverage** 80%+ (repo rule); **both-platform** validation (Linux + Windows CI); post-merge benchmark spot-check confirming the esmetrics ingest path is untouched (separate process → no hot-path regression expected, but verified per convention).

## Global constraints

- Files ≤ 800 lines; extract modules when a file grows unwieldy.
- `cargo fmt` clean; `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets` clean; windows-gnu cross-check compiles.
- No tokio; sync stack (esm-http server + blocking reqwest).
- Never log secrets/tokens; usernames-only in logs/metric labels.
- Faithful to upstream v1.146.0 semantics; metric/label names and wire formats preserved for compatibility.
- Commit style `<type>: <description>`, no attribution trailers.
- After push, watch the GitHub Actions run and fix failures (Windows tests run only in CI).
