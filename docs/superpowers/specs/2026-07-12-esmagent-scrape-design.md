# esmagent Scrape Engine (vmagent Phase 2) — Design Spec

**Status:** Approved design, pending implementation plan
**Date:** 2026-07-12
**Upstream:** VictoriaMetrics `lib/promscrape` (core, excl. cloud discovery) @ v1.146.0 (see `UPSTREAM`)
**Depends on:** the esmagent forwarding tier + `esm-relabel` (`docs/superpowers/specs/2026-07-11-esmagent-forwarding-design.md`).

## Goal

Port VictoriaMetrics vmagent's **scrape engine** (`lib/promscrape`) to Rust as a set of modules inside the existing `esmagent` binary: discover targets (static / file / http SD), scrape their `/metrics` on an interval, relabel, and push the samples through esmagent's existing forwarding tier — so `esmagent -promscrape.config <file>` behaves like vmagent's scrape+forward.

## Scope

**In scope**
- Scrape config YAML (`global` + `scrape_configs[]`), parse + validate, faithful to `lib/promscrape/config.go`.
- Service discovery: `static_configs`, `file_sd_configs` (watch + reload), `http_sd_configs` (poll).
- Target relabeling (via `esm-relabel`) + scrape-URL computation + dropped-target tracking.
- Per-target scrape loop — **full faithful behavior**: fetch → parse (Prometheus exposition) → `metric_relabel_configs` → honor_labels/honor_timestamps → `external_labels` → the standard auto-metrics → **staleness markers** → sample_limit/label_limit/label-length limits → push.
- Scraper manager: reconcile targets from SD, start/stop per-target workers, hot-reload on config change / SIGHUP.
- `/api/v1/targets` JSON (active + dropped targets, health, last scrape time/duration/error).
- New CLI flags on the esmagent binary; scraped + pushed data share the same forwarding pipeline.

**Out of scope (deferred / documented)**
- Cloud service discovery (kubernetes, consul, ec2, gce, azure, docker, dns, …).
- The HTML `/targets` status page (`targetstatus.qtpl`) — JSON API only.
- Relabel-debug endpoints and `scrape_config_files` (external scrape-config includes) — deferred; the config is a single `-promscrape.config` file for now.

## Architecture

The scrape engine is a **module set inside `crates/esmagent`** (not a new binary), activated by `-promscrape.config`. Real vmagent is one binary that scrapes and/or receives-and-forwards; esmagent becomes the same. Scraped samples flow into the **same forwarding pipeline** the push protocols feed. Sync stack, no tokio; one scrape worker thread per active target.

### Module layout (`crates/esmagent/src/scrape/`)
- `config.rs` — scrape-config YAML (`GlobalConfig` + `ScrapeConfig[]`), parse + validate.
- `discovery.rs` — SD providers (`static`, `file_sd`, `http_sd`) yielding `TargetGroup`s.
- `target.rs` — target relabeling (esm-relabel) + scrape-URL computation; dropped-target handling.
- `scrapework.rs` — the per-target scrape loop (fetch → parse → relabel → auto-metrics → staleness → limits → push).
- `manager.rs` — reconcile the target set, start/stop workers, reload.
- `status.rs` — `/api/v1/targets` JSON.

### Integration seam (small refactor to esmagent)
Today `ForwardingSink` does `decode rows → global relabel → Fanout`. Extract a shared `push_series(series: &[OwnedSeries])` that applies the global remote-write relabel (`-remoteWrite.relabelConfig`) then dispatches to `Fanout` — called by **both** `ForwardingSink` (after decode) and the scrape loop (after its own relabel + auto-metrics). Mirrors vmagent, where scraped and pushed data both go through `remotewrite.Push`.

### Reuse map
- `esm-relabel` — target relabel (`relabel_configs`) + sample relabel (`metric_relabel_configs`) + a `GetScrapeURL`-equivalent helper.
- `esm-protoparser::prometheus_stream` — parse the scraped exposition text (the same parser esm-insert uses).
- esmagent `Fanout` / `PersistentQueue` / `Client` (the whole forwarding tier) — delivery.
- esm-http server — host `/api/v1/targets`.
- blocking `reqwest` + esmagent's local `AuthConfig`/`TlsConfig` — scrape + http_sd HTTP with per-job auth/TLS/timeout.
- `esm_common::decimal::STALE_NAN` — staleness markers.

### Data flow
`SD → TargetGroups → target relabel → scrape URL → [per-target loop: GET /metrics → parse → metric_relabel → honor_labels + external_labels + limits → auto-metrics → staleness] → push_series → global relabel → Fanout → per-destination queue → remote-write`.

## Component: scrape config (`config.rs`)

Faithful to `lib/promscrape/config.go`:
- `GlobalConfig { scrape_interval (default 60s), scrape_timeout (default 10s), external_labels: BTreeMap<String,String>, sample_limit, label_limit }`.
- `ScrapeConfig { job_name, scrape_interval?, scrape_timeout?, metrics_path (default "/metrics"), scheme (default "http"), honor_labels, honor_timestamps, params: BTreeMap<String,Vec<String>>, relabel_configs: Vec<RelabelConfig>, metric_relabel_configs: Vec<RelabelConfig>, sample_limit, label_limit, max_scrape_size, enable_compression, auth (basic/bearer/authorization + tls, inline), static_configs, file_sd_configs, http_sd_configs }`.
- `relabel_configs`/`metric_relabel_configs` parse via `esm_relabel::parse_relabel_configs` (same YAML). Per-job auth/TLS reuse esmagent's local types. Durations via `esm_metricsql::duration_value` (as elsewhere).
- Validation: unique job names; valid scheme (`http`/`https`); `scrape_timeout ≤ scrape_interval`; relabel/regex compile at load (fail fast). Unknown cloud-SD keys are rejected with a clear "deferred / unsupported" error (not silently ignored).

## Component: service discovery (`discovery.rs`)

A `Discovery` trait; each provider yields `Vec<TargetGroup { targets: Vec<String>, labels: BTreeMap<String,String>, source: String }>`:
- **`static_configs`** — inline `[{ targets: [host:port,…], labels: {…} }]`; fixed at config load.
- **`file_sd_configs`** — `[{ files: [glob…], refresh_interval (default 5m) }]`: read JSON/YAML target files (Prometheus file-SD format: a list of `{ targets, labels }`), attach `__meta_filepath`; **watch** and re-read on `refresh_interval`, picking up add/remove/modify.
- **`http_sd_configs`** — `[{ url, refresh_interval (default 60s), auth/tls }]`: poll the URL; the JSON body is the same target-group list; attach `__meta_url`; blocking reqwest with the provider's auth/TLS.

Each provider runs on its own cadence and feeds the manager a current target-group set per job. Meta labels (`__meta_*`) flow into target relabeling.

## Component: target relabeling + scrape URL (`target.rs`)

Per discovered target (`__address__` + group/meta labels): merge the job's configured labels + synthetic labels (`__scheme__`, `__metrics_path__`, `__param_<k>`, `__scrape_interval__`, `__scrape_timeout__`, `job`, `instance=__address__`), apply the job's `relabel_configs` via `esm-relabel`. Relabeled-away (empty labels) → **dropped target** (recorded for `/api/v1/targets`, not scraped). Else compute the scrape URL (`<__scheme__>://<__address__><__metrics_path__>?<params>`), read `__scrape_interval__`/`__scrape_timeout__` overrides, strip `__*` labels (they don't reach samples). The surviving public label set = target identity + per-sample labels.

## Component: scrape loop (`scrapework.rs`)

One worker per active target, ticking on the target's `scrape_interval` (per-target phase offset to spread load, like upstream). Each tick:
1. GET the scrape URL (per-job auth/TLS, `scrape_timeout`, `Accept: text/plain;version=0.0.4`, optional gzip, `max_scrape_size` cap).
2. Parse the body via `esm-protoparser`'s Prometheus exposition parser.
3. Apply `metric_relabel_configs` (esm-relabel) per sample; drop killed samples.
4. Attach target labels with **honor_labels** semantics (on conflict keep scraped vs target per the flag) + `external_labels`; apply **honor_timestamps**.
5. Enforce **`sample_limit`** / **`label_limit`** / label name+value length limits — over-limit → the scrape fails (no samples ingested for this scrape), bumping the skipped-by-limit counters.
6. Emit **auto-metrics**: `up` (1/0), `scrape_duration_seconds`, `scrape_samples_scraped`, `scrape_samples_post_metric_relabeling`, `scrape_series_added`, `scrape_timeout_seconds`, `scrape_response_size_bytes`, plus the limit/failure counters.
7. **Staleness**: track series seen last scrape; series present-then-absent (or on target-down / target-removal) → emit a **stale-NaN** sample (`esm_common::decimal::STALE_NAN`), bumping `scrape_stale_samples_created_total`.
8. Push the resulting `OwnedSeries` batch through `push_series` → global relabel → `Fanout`.

Each worker is a self-contained unit (target + client + relabel + staleness state), independently testable.

## Component: manager (`manager.rs`)

Owns the scrape set: start SD providers, receive target-group updates, compute per-job target sets (via `target.rs`), and **reconcile** — start a worker per new active target, stop workers for vanished targets (emitting their staleness markers on stop), leave unchanged ones running. On `-promscrape.configCheckInterval` or SIGHUP, re-parse + reconcile (job add/remove/change; keep old config on a bad reload, log). Records active + dropped targets (health, last scrape time/duration/error) for `status.rs`. Graceful shutdown stops all workers (final staleness flush) + SD providers.

## Component: `/api/v1/targets` (`status.rs`)

Prometheus-compatible JSON on esmagent's esm-http server:
`{ "status":"success", "data": { "activeTargets": [ { scrapePool, scrapeUrl, labels, discoveredLabels, health, lastError, lastScrape, lastScrapeDuration } ], "droppedTargets": [ { discoveredLabels } ] } }`, with `?state=active|dropped` filtering. Sourced from the manager's per-target status + dropped-by-relabel targets.

## CLI & config

New flags on the `esmagent` binary (mirror the existing idiom): `-promscrape.config <file>` (activates the scrape manager), `-promscrape.configCheckInterval` (default 0 = off; SIGHUP always reloads), `-promscrape.suppressScrapeErrors`, `-promscrape.maxScrapeSize` (default), `-promscrape.config.dryRun` (parse+validate scrape config incl. all relabel, exit 0/nonzero). Three shapes, all faithful to vmagent: scrape-only, push-only (existing), or both. `≥1 -remoteWrite.url` still required (scraped data must go somewhere); at least one of `{-promscrape.config, a push listener}` active.

## Error handling

- A single target's scrape failure (connect/timeout/parse/non-2xx) → that target `up=0` + `lastError`, never affecting other targets or the manager; retries next interval. `-promscrape.suppressScrapeErrors` mutes logs.
- Config/relabel parse errors at load → fail fast; on reload → keep old config running + log.
- SD provider errors (file unreadable, http_sd down) → log + retain last-good target set for that provider; never crash the manager.
- Never panic in a scrape worker or the manager. Never log credentials.

## Testing

- **SD unit**: static parse; file_sd (write files → groups, modify → reload); http_sd (stub endpoint → groups). Deterministic, bounded polling.
- **target.rs unit**: relabel drop/keep + scrape-URL from `__scheme__`/`__address__`/`__metrics_path__`/`__param_*`; instance/job labels; interval override.
- **scrapework unit**: against a stub `/metrics` server — parse + metric_relabel + honor_labels + external_labels + auto-metric values (`up=1`, `scrape_samples_scraped`) + staleness (present→absent → stale-NaN) + sample_limit (over-limit → scrape fails, `up` still emitted). Capture `SeriesConsumer` asserts the pushed output.
- **manager unit**: reconcile (SD add/remove → worker start/stop, vanished target staleness); reload swaps a job.
- **e2e**: esmagent with `-promscrape.config` (one static job → stub `/metrics` server) + a destination stub → scraped series (with `up` + relabeled labels) delivered through the forwarding tier; `/api/v1/targets` reports `up`; kill the target → `up=0` + staleness marker forwarded.
- Both-platform (Linux + Windows CI — file-SD watching + thread-per-target are platform-sensitive); 80%+ coverage.

## Global constraints

- Files ≤ 800 lines; extract modules when a file grows unwieldy.
- `cargo fmt` clean; `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets` clean; windows-gnu cross-check compiles. (CI's stable toolchain auto-updates — `rustup update stable` locally before pushing to catch new deny-by-default lints.)
- No tokio; sync stack (esm-http server + blocking reqwest + std threads, one per target).
- Never log secrets/tokens; usernames-only in logs/metric labels.
- Faithful to upstream v1.146.0 scrape semantics (existing Prometheus/vmagent scrape configs work unchanged for the in-scope SD types).
- Scrape modules added under `crates/esmagent/src/scrape/`; no new crate.
- Commit style `<type>: <description>`, no attribution trailers.
- After push, watch the GitHub Actions run and fix failures (Windows tests run only in CI).
