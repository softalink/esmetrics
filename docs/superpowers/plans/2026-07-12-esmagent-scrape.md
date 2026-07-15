# esmagent Scrape Engine (vmagent Phase 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port VictoriaMetrics vmagent's scrape engine (`lib/promscrape` core) to Rust as modules inside the `esmagent` binary: discover targets (static/file/http SD) → relabel → scrape `/metrics` on an interval → relabel samples + auto-metrics + staleness + limits → push through esmagent's existing forwarding tier.

**Architecture:** New `crates/esmagent/src/scrape/` modules (config, discovery, target, scrapework, manager, status), activated by `-promscrape.config`. A small refactor extracts `push_series` from `ForwardingSink` so scraped and pushed data share the global-relabel → `Fanout` path. Reuses `esm-relabel` (target + metric relabel), `esm-protoparser::prometheus_stream` (parse scraped exposition), the forwarding tier, esm-http, blocking reqwest. Sync stack, one scrape thread per target.

**Tech Stack:** Rust (edition 2021, rust-version 1.85), `esm-relabel`, `esm-protoparser` (prometheus parser), esmagent forwarding tier, `esm-http`, blocking `reqwest`, `esm-metricsql` (durations), `esm-common` (STALE_NAN), `serde_yaml_ng`, `serde_json` (SD).

## Porting Convention (read before every task)

Faithful port. Authoritative behavioral source per task = the cited upstream file at `/home/test/refsrc/VictoriaMetrics/lib/promscrape/` (or `lib/promrelabel/`) pinned at v1.146.0. The plan gives exact Rust interfaces, real failing tests, and the subtle semantics. When it says "port `<file>:<lines>`", read and translate faithfully. Reuse in-repo crates.

Reference existing ports: `crates/esmagent/src/{sink,fanout,rwctx,client,flags,lib}.rs` (the forwarding tier this extends — the `SeriesConsumer`/`Fanout`/`OwnedSeries` types, the flag + signal idiom), `crates/esm-relabel/src/lib.rs` (`ParsedConfigs::parse`/`apply`, `parse_relabel_configs`, `Label`), `crates/esm-protoparser/src/prometheus_stream.rs` (`parse_stream`) + `prometheus.rs` (`Row { metric: &str, tags: Vec<Tag{key:&str, value:Cow<str>}>, value: f64, timestamp: i64 }`), `crates/esmetrics/src/lib.rs` (mounting handlers on esm-http).

## Global Constraints

- Files ≤ 800 lines; extract modules when a file grows unwieldy.
- `cargo fmt` clean; `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets` clean; windows-gnu cross-check compiles. (CI's stable auto-updates — `rustup update stable` locally before pushing.)
- No tokio; sync stack (esm-http + blocking reqwest + std threads, one per target).
- Never log secrets/tokens; usernames-only in logs/metric labels.
- Faithful to upstream v1.146.0 scrape semantics (existing scrape configs for static/file/http SD work unchanged).
- Scrape modules under `crates/esmagent/src/scrape/`; no new crate.
- Never panic in a scrape worker or the manager. Commit style `<type>: <description>`, no attribution trailers.
- After push, watch GitHub Actions + fix failures (Windows tests run only in CI; file-SD watching + thread-per-target are platform-sensitive).

---

## Task 1: scrape config (parse + validate)

**Files:**
- Create: `crates/esmagent/src/scrape/mod.rs` (`pub mod config;` …), `crates/esmagent/src/scrape/config.rs`
- Modify: `crates/esmagent/src/lib.rs` (add `pub mod scrape;`)
- Test: inline in `config.rs`

**Interfaces:**
- Produces (`config.rs`):
  - `pub struct GlobalConfig { pub scrape_interval: Duration (default 60s), pub scrape_timeout: Duration (default 10s), pub external_labels: BTreeMap<String,String>, pub sample_limit: usize, pub label_limit: usize }`
  - `pub struct ScrapeConfig { pub job_name: String, pub scrape_interval: Option<Duration>, pub scrape_timeout: Option<Duration>, pub metrics_path: String (default "/metrics"), pub scheme: String (default "http"), pub honor_labels: bool, pub honor_timestamps: bool (default true), pub params: BTreeMap<String,Vec<String>>, pub relabel_configs: Vec<esm_relabel::RelabelConfig>, pub metric_relabel_configs: Vec<esm_relabel::RelabelConfig>, pub sample_limit: usize, pub label_limit: usize, pub max_scrape_size: u64, pub enable_compression: bool (default true), pub auth: crate::client::AuthConfig, pub tls: crate::client::TlsConfig, pub static_configs: Vec<StaticConfig>, pub file_sd_configs: Vec<FileSdConfig>, pub http_sd_configs: Vec<HttpSdConfig> }`
  - `pub struct StaticConfig { pub targets: Vec<String>, pub labels: BTreeMap<String,String> }`
  - `pub struct FileSdConfig { pub files: Vec<String>, pub refresh_interval: Duration (default 5m) }`
  - `pub struct HttpSdConfig { pub url: String, pub refresh_interval: Duration (default 60s), pub auth: AuthConfig, pub tls: TlsConfig }`
  - `pub struct ScrapeConfigFile { pub global: GlobalConfig, pub scrape_configs: Vec<ScrapeConfig> }`
  - `pub fn parse_scrape_config(yaml: &str) -> Result<ScrapeConfigFile, ScrapeError>` (+ `pub struct ScrapeError { pub msg: String }`), and `pub fn validate(cfg: &ScrapeConfigFile) -> Result<(), ScrapeError>`.

**Reference:** `lib/promscrape/config.go` (`Config`/`GlobalConfig`/`ScrapeConfig` yaml structs + `unmarshalMaybeStrict`/validation). Durations are Prometheus duration strings → `esm_metricsql::duration_value` (ms) → `Duration` (as in the esmagent config/esmalert). `relabel_configs`/`metric_relabel_configs` deserialize as `Vec<esm_relabel::RelabelConfig>` — reuse esm-relabel's serde (grep `esm_relabel::RelabelConfig` derives; if not directly serde-deserializable, deserialize the raw YAML nodes to strings and call `esm_relabel::parse_relabel_configs`). **Cloud-SD keys** (`kubernetes_sd_configs`, `consul_sd_configs`, …) → reject at parse with a clear "unsupported (deferred): <key>" error (do NOT silently ignore). Validation: unique `job_name`; `scheme ∈ {http,https}`; `scrape_timeout ≤ scrape_interval`; each relabel config compiles.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn parses_a_scrape_config() {
    let y = r#"
global:
  scrape_interval: 30s
  external_labels: { env: prod }
scrape_configs:
  - job_name: node
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets: ['h1:9100', 'h2:9100']
        labels: { team: infra }
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
        action: replace
"#;
    let c = parse_scrape_config(y).unwrap();
    assert_eq!(c.global.scrape_interval, Duration::from_secs(30));
    assert_eq!(c.global.external_labels["env"], "prod");
    assert_eq!(c.scrape_configs[0].job_name, "node");
    assert_eq!(c.scrape_configs[0].static_configs[0].targets, vec!["h1:9100".to_string(), "h2:9100".to_string()]);
    assert_eq!(c.scrape_configs[0].relabel_configs.len(), 1);
    validate(&c).unwrap();
}

#[test]
fn rejects_cloud_sd_and_dup_job() {
    assert!(parse_scrape_config("scrape_configs:\n  - job_name: k\n    kubernetes_sd_configs: [{}]\n").is_err());
    let dup = "scrape_configs:\n  - job_name: a\n    static_configs: [{targets: [x]}]\n  - job_name: a\n    static_configs: [{targets: [y]}]\n";
    assert!(validate(&parse_scrape_config(dup).unwrap()).is_err());
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p esmagent scrape::config` → FAIL. Deps: `serde_yaml_ng`, `serde_json` (already or add).
- [ ] **Step 3: Implement** the structs + parse + validate.
- [ ] **Step 4: Run** — PASS; `RUSTFLAGS="-D warnings" cargo clippy -p esmagent --all-targets`.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent scrape config parse + validate"`

---

## Task 2: service discovery (static / file_sd / http_sd)

**Files:**
- Create: `crates/esmagent/src/scrape/discovery.rs`
- Test: inline

**Interfaces:**
- Consumes: `config::{StaticConfig, FileSdConfig, HttpSdConfig}`.
- Produces:
  - `pub struct TargetGroup { pub targets: Vec<String>, pub labels: BTreeMap<String,String>, pub source: String }`
  - `pub fn static_target_groups(cfgs: &[StaticConfig], job: &str) -> Vec<TargetGroup>` (source = `"<job>/static/<i>"`).
  - `pub fn read_file_sd(files: &[String]) -> Result<Vec<TargetGroup>, ScrapeError>` — expand globs, read each JSON (`.json`) or YAML (`.yml`/`.yaml`) file whose content is `[{ "targets": [...], "labels": {...} }]`, attach `__meta_filepath` label, source = the file path.
  - `pub fn fetch_http_sd(cfg: &HttpSdConfig) -> Result<Vec<TargetGroup>, ScrapeError>` — GET `cfg.url` (blocking reqwest, cfg.auth/tls), parse the same JSON target-group list, attach `__meta_url`, source = the url.
  - `pub trait Discovery: Send { fn poll(&mut self) -> Vec<TargetGroup>; fn source_prefix(&self) -> &str; }` with `StaticDiscovery`, `FileSdDiscovery` (re-reads on refresh_interval; returns last-good + logs on error), `HttpSdDiscovery` (polls on refresh_interval; last-good on error). `poll()` returns the CURRENT target groups (the manager calls it; the provider tracks its own refresh timer and only re-fetches when due, returning the cached set otherwise).

**Reference:** `lib/promscrape/discovery/{file,http}/` — the file-SD format (Prometheus `file_sd`), the http-SD JSON. Prometheus file-SD JSON is a list of `{ "targets": [...], "labels": {...} }`. Use `serde_json` (+ `serde_yaml_ng` for `.yml` files).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn static_groups_carry_labels() {
    let g = static_target_groups(&[StaticConfig{ targets: vec!["h1:9100".into()], labels: [("team".to_string(),"infra".to_string())].into() }], "node");
    assert_eq!(g[0].targets, vec!["h1:9100".to_string()]);
    assert_eq!(g[0].labels["team"], "infra");
}
#[test]
fn reads_file_sd_json() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("t.json");
    std::fs::write(&p, r#"[{"targets":["h1:9100"],"labels":{"team":"infra"}}]"#).unwrap();
    let g = read_file_sd(&[p.to_string_lossy().into_owned()]).unwrap();
    assert_eq!(g[0].targets, vec!["h1:9100".to_string()]);
    assert_eq!(g[0].labels["team"], "infra");
    assert!(g[0].labels.contains_key("__meta_filepath"));
}
```

Add an http_sd test against an in-process stub server returning the JSON (mirror the stub-server pattern from `crates/esmagent/src/client.rs` tests).

- [ ] **Step 2: Run to verify it fails** — FAIL. Add `tempfile` dev-dep (already present).
- [ ] **Step 3: Implement** the providers.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent scrape service discovery (static/file/http)"`

---

## Task 3: target relabeling + scrape URL

**Files:**
- Create: `crates/esmagent/src/scrape/target.rs`
- Test: inline

**Interfaces:**
- Consumes: `config::ScrapeConfig`, `discovery::TargetGroup`, `esm_relabel::{ParsedConfigs, Label}`.
- Produces:
  - `pub struct Target { pub scrape_url: String, pub labels: Vec<Label> (public per-sample labels, __* stripped), pub discovered_labels: Vec<Label> (pre-relabel, for /targets) }`
  - `pub struct DroppedTarget { pub discovered_labels: Vec<Label> }`
  - `pub fn build_targets(sc: &ScrapeConfig, target_relabel: &ParsedConfigs, groups: &[TargetGroup]) -> (Vec<Target>, Vec<DroppedTarget>)` — for each address in each group: assemble the label set (`__address__`=address, `__scheme__`=sc.scheme, `__metrics_path__`=sc.metrics_path, `__param_<k>`=first value, `job`=sc.job_name, `instance`=address, `__scrape_interval__`/`__scrape_timeout__` if set, plus group.labels), apply `target_relabel`; if dropped → `DroppedTarget`; else compute `scrape_url` (see below), strip `__*` labels for the public set, keep the pre-relabel set as discovered_labels.
  - `pub fn get_scrape_url(labels: &[Label], extra_params: &BTreeMap<String,Vec<String>>) -> Option<String>` — port of `promrelabel.GetScrapeURL`.

**Reference:** `lib/promrelabel/scrape_url.go` `GetScrapeURL` (the exact scheme/metrics_path/address extraction incl. scheme-in-`__address__`, path-in-`__address__`, param assembly from `__param_*` + extra_params) and `lib/promscrape/config.go:1204` `getScrapeWork`/`mergeLabels` (the synthetic-label assembly + `__*` stripping + relabel).

- [ ] **Step 1: Write the failing test**

```rust
fn ls(pairs: &[(&str,&str)]) -> Vec<Label> { pairs.iter().map(|(n,v)| Label{name:n.to_string(), value:v.to_string()}).collect() }

#[test]
fn builds_scrape_url_and_labels() {
    let sc = /* ScrapeConfig job_name="node", scheme="http", metrics_path="/metrics", static targets ["h1:9100"] */;
    let rel = ParsedConfigs::parse("[]").unwrap(); // no target relabel
    let groups = static_target_groups(&sc.static_configs, "node");
    let (active, dropped) = build_targets(&sc, &rel, &groups);
    assert!(dropped.is_empty());
    assert_eq!(active[0].scrape_url, "http://h1:9100/metrics");
    assert!(active[0].labels.iter().any(|l| l.name=="job" && l.value=="node"));
    assert!(active[0].labels.iter().any(|l| l.name=="instance" && l.value=="h1:9100"));
    assert!(active[0].labels.iter().all(|l| !l.name.starts_with("__"))); // __* stripped
}
#[test]
fn get_scrape_url_extracts_scheme_and_path_from_address() {
    assert_eq!(get_scrape_url(&ls(&[("__address__","https://h1:9100/m")]), &Default::default()).as_deref(), Some("https://h1:9100/m"));
}
#[test]
fn relabel_drop_yields_dropped_target() {
    let sc = /* same, one target */;
    let rel = ParsedConfigs::parse("- source_labels: [__address__]\n  regex: 'h1:.*'\n  action: drop\n").unwrap();
    let (active, dropped) = build_targets(&sc, &rel, &static_target_groups(&sc.static_configs, "node"));
    assert!(active.is_empty());
    assert_eq!(dropped.len(), 1);
}
```

(Construct the `ScrapeConfig` via a small test helper or `parse_scrape_config`; fill in the `/* */` with a real config.)

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** `build_targets` + `get_scrape_url`.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent scrape target relabel + scrape URL"`

---

## Task 4: push_series refactor + scrape-work core

**Files:**
- Modify: `crates/esmagent/src/sink.rs` (extract `push_series`)
- Create: `crates/esmagent/src/scrape/scrapework.rs`
- Test: inline in `scrapework.rs` (stub `/metrics` server)

**Interfaces:**
- Modify `sink.rs`: extract `pub fn push_series(global_relabel: &Option<ParsedConfigs>, consumer: &Arc<dyn SeriesConsumer>, mut series: Vec<OwnedSeries>)` — applies `global_relabel` (retain_mut) then `consumer.push(&series)`. `ForwardingSink::add_rows` now calls it. (Keep behavior identical; existing esmagent tests stay green.)
- Produces (`scrapework.rs`):
  - `pub struct ScrapeConfigResolved { /* the per-target resolved config the worker needs: metric_relabel: ParsedConfigs, honor_labels, honor_timestamps, external_labels: Vec<Label>, target_labels: Vec<Label>, sample_limit, label_limit, scrape_timeout, max_scrape_size, auth, tls */ }`
  - `pub fn scrape_once(client: &reqwest::blocking::Client, scrape_url: &str, cfg: &ScrapeConfigResolved) -> ScrapeResult` where `pub struct ScrapeResult { pub series: Vec<OwnedSeries>, pub up: bool, pub samples_scraped: usize, pub samples_post_relabel: usize, pub duration: Duration, pub error: Option<String>, pub response_size: usize }`. THIS TASK: fetch → parse → metric_relabel → honor_labels merge of target_labels + external_labels → build `series`. (auto-metrics = Task 5, staleness = Task 6, limits = Task 7 — leave hooks.)
  - Fetch: GET `scrape_url` with `Accept: text/plain;version=0.0.4`, cfg.auth/tls applied, cfg.scrape_timeout, optional gzip; read body (cap at max_scrape_size). Parse via `esm_protoparser::prometheus_stream::parse_stream`. Each `Row { metric, tags, value, timestamp }` → an `OwnedSeries` with labels (`__name__`=metric + tags) → apply `metric_relabel` (drop killed) → merge target_labels (honor_labels: on name conflict, honor_labels=false means the TARGET label wins and the scraped one is renamed `exported_<name>`; honor_labels=true means the scraped label wins — read upstream) + external_labels → sample (value, timestamp per honor_timestamps).

**Reference:** `lib/promscrape/scrapework.go` `scrapeInternal`/`addRowToTimeseries`/`mergeLabels`/`honor labels` handling; `lib/promscrape/client.go` (the scrape HTTP client + Accept header + gzip + max_scrape_size). Mirror `crates/esmagent/src/client.rs` for the reqwest-blocking + auth/tls build.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn scrape_once_parses_and_merges_target_labels() {
    // stub server returns:  up_metric{code="200"} 5\nother 7\n  on /metrics
    let (addr, _stop) = start_metrics_stub("up_metric{code=\"200\"} 5\nother 7\n");
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let cfg = /* ScrapeConfigResolved: no metric_relabel, honor_labels=false, target_labels=[job=node, instance=h1], external_labels=[env=prod] */;
    let r = scrape_once(&client, &format!("http://{addr}/metrics"), &cfg);
    assert!(r.up);
    assert_eq!(r.samples_scraped, 2);
    let up = r.series.iter().find(|s| s.labels.iter().any(|l| l.name=="__name__" && l.value=="up_metric")).unwrap();
    assert!(up.labels.iter().any(|l| l.name=="job" && l.value=="node"));
    assert!(up.labels.iter().any(|l| l.name=="instance" && l.value=="h1"));
    assert!(up.labels.iter().any(|l| l.name=="env" && l.value=="prod"));
    assert_eq!(up.samples[0].value, 5.0);
}
```

Flesh out `start_metrics_stub` (a TcpListener thread serving the body on `/metrics`) and the `ScrapeConfigResolved` construction fully. Also add a scrape-failure test (stub returns 500 → `up=false`, `error` set).

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** the push_series extraction + `scrape_once` core. Run `cargo test -p esmagent` (existing tests stay green after the sink refactor).
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent scrape-once core + push_series refactor"`

---

## Task 5: scrape auto-metrics + honor_timestamps

**Files:**
- Modify: `crates/esmagent/src/scrape/scrapework.rs`
- Test: inline

**Interfaces:**
- Produces: extend `scrape_once` to append the auto-metrics to `ScrapeResult.series` after the scraped samples: `up` (1.0 if success else 0.0), `scrape_duration_seconds`, `scrape_samples_scraped`, `scrape_samples_post_metric_relabeling`, `scrape_series_added`, `scrape_timeout_seconds`, `scrape_response_size_bytes`. Each auto-metric is an `OwnedSeries` carrying the target_labels + external_labels (same identity as the target) + `__name__`, one sample at the scrape timestamp. `honor_timestamps`: when false, override every scraped sample's timestamp with the scrape time; when true, keep the exposed timestamp (0 → scrape time). `scrape_series_added` = count of series new since last scrape (needs the last-scrape series set — a `&mut` state param on `scrape_once`, or a `Scraper` struct holding the state; introduce `pub struct Scraper { last_series: HashSet<u64>, ... }` with `scrape(&mut self, ...) -> ScrapeResult` and move `scrape_once`'s body into it, so Tasks 6-7 can hang staleness/limits off the same state).

**Reference:** `lib/promscrape/scrapework.go` `addAutoMetrics`/`sw.addAutoTimeseries` (the exact auto-metric names + values) and honor_timestamps handling. `up` is emitted even on scrape failure (with the target labels).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn emits_auto_metrics_including_up() {
    let (addr, _stop) = start_metrics_stub("a 1\nb 2\n");
    let mut sw = Scraper::new(/* cfg with target_labels=[job=node,instance=h1] */);
    let r = sw.scrape(&client(), &format!("http://{addr}/metrics"));
    let names: Vec<_> = r.series.iter().filter_map(|s| s.labels.iter().find(|l| l.name=="__name__").map(|l| l.value.as_str())).collect();
    assert!(names.contains(&"up"));
    assert!(names.contains(&"scrape_samples_scraped"));
    let up = r.series.iter().find(|s| s.labels.iter().any(|l| l.name=="__name__" && l.value=="up")).unwrap();
    assert_eq!(up.samples[0].value, 1.0);
    assert!(up.labels.iter().any(|l| l.name=="job" && l.value=="node"));
    let scraped = r.series.iter().find(|s| s.labels.iter().any(|l| l.name=="__name__" && l.value=="scrape_samples_scraped")).unwrap();
    assert_eq!(scraped.samples[0].value, 2.0);
}
#[test]
fn up_is_zero_on_failure() {
    // stub returns 500 -> up=0, still emitted with target labels
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** the `Scraper` struct + auto-metrics + honor_timestamps.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent scrape auto-metrics + honor_timestamps"`

---

## Task 6: scrape staleness markers

**Files:**
- Modify: `crates/esmagent/src/scrape/scrapework.rs`
- Test: inline

**Interfaces:**
- Produces: `Scraper` tracks the set of series (by label-set hash) emitted last scrape. On each scrape, for series present last time but absent this time, append a **stale-NaN** sample (`esm_common::decimal::STALE_NAN`, value carries that exact bit pattern) at the current scrape timestamp with that series' labels; count them into `scrape_stale_samples_created_total` (a new auto-metric). Add `pub fn mark_stale_all(&mut self, ts: i64) -> Vec<OwnedSeries>` — emits stale-NaN for ALL currently-tracked series (called by the manager when a target is removed / on shutdown). A failed scrape (up=0) marks all previously-seen series stale (upstream behavior — read scrapework.go's `sw.sendStaleSeries`).

**Reference:** `lib/promscrape/scrapework.go` `sendStaleSeries`/`addStaleMarkers` + `scrape_stale_samples_created_total`. Staleness applies to the SCRAPED series (post-relabel), not the auto-metrics.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn series_gone_gets_stale_marker() {
    // scrape 1: stub returns "a 1\nb 2\n" -> Scraper sees {a,b}
    // scrape 2: stub returns "a 1\n" (b gone) -> result contains a stale-NaN sample for b
    let mut sw = Scraper::new(/* cfg */);
    let body = std::sync::Arc::new(std::sync::Mutex::new("a 1\nb 2\n".to_string()));
    let (addr, _stop) = start_dynamic_stub(body.clone());
    sw.scrape(&client(), &url(addr));
    *body.lock().unwrap() = "a 1\n".to_string();
    let r = sw.scrape(&client(), &url(addr));
    let stale = r.series.iter().find(|s| s.labels.iter().any(|l| l.name=="__name__" && l.value=="b"));
    assert!(stale.is_some());
    assert!(stale.unwrap().samples[0].value.to_bits() == esm_common::decimal::STALE_NAN.to_bits());
}
#[test]
fn mark_stale_all_emits_for_tracked_series() {
    // after a scrape sees {a,b}, mark_stale_all returns stale-NaN for both
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** staleness tracking + `mark_stale_all` + the counter.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent scrape staleness markers"`

---

## Task 7: scrape limits (sample_limit / label_limit / label-length)

**Files:**
- Modify: `crates/esmagent/src/scrape/scrapework.rs`
- Test: inline

**Interfaces:**
- Produces: enforce, inside `Scraper::scrape`, after metric_relabel and before merging target labels:
  - **sample_limit** (>0): if post-relabel sample count > sample_limit → the scrape FAILS (no scraped samples ingested; `up=0`? — read upstream: upstream keeps `up=1` but drops all samples and sets `scrape_samples_scraped` etc. and bumps `scrape_scrapes_skipped_by_sample_limit_total`; confirm and match). Emit `scrape_samples_limit` as an auto-metric.
  - **label_limit** (>0): a series with more than label_limit labels → the scrape is skipped for the whole scrape (bump `scrape_scrapes_skipped_by_label_limit_total`, `scrape_labels_limit` auto-metric) — read upstream's exact behavior.
  - **label name/value length limits** — read `scrapework.go` for the exact limits/behavior (VM has `-promscrape.maxLabelNameLen`/`maxLabelValueLen`-style handling) and port faithfully; if these are flag-gated globals rather than per-job, port the per-job ones the config exposes and note the flag-gated globals as a follow-up.

**Reference:** `lib/promscrape/scrapework.go` sample_limit + label_limit enforcement + the `scrape_scrapes_skipped_by_*_total`/`scrape_samples_limit`/`scrape_labels_limit` metrics. Read the EXACT behavior (whether over-limit keeps up=1 and drops samples, or fails the scrape) and match it — do not guess.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn sample_limit_drops_samples_and_bumps_counter() {
    // stub returns 3 samples; sample_limit=2 -> scraped samples dropped, skipped counter present
    let mut sw = Scraper::new(/* cfg with sample_limit=2 */);
    let (addr, _stop) = start_metrics_stub("a 1\nb 2\nc 3\n");
    let r = sw.scrape(&client(), &url(addr));
    // assert the scraped a/b/c are NOT in series (dropped), and scrape_scrapes_skipped_by_sample_limit_total is present
    let skipped = r.series.iter().find(|s| s.labels.iter().any(|l| l.name=="__name__" && l.value=="scrape_scrapes_skipped_by_sample_limit_total"));
    assert!(skipped.is_some());
    assert!(!r.series.iter().any(|s| s.labels.iter().any(|l| l.name=="__name__" && l.value=="a")));
}
```

(Adjust the exact assertions to the upstream behavior you read — up value, which metrics survive.)

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** the limits per upstream.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent scrape sample/label limits"`

---

## Task 8: scraper manager (reconcile + worker lifecycle + reload)

**Files:**
- Create: `crates/esmagent/src/scrape/manager.rs`
- Test: inline

**Interfaces:**
- Consumes: `config::ScrapeConfigFile`, `discovery::*`, `target::*`, `scrapework::Scraper`, `sink::{SeriesConsumer, push_series}`, `esm_relabel::ParsedConfigs`.
- Produces:
  - `pub struct ScrapeManager { /* per-job: Discovery providers + target_relabel + a map target-url -> WorkerHandle; shared push seam (global_relabel + consumer) */ }`
  - `pub struct ManagerDeps { pub global_relabel: Option<ParsedConfigs>, pub consumer: Arc<dyn SeriesConsumer> }`
  - `impl ScrapeManager { pub fn start(cfg: ScrapeConfigFile, deps: ManagerDeps) -> Result<ScrapeManager, ScrapeError>; pub fn reload(&mut self, cfg: ScrapeConfigFile) -> Result<(), ScrapeError>; pub fn targets_snapshot(&self) -> TargetsSnapshot; pub fn stop(self); }`
  - A **reconcile loop** (background thread): periodically `poll()` each job's Discovery, `build_targets`, and diff against the running workers — start a worker (thread ticking on scrape_interval, calling `Scraper::scrape` → `push_series`) per new active target; stop workers for vanished targets (calling `mark_stale_all` and pushing the stale markers before dropping); record active + dropped targets + per-target health/last-scrape into a shared `Arc<Mutex<TargetsSnapshot>>` (for status.rs).
  - `pub struct TargetsSnapshot { pub active: Vec<ActiveTarget>, pub dropped: Vec<DroppedTargetView> }` (serde-serializable for Task 9); `ActiveTarget { scrape_pool: String, scrape_url, labels, discovered_labels, health: Health, last_error, last_scrape_ms, last_scrape_duration_ms }`.
  - `reload`: re-diff jobs (add/remove/change by job_name + a config checksum); changed job → rebuild its providers + reconcile; keep old config on a bad reload (log). `stop`: stop all workers (final `mark_stale_all` flush) + reconcile thread + providers.

**Reference:** `lib/promscrape/scraper.go` (`runScrapers`/`scrapersReloader`/target reconcile) + `lib/promscrape/config.go` diffing. Worker lifecycle mirrors the forwarding-tier `Client` worker pattern (stop flag + join, no hang). Keep locks off the blocking scrape path (don't hold the snapshot lock across a scrape/HTTP call).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn reconcile_starts_and_stops_workers() {
    // build a ScrapeManager with a single static job pointing at a stub /metrics server + a capture SeriesConsumer.
    // drive one reconcile+scrape cycle; assert the capture consumer received the scraped `up` series and the target appears active/up in targets_snapshot().
    // then reload with the job removed; assert the worker stops and a stale marker was pushed.
}
```

Flesh out fully with a capture `SeriesConsumer` + a stub `/metrics` server + a test `ManagerDeps`; drive reconcile deterministically (a `pub fn reconcile_once(&mut self)` test seam so the test doesn't depend on wall-clock loop timing). No blank stubs.

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** the manager.
- [ ] **Step 4: Run** — PASS; `cargo test -p esmagent` green; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent scrape manager"`

---

## Task 9: `/api/v1/targets` JSON

**Files:**
- Create: `crates/esmagent/src/scrape/status.rs`
- Test: inline in `status.rs`

**Interfaces:**
- Consumes: `manager::{ScrapeManager, TargetsSnapshot}`.
- Produces: `pub fn targets_json(snapshot: &TargetsSnapshot, state: Option<&str>) -> String` — the Prometheus-compatible envelope `{ "status":"success", "data": { "activeTargets": [...], "droppedTargets": [...] } }` with `state=active|dropped` filtering; each active target as `{ scrapePool, scrapeUrl, labels, discoveredLabels, health, lastError, lastScrape (RFC3339), lastScrapeDuration (seconds float) }`. Serialize with `serde_json`. Task 10 wires it into the esm-http server handler (a `GET /api/v1/targets` route reading `manager.targets_snapshot()`).

**Reference:** `lib/promscrape/targetstatus.go`'s `WriteTargetsResponse`/API JSON shape (the JSON only — NOT the HTML qtpl).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn targets_json_shape_and_filter() {
    let snap = /* TargetsSnapshot with one active (up) + one dropped */;
    let j: serde_json::Value = serde_json::from_str(&targets_json(&snap, None)).unwrap();
    assert_eq!(j["status"], "success");
    assert_eq!(j["data"]["activeTargets"][0]["health"], "up");
    assert!(j["data"]["droppedTargets"].as_array().unwrap().len() == 1);
    // state filter
    let active_only: serde_json::Value = serde_json::from_str(&targets_json(&snap, Some("active"))).unwrap();
    assert_eq!(active_only["data"]["droppedTargets"].as_array().unwrap().len(), 0);
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** `targets_json`.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent /api/v1/targets JSON"`

---

## Task 10: CLI flags + main wiring

**Files:**
- Modify: `crates/esmagent/src/flags.rs`, `crates/esmagent/src/lib.rs`, `crates/esmagent/src/main.rs`
- Test: inline in `flags.rs` + a config-validation smoke test

**Interfaces:**
- Modify `Flags` (flags.rs): add `pub promscrape_config: Option<String>`, `pub promscrape_config_check_interval: Duration`, `pub promscrape_suppress_scrape_errors: bool`, `pub promscrape_max_scrape_size: u64`, `pub promscrape_config_dry_run: bool` — parse from `-promscrape.config`, `-promscrape.configCheckInterval`, `-promscrape.suppressScrapeErrors`, `-promscrape.maxScrapeSize`, `-promscrape.config.dryRun` (mirror the existing flag idiom).
- Modify `lib.rs` main wiring: after building the `ForwardingSink`/`Fanout`, if `promscrape_config` is set: parse+validate the scrape config (`-promscrape.config.dryRun` → validate + exit); else build a `ScrapeManager::start(cfg, ManagerDeps{ global_relabel, consumer: fanout })` and keep it running alongside the push server. Add a `GET /api/v1/targets` route to the esm-http handler that reads `manager.targets_snapshot()` → `targets_json`. On SIGHUP / `-promscrape.configCheckInterval`, re-read + `manager.reload`. Graceful shutdown stops the manager (final staleness flush) then the fanout.
- `run_dry` (existing) extends to also validate the scrape config when `-promscrape.config` is set.

**Reference:** `app/vmagent/main.go` (scrape wiring); the existing `crates/esmagent/src/lib.rs` server/flag/signal wiring; `crates/esmalert/src/app.rs` signal idiom.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn parses_promscrape_flags() {
    let f = parse_flags(&["-remoteWrite.url=http://a/api/v1/write","-promscrape.config=/etc/scrape.yml","-promscrape.configCheckInterval=30s"]
        .iter().map(|s|s.to_string()).collect::<Vec<_>>()).unwrap();
    assert_eq!(f.promscrape_config.as_deref(), Some("/etc/scrape.yml"));
    assert_eq!(f.promscrape_config_check_interval, Duration::from_secs(30));
}
#[test]
fn dryrun_validates_scrape_config() {
    // write a temp scrape config (valid, then one with a cloud SD / dup job) -> run_dry Ok / Err
}
```

Flesh out `dryrun_validates_scrape_config` fully (temp files). No blank stubs.

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** flags + wiring + the targets route. `cargo build -p esmagent` produces the binary.
- [ ] **Step 4: Run** — PASS; `cargo build -p esmagent`; clippy workspace clean; `cargo fmt`.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent -promscrape.config CLI and wiring"`

---

## Task 11: e2e + docs

**Files:**
- Create: `crates/esmagent/tests/scrape_e2e.rs`
- Modify: `README.md` (top-level esmagent section — add scraping), `crates/esmagent/README.md` (scrape usage + limitations), `docs/PORTING.md` (add a `lib/promscrape (core: static/file/http SD)` row)
- Test: `crates/esmagent/tests/scrape_e2e.rs`

**Interfaces:** none new — drives esmagent's scrape+forward end to end.

**Reference:** `crates/esmagent/tests/e2e.rs` (the forwarding e2e harness pattern).

- [ ] **Step 1: Write the failing test** — the full scrape→forward scenario with in-process stubs:
  1. Start a stub `/metrics` server returning a couple of series.
  2. Start a destination stub (`/api/v1/write` capture).
  3. Build esmagent in-process with a scrape config (one static job → the `/metrics` stub) + one `-remoteWrite.url` → the destination stub, via `esmagent::run` (or the manager + fanout directly if the full binary path is awkward in-process — prefer the fuller path).
  4. Assert the destination stub receives the scraped series (snappy+protobuf decode) including `up=1` and the target/relabeled labels; bounded polling, no sleep-only.
  5. `GET /api/v1/targets` → the target is `up`.
  6. Stop/kill the `/metrics` stub; after the next scrape assert `up=0` is forwarded and a stale marker for the previously-seen series is forwarded to the destination.
- [ ] **Step 2: Run to verify it fails** — `cargo test -p esmagent --test scrape_e2e` → FAIL.
- [ ] **Step 3: Implement** any wiring gaps + the docs (honest: scrape engine covers static/file/http SD; cloud SD + HTML /targets deferred; the forwarding tier + esm-relabel are reused). Add the PORTING.md row.
- [ ] **Step 4: Run** — `cargo test -p esmagent --test scrape_e2e` PASS; full-workspace `cargo test --workspace`, `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets`, `cargo fmt --check`, windows-gnu check.
- [ ] **Step 5: Commit** — `git commit -m "test: esmagent scrape e2e; docs: scrape usage"`

---

## Final verification (after Task 11, before merge)

- [ ] `cargo test --workspace` green on Linux; push and confirm Windows CI green (file-SD watching + thread-per-target run on Windows only in CI — watch it).
- [ ] windows-gnu cross-compile check passes.
- [ ] Whole-branch code review (subagent-driven final review on the most capable model) — focus: no scrape-worker/manager panics; per-target failure isolation (one down target never blocks others or the manager); staleness correctness (present→absent and target-removal both emit stale-NaN, no false staleness); honor_labels/honor_timestamps + limits match upstream; the `push_series` refactor didn't change forwarding behavior; no credential logging; default-flag-value paths (like the forwarding tier's maxDiskUsage=0 trap) checked for data-loss/degradation.
- [ ] No esmetrics ingest/query hot-path impact (esmagent is a separate binary; esm-relabel/esm-protoparser consumed not modified) → no benchmark re-validation.
- [ ] Update memory: extend the `esmagent-forwarding` note (or add `esmagent-scrape`) + MEMORY.md; note the vmagent port is now scrape+forward (cloud SD still deferred).
