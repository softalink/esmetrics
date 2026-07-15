# esmagent (vmagent forwarding tier + esm-relabel) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port VictoriaMetrics vmagent's remote-write **forwarding tier** to a standalone Rust `esmagent` binary (receive via all push protocols → relabel → per-destination durable queue → remote-write to N backends), plus a reusable `esm-relabel` promrelabel engine.

**Architecture:** Two new crates — `esm-relabel` (the 20-action relabel engine, Phase 0) and `esmagent` (the forwarding binary, Phase 1). `esmagent` reuses `esm-insert`'s protocol router by swapping its `RowSink` for a `ForwardingSink`; the sink decodes `metric_name_raw`, applies global relabel, fans out to N `RemoteWriteCtx` (per-URL relabel → pendingseries batching → persistent disk queue → remote-write client with retry). Sync stack, no tokio. The scrape engine is a separate Phase 2.

**Tech Stack:** Rust (edition 2021, rust-version 1.85), `esm-relabel` (new), `esm-insert` (router + `RowSink`), `esm-storage` (`MetricName` decode), `esm-protoparser::prompb_encode` (block encode), `esm-metricsql` (`if:` selector), `esm-http` + blocking `reqwest`, `regex`, `serde_yaml_ng`.

## Porting Convention (read before every task)

Faithful port. Authoritative behavioral source per task = the cited upstream file at `/home/test/refsrc/VictoriaMetrics/` (pinned v1.146.0). The plan gives exact Rust interfaces, real failing tests, and the subtle semantics. When it says "port `<file>:<lines>`", read and translate faithfully. Reuse in-repo crates.

Reference existing ports: `crates/esmalert/src/remotewrite/client.rs` (queue + flush thread + snappy POST + send timeout — the closest analog to esmagent's client), `crates/esmalert/src/datasource/{auth,client}.rs` (`AuthConfig`/`TlsConfig` + blocking reqwest build), `crates/esmetrics/src/wiring.rs` (`StorageSink impl esm_insert::RowSink` — the seam esmagent mirrors), `crates/esmalert/src/flags.rs` + `crates/esmalert/src/app.rs` (CLI + signal/shutdown idiom), `crates/esm-protoparser/src/prompb_encode.rs` (`encode_and_compress`).

## Global Constraints

- Files ≤ 800 lines; extract modules when a file grows unwieldy.
- `cargo fmt` clean; `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets` clean; windows-gnu cross-check compiles. (CI's stable toolchain auto-updates — run `rustup update stable` locally to match it before pushing so new deny-by-default clippy lints don't surprise CI.)
- No tokio. Sync stack: `esm-http` server + blocking `reqwest` + std threads.
- Never log secrets/tokens; usernames-only in logs/metric labels.
- Faithful to upstream v1.146.0: remote-write wire format + relabel behavior preserved (existing vmagent relabel configs work unchanged).
- New workspace members `crates/esm-relabel` + `crates/esmagent` added to root `Cargo.toml` `members` (+ `esm-relabel` to `[workspace.dependencies]`).
- Never panic in a worker/forwarding loop. Commit style `<type>: <description>`, no attribution trailers.
- After push, watch the GitHub Actions run and fix failures (Windows tests run only in CI; the disk-queue file handling is platform-sensitive).

---

## Task 1: `esm-relabel` scaffold + config parse + regex

**Files:**
- Create: `crates/esm-relabel/Cargo.toml`, `crates/esm-relabel/src/lib.rs`, `crates/esm-relabel/src/config.rs`, `crates/esm-relabel/src/regex.rs`
- Modify: root `Cargo.toml` (`members`, `[workspace.dependencies]`)
- Test: inline in `config.rs` + `regex.rs`

**Interfaces:**
- Produces:
  - `pub enum Action { Replace, ReplaceAll, Keep, Drop, KeepEqual, DropEqual, KeepIfEqual, DropIfEqual, KeepIfContains, DropIfContains, KeepMetrics, DropMetrics, Labelmap, LabelmapAll, Labeldrop, Labelkeep, Hashmod, Lowercase, Uppercase, Graphite }` (serde `rename_all` to snake_case matching upstream YAML action strings — verify each against the list below).
  - `pub struct RelabelConfig { pub source_labels: Vec<String>, pub separator: String (default ";"), pub target_label: String, pub regex: AnchoredRegex, pub modulus: u64, pub replacement: String (default "$1"), pub action: Action, pub if_expr: Option<IfExpression> }` — where `IfExpression` is a placeholder type defined in Task 4 (for Task 1, parse the `if` field as `Option<String>` raw and store it; Task 4 compiles it).
  - `pub struct AnchoredRegex { re: regex::Regex, original: String }` with `pub fn compile(pattern: &str) -> Result<AnchoredRegex, RelabelError>` (anchors as `^(?:<pattern>)$`) and `pub fn is_match(&self, s: &str) -> bool`, `pub fn replace_all<'t>(&self, s: &'t str, rep: &str) -> std::borrow::Cow<'t, str>`.
  - `pub struct RelabelError { pub msg: String }` (Display + Error).
  - `pub fn parse_relabel_configs(yaml: &str) -> Result<Vec<RelabelConfig>, RelabelError>`.

**Reference:** action strings from `lib/promrelabel/config.go` — exact YAML values: `replace`, `replace_all`, `keep`, `drop`, `keepequal`, `dropequal`, `keep_if_equal`, `drop_if_equal`, `keep_if_contains`, `drop_if_contains`, `keep_metrics`, `drop_metrics`, `labelmap`, `labelmap_all`, `labeldrop`, `labelkeep`, `hashmod`, `lowercase`, `uppercase`, `graphite`. Default regex is `(.*)`, default separator `;`, default replacement `$1`, default action `replace`. `source_labels` accepts a scalar or list in YAML; `regex` accepts a string or list (list is `|`-joined) — port `config.go`'s `MultiLineRegex` handling.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn parses_a_relabel_config() {
    let y = r#"
- source_labels: [__name__, job]
  separator: "/"
  regex: "(.+)/(.+)"
  target_label: combined
  replacement: "$1-$2"
  action: replace
"#;
    let cfgs = parse_relabel_configs(y).unwrap();
    assert_eq!(cfgs.len(), 1);
    assert_eq!(cfgs[0].source_labels, vec!["__name__".to_string(), "job".to_string()]);
    assert_eq!(cfgs[0].separator, "/");
    assert!(matches!(cfgs[0].action, Action::Replace));
    assert_eq!(cfgs[0].target_label, "combined");
}

#[test]
fn defaults_applied() {
    let cfgs = parse_relabel_configs("- action: uppercase\n  source_labels: [x]\n  target_label: x\n").unwrap();
    assert_eq!(cfgs[0].separator, ";");
    assert_eq!(cfgs[0].replacement, "$1");
    assert_eq!(cfgs[0].regex.original, "(.*)");
}

#[test]
fn regex_is_anchored() {
    let re = AnchoredRegex::compile("foo.*").unwrap();
    assert!(re.is_match("foobar"));
    assert!(!re.is_match("xfoobar")); // anchored: must match whole string
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p esm-relabel` → FAIL.
- [ ] **Step 3: Implement** scaffold + structs + parse + `AnchoredRegex`. Deps: `serde`, `serde_yaml_ng`, `regex` (workspace). Keep the `if` field as raw `Option<String>` for now.
- [ ] **Step 4: Run** — PASS; `RUSTFLAGS="-D warnings" cargo clippy -p esm-relabel --all-targets`.
- [ ] **Step 5: Commit** — `git commit -m "feat: esm-relabel config parse + anchored regex"`

---

## Task 2: `esm-relabel` apply — standard actions

**Files:**
- Create: `crates/esm-relabel/src/apply.rs`, `crates/esm-relabel/src/label.rs`
- Test: inline in `apply.rs`

**Interfaces:**
- Consumes: `config::{RelabelConfig, Action}`.
- Produces:
  - `pub struct Label { pub name: String, pub value: String }` (a working label; `__name__` is a normal label).
  - `pub fn apply_one(cfg: &RelabelConfig, labels: &mut Vec<Label>) -> bool` — applies ONE config to the label set in place; returns `false` if the series should be dropped (only `keep`/`drop`-family actions can return false; all others return true). This task implements the STANDARD actions: `Replace`, `ReplaceAll`, `Keep`, `Drop`, `Labelmap`, `Labeldrop`, `Labelkeep`, `Hashmod`, `Lowercase`, `Uppercase`. (The equal/contains/metrics/graphite variants + `if` are Task 3/4 — for unimplemented actions in THIS task, `apply_one` may `unreachable!()` guarded by a match; Task 3 fills them.)
  - helpers: `fn get_label_value<'a>(labels: &'a [Label], name: &str) -> &'a str` (missing → ""), `fn concat_source_values(labels: &[Label], source_labels: &[String], separator: &str) -> String`, `fn set_label(labels: &mut Vec<Label>, name: &str, value: String)` (replace or push; empty value → remove), `fn remove_label(labels: &mut Vec<Label>, name: &str)`.

**Reference:** `lib/promrelabel/relabel.go` `applyRelabelConfig`. Exact semantics:
- **replace**: `s = concat(source_labels, sep)`; if `regex` doesn't match `s` → no-op; else `target_label`'s new value = `regex.replace_all(s, replacement)`; if result is empty → remove target label; else set it. (`target_label` itself may be templated via `$1` from the match — port the `expandCaptureGroups` on target_label too.)
- **replace_all**: replace ALL regex matches within the concatenated source value (VM applies the replacement across the string; read the body).
- **keep**: if `regex` matches `concat(source)` → keep (return true); else return false (drop).
- **drop**: if `regex` matches `concat(source)` → return false (drop); else true.
- **labelmap**: for each label whose NAME matches `regex`, add a new label named `regex.replace_all(name, replacement)` = old value.
- **labeldrop**: remove labels whose name matches `regex`.
- **labelkeep**: remove labels whose name does NOT match `regex`.
- **hashmod**: `target_label` = `fnv1a(concat(source)) % modulus` as a decimal string. Use FNV-1a 64-bit (same constants as elsewhere in the repo — grep `fnv` / offset `0xcbf29ce484222325`).
- **lowercase**/**uppercase**: `target_label` = ASCII-lower/upper of `concat(source)`.

- [ ] **Step 1: Write the failing test**

```rust
fn labels(pairs: &[(&str,&str)]) -> Vec<Label> {
    pairs.iter().map(|(n,v)| Label{name:n.to_string(), value:v.to_string()}).collect()
}
fn cfg(action: Action, src: &[&str], target: &str, regex: &str, repl: &str) -> RelabelConfig {
    RelabelConfig {
        source_labels: src.iter().map(|s|s.to_string()).collect(),
        separator: ";".into(), target_label: target.into(),
        regex: AnchoredRegex::compile(regex).unwrap(), modulus: 0,
        replacement: repl.into(), action, if_expr: None,
    }
}

#[test]
fn replace_sets_target() {
    let mut l = labels(&[("__name__","http_requests"),("code","200")]);
    assert!(apply_one(&cfg(Action::Replace, &["code"], "code_class", "(.)..", "${1}xx"), &mut l));
    assert_eq!(get_label_value(&l, "code_class"), "2xx");
}
#[test]
fn keep_drops_non_matching() {
    let mut l = labels(&[("__name__","up")]);
    assert!(apply_one(&cfg(Action::Keep, &["__name__"], "", "up", "$1"), &mut l));      // matches -> keep
    assert!(!apply_one(&cfg(Action::Keep, &["__name__"], "", "down", "$1"), &mut l));   // no match -> drop
}
#[test]
fn labeldrop_removes_matching_labels() {
    let mut l = labels(&[("__name__","up"),("tmp_a","1"),("tmp_b","2"),("keep","3")]);
    apply_one(&cfg(Action::Labeldrop, &[], "", "tmp_.*", "$1"), &mut l);
    assert!(l.iter().all(|x| !x.name.starts_with("tmp_")));
    assert_eq!(get_label_value(&l, "keep"), "3");
}
#[test]
fn hashmod_is_deterministic() {
    let mut c = cfg(Action::Hashmod, &["__name__"], "shard", "(.*)", "$1"); c.modulus = 8;
    let mut l = labels(&[("__name__","metric")]);
    apply_one(&c, &mut l);
    let v = get_label_value(&l, "shard").to_string();
    apply_one(&c, &mut labels(&[("__name__","metric")])); // stable
    assert!(v.parse::<u64>().unwrap() < 8);
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** the standard actions + helpers.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esm-relabel standard actions"`

---

## Task 3: `esm-relabel` apply — equal / contains / metrics / labelmap_all

**Files:**
- Modify: `crates/esm-relabel/src/apply.rs`
- Test: inline in `apply.rs`

**Interfaces:**
- Produces: extend `apply_one` to handle `KeepEqual`, `DropEqual`, `KeepIfEqual`, `DropIfEqual`, `KeepIfContains`, `DropIfContains`, `KeepMetrics`, `DropMetrics`, `LabelmapAll`.

**Reference:** `lib/promrelabel/relabel.go`. Semantics:
- **keepequal**: if value of `target_label` == `concat(source_labels)` → keep; else drop. **dropequal**: inverse.
- **keep_if_equal**: keep only if ALL of `source_labels` have equal values; else drop. **drop_if_equal**: inverse. (Read the body — these compare the source label values to each other.)
- **keep_if_contains**: keep if `target_label`'s value contains `concat(source)` as … (read exact: `keep_if_contains` keeps if the target label value contains all the source values — port the body precisely). **drop_if_contains**: inverse.
- **keep_metrics**: keep if `__name__` matches `regex` (sugar: like `keep` on `[__name__]`). **drop_metrics**: drop if `__name__` matches `regex`.
- **labelmap_all**: like labelmap but applies the regex replacement to ALL label names (not just matching ones) — read `config.go`/`relabel.go` to confirm the exact mapping.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn keep_metrics_filters_by_name() {
    let mut l = labels(&[("__name__","node_cpu")]);
    assert!(apply_one(&cfg(Action::KeepMetrics, &[], "", "node_.*", "$1"), &mut l));
    assert!(!apply_one(&cfg(Action::KeepMetrics, &[], "", "http_.*", "$1"), &mut labels(&[("__name__","node_cpu")])));
}
#[test]
fn keepequal_compares_target_to_source() {
    // target_label "a" equals concat(source ["b"]) -> keep
    let mut l = labels(&[("a","x"),("b","x")]);
    assert!(apply_one(&cfg(Action::KeepEqual, &["b"], "a", "(.*)", "$1"), &mut l));
    let mut l2 = labels(&[("a","x"),("b","y")]);
    assert!(!apply_one(&cfg(Action::KeepEqual, &["b"], "a", "(.*)", "$1"), &mut l2));
}
#[test]
fn drop_metrics_drops_by_name() {
    assert!(!apply_one(&cfg(Action::DropMetrics, &[], "", "up", "$1"), &mut labels(&[("__name__","up")])));
    assert!(apply_one(&cfg(Action::DropMetrics, &[], "", "up", "$1"), &mut labels(&[("__name__","down")])));
}
```

(Verify each expected value against the upstream body; adjust `keep_if_contains`/`labelmap_all` test values to the real semantics you read.)

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** the remaining non-graphite actions.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esm-relabel equal/contains/metrics actions"`

---

## Task 4: `esm-relabel` — `if:` gating, `graphite` action, `ParsedConfigs` public API

**Files:**
- Create: `crates/esm-relabel/src/if_expr.rs`, `crates/esm-relabel/src/graphite.rs`
- Modify: `crates/esm-relabel/src/lib.rs`, `crates/esm-relabel/src/config.rs`, `crates/esm-relabel/src/apply.rs`
- Test: inline

**Interfaces:**
- Produces:
  - `pub struct IfExpression { /* parsed metric-selector label matchers */ }` with `pub fn parse(s: &str) -> Result<IfExpression, RelabelError>` (parse via `esm_metricsql::parse` → extract the `MetricExpr`'s label filters; support the `a or b` multi-group form) and `pub fn matches(&self, labels: &[Label]) -> bool`.
  - `graphite::apply_graphite(cfg: &RelabelConfig, labels: &mut Vec<Label>)` — the `graphite` action (match a graphite-style `__name__` against `cfg.regex`/a match template and set labels from a template). Read `lib/promrelabel/graphite.go` for the `match`/`labels` template format (VM's graphite relabel uses `*`-globs, not RE2 — port faithfully).
  - `pub struct ParsedConfigs { cfgs: Vec<RelabelConfig> }` with `pub fn parse(yaml: &str) -> Result<ParsedConfigs, RelabelError>` (calls `parse_relabel_configs` + compiles each `if`) and `pub fn apply(&self, labels: &mut Vec<Label>) -> bool` (runs each cfg in order via `apply_one`, honoring `if_expr` gating: if a cfg has an `if_expr` that doesn't match, skip it; returns false as soon as any action drops the series).
- Consumes: `esm_metricsql::parse` (grep `crates/esm-metricsql/src/parser.rs` for `pub fn parse` and the `MetricExpr`/`LabelFilter` types to extract matchers).

**Reference:** `lib/promrelabel/if_expression.go` (the `if` selector — a metricsql-style label matcher, supports `or`) and `lib/promrelabel/graphite.go`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn if_gates_the_rule() {
    // rule only applies when {__name__="up"}; a "down" series is untouched
    let p = ParsedConfigs::parse(r#"
- if: '{__name__="up"}'
  source_labels: [__name__]
  target_label: matched
  replacement: "yes"
  action: replace
"#).unwrap();
    let mut up = labels(&[("__name__","up")]);
    assert!(p.apply(&mut up));
    assert_eq!(get_label_value(&up, "matched"), "yes");
    let mut down = labels(&[("__name__","down")]);
    assert!(p.apply(&mut down));
    assert_eq!(get_label_value(&down, "matched"), ""); // rule skipped
}

#[test]
fn full_config_drop_and_relabel_pipeline() {
    let p = ParsedConfigs::parse(r#"
- source_labels: [__name__]
  regex: "temp_.*"
  action: drop
- source_labels: [instance]
  target_label: host
  regex: "([^:]+):.*"
  replacement: "$1"
  action: replace
"#).unwrap();
    assert!(!p.apply(&mut labels(&[("__name__","temp_x")])));           // dropped
    let mut l = labels(&[("__name__","up"),("instance","h1:9090")]);
    assert!(p.apply(&mut l));
    assert_eq!(get_label_value(&l, "host"), "h1");
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** `IfExpression`, `graphite`, wire `if_expr` compilation in `ParsedConfigs::parse`, and `ParsedConfigs::apply`. Add `esm-metricsql` dep. Run the FULL crate suite.
- [ ] **Step 4: Run** — `cargo test -p esm-relabel` PASS; clippy clean; `cargo fmt`.
- [ ] **Step 5: Commit** — `git commit -m "feat: esm-relabel if-gating, graphite action, ParsedConfigs API"`

---

## Task 5: `esmagent` scaffold + `ForwardingSink` (decode + global relabel + owned series)

**Files:**
- Create: `crates/esmagent/Cargo.toml`, `crates/esmagent/src/main.rs` (temp `fn main(){}`), `crates/esmagent/src/series.rs`, `crates/esmagent/src/sink.rs`
- Modify: root `Cargo.toml` `members`
- Test: inline in `sink.rs`

**Interfaces:**
- Consumes: `esm_insert::{RowSink, MetricRow}` (`MetricRow<'a> { metric_name_raw: &'a [u8], timestamp: i64, value: f64 }`), `esm_storage::MetricName` (`.unmarshal_raw(&[u8]) -> Result<(),String>`, `.metric_group: Vec<u8>`, `.tags: Vec<Tag>` where `Tag` has `key`/`value` byte fields — grep `crates/esm-storage/src/metric_name.rs` for `Tag`'s field names), `esm_relabel::{ParsedConfigs, Label}`.
- Produces:
  - `pub struct OwnedSeries { pub labels: Vec<esm_relabel::Label>, pub samples: Vec<esm_protoparser::prompb::Sample> }` (in `series.rs`; reuse the owned `prompb::Sample`).
  - `pub trait SeriesConsumer: Send + Sync { fn push(&self, series: &[OwnedSeries]); }` — the fan-out is one impl (Task 9); the sink holds a `Arc<dyn SeriesConsumer>`.
  - `pub struct ForwardingSink { global_relabel: Option<ParsedConfigs>, consumer: Arc<dyn SeriesConsumer> }` impl `esm_insert::RowSink`: `add_rows` groups rows by identical `metric_name_raw` into `OwnedSeries` (decode once per distinct name via `MetricName::unmarshal_raw` → build labels: `__name__` = metric_group, plus each tag), applies `global_relabel` (dropping killed series), and calls `consumer.push(&survivors)`.
- **Decode note:** build the label set as `Label{name:"__name__", value: metric_group}` + one `Label` per tag (`name = tag.key`, `value = tag.value`), converting bytes→String (labels are UTF-8; on invalid UTF-8, lossy-convert and note — VM labels are UTF-8 in practice).

**Reference:** `crates/esmetrics/src/wiring.rs` (`StorageSink impl RowSink` — mirror the row-handling shape), `app/vmagent/remotewrite/remotewrite.go` `Push`/`tryPush` (global relabel then fan-out).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn sink_decodes_relabels_and_forwards() {
    use std::sync::Mutex;
    struct Cap(Mutex<Vec<OwnedSeries>>);
    impl SeriesConsumer for Cap { fn push(&self, s: &[OwnedSeries]) { self.0.lock().unwrap().extend_from_slice(s); } }
    let cap = Arc::new(Cap(Mutex::new(vec![])));
    // global relabel: drop temp_* metrics
    let gr = ParsedConfigs::parse("- source_labels: [__name__]\n  regex: \"temp_.*\"\n  action: drop\n").unwrap();
    let sink = ForwardingSink { global_relabel: Some(gr), consumer: cap.clone() };
    // build two rows via esm_storage::marshal_metric_name_raw
    let mut n1 = Vec::new(); esm_storage::marshal_metric_name_raw(&mut n1, &[(b"", b"up"), (b"job", b"x")]);
    let mut n2 = Vec::new(); esm_storage::marshal_metric_name_raw(&mut n2, &[(b"", b"temp_cpu")]);
    sink.add_rows(&[
        esm_insert::MetricRow{ metric_name_raw: &n1, timestamp: 1000, value: 1.0 },
        esm_insert::MetricRow{ metric_name_raw: &n2, timestamp: 1000, value: 2.0 },
    ]).unwrap();
    let got = cap.0.lock().unwrap();
    assert_eq!(got.len(), 1); // temp_cpu dropped
    assert!(got[0].labels.iter().any(|l| l.name=="__name__" && l.value=="up"));
    assert!(got[0].labels.iter().any(|l| l.name=="job" && l.value=="x"));
    assert_eq!(got[0].samples[0].value, 1.0);
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p esmagent sink` → FAIL. Deps: `esm-insert`, `esm-storage`, `esm-relabel`, `esm-protoparser`.
- [ ] **Step 3: Implement** the scaffold + `OwnedSeries` + `ForwardingSink`.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent ForwardingSink + owned series"`

---

## Task 6: `esmagent` PendingSeries (batch → snappy WriteRequest blocks)

**Files:**
- Create: `crates/esmagent/src/pendingseries.rs`
- Test: inline

**Interfaces:**
- Consumes: `series::OwnedSeries`, `esm_protoparser::prompb::{TimeSeries, Label, Sample}`, `esm_protoparser::prompb_encode::encode_and_compress`.
- Produces:
  - `pub struct PendingSeries { /* Vec<OwnedSeries>, max_block_size */ }`
  - `impl PendingSeries { pub fn new(max_block_size: usize) -> Self; pub fn add(&mut self, s: &[OwnedSeries]) -> Vec<Vec<u8>>; pub fn flush(&mut self) -> Option<Vec<u8>>; }` — `add` appends and, whenever the accumulated *uncompressed* estimated size ≥ `max_block_size`, marshals the accumulated series into a snappy-compressed block (via `to_prompb` borrow → `encode_and_compress`) and returns it (returning possibly multiple full blocks); `flush` emits a final block for any remainder.
  - `fn to_prompb(series: &[OwnedSeries]) -> Vec<TimeSeries<'_>>` — borrow each `OwnedSeries` label String as `&[u8]` into `prompb::Label`, reuse the `Sample`s. Lifetime valid for the encode call.

**Reference:** `app/vmagent/remotewrite/pendingseries.go` (the block-size accumulation + marshal). Block = snappy(protobuf WriteRequest). `-remoteWrite.maxBlockSize` default is 8MiB uncompressed — but for tests use a small size.

- [ ] **Step 1: Write the failing test**

```rust
fn series(name: &str) -> OwnedSeries {
    OwnedSeries {
        labels: vec![esm_relabel::Label{name:"__name__".into(), value:name.into()}],
        samples: vec![esm_protoparser::prompb::Sample{ value: 1.0, timestamp: 1 }],
    }
}
#[test]
fn flush_emits_a_decodable_block() {
    let mut ps = PendingSeries::new(8 * 1024 * 1024);
    let full = ps.add(&[series("a"), series("b")]);
    assert!(full.is_empty()); // under block size, nothing emitted yet
    let block = ps.flush().expect("a block");
    let raw = snap::raw::Decoder::new().decompress_vec(&block).unwrap();
    let wr = esm_protoparser::unmarshal_write_request(&raw).unwrap();
    assert_eq!(wr.timeseries.len(), 2);
}
#[test]
fn add_emits_block_when_full() {
    let mut ps = PendingSeries::new(1); // tiny -> every add flushes
    let blocks = ps.add(&[series("a")]);
    assert_eq!(blocks.len(), 1);
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** `PendingSeries` + `to_prompb`. Add `snap` dep (for the test decode).
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent pending-series block batching"`

---

## Task 7: `esmagent` PersistentQueue (durable FIFO of blocks)

**Files:**
- Create: `crates/esmagent/src/queue.rs`
- Test: inline

**Interfaces:**
- Produces:
  - `pub struct PersistentQueue { /* dir, in-mem VecDeque<Vec<u8>>, disk state, max_bytes, current_bytes */ }`
  - `impl PersistentQueue { pub fn open(dir: &Path, max_bytes: u64) -> Result<PersistentQueue, QueueError>; pub fn push(&self, block: Vec<u8>) -> Result<(), QueueError>; pub fn pop(&self, timeout: Duration) -> Option<Vec<u8>>; pub fn pending_bytes(&self) -> u64; pub fn flush_to_disk(&self); pub fn close(self); }`
  - Behavior (faithful *behavior*, not VM's exact chunk format): a durable FIFO of opaque blocks. `push` appends (in-mem fast path; spills whole blocks to numbered files under `dir` when in-mem exceeds a soft cap, and always on `flush_to_disk`/`close`). `pop` returns the oldest block (blocking up to `timeout`, for worker threads), removing it durably. On `open`, replay any on-disk blocks (in FIFO order by file index) into the queue. Enforce `max_bytes` (sum of queued block sizes): when a `push` would exceed it, **drop the oldest** block(s) until it fits (log + count). Thread-safe (`Arc<Mutex<...>>` + `Condvar` for `pop`).
  - Choose a simple, robust on-disk representation: one file per block (or an append log with an index) under `dir/`, plus a small metadata file for ordering. Document the format in a module comment. The ONLY requirement is: survives process restart, FIFO order, size-capped, no external reader.

**Reference:** `lib/persistentqueue/{queue.go,fastqueue.go}` for the BEHAVIOR (in-mem FastQueue over a disk Queue, `MustClose`/replay, `maxPendingBytes` drop-oldest). Do NOT port the exact chunk format.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn durable_fifo_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let q = PersistentQueue::open(dir.path(), 10_000_000).unwrap();
        q.push(b"block1".to_vec()).unwrap();
        q.push(b"block2".to_vec()).unwrap();
        q.flush_to_disk();
        q.close();
    }
    let q = PersistentQueue::open(dir.path(), 10_000_000).unwrap();
    assert_eq!(q.pop(Duration::from_secs(1)).as_deref(), Some(&b"block1"[..]));
    assert_eq!(q.pop(Duration::from_secs(1)).as_deref(), Some(&b"block2"[..]));
    assert!(q.pop(Duration::from_millis(50)).is_none());
}
#[test]
fn drops_oldest_when_over_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let q = PersistentQueue::open(dir.path(), 10).unwrap(); // 10-byte cap
    q.push(b"aaaaa".to_vec()).unwrap();  // 5
    q.push(b"bbbbb".to_vec()).unwrap();  // 10 total
    q.push(b"ccccc".to_vec()).unwrap();  // would be 15 -> drop oldest "aaaaa"
    assert_eq!(q.pop(Duration::from_millis(50)).as_deref(), Some(&b"bbbbb"[..]));
    assert_eq!(q.pop(Duration::from_millis(50)).as_deref(), Some(&b"ccccc"[..]));
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL. Add `tempfile` dev-dep.
- [ ] **Step 3: Implement** `PersistentQueue`.
- [ ] **Step 4: Run** — PASS; clippy clean; windows-gnu check (file handling is platform-sensitive).
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent persistent block queue"`

---

## Task 8: `esmagent` remote-write Client (workers + retry/backoff)

**Files:**
- Create: `crates/esmagent/src/client.rs`
- Test: inline (stub esm-http server)

**Interfaces:**
- Consumes: `queue::PersistentQueue`, `datasource`-style `AuthConfig`/`TlsConfig` (define local copies OR reuse — grep `crates/esmalert/src/datasource/auth.rs`; if not easily reusable across crates, define a local `AuthConfig`/`TlsConfig` mirroring it and note the duplication).
- Produces:
  - `pub struct ClientConfig { pub url: String, pub queues: usize, pub retry_min: Duration, pub retry_max: Duration, pub send_timeout: Duration, pub auth: AuthConfig, pub tls: TlsConfig }`
  - `pub struct Client { /* worker JoinHandles, stop flag */ }`
  - `impl Client { pub fn start(cfg: ClientConfig, queue: Arc<PersistentQueue>) -> Result<Client, ClientError>; pub fn stop(self); }` — spawns `cfg.queues` worker threads; each loops: `queue.pop(timeout)` → POST the block to `cfg.url` (`Content-Encoding: snappy`, `Content-Type: application/x-protobuf`, `X-Prometheus-Remote-Write-Version: 0.1.0`) with a `send_timeout`. On success (2xx/204) → block consumed. On retryable (5xx / 429 / transport error) → retry the SAME block with exponential backoff (`retry_min` doubling to `retry_max`), NOT re-queueing (hold the block in the worker and retry) until success or stop. On non-retryable 4xx → drop the block (log + count). Must have a request timeout (mirror esmalert's RwClient send-timeout to avoid shutdown hang). Never panic; a transport error logs + retries.

**Reference:** `app/vmagent/remotewrite/client.go` `runWorker`/`sendBlockHTTP`/retry loop, and `crates/esmalert/src/remotewrite/client.rs` (the send-timeout + snappy POST + graceful shutdown pattern — closest existing analog).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn retries_5xx_then_succeeds_and_drops_4xx() {
    // stub server: first 2 requests -> 503, 3rd -> 204; a separate path -> 400
    // start a Client with retry_min=10ms, one worker, pointed at the stub
    // push a block; assert it is eventually delivered (server saw >=3 attempts, block gone from queue)
    // push a block to the 400 path; assert it is dropped (not retried forever), queue drains
}
#[test]
fn shutdown_does_not_hang_against_dead_endpoint() {
    // Client pointed at a never-accepting TcpListener with a short send_timeout;
    // push a block; Client::stop() returns within a few seconds (send_timeout bounds it).
}
```

Flesh both out fully against an in-process stub (mirror esmalert's remotewrite test harness). No blank stubs.

- [ ] **Step 2: Run to verify it fails** — FAIL. Add `reqwest` (blocking, rustls — match esmalert's features).
- [ ] **Step 3: Implement** `Client`.
- [ ] **Step 4: Run** — PASS (no hang); clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent remote-write client with retry"`

---

## Task 9: `esmagent` RemoteWriteCtx + Fanout

**Files:**
- Create: `crates/esmagent/src/rwctx.rs`, `crates/esmagent/src/fanout.rs`
- Test: inline in `fanout.rs`

**Interfaces:**
- Consumes: `esm_relabel::ParsedConfigs`, `pendingseries::PendingSeries`, `queue::PersistentQueue`, `client::{Client, ClientConfig}`, `series::OwnedSeries`, `sink::SeriesConsumer`.
- Produces:
  - `pub struct RwCtxConfig { pub client: ClientConfig, pub url_relabel: Option<ParsedConfigs>, pub queue_dir: PathBuf, pub max_disk_bytes: u64, pub max_block_size: usize, pub flush_interval: Duration }`
  - `pub struct RemoteWriteCtx { /* Arc<PersistentQueue>, Client, Mutex<PendingSeries>, url_relabel, flush thread */ }`
  - `impl RemoteWriteCtx { pub fn start(cfg: RwCtxConfig) -> Result<RemoteWriteCtx, ...>; pub fn push(&self, series: &[OwnedSeries]); pub fn stop(self); }` — `push`: apply `url_relabel` (per-destination copy; drop killed), add to `PendingSeries`, and enqueue any full blocks to the queue; a background flush thread flushes the pending buffer on `flush_interval`. `stop`: flush pending → close queue → stop client.
  - `pub struct Fanout { ctxs: Vec<RemoteWriteCtx> }` impl `SeriesConsumer`: `push(series)` forwards to EVERY ctx (each does its own per-URL relabel + queue). `pub fn stop(self)`.

**Reference:** `app/vmagent/remotewrite/remotewrite.go` `newRemoteWriteCtx` + the fan-out in `tryPush`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn fanout_delivers_to_two_destinations_with_per_url_relabel() {
    // two stub servers capturing remote-write bodies
    // ctx A: no url relabel; ctx B: url relabel adding label {dc="b"} via replace on a constant
    // Fanout::push(one series) ; flush ; assert BOTH servers received the series,
    // and B's copy has the extra label while A's does not (decode the snappy+protobuf body)
}
```

Flesh out fully with two in-process stub servers + block decode. No blank stubs.

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** `RemoteWriteCtx` + `Fanout`.
- [ ] **Step 4: Run** — PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent per-destination ctx + fanout"`

---

## Task 10: `esmagent` CLI flags + main wiring

**Files:**
- Create: `crates/esmagent/src/flags.rs`
- Modify: `crates/esmagent/src/main.rs`
- Test: inline in `flags.rs` + a config-validation smoke test

**Interfaces:**
- Consumes: everything above + `esm_insert` router/HTTP handlers + `esm_http` server (grep `crates/esmetrics/src/wiring.rs` + `crates/esmetrics/src/lib.rs` for how the esm-insert `InsertHandlers`/router is mounted on the esm-http server — mirror that, substituting `ForwardingSink` for `StorageSink`).
- Produces:
  - `pub struct Flags { /* the in-scope flags from the spec */ }` + `pub fn parse_flags(argv: &[String]) -> Result<Flags, FlagError>` (mirror esmalert's `flags.rs`: `-flag=value`/`-flag value`, repeatable arrays for `-remoteWrite.url`/`-remoteWrite.urlRelabelConfig`, `*File` secrets, redaction).
  - `main()`: parse flags → validate (≥1 `-remoteWrite.url`; parse each relabel config file via `ParsedConfigs::parse`, fail fast on error) → build a `RemoteWriteCtx` per url (queue dir = `tmpDataPath/<sanitized-url-or-index>`) → `Fanout` → `ForwardingSink` → mount the esm-insert router on an esm-http server at `-httpListenAddr` (also serve `/metrics` gated by `-metrics.authKey`, `/-/healthy`) → run until SIGINT/SIGTERM → graceful shutdown (`Fanout::stop`). A `-dryRun` validates config + exits.
  - `dropDanglingQueues`-equivalent: on startup, remove queue subdirs under `tmpDataPath` that don't correspond to a configured destination.

**Reference:** in-scope flag list in the spec's "CLI & config" section; `app/vmagent/main.go` (wiring); `crates/esmalert/src/app.rs` (signal/shutdown idiom); `crates/esmetrics/src/wiring.rs`/`lib.rs` (mounting the esm-insert router).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn parses_core_flags() {
    let f = parse_flags(&["-remoteWrite.url=http://a/api/v1/write","-remoteWrite.url=http://b/api/v1/write",
                          "-httpListenAddr=:8429","-remoteWrite.tmpDataPath=/tmp/eq"]
        .iter().map(|s|s.to_string()).collect::<Vec<_>>()).unwrap();
    assert_eq!(f.remote_write_urls.len(), 2);
    assert_eq!(f.http_listen_addr, ":8429");
}
#[test]
fn dryrun_rejects_bad_relabel_config() {
    // write a temp relabel file with an invalid action; run_dry(flags) -> Err
    // and a valid one -> Ok
}
```

Flesh out `dryrun_rejects_bad_relabel_config` fully (temp files + a `run_dry(&Flags)->Result` seam). No blank stubs.

- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** flags + wiring + `run_dry`. `cargo build -p esmagent` produces the binary.
- [ ] **Step 4: Run** — PASS; `cargo build -p esmagent`; clippy workspace clean; `cargo fmt`.
- [ ] **Step 5: Commit** — `git commit -m "feat: esmagent CLI flags and main wiring"`

---

## Task 11: `esmagent` e2e integration + docs

**Files:**
- Create: `crates/esmagent/tests/e2e.rs`, `crates/esmagent/README.md`
- Modify: `crates/esmagent/src/main.rs`/add `src/lib.rs` (expose what the e2e test needs — mirror esmalert's bin/lib split), `README.md` (top-level), `docs/PORTING.md` (add an `app/vmagent (forwarding tier)` row)
- Test: `crates/esmagent/tests/e2e.rs`

**Interfaces:** none new — drives the built agent end to end.

**Reference:** `crates/esmalert/tests/e2e.rs` + `crates/esmalert-tool/tests/` for the in-process stub-server harness pattern.

- [ ] **Step 1: Write the failing test** — the full forwarding scenario, using in-process stub servers:
  1. Start esmagent (in-process: build `ForwardingSink` + `Fanout` with two destination stub servers + a temp `tmpDataPath`, mount on an esm-http server on `127.0.0.1:0`).
  2. POST a Prometheus remote-write request (and one influx line) to esmagent's input endpoint.
  3. Assert BOTH destination stubs receive the series (decode snappy+protobuf).
  4. Kill destination B's stub; POST more data; assert B's blocks accumulate on disk (`tmpDataPath/<b>` non-empty) while A keeps receiving.
  5. Bring B back; assert the queued blocks are delivered (B receives the backlog).
  6. Restart the `Fanout`/queue (drop + reopen with the same tmpDataPath) and assert queued-but-undelivered blocks survive.
  Deterministic — bounded polling for the captures, no sleep-only asserts.
- [ ] **Step 2: Run to verify it fails** — `cargo test -p esmagent --test e2e` → FAIL.
- [ ] **Step 3: Implement** any wiring gaps + the README (honest: this is vmagent's FORWARDING tier; scraping is deferred to Phase 2; document the deferred list — stream aggregation, cloud SD, oauth2, blocking backpressure mode). Add the PORTING.md row.
- [ ] **Step 4: Run** — `cargo test -p esmagent --test e2e` PASS; full-workspace `cargo test --workspace`, `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets`, `cargo fmt --check`, windows-gnu check.
- [ ] **Step 5: Commit** — `git commit -m "test: esmagent forwarding e2e; docs: esmagent usage"`

---

## Final verification (after Task 11, before merge)

- [ ] `cargo test --workspace` green on Linux; push and confirm Windows CI green (the persistent-queue disk handling runs on Windows only in CI — watch it).
- [ ] windows-gnu cross-compile check passes.
- [ ] Whole-branch code review (subagent-driven final review on the most capable model) — focus: no data-loss paths beyond documented drop-oldest/4xx-drop; queue durability + FIFO across restart; no worker-thread panics; per-destination failure isolation (one dead backend never blocks another or ingestion); no credential logging; relabel fidelity vs upstream vectors.
- [ ] No esmetrics ingest/query hot-path impact (esmagent is a separate binary; `esm-insert`/`esm-storage` were consumed, not modified — confirm no behavior change) → no benchmark re-validation needed unless a shared crate was modified.
- [ ] Update memory: add an `esmagent-forwarding` note + MEMORY.md index line; note Phase 2 (scrape engine) remains.
