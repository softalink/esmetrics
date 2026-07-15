//! Service discovery providers for `scrape_configs`: `static_configs`,
//! `file_sd_configs`, and `http_sd_configs`. Port of the relevant slices of
//! `lib/promscrape/discovery/{file,http}` plus the (trivial) `static_configs`
//! handling in `lib/promscrape/config.go`.
//!
//! Target relabel, the scrape loop, and the manager that owns/polls these
//! providers on a schedule are later tasks — see the module doc in
//! `scrape/mod.rs`. This module only produces [`TargetGroup`]s: one-shot
//! conversions ([`static_target_groups`], [`read_file_sd`],
//! [`fetch_http_sd`]) plus the [`Discovery`] trait and its three
//! implementations, which wrap those one-shot functions with a last-good
//! cache and a refresh timer so a caller can `poll()` on every scrape-manager
//! tick without re-reading a file or re-fetching a URL more often than
//! `refresh_interval`.
//!
//! ## `read_file_sd` error handling
//!
//! The task brief allows either "a malformed file errors the whole call" or
//! "skip + log the bad file, keep going", noting upstream `file_sd` does the
//! latter. This port does the latter for *every* per-pattern/per-file
//! problem — a syntactically invalid glob pattern, a glob entry error, a read
//! error, an unsupported extension, a parse error — so a single bad
//! pattern/file among several good ones never discards the good ones' groups
//! (nor skips the patterns configured after it). This mirrors upstream
//! `FileSDConfig.appendScrapeWork` (`config.go:1101-1109`), which logs a bad
//! glob and `continue`s: "Do not return this error, since other files may
//! contain valid scrape configs." [`read_file_sd`] therefore always returns
//! `Ok` today (the `Result` is kept for the brief-specified signature and a
//! future genuinely-unrecoverable condition).
//!
//! ## `Discovery::poll` refresh-timer design
//!
//! Each stateful provider ([`FileSdDiscovery`], [`HttpSdDiscovery`]) stores
//! its `refresh_interval` and a `next_refresh: Instant` deadline. `poll()`
//! only re-reads/re-fetches once `Instant::now() >= next_refresh`; otherwise
//! it returns a clone of the last-good group list. The very first `poll()`
//! always refreshes (the deadline is initialized to the construction time).
//! On the `Err` path (currently unreachable for `read_file_sd`, live for
//! `fetch_http_sd`: non-2xx HTTP, transport error, malformed body) the
//! provider logs a warning and keeps the previous last-good groups — an
//! http_sd hiccup is never seen as "all targets disappeared". The deadline
//! still advances on a failed refresh, so a permanently-broken source is
//! retried at `refresh_interval` cadence rather than every poll. Note this
//! last-good retention does *not* apply to file_sd's log+skip path: a file
//! that genuinely lost its targets (deleted, emptied, pattern now matching
//! nothing) surfaces as a smaller/empty successful refresh, not a masked
//! stale cache — see [`FileSdDiscovery`]'s doc.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde::Deserialize;

use super::config::{FileSdConfig, HttpSdConfig, ScrapeError, StaticConfig};
use crate::client::TlsConfig;

/// Label attached to every target group produced by [`read_file_sd`],
/// holding the path of the file the group came from (matches upstream
/// `file_sd`'s `__meta_filepath`).
const META_FILEPATH_LABEL: &str = "__meta_filepath";
/// Label attached to every target group produced by [`fetch_http_sd`],
/// holding the URL it was fetched from (matches upstream `http_sd`'s
/// `__meta_url`).
const META_URL_LABEL: &str = "__meta_url";

/// Cap on an http_sd response body/round-trip, so a slow or hung SD endpoint
/// can't block a [`HttpSdDiscovery::poll`] call indefinitely. `HttpSdConfig`
/// has no configurable timeout (see the task brief's shape), so a fixed,
/// generous default is applied here instead.
const HTTP_SD_TIMEOUT: Duration = Duration::from_secs(10);

/// One resolved group of scrape targets from a discovery source, plus the
/// labels the source itself attaches and an identifier for where the group
/// came from. Port of upstream `promutils.Labels`-carrying target groups
/// (`file.FileSDConfig`'s `filesdcache` entries, `http.SDConfig`'s API
/// response entries), narrowed to what this task's callers need.
#[derive(Debug, Clone, PartialEq)]
pub struct TargetGroup {
    pub targets: Vec<String>,
    pub labels: BTreeMap<String, String>,
    /// Where this group came from: `"<job>/static/<i>"` for a static config,
    /// the file path for file_sd, or the URL for http_sd. Not required to be
    /// unique across groups from the same source (a file/URL can list
    /// multiple groups; they share the same source string).
    pub source: String,
}

/// Raw shape of one entry in a Prometheus file_sd/http_sd JSON or YAML
/// document: `[{ "targets": [...], "labels": {...} }, ...]`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct SdEntry {
    targets: Vec<String>,
    labels: BTreeMap<String, String>,
}

/// Converts a scrape config's `static_configs` into [`TargetGroup`]s, one
/// per entry, with `source` set to `"<job>/static/<i>"` (`i` is the entry's
/// index in `cfgs`). Never fails: a `StaticConfig` is already fully
/// resolved, nothing here can go wrong.
pub fn static_target_groups(cfgs: &[StaticConfig], job: &str) -> Vec<TargetGroup> {
    cfgs.iter()
        .enumerate()
        .map(|(i, cfg)| TargetGroup {
            targets: cfg.targets.clone(),
            labels: cfg.labels.clone(),
            source: format!("{job}/static/{i}"),
        })
        .collect()
}

/// Expands each pattern in `files` (via [`glob::glob`]) and reads every
/// matched `.json`/`.yml`/`.yaml` file as a Prometheus file_sd document. A
/// glob pattern that is just a plain path (no wildcard) behaves the same as
/// upstream `filepath.Glob`: it "matches" only if the file exists, so a
/// missing file simply contributes no groups (nothing to log — [`glob`]
/// gives no distinguishable error for "no matches").
///
/// Every per-pattern/per-file problem — a syntactically invalid glob
/// pattern, a glob entry that errored, a read/parse failure, an unrecognized
/// extension — is logged and that one pattern/file is skipped rather than
/// failing the whole call, so one bad pattern never discards the groups
/// already collected from other valid patterns (nor skips the patterns after
/// it). This mirrors upstream `FileSDConfig.appendScrapeWork`
/// (`config.go:1101-1109`), which logs a bad glob and `continue`s: "Do not
/// return this error, since other files may contain valid scrape configs."
///
/// The [`Result`] return is kept for the brief-specified signature and for a
/// future genuinely-unrecoverable condition; today there is none, so this
/// always returns `Ok` with whatever it could collect.
pub fn read_file_sd(files: &[String]) -> Result<Vec<TargetGroup>, ScrapeError> {
    let mut groups = Vec::new();
    for pattern in files {
        let paths = match glob::glob(pattern) {
            Ok(paths) => paths,
            Err(e) => {
                log::warn!("esmagent file_sd: skipping invalid glob pattern {pattern:?}: {e}");
                continue;
            }
        };
        for entry in paths {
            let path = match entry {
                Ok(p) => p,
                Err(e) => {
                    log::warn!(
                        "esmagent file_sd: skipping unreadable glob entry for {pattern:?}: {e}"
                    );
                    continue;
                }
            };
            match read_one_file_sd(&path) {
                Ok(mut g) => groups.append(&mut g),
                Err(e) => {
                    log::warn!("esmagent file_sd: skipping {}: {e}", path.display());
                }
            }
        }
    }
    Ok(groups)
}

/// Reads and parses one file_sd file, dispatching on extension. Returns a
/// [`ScrapeError`] (never panics) for any I/O, extension, or parse problem —
/// the caller ([`read_file_sd`]) logs it and moves on rather than
/// propagating it.
fn read_one_file_sd(path: &std::path::Path) -> Result<Vec<TargetGroup>, ScrapeError> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    let content = std::fs::read_to_string(path)
        .map_err(|e| scrape_error(format!("cannot read file: {e}")))?;
    let entries: Vec<SdEntry> = match ext.as_deref() {
        Some("json") => serde_json::from_str(&content)
            .map_err(|e| scrape_error(format!("invalid json: {e}")))?,
        Some("yml") | Some("yaml") => serde_yaml_ng::from_str(&content)
            .map_err(|e| scrape_error(format!("invalid yaml: {e}")))?,
        _ => {
            return Err(scrape_error(
                "unsupported file_sd extension (expected .json, .yml, or .yaml)",
            ));
        }
    };
    let source = path.to_string_lossy().into_owned();
    Ok(entries
        .into_iter()
        .map(|entry| build_group(entry, META_FILEPATH_LABEL, &source, source.clone()))
        .collect())
}

/// GETs `cfg.url` (blocking, applying `cfg.auth`/`cfg.tls`) and parses the
/// response body as a Prometheus http_sd JSON document. Never panics: a
/// transport error, non-2xx status, or malformed body is returned as a
/// [`ScrapeError`] — the caller ([`HttpSdDiscovery::poll`]) decides whether
/// to fall back to a last-good cache.
pub fn fetch_http_sd(cfg: &HttpSdConfig) -> Result<Vec<TargetGroup>, ScrapeError> {
    let http = build_http_client(&cfg.tls)?;
    let mut req = http.get(&cfg.url);
    if let Some((user, pass)) = &cfg.auth.basic {
        req = req.basic_auth(user, Some(pass));
    } else if let Some(token) = &cfg.auth.bearer {
        req = req.bearer_auth(token);
    }

    let resp = req
        .send()
        .map_err(|e| scrape_error(format!("http_sd request to {:?} failed: {e}", cfg.url)))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(scrape_error(format!(
            "http_sd request to {:?} failed: status {status}",
            cfg.url
        )));
    }
    let body = resp
        .text()
        .map_err(|e| scrape_error(format!("http_sd response from {:?}: {e}", cfg.url)))?;
    let entries: Vec<SdEntry> = serde_json::from_str(&body).map_err(|e| {
        scrape_error(format!(
            "http_sd response from {:?}: invalid json: {e}",
            cfg.url
        ))
    })?;

    Ok(entries
        .into_iter()
        .map(|entry| build_group(entry, META_URL_LABEL, &cfg.url, cfg.url.clone()))
        .collect())
}

/// Turns one parsed [`SdEntry`] into a [`TargetGroup`], adding `meta_label`
/// (`__meta_filepath` or `__meta_url`) set to `meta_value`, with `source`.
/// A `meta_label` collision with a label already present in the entry's own
/// `labels` is overwritten by the meta value — mirrors upstream file_sd/
/// http_sd, where the meta label always wins.
fn build_group(entry: SdEntry, meta_label: &str, meta_value: &str, source: String) -> TargetGroup {
    let mut labels = entry.labels;
    labels.insert(meta_label.to_string(), meta_value.to_string());
    TargetGroup {
        targets: entry.targets,
        labels,
        source,
    }
}

/// Builds a `reqwest::blocking::Client` for one http_sd fetch, applying
/// `tls` the same way `crate::client::build_client` does (duplicated rather
/// than shared — that function is private to `client.rs` and its signature
/// takes a `send_timeout` this call site doesn't have; see
/// [`HTTP_SD_TIMEOUT`]).
fn build_http_client(tls: &TlsConfig) -> Result<reqwest::blocking::Client, ScrapeError> {
    let mut builder = reqwest::blocking::Client::builder().timeout(HTTP_SD_TIMEOUT);
    if tls.insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(ca_file) = &tls.ca_file {
        let pem = std::fs::read(ca_file)
            .map_err(|e| scrape_error(format!("cannot read CA file {ca_file:?}: {e}")))?;
        let cert = reqwest::Certificate::from_pem(&pem)
            .map_err(|e| scrape_error(format!("invalid CA certificate in {ca_file:?}: {e}")))?;
        builder = builder.add_root_certificate(cert);
    }
    if let (Some(cert_file), Some(key_file)) = (&tls.cert_file, &tls.key_file) {
        let mut identity_pem = std::fs::read(cert_file)
            .map_err(|e| scrape_error(format!("cannot read cert file {cert_file:?}: {e}")))?;
        let mut key_pem = std::fs::read(key_file)
            .map_err(|e| scrape_error(format!("cannot read key file {key_file:?}: {e}")))?;
        identity_pem.push(b'\n');
        identity_pem.append(&mut key_pem);
        let identity = reqwest::Identity::from_pem(&identity_pem)
            .map_err(|e| scrape_error(format!("invalid client cert/key: {e}")))?;
        builder = builder.identity(identity);
    }
    builder
        .build()
        .map_err(|e| scrape_error(format!("cannot build http_sd client: {e}")))
}

/// A pollable service-discovery source: the scrape manager (a later task)
/// calls [`Discovery::poll`] on a schedule and gets back the provider's
/// current target groups. [`Send`] so a manager can own a
/// `Vec<Box<dyn Discovery>>` across a worker thread boundary.
pub trait Discovery: Send {
    /// Returns the current target groups. Never fails — a provider that hit
    /// a transient error returns its last-good groups (or an empty `Vec` if
    /// it has never successfully refreshed) rather than propagating the
    /// error; see each implementation's doc for its retry cadence.
    fn poll(&mut self) -> Vec<TargetGroup>;
}

/// [`Discovery`] over a fixed `static_configs` list: computed once at
/// construction (a static config never changes at runtime), returned
/// unchanged on every `poll()`.
pub struct StaticDiscovery {
    groups: Vec<TargetGroup>,
}

impl StaticDiscovery {
    pub fn new(cfgs: &[StaticConfig], job: &str) -> Self {
        StaticDiscovery {
            groups: static_target_groups(cfgs, job),
        }
    }
}

impl Discovery for StaticDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        self.groups.clone()
    }
}

/// [`Discovery`] over `file_sd_configs`: re-reads `files` via
/// [`read_file_sd`] no more often than `refresh_interval`. The first
/// `poll()` always reads (the internal deadline starts at construction
/// time). On the (currently unreachable) `read_file_sd` `Err` path, a
/// warning is logged and the previous last-good groups are kept; either way
/// the deadline advances by `refresh_interval`, so a source is refreshed at
/// the configured cadence, not on every poll.
///
/// Because `read_file_sd` now logs+skips *every* per-pattern/per-file
/// problem (invalid glob pattern, unreadable file, malformed content, bad
/// extension) and returns `Ok` with whatever it could collect — see its doc
/// — a failed refresh normally surfaces here as a successful refresh to a
/// *smaller* (possibly empty) group list, not as a retained last-good. That
/// is deliberate: a target that genuinely disappeared (file deleted, edited
/// to zero targets, pattern now matching nothing) must propagate, not be
/// masked by a stale cache. See
/// `file_sd_discovery_drops_groups_when_sole_file_becomes_malformed` and
/// `file_sd_discovery_handles_invalid_glob_pattern_without_panicking` in the
/// tests below.
pub struct FileSdDiscovery {
    files: Vec<String>,
    refresh_interval: Duration,
    last_good: Vec<TargetGroup>,
    next_refresh: Instant,
    prefix: String,
}

impl FileSdDiscovery {
    pub fn new(cfg: &FileSdConfig, job: &str) -> Self {
        FileSdDiscovery {
            files: cfg.files.clone(),
            refresh_interval: cfg.refresh_interval,
            last_good: Vec::new(),
            next_refresh: Instant::now(),
            prefix: format!("{job}/file_sd"),
        }
    }
}

impl Discovery for FileSdDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        if Instant::now() >= self.next_refresh {
            match read_file_sd(&self.files) {
                Ok(groups) => self.last_good = groups,
                Err(e) => log::warn!(
                    "esmagent file_sd ({}): refresh failed, keeping last-good groups: {e}",
                    self.prefix
                ),
            }
            self.next_refresh = Instant::now() + self.refresh_interval;
        }
        self.last_good.clone()
    }
}

/// [`Discovery`] over `http_sd_configs`: re-fetches `cfg.url` via
/// [`fetch_http_sd`] no more often than `cfg.refresh_interval`. Same
/// first-poll-always-fetches / failure-keeps-last-good / deadline-still-
/// advances behavior as [`FileSdDiscovery`] — see its doc.
pub struct HttpSdDiscovery {
    cfg: HttpSdConfig,
    last_good: Vec<TargetGroup>,
    next_refresh: Instant,
    prefix: String,
}

impl HttpSdDiscovery {
    pub fn new(cfg: HttpSdConfig, job: &str) -> Self {
        HttpSdDiscovery {
            cfg,
            last_good: Vec::new(),
            next_refresh: Instant::now(),
            prefix: format!("{job}/http_sd"),
        }
    }
}

impl Discovery for HttpSdDiscovery {
    fn poll(&mut self) -> Vec<TargetGroup> {
        if Instant::now() >= self.next_refresh {
            match fetch_http_sd(&self.cfg) {
                Ok(groups) => self.last_good = groups,
                Err(e) => log::warn!(
                    "esmagent http_sd ({}): refresh failed, keeping last-good groups: {e}",
                    self.prefix
                ),
            }
            self.next_refresh = Instant::now() + self.cfg.refresh_interval;
        }
        self.last_good.clone()
    }
}

/// Builds a [`ScrapeError`] from a message. A free function rather than
/// `ScrapeError::new` — that inherent method already exists (private) in
/// `scrape::config`, and Rust forbids a second inherent `new` for the same
/// type in another module; `msg` is a `pub` field, so constructing the
/// struct literal directly is the crate-internal equivalent.
fn scrape_error(msg: impl Into<String>) -> ScrapeError {
    ScrapeError { msg: msg.into() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::AuthConfig;
    use esm_http::{Request, ResponseWriter, Server};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn static_groups_carry_labels() {
        let g = static_target_groups(
            &[StaticConfig {
                targets: vec!["h1:9100".into()],
                labels: [("team".to_string(), "infra".to_string())].into(),
            }],
            "node",
        );
        assert_eq!(g[0].targets, vec!["h1:9100".to_string()]);
        assert_eq!(g[0].labels["team"], "infra");
        assert_eq!(g[0].source, "node/static/0");
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

    #[test]
    fn reads_file_sd_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.yaml");
        std::fs::write(&p, "- targets: [\"h2:9100\"]\n  labels:\n    team: db\n").unwrap();
        let g = read_file_sd(&[p.to_string_lossy().into_owned()]).unwrap();
        assert_eq!(g[0].targets, vec!["h2:9100".to_string()]);
        assert_eq!(g[0].labels["team"], "db");
        assert_eq!(g[0].source, p.to_string_lossy().into_owned());
    }

    #[test]
    fn file_sd_expands_glob_across_multiple_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("a.json"),
            r#"[{"targets":["h1:9100"],"labels":{}}]"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("b.json"),
            r#"[{"targets":["h2:9100"],"labels":{}}]"#,
        )
        .unwrap();
        let pattern = dir.path().join("*.json").to_string_lossy().into_owned();
        let mut g = read_file_sd(&[pattern]).unwrap();
        g.sort_by(|a, b| a.targets.cmp(&b.targets));
        assert_eq!(g.len(), 2);
        assert_eq!(g[0].targets, vec!["h1:9100".to_string()]);
        assert_eq!(g[1].targets, vec!["h2:9100".to_string()]);
    }

    #[test]
    fn file_sd_skips_missing_and_malformed_files_without_failing() {
        let dir = tempfile::tempdir().unwrap();
        let good = dir.path().join("good.json");
        std::fs::write(&good, r#"[{"targets":["h1:9100"],"labels":{}}]"#).unwrap();
        let bad = dir.path().join("bad.json");
        std::fs::write(&bad, "not json").unwrap();
        let missing = dir
            .path()
            .join("missing.json")
            .to_string_lossy()
            .into_owned();

        let g = read_file_sd(&[
            good.to_string_lossy().into_owned(),
            bad.to_string_lossy().into_owned(),
            missing,
        ])
        .unwrap();
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].targets, vec!["h1:9100".to_string()]);
    }

    #[test]
    fn file_sd_bad_glob_pattern_does_not_discard_good_files() {
        // A syntactically invalid glob pattern (`"["`, an unterminated
        // character class) must be logged and skipped, NOT abort the whole
        // call — otherwise a valid file's groups collected before it (or
        // configured after it) would be silently discarded. Mirrors upstream
        // `FileSDConfig.appendScrapeWork` (config.go:1101-1109). This is the
        // regression test for that fix.
        let dir = tempfile::tempdir().unwrap();
        let good = dir.path().join("good.json");
        std::fs::write(&good, r#"[{"targets":["h1:9100"],"labels":{}}]"#).unwrap();

        let g = read_file_sd(&[good.to_string_lossy().into_owned(), "[".to_string()])
            .expect("a bad glob pattern must not turn the whole call into Err");
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].targets, vec!["h1:9100".to_string()]);

        // Order-independent: a bad pattern BEFORE the good file must not skip
        // the good file either.
        let g2 = read_file_sd(&["[".to_string(), good.to_string_lossy().into_owned()])
            .expect("a leading bad glob pattern must not discard later valid files");
        assert_eq!(g2.len(), 1);
        assert_eq!(g2[0].targets, vec!["h1:9100".to_string()]);
    }

    fn start_http_sd_stub(body: &'static str) -> (Server, Arc<AtomicUsize>) {
        let server = Server::bind("127.0.0.1:0").expect("bind http_sd stub");
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_for_handler = Arc::clone(&hits);
        server.serve(Arc::new(
            move |_req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
                hits_for_handler.fetch_add(1, Ordering::SeqCst);
                w.write_json(200, body);
            },
        ));
        (server, hits)
    }

    #[test]
    fn fetch_http_sd_parses_target_groups() {
        let (server, _hits) =
            start_http_sd_stub(r#"[{"targets":["h1:9100"],"labels":{"team":"infra"}}]"#);
        let addr = server.local_addr();
        let cfg = HttpSdConfig {
            url: format!("http://{addr}/sd"),
            refresh_interval: Duration::from_secs(60),
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
        };

        let g = fetch_http_sd(&cfg).unwrap();
        assert_eq!(g[0].targets, vec!["h1:9100".to_string()]);
        assert_eq!(g[0].labels["team"], "infra");
        assert_eq!(g[0].labels["__meta_url"], cfg.url);
        assert_eq!(g[0].source, cfg.url);

        server.stop();
    }

    #[test]
    fn fetch_http_sd_errors_on_non_success_status() {
        let server = Server::bind("127.0.0.1:0").expect("bind http_sd stub");
        server.serve(Arc::new(
            move |_req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
                w.write_status(500);
            },
        ));
        let addr = server.local_addr();
        let cfg = HttpSdConfig {
            url: format!("http://{addr}/sd"),
            refresh_interval: Duration::from_secs(60),
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
        };

        assert!(fetch_http_sd(&cfg).is_err());
        server.stop();
    }

    #[test]
    fn static_discovery_returns_same_groups_every_poll() {
        let mut d = StaticDiscovery::new(
            &[StaticConfig {
                targets: vec!["h1:9100".into()],
                labels: BTreeMap::new(),
            }],
            "node",
        );
        let first = d.poll();
        let second = d.poll();
        assert_eq!(first, second);
        assert_eq!(first[0].targets, vec!["h1:9100".to_string()]);
    }

    #[test]
    fn file_sd_discovery_caches_until_refresh_interval_elapses() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.json");
        std::fs::write(&p, r#"[{"targets":["h1:9100"],"labels":{}}]"#).unwrap();

        let cfg = FileSdConfig {
            files: vec![p.to_string_lossy().into_owned()],
            refresh_interval: Duration::from_millis(200),
        };
        let mut d = FileSdDiscovery::new(&cfg, "node");

        // First poll always refreshes.
        let first = d.poll();
        assert_eq!(first[0].targets, vec!["h1:9100".to_string()]);

        // Change the file, but poll again immediately: still cached.
        std::fs::write(&p, r#"[{"targets":["h2:9100"],"labels":{}}]"#).unwrap();
        let second = d.poll();
        assert_eq!(second[0].targets, vec!["h1:9100".to_string()]);

        // Wait past the refresh interval: the change is now picked up.
        thread::sleep(Duration::from_millis(250));
        let third = d.poll();
        assert_eq!(third[0].targets, vec!["h2:9100".to_string()]);
    }

    #[test]
    fn file_sd_discovery_drops_groups_when_sole_file_becomes_malformed() {
        // `read_file_sd` skips a malformed file (logs + continues) rather
        // than erroring the whole call — see its doc. So from
        // `FileSdDiscovery`'s point of view, a refresh where the only
        // configured file turns malformed legitimately yields zero groups
        // (`Ok(vec![])`), not an `Err` — the "keep last-good on error"
        // behavior below only intercepts a hard `read_file_sd` failure
        // (an invalid glob *pattern*, see
        // `file_sd_discovery_keeps_last_good_on_invalid_glob_pattern`), not
        // a single bad file's content. This asserts that documented
        // (non-retaining) behavior so a future change doesn't silently
        // start masking real "someone deleted the targets" refreshes.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.json");
        std::fs::write(&p, r#"[{"targets":["h1:9100"],"labels":{}}]"#).unwrap();

        let cfg = FileSdConfig {
            files: vec![p.to_string_lossy().into_owned()],
            refresh_interval: Duration::from_millis(50),
        };
        let mut d = FileSdDiscovery::new(&cfg, "node");
        let first = d.poll();
        assert_eq!(first[0].targets, vec!["h1:9100".to_string()]);

        std::fs::write(&p, "not json").unwrap();
        thread::sleep(Duration::from_millis(60));
        let second = d.poll();
        assert!(second.is_empty());
    }

    #[test]
    fn file_sd_discovery_handles_invalid_glob_pattern_without_panicking() {
        // A syntactically invalid glob pattern is now logged+skipped by
        // `read_file_sd` (returning `Ok(vec![])`), not propagated as `Err`
        // (see `file_sd_bad_glob_pattern_does_not_discard_good_files`). So
        // `FileSdDiscovery` sees an ordinary empty refresh — no groups, and
        // crucially no panic — across repeated polls spanning the refresh
        // interval.
        let cfg = FileSdConfig {
            files: vec!["[".to_string()],
            refresh_interval: Duration::from_millis(20),
        };
        let mut d = FileSdDiscovery::new(&cfg, "node");
        assert!(d.poll().is_empty());
        thread::sleep(Duration::from_millis(30));
        assert!(d.poll().is_empty());
    }

    #[test]
    fn http_sd_discovery_caches_until_refresh_interval_elapses() {
        let (server, hits) = start_http_sd_stub(r#"[{"targets":["h1:9100"],"labels":{}}]"#);
        let addr = server.local_addr();
        let cfg = HttpSdConfig {
            url: format!("http://{addr}/sd"),
            refresh_interval: Duration::from_millis(200),
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
        };
        let mut d = HttpSdDiscovery::new(cfg, "node");

        let first = d.poll();
        assert_eq!(first[0].targets, vec!["h1:9100".to_string()]);
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        // Immediate second poll: still cached, no extra HTTP hit.
        let second = d.poll();
        assert_eq!(second, first);
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        server.stop();
    }
}
