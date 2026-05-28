//! `esm-agent` — Phase 5 MVP.
//!
//! Scrapes one or more HTTP `/metrics` endpoints at a fixed interval and
//! forwards the raw Prometheus text exposition body to a remote-write target
//! via `POST /api/v1/import/prometheus`.
//!
//! Full vmagent parity (relabeling, persistent queue, service discovery
//! beyond `static`, multiple remote-write targets with retry policy) lands in
//! subsequent sub-phases. The MVP is the smallest loop that demonstrates the
//! agent can pull from a real exporter and feed `esm-single`.

#![allow(clippy::print_stderr)]

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "esm-agent",
    about = "Scrape Prometheus-compatible endpoints and forward to a remote-write target.",
    version
)]
struct Cli {
    /// Path to a vmagent-style `promscrape.config` YAML.
    /// When present, `scrape_configs.*.static_configs.*.targets` are merged
    /// into the scrape target list. Per-target relabel is honored.
    #[arg(long)]
    config: Option<PathBuf>,

    /// One scrape URL per `--scrape-url` flag. Repeat for multiple targets.
    #[arg(long = "scrape-url")]
    scrape_urls: Vec<String>,

    /// Scrape interval in seconds.
    #[arg(long, default_value_t = 15)]
    scrape_interval_secs: u64,

    /// Remote-write target. Must accept `POST /api/v1/import/prometheus`.
    #[arg(long, default_value = "http://127.0.0.1:8428")]
    remote_write_url: String,

    /// HTTP request timeout in seconds (applies to both scrapes and
    /// forwards).
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,

    /// On-disk queue directory. Forwarded bodies that fail to upload are
    /// buffered here and retried on later ticks. `None` disables the queue
    /// (failed forwards are dropped).
    #[arg(long)]
    queue_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();

    let mut scrape_targets: Vec<(String, std::sync::Arc<Vec<esm_scrape::relabel::RelabelRule>>)> =
        cli.scrape_urls.iter().map(|u| (u.clone(), std::sync::Arc::new(Vec::new()))).collect();
    if let Some(ref c) = cli.config {
        let raw = std::fs::read_to_string(c).with_context(|| format!("read {}", c.display()))?;
        let cfg: PromscrapeConfig =
            serde_yaml_ng::from_str(&raw).context("parse promscrape config")?;
        for sc in &cfg.scrape_configs {
            let path = sc.metrics_path.as_deref().unwrap_or("/metrics");
            let rules = std::sync::Arc::new(
                sc.metric_relabel_configs
                    .iter()
                    .map(compile_relabel)
                    .collect::<Result<Vec<_>>>()
                    .context("compile metric_relabel_configs")?,
            );
            for stat in &sc.static_configs {
                for t in &stat.targets {
                    scrape_targets.push((target_to_url(t, path), rules.clone()));
                }
            }
            for fsd in &sc.file_sd_configs {
                for pattern in &fsd.files {
                    for file in expand_glob(pattern)? {
                        for t in load_file_sd_targets(&file)? {
                            scrape_targets.push((target_to_url(&t, path), rules.clone()));
                        }
                    }
                }
            }
        }
        tracing::info!(targets = scrape_targets.len(), "loaded promscrape config");
    }
    if scrape_targets.is_empty() {
        bail!("no scrape URLs provided; pass --scrape-url or --config");
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(cli.http_timeout_secs))
        .user_agent("esm-agent/0.0.0")
        .build()
        .context("build HTTP client")?;

    let shutdown = tokio::sync::Notify::new();
    let shutdown = std::sync::Arc::new(shutdown);
    let shutdown_listener = shutdown.clone();
    tokio::spawn(async move {
        let _ = esm_platform::signal::wait_for_shutdown().await;
        tracing::info!("shutdown signal received");
        shutdown_listener.notify_waiters();
    });

    let queue = if let Some(qd) = cli.queue_dir.clone() {
        Some(std::sync::Arc::new(DiskQueue::open(qd).context("open disk queue")?))
    } else {
        None
    };

    let mut handles = Vec::new();
    for (url, rules) in scrape_targets {
        let client = client.clone();
        let remote = cli.remote_write_url.clone();
        let interval = Duration::from_secs(cli.scrape_interval_secs);
        let shutdown = shutdown.clone();
        let queue = queue.clone();
        let h = tokio::spawn(async move {
            scrape_loop(&client, &url, &remote, interval, shutdown, queue.as_deref(), &rules).await;
        });
        handles.push(h);
    }

    for h in handles {
        let _ = h.await;
    }
    tracing::info!("agent shutdown complete");
    Ok(())
}

/// Append-only file-per-entry queue: each queued body is `queue/<seq>.bin`.
/// Forward attempts pop in seq order; on failure the body is re-enqueued.
struct DiskQueue {
    dir: PathBuf,
    next_seq: std::sync::atomic::AtomicU64,
}

impl DiskQueue {
    fn open(dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("create queue dir {}", dir.display()))?;
        // Find the highest existing seq.
        let mut max_seq = 0u64;
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            if let Some(stem) = entry
                .path()
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
            {
                max_seq = max_seq.max(stem);
            }
        }
        Ok(Self { dir, next_seq: std::sync::atomic::AtomicU64::new(max_seq + 1) })
    }

    fn push(&self, body: &str) -> Result<()> {
        let seq = self.next_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = self.dir.join(format!("{seq:020}.bin"));
        let tmp = self.dir.join(format!("{seq:020}.bin.tmp"));
        std::fs::write(&tmp, body)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Drain queued entries, calling `forward(body)` for each in seq order.
    /// Stops on the first forward failure (leaving the entry in place).
    async fn drain<F, Fut>(&self, mut forward: F) -> Result<usize>
    where
        F: FnMut(String) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let mut entries: Vec<(u64, PathBuf)> = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(stem) =
                path.file_stem().and_then(|s| s.to_str()).and_then(|s| s.parse::<u64>().ok())
                && path.extension().is_some_and(|e| e == "bin")
            {
                entries.push((stem, path));
            }
        }
        entries.sort_by_key(|(seq, _)| *seq);
        let mut drained = 0;
        for (_, path) in entries {
            let body = std::fs::read_to_string(&path)?;
            match forward(body).await {
                Ok(()) => {
                    std::fs::remove_file(&path)?;
                    drained += 1;
                }
                Err(e) => {
                    tracing::debug!(path = %path.display(), error = %e, "queue drain blocked");
                    break;
                }
            }
        }
        Ok(drained)
    }
}

#[derive(Debug, serde::Deserialize)]
struct PromscrapeConfig {
    #[serde(default)]
    scrape_configs: Vec<PromscrapeJob>,
}

#[derive(Debug, serde::Deserialize)]
struct PromscrapeJob {
    #[serde(default)]
    #[allow(dead_code)]
    job_name: String,
    #[serde(default)]
    metrics_path: Option<String>,
    #[serde(default)]
    static_configs: Vec<StaticConfig>,
    #[serde(default)]
    file_sd_configs: Vec<FileSdConfig>,
    #[serde(default)]
    metric_relabel_configs: Vec<RelabelConfigYaml>,
}

#[derive(Debug, serde::Deserialize)]
struct RelabelConfigYaml {
    #[serde(default)]
    source_labels: Vec<String>,
    #[serde(default = "default_separator")]
    separator: String,
    #[serde(default)]
    target_label: Option<String>,
    #[serde(default = "default_regex")]
    regex: String,
    #[serde(default)]
    modulus: u64,
    #[serde(default = "default_replacement")]
    replacement: String,
    #[serde(default = "default_action")]
    action: String,
}

fn default_separator() -> String {
    ";".to_string()
}
fn default_regex() -> String {
    "(.*)".to_string()
}
fn default_replacement() -> String {
    "$1".to_string()
}
fn default_action() -> String {
    "replace".to_string()
}

#[derive(Debug, serde::Deserialize)]
struct StaticConfig {
    #[serde(default)]
    targets: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
struct FileSdConfig {
    #[serde(default)]
    files: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
struct FileSdEntry {
    #[serde(default)]
    targets: Vec<String>,
}

fn compile_relabel(y: &RelabelConfigYaml) -> Result<esm_scrape::relabel::RelabelRule> {
    let action = esm_scrape::relabel::Action::from_name(&y.action)
        .with_context(|| format!("unknown relabel action {:?}", y.action))?;
    let regex = regex::Regex::new(&y.regex).with_context(|| format!("bad regex {:?}", y.regex))?;
    Ok(esm_scrape::relabel::RelabelRule {
        source_labels: y.source_labels.clone(),
        separator: y.separator.clone(),
        target_label: y.target_label.clone(),
        regex,
        modulus: y.modulus,
        replacement: y.replacement.clone(),
        action,
    })
}

/// Re-emit a Prometheus text exposition document after applying the
/// relabel chain to each series. Unparseable lines and series filtered
/// out by `keep`/`drop` are handled gracefully — malformed bodies pass
/// through verbatim rather than getting dropped.
#[allow(clippy::unnecessary_wraps)]
fn relabel_text_exposition(
    body: &str,
    rules: &[esm_scrape::relabel::RelabelRule],
) -> Option<String> {
    let mut out = String::with_capacity(body.len());
    for raw in body.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            out.push_str(raw);
            out.push('\n');
            continue;
        }
        let Some((name, mut labels, rest)) = parse_text_line(line) else {
            // Unparseable line — pass it through verbatim so we don't drop data.
            out.push_str(raw);
            out.push('\n');
            continue;
        };
        labels.insert("__name__".to_string(), name);
        let Some(new_labels) = esm_scrape::relabel::apply(rules, labels) else {
            // `keep` / `drop` filtered this series out.
            continue;
        };
        let Some(new_name) = new_labels.get("__name__").cloned() else {
            continue;
        };
        let mut display_labels = new_labels;
        display_labels.remove("__name__");
        out.push_str(&new_name);
        if !display_labels.is_empty() {
            out.push('{');
            for (i, (k, v)) in display_labels.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(k);
                out.push_str("=\"");
                out.push_str(v);
                out.push('"');
            }
            out.push('}');
        }
        out.push(' ');
        out.push_str(&rest);
        out.push('\n');
    }
    Some(out)
}

/// Parse a single text-exposition line into `(name, labels, value_and_ts)`.
/// Returns `None` if the line is malformed.
fn parse_text_line(
    line: &str,
) -> Option<(String, std::collections::BTreeMap<String, String>, String)> {
    let mut labels = std::collections::BTreeMap::new();
    let (name, after_name) = read_text_name(line)?;
    let after_name = after_name.trim_start();
    let after_labels = if let Some(rest) = after_name.strip_prefix('{') {
        let (parsed, rest) = read_label_block(rest)?;
        labels.extend(parsed);
        rest
    } else {
        after_name
    };
    Some((name, labels, after_labels.trim().to_string()))
}

fn read_text_name(line: &str) -> Option<(String, &str)> {
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_alphanumeric() || b == b'_' || b == b':' {
            i += 1;
        } else {
            break;
        }
    }
    if i == 0 {
        return None;
    }
    Some((line[..i].to_string(), &line[i..]))
}

fn read_label_block(src: &str) -> Option<(std::collections::BTreeMap<String, String>, &str)> {
    let mut out = std::collections::BTreeMap::new();
    let bytes = src.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'}' {
            return Some((out, &src[i + 1..]));
        }
        if b == b',' || b == b' ' {
            i += 1;
            continue;
        }
        // Label name.
        let name_start = i;
        while i < bytes.len() {
            let bb = bytes[i];
            if bb.is_ascii_alphanumeric() || bb == b'_' {
                i += 1;
            } else {
                break;
            }
        }
        if i == name_start {
            return None;
        }
        let name = src[name_start..i].to_string();
        if i >= bytes.len() || bytes[i] != b'=' {
            return None;
        }
        i += 1;
        if i >= bytes.len() || bytes[i] != b'"' {
            return None;
        }
        i += 1;
        let val_start = i;
        while i < bytes.len() && bytes[i] != b'"' {
            // Minimal escape handling — Prometheus only requires \\, \" , \n.
            if bytes[i] == b'\\' && i + 1 < bytes.len() {
                i += 2;
            } else {
                i += 1;
            }
        }
        if i >= bytes.len() {
            return None;
        }
        let raw = &src[val_start..i];
        let value = raw.replace("\\\\", "\\").replace("\\\"", "\"").replace("\\n", "\n");
        out.insert(name, value);
        i += 1; // consume closing quote
    }
    None
}

fn target_to_url(t: &str, path: &str) -> String {
    if t.contains("://") { t.to_string() } else { format!("http://{t}{path}") }
}

fn expand_glob(pattern: &str) -> Result<Vec<std::path::PathBuf>> {
    // Tiny glob: only supports `*` in the filename (no `**`, no `?`, no `[]`).
    // Sufficient for the Prometheus file_sd convention (`/etc/prom/targets/*.json`).
    let path = std::path::Path::new(pattern);
    let Some(parent) = path.parent() else {
        return Ok(vec![path.to_path_buf()]);
    };
    let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
        return Ok(vec![path.to_path_buf()]);
    };
    if !name.contains('*') {
        return Ok(vec![path.to_path_buf()]);
    }
    let parent = if parent.as_os_str().is_empty() { std::path::Path::new(".") } else { parent };
    let Ok(entries) = std::fs::read_dir(parent) else {
        return Ok(Vec::new());
    };
    let (prefix, suffix) = name.split_once('*').map_or((name, ""), |(p, s)| (p, s));
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry?;
        let n = entry.file_name();
        let Some(s) = n.to_str() else { continue };
        if s.starts_with(prefix) && s.ends_with(suffix) && s.len() >= prefix.len() + suffix.len() {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}

fn load_file_sd_targets(path: &std::path::Path) -> Result<Vec<String>> {
    let body = std::fs::read_to_string(path)
        .with_context(|| format!("read file_sd file {}", path.display()))?;
    let entries: Vec<FileSdEntry> = if path.extension().and_then(|e| e.to_str()) == Some("yaml")
        || path.extension().and_then(|e| e.to_str()) == Some("yml")
    {
        serde_yaml_ng::from_str(&body).context("parse file_sd yaml")?
    } else {
        serde_json::from_str(&body).context("parse file_sd json")?
    };
    let mut targets = Vec::new();
    for e in entries {
        targets.extend(e.targets);
    }
    Ok(targets)
}

fn init_tracing() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .finish();
    if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
        eprintln!("warning: tracing init failed: {e}");
    }
}

async fn scrape_loop(
    client: &reqwest::Client,
    url: &str,
    remote_write_url: &str,
    interval: Duration,
    shutdown: std::sync::Arc<tokio::sync::Notify>,
    queue: Option<&DiskQueue>,
    relabel_rules: &[esm_scrape::relabel::RelabelRule],
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                // First, drain any queued entries.
                if let Some(q) = queue {
                    let forward_url = format!("{remote_write_url}/api/v1/import/prometheus");
                    let c = client.clone();
                    let url_ref = forward_url.clone();
                    let drained = q.drain(|body| {
                        let c = c.clone();
                        let url = url_ref.clone();
                        async move { forward_body(&c, &url, body).await }
                    }).await;
                    match drained {
                        Ok(n) if n > 0 => tracing::info!(target = %url, drained = n, "queue drained"),
                        Ok(_) => {}
                        Err(e) => tracing::warn!(error = %e, "queue drain failed"),
                    }
                }
                if let Err(e) = scrape_once(client, url, remote_write_url, queue, relabel_rules).await {
                    tracing::warn!(target = %url, error = %e, "scrape failed");
                }
            }
            () = shutdown.notified() => {
                tracing::info!(target = %url, "scrape loop stopping");
                break;
            }
        }
    }
}

async fn scrape_once(
    client: &reqwest::Client,
    url: &str,
    remote_write_url: &str,
    queue: Option<&DiskQueue>,
    relabel_rules: &[esm_scrape::relabel::RelabelRule],
) -> Result<()> {
    let resp = client.get(url).send().await.context("scrape GET")?;
    let status = resp.status();
    if !status.is_success() {
        bail!("scrape returned HTTP {status}");
    }
    let body = resp.text().await.context("read scrape body")?;
    let body = if relabel_rules.is_empty() {
        body
    } else {
        relabel_text_exposition(&body, relabel_rules).unwrap_or(body)
    };
    let forward_url = format!("{remote_write_url}/api/v1/import/prometheus");
    match forward_body(client, &forward_url, body.clone()).await {
        Ok(()) => {
            tracing::debug!(target = %url, forward = %forward_url, "scrape+forward ok");
            Ok(())
        }
        Err(e) => {
            if let Some(q) = queue {
                if let Err(qe) = q.push(&body) {
                    tracing::warn!(error = %qe, "queue push failed");
                } else {
                    tracing::info!(target = %url, "forward failed; queued");
                }
            }
            Err(e)
        }
    }
}

async fn forward_body(client: &reqwest::Client, forward_url: &str, body: String) -> Result<()> {
    let fr = client
        .post(forward_url)
        .header("Content-Type", "text/plain")
        .body(body)
        .send()
        .await
        .context("forward POST")?;
    let fs = fr.status();
    if !fs.is_success() {
        bail!("forward returned HTTP {fs}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use esm_scrape::relabel::{Action, RelabelRule};

    fn drop_rule(label: &str, regex: &str) -> RelabelRule {
        RelabelRule {
            source_labels: vec![label.into()],
            separator: ";".into(),
            target_label: None,
            regex: regex::Regex::new(regex).unwrap(),
            modulus: 0,
            replacement: "$1".into(),
            action: Action::Drop,
        }
    }

    #[test]
    fn drop_action_removes_matching_series() {
        let body = "noisy_metric{path=\"/health\"} 1 1700000000000\n\
                    quiet_metric{path=\"/data\"} 2 1700000000000\n";
        let rules = [drop_rule("path", "/health")];
        let out = relabel_text_exposition(body, &rules).unwrap();
        assert!(!out.contains("noisy_metric"));
        assert!(out.contains("quiet_metric"));
    }

    #[test]
    fn passthrough_when_rules_empty() {
        let body = "m{l=\"v\"} 1 1700000000000\n";
        let out = relabel_text_exposition(body, &[]).unwrap();
        assert!(out.contains("m{l=\"v\"}"));
    }

    #[test]
    fn parse_text_line_basic() {
        let (name, labels, rest) = parse_text_line("m{a=\"1\",b=\"2\"} 3 4").unwrap();
        assert_eq!(name, "m");
        assert_eq!(labels.get("a").map(String::as_str), Some("1"));
        assert_eq!(labels.get("b").map(String::as_str), Some("2"));
        assert_eq!(rest, "3 4");
    }
}
