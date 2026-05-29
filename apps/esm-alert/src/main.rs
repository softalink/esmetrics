//! `esm-alert` — Phase 6 MVP.
//!
//! Loads a YAML rule file in the Prometheus / vmalert style and evaluates
//! each rule against an esm-single (or VM) HTTP query endpoint at a fixed
//! interval. Firing alerts are POSTed as JSON to a configured Alertmanager
//! v2 URL.
//!
//! The MVP does NOT implement PromQL evaluation locally — it queries the
//! configured backend using its native API (`/api/v1/query?metric=...`,
//! the same surface esm-single exposes today). Once `esm-promql` gains a
//! real evaluator the rules will route through it; the YAML schema does
//! not need to change.

#![allow(clippy::print_stderr)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_precision_loss)]

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug)]
#[command(name = "esm-alert", about = "Alerting rule evaluator (Phase 6 MVP).", version)]
struct Cli {
    /// YAML rule file.
    #[arg(long)]
    rule_file: PathBuf,
    /// esm-single / VictoriaMetrics URL exposing `/api/v1/query`.
    /// Ignored when `--local-data-path` is set.
    #[arg(long, default_value = "http://127.0.0.1:8428")]
    datasource_url: String,
    /// If set, evaluate rules against a local EsMetrics data directory using
    /// the embedded PromQL evaluator instead of issuing HTTP queries.
    /// Acquires the data dir's exclusive lock — point this at a snapshot
    /// when esm-single is running on the live dir.
    #[arg(long)]
    local_data_path: Option<PathBuf>,
    /// Alertmanager v2 endpoint.
    #[arg(long)]
    alertmanager_url: String,
    /// Evaluation interval (seconds).
    #[arg(long, default_value_t = 30)]
    evaluation_interval_secs: u64,
    /// Path to persist alert state across restarts. State is rewritten
    /// atomically after each evaluation tick. Omit to keep state purely
    /// in-memory.
    #[arg(long)]
    state_file: Option<PathBuf>,
    /// Optional URL accepting `POST /api/v1/import/prometheus`. Recording-rule
    /// outputs are forwarded here on every evaluation tick.
    #[arg(long)]
    remote_write_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RuleFile {
    #[serde(default)]
    groups: Vec<RuleGroup>,
}

#[derive(Debug, Deserialize)]
struct RuleGroup {
    name: String,
    #[serde(default)]
    rules: Vec<Rule>,
    /// Recording rules — evaluated each tick, results forwarded to
    /// `--remote-write-url` if set. Names become the canonical metric
    /// name of the output series.
    #[serde(default)]
    recording_rules: Vec<RecordingRule>,
}

#[derive(Debug, Deserialize, Clone)]
struct RecordingRule {
    /// Output metric name.
    record: String,
    /// PromQL expression to evaluate.
    expr: String,
    #[serde(default)]
    labels: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Deserialize, Clone)]
struct Rule {
    alert: String,
    /// PromQL expression (passed through `/api/v1/promql`).
    metric: String,
    /// Trigger when the most recent sample value exceeds this threshold.
    threshold: i64,
    #[serde(default)]
    labels: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    annotations: std::collections::BTreeMap<String, String>,
    /// Duration (seconds) the condition must hold before the alert fires.
    /// Matches Prometheus' `for:` clause.
    #[serde(default)]
    for_secs: u64,
    /// Duration (seconds) the alert continues firing after the condition
    /// stops being true. Matches Prometheus' `keep_firing_for:`.
    #[serde(default)]
    keep_firing_for_secs: u64,
}

#[derive(Debug, Serialize)]
struct AlertmanagerAlert<'a> {
    labels: std::collections::BTreeMap<&'a str, &'a str>,
    annotations: std::collections::BTreeMap<&'a str, &'a str>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();

    let raw = std::fs::read_to_string(&cli.rule_file)
        .with_context(|| format!("read {}", cli.rule_file.display()))?;
    let rule_file: RuleFile = serde_yaml_ng::from_str(&raw).context("parse rule file")?;
    let total_rules: usize = rule_file.groups.iter().map(|g| g.rules.len()).sum();
    tracing::info!(rules = total_rules, "loaded rule file");

    let client = reqwest::Client::builder()
        .user_agent("esm-alert/0.0.0")
        .timeout(Duration::from_secs(10))
        .build()
        .context("build HTTP client")?;

    let shutdown = std::sync::Arc::new(tokio::sync::Notify::new());
    let s = shutdown.clone();
    tokio::spawn(async move {
        let _ = esm_platform::signal::wait_for_shutdown().await;
        s.notify_waiters();
    });

    let local_storage = if let Some(data_path) = cli.local_data_path.as_ref() {
        let s = esm_storage::Storage::open(data_path)
            .with_context(|| format!("open local data dir {}", data_path.display()))?;
        tracing::info!(data_dir = %data_path.display(), "local PromQL evaluator active");
        Some(std::sync::Arc::new(tokio::sync::Mutex::new(s)))
    } else {
        None
    };

    let mut ticker = tokio::time::interval(Duration::from_secs(cli.evaluation_interval_secs));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut state: std::collections::HashMap<String, AlertState> = match cli.state_file.as_ref() {
        Some(path) if path.exists() => match load_persisted_state(path) {
            Ok(s) => {
                tracing::info!(alerts = s.len(), file = %path.display(), "loaded alert state");
                s
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to load alert state; starting fresh");
                std::collections::HashMap::new()
            }
        },
        _ => std::collections::HashMap::new(),
    };
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let result = if let Some(storage) = local_storage.as_ref() {
                    evaluate_local(storage.clone(), &client, &cli.alertmanager_url, &rule_file, &mut state).await
                } else {
                    evaluate(&client, &cli.datasource_url, &cli.alertmanager_url, &rule_file, &mut state).await
                };
                if let Err(e) = result {
                    tracing::warn!(error = %e, "evaluation failed");
                }
                if let (Some(rw_url), Some(storage)) = (cli.remote_write_url.as_ref(), local_storage.as_ref())
                    && let Err(e) = evaluate_recording_rules(storage.clone(), &client, rw_url, &rule_file).await
                {
                    tracing::warn!(error = %e, "recording-rule evaluation failed");
                }
                if let Some(path) = cli.state_file.as_ref()
                    && let Err(e) = persist_state(path, &state)
                {
                    tracing::warn!(error = %e, "failed to persist alert state");
                }
            }
            () = shutdown.notified() => {
                tracing::info!("shutdown signal received");
                break;
            }
        }
    }
    Ok(())
}

async fn evaluate_recording_rules(
    storage: std::sync::Arc<tokio::sync::Mutex<esm_storage::Storage>>,
    client: &reqwest::Client,
    remote_write_url: &str,
    rule_file: &RuleFile,
) -> Result<()> {
    let now_ms = chrono_like_now_ms();
    let ctx = esm_promql::EvalContext::instant(now_ms);
    let mut lines = String::new();
    for group in &rule_file.groups {
        for rr in &group.recording_rules {
            let expr = match esm_promql::parser::parse(&rr.expr) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(rule = %rr.record, error = %e, "recording rule parse failed");
                    continue;
                }
            };
            let value = {
                let s = storage.lock().await;
                esm_promql::evaluator::evaluate(&expr, &*s, ctx)
            };
            let value = match value {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(rule = %rr.record, error = %e, "recording rule eval failed");
                    continue;
                }
            };
            match value {
                esm_promql::Value::Scalar(s) => {
                    lines.push_str(&format_record_line(&rr.record, &rr.labels, s, now_ms));
                }
                esm_promql::Value::InstantVector(elems) => {
                    for e in elems {
                        let mut labels = rr.labels.clone();
                        if let Ok(name) = std::str::from_utf8(&e.metric_name)
                            && let Some(start) = name.find('{')
                            && let Some(end) = name.rfind('}')
                            && end > start
                        {
                            for pair in name[start + 1..end].split(',') {
                                if let Some(eq) = pair.find('=') {
                                    let k = pair[..eq].trim().to_string();
                                    let v = pair[eq + 1..].trim().trim_matches('"').to_string();
                                    labels.entry(k).or_insert(v);
                                }
                            }
                        }
                        lines.push_str(&format_record_line(&rr.record, &labels, e.value, now_ms));
                    }
                }
            }
        }
    }
    if lines.is_empty() {
        return Ok(());
    }
    let url = format!("{}/api/v1/import/prometheus", remote_write_url.trim_end_matches('/'));
    let resp = client.post(&url).body(lines).send().await.context("remote-write POST")?;
    if !resp.status().is_success() {
        bail!("remote-write returned {}", resp.status());
    }
    Ok(())
}

fn format_record_line(
    name: &str,
    labels: &std::collections::BTreeMap<String, String>,
    value: f64,
    ts_ms: i64,
) -> String {
    use std::fmt::Write as _;
    let mut s = String::new();
    s.push_str(name);
    if !labels.is_empty() {
        s.push('{');
        for (i, (k, v)) in labels.iter().enumerate() {
            if i > 0 {
                s.push(',');
            }
            let _ = write!(s, "{k}=\"{v}\"");
        }
        s.push('}');
    }
    let _ = writeln!(s, " {value} {ts_ms}");
    s
}

async fn evaluate_local(
    storage: std::sync::Arc<tokio::sync::Mutex<esm_storage::Storage>>,
    client: &reqwest::Client,
    alertmanager_url: &str,
    rule_file: &RuleFile,
    state: &mut std::collections::HashMap<String, AlertState>,
) -> Result<()> {
    let now = std::time::Instant::now();
    let now_ms = chrono_like_now_ms();
    for group in &rule_file.groups {
        for rule in &group.rules {
            let key = format!("{}/{}", group.name, rule.alert);
            let expr = esm_promql::parser::parse(&rule.metric)
                .map_err(|e| anyhow::anyhow!("parse rule {}: {e}", rule.alert))?;
            let ctx = esm_promql::EvalContext::instant(now_ms);
            let storage_guard = storage.lock().await;
            let value = esm_promql::evaluator::evaluate(&expr, &*storage_guard, ctx)
                .map_err(|e| anyhow::anyhow!("evaluate rule {}: {e}", rule.alert))?;
            drop(storage_guard);
            let firing_now = match &value {
                esm_promql::Value::Scalar(n) => *n > rule.threshold as f64,
                esm_promql::Value::InstantVector(elems) => {
                    elems.iter().any(|e| e.value > rule.threshold as f64)
                }
            };
            let sample_value = match &value {
                esm_promql::Value::Scalar(n) => *n as i64,
                esm_promql::Value::InstantVector(elems) => {
                    elems.first().map_or(0, |e| e.value as i64)
                }
            };
            advance_state(
                state,
                &key,
                &group.name,
                rule,
                now,
                firing_now,
                sample_value,
                client,
                alertmanager_url,
            )
            .await?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn advance_state(
    state: &mut std::collections::HashMap<String, AlertState>,
    key: &str,
    group_name: &str,
    rule: &Rule,
    now: std::time::Instant,
    firing_now: bool,
    sample_value: i64,
    client: &reqwest::Client,
    alertmanager_url: &str,
) -> Result<()> {
    let entry = state.entry(key.to_string()).or_insert_with(|| AlertState {
        phase: AlertPhase::Pending,
        active_since: now,
        last_active: now,
        notified: false,
    });
    if firing_now {
        entry.last_active = now;
        match entry.phase {
            AlertPhase::Pending => {
                let dwell = now.duration_since(entry.active_since).as_secs();
                if dwell >= rule.for_secs {
                    entry.phase = AlertPhase::Firing;
                    if !entry.notified {
                        fire_alert(client, alertmanager_url, group_name, rule, sample_value)
                            .await?;
                        entry.notified = true;
                    }
                }
            }
            AlertPhase::Firing => {}
        }
    } else {
        let cool = rule.keep_firing_for_secs;
        let since_last = now.duration_since(entry.last_active).as_secs();
        if matches!(entry.phase, AlertPhase::Firing) && since_last < cool {
        } else {
            *entry = AlertState {
                phase: AlertPhase::Pending,
                active_since: now,
                last_active: now,
                notified: false,
            };
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AlertPhaseSer {
    Pending,
    Firing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AlertStateSer {
    phase: AlertPhaseSer,
    active_since_ms: i64,
    last_active_ms: i64,
    notified: bool,
}

#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_possible_wrap)]
fn persist_state(
    path: &std::path::Path,
    state: &std::collections::HashMap<String, AlertState>,
) -> Result<()> {
    let now_ms = chrono_like_now_ms();
    let now_inst = std::time::Instant::now();
    let serializable: std::collections::HashMap<String, AlertStateSer> = state
        .iter()
        .map(|(k, v)| {
            let active_ago = now_inst.saturating_duration_since(v.active_since).as_millis() as i64;
            let last_ago = now_inst.saturating_duration_since(v.last_active).as_millis() as i64;
            (
                k.clone(),
                AlertStateSer {
                    phase: match v.phase {
                        AlertPhase::Pending => AlertPhaseSer::Pending,
                        AlertPhase::Firing => AlertPhaseSer::Firing,
                    },
                    active_since_ms: now_ms - active_ago,
                    last_active_ms: now_ms - last_ago,
                    notified: v.notified,
                },
            )
        })
        .collect();
    let bytes = serde_json::to_vec_pretty(&serializable)?;
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, &bytes).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_possible_truncation)]
fn load_persisted_state(
    path: &std::path::Path,
) -> Result<std::collections::HashMap<String, AlertState>> {
    let raw = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let serializable: std::collections::HashMap<String, AlertStateSer> =
        serde_json::from_slice(&raw).context("parse alert state")?;
    let now_ms = chrono_like_now_ms();
    let now_inst = std::time::Instant::now();
    let mut out = std::collections::HashMap::new();
    for (k, v) in serializable {
        let active_ago_ms = (now_ms - v.active_since_ms).max(0);
        let last_ago_ms = (now_ms - v.last_active_ms).max(0);
        out.insert(
            k,
            AlertState {
                phase: match v.phase {
                    AlertPhaseSer::Pending => AlertPhase::Pending,
                    AlertPhaseSer::Firing => AlertPhase::Firing,
                },
                active_since: now_inst
                    .checked_sub(std::time::Duration::from_millis(active_ago_ms as u64))
                    .unwrap_or(now_inst),
                last_active: now_inst
                    .checked_sub(std::time::Duration::from_millis(last_ago_ms as u64))
                    .unwrap_or(now_inst),
                notified: v.notified,
            },
        );
    }
    Ok(out)
}

fn chrono_like_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    i64::try_from(dur.as_millis()).unwrap_or(i64::MAX)
}

#[derive(Debug, Clone, Copy)]
enum AlertPhase {
    Pending,
    Firing,
}

#[derive(Debug, Clone)]
struct AlertState {
    phase: AlertPhase,
    /// Wall-clock time the rule first became active or last condition-true.
    active_since: std::time::Instant,
    /// Last tick at which the condition was observed true.
    last_active: std::time::Instant,
    /// Whether the firing transition has been pushed to Alertmanager.
    notified: bool,
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

async fn evaluate(
    client: &reqwest::Client,
    datasource_url: &str,
    alertmanager_url: &str,
    rule_file: &RuleFile,
    state: &mut std::collections::HashMap<String, AlertState>,
) -> Result<()> {
    let now = std::time::Instant::now();
    for group in &rule_file.groups {
        for rule in &group.rules {
            let key = format!("{}/{}", group.name, rule.alert);
            let query_url = format!(
                "{}/api/v1/promql?query={}",
                datasource_url.trim_end_matches('/'),
                urlencode(&rule.metric)
            );
            let resp = client.get(&query_url).send().await.context("query")?;
            if !resp.status().is_success() {
                bail!("query returned HTTP {}", resp.status());
            }
            let body: PromqlEnvelope = resp.json().await.context("parse promql response")?;
            let firing_now = !body.firing_elements(rule.threshold).is_empty();
            let value = body.firing_elements(rule.threshold).first().map_or(0, |e| e.value as i64);

            let entry = state.entry(key.clone()).or_insert_with(|| AlertState {
                phase: AlertPhase::Pending,
                active_since: now,
                last_active: now,
                notified: false,
            });

            if firing_now {
                entry.last_active = now;
                match entry.phase {
                    AlertPhase::Pending => {
                        let dwell = now.duration_since(entry.active_since).as_secs();
                        if dwell >= rule.for_secs {
                            entry.phase = AlertPhase::Firing;
                            if !entry.notified {
                                fire_alert(client, alertmanager_url, &group.name, rule, value)
                                    .await?;
                                entry.notified = true;
                            }
                        }
                    }
                    AlertPhase::Firing => {
                        // Continue firing — Alertmanager handles re-notify cadence.
                    }
                }
            } else {
                // Condition cleared. Honor `keep_firing_for` before resetting.
                let cool = rule.keep_firing_for_secs;
                let since_last = now.duration_since(entry.last_active).as_secs();
                if matches!(entry.phase, AlertPhase::Firing) && since_last < cool {
                    // Still in the keep-firing window — leave state alone.
                } else {
                    // Reset: rule is no longer alerting.
                    *entry = AlertState {
                        phase: AlertPhase::Pending,
                        active_since: now,
                        last_active: now,
                        notified: false,
                    };
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct PromqlEnvelope {
    #[serde(default)]
    status: String,
    #[serde(default)]
    data: PromqlData,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct PromqlData {
    #[serde(rename = "resultType")]
    result_type: String,
    result: serde_json::Value,
}

#[derive(Debug, Clone)]
struct FiringElement {
    value: f64,
}

impl PromqlEnvelope {
    fn firing_elements(&self, threshold: i64) -> Vec<FiringElement> {
        if self.status != "success" {
            return Vec::new();
        }
        match self.data.result_type.as_str() {
            "scalar" => {
                if let Some(arr) = self.data.result.as_array()
                    && let Some(v_str) = arr.get(1).and_then(|v| v.as_str())
                    && let Ok(v) = v_str.parse::<f64>()
                    && v > threshold as f64
                {
                    return vec![FiringElement { value: v }];
                }
                Vec::new()
            }
            "vector" => {
                let mut out = Vec::new();
                if let Some(arr) = self.data.result.as_array() {
                    for entry in arr {
                        if let Some(value_pair) = entry.get("value").and_then(|v| v.as_array())
                            && let Some(v_str) = value_pair.get(1).and_then(|v| v.as_str())
                            && let Ok(v) = v_str.parse::<f64>()
                            && v > threshold as f64
                        {
                            out.push(FiringElement { value: v });
                        }
                    }
                }
                out
            }
            _ => Vec::new(),
        }
    }
}

async fn fire_alert(
    client: &reqwest::Client,
    alertmanager_url: &str,
    group: &str,
    rule: &Rule,
    sample_value: i64,
) -> Result<()> {
    let value_str = sample_value.to_string();
    let mut labels: std::collections::BTreeMap<&str, &str> = std::collections::BTreeMap::new();
    labels.insert("alertname", &rule.alert);
    labels.insert("group", group);
    labels.insert("value", &value_str);
    for (k, v) in &rule.labels {
        labels.insert(k, v);
    }
    let mut annotations: std::collections::BTreeMap<&str, &str> = std::collections::BTreeMap::new();
    for (k, v) in &rule.annotations {
        annotations.insert(k, v);
    }
    let payload = [AlertmanagerAlert { labels, annotations }];
    let resp = client
        .post(format!("{}/api/v2/alerts", alertmanager_url.trim_end_matches('/')))
        .json(&payload)
        .send()
        .await
        .context("post alert")?;
    if !resp.status().is_success() {
        bail!("alertmanager returned HTTP {}", resp.status());
    }
    tracing::info!(alert = %rule.alert, group = %group, value = sample_value, "alert fired");
    Ok(())
}

fn urlencode(s: &str) -> String {
    use std::fmt::Write as _;
    // Minimal percent-encoding for the characters we actually emit.
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}
