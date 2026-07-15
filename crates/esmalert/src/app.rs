//! Runtime wiring for the esmalert binary: turns parsed [`crate::flags::Flags`]
//! into the datasource/remote-write/notifier/manager/web collaborators and
//! owns the main reload/shutdown loop.
//!
//! Mirrors esmauth's `lib.rs::run` (`crates/esmauth/src/lib.rs`) — flag
//! parsing stays in `flags.rs`, `main.rs` stays a thin
//! parse-flags-then-call-`run`/`run_dry` shell, and this module holds
//! everything in between. Split out (rather than inlined in `main.rs`) so
//! [`run_dry`] is unit-testable without spawning a process (per the task
//! brief) and so `flags.rs`'s parser tests don't need to pull in the whole
//! engine.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam_channel::Receiver;
use esm_common::infof;
use esm_gotemplate::{Metric as TplMetric, QueryFn, TemplateError};

use crate::config::{self, Config};
use crate::datasource::{
    AuthConfig, AuthFlags as DsAuthFlags, Datasource, Metric as DsMetric, TlsConfig,
    DEFAULT_QUERY_TIMEOUT,
};
use crate::flags::{AuthFlagSet, Flags};
use crate::manager::{Manager, ManagerDeps};
use crate::notifier::{AlertManager, Notifiers};
use crate::remotewrite::{RwClient, RwConfig, DEFAULT_SEND_TIMEOUT};
use crate::rule::Querier;
use crate::signal;
use crate::web::{self, WebConfig};

/// `-rule.resendDelay` equivalent (upstream default `1m`). Not in this
/// task's in-scope flag list (see the task brief's flag list), so it's a
/// fixed constant here rather than flag-controlled — see the task report.
const RESEND_DELAY: Duration = Duration::from_secs(60);

/// Fixed per-request timeout for the Alertmanager notifier client; no
/// `-notifier.*` timeout flag is in this task's scope.
const NOTIFIER_TIMEOUT: Duration = Duration::from_secs(10);

/// `-dryRun`: loads and validates every `-rule` file (structural checks,
/// MetricsQL expression parsing, and Go-template validation for
/// labels/annotations, all via [`config::validate_config`]) and returns
/// without building any client or starting any server or thread. Testable
/// in-process (per the task brief) — never touches the network.
pub fn run_dry(flags: &Flags) -> Result<(), String> {
    if flags.rule_globs.is_empty() {
        return Err("at least one -rule flag must be set".to_string());
    }
    let cfg = config::load_config(&flags.rule_globs).map_err(|e| e.to_string())?;
    config::validate_config(&cfg).map_err(|e| e.to_string())?;
    Ok(())
}

/// Re-reads and validates every `-rule` file. Shared by the initial startup
/// load and every reload (config-check ticker / `POST /-/reload`).
fn load_and_validate(flags: &Flags) -> Result<Config, String> {
    let cfg = config::load_config(&flags.rule_globs).map_err(|e| e.to_string())?;
    config::validate_config(&cfg).map_err(|e| e.to_string())?;
    Ok(cfg)
}

/// Carried-over precondition (Task 17 brief, not enforced by [`Manager`]
/// itself — see its module doc comment): a config with recording rules but
/// no `-remoteWrite.url` has nowhere to push results; a config with
/// alerting rules but no `-notifier.url` has nowhere to send alerts.
/// Matches upstream vmalert, which refuses to start in either case.
/// Checked on every load (startup and reload) — an invalid reload is
/// rejected the same way a parse/validate failure is: logged, previous
/// config kept running.
pub fn check_rw_notifier_precondition(cfg: &Config, flags: &Flags) -> Result<(), String> {
    let mut has_recording = false;
    let mut has_alerting = false;
    for rule in cfg.groups.iter().flat_map(|g| &g.rules) {
        if rule.record.as_deref().is_some_and(|s| !s.is_empty()) {
            has_recording = true;
        }
        if rule.alert.as_deref().is_some_and(|s| !s.is_empty()) {
            has_alerting = true;
        }
    }
    if has_recording && flags.remote_write_url.is_none() {
        return Err("config contains recording rules but -remoteWrite.url is not set".to_string());
    }
    if has_alerting && flags.notifier_urls.is_empty() {
        return Err(
            "config contains alerting rules but no -notifier.url is configured".to_string(),
        );
    }
    Ok(())
}

/// Loads/validates `-rule`, builds every collaborator, starts the
/// [`Manager`] and web server, then blocks running the reload/config-check
/// loop until SIGINT/SIGTERM, and finally shuts everything down gracefully.
/// Returns once shutdown has completed.
pub fn run(flags: Flags) -> Result<(), String> {
    if flags.rule_globs.is_empty() {
        return Err("at least one -rule flag must be set".to_string());
    }
    if flags.datasource_url.is_empty() {
        return Err("-datasource.url must be set".to_string());
    }

    let cfg = load_and_validate(&flags)?;
    check_rw_notifier_precondition(&cfg, &flags)?;

    let datasource = Arc::new(build_datasource(&flags)?);
    let rw = build_remote_write(&flags)?;
    let notifiers = build_notifiers(&flags)?;
    let restore = build_restore_querier(&flags)?;
    let query_fn = build_query_fn(Arc::clone(&datasource));

    let deps = ManagerDeps {
        querier: Arc::clone(&datasource) as Arc<dyn Querier + Send + Sync>,
        restore,
        rw: rw.clone(),
        notifiers,
        external_url: resolve_external_url(&flags),
        path_prefix: String::new(),
        query_fn,
        resend_delay: RESEND_DELAY,
        max_resolve_duration: None,
        default_eval_interval: flags.evaluation_interval,
        max_start_delay: flags.group_max_start_delay,
        disable_alertgroup_label: flags.disable_alertgroup_label,
    };

    let manager = Manager::start(cfg, deps).map_err(|e| e.to_string())?;
    let manager = Arc::new(Mutex::new(manager));

    let (reload_tx, reload_rx) = crossbeam_channel::unbounded();
    let web_cfg = WebConfig {
        listen_addr: flags.http_listen_addr.clone(),
        reload_auth_key: non_empty(&flags.reload_auth_key),
        metrics_auth_key: non_empty(&flags.metrics_auth_key),
        read_timeout: flags.http_read_timeout,
    };
    let server = web::serve(Arc::clone(&manager), web_cfg, reload_tx).map_err(|e| e.to_string())?;

    infof!("esmalert: serving rule groups on {}", server.local_addr());

    run_main_loop(&flags, &manager, reload_rx);

    infof!("esmalert: shutting down");
    server.stop();
    match Arc::try_unwrap(manager) {
        Ok(mutex) => mutex
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .shutdown(),
        Err(_) => {
            log::warn!(
                "esmalert: manager still referenced after web server stop; skipping graceful \
                 group shutdown"
            );
        }
    }
    if let Some(rw) = rw {
        match Arc::try_unwrap(rw) {
            Ok(client) => client.shutdown(),
            Err(_) => log::warn!(
                "esmalert: remote-write client still referenced at shutdown; relying on its \
                 Drop safety net"
            ),
        }
    }

    Ok(())
}

/// Owns the reload/shutdown loop: wakes on `reload_rx` (fed by `POST
/// /-/reload`), on a `-configCheckInterval` ticker (disabled when zero, via
/// `crossbeam_channel::never()`), or on SIGINT/SIGTERM (bridged from
/// `signal::wait_for_shutdown_signal`'s blocking wait onto a channel by a
/// dedicated thread, so it can be `select!`-ed alongside the other two).
fn run_main_loop(flags: &Flags, manager: &Arc<Mutex<Manager>>, reload_rx: Receiver<()>) {
    let ticker = if flags.config_check_interval.is_zero() {
        crossbeam_channel::never()
    } else {
        crossbeam_channel::tick(flags.config_check_interval)
    };

    let (shutdown_tx, shutdown_rx) = crossbeam_channel::bounded(1);
    thread::Builder::new()
        .name("esmalert-shutdown-wait".to_owned())
        .spawn(move || {
            let sig = signal::wait_for_shutdown_signal();
            infof!("esmalert: received signal {sig}");
            let _ = shutdown_tx.send(());
        })
        .expect("failed to spawn esmalert shutdown-wait thread");

    loop {
        crossbeam_channel::select! {
            recv(reload_rx) -> _ => reload_config(flags, manager),
            recv(ticker) -> _ => reload_config(flags, manager),
            recv(shutdown_rx) -> _ => break,
        }
    }
}

/// Re-reads and validates `-rule`, re-checks the rw/notifier precondition,
/// and applies the result via [`Manager::reload`]. On any failure the
/// previous configuration is left running (logged, never propagated) —
/// matches [`crate::manager::Manager::reload`]'s own "keep previous on
/// error" contract one level up, for load/validate failures `reload` never
/// sees.
fn reload_config(flags: &Flags, manager: &Arc<Mutex<Manager>>) {
    let result = load_and_validate(flags)
        .and_then(|cfg| check_rw_notifier_precondition(&cfg, flags).map(|()| cfg));
    match result {
        Ok(cfg) => {
            let mut mgr = manager.lock().unwrap_or_else(|e| e.into_inner());
            match mgr.reload(cfg) {
                Ok(()) => log::info!("esmalert: reloaded rule config"),
                Err(e) => {
                    log::warn!("esmalert: reload failed, keeping previous configuration: {e}")
                }
            }
        }
        Err(e) => {
            log::warn!("esmalert: cannot reload rule config, keeping previous configuration: {e}")
        }
    }
}

/// Resolves one component's raw CLI auth fields into an [`AuthConfig`],
/// reading any `*File` secret from disk (never logging its contents — see
/// [`AuthConfig::from_flags`]).
fn build_auth_config(set: &AuthFlagSet) -> Result<AuthConfig, String> {
    let f = DsAuthFlags {
        username: Some(set.username.as_str()),
        password: Some(set.password.as_str()),
        password_file: Some(set.password_file.as_str()),
        bearer_token: Some(set.bearer_token.as_str()),
        bearer_token_file: Some(set.bearer_token_file.as_str()),
    };
    AuthConfig::from_flags(&f).map_err(|e| e.to_string())
}

fn build_tls_config(set: &AuthFlagSet) -> TlsConfig {
    TlsConfig {
        ca_file: non_empty(&set.tls_ca_file),
        cert_file: non_empty(&set.tls_cert_file),
        key_file: non_empty(&set.tls_key_file),
        server_name: non_empty(&set.tls_server_name),
        insecure_skip_verify: set.tls_insecure_skip_verify,
    }
}

/// Empty string means "unset" (Go's `flag.String` zero-value convention).
fn non_empty(s: &str) -> Option<String> {
    (!s.is_empty()).then(|| s.to_string())
}

fn build_datasource(flags: &Flags) -> Result<Datasource, String> {
    let auth = build_auth_config(&flags.datasource_auth)?;
    let tls = build_tls_config(&flags.datasource_auth);
    Datasource::new(
        &flags.datasource_url,
        auth,
        tls,
        BTreeMap::new(),
        Vec::new(),
        flags.evaluation_interval,
        DEFAULT_QUERY_TIMEOUT,
    )
    .map_err(|e| e.to_string())
}

fn build_remote_write(flags: &Flags) -> Result<Option<Arc<RwClient>>, String> {
    let Some(url) = &flags.remote_write_url else {
        return Ok(None);
    };
    let auth = build_auth_config(&flags.remote_write_auth)?;
    let tls = build_tls_config(&flags.remote_write_auth);
    let cfg = RwConfig {
        url: url.clone(),
        flush_interval: flags.remote_write_flush_interval,
        max_batch_size: flags.remote_write_max_batch_size,
        max_queue_size: flags.remote_write_max_queue_size,
        concurrency: flags.remote_write_concurrency,
        send_timeout: DEFAULT_SEND_TIMEOUT,
        auth,
        tls,
        headers: Vec::new(),
    };
    let client = RwClient::start(cfg).map_err(|e| e.to_string())?;
    Ok(Some(Arc::new(client)))
}

fn build_notifiers(flags: &Flags) -> Result<Option<Arc<Notifiers>>, String> {
    if flags.notifier_urls.is_empty() {
        return Ok(None);
    }
    let auth = build_auth_config(&flags.notifier_auth)?;
    let tls = build_tls_config(&flags.notifier_auth);
    let mut targets = Vec::with_capacity(flags.notifier_urls.len());
    for url in &flags.notifier_urls {
        let am = AlertManager::new(
            url,
            "",
            Vec::new(),
            auth.clone(),
            tls.clone(),
            NOTIFIER_TIMEOUT,
        )
        .map_err(|e| e.to_string())?;
        targets.push(am);
    }
    Ok(Some(Arc::new(Notifiers(targets))))
}

/// Matches [`ManagerDeps::restore`]'s field type: a remote-read-capable
/// querier paired with the `-remoteRead.lookback` window.
type RestoreQuerier = (Arc<dyn Querier + Send + Sync>, Duration);

fn build_restore_querier(flags: &Flags) -> Result<Option<RestoreQuerier>, String> {
    let Some(url) = &flags.remote_read_url else {
        return Ok(None);
    };
    let auth = build_auth_config(&flags.remote_read_auth)?;
    let tls = build_tls_config(&flags.remote_read_auth);
    let ds = Datasource::new(
        url,
        auth,
        tls,
        BTreeMap::new(),
        Vec::new(),
        flags.evaluation_interval,
        DEFAULT_QUERY_TIMEOUT,
    )
    .map_err(|e| e.to_string())?;
    Ok(Some((
        Arc::new(ds) as Arc<dyn Querier + Send + Sync>,
        flags.remote_read_lookback,
    )))
}

/// Builds the `esm_gotemplate` `query` builtin's datasource callback: an
/// instant query against `ds` at the current wall-clock time, converting
/// `datasource::Metric` (multi-point series) into `esm_gotemplate::Metric`
/// (single labeled value) by taking each series' last sample — an instant
/// query returns at most one point per series, so "last" and "only"
/// coincide in practice.
fn build_query_fn(ds: Arc<Datasource>) -> QueryFn {
    Arc::new(move |expr: &str| {
        let ts = now_ms();
        let result = ds
            .query(expr, ts)
            .map_err(|e| TemplateError::new(e.to_string()))?;
        Ok(result.data.into_iter().map(to_template_metric).collect())
    })
}

fn to_template_metric(m: DsMetric) -> TplMetric {
    TplMetric {
        labels: m.labels.into_iter().collect(),
        value: m.values.last().copied().unwrap_or(0.0),
        timestamp: m.timestamps.last().copied().unwrap_or(0),
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// `-external.url`'s default when unset: upstream vmalert defaults to
/// `http://<hostname>:<port>`; this port uses `localhost` in place of a real
/// hostname lookup (no other need for a hostname-resolution dependency
/// exists in this crate) — see the task report.
fn resolve_external_url(flags: &Flags) -> String {
    if !flags.external_url.is_empty() {
        return flags.external_url.clone();
    }
    let port_suffix = match flags.http_listen_addr.rfind(':') {
        Some(i) => &flags.http_listen_addr[i..],
        None => "",
    };
    format!("http://localhost{port_suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse_config_str;

    struct TempRuleFile {
        dir: std::path::PathBuf,
    }

    impl TempRuleFile {
        fn new(name: &str, contents: &str) -> (Self, String) {
            let dir = std::env::temp_dir().join(format!(
                "esmalert-app-test-{}-{}-{}",
                std::process::id(),
                name,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&dir).expect("create temp dir");
            let path = dir.join("rules.yml");
            std::fs::write(&path, contents).expect("write rule file");
            let glob = path.to_string_lossy().to_string();
            (TempRuleFile { dir }, glob)
        }
    }

    impl Drop for TempRuleFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn flags_with_rule(glob: String) -> Flags {
        Flags {
            rule_globs: vec![glob],
            datasource_url: "http://vm:8428".to_string(),
            ..Flags::default()
        }
    }

    #[test]
    fn run_dry_accepts_a_valid_rule_file() {
        let (_tmp, glob) = TempRuleFile::new(
            "valid",
            "groups:\n  - name: g1\n    rules:\n      - record: r\n        expr: up\n",
        );
        let flags = flags_with_rule(glob);
        assert!(run_dry(&flags).is_ok());
    }

    #[test]
    fn run_dry_rejects_a_bad_expression() {
        let (_tmp, glob) = TempRuleFile::new(
            "bad-expr",
            "groups:\n  - name: g1\n    rules:\n      - record: r\n        expr: \"up(((\"\n",
        );
        let flags = flags_with_rule(glob);
        let err = run_dry(&flags).unwrap_err();
        assert!(!err.is_empty());
    }

    #[test]
    fn run_dry_requires_at_least_one_rule_glob() {
        let flags = Flags::default();
        let err = run_dry(&flags).unwrap_err();
        assert!(err.contains("-rule"), "{err}");
    }

    #[test]
    fn precondition_errors_when_recording_rule_has_no_remote_write() {
        let cfg = parse_config_str(
            "groups:\n  - name: g1\n    rules:\n      - record: r\n        expr: up\n",
        )
        .unwrap();
        let flags = Flags::default(); // remote_write_url: None
        let err = check_rw_notifier_precondition(&cfg, &flags).unwrap_err();
        assert!(err.contains("remoteWrite"), "{err}");
    }

    #[test]
    fn precondition_errors_when_alerting_rule_has_no_notifier() {
        let cfg = parse_config_str(
            "groups:\n  - name: g1\n    rules:\n      - alert: A\n        expr: up == 0\n",
        )
        .unwrap();
        let flags = Flags::default(); // notifier_urls: empty
        let err = check_rw_notifier_precondition(&cfg, &flags).unwrap_err();
        assert!(err.contains("notifier"), "{err}");
    }

    #[test]
    fn precondition_passes_when_rw_and_notifier_are_configured() {
        let cfg = parse_config_str(
            "groups:\n  - name: g1\n    rules:\n      - record: r\n        expr: up\n      - alert: A\n        expr: up == 0\n",
        )
        .unwrap();
        let flags = Flags {
            remote_write_url: Some("http://vm:8428".to_string()),
            notifier_urls: vec!["http://am:9093".to_string()],
            ..Flags::default()
        };
        assert!(check_rw_notifier_precondition(&cfg, &flags).is_ok());
    }

    #[test]
    fn precondition_passes_for_empty_config() {
        let cfg = Config::default();
        assert!(check_rw_notifier_precondition(&cfg, &Flags::default()).is_ok());
    }

    #[test]
    fn resolve_external_url_prefers_the_flag_when_set() {
        let flags = Flags {
            external_url: "https://alerts.example.com".to_string(),
            ..Flags::default()
        };
        assert_eq!(resolve_external_url(&flags), "https://alerts.example.com");
    }

    #[test]
    fn resolve_external_url_derives_a_default_from_the_listen_addr() {
        let flags = Flags {
            http_listen_addr: ":8880".to_string(),
            ..Flags::default()
        };
        assert_eq!(resolve_external_url(&flags), "http://localhost:8880");
    }

    #[test]
    fn to_template_metric_takes_the_last_sample() {
        let m = DsMetric {
            labels: vec![("instance".to_string(), "a".to_string())],
            timestamps: vec![1, 2, 3],
            values: vec![1.0, 2.0, 42.0],
        };
        let tpl = to_template_metric(m);
        assert_eq!(tpl.value, 42.0);
        assert_eq!(tpl.timestamp, 3);
        assert_eq!(tpl.labels.get("instance").map(String::as_str), Some("a"));
    }
}
