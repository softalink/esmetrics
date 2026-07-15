//! esmagent: Rust port of upstream vmagent's forwarding tier.
//!
//! Split into `lib.rs`/`main.rs` (mirrors `esmalert`/`esmauth`'s structure)
//! so the binary entry point stays a thin shell and the forwarding pipeline
//! is reachable from tests. [`run_dry`] validates a config without starting
//! anything (network- and thread-free, so it's unit-testable in-process);
//! [`run`] builds every collaborator and starts serving, returning an
//! [`App`] the caller shuts down via [`App::stop`] — mirrors
//! `esmetrics::run`/`esmetrics::App`'s shape (this crate has no
//! storage/select tier, so the seam is simpler: one `esm_insert` router in
//! front of a [`sink::ForwardingSink`] -> [`fanout::Fanout`] pipeline, no
//! `/metrics`-adjacent storage endpoints).

pub mod client;
pub mod fanout;
pub mod flags;
pub mod pendingseries;
pub mod queue;
pub mod rwctx;
pub mod scrape;
pub mod series;
pub mod signal;
pub mod sink;
pub mod streamagg;

use std::borrow::Cow;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use esm_http::{Request, ResponseWriter, Server, ServerConfig};
use esm_insert::InsertHandlers;
use esm_relabel::ParsedConfigs;

use crate::client::{AuthConfig, ClientConfig, TlsConfig};
use crate::fanout::Fanout;
use crate::flags::{at, bool_at, Flags, RemoteWriteAuthFlags};
use crate::rwctx::{RemoteWriteCtx, RwCtxConfig};
use crate::scrape::manager::{ScrapeManager, TargetsHandle};
use crate::sink::{ForwardingSink, SeriesConsumer};

/// Fixed per-request timeout for every destination's remote-write HTTP
/// client; no `-remoteWrite.sendTimeout` flag is in this task's scope (see
/// the task brief's flag list) — same convention as esmalert's
/// `NOTIFIER_TIMEOUT` constant for a component with no dedicated timeout
/// flag.
const REMOTE_WRITE_SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// A running esmagent instance: one HTTP server accepting remote-write-style
/// ingestion, forwarding every accepted series to every configured
/// `-remoteWrite.url` destination, plus (when `-promscrape.config` is set)
/// an active [`ScrapeManager`] pulling from discovered targets into the same
/// forwarding pipeline. Stop with [`App::stop`], which shuts the server
/// down, then the scrape manager, then every destination's
/// [`Fanout`]/[`RemoteWriteCtx`] pipeline, giving each a chance to flush and
/// close cleanly.
pub struct App {
    server: Server,
    /// `None` when `-promscrape.config` is unset (scrape engine disabled).
    /// [`App::stop`] stops this BEFORE `fanout` — see its doc for why the
    /// order matters. [`App::reload_scrape_config`] needs `&mut` access to
    /// call [`ScrapeManager::reload`].
    scrape_manager: Option<ScrapeManager>,
    /// Kept alongside every `Arc<dyn SeriesConsumer>` clone embedded in the
    /// request handler's `ForwardingSink` (and, when scraping is enabled,
    /// each scrape worker) so [`App::stop`] can reclaim ownership (via
    /// `Arc::try_unwrap`, once the server has stopped and the scrape
    /// manager's workers have all been joined) and call [`Fanout::stop`],
    /// which needs `self` by value to join every destination's threads.
    fanout: Arc<Fanout>,
    /// The global stream-aggregation handle when `-streamAggr.config` is set
    /// (else `None`). [`App::stop`] flushes and stops it BEFORE `fanout` so
    /// the final aggregated state reaches the still-running destinations. See
    /// `crate::streamagg`.
    stream_aggregators: Option<Arc<esm_streamaggr::Aggregators>>,
}

impl App {
    /// The bound listen address (the real port when bound to `:0`).
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.server.local_addr()
    }

    /// Re-reads and reloads `-promscrape.config` if the scrape engine is
    /// enabled; a no-op (`Ok(())`) if `-promscrape.config` was never set
    /// (nothing to reload). On a bad reload — unreadable file, invalid
    /// YAML, failed validation, or an unbuildable job — the scrape
    /// manager's previous config keeps running unchanged (see
    /// [`ScrapeManager::reload`]'s doc); this method never panics, so the
    /// caller (`main`'s SIGHUP/`-promscrape.configCheckInterval` loop) only
    /// needs to log a returned `Err`, never crash on one.
    pub fn reload_scrape_config(&mut self, flags: &Flags) -> Result<(), String> {
        let (Some(manager), Some(path)) = (
            self.scrape_manager.as_mut(),
            flags.promscrape_config.as_deref(),
        ) else {
            return Ok(());
        };
        scrape::wiring::reload_scrape_manager(manager, flags, path)
    }

    /// Stops the HTTP server (draining in-flight requests), then the scrape
    /// manager (if any — its final act per target is a staleness-marker
    /// flush THROUGH the still-running fanout, which is why it must stop
    /// before `fanout` does), then every destination's forwarding pipeline.
    /// Idempotent-safe to call once.
    pub fn stop(mut self) {
        self.server.stop();
        if let Some(manager) = self.scrape_manager.take() {
            manager.stop();
        }
        // Stop stream aggregation next: `must_stop` runs the final flush,
        // whose aggregated output must still reach the running fan-out.
        // Dropping the last `Aggregators` handle here also releases the push
        // callback's `Fanout` reference, so the `try_unwrap` below can reclaim
        // the fan-out. Ordering matters (server -> scrape -> streamAggr ->
        // fanout).
        if let Some(aggregators) = self.stream_aggregators.take() {
            aggregators.must_stop();
            drop(aggregators);
        }
        match Arc::try_unwrap(self.fanout) {
            Ok(fanout) => fanout.stop(),
            Err(_) => {
                // In-flight requests still hold a `ForwardingSink` clone for
                // a moment; this only happens if `stop()` races a request
                // that started just before the server stopped accepting.
                log::warn!(
                    "esmagent: fanout still referenced after server stop; skipping graceful \
                     destination shutdown (buffered data may be lost)"
                );
            }
        }
    }
}

/// `-dryRun`: validates that at least one `-remoteWrite.url` is configured
/// and that every configured relabel config file (global
/// `-remoteWrite.relabelConfig` plus each per-destination
/// `-remoteWrite.urlRelabelConfig`) parses successfully, then (if
/// `-promscrape.config` is set) validates the scrape config too, then
/// returns without building any client, queue, or server. Testable
/// in-process (per the task brief) — never touches the network, never
/// spawns a thread.
pub fn run_dry(flags: &Flags) -> Result<(), String> {
    if flags.remote_write_urls.is_empty() {
        return Err("at least one -remoteWrite.url flag must be set".to_string());
    }
    if !flags.remote_write_relabel_config.is_empty() {
        load_relabel_config(&flags.remote_write_relabel_config)
            .map_err(|e| format!("-remoteWrite.relabelConfig: {e}"))?;
    }
    for (i, path) in flags.remote_write_url_relabel_configs.iter().enumerate() {
        if !path.is_empty() {
            load_relabel_config(path)
                .map_err(|e| format!("-remoteWrite.urlRelabelConfig (destination {i}): {e}"))?;
        }
    }
    if let Some(path) = &flags.promscrape_config {
        scrape::wiring::validate_scrape_config(path)?;
    }
    Ok(())
}

/// `-promscrape.config.dryRun`: validates `-promscrape.config` alone,
/// independent of `-remoteWrite.url`/the forwarding config (unlike
/// [`run_dry`], this does not require any `-remoteWrite.url` to be set).
/// Network- and thread-free, like [`run_dry`].
pub fn run_scrape_config_dry(flags: &Flags) -> Result<(), String> {
    let path = flags.promscrape_config.as_deref().ok_or_else(|| {
        "-promscrape.config.dryRun requires -promscrape.config to be set".to_string()
    })?;
    scrape::wiring::validate_scrape_config(path).map(|_| ())
}

/// Reads and parses a relabel config YAML file. The returned error never
/// carries `path`'s contents, only the path itself and the parser's
/// message. `pub(crate)` so `scrape::wiring` can load a second, independent
/// copy for the scrape manager (`ParsedConfigs` isn't `Clone` — see
/// `scrape::wiring`'s module doc for why it needs its own copy rather than
/// sharing the one moved into `ForwardingSink`).
pub(crate) fn load_relabel_config(path: &str) -> Result<ParsedConfigs, String> {
    let yaml = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read relabel config {path:?}: {e}"))?;
    ParsedConfigs::parse(&yaml).map_err(|e| format!("invalid relabel config {path:?}: {e}"))
}

/// Validates `flags`, builds one [`RemoteWriteCtx`] per `-remoteWrite.url`
/// destination, wires them into a [`Fanout`] behind a
/// [`sink::ForwardingSink`], mounts an `esm_insert` router on an `esm-http`
/// server at `-httpListenAddr`, and starts serving. Returns immediately;
/// the caller owns shutdown via [`App::stop`].
pub fn run(flags: &Flags) -> Result<App, String> {
    if flags.remote_write_urls.is_empty() {
        return Err("at least one -remoteWrite.url flag must be set".to_string());
    }

    let global_relabel = if flags.remote_write_relabel_config.is_empty() {
        None
    } else {
        Some(
            load_relabel_config(&flags.remote_write_relabel_config)
                .map_err(|e| format!("-remoteWrite.relabelConfig: {e}"))?,
        )
    };

    drop_dangling_queues(flags);

    let ctxs = build_remote_write_ctxs(flags)?;
    let fanout = Arc::new(Fanout::new(ctxs));

    // Insert the global stream-aggregation stage between global relabel and
    // the fan-out when `-streamAggr.config` is set; otherwise the fan-out is
    // the consumer directly.
    let base_consumer = Arc::clone(&fanout) as Arc<dyn SeriesConsumer>;
    let (consumer, stream_aggregators) = match streamagg::build(flags, base_consumer)? {
        Some((c, aggs)) => (c, Some(aggs)),
        None => (Arc::clone(&fanout) as Arc<dyn SeriesConsumer>, None),
    };

    let sink = Arc::new(ForwardingSink {
        global_relabel,
        consumer: Arc::clone(&consumer),
    });
    let insert = InsertHandlers::new(Arc::clone(&sink));

    let scrape_manager = scrape::wiring::build_scrape_manager(flags, Arc::clone(&consumer))
        .map_err(|e| format!("cannot start scrape engine: {e}"))?;
    let targets_handle = scrape_manager.as_ref().map(ScrapeManager::targets_handle);

    let addr = normalize_listen_addr(&flags.http_listen_addr);
    let read_timeout = (!flags.http_read_timeout.is_zero()).then_some(flags.http_read_timeout);
    let server = Server::bind_with_config(
        &addr,
        ServerConfig {
            read_timeout,
            ..ServerConfig::default()
        },
    )
    .map_err(|e| {
        format!(
            "cannot bind -httpListenAddr {:?}: {e}",
            flags.http_listen_addr
        )
    })?;

    let metrics_auth_key = flags.metrics_auth_key.clone();
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            request_handler(req, w, &insert, &metrics_auth_key, &targets_handle);
        },
    ));

    Ok(App {
        server,
        scrape_manager,
        fanout,
        stream_aggregators,
    })
}

/// Builds every destination's [`RemoteWriteCtx`] from `flags`. On any
/// failure, every already-started ctx is stopped (queues closed, worker
/// threads joined) before the error is returned, so a single misconfigured
/// destination never leaks the ones that started fine before it.
fn build_remote_write_ctxs(flags: &Flags) -> Result<Vec<RemoteWriteCtx>, String> {
    let mut ctxs = Vec::with_capacity(flags.remote_write_urls.len());
    for (i, url) in flags.remote_write_urls.iter().enumerate() {
        match build_one_ctx(flags, i, url) {
            Ok(ctx) => ctxs.push(ctx),
            Err(e) => {
                for ctx in ctxs {
                    ctx.stop();
                }
                return Err(format!(
                    "cannot start remote-write destination {url:?}: {e}"
                ));
            }
        }
    }
    Ok(ctxs)
}

fn build_one_ctx(flags: &Flags, i: usize, url: &str) -> Result<RemoteWriteCtx, String> {
    let url_relabel_path = at(&flags.remote_write_url_relabel_configs, i);
    let url_relabel = if url_relabel_path.is_empty() {
        None
    } else {
        Some(
            load_relabel_config(url_relabel_path)
                .map_err(|e| format!("-remoteWrite.urlRelabelConfig: {e}"))?,
        )
    };

    let client = ClientConfig {
        url: url.to_string(),
        queues: flags.remote_write_queues,
        retry_min: flags.remote_write_retry_min_interval,
        retry_max: flags.remote_write_retry_max_interval,
        send_timeout: REMOTE_WRITE_SEND_TIMEOUT,
        auth: resolve_auth(&flags.remote_write_auth, i)?,
        tls: resolve_tls(&flags.remote_write_auth, i),
    };

    let queue_dir = Path::new(&flags.remote_write_tmp_data_path).join(queue_dir_name(url, i));

    let stream_aggr_path = at(&flags.remote_write_stream_aggr_config, i);
    let stream_aggr_config = (!stream_aggr_path.is_empty()).then(|| stream_aggr_path.to_string());
    let stream_aggr_dedup_interval_ms = flags
        .remote_write_stream_aggr_dedup_interval
        .get(i)
        .copied()
        .unwrap_or(Duration::ZERO)
        .as_millis() as i64;

    RemoteWriteCtx::start(RwCtxConfig {
        client,
        url_relabel,
        queue_dir,
        max_disk_bytes: flags.remote_write_max_disk_usage_per_url,
        max_block_size: flags.remote_write_max_block_size,
        flush_interval: flags.remote_write_flush_interval,
        stream_aggr_config,
        stream_aggr_keep_input: bool_at(&flags.remote_write_stream_aggr_keep_input, i),
        stream_aggr_dedup_interval_ms,
    })
    .map_err(|e| e.to_string())
}

/// Resolves one destination's `AuthConfig`, reading `password`/`bearerToken`
/// from their `*File` path when the direct flag value is empty (matching
/// `esmalert::datasource::AuthConfig::from_flags`'s convention). The
/// returned error never includes the secret value itself, only the file
/// path and the I/O error — the secret is never formatted into a string
/// that could reach a log line.
fn resolve_auth(flags: &RemoteWriteAuthFlags, i: usize) -> Result<AuthConfig, String> {
    let username = at(&flags.username, i);
    let password = resolve_secret(
        at(&flags.password, i),
        at(&flags.password_file, i),
        "-remoteWrite.basicAuth.passwordFile",
    )?;
    let bearer = resolve_secret(
        at(&flags.bearer_token, i),
        at(&flags.bearer_token_file, i),
        "-remoteWrite.bearerTokenFile",
    )?;

    let basic =
        (!username.is_empty() || !password.is_empty()).then(|| (username.to_string(), password));
    let bearer = (!bearer.is_empty()).then_some(bearer);
    Ok(AuthConfig { basic, bearer })
}

/// Returns `direct` if non-empty; otherwise reads `file` (if non-empty) from
/// disk and trims trailing whitespace/newlines, matching
/// `esmalert::datasource::AuthConfig::from_flags`'s file-based credential
/// loading. Never logs or echoes the resolved value.
fn resolve_secret(direct: &str, file: &str, flag_name: &str) -> Result<String, String> {
    if !direct.is_empty() {
        return Ok(direct.to_string());
    }
    if file.is_empty() {
        return Ok(String::new());
    }
    let contents = std::fs::read_to_string(file)
        .map_err(|e| format!("cannot read {flag_name} {file:?}: {e}"))?;
    Ok(contents.trim().to_string())
}

fn resolve_tls(flags: &RemoteWriteAuthFlags, i: usize) -> TlsConfig {
    TlsConfig {
        ca_file: non_empty(at(&flags.tls_ca_file, i)),
        cert_file: non_empty(at(&flags.tls_cert_file, i)),
        key_file: non_empty(at(&flags.tls_key_file, i)),
        server_name: non_empty(at(&flags.tls_server_name, i)),
        insecure_skip_verify: bool_at(&flags.tls_insecure_skip_verify, i),
    }
}

fn non_empty(s: &str) -> Option<String> {
    (!s.is_empty()).then(|| s.to_string())
}

/// Derives a filesystem-safe, unique subdirectory name for one destination's
/// durable queue: `<index>-<sanitized-url>`, where every byte outside
/// `[A-Za-z0-9._-]` in `url` is replaced with `_`. The `<index>-` prefix
/// guarantees uniqueness even if two URLs sanitize to the same string (and
/// is what [`drop_dangling_queues`] relies on to recognize a still-valid
/// directory without re-deriving it from a `Flags` it doesn't have).
///
/// `pub` so integration tests (`tests/e2e.rs`) can locate a destination's
/// queue directory under `-remoteWrite.tmpDataPath` without duplicating
/// this naming scheme.
pub fn queue_dir_name(url: &str, index: usize) -> String {
    let sanitized: String = url
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{index}-{sanitized}")
}

/// Removes any subdirectory of `-remoteWrite.tmpDataPath` that doesn't
/// correspond to one of `flags`'s configured `-remoteWrite.url` destinations
/// (i.e. a leftover queue from a destination removed from the config since
/// the last run). Best-effort: a missing `tmpDataPath` or an I/O error
/// partway through is logged and otherwise ignored — this is disk-space
/// hygiene, not a correctness requirement, so it must never fail startup.
fn drop_dangling_queues(flags: &Flags) {
    let expected: HashSet<String> = flags
        .remote_write_urls
        .iter()
        .enumerate()
        .map(|(i, url)| queue_dir_name(url, i))
        .collect();

    let tmp_data_path = Path::new(&flags.remote_write_tmp_data_path);
    let entries = match std::fs::read_dir(tmp_data_path) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
        Err(e) => {
            log::warn!(
                "esmagent: cannot read -remoteWrite.tmpDataPath {tmp_data_path:?} to drop \
                 dangling queues: {e}"
            );
            return;
        }
    };

    for entry in entries.flatten() {
        let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
        if !is_dir {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if expected.contains(name) {
            continue;
        }
        let path: PathBuf = entry.path();
        log::info!("esmagent: dropping dangling remote-write queue {path:?}");
        if let Err(e) = std::fs::remove_dir_all(&path) {
            log::warn!("esmagent: failed to drop dangling remote-write queue {path:?}: {e}");
        }
    }
}

/// Go's `net.Listen` accepts `":8429"` (all interfaces); `std::net` needs an
/// explicit host.
fn normalize_listen_addr(addr: &str) -> Cow<'_, str> {
    if addr.starts_with(':') {
        Cow::Owned(format!("0.0.0.0{addr}"))
    } else {
        Cow::Borrowed(addr)
    }
}

/// Top-level request router: `esm_insert`'s ingestion router first, then
/// `/metrics` (gated by `-metrics.authKey`), `/-/healthy`, and (when the
/// scrape engine is enabled) `GET /api/v1/targets`, then 404. Mirrors
/// `esmetrics::request_handler`'s shape, narrowed to esmagent's
/// forwarding(+scrape)-only surface (no storage/select/vmui).
fn request_handler(
    req: &mut Request<'_>,
    w: &mut ResponseWriter<'_>,
    insert: &InsertHandlers<Arc<ForwardingSink>>,
    metrics_auth_key: &str,
    targets_handle: &Option<TargetsHandle>,
) {
    if insert.handle(req, w) {
        return;
    }
    match req.path() {
        "/metrics" => {
            if !metrics_auth_key.is_empty() {
                let supplied = query_param(req, "authKey");
                if supplied.as_deref() != Some(metrics_auth_key) {
                    w.set_status(401);
                    w.set_content_type("text/plain; charset=utf-8");
                    w.write_body(b"The provided authKey doesn't match -metrics.authKey\n");
                    return;
                }
            }
            w.set_content_type("text/plain; charset=utf-8");
            let mut body = String::new();
            esm_common::metrics::write_prometheus(&mut body);
            w.write_body(body.as_bytes());
        }
        "/-/healthy" => {
            w.set_content_type("text/plain; charset=utf-8");
            w.write_body(b"OK");
        }
        "/api/v1/targets" => match targets_handle {
            Some(handle) => {
                let state = query_param(req, "state");
                let body = scrape::wiring::targets_route_body(handle, state.as_deref());
                w.set_content_type("application/json");
                w.write_body(body.as_bytes());
            }
            None => {
                w.set_status(404);
                w.set_content_type("text/plain; charset=utf-8");
                w.write_body(b"scrape engine not enabled (-promscrape.config not set)\n");
            }
        },
        _ => {
            w.set_status(404);
            w.set_content_type("text/plain; charset=utf-8");
            w.write_body(format!("unsupported path requested: {:?}\n", req.path()).as_bytes());
        }
    }
}

fn query_param(req: &Request<'_>, key: &str) -> Option<String> {
    req.query_params()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
