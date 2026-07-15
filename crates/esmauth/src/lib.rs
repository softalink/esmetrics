//! esmauth server logic, lib-ified so integration tests can start the proxy
//! in-process (bind `127.0.0.1:0`, discover the port, hit endpoints).
//!
//! Wires the `esm-auth` library (config, auth map, routing, load balancing,
//! proxy) behind an `esm-http` server with SIGHUP/interval config hot-reload.
//! Port of the wiring in upstream `app/vmauth/main.go`.
//!
//! # Endpoint auth keys
//!
//! `/-/reload` and `/metrics` can be gated by `-reloadAuthKey` /
//! `-metricsAuthKey` (mirroring upstream vmauth's `httpserver.CheckAuthFlag`
//! and esmetrics' own `-snapshotAuthKey`): when the flag is non-empty the
//! request must carry a matching `?authKey=<value>` query param, else it gets
//! a 403 and the reload/metrics work is never performed. An empty flag (the
//! default) leaves the endpoint open, preserving prior behavior. Gating
//! `/-/reload` matters beyond info-disclosure: each reload re-reads and
//! re-parses the config and installs a fresh config generation, wiping backend
//! circuit-breaker state and resetting per-user concurrency limiters — an
//! amplification / rate-limit-reset vector if left unauthenticated.
//!
//! # Read timeout (slow-loris)
//!
//! The server is bound with `-readTimeout` as a **per-read idle timeout**
//! (`SO_RCVTIMEO`, reset on every successful read in `esm-http`'s conn loop),
//! not a whole-request deadline. It drops a connection that stops making
//! progress — e.g. a slow-loris trickling header bytes — after the idle
//! period, while a steadily progressing large upload is never cut. Zero
//! disables it.

pub mod flags;
pub mod signal;

use std::borrow::Cow;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use esm_auth::auth::{request_auth_tokens, AuthMap};
use esm_auth::balance::BackendPool;
use esm_auth::config::{parse_auth_config, LoadBalancingPolicy, UserInfo};
use esm_auth::proxy::Proxy;
use esm_auth::route::select_route;
use esm_common::limiter::Limiter;
use esm_http::{Request, ResponseWriter, Server, ServerConfig};

use crate::flags::Flags;

/// How often the reload watcher wakes to check for a SIGHUP request or an
/// elapsed `-configCheckInterval`.
const WATCHER_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// One config generation: the resolved [`AuthMap`] plus the per-generation
/// caches (backend pools and per-user limiters) that must be reset on reload.
struct AuthState {
    map: Arc<AuthMap>,
    /// Backend pools keyed by their `url_prefix` list, so backend health and
    /// round-robin state persist across requests within a config generation.
    pools: Mutex<HashMap<Vec<String>, Arc<BackendPool>>>,
    /// Per-user concurrency limiters keyed by a stable, secret-free user key.
    /// `None` means the user has no per-user limit.
    user_limiters: Mutex<HashMap<String, Option<Arc<Limiter>>>>,
    fail_timeout: Duration,
    max_queue_duration: Duration,
    default_max_concurrent_per_user: usize,
}

impl AuthState {
    /// Resolves (and caches) the [`BackendPool`] for a route's `url_prefixes`.
    ///
    /// Cached by URL list only, shared across every route/user that resolves
    /// to the same `url_prefixes` within this config generation — a
    /// deliberate divergence from upstream, which keys backend state
    /// per-`URLPrefix` (i.e. per config-file entry, not per distinct URL
    /// set); accepted by review, not a behavior change here.
    fn pool_for(&self, urls: &[String], policy: LoadBalancingPolicy) -> Arc<BackendPool> {
        let mut pools = self.pools.lock().unwrap();
        if let Some(pool) = pools.get(urls) {
            return Arc::clone(pool);
        }
        let pool = Arc::new(BackendPool::new(urls, policy, self.fail_timeout));
        pools.insert(urls.to_vec(), Arc::clone(&pool));
        pool
    }

    /// Resolves (and caches) the per-user concurrency limiter for `user`.
    /// Returns `None` when no per-user limit applies (effective cap of 0).
    fn user_limiter_for(&self, user: &UserInfo) -> Option<Arc<Limiter>> {
        let key = user_limiter_key(user);
        let mut limiters = self.user_limiters.lock().unwrap();
        if let Some(entry) = limiters.get(&key) {
            return entry.clone();
        }
        // Go `getMaxConcurrentRequests`: the user's own value wins when set,
        // else the -maxConcurrentPerUserRequests default; 0 => unlimited.
        let effective = match user.max_concurrent_requests {
            Some(n) if n > 0 => n,
            _ => self.default_max_concurrent_per_user,
        };
        let limiter = if effective > 0 {
            Some(Arc::new(Limiter::new(effective, self.max_queue_duration)))
        } else {
            None
        };
        limiters.insert(key, limiter.clone());
        limiter
    }
}

/// Owns the reloadable config state and the parameters needed to rebuild it.
/// Shared (via `Arc`) between the request handler, the reload watcher thread,
/// and [`App::reload`].
struct Reloader {
    config_path: PathBuf,
    auth: RwLock<Arc<AuthState>>,
    fail_timeout: Duration,
    max_queue_duration: Duration,
    default_max_concurrent_per_user: usize,
}

impl Reloader {
    /// The current config generation.
    fn current(&self) -> Arc<AuthState> {
        Arc::clone(&self.auth.read().unwrap())
    }

    /// Re-reads `-auth.config` and swaps in the new generation. On any failure
    /// the previous config is kept (upstream behavior) and a secret-free error
    /// is returned; the caller decides how to log/respond.
    fn reload(&self) -> Result<(), String> {
        let map = load_auth_map(&self.config_path)?;
        let new_state = build_state(
            map,
            self.fail_timeout,
            self.max_queue_duration,
            self.default_max_concurrent_per_user,
        );
        *self.auth.write().unwrap() = new_state;
        Ok(())
    }
}

/// A running esmauth instance: the HTTP server plus the reload watcher thread.
/// Stop with [`App::stop`].
pub struct App {
    server: Server,
    reloader: Arc<Reloader>,
    reload_stop: Arc<AtomicBool>,
    watcher: Option<JoinHandle<()>>,
}

impl App {
    /// The bound listen address (real port when bound to `:0`).
    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    /// Re-reads `-auth.config`. Returns a secret-free error on failure, with
    /// the previous configuration left in place.
    pub fn reload(&self) -> Result<(), String> {
        self.reloader.reload()
    }

    /// Stops the reload watcher and the HTTP server (graceful).
    pub fn stop(mut self) {
        self.reload_stop.store(true, Ordering::Release);
        if let Some(handle) = self.watcher.take() {
            let _ = handle.join();
        }
        self.server.stop();
    }
}

/// Loads `-auth.config`, binds the HTTP server, starts the reload watcher, and
/// begins serving. The caller owns shutdown via [`App::stop`].
pub fn run(flags: &Flags) -> io::Result<App> {
    let config_path = PathBuf::from(&flags.auth_config);
    let initial_map =
        load_auth_map(&config_path).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let reloader = Arc::new(Reloader {
        config_path,
        auth: RwLock::new(build_state(
            initial_map,
            flags.fail_timeout,
            flags.max_queue_duration,
            flags.max_concurrent_per_user_requests,
        )),
        fail_timeout: flags.fail_timeout,
        max_queue_duration: flags.max_queue_duration,
        default_max_concurrent_per_user: flags.max_concurrent_per_user_requests,
    });

    // Global (all-users) concurrency limiter; persists across reloads since it
    // is flag-derived, not config-derived.
    let global_limiter = Arc::new(Limiter::new(
        flags.max_concurrent_requests,
        flags.max_queue_duration,
    ));

    // A proxy must not follow backend redirects — 3xx responses are forwarded
    // to the client verbatim.
    let client = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| io::Error::other(format!("cannot build http client: {e}")))?;
    let proxy = Arc::new(Proxy::new(client, flags.max_request_body_size_to_retry));

    // The proxy needs to forward arbitrary client headers to backends, so the
    // server MUST capture all request headers (see `Proxy::proxy`'s
    // precondition) — without this, header forwarding silently breaks.
    let addr = normalize_listen_addr(&flags.http_listen_addr);
    // A zero `-readTimeout` disables the idle timeout (maps to `None`); any
    // non-zero value becomes the per-read `SO_RCVTIMEO` idle timeout that
    // closes slow-loris connections without cutting a progressing upload.
    let read_timeout = (!flags.read_timeout.is_zero()).then_some(flags.read_timeout);
    let server = Server::bind_with_config(
        &addr,
        ServerConfig {
            capture_all_headers: true,
            read_timeout,
            ..ServerConfig::default()
        },
    )?;

    let handler_reloader = Arc::clone(&reloader);
    let handler_proxy = Arc::clone(&proxy);
    let handler_global = Arc::clone(&global_limiter);
    let log_invalid = flags.log_invalid_auth_tokens;
    let reload_auth_key = flags.reload_auth_key.clone();
    let metrics_auth_key = flags.metrics_auth_key.clone();
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            request_handler(
                req,
                w,
                &handler_reloader,
                &handler_proxy,
                &handler_global,
                log_invalid,
                &reload_auth_key,
                &metrics_auth_key,
            );
        },
    ));

    let reload_stop = Arc::new(AtomicBool::new(false));
    let watcher = spawn_reload_watcher(
        Arc::clone(&reloader),
        Arc::clone(&reload_stop),
        flags.config_check_interval,
    );

    Ok(App {
        server,
        reloader,
        reload_stop,
        watcher: Some(watcher),
    })
}

/// Builds a fresh [`AuthState`] (empty caches) around a resolved map.
fn build_state(
    map: AuthMap,
    fail_timeout: Duration,
    max_queue_duration: Duration,
    default_max_concurrent_per_user: usize,
) -> Arc<AuthState> {
    Arc::new(AuthState {
        map: Arc::new(map),
        pools: Mutex::new(HashMap::new()),
        user_limiters: Mutex::new(HashMap::new()),
        fail_timeout,
        max_queue_duration,
        default_max_concurrent_per_user,
    })
}

/// Reads and validates `-auth.config` into an [`AuthMap`]. The returned error
/// is always secret-free (see [`sanitize_build_error`]).
fn load_auth_map(path: &Path) -> Result<AuthMap, String> {
    let yaml = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read -auth.config {}: {e}", path.display()))?;
    let cfg = parse_auth_config(&yaml)?;
    AuthMap::build(cfg).map_err(sanitize_build_error)
}

/// [`AuthMap::build`]'s duplicate-token error embeds the derived
/// `http_auth:<token>` credential string; that must never reach a log or an
/// HTTP response. Replace only that error with a generic summary; other build
/// errors are structural and secret-free, so keep them for diagnostics.
fn sanitize_build_error(err: String) -> String {
    if err.starts_with("duplicate auth token") {
        "invalid -auth.config: two users are configured with the same auth token".to_string()
    } else {
        err
    }
}

/// Spawns the reload watcher: reloads on SIGHUP (Unix, via `signal.rs`) and
/// every `config_check_interval` when it is non-zero. A failed reload keeps
/// the previous configuration.
fn spawn_reload_watcher(
    reloader: Arc<Reloader>,
    stop: Arc<AtomicBool>,
    config_check_interval: Duration,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("esmauth-config-watcher".to_owned())
        .spawn(move || {
            let mut next_interval_check = interval_deadline(config_check_interval);
            while !stop.load(Ordering::Acquire) {
                std::thread::sleep(WATCHER_POLL_INTERVAL);
                if stop.load(Ordering::Acquire) {
                    break;
                }
                let sighup = signal::take_reload_request();
                let interval_due =
                    matches!(next_interval_check, Some(deadline) if Instant::now() >= deadline);
                if sighup || interval_due {
                    match reloader.reload() {
                        Ok(()) => log::info!("esmauth: reloaded -auth.config"),
                        // `err` is secret-free (see `load_auth_map`).
                        Err(err) => log::warn!(
                            "esmauth: cannot reload -auth.config, keeping the previous \
                             configuration: {err}"
                        ),
                    }
                    next_interval_check = interval_deadline(config_check_interval);
                }
            }
        })
        .expect("failed to spawn esmauth config watcher thread")
}

fn interval_deadline(interval: Duration) -> Option<Instant> {
    if interval.is_zero() {
        None
    } else {
        Some(Instant::now() + interval)
    }
}

/// Go's `net.Listen` accepts `":8427"` (all interfaces); `std::net` needs an
/// explicit host.
fn normalize_listen_addr(addr: &str) -> Cow<'_, str> {
    if addr.starts_with(':') {
        Cow::Owned(format!("0.0.0.0{addr}"))
    } else {
        Cow::Borrowed(addr)
    }
}

/// Top-level request router (mirrors `requestHandler` in `app/vmauth/main.go`).
#[allow(clippy::too_many_arguments)]
fn request_handler(
    req: &mut Request<'_>,
    w: &mut ResponseWriter<'_>,
    reloader: &Reloader,
    proxy: &Proxy,
    global_limiter: &Limiter,
    log_invalid: bool,
    reload_auth_key: &str,
    metrics_auth_key: &str,
) {
    match req.path() {
        "/health" => {
            w.set_content_type("text/plain; charset=utf-8");
            w.write_body(b"OK");
            return;
        }
        "/metrics" => {
            if !check_auth_key(req, metrics_auth_key, "-metricsAuthKey", w) {
                return;
            }
            w.set_content_type("text/plain; charset=utf-8");
            let mut body = String::new();
            esm_common::metrics::write_prometheus(&mut body);
            w.write_body(body.as_bytes());
            return;
        }
        "/-/reload" => {
            if !check_auth_key(req, reload_auth_key, "-reloadAuthKey", w) {
                return;
            }
            handle_reload(reloader, w);
            return;
        }
        _ => {}
    }

    let state = reloader.current();

    // Auth: derive candidate tokens from the Authorization header(s), then look
    // the user up (never a secret comparison loop — pure map lookup).
    let auth_header_values = authorization_header_values(req);
    let auth_headers: Vec<&str> = auth_header_values.iter().map(String::as_str).collect();
    let tokens = request_auth_tokens(&auth_headers, None);

    let user: &UserInfo = if tokens.is_empty() {
        match state.map.unauthorized() {
            Some(uu) => uu,
            None => {
                write_missing_authorization(w);
                return;
            }
        }
    } else {
        match state.map.lookup(&tokens) {
            Some(ui) => ui,
            None => match state.map.unauthorized() {
                Some(uu) => uu,
                None => {
                    esm_auth::metrics::invalid_auth_token_requests().inc();
                    if log_invalid {
                        // Never log the token(s) — only the requested path.
                        log::warn!(
                            "esmauth: rejected request to {:?} with invalid auth token(s)",
                            req.path()
                        );
                    }
                    write_unauthorized(w);
                    return;
                }
            },
        }
    };

    // Concurrency limits: global first, then per-user — matching upstream
    // vmauth (main.go:283 beginConcurrencyLimit, then main.go:313
    // ui.beginConcurrencyLimit). Acquiring global first means a request never
    // holds a scarce per-user slot while queued on the global limiter, so a
    // user's other requests aren't starved. Both are bounded by
    // -maxQueueDuration; a queue timeout is a 429.
    let _global_permit = match global_limiter.acquire() {
        Ok(permit) => permit,
        Err(_) => {
            write_concurrency_limited(w);
            return;
        }
    };
    let user_limiter = state.user_limiter_for(user);
    let _user_permit = match &user_limiter {
        Some(limiter) => match limiter.acquire() {
            Ok(permit) => Some(permit),
            Err(_) => {
                write_concurrency_limited(w);
                return;
            }
        },
        None => None,
    };

    // Resolve the route's load-balancing policy up front so the pool cache is
    // keyed with the right policy; `Proxy::proxy` re-selects the route itself
    // (and answers 400 missing_route when there is none).
    let path = req.path().to_string();
    let host = req.host().to_string();
    let policy = select_route(user, &path, &host)
        .map(|route| route.policy)
        .unwrap_or_default();
    let pool_for = |urls: &[String]| state.pool_for(urls, policy);
    proxy.proxy(user, &pool_for, req, w);
}

/// Gates an endpoint on an authKey (mirrors esmetrics' `-snapshotAuthKey` and
/// upstream vmauth's `httpserver.CheckAuthFlag`). An empty `configured` key
/// leaves the endpoint open; otherwise the request's `?authKey=` query param
/// must match exactly, else a 403 is written and `false` is returned so the
/// caller skips the protected work. `flag_name` names the flag in the body.
fn check_auth_key(
    req: &Request<'_>,
    configured: &str,
    flag_name: &str,
    w: &mut ResponseWriter<'_>,
) -> bool {
    if configured.is_empty() {
        return true;
    }
    if query_param(req, "authKey").as_deref() == Some(configured) {
        return true;
    }
    w.set_status(403);
    w.set_content_type("text/plain; charset=utf-8");
    w.write_body(format!("The provided authKey doesn't match {flag_name}\n").as_bytes());
    false
}

/// Reads a single decoded query-string parameter by key (first match).
fn query_param(req: &Request<'_>, key: &str) -> Option<String> {
    req.query_params()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
}

/// Handles `/-/reload`: bumps the reload counter and triggers a reload.
fn handle_reload(reloader: &Reloader, w: &mut ResponseWriter<'_>) {
    esm_auth::metrics::config_reload_requests().inc();
    match reloader.reload() {
        Ok(()) => {
            log::info!("esmauth: reloaded -auth.config via /-/reload");
            w.set_content_type("text/plain; charset=utf-8");
            w.write_body(b"OK");
        }
        Err(err) => {
            // `err` is secret-free (see `load_auth_map`).
            log::warn!("esmauth: /-/reload failed, keeping the previous configuration: {err}");
            w.set_status(500);
            w.set_content_type("text/plain; charset=utf-8");
            w.write_body(format!("cannot reload -auth.config: {err}\n").as_bytes());
        }
    }
}

/// Collects the values of every `Authorization` request header (case-insensitive
/// name match) from the captured header list.
fn authorization_header_values(req: &Request<'_>) -> Vec<String> {
    req.all_headers()
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("authorization"))
        .map(|(_, value)| value.clone())
        .collect()
}

fn write_missing_authorization(w: &mut ResponseWriter<'_>) {
    w.set_status(401);
    w.set_header("WWW-Authenticate", "Basic realm=\"Restricted\"");
    w.set_content_type("text/plain; charset=utf-8");
    w.write_body(b"missing 'Authorization' request header\n");
}

fn write_unauthorized(w: &mut ResponseWriter<'_>) {
    w.set_status(401);
    w.set_content_type("text/plain; charset=utf-8");
    w.write_body(b"Unauthorized\n");
}

fn write_concurrency_limited(w: &mut ResponseWriter<'_>) {
    esm_auth::metrics::concurrent_requests_limit_reached().inc();
    w.set_status(429);
    w.set_header("Retry-After", "10");
    w.set_content_type("text/plain; charset=utf-8");
    w.write_body(b"too many concurrent requests\n");
}

/// A stable, secret-free key identifying a user for the per-user limiter cache.
/// Prefers the configured `name`, else `username`; for token-only users it is a
/// hash of the tokens (never the raw secret), used only as a map key and never
/// logged or exposed.
fn user_limiter_key(user: &UserInfo) -> String {
    use std::hash::{Hash, Hasher};
    if let Some(name) = non_empty(user.name.as_deref()) {
        return format!("n:{name}");
    }
    if let Some(username) = non_empty(user.username.as_deref()) {
        return format!("u:{username}");
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    user.bearer_token.hash(&mut hasher);
    user.auth_token.hash(&mut hasher);
    format!("t:{:016x}", hasher.finish())
}

fn non_empty(v: Option<&str>) -> Option<&str> {
    v.filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_go_style_listen_addrs() {
        assert_eq!(normalize_listen_addr(":8427"), "0.0.0.0:8427");
        assert_eq!(normalize_listen_addr("127.0.0.1:8427"), "127.0.0.1:8427");
        assert_eq!(normalize_listen_addr("localhost:80"), "localhost:80");
    }

    #[test]
    fn sanitize_build_error_hides_duplicate_token() {
        let leaky =
            r#"duplicate auth token="http_auth:Bearer s3cr3t" found for username=Some("a")"#
                .to_string();
        let safe = sanitize_build_error(leaky);
        assert!(
            !safe.contains("s3cr3t"),
            "sanitized error still leaks token: {safe}"
        );
        assert!(safe.contains("same auth token"), "{safe}");
    }

    #[test]
    fn sanitize_build_error_keeps_structural_errors() {
        let structural = "bearer_token cannot be specified if auth_token is set".to_string();
        assert_eq!(sanitize_build_error(structural.clone()), structural);
    }

    #[test]
    fn user_limiter_key_prefers_name_then_username_then_token_hash() {
        let named = UserInfo {
            name: Some("svc".to_string()),
            ..Default::default()
        };
        assert_eq!(user_limiter_key(&named), "n:svc");

        let user = UserInfo {
            username: Some("alice".to_string()),
            ..Default::default()
        };
        assert_eq!(user_limiter_key(&user), "u:alice");

        let token_only = UserInfo {
            bearer_token: Some("tok".to_string()),
            ..Default::default()
        };
        let key = user_limiter_key(&token_only);
        assert!(key.starts_with("t:"), "{key}");
        // The raw secret must never appear in the key.
        assert!(!key.contains("tok"), "{key}");
    }

    #[test]
    fn user_limiter_respects_effective_cap() {
        let map = AuthMap::build(esm_auth::config::AuthConfig::default()).unwrap();
        let state = build_state(map, Duration::from_secs(3), Duration::from_millis(10), 0);

        // default 0 => unlimited unless the user overrides.
        let unlimited = UserInfo {
            name: Some("u1".to_string()),
            ..Default::default()
        };
        assert!(state.user_limiter_for(&unlimited).is_none());

        // A user with an explicit cap gets a limiter regardless of the default.
        let capped = UserInfo {
            name: Some("u2".to_string()),
            max_concurrent_requests: Some(1),
            ..Default::default()
        };
        let limiter = state
            .user_limiter_for(&capped)
            .expect("capped user needs a limiter");
        assert_eq!(limiter.max_concurrent(), 1);
        // Cached: the same limiter instance is returned on the second call.
        let again = state.user_limiter_for(&capped).unwrap();
        assert!(Arc::ptr_eq(&limiter, &again));
    }
}
