//! esmalert's read-only JSON API server (Task 18).
//!
//! Mirrors esmauth's server wiring (`crates/esmauth/src/lib.rs`): bind an
//! `esm-http` [`Server`], hand it a request-handler closure, and gate
//! `/-/reload` + `/metrics` behind optional auth keys the same way esmauth
//! gates its own `/-/reload`/`/metrics`. See [`api`] for the route table,
//! JSON envelopes, and the auth-key check.
//!
//! Faithful (narrowed) subset of upstream `app/vmalert/web.go:138-224`: this
//! module never re-reads or re-parses the rule config itself. `POST
//! /-/reload` only signals `reload_tx`; owning the config path, reading it,
//! and calling `Manager::reload` is `main.rs`'s job (Task 19), which is the
//! only place that knows the config path/flags.

mod api;

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_channel::Sender;
use esm_http::{Server, ServerConfig};

use crate::manager::Manager;

/// Configuration for [`serve`].
pub struct WebConfig {
    /// Address to bind, e.g. `"127.0.0.1:8880"`; `":0"`/`"127.0.0.1:0"` picks
    /// an ephemeral port (read back via [`ServerHandle::local_addr`]).
    pub listen_addr: String,
    /// Gates `POST /-/reload` when set (mirrors upstream `-reloadAuthKey` /
    /// esmauth's `-reloadAuthKey`). `None` leaves the endpoint open.
    pub reload_auth_key: Option<String>,
    /// Gates `GET /metrics` when set (mirrors esmauth's `-metricsAuthKey`).
    /// `None` leaves the endpoint open.
    pub metrics_auth_key: Option<String>,
    /// Per-read idle socket timeout (slow-loris defense); see
    /// `esm_http::ServerConfig::read_timeout`. `Duration::ZERO` disables it.
    pub read_timeout: Duration,
}

/// Error binding/starting the web server.
#[derive(Debug)]
pub struct WebError(io::Error);

impl fmt::Display for WebError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "esmalert web server: {}", self.0)
    }
}

impl std::error::Error for WebError {}

impl From<io::Error> for WebError {
    fn from(err: io::Error) -> Self {
        WebError(err)
    }
}

/// A running esmalert web server. The accept loop runs on its own thread
/// (see `esm_http::Server::serve`); stop it with [`ServerHandle::stop`]
/// (also stopped on drop, since it just wraps `esm_http::Server`, which is
/// itself stop-on-drop).
pub struct ServerHandle {
    server: Server,
}

impl ServerHandle {
    /// The bound listen address (the real port when bound to `:0`).
    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    /// Graceful shutdown; idempotent.
    pub fn stop(self) {
        self.server.stop();
    }
}

/// Binds `cfg.listen_addr` and starts serving the read-only JSON API
/// described in [`api`]. Returns immediately; each connection is handled on
/// its own thread (`esm_http::Server::serve`'s thread-per-connection model).
///
/// `mgr` is read on every request via `groups_snapshot`/`alerts_snapshot`
/// (never mutated here — this server is read-only); `reload_tx` is signaled
/// by `POST /-/reload` and is otherwise unused by this module.
pub fn serve(
    mgr: Arc<Mutex<Manager>>,
    cfg: WebConfig,
    reload_tx: Sender<()>,
) -> Result<ServerHandle, WebError> {
    // A zero `read_timeout` disables the idle timeout, mirroring esmauth's
    // `-readTimeout` handling.
    let read_timeout = (!cfg.read_timeout.is_zero()).then_some(cfg.read_timeout);
    let server = Server::bind_with_config(
        &cfg.listen_addr,
        ServerConfig {
            // Auth-key gating uses only the `?authKey=` query arg (see
            // `api::check_auth_key`), matching esmauth/upstream vmalert, so
            // no request headers need capturing (the default).
            read_timeout,
            ..ServerConfig::default()
        },
    )?;

    let ctx = Arc::new(api::Context {
        mgr,
        reload_auth_key: cfg.reload_auth_key,
        metrics_auth_key: cfg.metrics_auth_key,
        reload_tx,
    });

    server.serve(Arc::new(move |req, w| {
        api::handle(&ctx, req, w);
    }));

    Ok(ServerHandle { server })
}
