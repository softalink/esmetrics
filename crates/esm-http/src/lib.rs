//! Minimal, fast, synchronous HTTP/1.1 server for esmetrics.
//!
//! # Design
//!
//! Thread-per-connection blocking server over [`std::net::TcpListener`] with
//! HTTP/1.1 keep-alive. TSBS clients (Go `net/http`) hold a small number of
//! persistent connections and never pipeline, so per-connection threads with
//! reused buffers give minimal latency without an async runtime. Only
//! portable `std::net` APIs are used, so the crate builds on Windows.
//!
//! No TLS, no HTTP/2. External deps: `flate2` (gzip) and `log` only.
//!
//! # Shutdown mechanics
//!
//! [`Server::stop`] is graceful and works like this:
//! 1. An `AtomicBool` (`stopped`) is set; accept and connection loops check
//!    it between blocking operations.
//! 2. The blocking `accept()` is woken portably by a loopback self-connect
//!    to the listener (no unix-only tricks), after which the accept thread
//!    observes `stopped` and exits.
//! 3. Every live connection is registered in a `Mutex<HashMap<id,
//!    TcpStream>>` of `try_clone`d handles; `stop()` calls
//!    `shutdown(Shutdown::Both)` on each, which unblocks in-flight reads on
//!    all platforms and makes connection threads exit their keep-alive loop.
//! 4. `stop()` then waits (condvar on an active-connection counter, capped
//!    at 5 s) for all connection threads to drain before returning.

mod conn;
mod query;
mod request;
mod response;
#[cfg(test)]
mod tests;

pub use query::{parse_form, parse_query, percent_decode, percent_decode_plus};
pub use request::{Body, BodyReader, ContentEncoding, Method, Request};
pub use response::ResponseWriter;

use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Request handler: called once per request on the connection's thread.
///
/// For `HEAD` requests write the response as if it were `GET`; the writer
/// suppresses the body and sends only the headers (with `Content-Length`).
pub type Handler = dyn Fn(&mut Request<'_>, &mut ResponseWriter<'_>) + Send + Sync;

/// How long `stop()` waits for connection threads to drain.
const STOP_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
/// Poll interval for stop checks while waiting for a connection slot.
const SLOT_POLL_INTERVAL: Duration = Duration::from_millis(100);

pub struct ServerConfig {
    /// Gzip buffered responses (flate2 level 1) when the client sent
    /// `Accept-Encoding: gzip` (mirrors the upstream httpserver
    /// default). Off by default; streamed responses are never compressed.
    pub compress_responses: bool,
    /// Per-read socket timeout. `None` (default) blocks indefinitely;
    /// `stop()` still unblocks reads via socket shutdown.
    pub read_timeout: Option<Duration>,
    /// Cap on concurrent connections; the accept loop applies backpressure
    /// (stops accepting) at the cap. Default 4096.
    pub max_connections: usize,
    /// Capture every request header into [`Request::all_headers`]. Off by
    /// default so the TSDB endpoints' fast path (which only needs a
    /// handful of headers) is unaffected; an auth proxy that must forward
    /// arbitrary headers upstream sets this to true.
    pub capture_all_headers: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            compress_responses: false,
            read_timeout: None,
            max_connections: 4096,
            capture_all_headers: false,
        }
    }
}

pub struct Server {
    inner: Arc<ServerInner>,
    accept_handle: Mutex<Option<JoinHandle<()>>>,
}

pub(crate) struct ServerInner {
    listener: TcpListener,
    addr: SocketAddr,
    pub(crate) config: ServerConfig,
    stopped: AtomicBool,
    next_conn_id: AtomicU64,
    conns: Mutex<HashMap<u64, TcpStream>>,
    active: Mutex<usize>,
    active_changed: Condvar,
}

impl ServerInner {
    pub(crate) fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}

impl Server {
    /// Binds with the default [`ServerConfig`].
    pub fn bind(addr: &str) -> io::Result<Server> {
        Server::bind_with_config(addr, ServerConfig::default())
    }

    pub fn bind_with_config(addr: &str, config: ServerConfig) -> io::Result<Server> {
        let listener = TcpListener::bind(addr)?;
        let addr = listener.local_addr()?;
        Ok(Server {
            inner: Arc::new(ServerInner {
                listener,
                addr,
                config,
                stopped: AtomicBool::new(false),
                next_conn_id: AtomicU64::new(0),
                conns: Mutex::new(HashMap::new()),
                active: Mutex::new(0),
                active_changed: Condvar::new(),
            }),
            accept_handle: Mutex::new(None),
        })
    }

    /// The bound address (useful with port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.addr
    }

    /// Spawns the accept loop and returns immediately. The handler runs on
    /// a dedicated thread per connection.
    ///
    /// # Panics
    /// Panics if called more than once.
    pub fn serve(&self, handler: Arc<Handler>) {
        let mut guard = self.accept_handle.lock().unwrap();
        assert!(guard.is_none(), "Server::serve called twice");
        let inner = Arc::clone(&self.inner);
        let handle = std::thread::Builder::new()
            .name("esm-http-accept".to_owned())
            .spawn(move || accept_loop(&inner, &handler))
            .expect("failed to spawn accept thread");
        *guard = Some(handle);
    }

    /// Graceful shutdown; see the crate docs for the mechanics. Idempotent.
    pub fn stop(&self) {
        if self.inner.stopped.swap(true, Ordering::AcqRel) {
            return;
        }
        self.inner.active_changed.notify_all();

        // Wake the blocking accept() with a loopback self-connect.
        let wake_ip = match self.inner.addr.ip() {
            IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
            ip => ip,
        };
        let wake_addr = SocketAddr::new(wake_ip, self.inner.addr.port());
        let _ = TcpStream::connect_timeout(&wake_addr, Duration::from_millis(250));

        // Unblock in-flight reads on every live connection.
        for stream in self.inner.conns.lock().unwrap().values() {
            let _ = stream.shutdown(Shutdown::Both);
        }

        if let Some(handle) = self.accept_handle.lock().unwrap().take() {
            let _ = handle.join();
        }

        // Drain: wait for connection threads to finish.
        let deadline = Instant::now() + STOP_DRAIN_TIMEOUT;
        let mut active = self.inner.active.lock().unwrap();
        while *active > 0 {
            let now = Instant::now();
            if now >= deadline {
                log::warn!(
                    "esm-http: stop timed out with {} connection(s) still active",
                    *active
                );
                break;
            }
            let (guard, _) = self
                .inner
                .active_changed
                .wait_timeout(active, deadline - now)
                .unwrap();
            active = guard;
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}

fn accept_loop(inner: &Arc<ServerInner>, handler: &Arc<Handler>) {
    loop {
        if !wait_for_slot(inner) {
            break;
        }
        let stream = match inner.listener.accept() {
            Ok((stream, _)) => stream,
            Err(err) => {
                if inner.is_stopped() {
                    break;
                }
                log::warn!("esm-http: accept error: {err}");
                std::thread::sleep(Duration::from_millis(10));
                continue;
            }
        };
        if inner.is_stopped() {
            break; // stop() wake-up connection or a raced client; drop it
        }

        let id = inner.next_conn_id.fetch_add(1, Ordering::Relaxed);
        if let Ok(clone) = stream.try_clone() {
            inner.conns.lock().unwrap().insert(id, clone);
        }
        // Close the race where stop() shut down registered connections
        // just before this one was registered.
        if inner.is_stopped() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        *inner.active.lock().unwrap() += 1;

        let inner_conn = Arc::clone(inner);
        let handler_conn = Arc::clone(handler);
        let spawned = std::thread::Builder::new()
            .name(format!("esm-http-conn-{id}"))
            .spawn(move || {
                // Guard so registry/counter cleanup happens even if the
                // handler panics.
                let _guard = ConnGuard {
                    inner: &inner_conn,
                    id,
                };
                conn::handle_connection(&inner_conn, &stream, &*handler_conn);
            });
        if let Err(err) = spawned {
            log::warn!("esm-http: failed to spawn connection thread: {err}");
            finish_connection(inner, id);
        }
    }
}

/// Blocks until the connection count is below the cap. Returns `false`
/// when the server is stopping.
fn wait_for_slot(inner: &ServerInner) -> bool {
    let mut active = inner.active.lock().unwrap();
    loop {
        if inner.is_stopped() {
            return false;
        }
        if *active < inner.config.max_connections {
            return true;
        }
        let (guard, _) = inner
            .active_changed
            .wait_timeout(active, SLOT_POLL_INTERVAL)
            .unwrap();
        active = guard;
    }
}

fn finish_connection(inner: &ServerInner, id: u64) {
    inner.conns.lock().unwrap().remove(&id);
    *inner.active.lock().unwrap() -= 1;
    inner.active_changed.notify_all();
}

struct ConnGuard<'a> {
    inner: &'a ServerInner,
    id: u64,
}

impl Drop for ConnGuard<'_> {
    fn drop(&mut self) {
        finish_connection(self.inner, self.id);
    }
}
