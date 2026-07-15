//! TCP + UDP ingest servers for the Graphite plaintext and OpenTSDB telnet
//! `put` protocols.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `lib/ingestserver/graphite/server.go` and `lib/ingestserver/opentsdb/server.go`.
//!
//! # Simplifications vs. upstream
//!
//! - **No proxy protocol** (`useProxyProtocol`/`netutil.NewTCPListener`) —
//!   not needed by this port's TSBS-focused scope.
//! - **Single UDP reader thread**, not upstream's `cgroup.AvailableCPUs()`
//!   worker pool reading concurrently off the same `net.PacketConn`
//!   (`server.go`'s `serveUDP`: `for range gomaxprocs { wg.Go(...) }`). A
//!   single thread is adequate for this port's ingest volumes and avoids
//!   needing a `Sync` pool of per-worker `ConvertCtx` buffers; documented
//!   here per the task brief's explicit allowance for this simplification.
//! - **OpenTSDB TCP is telnet-only.** Upstream's `opentsdb.Server` (`server.go`)
//!   multiplexes telnet *and* HTTP `/api/put` traffic on the same TCP
//!   listener via a `listenerSwitch` that peeks the first bytes of each
//!   connection to route it. `-opentsdbHTTPListenAddr` (`opentsdbhttp.Server`,
//!   a distinct HTTP listener) is a separate task; this module only serves
//!   the telnet `put ...` protocol on `serve_opentsdb_telnet`.
//! - **`esm_ingestserver_requests_total{type=..., name="write", net="tcp"|"udp"} (renamed from upstream vm_ingestserver_requests_total)`
//!   is ported** (see [`GRAPHITE_REQUESTS_TCP`] and friends) — incremented
//!   once per accepted TCP connection / once per received UDP datagram,
//!   right before the line handler processes it, matching
//!   `writeRequestsTCP.Inc()`/`writeRequestsUDP.Inc()`'s placement in
//!   `lib/ingestserver/graphite/server.go` and
//!   `lib/ingestserver/opentsdb/server.go`. `vm_ingestserver_request_errors_total`
//!   is **not** ported — this port logs (rather than counts) handler
//!   failures already (see `LineHandler`'s doc), and wiring a second counter
//!   through the same closure return path was judged not worth the
//!   complexity for this pass; only counters, not the accompanying error
//!   metric, are in scope here.
//! - **Connection close ordering**: upstream's `ConnsMap.CloseAll` closes
//!   connections gradually by remote IP (`lib/ingestserver/conns_map.go`) to
//!   spread client reconnect load; this port closes all registered
//!   connections at once (`shutdownDuration <= 0` in Go terms), matching
//!   the simpler shutdown already used by `esm_http::Server::stop`.
//!
//! # Shutdown mechanics
//!
//! Mirrors [`esm_http::Server`]'s documented approach for consistency:
//! 1. An `AtomicBool` (`stopped`) is set first; both loops check it between
//!    blocking operations.
//! 2. TCP: the blocking `accept()` is woken via a loopback self-connect (the
//!    same portable trick `esm_http::Server::stop` uses — no unix-only
//!    `Shutdown`-on-the-listener tricks, since `std::net::TcpListener` has
//!    no `shutdown()`/close-from-another-thread API). Every live connection
//!    is registered in a `Mutex<HashMap<id, TcpStream>>` of `try_clone`d
//!    handles; `stop()` calls `shutdown(Shutdown::Both)` on each. That
//!    cross-thread `shutdown()` reliably wakes a blocked `read()` on Unix
//!    but NOT on Windows (WinSock only guarantees *subsequent* calls fail;
//!    a pending `recv` can stay blocked), so it is a fast-path only: the
//!    portable guarantee is a `TCP_READ_POLL_INTERVAL` read timeout on
//!    every accepted connection, with [`StopPollReader`] absorbing the
//!    timeouts and returning EOF once `stopped` is set — the same polling
//!    pattern the UDP path uses.
//! 3. UDP: `std::net::UdpSocket` has no portable interrupt either, so the
//!    reader thread polls `stopped` between short (`UDP_POLL_INTERVAL`)
//!    `recv_from` timeouts instead of a wakeup datagram — a wakeup datagram
//!    would otherwise need a distinguishable sentinel payload (to avoid the
//!    reader mistaking it for real ingest data and logging a bogus parse
//!    error), and polling is simpler and equally portable (Windows UDP
//!    sockets support `set_read_timeout` too).
//! 4. `stop()` then waits (condvar on an active-connection counter, capped
//!    at 5 s, matching `esm_http`'s `STOP_DRAIN_TIMEOUT`) for all
//!    connection threads to drain before returning.

use std::collections::HashMap;
use std::io::{self, Read};
use std::net::{
    IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpListener, TcpStream, UdpSocket,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use esm_common::metrics::{get_or_create_counter, Counter};
use esm_protoparser::{graphite, opentsdb};

use crate::RowSink;

/// Go: `vm_ingestserver_requests_total{type="graphite", name="write", net="tcp"}`
/// (`lib/ingestserver/graphite/server.go`).
static GRAPHITE_REQUESTS_TCP: LazyLock<&'static Counter> = LazyLock::new(|| {
    get_or_create_counter(
        r#"esm_ingestserver_requests_total{type="graphite", name="write", net="tcp"}"#,
    )
});
/// Go: `vm_ingestserver_requests_total{type="graphite", name="write", net="udp"}`
/// (`lib/ingestserver/graphite/server.go`).
static GRAPHITE_REQUESTS_UDP: LazyLock<&'static Counter> = LazyLock::new(|| {
    get_or_create_counter(
        r#"esm_ingestserver_requests_total{type="graphite", name="write", net="udp"}"#,
    )
});
/// Go: `vm_ingestserver_requests_total{type="opentsdb", name="write", net="tcp"}`
/// (`lib/ingestserver/opentsdb/server.go`).
static OPENTSDB_REQUESTS_TCP: LazyLock<&'static Counter> = LazyLock::new(|| {
    get_or_create_counter(
        r#"esm_ingestserver_requests_total{type="opentsdb", name="write", net="tcp"}"#,
    )
});
/// Go: `vm_ingestserver_requests_total{type="opentsdb", name="write", net="udp"}`
/// (`lib/ingestserver/opentsdb/server.go`).
static OPENTSDB_REQUESTS_UDP: LazyLock<&'static Counter> = LazyLock::new(|| {
    get_or_create_counter(
        r#"esm_ingestserver_requests_total{type="opentsdb", name="write", net="udp"}"#,
    )
});

/// How long `stop()` waits for connection threads to drain. Matches
/// `esm_http::Server`'s `STOP_DRAIN_TIMEOUT`.
const STOP_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
/// UDP socket read timeout used to poll the `stopped` flag; see the module
/// doc's "Shutdown mechanics" section.
const UDP_POLL_INTERVAL: Duration = Duration::from_millis(200);
/// TCP per-connection read timeout used to poll the `stopped` flag —
/// the portable stop wakeup (cross-thread `shutdown()` does not unblock a
/// pending `recv` on Windows); see the module doc.
const TCP_READ_POLL_INTERVAL: Duration = Duration::from_millis(200);
/// UDP datagram receive buffer size. Go: `64*1024` in `server.go`'s
/// `serveUDP`.
const UDP_BUF_SIZE: usize = 64 * 1024;

/// Per-datagram/per-connection line processor: parses the protocol from `r`
/// and pushes converted rows to the sink, logging (not propagating) any
/// parse/sink error, matching Go's `logger.Errorf` on `insertHandler`
/// failure in `serveTCP`/`serveUDP`.
type LineHandler = dyn Fn(&mut dyn Read) + Send + Sync;

/// A running TCP+UDP ingest listener. Stop with [`IngestServer::stop`].
pub struct IngestServer {
    inner: Arc<Inner>,
    tcp_accept_handle: Mutex<Option<JoinHandle<()>>>,
    udp_handle: Mutex<Option<JoinHandle<()>>>,
}

struct Inner {
    tcp_listener: TcpListener,
    udp_socket: UdpSocket,
    addr: SocketAddr,
    stopped: AtomicBool,
    next_conn_id: AtomicU64,
    conns: Mutex<HashMap<u64, TcpStream>>,
    active: Mutex<usize>,
    active_changed: Condvar,
    /// `vm_ingestserver_requests_total{..., net="tcp"}` for this listener's
    /// protocol (graphite or opentsdb).
    requests_tcp: &'static Counter,
    /// `vm_ingestserver_requests_total{..., net="udp"}` for this listener's
    /// protocol (graphite or opentsdb).
    requests_udp: &'static Counter,
}

impl Inner {
    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}

/// Serves Graphite plaintext lines on TCP and UDP at `addr`. Each parsed
/// batch of lines goes through [`crate::graphite::insert_rows`] — the same
/// converter the HTTP ingestion path would use.
///
/// Go: `lib/ingestserver/graphite.MustStart` +
/// `app/vminsert/graphite.InsertHandler`.
pub fn serve_graphite<S: RowSink + 'static>(addr: &str, sink: Arc<S>) -> io::Result<IngestServer> {
    serve(
        addr,
        move |r: &mut dyn Read| {
            let result = graphite::parse_stream(
                r,
                "",
                |msg| log::warn!("graphite: {msg}"),
                |rows| crate::graphite::insert_rows(&*sink, rows).map_err(Into::into),
            );
            if let Err(err) = result {
                log::warn!("graphite: error processing ingest data: {err}");
            }
        },
        *GRAPHITE_REQUESTS_TCP,
        *GRAPHITE_REQUESTS_UDP,
    )
}

/// Serves the OpenTSDB telnet `put` protocol on TCP and UDP at `addr`.
/// `-opentsdbHTTPListenAddr` HTTP `/api/put` traffic is a separate listener
/// (task 15), not multiplexed onto this one — see the module doc's
/// "Simplifications" section.
///
/// Go: `lib/ingestserver/opentsdb.MustStart`'s telnet path +
/// `app/vminsert/opentsdb.InsertHandler`.
pub fn serve_opentsdb_telnet<S: RowSink + 'static>(
    addr: &str,
    sink: Arc<S>,
) -> io::Result<IngestServer> {
    serve(
        addr,
        move |r: &mut dyn Read| {
            let result = opentsdb::parse_stream(
                r,
                |msg| log::warn!("opentsdb: {msg}"),
                |rows| crate::opentsdb::insert_rows(&*sink, rows).map_err(Into::into),
            );
            if let Err(err) = result {
                log::warn!("opentsdb: error processing ingest data: {err}");
            }
        },
        *OPENTSDB_REQUESTS_TCP,
        *OPENTSDB_REQUESTS_UDP,
    )
}

fn serve(
    addr: &str,
    process: impl Fn(&mut dyn Read) + Send + Sync + 'static,
    requests_tcp: &'static Counter,
    requests_udp: &'static Counter,
) -> io::Result<IngestServer> {
    let tcp_listener = TcpListener::bind(addr)?;
    let bound_addr = tcp_listener.local_addr()?;
    // Bind UDP to the exact resolved TCP address so both protocols share the
    // same port even when `addr` uses the ephemeral-port wildcard (`:0`),
    // same as upstream binding both to the single configured `addr` flag.
    let udp_socket = UdpSocket::bind(bound_addr)?;
    udp_socket.set_read_timeout(Some(UDP_POLL_INTERVAL))?;

    let inner = Arc::new(Inner {
        tcp_listener,
        udp_socket,
        addr: bound_addr,
        stopped: AtomicBool::new(false),
        next_conn_id: AtomicU64::new(0),
        conns: Mutex::new(HashMap::new()),
        active: Mutex::new(0),
        active_changed: Condvar::new(),
        requests_tcp,
        requests_udp,
    });
    let process: Arc<LineHandler> = Arc::new(process);

    let tcp_inner = Arc::clone(&inner);
    let tcp_process = Arc::clone(&process);
    let tcp_accept_handle = std::thread::Builder::new()
        .name("esm-insert-ingest-accept".to_owned())
        .spawn(move || accept_loop(&tcp_inner, &tcp_process))
        .expect("failed to spawn ingest accept thread");

    let udp_inner = Arc::clone(&inner);
    let udp_process = Arc::clone(&process);
    let udp_handle = std::thread::Builder::new()
        .name("esm-insert-ingest-udp".to_owned())
        .spawn(move || udp_loop(&udp_inner, &udp_process))
        .expect("failed to spawn ingest UDP thread");

    Ok(IngestServer {
        inner,
        tcp_accept_handle: Mutex::new(Some(tcp_accept_handle)),
        udp_handle: Mutex::new(Some(udp_handle)),
    })
}

impl IngestServer {
    /// The bound address shared by the TCP and UDP listeners (useful with
    /// port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.addr
    }

    /// Graceful shutdown; see the module doc for the mechanics. Closes both
    /// listeners and joins the accept/UDP/connection threads.
    pub fn stop(self) {
        self.stop_inner();
    }

    fn stop_inner(&self) {
        if self.inner.stopped.swap(true, Ordering::AcqRel) {
            return;
        }
        self.inner.active_changed.notify_all();

        // Wake the blocking TCP accept() with a loopback self-connect (same
        // trick as esm_http::Server::stop).
        let wake_ip = match self.inner.addr.ip() {
            IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
            ip => ip,
        };
        let wake_addr = SocketAddr::new(wake_ip, self.inner.addr.port());
        let _ = TcpStream::connect_timeout(&wake_addr, Duration::from_millis(250));

        // Unblock in-flight reads on every live TCP connection.
        for stream in self.inner.conns.lock().unwrap().values() {
            let _ = stream.shutdown(Shutdown::Both);
        }

        if let Some(handle) = self.tcp_accept_handle.lock().unwrap().take() {
            let _ = handle.join();
        }
        // The UDP thread wakes on its own via its read timeout (see
        // `udp_loop`); no wakeup datagram needed.
        if let Some(handle) = self.udp_handle.lock().unwrap().take() {
            let _ = handle.join();
        }

        // Drain: wait for per-connection threads to finish.
        let deadline = Instant::now() + STOP_DRAIN_TIMEOUT;
        let mut active = self.inner.active.lock().unwrap();
        while *active > 0 {
            let now = Instant::now();
            if now >= deadline {
                log::warn!(
                    "ingestserver: stop timed out with {} connection(s) still active",
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

impl Drop for IngestServer {
    fn drop(&mut self) {
        self.stop_inner();
    }
}

fn accept_loop(inner: &Arc<Inner>, process: &Arc<LineHandler>) {
    loop {
        let stream = match inner.tcp_listener.accept() {
            Ok((stream, _)) => stream,
            Err(err) => {
                if inner.is_stopped() {
                    break;
                }
                log::warn!("ingestserver: accept error: {err}");
                std::thread::sleep(Duration::from_millis(10));
                continue;
            }
        };
        if inner.is_stopped() {
            break; // stop()'s wakeup connection or a raced client; drop it.
        }

        let id = inner.next_conn_id.fetch_add(1, Ordering::Relaxed);
        if let Ok(clone) = stream.try_clone() {
            inner.conns.lock().unwrap().insert(id, clone);
        }
        // The portable stop wakeup: without this, a connection blocked in
        // read() on Windows survives stop()'s shutdown() until the client
        // sends data or disconnects (see the module doc).
        let _ = stream.set_read_timeout(Some(TCP_READ_POLL_INTERVAL));
        // Close the race where stop() shut down registered connections just
        // before this one was registered.
        if inner.is_stopped() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        *inner.active.lock().unwrap() += 1;

        let inner_conn = Arc::clone(inner);
        let process_conn = Arc::clone(process);
        let stream_for_read = stream;
        let spawned = std::thread::Builder::new()
            .name(format!("esm-insert-ingest-conn-{id}"))
            .spawn(move || {
                // Guard so registry/active-connection-counter cleanup
                // happens even if the handler panics.
                let _guard = ConnGuard {
                    inner: &inner_conn,
                    id,
                };
                let mut reader = StopPollReader {
                    stream: stream_for_read,
                    inner: &inner_conn,
                };
                // Go: `writeRequestsTCP.Inc()` right before `insertHandler(c)`
                // (`server.go`'s `serveTCP`) — once per accepted connection
                // that reaches this point (the stop() wakeup connection
                // never gets here; see the `is_stopped()` check above).
                inner_conn.requests_tcp.inc();
                process_conn(&mut reader);
            });
        if let Err(err) = spawned {
            log::warn!("ingestserver: failed to spawn connection thread: {err}");
            finish_connection(inner, id);
        }
    }
}

fn udp_loop(inner: &Arc<Inner>, process: &Arc<LineHandler>) {
    // Single reader thread; see the module doc's "Simplifications" section
    // for why this departs from upstream's per-CPU worker pool.
    let mut buf = vec![0u8; UDP_BUF_SIZE];
    while !inner.is_stopped() {
        match inner.udp_socket.recv_from(&mut buf) {
            Ok((n, _addr)) => {
                // Go: `writeRequestsUDP.Inc()` right after a successful
                // `ReadFrom` and before `insertHandler` (`server.go`'s
                // `serveUDP`) — once per received datagram.
                inner.requests_udp.inc();
                let mut cursor = &buf[..n];
                process(&mut cursor);
            }
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(err) => {
                if inner.is_stopped() {
                    break;
                }
                log::warn!("ingestserver: UDP read error: {err}");
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

/// A [`Read`] over an accepted TCP stream (with `TCP_READ_POLL_INTERVAL`
/// read timeout) that absorbs the periodic timeouts: on `WouldBlock`/
/// `TimedOut` it keeps reading unless the server is stopping, in which
/// case it reports clean EOF so the line parser flushes any pending tail
/// and the connection thread exits. See the module doc's shutdown
/// mechanics — this, not the cross-thread `shutdown()`, is the portable
/// stop wakeup.
struct StopPollReader<'a> {
    stream: TcpStream,
    inner: &'a Inner,
}

impl Read for StopPollReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            match self.stream.read(buf) {
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    if self.inner.is_stopped() {
                        return Ok(0);
                    }
                }
                other => return other,
            }
        }
    }
}

/// Removes a finished connection from the registry and decrements the
/// active-connection counter, waking anyone in `stop()`'s drain wait.
fn finish_connection(inner: &Inner, id: u64) {
    inner.conns.lock().unwrap().remove(&id);
    *inner.active.lock().unwrap() -= 1;
    inner.active_changed.notify_all();
}

struct ConnGuard<'a> {
    inner: &'a Inner,
    id: u64,
}

impl Drop for ConnGuard<'_> {
    fn drop(&mut self) {
        finish_connection(self.inner, self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetricRow;
    use esm_storage::MetricName;
    use std::io::Write;
    use std::net::{TcpStream as StdTcpStream, UdpSocket as StdUdpSocket};
    use std::sync::Mutex as StdMutex;
    use std::time::{Duration as StdDuration, Instant as StdInstant};

    #[derive(Debug, Clone, PartialEq)]
    struct GotRow {
        metric_group: String,
        tags: Vec<(String, String)>,
        timestamp: i64,
        value: f64,
    }

    #[derive(Default)]
    struct MockSink {
        rows: StdMutex<Vec<GotRow>>,
    }

    impl RowSink for MockSink {
        fn add_rows(&self, rows: &[MetricRow<'_>]) -> Result<(), String> {
            let mut got = self.rows.lock().unwrap();
            for row in rows {
                let mut mn = MetricName::default();
                mn.unmarshal_raw(row.metric_name_raw)
                    .map_err(|err| format!("cannot unmarshal metric name: {err}"))?;
                got.push(GotRow {
                    metric_group: String::from_utf8(mn.metric_group.clone()).unwrap(),
                    tags: mn
                        .tags
                        .iter()
                        .map(|t| {
                            (
                                String::from_utf8(t.key.clone()).unwrap(),
                                String::from_utf8(t.value.clone()).unwrap(),
                            )
                        })
                        .collect(),
                    timestamp: row.timestamp,
                    value: row.value,
                });
            }
            Ok(())
        }
    }

    /// Polls `sink` until it has at least `want_len` rows or `timeout`
    /// elapses (rows arrive on a background thread). Pattern per
    /// `tests/influx_write.rs`'s synchronous-client-vs-background-server
    /// shape, adapted for a fire-and-forget TCP/UDP write.
    fn wait_for_rows(sink: &MockSink, want_len: usize, timeout: StdDuration) -> Vec<GotRow> {
        let deadline = StdInstant::now() + timeout;
        loop {
            let rows = sink.rows.lock().unwrap().clone();
            if rows.len() >= want_len || StdInstant::now() >= deadline {
                return rows;
            }
            std::thread::sleep(StdDuration::from_millis(5));
        }
    }

    const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(5);

    #[test]
    fn graphite_tcp_write_converts_rows() {
        let sink = Arc::new(MockSink::default());
        let server = serve_graphite("127.0.0.1:0", Arc::clone(&sink)).unwrap();
        let addr = server.local_addr();

        let mut stream = StdTcpStream::connect(addr).unwrap();
        stream
            .write_all(b"foo.bar;env=prod 1.5 100\nfoo.baz 2 200\n")
            .unwrap();
        drop(stream); // half-close so the server sees EOF after the lines

        let rows = wait_for_rows(&sink, 2, TEST_TIMEOUT);
        assert_eq!(rows.len(), 2, "rows: {rows:?}");
        assert_eq!(rows[0].metric_group, "foo.bar");
        assert_eq!(rows[0].tags, vec![("env".to_owned(), "prod".to_owned())]);
        assert_eq!(rows[0].timestamp, 100_000);
        assert_eq!(rows[0].value, 1.5);
        assert_eq!(rows[1].metric_group, "foo.baz");
        assert_eq!(rows[1].timestamp, 200_000);
        assert_eq!(rows[1].value, 2.0);

        server.stop();
    }

    #[test]
    fn graphite_udp_write_converts_rows() {
        let sink = Arc::new(MockSink::default());
        let server = serve_graphite("127.0.0.1:0", Arc::clone(&sink)).unwrap();
        let addr = server.local_addr();

        let client = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        client.send_to(b"foo.bar 42 300\n", addr).expect("send_to");

        let rows = wait_for_rows(&sink, 1, TEST_TIMEOUT);
        assert_eq!(rows.len(), 1, "rows: {rows:?}");
        assert_eq!(rows[0].metric_group, "foo.bar");
        assert_eq!(rows[0].timestamp, 300_000);
        assert_eq!(rows[0].value, 42.0);

        server.stop();
    }

    #[test]
    fn opentsdb_telnet_tcp_write_converts_rows() {
        let sink = Arc::new(MockSink::default());
        let server = serve_opentsdb_telnet("127.0.0.1:0", Arc::clone(&sink)).unwrap();
        let addr = server.local_addr();

        let mut stream = StdTcpStream::connect(addr).unwrap();
        stream
            .write_all(b"put sys.cpu 100 42 host=h1\nput sys.mem 200 7 host=h2\n")
            .unwrap();
        drop(stream);

        let rows = wait_for_rows(&sink, 2, TEST_TIMEOUT);
        assert_eq!(rows.len(), 2, "rows: {rows:?}");
        assert_eq!(rows[0].metric_group, "sys.cpu");
        assert_eq!(rows[0].tags, vec![("host".to_owned(), "h1".to_owned())]);
        assert_eq!(rows[0].timestamp, 100_000);
        assert_eq!(rows[0].value, 42.0);
        assert_eq!(rows[1].metric_group, "sys.mem");
        assert_eq!(rows[1].timestamp, 200_000);
        assert_eq!(rows[1].value, 7.0);

        server.stop();
    }

    #[test]
    fn opentsdb_telnet_udp_write_converts_rows() {
        let sink = Arc::new(MockSink::default());
        let server = serve_opentsdb_telnet("127.0.0.1:0", Arc::clone(&sink)).unwrap();
        let addr = server.local_addr();

        let client = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        client
            .send_to(b"put sys.cpu 400 99 host=h3\n", addr)
            .expect("send_to");

        let rows = wait_for_rows(&sink, 1, TEST_TIMEOUT);
        assert_eq!(rows.len(), 1, "rows: {rows:?}");
        assert_eq!(rows[0].metric_group, "sys.cpu");
        assert_eq!(rows[0].tags, vec![("host".to_owned(), "h3".to_owned())]);
        assert_eq!(rows[0].timestamp, 400_000);
        assert_eq!(rows[0].value, 99.0);

        server.stop();
    }

    #[test]
    fn stop_joins_cleanly_without_hanging() {
        let sink = Arc::new(MockSink::default());
        let server = serve_graphite("127.0.0.1:0", Arc::clone(&sink)).unwrap();

        let start = StdInstant::now();
        server.stop();
        assert!(
            start.elapsed() < StdDuration::from_secs(3),
            "stop() took too long: {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn stop_drains_an_open_connection() {
        let sink = Arc::new(MockSink::default());
        let server = serve_graphite("127.0.0.1:0", Arc::clone(&sink)).unwrap();
        let addr = server.local_addr();

        // Open a connection and leave it open (no EOF) while stopping, to
        // exercise the shutdown(Both)-unblocks-the-read path.
        let mut stream = StdTcpStream::connect(addr).unwrap();
        stream.write_all(b"foo 1 100\n").unwrap();

        let rows = wait_for_rows(&sink, 1, TEST_TIMEOUT);
        assert_eq!(rows.len(), 1);

        let start = StdInstant::now();
        server.stop();
        assert!(
            start.elapsed() < StdDuration::from_secs(3),
            "stop() took too long with an open connection: {:?}",
            start.elapsed()
        );
    }
}
