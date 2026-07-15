//! Blocking-thread remote-write client. Port of `remotewrite.Client`
//! (`remotewrite/client.go:52-269`), narrowed to what esmalert's rule
//! evaluator needs: `push` is a non-blocking append to a bounded queue and a
//! background thread flushes on a timer (with jitter) or when the queue
//! reaches `max_batch_size`.
//!
//! ## Queue holds owned `Series`, not `prompb::TimeSeries`
//!
//! Upstream's queue holds `prompb.TimeSeries` directly. This port's queue
//! holds [`crate::series::Series`] — an owned type — instead, because
//! `esm_protoparser::prompb::TimeSeries<'a>` borrows `&'a [u8]` label bytes
//! from a caller-owned buffer and can't outlive that buffer, let alone be
//! queued and handed across to a background thread. Each queued `Series` is
//! converted to a borrowed `prompb::TimeSeries<'_>` only at the moment of
//! encoding, in [`to_borrowed`], so the borrow's lifetime never has to
//! outlive one `encode_and_compress` call.
//!
//! ## Queue + flush thread design
//!
//! `Inner` holds a `Mutex<VecDeque<Series>>` plus a `Condvar` used both to
//! wake a background thread early (when a push makes the queue reach
//! `max_batch_size`) and to wake it for shutdown. `RwConfig::concurrency`
//! worker threads are spawned in [`RwClient::start`]; each runs [`run`],
//! which alternates between waiting on the condvar (bounded by
//! `flush_interval`, with a startup jitter derived deterministically from a
//! hash of the URL — see [`jitter_duration`] — so multiple esmalert
//! instances writing to the same target don't all flush in lockstep) and
//! draining whatever is queued via [`Inner::drain_and_flush`]. Every worker
//! shares the same queue/condvar, so extra concurrency simply adds more
//! drainers rather than partitioning the queue.
//!
//! [`RwClient::flush_now`] calls [`Inner::drain_and_flush`] directly from
//! the caller's thread — it does not wait for a worker to wake up, so tests
//! (and graceful shutdown) get a synchronous, deterministic flush.
//!
//! [`RwClient::shutdown`] sets a `stopping` flag and notifies every worker;
//! each worker observes the flag after waking, performs one more drain (to
//! catch anything pushed just before shutdown), and returns, after which
//! `shutdown` joins every thread. The queue lock is never held across the
//! blocking POST — [`Inner::drain_and_flush`] takes the lock only to pull a
//! batch out of the `VecDeque`, then drops it before calling
//! [`Inner::send_batch`].
//!
//! Encode and POST failures are logged (`log::warn!`) and the batch is
//! dropped; neither `drain_and_flush` nor the worker loop ever panics or
//! propagates such failures as an `Err`, so a single bad batch can't kill
//! the flush thread. Retries/backoff (upstream's `retryMinInterval`/
//! `retryMaxTime`) are out of scope for this task — see the task report for
//! this and other deviations.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use reqwest::blocking::Client;
use url::Url;

use esm_protoparser::prompb::{Label, TimeSeries};
use esm_protoparser::prompb_encode::encode_and_compress;

use crate::config::Header;
use crate::datasource::{AuthConfig, TlsConfig};
use crate::series::Series;

use super::RwError;

/// Path appended to the configured base URL, matching upstream's default
/// (`-remoteWrite.disablePathAppend=false`) behavior. This port always
/// appends it; there is no flag to disable the append.
const API_WRITE_PATH: &str = "/api/v1/write";

/// Config for [`RwClient::start`]. Port of `remotewrite.Config`
/// (`client.go:71-90`), narrowed to what esmalert's rule evaluator needs
/// (upstream's `Transport` field has no equivalent here — see
/// `build_client`, which always builds its own rustls-backed
/// `reqwest::blocking::Client`).
pub struct RwConfig {
    pub url: String,
    pub flush_interval: Duration,
    pub max_batch_size: usize,
    pub max_queue_size: usize,
    pub concurrency: usize,
    /// Per-request send timeout applied to the `reqwest::blocking::Client`.
    /// Port of upstream's `-remoteWrite.sendTimeout` (default 30s,
    /// `client.go:45`). Critical for shutdown liveness: without it a stalled
    /// remote endpoint blocks a worker inside `req.send()` forever, so the
    /// worker never returns to observe `stopping` and [`RwClient::shutdown`]'s
    /// `join()` hangs. Use [`DEFAULT_SEND_TIMEOUT`] when a caller has no
    /// specific value.
    pub send_timeout: Duration,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
    pub headers: Vec<Header>,
}

/// Default per-request send timeout, matching upstream's
/// `-remoteWrite.sendTimeout` default (`client.go:45`). Callers that build a
/// [`RwConfig`] without a specific timeout should use this.
pub const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// Shared state between [`RwClient`] and its background worker threads.
struct Inner {
    queue: Mutex<VecDeque<Series>>,
    cv: Condvar,
    max_queue_size: usize,
    max_batch_size: usize,
    flush_interval: Duration,
    stopping: AtomicBool,
    client: Client,
    url: Url,
    headers: Vec<Header>,
    auth: AuthConfig,
}

impl Inner {
    /// Non-blocking append. Drops (with a `log::warn!`) instead of blocking
    /// when the queue is already at `max_queue_size`, and wakes a worker
    /// early once the queue reaches `max_batch_size`. Port of `Client.Push`
    /// (`client.go:139-155`), minus the upstream error return — this port's
    /// `push` never fails; a full queue is signaled only via the log, since
    /// [`RwConfig`]'s `push` signature (per the task brief) has no `Result`.
    fn push(&self, series: Series) {
        let mut q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        if q.len() >= self.max_queue_size {
            drop(q);
            log::warn!(
                "esmalert remote-write: queue is full ({} entries); dropping series",
                self.max_queue_size
            );
            return;
        }
        q.push_back(series);
        let batch_full = q.len() >= self.max_batch_size;
        drop(q);
        if batch_full {
            self.cv.notify_all();
        }
    }

    /// Drains the queue in `max_batch_size` chunks, sending each chunk as
    /// its own `WriteRequest`. The queue lock is held only long enough to
    /// pull one chunk out of the `VecDeque`; it is dropped before
    /// [`Inner::send_batch`]'s blocking POST.
    fn drain_and_flush(&self) {
        loop {
            let batch = {
                let mut q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
                if q.is_empty() {
                    return;
                }
                let n = q.len().min(self.max_batch_size);
                q.drain(..n).collect::<Vec<_>>()
            };
            self.send_batch(&batch);
        }
    }

    /// Encodes `batch` as a `WriteRequest`, snappy-compresses it, and POSTs
    /// it to `<url>`. Never panics: an encode failure or a POST
    /// error/non-2xx response is logged and the batch is dropped rather
    /// than retried (see the module doc comment on scope).
    fn send_batch(&self, batch: &[Series]) {
        if batch.is_empty() {
            return;
        }
        let borrowed = to_borrowed(batch);
        let compressed = match encode_and_compress(&borrowed) {
            Ok(c) => c,
            Err(e) => {
                log::warn!(
                    "esmalert remote-write: failed to encode/compress a batch of {} series: {e}; dropping batch",
                    batch.len()
                );
                return;
            }
        };

        let mut req = self
            .client
            .post(self.url.clone())
            .header("Content-Encoding", "snappy")
            .header("Content-Type", "application/x-protobuf")
            .header("X-Prometheus-Remote-Write-Version", "0.1.0")
            .body(compressed);
        for h in &self.headers {
            req = req.header(h.key.as_str(), h.value.as_str());
        }
        if let Some((user, pass)) = &self.auth.basic {
            req = req.basic_auth(user, Some(pass));
        } else if let Some(token) = &self.auth.bearer {
            req = req.bearer_auth(token);
        }

        match req.send() {
            Ok(resp) => {
                let status = resp.status();
                if !status.is_success() {
                    log::warn!(
                        "esmalert remote-write: POST to {} returned status {status}; dropping batch of {} series",
                        self.url,
                        batch.len()
                    );
                }
            }
            Err(e) => {
                log::warn!(
                    "esmalert remote-write: POST to {} failed: {e}; dropping batch of {} series",
                    self.url,
                    batch.len()
                );
            }
        }
    }
}

/// Converts owned, queued [`Series`] into borrowed
/// `esm_protoparser::prompb::TimeSeries` for exactly the duration of one
/// [`encode_and_compress`] call — see the module doc comment for why the
/// queue can't hold the borrowed type directly.
fn to_borrowed(batch: &[Series]) -> Vec<TimeSeries<'_>> {
    batch
        .iter()
        .map(|s| TimeSeries {
            labels: s
                .labels
                .iter()
                .map(|(name, value)| Label {
                    name: name.as_bytes(),
                    value: value.as_bytes(),
                })
                .collect(),
            samples: s.samples.clone(),
        })
        .collect()
}

/// Background worker loop: wait (bounded by `flush_interval`, jittered on
/// the first iteration) for either a timeout or an early wake (batch full /
/// shutdown), then drain whatever is queued. Runs until `inner.stopping` is
/// observed, performing one final drain before returning so nothing pushed
/// just before shutdown is lost.
///
/// The `stopping`/non-empty predicate is checked *under the queue lock,
/// before* re-entering `wait_timeout` — the standard Condvar pattern that
/// closes the lost-wakeup race. Without it, a `notify_all()` from
/// [`RwClient::shutdown`] landing in the window between a worker's
/// `drain_and_flush()` returning and its next `wait_timeout` would be lost,
/// and that worker wouldn't observe `stopping` until a full `flush_interval`
/// later — needlessly bounding `shutdown()`'s latency by `flush_interval`.
fn run(inner: Arc<Inner>) {
    let mut wait = jitter_duration(inner.url.as_str(), inner.flush_interval);
    loop {
        let stop = {
            let q = inner.queue.lock().unwrap_or_else(|e| e.into_inner());
            // Predicate check under the lock, before waiting: if a shutdown
            // was signaled or work arrived while we were unlocked, don't
            // wait at all. `notify_all` can only be observed by a thread
            // parked in `wait_timeout`, so this pre-check is what makes a
            // notification delivered during that window not get lost.
            if inner.stopping.load(Ordering::SeqCst) || !q.is_empty() {
                inner.stopping.load(Ordering::SeqCst)
            } else {
                let (_guard, _timeout) = inner
                    .cv
                    .wait_timeout(q, wait)
                    .unwrap_or_else(|e| e.into_inner());
                inner.stopping.load(Ordering::SeqCst)
            }
        };
        inner.drain_and_flush();
        if stop {
            return;
        }
        wait = inner.flush_interval;
    }
}

/// Deterministic startup jitter derived from a hash of the remote-write URL
/// (never `rand`), spreading flush timing across multiple esmalert
/// instances writing to the same target. Port of upstream's xxhash-based
/// per-worker jitter (`client.go:191-194`); this port hashes the URL rather
/// than a worker id, so every worker of the same client shares one jittered
/// startup delay.
fn jitter_duration(url: &str, flush_interval: Duration) -> Duration {
    let h = fnv1a64(url.as_bytes());
    let frac = (h as f64) / (u64::MAX as f64);
    Duration::from_secs_f64(flush_interval.as_secs_f64() * frac)
}

/// A small, dependency-free FNV-1a hash, used only for [`jitter_duration`]'s
/// deterministic spread (not a cryptographic or collision-resistant hash).
fn fnv1a64(data: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    for &b in data {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Builds the `<url>/api/v1/write` target, appending to any existing path
/// component of `base_url` (mirrors
/// `datasource::client::Datasource::build_url`'s existing-path handling).
fn build_url(base_url: &str) -> Result<Url, RwError> {
    let mut url = Url::parse(base_url)
        .map_err(|e| RwError::new(format!("invalid remote-write url {base_url:?}: {e}")))?;
    let existing_path = url.path().trim_end_matches('/').to_string();
    url.set_path(&format!("{existing_path}{API_WRITE_PATH}"));
    Ok(url)
}

/// Builds the `reqwest::blocking::Client`, applying `tls` the same way
/// `datasource::client::build_client` / `notifier::alertmanager::build_client`
/// do (duplicated rather than shared, per this repo's established
/// convention of duplicating small already-verified helpers per module),
/// plus a `send_timeout` request timeout (like the notifier client, and
/// unlike the datasource client) so a stalled endpoint can't block a flush
/// worker — and therefore `shutdown()`'s join — indefinitely.
fn build_client(tls: &TlsConfig, send_timeout: Duration) -> Result<Client, RwError> {
    let mut builder = Client::builder().timeout(send_timeout);
    if tls.insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(ca_file) = &tls.ca_file {
        let pem = std::fs::read(ca_file)
            .map_err(|e| RwError::new(format!("cannot read CA file {ca_file:?}: {e}")))?;
        let cert = reqwest::Certificate::from_pem(&pem)
            .map_err(|e| RwError::new(format!("invalid CA certificate in {ca_file:?}: {e}")))?;
        builder = builder.add_root_certificate(cert);
    }
    if let (Some(cert_file), Some(key_file)) = (&tls.cert_file, &tls.key_file) {
        let mut identity_pem = std::fs::read(cert_file)
            .map_err(|e| RwError::new(format!("cannot read cert file {cert_file:?}: {e}")))?;
        let mut key_pem = std::fs::read(key_file)
            .map_err(|e| RwError::new(format!("cannot read key file {key_file:?}: {e}")))?;
        identity_pem.push(b'\n');
        identity_pem.append(&mut key_pem);
        let identity = reqwest::Identity::from_pem(&identity_pem)
            .map_err(|e| RwError::new(format!("invalid client cert/key: {e}")))?;
        builder = builder.identity(identity);
    }
    builder
        .build()
        .map_err(|e| RwError::new(format!("cannot build http client: {e}")))
}

/// Asynchronous remote-write client: [`RwClient::push`] is a non-blocking
/// append to a bounded in-memory queue; a background flush thread (or
/// threads, per `RwConfig::concurrency`) drains it on a timer or when it
/// reaches `max_batch_size`. Port of `remotewrite.Client`
/// (`client.go:52-68`) — see the module doc comment for the queue/thread
/// design.
pub struct RwClient {
    inner: Arc<Inner>,
    handles: Vec<JoinHandle<()>>,
}

impl RwClient {
    /// Builds the client and spawns `cfg.concurrency.max(1)` background
    /// flush threads. Port of `NewClient` (`client.go:94-135`), minus the
    /// upstream default-from-zero fallbacks for `MaxBatchSize`/
    /// `MaxQueueSize`/`FlushInterval`/`Concurrency` — this port's caller is
    /// expected to supply real values (`RwConfig` has no zero-means-default
    /// convention), except `concurrency`, which is clamped to at least 1 so
    /// a caller-supplied `0` still spawns a worker instead of leaving the
    /// queue undrained.
    pub fn start(cfg: RwConfig) -> Result<Self, RwError> {
        let url = build_url(&cfg.url)?;
        let client = build_client(&cfg.tls, cfg.send_timeout)?;
        let inner = Arc::new(Inner {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            max_queue_size: cfg.max_queue_size.max(1),
            max_batch_size: cfg.max_batch_size.max(1),
            flush_interval: cfg.flush_interval,
            stopping: AtomicBool::new(false),
            client,
            url,
            headers: cfg.headers,
            auth: cfg.auth,
        });

        let worker_count = cfg.concurrency.max(1);
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let worker_inner = Arc::clone(&inner);
            handles.push(thread::spawn(move || run(worker_inner)));
        }

        Ok(RwClient { inner, handles })
    }

    /// Appends `series` to the queue. Never blocks: if the queue is already
    /// at `max_queue_size`, the series is dropped and a warning is logged.
    pub fn push(&self, series: Series) {
        self.inner.push(series);
    }

    /// Synchronously drains the current queue, POSTing it in
    /// `max_batch_size` chunks, from the calling thread — it does not wait
    /// for a background worker to wake up. Always returns `Ok(())`: encode
    /// and POST failures are logged and the affected batch is dropped
    /// rather than surfaced here (see the module doc comment).
    ///
    /// Not called by `main`'s wiring (Task 19): shutdown relies on
    /// [`RwClient::shutdown`], whose worker threads perform their own final
    /// drain.
    #[allow(dead_code)]
    pub fn flush_now(&self) -> Result<(), RwError> {
        self.inner.drain_and_flush();
        Ok(())
    }

    /// Signals every background worker to stop, wakes them, and joins all
    /// of them. Each worker performs one final drain (catching anything
    /// pushed just before shutdown) before exiting, so no queued series is
    /// silently lost on a clean shutdown.
    pub fn shutdown(mut self) {
        self.inner.stopping.store(true, Ordering::SeqCst);
        self.inner.cv.notify_all();
        // `self` implements `Drop`, so its fields can't be moved out of by
        // value; `mem::take` swaps in an empty `Vec` instead, leaving `self`
        // in a valid (harmless-to-drop) state for when this function ends.
        for handle in std::mem::take(&mut self.handles) {
            let _ = handle.join();
        }
    }
}

impl Drop for RwClient {
    /// Safety net for a dropped-without-`shutdown` client: wakes any
    /// background worker so it observes `stopping` and exits soon instead of
    /// waiting out the rest of its `flush_interval`. Deliberately does not
    /// join — joining in `Drop` risks blocking (or deadlocking, if dropped
    /// from within a worker's own call stack) the dropping thread; explicit
    /// [`RwClient::shutdown`] remains the only join point.
    fn drop(&mut self) {
        self.inner.stopping.store(true, Ordering::SeqCst);
        self.inner.cv.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::super::{RwClient, RwConfig};
    use super::DEFAULT_SEND_TIMEOUT;
    use crate::datasource::{AuthConfig, TlsConfig};
    use crate::series::Series;
    use esm_http::{Request, ResponseWriter, Server};
    use esm_protoparser::prompb::Sample;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    fn start_capture_server() -> (Server, Arc<Mutex<Option<Vec<u8>>>>) {
        let server = Server::bind("127.0.0.1:0").expect("bind stub server");
        let captured: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
        let captured_for_handler = Arc::clone(&captured);
        server.serve(Arc::new(
            move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
                assert_eq!(req.path(), "/api/v1/write");
                let mut body_bytes = Vec::new();
                req.read_body_to(&mut body_bytes, 1 << 20).ok();
                *captured_for_handler.lock().unwrap() = Some(body_bytes);
                w.write_json(204, "{}");
            },
        ));
        (server, captured)
    }

    fn wait_for_capture(
        captured: &Arc<Mutex<Option<Vec<u8>>>>,
        timeout: Duration,
    ) -> Option<Vec<u8>> {
        let start = Instant::now();
        loop {
            if let Some(body) = captured.lock().unwrap().clone() {
                return Some(body);
            }
            if start.elapsed() > timeout {
                return None;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn flushes_pushed_series_to_remote_write() {
        let (server, captured) = start_capture_server();
        let addr = server.local_addr();
        let client = RwClient::start(RwConfig {
            url: format!("http://{addr}"),
            flush_interval: Duration::from_millis(50),
            max_batch_size: 10,
            max_queue_size: 1000,
            concurrency: 1,
            send_timeout: DEFAULT_SEND_TIMEOUT,
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
            headers: vec![],
        })
        .expect("start remote-write client");

        client.push(Series {
            labels: vec![("__name__".to_string(), "ALERTS".to_string())],
            samples: vec![Sample {
                value: 1.0,
                timestamp: 1,
            }],
        });
        client.flush_now().expect("flush_now failed");

        let body = wait_for_capture(&captured, Duration::from_secs(2)).expect("received write");
        let decompressed = snap::raw::Decoder::new()
            .decompress_vec(&body)
            .expect("snappy decompress");
        let wr = esm_protoparser::prompb::unmarshal_write_request(&decompressed).expect("decode");
        assert_eq!(wr.timeseries.len(), 1);
        assert_eq!(wr.timeseries[0].labels[0].value, b"ALERTS");
        assert_eq!(wr.timeseries[0].samples[0].value, 1.0);

        client.shutdown();
        server.stop();
    }

    #[test]
    fn push_drops_series_when_queue_is_full() {
        let (server, captured) = start_capture_server();
        let addr = server.local_addr();
        // A huge flush_interval and concurrency of 1 means the background
        // thread won't drain the queue on its own during this test; only a
        // full queue plus one more push is under test here.
        let client = RwClient::start(RwConfig {
            url: format!("http://{addr}"),
            flush_interval: Duration::from_secs(3600),
            max_batch_size: 100,
            max_queue_size: 2,
            concurrency: 1,
            send_timeout: DEFAULT_SEND_TIMEOUT,
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
            headers: vec![],
        })
        .expect("start remote-write client");

        let make_series = |name: &str| Series {
            labels: vec![("__name__".to_string(), name.to_string())],
            samples: vec![Sample {
                value: 1.0,
                timestamp: 1,
            }],
        };

        client.push(make_series("a"));
        client.push(make_series("b"));
        // Queue is now at max_queue_size (2); this push must be dropped, not
        // block and not panic.
        client.push(make_series("c"));

        client.flush_now().expect("flush_now failed");
        let body = wait_for_capture(&captured, Duration::from_secs(2)).expect("received write");
        let decompressed = snap::raw::Decoder::new()
            .decompress_vec(&body)
            .expect("snappy decompress");
        let wr = esm_protoparser::prompb::unmarshal_write_request(&decompressed).expect("decode");
        assert_eq!(
            wr.timeseries.len(),
            2,
            "the third push should have been dropped, not queued"
        );

        client.shutdown();
        server.stop();
    }

    #[test]
    fn flush_and_shutdown_do_not_hang_when_endpoint_never_responds() {
        // A bound-but-never-accepted TCP listener: the kernel completes the
        // handshake into the accept backlog, the client's request bytes are
        // buffered, and no response ever comes — the pathological case a send
        // timeout must guard against. Without `.timeout()` on the client,
        // `req.send()` would block a worker forever and `shutdown()`'s join
        // would deadlock. The listener is kept alive (bound to `_listener`)
        // for the whole test and torn down on drop at the end.
        use std::net::TcpListener;
        let _listener = TcpListener::bind("127.0.0.1:0").expect("bind stalled listener");
        let addr = _listener.local_addr().expect("local addr");

        let short_timeout = Duration::from_millis(300);
        let client = RwClient::start(RwConfig {
            url: format!("http://{addr}"),
            flush_interval: Duration::from_secs(3600),
            max_batch_size: 10,
            max_queue_size: 100,
            concurrency: 1,
            send_timeout: short_timeout,
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
            headers: vec![],
        })
        .expect("start remote-write client");

        client.push(Series {
            labels: vec![("__name__".to_string(), "ALERTS".to_string())],
            samples: vec![Sample {
                value: 1.0,
                timestamp: 1,
            }],
        });

        // flush_now drives send_batch on this thread; with the timeout it
        // returns after ~short_timeout instead of hanging. A generous 5s cap
        // still catches a genuine unbounded hang.
        let start = Instant::now();
        client.flush_now().expect("flush_now failed");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "flush_now hung despite send timeout: took {:?}",
            start.elapsed()
        );

        // shutdown must also return promptly (workers aren't blocked in send).
        let shutdown_start = Instant::now();
        client.shutdown();
        assert!(
            shutdown_start.elapsed() < Duration::from_secs(5),
            "shutdown hung: took {:?}",
            shutdown_start.elapsed()
        );
    }
}
