//! Per-destination remote-write HTTP client: a worker pool that drains a
//! [`crate::queue::PersistentQueue`] and POSTs blocks with retry/backoff.
//! Port of `app/vmagent/remotewrite/client.go`'s `runWorker`/`sendBlockHTTP`,
//! reshaped to reuse `crates/esmalert/src/remotewrite/client.rs`'s
//! send-timeout + snappy POST + graceful shutdown pattern (the closest
//! existing analog in this codebase).
//!
//! ## Retry design
//!
//! Each worker thread loops: [`PersistentQueue::pop`] (bounded by
//! [`POP_TIMEOUT`], so the stop flag is re-checked at least that often even
//! when the queue is idle) hands the worker one block, which it then owns
//! until it is either delivered or dropped — it is never pushed back onto
//! the queue. [`send_once`] classifies the result of one POST attempt:
//!
//! - success (2xx, including 204) -> the block is done; the worker pops the
//!   next one.
//! - unrecoverable (status 400, 409, or 415 — matches upstream
//!   `sendBlockHTTP`'s drop set) -> the block can never succeed against this
//!   endpoint; it is logged and dropped.
//! - retryable (every other non-2xx status — including 401, 403, 404, 429,
//!   and 5xx — or a transport-level error/timeout) -> the worker backs off
//!   ([`retry_min`](ClientConfig::retry_min) doubling to
//!   [`retry_max`](ClientConfig::retry_max)) and retries the *same* block.
//!
//! The backoff wait ([`wait_or_stop`]) sleeps in small polling increments
//! rather than one long `thread::sleep`, so a [`Client::stop`] request is
//! observed within one increment instead of at the end of a (potentially
//! multi-second, at `retry_max`) backoff — this is what keeps `stop()`
//! responsive even while a worker is mid-retry against a slow or dead
//! endpoint. If `stop` lands mid-backoff, the in-flight block is pushed back
//! onto the queue (mirrors upstream `runWorker`'s `MustWriteBlockIgnoreDisabledPQ`
//! on its stop path) rather than dropped, so it survives the shutdown and is
//! retried after a restart — the durability contract is "delivered, dropped
//! as unrecoverable, or re-queued," never silently lost.
//!
//! A per-request `send_timeout` (applied to the whole `reqwest::blocking`
//! client, mirroring `RwConfig::send_timeout`) is required so a stalled
//! endpoint can't block a worker inside `req.send()` forever — without it,
//! [`Client::stop`]'s `join()` would hang. See
//! `shutdown_does_not_hang_against_dead_endpoint` below.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use reqwest::blocking::Client as HttpClient;
use url::Url;

use crate::queue::PersistentQueue;

/// Resolved credentials to attach to every remote-write request. At most one
/// of `basic`/`bearer` is expected to be set; if both are somehow set,
/// [`send_once`] prefers `basic`. Local copy of
/// `esmalert::datasource::auth::AuthConfig` — duplicated rather than shared
/// across crates (see the module doc's cross-crate note in the task report).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AuthConfig {
    pub basic: Option<(String, String)>,
    pub bearer: Option<String>,
}

/// TLS options applied to the worker pool's shared `reqwest` client. Local
/// copy of `esmalert::datasource::auth::TlsConfig`.
///
/// Derives `Deserialize` (field names already match the upstream
/// `tls_config:` YAML shape 1:1 — see
/// `lib/promauth/config.go`'s `TLSConfig`) so `scrape::config` can
/// deserialize a scrape config's per-job `tls_config:` section straight into
/// this type instead of duplicating it.
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    pub ca_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub server_name: Option<String>,
    pub insecure_skip_verify: bool,
}

/// Error returned by [`Client::start`]. Never constructed from a panic;
/// never carries auth credentials (basic/bearer secrets are never formatted
/// into it) — mirrors `esmalert::remotewrite::RwError`'s shape.
#[derive(Debug)]
pub struct ClientError {
    msg: String,
}

impl ClientError {
    fn new(msg: impl Into<String>) -> Self {
        ClientError { msg: msg.into() }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for ClientError {}

/// Config for [`Client::start`].
pub struct ClientConfig {
    pub url: String,
    /// Number of worker threads draining the queue for this destination.
    /// Clamped to at least 1 (a caller-supplied `0` still spawns a worker
    /// instead of leaving the queue undrained), mirroring
    /// `RwConfig::concurrency`'s convention.
    pub queues: usize,
    /// Initial retry backoff for a retryable failure (5xx/429/transport
    /// error).
    pub retry_min: Duration,
    /// Backoff cap: `retry_min` doubles on each consecutive retryable
    /// failure until it reaches this ceiling.
    pub retry_max: Duration,
    /// Per-request timeout applied to the shared `reqwest::blocking::Client`.
    /// Required for shutdown liveness — see the module doc.
    pub send_timeout: Duration,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
}

/// How long a worker's [`PersistentQueue::pop`] blocks waiting for a block
/// before re-checking the stop flag. Bounds the worst-case latency of
/// [`Client::stop`] when a worker is idle (queue empty) rather than mid-send
/// or mid-backoff.
const POP_TIMEOUT: Duration = Duration::from_secs(1);

/// How often [`wait_or_stop`] re-checks the stop flag while backing off.
/// Small relative to typical `retry_min`/`retry_max` values so `stop()`
/// stays responsive without busy-spinning.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Floor for the retry backoff, so a zero (or misconfigured) `retry_min`/
/// `retry_max` can't collapse the retry loop into a zero-delay busy loop of
/// POSTs against a persistently retryable endpoint.
const MIN_BACKOFF: Duration = Duration::from_millis(1);

/// Remote-write client for one destination: a pool of worker threads that
/// drain a shared [`PersistentQueue`], each holding and retrying its own
/// in-flight block. See the module doc for the retry/backoff design.
pub struct Client {
    stop: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
}

impl Client {
    /// Builds the shared `reqwest::blocking::Client` and spawns
    /// `cfg.queues.max(1)` worker threads, each running [`worker_loop`]
    /// against `queue`.
    pub fn start(cfg: ClientConfig, queue: Arc<PersistentQueue>) -> Result<Client, ClientError> {
        let url = Url::parse(&cfg.url).map_err(|e| {
            ClientError::new(format!("invalid remote-write url {:?}: {e}", cfg.url))
        })?;
        let http = build_client(&cfg.tls, cfg.send_timeout)?;
        let stop = Arc::new(AtomicBool::new(false));

        let worker_count = cfg.queues.max(1);
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let worker_stop = Arc::clone(&stop);
            let worker_queue = Arc::clone(&queue);
            let worker_http = http.clone();
            let worker_url = url.clone();
            let worker_auth = cfg.auth.clone();
            let retry_min = cfg.retry_min;
            let retry_max = cfg.retry_max;
            handles.push(thread::spawn(move || {
                let ctx = WorkerCtx {
                    stop: &worker_stop,
                    queue: &worker_queue,
                    http: &worker_http,
                    url: &worker_url,
                    auth: &worker_auth,
                    retry_min,
                    retry_max,
                };
                worker_loop(&ctx);
            }));
        }

        Ok(Client { stop, handles })
    }

    /// Signals every worker to stop and joins all of them. A worker that is
    /// idle (blocked in `pop`) exits within [`POP_TIMEOUT`]; a worker that
    /// is mid-retry-backoff exits within [`STOP_POLL_INTERVAL`], re-queuing
    /// its in-flight block onto the durable queue first; a worker that is
    /// inside `req.send()` exits once that call returns, bounded by
    /// `send_timeout`.
    pub fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        for handle in self.handles {
            let _ = handle.join();
        }
    }
}

/// Everything one worker thread needs for the lifetime of its loop, bundled
/// so [`worker_loop`]/[`deliver_with_retry`] each take one reference instead
/// of a long, easy-to-transpose parameter list.
struct WorkerCtx<'a> {
    stop: &'a AtomicBool,
    queue: &'a PersistentQueue,
    http: &'a HttpClient,
    url: &'a Url,
    auth: &'a AuthConfig,
    retry_min: Duration,
    retry_max: Duration,
}

/// One worker's loop: pop a block, send it to completion (success or drop),
/// repeat, until `stop` is observed. Never panics — a transport error is
/// classified as retryable by [`send_once`] and logged there.
fn worker_loop(ctx: &WorkerCtx<'_>) {
    while !ctx.stop.load(Ordering::SeqCst) {
        let Some(block) = ctx.queue.pop(POP_TIMEOUT) else {
            continue;
        };
        deliver_with_retry(ctx, block);
    }
}

/// Drives one popped block to completion: retries a retryable failure with
/// exponential backoff (starting at `ctx.retry_min`, doubling to
/// `ctx.retry_max`) until it succeeds or `ctx.stop` is observed. Drops the
/// block immediately on an unrecoverable status (400/409/415); if `stop`
/// cuts a backoff wait short instead, the block is re-pushed onto
/// `ctx.queue` rather than dropped — see the module doc.
fn deliver_with_retry(ctx: &WorkerCtx<'_>, block: Vec<u8>) {
    // Clamp against misconfiguration: never below ~1ms (a zero delay would
    // busy-loop POSTs on a persistently retryable endpoint) and never above
    // retry_max (so retry_min > retry_max can't start above the cap). The
    // cap itself is floored at 1ms so retry_max == 0 doesn't collapse the
    // floor back to a zero-delay loop.
    let cap = ctx.retry_max.max(MIN_BACKOFF);
    let mut backoff = ctx.retry_min.clamp(MIN_BACKOFF, cap);
    let url = ctx.url;
    loop {
        match send_once(ctx.http, ctx.url, ctx.auth, &block) {
            SendOutcome::Success => return,
            SendOutcome::Drop(reason) => {
                log::warn!(
                    "esmagent remote-write: dropping block ({} bytes) posted to {url}: {reason}",
                    block.len()
                );
                return;
            }
            SendOutcome::Retryable(reason) => {
                log::warn!(
                    "esmagent remote-write: retryable failure posting {} bytes to {url}: {reason}; retrying in {backoff:?}",
                    block.len()
                );
                if wait_or_stop(ctx.stop, backoff) {
                    log::warn!(
                        "esmagent remote-write: shutdown requested mid-retry; re-queuing in-flight block ({} bytes) to {url} for delivery after restart",
                        block.len()
                    );
                    if let Err(e) = ctx.queue.push(block) {
                        log::warn!(
                            "esmagent remote-write: failed to re-queue in-flight block on shutdown to {url}: {e}"
                        );
                    }
                    return;
                }
                backoff = (backoff * 2).clamp(MIN_BACKOFF, cap);
            }
        }
    }
}

/// Outcome of one [`send_once`] attempt.
enum SendOutcome {
    /// 2xx (including 204): the block is delivered.
    Success,
    /// Any non-2xx status other than 400/409/415, or a transport-level
    /// error/timeout: worth retrying the same block. Matches upstream
    /// `sendBlockHTTP`'s fallthrough ("unexpected status code") path —
    /// includes 401, 403, 404, 408, 413, 429, and every 5xx.
    Retryable(String),
    /// Status 400, 409, or 415: this block will never succeed against this
    /// endpoint (matches upstream `sendBlockHTTP`'s drop set).
    Drop(String),
}

/// HTTP statuses that upstream `sendBlockHTTP` treats as unrecoverable for a
/// given block and drops rather than retries: 400 (bad request / unsupported
/// encoding), 409 (conflict, e.g. out-of-order/duplicate samples — dropped
/// like Prometheus does), and 415 (unsupported media type). Every other
/// non-2xx status is retried indefinitely.
fn is_unrecoverable_status(code: u16) -> bool {
    matches!(code, 400 | 409 | 415)
}

/// POSTs `block` to `url` once, applying `auth` and the remote-write
/// headers. Never panics: a transport error (including the `send_timeout`
/// firing) is reported as [`SendOutcome::Retryable`], not propagated.
fn send_once(http: &HttpClient, url: &Url, auth: &AuthConfig, block: &[u8]) -> SendOutcome {
    let mut req = http
        .post(url.clone())
        .header("Content-Encoding", "snappy")
        .header("Content-Type", "application/x-protobuf")
        .header("X-Prometheus-Remote-Write-Version", "0.1.0")
        .body(block.to_vec());
    if let Some((user, pass)) = &auth.basic {
        req = req.basic_auth(user, Some(pass));
    } else if let Some(token) = &auth.bearer {
        req = req.bearer_auth(token);
    }

    match req.send() {
        Ok(resp) => {
            let status = resp.status();
            if status.is_success() {
                SendOutcome::Success
            } else if is_unrecoverable_status(status.as_u16()) {
                SendOutcome::Drop(format!("status {status}"))
            } else {
                SendOutcome::Retryable(format!("status {status}"))
            }
        }
        Err(e) => SendOutcome::Retryable(e.to_string()),
    }
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`] instead of
/// in one long sleep, so a [`Client::stop`] request is observed within one
/// poll tick rather than at the end of the full backoff. Returns `true` if
/// `stop` was observed before `dur` elapsed (the caller should treat this as
/// "stop now," not "backoff finished normally").
fn wait_or_stop(stop: &AtomicBool, dur: Duration) -> bool {
    let mut remaining = dur;
    while remaining > Duration::ZERO {
        if stop.load(Ordering::SeqCst) {
            return true;
        }
        let step = remaining.min(STOP_POLL_INTERVAL);
        thread::sleep(step);
        remaining = remaining.saturating_sub(step);
    }
    stop.load(Ordering::SeqCst)
}

/// Builds the shared `reqwest::blocking::Client`, applying `tls` the same
/// way `esmalert::remotewrite::client::build_client` does (duplicated
/// rather than shared, per this repo's established convention — see that
/// function's doc comment) plus `send_timeout` as the client-wide request
/// timeout.
fn build_client(tls: &TlsConfig, send_timeout: Duration) -> Result<HttpClient, ClientError> {
    let mut builder = HttpClient::builder().timeout(send_timeout);
    if tls.insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(ca_file) = &tls.ca_file {
        let pem = std::fs::read(ca_file)
            .map_err(|e| ClientError::new(format!("cannot read CA file {ca_file:?}: {e}")))?;
        let cert = reqwest::Certificate::from_pem(&pem)
            .map_err(|e| ClientError::new(format!("invalid CA certificate in {ca_file:?}: {e}")))?;
        builder = builder.add_root_certificate(cert);
    }
    if let (Some(cert_file), Some(key_file)) = (&tls.cert_file, &tls.key_file) {
        let mut identity_pem = std::fs::read(cert_file)
            .map_err(|e| ClientError::new(format!("cannot read cert file {cert_file:?}: {e}")))?;
        let mut key_pem = std::fs::read(key_file)
            .map_err(|e| ClientError::new(format!("cannot read key file {key_file:?}: {e}")))?;
        identity_pem.push(b'\n');
        identity_pem.append(&mut key_pem);
        let identity = reqwest::Identity::from_pem(&identity_pem)
            .map_err(|e| ClientError::new(format!("invalid client cert/key: {e}")))?;
        builder = builder.identity(identity);
    }
    // `tls.server_name` (SNI/hostname override independent of the request
    // URL's host) has no direct equivalent in reqwest's blocking
    // `ClientBuilder`; not wired here — same gap as
    // `esmalert::datasource::client::build_client`.
    builder
        .build()
        .map_err(|e| ClientError::new(format!("cannot build http client: {e}")))
}

#[cfg(test)]
mod tests {
    use super::{AuthConfig, Client, ClientConfig, TlsConfig};
    use crate::queue::PersistentQueue;
    use esm_http::{Request, ResponseWriter, Server};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// Stub remote-write endpoint with three paths:
    /// - `/ok`: returns 503 (retryable) for the first `fail_before_ok`
    ///   requests, then 204.
    /// - `/bad`: always returns 400 (unrecoverable — dropped, never
    ///   retried).
    /// - `/notfound`: returns 404 for the first `fail_before_ok` requests,
    ///   then 204 — 404 is *not* in the drop set (400/409/415), so this
    ///   proves it is retried like a 5xx rather than dropped like `/bad`.
    ///
    /// Counts requests per path so tests can assert exactly how many
    /// attempts were made.
    struct Stub {
        server: Server,
        ok_attempts: Arc<AtomicUsize>,
        bad_attempts: Arc<AtomicUsize>,
        notfound_attempts: Arc<AtomicUsize>,
    }

    fn start_stub(fail_before_ok: usize) -> Stub {
        let server = Server::bind("127.0.0.1:0").expect("bind stub server");
        let ok_attempts = Arc::new(AtomicUsize::new(0));
        let bad_attempts = Arc::new(AtomicUsize::new(0));
        let notfound_attempts = Arc::new(AtomicUsize::new(0));
        let ok_for_handler = Arc::clone(&ok_attempts);
        let bad_for_handler = Arc::clone(&bad_attempts);
        let notfound_for_handler = Arc::clone(&notfound_attempts);
        server.serve(Arc::new(
            move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
                let mut body_bytes = Vec::new();
                req.read_body_to(&mut body_bytes, 1 << 20).ok();
                match req.path() {
                    "/ok" => {
                        let n = ok_for_handler.fetch_add(1, Ordering::SeqCst);
                        if n < fail_before_ok {
                            w.write_status(503);
                        } else {
                            w.write_status(204);
                        }
                    }
                    "/bad" => {
                        bad_for_handler.fetch_add(1, Ordering::SeqCst);
                        w.write_status(400);
                    }
                    "/notfound" => {
                        let n = notfound_for_handler.fetch_add(1, Ordering::SeqCst);
                        if n < fail_before_ok {
                            w.write_status(404);
                        } else {
                            w.write_status(204);
                        }
                    }
                    _ => w.write_status(404),
                }
            },
        ));
        Stub {
            server,
            ok_attempts,
            bad_attempts,
            notfound_attempts,
        }
    }

    /// Polls `check` until it returns `true` or `timeout` elapses, sleeping
    /// briefly between polls. Returns whether `check` was ever satisfied.
    fn wait_until(mut check: impl FnMut() -> bool, timeout: Duration) -> bool {
        let start = Instant::now();
        loop {
            if check() {
                return true;
            }
            if start.elapsed() > timeout {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn base_config(url: String) -> ClientConfig {
        ClientConfig {
            url,
            queues: 1,
            retry_min: Duration::from_millis(10),
            retry_max: Duration::from_millis(50),
            send_timeout: Duration::from_secs(2),
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
        }
    }

    #[test]
    fn retries_5xx_then_succeeds_and_drops_4xx() {
        let stub = start_stub(2); // first 2 attempts on /ok get 503, 3rd gets 204
        let addr = stub.server.local_addr();

        // --- retryable path: eventually delivered ---
        let ok_dir = tempfile::tempdir().unwrap();
        let ok_queue = Arc::new(PersistentQueue::open(ok_dir.path(), 10_000_000).unwrap());
        let ok_client = Client::start(
            base_config(format!("http://{addr}/ok")),
            Arc::clone(&ok_queue),
        )
        .expect("start ok client");

        ok_queue.push(b"ok-block".to_vec()).unwrap();

        let delivered = wait_until(
            || stub.ok_attempts.load(Ordering::SeqCst) >= 3 && ok_queue.pending_bytes() == 0,
            Duration::from_secs(5),
        );
        assert!(
            delivered,
            "block was not delivered after retries (attempts={})",
            stub.ok_attempts.load(Ordering::SeqCst)
        );
        assert!(
            stub.ok_attempts.load(Ordering::SeqCst) >= 3,
            "expected at least 3 attempts (2 failures + 1 success), got {}",
            stub.ok_attempts.load(Ordering::SeqCst)
        );

        ok_client.stop();

        // --- non-retryable path: dropped after exactly one attempt ---
        let bad_dir = tempfile::tempdir().unwrap();
        let bad_queue = Arc::new(PersistentQueue::open(bad_dir.path(), 10_000_000).unwrap());
        let bad_client = Client::start(
            base_config(format!("http://{addr}/bad")),
            Arc::clone(&bad_queue),
        )
        .expect("start bad client");

        bad_queue.push(b"bad-block".to_vec()).unwrap();

        let drained = wait_until(|| bad_queue.pending_bytes() == 0, Duration::from_secs(5));
        assert!(drained, "block was never popped from the bad-path queue");

        // Give any (incorrect) retry loop a chance to fire before asserting
        // it never did.
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            bad_queue.pending_bytes(),
            0,
            "queue should remain empty (block dropped, not re-queued)"
        );
        assert_eq!(
            stub.bad_attempts.load(Ordering::SeqCst),
            1,
            "a 400 response must not be retried"
        );

        bad_client.stop();
        stub.server.stop();
    }

    #[test]
    fn retries_404_then_succeeds() {
        // 404 is not in the 400/409/415 drop set, so — unlike `/bad` above —
        // it must be retried like a 5xx, not dropped after one attempt.
        let stub = start_stub(2); // first 2 attempts on /notfound get 404, 3rd gets 204
        let addr = stub.server.local_addr();

        let dir = tempfile::tempdir().unwrap();
        let queue = Arc::new(PersistentQueue::open(dir.path(), 10_000_000).unwrap());
        let client = Client::start(
            base_config(format!("http://{addr}/notfound")),
            Arc::clone(&queue),
        )
        .expect("start client");

        queue.push(b"notfound-block".to_vec()).unwrap();

        let delivered = wait_until(
            || stub.notfound_attempts.load(Ordering::SeqCst) >= 3 && queue.pending_bytes() == 0,
            Duration::from_secs(5),
        );
        assert!(
            delivered,
            "block was not delivered after retries (attempts={})",
            stub.notfound_attempts.load(Ordering::SeqCst)
        );
        assert!(
            stub.notfound_attempts.load(Ordering::SeqCst) >= 3,
            "expected a 404 to be retried (2 failures + 1 success), got {} attempts",
            stub.notfound_attempts.load(Ordering::SeqCst)
        );

        client.stop();
        stub.server.stop();
    }

    #[test]
    fn shutdown_requeues_in_flight_block_instead_of_dropping_it() {
        // The endpoint always returns 503 (retryable), so the worker parks
        // in a retry backoff with the popped block still in hand when
        // `stop()` lands. Fix #2: that in-flight block must be pushed back
        // onto the durable queue (not dropped) so it survives a restart.
        let stub = start_stub(usize::MAX); // /ok always returns 503
        let addr = stub.server.local_addr();

        let dir = tempfile::tempdir().unwrap();
        let queue = Arc::new(PersistentQueue::open(dir.path(), 10_000_000).unwrap());
        let mut cfg = base_config(format!("http://{addr}/ok"));
        cfg.retry_min = Duration::from_millis(20);
        cfg.retry_max = Duration::from_millis(50);
        let client = Client::start(cfg, Arc::clone(&queue)).expect("start client");

        queue.push(b"in-flight-block".to_vec()).unwrap();

        // Wait until the worker has popped the block and made at least one
        // failed attempt, so it's parked in a retry backoff (not idle in
        // `pop`) when we call stop().
        let attempted = wait_until(
            || stub.ok_attempts.load(Ordering::SeqCst) >= 1,
            Duration::from_secs(5),
        );
        assert!(attempted, "worker never attempted the in-flight block");

        client.stop();

        // stop() joins the worker, and the worker re-pushes the in-flight
        // block onto the queue before exiting, so it must be poppable here.
        let requeued = queue.pop(Duration::from_millis(100));
        assert_eq!(
            requeued.as_deref(),
            Some(b"in-flight-block".as_slice()),
            "in-flight block was dropped on shutdown instead of being re-queued"
        );

        stub.server.stop();
    }

    #[test]
    fn shutdown_does_not_hang_against_dead_endpoint() {
        // A bound-but-never-accepted TCP listener: connect() succeeds into
        // the kernel's backlog, request bytes get buffered, and no response
        // ever comes back — the case a send timeout must guard against.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind stalled listener");
        let addr = listener.local_addr().expect("local addr");

        let dir = tempfile::tempdir().unwrap();
        let queue = Arc::new(PersistentQueue::open(dir.path(), 10_000_000).unwrap());
        let mut cfg = base_config(format!("http://{addr}"));
        cfg.send_timeout = Duration::from_millis(300);
        cfg.retry_min = Duration::from_millis(20);
        cfg.retry_max = Duration::from_millis(50);
        let client = Client::start(cfg, Arc::clone(&queue)).expect("start client");

        queue.push(b"stuck-block".to_vec()).unwrap();
        // Let the worker pop the block and start (or finish) its first,
        // timeout-bound send attempt.
        std::thread::sleep(Duration::from_millis(100));

        let start = Instant::now();
        client.stop();
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "stop() hung against a dead endpoint: took {:?}",
            start.elapsed()
        );

        drop(listener);
    }

    #[test]
    fn shutdown_interrupts_a_multi_second_retry_backoff() {
        // Unlike `shutdown_does_not_hang_against_dead_endpoint` (where
        // stop()'s latency is dominated by `send_timeout`, so even a naive
        // `thread::sleep(backoff)` would pass), this test isolates the
        // BACKOFF wait: the endpoint responds *fast* with a retryable 503,
        // so the worker spends essentially all its time asleep in the
        // backoff, not blocked in `send`. With retry_max=30s the backoff
        // grows to multiple seconds after a few retries. A correct,
        // poll-based `wait_or_stop` interrupts that sleep within one poll
        // tick; a regression to `thread::sleep(backoff)` would make stop()
        // block for the remaining multi-second backoff and fail the
        // sub-second assertion below.
        let stub = start_stub(usize::MAX); // /ok always returns 503 (never 204)
        let addr = stub.server.local_addr();

        let dir = tempfile::tempdir().unwrap();
        let queue = Arc::new(PersistentQueue::open(dir.path(), 10_000_000).unwrap());
        let mut cfg = base_config(format!("http://{addr}/ok"));
        cfg.retry_min = Duration::from_millis(50);
        cfg.retry_max = Duration::from_secs(30);
        cfg.send_timeout = Duration::from_secs(5); // ample; the stub is fast
        let client = Client::start(cfg, Arc::clone(&queue)).expect("start client");

        queue.push(b"retry-forever-block".to_vec()).unwrap();

        // Deterministically wait until the backoff has grown to multiple
        // seconds. After the Nth failed attempt the worker sleeps
        // 50ms * 2^(N-1); at N=7 that is ~3.2s. Observing >=7 attempts means
        // the worker is now parked in a ~3.2s backoff wait.
        let grew = wait_until(
            || stub.ok_attempts.load(Ordering::SeqCst) >= 7,
            Duration::from_secs(10),
        );
        assert!(
            grew,
            "backoff never grew to the multi-second regime (attempts={})",
            stub.ok_attempts.load(Ordering::SeqCst)
        );

        // stop() must interrupt the in-progress multi-second sleep, not wait
        // it out. Poll-based wait_or_stop returns within ~STOP_POLL_INTERVAL
        // (50ms); a plain thread::sleep(backoff) would take up to ~3.2s.
        let start = Instant::now();
        client.stop();
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "stop() did not interrupt the multi-second backoff (took {:?}); \
             wait_or_stop must poll, not thread::sleep(backoff)",
            start.elapsed()
        );

        stub.server.stop();
    }
}
