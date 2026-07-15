//! `RemoteWriteCtx`: one self-contained per-destination pipeline —
//! per-URL relabel -> [`PendingSeries`] block accumulation ->
//! [`PersistentQueue`] -> [`Client`] worker pool.
//!
//! Port of `app/vmagent/remotewrite/remotewrite.go`'s `newRemoteWriteCtx`
//! (construction) plus the periodic-flush goroutine it starts
//! (`rwctx.flushLoop` upstream): a background thread flushes whatever is
//! buffered in `pending` every `flush_interval`, so series don't wait
//! indefinitely for a block to fill up before being queued for delivery.

use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use esm_relabel::ParsedConfigs;
use esm_streamaggr::{Aggregators, Options, PushFunc, TimeSeries};

use crate::client::{Client, ClientConfig, ClientError};
use crate::pendingseries::PendingSeries;
use crate::queue::{PersistentQueue, QueueError};
use crate::series::OwnedSeries;
use crate::streamagg;

/// How often [`flush_loop`] re-checks the stop flag while waiting out
/// `flush_interval`. Small relative to typical flush intervals so
/// [`RemoteWriteCtx::stop`] stays responsive without busy-spinning — mirrors
/// `client::STOP_POLL_INTERVAL`'s role for the retry backoff wait.
const FLUSH_STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Floor for `flush_interval`, mirroring `client::MIN_BACKOFF`'s role: a
/// zero (or otherwise too-small) configured interval would otherwise
/// collapse [`flush_loop`] into a zero-delay busy loop of `pending.lock()` +
/// `queue.push` calls, pegging a CPU core for no benefit.
const MIN_FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// Config for [`RemoteWriteCtx::start`].
pub struct RwCtxConfig {
    pub client: ClientConfig,
    /// Per-URL relabel config, applied to a per-destination copy of each
    /// pushed series before it is buffered. `None` means this destination
    /// gets every series the global relabel stage (in [`crate::sink`])
    /// already let through, unmodified.
    pub url_relabel: Option<ParsedConfigs>,
    pub queue_dir: PathBuf,
    pub max_disk_bytes: u64,
    pub max_block_size: usize,
    /// How often the background flush thread flushes whatever is buffered
    /// in [`PendingSeries`], even if it hasn't reached `max_block_size`.
    /// Floored at [`MIN_FLUSH_INTERVAL`] by [`RemoteWriteCtx::start`].
    pub flush_interval: Duration,
    /// Per-URL `-remoteWrite.streamAggr.config` YAML (aggregation applied to
    /// this destination's series *after* `url_relabel`, so grouping keys and
    /// output labels are computed from post-relabel labels, matching Go's
    /// `remoteWriteCtx.TryPushTimeSeries`). `None` disables it.
    pub stream_aggr_config: Option<String>,
    /// Per-URL `-remoteWrite.streamAggr.keepInput`.
    pub stream_aggr_keep_input: bool,
    /// Per-URL `-remoteWrite.streamAggr.dedupInterval` in milliseconds.
    pub stream_aggr_dedup_interval_ms: i64,
}

/// Error returned by [`RemoteWriteCtx::start`].
#[derive(Debug)]
pub enum RwCtxError {
    Queue(QueueError),
    Client(ClientError),
    StreamAggr(String),
}

impl fmt::Display for RwCtxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RwCtxError::Queue(e) => write!(f, "rwctx: queue open failed: {e}"),
            RwCtxError::Client(e) => write!(f, "rwctx: client start failed: {e}"),
            RwCtxError::StreamAggr(e) => write!(f, "rwctx: stream aggregation config failed: {e}"),
        }
    }
}

impl std::error::Error for RwCtxError {}

impl From<QueueError> for RwCtxError {
    fn from(e: QueueError) -> Self {
        RwCtxError::Queue(e)
    }
}

impl From<ClientError> for RwCtxError {
    fn from(e: ClientError) -> Self {
        RwCtxError::Client(e)
    }
}

/// One destination's full forwarding pipeline: relabel -> buffer -> durable
/// queue -> HTTP delivery. Built by [`RemoteWriteCtx::start`], fed via
/// [`RemoteWriteCtx::push`], torn down via [`RemoteWriteCtx::stop`].
pub struct RemoteWriteCtx {
    queue: Arc<PersistentQueue>,
    client: Client,
    pending: Arc<Mutex<PendingSeries>>,
    /// Per-URL relabel, applied to every pushed series before it is buffered
    /// (and, when stream aggregation is enabled, before aggregation — see
    /// [`RemoteWriteCtx::push`]). Stream-aggregation *output* is not relabeled
    /// again, matching Go (its aggregated series bypass the per-URL relabel).
    url_relabel: Option<ParsedConfigs>,
    /// Per-URL stream aggregation (`None` if `-remoteWrite.streamAggr.config`
    /// is unset for this destination).
    stream_agg: Option<Arc<Aggregators>>,
    stream_agg_keep_input: bool,
    flush_stop: Arc<AtomicBool>,
    flush_handle: JoinHandle<()>,
}

impl RemoteWriteCtx {
    /// Opens the durable queue, starts the HTTP worker pool draining it, and
    /// spawns the background thread that flushes [`PendingSeries`] every
    /// `cfg.flush_interval`.
    pub fn start(cfg: RwCtxConfig) -> Result<RemoteWriteCtx, RwCtxError> {
        let queue = Arc::new(PersistentQueue::open(&cfg.queue_dir, cfg.max_disk_bytes)?);
        let client = Client::start(cfg.client, Arc::clone(&queue))?;
        let pending = Arc::new(Mutex::new(PendingSeries::new(cfg.max_block_size)));
        let url_relabel = cfg.url_relabel;

        // Per-URL stream aggregation: its aggregated output re-enters this
        // destination's buffer → queue path *without* per-URL relabel (the
        // input was already relabeled before aggregation), matching Go.
        let stream_agg = match &cfg.stream_aggr_config {
            None => None,
            Some(path) => {
                let yaml = std::fs::read_to_string(path)
                    .map_err(|e| RwCtxError::StreamAggr(format!("cannot read {path:?}: {e}")))?;
                let pending_cb = Arc::clone(&pending);
                let queue_cb = Arc::clone(&queue);
                let push_func: PushFunc = Arc::new(move |tss: &[TimeSeries]| {
                    let owned = streamagg::from_agg(tss);
                    buffer_and_enqueue(&pending_cb, &queue_cb, &owned);
                });
                let opts = Options {
                    dedup_interval_ms: cfg.stream_aggr_dedup_interval_ms,
                    keep_input: cfg.stream_aggr_keep_input,
                    ..Options::default()
                };
                let aggs = Aggregators::load_from_data(&yaml, push_func, &opts)
                    .map_err(|e| RwCtxError::StreamAggr(format!("invalid {path:?}: {}", e.msg)))?;
                Some(Arc::new(aggs))
            }
        };

        let flush_stop = Arc::new(AtomicBool::new(false));
        let flush_handle = {
            let stop = Arc::clone(&flush_stop);
            let pending = Arc::clone(&pending);
            let queue = Arc::clone(&queue);
            let flush_interval = cfg.flush_interval.max(MIN_FLUSH_INTERVAL);
            thread::spawn(move || flush_loop(&stop, &pending, &queue, flush_interval))
        };

        Ok(RemoteWriteCtx {
            queue,
            client,
            pending,
            url_relabel,
            stream_agg,
            stream_agg_keep_input: cfg.stream_aggr_keep_input,
            flush_stop,
            flush_handle,
        })
    }

    /// Applies this destination's per-URL relabel, then (if configured) feeds
    /// the relabeled survivors through stream aggregation and buffers the
    /// pass-through survivors, enqueuing full blocks onto the durable queue.
    ///
    /// Per-URL relabel runs *before* aggregation (matching Go's
    /// `remoteWriteCtx.TryPushTimeSeries`), so the aggregator's grouping keys
    /// and output labels are derived from post-relabel labels. Input consumed
    /// by an aggregator is dropped from the pass-through path unless
    /// `keepInput` is set; aggregated output is enqueued via the aggregator
    /// callback and is not relabeled again.
    pub fn push(&self, series: &[OwnedSeries]) {
        match &self.stream_agg {
            None => enqueue(&self.url_relabel, &self.pending, &self.queue, series),
            Some(aggs) => {
                let relabeled = apply_url_relabel(&self.url_relabel, series);
                if relabeled.is_empty() {
                    return;
                }
                let tss = streamagg::to_agg(&relabeled);
                let mut match_idxs = Vec::new();
                aggs.push(&tss, &mut match_idxs);
                if self.stream_agg_keep_input {
                    buffer_and_enqueue(&self.pending, &self.queue, &relabeled);
                } else {
                    let passthrough: Vec<OwnedSeries> = relabeled
                        .iter()
                        .zip(match_idxs.iter())
                        .filter(|(_, &m)| m == 0)
                        .map(|(s, _)| s.clone())
                        .collect();
                    if !passthrough.is_empty() {
                        buffer_and_enqueue(&self.pending, &self.queue, &passthrough);
                    }
                }
            }
        }
    }

    /// Tears down the pipeline in the order that keeps buffered data from
    /// being lost: stop (and join) the flush thread first so no more
    /// concurrent flushes race the final one below; flush whatever remains
    /// buffered onto the durable queue; then stop the client (drains/joins
    /// its worker pool, delivering as much of the queue as it can); finally
    /// close the queue.
    pub fn stop(self) {
        self.flush_stop.store(true, Ordering::SeqCst);
        let _ = self.flush_handle.join();

        // Stop per-URL stream aggregation next: its final flush enqueues the
        // last aggregated output into `pending`/`queue` (still open), and
        // dropping the last `Aggregators` handle releases the callback's
        // `queue`/`pending` references so the `try_unwrap` below can reclaim
        // the queue.
        if let Some(aggs) = self.stream_agg {
            aggs.must_stop();
            drop(aggs);
        }

        {
            let mut pending = self.pending.lock().unwrap();
            if let Some(block) = pending.flush() {
                if let Err(e) = self.queue.push(block) {
                    log::warn!("esmagent: rwctx: failed to enqueue final flush block: {e}");
                }
            }
        }

        self.client.stop();

        // `client.stop()` joins every worker thread, dropping each worker's
        // `Arc<PersistentQueue>` clone, so this ctx's own clone should be
        // the sole remaining reference.
        match Arc::try_unwrap(self.queue) {
            Ok(queue) => queue.close(),
            Err(queue) => {
                log::warn!(
                    "esmagent: rwctx: queue still shared after client stop; \
                     flushing to disk without a full close"
                );
                queue.flush_to_disk();
            }
        }
    }
}

/// Applies `url_relabel` to a per-destination copy of each series, returning
/// the survivors (series dropped by relabel are omitted). `None` passes every
/// series through unmodified. Copies are made so relabel never mutates the
/// caller's series (they may be pushed to other destinations concurrently).
fn apply_url_relabel(
    url_relabel: &Option<ParsedConfigs>,
    series: &[OwnedSeries],
) -> Vec<OwnedSeries> {
    match url_relabel {
        Some(relabel) => series
            .iter()
            .filter_map(|s| {
                let mut copy = s.clone();
                relabel.apply(&mut copy.labels).then_some(copy)
            })
            .collect(),
        None => series.to_vec(),
    }
}

/// Buffers already-relabeled `series` and enqueues any full blocks onto the
/// durable queue. Used directly for stream-aggregation output and pass-through
/// (both already relabeled); [`enqueue`] wraps it with the relabel step.
fn buffer_and_enqueue(
    pending: &Mutex<PendingSeries>,
    queue: &PersistentQueue,
    series: &[OwnedSeries],
) {
    if series.is_empty() {
        return;
    }
    let blocks = {
        let mut pending = pending.lock().unwrap();
        pending.add(series)
    };
    for block in blocks {
        if let Err(e) = queue.push(block) {
            log::warn!("esmagent: rwctx: failed to enqueue block: {e}");
        }
    }
}

/// Applies `url_relabel` (survivors only) then buffers and enqueues them. The
/// non-aggregated push path; the aggregated path relabels up front instead
/// (see [`RemoteWriteCtx::push`]).
fn enqueue(
    url_relabel: &Option<ParsedConfigs>,
    pending: &Mutex<PendingSeries>,
    queue: &PersistentQueue,
    series: &[OwnedSeries],
) {
    let survivors = apply_url_relabel(url_relabel, series);
    buffer_and_enqueue(pending, queue, &survivors);
}

/// Background loop: sleeps up to `flush_interval` (interruptibly, so `stop`
/// is observed within one [`FLUSH_STOP_POLL_INTERVAL`] tick), then flushes
/// whatever is buffered in `pending` onto `queue`. Exits as soon as `stop`
/// is observed, leaving any final flush to [`RemoteWriteCtx::stop`] so it
/// isn't racing this loop.
fn flush_loop(
    stop: &AtomicBool,
    pending: &Mutex<PendingSeries>,
    queue: &PersistentQueue,
    flush_interval: Duration,
) {
    loop {
        if wait_or_stop(stop, flush_interval) {
            return;
        }
        let block = {
            let mut pending = pending.lock().unwrap();
            pending.flush()
        };
        if let Some(block) = block {
            if let Err(e) = queue.push(block) {
                log::warn!("esmagent: rwctx: failed to enqueue periodic flush block: {e}");
            }
        }
    }
}

/// Sleeps up to `dur`, polling `stop` every [`FLUSH_STOP_POLL_INTERVAL`]
/// instead of in one long sleep. Returns `true` if `stop` was observed
/// before `dur` elapsed. Mirrors `client::wait_or_stop`.
fn wait_or_stop(stop: &AtomicBool, dur: Duration) -> bool {
    let mut remaining = dur;
    while remaining > Duration::ZERO {
        if stop.load(Ordering::SeqCst) {
            return true;
        }
        let step = remaining.min(FLUSH_STOP_POLL_INTERVAL);
        thread::sleep(step);
        remaining = remaining.saturating_sub(step);
    }
    stop.load(Ordering::SeqCst)
}
