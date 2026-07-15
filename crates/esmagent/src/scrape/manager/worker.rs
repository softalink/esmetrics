//! One target's scrape worker: owns a [`Scraper`] + `reqwest::blocking::Client`
//! exclusively (no lock held across a scrape), ticks on its job's resolved
//! `scrape_interval`, and reports status into the manager's shared
//! [`TargetsSnapshot`]. Split out of `manager.rs` to keep that file under
//! the repo's 800-line cap — see its module doc for the full design
//! (effective-config resolution, reconcile diff, lock discipline,
//! per-target failure isolation).
//!
//! Mirrors `crate::client::Client`'s worker-pool shutdown pattern: a
//! per-worker `stop: Arc<AtomicBool>` plus a poll-based idle wait
//! ([`wait_or_stop`], duplicated from `crate::client`'s `wait_or_stop` —
//! that one is private to its module, same rationale as
//! `discovery::build_http_client`'s doc comment for why this is
//! duplicated rather than shared) so [`stop_worker`]'s `join()` never
//! hangs waiting out a long `scrape_interval`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use esm_relabel::{Label, ParsedConfigs};

use super::{ActiveTarget, Health, Job, TargetsSnapshot};
use crate::client::TlsConfig;
use crate::scrape::config::ScrapeError;
use crate::scrape::scrapework::{ScrapeConfigResolved, ScrapeResult, Scraper};
use crate::scrape::target::Target;
use crate::sink::{push_series, SeriesConsumer};

/// How often a worker parked in its idle-between-scrapes wait re-checks its
/// stop flag. Small relative to any realistic `scrape_interval`, so
/// [`stop_worker`] stays responsive without busy-spinning — same constant
/// value/rationale as `crate::client::STOP_POLL_INTERVAL`.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// A running target worker: its stop flag and thread handle, plus the
/// `scrape_url` it owns (so the manager's `HashMap<String, WorkerHandle>`
/// key and the handle agree without a second lookup).
pub(super) struct WorkerHandle {
    pub(super) scrape_url: String,
    stop: Arc<AtomicBool>,
    thread: JoinHandle<()>,
}

/// Everything one worker thread needs for the lifetime of its loop.
/// Bundled (mirrors `crate::client::WorkerCtx`) so [`run_worker_loop`]
/// takes one value instead of a long parameter list.
struct WorkerCtx {
    stop: Arc<AtomicBool>,
    client: reqwest::blocking::Client,
    job_name: String,
    scrape_url: String,
    labels: Vec<Label>,
    discovered_labels: Vec<Label>,
    interval: Duration,
    global_relabel: Arc<Option<ParsedConfigs>>,
    consumer: Arc<dyn SeriesConsumer>,
    snapshot: Arc<Mutex<TargetsSnapshot>>,
    /// `-promscrape.suppressScrapeErrors`: when `false` (default),
    /// [`record_result`] logs each failed scrape once. See
    /// [`super::ManagerDeps::suppress_scrape_errors`].
    suppress_scrape_errors: bool,
}

/// Finishes resolving `target`'s [`ScrapeConfigResolved`] from its owning
/// `job` (see `manager.rs`'s module doc for the split between `build_job`'s
/// job-level resolution and this target-level finish), builds a dedicated
/// `reqwest::blocking::Client` for it, seeds an initial `Health::Unknown`
/// entry into `snapshot` (so the target is visible immediately, not just
/// after its first scrape), and spawns its worker thread.
///
/// Never panics: a `metric_relabel_configs` rebuild failure (unexpected —
/// `manager::build_job` already validated the same raw configs once for
/// this job) or a thread-spawn failure is returned as `Err` and logged by
/// the caller, not propagated as a panic — one target's start-up failure
/// must never take down the manager or any other target's worker.
pub(super) fn spawn_worker(
    job_name: String,
    job: &Job,
    target: Target,
    global_relabel: Arc<Option<ParsedConfigs>>,
    consumer: Arc<dyn SeriesConsumer>,
    snapshot: Arc<Mutex<TargetsSnapshot>>,
    suppress_scrape_errors: bool,
) -> Result<WorkerHandle, ScrapeError> {
    let metric_relabel = ParsedConfigs::from_raw_configs(job.sc.metric_relabel_configs.clone())
        .map_err(|e| ScrapeError {
            msg: format!("job_name {job_name:?}: invalid `metric_relabel_configs`: {e}"),
        })?;

    let resolved = ScrapeConfigResolved {
        metric_relabel,
        honor_labels: job.sc.honor_labels,
        honor_timestamps: job.sc.honor_timestamps,
        external_labels: job.external_labels.clone(),
        target_labels: target.labels.clone(),
        sample_limit: job.sample_limit,
        label_limit: job.label_limit,
        scrape_timeout: job.scrape_timeout,
        max_scrape_size: job.sc.max_scrape_size,
        enable_compression: job.sc.enable_compression,
        auth: job.sc.auth.clone(),
        tls: job.sc.tls.clone(),
    };

    let client = build_scrape_client(&job.sc.tls).map_err(|e| ScrapeError {
        msg: format!("job_name {job_name:?}: {e}"),
    })?;

    let scrape_url = target.scrape_url;
    let labels = target.labels;
    let discovered_labels = target.discovered_labels;

    upsert_active(
        &snapshot,
        ActiveTarget {
            scrape_pool: job_name.clone(),
            scrape_url: scrape_url.clone(),
            labels: labels.clone(),
            discovered_labels: discovered_labels.clone(),
            health: Health::Unknown,
            last_error: None,
            last_scrape_ms: 0,
            last_scrape_duration_ms: 0,
        },
    );

    let stop = Arc::new(AtomicBool::new(false));
    let ctx = WorkerCtx {
        stop: Arc::clone(&stop),
        client,
        job_name,
        scrape_url: scrape_url.clone(),
        labels,
        discovered_labels,
        interval: job.scrape_interval,
        global_relabel,
        consumer,
        snapshot,
        suppress_scrape_errors,
    };

    let thread_name = format!("esmagent-scrape-{}", ctx.job_name);
    let thread = thread::Builder::new()
        .name(thread_name)
        .spawn(move || run_worker_loop(ctx, resolved))
        .map_err(|e| ScrapeError {
            msg: format!("cannot spawn scrape worker for {scrape_url:?}: {e}"),
        })?;

    Ok(WorkerHandle {
        scrape_url,
        stop,
        thread,
    })
}

/// Signals `handle`'s worker to stop and joins it (the worker's last act
/// before its thread exits is a `Scraper::mark_stale_all` +
/// `push_series` flush — see [`run_worker_loop`]), then removes its entry
/// from `snapshot.active`. Removal happens only after `join()` returns, so
/// a target never disappears from `/targets` before its stale markers have
/// actually been pushed.
pub(super) fn stop_worker(
    handle: WorkerHandle,
    snapshot: &Arc<Mutex<TargetsSnapshot>>,
    job_name: &str,
    scrape_url: &str,
) {
    handle.stop.store(true, Ordering::SeqCst);
    let _ = handle.thread.join();
    remove_active(snapshot, job_name, scrape_url);
}

/// One worker's loop: scrape immediately (so a caller polling for the
/// effect of a fresh `reconcile_once()` doesn't need to wait out a full
/// `scrape_interval` — production behavior is still interval-driven, this
/// just means the FIRST tick isn't delayed), push the result, update this
/// target's snapshot entry, then idle-wait for `ctx.interval` (polling
/// `stop`) before the next tick. On stop, flushes stale markers for every
/// series this target's `Scraper` was still tracking.
///
/// Never panics: [`Scraper::scrape`] itself never panics (see its doc), and
/// every lock taken here ([`upsert_active`]) is held only for a plain data
/// mutation, never across the scrape's blocking HTTP call.
fn run_worker_loop(ctx: WorkerCtx, resolved: ScrapeConfigResolved) {
    let mut scraper = Scraper::new(resolved);
    loop {
        let result = scraper.scrape(&ctx.client, &ctx.scrape_url);
        record_result(&ctx, result);

        if wait_or_stop(&ctx.stop, ctx.interval) {
            break;
        }
    }

    let stale = scraper.mark_stale_all(now_millis());
    push_series(&ctx.global_relabel, &ctx.consumer, stale);
}

/// Pushes a scrape's series to `ctx.consumer` and records its outcome
/// (health/error/timing) into `ctx.snapshot`. A failed scrape (`up ==
/// false`) is also logged once via `log::warn!` unless
/// `-promscrape.suppressScrapeErrors` is set — port of upstream vmagent's
/// default-on `logScrapeError`/`logger.Warnf` scrape-failure logging
/// (`scrapework.go`). The error string comes from `scrapework` and is
/// already secret-free (target URL + failure message, never auth
/// credentials — see `scrapework::fetch_and_parse`'s error construction).
fn record_result(ctx: &WorkerCtx, result: ScrapeResult) {
    let ScrapeResult {
        series,
        up,
        error,
        duration,
        ..
    } = result;

    if !up && !ctx.suppress_scrape_errors {
        log::warn!(
            "esmagent: scrape of {} failed: {}",
            ctx.scrape_url,
            error.as_deref().unwrap_or("unknown error")
        );
    }

    push_series(&ctx.global_relabel, &ctx.consumer, series);

    upsert_active(
        &ctx.snapshot,
        ActiveTarget {
            scrape_pool: ctx.job_name.clone(),
            scrape_url: ctx.scrape_url.clone(),
            labels: ctx.labels.clone(),
            discovered_labels: ctx.discovered_labels.clone(),
            health: if up { Health::Up } else { Health::Down },
            last_error: error,
            last_scrape_ms: now_millis(),
            last_scrape_duration_ms: duration.as_millis().min(i64::MAX as u128) as i64,
        },
    );
}

/// Inserts or replaces `entry` in `snapshot.active`, matched by
/// `(scrape_pool, scrape_url)` (a `scrape_url` alone isn't guaranteed
/// unique across two different jobs scraping the same address).
fn upsert_active(snapshot: &Mutex<TargetsSnapshot>, entry: ActiveTarget) {
    let mut snap = snapshot.lock().unwrap();
    match snap
        .active
        .iter_mut()
        .find(|a| a.scrape_pool == entry.scrape_pool && a.scrape_url == entry.scrape_url)
    {
        Some(existing) => *existing = entry,
        None => snap.active.push(entry),
    }
}

fn remove_active(snapshot: &Mutex<TargetsSnapshot>, job_name: &str, scrape_url: &str) {
    let mut snap = snapshot.lock().unwrap();
    snap.active
        .retain(|a| !(a.scrape_pool == job_name && a.scrape_url == scrape_url));
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`] instead
/// of in one long sleep — see `crate::client::wait_or_stop`'s doc (this is
/// the same algorithm, duplicated per that function's "why duplicated"
/// rationale: it's private to its module). Returns `true` if `stop` was
/// observed before `dur` elapsed.
pub(super) fn wait_or_stop(stop: &AtomicBool, dur: Duration) -> bool {
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

/// Builds a `reqwest::blocking::Client` applying `tls`, the same way
/// `discovery::build_http_client`/`client::build_client` do (duplicated —
/// both are private to their own modules; see their doc comments for the
/// established "duplicated rather than shared" rationale in this crate).
/// No client-wide timeout is set here: `scrapework::fetch_and_parse` sets a
/// per-request timeout from `ScrapeConfigResolved::scrape_timeout` already.
fn build_scrape_client(tls: &TlsConfig) -> Result<reqwest::blocking::Client, ScrapeError> {
    let mut builder = reqwest::blocking::Client::builder();
    if tls.insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(ca_file) = &tls.ca_file {
        let pem = std::fs::read(ca_file)
            .map_err(|e| scrape_error(format!("cannot read CA file {ca_file:?}: {e}")))?;
        let cert = reqwest::Certificate::from_pem(&pem)
            .map_err(|e| scrape_error(format!("invalid CA certificate in {ca_file:?}: {e}")))?;
        builder = builder.add_root_certificate(cert);
    }
    if let (Some(cert_file), Some(key_file)) = (&tls.cert_file, &tls.key_file) {
        let mut identity_pem = std::fs::read(cert_file)
            .map_err(|e| scrape_error(format!("cannot read cert file {cert_file:?}: {e}")))?;
        let mut key_pem = std::fs::read(key_file)
            .map_err(|e| scrape_error(format!("cannot read key file {key_file:?}: {e}")))?;
        identity_pem.push(b'\n');
        identity_pem.append(&mut key_pem);
        let identity = reqwest::Identity::from_pem(&identity_pem)
            .map_err(|e| scrape_error(format!("invalid client cert/key: {e}")))?;
        builder = builder.identity(identity);
    }
    builder
        .build()
        .map_err(|e| scrape_error(format!("cannot build scrape http client: {e}")))
}

fn scrape_error(msg: impl Into<String>) -> ScrapeError {
    ScrapeError { msg: msg.into() }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
