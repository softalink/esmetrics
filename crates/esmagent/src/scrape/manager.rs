//! Scrape manager: reconciles discovered targets against a pool of
//! per-target scrape worker threads, and reports their status.
//!
//! Port of the target-lifecycle slice of `lib/promscrape/scraper.go`
//! (`runScrapers`/`scrapersReloader`) plus `lib/promscrape/targetstatus.go`
//! (the `ActiveTarget` shape) — faithful behavior, not identical internals.
//! Worker lifecycle (stop flag + join, poll-based idle wait) mirrors
//! `crate::client::Client`'s forwarding-tier worker pool. The actual
//! per-tick scrape/push loop lives in [`worker`] (split out to keep this
//! file under the repo's 800-line file cap); this module owns
//! configuration resolution, target discovery/diffing, and the manager's
//! public lifecycle (`start`/`reload`/`stop`/`targets_snapshot`). The
//! `/api/v1/targets` HTTP route and CLI wiring are later tasks — this
//! module only produces [`TargetsSnapshot`], not the route.
//!
//! ## Effective per-target config resolution
//!
//! [`build_job`] resolves everything that depends only on the job + global
//! config (not on a specific discovered target): `metric_relabel`/
//! `target_relabel` (compiled once from `ScrapeConfig::relabel_configs`/
//! `metric_relabel_configs` via [`esm_relabel::ParsedConfigs::from_raw_configs`]
//! — reusing that crate's existing validate + `if:`-compile path rather
//! than re-deriving it), `external_labels` (from
//! `GlobalConfig::external_labels`), and the sample/label-limit and
//! scrape-timeout/interval overrides (job value if set, else the global
//! default; `0` continues to mean "unlimited" for the limits, matching
//! [`super::scrapework::ScrapeConfigResolved`]'s convention).
//!
//! `worker::spawn_worker` finishes the resolution per target: it merges in
//! that target's own `Target::labels` (`__*`-stripped, post target-relabel)
//! as `ScrapeConfigResolved::target_labels`, and rebuilds a fresh
//! `metric_relabel` `ParsedConfigs` from the job's already-validated raw
//! `Vec<RelabelConfig>` (`ParsedConfigs` isn't `Clone`, and each worker
//! owns its `Scraper`/`ScrapeConfigResolved` independently so scraping
//! never needs a shared lock).
//!
//! **Deferred (documented, not implemented):** a relabel-overridden
//! per-target `__scrape_interval__`/`__scrape_timeout__` label does not
//! change that target's actual tick period here — the worker's tick
//! interval is always the job-or-global `scrape_interval`, matching this
//! task's brief. `target.rs` still emits those two labels (for `/targets`
//! reporting parity with upstream) when the job overrides them; this
//! module just doesn't read them back to vary a worker's cadence.
//!
//! ## Reconcile diff (by `scrape_url`)
//!
//! [`reconcile_locked`] is the single diff implementation shared by
//! [`ScrapeManager::reconcile_once`] (the deterministic test seam — tests
//! call it directly instead of waiting on the background timer) and the
//! background reconcile thread ([`ScrapeManager::spawn_reconcile_thread`]).
//! Per job: poll every [`super::discovery::Discovery`] provider, run
//! [`super::target::build_targets`], then diff the resulting active-target
//! `scrape_url`s against that job's currently-running
//! `HashMap<scrape_url, WorkerHandle>` — new URLs get a worker
//! ([`worker::spawn_worker`]); URLs no longer present get stopped+flushed
//! ([`worker::stop_worker`], which joins the worker thread, whose last act
//! before exiting is `Scraper::mark_stale_all` + a final `push_series`).
//! `dropped` targets are recomputed from scratch every cycle and replace
//! the shared snapshot's `dropped` list wholesale (no per-target dropped
//! diffing needed — a dropped target carries no worker/health state to
//! preserve).
//!
//! ## Lock discipline
//!
//! The shared `Arc<Mutex<TargetsSnapshot>>` is only ever locked for a
//! short, synchronous mutation (upsert/remove one `ActiveTarget`, or
//! replace the whole `dropped` list) — never across a scrape's blocking
//! HTTP call. Each worker owns its `Scraper` and `reqwest::blocking::Client`
//! exclusively, so no lock is held while scraping. The `Arc<Mutex<HashMap<String,
//! Job>>>` job registry IS locked for the duration of one reconcile pass
//! (diffing + starting/stopping workers) — and that pass DOES call
//! `provider.poll()` for every job's [`super::discovery::Discovery`] while
//! still holding the lock. That poll can block on network/file I/O:
//! `HttpSdDiscovery::poll` does a blocking HTTP GET (capped at that
//! provider's fixed 10s `HTTP_SD_TIMEOUT`) and `FileSdDiscovery::poll`
//! does blocking file reads. So `reconcile_locked` CAN block on http_sd/file
//! I/O for up to the SD refresh timeout while holding the `jobs` lock.
//! Scrape WORKERS are unaffected either way — they never acquire the `jobs`
//! lock at all, only [`worker::run_worker_loop`]'s brief `snapshot` lock,
//! never held across a scrape's HTTP call — so a slow discovery poll never
//! stalls an in-flight scrape. The practical consequence is bounded latency,
//! not a hang: [`ScrapeManager::reload`]/[`ScrapeManager::stop`]/startup
//! (all of which take the `jobs` lock) can wait up to ~the SD timeout per
//! hung http_sd poll before proceeding, matching this crate's existing
//! `client.rs` stop-latency convention.
//!
//! ## Per-target failure isolation
//!
//! [`super::scrapework::Scraper::scrape`] never panics (a fetch/status/
//! parse failure becomes `ScrapeResult { up: false, error: Some(_), .. }`,
//! not a panic or `Result::Err` bubbling up) — see its doc. Each target's
//! worker is an independent OS thread with its own `Scraper`/HTTP client,
//! so one target being unreachable only ever produces a `Health::Down`
//! entry for that target; it neither blocks nor crashes any other worker,
//! the reconcile thread, or the manager. [`worker::spawn_worker`] itself
//! returns a `Result` rather than panicking on a spawn/config-resolution
//! failure, so even a failure to *start* one target's worker is logged and
//! skipped rather than propagated — this codebase's release profile sets
//! `panic = "abort"`, so avoiding panics (not merely catching them) is the
//! actual isolation mechanism, not thread-boundary unwinding.

mod worker;

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use esm_relabel::{Label, ParsedConfigs};

use super::config::{self, GlobalConfig, ScrapeConfig, ScrapeConfigFile, ScrapeError};
use super::discovery::{Discovery, TargetGroup};
use super::providers::{build_providers, hash_secrets};
use super::target::build_targets;
use crate::sink::SeriesConsumer;
use worker::{spawn_worker, stop_worker, WorkerHandle};

/// How often the background reconcile thread re-polls discovery and
/// re-diffs targets. Fixed and small per the task brief ("keep simple") —
/// not derived from any job's `scrape_interval`, so a target still gets
/// picked up/torn down promptly even for a job with a long interval.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);

/// Dependencies [`ScrapeManager::start`] needs beyond the parsed config:
/// the global-relabel + fan-out seam shared with `crate::sink::push_series`
/// (the same seam pushed-data ingestion routes through — see
/// `crate::sink`'s module doc).
pub struct ManagerDeps {
    pub global_relabel: Option<ParsedConfigs>,
    pub consumer: Arc<dyn SeriesConsumer>,
    /// `-promscrape.suppressScrapeErrors`: when `true`, a worker whose
    /// scrape fails does NOT emit a `log::warn!` for that failure (it still
    /// records `last_error` into the snapshot for `/api/v1/targets`). When
    /// `false` (the default, matching upstream vmagent's default-on
    /// scrape-error logging in `scrapework.go`'s `logScrapeError`), each
    /// failed scrape logs once. See [`worker::record_result`].
    pub suppress_scrape_errors: bool,
}

/// A snapshot of every currently-known target: active (currently being
/// scraped, with live health/timing) and dropped (discovered but relabeled
/// away, or whose scrape URL couldn't be computed). Serde-serializable for
/// a later task's `/api/v1/targets` route. Cloned out of the manager's
/// shared state by [`ScrapeManager::targets_snapshot`] — never the live
/// state itself, so a caller holding a snapshot never blocks a worker.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct TargetsSnapshot {
    pub active: Vec<ActiveTarget>,
    pub dropped: Vec<DroppedTargetView>,
}

/// One actively-scraped target's current status. `labels`/`discovered_labels`
/// are fixed at worker-spawn time (a target whose labels change is, by
/// definition, a different `scrape_url`-or-not situation resolved by the
/// next reconcile — see the module doc); `health`/`last_error`/
/// `last_scrape_ms`/`last_scrape_duration_ms` are updated by the target's
/// own worker after every scrape.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ActiveTarget {
    pub scrape_pool: String,
    pub scrape_url: String,
    pub labels: Vec<Label>,
    pub discovered_labels: Vec<Label>,
    pub health: Health,
    pub last_error: Option<String>,
    pub last_scrape_ms: i64,
    pub last_scrape_duration_ms: i64,
}

/// A cheap, `Clone`-able read handle onto a [`ScrapeManager`]'s shared
/// [`TargetsSnapshot`], independent of the manager's own lifetime/borrows.
/// See [`ScrapeManager::targets_handle`].
#[derive(Clone)]
pub struct TargetsHandle(Arc<Mutex<TargetsSnapshot>>);

impl TargetsHandle {
    /// A clone of the current target status. Never blocks a worker — the
    /// lock is held only for the `clone()`. Same behavior as
    /// [`ScrapeManager::targets_snapshot`], reachable without a manager
    /// borrow.
    pub fn snapshot(&self) -> TargetsSnapshot {
        self.0.lock().unwrap().clone()
    }
}

/// A dropped target's pre-relabel labels — matches upstream's
/// `droppedTargetsMap`, used for `/targets` reporting.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DroppedTargetView {
    pub discovered_labels: Vec<Label>,
}

/// [`ActiveTarget::health`]: whether the target's most recent scrape
/// succeeded. `Unknown` is the transient state between a worker starting
/// and its first scrape completing (seeded by [`worker::spawn_worker`] so
/// the target is visible in [`ScrapeManager::targets_snapshot`]
/// immediately, not just after the first scrape).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Health {
    Up,
    Down,
    Unknown,
}

/// One `scrape_configs` job's resolved, running state: its discovery
/// providers, its compiled target-relabel, the job-vs-global config
/// resolution [`worker::spawn_worker`] needs to finish per target, and the
/// pool of currently-running workers keyed by `scrape_url`.
struct Job {
    /// The full parsed scrape config, kept for [`build_targets`] (which
    /// needs `job_name`/`scheme`/`metrics_path`/`params`/`static_configs`/
    /// ...) and for re-deriving each worker's `metric_relabel`
    /// (`sc.metric_relabel_configs`, already validated once in
    /// [`build_job`] — see its doc).
    sc: ScrapeConfig,
    external_labels: Vec<Label>,
    sample_limit: usize,
    label_limit: usize,
    scrape_timeout: Duration,
    /// The worker tick period — job's `scrape_interval` if set, else the
    /// global default. See the module doc's "deferred" note: this is NOT
    /// re-read from a relabel-overridden `__scrape_interval__` label.
    scrape_interval: Duration,
    providers: Vec<Box<dyn Discovery>>,
    target_relabel: ParsedConfigs,
    workers: HashMap<String, WorkerHandle>,
    /// Digest of this job's config (plus the global config it was resolved
    /// against), used by [`ScrapeManager::reload`] to decide whether a
    /// job needs rebuilding. See [`job_checksum`].
    checksum: u64,
}

/// Reconciles discovered targets against a pool of per-target scrape
/// worker threads. See the module doc for the full design.
pub struct ScrapeManager {
    jobs: Arc<Mutex<HashMap<String, Job>>>,
    snapshot: Arc<Mutex<TargetsSnapshot>>,
    global_relabel: Arc<Option<ParsedConfigs>>,
    consumer: Arc<dyn SeriesConsumer>,
    /// `-promscrape.suppressScrapeErrors`, threaded to every worker (see
    /// [`ManagerDeps::suppress_scrape_errors`]). Plain `bool` (Copy) — moved
    /// into each `spawn_worker` call and the reconcile thread's closure.
    suppress_scrape_errors: bool,
    reconcile_stop: Arc<AtomicBool>,
    reconcile_handle: Option<JoinHandle<()>>,
}

impl ScrapeManager {
    /// Validates `cfg`, builds every job's discovery providers +
    /// target-relabel, runs one reconcile pass synchronously (so `start`
    /// returns with scraping already under way, not waiting for the first
    /// background tick), then spawns the background reconcile thread.
    pub fn start(cfg: ScrapeConfigFile, deps: ManagerDeps) -> Result<ScrapeManager, ScrapeError> {
        config::validate(&cfg)?;

        let mut jobs = HashMap::with_capacity(cfg.scrape_configs.len());
        for sc in &cfg.scrape_configs {
            let checksum = job_checksum(sc, &cfg.global);
            let job = build_job(sc, &cfg.global, checksum)?;
            jobs.insert(sc.job_name.clone(), job);
        }

        let mut manager = ScrapeManager {
            jobs: Arc::new(Mutex::new(jobs)),
            snapshot: Arc::new(Mutex::new(TargetsSnapshot::default())),
            global_relabel: Arc::new(deps.global_relabel),
            consumer: deps.consumer,
            suppress_scrape_errors: deps.suppress_scrape_errors,
            reconcile_stop: Arc::new(AtomicBool::new(false)),
            reconcile_handle: None,
        };

        manager.reconcile_once();
        manager.spawn_reconcile_thread()?;

        Ok(manager)
    }

    /// Test seam (also the real implementation the background thread
    /// calls): one poll-diff-reconcile pass over every job, run
    /// synchronously on the caller's thread. See the module doc's
    /// "Reconcile diff" section.
    pub fn reconcile_once(&mut self) {
        let mut jobs = self.jobs.lock().unwrap();
        reconcile_locked(
            &mut jobs,
            &self.snapshot,
            &self.global_relabel,
            &self.consumer,
            self.suppress_scrape_errors,
        );
    }

    /// A clone of the current target status. Never blocks a worker — the
    /// lock is held only for the `clone()`.
    pub fn targets_snapshot(&self) -> TargetsSnapshot {
        self.snapshot.lock().unwrap().clone()
    }

    /// A cheap, cloneable handle onto the same shared snapshot
    /// [`targets_snapshot`](Self::targets_snapshot) reads. Unlike a
    /// `&ScrapeManager` borrow, a [`TargetsHandle`] outlives any particular
    /// borrow of the manager and stays valid across [`ScrapeManager::reload`]
    /// (which never replaces this `Arc`, only what's inside it) — so an HTTP
    /// handler can capture one at server-start time and keep reading live
    /// target status even while the caller elsewhere holds `&mut
    /// ScrapeManager` for a reload. See `scrape::wiring`'s module doc for the
    /// `/api/v1/targets` route this exists for.
    pub fn targets_handle(&self) -> TargetsHandle {
        TargetsHandle(Arc::clone(&self.snapshot))
    }

    /// Re-diffs jobs by `job_name` + [`job_checksum`]: a job present in
    /// `cfg` but not currently running (or whose checksum changed) is
    /// (re)built — a rebuilt job's OLD workers are stopped+flushed first,
    /// then the new job (with fresh, empty `workers`) is reconciled in on
    /// the same pass so its targets start immediately. A job no longer
    /// present in `cfg` is stopped+flushed and removed. A job whose
    /// checksum is unchanged is left completely untouched (its workers and
    /// discovery-provider caches survive the reload).
    ///
    /// `cfg` is validated as a whole before anything is touched, so a
    /// wholesale-invalid reload leaves the running manager exactly as it
    /// was (returns `Err`, no partial effect). A single job that is
    /// individually unbuildable (its `relabel_configs`/
    /// `metric_relabel_configs` pass this crate's structural YAML parse
    /// but fail `esm_relabel`'s deeper action-field/`if:` validation — see
    /// the module doc) does not fail the whole reload: it is logged and
    /// skipped, leaving that one job's previous state (if any) running
    /// unchanged.
    pub fn reload(&mut self, cfg: ScrapeConfigFile) -> Result<(), ScrapeError> {
        config::validate(&cfg)?;

        let mut jobs = self.jobs.lock().unwrap();

        let new_job_names: HashSet<&str> = cfg
            .scrape_configs
            .iter()
            .map(|sc| sc.job_name.as_str())
            .collect();
        let removed: Vec<String> = jobs
            .keys()
            .filter(|name| !new_job_names.contains(name.as_str()))
            .cloned()
            .collect();
        for name in removed {
            if let Some(job) = jobs.remove(&name) {
                stop_job(job, &name, &self.snapshot);
            }
        }

        for sc in &cfg.scrape_configs {
            let checksum = job_checksum(sc, &cfg.global);
            let unchanged = jobs
                .get(&sc.job_name)
                .is_some_and(|j| j.checksum == checksum);
            if unchanged {
                continue;
            }
            match build_job(sc, &cfg.global, checksum) {
                Ok(new_job) => {
                    if let Some(old_job) = jobs.remove(&sc.job_name) {
                        stop_job(old_job, &sc.job_name, &self.snapshot);
                    }
                    jobs.insert(sc.job_name.clone(), new_job);
                }
                Err(e) => log::warn!(
                    "esmagent scrape manager: reload: job {:?} failed to build ({e}); keeping its previous state",
                    sc.job_name
                ),
            }
        }

        reconcile_locked(
            &mut jobs,
            &self.snapshot,
            &self.global_relabel,
            &self.consumer,
            self.suppress_scrape_errors,
        );
        Ok(())
    }

    /// Stops the background reconcile thread, then every job's workers
    /// (each flushing a final stale-marker push before its thread exits).
    /// Joins the reconcile thread first so no concurrent reconcile pass can
    /// race this drain.
    pub fn stop(self) {
        self.reconcile_stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.reconcile_handle {
            let _ = handle.join();
        }

        let taken: Vec<(String, Job)> = self.jobs.lock().unwrap().drain().collect();
        for (job_name, job) in taken {
            stop_job(job, &job_name, &self.snapshot);
        }
    }

    /// Spawns the background reconcile thread. Never panics: an OS
    /// thread-creation failure is propagated as `Err` (see
    /// [`ScrapeManager::start`]) rather than via `.expect(...)` — mirroring
    /// [`worker::spawn_worker`], which returns `Err` on the same class of
    /// failure instead of panicking.
    fn spawn_reconcile_thread(&mut self) -> Result<(), ScrapeError> {
        let jobs = Arc::clone(&self.jobs);
        let snapshot = Arc::clone(&self.snapshot);
        let global_relabel = Arc::clone(&self.global_relabel);
        let consumer = Arc::clone(&self.consumer);
        let suppress_scrape_errors = self.suppress_scrape_errors;
        let stop = Arc::clone(&self.reconcile_stop);

        let handle = thread::Builder::new()
            .name("esmagent-scrape-reconcile".to_string())
            .spawn(move || {
                while !worker::wait_or_stop(&stop, RECONCILE_INTERVAL) {
                    let mut guard = jobs.lock().unwrap();
                    reconcile_locked(
                        &mut guard,
                        &snapshot,
                        &global_relabel,
                        &consumer,
                        suppress_scrape_errors,
                    );
                }
            })
            .map_err(|e| ScrapeError {
                msg: format!("cannot spawn esmagent scrape reconcile thread: {e}"),
            })?;
        self.reconcile_handle = Some(handle);
        Ok(())
    }
}

/// One poll-diff-reconcile pass, shared by [`ScrapeManager::reconcile_once`]
/// and the background reconcile thread — see the module doc.
fn reconcile_locked(
    jobs: &mut HashMap<String, Job>,
    snapshot: &Arc<Mutex<TargetsSnapshot>>,
    global_relabel: &Arc<Option<ParsedConfigs>>,
    consumer: &Arc<dyn SeriesConsumer>,
    suppress_scrape_errors: bool,
) {
    let mut all_dropped = Vec::new();

    for (job_name, job) in jobs.iter_mut() {
        let mut groups: Vec<TargetGroup> = Vec::new();
        for provider in job.providers.iter_mut() {
            groups.extend(provider.poll());
        }

        let (active, dropped) = build_targets(&job.sc, &job.target_relabel, &groups);
        all_dropped.extend(dropped.into_iter().map(|d| DroppedTargetView {
            discovered_labels: d.discovered_labels,
        }));

        let seen_urls: HashSet<String> = active.iter().map(|t| t.scrape_url.clone()).collect();

        for target in active {
            if job.workers.contains_key(&target.scrape_url) {
                continue;
            }
            let scrape_url = target.scrape_url.clone();
            match spawn_worker(
                job_name.clone(),
                job,
                target,
                Arc::clone(global_relabel),
                Arc::clone(consumer),
                Arc::clone(snapshot),
                suppress_scrape_errors,
            ) {
                Ok(handle) => {
                    job.workers.insert(handle.scrape_url.clone(), handle);
                }
                Err(e) => log::warn!(
                    "esmagent scrape manager: job {job_name:?}: failed to start worker for {scrape_url:?}: {e}"
                ),
            }
        }

        let vanished: Vec<String> = job
            .workers
            .keys()
            .filter(|url| !seen_urls.contains(*url))
            .cloned()
            .collect();
        for url in vanished {
            if let Some(handle) = job.workers.remove(&url) {
                stop_worker(handle, snapshot, job_name, &url);
            }
        }
    }

    snapshot.lock().unwrap().dropped = all_dropped;
}

/// Stops+flushes every worker in `job` (see [`worker::stop_worker`]).
fn stop_job(job: Job, job_name: &str, snapshot: &Arc<Mutex<TargetsSnapshot>>) {
    for (url, handle) in job.workers {
        stop_worker(handle, snapshot, job_name, &url);
    }
}

/// Builds one job's discovery providers + compiled target-relabel + the
/// job-vs-global config resolution [`worker::spawn_worker`] needs.
///
/// Eagerly validates BOTH `sc.relabel_configs` (used here, for
/// `target_relabel`) and `sc.metric_relabel_configs` (only actually
/// compiled per-worker in [`worker::spawn_worker`] — see the module doc)
/// so a job with an invalid `metric_relabel_configs` fails fast here
/// rather than only once a target is discovered for it.
fn build_job(sc: &ScrapeConfig, global: &GlobalConfig, checksum: u64) -> Result<Job, ScrapeError> {
    let target_relabel = ParsedConfigs::from_raw_configs(sc.relabel_configs.clone())
        .map_err(|e| relabel_build_error(&sc.job_name, "relabel_configs", &e))?;
    ParsedConfigs::from_raw_configs(sc.metric_relabel_configs.clone())
        .map_err(|e| relabel_build_error(&sc.job_name, "metric_relabel_configs", &e))?;

    let external_labels = global
        .external_labels
        .iter()
        .map(|(name, value)| Label {
            name: name.clone(),
            value: value.clone(),
        })
        .collect();
    let sample_limit = if sc.sample_limit > 0 {
        sc.sample_limit
    } else {
        global.sample_limit
    };
    let label_limit = if sc.label_limit > 0 {
        sc.label_limit
    } else {
        global.label_limit
    };
    let scrape_timeout = sc.scrape_timeout.unwrap_or(global.scrape_timeout);
    let scrape_interval = sc.scrape_interval.unwrap_or(global.scrape_interval);

    Ok(Job {
        sc: sc.clone(),
        external_labels,
        sample_limit,
        label_limit,
        scrape_timeout,
        scrape_interval,
        providers: build_providers(sc)?,
        target_relabel,
        workers: HashMap::new(),
        checksum,
    })
}

fn relabel_build_error(job_name: &str, field: &str, e: &esm_relabel::RelabelError) -> ScrapeError {
    ScrapeError {
        msg: format!("job_name {job_name:?}: invalid `{field}`: {e}"),
    }
}

/// Digest of everything that affects one job's resolved behavior: the job's
/// own config plus the global config it's resolved against (global affects
/// `external_labels`/limit-fallback/interval-fallback/timeout-fallback —
/// see [`build_job`]). Hashes the `Debug` representation rather than adding
/// `Hash` to every config type transitively (`ScrapeConfig` alone pulls in
/// `AuthConfig`/`TlsConfig`/`RelabelConfig`/`StaticConfig`/...) — cheap,
/// and every one of those types already derives `Debug`.
///
/// The `Debug` string covers every non-secret field, but several SD-provider
/// configs (and `oauth2.client_secret`) hand-write a *redacting* `Debug` that
/// prints `"<redacted>"` in place of secret values. Hashing the `Debug`
/// alone would therefore be blind to a secret-only change: rotating a bearer
/// token / EC2 secret key / OAuth2 client secret would leave the digest
/// unchanged, so [`ScrapeManager::reload`] would treat the job as unchanged
/// and never rebuild its SD provider (the rotated secret would not take
/// effect until a full process restart). To close that gap,
/// [`hash_secrets`](super::providers::hash_secrets)
/// additionally feeds the *real* secret values into the same hasher — as raw
/// bytes only, never as a `String`, log, or `Debug` output, so no secret is
/// ever materialized in a leakable form.
fn job_checksum(sc: &ScrapeConfig, global: &GlobalConfig) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    format!("{sc:?}").hash(&mut hasher);
    format!("{global:?}").hash(&mut hasher);
    hash_secrets(sc, &mut hasher);
    hasher.finish()
}

#[cfg(test)]
#[path = "manager_tests.rs"]
mod tests;
