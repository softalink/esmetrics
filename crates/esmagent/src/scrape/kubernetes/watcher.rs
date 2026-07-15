//! The Kubernetes watch cache: a background thread that runs an initial LIST
//! (with `continue`-token pagination), then a streamed WATCH long-poll,
//! keeping an in-memory `key() -> K8sObject` cache current. [`Watcher::target_groups`]
//! builds [`TargetGroup`]s from a snapshot of that cache on demand.
//!
//! Port of the relevant slices of upstream `lib/promscrape/discoveryutils/
//! kubernetes/api_watcher.go`'s `reloadObjects`/`watchForUpdates` (v1.146.0),
//! reshaped for `reqwest::blocking` + `std::thread` (no tokio in this crate
//! — see [`crate::client`]'s module doc for the established pattern this
//! mirrors).
//!
//! ## Loop design
//!
//! One background thread runs [`run`]: outer loop is LIST-then-WATCH.
//!
//! - **LIST** ([`list_all`]): fetches pages via [`ApiConfig::list_url`],
//!   following `metadata.continue` until a page has none, accumulating
//!   objects and taking the *last* page's `resourceVersion` as the watch
//!   start version (matches upstream: only the final page's resourceVersion
//!   is meaningful once pagination completes). The cache is replaced
//!   wholesale ([`replace_cache`]) with the result — never merged. A `410
//!   Gone` mid-pagination (an expired `continue` token) restarts pagination
//!   from scratch immediately, no backoff (the token is already useless, so
//!   waiting buys nothing). Any other LIST error is logged and retried with
//!   an exponential, stop-aware backoff ([`wait_or_stop`], mirroring
//!   `crate::client`'s helper of the same name/shape — duplicated locally
//!   since that one is private to its module).
//! - **WATCH** ([`watch_once`]): one GET to [`ApiConfig::watch_url`] with a
//!   fixed `timeoutSeconds` ([`WATCH_TIMEOUT_SECS`]), streamed line-by-line
//!   via `BufReader::read_line` (each line is one [`WatchEvent`] JSON).
//!   `ADDED`/`MODIFIED` upsert the cache by [`K8sObject::key`];  `DELETED`
//!   removes by key (computed straight from the raw JSON `metadata`, so a
//!   delete doesn't depend on the rest of the object parsing cleanly);
//!   `BOOKMARK` only updates the locally tracked latest `resourceVersion` and
//!   touches nothing else; an in-band `ERROR` event ends the stream (code
//!   410 -> re-LIST, any other code -> re-watch), mirroring upstream
//!   `readObjectUpdateStream`. A single malformed line or object is logged
//!   and skipped — the stream is not aborted for it.
//!
//! **Resume, don't re-LIST** — mirroring upstream `reloadObjects` +
//! `watchForUpdates`. The loop keeps a `resource_version: Option<String>`:
//! it LISTs only when that is `None`, and otherwise WATCHes from the tracked
//! rv without a LIST (upstream `reloadObjects` returns the cached rv without
//! re-listing whenever it is non-empty). On watch end:
//! - **`410 Gone`** (HTTP status on the watch, or an in-band `ERROR` event
//!   with code 410): the rv is stale — clear it, so the next iteration
//!   re-LISTs. No backoff for a single `410` (the old state is already
//!   invalid, and waiting buys nothing). A pathological server that answers
//!   LIST `200` but immediately `410`s every WATCH would otherwise flood
//!   LIST+WATCH requests unbounded (no delay, bounded only by RTT); to guard
//!   against that, a `consecutive_gone` counter tracks back-to-back `Gone`
//!   endings (reset by any watch that applied at least one event or closed
//!   cleanly) and, once it exceeds one, applies [`RETRY_MIN`] via
//!   [`wait_or_stop`] before the next re-LIST. The common single-`410` case
//!   stays immediate; only a *repeated* `410` loop is throttled.
//! - **clean EOF** (the server closing after `timeoutSeconds`): resume the
//!   watch from the latest tracked rv, no LIST, after only a
//!   [`MIN_REWATCH_DELAY`] (guards against a hot loop if the server closes
//!   instantly; a normal 60s close re-watches promptly).
//! - **transient error** (read/transport error, non-2xx status, non-410
//!   `ERROR` event): keep the rv and re-watch from it after a growing,
//!   stop-aware backoff ([`wait_or_stop`], `RETRY_MIN`..`RETRY_MAX`). If the
//!   rv has gone stale, the next attempt surfaces a `410`, which clears it
//!   and forces the re-LIST above.
//! - **stop**: exit.
//!
//! `wait_or_stop` mirrors `crate::client`'s helper of the same name/shape
//! (duplicated locally since that one is private to its module).
//!
//! ## Bounded (not instant) `stop()` latency
//!
//! `stop()` is observed within [`STOP_POLL_INTERVAL`] while the thread is
//! backing off or between watch lines, but if it lands while a watch
//! `read_line` is genuinely blocked on the socket, the join is bounded by
//! [`WATCH_HTTP_TIMEOUT`] (~70s) instead — `reqwest::blocking` has no cheaper
//! mid-request cancel. `WATCH_TIMEOUT_SECS` is kept at 60s specifically to
//! cap this at ~1min; the resume-not-relist design above makes the resulting
//! frequent clean closes cheap (one re-watch round-trip, not a LIST). This
//! matches `crate::client::Client::stop`'s documented precedent of being
//! bounded by an in-flight request's own timeout.
//!
//! ## Lock discipline
//!
//! The cache `Mutex` is held only for the duration of a single swap/insert/
//! remove — never across an HTTP call or a JSON parse. [`Watcher::target_groups`]
//! locks just long enough to clone the current `Arc<K8sObject>` values into a
//! `Vec`, then builds groups from that snapshot with the lock released.
//!
//! ## Ingress v1 -> v1beta1 fallback
//!
//! Mirrors upstream's `useNetworkingV1Beta1`: the `ingress` role's LIST/WATCH
//! URLs point at `networking.k8s.io/v1` by default. If a request against
//! that path 404s, [`get_with_v1beta1_fallback`] flips a per-`Watcher`
//! sticky `AtomicBool` and retries once against `v1beta1` (a plain string
//! replace on the already-built URL); every later request in this watcher's
//! lifetime starts on `v1beta1` directly.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use reqwest::blocking::Response;

use super::client::ApiConfig;
use super::object::{self, K8sObject, WatchEvent};
use super::registry::BuildCtx;
use crate::scrape::config::K8sSelector;
use crate::scrape::discovery::TargetGroup;

/// Client-side timeout for a single LIST page fetch.
const LIST_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// `timeoutSeconds` query param sent to the k8s watch endpoint: how long the
/// server holds the long-poll connection open before closing it cleanly. Kept
/// deliberately short (60s) so that a [`Watcher::stop`] landing while a watch
/// read is genuinely blocked mid-socket is bounded by ~1min rather than the
/// several minutes a larger value would imply — see [`WATCH_HTTP_TIMEOUT`].
/// A clean 60s close is cheap now that the loop *resumes* the watch from the
/// tracked resourceVersion instead of re-LISTing (see the module doc), so a
/// frequent re-watch costs one HTTP round-trip, not a full LIST.
const WATCH_TIMEOUT_SECS: u64 = 60;

/// Client-side ceiling for the watch HTTP call: `WATCH_TIMEOUT_SECS` plus a
/// small grace buffer so the client doesn't race the server's own close. This
/// bounds the worst-case latency of [`Watcher::stop`] while a watch read is
/// blocked — mirroring `crate::client::Client::stop`'s documented precedent
/// of being bounded by an in-flight request's own timeout rather than
/// cancellable instantly (`reqwest::blocking` has no cheaper cancel path).
/// With `WATCH_TIMEOUT_SECS = 60` this is ~70s, so `stop()` returns within
/// ~1min in that worst case, not ~5.5min.
const WATCH_HTTP_TIMEOUT: Duration = Duration::from_secs(WATCH_TIMEOUT_SECS + 10);

/// Backoff floor/ceiling for LIST retry and watch-*error* reconnect loops.
const RETRY_MIN: Duration = Duration::from_millis(200);
const RETRY_MAX: Duration = Duration::from_secs(30);

/// Minimal delay before resuming a watch after a *clean* server-side close
/// (the 60s `timeoutSeconds` elapsing). Small — a clean close is the normal
/// case and should re-watch promptly — but non-zero so a server that closes
/// the connection immediately can't spin the loop into a hot re-watch loop.
const MIN_REWATCH_DELAY: Duration = Duration::from_millis(50);

/// How often [`wait_or_stop`] re-checks the stop flag while backing off.
/// Local copy of `crate::client`'s constant of the same name/purpose (that
/// one is private to its module).
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Everything the background watch thread needs for its lifetime. Bundled so
/// helper functions take one reference instead of a long parameter list.
struct WatchCtx {
    api: Arc<ApiConfig>,
    role: String,
    namespace: Option<String>,
    selectors: Vec<K8sSelector>,
    cache: Arc<Mutex<HashMap<String, Arc<K8sObject>>>>,
    stop: Arc<AtomicBool>,
    /// Sticky ingress v1 -> v1beta1 fallback flag — see the module doc.
    use_v1beta1: AtomicBool,
}

/// Owns the background watch thread and the object cache it maintains.
/// [`Watcher::target_groups`] reads a snapshot of the cache;
/// [`Watcher::stop`] (also run on `Drop`) signals the thread and joins it.
pub struct Watcher {
    cache: Arc<Mutex<HashMap<String, Arc<K8sObject>>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Spawns the background LIST+WATCH thread for one `(role, namespace,
/// selectors)` and returns immediately; the cache starts empty and is
/// populated by the thread's first LIST.
pub fn start(
    api: Arc<ApiConfig>,
    role: String,
    namespace: Option<String>,
    selectors: Vec<K8sSelector>,
) -> Watcher {
    let cache: Arc<Mutex<HashMap<String, Arc<K8sObject>>>> = Arc::new(Mutex::new(HashMap::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let ctx = Arc::new(WatchCtx {
        api,
        role,
        namespace,
        selectors,
        cache: Arc::clone(&cache),
        stop: Arc::clone(&stop),
        use_v1beta1: AtomicBool::new(false),
    });

    let handle = thread::spawn(move || run(&ctx));

    Watcher {
        cache,
        stop,
        handle: Some(handle),
    }
}

impl Watcher {
    /// Builds [`TargetGroup`]s from a snapshot of the current cache. Locks
    /// only long enough to clone the `Arc<K8sObject>` values out, then
    /// builds groups (which may allocate but never blocks on I/O) with the
    /// lock released.
    pub fn target_groups(&self, ctx: &BuildCtx) -> Vec<TargetGroup> {
        let objects: Vec<Arc<K8sObject>> = {
            let cache = self.cache.lock().unwrap();
            cache.values().cloned().collect()
        };
        objects.iter().flat_map(|o| o.target_groups(ctx)).collect()
    }

    /// Clone of the cache `Arc`, for registration in an
    /// [`super::registry::ObjectRegistry`] so other roles' builders can reach
    /// this watcher's objects. Never locks — just bumps the refcount.
    pub fn cache(&self) -> Arc<Mutex<HashMap<String, Arc<K8sObject>>>> {
        Arc::clone(&self.cache)
    }

    /// Signals the watch thread to stop and joins it. Idempotent (a second
    /// call is a no-op since the handle is already taken). Also invoked by
    /// `Drop`.
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The background thread's whole life, mirroring upstream `reloadObjects` +
/// `watchForUpdates`: keep a `resource_version: Option<String>`. When it is
/// `None`, LIST (populating the cache and taking the list's
/// `resourceVersion`); then WATCH from that rv. On watch end, *resume* from
/// the tracked rv rather than re-LISTing, except when the rv is known stale
/// (a `410`), which clears it and forces a fresh LIST on the next iteration.
/// Never panics — every I/O/parse error is logged and drives a retry (see the
/// module doc for the exact recovery rules).
fn run(ctx: &WatchCtx) {
    let mut resource_version: Option<String> = None;
    // Grows only across consecutive *error* re-watches; reset after any LIST
    // or clean close.
    let mut error_backoff = RETRY_MIN;
    // Counts back-to-back `Gone` (410) watch endings with no event/clean-close
    // in between, so a pathological always-410 server can't hot-loop
    // LIST+WATCH — see the module doc's `410 Gone` bullet.
    let mut consecutive_gone: u32 = 0;

    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        // (Re-)LIST only when we have no valid resourceVersion to resume
        // from. This is the upstream `reloadObjects` invariant: it re-lists
        // iff the cached rv is empty, and otherwise returns the cached rv
        // for the watch to resume from without a LIST.
        let rv = match resource_version.take() {
            Some(rv) => rv,
            None => {
                let Some((objects, listed_rv)) = list_all(ctx) else {
                    return; // stop requested mid-list
                };
                replace_cache(ctx, objects);
                error_backoff = RETRY_MIN;
                listed_rv
            }
        };

        if ctx.stop.load(Ordering::SeqCst) {
            return;
        }

        match watch_once(ctx, &rv) {
            WatchEnd::Stopped => return,
            WatchEnd::Gone { had_events } => {
                // The rv is stale (`410 Gone`, or an in-band `ERROR` event
                // with code 410): drop it so the next iteration re-LISTs.
                log::info!(
                    "esmagent k8s watcher ({}): watch resourceVersion gone (410); re-listing",
                    ctx.role
                );
                resource_version = None;
                // A watch that actually delivered an event before going
                // stale isn't a hot-loop symptom — only a *string* of
                // event-free `Gone`s is.
                consecutive_gone = if had_events { 0 } else { consecutive_gone + 1 };
                if consecutive_gone > 1 {
                    // Repeated back-to-back 410s: apply a small stop-aware
                    // floor before the next re-LIST so a pathological
                    // always-410 server can't flood LIST+WATCH requests.
                    if wait_or_stop(&ctx.stop, RETRY_MIN) {
                        return;
                    }
                }
            }
            WatchEnd::Closed(latest) => {
                // Normal server-side close after `timeoutSeconds`: resume the
                // watch from the latest tracked rv, no re-LIST. Minimal delay
                // guards against a hot loop if the server closes instantly.
                resource_version = Some(latest);
                error_backoff = RETRY_MIN;
                consecutive_gone = 0;
                if wait_or_stop(&ctx.stop, MIN_REWATCH_DELAY) {
                    return;
                }
            }
            WatchEnd::Errored(latest) => {
                // Transient error mid-watch (read/transport error, non-2xx
                // status, or a non-410 `ERROR` event): keep the rv and
                // re-watch from it after a stop-aware backoff. If the rv has
                // gone stale, the next attempt surfaces a `410`, which clears
                // it and forces the re-LIST above.
                resource_version = Some(latest);
                if wait_or_stop(&ctx.stop, error_backoff) {
                    return;
                }
                error_backoff = next_backoff(error_backoff);
            }
        }
    }
}

/// Doubles `cur`, capped at [`RETRY_MAX`].
fn next_backoff(cur: Duration) -> Duration {
    (cur * 2).min(RETRY_MAX)
}

/// Sleeps up to `dur`, polling `stop` every [`STOP_POLL_INTERVAL`] instead of
/// in one long sleep, so a [`Watcher::stop`] request is observed within one
/// poll tick rather than at the end of the full backoff. Returns `true` if
/// `stop` was observed before `dur` elapsed. Local copy of
/// `crate::client::wait_or_stop` (private to its module) — see this crate's
/// established pattern for the stop/backoff cadence.
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

/// Replaces the whole cache with `objects`, keyed by [`K8sObject::key`].
/// Locks only for the swap itself.
fn replace_cache(ctx: &WatchCtx, objects: Vec<K8sObject>) {
    let mut map = HashMap::with_capacity(objects.len());
    for obj in objects {
        map.insert(obj.key(), Arc::new(obj));
    }
    *ctx.cache.lock().unwrap() = map;
}

/// Runs a full LIST (following `continue` across pages) to completion,
/// retrying on error per the module doc. Returns `None` only when `stop` was
/// observed mid-retry (the thread should exit); otherwise blocks until a
/// full, consistent object set is obtained.
fn list_all(ctx: &WatchCtx) -> Option<(Vec<K8sObject>, String)> {
    let mut backoff = RETRY_MIN;
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return None;
        }

        match list_pages(ctx) {
            ListPagesOutcome::Ok(objects, resource_version) => {
                return Some((objects, resource_version));
            }
            ListPagesOutcome::Gone => {
                log::info!(
                    "esmagent k8s watcher ({}): list continue token gone (410); restarting list",
                    ctx.role
                );
                // No backoff: the token is already unusable, waiting buys
                // nothing.
            }
            ListPagesOutcome::Err(msg) => {
                log::warn!(
                    "esmagent k8s watcher ({}): list failed: {msg}; retrying in {backoff:?}",
                    ctx.role
                );
                if wait_or_stop(&ctx.stop, backoff) {
                    return None;
                }
                backoff = next_backoff(backoff);
            }
        }
    }
}

enum ListPagesOutcome {
    Ok(Vec<K8sObject>, String),
    Gone,
    Err(String),
}

/// Fetches every page of one LIST attempt (following `metadata.continue`),
/// accumulating objects. Stops at the first page-level error/410 without
/// retrying internally — that's [`list_all`]'s job, so a mid-pagination
/// failure always restarts pagination from the beginning rather than resuming
/// with a possibly-stale `continue` token.
fn list_pages(ctx: &WatchCtx) -> ListPagesOutcome {
    let mut objects = Vec::new();
    let mut cont: Option<String> = None;

    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return ListPagesOutcome::Err("stop requested".to_string());
        }
        match list_page(ctx, cont.as_deref()) {
            PageOutcome::Page {
                objects: page_objects,
                resource_version,
                next_continue,
            } => {
                objects.extend(page_objects);
                match next_continue {
                    Some(c) => cont = Some(c),
                    // Only the final page's resourceVersion matters once
                    // pagination completes — matches upstream.
                    None => return ListPagesOutcome::Ok(objects, resource_version),
                }
            }
            PageOutcome::Gone => return ListPagesOutcome::Gone,
            PageOutcome::Err(msg) => return ListPagesOutcome::Err(msg),
        }
    }
}

enum PageOutcome {
    Page {
        objects: Vec<K8sObject>,
        resource_version: String,
        next_continue: Option<String>,
    },
    Gone,
    Err(String),
}

/// Fetches and parses a single LIST page (`cont` is the `continue` token for
/// pages after the first).
fn list_page(ctx: &WatchCtx, cont: Option<&str>) -> PageOutcome {
    let resp = get_with_v1beta1_fallback(ctx, LIST_HTTP_TIMEOUT, |use_v1beta1| {
        let url = ctx
            .api
            .list_url(&ctx.role, ctx.namespace.as_deref(), &ctx.selectors, cont);
        apply_v1beta1_fallback(&url, &ctx.role, use_v1beta1)
    });
    let mut resp = match resp {
        Ok(r) => r,
        Err(e) => return PageOutcome::Err(e),
    };

    let status = resp.status().as_u16();
    if status == 410 {
        return PageOutcome::Gone;
    }

    let mut buf = Vec::new();
    if let Err(e) = resp.read_to_end(&mut buf) {
        return PageOutcome::Err(format!("reading list response: {e}"));
    }
    if !(200..300).contains(&status) {
        return PageOutcome::Err(format!("list returned status {status}"));
    }

    let next_continue = extract_continue_token(&buf);
    match object::parse_list(&ctx.role, &buf) {
        Ok((objects, resource_version)) => PageOutcome::Page {
            objects,
            resource_version,
            next_continue,
        },
        Err(e) => PageOutcome::Err(format!("parsing list response: {e}")),
    }
}

/// Pulls `metadata.continue` out of a raw LIST response body. `object::parse_list`
/// only returns `resourceVersion` (see its doc), so pagination reads this
/// separately from the same bytes rather than requiring a foundation change.
fn extract_continue_token(buf: &[u8]) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(buf).ok()?;
    v.get("metadata")?
        .get("continue")?
        .as_str()
        .map(|s| s.to_string())
}

/// Outcome of one [`watch_once`] call.
enum WatchEnd {
    /// `stop` was observed while the stream was open.
    Stopped,
    /// The resourceVersion is no longer valid — an HTTP `410 Gone` on the
    /// watch request, or an in-band `ERROR` watch event carrying code 410.
    /// The caller must re-LIST before watching again. `had_events` is `true`
    /// if this watch applied at least one `ADDED`/`MODIFIED`/`DELETED`/
    /// `BOOKMARK` event before going stale — used by [`run`]'s
    /// `consecutive_gone` hot-loop guard to tell a healthy-then-stale watch
    /// apart from an immediate, event-free `410`.
    Gone { had_events: bool },
    /// The server closed the stream cleanly after `timeoutSeconds` (a normal
    /// end-of-long-poll, `read_line` returning `Ok(0)`). Carries the latest
    /// tracked resourceVersion so the caller can resume the watch from it
    /// without re-LISTing.
    Closed(String),
    /// The stream ended on a transient error: a read/transport error, a
    /// non-2xx/410 status, or a non-410 `ERROR` watch event. Carries the
    /// latest tracked resourceVersion; the caller re-watches from it after a
    /// backoff (a stale rv then surfaces as a `410`).
    Errored(String),
}

/// What an in-band `ERROR` watch event tells the stream loop to do (upstream
/// `readObjectUpdateStream` parses the event object as a `Status{code}`).
enum ErrorEvent {
    /// Code 410: the resourceVersion has expired — end the watch and re-LIST.
    Gone,
    /// Any other code: end the watch and re-watch from the same rv.
    Errored,
}

/// Issues one WATCH request starting at `resource_version` and streams its
/// body line-by-line until it ends, applying each event to the cache. Never
/// panics: a malformed line or object is logged and skipped, not fatal to
/// the stream.
fn watch_once(ctx: &WatchCtx, resource_version: &str) -> WatchEnd {
    let resp = get_with_v1beta1_fallback(ctx, WATCH_HTTP_TIMEOUT, |use_v1beta1| {
        let url = ctx.api.watch_url(
            &ctx.role,
            ctx.namespace.as_deref(),
            &ctx.selectors,
            resource_version,
            WATCH_TIMEOUT_SECS,
        );
        apply_v1beta1_fallback(&url, &ctx.role, use_v1beta1)
    });
    let resp = match resp {
        Ok(r) => r,
        Err(e) => {
            log::warn!(
                "esmagent k8s watcher ({}): watch request failed: {e}",
                ctx.role
            );
            return WatchEnd::Errored(resource_version.to_string());
        }
    };

    let status = resp.status().as_u16();
    if status == 410 {
        // No body was ever read on this connection, so no event could have
        // been applied.
        return WatchEnd::Gone { had_events: false };
    }
    if !(200..300).contains(&status) {
        log::warn!(
            "esmagent k8s watcher ({}): watch returned status {status}",
            ctx.role
        );
        return WatchEnd::Errored(resource_version.to_string());
    }

    let mut latest_rv = resource_version.to_string();
    let mut had_events = false;
    let mut reader = BufReader::new(resp);
    let mut line = String::new();
    loop {
        if ctx.stop.load(Ordering::SeqCst) {
            return WatchEnd::Stopped;
        }
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => {
                log::debug!(
                    "esmagent k8s watcher ({}): watch stream closed (last resourceVersion {latest_rv})",
                    ctx.role
                );
                return WatchEnd::Closed(latest_rv);
            }
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                // An in-band `ERROR` event ends the stream (upstream
                // `readObjectUpdateStream`): 410 -> re-LIST, else re-watch.
                if let Some(err) = apply_watch_line(ctx, trimmed, &mut latest_rv, &mut had_events) {
                    return match err {
                        ErrorEvent::Gone => WatchEnd::Gone { had_events },
                        ErrorEvent::Errored => WatchEnd::Errored(latest_rv),
                    };
                }
            }
            Err(e) => {
                log::warn!(
                    "esmagent k8s watcher ({}): watch read error: {e} (last resourceVersion {latest_rv})",
                    ctx.role
                );
                return WatchEnd::Errored(latest_rv);
            }
        }
    }
}

/// Parses and applies one watch-stream line (one [`WatchEvent`] JSON object).
/// Updates `latest_rv` from the event object's `metadata.resourceVersion`
/// when present (this includes `BOOKMARK` events, whose only purpose is to
/// advance this tracker). A malformed line is logged and skipped. Sets
/// `*had_events = true` for any applied `ADDED`/`MODIFIED`/`DELETED`/
/// `BOOKMARK` event (used by [`run`]'s `consecutive_gone` hot-loop guard —
/// see [`WatchEnd::Gone`]'s doc). Returns `Some(ErrorEvent)` for an
/// `ERROR`-type event, which ends the stream; `None` for every
/// applied/ignored event, meaning "keep reading".
fn apply_watch_line(
    ctx: &WatchCtx,
    line: &str,
    latest_rv: &mut String,
    had_events: &mut bool,
) -> Option<ErrorEvent> {
    let event: WatchEvent = match serde_json::from_str(line) {
        Ok(e) => e,
        Err(e) => {
            log::warn!(
                "esmagent k8s watcher ({}): skipping unparseable watch line: {e}",
                ctx.role
            );
            return None;
        }
    };

    if let Some(rv) = object_resource_version(&event.object) {
        *latest_rv = rv;
    }

    match event.event_type.as_str() {
        "ADDED" | "MODIFIED" => {
            *had_events = true;
            apply_upsert(ctx, &event.object);
        }
        "DELETED" => {
            *had_events = true;
            apply_delete(ctx, &event.object);
        }
        "BOOKMARK" => {
            // resourceVersion already advanced above; nothing else to apply
            // for a bookmark (matches upstream: it carries no object data).
            *had_events = true;
        }
        "ERROR" => {
            // The event object is a `Status{code}`. Code 410 is k8s's
            // documented in-band "resourceVersion too old" signal: end the
            // watch and re-LIST. Any other error code ends the watch too, to
            // be re-watched from the same rv after a backoff.
            let code = status_code(&event.object);
            if code == Some(410) {
                log::info!(
                    "esmagent k8s watcher ({}): watch ERROR event (410 expired); re-listing",
                    ctx.role
                );
                return Some(ErrorEvent::Gone);
            }
            log::warn!(
                "esmagent k8s watcher ({}): watch ERROR event (code {code:?}); re-watching",
                ctx.role
            );
            return Some(ErrorEvent::Errored);
        }
        other => {
            log::debug!(
                "esmagent k8s watcher ({}): ignoring watch event type {other:?}",
                ctx.role
            );
        }
    }
    None
}

/// `code` field of a k8s `Status` object (an `ERROR` watch event's payload).
fn status_code(value: &serde_json::Value) -> Option<u64> {
    value.get("code")?.as_u64()
}

/// Parses `value` as this watcher's role type and upserts it into the cache
/// by key. Logs and skips (does not abort the stream) on a parse failure.
fn apply_upsert(ctx: &WatchCtx, value: &serde_json::Value) {
    let bytes = match serde_json::to_vec(value) {
        Ok(b) => b,
        Err(e) => {
            log::warn!(
                "esmagent k8s watcher ({}): failed to re-encode watch object: {e}",
                ctx.role
            );
            return;
        }
    };
    match object::parse_object(&ctx.role, &bytes) {
        Ok(obj) => {
            let key = obj.key();
            ctx.cache.lock().unwrap().insert(key, Arc::new(obj));
        }
        Err(e) => {
            log::warn!(
                "esmagent k8s watcher ({}): skipping unparseable {} object: {e}",
                ctx.role,
                ctx.role
            );
        }
    }
}

/// Removes the object named by `value`'s `metadata.namespace`/`name` from
/// the cache. Computed straight from the raw JSON rather than going through
/// [`object::parse_object`], so a delete doesn't depend on the rest of the
/// object (which a `DELETED` event may omit) parsing cleanly.
fn apply_delete(ctx: &WatchCtx, value: &serde_json::Value) {
    let key = object_key(value);
    ctx.cache.lock().unwrap().remove(&key);
}

/// `<namespace>/<name>` for a raw watch-event object, matching
/// `ObjectMeta::key`'s format. Missing fields default to empty strings.
fn object_key(value: &serde_json::Value) -> String {
    let namespace = value
        .get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let name = value
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    format!("{namespace}/{name}")
}

/// `metadata.resourceVersion` for a raw watch-event object, if present.
fn object_resource_version(value: &serde_json::Value) -> Option<String> {
    value
        .get("metadata")?
        .get("resourceVersion")?
        .as_str()
        .map(|s| s.to_string())
}

/// Issues a GET built by `build_url(use_v1beta1)`, using this watcher's
/// current sticky v1beta1-fallback flag. If `ctx.role` is `"ingress"` or
/// `"endpointslice"`, the flag isn't already set, and the response is `404`,
/// flips the flag and retries once against `build_url(true)` — see the module
/// doc.
fn get_with_v1beta1_fallback(
    ctx: &WatchCtx,
    timeout: Duration,
    build_url: impl Fn(bool) -> String,
) -> Result<Response, String> {
    let use_v1beta1 = ctx.use_v1beta1.load(Ordering::SeqCst);
    let url = build_url(use_v1beta1);
    let resp = ctx.api.get(&url, timeout).map_err(|e| e.msg)?;

    if matches!(ctx.role.as_str(), "ingress" | "endpointslice")
        && !use_v1beta1
        && resp.status().as_u16() == 404
    {
        ctx.use_v1beta1.store(true, Ordering::SeqCst);
        let retry_url = build_url(true);
        return ctx.api.get(&retry_url, timeout).map_err(|e| e.msg);
    }

    Ok(resp)
}

/// Rewrites the API group version to `v1beta1` in `url` when `use_v1beta1`
/// is set: `ingress` rewrites `networking.k8s.io/v1`, `endpointslice`
/// rewrites `discovery.k8s.io/v1` (upstream `useNetworkingV1Beta1` /
/// `useDiscoveryV1Beta1`). Plain string replace on the already-built URL.
fn apply_v1beta1_fallback(url: &str, role: &str, use_v1beta1: bool) -> String {
    let group = match (use_v1beta1, role) {
        (true, "ingress") => "networking.k8s.io",
        (true, "endpointslice") => "discovery.k8s.io",
        _ => return url.to_string(),
    };
    url.replacen(&format!("/{group}/v1/"), &format!("/{group}/v1beta1/"), 1)
}

#[cfg(test)]
#[path = "watcher_tests.rs"]
mod tests;
