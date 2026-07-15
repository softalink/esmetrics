//! Load balancing and backend health across a `url_prefix`'s backend URLs.
//!
//! Port of the `backendURL`/`URLPrefix.getBackendURL` machinery
//! (`app/vmauth/auth_config.go:316-693`), minus DNS-based backend discovery
//! (`discoverBackendAddrsIfNeeded`) and minus the TCP-dial health-check
//! goroutine (`backendURL.setBroken`/`runHealthCheck`).
//!
//! **Deviation from upstream (deliberate):** upstream marks a backend broken
//! with an atomic bool and spawns a goroutine that TCP-dials the backend
//! every `fail_timeout` until it connects, then clears the bool. This port
//! instead records a "broken until" `Instant` in [`Backend::set_broken`] and
//! lazily treats the backend as healthy again once `now >= broken_until`,
//! checked in [`Backend::is_broken`] on each selection. This is behaviorally
//! equivalent for proxy routing (a backend that's actually still down will
//! immediately fail again and get re-marked broken) and avoids spawning a
//! background task per backend. `is_broken`/`select` take an explicit `now:
//! Instant` rather than calling `Instant::now()` internally so tests can
//! inject a controllable clock.

use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::LoadBalancingPolicy;

/// One backend: its URL string, an in-flight request counter, and a "broken
/// until" instant. Port of `backendURL` (auth_config.go:417-427).
pub struct Backend {
    url: String,
    in_flight: AtomicI32,
    broken_until: Mutex<Option<Instant>>,
}

impl Backend {
    fn new(url: String) -> Self {
        Backend {
            url,
            in_flight: AtomicI32::new(0),
            broken_until: Mutex::new(None),
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Port of `backendURL.isBroken` (auth_config.go:429-431), adapted for
    /// the lazy-recovery deviation: broken until `now >= broken_until`.
    pub fn is_broken(&self, now: Instant) -> bool {
        match *self.broken_until.lock().unwrap() {
            Some(until) => now < until,
            None => false,
        }
    }

    /// Marks this backend broken until `until`. Port of `backendURL.setBroken`
    /// (auth_config.go:433-440), minus the TCP-dial health-check goroutine.
    pub fn set_broken(&self, until: Instant) {
        *self.broken_until.lock().unwrap() = Some(until);
    }

    /// Port of `backendURL.get` (auth_config.go:476-478).
    pub fn acquire(&self) {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
    }

    /// Port of `backendURL.put` (auth_config.go:480-482).
    pub fn release(&self) {
        self.in_flight.fetch_add(-1, Ordering::SeqCst);
    }

    fn in_flight(&self) -> i32 {
        self.in_flight.load(Ordering::SeqCst)
    }
}

/// A pool of backends for one `url_prefix`, with a round-robin counter. Port
/// of the parts of `URLPrefix` (auth_config.go:316-350) needed for backend
/// selection: `busOriginal`/`bus` become `backends`, `n` is the round-robin
/// counter, and `loadBalancingPolicy` is `policy`.
pub struct BackendPool {
    backends: Vec<Arc<Backend>>,
    n: AtomicU32,
    policy: LoadBalancingPolicy,
    fail_timeout: Duration,
}

impl BackendPool {
    pub fn new(
        urls: &[String],
        policy: LoadBalancingPolicy,
        fail_timeout: Duration,
    ) -> BackendPool {
        BackendPool {
            backends: urls
                .iter()
                .map(|u| Arc::new(Backend::new(u.clone())))
                .collect(),
            n: AtomicU32::new(0),
            policy,
            fail_timeout,
        }
    }

    pub fn len(&self) -> usize {
        self.backends.len()
    }

    pub fn is_empty(&self) -> bool {
        self.backends.is_empty()
    }

    pub fn fail_timeout(&self) -> Duration {
        self.fail_timeout
    }

    /// Select a backend per policy; returns an `Arc` whose `acquire()` was
    /// already called (caller MUST `release()`). `None` only if
    /// `len() == 0`. Port of `URLPrefix.getBackendURL` (auth_config.go:494-506),
    /// minus `discoverBackendAddrsIfNeeded`.
    pub fn select(&self, now: Instant) -> Option<Arc<Backend>> {
        if self.backends.is_empty() {
            return None;
        }

        Some(match self.policy {
            LoadBalancingPolicy::FirstAvailable => get_first_available_backend(&self.backends, now),
            LoadBalancingPolicy::LeastLoaded => {
                get_least_loaded_backend(&self.backends, &self.n, now)
            }
        })
    }
}

/// Port of `getFirstAvailableBackendURL` (auth_config.go:616-637): the first
/// non-broken backend, else the first non-broken among the rest, else the
/// first backend (all broken).
fn get_first_available_backend(backends: &[Arc<Backend>], now: Instant) -> Arc<Backend> {
    let first = &backends[0];
    if !first.is_broken(now) {
        // Fast path - send the request to the first url.
        first.acquire();
        return Arc::clone(first);
    }

    // Slow path - the first url is temporarily unavailable. Fall back to the
    // remaining urls.
    for backend in &backends[1..] {
        if !backend.is_broken(now) {
            backend.acquire();
            return Arc::clone(backend);
        }
    }

    // All backend urls are unavailable, then returning a first one, it could
    // help increase the success rate of the requests.
    first.acquire();
    Arc::clone(first)
}

/// Port of `getLeastLoadedBackendURL` (auth_config.go:643-693): single-backend
/// fast path; else a CAS-0 fast path that finds an idle non-broken backend
/// without double-incrementing in_flight; else the min-in_flight non-broken
/// backend; else the first backend (all broken).
fn get_least_loaded_backend(
    backends: &[Arc<Backend>],
    counter: &AtomicU32,
    now: Instant,
) -> Arc<Backend> {
    let first = &backends[0];
    let len = backends.len() as u32;
    if len == 1 {
        first.acquire();
        return Arc::clone(first);
    }

    // Fast path - select other backend urls.
    let n = counter.fetch_add(1, Ordering::SeqCst);
    for i in 0..len {
        let idx = ((n + i) % len) as usize;
        let backend = &backends[idx];
        if backend.is_broken(now) {
            continue;
        }

        // The Load() in front of CompareAndSwap() avoids CAS overhead for
        // items with values bigger than 0.
        if backend.in_flight() == 0
            && backend
                .in_flight
                .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            let _ =
                counter.compare_exchange(n + 1, idx as u32 + 1, Ordering::SeqCst, Ordering::SeqCst);
            // There is no need to call backend.acquire(), because we already
            // incremented in_flight above.
            return Arc::clone(backend);
        }
    }

    // Slow path - return the backend with the minimum number of concurrently
    // executed requests.
    let mut min_idx = (n % len) as usize;
    let mut min_requests = backends[min_idx].in_flight();
    for i in 1..len {
        let idx = ((n + i) % len) as usize;
        let backend = &backends[idx];
        if backend.is_broken(now) {
            continue;
        }

        let reqs = backend.in_flight();
        if reqs < min_requests || backends[min_idx].is_broken(now) {
            min_idx = idx;
            min_requests = reqs;
        }
    }
    let min_backend = &backends[min_idx];
    if min_backend.is_broken(now) {
        // If all backendURLs are broken, then returns the first backendURL.
        first.acquire();
        return Arc::clone(first);
    }
    min_backend.acquire();
    let _ = counter.compare_exchange(
        n + 1,
        min_idx as u32 + 1,
        Ordering::SeqCst,
        Ordering::SeqCst,
    );
    Arc::clone(min_backend)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn urls(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("http://backend-{i}")).collect()
    }

    #[test]
    fn least_loaded_prefers_zero_inflight() {
        let pool = BackendPool::new(
            &urls(2),
            LoadBalancingPolicy::LeastLoaded,
            Duration::from_secs(1),
        );
        let now = Instant::now();

        // Make backend 0 busy.
        let busy = pool.select(now).unwrap();
        // Whichever backend the fast path picked is now "busy"; grab the
        // other one directly to control the scenario precisely instead of
        // relying on which one select() happened to choose.
        let busy_idx = pool
            .backends
            .iter()
            .position(|b| Arc::ptr_eq(b, &busy))
            .unwrap();
        let idle_idx = 1 - busy_idx;

        let selected = pool.select(now).unwrap();
        let selected_idx = pool
            .backends
            .iter()
            .position(|b| Arc::ptr_eq(b, &selected))
            .unwrap();
        assert_eq!(
            selected_idx, idle_idx,
            "expected the idle backend to be selected"
        );

        busy.release();
        selected.release();
    }

    #[test]
    fn least_loaded_single_backend_always_selected() {
        let pool = BackendPool::new(
            &urls(1),
            LoadBalancingPolicy::LeastLoaded,
            Duration::from_secs(1),
        );
        let now = Instant::now();

        let a = pool.select(now).unwrap();
        assert_eq!(a.url(), "http://backend-0");
        a.release();

        let b = pool.select(now).unwrap();
        assert_eq!(b.url(), "http://backend-0");
        b.release();
    }

    #[test]
    fn first_available_returns_first_when_healthy() {
        let pool = BackendPool::new(
            &urls(3),
            LoadBalancingPolicy::FirstAvailable,
            Duration::from_secs(1),
        );
        let now = Instant::now();

        let selected = pool.select(now).unwrap();
        assert_eq!(selected.url(), "http://backend-0");
        selected.release();
    }

    #[test]
    fn first_available_skips_broken() {
        let pool = BackendPool::new(
            &urls(3),
            LoadBalancingPolicy::FirstAvailable,
            Duration::from_secs(1),
        );
        let now = Instant::now();
        pool.backends[0].set_broken(now + Duration::from_secs(5));

        let selected = pool.select(now).unwrap();
        assert_eq!(selected.url(), "http://backend-1");
        selected.release();
    }

    #[test]
    fn broken_backend_recovers_after_fail_timeout() {
        let backend = Backend::new("http://backend-0".to_string());
        let now = Instant::now();
        let fail_timeout = Duration::from_secs(10);

        backend.set_broken(now + fail_timeout);
        assert!(backend.is_broken(now));
        assert!(backend.is_broken(now + Duration::from_secs(5)));

        let later = now + fail_timeout;
        assert!(!backend.is_broken(later));

        let much_later = now + fail_timeout + Duration::from_secs(1);
        assert!(!backend.is_broken(much_later));
    }

    #[test]
    fn all_broken_falls_back_to_first() {
        let pool = BackendPool::new(
            &urls(3),
            LoadBalancingPolicy::FirstAvailable,
            Duration::from_secs(1),
        );
        let now = Instant::now();
        for backend in &pool.backends {
            backend.set_broken(now + Duration::from_secs(5));
        }

        let selected = pool.select(now).unwrap();
        assert_eq!(selected.url(), "http://backend-0");
        selected.release();

        // Same for least_loaded.
        let pool2 = BackendPool::new(
            &urls(3),
            LoadBalancingPolicy::LeastLoaded,
            Duration::from_secs(1),
        );
        for backend in &pool2.backends {
            backend.set_broken(now + Duration::from_secs(5));
        }
        let selected2 = pool2.select(now).unwrap();
        assert_eq!(selected2.url(), "http://backend-0");
        selected2.release();
    }

    #[test]
    fn select_returns_none_for_empty_pool() {
        let pool = BackendPool::new(
            &[],
            LoadBalancingPolicy::LeastLoaded,
            Duration::from_secs(1),
        );
        assert!(pool.select(Instant::now()).is_none());
    }
}
