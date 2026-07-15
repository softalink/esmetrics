//! Insert concurrency limiter.
//!
//! Port of the upstream VictoriaMetrics v1.146.0 `lib/writeconcurrencylimiter`:
//! at most `-maxConcurrentInserts` (default 2 x available CPUs) insert
//! requests are processed concurrently; excess requests wait up to
//! `-insert.maxQueueDuration` (default 1 minute) for a slot and then fail
//! with HTTP 503.
//!
//! The Condvar+counter+queue-timeout mechanism now lives in
//! [`esm_common::limiter`] so it can be shared with esmauth; this module is a
//! thin insert-specific wrapper that keeps the upstream default (2 x CPUs / 1
//! minute) and the insert-flavored error message (mapped to HTTP 503).
//!
//! Deviation from Go: the Go limiter wraps the request-body reader and
//! releases its token around every blocking `Read` so that slow clients do
//! not hold processing slots. This port holds the token for the whole
//! request; TSBS-style clients stream bodies fast enough that the
//! distinction does not matter for the load path.

use std::fmt;
use std::time::Duration;

use esm_common::limiter::{Limiter, Permit};

/// Go: `-insert.maxQueueDuration` default.
const DEFAULT_MAX_QUEUE_DURATION: Duration = Duration::from_secs(60);

/// Returned when no concurrency slot became free within the queue duration.
/// Maps to HTTP 503, like Go `httpserver.ErrorWithStatusCode`.
#[derive(Debug)]
pub struct LimitExceededError {
    max_queue_duration: Duration,
    max_concurrent_inserts: usize,
}

impl fmt::Display for LimitExceededError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cannot process insert request for {:.3} seconds because {} concurrent insert \
             requests are executed. Possible solutions: to reduce workload; to increase compute \
             resources at the server; to increase -insert.maxQueueDuration; to increase \
             -maxConcurrentInserts",
            self.max_queue_duration.as_secs_f64(),
            self.max_concurrent_inserts
        )
    }
}

impl std::error::Error for LimitExceededError {}

/// Semaphore-with-timeout capping concurrent insert requests. Wraps the shared
/// [`esm_common::limiter::Limiter`].
pub struct ConcurrencyLimiter {
    inner: Limiter,
}

impl Default for ConcurrencyLimiter {
    fn default() -> ConcurrencyLimiter {
        ConcurrencyLimiter::new(2 * available_cpus(), DEFAULT_MAX_QUEUE_DURATION)
    }
}

impl ConcurrencyLimiter {
    pub fn new(max_concurrent_inserts: usize, max_queue_duration: Duration) -> ConcurrencyLimiter {
        ConcurrencyLimiter {
            inner: Limiter::new(max_concurrent_inserts, max_queue_duration),
        }
    }

    /// Obtains a concurrency token, waiting up to the queue duration.
    /// Go: `IncConcurrency` (the guard drop is `DecConcurrency`).
    pub fn acquire(&self) -> Result<Permit<'_>, LimitExceededError> {
        self.inner.acquire().map_err(|err| LimitExceededError {
            max_queue_duration: err.queue_duration,
            max_concurrent_inserts: err.max_concurrent,
        })
    }
}

/// Go: `cgroup.AvailableCPUs`. `available_parallelism` is cgroup-aware on
/// Linux.
fn available_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn acquires_up_to_cap_then_times_out() {
        let limiter = ConcurrencyLimiter::new(2, Duration::from_millis(20));
        let g1 = limiter.acquire().unwrap();
        let g2 = limiter.acquire().unwrap();
        let err = match limiter.acquire() {
            Ok(_) => panic!("expected 503-style error"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("2 concurrent insert requests"));
        drop(g1);
        let g3 = limiter.acquire().unwrap();
        drop(g2);
        drop(g3);
    }

    #[test]
    fn waiter_gets_slot_when_released() {
        let limiter = Arc::new(ConcurrencyLimiter::new(1, Duration::from_secs(10)));
        let guard = limiter.acquire().unwrap();
        let acquired = Arc::new(AtomicUsize::new(0));

        let limiter2 = Arc::clone(&limiter);
        let acquired2 = Arc::clone(&acquired);
        let handle = std::thread::spawn(move || {
            let _g = limiter2.acquire().unwrap();
            acquired2.store(1, Ordering::SeqCst);
        });

        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(acquired.load(Ordering::SeqCst), 0, "waiter ran too early");
        drop(guard);
        handle.join().unwrap();
        assert_eq!(acquired.load(Ordering::SeqCst), 1);
    }
}
