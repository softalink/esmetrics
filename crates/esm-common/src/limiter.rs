//! Generic concurrency limiter (Condvar + counter + queue timeout).
//!
//! Lifted from esm-insert's `lib/writeconcurrencylimiter` port so both
//! esm-insert (ingestion backpressure) and esmauth (per-user/global request
//! limiting) can share one implementation without esmauth depending on
//! esm-insert. At most `max_concurrent` permits are held at once; an
//! [`Limiter::acquire`] that cannot get a permit immediately waits up to
//! `queue_duration` for one to free up and then fails with [`LimitExceeded`].
//!
//! Deviation from Go (carried over from the esm-insert port): the Go limiter
//! releases its token around every blocking `Read` so slow clients don't hold
//! slots; here a [`Permit`] is held for the whole caller-defined critical
//! section and released on drop.

use std::fmt;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// Returned when no permit became free within the queue duration. Carries the
/// limiter's parameters so callers can format a domain-specific message (and
/// map to their own status code, e.g. 503 for inserts, 429 for esmauth).
#[derive(Debug, Clone, Copy)]
pub struct LimitExceeded {
    /// The `max_concurrent` the limiter was built with (after the `>= 1`
    /// clamp applied by [`Limiter::new`]).
    pub max_concurrent: usize,
    /// The `queue_duration` the acquire waited before giving up.
    pub queue_duration: Duration,
}

impl fmt::Display for LimitExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cannot obtain a concurrency permit within {:.3} seconds because {} concurrent \
             requests are already being processed",
            self.queue_duration.as_secs_f64(),
            self.max_concurrent
        )
    }
}

impl std::error::Error for LimitExceeded {}

/// A semaphore-with-timeout capping the number of concurrently held permits.
pub struct Limiter {
    current: Mutex<usize>,
    released: Condvar,
    max_concurrent: usize,
    queue_duration: Duration,
}

impl Limiter {
    /// Builds a limiter allowing at most `max_concurrent` permits (clamped to
    /// at least 1), where a blocked [`acquire`](Limiter::acquire) waits up to
    /// `queue_duration` for a permit before failing.
    pub fn new(max_concurrent: usize, queue_duration: Duration) -> Limiter {
        Limiter {
            current: Mutex::new(0),
            released: Condvar::new(),
            max_concurrent: max_concurrent.max(1),
            queue_duration,
        }
    }

    /// The effective concurrency cap (after the `>= 1` clamp).
    pub fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }

    /// The configured queue-wait duration.
    pub fn queue_duration(&self) -> Duration {
        self.queue_duration
    }

    /// Obtains a permit, waiting up to `queue_duration` for one to free up.
    /// The returned [`Permit`] releases its slot when dropped.
    pub fn acquire(&self) -> Result<Permit<'_>, LimitExceeded> {
        let deadline = Instant::now() + self.queue_duration;
        let mut current = self.current.lock().unwrap();
        while *current >= self.max_concurrent {
            let now = Instant::now();
            if now >= deadline {
                return Err(LimitExceeded {
                    max_concurrent: self.max_concurrent,
                    queue_duration: self.queue_duration,
                });
            }
            let (guard, _) = self.released.wait_timeout(current, deadline - now).unwrap();
            current = guard;
        }
        *current += 1;
        Ok(Permit { limiter: self })
    }
}

/// A held concurrency slot returned by [`Limiter::acquire`]; releases the slot
/// on drop.
pub struct Permit<'a> {
    limiter: &'a Limiter,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        let mut current = self.limiter.current.lock().unwrap();
        *current -= 1;
        drop(current);
        self.limiter.released.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn acquires_up_to_cap_then_times_out() {
        let limiter = Limiter::new(2, Duration::from_millis(20));
        let g1 = limiter.acquire().unwrap();
        let g2 = limiter.acquire().unwrap();
        let err = match limiter.acquire() {
            Ok(_) => panic!("expected a queue-timeout error"),
            Err(err) => err,
        };
        assert_eq!(err.max_concurrent, 2);
        drop(g1);
        let g3 = limiter.acquire().unwrap();
        drop(g2);
        drop(g3);
    }

    #[test]
    fn waiter_gets_slot_when_released() {
        let limiter = Arc::new(Limiter::new(1, Duration::from_secs(10)));
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

    #[test]
    fn max_concurrent_is_clamped_to_at_least_one() {
        let limiter = Limiter::new(0, Duration::from_millis(1));
        assert_eq!(limiter.max_concurrent(), 1);
        let _g = limiter.acquire().unwrap();
        assert!(limiter.acquire().is_err());
    }
}
