//! Concurrent-request limiter. Port of the `concurrencyLimitCh` semaphore
//! in `app/vmselect/main.go`: non-blocking acquire, then a bounded wait
//! (`min(query timeout, -search.maxQueueDuration)`) before giving up.

use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

pub(crate) struct ConcurrencyLimiter {
    capacity: usize,
    used: Mutex<usize>,
    freed: Condvar,
}

impl ConcurrencyLimiter {
    pub(crate) fn new(capacity: usize) -> ConcurrencyLimiter {
        ConcurrencyLimiter {
            capacity: capacity.max(1),
            used: Mutex::new(0),
            freed: Condvar::new(),
        }
    }

    /// Tries to take a slot, waiting up to `max_wait`. Returns a guard that
    /// releases the slot on drop, or `None` on timeout.
    pub(crate) fn acquire(&self, max_wait: Duration) -> Option<LimiterGuard<'_>> {
        let mut used = self.used.lock().expect("limiter mutex poisoned");
        if *used < self.capacity {
            *used += 1;
            return Some(LimiterGuard { limiter: self });
        }
        let deadline = Instant::now() + max_wait;
        loop {
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            let (guard, _) = self
                .freed
                .wait_timeout(used, deadline - now)
                .expect("limiter mutex poisoned");
            used = guard;
            if *used < self.capacity {
                *used += 1;
                return Some(LimiterGuard { limiter: self });
            }
        }
    }
}

pub(crate) struct LimiterGuard<'a> {
    limiter: &'a ConcurrencyLimiter,
}

impl Drop for LimiterGuard<'_> {
    fn drop(&mut self) {
        let mut used = self.limiter.used.lock().expect("limiter mutex poisoned");
        *used -= 1;
        drop(used);
        self.limiter.freed.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_and_timeout() {
        let limiter = ConcurrencyLimiter::new(1);
        let g1 = limiter.acquire(Duration::from_millis(1)).unwrap();
        assert!(limiter.acquire(Duration::from_millis(30)).is_none());
        drop(g1);
        assert!(limiter.acquire(Duration::from_millis(1)).is_some());
    }

    #[test]
    fn waiting_acquire_succeeds_when_freed() {
        use std::sync::Arc;
        let limiter = Arc::new(ConcurrencyLimiter::new(1));
        let g1 = limiter.acquire(Duration::from_millis(1)).unwrap();
        let l2 = Arc::clone(&limiter);
        let handle = std::thread::spawn(move || l2.acquire(Duration::from_secs(5)).is_some());
        std::thread::sleep(Duration::from_millis(50));
        drop(g1);
        assert!(handle.join().unwrap());
    }
}
