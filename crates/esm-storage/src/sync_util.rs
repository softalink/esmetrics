//! Small concurrency helpers shared by the stage-4/5 modules (partition,
//! table, storage). These mirror the idioms used by esm-mergeset's
//! `util.rs` (translations of Go's `chan struct{}` semaphores, closed
//! `stopCh` channels and `sync.WaitGroup`).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::{Condvar, Mutex};

/// Returns the number of available CPU cores (upstream `cgroup.AvailableCPUs`).
pub(crate) fn available_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// The current unix time in milliseconds (Go `time.Now().UnixMilli()`).
pub(crate) fn now_unix_milli() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Adds up to 25% random jitter to `d` (upstream `timeutil.AddJitterToDuration`).
pub(crate) fn add_jitter_to_duration(d: Duration) -> Duration {
    static STATE: AtomicU64 = AtomicU64::new(0x9e3779b97f4a7c15);
    let x = STATE
        .fetch_add(0x9e3779b97f4a7c15, Ordering::Relaxed)
        .wrapping_mul(0xbf58476d1ce4e5b9);
    let p = (x >> 40) as f64 / (1u64 << 24) as f64; // [0, 1)
    d + Duration::from_secs_f64(d.as_secs_f64() * 0.25 * p)
}

/// Counting semaphore (translation of Go's `chan struct{}` semaphores).
pub(crate) struct Sema {
    permits: Mutex<usize>,
    cv: Condvar,
    cap: usize,
}

impl Sema {
    pub fn new(permits: usize) -> Sema {
        Sema {
            permits: Mutex::new(permits),
            cv: Condvar::new(),
            cap: permits,
        }
    }

    /// The total number of permits (Go `cap(ch)`).
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Blocks until a permit is available.
    pub fn acquire(&self) {
        let mut n = self.permits.lock();
        while *n == 0 {
            self.cv.wait(&mut n);
        }
        *n -= 1;
    }

    /// Blocks until a permit is available or `shutdown` is signaled.
    /// Returns true if the permit was acquired.
    pub fn acquire_or_stop(&self, shutdown: &Shutdown) -> bool {
        const POLL_INTERVAL: Duration = Duration::from_millis(20);
        let mut n = self.permits.lock();
        loop {
            if *n > 0 {
                *n -= 1;
                return true;
            }
            if shutdown.is_stopped() {
                return false;
            }
            // Bounded wait, so shutdown signals are noticed promptly.
            self.cv.wait_for(&mut n, POLL_INTERVAL);
        }
    }

    pub fn release(&self) {
        let mut n = self.permits.lock();
        *n += 1;
        self.cv.notify_one();
    }
}

/// Shutdown signal shared by the background workers of a partition/table/
/// storage (translation of Go's closed `stopCh`).
pub(crate) struct Shutdown {
    stopped: AtomicBool,
    m: Mutex<()>,
    cv: Condvar,
}

impl Shutdown {
    pub fn new() -> Shutdown {
        Shutdown {
            stopped: AtomicBool::new(false),
            m: Mutex::new(()),
            cv: Condvar::new(),
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// The raw stop flag, for passing to `merge_block_streams`.
    pub fn flag(&self) -> &AtomicBool {
        &self.stopped
    }

    pub fn signal(&self) {
        self.stopped.store(true, Ordering::Release);
        let _guard = self.m.lock();
        self.cv.notify_all();
    }

    /// Waits for up to `timeout`. Returns true if shutdown was signaled.
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let mut guard = self.m.lock();
        if self.is_stopped() {
            return true;
        }
        self.cv.wait_for(&mut guard, timeout);
        self.is_stopped()
    }
}

/// Wait group usable concurrently from multiple threads
/// (translation of Go `sync.WaitGroup`).
pub(crate) struct WaitCounter {
    count: Mutex<usize>,
    cv: Condvar,
}

impl WaitCounter {
    pub fn new() -> WaitCounter {
        WaitCounter {
            count: Mutex::new(0),
            cv: Condvar::new(),
        }
    }

    pub fn add(&self) {
        *self.count.lock() += 1;
    }

    pub fn done(&self) {
        let mut n = self.count.lock();
        assert!(*n > 0, "BUG: WaitCounter.done called more times than add");
        *n -= 1;
        if *n == 0 {
            self.cv.notify_all();
        }
    }

    pub fn wait(&self) {
        let mut n = self.count.lock();
        while *n > 0 {
            self.cv.wait(&mut n);
        }
    }
}
