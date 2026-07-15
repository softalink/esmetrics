//! Small concurrency and encoding helpers used across the crate.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

/// Returns the number of available CPU cores (upstream `cgroup.AvailableCPUs`).
pub(crate) fn available_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Counting semaphore (translation of Go's `chan struct{}` semaphores).
pub(crate) struct Sema {
    permits: Mutex<usize>,
    cv: Condvar,
}

impl Sema {
    pub fn new(permits: usize) -> Self {
        Sema {
            permits: Mutex::new(permits),
            cv: Condvar::new(),
        }
    }

    /// Blocks until a permit is available.
    pub fn acquire(&self) {
        let mut n = self.permits.lock();
        while *n == 0 {
            self.cv.wait(&mut n);
        }
        *n -= 1;
    }

    pub fn try_acquire(&self) -> bool {
        let mut n = self.permits.lock();
        if *n == 0 {
            return false;
        }
        *n -= 1;
        true
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

/// Shutdown signal shared by all background workers of a table
/// (translation of Go's closed `stopCh`).
pub(crate) struct Shutdown {
    stopped: AtomicBool,
    m: Mutex<()>,
    cv: Condvar,
}

impl Shutdown {
    pub fn new() -> Self {
        Shutdown {
            stopped: AtomicBool::new(false),
            m: Mutex::new(()),
            cv: Condvar::new(),
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
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
/// (translation of upstream `syncwg.WaitGroup`).
pub(crate) struct WaitCounter {
    count: Mutex<usize>,
    cv: Condvar,
}

impl WaitCounter {
    pub fn new() -> Self {
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

/// Encodes bytes to a lowercase hex string.
pub(crate) fn hex_encode(b: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for &x in b {
        s.push(HEX[(x >> 4) as usize] as char);
        s.push(HEX[(x & 0x0f) as usize] as char);
    }
    s
}

/// Decodes a hex string into bytes.
pub(crate) fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    let s = s.as_bytes();
    if s.len() % 2 != 0 {
        return Err(format!("odd-length hex string: {}", s.len()));
    }
    fn nibble(c: u8) -> Result<u8, String> {
        match c {
            b'0'..=b'9' => Ok(c - b'0'),
            b'a'..=b'f' => Ok(c - b'a' + 10),
            b'A'..=b'F' => Ok(c - b'A' + 10),
            _ => Err(format!("invalid hex char {c}")),
        }
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for pair in s.chunks_exact(2) {
        out.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        let data: Vec<u8> = (0..=255).collect();
        let s = hex_encode(&data);
        assert_eq!(hex_decode(&s).unwrap(), data);
        assert_eq!(hex_decode("").unwrap(), Vec::<u8>::new());
        assert!(hex_decode("a").is_err());
        assert!(hex_decode("zz").is_err());
    }

    #[test]
    fn sema_acquire_release() {
        let s = Sema::new(2);
        assert!(s.try_acquire());
        assert!(s.try_acquire());
        assert!(!s.try_acquire());
        s.release();
        assert!(s.try_acquire());
    }

    #[test]
    fn shutdown_wait() {
        let s = Shutdown::new();
        assert!(!s.wait_timeout(Duration::from_millis(1)));
        s.signal();
        assert!(s.wait_timeout(Duration::from_millis(1)));
        assert!(s.is_stopped());
    }
}
