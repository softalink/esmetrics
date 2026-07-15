//! Byte-budget gate for rollup memory estimation. Port of
//! `memory_limiter.go`.

use parking_lot::Mutex;

/// A simple mutex-protected byte budget.
pub struct MemoryLimiter {
    pub max_size: u64,
    usage: Mutex<u64>,
}

impl MemoryLimiter {
    pub fn new(max_size: u64) -> Self {
        MemoryLimiter {
            max_size,
            usage: Mutex::new(0),
        }
    }

    /// Tries reserving `n` bytes. Port of Go `memoryLimiter.Get`.
    pub fn get(&self, n: u64) -> bool {
        let mut usage = self.usage.lock();
        let ok = n <= self.max_size && self.max_size - n >= *usage;
        if ok {
            *usage += n;
        }
        ok
    }

    /// Releases `n` bytes reserved with [`MemoryLimiter::get`].
    /// Port of Go `memoryLimiter.Put`.
    pub fn put(&self, n: u64) {
        let mut usage = self.usage.lock();
        assert!(n <= *usage, "BUG: n={n} cannot exceed {}", *usage);
        *usage -= n;
    }
}

/// Global rollup memory limiter: `memory.Allowed() / 4`.
/// Port of Go `getRollupMemoryLimiter`.
pub fn get_rollup_memory_limiter() -> &'static MemoryLimiter {
    use std::sync::OnceLock;
    static LIMITER: OnceLock<MemoryLimiter> = OnceLock::new();
    LIMITER.get_or_init(|| MemoryLimiter::new(esm_common::memory::allowed() as u64 / 4))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Port of TestMemoryLimiter.
    #[test]
    fn memory_limiter() {
        let ml = MemoryLimiter::new(100);
        assert!(ml.get(10));
        assert!(ml.get(90));
        assert!(!ml.get(1));
        ml.put(10);
        assert!(ml.get(10));
        ml.put(100);
        assert!(ml.get(100));
        assert!(!ml.get(1));
        ml.put(100);
    }
}
