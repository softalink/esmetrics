//! Shared helpers for esm-mergeset integration tests.
//!
//! Each integration-test binary uses only a subset of these helpers.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use esm_mergeset::{FlushCallback, Table, TableMetrics};

/// Deterministic pseudo-random generator (SplitMix64).
pub struct Rng(pub u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    pub fn intn(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    /// Random bytes with length in [0, 50), mimicking Go's testing/quick
    /// generated []byte values.
    pub fn random_bytes(&mut self) -> Vec<u8> {
        let n = self.intn(50);
        (0..n).map(|_| self.next_u64() as u8).collect()
    }
}

/// A unique test directory under the system temp dir.
pub fn test_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("esm-mergeset-test-{name}"))
}

pub fn remove_dir(path: &PathBuf) {
    let _ = std::fs::remove_dir_all(path);
}

pub fn open_table(path: &PathBuf) -> Table {
    Table::must_open(
        path,
        Duration::ZERO,
        None,
        Duration::ZERO,
        None,
        Arc::new(AtomicBool::new(false)),
    )
}

pub fn open_table_with_flush_counter(path: &PathBuf) -> (Table, Arc<AtomicU64>) {
    let flushes = Arc::new(AtomicU64::new(0));
    let flushes_clone = Arc::clone(&flushes);
    let cb: FlushCallback = Arc::new(move || {
        flushes_clone.fetch_add(1, Ordering::Relaxed);
    });
    let tb = Table::must_open(
        path,
        Duration::ZERO,
        Some(cb),
        Duration::ZERO,
        None,
        Arc::new(AtomicBool::new(false)),
    );
    (tb, flushes)
}

pub fn total_items_count(tb: &Table) -> u64 {
    let mut m = TableMetrics::default();
    tb.update_metrics(&mut m);
    m.total_items_count()
}

/// Re-opens the table several times, making sure the items count persists.
pub fn test_reopen_table(path: &PathBuf, items_count: u64) {
    for _ in 0..10 {
        let tb = open_table(path);
        let n = total_items_count(&tb);
        assert_eq!(n, items_count, "unexpected itemsCount after re-opening");
        tb.must_close();
    }
}
