//! Port of VictoriaMetrics lib/backup: incremental backup/restore of
//! esm-storage snapshots to fs/s3/gcs/azblob destinations.

pub mod localfs;
pub mod names;
pub mod part;
pub mod remote;
pub mod timeutil;

pub mod backup;
pub mod cliflags;
pub mod restore;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// Runs `f` over `items` on up to `concurrency` threads; returns the first
/// error (remaining items are skipped once an error is seen).
pub fn run_parallel<T: Sync>(
    items: &[T],
    concurrency: usize,
    f: impl Fn(&T) -> anyhow::Result<()> + Sync,
) -> anyhow::Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    let next = AtomicUsize::new(0);
    let failed = std::sync::atomic::AtomicBool::new(false);
    let first_err: Mutex<Option<anyhow::Error>> = Mutex::new(None);
    let workers = concurrency.clamp(1, items.len());
    std::thread::scope(|s| {
        for _ in 0..workers {
            s.spawn(|| loop {
                if failed.load(Ordering::Relaxed) {
                    return;
                }
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= items.len() {
                    return;
                }
                if let Err(e) = f(&items[i]) {
                    failed.store(true, Ordering::Relaxed);
                    first_err.lock().unwrap().get_or_insert(e);
                    return;
                }
            });
        }
    });
    match first_err.into_inner().unwrap() {
        Some(e) => Err(e),
        None => Ok(()),
    }
}
