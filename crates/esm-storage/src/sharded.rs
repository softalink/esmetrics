//! Sharded multi-writer wrapper over [`Storage`].
//!
//! Splits series across N independent [`Storage`] shards — each its own
//! subdirectory and lock — keyed by a stable hash of the metric name, so
//! concurrent ingest from many connections proceeds in parallel instead of
//! serializing on a single global lock. Point reads route to the owning shard
//! by the same hash; whole-store scans fan out. The query evaluator consumes
//! this through the [`QueryStore`] trait, agnostic to partitioning.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::storage::{
    KeyedEntry, QueryStore, Sample, Storage, StorageError, StoredSample, TimeRange,
};
use crate::timeseries::Tsid;

/// Multi-shard storage. Each shard is a self-contained [`Storage`]; a series
/// always lands in (and is read from) the same shard. The per-shard lock is an
/// `RwLock` so concurrent queries (all `&self` reads) share a shard instead of
/// serializing — only ingest/flush/retention (`&mut self`) take it exclusively.
///
/// A background compactor thread keeps part count bounded: the ingest path
/// only spills pending (cheap, frequent → low buffered memory), and merging
/// runs off the write lock on the compactor thread (read-lock to pick a batch,
/// merge to a temp part with no lock held, brief write-lock to commit). This
/// is what lets ingest stay ahead of VM *and* peak RAM stay below it — without
/// it, bounding RAM by flushing more would either stall ingest (synchronous
/// merge) or explode part count (no merge), per the trilemma in
/// `docs/perf/tsbs-comparison.md`.
#[allow(missing_debug_implementations)]
pub struct ShardedStorage {
    data_dir: PathBuf,
    shards: Arc<Vec<RwLock<Storage>>>,
    compactor_stop: Arc<AtomicBool>,
    compactor: Mutex<Option<JoinHandle<()>>>,
}

/// Shared read lock on a shard, recovering on poison instead of panicking. A
/// poisoned shard means a prior panic mid-write; the structures stay usable
/// for best-effort continuation.
fn read_guard(m: &RwLock<Storage>) -> RwLockReadGuard<'_, Storage> {
    m.read().unwrap_or_else(PoisonError::into_inner)
}

/// Exclusive write lock on a shard (ingest / flush / retention / snapshot
/// creation), recovering on poison. See [`read_guard`].
fn write_guard(m: &RwLock<Storage>) -> RwLockWriteGuard<'_, Storage> {
    m.write().unwrap_or_else(PoisonError::into_inner)
}

impl ShardedStorage {
    /// Open (or create) `n_shards` shards under `data_dir/shard-NN`.
    ///
    /// # Errors
    /// Propagates any shard's [`Storage::open`] failure.
    pub fn open(data_dir: impl Into<PathBuf>, n_shards: usize) -> Result<Self, StorageError> {
        let data_dir = data_dir.into();
        let n = n_shards.max(1);
        // Divide a fixed total pending budget across shards so peak buffered
        // memory is independent of shard count — frequent cheap flushes keep
        // RAM low; the background compactor (below) keeps part count bounded.
        let per_shard = Self::TOTAL_PENDING_BUDGET_SAMPLES / n;
        let mut shards = Vec::with_capacity(n);
        for i in 0..n {
            let mut shard = Storage::open(data_dir.join(format!("shard-{i:02}")))?;
            shard.set_flush_threshold(per_shard);
            shards.push(RwLock::new(shard));
        }
        let shards = Arc::new(shards);
        let compactor_stop = Arc::new(AtomicBool::new(false));
        let compactor = Mutex::new(Some(Self::spawn_compactor(
            Arc::clone(&shards),
            Arc::clone(&compactor_stop),
        )));
        Ok(Self { data_dir, shards, compactor_stop, compactor })
    }

    /// Total samples buffered across all shards before a flush. Split evenly so
    /// peak pending memory doesn't scale with shard count. Low because flushes
    /// are cheap (no synchronous merge); the compactor reclaims parts.
    const TOTAL_PENDING_BUDGET_SAMPLES: usize = 8_000_000;

    /// Spawn the background compactor. It sweeps shards forever (until `stop`):
    /// pick a merge batch under a read lock, merge it to a temp part with no
    /// lock held, then commit the swap under a brief write lock. Sleeps only
    /// when a full sweep found nothing to do, so it keeps pace under load
    /// without spinning when idle.
    fn spawn_compactor(shards: Arc<Vec<RwLock<Storage>>>, stop: Arc<AtomicBool>) -> JoinHandle<()> {
        std::thread::spawn(move || {
            // One sweep over all shards at a given tier-fill threshold. Returns
            // whether any shard was compacted.
            let sweep = |min_parts: usize| -> bool {
                let mut did = false;
                for sh in shards.iter() {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let (batch, parts_dir) = {
                        let g = read_guard(sh);
                        (g.pick_merge_batch(min_parts), g.parts_dir().to_path_buf())
                    };
                    let Some(batch) = batch else { continue };
                    // Heavy merge runs with NO lock held (input parts are
                    // immutable); only the commit takes the write lock briefly.
                    // A transient error (e.g. retention raced a delete) just
                    // retries next sweep.
                    if let Ok(tmp) = Storage::merge_files_to_tmp(&batch, &parts_dir) {
                        let _ = write_guard(sh).commit_merged(&batch, &tmp);
                        did = true;
                    }
                }
                did
            };
            while !stop.load(Ordering::Relaxed) {
                // Size-tiered compaction (merge tiers of ≥MERGE_MIN_PARTS).
                // Deliberately NOT more aggressive: consolidating the last few
                // parts per shard down to one is expensive (rewrites large
                // parts) and, running concurrently with queries, costs more in
                // CPU/lock contention than the extra parts cost in reads.
                if !sweep(Storage::MERGE_MIN_PARTS) {
                    std::thread::sleep(Duration::from_millis(25));
                }
            }
        })
    }

    /// Stop and join the background compactor (idempotent).
    fn stop_compactor(&self) {
        self.compactor_stop.store(true, Ordering::Relaxed);
        if let Ok(mut guard) = self.compactor.lock()
            && let Some(handle) = guard.take()
        {
            let _ = handle.join();
        }
    }

    /// Number of shards.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Stable FNV-1a hash of the metric name → shard index. Deterministic
    /// across restarts (unlike `HashMap`'s randomized hasher), so routing is
    /// stable for the lifetime of the on-disk data.
    fn shard_idx(&self, name: &[u8]) -> usize {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in name {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        usize::try_from(h % self.shards.len() as u64).unwrap_or(0)
    }

    /// Ingest a batch, partitioning samples to their shards. Concurrency comes
    /// from multiple callers hitting different shards in parallel.
    ///
    /// # Errors
    /// Propagates the first shard ingest failure.
    pub fn ingest(&self, samples: &[Sample]) -> Result<(), StorageError> {
        if self.shards.len() == 1 {
            return write_guard(&self.shards[0]).ingest(samples);
        }
        // Route by index — no `Sample` clone. Each shard ingests its subset
        // directly from the original slice.
        let mut buckets: Vec<Vec<usize>> = (0..self.shards.len()).map(|_| Vec::new()).collect();
        for (i, s) in samples.iter().enumerate() {
            buckets[self.shard_idx(&s.metric_name)].push(i);
        }
        for (shard, indices) in buckets.into_iter().enumerate() {
            if !indices.is_empty() {
                write_guard(&self.shards[shard]).ingest_selected(samples, &indices)?;
            }
        }
        Ok(())
    }

    /// Ingest arena-keyed samples (see [`Storage::ingest_keyed`]), routing each
    /// to its shard by the key bytes — no per-sample heap key.
    ///
    /// # Errors
    /// Propagates the first shard ingest failure.
    pub fn ingest_keyed(&self, arena: &[u8], entries: &[KeyedEntry]) -> Result<(), StorageError> {
        if self.shards.len() == 1 {
            return write_guard(&self.shards[0]).ingest_keyed(arena, entries);
        }
        let mut buckets: Vec<Vec<usize>> = (0..self.shards.len()).map(|_| Vec::new()).collect();
        for (i, (range, _, _)) in entries.iter().enumerate() {
            buckets[self.shard_idx(&arena[range.clone()])].push(i);
        }
        for (shard, indices) in buckets.into_iter().enumerate() {
            if !indices.is_empty() {
                write_guard(&self.shards[shard]).ingest_keyed_subset(arena, entries, &indices)?;
            }
        }
        Ok(())
    }

    /// Flush every shard.
    ///
    /// # Errors
    /// Propagates the first shard flush failure.
    pub fn flush(&self) -> Result<(), StorageError> {
        // No synchronous merge — the background compactor is the sole merger,
        // so it never contends with this path on the temp merge dir.
        for sh in self.shards.iter() {
            write_guard(sh).flush_no_merge()?;
        }
        Ok(())
    }

    /// Stop the compactor and flush every shard. Shards release their data-dir
    /// locks when this `ShardedStorage` is dropped.
    ///
    /// # Errors
    /// Propagates the first shard flush failure.
    pub fn shutdown(self) -> Result<(), StorageError> {
        self.stop_compactor();
        for sh in self.shards.iter() {
            write_guard(sh).flush_no_merge()?;
        }
        Ok(())
    }

    /// Total distinct series across all shards.
    #[must_use]
    pub fn metric_count(&self) -> usize {
        self.shards.iter().map(|sh| read_guard(sh).metric_count()).sum()
    }

    /// Base data directory.
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Drop stale parts in every shard; returns total parts removed.
    ///
    /// # Errors
    /// Propagates the first shard failure.
    pub fn enforce_retention(&self, cutoff_ms: i64) -> Result<usize, StorageError> {
        let mut removed = 0;
        for sh in self.shards.iter() {
            removed += write_guard(sh).enforce_retention(cutoff_ms)?;
        }
        Ok(removed)
    }

    /// Drop stale snapshots in every shard; returns total removed.
    ///
    /// # Errors
    /// Propagates the first shard failure.
    pub fn enforce_snapshot_retention(&self, cutoff_ms: i64) -> Result<usize, StorageError> {
        let mut removed = 0;
        for sh in self.shards.iter() {
            removed += read_guard(sh).enforce_snapshot_retention(cutoff_ms)?;
        }
        Ok(removed)
    }

    /// Snapshot every shard under `<shard>/snapshots/<name>`. Returns the base
    /// data directory (the snapshot spans all shards).
    ///
    /// # Errors
    /// Propagates the first shard failure.
    pub fn create_snapshot(&self, name: &str) -> Result<PathBuf, StorageError> {
        for sh in self.shards.iter() {
            write_guard(sh).create_snapshot(name)?;
        }
        Ok(self.data_dir.clone())
    }

    /// Delete a snapshot from every shard.
    ///
    /// # Errors
    /// Propagates the first shard failure.
    pub fn delete_snapshot(&self, name: &str) -> Result<(), StorageError> {
        for sh in self.shards.iter() {
            read_guard(sh).delete_snapshot(name)?;
        }
        Ok(())
    }

    /// Union of snapshot names across shards (they are created in lockstep).
    ///
    /// # Errors
    /// Propagates the first shard failure.
    pub fn list_snapshots(&self) -> Result<Vec<String>, StorageError> {
        let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for sh in self.shards.iter() {
            set.extend(read_guard(sh).list_snapshots()?);
        }
        Ok(set.into_iter().collect())
    }
}

impl Drop for ShardedStorage {
    fn drop(&mut self) {
        // Stop the compactor so its `Arc` clone of the shards is released and
        // the thread doesn't outlive the store (e.g. when used without an
        // explicit `shutdown()`).
        self.stop_compactor();
    }
}

impl QueryStore for ShardedStorage {
    fn iter_metric_names(&self) -> Vec<(Vec<u8>, Tsid)> {
        let mut out = Vec::new();
        for sh in self.shards.iter() {
            out.extend(read_guard(sh).iter_metric_names());
        }
        out
    }

    fn search_by_metric_name(
        &self,
        metric_name: &[u8],
        range: TimeRange,
    ) -> Result<Vec<StoredSample>, StorageError> {
        let idx = self.shard_idx(metric_name);
        read_guard(&self.shards[idx]).search_by_metric_name(metric_name, range)
    }

    fn lookup_tsid(&self, metric_name: &[u8]) -> Option<Tsid> {
        let idx = self.shard_idx(metric_name);
        read_guard(&self.shards[idx]).lookup_tsid(metric_name)
    }

    fn series_for_metric_name(&self, name_part: &[u8]) -> Vec<Vec<u8>> {
        // A name's series may live in different shards (they hash by full key),
        // so fan out and concatenate.
        let mut out = Vec::new();
        for sh in self.shards.iter() {
            out.extend(read_guard(sh).series_for_metric_name(name_part));
        }
        out
    }

    fn series_for_label(&self, label_name: &[u8], label_value: &[u8]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for sh in self.shards.iter() {
            out.extend(read_guard(sh).series_for_label(label_name, label_value));
        }
        out
    }

    fn distinct_metric_names(&self) -> Vec<Vec<u8>> {
        let mut set: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        for sh in self.shards.iter() {
            set.extend(read_guard(sh).distinct_metric_names());
        }
        set.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(name: &[u8], ts: i64, v: i64) -> Sample {
        Sample { metric_name: name.to_vec(), timestamp_ms: ts, value: v }
    }

    #[test]
    fn ingest_routes_and_reads_back_across_shards() {
        let tmp = tempfile::tempdir().unwrap();
        let s = ShardedStorage::open(tmp.path().join("d"), 8).unwrap();
        let names: Vec<Vec<u8>> = (0..50).map(|i| format!("metric_{i}").into_bytes()).collect();
        let batch: Vec<Sample> = names.iter().map(|n| sample(n, 1000, 7)).collect();
        s.ingest(&batch).unwrap();
        assert_eq!(s.metric_count(), 50);
        // Every series is readable from whichever shard owns it.
        let range = TimeRange { min_timestamp_ms: 0, max_timestamp_ms: 2000 };
        for n in &names {
            let hits = s.search_by_metric_name(n, range).unwrap();
            assert_eq!(hits.len(), 1, "missing {:?}", String::from_utf8_lossy(n));
            assert_eq!(hits[0].value, 7);
        }
        // iter_metric_names unions all shards.
        assert_eq!(s.iter_metric_names().len(), 50);
    }

    #[test]
    fn routing_is_stable_across_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("d");
        {
            let s = ShardedStorage::open(&dir, 8).unwrap();
            s.ingest(&[sample(b"cpu_usage_user", 1000, 42)]).unwrap();
            s.flush().unwrap();
            s.shutdown().unwrap();
        }
        let s = ShardedStorage::open(&dir, 8).unwrap();
        let hits = s
            .search_by_metric_name(
                b"cpu_usage_user",
                TimeRange { min_timestamp_ms: 0, max_timestamp_ms: 2000 },
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].value, 42);
    }
}
