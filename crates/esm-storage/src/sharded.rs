//! Sharded multi-writer wrapper over [`Storage`].
//!
//! Splits series across N independent [`Storage`] shards — each its own
//! subdirectory and lock — keyed by a stable hash of the metric name, so
//! concurrent ingest from many connections proceeds in parallel instead of
//! serializing on a single global lock. Point reads route to the owning shard
//! by the same hash; whole-store scans fan out. The query evaluator consumes
//! this through the [`QueryStore`] trait, agnostic to partitioning.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use crate::storage::{QueryStore, Sample, Storage, StorageError, StoredSample, TimeRange};
use crate::timeseries::Tsid;

/// Multi-shard storage. Each shard is a self-contained [`Storage`]; a series
/// always lands in (and is read from) the same shard.
#[allow(missing_debug_implementations)]
pub struct ShardedStorage {
    data_dir: PathBuf,
    shards: Vec<Mutex<Storage>>,
}

/// Lock a shard, recovering the guard on poison instead of panicking. A
/// poisoned shard means a prior panic mid-write; the structures stay usable
/// for best-effort continuation.
fn guard(m: &Mutex<Storage>) -> MutexGuard<'_, Storage> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl ShardedStorage {
    /// Open (or create) `n_shards` shards under `data_dir/shard-NN`.
    ///
    /// # Errors
    /// Propagates any shard's [`Storage::open`] failure.
    pub fn open(data_dir: impl Into<PathBuf>, n_shards: usize) -> Result<Self, StorageError> {
        let data_dir = data_dir.into();
        let n = n_shards.max(1);
        let mut shards = Vec::with_capacity(n);
        for i in 0..n {
            shards.push(Mutex::new(Storage::open(data_dir.join(format!("shard-{i:02}")))?));
        }
        Ok(Self { data_dir, shards })
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
            return guard(&self.shards[0]).ingest(samples);
        }
        let mut buckets: Vec<Vec<Sample>> = (0..self.shards.len()).map(|_| Vec::new()).collect();
        for s in samples {
            let idx = self.shard_idx(&s.metric_name);
            buckets[idx].push(s.clone());
        }
        for (i, batch) in buckets.into_iter().enumerate() {
            if !batch.is_empty() {
                guard(&self.shards[i]).ingest(&batch)?;
            }
        }
        Ok(())
    }

    /// Flush every shard.
    ///
    /// # Errors
    /// Propagates the first shard flush failure.
    pub fn flush(&self) -> Result<(), StorageError> {
        for sh in &self.shards {
            guard(sh).flush()?;
        }
        Ok(())
    }

    /// Flush and release every shard.
    ///
    /// # Errors
    /// Propagates the first shard shutdown failure.
    pub fn shutdown(self) -> Result<(), StorageError> {
        for m in self.shards {
            m.into_inner().unwrap_or_else(PoisonError::into_inner).shutdown()?;
        }
        Ok(())
    }

    /// Total distinct series across all shards.
    #[must_use]
    pub fn metric_count(&self) -> usize {
        self.shards.iter().map(|sh| guard(sh).metric_count()).sum()
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
        for sh in &self.shards {
            removed += guard(sh).enforce_retention(cutoff_ms)?;
        }
        Ok(removed)
    }

    /// Drop stale snapshots in every shard; returns total removed.
    ///
    /// # Errors
    /// Propagates the first shard failure.
    pub fn enforce_snapshot_retention(&self, cutoff_ms: i64) -> Result<usize, StorageError> {
        let mut removed = 0;
        for sh in &self.shards {
            removed += guard(sh).enforce_snapshot_retention(cutoff_ms)?;
        }
        Ok(removed)
    }

    /// Snapshot every shard under `<shard>/snapshots/<name>`. Returns the base
    /// data directory (the snapshot spans all shards).
    ///
    /// # Errors
    /// Propagates the first shard failure.
    pub fn create_snapshot(&self, name: &str) -> Result<PathBuf, StorageError> {
        for sh in &self.shards {
            guard(sh).create_snapshot(name)?;
        }
        Ok(self.data_dir.clone())
    }

    /// Delete a snapshot from every shard.
    ///
    /// # Errors
    /// Propagates the first shard failure.
    pub fn delete_snapshot(&self, name: &str) -> Result<(), StorageError> {
        for sh in &self.shards {
            guard(sh).delete_snapshot(name)?;
        }
        Ok(())
    }

    /// Union of snapshot names across shards (they are created in lockstep).
    ///
    /// # Errors
    /// Propagates the first shard failure.
    pub fn list_snapshots(&self) -> Result<Vec<String>, StorageError> {
        let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for sh in &self.shards {
            set.extend(guard(sh).list_snapshots()?);
        }
        Ok(set.into_iter().collect())
    }
}

impl QueryStore for ShardedStorage {
    fn iter_metric_names(&self) -> Vec<(Vec<u8>, Tsid)> {
        let mut out = Vec::new();
        for sh in &self.shards {
            out.extend(guard(sh).iter_metric_names());
        }
        out
    }

    fn search_by_metric_name(
        &self,
        metric_name: &[u8],
        range: TimeRange,
    ) -> Result<Vec<StoredSample>, StorageError> {
        let idx = self.shard_idx(metric_name);
        guard(&self.shards[idx]).search_by_metric_name(metric_name, range)
    }

    fn lookup_tsid(&self, metric_name: &[u8]) -> Option<Tsid> {
        let idx = self.shard_idx(metric_name);
        guard(&self.shards[idx]).lookup_tsid(metric_name)
    }

    fn series_for_metric_name(&self, name_part: &[u8]) -> Vec<Vec<u8>> {
        // A name's series may live in different shards (they hash by full key),
        // so fan out and concatenate.
        let mut out = Vec::new();
        for sh in &self.shards {
            out.extend(guard(sh).series_for_metric_name(name_part));
        }
        out
    }

    fn distinct_metric_names(&self) -> Vec<Vec<u8>> {
        let mut set: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        for sh in &self.shards {
            set.extend(guard(sh).distinct_metric_names());
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
