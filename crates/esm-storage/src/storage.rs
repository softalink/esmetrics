//! `Storage` — the top-level engine API tying the mergeset index, the
//! time-series part storage, and the (future) in-memory write buffer into one
//! `open / ingest / search / shutdown` surface.
//!
//! Phase 1E **minimum viable** implementation: in-memory TSID assignment +
//! per-shutdown flush to disk. The merger, on-disk indexdb, and incremental
//! flush land in subsequent sub-phases.
//!
//! Lifecycle:
//! 1. [`Storage::open`] locks the data directory via [`esm_platform::file_lock`]
//!    and (re-)loads existing parts from disk.
//! 2. [`Storage::ingest`] accepts batches of `(metric_name, timestamp, value)`
//!    samples, assigns TSIDs, and stages them in a write buffer.
//! 3. [`Storage::flush`] writes a new time-series part to disk for any pending
//!    samples.
//! 4. [`Storage::search_by_metric_name`] returns matching samples within a
//!    time range.
//! 5. [`Storage::shutdown`] flushes, syncs, and releases the lock.

use std::collections::{BTreeMap, HashMap};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use esm_platform::file_lock::FileLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use std::sync::Arc;

use crate::timeseries::{
    self, BlockStreamReader, BlockStreamWriter, MetaindexRow, PartHeader, Tsid,
    block_stream_writer::{DEFAULT_PRECISION_BITS, DEFAULT_SCALE},
};

const INDEX_FILENAME: &str = "index.json";
const INDEX_SCHEMA_VERSION: u32 = 1;
const INDEX_BIN_FILENAME: &str = "index.bin";
/// Magic bytes + version. Changing the magic invalidates older binary
/// indexes; bumping the version while keeping the magic invites silent
/// data corruption, so any format change must change BOTH.
const INDEX_BIN_MAGIC: &[u8; 8] = b"ESMIDX01";

#[derive(Debug, Serialize, Deserialize)]
struct IndexFile {
    schema_version: u32,
    next_metric_id: u64,
    /// One entry per known metric, sorted by `metric_id` for diff-friendliness.
    entries: Vec<IndexEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexEntry {
    metric_id: u64,
    /// Lower-case hex encoding of the metric-name bytes. Hex chosen over
    /// base64 to avoid adding a third-party crate; metric-name strings are
    /// small enough that the 2x size cost is irrelevant.
    name_hex: String,
}

#[allow(dead_code)]
fn bytes_to_hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        let _ = write!(s, "{byte:02x}");
    }
    s
}

/// Compute the on-disk byte size of a part directory by summing all regular
/// file sizes inside it. Returns zero for an empty (or missing) directory.
fn part_byte_size(path: &Path) -> std::io::Result<u64> {
    let mut total: u64 = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}

fn hex_to_bytes(s: &str) -> Result<Vec<u8>, &'static str> {
    if !s.len().is_multiple_of(2) {
        return Err("hex string length must be even");
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for chunk in s.as_bytes().chunks(2) {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_nibble(c: u8) -> Result<u8, &'static str> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err("invalid hex digit"),
    }
}

/// One ingested sample.
#[derive(Debug, Clone)]
pub struct Sample {
    /// Canonical metric identifier (label-set serialised into bytes). For
    /// Phase 1E MVP we use the raw bytes as the in-memory key.
    pub metric_name: Vec<u8>,
    /// Unix epoch millis.
    pub timestamp_ms: i64,
    /// Sample value. Stored as int64 to match VM's storage path. Floats
    /// are converted via the decimal layer (deferred to a later phase).
    pub value: i64,
}

/// Inclusive time range query.
#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    pub min_timestamp_ms: i64,
    pub max_timestamp_ms: i64,
}

/// Decoded sample yielded by [`Storage::search_by_metric_name`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredSample {
    pub timestamp_ms: i64,
    pub value: i64,
}

/// One sample for the arena-keyed ingest path: `(key_byte_range, timestamp_ms,
/// value)`, where the range indexes the caller's key arena.
pub type KeyedEntry = (std::ops::Range<usize>, i64, i64);

/// Cached metadata for one on-disk part. Lets queries prune parts by time
/// range without a `read_dir` + part-header open per call, and reuse the
/// parsed header + metaindex so a per-series read opens only the data files
/// (no metadata-JSON parse or metaindex zstd-decompress per read).
#[derive(Debug, Clone)]
struct PartMeta {
    path: PathBuf,
    min_ts: i64,
    max_ts: i64,
    header: PartHeader,
    metaindex: Arc<Vec<MetaindexRow>>,
}

impl PartMeta {
    /// Build a `PartMeta` from an opened reader, caching its header +
    /// metaindex for reuse by [`BlockStreamReader::open_with_index`].
    fn from_reader(path: PathBuf, reader: &BlockStreamReader) -> Self {
        Self {
            min_ts: reader.part_header.min_timestamp,
            max_ts: reader.part_header.max_timestamp,
            header: reader.part_header.clone(),
            metaindex: reader.metaindex(),
            path,
        }
    }

    /// Open `path` once to load and cache its header + metaindex.
    fn load(path: PathBuf) -> Result<Self, StorageError> {
        let reader = BlockStreamReader::open(&path)
            .map_err(|e| StorageError::OpenPart { path: path.clone(), source: e })?;
        Ok(Self::from_reader(path, &reader))
    }
}

/// Read-only series source consumed by the query evaluator. Implemented by
/// both the single-shard [`Storage`] and the multi-shard
/// [`crate::sharded::ShardedStorage`], so the evaluator is agnostic to how the
/// data is partitioned.
pub trait QueryStore {
    /// Every `(metric_name, tsid)` pair known to the store.
    fn iter_metric_names(&self) -> Vec<(Vec<u8>, Tsid)>;
    /// Samples for an exact metric-name key within `range`.
    ///
    /// # Errors
    /// Propagates storage I/O / decode failures.
    fn search_by_metric_name(
        &self,
        metric_name: &[u8],
        range: TimeRange,
    ) -> Result<Vec<StoredSample>, StorageError>;
    /// TSID for an exact metric-name key, if present (existence check).
    fn lookup_tsid(&self, metric_name: &[u8]) -> Option<Tsid>;
    /// Full series keys whose metric-name part equals `name_part`.
    fn series_for_metric_name(&self, name_part: &[u8]) -> Vec<Vec<u8>>;
    /// Full series keys carrying `label_name="label_value"`.
    fn series_for_label(&self, label_name: &[u8], label_value: &[u8]) -> Vec<Vec<u8>>;
    /// Every distinct metric-name part in the store.
    fn distinct_metric_names(&self) -> Vec<Vec<u8>>;
}

impl QueryStore for Storage {
    fn iter_metric_names(&self) -> Vec<(Vec<u8>, Tsid)> {
        Storage::iter_metric_names(self)
    }
    fn search_by_metric_name(
        &self,
        metric_name: &[u8],
        range: TimeRange,
    ) -> Result<Vec<StoredSample>, StorageError> {
        Storage::search_by_metric_name(self, metric_name, range)
    }
    fn lookup_tsid(&self, metric_name: &[u8]) -> Option<Tsid> {
        Storage::lookup_tsid(self, metric_name)
    }
    fn series_for_metric_name(&self, name_part: &[u8]) -> Vec<Vec<u8>> {
        Storage::series_for_metric_name(self, name_part)
    }
    fn series_for_label(&self, label_name: &[u8], label_value: &[u8]) -> Vec<Vec<u8>> {
        Storage::series_for_label(self, label_name, label_value)
    }
    fn distinct_metric_names(&self) -> Vec<Vec<u8>> {
        Storage::distinct_metric_names(self)
    }
}

/// Top-level storage engine.
#[allow(missing_debug_implementations)]
pub struct Storage {
    data_dir: PathBuf,
    _lock: FileLock,

    /// `metric_name (raw bytes) -> Tsid`. Populated lazily as new metrics
    /// arrive. Persisted to a sidecar file in Phase 1D; for now we treat the
    /// mapping as ephemeral and recompute on each open from on-disk parts.
    name_to_tsid: FnvHashMap<Vec<u8>, Tsid>,
    /// Reverse mapping for query results.
    tsid_to_name: HashMap<Tsid, Vec<u8>>,

    /// Next MetricID to hand out. Phase 1D will replace this with a
    /// persistent generation counter.
    next_metric_id: u64,

    /// Buffered samples awaiting flush: `tsid -> (ts, value) pairs`.
    /// An `FnvHashMap` for O(1) amortized per-sample insert — the on-disk part
    /// requires tsids in sorted order, but that ordering is needed only once at
    /// flush (where the tsids are sorted), not on every ingested sample.
    pending: FnvHashMap<Tsid, Vec<(i64, i64)>>,

    /// Parts directory. Each part is a `BlockStreamWriter`-produced subdir.
    parts_dir: PathBuf,

    /// Sidecar path holding the persistent metric-name ↔ TSID map.
    index_path: PathBuf,

    /// Whether the in-memory index has unsaved changes.
    index_dirty: bool,

    /// Monotonic part counter for naming.
    next_part_id: u64,

    /// Count of samples currently buffered in `pending`. Drives the
    /// incremental-flush trigger so memory stays bounded during sustained
    /// ingest instead of growing until an explicit `flush()`/`shutdown()`.
    pending_samples: usize,

    /// Cached time-range metadata for every on-disk part. Maintained
    /// incrementally on flush/merge/retention so reads prune parts by time
    /// range without re-scanning the parts directory on every query.
    parts_index: Vec<PartMeta>,

    /// Inverted index: metric-name part (bytes before `{`) → the full series
    /// keys sharing that name. Lets name-anchored selectors resolve to just
    /// the relevant series instead of scanning every series.
    metric_name_index: HashMap<Vec<u8>, Vec<Vec<u8>>>,

    /// Inverted index: `(label_name, label_value)` → full series keys carrying
    /// that label. Lets selectors with equality label matchers (e.g.
    /// `hostname="host_0"`) narrow candidates without a full scan.
    label_index: LabelIndex,
}

/// `(label_name, label_value)` → full series keys carrying that label pair.
type LabelIndex = HashMap<(Vec<u8>, Vec<u8>), Vec<Vec<u8>>>;

/// FNV-1a hasher — much faster than std's SipHash for the short byte keys in
/// the hot ingest path (metric-name → TSID lookup, once per sample). Internal
/// indexes only; not exposed to untrusted hash-flooding.
struct FnvHasher(u64);
impl Default for FnvHasher {
    fn default() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}
impl std::hash::Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        let mut h = self.0;
        for &b in bytes {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        self.0 = h;
    }
}
type FnvBuild = std::hash::BuildHasherDefault<FnvHasher>;
type FnvHashMap<K, V> = HashMap<K, V, FnvBuild>;

/// The metric-name part of a full series key: the bytes before the first `{`.
fn metric_name_part(key: &[u8]) -> &[u8] {
    match key.iter().position(|&b| b == b'{') {
        Some(i) => &key[..i],
        None => key,
    }
}

/// Parse the `{k="v",...}` label part of a canonical series key into
/// `(label_name, label_value)` byte pairs. Trusts the canonical
/// text-exposition encoding (sorted `name="value"`, comma-separated).
fn parse_label_pairs(key: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let Some(open) = key.iter().position(|&b| b == b'{') else { return Vec::new() };
    let Ok(s) = std::str::from_utf8(&key[open..]) else { return Vec::new() };
    let inner = s.trim_start_matches('{').trim_end_matches('}');
    let mut out = Vec::new();
    if inner.is_empty() {
        return out;
    }
    for part in inner.split(',') {
        if let Some((name, raw)) = part.split_once('=') {
            let value = raw.trim_start_matches('"').trim_end_matches('"');
            out.push((name.as_bytes().to_vec(), value.as_bytes().to_vec()));
        }
    }
    out
}

impl Storage {
    /// Open a data directory. Creates it if missing. Acquires an exclusive
    /// lock on a sentinel file inside the directory.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the directory cannot be created, the lock
    /// cannot be acquired, or any existing parts are malformed.
    pub fn open(data_dir: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir)?;
        let parts_dir = data_dir.join("ts_parts");
        std::fs::create_dir_all(&parts_dir)?;

        let lock_path = data_dir.join(".lock");
        let lock = FileLock::try_acquire_exclusive(&lock_path)?
            .ok_or_else(|| StorageError::DataDirLocked(data_dir.clone()))?;

        let index_path = data_dir.join(INDEX_FILENAME);
        let mut storage = Self {
            data_dir,
            _lock: lock,
            name_to_tsid: FnvHashMap::default(),
            tsid_to_name: HashMap::new(),
            next_metric_id: 1,
            pending: FnvHashMap::default(),
            parts_dir,
            index_path,
            index_dirty: false,
            next_part_id: 0,
            pending_samples: 0,
            parts_index: Vec::new(),
            metric_name_index: HashMap::new(),
            label_index: HashMap::new(),
        };
        storage.load_index()?;
        storage.bootstrap_from_disk()?;
        storage.rebuild_query_indexes();
        Ok(storage)
    }

    /// Rebuild the metric-name and label inverted indexes from the current
    /// `name_to_tsid` map. Called once after open; kept current incrementally
    /// by [`Self::get_or_create_tsid`] thereafter.
    fn rebuild_query_indexes(&mut self) {
        self.metric_name_index.clear();
        self.label_index.clear();
        let keys: Vec<Vec<u8>> = self.name_to_tsid.keys().cloned().collect();
        for key in keys {
            self.index_series_key(&key);
        }
    }

    /// Add one series key to both inverted indexes.
    fn index_series_key(&mut self, key: &[u8]) {
        self.metric_name_index
            .entry(metric_name_part(key).to_vec())
            .or_default()
            .push(key.to_vec());
        for (ln, lv) in parse_label_pairs(key) {
            self.label_index.entry((ln, lv)).or_default().push(key.to_vec());
        }
    }

    fn load_index(&mut self) -> Result<(), StorageError> {
        // Prefer the binary index. Fall back to the JSON sidecar so
        // existing on-disk data survives the upgrade.
        let bin_path = self.data_dir.join(INDEX_BIN_FILENAME);
        if bin_path.exists() {
            return self.load_index_binary(&bin_path);
        }
        let raw = match std::fs::read(&self.index_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let parsed: IndexFile = serde_json::from_slice(&raw).map_err(StorageError::IndexParse)?;
        if parsed.schema_version != INDEX_SCHEMA_VERSION {
            return Err(StorageError::IndexSchemaVersion {
                got: parsed.schema_version,
                want: INDEX_SCHEMA_VERSION,
            });
        }
        for entry in parsed.entries {
            let name = hex_to_bytes(&entry.name_hex).map_err(StorageError::IndexHex)?;
            let tsid = Tsid { metric_id: entry.metric_id, ..Default::default() };
            self.name_to_tsid.insert(name.clone(), tsid);
            self.tsid_to_name.insert(tsid, name);
        }
        self.next_metric_id = parsed.next_metric_id;
        Ok(())
    }

    fn save_index(&mut self) -> Result<(), StorageError> {
        if !self.index_dirty {
            return Ok(());
        }
        self.save_index_binary()?;
        // Remove the legacy JSON sidecar if it's still around — keeping it
        // would let a downgraded reader hit stale data.
        let json_path = self.data_dir.join(INDEX_FILENAME);
        if json_path.exists() {
            let _ = std::fs::remove_file(json_path);
        }
        self.index_dirty = false;
        Ok(())
    }

    /// Persistence helper: write the binary index.
    ///
    /// On-disk layout (little-endian throughout):
    /// ```text
    /// magic[8]            = "ESMIDX01"
    /// next_metric_id[u64]
    /// entry_count[u32]
    /// for each entry, sorted by metric_name bytes ascending:
    ///   metric_id[u64]
    ///   name_len[u32]
    ///   name[name_len]
    /// ```
    fn save_index_binary(&self) -> Result<(), StorageError> {
        let bin_path = self.data_dir.join(INDEX_BIN_FILENAME);
        let mut entries: Vec<(&Vec<u8>, u64)> =
            self.name_to_tsid.iter().map(|(name, tsid)| (name, tsid.metric_id)).collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let mut buf = Vec::with_capacity(8 + 8 + 4 + entries.len() * 32);
        buf.extend_from_slice(INDEX_BIN_MAGIC);
        buf.extend_from_slice(&self.next_metric_id.to_le_bytes());
        let count = u32::try_from(entries.len()).unwrap_or(u32::MAX);
        buf.extend_from_slice(&count.to_le_bytes());
        for (name, metric_id) in &entries {
            buf.extend_from_slice(&metric_id.to_le_bytes());
            let name_len = u32::try_from(name.len()).unwrap_or(u32::MAX);
            buf.extend_from_slice(&name_len.to_le_bytes());
            buf.extend_from_slice(name);
        }
        let tmp = bin_path.with_extension("bin.tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&buf)?;
            f.sync_all()?;
        }
        esm_platform::atomic_rename::rename(&tmp, &bin_path)?;
        esm_platform::durability::fsync_dir(&self.data_dir)?;
        Ok(())
    }

    fn load_index_binary(&mut self, path: &Path) -> Result<(), StorageError> {
        let raw = std::fs::read(path)?;
        if raw.len() < 8 + 8 + 4 {
            return Err(StorageError::IndexHex("binary index truncated"));
        }
        if &raw[..8] != INDEX_BIN_MAGIC {
            return Err(StorageError::IndexHex("binary index magic mismatch"));
        }
        let mut p = 8;
        let next_metric_id = u64::from_le_bytes(raw[p..p + 8].try_into().unwrap_or([0; 8]));
        p += 8;
        let count = u32::from_le_bytes(raw[p..p + 4].try_into().unwrap_or([0; 4])) as usize;
        p += 4;
        for _ in 0..count {
            if raw.len() < p + 8 + 4 {
                return Err(StorageError::IndexHex("binary index truncated entry"));
            }
            let metric_id = u64::from_le_bytes(raw[p..p + 8].try_into().unwrap_or([0; 8]));
            p += 8;
            let name_len = u32::from_le_bytes(raw[p..p + 4].try_into().unwrap_or([0; 4])) as usize;
            p += 4;
            if raw.len() < p + name_len {
                return Err(StorageError::IndexHex("binary index truncated name"));
            }
            let name = raw[p..p + name_len].to_vec();
            p += name_len;
            let tsid = Tsid { metric_id, ..Default::default() };
            self.name_to_tsid.insert(name.clone(), tsid);
            self.tsid_to_name.insert(tsid, name);
        }
        self.next_metric_id = next_metric_id;
        Ok(())
    }

    /// Reconstruct the in-memory TSID map by scanning existing parts. For
    /// Phase 1E MVP we walk each part once and record any TSID we see.
    fn bootstrap_from_disk(&mut self) -> Result<(), StorageError> {
        let entries = match std::fs::read_dir(&self.parts_dir) {
            Ok(it) => it,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let mut max_id_seen: u64 = 0;
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let part_path = entry.path();
            // Track the highest "part-N" suffix so new parts don't collide.
            if let Some(name) = part_path.file_name().and_then(|s| s.to_str())
                && let Some(rest) = name.strip_prefix("part-")
                && let Ok(n) = rest.parse::<u64>()
                && n >= max_id_seen
            {
                max_id_seen = n + 1;
            }
            // Discover TSIDs in this part.
            let mut reader = BlockStreamReader::open(&part_path)
                .map_err(|e| StorageError::OpenPart { path: part_path.clone(), source: e })?;
            self.parts_index.push(PartMeta::from_reader(part_path.clone(), &reader));
            while let Some(block) = reader
                .next_block()
                .map_err(|e| StorageError::ReadPart { path: part_path.clone(), source: e })?
            {
                let tsid = block.header.tsid;
                if self.next_metric_id <= tsid.metric_id {
                    self.next_metric_id = tsid.metric_id + 1;
                }
                // If the index file already has a real name for this TSID
                // we keep it (preferred); otherwise pre-seed with a
                // placeholder so the engine remains queryable.
                if !self.tsid_to_name.contains_key(&tsid) {
                    let placeholder_name = format!("tsid:{}", tsid.metric_id).into_bytes();
                    self.name_to_tsid.entry(placeholder_name.clone()).or_insert(tsid);
                    self.tsid_to_name.entry(tsid).or_insert(placeholder_name);
                    self.index_dirty = true;
                }
            }
        }
        self.next_part_id = max_id_seen;
        Ok(())
    }

    /// Ingest a batch of samples. Samples are buffered in memory until
    /// [`Self::flush`] or [`Self::shutdown`].
    ///
    /// # Errors
    /// Currently infallible; the signature reserves room for future
    /// validation (timestamp ordering, label-set parsing).
    pub fn ingest(&mut self, samples: &[Sample]) -> Result<(), StorageError> {
        for s in samples {
            self.buffer_one(&s.metric_name, s.timestamp_ms, s.value);
        }
        self.after_ingest(samples.len())
    }

    /// Ingest only `samples[i]` for `i` in `indices`. Lets a sharded wrapper
    /// route a batch to per-shard subsets without cloning any `Sample`.
    ///
    /// # Errors
    /// Returns [`StorageError`] if a triggered flush fails.
    pub fn ingest_selected(
        &mut self,
        samples: &[Sample],
        indices: &[usize],
    ) -> Result<(), StorageError> {
        for &i in indices {
            let s = &samples[i];
            self.buffer_one(&s.metric_name, s.timestamp_ms, s.value);
        }
        self.after_ingest(indices.len())
    }

    /// Ingest samples whose metric-name keys live as slices of `arena`
    /// (`entry.0` is the key's byte range). Avoids a heap key per sample — keys
    /// are interned by [`Self::get_or_create_tsid`], allocating only for new
    /// series.
    ///
    /// # Errors
    /// Returns [`StorageError`] if a triggered flush fails.
    pub fn ingest_keyed(
        &mut self,
        arena: &[u8],
        entries: &[KeyedEntry],
    ) -> Result<(), StorageError> {
        for (range, timestamp_ms, value) in entries {
            self.buffer_one(&arena[range.clone()], *timestamp_ms, *value);
        }
        self.after_ingest(entries.len())
    }

    /// As [`Self::ingest_keyed`] but only for `entries[i]` where `i` in
    /// `indices` (used by the sharded wrapper to route a batch).
    ///
    /// # Errors
    /// Returns [`StorageError`] if a triggered flush fails.
    pub fn ingest_keyed_subset(
        &mut self,
        arena: &[u8],
        entries: &[KeyedEntry],
        indices: &[usize],
    ) -> Result<(), StorageError> {
        for &i in indices {
            let (range, timestamp_ms, value) = &entries[i];
            self.buffer_one(&arena[range.clone()], *timestamp_ms, *value);
        }
        self.after_ingest(indices.len())
    }

    fn buffer_one(&mut self, name: &[u8], timestamp_ms: i64, value: i64) {
        let tsid = self.get_or_create_tsid(name);
        self.pending.entry(tsid).or_default().push((timestamp_ms, value));
    }

    fn after_ingest(&mut self, added: usize) -> Result<(), StorageError> {
        self.pending_samples += added;
        // Bound memory: flush once the buffer crosses the threshold. Frequent
        // small flushes also keep each series short per part, which avoids the
        // expensive large-series flush path.
        if self.pending_samples >= Self::FLUSH_THRESHOLD_SAMPLES {
            self.flush()?;
        }
        Ok(())
    }

    /// Auto-flush trigger: buffer this many samples before spilling a part.
    /// ~1M (ts,value) pairs ≈ a few tens of MB of live buffer.
    const FLUSH_THRESHOLD_SAMPLES: usize = 1_000_000;

    fn get_or_create_tsid(&mut self, name: &[u8]) -> Tsid {
        if let Some(t) = self.name_to_tsid.get(name) {
            return *t;
        }
        let metric_id = self.next_metric_id;
        self.next_metric_id += 1;
        let tsid = Tsid { metric_id, ..Default::default() };
        self.name_to_tsid.insert(name.to_vec(), tsid);
        self.tsid_to_name.insert(tsid, name.to_vec());
        self.index_series_key(name);
        self.index_dirty = true;
        tsid
    }

    /// Full series keys whose metric-name part equals `name_part`. Empty if
    /// none. Used by the query layer to avoid scanning unrelated series.
    #[must_use]
    pub fn series_for_metric_name(&self, name_part: &[u8]) -> Vec<Vec<u8>> {
        self.metric_name_index.get(name_part).cloned().unwrap_or_default()
    }

    /// Full series keys carrying `label_name="label_value"`. Empty if none.
    #[must_use]
    pub fn series_for_label(&self, label_name: &[u8], label_value: &[u8]) -> Vec<Vec<u8>> {
        self.label_index
            .get(&(label_name.to_vec(), label_value.to_vec()))
            .cloned()
            .unwrap_or_default()
    }

    /// Every distinct metric-name part known to this store.
    #[must_use]
    pub fn distinct_metric_names(&self) -> Vec<Vec<u8>> {
        self.metric_name_index.keys().cloned().collect()
    }

    /// Sync any pending samples to a new on-disk part.
    ///
    /// # Errors
    /// Returns [`StorageError`] on I/O failure or encoding error.
    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.save_index()?;
        if self.pending.is_empty() {
            return Ok(());
        }
        let part_id = self.next_part_id;
        self.next_part_id += 1;
        let part_path = self.parts_dir.join(format!("part-{part_id:06}"));
        let mut writer =
            BlockStreamWriter::create(&part_path, esm_compress::zstd_codec::DEFAULT_LEVEL)
                .map_err(|e| StorageError::CreatePart { path: part_path.clone(), source: e })?;

        // Drain the pending map and sort by TSID — the on-disk metaindex is
        // binary-searched by tsid, so blocks must be written in ascending tsid
        // order. Sorting once here is cheaper than keeping an ordered map hot
        // on every ingested sample (the ingest hot path).
        let pending = std::mem::take(&mut self.pending);
        self.pending_samples = 0;
        let mut pending: Vec<(Tsid, Vec<(i64, i64)>)> = pending.into_iter().collect();
        pending.sort_unstable_by_key(|&(tsid, _)| tsid);
        for (tsid, mut samples) in pending {
            samples.sort_by_key(|&(ts, _)| ts);
            // Time-series block size is bounded by MAX_ROWS_PER_BLOCK; split
            // into chunks.
            for chunk in samples.chunks(timeseries::MAX_ROWS_PER_BLOCK as usize) {
                let timestamps: Vec<i64> = chunk.iter().map(|&(t, _)| t).collect();
                let values: Vec<i64> = chunk.iter().map(|&(_, v)| v).collect();
                writer
                    .write_block(tsid, &timestamps, &values, DEFAULT_SCALE, DEFAULT_PRECISION_BITS)
                    .map_err(|e| StorageError::WriteBlock { path: part_path.clone(), source: e })?;
            }
        }
        writer
            .finish()
            .map_err(|e| StorageError::FinishPart { path: part_path.clone(), source: e })?;
        self.parts_index.push(PartMeta::load(part_path)?);
        // Opportunistically merge small parts. Keeps part count bounded so
        // read amplification stays sane.
        self.maybe_merge_small_parts()?;
        Ok(())
    }

    /// Size-tiered compaction tunables. Parts are bucketed by their size tier
    /// (`floor(log2(bytes))`); a tier is compacted once it holds at least
    /// [`MERGE_MIN_PARTS`] parts. Merging only *similar-sized* parts keeps a
    /// given byte from being rewritten more than ~`O(log N)` times, instead of
    /// the old "merge the 4 smallest on every flush" policy that repeatedly
    /// re-merged a growing part with tiny new ones (heavy write amplification
    /// that dominated bulk-ingest cost).
    const MERGE_MIN_PARTS: usize = 4;
    /// Cap parts per merge so a single compaction's in-memory working set
    /// stays bounded regardless of how full a tier gets.
    const MERGE_MAX_BATCH: usize = 8;

    /// One-shot size-tiered merge. Idempotent; safe to call after every flush.
    /// Buckets parts by size tier and compacts the smallest tier that has
    /// accumulated at least [`Self::MERGE_MIN_PARTS`] parts (up to
    /// [`Self::MERGE_MAX_BATCH`] of them). Old parts are removed only after the
    /// new part is fsynced.
    ///
    /// # Errors
    /// Returns [`StorageError`] on any I/O failure during read, write, or
    /// removal.
    pub fn maybe_merge_small_parts(&mut self) -> Result<(), StorageError> {
        let entries = match std::fs::read_dir(&self.parts_dir) {
            Ok(it) => it,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        // Bucket parts by size tier = floor(log2(bytes)).
        let mut tiers: BTreeMap<u32, Vec<PathBuf>> = BTreeMap::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let p = entry.path();
            let size = part_byte_size(&p)?;
            let tier = 64 - size.max(1).leading_zeros();
            tiers.entry(tier).or_default().push(p);
        }
        // Compact the smallest tier that has enough parts. Only one tier per
        // call keeps each post-flush merge cheap; subsequent flushes drain the
        // rest. `BTreeMap` iterates tiers ascending, so smallest first.
        for group in tiers.into_values() {
            if group.len() >= Self::MERGE_MIN_PARTS {
                let batch: Vec<PathBuf> = group.into_iter().take(Self::MERGE_MAX_BATCH).collect();
                return self.merge_parts(&batch);
            }
        }
        Ok(())
    }

    /// Force a merge of `parts` into a single new part. Public so callers
    /// (and tests) can trigger compaction without waiting for the heuristic.
    ///
    /// # Errors
    /// Returns [`StorageError`] on read/write/delete failure.
    pub fn merge_parts(&mut self, parts: &[PathBuf]) -> Result<(), StorageError> {
        if parts.len() < 2 {
            return Ok(());
        }
        // Collect every block from every part, keyed by TSID, then rewrite.
        let mut collected: BTreeMap<Tsid, Vec<(i64, i64)>> = BTreeMap::new();
        for p in parts {
            let mut reader = BlockStreamReader::open(p)
                .map_err(|e| StorageError::OpenPart { path: p.clone(), source: e })?;
            while let Some(block) = reader
                .next_block()
                .map_err(|e| StorageError::ReadPart { path: p.clone(), source: e })?
            {
                let tsid = block.header.tsid;
                let entry = collected.entry(tsid).or_default();
                for (ts, v) in block.timestamps.iter().zip(block.values.iter()) {
                    entry.push((*ts, *v));
                }
            }
        }
        // Pick a new part id.
        let part_id = self.next_part_id;
        self.next_part_id += 1;
        let out_path = self.parts_dir.join(format!("part-{part_id:06}"));
        let mut writer =
            BlockStreamWriter::create(&out_path, esm_compress::zstd_codec::DEFAULT_LEVEL)
                .map_err(|e| StorageError::CreatePart { path: out_path.clone(), source: e })?;
        for (tsid, mut samples) in collected {
            samples.sort_by_key(|&(ts, _)| ts);
            for chunk in samples.chunks(timeseries::MAX_ROWS_PER_BLOCK as usize) {
                let ts: Vec<i64> = chunk.iter().map(|&(t, _)| t).collect();
                let vs: Vec<i64> = chunk.iter().map(|&(_, v)| v).collect();
                writer
                    .write_block(tsid, &ts, &vs, DEFAULT_SCALE, DEFAULT_PRECISION_BITS)
                    .map_err(|e| StorageError::WriteBlock { path: out_path.clone(), source: e })?;
            }
        }
        writer
            .finish()
            .map_err(|e| StorageError::FinishPart { path: out_path.clone(), source: e })?;
        // Remove input parts only after the output is fsynced.
        for p in parts {
            std::fs::remove_dir_all(p)?;
        }
        esm_platform::durability::fsync_dir(&self.parts_dir)?;
        // Keep the parts cache consistent: drop merged inputs, add the output.
        self.parts_index.retain(|m| !parts.contains(&m.path));
        self.parts_index.push(PartMeta::load(out_path)?);
        Ok(())
    }

    /// Create a snapshot: a hardlinked clone of the current parts directory
    /// at `<data_dir>/snapshots/<name>`. Returns the snapshot path. Matches
    /// VM's `/snapshot/create` semantics — the snapshot stays consistent
    /// regardless of subsequent merges or ingest because parts files are
    /// immutable (writes go to new part directories, and merges only delete
    /// after creating the new part).
    ///
    /// On platforms where hardlinks are unavailable (or across filesystems),
    /// falls back to byte-copy.
    ///
    /// # Errors
    /// Returns [`StorageError`] on I/O failure.
    pub fn create_snapshot(&mut self, name: &str) -> Result<PathBuf, StorageError> {
        // Flush pending data first so the snapshot reflects everything ingested.
        self.flush()?;
        let snapshots_root = self.data_dir.join("snapshots");
        std::fs::create_dir_all(&snapshots_root)?;
        let snap_dir = snapshots_root.join(name);
        if snap_dir.exists() {
            return Err(StorageError::SnapshotExists(snap_dir));
        }
        std::fs::create_dir_all(&snap_dir)?;
        // Copy whichever index file(s) exist (binary preferred, JSON as fallback).
        for name in [INDEX_BIN_FILENAME, INDEX_FILENAME] {
            let src = self.data_dir.join(name);
            if src.exists() {
                std::fs::copy(&src, snap_dir.join(name))?;
            }
        }
        // Hardlink every part file.
        let parts_dst = snap_dir.join("parts");
        std::fs::create_dir_all(&parts_dst)?;
        for entry in std::fs::read_dir(&self.parts_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let src_part = entry.path();
            let part_name = src_part.file_name().unwrap_or_default().to_owned();
            let dst_part = parts_dst.join(&part_name);
            std::fs::create_dir_all(&dst_part)?;
            for sub in std::fs::read_dir(&src_part)? {
                let sub = sub?;
                if !sub.file_type()?.is_file() {
                    continue;
                }
                let s = sub.path();
                let d = dst_part.join(sub.file_name());
                if std::fs::hard_link(&s, &d).is_err() {
                    std::fs::copy(&s, &d)?;
                }
            }
        }
        esm_platform::durability::fsync_dir(&snap_dir)?;
        Ok(snap_dir)
    }

    /// Delete a previously-created snapshot by name. No-op if absent.
    ///
    /// # Errors
    /// Returns [`StorageError`] on I/O failure.
    pub fn delete_snapshot(&self, name: &str) -> Result<(), StorageError> {
        let p = self.data_dir.join("snapshots").join(name);
        if p.exists() {
            std::fs::remove_dir_all(p)?;
        }
        Ok(())
    }

    /// Drop snapshots whose directory mtime is older than `cutoff_ms`.
    /// Returns the number of snapshots removed.
    ///
    /// # Errors
    /// Returns [`StorageError`] on I/O failure.
    pub fn enforce_snapshot_retention(&self, cutoff_ms: i64) -> Result<usize, StorageError> {
        let root = self.data_dir.join("snapshots");
        if !root.exists() {
            return Ok(0);
        }
        let mut removed = 0_usize;
        for entry in std::fs::read_dir(&root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let mtime = entry.metadata()?.modified()?;
            let mtime_ms = i64::try_from(
                mtime.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis(),
            )
            .unwrap_or(i64::MAX);
            if mtime_ms < cutoff_ms {
                std::fs::remove_dir_all(entry.path())?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// List snapshot names.
    ///
    /// # Errors
    /// Returns [`StorageError`] on I/O failure.
    pub fn list_snapshots(&self) -> Result<Vec<String>, StorageError> {
        let root = self.data_dir.join("snapshots");
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && let Some(name) = entry.file_name().to_str()
            {
                out.push(name.to_string());
            }
        }
        out.sort();
        Ok(out)
    }

    /// Drop every part whose newest sample is older than `cutoff_ms`.
    /// Conservative: a part is only removed if **all** of its blocks are
    /// fully below the cutoff. Partial retention (rewriting parts to drop
    /// individual stale blocks) is left for a later compaction pass.
    ///
    /// Returns the number of parts removed.
    ///
    /// # Errors
    /// Returns [`StorageError`] on I/O failure or part corruption.
    pub fn enforce_retention(&mut self, cutoff_ms: i64) -> Result<usize, StorageError> {
        // Use the cached per-part time ranges to find fully-stale parts without
        // re-reading every block. A part is dropped only if its newest sample
        // is below the cutoff.
        let stale: Vec<PathBuf> = self
            .parts_index
            .iter()
            .filter(|m| m.max_ts < cutoff_ms)
            .map(|m| m.path.clone())
            .collect();
        for p in &stale {
            std::fs::remove_dir_all(p)?;
        }
        if !stale.is_empty() {
            self.parts_index.retain(|m| !stale.contains(&m.path));
            esm_platform::durability::fsync_dir(&self.parts_dir)?;
        }
        Ok(stale.len())
    }

    /// Search for samples matching `metric_name` within `range`.
    /// Walks every part directory; suitable for Phase 1E correctness work
    /// but not for production scale (Phase 1A.7 merger will collapse parts).
    ///
    /// # Errors
    /// Returns [`StorageError`] on I/O failure or part corruption.
    pub fn search_by_metric_name(
        &self,
        metric_name: &[u8],
        range: TimeRange,
    ) -> Result<Vec<StoredSample>, StorageError> {
        // Resolve TSID via in-memory map. If unknown, return empty.
        let Some(tsid) = self.name_to_tsid.get(metric_name).copied() else {
            return Ok(Vec::new());
        };
        self.search_by_tsid(tsid, range)
    }

    /// Search for samples for a specific TSID within `range`.
    ///
    /// # Errors
    /// Returns [`StorageError`] on I/O failure or part corruption.
    pub fn search_by_tsid(
        &self,
        tsid: Tsid,
        range: TimeRange,
    ) -> Result<Vec<StoredSample>, StorageError> {
        // Accumulate every in-range sample into a flat Vec, in priority order:
        // parts oldest→newest (the `parts_index` order), then the in-memory
        // pending buffer last — so the newest write wins on duplicate
        // timestamps. A single part's blocks for one tsid arrive strictly
        // time-ordered and non-overlapping, so the overwhelmingly common case
        // (one contributing part, no pending) yields an already-sorted, unique
        // Vec needing no further work — avoiding a per-sample `BTreeMap` insert
        // (the dominant cost for heavy aggregations that read thousands of
        // samples per series). Only when timestamps actually arrive out of
        // order or duplicated (multiple overlapping parts / pending) do we pay
        // a single sort + dedup.
        let mut out: Vec<StoredSample> = Vec::new();
        let mut last_ts = i64::MIN;
        let mut needs_merge = false;
        // Iterate the cached part list (no per-query `read_dir`) and prune by
        // time range before opening anything.
        for meta in &self.parts_index {
            if meta.max_ts < range.min_timestamp_ms || meta.min_ts > range.max_timestamp_ms {
                continue;
            }
            let part_path = &meta.path;
            // Reuse the cached header + metaindex; opens only the data files.
            let mut reader = BlockStreamReader::open_with_index(
                part_path,
                meta.header.clone(),
                Arc::clone(&meta.metaindex),
            )
            .map_err(|e| StorageError::OpenPart { path: part_path.clone(), source: e })?;
            // Fast-forward to the index block whose start tsid <= target.
            reader.seek_to_tsid(tsid);
            while let Some(header) = reader
                .next_block_header()
                .map_err(|e| StorageError::ReadPart { path: part_path.clone(), source: e })?
            {
                // Blocks are sorted by (tsid, min_timestamp). Once we walk
                // past `tsid` we're done with this part.
                if header.tsid > tsid {
                    break;
                }
                if header.tsid != tsid {
                    continue;
                }
                if header.max_timestamp < range.min_timestamp_ms
                    || header.min_timestamp > range.max_timestamp_ms
                {
                    continue;
                }
                let (timestamps, values) = reader
                    .read_data_block_for(&header)
                    .map_err(|e| StorageError::ReadPart { path: part_path.clone(), source: e })?;
                for (ts, val) in timestamps.iter().zip(values.iter()) {
                    if *ts >= range.min_timestamp_ms && *ts <= range.max_timestamp_ms {
                        if *ts <= last_ts {
                            needs_merge = true;
                        }
                        last_ts = *ts;
                        out.push(StoredSample { timestamp_ms: *ts, value: *val });
                    }
                }
            }
        }
        // Overlay in-memory pending samples for this TSID.
        if let Some(buf) = self.pending.get(&tsid) {
            for &(ts, v) in buf {
                if ts >= range.min_timestamp_ms && ts <= range.max_timestamp_ms {
                    if ts <= last_ts {
                        needs_merge = true;
                    }
                    last_ts = ts;
                    out.push(StoredSample { timestamp_ms: ts, value: v });
                }
            }
        }
        if needs_merge {
            // Stable sort keeps equal-timestamp samples in push (priority)
            // order; the dedup below then keeps the last (newest) per timestamp.
            out.sort_by_key(|s| s.timestamp_ms);
            let mut w = 0;
            for r in 0..out.len() {
                if w > 0 && out[w - 1].timestamp_ms == out[r].timestamp_ms {
                    out[w - 1] = out[r];
                } else {
                    out[w] = out[r];
                    w += 1;
                }
            }
            out.truncate(w);
        }
        Ok(out)
    }

    /// Look up the TSID for an already-ingested metric, if known.
    #[must_use]
    pub fn lookup_tsid(&self, metric_name: &[u8]) -> Option<Tsid> {
        self.name_to_tsid.get(metric_name).copied()
    }

    /// Number of distinct metric names known to the engine.
    #[must_use]
    pub fn metric_count(&self) -> usize {
        self.name_to_tsid.len()
    }

    /// Iterate every known `(metric_name, tsid)` pair. Materialises the
    /// list because the underlying map is held mutably during ingestion;
    /// the result is fine for query-time scans at Phase 1E MVP scale. A
    /// streaming indexdb scan replaces this once Phase 1D persists the
    /// label-aware index.
    #[must_use]
    pub fn iter_metric_names(&self) -> Vec<(Vec<u8>, Tsid)> {
        self.name_to_tsid.iter().map(|(k, v)| (k.clone(), *v)).collect()
    }

    /// Flush any pending data and release the data-directory lock.
    ///
    /// # Errors
    /// See [`StorageError`].
    pub fn shutdown(mut self) -> Result<(), StorageError> {
        self.flush()?;
        Ok(())
    }

    /// Data directory the engine is using.
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("data directory {0} is already locked by another process")]
    DataDirLocked(PathBuf),
    #[error("snapshot already exists at {0}")]
    SnapshotExists(PathBuf),
    #[error("malformed index.json: {0}")]
    IndexParse(serde_json::Error),
    #[error("cannot serialise index.json: {0}")]
    IndexSerialise(serde_json::Error),
    #[error("malformed hex in index.json: {0}")]
    IndexHex(&'static str),
    #[error("index.json schema version {got} is not supported; need {want}")]
    IndexSchemaVersion { got: u32, want: u32 },
    #[error("cannot open part {path}: {source}")]
    OpenPart {
        path: PathBuf,
        #[source]
        source: timeseries::block_stream_reader::ReadError,
    },
    #[error("cannot read part {path}: {source}")]
    ReadPart {
        path: PathBuf,
        #[source]
        source: timeseries::block_stream_reader::ReadError,
    },
    #[error("cannot create part {path}: {source}")]
    CreatePart { path: PathBuf, source: std::io::Error },
    #[error("cannot write block to {path}: {source}")]
    WriteBlock {
        path: PathBuf,
        #[source]
        source: timeseries::block_stream_writer::WriteError,
    },
    #[error("cannot finish part {path}: {source}")]
    FinishPart {
        path: PathBuf,
        #[source]
        source: timeseries::block_stream_writer::WriteError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_close_empty_data_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let s = Storage::open(tmp.path().join("d")).unwrap();
        assert_eq!(s.metric_count(), 0);
        s.shutdown().unwrap();
    }

    #[test]
    fn double_open_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("d");
        let _s1 = Storage::open(&d).unwrap();
        let r = Storage::open(&d);
        assert!(matches!(r, Err(StorageError::DataDirLocked(_))));
    }

    #[test]
    fn ingest_flush_search_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = Storage::open(tmp.path().join("d")).unwrap();

        let samples = [
            Sample { metric_name: b"http_requests_total".to_vec(), timestamp_ms: 100, value: 1 },
            Sample { metric_name: b"http_requests_total".to_vec(), timestamp_ms: 200, value: 2 },
            Sample { metric_name: b"http_requests_total".to_vec(), timestamp_ms: 300, value: 3 },
            Sample { metric_name: b"cpu_seconds_total".to_vec(), timestamp_ms: 100, value: 50 },
            Sample { metric_name: b"cpu_seconds_total".to_vec(), timestamp_ms: 200, value: 75 },
        ];
        s.ingest(&samples).unwrap();
        assert_eq!(s.metric_count(), 2);
        s.flush().unwrap();
        assert!(s.lookup_tsid(b"http_requests_total").is_some());

        // Query a window that includes everything.
        let hits = s
            .search_by_metric_name(
                b"http_requests_total",
                TimeRange { min_timestamp_ms: 0, max_timestamp_ms: 1_000 },
            )
            .unwrap();
        assert_eq!(hits.len(), 3);
        assert_eq!(hits[0].value, 1);
        assert_eq!(hits[2].value, 3);

        // Window that excludes the first sample.
        let hits = s
            .search_by_metric_name(
                b"http_requests_total",
                TimeRange { min_timestamp_ms: 150, max_timestamp_ms: 1_000 },
            )
            .unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].timestamp_ms, 200);
    }

    #[test]
    #[ignore = "manual micro-benchmark; run with --ignored --nocapture --release"]
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation, clippy::print_stderr)]
    fn bench_flush_write_scaling() {
        use std::time::Instant;
        // Same total points (~2M), different series shapes. Isolates pure
        // flush-write cost (single part => merger never triggers).
        for (series, pts) in [(10_000usize, 200usize), (1_000, 2_000), (100, 20_000)] {
            let tmp = tempfile::tempdir().unwrap();
            let mut s = Storage::open(tmp.path().join("d")).unwrap();
            for sidx in 0..series {
                let tsid = Tsid { metric_id: sidx as u64 + 1, ..Default::default() };
                // Realistic shape: large epoch-ms timestamps (10s step) and
                // pseudo-random small values (like cpu %), which don't collapse
                // to const/delta encodings.
                s.pending.insert(
                    tsid,
                    (0..pts as i64)
                        .map(|i| {
                            let ts = 1_704_067_200_000 + i * 10_000;
                            let v = (i.wrapping_mul(2_654_435_761)).rem_euclid(100);
                            (ts, v)
                        })
                        .collect(),
                );
            }
            s.pending_samples = series * pts;
            let start = Instant::now();
            s.flush().unwrap();
            eprintln!(
                "FLUSH series={series:>6} pts/series={pts:>6} total={:>9} took={:?}",
                series * pts,
                start.elapsed()
            );
        }
    }

    #[test]
    fn metric_name_index_groups_series_by_name() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = Storage::open(tmp.path().join("d")).unwrap();
        s.ingest(&[
            Sample { metric_name: b"cpu{host=\"a\"}".to_vec(), timestamp_ms: 1, value: 1 },
            Sample { metric_name: b"cpu{host=\"b\"}".to_vec(), timestamp_ms: 1, value: 2 },
            Sample { metric_name: b"mem{host=\"a\"}".to_vec(), timestamp_ms: 1, value: 3 },
        ])
        .unwrap();
        let mut cpu = s.series_for_metric_name(b"cpu");
        cpu.sort();
        assert_eq!(cpu, vec![b"cpu{host=\"a\"}".to_vec(), b"cpu{host=\"b\"}".to_vec()]);
        assert_eq!(s.series_for_metric_name(b"mem").len(), 1);
        assert!(s.series_for_metric_name(b"nope").is_empty());
        let mut names = s.distinct_metric_names();
        names.sort();
        assert_eq!(names, vec![b"cpu".to_vec(), b"mem".to_vec()]);
        // Label index: host="a" carries both cpu and mem series.
        let mut by_host_a = s.series_for_label(b"host", b"a");
        by_host_a.sort();
        assert_eq!(by_host_a, vec![b"cpu{host=\"a\"}".to_vec(), b"mem{host=\"a\"}".to_vec()]);
        assert_eq!(s.series_for_label(b"host", b"b"), vec![b"cpu{host=\"b\"}".to_vec()]);
        assert!(s.series_for_label(b"host", b"z").is_empty());
        // Survives reopen (rebuilt from the persisted name map).
        s.flush().unwrap();
        s.shutdown().unwrap();
        let s2 = Storage::open(tmp.path().join("d")).unwrap();
        assert_eq!(s2.series_for_metric_name(b"cpu").len(), 2);
    }

    #[test]
    fn search_sees_unflushed_pending_data() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = Storage::open(tmp.path().join("d")).unwrap();
        s.ingest(&[
            Sample { metric_name: b"m".to_vec(), timestamp_ms: 100, value: 1 },
            Sample { metric_name: b"m".to_vec(), timestamp_ms: 200, value: 2 },
        ])
        .unwrap();
        // No flush: query must still see the buffered samples.
        let hits = s
            .search_by_metric_name(b"m", TimeRange { min_timestamp_ms: 0, max_timestamp_ms: 1_000 })
            .unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].timestamp_ms, 100);
        assert_eq!(hits[1].value, 2);
    }

    #[test]
    fn search_merges_disk_and_pending_pending_wins_on_ties() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = Storage::open(tmp.path().join("d")).unwrap();
        // Flushed to disk: ts 100=1, 200=2.
        s.ingest(&[
            Sample { metric_name: b"m".to_vec(), timestamp_ms: 100, value: 1 },
            Sample { metric_name: b"m".to_vec(), timestamp_ms: 200, value: 2 },
        ])
        .unwrap();
        s.flush().unwrap();
        // Buffered (not flushed): a new ts 300=3, plus an overriding write at ts 200=99.
        s.ingest(&[
            Sample { metric_name: b"m".to_vec(), timestamp_ms: 300, value: 3 },
            Sample { metric_name: b"m".to_vec(), timestamp_ms: 200, value: 99 },
        ])
        .unwrap();
        let hits = s
            .search_by_metric_name(b"m", TimeRange { min_timestamp_ms: 0, max_timestamp_ms: 1_000 })
            .unwrap();
        // Sorted, deduped by timestamp; pending overrides disk at ts 200.
        assert_eq!(hits.len(), 3);
        assert_eq!(hits[0].timestamp_ms, 100);
        assert_eq!((hits[1].timestamp_ms, hits[1].value), (200, 99));
        assert_eq!((hits[2].timestamp_ms, hits[2].value), (300, 3));
    }

    #[test]
    fn binary_index_roundtrip_after_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("d");
        {
            let mut s = Storage::open(&d).unwrap();
            s.ingest(&[
                Sample { metric_name: b"alpha".to_vec(), timestamp_ms: 1, value: 1 },
                Sample { metric_name: b"beta".to_vec(), timestamp_ms: 1, value: 2 },
                Sample { metric_name: b"gamma".to_vec(), timestamp_ms: 1, value: 3 },
            ])
            .unwrap();
            s.shutdown().unwrap();
        }
        // Binary file was written, legacy JSON file was not.
        assert!(d.join(INDEX_BIN_FILENAME).exists(), "binary index must exist");
        assert!(!d.join(INDEX_FILENAME).exists(), "legacy JSON must be removed");

        // Reopen and confirm every name → TSID mapping survives.
        let s = Storage::open(&d).unwrap();
        assert_eq!(s.metric_count(), 3);
        assert!(s.lookup_tsid(b"alpha").is_some());
        assert!(s.lookup_tsid(b"beta").is_some());
        assert!(s.lookup_tsid(b"gamma").is_some());
    }

    #[test]
    fn legacy_json_index_is_migrated() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("d");
        std::fs::create_dir_all(&d).unwrap();
        std::fs::create_dir_all(d.join("ts_parts")).unwrap();
        // Hand-write a JSON sidecar (the old format).
        let json = serde_json::json!({
            "schema_version": 1,
            "next_metric_id": 7,
            "entries": [
                { "metric_id": 5, "name_hex": "616263" }, // "abc"
                { "metric_id": 6, "name_hex": "646566" }, // "def"
            ],
        });
        std::fs::write(d.join(INDEX_FILENAME), json.to_string()).unwrap();

        let mut s = Storage::open(&d).unwrap();
        assert!(s.lookup_tsid(b"abc").is_some());
        assert!(s.lookup_tsid(b"def").is_some());
        // Trigger a save: the JSON file should be deleted and the binary
        // index should appear.
        s.ingest(&[Sample { metric_name: b"ghi".to_vec(), timestamp_ms: 1, value: 1 }]).unwrap();
        s.flush().unwrap();
        assert!(d.join(INDEX_BIN_FILENAME).exists());
        assert!(!d.join(INDEX_FILENAME).exists());
    }

    #[test]
    fn snapshot_creates_hardlinked_copy() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = Storage::open(tmp.path().join("d")).unwrap();
        s.ingest(&[Sample { metric_name: b"m".to_vec(), timestamp_ms: 1, value: 1 }]).unwrap();
        s.flush().unwrap();
        let path = s.create_snapshot("a").unwrap();
        assert!(path.is_dir());
        // After the format migration the binary index is preferred; the
        // legacy JSON file is removed once it's been superseded.
        assert!(path.join(INDEX_BIN_FILENAME).exists());
        assert!(path.join("parts").is_dir());
        let names = s.list_snapshots().unwrap();
        assert_eq!(names, vec!["a".to_string()]);
        s.delete_snapshot("a").unwrap();
        assert!(s.list_snapshots().unwrap().is_empty());
    }

    #[test]
    fn retention_drops_old_parts_only() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = Storage::open(tmp.path().join("d")).unwrap();
        // Flush an "old" part.
        s.ingest(&[Sample { metric_name: b"old".to_vec(), timestamp_ms: 100, value: 1 }]).unwrap();
        s.flush().unwrap();
        // Flush a "new" part.
        s.ingest(&[Sample { metric_name: b"new".to_vec(), timestamp_ms: 1_000_000, value: 2 }])
            .unwrap();
        s.flush().unwrap();
        // Cutoff between them — should drop the old part only.
        let dropped = s.enforce_retention(500_000).unwrap();
        assert_eq!(dropped, 1);
        // The new metric is still queryable.
        let hits = s
            .search_by_metric_name(
                b"new",
                TimeRange { min_timestamp_ms: 0, max_timestamp_ms: i64::MAX },
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        // The old metric is gone from disk.
        let hits = s
            .search_by_metric_name(
                b"old",
                TimeRange { min_timestamp_ms: 0, max_timestamp_ms: i64::MAX },
            )
            .unwrap();
        assert_eq!(hits.len(), 0);
    }

    #[test]
    fn bootstrap_assigns_unique_tsids_after_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("d");
        {
            let mut s = Storage::open(&d).unwrap();
            s.ingest(&[Sample { metric_name: b"foo".to_vec(), timestamp_ms: 1, value: 1 }])
                .unwrap();
            s.shutdown().unwrap();
        }
        // Reopen and ingest a new metric. The new TSID must not collide
        // with the one persisted in the first session.
        let mut s = Storage::open(&d).unwrap();
        s.ingest(&[Sample { metric_name: b"bar".to_vec(), timestamp_ms: 1, value: 2 }]).unwrap();
        s.flush().unwrap();
        assert!(s.metric_count() >= 2);
    }

    #[test]
    fn merge_reduces_part_count() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("d");
        let mut s = Storage::open(&d).unwrap();
        // Push enough same-sized flushes to fill a size tier and trigger merges.
        for i in 0..10 {
            s.ingest(&[Sample {
                metric_name: format!("metric_{i}").into_bytes(),
                timestamp_ms: i64::from(i) * 1000,
                value: i64::from(i),
            }])
            .unwrap();
            s.flush().unwrap();
        }
        // After the auto-merge fires, the parts directory should have
        // strictly fewer than the 10 flushes we performed (some collapsed).
        let parts_dir = d.join("ts_parts");
        let part_count = std::fs::read_dir(&parts_dir)
            .unwrap()
            .filter(|e| e.as_ref().is_ok_and(|e| e.file_type().unwrap().is_dir()))
            .count();
        assert!(part_count < 10, "expected merger to collapse parts; got {part_count}");
        // And every metric is still queryable.
        let range = TimeRange { min_timestamp_ms: 0, max_timestamp_ms: 1_000_000 };
        for i in 0..10 {
            let hits = s.search_by_metric_name(format!("metric_{i}").as_bytes(), range).unwrap();
            assert_eq!(hits.len(), 1, "metric_{i} missing after merge");
        }
    }

    #[test]
    fn index_persists_metric_name_across_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("d");
        let original_tsid;
        {
            let mut s = Storage::open(&d).unwrap();
            s.ingest(&[Sample {
                metric_name: b"http_requests_total".to_vec(),
                timestamp_ms: 1,
                value: 99,
            }])
            .unwrap();
            original_tsid = s.lookup_tsid(b"http_requests_total").unwrap();
            s.shutdown().unwrap();
        }
        // Reopen — same metric name must resolve to the same TSID, and
        // searching by name returns the original sample (no placeholder).
        let s = Storage::open(&d).unwrap();
        let resolved = s.lookup_tsid(b"http_requests_total").unwrap();
        assert_eq!(resolved, original_tsid);
        let hits = s
            .search_by_metric_name(
                b"http_requests_total",
                TimeRange { min_timestamp_ms: 0, max_timestamp_ms: 10 },
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].value, 99);
    }
}
