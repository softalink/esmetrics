//! In-memory rollup result cache. Port of `rollup_result_cache.go`
//! (series entries only; instant-values entries belong to the
//! `evalInstantRollup` family, which isn't ported yet).
//!
//! Successful rollup results are cached per `(expr string, window, step,
//! enforced tag filters)` under a two-level scheme:
//! - the **metainfo key** maps the query identity to up to 10
//!   `{start, end, data key}` entries;
//! - each **data key** `{prefix, suffix}` maps to a marshaled timeseries
//!   blob (shared timestamps + per-series values + metric names).
//!
//! On lookup the best entry covering `ec.start` is trimmed to the request
//! grid (timestamps must match the step grid exactly) and the caller only
//! evaluates the uncovered tail, merging via [`merge_series`]. On store,
//! points newer than `now - step - CACHE_TIMESTAMP_OFFSET_MS` are truncated
//! so possibly-still-arriving data is never cached; global O(1) invalidation
//! bumps the key prefix ([`reset_rollup_result_cache`], the analog of Go
//! `ResetRollupResultCache` used for backfill protection).
//!
//! The backing store is an in-memory workingset-style byte cache bounded by
//! `esm_common::memory::allowed() / 16` (the Go size constant); no file
//! persistence.

use crate::eval::EvalConfig;
use crate::timeseries::{marshal_metric_name_sorted, Timeseries};
use esm_metricsql::{Expr, LabelFilter};
use esm_storage::metric_name::{MetricName, Tag};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// Bump when the marshaled entry format changes (Go: `rollupResultCacheVersion`).
const ROLLUP_RESULT_CACHE_VERSION: u8 = 11;
const ROLLUP_RESULT_CACHE_TYPE_SERIES: u8 = 0;

/// `-search.cacheTimestampOffset` default: points younger than
/// `now - step - offset` are never cached, since they may still change.
const CACHE_TIMESTAMP_OFFSET_MS: i64 = 5 * 60 * 1000;

/// The maximum number of `{start, end, key}` entries per metainfo value;
/// on overflow the 5 oldest entries are dropped (Go behavior).
const MAX_METAINFO_ENTRIES: usize = 10;

/// The process-wide rollup result cache
/// (Go `rollupResultCacheV` + `InitRollupResultCache`, in-memory flavor).
pub fn rollup_result_cache() -> &'static RollupResultCache {
    static CACHE: OnceLock<RollupResultCache> = OnceLock::new();
    CACHE.get_or_init(|| {
        let mut n = esm_common::memory::allowed() / 16;
        if n == 0 {
            n = 1024 * 1024;
        }
        RollupResultCache::new(n)
    })
}

/// Drops all cached rollup results in O(1) by bumping the key prefix.
/// Port of Go `ResetRollupResultCache`; call when freshly ingested samples
/// are older than the cache timestamp offset (backfill).
pub fn reset_rollup_result_cache() {
    rollup_result_cache().reset();
}

/// Port of the Go `rollupResultCache` struct.
pub struct RollupResultCache {
    cache: ByteCache,
    key_prefix: AtomicU64,
    key_suffix: AtomicU64,
}

impl RollupResultCache {
    /// A cache bounded by `max_size` bytes.
    pub fn new(max_size: usize) -> Self {
        let seed = coarse_random_seed();
        RollupResultCache {
            cache: ByteCache::new(max_size),
            key_prefix: AtomicU64::new(seed),
            key_suffix: AtomicU64::new(now_unix_ms() as u64),
        }
    }

    /// O(1) invalidation of every entry.
    pub fn reset(&self) {
        self.key_prefix.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns cached series trimmed to `[ec.start .. ec.end]` plus the
    /// `new_start` from which the caller must evaluate the remaining tail.
    /// `new_start == ec.start` (with no series) means a full miss;
    /// `new_start > ec.end` means the result is fully cached.
    /// Port of Go `rollupResultCache.GetSeries`.
    pub fn get_series(&self, ec: &EvalConfig, expr: &Expr, window: i64) -> (Vec<Timeseries>, i64) {
        let miss = (Vec::new(), ec.start);
        let metainfo_key = self.marshal_metainfo_key(ec, expr, window);
        let Some(metainfo_buf) = self.cache.get(&metainfo_key) else {
            return miss;
        };
        let Some(mut mi) = Metainfo::unmarshal(&metainfo_buf) else {
            // Improperly saved metainfo (Go panics; treat as a miss).
            return miss;
        };
        let Some(key) = mi.get_best_key(ec.start, ec.end) else {
            return miss;
        };
        let Some(data_buf) = self.cache.get(&key.marshal()) else {
            // The data entry was evicted; drop the dangling metainfo key.
            mi.remove_key(key);
            self.cache.set(metainfo_key, mi.marshal());
            return miss;
        };
        let Some(tss) = unmarshal_timeseries_fast(&data_buf) else {
            return miss;
        };
        if tss.is_empty() {
            return miss;
        }

        // Extract values for the matching timestamps; the cached grid must
        // contain ec.start exactly (mayCache alignment guarantees the grids
        // are compatible).
        let timestamps = &tss[0].timestamps;
        let mut i = 0;
        while i < timestamps.len() && timestamps[i] < ec.start {
            i += 1;
        }
        if i == timestamps.len() || timestamps[i] != ec.start {
            return miss;
        }
        let mut j = timestamps.len();
        while j > 0 && timestamps[j - 1] > ec.end {
            j -= 1;
        }
        if j <= i {
            return miss;
        }

        let trimmed: Arc<Vec<i64>> = Arc::new(timestamps[i..j].to_vec());
        let new_start = *trimmed.last().expect("j > i") + ec.step;
        let tss = tss
            .into_iter()
            .map(|ts| Timeseries {
                metric_name: ts.metric_name,
                values: ts.values[i..j].to_vec(),
                timestamps: Arc::clone(&trimmed),
            })
            .collect();
        (tss, new_start)
    }

    /// Stores the rollup result for future partial reuse.
    /// Port of Go `rollupResultCache.PutSeries`.
    pub fn put_series(&self, ec: &EvalConfig, expr: &Expr, window: i64, tss: &[Timeseries]) {
        if tss.is_empty() {
            return;
        }

        if tss.len() > 1 {
            // Series with duplicate naming cannot be merged in merge_series
            // later, so there is no sense in caching them.
            let mut seen: HashSet<Vec<u8>> = HashSet::with_capacity(tss.len());
            let mut key_buf = Vec::with_capacity(64);
            for ts in tss {
                key_buf.clear();
                let mut mn = ts.metric_name.clone();
                marshal_metric_name_sorted(&mut key_buf, &mut mn);
                if !seen.insert(key_buf.clone()) {
                    return;
                }
            }
        }

        // Remove values up to `now - step - cacheTimestampOffset`, since
        // these values may still be overwritten by incoming samples.
        let timestamps = &tss[0].timestamps;
        let deadline = now_unix_ms() - ec.step - CACHE_TIMESTAMP_OFFSET_MS;
        let mut n_points = timestamps.len();
        while n_points > 0 && timestamps[n_points - 1] > deadline {
            n_points -= 1;
        }
        if n_points == 0 {
            // Nothing to store in the cache.
            return;
        }

        let start = timestamps[0];
        let end = timestamps[n_points - 1];
        let metainfo_key = self.marshal_metainfo_key(ec, expr, window);
        let mut mi = self
            .cache
            .get(&metainfo_key)
            .and_then(|buf| Metainfo::unmarshal(&buf))
            .unwrap_or_default();
        if mi.covers_time_range(start, end) {
            return;
        }

        let max_marshaled_size = self.cache.max_size / 4;
        let Some(data_buf) = marshal_timeseries_fast(tss, n_points, max_marshaled_size) else {
            // Too big to cache.
            return;
        };
        let key = CacheKey {
            prefix: self.key_prefix.load(Ordering::Relaxed),
            suffix: self.key_suffix.fetch_add(1, Ordering::Relaxed) + 1,
        };
        self.cache.set(key.marshal(), data_buf);
        mi.add_key(key, start, end);
        self.cache.set(metainfo_key, mi.marshal());
    }

    /// Port of Go `marshalRollupResultCacheKeyForSeries`:
    /// `version ++ keyPrefix ++ type ++ window ++ step ++ etfs ++ expr`.
    fn marshal_metainfo_key(&self, ec: &EvalConfig, expr: &Expr, window: i64) -> Vec<u8> {
        let mut dst = Vec::with_capacity(64);
        dst.push(ROLLUP_RESULT_CACHE_VERSION);
        dst.extend_from_slice(&self.key_prefix.load(Ordering::Relaxed).to_le_bytes());
        dst.push(ROLLUP_RESULT_CACHE_TYPE_SERIES);
        dst.extend_from_slice(&window.to_le_bytes());
        dst.extend_from_slice(&ec.step.to_le_bytes());
        marshal_tag_filterss(&mut dst, &ec.enforced_tag_filterss);
        let mut s = String::new();
        expr.append_string(&mut s);
        dst.extend_from_slice(s.as_bytes());
        dst
    }
}

fn marshal_tag_filterss(dst: &mut Vec<u8>, etfs: &[Vec<LabelFilter>]) {
    let mut s = String::new();
    for (i, etf) in etfs.iter().enumerate() {
        if i > 0 {
            s.push('|');
        }
        for f in etf {
            f.append_string(&mut s);
            s.push(',');
        }
    }
    dst.extend_from_slice(s.as_bytes());
}

/// Merges cached series `a` (covering `[ec.start .. b_start - step]`) with
/// freshly evaluated series `b` (covering `[b_start .. ec.end]`), filling
/// NaN runs for series present on only one side. Returns `None` when the
/// sides cannot be merged (duplicate or misaligned series) — the caller must
/// fall back to a full re-evaluation. Port of Go `mergeSeries`.
pub(crate) fn merge_series(
    a: Vec<Timeseries>,
    b: Vec<Timeseries>,
    b_start: i64,
    ec: &EvalConfig,
) -> Option<Vec<Timeseries>> {
    let shared_timestamps = ec.shared_timestamps();
    let mut i = 0;
    while i < shared_timestamps.len() && shared_timestamps[i] < b_start {
        i += 1;
    }
    let a_timestamps = &shared_timestamps[..i];
    let b_timestamps = &shared_timestamps[i..];

    if b_timestamps.len() == shared_timestamps.len() {
        // Nothing to merge — return b as is.
        let mut b = b;
        for ts_b in &mut b {
            if ts_b.timestamps.as_slice() != b_timestamps {
                return None;
            }
            ts_b.timestamps = Arc::clone(&shared_timestamps);
        }
        return Some(b);
    }

    let mut m_a: HashMap<Vec<u8>, Timeseries> = HashMap::with_capacity(a.len());
    for mut ts in a {
        if ts.timestamps.as_slice() != a_timestamps {
            return None;
        }
        let mut key = Vec::with_capacity(64);
        marshal_metric_name_sorted(&mut key, &mut ts.metric_name);
        if m_a.insert(key, ts).is_some() {
            // a contains duplicate series.
            return None;
        }
    }

    let mut m_b: HashSet<Vec<u8>> = HashSet::with_capacity(b.len());
    let mut rvs: Vec<Timeseries> = Vec::with_capacity(m_a.len().max(b.len()));
    for mut ts_b in b {
        if ts_b.timestamps.as_slice() != b_timestamps {
            return None;
        }
        let mut key = Vec::with_capacity(64);
        marshal_metric_name_sorted(&mut key, &mut ts_b.metric_name);
        if !m_b.insert(key.clone()) {
            // b contains duplicate series.
            return None;
        }

        let mut values: Vec<f64> = Vec::with_capacity(shared_timestamps.len());
        match m_a.remove(&key) {
            Some(ts_a) => values.extend_from_slice(&ts_a.values),
            None => values.resize(a_timestamps.len(), f64::NAN),
        }
        values.extend_from_slice(&ts_b.values);
        rvs.push(Timeseries {
            metric_name: ts_b.metric_name,
            values,
            timestamps: Arc::clone(&shared_timestamps),
        });
    }

    // Copy the remaining series present only in a, padding the tail with NaN.
    for (_, ts_a) in m_a {
        let mut values: Vec<f64> = Vec::with_capacity(shared_timestamps.len());
        values.extend_from_slice(&ts_a.values);
        values.resize(shared_timestamps.len(), f64::NAN);
        rvs.push(Timeseries {
            metric_name: ts_a.metric_name,
            values,
            timestamps: Arc::clone(&shared_timestamps),
        });
    }
    Some(rvs)
}

// --- Two-level metainfo -----------------------------------------------------

/// Data-entry key; unique per stored blob. Port of `rollupResultCacheKey`.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct CacheKey {
    prefix: u64,
    suffix: u64,
}

impl CacheKey {
    fn marshal(&self) -> Vec<u8> {
        let mut dst = Vec::with_capacity(17);
        dst.push(ROLLUP_RESULT_CACHE_VERSION);
        dst.extend_from_slice(&self.prefix.to_le_bytes());
        dst.extend_from_slice(&self.suffix.to_le_bytes());
        dst
    }
}

/// Port of `rollupResultCacheMetainfo` (+Entry): the list of cached time
/// ranges for one query identity.
#[derive(Default)]
struct Metainfo {
    entries: Vec<MetainfoEntry>,
}

struct MetainfoEntry {
    start: i64,
    end: i64,
    key: CacheKey,
}

impl Metainfo {
    fn marshal(&self) -> Vec<u8> {
        let mut dst = Vec::with_capacity(4 + self.entries.len() * 32);
        dst.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for e in &self.entries {
            dst.extend_from_slice(&e.start.to_le_bytes());
            dst.extend_from_slice(&e.end.to_le_bytes());
            dst.extend_from_slice(&e.key.prefix.to_le_bytes());
            dst.extend_from_slice(&e.key.suffix.to_le_bytes());
        }
        dst
    }

    fn unmarshal(src: &[u8]) -> Option<Metainfo> {
        let n = u32::from_le_bytes(src.get(..4)?.try_into().ok()?) as usize;
        let mut src = &src[4..];
        if src.len() != n * 32 {
            return None;
        }
        let mut entries = Vec::with_capacity(n);
        for _ in 0..n {
            entries.push(MetainfoEntry {
                start: i64::from_le_bytes(src[..8].try_into().unwrap()),
                end: i64::from_le_bytes(src[8..16].try_into().unwrap()),
                key: CacheKey {
                    prefix: u64::from_le_bytes(src[16..24].try_into().unwrap()),
                    suffix: u64::from_le_bytes(src[24..32].try_into().unwrap()),
                },
            });
            src = &src[32..];
        }
        Some(Metainfo { entries })
    }

    fn covers_time_range(&self, start: i64, end: i64) -> bool {
        self.entries
            .iter()
            .any(|e| start >= e.start && end <= e.end)
    }

    /// The entry with `e.start <= start` maximizing the covered span.
    fn get_best_key(&self, start: i64, end: i64) -> Option<CacheKey> {
        let mut best: Option<CacheKey> = None;
        let mut d_max = 0i64;
        for e in &self.entries {
            if start < e.start {
                continue;
            }
            let d = if end <= e.end {
                end - start
            } else {
                e.end - start
            };
            if d >= d_max {
                d_max = d;
                best = Some(e.key);
            }
        }
        best
    }

    fn add_key(&mut self, key: CacheKey, start: i64, end: i64) {
        self.entries.push(MetainfoEntry { start, end, key });
        if self.entries.len() > MAX_METAINFO_ENTRIES {
            // Remove the oldest half.
            self.entries.drain(..MAX_METAINFO_ENTRIES / 2);
        }
    }

    fn remove_key(&mut self, key: CacheKey) {
        if let Some(i) = self.entries.iter().position(|e| e.key == key) {
            self.entries.remove(i);
        }
    }
}

// --- Timeseries blob format --------------------------------------------------

/// Marshals `tss` truncated to the first `n_points` points. All series must
/// share identical timestamps. Returns `None` when the marshaled size would
/// exceed `max_size`. Port of Go `marshalTimeseriesFast` (layout kept:
/// u64 series count ++ u64 point count ++ timestamps ++ per-series values ++
/// per-series metric names).
fn marshal_timeseries_fast(
    tss: &[Timeseries],
    n_points: usize,
    max_size: usize,
) -> Option<Vec<u8>> {
    let mut size = 16 + 8 * n_points + 8 * tss.len() * n_points;
    for ts in tss {
        debug_assert_eq!(
            ts.timestamps.as_slice(),
            tss[0].timestamps.as_slice(),
            "BUG: all series in a cached rollup result must share timestamps"
        );
        size += marshaled_metric_name_size(&ts.metric_name);
    }
    if size > max_size {
        return None;
    }

    let mut dst = Vec::with_capacity(size);
    dst.extend_from_slice(&(tss.len() as u64).to_le_bytes());
    dst.extend_from_slice(&(n_points as u64).to_le_bytes());
    for &ts in &tss[0].timestamps[..n_points] {
        dst.extend_from_slice(&ts.to_le_bytes());
    }
    for ts in tss {
        for &v in &ts.values[..n_points] {
            // Bit-exact round trip (stale-marker NaN patterns included).
            dst.extend_from_slice(&v.to_bits().to_le_bytes());
        }
    }
    for ts in tss {
        marshal_metric_name_fast(&mut dst, &ts.metric_name);
    }
    Some(dst)
}

/// Port of Go `unmarshalTimeseriesFast`; all series share one timestamps
/// allocation. Returns `None` on any format inconsistency.
fn unmarshal_timeseries_fast(src: &[u8]) -> Option<Vec<Timeseries>> {
    let tss_len = u64::from_le_bytes(src.get(..8)?.try_into().ok()?) as usize;
    let n_points = u64::from_le_bytes(src.get(8..16)?.try_into().ok()?) as usize;
    let mut src = src.get(16..)?;

    if src.len() < 8 * n_points {
        return None;
    }
    let mut timestamps = Vec::with_capacity(n_points);
    for chunk in src[..8 * n_points].chunks_exact(8) {
        timestamps.push(i64::from_le_bytes(chunk.try_into().unwrap()));
    }
    let timestamps = Arc::new(timestamps);
    src = &src[8 * n_points..];

    let mut tss = Vec::with_capacity(tss_len);
    for _ in 0..tss_len {
        if src.len() < 8 * n_points {
            return None;
        }
        let mut values = Vec::with_capacity(n_points);
        for chunk in src[..8 * n_points].chunks_exact(8) {
            values.push(f64::from_bits(u64::from_le_bytes(
                chunk.try_into().unwrap(),
            )));
        }
        src = &src[8 * n_points..];
        tss.push(Timeseries {
            metric_name: MetricName::default(),
            values,
            timestamps: Arc::clone(&timestamps),
        });
    }
    for ts in &mut tss {
        src = unmarshal_metric_name_fast(&mut ts.metric_name, src)?;
    }
    if !src.is_empty() {
        return None;
    }
    Some(tss)
}

fn marshaled_metric_name_size(mn: &MetricName) -> usize {
    let mut n = 2 + mn.metric_group.len() + 2;
    for tag in &mn.tags {
        n += 2 + tag.key.len() + 2 + tag.value.len();
    }
    n
}

fn marshal_metric_name_fast(dst: &mut Vec<u8>, mn: &MetricName) {
    marshal_bytes_u16(dst, &mn.metric_group);
    dst.extend_from_slice(&(mn.tags.len() as u16).to_le_bytes());
    for tag in &mn.tags {
        marshal_bytes_u16(dst, &tag.key);
        marshal_bytes_u16(dst, &tag.value);
    }
}

fn unmarshal_metric_name_fast<'a>(mn: &mut MetricName, src: &'a [u8]) -> Option<&'a [u8]> {
    let (group, mut src) = unmarshal_bytes_u16(src)?;
    mn.metric_group = group.to_vec();
    let tags_len = u16::from_le_bytes(src.get(..2)?.try_into().ok()?) as usize;
    src = &src[2..];
    mn.tags = Vec::with_capacity(tags_len);
    for _ in 0..tags_len {
        let (key, tail) = unmarshal_bytes_u16(src)?;
        let (value, tail) = unmarshal_bytes_u16(tail)?;
        mn.tags.push(Tag {
            key: key.to_vec(),
            value: value.to_vec(),
        });
        src = tail;
    }
    Some(src)
}

fn marshal_bytes_u16(dst: &mut Vec<u8>, s: &[u8]) {
    dst.extend_from_slice(&(s.len() as u16).to_le_bytes());
    dst.extend_from_slice(s);
}

fn unmarshal_bytes_u16(src: &[u8]) -> Option<(&[u8], &[u8])> {
    let n = u16::from_le_bytes(src.get(..2)?.try_into().ok()?) as usize;
    let src = &src[2..];
    if src.len() < n {
        return None;
    }
    Some((&src[..n], &src[n..]))
}

// --- Byte cache ---------------------------------------------------------------

/// Size-bounded in-memory byte cache with two workingset generations:
/// when the current generation exceeds half the budget it becomes the
/// previous generation (entries hit there are promoted back). This keeps
/// total memory under `max_size` without per-entry LRU bookkeeping,
/// mirroring the `workingsetcache` behavior the Go cache is built on.
struct ByteCache {
    max_size: usize,
    inner: Mutex<ByteCacheInner>,
}

struct ByteCacheInner {
    curr: HashMap<Vec<u8>, Arc<Vec<u8>>>,
    prev: HashMap<Vec<u8>, Arc<Vec<u8>>>,
    curr_bytes: usize,
}

impl ByteCache {
    fn new(max_size: usize) -> Self {
        ByteCache {
            max_size,
            inner: Mutex::new(ByteCacheInner {
                curr: HashMap::new(),
                prev: HashMap::new(),
                curr_bytes: 0,
            }),
        }
    }

    fn get(&self, key: &[u8]) -> Option<Arc<Vec<u8>>> {
        let mut inner = self.inner.lock();
        if let Some(v) = inner.curr.get(key) {
            return Some(Arc::clone(v));
        }
        let v = inner.prev.remove(key)?;
        // Promote the entry to the current generation.
        Self::insert(&mut inner, self.max_size, key.to_vec(), Arc::clone(&v));
        Some(v)
    }

    fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut inner = self.inner.lock();
        Self::insert(&mut inner, self.max_size, key, Arc::new(value));
    }

    fn insert(inner: &mut ByteCacheInner, max_size: usize, key: Vec<u8>, value: Arc<Vec<u8>>) {
        let entry_bytes = key.len() + value.len() + 64;
        if inner.curr_bytes + entry_bytes > max_size / 2 {
            inner.prev = std::mem::take(&mut inner.curr);
            inner.curr_bytes = 0;
        }
        if let Some(old) = inner.curr.insert(key, value) {
            inner.curr_bytes = inner.curr_bytes.saturating_sub(old.len() + 64);
        }
        inner.curr_bytes += entry_bytes;
    }
}

// --- Misc helpers --------------------------------------------------------------

fn now_unix_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// A cheap random-ish seed for the key prefix (Go uses crypto/rand; the
/// prefix only needs to differ across restarts).
fn coarse_random_seed() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let addr = &nanos as *const _ as u64;
    nanos ^ addr.rotate_left(32) ^ ((std::process::id() as u64) << 17)
}

#[cfg(test)]
mod tests {
    //! Ports of `TestRollupResultCache` and `TestMergeSeries` from
    //! `rollup_result_cache_test.go`.

    use super::*;

    const NAN: f64 = f64::NAN;

    fn test_ec() -> EvalConfig {
        let mut ec = EvalConfig::new(1000, 2000, 200);
        ec.max_points_per_series = 10_000;
        ec.may_cache = true;
        ec
    }

    fn test_expr() -> Expr {
        esm_metricsql::parse(r#"avg_over_time(bar{aaa="xxx"}[1m])"#).unwrap()
    }

    fn ts(timestamps: &[i64], values: &[f64]) -> Timeseries {
        Timeseries {
            metric_name: MetricName::default(),
            values: values.to_vec(),
            timestamps: Arc::new(timestamps.to_vec()),
        }
    }

    fn named_ts(name: &str, timestamps: &[i64], values: &[f64]) -> Timeseries {
        let mut t = ts(timestamps, values);
        t.metric_name.metric_group = name.as_bytes().to_vec();
        t
    }

    #[track_caller]
    fn assert_tss_eq(got: &[Timeseries], want: &[Timeseries]) {
        assert_eq!(got.len(), want.len(), "series count mismatch");
        for (g, w) in got.iter().zip(want) {
            assert_eq!(g.metric_name.metric_group, w.metric_name.metric_group);
            assert_eq!(g.timestamps.as_slice(), w.timestamps.as_slice());
            assert_eq!(g.values.len(), w.values.len());
            for (i, (&gv, &wv)) in g.values.iter().zip(&w.values).enumerate() {
                assert!(
                    gv == wv || (gv.is_nan() && wv.is_nan()),
                    "value #{i}: got {gv}; want {wv}"
                );
            }
        }
    }

    const WINDOW: i64 = 456;

    #[test]
    fn get_series_on_empty_cache_misses() {
        let c = RollupResultCache::new(1024 * 1024);
        let ec = test_ec();
        let (tss, new_start) = c.get_series(&ec, &test_expr(), WINDOW);
        assert_eq!(new_start, ec.start);
        assert!(tss.is_empty());
    }

    #[test]
    fn start_overlap() {
        let c = RollupResultCache::new(1024 * 1024);
        let ec = test_ec();
        let e = test_expr();
        c.put_series(&ec, &e, WINDOW, &[ts(&[800, 1000, 1200], &[0.0, 1.0, 2.0])]);
        let (tss, new_start) = c.get_series(&ec, &e, WINDOW);
        assert_eq!(new_start, 1400);
        assert_tss_eq(&tss, &[ts(&[1000, 1200], &[1.0, 2.0])]);
    }

    #[test]
    fn end_overlap_misses() {
        let c = RollupResultCache::new(1024 * 1024);
        let ec = test_ec();
        let e = test_expr();
        c.put_series(
            &ec,
            &e,
            WINDOW,
            &[ts(&[1800, 2000, 2200, 2400], &[333.0, 0.0, 1.0, 2.0])],
        );
        let (tss, new_start) = c.get_series(&ec, &e, WINDOW);
        assert_eq!(new_start, 1000);
        assert!(tss.is_empty());
    }

    #[test]
    fn full_cover_inside_range_misses() {
        let c = RollupResultCache::new(1024 * 1024);
        let ec = test_ec();
        let e = test_expr();
        c.put_series(
            &ec,
            &e,
            WINDOW,
            &[ts(&[1200, 1400, 1600], &[0.0, 1.0, 2.0])],
        );
        let (tss, new_start) = c.get_series(&ec, &e, WINDOW);
        assert_eq!(new_start, 1000);
        assert!(tss.is_empty());
    }

    #[test]
    fn before_start_misses() {
        let c = RollupResultCache::new(1024 * 1024);
        let ec = test_ec();
        let e = test_expr();
        c.put_series(&ec, &e, WINDOW, &[ts(&[200, 400, 600], &[0.0, 1.0, 2.0])]);
        let (tss, new_start) = c.get_series(&ec, &e, WINDOW);
        assert_eq!(new_start, 1000);
        assert!(tss.is_empty());
    }

    #[test]
    fn after_end_misses() {
        let c = RollupResultCache::new(1024 * 1024);
        let ec = test_ec();
        let e = test_expr();
        c.put_series(
            &ec,
            &e,
            WINDOW,
            &[ts(&[2200, 2400, 2600], &[0.0, 1.0, 2.0])],
        );
        let (tss, new_start) = c.get_series(&ec, &e, WINDOW);
        assert_eq!(new_start, 1000);
        assert!(tss.is_empty());
    }

    #[test]
    fn bigger_than_start_end() {
        let c = RollupResultCache::new(1024 * 1024);
        let ec = test_ec();
        let e = test_expr();
        c.put_series(
            &ec,
            &e,
            WINDOW,
            &[ts(
                &[800, 1000, 1200, 1400, 1600, 1800, 2000, 2200],
                &[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
            )],
        );
        let (tss, new_start) = c.get_series(&ec, &e, WINDOW);
        assert_eq!(new_start, 2200);
        assert_tss_eq(
            &tss,
            &[ts(
                &[1000, 1200, 1400, 1600, 1800, 2000],
                &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            )],
        );
    }

    #[test]
    fn start_end_match() {
        let c = RollupResultCache::new(1024 * 1024);
        let ec = test_ec();
        let e = test_expr();
        let series = ts(
            &[1000, 1200, 1400, 1600, 1800, 2000],
            &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        );
        c.put_series(&ec, &e, WINDOW, std::slice::from_ref(&series));
        let (tss, new_start) = c.get_series(&ec, &e, WINDOW);
        assert_eq!(new_start, 2200);
        assert_tss_eq(&tss, &[series]);
    }

    #[test]
    fn big_timeseries() {
        let c = RollupResultCache::new(1024 * 1024);
        let ec = test_ec();
        let e = test_expr();
        let tss: Vec<Timeseries> = (0..1000)
            .map(|i| {
                named_ts(
                    &format!("metric {i}"),
                    &[1000, 1200, 1400, 1600, 1800, 2000],
                    &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                )
            })
            .collect();
        c.put_series(&ec, &e, WINDOW, &tss);
        let (got, new_start) = c.get_series(&ec, &e, WINDOW);
        assert_eq!(new_start, 2200);
        assert_tss_eq(&got, &tss);
    }

    #[test]
    fn duplicate_series_not_stored() {
        let c = RollupResultCache::new(1024 * 1024);
        let ec = test_ec();
        let e = test_expr();
        c.put_series(
            &ec,
            &e,
            WINDOW,
            &[
                ts(&[800, 1000, 1200], &[0.0, 1.0, 2.0]),
                ts(&[800, 1000, 1200], &[0.0, 1.0, 2.0]),
            ],
        );
        let (tss, new_start) = c.get_series(&ec, &e, WINDOW);
        assert_eq!(new_start, ec.start);
        assert!(tss.is_empty());
    }

    #[test]
    fn multi_entries_pick_best() {
        let c = RollupResultCache::new(1024 * 1024);
        let ec = test_ec();
        let e = test_expr();
        c.put_series(&ec, &e, WINDOW, &[ts(&[800, 1000, 1200], &[0.0, 1.0, 2.0])]);
        c.put_series(
            &ec,
            &e,
            WINDOW,
            &[ts(&[1800, 2000, 2200, 2400], &[333.0, 0.0, 1.0, 2.0])],
        );
        c.put_series(
            &ec,
            &e,
            WINDOW,
            &[ts(&[1200, 1400, 1600], &[0.0, 1.0, 2.0])],
        );
        let (tss, new_start) = c.get_series(&ec, &e, WINDOW);
        assert_eq!(new_start, 1400);
        assert_tss_eq(&tss, &[ts(&[1000, 1200], &[1.0, 2.0])]);
    }

    #[test]
    fn key_isolation_by_window_step_and_expr() {
        let c = RollupResultCache::new(1024 * 1024);
        let ec = test_ec();
        let e = test_expr();
        c.put_series(&ec, &e, WINDOW, &[ts(&[800, 1000, 1200], &[0.0, 1.0, 2.0])]);
        // Different window.
        let (tss, _) = c.get_series(&ec, &e, WINDOW + 1);
        assert!(tss.is_empty());
        // Different expr.
        let e2 = esm_metricsql::parse(r#"bar{aaa="yyy"}"#).unwrap();
        let (tss, _) = c.get_series(&ec, &e2, WINDOW);
        assert!(tss.is_empty());
        // Different step.
        let mut ec2 = test_ec();
        ec2.step = 100;
        let (tss, _) = c.get_series(&ec2, &e, WINDOW);
        assert!(tss.is_empty());
        // Different enforced tag filters.
        let mut ec3 = test_ec();
        ec3.enforced_tag_filterss = vec![vec![LabelFilter {
            label: "tenant".to_string(),
            value: "1".to_string(),
            is_negative: false,
            is_regexp: false,
        }]];
        let (tss, _) = c.get_series(&ec3, &e, WINDOW);
        assert!(tss.is_empty());
        // The original key still hits.
        let (tss, new_start) = c.get_series(&ec, &e, WINDOW);
        assert_eq!(new_start, 1400);
        assert_eq!(tss.len(), 1);
    }

    #[test]
    fn reset_invalidates_everything() {
        let c = RollupResultCache::new(1024 * 1024);
        let ec = test_ec();
        let e = test_expr();
        c.put_series(&ec, &e, WINDOW, &[ts(&[800, 1000, 1200], &[0.0, 1.0, 2.0])]);
        c.reset();
        let (tss, new_start) = c.get_series(&ec, &e, WINDOW);
        assert_eq!(new_start, ec.start);
        assert!(tss.is_empty());
    }

    #[test]
    fn put_truncates_points_fresher_than_cache_timestamp_offset() {
        let c = RollupResultCache::new(1024 * 1024);
        let step = 60_000i64;
        let now = now_unix_ms();
        // A grid ending now: only the points older than
        // now - step - offset may be cached.
        let start = (now - 30 * 60_000) / step * step;
        let timestamps: Vec<i64> = (0..30).map(|i| start + i * step).collect();
        let values: Vec<f64> = (0..30).map(|i| i as f64).collect();
        let mut ec = EvalConfig::new(start, timestamps[29], step);
        ec.max_points_per_series = 10_000;
        ec.may_cache = true;
        let e = test_expr();
        c.put_series(&ec, &e, WINDOW, &[ts(&timestamps, &values)]);
        let (tss, new_start) = c.get_series(&ec, &e, WINDOW);
        assert_eq!(tss.len(), 1, "cache must hold the old prefix");
        let deadline = now_unix_ms() - step - CACHE_TIMESTAMP_OFFSET_MS;
        assert!(
            *tss[0].timestamps.last().unwrap() <= deadline,
            "fresh points must not be cached: {} > {deadline}",
            tss[0].timestamps.last().unwrap()
        );
        assert!(new_start > ec.start && new_start <= ec.end);
    }

    #[test]
    fn marshal_timeseries_roundtrip_preserves_names_and_nan_bits() {
        let mut series = named_ts(
            "cpu_usage_user",
            &[1000, 1200, 1400],
            &[
                1.5,
                f64::from_bits(esm_common::decimal::STALE_NAN.to_bits()),
                NAN,
            ],
        );
        series.metric_name.add_tag("hostname", "h1");
        series.metric_name.add_tag("dc", "east");
        let buf = marshal_timeseries_fast(std::slice::from_ref(&series), 3, usize::MAX).unwrap();
        let got = unmarshal_timeseries_fast(&buf).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].metric_name.metric_group, b"cpu_usage_user");
        assert_eq!(
            got[0].metric_name.get_tag_value("hostname"),
            Some(b"h1".as_slice())
        );
        assert_eq!(got[0].timestamps.as_slice(), &[1000, 1200, 1400]);
        assert_eq!(got[0].values[0], 1.5);
        assert_eq!(
            got[0].values[1].to_bits(),
            esm_common::decimal::STALE_NAN.to_bits()
        );
        assert!(got[0].values[2].is_nan());
    }

    #[test]
    fn marshal_timeseries_respects_max_size() {
        let series = ts(&[1000, 1200, 1400], &[1.0, 2.0, 3.0]);
        assert!(marshal_timeseries_fast(std::slice::from_ref(&series), 3, 10).is_none());
    }

    #[test]
    fn metainfo_overflow_drops_oldest_half() {
        let mut mi = Metainfo::default();
        for i in 0..11i64 {
            mi.add_key(
                CacheKey {
                    prefix: 1,
                    suffix: i as u64,
                },
                i * 100,
                i * 100 + 50,
            );
        }
        assert_eq!(mi.entries.len(), 6);
        // The oldest 5 entries are gone.
        assert!(mi.get_best_key(0, 50).is_none());
        assert!(mi.get_best_key(500, 550).is_some());
    }

    // --- mergeSeries (Go TestMergeSeries) --------------------------------

    const B_START: i64 = 1400;

    #[test]
    fn merge_b_start_equals_ec_start() {
        let ec = test_ec();
        let b = vec![ts(
            &[1000, 1200, 1400, 1600, 1800, 2000],
            &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        )];
        let got = merge_series(Vec::new(), b, 1000, &ec).unwrap();
        assert_tss_eq(
            &got,
            &[ts(
                &[1000, 1200, 1400, 1600, 1800, 2000],
                &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            )],
        );
    }

    #[test]
    fn merge_a_empty() {
        let ec = test_ec();
        let b = vec![ts(&[1400, 1600, 1800, 2000], &[3.0, 4.0, 5.0, 6.0])];
        let got = merge_series(Vec::new(), b, B_START, &ec).unwrap();
        assert_tss_eq(
            &got,
            &[ts(
                &[1000, 1200, 1400, 1600, 1800, 2000],
                &[NAN, NAN, 3.0, 4.0, 5.0, 6.0],
            )],
        );
    }

    #[test]
    fn merge_b_empty() {
        let ec = test_ec();
        let a = vec![ts(&[1000, 1200], &[2.0, 1.0])];
        let got = merge_series(a, Vec::new(), B_START, &ec).unwrap();
        assert_tss_eq(
            &got,
            &[ts(
                &[1000, 1200, 1400, 1600, 1800, 2000],
                &[2.0, 1.0, NAN, NAN, NAN, NAN],
            )],
        );
    }

    #[test]
    fn merge_non_empty() {
        let ec = test_ec();
        let a = vec![ts(&[1000, 1200], &[2.0, 1.0])];
        let b = vec![ts(&[1400, 1600, 1800, 2000], &[3.0, 4.0, 5.0, 6.0])];
        let got = merge_series(a, b, B_START, &ec).unwrap();
        assert_tss_eq(
            &got,
            &[ts(
                &[1000, 1200, 1400, 1600, 1800, 2000],
                &[2.0, 1.0, 3.0, 4.0, 5.0, 6.0],
            )],
        );
    }

    #[test]
    fn merge_non_empty_distinct_metric_names() {
        let ec = test_ec();
        let a = vec![named_ts("bar", &[1000, 1200], &[2.0, 1.0])];
        let b = vec![named_ts(
            "foo",
            &[1400, 1600, 1800, 2000],
            &[3.0, 4.0, 5.0, 6.0],
        )];
        let got = merge_series(a, b, B_START, &ec).unwrap();
        assert_tss_eq(
            &got,
            &[
                named_ts(
                    "foo",
                    &[1000, 1200, 1400, 1600, 1800, 2000],
                    &[NAN, NAN, 3.0, 4.0, 5.0, 6.0],
                ),
                named_ts(
                    "bar",
                    &[1000, 1200, 1400, 1600, 1800, 2000],
                    &[2.0, 1.0, NAN, NAN, NAN, NAN],
                ),
            ],
        );
    }

    #[test]
    fn merge_duplicate_series_a_fails() {
        let ec = test_ec();
        let a = vec![
            ts(&[1000, 1200], &[2.0, 1.0]),
            ts(&[1000, 1200], &[3.0, 3.0]),
        ];
        let b = vec![ts(&[1400, 1600, 1800, 2000], &[3.0, 4.0, 5.0, 6.0])];
        assert!(merge_series(a, b, B_START, &ec).is_none());
    }

    #[test]
    fn merge_duplicate_series_b_fails() {
        let ec = test_ec();
        let a = vec![ts(&[1000, 1200], &[1.0, 2.0])];
        let b = vec![
            ts(&[1400, 1600, 1800, 2000], &[3.0, 4.0, 5.0, 6.0]),
            ts(&[1400, 1600, 1800, 2000], &[13.0, 14.0, 15.0, 16.0]),
        ];
        assert!(merge_series(a, b, B_START, &ec).is_none());
    }
}
