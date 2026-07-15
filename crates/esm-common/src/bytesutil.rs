//! Port of the upstream VictoriaMetrics `lib/bytesutil` (v1.146.0).
//!
//! Ported: buffer resize helpers (`bytesutil.go`), `FastStringMatcher`
//! (`fast_string_matcher.go`), `FastStringTransformer`
//! (`fast_string_transformer.go`) and the string interning cache
//! (`internstring.go`).
//!
//! Skipped:
//! - `ToUnsafeString` / `ToUnsafeBytes`: Go-specific zero-copy conversions
//!   between `string` and `[]byte`; unnecessary in Rust where `String` /
//!   `&[u8]` conversions are already safe and explicit.
//! - `itoa.go`: Go-specific pooled integer formatting; Rust's std formatting
//!   (`itoa`-style `format!`/`write!`) covers it.
//! - `bytebuffer.go`: depends on `lib/fs` and `lib/filestream`, which are out
//!   of scope for this module.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};

use crate::fasttime;

// ---------------------------------------------------------------------------
// bytesutil.go
// ---------------------------------------------------------------------------

/// Port of Go `bytesutil.ResizeWithCopyMayOverallocate`.
///
/// Resizes `b` to minimum `n` bytes and returns the resized buffer (which may
/// be newly allocated). If a newly allocated buffer is returned then `b`
/// contents is copied to it. The capacity of a newly allocated buffer is
/// rounded up to the nearest power of 2.
pub fn resize_with_copy_may_overallocate(mut b: Vec<u8>, n: usize) -> Vec<u8> {
    if n <= b.capacity() {
        b.resize(n, 0);
        return b;
    }
    let n_new = round_to_nearest_pow2(n);
    let mut b_new = Vec::with_capacity(n_new);
    b_new.extend_from_slice(&b);
    b_new.resize(n, 0);
    b_new
}

/// Port of Go `bytesutil.ResizeWithCopyNoOverallocate`.
///
/// Resizes `b` to exactly `n` bytes and returns the resized buffer (which may
/// be newly allocated). If a newly allocated buffer is returned then `b`
/// contents is copied to it.
pub fn resize_with_copy_no_overallocate(mut b: Vec<u8>, n: usize) -> Vec<u8> {
    if n <= b.capacity() {
        b.resize(n, 0);
        return b;
    }
    let mut b_new = Vec::with_capacity(n);
    b_new.extend_from_slice(&b);
    b_new.resize(n, 0);
    b_new
}

/// Port of Go `bytesutil.ResizeNoCopyMayOverallocate`.
///
/// Resizes `b` to minimum `n` bytes and returns the resized buffer (which may
/// be newly allocated). The contents of the returned buffer are unspecified
/// (a newly allocated buffer is not populated from `b`). The capacity of a
/// newly allocated buffer is rounded up to the nearest power of 2.
///
/// Deviation from Go: Go's `b[:n]` may expose old garbage bytes; safe Rust
/// zero-fills the grown region instead. Contents remain "unspecified" per the
/// contract, so callers must not rely on the zeroing.
pub fn resize_no_copy_may_overallocate(mut b: Vec<u8>, n: usize) -> Vec<u8> {
    if n <= b.capacity() {
        b.resize(n, 0);
        return b;
    }
    let mut b_new = vec![0u8; round_to_nearest_pow2(n)];
    b_new.truncate(n);
    b_new
}

/// Port of Go `bytesutil.ResizeNoCopyNoOverallocate`.
///
/// Resizes `b` to exactly `n` bytes and returns the resized buffer (which may
/// be newly allocated). The contents of the returned buffer are unspecified
/// (a newly allocated buffer is not populated from `b`).
///
/// Deviation from Go: same zero-fill note as
/// [`resize_no_copy_may_overallocate`].
pub fn resize_no_copy_no_overallocate(mut b: Vec<u8>, n: usize) -> Vec<u8> {
    if n <= b.capacity() {
        b.resize(n, 0);
        return b;
    }
    vec![0u8; n]
}

/// Port of Go `bytesutil.roundToNearestPow2`.
///
/// Rounds `n` to the nearest power of 2. It is expected that `n > 0`.
fn round_to_nearest_pow2(n: usize) -> usize {
    // Go: pow2 := uint8(bits.Len(uint(n - 1))); return 1 << pow2.
    // wrapping_sub + checked_shl reproduce Go's wrap-around semantics for n=0
    // (Go returns 0 there, since a shift by the full bit width yields 0).
    let pow2 = usize::BITS - n.wrapping_sub(1).leading_zeros();
    1usize.checked_shl(pow2).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// internstring.go
// ---------------------------------------------------------------------------

/// Port of the Go `-internStringMaxLen` flag (default 500).
///
/// The maximum length for strings to intern.
pub const INTERN_STRING_MAX_LEN: usize = 500;

/// Port of the Go `-internStringDisableCache` flag (default false).
///
/// Whether to disable caches for interned strings.
pub const INTERN_STRING_DISABLE_CACHE: bool = false;

/// Port of the Go `-internStringCacheExpireDuration` flag (default 6 minutes).
///
/// The expiry duration for caches for interned strings.
pub const INTERN_STRING_CACHE_EXPIRE_DURATION: Duration = Duration::from_secs(6 * 60);

/// Port of Go `bytesutil.isSkipCache`.
fn is_skip_cache(s: &str) -> bool {
    INTERN_STRING_DISABLE_CACHE || s.len() > INTERN_STRING_MAX_LEN
}

/// Port of Go `bytesutil.internStringMapEntry`.
struct InternStringMapEntry {
    deadline: u64,
    s: String,
}

struct InternStringMutable {
    map: HashMap<String, String>,
    reads: u64,
}

/// Port of Go `bytesutil.internStringMap`.
///
/// Go uses a lock-free `atomic.Pointer` snapshot for the readonly map; here a
/// `parking_lot::RwLock` provides the equivalent read-mostly access pattern.
struct InternStringMap {
    mutable: Mutex<InternStringMutable>,
    readonly: RwLock<HashMap<String, InternStringMapEntry>>,
}

impl InternStringMap {
    fn new() -> Self {
        InternStringMap {
            mutable: Mutex::new(InternStringMutable {
                map: HashMap::new(),
                reads: 0,
            }),
            readonly: RwLock::new(HashMap::new()),
        }
    }

    /// Port of Go `internStringMap.intern`.
    fn intern(&self, s: &str) -> String {
        if is_skip_cache(s) {
            return s.to_string();
        }

        {
            let readonly = self.readonly.read();
            if let Some(e) = readonly.get(s) {
                // Fast path - the string has been found in the readonly map.
                return e.s.clone();
            }
        }

        // Slower path - search for the string in the mutable map under the lock.
        let mut mutable = self.mutable.lock();
        let s_interned = if let Some(v) = mutable.map.get(s) {
            v.clone()
        } else {
            // Verify whether s has been already registered by concurrent
            // threads in the readonly map.
            let from_readonly = self.readonly.read().get(s).map(|e| e.s.clone());
            match from_readonly {
                Some(v) => v,
                None => {
                    // Slowest path - register the string in the mutable map.
                    let s_interned = s.to_string();
                    mutable.map.insert(s_interned.clone(), s_interned.clone());
                    s_interned
                }
            }
        };
        mutable.reads += 1;
        if mutable.reads > self.readonly.read().len() as u64 {
            self.migrate_mutable_to_readonly_locked(&mut mutable);
            mutable.reads = 0;
        }
        drop(mutable);

        s_interned
    }

    /// Port of Go `internStringMap.migrateMutableToReadonlyLocked`.
    ///
    /// Go builds a fresh readonly map copy and atomically swaps the pointer;
    /// with an `RwLock` inserting under the write lock is equivalent.
    fn migrate_mutable_to_readonly_locked(&self, mutable: &mut InternStringMutable) {
        let deadline = fasttime::unix_timestamp() + INTERN_STRING_CACHE_EXPIRE_DURATION.as_secs();
        let mut readonly = self.readonly.write();
        for (k, s) in mutable.map.drain() {
            readonly.insert(k, InternStringMapEntry { deadline, s });
        }
    }

    /// Port of Go `internStringMap.cleanup`.
    ///
    /// Drops readonly entries whose deadline has passed.
    fn cleanup(&self) {
        let current_time = fasttime::unix_timestamp();
        {
            let readonly = self.readonly.read();
            let needs_cleanup = readonly.values().any(|e| e.deadline <= current_time);
            if !needs_cleanup {
                return;
            }
        }
        // Go rebuilds a filtered copy and swaps the pointer; retain under the
        // write lock is the RwLock equivalent.
        self.readonly
            .write()
            .retain(|_, e| e.deadline > current_time);
    }
}

/// Returns the global intern string map (Go: package-level `ism` var).
///
/// The background cleanup thread mirrors the goroutine spawned in Go's
/// `newInternStringMap`.
fn ism() -> &'static InternStringMap {
    static ISM: OnceLock<&'static InternStringMap> = OnceLock::new();
    ISM.get_or_init(|| {
        let m: &'static InternStringMap = Box::leak(Box::new(InternStringMap::new()));
        let cleanup_interval = add_jitter_to_duration(INTERN_STRING_CACHE_EXPIRE_DURATION) / 2;
        std::thread::Builder::new()
            .name("internstring-cleanup".to_string())
            .spawn(move || loop {
                std::thread::sleep(cleanup_interval);
                m.cleanup();
            })
            .expect("failed to spawn internstring cleanup thread");
        m
    })
}

/// Port of Go `timeutil.AddJitterToDuration`: adds up to `min(d/10, 10s)` of
/// random jitter to `d` (inlined here since `lib/timeutil` is out of scope).
fn add_jitter_to_duration(d: Duration) -> Duration {
    let dv = (d / 10).min(Duration::from_secs(10));
    let p = f64::from(rand::random::<u32>()) / (1u64 << 32) as f64;
    d + dv.mul_f64(p)
}

/// Port of Go `bytesutil.InternString`.
///
/// Returns interned `s`.
///
/// Deviation from Go: Go returns a shared reference to the interned string
/// (deduplicating memory); this port returns an owned `String` clone, so it
/// acts as a value-level cache rather than a memory deduplicator.
pub fn intern_string(s: &str) -> String {
    ism().intern(s)
}

/// Port of Go `bytesutil.InternBytes`.
///
/// Interns `b` as a string.
///
/// Deviation from Go: Go strings may hold arbitrary bytes, while Rust `String`
/// must be valid UTF-8, so invalid sequences are replaced (lossy conversion).
pub fn intern_bytes(b: &[u8]) -> String {
    intern_string(&String::from_utf8_lossy(b))
}

// ---------------------------------------------------------------------------
// fast_string_matcher.go
// ---------------------------------------------------------------------------

// Reduce the frequency of lastAccessTime updates to once per 10 seconds (Go
// inlines the constant 10 in fast_string_matcher.go / fast_string_transformer.go).
const LAST_ACCESS_TIME_UPDATE_INTERVAL_SECONDS: u64 = 10;

/// Port of Go `bytesutil.FastStringMatcher`.
///
/// Implements a fast matcher for strings. It caches string match results and
/// returns them back on the next calls without calling the match function,
/// which may be expensive.
pub struct FastStringMatcher {
    last_cleanup_time: AtomicU64,
    // Go uses sync.Map; parking_lot::RwLock<HashMap> is the Rust equivalent.
    m: RwLock<HashMap<String, FsmEntry>>,
    match_func: Box<dyn Fn(&str) -> bool + Send + Sync>,
}

/// Port of Go `bytesutil.fsmEntry`.
struct FsmEntry {
    last_access_time: AtomicU64,
    ok: bool,
}

impl FastStringMatcher {
    /// Port of Go `bytesutil.NewFastStringMatcher`.
    ///
    /// Creates a new matcher, which applies `match_fn` to strings passed to
    /// [`FastStringMatcher::matches`].
    ///
    /// `match_fn` must return the same result for the same input.
    pub fn new(match_fn: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        FastStringMatcher {
            last_cleanup_time: AtomicU64::new(fasttime::unix_timestamp()),
            m: RwLock::new(HashMap::new()),
            match_func: Box::new(match_fn),
        }
    }

    /// Port of Go `FastStringMatcher.Match`.
    ///
    /// Applies the match function to `s` and returns the result.
    pub fn matches(&self, s: &str) -> bool {
        if is_skip_cache(s) {
            return (self.match_func)(s);
        }

        let ct = fasttime::unix_timestamp();
        {
            let m = self.m.read();
            if let Some(e) = m.get(s) {
                // Fast path - the s match result is found in the cache.
                if e.last_access_time.load(Ordering::Relaxed)
                    + LAST_ACCESS_TIME_UPDATE_INTERVAL_SECONDS
                    < ct
                {
                    // Reduce the frequency of last_access_time updates to once
                    // per 10 seconds in order to improve the fast path speed
                    // on systems with many CPU cores.
                    e.last_access_time.store(ct, Ordering::Relaxed);
                }
                return e.ok;
            }
        }
        // Slow path - run the match function for s and store the result in
        // the cache. The key is an owned copy of s (Go clones for the same
        // reason: to avoid retaining a bigger backing string).
        let b = (self.match_func)(s);
        let e = FsmEntry {
            last_access_time: AtomicU64::new(ct),
            ok: b,
        };
        self.m.write().insert(s.to_string(), e);

        if need_cleanup(&self.last_cleanup_time, ct) {
            // Perform a global cleanup by removing items which weren't
            // accessed within the cache expire duration.
            let deadline = ct.wrapping_sub(INTERN_STRING_CACHE_EXPIRE_DURATION.as_secs());
            self.m
                .write()
                .retain(|_, e| e.last_access_time.load(Ordering::Relaxed) >= deadline);
        }

        b
    }
}

/// Port of Go `bytesutil.needCleanup`.
fn need_cleanup(last_cleanup_time: &AtomicU64, current_time: u64) -> bool {
    let lct = last_cleanup_time.load(Ordering::Relaxed);
    if lct.wrapping_add(61) >= current_time {
        return false;
    }
    // Atomically compare and swap the current time with last_cleanup_time in
    // order to guarantee that only a single thread out of multiple
    // concurrently executing threads gets true from the call.
    last_cleanup_time
        .compare_exchange(lct, current_time, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
}

// ---------------------------------------------------------------------------
// fast_string_transformer.go
// ---------------------------------------------------------------------------

/// Port of Go `bytesutil.FastStringTransformer`.
///
/// Implements a fast transformer for strings. It caches transformed strings
/// and returns them back on the next calls without calling the transform
/// function, which may be expensive.
pub struct FastStringTransformer {
    last_cleanup_time: AtomicU64,
    // Go uses sync.Map; parking_lot::RwLock<HashMap> is the Rust equivalent.
    m: RwLock<HashMap<String, FstEntry>>,
    transform_func: Box<dyn Fn(&str) -> String + Send + Sync>,
}

/// Port of Go `bytesutil.fstEntry`.
struct FstEntry {
    last_access_time: AtomicU64,
    s: String,
}

impl FastStringTransformer {
    /// Port of Go `bytesutil.NewFastStringTransformer`.
    ///
    /// Creates a new transformer, which applies `transform_fn` to strings
    /// passed to [`FastStringTransformer::transform`].
    ///
    /// `transform_fn` must return the same result for the same input.
    pub fn new(transform_fn: impl Fn(&str) -> String + Send + Sync + 'static) -> Self {
        FastStringTransformer {
            last_cleanup_time: AtomicU64::new(fasttime::unix_timestamp()),
            m: RwLock::new(HashMap::new()),
            transform_func: Box::new(transform_fn),
        }
    }

    /// Port of Go `FastStringTransformer.Transform`.
    ///
    /// Applies the transform function to `s` and returns the result.
    pub fn transform(&self, s: &str) -> String {
        if is_skip_cache(s) {
            // Go clones identity results here to guard against unsafe
            // strings; unnecessary in Rust, where the result is owned.
            return (self.transform_func)(s);
        }

        let ct = fasttime::unix_timestamp();
        {
            let m = self.m.read();
            if let Some(e) = m.get(s) {
                // Fast path - the transformed s is found in the cache.
                if e.last_access_time.load(Ordering::Relaxed)
                    + LAST_ACCESS_TIME_UPDATE_INTERVAL_SECONDS
                    < ct
                {
                    // Reduce the frequency of last_access_time updates to once
                    // per 10 seconds in order to improve the fast path speed
                    // on systems with many CPU cores.
                    e.last_access_time.store(ct, Ordering::Relaxed);
                }
                return e.s.clone();
            }
        }
        // Slow path - transform s and store it in the cache.
        let s_transformed = (self.transform_func)(s);
        let s_owned = s.to_string();
        // Go detail: if the transformed string equals the input, the freshly
        // cloned input is stored/returned instead of the transform result
        // (in Go this avoids retaining a bigger backing string).
        let s_transformed = if s_transformed == s_owned {
            s_owned.clone()
        } else {
            s_transformed
        };
        let e = FstEntry {
            last_access_time: AtomicU64::new(ct),
            s: s_transformed.clone(),
        };
        self.m.write().insert(s_owned, e);

        if need_cleanup(&self.last_cleanup_time, ct) {
            // Perform a global cleanup by removing items which weren't
            // accessed within the cache expire duration.
            let deadline = ct.wrapping_sub(INTERN_STRING_CACHE_EXPIRE_DURATION.as_secs());
            self.m
                .write()
                .retain(|_, e| e.last_access_time.load(Ordering::Relaxed) >= deadline);
        }

        s_transformed
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Port of Go TestRoundToNearestPow2.
    #[test]
    fn test_round_to_nearest_pow2() {
        fn f(n: usize, result_expected: usize) {
            let result = round_to_nearest_pow2(n);
            assert_eq!(
                result, result_expected,
                "unexpected round_to_nearest_pow2({n}); got {result}; want {result_expected}"
            );
        }
        f(1, 1);
        f(2, 2);
        f(3, 4);
        f(4, 4);
        f(5, 8);
        f(6, 8);
        f(7, 8);
        f(8, 8);
        f(9, 16);
        f(10, 16);
        f(16, 16);
        f(17, 32);
        f(32, 32);
        f(33, 64);
        f(64, 64);
    }

    // Port of Go TestResizeNoCopyNoOverallocate.
    #[test]
    fn test_resize_no_copy_no_overallocate() {
        for i in 0..1000usize {
            let b = resize_no_copy_no_overallocate(Vec::new(), i);
            assert_eq!(b.len(), i, "invalid b size; got {}; want {i}", b.len());
            assert_eq!(
                b.capacity(),
                i,
                "invalid cap(b); got {}; want {i}",
                b.capacity()
            );
            let b_ptr = b.as_ptr();

            let b1 = resize_no_copy_no_overallocate(b, i);
            assert!(
                b1.len() == i && (i == 0 || b1.as_ptr() == b_ptr),
                "invalid b1; got {:p}; want {b_ptr:p}",
                b1.as_ptr()
            );
            assert_eq!(
                b1.capacity(),
                i,
                "invalid cap(b1); got {}; want {i}",
                b1.capacity()
            );

            // Go: ResizeNoCopyNoOverallocate(b[:0], i).
            let mut b1 = b1;
            b1.clear();
            let b2 = resize_no_copy_no_overallocate(b1, i);
            assert!(
                b2.len() == i && (i == 0 || b2.as_ptr() == b_ptr),
                "invalid b2; got {:p}; want {b_ptr:p}",
                b2.as_ptr()
            );
            assert_eq!(
                b2.capacity(),
                i,
                "invalid cap(b2); got {}; want {i}",
                b2.capacity()
            );

            if i > 0 {
                let mut b = b2;
                b[0] = 123;
                let b_ptr = b.as_ptr();
                let b3 = resize_no_copy_no_overallocate(b, i + 1);
                assert_eq!(
                    b3.len(),
                    i + 1,
                    "invalid b3 len; got {}; want {}",
                    b3.len(),
                    i + 1
                );
                assert_eq!(
                    b3.capacity(),
                    i + 1,
                    "invalid cap(b3); got {}; want {}",
                    b3.capacity(),
                    i + 1
                );
                assert_ne!(b3.as_ptr(), b_ptr, "b3 must be newly allocated");
                assert_eq!(b3[0], 0, "b3[0] must be zeroed; got {}", b3[0]);
            }
        }
    }

    // Port of Go TestResizeNoCopyMayOverallocate.
    #[test]
    fn test_resize_no_copy_may_overallocate() {
        for i in 0..1000usize {
            let b = resize_no_copy_may_overallocate(Vec::new(), i);
            assert_eq!(b.len(), i, "invalid b size; got {}; want {i}", b.len());
            let mut cap_expected = round_to_nearest_pow2(i);
            assert_eq!(
                b.capacity(),
                cap_expected,
                "invalid cap(b); got {}; want {cap_expected}",
                b.capacity()
            );
            let b_ptr = b.as_ptr();

            let b1 = resize_no_copy_may_overallocate(b, i);
            assert!(
                b1.len() == i && (i == 0 || b1.as_ptr() == b_ptr),
                "invalid b1; got {:p}; want {b_ptr:p}",
                b1.as_ptr()
            );
            assert_eq!(
                b1.capacity(),
                cap_expected,
                "invalid cap(b1); got {}; want {cap_expected}",
                b1.capacity()
            );

            // Go: ResizeNoCopyMayOverallocate(b[:0], i).
            let mut b1 = b1;
            b1.clear();
            let b2 = resize_no_copy_may_overallocate(b1, i);
            assert!(
                b2.len() == i && (i == 0 || b2.as_ptr() == b_ptr),
                "invalid b2; got {:p}; want {b_ptr:p}",
                b2.as_ptr()
            );
            assert_eq!(
                b2.capacity(),
                cap_expected,
                "invalid cap(b2); got {}; want {cap_expected}",
                b2.capacity()
            );

            if i > 0 {
                let b3 = resize_no_copy_may_overallocate(b2, i + 1);
                assert_eq!(
                    b3.len(),
                    i + 1,
                    "invalid b3 len; got {}; want {}",
                    b3.len(),
                    i + 1
                );
                cap_expected = round_to_nearest_pow2(i + 1);
                assert_eq!(
                    b3.capacity(),
                    cap_expected,
                    "invalid cap(b3); got {}; want {cap_expected}",
                    b3.capacity()
                );
            }
        }
    }

    // Port of Go TestResizeWithCopyNoOverallocate.
    #[test]
    fn test_resize_with_copy_no_overallocate() {
        for i in 0..1000usize {
            let b = resize_with_copy_no_overallocate(Vec::new(), i);
            assert_eq!(b.len(), i, "invalid b size; got {}; want {i}", b.len());
            assert_eq!(
                b.capacity(),
                i,
                "invalid cap(b); got {}; want {i}",
                b.capacity()
            );
            let b_ptr = b.as_ptr();

            let b1 = resize_with_copy_no_overallocate(b, i);
            assert!(
                b1.len() == i && (i == 0 || b1.as_ptr() == b_ptr),
                "invalid b1; got {:p}; want {b_ptr:p}",
                b1.as_ptr()
            );
            assert_eq!(
                b1.capacity(),
                i,
                "invalid cap(b1); got {}; want {i}",
                b1.capacity()
            );

            // Go: ResizeWithCopyNoOverallocate(b[:0], i).
            let mut b1 = b1;
            b1.clear();
            let b2 = resize_with_copy_no_overallocate(b1, i);
            assert!(
                b2.len() == i && (i == 0 || b2.as_ptr() == b_ptr),
                "invalid b2; got {:p}; want {b_ptr:p}",
                b2.as_ptr()
            );
            assert_eq!(
                b2.capacity(),
                i,
                "invalid cap(b2); got {}; want {i}",
                b2.capacity()
            );

            if i > 0 {
                let mut b = b2;
                b[0] = 123;
                let b_ptr = b.as_ptr();
                let b3 = resize_with_copy_no_overallocate(b, i + 1);
                assert_eq!(
                    b3.len(),
                    i + 1,
                    "invalid b3 len; got {}; want {}",
                    b3.len(),
                    i + 1
                );
                assert_eq!(
                    b3.capacity(),
                    i + 1,
                    "invalid cap(b3); got {}; want {}",
                    b3.capacity(),
                    i + 1
                );
                assert_ne!(b3.as_ptr(), b_ptr, "b3 must be newly allocated for i={i}");
                // Go: b3[0] must equal b[0] (== 123); b was moved, but b[0]
                // cannot change since b3 is a new allocation.
                assert_eq!(b3[0], 123, "b3[0] must equal b[0]; got {}; want 123", b3[0]);
            }
        }
    }

    // Port of Go TestResizeWithCopyMayOverallocate.
    #[test]
    fn test_resize_with_copy_may_overallocate() {
        for i in 0..1000usize {
            let b = resize_with_copy_may_overallocate(Vec::new(), i);
            assert_eq!(b.len(), i, "invalid b size; got {}; want {i}", b.len());
            let mut cap_expected = round_to_nearest_pow2(i);
            assert_eq!(
                b.capacity(),
                cap_expected,
                "invalid cap(b); got {}; want {cap_expected}",
                b.capacity()
            );
            let b_ptr = b.as_ptr();

            let b1 = resize_with_copy_may_overallocate(b, i);
            assert!(
                b1.len() == i && (i == 0 || b1.as_ptr() == b_ptr),
                "invalid b1; got {:p}; want {b_ptr:p}",
                b1.as_ptr()
            );
            assert_eq!(
                b1.capacity(),
                cap_expected,
                "invalid cap(b1); got {}; want {cap_expected}",
                b1.capacity()
            );

            // Go: ResizeWithCopyMayOverallocate(b[:0], i).
            let mut b1 = b1;
            b1.clear();
            let b2 = resize_with_copy_may_overallocate(b1, i);
            assert!(
                b2.len() == i && (i == 0 || b2.as_ptr() == b_ptr),
                "invalid b2; got {:p}; want {b_ptr:p}",
                b2.as_ptr()
            );
            assert_eq!(
                b2.capacity(),
                cap_expected,
                "invalid cap(b2); got {}; want {cap_expected}",
                b2.capacity()
            );

            if i > 0 {
                let mut b = b2;
                b[0] = 123;
                let b3 = resize_with_copy_may_overallocate(b, i + 1);
                assert_eq!(
                    b3.len(),
                    i + 1,
                    "invalid b3 len; got {}; want {}",
                    b3.len(),
                    i + 1
                );
                cap_expected = round_to_nearest_pow2(i + 1);
                assert_eq!(
                    b3.capacity(),
                    cap_expected,
                    "invalid cap(b3); got {}; want {cap_expected}",
                    b3.capacity()
                );
                assert_eq!(b3[0], 123, "b3[0] must equal b[0]; got {}; want 123", b3[0]);
            }
        }
    }

    // Go TestToUnsafeString is skipped: ToUnsafeString/ToUnsafeBytes are not
    // ported (see module docs).

    // Port of Go TestFastStringMatcher.
    #[test]
    fn test_fast_string_matcher() {
        let fsm = FastStringMatcher::new(|s: &str| s.starts_with("foo"));
        let f = |s: &str, result_expected: bool| {
            for i in 0..10 {
                let result = fsm.matches(s);
                assert_eq!(
                    result, result_expected,
                    "unexpected result for matches({s:?}) at iteration {i}; got {result}; want {result_expected}"
                );
            }
        };
        f("", false);
        f("foo", true);
        f("a_b-C", false);
        f("foobar", true);
    }

    // Port of Go TestNeedCleanup.
    #[test]
    fn test_need_cleanup() {
        fn f(last_cleanup_time: u64, current_time: u64, result_expected: bool) {
            let lct = AtomicU64::new(last_cleanup_time);
            let result = need_cleanup(&lct, current_time);
            assert_eq!(
                result, result_expected,
                "unexpected result for need_cleanup({last_cleanup_time}, {current_time}); got {result}; want {result_expected}"
            );
            if result {
                let n = lct.load(Ordering::Relaxed);
                assert_eq!(
                    n, current_time,
                    "unexpected value for lct; got {n}; want current_time={current_time}"
                );
            } else {
                let n = lct.load(Ordering::Relaxed);
                assert_eq!(
                    n, last_cleanup_time,
                    "unexpected value for lct; got {n}; want last_cleanup_time={last_cleanup_time}"
                );
            }
        }
        f(0, 0, false);
        f(0, 61, false);
        f(0, 62, true);
        f(10, 100, true);
    }

    // Port of Go TestFastStringTransformer.
    #[test]
    fn test_fast_string_transformer() {
        let fst = FastStringTransformer::new(|s: &str| s.to_uppercase());
        let f = |s: &str, result_expected: &str| {
            for i in 0..10 {
                let result = fst.transform(s);
                assert_eq!(
                    result, result_expected,
                    "unexpected result for transform({s:?}) at iteration {i}; got {result:?}; want {result_expected:?}"
                );
            }
        };
        f("", "");
        f("foo", "FOO");
        f("a_b-C", "A_B-C");
    }

    // Port of Go TestInternStringSerial.
    #[test]
    fn test_intern_string_serial() {
        test_intern_string_inner();
    }

    // Port of Go TestInternStringConcurrent. Go runs 5 goroutines with a 5s
    // channel timeout; thread::scope joins all threads directly, so the
    // timeout machinery is unnecessary.
    #[test]
    fn test_intern_string_concurrent() {
        std::thread::scope(|scope| {
            for _ in 0..5 {
                scope.spawn(test_intern_string_inner);
            }
        });
    }

    // Port of Go testInternString (assertion-based instead of error-returning).
    fn test_intern_string_inner() {
        for i in 0..1000 {
            let s = format!("foo_{i}");
            let s1 = intern_string(&s);
            assert_eq!(
                s, s1,
                "unexpected string returned from intern_string; got {s1:?}; want {s:?}"
            );
        }
    }
}
