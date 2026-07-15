//! `partWrapper` and the part-selection heuristics from `table.go`.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::inmemory_part::InmemoryPart;
use crate::part::Part;

/// Default number of parts to merge at once.
///
/// This number has been obtained empirically - it gives the lowest possible
/// overhead. See appendPartsToMerge tests for details.
pub(crate) const DEFAULT_PARTS_TO_MERGE: usize = 15;

/// The minimum multiplier for the size of the output part compared to the
/// size of the maximum input part for the merge.
///
/// Higher value reduces write amplification (disk write IO induced by the
/// merge), while increases the number of unmerged parts.
/// The 1.7 is good enough for production workloads.
pub(crate) const MIN_MERGE_MULTIPLIER: f64 = 1.7;

/// A refcounted wrapper around a part.
///
/// The `Arc` strong count plays the role of Go's manual `refCount`.
pub(crate) struct PartWrapper {
    /// The part itself. `Arc`ed separately so searches can outlive the
    /// wrapper (deletion of dropped parts happens when the last `Arc<Part>`
    /// is released).
    pub p: Arc<Part>,

    /// The in-memory part backing `p`, if any.
    pub mp: Option<InmemoryPart>,

    /// Marks the part for deletion once the wrapper is dropped.
    /// This field should be updated only after the wrapper was removed from
    /// the list of active parts.
    pub must_drop: AtomicBool,

    /// Whether the part takes part in a merge. Guarded by the table parts
    /// lock (stored as an atomic only to keep `PartWrapper: Sync`).
    pub is_in_merge: AtomicBool,

    /// The deadline when the in-memory part must be flushed to disk.
    pub flush_to_disk_deadline: Instant,
}

impl PartWrapper {
    pub fn new_from_inmemory_part(
        mp: InmemoryPart,
        flush_to_disk_deadline: Instant,
    ) -> Arc<PartWrapper> {
        let p = Arc::new(mp.new_part());
        Arc::new(PartWrapper {
            p,
            mp: Some(mp),
            must_drop: AtomicBool::new(false),
            is_in_merge: AtomicBool::new(false),
            flush_to_disk_deadline,
        })
    }

    pub fn new_from_file_part(p: Part) -> Arc<PartWrapper> {
        Arc::new(PartWrapper {
            p: Arc::new(p),
            mp: None,
            must_drop: AtomicBool::new(false),
            is_in_merge: AtomicBool::new(false),
            flush_to_disk_deadline: Instant::now(),
        })
    }
}

impl Drop for PartWrapper {
    fn drop(&mut self) {
        if self.must_drop.load(Ordering::Acquire) {
            // The actual directory removal happens when the last Arc<Part>
            // reference (e.g. held by an in-flight search) is dropped.
            self.p.must_drop_on_release.store(true, Ordering::Release);
        }
    }
}

/// Returns the earliest flush-to-disk deadline among the in-memory `pws`,
/// defaulting to now + `flush_interval`.
pub(crate) fn get_flush_to_disk_deadline(
    pws: &[Arc<PartWrapper>],
    flush_interval: std::time::Duration,
) -> Instant {
    let mut d = Instant::now() + flush_interval;
    for pw in pws {
        if pw.mp.is_some() && pw.flush_to_disk_deadline < d {
            d = pw.flush_to_disk_deadline;
        }
    }
    d
}

pub(crate) fn get_parts_size(pws: &[Arc<PartWrapper>]) -> u64 {
    pws.iter().map(|pw| pw.p.size).sum()
}

pub(crate) fn are_all_inmemory_parts(pws: &[Arc<PartWrapper>]) -> bool {
    pws.iter().all(|pw| pw.mp.is_some())
}

pub(crate) fn make_ptr_set(pws: &[Arc<PartWrapper>]) -> HashSet<*const PartWrapper> {
    let m: HashSet<*const PartWrapper> = pws.iter().map(Arc::as_ptr).collect();
    assert!(
        m.len() == pws.len(),
        "BUG: {} duplicate parts found in {} source parts",
        pws.len() - m.len(),
        pws.len()
    );
    m
}

/// Removes parts listed in `parts_to_remove` from `pws`. Returns the number
/// of removed parts.
pub(crate) fn remove_parts(
    pws: &mut Vec<Arc<PartWrapper>>,
    parts_to_remove: &HashSet<*const PartWrapper>,
) -> usize {
    let before = pws.len();
    pws.retain(|pw| !parts_to_remove.contains(&Arc::as_ptr(pw)));
    before - pws.len()
}

/// Finds optimal parts to merge from `src`.
///
/// The summary size of the returned parts must be smaller than `max_out_bytes`.
pub(crate) fn append_parts_to_merge(
    src: &[Arc<PartWrapper>],
    max_parts_to_merge: usize,
    max_out_bytes: u64,
) -> Vec<Arc<PartWrapper>> {
    if src.len() < 2 {
        // There is no need in merging zero or one part :)
        return Vec::new();
    }
    assert!(
        max_parts_to_merge >= 2,
        "BUG: maxPartsToMerge cannot be smaller than 2; got {max_parts_to_merge}"
    );

    // Filter out too big parts.
    // This should reduce N for the O(n^2) algorithm below.
    let max_in_part_bytes = (max_out_bytes as f64 / MIN_MERGE_MULTIPLIER) as u64;
    let mut src: Vec<Arc<PartWrapper>> = src
        .iter()
        .filter(|pw| pw.p.size <= max_in_part_bytes)
        .cloned()
        .collect();

    // Sort src parts by size.
    src.sort_by_key(|pw| pw.p.size);

    let max_src_parts = max_parts_to_merge.min(src.len());
    let min_src_parts = max_src_parts.div_ceil(2).max(2);

    // Exhaustive search for parts giving the lowest write amplification when merged.
    let mut best: Option<(usize, usize)> = None;
    let mut max_m = 0f64;
    for i in min_src_parts..=max_src_parts {
        for j in 0..=(src.len() - i) {
            let a = &src[j..j + i];
            if a[0].p.size * (a.len() as u64) < a[a.len() - 1].p.size {
                // Do not merge parts with too big difference in size,
                // since this results in unbalanced merges.
                continue;
            }
            let out_bytes: u64 = a.iter().map(|pw| pw.p.size).sum();
            if out_bytes > max_out_bytes {
                // There is no sense in checking the remaining bigger parts.
                break;
            }
            let m = out_bytes as f64 / a[a.len() - 1].p.size as f64;
            if m < max_m {
                continue;
            }
            max_m = m;
            best = Some((j, i));
        }
    }

    let min_m = (max_parts_to_merge as f64 / 2.0).max(MIN_MERGE_MULTIPLIER);
    if max_m < min_m {
        // There is no sense in merging parts with too small m,
        // since this leads to high disk write IO.
        return Vec::new();
    }
    let (j, i) = best.expect("BUG: best merge window must be set when max_m >= min_m");
    src[j..j + i].to_vec()
}

/// Returns parts from `pws` for optimal merge, plus the remaining parts.
pub(crate) fn get_parts_for_optimal_merge(
    pws: Vec<Arc<PartWrapper>>,
) -> (Vec<Arc<PartWrapper>>, Vec<Arc<PartWrapper>>) {
    let pws_to_merge = append_parts_to_merge(&pws, DEFAULT_PARTS_TO_MERGE, u64::MAX);
    if pws_to_merge.is_empty() {
        return (pws, Vec::new());
    }

    let m = make_ptr_set(&pws_to_merge);
    let pws_remaining: Vec<Arc<PartWrapper>> = pws
        .iter()
        .filter(|pw| !m.contains(&Arc::as_ptr(pw)))
        .cloned()
        .collect();

    (pws_to_merge, pws_remaining)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inmemory_block::InmemoryBlock;

    fn fake_pw(size: u64) -> Arc<PartWrapper> {
        // Build a tiny real in-memory part, then override the reported size.
        let mut ib = InmemoryBlock::default();
        ib.add(b"x");
        let mp = InmemoryPart::init(&mut ib);
        let mut p = mp.new_part();
        p.size = size;
        Arc::new(PartWrapper {
            p: Arc::new(p),
            mp: Some(mp),
            must_drop: AtomicBool::new(false),
            is_in_merge: AtomicBool::new(false),
            flush_to_disk_deadline: Instant::now(),
        })
    }

    #[test]
    fn append_parts_to_merge_needs_at_least_two() {
        assert!(append_parts_to_merge(&[], 15, u64::MAX).is_empty());
        assert!(append_parts_to_merge(&[fake_pw(100)], 15, u64::MAX).is_empty());
    }

    #[test]
    fn append_parts_to_merge_merges_equal_parts() {
        // 15 equal parts have m = 15 >= 7.5, so they must be selected.
        let pws: Vec<_> = (0..15).map(|_| fake_pw(1000)).collect();
        let selected = append_parts_to_merge(&pws, 15, u64::MAX);
        assert_eq!(selected.len(), 15);
    }

    #[test]
    fn append_parts_to_merge_rejects_low_multiplier() {
        // Two parts give m ~= 2 < 7.5, so nothing must be merged.
        let pws = vec![fake_pw(1000), fake_pw(1000)];
        assert!(append_parts_to_merge(&pws, 15, u64::MAX).is_empty());
    }

    #[test]
    fn append_parts_to_merge_honors_max_out_bytes() {
        let pws: Vec<_> = (0..15).map(|_| fake_pw(1000)).collect();
        // The full merge would produce 15000 bytes, which exceeds the limit.
        assert!(append_parts_to_merge(&pws, 15, 5000).is_empty());
    }

    #[test]
    fn append_parts_to_merge_skips_unbalanced_windows() {
        // A single giant part must not be merged with tiny ones.
        let mut pws: Vec<_> = (0..14).map(|_| fake_pw(100)).collect();
        pws.push(fake_pw(1_000_000));
        let selected = append_parts_to_merge(&pws, 15, u64::MAX);
        assert_eq!(selected.len(), 14);
        assert!(selected.iter().all(|pw| pw.p.size == 100));
    }

    #[test]
    fn get_parts_for_optimal_merge_splits() {
        let pws: Vec<_> = (0..20).map(|_| fake_pw(1000)).collect();
        let (to_merge, remaining) = get_parts_for_optimal_merge(pws);
        assert_eq!(to_merge.len(), 15);
        assert_eq!(remaining.len(), 5);
    }

    #[test]
    fn get_parts_for_optimal_merge_returns_all_when_no_optimal() {
        let pws = vec![fake_pw(1000), fake_pw(1000)];
        let (to_merge, remaining) = get_parts_for_optimal_merge(pws);
        assert_eq!(to_merge.len(), 2);
        assert!(remaining.is_empty());
    }
}
