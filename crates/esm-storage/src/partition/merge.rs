//! Merge scheduling, part-selection heuristics, `mergeParts` and the
//! parts.json persistence of partition.go.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::block_stream::{
    get_compress_level, merge_block_streams, BlockStreamReader, BlockStreamWriter, MergeError,
};
use crate::part::{must_open_file_part, InmemoryPart, PartHeader};
use crate::partition::{
    are_all_inmemory_parts, big_parts_concurrency, get_flush_to_disk_deadline, get_parts_size,
    inmemory_parts_concurrency, make_ptr_set, remove_parts, small_parts_concurrency, PartWrapper,
    PtInner, DEFAULT_PARTS_TO_MERGE, MAX_BIG_PART_SIZE, MAX_INMEMORY_PARTS,
};
use crate::sync_util::{now_unix_milli, Sema};

/// The name of the file listing the live file parts of a partition.
pub(crate) const PARTS_FILENAME: &str = "parts.json";

/// The result of a low-level parts merge: the merged part header plus the
/// four stream buffers when the destination is an in-memory part.
pub(crate) type MergeOutput = (PartHeader, Option<[Vec<u8>; 4]>);

/// The minimum multiplier for the size of the output part compared to the
/// size of the maximum input part for the merge. Go: minMergeMultiplier.
pub(crate) const MIN_MERGE_MULTIPLIER: f64 = 1.7;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PartType {
    Inmemory,
    Small,
    Big,
}

/// Go: getMaxInmemoryPartSize.
pub(crate) fn get_max_inmemory_part_size() -> u64 {
    // Allocate 10% of allowed memory for in-memory parts.
    ((0.1 * esm_common::memory::allowed() as f64) as u64 / MAX_INMEMORY_PARTS as u64).max(1_000_000)
}

/// Go: getMaxOutBytes.
fn get_max_out_bytes(path: &Path, workers_count: u64) -> u64 {
    let n = esm_common::fs::must_get_free_space(path);
    // Divide free space by the max number of concurrent merges.
    (n / workers_count).min(MAX_BIG_PART_SIZE)
}

impl PtInner {
    /// Go: partition.getMaxSmallPartSize.
    fn get_max_small_part_size(&self) -> u64 {
        // Small parts are cached in the OS page cache, so limit their size
        // by the remaining free RAM.
        let mem = esm_common::memory::remaining() as u64;
        let n = (mem / DEFAULT_PARTS_TO_MERGE as u64).max(10_000_000);
        // Make sure the output part fits available disk space for small parts.
        let size_limit = get_max_out_bytes(
            &self.small_parts_path,
            small_parts_concurrency().capacity() as u64,
        );
        n.min(size_limit)
    }

    /// Go: partition.getMaxBigPartSize.
    pub(crate) fn get_max_big_part_size(&self) -> u64 {
        // Always use 4 workers for big merges due to historical reasons.
        get_max_out_bytes(&self.big_parts_path, 4)
    }

    pub(crate) fn next_merge_idx(&self) -> u64 {
        self.merge_idx.fetch_add(1, Ordering::Relaxed) + 1
    }

    // --- background mergers (Go: {inmemory,small,big}PartsMerger) ---

    pub(crate) fn inmemory_parts_merger(self: &Arc<Self>) {
        self.parts_merger(
            |state| &state.inmemory_parts,
            inmemory_parts_concurrency(),
            "inmemory",
        );
    }

    pub(crate) fn small_parts_merger(self: &Arc<Self>) {
        self.parts_merger(
            |state| &state.small_parts,
            small_parts_concurrency(),
            "small",
        );
    }

    pub(crate) fn big_parts_merger(self: &Arc<Self>) {
        self.parts_merger(|state| &state.big_parts, big_parts_concurrency(), "big");
    }

    fn parts_merger(
        self: &Arc<Self>,
        list: impl Fn(&super::PartsState) -> &Vec<Arc<PartWrapper>>,
        sema: &Sema,
        what: &str,
    ) {
        loop {
            if self.env.is_read_only.load(Ordering::Acquire) {
                return;
            }
            let max_out_bytes = self.get_max_big_part_size();

            let pws = {
                let state = self.parts.lock();
                get_parts_to_merge(list(&state), max_out_bytes)
            };
            if pws.is_empty() {
                // Nothing to merge.
                return;
            }

            if !sema.acquire_or_stop(&self.shutdown) {
                self.release_parts_to_merge(&pws);
                return;
            }
            let res = self.merge_parts(pws, Some(self.shutdown.flag()), false);
            sema.release();

            match res {
                // Try merging additional parts.
                Ok(()) => continue,
                // Nothing to do - finish the merger.
                Err(MergeError::ForciblyStopped) => return,
                Err(err) => panic!(
                    "FATAL: unrecoverable error when merging {what} parts in partition {:?}: {err}",
                    self.name
                ),
            }
        }
    }

    pub(crate) fn release_parts_to_merge(&self, pws: &[Arc<PartWrapper>]) {
        let _state = self.parts.lock();
        for pw in pws {
            assert!(
                pw.is_in_merge.load(Ordering::Relaxed),
                "BUG: missing isInMerge flag on the part {:?}",
                pw.p.path
            );
            pw.is_in_merge.store(false, Ordering::Relaxed);
        }
    }

    // --- merge entry points ---

    /// Merges the given parts (optimal groups at a time) into file parts.
    /// Go: partition.mergePartsToFiles.
    pub(crate) fn merge_parts_to_files(
        self: &Arc<Self>,
        mut pws: Vec<Arc<PartWrapper>>,
        stop: Option<&AtomicBool>,
        sema: &Sema,
    ) -> Result<(), String> {
        let pws_len = pws.len();
        let err_global: Mutex<Option<String>> = Mutex::new(None);
        std::thread::scope(|scope| {
            while !pws.is_empty() {
                let (pws_to_merge, pws_remaining) = get_parts_for_optimal_merge(pws);
                pws = pws_remaining;
                sema.acquire();
                let err_global = &err_global;
                scope.spawn(move || {
                    match self.merge_parts(pws_to_merge, stop, true) {
                        Ok(()) | Err(MergeError::ForciblyStopped) => {}
                        Err(err) => {
                            let mut guard = err_global.lock();
                            if guard.is_none() {
                                *guard = Some(err.to_string());
                            }
                        }
                    }
                    sema.release();
                });
            }
        });

        match err_global.into_inner() {
            Some(err) => Err(format!("cannot merge {pws_len} parts optimally: {err}")),
            None => Ok(()),
        }
    }

    /// Go: partition.ForceMergeAllParts.
    pub(crate) fn force_merge_all_parts(
        self: &Arc<Self>,
        stop: Option<&AtomicBool>,
    ) -> Result<(), String> {
        let pws = self.get_all_parts_for_merge();
        if pws.is_empty() {
            // Nothing to merge.
            return Ok(());
        }

        // Check whether there is enough disk space for merging pws.
        let new_part_size = get_parts_size(&pws);
        let max_out_bytes = esm_common::fs::must_get_free_space(&self.big_parts_path);
        if new_part_size > max_out_bytes {
            log::warn!(
                "cannot initiate force merge for the partition {}; additional space needed: {} bytes",
                self.name,
                new_part_size - max_out_bytes
            );
            self.release_parts_to_merge(&pws);
            return Ok(());
        }

        // If len(pws) == 1, then the merge must run anyway. This allows
        // applying the configured retention, removing the deleted series and
        // performing de-duplication if needed.
        let n = pws.len();
        self.merge_parts_to_files(pws, stop, big_parts_concurrency())
            .map_err(|err| {
                format!(
                    "cannot force merge {n} parts from partition {:?}: {err}",
                    self.name
                )
            })
    }

    /// Go: partition.getAllPartsForMerge.
    fn get_all_parts_for_merge(&self) -> Vec<Arc<PartWrapper>> {
        let mut pws = Vec::new();
        let state = self.parts.lock();
        let has_active = has_active_merges(&state.inmemory_parts)
            || has_active_merges(&state.small_parts)
            || has_active_merges(&state.big_parts);
        if !has_active {
            append_all_parts_for_merge(&mut pws, &state.inmemory_parts);
            append_all_parts_for_merge(&mut pws, &state.small_parts);
            append_all_parts_for_merge(&mut pws, &state.big_parts);
        }
        pws
    }

    // --- mergeParts (Go: partition.mergeParts) ---

    /// Merges pws into a single resulting part. All the parts inside pws
    /// must have the `is_in_merge` flag set; it is cleared before returning.
    pub(crate) fn merge_parts(
        self: &Arc<Self>,
        pws: Vec<Arc<PartWrapper>>,
        stop: Option<&AtomicBool>,
        is_final: bool,
    ) -> Result<(), MergeError> {
        assert!(
            !pws.is_empty(),
            "BUG: empty pws cannot be passed to merge_parts()"
        );
        for pw in &pws {
            assert!(
                pw.is_in_merge.load(Ordering::Relaxed),
                "BUG: partWrapper.isInMerge unexpectedly set to false"
            );
        }

        let res = self.merge_parts_inner(&pws, stop, is_final);
        self.release_parts_to_merge(&pws);
        res
    }

    fn merge_parts_inner(
        self: &Arc<Self>,
        pws: &[Arc<PartWrapper>],
        stop: Option<&AtomicBool>,
        is_final: bool,
    ) -> Result<(), MergeError> {
        // Initialize the destination paths.
        let dst_part_type = self.get_dst_part_type(pws, is_final);
        let merge_idx = self.next_merge_idx();
        let dst_part_path = self.get_dst_part_path(dst_part_type, merge_idx);

        if !crate::dedup::is_dedup_enabled() && is_final && pws.len() == 1 && pws[0].mp.is_some() {
            // Fast path: flush a single in-memory part to disk.
            let mp = pws[0].mp.as_ref().unwrap();
            let dst_part_path = dst_part_path.expect("BUG: dstPartPath must be non-empty");
            mp.must_store_to_disk(&dst_part_path);
            let pw_new = PartWrapper::new_from_file_part(must_open_file_part(&dst_part_path));
            self.swap_src_with_dst_parts(pws, Some(pw_new), dst_part_type);
            return Ok(());
        }

        // Prepare BlockStreamReaders for the source parts.
        let mut bsrs: Vec<BlockStreamReader> = pws
            .iter()
            .map(|pw| match &pw.mp {
                Some(mp) => BlockStreamReader::from_inmemory_part(mp),
                None => BlockStreamReader::from_file_part(&pw.p.path),
            })
            .collect();

        // Prepare BlockStreamWriter for the destination part.
        let src_rows_count: u64 = pws.iter().map(|pw| pw.p.ph.rows_count).sum();
        let src_blocks_count: u64 = pws.iter().map(|pw| pw.p.ph.blocks_count).sum();
        let compress_level =
            get_compress_level(src_rows_count as f64 / src_blocks_count.max(1) as f64);
        let current_timestamp = now_unix_milli();
        let mut bsw = match &dst_part_path {
            None => BlockStreamWriter::new_inmemory_part(compress_level),
            Some(path) => {
                let nocache = dst_part_type == PartType::Big;
                BlockStreamWriter::new_file_part(path, nocache, compress_level)
            }
        };

        // Merge the source parts into the destination part.
        let (ph, bufs) = self.merge_parts_internal(
            dst_part_path.as_deref(),
            &mut bsw,
            &mut bsrs,
            dst_part_type,
            stop,
            current_timestamp,
        )?;
        drop(bsrs);

        let mp_new = if dst_part_type == PartType::Inmemory {
            Some(InmemoryPart::from_buffers(
                ph,
                bufs.expect("BUG: in-memory merge must return buffers"),
            ))
        } else {
            // Make sure the created part directory listing is synced.
            let path = dst_part_path.as_ref().unwrap();
            esm_common::fs::must_sync_path_and_parent_dir(path);
            None
        };

        // Atomically swap the source parts with the newly created part.
        let pw_new = open_created_part(&ph, pws, mp_new, dst_part_path.as_deref());
        self.swap_src_with_dst_parts(pws, pw_new, dst_part_type);
        Ok(())
    }

    fn get_dst_part_type(&self, pws: &[Arc<PartWrapper>], is_final: bool) -> PartType {
        let dst_part_size = get_parts_size(pws);
        if dst_part_size > self.get_max_small_part_size() {
            return PartType::Big;
        }
        if is_final || dst_part_size > get_max_inmemory_part_size() {
            return PartType::Small;
        }
        if !are_all_inmemory_parts(pws) {
            // If at least a single source part is located in a file, then
            // the destination part must be in a file for durability reasons.
            return PartType::Small;
        }
        PartType::Inmemory
    }

    fn get_dst_part_path(&self, dst_part_type: PartType, merge_idx: u64) -> Option<PathBuf> {
        match dst_part_type {
            PartType::Inmemory => None,
            PartType::Small => Some(self.small_parts_path.join(format!("{merge_idx:016X}"))),
            PartType::Big => Some(self.big_parts_path.join(format!("{merge_idx:016X}"))),
        }
    }

    /// Go: partition.mergePartsInternal.
    pub(crate) fn merge_parts_internal(
        &self,
        dst_part_path: Option<&Path>,
        bsw: &mut BlockStreamWriter,
        bsrs: &mut [BlockStreamReader],
        dst_part_type: PartType,
        stop: Option<&AtomicBool>,
        current_timestamp: i64,
    ) -> Result<MergeOutput, MergeError> {
        let (rows_merged, rows_deleted, merges_count) = match dst_part_type {
            PartType::Inmemory => (
                &self.inmemory_rows_merged,
                &self.inmemory_rows_deleted,
                &self.inmemory_merges_count,
            ),
            PartType::Small => (
                &self.small_rows_merged,
                &self.small_rows_deleted,
                &self.small_merges_count,
            ),
            PartType::Big => (
                &self.big_rows_merged,
                &self.big_rows_deleted,
                &self.big_merges_count,
            ),
        };
        let retention_deadline = current_timestamp - self.env.retention_msecs;
        let dmis = self.idb.get_deleted_metric_ids();
        let mut ph = PartHeader::default();
        let res = merge_block_streams(
            &mut ph,
            bsw,
            bsrs,
            stop,
            Some(&dmis),
            retention_deadline,
            rows_merged,
            rows_deleted,
        );
        merges_count.fetch_add(1, Ordering::Relaxed);
        let bufs = res.map_err(|err| match err {
            MergeError::ForciblyStopped => MergeError::ForciblyStopped,
            MergeError::Other(msg) => MergeError::Other(format!(
                "cannot merge {} parts to {dst_part_path:?}: {msg}",
                bsrs.len()
            )),
        })?;
        if let Some(path) = dst_part_path {
            ph.min_dedup_interval = crate::dedup::get_dedup_interval();
            ph.must_write_metadata(path);
        }
        Ok((ph, bufs))
    }

    /// Go: partition.swapSrcWithDstParts.
    pub(crate) fn swap_src_with_dst_parts(
        self: &Arc<Self>,
        pws: &[Arc<PartWrapper>],
        pw_new: Option<Arc<PartWrapper>>,
        dst_part_type: PartType,
    ) {
        // Atomically unregister the old parts and add the new part to pt.
        let m = make_ptr_set(pws);

        let removed_inmemory_parts;
        let removed_small_parts;
        let removed_big_parts;
        {
            let mut state = self.parts.lock();

            removed_inmemory_parts = remove_parts(&mut state.inmemory_parts, &m);
            removed_small_parts = remove_parts(&mut state.small_parts, &m);
            removed_big_parts = remove_parts(&mut state.big_parts, &m);
            if let Some(pw_new) = &pw_new {
                match dst_part_type {
                    PartType::Inmemory => {
                        state.inmemory_parts.push(Arc::clone(pw_new));
                        self.start_inmemory_parts_merger_locked(&state);
                    }
                    PartType::Small => {
                        state.small_parts.push(Arc::clone(pw_new));
                        self.start_small_parts_merger_locked(&state);
                    }
                    PartType::Big => {
                        state.big_parts.push(Arc::clone(pw_new));
                        self.start_big_parts_merger_locked(&state);
                    }
                }
            }

            // Atomically store the updated list of file-based parts on disk.
            // This must be performed under the parts lock in order to
            // prevent from races when multiple concurrently running threads
            // update the list.
            if removed_small_parts > 0
                || removed_big_parts > 0
                || (pw_new.is_some() && dst_part_type != PartType::Inmemory)
            {
                must_write_part_names(&state.small_parts, &state.big_parts, &self.small_parts_path);
            }
        }

        let removed_parts = removed_inmemory_parts + removed_small_parts + removed_big_parts;
        assert!(
            removed_parts == m.len(),
            "BUG: unexpected number of parts removed; got {removed_parts}, want {}",
            m.len()
        );

        // Mark the old parts as must-be-deleted. The parts are closed and
        // deleted once the last search using them drops its references.
        for pw in pws {
            pw.must_drop.store(true, Ordering::Release);
        }
    }

    /// Go: partition.removeStaleParts.
    pub(crate) fn remove_stale_parts(self: &Arc<Self>) {
        let retention_deadline = now_unix_milli() - self.env.retention_msecs;

        let mut pws = Vec::new();
        {
            let state = self.parts.lock();
            let mut collect = |list: &Vec<Arc<PartWrapper>>, rows_deleted: &AtomicU64| {
                for pw in list {
                    if !pw.is_in_merge.load(Ordering::Relaxed)
                        && pw.p.ph.max_timestamp < retention_deadline
                    {
                        rows_deleted.fetch_add(pw.p.ph.rows_count, Ordering::Relaxed);
                        pw.is_in_merge.store(true, Ordering::Relaxed);
                        pws.push(Arc::clone(pw));
                    }
                }
            };
            collect(&state.inmemory_parts, &self.inmemory_rows_deleted);
            collect(&state.small_parts, &self.small_rows_deleted);
            collect(&state.big_parts, &self.big_rows_deleted);
        }

        self.swap_src_with_dst_parts(&pws, None, PartType::Small);
    }
}

fn has_active_merges(pws: &[Arc<PartWrapper>]) -> bool {
    pws.iter().any(|pw| pw.is_in_merge.load(Ordering::Relaxed))
}

fn append_all_parts_for_merge(dst: &mut Vec<Arc<PartWrapper>>, src: &[Arc<PartWrapper>]) {
    for pw in src {
        assert!(
            !pw.is_in_merge.load(Ordering::Relaxed),
            "BUG: part {:?} is already in merge",
            pw.p.path
        );
        pw.is_in_merge.store(true, Ordering::Relaxed);
        dst.push(Arc::clone(pw));
    }
}

/// Go: partition.openCreatedPart.
fn open_created_part(
    ph: &PartHeader,
    pws: &[Arc<PartWrapper>],
    mp_new: Option<InmemoryPart>,
    dst_part_path: Option<&Path>,
) -> Option<Arc<PartWrapper>> {
    if ph.rows_count == 0 {
        // The created part is empty. Remove it.
        if mp_new.is_none() {
            esm_common::fs::must_remove_dir(dst_part_path.expect("BUG: missing dstPartPath"));
        }
        return None;
    }
    if let Some(mp) = mp_new {
        // Open the created part from memory.
        let flush_to_disk_deadline = get_flush_to_disk_deadline(pws);
        return Some(PartWrapper::new_from_inmemory_part(
            mp,
            flush_to_disk_deadline,
        ));
    }
    // Open the created part from disk.
    let p_new = must_open_file_part(dst_part_path.expect("BUG: missing dstPartPath"));
    Some(PartWrapper::new_from_file_part(p_new))
}

// --- part-selection heuristics (Go: getPartsToMerge / appendPartsToMerge) ---

/// Returns optimal parts to merge from `pws`, marking them as in-merge.
/// The summary size of the returned parts is smaller than `max_out_bytes`.
/// Must be called under the partition parts lock. Go: getPartsToMerge.
pub(crate) fn get_parts_to_merge(
    pws: &[Arc<PartWrapper>],
    max_out_bytes: u64,
) -> Vec<Arc<PartWrapper>> {
    let pws_remaining: Vec<Arc<PartWrapper>> = pws
        .iter()
        .filter(|pw| !pw.is_in_merge.load(Ordering::Relaxed))
        .cloned()
        .collect();

    let pws_to_merge = append_parts_to_merge(&pws_remaining, DEFAULT_PARTS_TO_MERGE, max_out_bytes);

    for pw in &pws_to_merge {
        assert!(
            !pw.is_in_merge.load(Ordering::Relaxed),
            "BUG: partWrapper.isInMerge cannot be set"
        );
        pw.is_in_merge.store(true, Ordering::Relaxed);
    }
    pws_to_merge
}

/// Returns parts from `pws` for optimal merge, plus the remaining parts.
/// Go: getPartsForOptimalMerge.
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

/// Finds optimal parts to merge from `src`. The summary size of the returned
/// parts must be smaller than `max_out_bytes`. Go: appendPartsToMerge.
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

    // Filter out too big parts. This should reduce N for the O(N^2)
    // algorithm below.
    let max_in_part_bytes = (max_out_bytes as f64 / MIN_MERGE_MULTIPLIER) as u64;
    let mut src: Vec<Arc<PartWrapper>> = src
        .iter()
        .filter(|pw| pw.p.size <= max_in_part_bytes)
        .cloned()
        .collect();

    // Sort src parts by size and backwards timestamp. This should improve
    // adjacent points' locality in the merged parts.
    src.sort_by(|a, b| {
        a.p.size
            .cmp(&b.p.size)
            .then_with(|| b.p.ph.min_timestamp.cmp(&a.p.ph.min_timestamp))
    });

    let max_src_parts = max_parts_to_merge.min(src.len());
    let min_src_parts = max_src_parts.div_ceil(2).max(2);

    // Exhaustive search for parts giving the lowest write amplification
    // when merged.
    let mut best: Option<(usize, usize)> = None;
    let mut max_m = 0f64;
    for i in min_src_parts..=max_src_parts {
        for j in 0..=(src.len() - i) {
            let a = &src[j..j + i];
            if a[0].p.size * (a.len() as u64) < a[a.len() - 1].p.size {
                // Do not merge parts with too big difference in size, since
                // this results in unbalanced merges.
                continue;
            }
            let out_size: u64 = a.iter().map(|pw| pw.p.size).sum();
            if out_size > max_out_bytes {
                // There is no need in verifying remaining parts with bigger
                // sizes.
                break;
            }
            let m = out_size as f64 / a[a.len() - 1].p.size as f64;
            if m < max_m {
                continue;
            }
            max_m = m;
            best = Some((j, i));
        }
    }

    let min_m = (max_parts_to_merge as f64 / 2.0).max(MIN_MERGE_MULTIPLIER);
    if max_m < min_m {
        // There is no sense in merging parts with too small m, since this
        // leads to high disk write IO.
        return Vec::new();
    }
    let (j, i) = best.expect("BUG: best merge window must be set when max_m >= min_m");
    src[j..j + i].to_vec()
}

// --- parts.json (Go: mustWritePartNames / mustReadPartNames / mustOpenParts) ---

#[derive(Serialize, Deserialize, Default)]
struct PartNamesJson {
    #[serde(rename = "Small", default)]
    small: Vec<String>,
    #[serde(rename = "Big", default)]
    big: Vec<String>,
}

fn get_part_names(pws: &[Arc<PartWrapper>]) -> Vec<String> {
    let mut names: Vec<String> = pws
        .iter()
        .filter(|pw| pw.mp.is_none()) // skip in-memory parts
        .map(|pw| {
            pw.p.path
                .file_name()
                .unwrap_or_else(|| panic!("BUG: part path {:?} has no base name", pw.p.path))
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

pub(crate) fn must_write_part_names(
    pws_small: &[Arc<PartWrapper>],
    pws_big: &[Arc<PartWrapper>],
    dst_dir: &Path,
) {
    let part_names = PartNamesJson {
        small: get_part_names(pws_small),
        big: get_part_names(pws_big),
    };
    let data = serde_json::to_vec(&part_names)
        .unwrap_or_else(|err| panic!("BUG: cannot marshal partNames to JSON: {err}"));
    let parts_file = dst_dir.join(PARTS_FILENAME);
    esm_common::fs::must_write_atomic(&parts_file, &data, true);
}

pub(crate) fn must_read_part_names(
    parts_file: &Path,
    small_parts_path: &Path,
    big_parts_path: &Path,
) -> (Vec<String>, Vec<String>) {
    if esm_common::fs::is_path_exist(parts_file) {
        let data = std::fs::read(parts_file)
            .unwrap_or_else(|err| panic!("FATAL: cannot read {parts_file:?}: {err}"));
        let part_names: PartNamesJson = serde_json::from_slice(&data)
            .unwrap_or_else(|err| panic!("FATAL: cannot parse {parts_file:?}: {err}"));
        return (part_names.small, part_names.big);
    }
    // The parts file is missing. Read part names from the part directories.
    (
        must_read_part_names_from_dir(small_parts_path),
        must_read_part_names_from_dir(big_parts_path),
    )
}

fn must_read_part_names_from_dir(src_dir: &Path) -> Vec<String> {
    if !esm_common::fs::is_path_exist(src_dir) {
        return Vec::new();
    }
    let mut part_names = Vec::new();
    for de in esm_common::fs::must_read_dir(src_dir) {
        if !esm_common::fs::is_dir_or_symlink(&de) {
            // Skip non-directories.
            continue;
        }
        let part_name = de.file_name().to_string_lossy().into_owned();
        if is_special_dir(&part_name) {
            // Skip special dirs.
            continue;
        }
        part_names.push(part_name);
    }
    part_names
}

fn is_special_dir(name: &str) -> bool {
    name == "tmp" || name == "txn" || name == "snapshots"
}

/// Opens the file parts listed in `part_names` under `path`, removing
/// unlisted directories left after unclean shutdown. Go: mustOpenParts.
pub(crate) fn must_open_parts(
    parts_file: &Path,
    path: &Path,
    part_names: &[String],
) -> Vec<Arc<PartWrapper>> {
    // Remove txn and tmp directories, which may be left after unclean
    // shutdown in old layouts.
    esm_common::fs::must_remove_dir(path.join("txn"));
    esm_common::fs::must_remove_dir(path.join("tmp"));

    // Remove dirs missing in part_names. These dirs may be left after
    // unclean shutdown.
    for part_name in part_names {
        let part_path = path.join(part_name);
        assert!(
            esm_common::fs::is_path_exist(&part_path),
            "FATAL: part {part_path:?} is listed in {parts_file:?}, but is missing on disk; \
             remove it from {parts_file:?} in order to restore access to the remaining data"
        );
    }
    for de in esm_common::fs::must_read_dir(path) {
        if !esm_common::fs::is_dir_or_symlink(&de) {
            // Skip non-directories.
            continue;
        }
        let fn_name = de.file_name().to_string_lossy().into_owned();
        if !part_names.iter().any(|n| n == &fn_name) {
            let delete_path = path.join(&fn_name);
            log::info!(
                "deleting {delete_path:?} because it isn't listed in {parts_file:?}; \
                 this is the expected case after unclean shutdown"
            );
            esm_common::fs::must_remove_dir(&delete_path);
        }
    }

    // Open the parts.
    part_names
        .iter()
        .map(|part_name| {
            PartWrapper::new_from_file_part(must_open_file_part(&path.join(part_name)))
        })
        .collect()
}
