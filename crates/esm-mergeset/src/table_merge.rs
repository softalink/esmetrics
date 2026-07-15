//! The merge machinery from `table.go`: background mergers, `mergeParts`
//! and the atomic parts swap.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use crate::block_stream_reader::BlockStreamReader;
use crate::block_stream_writer::BlockStreamWriter;
use crate::inmemory_part::InmemoryPart;
use crate::merge::{merge_block_streams, MergeError};
use crate::part::must_open_file_part;
use crate::part_header::PartHeader;
use crate::part_wrapper::{
    append_parts_to_merge, are_all_inmemory_parts, get_flush_to_disk_deadline, get_parts_size,
    make_ptr_set, remove_parts, PartWrapper, DEFAULT_PARTS_TO_MERGE,
};
use crate::table::{
    file_parts_concurrency, file_parts_concurrency_cap, inmemory_parts_concurrency, TableInner,
    MAX_INMEMORY_PARTS,
};
use crate::table_parts::must_write_part_names;
use crate::util::Shutdown;

/// The maximum part size in bytes.
///
/// This number should be limited by the amount of time required to merge
/// parts of this summary size. The required time shouldn't exceed a day.
const MAX_PART_SIZE: u64 = 400_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PartType {
    Inmemory,
    File,
}

/// The result of a low-level parts merge: the destination part header plus
/// the four stream buffers for in-memory destinations.
pub(crate) type MergedPart = (PartHeader, Option<[Vec<u8>; 4]>);

/// The maximum number of items per part created by a merge which must be
/// cached in the OS page cache.
///
/// Such parts are usually frequently accessed, so it is good to cache their
/// contents in the OS page cache.
fn max_items_per_cached_part() -> u64 {
    let mem = esm_common::memory::remaining() as u64;
    // Production data shows that each item occupies ~4 bytes in the
    // compressed part. It is expected no more than DEFAULT_PARTS_TO_MERGE/2
    // parts exist in the OS page cache before they are merged into a bigger
    // part. Half of the remaining RAM must be left for lib/storage parts.
    (mem / (4 * DEFAULT_PARTS_TO_MERGE as u64)).max(1_000_000)
}

pub(crate) fn get_max_inmemory_part_size() -> u64 {
    // Allow up to 5% of memory for in-memory parts.
    let allowed = esm_common::memory::allowed() as f64;
    ((0.05 * allowed / MAX_INMEMORY_PARTS as f64) as u64).max(1_000_000)
}

pub(crate) fn get_compress_level(items_count: u64) -> i32 {
    if items_count <= 1 << 16 {
        // -5 is the minimum supported compression level for zstd.
        return -5;
    }
    if items_count <= 1 << 17 {
        return -4;
    }
    if items_count <= 1 << 18 {
        return -3;
    }
    if items_count <= 1 << 19 {
        return -2;
    }
    if items_count <= 1 << 20 {
        return -1;
    }
    if items_count <= 1 << 22 {
        return 1;
    }
    if items_count <= 1 << 25 {
        return 2;
    }
    3
}

fn get_dst_part_type(pws: &[Arc<PartWrapper>], is_final: bool) -> PartType {
    let dst_part_size = get_parts_size(pws);
    if is_final || dst_part_size > get_max_inmemory_part_size() {
        return PartType::File;
    }
    if !are_all_inmemory_parts(pws) {
        // If at least a single source part is located in file, then the
        // destination part must be in file for durability reasons.
        return PartType::File;
    }
    PartType::Inmemory
}

fn must_open_block_stream_readers(pws: &[Arc<PartWrapper>]) -> Vec<BlockStreamReader> {
    pws.iter()
        .map(|pw| match &pw.mp {
            Some(mp) => BlockStreamReader::from_inmemory_part(mp),
            None => BlockStreamReader::from_file_part(&pw.p.path),
        })
        .collect()
}

/// Returns optimal parts to merge from `parts` and marks them as being
/// merged. Must be called with the table parts lock held.
pub(crate) fn get_parts_to_merge(
    parts: &[Arc<PartWrapper>],
    max_out_bytes: u64,
) -> Vec<Arc<PartWrapper>> {
    let pws_remaining: Vec<Arc<PartWrapper>> = parts
        .iter()
        .filter(|pw| !pw.is_in_merge.load(Ordering::Relaxed))
        .cloned()
        .collect();

    let pws_to_merge = append_parts_to_merge(&pws_remaining, DEFAULT_PARTS_TO_MERGE, max_out_bytes);

    for pw in &pws_to_merge {
        assert!(
            !pw.is_in_merge.swap(true, Ordering::Relaxed),
            "BUG: partWrapper.isInMerge unexpectedly set to true"
        );
    }

    pws_to_merge
}

impl TableInner {
    pub(crate) fn next_merge_idx(&self) -> u64 {
        self.merge_idx.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub(crate) fn get_max_file_part_size(&self) -> u64 {
        let n = esm_common::fs::must_get_free_space(&self.path);
        // Divide free space by the max number of concurrent merges for file parts.
        (n / file_parts_concurrency_cap() as u64).min(MAX_PART_SIZE)
    }

    pub(crate) fn inmemory_parts_merger(self: &Arc<Self>) {
        loop {
            if self.is_read_only.load(Ordering::Acquire) {
                return;
            }
            let max_out_bytes = self.get_max_file_part_size();

            let pws = {
                let state = self.parts.lock();
                get_parts_to_merge(&state.inmemory_parts, max_out_bytes)
            };

            if pws.is_empty() {
                // Nothing to merge.
                return;
            }

            inmemory_parts_concurrency().acquire();
            let res = self.merge_parts(pws, Some(&self.shutdown), false);
            inmemory_parts_concurrency().release();

            match res {
                // Try merging additional parts.
                Ok(()) => continue,
                // Nothing to do - the merger has been stopped.
                Err(MergeError::ForciblyStopped) => return,
                Err(e) => panic!(
                    "FATAL: unrecoverable error when merging inmemory parts in {:?}: {e}",
                    self.path
                ),
            }
        }
    }

    pub(crate) fn file_parts_merger(self: &Arc<Self>) {
        loop {
            if self.is_read_only.load(Ordering::Acquire) {
                return;
            }
            let max_out_bytes = self.get_max_file_part_size();

            let pws = {
                let state = self.parts.lock();
                get_parts_to_merge(&state.file_parts, max_out_bytes)
            };

            if pws.is_empty() {
                // Nothing to merge.
                return;
            }

            file_parts_concurrency().acquire();
            let res = self.merge_parts(pws, Some(&self.shutdown), false);
            file_parts_concurrency().release();

            match res {
                // Try merging additional parts.
                Ok(()) => continue,
                // The merger has been stopped.
                Err(MergeError::ForciblyStopped) => return,
                Err(e) => panic!(
                    "FATAL: unrecoverable error when merging file parts in {:?}: {e}",
                    self.path
                ),
            }
        }
    }

    fn release_parts_to_merge(&self, pws: &[Arc<PartWrapper>]) {
        let _state = self.parts.lock();
        for pw in pws {
            assert!(
                pw.is_in_merge.swap(false, Ordering::Relaxed),
                "BUG: missing isInMerge flag on the part {:?}",
                pw.p.path
            );
        }
    }

    /// Merges `pws` into a single resulting part.
    ///
    /// The merge is stopped if `stop` is signaled. If `is_final` is set (or
    /// any source part is file-based, or the result is too big for memory),
    /// the resulting part is stored to disk.
    ///
    /// All the parts in `pws` must have `is_in_merge` set; it is reset before
    /// returning.
    pub(crate) fn merge_parts(
        self: &Arc<Self>,
        pws: Vec<Arc<PartWrapper>>,
        stop: Option<&Shutdown>,
        is_final: bool,
    ) -> Result<(), MergeError> {
        assert!(
            !pws.is_empty(),
            "BUG: empty pws cannot be passed to mergeParts()"
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
        stop: Option<&Shutdown>,
        is_final: bool,
    ) -> Result<(), MergeError> {
        let start_time = Instant::now();

        // Initialize the destination paths.
        let dst_part_type = get_dst_part_type(pws, is_final);
        let merge_idx = self.next_merge_idx();
        let dst_part_path: Option<PathBuf> = match dst_part_type {
            PartType::File => Some(self.path.join(format!("{merge_idx:016X}"))),
            PartType::Inmemory => None,
        };

        if is_final && pws.len() == 1 {
            if let Some(mp) = &pws[0].mp {
                // Fast path: flush a single in-memory part to disk.
                let dst = dst_part_path
                    .as_deref()
                    .expect("final merge must have a file path");
                mp.must_store_to_disk(dst);
                let pw_new = self.open_created_part(pws, None, dst_part_path.as_deref());
                self.swap_src_with_dst_parts(pws, pw_new, dst_part_type);
                return Ok(());
            }
        }

        // Prepare blockStreamReaders for the source parts.
        let mut bsrs = must_open_block_stream_readers(pws);

        // Prepare the blockStreamWriter for the destination part.
        let src_size: u64 = pws.iter().map(|pw| pw.p.size).sum();
        let src_items_count: u64 = pws.iter().map(|pw| pw.p.ph.items_count).sum();
        let src_blocks_count: u64 = pws.iter().map(|pw| pw.p.ph.blocks_count).sum();
        let compress_level = get_compress_level(src_items_count);
        let mut bsw = match dst_part_type {
            PartType::Inmemory => BlockStreamWriter::new_inmemory_part(compress_level),
            PartType::File => {
                let nocache = src_items_count > max_items_per_cached_part();
                BlockStreamWriter::new_file_part(
                    dst_part_path
                        .as_deref()
                        .expect("file part must have a path"),
                    nocache,
                    compress_level,
                )
            }
        };

        // Merge the source parts into the destination part.
        let res = self.merge_parts_internal(
            dst_part_path.as_deref(),
            &mut bsw,
            &mut bsrs,
            dst_part_type,
            stop,
        );
        for bsr in &mut bsrs {
            bsr.must_close();
        }
        let (ph, bufs) = res?;

        let mp_new: Option<InmemoryPart> = match dst_part_type {
            PartType::Inmemory => Some(InmemoryPart::from_buffers(
                ph,
                bufs.expect("BUG: in-memory merge must return buffers"),
            )),
            PartType::File => {
                // Make sure the created part directory listing is synced.
                let dst = dst_part_path
                    .as_deref()
                    .expect("file part must have a path");
                esm_common::fs::must_sync_path_and_parent_dir(dst);
                None
            }
        };

        // Atomically swap the source parts with the newly created part.
        let pw_new = self.open_created_part(pws, mp_new, dst_part_path.as_deref());
        let p_dst = &pw_new.p;
        let dst_items_count = p_dst.ph.items_count;
        let dst_blocks_count = p_dst.ph.blocks_count;
        let dst_size = p_dst.size;

        self.swap_src_with_dst_parts(pws, pw_new, dst_part_type);

        let d = start_time.elapsed();
        if d.as_secs() <= 30 {
            return Ok(());
        }

        // Log stats for long merges.
        let duration_secs = d.as_secs_f64();
        let items_per_sec = (src_items_count as f64 / duration_secs) as u64;
        log::info!(
            "merged ({} parts, {src_items_count} items, {src_blocks_count} blocks, {src_size} bytes) \
             into (1 part, {dst_items_count} items, {dst_blocks_count} blocks, {dst_size} bytes) \
             in {duration_secs:.3} seconds at {items_per_sec} items/sec to {dst_part_path:?}",
            pws.len()
        );

        Ok(())
    }

    pub(crate) fn merge_parts_internal(
        &self,
        dst_part_path: Option<&Path>,
        bsw: &mut BlockStreamWriter,
        bsrs: &mut [BlockStreamReader],
        dst_part_type: PartType,
        stop: Option<&Shutdown>,
    ) -> Result<MergedPart, MergeError> {
        let (items_merged, merges_count, active_merges) = match dst_part_type {
            PartType::Inmemory => (
                &self.inmemory_items_merged,
                &self.inmemory_merges_count,
                &self.active_inmemory_merges,
            ),
            PartType::File => (
                &self.file_items_merged,
                &self.file_merges_count,
                &self.active_file_merges,
            ),
        };

        let mut ph = PartHeader::default();
        active_merges.fetch_add(1, Ordering::Relaxed);
        let res = merge_block_streams(
            &mut ph,
            bsw,
            bsrs,
            self.prepare_block.as_ref(),
            stop,
            items_merged,
        );
        active_merges.fetch_sub(1, Ordering::Relaxed);
        merges_count.fetch_add(1, Ordering::Relaxed);

        let bufs = match res {
            Ok(bufs) => bufs,
            Err(MergeError::ForciblyStopped) => return Err(MergeError::ForciblyStopped),
            Err(MergeError::Other(msg)) => {
                return Err(MergeError::Other(format!(
                    "cannot merge {} parts to {dst_part_path:?}: {msg}",
                    bsrs.len()
                )))
            }
        };
        if let Some(path) = dst_part_path {
            ph.must_write_metadata(path);
        }
        Ok((ph, bufs))
    }

    fn open_created_part(
        &self,
        pws: &[Arc<PartWrapper>],
        mp_new: Option<InmemoryPart>,
        dst_part_path: Option<&Path>,
    ) -> Arc<PartWrapper> {
        if let Some(mp) = mp_new {
            // Open the created part from memory.
            let flush_to_disk_deadline = get_flush_to_disk_deadline(pws, self.flush_interval);
            return PartWrapper::new_from_inmemory_part(mp, flush_to_disk_deadline);
        }
        // Open the created part from disk.
        let p_new = must_open_file_part(dst_part_path.expect("file part must have a path"));
        PartWrapper::new_from_file_part(p_new)
    }

    fn swap_src_with_dst_parts(
        self: &Arc<Self>,
        pws: &[Arc<PartWrapper>],
        pw_new: Arc<PartWrapper>,
        dst_part_type: PartType,
    ) {
        // Atomically unregister the old parts and add the new part to the table.
        let m = make_ptr_set(pws);

        let removed_inmemory_parts;
        let removed_file_parts;

        {
            let mut state = self.parts.lock();

            removed_inmemory_parts = remove_parts(&mut state.inmemory_parts, &m);
            removed_file_parts = remove_parts(&mut state.file_parts, &m);
            match dst_part_type {
                PartType::Inmemory => {
                    state.inmemory_parts.push(Arc::clone(&pw_new));
                    self.start_inmemory_parts_merger_locked(&state);
                }
                PartType::File => {
                    state.file_parts.push(Arc::clone(&pw_new));
                    self.start_file_parts_merger_locked(&state);
                }
            }

            // Atomically store the updated list of file-based parts on disk.
            // This must be performed under the parts lock in order to prevent
            // from races when multiple concurrently running threads update
            // the list.
            if removed_file_parts > 0 || dst_part_type == PartType::File {
                must_write_part_names(&state.file_parts, &self.path);
            }
        }

        // Update the in-memory parts limit accordingly to the number of the
        // removed in-memory parts.
        for _ in 0..removed_inmemory_parts {
            self.inmemory_parts_limit.release();
        }
        if dst_part_type == PartType::Inmemory {
            self.inmemory_parts_limit.acquire_or_stop(&self.shutdown);
        }

        let removed_parts = removed_inmemory_parts + removed_file_parts;
        assert!(
            removed_parts == m.len(),
            "BUG: unexpected number of parts removed; got {removed_parts}, want {}",
            m.len()
        );

        // Mark the old parts as ready for deletion. The references are
        // released when the caller drops its part wrapper clones.
        for pw in pws {
            pw.must_drop.store(true, Ordering::Release);
        }
    }
}
