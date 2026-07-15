//! Port of `part.go`, including the process-global block caches
//! (`idxbCache`, `ibCache`, `ibSparseCache` in Go).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use crate::block_header::BlockHeader;
use crate::blockcache::{Cache, CachedBlock};
use crate::filenames::{INDEX_FILENAME, ITEMS_FILENAME, LENS_FILENAME, METAINDEX_FILENAME};
use crate::inmemory_block::InmemoryBlock;
use crate::metaindex_row::{unmarshal_metaindex_rows, MetaindexRow};
use crate::part_header::PartHeader;

// --- process-global block caches (Go: idxbCache, ibCache, ibSparseCache) ---

static IDXB_CACHE: OnceLock<Cache<IndexBlock>> = OnceLock::new();
static IB_CACHE: OnceLock<Cache<InmemoryBlock>> = OnceLock::new();
static IB_SPARSE_CACHE: OnceLock<Cache<InmemoryBlock>> = OnceLock::new();

/// The process-global cache of decompressed index blocks.
pub(crate) fn idxb_cache() -> &'static Cache<IndexBlock> {
    IDXB_CACHE.get_or_init(|| Cache::new(get_max_index_blocks_cache_size))
}

/// The process-global cache of decoded data blocks.
pub(crate) fn ib_cache() -> &'static Cache<InmemoryBlock> {
    IB_CACHE.get_or_init(|| Cache::new(get_max_inmemory_blocks_cache_size))
}

/// The process-global cache of decoded data blocks read by sparse
/// (cache-unfriendly) searches.
pub(crate) fn ib_sparse_cache() -> &'static Cache<InmemoryBlock> {
    IB_SPARSE_CACHE.get_or_init(|| Cache::new(get_max_inmemory_blocks_sparse_cache_size))
}

static MAX_INDEX_BLOCK_CACHE_SIZE: AtomicUsize = AtomicUsize::new(0);
static MAX_INMEMORY_BLOCK_CACHE_SIZE: AtomicUsize = AtomicUsize::new(0);
static MAX_INMEMORY_SPARSE_MERGE_CACHE_SIZE: AtomicUsize = AtomicUsize::new(0);

/// Overrides the default size of the indexdb/indexBlocks cache
/// (10% of the allowed memory). Zero restores the default.
pub fn set_index_blocks_cache_size(size: usize) {
    MAX_INDEX_BLOCK_CACHE_SIZE.store(size, Ordering::Relaxed);
}

fn get_max_index_blocks_cache_size() -> usize {
    match MAX_INDEX_BLOCK_CACHE_SIZE.load(Ordering::Relaxed) {
        0 => (0.10 * esm_common::memory::allowed() as f64) as usize,
        n => n,
    }
}

/// Overrides the default size of the indexdb/dataBlocks cache
/// (25% of the allowed memory). Zero restores the default.
pub fn set_data_blocks_cache_size(size: usize) {
    MAX_INMEMORY_BLOCK_CACHE_SIZE.store(size, Ordering::Relaxed);
}

fn get_max_inmemory_blocks_cache_size() -> usize {
    match MAX_INMEMORY_BLOCK_CACHE_SIZE.load(Ordering::Relaxed) {
        0 => (0.25 * esm_common::memory::allowed() as f64) as usize,
        n => n,
    }
}

/// Overrides the default size of the indexdb/dataBlocksSparse cache
/// (5% of the allowed memory). Zero restores the default.
pub fn set_data_blocks_sparse_cache_size(size: usize) {
    MAX_INMEMORY_SPARSE_MERGE_CACHE_SIZE.store(size, Ordering::Relaxed);
}

fn get_max_inmemory_blocks_sparse_cache_size() -> usize {
    match MAX_INMEMORY_SPARSE_MERGE_CACHE_SIZE.load(Ordering::Relaxed) {
        0 => (0.05 * esm_common::memory::allowed() as f64) as usize,
        n => n,
    }
}

/// A decompressed index block: the parsed block headers for one
/// metaindex row (Go's `indexBlock`; the headers own their data here, so
/// there is no separate backing `buf`).
#[derive(Default)]
pub(crate) struct IndexBlock {
    pub bhs: Vec<BlockHeader>,
}

impl CachedBlock for IndexBlock {
    fn size_bytes(&self) -> usize {
        std::mem::size_of::<IndexBlock>()
            + self.bhs.iter().map(BlockHeader::size_bytes).sum::<usize>()
    }
}

impl CachedBlock for InmemoryBlock {
    fn size_bytes(&self) -> usize {
        InmemoryBlock::size_bytes(self)
    }
}

/// Random-access reader over a part stream: either an on-disk file or an
/// in-memory buffer.
pub(crate) enum PartFile {
    File(esm_common::fs::ReaderAt),
    Mem(Arc<Vec<u8>>),
    /// Placeholder used while tearing a part down.
    Closed,
}

impl PartFile {
    pub fn must_read_at(&self, p: &mut [u8], off: u64) {
        match self {
            PartFile::File(r) => r.must_read_at(p, off),
            PartFile::Mem(buf) => {
                let off = usize::try_from(off)
                    .unwrap_or_else(|_| panic!("BUG: off={off} overflows usize"));
                assert!(
                    off + p.len() <= buf.len(),
                    "BUG: off={off}+len={} is out of range for in-memory part stream of size {}",
                    p.len(),
                    buf.len()
                );
                p.copy_from_slice(&buf[off..off + p.len()]);
            }
            PartFile::Closed => panic!("BUG: read from a closed part stream"),
        }
    }
}

/// An immutable part, either file-backed or in-memory.
pub(crate) struct Part {
    /// Unique id used as the block-cache key component (Go uses the part
    /// pointer identity instead).
    pub id: u64,
    pub ph: PartHeader,
    /// Path to the part directory; empty for in-memory parts.
    pub path: PathBuf,
    pub size: u64,
    pub mrs: Vec<MetaindexRow>,
    pub index_file: PartFile,
    pub items_file: PartFile,
    pub lens_file: PartFile,
    /// When set, the part directory is removed once the part is dropped
    /// (i.e. once the last search using it finishes).
    pub must_drop_on_release: AtomicBool,
}

impl Part {
    pub fn new(
        ph: &PartHeader,
        path: &Path,
        size: u64,
        metaindex_data: &[u8],
        index_file: PartFile,
        items_file: PartFile,
        lens_file: PartFile,
    ) -> Part {
        let mrs = unmarshal_metaindex_rows(metaindex_data)
            .unwrap_or_else(|e| panic!("FATAL: cannot unmarshal metaindexRows from {path:?}: {e}"));

        static NEXT_PART_ID: AtomicU64 = AtomicU64::new(1);

        let mut p = Part {
            id: NEXT_PART_ID.fetch_add(1, Ordering::Relaxed),
            ph: PartHeader::default(),
            path: path.to_path_buf(),
            size,
            mrs,
            index_file,
            items_file,
            lens_file,
            must_drop_on_release: AtomicBool::new(false),
        };
        p.ph.copy_from(ph);
        p
    }
}

/// Opens a file-based part from the given path.
pub(crate) fn must_open_file_part(path: &Path) -> Part {
    let mut ph = PartHeader::default();
    ph.must_read_metadata(path);

    let metaindex_path = path.join(METAINDEX_FILENAME);
    let metaindex_data = esm_common::fs::read_full_file(&metaindex_path);
    let metaindex_size = metaindex_data.len() as u64;

    let index_path = path.join(INDEX_FILENAME);
    let items_path = path.join(ITEMS_FILENAME);
    let lens_path = path.join(LENS_FILENAME);

    let index_file = esm_common::fs::must_open_reader_at(&index_path);
    let index_size = esm_common::fs::must_file_size(&index_path);
    let items_file = esm_common::fs::must_open_reader_at(&items_path);
    let items_size = esm_common::fs::must_file_size(&items_path);
    let lens_file = esm_common::fs::must_open_reader_at(&lens_path);
    let lens_size = esm_common::fs::must_file_size(&lens_path);

    let size = metaindex_size + index_size + items_size + lens_size;
    Part::new(
        &ph,
        path,
        size,
        &metaindex_data,
        PartFile::File(index_file),
        PartFile::File(items_file),
        PartFile::File(lens_file),
    )
}

/// Drops all the cached blocks belonging to the part with the given id
/// (mirrors Go `part.MustClose`). Only touches initialized caches.
fn purge_caches_for_part(id: u64) {
    if let Some(c) = IDXB_CACHE.get() {
        c.remove_blocks_for_part(id);
    }
    if let Some(c) = IB_CACHE.get() {
        c.remove_blocks_for_part(id);
    }
    if let Some(c) = IB_SPARSE_CACHE.get() {
        c.remove_blocks_for_part(id);
    }
}

impl Drop for Part {
    fn drop(&mut self) {
        if self.must_drop_on_release.load(Ordering::Acquire) && !self.path.as_os_str().is_empty() {
            // The last reference to a merged-away part is often dropped by a
            // query thread. Offload the cache purges, the file closes
            // (munmap/CloseHandle) and the recursive directory removal to
            // the background dir remover so queries don't stall on slow
            // filesystem work (Go pays these costs on merger goroutines and
            // dir_remover.go). Callers re-opening the same directory must
            // drain via esm_common::fs::remove_dir_async_drain().
            let id = self.id;
            let index_file = std::mem::replace(&mut self.index_file, PartFile::Closed);
            let items_file = std::mem::replace(&mut self.items_file, PartFile::Closed);
            let lens_file = std::mem::replace(&mut self.lens_file, PartFile::Closed);
            let path = std::mem::take(&mut self.path);
            esm_common::fs::remove_dir_async(
                path,
                Box::new(move || {
                    // Close the files before removing the directory, so the
                    // removal works on platforms where open files cannot be
                    // deleted (Windows).
                    drop(index_file);
                    drop(items_file);
                    drop(lens_file);
                    // Part ids are never reused, so purging the caches after
                    // the part is gone is safe: stale entries are unreachable
                    // by new parts and are removed here.
                    purge_caches_for_part(id);
                }),
            );
            return;
        }
        purge_caches_for_part(self.id);
    }
}
