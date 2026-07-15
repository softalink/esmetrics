//! Stage-2 part layer: immutable searchable parts.
//!
//! Ports of lib/storage `part.go` (this file), `part_header.go`
//! ([`header`]), `inmemory_part.go` + the rawRowsMarshaler from
//! `raw_row.go` ([`inmemory`]) and `part_search.go` ([`search`]).

pub mod header;
pub mod inmemory;
pub mod search;

pub use header::PartHeader;
pub use inmemory::InmemoryPart;
pub use search::{BlockRef, PartSearch};

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use esm_mergeset::blockcache::{Cache, CachedBlock};

use crate::block_header::BlockHeader;
use crate::metaindex_row::{unmarshal_metaindex_rows, MetaindexRow};

/// A decoded index block: the blockHeaders of one `index.bin` block.
/// Cached in [`idxb_cache`], mirroring Go's storage `ibCache` (part.go).
pub(crate) struct IndexBlock {
    pub bhs: Vec<BlockHeader>,
}

impl CachedBlock for IndexBlock {
    fn size_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.bhs.capacity() * std::mem::size_of::<BlockHeader>()
    }
}

/// Process-global cache of decoded index blocks, sized like Go's storage
/// `ibCache`: 10% of allowed memory (lib/storage/part.go getMaxIndexBlocksCacheSize).
pub(crate) fn idxb_cache() -> &'static Cache<IndexBlock> {
    static CACHE: OnceLock<Cache<IndexBlock>> = OnceLock::new();
    CACHE.get_or_init(|| Cache::new(|| esm_common::memory::allowed() / 10))
}

/// A fully decoded data block: all its timestamps plus its values converted
/// to floats. Cached in [`decoded_block_cache`].
pub(crate) struct DecodedBlock {
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
}

impl CachedBlock for DecodedBlock {
    fn size_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.timestamps.capacity() * std::mem::size_of::<i64>()
            + self.values.capacity() * std::mem::size_of::<f64>()
    }
}

/// Process-global cache of decoded data blocks, keyed by
/// `(part id, values block offset)`. The values block offset is unique per
/// block within a part ONLY for blocks with `values_block_size > 0`:
/// constant-encoded values blocks write no data and all share offset 0, so
/// they must bypass this cache (see `get_decoded_block`). Timestamps blocks
/// may additionally be shared between blocks.
///
/// Deviation from Go: the upstream re-decodes data blocks on every query
/// and relies on the OS page cache for the compressed data only. Repeated
/// queries over the same series spend most of their time in
/// zstd/varint/delta decoding, so caching the decoded samples (bounded, LRU,
/// invalidated when a part is dropped) removes that cost entirely on hits.
pub(crate) fn decoded_block_cache() -> &'static Cache<DecodedBlock> {
    static CACHE: OnceLock<Cache<DecodedBlock>> = OnceLock::new();
    // Blocks are cached on the first decode (misses_before_caching = 0), so
    // repeated queries hit the cache from their second access on.
    CACHE.get_or_init(|| Cache::with_misses_before_caching(|| esm_common::memory::allowed() / 8, 0))
}

/// Source of unique part ids for cache keying (Go keys by part pointer).
static NEXT_PART_ID: AtomicU64 = AtomicU64::new(1);

// Part file names (filenames.go).
pub(crate) const METAINDEX_FILENAME: &str = "metaindex.bin";
pub(crate) const INDEX_FILENAME: &str = "index.bin";
pub(crate) const VALUES_FILENAME: &str = "values.bin";
pub(crate) const TIMESTAMPS_FILENAME: &str = "timestamps.bin";
pub(crate) const METADATA_FILENAME: &str = "metadata.json";

/// Random-access reader over a part data stream: either an on-disk file or
/// an in-memory buffer. Go: fs.MustReadAtCloser.
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

/// An immutable searchable part containing time series data, either
/// file-backed or in-memory. Go: part.
pub struct Part {
    /// Unique id for index-block cache keying (Go keys by part pointer).
    pub(crate) id: u64,

    /// The part header.
    pub ph: PartHeader,

    /// Filesystem path to the part; empty for in-memory parts.
    pub path: PathBuf,

    /// Total size in bytes of the part data.
    pub size: u64,

    pub(crate) timestamps_file: PartFile,
    pub(crate) values_file: PartFile,
    pub(crate) index_file: PartFile,

    /// The in-RAM metaindex (decompressed `metaindex.bin`).
    pub(crate) metaindex: Vec<MetaindexRow>,

    /// When set, the part directory is removed once the part is dropped
    /// (i.e. once the last search using it finishes). Used by the stage-4
    /// partWrapper mustDrop logic.
    pub must_drop_on_release: AtomicBool,
}

impl Part {
    /// Returns a new part initialized with the given arguments.
    /// Go: newPart.
    pub(crate) fn new(
        ph: &PartHeader,
        path: &Path,
        size: u64,
        metaindex_data: &[u8],
        timestamps_file: PartFile,
        values_file: PartFile,
        index_file: PartFile,
    ) -> Part {
        let metaindex = unmarshal_metaindex_rows(metaindex_data).unwrap_or_else(|err| {
            panic!("FATAL: cannot unmarshal metaindex data from {path:?}: {err}")
        });

        Part {
            id: NEXT_PART_ID.fetch_add(1, Ordering::Relaxed),
            ph: *ph,
            path: path.to_path_buf(),
            size,
            timestamps_file,
            values_file,
            index_file,
            metaindex,
            must_drop_on_release: AtomicBool::new(false),
        }
    }

    /// Human-readable representation of the part. Go: part.String.
    pub fn describe(&self) -> String {
        if !self.path.as_os_str().is_empty() {
            return self.path.display().to_string();
        }
        self.ph.to_string()
    }
}

/// Opens a file-based part from the given path. Go: mustOpenFilePart.
pub fn must_open_file_part(path: &Path) -> Part {
    let mut ph = PartHeader::default();
    ph.must_read_metadata(path);

    let metaindex_path = path.join(METAINDEX_FILENAME);
    let metaindex_data = esm_common::fs::read_full_file(&metaindex_path);
    let metaindex_size = metaindex_data.len() as u64;

    let timestamps_path = path.join(TIMESTAMPS_FILENAME);
    let values_path = path.join(VALUES_FILENAME);
    let index_path = path.join(INDEX_FILENAME);

    let timestamps_file = esm_common::fs::must_open_reader_at(&timestamps_path);
    let timestamps_size = esm_common::fs::must_file_size(&timestamps_path);
    let values_file = esm_common::fs::must_open_reader_at(&values_path);
    let values_size = esm_common::fs::must_file_size(&values_path);
    let index_file = esm_common::fs::must_open_reader_at(&index_path);
    let index_size = esm_common::fs::must_file_size(&index_path);

    let size = timestamps_size + values_size + index_size + metaindex_size;
    Part::new(
        &ph,
        path,
        size,
        &metaindex_data,
        PartFile::File(timestamps_file),
        PartFile::File(values_file),
        PartFile::File(index_file),
    )
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
            let timestamps_file = std::mem::replace(&mut self.timestamps_file, PartFile::Closed);
            let values_file = std::mem::replace(&mut self.values_file, PartFile::Closed);
            let index_file = std::mem::replace(&mut self.index_file, PartFile::Closed);
            let path = std::mem::take(&mut self.path);
            esm_common::fs::remove_dir_async(
                path,
                Box::new(move || {
                    // Close the files before removing the directory, so the
                    // removal works on platforms where open files cannot be
                    // deleted (Windows).
                    drop(timestamps_file);
                    drop(values_file);
                    drop(index_file);
                    // Part ids are never reused, so purging the caches after
                    // the part is gone is safe: stale entries are unreachable
                    // by new parts and are removed here.
                    idxb_cache().remove_blocks_for_part(id);
                    decoded_block_cache().remove_blocks_for_part(id);
                }),
            );
            return;
        }
        idxb_cache().remove_blocks_for_part(self.id);
        decoded_block_cache().remove_blocks_for_part(self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_stream::BlockStreamReader;
    use crate::raw_row::RawRow;
    use crate::tsid::Tsid;

    fn test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("esm-storage-{name}-{}", std::process::id()))
    }

    fn make_rows(n: usize) -> Vec<RawRow> {
        (0..n)
            .map(|i| RawRow {
                tsid: Tsid {
                    metric_id: (i as u64) / 100 + 1,
                    ..Default::default()
                },
                timestamp: (i as i64 % 100) * 10_000,
                value: (i as f64) * 0.5,
                precision_bits: 64,
            })
            .collect()
    }

    // InmemoryPart -> store to disk -> must_open_file_part -> read back via
    // the block stream reader.
    #[test]
    fn file_part_store_and_reopen() {
        let dir = test_dir("file-part-store-reopen");
        let _ = std::fs::remove_dir_all(&dir);

        let mut rows = make_rows(1000);
        let mp = InmemoryPart::init_from_rows(&mut rows);
        mp.must_store_to_disk(&dir);

        let p = must_open_file_part(&dir);
        assert_eq!(p.ph, mp.ph);
        assert_eq!(p.ph.rows_count, 1000);
        assert!(!p.metaindex.is_empty());
        assert!(p.size > 0);
        assert_eq!(p.path, dir);

        // The file part must be readable by the block stream reader too.
        let mut bsr = BlockStreamReader::from_file_part(&dir);
        let mut rows_read = 0usize;
        let mut blocks_read = 0usize;
        while bsr.next_block() {
            rows_read += bsr.block.timestamps().len();
            blocks_read += 1;
        }
        assert_eq!(bsr.error(), None);
        assert_eq!(rows_read, 1000);
        assert_eq!(blocks_read as u64, p.ph.blocks_count);
        bsr.must_close();

        drop(p);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // Merge two in-memory parts into a file part directory via
    // BlockStreamWriter::new_file_part, reopen the resulting part and search
    // it with PartSearch.
    #[test]
    fn file_part_merge_reopen_and_search() {
        use crate::block_stream::{merge_block_streams, BlockStreamWriter};
        use crate::time_range::TimeRange;
        use std::sync::atomic::AtomicU64;

        let dir = test_dir("file-part-merge-search");
        let _ = std::fs::remove_dir_all(&dir);

        // Two streams sharing metric ids 1..=10, 100 rows per metric each.
        let mut rows1 = make_rows(1000);
        let mp1 = InmemoryPart::init_from_rows(&mut rows1);
        let mut rows2: Vec<RawRow> = make_rows(1000)
            .into_iter()
            .map(|mut r| {
                r.timestamp += 5_000; // interleave with stream 1
                r.value += 0.25;
                r
            })
            .collect();
        let mp2 = InmemoryPart::init_from_rows(&mut rows2);

        let mut bsrs = vec![
            BlockStreamReader::from_inmemory_part(&mp1),
            BlockStreamReader::from_inmemory_part(&mp2),
        ];
        let mut ph = PartHeader::default();
        let mut bsw = BlockStreamWriter::new_file_part(&dir, false, 1);
        let rows_merged = AtomicU64::new(0);
        let rows_deleted = AtomicU64::new(0);
        let bufs = merge_block_streams(
            &mut ph,
            &mut bsw,
            &mut bsrs,
            None,
            None,
            0,
            &rows_merged,
            &rows_deleted,
        )
        .expect("unexpected error in merge_block_streams");
        assert!(bufs.is_none(), "file-part merge must not return buffers");
        assert_eq!(ph.rows_count, 2000);
        ph.must_write_metadata(&dir);

        let p = Arc::new(must_open_file_part(&dir));
        assert_eq!(p.ph.rows_count, 2000);
        assert_eq!(p.ph.blocks_count, 10);

        // Search two of the ten series over a sub-range.
        let tsids = Arc::new(vec![
            Tsid {
                metric_id: 3,
                ..Default::default()
            },
            Tsid {
                metric_id: 7,
                ..Default::default()
            },
        ]);
        let tr = TimeRange {
            min_timestamp: 100_000,
            max_timestamp: 500_000,
        };
        let mut ps = PartSearch::new(Arc::clone(&p), tsids, tr);
        let mut found = Vec::new();
        while ps.next_block() {
            let mut b = crate::block::Block::default();
            ps.block_ref()
                .read_block(&mut b)
                .expect("cannot read block");
            found.push((
                b.header().tsid.metric_id,
                b.timestamps().to_vec(),
                b.values().to_vec(),
            ));
        }
        assert_eq!(ps.error(), None);
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].0, 3);
        assert_eq!(found[1].0, 7);
        for (_, timestamps, values) in &found {
            // Each merged series block holds 200 interleaved rows; the
            // search returns whole blocks — time-range trimming is up to
            // the caller.
            assert_eq!(timestamps.len(), 200);
            assert_eq!(values.len(), 200);
            assert!(timestamps.is_sorted());
        }

        drop(ps);
        drop(p);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // A part flagged with must_drop_on_release removes its directory when
    // the last reference is dropped.
    #[test]
    fn part_drop_removes_dir_when_flagged() {
        let dir = test_dir("part-drop-removes-dir");
        let _ = std::fs::remove_dir_all(&dir);

        let mut rows = make_rows(10);
        let mp = InmemoryPart::init_from_rows(&mut rows);
        mp.must_store_to_disk(&dir);

        let p = must_open_file_part(&dir);
        p.must_drop_on_release.store(true, Ordering::Release);
        drop(p);
        esm_common::fs::remove_dir_async_drain();
        assert!(!dir.exists());
    }
}
