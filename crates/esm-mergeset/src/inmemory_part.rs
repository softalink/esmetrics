//! Port of `inmemory_part.go`.

use std::path::Path;
use std::sync::Arc;

use esm_encoding::compress_zstd_level;

use crate::block_header::BlockHeader;
use crate::block_stream_writer::MAX_INDEX_BLOCK_SIZE;
use crate::filenames::{INDEX_FILENAME, ITEMS_FILENAME, LENS_FILENAME, METAINDEX_FILENAME};
use crate::inmemory_block::{InmemoryBlock, StorageBlock};
use crate::metaindex_row::MetaindexRow;
use crate::part::{Part, PartFile};
use crate::part_header::PartHeader;

/// A complete part held in memory: the four part streams as byte buffers.
///
/// The buffers are `Arc`ed so a [`Part`] and block stream readers can share
/// them without copying.
pub(crate) struct InmemoryPart {
    pub ph: PartHeader,
    pub metaindex_data: Arc<Vec<u8>>,
    pub index_data: Arc<Vec<u8>>,
    pub items_data: Arc<Vec<u8>>,
    pub lens_data: Arc<Vec<u8>>,
}

impl InmemoryPart {
    /// Builds a complete single-block part from `ib`.
    pub fn init(ib: &mut InmemoryBlock) -> InmemoryPart {
        let mut sb = StorageBlock::default();
        let mut bh = BlockHeader::default();
        let mut mr = MetaindexRow::default();
        let mut ph = PartHeader::default();

        // Use the minimum possible compressLevel for compressing inmemoryPart,
        // since it will be merged into file part soon.
        let compress_level = -5;
        let (items_count, marshal_type) = ib.marshal_unsorted_data(
            &mut sb,
            &mut bh.first_item,
            &mut bh.common_prefix,
            compress_level,
        );
        bh.items_count = items_count;
        bh.marshal_type = marshal_type;

        ph.items_count = ib.items.len() as u64;
        ph.blocks_count = 1;
        ph.first_item = ib.items[0].bytes(&ib.data).to_vec();
        ph.last_item = ib.items[ib.items.len() - 1].bytes(&ib.data).to_vec();

        let items_data = std::mem::take(&mut sb.items_data);
        bh.items_block_offset = 0;
        bh.items_block_size = items_data.len() as u32;

        let lens_data = std::mem::take(&mut sb.lens_data);
        bh.lens_block_offset = 0;
        bh.lens_block_size = lens_data.len() as u32;

        let mut bh_buf = Vec::new();
        bh.marshal(&mut bh_buf);
        assert!(
            bh_buf.len() <= 3 * MAX_INDEX_BLOCK_SIZE,
            "BUG: too big index block: {} bytes; mustn't exceed {} bytes",
            bh_buf.len(),
            3 * MAX_INDEX_BLOCK_SIZE
        );
        let mut index_data = Vec::new();
        compress_zstd_level(&mut index_data, &bh_buf, compress_level);

        mr.first_item = bh.first_item.clone();
        mr.block_headers_count = 1;
        mr.index_block_offset = 0;
        mr.index_block_size = index_data.len() as u32;
        let mut mr_buf = Vec::new();
        mr.marshal(&mut mr_buf);
        let mut metaindex_data = Vec::new();
        compress_zstd_level(&mut metaindex_data, &mr_buf, compress_level);

        InmemoryPart {
            ph,
            metaindex_data: Arc::new(metaindex_data),
            index_data: Arc::new(index_data),
            items_data: Arc::new(items_data),
            lens_data: Arc::new(lens_data),
        }
    }

    /// Builds an in-memory part from a part header and the four stream
    /// buffers produced by a block stream writer.
    pub fn from_buffers(ph: PartHeader, bufs: [Vec<u8>; 4]) -> InmemoryPart {
        let [metaindex_data, index_data, items_data, lens_data] = bufs;
        InmemoryPart {
            ph,
            metaindex_data: Arc::new(metaindex_data),
            index_data: Arc::new(index_data),
            items_data: Arc::new(items_data),
            lens_data: Arc::new(lens_data),
        }
    }

    /// Stores the part to the given path on disk.
    pub fn must_store_to_disk(&self, path: &Path) {
        esm_common::fs::must_mkdir_fail_if_exist(path);

        esm_common::fs::must_write_sync(path.join(METAINDEX_FILENAME), &self.metaindex_data);
        esm_common::fs::must_write_sync(path.join(INDEX_FILENAME), &self.index_data);
        esm_common::fs::must_write_sync(path.join(ITEMS_FILENAME), &self.items_data);
        esm_common::fs::must_write_sync(path.join(LENS_FILENAME), &self.lens_data);

        self.ph.must_write_metadata(path);

        esm_common::fs::must_sync_path_and_parent_dir(path);
    }

    /// Creates a [`Part`] backed by the in-memory buffers.
    ///
    /// It is safe to call `new_part` multiple times.
    pub fn new_part(&self) -> Part {
        Part::new(
            &self.ph,
            Path::new(""),
            self.size(),
            &self.metaindex_data,
            PartFile::Mem(Arc::clone(&self.index_data)),
            PartFile::Mem(Arc::clone(&self.items_data)),
            PartFile::Mem(Arc::clone(&self.lens_data)),
        )
    }

    pub fn size(&self) -> u64 {
        (self.metaindex_data.len()
            + self.index_data.len()
            + self.items_data.len()
            + self.lens_data.len()) as u64
    }
}
