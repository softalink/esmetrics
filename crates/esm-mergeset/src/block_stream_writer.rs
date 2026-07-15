//! Port of `block_stream_writer.go`.

use std::path::Path;

use esm_common::filestream;
use esm_encoding::compress_zstd_level;

use crate::block_header::BlockHeader;
use crate::filenames::{INDEX_FILENAME, ITEMS_FILENAME, LENS_FILENAME, METAINDEX_FILENAME};
use crate::inmemory_block::{InmemoryBlock, StorageBlock};
use crate::metaindex_row::MetaindexRow;

/// The maximum size of an index block with multiple block headers.
pub(crate) const MAX_INDEX_BLOCK_SIZE: usize = 64 * 1024;

enum StreamWriter {
    File(filestream::Writer),
    Mem(Vec<u8>),
    Closed,
}

impl StreamWriter {
    fn must_write(&mut self, data: &[u8]) {
        match self {
            StreamWriter::File(w) => esm_common::fs::must_write_data(w, data),
            StreamWriter::Mem(buf) => buf.extend_from_slice(data),
            StreamWriter::Closed => panic!("BUG: write to closed stream writer"),
        }
    }

    /// Closes the writer. For in-memory writers, returns the written buffer.
    fn must_close(&mut self) -> Option<Vec<u8>> {
        match std::mem::replace(self, StreamWriter::Closed) {
            StreamWriter::File(w) => {
                w.must_close();
                None
            }
            StreamWriter::Mem(buf) => Some(buf),
            StreamWriter::Closed => panic!("BUG: double close of stream writer"),
        }
    }
}

/// Streaming writer of part blocks: produces `metaindex.bin`, `index.bin`,
/// `items.bin` and `lens.bin` streams.
pub(crate) struct BlockStreamWriter {
    compress_level: i32,

    metaindex_writer: StreamWriter,
    index_writer: StreamWriter,
    items_writer: StreamWriter,
    lens_writer: StreamWriter,

    sb: StorageBlock,
    bh: BlockHeader,
    mr: MetaindexRow,

    unpacked_index_block_buf: Vec<u8>,
    packed_index_block_buf: Vec<u8>,

    unpacked_metaindex_buf: Vec<u8>,
    packed_metaindex_buf: Vec<u8>,

    items_block_offset: u64,
    lens_block_offset: u64,
    index_block_offset: u64,

    // Whether the first item for mr has been caught.
    mr_first_item_caught: bool,
}

impl BlockStreamWriter {
    fn new(
        metaindex_writer: StreamWriter,
        index_writer: StreamWriter,
        items_writer: StreamWriter,
        lens_writer: StreamWriter,
        compress_level: i32,
    ) -> BlockStreamWriter {
        BlockStreamWriter {
            compress_level,
            metaindex_writer,
            index_writer,
            items_writer,
            lens_writer,
            sb: StorageBlock::default(),
            bh: BlockHeader::default(),
            mr: MetaindexRow::default(),
            unpacked_index_block_buf: Vec::new(),
            packed_index_block_buf: Vec::new(),
            unpacked_metaindex_buf: Vec::new(),
            packed_metaindex_buf: Vec::new(),
            items_block_offset: 0,
            lens_block_offset: 0,
            index_block_offset: 0,
            mr_first_item_caught: false,
        }
    }

    /// Creates a writer that produces an in-memory part.
    pub fn new_inmemory_part(compress_level: i32) -> BlockStreamWriter {
        BlockStreamWriter::new(
            StreamWriter::Mem(Vec::new()),
            StreamWriter::Mem(Vec::new()),
            StreamWriter::Mem(Vec::new()),
            StreamWriter::Mem(Vec::new()),
            compress_level,
        )
    }

    /// Creates a writer producing a file-based part at the given path.
    ///
    /// The writer doesn't pollute the OS page cache if `nocache` is set.
    pub fn new_file_part(path: &Path, nocache: bool, compress_level: i32) -> BlockStreamWriter {
        // Create the directory.
        esm_common::fs::must_mkdir_fail_if_exist(path);

        let index_writer = filestream::Writer::must_create(path.join(INDEX_FILENAME), nocache);
        let items_writer = filestream::Writer::must_create(path.join(ITEMS_FILENAME), nocache);
        let lens_writer = filestream::Writer::must_create(path.join(LENS_FILENAME), nocache);
        // Always cache metaindex file in OS page cache, since it is
        // immediately read after the merge.
        let metaindex_writer =
            filestream::Writer::must_create(path.join(METAINDEX_FILENAME), false);

        BlockStreamWriter::new(
            StreamWriter::File(metaindex_writer),
            StreamWriter::File(index_writer),
            StreamWriter::File(items_writer),
            StreamWriter::File(lens_writer),
            compress_level,
        )
    }

    /// Writes `ib` (which must be sorted) to the output streams.
    pub fn write_block(&mut self, ib: &mut InmemoryBlock) {
        self.bh.first_item.clear();
        self.bh.common_prefix.clear();
        let (items_count, marshal_type) = ib.marshal_sorted_data(
            &mut self.sb,
            &mut self.bh.first_item,
            &mut self.bh.common_prefix,
            self.compress_level,
        );
        self.bh.items_count = items_count;
        self.bh.marshal_type = marshal_type;

        // Write itemsData.
        self.items_writer.must_write(&self.sb.items_data);
        self.bh.items_block_size = self.sb.items_data.len() as u32;
        self.bh.items_block_offset = self.items_block_offset;
        self.items_block_offset += self.bh.items_block_size as u64;

        // Write lensData.
        self.lens_writer.must_write(&self.sb.lens_data);
        self.bh.lens_block_size = self.sb.lens_data.len() as u32;
        self.bh.lens_block_offset = self.lens_block_offset;
        self.lens_block_offset += self.bh.lens_block_size as u64;

        // Write blockHeader.
        let unpacked_len = self.unpacked_index_block_buf.len();
        self.bh.marshal(&mut self.unpacked_index_block_buf);
        if self.unpacked_index_block_buf.len() > MAX_INDEX_BLOCK_SIZE {
            self.unpacked_index_block_buf.truncate(unpacked_len);
            self.flush_index_data();
            self.bh.marshal(&mut self.unpacked_index_block_buf);
        }

        if !self.mr_first_item_caught {
            self.mr.first_item.clear();
            self.mr.first_item.extend_from_slice(&self.bh.first_item);
            self.mr_first_item_caught = true;
        }
        self.bh.reset();
        self.mr.block_headers_count += 1;
    }

    fn flush_index_data(&mut self) {
        if self.unpacked_index_block_buf.is_empty() {
            // Nothing to flush.
            return;
        }

        // Write indexBlock.
        self.packed_index_block_buf.clear();
        compress_zstd_level(
            &mut self.packed_index_block_buf,
            &self.unpacked_index_block_buf,
            self.compress_level,
        );
        self.index_writer.must_write(&self.packed_index_block_buf);
        self.mr.index_block_size = self.packed_index_block_buf.len() as u32;
        self.mr.index_block_offset = self.index_block_offset;
        self.index_block_offset += self.mr.index_block_size as u64;
        self.unpacked_index_block_buf.clear();

        // Write metaindexRow.
        self.mr.marshal(&mut self.unpacked_metaindex_buf);
        self.mr.reset();

        // Notify that the next call to write_block must catch the first item.
        self.mr_first_item_caught = false;
    }

    /// Closes the writer.
    ///
    /// For in-memory writers returns the four stream buffers in
    /// `[metaindex, index, items, lens]` order; file writers return None.
    pub fn must_close(&mut self) -> Option<[Vec<u8>; 4]> {
        // Flush the remaining data.
        self.flush_index_data();

        // Compress and write metaindex.
        self.packed_metaindex_buf.clear();
        compress_zstd_level(
            &mut self.packed_metaindex_buf,
            &self.unpacked_metaindex_buf,
            self.compress_level,
        );
        self.metaindex_writer.must_write(&self.packed_metaindex_buf);

        let metaindex = self.metaindex_writer.must_close();
        let index = self.index_writer.must_close();
        let items = self.items_writer.must_close();
        let lens = self.lens_writer.must_close();

        match (metaindex, index, items, lens) {
            (Some(m), Some(idx), Some(it), Some(l)) => Some([m, idx, it, l]),
            (None, None, None, None) => None,
            _ => panic!("BUG: mixed in-memory and file part streams"),
        }
    }
}
