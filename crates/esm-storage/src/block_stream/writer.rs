//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/block_stream_writer.go
//! (plus `getCompressLevel` from partition.go).

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use esm_common::filestream;
use esm_encoding::compress_zstd_level;

use crate::block::{Block, MAX_BLOCK_SIZE};
use crate::metaindex_row::MetaindexRow;
use crate::part::header::PartHeader;
use crate::part::{INDEX_FILENAME, METAINDEX_FILENAME, TIMESTAMPS_FILENAME, VALUES_FILENAME};

/// The number of times the identical-timestamps optimization kicked in.
/// Go: timestampsBlocksMerged (esm_timestamps_blocks_merged_total).
static TIMESTAMPS_BLOCKS_MERGED: AtomicU64 = AtomicU64::new(0);

/// The number of bytes saved by the identical-timestamps optimization.
/// Go: timestampsBytesSaved (esm_timestamps_bytes_saved_total).
static TIMESTAMPS_BYTES_SAVED: AtomicU64 = AtomicU64::new(0);

/// Returns the total number of shared (not re-written) timestamp blocks.
pub fn timestamps_blocks_merged() -> u64 {
    TIMESTAMPS_BLOCKS_MERGED.load(Ordering::Relaxed)
}

/// Returns the total number of bytes saved by timestamp-block sharing.
pub fn timestamps_bytes_saved() -> u64 {
    TIMESTAMPS_BYTES_SAVED.load(Ordering::Relaxed)
}

/// Returns the ZSTD compression level for the given average number of rows
/// per block in a part. Go: getCompressLevel (partition.go).
pub fn get_compress_level(rows_per_block: f64) -> i32 {
    // See https://github.com/facebook/zstd/releases/tag/v1.3.4
    // about negative compression levels.
    if rows_per_block <= 10.0 {
        return -5;
    }
    if rows_per_block <= 50.0 {
        return -2;
    }
    if rows_per_block <= 200.0 {
        return -1;
    }
    if rows_per_block <= 500.0 {
        return 1;
    }
    if rows_per_block <= 1000.0 {
        return 2;
    }
    3
}

/// A single output stream of the writer: an on-disk file or an in-memory
/// buffer.
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

/// Block stream writer producing the four part streams
/// (`timestamps.bin`, `values.bin`, `index.bin`, `metaindex.bin`).
/// Go: blockStreamWriter.
pub struct BlockStreamWriter {
    compress_level: i32,

    timestamps_writer: StreamWriter,
    values_writer: StreamWriter,
    index_writer: StreamWriter,
    metaindex_writer: StreamWriter,

    mr: MetaindexRow,

    timestamps_block_offset: u64,
    values_block_offset: u64,
    index_block_offset: u64,

    index_data: Vec<u8>,
    compressed_index_data: Vec<u8>,

    metaindex_data: Vec<u8>,
    compressed_metaindex_data: Vec<u8>,

    // prev_timestamps_* is used as an optimization for reducing disk space
    // usage when serially written blocks have identical timestamps. This is
    // usually the case when adjacent blocks contain metrics scraped from the
    // same target, since such metrics have identical timestamps.
    prev_timestamps_data: Vec<u8>,
    prev_timestamps_block_offset: u64,
}

impl BlockStreamWriter {
    fn new(
        timestamps_writer: StreamWriter,
        values_writer: StreamWriter,
        index_writer: StreamWriter,
        metaindex_writer: StreamWriter,
        compress_level: i32,
    ) -> BlockStreamWriter {
        BlockStreamWriter {
            compress_level,
            timestamps_writer,
            values_writer,
            index_writer,
            metaindex_writer,
            mr: MetaindexRow::default(),
            timestamps_block_offset: 0,
            values_block_offset: 0,
            index_block_offset: 0,
            index_data: Vec::new(),
            compressed_index_data: Vec::new(),
            metaindex_data: Vec::new(),
            compressed_metaindex_data: Vec::new(),
            prev_timestamps_data: Vec::new(),
            prev_timestamps_block_offset: 0,
        }
    }

    /// Creates a writer producing an in-memory part.
    /// Go: blockStreamWriter.MustInitFromInmemoryPart.
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
    /// The writer doesn't pollute the OS page cache if `nocache` is set.
    /// Go: blockStreamWriter.MustInitFromFilePart.
    pub fn new_file_part(path: &Path, nocache: bool, compress_level: i32) -> BlockStreamWriter {
        // Create the directory.
        esm_common::fs::must_mkdir_fail_if_exist(path);

        let timestamps_writer =
            filestream::Writer::must_create(path.join(TIMESTAMPS_FILENAME), nocache);
        let values_writer = filestream::Writer::must_create(path.join(VALUES_FILENAME), nocache);
        let index_writer = filestream::Writer::must_create(path.join(INDEX_FILENAME), nocache);
        // Always cache the metaindex file in the OS page cache, since it is
        // immediately read after the merge.
        let metaindex_writer =
            filestream::Writer::must_create(path.join(METAINDEX_FILENAME), false);

        BlockStreamWriter::new(
            StreamWriter::File(timestamps_writer),
            StreamWriter::File(values_writer),
            StreamWriter::File(index_writer),
            StreamWriter::File(metaindex_writer),
            compress_level,
        )
    }

    /// Writes `b` to the writer and updates `ph` and `rows_merged`.
    /// Go: blockStreamWriter.WriteExternalBlock.
    pub fn write_external_block(
        &mut self,
        b: &mut Block,
        ph: &mut PartHeader,
        rows_merged: &mut u64,
    ) {
        *rows_merged += b.pending_rows_count() as u64;
        b.deduplicate_samples_during_merge();

        // First pass: marshal at the current offsets and check whether the
        // timestamps block is identical to the previous one.
        let (use_prev_timestamps, timestamps_data_len) = {
            let (_header_data, timestamps_data, _values_data) =
                b.marshal_data(self.timestamps_block_offset, self.values_block_offset);
            (
                !self.prev_timestamps_data.is_empty()
                    && timestamps_data == &self.prev_timestamps_data[..],
                timestamps_data.len(),
            )
        };

        // Re-marshal the header so it points at the shared timestamps block
        // when the current timestamps block equals the previous one. This
        // saves disk space. (For the non-shared case this recreates the same
        // header — Block::marshal_data on already-marshaled data only
        // rebuilds the 81-byte header.)
        let timestamps_block_offset = if use_prev_timestamps {
            TIMESTAMPS_BLOCKS_MERGED.fetch_add(1, Ordering::Relaxed);
            TIMESTAMPS_BYTES_SAVED.fetch_add(timestamps_data_len as u64, Ordering::Relaxed);
            self.prev_timestamps_block_offset
        } else {
            self.timestamps_block_offset
        };
        let (header_data, timestamps_data, values_data) =
            b.marshal_data(timestamps_block_offset, self.values_block_offset);

        if self.index_data.len() + header_data.len() > MAX_BLOCK_SIZE {
            self.flush_index_data();
        }
        self.index_data.extend_from_slice(header_data);

        if !use_prev_timestamps {
            self.prev_timestamps_data.clear();
            self.prev_timestamps_data.extend_from_slice(timestamps_data);
            self.prev_timestamps_block_offset = self.timestamps_block_offset;
            self.timestamps_writer.must_write(timestamps_data);
            self.timestamps_block_offset += timestamps_data.len() as u64;
        }
        self.values_writer.must_write(values_data);
        self.values_block_offset += values_data.len() as u64;

        // (Go registers the header before writing the data; the order is
        // irrelevant, but the borrow checker wants the marshaled slices
        // released before `b.header()` is borrowed again.)
        self.mr.register_block_header(b.header());
        update_part_header(b, ph);
    }

    /// Go: blockStreamWriter.flushIndexData.
    fn flush_index_data(&mut self) {
        if self.index_data.is_empty() {
            // Nothing to flush.
            return;
        }

        // Write the compressed index block to the index data.
        self.compressed_index_data.clear();
        compress_zstd_level(
            &mut self.compressed_index_data,
            &self.index_data,
            self.compress_level,
        );
        let index_block_size = self.compressed_index_data.len();
        assert!(
            (index_block_size as u64) < (1u64 << 32),
            "BUG: indexBlock size must fit uint32; got {index_block_size}"
        );
        self.index_writer.must_write(&self.compressed_index_data);

        // Write the metaindex row to the metaindex data.
        self.mr.index_block_offset = self.index_block_offset;
        self.mr.index_block_size = index_block_size as u32;
        self.mr.marshal(&mut self.metaindex_data);

        // Update offsets.
        self.index_block_offset += index_block_size as u64;

        self.index_data.clear();
        self.mr.reset();
    }

    /// Closes the writer.
    ///
    /// For in-memory writers returns the four stream buffers in
    /// `[timestamps, values, index, metaindex]` order; file writers return
    /// `None`. Go: blockStreamWriter.MustClose.
    pub fn must_close(&mut self) -> Option<[Vec<u8>; 4]> {
        // Flush remaining data.
        self.flush_index_data();

        // Write metaindex data.
        self.compressed_metaindex_data.clear();
        compress_zstd_level(
            &mut self.compressed_metaindex_data,
            &self.metaindex_data,
            self.compress_level,
        );
        self.metaindex_writer
            .must_write(&self.compressed_metaindex_data);

        let timestamps = self.timestamps_writer.must_close();
        let values = self.values_writer.must_close();
        let index = self.index_writer.must_close();
        let metaindex = self.metaindex_writer.must_close();

        match (timestamps, values, index, metaindex) {
            (Some(t), Some(v), Some(i), Some(m)) => Some([t, v, i, m]),
            (None, None, None, None) => None,
            _ => panic!("BUG: mixed in-memory and file part streams"),
        }
    }
}

/// Go: updatePartHeader.
fn update_part_header(b: &Block, ph: &mut PartHeader) {
    ph.blocks_count += 1;
    ph.rows_count += b.header().rows_count as u64;
    if b.header().min_timestamp < ph.min_timestamp {
        ph.min_timestamp = b.header().min_timestamp;
    }
    if b.header().max_timestamp > ph.max_timestamp {
        ph.max_timestamp = b.header().max_timestamp;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compress_level_table() {
        assert_eq!(get_compress_level(1.0), -5);
        assert_eq!(get_compress_level(10.0), -5);
        assert_eq!(get_compress_level(11.0), -2);
        assert_eq!(get_compress_level(50.0), -2);
        assert_eq!(get_compress_level(200.0), -1);
        assert_eq!(get_compress_level(500.0), 1);
        assert_eq!(get_compress_level(1000.0), 2);
        assert_eq!(get_compress_level(1001.0), 3);
    }
}
