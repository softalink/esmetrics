//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/block_stream_reader.go.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use esm_common::filestream;
use esm_encoding::decompress_zstd;

use crate::block::Block;
use crate::block_header::{BlockHeader, MARSHALED_BLOCK_HEADER_SIZE};
use crate::block_stream::unmarshal_block_data;
use crate::metaindex_row::{unmarshal_metaindex_rows, MetaindexRow};
use crate::part::header::PartHeader;
use crate::part::inmemory::InmemoryPart;
use crate::part::{INDEX_FILENAME, METAINDEX_FILENAME, TIMESTAMPS_FILENAME, VALUES_FILENAME};
use crate::tsid::Tsid;

/// A single input stream of the reader: an on-disk file or a shared
/// in-memory buffer.
enum StreamReader {
    File(filestream::Reader),
    Mem { buf: Arc<Vec<u8>>, pos: usize },
    Closed,
}

impl StreamReader {
    fn must_read(&mut self, dst: &mut [u8]) {
        match self {
            StreamReader::File(r) => esm_common::fs::must_read_data(r, dst),
            StreamReader::Mem { buf, pos } => {
                assert!(
                    *pos + dst.len() <= buf.len(),
                    "BUG: cannot read {} bytes at offset {pos} from in-memory stream of size {}",
                    dst.len(),
                    buf.len()
                );
                dst.copy_from_slice(&buf[*pos..*pos + dst.len()]);
                *pos += dst.len();
            }
            StreamReader::Closed => panic!("BUG: read from closed stream reader"),
        }
    }

    fn must_close(&mut self) {
        if let StreamReader::File(r) = std::mem::replace(self, StreamReader::Closed) {
            r.must_close();
        }
    }
}

#[derive(Debug)]
enum BsrError {
    Eof,
    Other(String),
}

/// Block stream reader over a file part or an in-memory part.
/// Go: blockStreamReader.
///
/// PORT-NOTE: unlike Go, `block` is already unmarshaled (unpacked) when
/// `next_block` returns true — see `block_stream::unmarshal_block_data`.
/// `Block::unmarshal_data` on it is a no-op, so Go-shaped call sites work
/// unchanged.
pub struct BlockStreamReader {
    /// Currently active block, valid after `next_block` returned true.
    pub block: Block,

    /// TSID of the previous block, for validating that TSIDs increase over
    /// time when reading blocks.
    tsid_prev: Tsid,

    /// Filesystem path of the part; empty for in-memory stream readers.
    path: PathBuf,

    /// Header of the part being read.
    pub ph: PartHeader,

    timestamps_reader: StreamReader,
    values_reader: StreamReader,
    index_reader: StreamReader,

    mrs: Vec<MetaindexRow>,
    /// Index of the next metaindex row to process in `mrs`.
    mr_idx: usize,
    /// Index of the currently processed metaindex row (`mr_idx - 1` once an
    /// index block is loaded); `None` before the first index block.
    cur_mr: Option<usize>,

    /// The total number of rows read so far.
    rows_count: u64,

    /// The total number of blocks read so far.
    blocks_count: u64,

    /// The number of block headers read from the current index block.
    index_block_headers_count: u32,

    timestamps_block_offset: u64,
    values_block_offset: u64,
    index_block_offset: u64,

    prev_timestamps_block_offset: u64,
    prev_timestamps_data: Vec<u8>,

    index_data: Vec<u8>,
    compressed_index_data: Vec<u8>,

    /// Cursor into `index_data`.
    index_cursor: usize,

    /// Scratch buffers for the packed timestamps/values of the current block.
    timestamps_buf: Vec<u8>,
    values_buf: Vec<u8>,

    err: Option<BsrError>,
}

impl BlockStreamReader {
    fn new_empty() -> BlockStreamReader {
        BlockStreamReader {
            block: Block::default(),
            tsid_prev: Tsid::default(),
            path: PathBuf::new(),
            ph: PartHeader::default(),
            timestamps_reader: StreamReader::Closed,
            values_reader: StreamReader::Closed,
            index_reader: StreamReader::Closed,
            mrs: Vec::new(),
            mr_idx: 0,
            cur_mr: None,
            rows_count: 0,
            blocks_count: 0,
            index_block_headers_count: 0,
            timestamps_block_offset: 0,
            values_block_offset: 0,
            index_block_offset: 0,
            prev_timestamps_block_offset: 0,
            prev_timestamps_data: Vec::new(),
            index_data: Vec::new(),
            compressed_index_data: Vec::new(),
            index_cursor: 0,
            timestamps_buf: Vec::new(),
            values_buf: Vec::new(),
            err: None,
        }
    }

    /// Creates a reader over the given in-memory part.
    /// Go: blockStreamReader.MustInitFromInmemoryPart.
    pub fn from_inmemory_part(mp: &InmemoryPart) -> BlockStreamReader {
        let mut bsr = BlockStreamReader::new_empty();

        bsr.ph = mp.ph;
        bsr.timestamps_reader = StreamReader::Mem {
            buf: Arc::clone(&mp.timestamps_data),
            pos: 0,
        };
        bsr.values_reader = StreamReader::Mem {
            buf: Arc::clone(&mp.values_data),
            pos: 0,
        };
        bsr.index_reader = StreamReader::Mem {
            buf: Arc::clone(&mp.index_data),
            pos: 0,
        };
        bsr.mrs = unmarshal_metaindex_rows(&mp.metaindex_data).unwrap_or_else(|err| {
            panic!("BUG: cannot unmarshal metaindex rows from inmemoryPart: {err}")
        });
        bsr
    }

    /// Creates a reader over a file-based part at the given path.
    ///
    /// Files in the part are always read without OS cache pollution, since
    /// they are usually deleted after the merge.
    /// Go: blockStreamReader.MustInitFromFilePart.
    pub fn from_file_part(path: &Path) -> BlockStreamReader {
        let mut bsr = BlockStreamReader::new_empty();

        bsr.ph.must_read_metadata(path);

        let metaindex_path = path.join(METAINDEX_FILENAME);
        let metaindex_data = esm_common::fs::read_full_file(&metaindex_path);
        bsr.mrs = unmarshal_metaindex_rows(&metaindex_data).unwrap_or_else(|err| {
            panic!(
                "FATAL: cannot unmarshal metaindex rows from file part {metaindex_path:?}: {err}"
            )
        });

        bsr.path = path.to_path_buf();

        bsr.timestamps_reader = StreamReader::File(filestream::Reader::must_open(
            path.join(TIMESTAMPS_FILENAME),
            true,
        ));
        bsr.values_reader = StreamReader::File(filestream::Reader::must_open(
            path.join(VALUES_FILENAME),
            true,
        ));
        bsr.index_reader = StreamReader::File(filestream::Reader::must_open(
            path.join(INDEX_FILENAME),
            true,
        ));
        bsr
    }

    /// Closes the reader. Go: blockStreamReader.MustClose.
    pub fn must_close(&mut self) {
        self.timestamps_reader.must_close();
        self.values_reader.must_close();
        self.index_reader.must_close();
    }

    /// Human-readable description of the source part.
    /// Go: blockStreamReader.String.
    pub fn describe(&self) -> String {
        if !self.path.as_os_str().is_empty() {
            return self.path.display().to_string();
        }
        self.ph.to_string()
    }

    /// Returns the last error, ignoring EOF. Go: blockStreamReader.Error.
    pub fn error(&self) -> Option<String> {
        match &self.err {
            Some(BsrError::Other(msg)) => Some(format!(
                "error when reading part {:?}: {msg}",
                self.describe()
            )),
            _ => None,
        }
    }

    /// Advances the reader to the next block.
    /// Go: blockStreamReader.NextBlock.
    pub fn next_block(&mut self) -> bool {
        if self.err.is_some() {
            return false;
        }
        self.tsid_prev = self.block.header().tsid;
        self.block.reset();
        match self.read_block() {
            Ok(()) => {
                if self.block.header().tsid < self.tsid_prev {
                    self.err = Some(BsrError::Other(format!(
                        "possible data corruption: the next TSID={:?} is smaller than the \
                         previous TSID={:?}",
                        self.block.header().tsid,
                        self.tsid_prev
                    )));
                    return false;
                }
                true
            }
            Err(BsrError::Eof) => {
                self.err = Some(BsrError::Eof);
                false
            }
            Err(BsrError::Other(msg)) => {
                self.err = Some(BsrError::Other(format!("cannot read next block: {msg}")));
                false
            }
        }
    }

    /// Go: blockStreamReader.readBlock.
    fn read_block(&mut self) -> Result<(), BsrError> {
        if self.index_cursor >= self.index_data.len() {
            if let Some(cur) = self.cur_mr {
                let mr = &self.mrs[cur];
                if self.index_block_headers_count != mr.block_headers_count {
                    return Err(BsrError::Other(format!(
                        "invalid number of block headers in the previous index block at offset \
                         {}; got {}; want {}",
                        self.prev_index_block_offset(),
                        self.index_block_headers_count,
                        mr.block_headers_count
                    )));
                }
            }
            self.index_block_headers_count = 0;
            self.read_index_block().map_err(|err| match err {
                BsrError::Eof => BsrError::Eof,
                BsrError::Other(msg) => BsrError::Other(format!("cannot read index block: {msg}")),
            })?;
        }

        // Read the block header.
        let index_tail = &self.index_data[self.index_cursor..];
        if index_tail.len() < MARSHALED_BLOCK_HEADER_SIZE {
            return Err(BsrError::Other(format!(
                "too short index data for reading block header at offset {}; got {} bytes; want \
                 {} bytes",
                self.prev_index_block_offset(),
                index_tail.len(),
                MARSHALED_BLOCK_HEADER_SIZE
            )));
        }
        let mut bh = BlockHeader::default();
        bh.unmarshal(&index_tail[..MARSHALED_BLOCK_HEADER_SIZE])
            .map_err(|err| {
                BsrError::Other(format!(
                    "cannot parse block header read from index data at offset {}: {err}",
                    self.prev_index_block_offset()
                ))
            })?;
        self.index_cursor += MARSHALED_BLOCK_HEADER_SIZE;

        self.blocks_count += 1;
        if self.blocks_count > self.ph.blocks_count {
            return Err(BsrError::Other(format!(
                "too many blocks found in the block stream; got {}; cannot be bigger than {}",
                self.blocks_count, self.ph.blocks_count
            )));
        }

        // Validate the block header.
        self.rows_count += bh.rows_count as u64;
        if self.rows_count > self.ph.rows_count {
            return Err(BsrError::Other(format!(
                "too many rows found in the block stream; got {}; cannot be bigger than {}",
                self.rows_count, self.ph.rows_count
            )));
        }
        if bh.min_timestamp < self.ph.min_timestamp {
            return Err(BsrError::Other(format!(
                "invalid MinTimestamp at block header at offset {}; got {}; cannot be smaller \
                 than {}",
                self.prev_index_block_offset(),
                bh.min_timestamp,
                self.ph.min_timestamp
            )));
        }
        if bh.max_timestamp > self.ph.max_timestamp {
            return Err(BsrError::Other(format!(
                "invalid MaxTimestamp at block header at offset {}; got {}; cannot be bigger \
                 than {}",
                self.prev_index_block_offset(),
                bh.max_timestamp,
                self.ph.max_timestamp
            )));
        }
        let use_prev_timestamps = !self.prev_timestamps_data.is_empty()
            && bh.timestamps_block_offset == self.prev_timestamps_block_offset;
        if use_prev_timestamps {
            if bh.timestamps_block_size as usize != self.prev_timestamps_data.len() {
                return Err(BsrError::Other(format!(
                    "invalid TimestampsBlockSize at block header at offset {}; got {}; want {}",
                    self.prev_index_block_offset(),
                    bh.timestamps_block_size,
                    self.prev_timestamps_data.len()
                )));
            }
        } else if bh.timestamps_block_offset != self.timestamps_block_offset {
            return Err(BsrError::Other(format!(
                "invalid TimestampsBlockOffset at block header at offset {}; got {}; want {}",
                self.prev_index_block_offset(),
                bh.timestamps_block_offset,
                self.timestamps_block_offset
            )));
        }
        if bh.values_block_offset != self.values_block_offset {
            return Err(BsrError::Other(format!(
                "invalid ValuesBlockOffset at block header at offset {}; got {}; want {}",
                self.prev_index_block_offset(),
                bh.values_block_offset,
                self.values_block_offset
            )));
        }

        // Read the timestamps data.
        if use_prev_timestamps {
            self.timestamps_buf.clear();
            self.timestamps_buf
                .extend_from_slice(&self.prev_timestamps_data);
        } else {
            self.timestamps_buf
                .resize(bh.timestamps_block_size as usize, 0);
            self.timestamps_reader.must_read(&mut self.timestamps_buf);
            self.prev_timestamps_block_offset = self.timestamps_block_offset;
            self.prev_timestamps_data.clear();
            self.prev_timestamps_data
                .extend_from_slice(&self.timestamps_buf);
        }

        // Read the values data.
        self.values_buf.resize(bh.values_block_size as usize, 0);
        self.values_reader.must_read(&mut self.values_buf);

        // Update offsets.
        if !use_prev_timestamps {
            self.timestamps_block_offset += bh.timestamps_block_size as u64;
        }
        self.values_block_offset += bh.values_block_size as u64;
        self.index_block_headers_count += 1;

        // PORT-NOTE: unpack the block eagerly (Go defers to
        // Block.UnmarshalData; see block_stream::unmarshal_block_data).
        unmarshal_block_data(&mut self.block, &bh, &self.timestamps_buf, &self.values_buf)
            .map_err(|err| BsrError::Other(format!("cannot unmarshal block: {err}")))?;

        Ok(())
    }

    /// Go: blockStreamReader.readIndexBlock.
    fn read_index_block(&mut self) -> Result<(), BsrError> {
        // Go to the next metaindex row.
        if self.mr_idx >= self.mrs.len() {
            return Err(BsrError::Eof);
        }
        let mr = self.mrs[self.mr_idx];
        self.cur_mr = Some(self.mr_idx);
        self.mr_idx += 1;

        // Validate the metaindex row.
        if self.index_block_offset != mr.index_block_offset {
            return Err(BsrError::Other(format!(
                "invalid IndexBlockOffset in metaindex row; got {}; want {}",
                mr.index_block_offset, self.index_block_offset
            )));
        }
        if mr.min_timestamp < self.ph.min_timestamp {
            return Err(BsrError::Other(format!(
                "invalid MinTimestamp in metaindex row; got {}; cannot be smaller than {}",
                mr.min_timestamp, self.ph.min_timestamp
            )));
        }
        if mr.max_timestamp > self.ph.max_timestamp {
            return Err(BsrError::Other(format!(
                "invalid MaxTimestamp in metaindex row; got {}; cannot be bigger than {}",
                mr.max_timestamp, self.ph.max_timestamp
            )));
        }

        // Read the index block.
        self.compressed_index_data
            .resize(mr.index_block_size as usize, 0);
        self.index_reader.must_read(&mut self.compressed_index_data);
        self.index_data.clear();
        decompress_zstd(&mut self.index_data, &self.compressed_index_data).map_err(|err| {
            BsrError::Other(format!(
                "cannot decompress index block at offset {}: {err}",
                self.index_block_offset
            ))
        })?;
        self.index_cursor = 0;

        // Update offsets.
        self.index_block_offset += mr.index_block_size as u64;

        Ok(())
    }

    /// Go: blockStreamReader.prevIndexBlockOffset.
    fn prev_index_block_offset(&self) -> u64 {
        match self.cur_mr {
            Some(cur) => self.index_block_offset - self.mrs[cur].index_block_size as u64,
            None => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::part::inmemory::InmemoryPart;
    use crate::raw_row::RawRow;
    use crate::util::splitmix64;

    const DEFAULT_PRECISION_BITS: u8 = 4;

    fn rand_below(state: &mut u64, n: u64) -> u64 {
        splitmix64(state) % n
    }

    fn new_test_block_stream_reader(rows: &mut [RawRow]) -> BlockStreamReader {
        let mp = InmemoryPart::init_from_rows(rows);
        BlockStreamReader::from_inmemory_part(&mp)
    }

    // Port of testBlocksStreamReader.
    fn check_block_stream_reader(rows: &mut [RawRow], expected_blocks_count: usize) {
        let rows_len = rows.len();
        let mut bsr = new_test_block_stream_reader(rows);
        let mut blocks_count = 0usize;
        let mut rows_count = 0usize;
        while bsr.next_block() {
            bsr.block.unmarshal_data().expect("cannot unmarshal block");
            rows_count += bsr.block.timestamps().len();
            blocks_count += 1;
        }
        assert_eq!(bsr.error(), None);
        assert_eq!(
            blocks_count, expected_blocks_count,
            "unexpected number of blocks read"
        );
        assert_eq!(rows_count, rows_len, "unexpected number of rows read");
    }

    // Port of TestBlockStreamReaderSingleRow.
    #[test]
    fn single_row() {
        let mut rows = [RawRow {
            timestamp: 12334545,
            value: 1.2345,
            precision_bits: DEFAULT_PRECISION_BITS,
            ..Default::default()
        }];
        check_block_stream_reader(&mut rows, 1);
    }

    // Port of TestBlockStreamReaderSingleBlockManyRows.
    #[test]
    fn single_block_many_rows() {
        let mut state = 1u64;
        let mut rows: Vec<RawRow> = (0..crate::block::MAX_ROWS_PER_BLOCK)
            .map(|i| RawRow {
                value: rand_below(&mut state, 1_000_000_000) as f64 - 5e8,
                timestamp: (i as i64) * 1_000_000_000,
                precision_bits: DEFAULT_PRECISION_BITS,
                ..Default::default()
            })
            .collect();
        check_block_stream_reader(&mut rows, 1);
    }

    // Port of TestBlockStreamReaderSingleTSIDManyBlocks.
    #[test]
    fn single_tsid_many_blocks() {
        let mut state = 1u64;
        let mut rows: Vec<RawRow> = (0..5 * crate::block::MAX_ROWS_PER_BLOCK)
            .map(|_| RawRow {
                value: (rand_below(&mut state, 20_000) as f64 - 10_000.0) * 1.5,
                timestamp: rand_below(&mut state, 2_000_000_000) as i64 - 1_000_000_000,
                precision_bits: 1,
                ..Default::default()
            })
            .collect();
        check_block_stream_reader(&mut rows, 5);
    }

    // Port of TestBlockStreamReaderManyTSIDSingleRow.
    #[test]
    fn many_tsid_single_row() {
        let mut state = 1u64;
        let mut rows: Vec<RawRow> = (0..1000)
            .map(|i| {
                let mut r = RawRow {
                    value: rand_below(&mut state, 1_000_000_000) as f64 - 5e8,
                    timestamp: (i as i64) * 1_000_000_000,
                    precision_bits: DEFAULT_PRECISION_BITS,
                    ..Default::default()
                };
                r.tsid.metric_id = i as u64;
                r
            })
            .collect();
        check_block_stream_reader(&mut rows, 1000);
    }

    // Port of TestBlockStreamReaderManyTSIDManyRows.
    #[test]
    fn many_tsid_many_rows() {
        let mut state = 1u64;
        const BLOCKS: usize = 123;
        let mut rows: Vec<RawRow> = (0..3210)
            .map(|i| {
                let mut r = RawRow {
                    value: rand_below(&mut state, 1000) as f64 / 1000.0,
                    timestamp: rand_below(&mut state, 1_000_000_000) as i64,
                    precision_bits: DEFAULT_PRECISION_BITS,
                    ..Default::default()
                };
                r.tsid.metric_id = ((1_000_000_000usize - i) % BLOCKS) as u64;
                r
            })
            .collect();
        check_block_stream_reader(&mut rows, BLOCKS);
    }

    // Port of TestBlockStreamReaderReadConcurrent.
    #[test]
    fn read_concurrent() {
        let mut state = 1u64;
        const BLOCKS: usize = 123;
        let mut rows: Vec<RawRow> = (0..3210)
            .map(|i| {
                let mut r = RawRow {
                    value: rand_below(&mut state, 1000) as f64 / 1000.0,
                    timestamp: rand_below(&mut state, 1_000_000_000) as i64,
                    precision_bits: DEFAULT_PRECISION_BITS,
                    ..Default::default()
                };
                r.tsid.metric_id = ((1_000_000_000usize - i) % BLOCKS) as u64;
                r
            })
            .collect();
        let rows_len = rows.len();
        let mp = InmemoryPart::init_from_rows(&mut rows);

        std::thread::scope(|s| {
            let handles: Vec<_> = (0..5)
                .map(|_| {
                    s.spawn(|| -> Result<(), String> {
                        let mut bsr = BlockStreamReader::from_inmemory_part(&mp);
                        let mut rows_count = 0usize;
                        while bsr.next_block() {
                            bsr.block.unmarshal_data()?;
                            rows_count += bsr.block.timestamps().len();
                        }
                        if let Some(err) = bsr.error() {
                            return Err(err);
                        }
                        if rows_count != rows_len {
                            return Err(format!(
                                "unexpected number of rows read; got {rows_count}; want {rows_len}"
                            ));
                        }
                        Ok(())
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap().unwrap();
            }
        });
    }
}
