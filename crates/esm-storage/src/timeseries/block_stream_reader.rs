//! Stream reader for an on-disk time-series part.
//!
//! Mirrors VM's `lib/storage/block_stream_reader.go`. Yields fully-decoded
//! `(BlockHeader, timestamps, values)` triples one at a time.

use std::fs::File;
use std::io::{self, Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use esm_compress::timeseries as ts_codec;
use esm_compress::zstd_codec::{ZstdError, decompress_zstd};
use thiserror::Error;

use super::{
    BlockHeader, MetaindexRow, PartHeader, Tsid,
    block_header::BlockHeaderError,
    filenames::{INDEX, METADATA, METAINDEX, TIMESTAMPS, VALUES},
    metaindex_row::{UnmarshalMetaindexError, unmarshal_metaindex_rows},
    part_header::PartHeaderError,
};

/// One decoded block ready for downstream consumption.
#[derive(Debug, Clone)]
pub struct DecodedBlock {
    pub header: BlockHeader,
    pub timestamps: Vec<i64>,
    pub values: Vec<i64>,
}

/// Opened on-disk time-series part. Iterate via [`Self::next_block`].
#[allow(missing_debug_implementations)] // file handles + buffers
pub struct BlockStreamReader {
    path: PathBuf,
    pub part_header: PartHeader,
    metaindex_rows: Arc<Vec<MetaindexRow>>,

    index_file: File,
    timestamps_file: File,
    values_file: File,

    metaindex_idx: usize,
    cur_index_block: Vec<BlockHeader>,
    cur_index_idx: usize,

    scratch_index_compressed: Vec<u8>,
    scratch_index_unpacked: Vec<u8>,
    scratch_ts_payload: Vec<u8>,
    scratch_v_payload: Vec<u8>,
}

impl BlockStreamReader {
    /// Open the part at `path`.
    ///
    /// # Errors
    /// Returns [`ReadError`] on I/O, parse, or validation failure.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, ReadError> {
        let path = path.into();
        let metadata_bytes = std::fs::read(path.join(METADATA))?;
        let part_header = PartHeader::from_json(&metadata_bytes)?;

        let metaindex_bytes = std::fs::read(path.join(METAINDEX))?;
        let mut metaindex_raw = Vec::new();
        decompress_zstd(&mut metaindex_raw, &metaindex_bytes)?;
        let mut metaindex_rows = Vec::new();
        unmarshal_metaindex_rows(&mut metaindex_rows, &metaindex_raw)?;

        Self::open_with_index(path, part_header, Arc::new(metaindex_rows))
    }

    /// Open a part reusing an already-parsed header and metaindex (cached by
    /// the caller), so repeated reads of the same part skip re-reading and
    /// re-decompressing `metadata`/`metaindex` — only the three data files are
    /// opened. The hot query path opens each overlapping part once per series,
    /// so this avoids a JSON parse + zstd decompress per series read.
    ///
    /// # Errors
    /// Returns [`ReadError`] if the index/timestamps/values files can't open.
    pub fn open_with_index(
        path: impl Into<PathBuf>,
        part_header: PartHeader,
        metaindex_rows: Arc<Vec<MetaindexRow>>,
    ) -> Result<Self, ReadError> {
        let path = path.into();
        Ok(Self {
            index_file: File::open(path.join(INDEX))?,
            timestamps_file: File::open(path.join(TIMESTAMPS))?,
            values_file: File::open(path.join(VALUES))?,
            path,
            part_header,
            metaindex_rows,
            metaindex_idx: 0,
            cur_index_block: Vec::new(),
            cur_index_idx: 0,
            scratch_index_compressed: Vec::new(),
            scratch_index_unpacked: Vec::new(),
            scratch_ts_payload: Vec::new(),
            scratch_v_payload: Vec::new(),
        })
    }

    /// Shared handle to this part's parsed metaindex, for caching in
    /// `parts_index` and reuse via [`Self::open_with_index`].
    #[must_use]
    pub fn metaindex(&self) -> Arc<Vec<MetaindexRow>> {
        Arc::clone(&self.metaindex_rows)
    }

    /// Yield the next decoded block. `Ok(None)` at end-of-part.
    ///
    /// # Errors
    /// See [`ReadError`].
    pub fn next_block(&mut self) -> Result<Option<DecodedBlock>, ReadError> {
        loop {
            if self.cur_index_idx >= self.cur_index_block.len() {
                if self.metaindex_idx >= self.metaindex_rows.len() {
                    return Ok(None);
                }
                self.load_current_index_block()?;
                self.metaindex_idx += 1;
                if self.cur_index_block.is_empty() {
                    continue;
                }
            }
            let bh_idx = self.cur_index_idx;
            self.cur_index_idx += 1;
            let bh = self.cur_index_block[bh_idx].clone();
            let (timestamps, values) = self.read_data_block(&bh)?;
            return Ok(Some(DecodedBlock { header: bh, timestamps, values }));
        }
    }

    /// Yield the next block header without decoding its timestamps/values.
    /// Pair with [`Self::read_data_block_for`] to fetch the payload on
    /// demand. Used by the TSID-keyed fast path so non-matching blocks
    /// don't pay decompression cost.
    ///
    /// # Errors
    /// See [`ReadError`].
    pub fn next_block_header(&mut self) -> Result<Option<BlockHeader>, ReadError> {
        loop {
            if self.cur_index_idx >= self.cur_index_block.len() {
                if self.metaindex_idx >= self.metaindex_rows.len() {
                    return Ok(None);
                }
                self.load_current_index_block()?;
                self.metaindex_idx += 1;
                if self.cur_index_block.is_empty() {
                    continue;
                }
            }
            let bh_idx = self.cur_index_idx;
            self.cur_index_idx += 1;
            return Ok(Some(self.cur_index_block[bh_idx].clone()));
        }
    }

    /// Decode the timestamps + values for a header produced by
    /// [`Self::next_block_header`].
    ///
    /// # Errors
    /// See [`ReadError`].
    pub fn read_data_block_for(
        &mut self,
        header: &BlockHeader,
    ) -> Result<(Vec<i64>, Vec<i64>), ReadError> {
        self.read_data_block(header)
    }

    /// Advance the cursor to the first index block whose starting TSID is
    /// `<= target`. After this call, [`Self::next_block_header`] yields
    /// the first block of that index block. If no index block starts at or
    /// before `target`, the cursor is positioned at the end (so the next
    /// header call returns `None`).
    ///
    /// Internally a binary search over the metaindex rows. Safe to call
    /// repeatedly; resets in-progress block decoding.
    pub fn seek_to_tsid(&mut self, target: Tsid) {
        // Find the largest i where metaindex_rows[i].tsid <= target.
        // Rows are sorted by tsid (the writer guarantees this).
        let n = self.metaindex_rows.len();
        if n == 0 {
            self.metaindex_idx = 0;
            self.cur_index_block.clear();
            self.cur_index_idx = 0;
            return;
        }
        // partition_point returns the index of the first row whose tsid > target;
        // candidates are rows[0..pp].
        let pp = self.metaindex_rows.partition_point(|r| r.tsid <= target);
        let start = pp.saturating_sub(1);
        self.metaindex_idx = start;
        self.cur_index_block.clear();
        self.cur_index_idx = 0;
    }

    fn load_current_index_block(&mut self) -> Result<(), ReadError> {
        let mr = &self.metaindex_rows[self.metaindex_idx];
        self.index_file.seek(SeekFrom::Start(mr.index_block_offset))?;
        let size = usize::try_from(mr.index_block_size)
            .map_err(|_| ReadError::SizeOverflow(u64::from(mr.index_block_size)))?;
        self.scratch_index_compressed.resize(size, 0);
        self.index_file.read_exact(&mut self.scratch_index_compressed)?;
        decompress_zstd(&mut self.scratch_index_unpacked, &self.scratch_index_compressed)?;

        // Parse fixed-size headers back-to-back until the buffer is exhausted.
        self.cur_index_block.clear();
        let mut cursor: &[u8] = &self.scratch_index_unpacked;
        let want = usize::try_from(mr.block_headers_count)
            .map_err(|_| ReadError::SizeOverflow(u64::from(mr.block_headers_count)))?;
        while !cursor.is_empty() {
            let (bh, rest) = BlockHeader::unmarshal(cursor)?;
            self.cur_index_block.push(bh);
            cursor = rest;
        }
        if self.cur_index_block.len() != want {
            return Err(ReadError::HeaderCountMismatch { got: self.cur_index_block.len(), want });
        }
        self.cur_index_idx = 0;
        Ok(())
    }

    fn read_data_block(&mut self, bh: &BlockHeader) -> Result<(Vec<i64>, Vec<i64>), ReadError> {
        self.timestamps_file.seek(SeekFrom::Start(bh.timestamps_block_offset))?;
        let ts_size = usize::try_from(bh.timestamps_block_size)
            .map_err(|_| ReadError::SizeOverflow(u64::from(bh.timestamps_block_size)))?;
        self.scratch_ts_payload.resize(ts_size, 0);
        self.timestamps_file.read_exact(&mut self.scratch_ts_payload)?;

        self.values_file.seek(SeekFrom::Start(bh.values_block_offset))?;
        let v_size = usize::try_from(bh.values_block_size)
            .map_err(|_| ReadError::SizeOverflow(u64::from(bh.values_block_size)))?;
        self.scratch_v_payload.resize(v_size, 0);
        self.values_file.read_exact(&mut self.scratch_v_payload)?;

        let mut timestamps = Vec::new();
        ts_codec::unmarshal_int64_array(
            &mut timestamps,
            &self.scratch_ts_payload,
            bh.timestamps_marshal_type,
            bh.min_timestamp,
            bh.rows_count as usize,
        )
        .map_err(ReadError::Ts)?;

        let mut values = Vec::new();
        ts_codec::unmarshal_int64_array(
            &mut values,
            &self.scratch_v_payload,
            bh.values_marshal_type,
            bh.first_value,
            bh.rows_count as usize,
        )
        .map_err(ReadError::Ts)?;

        Ok((timestamps, values))
    }

    /// Path the reader was opened against.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Zstd(#[from] ZstdError),
    #[error(transparent)]
    Metadata(#[from] PartHeaderError),
    #[error(transparent)]
    Metaindex(#[from] UnmarshalMetaindexError),
    #[error(transparent)]
    BlockHeader(#[from] BlockHeaderError),
    #[error(transparent)]
    Ts(ts_codec::TsError),
    #[error("expected {want} block headers in index block, got {got}")]
    HeaderCountMismatch { got: usize, want: usize },
    #[error("size {0} does not fit in usize on this platform")]
    SizeOverflow(u64),
}

#[cfg(test)]
mod tests {
    use super::super::Tsid;
    use super::super::block_stream_writer::{
        BlockStreamWriter, DEFAULT_PRECISION_BITS, DEFAULT_SCALE,
    };
    use super::*;

    fn level() -> i32 {
        esm_compress::zstd_codec::DEFAULT_LEVEL
    }

    #[test]
    fn single_block_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let part_path = tmp.path().join("part-0");
        let mut w = BlockStreamWriter::create(&part_path, level()).unwrap();
        let tsid = Tsid { metric_group_id: 1, job_id: 2, instance_id: 3, metric_id: 100 };
        let timestamps: Vec<i64> = (0..10).map(|i| 1_000 + i * 100).collect();
        let values: Vec<i64> = (0..10).map(|i| 42 + i).collect();
        w.write_block(tsid, &timestamps, &values, DEFAULT_SCALE, DEFAULT_PRECISION_BITS).unwrap();
        let ph = w.finish().unwrap();
        assert_eq!(ph.rows_count, 10);
        assert_eq!(ph.blocks_count, 1);
        assert_eq!(ph.min_timestamp, 1_000);
        assert_eq!(ph.max_timestamp, 1_900);

        let mut r = BlockStreamReader::open(&part_path).unwrap();
        let block = r.next_block().unwrap().unwrap();
        assert_eq!(block.header.tsid, tsid);
        assert_eq!(block.timestamps, timestamps);
        assert_eq!(block.values, values);
        assert!(r.next_block().unwrap().is_none());
    }

    #[test]
    fn multi_block_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let part_path = tmp.path().join("part-0");
        let mut w = BlockStreamWriter::create(&part_path, level()).unwrap();

        let mk_block = |metric_id: u64, base_ts: i64| -> (Tsid, Vec<i64>, Vec<i64>) {
            let tsid = Tsid { metric_id, ..Default::default() };
            let ts: Vec<i64> = (0..20).map(|i| base_ts + i * 1_000).collect();
            let vals: Vec<i64> =
                (0..20).map(|i| i64::try_from(metric_id).unwrap() * 1000 + i).collect();
            (tsid, ts, vals)
        };

        let blocks = [mk_block(1, 100), mk_block(2, 200), mk_block(3, 50)];
        for (tsid, ts, vals) in &blocks {
            w.write_block(*tsid, ts, vals, DEFAULT_SCALE, DEFAULT_PRECISION_BITS).unwrap();
        }
        let ph = w.finish().unwrap();
        assert_eq!(ph.rows_count, 60);
        assert_eq!(ph.blocks_count, 3);
        assert_eq!(ph.min_timestamp, 50);

        let mut r = BlockStreamReader::open(&part_path).unwrap();
        for (tsid, ts, vals) in &blocks {
            let b = r.next_block().unwrap().expect("more blocks");
            assert_eq!(b.header.tsid, *tsid);
            assert_eq!(&b.timestamps, ts);
            assert_eq!(&b.values, vals);
        }
        assert!(r.next_block().unwrap().is_none());
    }

    #[test]
    fn large_block_uses_zstd_path() {
        let tmp = tempfile::tempdir().unwrap();
        let part_path = tmp.path().join("part-0");
        let mut w = BlockStreamWriter::create(&part_path, level()).unwrap();

        let tsid = Tsid { metric_id: 999, ..Default::default() };
        let timestamps: Vec<i64> = (0..500).map(|i| 1_000_000 + i * 1_000).collect();
        let values: Vec<i64> = (0..500).map(|i| i * 7 + 100).collect();
        w.write_block(tsid, &timestamps, &values, DEFAULT_SCALE, DEFAULT_PRECISION_BITS).unwrap();
        w.finish().unwrap();

        let mut r = BlockStreamReader::open(&part_path).unwrap();
        let b = r.next_block().unwrap().unwrap();
        assert_eq!(b.timestamps, timestamps);
        assert_eq!(b.values, values);
    }
}
