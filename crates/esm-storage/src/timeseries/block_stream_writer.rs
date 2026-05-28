//! Stream writer for an on-disk time-series part.
//!
//! Mirrors VM's `lib/storage/block_stream_writer.go`. Produces:
//! `timestamps.bin`, `values.bin`, `index.bin`, `metaindex.bin`, plus
//! `metadata.json` on `finish()`.

use std::fs::{File, create_dir_all};
use std::io::{self, BufWriter, Write as _};
use std::path::{Path, PathBuf};

use esm_compress::timeseries::{self as ts_codec, MarshalArrayResult, TsError};
use esm_compress::zstd_codec::{ZstdError, compress_zstd_level};
use thiserror::Error;

use super::{
    BlockHeader, MAX_ROWS_PER_BLOCK, MetaindexRow, PartHeader, Tsid,
    block_header::{self as bh_module},
    filenames::{INDEX, METADATA, METAINDEX, TIMESTAMPS, VALUES},
};

/// Default precision-bits passed to the value encoder. Lossless. Lower
/// precision lands when Phase 1B gains the precision-bits codec.
pub const DEFAULT_PRECISION_BITS: u8 = 64;

/// Default scale factor `10^Scale` (= 1.0, no scaling).
pub const DEFAULT_SCALE: i16 = 0;

/// Writes one or more blocks (each a sorted (timestamps, values) pair for a
/// single TSID) to a part directory.
#[allow(missing_debug_implementations)] // file handles + buffers
pub struct BlockStreamWriter {
    path: PathBuf,
    index_compress_level: i32,

    timestamps_writer: BufWriter<File>,
    values_writer: BufWriter<File>,
    index_writer: BufWriter<File>,
    metaindex_writer: BufWriter<File>,

    // Index-block scratch.
    unpacked_index_block_buf: Vec<u8>,
    packed_index_block_buf: Vec<u8>,
    unpacked_metaindex_buf: Vec<u8>,
    packed_metaindex_buf: Vec<u8>,
    cur_metaindex_row: MetaindexRow,

    timestamps_block_offset: u64,
    values_block_offset: u64,
    index_block_offset: u64,

    // Part-level state.
    rows_count: u64,
    blocks_count: u64,
    min_timestamp: i64,
    max_timestamp: i64,
    first_block_caught: bool,
}

impl BlockStreamWriter {
    /// Open a fresh part directory. The path must not exist.
    ///
    /// # Errors
    /// Returns `io::Error` if the directory or any file cannot be created.
    pub fn create(path: impl Into<PathBuf>, index_compress_level: i32) -> io::Result<Self> {
        let path = path.into();
        if path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("part path already exists: {}", path.display()),
            ));
        }
        create_dir_all(&path)?;

        let timestamps_writer = BufWriter::new(File::create(path.join(TIMESTAMPS))?);
        let values_writer = BufWriter::new(File::create(path.join(VALUES))?);
        let index_writer = BufWriter::new(File::create(path.join(INDEX))?);
        let metaindex_writer = BufWriter::new(File::create(path.join(METAINDEX))?);

        let mut mr = MetaindexRow::default();
        mr.reset();

        Ok(Self {
            path,
            index_compress_level,
            timestamps_writer,
            values_writer,
            index_writer,
            metaindex_writer,
            unpacked_index_block_buf: Vec::new(),
            packed_index_block_buf: Vec::new(),
            unpacked_metaindex_buf: Vec::new(),
            packed_metaindex_buf: Vec::new(),
            cur_metaindex_row: mr,
            timestamps_block_offset: 0,
            values_block_offset: 0,
            index_block_offset: 0,
            rows_count: 0,
            blocks_count: 0,
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
            first_block_caught: false,
        })
    }

    /// Append a single block of samples for `tsid`. `timestamps` and
    /// `values` must be the same length, sorted by timestamp, and
    /// non-empty.
    ///
    /// `scale` (10^N multiplier) and `precision_bits` are passed through
    /// to the per-block header.
    ///
    /// # Errors
    /// See [`WriteError`].
    pub fn write_block(
        &mut self,
        tsid: Tsid,
        timestamps: &[i64],
        values: &[i64],
        scale: i16,
        precision_bits: u8,
    ) -> Result<(), WriteError> {
        if timestamps.is_empty() {
            return Err(WriteError::EmptyBlock);
        }
        if timestamps.len() != values.len() {
            return Err(WriteError::LengthMismatch {
                timestamps: timestamps.len(),
                values: values.len(),
            });
        }
        let rows = u32::try_from(timestamps.len())
            .map_err(|_| WriteError::TooManyRows(timestamps.len()))?;
        if rows > MAX_ROWS_PER_BLOCK {
            return Err(WriteError::RowsExceedMax { got: rows, max: MAX_ROWS_PER_BLOCK });
        }

        // Encode timestamps + values into scratch buffers.
        let mut ts_buf = Vec::new();
        let MarshalArrayResult { marshal_type: ts_mt, first_value: ts_first } =
            ts_codec::marshal_int64_array(&mut ts_buf, timestamps, precision_bits)
                .map_err(WriteError::Ts)?;
        let mut v_buf = Vec::new();
        let MarshalArrayResult { marshal_type: v_mt, first_value: v_first } =
            ts_codec::marshal_int64_array(&mut v_buf, values, precision_bits)
                .map_err(WriteError::Ts)?;

        // Write payloads.
        self.timestamps_writer.write_all(&ts_buf)?;
        self.values_writer.write_all(&v_buf)?;

        let ts_block_size =
            u32::try_from(ts_buf.len()).map_err(|_| WriteError::BlockSizeOverflow(ts_buf.len()))?;
        let v_block_size =
            u32::try_from(v_buf.len()).map_err(|_| WriteError::BlockSizeOverflow(v_buf.len()))?;

        // Compute block-level min/max from the input. timestamps[0] = min,
        // timestamps[len-1] = max because the input must be sorted by ts.
        let min_ts = timestamps[0];
        let max_ts = timestamps[timestamps.len() - 1];

        let bh = BlockHeader {
            tsid,
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            first_value: v_first,
            timestamps_block_offset: self.timestamps_block_offset,
            values_block_offset: self.values_block_offset,
            timestamps_block_size: ts_block_size,
            values_block_size: v_block_size,
            rows_count: rows,
            scale,
            timestamps_marshal_type: ts_mt,
            values_marshal_type: v_mt,
            precision_bits,
        };
        bh.validate().map_err(WriteError::BlockHeader)?;
        // `ts_first` lives in min_timestamp for time-series — assert
        // consistency in debug builds.
        debug_assert_eq!(ts_first, min_ts, "marshal_int64_array first value vs timestamps[0]");

        // Append block header to the staging index block. VM caps the
        // unpacked index block at `2 * marshaledBlockHeaderSize * 1024`
        // implicitly; we trigger a flush once the buffer exceeds
        // `2 * MAX_BLOCK_SIZE` bytes (matches the writer in
        // `lib/storage/block_stream_writer.go` which flushes when the
        // count reaches `1024`).
        let buf_len_before = self.unpacked_index_block_buf.len();
        bh.marshal(&mut self.unpacked_index_block_buf);
        if self.unpacked_index_block_buf.len() > MAX_INDEX_BLOCK_BYTES {
            self.unpacked_index_block_buf.truncate(buf_len_before);
            self.flush_index_data()?;
            bh.marshal(&mut self.unpacked_index_block_buf);
        }

        // Register with the current metaindex row.
        self.cur_metaindex_row.register_block_header(&bh);

        // Track per-part counters.
        self.timestamps_block_offset += u64::from(ts_block_size);
        self.values_block_offset += u64::from(v_block_size);
        self.rows_count += u64::from(rows);
        self.blocks_count += 1;
        if self.first_block_caught {
            if min_ts < self.min_timestamp {
                self.min_timestamp = min_ts;
            }
            if max_ts > self.max_timestamp {
                self.max_timestamp = max_ts;
            }
        } else {
            self.min_timestamp = min_ts;
            self.max_timestamp = max_ts;
            self.first_block_caught = true;
        }

        Ok(())
    }

    fn flush_index_data(&mut self) -> Result<(), WriteError> {
        if self.unpacked_index_block_buf.is_empty() {
            return Ok(());
        }

        self.packed_index_block_buf.clear();
        compress_zstd_level(
            &mut self.packed_index_block_buf,
            &self.unpacked_index_block_buf,
            self.index_compress_level,
        )?;
        self.index_writer.write_all(&self.packed_index_block_buf)?;

        let index_block_size = u32::try_from(self.packed_index_block_buf.len())
            .map_err(|_| WriteError::BlockSizeOverflow(self.packed_index_block_buf.len()))?;
        self.cur_metaindex_row.index_block_offset = self.index_block_offset;
        self.cur_metaindex_row.index_block_size = index_block_size;
        self.index_block_offset += u64::from(index_block_size);

        self.cur_metaindex_row.marshal(&mut self.unpacked_metaindex_buf);

        self.unpacked_index_block_buf.clear();
        self.cur_metaindex_row.reset();

        Ok(())
    }

    /// Finalise the part, returning the `PartHeader` and syncing all files.
    ///
    /// # Errors
    /// See [`WriteError`].
    pub fn finish(mut self) -> Result<PartHeader, WriteError> {
        if self.blocks_count == 0 {
            return Err(WriteError::EmptyPart);
        }
        self.flush_index_data()?;

        self.packed_metaindex_buf.clear();
        compress_zstd_level(
            &mut self.packed_metaindex_buf,
            &self.unpacked_metaindex_buf,
            self.index_compress_level,
        )?;
        self.metaindex_writer.write_all(&self.packed_metaindex_buf)?;

        self.timestamps_writer
            .into_inner()
            .map_err(|e| WriteError::Io(e.into_error()))?
            .sync_all()?;
        self.values_writer.into_inner().map_err(|e| WriteError::Io(e.into_error()))?.sync_all()?;
        self.index_writer.into_inner().map_err(|e| WriteError::Io(e.into_error()))?.sync_all()?;
        self.metaindex_writer
            .into_inner()
            .map_err(|e| WriteError::Io(e.into_error()))?
            .sync_all()?;

        let ph = PartHeader {
            rows_count: self.rows_count,
            blocks_count: self.blocks_count,
            min_timestamp: self.min_timestamp,
            max_timestamp: self.max_timestamp,
        };
        let metadata_bytes = ph.to_json().map_err(WriteError::Metadata)?;
        let metadata_path = self.path.join(METADATA);
        let mut metadata_file = File::create(&metadata_path)?;
        metadata_file.write_all(&metadata_bytes)?;
        metadata_file.sync_all()?;

        esm_platform::durability::fsync_dir(&self.path)?;
        Ok(ph)
    }

    /// Path the writer was opened against.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Flush threshold for the unpacked index block buffer. We choose
/// `1024 * 81` bytes (1024 block headers, each fixed-size 81 bytes), matching
/// the count-based trigger VM uses in its writer.
const MAX_INDEX_BLOCK_BYTES: usize = 1024 * bh_module::SIZE;

#[derive(Debug, Error)]
pub enum WriteError {
    #[error("cannot write empty block")]
    EmptyBlock,
    #[error("timestamps.len()={timestamps} differs from values.len()={values}")]
    LengthMismatch { timestamps: usize, values: usize },
    #[error("rows={got} exceeds MAX_ROWS_PER_BLOCK={max}")]
    RowsExceedMax { got: u32, max: u32 },
    #[error("block row count {0} exceeds u32::MAX")]
    TooManyRows(usize),
    #[error("block size {0} exceeds u32::MAX")]
    BlockSizeOverflow(usize),
    #[error("cannot finish a part with zero blocks")]
    EmptyPart,
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Zstd(#[from] ZstdError),
    #[error(transparent)]
    Ts(TsError),
    #[error("block header validation: {0}")]
    BlockHeader(bh_module::BlockHeaderError),
    #[error("write metadata.json: {0}")]
    Metadata(serde_json::Error),
}
