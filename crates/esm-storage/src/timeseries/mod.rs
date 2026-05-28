//! Time-series storage: VictoriaMetrics-compatible per-day parts holding
//! timestamps + values for one or more series. See
//! [`docs/format/timeseries-part.md`](../../../../docs/format/timeseries-part.md).
//!
//! Phase 1C ships types + on-the-wire marshal/unmarshal for `TSID` and
//! `BlockHeader`. Block readers/writers and the part-level IO layer follow.

pub mod block_header;
pub mod block_stream_reader;
pub mod block_stream_writer;
pub mod metaindex_row;
pub mod part_header;
pub mod tsid;

pub use block_header::BlockHeader;
pub use block_stream_reader::BlockStreamReader;
pub use block_stream_writer::BlockStreamWriter;
pub use metaindex_row::MetaindexRow;
pub use part_header::PartHeader;
pub use tsid::Tsid;

/// Default zstd compression level VM uses for the index_block / metaindex
/// payloads (the per-block timestamp+value blocks use a level derived from
/// the block size; see `esm_compress::timeseries`).
pub const DEFAULT_INDEX_COMPRESS_LEVEL: i32 = esm_compress::zstd_codec::DEFAULT_LEVEL;

/// On-disk filenames inside a time-series part directory. Matches
/// `lib/storage/part.go:55-66`.
pub mod filenames {
    pub const METAINDEX: &str = "metaindex.bin";
    pub const INDEX: &str = "index.bin";
    pub const TIMESTAMPS: &str = "timestamps.bin";
    pub const VALUES: &str = "values.bin";
    pub const METADATA: &str = "metadata.json";
}

/// Maximum rows per block (VM `maxRowsPerBlock` in `lib/storage/`).
/// Used to validate `BlockHeader::rows_count`.
pub const MAX_ROWS_PER_BLOCK: u32 = 8 * 1024;

/// Maximum per-block payload size after encoding (VM `maxBlockSize`,
/// `lib/storage/encoding.go`). Both timestamps and values are bounded by
/// `2 * MAX_BLOCK_SIZE` post-marshal.
pub const MAX_BLOCK_SIZE: u32 = 128 * 1024;
