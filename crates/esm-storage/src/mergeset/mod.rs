//! Mergeset: VictoriaMetrics-compatible LSM-like sorted KV store used for
//! the inverted index. See [`docs/format/mergeset-part.md`](../../../../docs/format/mergeset-part.md)
//! for the canonical byte-layout specification.
//!
//! Phase 1A.3 ships **types and module boundaries only**. Reader, writer, and
//! merger implementations land in Phase 1A.4–1A.7.

pub mod block_header;
pub mod block_stream_reader;
pub mod block_stream_writer;
pub mod inmemory_block;
pub mod marshal_type;
pub mod metaindex_row;
pub mod part_header;
pub mod storage_block;

pub use block_header::BlockHeader;
pub use block_stream_reader::BlockStreamReader;
pub use block_stream_writer::BlockStreamWriter;
pub use inmemory_block::InmemoryBlock;
pub use marshal_type::MarshalType;
pub use metaindex_row::MetaindexRow;
pub use part_header::PartHeader;
pub use storage_block::StorageBlock;

/// Maximum in-memory block size; matches VM's `maxInmemoryBlockSize`
/// (`lib/mergeset/encoding.go:184`). Sized to fit the L1 cache of current
/// CPUs.
pub const MAX_INMEMORY_BLOCK_SIZE: usize = 64 * 1024;

/// Maximum unpacked size of an index block before the writer flushes it.
/// Matches VM's `maxIndexBlockSize` (`lib/mergeset/block_stream_writer.go:166`).
pub const MAX_INDEX_BLOCK_SIZE: usize = 64 * 1024;

/// On-disk filenames inside a part directory. Matches
/// `lib/mergeset/filenames.go`.
pub mod filenames {
    pub const METAINDEX: &str = "metaindex.bin";
    pub const INDEX: &str = "index.bin";
    pub const ITEMS: &str = "items.bin";
    pub const LENS: &str = "lens.bin";
    pub const METADATA: &str = "metadata.json";
}
