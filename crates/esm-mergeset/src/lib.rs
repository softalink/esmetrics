//! Port of the upstream VictoriaMetrics `lib/mergeset` (v1.146.0).
//!
//! An LSM-tree over sorted byte-string items (the whole item is the key),
//! used as the storage engine for the inverted index.

mod block_header;
mod block_stream_reader;
mod block_stream_writer;
pub mod blockcache;
mod inmemory_block;
mod inmemory_part;
mod merge;
mod metaindex_row;
mod part;
mod part_header;
mod part_search;
mod part_wrapper;
mod raw_items;
mod table;
mod table_merge;
mod table_parts;
mod table_search;
mod util;

pub use inmemory_block::{Item, MAX_INMEMORY_BLOCK_SIZE};
pub use merge::PrepareBlockCallback;
pub use part::{
    set_data_blocks_cache_size, set_data_blocks_sparse_cache_size, set_index_blocks_cache_size,
};
pub use table::{FlushCallback, Table, TableMetrics};
pub use table_search::TableSearch;

#[doc(hidden)]
pub use raw_items::set_raw_items_shard_params_for_tests;

pub(crate) mod filenames {
    pub const METAINDEX_FILENAME: &str = "metaindex.bin";
    pub const INDEX_FILENAME: &str = "index.bin";
    pub const ITEMS_FILENAME: &str = "items.bin";
    pub const LENS_FILENAME: &str = "lens.bin";
    pub const METADATA_FILENAME: &str = "metadata.json";
    pub const PARTS_FILENAME: &str = "parts.json";
}

/// Error returned by search operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// End of data reached (equivalent of Go's `io.EOF`).
    Eof,
    /// Any other (corruption/IO) error.
    Other(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Eof => write!(f, "EOF"),
            Error::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for Error {}
