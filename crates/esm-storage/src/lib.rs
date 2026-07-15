//! esm-storage: Rust port of the upstream VictoriaMetrics v1.146.0 `lib/storage`.
//!
//! Stage 1 (core data types and codecs, no I/O):
//! - [`tsid`] — TSID (24-byte marshaled form, part sort key).
//! - [`metric_name`] — Tag/MetricName, escaping, sortTags canonicalization,
//!   canonical (index) and raw (MetricNameRaw) encodings.
//! - [`raw_row`] — rawRow/rawBlock and the (TSID, Timestamp) row sort.
//! - [`block_header`] — 81-byte blockHeader codec.
//! - [`block`] — Block marshal/unmarshal on top of esm-encoding + decimal.
//! - [`dedup`] — DeduplicateSamples/needsDedup and the global dedup interval.
//! - [`time_range`] — TimeRange, date math, monthly partition names.

pub mod block;
pub mod block_header;
pub mod dedup;
pub mod metric_name;
pub mod raw_row;
pub(crate) mod snapshot;
pub mod time_range;
pub mod tsid;
pub(crate) mod util;

pub mod block_stream; // stage 2: block_stream_{writer,reader,merger}
pub mod index; // stage 3: per-partition indexDB on esm-mergeset, tag filters, caches
pub mod metaindex_row; // stage 2: metaindex.bin rows
pub mod parallel_search; // stage 5: parallel per-series block unpacking (netstorage port)
pub mod part; // stage 2: part, part_header, inmemory_part, part_search
pub(crate) mod partition; // stage 4: partition, partWrapper, rawRows shards, mergers
pub mod search; // stage 5: Search + metricNameSearch + per-series read API
pub mod storage; // stage 4: Storage, AddRows, caches, bg workers
pub(crate) mod sync_util; // Sema/Shutdown/WaitCounter helpers
pub(crate) mod table; // stage 4: monthly partition set, ptw refcounts
pub(crate) mod table_search; // stage 5: k-way heap over partition searches

pub use block::{Block, MAX_BLOCK_SIZE, MAX_ROWS_PER_BLOCK};
pub use block_header::{BlockHeader, MARSHALED_BLOCK_HEADER_SIZE};
pub use dedup::{
    deduplicate_samples, deduplicate_samples_during_merge, get_dedup_interval, needs_dedup,
    set_dedup_interval,
};
pub use index::{SearchError, TagFilters, NO_DEADLINE};
pub use metric_name::{marshal_metric_name_raw, MetricName, Tag};
pub use parallel_search::SeriesRefs;
pub use raw_row::{sort_raw_rows, RawBlock, RawRow};
pub use search::{Search, SeriesBlock};
pub use storage::{MetricRow, MetricRowRef, OpenOptions, Storage, RETENTION_MAX_MSECS};
pub use time_range::{
    timestamp_to_partition_name, TimeRange, GLOBAL_INDEX_DATE, GLOBAL_INDEX_TIME_RANGE,
    MAX_UNIX_MILLI, MSEC_PER_DAY, MSEC_PER_HOUR,
};
pub use tsid::{merge_sorted_tsids, Tsid, MARSHALED_TSID_SIZE};
