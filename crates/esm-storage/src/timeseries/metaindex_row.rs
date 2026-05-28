//! `MetaindexRow` — one entry per index block in `metaindex.bin`.
//!
//! Format reference: `docs/format/timeseries-part.md` §4.
//! VM source: `lib/storage/metaindex_row.go:14-126`.

use esm_compress::int::{
    DecodeError, marshal_uint32, marshal_uint64, unmarshal_uint32, unmarshal_uint64,
};
use thiserror::Error;

use super::{MAX_BLOCK_SIZE, Tsid};

/// One metaindex row. Each row covers a single index block.
/// `metaindex.bin` is a zstd-compressed concatenation of these rows, sorted
/// by `tsid`.
#[derive(Debug, Clone, Default)]
pub struct MetaindexRow {
    /// First TSID in the index block.
    pub tsid: Tsid,
    /// Minimum timestamp across all blocks in this index block.
    pub min_timestamp: i64,
    /// Maximum timestamp across all blocks in this index block.
    pub max_timestamp: i64,
    /// Byte offset into `index.bin` where this index block starts.
    pub index_block_offset: u64,
    /// Number of block headers in this index block. Must be > 0.
    pub block_headers_count: u32,
    /// Zstd-compressed byte length of the index block. Must be ≤
    /// `2 * MAX_BLOCK_SIZE`.
    pub index_block_size: u32,
}

impl MetaindexRow {
    /// Append the on-disk byte representation to `dst`. Field order matches
    /// VM's `Marshal` (`lib/storage/metaindex_row.go:64-72`):
    /// TSID(24) || BE-u32(block_headers_count) || BE-i64(min_ts) ||
    /// BE-i64(max_ts) || BE-u64(index_block_offset) ||
    /// BE-u32(index_block_size).
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        self.tsid.marshal(dst);
        marshal_uint32(dst, self.block_headers_count);
        marshal_int64(dst, self.min_timestamp);
        marshal_int64(dst, self.max_timestamp);
        marshal_uint64(dst, self.index_block_offset);
        marshal_uint32(dst, self.index_block_size);
    }

    /// Parse one row. Returns the row plus the unconsumed remainder.
    ///
    /// # Errors
    /// See [`MetaindexRowError`].
    pub fn unmarshal(src: &[u8]) -> Result<(Self, &[u8]), MetaindexRowError> {
        let (tsid, src) = Tsid::unmarshal(src).map_err(MetaindexRowError::Tsid)?;
        let (block_headers_count, n) = unmarshal_uint32(src).map_err(MetaindexRowError::Field)?;
        let src = &src[n..];
        let (min_timestamp, n) = unmarshal_int64(src)?;
        let src = &src[n..];
        let (max_timestamp, n) = unmarshal_int64(src)?;
        let src = &src[n..];
        let (index_block_offset, n) = unmarshal_uint64(src).map_err(MetaindexRowError::Field)?;
        let src = &src[n..];
        let (index_block_size, n) = unmarshal_uint32(src).map_err(MetaindexRowError::Field)?;
        let src = &src[n..];

        if block_headers_count == 0 {
            return Err(MetaindexRowError::ZeroBlockHeadersCount);
        }
        if index_block_size > 2 * MAX_BLOCK_SIZE {
            return Err(MetaindexRowError::IndexBlockTooLarge(index_block_size));
        }
        Ok((
            Self {
                tsid,
                min_timestamp,
                max_timestamp,
                index_block_offset,
                block_headers_count,
                index_block_size,
            },
            src,
        ))
    }

    /// Update accumulating min/max + block count given a new block header.
    /// Mirrors VM's `RegisterBlockHeader` (`metaindex_row.go:46-61`).
    pub fn register_block_header(&mut self, bh: &super::BlockHeader) {
        self.block_headers_count += 1;
        if self.block_headers_count == 1 {
            self.tsid = bh.tsid;
            self.min_timestamp = bh.min_timestamp;
            self.max_timestamp = bh.max_timestamp;
            return;
        }
        if bh.min_timestamp < self.min_timestamp {
            self.min_timestamp = bh.min_timestamp;
        }
        if bh.max_timestamp > self.max_timestamp {
            self.max_timestamp = bh.max_timestamp;
        }
    }

    /// Reset to VM's start-of-row state: zero TSID, +inf min, -inf max,
    /// zero counters.
    pub fn reset(&mut self) {
        self.tsid = Tsid::default();
        self.block_headers_count = 0;
        self.min_timestamp = i64::MAX;
        self.max_timestamp = i64::MIN;
        self.index_block_offset = 0;
        self.index_block_size = 0;
    }
}

/// Decode a concatenated sequence of metaindex rows. Mirrors VM's
/// `unmarshalMetaindexRows` (`metaindex_row.go:129-165`) minus the zstd
/// decompression step — callers pass already-decompressed bytes.
///
/// # Errors
/// Returns [`UnmarshalMetaindexError`] on parse, exhaustion, or sort failure.
pub fn unmarshal_metaindex_rows(
    dst: &mut Vec<MetaindexRow>,
    mut src: &[u8],
) -> Result<(), UnmarshalMetaindexError> {
    let dst_start = dst.len();
    while !src.is_empty() {
        let (row, rest) = MetaindexRow::unmarshal(src)
            .map_err(|e| UnmarshalMetaindexError::Row { i: dst.len() - dst_start, source: e })?;
        dst.push(row);
        src = rest;
    }
    if dst.len() == dst_start {
        return Err(UnmarshalMetaindexError::EmptyInput);
    }
    for w in dst[dst_start..].windows(2) {
        if w[0].tsid > w[1].tsid {
            return Err(UnmarshalMetaindexError::Unsorted);
        }
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum MetaindexRowError {
    #[error("cannot unmarshal TSID: {0}")]
    Tsid(DecodeError),
    #[error("cannot unmarshal field: {0}")]
    Field(DecodeError),
    #[error("cannot unmarshal int64 field: truncated (need {needed}, have {have})")]
    Int64Truncated { needed: usize, have: usize },
    #[error("block_headers_count must be > 0")]
    ZeroBlockHeadersCount,
    #[error("index_block_size={0} exceeds 2 * MAX_BLOCK_SIZE")]
    IndexBlockTooLarge(u32),
}

#[derive(Debug, Error)]
pub enum UnmarshalMetaindexError {
    #[error("metaindex must contain at least one row")]
    EmptyInput,
    #[error("cannot unmarshal metaindex row #{i}: {source}")]
    Row {
        i: usize,
        #[source]
        source: MetaindexRowError,
    },
    #[error("metaindex rows are not sorted by tsid")]
    Unsorted,
}

fn marshal_int64(dst: &mut Vec<u8>, v: i64) {
    dst.extend_from_slice(&v.to_be_bytes());
}

fn unmarshal_int64(src: &[u8]) -> Result<(i64, usize), MetaindexRowError> {
    if src.len() < 8 {
        return Err(MetaindexRowError::Int64Truncated { needed: 8, have: src.len() });
    }
    let v = i64::from_be_bytes([src[0], src[1], src[2], src[3], src[4], src[5], src[6], src[7]]);
    Ok((v, 8))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> MetaindexRow {
        MetaindexRow {
            tsid: Tsid { metric_group_id: 1, job_id: 2, instance_id: 3, metric_id: 4 },
            block_headers_count: 10,
            min_timestamp: 100_000,
            max_timestamp: 200_000,
            index_block_offset: 4096,
            index_block_size: 1024,
        }
    }

    #[test]
    fn roundtrip() {
        let row = sample();
        let mut buf = Vec::new();
        row.marshal(&mut buf);
        let (decoded, rest) = MetaindexRow::unmarshal(&buf).unwrap();
        assert!(rest.is_empty());
        assert_eq!(decoded.tsid, row.tsid);
        assert_eq!(decoded.block_headers_count, row.block_headers_count);
        assert_eq!(decoded.min_timestamp, row.min_timestamp);
        assert_eq!(decoded.max_timestamp, row.max_timestamp);
        assert_eq!(decoded.index_block_offset, row.index_block_offset);
        assert_eq!(decoded.index_block_size, row.index_block_size);
    }

    #[test]
    fn zero_block_headers_count_rejected() {
        let mut row = sample();
        row.block_headers_count = 0;
        let mut buf = Vec::new();
        row.marshal(&mut buf);
        assert!(matches!(
            MetaindexRow::unmarshal(&buf),
            Err(MetaindexRowError::ZeroBlockHeadersCount)
        ));
    }

    #[test]
    fn index_block_too_large_rejected() {
        let mut row = sample();
        row.index_block_size = 2 * MAX_BLOCK_SIZE + 1;
        let mut buf = Vec::new();
        row.marshal(&mut buf);
        assert!(matches!(
            MetaindexRow::unmarshal(&buf),
            Err(MetaindexRowError::IndexBlockTooLarge(_))
        ));
    }

    #[test]
    fn register_block_header_tracks_min_max() {
        let mut mr = MetaindexRow::default();
        mr.reset();
        let bh1 = super::super::BlockHeader {
            tsid: Tsid { metric_id: 100, ..Default::default() },
            min_timestamp: 200,
            max_timestamp: 400,
            rows_count: 5,
            ..Default::default()
        };
        let bh2 =
            super::super::BlockHeader { min_timestamp: 50, max_timestamp: 600, ..bh1.clone() };

        mr.register_block_header(&bh1);
        assert_eq!(mr.block_headers_count, 1);
        assert_eq!(mr.tsid, bh1.tsid);
        assert_eq!(mr.min_timestamp, 200);
        assert_eq!(mr.max_timestamp, 400);

        mr.register_block_header(&bh2);
        assert_eq!(mr.block_headers_count, 2);
        assert_eq!(mr.min_timestamp, 50);
        assert_eq!(mr.max_timestamp, 600);
    }

    #[test]
    fn rows_roundtrip_with_sort_check() {
        let mut a = sample();
        a.tsid = Tsid { metric_id: 1, ..Default::default() };
        let mut b = sample();
        b.tsid = Tsid { metric_id: 2, ..Default::default() };
        let mut c = sample();
        c.tsid = Tsid { metric_id: 3, ..Default::default() };

        let mut buf = Vec::new();
        a.marshal(&mut buf);
        b.marshal(&mut buf);
        c.marshal(&mut buf);

        let mut rows = Vec::new();
        unmarshal_metaindex_rows(&mut rows, &buf).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].tsid.metric_id, 1);
        assert_eq!(rows[2].tsid.metric_id, 3);
    }

    #[test]
    fn rows_reject_unsorted() {
        let mut a = sample();
        a.tsid = Tsid { metric_id: 99, ..Default::default() };
        let mut b = sample();
        b.tsid = Tsid { metric_id: 1, ..Default::default() };

        let mut buf = Vec::new();
        a.marshal(&mut buf);
        b.marshal(&mut buf);

        let mut rows = Vec::new();
        assert!(matches!(
            unmarshal_metaindex_rows(&mut rows, &buf),
            Err(UnmarshalMetaindexError::Unsorted)
        ));
    }
}
