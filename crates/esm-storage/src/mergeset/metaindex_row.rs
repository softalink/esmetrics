//! `MetaindexRow` — one entry per index block inside `metaindex.bin`.
//!
//! Format reference: `docs/format/mergeset-part.md` §3.
//! VM source: `lib/mergeset/metaindex_row.go:12-25,34-83`.

use esm_compress::int::{
    DecodeError, marshal_bytes, marshal_uint32, marshal_uint64, unmarshal_bytes, unmarshal_uint32,
    unmarshal_uint64,
};
use thiserror::Error;

/// Describes one index block. `metaindex.bin` is a zstd-compressed
/// concatenation of `MetaindexRow` rows sorted by `first_item`.
#[derive(Debug, Default, Clone)]
pub struct MetaindexRow {
    /// First item in the first block-header of this index block. Used for
    /// fast lookup of the relevant index block.
    pub first_item: Vec<u8>,
    /// Number of `BlockHeader` rows contained in this index block. Must be > 0.
    pub block_headers_count: u32,
    /// Byte offset into `index.bin` where this index block starts.
    pub index_block_offset: u64,
    /// Byte length of the index block in `index.bin` (zstd-compressed bytes).
    pub index_block_size: u32,
}

impl MetaindexRow {
    /// Validation bound applied by VM's reader (`lib/mergeset/metaindex_row.go:75-79`).
    ///
    /// The index block size can exceed [`super::MAX_INDEX_BLOCK_SIZE`] by up
    /// to 4× because each `BlockHeader` can contain a `common_prefix` and a
    /// `first_item` of roughly that size. = `4 * MAX_INDEX_BLOCK_SIZE`
    /// (= 64 KiB).
    pub const MAX_INDEX_BLOCK_SIZE: u32 = 4 * 64 * 1024;

    /// Append the on-disk byte representation of this row to `dst`.
    /// Field order: `Bytes(first_item) || BE-u32(block_headers_count) ||
    /// BE-u64(index_block_offset) || BE-u32(index_block_size)`.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        marshal_bytes(dst, &self.first_item);
        marshal_uint32(dst, self.block_headers_count);
        marshal_uint64(dst, self.index_block_offset);
        marshal_uint32(dst, self.index_block_size);
    }

    /// Parse one row from the head of `src`. Returns the row and the
    /// unconsumed remainder.
    ///
    /// # Errors
    /// See [`MetaindexRowError`].
    pub fn unmarshal(src: &[u8]) -> Result<(Self, &[u8]), MetaindexRowError> {
        let (first_item, n) = unmarshal_bytes(src).map_err(MetaindexRowError::FirstItem)?;
        let first_item = first_item.to_vec();
        let src = &src[n..];

        let (block_headers_count, n) =
            unmarshal_uint32(src).map_err(MetaindexRowError::BlockHeadersCount)?;
        let src = &src[n..];

        let (index_block_offset, n) =
            unmarshal_uint64(src).map_err(MetaindexRowError::IndexBlockOffset)?;
        let src = &src[n..];

        let (index_block_size, n) =
            unmarshal_uint32(src).map_err(MetaindexRowError::IndexBlockSize)?;
        let src = &src[n..];

        if block_headers_count == 0 {
            return Err(MetaindexRowError::ZeroBlockHeadersCount);
        }
        if index_block_size > Self::MAX_INDEX_BLOCK_SIZE {
            return Err(MetaindexRowError::IndexBlockTooLarge(index_block_size));
        }

        Ok((Self { first_item, block_headers_count, index_block_offset, index_block_size }, src))
    }
}

/// Errors returned by [`MetaindexRow::unmarshal`].
#[derive(Debug, Error)]
pub enum MetaindexRowError {
    #[error("cannot unmarshal first_item: {0}")]
    FirstItem(DecodeError),
    #[error("cannot unmarshal block_headers_count: {0}")]
    BlockHeadersCount(DecodeError),
    #[error("cannot unmarshal index_block_offset: {0}")]
    IndexBlockOffset(DecodeError),
    #[error("cannot unmarshal index_block_size: {0}")]
    IndexBlockSize(DecodeError),
    #[error("block_headers_count must be > 0")]
    ZeroBlockHeadersCount,
    #[error("index_block_size={0} exceeds MAX_INDEX_BLOCK_SIZE")]
    IndexBlockTooLarge(u32),
}

/// Decode a concatenated sequence of `MetaindexRow` entries into `dst`,
/// verifying they are sorted by `first_item` and that `src` is fully
/// consumed. Mirrors VM's `unmarshalMetaindexRows`
/// (`lib/mergeset/metaindex_row.go:85-125`), minus the upstream zstd
/// decompression — callers pass the already-decompressed payload.
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
    for window in dst[dst_start..].windows(2) {
        if window[0].first_item > window[1].first_item {
            return Err(UnmarshalMetaindexError::Unsorted);
        }
    }
    Ok(())
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
    #[error("metaindex rows are not sorted by first_item")]
    Unsorted,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> MetaindexRow {
        MetaindexRow {
            first_item: vec![1, 2, 3, 4],
            block_headers_count: 10,
            index_block_offset: 0x100,
            index_block_size: 0x80,
        }
    }

    #[test]
    fn marshal_unmarshal_roundtrip() {
        let row = sample();
        let mut buf = Vec::new();
        row.marshal(&mut buf);

        let (decoded, rest) = MetaindexRow::unmarshal(&buf).unwrap();
        assert!(rest.is_empty());
        assert_eq!(decoded.first_item, row.first_item);
        assert_eq!(decoded.block_headers_count, row.block_headers_count);
        assert_eq!(decoded.index_block_offset, row.index_block_offset);
        assert_eq!(decoded.index_block_size, row.index_block_size);
    }

    #[test]
    fn marshal_layout_matches_spec() {
        let row = MetaindexRow {
            first_item: vec![],
            block_headers_count: 1,
            index_block_offset: 0,
            index_block_size: 1,
        };
        let mut buf = Vec::new();
        row.marshal(&mut buf);
        let expected = [
            0x00, // varuint(0) empty first_item
            0x00, 0x00, 0x00, 0x01, // block_headers_count = 1 BE
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // index_block_offset BE u64
            0x00, 0x00, 0x00, 0x01, // index_block_size BE u32
        ];
        assert_eq!(buf, expected);
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
        row.index_block_size = MetaindexRow::MAX_INDEX_BLOCK_SIZE + 1;
        let mut buf = Vec::new();
        row.marshal(&mut buf);
        assert!(matches!(
            MetaindexRow::unmarshal(&buf),
            Err(MetaindexRowError::IndexBlockTooLarge(_))
        ));
    }

    #[test]
    fn rows_roundtrip_with_sort_check() {
        let mut a = sample();
        a.first_item = vec![1];
        let mut b = sample();
        b.first_item = vec![2];
        let mut c = sample();
        c.first_item = vec![3];

        let mut buf = Vec::new();
        a.marshal(&mut buf);
        b.marshal(&mut buf);
        c.marshal(&mut buf);

        let mut rows = Vec::new();
        unmarshal_metaindex_rows(&mut rows, &buf).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].first_item, vec![1]);
        assert_eq!(rows[2].first_item, vec![3]);
    }

    #[test]
    fn rows_reject_unsorted() {
        let mut a = sample();
        a.first_item = vec![9];
        let mut b = sample();
        b.first_item = vec![1];

        let mut buf = Vec::new();
        a.marshal(&mut buf);
        b.marshal(&mut buf);

        let mut rows = Vec::new();
        assert!(matches!(
            unmarshal_metaindex_rows(&mut rows, &buf),
            Err(UnmarshalMetaindexError::Unsorted)
        ));
    }

    #[test]
    fn rows_reject_empty_input() {
        let mut rows = Vec::new();
        assert!(matches!(
            unmarshal_metaindex_rows(&mut rows, &[]),
            Err(UnmarshalMetaindexError::EmptyInput)
        ));
    }
}
