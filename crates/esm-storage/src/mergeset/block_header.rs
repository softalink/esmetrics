//! `BlockHeader` — one entry per data block inside an index block.
//!
//! Format reference: `docs/format/mergeset-part.md` §4.
//! VM source: `lib/mergeset/block_header.go:13-40,62-72,77-151`.

use esm_compress::int::{
    DecodeError, marshal_bytes, marshal_uint32, marshal_uint64, unmarshal_bytes, unmarshal_uint32,
    unmarshal_uint64,
};
use thiserror::Error;

use super::{MarshalType, marshal_type::MarshalTypeError};

/// Describes one data block. Index blocks are concatenations of `BlockHeader`
/// rows sorted by `first_item`.
///
/// Owned variant — `first_item` and `common_prefix` are heap-allocated.
/// A future `BlockHeaderRef<'a>` that borrows into a decompressed index block
/// will land alongside the reader implementation.
#[derive(Debug, Default, Clone)]
pub struct BlockHeader {
    /// Common byte prefix of every item in the block. May be empty.
    pub common_prefix: Vec<u8>,
    /// First item in the block (full bytes — `common_prefix` is *not* stripped).
    pub first_item: Vec<u8>,
    /// Per-block compression discriminator.
    pub marshal_type: MarshalType,
    /// Total items in the block, including `first_item`. Must be > 0.
    pub items_count: u32,
    /// Byte offset into `items.bin` where this block's items chunk starts.
    pub items_block_offset: u64,
    /// Byte offset into `lens.bin` where this block's lens chunk starts.
    pub lens_block_offset: u64,
    /// Byte length of the items chunk. Max `2 * MAX_INMEMORY_BLOCK_SIZE`.
    pub items_block_size: u32,
    /// Byte length of the lens chunk. Max `16 * MAX_INMEMORY_BLOCK_SIZE`.
    pub lens_block_size: u32,
}

impl BlockHeader {
    /// Validation bounds applied by VM's reader
    /// (`lib/mergeset/block_header.go:140-148`).
    /// = `2 * MAX_INMEMORY_BLOCK_SIZE` (= 64 KiB), computed in u32 to avoid
    /// `usize -> u32` cast warnings on 64-bit targets.
    pub const MAX_ITEMS_BLOCK_SIZE: u32 = 2 * 64 * 1024;
    /// = `16 * MAX_INMEMORY_BLOCK_SIZE` (= 64 KiB).
    pub const MAX_LENS_BLOCK_SIZE: u32 = 16 * 64 * 1024;

    /// Append the on-disk byte representation of this header to `dst`.
    ///
    /// Field order mirrors VM's `Marshal` (`lib/mergeset/block_header.go:62-72`):
    /// `Bytes(common_prefix) || Bytes(first_item) || u8(marshal_type) ||
    ///  BE-u32(items_count) || BE-u64(items_block_offset) || BE-u64(lens_block_offset) ||
    ///  BE-u32(items_block_size) || BE-u32(lens_block_size)`.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        marshal_bytes(dst, &self.common_prefix);
        marshal_bytes(dst, &self.first_item);
        dst.push(self.marshal_type.as_byte());
        marshal_uint32(dst, self.items_count);
        marshal_uint64(dst, self.items_block_offset);
        marshal_uint64(dst, self.lens_block_offset);
        marshal_uint32(dst, self.items_block_size);
        marshal_uint32(dst, self.lens_block_size);
    }

    /// Parse one header from the head of `src`. Returns the header and the
    /// unconsumed remainder of `src`.
    ///
    /// Field order mirrors VM's `UnmarshalNoCopy`
    /// (`lib/mergeset/block_header.go:77-151`), with VM's validation rules
    /// applied. Unlike VM, this implementation makes owned copies of the
    /// byte fields; a zero-copy `unmarshal_ref` lands when the reader needs
    /// it.
    ///
    /// # Errors
    /// See [`BlockHeaderError`] for the validation rules.
    pub fn unmarshal(src: &[u8]) -> Result<(Self, &[u8]), BlockHeaderError> {
        let (common_prefix, n) = unmarshal_bytes(src).map_err(BlockHeaderError::CommonPrefix)?;
        let common_prefix = common_prefix.to_vec();
        let src = &src[n..];

        let (first_item, n) = unmarshal_bytes(src).map_err(BlockHeaderError::FirstItem)?;
        let first_item = first_item.to_vec();
        let src = &src[n..];

        let (mt_byte, rest) = src.split_first().ok_or(BlockHeaderError::TruncatedMarshalType)?;
        let marshal_type = MarshalType::from_byte(*mt_byte)?;
        let src = rest;

        let (items_count, n) = unmarshal_uint32(src).map_err(BlockHeaderError::ItemsCount)?;
        let src = &src[n..];

        let (items_block_offset, n) =
            unmarshal_uint64(src).map_err(BlockHeaderError::ItemsBlockOffset)?;
        let src = &src[n..];

        let (lens_block_offset, n) =
            unmarshal_uint64(src).map_err(BlockHeaderError::LensBlockOffset)?;
        let src = &src[n..];

        let (items_block_size, n) =
            unmarshal_uint32(src).map_err(BlockHeaderError::ItemsBlockSize)?;
        let src = &src[n..];

        let (lens_block_size, n) =
            unmarshal_uint32(src).map_err(BlockHeaderError::LensBlockSize)?;
        let src = &src[n..];

        if items_count == 0 {
            return Err(BlockHeaderError::ZeroItemsCount);
        }
        if items_block_size > Self::MAX_ITEMS_BLOCK_SIZE {
            return Err(BlockHeaderError::ItemsBlockTooLarge(items_block_size));
        }
        if lens_block_size > Self::MAX_LENS_BLOCK_SIZE {
            return Err(BlockHeaderError::LensBlockTooLarge(lens_block_size));
        }

        Ok((
            Self {
                common_prefix,
                first_item,
                marshal_type,
                items_count,
                items_block_offset,
                lens_block_offset,
                items_block_size,
                lens_block_size,
            },
            src,
        ))
    }
}

/// Errors returned by [`BlockHeader::unmarshal`].
#[derive(Debug, Error)]
pub enum BlockHeaderError {
    #[error("cannot unmarshal common_prefix: {0}")]
    CommonPrefix(DecodeError),
    #[error("cannot unmarshal first_item: {0}")]
    FirstItem(DecodeError),
    #[error("missing marshal_type byte")]
    TruncatedMarshalType,
    #[error(transparent)]
    MarshalType(#[from] MarshalTypeError),
    #[error("cannot unmarshal items_count: {0}")]
    ItemsCount(DecodeError),
    #[error("cannot unmarshal items_block_offset: {0}")]
    ItemsBlockOffset(DecodeError),
    #[error("cannot unmarshal lens_block_offset: {0}")]
    LensBlockOffset(DecodeError),
    #[error("cannot unmarshal items_block_size: {0}")]
    ItemsBlockSize(DecodeError),
    #[error("cannot unmarshal lens_block_size: {0}")]
    LensBlockSize(DecodeError),
    #[error("items_count must be > 0")]
    ZeroItemsCount,
    #[error("items_block_size={0} exceeds MAX_ITEMS_BLOCK_SIZE")]
    ItemsBlockTooLarge(u32),
    #[error("lens_block_size={0} exceeds MAX_LENS_BLOCK_SIZE")]
    LensBlockTooLarge(u32),
}

/// Decode a concatenated sequence of `BlockHeader` entries (one *index
/// block*) into `dst`. Verifies that they are sorted by `first_item` and that
/// `src` is fully consumed. Mirrors VM's `unmarshalBlockHeadersNoCopy`
/// (`lib/mergeset/block_header.go:159-183`).
///
/// # Errors
/// Returns [`UnmarshalHeadersError`] if any header fails to parse, the input
/// is not fully consumed, or the headers are not sorted by `first_item`.
pub fn unmarshal_block_headers(
    dst: &mut Vec<BlockHeader>,
    mut src: &[u8],
    count: usize,
) -> Result<(), UnmarshalHeadersError> {
    if count == 0 {
        return Err(UnmarshalHeadersError::ZeroCount);
    }
    let dst_start = dst.len();
    for i in 0..count {
        let (bh, rest) = BlockHeader::unmarshal(src)
            .map_err(|e| UnmarshalHeadersError::Header { i, source: e })?;
        dst.push(bh);
        src = rest;
    }
    if !src.is_empty() {
        return Err(UnmarshalHeadersError::TrailingBytes(src.len()));
    }
    for window in dst[dst_start..].windows(2) {
        if window[0].first_item > window[1].first_item {
            return Err(UnmarshalHeadersError::Unsorted);
        }
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum UnmarshalHeadersError {
    #[error("count must be > 0")]
    ZeroCount,
    #[error("cannot unmarshal block header #{i}: {source}")]
    Header {
        i: usize,
        #[source]
        source: BlockHeaderError,
    },
    #[error("{0} unexpected trailing bytes after unmarshaling block headers")]
    TrailingBytes(usize),
    #[error("block headers are not sorted by first_item")]
    Unsorted,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> BlockHeader {
        BlockHeader {
            common_prefix: vec![0xaa, 0xbb],
            first_item: vec![0xaa, 0xbb, 0x01, 0x02, 0x03],
            marshal_type: MarshalType::Zstd,
            items_count: 42,
            items_block_offset: 0x1000,
            lens_block_offset: 0x2000,
            items_block_size: 1024,
            lens_block_size: 64,
        }
    }

    #[test]
    fn marshal_unmarshal_roundtrip() {
        let bh = sample();
        let mut buf = Vec::new();
        bh.marshal(&mut buf);

        let (decoded, rest) = BlockHeader::unmarshal(&buf).unwrap();
        assert!(rest.is_empty());
        assert_eq!(decoded.common_prefix, bh.common_prefix);
        assert_eq!(decoded.first_item, bh.first_item);
        assert_eq!(decoded.marshal_type, bh.marshal_type);
        assert_eq!(decoded.items_count, bh.items_count);
        assert_eq!(decoded.items_block_offset, bh.items_block_offset);
        assert_eq!(decoded.lens_block_offset, bh.lens_block_offset);
        assert_eq!(decoded.items_block_size, bh.items_block_size);
        assert_eq!(decoded.lens_block_size, bh.lens_block_size);
    }

    #[test]
    fn marshal_layout_matches_spec() {
        // Verify field order on the wire for a sample we can hand-compute.
        let bh = BlockHeader {
            common_prefix: vec![],
            first_item: vec![0xaa],
            marshal_type: MarshalType::Plain,
            items_count: 1,
            items_block_offset: 0,
            lens_block_offset: 0,
            items_block_size: 0,
            lens_block_size: 0,
        };
        let mut buf = Vec::new();
        bh.marshal(&mut buf);
        let expected = [
            0x00, // varuint(0) common_prefix
            0x01, 0xaa, // varuint(1) + 0xaa first_item
            0x00, // marshal_type = plain
            0x00, 0x00, 0x00, 0x01, // items_count = 1 BE
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // items_block_offset BE u64
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // lens_block_offset BE u64
            0x00, 0x00, 0x00, 0x00, // items_block_size BE u32
            0x00, 0x00, 0x00, 0x00, // lens_block_size BE u32
        ];
        assert_eq!(buf, expected);
    }

    #[test]
    fn zero_items_count_rejected() {
        let mut bh = sample();
        bh.items_count = 0;
        let mut buf = Vec::new();
        bh.marshal(&mut buf);
        assert!(matches!(BlockHeader::unmarshal(&buf), Err(BlockHeaderError::ZeroItemsCount)));
    }

    #[test]
    fn items_block_size_too_large_rejected() {
        let mut bh = sample();
        bh.items_block_size = BlockHeader::MAX_ITEMS_BLOCK_SIZE + 1;
        let mut buf = Vec::new();
        bh.marshal(&mut buf);
        assert!(matches!(
            BlockHeader::unmarshal(&buf),
            Err(BlockHeaderError::ItemsBlockTooLarge(_))
        ));
    }

    #[test]
    fn unmarshal_block_headers_roundtrips_in_sort_order() {
        let mut a = sample();
        a.first_item = vec![1, 2, 3];
        let mut b = sample();
        b.first_item = vec![4, 5, 6];

        let mut buf = Vec::new();
        a.marshal(&mut buf);
        b.marshal(&mut buf);

        let mut headers = Vec::new();
        unmarshal_block_headers(&mut headers, &buf, 2).unwrap();
        assert_eq!(headers.len(), 2);
        assert_eq!(headers[0].first_item, vec![1, 2, 3]);
        assert_eq!(headers[1].first_item, vec![4, 5, 6]);
    }

    #[test]
    fn unmarshal_block_headers_rejects_unsorted() {
        let mut a = sample();
        a.first_item = vec![5, 5, 5];
        let mut b = sample();
        b.first_item = vec![1, 1, 1];

        let mut buf = Vec::new();
        a.marshal(&mut buf);
        b.marshal(&mut buf);

        let mut headers = Vec::new();
        assert!(matches!(
            unmarshal_block_headers(&mut headers, &buf, 2),
            Err(UnmarshalHeadersError::Unsorted)
        ));
    }
}
