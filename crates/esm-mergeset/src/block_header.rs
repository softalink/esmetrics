//! Port of `block_header.go`.

use esm_encoding::{
    marshal_bytes, marshal_uint32, marshal_uint64, unmarshal_bytes, unmarshal_uint32,
    unmarshal_uint64,
};

use crate::inmemory_block::{MarshalType, MAX_INMEMORY_BLOCK_SIZE};

/// Header of a single data block; lives in `index.bin`.
///
/// Note: `items_count` INCLUDES the first item (the Go comment saying
/// "excluding the first item" is wrong).
#[derive(Debug, Default, Clone)]
pub(crate) struct BlockHeader {
    /// Common prefix for all the items in the block.
    pub common_prefix: Vec<u8>,
    /// The first item.
    pub first_item: Vec<u8>,
    /// Marshal type used for block compression.
    pub marshal_type: MarshalType,
    /// The number of items in the block, including the first item.
    pub items_count: u32,
    /// The offset of the items block.
    pub items_block_offset: u64,
    /// The offset of the lens block.
    pub lens_block_offset: u64,
    /// The size of the items block.
    pub items_block_size: u32,
    /// The size of the lens block.
    pub lens_block_size: u32,
}

impl BlockHeader {
    /// The approximate in-memory size of the header (Go `blockHeader.SizeBytes`).
    pub fn size_bytes(&self) -> usize {
        std::mem::size_of::<BlockHeader>()
            + self.common_prefix.capacity()
            + self.first_item.capacity()
    }

    pub fn reset(&mut self) {
        self.common_prefix.clear();
        self.first_item.clear();
        self.marshal_type = MarshalType::Plain;
        self.items_count = 0;
        self.items_block_offset = 0;
        self.lens_block_offset = 0;
        self.items_block_size = 0;
        self.lens_block_size = 0;
    }

    pub fn marshal(&self, dst: &mut Vec<u8>) {
        marshal_bytes(dst, &self.common_prefix);
        marshal_bytes(dst, &self.first_item);
        dst.push(self.marshal_type.as_u8());
        marshal_uint32(dst, self.items_count);
        marshal_uint64(dst, self.items_block_offset);
        marshal_uint64(dst, self.lens_block_offset);
        marshal_uint32(dst, self.items_block_size);
        marshal_uint32(dst, self.lens_block_size);
    }

    /// Unmarshals the header from `src`, returning the remaining tail.
    ///
    /// Deviation from Go's `UnmarshalNoCopy`: `common_prefix` and
    /// `first_item` are copied into owned buffers.
    pub fn unmarshal<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        // Unmarshal commonPrefix.
        let (cp, n_size) = unmarshal_bytes(src).ok_or("cannot unmarshal commonPrefix")?;
        self.common_prefix.clear();
        self.common_prefix.extend_from_slice(cp);
        let src = &src[n_size..];

        // Unmarshal firstItem.
        let (fi, n_size) = unmarshal_bytes(src).ok_or("cannot unmarshal firstItem")?;
        self.first_item.clear();
        self.first_item.extend_from_slice(fi);
        let src = &src[n_size..];

        // Unmarshal marshalType.
        if src.is_empty() {
            return Err("cannot unmarshal marshalType from zero bytes".to_string());
        }
        self.marshal_type =
            MarshalType::from_u8(src[0]).map_err(|e| format!("unexpected marshalType: {e}"))?;
        let src = &src[1..];

        // Unmarshal itemsCount.
        if src.len() < 4 {
            return Err(format!(
                "cannot unmarshal itemsCount from {} bytes; need at least 4 bytes",
                src.len()
            ));
        }
        self.items_count = unmarshal_uint32(src);
        let src = &src[4..];

        // Unmarshal itemsBlockOffset.
        if src.len() < 8 {
            return Err(format!(
                "cannot unmarshal itemsBlockOffset from {} bytes; need at least 8 bytes",
                src.len()
            ));
        }
        self.items_block_offset = unmarshal_uint64(src);
        let src = &src[8..];

        // Unmarshal lensBlockOffset.
        if src.len() < 8 {
            return Err(format!(
                "cannot unmarshal lensBlockOffset from {} bytes; need at least 8 bytes",
                src.len()
            ));
        }
        self.lens_block_offset = unmarshal_uint64(src);
        let src = &src[8..];

        // Unmarshal itemsBlockSize.
        if src.len() < 4 {
            return Err(format!(
                "cannot unmarshal itemsBlockSize from {} bytes; need at least 4 bytes",
                src.len()
            ));
        }
        self.items_block_size = unmarshal_uint32(src);
        let src = &src[4..];

        // Unmarshal lensBlockSize.
        if src.len() < 4 {
            return Err(format!(
                "cannot unmarshal lensBlockSize from {} bytes; need at least 4 bytes",
                src.len()
            ));
        }
        self.lens_block_size = unmarshal_uint32(src);
        let src = &src[4..];

        if self.items_count == 0 {
            return Err("itemsCount must be bigger than 0; got 0".to_string());
        }
        if self.items_block_size as usize > 2 * MAX_INMEMORY_BLOCK_SIZE {
            return Err(format!(
                "too big itemsBlockSize; got {}; cannot exceed {}",
                self.items_block_size,
                2 * MAX_INMEMORY_BLOCK_SIZE
            ));
        }
        if self.lens_block_size as usize > 2 * 8 * MAX_INMEMORY_BLOCK_SIZE {
            return Err(format!(
                "too big lensBlockSize; got {}; cannot exceed {}",
                self.lens_block_size,
                2 * 8 * MAX_INMEMORY_BLOCK_SIZE
            ));
        }

        Ok(src)
    }
}

/// Unmarshals `block_headers_count` block headers from `src` and appends them
/// to `dst`.
///
/// Block headers must be sorted by `first_item`.
pub(crate) fn unmarshal_block_headers(
    dst: &mut Vec<BlockHeader>,
    src: &[u8],
    block_headers_count: usize,
) -> Result<(), String> {
    assert!(
        block_headers_count > 0,
        "BUG: blockHeadersCount must be greater than 0; got {block_headers_count}"
    );
    let dst_len = dst.len();
    let mut src = src;
    for i in 0..block_headers_count {
        let mut bh = BlockHeader::default();
        src = bh.unmarshal(src).map_err(|e| {
            format!("cannot unmarshal block header #{i} out of {block_headers_count}: {e}")
        })?;
        dst.push(bh);
    }
    if !src.is_empty() {
        return Err(format!(
            "unexpected non-zero tail left after unmarshaling {block_headers_count} block headers; len(tail)={}",
            src.len()
        ));
    }

    // Verify that block headers are sorted by firstItem.
    let new_bhs = &dst[dst_len..];
    if !new_bhs
        .windows(2)
        .all(|w| w[0].first_item <= w[1].first_item)
    {
        return Err(
            "block headers must be sorted by firstItem; unmarshaled unsorted block headers"
                .to_string(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marshal_unmarshal_roundtrip() {
        let bh = BlockHeader {
            common_prefix: b"pre".to_vec(),
            first_item: b"prefix-item".to_vec(),
            marshal_type: MarshalType::Zstd,
            items_count: 42,
            items_block_offset: 12345,
            lens_block_offset: 6789,
            items_block_size: 111,
            lens_block_size: 222,
        };
        let mut buf = Vec::new();
        bh.marshal(&mut buf);

        let mut bh2 = BlockHeader::default();
        let tail = bh2.unmarshal(&buf).unwrap();
        assert!(tail.is_empty());
        assert_eq!(bh2.common_prefix, bh.common_prefix);
        assert_eq!(bh2.first_item, bh.first_item);
        assert_eq!(bh2.marshal_type, bh.marshal_type);
        assert_eq!(bh2.items_count, bh.items_count);
        assert_eq!(bh2.items_block_offset, bh.items_block_offset);
        assert_eq!(bh2.lens_block_offset, bh.lens_block_offset);
        assert_eq!(bh2.items_block_size, bh.items_block_size);
        assert_eq!(bh2.lens_block_size, bh.lens_block_size);
    }

    #[test]
    fn unmarshal_rejects_zero_items() {
        let bh = BlockHeader {
            first_item: b"x".to_vec(),
            items_count: 0,
            ..Default::default()
        };
        let mut buf = Vec::new();
        bh.marshal(&mut buf);
        let mut bh2 = BlockHeader::default();
        assert!(bh2.unmarshal(&buf).is_err());
    }
}
