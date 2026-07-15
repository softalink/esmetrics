//! Port of `metaindex_row.go`.

use esm_encoding::{
    decompress_zstd, marshal_bytes, marshal_uint32, marshal_uint64, unmarshal_bytes,
    unmarshal_uint32, unmarshal_uint64,
};

use crate::block_stream_writer::MAX_INDEX_BLOCK_SIZE;

/// Describes a block of block headers, aka index block; lives in
/// `metaindex.bin`.
#[derive(Debug, Default, Clone)]
pub(crate) struct MetaindexRow {
    /// First item in the first block.
    /// It is used for fast lookup of the required index block.
    pub first_item: Vec<u8>,
    /// The number of block headers the index block contains.
    pub block_headers_count: u32,
    /// The offset of the index block in the index file.
    pub index_block_offset: u64,
    /// The size of the index block in the index file.
    pub index_block_size: u32,
}

impl MetaindexRow {
    pub fn reset(&mut self) {
        self.first_item.clear();
        self.block_headers_count = 0;
        self.index_block_offset = 0;
        self.index_block_size = 0;
    }

    pub fn marshal(&self, dst: &mut Vec<u8>) {
        marshal_bytes(dst, &self.first_item);
        marshal_uint32(dst, self.block_headers_count);
        marshal_uint64(dst, self.index_block_offset);
        marshal_uint32(dst, self.index_block_size);
    }

    pub fn unmarshal<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        // Unmarshal firstItem.
        let (fi, n_size) = unmarshal_bytes(src).ok_or("cannot unmarshal firstItem")?;
        self.first_item.clear();
        self.first_item.extend_from_slice(fi);
        let src = &src[n_size..];

        // Unmarshal blockHeadersCount.
        if src.len() < 4 {
            return Err(format!(
                "cannot unmarshal blockHeadersCount from {} bytes; need at least 4 bytes",
                src.len()
            ));
        }
        self.block_headers_count = unmarshal_uint32(src);
        let src = &src[4..];

        // Unmarshal indexBlockOffset.
        if src.len() < 8 {
            return Err(format!(
                "cannot unmarshal indexBlockOffset from {} bytes; need at least 8 bytes",
                src.len()
            ));
        }
        self.index_block_offset = unmarshal_uint64(src);
        let src = &src[8..];

        // Unmarshal indexBlockSize.
        if src.len() < 4 {
            return Err(format!(
                "cannot unmarshal indexBlockSize from {} bytes; need at least 4 bytes",
                src.len()
            ));
        }
        self.index_block_size = unmarshal_uint32(src);
        let src = &src[4..];

        if self.block_headers_count == 0 {
            return Err("blockHeadersCount must be bigger than 0; got 0".to_string());
        }
        if self.index_block_size as usize > 4 * MAX_INDEX_BLOCK_SIZE {
            // The index block size can exceed MAX_INDEX_BLOCK_SIZE by up to
            // 4x, since it can contain commonPrefix and firstItem at
            // blockHeader with the maximum length of MAX_INDEX_BLOCK_SIZE
            // per each field.
            return Err(format!(
                "too big indexBlockSize: {}; cannot exceed {}",
                self.index_block_size,
                4 * MAX_INDEX_BLOCK_SIZE
            ));
        }

        Ok(src)
    }
}

/// Unmarshals metaindex rows from zstd-compressed `compressed_data`.
pub(crate) fn unmarshal_metaindex_rows(
    compressed_data: &[u8],
) -> Result<Vec<MetaindexRow>, String> {
    let mut data = Vec::new();
    decompress_zstd(&mut data, compressed_data)
        .map_err(|e| format!("cannot decompress metaindex data: {e}"))?;

    let mut dst: Vec<MetaindexRow> = Vec::new();
    let mut tail: &[u8] = &data;
    while !tail.is_empty() {
        let mut mr = MetaindexRow::default();
        tail = mr.unmarshal(tail).map_err(|e| {
            format!(
                "cannot unmarshal metaindexRow #{} from metaindex data: {e}",
                dst.len()
            )
        })?;
        dst.push(mr);
    }
    if dst.is_empty() {
        return Err("expecting non-zero metaindex rows; got zero".to_string());
    }

    // Make sure metaindexRows are sorted by firstItem.
    if !dst.windows(2).all(|w| w[0].first_item <= w[1].first_item) {
        return Err(format!(
            "metaindex {} rows aren't sorted by firstItem",
            dst.len()
        ));
    }

    Ok(dst)
}

#[cfg(test)]
mod tests {
    use super::*;
    use esm_encoding::compress_zstd_level;

    #[test]
    fn marshal_unmarshal_rows() {
        let mut buf = Vec::new();
        for i in 0..10u32 {
            let mr = MetaindexRow {
                first_item: format!("item {i:03}").into_bytes(),
                block_headers_count: i + 1,
                index_block_offset: (i as u64) * 1000,
                index_block_size: i * 10,
            };
            mr.marshal(&mut buf);
        }
        let mut compressed = Vec::new();
        compress_zstd_level(&mut compressed, &buf, 1);

        let rows = unmarshal_metaindex_rows(&compressed).unwrap();
        assert_eq!(rows.len(), 10);
        assert_eq!(rows[3].first_item, b"item 003");
        assert_eq!(rows[3].block_headers_count, 4);
        assert_eq!(rows[3].index_block_offset, 3000);
        assert_eq!(rows[3].index_block_size, 30);
    }

    #[test]
    fn unmarshal_rejects_empty() {
        let mut compressed = Vec::new();
        compress_zstd_level(&mut compressed, b"", 1);
        assert!(unmarshal_metaindex_rows(&compressed).is_err());
    }
}
