//! Stream reader for an on-disk mergeset part.
//!
//! Walks the metaindex, decompresses each index block lazily, and yields one
//! [`InmemoryBlock`] per data block. Mirrors VM's `blockStreamReader`
//! (`lib/mergeset/block_stream_reader.go`).

use std::fs::File;
use std::io::{self, Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};

use esm_compress::zstd_codec::{ZstdError, decompress_zstd};
use thiserror::Error;

use super::{
    BlockHeader, InmemoryBlock, MetaindexRow, PartHeader, StorageBlock,
    block_header::{BlockHeaderError, UnmarshalHeadersError, unmarshal_block_headers},
    filenames::{INDEX, ITEMS, LENS, METADATA, METAINDEX},
    inmemory_block::UnmarshalDataError,
    metaindex_row::{UnmarshalMetaindexError, unmarshal_metaindex_rows},
    part_header::PartHeaderError,
};

/// Opened on-disk mergeset part, ready to be read block-by-block.
///
/// The reader holds file handles for `index.bin`, `items.bin`, `lens.bin`,
/// plus the fully-parsed `metaindex.bin` and `metadata.json`. Calling
/// [`Self::next_block`] yields the next block; `None` indicates EOF.
#[allow(missing_debug_implementations)] // file handles + buffers; no useful Debug output.
pub struct BlockStreamReader {
    path: PathBuf,
    pub part_header: PartHeader,
    metaindex_rows: Vec<MetaindexRow>,

    index_file: File,
    items_file: File,
    lens_file: File,

    // Cursor through the metaindex + currently-loaded index block.
    metaindex_idx: usize,
    cur_index_block: Vec<BlockHeader>,
    cur_index_idx: usize,

    // Scratch buffers reused across calls.
    scratch_index_compressed: Vec<u8>,
    scratch_index_unpacked: Vec<u8>,
    scratch_storage_block: StorageBlock,
}

impl BlockStreamReader {
    /// Open the part at `path`. Reads + validates `metadata.json` and
    /// `metaindex.bin`; defers per-block I/O to [`Self::next_block`].
    ///
    /// # Errors
    /// Returns [`ReadError`] if any of the four expected files is missing,
    /// metadata is malformed, or the metaindex cannot be decoded.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, ReadError> {
        let path = path.into();

        // metadata.json
        let metadata_bytes = std::fs::read(path.join(METADATA))?;
        let part_header = PartHeader::from_json(&metadata_bytes)?;

        // metaindex.bin
        let metaindex_bytes = std::fs::read(path.join(METAINDEX))?;
        let mut metaindex_raw = Vec::new();
        decompress_zstd(&mut metaindex_raw, &metaindex_bytes)?;
        let mut metaindex_rows = Vec::new();
        unmarshal_metaindex_rows(&mut metaindex_rows, &metaindex_raw)?;

        Ok(Self {
            path: path.clone(),
            part_header,
            metaindex_rows,
            index_file: File::open(path.join(INDEX))?,
            items_file: File::open(path.join(ITEMS))?,
            lens_file: File::open(path.join(LENS))?,
            metaindex_idx: 0,
            cur_index_block: Vec::new(),
            cur_index_idx: 0,
            scratch_index_compressed: Vec::new(),
            scratch_index_unpacked: Vec::new(),
            scratch_storage_block: StorageBlock::default(),
        })
    }

    /// Yield the next data block as a fully-decoded [`InmemoryBlock`].
    /// Returns `Ok(None)` at end-of-part.
    ///
    /// # Errors
    /// Returns [`ReadError`] on I/O, decompression, parse, or sort-invariant
    /// failures.
    pub fn next_block(&mut self) -> Result<Option<InmemoryBlock>, ReadError> {
        loop {
            // If we've exhausted the current index block, load the next.
            if self.cur_index_idx >= self.cur_index_block.len() {
                if self.metaindex_idx >= self.metaindex_rows.len() {
                    return Ok(None);
                }
                self.load_current_index_block()?;
                self.metaindex_idx += 1;
                if self.cur_index_block.is_empty() {
                    continue;
                }
            }

            let bh_idx = self.cur_index_idx;
            self.cur_index_idx += 1;
            let bh = self.cur_index_block[bh_idx].clone();
            let mut ib = InmemoryBlock::default();
            self.read_data_block(&bh, &mut ib)?;
            return Ok(Some(ib));
        }
    }

    fn load_current_index_block(&mut self) -> Result<(), ReadError> {
        let mr = &self.metaindex_rows[self.metaindex_idx];
        self.index_file.seek(SeekFrom::Start(mr.index_block_offset))?;
        let size = usize::try_from(mr.index_block_size)
            .map_err(|_| ReadError::SizeOverflow(u64::from(mr.index_block_size)))?;
        self.scratch_index_compressed.resize(size, 0);
        self.index_file.read_exact(&mut self.scratch_index_compressed)?;
        decompress_zstd(&mut self.scratch_index_unpacked, &self.scratch_index_compressed)?;

        self.cur_index_block.clear();
        let count = usize::try_from(mr.block_headers_count)
            .map_err(|_| ReadError::SizeOverflow(u64::from(mr.block_headers_count)))?;
        unmarshal_block_headers(&mut self.cur_index_block, &self.scratch_index_unpacked, count)?;
        self.cur_index_idx = 0;
        Ok(())
    }

    fn read_data_block(
        &mut self,
        bh: &BlockHeader,
        out: &mut InmemoryBlock,
    ) -> Result<(), ReadError> {
        // Items chunk.
        self.items_file.seek(SeekFrom::Start(bh.items_block_offset))?;
        let items_size = usize::try_from(bh.items_block_size)
            .map_err(|_| ReadError::SizeOverflow(u64::from(bh.items_block_size)))?;
        self.scratch_storage_block.items_data.resize(items_size, 0);
        self.items_file.read_exact(&mut self.scratch_storage_block.items_data)?;

        // Lens chunk.
        self.lens_file.seek(SeekFrom::Start(bh.lens_block_offset))?;
        let lens_size = usize::try_from(bh.lens_block_size)
            .map_err(|_| ReadError::SizeOverflow(u64::from(bh.lens_block_size)))?;
        self.scratch_storage_block.lens_data.resize(lens_size, 0);
        self.lens_file.read_exact(&mut self.scratch_storage_block.lens_data)?;

        out.unmarshal_data(
            &self.scratch_storage_block,
            &bh.first_item,
            &bh.common_prefix,
            bh.items_count,
            bh.marshal_type,
        )?;
        Ok(())
    }

    /// Path the reader was opened against.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Zstd(#[from] ZstdError),
    #[error(transparent)]
    Metadata(#[from] PartHeaderError),
    #[error(transparent)]
    Metaindex(#[from] UnmarshalMetaindexError),
    #[error(transparent)]
    IndexBlock(#[from] UnmarshalHeadersError),
    #[error(transparent)]
    BlockHeader(#[from] BlockHeaderError),
    #[error(transparent)]
    Decode(#[from] UnmarshalDataError),
    #[error("size {0} does not fit in usize on this platform")]
    SizeOverflow(u64),
}

#[cfg(test)]
mod tests {
    use super::super::block_stream_writer::BlockStreamWriter;
    use super::*;

    fn make_block(items: &[&[u8]]) -> InmemoryBlock {
        let mut ib = InmemoryBlock::default();
        for it in items {
            assert!(ib.add(it));
        }
        ib.sort_items();
        ib
    }

    #[test]
    fn write_read_single_block_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let part_path = tmp.path().join("part-0");

        // Write
        let mut writer = BlockStreamWriter::create(&part_path, 5).unwrap();
        let mut ib = make_block(&[b"alpha", b"beta", b"gamma"]);
        writer.write_block(&mut ib).unwrap();
        let ph = writer.finish().unwrap();
        assert_eq!(ph.items_count, 3);
        assert_eq!(ph.blocks_count, 1);
        assert_eq!(ph.first_item, b"alpha");
        assert_eq!(ph.last_item, b"gamma");

        // Read back
        let mut reader = BlockStreamReader::open(&part_path).unwrap();
        assert_eq!(reader.part_header.items_count, 3);
        let block = reader.next_block().unwrap().unwrap();
        assert_eq!(block.len(), 3);
        assert_eq!(block.item_bytes(0), b"alpha");
        assert_eq!(block.item_bytes(2), b"gamma");
        assert!(reader.next_block().unwrap().is_none());
    }

    #[test]
    fn write_read_multi_block_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let part_path = tmp.path().join("part-0");

        // Three blocks of distinct items.
        let blocks = [
            vec![b"aa".to_vec(), b"ab".to_vec(), b"ac".to_vec()],
            vec![b"ba".to_vec(), b"bb".to_vec()],
            vec![b"ca".to_vec(), b"cb".to_vec(), b"cc".to_vec(), b"cd".to_vec()],
        ];

        let mut writer = BlockStreamWriter::create(&part_path, 5).unwrap();
        for block in &blocks {
            let refs: Vec<&[u8]> = block.iter().map(Vec::as_slice).collect();
            let mut ib = make_block(&refs);
            writer.write_block(&mut ib).unwrap();
        }
        let ph = writer.finish().unwrap();
        assert_eq!(ph.items_count, 9);
        assert_eq!(ph.blocks_count, 3);
        assert_eq!(ph.first_item, b"aa");
        assert_eq!(ph.last_item, b"cd");

        let mut reader = BlockStreamReader::open(&part_path).unwrap();
        for (block_idx, block) in blocks.iter().enumerate() {
            let decoded = reader.next_block().unwrap().expect("expected more blocks");
            assert_eq!(decoded.len(), block.len(), "block {block_idx}");
            for (i, item) in block.iter().enumerate() {
                assert_eq!(decoded.item_bytes(i), item.as_slice());
            }
        }
        assert!(reader.next_block().unwrap().is_none());
    }

    #[test]
    fn write_read_with_zstd_block() {
        let tmp = tempfile::tempdir().unwrap();
        let part_path = tmp.path().join("part-0");

        // Large repetitive block to trigger zstd marshaling.
        let mut ib = InmemoryBlock::default();
        for i in 0..150u32 {
            let mut item = b"shared/prefix/".to_vec();
            item.extend_from_slice(format!("{i:05}").as_bytes());
            item.extend_from_slice(&[0xab; 32]);
            assert!(ib.add(&item));
        }
        ib.sort_items();

        let mut writer = BlockStreamWriter::create(&part_path, 5).unwrap();
        writer.write_block(&mut ib).unwrap();
        let ph = writer.finish().unwrap();
        assert_eq!(ph.items_count, 150);

        let mut reader = BlockStreamReader::open(&part_path).unwrap();
        let decoded = reader.next_block().unwrap().unwrap();
        assert_eq!(decoded.len(), 150);
        // Verify a sample item value.
        let mut expected = b"shared/prefix/".to_vec();
        expected.extend_from_slice(b"00042");
        expected.extend_from_slice(&[0xab; 32]);
        assert_eq!(decoded.item_bytes(42), expected.as_slice());
        assert!(reader.next_block().unwrap().is_none());
    }
}
