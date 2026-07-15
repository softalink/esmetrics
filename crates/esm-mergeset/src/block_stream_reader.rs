//! Port of `block_stream_reader.go`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use esm_common::filestream;
use esm_encoding::decompress_zstd;

use crate::block_header::{unmarshal_block_headers, BlockHeader};
use crate::filenames::{INDEX_FILENAME, ITEMS_FILENAME, LENS_FILENAME, METAINDEX_FILENAME};
use crate::inmemory_block::{InmemoryBlock, StorageBlock};
use crate::inmemory_part::InmemoryPart;
use crate::metaindex_row::{unmarshal_metaindex_rows, MetaindexRow};
use crate::part_header::PartHeader;

enum StreamReader {
    File(filestream::Reader),
    Mem { buf: Arc<Vec<u8>>, pos: usize },
    Closed,
}

impl StreamReader {
    fn must_read(&mut self, dst: &mut [u8]) {
        match self {
            StreamReader::File(r) => esm_common::fs::must_read_data(r, dst),
            StreamReader::Mem { buf, pos } => {
                assert!(
                    *pos + dst.len() <= buf.len(),
                    "BUG: cannot read {} bytes at offset {pos} from in-memory stream of size {}",
                    dst.len(),
                    buf.len()
                );
                dst.copy_from_slice(&buf[*pos..*pos + dst.len()]);
                *pos += dst.len();
            }
            StreamReader::Closed => panic!("BUG: read from closed stream reader"),
        }
    }

    fn must_close(&mut self) {
        if let StreamReader::File(r) = std::mem::replace(self, StreamReader::Closed) {
            r.must_close();
        }
    }
}

#[derive(Debug)]
enum BsrError {
    Eof,
    Other(String),
}

/// Streaming reader over the blocks of a part (or of a single sorted
/// in-memory block).
pub(crate) struct BlockStreamReader {
    /// Contains the current block if `next` returned true.
    pub block: InmemoryBlock,

    /// Set if the reader was initialized from a single in-memory block.
    is_inmemory_block: bool,

    /// The index of the current item in `block`, used by `curr_item`.
    pub curr_item_idx: usize,

    path: PathBuf,

    /// partHeader of the read part.
    pub ph: PartHeader,

    mrs: Vec<MetaindexRow>,
    mr_idx: usize,

    /// Currently processed block headers.
    bhs: Vec<BlockHeader>,
    /// The index of the next block header to process.
    bh_idx: usize,

    index_reader: StreamReader,
    items_reader: StreamReader,
    lens_reader: StreamReader,

    sb: StorageBlock,

    items_read: u64,
    blocks_read: u64,
    first_item_checked: bool,

    packed_buf: Vec<u8>,
    unpacked_buf: Vec<u8>,

    err: Option<BsrError>,
}

impl BlockStreamReader {
    fn new_empty() -> BlockStreamReader {
        BlockStreamReader {
            block: InmemoryBlock::default(),
            is_inmemory_block: false,
            curr_item_idx: 0,
            path: PathBuf::new(),
            ph: PartHeader::default(),
            mrs: Vec::new(),
            mr_idx: 0,
            bhs: Vec::new(),
            bh_idx: 0,
            index_reader: StreamReader::Closed,
            items_reader: StreamReader::Closed,
            lens_reader: StreamReader::Closed,
            sb: StorageBlock::default(),
            items_read: 0,
            blocks_read: 0,
            first_item_checked: false,
            packed_buf: Vec::new(),
            unpacked_buf: Vec::new(),
            err: None,
        }
    }

    /// Initializes the reader from the given single in-memory block.
    pub fn from_inmemory_block(ib: &InmemoryBlock) -> BlockStreamReader {
        let mut bsr = BlockStreamReader::new_empty();
        bsr.block.copy_from(ib);
        bsr.block.sort_items();
        bsr.is_inmemory_block = true;
        bsr
    }

    /// Initializes the reader from the given in-memory part.
    pub fn from_inmemory_part(mp: &InmemoryPart) -> BlockStreamReader {
        let mut bsr = BlockStreamReader::new_empty();

        bsr.mrs = unmarshal_metaindex_rows(&mp.metaindex_data).unwrap_or_else(|e| {
            panic!("BUG: cannot unmarshal metaindex rows from inmemory part: {e}")
        });

        bsr.ph.copy_from(&mp.ph);
        bsr.index_reader = StreamReader::Mem {
            buf: Arc::clone(&mp.index_data),
            pos: 0,
        };
        bsr.items_reader = StreamReader::Mem {
            buf: Arc::clone(&mp.items_data),
            pos: 0,
        };
        bsr.lens_reader = StreamReader::Mem {
            buf: Arc::clone(&mp.lens_data),
            pos: 0,
        };

        assert!(
            bsr.ph.items_count > 0,
            "BUG: source inmemoryPart must contain at least a single item"
        );
        assert!(
            bsr.ph.blocks_count > 0,
            "BUG: source inmemoryPart must contain at least a single block"
        );
        bsr
    }

    /// Initializes the reader from a file-based part at the given path.
    ///
    /// Part files are read without OS cache pollution, since the part is
    /// usually deleted after the merge.
    pub fn from_file_part(path: &Path) -> BlockStreamReader {
        let mut bsr = BlockStreamReader::new_empty();

        bsr.ph.must_read_metadata(path);

        let metaindex_path = path.join(METAINDEX_FILENAME);
        let metaindex_data = esm_common::fs::read_full_file(&metaindex_path);
        bsr.mrs = unmarshal_metaindex_rows(&metaindex_data).unwrap_or_else(|e| {
            panic!("FATAL: cannot unmarshal metaindex rows from file {metaindex_path:?}: {e}")
        });

        bsr.path = path.to_path_buf();

        bsr.index_reader = StreamReader::File(filestream::Reader::must_open(
            path.join(INDEX_FILENAME),
            true,
        ));
        bsr.items_reader = StreamReader::File(filestream::Reader::must_open(
            path.join(ITEMS_FILENAME),
            true,
        ));
        bsr.lens_reader = StreamReader::File(filestream::Reader::must_open(
            path.join(LENS_FILENAME),
            true,
        ));
        bsr
    }

    /// Closes the reader.
    pub fn must_close(&mut self) {
        if !self.is_inmemory_block {
            self.index_reader.must_close();
            self.items_reader.must_close();
            self.lens_reader.must_close();
        }
    }

    pub fn curr_item(&self) -> &[u8] {
        self.block.items[self.curr_item_idx].bytes(&self.block.data)
    }

    /// The current block header (valid after `next` returned true for
    /// non-inmemory-block readers).
    #[cfg(test)]
    pub fn curr_bh(&self) -> &BlockHeader {
        &self.bhs[self.bh_idx - 1]
    }

    pub fn next(&mut self) -> bool {
        if self.err.is_some() {
            return false;
        }
        if self.is_inmemory_block {
            self.err = Some(BsrError::Eof);
            return true;
        }

        if self.bh_idx >= self.bhs.len() {
            // The current index block is over. Try reading the next index block.
            if let Err(err) = self.read_next_bhs() {
                let err = match err {
                    BsrError::Eof => {
                        // Check the last item.
                        let b = &self.block;
                        let last_item = b.items[b.items.len() - 1].bytes(&b.data);
                        if self.ph.last_item != last_item {
                            BsrError::Other(format!(
                                "unexpected last item; got {}; want {}",
                                crate::util::hex_encode(last_item),
                                crate::util::hex_encode(&self.ph.last_item)
                            ))
                        } else {
                            BsrError::Eof
                        }
                    }
                    BsrError::Other(msg) => {
                        BsrError::Other(format!("cannot read the next index block: {msg}"))
                    }
                };
                self.err = Some(err);
                return false;
            }
        }

        let bh = &self.bhs[self.bh_idx];
        self.bh_idx += 1;

        self.sb.items_data.resize(bh.items_block_size as usize, 0);
        self.items_reader.must_read(&mut self.sb.items_data);

        self.sb.lens_data.resize(bh.lens_block_size as usize, 0);
        self.lens_reader.must_read(&mut self.sb.lens_data);

        if let Err(err) = self.block.unmarshal_data(
            &self.sb,
            &bh.first_item,
            &bh.common_prefix,
            bh.items_count,
            bh.marshal_type,
        ) {
            self.err = Some(BsrError::Other(format!(
                "cannot unmarshal inmemoryBlock from storageBlock with firstItem={}, commonPrefix={}, itemsCount={}, marshalType={}: {err}",
                crate::util::hex_encode(&bh.first_item),
                crate::util::hex_encode(&bh.common_prefix),
                bh.items_count,
                bh.marshal_type.as_u8()
            )));
            return false;
        }
        self.blocks_read += 1;
        if self.blocks_read > self.ph.blocks_count {
            self.err = Some(BsrError::Other(format!(
                "too many blocks read: {}; must be smaller than partHeader.blocksCount {}",
                self.blocks_read, self.ph.blocks_count
            )));
            return false;
        }
        self.curr_item_idx = 0;
        self.items_read += self.block.items.len() as u64;
        if self.items_read > self.ph.items_count {
            self.err = Some(BsrError::Other(format!(
                "too many items read: {}; must be smaller than partHeader.itemsCount {}",
                self.items_read, self.ph.items_count
            )));
            return false;
        }
        if !self.first_item_checked {
            self.first_item_checked = true;
            let first_item = self.block.items[0].bytes(&self.block.data);
            if self.ph.first_item != first_item {
                self.err = Some(BsrError::Other(format!(
                    "unexpected first item; got {}; want {}",
                    crate::util::hex_encode(first_item),
                    crate::util::hex_encode(&self.ph.first_item)
                )));
                return false;
            }
        }
        true
    }

    fn read_next_bhs(&mut self) -> Result<(), BsrError> {
        if self.mr_idx >= self.mrs.len() {
            return Err(BsrError::Eof);
        }

        let mr = &self.mrs[self.mr_idx];
        self.mr_idx += 1;

        // Read compressed index block.
        self.packed_buf.resize(mr.index_block_size as usize, 0);
        self.index_reader.must_read(&mut self.packed_buf);

        // Unpack the compressed index block.
        self.unpacked_buf.clear();
        decompress_zstd(&mut self.unpacked_buf, &self.packed_buf)
            .map_err(|e| BsrError::Other(format!("cannot decompress index block: {e}")))?;

        // Unmarshal the unpacked index block into bhs.
        self.bhs.clear();
        unmarshal_block_headers(
            &mut self.bhs,
            &self.unpacked_buf,
            mr.block_headers_count as usize,
        )
        .map_err(|e| {
            BsrError::Other(format!(
                "cannot unmarshal blockHeaders in the index block #{}: {e}",
                self.mr_idx
            ))
        })?;
        self.bh_idx = 0;
        Ok(())
    }

    /// Returns the last error, ignoring EOF.
    pub fn error(&self) -> Option<String> {
        match &self.err {
            Some(BsrError::Other(msg)) => Some(msg.clone()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inmemory_block::testutil::Rng;

    fn test_block_stream_reader_read(ip: &InmemoryPart, items: &[Vec<u8>]) -> Result<(), String> {
        let mut bsr = BlockStreamReader::from_inmemory_part(ip);
        let mut i = 0;
        while bsr.next() {
            for it in &bsr.block.items {
                let item = it.bytes(&bsr.block.data);
                if item != &items[i][..] {
                    return Err(format!("unexpected item[{i}]"));
                }
                i += 1;
            }
        }
        if let Some(err) = bsr.error() {
            return Err(err);
        }
        if i != items.len() {
            return Err(format!(
                "unexpected number of items read; got {i}; want {}",
                items.len()
            ));
        }
        Ok(())
    }

    #[test]
    fn test_block_stream_reader_read_from_inmemory_part() {
        let mut r = Rng::new(1);
        let mut items: Vec<Vec<u8>> = Vec::new();
        let mut ib = InmemoryBlock::default();
        for _ in 0..100 {
            let item = r.random_bytes();
            if !ib.add(&item) {
                break;
            }
            items.push(item);
        }
        items.sort();
        let ip = InmemoryPart::init(&mut ib);

        // Make sure items may be read concurrently from the same inmemoryPart.
        std::thread::scope(|s| {
            let handles: Vec<_> = (0..5)
                .map(|_| s.spawn(|| test_block_stream_reader_read(&ip, &items)))
                .collect();
            for h in handles {
                h.join().unwrap().unwrap();
            }
        });
    }
}
