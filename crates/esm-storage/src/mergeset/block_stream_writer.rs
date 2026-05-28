//! Stream writer for an on-disk mergeset part.
//!
//! Translates a sequence of [`InmemoryBlock`]s into the four bin files
//! (`items.bin`, `lens.bin`, `index.bin`, `metaindex.bin`) plus a final
//! `metadata.json`. Mirrors VM's `blockStreamWriter`
//! (`lib/mergeset/block_stream_writer.go:12-188`).

use std::fs::{File, create_dir_all};
use std::io::{self, BufWriter, Write as _};
use std::path::{Path, PathBuf};

use esm_compress::zstd_codec::{ZstdError, compress_zstd_level};
use thiserror::Error;

use super::{
    BlockHeader, InmemoryBlock, MAX_INDEX_BLOCK_SIZE, MetaindexRow, PartHeader, StorageBlock,
    filenames::{INDEX, ITEMS, LENS, METADATA, METAINDEX},
    inmemory_block::MarshalError,
};

/// Writes a single mergeset part to a freshly created directory.
///
/// Lifecycle:
/// 1. [`BlockStreamWriter::create`] — creates the directory and opens the
///    four bin files.
/// 2. Call [`Self::write_block`] for each sorted [`InmemoryBlock`].
/// 3. [`Self::finish`] flushes the index block tail, writes the metaindex
///    and `metadata.json`, and returns the resulting [`PartHeader`].
///
/// `write_block` may be called with the same block reused across calls; the
/// writer marshals into its own scratch storage block.
#[allow(missing_debug_implementations)] // file handles + buffers; no useful Debug output.
pub struct BlockStreamWriter {
    path: PathBuf,
    compress_level: i32,

    metaindex_writer: BufWriter<File>,
    index_writer: BufWriter<File>,
    items_writer: BufWriter<File>,
    lens_writer: BufWriter<File>,

    // Scratch buffers.
    sb: StorageBlock,
    bh: BlockHeader,
    mr: MetaindexRow,

    unpacked_index_block_buf: Vec<u8>,
    packed_index_block_buf: Vec<u8>,
    unpacked_metaindex_buf: Vec<u8>,
    packed_metaindex_buf: Vec<u8>,

    items_block_offset: u64,
    lens_block_offset: u64,
    index_block_offset: u64,

    mr_first_item_caught: bool,

    // Part-level state, exposed via finish().
    items_count: u64,
    blocks_count: u64,
    first_item: Vec<u8>,
    last_item: Vec<u8>,
    first_item_caught: bool,
}

impl BlockStreamWriter {
    /// Create the part directory at `path` and open the four bin files for
    /// writing. The directory must not already exist (matches VM's
    /// `MustMkdirFailIfExist`).
    ///
    /// # Errors
    /// Returns `io::Error` if the directory or files cannot be created.
    pub fn create(path: impl Into<PathBuf>, compress_level: i32) -> io::Result<Self> {
        let path = path.into();
        if path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("part path already exists: {}", path.display()),
            ));
        }
        create_dir_all(&path)?;

        let metaindex_writer = BufWriter::new(File::create(path.join(METAINDEX))?);
        let index_writer = BufWriter::new(File::create(path.join(INDEX))?);
        let items_writer = BufWriter::new(File::create(path.join(ITEMS))?);
        let lens_writer = BufWriter::new(File::create(path.join(LENS))?);

        Ok(Self {
            path,
            compress_level,
            metaindex_writer,
            index_writer,
            items_writer,
            lens_writer,
            sb: StorageBlock::default(),
            bh: BlockHeader::default(),
            mr: MetaindexRow::default(),
            unpacked_index_block_buf: Vec::new(),
            packed_index_block_buf: Vec::new(),
            unpacked_metaindex_buf: Vec::new(),
            packed_metaindex_buf: Vec::new(),
            items_block_offset: 0,
            lens_block_offset: 0,
            index_block_offset: 0,
            mr_first_item_caught: false,
            items_count: 0,
            blocks_count: 0,
            first_item: Vec::new(),
            last_item: Vec::new(),
            first_item_caught: false,
        })
    }

    /// Encode `ib` as one block and append it to the part. `ib` must contain
    /// at least one item and be sorted.
    ///
    /// # Errors
    /// Returns [`WriteError`] for encoding or I/O failures.
    pub fn write_block(&mut self, ib: &mut InmemoryBlock) -> Result<(), WriteError> {
        if ib.is_empty() {
            return Err(WriteError::EmptyBlock);
        }

        // Reset the block header before reuse.
        self.bh.common_prefix.clear();
        self.bh.first_item.clear();

        // Marshal block contents.
        let (items_count, marshal_type) = ib
            .marshal_sorted_data(
                &mut self.sb,
                &mut self.bh.first_item,
                &mut self.bh.common_prefix,
                self.compress_level,
            )
            .map_err(WriteError::Marshal)?;

        // Capture first/last item of the part.
        if !self.first_item_caught {
            self.first_item.clone_from(&self.bh.first_item);
            self.first_item_caught = true;
        }
        self.last_item.clear();
        self.last_item.extend_from_slice(ib.item_bytes(ib.len() - 1));

        // Write items + lens chunks.
        self.items_writer.write_all(&self.sb.items_data)?;
        let items_block_size = u32::try_from(self.sb.items_data.len())
            .map_err(|_| WriteError::ItemsBlockTooLarge(self.sb.items_data.len()))?;
        self.bh.items_block_size = items_block_size;
        self.bh.items_block_offset = self.items_block_offset;
        self.items_block_offset += u64::from(items_block_size);

        self.lens_writer.write_all(&self.sb.lens_data)?;
        let lens_block_size = u32::try_from(self.sb.lens_data.len())
            .map_err(|_| WriteError::LensBlockTooLarge(self.sb.lens_data.len()))?;
        self.bh.lens_block_size = lens_block_size;
        self.bh.lens_block_offset = self.lens_block_offset;
        self.lens_block_offset += u64::from(lens_block_size);

        // Fill remaining block-header fields and serialise into the staging
        // index-block buffer.
        self.bh.marshal_type = marshal_type;
        self.bh.items_count = items_count;

        let unpacked_len_before = self.unpacked_index_block_buf.len();
        self.bh.marshal(&mut self.unpacked_index_block_buf);

        // VM flushes the *previous* contents if adding this header would
        // exceed the target index-block size, then re-marshals into the
        // freshly drained buffer.
        if self.unpacked_index_block_buf.len() > MAX_INDEX_BLOCK_SIZE {
            self.unpacked_index_block_buf.truncate(unpacked_len_before);
            self.flush_index_data()?;
            self.bh.marshal(&mut self.unpacked_index_block_buf);
        }

        if !self.mr_first_item_caught {
            self.mr.first_item.clear();
            self.mr.first_item.extend_from_slice(&self.bh.first_item);
            self.mr_first_item_caught = true;
        }
        self.mr.block_headers_count += 1;
        self.blocks_count += 1;
        self.items_count += u64::from(items_count);

        Ok(())
    }

    fn flush_index_data(&mut self) -> Result<(), WriteError> {
        if self.unpacked_index_block_buf.is_empty() {
            return Ok(());
        }

        self.packed_index_block_buf.clear();
        compress_zstd_level(
            &mut self.packed_index_block_buf,
            &self.unpacked_index_block_buf,
            self.compress_level,
        )?;
        self.index_writer.write_all(&self.packed_index_block_buf)?;

        let index_block_size = u32::try_from(self.packed_index_block_buf.len())
            .map_err(|_| WriteError::IndexBlockTooLarge(self.packed_index_block_buf.len()))?;
        self.mr.index_block_size = index_block_size;
        self.mr.index_block_offset = self.index_block_offset;
        self.index_block_offset += u64::from(index_block_size);

        self.mr.marshal(&mut self.unpacked_metaindex_buf);

        // Reset for next index block.
        self.unpacked_index_block_buf.clear();
        self.mr.first_item.clear();
        self.mr.block_headers_count = 0;
        self.mr.index_block_offset = 0;
        self.mr.index_block_size = 0;
        self.mr_first_item_caught = false;

        Ok(())
    }

    /// Flush trailing buffers, write metaindex + metadata.json, close all
    /// files, fsync them, and return the resulting [`PartHeader`].
    ///
    /// # Errors
    /// Returns [`WriteError`] for I/O failures or an empty part (matches
    /// VM's `BlocksCount > 0` requirement).
    pub fn finish(mut self) -> Result<PartHeader, WriteError> {
        if self.blocks_count == 0 {
            return Err(WriteError::EmptyPart);
        }

        // Final index block flush.
        self.flush_index_data()?;

        // Compress + write metaindex.
        self.packed_metaindex_buf.clear();
        compress_zstd_level(
            &mut self.packed_metaindex_buf,
            &self.unpacked_metaindex_buf,
            self.compress_level,
        )?;
        self.metaindex_writer.write_all(&self.packed_metaindex_buf)?;

        // Close (drop) the buffered writers, propagating any pending IO error.
        self.items_writer.into_inner().map_err(|e| WriteError::Io(e.into_error()))?.sync_all()?;
        self.lens_writer.into_inner().map_err(|e| WriteError::Io(e.into_error()))?.sync_all()?;
        self.index_writer.into_inner().map_err(|e| WriteError::Io(e.into_error()))?.sync_all()?;
        self.metaindex_writer
            .into_inner()
            .map_err(|e| WriteError::Io(e.into_error()))?
            .sync_all()?;

        // metadata.json
        let ph = PartHeader {
            items_count: self.items_count,
            blocks_count: self.blocks_count,
            first_item: self.first_item,
            last_item: self.last_item,
        };
        let metadata_bytes = ph.to_json().map_err(WriteError::Metadata)?;
        let metadata_path = self.path.join(METADATA);
        let mut metadata_file = File::create(&metadata_path)?;
        metadata_file.write_all(&metadata_bytes)?;
        metadata_file.sync_all()?;

        // Directory fsync via esm-platform.
        esm_platform::durability::fsync_dir(&self.path)?;

        Ok(ph)
    }

    /// Path the writer was opened against.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Error)]
pub enum WriteError {
    #[error("cannot write an empty inmemory block")]
    EmptyBlock,
    #[error("cannot finish a part with zero blocks")]
    EmptyPart,
    #[error("items block size {0} exceeds u32::MAX")]
    ItemsBlockTooLarge(usize),
    #[error("lens block size {0} exceeds u32::MAX")]
    LensBlockTooLarge(usize),
    #[error("index block size {0} exceeds u32::MAX")]
    IndexBlockTooLarge(usize),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Zstd(#[from] ZstdError),
    #[error("marshal: {0}")]
    Marshal(MarshalError),
    #[error("write metadata.json: {0}")]
    Metadata(serde_json::Error),
}
