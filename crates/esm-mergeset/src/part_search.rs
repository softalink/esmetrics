//! Port of `part_search.go`.
//!
//! Decoded index/data blocks are looked up in the process-global block
//! caches first (see `blockcache.rs` / `part.rs`); on a miss they are
//! decoded into per-search scratch blocks. When the cache accepts a block,
//! ownership moves to the cache (the search keeps an `Arc` clone); when it
//! doesn't, the scratch block is reclaimed once the search moves past it.

use std::sync::Arc;

use esm_encoding::decompress_zstd;

use crate::block_header::{unmarshal_block_headers, BlockHeader};
use crate::blockcache::Key;
use crate::inmemory_block::{common_prefix_len, InmemoryBlock, Item, StorageBlock};
use crate::part::{ib_cache, ib_sparse_cache, idxb_cache, IndexBlock, Part};

#[derive(Debug, Clone)]
pub(crate) enum PsError {
    Eof,
    Other(String),
}

/// A search cursor over a single part.
pub(crate) struct PartSearch {
    /// The part to search in.
    p: Arc<Part>,

    /// Whether to use the sparse data-block cache.
    sparse: bool,

    /// Index of the next metaindex row to read from `p.mrs`.
    mr_next: usize,

    /// The current index block (its `bhs` are the block headers of the
    /// current metaindex row).
    idxb: Arc<IndexBlock>,
    /// Index of the next block header to process in `idxb.bhs`.
    bh_next: usize,
    /// Scratch index block reclaimed from blocks the cache didn't accept
    /// (Go's `tmpIdB`).
    tmp_idxb: Option<IndexBlock>,

    err: Option<PsError>,

    index_buf: Vec<u8>,
    compressed_index_buf: Vec<u8>,

    sb: StorageBlock,

    /// The current decoded data block.
    ib: Arc<InmemoryBlock>,
    /// Scratch data block reclaimed from blocks the cache didn't accept
    /// (Go's `tmpIB`).
    tmp_ib: Option<InmemoryBlock>,
    ib_valid: bool,
    ib_item_idx: usize,

    /// The last item found by `next_item`, as a range into `ib.data`.
    cur_item: Item,
}

impl PartSearch {
    /// Creates a search cursor over `p`.
    ///
    /// `sparse` routes data blocks through the sparse data-block cache,
    /// which is meant for cache-unfriendly scans.
    pub fn new(p: Arc<Part>, sparse: bool) -> PartSearch {
        PartSearch {
            p,
            sparse,
            mr_next: 0,
            idxb: Arc::new(IndexBlock::default()),
            bh_next: 0,
            tmp_idxb: None,
            err: None,
            index_buf: Vec::new(),
            compressed_index_buf: Vec::new(),
            sb: StorageBlock::default(),
            ib: Arc::new(InmemoryBlock::default()),
            tmp_ib: None,
            ib_valid: false,
            ib_item_idx: 0,
            cur_item: Item::default(),
        }
    }

    /// The last item found after a successful `next_item` call.
    ///
    /// The content is valid until the next call to `next_item`.
    pub fn item(&self) -> &[u8] {
        self.cur_item.bytes(&self.ib.data)
    }

    /// Returns the last error, ignoring EOF.
    pub fn error(&self) -> Option<String> {
        match &self.err {
            Some(PsError::Other(msg)) => Some(msg.clone()),
            _ => None,
        }
    }

    /// Seeks for the first item greater or equal to `k`.
    pub fn seek(&mut self, k: &[u8]) {
        if self.error().is_some() {
            // Do nothing on unrecoverable error.
            return;
        }
        self.err = None;

        if k > &self.p.ph.last_item[..] {
            // No matching items in the part.
            self.err = Some(PsError::Eof);
            return;
        }

        if self.try_fast_seek(k) {
            return;
        }

        self.mr_next = 0;
        // Force next_block to fetch the next index block.
        self.bh_next = self.idxb.bhs.len();
        self.ib_valid = false;
        self.ib_item_idx = 0;

        if k <= &self.p.ph.first_item[..] {
            // The first item in the first block matches.
            if let Err(err) = self.next_block() {
                self.err = Some(err);
            }
            return;
        }

        // Locate the first metaindexRow to scan.
        let p = Arc::clone(&self.p);
        assert!(
            !p.mrs.is_empty(),
            "BUG: part without metaindex rows passed to PartSearch"
        );
        // The given k may be located in the previous metaindexRow, so step back.
        let n = p.mrs.partition_point(|mr| &mr.first_item[..] < k);
        self.mr_next = n.saturating_sub(1);

        // Read block headers for the found metaindexRow.
        if let Err(err) = self.next_bhs() {
            self.err = Some(err);
            return;
        }

        // Locate the first block to scan.
        // The given k may be located in the previous block, so step back.
        let n = self.idxb.bhs.partition_point(|bh| &bh.first_item[..] < k);
        self.bh_next = n.saturating_sub(1);

        // Read the block.
        if let Err(err) = self.next_block() {
            self.err = Some(err);
            return;
        }

        // Locate the first item to scan in the block.
        let cp_len = common_prefix_len(&self.ib.common_prefix, k);
        self.ib_item_idx = binary_search_key(&self.ib.data, &self.ib.items, k, cp_len);
        if self.ib_item_idx < self.ib.items.len() {
            // The item has been found.
            return;
        }

        // Nothing found in the current block. Proceed to the next block.
        // The item to search must be the first in the next block.
        if let Err(err) = self.next_block() {
            self.err = Some(err);
        }
    }

    fn try_fast_seek(&mut self, k: &[u8]) -> bool {
        if !self.ib_valid {
            return false;
        }
        let data = &self.ib.data;
        let items = &self.ib.items;
        let mut idx = self.ib_item_idx;
        if idx >= items.len() {
            // The ib is exhausted.
            return false;
        }
        let cp_len = common_prefix_len(&self.ib.common_prefix, k);
        let suffix = &k[cp_len..];
        let suffix_of = |it: Item| -> &[u8] { &data[it.start as usize + cp_len..it.end as usize] };

        if suffix > suffix_of(items[items.len() - 1]) {
            // The item is located in next blocks.
            return false;
        }

        // The item is located either in the current block or in previous blocks.
        idx = idx.saturating_sub(1);
        let mut items_window = &items[..];
        if suffix < suffix_of(items_window[idx]) {
            items_window = &items_window[..idx];
            if items_window.is_empty() {
                return false;
            }
            if suffix < suffix_of(items_window[0]) {
                // The item is located in previous blocks.
                return false;
            }
            idx = 0;
        }

        // The item is located in the current block.
        self.ib_item_idx = idx + binary_search_key(data, &items_window[idx..], k, cp_len);
        true
    }

    /// Advances to the next item. Returns true on success.
    pub fn next_item(&mut self) -> bool {
        if self.err.is_some() {
            return false;
        }

        if self.ib_valid && self.ib_item_idx < self.ib.items.len() {
            // Fast path - the current block contains more items.
            self.cur_item = self.ib.items[self.ib_item_idx];
            self.ib_item_idx += 1;
            return true;
        }

        // The current block is over. Proceed to the next block.
        if let Err(err) = self.next_block() {
            self.err = Some(match err {
                PsError::Eof => PsError::Eof,
                PsError::Other(msg) => PsError::Other(format!("error in {:?}: {msg}", self.p.path)),
            });
            return false;
        }

        // Invariant: !ib.items.is_empty() after next_block.
        self.cur_item = self.ib.items[0];
        self.ib_item_idx = 1;
        true
    }

    fn next_block(&mut self) -> Result<(), PsError> {
        if self.bh_next >= self.idxb.bhs.len() {
            // The current metaindexRow is over. Proceed to the next one.
            self.next_bhs()?;
        }
        let bh = self.idxb.bhs[self.bh_next].clone();
        self.bh_next += 1;
        self.get_inmemory_block(&bh)?;
        self.ib_item_idx = 0;
        Ok(())
    }

    fn next_bhs(&mut self) -> Result<(), PsError> {
        if self.mr_next >= self.p.mrs.len() {
            return Err(PsError::Eof);
        }
        let p = Arc::clone(&self.p);
        let mr = &p.mrs[self.mr_next];
        self.mr_next += 1;

        let idxb_key = Key {
            part_id: p.id,
            offset: mr.index_block_offset,
        };
        let cache = idxb_cache();
        let idxb = match cache.get_block(&idxb_key) {
            Some(idxb) => idxb,
            None => {
                let idxb = Arc::new(self.read_index_block(mr)?);
                // If the cache accepts the block, it owns it from now on;
                // otherwise the scratch block is reclaimed by
                // set_current_idxb once the search moves past it.
                cache.try_put_block(&idxb_key, &idxb);
                idxb
            }
        };
        self.set_current_idxb(idxb);
        self.bh_next = 0;
        Ok(())
    }

    /// Replaces the current index block, reclaiming the old one as the
    /// scratch block if it isn't owned by the cache.
    fn set_current_idxb(&mut self, idxb: Arc<IndexBlock>) {
        let old = std::mem::replace(&mut self.idxb, idxb);
        if self.tmp_idxb.is_none() {
            if let Some(mut b) = Arc::into_inner(old) {
                b.bhs.clear();
                self.tmp_idxb = Some(b);
            }
        }
    }

    fn read_index_block(
        &mut self,
        mr: &crate::metaindex_row::MetaindexRow,
    ) -> Result<IndexBlock, PsError> {
        // Read the compressed index block.
        self.compressed_index_buf
            .resize(mr.index_block_size as usize, 0);
        self.p
            .index_file
            .must_read_at(&mut self.compressed_index_buf, mr.index_block_offset);

        // Unpack the compressed index block.
        self.index_buf.clear();
        decompress_zstd(&mut self.index_buf, &self.compressed_index_buf).map_err(|e| {
            PsError::Other(format!(
                "cannot read index block: cannot decompress index block: {e}"
            ))
        })?;

        let mut idxb = self.tmp_idxb.take().unwrap_or_default();
        idxb.bhs.clear();
        unmarshal_block_headers(
            &mut idxb.bhs,
            &self.index_buf,
            mr.block_headers_count as usize,
        )
        .map_err(|e| {
            PsError::Other(format!(
                "cannot unmarshal block headers from index block (offset={}, size={}): {e}",
                mr.index_block_offset, mr.index_block_size
            ))
        })?;
        Ok(idxb)
    }

    fn get_inmemory_block(&mut self, bh: &BlockHeader) -> Result<(), PsError> {
        if bh.items_count == 1 {
            // Special case for a single item: there is no need to read the
            // data files (nor to cache the block), since firstItem is stored
            // in the header.
            let mut ib = self.tmp_ib.take().unwrap_or_default();
            ib.unmarshal_single_item(&bh.common_prefix, &bh.first_item, bh.marshal_type);
            self.set_current_ib(Arc::new(ib));
            return Ok(());
        }

        let cache = if self.sparse {
            ib_sparse_cache()
        } else {
            ib_cache()
        };
        let ib_key = Key {
            part_id: self.p.id,
            offset: bh.items_block_offset,
        };
        let ib = match cache.get_block(&ib_key) {
            Some(ib) => ib,
            None => {
                let ib = Arc::new(self.read_inmemory_block(bh)?);
                // If the cache accepts the block, it owns it from now on;
                // otherwise the scratch block is reclaimed by
                // set_current_ib once the search moves past it.
                cache.try_put_block(&ib_key, &ib);
                ib
            }
        };
        self.set_current_ib(ib);
        Ok(())
    }

    /// Replaces the current data block, reclaiming the old one as the
    /// scratch block if it isn't owned by the cache.
    fn set_current_ib(&mut self, ib: Arc<InmemoryBlock>) {
        let old = std::mem::replace(&mut self.ib, ib);
        if self.tmp_ib.is_none() {
            if let Some(mut b) = Arc::into_inner(old) {
                b.reset();
                self.tmp_ib = Some(b);
            }
        }
        self.ib_valid = true;
    }

    fn read_inmemory_block(&mut self, bh: &BlockHeader) -> Result<InmemoryBlock, PsError> {
        self.sb.reset();
        self.sb.items_data.resize(bh.items_block_size as usize, 0);
        self.p
            .items_file
            .must_read_at(&mut self.sb.items_data, bh.items_block_offset);

        self.sb.lens_data.resize(bh.lens_block_size as usize, 0);
        self.p
            .lens_file
            .must_read_at(&mut self.sb.lens_data, bh.lens_block_offset);

        let mut ib = self.tmp_ib.take().unwrap_or_default();
        ib.reset();
        ib.unmarshal_data(
            &self.sb,
            &bh.first_item,
            &bh.common_prefix,
            bh.items_count,
            bh.marshal_type,
        )
        .map_err(|e| {
            PsError::Other(format!(
                "cannot unmarshal storage block with {} items: {e}",
                bh.items_count
            ))
        })?;
        Ok(ib)
    }
}

pub(crate) fn binary_search_key(data: &[u8], items: &[Item], k: &[u8], cp_len: usize) -> usize {
    if items.is_empty() {
        return 0;
    }
    let suffix = &k[cp_len..];
    let suffix_of = |it: Item| -> &[u8] { &data[it.start as usize + cp_len..it.end as usize] };
    if suffix <= suffix_of(items[0]) {
        // Fast path - the item is the first.
        return 0;
    }
    let items = &items[1..];
    let offset = 1usize;

    items.partition_point(|&it| suffix > suffix_of(it)) + offset
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_stream_writer::BlockStreamWriter;
    use crate::inmemory_block::testutil::Rng;
    use crate::inmemory_part::InmemoryPart;
    use crate::merge::merge_block_streams;
    use crate::merge::tests::new_test_inmemory_block_stream_readers;
    use crate::part_header::PartHeader;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn new_test_part(
        r: &mut Rng,
        blocks_count: usize,
        max_items_per_block: usize,
    ) -> (Arc<Part>, Vec<Vec<u8>>) {
        let (mut bsrs, items) =
            new_test_inmemory_block_stream_readers(r, blocks_count, max_items_per_block);

        let items_merged = AtomicU64::new(0);
        let mut ph = PartHeader::default();
        let mut bsw = BlockStreamWriter::new_inmemory_part(-3);
        let bufs = merge_block_streams(&mut ph, &mut bsw, &mut bsrs, None, None, &items_merged)
            .unwrap()
            .unwrap();
        assert_eq!(items_merged.load(Ordering::Relaxed) as usize, items.len());
        let ip = InmemoryPart::from_buffers(ph, bufs);
        (Arc::new(ip.new_part()), items)
    }

    fn test_part_search_serial(r: &mut Rng, p: &Arc<Part>, items: &[Vec<u8>]) {
        let mut ps = PartSearch::new(Arc::clone(p), true);

        // Search for the item smaller than items[0].
        let mut k: Vec<u8> = items[0].clone();
        if !k.is_empty() {
            k.pop();
        }
        ps.seek(&k);
        for (i, item) in items.iter().enumerate() {
            assert!(ps.next_item(), "missing item at position {i}");
            assert_eq!(ps.item(), &item[..], "unexpected item at position {i}");
        }
        assert!(!ps.next_item(), "unexpected item found past the end");
        assert!(ps.error().is_none());

        // Search for the item bigger than items[len-1].
        let mut k: Vec<u8> = items[items.len() - 1].clone();
        k.extend_from_slice(b"tail");
        ps.seek(&k);
        assert!(!ps.next_item());
        assert!(ps.error().is_none());

        // Search for inner items.
        for loop_idx in 0..100 {
            let idx = r.intn(items.len());
            let k = &items[idx];
            ps.seek(k);
            let n = items.partition_point(|item| item[..] < k[..]);
            for (i, item) in items.iter().enumerate().skip(n) {
                assert!(
                    ps.next_item(),
                    "missing item at position {i} for idx {n} on the loop {loop_idx}"
                );
                assert_eq!(ps.item(), &item[..], "loop {loop_idx} position {i}");
            }
            assert!(!ps.next_item(), "loop {loop_idx}: superfluous item");
            assert!(ps.error().is_none());
        }

        // Search for sorted items.
        for (i, item) in items.iter().enumerate() {
            ps.seek(item);
            assert!(ps.next_item(), "cannot find items[{i}]");
            assert_eq!(ps.item(), &item[..]);
            assert!(ps.error().is_none());
        }

        // Search for reversely sorted items.
        for i in 0..items.len() {
            let item = &items[items.len() - i - 1];
            ps.seek(item);
            assert!(ps.next_item(), "cannot find items[{i}] (reverse)");
            assert_eq!(ps.item(), &item[..]);
            assert!(ps.error().is_none());
        }
    }

    #[test]
    fn test_part_search() {
        let mut r = Rng::new(1);
        let (p, items) = new_test_part(&mut r, 10, 4000);

        // Serial.
        test_part_search_serial(&mut r, &p, &items);

        // Concurrent.
        std::thread::scope(|s| {
            for n in 0..5u64 {
                let p = &p;
                let items = &items;
                s.spawn(move || {
                    let mut r_local = Rng::new(n);
                    test_part_search_serial(&mut r_local, p, items);
                });
            }
        });
    }

    #[test]
    fn test_part_search_single_and_multi_item_blocks() {
        // Mirrors TestGetInmemoryBlockWithZeroSizeBlock: the first block
        // contains a single item (zero itemsBlockSize), the second contains
        // two items; both must be readable.
        let mut ph = PartHeader::default();
        let mut bsw = BlockStreamWriter::new_inmemory_part(-3);

        let write_block = |bsw: &mut BlockStreamWriter, ph: &mut PartHeader, items: &[&[u8]]| {
            let mut ib = InmemoryBlock::default();
            for item in items {
                assert!(ib.add(item));
            }
            ib.sort_items();
            ph.items_count += ib.items.len() as u64;
            if ph.blocks_count == 0 {
                ph.first_item = ib.items[0].bytes(&ib.data).to_vec();
            }
            ph.last_item = ib.items[ib.items.len() - 1].bytes(&ib.data).to_vec();
            ph.blocks_count += 1;
            bsw.write_block(&mut ib);
        };

        write_block(&mut bsw, &mut ph, &[b"a"]);
        write_block(&mut bsw, &mut ph, &[b"b0", b"b1"]);
        let bufs = bsw.must_close().unwrap();
        let ip = InmemoryPart::from_buffers(ph, bufs);
        let p = Arc::new(ip.new_part());

        let mut ps = PartSearch::new(Arc::clone(&p), false);
        ps.next_bhs().unwrap();
        let bhs = &ps.idxb.bhs;
        assert_eq!(bhs.len(), 2);
        assert_eq!(bhs[0].items_block_offset, bhs[1].items_block_offset);
        assert_eq!(bhs[0].items_block_size, 0);

        let bh0 = bhs[0].clone();
        let bh1 = bhs[1].clone();
        ps.get_inmemory_block(&bh1).unwrap();
        assert_eq!(ps.ib.items.len(), 2);
        assert_eq!(ps.ib.items[0].bytes(&ps.ib.data), b"b0");
        ps.get_inmemory_block(&bh0).unwrap();
        assert_eq!(ps.ib.items.len(), 1);
        assert_eq!(ps.ib.items[0].bytes(&ps.ib.data), b"a");
    }
}
