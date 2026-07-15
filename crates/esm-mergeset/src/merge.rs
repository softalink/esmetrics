//! Port of `merge.go`: k-way merging of block streams.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::block_stream_reader::BlockStreamReader;
use crate::block_stream_writer::BlockStreamWriter;
use crate::inmemory_block::{InmemoryBlock, Item};
use crate::part_header::PartHeader;
use crate::util::Shutdown;

/// Callback that can transform the items allocated at the given data before
/// a full block is flushed to persistent storage during merge.
///
/// The callback must keep the items sorted. The first item must not become
/// smaller and the last item must not become bigger. The callback can mutate
/// `data` and `items` in place (e.g. deduplicate or merge rows).
pub type PrepareBlockCallback = Arc<dyn Fn(&mut Vec<u8>, &mut Vec<Item>) + Send + Sync>;

#[derive(Debug)]
pub(crate) enum MergeError {
    ForciblyStopped,
    Other(String),
}

impl std::fmt::Display for MergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MergeError::ForciblyStopped => write!(f, "forcibly stopped"),
            MergeError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

/// Merges `bsrs` and writes the result to `bsw`. Fills `ph`.
///
/// `prepare_block` is optional. The merge is immediately stopped when `stop`
/// is signaled. The number of merged items is atomically added to
/// `items_merged`.
///
/// `bsw` is always closed; for in-memory destinations the four stream
/// buffers are returned on success.
pub(crate) fn merge_block_streams(
    ph: &mut PartHeader,
    bsw: &mut BlockStreamWriter,
    bsrs: &mut [BlockStreamReader],
    prepare_block: Option<&PrepareBlockCallback>,
    stop: Option<&Shutdown>,
    items_merged: &AtomicU64,
) -> Result<Option<[Vec<u8>; 4]>, MergeError> {
    let mut bsm = BlockStreamMerger::default();
    let res = bsm
        .init(bsrs)
        .and_then(|_| bsm.merge(bsw, bsrs, ph, prepare_block, stop, items_merged));
    let bufs = bsw.must_close();
    match res {
        Ok(()) => Ok(bufs),
        Err(MergeError::ForciblyStopped) => Err(MergeError::ForciblyStopped),
        Err(MergeError::Other(msg)) => Err(MergeError::Other(format!(
            "cannot merge {} block streams: {msg}",
            bsrs.len()
        ))),
    }
}

#[derive(Default)]
struct BlockStreamMerger {
    /// Heap of indices into the bsrs slice, ordered by current item.
    heap: Vec<usize>,

    /// Scratch block with pending items.
    ib: InmemoryBlock,

    ph_first_item_caught: bool,

    // Auxiliary buffers used in flush_ib for consistency checks after the
    // prepare_block call.
    first_item: Vec<u8>,
    last_item: Vec<u8>,

    // Scratch buffer holding a copy of the next reader's current item.
    next_item: Vec<u8>,
}

fn heap_less(bsrs: &[BlockStreamReader], heap: &[usize], i: usize, j: usize) -> bool {
    bsrs[heap[i]].curr_item() < bsrs[heap[j]].curr_item()
}

fn sift_down(bsrs: &[BlockStreamReader], heap: &mut [usize], mut i: usize) {
    let n = heap.len();
    loop {
        let left = 2 * i + 1;
        if left >= n {
            return;
        }
        let mut smallest = left;
        let right = left + 1;
        if right < n && heap_less(bsrs, heap, right, left) {
            smallest = right;
        }
        if !heap_less(bsrs, heap, smallest, i) {
            return;
        }
        heap.swap(i, smallest);
        i = smallest;
    }
}

fn heap_init(bsrs: &[BlockStreamReader], heap: &mut [usize]) {
    let n = heap.len();
    for i in (0..n / 2).rev() {
        sift_down(bsrs, heap, i);
    }
}

fn heap_pop_top(bsrs: &[BlockStreamReader], heap: &mut Vec<usize>) {
    let n = heap.len();
    heap.swap(0, n - 1);
    heap.pop();
    sift_down(bsrs, heap, 0);
}

impl BlockStreamMerger {
    fn init(&mut self, bsrs: &mut [BlockStreamReader]) -> Result<(), MergeError> {
        self.heap.clear();
        self.ib.reset();
        self.ph_first_item_caught = false;

        for (i, bsr) in bsrs.iter_mut().enumerate() {
            if bsr.next() {
                self.heap.push(i);
            }
            if let Some(err) = bsr.error() {
                return Err(MergeError::Other(format!(
                    "cannot obtain the next block from blockStreamReader: {err}"
                )));
            }
        }
        heap_init(bsrs, &mut self.heap);

        if self.heap.is_empty() {
            return Err(MergeError::Other("bsrHeap cannot be empty".to_string()));
        }
        Ok(())
    }

    /// Returns the index (into bsrs) of the heap reader with the smallest
    /// current item among the children of the heap root.
    fn next_reader(&self, bsrs: &[BlockStreamReader]) -> Option<usize> {
        if self.heap.len() < 2 {
            return None;
        }
        if self.heap.len() < 3 {
            return Some(self.heap[1]);
        }
        let a = self.heap[1];
        let b = self.heap[2];
        if bsrs[a].curr_item() <= bsrs[b].curr_item() {
            Some(a)
        } else {
            Some(b)
        }
    }

    fn merge(
        &mut self,
        bsw: &mut BlockStreamWriter,
        bsrs: &mut [BlockStreamReader],
        ph: &mut PartHeader,
        prepare_block: Option<&PrepareBlockCallback>,
        stop: Option<&Shutdown>,
        items_merged: &AtomicU64,
    ) -> Result<(), MergeError> {
        // Track the number of merged items locally and propagate the stats to
        // the shared counter about once per second, to minimize expensive
        // inter-CPU synchronization.
        let mut update_stats_deadline = 0u64;
        let mut local_items_merged = 0u64;

        loop {
            let ct = esm_common::fasttime::unix_timestamp();
            if ct > update_stats_deadline {
                items_merged.fetch_add(local_items_merged, Ordering::Relaxed);
                local_items_merged = 0;
                update_stats_deadline = ct + 1;
            }

            if self.heap.is_empty() {
                // Write the last (maybe incomplete) inmemoryBlock to bsw.
                self.flush_ib(bsw, ph, prepare_block, &mut local_items_merged);
                items_merged.fetch_add(local_items_merged, Ordering::Relaxed);
                return Ok(());
            }

            if let Some(stop) = stop {
                if stop.is_stopped() {
                    items_merged.fetch_add(local_items_merged, Ordering::Relaxed);
                    return Err(MergeError::ForciblyStopped);
                }
            }

            let top = self.heap[0];

            let has_next_item = match self.next_reader(bsrs) {
                Some(next_idx) => {
                    self.next_item.clear();
                    let item = bsrs[next_idx].curr_item();
                    self.next_item.extend_from_slice(item);
                    true
                }
                None => false,
            };

            // An optimization which allows skipping the costly comparison for
            // every merged item: if the last item of the top block doesn't
            // exceed the next reader's current item, the whole rest of the
            // block can be copied without comparisons.
            let compare_every_item = {
                let bsr = &bsrs[top];
                let items = &bsr.block.items;
                if bsr.curr_item_idx < items.len() {
                    let last_item = items[items.len() - 1].bytes(&bsr.block.data);
                    has_next_item && last_item > &self.next_item[..]
                } else {
                    true
                }
            };

            let mut next_item_exceeded = false;
            loop {
                let bsr = &bsrs[top];
                let items = &bsr.block.items;
                if bsr.curr_item_idx >= items.len() {
                    break;
                }
                let item = items[bsr.curr_item_idx].bytes(&bsr.block.data);
                if compare_every_item && item > &self.next_item[..] {
                    next_item_exceeded = true;
                    break;
                }
                if !self.ib.add(item) {
                    // The scratch block is full. Flush it to bsw and retry the
                    // same item.
                    Self::flush_ib_parts(
                        &mut self.ib,
                        &mut self.ph_first_item_caught,
                        &mut self.first_item,
                        &mut self.last_item,
                        bsw,
                        ph,
                        prepare_block,
                        &mut local_items_merged,
                    );
                    continue;
                }
                bsrs[top].curr_item_idx += 1;
            }

            if !next_item_exceeded {
                // bsr.block is fully read. Proceed to the next block.
                if bsrs[top].next() {
                    sift_down(bsrs, &mut self.heap, 0);
                    continue;
                }
                if let Some(err) = bsrs[top].error() {
                    items_merged.fetch_add(local_items_merged, Ordering::Relaxed);
                    return Err(MergeError::Other(format!(
                        "cannot read storageBlock: {err}"
                    )));
                }
                heap_pop_top(bsrs, &mut self.heap);
                continue;
            }

            // The next item in bsr.block exceeds nextItem. Return bsr to heap.
            sift_down(bsrs, &mut self.heap, 0);
        }
    }

    fn flush_ib(
        &mut self,
        bsw: &mut BlockStreamWriter,
        ph: &mut PartHeader,
        prepare_block: Option<&PrepareBlockCallback>,
        items_merged: &mut u64,
    ) {
        Self::flush_ib_parts(
            &mut self.ib,
            &mut self.ph_first_item_caught,
            &mut self.first_item,
            &mut self.last_item,
            bsw,
            ph,
            prepare_block,
            items_merged,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn flush_ib_parts(
        ib: &mut InmemoryBlock,
        ph_first_item_caught: &mut bool,
        first_item_buf: &mut Vec<u8>,
        last_item_buf: &mut Vec<u8>,
        bsw: &mut BlockStreamWriter,
        ph: &mut PartHeader,
        prepare_block: Option<&PrepareBlockCallback>,
        items_merged: &mut u64,
    ) {
        if ib.items.is_empty() {
            // Nothing to flush.
            return;
        }
        *items_merged += ib.items.len() as u64;
        if let Some(cb) = prepare_block {
            first_item_buf.clear();
            first_item_buf.extend_from_slice(ib.items[0].bytes(&ib.data));
            last_item_buf.clear();
            last_item_buf.extend_from_slice(ib.items[ib.items.len() - 1].bytes(&ib.data));

            cb(&mut ib.data, &mut ib.items);
            if ib.items.is_empty() {
                // Nothing to flush.
                return;
            }
            // Consistency checks after the prepare_block call.
            let first_item = ib.items[0].bytes(&ib.data);
            assert!(
                first_item >= &first_item_buf[..],
                "BUG: prepareBlock must return the first item bigger or equal to the original first item"
            );
            let last_item = ib.items[ib.items.len() - 1].bytes(&ib.data);
            assert!(
                last_item <= &last_item_buf[..],
                "BUG: prepareBlock must return the last item smaller or equal to the original last item"
            );
            // Verify whether the items are sorted only in debug builds, since
            // this can be an expensive check in prod for items with a long
            // common prefix.
            #[cfg(debug_assertions)]
            assert!(
                ib.is_sorted(),
                "BUG: prepareBlock must return sorted items;\ngot\n{}",
                ib.debug_items_string()
            );
        }
        ph.items_count += ib.items.len() as u64;
        if !*ph_first_item_caught {
            ph.first_item.clear();
            ph.first_item.extend_from_slice(ib.items[0].bytes(&ib.data));
            *ph_first_item_caught = true;
        }
        ph.last_item.clear();
        ph.last_item
            .extend_from_slice(ib.items[ib.items.len() - 1].bytes(&ib.data));
        bsw.write_block(ib);
        ib.reset();
        ph.blocks_count += 1;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::inmemory_block::testutil::Rng;
    use crate::inmemory_part::InmemoryPart;

    pub(crate) fn new_test_inmemory_block_stream_readers(
        r: &mut Rng,
        blocks_count: usize,
        max_items_per_block: usize,
    ) -> (Vec<BlockStreamReader>, Vec<Vec<u8>>) {
        let mut items: Vec<Vec<u8>> = Vec::new();
        let mut bsrs: Vec<BlockStreamReader> = Vec::new();
        for _ in 0..blocks_count {
            let mut ib = InmemoryBlock::default();
            let items_per_block = r.intn(max_items_per_block) + 1;
            for _ in 0..items_per_block {
                let item = r.random_bytes();
                if !ib.add(&item) {
                    break;
                }
                items.push(item);
            }
            let ip = InmemoryPart::init(&mut ib);
            bsrs.push(BlockStreamReader::from_inmemory_part(&ip));
        }
        items.sort();
        (bsrs, items)
    }

    fn test_check_items(dst_ip: &InmemoryPart, items: &[Vec<u8>]) {
        assert_eq!(dst_ip.ph.items_count as usize, items.len());
        assert_eq!(&dst_ip.ph.first_item[..], &items[0][..]);
        assert_eq!(&dst_ip.ph.last_item[..], &items[items.len() - 1][..]);

        let mut dst_items: Vec<Vec<u8>> = Vec::new();
        let mut dst_bsr = BlockStreamReader::from_inmemory_part(dst_ip);
        while dst_bsr.next() {
            let bh = dst_bsr.curr_bh().clone();
            assert_eq!(bh.items_count as usize, dst_bsr.block.items.len());
            assert!(bh.items_count > 0);
            let first = dst_bsr.block.items[0].bytes(&dst_bsr.block.data);
            assert_eq!(&bh.first_item[..], first);
            for it in &dst_bsr.block.items {
                dst_items.push(it.bytes(&dst_bsr.block.data).to_vec());
            }
        }
        assert!(dst_bsr.error().is_none(), "{:?}", dst_bsr.error());
        assert_eq!(dst_items.len(), items.len());
        assert!(dst_items.iter().zip(items.iter()).all(|(a, b)| a == b));
    }

    fn merge_into_inmemory_part(
        bsrs: &mut [BlockStreamReader],
        compress_level: i32,
        items_merged: &AtomicU64,
    ) -> InmemoryPart {
        let mut ph = PartHeader::default();
        let mut bsw = BlockStreamWriter::new_inmemory_part(compress_level);
        let bufs = merge_block_streams(&mut ph, &mut bsw, bsrs, None, None, items_merged)
            .unwrap()
            .unwrap();
        InmemoryPart::from_buffers(ph, bufs)
    }

    fn test_merge_block_streams_serial(
        r: &mut Rng,
        blocks_to_merge: usize,
        max_items_per_block: usize,
    ) {
        let (mut bsrs, items) =
            new_test_inmemory_block_stream_readers(r, blocks_to_merge, max_items_per_block);

        let items_merged = AtomicU64::new(0);
        let dst_ip = merge_into_inmemory_part(&mut bsrs, -4, &items_merged);
        assert_eq!(items_merged.load(Ordering::Relaxed) as usize, items.len());
        test_check_items(&dst_ip, &items);
    }

    #[test]
    fn test_merge_block_streams() {
        for blocks_to_merge in [1usize, 2, 3, 4, 5, 10, 20] {
            for max_items_per_block in [1usize, 2, 10, 100, 1000, 10000] {
                let mut r = Rng::new(1);
                test_merge_block_streams_serial(&mut r, blocks_to_merge, max_items_per_block);
            }
        }
    }

    #[test]
    fn test_merge_block_streams_concurrent() {
        std::thread::scope(|s| {
            for n in 0..3u64 {
                s.spawn(move || {
                    let mut r = Rng::new(n);
                    test_merge_block_streams_serial(&mut r, 5, 1000);
                });
            }
        });
    }

    #[test]
    fn test_multilevel_merge() {
        let mut r = Rng::new(1);

        // Prepare blocks to merge.
        let (mut bsrs, items) = new_test_inmemory_block_stream_readers(&mut r, 10, 4000);
        let items_merged = AtomicU64::new(0);

        // First level merge.
        let (left, right) = bsrs.split_at_mut(5);
        let dst_ip1 = merge_into_inmemory_part(left, -5, &items_merged);
        let dst_ip2 = merge_into_inmemory_part(right, -5, &items_merged);
        assert_eq!(items_merged.load(Ordering::Relaxed) as usize, items.len());

        // Second level merge (aka the final merge).
        items_merged.store(0, Ordering::Relaxed);
        let mut bsrs_top = vec![
            BlockStreamReader::from_inmemory_part(&dst_ip1),
            BlockStreamReader::from_inmemory_part(&dst_ip2),
        ];
        let dst_ip = merge_into_inmemory_part(&mut bsrs_top, 1, &items_merged);
        assert_eq!(items_merged.load(Ordering::Relaxed) as usize, items.len());

        test_check_items(&dst_ip, &items);
    }

    #[test]
    fn test_merge_forcibly_stop() {
        let mut r = Rng::new(1);
        let (mut bsrs, _) = new_test_inmemory_block_stream_readers(&mut r, 20, 4000);
        let mut ph = PartHeader::default();
        let mut bsw = BlockStreamWriter::new_inmemory_part(1);
        let stop = Shutdown::new();
        stop.signal();
        let items_merged = AtomicU64::new(0);
        match merge_block_streams(
            &mut ph,
            &mut bsw,
            &mut bsrs,
            None,
            Some(&stop),
            &items_merged,
        ) {
            Err(MergeError::ForciblyStopped) => {}
            other => panic!("unexpected result during merge: {other:?}"),
        }
        assert_eq!(items_merged.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_merge_with_prepare_block_dedup() {
        // Build several streams containing overlapping items and verify that
        // a dedup prepare_block callback removes duplicates within blocks.
        let mut bsrs = Vec::new();
        let mut all_items: Vec<Vec<u8>> = Vec::new();
        for stream in 0..3 {
            let mut ib = InmemoryBlock::default();
            let _ = stream;
            for i in 0..2000 {
                // Every stream contains the same keys, so the merged stream
                // contains three copies of each key.
                let item = format!("key_{i:05}").into_bytes();
                assert!(ib.add(&item));
                all_items.push(item);
            }
            let ip = InmemoryPart::init(&mut ib);
            bsrs.push(BlockStreamReader::from_inmemory_part(&ip));
        }
        all_items.sort();
        all_items.dedup();

        let dedup: PrepareBlockCallback = Arc::new(|data: &mut Vec<u8>, items: &mut Vec<Item>| {
            let mut result: Vec<Item> = Vec::with_capacity(items.len());
            for &it in items.iter() {
                if let Some(last) = result.last() {
                    if last.bytes(data) == it.bytes(data) {
                        continue;
                    }
                }
                result.push(it);
            }
            *items = result;
        });

        let mut ph = PartHeader::default();
        let mut bsw = BlockStreamWriter::new_inmemory_part(1);
        let items_merged = AtomicU64::new(0);
        let bufs = merge_block_streams(
            &mut ph,
            &mut bsw,
            &mut bsrs,
            Some(&dedup),
            None,
            &items_merged,
        )
        .unwrap()
        .unwrap();
        // items_merged counts the pre-dedup items.
        assert_eq!(items_merged.load(Ordering::Relaxed), 6000);
        let dst_ip = InmemoryPart::from_buffers(ph, bufs);

        // Blocks are deduped independently, so duplicates may remain at block
        // boundaries; verify no duplicates within a block and that all the
        // unique items are present.
        let mut merged: Vec<Vec<u8>> = Vec::new();
        let mut bsr = BlockStreamReader::from_inmemory_part(&dst_ip);
        while bsr.next() {
            let mut prev: Option<Vec<u8>> = None;
            for it in &bsr.block.items {
                let item = it.bytes(&bsr.block.data).to_vec();
                if let Some(p) = &prev {
                    assert!(p < &item, "duplicate or unsorted item within a block");
                }
                prev = Some(item.clone());
                merged.push(item);
            }
        }
        assert!(bsr.error().is_none());
        merged.dedup();
        assert_eq!(merged, all_items);
    }
}
