//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/part_search.go, plus the
//! `BlockRef` block-fetch handle from search.go that stage 5 builds on.
//!
//! PORT-NOTE: the process-global index-block cache (`ibCache`) is deferred —
//! index blocks are decoded into per-search scratch buffers, mirroring
//! esm-mergeset's part_search.

use std::sync::Arc;

use esm_encoding::decompress_zstd;

use crate::block::Block;
use crate::block_header::{unmarshal_block_headers, BlockHeader};
use crate::block_stream::unmarshal_block_data;
use crate::part::Part;
use crate::time_range::TimeRange;
use crate::tsid::Tsid;

/// A reference to a single data block inside a part. Go: BlockRef
/// (search.go).
///
/// The referenced block is fetched lazily with [`BlockRef::read_block`];
/// the block header is available immediately via [`BlockRef::header`].
#[derive(Clone)]
pub struct BlockRef {
    p: Arc<Part>,
    bh: BlockHeader,
}

impl BlockRef {
    fn new(p: Arc<Part>) -> BlockRef {
        BlockRef {
            p,
            bh: BlockHeader::default(),
        }
    }

    /// The header of the referenced block.
    pub fn header(&self) -> &BlockHeader {
        &self.bh
    }

    /// The number of rows in the referenced block. Go: BlockRef.RowsCount.
    pub fn rows_count(&self) -> usize {
        self.bh.rows_count as usize
    }

    /// The part the block lives in.
    pub fn part(&self) -> &Arc<Part> {
        &self.p
    }

    /// Reads the referenced block into `dst`.
    ///
    /// Go: BlockRef.MustReadBlock followed by Block.UnmarshalData — see the
    /// PORT-NOTE on `block_stream::unmarshal_block_data`: `dst` is returned
    /// already unpacked, so a subsequent `Block::unmarshal_data` call is a
    /// no-op.
    pub fn read_block(&self, dst: &mut Block) -> Result<(), String> {
        let mut timestamps_data = vec![0u8; self.bh.timestamps_block_size as usize];
        self.p
            .timestamps_file
            .must_read_at(&mut timestamps_data, self.bh.timestamps_block_offset);

        let mut values_data = vec![0u8; self.bh.values_block_size as usize];
        self.p
            .values_file
            .must_read_at(&mut values_data, self.bh.values_block_offset);

        unmarshal_block_data(dst, &self.bh, &timestamps_data, &values_data)
    }
}

#[derive(Debug, Clone)]
enum PsError {
    Eof,
    Other(String),
}

/// A stream of blocks for the given `(tsids, tr)` search args over a single
/// part. Go: partSearch.
pub struct PartSearch {
    /// Reference to the found block after a successful `next_block` call.
    block_ref: BlockRef,

    /// The part to search.
    p: Arc<Part>,

    /// Sorted tsids to search; empty when the part doesn't overlap `tr`.
    tsids: Arc<Vec<Tsid>>,

    /// Index of the next tsid to use from `tsids`.
    tsid_idx: usize,

    /// The tsid currently being searched (Go keeps it in BlockRef.bh.TSID).
    cur_tsid: Tsid,

    /// The time range to search.
    tr: TimeRange,

    /// Index of the next metaindex row to process in `p.metaindex`.
    metaindex_idx: usize,

    /// Block headers of the current index block.
    bhs: Option<std::sync::Arc<crate::part::IndexBlock>>,
    /// Index of the next block header to process in `bhs`.
    bh_idx: usize,

    compressed_index_buf: Vec<u8>,
    index_buf: Vec<u8>,

    err: Option<PsError>,
}

impl PartSearch {
    /// Creates a search over `p` for the given sorted `tsids` within `tr`.
    /// Go: partSearch.Init.
    ///
    /// `tsids` must be sorted; it is shared, not copied.
    pub fn new(p: Arc<Part>, tsids: Arc<Vec<Tsid>>, tr: TimeRange) -> PartSearch {
        let use_tsids =
            p.ph.min_timestamp <= tr.max_timestamp && p.ph.max_timestamp >= tr.min_timestamp;
        if use_tsids {
            debug_assert!(
                tsids.is_sorted(),
                "BUG: tsids must be sorted; got {tsids:?}"
            );
        }
        let mut ps = PartSearch {
            block_ref: BlockRef::new(Arc::clone(&p)),
            p,
            tsids: if use_tsids {
                tsids
            } else {
                Arc::new(Vec::new())
            },
            tsid_idx: 0,
            cur_tsid: Tsid::default(),
            tr,
            metaindex_idx: 0,
            bhs: None,
            bh_idx: 0,
            compressed_index_buf: Vec::new(),
            index_buf: Vec::new(),
            err: None,
        };

        // Advance to the first tsid. There is no need in checking the
        // returned result, since it will be checked in next_block.
        ps.next_tsid();
        ps
    }

    /// The reference to the block found by the last successful `next_block`
    /// call.
    pub fn block_ref(&self) -> &BlockRef {
        &self.block_ref
    }

    /// Advances to the next block reference. Returns true on success.
    ///
    /// The blocks are sorted by (TSID, MinTimestamp). Two subsequent blocks
    /// for the same TSID may contain overlapped time ranges.
    /// Go: partSearch.NextBlock.
    pub fn next_block(&mut self) -> bool {
        loop {
            if self.err.is_some() {
                return false;
            }
            if self.bh_idx >= self.bhs().len() && !self.next_bhs() {
                return false;
            }
            if self.search_bhs() {
                return true;
            }
        }
    }

    /// Returns the last error, ignoring EOF. Go: partSearch.Error.
    pub fn error(&self) -> Option<String> {
        match &self.err {
            Some(PsError::Other(msg)) => Some(msg.clone()),
            _ => None,
        }
    }

    /// Go: partSearch.nextTSID.
    fn next_tsid(&mut self) -> bool {
        if self.tsid_idx >= self.tsids.len() {
            self.err = Some(PsError::Eof);
            return false;
        }
        self.cur_tsid = self.tsids[self.tsid_idx];
        self.tsid_idx += 1;
        true
    }

    /// Go: partSearch.skipTSIDsSmallerThan.
    fn skip_tsids_smaller_than(&mut self, tsid: &Tsid) -> bool {
        if self.cur_tsid >= *tsid {
            return true;
        }
        if !self.next_tsid() {
            return false;
        }
        if self.cur_tsid >= *tsid {
            // Fast path: the next TSID isn't smaller than the tsid.
            return true;
        }

        // Slower path - binary search for the next TSID which isn't smaller
        // than the tsid.
        let tail = &self.tsids[self.tsid_idx..];
        self.tsid_idx += tail.partition_point(|t| t < tsid);
        if self.tsid_idx >= self.tsids.len() {
            self.tsid_idx = self.tsids.len();
            self.err = Some(PsError::Eof);
            return false;
        }
        self.cur_tsid = self.tsids[self.tsid_idx];
        self.tsid_idx += 1;
        true
    }

    /// Go: partSearch.nextBHS.
    fn next_bhs(&mut self) -> bool {
        let p = Arc::clone(&self.p);
        while self.metaindex_idx < p.metaindex.len() {
            // Optimization: skip tsid values smaller than the minimum value
            // of the remaining metaindex rows.
            if !self.skip_tsids_smaller_than(&p.metaindex[self.metaindex_idx].tsid) {
                return false;
            }
            // Invariant: cur_tsid >= metaindex[metaindex_idx].TSID

            self.metaindex_idx =
                skip_small_metaindex_rows(&p.metaindex, self.metaindex_idx, &self.cur_tsid);
            // Invariant: metaindex_idx < len && cur_tsid >= metaindex[metaindex_idx].TSID

            let mr = &p.metaindex[self.metaindex_idx];
            self.metaindex_idx += 1;
            assert!(
                self.cur_tsid >= mr.tsid,
                "BUG: invariant violation: cur_tsid cannot be smaller than mr.tsid; got {:?} vs {:?}",
                self.cur_tsid,
                mr.tsid
            );

            if mr.max_timestamp < self.tr.min_timestamp {
                // Skip mr with too small timestamps.
                continue;
            }
            if mr.min_timestamp > self.tr.max_timestamp {
                // Skip mr with too big timestamps.
                continue;
            }

            // Found the index block which may contain the required data for
            // the cur_tsid and the given timestamp range. Consult the
            // process-global index-block cache first (Go: ibCache in
            // part.go / readIndexBlock).
            let key = esm_mergeset::blockcache::Key {
                part_id: p.id,
                offset: mr.index_block_offset,
            };
            if let Some(idxb) = crate::part::idxb_cache().get_block(&key) {
                self.bhs = Some(idxb);
                self.bh_idx = 0;
                return true;
            }

            self.compressed_index_buf
                .resize(mr.index_block_size as usize, 0);
            p.index_file
                .must_read_at(&mut self.compressed_index_buf, mr.index_block_offset);

            self.index_buf.clear();
            if let Err(err) = decompress_zstd(&mut self.index_buf, &self.compressed_index_buf) {
                self.err = Some(PsError::Other(format!(
                    "cannot read index block for part {:?} at offset {} with size {}: cannot \
                     decompress index block: {err}",
                    p.ph, mr.index_block_offset, mr.index_block_size
                )));
                return false;
            }
            let mut bhs = Vec::new();
            if let Err(err) =
                unmarshal_block_headers(&mut bhs, &self.index_buf, mr.block_headers_count as usize)
            {
                self.err = Some(PsError::Other(format!(
                    "cannot read index block for part {:?} at offset {} with size {}: cannot \
                     unmarshal index block: {err}",
                    p.ph, mr.index_block_offset, mr.index_block_size
                )));
                return false;
            }
            let idxb = std::sync::Arc::new(crate::part::IndexBlock { bhs });
            crate::part::idxb_cache().try_put_block(&key, &idxb);
            self.bhs = Some(idxb);
            self.bh_idx = 0;
            return true;
        }

        // No more metaindex rows to search.
        self.err = Some(PsError::Eof);
        false
    }

    #[inline]
    fn bhs(&self) -> &[BlockHeader] {
        match &self.bhs {
            Some(idxb) => &idxb.bhs,
            None => &[],
        }
    }

    /// Go: partSearch.searchBHS.
    fn search_bhs(&mut self) -> bool {
        while self.bh_idx < self.bhs().len() {
            // Skip block headers with tsids smaller than the current tsid.
            if self.bhs()[self.bh_idx].tsid < self.cur_tsid {
                let tsid = self.cur_tsid;
                let n = self.bhs()[self.bh_idx..].partition_point(|bh| bh.tsid < tsid);
                if self.bh_idx + n == self.bhs().len() {
                    // Nothing found.
                    break;
                }
                self.bh_idx += n;
            }
            let bh = self.bhs()[self.bh_idx];

            // Invariant: cur_tsid <= bh.tsid

            if bh.tsid.metric_id != self.cur_tsid.metric_id {
                // cur_tsid < bh.tsid: no more blocks with the given tsid.
                // Proceed to the next (bigger) tsid.
                if !self.skip_tsids_smaller_than(&bh.tsid) {
                    return false;
                }
                continue;
            }

            // Found the block with the given tsid. Verify the timestamp
            // range. While blocks for the same TSID are sorted by
            // MinTimestamp, they may contain overlapped time ranges. So use
            // linear search instead of binary search.
            if bh.max_timestamp < self.tr.min_timestamp {
                // Skip the block with too small timestamps.
                self.bh_idx += 1;
                continue;
            }
            if bh.min_timestamp > self.tr.max_timestamp {
                // Proceed to the next tsid, since the remaining blocks for
                // the current tsid contain too big timestamps.
                if !self.next_tsid() {
                    return false;
                }
                continue;
            }

            // Found the tsid block with the matching timestamp range.
            self.block_ref.bh = bh;
            self.bh_idx += 1;
            return true;
        }
        self.bhs = None;
        self.bh_idx = 0;
        false
    }
}

/// Go: skipSmallMetaindexRows. Returns the index of the first metaindex row
/// to scan for the given tsid, starting the search at `start`.
fn skip_small_metaindex_rows(
    metaindex: &[crate::metaindex_row::MetaindexRow],
    start: usize,
    tsid: &Tsid,
) -> usize {
    // Invariant: start < len && tsid >= metaindex[start].TSID.
    assert!(
        *tsid >= metaindex[start].tsid,
        "BUG: invariant violation: tsid cannot be smaller than metaindex[start]; got {:?} vs {:?}",
        tsid,
        metaindex[start].tsid
    );

    if tsid.metric_id == metaindex[start].tsid.metric_id {
        return start;
    }

    // Invariant: tsid > metaindex[start].TSID, so partition_point cannot
    // return 0.
    let n = metaindex[start..].partition_point(|mr| mr.tsid < *tsid);
    assert!(
        n > 0,
        "BUG: invariant violation: binary search returned 0 for tsid > metaindex[start].TSID"
    );

    // The given tsid may be located in the previous metaindex row, so go to
    // the previous row. Suppose the following metaindex rows exist
    // [tsid10, tsid20, tsid30]. The following table contains the
    // corresponding rows to start the search at for tsid values greater
    // than tsid10:
    //
    //   * tsid11 -> tsid10
    //   * tsid20 -> tsid10, since tsid20 items may be in [tsid10...tsid20]
    //   * tsid21 -> tsid20
    //   * tsid30 -> tsid20
    //   * tsid99 -> tsid30, since tsid99 items may be in [tsid30...tsidInf]
    start + n - 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::part::inmemory::InmemoryPart;
    use crate::raw_row::{RawBlock, RawRow};
    use crate::util::splitmix64;
    use esm_common::decimal;

    fn rand_below(state: &mut u64, n: u64) -> u64 {
        splitmix64(state) % n
    }

    // Signed integer-valued sample in roughly [-3n, 3n].
    fn rand_signed_int(state: &mut u64, n: u64) -> i64 {
        rand_below(state, 2 * n) as i64
            + rand_below(state, 2 * n) as i64
            + rand_below(state, 2 * n) as i64
            - 3 * n as i64
    }

    fn tsid(metric_id: u64) -> Tsid {
        Tsid {
            metric_id,
            ..Default::default()
        }
    }

    // Port of newTestPart.
    fn new_test_part(rows: &mut [RawRow]) -> Arc<Part> {
        let mp = InmemoryPart::init_from_rows(rows);
        Arc::new(mp.new_part())
    }

    // Port of getTestExpectedRawBlocks.
    fn get_test_expected_raw_blocks(
        rows_original: &[RawRow],
        tsids: &[Tsid],
        tr: TimeRange,
    ) -> Vec<RawBlock> {
        if rows_original.is_empty() {
            return Vec::new();
        }

        let mut rows = rows_original.to_vec();
        rows.sort_by(|a, b| {
            a.tsid
                .cmp(&b.tsid)
                .then(a.timestamp.cmp(&b.timestamp))
                .then(a.value.partial_cmp(&b.value).unwrap())
        });

        let tsids_set: std::collections::HashSet<Tsid> = tsids.iter().copied().collect();

        let mut expected: Vec<RawBlock> = Vec::new();
        let mut rb = RawBlock {
            tsid: rows[0].tsid,
            ..Default::default()
        };
        let mut rows_per_block = 0usize;
        for r in &rows {
            if r.tsid.metric_id != rb.tsid.metric_id
                || rows_per_block >= crate::block::MAX_ROWS_PER_BLOCK
            {
                if tsids_set.contains(&rb.tsid) && !rb.timestamps.is_empty() {
                    let mut tmp = RawBlock::default();
                    tmp.copy_from(&rb);
                    expected.push(tmp);
                }
                rb.reset();
                rb.tsid = r.tsid;
                rows_per_block = 0;
            }
            rows_per_block += 1;
            if r.timestamp < tr.min_timestamp || r.timestamp > tr.max_timestamp {
                continue;
            }
            rb.timestamps.push(r.timestamp);
            rb.values.push(r.value);
        }
        if tsids_set.contains(&rb.tsid) && !rb.timestamps.is_empty() {
            expected.push(rb);
        }
        expected
    }

    // Port of newTestRawBlock: converts a found block into a RawBlock,
    // trimming samples outside tr.
    fn new_test_raw_block(b: &Block, tr: TimeRange) -> RawBlock {
        let mut rb = RawBlock {
            tsid: b.header().tsid,
            ..Default::default()
        };
        let mut values = Vec::new();
        for (&timestamp, &value) in b.timestamps().iter().zip(b.values().iter()) {
            if timestamp < tr.min_timestamp {
                continue;
            }
            if timestamp > tr.max_timestamp {
                break;
            }
            rb.timestamps.push(timestamp);
            values.push(value);
        }
        decimal::append_decimal_to_float(&mut rb.values, &values, b.header().scale);
        rb
    }

    // Port of newTestMergeRawBlocks: merges per-metric blocks and sorts the
    // samples by (timestamp, value).
    fn merge_raw_blocks(src: &[RawBlock]) -> Vec<RawBlock> {
        let mut dst = Vec::new();
        if src.is_empty() {
            return dst;
        }
        let sort_rb = |rb: &mut RawBlock| {
            let mut idx: Vec<usize> = (0..rb.timestamps.len()).collect();
            idx.sort_by(|&i, &j| {
                rb.timestamps[i]
                    .cmp(&rb.timestamps[j])
                    .then(rb.values[i].partial_cmp(&rb.values[j]).unwrap())
            });
            rb.timestamps = idx.iter().map(|&i| rb.timestamps[i]).collect();
            rb.values = idx.iter().map(|&i| rb.values[i]).collect();
        };
        let mut rb = RawBlock {
            tsid: src[0].tsid,
            ..Default::default()
        };
        for s in src {
            if s.tsid.metric_id != rb.tsid.metric_id {
                sort_rb(&mut rb);
                dst.push(std::mem::take(&mut rb));
                rb.tsid = s.tsid;
            }
            rb.timestamps.extend_from_slice(&s.timestamps);
            rb.values.extend_from_slice(&s.values);
        }
        sort_rb(&mut rb);
        dst.push(rb);
        dst
    }

    // Port of testEqualRawBlocks.
    fn check_equal_raw_blocks(got: &[RawBlock], want: &[RawBlock]) -> Result<(), String> {
        let a = merge_raw_blocks(got);
        let b = merge_raw_blocks(want);
        if a.len() != b.len() {
            return Err(format!(
                "blocks length mismatch: got {}; want {}",
                a.len(),
                b.len()
            ));
        }
        for (i, (rb1, rb2)) in a.iter().zip(b.iter()).enumerate() {
            if rb1 != rb2 {
                return Err(format!(
                    "blocks mismatch on position {i} out of {}; got\n{rb1:?}; want\n{rb2:?}",
                    a.len()
                ));
            }
        }
        Ok(())
    }

    // Port of testPartSearchSerial.
    fn check_part_search_serial(
        p: &Arc<Part>,
        tsids: &Arc<Vec<Tsid>>,
        tr: TimeRange,
        expected: &[RawBlock],
    ) -> Result<(), String> {
        let mut ps = PartSearch::new(Arc::clone(p), Arc::clone(tsids), tr);
        let mut rbs: Vec<RawBlock> = Vec::new();
        while ps.next_block() {
            let mut b = Block::default();
            ps.block_ref().read_block(&mut b)?;
            let rb = new_test_raw_block(&b, tr);
            if !rb.values.is_empty() {
                rbs.push(rb);
            }
        }
        if let Some(err) = ps.error() {
            return Err(format!("unexpected error in search: {err}"));
        }
        check_equal_raw_blocks(&rbs, expected).map_err(|err| format!("unequal blocks: {err}"))
    }

    // Port of testPartSearch (serial + concurrent).
    fn check_part_search(p: &Arc<Part>, tsids: Vec<Tsid>, tr: TimeRange, expected: &[RawBlock]) {
        let tsids = Arc::new(tsids);
        check_part_search_serial(p, &tsids, tr, expected)
            .unwrap_or_else(|err| panic!("unexpected error in serial part search: {err}"));

        std::thread::scope(|s| {
            let handles: Vec<_> = (0..3)
                .map(|_| s.spawn(|| check_part_search_serial(p, &tsids, tr, expected)))
                .collect();
            for h in handles {
                h.join().unwrap().unwrap_or_else(|err| {
                    panic!("unexpected error in concurrent part search: {err}")
                });
            }
        });
    }

    // Port of TestPartSearchOneRow (condensed matrix).
    #[test]
    fn part_search_one_row() {
        let mut rows = [RawRow {
            tsid: tsid(1234),
            timestamp: 100,
            value: 345.0,
            precision_bits: 64,
        }];
        let p = new_test_part(&mut rows);

        let outer = TimeRange {
            min_timestamp: -1000,
            max_timestamp: 300,
        };
        let exact = TimeRange {
            min_timestamp: 100,
            max_timestamp: 100,
        };
        let lower = TimeRange {
            min_timestamp: -2_000_000,
            max_timestamp: -1_000_000,
        };
        let higher = TimeRange {
            min_timestamp: 1_000_000,
            max_timestamp: 2_000_000,
        };

        // Non-matching TSID sets yield no blocks for any time range.
        let non_matching: [Vec<Tsid>; 4] = [
            vec![],
            vec![tsid(10)],
            vec![tsid(12345), tsid(12346)],
            vec![tsid(10), tsid(20), tsid(12345), tsid(12346)],
        ];
        for tsids in non_matching {
            for tr in [outer, exact, lower, higher] {
                check_part_search(&p, tsids.clone(), tr, &[]);
            }
        }

        // Matching TSID.
        let rbs = vec![RawBlock {
            tsid: tsid(1234),
            timestamps: vec![100],
            values: vec![345.0],
        }];
        let matching: [Vec<Tsid>; 4] = [
            vec![tsid(1234)],
            vec![tsid(1234), tsid(12345)],
            vec![tsid(10), tsid(1234)],
            vec![tsid(10), tsid(1234), tsid(12345)],
        ];
        for tsids in matching {
            check_part_search(&p, tsids.clone(), outer, &rbs);
            check_part_search(&p, tsids.clone(), exact, &rbs);
            check_part_search(&p, tsids.clone(), lower, &[]);
            check_part_search(&p, tsids.clone(), higher, &[]);
        }
    }

    // Port of TestPartSearchMultiRowsOneTSID.
    #[test]
    fn part_search_multi_rows_one_tsid() {
        let mut rows_count = 1usize;
        while rows_count <= 10_000 {
            check_part_search_multi_rows_one_tsid(rows_count);
            rows_count *= 10;
        }
    }

    fn check_part_search_multi_rows_one_tsid(rows_count: usize) {
        let mut state = 1u64;
        let mut rows: Vec<RawRow> = (0..rows_count)
            .map(|_| RawRow {
                tsid: tsid(1111),
                timestamp: rand_signed_int(&mut state, 1_000_000),
                value: rand_signed_int(&mut state, 100_000) as f64,
                precision_bits: 64,
            })
            .collect();

        let tsids = vec![tsid(1111)];
        let tr = TimeRange {
            min_timestamp: -100_000,
            max_timestamp: 100_000,
        };
        let expected = get_test_expected_raw_blocks(&rows, &tsids, tr);
        let p = new_test_part(&mut rows);
        check_part_search(&p, tsids, tr, &expected);
    }

    // Port of TestPartSearchMultiRowsMultiTSIDs.
    #[test]
    fn part_search_multi_rows_multi_tsids() {
        let mut rows_count = 1usize;
        while rows_count <= 10_000 {
            let mut tsids_count = 1usize;
            while tsids_count <= rows_count {
                check_part_search_multi_rows_multi_tsids(rows_count, tsids_count);
                tsids_count *= 10;
            }
            rows_count *= 10;
        }
    }

    fn check_part_search_multi_rows_multi_tsids(rows_count: usize, tsids_count: usize) {
        let mut state = 2u64;
        let mut rows: Vec<RawRow> = (0..rows_count)
            .map(|_| RawRow {
                tsid: tsid(rand_below(&mut state, tsids_count as u64)),
                timestamp: rand_signed_int(&mut state, 1_000_000),
                value: rand_signed_int(&mut state, 100_000) as f64,
                precision_bits: 64,
            })
            .collect();

        let mut tsids: Vec<Tsid> = (0..100)
            .map(|_| tsid(rand_below(&mut state, tsids_count as u64 * 3)))
            .collect();
        tsids.sort();
        let tr = TimeRange {
            min_timestamp: -100_000,
            max_timestamp: 100_000,
        };
        let expected = get_test_expected_raw_blocks(&rows, &tsids, tr);
        let p = new_test_part(&mut rows);
        check_part_search(&p, tsids, tr, &expected);
    }
}
