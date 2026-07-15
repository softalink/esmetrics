//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/merge_test.go
//! (block stream merging), plus retention/dmis/dedup merge tests.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use esm_common::uint64set;
use esm_storage::block_stream::{
    merge_block_streams, BlockStreamReader, BlockStreamWriter, MergeError,
};
use esm_storage::part::{InmemoryPart, PartHeader};
use esm_storage::{RawRow, Tsid, MAX_ROWS_PER_BLOCK};

/// `merge_block_streams` consults the process-global dedup interval.
/// Tests here either mutate it or assert row counts that assume it is 0,
/// and the test harness runs them on parallel threads — so every merge
/// test serializes on this lock (a poisoned lock is fine to reuse).
static DEDUP_INTERVAL_LOCK: Mutex<()> = Mutex::new(());

fn dedup_interval_guard() -> std::sync::MutexGuard<'static, ()> {
    DEDUP_INTERVAL_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// splitmix64 PRNG (same generator the in-crate tests use).
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

const DEFAULT_PRECISION_BITS: u8 = 4;

fn rand_below(state: &mut u64, n: u64) -> u64 {
    splitmix64(state) % n
}

fn rand_tsid(state: &mut u64) -> Tsid {
    Tsid {
        metric_group_id: splitmix64(state),
        job_id: splitmix64(state) as u32,
        instance_id: splitmix64(state) as u32,
        metric_id: splitmix64(state),
    }
}

fn new_test_block_stream_reader(rows: &mut [RawRow]) -> BlockStreamReader {
    let mp = InmemoryPart::init_from_rows(rows);
    BlockStreamReader::from_inmemory_part(&mp)
}

// Port of testMergeBlockStreams.
fn check_merge_block_streams(
    mut bsrs: Vec<BlockStreamReader>,
    expected_blocks_count: usize,
    expected_rows_count: usize,
    expected_min_timestamp: i64,
    expected_max_timestamp: i64,
) {
    let mut ph = PartHeader::default();
    let mut bsw = BlockStreamWriter::new_inmemory_part(-5);
    let rows_merged = AtomicU64::new(0);
    let rows_deleted = AtomicU64::new(0);
    let bufs = merge_block_streams(
        &mut ph,
        &mut bsw,
        &mut bsrs,
        None,
        None,
        0,
        &rows_merged,
        &rows_deleted,
    )
    .expect("unexpected error in merge_block_streams")
    .expect("in-memory merge must return buffers");

    // Verify written data.
    assert_eq!(
        ph.rows_count, expected_rows_count as u64,
        "unexpected rows count in partHeader"
    );
    assert_eq!(
        rows_merged.load(Ordering::Relaxed),
        ph.rows_count,
        "unexpected rowsMerged"
    );
    assert_eq!(
        rows_deleted.load(Ordering::Relaxed),
        0,
        "unexpected rowsDeleted"
    );
    assert_eq!(
        ph.min_timestamp, expected_min_timestamp,
        "unexpected MinTimestamp in partHeader"
    );
    assert_eq!(
        ph.max_timestamp, expected_max_timestamp,
        "unexpected MaxTimestamp in partHeader"
    );

    let mp = InmemoryPart::from_buffers(ph, bufs);
    let mut bsr1 = BlockStreamReader::from_inmemory_part(&mp);
    let mut blocks_count = 0usize;
    let mut rows_count = 0usize;
    let mut prev_tsid = Tsid::default();
    while bsr1.next_block() {
        let bh = *bsr1.block.header();
        assert!(
            bh.tsid >= prev_tsid,
            "the next block cannot have smaller TSID than the previous block"
        );
        prev_tsid = bh.tsid;

        let expected_rows_per_block = bh.rows_count as usize;
        assert!(expected_rows_per_block > 0, "got zero rows in a block");
        assert!(
            bh.min_timestamp >= expected_min_timestamp,
            "too small MinTimestamp in the blockHeader; got {}",
            bh.min_timestamp
        );
        assert!(
            bh.max_timestamp <= expected_max_timestamp,
            "too big MaxTimestamp in the blockHeader; got {}",
            bh.max_timestamp
        );

        bsr1.block
            .unmarshal_data()
            .expect("cannot unmarshal block from merged stream");

        let mut prev_timestamp = bh.min_timestamp;
        let mut rows_per_block = 0usize;
        for &timestamp in bsr1.block.timestamps() {
            assert!(
                timestamp >= prev_timestamp,
                "the next timestamp cannot be smaller than the previous one; \
                 got {timestamp} vs {prev_timestamp}"
            );
            prev_timestamp = timestamp;
            rows_per_block += 1;
        }
        assert!(
            prev_timestamp <= bh.max_timestamp,
            "the last timestamp cannot be bigger than MaxTimestamp in the blockHeader"
        );
        assert_eq!(
            rows_per_block, expected_rows_per_block,
            "unexpected rows read in the block"
        );
        rows_count += rows_per_block;
        blocks_count += 1;
    }
    assert_eq!(bsr1.error(), None);
    assert_eq!(
        blocks_count, expected_blocks_count,
        "unexpected blocks read from merged stream"
    );
    assert_eq!(
        rows_count, expected_rows_count,
        "unexpected rows read from merged stream"
    );
}

// Port of TestMergeBlockStreamsOneStreamOneRow.
#[test]
fn one_stream_one_row() {
    let _dedup_guard = dedup_interval_guard();
    let mut rows = [RawRow {
        timestamp: 82394327423432,
        value: 123.42389,
        precision_bits: DEFAULT_PRECISION_BITS,
        ..Default::default()
    }];
    let ts = rows[0].timestamp;
    let bsr = new_test_block_stream_reader(&mut rows);
    check_merge_block_streams(vec![bsr], 1, 1, ts, ts);
}

// Port of TestMergeBlockStreamsOneStreamOneBlockManyRows.
#[test]
fn one_stream_one_block_many_rows() {
    let _dedup_guard = dedup_interval_guard();
    let mut state = 1u64;
    let mut min_timestamp = i64::MAX;
    let mut max_timestamp = i64::MIN;
    let mut rows: Vec<RawRow> = (0..MAX_ROWS_PER_BLOCK)
        .map(|_| {
            let timestamp = rand_below(&mut state, 1_000_000_000) as i64;
            min_timestamp = min_timestamp.min(timestamp);
            max_timestamp = max_timestamp.max(timestamp);
            RawRow {
                timestamp,
                value: rand_below(&mut state, 4_000_000) as f64 - 2_000_000.0,
                precision_bits: DEFAULT_PRECISION_BITS,
                ..Default::default()
            }
        })
        .collect();
    let bsr = new_test_block_stream_reader(&mut rows);
    check_merge_block_streams(
        vec![bsr],
        1,
        MAX_ROWS_PER_BLOCK,
        min_timestamp,
        max_timestamp,
    );
}

// Port of TestMergeBlockStreamsOneStreamManyBlocksOneRow.
#[test]
fn one_stream_many_blocks_one_row() {
    let _dedup_guard = dedup_interval_guard();
    let mut state = 1u64;
    const BLOCKS_COUNT: usize = 1234;
    let mut min_timestamp = i64::MAX;
    let mut max_timestamp = i64::MIN;
    let mut rows: Vec<RawRow> = (0..BLOCKS_COUNT)
        .map(|i| {
            let mut tsid = rand_tsid(&mut state);
            tsid.metric_id = (i * 123) as u64;
            let timestamp = rand_below(&mut state, 1_000_000_000) as i64;
            min_timestamp = min_timestamp.min(timestamp);
            max_timestamp = max_timestamp.max(timestamp);
            RawRow {
                tsid,
                timestamp,
                value: rand_below(&mut state, 4_000_000) as f64 - 2_000_000.0,
                precision_bits: DEFAULT_PRECISION_BITS,
            }
        })
        .collect();
    let bsr = new_test_block_stream_reader(&mut rows);
    check_merge_block_streams(
        vec![bsr],
        BLOCKS_COUNT,
        BLOCKS_COUNT,
        min_timestamp,
        max_timestamp,
    );
}

// Port of TestMergeBlockStreamsOneStreamManyBlocksManyRows.
#[test]
fn one_stream_many_blocks_many_rows() {
    let _dedup_guard = dedup_interval_guard();
    let mut state = 1u64;
    let tsid_base = rand_tsid(&mut state);
    const BLOCKS_COUNT: usize = 1234;
    const ROWS_COUNT: usize = 4938;
    let mut min_timestamp = i64::MAX;
    let mut max_timestamp = i64::MIN;
    let mut rows: Vec<RawRow> = (0..ROWS_COUNT)
        .map(|i| {
            let mut tsid = tsid_base;
            tsid.metric_id = (i % BLOCKS_COUNT) as u64;
            let timestamp = rand_below(&mut state, 1_000_000_000) as i64;
            min_timestamp = min_timestamp.min(timestamp);
            max_timestamp = max_timestamp.max(timestamp);
            RawRow {
                tsid,
                timestamp,
                value: rand_below(&mut state, 4_000_000) as f64 - 2_000_000.0,
                precision_bits: DEFAULT_PRECISION_BITS,
            }
        })
        .collect();
    let bsr = new_test_block_stream_reader(&mut rows);
    check_merge_block_streams(
        vec![bsr],
        BLOCKS_COUNT,
        ROWS_COUNT,
        min_timestamp,
        max_timestamp,
    );
}

// Port of TestMergeBlockStreamsTwoStreamsOneBlockTwoRows.
#[test]
fn two_streams_one_block_two_rows() {
    let _dedup_guard = dedup_interval_guard();
    // Identical rows.
    let mut rows = [RawRow {
        timestamp: 182394327423432,
        value: 3123.42389,
        precision_bits: DEFAULT_PRECISION_BITS,
        ..Default::default()
    }];
    let ts = rows[0].timestamp;
    let bsr1 = new_test_block_stream_reader(&mut rows.clone());
    let bsr2 = new_test_block_stream_reader(&mut rows);
    check_merge_block_streams(vec![bsr1, bsr2], 1, 2, ts, ts);

    // Distinct rows for the same TSID.
    let min_timestamp = 12332443i64;
    let max_timestamp = 23849834543i64;
    let mut rows = [RawRow {
        timestamp: max_timestamp,
        value: 3123.42389,
        precision_bits: DEFAULT_PRECISION_BITS,
        ..Default::default()
    }];
    let bsr1 = new_test_block_stream_reader(&mut rows);
    let mut rows = [RawRow {
        timestamp: min_timestamp,
        value: 23.42389,
        precision_bits: DEFAULT_PRECISION_BITS,
        ..Default::default()
    }];
    let bsr2 = new_test_block_stream_reader(&mut rows);
    check_merge_block_streams(vec![bsr1, bsr2], 1, 2, min_timestamp, max_timestamp);
}

// Port of TestMergeBlockStreamsTwoStreamsTwoBlocksOneRow.
#[test]
fn two_streams_two_blocks_one_row() {
    let _dedup_guard = dedup_interval_guard();
    let min_timestamp = 4389345i64;
    let max_timestamp = 8394584354i64;

    let mut rows = [RawRow {
        tsid: Tsid {
            metric_id: 8454,
            ..Default::default()
        },
        timestamp: min_timestamp,
        value: 33.42389,
        precision_bits: DEFAULT_PRECISION_BITS,
    }];
    let bsr1 = new_test_block_stream_reader(&mut rows);

    let mut rows = [RawRow {
        tsid: Tsid {
            metric_id: 4454,
            ..Default::default()
        },
        timestamp: max_timestamp,
        value: 323.42389,
        precision_bits: DEFAULT_PRECISION_BITS,
    }];
    let bsr2 = new_test_block_stream_reader(&mut rows);

    check_merge_block_streams(vec![bsr1, bsr2], 2, 2, min_timestamp, max_timestamp);
}

// Port of TestMergeBlockStreamsTwoStreamsManyBlocksManyRows.
#[test]
fn two_streams_many_blocks_many_rows() {
    let _dedup_guard = dedup_interval_guard();
    const BLOCKS_COUNT: usize = 1234;
    let mut min_timestamp = i64::MAX;
    let mut max_timestamp = i64::MIN;

    let mut state = 1u64;
    let tsid_base = rand_tsid(&mut state);
    const ROWS_COUNT1: usize = 4938;
    let mut rows: Vec<RawRow> = (0..ROWS_COUNT1)
        .map(|i| {
            let mut tsid = tsid_base;
            tsid.metric_id = (i % BLOCKS_COUNT) as u64;
            let timestamp = rand_below(&mut state, 1_000_000_000) as i64;
            min_timestamp = min_timestamp.min(timestamp);
            max_timestamp = max_timestamp.max(timestamp);
            RawRow {
                tsid,
                timestamp,
                value: rand_below(&mut state, 4_000_000) as f64 - 2_000_000.0,
                precision_bits: 2,
            }
        })
        .collect();
    let bsr1 = new_test_block_stream_reader(&mut rows);

    const ROWS_COUNT2: usize = 3281;
    let mut rows: Vec<RawRow> = (0..ROWS_COUNT2)
        .map(|i| {
            let mut tsid = tsid_base;
            tsid.metric_id = ((i + 17) % BLOCKS_COUNT) as u64;
            let timestamp = rand_below(&mut state, 1_000_000_000) as i64;
            min_timestamp = min_timestamp.min(timestamp);
            max_timestamp = max_timestamp.max(timestamp);
            RawRow {
                tsid,
                timestamp,
                value: rand_below(&mut state, 4_000_000) as f64 - 2_000_000.0,
                precision_bits: 2,
            }
        })
        .collect();
    let bsr2 = new_test_block_stream_reader(&mut rows);

    check_merge_block_streams(
        vec![bsr1, bsr2],
        BLOCKS_COUNT,
        ROWS_COUNT1 + ROWS_COUNT2,
        min_timestamp,
        max_timestamp,
    );
}

// Port of TestMergeBlockStreamsTwoStreamsBigOverlappingBlocks.
#[test]
fn two_streams_big_overlapping_blocks() {
    let _dedup_guard = dedup_interval_guard();
    let mut state = 1u64;
    let mut min_timestamp = i64::MAX;
    let mut max_timestamp = i64::MIN;

    const ROWS_COUNT1: usize = MAX_ROWS_PER_BLOCK + 234;
    let mut rows: Vec<RawRow> = (0..ROWS_COUNT1)
        .map(|i| {
            let timestamp = (i * 2894) as i64;
            min_timestamp = min_timestamp.min(timestamp);
            max_timestamp = max_timestamp.max(timestamp);
            RawRow {
                timestamp,
                value: rand_below(&mut state, 200) as f64 - 100.0,
                precision_bits: 5,
                ..Default::default()
            }
        })
        .collect();
    let bsr1 = new_test_block_stream_reader(&mut rows);

    const ROWS_COUNT2: usize = MAX_ROWS_PER_BLOCK + 2344;
    let mut rows: Vec<RawRow> = (0..ROWS_COUNT2)
        .map(|i| {
            let timestamp = (i * 2494) as i64;
            min_timestamp = min_timestamp.min(timestamp);
            max_timestamp = max_timestamp.max(timestamp);
            RawRow {
                timestamp,
                value: rand_below(&mut state, 200) as f64 - 100.0,
                precision_bits: 5,
                ..Default::default()
            }
        })
        .collect();
    let bsr2 = new_test_block_stream_reader(&mut rows);

    check_merge_block_streams(
        vec![bsr1, bsr2],
        3,
        ROWS_COUNT1 + ROWS_COUNT2,
        min_timestamp,
        max_timestamp,
    );
}

// Port of TestMergeBlockStreamsTwoStreamsBigSequentialBlocks.
#[test]
fn two_streams_big_sequential_blocks() {
    let _dedup_guard = dedup_interval_guard();
    let mut state = 1u64;
    let mut min_timestamp = i64::MAX;
    let mut max_timestamp = i64::MIN;

    const ROWS_COUNT1: usize = MAX_ROWS_PER_BLOCK + 234;
    let mut rows: Vec<RawRow> = (0..ROWS_COUNT1)
        .map(|i| {
            let timestamp = (i * 2894) as i64;
            min_timestamp = min_timestamp.min(timestamp);
            max_timestamp = max_timestamp.max(timestamp);
            RawRow {
                timestamp,
                value: rand_below(&mut state, 200) as f64 - 100.0,
                precision_bits: 5,
                ..Default::default()
            }
        })
        .collect();
    let max_timestamp_b1 = rows[rows.len() - 1].timestamp;
    let bsr1 = new_test_block_stream_reader(&mut rows);

    const ROWS_COUNT2: usize = MAX_ROWS_PER_BLOCK - 233;
    let mut rows: Vec<RawRow> = (0..ROWS_COUNT2)
        .map(|i| {
            let timestamp = max_timestamp_b1 + (i * 2494) as i64;
            min_timestamp = min_timestamp.min(timestamp);
            max_timestamp = max_timestamp.max(timestamp);
            RawRow {
                timestamp,
                value: rand_below(&mut state, 200) as f64 - 100.0,
                precision_bits: 5,
                ..Default::default()
            }
        })
        .collect();
    let bsr2 = new_test_block_stream_reader(&mut rows);

    check_merge_block_streams(
        vec![bsr1, bsr2],
        3,
        ROWS_COUNT1 + ROWS_COUNT2,
        min_timestamp,
        max_timestamp,
    );
}

// Port of TestMergeBlockStreamsManyStreamsManyBlocksManyRows.
#[test]
fn many_streams_many_blocks_many_rows() {
    let _dedup_guard = dedup_interval_guard();
    let mut state = 1u64;
    let tsid_base = rand_tsid(&mut state);
    let mut min_timestamp = i64::MAX;
    let mut max_timestamp = i64::MIN;

    const BLOCKS_COUNT: usize = 113;
    let mut rows_count = 0usize;
    let mut bsrs = Vec::new();
    for _ in 0..20 {
        // Guarantee that every stream covers all the BLOCKS_COUNT
        // residues, so the merged stream contains exactly BLOCKS_COUNT
        // blocks.
        let rows_per_stream = BLOCKS_COUNT + rand_below(&mut state, 400) as usize;
        let mut rows: Vec<RawRow> = (0..rows_per_stream)
            .map(|j| {
                let mut tsid = tsid_base;
                tsid.metric_id = (j % BLOCKS_COUNT) as u64;
                let timestamp = rand_below(&mut state, 1_000_000_000) as i64;
                min_timestamp = min_timestamp.min(timestamp);
                max_timestamp = max_timestamp.max(timestamp);
                RawRow {
                    tsid,
                    timestamp,
                    value: rand_below(&mut state, 2000) as f64 / 1000.0 - 1.0,
                    precision_bits: DEFAULT_PRECISION_BITS,
                }
            })
            .collect();
        bsrs.push(new_test_block_stream_reader(&mut rows));
        rows_count += rows_per_stream;
    }
    check_merge_block_streams(bsrs, BLOCKS_COUNT, rows_count, min_timestamp, max_timestamp);
}

// Port of TestMergeForciblyStop.
#[test]
fn merge_forcibly_stop() {
    let _dedup_guard = dedup_interval_guard();
    let mut state = 1u64;
    let tsid_base = rand_tsid(&mut state);
    const BLOCKS_COUNT: usize = 113;
    let mut bsrs = Vec::new();
    for _ in 0..20 {
        let rows_per_stream = 1 + rand_below(&mut state, 1000) as usize;
        let mut rows: Vec<RawRow> = (0..rows_per_stream)
            .map(|j| {
                let mut tsid = tsid_base;
                tsid.metric_id = (j % BLOCKS_COUNT) as u64;
                RawRow {
                    tsid,
                    timestamp: rand_below(&mut state, 1_000_000_000) as i64,
                    value: rand_below(&mut state, 2000) as f64 / 1000.0 - 1.0,
                    precision_bits: DEFAULT_PRECISION_BITS,
                }
            })
            .collect();
        bsrs.push(new_test_block_stream_reader(&mut rows));
    }

    let mut ph = PartHeader::default();
    let mut bsw = BlockStreamWriter::new_inmemory_part(-5);
    let stop = AtomicBool::new(true); // forcibly stop the merge
    let rows_merged = AtomicU64::new(0);
    let rows_deleted = AtomicU64::new(0);
    let err = merge_block_streams(
        &mut ph,
        &mut bsw,
        &mut bsrs,
        Some(&stop),
        None,
        0,
        &rows_merged,
        &rows_deleted,
    )
    .expect_err("expected an error in merge_block_streams");
    assert_eq!(err, MergeError::ForciblyStopped);
    assert_eq!(
        rows_merged.load(Ordering::Relaxed),
        0,
        "unexpected rowsMerged"
    );
    assert_eq!(
        rows_deleted.load(Ordering::Relaxed),
        0,
        "unexpected rowsDeleted"
    );
}

// Rows outside the retention deadline must be dropped during the merge
// and counted in rows_deleted. Whole blocks below the deadline are
// dropped at the block level; rows inside partially-retained blocks are
// dropped by the slow (same-TSID merge) path — matching Go, where such
// rows are only removed opportunistically when blocks are merged.
#[test]
fn merge_retention_deadline() {
    let _dedup_guard = dedup_interval_guard();
    let retention_deadline = 5_000_000i64; // drops timestamps < 5e6

    // Streams 1+2: the same series with overlapping ranges, so the
    // slow merge path (with row-level retention filtering) runs.
    // Stream 1: timestamps 0..9_990_000 — 500 rows below the deadline.
    let mut rows: Vec<RawRow> = (0..1000)
        .map(|i| RawRow {
            tsid: Tsid {
                metric_id: 1,
                ..Default::default()
            },
            timestamp: i as i64 * 10_000,
            value: i as f64,
            precision_bits: 64,
        })
        .collect();
    let bsr1 = new_test_block_stream_reader(&mut rows);

    // Stream 2: timestamps 5_000_000..14_990_000 — all retained.
    let mut rows: Vec<RawRow> = (0..1000)
        .map(|i| RawRow {
            tsid: Tsid {
                metric_id: 1,
                ..Default::default()
            },
            timestamp: (i + 500) as i64 * 10_000,
            value: -(i as f64),
            precision_bits: 64,
        })
        .collect();
    let bsr2 = new_test_block_stream_reader(&mut rows);

    // Stream 3: another series entirely below the retention deadline —
    // dropped as a whole block.
    let mut rows: Vec<RawRow> = (0..100)
        .map(|i| RawRow {
            tsid: Tsid {
                metric_id: 2,
                ..Default::default()
            },
            timestamp: i as i64 * 10,
            value: i as f64,
            precision_bits: 64,
        })
        .collect();
    let bsr3 = new_test_block_stream_reader(&mut rows);

    let expected_deleted = 500 + 100; // half of stream 1 + all of stream 3
    let expected_rows = 500 + 1000; // stream 1 tail + all of stream 2

    let mut ph = PartHeader::default();
    let mut bsw = BlockStreamWriter::new_inmemory_part(-5);
    let rows_merged = AtomicU64::new(0);
    let rows_deleted = AtomicU64::new(0);
    let bufs = merge_block_streams(
        &mut ph,
        &mut bsw,
        &mut [bsr1, bsr2, bsr3],
        None,
        None,
        retention_deadline,
        &rows_merged,
        &rows_deleted,
    )
    .expect("unexpected error in merge_block_streams")
    .expect("in-memory merge must return buffers");

    assert_eq!(rows_deleted.load(Ordering::Relaxed), expected_deleted);
    assert_eq!(ph.rows_count, expected_rows);
    assert!(ph.min_timestamp >= retention_deadline);

    let mp = InmemoryPart::from_buffers(ph, bufs);
    let mut bsr = BlockStreamReader::from_inmemory_part(&mp);
    let mut rows_read = 0usize;
    while bsr.next_block() {
        assert_eq!(bsr.block.header().tsid.metric_id, 1);
        assert!(bsr
            .block
            .timestamps()
            .iter()
            .all(|&ts| ts >= retention_deadline));
        rows_read += bsr.block.timestamps().len();
    }
    assert_eq!(bsr.error(), None);
    assert_eq!(rows_read, expected_rows as usize);
}

// Blocks of deleted metric IDs (dmis) must be dropped during the merge.
#[test]
fn merge_deleted_metric_ids() {
    let _dedup_guard = dedup_interval_guard();
    let mut rows: Vec<RawRow> = (0..200)
        .map(|i| RawRow {
            tsid: Tsid {
                metric_id: 1 + (i % 2) as u64,
                ..Default::default()
            },
            timestamp: i as i64 * 1000,
            value: i as f64,
            precision_bits: 64,
        })
        .collect();
    let bsr = new_test_block_stream_reader(&mut rows);

    let mut dmis = uint64set::Set::default();
    dmis.add(2);

    let mut ph = PartHeader::default();
    let mut bsw = BlockStreamWriter::new_inmemory_part(-5);
    let rows_merged = AtomicU64::new(0);
    let rows_deleted = AtomicU64::new(0);
    let bufs = merge_block_streams(
        &mut ph,
        &mut bsw,
        &mut [bsr],
        None,
        Some(&dmis),
        0,
        &rows_merged,
        &rows_deleted,
    )
    .expect("unexpected error in merge_block_streams")
    .expect("in-memory merge must return buffers");

    assert_eq!(rows_deleted.load(Ordering::Relaxed), 100);
    assert_eq!(rows_merged.load(Ordering::Relaxed), 100);
    assert_eq!(ph.rows_count, 100);

    let mp = InmemoryPart::from_buffers(ph, bufs);
    let mut bsr = BlockStreamReader::from_inmemory_part(&mp);
    while bsr.next_block() {
        assert_eq!(bsr.block.header().tsid.metric_id, 1);
    }
    assert_eq!(bsr.error(), None);
}

// Dedup must be applied while writing merged blocks when the global
// dedup interval is set.
//
// NOTE: the global dedup interval is process-wide; another test touching
// it concurrently (block.rs::block_dedup_during_merge) may race with
// this one, so the check is retried a few times.
#[test]
fn merge_with_dedup() {
    let _dedup_guard = dedup_interval_guard();
    const DEDUP_INTERVAL: i64 = 1000;
    let timestamps: [i64; 5] = [0, 100, 1000, 2000, 2100];
    let expected_timestamps: [i64; 4] = [0, 1000, 2000, 2100];

    let mut last_err = String::new();
    for _ in 0..3 {
        let mut rows: Vec<RawRow> = timestamps
            .iter()
            .enumerate()
            .map(|(i, &ts)| RawRow {
                tsid: Tsid {
                    metric_id: 7,
                    ..Default::default()
                },
                timestamp: ts,
                value: i as f64,
                precision_bits: 64,
            })
            .collect();
        let bsr = new_test_block_stream_reader(&mut rows);

        let mut ph = PartHeader::default();
        let mut bsw = BlockStreamWriter::new_inmemory_part(-5);
        let rows_merged = AtomicU64::new(0);
        let rows_deleted = AtomicU64::new(0);
        esm_storage::set_dedup_interval(DEDUP_INTERVAL);
        let res = merge_block_streams(
            &mut ph,
            &mut bsw,
            &mut [bsr],
            None,
            None,
            0,
            &rows_merged,
            &rows_deleted,
        );
        esm_storage::set_dedup_interval(0);
        let bufs = res
            .expect("unexpected error in merge_block_streams")
            .expect("in-memory merge must return buffers");

        let mp = InmemoryPart::from_buffers(ph, bufs);
        let mut bsr = BlockStreamReader::from_inmemory_part(&mp);
        let mut got = Vec::new();
        while bsr.next_block() {
            got.extend_from_slice(bsr.block.timestamps());
        }
        assert_eq!(bsr.error(), None);
        if got == expected_timestamps {
            return;
        }
        last_err =
            format!("unexpected timestamps after dedup; got {got:?}; want {expected_timestamps:?}");
    }
    panic!("{last_err}");
}
