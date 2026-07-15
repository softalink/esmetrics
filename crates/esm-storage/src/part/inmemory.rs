//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/inmemory_part.go plus the
//! rawRowsMarshaler from raw_row.go.

use std::path::Path;
use std::sync::Arc;

use esm_common::{decimal, fasttime};

use crate::block::{Block, MAX_ROWS_PER_BLOCK};
use crate::block_stream::BlockStreamWriter;
use crate::part::header::PartHeader;
use crate::part::{
    Part, PartFile, INDEX_FILENAME, METAINDEX_FILENAME, TIMESTAMPS_FILENAME, VALUES_FILENAME,
};
use crate::raw_row::{sort_raw_rows, RawRow};

/// In-memory part: the four part streams held as byte buffers.
/// Go: inmemoryPart.
///
/// The buffers are `Arc`ed so [`Part`]s and block stream readers can share
/// them without copying (Go shares the chunkedbuffer.Buffer instances the
/// same way).
pub struct InmemoryPart {
    pub ph: PartHeader,

    pub(crate) timestamps_data: Arc<Vec<u8>>,
    pub(crate) values_data: Arc<Vec<u8>>,
    pub(crate) index_data: Arc<Vec<u8>>,
    pub(crate) metaindex_data: Arc<Vec<u8>>,

    /// Unix timestamp (seconds) of the part creation, used by the stage-4
    /// flush-to-disk deadline logic.
    pub creation_time: u64,
}

impl InmemoryPart {
    /// Creates an in-memory part from the given rows, sorting them by
    /// `(TSID, Timestamp)` first.
    /// Go: inmemoryPart.InitFromRows + rawRowsMarshaler.marshalToInmemoryPart.
    pub fn init_from_rows(rows: &mut [RawRow]) -> InmemoryPart {
        assert!(
            !rows.is_empty(),
            "BUG: init_from_rows must accept at least one row"
        );
        assert!(
            (rows.len() as u64) < (1u64 << 32),
            "BUG: rows count must be smaller than 2^32; got {}",
            rows.len()
        );

        // Use the minimum compression level for first-level in-memory
        // blocks, since they are going to be re-compressed during subsequent
        // merges.
        const COMPRESS_LEVEL: i32 = -5;
        let mut bsw = BlockStreamWriter::new_inmemory_part(COMPRESS_LEVEL);

        let mut ph = PartHeader::default();

        // Sort rows by (TSID, Timestamp) if they aren't sorted yet.
        sort_raw_rows(rows);

        // Group rows into blocks.
        let mut aux_timestamps: Vec<i64> = Vec::new();
        let mut aux_values: Vec<i64> = Vec::new();
        let mut aux_float_values: Vec<f64> = Vec::new();
        let mut rows_merged = 0u64;
        let mut tsid = rows[0].tsid;
        let mut precision_bits = rows[0].precision_bits;
        let mut tmp_block = Block::default();
        for r in rows.iter() {
            if r.tsid.metric_id == tsid.metric_id && aux_timestamps.len() < MAX_ROWS_PER_BLOCK {
                aux_timestamps.push(r.timestamp);
                aux_float_values.push(r.value);
                continue;
            }

            aux_values.clear();
            let scale = decimal::append_float_to_decimal(&mut aux_values, &aux_float_values);
            tmp_block.init(&tsid, &aux_timestamps, &aux_values, scale, precision_bits);
            bsw.write_external_block(&mut tmp_block, &mut ph, &mut rows_merged);

            tsid = r.tsid;
            precision_bits = r.precision_bits;
            aux_timestamps.clear();
            aux_timestamps.push(r.timestamp);
            aux_float_values.clear();
            aux_float_values.push(r.value);
        }

        aux_values.clear();
        let scale = decimal::append_float_to_decimal(&mut aux_values, &aux_float_values);
        tmp_block.init(&tsid, &aux_timestamps, &aux_values, scale, precision_bits);
        bsw.write_external_block(&mut tmp_block, &mut ph, &mut rows_merged);
        assert!(
            rows_merged == rows.len() as u64,
            "BUG: unexpected rowsMerged; got {rows_merged}; want {}",
            rows.len()
        );

        let bufs = bsw
            .must_close()
            .expect("BUG: in-memory block stream writer must return buffers");
        InmemoryPart::from_buffers(ph, bufs)
    }

    /// Builds an in-memory part from a part header and the four stream
    /// buffers (`[timestamps, values, index, metaindex]`) produced by an
    /// in-memory [`BlockStreamWriter`].
    pub fn from_buffers(ph: PartHeader, bufs: [Vec<u8>; 4]) -> InmemoryPart {
        let [timestamps_data, values_data, index_data, metaindex_data] = bufs;
        InmemoryPart {
            ph,
            timestamps_data: Arc::new(timestamps_data),
            values_data: Arc::new(values_data),
            index_data: Arc::new(index_data),
            metaindex_data: Arc::new(metaindex_data),
            creation_time: fasttime::unix_timestamp(),
        }
    }

    /// Stores the part to the given path on disk.
    /// Go: inmemoryPart.MustStoreToDisk.
    pub fn must_store_to_disk(&self, path: &Path) {
        esm_common::fs::must_mkdir_fail_if_exist(path);

        esm_common::fs::must_write_sync(path.join(TIMESTAMPS_FILENAME), &self.timestamps_data);
        esm_common::fs::must_write_sync(path.join(VALUES_FILENAME), &self.values_data);
        esm_common::fs::must_write_sync(path.join(INDEX_FILENAME), &self.index_data);
        esm_common::fs::must_write_sync(path.join(METAINDEX_FILENAME), &self.metaindex_data);

        self.ph.must_write_metadata(path);

        esm_common::fs::must_sync_path_and_parent_dir(path);
    }

    /// Creates a [`Part`] backed by the in-memory buffers.
    ///
    /// It is safe to call `new_part` multiple times. Go: inmemoryPart.NewPart.
    pub fn new_part(&self) -> Part {
        Part::new(
            &self.ph,
            Path::new(""),
            self.size(),
            &self.metaindex_data,
            PartFile::Mem(Arc::clone(&self.timestamps_data)),
            PartFile::Mem(Arc::clone(&self.values_data)),
            PartFile::Mem(Arc::clone(&self.index_data)),
        )
    }

    /// The total size of the part data in bytes. Go: inmemoryPart.size.
    pub fn size(&self) -> u64 {
        (self.timestamps_data.len()
            + self.values_data.len()
            + self.index_data.len()
            + self.metaindex_data.len()) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_stream::{timestamps_blocks_merged, BlockStreamReader};
    use crate::tsid::Tsid;
    use crate::util::splitmix64;

    const DEFAULT_PRECISION_BITS: u8 = 4;

    fn rand_tsid(state: &mut u64) -> Tsid {
        Tsid {
            metric_group_id: splitmix64(state),
            job_id: splitmix64(state) as u32,
            instance_id: splitmix64(state) as u32,
            metric_id: splitmix64(state),
        }
    }

    // Roughly normal-ish signed value in [-3*scale, 3*scale]; the exact
    // distribution doesn't matter for these tests, determinism does.
    fn rand_signed(state: &mut u64, scale: f64) -> f64 {
        let a = (splitmix64(state) % 2000) as f64 / 1000.0 - 1.0;
        let b = (splitmix64(state) % 2000) as f64 / 1000.0 - 1.0;
        let c = (splitmix64(state) % 2000) as f64 / 1000.0 - 1.0;
        (a + b + c) * scale
    }

    // Port of TestInmemoryPartInitFromRows.
    #[test]
    fn inmemory_part_init_from_rows() {
        // Single row.
        check_init_from_rows(
            &mut [RawRow {
                tsid: Tsid {
                    metric_id: 234,
                    ..Default::default()
                },
                timestamp: 123,
                value: 456.789,
                precision_bits: DEFAULT_PRECISION_BITS,
            }],
            1,
        );

        // Check a single tsid.
        let mut state = 1u64;
        let tsid = rand_tsid(&mut state);
        let mut rows: Vec<RawRow> = (0..10_000)
            .map(|_| RawRow {
                tsid,
                timestamp: rand_signed(&mut state, 1e7) as i64,
                value: rand_signed(&mut state, 100.0),
                precision_bits: DEFAULT_PRECISION_BITS,
            })
            .collect();
        check_init_from_rows(&mut rows, 2);

        // Check distinct tsids.
        let mut rows: Vec<RawRow> = (0..10_000)
            .map(|i| {
                let mut tsid = rand_tsid(&mut state);
                tsid.metric_id = i as u64;
                RawRow {
                    tsid,
                    timestamp: rand_signed(&mut state, 1e7) as i64,
                    value: rand_signed(&mut state, 100.0),
                    precision_bits: (i % 64) as u8 + 1,
                }
            })
            .collect();
        check_init_from_rows(&mut rows, 10_000);
    }

    // Port of testInmemoryPartInitFromRows.
    fn check_init_from_rows(rows: &mut [RawRow], blocks_count: usize) {
        let mut min_timestamp = i64::MAX;
        let mut max_timestamp = i64::MIN;
        for r in rows.iter() {
            min_timestamp = min_timestamp.min(r.timestamp);
            max_timestamp = max_timestamp.max(r.timestamp);
        }

        let rows_len = rows.len();
        let mp = InmemoryPart::init_from_rows(rows);
        assert_eq!(mp.ph.rows_count as usize, rows_len, "unexpected rows count");
        assert_eq!(
            mp.ph.min_timestamp, min_timestamp,
            "unexpected minTimestamp"
        );
        assert_eq!(
            mp.ph.max_timestamp, max_timestamp,
            "unexpected maxTimestamp"
        );

        let mut bsr = BlockStreamReader::from_inmemory_part(&mp);
        let mut rows_count = 0usize;
        let mut block_num = 0usize;
        let mut prev_tsid = Tsid::default();
        while bsr.next_block() {
            let bh = *bsr.block.header();
            assert!(
                bh.tsid >= prev_tsid,
                "TSID for the current block cannot be smaller than the TSID of the previous block"
            );
            prev_tsid = bh.tsid;

            assert!(
                bh.min_timestamp >= min_timestamp,
                "unexpected MinTimestamp in block; got {}; cannot be smaller than {min_timestamp}",
                bh.min_timestamp
            );
            assert!(
                bh.max_timestamp <= max_timestamp,
                "unexpected MaxTimestamp in block; got {}; cannot be higher than {max_timestamp}",
                bh.max_timestamp
            );

            bsr.block
                .unmarshal_data()
                .unwrap_or_else(|err| panic!("cannot unmarshal block #{block_num}: {err}"));

            let mut prev_timestamp = bh.min_timestamp;
            let mut block_rows_count = 0usize;
            for &timestamp in bsr.block.timestamps() {
                assert!(
                    timestamp >= bh.min_timestamp,
                    "unexpected timestamp {timestamp}; cannot be smaller than {}",
                    bh.min_timestamp
                );
                assert!(
                    timestamp <= bh.max_timestamp,
                    "unexpected timestamp {timestamp}; cannot be higher than {}",
                    bh.max_timestamp
                );
                assert!(
                    timestamp >= prev_timestamp,
                    "too small timestamp {timestamp}; cannot be smaller than the previous \
                     {prev_timestamp}"
                );
                prev_timestamp = timestamp;
                block_rows_count += 1;
            }
            assert_eq!(
                block_rows_count, bh.rows_count as usize,
                "unexpected number of rows in the block"
            );
            rows_count += block_rows_count;
            block_num += 1;
        }
        assert_eq!(bsr.error(), None);
        assert_eq!(block_num, blocks_count, "unexpected number of blocks read");
        assert_eq!(rows_count, rows_len, "unexpected number of rows");
    }

    // Values must survive the rows -> InmemoryPart -> reader roundtrip
    // exactly at lossless precision.
    #[test]
    fn inmemory_part_lossless_values_roundtrip() {
        let mut rows: Vec<RawRow> = (0..500)
            .map(|i| RawRow {
                tsid: Tsid {
                    metric_id: 42,
                    ..Default::default()
                },
                timestamp: i as i64 * 15_000,
                value: (i as f64) * 1.25 - 100.0,
                precision_bits: 64,
            })
            .collect();
        let expected: Vec<(i64, f64)> = rows.iter().map(|r| (r.timestamp, r.value)).collect();

        let mp = InmemoryPart::init_from_rows(&mut rows);
        let mut bsr = BlockStreamReader::from_inmemory_part(&mp);
        let mut got: Vec<(i64, f64)> = Vec::new();
        while bsr.next_block() {
            let mut values = Vec::new();
            decimal::append_decimal_to_float(
                &mut values,
                bsr.block.values(),
                bsr.block.header().scale,
            );
            got.extend(bsr.block.timestamps().iter().copied().zip(values));
        }
        assert_eq!(bsr.error(), None);
        assert_eq!(got, expected);
    }

    // Two series sharing identical timestamps must share a single
    // timestamps block on disk (the identical-timestamps optimization in
    // BlockStreamWriter::write_external_block).
    #[test]
    fn timestamps_block_sharing() {
        let timestamps: Vec<i64> = (0..100).map(|i| i * 30_000).collect();
        let mut rows = Vec::new();
        for metric_id in [1u64, 2, 3] {
            for (i, &ts) in timestamps.iter().enumerate() {
                rows.push(RawRow {
                    tsid: Tsid {
                        metric_id,
                        ..Default::default()
                    },
                    timestamp: ts,
                    value: (metric_id * 1000 + i as u64) as f64,
                    precision_bits: 64,
                });
            }
        }

        let shared_before = timestamps_blocks_merged();
        let mp = InmemoryPart::init_from_rows(&mut rows);
        assert!(
            timestamps_blocks_merged() >= shared_before + 2,
            "expected at least 2 shared timestamp blocks"
        );

        // All three blocks must point at the same timestamps block.
        let mut bsr = BlockStreamReader::from_inmemory_part(&mp);
        let mut offsets = Vec::new();
        let mut sizes = Vec::new();
        while bsr.next_block() {
            offsets.push(bsr.block.header().timestamps_block_offset);
            sizes.push(bsr.block.header().timestamps_block_size);
            assert_eq!(bsr.block.timestamps(), &timestamps[..]);
        }
        assert_eq!(bsr.error(), None);
        assert_eq!(offsets.len(), 3);
        assert!(offsets.iter().all(|&off| off == offsets[0]));
        // timestamps.bin contains exactly one block.
        assert_eq!(mp.timestamps_data.len(), sizes[0] as usize);
    }
}
