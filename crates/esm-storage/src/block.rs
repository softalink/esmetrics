//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/block.go.

use crate::block_header::BlockHeader;
use crate::dedup;
use crate::time_range::TimeRange;
use crate::tsid::Tsid;
use esm_common::decimal;
use esm_encoding as encoding;
use esm_encoding::MarshalType;

/// The maximum number of rows per block. Go: maxRowsPerBlock.
pub const MAX_ROWS_PER_BLOCK: usize = 8 * 1024;

/// The maximum size of values in the block. Go: maxBlockSize.
pub const MAX_BLOCK_SIZE: usize = 8 * MAX_ROWS_PER_BLOCK;

/// Block represents a block of time series values for a single TSID.
/// Go: Block.
///
/// A block holds either unpacked `timestamps`/`values` (decimal i64 plus the
/// `scale` in the header) or the packed `timestamps_data`/`values_data` byte
/// representation, never both.
#[derive(Debug, Default, Clone)]
pub struct Block {
    pub(crate) bh: BlockHeader,

    /// The next index for reading timestamps and values.
    next_idx: usize,

    pub(crate) timestamps: Vec<i64>,
    pub(crate) values: Vec<i64>,

    /// Marshaled representation of the block header.
    header_data: Vec<u8>,

    /// Marshaled representation of timestamps.
    timestamps_data: Vec<u8>,

    /// Marshaled representation of values.
    values_data: Vec<u8>,
}

impl Block {
    /// Resets the block. Go: Block.Reset.
    pub fn reset(&mut self) {
        self.bh = BlockHeader::default();
        self.next_idx = 0;
        self.timestamps.clear();
        self.values.clear();
        self.header_data.clear();
        self.timestamps_data.clear();
        self.values_data.clear();
    }

    /// Copies `src` to `self`. Go: Block.CopyFrom.
    pub fn copy_from(&mut self, src: &Block) {
        self.bh = src.bh;
        self.next_idx = 0;
        self.timestamps.clear();
        self.timestamps
            .extend_from_slice(&src.timestamps[src.next_idx..]);
        self.values.clear();
        self.values.extend_from_slice(&src.values[src.next_idx..]);

        self.header_data.clone_from(&src.header_data);
        self.timestamps_data.clone_from(&src.timestamps_data);
        self.values_data.clone_from(&src.values_data);
    }

    /// Returns the block header.
    pub fn header(&self) -> &BlockHeader {
        &self.bh
    }

    /// Returns the unpacked timestamps that haven't been consumed yet.
    pub fn timestamps(&self) -> &[i64] {
        &self.timestamps[self.next_idx..]
    }

    /// Returns the unpacked decimal values that haven't been consumed yet.
    /// Apply `10^bh.scale` (see `decimal::append_decimal_to_float`) to get f64s.
    pub fn values(&self) -> &[i64] {
        &self.values[self.next_idx..]
    }

    /// Go: Block.fixupTimestamps.
    fn fixup_timestamps(&mut self) {
        self.bh.min_timestamp = self.timestamps[self.next_idx];
        self.bh.max_timestamp = self.timestamps[self.timestamps.len() - 1];
    }

    /// Returns the number of rows in the block (per header).
    /// Go: Block.RowsCount.
    pub fn rows_count(&self) -> usize {
        self.bh.rows_count as usize
    }

    /// Initializes the block with the given tsid, timestamps, values and
    /// scale. Go: Block.Init.
    pub fn init(
        &mut self,
        tsid: &Tsid,
        timestamps: &[i64],
        values: &[i64],
        scale: i16,
        precision_bits: u8,
    ) {
        self.reset();
        self.bh.tsid = *tsid;
        self.bh.scale = scale;
        self.bh.precision_bits = precision_bits;
        self.timestamps.extend_from_slice(timestamps);
        self.values.extend_from_slice(values);
        if !self.timestamps.is_empty() {
            self.fixup_timestamps();
        }
    }

    /// Advances to the next row. Returns false if there are no more rows in
    /// the block. Go: Block.nextRow.
    #[allow(dead_code)] // used by the stage-2 merge path
    pub(crate) fn next_row(&mut self) -> bool {
        if self.next_idx == self.values.len() {
            return false;
        }
        self.next_idx += 1;
        true
    }

    /// Makes sure the block is unmarshaled. Go: Block.assertUnmarshaled.
    #[allow(dead_code)] // used by the stage-2 merge path
    pub(crate) fn assert_unmarshaled(&self) {
        assert!(
            self.values_data.is_empty(),
            "BUG: valuesData must be empty; got {} bytes",
            self.values_data.len()
        );
        assert!(
            self.timestamps_data.is_empty(),
            "BUG: timestampsData must be empty; got {} bytes",
            self.timestamps_data.len()
        );
        assert!(
            self.values.len() == self.timestamps.len(),
            "BUG: the number of values must match the number of timestamps; got {} vs {}",
            self.values.len(),
            self.timestamps.len()
        );
        assert!(
            self.next_idx <= self.values.len(),
            "BUG: nextIdx cannot exceed the number of values; got {} vs {}",
            self.next_idx,
            self.values.len()
        );
    }

    /// Makes sure `self` and `ib` are mergeable, i.e. they have the same tsid
    /// and scale. Go: Block.assertMergeable.
    #[allow(dead_code)] // used by the stage-2 merge path
    pub(crate) fn assert_mergeable(&self, ib: &Block) {
        assert!(
            self.bh.tsid.metric_id == ib.bh.tsid.metric_id,
            "BUG: unequal TSID: {:?} vs {:?}",
            self.bh.tsid,
            ib.bh.tsid
        );
        assert!(
            self.bh.scale == ib.bh.scale,
            "BUG: unequal Scale: {} vs {}",
            self.bh.scale,
            ib.bh.scale
        );
    }

    /// Returns true if the block is too big to be extended.
    /// Go: Block.tooBig.
    #[allow(dead_code)] // used by the stage-2 merge path
    pub(crate) fn too_big(&self) -> bool {
        if self.bh.rows_count as usize >= MAX_ROWS_PER_BLOCK
            || self.values.len() - self.next_idx >= MAX_ROWS_PER_BLOCK
        {
            return true;
        }
        self.values_data.len() >= MAX_BLOCK_SIZE
    }

    /// Applies the global dedup interval to the unpacked samples.
    /// Go: Block.deduplicateSamplesDuringMerge.
    ///
    /// PORT-NOTE: the `dedupsDuringMerge` metrics counter is deferred to the
    /// stage-4 storage-level metrics.
    #[allow(dead_code)] // used by the stage-2 merge path (exercised in tests)
    pub(crate) fn deduplicate_samples_during_merge(&mut self) {
        if !dedup::is_dedup_enabled() {
            // Deduplication is disabled.
            return;
        }
        // Unmarshal the block if it isn't unmarshaled yet in order to apply
        // the de-duplication to unmarshaled samples.
        if let Err(err) = self.unmarshal_data() {
            panic!("FATAL: cannot unmarshal block: {err}");
        }
        if self.timestamps.len() - self.next_idx < 2 {
            // Nothing to dedup.
            return;
        }
        let dedup_interval = dedup::get_dedup_interval();
        if dedup_interval <= 0 {
            // Deduplication is disabled.
            return;
        }
        let idx = self.next_idx;
        let n = dedup::deduplicate_samples_during_merge_in_place(
            &mut self.timestamps[idx..],
            &mut self.values[idx..],
            dedup_interval,
        );
        self.timestamps.truncate(idx + n);
        self.values.truncate(idx + n);
    }

    /// Go: Block.rowsCount (lowercase) — the number of not-yet-consumed rows.
    #[allow(dead_code)] // used by the stage-2 merge path
    pub(crate) fn pending_rows_count(&self) -> usize {
        if self.values.is_empty() {
            return self.bh.rows_count as usize;
        }
        self.values.len() - self.next_idx
    }

    /// Marshals the block into binary representation and returns
    /// `(header_data, timestamps_data, values_data)`. Go: Block.MarshalData.
    pub fn marshal_data(
        &mut self,
        timestamps_block_offset: u64,
        values_block_offset: u64,
    ) -> (&[u8], &[u8], &[u8]) {
        if self.values.is_empty() {
            // The data has been already marshaled.

            // values_data and timestamps_data may be empty for certain
            // marshal type values, so don't check them.

            assert!(
                self.next_idx == 0,
                "BUG: nextIdx must be zero; got {}",
                self.next_idx
            );
            assert!(
                self.bh.timestamps_block_size as usize == self.timestamps_data.len(),
                "BUG: invalid TimestampsBlockSize; got {}; expecting {}",
                self.bh.timestamps_block_size,
                self.timestamps_data.len()
            );
            assert!(
                self.bh.values_block_size as usize == self.values_data.len(),
                "BUG: invalid ValuesBlockSize; got {}; expecting {}",
                self.bh.values_block_size,
                self.values_data.len()
            );
            assert!(
                self.bh.rows_count > 0,
                "BUG: RowsCount must be greater than 0; got {}",
                self.bh.rows_count
            );

            // header_data must be always recreated, since it contains
            // timestamps_block_offset and values_block_offset.
            self.bh.timestamps_block_offset = timestamps_block_offset;
            self.bh.values_block_offset = values_block_offset;
            self.header_data.clear();
            self.bh.marshal(&mut self.header_data);

            return (&self.header_data, &self.timestamps_data, &self.values_data);
        }

        assert!(
            self.next_idx <= self.values.len(),
            "BUG: nextIdx cannot exceed values size; got {} vs {}",
            self.next_idx,
            self.values.len()
        );
        assert!(
            self.values.len() == self.timestamps.len(),
            "BUG: the number of values must match the number of timestamps; got {} vs {}",
            self.values.len(),
            self.timestamps.len()
        );
        let rows_count = self.values.len() - self.next_idx;
        assert!(
            rows_count > 0,
            "BUG: values cannot be empty; nextIdx={}, timestampsBlockOffset={}, valuesBlockOffset={}",
            self.next_idx,
            timestamps_block_offset,
            values_block_offset
        );

        self.values_data.clear();
        let (values_marshal_type, first_value) = encoding::marshal_values(
            &mut self.values_data,
            &self.values[self.next_idx..],
            self.bh.precision_bits,
        );
        self.bh.values_marshal_type = values_marshal_type.as_u8();
        self.bh.first_value = first_value;
        self.bh.values_block_offset = values_block_offset;
        self.bh.values_block_size = self.values_data.len() as u32;
        self.values.clear();

        self.timestamps_data.clear();
        let (timestamps_marshal_type, min_timestamp) = encoding::marshal_timestamps(
            &mut self.timestamps_data,
            &self.timestamps[self.next_idx..],
            self.bh.precision_bits,
        );
        self.bh.timestamps_marshal_type = timestamps_marshal_type.as_u8();
        self.bh.min_timestamp = min_timestamp;
        self.bh.timestamps_block_offset = timestamps_block_offset;
        self.bh.timestamps_block_size = self.timestamps_data.len() as u32;
        self.bh.max_timestamp = self.timestamps[self.timestamps.len() - 1];
        self.timestamps.clear();

        self.bh.rows_count = rows_count as u32;
        self.header_data.clear();
        self.bh.marshal(&mut self.header_data);

        self.next_idx = 0;

        (&self.header_data, &self.timestamps_data, &self.values_data)
    }

    /// Unmarshals block data (the header must be already unmarshaled).
    /// Go: Block.UnmarshalData.
    pub fn unmarshal_data(&mut self) -> Result<(), String> {
        if !self.values.is_empty() {
            // The data has been already unmarshaled.
            assert!(
                self.values_data.is_empty(),
                "BUG: valuesData must be empty; contains {} bytes",
                self.values_data.len()
            );
            assert!(
                self.timestamps_data.is_empty(),
                "BUG: timestampsData must be empty; contains {} bytes",
                self.timestamps_data.len()
            );
            return Ok(());
        }

        if self.bh.rows_count == 0 {
            return Err(format!(
                "RowsCount must be greater than 0; got {}",
                self.bh.rows_count
            ));
        }

        let timestamps_marshal_type = MarshalType::from_u8(self.bh.timestamps_marshal_type)
            .ok_or_else(|| {
                format!(
                    "unsupported TimestampsMarshalType: {}",
                    self.bh.timestamps_marshal_type
                )
            })?;
        self.timestamps.clear();
        encoding::unmarshal_timestamps(
            &mut self.timestamps,
            &self.timestamps_data,
            timestamps_marshal_type,
            self.bh.min_timestamp,
            self.bh.rows_count as usize,
        )?;
        if self.bh.precision_bits < 64 {
            // Recover timestamps order after lossy compression.
            encoding::ensure_non_decreasing_sequence(
                &mut self.timestamps,
                self.bh.min_timestamp,
                self.bh.max_timestamp,
            );
        } else if timestamps_marshal_type.needs_validation() {
            // Ensure timestamps are in the range
            // [MinTimestamp ... MaxTimestamp] and are ordered.
            check_timestamps_bounds(
                &self.timestamps,
                self.bh.min_timestamp,
                self.bh.max_timestamp,
            )?;
        }
        self.timestamps_data.clear();

        let values_marshal_type =
            MarshalType::from_u8(self.bh.values_marshal_type).ok_or_else(|| {
                format!(
                    "unsupported ValuesMarshalType: {}",
                    self.bh.values_marshal_type
                )
            })?;
        self.values.clear();
        encoding::unmarshal_values(
            &mut self.values,
            &self.values_data,
            values_marshal_type,
            self.bh.first_value,
            self.bh.rows_count as usize,
        )?;
        self.values_data.clear();

        if self.timestamps.len() != self.values.len() {
            return Err(format!(
                "timestamps and values count mismatch; got {} vs {}",
                self.timestamps.len(),
                self.values.len()
            ));
        }

        self.next_idx = 0;

        Ok(())
    }

    /// Filters samples by `tr` and appends them to `dst_timestamps` /
    /// `dst_values` (converting decimal values to f64). It is expected that
    /// `unmarshal_data` has been already called.
    /// Go: Block.AppendRowsWithTimeRangeFilter.
    pub fn append_rows_with_time_range_filter(
        &self,
        dst_timestamps: &mut Vec<i64>,
        dst_values: &mut Vec<f64>,
        tr: TimeRange,
    ) {
        let (timestamps, values) = self.filter_timestamps(tr);
        dst_timestamps.extend_from_slice(timestamps);
        decimal::append_decimal_to_float(dst_values, values, self.bh.scale);
    }

    /// Go: Block.filterTimestamps.
    fn filter_timestamps(&self, tr: TimeRange) -> (&[i64], &[i64]) {
        let timestamps = &self.timestamps[..];

        // Skip timestamps smaller than tr.min_timestamp.
        let mut i = 0;
        while i < timestamps.len() && timestamps[i] < tr.min_timestamp {
            i += 1;
        }

        // Skip timestamps bigger than tr.max_timestamp.
        let mut j = timestamps.len();
        while j > i && timestamps[j - 1] > tr.max_timestamp {
            j -= 1;
        }

        if i == j {
            return (&[], &[]);
        }
        (&timestamps[i..j], &self.values[i..j])
    }

    /// Marshals the block to `dst` so it could be portably migrated to
    /// another instance. The marshaled value must be unmarshaled with
    /// [`Block::unmarshal_portable`]. Go: Block.MarshalPortable.
    pub fn marshal_portable(&mut self, dst: &mut Vec<u8>) {
        self.marshal_data(0, 0);
        self.bh.marshal_portable(dst);
        encoding::marshal_bytes(dst, &self.timestamps_data);
        encoding::marshal_bytes(dst, &self.values_data);
    }

    /// Unmarshals a block marshaled with [`Block::marshal_portable`] from
    /// `src` and returns the remaining tail. Go: Block.UnmarshalPortable.
    pub fn unmarshal_portable<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        self.reset();
        let src = self.bh.unmarshal_portable(src)?;

        let (timestamps_data, n_size) =
            encoding::unmarshal_bytes(src).ok_or("cannot read timestampsData")?;
        self.timestamps_data.extend_from_slice(timestamps_data);
        let src = &src[n_size..];

        let (values_data, n_size) =
            encoding::unmarshal_bytes(src).ok_or("cannot read valuesData")?;
        self.values_data.extend_from_slice(values_data);
        let src = &src[n_size..];

        self.bh
            .validate()
            .map_err(|err| format!("invalid blockHeader: {err}"))?;
        self.unmarshal_data()
            .map_err(|err| format!("invalid data: {err}"))?;

        Ok(src)
    }
}

/// Go: checkTimestampsBounds.
fn check_timestamps_bounds(
    timestamps: &[i64],
    min_timestamp: i64,
    max_timestamp: i64,
) -> Result<(), String> {
    let Some(&first) = timestamps.first() else {
        return Ok(());
    };
    let mut ts_prev = first;
    if ts_prev < min_timestamp {
        return Err(format!(
            "timestamp for the row 0 out of {} rows cannot be smaller than {}; got {}",
            timestamps.len(),
            min_timestamp,
            ts_prev
        ));
    }
    for (i, &ts) in timestamps[1..].iter().enumerate() {
        if ts < ts_prev {
            return Err(format!(
                "timestamp for the row {} cannot be smaller than the timestamp for the row {} (total {} rows); got {} vs {}",
                i + 1,
                i,
                timestamps.len(),
                ts,
                ts_prev
            ));
        }
        ts_prev = ts;
    }
    if ts_prev > max_timestamp {
        return Err(format!(
            "timestamp for the row {} (the last one) cannot be bigger than {}; got {}",
            timestamps.len() - 1,
            max_timestamp,
            ts_prev
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::splitmix64;

    // Port of TestBlockMarshalUnmarshalPortable.
    #[test]
    fn block_marshal_unmarshal_portable() {
        let mut state = 1u64;
        let mut b = Block::default();
        for i in 0..1000usize {
            b.reset();
            let rows_count = (splitmix64(&mut state) as usize % MAX_ROWS_PER_BLOCK) + 1;
            b.timestamps = rand_timestamps(&mut state, rows_count);
            b.values = rand_values(&mut state, rows_count);
            b.bh.scale = (splitmix64(&mut state) % 30) as i16 - 15;
            b.bh.precision_bits = (64 - (i % 64)) as u8;
            check_marshal_unmarshal_portable(&b);
        }
    }

    fn check_marshal_unmarshal_portable(b: &Block) {
        let mut b1 = Block::default();
        let mut b2 = Block::default();
        let rows_count = b.values.len();
        b1.copy_from(b);
        let mut data = Vec::new();
        b1.marshal_portable(&mut data);
        assert_eq!(
            b1.bh.rows_count as usize, rows_count,
            "unexpected number of rows marshaled"
        );
        let bh_expected = b1.bh;
        let tail = b2.unmarshal_portable(&data).expect("unexpected error");
        assert!(tail.is_empty(), "unexpected non-empty tail");
        compare_blocks_portable(&b2, b, &bh_expected);

        // Verify non-empty prefix and suffix.
        let prefix = b"prefix";
        let suffix = b"suffix";
        let mut data = prefix.to_vec();
        b1.marshal_portable(&mut data);
        assert_eq!(
            b1.bh.rows_count as usize, rows_count,
            "unexpected number of rows marshaled"
        );
        assert!(data.starts_with(prefix), "unexpected prefix");
        let mut data = data[prefix.len()..].to_vec();
        data.extend_from_slice(suffix);
        let tail = b2.unmarshal_portable(&data).expect("unexpected error");
        assert_eq!(tail, suffix, "unexpected tail");
        compare_blocks_portable(&b2, b, &bh_expected);
    }

    fn compare_blocks_portable(b1: &Block, b_expected: &Block, bh_expected: &BlockHeader) {
        assert_eq!(
            b1.bh.min_timestamp, bh_expected.min_timestamp,
            "unexpected MinTimestamp"
        );
        assert_eq!(
            b1.bh.max_timestamp, bh_expected.max_timestamp,
            "unexpected MaxTimestamp"
        );
        assert_eq!(
            b1.bh.first_value, bh_expected.first_value,
            "unexpected FirstValue"
        );
        assert_eq!(
            b1.bh.rows_count, bh_expected.rows_count,
            "unexpected RowsCount"
        );
        assert_eq!(b1.bh.scale, bh_expected.scale, "unexpected Scale");
        assert_eq!(
            b1.bh.timestamps_marshal_type, bh_expected.timestamps_marshal_type,
            "unexpected TimestampsMarshalType"
        );
        assert_eq!(
            b1.bh.values_marshal_type, bh_expected.values_marshal_type,
            "unexpected ValuesMarshalType"
        );
        assert_eq!(
            b1.bh.precision_bits, bh_expected.precision_bits,
            "unexpected PrecisionBits"
        );

        let timestamps_expected =
            timestamps_for_precision_bits(&b_expected.timestamps, bh_expected.precision_bits);
        let values_expected =
            values_for_precision_bits(&b_expected.values, bh_expected.precision_bits);

        assert_eq!(
            b1.values, values_expected,
            "unexpected values for precisionBits={}",
            bh_expected.precision_bits
        );
        assert_eq!(
            b1.timestamps, timestamps_expected,
            "unexpected timestamps for precisionBits={}",
            bh_expected.precision_bits
        );
        assert_eq!(b1.values.len(), bh_expected.rows_count as usize);
        assert_eq!(b1.timestamps.len(), bh_expected.rows_count as usize);
    }

    fn timestamps_for_precision_bits(timestamps: &[i64], precision_bits: u8) -> Vec<i64> {
        let mut data = Vec::new();
        let (marshal_type, first_timestamp) =
            encoding::marshal_timestamps(&mut data, timestamps, precision_bits);
        let mut adjusted = Vec::new();
        encoding::unmarshal_timestamps(
            &mut adjusted,
            &data,
            marshal_type,
            first_timestamp,
            timestamps.len(),
        )
        .unwrap_or_else(|err| {
            panic!("BUG: cannot unmarshal timestamps with precisionBits {precision_bits}: {err}")
        });
        let min_timestamp = timestamps[0];
        let max_timestamp = timestamps[timestamps.len() - 1];
        encoding::ensure_non_decreasing_sequence(&mut adjusted, min_timestamp, max_timestamp);
        adjusted
    }

    fn values_for_precision_bits(values: &[i64], precision_bits: u8) -> Vec<i64> {
        let mut data = Vec::new();
        let (marshal_type, first_value) =
            encoding::marshal_values(&mut data, values, precision_bits);
        let mut adjusted = Vec::new();
        encoding::unmarshal_values(
            &mut adjusted,
            &data,
            marshal_type,
            first_value,
            values.len(),
        )
        .unwrap_or_else(|err| {
            panic!("BUG: cannot unmarshal values with precisionBits {precision_bits}: {err}")
        });
        adjusted
    }

    fn rand_values(state: &mut u64, rows_count: usize) -> Vec<i64> {
        (0..rows_count)
            .map(|_| (splitmix64(state) % 100_000) as i64 - 50_000)
            .collect()
    }

    fn rand_timestamps(state: &mut u64, rows_count: usize) -> Vec<i64> {
        let mut ts = (splitmix64(state) % 1_000_000_000) as i64;
        (0..rows_count)
            .map(|_| {
                let cur = ts;
                ts += (splitmix64(state) % 100_000) as i64;
                cur
            })
            .collect()
    }

    // Full marshal_data/unmarshal_data roundtrip through the non-portable
    // (on-disk) representation with lossless precision.
    #[test]
    fn block_marshal_unmarshal_data_roundtrip() {
        let tsid = Tsid {
            metric_id: 42,
            ..Default::default()
        };
        let mut state = 42u64;
        for rows_count in [1usize, 2, 63, 1000, MAX_ROWS_PER_BLOCK] {
            let timestamps = rand_timestamps(&mut state, rows_count);
            let values = rand_values(&mut state, rows_count);
            let mut b = Block::default();
            b.init(&tsid, &timestamps, &values, 3, 64);
            assert_eq!(b.bh.min_timestamp, timestamps[0]);
            assert_eq!(b.bh.max_timestamp, timestamps[rows_count - 1]);

            let (header_data, timestamps_data, values_data) = b.marshal_data(17, 39);
            let (header_data, timestamps_data, values_data) = (
                header_data.to_vec(),
                timestamps_data.to_vec(),
                values_data.to_vec(),
            );

            let mut b1 = Block::default();
            let tail = b1.bh.unmarshal(&header_data).expect("cannot unmarshal bh");
            assert!(tail.is_empty());
            assert_eq!(b1.bh.tsid, tsid);
            assert_eq!(b1.bh.timestamps_block_offset, 17);
            assert_eq!(b1.bh.values_block_offset, 39);
            assert_eq!(b1.bh.rows_count as usize, rows_count);
            assert_eq!(b1.bh.scale, 3);
            b1.timestamps_data = timestamps_data;
            b1.values_data = values_data;
            b1.unmarshal_data().expect("cannot unmarshal data");
            assert_eq!(b1.timestamps, timestamps);
            assert_eq!(b1.values, values);
        }
    }

    // Roundtrip with decimal conversion and special values
    // (StaleNaN, +/-Inf) at lossless precision.
    #[test]
    fn block_roundtrip_special_values() {
        use esm_common::decimal::STALE_NAN;
        let float_values = [1.5, f64::INFINITY, STALE_NAN, -42.0, f64::NEG_INFINITY, 0.0];
        let timestamps: Vec<i64> = (0..float_values.len() as i64).map(|i| i * 1000).collect();
        let mut values = Vec::new();
        let scale = decimal::append_float_to_decimal(&mut values, &float_values);

        let mut b = Block::default();
        b.init(&Tsid::default(), &timestamps, &values, scale, 64);
        let mut data = Vec::new();
        b.marshal_portable(&mut data);

        let mut b1 = Block::default();
        let tail = b1.unmarshal_portable(&data).expect("cannot unmarshal");
        assert!(tail.is_empty());

        let mut dst_timestamps = Vec::new();
        let mut dst_values = Vec::new();
        b1.append_rows_with_time_range_filter(
            &mut dst_timestamps,
            &mut dst_values,
            TimeRange {
                min_timestamp: 0,
                max_timestamp: i64::MAX,
            },
        );
        assert_eq!(dst_timestamps, timestamps);
        assert_eq!(dst_values.len(), float_values.len());
        for (got, want) in dst_values.iter().zip(&float_values) {
            if decimal::is_stale_nan(*want) {
                assert!(decimal::is_stale_nan(*got), "expected StaleNaN, got {got}");
            } else {
                assert_eq!(got, want);
            }
        }
    }

    #[test]
    fn append_rows_with_time_range_filter_trims() {
        let timestamps = [0i64, 1000, 2000, 3000, 4000];
        let values = [10i64, 11, 12, 13, 14];
        let mut b = Block::default();
        b.init(&Tsid::default(), &timestamps, &values, 0, 64);

        let mut dst_ts = Vec::new();
        let mut dst_vals = Vec::new();
        b.append_rows_with_time_range_filter(
            &mut dst_ts,
            &mut dst_vals,
            TimeRange {
                min_timestamp: 1000,
                max_timestamp: 3000,
            },
        );
        assert_eq!(dst_ts, [1000, 2000, 3000]);
        assert_eq!(dst_vals, [11.0, 12.0, 13.0]);

        // Empty result when the range misses all samples.
        let mut dst_ts = Vec::new();
        let mut dst_vals = Vec::new();
        b.append_rows_with_time_range_filter(
            &mut dst_ts,
            &mut dst_vals,
            TimeRange {
                min_timestamp: 4500,
                max_timestamp: 5000,
            },
        );
        assert!(dst_ts.is_empty());
        assert!(dst_vals.is_empty());
    }

    #[test]
    fn unmarshal_portable_truncated() {
        let timestamps = [0i64, 1000, 2000];
        let values = [1i64, 2, 3];
        let mut b = Block::default();
        b.init(&Tsid::default(), &timestamps, &values, 0, 64);
        let mut data = Vec::new();
        b.marshal_portable(&mut data);

        let mut b1 = Block::default();
        for n in 0..data.len() {
            assert!(
                b1.unmarshal_portable(&data[..n]).is_err(),
                "expected error for {n}-byte src out of {}",
                data.len()
            );
        }
    }

    // Exercises Block::deduplicate_samples_during_merge with the global
    // dedup interval (last sample in each aligned bucket wins).
    #[test]
    fn block_dedup_during_merge() {
        let timestamps = [0i64, 100, 1000, 2000, 2100];
        let values = [1i64, 2, 3, 4, 5];
        let mut b = Block::default();
        b.init(&Tsid::default(), &timestamps, &values, 0, 64);

        // Disabled dedup keeps samples intact.
        dedup::set_dedup_interval(0);
        b.deduplicate_samples_during_merge();
        assert_eq!(b.timestamps(), timestamps);
        assert_eq!(b.values(), values);

        dedup::set_dedup_interval(1000);
        b.deduplicate_samples_during_merge();
        dedup::set_dedup_interval(0);
        assert_eq!(b.timestamps(), [0, 1000, 2000, 2100]);
        assert_eq!(b.values(), [1, 3, 4, 5]);
    }

    #[test]
    fn check_timestamps_bounds_errors() {
        assert!(check_timestamps_bounds(&[], 0, 10).is_ok());
        assert!(check_timestamps_bounds(&[0, 5, 10], 0, 10).is_ok());
        // First row below min.
        assert!(check_timestamps_bounds(&[-1, 5], 0, 10).is_err());
        // Unordered rows.
        assert!(check_timestamps_bounds(&[5, 4], 0, 10).is_err());
        // Last row above max.
        assert!(check_timestamps_bounds(&[0, 11], 0, 10).is_err());
    }
}
