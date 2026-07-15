//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/raw_row.go and raw_block.go
//! (data types and sorting).
//!
//! PORT-NOTE (stage 2): Go's `rawRowsMarshaler.marshalToInmemoryPart` — the
//! rawRows → inmemoryPart conversion (grouping sorted rows into ≤8192-row
//! blocks per TSID, `decimal::append_float_to_decimal`, then
//! `blockStreamWriter.WriteExternalBlock`) — depends on `blockStreamWriter`
//! and `inmemoryPart` and is ported in stage 2 (part layer).

use crate::tsid::Tsid;
use std::cmp::Ordering;

/// RawRow represents a raw timeseries row. Go: rawRow.
#[derive(Debug, Default, Clone, Copy)]
pub struct RawRow {
    /// The time series id.
    pub tsid: Tsid,

    /// Unix timestamp in milliseconds.
    pub timestamp: i64,

    /// The time series value for the given timestamp.
    pub value: f64,

    /// The number of significant bits in the value to store.
    /// Possible values are [1..64]; 64 means the value is stored without
    /// information loss.
    pub precision_bits: u8,
}

/// The `(TSID, Timestamp)` comparator used for sorting raw rows before
/// grouping them into blocks. Go: rawRowsSort.Less.
#[inline]
fn raw_row_cmp(a: &RawRow, b: &RawRow) -> Ordering {
    a.tsid
        .cmp(&b.tsid)
        .then_with(|| a.timestamp.cmp(&b.timestamp))
}

/// Sorts `rows` by `(TSID, Timestamp)` if they aren't sorted yet.
/// Go: the sort step of rawRowsMarshaler.marshalToInmemoryPart.
pub fn sort_raw_rows(rows: &mut [RawRow]) {
    if !rows.is_sorted_by(|a, b| raw_row_cmp(a, b) != Ordering::Greater) {
        rows.sort_unstable_by(raw_row_cmp);
    }
}

/// RawBlock represents a raw block of a single time-series rows.
/// Go: rawBlock.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct RawBlock {
    pub tsid: Tsid,
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
}

impl RawBlock {
    /// Resets the raw block. Go: rawBlock.Reset.
    pub fn reset(&mut self) {
        self.tsid = Tsid::default();
        self.timestamps.clear();
        self.values.clear();
    }

    /// Copies `src` to `self`. Go: rawBlock.CopyFrom.
    pub fn copy_from(&mut self, src: &RawBlock) {
        self.tsid = src.tsid;
        self.timestamps.clear();
        self.timestamps.extend_from_slice(&src.timestamps);
        self.values.clear();
        self.values.extend_from_slice(&src.values);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(
        metric_group_id: u64,
        job_id: u32,
        instance_id: u32,
        metric_id: u64,
        timestamp: i64,
    ) -> RawRow {
        RawRow {
            tsid: Tsid {
                metric_group_id,
                job_id,
                instance_id,
                metric_id,
            },
            timestamp,
            value: 0.0,
            precision_bits: 64,
        }
    }

    // Sorting must order by all TSID fields first (in TSID.Less field
    // order), then by timestamp.
    #[test]
    fn sort_raw_rows_order() {
        let sorted = vec![
            row(1, 0, 0, 9, 100),
            row(1, 0, 0, 9, 200),
            row(1, 0, 1, 3, 50),
            row(1, 2, 0, 1, 10),
            row(2, 0, 0, 0, 5),
            row(2, 0, 0, 1, 1),
        ];

        // Reverse and re-sort.
        let mut rows = sorted.clone();
        rows.reverse();
        sort_raw_rows(&mut rows);

        let got: Vec<(Tsid, i64)> = rows.iter().map(|r| (r.tsid, r.timestamp)).collect();
        let want: Vec<(Tsid, i64)> = sorted.iter().map(|r| (r.tsid, r.timestamp)).collect();
        assert_eq!(got, want);

        // Sorting already-sorted rows keeps them intact.
        sort_raw_rows(&mut rows);
        let got: Vec<(Tsid, i64)> = rows.iter().map(|r| (r.tsid, r.timestamp)).collect();
        assert_eq!(got, want);
    }

    #[test]
    fn raw_block_reset_copy() {
        let mut rb = RawBlock {
            tsid: Tsid {
                metric_id: 7,
                ..Default::default()
            },
            timestamps: vec![1, 2, 3],
            values: vec![1.0, 2.0, 3.0],
        };

        let mut rb2 = RawBlock::default();
        rb2.copy_from(&rb);
        assert_eq!(rb, rb2);

        rb.reset();
        assert_eq!(rb, RawBlock::default());
        // The copy is unaffected.
        assert_eq!(rb2.timestamps, [1, 2, 3]);
    }
}
