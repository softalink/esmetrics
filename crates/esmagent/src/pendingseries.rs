//! Accumulates [`OwnedSeries`] and marshals them into snappy-compressed
//! `WriteRequest` blocks once enough has accumulated.
//!
//! Port of the block-accumulation shape in upstream vmagent's
//! `app/vmagent/remotewrite/pendingseries.go`: series are appended to an
//! in-memory buffer; once the buffer's *estimated uncompressed* size reaches
//! `-remoteWrite.maxBlockSize` (default 8MiB upstream), the buffer is
//! protobuf-encoded and snappy-compressed into one block and cleared.

use esm_protoparser::prompb::{Label, TimeSeries};
use esm_protoparser::prompb_encode::encode_and_compress;

use crate::series::OwnedSeries;

/// Rough per-sample cost (value: f64 + timestamp: i64) used by the size
/// estimate below. Matches the wire size of the two fixed-width fields; it
/// does not account for protobuf tag/length overhead, which is intentional
/// — this is a threshold heuristic, not an exact byte count.
const BYTES_PER_SAMPLE_ESTIMATE: usize = 16;

/// Maximum samples per block before it is sealed, regardless of estimated
/// size. Port of upstream's `-remoteWrite.maxRowsPerBlock` default (10000) in
/// `pendingseries.go`'s `tryPushTimeSeries`.
const MAX_ROWS_PER_BLOCK: usize = 10_000;

/// Maximum labels per block before it is sealed. Port of upstream's
/// `maxLabelsPerBlock = 10 * maxRowsPerBlock` (100000).
const MAX_LABELS_PER_BLOCK: usize = 10 * MAX_ROWS_PER_BLOCK;

/// Buffers [`OwnedSeries`] until their estimated uncompressed encoded size
/// reaches `max_block_size` — or the buffered sample count reaches
/// [`MAX_ROWS_PER_BLOCK`] or label count reaches [`MAX_LABELS_PER_BLOCK`] —
/// then marshals the buffered series into one snappy-compressed
/// `WriteRequest` block.
pub struct PendingSeries {
    buffered: Vec<OwnedSeries>,
    max_block_size: usize,
    est_bytes: usize,
    samples: usize,
    labels: usize,
}

impl PendingSeries {
    /// Creates an empty buffer that flushes a block once the estimated
    /// uncompressed size of buffered series reaches `max_block_size` bytes.
    pub fn new(max_block_size: usize) -> Self {
        Self {
            buffered: Vec::new(),
            max_block_size,
            est_bytes: 0,
            samples: 0,
            labels: 0,
        }
    }

    /// Appends `s` to the buffer. Whenever the accumulated estimated size
    /// reaches `max_block_size`, marshals the buffered series into one
    /// snappy-compressed block, clears the buffer, and continues — so a
    /// single `add` call carrying many series can return multiple blocks.
    ///
    /// A block that fails to encode (see [`encode_and_compress`]) is
    /// dropped silently rather than returned or panicked on: it can't be
    /// sent either way, and encode errors are not expected for in-memory
    /// data (snappy only errors above ~4GiB).
    pub fn add(&mut self, s: &[OwnedSeries]) -> Vec<Vec<u8>> {
        let mut blocks = Vec::new();
        for series in s {
            self.est_bytes += estimate_size(series);
            self.samples += series.samples.len();
            self.labels += series.labels.len();
            self.buffered.push(series.clone());
            if self.est_bytes >= self.max_block_size
                || self.samples >= MAX_ROWS_PER_BLOCK
                || self.labels >= MAX_LABELS_PER_BLOCK
            {
                if let Some(block) = self.take_block() {
                    blocks.push(block);
                }
            }
        }
        blocks
    }

    /// Marshals any remaining buffered series into a final block. Returns
    /// `None` if nothing is buffered.
    pub fn flush(&mut self) -> Option<Vec<u8>> {
        self.take_block()
    }

    /// Encodes and clears the current buffer, if non-empty.
    fn take_block(&mut self) -> Option<Vec<u8>> {
        if self.buffered.is_empty() {
            return None;
        }
        let prompb_series = to_prompb(&self.buffered);
        let block = encode_and_compress(&prompb_series).ok();
        self.buffered.clear();
        self.est_bytes = 0;
        self.samples = 0;
        self.labels = 0;
        block
    }
}

/// Estimates the uncompressed wire size of `series`: label name/value byte
/// lengths plus a fixed per-sample cost. Rough on purpose — it drives a
/// batching threshold, not an exact accounting of protobuf framing
/// overhead.
fn estimate_size(series: &OwnedSeries) -> usize {
    let labels_bytes: usize = series
        .labels
        .iter()
        .map(|l| l.name.len() + l.value.len())
        .sum();
    labels_bytes + series.samples.len() * BYTES_PER_SAMPLE_ESTIMATE
}

/// Borrows each [`OwnedSeries`] in `series` as a [`TimeSeries`] for encoding:
/// label name/value `String`s become `&[u8]` borrows, and samples are
/// reused as-is (`Sample` is `Copy`).
fn to_prompb(series: &[OwnedSeries]) -> Vec<TimeSeries<'_>> {
    series
        .iter()
        .map(|s| TimeSeries {
            labels: s
                .labels
                .iter()
                .map(|l| Label {
                    name: l.name.as_bytes(),
                    value: l.value.as_bytes(),
                })
                .collect(),
            samples: s.samples.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(name: &str) -> OwnedSeries {
        OwnedSeries {
            labels: vec![esm_relabel::Label {
                name: "__name__".into(),
                value: name.into(),
            }],
            samples: vec![esm_protoparser::prompb::Sample {
                value: 1.0,
                timestamp: 1,
            }],
        }
    }

    #[test]
    fn flush_emits_a_decodable_block() {
        let mut ps = PendingSeries::new(8 * 1024 * 1024);
        let full = ps.add(&[series("a"), series("b")]);
        assert!(full.is_empty()); // under block size, nothing emitted yet
        let block = ps.flush().expect("a block");
        let raw = snap::raw::Decoder::new().decompress_vec(&block).unwrap();
        let wr = esm_protoparser::prompb::unmarshal_write_request(&raw).unwrap();
        assert_eq!(wr.timeseries.len(), 2);
    }

    #[test]
    fn add_emits_block_when_full() {
        let mut ps = PendingSeries::new(1); // tiny -> every add flushes
        let blocks = ps.add(&[series("a")]);
        assert_eq!(blocks.len(), 1);
    }

    #[test]
    fn flush_on_empty_buffer_returns_none() {
        let mut ps = PendingSeries::new(8 * 1024 * 1024);
        assert!(ps.flush().is_none());
    }

    /// N labels, no samples: isolates the label-count seal path from the
    /// row-count one (samples stay at 0, so `MAX_ROWS_PER_BLOCK` never trips).
    fn multi_label_series(n: usize) -> OwnedSeries {
        OwnedSeries {
            labels: (0..n)
                .map(|i| esm_relabel::Label {
                    name: format!("l{i}"),
                    value: "v".into(),
                })
                .collect(),
            samples: vec![],
        }
    }

    #[test]
    fn add_seals_on_row_count_threshold() {
        // Huge byte cap so only the row count can seal the block.
        let mut ps = PendingSeries::new(usize::MAX);
        let batch: Vec<OwnedSeries> = (0..MAX_ROWS_PER_BLOCK)
            .map(|i| series(&format!("m{i}")))
            .collect();
        let blocks = ps.add(&batch);
        assert_eq!(blocks.len(), 1, "should seal once at MAX_ROWS_PER_BLOCK");
        assert!(ps.flush().is_none(), "buffer emptied after the seal");
    }

    #[test]
    fn add_seals_on_label_count_threshold() {
        let mut ps = PendingSeries::new(usize::MAX);
        // 11 labels/series, 0 samples -> labels reach the cap before rows do.
        let per = 11;
        let count = MAX_LABELS_PER_BLOCK / per + 1;
        let batch: Vec<OwnedSeries> = (0..count).map(|_| multi_label_series(per)).collect();
        let blocks = ps.add(&batch);
        assert_eq!(blocks.len(), 1, "should seal once at MAX_LABELS_PER_BLOCK");
    }

    #[test]
    fn add_can_return_multiple_blocks_in_one_call() {
        let mut ps = PendingSeries::new(1); // tiny -> every add flushes
        let blocks = ps.add(&[series("a"), series("b"), series("c")]);
        assert_eq!(blocks.len(), 3);
        for block in &blocks {
            let raw = snap::raw::Decoder::new().decompress_vec(block).unwrap();
            let wr = esm_protoparser::prompb::unmarshal_write_request(&raw).unwrap();
            assert_eq!(wr.timeseries.len(), 1);
        }
        assert!(ps.flush().is_none());
    }
}
