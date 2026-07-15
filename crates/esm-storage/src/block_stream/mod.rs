//! Stage-2 block stream machinery: streaming writer/reader over the four
//! part files (`timestamps.bin`, `values.bin`, `index.bin`,
//! `metaindex.bin`) and the k-way block stream merger.
//!
//! Ports of lib/storage `block_stream_writer.go`, `block_stream_reader.go`,
//! `block_stream_merger.go` and `merge.go`.

mod merger;
mod reader;
mod writer;

pub use merger::{merge_block_streams, MergeError};
pub use reader::BlockStreamReader;
pub use writer::{
    get_compress_level, timestamps_blocks_merged, timestamps_bytes_saved, BlockStreamWriter,
};

use crate::block::Block;
use crate::block_header::BlockHeader;
use esm_encoding as encoding;
use esm_encoding::MarshalType;

/// Unmarshals the packed on-disk representation (`timestamps_data`,
/// `values_data` plus the block header) into `dst`, leaving `dst` in the
/// unpacked state (`timestamps`/`values` filled).
///
/// PORT-NOTE: Go keeps the packed byte blobs inside `Block`
/// (`Block.timestampsData`/`valuesData`) and unmarshals lazily via
/// `Block.UnmarshalData`. Those fields are private to the stage-1 `block`
/// module, so the stream reader and `BlockRef::read_block` decode blocks
/// eagerly with this helper instead. The decode logic below replicates
/// `Block.UnmarshalData` (block.go) exactly; calling `Block::unmarshal_data`
/// on the result is a validated no-op, so callers written against the Go
/// semantics keep working.
pub(crate) fn unmarshal_block_data(
    dst: &mut Block,
    bh: &BlockHeader,
    timestamps_data: &[u8],
    values_data: &[u8],
) -> Result<(), String> {
    dst.reset();
    dst.bh = *bh;

    if bh.rows_count == 0 {
        return Err(format!(
            "RowsCount must be greater than 0; got {}",
            bh.rows_count
        ));
    }

    let timestamps_marshal_type =
        MarshalType::from_u8(bh.timestamps_marshal_type).ok_or_else(|| {
            format!(
                "unsupported TimestampsMarshalType: {}",
                bh.timestamps_marshal_type
            )
        })?;
    encoding::unmarshal_timestamps(
        &mut dst.timestamps,
        timestamps_data,
        timestamps_marshal_type,
        bh.min_timestamp,
        bh.rows_count as usize,
    )?;
    if bh.precision_bits < 64 {
        // Recover timestamps order after lossy compression.
        encoding::ensure_non_decreasing_sequence(
            &mut dst.timestamps,
            bh.min_timestamp,
            bh.max_timestamp,
        );
    } else if timestamps_marshal_type.needs_validation() {
        // Ensure timestamps are in the range [MinTimestamp...MaxTimestamp]
        // and are ordered.
        check_timestamps_bounds(&dst.timestamps, bh.min_timestamp, bh.max_timestamp)?;
    }

    let values_marshal_type = MarshalType::from_u8(bh.values_marshal_type)
        .ok_or_else(|| format!("unsupported ValuesMarshalType: {}", bh.values_marshal_type))?;
    encoding::unmarshal_values(
        &mut dst.values,
        values_data,
        values_marshal_type,
        bh.first_value,
        bh.rows_count as usize,
    )?;

    if dst.timestamps.len() != dst.values.len() {
        return Err(format!(
            "timestamps and values count mismatch; got {} vs {}",
            dst.timestamps.len(),
            dst.values.len()
        ));
    }

    Ok(())
}

/// Replica of the private `checkTimestampsBounds` from block.go, needed by
/// [`unmarshal_block_data`].
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
                "timestamp for the row {} cannot be smaller than the timestamp for the row {} \
                 (total {} rows); got {} vs {}",
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
    use crate::tsid::Tsid;

    // unmarshal_block_data must be the exact inverse of Block::marshal_data,
    // matching Block::unmarshal_data behavior.
    #[test]
    fn unmarshal_block_data_roundtrip() {
        let tsid = Tsid {
            metric_id: 99,
            ..Default::default()
        };
        let timestamps: Vec<i64> = (0..1000i64).map(|i| i * 30_000).collect();
        let values: Vec<i64> = (0..1000i64).map(|i| i * 7 - 3500).collect();

        let mut b = Block::default();
        b.init(&tsid, &timestamps, &values, 2, 64);
        let (header_data, ts_data, v_data) = b.marshal_data(0, 0);
        let (header_data, ts_data, v_data) =
            (header_data.to_vec(), ts_data.to_vec(), v_data.to_vec());

        let mut bh = BlockHeader::default();
        let tail = bh.unmarshal(&header_data).unwrap();
        assert!(tail.is_empty());

        let mut b2 = Block::default();
        unmarshal_block_data(&mut b2, &bh, &ts_data, &v_data).unwrap();
        assert_eq!(b2.timestamps(), &timestamps[..]);
        assert_eq!(b2.values(), &values[..]);
        assert_eq!(b2.header().tsid, tsid);
        assert_eq!(b2.header().scale, 2);
        // Calling unmarshal_data on an already-unpacked block is a no-op.
        b2.unmarshal_data().unwrap();
        assert_eq!(b2.timestamps().len(), 1000);
    }

    #[test]
    fn unmarshal_block_data_rejects_corrupt_header() {
        let bh = BlockHeader {
            rows_count: 0,
            ..Default::default()
        };
        let mut b = Block::default();
        assert!(unmarshal_block_data(&mut b, &bh, &[], &[]).is_err());

        let bh = BlockHeader {
            rows_count: 1,
            timestamps_marshal_type: 200,
            ..Default::default()
        };
        assert!(unmarshal_block_data(&mut b, &bh, &[], &[]).is_err());
    }
}
