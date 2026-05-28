//! `BlockHeader` — fixed-size 81-byte header for one time-series block.
//!
//! Format reference: `docs/format/timeseries-part.md` §5.
//! VM source: `lib/storage/block_header.go:19-156`.

use esm_compress::int::{
    DecodeError, marshal_uint32, marshal_uint64, unmarshal_uint32, unmarshal_uint64,
};
use esm_compress::timeseries::{MarshalType, MarshalTypeByteError};
use thiserror::Error;

use super::{MAX_BLOCK_SIZE, MAX_ROWS_PER_BLOCK, Tsid};

/// Fixed on-disk size in bytes of a time-series block header
/// (`docs/format/timeseries-part.md` §5).
pub const SIZE: usize = 81;

/// Per-block header. Index blocks are concatenations of `BlockHeader` rows
/// sorted by `(tsid, min_timestamp)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    pub tsid: Tsid,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub first_value: i64,
    pub timestamps_block_offset: u64,
    pub values_block_offset: u64,
    pub timestamps_block_size: u32,
    pub values_block_size: u32,
    pub rows_count: u32,
    pub scale: i16,
    pub timestamps_marshal_type: MarshalType,
    pub values_marshal_type: MarshalType,
    pub precision_bits: u8,
}

impl Default for BlockHeader {
    fn default() -> Self {
        Self {
            tsid: Tsid::default(),
            min_timestamp: 0,
            max_timestamp: 0,
            first_value: 0,
            timestamps_block_offset: 0,
            values_block_offset: 0,
            timestamps_block_size: 0,
            values_block_size: 0,
            rows_count: 0,
            scale: 0,
            timestamps_marshal_type: MarshalType::Const,
            values_marshal_type: MarshalType::Const,
            precision_bits: 64,
        }
    }
}

impl BlockHeader {
    /// Append the on-disk byte representation to `dst`. Order matches
    /// `lib/storage/block_header.go:103-115`.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        self.tsid.marshal(dst);
        marshal_int64(dst, self.min_timestamp);
        marshal_int64(dst, self.max_timestamp);
        marshal_int64(dst, self.first_value);
        marshal_uint64(dst, self.timestamps_block_offset);
        marshal_uint64(dst, self.values_block_offset);
        marshal_uint32(dst, self.timestamps_block_size);
        marshal_uint32(dst, self.values_block_size);
        marshal_uint32(dst, self.rows_count);
        marshal_int16(dst, self.scale);
        dst.push(self.timestamps_marshal_type.as_byte());
        dst.push(self.values_marshal_type.as_byte());
        dst.push(self.precision_bits);
    }

    /// Parse one header from the head of `src`. Returns the header plus the
    /// unconsumed remainder.
    ///
    /// # Errors
    /// See [`BlockHeaderError`].
    pub fn unmarshal(src: &[u8]) -> Result<(Self, &[u8]), BlockHeaderError> {
        if src.len() < SIZE {
            return Err(BlockHeaderError::Truncated { needed: SIZE, have: src.len() });
        }

        let (tsid, src) = Tsid::unmarshal(src).map_err(BlockHeaderError::Tsid)?;
        let (min_timestamp, n) = unmarshal_int64(src)?;
        let src = &src[n..];
        let (max_timestamp, n) = unmarshal_int64(src)?;
        let src = &src[n..];
        let (first_value, n) = unmarshal_int64(src)?;
        let src = &src[n..];

        let (timestamps_block_offset, n) =
            unmarshal_uint64(src).map_err(BlockHeaderError::Field)?;
        let src = &src[n..];
        let (values_block_offset, n) = unmarshal_uint64(src).map_err(BlockHeaderError::Field)?;
        let src = &src[n..];
        let (timestamps_block_size, n) = unmarshal_uint32(src).map_err(BlockHeaderError::Field)?;
        let src = &src[n..];
        let (values_block_size, n) = unmarshal_uint32(src).map_err(BlockHeaderError::Field)?;
        let src = &src[n..];
        let (rows_count, n) = unmarshal_uint32(src).map_err(BlockHeaderError::Field)?;
        let src = &src[n..];
        let (scale, n) = unmarshal_int16(src)?;
        let src = &src[n..];

        let (&ts_mt, rest) = src.split_first().ok_or(BlockHeaderError::TruncatedTrailing)?;
        let (&v_mt, rest) = rest.split_first().ok_or(BlockHeaderError::TruncatedTrailing)?;
        let (&precision_bits, rest) =
            rest.split_first().ok_or(BlockHeaderError::TruncatedTrailing)?;
        let src = rest;

        let timestamps_marshal_type =
            MarshalType::from_byte(ts_mt).map_err(BlockHeaderError::TimestampsType)?;
        let values_marshal_type =
            MarshalType::from_byte(v_mt).map_err(BlockHeaderError::ValuesType)?;

        let bh = Self {
            tsid,
            min_timestamp,
            max_timestamp,
            first_value,
            timestamps_block_offset,
            values_block_offset,
            timestamps_block_size,
            values_block_size,
            rows_count,
            scale,
            timestamps_marshal_type,
            values_marshal_type,
            precision_bits,
        };
        bh.validate()?;
        Ok((bh, src))
    }

    /// Apply VM's validation rules (`lib/storage/block_header.go:232-255`).
    ///
    /// # Errors
    /// See [`BlockHeaderError`].
    pub fn validate(&self) -> Result<(), BlockHeaderError> {
        if self.rows_count == 0 {
            return Err(BlockHeaderError::ZeroRowsCount);
        }
        if self.rows_count > 2 * MAX_ROWS_PER_BLOCK {
            return Err(BlockHeaderError::RowsCountTooLarge(self.rows_count));
        }
        if self.precision_bits == 0 || self.precision_bits > 64 {
            return Err(BlockHeaderError::PrecisionBitsOutOfRange(self.precision_bits));
        }
        if self.timestamps_block_size > 2 * MAX_BLOCK_SIZE {
            return Err(BlockHeaderError::TimestampsBlockTooLarge(self.timestamps_block_size));
        }
        if self.values_block_size > 2 * MAX_BLOCK_SIZE {
            return Err(BlockHeaderError::ValuesBlockTooLarge(self.values_block_size));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum BlockHeaderError {
    #[error("truncated block header: need {needed} bytes, have {have}")]
    Truncated { needed: usize, have: usize },
    #[error("truncated block header at trailing marshal-type / precision bytes")]
    TruncatedTrailing,
    #[error("cannot unmarshal TSID: {0}")]
    Tsid(DecodeError),
    #[error("cannot unmarshal field: {0}")]
    Field(DecodeError),
    #[error("invalid timestamps marshal type: {0}")]
    TimestampsType(#[source] MarshalTypeByteError),
    #[error("invalid values marshal type: {0}")]
    ValuesType(#[source] MarshalTypeByteError),
    #[error("rows_count must be > 0")]
    ZeroRowsCount,
    #[error("rows_count={0} exceeds 2 * MAX_ROWS_PER_BLOCK")]
    RowsCountTooLarge(u32),
    #[error("precision_bits {0} not in [1, 64]")]
    PrecisionBitsOutOfRange(u8),
    #[error("timestamps_block_size={0} exceeds 2 * MAX_BLOCK_SIZE")]
    TimestampsBlockTooLarge(u32),
    #[error("values_block_size={0} exceeds 2 * MAX_BLOCK_SIZE")]
    ValuesBlockTooLarge(u32),
}

// ---- BE int16/int64 helpers (VM `lib/encoding/int.go:50-83`) ---------------

fn marshal_int16(dst: &mut Vec<u8>, v: i16) {
    dst.extend_from_slice(&v.to_be_bytes());
}

fn unmarshal_int16(src: &[u8]) -> Result<(i16, usize), BlockHeaderError> {
    if src.len() < 2 {
        return Err(BlockHeaderError::Truncated { needed: 2, have: src.len() });
    }
    Ok((i16::from_be_bytes([src[0], src[1]]), 2))
}

fn marshal_int64(dst: &mut Vec<u8>, v: i64) {
    dst.extend_from_slice(&v.to_be_bytes());
}

fn unmarshal_int64(src: &[u8]) -> Result<(i64, usize), BlockHeaderError> {
    if src.len() < 8 {
        return Err(BlockHeaderError::Truncated { needed: 8, have: src.len() });
    }
    let v = i64::from_be_bytes([src[0], src[1], src[2], src[3], src[4], src[5], src[6], src[7]]);
    Ok((v, 8))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> BlockHeader {
        BlockHeader {
            tsid: Tsid { metric_group_id: 100, job_id: 7, instance_id: 11, metric_id: 9_999 },
            min_timestamp: 1_700_000_000_000,
            max_timestamp: 1_700_001_000_000,
            first_value: 42,
            timestamps_block_offset: 0x100,
            values_block_offset: 0x200,
            timestamps_block_size: 1024,
            values_block_size: 2048,
            rows_count: 500,
            scale: -3,
            timestamps_marshal_type: MarshalType::ZstdNearestDelta2,
            values_marshal_type: MarshalType::ZstdNearestDelta,
            precision_bits: 64,
        }
    }

    #[test]
    fn marshal_length_is_81() {
        let mut buf = Vec::new();
        sample().marshal(&mut buf);
        assert_eq!(buf.len(), SIZE);
        assert_eq!(SIZE, 81);
    }

    #[test]
    fn roundtrip() {
        let bh = sample();
        let mut buf = Vec::new();
        bh.marshal(&mut buf);
        let (decoded, rest) = BlockHeader::unmarshal(&buf).unwrap();
        assert!(rest.is_empty());
        assert_eq!(decoded, bh);
    }

    #[test]
    fn zero_rows_count_rejected() {
        let mut bh = sample();
        bh.rows_count = 0;
        let mut buf = Vec::new();
        bh.marshal(&mut buf);
        assert!(matches!(BlockHeader::unmarshal(&buf), Err(BlockHeaderError::ZeroRowsCount)));
    }

    #[test]
    fn precision_bits_out_of_range_rejected() {
        let mut bh = sample();
        bh.precision_bits = 0;
        let mut buf = Vec::new();
        bh.marshal(&mut buf);
        assert!(matches!(
            BlockHeader::unmarshal(&buf),
            Err(BlockHeaderError::PrecisionBitsOutOfRange(0))
        ));
    }

    #[test]
    fn truncated_input_rejected() {
        let bad = [0u8; 50];
        assert!(matches!(BlockHeader::unmarshal(&bad), Err(BlockHeaderError::Truncated { .. })));
    }

    #[test]
    fn invalid_marshal_type_byte_rejected() {
        // Encode a header, then corrupt the timestamps_marshal_type byte
        // (offset 78) to an out-of-range value.
        let bh = sample();
        let mut buf = Vec::new();
        bh.marshal(&mut buf);
        buf[78] = 7; // not in [1, 6]
        assert!(matches!(BlockHeader::unmarshal(&buf), Err(BlockHeaderError::TimestampsType(_))));
    }
}
