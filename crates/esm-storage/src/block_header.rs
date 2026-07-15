//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/block_header.go.

use crate::block::{MAX_BLOCK_SIZE, MAX_ROWS_PER_BLOCK};
use crate::tsid::Tsid;
use esm_encoding as encoding;

/// The size of a marshaled [`BlockHeader`]. Go: marshaledBlockHeaderSize.
///
/// Layout: TSID(24) | MinTimestamp i64 | MaxTimestamp i64 | FirstValue i64 |
/// TimestampsBlockOffset u64 | ValuesBlockOffset u64 | TimestampsBlockSize
/// u32 | ValuesBlockSize u32 | RowsCount u32 | Scale i16 | 3 single bytes
/// (TimestampsMarshalType, ValuesMarshalType, PrecisionBits) = 81 bytes.
/// The i64/i16 fields use the zig-zag big-endian encoding of
/// `encoding::marshal_int64`/`marshal_int16`, matching Go exactly.
pub const MARSHALED_BLOCK_HEADER_SIZE: usize = 81;

/// BlockHeader is a header for a time series block.
///
/// Each block contains rows for a single time series. Rows are sorted by
/// timestamp. A single time series may span multiple blocks.
///
/// Go: blockHeader.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BlockHeader {
    /// TSID for the block. Multiple blocks may have the same TSID.
    pub tsid: Tsid,

    /// The minimum timestamp in the block. This is the first timestamp,
    /// since rows are sorted by timestamps.
    pub min_timestamp: i64,

    /// The maximum timestamp in the block (the last timestamp).
    pub max_timestamp: i64,

    /// The first value in the block, stored here for better compression,
    /// since subsequent values may be delta-encoded against it.
    pub first_value: i64,

    /// The offset in bytes for the block with timestamps in timestamps file.
    pub timestamps_block_offset: u64,

    /// The offset in bytes for the block with values in values file.
    pub values_block_offset: u64,

    /// The size in bytes for the block with timestamps.
    pub timestamps_block_size: u32,

    /// The size in bytes for the block with values.
    pub values_block_size: u32,

    /// The number of rows in the block. The block must contain at least one
    /// row.
    pub rows_count: u32,

    /// The 10^scale multiplier for values in the block.
    pub scale: i16,

    /// The marshal type used for marshaling the block with timestamps
    /// (raw byte; see `esm_encoding::MarshalType` discriminants).
    pub timestamps_marshal_type: u8,

    /// The marshal type used for marshaling the block with values.
    pub values_marshal_type: u8,

    /// The number of significant bits when using NearestDelta encodings.
    /// Possible values are in the range [1...64]; 64 means exact values.
    pub precision_bits: u8,
}

impl BlockHeader {
    /// Returns true if `self` is less than `other` — order by TSID, then by
    /// MinTimestamp. Go: blockHeader.Less.
    pub fn less(&self, other: &BlockHeader) -> bool {
        if self.tsid.metric_id == other.tsid.metric_id {
            // Fast path for identical TSIDs.
            return self.min_timestamp < other.min_timestamp;
        }
        // Slow path for distinct TSIDs.
        self.tsid < other.tsid
    }

    /// Appends marshaled `self` to `dst`. Go: blockHeader.Marshal.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        self.tsid.marshal(dst);
        encoding::marshal_int64(dst, self.min_timestamp);
        encoding::marshal_int64(dst, self.max_timestamp);
        encoding::marshal_int64(dst, self.first_value);
        encoding::marshal_uint64(dst, self.timestamps_block_offset);
        encoding::marshal_uint64(dst, self.values_block_offset);
        encoding::marshal_uint32(dst, self.timestamps_block_size);
        encoding::marshal_uint32(dst, self.values_block_size);
        encoding::marshal_uint32(dst, self.rows_count);
        encoding::marshal_int16(dst, self.scale);
        dst.extend_from_slice(&[
            self.timestamps_marshal_type,
            self.values_marshal_type,
            self.precision_bits,
        ]);
    }

    /// Unmarshals `self` from `src` and returns the rest of `src`.
    /// Go: blockHeader.Unmarshal.
    pub fn unmarshal<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        if src.len() < MARSHALED_BLOCK_HEADER_SIZE {
            return Err(format!(
                "too short block header; got {} bytes; want {} bytes",
                src.len(),
                MARSHALED_BLOCK_HEADER_SIZE
            ));
        }

        let src = self
            .tsid
            .unmarshal(src)
            .map_err(|err| format!("cannot unmarshal TSID: {err}"))?;

        self.min_timestamp = encoding::unmarshal_int64(src);
        let src = &src[8..];
        self.max_timestamp = encoding::unmarshal_int64(src);
        let src = &src[8..];
        self.first_value = encoding::unmarshal_int64(src);
        let src = &src[8..];
        self.timestamps_block_offset = encoding::unmarshal_uint64(src);
        let src = &src[8..];
        self.values_block_offset = encoding::unmarshal_uint64(src);
        let src = &src[8..];
        self.timestamps_block_size = encoding::unmarshal_uint32(src);
        let src = &src[4..];
        self.values_block_size = encoding::unmarshal_uint32(src);
        let src = &src[4..];
        self.rows_count = encoding::unmarshal_uint32(src);
        let src = &src[4..];
        self.scale = encoding::unmarshal_int16(src);
        let src = &src[2..];
        self.timestamps_marshal_type = src[0];
        self.values_marshal_type = src[1];
        self.precision_bits = src[2];
        let src = &src[3..];

        self.validate()?;
        Ok(src)
    }

    /// Portable (varint-based) marshaling for cross-instance migration.
    /// Go: blockHeader.marshalPortable.
    pub(crate) fn marshal_portable(&self, dst: &mut Vec<u8>) {
        encoding::marshal_var_int64(dst, self.min_timestamp);
        encoding::marshal_var_int64(dst, self.max_timestamp);
        encoding::marshal_var_int64(dst, self.first_value);
        encoding::marshal_var_uint64(dst, self.rows_count as u64);
        encoding::marshal_var_int64(dst, self.scale as i64);
        dst.extend_from_slice(&[
            self.timestamps_marshal_type,
            self.values_marshal_type,
            self.precision_bits,
        ]);
    }

    /// Go: blockHeader.unmarshalPortable.
    pub(crate) fn unmarshal_portable<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        let (min_timestamp, n_size) = encoding::unmarshal_var_int64(src)
            .ok_or("cannot unmarshal firstTimestamp from varint")?;
        let src = &src[n_size..];
        self.min_timestamp = min_timestamp;

        let (max_timestamp, n_size) = encoding::unmarshal_var_int64(src)
            .ok_or("cannot unmarshal maxTimestamp from varint")?;
        let src = &src[n_size..];
        self.max_timestamp = max_timestamp;

        let (first_value, n_size) =
            encoding::unmarshal_var_int64(src).ok_or("cannot unmarshal firstValue from varint")?;
        let src = &src[n_size..];
        self.first_value = first_value;

        let (rows_count, n_size) =
            encoding::unmarshal_var_uint64(src).ok_or("cannot unmarshal rowsCount from varuint")?;
        let src = &src[n_size..];
        if rows_count > u32::MAX as u64 {
            return Err(format!(
                "got too big rowsCount={rows_count}; it mustn't exceed {}",
                u32::MAX
            ));
        }
        self.rows_count = rows_count as u32;

        let (scale, n_size) =
            encoding::unmarshal_var_int64(src).ok_or("cannot unmarshal scale from varint")?;
        let src = &src[n_size..];
        if scale < i16::MIN as i64 {
            return Err(format!(
                "got too small scale={scale}; it mustn't be smaller than {}",
                i16::MIN
            ));
        }
        if scale > i16::MAX as i64 {
            return Err(format!(
                "got too big scale={scale}; it mustn't exceed {}",
                i16::MAX
            ));
        }
        self.scale = scale as i16;

        if src.len() < 3 {
            return Err(format!(
                "cannot unmarshal marshalTypes and precisionBits from {} bytes; need at least 3 bytes",
                src.len()
            ));
        }
        self.timestamps_marshal_type = src[0];
        self.values_marshal_type = src[1];
        self.precision_bits = src[2];
        Ok(&src[3..])
    }

    /// Validates the header fields. Go: blockHeader.validate.
    pub fn validate(&self) -> Result<(), String> {
        if self.rows_count == 0 {
            return Err("RowsCount in block header cannot be zero".to_string());
        }
        if self.rows_count as usize > 2 * MAX_ROWS_PER_BLOCK {
            return Err(format!(
                "too big RowsCount; got {}; cannot exceed {}",
                self.rows_count,
                2 * MAX_ROWS_PER_BLOCK
            ));
        }
        encoding::check_marshal_type(self.timestamps_marshal_type)
            .map_err(|err| format!("unsupported TimestampsMarshalType: {err}"))?;
        encoding::check_marshal_type(self.values_marshal_type)
            .map_err(|err| format!("unsupported ValuesMarshalType: {err}"))?;
        encoding::check_precision_bits(self.precision_bits)?;
        if self.timestamps_block_size as usize > 2 * MAX_BLOCK_SIZE {
            return Err(format!(
                "too big TimestampsBlockSize; got {}; cannot exceed {}",
                self.timestamps_block_size,
                2 * MAX_BLOCK_SIZE
            ));
        }
        if self.values_block_size as usize > 2 * MAX_BLOCK_SIZE {
            return Err(format!(
                "too big ValuesBlockSize; got {}; cannot exceed {}",
                self.values_block_size,
                2 * MAX_BLOCK_SIZE
            ));
        }
        Ok(())
    }
}

/// Unmarshals all the block headers from `src` and appends them to `dst`.
/// Block headers must be sorted by TSID. Go: unmarshalBlockHeaders.
pub fn unmarshal_block_headers(
    dst: &mut Vec<BlockHeader>,
    src: &[u8],
    block_headers_count: usize,
) -> Result<(), String> {
    assert!(
        block_headers_count > 0,
        "BUG: blockHeadersCount must be greater than zero; got {block_headers_count}"
    );
    let dst_len = dst.len();
    dst.reserve(block_headers_count);
    let mut bh = BlockHeader::default();
    let mut src = src;
    while !src.is_empty() {
        src = bh
            .unmarshal(src)
            .map_err(|err| format!("cannot unmarshal block header: {err}"))?;
        dst.push(bh);
    }

    let new_bhs = &dst[dst_len..];

    // Verify the number of read block headers.
    if new_bhs.len() != block_headers_count {
        return Err(format!(
            "invalid number of block headers found: {}; want {} block headers",
            new_bhs.len(),
            block_headers_count
        ));
    }

    // Verify that block headers are sorted by tsid.
    if !new_bhs.is_sorted_by(|a, b| a.tsid <= b.tsid) {
        return Err(format!(
            "block headers must be sorted by tsid; unmarshaled unsorted block headers: {new_bhs:?}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Port of TestMarshaledBlockHeaderSize: makes sure marshaled format
    // isn't changed. If this test breaks then the storage format has been
    // changed, so it may become incompatible with the previously written
    // data.
    #[test]
    fn marshaled_block_header_size() {
        let mut dst = Vec::new();
        BlockHeader::default().marshal(&mut dst);
        assert_eq!(dst.len(), 81);
        assert_eq!(MARSHALED_BLOCK_HEADER_SIZE, 81);
    }

    // Golden byte layout (big-endian, zig-zag for the signed fields).
    #[test]
    fn marshal_golden_bytes() {
        let bh = BlockHeader {
            tsid: Tsid {
                metric_id: 1,
                ..Default::default()
            },
            min_timestamp: -1, // zig-zag -> 1
            max_timestamp: 1,  // zig-zag -> 2
            first_value: 2,    // zig-zag -> 4
            timestamps_block_offset: 0x10,
            values_block_offset: 0x20,
            timestamps_block_size: 4,
            values_block_size: 8,
            rows_count: 1,
            scale: -1, // zig-zag -> 1
            timestamps_marshal_type: 1,
            values_marshal_type: 3,
            precision_bits: 64,
        };
        let mut dst = Vec::new();
        bh.marshal(&mut dst);
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0, 0, 0, 0, 0, 0, 0, 0, // MetricGroupID
            0, 0, 0, 0, // JobID
            0, 0, 0, 0, // InstanceID
            0, 0, 0, 0, 0, 0, 0, 1, // MetricID
            0, 0, 0, 0, 0, 0, 0, 1, // MinTimestamp (-1 zig-zag)
            0, 0, 0, 0, 0, 0, 0, 2, // MaxTimestamp (1 zig-zag)
            0, 0, 0, 0, 0, 0, 0, 4, // FirstValue (2 zig-zag)
            0, 0, 0, 0, 0, 0, 0, 0x10, // TimestampsBlockOffset
            0, 0, 0, 0, 0, 0, 0, 0x20, // ValuesBlockOffset
            0, 0, 0, 4, // TimestampsBlockSize
            0, 0, 0, 8, // ValuesBlockSize
            0, 0, 0, 1, // RowsCount
            0, 1, // Scale (-1 zig-zag)
            1, 3, 64, // marshal types + precision bits
        ];
        assert_eq!(dst, expected);
    }

    // Port of TestBlockHeaderMarshalUnmarshal.
    #[test]
    fn block_header_marshal_unmarshal() {
        let mut bh = BlockHeader::default();
        for i in 0..1000u64 {
            let i = i as i64;
            bh.tsid.metric_id = (i + 1) as u64;
            bh.min_timestamp = -i * 1000 + 2;
            bh.max_timestamp = i * 2000 + 3;
            bh.timestamps_block_offset = (i * 12345 + 4) as u64;
            bh.values_block_offset = (i * 3243 + 5) as u64;
            bh.timestamps_block_size = ((i * 892 + 6) % MAX_BLOCK_SIZE as i64) as u32;
            bh.values_block_size = ((i * 894 + 7) % MAX_BLOCK_SIZE as i64) as u32;
            bh.rows_count = (i * 3 + 8) as u32;
            bh.scale = (i - 434 + 9) as i16;
            bh.timestamps_marshal_type = ((i + 10) % 7) as u8;
            bh.values_marshal_type = ((i + 11) % 7) as u8;
            bh.precision_bits = (1 + (i + 12) % 64) as u8;

            check_marshal_unmarshal(&bh);
        }
    }

    fn check_marshal_unmarshal(bh: &BlockHeader) {
        let mut dst = Vec::new();
        bh.marshal(&mut dst);
        assert_eq!(dst.len(), MARSHALED_BLOCK_HEADER_SIZE);

        let mut bh1 = BlockHeader::default();
        let tail = bh1.unmarshal(&dst).expect("cannot unmarshal bh");
        assert!(tail.is_empty(), "unexpected tail left: {tail:x?}");
        assert_eq!(*bh, bh1);

        // Marshal with a pre-existing prefix.
        let prefix = b"foo";
        let mut dst_new = prefix.to_vec();
        bh.marshal(&mut dst_new);
        assert_eq!(&dst_new[..prefix.len()], prefix);
        assert_eq!(&dst_new[prefix.len()..], &dst[..]);

        // Unmarshal with a suffix.
        let suffix = b"bar";
        dst.extend_from_slice(suffix);
        let mut bh2 = BlockHeader::default();
        let tail = bh2
            .unmarshal(&dst)
            .expect("cannot unmarshal bh from suffixed dst");
        assert_eq!(tail, suffix);
        assert_eq!(*bh, bh2);
    }

    #[test]
    fn block_header_unmarshal_truncated() {
        let bh = BlockHeader {
            rows_count: 1,
            precision_bits: 64,
            ..Default::default()
        };
        let mut dst = Vec::new();
        bh.marshal(&mut dst);
        for n in 0..MARSHALED_BLOCK_HEADER_SIZE {
            let mut bh1 = BlockHeader::default();
            assert!(
                bh1.unmarshal(&dst[..n]).is_err(),
                "expected error for {n}-byte src"
            );
        }
    }

    #[test]
    fn block_header_validate_errors() {
        let valid = BlockHeader {
            rows_count: 1,
            precision_bits: 64,
            timestamps_marshal_type: 1,
            values_marshal_type: 3,
            ..Default::default()
        };
        assert!(valid.validate().is_ok());

        let mut bh = valid;
        bh.rows_count = 0;
        assert!(bh.validate().is_err(), "zero RowsCount must fail");

        let mut bh = valid;
        bh.rows_count = (2 * MAX_ROWS_PER_BLOCK + 1) as u32;
        assert!(bh.validate().is_err(), "too big RowsCount must fail");

        let mut bh = valid;
        bh.timestamps_marshal_type = 7;
        assert!(bh.validate().is_err(), "bad marshal type must fail");

        let mut bh = valid;
        bh.precision_bits = 0;
        assert!(bh.validate().is_err(), "bad precision bits must fail");
        bh.precision_bits = 65;
        assert!(bh.validate().is_err(), "bad precision bits must fail");

        let mut bh = valid;
        bh.timestamps_block_size = (2 * MAX_BLOCK_SIZE + 1) as u32;
        assert!(bh.validate().is_err(), "too big timestamps block size");

        let mut bh = valid;
        bh.values_block_size = (2 * MAX_BLOCK_SIZE + 1) as u32;
        assert!(bh.validate().is_err(), "too big values block size");
    }

    #[test]
    fn unmarshal_block_headers_sorted() {
        let mk = |metric_id: u64, min_ts: i64| BlockHeader {
            tsid: Tsid {
                metric_id,
                ..Default::default()
            },
            min_timestamp: min_ts,
            rows_count: 1,
            precision_bits: 64,
            ..Default::default()
        };

        let mut data = Vec::new();
        mk(1, 0).marshal(&mut data);
        mk(2, 0).marshal(&mut data);
        mk(2, 10).marshal(&mut data);

        let mut dst = Vec::new();
        unmarshal_block_headers(&mut dst, &data, 3).expect("cannot unmarshal block headers");
        assert_eq!(dst.len(), 3);
        assert_eq!(dst[0].tsid.metric_id, 1);
        assert_eq!(dst[2].min_timestamp, 10);

        // Wrong count.
        let mut dst = Vec::new();
        assert!(unmarshal_block_headers(&mut dst, &data, 2).is_err());

        // Unsorted headers.
        let mut data = Vec::new();
        mk(2, 0).marshal(&mut data);
        mk(1, 0).marshal(&mut data);
        let mut dst = Vec::new();
        assert!(unmarshal_block_headers(&mut dst, &data, 2).is_err());

        // Truncated input.
        let mut data = Vec::new();
        mk(1, 0).marshal(&mut data);
        data.truncate(data.len() - 1);
        let mut dst = Vec::new();
        assert!(unmarshal_block_headers(&mut dst, &data, 1).is_err());
    }
}
