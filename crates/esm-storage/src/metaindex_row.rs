//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/metaindex_row.go.

use crate::block::MAX_BLOCK_SIZE;
use crate::block_header::BlockHeader;
use crate::tsid::Tsid;
use esm_encoding as encoding;

/// The size of a marshaled [`MetaindexRow`]:
/// `TSID(24) | BlockHeadersCount u32 | MinTimestamp i64 | MaxTimestamp i64 |
/// IndexBlockOffset u64 | IndexBlockSize u32` = 56 bytes.
///
/// Note: the marshal order differs from the struct field order — port of the
/// Go marshaling code, which writes BlockHeadersCount right after the TSID.
pub const MARSHALED_METAINDEX_ROW_SIZE: usize = 56;

/// MetaindexRow is a single metaindex row pointing to a single index block
/// containing block headers. Go: metaindexRow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaindexRow {
    /// The first TSID in the corresponding index block.
    pub tsid: Tsid,

    /// The minimum timestamp in the given index block.
    pub min_timestamp: i64,

    /// The maximum timestamp in the given index block.
    pub max_timestamp: i64,

    /// The offset of the index block.
    pub index_block_offset: u64,

    /// The number of block headers in the given index block.
    pub block_headers_count: u32,

    /// The size of the compressed index block.
    pub index_block_size: u32,
}

impl Default for MetaindexRow {
    fn default() -> MetaindexRow {
        MetaindexRow {
            tsid: Tsid::default(),
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
            index_block_offset: 0,
            block_headers_count: 0,
            index_block_size: 0,
        }
    }
}

impl MetaindexRow {
    /// Resets the row. Go: metaindexRow.Reset.
    pub fn reset(&mut self) {
        *self = MetaindexRow::default();
    }

    /// Registers the given block header in the row.
    /// Go: metaindexRow.RegisterBlockHeader.
    pub fn register_block_header(&mut self, bh: &BlockHeader) {
        self.block_headers_count += 1;
        if self.block_headers_count == 1 {
            self.tsid = bh.tsid;
            self.min_timestamp = bh.min_timestamp;
            self.max_timestamp = bh.max_timestamp;
            return;
        }

        if bh.min_timestamp < self.min_timestamp {
            self.min_timestamp = bh.min_timestamp;
        }
        if bh.max_timestamp > self.max_timestamp {
            self.max_timestamp = bh.max_timestamp;
        }
    }

    /// Appends marshaled `self` to `dst`. Go: metaindexRow.Marshal.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        self.tsid.marshal(dst);
        encoding::marshal_uint32(dst, self.block_headers_count);
        encoding::marshal_int64(dst, self.min_timestamp);
        encoding::marshal_int64(dst, self.max_timestamp);
        encoding::marshal_uint64(dst, self.index_block_offset);
        encoding::marshal_uint32(dst, self.index_block_size);
    }

    /// Unmarshals `self` from `src` and returns the tail of `src`.
    /// Go: metaindexRow.Unmarshal.
    pub fn unmarshal<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        if src.len() < MARSHALED_METAINDEX_ROW_SIZE {
            return Err(format!(
                "cannot unmarshal metaindexRow from {} bytes; want at least {} bytes",
                src.len(),
                MARSHALED_METAINDEX_ROW_SIZE
            ));
        }

        let src = self
            .tsid
            .unmarshal(src)
            .map_err(|err| format!("cannot unmarshal TSID: {err}"))?;

        self.block_headers_count = encoding::unmarshal_uint32(src);
        let src = &src[4..];
        self.min_timestamp = encoding::unmarshal_int64(src);
        let src = &src[8..];
        self.max_timestamp = encoding::unmarshal_int64(src);
        let src = &src[8..];
        self.index_block_offset = encoding::unmarshal_uint64(src);
        let src = &src[8..];
        self.index_block_size = encoding::unmarshal_uint32(src);
        let src = &src[4..];

        // Validate unmarshaled data.
        if self.block_headers_count == 0 {
            return Err("BlockHeadersCount must be greater than 0".to_string());
        }
        if self.index_block_size as usize > 2 * MAX_BLOCK_SIZE {
            return Err(format!(
                "too big IndexBlockSize; got {}; cannot exceed {}",
                self.index_block_size,
                2 * MAX_BLOCK_SIZE
            ));
        }

        Ok(src)
    }
}

/// Unmarshals all the metaindex rows from the ZSTD-compressed
/// `compressed_data` (the full contents of `metaindex.bin`). The rows must be
/// sorted by TSID. Go: unmarshalMetaindexRows.
pub fn unmarshal_metaindex_rows(compressed_data: &[u8]) -> Result<Vec<MetaindexRow>, String> {
    let mut data = Vec::new();
    encoding::decompress_zstd(&mut data, compressed_data)
        .map_err(|err| format!("cannot decompress metaindex rows: {err}"))?;

    let mut dst = Vec::new();
    let mut src = &data[..];
    while !src.is_empty() {
        let mut mr = MetaindexRow::default();
        src = mr.unmarshal(src).map_err(|err| {
            format!(
                "cannot unmarshal metaindexRow #{} from metaindex data: {err}",
                dst.len()
            )
        })?;
        dst.push(mr);
    }
    if dst.is_empty() {
        return Err("expecting non-zero metaindex rows; got zero".to_string());
    }

    // Make sure metaindex rows are sorted by tsid.
    if !dst.is_sorted_by(|a, b| a.tsid <= b.tsid) {
        return Err(format!(
            "metaindexRow values must be sorted by TSID; got {dst:?}"
        ));
    }

    Ok(dst)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::splitmix64;

    // Port of TestMetaindexRowReset.
    #[test]
    fn metaindex_row_reset() {
        let mut mr = MetaindexRow {
            tsid: Tsid {
                metric_id: 234,
                ..Default::default()
            },
            block_headers_count: 1323,
            min_timestamp: -234,
            max_timestamp: 8989,
            index_block_offset: 89439,
            index_block_size: 89984,
        };
        let mr_empty = MetaindexRow::default();
        assert_ne!(mr, mr_empty);
        mr.reset();
        assert_eq!(mr, mr_empty);
        assert_eq!(mr_empty.min_timestamp, i64::MAX);
        assert_eq!(mr_empty.max_timestamp, i64::MIN);
    }

    // Golden byte layout: TSID | BlockHeadersCount u32 | MinTimestamp i64 |
    // MaxTimestamp i64 | IndexBlockOffset u64 | IndexBlockSize u32.
    #[test]
    fn marshal_golden_bytes() {
        let mr = MetaindexRow {
            tsid: Tsid {
                metric_id: 1,
                ..Default::default()
            },
            block_headers_count: 2,
            min_timestamp: -1, // zig-zag -> 1
            max_timestamp: 1,  // zig-zag -> 2
            index_block_offset: 0x10,
            index_block_size: 4,
        };
        let mut dst = Vec::new();
        mr.marshal(&mut dst);
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0, 0, 0, 0, 0, 0, 0, 0, // MetricGroupID
            0, 0, 0, 0, // JobID
            0, 0, 0, 0, // InstanceID
            0, 0, 0, 0, 0, 0, 0, 1, // MetricID
            0, 0, 0, 2, // BlockHeadersCount
            0, 0, 0, 0, 0, 0, 0, 1, // MinTimestamp (-1 zig-zag)
            0, 0, 0, 0, 0, 0, 0, 2, // MaxTimestamp (1 zig-zag)
            0, 0, 0, 0, 0, 0, 0, 0x10, // IndexBlockOffset
            0, 0, 0, 4, // IndexBlockSize
        ];
        assert_eq!(dst, expected);
        assert_eq!(dst.len(), MARSHALED_METAINDEX_ROW_SIZE);
    }

    fn rand_metaindex_row(state: &mut u64) -> MetaindexRow {
        let mut mr = MetaindexRow {
            tsid: Tsid {
                metric_group_id: splitmix64(state),
                job_id: splitmix64(state) as u32,
                instance_id: splitmix64(state) as u32,
                metric_id: splitmix64(state),
            },
            min_timestamp: splitmix64(state) as i64,
            max_timestamp: splitmix64(state) as i64,
            index_block_offset: splitmix64(state),
            block_headers_count: splitmix64(state) as u32,
            index_block_size: splitmix64(state) as u32,
        };
        if mr.block_headers_count == 0 {
            mr.block_headers_count = 1;
        }
        if mr.index_block_size as usize > 2 * MAX_BLOCK_SIZE {
            mr.index_block_size = (2 * MAX_BLOCK_SIZE) as u32;
        }
        mr
    }

    // Port of TestMetaindexRowMarshalUnmarshal.
    #[test]
    fn metaindex_row_marshal_unmarshal() {
        let mut state = 1u64;
        for _ in 0..1000 {
            let mr = rand_metaindex_row(&mut state);
            check_marshal_unmarshal(&mr);
        }
    }

    fn check_marshal_unmarshal(mr: &MetaindexRow) {
        let mut dst = Vec::new();
        mr.marshal(&mut dst);
        assert_eq!(dst.len(), MARSHALED_METAINDEX_ROW_SIZE);

        let mut mr1 = MetaindexRow::default();
        let tail = mr1.unmarshal(&dst).expect("cannot unmarshal");
        assert!(tail.is_empty(), "unexpected non-zero tail: {tail:x?}");
        assert_eq!(*mr, mr1);

        // Marshal with a pre-existing prefix.
        let prefix = b"foo";
        let mut dst_new = prefix.to_vec();
        mr.marshal(&mut dst_new);
        assert_eq!(&dst_new[..prefix.len()], prefix);
        assert_eq!(&dst_new[prefix.len()..], &dst[..]);

        // Unmarshal with a suffix.
        let suffix = b"bar";
        dst.extend_from_slice(suffix);
        let mut mr2 = MetaindexRow::default();
        let tail = mr2.unmarshal(&dst).expect("cannot unmarshal suffixed");
        assert_eq!(tail, suffix);
        assert_eq!(*mr, mr2);
    }

    #[test]
    fn unmarshal_rejects_invalid_rows() {
        // Truncated input.
        let mut mr = MetaindexRow::default();
        assert!(mr.unmarshal(&[0u8; 55]).is_err());

        // Zero BlockHeadersCount.
        let mut dst = Vec::new();
        MetaindexRow {
            block_headers_count: 0,
            ..Default::default()
        }
        .marshal(&mut dst);
        assert!(mr.unmarshal(&dst).is_err());

        // Too big IndexBlockSize.
        let mut dst = Vec::new();
        MetaindexRow {
            block_headers_count: 1,
            index_block_size: (2 * MAX_BLOCK_SIZE + 1) as u32,
            ..Default::default()
        }
        .marshal(&mut dst);
        assert!(mr.unmarshal(&dst).is_err());
    }

    #[test]
    fn register_block_header_aggregates() {
        let mut mr = MetaindexRow::default();
        let mut bh = BlockHeader {
            tsid: Tsid {
                metric_id: 7,
                ..Default::default()
            },
            min_timestamp: 100,
            max_timestamp: 200,
            ..Default::default()
        };
        mr.register_block_header(&bh);
        assert_eq!(mr.block_headers_count, 1);
        assert_eq!(mr.tsid.metric_id, 7);
        assert_eq!(mr.min_timestamp, 100);
        assert_eq!(mr.max_timestamp, 200);

        bh.tsid.metric_id = 8;
        bh.min_timestamp = 50;
        bh.max_timestamp = 300;
        mr.register_block_header(&bh);
        assert_eq!(mr.block_headers_count, 2);
        // TSID stays at the first registered block header.
        assert_eq!(mr.tsid.metric_id, 7);
        assert_eq!(mr.min_timestamp, 50);
        assert_eq!(mr.max_timestamp, 300);
    }

    #[test]
    fn unmarshal_metaindex_rows_roundtrip() {
        let mut data = Vec::new();
        for metric_id in [1u64, 2, 5] {
            MetaindexRow {
                tsid: Tsid {
                    metric_id,
                    ..Default::default()
                },
                min_timestamp: 0,
                max_timestamp: 10,
                index_block_offset: metric_id * 100,
                block_headers_count: 3,
                index_block_size: 42,
            }
            .marshal(&mut data);
        }
        let mut compressed = Vec::new();
        encoding::compress_zstd_level(&mut compressed, &data, 1);

        let mrs = unmarshal_metaindex_rows(&compressed).expect("cannot unmarshal rows");
        assert_eq!(mrs.len(), 3);
        assert_eq!(mrs[0].tsid.metric_id, 1);
        assert_eq!(mrs[2].index_block_offset, 500);

        // Zero rows must be rejected.
        let mut empty = Vec::new();
        encoding::compress_zstd_level(&mut empty, &[], 1);
        assert!(unmarshal_metaindex_rows(&empty).is_err());

        // Unsorted rows must be rejected.
        let mut data = Vec::new();
        for metric_id in [5u64, 1] {
            MetaindexRow {
                tsid: Tsid {
                    metric_id,
                    ..Default::default()
                },
                block_headers_count: 1,
                ..Default::default()
            }
            .marshal(&mut data);
        }
        let mut compressed = Vec::new();
        encoding::compress_zstd_level(&mut compressed, &data, 1);
        assert!(unmarshal_metaindex_rows(&compressed).is_err());
    }
}
