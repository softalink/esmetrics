//! `Gauge`/`Sum`/`Histogram`/`ExponentialHistogram`/`Summary` and their data
//! point message types, split out of `pb.rs` to stay under the 800-line
//! guideline (see the note where this module is declared).
//!
//! Same porting rules as `pb.rs`: decode-only, field numbers transcribed
//! from `pb.go`'s `case` arms, unknown fields skipped by wire type.

use super::{decode_key_value_list, KeyValue};
use crate::wire::{WireError, WireReader};

/// Go: `Gauge`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Gauge {
    pub data_points: Vec<NumberDataPoint>,
}

impl Gauge {
    /// Field map: `{1: repeated NumberDataPoint}`.
    ///
    /// Go: `decoderContext.decodeGauge`.
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut g = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    g.data_points.push(NumberDataPoint::unmarshal(data)?);
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(g)
    }
}

/// Go: `Sum`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Sum {
    pub data_points: Vec<NumberDataPoint>,
    pub is_monotonic: bool,
}

impl Sum {
    /// Field map: `{1: repeated NumberDataPoint, 3: bool is_monotonic}`.
    ///
    /// Go: `decoderContext.decodeSum`.
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut s = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    s.data_points.push(NumberDataPoint::unmarshal(data)?);
                }
                3 => {
                    if wire_type != 0 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    s.is_monotonic = r.read_bool()?;
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(s)
    }
}

/// Go: `NumberDataPoint`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct NumberDataPoint {
    pub attributes: Vec<KeyValue>,
    pub time_unix_nano: u64,
    /// `oneof value`, case `as_double` (field 4).
    pub double_value: Option<f64>,
    /// `oneof value`, case `as_int` (field 6, `sfixed64`).
    pub int_value: Option<i64>,
    pub flags: u32,
}

impl NumberDataPoint {
    /// Field map: `{7: repeated KeyValue attributes, 3: fixed64
    /// time_unix_nano, oneof value { 4: double as_double, 6: sfixed64
    /// as_int }, 8: uint32 flags}`.
    ///
    /// Go: `decoderContext.decodeNumberDataPoint`.
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut dp = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                7 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    decode_key_value_list(data, &mut dp.attributes)?;
                }
                3 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.time_unix_nano = r.read_fixed64()?;
                }
                4 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.double_value = Some(r.read_double()?);
                }
                6 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.int_value = Some(r.read_sfixed64()?);
                }
                8 => {
                    if wire_type != 0 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.flags = r.read_uint32()?;
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(dp)
    }
}

/// Go: `Histogram`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Histogram {
    pub data_points: Vec<HistogramDataPoint>,
}

impl Histogram {
    /// Field map: `{1: repeated HistogramDataPoint}`.
    ///
    /// Go: `decoderContext.decodeHistogram`.
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut h = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    h.data_points.push(HistogramDataPoint::unmarshal(data)?);
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(h)
    }
}

/// Go: `HistogramDataPoint`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct HistogramDataPoint {
    pub attributes: Vec<KeyValue>,
    pub time_unix_nano: u64,
    pub count: u64,
    pub sum: Option<f64>,
    /// Accepts both the packed (single length-delimited field 6 occurrence)
    /// and legacy unpacked (multiple wire-type-1 field 6 occurrences)
    /// encodings — see [`WireReader::read_packed_fixed64s`].
    pub bucket_counts: Vec<u64>,
    /// Same dual packed/unpacked acceptance as `bucket_counts`, for field 7
    /// — see [`WireReader::read_packed_doubles`].
    pub explicit_bounds: Vec<f64>,
    pub flags: u32,
}

impl HistogramDataPoint {
    /// Field map: `{9: repeated KeyValue attributes, 3: fixed64
    /// time_unix_nano, 4: fixed64 count, 5: optional double sum, 6: repeated
    /// fixed64 bucket_counts, 7: repeated double explicit_bounds, 10: uint32
    /// flags}`.
    ///
    /// Note the attributes field number (9) differs from
    /// `ExponentialHistogramDataPoint`'s (1) — both are transcribed
    /// directly from their respective `pb.go` doc comments, not assumed
    /// consistent across message types.
    ///
    /// Go: `decoderContext.decodeHistogramDataPoint` (the
    /// `histogramDataPointContext.pushSamples` bucket/cumulative-count
    /// expansion into `_count`/`_sum`/`_bucket` series is downstream
    /// conversion, out of scope for decode).
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut dp = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                9 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    decode_key_value_list(data, &mut dp.attributes)?;
                }
                3 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.time_unix_nano = r.read_fixed64()?;
                }
                4 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.count = r.read_fixed64()?;
                }
                5 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.sum = Some(r.read_double()?);
                }
                6 => {
                    r.read_packed_fixed64s(wire_type, &mut dp.bucket_counts)?;
                }
                7 => {
                    r.read_packed_doubles(wire_type, &mut dp.explicit_bounds)?;
                }
                10 => {
                    if wire_type != 0 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.flags = r.read_uint32()?;
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(dp)
    }
}

/// Go: `ExponentialHistogram`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ExponentialHistogram {
    pub data_points: Vec<ExponentialHistogramDataPoint>,
}

impl ExponentialHistogram {
    /// Field map: `{1: repeated ExponentialHistogramDataPoint}`.
    ///
    /// Go: `decoderContext.decodeExponentialHistogram`.
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut eh = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    eh.data_points
                        .push(ExponentialHistogramDataPoint::unmarshal(data)?);
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(eh)
    }
}

/// Go: `ExponentialHistogramDataPoint`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ExponentialHistogramDataPoint {
    pub attributes: Vec<KeyValue>,
    pub time_unix_nano: u64,
    pub count: u64,
    pub sum: Option<f64>,
    pub scale: i32,
    pub zero_count: u64,
    pub positive: Option<Buckets>,
    pub negative: Option<Buckets>,
    pub flags: u32,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub zero_threshold: f64,
}

impl ExponentialHistogramDataPoint {
    /// Field map: `{1: repeated KeyValue attributes, 3: fixed64
    /// time_unix_nano, 4: fixed64 count, 5: optional double sum,
    /// 6: sint32 scale, 7: fixed64 zero_count, 8: Buckets positive,
    /// 9: Buckets negative, 10: uint32 flags, 12: optional double min,
    /// 13: optional double max, 14: double zero_threshold}`.
    ///
    /// Go: `decoderContext.decodeExponentialHistogramDataPoint` (the
    /// `exponentialHistogramDataPointContext.pushSamples` vmrange-bucket
    /// expansion is downstream conversion, out of scope for decode).
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut dp = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    decode_key_value_list(data, &mut dp.attributes)?;
                }
                3 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.time_unix_nano = r.read_fixed64()?;
                }
                4 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.count = r.read_fixed64()?;
                }
                5 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.sum = Some(r.read_double()?);
                }
                6 => {
                    if wire_type != 0 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.scale = r.read_sint32()?;
                }
                7 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.zero_count = r.read_fixed64()?;
                }
                8 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    dp.positive = Some(Buckets::unmarshal(data)?);
                }
                9 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    dp.negative = Some(Buckets::unmarshal(data)?);
                }
                10 => {
                    if wire_type != 0 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.flags = r.read_uint32()?;
                }
                12 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.min = Some(r.read_double()?);
                }
                13 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.max = Some(r.read_double()?);
                }
                14 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.zero_threshold = r.read_double()?;
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(dp)
    }
}

/// Go: `Buckets` (the `ExponentialHistogramDataPoint.positive`/`.negative`
/// message, not to be confused with `HistogramDataPoint`'s
/// `bucket_counts`).
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Buckets {
    pub offset: i32,
    /// Accepts both the packed (length-delimited varint run) and legacy
    /// unpacked (multiple wire-type-0 occurrences) encodings of `repeated
    /// uint64 bucket_counts` — see [`WireReader::read_packed_uint64s`].
    pub bucket_counts: Vec<u64>,
}

impl Buckets {
    /// Field map: `{1: sint32 offset, 2: repeated uint64 bucket_counts}`.
    ///
    /// Go: `buckets.decodeBuckets`.
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut b = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 0 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    b.offset = r.read_sint32()?;
                }
                2 => {
                    r.read_packed_uint64s(wire_type, &mut b.bucket_counts)?;
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(b)
    }
}

/// Go: `Summary`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Summary {
    pub data_points: Vec<SummaryDataPoint>,
}

impl Summary {
    /// Field map: `{1: repeated SummaryDataPoint}`.
    ///
    /// Go: `decoderContext.decodeSummary`.
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut s = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    s.data_points.push(SummaryDataPoint::unmarshal(data)?);
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(s)
    }
}

/// Go: `SummaryDataPoint`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SummaryDataPoint {
    pub attributes: Vec<KeyValue>,
    pub time_unix_nano: u64,
    pub count: u64,
    pub sum: f64,
    pub quantile_values: Vec<ValueAtQuantile>,
    pub flags: u32,
}

impl SummaryDataPoint {
    /// Field map: `{7: repeated KeyValue attributes, 3: fixed64
    /// time_unix_nano, 4: fixed64 count, 5: double sum, 6: repeated
    /// ValueAtQuantile quantile_values, 8: uint32 flags}`.
    ///
    /// Go: `decoderContext.decodeSummaryDataPoint` (the
    /// `summaryDataPointContext.pushSamples` `quantile`-labeled sample
    /// emission is downstream conversion, out of scope for decode).
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut dp = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                7 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    decode_key_value_list(data, &mut dp.attributes)?;
                }
                3 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.time_unix_nano = r.read_fixed64()?;
                }
                4 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.count = r.read_fixed64()?;
                }
                5 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.sum = r.read_double()?;
                }
                6 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    dp.quantile_values.push(ValueAtQuantile::unmarshal(data)?);
                }
                8 => {
                    if wire_type != 0 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    dp.flags = r.read_uint32()?;
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(dp)
    }
}

/// Go: `ValueAtQuantile`.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct ValueAtQuantile {
    pub quantile: f64,
    pub value: f64,
}

impl ValueAtQuantile {
    /// Field map: `{1: double quantile, 2: double value}`.
    ///
    /// Go: `decodeValueAtQuantile`.
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut v = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    v.quantile = r.read_double()?;
                }
                2 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    v.value = r.read_double()?;
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(v)
    }
}
