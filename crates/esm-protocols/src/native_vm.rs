//! Native VictoriaMetrics binary block-stream import (`/api/v1/import/native`).
//!
//! Wire format reference: `lib/protoparser/native/stream/streamparser.go`.
//! Block payload reference: `lib/storage/block.go::MarshalPortable`.
//!
//! Outer framing:
//!
//! ```text
//! tr[16]                       // zig-zag BE i64 min_ms || max_ms
//! while not EOF:
//!   metric_name_size [BE u32]
//!   metric_name_bytes
//!   block_size       [BE u32]
//!   block_bytes
//! ```
//!
//! `metric_name_bytes` is the VM `MetricName.Marshal` form: tag-separator
//! escapes (0x00 / 0x01 / 0x02) with a 0x01 terminator after each value.
//!
//! `block_bytes` is `BlockHeader.marshalPortable` + var-length-prefixed
//! timestamps payload + var-length-prefixed values payload. The two
//! payloads are decoded with the same `MarshalType` byte values our own
//! storage layer uses.

#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]

use esm_compress::int::{
    marshal_varint64, marshal_varuint64, unmarshal_varint64, unmarshal_varuint64,
};
use esm_compress::timeseries::{MarshalType, marshal_int64_array, unmarshal_int64_array};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedSample {
    pub metric_name: Vec<u8>,
    pub timestamp_ms: i64,
    pub value: i64,
}

/// Parse a native VM binary import stream.
///
/// # Errors
/// Returns [`NativeError`] on truncated input or any header-field
/// validation failure.
pub fn parse(body: &[u8]) -> Result<Vec<ParsedSample>, NativeError> {
    if body.len() < 16 {
        return Err(NativeError::Truncated("time range header"));
    }
    // The time range is informational only — the per-block headers carry
    // the real min/max. Discard the leading 16 bytes.
    let mut cursor = &body[16..];
    let mut out = Vec::new();
    while !cursor.is_empty() {
        if cursor.len() < 4 {
            return Err(NativeError::Truncated("metric_name size"));
        }
        let name_size = read_u32_be(&cursor[..4]) as usize;
        cursor = &cursor[4..];
        if cursor.len() < name_size {
            return Err(NativeError::Truncated("metric_name bytes"));
        }
        let name_bytes = &cursor[..name_size];
        cursor = &cursor[name_size..];

        if cursor.len() < 4 {
            return Err(NativeError::Truncated("block size"));
        }
        let block_size = read_u32_be(&cursor[..4]) as usize;
        cursor = &cursor[4..];
        if cursor.len() < block_size {
            return Err(NativeError::Truncated("block bytes"));
        }
        let block_bytes = &cursor[..block_size];
        cursor = &cursor[block_size..];

        let canonical_name = decode_metric_name(name_bytes)?;
        decode_block_into(&canonical_name, block_bytes, &mut out)?;
    }
    Ok(out)
}

/// Decode VM's `MetricName.Marshal` output to our canonical
/// `metric_name{k="v",l="w"}` byte form, with labels sorted.
fn decode_metric_name(src: &[u8]) -> Result<Vec<u8>, NativeError> {
    let (metric_group, mut rest) = consume_tag_value(src)?;
    let mut tags: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    while !rest.is_empty() {
        let (key, after_key) = consume_tag_value(rest)?;
        let (value, after_value) = consume_tag_value(after_key)?;
        rest = after_value;
        tags.push((key, value));
    }
    tags.sort();
    let mut out = Vec::new();
    out.extend_from_slice(&metric_group);
    if !tags.is_empty() {
        out.push(b'{');
        for (i, (k, v)) in tags.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.extend_from_slice(k);
            out.extend_from_slice(b"=\"");
            out.extend_from_slice(v);
            out.push(b'"');
        }
        out.push(b'}');
    }
    Ok(out)
}

/// Read one escape-encoded value terminated by 0x01.
/// Escape rules (`metric_name.go`):
///   0x00 0x30 -> 0x00
///   0x00 0x31 -> 0x01
///   0x00 0x32 -> 0x02
fn consume_tag_value(src: &[u8]) -> Result<(Vec<u8>, &[u8]), NativeError> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < src.len() {
        match src[i] {
            0x01 => return Ok((out, &src[i + 1..])),
            0x00 => {
                if i + 1 >= src.len() {
                    return Err(NativeError::Truncated("escape sequence"));
                }
                match src[i + 1] {
                    b'0' => out.push(0x00),
                    b'1' => out.push(0x01),
                    b'2' => out.push(0x02),
                    other => return Err(NativeError::BadEscape(other)),
                }
                i += 2;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    Err(NativeError::Truncated("tag value terminator"))
}

fn decode_block_into(
    metric_name: &[u8],
    src: &[u8],
    out: &mut Vec<ParsedSample>,
) -> Result<(), NativeError> {
    let mut cursor = src;
    let (min_ts, n) = unmarshal_varint64(cursor).map_err(NativeError::VarInt)?;
    cursor = &cursor[n..];
    let (_max_ts, n) = unmarshal_varint64(cursor).map_err(NativeError::VarInt)?;
    cursor = &cursor[n..];
    let (first_value, n) = unmarshal_varint64(cursor).map_err(NativeError::VarInt)?;
    cursor = &cursor[n..];
    let (rows_count, n) = unmarshal_varuint64(cursor).map_err(NativeError::VarInt)?;
    cursor = &cursor[n..];
    let (scale, n) = unmarshal_varint64(cursor).map_err(NativeError::VarInt)?;
    cursor = &cursor[n..];
    if cursor.len() < 3 {
        return Err(NativeError::Truncated("block header trailer"));
    }
    let ts_marshal_type =
        MarshalType::from_byte(cursor[0]).map_err(|e| NativeError::BadMarshalType(e.0))?;
    let v_marshal_type =
        MarshalType::from_byte(cursor[1]).map_err(|e| NativeError::BadMarshalType(e.0))?;
    // cursor[2] is precision_bits — informational only; the decoder side
    // doesn't need it because the encoded payload already reflects the
    // quantization applied by the producer.
    cursor = &cursor[3..];

    let (ts_data, after_ts) = read_marshal_bytes(cursor)?;
    let (v_data, _rest) = read_marshal_bytes(after_ts)?;

    let items = rows_count as usize;
    if items == 0 {
        return Ok(());
    }

    // Need a first_timestamp for the timestamps array; VM passes
    // bh.MinTimestamp as the first value to its decoder. Our
    // unmarshal_int64_array uses `first_value` argument as the seed of
    // the array's first element for Const / Delta / Delta2 codecs.
    let mut timestamps = Vec::with_capacity(items);
    unmarshal_int64_array(&mut timestamps, ts_data, ts_marshal_type, min_ts, items)
        .map_err(NativeError::TsArray)?;
    let mut values = Vec::with_capacity(items);
    unmarshal_int64_array(&mut values, v_data, v_marshal_type, first_value, items)
        .map_err(NativeError::TsArray)?;

    if timestamps.len() != values.len() {
        return Err(NativeError::LengthMismatch { ts: timestamps.len(), v: values.len() });
    }
    for (ts, v) in timestamps.into_iter().zip(values) {
        out.push(ParsedSample {
            metric_name: metric_name.to_vec(),
            timestamp_ms: ts,
            // VM applies `value * 10^scale` to recover the original
            // sample magnitude. `scale` is a signed exponent.
            value: apply_decimal_scale(v, scale),
        });
    }
    Ok(())
}

fn read_marshal_bytes(src: &[u8]) -> Result<(&[u8], &[u8]), NativeError> {
    let (n, used) = unmarshal_varuint64(src).map_err(NativeError::VarInt)?;
    let n = n as usize;
    let rest = &src[used..];
    if rest.len() < n {
        return Err(NativeError::Truncated("payload bytes"));
    }
    Ok((&rest[..n], &rest[n..]))
}

/// Apply VM's `value * 10^scale` decimal-scale recovery. `scale` is a
/// signed exponent — positive means multiply by 10, negative means
/// divide. Overflow saturates to `i64` bounds; underflow truncates
/// toward zero (matching VM's behaviour).
fn apply_decimal_scale(value: i64, scale: i64) -> i64 {
    if scale == 0 {
        return value;
    }
    if scale > 0 {
        let mut v = value;
        for _ in 0..scale {
            v = v.saturating_mul(10);
        }
        v
    } else {
        let mut v = value;
        for _ in 0..(-scale) {
            v /= 10;
        }
        v
    }
}

/// One series with sorted (label, value) pairs and a list of samples.
/// Used by the encoder to construct VM-compatible payloads.
#[derive(Debug, Clone)]
pub struct Series<'a> {
    pub metric_name: &'a str,
    /// Sorted by `(key, value)` for canonical output.
    pub labels: Vec<(&'a str, &'a str)>,
    /// Sorted by timestamp ascending.
    pub samples: Vec<(i64, i64)>,
}

/// Encode `series` as a VM `/api/v1/import/native`-compatible payload.
/// Each series produces exactly one block; multi-block partitioning per
/// MAX_ROWS_PER_BLOCK is not yet implemented.
#[must_use]
pub fn encode(series: &[Series<'_>]) -> Vec<u8> {
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    for s in series {
        for (t, _) in &s.samples {
            min_ts = min_ts.min(*t);
            max_ts = max_ts.max(*t);
        }
    }
    let mut out = Vec::new();
    out.extend_from_slice(&zigzag_i64_be(min_ts));
    out.extend_from_slice(&zigzag_i64_be(max_ts));
    for s in series {
        let name_bytes = encode_metric_name(s.metric_name, &s.labels);
        let block_bytes = encode_portable_block(&s.samples);
        out.extend_from_slice(&u32::try_from(name_bytes.len()).unwrap_or(u32::MAX).to_be_bytes());
        out.extend_from_slice(&name_bytes);
        out.extend_from_slice(&u32::try_from(block_bytes.len()).unwrap_or(u32::MAX).to_be_bytes());
        out.extend_from_slice(&block_bytes);
    }
    out
}

fn zigzag_i64_be(v: i64) -> [u8; 8] {
    let z = ((v << 1) ^ (v >> 63)) as u64;
    z.to_be_bytes()
}

fn encode_metric_name(name: &str, labels: &[(&str, &str)]) -> Vec<u8> {
    let mut out = Vec::new();
    push_tag_value(&mut out, name.as_bytes());
    for (k, v) in labels {
        push_tag_value(&mut out, k.as_bytes());
        push_tag_value(&mut out, v.as_bytes());
    }
    out
}

fn push_tag_value(dst: &mut Vec<u8>, src: &[u8]) {
    for &b in src {
        match b {
            0x00 => dst.extend_from_slice(b"\x000"),
            0x01 => dst.extend_from_slice(b"\x001"),
            0x02 => dst.extend_from_slice(b"\x002"),
            _ => dst.push(b),
        }
    }
    dst.push(0x01);
}

fn encode_portable_block(samples: &[(i64, i64)]) -> Vec<u8> {
    let timestamps: Vec<i64> = samples.iter().map(|(t, _)| *t).collect();
    let values: Vec<i64> = samples.iter().map(|(_, v)| *v).collect();
    let mut ts_data = Vec::new();
    // marshal_int64_array only fails on empty input; the caller must
    // pass at least one sample. Guard at the start so the unwraps are
    // statically justified.
    if timestamps.is_empty() {
        return Vec::new();
    }
    let Ok(ts_result) = marshal_int64_array(&mut ts_data, &timestamps, 64) else {
        return Vec::new();
    };
    let mut v_data = Vec::new();
    let Ok(v_result) = marshal_int64_array(&mut v_data, &values, 64) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    marshal_varint64(&mut out, *timestamps.first().unwrap_or(&0));
    marshal_varint64(&mut out, *timestamps.last().unwrap_or(&0));
    marshal_varint64(&mut out, v_result.first_value);
    marshal_varuint64(&mut out, timestamps.len() as u64);
    marshal_varint64(&mut out, 0_i64); // scale=0: lossless integer encoding
    out.push(ts_result.marshal_type.as_byte());
    out.push(v_result.marshal_type.as_byte());
    out.push(64_u8); // precision_bits
    marshal_varuint64(&mut out, ts_data.len() as u64);
    out.extend_from_slice(&ts_data);
    marshal_varuint64(&mut out, v_data.len() as u64);
    out.extend_from_slice(&v_data);
    out
}

fn read_u32_be(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

#[derive(Debug, Error)]
pub enum NativeError {
    #[error("truncated input at {0}")]
    Truncated(&'static str),
    #[error("bad escape byte after 0x00: {0:#04x}")]
    BadEscape(u8),
    #[error("bad MarshalType byte: {0}")]
    BadMarshalType(u8),
    #[error("varint decode: {0}")]
    VarInt(esm_compress::int::DecodeError),
    #[error("ts/v array decode: {0}")]
    TsArray(esm_compress::timeseries::TsError),
    #[error("timestamps ({ts}) and values ({v}) length disagree")]
    LengthMismatch { ts: usize, v: usize },
}

#[cfg(test)]
mod tests {
    use super::*;
    use esm_compress::int::{marshal_varint64, marshal_varuint64};
    use esm_compress::timeseries::marshal_int64_array;

    fn marshal_i64_be_zigzag(v: i64) -> [u8; 8] {
        let z = ((v << 1) ^ (v >> 63)) as u64;
        z.to_be_bytes()
    }

    fn marshal_tag_value(dst: &mut Vec<u8>, src: &[u8]) {
        for &b in src {
            match b {
                0x00 => dst.extend_from_slice(b"\x000"),
                0x01 => dst.extend_from_slice(b"\x001"),
                0x02 => dst.extend_from_slice(b"\x002"),
                _ => dst.push(b),
            }
        }
        dst.push(0x01);
    }

    fn marshal_metric_name(name: &str, tags: &[(&str, &str)]) -> Vec<u8> {
        let mut out = Vec::new();
        marshal_tag_value(&mut out, name.as_bytes());
        for (k, v) in tags {
            marshal_tag_value(&mut out, k.as_bytes());
            marshal_tag_value(&mut out, v.as_bytes());
        }
        out
    }

    fn marshal_portable_block(timestamps: &[i64], values: &[i64]) -> Vec<u8> {
        let mut ts_data = Vec::new();
        let ts_result = marshal_int64_array(&mut ts_data, timestamps, 64).unwrap();
        let mut v_data = Vec::new();
        let v_result = marshal_int64_array(&mut v_data, values, 64).unwrap();
        let mut out = Vec::new();
        marshal_varint64(&mut out, *timestamps.first().unwrap_or(&0));
        marshal_varint64(&mut out, *timestamps.last().unwrap_or(&0));
        marshal_varint64(&mut out, v_result.first_value);
        marshal_varuint64(&mut out, timestamps.len() as u64);
        marshal_varint64(&mut out, 0_i64); // scale
        out.push(ts_result.marshal_type.as_byte());
        out.push(v_result.marshal_type.as_byte());
        out.push(64_u8);
        marshal_varuint64(&mut out, ts_data.len() as u64);
        out.extend_from_slice(&ts_data);
        marshal_varuint64(&mut out, v_data.len() as u64);
        out.extend_from_slice(&v_data);
        out
    }

    #[test]
    fn roundtrip_single_block() {
        let timestamps = vec![1_700_000_000_000_i64, 1_700_000_001_000, 1_700_000_002_000];
        let values = vec![10_i64, 20, 30];
        let name_bytes = marshal_metric_name("cpu", &[("host", "a"), ("region", "us")]);
        let block_bytes = marshal_portable_block(&timestamps, &values);

        let mut body = Vec::new();
        body.extend_from_slice(&marshal_i64_be_zigzag(timestamps[0]));
        body.extend_from_slice(&marshal_i64_be_zigzag(*timestamps.last().unwrap()));
        body.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
        body.extend_from_slice(&name_bytes);
        body.extend_from_slice(&(block_bytes.len() as u32).to_be_bytes());
        body.extend_from_slice(&block_bytes);

        let out = parse(&body).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].metric_name, br#"cpu{host="a",region="us"}"#);
        assert_eq!(out[0].timestamp_ms, 1_700_000_000_000);
        assert_eq!(out[0].value, 10);
        assert_eq!(out[2].timestamp_ms, 1_700_000_002_000);
        assert_eq!(out[2].value, 30);
    }
}
