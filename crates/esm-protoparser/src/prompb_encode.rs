//! Encode-only counterpart to [`crate::prompb`].
//!
//! Serializes [`TimeSeries`](crate::prompb::TimeSeries) into a
//! Protobuf-encoded `WriteRequest` message and snappy-block-compresses it,
//! matching the wire format the decode side in `crate::prompb` reads and the
//! upstream `github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb` shape:
//! `WriteRequest{1: repeated TimeSeries}`, `TimeSeries{1: repeated Label,
//! 2: repeated Sample}`, `Label{1: name bytes, 2: value bytes}`,
//! `Sample{1: double value, 2: varint(int64) timestamp}`.

use crate::prompb::{Sample, TimeSeries};

fn append_varint(dst: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            dst.push(byte);
            break;
        }
        dst.push(byte | 0x80);
    }
}

fn append_tag(dst: &mut Vec<u8>, field_num: u32, wire_type: u8) {
    append_varint(dst, (u64::from(field_num) << 3) | u64::from(wire_type));
}

fn append_bytes_field(dst: &mut Vec<u8>, field_num: u32, data: &[u8]) {
    append_tag(dst, field_num, 2);
    append_varint(dst, data.len() as u64);
    dst.extend_from_slice(data);
}

fn append_double_field(dst: &mut Vec<u8>, field_num: u32, v: f64) {
    append_tag(dst, field_num, 1);
    dst.extend_from_slice(&v.to_le_bytes());
}

fn append_varint_field(dst: &mut Vec<u8>, field_num: u32, v: i64) {
    append_tag(dst, field_num, 0);
    append_varint(dst, v as u64);
}

fn encode_label(dst: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    append_bytes_field(dst, 1, name);
    append_bytes_field(dst, 2, value);
}

fn encode_sample(dst: &mut Vec<u8>, sample: &Sample) {
    append_double_field(dst, 1, sample.value);
    append_varint_field(dst, 2, sample.timestamp);
}

fn encode_time_series(ts: &TimeSeries) -> Vec<u8> {
    let mut dst = Vec::new();
    for label in &ts.labels {
        let mut label_buf = Vec::new();
        encode_label(&mut label_buf, label.name, label.value);
        append_bytes_field(&mut dst, 1, &label_buf);
    }
    for sample in &ts.samples {
        let mut sample_buf = Vec::new();
        encode_sample(&mut sample_buf, sample);
        append_bytes_field(&mut dst, 2, &sample_buf);
    }
    dst
}

/// Protobuf-encodes `WriteRequest{ timeseries: series }` (field 1, repeated
/// `TimeSeries`), matching the message [`crate::prompb::unmarshal_write_request`]
/// decodes.
pub fn encode_write_request(series: &[TimeSeries]) -> Vec<u8> {
    let mut dst = Vec::new();
    for ts in series {
        let ts_buf = encode_time_series(ts);
        append_bytes_field(&mut dst, 1, &ts_buf);
    }
    dst
}

/// Snappy block-compresses `buf` (not the streaming/framed format), matching
/// what Prometheus remote-write clients POST as the request body.
///
/// Returns [`snap::Error::TooBig`] when `buf` exceeds snappy's block-format
/// limit (`u32::MAX` bytes, ~4.3 GiB).
pub fn compress_snappy(buf: &[u8]) -> Result<Vec<u8>, snap::Error> {
    snap::raw::Encoder::new().compress_vec(buf)
}

/// Encodes `series` as a `WriteRequest` and snappy-compresses the result —
/// the exact bytes a remote-write client POSTs.
///
/// Propagates [`snap::Error::TooBig`] from [`compress_snappy`] when the encoded
/// message exceeds snappy's block-format limit.
pub fn encode_and_compress(series: &[TimeSeries]) -> Result<Vec<u8>, snap::Error> {
    compress_snappy(&encode_write_request(series))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prompb::{unmarshal_write_request, Label};

    #[test]
    fn write_request_roundtrips_through_decoder() {
        let ts = vec![TimeSeries {
            labels: vec![
                Label {
                    name: b"__name__",
                    value: b"ALERTS",
                },
                Label {
                    name: b"alertname",
                    value: b"HighLoad",
                },
            ],
            samples: vec![Sample {
                value: 1.0,
                timestamp: 1_700_000_000_000,
            }],
        }];
        let raw = encode_write_request(&ts);
        let decoded = unmarshal_write_request(&raw).expect("decode");
        assert_eq!(decoded.timeseries.len(), 1);
        assert_eq!(decoded.timeseries[0].labels[0].name, b"__name__");
        assert_eq!(decoded.timeseries[0].labels[0].value, b"ALERTS");
        assert_eq!(decoded.timeseries[0].labels[1].name, b"alertname");
        assert_eq!(decoded.timeseries[0].labels[1].value, b"HighLoad");
        assert_eq!(decoded.timeseries[0].samples[0].value, 1.0);
        assert_eq!(
            decoded.timeseries[0].samples[0].timestamp,
            1_700_000_000_000
        );
    }

    #[test]
    fn snappy_block_roundtrips() {
        let data = b"hello prometheus remote write";
        let c = compress_snappy(data).unwrap();
        let d = snap::raw::Decoder::new().decompress_vec(&c).unwrap();
        assert_eq!(d, data);
    }

    #[test]
    fn encode_and_compress_produces_snappy_that_decompresses_to_the_encoded_message() {
        let ts = vec![TimeSeries {
            labels: vec![Label {
                name: b"__name__",
                value: b"ALERTS",
            }],
            samples: vec![Sample {
                value: 2.0,
                timestamp: 42,
            }],
        }];

        let expected_raw = encode_write_request(&ts);
        let compressed = encode_and_compress(&ts).unwrap();
        let decompressed = snap::raw::Decoder::new()
            .decompress_vec(&compressed)
            .unwrap();
        assert_eq!(decompressed, expected_raw);

        let decoded = unmarshal_write_request(&decompressed).expect("decode");
        assert_eq!(decoded.timeseries[0].samples[0].value, 2.0);
    }

    #[test]
    fn empty_series_encodes_to_empty_message() {
        let raw = encode_write_request(&[]);
        assert!(raw.is_empty());
        let decoded = unmarshal_write_request(&raw).expect("decode");
        assert_eq!(decoded.timeseries.len(), 0);
    }
}
