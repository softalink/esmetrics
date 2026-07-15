//! Decode-only port of the Prometheus remote-write `WriteRequest` protobuf.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `lib/prompb/write_request_unmarshaler.go` (decode path only) using
//! [`crate::wire::WireReader`] in place of `easyproto`.
//!
//! Deviations from the Go original:
//! - No native histogram support (`TimeSeries.histograms`, field 4): the Go
//!   unmarshaler expands native histograms into synthetic `_count`/`_sum`/
//!   `_bucket` series. That expansion is out of scope for this decode-only
//!   port; histogram fields are skipped like any other unknown field.
//! - `WriteRequest.metadata` (field 3) is skipped, not parsed — the ingestion
//!   path this crate serves does not consume metric metadata.
//! - No object pooling (`WriteRequestUnmarshaler`'s `labelsPool`/
//!   `samplesPool`/`sync.Pool` reuse): every call allocates fresh `Vec`s.
//! - `Label`/byte fields borrow `&[u8]` slices of `src` with no UTF-8
//!   validation, whereas Go's easyproto-backed `String()` accessor also does
//!   no validation (`unsafe` byte-to-string cast), so behavior matches.

pub use crate::wire::WireError;
use crate::wire::WireReader;

/// A single label (name/value pair) of a time series.
///
/// Go: `prompb.Label`. `name`/`value` are raw byte borrows of the input
/// buffer (`src` passed to [`unmarshal_write_request`]), not validated UTF-8.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Label<'a> {
    pub name: &'a [u8],
    pub value: &'a [u8],
}

/// A single sample (value + millisecond timestamp) of a time series.
///
/// Go: `prompb.Sample`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample {
    pub value: f64,
    pub timestamp: i64,
}

/// A time series: a set of labels plus its samples.
///
/// Go: `prompb.TimeSeries` (native-histogram fields are not represented;
/// see module-level deviations).
#[derive(Debug, Default, PartialEq)]
pub struct TimeSeries<'a> {
    pub labels: Vec<Label<'a>>,
    pub samples: Vec<Sample>,
}

/// A decoded Prometheus remote-write request.
///
/// Go: `prompb.WriteRequest` (the `Metadata` field is not represented; see
/// module-level deviations).
#[derive(Debug, Default, PartialEq)]
pub struct WriteRequest<'a> {
    pub timeseries: Vec<TimeSeries<'a>>,
}

/// Parses a Protobuf-encoded `WriteRequest` message from `src`.
///
/// Go: `WriteRequestUnmarshaler.UnmarshalProtobuf`. Field map:
/// `WriteRequest{1: repeated TimeSeries, 3: repeated MetricMetadata(skipped)}`
/// `TimeSeries{1: repeated Label, 2: repeated Sample, others skipped}`
/// `Label{1: name bytes, 2: value bytes}` `Sample{1: double value, 2: varint(int64) timestamp}`
/// Unknown fields are skipped by wire type, like easyproto.
pub fn unmarshal_write_request(src: &[u8]) -> Result<WriteRequest<'_>, WireError> {
    let mut wr = WriteRequest::default();
    let mut r = WireReader::new(src);
    while !r.is_eof() {
        let (field_num, wire_type) = r.read_tag()?;
        match field_num {
            1 => {
                if wire_type != 2 {
                    return Err(WireError::InvalidWireType(wire_type));
                }
                let data = r.read_len_delim()?;
                wr.timeseries.push(unmarshal_time_series(data)?);
            }
            _ => r.skip(wire_type)?,
        }
    }
    Ok(wr)
}

/// Go: `unmarshalTimeSeries`, restricted to the labels/samples fields
/// (native histograms are out of scope; see module-level deviations).
fn unmarshal_time_series(src: &[u8]) -> Result<TimeSeries<'_>, WireError> {
    let mut ts = TimeSeries::default();
    let mut r = WireReader::new(src);
    while !r.is_eof() {
        let (field_num, wire_type) = r.read_tag()?;
        match field_num {
            1 => {
                if wire_type != 2 {
                    return Err(WireError::InvalidWireType(wire_type));
                }
                let data = r.read_len_delim()?;
                ts.labels.push(unmarshal_label(data)?);
            }
            2 => {
                if wire_type != 2 {
                    return Err(WireError::InvalidWireType(wire_type));
                }
                let data = r.read_len_delim()?;
                ts.samples.push(unmarshal_sample(data)?);
            }
            _ => r.skip(wire_type)?,
        }
    }
    Ok(ts)
}

/// Go: `Label.unmarshalProtobuf`.
fn unmarshal_label(src: &[u8]) -> Result<Label<'_>, WireError> {
    let mut name: &[u8] = &[];
    let mut value: &[u8] = &[];
    let mut r = WireReader::new(src);
    while !r.is_eof() {
        let (field_num, wire_type) = r.read_tag()?;
        match field_num {
            1 => {
                if wire_type != 2 {
                    return Err(WireError::InvalidWireType(wire_type));
                }
                name = r.read_len_delim()?;
            }
            2 => {
                if wire_type != 2 {
                    return Err(WireError::InvalidWireType(wire_type));
                }
                value = r.read_len_delim()?;
            }
            _ => r.skip(wire_type)?,
        }
    }
    Ok(Label { name, value })
}

/// Go: `Sample.unmarshalProtobuf`. The timestamp is a plain proto3 `int64`
/// varint (two's-complement via `as i64`), not zigzag-encoded.
fn unmarshal_sample(src: &[u8]) -> Result<Sample, WireError> {
    let mut value = 0.0f64;
    let mut timestamp = 0i64;
    let mut r = WireReader::new(src);
    while !r.is_eof() {
        let (field_num, wire_type) = r.read_tag()?;
        match field_num {
            1 => {
                if wire_type != 1 {
                    return Err(WireError::InvalidWireType(wire_type));
                }
                value = r.read_double()?;
            }
            2 => {
                if wire_type != 0 {
                    return Err(WireError::InvalidWireType(wire_type));
                }
                timestamp = r.read_varint()? as i64;
            }
            _ => r.skip(wire_type)?,
        }
    }
    Ok(Sample { value, timestamp })
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- tiny protobuf wire-writer test helpers (no protobuf dependency) ---

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

    // --- message-shaped encoders built on the primitives above ---

    fn encode_label(name: &[u8], value: &[u8]) -> Vec<u8> {
        let mut dst = Vec::new();
        append_bytes_field(&mut dst, 1, name);
        append_bytes_field(&mut dst, 2, value);
        dst
    }

    fn encode_sample(value: f64, timestamp: i64) -> Vec<u8> {
        let mut dst = Vec::new();
        append_double_field(&mut dst, 1, value);
        append_varint_field(&mut dst, 2, timestamp);
        dst
    }

    fn encode_time_series(labels: &[(&[u8], &[u8])], samples: &[(f64, i64)]) -> Vec<u8> {
        let mut dst = Vec::new();
        for (name, value) in labels {
            append_bytes_field(&mut dst, 1, &encode_label(name, value));
        }
        for (value, ts) in samples {
            append_bytes_field(&mut dst, 2, &encode_sample(*value, *ts));
        }
        dst
    }

    fn encode_write_request(timeseries: &[Vec<u8>]) -> Vec<u8> {
        let mut dst = Vec::new();
        for ts in timeseries {
            append_bytes_field(&mut dst, 1, ts);
        }
        dst
    }

    #[test]
    fn empty_write_request() {
        let wr = unmarshal_write_request(&[]).unwrap();
        assert_eq!(wr, WriteRequest { timeseries: vec![] });
    }

    #[test]
    fn single_series_single_sample() {
        let ts = encode_time_series(
            &[(b"__name__", b"foo"), (b"job", b"x")],
            &[(42.5, 1727879909390)],
        );
        let src = encode_write_request(&[ts]);

        let wr = unmarshal_write_request(&src).unwrap();

        assert_eq!(
            wr,
            WriteRequest {
                timeseries: vec![TimeSeries {
                    labels: vec![
                        Label {
                            name: b"__name__",
                            value: b"foo"
                        },
                        Label {
                            name: b"job",
                            value: b"x"
                        },
                    ],
                    samples: vec![Sample {
                        value: 42.5,
                        timestamp: 1727879909390,
                    }],
                }],
            }
        );
    }

    #[test]
    fn multiple_series_reuse() {
        let ts1 = encode_time_series(&[(b"__name__", b"foo")], &[(1.0, 100)]);
        let ts2 = encode_time_series(
            &[(b"__name__", b"bar"), (b"instance", b"h1")],
            &[(2.0, 200), (3.0, 300)],
        );
        let src = encode_write_request(&[ts1, ts2]);

        let wr = unmarshal_write_request(&src).unwrap();

        assert_eq!(wr.timeseries.len(), 2);
        assert_eq!(
            wr.timeseries[0],
            TimeSeries {
                labels: vec![Label {
                    name: b"__name__",
                    value: b"foo"
                }],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 100
                }],
            }
        );
        assert_eq!(
            wr.timeseries[1],
            TimeSeries {
                labels: vec![
                    Label {
                        name: b"__name__",
                        value: b"bar"
                    },
                    Label {
                        name: b"instance",
                        value: b"h1"
                    },
                ],
                samples: vec![
                    Sample {
                        value: 2.0,
                        timestamp: 200
                    },
                    Sample {
                        value: 3.0,
                        timestamp: 300
                    },
                ],
            }
        );
    }

    #[test]
    fn unknown_fields_are_skipped() {
        let ts = encode_time_series(&[(b"__name__", b"foo")], &[(1.5, 5000)]);

        let mut src = Vec::new();
        // Unknown field 9, wire type 2 (length-delimited), at the
        // WriteRequest level: must be skipped without affecting decoding.
        append_bytes_field(&mut src, 9, b"unexpected-junk");
        // Field 3 (metadata) is a real WriteRequest field, but this port
        // skips it rather than parsing it (see module-level deviations).
        append_bytes_field(&mut src, 3, b"some-metadata-bytes");
        append_bytes_field(&mut src, 1, &ts);

        let wr = unmarshal_write_request(&src).unwrap();

        assert_eq!(
            wr,
            WriteRequest {
                timeseries: vec![TimeSeries {
                    labels: vec![Label {
                        name: b"__name__",
                        value: b"foo"
                    }],
                    samples: vec![Sample {
                        value: 1.5,
                        timestamp: 5000,
                    }],
                }],
            }
        );
    }

    #[test]
    fn truncated_input_errors() {
        let ts = encode_time_series(&[(b"__name__", b"foo")], &[(1.0, 100)]);
        let src = encode_write_request(&[ts]);

        // Chop off the last few bytes so the length-delimited TimeSeries
        // payload runs past the end of the buffer.
        let truncated = &src[..src.len() - 3];

        let err = unmarshal_write_request(truncated).unwrap_err();
        assert!(matches!(
            err,
            WireError::LengthOutOfRange | WireError::UnexpectedEof
        ));
    }

    #[test]
    fn negative_timestamp_roundtrips() {
        // -1i64 as u64 is all-ones (64 bits), which needs 10 varint bytes
        // (ceil(64/7) = 10) to encode — verifies plain two's-complement
        // varint decoding, not zigzag.
        let mut varint_bytes = Vec::new();
        append_varint(&mut varint_bytes, u64::MAX);
        assert_eq!(varint_bytes.len(), 10);

        let ts = encode_time_series(&[(b"__name__", b"foo")], &[(1.0, -1)]);
        let src = encode_write_request(&[ts]);

        let wr = unmarshal_write_request(&src).unwrap();

        assert_eq!(wr.timeseries.len(), 1);
        assert_eq!(
            wr.timeseries[0].samples,
            vec![Sample {
                value: 1.0,
                timestamp: -1,
            }]
        );
    }
}
