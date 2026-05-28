//! Integration test for the VM-writes → esm-reads direction.
//!
//! Constructs a payload using VM's portable block format
//! (`MarshalPortable` + `MetricName.Marshal`), feeds it through our
//! `native_vm::parse`, and then through `esm-storage` to confirm that
//! samples produced by an upstream VM agent would land correctly in
//! EsMetrics' storage.

#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::type_complexity)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::needless_borrow)]
#![allow(clippy::unwrap_used)]

use esm_compress::int::{marshal_varint64, marshal_varuint64};
use esm_compress::timeseries::marshal_int64_array;
use esm_protocols::native_vm::{ParsedSample, parse};
use esm_storage::{Sample, Storage, TimeRange};

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
    marshal_varint64(&mut out, 0_i64);
    out.push(ts_result.marshal_type.as_byte());
    out.push(v_result.marshal_type.as_byte());
    out.push(64_u8);
    marshal_varuint64(&mut out, ts_data.len() as u64);
    out.extend_from_slice(&ts_data);
    marshal_varuint64(&mut out, v_data.len() as u64);
    out.extend_from_slice(&v_data);
    out
}

fn build_vm_payload(series: &[(&str, &[(&str, &str)], Vec<(i64, i64)>)]) -> Vec<u8> {
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    for (_, _, points) in series {
        for (t, _) in points {
            min_ts = min_ts.min(*t);
            max_ts = max_ts.max(*t);
        }
    }
    let mut body = Vec::new();
    body.extend_from_slice(&marshal_i64_be_zigzag(min_ts));
    body.extend_from_slice(&marshal_i64_be_zigzag(max_ts));
    for (name, tags, points) in series {
        let timestamps: Vec<i64> = points.iter().map(|(t, _)| *t).collect();
        let values: Vec<i64> = points.iter().map(|(_, v)| *v).collect();
        let name_bytes = marshal_metric_name(name, tags);
        let block_bytes = marshal_portable_block(&timestamps, &values);
        body.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
        body.extend_from_slice(&name_bytes);
        body.extend_from_slice(&(block_bytes.len() as u32).to_be_bytes());
        body.extend_from_slice(&block_bytes);
    }
    body
}

#[test]
fn vm_writes_esm_reads_end_to_end() {
    // Two series, several samples each.
    let series: Vec<(&str, &[(&str, &str)], Vec<(i64, i64)>)> = vec![
        (
            "http_requests_total",
            &[("job", "api"), ("inst", "1")],
            vec![(1_700_000_000_000, 100), (1_700_000_001_000, 200), (1_700_000_002_000, 300)],
        ),
        (
            "http_requests_total",
            &[("job", "api"), ("inst", "2")],
            vec![(1_700_000_000_000, 50), (1_700_000_001_000, 60)],
        ),
    ];
    let payload = build_vm_payload(&series);

    let parsed: Vec<ParsedSample> = parse(&payload).expect("decoder");
    assert_eq!(parsed.len(), 5);

    let tmp = tempfile::tempdir().unwrap();
    let mut storage = Storage::open(tmp.path().join("d")).unwrap();
    let samples: Vec<Sample> = parsed
        .into_iter()
        .map(|p| Sample {
            metric_name: p.metric_name,
            timestamp_ms: p.timestamp_ms,
            value: p.value,
        })
        .collect();
    storage.ingest(&samples).unwrap();
    storage.flush().unwrap();

    // Series 1: three samples
    let hits = storage
        .search_by_metric_name(
            br#"http_requests_total{inst="1",job="api"}"#,
            TimeRange { min_timestamp_ms: 0, max_timestamp_ms: i64::MAX },
        )
        .unwrap();
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0].value, 100);
    assert_eq!(hits[2].value, 300);

    // Series 2: two samples
    let hits = storage
        .search_by_metric_name(
            br#"http_requests_total{inst="2",job="api"}"#,
            TimeRange { min_timestamp_ms: 0, max_timestamp_ms: i64::MAX },
        )
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].value, 50);
}
