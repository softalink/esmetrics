//! Microbenchmark: split block value-decode into its zstd-decompress vs
//! delta-decode (varint + prefix-sum) components, to judge whether a SIMD
//! prefix-sum is worth pursuing. Run:
//!
//! ```text
//! cargo test -p esm-compress --release --test decode_split -- --ignored --nocapture
//! ```
#![allow(clippy::print_stderr, clippy::cast_precision_loss, clippy::unwrap_used)]

use std::time::Instant;

use esm_compress::timeseries::{MarshalType, marshal_int64_array, unmarshal_int64_array};
use esm_compress::zstd_codec::decompress_zstd;

const ROWS: usize = 8192; // MAX_ROWS_PER_BLOCK
const PRECISION: u8 = 64;

/// TSBS cpu-usage-like random walk: integers in [0,100], small drifts.
fn tsbs_values() -> Vec<i64> {
    let mut v = Vec::with_capacity(ROWS);
    let mut x: i64 = 50;
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    for _ in 0..ROWS {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let step = ((seed >> 33) % 11) as i64 - 5; // -5..=5
        x = (x + step).clamp(0, 100);
        v.push(x);
    }
    v
}

#[test]
#[ignore = "manual microbench; run with --release --ignored --nocapture"]
fn decode_split() {
    let values = tsbs_values();
    let mut marshaled = Vec::new();
    let res = marshal_int64_array(&mut marshaled, &values, PRECISION).unwrap();
    eprintln!(
        "marshal_type={:?}  marshaled_bytes={} ({:.2} bytes/sample)",
        res.marshal_type,
        marshaled.len(),
        marshaled.len() as f64 / ROWS as f64
    );

    let iters = 20_000u32;

    // Full decode: zstd decompress + delta reconstruct.
    let mut out = Vec::with_capacity(ROWS);
    let t = Instant::now();
    for _ in 0..iters {
        out.clear();
        unmarshal_int64_array(&mut out, &marshaled, res.marshal_type, res.first_value, ROWS)
            .unwrap();
    }
    let full = t.elapsed();
    assert_eq!(out.len(), ROWS);

    // zstd-decompress component alone (only if the type is zstd-wrapped).
    let zstd_wrapped =
        matches!(res.marshal_type, MarshalType::ZstdNearestDelta | MarshalType::ZstdNearestDelta2);
    let zstd = if zstd_wrapped {
        let mut buf = Vec::new();
        let t = Instant::now();
        for _ in 0..iters {
            decompress_zstd(&mut buf, &marshaled).unwrap();
        }
        Some((t.elapsed(), buf.len()))
    } else {
        None
    };

    let ns = |d: std::time::Duration| d.as_secs_f64() * 1e9 / f64::from(iters) / ROWS as f64;
    eprintln!(
        "full decode      : {:.2} ns/sample  ({:.1}M samples/s)",
        ns(full),
        1000.0 / ns(full)
    );
    if let Some((z, raw_len)) = zstd {
        eprintln!(
            "  zstd decompress: {:.2} ns/sample  ({} -> {} bytes)",
            ns(z),
            marshaled.len(),
            raw_len
        );
        eprintln!(
            "  delta decode   : {:.2} ns/sample  (= full - zstd; SIMD-able part)",
            ns(full) - ns(z)
        );
        eprintln!(
            "  -> zstd is {:.0}% of decode, delta is {:.0}%",
            100.0 * ns(z) / ns(full),
            100.0 * (ns(full) - ns(z)) / ns(full)
        );
    }
}
