//! Byte-identity tests against real VictoriaMetrics v1.144.0 output.
//!
//! The fixtures under `tests/fixtures/` are produced by running an
//! actual VM container, ingesting a known dataset, and exporting via
//! `/api/v1/export/native`. This pins the wire-format expectations of
//! our `native_vm::parse` decoder to the upstream reality, not just
//! our own encoder.

use esm_protocols::native_vm::parse;

const FIXTURE_HTTP_REQUESTS: &[u8] = include_bytes!("fixtures/vm-native-http-requests.bin");

#[test]
fn decodes_real_vm_native_export() {
    let parsed = parse(FIXTURE_HTTP_REQUESTS).expect("decoder");
    assert!(!parsed.is_empty(), "expected ≥ 1 sample from the VM-produced fixture");

    // Sanity-check the canonical metric_name form.
    for sample in &parsed {
        let name = std::str::from_utf8(&sample.metric_name).expect("utf8");
        assert!(name.starts_with("http_requests_total"));
        assert!(name.contains("job=\"api\""));
        assert!(name.contains("instance=\""));
    }

    // The dataset we ingested into VM had instance="1" and instance="2".
    let mut saw_inst1 = false;
    let mut saw_inst2 = false;
    for sample in &parsed {
        let name = std::str::from_utf8(&sample.metric_name).unwrap();
        if name.contains("instance=\"1\"") {
            saw_inst1 = true;
        }
        if name.contains("instance=\"2\"") {
            saw_inst2 = true;
        }
    }
    assert!(saw_inst1 && saw_inst2, "expected both inst=1 and inst=2 series");

    // Values were 100, 200 and 50 — confirm they appear.
    let values: std::collections::BTreeSet<i64> = parsed.iter().map(|s| s.value).collect();
    assert!(values.contains(&100), "missing value 100; got {values:?}");
    assert!(values.contains(&200), "missing value 200; got {values:?}");
    assert!(values.contains(&50), "missing value 50; got {values:?}");
}
