//! Property tests for ingest parsers.
//!
//! Goal: each parser must never panic on any input. We feed it well-formed
//! inputs (success path) and arbitrary bytes (don't-panic path).

use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    #[test]
    fn text_exposition_no_panic(s in ".{0,500}") {
        let _ = esm_protocols::text_exposition::parse(&s, 0);
    }

    #[test]
    fn influx_no_panic(s in ".{0,500}") {
        let _ = esm_protocols::influx_line::parse(&s, 0, 1_000_000);
    }

    #[test]
    fn graphite_no_panic(s in ".{0,500}") {
        let _ = esm_protocols::graphite::parse(&s, 0);
    }

    #[test]
    fn opentsdb_telnet_no_panic(s in ".{0,500}") {
        let _ = esm_protocols::opentsdb::parse_telnet(&s);
    }

    #[test]
    fn opentsdb_http_no_panic(s in ".{0,500}") {
        let _ = esm_protocols::opentsdb::parse_http_json(&s);
    }

    #[test]
    fn datadog_no_panic(s in ".{0,500}") {
        let _ = esm_protocols::datadog::parse(&s);
    }

    #[test]
    fn csv_no_panic(s in ".{0,500}") {
        let _ = esm_protocols::csv_import::parse(&s);
    }

    #[test]
    fn newrelic_no_panic(bytes in prop::collection::vec(any::<u8>(), 0..500)) {
        let _ = esm_protocols::newrelic::parse(&bytes);
    }

    #[test]
    fn otlp_no_panic(bytes in prop::collection::vec(any::<u8>(), 0..500)) {
        let _ = esm_protocols::otlp::parse(&bytes);
    }

    #[test]
    fn prom_remote_write_no_panic(bytes in prop::collection::vec(any::<u8>(), 0..500)) {
        let _ = esm_protocols::prom_remote_write::parse_snappy(&bytes);
        let _ = esm_protocols::prom_remote_write::parse_proto(&bytes);
    }
}
