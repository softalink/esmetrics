use super::*;

// ---------------------------------------------------------------------------
// datadogutil: SplitTag / SanitizeName.
// Golden vectors ported verbatim from
// `lib/protoparser/datadogutil/datadogutil_test.go`.
// ---------------------------------------------------------------------------

#[test]
fn split_tag_matches_upstream_golden_vectors() {
    assert_eq!(split_tag(""), ("", "no_label_value"));
    assert_eq!(split_tag("foo"), ("foo", "no_label_value"));
    assert_eq!(split_tag("foo:bar"), ("foo", "bar"));
    assert_eq!(split_tag(":bar"), ("", "bar"));
}

#[test]
fn split_tag_splits_on_first_colon_only() {
    assert_eq!(split_tag("foo:bar:baz"), ("foo", "bar:baz"));
}

#[test]
fn sanitize_name_matches_upstream_golden_vectors() {
    let f = |s: &str, expected: &str| assert_eq!(sanitize_name(s), expected, "input {s:?}");
    f("before.dot.metric!.name", "before.dot.metric.name");
    f("after.dot.metric.!name", "after.dot.metric.name");
    f("in.the.middle.met!ric.name", "in.the.middle.met_ric.name");
    f(
        "before.and.after.and.middle.met!ric!.!name",
        "before.and.after.and.middle.met_ric.name",
    );
    f(
        "many.consecutive.met!!!!ric!!.!!name",
        "many.consecutive.met_ric.name",
    );
    f(
        "many.non.consecutive.m!e!t!r!i!c!.!name",
        "many.non.consecutive.m_e_t_r_i_c.name",
    );
    f(
        "how.about.underscores_.!_metric!_!.__!!name",
        "how.about.underscores.metric.name",
    );
    f(
        "how.about.underscores.middle.met!_!_ric.name",
        "how.about.underscores.middle.met_ric.name",
    );
}

// ---------------------------------------------------------------------------
// v1::Request::unmarshal.
// Golden vectors ported from `lib/protoparser/datadogv1/parser_test.go`.
// ---------------------------------------------------------------------------

mod v1_tests {
    use super::v1::{Point, Request, Series};

    #[test]
    fn unmarshal_failure_cases_from_upstream() {
        for s in ["", "foobar", r#"{"series":123"#, "1234", "[]"] {
            let mut req = Request::default();
            assert!(
                req.unmarshal(s.as_bytes()).is_err(),
                "expected error for {s:?}"
            );
        }
    }

    #[test]
    fn unmarshal_null_body_is_a_no_op_not_an_error() {
        // Go: json.Unmarshal(null, *Request) is documented as a no-op for
        // non-pointer/map/slice destinations, not an error.
        let mut req = Request::default();
        req.unmarshal(b"null").unwrap();
        assert_eq!(req, Request::default());
    }

    #[test]
    fn unmarshal_empty_object_yields_zero_series() {
        let mut req = Request::default();
        req.unmarshal(b"{}").unwrap();
        assert_eq!(req, Request::default());
    }

    #[test]
    fn unmarshal_success_matches_upstream_fixture() {
        let body = br#"
{
  "series": [
    {
      "host": "test.example.com",
      "interval": 20,
      "metric": "system.load.1",
      "device": "/dev/sda",
      "points": [[
        1575317847,
        0.5
      ]],
      "tags": [
        "environment:test"
      ],
      "type": "rate"
    }
  ]
}
"#;
        let mut req = Request::default();
        req.unmarshal(body).unwrap();
        assert_eq!(req.series.len(), 1);
        let s = &req.series[0];
        assert_eq!(s.host, "test.example.com");
        assert_eq!(s.metric, "system.load.1");
        assert_eq!(s.device, "/dev/sda");
        assert_eq!(
            s.points,
            vec![Point {
                timestamp_seconds: 1575317847.0,
                value: 0.5,
            }]
        );
        assert_eq!(s.tags, vec!["environment:test".to_owned()]);
    }

    #[test]
    fn unmarshal_missing_host_resets_previous_request_fields() {
        // Go: TestRequestUnmarshalMissingHost — a `Request` reused across
        // calls must not leak a previous call's Host/Device into a new
        // parse where those fields are absent.
        let mut req = Request {
            series: vec![Series {
                host: "prev-host".to_owned(),
                device: "prev-device".to_owned(),
                ..Series::default()
            }],
        };
        let body = br#"
{
  "series": [
    {
      "metric": "system.load.1",
      "points": [[
        1575317847,
        0.5
      ]]
    }
  ]
}"#;
        req.unmarshal(body).unwrap();
        assert_eq!(req.series.len(), 1);
        assert_eq!(req.series[0].host, "");
        assert_eq!(req.series[0].device, "");
        assert_eq!(req.series[0].metric, "system.load.1");
    }

    #[test]
    fn missing_or_nonpositive_timestamp_is_filled_with_current_time() {
        let mut req = Request::default();
        req.unmarshal(br#"{"series":[{"metric":"m","points":[[0,1],[-5,2],[100,3]]}]}"#)
            .unwrap();
        let pts = &req.series[0].points;
        assert_eq!(pts[2].timestamp_seconds, 100.0, "positive ts kept as-is");
        let now = super::current_time_seconds() as f64;
        for pt in [&pts[0], &pts[1]] {
            assert!(
                (pt.timestamp_seconds - now).abs() < 5.0,
                "expected ~now, got {}",
                pt.timestamp_seconds
            );
        }
    }

    #[test]
    fn missing_point_element_defaults_to_zero() {
        let mut req = Request::default();
        req.unmarshal(br#"{"series":[{"metric":"m","points":[[100]]}]}"#)
            .unwrap();
        assert_eq!(req.series[0].points[0].value, 0.0);
    }

    #[test]
    fn wrong_series_type_errors() {
        let mut req = Request::default();
        assert!(req.unmarshal(br#"{"series":"nope"}"#).is_err());
    }

    #[test]
    fn wrong_field_type_errors() {
        let mut req = Request::default();
        assert!(req.unmarshal(br#"{"series":[{"metric":123}]}"#).is_err());
    }

    // Go: encoding/json treats a JSON `null` as a no-op for every
    // destination type (nil for slice/map/pointer/interface, zero value for
    // struct/scalar), at any nesting depth — never an error. These verify
    // the port matches that at each level, not just for a top-level null.

    #[test]
    fn null_series_field_yields_empty_series() {
        let mut req = Request::default();
        req.unmarshal(br#"{"series":null}"#).unwrap();
        assert_eq!(req, Request::default());
    }

    #[test]
    fn null_scalar_and_array_fields_are_zero_valued() {
        let mut req = Request::default();
        req.unmarshal(
            br#"{"series":[{"metric":null,"host":null,"device":null,"tags":null,"points":null}]}"#,
        )
        .unwrap();
        assert_eq!(req.series, vec![Series::default()]);
    }

    #[test]
    fn null_point_element_defaults_to_zero_value() {
        let mut req = Request::default();
        req.unmarshal(br#"{"series":[{"metric":"m","points":[[null,5]]}]}"#)
            .unwrap();
        // The null timestamp decodes to 0.0, then the missing-timestamp
        // fixup replaces it with the current time; the value is kept.
        assert_eq!(req.series[0].points[0].value, 5.0);
    }

    #[test]
    fn null_series_element_is_a_zero_valued_series() {
        let mut req = Request::default();
        req.unmarshal(br#"{"series":[null]}"#).unwrap();
        assert_eq!(req.series, vec![Series::default()]);
    }

    #[test]
    fn null_tag_element_is_an_empty_string() {
        let mut req = Request::default();
        req.unmarshal(br#"{"series":[{"tags":[null]}]}"#).unwrap();
        assert_eq!(req.series[0].tags, vec![String::new()]);
    }

    #[test]
    fn non_null_wrong_type_still_errors() {
        // Only `null` becomes a no-op; a genuinely wrong-typed value keeps
        // the crate's strict all-or-nothing validation.
        let mut req = Request::default();
        assert!(req.unmarshal(br#"{"series":[{"metric":123}]}"#).is_err());
        assert!(req
            .unmarshal(br#"{"series":[{"points":[["x",5]]}]}"#)
            .is_err());
    }
}

// ---------------------------------------------------------------------------
// v2::Request::unmarshal_json / unmarshal_protobuf.
// Golden vectors ported from `lib/protoparser/datadogv2/parser_test.go`.
// ---------------------------------------------------------------------------

mod v2_tests {
    use super::v2::{Point, Request, Resource};

    #[test]
    fn unmarshal_json_failure_cases_from_upstream() {
        for s in ["", "foobar", r#"{"series":123"#, "1234", "[]"] {
            let mut req = Request::default();
            assert!(
                req.unmarshal_json(s.as_bytes()).is_err(),
                "expected error for {s:?}"
            );
        }
    }

    #[test]
    fn unmarshal_json_empty_object_yields_zero_series() {
        let mut req = Request::default();
        req.unmarshal_json(b"{}").unwrap();
        assert_eq!(req, Request::default());
    }

    #[test]
    fn unmarshal_json_success_matches_upstream_fixture() {
        let body = br#"
{
  "series": [
    {
      "metric": "system.load.1",
      "type": 0,
      "points": [
        {
          "timestamp": 1636629071,
          "value": 0.7
        }
      ],
      "resources": [
        {
          "name": "dummyhost",
          "type": "host"
        }
      ],
      "source_type_name": "kubernetes",
      "tags": ["environment:test"]
    }
  ]
}
"#;
        let mut req = Request::default();
        req.unmarshal_json(body).unwrap();
        assert_eq!(req.series.len(), 1);
        let s = &req.series[0];
        assert_eq!(s.metric, "system.load.1");
        assert_eq!(
            s.points,
            vec![Point {
                timestamp: 1636629071,
                value: 0.7,
            }]
        );
        assert_eq!(
            s.resources,
            vec![Resource {
                name: "dummyhost".to_owned(),
                r#type: "host".to_owned(),
            }]
        );
        assert_eq!(s.source_type_name, "kubernetes");
        assert_eq!(s.tags, vec!["environment:test".to_owned()]);
    }

    #[test]
    fn unmarshal_json_missing_or_nonpositive_timestamp_is_filled() {
        let mut req = Request::default();
        req.unmarshal_json(br#"{"series":[{"metric":"m","points":[{"value":1},{"timestamp":-1,"value":2},{"timestamp":100,"value":3}]}]}"#)
            .unwrap();
        let pts = &req.series[0].points;
        assert_eq!(pts[2].timestamp, 100);
        let now = super::current_time_seconds();
        for pt in [&pts[0], &pts[1]] {
            assert!(
                (pt.timestamp - now).abs() <= 5,
                "expected ~now, got {}",
                pt.timestamp
            );
        }
    }

    // Go: encoding/json null-as-no-op, same as v1 — verified at each level
    // of the v2 JSON shape.

    #[test]
    fn null_series_field_yields_empty_series() {
        let mut req = Request::default();
        req.unmarshal_json(br#"{"series":null}"#).unwrap();
        assert_eq!(req, Request::default());
    }

    #[test]
    fn null_scalar_and_array_fields_are_zero_valued() {
        let mut req = Request::default();
        req.unmarshal_json(
            br#"{"series":[{"metric":null,"source_type_name":null,"points":null,"resources":null,"tags":null}]}"#,
        )
        .unwrap();
        assert_eq!(req.series, vec![super::v2::Series::default()]);
    }

    #[test]
    fn null_point_fields_default_to_zero() {
        let mut req = Request::default();
        req.unmarshal_json(
            br#"{"series":[{"metric":"m","points":[{"timestamp":null,"value":null}]}]}"#,
        )
        .unwrap();
        // timestamp null -> 0 -> filled with current time by fixup;
        // value null -> 0.0, kept.
        assert_eq!(req.series[0].points[0].value, 0.0);
    }

    #[test]
    fn null_resource_fields_default_to_empty_strings() {
        let mut req = Request::default();
        req.unmarshal_json(br#"{"series":[{"resources":[{"name":null,"type":null}]}]}"#)
            .unwrap();
        assert_eq!(req.series[0].resources, vec![Resource::default()]);
    }

    #[test]
    fn null_series_element_is_a_zero_valued_series() {
        let mut req = Request::default();
        req.unmarshal_json(br#"{"series":[null]}"#).unwrap();
        assert_eq!(req.series, vec![super::v2::Series::default()]);
    }

    #[test]
    fn null_tag_element_is_an_empty_string() {
        let mut req = Request::default();
        req.unmarshal_json(br#"{"series":[{"tags":[null]}]}"#)
            .unwrap();
        assert_eq!(req.series[0].tags, vec![String::new()]);
    }

    #[test]
    fn non_null_wrong_type_still_errors() {
        let mut req = Request::default();
        assert!(req
            .unmarshal_json(br#"{"series":[{"metric":123}]}"#)
            .is_err());
        assert!(req
            .unmarshal_json(br#"{"series":[{"points":[{"timestamp":"x"}]}]}"#)
            .is_err());
    }

    // --- protobuf ---

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

    fn append_len_delim(dst: &mut Vec<u8>, field_num: u32, bytes: &[u8]) {
        append_tag(dst, field_num, 2);
        append_varint(dst, bytes.len() as u64);
        dst.extend_from_slice(bytes);
    }

    fn append_string_field(dst: &mut Vec<u8>, field_num: u32, s: &str) {
        append_len_delim(dst, field_num, s.as_bytes());
    }

    /// Builds one protobuf-encoded `Point` message (`value=1 double`,
    /// `timestamp=2 int64`).
    fn encode_point(timestamp: i64, value: f64) -> Vec<u8> {
        let mut buf = Vec::new();
        append_tag(&mut buf, 1, 1); // fixed64 (double)
        buf.extend_from_slice(&value.to_bits().to_le_bytes());
        append_tag(&mut buf, 2, 0); // varint
        append_varint(&mut buf, timestamp as u64);
        buf
    }

    /// Builds one protobuf-encoded `Resource` message (`type=1`, `name=2`).
    fn encode_resource(name: &str, typ: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        append_string_field(&mut buf, 1, typ);
        append_string_field(&mut buf, 2, name);
        buf
    }

    /// Builds one protobuf-encoded `Series` message.
    fn encode_series(
        metric: &str,
        points: &[(i64, f64)],
        resources: &[(&str, &str)],
        tags: &[&str],
        source_type_name: &str,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        for (name, typ) in resources {
            append_len_delim(&mut buf, 1, &encode_resource(name, typ));
        }
        append_string_field(&mut buf, 2, metric);
        for tag in tags {
            append_string_field(&mut buf, 3, tag);
        }
        for (ts, val) in points {
            append_len_delim(&mut buf, 4, &encode_point(*ts, *val));
        }
        if !source_type_name.is_empty() {
            append_string_field(&mut buf, 7, source_type_name);
        }
        buf
    }

    /// Builds a protobuf-encoded `Request` message (`repeated Series series = 1`).
    fn encode_request(series: &[Vec<u8>]) -> Vec<u8> {
        let mut buf = Vec::new();
        for s in series {
            append_len_delim(&mut buf, 1, s);
        }
        buf
    }

    #[test]
    fn unmarshal_protobuf_roundtrips_all_fields() {
        let series_bytes = encode_series(
            "system.load.1",
            &[(1636629071, 0.7)],
            &[("dummyhost", "host")],
            &["environment:test"],
            "kubernetes",
        );
        let body = encode_request(&[series_bytes]);

        let mut req = Request::default();
        req.unmarshal_protobuf(&body).unwrap();
        assert_eq!(req.series.len(), 1);
        let s = &req.series[0];
        assert_eq!(s.metric, "system.load.1");
        assert_eq!(
            s.points,
            vec![Point {
                timestamp: 1636629071,
                value: 0.7,
            }]
        );
        assert_eq!(
            s.resources,
            vec![Resource {
                name: "dummyhost".to_owned(),
                r#type: "host".to_owned(),
            }]
        );
        assert_eq!(s.source_type_name, "kubernetes");
        assert_eq!(s.tags, vec!["environment:test".to_owned()]);
    }

    #[test]
    fn unmarshal_protobuf_multiple_series_and_tags() {
        let a = encode_series("a", &[(1, 1.0)], &[], &["x:1", "y:2"], "");
        let b = encode_series("b", &[(2, 2.0), (3, 3.0)], &[], &[], "");
        let body = encode_request(&[a, b]);

        let mut req = Request::default();
        req.unmarshal_protobuf(&body).unwrap();
        assert_eq!(req.series.len(), 2);
        assert_eq!(req.series[0].metric, "a");
        assert_eq!(req.series[0].tags, vec!["x:1".to_owned(), "y:2".to_owned()]);
        assert_eq!(req.series[1].metric, "b");
        assert_eq!(req.series[1].points.len(), 2);
    }

    #[test]
    fn unmarshal_protobuf_missing_or_nonpositive_timestamp_is_filled() {
        let series_bytes = encode_series("m", &[(0, 1.0), (-5, 2.0)], &[], &[], "");
        let body = encode_request(&[series_bytes]);
        let mut req = Request::default();
        req.unmarshal_protobuf(&body).unwrap();
        let now = super::current_time_seconds();
        for pt in &req.series[0].points {
            assert!((pt.timestamp - now).abs() <= 5);
        }
    }

    #[test]
    fn unmarshal_protobuf_rejects_truncated_input() {
        let mut req = Request::default();
        assert!(
            req.unmarshal_protobuf(&[0x0a]).is_err(),
            "truncated varint/len"
        );
    }

    #[test]
    fn unmarshal_protobuf_skips_unknown_fields() {
        // Field 99 (varint) before a valid series (field 1).
        let mut buf = Vec::new();
        append_tag(&mut buf, 99, 0);
        append_varint(&mut buf, 12345);
        let series_bytes = encode_series("m", &[(10, 1.0)], &[], &[], "");
        append_len_delim(&mut buf, 1, &series_bytes);

        let mut req = Request::default();
        req.unmarshal_protobuf(&buf).unwrap();
        assert_eq!(req.series.len(), 1);
        assert_eq!(req.series[0].metric, "m");
    }
}

// ---------------------------------------------------------------------------
// v1::parse_stream / v2::parse_stream.
// ---------------------------------------------------------------------------

mod stream_tests {
    use super::v1;
    use super::v2;
    use std::io::Write;

    #[test]
    fn v1_parse_stream_sanitizes_metric_name_and_invokes_callback() {
        let body = br#"{"series":[{"metric":"foo!bar","points":[[100,1.5]]}]}"#;
        let mut got = None;
        v1::parse_stream(body.as_slice(), "", |series| {
            got = Some(series.to_vec());
            Ok(())
        })
        .unwrap();
        let series = got.unwrap();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].metric, "foo_bar");
        assert_eq!(series[0].points[0].value, 1.5);
    }

    #[test]
    fn v1_parse_stream_gzip_body_is_decoded() {
        let body = br#"{"series":[{"metric":"m","points":[[100,1.0]]}]}"#;
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        enc.write_all(body).unwrap();
        let gz = enc.finish().unwrap();

        let mut count = 0;
        v1::parse_stream(gz.as_slice(), "gzip", |series| {
            count = series.len();
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn v1_parse_stream_propagates_unmarshal_error() {
        let err = v1::parse_stream(b"not json".as_slice(), "", |_| Ok(())).unwrap_err();
        assert!(matches!(err, super::Error::Unmarshal(_)));
    }

    #[test]
    fn v1_parse_stream_propagates_callback_error() {
        let body = br#"{"series":[{"metric":"m","points":[[100,1.0]]}]}"#;
        let err = v1::parse_stream(body.as_slice(), "", |_| Err("boom".into())).unwrap_err();
        assert!(matches!(err, super::Error::Callback(_)));
    }

    #[test]
    fn v2_parse_stream_defaults_to_json_when_content_type_is_not_protobuf() {
        let body = br#"{"series":[{"metric":"foo!bar","points":[{"timestamp":100,"value":2.0}]}]}"#;
        let mut got = None;
        v2::parse_stream(body.as_slice(), "", "", |series| {
            got = Some(series.to_vec());
            Ok(())
        })
        .unwrap();
        let series = got.unwrap();
        assert_eq!(series[0].metric, "foo_bar");
        assert_eq!(series[0].points[0].value, 2.0);
    }

    #[test]
    fn v2_parse_stream_decodes_protobuf_when_content_type_matches() {
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
        fn append_len_delim(dst: &mut Vec<u8>, field_num: u32, bytes: &[u8]) {
            append_tag(dst, field_num, 2);
            append_varint(dst, bytes.len() as u64);
            dst.extend_from_slice(bytes);
        }
        // One Point message (value=1 double, timestamp=2 varint int64).
        let mut point = Vec::new();
        append_tag(&mut point, 1, 1);
        point.extend_from_slice(&3.0f64.to_bits().to_le_bytes());
        append_tag(&mut point, 2, 0);
        append_varint(&mut point, 100);

        // One Series message (metric=2 string, points=4 message).
        let mut series = Vec::new();
        append_len_delim(&mut series, 2, b"proto!name");
        append_len_delim(&mut series, 4, &point);

        // Request message (series=1 message).
        let mut body = Vec::new();
        append_len_delim(&mut body, 1, &series);

        let mut got = None;
        v2::parse_stream(body.as_slice(), "", "application/x-protobuf", |series| {
            got = Some(series.to_vec());
            Ok(())
        })
        .unwrap();
        let series = got.unwrap();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].metric, "proto_name", "sanitized like JSON path");
        assert_eq!(series[0].points[0].timestamp, 100);
        assert_eq!(series[0].points[0].value, 3.0);
    }

    #[test]
    fn v2_parse_stream_protobuf_with_charset_suffix_still_uses_json() {
        // Go's switch is an exact string match; a Content-Type with extra
        // parameters (e.g. a charset) falls through to the JSON default,
        // just like upstream.
        let body = br#"{"series":[{"metric":"m","points":[{"timestamp":1,"value":1.0}]}]}"#;
        let mut count = 0;
        v2::parse_stream(
            body.as_slice(),
            "",
            "application/x-protobuf; charset=utf-8",
            |series| {
                count = series.len();
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(count, 1);
    }
}
