//! Differential codec test against a live VictoriaMetrics oracle.
//!
//! We feed pseudo-random sample sequences through `native_vm::encode`,
//! POST them to a running VM, then read them back via
//! `/api/v1/export` (JSON). VM accepting our payload + returning the
//! exact same values is the strongest proof of byte-format compatibility
//! we can produce without re-implementing VM.
//!
//! Skipped unless `VM_URL` is set.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::print_stderr)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{SystemTime, UNIX_EPOCH};

use esm_protocols::native_vm;

fn vm_url() -> Option<String> {
    std::env::var("VM_URL").ok()
}

fn http(
    authority: &str,
    method: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
) -> (String, String) {
    let mut stream = TcpStream::connect(authority).expect("connect");
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Length: {}\r\nContent-Type: {content_type}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(req.as_bytes()).unwrap();
    if !body.is_empty() {
        stream.write_all(body).unwrap();
    }
    let mut buf = String::new();
    let _ = stream.read_to_string(&mut buf);
    let status = buf.lines().next().unwrap_or("").to_string();
    let body_start = buf.find("\r\n\r\n").map_or(buf.len(), |i| i + 4);
    (status, buf[body_start..].to_string())
}

/// xorshift64 — deterministic and good enough for our case generation.
struct Xs64(u64);
impl Xs64 {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn urlencode(s: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

#[test]
fn differential_codec_vs_vm() {
    let Some(url) = vm_url() else {
        eprintln!("VM_URL not set — skipping");
        return;
    };
    let authority = url.trim_start_matches("http://").split('/').next().unwrap();
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let mut rng = Xs64(now_ms as u64);

    // Three randomized cases × 32 samples each.
    let mut failures = Vec::new();
    for case_idx in 0..3 {
        let metric_name = format!("esm_diff_codec_{now_ms}_{case_idx}");
        let mut samples = Vec::new();
        for i in 0..32 {
            let v = (rng.next() % 20_000) as i64 - 10_000;
            samples.push((now_ms + i64::from(i) * 1000, v));
        }
        let case_label = case_idx.to_string();
        let series = vec![native_vm::Series {
            metric_name: &metric_name,
            labels: vec![("case", &case_label)],
            samples: samples.clone(),
        }];
        let payload = native_vm::encode(&series);

        let (status, _) =
            http(authority, "POST", "/api/v1/import/native", &payload, "application/octet-stream");
        if !status.contains("204") {
            failures.push(format!("case {case_idx}: VM rejected payload: {status}"));
            continue;
        }
        let _ = http(authority, "GET", "/internal/force_flush", &[], "text/plain");
        std::thread::sleep(std::time::Duration::from_millis(1500));

        let q = format!(
            "/api/v1/export?match%5B%5D={}&start=2026-01-01T00:00:00Z&end=2027-12-31T23:59:59Z",
            urlencode(&metric_name)
        );
        let (_, body) = http(authority, "GET", &q, &[], "text/plain");

        // Parse VM's JSON-lines export: each line is
        // {"metric":..., "values":[...], "timestamps":[...]}.
        let line = body.lines().find(|l| l.contains(&metric_name)).unwrap_or("");
        if line.is_empty() {
            failures.push(format!("case {case_idx}: metric absent in export"));
            continue;
        }
        // Extract `values` and `timestamps` arrays without a full JSON parser.
        let want_values: Vec<i64> = samples.iter().map(|(_, v)| *v).collect();
        let want_ts: Vec<i64> = samples.iter().map(|(t, _)| *t).collect();
        let got_values = parse_int_array_in_json(line, "\"values\":[").unwrap_or_default();
        let got_ts = parse_int_array_in_json(line, "\"timestamps\":[").unwrap_or_default();
        if got_values != want_values {
            failures.push(format!(
                "case {case_idx}: values differ: want {want_values:?}, got {got_values:?}"
            ));
        }
        if got_ts != want_ts {
            failures.push(format!(
                "case {case_idx}: timestamps differ: want {want_ts:?}, got {got_ts:?}"
            ));
        }
    }
    assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
}

fn parse_int_array_in_json(json: &str, marker: &str) -> Option<Vec<i64>> {
    let start = json.find(marker)? + marker.len();
    let end = json[start..].find(']')? + start;
    let inner = &json[start..end];
    inner
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<i64>().ok())
        .collect()
}
