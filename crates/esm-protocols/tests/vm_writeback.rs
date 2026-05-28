//! End-to-end test: esm-encoded native_vm payload → upstream
//! VictoriaMetrics → JSON export. Validates the H6 direction
//! (esm-writes → VM-reads).
//!
//! Skipped unless `VM_URL` is set to a live VM HTTP endpoint. The CI
//! workflow starts a VM container and exports `VM_URL=http://127.0.0.1:18430`.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::print_stderr)]
#![allow(clippy::missing_panics_doc)]

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

#[test]
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_possible_truncation)]
fn esm_writes_vm_reads_end_to_end() {
    let Some(url) = vm_url() else {
        eprintln!("VM_URL not set — skipping VM-roundtrip test");
        return;
    };
    let authority = url.trim_start_matches("http://").split('/').next().unwrap();
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    // Use a unique series suffix so the assertion is robust to prior runs.
    let suffix = now_ms;
    let metric_name = format!("esm_test_total_{suffix}");
    let series = vec![native_vm::Series {
        metric_name: &metric_name,
        labels: vec![("inst", "1")],
        samples: vec![(now_ms, 111), (now_ms + 1000, 222), (now_ms + 2000, 333)],
    }];
    let payload = native_vm::encode(&series);

    let (status, _) =
        http(authority, "POST", "/api/v1/import/native", &payload, "application/octet-stream");
    assert!(status.contains("204") || status.contains("200"), "VM rejected our payload: {status}");

    // Force a flush so the export call sees the data.
    let _ = http(authority, "GET", "/internal/force_flush", &[], "text/plain");
    std::thread::sleep(std::time::Duration::from_millis(1500));

    // Pull the data back via the JSON export.
    let q = format!(
        "/api/v1/export?match%5B%5D={}&start=2026-01-01T00:00:00Z&end=2027-12-31T23:59:59Z",
        urlencode(&metric_name)
    );
    let (qstatus, body) = http(authority, "GET", &q, &[], "text/plain");
    assert!(qstatus.contains("200"), "export failed: {qstatus}");
    assert!(body.contains(&metric_name), "metric not found in export: {body}");
    assert!(body.contains("111"), "missing value 111: {body}");
    assert!(body.contains("222"), "missing value 222: {body}");
    assert!(body.contains("333"), "missing value 333: {body}");
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
