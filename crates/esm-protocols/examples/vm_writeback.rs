//! Encode samples via our `native_vm::encode` and POST them to a real
//! upstream VictoriaMetrics container. Used as a one-shot verifier of
//! the esm → VM direction. Not a unit test — needs a live VM on the
//! configured URL.
//!
//! Run via:
//! ```sh
//! VM_URL=http://127.0.0.1:18430 \
//!     cargo run --release --example vm_writeback --package esm-protocols
//! ```

#![allow(clippy::print_stderr)]
#![allow(clippy::print_stdout)]
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{SystemTime, UNIX_EPOCH};

use esm_protocols::native_vm;

fn main() {
    let vm_url = std::env::var("VM_URL").unwrap_or_else(|_| "http://127.0.0.1:18430".into());
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let series = vec![
        native_vm::Series {
            metric_name: "esm_roundtrip_total",
            labels: vec![("inst", "1"), ("job", "esm")],
            samples: vec![(now_ms, 700), (now_ms + 1000, 800), (now_ms + 2000, 900)],
        },
        native_vm::Series {
            metric_name: "esm_roundtrip_total",
            labels: vec![("inst", "2"), ("job", "esm")],
            samples: vec![(now_ms, 17), (now_ms + 1000, 33)],
        },
    ];
    let payload = native_vm::encode(&series);
    eprintln!("encoded {} bytes", payload.len());

    let url = vm_url.strip_prefix("http://").expect("http://");
    let (authority, rest) = url.split_once('/').map_or((url, ""), |(a, p)| (a, p));
    let path = if rest.is_empty() {
        "/api/v1/import/native".to_string()
    } else {
        format!("/{rest}/api/v1/import/native")
    };
    let mut stream = TcpStream::connect(authority).expect("connect");
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    stream.write_all(req.as_bytes()).expect("write headers");
    stream.write_all(&payload).expect("write body");
    let mut buf = String::new();
    stream.read_to_string(&mut buf).expect("read response");
    let status = buf.lines().next().unwrap_or("");
    println!("VM response: {status}");
    println!("now_ms={now_ms}");
    println!("samples emitted: {}", series.iter().map(|s| s.samples.len()).sum::<usize>());
}
