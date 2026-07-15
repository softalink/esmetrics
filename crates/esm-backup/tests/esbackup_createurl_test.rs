//! End-to-end coverage for the esbackup `-snapshot.createURL` lifecycle:
//! create a snapshot via HTTP, back it up, then delete it via HTTP — all
//! exercised through the real `esbackup` binary against a minimal stub
//! snapshot-API server.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SNAPSHOT_NAME: &str = "20260705000000-0000000A";

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "esm-backup-createurl-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Reads a single HTTP/1.1 request (request line + headers up to the blank
/// line, body ignored — our client never sends one) and writes back a fixed
/// JSON response, then closes the connection.
fn handle_conn(mut stream: TcpStream, requests: &Mutex<Vec<String>>) {
    stream.set_nonblocking(false).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());

    let mut request_line = String::new();
    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
        return;
    }
    loop {
        let mut header_line = String::new();
        match reader.read_line(&mut header_line) {
            Ok(0) => break,
            Ok(_) if header_line == "\r\n" || header_line == "\n" => break,
            Ok(_) => continue,
            Err(_) => break,
        }
    }

    let request_line = request_line.trim_end().to_string();
    let path = request_line.split_whitespace().nth(1).unwrap_or("");
    let body = if path.starts_with("/snapshot/create") {
        format!(r#"{{"status":"ok","snapshot":"{SNAPSHOT_NAME}"}}"#)
    } else {
        r#"{"status":"ok"}"#.to_string()
    };
    requests.lock().unwrap().push(request_line);

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

#[test]
fn esbackup_create_url_lifecycle_creates_backs_up_and_deletes_snapshot() {
    let scratch = test_dir("main");
    let data_dir = scratch.join("data");
    let backup_dir = scratch.join("backup");

    // Pre-create the snapshot dir the stub's /snapshot/create response will
    // claim to have created, with one small file to back up.
    let snapshot_dir = data_dir.join("snapshots").join(SNAPSHOT_NAME);
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    std::fs::write(snapshot_dir.join("small.bin"), b"hello world").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let requests: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let server_requests = Arc::clone(&requests);

    let server = std::thread::spawn(move || {
        // Bounded wait so a broken lifecycle (missing create/delete call)
        // fails the test instead of hanging the suite forever.
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut handled = 0;
        while handled < 2 && Instant::now() < deadline {
            match listener.accept() {
                Ok((stream, _)) => {
                    handle_conn(stream, &server_requests);
                    handled += 1;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
    });

    let create_url = format!("http://{addr}/snapshot/create");
    let output = Command::new(env!("CARGO_BIN_EXE_esbackup"))
        .arg(format!("-storageDataPath={}", data_dir.display()))
        .arg(format!("-snapshot.createURL={create_url}"))
        .arg(format!("-dst=fs://{}", backup_dir.display()))
        .output()
        .expect("failed to run esbackup binary");

    server.join().unwrap();

    assert!(
        output.status.success(),
        "esbackup exited non-zero (status {:?}): stdout={} stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        backup_dir.join("backup_complete.ignore").exists(),
        "backup_complete.ignore missing from dst"
    );

    let recorded = requests.lock().unwrap();
    assert!(
        recorded
            .iter()
            .any(|line| line.contains("GET /snapshot/create")),
        "stub never recorded a create request; recorded: {recorded:?}"
    );
    assert!(
        recorded
            .iter()
            .any(|line| line.contains("GET /snapshot/delete")
                && line.contains(&format!("snapshot={SNAPSHOT_NAME}"))),
        "stub never recorded a matching delete request; recorded: {recorded:?}"
    );

    let _ = std::fs::remove_dir_all(&scratch);
}
