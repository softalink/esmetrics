//! End-to-end test: ingest into server A, flush, snapshot, back up the
//! snapshot with the esm-backup library to a `fs://` destination, restore
//! into a fresh data dir, then serve the restored data from server B.
//!
//! `test_flags()`/`http_get` are copied from `server_test.rs` (test binaries
//! can't share code without a common module; copying is this repo's existing
//! convention for integration tests).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

use esmetrics::flags::Flags;

fn test_flags() -> Flags {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "esmetrics-backup-e2e-test-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    Flags {
        http_listen_addr: "127.0.0.1:0".to_string(),
        storage_data_path: dir.to_string_lossy().into_owned(),
        ..Flags::default()
    }
}

/// Sends `GET <target>` with `Connection: close` and returns
/// (status line, body).
fn http_get(addr: SocketAddr, target: &str) -> (String, String) {
    let mut stream = TcpStream::connect(addr).expect("connect failed");
    write!(
        stream,
        "GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .expect("write failed");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read failed");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed response: {response:?}"));
    let status_line = head.lines().next().unwrap_or_default().to_string();
    (status_line, body.to_string())
}

/// Sends `POST <target>` with `Connection: close` and the given body,
/// returning (status line, body). Mirrors `http_get` above.
fn http_post(addr: SocketAddr, target: &str, body: &str) -> (String, String) {
    let mut stream = TcpStream::connect(addr).expect("connect failed");
    write!(
        stream,
        "POST {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .expect("write failed");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read failed");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed response: {response:?}"));
    let status_line = head.lines().next().unwrap_or_default().to_string();
    (status_line, body.to_string())
}

#[test]
fn ingest_snapshot_backup_restore_serve() {
    // 1. Server A: ingest via Influx line protocol, flush, snapshot.
    let flags_a = test_flags();
    let server_a = esmetrics::run(&flags_a).expect("run failed");
    let addr_a = server_a.local_addr();

    // Use a "now-ish" timestamp so it always falls inside the default
    // -retentionPeriod (1 month): a fixed historical timestamp would age
    // out of retention over time.
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos() as i64;
    let now_secs = now_ns / 1_000_000_000;

    let line = format!("e2e_metric,tag=v value=42 {now_ns}");
    let (status, _) = http_post(addr_a, "/write", &line);
    // esm-insert's /write returns 204 No Content on success (see
    // crates/esm-insert/tests/influx_write.rs).
    assert_eq!(status, "HTTP/1.1 204 No Content");
    http_get(addr_a, "/internal/force_flush");
    let (_, body) = http_get(addr_a, "/snapshot/create");
    let name = body
        .strip_prefix("{\"status\":\"ok\",\"snapshot\":\"")
        .and_then(|s| s.strip_suffix("\"}"))
        .expect("create response")
        .to_string();

    // 2. Backup the snapshot with the esm-backup library (fs:// dst).
    let backup_dir =
        std::env::temp_dir().join(format!("esmetrics-e2e-backup-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&backup_dir);
    let snap_dir = std::path::PathBuf::from(&flags_a.storage_data_path)
        .join("snapshots")
        .join(&name);
    let src = esm_backup::localfs::LocalFs::new(&snap_dir);
    let dst = esm_backup::remote::new_remote_fs(&format!("fs://{}", backup_dir.display())).unwrap();
    esm_backup::backup::Backup {
        concurrency: 4,
        src: &src,
        dst: dst.as_ref(),
        origin: None,
        created_at: None,
    }
    .run()
    .unwrap();
    server_a.stop();

    // 3. Restore into a fresh dir and serve it with server B.
    let flags_b = test_flags();
    esm_backup::restore::Restore {
        concurrency: 4,
        src: dst.as_ref(),
        dst_dir: std::path::PathBuf::from(&flags_b.storage_data_path),
        skip_backup_complete_check: false,
    }
    .run()
    .unwrap();
    let server_b = esmetrics::run(&flags_b).expect("run failed");
    // esm-select's /api/v1/series takes `match[]` (URL-encoded as
    // `match%5B%5D`) plus unix-second `start`/`end` params (see
    // crates/esm-select/tests/http_test.rs golden tests). esm-insert names
    // the ingested metric `{measurement}_{field_key}` (see
    // crates/esm-insert/src/influx.rs), so the `value` field of the
    // `e2e_metric` line becomes `e2e_metric_value`.
    let (status, body) = http_get(
        server_b.local_addr(),
        &format!(
            "/api/v1/series?match%5B%5D=e2e_metric_value&start={}&end={}",
            now_secs - 3600,
            now_secs + 3600
        ),
    );
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(
        body.contains("e2e_metric"),
        "restored data must be queryable: {body}"
    );
    server_b.stop();

    let _ = std::fs::remove_dir_all(&backup_dir);
}
