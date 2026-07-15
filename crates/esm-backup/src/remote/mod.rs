//! Remote backup destinations. Go: lib/backup/common.RemoteFS.

mod local;
mod object;

use std::io::{Read, Write};

pub use local::LocalRemote;
pub use object::ObjectRemote;

use crate::part::Part;

pub trait RemoteFs: Send + Sync {
    fn describe(&self) -> String;
    fn list_parts(&self) -> anyhow::Result<Vec<Part>>;
    fn delete_part(&self, p: &Part) -> anyhow::Result<()>;
    fn download_part(&self, p: &Part, w: &mut dyn Write) -> anyhow::Result<()>;
    fn upload_part(&self, p: &Part, r: &mut dyn Read) -> anyhow::Result<()>;
    /// Server-side copy of `p` from `src` into self. Ok(false) = not
    /// possible (different backend/bucket) — caller must stream-copy.
    fn copy_part_from(&self, src: &dyn RemoteFs, p: &Part) -> anyhow::Result<bool>;
    fn remove_empty_dirs(&self) -> anyhow::Result<()>;
    fn create_file(&self, file_path: &str, data: &[u8]) -> anyhow::Result<()>;
    fn delete_file(&self, file_path: &str) -> anyhow::Result<()>;
    fn has_file(&self, file_path: &str) -> anyhow::Result<bool>;
    fn read_file(&self, file_path: &str) -> anyhow::Result<Vec<u8>>;
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Builds a RemoteFs from a `-dst`/`-src`/`-origin` URL.
/// Credentials for cloud schemes come from standard env vars
/// (AWS_*, GOOGLE_APPLICATION_CREDENTIALS, AZURE_STORAGE_*).
pub fn new_remote_fs(url: &str) -> anyhow::Result<Box<dyn RemoteFs>> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| anyhow::anyhow!("missing scheme in {url:?}; expected <scheme>://<path>"))?;
    match scheme {
        "fs" => {
            anyhow::ensure!(
                std::path::Path::new(rest).is_absolute(),
                "dir must be absolute in fs:// url, got {rest:?}"
            );
            Ok(Box::new(LocalRemote::new(rest)?))
        }
        "s3" | "gs" | "gcs" | "azblob" => {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            anyhow::ensure!(!bucket.is_empty(), "missing bucket/container in {url:?}");
            Ok(Box::new(ObjectRemote::new(scheme, bucket, prefix)?))
        }
        other => {
            anyhow::bail!("unsupported scheme {other:?} in {url:?}; supported: fs, s3, gs, azblob")
        }
    }
}

/// Streams a part between two RemoteFs that cannot server-side copy:
/// a reader thread downloads into a bounded channel while the caller's
/// thread uploads. Go: actions.crossTypeCopy.
pub fn cross_copy(src: &dyn RemoteFs, dst: &dyn RemoteFs, p: &Part) -> anyhow::Result<()> {
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(4);
    std::thread::scope(|s| -> anyhow::Result<()> {
        let download = s.spawn(move || -> anyhow::Result<()> {
            let mut w = ChannelWriter { tx };
            src.download_part(p, &mut w)
        });
        let mut r = ChannelReader {
            rx,
            buf: Vec::new(),
            pos: 0,
        };
        let upload_res = dst.upload_part(p, &mut r);
        // Drop the reader (and its Receiver) before joining the download
        // thread: if upload_part bailed early without draining the channel,
        // the download thread may be blocked in tx.send() on a full bounded
        // channel. Dropping `r` disconnects the channel so that blocked send
        // errors out (ChannelWriter maps that to BrokenPipe) instead of
        // hanging forever.
        drop(r);
        let download_res = download.join().expect("download thread panicked");

        let Err(upload_err) = upload_res else {
            // Upload succeeded: any download failure is the real error.
            download_res?;
            return Ok(());
        };
        // The upload side failed. When it bails before draining the channel,
        // the download thread commonly dies with the synthetic
        // BrokenPipe("upload side gone") raised by ChannelWriter — that's a
        // side-effect of the upload failure, not an independent root cause,
        // so it must not mask the real upload error.
        match download_res {
            Err(download_err) if !is_synthetic_broken_pipe(&download_err) => {
                Err(upload_err.context(format!("download side also failed: {download_err:#}")))
            }
            _ => Err(upload_err),
        }
    })
}

/// True if `err` is (or wraps, via anyhow context) the synthetic BrokenPipe
/// that `ChannelWriter` raises when its paired upload side has already given
/// up and stopped draining the channel.
fn is_synthetic_broken_pipe(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_err| {
                io_err.kind() == std::io::ErrorKind::BrokenPipe
                    && io_err.to_string().contains("upload side gone")
            })
    })
}

struct ChannelWriter {
    tx: std::sync::mpsc::SyncSender<Vec<u8>>,
}

impl Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.tx
            .send(buf.to_vec())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "upload side gone"))?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct ChannelReader {
    rx: std::sync::mpsc::Receiver<Vec<u8>>,
    buf: Vec<u8>,
    pos: usize,
}

impl Read for ChannelReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.buf.len() {
            match self.rx.recv() {
                Ok(chunk) => {
                    self.buf = chunk;
                    self.pos = 0;
                }
                Err(_) => return Ok(0), // sender closed = EOF
            }
        }
        let n = (self.buf.len() - self.pos).min(out.len());
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("esm-backup-remote-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn factory_parses_schemes() {
        let dir = test_dir("factory");
        let url = format!("fs://{}", dir.display());
        assert!(new_remote_fs(&url).is_ok());
        assert!(new_remote_fs("fs://relative/path").is_err()); // must be absolute
        assert!(new_remote_fs("ftp://nope").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fs_remote_roundtrip() {
        let dir = test_dir("roundtrip");
        let fs = new_remote_fs(&format!("fs://{}", dir.display())).unwrap();
        let p = crate::part::Part {
            path: "data/f.bin".into(),
            file_size: 5,
            offset: 0,
            size: 5,
            actual_size: 5,
        };
        fs.upload_part(&p, &mut &b"hello"[..]).unwrap();
        let listed = fs.list_parts().unwrap();
        assert_eq!(listed, vec![p.clone()]);

        let mut out = Vec::new();
        fs.download_part(&p, &mut out).unwrap();
        assert_eq!(out, b"hello");

        // marker files are excluded from part listings
        fs.create_file("backup_complete.ignore", b"").unwrap();
        assert!(fs.has_file("backup_complete.ignore").unwrap());
        assert_eq!(fs.list_parts().unwrap().len(), 1);
        fs.delete_file("backup_complete.ignore").unwrap();
        assert!(!fs.has_file("backup_complete.ignore").unwrap());

        fs.delete_part(&p).unwrap();
        fs.remove_empty_dirs().unwrap();
        assert!(fs.list_parts().unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fs_remote_server_side_copy() {
        let a_dir = test_dir("copy-a");
        let b_dir = test_dir("copy-b");
        let a = new_remote_fs(&format!("fs://{}", a_dir.display())).unwrap();
        let b = new_remote_fs(&format!("fs://{}", b_dir.display())).unwrap();
        let p = crate::part::Part {
            path: "f.bin".into(),
            file_size: 3,
            offset: 0,
            size: 3,
            actual_size: 3,
        };
        a.upload_part(&p, &mut &b"abc"[..]).unwrap();
        assert!(b.copy_part_from(a.as_ref(), &p).unwrap()); // local↔local: hardlink/copy
        let mut out = Vec::new();
        b.download_part(&p, &mut out).unwrap();
        assert_eq!(out, b"abc");
        let _ = std::fs::remove_dir_all(&a_dir);
        let _ = std::fs::remove_dir_all(&b_dir);
    }

    #[test]
    fn cross_copy_between_local_remotes_succeeds() {
        let src_dir = test_dir("cross-copy-src");
        let dst_dir = test_dir("cross-copy-dst");
        let src = LocalRemote::new(&src_dir).unwrap();
        let dst = LocalRemote::new(&dst_dir).unwrap();
        let data = b"cross copy payload";
        let p = crate::part::Part {
            path: "f.bin".into(),
            file_size: data.len() as u64,
            offset: 0,
            size: data.len() as u64,
            actual_size: data.len() as u64,
        };
        src.upload_part(&p, &mut &data[..]).unwrap();

        cross_copy(&src, &dst, &p).unwrap();

        let mut out = Vec::new();
        dst.download_part(&p, &mut out).unwrap();
        assert_eq!(out, data);
        let _ = std::fs::remove_dir_all(&src_dir);
        let _ = std::fs::remove_dir_all(&dst_dir);
    }

    /// A destination whose `upload_part` reads a few bytes and then fails,
    /// without draining the rest of the channel. Used to prove `cross_copy`
    /// doesn't deadlock when this happens.
    struct FailingDst {
        fail_after: usize,
    }

    impl RemoteFs for FailingDst {
        fn describe(&self) -> String {
            "failing-dst".into()
        }
        fn list_parts(&self) -> anyhow::Result<Vec<Part>> {
            unimplemented!()
        }
        fn delete_part(&self, _p: &Part) -> anyhow::Result<()> {
            unimplemented!()
        }
        fn download_part(&self, _p: &Part, _w: &mut dyn Write) -> anyhow::Result<()> {
            unimplemented!()
        }
        fn upload_part(&self, _p: &Part, r: &mut dyn Read) -> anyhow::Result<()> {
            let mut buf = vec![0u8; self.fail_after];
            let _ = r.read(&mut buf)?;
            anyhow::bail!("simulated upload failure")
        }
        fn copy_part_from(&self, _src: &dyn RemoteFs, _p: &Part) -> anyhow::Result<bool> {
            unimplemented!()
        }
        fn remove_empty_dirs(&self) -> anyhow::Result<()> {
            unimplemented!()
        }
        fn create_file(&self, _file_path: &str, _data: &[u8]) -> anyhow::Result<()> {
            unimplemented!()
        }
        fn delete_file(&self, _file_path: &str) -> anyhow::Result<()> {
            unimplemented!()
        }
        fn has_file(&self, _file_path: &str) -> anyhow::Result<bool> {
            unimplemented!()
        }
        fn read_file(&self, _file_path: &str) -> anyhow::Result<Vec<u8>> {
            unimplemented!()
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    #[test]
    fn cross_copy_upload_failure_does_not_hang() {
        let src_dir = test_dir("cross-copy-fail-src");
        let src = LocalRemote::new(&src_dir).unwrap();
        // ~1 MiB file so the download thread produces far more chunks than
        // fit in the bounded channel (capacity 4): if the fix regresses,
        // the download thread blocks forever in tx.send() once upload_part
        // bails without draining the channel.
        let data = vec![7u8; 1024 * 1024];
        let p = crate::part::Part {
            path: "big.bin".into(),
            file_size: data.len() as u64,
            offset: 0,
            size: data.len() as u64,
            actual_size: data.len() as u64,
        };
        src.upload_part(&p, &mut &data[..]).unwrap();

        let dst = FailingDst { fail_after: 16 };

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let result = cross_copy(&src, &dst, &p);
            let _ = done_tx.send(result.map_err(|e| format!("{e:#}")));
        });

        match done_rx.recv_timeout(std::time::Duration::from_secs(30)) {
            Ok(Ok(())) => panic!("cross_copy should return Err on upload failure"),
            Ok(Err(msg)) => {
                assert!(
                    msg.contains("simulated upload failure"),
                    "error should surface the real upload failure, got: {msg}"
                );
                assert!(
                    !msg.contains("upload side gone"),
                    "error should not be masked by the synthetic download-side BrokenPipe, got: {msg}"
                );
            }
            Err(_) => panic!("cross_copy hung (deadlock) instead of returning promptly"),
        }
        let _ = std::fs::remove_dir_all(&src_dir);
    }
}
