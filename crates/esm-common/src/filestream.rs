// Ported from the upstream VictoriaMetrics lib/filestream (v1.146.0).
//! Buffered streaming file reader/writer with `Must*` close semantics.
//!
//! When `nocache` is set, Linux builds issue `posix_fadvise(DONTNEED)` for
//! each fully written/read 16MiB block (and `fdatasync` before dropping
//! written pages), so large sequential IO does not evict hot data from the
//! OS page cache. On Windows and non-Linux unixes this is a no-op.
//!
//! Deviations from Go:
//! - The `bufio.Reader`/`bufio.Writer` pooling is replaced by a bounded pool
//!   of plain `Vec<u8>` buffers.
//! - Go's Windows implementation calls `FlushFileBuffers` on every buffered
//!   write of a nocache writer; this port makes nocache tracking a no-op on
//!   non-Linux targets instead (data is still fsynced on `must_close`).
//! - Metrics counters are not ported.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use parking_lot::Mutex;

use crate::memory;

/// Block size for `fadvise(DONTNEED)` on nocache streams.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
const DONT_NEED_BLOCK_SIZE: u64 = 16 * 1024 * 1024;

/// Max number of pooled buffers kept per pool.
const MAX_POOLED_BUFFERS: usize = 256;

fn read_buffer_size() -> usize {
    static SIZE: OnceLock<usize> = OnceLock::new();
    *SIZE.get_or_init(|| (memory::allowed() / 1024 / 64).clamp(4 * 1024, 64 * 1024))
}

fn write_buffer_size() -> usize {
    static SIZE: OnceLock<usize> = OnceLock::new();
    *SIZE.get_or_init(|| (memory::allowed() / 1024 / 8).clamp(4 * 1024, 128 * 1024))
}

static READ_BUF_POOL: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());
static WRITE_BUF_POOL: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

fn get_buffer(pool: &Mutex<Vec<Vec<u8>>>, size: usize) -> Vec<u8> {
    if let Some(buf) = pool.lock().pop() {
        return buf;
    }
    Vec::with_capacity(size)
}

fn put_buffer(pool: &Mutex<Vec<Vec<u8>>>, mut buf: Vec<u8>) {
    buf.clear();
    let mut pool = pool.lock();
    if pool.len() < MAX_POOLED_BUFFERS {
        pool.push(buf);
    }
}

/// Tracks stream progress for `fadvise(DONTNEED)` on nocache streams.
#[derive(Default)]
struct StreamTracker {
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    nocache: bool,
    #[cfg(target_os = "linux")]
    offset: u64,
    #[cfg(target_os = "linux")]
    length: u64,
}

impl StreamTracker {
    #[cfg(target_os = "linux")]
    fn advise_dont_need(&mut self, f: &File, n: usize, fdatasync: bool) -> io::Result<()> {
        use std::os::unix::io::AsRawFd;

        self.length += n as u64;
        if !self.nocache || self.length < DONT_NEED_BLOCK_SIZE {
            return Ok(());
        }
        let block_size = self.length - self.length % DONT_NEED_BLOCK_SIZE;
        let fd = f.as_raw_fd();
        if fdatasync {
            // SAFETY: fd is a valid open file descriptor owned by `f`.
            if unsafe { libc::fdatasync(fd) } != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        // SAFETY: fd is a valid open file descriptor owned by `f`; the offset
        // and length describe a byte range, which is always valid for fadvise.
        let ret = unsafe {
            libc::posix_fadvise(
                fd,
                self.offset as libc::off_t,
                block_size as libc::off_t,
                libc::POSIX_FADV_DONTNEED,
            )
        };
        if ret != 0 {
            return Err(io::Error::from_raw_os_error(ret));
        }
        self.offset += block_size;
        self.length -= block_size;
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn advise_dont_need(&mut self, _f: &File, _n: usize, _fdatasync: bool) -> io::Result<()> {
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn close(&mut self, f: &File) -> io::Result<()> {
        use std::os::unix::io::AsRawFd;

        if !self.nocache {
            return Ok(());
        }
        // Advise the whole file as it shouldn't be cached.
        // SAFETY: fd is a valid open file descriptor owned by `f`.
        let ret = unsafe { libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        if ret != 0 {
            return Err(io::Error::from_raw_os_error(ret));
        }
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn close(&mut self, _f: &File) -> io::Result<()> {
        Ok(())
    }
}

/// Buffered file reader (upstream `filestream.Reader`).
pub struct Reader {
    f: File,
    path: PathBuf,
    buf: Vec<u8>,
    pos: usize,
    filled: usize,
    st: StreamTracker,
}

impl Reader {
    /// Opens the file from the given path (upstream `filestream.MustOpen`).
    ///
    /// If `nocache` is set, then the reader doesn't pollute the OS page cache.
    pub fn must_open(path: impl AsRef<Path>, nocache: bool) -> Reader {
        let path = path.as_ref();
        let f =
            File::open(path).unwrap_or_else(|e| panic!("FATAL: cannot open file {path:?}: {e}"));
        let mut buf = get_buffer(&READ_BUF_POOL, read_buffer_size());
        buf.resize(read_buffer_size(), 0);
        Reader {
            f,
            path: path.to_path_buf(),
            buf,
            pos: 0,
            filled: 0,
            st: StreamTracker {
                nocache,
                ..Default::default()
            },
        }
    }

    /// Opens the file at the given path at the given offset
    /// (upstream `filestream.OpenReaderAt`).
    pub fn open_reader_at(
        path: impl AsRef<Path>,
        offset: u64,
        nocache: bool,
    ) -> io::Result<Reader> {
        let path = path.as_ref();
        let mut r = Reader::must_open(path, nocache);
        match r.f.seek(SeekFrom::Start(offset)) {
            Ok(n) if n == offset => Ok(r),
            Ok(n) => {
                r.must_close();
                Err(io::Error::other(format!(
                    "invalid seek offset for {path:?}; got {n}; want {offset}"
                )))
            }
            Err(e) => {
                r.must_close();
                Err(io::Error::other(format!(
                    "cannot seek to offset={offset} for {path:?}: {e}"
                )))
            }
        }
    }

    /// Returns the path to the file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Closes the reader (upstream `Reader.MustClose`); panics on failure.
    pub fn must_close(mut self) {
        if let Err(e) = self.st.close(&self.f) {
            panic!(
                "FATAL: cannot close streamTracker for file {:?}: {e}",
                self.path
            );
        }
        let buf = std::mem::take(&mut self.buf);
        put_buffer(&READ_BUF_POOL, buf);
        // The file is closed on drop; std::fs::File cannot report close errors.
    }
}

impl Read for Reader {
    fn read(&mut self, p: &mut [u8]) -> io::Result<usize> {
        if p.is_empty() {
            return Ok(0);
        }
        if self.pos == self.filled {
            if p.len() >= self.buf.len() {
                // Large read: bypass the buffer (bufio.Reader behavior).
                let n = self.f.read(p)?;
                self.st
                    .advise_dont_need(&self.f, n, false)
                    .map_err(|e| advise_error(&self.path, e))?;
                return Ok(n);
            }
            self.filled = self.f.read(&mut self.buf)?;
            self.pos = 0;
            if self.filled == 0 {
                return Ok(0); // EOF
            }
            self.st
                .advise_dont_need(&self.f, self.filled, false)
                .map_err(|e| advise_error(&self.path, e))?;
        }
        let n = p.len().min(self.filled - self.pos);
        p[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

fn advise_error(path: &Path, e: io::Error) -> io::Error {
    io::Error::other(format!("advise error for {path:?}: {e}"))
}

/// Buffered file writer (upstream `filestream.Writer`).
pub struct Writer {
    f: File,
    path: PathBuf,
    buf: Vec<u8>,
    st: StreamTracker,
}

impl Writer {
    /// Creates (truncating) the file at the given path
    /// (upstream `filestream.MustCreate`).
    ///
    /// If `nocache` is set, the writer doesn't pollute the OS page cache.
    pub fn must_create(path: impl AsRef<Path>, nocache: bool) -> Writer {
        let path = path.as_ref();
        let f = File::create(path)
            .unwrap_or_else(|e| panic!("FATAL: cannot create file {path:?}: {e}"));
        Writer::new(f, path, nocache)
    }

    /// Opens the file at path for writing at the given offset, creating it if
    /// missing (upstream `filestream.OpenWriterAt`).
    pub fn open_writer_at(
        path: impl AsRef<Path>,
        offset: u64,
        nocache: bool,
    ) -> io::Result<Writer> {
        let path = path.as_ref();
        let mut f = File::options()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let n = f.seek(SeekFrom::Start(offset))?;
        if n != offset {
            return Err(io::Error::other(format!(
                "invalid seek offset for {path:?}; got {n}; want {offset}"
            )));
        }
        Ok(Writer::new(f, path, nocache))
    }

    fn new(f: File, path: &Path, nocache: bool) -> Writer {
        Writer {
            f,
            path: path.to_path_buf(),
            buf: get_buffer(&WRITE_BUF_POOL, write_buffer_size()),
            st: StreamTracker {
                nocache,
                ..Default::default()
            },
        }
    }

    /// Returns the path to the file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Flushes all the buffered data to the file (upstream `Writer.MustFlush`).
    ///
    /// If `is_sync` is true, the flushed data is fsynced to storage.
    pub fn must_flush(&mut self, is_sync: bool) {
        self.flush_buf_or_panic();
        if is_sync {
            self.sync_or_panic();
        }
    }

    /// Syncs the file to storage and closes it (upstream `Writer.MustClose`);
    /// panics on failure.
    pub fn must_close(mut self) {
        self.flush_buf_or_panic();
        let buf = std::mem::take(&mut self.buf);
        put_buffer(&WRITE_BUF_POOL, buf);
        self.sync_or_panic();
        if let Err(e) = self.st.close(&self.f) {
            panic!(
                "FATAL: cannot close streamTracker for file {:?}: {e}",
                self.path
            );
        }
        // The file is closed on drop; std::fs::File cannot report close errors.
    }

    fn flush_buf(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            self.f.write_all(&self.buf)?;
            self.buf.clear();
        }
        Ok(())
    }

    fn flush_buf_or_panic(&mut self) {
        if let Err(e) = self.flush_buf() {
            panic!(
                "FATAL: cannot flush buffered data to file {:?}: {e}",
                self.path
            );
        }
    }

    fn sync_or_panic(&mut self) {
        if !crate::fs::is_fsync_disabled() {
            if let Err(e) = self.f.sync_all() {
                panic!("FATAL: cannot sync file {:?}: {e}", self.path);
            }
        }
    }
}

impl Write for Writer {
    fn write(&mut self, p: &[u8]) -> io::Result<usize> {
        let cap = self.buf.capacity();
        let mut written = 0;
        while written < p.len() {
            if self.buf.is_empty() && p.len() - written >= cap {
                // Large write: bypass the buffer (bufio.Writer behavior).
                let n = self.f.write(&p[written..])?;
                written += n;
                continue;
            }
            let avail = cap - self.buf.len();
            if avail == 0 {
                self.flush_buf()?;
                continue;
            }
            let take = avail.min(p.len() - written);
            self.buf.extend_from_slice(&p[written..written + take]);
            written += take;
        }
        self.st
            .advise_dont_need(&self.f, p.len(), true)
            .map_err(|e| advise_error(&self.path, e))?;
        Ok(p.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_buf()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_dir(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "esm-filestream-test-{name}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn test_write_read(dir: &Path, nocache: bool, test_data: &[u8]) {
        let path = dir.join("nocache_test.txt");

        let mut w = Writer::must_create(&path, nocache);
        w.write_all(test_data).unwrap();
        w.must_close();

        let mut r = Reader::must_open(&path, nocache);
        assert_eq!(r.path(), path.as_path());
        let mut buf = vec![0u8; test_data.len()];
        r.read_exact(&mut buf).unwrap();
        assert_eq!(buf, test_data, "unexpected data read (nocache={nocache})");
        // Reading past EOF returns 0.
        assert_eq!(r.read(&mut [0u8; 16]).unwrap(), 0);
        r.must_close();
    }

    #[test]
    fn write_read_roundtrip() {
        let dir = temp_dir("roundtrip");
        for nocache in [false, true] {
            test_write_read(&dir, nocache, b"");
            test_write_read(&dir, nocache, b"foobar");
            test_write_read(&dir, nocache, b"a\nb\nc\n");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn write_read_roundtrip_large() {
        // Exercise the fadvise(DONTNEED) path: > 3 * DONT_NEED_BLOCK_SIZE.
        let dir = temp_dir("roundtrip-large");
        let mut data = Vec::new();
        while (data.len() as u64) < 3 * DONT_NEED_BLOCK_SIZE {
            let line = format!("line {}\n", data.len());
            data.extend_from_slice(line.as_bytes());
        }
        for nocache in [false, true] {
            test_write_read(&dir, nocache, &data);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn open_reader_at_seeks_to_offset() {
        let dir = temp_dir("reader-at");
        let path = dir.join("f.bin");
        let mut w = Writer::must_create(&path, false);
        w.write_all(b"0123456789").unwrap();
        w.must_close();

        let mut r = Reader::open_reader_at(&path, 4, true).unwrap();
        let mut buf = [0u8; 6];
        r.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"456789");
        r.must_close();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn open_writer_at_writes_at_offset() {
        let dir = temp_dir("writer-at");
        let path = dir.join("f.bin");
        let mut w = Writer::must_create(&path, false);
        w.write_all(b"0123456789").unwrap();
        w.must_close();

        let mut w = Writer::open_writer_at(&path, 4, false).unwrap();
        w.write_all(b"WXYZ").unwrap();
        w.must_flush(true);
        w.must_close();

        assert_eq!(std::fs::read(&path).unwrap(), b"0123WXYZ89");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    #[should_panic(expected = "cannot open file")]
    fn must_open_missing_file_panics() {
        let dir = temp_dir("missing");
        let _ = Reader::must_open(dir.join("no-such-file"), false);
    }
}
