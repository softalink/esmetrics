//! `PersistentQueue`: a durable FIFO of opaque `Vec<u8>` blocks (snappy-
//! compressed `WriteRequest`s from [`crate::pendingseries`]) that survives
//! process restarts, feeding the remote-write client (a later task).
//!
//! Faithful *behavior* port of upstream vmagent's `lib/persistentqueue`
//! (`Queue` + the in-memory `FastQueue` wrapper), not its exact chunk/
//! metadata file format — this queue has no external reader, so the on-disk
//! representation is a private implementation detail.
//!
//! # On-disk format
//!
//! One file per block under `dir/`, named by a monotonically increasing
//! `u64` index rendered as a fixed-width, zero-padded decimal string (e.g.
//! `00000000000000000007`). Zero-padding to a fixed width makes lexical
//! filename order equal numeric index order, so [`PersistentQueue::open`]
//! only needs to sort filenames to recover FIFO order — no separate index/
//! metadata file is needed. The file's entire contents are the block's
//! bytes; there is no length prefix or header because one file always holds
//! exactly one block.
//!
//! A block is written durably with a temp-file-then-rename: bytes are
//! written to `<index>.tmp`, `fsync`ed, then renamed to `<index>`. Rename is
//! atomic on both POSIX and Windows for a same-directory target that does
//! not yet exist (true here: indices are never reused), so a reader can
//! never observe a partially written block file. A crash between the
//! `fsync` and the `rename`, or mid-write, leaves at most a stray `.tmp`
//! file, which [`PersistentQueue::open`] recognizes and deletes; it is
//! never mistaken for a valid block.
//!
//! # Write-through, not deferred spill
//!
//! Unlike upstream's `FastQueue` (which buffers whole blocks in memory and
//! only spills to the on-disk `Queue` past a block-count threshold),
//! `push` here writes each block's temp-file-then-rename to disk
//! *synchronously, every time*, before it becomes visible to `pop`. This
//! trades a per-push disk write for a strictly simpler durability story,
//! with two distinct guarantees worth separating:
//!
//! - **Process-crash durability (per-push):** once `push` returns `Ok`,
//!   the block's bytes have been written, `fsync`ed, and atomically
//!   `rename`d into place. If the *process* dies (panic, kill, OOM) right
//!   after that, a fresh `open` replays the block — there is no
//!   in-memory-only window that `flush_to_disk`/`close` must close first.
//!   This is what the reopen test exercises.
//! - **Power-loss durability (needs the directory fsync):** the per-push
//!   `fsync` makes the block *file's* contents durable, but the directory
//!   entry created by the `rename` is only guaranteed durable across a
//!   power loss / OS crash after the *containing directory* is itself
//!   `fsync`ed. That is what `flush_to_disk` does (and `close` calls it):
//!   it fsyncs the queue directory. `flush_to_disk`/`close` have no
//!   per-block *content* left to flush — only this directory-entry
//!   durability step. Opening a directory as a file isn't portable to
//!   Windows, so the step is best-effort and silently skipped there;
//!   per-file `sync_all` already covers the practical requirement of a
//!   normal close/reopen on every platform.
//!
//! # Concurrency
//!
//! One `Mutex<Inner>` guards the in-memory `VecDeque<QueuedBlock>` (the
//! `pop`-serving fast path), the running byte total, the next block index,
//! and a `closed` flag; a `Condvar` wakes blocked poppers. `push` holds the
//! mutex for its entire body, including the disk write — this serializes
//! pushes but keeps index assignment, capacity enforcement, and enqueueing
//! atomic with respect to each other and to concurrent `pop`s, which is
//! what rules out interleavings that would break FIFO order or lose a
//! wakeup. `pop` recomputes its remaining wait time against a fixed
//! deadline on every loop iteration, so spurious `Condvar` wakeups (which
//! the std docs explicitly allow) can't cause an early `None` or a hang.
//!
//! # Size cap
//!
//! `max_bytes` bounds the sum of queued block lengths. A `push` that would
//! exceed it first drops the oldest queued block(s) (removing them from
//! disk too) until the new block fits or the queue is empty — matching
//! upstream's default drop-oldest `-remoteWrite.maxDiskUsagePerURL`
//! behavior. A single block larger than `max_bytes` is still accepted once
//! the queue is empty (there is nothing left to drop), rather than
//! rejected. `max_bytes == 0` means unlimited (see [`PersistentQueue::open`]),
//! matching the `-remoteWrite.maxDiskUsagePerURL` flag's documented default.

use std::collections::VecDeque;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// Fixed width of a block's decimal index filename (`u64::MAX` is 20
/// digits). Zero-padding to this width makes lexical filename order equal
/// numeric index order, which is what lets [`PersistentQueue::open`] recover
/// FIFO order by sorting filenames.
const INDEX_WIDTH: usize = 20;

/// Error returned by [`PersistentQueue::open`] and [`PersistentQueue::push`].
/// Both are the only fallible operations; `pop`/`pending_bytes`/
/// `flush_to_disk`/`close` never fail (I/O errors there are logged and
/// degrade gracefully rather than propagated, per the "never panic on a
/// disk error" contract this queue is built to).
#[derive(Debug)]
pub enum QueueError {
    Io(std::io::Error),
}

impl std::fmt::Display for QueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueError::Io(e) => write!(f, "persistent queue I/O error: {e}"),
        }
    }
}

impl std::error::Error for QueueError {}

impl From<std::io::Error> for QueueError {
    fn from(e: std::io::Error) -> Self {
        QueueError::Io(e)
    }
}

/// One queued block, tagged with the on-disk index it was (or will be)
/// persisted under so it can be located for removal on `pop` or drop-oldest
/// eviction.
struct QueuedBlock {
    index: u64,
    data: Vec<u8>,
}

/// Mutex-guarded queue state. See the module doc for the concurrency model.
struct Inner {
    blocks: VecDeque<QueuedBlock>,
    current_bytes: u64,
    next_index: u64,
    closed: bool,
}

/// A durable FIFO of opaque byte blocks. See the module doc for the on-disk
/// format, durability, concurrency, and size-cap behavior.
pub struct PersistentQueue {
    dir: PathBuf,
    max_bytes: u64,
    inner: Mutex<Inner>,
    not_empty: Condvar,
}

impl PersistentQueue {
    /// Opens (creating if absent) the queue directory `dir` and replays any
    /// on-disk blocks, in FIFO order, into memory. `max_bytes` bounds the
    /// sum of queued block lengths; if the on-disk state already exceeds it
    /// (e.g. `max_bytes` shrank since the last run), the oldest blocks are
    /// dropped immediately to re-establish the invariant. `max_bytes == 0`
    /// means unlimited (matches `-remoteWrite.maxDiskUsagePerURL`'s
    /// documented "0 = unlimited" default in `flags.rs`) — internally
    /// normalized to `u64::MAX` so the size-cap logic in [`open`](Self::open)
    /// and [`push`](Self::push) never has to special-case it again.
    ///
    /// A fresh, empty (or freshly created) `dir` opens as an empty queue.
    /// Unreadable block files are logged and skipped rather than failing
    /// the whole open; stray `.tmp` files from a prior crash mid-write are
    /// deleted.
    pub fn open(dir: &Path, max_bytes: u64) -> Result<PersistentQueue, QueueError> {
        let max_bytes = if max_bytes == 0 { u64::MAX } else { max_bytes };
        fs::create_dir_all(dir)?;

        let mut indexed: Vec<(u64, PathBuf)> = Vec::new();
        for entry in fs::read_dir(dir)? {
            let Ok(entry) = entry else { continue };
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if let Some(index) = parse_index(name) {
                indexed.push((index, path));
            } else if name.ends_with(".tmp") {
                // Stray partial write from a crash between fsync and
                // rename (or mid-write) in a prior run; never a valid block.
                let _ = fs::remove_file(&path);
            }
        }
        indexed.sort_unstable_by_key(|(index, _)| *index);

        let mut blocks = VecDeque::with_capacity(indexed.len());
        let mut current_bytes: u64 = 0;
        let mut next_index: u64 = 0;
        for (index, path) in indexed {
            match fs::read(&path) {
                Ok(data) => {
                    current_bytes += data.len() as u64;
                    next_index = next_index.max(index + 1);
                    blocks.push_back(QueuedBlock { index, data });
                }
                Err(e) => {
                    log::warn!(
                        "esmagent: persistent queue: skipping unreadable block file {}: {e}",
                        path.display()
                    );
                }
            }
        }

        while current_bytes > max_bytes {
            let Some(dropped) = blocks.pop_front() else {
                break;
            };
            current_bytes -= dropped.data.len() as u64;
            remove_block_file(dir, dropped.index);
            log::warn!(
                "esmagent: persistent queue: dropped oldest block {} on open (over max_bytes={max_bytes})",
                dropped.index
            );
        }

        Ok(PersistentQueue {
            dir: dir.to_path_buf(),
            max_bytes,
            inner: Mutex::new(Inner {
                blocks,
                current_bytes,
                next_index,
                closed: false,
            }),
            not_empty: Condvar::new(),
        })
    }

    /// Durably appends `block` to the tail of the queue: the block is
    /// written to disk (temp-file-then-rename, fsynced) before this
    /// returns `Ok`, so it survives a crash immediately after `push`
    /// returns, with no need to call `flush_to_disk` first.
    ///
    /// If the queue's total queued bytes would exceed `max_bytes` after
    /// adding `block`, the oldest queued block(s) are dropped (from memory
    /// and disk) first, until it fits or the queue is empty. Wakes one
    /// blocked `pop`, if any.
    ///
    /// Returns `Err` only on a disk write failure (e.g. disk full);
    /// nothing about the queue's state is mutated in that case.
    pub fn push(&self, block: Vec<u8>) -> Result<(), QueueError> {
        let block_len = block.len() as u64;
        let mut inner = self.inner.lock().unwrap();

        // Write first: if this fails, leave the queue's state untouched
        // rather than evicting old blocks to make room for data that never
        // made it to disk.
        let index = inner.next_index;
        write_block_file(&self.dir, index, &block)?;
        inner.next_index += 1;

        while inner.current_bytes + block_len > self.max_bytes {
            let Some(dropped) = inner.blocks.pop_front() else {
                break;
            };
            inner.current_bytes -= dropped.data.len() as u64;
            remove_block_file(&self.dir, dropped.index);
            log::warn!(
                "esmagent: persistent queue: dropped oldest block {} ({} bytes) over max_bytes={}",
                dropped.index,
                dropped.data.len(),
                self.max_bytes
            );
        }

        inner.current_bytes += block_len;
        inner.blocks.push_back(QueuedBlock { index, data: block });
        drop(inner);
        self.not_empty.notify_one();
        Ok(())
    }

    /// Removes and returns the oldest queued block, durably (the block's
    /// on-disk file is deleted before returning). Blocks up to `timeout`
    /// waiting for a block to arrive if the queue is currently empty;
    /// returns `None` on timeout or once the queue has been [`close`]d
    /// with nothing left to pop.
    ///
    /// [`close`]: PersistentQueue::close
    pub fn pop(&self, timeout: Duration) -> Option<Vec<u8>> {
        let deadline = Instant::now() + timeout;
        let mut inner = self.inner.lock().unwrap();
        loop {
            if let Some(block) = inner.blocks.pop_front() {
                inner.current_bytes -= block.data.len() as u64;
                remove_block_file(&self.dir, block.index);
                return Some(block.data);
            }
            if inner.closed {
                return None;
            }
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            // Recompute the remaining wait against the fixed deadline on
            // every loop iteration so a spurious wakeup (permitted by the
            // std::sync::Condvar contract) can't shorten or lengthen the
            // effective timeout.
            let (guard, _) = self.not_empty.wait_timeout(inner, deadline - now).unwrap();
            inner = guard;
        }
    }

    /// Current sum of queued block byte lengths.
    pub fn pending_bytes(&self) -> u64 {
        self.inner.lock().unwrap().current_bytes
    }

    /// Fsyncs the queue directory so that block files' `rename`s (already
    /// individually fsynced in `push`) are durable across a power loss.
    /// Best-effort: opening a directory as a file isn't portable, so this
    /// is a silent no-op on platforms/filesystems where it fails (notably
    /// Windows). There is no per-block content to flush here — see the
    /// "Write-through, not deferred spill" section of the module doc.
    pub fn flush_to_disk(&self) {
        if let Ok(dir_file) = fs::File::open(&self.dir) {
            let _ = dir_file.sync_all();
        }
    }

    /// Marks the queue closed (waking any blocked `pop` so it returns
    /// `None` instead of waiting out its full timeout) and flushes the
    /// directory. All blocks pushed so far are already durably on disk
    /// (write-through), so nothing else needs to happen before `self` is
    /// dropped.
    pub fn close(self) {
        self.signal_closed();
        self.flush_to_disk();
    }

    /// Sets the `closed` flag and wakes every blocked `pop` so it re-checks,
    /// sees `closed`, and returns `None` immediately instead of waiting out
    /// its timeout. Split out from [`close`] (which consumes `self`, so its
    /// wake path can't be driven while another thread holds a live
    /// `&PersistentQueue` — e.g. an `Arc` clone — blocked in `pop`) so this
    /// exact mechanism is reachable through a shared `&self` and can be
    /// exercised end-to-end by a test.
    ///
    /// [`close`]: PersistentQueue::close
    fn signal_closed(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.closed = true;
        drop(inner);
        self.not_empty.notify_all();
    }
}

/// Parses a block filename back into its index: exactly [`INDEX_WIDTH`]
/// ASCII digits, zero-padded. Anything else (including the `.tmp` staging
/// files `write_block_file` creates) is not a valid block filename.
fn parse_index(name: &str) -> Option<u64> {
    if name.len() != INDEX_WIDTH || !name.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    name.parse().ok()
}

fn block_path(dir: &Path, index: u64) -> PathBuf {
    dir.join(format!("{index:0width$}", width = INDEX_WIDTH))
}

/// Durably writes `data` as block `index` under `dir`: write + fsync a temp
/// file, then rename it into place. The rename is atomic and `index` is
/// never reused, so a concurrent reader (a fresh `open` after a crash) can
/// never observe a partially written block. On failure, best-effort removes
/// the temp file so it doesn't linger until the next `open`'s sweep.
fn write_block_file(dir: &Path, index: u64, data: &[u8]) -> std::io::Result<()> {
    let tmp_path = dir.join(format!("{index:0width$}.tmp", width = INDEX_WIDTH));
    let result = (|| {
        let mut f = fs::File::create(&tmp_path)?;
        f.write_all(data)?;
        f.sync_all()
    })();
    if let Err(e) = result {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }
    fs::rename(&tmp_path, block_path(dir, index))
}

/// Best-effort removal of block `index`'s file. Missing is not an error (a
/// double-remove or a file that was never written can't corrupt queue
/// state, which lives in `Inner`, not on disk); any other error is logged,
/// never propagated — a disk error here must not crash the process.
fn remove_block_file(dir: &Path, index: u64) {
    let path = block_path(dir, index);
    if let Err(e) = fs::remove_file(&path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            log::warn!(
                "esmagent: persistent queue: failed to remove block file {}: {e}",
                path.display()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn durable_fifo_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let q = PersistentQueue::open(dir.path(), 10_000_000).unwrap();
            q.push(b"block1".to_vec()).unwrap();
            q.push(b"block2".to_vec()).unwrap();
            q.flush_to_disk();
            q.close();
        }
        let q = PersistentQueue::open(dir.path(), 10_000_000).unwrap();
        assert_eq!(
            q.pop(Duration::from_secs(1)).as_deref(),
            Some(&b"block1"[..])
        );
        assert_eq!(
            q.pop(Duration::from_secs(1)).as_deref(),
            Some(&b"block2"[..])
        );
        assert!(q.pop(Duration::from_millis(50)).is_none());
    }

    #[test]
    fn zero_max_bytes_means_unlimited_not_a_zero_byte_cap() {
        // Regression: `-remoteWrite.maxDiskUsagePerURL`'s documented default
        // is "0 = unlimited" (flags.rs), which flows straight through to
        // `open`'s `max_bytes` with no translation at the call site. Every
        // push must be retained (nothing evicted) when `max_bytes == 0` —
        // treating it as a literal 0-byte cap would drop-evict almost every
        // block on almost every push (surfaced by esmagent's e2e test).
        let dir = tempfile::tempdir().unwrap();
        let q = PersistentQueue::open(dir.path(), 0).unwrap();
        q.push(b"aaaaa".to_vec()).unwrap();
        q.push(b"bbbbb".to_vec()).unwrap();
        q.push(b"ccccc".to_vec()).unwrap();
        assert_eq!(q.pending_bytes(), 15, "no block should have been evicted");
        assert_eq!(
            q.pop(Duration::from_millis(50)).as_deref(),
            Some(&b"aaaaa"[..])
        );
        assert_eq!(
            q.pop(Duration::from_millis(50)).as_deref(),
            Some(&b"bbbbb"[..])
        );
        assert_eq!(
            q.pop(Duration::from_millis(50)).as_deref(),
            Some(&b"ccccc"[..])
        );
    }

    #[test]
    fn drops_oldest_when_over_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let q = PersistentQueue::open(dir.path(), 10).unwrap(); // 10-byte cap
        q.push(b"aaaaa".to_vec()).unwrap(); // 5
        q.push(b"bbbbb".to_vec()).unwrap(); // 10 total
        q.push(b"ccccc".to_vec()).unwrap(); // would be 15 -> drop oldest "aaaaa"
        assert_eq!(
            q.pop(Duration::from_millis(50)).as_deref(),
            Some(&b"bbbbb"[..])
        );
        assert_eq!(
            q.pop(Duration::from_millis(50)).as_deref(),
            Some(&b"ccccc"[..])
        );
    }

    #[test]
    fn pop_blocks_then_wakes_on_push() {
        let dir = tempfile::tempdir().unwrap();
        let q = PersistentQueue::open(dir.path(), 10_000_000).unwrap();
        std::thread::scope(|s| {
            let handle = s.spawn(|| q.pop(Duration::from_secs(1)));
            std::thread::sleep(Duration::from_millis(100));
            q.push(b"woken".to_vec()).unwrap();
            let got = handle.join().unwrap();
            assert_eq!(got.as_deref(), Some(&b"woken"[..]));
        });
    }

    #[test]
    fn open_on_fresh_dir_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let fresh = dir.path().join("does-not-exist-yet");
        let q = PersistentQueue::open(&fresh, 1_000).unwrap();
        assert_eq!(q.pending_bytes(), 0);
        assert!(q.pop(Duration::from_millis(50)).is_none());
    }

    // A popper blocked on the empty-queue Condvar must be woken by close so
    // it returns `None` promptly, not left parked until its own timeout.
    // `close(self)` consumes the queue and so can't be called while a shared
    // `Arc` clone sits blocked in `pop`, so this drives the exact mechanism
    // `close` uses — `signal_closed` (sets `closed` + `notify_all`) — through
    // an `Arc`, then asserts the popper returns well under its 5s timeout.
    #[test]
    fn close_wakes_blocked_pop() {
        use std::sync::Arc;
        use std::time::Instant;

        let dir = tempfile::tempdir().unwrap();
        let q = Arc::new(PersistentQueue::open(dir.path(), 10_000_000).unwrap());
        let popper = Arc::clone(&q);
        let started = Instant::now();
        let handle = std::thread::spawn(move || popper.pop(Duration::from_secs(5)));

        // Give the popper time to actually park on the Condvar.
        std::thread::sleep(Duration::from_millis(100));
        q.signal_closed();

        let got = handle.join().unwrap();
        assert!(
            got.is_none(),
            "queue is empty + closed, pop must yield None"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "blocked pop was not woken by close (took {:?}, near its 5s timeout)",
            started.elapsed()
        );
    }
}
