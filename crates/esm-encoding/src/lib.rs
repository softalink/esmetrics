//! Port of the upstream VictoriaMetrics v1.146.0 `lib/encoding` (Go) — encoding/decoding of
//! TSDB timestamp and value blocks.
//!
//! Modules mirror the Go source files:
//! - [`int`] — int.go
//! - [`nearest_delta`] — nearest_delta.go
//! - [`nearest_delta2`] — nearest_delta2.go
//! - [`encoding`] — encoding.go
//! - [`compress`] — compress.go + zstd/ (plus util.go `IsZstd`)
//! - [`float`] — float.go

mod compress;
mod encoding;
mod float;
mod int;
mod nearest_delta;
mod nearest_delta2;

pub use compress::*;
pub use encoding::*;
pub use float::*;
pub use int::*;

use std::cell::RefCell;

/// Runs `f` with a zero-filled thread-local `Vec<i64>` scratch buffer of length `len`.
///
/// Replaces Go's `GetInt64s`/`PutInt64s` sync.Pool so the hot marshal/unmarshal
/// paths perform no per-call allocations once the buffer is warm.
pub(crate) fn with_int64_scratch<R>(len: usize, f: impl FnOnce(&mut Vec<i64>) -> R) -> R {
    thread_local! {
        static SCRATCH: RefCell<Vec<i64>> = const { RefCell::new(Vec::new()) };
    }
    SCRATCH.with(|cell| {
        let mut v = cell.borrow_mut();
        v.clear();
        v.resize(len, 0);
        f(&mut v)
    })
}

#[cfg(test)]
mod testutil;
