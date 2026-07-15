//! Shared per-thread conversion buffers for the protocol insert handlers —
//! the Rust analogue of Go's pooled `common.InsertCtx`/`pushCtx` (the server
//! is thread-per-connection, so a thread-local replaces Go's `sync.Pool`).
//!
//! Every handler follows the same shape: marshal each row's
//! `MetricNameRaw` bytes into a reused byte [`arena`](ConvertCtx::arena),
//! record an [`Entry`] per data point pointing into the arena, then
//! [`flush_to`](ConvertCtx::flush_to) materializes the borrowed
//! [`MetricRow`] batch, hands it to the [`RowSink`] and recycles the batch
//! allocation. Steady-state, a request allocates nothing once the buffers
//! have grown.
//!
//! What goes INTO the arena — label order, group-first vs group-last
//! layout, extra labels — is protocol-specific and stays in the handlers;
//! this module only owns the buffer/flush/recycle mechanics (which were
//! previously copy-pasted, byte-identical, across all ten handlers).

use std::cell::RefCell;

use crate::{MetricRow, RowSink};

/// One converted data point: an `(offset, len)` window into
/// [`ConvertCtx::arena`] holding the row's MetricNameRaw bytes, plus its
/// timestamp (ms) and value.
pub(crate) struct Entry {
    pub(crate) offset: usize,
    pub(crate) len: usize,
    pub(crate) timestamp: i64,
    pub(crate) value: f64,
}

/// Reused conversion buffers; the analogue of Go's pooled `pushCtx`.
#[derive(Default)]
pub(crate) struct ConvertCtx {
    /// Arena all metric_name_raw bytes are appended to.
    /// Go: `pushCtx.metricNameBuf` + `InsertCtx.metricNameBuf`.
    pub(crate) arena: Vec<u8>,
    /// (offset, len) into `arena` plus timestamp/value, per data point.
    pub(crate) entries: Vec<Entry>,
    /// Recycled allocation for the `&[MetricRow]` batch handed to the sink.
    /// Always empty between calls; only its capacity is retained.
    rows: Vec<MetricRow<'static>>,
}

impl ConvertCtx {
    /// Clears the arena and entries for a fresh conversion batch. The
    /// buffer capacities (and the recycled rows allocation) are retained.
    pub(crate) fn begin(&mut self) {
        self.arena.clear();
        self.entries.clear();
    }

    /// Materializes the recorded entries as a borrowed [`MetricRow`] batch,
    /// pushes it to `sink` and recycles the batch allocation.
    pub(crate) fn flush_to<S: RowSink>(&mut self, sink: &S) -> Result<(), String> {
        // Materialize the borrowed batch. `Vec<MetricRow<'static>>` coerces
        // to `Vec<MetricRow<'_>>` (covariance); the reverse recycling is
        // below.
        let mut batch: Vec<MetricRow<'_>> = std::mem::take(&mut self.rows);
        let arena = &self.arena;
        for e in &self.entries {
            // A zero-length metric_name_raw window is the marshaled form of an
            // all-empty-label series (every label had an empty value and the
            // metric group is empty, so `marshal_metric_name_raw` emitted
            // nothing). Go's `InsertCtx.TryPrepareLabels` early-skips such a
            // series (`len(ctx.Labels) == 0`) from every `insertRows` path, so
            // it is silently dropped rather than stored. This is the shared
            // chokepoint every handler funnels through; the rows-inserted
            // counters are incremented before flush (matching Go's `rowsTotal`,
            // which counts skipped samples too), so only storage is skipped.
            if e.len == 0 {
                continue;
            }
            batch.push(MetricRow {
                metric_name_raw: &arena[e.offset..e.offset + e.len],
                timestamp: e.timestamp,
                value: e.value,
            });
        }
        let result = sink.add_rows(&batch);
        self.rows = recycle_rows(batch);
        result
    }

    /// The retained capacity of the recycled rows buffer (test aid for the
    /// per-handler `buffers_are_reused_across_batches` assertions).
    #[cfg(test)]
    pub(crate) fn rows_capacity(&self) -> usize {
        self.rows.capacity()
    }
}

/// Runs `f` with this thread's shared [`ConvertCtx`]. All handlers use the
/// same thread-local: conversions never nest (a handler converts and
/// flushes before returning; sinks never call back into a handler), so one
/// context per thread suffices.
pub(crate) fn with_ctx<R>(f: impl FnOnce(&mut ConvertCtx) -> R) -> R {
    thread_local! {
        static CONVERT_CTX: RefCell<ConvertCtx> = RefCell::new(ConvertCtx::default());
    }
    CONVERT_CTX.with(|cell| f(&mut cell.borrow_mut()))
}

/// Reclaims the allocation of a drained batch Vec so its capacity can be
/// reused for rows borrowing a different (future) arena state.
fn recycle_rows(mut batch: Vec<MetricRow<'_>>) -> Vec<MetricRow<'static>> {
    batch.clear();
    let mut batch = std::mem::ManuallyDrop::new(batch);
    let ptr = batch.as_mut_ptr();
    let cap = batch.capacity();
    // SAFETY: `batch` is empty and its buffer is owned (taken out of
    // ManuallyDrop, so it is not freed twice). `MetricRow<'a>` and
    // `MetricRow<'static>` differ only in a lifetime parameter, so they have
    // identical size and alignment, and with len 0 no value carrying the old
    // lifetime can ever be read from the recycled vector.
    unsafe { Vec::from_raw_parts(ptr.cast::<MetricRow<'static>>(), 0, cap) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct CollectSink {
        rows: Mutex<Vec<(Vec<u8>, i64, f64)>>,
    }

    impl RowSink for CollectSink {
        fn add_rows(&self, rows: &[MetricRow<'_>]) -> Result<(), String> {
            let mut got = self.rows.lock().unwrap();
            for r in rows {
                got.push((r.metric_name_raw.to_vec(), r.timestamp, r.value));
            }
            Ok(())
        }
    }

    #[test]
    fn flush_skips_degenerate_zero_length_entries() {
        // A zero-length metric_name_raw window is the marshaled form of an
        // all-empty-label series (every label has an empty value and the
        // metric group is empty). Go's InsertCtx.TryPrepareLabels early-skips
        // such a series (`len(ctx.Labels) == 0`), so it must never be stored.
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();

        ctx.begin();
        ctx.arena.extend_from_slice(b"ok");
        // Real series.
        ctx.entries.push(Entry {
            offset: 0,
            len: 2,
            timestamp: 1,
            value: 1.0,
        });
        // Degenerate all-empty-label series: zero-length window.
        ctx.entries.push(Entry {
            offset: 2,
            len: 0,
            timestamp: 2,
            value: 2.0,
        });
        ctx.flush_to(&sink).unwrap();

        assert_eq!(
            *sink.rows.lock().unwrap(),
            vec![(b"ok".to_vec(), 1, 1.0)],
            "zero-length (all-empty-label) series must be skipped, not stored"
        );
    }

    #[test]
    fn flush_materializes_entries_and_recycles_the_batch() {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();

        ctx.begin();
        ctx.arena.extend_from_slice(b"aaaa");
        ctx.arena.extend_from_slice(b"bb");
        ctx.entries.push(Entry {
            offset: 0,
            len: 4,
            timestamp: 1,
            value: 1.5,
        });
        ctx.entries.push(Entry {
            offset: 4,
            len: 2,
            timestamp: 2,
            value: 2.5,
        });
        ctx.flush_to(&sink).unwrap();

        assert_eq!(
            *sink.rows.lock().unwrap(),
            vec![(b"aaaa".to_vec(), 1, 1.5), (b"bb".to_vec(), 2, 2.5)]
        );

        // The batch allocation is retained across flushes.
        let cap = ctx.rows_capacity();
        assert!(cap >= 2);
        ctx.begin();
        ctx.arena.extend_from_slice(b"c");
        ctx.entries.push(Entry {
            offset: 0,
            len: 1,
            timestamp: 3,
            value: 3.0,
        });
        ctx.flush_to(&sink).unwrap();
        assert_eq!(ctx.rows_capacity(), cap, "rows batch must be recycled");
    }

    #[test]
    fn sink_error_is_propagated() {
        struct FailSink;
        impl RowSink for FailSink {
            fn add_rows(&self, _rows: &[MetricRow<'_>]) -> Result<(), String> {
                Err("storage full".to_owned())
            }
        }
        let mut ctx = ConvertCtx::default();
        ctx.begin();
        ctx.arena.extend_from_slice(b"x");
        ctx.entries.push(Entry {
            offset: 0,
            len: 1,
            timestamp: 1,
            value: 1.0,
        });
        assert_eq!(ctx.flush_to(&FailSink).unwrap_err(), "storage full");
        // The rows buffer must have been recycled even on error.
        assert!(ctx.rows_capacity() >= 1);
    }

    #[test]
    fn with_ctx_reuses_the_thread_local() {
        let cap0 = with_ctx(|ctx| {
            ctx.begin();
            ctx.arena.extend_from_slice(b"hello");
            ctx.arena.capacity()
        });
        let cap1 = with_ctx(|ctx| ctx.arena.capacity());
        assert_eq!(cap0, cap1);
    }
}
