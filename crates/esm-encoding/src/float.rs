//! Pooled float64 buffers. Port of Go lib/encoding/float.go.
//!
//! Deviation: Go's sync.Pool returns slices with garbage contents; the Rust
//! version zero-fills, and the pool is thread-local.

use std::cell::RefCell;

const MAX_POOLED_BUFFERS: usize = 8;

thread_local! {
    static FLOAT64S_POOL: RefCell<Vec<Vec<f64>>> = const { RefCell::new(Vec::new()) };
}

/// Returns a zero-filled `Vec<f64>` of the given `size` from a thread-local
/// pool. Go: GetFloat64s.
///
/// When the returned buffer is no longer needed, it is advised to call
/// [`put_float64s`] on it, so it can be reused.
pub fn get_float64s(size: usize) -> Vec<f64> {
    let mut a = FLOAT64S_POOL
        .with(|pool| pool.borrow_mut().pop())
        .unwrap_or_default();
    a.clear();
    a.resize(size, 0.0);
    a
}

/// Returns `a` to the pool, so it can be reused via [`get_float64s`].
/// Go: PutFloat64s.
pub fn put_float64s(mut a: Vec<f64>) {
    a.clear();
    FLOAT64S_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.len() < MAX_POOLED_BUFFERS {
            pool.push(a);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_put_float64s() {
        let a = get_float64s(10);
        assert_eq!(a.len(), 10);
        assert!(a.iter().all(|&v| v == 0.0));
        put_float64s(a);

        // The pooled buffer must come back zero-filled at the requested size.
        let b = get_float64s(4);
        assert_eq!(b.len(), 4);
        assert!(b.iter().all(|&v| v == 0.0));
        put_float64s(b);
    }
}
