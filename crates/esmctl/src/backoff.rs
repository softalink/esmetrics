//! Retry with exponential backoff. Ports `app/vmctl/backoff/backoff.go`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Error returned by a retryable operation.
pub(crate) struct RetryError {
    pub(crate) msg: String,
    /// When true, the error is unrecoverable (bad request / cancellation) and
    /// retries stop immediately. Ports the `ErrBadRequest`/`context.Canceled`
    /// fast-fail.
    pub(crate) fatal: bool,
}

impl RetryError {
    pub(crate) fn retryable(msg: impl Into<String>) -> RetryError {
        RetryError {
            msg: msg.into(),
            fatal: false,
        }
    }
    /// Marks an error as unrecoverable (skips retries). Reserved for the
    /// bad-request/cancellation fast-fail path (not yet wired into the native
    /// client's 4xx handling; exercised by the backoff tests).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn fatal(msg: impl Into<String>) -> RetryError {
        RetryError {
            msg: msg.into(),
            fatal: true,
        }
    }
}

/// Exponential backoff policy. Ports `backoff.Backoff`.
pub(crate) struct Backoff {
    retries: u32,
    factor: f64,
    min_duration: Duration,
}

impl Backoff {
    /// Ports `backoff.New`.
    pub(crate) fn new(
        retries: i64,
        factor: f64,
        min_duration: Duration,
    ) -> Result<Backoff, String> {
        if retries <= 0 {
            return Err("number of backoff retries must be greater than 0".to_string());
        }
        if factor <= 1.0 {
            return Err("backoff retry factor must be greater than 1".to_string());
        }
        if min_duration.is_zero() {
            return Err("backoff retry minimum duration must be greater than 0".to_string());
        }
        Ok(Backoff {
            retries: retries as u32,
            factor,
            min_duration,
        })
    }

    /// Runs `cb`, retrying on non-fatal errors up to `retries` times with an
    /// exponentially growing delay. Returns `(attempts, result)`. Ports
    /// `Backoff.Retry`. `cancel` aborts the wait between attempts.
    pub(crate) fn retry<F>(&self, cancel: &AtomicBool, mut cb: F) -> (u64, Result<(), String>)
    where
        F: FnMut() -> Result<(), RetryError>,
    {
        let mut attempt: u64 = 0;
        let mut last_err = String::new();
        for i in 0..self.retries {
            match cb() {
                Ok(()) => return (attempt, Ok(())),
                Err(e) if e.fatal => return (attempt, Err(e.msg)),
                Err(e) => {
                    attempt += 1;
                    last_err = e.msg;
                    let backoff = self.min_duration.as_secs_f64() * self.factor.powi(i as i32);
                    let dur = Duration::from_secs_f64(backoff);
                    log::error!(
                        "got error: {last_err} on attempt: {attempt}; will retry in {dur:?}"
                    );
                    if !sleep_or_cancel(dur, cancel) {
                        return (attempt, Err(last_err));
                    }
                }
            }
        }
        let _ = last_err;
        (
            attempt,
            Err(format!(
                "execution failed after {} retry attempts",
                self.retries
            )),
        )
    }
}

/// Sleeps for `dur`, returning `false` early if `cancel` is set.
fn sleep_or_cancel(dur: Duration, cancel: &AtomicBool) -> bool {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        if cancel.load(Ordering::SeqCst) {
            return false;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        std::thread::sleep(remaining.min(Duration::from_millis(100)));
    }
    !cancel.load(Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_params() {
        assert!(Backoff::new(0, 2.0, Duration::from_secs(1)).is_err());
        assert!(Backoff::new(3, 1.0, Duration::from_secs(1)).is_err());
        assert!(Backoff::new(3, 2.0, Duration::ZERO).is_err());
    }

    #[test]
    fn succeeds_first_try() {
        let b = Backoff::new(3, 2.0, Duration::from_millis(1)).unwrap();
        let cancel = AtomicBool::new(false);
        let (attempts, res) = b.retry(&cancel, || Ok(()));
        assert_eq!(attempts, 0);
        assert!(res.is_ok());
    }

    #[test]
    fn stops_on_fatal() {
        let b = Backoff::new(3, 2.0, Duration::from_millis(1)).unwrap();
        let cancel = AtomicBool::new(false);
        let (attempts, res) = b.retry(&cancel, || Err(RetryError::fatal("bad request")));
        assert_eq!(attempts, 0);
        assert!(res.is_err());
    }

    #[test]
    fn retries_then_gives_up() {
        let b = Backoff::new(2, 2.0, Duration::from_millis(1)).unwrap();
        let cancel = AtomicBool::new(false);
        let mut calls = 0;
        let (_attempts, res) = b.retry(&cancel, || {
            calls += 1;
            Err(RetryError::retryable("boom"))
        });
        assert_eq!(calls, 2);
        assert!(res.is_err());
    }
}
