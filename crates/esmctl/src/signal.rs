//! Ctrl-C / termination handling for the one-shot migration commands.
//!
//! Ports the `signal.Notify(SIGINT, SIGTERM)` goroutine in `app/vmctl/main.go`:
//! the handler flips a process-global cancel flag, which the migration loops
//! and the [`crate::backoff`] retry waits poll to stop promptly instead of
//! being killed mid-request. A `&'static AtomicBool` view of the flag is
//! handed to code that already speaks `&AtomicBool` (the backoff and native
//! workers).
//!
//! `libc::signal` is used for both Unix and Windows: the mingw/MSVCRT C
//! runtime implements `SIGINT` for console Ctrl-C, so no platform-specific
//! console API is needed. The handler does only an atomic store, which is
//! async-signal-safe.

use std::sync::atomic::{AtomicBool, Ordering};

static CANCEL: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_sig: libc::c_int) {
    CANCEL.store(true, Ordering::SeqCst);
}

/// Installs the Ctrl-C (and, on Unix, SIGTERM) handler.
pub(crate) fn install() {
    let handler: extern "C" fn(libc::c_int) = on_signal;
    // SAFETY: `on_signal` only performs a single atomic store, which is
    // async-signal-safe; registering it cannot fail in a way we can recover
    // from, so the return value is ignored as upstream does.
    unsafe {
        libc::signal(libc::SIGINT, handler as libc::sighandler_t);
        #[cfg(unix)]
        libc::signal(libc::SIGTERM, handler as libc::sighandler_t);
    }
}

/// A static view of the cancel flag, for passing where an `&AtomicBool` is
/// expected (e.g. `Backoff::retry`).
pub(crate) fn cancel_flag() -> &'static AtomicBool {
    &CANCEL
}

/// Returns true once a Ctrl-C / termination signal has been received.
pub(crate) fn is_cancelled() -> bool {
    CANCEL.load(Ordering::SeqCst)
}
