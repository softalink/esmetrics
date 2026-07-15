//! Shutdown-signal handling without external runtime crates
//! (mirrors upstream `lib/procutil.WaitForSigterm`).
//!
//! - **Unix**: `libc::sigaction` handlers for SIGINT/SIGTERM store the signal
//!   number in an atomic — the only async-signal-safe action available
//!   (`pthread_cond_signal` is not async-signal-safe, so the handler cannot
//!   notify the condvar). The waiter polls the atomic via
//!   `Condvar::wait_timeout`.
//! - **Windows**: `SetConsoleCtrlHandler`; the handler runs on its own thread,
//!   so it stores the event *and* notifies the condvar for an immediate wake.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

/// Raw signal value as stored by the platform handler; 0 means "none yet".
static RECEIVED: AtomicI32 = AtomicI32::new(0);
static WAKE_LOCK: Mutex<()> = Mutex::new(());
static WAKE: Condvar = Condvar::new();
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Installs the platform handlers and blocks until SIGINT/SIGTERM
/// (unix) or a console ctrl event (windows) arrives. Returns the
/// human-readable signal name for logging.
pub fn wait_for_shutdown_signal() -> &'static str {
    imp::install();
    let mut guard = WAKE_LOCK.lock().unwrap();
    loop {
        if let Some(name) = imp::name(RECEIVED.load(Ordering::Acquire)) {
            return name;
        }
        let (g, _) = WAKE.wait_timeout(guard, POLL_INTERVAL).unwrap();
        guard = g;
    }
}

#[cfg(unix)]
mod imp {
    use super::RECEIVED;
    use std::sync::atomic::Ordering;

    extern "C" fn on_signal(sig: libc::c_int) {
        // Only async-signal-safe work here: a plain atomic store.
        RECEIVED.store(sig, Ordering::Release);
    }

    pub(super) fn install() {
        let handler: extern "C" fn(libc::c_int) = on_signal;
        // SAFETY: `sa` is a valid, fully initialized sigaction whose handler
        // only performs an atomic store (async-signal-safe).
        unsafe {
            let mut sa: libc::sigaction = std::mem::zeroed();
            sa.sa_sigaction = handler as usize;
            libc::sigemptyset(&mut sa.sa_mask);
            sa.sa_flags = libc::SA_RESTART;
            for sig in [libc::SIGINT, libc::SIGTERM] {
                if libc::sigaction(sig, &sa, std::ptr::null_mut()) != 0 {
                    panic!(
                        "FATAL: cannot install handler for signal {sig}: {}",
                        std::io::Error::last_os_error()
                    );
                }
            }
        }
    }

    pub(super) fn name(raw: i32) -> Option<&'static str> {
        match raw {
            0 => None,
            libc::SIGINT => Some("SIGINT"),
            libc::SIGTERM => Some("SIGTERM"),
            _ => Some("signal"),
        }
    }
}

#[cfg(windows)]
mod imp {
    use super::{RECEIVED, WAKE};
    use std::sync::atomic::Ordering;
    use windows_sys::Win32::Foundation::{BOOL, TRUE};
    use windows_sys::Win32::System::Console::{
        SetConsoleCtrlHandler, CTRL_BREAK_EVENT, CTRL_CLOSE_EVENT, CTRL_C_EVENT,
    };

    // Ctrl events are small integers starting at 0 (CTRL_C_EVENT == 0), so
    // they are stored offset by 1 to keep 0 meaning "no signal yet".
    unsafe extern "system" fn on_ctrl_event(ctrl_type: u32) -> BOOL {
        RECEIVED.store(ctrl_type as i32 + 1, Ordering::Release);
        // This handler runs on a dedicated thread, so notifying is safe.
        WAKE.notify_all();
        TRUE
    }

    pub(super) fn install() {
        // SAFETY: registering a valid handler routine.
        if unsafe { SetConsoleCtrlHandler(Some(on_ctrl_event), TRUE) } == 0 {
            panic!(
                "FATAL: cannot install console ctrl handler: {}",
                std::io::Error::last_os_error()
            );
        }
    }

    pub(super) fn name(raw: i32) -> Option<&'static str> {
        if raw == 0 {
            return None;
        }
        match (raw - 1) as u32 {
            CTRL_C_EVENT => Some("CTRL_C_EVENT"),
            CTRL_BREAK_EVENT => Some("CTRL_BREAK_EVENT"),
            CTRL_CLOSE_EVENT => Some("CTRL_CLOSE_EVENT"),
            _ => Some("console ctrl event"),
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::time::Duration;

    #[test]
    fn wait_returns_signal_name_after_sigterm() {
        // Install first so the raised SIGTERM cannot hit the default action.
        super::imp::install();
        std::thread::spawn(|| {
            std::thread::sleep(Duration::from_millis(50));
            // SAFETY: raise() delivers the signal to this thread; the
            // installed handler only stores an atomic.
            unsafe { libc::raise(libc::SIGTERM) };
        });
        assert_eq!(super::wait_for_shutdown_signal(), "SIGTERM");
    }
}
