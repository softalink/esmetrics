//! Shutdown- and reload-signal handling without external runtime crates
//! (mirrors upstream `lib/procutil.WaitForSigterm` plus vmagent/vmalert's
//! SIGHUP config-reload trigger). Ported from `esmauth::signal`/
//! `esmalert::signal` (`crates/esmauth/src/signal.rs`,
//! `crates/esmalert/src/signal.rs`) — the mechanism is process-generic, not
//! specific to any one binary. Originally `esmagent` had no config-reload
//! trigger and this module only exposed shutdown-signal waiting; Task 10
//! (`-promscrape.config` CLI + main wiring) adds the SIGHUP "reload
//! pending" plumbing (mirroring `esmalert::signal`) plus [`wait_for_event`],
//! which `main`'s loop uses to block on "shutdown OR reload OR
//! `-promscrape.configCheckInterval` elapsed" in one call.
//!
//! - **Unix**: `libc::sigaction` handlers for SIGINT/SIGTERM store the signal
//!   number in an atomic — the only async-signal-safe action available
//!   (`pthread_cond_signal` is not async-signal-safe, so the handler cannot
//!   notify the condvar). SIGHUP instead sets a separate "reload pending"
//!   atomic flag, polled via [`take_reload_request`]. The shutdown waiter
//!   polls the shutdown atomic via `Condvar::wait_timeout`.
//! - **Windows**: `SetConsoleCtrlHandler`; the handler runs on its own thread,
//!   so it stores the event *and* notifies the condvar for an immediate wake.
//!   Windows has no SIGHUP, so [`take_reload_request`] is always `false`
//!   there and config reload is interval-only
//!   (`-promscrape.configCheckInterval`).

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Condvar, Mutex, Once};
use std::time::{Duration, Instant};

/// Raw shutdown-signal value as stored by the platform handler; 0 = none yet.
static RECEIVED: AtomicI32 = AtomicI32::new(0);
/// Set by the SIGHUP handler (unix only); cleared by [`take_reload_request`].
static RELOAD_PENDING: AtomicBool = AtomicBool::new(false);
static WAKE_LOCK: Mutex<()> = Mutex::new(());
static WAKE: Condvar = Condvar::new();
static INSTALL: Once = Once::new();
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Installs the platform signal handlers exactly once. Safe to call multiple
/// times (subsequent calls are no-ops).
pub fn install() {
    INSTALL.call_once(imp::install);
}

/// Installs the handlers and blocks until SIGINT/SIGTERM (unix) or a console
/// ctrl event (windows) arrives. SIGHUP does NOT wake this — it only sets the
/// reload flag (see [`wait_for_event`] for a wait that also observes SIGHUP).
/// Returns the human-readable signal name for logging.
pub fn wait_for_shutdown_signal() -> &'static str {
    install();
    let mut guard = WAKE_LOCK.lock().unwrap();
    loop {
        if let Some(name) = imp::name(RECEIVED.load(Ordering::Acquire)) {
            return name;
        }
        let (g, _) = WAKE.wait_timeout(guard, POLL_INTERVAL).unwrap();
        guard = g;
    }
}

/// Returns `true` (clearing the flag) if a config reload was requested via
/// SIGHUP since the last call. Always `false` on Windows (no SIGHUP).
pub fn take_reload_request() -> bool {
    RELOAD_PENDING.swap(false, Ordering::AcqRel)
}

/// The three things `main`'s event loop cares about: a shutdown signal (with
/// its name, for logging), a SIGHUP reload request, or the `timeout` passed
/// to [`wait_for_event`] elapsing with neither having happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    Shutdown(&'static str),
    Reload,
    Timeout,
}

/// Blocks until a shutdown signal arrives, a SIGHUP reload is requested, or
/// `timeout` elapses (`None` waits indefinitely — used when
/// `-promscrape.configCheckInterval` is `0`, i.e. reload is SIGHUP-only).
/// Installs the signal handlers if they aren't already (safe/idempotent, see
/// [`install`]). Polls both flags every [`POLL_INTERVAL`] so a `timeout`
/// shorter than that still returns reasonably promptly via the deadline
/// check below, and a signal arriving mid-wait is observed within one poll
/// tick, matching [`wait_for_shutdown_signal`]'s existing responsiveness.
pub fn wait_for_event(timeout: Option<Duration>) -> Event {
    install();
    let deadline = timeout.map(|d| Instant::now() + d);
    let mut guard = WAKE_LOCK.lock().unwrap();
    loop {
        if let Some(name) = imp::name(RECEIVED.load(Ordering::Acquire)) {
            return Event::Shutdown(name);
        }
        if take_reload_request() {
            return Event::Reload;
        }
        let wait_dur = match deadline {
            Some(dl) => {
                let now = Instant::now();
                if now >= dl {
                    return Event::Timeout;
                }
                (dl - now).min(POLL_INTERVAL)
            }
            None => POLL_INTERVAL,
        };
        let (g, _) = WAKE.wait_timeout(guard, wait_dur).unwrap();
        guard = g;
    }
}

#[cfg(unix)]
mod imp {
    use super::{RECEIVED, RELOAD_PENDING};
    use std::sync::atomic::Ordering;

    extern "C" fn on_signal(sig: libc::c_int) {
        // Only async-signal-safe work here: plain atomic stores.
        if sig == libc::SIGHUP {
            RELOAD_PENDING.store(true, Ordering::Release);
        } else {
            RECEIVED.store(sig, Ordering::Release);
        }
    }

    pub(super) fn install() {
        let handler: extern "C" fn(libc::c_int) = on_signal;
        // SAFETY: `sa` is a valid, fully initialized sigaction whose handler
        // only performs atomic stores (async-signal-safe).
        unsafe {
            let mut sa: libc::sigaction = std::mem::zeroed();
            sa.sa_sigaction = handler as usize;
            libc::sigemptyset(&mut sa.sa_mask);
            sa.sa_flags = libc::SA_RESTART;
            for sig in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP] {
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
        super::install();
        std::thread::spawn(|| {
            std::thread::sleep(Duration::from_millis(50));
            // SAFETY: raise() delivers the signal to this thread; the
            // installed handler only stores an atomic.
            unsafe { libc::raise(libc::SIGTERM) };
        });
        assert_eq!(super::wait_for_shutdown_signal(), "SIGTERM");
    }

    /// Deliberately does not exercise `wait_for_shutdown_signal`/
    /// `wait_for_event` here: `RECEIVED` is a process-wide static that
    /// `wait_returns_signal_name_after_sigterm` (above) may set concurrently
    /// on another test thread within the same `cargo test` process, and once
    /// set it never resets — so any assertion depending on `RECEIVED` being
    /// unset would be racy. This test only touches the independent
    /// `RELOAD_PENDING` flag via [`super::take_reload_request`], which never
    /// interacts with `RECEIVED`.
    #[test]
    fn sighup_sets_reload_flag_not_shutdown() {
        super::install();
        // Drain any pending flag from an earlier test.
        let _ = super::take_reload_request();
        // SAFETY: raise() delivers SIGHUP to this thread; the handler only
        // sets an atomic flag.
        unsafe { libc::raise(libc::SIGHUP) };
        // Give the handler a moment to run.
        std::thread::sleep(Duration::from_millis(20));
        assert!(super::take_reload_request(), "SIGHUP must set reload flag");
        // Flag is cleared after being taken.
        assert!(!super::take_reload_request());
    }
}
