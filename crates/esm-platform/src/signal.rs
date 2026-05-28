//! Graceful shutdown / reload signal streams.
//!
//! Tiny wrappers over `tokio::signal` that yield typed events and abstract
//! over the per-platform signal differences. All functions here are
//! `async`-only and must be awaited from a Tokio runtime context.

use std::io;

/// Source of a shutdown signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownSignal {
    /// SIGTERM (Unix) — typical service-stop signal.
    Term,
    /// SIGINT / Ctrl-C (Unix and Windows).
    Int,
}

/// Wait for the first incoming shutdown signal and return its source.
///
/// Unix: races SIGTERM and SIGINT; whichever fires first wins.
///
/// Windows: waits on `Ctrl-C` via the standard tokio handler. (Service-stop
/// integration is handled by the Windows-service crate at the binary level,
/// not here.)
pub async fn wait_for_shutdown() -> io::Result<ShutdownSignal> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sigint = signal(SignalKind::interrupt())?;
        tokio::select! {
            _ = sigterm.recv() => Ok(ShutdownSignal::Term),
            _ = sigint.recv()  => Ok(ShutdownSignal::Int),
        }
    }
    #[cfg(windows)]
    {
        tokio::signal::ctrl_c().await?;
        Ok(ShutdownSignal::Int)
    }
}

/// Wait for a reload signal.
///
/// Unix: SIGHUP. Windows: a future that never resolves (reload via signal is
/// not wired on Windows; service-control integration handles it at the binary
/// level once we ship a service wrapper).
pub async fn wait_for_reload() -> io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut hup = signal(SignalKind::hangup())?;
        hup.recv().await;
        Ok(())
    }
    #[cfg(windows)]
    {
        std::future::pending::<()>().await;
        unreachable!("std::future::pending never resolves");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke test: ensure the futures construct without panicking. We can't
    /// easily drive a real shutdown in a unit test without racing against the
    /// test harness, so we verify the future is `Future` and creates cleanly.
    #[tokio::test]
    async fn shutdown_future_constructs() {
        use std::time::Duration;
        let r = tokio::time::timeout(Duration::from_millis(10), wait_for_shutdown()).await;
        assert!(r.is_err(), "expected the timeout to elapse before any signal arrived");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reload_future_constructs() {
        use std::time::Duration;
        let r = tokio::time::timeout(Duration::from_millis(10), wait_for_reload()).await;
        assert!(r.is_err(), "expected the timeout to elapse before SIGHUP arrived");
    }
}
