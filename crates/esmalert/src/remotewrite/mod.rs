//! Remote-write client: pushes recording-rule results and alert-state series
//! (`ALERTS`/`ALERTS_FOR_STATE`) to esmetrics over the Prometheus remote-write
//! protocol. Port of `app/vmalert/remotewrite/client.go:52-269`, narrowed to
//! what esmalert's rule evaluator needs: a bounded, non-blocking push queue
//! drained by a background flush thread (see [`client::RwClient`] for the
//! queue/thread design and documented deviations from upstream).

mod client;

#[allow(unused_imports)]
pub use client::{RwClient, RwConfig, DEFAULT_SEND_TIMEOUT};

use std::fmt;

/// Error returned by [`RwClient::start`]. Never constructed from a panic;
/// always carries a human-readable message. Mirrors
/// [`crate::datasource::DsError`] / [`crate::notifier::NotifyError`]'s shape
/// — in particular, never carries auth credentials (basic/bearer secrets are
/// never formatted into it). Background flush failures (encode errors,
/// non-2xx responses, network errors) are logged and dropped rather than
/// surfaced as `RwError`, matching the "never panic, log and continue"
/// requirement for the flush thread.
#[derive(Debug)]
pub struct RwError {
    msg: String,
}

impl RwError {
    fn new(msg: impl Into<String>) -> Self {
        RwError { msg: msg.into() }
    }
}

impl fmt::Display for RwError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for RwError {}

impl From<reqwest::Error> for RwError {
    fn from(e: reqwest::Error) -> Self {
        // reqwest's Display never includes header/body content, so no
        // credential (basic/bearer) can leak through this conversion.
        RwError::new(e.to_string())
    }
}
