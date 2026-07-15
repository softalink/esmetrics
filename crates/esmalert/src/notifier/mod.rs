//! Notifier: sends firing/resolved alerts to Alertmanager. Port of
//! `app/vmalert/notifier/` narrowed to the Alertmanager v2 integration
//! ([`alertmanager::AlertManager`]) plus the multi-target fan-out
//! ([`Notifiers`]) that upstream's `notifier.Send` (`init.go:296-321`)
//! performs across every configured notifier.

mod alertmanager;

pub use alertmanager::AlertManager;

use std::fmt;

use crate::rule::Alert;

/// Error returned by [`AlertManager::send`]. Never constructed from a panic;
/// always carries a human-readable message. Mirrors
/// [`crate::datasource::DsError`]'s shape — in particular, never carries
/// auth credentials (basic/bearer secrets are never formatted into it).
#[derive(Debug)]
pub struct NotifyError {
    msg: String,
}

impl NotifyError {
    pub(crate) fn new(msg: impl Into<String>) -> Self {
        NotifyError { msg: msg.into() }
    }
}

impl fmt::Display for NotifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for NotifyError {}

impl From<reqwest::Error> for NotifyError {
    fn from(e: reqwest::Error) -> Self {
        // reqwest's Display never includes header/body content, so no
        // credential (basic/bearer) can leak through this conversion.
        NotifyError::new(e.to_string())
    }
}

/// Every configured Alertmanager target. Port of the fan-out upstream's
/// `notifier.Send` (`init.go:296-321`) does across `ActiveNotifiers()`.
#[derive(Default)]
pub struct Notifiers(pub Vec<AlertManager>);

impl Notifiers {
    /// Sends `alerts` to every target, continuing past a failing target
    /// instead of aborting the whole batch (matches upstream: each notifier
    /// is sent to independently, and one's error doesn't stop the others —
    /// `init.go:310-318`). Returns the `(target index, error)` pairs for
    /// every target that failed; an empty result means every target
    /// succeeded.
    pub fn send(&self, alerts: &[Alert], external_url: &str) -> Vec<(usize, NotifyError)> {
        self.0
            .iter()
            .enumerate()
            .filter_map(|(i, am)| am.send(alerts, external_url).err().map(|e| (i, e)))
            .collect()
    }
}
