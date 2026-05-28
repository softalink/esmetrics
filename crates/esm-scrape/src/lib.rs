//! Scraping engine and relabeling for `esm-agent`.
//!
//! Implements the Prometheus-compatible scrape loop: HTTP target polling with
//! TLS / auth, exposition-format parsing (text + OpenMetrics), the full
//! Prometheus relabel action set, and a disk-backed retry queue that is
//! byte-compatible with vmagent v1.144.0's `persistentqueue`.

pub mod relabel;

pub use relabel::{Action, RelabelError, RelabelRule, apply};
