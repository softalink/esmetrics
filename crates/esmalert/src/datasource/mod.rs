//! `esmalert` datasource read path: queries esmetrics' Prometheus-compatible
//! `/api/v1/query{,_range}` endpoints and parses the JSON response.
//!
//! Port of `app/vmalert/datasource/datasource.go` (`Result`/`Metric`) and
//! `app/vmalert/datasource/client_prom.go` (request building, response
//! parsing), narrowed to the Prometheus API surface esmalert's rule
//! evaluator needs. Never panics on malformed input or a network failure —
//! every fallible path returns [`DsError`].

mod auth;
mod client;
mod prom_json;

// Real wiring into the rule evaluator lands in a later task; these
// re-exports are unused from `main` until then (mirrors
// `esmalert::config`'s `#[allow(unused_imports)]` re-exports).
#[allow(unused_imports)]
pub use auth::{AuthConfig, AuthFlags, TlsConfig};
#[allow(unused_imports)]
pub use client::{Datasource, DEFAULT_QUERY_TIMEOUT};
#[allow(unused_imports)]
pub use prom_json::parse_prom_response;

use std::fmt;

/// One labeled time series in a datasource response. Port of `Metric`
/// (`datasource.go:60-65`); `labels` is an ordered list rather than a
/// `prompb.Label` slice since esmalert doesn't otherwise depend on
/// `esm-protoparser`'s label wire type here.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Metric {
    pub labels: Vec<(String, String)>,
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
}

/// Port of `Result` (`datasource.go:28-38`), minus `SeriesFetched` (a
/// VictoriaMetrics-only `stats.seriesFetched` extension not needed by this
/// task).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryResult {
    pub data: Vec<Metric>,
    pub is_partial: Option<bool>,
}

/// Error returned by [`parse_prom_response`] and [`Datasource`]'s query
/// methods. Never constructed from a panic; always carries a human-readable
/// message. Mirrors `esmalert::config::ConfigError`'s shape.
#[derive(Debug)]
pub struct DsError {
    msg: String,
}

impl DsError {
    fn new(msg: impl Into<String>) -> Self {
        DsError { msg: msg.into() }
    }
}

impl fmt::Display for DsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for DsError {}

impl From<reqwest::Error> for DsError {
    fn from(e: reqwest::Error) -> Self {
        // reqwest's Display never includes header/body content, so no
        // credential (basic/bearer) can leak through this conversion.
        DsError::new(e.to_string())
    }
}
