//! Owned time-series type built by the rule evaluator.
//!
//! The wire-format types in `esm_protoparser::prompb` (`TimeSeries<'a>`,
//! `Label<'a>`) borrow `&[u8]` slices of a decode/encode buffer — correct for
//! parsing/encoding, but unusable for series a rule *builds at runtime* from
//! owned `String`s (record name + rule labels + queried labels) and hands to
//! the remote-write client's background flush thread. This mirrors the same
//! owned-vs-borrowed decision already made for [`crate::datasource::Metric`]
//! (see its doc comment), which likewise uses `Vec<(String, String)>` instead
//! of `prompb::Label`.
//!
//! Conversion to the borrowed `esm_protoparser::prompb::TimeSeries<'_>` happens
//! at the remote-write encode boundary (a later task), where each `String` is
//! borrowed as `.as_bytes()` for the duration of one `encode_and_compress`
//! call.

/// A single sample. Reused from `esm_protoparser::prompb` — it is already an
/// owned, `Copy` value type (`{ value: f64, timestamp: i64 }`) with no
/// lifetime, so no separate owned variant is needed.
pub use esm_protoparser::prompb::Sample;

/// An owned time series produced by rule evaluation and queued for
/// remote-write.
///
/// `labels` always includes a `("__name__", <name>)` pair and carries UTF-8
/// label names/values (the datasource JSON path already yields UTF-8 strings).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Series {
    pub labels: Vec<(String, String)>,
    pub samples: Vec<Sample>,
}
