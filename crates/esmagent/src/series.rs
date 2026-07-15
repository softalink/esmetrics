//! The owned, decoded series shape passed from [`crate::sink::ForwardingSink`]
//! to downstream consumers (pendingseries/queue in later tasks).

use esm_protoparser::prompb::Sample;
use esm_relabel::Label;

/// A single time series decoded from one or more [`esm_insert::MetricRow`]s
/// that share the same `metric_name_raw`, with its labels and samples fully
/// owned (no borrow of the ingestion batch arena survives past decode).
#[derive(Debug, Clone, PartialEq)]
pub struct OwnedSeries {
    pub labels: Vec<Label>,
    pub samples: Vec<Sample>,
}
