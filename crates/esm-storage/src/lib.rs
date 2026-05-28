//! EsMetrics storage engine.
//!
//! Implements VictoriaMetrics v1.144.0-compatible on-disk format:
//! - **mergeset**: LSM-like sorted-string KV store used for the inverted index.
//! - **indexdb**: TSID assignment and label-set lookup.
//! - **parts**: immutable per-day time-series part directories.
//! - **merger**: background concurrent part merger.
//! - **retention**: time-based + size-based retention enforcement.
//!
//! Byte-level on-disk compatibility with VictoriaMetrics v1.144.0 is the design
//! goal. See `docs/format/` for reverse-engineered format specifications.

pub mod mergeset;
pub mod storage;
pub mod timeseries;

pub use storage::{Sample, Storage, StorageError, StoredSample, TimeRange};
