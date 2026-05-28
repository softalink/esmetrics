//! Ingest-protocol parsers for EsMetrics.
//!
//! Parses wire-format inputs and yields rows compatible with [`esm_storage`].
//! Zero-copy where the source format permits.
//!
//! Implemented:
//! - [`text_exposition`] — Prometheus text exposition format (the `/metrics`
//!   scrape format). The simplest protocol; lands first because it has no
//!   external dependencies and is broadly applicable.
//!
//! Targets (priority order, remainder):
//! 1. Prometheus remote-write (`/api/v1/write`) — protobuf + snappy.
//! 2. Native VM protocol (`/api/v1/import/native`).
//! 3. JSON line import (`/api/v1/import`).
//! 4. CSV (`/api/v1/import/csv`).
//! 5. Influx line v1 + v2 (`/write`, `/api/v2/write`).
//! 6. Graphite (TCP plaintext + HTTP).
//! 7. OpenTSDB (telnet + HTTP).
//! 8. DataDog (`/api/v1/series`).
//! 9. NewRelic.
//! 10. OpenTelemetry metrics (`/opentelemetry/v1/metrics`).

pub mod csv_import;
pub mod datadog;
pub mod graphite;
pub mod influx_line;
pub mod json_line;
pub mod native_vm;
pub mod newrelic;
pub mod opentsdb;
pub mod otlp;
pub mod prom_remote_write;
pub mod text_exposition;
