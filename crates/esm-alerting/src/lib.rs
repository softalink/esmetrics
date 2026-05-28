//! Rule evaluation engine and Alertmanager client for `esm-alert`.
//!
//! Loads Prometheus / vmalert-compatible YAML rule files, evaluates recording
//! and alerting rules on a tick schedule, persists alert state across restarts
//! via remote storage, and pushes firing alerts to Alertmanager v2 endpoints
//! with HA-aware fanout.
