//! esm-auth: the vmauth port library (config, auth, routing, load
//! balancing, and the proxy request lifecycle). Port of the open-source
//! subset of VictoriaMetrics v1.146.0 `app/vmauth`
//! (`{main,auth_config,target_url}.go`); JWT/OIDC, DNS backend discovery,
//! and backend-TLS tuning are out of scope (see docs/PORTING.md).
//!
//! YAML dependency: Uses `serde_yaml_ng` (maintained drop-in fork of
//! deprecated `serde_yaml`) for config serialization.

pub mod auth;
pub mod balance;
pub mod config;
pub mod metrics;
pub mod proxy;
pub mod route;
