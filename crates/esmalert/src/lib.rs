//! esmalert library: parsed CLI flags, the rule engine (config parsing,
//! MetricsQL/Go-template evaluation, alerting/recording rule state machine),
//! the datasource/remote-write/notifier clients, and the runtime wiring
//! (`app::run`).
//!
//! Lib-ified (mirrors `esmauth`'s `lib.rs`/`main.rs` split) so integration
//! tests can drive the daemon in-process: build real `Datasource`/`RwClient`/
//! `AlertManager` clients against in-process stub `esm-http` servers and call
//! `rule::Group::eval_once` directly, without spawning a process or waiting
//! on wall-clock ticks. `main.rs` stays a thin
//! parse-flags -> `-dryRun`/`-help`/`-version` short-circuit -> `app::run`
//! shell; everything else lives here.
//!
//! Only the modules an external caller (`main.rs`, integration tests) needs
//! are `pub`; `manager`/`remoteread`/`templating`/`web` stay crate-private
//! implementation details, reached the same way they always were via
//! `crate::` paths from sibling modules.

pub mod app;
pub mod config;
pub mod datasource;
pub mod flags;
pub mod notifier;
pub mod remotewrite;
pub mod rule;
pub mod series;
pub mod signal;

mod manager;
mod remoteread;
mod templating;
mod web;

/// Builds a runtime `rule::group::Group` that a caller can evaluate directly
/// via `rule::Group::eval_once` — outside a `Manager`-owned background
/// thread. The only piece of `manager`'s internals a non-daemon caller needs
/// (`esmalert-tool`'s offline unit-test runner), so it's re-exported here
/// rather than making all of `manager` `pub`. See its doc comment in
/// `manager.rs` for the label-merge semantics it applies.
pub use manager::build_group_for_eval;
