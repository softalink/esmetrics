//! Built-in template functions ("FuncMap" in Go's `text/template`).
//!
//! Each submodule registers a themed batch of vmalert's builtin functions
//! (see `app/vmalert/templates/template.go` upstream) into the executor's
//! [`crate::exec::FuncFn`] map. This module covers the string/format,
//! humanize/time, and query/vector + context batches.

pub mod builtins;
pub mod humanize;
pub mod query;
pub mod strings;

pub use builtins::register_builtin_funcs;
pub use humanize::register_humanize_funcs;
pub use query::register_query_funcs;
pub use strings::register_string_funcs;
