//! Cross-platform OS abstractions for EsMetrics.
//!
//! All code that touches platform-specific syscalls (mmap, fsync, file locks,
//! signal handlers, path canonicalisation) lives here. The rest of the
//! workspace consumes only the platform-neutral traits and structs this crate
//! exposes, keeping `#[cfg(unix)]` / `#[cfg(windows)]` blocks out of business
//! logic.
//!
//! Module map:
//! - [`mmap`] — memory-mapped file access.
//! - [`durability`] — fsync semantics for files and directories.
//! - [`atomic_rename`] — atomic rename with cross-platform replacement semantics.
//! - [`file_lock`] — exclusive data-directory locking.
//! - [`signal`] — graceful shutdown / reload streams.
//! - [`paths`] — path canonicalisation and validation, incl. Windows long paths.
//! - [`proc`] — process-level OS knobs (file-descriptor limits, etc.).

pub mod atomic_rename;
pub mod durability;
pub mod file_lock;
pub mod mmap;
pub mod paths;
pub mod proc;
pub mod signal;
