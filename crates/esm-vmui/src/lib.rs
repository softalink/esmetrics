//! Embeds the upstream VictoriaMetrics React UI ("vmui") as static assets.
//!
//! vmui is licensed under Apache 2.0 and is incorporated unchanged into
//! EsMetrics binaries (see `NOTICE` and `CREDITS.md`). The build script
//! downloads and sha256-verifies the bundle from the upstream VictoriaMetrics
//! v1.144.0 release; a vendored copy under `vendor/vmui/` is used as offline
//! fallback.
//!
//! **Current state:** the build-script download is deferred (we don't want
//! a network requirement at `cargo build` time). Today the crate exposes a
//! single-page placeholder explaining how to point a browser at a real vmui
//! instance. Replacing this with the real vmui bundle is a future build.rs
//! change — the consumer API ([`asset`] / [`mime_for`]) does not need to
//! change.

const PLACEHOLDER_HTML: &[u8] = include_bytes!("../assets/index.html");

/// Look up a static asset by path. Returns `None` if the asset doesn't exist.
///
/// Path matching is exact and case-sensitive. Empty path or `/` returns the
/// index.
#[must_use]
pub fn asset(path: &str) -> Option<&'static [u8]> {
    match path.trim_start_matches('/') {
        "" | "index.html" => Some(PLACEHOLDER_HTML),
        _ => None,
    }
}

/// Guess a Content-Type for the given asset path.
#[must_use]
pub fn mime_for(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().unwrap_or("");
    match ext {
        "html" | "" => "text/html; charset=utf-8",
        "js" => "application/javascript",
        "css" => "text/css",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "json" => "application/json",
        _ => "application/octet-stream",
    }
}
