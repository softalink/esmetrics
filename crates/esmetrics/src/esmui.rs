//! Serves the vendored vmui (VictoriaMetrics web UI) static build at the
//! EsMetrics-branded path `/esmui` (`/vmui...` 302-redirects there for
//! upstream-link compatibility). The build is rebranded (product name,
//! logo, footer links/copyright, and the API-base path regex that now
//! recognizes the `/esmui` segment) via a source patch applied to
//! upstream vmui *before* it's built — not a post-build edit of the
//! compiled bundle — so the fix survives minification/hashing across
//! upstream version bumps; see `assets/esmui/PATCHES.md` for the full
//! re-vendoring procedure. The files under `assets/esmui/` are vendored
//! from VictoriaMetrics v1.146.0's `app/vmui/packages/vmui` (Apache-2.0 —
//! see `NOTICE`), rebranded; `build.rs` walks that directory at compile
//! time and emits the `ESMUI_ASSETS` table this module serves from via
//! `include!`.
//!
//! Mirrors the essentials of the upstream `app/vmselect/main.go`
//! `RequestHandler` / `handleStaticAndSimpleRequests` vmui section, with
//! `/esmui` as the canonical path:
//! - The `/prometheus` path prefix is stripped before routing (cluster
//!   path compatibility; upstream vmselect strips `/prometheus/` and
//!   `/graphite/` at the top of `RequestHandler`, so the whole UI tree
//!   is also reachable as `/prometheus/esmui...` — and the app computes
//!   its API base as `<origin>/prometheus`).
//! - `/esmui` and `/graph` (no trailing slash) redirect to `esmui/` /
//!   `graph/`, like upstream's `httpserver.Redirect` (a relative
//!   redirect, so it keeps working if esmetrics is reverse-proxied under
//!   a sub-path), preserving the query string. The legacy `/vmui` tree
//!   302-redirects to the `/esmui` equivalents.
//! - `/graph/<path>` is rewritten to `/esmui/<path>` (upstream rewrites
//!   `/graph/` to its UI path for Grafana's Prometheus-datasource links).
//! - `/esmui/custom-dashboards` and `/esmui/timezone` are dynamic JSON
//!   endpoints in upstream (`app/vmselect/vmui.go`); matched here BEFORE
//!   the static lookup, like upstream's routing order, and answered with
//!   the upstream default-flag responses (see below).
//! - `/esmui/<path>` serves the matching vendored file with a
//!   content-type derived from its extension.
//! - Hashed build output (`/esmui/assets/...`) gets a longer cache
//!   lifetime; `index.html` (and anything falling back to it) is
//!   `no-cache` so a redeploy is picked up on the next load — mirroring
//!   upstream's "hashed assets cache longer, entry point doesn't" split,
//!   though the concrete lifetime here (1 hour) follows this codebase's
//!   existing `/favicon.ico` / `/logo.svg` convention rather than
//!   upstream's one-year value.
//! - Unknown `/esmui/<path>` falls back to `index.html` (200), since the
//!   UI is a client-side-routed single-page app.
//!
//! Deviations from upstream (vmui's Go app logic is listed as out of
//! scope for porting in `docs/PORTING.md` — this module vendors and
//! serves the built assets):
//! - The `-vmui.customDashboardsPath` and `-vmui.defaultTimezone` flags
//!   are not ported; the two dynamic endpoints answer with the
//!   default-flag responses baked in: `{"dashboardsSettings": []}`
//!   (upstream with the path flag unset) and `{"timezone": "UTC"}`
//!   (upstream's flag defaults to `""` and Go's `time.LoadLocation("")`
//!   returns UTC, so `fmt.Sprintf` with `%q` yields `"UTC"`).
//! - `config.json` is served as the static file vmui's own build already
//!   produces. Upstream instead intercepts it and injects `version` (build
//!   version, cosmetic footer) and `vmalert.enabled` (false with the
//!   unported `-vmalert.proxyURL` unset) at startup — the observable values
//!   coincide today, but the MECHANISM differs; revisit if either field
//!   gains a real source.

use esm_http::{Request, ResponseWriter};

include!(concat!(env!("OUT_DIR"), "/esmui_assets.rs"));

/// Handles `/esmui`, `/graph`, the redirecting legacy `/vmui` tree, their
/// subtrees, and the same paths under the `/prometheus` cluster-compat
/// prefix. Returns `false` for any other path so the caller can fall
/// through to its own routing / 404.
pub fn handle(req: &mut Request<'_>, w: &mut ResponseWriter<'_>) -> bool {
    // Upstream normalizes doubled slashes before any routing (single-pass
    // ReplaceAll, same as esm-insert's router); without this,
    // `/esmui//assets/x.js` would miss the asset table and fall back to
    // index.html instead of serving the asset.
    let raw = req.path();
    let collapsed: std::borrow::Cow<'_, str> = if raw.contains("//") {
        std::borrow::Cow::Owned(raw.replace("//", "/"))
    } else {
        std::borrow::Cow::Borrowed(raw)
    };
    // Cluster path compatibility: strip the /prometheus prefix, like
    // upstream vmselect's RequestHandler (which strips "/prometheus/",
    // i.e. only when something follows the prefix).
    let path = match collapsed.strip_prefix("/prometheus") {
        Some(rest) if rest.starts_with('/') => rest,
        _ => &collapsed,
    };

    // Upstream: the UI path and `/graph` without the trailing slash
    // redirect to the complete relative URL, keeping the query string
    // (`newURL := path + "/?" + r.Form.Encode()`). EsMetrics serves the
    // UI at `/esmui`; the upstream `/vmui` page URL redirects there so
    // old links and tooling habits keep working.
    if path == "/esmui" || path == "/graph" || path == "/vmui" {
        let target = if path == "/graph" { "graph" } else { "esmui" };
        let query = req.query();
        let location = if query.is_empty() {
            format!("{target}/")
        } else {
            format!("{target}/?{query}")
        };
        w.set_status(302);
        w.set_header("Location", &location);
        return true;
    }

    // Rebrand compatibility: the whole `/vmui/<rest>` subtree redirects
    // to `/esmui/<rest>` (relative `../esmui/...`, so it resolves
    // correctly under the `/prometheus` prefix and behind reverse
    // proxies). This also covers the three URLs the vendored app fetches
    // with a hardcoded `/vmui/` segment (config.json, custom-dashboards,
    // timezone) — `fetch` follows redirects.
    if let Some(rest) = path.strip_prefix("/vmui/") {
        let query = req.query();
        let location = if query.is_empty() {
            format!("../esmui/{rest}")
        } else {
            format!("../esmui/{rest}?{query}")
        };
        w.set_status(302);
        w.set_header("Location", &location);
        return true;
    }

    // Upstream rewrites `/graph/...` to the UI path (Grafana Prometheus
    // datasource links), then routes as the UI.
    let rel = match (path.strip_prefix("/esmui/"), path.strip_prefix("/graph/")) {
        (Some(rel), _) | (None, Some(rel)) => rel,
        (None, None) => return false,
    };

    // Dynamic endpoints, matched before the static lookup (upstream
    // main.go checks these before the `/vmui/` FileServer dispatch).
    match rel {
        // app/vmselect/vmui.go handleVMUICustomDashboards with
        // -vmui.customDashboardsPath unset (flag not ported).
        "custom-dashboards" => {
            w.write_json(200, r#"{"dashboardsSettings": []}"#);
            return true;
        }
        // app/vmselect/vmui.go handleVMUITimezone with
        // -vmui.defaultTimezone unset (flag not ported):
        // time.LoadLocation("") is UTC. Upstream wraps this in
        // httpserver.EnableCORS, which (with -http.disableCORS at its
        // default) sets the three wildcard CORS headers below.
        "timezone" => {
            w.set_header("Access-Control-Allow-Origin", "*");
            w.set_header("Access-Control-Allow-Methods", "*");
            w.set_header("Access-Control-Allow-Headers", "*");
            w.write_json(200, r#"{"timezone": "UTC"}"#);
            return true;
        }
        _ => {}
    }

    let lookup = if rel.is_empty() { "index.html" } else { rel };

    let (logical, bytes) = find_asset(lookup).unwrap_or_else(|| {
        // Client-side routing: an unknown sub-path falls back to
        // index.html (e.g. a bookmarked pathname-style deep link).
        find_asset("index.html").expect("esmui index.html missing from vendored assets")
    });

    w.set_content_type(content_type(logical));
    w.set_header("Cache-Control", cache_control(logical));
    w.write_body(bytes);
    true
}

fn find_asset(logical: &str) -> Option<(&'static str, &'static [u8])> {
    ESMUI_ASSETS
        .iter()
        .find(|(name, _)| *name == logical)
        .copied()
}

/// Longer-lived caching for the UI's content-hashed build output
/// (`assets/<name>-<hash>.<ext>`, safe to cache since a new build changes
/// the filename); everything else — starting with `index.html`, the SPA
/// entry point — is `no-cache` so a redeploy is visible immediately.
fn cache_control(logical: &str) -> &'static str {
    if logical.starts_with("assets/") {
        "max-age=3600"
    } else {
        "no-cache"
    }
}

fn content_type(logical: &str) -> &'static str {
    let ext = logical.rsplit('.').next().unwrap_or("");
    match ext {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "woff2" => "font/woff2",
        "json" => "application/json",
        "md" => "text/markdown; charset=utf-8",
        "txt" => "text/plain; charset=utf-8",
        "jpg" | "jpeg" => "image/jpeg",
        "ico" => "image/x-icon",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_type_matches_known_extensions() {
        assert_eq!(content_type("index.html"), "text/html; charset=utf-8");
        assert_eq!(
            content_type("assets/index-abc123.js"),
            "text/javascript; charset=utf-8"
        );
        assert_eq!(
            content_type("assets/index-abc123.css"),
            "text/css; charset=utf-8"
        );
        assert_eq!(content_type("favicon.svg"), "image/svg+xml");
        assert_eq!(content_type("config.json"), "application/json");
        assert_eq!(content_type("robots.txt"), "text/plain; charset=utf-8");
        assert_eq!(content_type("preview.jpg"), "image/jpeg");
        assert_eq!(content_type("no-extension"), "application/octet-stream");
    }

    #[test]
    fn cache_control_is_long_for_hashed_assets_and_no_cache_otherwise() {
        assert_eq!(cache_control("assets/index-abc123.js"), "max-age=3600");
        assert_eq!(cache_control("index.html"), "no-cache");
        assert_eq!(cache_control("config.json"), "no-cache");
    }

    #[test]
    fn vendored_index_html_is_present() {
        assert!(
            find_asset("index.html").is_some(),
            "build.rs must vendor at least index.html from assets/esmui/"
        );
    }
}
