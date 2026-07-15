//! Target-URL routing: which backend a request goes to, and the concrete
//! URL to send it to.
//!
//! Port of `getURLPrefixAndHeaders`, `mergeURLs`, and `dropPrefixParts`
//! (`app/vmauth/target_url.go:11-153`), plus the default-route branch from
//! `app/vmauth/main.go:414-439`.

use std::collections::{BTreeMap, HashSet};

use crate::config::{LoadBalancingPolicy, UserInfo};

/// The routing decision for a request: which backend url_prefix list applies,
/// plus the merged headers/response-headers/retry/policy for it.
pub struct Route<'a> {
    pub url_prefixes: &'a [String],
    pub headers: &'a [(String, String)],
    pub response_headers: &'a [(String, String)],
    pub retry_status_codes: &'a [u16],
    pub policy: LoadBalancingPolicy,
    pub drop_src_path_prefix_parts: Option<usize>,
    pub merge_query_args: &'a [String],
    /// `true` when this route came from `default_url`, not a `url_map` entry
    /// or the user's own `url_prefix`.
    pub is_default: bool,
}

/// Picks the route for `(path, host)`. Port of `getURLPrefixAndHeaders`
/// (target_url.go:67-88) plus the default-route fallback the caller performs
/// in `main.go:414-417`.
///
/// Iterates `url_map` entries in order; the first entry whose `src_paths` and
/// `src_hosts` both match wins (an empty pattern list matches anything, like
/// upstream's `matchAnyRegex`) and its own headers/retry/policy/etc. are used
/// verbatim — they are not merged with the user's. Falls back to the user's
/// own `url_prefix`, then to `default_url` (`is_default = true`), then to
/// `None` when nothing applies (the caller's "missing route" case).
pub fn select_route<'a>(user: &'a UserInfo, path: &str, host: &str) -> Option<Route<'a>> {
    for entry in &user.url_map {
        if !matches_any(&entry.src_paths, path) || !matches_any(&entry.src_hosts, host) {
            continue;
        }
        return Some(Route {
            url_prefixes: &entry.url_prefix,
            headers: &entry.headers,
            response_headers: &entry.response_headers,
            retry_status_codes: &entry.retry_status_codes,
            policy: entry.load_balancing_policy,
            drop_src_path_prefix_parts: entry.drop_src_path_prefix_parts,
            merge_query_args: &entry.merge_query_args,
            is_default: false,
        });
    }

    if let Some(prefixes) = &user.url_prefix {
        return Some(Route {
            url_prefixes: prefixes,
            headers: &user.headers,
            response_headers: &user.response_headers,
            retry_status_codes: &user.retry_status_codes,
            policy: user.load_balancing_policy,
            drop_src_path_prefix_parts: user.drop_src_path_prefix_parts,
            merge_query_args: &user.merge_query_args,
            is_default: false,
        });
    }

    let prefixes = user.default_url.as_ref()?;
    Some(Route {
        url_prefixes: prefixes,
        headers: &user.headers,
        response_headers: &user.response_headers,
        retry_status_codes: &user.retry_status_codes,
        policy: user.load_balancing_policy,
        drop_src_path_prefix_parts: user.drop_src_path_prefix_parts,
        merge_query_args: &user.merge_query_args,
        is_default: true,
    })
}

/// Mirrors `matchAnyRegex` (target_url.go:90-100): an empty pattern list
/// matches everything.
fn matches_any(patterns: &[regex::Regex], s: &str) -> bool {
    patterns.is_empty() || patterns.iter().any(|r| r.is_match(s))
}

/// Cleans and normalizes the request path exactly like `normalizeURL`
/// (target_url.go:132-153) before it is used for route matching and
/// target-URL construction. This runs `path.Clean` (its comment: "Prevent
/// from attacks with using `..` in r.URL.Path"), then restores a trailing
/// slash `path.Clean` stripped (upstream #1752), forces a leading slash, and
/// maps a bare "/" to "" so a root request appends nothing to the backend
/// path (upstream PR #1554).
///
/// Operates on the already-percent-decoded path the proxy passes in — it does
/// NOT decode again.
pub fn normalize_path(path: &str) -> String {
    let mut cleaned = path_clean(path);
    // path.Clean returns "." for an empty/all-dots path; normalizeURL turns
    // that into the root.
    if cleaned == "." {
        cleaned = "/".to_string();
    }
    if !cleaned.ends_with('/') && path.ends_with('/') {
        // path.Clean removes the trailing slash; put it back (upstream #1752).
        cleaned.push('/');
    }
    if !cleaned.starts_with('/') {
        cleaned.insert(0, '/');
    }
    if cleaned == "/" {
        // A bare root maps to "" so nothing is appended to the backend path
        // (upstream PR #1554).
        cleaned.clear();
    }
    cleaned
}

/// Faithful port of Go's `path.Clean` (the `path` package, not `filepath`):
/// purely lexical resolution of `.`/`..` segments and duplicate slashes, with
/// no filesystem access. Returns "." for an empty input, matching Go.
///
/// Works byte-wise like the Go original; only ASCII `/` and `.` are special,
/// so multi-byte UTF-8 sequences pass through untouched and the result stays
/// valid UTF-8.
fn path_clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }
    let bytes = path.as_bytes();
    let n = bytes.len();
    let rooted = bytes[0] == b'/';

    // `out` is the buffer being built; `dotdot` is the index in `out` past
    // which a `..` may not backtrack (the leading `/` or a leading `../`
    // prefix).
    let mut out: Vec<u8> = Vec::with_capacity(n);
    let mut r = 0usize;
    let mut dotdot = 0usize;
    if rooted {
        out.push(b'/');
        r = 1;
        dotdot = 1;
    }

    while r < n {
        if bytes[r] == b'/' {
            // empty path element
            r += 1;
        } else if bytes[r] == b'.' && (r + 1 == n || bytes[r + 1] == b'/') {
            // . element
            r += 1;
        } else if bytes[r] == b'.'
            && r + 1 < n
            && bytes[r + 1] == b'.'
            && (r + 2 == n || bytes[r + 2] == b'/')
        {
            // .. element: remove to last '/'
            r += 2;
            if out.len() > dotdot {
                // can backtrack
                let mut w = out.len() - 1;
                while w > dotdot && out[w] != b'/' {
                    w -= 1;
                }
                out.truncate(w);
            } else if !rooted {
                // cannot backtrack, but not rooted, so append a `..` element.
                if !out.is_empty() {
                    out.push(b'/');
                }
                out.push(b'.');
                out.push(b'.');
                dotdot = out.len();
            }
        } else {
            // real path element; add a slash if needed
            if (rooted && out.len() != 1) || (!rooted && !out.is_empty()) {
                out.push(b'/');
            }
            // copy element
            while r < n && bytes[r] != b'/' {
                out.push(bytes[r]);
                r += 1;
            }
        }
    }

    if out.is_empty() {
        return ".".to_string();
    }
    // `out` holds only bytes copied from a valid UTF-8 `str` plus ASCII '/'
    // and '.', so it is always valid UTF-8.
    String::from_utf8(out).expect("path_clean output is valid UTF-8")
}

/// Strips `parts` leading `/`-delimited segments from `path`. Port of
/// `dropPrefixParts` (target_url.go:51-65). `parts == 0` returns `path`
/// unchanged; running out of segments returns `""`.
fn drop_prefix_parts(path: &str, parts: usize) -> String {
    if parts == 0 {
        return path.to_string();
    }
    let mut rest = path;
    for _ in 0..parts {
        rest = rest.strip_prefix('/').unwrap_or(rest);
        match rest.find('/') {
            None => return String::new(),
            Some(n) => rest = &rest[n..],
        }
    }
    rest.to_string()
}

/// Builds the concrete target URL from a chosen backend `prefix` (one entry
/// of `Route::url_prefixes`) and the request. Port of `mergeURLs`
/// (target_url.go:11-49) plus the default-route branch (`main.go:430-438`).
///
/// For non-default routes: the (prefix-dropped) request path is appended to
/// the backend path (trimming one trailing `/` off the backend path first,
/// per upstream, when the dropped path starts with `/`), and the request's
/// query args are merged into the backend's own query args — the backend
/// wins on a key clash unless that key is listed in `merge_query_args`.
///
/// For default routes (`is_default = true`): the path is left untouched and
/// `request_path` is set (overwriting any existing value) to
/// `full_request_uri` in the backend's own query args.
pub fn build_target_url(
    prefix: &str,
    req_path: &str,
    req_query: &str,
    drop_parts: usize,
    merge_query_args: &[String],
    is_default: bool,
    full_request_uri: &str,
) -> String {
    let (base, target_query) = split_query(prefix);

    if is_default {
        let mut merged = decode_multimap(target_query);
        merged.insert(
            "request_path".to_string(),
            vec![full_request_uri.to_string()],
        );
        return format!("{base}?{}", encode_multimap(&merged));
    }

    let src_path = drop_prefix_parts(req_path, drop_parts);
    let mut base = base.to_string();
    if src_path.starts_with('/') {
        if let Some(stripped) = base.strip_suffix('/') {
            base = stripped.to_string();
        }
    }
    base.push_str(&src_path);

    if esm_http::parse_query(req_query).next().is_none() {
        // mergeURLs returns immediately when the request has zero decoded
        // query params (target_url.go:21), without touching RawQuery — the
        // backend's original (unsorted, unre-encoded) query is kept. Note
        // this gates on decoded-param-count, not the literal string: a query
        // like "&" is non-empty but yields zero params.
        return if target_query.is_empty() {
            base
        } else {
            format!("{base}?{target_query}")
        };
    }

    let merged = merge_query(target_query, req_query, merge_query_args);
    let encoded = encode_multimap(&merged);
    if encoded.is_empty() {
        base
    } else {
        format!("{base}?{encoded}")
    }
}

/// Splits `url` into `(everything before '?', raw query without '?')`.
fn split_query(url: &str) -> (&str, &str) {
    match url.split_once('?') {
        Some((base, query)) => (base, query),
        None => (url, ""),
    }
}

fn decode_multimap(raw_query: &str) -> BTreeMap<String, Vec<String>> {
    let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (k, v) in esm_http::parse_query(raw_query) {
        map.entry(k.into_owned()).or_default().push(v.into_owned());
    }
    map
}

/// Merges backend (target) and client query args like `mergeURLs`
/// (target_url.go:26-45): the backend's args are copied first; a client arg
/// is added only if the backend doesn't already have that key, or the key is
/// listed in `merge_query_args` — clashing client args are otherwise dropped
/// for security.
fn merge_query(
    target_query: &str,
    client_query: &str,
    merge_query_args: &[String],
) -> BTreeMap<String, Vec<String>> {
    let mut merged = decode_multimap(target_query);
    let target_keys: HashSet<String> = merged.keys().cloned().collect();

    for (k, v) in esm_http::parse_query(client_query) {
        let key = k.into_owned();
        if target_keys.contains(&key) && !merge_query_args.iter().any(|a| a == &key) {
            // Skip clashed client query params for security reasons.
            continue;
        }
        merged.entry(key).or_default().push(v.into_owned());
    }
    merged
}

/// Encodes a decoded query multimap like Go's `url.Values.Encode()`: keys
/// sorted lexicographically (`BTreeMap` gives us this for free) and each
/// key's values emitted in insertion order.
fn encode_multimap(map: &BTreeMap<String, Vec<String>>) -> String {
    let mut parts = Vec::new();
    for (k, values) in map {
        let encoded_key = query_escape(k);
        for v in values {
            parts.push(format!("{encoded_key}={}", query_escape(v)));
        }
    }
    parts.join("&")
}

/// Percent-encodes a query-string component like Go's `url.QueryEscape`:
/// unreserved bytes (`A-Za-z0-9`, `-`, `_`, `.`, `~`) pass through unchanged,
/// a space becomes `+`, and everything else — including all of `$&+,/:;=?@`,
/// which `QueryEscape` reserves in query-component mode — is percent-encoded
/// with uppercase hex.
fn query_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::UrlMap;

    fn anchored(pat: &str) -> regex::Regex {
        regex::Regex::new(&format!("^(?:{pat})$")).unwrap()
    }

    fn url_map(src_paths: &[&str], src_hosts: &[&str], url_prefix: &str) -> UrlMap {
        UrlMap {
            src_paths: src_paths.iter().map(|p| anchored(p)).collect(),
            src_hosts: src_hosts.iter().map(|p| anchored(p)).collect(),
            url_prefix: vec![url_prefix.to_string()],
            headers: vec![],
            response_headers: vec![],
            retry_status_codes: vec![],
            load_balancing_policy: LoadBalancingPolicy::LeastLoaded,
            drop_src_path_prefix_parts: None,
            merge_query_args: vec![],
        }
    }

    // --- build_target_url: the 6 brief cases (ported from target_url_test.go) ---

    #[test]
    fn merge_appends_request_path_to_prefix_path() {
        let got = build_target_url("http://b/x", "/y", "", 0, &[], false, "");
        assert_eq!(got, "http://b/x/y");
    }

    #[test]
    fn merge_backend_query_wins_on_clash() {
        // Backend already has `extra`; the client's clashing `extra` is
        // dropped, its non-clashing `a` passes through. Go's
        // url.Values.Encode() sorts keys, so `a` (< `extra`) comes first.
        let got = build_target_url("http://b/?extra=1", "", "extra=2&a=3", 0, &[], false, "");
        assert_eq!(got, "http://b/?a=3&extra=1");
    }

    #[test]
    fn merge_query_args_allows_override() {
        // Same clash, but `extra` is listed in merge_query_args: both the
        // backend's and the client's `extra` values survive, backend first.
        let got = build_target_url(
            "http://b/?extra=1",
            "",
            "extra=2&a=3",
            0,
            &["extra".to_string()],
            false,
            "",
        );
        assert_eq!(got, "http://b/?a=3&extra=1&extra=2");
    }

    #[test]
    fn drop_src_path_prefix_parts_strips_leading_segments() {
        let got = build_target_url("http://b/x", "/prefix/y", "", 1, &[], false, "");
        assert_eq!(got, "http://b/x/y");
    }

    #[test]
    fn default_route_sets_request_path_query() {
        let got = build_target_url(
            "http://b/x",
            "/ignored",
            "",
            0,
            &[],
            true,
            "/prefix/y?query=up",
        );
        // Path is untouched (no "/ignored" appended); the full request URI is
        // percent-encoded as a single query value.
        assert_eq!(got, "http://b/x?request_path=%2Fprefix%2Fy%3Fquery%3Dup");
    }

    #[test]
    fn select_route_matches_url_map_src_paths() {
        let user = UserInfo {
            url_prefix: Some(vec!["http://default-server".to_string()]),
            url_map: vec![url_map(&["/api/v1/write"], &[], "http://vminsert")],
            ..Default::default()
        };

        let route = select_route(&user, "/api/v1/write", "host").expect("route");
        assert_eq!(route.url_prefixes, ["http://vminsert".to_string()]);
        assert!(!route.is_default);
    }

    #[test]
    fn falls_back_to_default_url() {
        let user = UserInfo {
            default_url: Some(vec!["http://default-server".to_string()]),
            url_map: vec![url_map(&["/api/v1/write"], &[], "http://vminsert")],
            ..Default::default()
        };

        let route = select_route(&user, "/other", "host").expect("route");
        assert_eq!(route.url_prefixes, ["http://default-server".to_string()]);
        assert!(route.is_default);
    }

    // --- extra fidelity: additional cases ported verbatim from
    // target_url_test.go's TestMergeURLs / TestCreateTargetURLSuccess / TestDropPrefixParts ---

    #[test]
    fn merge_query_preserves_backend_arg_with_empty_value() {
        // target_url_test.go:560
        let got = build_target_url(
            "https://backend/foo/bar?baz=abc&de",
            "/x/y",
            "z=xxx",
            0,
            &[],
            false,
            "",
        );
        assert_eq!(got, "https://backend/foo/bar/x/y?baz=abc&de=&z=xxx");
    }

    #[test]
    fn merge_query_drops_clashing_client_arg_by_default() {
        // target_url_test.go:566
        let got = build_target_url(
            "https://backend/foo/bar?password=abc",
            "/x/y",
            "password=hack&qqq=www",
            0,
            &[],
            false,
            "",
        );
        assert_eq!(got, "https://backend/foo/bar/x/y?password=abc&qqq=www");
    }

    #[test]
    fn no_client_query_leaves_backend_query_untouched() {
        // target_url_test.go:555 (unmodified RawQuery, order/formatting kept as-is)
        let got = build_target_url(
            "https://backend/foo/bar/?baz=abc&de",
            "",
            "",
            0,
            &[],
            false,
            "",
        );
        assert_eq!(got, "https://backend/foo/bar/?baz=abc&de");
    }

    #[test]
    fn empty_param_query_leaves_backend_query_untouched() {
        // "&" is a non-empty string with zero decoded params; Go gates the
        // early return on decoded-param-count (target_url.go:21), so the
        // backend's own query must be kept verbatim.
        let got = build_target_url(
            "https://backend/foo/bar?baz=abc&de",
            "",
            "&",
            0,
            &[],
            false,
            "",
        );
        assert_eq!(got, "https://backend/foo/bar?baz=abc&de");
    }

    #[test]
    fn no_url_map_and_no_url_prefix_and_no_default_is_missing_route() {
        let user = UserInfo::default();
        assert!(select_route(&user, "/foo/bar", "host").is_none());
    }

    #[test]
    fn url_map_entry_with_no_match_falls_through_to_next_entry() {
        let user = UserInfo {
            url_map: vec![
                url_map(&["/api/v1/query"], &[], "http://vmselect"),
                url_map(&["/api/v1/write"], &[], "http://vminsert"),
            ],
            ..Default::default()
        };

        let route = select_route(&user, "/api/v1/write", "host").expect("route");
        assert_eq!(route.url_prefixes, ["http://vminsert".to_string()]);
    }

    #[test]
    fn src_hosts_must_also_match() {
        let user = UserInfo {
            url_prefix: Some(vec!["http://default-server".to_string()]),
            url_map: vec![url_map(&["/select/.+"], &["vmui\\..+"], "http://vmui")],
            ..Default::default()
        };

        // Path matches but host doesn't -> falls through to url_prefix.
        let route = select_route(&user, "/select/0", "other-host").expect("route");
        assert_eq!(route.url_prefixes, ["http://default-server".to_string()]);
        assert!(!route.is_default);
    }

    // --- normalize_path: ported from normalizeURL (target_url.go:132-153) ---

    #[test]
    fn normalize_root_maps_to_empty() {
        // Go maps a bare "/" to "" (PR #1554) so nothing is appended to the
        // backend path.
        assert_eq!(normalize_path("/"), "");
    }

    #[test]
    fn normalize_collapses_double_slash() {
        assert_eq!(normalize_path("//foo"), "/foo");
    }

    #[test]
    fn normalize_resolves_single_dot() {
        assert_eq!(normalize_path("/a/./b"), "/a/b");
    }

    #[test]
    fn normalize_resolves_dotdot_traversal() {
        assert_eq!(normalize_path("/select/../admin"), "/admin");
    }

    #[test]
    fn normalize_preserves_trailing_slash_on_non_root() {
        // path.Clean strips the trailing slash; normalizeURL restores it
        // (upstream #1752).
        assert_eq!(normalize_path("/a/b/"), "/a/b/");
        assert_eq!(normalize_path("/a/./b/"), "/a/b/");
    }

    #[test]
    fn normalize_forces_leading_slash() {
        assert_eq!(normalize_path("foo/bar"), "/foo/bar");
    }

    #[test]
    fn normalize_traversal_feeds_route_matching_and_target_building() {
        // Route matching: a url_map anchored on "/admin" must match the
        // cleaned path, not the raw "/select/../admin" (which would otherwise
        // dodge the route and reach the admin backend un-gated).
        let user = UserInfo {
            url_map: vec![url_map(&["/admin"], &[], "http://admin-backend")],
            ..Default::default()
        };
        let cleaned = normalize_path("/select/../admin");
        let route = select_route(&user, &cleaned, "host").expect("route");
        assert_eq!(route.url_prefixes, ["http://admin-backend".to_string()]);

        // Target building: the cleaned path is appended, not the raw one.
        let got = build_target_url("http://admin-backend", &cleaned, "", 0, &[], false, "");
        assert_eq!(got, "http://admin-backend/admin");
    }

    #[test]
    fn normalize_root_yields_no_trailing_slash_in_target() {
        // Go maps "/" to "" so the backend path is used verbatim. The raw "/"
        // would instead append a trailing slash to the backend path.
        let cleaned = normalize_path("/");
        assert_eq!(
            build_target_url("http://b/x", &cleaned, "", 0, &[], false, ""),
            "http://b/x"
        );
        assert_eq!(
            build_target_url("http://b/x", "/", "", 0, &[], false, ""),
            "http://b/x/"
        );
    }

    // --- dropPrefixParts: ported verbatim from TestDropPrefixParts (target_url_test.go:15-83) ---

    #[test]
    fn drop_prefix_parts_matches_go_table() {
        let cases: &[(&str, usize, &str)] = &[
            ("", 0, ""),
            ("", 1, ""),
            ("", 10, ""),
            ("foo", 0, "foo"),
            ("/foo", 0, "/foo"),
            ("/foo/bar", 0, "/foo/bar"),
            ("/foo/", 0, "/foo/"),
            ("/foo", 1, ""),
            ("/foo/bar", 1, "/bar"),
            ("/foo/bar/baz", 1, "/bar/baz"),
            ("foo", 1, ""),
            ("foo/bar", 1, "/bar"),
            ("foo/bar/baz", 1, "/bar/baz"),
            ("/foo/", 1, "/"),
            ("/foo/bar/", 1, "/bar/"),
            ("/foo/bar/baz/", 1, "/bar/baz/"),
            ("/foo", 2, ""),
            ("/foo/bar", 2, ""),
            ("/foo/bar/baz", 2, "/baz"),
            ("/foo/", 2, ""),
            ("/foo/bar/", 2, "/"),
            ("/foo/bar/baz/", 2, "/baz/"),
            ("/foo", 3, ""),
            ("/foo/bar", 3, ""),
            ("/foo/bar/baz", 3, ""),
            ("/foo/", 3, ""),
            ("/foo/bar/", 3, ""),
            ("/foo/bar/baz/", 3, "/"),
            ("/foo/", 4, ""),
            ("/foo/bar/", 4, ""),
            ("/foo/bar/baz/", 4, ""),
        ];
        for (path, parts, expected) in cases {
            assert_eq!(
                drop_prefix_parts(path, *parts),
                *expected,
                "drop_prefix_parts({path:?}, {parts}) mismatch"
            );
        }
    }
}
