//! The proxy request lifecycle: retry loop, header sanitize, buffered-body
//! retry gating, and response streaming.
//!
//! Port of `processRequest`/`tryProcessingRequest`/`bufferedBody`
//! (`app/vmauth/main.go:394-570,799-870`).
//!
//! # Deviations from upstream (deliberate)
//!
//! - **Full in-memory body buffering.** Upstream streams request bodies
//!   larger than `-maxRequestBodySizeToRetry` (16 KiB default, configurable)
//!   straight through to the backend without holding the whole thing in
//!   memory.
//!   `reqwest::blocking::Body::new` requires an owned/`'static` `Read`, and
//!   `esm_http::Body<'c>` borrows the connection's `TcpStream` for its
//!   lifetime `'c`, so there is no way to hand it to `reqwest` without
//!   copying. This port always reads the complete body into a `Vec<u8>`
//!   first. The **retry/no-retry gating** (bodies over 16 KiB are sent once
//!   and are never retried at another backend) is preserved exactly; only
//!   the *streaming* is traded away for a copy. Flagged for a follow-up if
//!   very large request bodies through esmauth become a real workload.
//! - **No same-backend "trivial network error" local retry.** Upstream
//!   special-cases OS-level errors whose message contains "broken pipe" or
//!   "reset by peer" (`netutil.IsTrivialNetworkError`, main.go:508-514) and
//!   retries once at the *same* backend before falling through to the
//!   normal next-backend retry (the `wasLocalRetry`/`goto again` dance).
//!   `reqwest`'s error messages don't carry Go's OS-string shape, so
//!   detecting this case reliably isn't feasible here; such errors instead
//!   fall through the normal next-backend retry path below, which is still
//!   correct — just an extra backend hop instead of one retry on the same
//!   backend.
//! - **`keep_original_host` is user-level only.** [`crate::route::Route`]
//!   doesn't carry a per-`url_map`-entry `keep_original_host` (that field
//!   only exists on [`UserInfo`] in this port's `config.rs`), so only
//!   `user.keep_original_host` is honored, not a per-route override. This is
//!   a pre-existing gap in `config.rs`/`route.rs`, not something in scope to
//!   fix here.
//! - **`esm_http::Method::Other` loses the original method string** (a
//!   pre-existing `esm-http` limitation — the parser doesn't retain
//!   non-standard method tokens); such requests are forwarded as `GET`.
//! - **`X-Forwarded-For` is stripped, not overwritten.** Upstream
//!   *overwrites* `X-Forwarded-For` with the real client peer address
//!   (`sanitizeRequestHeaders`, main.go:643-651), which both prevents
//!   spoofing and gives the backend an authoritative client IP.
//!   `esm_http::Request` doesn't yet expose the peer address to this layer,
//!   so authoritative peer-derived XFF is a **follow-up pending an esm-http
//!   change**. In the meantime this port strips any client-supplied
//!   `X-Forwarded-For` (plus `X-Forwarded-Host`/`X-Forwarded-Proto`) before
//!   forwarding — see [`SPOOFABLE_FORWARDED_HEADERS`] — so a client cannot
//!   spoof a value through esmauth to a backend that trusts XFF for IP
//!   allowlisting or logging. The only loss vs upstream is that the backend
//!   receives no XFF at all (rather than a trustworthy one) until the
//!   follow-up lands.
//! - **The 401 "missing authorization" special case is not implemented
//!   here.** Upstream's `processRequest` (main.go:399-405) special-cases a
//!   route-less request from a credential-less user when *other* users have
//!   credentials configured, answering 401 instead of the generic
//!   missing-route 400. That decision needs the whole `AuthMap` (how many
//!   users have credentials), which this function doesn't have access to
//!   (it only sees the already-resolved `user`); it belongs to the caller
//!   that owns `AuthMap` (the `esmauth` binary, T8).
//! - **Configured response headers are applied before, not after,
//!   hop-by-hop stripping.** Upstream applies `hc.ResponseHeaders`
//!   (`updateHeadersByConfig`, main.go:547) strictly after
//!   `removeHopHeaders`+`copyHeader`, and never strips again — so an admin
//!   who explicitly configures a hop-by-hop-named response header (e.g.
//!   `response_headers: ["Connection: keep-alive"]`, an unusual config)
//!   would see it reach the client. Here, `apply_header_config` runs on the
//!   backend's response headers first, and the merged list is then handed
//!   to [`ResponseWriter::stream_response`], which unconditionally strips
//!   hop-by-hop headers (that's its contract — see its doc comment in
//!   `esm-http`). A configured hop-by-hop-named response header is
//!   therefore always dropped here, whereas upstream would forward it.
//!   Safer (protects the always-chunked framing this port re-establishes
//!   per response) but a real, if obscure, behavioral difference.

use std::io::{self, Read};
use std::sync::Arc;
use std::time::Instant;

use esm_http::{Method, Request, ResponseWriter};

use crate::balance::BackendPool;
use crate::config::UserInfo;
use crate::metrics;
use crate::route::{build_target_url, normalize_path, select_route};

/// `-maxRequestBodySizeToRetry` default (main.go:53): request bodies at or
/// under this size are buffered and can be retried at another backend on
/// failure; larger bodies are sent once and are not retried. This is only
/// the *default* — the effective, runtime cap is `Proxy`'s own
/// `max_retry_body_size` field, set from the flag value at [`Proxy::new`]
/// time. Only referenced by tests exercising the default (16 KiB) behavior;
/// `#[cfg(test)]` because it would otherwise be dead code in the lib target.
#[cfg(test)]
const MAX_RETRY_BODY_SIZE: usize = 16 * 1024;

/// Hard ceiling on the total request body this proxy will buffer in memory.
/// Because this port always fully buffers the body (see the module doc's
/// "Full in-memory body buffering" deviation), an unbounded body would be a
/// memory-exhaustion vector — the real workload (`remote_write`) is
/// routinely over the retry cap, so the non-retryable path must be capped.
/// Set to 32 MiB to match the repo-wide ingestion ceiling
/// `esm_protoparser::util::MAX_INSERT_REQUEST_SIZE`; a body exceeding it gets
/// a `413 Payload Too Large` and is never fully buffered. This is an
/// absolute ceiling: [`Proxy::new`] clamps `-maxRequestBodySizeToRetry` to
/// this value, so a config that sets the retry threshold above the ceiling
/// has no effect beyond the ceiling itself.
const MAX_REQUEST_BODY_SIZE: usize = 32 * 1024 * 1024;

/// Hop-by-hop headers (RFC 9110 §7.6.1 / RFC 7230 §6.1), stripped both
/// directions. Port of `hopHeaders` (main.go:679-690).
const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "proxy-connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// Client-supplied `X-Forwarded-*` headers that this proxy strips before
/// forwarding, so a client cannot spoof them. Upstream *overwrites*
/// `X-Forwarded-For` with the real peer address (main.go:643-651); this port
/// can't yet derive the peer address (see the module doc's "No
/// `X-Forwarded-For`" note / the follow-up on `esm-http` peer exposure), so
/// it does the safe thing in the meantime: drop any client-provided value so
/// a backend that trusts XFF for IP allowlisting/logging can't be spoofed
/// through esmauth. `X-Forwarded-Host`/`X-Forwarded-Proto` are stripped for
/// the same reason while we're already iterating.
const SPOOFABLE_FORWARDED_HEADERS: &[&str] =
    &["x-forwarded-for", "x-forwarded-host", "x-forwarded-proto"];

/// The proxy engine: a `reqwest` client used to forward requests to
/// backends. Holds no per-user/per-route state — callers are responsible
/// for building/caching the [`BackendPool`] a route resolves to (via
/// `pool_for` in [`Proxy::proxy`]).
pub struct Proxy {
    client: reqwest::blocking::Client,
    /// `-maxRequestBodySizeToRetry`, clamped to [`MAX_REQUEST_BODY_SIZE`] (see
    /// that constant's doc comment): request bodies at or under this size
    /// are buffered and can be retried at another backend on failure; larger
    /// bodies are sent once and are not retried.
    max_retry_body_size: usize,
}

impl Proxy {
    /// `max_retry_body_size` is the parsed `-maxRequestBodySizeToRetry` flag
    /// value; it is clamped to [`MAX_REQUEST_BODY_SIZE`] so a config that sets
    /// the retry threshold above the hard buffering ceiling can't grow the
    /// ceiling itself.
    pub fn new(client: reqwest::blocking::Client, max_retry_body_size: usize) -> Proxy {
        Proxy {
            client,
            max_retry_body_size: max_retry_body_size.min(MAX_REQUEST_BODY_SIZE),
        }
    }

    /// Handles one already-authenticated request against `user`'s resolved
    /// route. `pool_for` resolves (and, per the caller's own caching policy,
    /// caches) the [`BackendPool`] for a route's `url_prefixes`. Runs the
    /// `maxAttempts` retry loop — port of `processRequest`
    /// (main.go:394-462) — and streams the winning response into `w`.
    ///
    /// # Precondition
    ///
    /// `req` must come from an [`esm_http::Server`] configured with
    /// `ServerConfig { capture_all_headers: true, .. }` (the default is
    /// `false`). Without it, [`Request::all_headers`] is always empty and
    /// this forwards essentially no client request headers to the backend —
    /// silently, with no error.
    pub fn proxy(
        &self,
        user: &UserInfo,
        pool_for: &dyn Fn(&[String]) -> Arc<BackendPool>,
        req: &mut Request<'_>,
        w: &mut ResponseWriter<'_>,
    ) {
        let raw_label = user_label(user);
        let metric_label = if raw_label.is_empty() {
            None
        } else {
            Some(raw_label.as_str())
        };
        metrics::user_requests(metric_label).inc();
        let display_name = if raw_label.is_empty() {
            "unauthorized".to_string()
        } else {
            raw_label
        };

        let host = req.host().to_string();
        // Port of `u := normalizeURL(r.URL)` (main.go:395): clean the request
        // path with path.Clean semantics BEFORE it is used for route matching
        // (`select_route`), target-URL construction (`build_target_url`), and
        // the default-route `request_path` / missing-route message
        // (`full_request_uri`) — exactly the places Go uses `u`. Without this,
        // paths like `/select/../admin` would route and forward un-cleaned.
        let path = normalize_path(req.path());
        let query = req.query().to_string();
        let full_request_uri = if query.is_empty() {
            path.clone()
        } else {
            format!("{path}?{query}")
        };

        let Some(route) = select_route(user, &path, &host) else {
            metrics::missing_route_requests().inc();
            write_error(
                w,
                400,
                &format!("user {display_name:?} missing route for {full_request_uri:?}"),
            );
            return;
        };

        let pool = pool_for(route.url_prefixes);
        let max_attempts = pool.len();
        if max_attempts == 0 {
            write_error(w, 502, "no backends are configured for the resolved route");
            return;
        }

        let reqwest_method = map_method(req.method());

        let body = match read_buffered_body(req, self.max_retry_body_size) {
            Ok(BodyReadResult::Buffered(body)) => body,
            Ok(BodyReadResult::TooLarge) => {
                write_error(
                    w,
                    413,
                    &format!(
                        "request body exceeds the {MAX_REQUEST_BODY_SIZE}-byte limit for proxying"
                    ),
                );
                return;
            }
            Err(err) => {
                write_error(w, 400, &format!("cannot read request body: {err}"));
                return;
            }
        };

        let all_headers = req.all_headers().to_vec();
        let mut request_headers = sanitize_request_headers(&all_headers);
        set_default_user_agent(&mut request_headers);
        apply_header_config(&mut request_headers, route.headers);

        let mut header_map = to_header_map(&request_headers);
        if let Some(host_value) = resolve_host_header(user, route.headers, &host) {
            if let Ok(value) = reqwest::header::HeaderValue::from_str(&host_value) {
                header_map.insert(reqwest::header::HOST, value);
            }
        }

        for _ in 0..max_attempts {
            let Some(backend) = pool.select(Instant::now()) else {
                break;
            };
            let target = build_target_url(
                backend.url(),
                &path,
                &query,
                route.drop_src_path_prefix_parts.unwrap_or(0),
                route.merge_query_args,
                route.is_default,
                &full_request_uri,
            );

            let outcome = self.attempt(
                reqwest_method.clone(),
                &target,
                &header_map,
                &body.bytes,
                route.retry_status_codes,
                route.response_headers,
                w,
            );
            backend.release();

            match outcome {
                AttemptOutcome::Success => return,
                AttemptOutcome::ClientCanceled => {
                    metrics::client_canceled_requests().inc();
                    return;
                }
                AttemptOutcome::ConnectError(err) => {
                    // Upstream marks the backend broken in BOTH the
                    // retryable and non-retryable connect-error branches
                    // (main.go:495-522: `bu.setBroken()` is called inside
                    // the `!canRetry` branch itself, and the retryable
                    // branch falls through to the caller's own
                    // `bu.setBroken()`).
                    backend.set_broken(Instant::now() + pool.fail_timeout());
                    if !body.can_retry {
                        write_error(
                            w,
                            503,
                            &format!(
                                "cannot proxy the request to {}: {err}",
                                redact_url_userinfo(&target)
                            ),
                        );
                        return;
                    }
                }
                AttemptOutcome::RetryStatus(status) => {
                    // Upstream is asymmetric here (main.go:522-540): the
                    // `retryStatusCodes` + `!canRetry` branch does NOT call
                    // `bu.setBroken()` (only the retryable branch does, via
                    // the caller's post-loop `bu.setBroken()`) — a backend
                    // that answered with a normal HTTP response is not
                    // marked broken just because the client's body
                    // couldn't be replayed elsewhere. Mirrored exactly:
                    // `set_broken` must run only on the retry path below.
                    if !body.can_retry {
                        write_error(
                            w,
                            503,
                            &format!(
                                "got response status code={status} from {}, but cannot \
                                 retry the request at another backend, because the request body \
                                 has been already consumed",
                                redact_url_userinfo(&target)
                            ),
                        );
                        return;
                    }
                    backend.set_broken(Instant::now() + pool.fail_timeout());
                }
            }
        }

        write_error(
            w,
            502,
            &format!(
                "all the {max_attempts} backends for the user {display_name:?} are unavailable \
                 for proxying the request"
            ),
        );
    }

    /// One attempt against a single backend. Port of `tryProcessingRequest`
    /// (main.go:465-570), minus the `canRetry` gating (the caller decides
    /// whether a non-success outcome gets a 503 written or another attempt).
    #[allow(clippy::too_many_arguments)]
    fn attempt(
        &self,
        method: reqwest::Method,
        target: &str,
        headers: &reqwest::header::HeaderMap,
        body: &[u8],
        retry_status_codes: &[u16],
        response_header_config: &[(String, String)],
        w: &mut ResponseWriter<'_>,
    ) -> AttemptOutcome {
        let result = self
            .client
            .request(method, target)
            .headers(headers.clone())
            .body(body.to_vec())
            .send();

        let response = match result {
            Ok(response) => response,
            Err(err) => return AttemptOutcome::ConnectError(err.to_string()),
        };

        let status = response.status().as_u16();
        if retry_status_codes.contains(&status) {
            return AttemptOutcome::RetryStatus(status);
        }

        let mut response_headers: Vec<(String, String)> = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    value.to_str().unwrap_or("").to_string(),
                )
            })
            .collect();
        apply_header_config(&mut response_headers, response_header_config);

        let mut body = response;
        match w.stream_response(status, &response_headers, &mut body) {
            Ok(()) => AttemptOutcome::Success,
            // Either the client vanished mid-response or the backend body
            // read failed; `ResponseWriter::stream_response` doesn't
            // distinguish which side failed. Best-effort client-cancel
            // detection per the brief: treat any write-to-client failure as
            // a canceled client rather than retrying (the response has
            // already been partially written, so another backend can't
            // recover it anyway).
            Err(_) => AttemptOutcome::ClientCanceled,
        }
    }
}

enum AttemptOutcome {
    Success,
    ClientCanceled,
    ConnectError(String),
    RetryStatus(u16),
}

/// The buffered request body plus whether it can be resent to another
/// backend. Port of `bufferedBody`/`newBufferedBody`/`canRetry`
/// (main.go:799-870), adapted to always fully buffer (see the module doc's
/// "Full in-memory body buffering" deviation): `can_retry` is `true` iff the
/// body's total size is `<= max_retry_body_size` (the effective, clamped
/// `-maxRequestBodySizeToRetry`), matching `bufferedBody.canRetry`'s
/// `len(bb.buf) <= maxRetrySize` check for the case where the whole body fit
/// in the initial read.
struct BufferedRequestBody {
    bytes: Vec<u8>,
    can_retry: bool,
}

/// Outcome of reading the request body: either a fully-buffered body (with
/// its retryability), or the body exceeded [`MAX_REQUEST_BODY_SIZE`] and was
/// not buffered further (caller answers `413`).
enum BodyReadResult {
    Buffered(BufferedRequestBody),
    TooLarge,
}

/// Reads the whole request body, deciding retryability by whether it fits
/// within `max_retry_body_size` (the effective, already-clamped
/// `-maxRequestBodySizeToRetry`; see [`Proxy::new`]), and bounding total
/// buffering at [`MAX_REQUEST_BODY_SIZE`]. Reads up to the retry cap first;
/// if that cap is hit, probes for one more byte to distinguish "exactly at
/// the cap" (still retryable) from "more data follows" (not), then drains the
/// rest — but stops and reports [`BodyReadResult::TooLarge`] the moment the
/// buffered total would exceed the hard [`MAX_REQUEST_BODY_SIZE`] ceiling, so
/// a hostile or oversized body can't exhaust memory. Because
/// `max_retry_body_size <= MAX_REQUEST_BODY_SIZE` (enforced by the clamp in
/// [`Proxy::new`]), the retryable (small-body) path never trips the ceiling.
fn read_buffered_body(
    req: &mut Request<'_>,
    max_retry_body_size: usize,
) -> io::Result<BodyReadResult> {
    let body = req.body();
    let mut buf = vec![0u8; max_retry_body_size];
    let mut filled = 0usize;
    while filled < buf.len() {
        let n = body.read(&mut buf[filled..])?;
        if n == 0 {
            buf.truncate(filled);
            return Ok(BodyReadResult::Buffered(BufferedRequestBody {
                bytes: buf,
                can_retry: true,
            }));
        }
        filled += n;
    }

    let mut probe = [0u8; 1];
    if body.read(&mut probe)? == 0 {
        // Exactly max_retry_body_size bytes total: still retryable.
        return Ok(BodyReadResult::Buffered(BufferedRequestBody {
            bytes: buf,
            can_retry: true,
        }));
    }
    buf.push(probe[0]);

    let mut chunk = [0u8; 16 * 1024];
    loop {
        let n = body.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        if buf.len() + n > MAX_REQUEST_BODY_SIZE {
            // Stop buffering immediately; do not grow past the ceiling.
            return Ok(BodyReadResult::TooLarge);
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Ok(BodyReadResult::Buffered(BufferedRequestBody {
        bytes: buf,
        can_retry: false,
    }))
}

fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP_HEADERS
        .iter()
        .any(|h| name.eq_ignore_ascii_case(h))
}

/// Port of `sanitizeRequestHeaders` (main.go:638-653). `Authorization` is
/// deliberately NOT stripped: it isn't in upstream's hop-by-hop list, so
/// upstream forwards it to the backend verbatim, and esmauth's own auth
/// decision has already been made earlier in the pipeline by this point.
///
/// Client-supplied `X-Forwarded-*` headers ARE stripped
/// ([`SPOOFABLE_FORWARDED_HEADERS`]) so a client can't spoof them through the
/// proxy. Upstream instead overwrites `X-Forwarded-For` with the real peer
/// address; deriving the peer address needs an `esm-http` change (a
/// documented follow-up), so stripping is the safe interim behavior.
fn sanitize_request_headers(all_headers: &[(String, String)]) -> Vec<(String, String)> {
    // Headers named in the "Connection" header's comma-separated value are
    // hop-by-hop too (RFC 7230 §6.1), in addition to the fixed list.
    let connection_options: Vec<&str> = all_headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("connection"))
        .flat_map(|(_, value)| value.split(','))
        .map(str::trim)
        .filter(|opt| !opt.is_empty())
        .collect();

    all_headers
        .iter()
        // `Host` is a dedicated field on Go's http.Request, never part of
        // its Header map; esm_http's flat `all_headers()` captures it as a
        // regular header, so it must be excluded here to match. It's
        // resolved separately by `resolve_host_header`.
        .filter(|(name, _)| !name.eq_ignore_ascii_case("host"))
        .filter(|(name, _)| !is_hop_by_hop(name))
        .filter(|(name, _)| !is_spoofable_forwarded_header(name))
        .filter(|(name, _)| {
            !connection_options
                .iter()
                .any(|opt| name.eq_ignore_ascii_case(opt))
        })
        .cloned()
        .collect()
}

fn is_spoofable_forwarded_header(name: &str) -> bool {
    SPOOFABLE_FORWARDED_HEADERS
        .iter()
        .any(|h| name.eq_ignore_ascii_case(h))
}

/// Port of `updateHeadersByConfig` (main.go:625-632): an empty configured
/// value deletes all occurrences of that header; a non-empty value replaces
/// all occurrences with a single one.
fn apply_header_config(headers: &mut Vec<(String, String)>, config: &[(String, String)]) {
    for (name, value) in config {
        headers.retain(|(n, _)| !n.eq_ignore_ascii_case(name));
        if !value.is_empty() {
            headers.push((name.clone(), value.clone()));
        }
    }
}

/// Port of `req.Header.Set("User-Agent", "vmauth")` (main.go:471), renamed
/// per the brief. Applied before `apply_header_config` so a user-configured
/// `User-Agent` header (via `headers:`) can still override it, matching
/// upstream's call order.
fn set_default_user_agent(headers: &mut Vec<(String, String)>) {
    headers.retain(|(name, _)| !name.eq_ignore_ascii_case("user-agent"));
    headers.push(("User-Agent".to_string(), "esmauth".to_string()));
}

/// Port of the Host-handling branch (main.go:472-479). `None` means "leave
/// it to the HTTP client", matching upstream's default `req.Host =
/// targetURL.Host` (a no-op to state explicitly, since the request already
/// targets that URL).
fn resolve_host_header(
    user: &UserInfo,
    route_headers: &[(String, String)],
    original_host: &str,
) -> Option<String> {
    if user.keep_original_host == Some(true) {
        return Some(original_host.to_string());
    }
    route_headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("host"))
        .map(|(_, value)| value.clone())
}

fn to_header_map(headers: &[(String, String)]) -> reqwest::header::HeaderMap {
    let mut map = reqwest::header::HeaderMap::new();
    for (name, value) in headers {
        let (Ok(name), Ok(value)) = (
            reqwest::header::HeaderName::from_bytes(name.as_bytes()),
            reqwest::header::HeaderValue::from_str(value),
        ) else {
            continue;
        };
        map.append(name, value);
    }
    map
}

fn map_method(method: Method) -> reqwest::Method {
    match method {
        Method::Get => reqwest::Method::GET,
        Method::Head => reqwest::Method::HEAD,
        Method::Post => reqwest::Method::POST,
        Method::Put => reqwest::Method::PUT,
        Method::Delete => reqwest::Method::DELETE,
        Method::Options => reqwest::Method::OPTIONS,
        Method::Patch => reqwest::Method::PATCH,
        Method::Other => reqwest::Method::GET,
    }
}

fn write_error(w: &mut ResponseWriter<'_>, status: u16, message: &str) {
    w.set_status(status);
    w.write_body(message.as_bytes());
}

/// Strips any `user:pass@` userinfo from a URL's authority before it is placed
/// in a **client-facing** error body. An operator may legitimately configure a
/// backend `url_prefix` with embedded credentials (`http://user:pass@host` — a
/// valid way to authenticate to the backend); those credentials must never be
/// disclosed to the client in a 5xx body. The host (and the rest of the URL)
/// is preserved so the error still identifies which backend failed. Inputs
/// that don't parse as `scheme://authority/...` (or carry no userinfo) are
/// returned unchanged.
fn redact_url_userinfo(url: &str) -> String {
    let Some((scheme, after_scheme)) = url.split_once("://") else {
        return url.to_string();
    };
    // The authority runs from just after "://" up to the first '/', '?', or
    // '#'. Userinfo (if any) is everything up to and including the '@' within
    // that authority — a host cannot contain an unencoded '@'.
    let authority_end = after_scheme
        .find(['/', '?', '#'])
        .unwrap_or(after_scheme.len());
    let authority = &after_scheme[..authority_end];
    let Some(at) = authority.find('@') else {
        return url.to_string();
    };
    let rest = &after_scheme[at + 1..];
    format!("{scheme}://{rest}")
}

/// Port of `UserInfo.name()` (auth_config.go:1191-1209), minus JWT (out of
/// scope for this port): the configured `name`, else `username`, else a
/// one-way XXH64 hash of the bearer/auth token (never the raw secret; see
/// the crate-wide security note carried from the T4 review). Empty string
/// when the user has neither — the anonymous/unauthorized fallback case.
fn user_label(user: &UserInfo) -> String {
    if let Some(name) = non_empty(user.name.as_deref()) {
        return name.to_string();
    }
    if let Some(username) = non_empty(user.username.as_deref()) {
        return username.to_string();
    }
    if let Some(bearer) = non_empty(user.bearer_token.as_deref()) {
        return format!(
            "bearer_token:hash:{:016X}",
            xxhash_rust::xxh64::xxh64(bearer.as_bytes(), 0)
        );
    }
    if let Some(auth_token) = non_empty(user.auth_token.as_deref()) {
        return format!(
            "auth_token:hash:{:016X}",
            xxhash_rust::xxh64::xxh64(auth_token.as_bytes(), 0)
        );
    }
    String::new()
}

fn non_empty(v: Option<&str>) -> Option<&str> {
    v.filter(|s| !s.is_empty())
}

#[cfg(test)]
#[path = "proxy/tests.rs"]
mod tests;
