# esmauth — vmauth port design

**Status:** approved 2026-07-06. Next step: implementation plan (writing-plans).

**One-line:** A standalone Rust auth proxy `esmauth`, porting the open-source
subset of VictoriaMetrics v1.146.0 `app/vmauth` — bearer/basic auth,
path/host routing, load balancing across several backends, retries, backend
health, global + per-user concurrency limits, and config hot-reload — built
on the repo's existing synchronous `esm-http` server and blocking `reqwest`
client.

## Scope

**In scope (target surface = upstream "B" + hot-reload):**
- YAML `-auth.config` fully compatible with upstream's schema for the ported
  features (see Config below), including `unauthorized_user` and hot-reload.
- Auth: `bearer_token`, and `username` + `password` (HTTP Basic). Token → user
  lookup; constant-time credential comparison.
- Routing: `url_prefix` (scalar or list), `url_map` with `src_paths` /
  `src_hosts` regex matching, `default_url`, header injection (`headers`,
  `response_headers`), query-arg merging.
- Load balancing: `least_loaded` (default) and `first_available`, with
  `-failTimeout` backend health (a backend that errors sits out until the
  timeout lapses).
- Retries: on connection errors and configured `retry_status_codes`, bounded
  by request-body buffering (`-maxRequestBodySizeToRetry`, default 16 KiB —
  larger bodies are non-retryable, matching upstream `bufferedBody.canRetry`).
- Concurrency: global `-maxConcurrentRequests` and per-user
  `max_concurrent_requests`, with `-maxQueueDuration` queueing.
- Hot-reload: SIGHUP (Unix) + `-configCheckInterval` polling (both platforms);
  in-flight requests finish on the old config (atomic swap).
- `/metrics` (esmauth's own), `/health`, `/-/reload`.

**Out of scope (record in PORTING.md "not ported"):**
- JWT auth (`jwt.go`) and OIDC (`oidc.go`).
- Backend DNS discovery (`-discoverBackendIPs`).
- Backend TLS client certs / custom CA / `tlsInsecureSkipVerify` /
  `tlsServerName` (plain HTTPS backends via reqwest's default root store ARE
  supported; the per-backend TLS *tuning* flags are not).
- WebSocket proxying, enterprise `ip_filters`, response caching, `-dryRun`
  beyond a basic config-validate exit.

## Global Constraints

- Upstream baseline **VictoriaMetrics v1.146.0** (`/home/test/refsrc/VictoriaMetrics`),
  sources `app/vmauth/{main,auth_config,target_url}.go` (~2.3k LOC ported;
  jwt/oidc excluded). Algorithmic fidelity is the goal; port the upstream
  `*_test.go` cases (`auth_config_test.go`, `target_url_test.go`,
  `main_test.go` — ~4.6k lines) as the primary safety net.
- **New workspace deps** (pin exact versions + health-check at plan time,
  listed as a plan Global Constraint): a maintained serde YAML crate (the
  ecosystem-converged `serde_yaml` fork), and `arc-swap` OR a plain
  `RwLock<Arc<AuthConfig>>` for the atomic config swap (decide at plan time —
  prefer `RwLock<Arc<_>>`, no new dep, unless a read-mostly hot path measures
  worse). `reqwest` (blocking, rustls) is already a workspace dep.
- Every crate builds warning-free on `x86_64-unknown-linux-gnu` and
  `x86_64-pc-windows-gnu`; MSVC is the release Windows artifact. `cargo fmt` +
  `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets` clean;
  files ≤ 800 lines (split by responsibility as elsewhere).
- **Security-critical.** Beyond per-task review, the plan includes a dedicated
  adversarial security-review pass. Non-negotiables: constant-time credential
  comparison; auth tokens NEVER logged unless `-logInvalidAuthTokens` is
  explicitly set (default off); no credential leakage into `/metrics` label
  values or error bodies; hop-by-hop headers stripped both directions;
  `src_paths` matching cannot be bypassed by path-normalization tricks
  (decode/normalize before matching exactly as upstream does — verify the
  order in `target_url.go`/`main.go`).
- Commit style `<type>: <description>`, no attribution trailers. After every
  push, check CI (Linux + Windows MSVC) and fix failures before proceeding.

## Architecture

Two new workspace members, mirroring the `esmetrics` (thin bin) / `esm-*`
(libs) split:

- **`crates/esm-auth`** — library, all logic, unit-testable without sockets:
  - `config` — YAML parse + validation → an immutable `AuthConfig` (users map,
    unauthorized_user, global defaults). Port of `auth_config.go`.
  - `auth` — request → authenticated `User` (or unauthorized), constant-time
    compare, token/basic extraction.
  - `route` — port of `target_url.go`: pick `url_map`/`url_prefix`, join
    prefix+path, merge query, apply header/response-header mutations.
  - `balance` — `least_loaded` / `first_available` backend selection +
    `failTimeout` health state (per-backend in-flight counter + fail-until
    instant).
  - `proxy` — the request lifecycle: auth → route → acquire concurrency
    permit → select backend → forward via reqwest → retry policy → stream
    response back. Port of `main.go`'s `requestHandler` + `bufferedBody`.
  - `reload` — `RwLock<Arc<AuthConfig>>` holder + reload-from-path; SIGHUP and
    interval polling drive it (wired in the binary).
  - `metrics` — counter handles over `esm_common::metrics`.
- **`crates/esmauth`** — binary: flag parsing (mirror `esmetrics/src/flags.rs`
  conventions), signal handling (reuse `esmetrics/src/signal.rs` pattern),
  the `esm-http` server serving the proxy handler + `/metrics` `/health`
  `/-/reload`, and the reload watcher thread.

**`esm-http` extensions (scoped, additive — existing endpoints untouched):**
1. `Head` optionally captures the full raw `(name, value)` header list (behind
   a builder/flag so the TSDB fast path keeps its current
   only-what-it-needs parsing). The proxy needs to forward all client headers
   and read all backend response headers.
2. A streaming response pass-through on `ResponseWriter`: write an arbitrary
   status + arbitrary header set, then `io::copy` the backend body through
   (chunked when the backend length is unknown; content-length passthrough
   otherwise). Hop-by-hop headers (`Connection`, `Keep-Alive`,
   `Proxy-Authenticate`, `Proxy-Authorization`, `TE`, `Trailer`,
   `Transfer-Encoding`, `Upgrade`, plus anything named in the request/response
   `Connection` header) are stripped both directions per RFC 9110/9112 —
   exactly the set upstream strips.

## Data flow (one request)

client → esm-http accept (thread-per-conn) → proxy::handle:
1. Extract credentials → `auth`: match to `User`, else `unauthorized_user`,
   else 401 + `WWW-Authenticate`. (`vmauth_http_request_errors_total{reason="invalid_auth_token"}`.)
2. `route`: match `url_map` (src_paths/src_hosts) → target `url_prefix`; no
   match and no `default_url` → 400/`missing_route`.
3. Acquire per-user then global concurrency permit (queue up to
   `-maxQueueDuration`; timeout → 429/`reject_slow_client`,
   `vmauth_concurrent_requests_limit_reached_total`).
4. Buffer body if ≤ `-maxRequestBodySizeToRetry` (retryable) else stream
   (non-retryable).
5. `balance`: pick a healthy backend by policy. Forward via reqwest with the
   mutated URL/headers.
6. On connect error or `retry_status_codes`: mark backend unhealthy
   (`failTimeout`), retry next backend if retryable; exhausted → 502.
7. Stream response through (status, filtered headers, body copy). Apply
   `response_headers`.

## Error / status mapping (upstream-faithful)
- Missing/invalid creds → 401 + `WWW-Authenticate: Basic`.
- No route match, no default → 400 (`missing_route`).
- Concurrency queue timeout → 429 (`reject_slow_client`).
- All backends failed / connect error after retries → 502.
- Client canceled mid-request → `client_canceled` counter, no backend charge.
- `-logInvalidAuthTokens` off (default): invalid-token logs omit the token.

## Observability
esmauth `/metrics` via `esm_common::metrics`, upstream names with the
`esmauth_` prefix (rename convention consistent with `esm_*`; mapping recorded
in PORTING.md):
- `esmauth_http_requests_total{path="/-/reload"}`
- `esmauth_http_request_errors_total{reason="invalid_auth_token"|"missing_route"|"reject_slow_client"|"client_canceled"}`
- `esmauth_concurrent_requests_limit_reached_total`
- per-user request counter (username as a label — usernames are operator-chosen
  config identifiers, not secrets; bearer tokens are NEVER used as labels).
Gauges (`concurrent_requests_capacity/current`, `vmauth_backend...`) are
deferred with the histogram note — the registry is counters-only today;
document as a follow-up, don't half-build a gauge type.

## Testing
- **Unit** (esm-auth): port `auth_config_test.go` (9 tests — parse, validate,
  user maps, url_map), `target_url_test.go` (6 — prefix/path/query/header
  merge), `main_test.go` (9 — request handling, retry, buffered body). These
  are the fidelity net.
- **Integration** (esmauth tests/): esm-http server + in-process mock backends
  — auth allow/deny/401, url_map routing, least-loaded distribution,
  first-available failover, retry-on-5xx, failTimeout marking, per-user +
  global concurrency limits + queue timeout, hot-reload swap (change config on
  disk → `/-/reload` → new routing takes effect, in-flight finishes on old).
- **e2e smoke:** two real in-process `esmetrics` instances behind esmauth;
  ingest via `/write` through the proxy with a write-scoped token, query via
  `/api/v1/query` with a read-scoped token, confirm balancing hit both
  backends. Both platforms per the benchmark/spot-check convention (Windows on
  agent-6, MSVC).
- **Security pass:** dedicated adversarial review — auth bypass via path
  normalization, header smuggling (duplicate/By-hop), credential leakage in
  logs/metrics/error bodies, timing side-channel on token compare, config
  reload races.

## Release
`release.yml` gains an `esmauth` artifact per platform (Linux amd64 + Windows
MSVC), packaged alongside `esmetrics`.
