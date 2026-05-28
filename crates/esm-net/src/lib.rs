//! HTTP server scaffolding shared across EsMetrics apps.
//!
//! Wraps `axum` + `hyper` with the cross-cutting middleware EsMetrics binaries
//! need (structured request logging, basic-auth / bearer-token, request
//! metrics, graceful shutdown signal wiring). TLS termination uses `rustls`
//! by default; `native-tls` is opt-in for enterprise environments that need
//! the OS certificate store.
