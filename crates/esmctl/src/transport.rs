//! Shared HTTP transport concerns: TLS client configuration and a byte-rate
//! limiter. Ports the TLS-config wiring (`promauth.NewTLSConfig`/`Transport`)
//! and `app/vmctl/limiter`.

use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::blocking::{Client, ClientBuilder};

/// TLS file paths for a client. Empty strings mean "unset".
#[derive(Default)]
pub(crate) struct TlsFiles {
    pub(crate) ca_file: String,
    pub(crate) cert_file: String,
    pub(crate) key_file: String,
    /// `server_name` (SNI override) is accepted but not applied — the
    /// reqwest-blocking client offers no SNI override hook (same limitation
    /// as esmauth/esmalert/esmagent). Kept so the flag doesn't error.
    pub(crate) server_name: String,
    pub(crate) insecure_skip_verify: bool,
}

/// Builds a blocking HTTP client with the given TLS config and an optional
/// request timeout. Ports `promauth.NewTLSConfig` + `NewTLSTransport`: custom
/// CA, client certificate, and `insecure_skip_verify`.
pub(crate) fn build_client(tls: &TlsFiles, timeout: Option<Duration>) -> Result<Client, String> {
    let mut builder: ClientBuilder = Client::builder();
    if let Some(t) = timeout {
        builder = builder.timeout(t);
    }
    if tls.insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if !tls.ca_file.is_empty() {
        let pem = std::fs::read(&tls.ca_file)
            .map_err(|e| format!("cannot read CA file {:?}: {e}", tls.ca_file))?;
        let cert = reqwest::Certificate::from_pem(&pem)
            .map_err(|e| format!("invalid CA file {:?}: {e}", tls.ca_file))?;
        builder = builder.add_root_certificate(cert);
    }
    if !tls.cert_file.is_empty() || !tls.key_file.is_empty() {
        if tls.cert_file.is_empty() || tls.key_file.is_empty() {
            return Err("both a cert file and a key file must be set for client TLS".to_string());
        }
        // reqwest's rustls `Identity::from_pem` wants the cert and key in one
        // PEM blob.
        let mut pem = std::fs::read(&tls.cert_file)
            .map_err(|e| format!("cannot read cert file {:?}: {e}", tls.cert_file))?;
        let key = std::fs::read(&tls.key_file)
            .map_err(|e| format!("cannot read key file {:?}: {e}", tls.key_file))?;
        pem.push(b'\n');
        pem.extend_from_slice(&key);
        let identity = reqwest::Identity::from_pem(&pem)
            .map_err(|e| format!("invalid client cert/key: {e}"))?;
        builder = builder.identity(identity);
    }
    if !tls.server_name.is_empty() {
        log::warn!(
            "tls server_name {:?} is accepted but not applied (reqwest-blocking has no SNI override)",
            tls.server_name
        );
    }
    builder
        .build()
        .map_err(|e| format!("failed to build HTTP client: {e}"))
}

/// A leaky-bucket byte-rate limiter. Ports `limiter.Limiter`.
pub(crate) struct Limiter {
    per_second_limit: i64,
    state: Mutex<LimiterState>,
}

struct LimiterState {
    budget: i64,
    deadline: Instant,
}

impl Limiter {
    /// Creates a limiter of `per_second_limit` bytes/second (`<= 0` disables
    /// it). Ports `NewLimiter`.
    pub(crate) fn new(per_second_limit: i64) -> Limiter {
        Limiter {
            per_second_limit,
            state: Mutex::new(LimiterState {
                budget: 0,
                deadline: Instant::now(),
            }),
        }
    }

    /// Blocks until `data_len` bytes fit in the budget, then deducts them.
    /// Ports `Limiter.Register`.
    pub(crate) fn register(&self, data_len: usize) {
        let limit = self.per_second_limit;
        if limit <= 0 {
            return;
        }
        let mut st = self.state.lock().unwrap();
        while st.budget <= 0 {
            let now = Instant::now();
            if st.deadline > now {
                let d = st.deadline - now;
                // Release the lock while sleeping so other workers can also
                // account (they'll re-check the budget on wake).
                drop(st);
                std::thread::sleep(d);
                st = self.state.lock().unwrap();
            }
            st.budget += limit;
            st.deadline = Instant::now() + Duration::from_secs(1);
        }
        st.budget -= data_len as i64;
    }
}

/// Wraps a reader, throttling reads through a [`Limiter`]. Ports
/// `limiter.WriteLimiter` (applied on the read side of esmctl's streaming
/// paths).
pub(crate) struct RateLimitedReader<R> {
    inner: R,
    limiter: Arc<Limiter>,
}

impl<R: Read> RateLimitedReader<R> {
    pub(crate) fn new(inner: R, limiter: Arc<Limiter>) -> RateLimitedReader<R> {
        RateLimitedReader { inner, limiter }
    }
}

impl<R: Read> Read for RateLimitedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.limiter.register(n);
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn disabled_limiter_is_instant() {
        let lim = Limiter::new(0);
        let start = Instant::now();
        for _ in 0..1000 {
            lim.register(1_000_000);
        }
        assert!(start.elapsed() < Duration::from_millis(200));
    }

    #[test]
    fn limiter_throttles_above_budget() {
        // 1000 bytes/s budget; consuming 3000 bytes forces ~2 refills.
        let lim = Limiter::new(1000);
        let start = Instant::now();
        lim.register(1000); // first refill, budget 1000 -> 0
        lim.register(1000); // waits ~1s, refill -> 0
        assert!(start.elapsed() >= Duration::from_millis(800));
    }

    #[test]
    fn rate_limited_reader_passes_bytes_through() {
        let data = vec![7u8; 4096];
        let mut r = RateLimitedReader::new(Cursor::new(data), Arc::new(Limiter::new(0)));
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        assert_eq!(out.len(), 4096);
    }
}
