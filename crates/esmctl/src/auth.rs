//! Request auth config for source/destination endpoints. Ports the subset of
//! `app/vmctl/auth` used by `vm-native`: basic auth, bearer token, and custom
//! `^^`-separated headers.

use reqwest::blocking::RequestBuilder;

/// Parsed auth applied to every request to an endpoint.
#[derive(Clone, Default)]
pub(crate) struct AuthConfig {
    basic: Option<(String, Option<String>)>,
    bearer: Option<String>,
    headers: Vec<(String, String)>,
}

impl AuthConfig {
    /// Builds an auth config. `headers` is a `^^`-separated list of
    /// `Key: Value` pairs (ports `auth.WithHeaders`).
    pub(crate) fn new(
        user: &str,
        password: &str,
        bearer: &str,
        headers: &str,
    ) -> Result<AuthConfig, String> {
        let basic = if !user.is_empty() || !password.is_empty() {
            let pass = if password.is_empty() {
                None
            } else {
                Some(password.to_string())
            };
            Some((user.to_string(), pass))
        } else {
            None
        };
        let bearer = if bearer.is_empty() {
            None
        } else {
            Some(bearer.to_string())
        };
        let mut parsed_headers = Vec::new();
        if !headers.is_empty() {
            for h in headers.split("^^") {
                let (k, v) = h
                    .split_once(':')
                    .ok_or_else(|| format!("cannot split header {h:?} by `:`"))?;
                parsed_headers.push((k.trim().to_string(), v.trim().to_string()));
            }
        }
        Ok(AuthConfig {
            basic,
            bearer,
            headers: parsed_headers,
        })
    }

    /// Applies the auth to a request builder.
    pub(crate) fn apply(&self, mut rb: RequestBuilder) -> RequestBuilder {
        if let Some((user, pass)) = &self.basic {
            rb = rb.basic_auth(user, pass.clone());
        }
        if let Some(token) = &self.bearer {
            rb = rb.bearer_auth(token);
        }
        for (k, v) in &self.headers {
            rb = rb.header(k.as_str(), v.as_str());
        }
        rb
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_headers() {
        let c = AuthConfig::new("", "", "", "X-A: 1^^X-B: 2").unwrap();
        assert_eq!(c.headers.len(), 2);
        assert_eq!(c.headers[0], ("X-A".to_string(), "1".to_string()));
    }

    #[test]
    fn rejects_bad_header() {
        assert!(AuthConfig::new("", "", "", "no-colon").is_err());
    }

    #[test]
    fn empty_is_noop() {
        let c = AuthConfig::new("", "", "", "").unwrap();
        assert!(c.basic.is_none() && c.bearer.is_none() && c.headers.is_empty());
    }
}
