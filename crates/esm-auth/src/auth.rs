//! The `http_auth:` token map and request token extraction.
//!
//! Port of `getAuthTokens`/`getHTTPAuth{,Bearer,Basic}Token`/
//! `getAuthTokensFromRequest` (`app/vmauth/auth_config.go:1212-1283`) and the
//! lookup in `getUserInfoByAuthTokens` (`app/vmauth/main.go:218-227`).
//!
//! # Security model
//!
//! vmauth never compares credentials per request. At config load it builds a
//! map keyed by the full string `http_auth:<header value>` -> user; at
//! request time it derives the same kind of key(s) from the incoming request
//! and does a `HashMap::get`. There is no loop over the user list comparing
//! secrets, so there is no timing side-channel to worry about — [`AuthMap`]
//! preserves this by doing lookups exclusively through `HashMap::get`.

use std::collections::HashMap;

use crate::config::{AuthConfig, UserInfo};

/// A resolved auth map built once per config load: `http_auth:<value>` ->
/// user. Lookups are `HashMap::get` only — never a comparison loop over
/// [`UserInfo`] secrets.
#[derive(Debug)]
pub struct AuthMap {
    tokens: HashMap<String, usize>,
    users: Vec<UserInfo>,
    unauthorized: Option<UserInfo>,
}

impl AuthMap {
    /// Builds the map from a parsed [`AuthConfig`]. Returns `Err` on invalid
    /// per-user auth combos (the `getAuthTokens` error branches) or on a
    /// duplicate `http_auth:` token across users (`parseAuthConfigUsers`,
    /// auth_config.go:1037-1042) — the latter mirrors the same collision
    /// check upstream and matters here for the same reason: a silent
    /// overwrite would let two users share a token slot.
    pub fn build(config: AuthConfig) -> Result<AuthMap, String> {
        let AuthConfig {
            users,
            unauthorized_user,
        } = config;

        let mut tokens: HashMap<String, usize> = HashMap::with_capacity(users.len());
        for (idx, ui) in users.iter().enumerate() {
            let ats = get_auth_tokens(
                ui.auth_token.as_deref(),
                ui.bearer_token.as_deref(),
                ui.username.as_deref(),
                ui.password.as_deref(),
            )?;
            for at in ats {
                if let Some(&old_idx) = tokens.get(&at) {
                    let old = &users[old_idx];
                    return Err(format!(
                        "duplicate auth token={at:?} found for username={:?}, name={:?}; \
                         the previous one is set for username={:?}, name={:?}",
                        ui.username, ui.name, old.username, old.name
                    ));
                }
                tokens.insert(at, idx);
            }
        }

        Ok(AuthMap {
            tokens,
            users,
            unauthorized: unauthorized_user,
        })
    }

    /// Looks up a user by the request's derived candidate tokens, in order,
    /// mirroring `getUserInfoByAuthTokens`: the first token with a map hit
    /// wins. Pure `HashMap::get` — no secret comparison.
    pub fn lookup(&self, tokens: &[String]) -> Option<&UserInfo> {
        tokens
            .iter()
            .find_map(|t| self.tokens.get(t))
            .map(|&idx| &self.users[idx])
    }

    pub fn unauthorized(&self) -> Option<&UserInfo> {
        self.unauthorized.as_ref()
    }

    pub fn has_users(&self) -> bool {
        !self.users.is_empty()
    }
}

/// Treats an absent or empty string as "unset", matching Go's `!= ""` checks
/// on string-typed config fields.
fn non_empty(v: Option<&str>) -> Option<&str> {
    v.filter(|s| !s.is_empty())
}

/// Port of `getAuthTokens`: derives the `http_auth:` token(s) a single user
/// registers in the map, or an error for an invalid combination of
/// `auth_token`/`bearer_token`/`username`/`password`.
fn get_auth_tokens(
    auth_token: Option<&str>,
    bearer_token: Option<&str>,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<Vec<String>, String> {
    let auth_token = non_empty(auth_token);
    let bearer_token = non_empty(bearer_token);
    let username = non_empty(username);
    let password = non_empty(password);

    if let Some(at) = auth_token {
        if bearer_token.is_some() {
            return Err("bearer_token cannot be specified if auth_token is set".to_string());
        }
        if username.is_some() || password.is_some() {
            return Err(
                "username and password cannot be specified if auth_token is set".to_string(),
            );
        }
        return Ok(vec![http_auth_token(at)]);
    }

    if let Some(bt) = bearer_token {
        if username.is_some() || password.is_some() {
            return Err(
                "username and password cannot be specified if bearer_token is set".to_string(),
            );
        }
        // Accept the bearer token as a Basic Auth username with empty password.
        let at1 = http_auth_bearer(bt);
        let at2 = http_auth_basic(bt, "");
        return Ok(vec![at1, at2]);
    }

    if let Some(u) = username {
        let at = http_auth_basic(u, password.unwrap_or(""));
        return Ok(vec![at]);
    }

    Err("missing authorization options; bearer_token or username must be set".to_string())
}

/// Derives candidate `http_auth:...` tokens from a request's Authorization
/// header value(s) and any userinfo in the URL. Port of
/// `getAuthTokensFromRequest`; `auth_headers` are the raw values already
/// resolved from the configured header names (default: just
/// `Authorization`), and `url_userinfo` is the `(username, password)` from
/// `http://user:pass@host/path`, if present.
pub fn request_auth_tokens(
    auth_headers: &[&str],
    url_userinfo: Option<(&str, &str)>,
) -> Vec<String> {
    let mut ats = Vec::with_capacity(auth_headers.len() + 1);

    for &ah in auth_headers {
        if ah.is_empty() {
            continue;
        }
        // Handle InfluxDB's proprietary token authentication scheme as a
        // bearer token authentication.
        // See https://docs.influxdata.com/influxdb/v2.0/api/
        let rewritten = match ah.strip_prefix("Token ") {
            Some(rest) => format!("Bearer {rest}"),
            None => ah.to_string(),
        };
        ats.push(http_auth_token(&rewritten));
    }

    if let Some((username, password)) = url_userinfo {
        if !username.is_empty() {
            ats.push(http_auth_basic(username, password));
        }
    }

    ats
}

/// Port of `getHTTPAuthToken`.
pub(crate) fn http_auth_token(v: &str) -> String {
    format!("http_auth:{v}")
}

/// Port of `getHTTPAuthBearerToken`.
pub(crate) fn http_auth_bearer(t: &str) -> String {
    format!("http_auth:Bearer {t}")
}

/// Port of `getHTTPAuthBasicToken`.
pub(crate) fn http_auth_basic(username: &str, password: &str) -> String {
    let token = format!("{username}:{password}");
    format!("http_auth:Basic {}", base64_std_encode(token.as_bytes()))
}

/// Encodes bytes using the standard base64 alphabet with `=` padding
/// (`base64.StdEncoding` in Go). Hand-rolled to avoid pulling in a
/// dependency for this one narrow use (see `base64url_decode` in
/// `esm-insert/src/common.rs` for the same style, decoding a different
/// alphabet).
pub(crate) fn base64_std_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        let n = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);

        out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[((n >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(n & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::UserInfo;

    fn user(mutate: impl FnOnce(&mut UserInfo)) -> UserInfo {
        let mut ui = UserInfo::default();
        mutate(&mut ui);
        ui
    }

    // -- base64 -------------------------------------------------------------

    #[test]
    fn base64_std_encode_matches_rfc4648_vectors() {
        assert_eq!(base64_std_encode(b""), "");
        assert_eq!(base64_std_encode(b"f"), "Zg==");
        assert_eq!(base64_std_encode(b"fo"), "Zm8=");
        assert_eq!(base64_std_encode(b"foo"), "Zm9v");
        assert_eq!(base64_std_encode(b"foobar"), "Zm9vYmFy");
    }

    // -- token constructors ---------------------------------------------------

    #[test]
    fn bearer_user_registers_bearer_and_basic_keys() {
        let ats = get_auth_tokens(None, Some("tok"), None, None).expect("should build");
        assert_eq!(
            ats,
            vec![
                "http_auth:Bearer tok".to_string(),
                format!("http_auth:Basic {}", base64_std_encode(b"tok:")),
            ]
        );
    }

    #[test]
    fn basic_user_registers_basic_key() {
        let ats = get_auth_tokens(None, None, Some("alice"), Some("s3cr3t")).expect("should build");
        assert_eq!(
            ats,
            vec![format!(
                "http_auth:Basic {}",
                base64_std_encode(b"alice:s3cr3t")
            )]
        );
    }

    #[test]
    fn auth_token_user_registers_verbatim_key() {
        let ats = get_auth_tokens(Some("plain-token"), None, None, None).expect("should build");
        assert_eq!(ats, vec!["http_auth:plain-token".to_string()]);
    }

    // -- getAuthTokens error branches -----------------------------------------

    #[test]
    fn rejects_auth_token_and_bearer_token_together() {
        let err = get_auth_tokens(Some("a"), Some("b"), None, None).unwrap_err();
        assert!(err.contains("bearer_token"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_auth_token_with_username() {
        let err = get_auth_tokens(Some("a"), None, Some("u"), None).unwrap_err();
        assert!(
            err.contains("username and password"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_bearer_and_username_together() {
        let err = get_auth_tokens(None, Some("b"), Some("u"), None).unwrap_err();
        assert!(err.contains("bearer_token"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_missing_auth_options() {
        let err = get_auth_tokens(None, None, None, None).unwrap_err();
        assert!(
            err.contains("missing authorization options"),
            "unexpected error: {err}"
        );
    }

    // -- request token extraction ---------------------------------------------

    #[test]
    fn request_tokens_from_authorization_header() {
        let ats = request_auth_tokens(&["Bearer tok"], None);
        assert_eq!(ats, vec!["http_auth:Bearer tok".to_string()]);
    }

    #[test]
    fn influx_token_scheme_rewritten_to_bearer() {
        let ats = request_auth_tokens(&["Token influx-tok"], None);
        assert_eq!(ats, vec!["http_auth:Bearer influx-tok".to_string()]);
    }

    #[test]
    fn url_userinfo_becomes_basic_token() {
        let ats = request_auth_tokens(&[], Some(("alice", "s3cr3t")));
        assert_eq!(
            ats,
            vec![format!(
                "http_auth:Basic {}",
                base64_std_encode(b"alice:s3cr3t")
            )]
        );
    }

    #[test]
    fn empty_auth_header_produces_no_token() {
        let ats = request_auth_tokens(&[""], None);
        assert!(ats.is_empty());
    }

    // -- AuthMap::build / lookup -----------------------------------------------

    fn config_with_users(users: Vec<UserInfo>) -> AuthConfig {
        AuthConfig {
            users,
            unauthorized_user: None,
        }
    }

    #[test]
    fn lookup_returns_user_for_matching_bearer() {
        let ui = user(|u| {
            u.name = Some("svc".to_string());
            u.bearer_token = Some("tok".to_string());
        });
        let map = AuthMap::build(config_with_users(vec![ui])).expect("should build");

        let tokens = request_auth_tokens(&["Bearer tok"], None);
        let found = map.lookup(&tokens).expect("should find user");
        assert_eq!(found.name.as_deref(), Some("svc"));
    }

    #[test]
    fn lookup_returns_user_for_bearer_token_used_as_basic_username() {
        // Fidelity check for the dual-key registration: the bearer token is
        // also accepted as a Basic Auth username with an empty password.
        let ui = user(|u| {
            u.name = Some("svc".to_string());
            u.bearer_token = Some("tok".to_string());
        });
        let map = AuthMap::build(config_with_users(vec![ui])).expect("should build");

        let basic = format!("http_auth:Basic {}", base64_std_encode(b"tok:"));
        let found = map.lookup(&[basic]).expect("should find user");
        assert_eq!(found.name.as_deref(), Some("svc"));
    }

    #[test]
    fn lookup_returns_none_for_unknown_token() {
        let ui = user(|u| u.bearer_token = Some("tok".to_string()));
        let map = AuthMap::build(config_with_users(vec![ui])).expect("should build");

        assert!(map
            .lookup(&["http_auth:Bearer other".to_string()])
            .is_none());
    }

    #[test]
    fn build_rejects_bearer_and_username_together() {
        let ui = user(|u| {
            u.bearer_token = Some("tok".to_string());
            u.username = Some("alice".to_string());
        });
        let err = AuthMap::build(config_with_users(vec![ui])).unwrap_err();
        assert!(err.contains("bearer_token"), "unexpected error: {err}");
    }

    #[test]
    fn build_rejects_duplicate_auth_token() {
        let a = user(|u| {
            u.name = Some("a".to_string());
            u.bearer_token = Some("dup".to_string());
        });
        let b = user(|u| {
            u.name = Some("b".to_string());
            u.bearer_token = Some("dup".to_string());
        });
        let err = AuthMap::build(config_with_users(vec![a, b])).unwrap_err();
        assert!(
            err.contains("duplicate auth token"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn has_users_reflects_configured_users() {
        let empty = AuthMap::build(config_with_users(vec![])).expect("should build");
        assert!(!empty.has_users());

        let ui = user(|u| u.bearer_token = Some("tok".to_string()));
        let with_user = AuthMap::build(config_with_users(vec![ui])).expect("should build");
        assert!(with_user.has_users());
    }

    #[test]
    fn unauthorized_user_is_returned() {
        let cfg = AuthConfig {
            users: vec![],
            unauthorized_user: Some(user(|u| u.name = Some("uu".to_string()))),
        };
        let map = AuthMap::build(cfg).expect("should build");
        assert_eq!(
            map.unauthorized().and_then(|u| u.name.as_deref()),
            Some("uu")
        );
    }
}
