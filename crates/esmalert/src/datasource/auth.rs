//! Auth/TLS config for the datasource client. Port of the auth half of
//! `app/vmalert/utils.AuthConfig` (flag-driven basic/bearer credentials,
//! optionally loaded from a file) plus vmalert's `-datasource.tls*` flags.

use std::fs;

use super::DsError;

/// Resolved credentials to attach to every datasource request. At most one
/// of `basic`/`bearer` is expected to be set; if both are somehow set,
/// [`super::Datasource`] prefers `basic`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AuthConfig {
    pub basic: Option<(String, String)>,
    pub bearer: Option<String>,
}

/// TLS options applied to the datasource's `reqwest` client. Port of
/// vmalert's `-datasource.tlsCAFile` / `-datasource.tlsCertFile` /
/// `-datasource.tlsKeyFile` / `-datasource.tlsServerName` /
/// `-datasource.tlsInsecureSkipVerify` flags.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TlsConfig {
    pub ca_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub server_name: Option<String>,
    pub insecure_skip_verify: bool,
}

/// Raw flag values [`AuthConfig::from_flags`] resolves into credentials.
/// Direct value flags take priority over their `*_file` counterpart, and an
/// empty string is treated the same as an unset flag (mirrors upstream's
/// `flag.String` defaults).
#[derive(Debug, Clone, Copy, Default)]
pub struct AuthFlags<'a> {
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
    pub password_file: Option<&'a str>,
    pub bearer_token: Option<&'a str>,
    pub bearer_token_file: Option<&'a str>,
}

impl AuthConfig {
    /// Builds an [`AuthConfig`] from raw flag values, reading `password`
    /// and/or `bearer_token` from their `*_file` path when the direct flag
    /// is empty/unset. Fails only if a referenced file can't be read.
    pub fn from_flags(flags: &AuthFlags<'_>) -> Result<Self, DsError> {
        let password = resolve_secret(flags.password, flags.password_file)?;
        let basic = match (non_empty(flags.username), password) {
            (Some(user), Some(pass)) => Some((user.to_string(), pass)),
            _ => None,
        };
        let bearer = resolve_secret(flags.bearer_token, flags.bearer_token_file)?;
        Ok(AuthConfig { basic, bearer })
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.filter(|v| !v.is_empty())
}

/// Returns `direct` if non-empty, else the trimmed contents of `file_path`
/// if that's non-empty, else `None`.
fn resolve_secret(
    direct: Option<&str>,
    file_path: Option<&str>,
) -> Result<Option<String>, DsError> {
    if let Some(v) = non_empty(direct) {
        return Ok(Some(v.to_string()));
    }
    match non_empty(file_path) {
        Some(path) => Ok(Some(read_secret_file(path)?)),
        None => Ok(None),
    }
}

fn read_secret_file(path: &str) -> Result<String, DsError> {
    let content = fs::read_to_string(path)
        .map_err(|e| DsError::new(format!("cannot read secret file {path:?}: {e}")))?;
    Ok(content.trim_end_matches(['\n', '\r']).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_values_build_basic_and_bearer() {
        let flags = AuthFlags {
            username: Some("alice"),
            password: Some("s3cr3t"),
            bearer_token: Some("tok"),
            ..AuthFlags::default()
        };
        let cfg = AuthConfig::from_flags(&flags).expect("from_flags failed");
        assert_eq!(cfg.basic, Some(("alice".to_string(), "s3cr3t".to_string())));
        assert_eq!(cfg.bearer, Some("tok".to_string()));
    }

    #[test]
    fn empty_flags_yield_no_auth() {
        let cfg = AuthConfig::from_flags(&AuthFlags::default()).expect("from_flags failed");
        assert_eq!(cfg, AuthConfig::default());
    }

    #[test]
    fn missing_username_suppresses_basic_even_with_password() {
        let flags = AuthFlags {
            password: Some("s3cr3t"),
            ..AuthFlags::default()
        };
        let cfg = AuthConfig::from_flags(&flags).expect("from_flags failed");
        assert_eq!(cfg.basic, None);
    }

    #[test]
    fn password_file_is_read_and_trimmed() {
        let dir = std::env::temp_dir().join(format!(
            "esmalert-auth-test-{}-{}",
            std::process::id(),
            line!()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("password.txt");
        fs::write(&path, "filepass\n").expect("write password file");

        let flags = AuthFlags {
            username: Some("bob"),
            password_file: Some(path.to_str().expect("utf8 path")),
            ..AuthFlags::default()
        };
        let cfg = AuthConfig::from_flags(&flags).expect("from_flags failed");
        assert_eq!(cfg.basic, Some(("bob".to_string(), "filepass".to_string())));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_secret_file_is_an_error() {
        let flags = AuthFlags {
            bearer_token_file: Some("/nonexistent/path/does-not-exist"),
            ..AuthFlags::default()
        };
        assert!(AuthConfig::from_flags(&flags).is_err());
    }
}
