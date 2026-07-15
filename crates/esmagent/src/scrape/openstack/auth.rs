//! Keystone v3 authentication: the auth-request body builder (password AND
//! application-credential methods) and the service-catalog parsing that
//! resolves the Nova/compute endpoint.
//!
//! Port of `lib/promscrape/discovery/openstack/auth.go`
//! (`buildAuthRequestBody`/`buildScope`/`readCredentialsFromEnv`) plus the
//! catalog types + `getComputeEndpointURL` from the same file. The emitted JSON
//! matches upstream's `json.Marshal` output byte-for-byte (validated against
//! `auth_test.go`):
//! - The `identity` object is a Go struct, so its keys follow struct-field
//!   declaration order — reproduced here via serde struct field order.
//! - The `scope` object is a Go `map[string]any`, which `json.Marshal`
//!   serializes with keys sorted alphabetically at every level. This port does
//!   NOT rely on `serde_json`'s key ordering (the `preserve_order` feature is
//!   enabled transitively via Cargo feature unification, so `json!` objects
//!   would otherwise emit insertion order). Instead every `json!` object below
//!   is hand-written with its keys already in alphabetical order, so the output
//!   is alphabetical whether `preserve_order` is on (IndexMap, insertion order)
//!   or off (BTreeMap, sorted) — matching Go's map-key sorting either way.

use serde::{Deserialize, Serialize};

use super::OpenstackSdConfig;
use crate::scrape::config::ScrapeError;

// ---- auth response / service catalog -------------------------------------

/// Keystone identity API auth response. Port of `auth.go`'s `authResponse`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct AuthResponse {
    pub token: AuthToken,
}

/// Port of `auth.go`'s `authToken`. `expires_at` is an absolute RFC3339
/// timestamp (converted to a monotonic cache deadline in [`super::client`]).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct AuthToken {
    #[serde(rename = "expires_at")]
    pub expires_at: String,
    pub catalog: Vec<CatalogItem>,
}

/// Port of `auth.go`'s `catalogItem`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct CatalogItem {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub endpoints: Vec<Endpoint>,
}

/// Port of `auth.go`'s `endpoint`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Endpoint {
    #[serde(rename = "region_id")]
    pub region_id: String,
    #[serde(rename = "region_name")]
    pub region_name: String,
    pub url: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub interface: String,
}

/// Extracts the compute endpoint URL matching `availability` (`interface`) and
/// `region` from the Keystone catalog. Port of `getComputeEndpointURL`: an
/// empty `region` matches any endpoint; a set `region` must equal the
/// endpoint's `region_id` or `region_name`.
pub fn get_compute_endpoint_url(
    catalog: &[CatalogItem],
    availability: &str,
    region: &str,
) -> Result<String, ScrapeError> {
    for eps in catalog {
        if eps.type_ != "compute" {
            continue;
        }
        for ep in &eps.endpoints {
            if ep.interface == availability
                && (region.is_empty() || region == ep.region_id || region == ep.region_name)
            {
                return Ok(ep.url.clone());
            }
        }
    }
    Err(ScrapeError::new(format!(
        "cannot find compute url for the given availability: {availability:?}, region: {region:?}"
    )))
}

// ---- auth request body ---------------------------------------------------

#[derive(Serialize)]
struct Request {
    auth: AuthReq,
}

#[derive(Serialize)]
struct AuthReq {
    identity: IdentityReq,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<serde_json::Value>,
}

#[derive(Serialize, Default)]
struct IdentityReq {
    methods: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    password: Option<PasswordReq>,
    #[serde(
        rename = "application_credential",
        skip_serializing_if = "Option::is_none"
    )]
    application_credential: Option<ApplicationCredentialReq>,
}

#[derive(Serialize)]
struct PasswordReq {
    user: UserReq,
}

/// Field order matches Go's `userReq` (`id`, `name`, `password`, `passcode`,
/// `domain`) so the marshaled JSON matches byte-for-byte. `passcode` is never
/// set by this port, so it is omitted from the struct entirely (it would never
/// serialize regardless).
#[derive(Serialize, Default)]
struct UserReq {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    domain: Option<DomainReq>,
}

#[derive(Serialize, Default)]
struct DomainReq {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

#[derive(Serialize, Default)]
struct ApplicationCredentialReq {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<UserReq>,
    #[serde(skip_serializing_if = "Option::is_none")]
    secret: Option<String>,
}

fn non_empty(s: &str) -> bool {
    !s.is_empty()
}

/// Builds the Keystone v3 auth-request body. Faithful port of
/// `buildAuthRequestBody` — password authentication and all three
/// application-credential variants, with the same validation errors.
pub fn build_auth_request_body(sdc: &OpenstackSdConfig) -> Result<Vec<u8>, ScrapeError> {
    if sdc.password.is_none()
        && sdc.application_credential_id.is_empty()
        && sdc.application_credential_name.is_empty()
    {
        return Err(ScrapeError::new(
            "password and application credentials are missing",
        ));
    }

    let mut identity = IdentityReq::default();
    let mut scope: Option<serde_json::Value> = None;

    if sdc.password.is_none() {
        // Application-credential auth.
        if non_empty(&sdc.application_credential_id) {
            let secret = sdc
                .application_credential_secret
                .clone()
                .ok_or_else(|| ScrapeError::new("ApplicationCredentialSecret is empty"))?;
            identity.methods = vec!["application_credential".into()];
            identity.application_credential = Some(ApplicationCredentialReq {
                id: Some(sdc.application_credential_id.clone()),
                secret: Some(secret),
                ..ApplicationCredentialReq::default()
            });
            return marshal(identity, scope);
        }

        let secret = sdc.application_credential_secret.clone().ok_or_else(|| {
            ScrapeError::new(
                "missing application_credential_secret when application_credential_name is set",
            )
        })?;
        let mut user: Option<UserReq> = None;
        if non_empty(&sdc.userid) {
            // UserID can be used without domain information.
            user = Some(UserReq {
                id: Some(sdc.userid.clone()),
                ..UserReq::default()
            });
        }
        if user.is_none() && sdc.username.is_empty() {
            return Err(ScrapeError::new("username and userid is empty"));
        }
        if user.is_none() && non_empty(&sdc.domain_id) {
            user = Some(UserReq {
                name: Some(sdc.username.clone()),
                domain: Some(DomainReq {
                    id: Some(sdc.domain_id.clone()),
                    ..DomainReq::default()
                }),
                ..UserReq::default()
            });
        }
        if user.is_none() && non_empty(&sdc.domain_name) {
            user = Some(UserReq {
                name: Some(sdc.username.clone()),
                domain: Some(DomainReq {
                    name: Some(sdc.domain_name.clone()),
                    ..DomainReq::default()
                }),
                ..UserReq::default()
            });
        }
        let Some(user) = user else {
            return Err(ScrapeError::new(
                "domain_id and domain_name cannot be empty for application_credential_name auth",
            ));
        };
        identity.methods = vec!["application_credential".into()];
        identity.application_credential = Some(ApplicationCredentialReq {
            name: Some(sdc.application_credential_name.clone()),
            user: Some(user),
            secret: Some(secret),
            ..ApplicationCredentialReq::default()
        });
        return marshal(identity, scope);
    }

    // Password authentication (password is Some here).
    let password = sdc.password.clone().unwrap();
    identity.methods.push("password".into());
    if sdc.username.is_empty() && sdc.userid.is_empty() {
        return Err(ScrapeError::new(
            "username and userid is empty for username/password auth",
        ));
    }
    if non_empty(&sdc.username) {
        if non_empty(&sdc.userid) {
            return Err(ScrapeError::new("both username and userid is present"));
        }
        if sdc.domain_id.is_empty() && sdc.domain_name.is_empty() {
            return Err(ScrapeError::new(format!(
                " domain_id or domain_name is missing for username/password auth: {}",
                sdc.username
            )));
        }
        if non_empty(&sdc.domain_id) {
            if non_empty(&sdc.domain_name) {
                return Err(ScrapeError::new(
                    "both domain_id and domain_name is present",
                ));
            }
            identity.password = Some(PasswordReq {
                user: UserReq {
                    name: Some(sdc.username.clone()),
                    password: Some(password.clone()),
                    domain: Some(DomainReq {
                        id: Some(sdc.domain_id.clone()),
                        ..DomainReq::default()
                    }),
                    ..UserReq::default()
                },
            });
        }
        if non_empty(&sdc.domain_name) {
            identity.password = Some(PasswordReq {
                user: UserReq {
                    name: Some(sdc.username.clone()),
                    password: Some(password.clone()),
                    domain: Some(DomainReq {
                        name: Some(sdc.domain_name.clone()),
                        ..DomainReq::default()
                    }),
                    ..UserReq::default()
                },
            });
        }
    }
    if non_empty(&sdc.userid) {
        if non_empty(&sdc.domain_id) {
            return Err(ScrapeError::new("both user_id and domain_id is present"));
        }
        if non_empty(&sdc.domain_name) {
            return Err(ScrapeError::new("both user_id and domain_name is present"));
        }
        identity.password = Some(PasswordReq {
            user: UserReq {
                id: Some(sdc.userid.clone()),
                password: Some(password.clone()),
                ..UserReq::default()
            },
        });
    }

    scope = build_scope(sdc)?;
    marshal(identity, scope)
}

fn marshal(
    identity: IdentityReq,
    scope: Option<serde_json::Value>,
) -> Result<Vec<u8>, ScrapeError> {
    let req = Request {
        auth: AuthReq { identity, scope },
    };
    serde_json::to_vec(&req)
        .map_err(|e| ScrapeError::new(format!("cannot marshal openstack auth request: {e}")))
}

/// Builds the `scope` object for password auth. Port of `buildScope`. Returns
/// a [`serde_json::Value`] or `None` when no scope applies. Every `json!`
/// object literal below is written with its keys in alphabetical order at
/// every nesting level (`domain` before `name`, etc.) so the marshaled bytes
/// match Go's `map[string]any` key sorting regardless of `serde_json`'s
/// `preserve_order` feature — see the module comment.
fn build_scope(sdc: &OpenstackSdConfig) -> Result<Option<serde_json::Value>, ScrapeError> {
    use serde_json::json;
    if sdc.project_name.is_empty()
        && sdc.project_id.is_empty()
        && sdc.domain_id.is_empty()
        && sdc.domain_name.is_empty()
    {
        return Ok(None);
    }
    if non_empty(&sdc.project_name) {
        if sdc.domain_id.is_empty() && sdc.domain_name.is_empty() {
            return Err(ScrapeError::new("domain_id or domain_name must present"));
        }
        if non_empty(&sdc.domain_id) {
            return Ok(Some(json!({
                "project": {"domain": {"id": sdc.domain_id}, "name": sdc.project_name}
            })));
        }
        if non_empty(&sdc.domain_name) {
            return Ok(Some(json!({
                "project": {"domain": {"name": sdc.domain_name}, "name": sdc.project_name}
            })));
        }
    } else if non_empty(&sdc.project_id) {
        return Ok(Some(json!({"project": {"id": sdc.project_id}})));
    } else if non_empty(&sdc.domain_id) {
        if non_empty(&sdc.domain_name) {
            return Err(ScrapeError::new("both domain_id and domain_name present"));
        }
        return Ok(Some(json!({"domain": {"id": sdc.domain_id}})));
    } else if non_empty(&sdc.domain_name) {
        return Ok(Some(json!({"domain": {"name": sdc.domain_name}})));
    }
    Ok(None)
}

/// Reads OpenStack credentials from the standard `OS_*` environment variables,
/// used when `identity_endpoint` is unset. Port of `readCredentialsFromEnv`.
pub fn read_credentials_from_env() -> OpenstackSdConfig {
    let env = |k: &str| std::env::var(k).unwrap_or_default();
    let mut tenant_id = env("OS_TENANT_ID");
    let mut tenant_name = env("OS_TENANT_NAME");
    if let Ok(v) = std::env::var("OS_PROJECT_ID") {
        if !v.is_empty() {
            tenant_id = v;
        }
    }
    if let Ok(v) = std::env::var("OS_PROJECT_NAME") {
        if !v.is_empty() {
            tenant_name = v;
        }
    }
    let password = env("OS_PASSWORD");
    let app_secret = env("OS_APPLICATION_CREDENTIAL_SECRET");
    OpenstackSdConfig {
        identity_endpoint: env("OS_AUTH_URL"),
        username: env("OS_USERNAME"),
        userid: env("OS_USERID"),
        password: Some(password),
        project_name: tenant_name,
        project_id: tenant_id,
        domain_name: env("OS_DOMAIN_NAME"),
        domain_id: env("OS_DOMAIN_ID"),
        application_credential_name: env("OS_APPLICATION_CREDENTIAL_NAME"),
        application_credential_id: env("OS_APPLICATION_CREDENTIAL_ID"),
        application_credential_secret: Some(app_secret),
        ..OpenstackSdConfig::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(sdc: &OpenstackSdConfig) -> String {
        String::from_utf8(build_auth_request_body(sdc).unwrap()).unwrap()
    }

    /// Byte-for-byte match with upstream `auth_test.go`
    /// `TestBuildAuthRequestBody_Success` — username/password with domain_name.
    #[test]
    fn password_auth_with_domain_name_matches_upstream() {
        let sdc = OpenstackSdConfig {
            username: "some-user".into(),
            password: Some("some-password".into()),
            domain_name: "some-domain".into(),
            ..OpenstackSdConfig::default()
        };
        assert_eq!(
            body(&sdc),
            r#"{"auth":{"identity":{"methods":["password"],"password":{"user":{"name":"some-user","password":"some-password","domain":{"name":"some-domain"}}}},"scope":{"domain":{"name":"some-domain"}}}}"#
        );
    }

    /// Byte-parity for PROJECT-scoped password auth (username + password +
    /// project_name + domain_name). The `scope` is a Go `map[string]any`, so
    /// its keys are sorted alphabetically: `domain` before `name` inside
    /// `project`. Guards against regressing to `serde_json` insertion order.
    #[test]
    fn project_scoped_password_auth_has_sorted_scope_keys() {
        let sdc = OpenstackSdConfig {
            username: "some-user".into(),
            password: Some("some-password".into()),
            project_name: "some-project".into(),
            domain_name: "some-domain".into(),
            ..OpenstackSdConfig::default()
        };
        assert_eq!(
            body(&sdc),
            r#"{"auth":{"identity":{"methods":["password"],"password":{"user":{"name":"some-user","password":"some-password","domain":{"name":"some-domain"}}}},"scope":{"project":{"domain":{"name":"some-domain"},"name":"some-project"}}}}"#
        );
    }

    /// Byte-for-byte match with upstream — application-credential id + secret.
    #[test]
    fn application_credential_id_matches_upstream() {
        let sdc = OpenstackSdConfig {
            application_credential_id: "some-id".into(),
            application_credential_secret: Some("some-secret".into()),
            ..OpenstackSdConfig::default()
        };
        assert_eq!(
            body(&sdc),
            r#"{"auth":{"identity":{"methods":["application_credential"],"application_credential":{"id":"some-id","secret":"some-secret"}}}}"#
        );
    }

    /// Upstream `TestBuildAuthRequestBody_Failure` — empty config errors.
    #[test]
    fn empty_config_is_rejected() {
        let sdc = OpenstackSdConfig::default();
        assert!(build_auth_request_body(&sdc).is_err());
    }

    #[test]
    fn password_without_username_or_userid_is_rejected() {
        let sdc = OpenstackSdConfig {
            password: Some("p".into()),
            ..OpenstackSdConfig::default()
        };
        let err = build_auth_request_body(&sdc).unwrap_err();
        assert!(
            err.msg.contains("username and userid is empty"),
            "{}",
            err.msg
        );
    }

    #[test]
    fn password_username_without_domain_is_rejected() {
        let sdc = OpenstackSdConfig {
            username: "u".into(),
            password: Some("p".into()),
            ..OpenstackSdConfig::default()
        };
        assert!(build_auth_request_body(&sdc).is_err());
    }

    #[test]
    fn userid_password_auth_serializes() {
        let sdc = OpenstackSdConfig {
            userid: "uid".into(),
            password: Some("p".into()),
            ..OpenstackSdConfig::default()
        };
        assert_eq!(
            body(&sdc),
            r#"{"auth":{"identity":{"methods":["password"],"password":{"user":{"id":"uid","password":"p"}}}}}"#
        );
    }

    #[test]
    fn app_credential_id_without_secret_is_rejected() {
        let sdc = OpenstackSdConfig {
            application_credential_id: "id".into(),
            ..OpenstackSdConfig::default()
        };
        let err = build_auth_request_body(&sdc).unwrap_err();
        assert!(
            err.msg.contains("ApplicationCredentialSecret is empty"),
            "{}",
            err.msg
        );
    }

    #[test]
    fn compute_endpoint_url_matches_availability() {
        let catalog = vec![
            CatalogItem {
                type_: "compute".into(),
                endpoints: vec![Endpoint {
                    interface: "private".into(),
                    type_: "compute".into(),
                    url: "https://compute.test.local:8083/v2.1".into(),
                    ..Endpoint::default()
                }],
                ..CatalogItem::default()
            },
            CatalogItem {
                type_: "keystone".into(),
                endpoints: vec![],
                ..CatalogItem::default()
            },
        ];
        assert_eq!(
            get_compute_endpoint_url(&catalog, "private", "").unwrap(),
            "https://compute.test.local:8083/v2.1"
        );
    }

    #[test]
    fn compute_endpoint_url_errors_when_absent() {
        let catalog = vec![CatalogItem {
            type_: "keystone".into(),
            endpoints: vec![],
            ..CatalogItem::default()
        }];
        assert!(get_compute_endpoint_url(&catalog, "", "").is_err());
    }
}
