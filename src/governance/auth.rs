//! Authentication.
//!
//! Two schemes, one interface. [`Authenticator`] is what the request path
//! depends on, so swapping API keys for JWT (or adding OIDC introspection
//! later) is a wiring change, not a hot-path change.
//!
//! Raw keys are never stored at runtime: they are hashed at boot and the
//! digest is what gets compared, in constant time. A leaked memory dump yields
//! digests, not credentials.

use std::sync::Arc;

use axum::http::HeaderMap;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::config::{AuthConfig, AuthMode};

/// Capability required to stream. Deny-by-default: a token with no scopes
/// gets no scopes.
pub const SCOPE_CHAT_STREAM: &str = "chat:stream";
pub const SCOPE_MODELS_READ: &str = "models:read";
pub const SCOPE_ADMIN: &str = "admin";

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("missing credentials: supply `{header}` or `Authorization: Bearer <key>`")]
    Missing { header: String },
    #[error("invalid credentials")]
    Invalid,
    #[error("credential is not valid for this gateway")]
    Unknown,
    #[error("token has expired")]
    Expired,
    #[error("token is not yet valid")]
    NotYetValid,
    #[error("token audience/issuer mismatch")]
    WrongAudience,
    #[error("token signature is invalid")]
    BadSignature,
    #[error("token is missing required scope `{0}`")]
    MissingScope(String),
    #[error("authentication is disabled on this gateway")]
    Disabled,
    #[error("malformed token: {0}")]
    Malformed(String),
}

/// Who is calling, and what they are allowed to do.
#[derive(Debug, Clone)]
pub struct Principal {
    pub tenant_id: Arc<str>,
    /// Non-secret identifier for the credential, safe to log.
    pub key_id: Arc<str>,
    pub scopes: Arc<Vec<Scope>>,
    /// `api_key` or `jwt`.
    pub scheme: Arc<str>,
}

impl Principal {
    pub fn anonymous() -> Self {
        Self {
            tenant_id: "anonymous".into(),
            key_id: "anonymous".into(),
            scopes: Arc::new(Vec::new()),
            scheme: "anonymous".into(),
        }
    }

    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s.name == scope || s.wildcard)
    }

    /// Tenants created before scopes existed have a wildcard. Represented as a
    /// single entry so logging stays legible.
    pub fn describe(&self) -> String {
        format!(
            "tenant={} key={} scopes=[{}] scheme={}",
            self.tenant_id,
            self.key_id,
            self.scopes
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>()
                .join(","),
            self.scheme
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Scope {
    pub name: String,
    #[serde(default)]
    pub wildcard: bool,
}

impl Scope {
    pub fn exact(name: impl Into<String>) -> Self {
        Self { name: name.into(), wildcard: false }
    }
}

#[async_trait::async_trait]
pub trait Authenticator: Send + Sync {
    /// Human-readable name for startup logs.
    fn scheme(&self) -> &'static str;

    async fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthError>;
}

// ---------------------------------------------------------------------------
// API keys
// ---------------------------------------------------------------------------

/// A configured credential: the SHA-256 of the secret, plus metadata.
#[derive(Debug, Clone)]
pub struct KeyRecord {
    pub key_id: String,
    pub tenant_id: String,
    pub digest: [u8; 32],
    pub scopes: Vec<Scope>,
    pub enabled: bool,
}

impl KeyRecord {
    pub fn matches(&self, presented: &[u8]) -> bool {
        bool::from(self.digest.ct_eq(presented))
    }
}

pub fn key_digest(secret: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hasher.finalize().into()
}

/// Linear scan over digests with constant-time comparison. Key counts here are
/// in the tens-to-hundreds, so this is both fast enough and immune to the
/// timing oracle a per-byte `==` would give an attacker.
pub struct ApiKeyAuthenticator {
    records: Vec<KeyRecord>,
    api_key_header: String,
    accept_bearer: bool,
    fallback: Option<Arc<dyn Authenticator>>,
    allow_anonymous: bool,
}

impl ApiKeyAuthenticator {
    pub fn new(
        records: Vec<KeyRecord>,
        cfg: &AuthConfig,
        fallback: Option<Arc<dyn Authenticator>>,
    ) -> Self {
        Self {
            records,
            api_key_header: cfg.api_key_header.to_ascii_lowercase(),
            accept_bearer: cfg.accept_bearer,
            fallback,
            allow_anonymous: cfg.allow_anonymous,
        }
    }

    pub fn len(&self) -> usize {
        self.records.iter().filter(|r| r.enabled).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Where a presented secret came from. `Authorization` is ambiguous: it
    /// carries both API keys and JWTs, so a key miss there must be retried
    /// against the token authenticator.
    fn presented_secret<'a>(&self, headers: &'a HeaderMap) -> (Option<&'a str>, PresentedVia) {
        if let Some(v) = headers.get(&self.api_key_header) {
            if let Ok(s) = v.to_str() {
                let s = s.trim();
                if !s.is_empty() {
                    return (Some(s), PresentedVia::ApiKeyHeader);
                }
            }
        }
        if self.accept_bearer {
            if let Some(v) = headers.get(axum::http::header::AUTHORIZATION) {
                if let Ok(s) = v.to_str() {
                    if let Some(token) = s
                        .strip_prefix("Bearer ")
                        .or_else(|| s.strip_prefix("bearer "))
                    {
                        let token = token.trim();
                        if !token.is_empty() {
                            return (Some(token), PresentedVia::Authorization);
                        }
                    }
                }
            }
        }
        (None, PresentedVia::ApiKeyHeader)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PresentedVia {
    ApiKeyHeader,
    Authorization,
}

#[async_trait::async_trait]
impl Authenticator for ApiKeyAuthenticator {
    fn scheme(&self) -> &'static str {
        "api_key"
    }

    async fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
        let (secret, via) = self.presented_secret(headers);

        if let Some(secret) = secret {
            let digest = key_digest(secret);
            if let Some(rec) = self.records.iter().find(|r| r.enabled && r.matches(&digest)) {
                return Ok(Principal {
                    tenant_id: rec.tenant_id.as_str().into(),
                    key_id: rec.key_id.as_str().into(),
                    scopes: Arc::new(rec.scopes.clone()),
                    scheme: "api_key".into(),
                });
            }
        }

        // A key miss on an explicit key header is terminal. A miss in
        // `Authorization` might just be a JWT we were not configured to see
        // inline, so hand it to the token authenticator.
        if via == PresentedVia::Authorization {
            if let Some(next) = &self.fallback {
                return next.authenticate(headers).await;
            }
        }
        if self.allow_anonymous {
            return Ok(Principal::anonymous());
        }
        match secret {
            None => Err(AuthError::Missing {
                header: self.api_key_header.clone(),
            }),
            Some(_) => Err(AuthError::Invalid),
        }
    }
}

// ---------------------------------------------------------------------------
// JWT (HS256)
// ---------------------------------------------------------------------------

/// Verifies `alg=HS256` tokens. Symmetric only, on purpose: a gateway that
/// verifies its own tenants' tokens should not be forwarding trust to a
/// third-party JWKS endpoint on the hot path.
pub struct JwtAuthenticator {
    secret: Vec<u8>,
    issuer: String,
    audience: String,
    leeway_secs: u64,
    now: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl JwtAuthenticator {
    pub fn new(secret: &str, cfg: &crate::config::JwtConfig) -> Self {
        Self {
            secret: secret.as_bytes().to_vec(),
            issuer: cfg.issuer.clone(),
            audience: cfg.audience.clone(),
            leeway_secs: cfg.leeway_secs,
            now: Box::new(unix_now),
        }
    }

    /// Overridable clock so expiry tests do not have to sleep.
    pub fn with_clock(mut self, now: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        self.now = Box::new(now);
        self
    }

    fn verify(&self, token: &str) -> Result<Principal, AuthError> {
        let mut parts = token.split('.');
        let (Some(header_b64), Some(claims_b64), Some(sig_b64), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(AuthError::Malformed(
                "expected three dot-separated segments".into(),
            ));
        };

        let header_raw = b64_decode(header_b64)?;
        let header: serde_json::Value = serde_json::from_slice(&header_raw)
            .map_err(|e| AuthError::Malformed(format!("header: {e}")))?;
        let alg = header.get("alg").and_then(|v| v.as_str()).unwrap_or_default();
        if alg != "HS256" {
            return Err(AuthError::Malformed(format!(
                "unsupported alg `{alg}` (this gateway verifies HS256 only)"
            )));
        }

        let signing_input = format!("{header_b64}.{claims_b64}");
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret)
            .map_err(|_| AuthError::BadSignature)?;
        mac.update(signing_input.as_bytes());
        let expected = mac.finalize().into_bytes();
        let presented = b64_decode(sig_b64)?;
        if !bool::from(expected.ct_eq(&presented)) {
            return Err(AuthError::BadSignature);
        }

        let claims_raw = b64_decode(claims_b64)?;
        let claims: serde_json::Value = serde_json::from_slice(&claims_raw)
            .map_err(|e| AuthError::Malformed(format!("claims: {e}")))?;

        let now = (self.now)();
        if let Some(exp) = claims.get("exp").and_then(|v| v.as_u64())
            && now > exp.saturating_add(self.leeway_secs)
        {
            return Err(AuthError::Expired);
        }
        if let Some(nbf) = claims.get("nbf").and_then(|v| v.as_u64())
            && now.saturating_add(self.leeway_secs) < nbf
        {
            return Err(AuthError::NotYetValid);
        }
        if let Some(iss) = claims.get("iss").and_then(|v| v.as_str())
            && iss != self.issuer
        {
            return Err(AuthError::WrongAudience);
        }
        // `aud` may be a bare string or an array of strings per RFC 7519.
        let audience_ok = match claims.get("aud") {
            Some(Value::String(aud)) => aud == &self.audience,
            Some(Value::Array(list)) => list.iter().any(|a| a.as_str() == Some(self.audience.as_str())),
            _ => false,
        };
        if !audience_ok {
            return Err(AuthError::WrongAudience);
        }

        let tenant_id = claims
            .get("tenant_id")
            .or_else(|| claims.get("tid"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| AuthError::Malformed("missing `tenant_id` claim".into()))?;

        let scopes = match claims.get("scope") {
            Some(serde_json::Value::String(s)) => s.split_whitespace().map(Scope::exact).collect(),
            Some(serde_json::Value::Array(items)) => items
                .iter()
                .filter_map(|i| i.as_str())
                .map(Scope::exact)
                .collect(),
            _ => Vec::new(),
        };

        Ok(Principal {
            tenant_id: tenant_id.into(),
            key_id: claims
                .get("jti")
                .and_then(|v| v.as_str())
                .unwrap_or("jwt")
                .into(),
            scopes: Arc::new(scopes),
            scheme: "jwt".into(),
        })
    }
}

use serde_json::Value;

fn b64_decode(input: &str) -> Result<Vec<u8>, AuthError> {
    URL_SAFE_NO_PAD
        .decode(input)
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(input))
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(input))
        .map_err(|e| AuthError::Malformed(format!("base64url: {e}")))
}

pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

#[async_trait::async_trait]
impl Authenticator for JwtAuthenticator {
    fn scheme(&self) -> &'static str {
        "jwt"
    }

    async fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
        let Some(value) = headers.get(axum::http::header::AUTHORIZATION) else {
            return Err(AuthError::Missing {
                header: "authorization".into(),
            });
        };
        let raw = value
            .to_str()
            .map_err(|_| AuthError::Malformed("non-ascii header".into()))?;
        let token = raw
            .strip_prefix("Bearer ")
            .or_else(|| raw.strip_prefix("bearer "))
            .ok_or(AuthError::Missing {
                header: "authorization: Bearer <jwt>".into(),
            })?;
        self.verify(token.trim())
    }
}

/// Build the authenticator chain implied by config.
pub fn build_authenticator(
    cfg: &AuthConfig,
    records: Vec<KeyRecord>,
) -> Result<Arc<dyn Authenticator>, AuthError> {
    if cfg.mode == AuthMode::Disabled && !cfg.allow_anonymous {
        return Err(AuthError::Disabled);
    }
    let jwt = match (&cfg.mode, &cfg.jwt) {
        (AuthMode::ApiKeyOrJwt, Some(jwt_cfg)) => {
            let secret = jwt_cfg
                .secret()
                .map_err(|_| AuthError::Malformed("JWT secret env var is not set".into()))?;
            Some(Arc::new(JwtAuthenticator::new(&secret, jwt_cfg)) as Arc<dyn Authenticator>)
        }
        _ => None,
    };
    Ok(Arc::new(ApiKeyAuthenticator::new(records, cfg, jwt)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers_with(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    fn auth(keys: Vec<KeyRecord>, allow_anonymous: bool) -> ApiKeyAuthenticator {
        ApiKeyAuthenticator::new(
            keys,
            &AuthConfig {
                allow_anonymous,
                ..Default::default()
            },
            None,
        )
    }

    fn record(id: &str, tenant: &str, secret: &str) -> KeyRecord {
        KeyRecord {
            key_id: id.into(),
            tenant_id: tenant.into(),
            digest: key_digest(secret),
            scopes: vec![Scope::exact(SCOPE_CHAT_STREAM), Scope::exact(SCOPE_MODELS_READ)],
            enabled: true,
        }
    }

    #[tokio::test]
    async fn api_key_header_authenticates() {
        let a = auth(vec![record("ak_1", "acme", "s3cret")], false);
        let p = a
            .authenticate(&headers_with(&[("x-api-key", "s3cret")]))
            .await
            .unwrap();
        assert_eq!(&*p.tenant_id, "acme");
        assert_eq!(&*p.key_id, "ak_1");
        assert!(p.has_scope(SCOPE_CHAT_STREAM));
    }

    #[tokio::test]
    async fn bearer_token_is_accepted() {
        let a = auth(vec![record("ak_1", "acme", "s3cret")], false);
        let p = a
            .authenticate(&headers_with(&[("authorization", "Bearer s3cret")]))
            .await
            .unwrap();
        assert_eq!(&*p.tenant_id, "acme");
    }

    #[tokio::test]
    async fn wrong_key_is_rejected() {
        let a = auth(vec![record("ak_1", "acme", "s3cret")], false);
        assert!(matches!(
            a.authenticate(&headers_with(&[("x-api-key", "nope")])).await,
            Err(AuthError::Invalid)
        ));
    }

    #[tokio::test]
    async fn missing_credentials_name_the_header() {
        let a = auth(vec![record("ak_1", "acme", "s3cret")], false);
        match a.authenticate(&HeaderMap::new()).await {
            Err(AuthError::Missing { header }) => assert_eq!(header, "x-api-key"),
            other => panic!("expected Missing, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn disabled_key_cannot_authenticate() {
        let mut r = record("ak_1", "acme", "s3cret");
        r.enabled = false;
        let a = auth(vec![r], false);
        assert!(a.authenticate(&headers_with(&[("x-api-key", "s3cret")])).await.is_err());
    }

    fn sign(secret: &str, header: &str, claims: &str) -> String {
        let input = format!("{header}.{claims}");
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(input.as_bytes());
        let sig = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        format!("{input}.{sig}")
    }

    fn b64(v: &serde_json::Value) -> String {
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(v).unwrap())
    }

    fn jwt_auth(now: u64) -> JwtAuthenticator {
        let cfg = crate::config::JwtConfig {
            issuer: "jarvis".into(),
            audience: "llm-gateway".into(),
            secret_env: "TEST_JWT_SECRET".into(),
            leeway_secs: 0,
            required_scopes: vec![SCOPE_CHAT_STREAM.into()],
        };
        JwtAuthenticator::new("test-secret", &cfg).with_clock(move || now)
    }

    #[tokio::test]
    async fn valid_jwt_yields_tenant_and_scopes() {
        let now = 1_700_000_000u64;
        let a = jwt_auth(now);
        let header = b64(&serde_json::json!({"alg":"HS256","typ":"JWT"}));
        let claims = b64(&serde_json::json!({
            "iss":"jarvis","aud":"llm-gateway","tenant_id":"acme","jti":"j1",
            "scope":"chat:stream models:read","exp": now + 60
        }));
        let token = sign("test-secret", &header, &claims);
        let p = a
            .authenticate(&headers_with(&[("authorization", &format!("Bearer {token}"))]))
            .await
            .unwrap();
        assert_eq!(&*p.tenant_id, "acme");
        assert_eq!(p.scheme.as_ref(), "jwt");
        assert!(p.has_scope(SCOPE_MODELS_READ));
    }

    #[tokio::test]
    async fn expired_jwt_is_rejected() {
        let a = jwt_auth(1_700_000_100);
        let header = b64(&serde_json::json!({"alg":"HS256"}));
        let claims = b64(&serde_json::json!({
            "iss":"jarvis","aud":"llm-gateway","tenant_id":"acme","exp": 1_700_000_000
        }));
        let token = sign("test-secret", &header, &claims);
        assert!(matches!(
            a.authenticate(&headers_with(&[("authorization", &format!("Bearer {token}"))])).await,
            Err(AuthError::Expired)
        ));
    }

    #[tokio::test]
    async fn wrong_signature_is_rejected() {
        let a = jwt_auth(1_700_000_000);
        let header = b64(&serde_json::json!({"alg":"HS256"}));
        let claims = b64(&serde_json::json!({
            "iss":"jarvis","aud":"llm-gateway","tenant_id":"acme","exp": 1_700_000_600
        }));
        let token = sign("attacker-secret", &header, &claims);
        assert!(matches!(
            a.authenticate(&headers_with(&[("authorization", &format!("Bearer {token}"))])).await,
            Err(AuthError::BadSignature)
        ));
    }

    #[tokio::test]
    async fn alg_none_is_refused() {
        let a = jwt_auth(1_700_000_000);
        let header = b64(&serde_json::json!({"alg":"none"}));
        let claims = b64(&serde_json::json!({"iss":"jarvis","aud":"llm-gateway","tenant_id":"acme"}));
        let token = format!("{header}.{claims}.");
        assert!(matches!(
            a.authenticate(&headers_with(&[("authorization", &format!("Bearer {token}"))])).await,
            Err(AuthError::Malformed(_)) | Err(AuthError::BadSignature)
        ));
    }

    #[tokio::test]
    async fn wrong_audience_is_rejected() {
        let a = jwt_auth(1_700_000_000);
        let header = b64(&serde_json::json!({"alg":"HS256"}));
        let claims = b64(&serde_json::json!({
            "iss":"jarvis","aud":"someone-else","tenant_id":"acme","exp": 1_700_000_600
        }));
        let token = sign("test-secret", &header, &claims);
        assert!(matches!(
            a.authenticate(&headers_with(&[("authorization", &format!("Bearer {token}"))])).await,
            Err(AuthError::WrongAudience)
        ));
    }
}
