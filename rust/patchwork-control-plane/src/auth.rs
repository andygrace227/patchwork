use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use axum::{
    Json,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::Deserialize;
use subtle::ConstantTimeEq;

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
pub enum PrincipalType {
    AtgsInternalService,
    #[default]
    User,
    Service,
    Container,
    Function,
}

#[derive(Clone, Debug)]
pub struct Principal {
    pub id: String,
    pub account_id: Option<String>,
    pub principal_type: PrincipalType,
}

#[derive(Clone, Deserialize)]
struct Claims {
    sub: String,
    account_id: Option<String>,
    #[serde(default)]
    principal_type: PrincipalType,
}

pub struct JwtVerifier {
    key: DecodingKey,
    validation: Validation,
}

impl JwtVerifier {
    pub fn new(public_key_pem: &[u8], issuer: &str, audience: &str) -> Result<Self> {
        ensure!(!issuer.trim().is_empty(), "JWT issuer must not be empty");
        ensure!(
            !audience.trim().is_empty(),
            "JWT audience must not be empty"
        );
        let key = DecodingKey::from_rsa_pem(public_key_pem).context("Invalid JWT public key")?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[issuer]);
        validation.set_audience(&[audience]);
        validation.set_required_spec_claims(&["sub", "iss", "aud", "exp"]);
        validation.validate_nbf = true;
        validation.leeway = 0;
        Ok(Self { key, validation })
    }
}

pub struct Auth {
    internal_token: String,
    jwt: Option<JwtVerifier>,
}

impl Auth {
    pub fn new(internal_token: String, jwt: Option<JwtVerifier>) -> Result<Self> {
        ensure!(
            !internal_token.is_empty() && !internal_token.chars().any(char::is_whitespace),
            "ATGS_INTERNAL_SERVICE_TOKEN must be a nonempty token without whitespace"
        );
        Ok(Self {
            internal_token,
            jwt,
        })
    }

    pub fn from_env() -> Result<Self> {
        let internal_token = std::env::var("ATGS_INTERNAL_SERVICE_TOKEN")
            .context("Set ATGS_INTERNAL_SERVICE_TOKEN in .env or the environment")?;
        let public_key_path = std::env::var("JWT_PUBLIC_KEY_PATH").ok();
        let issuer = std::env::var("JWT_ISSUER").ok();
        let audience = std::env::var("JWT_AUDIENCE").ok();
        let jwt = match (public_key_path, issuer, audience) {
            (None, None, None) => None,
            (Some(path), Some(issuer), Some(audience)) => Some(JwtVerifier::new(
                &std::fs::read(path).context("Could not read JWT public key")?,
                &issuer,
                &audience,
            )?),
            _ => anyhow::bail!("Set JWT_PUBLIC_KEY_PATH, JWT_ISSUER, and JWT_AUDIENCE together"),
        };
        Self::new(internal_token, jwt)
    }

    pub fn authenticate(&self, headers: &HeaderMap) -> Option<Principal> {
        if headers.get_all(header::AUTHORIZATION).iter().count() != 1 {
            return None;
        }
        let (scheme, token) = headers
            .get(header::AUTHORIZATION)?
            .to_str()
            .ok()?
            .split_once(' ')?;
        if !scheme.eq_ignore_ascii_case("Bearer")
            || token.is_empty()
            || token.chars().any(char::is_whitespace)
        {
            return None;
        }
        if bool::from(self.internal_token.as_bytes().ct_eq(token.as_bytes())) {
            return Some(Principal {
                id: "atgs-internal-service".into(),
                account_id: None,
                principal_type: PrincipalType::AtgsInternalService,
            });
        }
        let jwt = self.jwt.as_ref()?;
        let claims = decode::<Claims>(token, &jwt.key, &jwt.validation)
            .ok()?
            .claims;
        // The bootstrap exception is available only through the configured token.
        if claims.sub.trim().is_empty()
            || claims.principal_type == PrincipalType::AtgsInternalService
        {
            return None;
        }
        Some(Principal {
            id: claims.sub,
            account_id: claims.account_id,
            principal_type: claims.principal_type,
        })
    }
}

pub(crate) async fn authenticate(
    State(auth): State<Arc<Auth>>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(principal) = auth.authenticate(request.headers()) else {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            Json(serde_json::json!({"error": "valid bearer token required"})),
        )
            .into_response();
    };
    request.extensions_mut().insert(principal);
    next.run(request).await
}
