//! Backend S3 client. The daemon signs with its OWN identity (EKS Pod
//! Identity — ADR-0006). The backend is one of two shapes (ADR-0023):
//!
//! - **S3 Express One Zone** directory bucket in the nodepool's AZ (ADR-0002,
//!   the original locked default): the SDK transparently manages
//!   `CreateSession` credentials, zonal (`*--x-s3`) addressing, and the
//!   `sigv4-s3express` signing scheme, so nothing special is needed here beyond
//!   the endpoint — we manage no Express auth code (planning/03).
//! - **S3 Standard** general-purpose regional bucket (ADR-0023, full parity):
//!   ordinary regional addressing + plain SigV4, no `CreateSession`, cross-AZ.
//!   We disable the SDK's Express session auth defensively so no `CreateSession`
//!   is ever attempted for this backend, regardless of the real bucket's name.
//!
//! The shape is chosen by explicit config ([`BackendConfig::backend_type`]), not
//! sniffed from the bucket name — a name-pattern guess is fragile (a Standard
//! bucket can be named anything, and the alias the client uses hides the real
//! name), so the operator states it. Express-only multipart normalization
//! (consecutive parts, CRC32) is enforced in the proxy, also gated on this type.
//!
//! Reading *through* that client is [`retry`]: one chunk's ranged GET and its
//! body, as a single retryable unit. It lives here rather than in the daemon
//! because it is a property of talking to S3, not of the cache policy above it.

use std::str::FromStr;

pub mod retry;

/// Config-file / env token selecting the S3 Express One Zone backend.
const BACKEND_TYPE_EXPRESS: &str = "express";
/// Config-file / env token selecting the S3 Standard (general-purpose) backend.
const BACKEND_TYPE_STANDARD: &str = "standard";

/// Which S3 backend shape the daemon fronts (ADR-0023). Gates every place the
/// two diverge: Express needs `CreateSession` / zonal addressing /
/// `sigv4-s3express` (all SDK-managed) and requires consecutive multipart
/// parts; Standard uses plain regional SigV4 and allows sparse (ascending-only)
/// multipart part numbers. Chosen by explicit config, never sniffed from the
/// bucket name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackendType {
    /// S3 Express One Zone directory bucket, same AZ ID as the nodepool
    /// (ADR-0002). The historical default, so it is [`Default`] — an existing
    /// deployment keeps its behavior when the new knob is unset.
    #[default]
    Express,
    /// S3 Standard general-purpose regional bucket (ADR-0023). Regional /
    /// cross-AZ: no same-AZ latency weld, so parity here is *functional*, not
    /// performance-equivalent (the Phase-4 D2 benchmark quantifies the delta).
    Standard,
}

impl BackendType {
    /// Whether this backend needs S3 Express quirk handling — SDK-managed
    /// `CreateSession` session auth and the proxy's Express-only request
    /// normalization (consecutive multipart parts). `false` for Standard.
    #[must_use]
    pub fn is_express(self) -> bool {
        matches!(self, BackendType::Express)
    }
}

impl FromStr for BackendType {
    type Err = String;

    /// Parse the `express` / `standard` config token (case-insensitive).
    ///
    /// # Errors
    ///
    /// Any other value — a typo must fail fast at startup rather than silently
    /// pick a shape (the two behave differently on the write path).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            BACKEND_TYPE_EXPRESS => Ok(BackendType::Express),
            BACKEND_TYPE_STANDARD => Ok(BackendType::Standard),
            other => Err(format!(
                "unknown backend type {other:?} (expected {BACKEND_TYPE_EXPRESS:?} or \
                 {BACKEND_TYPE_STANDARD:?})"
            )),
        }
    }
}

/// Backend S3 client settings.
#[derive(Debug, Clone, Default)]
pub struct BackendConfig {
    /// Which backend shape this daemon fronts (ADR-0023). Gates Express-only
    /// SDK behavior here and request normalization in the proxy.
    pub backend_type: BackendType,
    /// Endpoint override. For S3 Express this is the zonal endpoint of the
    /// directory bucket's AZ (same AZ as the nodepool — ADR-0002); for S3
    /// Standard the regional endpoint (usually left `None` for default
    /// resolution). None = regular S3 resolution (localstack/minio tests use
    /// Some + path style).
    pub endpoint: Option<String>,
    /// Force path-style addressing (test backends).
    pub force_path_style: bool,
}

/// Build the backend client from the ambient AWS environment (region and
/// credentials come from EKS Pod Identity in-cluster, or the usual env/profile
/// chain locally).
///
/// For a [`BackendType::Standard`] backend the SDK's S3 Express session auth is
/// disabled, so no `s3express:CreateSession` is ever attempted and requests use
/// plain regional SigV4 (ADR-0023) — belt-and-suspenders even if a bucket name
/// were to resemble a directory bucket. For [`BackendType::Express`] the SDK
/// manages session auth transparently (ADR-0002/planning/03), so nothing is
/// changed here.
pub async fn build_client(cfg: &BackendConfig) -> aws_sdk_s3::Client {
    let base = aws_config::load_from_env().await;
    let mut builder = aws_sdk_s3::config::Builder::from(&base);
    if let Some(endpoint) = &cfg.endpoint {
        builder = builder.endpoint_url(endpoint);
    }
    if cfg.force_path_style {
        builder = builder.force_path_style(true);
    }
    if !cfg.backend_type.is_express() {
        // Standard: revert any Express session auth to conventional SigV4 so a
        // cross-AZ regional bucket never triggers a (doomed) CreateSession.
        builder = builder.disable_s3_express_session_auth(true);
    }
    aws_sdk_s3::Client::from_conf(builder.build())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_type_parses_case_insensitively() {
        assert_eq!(
            "express".parse::<BackendType>().unwrap(),
            BackendType::Express
        );
        assert_eq!(
            "Standard".parse::<BackendType>().unwrap(),
            BackendType::Standard
        );
        assert_eq!(
            "  EXPRESS ".parse::<BackendType>().unwrap(),
            BackendType::Express
        );
    }

    #[test]
    fn backend_type_defaults_to_express() {
        // An existing deployment (no knob set) keeps the ADR-0002 Express path.
        assert_eq!(BackendType::default(), BackendType::Express);
        assert!(BackendType::default().is_express());
        assert!(!BackendType::Standard.is_express());
    }

    #[test]
    fn unknown_backend_type_is_rejected() {
        let err = "s3-standard".parse::<BackendType>().unwrap_err();
        assert!(err.contains("unknown backend type"), "got: {err}");
    }
}
