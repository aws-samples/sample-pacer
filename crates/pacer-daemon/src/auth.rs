//! Strip-and-re-sign auth (ADR-0006): clients sign with well-known placeholder
//! credentials; the daemon verifies that signature (so garbage requests are
//! rejected at the door) and re-signs outbound with its own IAM identity.
//! The placeholder credential is not a real secret: it only lets the daemon
//! reject malformed requests. Secure the pod↔daemon hop at the network layer.

use s3s::auth::{S3Auth, SecretKey};
use s3s::{s3_error, S3Result};

/// s3s auth provider that accepts exactly one well-known key pair.
pub struct PlaceholderAuth {
    access_key: String,
    secret_key: SecretKey,
}

impl PlaceholderAuth {
    /// Provider accepting exactly this key pair.
    pub fn new(access_key: String, secret_key: String) -> Self {
        Self {
            access_key,
            secret_key: SecretKey::from(secret_key),
        }
    }
}

#[async_trait::async_trait]
impl S3Auth for PlaceholderAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        if access_key == self.access_key {
            Ok(self.secret_key.clone())
        } else {
            Err(s3_error!(
                InvalidAccessKeyId,
                "point your SDK at the PACER placeholder credentials"
            ))
        }
    }
}
