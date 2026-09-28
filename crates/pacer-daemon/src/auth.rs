//! Strip-and-re-sign auth (ADR-0006): clients sign with well-known placeholder
//! credentials; the daemon verifies that signature (so garbage requests are
//! rejected at the door) and re-signs outbound with its own IAM identity.
//! The placeholder credential is not a real secret: it only lets the daemon
//! reject malformed requests. Secure the pod↔daemon hop at the network layer.
//!
//! ADR-0041 adds a second mode, `requester`, that lets S3 authorize the
//! caller's own SigV4 signature instead. [`AuthMode`] selects between them;
//! everything in this module below it still implements `node` mode only.

use s3s::auth::{S3Auth, SecretKey};
use s3s::{s3_error, S3Result};

/// Which identity authorizes a request against S3 (ADR-0041). One value per
/// release — not configurable per caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuthMode {
    /// ADR-0006 as shipped: the daemon strips the client's signature and
    /// re-signs with its own IAM identity. A `NetworkPolicy` is the only
    /// authorization boundary.
    #[default]
    Node,
    /// The daemon is an HTTP(S) proxy the caller's own signature passes
    /// through; S3 authorizes each request itself.
    Requester,
}

impl std::str::FromStr for AuthMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "node" | "" => Ok(Self::Node),
            "requester" => Ok(Self::Requester),
            other => anyhow::bail!("unknown auth mode {other:?} (node|requester)"),
        }
    }
}

impl std::fmt::Display for AuthMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Node => "node",
            Self::Requester => "requester",
        })
    }
}

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
