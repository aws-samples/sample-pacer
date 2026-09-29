//! Warming the cache ahead of first read (ADR-0048): the wire contract, and the
//! `pacer-daemon warm` command that drives it.
//!
//! A warm is an ordinary `GetObject` through the daemon carrying [`WARM_HEADER`]. The
//! daemon runs the read path it always runs — header, admission, and in
//! `auth.mode: requester` the caller's own authorization — resolves every covering chunk,
//! drops the bytes, and answers a header-only `200` carrying [`WARMED_HEADER`]:
//!
//! ```text
//!   GET /bucket/key   x-pacer-warm: 1   Range: bytes=0-1073741823  ──►  resolve chunks
//!        ◄──── 200, empty body, x-pacer-warmed: 1073741824 ──────────────  (fill misses)
//! ```
//!
//! The same shape the delivery protocol uses (a request header in, headers out, no body),
//! for the same reasons: it rides the SDK every client already has, it goes through the
//! authorization every GET goes through, and a daemon that predates it degrades safely — it
//! ignores the header and streams the body, which still warms the object. The presence of
//! [`WARMED_HEADER`] is therefore the completion signal: a caller that does not see it read
//! a body, and must drain it.
//!
//! **Synchronous by design.** Nothing runs after the answer. In `requester` mode the daemon
//! can only read S3 on the caller's signature, which lives as long as the request; and a
//! fill no one is waiting on has no back-pressure (ADR-0011). Asynchrony belongs to the
//! caller: the [`command`] runs as a Kubernetes Job, bounds each request with a byte range,
//! and holds the only concurrency knob that loads S3.

pub mod command;
pub mod plan;

/// Request header asking for a warm-only GET. `x-pacer-*` for the reason
/// [`crate::delivery::TARGET_HEADER`] is: an extension of ours, visibly so.
///
/// Sent **after** signing, never under the signature: in `auth.mode: requester` the daemon
/// re-emits the caller's held request to S3 for every chunk, and a signed header it does
/// not forward verbatim would break that signature.
pub const WARM_HEADER: &str = "x-pacer-warm";

/// The one value [`WARM_HEADER`] accepts. Anything else is refused rather than read as
/// "no", so a typo is an error the caller sees instead of a full body it did not want.
pub const WARM_REQUESTED: &str = "1";

/// Response header on every warm answer: bytes of the object the warm resolved through the
/// cache path — the requested range's length when it warmed, `0` when it was skipped.
pub const WARMED_HEADER: &str = "x-pacer-warmed";

/// Response header naming why a warm read nothing — present only when it did not. One of
/// the [`SkipReason`] values.
pub const WARM_SKIPPED_HEADER: &str = "x-pacer-warm-skipped";

/// Why a warm read nothing: the request, or the object, is one the cache never holds.
///
/// A skip is an answer, not a failure — warming a model directory skips its small
/// `config.json` by design — so the command counts these apart from errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkipReason {
    /// The request shape bypasses the cache: a version id, SSE-C, a conditional, a
    /// `response-*` override, a `Cache-Control` that bypasses. The command sends none.
    Uncacheable,
    /// `Cache-Control: no-store` — the caller asked for nothing to be kept.
    NoStore,
    /// The object is outside the size band the daemon admits (ADR-0002's small-object
    /// bypass, or an operator's cap).
    ObjectSize,
}

impl SkipReason {
    /// The [`WARM_SKIPPED_HEADER`] value for this reason.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Uncacheable => "uncacheable-request",
            Self::NoStore => "no-store",
            Self::ObjectSize => "object-size",
        }
    }

    /// The reason a [`WARM_SKIPPED_HEADER`] value names, or `None` for one this build does
    /// not know — a newer daemon's reason is still a skip, just an unnamed one.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        [Self::Uncacheable, Self::NoStore, Self::ObjectSize]
            .into_iter()
            .find(|r| r.as_str() == value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_skip_reason_round_trips_through_its_header_value() {
        for r in [
            SkipReason::Uncacheable,
            SkipReason::NoStore,
            SkipReason::ObjectSize,
        ] {
            assert_eq!(SkipReason::parse(r.as_str()), Some(r));
        }
        assert_eq!(SkipReason::parse("from-a-newer-daemon"), None);
    }
}
