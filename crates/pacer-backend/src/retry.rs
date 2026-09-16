//! Bounded retry for one backend chunk read — the ranged `GetObject` **and**
//! its body, as a single retryable unit.
//!
//! # Why this exists
//!
//! A chunked read (ADR-0015) turns one client GET into N ranged backend GETs
//! driven by an ordered pipeline, and that pipeline aborts the client's body
//! stream the instant one chunk errors. So a single transient S3 failure on
//! chunk *k* truncates a response whose first *k* chunks were already correct,
//! and the client cannot even see it as an error: the `200` status line and
//! `Content-Length` went out with the headers, long before chunk *k* was read.
//! A truncated body is the worst shape a transient fault can take, because it
//! is the one shape a caller may not notice.
//!
//! # Why the SDK's own retry does not close this
//!
//! `aws-sdk-s3` retries the *request* — the attempt that produces the response
//! headers. Once those headers are in hand the operation has succeeded as far
//! as the SDK is concerned, and the body is an independent stream: a connection
//! reset at 15 MiB of a 16 MiB chunk surfaces from
//! [`aws_sdk_s3::primitives::ByteStream::collect`], which no SDK attempt
//! covers. That is why the retry here has to wrap the GET and the body read
//! together, and why it cannot be replaced by raising the SDK's
//! `max_attempts`.
//!
//! The corollary is that a `send()` failure is retried at two layers: the SDK's
//! attempts happen inside one of ours. That is deliberate (the SDK's layer also
//! refreshes credentials and corrects clock skew, which we must not
//! reimplement), but it means the effective request budget is the product of
//! the two — keep [`RetryPolicy::max_attempts`] small.
//!
//! # Why the whole chunk is re-read rather than resumed
//!
//! Resuming (narrowing the range to the missing tail and splicing) would avoid
//! re-reading bytes already in hand, but it can splice two *different* object
//! versions if the key is overwritten between attempts — a corrupt chunk that
//! passes every length check. Re-reading the whole chunk always yields one
//! self-consistent chunk, and a chunk is bounded (`chunk_size`, 16 MiB by
//! default), so the re-read is bounded too. The bandwidth is worth the
//! guarantee.

use std::future::Future;
use std::ops::Range;
use std::time::Duration;

use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::get_object::GetObjectError;
use bytes::Bytes;
use tracing::warn;

/// Attempts per chunk read when the operator sets no policy.
///
/// Three, not more: each attempt re-reads a whole chunk body, and the SDK is
/// already retrying inside each one (see the module header), so the effective
/// request budget is `3 × SDK attempts`. Three covers an isolated transient
/// fault — the observed failure mode — without turning a genuinely unhealthy
/// backend into a bandwidth amplifier.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// First backoff before a second attempt, and the base of the doubling.
///
/// A same-AZ S3 Express chunk read is a few milliseconds, so 50 ms is already
/// an order of magnitude longer than the operation being retried: long enough
/// for a reset connection to be replaced, short enough that the added latency
/// is invisible against the 16 MiB re-read it precedes.
pub const DEFAULT_BASE_BACKOFF: Duration = Duration::from_millis(50);

/// Ceiling on one backoff, however many attempts the operator allows.
///
/// A client is blocked on this read with its response already committed, so the
/// backoff must stay far below any sane client-side read timeout. One second
/// bounds the worst case at roughly `max_attempts × 1s` of added latency.
pub const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// Cap on the doubling exponent, so `1 << exp` cannot overflow `u32` no matter
/// how many attempts an operator configures. Well past where
/// [`RetryPolicy::max_backoff`] clamps anyway.
const MAX_BACKOFF_SHIFT: u32 = 16;

/// Denominator of the jitter fraction. A power of two so the mix reduces to a
/// mask, and large enough that concurrent chunks land on distinct delays.
const JITTER_SCALE: u64 = 1 << 10;

/// S3 error codes that are retryable despite a non-5xx HTTP status, so a status
/// check alone would classify them as permanent. `RequestTimeout` in particular
/// arrives as a `400`.
const RETRYABLE_ERROR_CODES: &[&str] = &[
    "RequestTimeout",
    "RequestTimeoutException",
    "SlowDown",
    "ThrottlingException",
    "TooManyRequests",
    "InternalError",
    "ServiceUnavailable",
];

/// HTTP status for "too many requests" — throttling, retryable, and the one
/// retryable status below `500`.
const STATUS_TOO_MANY_REQUESTS: u16 = 429;
/// First 5xx status; at or above this the fault is the service's, so retry.
const STATUS_SERVER_ERROR: u16 = 500;

/// How hard to try one chunk read before giving up on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts, including the first. `1` disables retrying (the
    /// pre-retry behavior); `0` is normalized to `1` rather than rejected, so a
    /// misconfigured value cannot make every read fail without trying.
    pub max_attempts: u32,
    /// Backoff before the second attempt, doubled per attempt thereafter and
    /// clamped by [`Self::max_backoff`].
    pub base_backoff: Duration,
    /// Ceiling on any single backoff.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            base_backoff: DEFAULT_BASE_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
        }
    }
}

impl RetryPolicy {
    /// A policy with `max_attempts` attempts and the default backoff shape.
    /// `0` normalizes to `1` (try once, never retry).
    #[must_use]
    pub fn with_max_attempts(max_attempts: u32) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            ..Self::default()
        }
    }

    /// How long to wait before attempt `attempt + 1`, given that `attempt`
    /// (1-based) just failed.
    ///
    /// Exponential (`base × 2^(attempt-1)`, clamped to
    /// [`Self::max_backoff`]) with decorrelated jitter over the upper half of
    /// that ceiling. `jitter_index` decorrelates *concurrent* reads: the pipeline
    /// has `fill_parallelism` chunks in flight, and a backend-wide fault fails
    /// them together, so an unjittered policy would retry all of them in lockstep
    /// and re-create the burst that caused it. Passing the chunk index spreads
    /// them deterministically — no PRNG, and the spread is reproducible in a
    /// test.
    #[must_use]
    pub fn backoff(&self, attempt: u32, jitter_index: u64) -> Duration {
        let exp = attempt.saturating_sub(1).min(MAX_BACKOFF_SHIFT);
        let ceiling = self
            .base_backoff
            .saturating_mul(1u32 << exp)
            .min(self.max_backoff);
        // Nanoseconds, not floats: a 16-bit-shifted Duration is still far
        // inside u64 nanos once max_backoff has clamped it.
        let half = u64::try_from(ceiling.as_nanos() / 2).unwrap_or(u64::MAX);
        let fraction = mix(jitter_index, attempt) % JITTER_SCALE;
        Duration::from_nanos(half + half.saturating_mul(fraction) / JITTER_SCALE)
    }
}

/// One chunk read to issue: a byte range of one key.
// Not `Copy`: `Range` is deliberately not, so that `for x in range` cannot
// silently iterate a copy. Every attempt borrows this value anyway.
#[derive(Debug, Clone)]
pub struct ChunkRead<'a> {
    /// Real backend bucket (already un-aliased by the caller).
    pub bucket: &'a str,
    /// Object key.
    pub key: &'a str,
    /// Half-open byte range to read. An empty range reads nothing and issues no
    /// request — the HTTP `Range` header cannot express one.
    pub range: Range<u64>,
    /// Jitter decorrelation index; pass the chunk index (see
    /// [`RetryPolicy::backoff`]).
    pub jitter_index: u64,
}

/// A chunk read that succeeded, and what it cost.
#[derive(Debug, Clone)]
pub struct ChunkBody {
    /// The chunk's bytes.
    pub body: Bytes,
    /// Attempts made, including the successful one. `1` means no retry
    /// happened; the caller reports `attempts - 1` as retries. `0` is the empty
    /// range, which issues no request at all — so subtract saturatingly.
    pub attempts: u32,
}

/// Why a chunk read failed for good, and how many attempts it took to find out.
#[derive(Debug, thiserror::Error)]
#[error("{kind} after {attempts} attempt(s)")]
pub struct BackendReadError {
    /// What went wrong.
    pub kind: BackendReadErrorKind,
    /// Attempts made before giving up. Includes the one that failed
    /// permanently, so a `Permanent` on the first try reports `1`.
    pub attempts: u32,
}

/// The shape of a terminal chunk-read failure. The caller maps each to a
/// client-visible status, which is the whole reason they are distinguished:
/// only [`Self::Missing`] may become a `404`.
#[derive(Debug, thiserror::Error)]
pub enum BackendReadErrorKind {
    /// The key does not exist at the backend.
    #[error("no such key")]
    Missing,
    /// A failure retrying cannot fix — a `4xx` other than throttling, or a
    /// request we could not even construct. Never retried.
    #[error("permanent backend read failure: {0}")]
    Permanent(String),
    /// Every attempt failed on a retryable fault. This is the case the retry
    /// exists for, and reaching it means the backend was unhealthy for the
    /// whole backoff window, not momentarily.
    #[error("backend read failed on every attempt: {0}")]
    Exhausted(String),
}

/// Outcome of one attempt, before the policy decides whether there is another.
#[derive(Debug)]
enum AttemptError {
    /// Worth another attempt.
    Transient(String),
    /// Not worth another attempt.
    Permanent(String),
    /// The key does not exist.
    Missing,
}

/// Read `read`'s byte range, retrying transient failures per `policy`.
///
/// Retries a `GetObject` whose fault was the service's or the network's, and —
/// the case the SDK's own retry cannot reach — **any** failure of the response
/// body stream. A body that arrives shorter than the response promised is
/// treated as such a failure rather than returned: a short chunk is exactly the
/// silent truncation this module exists to prevent.
///
/// # Errors
///
/// [`BackendReadErrorKind::Missing`] when the key does not exist,
/// [`BackendReadErrorKind::Permanent`] for a fault retrying cannot fix (a `4xx`
/// other than throttling, an unconstructable request), and
/// [`BackendReadErrorKind::Exhausted`] when every allowed attempt hit a
/// retryable fault.
pub async fn read_range(
    client: &aws_sdk_s3::Client,
    read: &ChunkRead<'_>,
    policy: &RetryPolicy,
) -> Result<ChunkBody, BackendReadError> {
    if read.range.is_empty() {
        return Ok(ChunkBody {
            body: Bytes::new(),
            attempts: 0,
        });
    }
    retrying(policy, read.jitter_index, || read_once(client, read)).await
}

/// The retry loop itself, over any attempt operation.
///
/// Split from [`read_range`] so the loop's contract — attempt counting, which
/// outcomes stop it early, what it reports on exhaustion — is testable without
/// a live S3 endpoint.
async fn retrying<F, Fut>(
    policy: &RetryPolicy,
    jitter_index: u64,
    mut attempt_op: F,
) -> Result<ChunkBody, BackendReadError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Bytes, AttemptError>>,
{
    let budget = policy.max_attempts.max(1);
    let mut last = String::new();
    for attempt in 1..=budget {
        let err = match attempt_op().await {
            Ok(body) => {
                return Ok(ChunkBody {
                    body,
                    attempts: attempt,
                })
            }
            Err(e) => e,
        };
        let message = match err {
            AttemptError::Missing => {
                return Err(BackendReadError {
                    kind: BackendReadErrorKind::Missing,
                    attempts: attempt,
                })
            }
            AttemptError::Permanent(m) => {
                return Err(BackendReadError {
                    kind: BackendReadErrorKind::Permanent(m),
                    attempts: attempt,
                })
            }
            AttemptError::Transient(m) => m,
        };
        last = message;
        if attempt == budget {
            break;
        }
        let delay = policy.backoff(attempt, jitter_index);
        warn!(
            attempt,
            budget,
            backoff_ms = delay.as_millis(),
            error = %last,
            "backend chunk read failed transiently; retrying"
        );
        tokio::time::sleep(delay).await;
    }
    Err(BackendReadError {
        kind: BackendReadErrorKind::Exhausted(last),
        attempts: budget,
    })
}

/// One attempt: ranged GET, then the whole body.
async fn read_once(
    client: &aws_sdk_s3::Client,
    read: &ChunkRead<'_>,
) -> Result<Bytes, AttemptError> {
    // HTTP Range is inclusive on both ends; `range.end` is exclusive. The empty
    // range was rejected in `read_range`, so `end - 1` cannot underflow.
    let header = format!("bytes={}-{}", read.range.start, read.range.end - 1);
    let resp = client
        .get_object()
        .bucket(read.bucket)
        .key(read.key)
        .range(header)
        .send()
        .await
        .map_err(classify_send_error)?;
    // Read before consuming `body`: the promise this attempt has to keep.
    let promised = resp.content_length().and_then(|l| u64::try_from(l).ok());
    let body = resp
        .body
        .collect()
        .await
        // Every body failure is transient by construction. A body stream that
        // breaks did so in transit — the request itself already succeeded, and
        // a permanent fault would have been a status code, not a severed
        // stream.
        .map_err(|e| AttemptError::Transient(format!("body read failed: {e}")))?
        .into_bytes();
    // Compare against what the response promised, NOT against the requested
    // range: S3 clamps a range that runs past the object, so a caller who
    // over-requests must not see every attempt fail as a short read.
    if let Some(promised) = promised {
        if body.len() as u64 != promised {
            return Err(AttemptError::Transient(format!(
                "short body: got {} of {promised} bytes",
                body.len()
            )));
        }
    }
    Ok(body)
}

/// Classify a `GetObject` `send()` failure.
///
/// Note this does NOT go through `into_service_error()`, which collapses a
/// dispatch failure or a timeout into `GetObjectError::Unhandled` and so erases
/// the distinction this function exists to make: a connection that was never
/// established is the most retryable fault there is, and reporting it as an
/// unhandled service error made it indistinguishable from a real `4xx`.
fn classify_send_error(err: SdkError<GetObjectError>) -> AttemptError {
    let describe = || format!("{}", aws_sdk_s3::error::DisplayErrorContext(&err));
    match &err {
        // Never dispatched: the request itself is malformed or unsignable.
        SdkError::ConstructionFailure(_) => AttemptError::Permanent(describe()),
        SdkError::TimeoutError(_) => AttemptError::Transient(describe()),
        // The server hung up mid-response, or sent something unparseable.
        SdkError::ResponseError(_) => AttemptError::Transient(describe()),
        SdkError::DispatchFailure(d) => {
            if d.is_user() {
                AttemptError::Permanent(describe())
            } else {
                AttemptError::Transient(describe())
            }
        }
        SdkError::ServiceError(svc) => {
            if svc.err().is_no_such_key() {
                return AttemptError::Missing;
            }
            if retryable_response(svc.raw().status().as_u16(), svc.err().code()) {
                AttemptError::Transient(describe())
            } else {
                AttemptError::Permanent(describe())
            }
        }
        // `SdkError` is `#[non_exhaustive]`: a variant added upstream is a
        // fault we cannot reason about, so do not spend attempts on it.
        _ => AttemptError::Permanent(describe()),
    }
}

/// Whether an S3 error response is worth another attempt, from its HTTP status
/// and error code. Pure, so the classification is testable without
/// constructing SDK error types.
fn retryable_response(status: u16, code: Option<&str>) -> bool {
    if status >= STATUS_SERVER_ERROR || status == STATUS_TOO_MANY_REQUESTS {
        return true;
    }
    code.is_some_and(|c| RETRYABLE_ERROR_CODES.contains(&c))
}

/// One splitmix64 finalizer over `jitter_index` and `attempt` — a cheap avalanche
/// that gives every (chunk, attempt) pair its own jitter fraction with no `rand`
/// dependency and no shared state. Not cryptographic and does not need to be:
/// the only requirement is that concurrent chunks disagree.
///
/// The parameter is `jitter_index`, not `salt` (its name until 2026-09-16): a
/// name is an interface to scanners as well as to readers, and CodeQL's
/// `rust/hard-coded-cryptographic-value` matches the identifier alone — it raised
/// nine CRITICAL "hard-coded value is used as a salt" alerts against the unit
/// tests below, which pass `0` for determinism. Nothing here is a salt: the value
/// picks a jitter position and never reaches a key, a digest or a cipher. Do not
/// rename it back, and not to `seed` either — that is on the same query's list.
fn mix(jitter_index: u64, attempt: u32) -> u64 {
    /// The golden-ratio odd constant from splitmix64's `next`, used to fold the
    /// attempt number in so retries of one chunk also differ from each other.
    const GOLDEN_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;
    /// splitmix64's two finalizer multipliers.
    const MIX_A: u64 = 0xBF58_476D_1CE4_E5B9;
    /// Second finalizer multiplier.
    const MIX_B: u64 = 0x94D0_49BB_1331_11EB;

    let mut z = jitter_index
        .wrapping_mul(GOLDEN_GAMMA)
        .wrapping_add(u64::from(attempt).wrapping_mul(GOLDEN_GAMMA));
    z = (z ^ (z >> 30)).wrapping_mul(MIX_A);
    z = (z ^ (z >> 27)).wrapping_mul(MIX_B);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// An attempt op that fails transiently `fail_first` times, then succeeds.
    /// Counts calls so a test can assert the loop stopped when it should have.
    struct Flaky {
        fail_first: u32,
        calls: Cell<u32>,
    }

    impl Flaky {
        fn new(fail_first: u32) -> Self {
            Self {
                fail_first,
                calls: Cell::new(0),
            }
        }

        async fn attempt(&self) -> Result<Bytes, AttemptError> {
            let n = self.calls.get() + 1;
            self.calls.set(n);
            if n <= self.fail_first {
                Err(AttemptError::Transient(format!("reset on call {n}")))
            } else {
                Ok(Bytes::from_static(b"chunk"))
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_transient_failure_is_retried_and_the_read_succeeds() {
        // The reported defect: one transient streaming error used to end the
        // whole client GET. Now the second attempt serves the chunk.
        let flaky = Flaky::new(1);
        let got = retrying(&RetryPolicy::default(), 0, || flaky.attempt())
            .await
            .unwrap();
        assert_eq!(&got.body[..], b"chunk");
        assert_eq!(got.attempts, 2, "one retry, so two attempts");
        assert_eq!(flaky.calls.get(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_clean_first_attempt_never_retries() {
        let flaky = Flaky::new(0);
        let got = retrying(&RetryPolicy::default(), 0, || flaky.attempt())
            .await
            .unwrap();
        assert_eq!(got.attempts, 1);
        assert_eq!(flaky.calls.get(), 1, "the happy path must cost one GET");
    }

    #[tokio::test(start_paused = true)]
    async fn transient_failures_beyond_the_budget_exhaust() {
        let flaky = Flaky::new(u32::MAX);
        let policy = RetryPolicy::with_max_attempts(3);
        let err = retrying(&policy, 0, || flaky.attempt()).await.unwrap_err();
        assert_eq!(flaky.calls.get(), 3, "exactly the budget, no more");
        assert_eq!(err.attempts, 3);
        assert!(
            matches!(err.kind, BackendReadErrorKind::Exhausted(ref m) if m.contains("call 3")),
            "exhaustion must report the LAST failure, not the first: {err}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_missing_key_is_not_retried() {
        // A 404 is the answer, not a fault: retrying it would trade a correct
        // 404 for three round trips and the same 404.
        let calls = Cell::new(0);
        let err = retrying(&RetryPolicy::with_max_attempts(5), 0, || {
            calls.set(calls.get() + 1);
            async { Err(AttemptError::Missing) }
        })
        .await
        .unwrap_err();
        assert_eq!(calls.get(), 1);
        assert!(matches!(err.kind, BackendReadErrorKind::Missing));
        assert_eq!(err.attempts, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_permanent_failure_is_not_retried() {
        let calls = Cell::new(0);
        let err = retrying(&RetryPolicy::with_max_attempts(5), 0, || {
            calls.set(calls.get() + 1);
            async { Err(AttemptError::Permanent("AccessDenied".into())) }
        })
        .await
        .unwrap_err();
        assert_eq!(calls.get(), 1, "no attempt may be spent on a 4xx");
        assert!(matches!(err.kind, BackendReadErrorKind::Permanent(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_single_attempt_policy_restores_the_pre_retry_behaviour() {
        let flaky = Flaky::new(1);
        let err = retrying(&RetryPolicy::with_max_attempts(1), 0, || flaky.attempt())
            .await
            .unwrap_err();
        assert_eq!(flaky.calls.get(), 1);
        assert!(matches!(err.kind, BackendReadErrorKind::Exhausted(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_zero_attempt_policy_still_tries_once() {
        // A misconfigured 0 must not turn every read into an instant failure.
        let flaky = Flaky::new(0);
        let policy = RetryPolicy {
            max_attempts: 0,
            ..RetryPolicy::default()
        };
        assert_eq!(
            retrying(&policy, 0, || flaky.attempt())
                .await
                .unwrap()
                .attempts,
            1
        );
    }

    #[test]
    fn backoff_grows_and_stays_inside_the_ceiling() {
        let policy = RetryPolicy::default();
        for attempt in 1..8u32 {
            let exp = attempt.saturating_sub(1).min(MAX_BACKOFF_SHIFT);
            let ceiling = policy
                .base_backoff
                .saturating_mul(1u32 << exp)
                .min(policy.max_backoff);
            let d = policy.backoff(attempt, 7);
            assert!(
                d >= ceiling / 2 && d <= ceiling,
                "attempt {attempt}: {d:?} outside [{:?}, {ceiling:?}]",
                ceiling / 2
            );
        }
    }

    #[test]
    fn backoff_is_clamped_by_max_backoff() {
        // Doubling for 40 attempts must not overflow or escape the ceiling.
        let policy = RetryPolicy::default();
        let d = policy.backoff(40, 1);
        assert!(d <= policy.max_backoff, "{d:?}");
        assert!(d >= policy.max_backoff / 2, "{d:?}");
    }

    #[test]
    fn concurrent_chunks_do_not_retry_in_lockstep() {
        // The point of the jitter index: `fill_parallelism` chunks failing together
        // must not all wake at the same instant and re-create the burst.
        let policy = RetryPolicy::default();
        let delays: std::collections::HashSet<Duration> =
            (0..8u64).map(|idx| policy.backoff(1, idx)).collect();
        assert!(
            delays.len() >= 7,
            "8 concurrent chunks collapsed onto {} distinct delays",
            delays.len()
        );
    }

    #[test]
    fn server_errors_and_throttling_are_retryable() {
        assert!(retryable_response(500, Some("InternalError")));
        assert!(retryable_response(503, Some("SlowDown")));
        assert!(retryable_response(504, None));
        assert!(retryable_response(429, None));
        // Retryable despite a 4xx status — the reason the code list exists.
        assert!(retryable_response(400, Some("RequestTimeout")));
    }

    #[test]
    fn client_errors_are_not_retryable() {
        assert!(!retryable_response(403, Some("AccessDenied")));
        assert!(!retryable_response(416, Some("InvalidRange")));
        assert!(!retryable_response(400, Some("InvalidArgument")));
        assert!(!retryable_response(404, Some("NoSuchKey")));
        assert!(!retryable_response(400, None));
    }
}
