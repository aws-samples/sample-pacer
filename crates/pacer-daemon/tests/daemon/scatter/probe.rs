//! What stands in for S3 under the scatter fleet, and what that can and cannot prove.
//!
//! [`ProbeFs`] is `s3s-fs` plus the two things the ADR-0032 design leans on and a
//! filesystem backend does not do:
//!
//! * **the checksums are enforced** — a per-part CRC32 against the bytes that arrived,
//!   and the `FULL_OBJECT` CRC32 at Complete against the assembly of the parts *as
//!   stored*;
//! * **failures can be injected** — Complete made to fail, one part corrupted either
//!   before its own digest is checked or after it passes, or every `UploadPart` parked
//!   at the door for ever.
//!
//! That makes the coordinator's half of gate 3.8 testable in process: the digests it
//! computes are the ones a checking backend accepts, and a damaged window never
//! assembles. It does **not** re-prove that S3 itself enforces them — Phase 0 did that
//! against the real service (gates 0.2a–0.2c, `spike/mpu-scatter`), and no in-process
//! fake can add to it.
//!
//! Only the operations the fleet drives are implemented; the rest keep the `S3` trait's
//! `NotImplemented` default, so an unexpected call fails loudly instead of passing
//! through unobserved.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::StreamExt;
use s3s::dto;
use s3s::{s3_error, S3Request, S3Response, S3Result};

/// Failures a test asks the backend to produce, and what it recorded doing.
///
/// Atomics rather than a lock: they are set before a request runs and read from inside
/// it, on whatever thread the runtime picked.
#[derive(Debug, Default)]
pub(super) struct Faults {
    /// Fail every `CompleteMultipartUpload` (gate 3.6).
    pub(super) fail_complete: AtomicBool,
    /// Corrupt this part number as it arrives, *before* its own digest is checked —
    /// a window damaged on the wire (gate 3.8, Phase-0 control 0.2c). `0` is off,
    /// since S3 part numbers start at 1.
    pub(super) corrupt_on_arrival: AtomicI32,
    /// Corrupt this part number *after* its digest passes, so only the whole-object
    /// digest can catch it (gate 3.8, Phase-0 control 0.2b).
    pub(super) corrupt_after_check: AtomicI32,
    /// `(key, upload_id)` of every abort the daemon issued.
    pub(super) aborts: Mutex<Vec<(String, String)>>,
    /// Parts that arrived carrying no per-part digest at all.
    pub(super) parts_without_digest: AtomicUsize,
    /// Whole-object digests actually verified — asserted non-zero so a checksum
    /// test cannot pass because the coordinator quietly stopped sending one.
    pub(super) full_object_checks: AtomicUsize,
    /// Park every `UploadPart` at the door, for ever, instead of serving it.
    ///
    /// How a test holds a coordinator's pipeline *full*: no window can finish, so
    /// no slot is returned, and whatever the coordinator does next is what it does
    /// with nothing in flight available. Deliberately never released — the one test
    /// that sets it asserts on the stalled pipeline and then drops the scatter, so
    /// there is nothing to wake and no wake-up race to get wrong.
    hold_uploads: AtomicBool,
    /// `UploadPart` calls currently parked by [`Faults::hold_uploads`], counted so a
    /// test can tell "the pipeline is full" from "the coordinator stalled early".
    uploads_parked: AtomicUsize,
}

impl Faults {
    /// Aborted upload ids for `key`.
    ///
    /// # Panics
    ///
    /// If the abort log's lock is poisoned.
    pub(super) fn aborts_for(&self, key: &str) -> Vec<String> {
        self.aborts
            .lock()
            .expect("aborts lock poisoned")
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, id)| id.clone())
            .collect()
    }

    /// Hold every later `UploadPart` for ever. See [`Faults::hold_uploads`].
    pub(super) fn hold_all_uploads(&self) {
        self.hold_uploads.store(true, Ordering::SeqCst);
    }

    /// How many `UploadPart` calls are parked right now.
    pub(super) fn uploads_parked(&self) -> usize {
        self.uploads_parked.load(Ordering::SeqCst)
    }
}

/// `s3s-fs` with the checksum enforcement and the fault injection the gates need.
pub(super) struct ProbeFs {
    inner: s3s_fs::FileSystem,
    faults: Arc<Faults>,
    /// CRC32 of each part *as stored*, keyed by `(upload_id, part_number)`, so
    /// Complete can check the assembly the way S3's `FULL_OBJECT` does. Hashers
    /// rather than bodies: a few bytes of state per part instead of the object.
    parts: Mutex<HashMap<(String, i32), crc32fast::Hasher>>,
}

impl ProbeFs {
    /// Wrap `inner`, reporting to and driven by `faults`.
    pub(super) fn new(inner: s3s_fs::FileSystem, faults: Arc<Faults>) -> Self {
        Self {
            inner,
            faults,
            parts: Mutex::new(HashMap::new()),
        }
    }

    /// Record one part's digest, replacing any earlier attempt at the same number
    /// (a retry of a lost `StoreChunk`, or the coordinator taking over a window an
    /// owner failed on — S3 keeps the last upload of a part number, and so must
    /// the digest this suite checks the assembly against).
    fn record_part(&self, upload_id: &str, part_number: i32, body: &[u8]) {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(body);
        self.parts
            .lock()
            .expect("parts lock poisoned")
            .insert((upload_id.to_owned(), part_number), hasher);
    }

    /// The CRC32 of `parts` concatenated in ascending part order, and the entries
    /// removed — an upload is completed or aborted exactly once.
    fn take_assembled_crc32(&self, upload_id: &str, numbers: &[i32]) -> u32 {
        let mut parts = self.parts.lock().expect("parts lock poisoned");
        let mut ordered: Vec<i32> = numbers.to_vec();
        ordered.sort_unstable();
        let mut whole = crc32fast::Hasher::new();
        for number in ordered {
            if let Some(part) = parts.remove(&(upload_id.to_owned(), number)) {
                whole.combine(&part);
            }
        }
        whole.finalize()
    }
}

/// Collect a request body into one buffer. Fine for a test: the bodies are windows.
async fn collect_body(mut body: dto::StreamingBlob) -> S3Result<Bytes> {
    let mut buf = Vec::new();
    while let Some(frame) = body.next().await {
        let frame = frame.map_err(|e| s3_error!(InternalError, "probe body read failed: {e}"))?;
        buf.extend_from_slice(&frame);
    }
    Ok(Bytes::from(buf))
}

/// Wrap bytes back into a request body, to hand on to the real backend.
fn body_of(bytes: Bytes) -> dto::StreamingBlob {
    dto::StreamingBlob::wrap(futures::stream::once(async move {
        Ok::<_, std::io::Error>(bytes)
    }))
}

/// Damage `body` if `part` is the number a test asked to corrupt.
fn corrupt_if(body: Bytes, part: i32, chosen: &AtomicI32) -> Bytes {
    if chosen.load(Ordering::Relaxed) != part {
        return body;
    }
    let mut damaged = body.to_vec();
    // One bit is enough for a CRC32 and keeps the length — a short body would be
    // caught by the length check instead of by the digest, proving nothing.
    damaged[0] ^= 0xff;
    Bytes::from(damaged)
}

#[async_trait::async_trait]
impl s3s::S3 for ProbeFs {
    async fn create_bucket(
        &self,
        req: S3Request<dto::CreateBucketInput>,
    ) -> S3Result<S3Response<dto::CreateBucketOutput>> {
        self.inner.create_bucket(req).await
    }

    async fn head_bucket(
        &self,
        req: S3Request<dto::HeadBucketInput>,
    ) -> S3Result<S3Response<dto::HeadBucketOutput>> {
        self.inner.head_bucket(req).await
    }

    async fn put_object(
        &self,
        req: S3Request<dto::PutObjectInput>,
    ) -> S3Result<S3Response<dto::PutObjectOutput>> {
        self.inner.put_object(req).await
    }

    async fn get_object(
        &self,
        req: S3Request<dto::GetObjectInput>,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        self.inner.get_object(req).await
    }

    async fn head_object(
        &self,
        req: S3Request<dto::HeadObjectInput>,
    ) -> S3Result<S3Response<dto::HeadObjectOutput>> {
        self.inner.head_object(req).await
    }

    async fn delete_object(
        &self,
        req: S3Request<dto::DeleteObjectInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectOutput>> {
        self.inner.delete_object(req).await
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<dto::ListObjectsV2Input>,
    ) -> S3Result<S3Response<dto::ListObjectsV2Output>> {
        self.inner.list_objects_v2(req).await
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<dto::CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CreateMultipartUploadOutput>> {
        self.inner.create_multipart_upload(req).await
    }

    /// Enforce the per-part digest the way S3 does, with the corruption hooks on
    /// either side of the check.
    async fn upload_part(
        &self,
        mut req: S3Request<dto::UploadPartInput>,
    ) -> S3Result<S3Response<dto::UploadPartOutput>> {
        // Before the body is even read: a held upload must occupy its window's slot
        // exactly as a slow one would, and read nothing while it does.
        if self.faults.hold_uploads.load(Ordering::SeqCst) {
            self.faults.uploads_parked.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
        }
        let Some(body) = req.input.body.take() else {
            return Err(s3_error!(IncompleteBody, "UploadPart with no body"));
        };
        let part = req.input.part_number;
        let arrived = corrupt_if(
            collect_body(body).await?,
            part,
            &self.faults.corrupt_on_arrival,
        );
        match &req.input.checksum_crc32 {
            None => {
                self.faults
                    .parts_without_digest
                    .fetch_add(1, Ordering::Relaxed);
            }
            Some(claimed) if *claimed != base64_crc32(&arrived) => {
                return Err(s3_error!(BadDigest, "part CRC32 does not match the bytes"));
            }
            Some(_) => {}
        }
        let stored = corrupt_if(arrived, part, &self.faults.corrupt_after_check);
        self.record_part(&req.input.upload_id, part, &stored);
        req.input.body = Some(body_of(stored));
        self.inner.upload_part(req).await
    }

    /// Enforce the `FULL_OBJECT` digest over the parts as stored, and fail here
    /// when a test asked for it.
    async fn complete_multipart_upload(
        &self,
        req: S3Request<dto::CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CompleteMultipartUploadOutput>> {
        if self.faults.fail_complete.load(Ordering::Relaxed) {
            return Err(s3_error!(InternalError, "injected Complete failure"));
        }
        let numbers: Vec<i32> = req
            .input
            .multipart_upload
            .as_ref()
            .and_then(|m| m.parts.as_ref())
            .map(|parts| parts.iter().filter_map(|p| p.part_number).collect())
            .unwrap_or_default();
        let assembled = self.take_assembled_crc32(&req.input.upload_id, &numbers);
        // Only a FULL_OBJECT claim is a digest of the assembly. A COMPOSITE one — what
        // an SDK sends for a client-driven multipart upload — is a digest of digests,
        // and checking it against the concatenation would reject a valid upload.
        let full_object = req
            .input
            .checksum_type
            .as_ref()
            .is_some_and(|t| t.as_str() == dto::ChecksumType::FULL_OBJECT);
        if let (Some(claimed), true) = (&req.input.checksum_crc32, full_object) {
            self.faults
                .full_object_checks
                .fetch_add(1, Ordering::Relaxed);
            if *claimed != base64_of(assembled) {
                return Err(s3_error!(
                    BadDigest,
                    "whole-object CRC32 does not match the assembled parts"
                ));
            }
        }
        self.inner.complete_multipart_upload(req).await
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<dto::AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::AbortMultipartUploadOutput>> {
        self.faults
            .aborts
            .lock()
            .expect("aborts lock poisoned")
            .push((req.input.key.clone(), req.input.upload_id.clone()));
        self.inner.abort_multipart_upload(req).await
    }

    async fn list_parts(
        &self,
        req: S3Request<dto::ListPartsInput>,
    ) -> S3Result<S3Response<dto::ListPartsOutput>> {
        self.inner.list_parts(req).await
    }
}

/// CRC32 of `body` in S3's `ChecksumCRC32` form.
pub(super) fn base64_crc32(body: &[u8]) -> String {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(body);
    base64_of(hasher.finalize())
}

/// Base64 of a big-endian `u32`, which is how S3 carries a CRC32.
///
/// Written out here rather than reused from the daemon on purpose: this is the
/// oracle the daemon's own encoder is checked against, and sharing an
/// implementation would make an endianness slip agree with itself. Pinned by
/// [`the_probe_encodes_a_checksum_the_way_s3_reports_it`].
pub(super) fn base64_of(value: u32) -> String {
    /// Standard base64 alphabet (RFC 4648) — not the URL-safe variant.
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = value.to_be_bytes();
    let triple = (u32::from(bytes[0]) << 16) | (u32::from(bytes[1]) << 8) | u32::from(bytes[2]);
    let mut out = String::with_capacity(8);
    for shift in [18, 12, 6, 0] {
        out.push(ALPHABET[((triple >> shift) & 0x3f) as usize] as char);
    }
    // The fourth byte is a partial group: 8 bits become two characters plus one
    // padding group.
    let tail = u32::from(bytes[3]) << 4;
    out.push(ALPHABET[((tail >> 6) & 0x3f) as usize] as char);
    out.push(ALPHABET[(tail & 0x3f) as usize] as char);
    out.push_str("==");
    out
}

/// The CRC32 S3 reported for the Phase-0 probe object, and its base64 form.
///
/// The one value in this suite that came from the real service. Without it every
/// checksum assertion below is self-consistent and could still be wrong.
const PHASE_0_PROBE_CRC32: (u32, &str) = (0x8b1d_7a0b, "ix16Cw==");

/// The suite's own checksum encoder must agree with what S3 reported for the
/// Phase-0 probe object, or every checksum assertion below is self-consistent and
/// wrong.
///
/// A pure unit test with no daemon dependency, and it stays in the integration binary
/// anyway: the thing it pins is [`base64_of`] a few lines up, which exists *only* here
/// on purpose — it is the independent oracle the daemon's own encoder is checked
/// against, and moving either half into `src/` would either make an endianness slip
/// agree with itself or ship test-only code in the crate. Adjacent to what it pins
/// beats adjacent to nothing.
#[test]
fn the_probe_encodes_a_checksum_the_way_s3_reports_it() {
    let (value, reported) = PHASE_0_PROBE_CRC32;
    assert_eq!(base64_of(value), reported);
}
