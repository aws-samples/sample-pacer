//! Gates 3.10 (the size half) and 3.11 — **which writes the scatter declines, and what
//! it leaves behind when it does.**
//!
//! Three shapes are deliberately *not* scattered, and each one has to say so:
//!
//! * an object below the size threshold — [`MIN_SCATTER_BYTES`], `declined: too_small`;
//! * an **overwrite**, whatever its size — `declined: overwrite`, because populating a
//!   new version over an old one's chunk keys would leave stale copies at holders this
//!   write never hears about;
//! * a client driving its own multipart upload — not a PUT at all, so the daemon's
//!   `upload_part` is passthrough and populates nothing.
//!
//! In each the object still reads back correctly, which is what makes "populates
//! nothing" a warmth gap rather than a correctness one. Gate 3.10's *other* half — that
//! enabling the scatter on Express is refused — is a startup check, and lives in
//! `config::tests::enabling_the_scatter_on_express_fails_startup`.
//!
//! [`MIN_SCATTER_BYTES`]: super::MIN_SCATTER_BYTES

use aws_sdk_s3::primitives::ByteStream;

use super::{body, declined, fleet, CHUNK_SIZE, OBJECT_LEN, ROOMY_STAGING};
use crate::common::BUCKET;

/// Gate 3.10: an object below the size threshold — and any overwrite, whatever its
/// size — takes ADR-0007's path instead, keeps a non-composite ETag, and says which
/// reason applied. The overwrite arm doubles as the read-after-write check that
/// matters most once writes populate: the *old* version's cached chunks must die.
#[tokio::test]
async fn a_small_object_and_an_overwrite_decline_to_scatter() {
    let h = fleet(ROOMY_STAGING).await;
    let node = &h.nodes[0];

    let small = body(2, CHUNK_SIZE);
    let small_key = "scatter/small.bin";
    let out = node
        .client
        .put_object()
        .bucket(BUCKET)
        .key(small_key)
        .body(ByteStream::from(small.clone()))
        .send()
        .await
        .unwrap();
    assert_eq!(declined(node, "too_small"), 1);
    assert_eq!(node.metrics.scatter.scattered.get(), 0);
    assert!(
        !out.e_tag().unwrap_or_default().contains('-'),
        "an unscattered PUT keeps the backend's own single-part ETag"
    );
    assert_eq!(h.read_backend(small_key).await, small);

    // First write of a fresh key scatters and populates.
    let over_key = "scatter/overwrite.bin";
    let first = body(3, OBJECT_LEN);
    node.client
        .put_object()
        .bucket(BUCKET)
        .key(over_key)
        .body(ByteStream::from(first.clone()))
        .send()
        .await
        .unwrap();
    assert_eq!(node.metrics.scatter.scattered.get(), 1);
    let windows_of = h.plan(over_key, OBJECT_LEN);
    for w in &windows_of {
        h.wait_announced(&w.home, &w.chunk_key).await;
    }

    // The second write of the same key must decline: populating a new version over
    // an old one's chunk keys would leave stale copies at holders this write never
    // hears about, so it needs ADR-0007's awaited invalidation instead.
    let second = body(4, OBJECT_LEN);
    let out = node
        .client
        .put_object()
        .bucket(BUCKET)
        .key(over_key)
        .body(ByteStream::from(second.clone()))
        .send()
        .await
        .unwrap();
    assert_eq!(declined(node, "overwrite"), 1);
    assert_eq!(
        node.metrics.scatter.scattered.get(),
        1,
        "the overwrite must not have scattered"
    );
    assert!(
        !out.e_tag().unwrap_or_default().contains('-'),
        "the overwrite took the plain PUT path, so its ETag is single-part"
    );
    assert_eq!(h.read_backend(over_key).await, second);
    for idx in 0..h.nodes.len() {
        assert_eq!(
            h.read_whole(idx, over_key).await,
            second,
            "stale bytes through node {} after an overwrite of populated chunks",
            h.nodes[idx].name
        );
    }
}

/// Gate 3.11: a client driving its own multipart upload through a node.
///
/// The scatter has nothing to do with it — the daemon's `upload_part` is passthrough —
/// so the answer to "which chunk-aligned interiors populate" is **none**, whatever the
/// client's part size, and this pins that rather than leaving it to be discovered. The
/// object still reads back correctly and fills on the first read like any other
/// backend object, which is what makes "populates nothing" a warmth gap and not a
/// correctness one.
#[tokio::test]
async fn a_client_driven_multipart_upload_populates_nothing() {
    let h = fleet(ROOMY_STAGING).await;
    let node = &h.nodes[0];
    let key = "scatter/client-mpu.bin";
    // Two chunk-aligned parts: the case that *could* populate if the daemon tried.
    let parts = [body(14, CHUNK_SIZE), body(15, CHUNK_SIZE)];

    let created = node
        .client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = created.upload_id().unwrap().to_owned();
    let mut completed = Vec::new();
    for (i, part) in parts.iter().enumerate() {
        let number = i32::try_from(i).unwrap() + 1;
        let out = node
            .client
            .upload_part()
            .bucket(BUCKET)
            .key(key)
            .upload_id(&upload_id)
            .part_number(number)
            .body(ByteStream::from(part.clone()))
            .send()
            .await
            .unwrap();
        completed.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(number)
                .e_tag(out.e_tag().unwrap())
                .build(),
        );
    }
    node.client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(
        node.metrics.scatter.scattered.get(),
        0,
        "a client's multipart upload is not a PUT and must not scatter"
    );
    let whole = bytes::Bytes::from(parts.concat());
    let windows = h.plan(key, whole.len() as u64);
    for w in &windows {
        assert!(
            !h.is_announced(&w.home, &w.chunk_key).await,
            "a client multipart upload populated window {} — it is passthrough today",
            w.index
        );
    }
    // Correct, and warm afterwards by the ordinary read path rather than by the write.
    assert_eq!(h.read_whole(0, key).await, whole);
    for w in &windows {
        h.wait_announced(&w.home, &w.chunk_key).await;
    }
}
