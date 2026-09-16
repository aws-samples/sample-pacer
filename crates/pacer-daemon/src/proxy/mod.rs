//! The S3-facing service: cached GETs, write-through everything else.
//!
//! [`PacerProxy`] is the [`s3s::S3`] implementation the client-facing listener
//! serves: cache policy in front of a re-signing proxy (ADR-0002 policy, ADR-0006
//! strip-and-re-sign). This module owns the type — its fields, its construction
//! and the builder-set policy on it — plus the trait impl, which is deliberately
//! thin: every operation dispatches into the sibling module that owns that path,
//! so the S3 surface reads as a table of contents rather than as the
//! implementation.
//!
//! * `read` — the cached GET: header, range, covering chunks in order
//!   (ADR-0002/ADR-0011/ADR-0015).
//! * `write` — PUT/COPY/DELETE/multipart: the scattered write (ADR-0032), else
//!   proxy-and-invalidate (ADR-0007/ADR-0023).
//! * `fill` — one chunk's resolution and the node-wide fill guard that dedupes it
//!   (ADR-0015/ADR-0016/ADR-0017/ADR-0028).
//! * `cluster` — [`Cluster`]: who homes a key, who holds it, in what order to ask
//!   (ADR-0012/ADR-0016/ADR-0017).
//! * `target` — the client memory a request named (ADR-0026/ADR-0027/ADR-0030).
//! * `deliver` — the decision to deliver into it, and the header-only answer
//!   (ADR-0026/ADR-0030).
//! * `place` — one window's bytes into that memory, by copy or one-sided WRITE
//!   (ADR-0026 point 4, ADR-0018, ADR-0030).
//!
//! Everything a *passthrough* op does is here in full, because there is nothing to
//! it: count the op, resolve the bucket alias (ADR-0002), forward to `inner`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use pacer_backend::retry::RetryPolicy;
use pacer_backend::BackendType;
use pacer_cache::chunk::ChunkConfig;
use pacer_cache::tier::ChunkTier;
use pacer_cache::Promotion;
use s3s::{dto, S3Request, S3Response, S3Result, S3};

use crate::cachefill::ChunkFill;
use crate::delivery::{DeliveryConfig, DeliveryQuota};
use crate::metrics::Metrics;

mod cluster;
mod deliver;
mod fill;
mod place;
mod read;
mod target;
mod write;

pub use cluster::Cluster;

pub(crate) use cluster::chunk_sources;
pub(crate) use fill::FillGuard;

/// The S3 service implementation: cache policy in front of a re-signing proxy.
pub struct PacerProxy {
    /// Passthrough path: converts s3s DTOs ↔ aws-sdk-s3 and re-signs with the
    /// daemon's own identity (strip-and-re-sign, ADR-0006).
    inner: s3s_aws::Proxy,
    /// Direct backend client (same identity as `inner`, ADR-0006) for the
    /// per-chunk range GETs and the suffix-range HEAD fallback — the chunked read
    /// path issues its own ranged backend reads rather than one whole-object GET.
    backend: aws_sdk_s3::Client,
    tier: ChunkTier,
    metrics: Metrics,
    min_object_size: u64,
    /// Optional whole-object admission cap; `None` = unbounded (ADR-0015: an
    /// object of any size is chunk-cached, so this is a policy valve).
    max_object_size: Option<u64>,
    /// Chunking (ADR-0015): key derivation + covering-chunk math.
    chunk: ChunkConfig,
    /// Max concurrent chunk resolutions per GET (memory bound = this × chunk_size).
    fill_parallelism: usize,
    /// How hard one chunk's backend read tries before the client's GET fails
    /// (`pacer_backend::retry`). The default is not "off": a chunked read turns a
    /// client GET into N backend GETs, and without a retry the *first* transient
    /// fault among the N truncates a response whose status line already said 200.
    read_retry: RetryPolicy,
    /// Client-facing bucket aliases → real backend buckets. On an **Express**
    /// backend this is mandatory: directory-bucket names (`*--x-s3`) trip
    /// Express-specific SDK behavior (zonal DNS, `s3express` signing scope) that
    /// a proxy endpoint can't honor, so clients address an alias and the daemon
    /// rewrites it. On a **Standard** backend (ADR-0023) the real name is an
    /// ordinary regional bucket that trips no such behavior, so aliasing is
    /// optional — clients may address the real bucket directly. The rewrite
    /// itself is a plain string map, identical for both backends.
    bucket_map: HashMap<String, String>,
    /// Keys with a tee fill in flight — one concurrent fill per object.
    /// Shared with the peer server (one fill per key node-wide).
    filling: Arc<Mutex<HashSet<String>>>,
    /// Cluster tier (Phase 2): ring + peer transport. None = single-node.
    cluster: Option<Cluster>,
    /// Backend shape (ADR-0023). Gates the Express-only request normalization
    /// on the write path (consecutive multipart parts, Content-MD5 stripping);
    /// Standard uses general-purpose semantics for both.
    backend_type: BackendType,
    /// Client-memory delivery settings (ADR-0026). Disabled by default, in which
    /// case a target header is ignored and every read is body-delivered.
    delivery: DeliveryConfig,
    /// Node-wide pinned-client-memory accounting, shared with the metrics layer
    /// so `pacer_delivery_pinned_bytes` reports the same number the admission
    /// decision used.
    delivery_quota: Arc<DeliveryQuota>,
    /// Where a cached chunk's bytes go (ADR-0028) — the same value the peer
    /// server holds, so both fill paths agree. Defaults to the heap.
    fill: ChunkFill,
    /// Whether a chunk read promotes a disk hit into the RAM tier — the same value
    /// the peer server holds, so both read paths agree. Defaults to foyer's own
    /// promoting behaviour.
    promotion: Promotion,
    /// Drives scattered PUTs (ADR-0032). `None` — the default — means every write
    /// takes ADR-0007's proxy-and-invalidate path, which is also what a
    /// single-node daemon gets: there are no homes to scatter to.
    scatter: Option<Arc<crate::coordinate::ScatterCoordinator>>,
    /// Smallest object worth scattering. Distinct from [`Self::min_object_size`],
    /// which decides what is *cacheable*: an object can be well worth caching and
    /// still too small for the extra round trips and the composite-ETag change.
    scatter_min_object_bytes: u64,
    /// Whether an `If-Match` GET may be served from cache on an ETag match (ADR-0039).
    /// `false` restores the unconditional passthrough every conditional GET took before it.
    conditional_get_from_cache: bool,
}

impl PacerProxy {
    /// Single-node proxy with no bucket aliases — add them with
    /// [`Self::with_bucket_map`].
    ///
    /// Every optional knob on this type is a builder method rather than a
    /// parameter (`with_scatter`, `with_cluster`, `with_delivery`,
    /// `with_promotion`, `with_chunk_fill`, `with_read_retry`,
    /// `with_backend_type`, `with_bucket_map`), which is what keeps this list at
    /// the seven values a proxy cannot be built without.
    pub fn new(
        client: aws_sdk_s3::Client,
        tier: ChunkTier,
        metrics: Metrics,
        min_object_size: u64,
        max_object_size: Option<u64>,
        chunk: ChunkConfig,
        fill_parallelism: usize,
    ) -> Self {
        Self {
            inner: s3s_aws::Proxy::from(client.clone()),
            backend: client,
            tier,
            metrics,
            min_object_size,
            max_object_size,
            chunk,
            fill_parallelism,
            read_retry: RetryPolicy::default(),
            // No client-facing aliases unless an operator configured some
            // (ADR-0002): an empty map resolves every bucket to itself.
            bucket_map: HashMap::new(),
            filling: Arc::new(Mutex::new(HashSet::new())),
            cluster: None,
            // Defaults to Express (ADR-0002/0023) so callers that don't set a
            // backend shape keep the historical behavior; main.rs overrides it
            // from config via `with_backend_type`.
            backend_type: BackendType::Express,
            // Delivery off unless an operator asks for it (ADR-0026): mapping and
            // pinning memory a client named is a privilege, not a default.
            delivery: DeliveryConfig::default(),
            delivery_quota: Arc::new(DeliveryQuota::new(
                DeliveryConfig::default().pinned_bytes_max,
                DeliveryConfig::default().max_target_bytes,
            )),
            // Heap unless a slab is handed in: ADR-0028 is off by default.
            fill: ChunkFill::default(),
            // foyer's own behaviour unless an operator opts out.
            promotion: Promotion::default(),
            // Scatter off unless an operator asks for it (ADR-0032): it changes
            // the ETag a client sees, so it is never a default.
            scatter: None,
            scatter_min_object_bytes: crate::scatter::DEFAULT_MIN_SCATTER_BYTES,
            // ADR-0039's default, stated here as well as in config.rs so a caller that
            // builds a proxy directly (every integration suite does) gets the shipped
            // behaviour rather than the strict one, and the suites therefore exercise what
            // a deployment runs.
            conditional_get_from_cache: true,
        }
    }

    /// Whether an `If-Match` GET may be served from cache on an ETag match (ADR-0039).
    ///
    /// Builder-style like every other policy here. `false` is the pre-ADR-0039 behaviour:
    /// every conditional GET passes through untouched.
    #[must_use]
    pub fn with_conditional_get_from_cache(mut self, allow: bool) -> Self {
        self.conditional_get_from_cache = allow;
        self
    }

    /// Resolve client-facing bucket aliases through `bucket_map` (ADR-0002):
    /// `alias → real bucket`, applied to every request before it reaches the
    /// backend.
    ///
    /// Builder-style rather than a constructor argument because it is genuinely
    /// optional — an empty map means "every bucket is itself", which is what a
    /// single-bucket deployment and all nine integration suites want. It was the
    /// eighth parameter of a `new` that had to suppress `too_many_arguments` to
    /// exist; demoting it removed the last lint suppression in this module.
    #[must_use]
    pub fn with_bucket_map(mut self, bucket_map: HashMap<String, String>) -> Self {
        self.bucket_map = bucket_map;
        self
    }

    /// Scatter qualifying PUTs through `coordinator` (ADR-0032).
    ///
    /// Builder-style and separate from [`Self::with_cluster`] even though the
    /// scatter needs a cluster: the two are independently configurable, and a
    /// clustered daemon with the scatter off must keep ADR-0007's write path
    /// exactly as it was.
    #[must_use]
    pub fn with_scatter(
        mut self,
        coordinator: Arc<crate::coordinate::ScatterCoordinator>,
        min_object_bytes: u64,
    ) -> Self {
        self.scatter = Some(coordinator);
        self.scatter_min_object_bytes = min_object_bytes;
        self
    }

    /// Put cached chunk bytes in ADR-0028's registered slab instead of the heap.
    ///
    /// Takes the same [`ChunkFill`] the peer server is given, because a node
    /// where only one of the two fill paths uses the slab is the failure mode
    /// [`crate::cachefill`] documents: the slab looks healthy and does nothing.
    #[must_use]
    pub fn with_chunk_fill(mut self, fill: ChunkFill) -> Self {
        self.fill = fill;
        self
    }

    /// Set whether a chunk read promotes a disk hit into the RAM tier.
    ///
    /// Takes the same [`Promotion`] the peer server is given, for the same reason
    /// [`Self::with_chunk_fill`] does: the two read paths must not disagree about
    /// what a disk hit costs, or `foyer_memory_op` stops meaning one thing.
    #[must_use]
    pub fn with_promotion(mut self, promotion: Promotion) -> Self {
        self.promotion = promotion;
        self
    }

    /// Override how hard a chunk's backend read tries before the client's GET
    /// fails.
    ///
    /// Exists for tests that need the exhaustion path deterministically (a
    /// one-attempt policy is the pre-retry behaviour) — production takes
    /// [`RetryPolicy::default`], whose values are justified in
    /// [`pacer_backend::retry`].
    #[must_use]
    pub fn with_read_retry(mut self, policy: RetryPolicy) -> Self {
        self.read_retry = policy;
        self
    }

    /// Enable the cluster tier: misses for peer-owned keys go through the
    /// transport instead of the backend (ADR-0012).
    pub fn with_cluster(mut self, cluster: Cluster) -> Self {
        self.cluster = Some(cluster);
        self
    }

    /// Select the backend shape (ADR-0023). Standard relaxes the Express-only
    /// write-path normalization (consecutive multipart parts, Content-MD5
    /// stripping). Defaults to Express when never called.
    pub fn with_backend_type(mut self, backend_type: BackendType) -> Self {
        self.backend_type = backend_type;
        self
    }

    /// Enable client-memory delivery (ADR-0026) with `cfg`'s ceilings. The
    /// `quota` is passed in rather than built here so the metrics layer can
    /// report the same accounting object — a second quota would publish a gauge
    /// that no admission decision ever consulted.
    pub fn with_delivery(mut self, cfg: DeliveryConfig, quota: Arc<DeliveryQuota>) -> Self {
        self.delivery = cfg;
        self.delivery_quota = quota;
        self
    }

    /// The in-flight-fill guard, shared with the peer server so a client GET
    /// and a peer FetchBlob never fill the same key concurrently.
    pub fn filling(&self) -> Arc<Mutex<HashSet<String>>> {
        Arc::clone(&self.filling)
    }

    fn count(&self, op: &str) {
        self.metrics.ops_total.with_label_values(&[op]).inc();
    }

    /// Rewrite a client-facing bucket alias to the real backend bucket.
    fn map_bucket(&self, bucket: &mut String) {
        if let Some(real) = self.bucket_map.get(bucket.as_str()) {
            *bucket = real.clone();
        }
    }
}

#[async_trait::async_trait]
impl S3 for PacerProxy {
    async fn get_object(
        &self,
        req: S3Request<dto::GetObjectInput>,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        self.serve_get(req).await
    }

    // ---- write path: proxy + invalidate (ADR-0007) ----

    async fn put_object(
        &self,
        req: S3Request<dto::PutObjectInput>,
    ) -> S3Result<S3Response<dto::PutObjectOutput>> {
        self.scatter_or_write_through(req).await
    }

    async fn copy_object(
        &self,
        req: S3Request<dto::CopyObjectInput>,
    ) -> S3Result<S3Response<dto::CopyObjectOutput>> {
        self.copy_and_invalidate(req).await
    }

    async fn delete_object(
        &self,
        req: S3Request<dto::DeleteObjectInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectOutput>> {
        self.delete_and_invalidate(req).await
    }

    async fn delete_objects(
        &self,
        req: S3Request<dto::DeleteObjectsInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectsOutput>> {
        self.delete_many_and_invalidate(req).await
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<dto::CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CompleteMultipartUploadOutput>> {
        self.complete_mpu_and_invalidate(req).await
    }

    // ---- passthrough ----

    async fn head_bucket(
        &self,
        mut req: S3Request<dto::HeadBucketInput>,
    ) -> S3Result<S3Response<dto::HeadBucketOutput>> {
        self.count("head_bucket");
        self.map_bucket(&mut req.input.bucket);
        self.inner.head_bucket(req).await
    }

    async fn head_object(
        &self,
        mut req: S3Request<dto::HeadObjectInput>,
    ) -> S3Result<S3Response<dto::HeadObjectOutput>> {
        self.count("head_object");
        self.map_bucket(&mut req.input.bucket);
        self.inner.head_object(req).await
    }

    async fn list_buckets(
        &self,
        req: S3Request<dto::ListBucketsInput>,
    ) -> S3Result<S3Response<dto::ListBucketsOutput>> {
        self.count("list_buckets");
        self.inner.list_buckets(req).await
    }

    async fn list_objects(
        &self,
        mut req: S3Request<dto::ListObjectsInput>,
    ) -> S3Result<S3Response<dto::ListObjectsOutput>> {
        self.count("list_objects");
        self.map_bucket(&mut req.input.bucket);
        self.inner.list_objects(req).await
    }

    async fn list_objects_v2(
        &self,
        mut req: S3Request<dto::ListObjectsV2Input>,
    ) -> S3Result<S3Response<dto::ListObjectsV2Output>> {
        self.count("list_objects_v2");
        self.map_bucket(&mut req.input.bucket);
        self.inner.list_objects_v2(req).await
    }

    async fn get_bucket_location(
        &self,
        mut req: S3Request<dto::GetBucketLocationInput>,
    ) -> S3Result<S3Response<dto::GetBucketLocationOutput>> {
        self.count("get_bucket_location");
        self.map_bucket(&mut req.input.bucket);
        self.inner.get_bucket_location(req).await
    }

    async fn get_object_attributes(
        &self,
        mut req: S3Request<dto::GetObjectAttributesInput>,
    ) -> S3Result<S3Response<dto::GetObjectAttributesOutput>> {
        self.count("get_object_attributes");
        self.map_bucket(&mut req.input.bucket);
        self.inner.get_object_attributes(req).await
    }

    async fn create_multipart_upload(
        &self,
        mut req: S3Request<dto::CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CreateMultipartUploadOutput>> {
        self.count("create_multipart_upload");
        self.map_bucket(&mut req.input.bucket);
        self.inner.create_multipart_upload(req).await
    }

    async fn upload_part(
        &self,
        mut req: S3Request<dto::UploadPartInput>,
    ) -> S3Result<S3Response<dto::UploadPartOutput>> {
        self.count("upload_part");
        self.map_bucket(&mut req.input.bucket);
        // Express rejects Content-MD5; Standard accepts it — same gate as
        // put_object (ADR-0023).
        if self.backend_type.is_express() {
            req.input.content_md5 = None;
        }
        self.inner.upload_part(req).await
    }

    async fn upload_part_copy(
        &self,
        mut req: S3Request<dto::UploadPartCopyInput>,
    ) -> S3Result<S3Response<dto::UploadPartCopyOutput>> {
        self.count("upload_part_copy");
        self.map_bucket(&mut req.input.bucket);
        self.inner.upload_part_copy(req).await
    }

    async fn abort_multipart_upload(
        &self,
        mut req: S3Request<dto::AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::AbortMultipartUploadOutput>> {
        self.count("abort_multipart_upload");
        self.map_bucket(&mut req.input.bucket);
        self.inner.abort_multipart_upload(req).await
    }

    async fn list_multipart_uploads(
        &self,
        mut req: S3Request<dto::ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<dto::ListMultipartUploadsOutput>> {
        self.count("list_multipart_uploads");
        self.map_bucket(&mut req.input.bucket);
        self.inner.list_multipart_uploads(req).await
    }

    async fn list_parts(
        &self,
        mut req: S3Request<dto::ListPartsInput>,
    ) -> S3Result<S3Response<dto::ListPartsOutput>> {
        self.count("list_parts");
        self.map_bucket(&mut req.input.bucket);
        self.inner.list_parts(req).await
    }
}
