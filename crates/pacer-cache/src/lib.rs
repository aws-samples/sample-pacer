//! foyer HybridCache (RAM + NVMe) wrapper carrying the cache policy:
//! GETs only, whole objects > `min_object_size`, LRU eviction, Cache-Control
//! bypass, range reads served by slicing the cached whole object.
//!
//! Phase 3 (ADR-0015) adds chunk-granular addressing in [`chunk`]; the read
//! path migrates onto it incrementally (B1).

use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

pub mod admission;
pub mod chunk;
mod codec;
pub mod frames;
pub mod slot;
pub mod store;
pub mod tier;

use bytes::Bytes;
use foyer::{
    BlockEngineConfig, Device, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCacheProperties, Load, LruConfig, PsyncIoEngineConfig, Spawner, StorageKey, StorageValue,
};
use serde::{Deserialize, Serialize};

/// Cache name foyer tags its metrics with (Prometheus label, ADR-0002).
///
/// Public because it is the `name` label on every `foyer_*` series this crate's
/// caches publish, so anything that reads one — a dashboard query, or a test
/// asserting on the memory tier's residency — has to address it. That was a
/// hardcoded `"pacer"` in the daemon's test suite, which would have failed as
/// "expected exactly one series, found 0" if this were ever renamed; now the
/// rename moves both sides at once.
pub const CACHE_NAME: &str = "pacer";
/// Default io_uring worker threads. Sized for NVMe queue depths; sqpoll
/// deliberately off (burns a core). Operator-tunable via config (ADR-0013).
const DEFAULT_URING_THREADS: usize = 4;
/// Default io_uring submission-queue depth per thread (NVMe-queue-depth
/// territory). Operator-tunable via config (ADR-0013).
const DEFAULT_URING_IO_DEPTH: usize = 256;
/// Thread-name prefix for a dedicated storage runtime, so `top`/`perf` can tell
/// the disk tier's CPU apart from the main runtime's (see
/// [`StorageTuning::storage_runtime_threads`]).
const STORAGE_RUNTIME_THREAD_NAME: &str = "pacer-foyer";

/// A whole S3 object held in cache (whole-object caching only,
/// range requests are served by slicing the cached body).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedObject {
    /// The complete object body.
    pub body: Bytes,
    /// Backend ETag, replayed on cache hits.
    pub e_tag: Option<String>,
    /// Backend Content-Type, replayed on cache hits.
    pub content_type: Option<String>,
    /// Seconds since epoch, from the backend's Last-Modified.
    pub last_modified_epoch_secs: Option<i64>,
}

/// The hybrid (RAM + NVMe) cache, keyed by [`object_key`].
pub type ObjectCache = HybridCache<String, CachedObject>;

/// A single value in the chunk-granular cache (ADR-0015). One `foyer`
/// `HybridCache` holds both kinds, distinguished by key shape — object headers
/// under the plain `"{bucket}/{key}"` object key, chunk bodies under the
/// `"{bucket}/{key}#{size}:{index}"` chunk key (see [`chunk::ChunkConfig`]) — so
/// the two never collide and share one capacity/eviction pool. LRU evicts a cold
/// header and its chunks independently; a resurrected header simply refetches any
/// missing chunk (each chunk read is idempotent), so an evicted-header/live-chunk
/// skew costs at most a re-fetch, never a wrong serve.
///
/// **Deliberately does not derive `Serialize`/`Deserialize`.** The disk tier
/// serializes through foyer's `Code`, which the private `codec` module implements by
/// hand (no doc link on purpose — it is private, and rustdoc rejects the link) so a
/// promoted chunk's bytes are read into an ADR-0028 slab frame rather than a fresh
/// heap `Vec`. foyer's `serde` feature supplies a blanket `Code` impl for anything
/// deriving serde, and it would overlap this one — so the derives are what has to
/// go, and the byte format they produced is pinned by a test instead (the cache
/// directory outlives a rollout).
#[derive(Debug, Clone)]
pub enum CacheValue {
    /// Per-object metadata (keyed by the plain object key).
    Header(chunk::ObjectHeader),
    /// One chunk's bytes (keyed by the chunk key).
    Chunk(chunk::CachedChunk),
}

impl CacheValue {
    /// The byte weight foyer charges for this entry: a chunk costs its bytes; a
    /// header is metadata-only, charged a small fixed weight so headers can still
    /// be evicted under pressure but never dominate capacity accounting.
    fn weight(&self) -> usize {
        match self {
            CacheValue::Chunk(c) => c.body.len(),
            // Header holds no object bytes; a nominal weight (a few small strings)
            // keeps it in the LRU without letting metadata skew the byte budget.
            CacheValue::Header(_) => HEADER_WEIGHT,
        }
    }

    /// Borrow the object header, or `None` if this entry is a chunk.
    pub fn as_header(&self) -> Option<&chunk::ObjectHeader> {
        match self {
            CacheValue::Header(h) => Some(h),
            CacheValue::Chunk(_) => None,
        }
    }

    /// Borrow the cached chunk, or `None` if this entry is a header.
    pub fn as_chunk(&self) -> Option<&chunk::CachedChunk> {
        match self {
            CacheValue::Chunk(c) => Some(c),
            CacheValue::Header(_) => None,
        }
    }
}

/// Fixed LRU weight for a header entry (bytes). Headers carry no object payload;
/// this nominal charge keeps them evictable without letting metadata distort the
/// byte-capacity budget the chunks actually consume.
const HEADER_WEIGHT: usize = 256;

/// The chunk-granular hybrid cache (ADR-0015): one keyspace, [`CacheValue`]
/// entries. Replaces [`ObjectCache`] as the daemon's cache once the read path is
/// migrated (B1d); defined here so the storage layer builds and tests in
/// isolation first.
pub type ChunkCache = HybridCache<String, CacheValue>;

/// Sizing and placement of the two cache tiers.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Directory on local NVMe (instance-store RAID0) for the disk tier.
    pub dir: PathBuf,
    /// In-memory tier capacity in bytes.
    pub mem_capacity: usize,
    /// Disk tier capacity in bytes.
    pub disk_capacity: usize,
    /// Size of each block file on disk. Two hard constraints:
    /// - foyer's FsDevice opens one fd per block, so the fd budget bounds
    ///   `disk_capacity / block_size` (foyer's 16 MiB default blows past a
    ///   65k nofile limit at ~1 TiB);
    /// - a block is the eviction unit AND the max cacheable entry size, so
    ///   objects larger than this never reach the disk tier.
    pub block_size: usize,
    /// foyer flush (DRAM→NVMe write) buffer pool size; `0` = auto-size to
    /// `2 × block_size` ([`CacheConfig::flush_buffer_size_or_default`]). An
    /// entry that does not fit the flusher's io buffer is **silently dropped on
    /// demotion** — foyer's `Buffer::push` returns `false`, the piece is
    /// unpinned, and the only trace is a `queue_buffer_overflow` counter — so
    /// this must comfortably exceed the largest cacheable entry (`block_size`);
    /// foyer's built-in default (16 MiB) loses every chunk-sized entry at
    /// production chunk sizes. Discovered by the rung-1 NVMe saturation gate
    /// (planning/15 ladder): 32 GiB seeded, 0 bytes ever reached the disk tier.
    pub flush_buffer_size: usize,
    /// Disk I/O engine: `psync` (default, portable) or `uring` (Linux only;
    /// higher throughput at NVMe queue depths).
    pub io_engine: IoEngine,
    /// io_uring tuning; ignored unless `io_engine` is [`IoEngine::Uring`].
    pub uring: UringConfig,
    /// foyer storage-engine tuning; every field defaults to foyer's own value,
    /// so an untouched [`StorageTuning`] reproduces the pre-tuning behaviour.
    pub tuning: StorageTuning,
    /// Ceiling on a whole object admitted through the LEGACY (pre-chunking,
    /// ADR-0002) [`ObjectCache`] — read via
    /// [`Self::max_object_bytes_or_default`], never directly, since `None`
    /// means "use [`DEFAULT_MAX_OBJECT_BYTES`]", not "unbounded". Unrelated to
    /// the chunked path's own (deliberately unbounded) size gate — see
    /// [`should_admit_legacy_object`].
    pub max_object_bytes: Option<u64>,
}

/// foyer storage-engine knobs that foyer defaults conservatively and that the
/// C4 read-path investigation (2026-08-26) found to bound per-node read
/// throughput. Every field is `0` for "keep foyer's built-in default", so the
/// zero value is the historical behaviour and each knob can be moved alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StorageTuning {
    /// Blocks the storage engine keeps open and flushes into concurrently.
    /// `0` = foyer's default, which is **1**.
    ///
    /// This is a **read**-path control as much as a write one, which is the
    /// non-obvious part. One flusher appends every entry into one block until it
    /// fills, so a sequential fill lays `block_size / chunk_size` *consecutive*
    /// chunks into the same block (64 at a 1 GiB block and 16 MiB chunks). foyer
    /// then picks the io_uring worker for a read by `partition.id() %
    /// uring.threads`, and there is exactly one partition per block — so
    /// contiguous chunks read back through a **single** io_uring shard, whose
    /// page-cache `memcpy`s serialize inside one `io_uring_enter`, however many
    /// `uring.threads` were configured. Raising this hashes entries across that
    /// many concurrently-open blocks, which is what spreads the read-back across
    /// shards; raising `uring.threads` alone cannot, and measurably did not
    /// (`bench/ladder/results/c4-fanout-depth.md`: 4 -> 32 threads bought +16 %).
    pub flushers: usize,
    /// Concurrent block reclaimers. `0` = foyer's default, which is **1**.
    /// Reclaim frees whole blocks for reuse, so one reclaimer against `flushers`
    /// writers is what makes the engine run out of clean blocks under a
    /// sustained fill; keep it in step with `flushers`.
    pub reclaimers: usize,
    /// In-flight DRAM→NVMe write budget in bytes. An enqueue arriving when this
    /// much is already submitted-but-unwritten is **silently dropped** (foyer
    /// counts it in `storage_queue_channel_overflow` and the entry simply never
    /// reaches the disk tier, so a later read misses and goes to a peer or the
    /// backend). `0` = foyer's default of **16 MiB**, which equals *one* 16 MiB
    /// chunk entry and therefore admits about two chunks before it starts
    /// dropping — the C4 arm recorded 1463 such drops. Size it to the write
    /// fan-out actually wanted: `flushers × chunk_size × depth`.
    pub submit_queue_threshold: usize,
    /// Worker threads for a runtime dedicated to the disk tier. `0` = share the
    /// runtime that built the cache, which is foyer's default
    /// (`Spawner::current()`).
    ///
    /// Sharing is what the daemon did until this knob existed, and it puts every
    /// storage load's CPU — an XxHash64 over the whole entry plus the decode
    /// copy, 6.5 ms per 16 MiB chunk — on the main tokio runtime, against the
    /// RDMA completion pumps that already burn ~14 cores. A dedicated runtime
    /// isolates the two.
    pub storage_runtime_threads: usize,
}

impl CacheConfig {
    /// The flush buffer size actually applied: the configured value, or
    /// `2 × block_size` when unset (`0`). `block_size` is the max cacheable
    /// entry, so 2× guarantees the flusher's io buffer always fits one entry
    /// with room to batch a second — entries larger than the buffer are
    /// silently dropped on DRAM→NVMe demotion (see
    /// [`CacheConfig::flush_buffer_size`]).
    #[must_use]
    pub fn flush_buffer_size_or_default(&self) -> usize {
        if self.flush_buffer_size > 0 {
            self.flush_buffer_size
        } else {
            2 * self.block_size
        }
    }

    /// The legacy whole-object admission ceiling actually applied: the
    /// configured value, or [`DEFAULT_MAX_OBJECT_BYTES`] when unset. Mirrors
    /// [`Self::flush_buffer_size_or_default`]'s "0/None means take the
    /// built-in default" shape.
    #[must_use]
    pub fn max_object_bytes_or_default(&self) -> u64 {
        self.max_object_bytes.unwrap_or(DEFAULT_MAX_OBJECT_BYTES)
    }
}

/// io_uring engine tuning (ADR-0013). Defaults sized for NVMe queue depths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UringConfig {
    /// Worker threads (sqpoll off — a poll thread would burn a core).
    pub threads: usize,
    /// Submission-queue depth per thread.
    pub io_depth: usize,
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            threads: DEFAULT_URING_THREADS,
            io_depth: DEFAULT_URING_IO_DEPTH,
        }
    }
}

/// Disk I/O engine selection (see [`CacheConfig::io_engine`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IoEngine {
    /// Portable pread/pwrite engine (default).
    #[default]
    Psync,
    /// io_uring engine (Linux only; higher throughput at NVMe queue depths).
    Uring,
}

impl std::str::FromStr for IoEngine {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "psync" | "" => Ok(Self::Psync),
            "uring" => Ok(Self::Uring),
            other => anyhow::bail!("unknown io engine {other:?} (psync|uring)"),
        }
    }
}

/// Cache key: bucket + key. The proxy fronts exactly one backend endpoint, so
/// this is unambiguous. Also the wire format in FetchBlobRequest.cache_key.
pub fn object_key(bucket: &str, key: &str) -> String {
    format!("{bucket}/{key}")
}

/// Split a cache key back into (bucket, key). Bucket names cannot contain
/// `/`, so the first slash is the boundary. None if malformed.
pub fn object_key_parts(cache_key: &str) -> Option<(&str, &str)> {
    let (bucket, key) = cache_key.split_once('/')?;
    (!bucket.is_empty() && !key.is_empty()).then_some((bucket, key))
}

/// Create the cache directory and open the disk device for `cfg`.
async fn open_device(cfg: &CacheConfig) -> anyhow::Result<Arc<dyn Device>> {
    tokio::fs::create_dir_all(&cfg.dir).await?;
    Ok(FsDeviceBuilder::new(&cfg.dir)
        .with_capacity(cfg.disk_capacity)
        .build()?)
}

/// Build the io engine config named by `cfg.io_engine`.
///
/// # Errors
///
/// Fails when `uring` is requested on a non-Linux target.
fn io_engine_config(cfg: &CacheConfig) -> anyhow::Result<Box<dyn foyer::IoEngineConfig>> {
    Ok(match cfg.io_engine {
        IoEngine::Psync => Box::new(PsyncIoEngineConfig::new()),
        #[cfg(target_os = "linux")]
        IoEngine::Uring => Box::new(
            foyer::UringIoEngineConfig::new()
                .with_threads(cfg.uring.threads)
                .with_io_depth(cfg.uring.io_depth),
        ),
        #[cfg(not(target_os = "linux"))]
        IoEngine::Uring => anyhow::bail!("io_uring engine requires Linux"),
    })
}

/// Build the block engine config: block sizing plus any [`StorageTuning`] knob
/// the operator moved off `0` (`0` leaves foyer's own default in place).
fn block_engine_config<K, V>(
    cfg: &CacheConfig,
    device: Arc<dyn Device>,
) -> BlockEngineConfig<K, V, HybridCacheProperties>
where
    K: StorageKey,
    V: StorageValue,
{
    let mut engine = BlockEngineConfig::new(device)
        .with_block_size(cfg.block_size)
        .with_buffer_pool_size(cfg.flush_buffer_size_or_default());
    if cfg.tuning.flushers > 0 {
        engine = engine.with_flushers(cfg.tuning.flushers);
    }
    if cfg.tuning.reclaimers > 0 {
        engine = engine.with_reclaimers(cfg.tuning.reclaimers);
    }
    if cfg.tuning.submit_queue_threshold > 0 {
        engine = engine.with_submit_queue_size_threshold(cfg.tuning.submit_queue_threshold);
    }
    engine
}

/// A runtime dedicated to the disk tier, or `None` to share the caller's (see
/// [`StorageTuning::storage_runtime_threads`]).
///
/// The returned [`Spawner`] owns the runtime and shuts it down in the background
/// when the cache drops it, so nothing here has to be kept alive by the caller.
///
/// # Errors
///
/// Fails when the runtime cannot be built.
fn storage_spawner(cfg: &CacheConfig) -> anyhow::Result<Option<Spawner>> {
    if cfg.tuning.storage_runtime_threads == 0 {
        return Ok(None);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.tuning.storage_runtime_threads)
        .thread_name(STORAGE_RUNTIME_THREAD_NAME)
        .enable_all()
        .build()?;
    Ok(Some(Spawner::from(runtime)))
}

/// Build the cache without metrics (tests and tools; the daemon uses
/// [`build_cache_with_metrics`]).
///
/// # Errors
///
/// Same failure modes as [`build_cache_with_metrics`].
pub async fn build_cache(cfg: &CacheConfig) -> anyhow::Result<ObjectCache> {
    build_cache_with_metrics(cfg, Box::new(mixtrics::registry::noop::NoopMetricsRegistry)).await
}

/// Build the cache with a metrics registry (Prometheus in the daemon; the
/// plain [`build_cache`] keeps tests and tools registry-free).
///
/// # Errors
///
/// Fails when the cache directory cannot be created, the disk device cannot
/// be opened/sized, or `uring` is requested on a non-Linux target.
pub async fn build_cache_with_metrics(
    cfg: &CacheConfig,
    registry: mixtrics::metrics::BoxedRegistry,
) -> anyhow::Result<ObjectCache> {
    let device = open_device(cfg).await?;
    let mut storage = HybridCacheBuilder::new()
        .with_name(CACHE_NAME)
        .with_metrics_registry(registry)
        .memory(cfg.mem_capacity)
        // LRU per cache policy (foyer defaults to w-TinyLFU).
        .with_eviction_config(LruConfig::default())
        .with_weighter(|_k: &String, v: &CachedObject| v.body.len())
        .storage()
        .with_io_engine_config(io_engine_config(cfg)?)
        .with_engine_config(block_engine_config(cfg, device));
    if let Some(spawner) = storage_spawner(cfg)? {
        storage = storage.with_spawner(spawner);
    }
    Ok(storage.build().await?)
}

/// Build the chunk-granular cache ([`ChunkCache`]) without metrics (tests/tools).
///
/// # Errors
///
/// Same failure modes as [`build_chunk_cache_with_metrics`].
pub async fn build_chunk_cache(cfg: &CacheConfig) -> anyhow::Result<ChunkCache> {
    build_chunk_cache_with_metrics(cfg, Box::new(mixtrics::registry::noop::NoopMetricsRegistry))
        .await
}

/// Build the chunk-granular cache with a metrics registry (ADR-0015). Same
/// two-tier device/engine setup as [`build_cache_with_metrics`]; the only
/// difference is the value type ([`CacheValue`]) and its weighter (chunk bytes,
/// or a nominal weight for a header — see `CacheValue::weight`).
///
/// # Errors
///
/// Fails when the cache directory cannot be created, the disk device cannot be
/// opened/sized, or `uring` is requested on a non-Linux target.
pub async fn build_chunk_cache_with_metrics(
    cfg: &CacheConfig,
    registry: mixtrics::metrics::BoxedRegistry,
) -> anyhow::Result<ChunkCache> {
    let device = open_device(cfg).await?;
    let mut storage = HybridCacheBuilder::new()
        .with_name(CACHE_NAME)
        .with_metrics_registry(registry)
        .memory(cfg.mem_capacity)
        .with_eviction_config(LruConfig::default())
        .with_weighter(|_k: &String, v: &CacheValue| v.weight())
        .storage()
        .with_io_engine_config(io_engine_config(cfg)?)
        .with_engine_config(block_engine_config(cfg, device));
    if let Some(spawner) = storage_spawner(cfg)? {
        storage = storage.with_spawner(spawner);
    }
    Ok(storage.build().await?)
}

/// Which implementation holds chunk bodies on disk (ADR-0033).
///
/// The two are not a tuning choice, they are different code: [`Self::Foyer`] stores a
/// chunk as a foyer entry, paying a fresh page-aligned allocation, an XxHash64 over
/// every byte and a decode copy per hit — measured at **40–75 ms** for a 16 MiB entry
/// the device serves in **1.335 ms**. [`Self::Store`] is
/// [`store::ChunkStore`]: one `pread` into a registered frame.
///
/// Defaults to [`Self::Foyer`] so the change is opt-in and a regression is one flag back,
/// per ADR-0033.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DiskTier {
    /// Chunks are foyer entries (the behaviour before ADR-0033).
    #[default]
    Foyer,
    /// Chunks live in the ADR-0033 chunk store; foyer keeps the object headers.
    Store,
}

impl std::str::FromStr for DiskTier {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "foyer" | "" => Ok(Self::Foyer),
            "store" => Ok(Self::Store),
            other => anyhow::bail!("unknown disk tier {other:?} (foyer|store)"),
        }
    }
}

/// Whether a chunk read that misses the RAM tier promotes the disk hit into it.
///
/// foyer's `HybridCache::get` always promotes: it is `get_or_fetch` underneath,
/// and the value the disk tier returns is inserted into the in-memory tier at the
/// **hot end** of the LRU (foyer carries a `Source::Disk` marker on the entry but
/// neither the shard nor the LRU ever reads it). On a one-pass checkpoint sweep
/// that promotion is close to pure waste — the C4 arm measured a 5.3 % RAM-tier
/// hit rate — and it is not free: each promotion evicts its own weight of other
/// entries, and an evicted entry that is still `Age::Fresh` (filled from the
/// backend, not yet written down) costs a real DRAM→NVMe write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Promotion {
    /// foyer's own behaviour: insert a disk hit into the RAM tier (default).
    #[default]
    OnDiskHit,
    /// Serve a disk hit without inserting it into the RAM tier.
    Never,
}

impl std::str::FromStr for Promotion {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "on-disk-hit" | "" => Ok(Self::OnDiskHit),
            "never" => Ok(Self::Never),
            other => anyhow::bail!("unknown promotion policy {other:?} (on-disk-hit|never)"),
        }
    }
}

/// Read one entry out of the chunk cache, honouring `promotion`.
///
/// [`Promotion::OnDiskHit`] is exactly `cache.get(key)`. [`Promotion::Never`]
/// splits that into its two tiers by hand — the RAM tier synchronously, then the
/// disk tier through `storage().load()` — and drops the value on the floor
/// instead of inserting it.
///
/// ⚠ **`Never` gives up foyer's single-flight coalescing.** `get_or_fetch`
/// collapses concurrent misses of one key into one load; two concurrent readers
/// of the same chunk key will each do their own disk read here. That is a
/// deliberate trade for the checkpoint shape, where a node reads each chunk once
/// per pass — it is *not* safe to assume for a fan-in of many readers over a hot
/// key, which is what the daemon's own per-key fill guard covers for backend
/// fills but not for disk reads.
///
/// A throttled load is reported as a miss, which is what `HybridCache::get` does
/// with it too, so the caller's fallback order is unchanged.
///
/// # Errors
///
/// Propagates a disk-tier read failure (a checksum or magic mismatch is not one —
/// foyer drops that entry from its index and reports a miss).
pub async fn read_chunk_entry(
    cache: &ChunkCache,
    key: &str,
    promotion: Promotion,
) -> anyhow::Result<Option<CacheValue>> {
    if promotion == Promotion::OnDiskHit {
        return Ok(cache.get(key).await?.map(|e| e.value().clone()));
    }
    if let Some(entry) = cache.memory().get(key) {
        return Ok(Some(entry.value().clone()));
    }
    match cache.storage().load(key).await? {
        Load::Entry { value, .. } => Ok(Some(value)),
        Load::Piece { piece, .. } => Ok(Some(piece.value().clone())),
        Load::Miss | Load::Throttled => Ok(None),
    }
}

/// Which read path a GET takes, per the cache policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadDecision {
    /// Serve from cache if present; on miss, fill while streaming to the client.
    CacheAndFill,
    /// Serve from cache if present; on miss, proxy WITHOUT populating
    /// (`Cache-Control: no-store` on the request).
    CacheNoFill,
    /// Proxy straight through, never touching the cache
    /// (`Cache-Control: no-cache`, part-number reads, …).
    Bypass,
}

/// Decide the read path from the request's Cache-Control header and shape.
///
/// Size-based admission can't happen here — the object size is only known once
/// the backend responds; [`should_admit`] gates the fill at that point.
pub fn read_decision(cache_control: Option<&str>, part_number: Option<i32>) -> ReadDecision {
    // GetObject with partNumber returns one part of a multipart object, not
    // the whole body — slicing a cached whole object can't reproduce part
    // boundaries, so those reads always bypass.
    if part_number.is_some() {
        return ReadDecision::Bypass;
    }
    match cache_control {
        Some(cc) => {
            let cc = cc.to_ascii_lowercase();
            if cc.contains("no-cache") {
                ReadDecision::Bypass
            } else if cc.contains("no-store") {
                ReadDecision::CacheNoFill
            } else {
                ReadDecision::CacheAndFill
            }
        }
        None => ReadDecision::CacheAndFill,
    }
}

/// Size-based admission: an object is chunk-cached when it is strictly larger
/// than `min` and — only if a `max` cap is set — at most `max`.
///
/// `max` is `None` by default (no upper bound). Chunk-granular caching
/// (ADR-0015) streams and stores an object of any size as `chunk_size` pieces
/// distributed across the ring, with per-read memory bounded to
/// `fill_parallelism × chunk_size` regardless of object length — so there is no
/// memory or disk reason to cap object size, and a multi-GiB (or larger)
/// checkpoint shard is cached rather than bypassed. A `Some` value is an
/// optional operator safety valve to proxy pathologically large objects
/// through uncached, not a design limit. `None` content length (unknown, e.g.
/// no `Content-Length`) is never admitted.
pub fn should_admit(content_length: Option<u64>, min: u64, max: Option<u64>) -> bool {
    matches!(content_length, Some(len) if len > min && max.is_none_or(|m| len <= m))
}

/// Multiple of [`chunk::DEFAULT_CHUNK_SIZE`] that sizes [`DEFAULT_MAX_OBJECT_BYTES`].
///
/// Not a reference to `pacer-daemon`'s own `DEFAULT_FILL_PARALLELISM` (this crate
/// cannot depend on that one — the dependency runs the other way) but the same
/// value, 8, chosen for the same reason: the chunked read path's typical worst
/// case holds `fill_parallelism × chunk_size` resident at once (one GET's
/// look-ahead window, see [`should_admit`]'s own doc), so multiplying the
/// chunked path's default chunk size by this keeps a single legacy whole-object
/// admission's RAM cost in that same order of magnitude instead of picking an
/// unrelated number.
const DEFAULT_MAX_OBJECT_CHUNK_MULTIPLE: u64 = 8;

/// Default ceiling, in bytes, on a whole object admitted through the LEGACY
/// (pre-chunking, ADR-0002) [`ObjectCache`] — applied by
/// [`should_admit_legacy_object`] whenever [`CacheConfig::max_object_bytes`] is
/// unset ([`CacheConfig::max_object_bytes_or_default`]).
///
/// [`should_admit`] itself stays uncapped by default (`max: None` = unbounded)
/// because that is correct for the CHUNKED path: `fill_parallelism ×
/// chunk_size` already bounds a chunked read's resident memory regardless of
/// object length, so nothing about admitting a multi-GiB object is unsafe
/// there. The legacy whole-object path has no equivalent bound — one admitted
/// object is one [`CachedObject`] holding the entire body in the RAM tier, so
/// its memory cost IS the object's size, with nothing dividing it — which is
/// exactly the gap this constant closes: an unbounded default there really did
/// mean "a multi-GiB object can be admitted whole" (ADR-0002 predates chunking
/// and never set one).
///
/// `8 × 16 MiB = 128 MiB`: see `DEFAULT_MAX_OBJECT_CHUNK_MULTIPLE` (private —
/// its own doc, just above) for why 8.
pub const DEFAULT_MAX_OBJECT_BYTES: u64 =
    DEFAULT_MAX_OBJECT_CHUNK_MULTIPLE * chunk::DEFAULT_CHUNK_SIZE;

/// Why [`should_admit_legacy_object`] refused to admit a whole object.
///
/// Kept distinct from a bare `bool` so a caller with a metrics facility (the
/// daemon, not this crate — see [`should_admit_legacy_object`]'s doc) can
/// count *why* rather than merely *that*, and so a caller with none can still
/// log something more useful than "not admitted".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionRejection {
    /// No `Content-Length` was known at all. [`should_admit`] never admits
    /// this case either — an object of unknown size cannot be weighed against
    /// any bound.
    UnknownLength,
    /// At or below `min` — too small to be worth a cache entry.
    TooSmall,
    /// Above [`CacheConfig::max_object_bytes_or_default`] — the bound
    /// [`DEFAULT_MAX_OBJECT_BYTES`] exists to enforce.
    TooLarge,
}

/// Whole-object admission for the LEGACY (pre-chunking, ADR-0002)
/// [`ObjectCache`]: [`should_admit`] with `cfg`'s ceiling
/// ([`CacheConfig::max_object_bytes_or_default`]) substituted for an explicit
/// `max`, so a caller of the legacy path gets [`DEFAULT_MAX_OBJECT_BYTES`]
/// (or an operator override) without having to remember to pass one.
///
/// Deliberately built ON TOP of [`should_admit`] rather than duplicating its
/// comparison, so the two can never silently disagree about what "admitted"
/// means — only about which `max` applies.
///
/// # Errors
///
/// The specific [`AdmissionRejection`] when the object should not be cached.
/// This crate has no Prometheus registry (it lives in `pacer-daemon`), so the
/// reason is returned rather than counted here — a caller that wants
/// `pacer_cache_admission_rejected_total{reason="too_large"}` increments it
/// from the `Err` arm.
pub fn should_admit_legacy_object(
    content_length: Option<u64>,
    min: u64,
    cfg: &CacheConfig,
) -> Result<(), AdmissionRejection> {
    let max = cfg.max_object_bytes_or_default();
    if should_admit(content_length, min, Some(max)) {
        return Ok(());
    }
    Err(match content_length {
        None => AdmissionRejection::UnknownLength,
        Some(len) if len <= min => AdmissionRejection::TooSmall,
        Some(_) => AdmissionRejection::TooLarge,
    })
}

/// Resolve an HTTP byte range against an object of `len` bytes, mirroring S3
/// semantics: `first-last` (inclusive, `last` clamped), `first-` (to end),
/// `-suffix` (last N bytes). Returns `None` when the range is unsatisfiable
/// (416 InvalidRange).
pub fn resolve_range(
    first: Option<u64>,
    last: Option<u64>,
    suffix: Option<u64>,
    len: u64,
) -> Option<Range<u64>> {
    match (first, suffix) {
        (Some(first), None) => {
            if first >= len {
                return None;
            }
            let end = match last {
                Some(last) if last < first => return None,
                // `saturating_add`, not `+`: a client's inclusive upper bound can
                // legally be `u64::MAX` (e.g. `bytes=0-18446744073709551615`, a
                // verbose way of saying "to the end"), and `last + 1` overflows
                // for exactly that value — a panic under debug assertions (this
                // is exercised by `proptests::resolve_range_never_panics` in
                // this module) and, in a release build, a silent wraparound to
                // `0` that turned "give me everything" into an empty range.
                Some(last) => last.saturating_add(1).min(len),
                None => len,
            };
            Some(first..end)
        }
        (None, Some(suffix)) => {
            if suffix == 0 {
                return None;
            }
            Some(len.saturating_sub(suffix)..len)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_value_weight_and_accessors() {
        let header = CacheValue::Header(chunk::ObjectHeader {
            object_len: 1 << 30,
            e_tag: Some("etag".into()),
            content_type: None,
            last_modified_epoch_secs: None,
        });
        let body = Bytes::from(vec![0u8; 4096]);
        let chunk = CacheValue::Chunk(chunk::CachedChunk::new(body));
        // A chunk is charged its bytes; a header a small fixed weight.
        assert_eq!(chunk.weight(), 4096);
        assert_eq!(header.weight(), HEADER_WEIGHT);
        // Accessors are exclusive: each kind reads out one way, None the other.
        assert!(header.as_header().is_some() && header.as_chunk().is_none());
        assert!(chunk.as_chunk().is_some() && chunk.as_header().is_none());
    }

    /// A chunk-sized entry demoted out of a small RAM tier must be readable
    /// back from the disk tier. Regression test for the silent flush-buffer
    /// drop found by the rung-1 NVMe saturation gate (planning/15 ladder):
    /// foyer's io buffer defaults to 16 MiB, `Buffer::push` returns `false`
    /// for any entry that doesn't fit, and the entry vanishes on demotion with
    /// no error — 32 GiB of seeded chunks, zero bytes on disk. The auto-sized
    /// flush buffer (`2 × block_size`) makes every cacheable entry fit.
    #[tokio::test(flavor = "multi_thread")]
    async fn demoted_chunk_survives_to_disk_tier() {
        /// Larger than foyer's 16 MiB built-in flush buffer, so this test
        /// fails (entry silently dropped) without the auto-sizing.
        const CHUNK_BYTES: usize = 24 << 20;
        let dir = tempfile::tempdir().unwrap();
        let cache = build_chunk_cache(&CacheConfig {
            dir: dir.path().to_path_buf(),
            // RAM tier far below one chunk: the insert demotes immediately.
            mem_capacity: 1 << 20,
            disk_capacity: 256 << 20,
            block_size: 64 << 20,
            flush_buffer_size: 0,
            io_engine: IoEngine::Psync,
            uring: UringConfig::default(),
            tuning: StorageTuning::default(),
            max_object_bytes: None,
        })
        .await
        .unwrap();

        let key = object_key("bucket", "big-chunk");
        let body = Bytes::from(vec![7u8; CHUNK_BYTES]);
        cache.insert(
            key.clone(),
            CacheValue::Chunk(chunk::CachedChunk::new(body)),
        );
        // Push the write pipeline to disk deterministically.
        cache.close().await.unwrap();

        let entry = cache
            .get(&key)
            .await
            .unwrap()
            .expect("chunk must survive DRAM→NVMe demotion (silently dropped?)");
        let chunk = entry.value().as_chunk().unwrap();
        assert_eq!(chunk.body.len(), CHUNK_BYTES);
        assert!(chunk.body.iter().all(|&b| b == 7));
    }

    /// [`Promotion::Never`] must serve a disk-resident chunk without inserting it
    /// into the RAM tier, and [`Promotion::OnDiskHit`] must still insert it —
    /// the pair is the whole point of the knob, and each half is meaningless
    /// without the other (a `Never` that also failed to *read* would pass a
    /// no-promotion assertion trivially).
    #[tokio::test(flavor = "multi_thread")]
    async fn never_promotion_serves_from_disk_without_filling_the_ram_tier() {
        /// Small enough that the RAM tier below could hold many of these, so an
        /// absent promotion is this test's doing and not a capacity eviction.
        const CHUNK_BYTES: usize = 1 << 20;
        let dir = tempfile::tempdir().unwrap();
        let cache = build_chunk_cache(&CacheConfig {
            dir: dir.path().to_path_buf(),
            mem_capacity: 64 << 20,
            disk_capacity: 256 << 20,
            block_size: 64 << 20,
            flush_buffer_size: 0,
            io_engine: IoEngine::Psync,
            uring: UringConfig::default(),
            tuning: StorageTuning::default(),
            max_object_bytes: None,
        })
        .await
        .unwrap();

        let key = object_key("bucket", "disk-only-chunk");
        let body = Bytes::from(vec![3u8; CHUNK_BYTES]);
        // `storage_writer` marks the entry phantom, so it never occupies the RAM
        // tier and is piped to disk when the entry drops — which is how this test
        // gets a disk-resident chunk without evicting anything.
        let entry = cache
            .storage_writer(key.clone())
            .insert(CacheValue::Chunk(chunk::CachedChunk::new(body)))
            .expect("disk-only insert must be admitted");
        drop(entry);
        cache.close().await.unwrap();
        assert!(
            cache.memory().get(&key).is_none(),
            "a disk-only insert must leave the RAM tier empty"
        );

        let value = read_chunk_entry(&cache, &key, Promotion::Never)
            .await
            .unwrap()
            .expect("the chunk is on disk, so Never must still find it");
        assert_eq!(value.as_chunk().unwrap().body.len(), CHUNK_BYTES);
        assert!(
            cache.memory().get(&key).is_none(),
            "Promotion::Never must not populate the RAM tier"
        );

        let value = read_chunk_entry(&cache, &key, Promotion::OnDiskHit)
            .await
            .unwrap()
            .expect("the chunk is on disk, so OnDiskHit must find it too");
        assert_eq!(value.as_chunk().unwrap().body.len(), CHUNK_BYTES);
        assert!(
            cache.memory().get(&key).is_some(),
            "OnDiskHit promotes — that is exactly what Never opts out of"
        );
    }

    #[test]
    fn disk_tier_defaults_to_foyer_and_rejects_unknown() {
        use std::str::FromStr;
        // Empty and absent must both mean "what the daemon did before ADR-0033": the
        // new tier is opt-in, so a config that does not mention it cannot select it.
        assert_eq!(DiskTier::default(), DiskTier::Foyer);
        assert_eq!(DiskTier::from_str("").unwrap(), DiskTier::Foyer);
        assert_eq!(DiskTier::from_str("foyer").unwrap(), DiskTier::Foyer);
        assert_eq!(DiskTier::from_str("store").unwrap(), DiskTier::Store);
        assert!(DiskTier::from_str("chunkstore").is_err());
    }

    #[test]
    fn promotion_parses_its_two_policies() {
        use std::str::FromStr;
        assert_eq!(Promotion::from_str("").unwrap(), Promotion::OnDiskHit);
        assert_eq!(
            Promotion::from_str("on-disk-hit").unwrap(),
            Promotion::OnDiskHit
        );
        assert_eq!(Promotion::from_str("never").unwrap(), Promotion::Never);
        assert!(Promotion::from_str("sometimes").is_err());
    }

    #[test]
    fn header_and_chunk_keys_never_collide() {
        // The header lives under the plain object key; chunk 0 under the chunk
        // key. They must differ, or a header would clobber a chunk in one cache.
        let obj = object_key("bucket", "path/to/obj");
        let chunk0 = chunk::ChunkConfig::default().chunk_key(&obj, 0);
        assert_ne!(obj, chunk0);
        assert!(!obj.contains('#') && chunk0.contains('#'));
    }

    #[test]
    fn no_cache_bypasses() {
        assert_eq!(read_decision(Some("no-cache"), None), ReadDecision::Bypass);
        assert_eq!(
            read_decision(Some("No-Cache, max-age=0"), None),
            ReadDecision::Bypass
        );
    }

    #[test]
    fn no_store_reads_but_never_fills() {
        assert_eq!(
            read_decision(Some("no-store"), None),
            ReadDecision::CacheNoFill
        );
    }

    #[test]
    fn default_is_cache_and_fill() {
        assert_eq!(read_decision(None, None), ReadDecision::CacheAndFill);
        assert_eq!(
            read_decision(Some("max-age=3600"), None),
            ReadDecision::CacheAndFill
        );
    }

    #[test]
    fn part_number_reads_bypass() {
        assert_eq!(read_decision(None, Some(3)), ReadDecision::Bypass);
    }

    #[test]
    fn admission_is_strictly_greater_than_min() {
        let min = 4 << 20;
        let max = 8u64 << 30;
        assert!(!should_admit(Some(min), min, Some(max)));
        assert!(should_admit(Some(min + 1), min, Some(max)));
        assert!(should_admit(Some(max), min, Some(max)));
        assert!(!should_admit(Some(max + 1), min, Some(max)));
        assert!(!should_admit(None, min, Some(max)));
        // No cap (the default): every object above `min` is admitted, however
        // large — chunking is not bounded by whole-object size.
        assert!(should_admit(Some(max + 1), min, None));
        assert!(should_admit(Some(1 << 50), min, None)); // 1 PiB
        assert!(!should_admit(Some(min), min, None));
        assert!(!should_admit(None, min, None));
    }

    /// A [`CacheConfig`] whose non-admission fields are irrelevant to these
    /// tests (no cache is ever built from it) — only `max_object_bytes`
    /// varies per call.
    fn admission_only_config(max_object_bytes: Option<u64>) -> CacheConfig {
        CacheConfig {
            dir: PathBuf::from("/dev/null"),
            mem_capacity: 0,
            disk_capacity: 0,
            block_size: 0,
            flush_buffer_size: 0,
            io_engine: IoEngine::Psync,
            uring: UringConfig::default(),
            tuning: StorageTuning::default(),
            max_object_bytes,
        }
    }

    #[test]
    fn max_object_bytes_or_default_falls_back_when_unset() {
        assert_eq!(
            admission_only_config(None).max_object_bytes_or_default(),
            DEFAULT_MAX_OBJECT_BYTES
        );
        assert_eq!(
            admission_only_config(Some(4 << 20)).max_object_bytes_or_default(),
            4 << 20
        );
    }

    #[test]
    fn legacy_admission_default_bound_admits_at_and_below_rejects_above() {
        let cfg = admission_only_config(None);
        let min = 0;
        let bound = DEFAULT_MAX_OBJECT_BYTES;

        // Below the bound: admitted.
        assert_eq!(
            should_admit_legacy_object(Some(bound - 1), min, &cfg),
            Ok(())
        );
        // At the bound: admitted (should_admit's own comparison is `<=`).
        assert_eq!(should_admit_legacy_object(Some(bound), min, &cfg), Ok(()));
        // Above the bound: rejected, and the reason says why.
        assert_eq!(
            should_admit_legacy_object(Some(bound + 1), min, &cfg),
            Err(AdmissionRejection::TooLarge)
        );
    }

    #[test]
    fn legacy_admission_honours_an_explicit_override_over_the_default() {
        // Well under DEFAULT_MAX_OBJECT_BYTES, so this test would fail if the
        // override were silently ignored in favour of the built-in default.
        let explicit = 4u64 << 20;
        assert!(explicit < DEFAULT_MAX_OBJECT_BYTES);
        let cfg = admission_only_config(Some(explicit));
        let min = 0;

        assert_eq!(
            should_admit_legacy_object(Some(explicit), min, &cfg),
            Ok(())
        );
        assert_eq!(
            should_admit_legacy_object(Some(explicit + 1), min, &cfg),
            Err(AdmissionRejection::TooLarge)
        );
        // A size the DEFAULT would have admitted is still rejected once an
        // operator has capped this config lower.
        assert_eq!(
            should_admit_legacy_object(Some(DEFAULT_MAX_OBJECT_BYTES), min, &cfg),
            Err(AdmissionRejection::TooLarge)
        );
    }

    #[test]
    fn legacy_admission_rejects_too_small_and_unknown_length() {
        let cfg = admission_only_config(None);
        let min = 4 << 20;
        assert_eq!(
            should_admit_legacy_object(Some(min), min, &cfg),
            Err(AdmissionRejection::TooSmall)
        );
        assert_eq!(
            should_admit_legacy_object(None, min, &cfg),
            Err(AdmissionRejection::UnknownLength)
        );
    }

    #[test]
    fn range_resolution_matches_s3() {
        // bytes=0-499
        assert_eq!(resolve_range(Some(0), Some(499), None, 1000), Some(0..500));
        // bytes=500- (to end)
        assert_eq!(resolve_range(Some(500), None, None, 1000), Some(500..1000));
        // bytes=-200 (suffix)
        assert_eq!(resolve_range(None, None, Some(200), 1000), Some(800..1000));
        // suffix longer than the object → whole object
        assert_eq!(resolve_range(None, None, Some(5000), 1000), Some(0..1000));
        // last clamped to len-1
        assert_eq!(
            resolve_range(Some(900), Some(5000), None, 1000),
            Some(900..1000)
        );
        // unsatisfiable
        assert_eq!(resolve_range(Some(1000), None, None, 1000), None);
        assert_eq!(resolve_range(None, None, Some(0), 1000), None);
        assert_eq!(resolve_range(Some(10), Some(5), None, 1000), None);
    }
}

/// Property tests for [`resolve_range`] (T5, the HTTP `Range` header's semantics).
///
/// `resolve_input_range` in `pacer-daemon`'s `proxy.rs` is a thin adapter that
/// destructures s3s's already-parsed `HttpRange` into this function's
/// `(first, last, suffix)` triple — the actual "first-last", "first-", and
/// "-suffix" arithmetic lives entirely here, so property-testing this one `pub`
/// function covers that adapter's behaviour without needing a copy of it (or a
/// dependency on s3s) in this crate.
#[cfg(test)]
mod proptests {
    use super::resolve_range;
    use proptest::prelude::*;

    /// Default proptest case count for this module. Proptest's own built-in
    /// default is already 256 and it is deliberately left in force below (no
    /// `ProptestConfig::with_cases` override) so the `PROPTEST_CASES` env var
    /// still works — this const exists to document the number a run uses when
    /// that var is unset, not to hardcode it.
    #[allow(dead_code)]
    const PROPTEST_CASES: u32 = 256;

    /// Upper bound for the object lengths and offsets generated below. Large
    /// enough to exercise multi-GiB objects (the checkpoint-shard shape this
    /// code serves) while staying far under `u64::MAX`, where the *inputs*
    /// this module intentionally probes (`u64::MAX`-adjacent `last`/`suffix`
    /// values, tested separately in `resolve_range_never_panics`) live.
    const MAX_LEN: u64 = 1 << 40; // 1 TiB

    proptest! {
        /// A satisfiable `first-last` (inclusive) range round-trips: resolving
        /// the exact `(first, end-1)` pair that produced `[start, end)` must
        /// reproduce that same half-open range.
        #[test]
        fn first_last_round_trips(len in 0..=MAX_LEN, start in 0..=MAX_LEN, extra in 0..=MAX_LEN) {
            prop_assume!(start < len);
            let end = (start + 1 + extra).min(len);
            prop_assert_eq!(resolve_range(Some(start), Some(end - 1), None, len), Some(start..end));
        }

        /// An open-ended `first-` range round-trips to `[first, len)`.
        #[test]
        fn open_ended_round_trips(len in 0..=MAX_LEN, first in 0..=MAX_LEN) {
            prop_assume!(first < len);
            prop_assert_eq!(resolve_range(Some(first), None, None, len), Some(first..len));
        }

        /// A `-suffix` range round-trips to the last `suffix` bytes, and — since
        /// a suffix larger than the object means "the whole object" — to
        /// `[0, len)` when `suffix >= len`.
        #[test]
        fn suffix_round_trips(len in 0..=MAX_LEN, suffix in 1..=MAX_LEN) {
            let expected = len.saturating_sub(suffix)..len;
            prop_assert_eq!(resolve_range(None, None, Some(suffix), len), Some(expected));
        }

        /// Whatever comes back is never longer than the object it was resolved
        /// against — a resolved range is a promise about real bytes, and a range
        /// past `len` would be a promise this object cannot keep.
        #[test]
        fn resolved_range_never_exceeds_object_len(
            len in 0..=MAX_LEN,
            first in prop::option::of(0..=MAX_LEN),
            last in prop::option::of(0..=MAX_LEN),
            suffix in prop::option::of(0..=MAX_LEN),
        ) {
            if let Some(range) = resolve_range(first, last, suffix, len) {
                prop_assert!(range.start <= range.end);
                prop_assert!(range.end <= len);
            }
        }

        /// No combination of inputs panics — including the two `u64::MAX`
        /// edges a real `Range` header can carry (`bytes=0-18446744073709551615`
        /// is a legal, if verbose, "to the end"; a suffix of `u64::MAX` is the
        /// same for `-18446744073709551615`) and every unsatisfiable shape
        /// (`first >= len`, `last < first`, `suffix == 0`, both or neither of
        /// `first`/`suffix` present).
        #[test]
        fn resolve_range_never_panics(
            len in any::<u64>(),
            first in prop::option::of(any::<u64>()),
            last in prop::option::of(any::<u64>()),
            suffix in prop::option::of(any::<u64>()),
        ) {
            let _ = resolve_range(first, last, suffix, len);
        }
    }
}
