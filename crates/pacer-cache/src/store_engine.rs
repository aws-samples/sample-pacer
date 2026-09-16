//! ADR-0033's slot store, plugged into foyer as its **disk-cache engine**.
//!
//! Not selected by any config yet, and unmeasured on hardware: nothing constructs
//! [`ChunkEngineConfig`] outside this crate's tests, so the module is inert until a
//! `diskTier` value names it.
//!
//! # What this is for
//!
//! `diskTier=store` (see [`crate::store`]) replaced foyer's whole hybrid cache for chunk
//! bodies, which bought **1.711×** on a 70B load
//! (`bench/ladder/results/c5-dcp-chunk-store.md`) but threw out three things worth having:
//! the RAM tier and its LRU, `get_or_fetch`'s single-flight coalescing, and — the one that
//! bit operationally — foyer's capacity arithmetic, where `memCapacity` counted toward
//! chunk capacity so a 131 GiB checkpoint fitted `48GiB + 100GiB`. Losing that is why any
//! `store` deployment must now size `diskCapacity` to the whole working set alone.
//!
//! foyer has a seat for exactly this. `StoreBuilder::with_engine_config` takes a
//! `Box<dyn EngineConfig>`, and [`foyer_storage::Engine`] is a public, documented trait —
//! the same seat `BlockEngineConfig` occupies. Filling it keeps everything above the disk
//! tier and replaces only the tier itself:
//!
//! | kept by foyer | replaced by us |
//! |---|---|
//! | RAM tier, LRU, weighter, capacity | the on-disk format |
//! | `get_or_fetch` single-flight coalescing | the index |
//! | promotion, the eviction pipe, metrics | the read: one `pread` into a slab frame |
//!
//! # Why this is where the read win comes from
//!
//! The **checksum is the BlockEngine's, not the `Store`'s**: `checksum: u64` lives in
//! `foyer-storage/src/engine/block/serde.rs`'s entry header, and the crate-level
//! deserializer only verifies when an engine passes `Some(..)`. An engine that owns its
//! format simply does not compute one. That is the 11.7 ms per entry of XxHash64 plus
//! decode copy measured on hardware — gone by construction rather than by a flag.
//!
//! And [`foyer_storage::Engine::load`] takes a **hash**, returns the key, and documents
//! that *the caller* must check the returned key matches. So storing the key beside the
//! bytes and having the caller compare is already foyer's own contract — which is exactly
//! what [`crate::slot::SlotHeader`] was built to do.
//!
//! # The two things that used to be unfinished, and what closed them
//!
//! 1. ~~It needs a one-line patch to foyer.~~ **Resolved upstream.** `Engine::enqueue` names
//!    `PieceRef<K, V, P>`, which used to live in foyer-storage's **private** `mod keeper` and
//!    was re-exported nowhere — so a public trait could not be implemented downstream at all.
//!    Exporting it is [foyer#1330](https://github.com/foyer-rs/foyer/pull/1330), merged
//!    2026-09-09 and released in **0.22.6**, in both
//!    `foyer-storage`'s prelude and `foyer`'s. The vendored `[patch.crates-io]` this module
//!    was parked behind is gone; it builds against a released crate.
//!
//!    One symbol short of complete, and it is the same gap: `Populated`, which
//!    [`Engine::load`] must return, is in `foyer-storage`'s prelude but not `foyer`'s, and
//!    `foyer`'s `lib.rs` is `pub use prelude::*` alone — no public `foyer::storage` path — so
//!    there is no way to reach it through the umbrella crate. That is the one reason this
//!    crate still names `foyer-storage` as a direct dependency, and a follow-up one-liner
//!    upstream would delete it.
//!
//! 2. ~~Object headers are not persisted.~~ **Now routed, not dropped.** One keyspace holds
//!    headers and chunk bodies (see [`crate::CacheValue`]), and a fixed 16 MiB slot is the
//!    wrong home for a few-hundred-byte header — so an earlier cut of this engine dropped
//!    non-chunks, and a header evicted from the RAM tier cost a `HEAD` refetch.
//!
//!    **This is a composite engine, and that is deliberately not "a home for headers in the
//!    slot store".** Variable-size entries in a fixed-slot layout would mean an allocator,
//!    which is the complexity ADR-0033 exists to avoid. Instead [`ChunkEngine`] holds an
//!    inner `Arc<dyn Engine>` — in practice foyer's own `BlockEngine`, which is good at
//!    exactly the small entries the slots are bad at — and routes **by value type**:
//!
//!    * [`ChunkEngine::enqueue`] sends a chunk to a slot and delegates everything else.
//!    * [`ChunkEngine::load`] tries the slots and falls back to the inner engine, so a hit
//!      costs one lookup and a miss two. A chunk is never in the block engine and a header
//!      is never in a slot, so the fallback cannot serve the wrong thing — it only makes the
//!      pair total.
//!    * `delete`, `may_contains`, `destroy`, `wait` and `close` cover **both** halves, since
//!      either may hold the hash and neither knows which.
//!
//!    The inner engine's config is supplied by the caller ([`ChunkEngineConfig::new`])
//!    rather than built here, so headers get exactly the tuning the shipped foyer tier
//!    would have given them — `flushers`, `reclaimers`, `submitQueueThreshold` and block
//!    size all keep whatever `crate::CacheConfig` says — and this module needs to know
//!    nothing about how that is spelled.
//!
//! # Two halves, one device, and why they cannot collide
//!
//! The inner block engine owns the [`Device`] foyer opened, and this engine reports that
//! same device as its own. They do not overlap: the slot store never touches foyer's device
//! at all — it opens its own directory (`StoreConfig::dir`) and issues its own `pread`s —
//! so `Device` here is a handle the composite reports and the inner half actually uses.
//!
//! The inner engine is built from a second [`EngineBuildContext`] cloned field by field
//! from ours, which is possible because all four are cheap to copy (`Arc<dyn IoEngine>`,
//! `Arc<Metrics>`, `Spawner: Clone`, `RecoverMode: Copy`). Both halves therefore share one
//! io engine and one metrics registry, which is what makes foyer's own `foyer_storage_*`
//! series keep meaning what they did.
//!
//! # Restoring the RAM tier re-arms a slab term that `diskTier=store` left slack
//!
//! A chunk this engine loads holds a **registered hugepage frame**: `ChunkStore`'s read
//! claims one ([`crate::frames::claim`]) and seals it into the chunk's `Bytes`, which is
//! released only when the last reference drops. So once foyer promotes that chunk, the frame
//! stays claimed for as long as the RAM tier keeps it — and the frame pool is bounded.
//!
//! **That is already paid for, and this is the reason to check rather than assume.** The
//! chart derives the slab as `memCapacity + 256 × chunkSize`
//! (`pacer.cacheSlabBytes` in `deploy/helm/pacer/templates/_helpers.tpl`), so it covers a
//! *fully resident* RAM tier by construction, plus a 256-chunk working margin — and
//! `pacer.validateHugepages` fails the render when the node's reservation cannot cover
//! arena + slab + holder. The `memCapacity` term was put there for foyer's own promotion
//! path (`install_promotion_frames`, which exists so a promoted chunk lands in a frame
//! rather than the heap). Under `diskTier=store` that term is slack, because ADR-0033 has no
//! chunk RAM tier at all; the composite makes it load-bearing again, which is what it was
//! sized for. No new arithmetic is owed. What is worth watching on the first arm is
//! `pacer_cache_slab_heap_fallbacks_total`: a frame pool exhausted by residency falls back
//! to a heap `Vec`, whose alignment cannot use the direct descriptor, so the read silently
//! becomes **buffered** rather than failing.
//!
//! ⚠ **Two capacities, and they no longer mean what they did on either earlier path.**
//! `CacheConfig::disk_capacity` sizes foyer's device, which under the composite holds
//! **headers only**; `StoreConfig::capacity_bytes` sizes the slot store's own directory,
//! which holds **chunks**. So a deployment must set both, and setting `diskCapacity` to the
//! whole checkpoint — correct for `diskTier=store`, where it sized nothing at all here —
//! now over-reserves the header tier by orders of magnitude. Under-setting it is worse than
//! wasteful: a device with room for **zero blocks does not fail to build**, because the
//! block engine's reclaimer waits for a clean block that can never exist. That is a hang
//! with no log line, so [`crate::build_chunk_cache_with_engine`] refuses it up front.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use foyer::{
    Age, Device, Engine, EngineBuildContext, EngineConfig, Load, PieceRef, Result as FoyerResult,
    Spawner, StorageFilterResult,
};
// The one symbol the umbrella crate does not re-export — see `Cargo.toml` for why, and why
// this line is expected to go away.
use foyer_storage::Populated;
use futures_core::future::BoxFuture;

use crate::store::ChunkStore;
use crate::{CacheValue, HybridCacheProperties};

/// The key type this engine is specialised for — `pacer`'s chunk and object keys.
///
/// Concrete rather than generic on purpose. A generic engine would have to round-trip the
/// value through `V::encode`/`V::decode`, which is the copy this design exists to remove;
/// knowing the value type is what lets [`ChunkEngine::load`] `pread` straight into a slab
/// frame and hand back a [`CacheValue`] with nothing in between.
type Key = String;

/// Counters the daemon can publish, mirroring [`crate::store::StoreStats`]'s role.
#[derive(Debug, Default)]
pub struct EngineStats {
    /// Pieces foyer handed us that were not chunk bodies — object headers — and which we
    /// therefore passed to the inner block engine instead of spending a slot on.
    ///
    /// Not an error count. It is the one number that says the composite is routing rather
    /// than quietly dropping, which is what this engine used to do.
    pub headers_delegated: AtomicU64,
    /// `load` calls whose hash was in our index but whose slot no longer held the chunk,
    /// because the store's own LRU had reused it. Reported as a miss, and the stale hash
    /// is pruned.
    pub stale_hashes_pruned: AtomicU64,
    /// Writes that failed. foyer's `enqueue` is fire-and-forget, so a failure here can
    /// only be counted, never returned.
    pub enqueue_errors: AtomicU64,
}

/// Config for [`ChunkEngine`], handed to `StoreBuilder::with_engine_config`.
///
/// Carries the [`Device`] because [`EngineBuildContext`] does not: it supplies the io
/// engine, metrics, spawner and recover mode, and leaves the device to the config — which
/// is how `BlockEngineConfig` gets its own.
///
/// Takes an **already-open** [`ChunkStore`] rather than a [`crate::store::StoreConfig`], so the caller
/// keeps a handle to it. That is not a convenience: the daemon publishes the store's
/// counters (`pacer_chunk_store_*`), and a store opened inside `build` would be sealed
/// behind `Arc<dyn Engine>` with no way to reach it.
#[derive(Debug)]
pub struct ChunkEngineConfig {
    device: Arc<dyn Device>,
    store: ChunkStore,
    headers: Box<dyn EngineConfig<Key, CacheValue, HybridCacheProperties>>,
}

impl ChunkEngineConfig {
    /// An engine over `store`, reporting `device` as the device it sits on, with `headers`
    /// as the inner engine every value that is not a chunk body is routed to.
    ///
    /// `headers` is a config and not a built engine because [`EngineConfig::build`] needs
    /// the [`EngineBuildContext`] foyer only supplies later. In practice it is a
    /// `BlockEngineConfig` carrying the same tuning the non-composite tier would have used;
    /// taking it from the caller is what keeps that tuning in one place.
    #[must_use]
    pub fn new(
        device: Arc<dyn Device>,
        store: ChunkStore,
        headers: Box<dyn EngineConfig<Key, CacheValue, HybridCacheProperties>>,
    ) -> Self {
        Self {
            device,
            store,
            headers,
        }
    }
}

impl EngineConfig<Key, CacheValue, HybridCacheProperties> for ChunkEngineConfig {
    fn build(
        self: Box<Self>,
        ctx: EngineBuildContext,
    ) -> BoxFuture<'static, FoyerResult<Arc<dyn Engine<Key, CacheValue, HybridCacheProperties>>>>
    {
        Box::pin(async move {
            let store = self.store;
            // Rebuild the hash -> key map from whatever the store's own scan recovered, so a
            // restart serves from the tier instead of refetching. See `rebuild_hashes` for
            // why this hashes the keys rather than reading a hash off disk.
            let hashes = rebuild_hashes(&store);
            // A second context for the inner half, cloned field by field because
            // `EngineBuildContext` is not `Clone`. Sharing the io engine and the metrics
            // registry is the point, not an accident: two registries would split foyer's own
            // `foyer_storage_*` series in half.
            let inner_ctx = EngineBuildContext {
                io_engine: Arc::clone(&ctx.io_engine),
                metrics: Arc::clone(&ctx.metrics),
                spawner: ctx.spawner.clone(),
                recover_mode: ctx.recover_mode,
            };
            // Awaited before our own state is assembled so a failure to recover the header
            // half is reported as a build failure, not discovered on the first `HEAD`.
            let headers = self.headers.build(inner_ctx).await?;
            let engine: Arc<dyn Engine<Key, CacheValue, HybridCacheProperties>> =
                Arc::new(ChunkEngine {
                    inner: Arc::new(Inner {
                        device: self.device,
                        store,
                        headers,
                        spawner: ctx.spawner,
                        hashes: Mutex::new(hashes),
                        inflight: AtomicU64::new(0),
                        idle: tokio::sync::Notify::new(),
                        stats: EngineStats::default(),
                    }),
                });
            Ok(engine)
        })
    }
}

/// Rebuild the hash -> key map for every chunk the store recovered at startup.
///
/// # Why hash the keys instead of storing the hash on disk
///
/// foyer's own `BlockEngine` writes the hash into its on-disk entry header, which is the
/// robust answer and the one to adopt if this design is kept. It is deliberately NOT what
/// this does, for one reason: **the need may disappear.** `Engine::load` takes only a hash
/// today, which is the entire reason this map exists — and both foyer#1287 and the
/// maintainer's own "expose `Eq` via type erasure" idea would pass the key down instead, at
/// which point the map, and any on-disk hash beside it, is dead weight. Hashing at startup
/// costs nothing to delete; a slot-format version bump does not.
///
/// # The coupling this accepts
///
/// It reproduces foyer's hash with [`foyer::DefaultHasher`], which is what
/// `HybridCacheBuilder` uses unless a caller supplies its own. **A cache built with a custom
/// hash builder would compute different hashes here**, and every recovered chunk would then
/// miss and be refetched. That degrades to a cold tier, never to a wrong serve — the slot's
/// key is still compared on every read — but it is a real limitation, and the reason this is
/// a prototype's answer rather than the design's.
fn rebuild_hashes(store: &ChunkStore) -> HashMap<u64, Arc<str>> {
    let hasher = foyer::DefaultHasher::default();
    store
        .keys()
        .into_iter()
        .map(|key| {
            // Hash the `str`, not the `Arc`: foyer hashes the KEY, and `Hash for Arc<T>`
            // forwards to `T`, but being explicit removes the question.
            let hash = hasher.hash_one(key.as_ref());
            (hash, key)
        })
        .collect()
}

/// Shared state behind the engine's `Arc`.
struct Inner {
    device: Arc<dyn Device>,
    store: ChunkStore,
    /// The other half of the composite: everything that is not a chunk body lives here.
    /// Built from the config the caller supplied, so this module never names a block size or
    /// a flusher count.
    headers: Arc<dyn Engine<Key, CacheValue, HybridCacheProperties>>,
    /// The runtime foyer gave us for disk work. Used for the blocking `pwrite` in
    /// `enqueue`, which must not run on the caller's thread — that caller is foyer's
    /// eviction path.
    spawner: Spawner,
    /// foyer addresses the disk tier by **hash**; the store is keyed by the chunk key. This
    /// is the only bookkeeping the adapter adds, and it is why the store itself needed no
    /// change to be plugged in here.
    hashes: Mutex<HashMap<u64, Arc<str>>>,
    /// Writes handed to the spawner and not yet finished. `enqueue` is fire-and-forget by
    /// contract, so this is the only thing that makes [`Engine::wait`] and
    /// [`Engine::close`] mean anything — without it a caller has no point at which a
    /// demoted chunk is guaranteed to be on disk.
    inflight: AtomicU64,
    /// Notified whenever [`Inner::inflight`] reaches zero.
    idle: tokio::sync::Notify,
    stats: EngineStats,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkEngine")
            .field("slots", &self.store.capacity_slots())
            .field("held", &self.store.len())
            .field("headers", &self.headers)
            .finish()
    }
}

/// ADR-0033's slot store as a foyer disk-cache engine.
#[derive(Debug)]
pub struct ChunkEngine {
    inner: Arc<Inner>,
}

impl ChunkEngine {
    /// The adapter's own counters.
    #[must_use]
    pub fn stats(&self) -> &EngineStats {
        &self.inner.stats
    }

    /// The slot store underneath, for the daemon's `pacer_chunk_store_*` gauges.
    #[must_use]
    pub fn store(&self) -> &ChunkStore {
        &self.inner.store
    }
}

impl Inner {
    /// The key a hash currently names, if any.
    fn key_for(&self, hash: u64) -> Option<Arc<str>> {
        self.hashes
            .lock()
            .expect("the hash index lock is never held across a fallible call")
            .get(&hash)
            .cloned()
    }

    /// Wait until every slot write this engine handed the spawner has finished.
    ///
    /// The `notified()` future is created BEFORE the count is read, which is what closes the
    /// race: a write finishing in between still wakes this waiter instead of leaving it
    /// parked on a count that already reached zero.
    async fn drain(&self) {
        loop {
            let notified = self.idle.notified();
            if self.inflight.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Forget a hash whose slot the store no longer holds.
    ///
    /// Without this the map would grow by one entry per eviction forever, since the store
    /// evicts slots on its own LRU and cannot tell this adapter about it.
    fn prune(&self, hash: u64) {
        self.hashes
            .lock()
            .expect("the hash index lock is never held across a fallible call")
            .remove(&hash);
        self.stats
            .stale_hashes_pruned
            .fetch_add(1, Ordering::Relaxed);
    }
}

impl Engine<Key, CacheValue, HybridCacheProperties> for ChunkEngine {
    fn device(&self) -> &Arc<dyn Device> {
        &self.inner.device
    }

    fn filter(&self, _hash: u64, _estimated_size: usize) -> StorageFilterResult {
        // Admit everything and discriminate in `enqueue`, where the VALUE is visible.
        // `filter` sees only a hash and a size estimate, and separating a header from a
        // chunk on size alone would be a guess about the value type.
        StorageFilterResult::Admit
    }

    fn enqueue(&self, piece: PieceRef<Key, CacheValue, HybridCacheProperties>, estimated: usize) {
        let hash = piece.hash();
        let Some(chunk) = piece.value().as_chunk() else {
            // An object header: a few hundred bytes, so it goes to the block engine rather
            // than costing a whole slot. `estimated` is forwarded untouched — it is the
            // inner engine's own admission input, and second-guessing it here would mean
            // this module deciding what the block engine's buffer budget is.
            self.inner
                .stats
                .headers_delegated
                .fetch_add(1, Ordering::Relaxed);
            self.inner.headers.enqueue(piece, estimated);
            return;
        };
        let key: Arc<str> = Arc::from(piece.key().as_str());
        let chunk = chunk.clone();
        let inner = Arc::clone(&self.inner);
        // Publish the hash BEFORE the write completes. A `load` that arrives in between
        // finds the key, asks the store, and gets a miss — which is the same answer it
        // would get if the write had not started, and is what foyer's read path already
        // handles. The alternative ordering would drop the mapping if the task were
        // cancelled after a successful write.
        inner
            .hashes
            .lock()
            .expect("the hash index lock is never held across a fallible call")
            .insert(hash, Arc::clone(&key));
        // Counted BEFORE the spawn, so `wait` cannot observe zero between the two.
        inner.inflight.fetch_add(1, Ordering::AcqRel);
        // `enqueue` is sync and fire-and-forget by contract, so the write goes to foyer's
        // own disk runtime rather than the eviction thread that called us.
        let handle = self.inner.spawner.spawn(async move {
            if let Err(e) = inner.store.put(&key, &chunk).await {
                inner.stats.enqueue_errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(key = %key, error = %e, "chunk engine could not write a demoted chunk");
            }
            if inner.inflight.fetch_sub(1, Ordering::AcqRel) == 1 {
                inner.idle.notify_waiters();
            }
        });
        // Nothing awaits this: foyer's contract for `enqueue` is that it returns
        // immediately and the write lands later, which is what `wait`/`close` are for.
        drop(handle);
    }

    fn load(
        &self,
        hash: u64,
    ) -> BoxFuture<'static, FoyerResult<Load<Key, CacheValue, HybridCacheProperties>>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            // The slots first, and only if this hash names a chunk we hold. A header's hash
            // is never in the map, so the common case costs one lookup and no I/O.
            if let Some(key) = inner.key_for(hash) {
                match inner.store.get(&key).await {
                    Ok(Some(chunk)) => {
                        return Ok(Load::Entry {
                            key: key.to_string(),
                            value: CacheValue::Chunk(chunk),
                            // `Young`, matching what BlockEngine reports for a loaded entry:
                            // it is what makes foyer skip re-enqueueing the chunk when the
                            // promotion it just did is later evicted, so a read does not
                            // become a write.
                            populated: Populated { age: Age::Young },
                        });
                    }
                    // The store's own LRU reused the slot. Forget the hash and fall through:
                    // the inner engine cannot hold this chunk, but asking it is what keeps
                    // `load` total, and a miss there is the same `Load::Miss` we would return.
                    Ok(None) => inner.prune(hash),
                    // Returned rather than masked by a header lookup. An I/O error on the
                    // chunk tier is information, and turning it into a miss would send the
                    // proxy back to S3 with nothing logged about why.
                    Err(e) => {
                        return Err(foyer::Error::new(
                            foyer::ErrorKind::Io,
                            format!("chunk engine read failed: {e}"),
                        ));
                    }
                }
            }
            inner.headers.load(hash).await
        })
    }

    fn delete(&self, hash: u64) {
        if let Some(key) = self.inner.key_for(hash) {
            self.inner.store.remove(&key);
            self.inner.prune(hash);
        }
        // Unconditional, not an `else`: neither half knows which one holds a hash, and a
        // delete that reached only the slots would leave a header foyer believes is gone.
        self.inner.headers.delete(hash);
    }

    fn may_contains(&self, hash: u64) -> bool {
        self.inner.key_for(hash).is_some() || self.inner.headers.may_contains(hash)
    }

    fn destroy(&self) -> BoxFuture<'static, FoyerResult<()>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let keys: Vec<Arc<str>> = {
                let mut map = inner
                    .hashes
                    .lock()
                    .expect("the hash index lock is never held across a fallible call");
                map.drain().map(|(_, key)| key).collect()
            };
            for key in keys {
                inner.store.remove(&key);
            }
            // Both halves, and the inner one's error is propagated: `destroy` is what
            // invalidation calls, and a half-destroyed tier would serve stale headers for
            // objects whose chunks are gone.
            inner.headers.destroy().await
        })
    }

    fn wait(&self) -> BoxFuture<'static, ()> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner.drain().await;
            // Both halves, because a caller that has waited must be able to conclude that
            // every demoted value is down — a header included.
            inner.headers.wait().await;
        })
    }

    fn close(&self) -> BoxFuture<'static, FoyerResult<()>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner.drain().await;
            inner.headers.close().await
        })
    }
}

/// How long a throttled filter result would ask a caller to wait. Unused while
/// [`ChunkEngine::filter`] always admits; named so the intent is visible if throttling is
/// ever added rather than a bare literal appearing then.
#[allow(dead_code)]
const THROTTLE_RETRY_AFTER: Duration = Duration::from_millis(10);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::{CachedChunk, ObjectHeader};
    use crate::object_key;
    use crate::store::{ReadShape, StoreConfig, DEFAULT_READ_CONCURRENCY};
    use crate::{build_chunk_cache_with_engine, CacheConfig, IoEngine, StorageTuning, UringConfig};
    use bytes::Bytes;

    /// Small enough to keep the test quick, larger than a slot header so bodies are real.
    const CHUNK: usize = 32 << 10;
    /// Slots in the test tier.
    const SLOTS: u64 = 8;

    fn chunk_of(byte: u8, len: usize) -> CachedChunk {
        CachedChunk::new(Bytes::from(vec![byte; len]))
    }

    /// One foyer block, so the header half has somewhere to put a block and the whole
    /// fixture stays small. **`disk_capacity / block_size` must be at least 1**: the two
    /// capacities below size *different* tiers now, and a device with room for zero blocks
    /// does not fail the build — the block engine's reclaimer waits for a clean block that
    /// can never exist, and the test hangs instead of erroring. That is exactly what the
    /// earlier fixture did, pairing a 64 MiB block with a 288 KiB device, and it only
    /// surfaced once the inner engine started using the device this engine merely reports.
    const BLOCK: usize = 1 << 20;

    /// Config pair for a tier whose RAM half cannot hold one chunk, so an insert has to
    /// reach the disk tier to survive at all.
    ///
    /// The two capacities are independent and must both be set, which is the composite's
    /// one operational consequence: `CacheConfig::disk_capacity` is foyer's device and now
    /// holds **headers**, while `StoreConfig::capacity_bytes` is the slot store's own
    /// directory and holds **chunks**. Sizing only one leaves the other tier unable to
    /// accept anything.
    fn configs(dir: &std::path::Path) -> (CacheConfig, StoreConfig) {
        (
            CacheConfig {
                dir: dir.to_path_buf(),
                mem_capacity: 1 << 10,
                disk_capacity: 4 * BLOCK,
                block_size: BLOCK,
                flush_buffer_size: 0,
                io_engine: IoEngine::Psync,
                uring: UringConfig::default(),
                tuning: StorageTuning::default(),
                // Unset, so the admission bound is whatever ships. This tier's own
                // capacity is what these tests constrain, and it does so in slots.
                max_object_bytes: None,
            },
            StoreConfig {
                dir: dir.join("chunk-store"),
                chunk_size: CHUNK,
                capacity_bytes: SLOTS * ((4 << 10) + CHUNK as u64),
                verify_body: true,
                // Both at their shipped values rather than anything convenient: these tests
                // assert the engine seat carries bytes through foyer intact, so the read
                // underneath it should be the read production performs. `0` here would mean
                // *unlimited*, which is a shape no deployment uses.
                read_shape: ReadShape::default(),
                read_concurrency: DEFAULT_READ_CONCURRENCY,
            },
        )
    }

    /// **The whole point of this module, in one test.** A chunk that exists ONLY on disk
    /// must be readable back through our engine, byte-exact.
    ///
    /// `storage_writer` is what makes that conclusive — it is the disk-only insert
    /// `never_promotion_serves_from_disk_without_filling_the_ram_tier` uses in lib.rs, and
    /// it marks the entry phantom so it never occupies the RAM tier at all. So the `get`
    /// below cannot be answered from memory, and a hit proves the whole path: foyer's
    /// lookup, our `Engine::load`, the store's `pread` into a slab frame, and back out as a
    /// `CacheValue` — with no foyer checksum or codec anywhere in it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_disk_only_chunk_reads_back_through_our_engine() {
        let dir = tempfile::tempdir().unwrap();
        let (cache_cfg, store_cfg) = configs(dir.path());
        let (cache, store) = build_chunk_cache_with_engine(&cache_cfg, store_cfg)
            .await
            .unwrap();

        let key = object_key("bucket", "obj#100:0");
        let entry = cache
            .storage_writer(key.clone())
            .insert(CacheValue::Chunk(chunk_of(0x5a, CHUNK)))
            .expect("a disk-only insert must be admitted");
        drop(entry);
        // Pushes the write pipeline to disk deterministically. Polling `cache.get` instead
        // does not work: foyer can answer a get out of its in-flight write queue
        // (`Load::Piece`) before the bytes are on disk.
        cache.close().await.unwrap();

        assert_eq!(store.len(), 1, "the chunk must occupy a slot");
        assert!(
            cache.memory().get(&key).is_none(),
            "a disk-only insert must leave the RAM tier empty, or this proves nothing"
        );

        let entry = cache
            .get(&key)
            .await
            .unwrap()
            .expect("the chunk must come back through our engine");
        let chunk = entry.value().as_chunk().expect("a chunk, not a header");
        assert_eq!(chunk.body.len(), CHUNK);
        assert!(chunk.body.iter().all(|&b| b == 0x5a));
    }

    /// **A restart serves from the tier.** The store recovers its key→slot index by scanning
    /// slot headers, and the adapter rebuilds its hash→key map from those keys
    /// ([`rebuild_hashes`]), so a chunk written before the restart is readable after it —
    /// through the cache, not just directly out of the store.
    ///
    /// This test previously asserted the opposite and passed: the map was memory-only, so
    /// `load(hash)` had nothing to resolve and every recovered chunk was refetched. Reading
    /// it back through `cache.get` rather than `store.get` is the point — the latter passed
    /// even when this was broken.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_restart_serves_from_the_recovered_tier() {
        let dir = tempfile::tempdir().unwrap();
        let (cache_cfg, store_cfg) = configs(dir.path());
        let key = object_key("bucket", "obj#100:0");

        let (cache, store) = build_chunk_cache_with_engine(&cache_cfg, store_cfg.clone())
            .await
            .unwrap();
        let entry = cache
            .storage_writer(key.clone())
            .insert(CacheValue::Chunk(chunk_of(0x5a, CHUNK)))
            .expect("a disk-only insert must be admitted");
        drop(entry);
        cache.close().await.unwrap();
        assert_eq!(store.len(), 1);
        drop(cache);

        let (cache, store) = build_chunk_cache_with_engine(&cache_cfg, store_cfg)
            .await
            .unwrap();
        assert_eq!(
            store.len(),
            1,
            "the store recovers its slots from their headers"
        );
        assert!(
            cache.memory().get(&key).is_none(),
            "a fresh cache's RAM tier is empty, so the read below must come from the tier"
        );
        let entry = cache
            .get(&key)
            .await
            .unwrap()
            .expect("the recovered chunk must be reachable THROUGH THE CACHE after a restart");
        let chunk = entry.value().as_chunk().expect("a chunk, not a header");
        assert_eq!(chunk.body.len(), CHUNK);
        assert!(chunk.body.iter().all(|&b| b == 0x5a));
    }

    /// An object header must never be given a slot: a fixed 16 MiB slot is the wrong home
    /// for a few-hundred-byte header, and spending one on each would waste the tier.
    ///
    /// Asserted through on-disk state rather than through `cache.get`, which was an earlier
    /// mistake here: `CacheValue::weight()` charges a header 256 bytes, so it fits even a
    /// 1 KiB RAM tier and never reaches the disk tier at all.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_header_is_not_given_a_slot() {
        let dir = tempfile::tempdir().unwrap();
        let (cache_cfg, store_cfg) = configs(dir.path());
        let (cache, store) = build_chunk_cache_with_engine(&cache_cfg, store_cfg)
            .await
            .unwrap();

        let chunk_key = object_key("bucket", "obj#100:0");
        cache.insert(
            object_key("bucket", "obj"),
            CacheValue::Header(ObjectHeader::new(4096, Some("etag".into()), None, None)),
        );
        cache.insert(chunk_key.clone(), CacheValue::Chunk(chunk_of(3, CHUNK)));
        cache.close().await.unwrap();

        assert_eq!(
            store.len(),
            1,
            "exactly one slot should be spent, on the chunk — a header must not get one"
        );
        assert!(
            store.get(&chunk_key).await.unwrap().is_some(),
            "and the slot that was spent must hold the chunk"
        );
    }

    /// **The composite's other half — the case this engine used to get wrong.** A header
    /// written disk-only must come back. It cannot be in a slot (asserted below), so a hit
    /// proves it was routed to the inner block engine *and* read back out of it.
    ///
    /// `storage_writer` is load-bearing for the same reason as in the chunk test: a header
    /// weighs 256 bytes by `CacheValue::weight()`, so an ordinary `insert` would sit in even
    /// a 1 KiB RAM tier and this would assert nothing about disk.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_disk_only_header_reads_back_through_the_inner_engine() {
        let dir = tempfile::tempdir().unwrap();
        let (cache_cfg, store_cfg) = configs(dir.path());
        let (cache, store) = build_chunk_cache_with_engine(&cache_cfg, store_cfg)
            .await
            .unwrap();

        let key = object_key("bucket", "obj");
        let entry = cache
            .storage_writer(key.clone())
            .insert(CacheValue::Header(ObjectHeader::new(
                4096,
                Some("etag".into()),
                None,
                None,
            )))
            .expect("a disk-only insert must be admitted");
        drop(entry);
        cache.close().await.unwrap();

        assert_eq!(store.len(), 0, "a header must not spend a slot");
        assert!(
            cache.memory().get(&key).is_none(),
            "a disk-only insert must leave the RAM tier empty, or this proves nothing"
        );

        let entry = cache
            .get(&key)
            .await
            .unwrap()
            .expect("the header must come back through the inner block engine");
        let header = entry.value().as_header().expect("a header, not a chunk");
        assert_eq!(header.object_len, 4096);
        assert_eq!(header.e_tag.as_deref(), Some("etag"));
    }

    /// A device with room for zero blocks is refused at build time.
    ///
    /// Worth a test rather than trust, because the failure it replaces is the worst kind:
    /// the block engine's reclaimer waits for a clean block that can never exist, so the
    /// caller **hangs with nothing logged**. It cost this module a debugging round.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_device_too_small_for_one_block_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (mut cache_cfg, store_cfg) = configs(dir.path());
        cache_cfg.disk_capacity = BLOCK - 1;
        let err = build_chunk_cache_with_engine(&cache_cfg, store_cfg)
            .await
            .expect_err("a device with room for zero blocks must not build");
        assert!(
            err.to_string().contains("disk_capacity"),
            "the error must name the knob to change, got: {err}"
        );
    }

    /// Both halves at once, across a restart: the chunk comes back from a recovered slot and
    /// the header from a recovered foyer block. Neither path can serve the other's value, so
    /// this is the test that would fail if `load`'s fallthrough were dropped.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_restart_recovers_both_halves() {
        let dir = tempfile::tempdir().unwrap();
        let (cache_cfg, store_cfg) = configs(dir.path());
        let chunk_key = object_key("bucket", "obj#100:0");
        let header_key = object_key("bucket", "obj");

        let (cache, _store) = build_chunk_cache_with_engine(&cache_cfg, store_cfg.clone())
            .await
            .unwrap();
        for (key, value) in [
            (chunk_key.clone(), CacheValue::Chunk(chunk_of(0x5a, CHUNK))),
            (
                header_key.clone(),
                CacheValue::Header(ObjectHeader::new(4096, Some("etag".into()), None, None)),
            ),
        ] {
            drop(
                cache
                    .storage_writer(key)
                    .insert(value)
                    .expect("a disk-only insert must be admitted"),
            );
        }
        cache.close().await.unwrap();
        drop(cache);

        let (cache, store) = build_chunk_cache_with_engine(&cache_cfg, store_cfg)
            .await
            .unwrap();
        assert_eq!(store.len(), 1, "exactly the chunk should hold a slot");

        let chunk = cache
            .get(&chunk_key)
            .await
            .unwrap()
            .expect("the chunk must survive the restart");
        assert_eq!(chunk.value().as_chunk().expect("a chunk").body.len(), CHUNK);

        let header = cache
            .get(&header_key)
            .await
            .unwrap()
            .expect("the header must survive the restart too");
        assert_eq!(
            header.value().as_header().expect("a header").object_len,
            4096
        );
    }
}
