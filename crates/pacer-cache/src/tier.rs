//! The one seam every chunk read and fill goes through (ADR-0033).
//!
//! Two implementations of "where a chunk's bytes live" now exist — foyer's hybrid cache
//! and the purpose-built [`crate::store::ChunkStore`] — and the daemon must not care which. So
//! the read paths ([`crate::read_chunk_entry`]'s three callers) and the fill paths hold a
//! [`ChunkTier`] instead of a [`ChunkCache`], and the choice is made once at startup.
//!
//! # Why a facade rather than a trait
//!
//! The two backends are not interchangeable in shape: foyer's is sync-insert /
//! async-get over a `CacheValue` that may be a header *or* a chunk, while the store is
//! async both ways and holds only chunks. A trait would have to be the union of both and
//! every caller would still branch. One concrete type with two constructors keeps the
//! branch in one place — here — and lets the call sites read as a single operation.
//!
//! # Headers stay in foyer either way
//!
//! An object header is a few hundred bytes, hot, and wants exactly a hybrid LRU; nothing
//! measured says it is a problem. So [`ChunkTier::cache`] hands out the foyer cache for
//! header work, unchanged, and only the *chunk* keyspace moves.

use crate::chunk::CachedChunk;
use crate::store::ChunkStore;
use crate::{read_chunk_entry, CacheValue, ChunkCache, Promotion};

/// Which backend holds chunk bodies on this node.
///
/// Selected by `config.diskTier` and fixed for the process. Cheap to clone (both
/// backends are `Arc`-backed), so every read and fill path can hold one.
#[derive(Clone)]
pub struct ChunkTier {
    /// Always present: it holds the object headers, and the chunk bodies too when
    /// `store` is `None`.
    cache: ChunkCache,
    /// `Some` when ADR-0033's store owns chunk bodies.
    store: Option<ChunkStore>,
    /// Only consulted on the foyer path — the store *is* the tier, so there is nothing
    /// for it to promote into.
    promotion: Promotion,
}

impl ChunkTier {
    /// Chunks live in foyer, as they did before ADR-0033. `config.diskTier=foyer`.
    #[must_use]
    pub fn foyer(cache: ChunkCache, promotion: Promotion) -> Self {
        Self {
            cache,
            store: None,
            promotion,
        }
    }

    /// Chunks live in `store`; foyer keeps the headers. `config.diskTier=store`.
    ///
    /// `promotion` is still taken so the value can be reported and so switching back is
    /// a config flip, but the chunk path does not read it.
    #[must_use]
    pub fn with_store(cache: ChunkCache, store: ChunkStore, promotion: Promotion) -> Self {
        Self {
            cache,
            store: Some(store),
            promotion,
        }
    }

    /// The foyer cache, for object-header reads and writes.
    #[must_use]
    pub fn cache(&self) -> &ChunkCache {
        &self.cache
    }

    /// The chunk store, when this node has one — for the daemon's metrics.
    #[must_use]
    pub fn store(&self) -> Option<&ChunkStore> {
        self.store.as_ref()
    }

    /// Whether chunks are in the ADR-0033 store. For the startup log line, so a node
    /// says which tier it is running rather than leaving it to be inferred.
    #[must_use]
    pub fn has_store(&self) -> bool {
        self.store.is_some()
    }

    /// Read one chunk's bytes, or `None` if this node does not hold it.
    ///
    /// Both backends report every kind of local absence — a genuine miss, and (in the
    /// store) a slot that failed its key or CRC check — as `Ok(None)`, because the
    /// caller's fallback is the same for all of them: the owning peer, then the backend.
    ///
    /// # Errors
    ///
    /// An I/O error from whichever backend holds chunks.
    pub async fn get_chunk(&self, key: &str) -> anyhow::Result<Option<CachedChunk>> {
        match &self.store {
            Some(store) => store.get(key).await,
            None => Ok(read_chunk_entry(&self.cache, key, self.promotion)
                .await?
                .as_ref()
                .and_then(CacheValue::as_chunk)
                .cloned()),
        }
    }

    /// Forget everything this node holds under `key` — ADR-0007's invalidation, which a
    /// write to an existing key issues so no stale body can be served afterwards.
    ///
    /// Clears **both** backends unconditionally rather than only the live one. A node
    /// whose `diskTier` was flipped between two runs over the same cache directory can
    /// have the key in either, and an invalidation that cleared only the current tier
    /// would leave the other holding bytes that a later flip back would serve.
    pub async fn forget(&self, key: &str) {
        self.cache.remove(key);
        if let Some(store) = &self.store {
            store.remove(key);
        }
    }

    /// Put a chunk's bytes in this node's tier.
    ///
    /// On the store path this is write-through — one `pwrite` — which costs nothing on a
    /// shape that demotes every chunk anyway, and removes foyer's submit queue and the
    /// silent drops it produced. On the foyer path it is the insert it always was.
    ///
    /// # Errors
    ///
    /// An I/O error on the store path. The foyer path does not fail: an entry foyer
    /// declines to admit is simply not cached.
    pub async fn put_chunk(&self, key: &str, chunk: CachedChunk) -> anyhow::Result<()> {
        match &self.store {
            Some(store) => store.put(key, &chunk).await,
            None => {
                self.cache.insert(key.to_owned(), CacheValue::Chunk(chunk));
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::StoreConfig;
    use crate::{
        build_chunk_cache, chunk::ObjectHeader, object_key, CacheConfig, IoEngine, StorageTuning,
        UringConfig,
    };
    use bytes::Bytes;

    /// Small so the tests are fast; larger than a slot header so bodies are real.
    const CHUNK: usize = 32 << 10;

    async fn foyer_cache(dir: &std::path::Path) -> ChunkCache {
        build_chunk_cache(&CacheConfig {
            dir: dir.to_path_buf(),
            mem_capacity: 8 << 20,
            disk_capacity: 64 << 20,
            block_size: 8 << 20,
            flush_buffer_size: 0,
            io_engine: IoEngine::Psync,
            uring: UringConfig::default(),
            tuning: StorageTuning::default(),
            max_object_bytes: None,
        })
        .await
        .unwrap()
    }

    async fn store_of(dir: &std::path::Path) -> ChunkStore {
        ChunkStore::open(StoreConfig {
            dir: dir.join("store"),
            chunk_size: CHUNK,
            capacity_bytes: 16 * (4096 + CHUNK as u64),
            verify_body: true,
        })
        .await
        .unwrap()
    }

    fn chunk_of(byte: u8, len: usize) -> CachedChunk {
        CachedChunk::new(Bytes::from(vec![byte; len]))
    }

    /// **The property that makes the facade worth having**: both tiers answer a
    /// put-then-get identically, so no call site has to know which one it holds. Driven
    /// over both constructors from one body, because a test that exercised only the new
    /// path would not show they agree.
    #[tokio::test(flavor = "multi_thread")]
    async fn both_tiers_round_trip_a_chunk_identically() {
        let dir = tempfile::tempdir().unwrap();
        let cache = foyer_cache(dir.path()).await;
        let tiers = [
            ChunkTier::foyer(cache.clone(), Promotion::OnDiskHit),
            ChunkTier::with_store(
                cache.clone(),
                store_of(dir.path()).await,
                Promotion::OnDiskHit,
            ),
        ];
        for (nth, tier) in tiers.iter().enumerate() {
            let key = format!("bucket/obj#100:{nth}");
            assert!(
                tier.get_chunk(&key).await.unwrap().is_none(),
                "tier {nth} must start empty for this key"
            );
            tier.put_chunk(&key, chunk_of(0x5a, CHUNK)).await.unwrap();
            let got = tier
                .get_chunk(&key)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("tier {nth} lost the chunk it was just given"));
            assert_eq!(got.body.len(), CHUNK, "tier {nth} body length");
            assert!(
                got.body.iter().all(|&b| b == 0x5a),
                "tier {nth} body contents"
            );
        }
    }

    /// A short last chunk (ADR-0015 clamps it) must survive both tiers — the case a
    /// fixed-slot layout is most likely to get wrong.
    #[tokio::test(flavor = "multi_thread")]
    async fn both_tiers_keep_a_short_last_chunk_short() {
        let dir = tempfile::tempdir().unwrap();
        let cache = foyer_cache(dir.path()).await;
        let tiers = [
            ChunkTier::foyer(cache.clone(), Promotion::OnDiskHit),
            ChunkTier::with_store(cache.clone(), store_of(dir.path()).await, Promotion::Never),
        ];
        for (nth, tier) in tiers.iter().enumerate() {
            let key = format!("bucket/obj#100:short{nth}");
            tier.put_chunk(&key, chunk_of(1, 7)).await.unwrap();
            assert_eq!(tier.get_chunk(&key).await.unwrap().unwrap().body.len(), 7);
        }
    }

    /// Headers must stay reachable through the foyer cache on **both** tiers: the store
    /// holds chunks only, and a header lookup that started failing would break every
    /// ranged read.
    #[tokio::test(flavor = "multi_thread")]
    async fn headers_stay_in_foyer_on_either_tier() {
        let dir = tempfile::tempdir().unwrap();
        let cache = foyer_cache(dir.path()).await;
        let tier = ChunkTier::with_store(cache, store_of(dir.path()).await, Promotion::OnDiskHit);
        let key = object_key("bucket", "obj");
        let header = ObjectHeader::new(4096, Some("etag".into()), None, None);
        tier.cache()
            .insert(key.clone(), CacheValue::Header(header.clone()));
        let got = tier.cache().get(&key).await.unwrap().unwrap();
        assert_eq!(got.value().as_header(), Some(&header));
        // And a header key must not be mistaken for a chunk by the chunk path.
        assert!(tier.get_chunk(&key).await.unwrap().is_none());
    }

    /// `has_store` must report the constructor honestly — the startup log line and the
    /// metrics both branch on it, and a wrong answer would misattribute an arm.
    #[tokio::test(flavor = "multi_thread")]
    async fn has_store_reports_which_tier_is_live() {
        let dir = tempfile::tempdir().unwrap();
        let cache = foyer_cache(dir.path()).await;
        assert!(!ChunkTier::foyer(cache.clone(), Promotion::OnDiskHit).has_store());
        let with = ChunkTier::with_store(cache, store_of(dir.path()).await, Promotion::OnDiskHit);
        assert!(with.has_store());
        assert!(with.store().is_some());
    }
}
