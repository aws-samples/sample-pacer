//! Getting one window's bytes into the client's memory, whoever writes them.
//!
//! ADR-0026 point 4's tier table, per window: a local cache hit is a `memcpy`, a
//! read-through is a copy, and only a REMOTE holder's serve is one-sided — over
//! ADR-0018's data plane into a window this node registered (ADR-0026 point 3) or,
//! when the client registered its own, into the client directly (ADR-0030 point 4,
//! with the digest coming back on the wire per point 7).
//!
//! **The invariant this module owns: a window is placed, or the decline says
//! whether trying again could help.** Only a client-registered target can decline
//! at all — a mapped one is `memcpy`-able, so it succeeds or errors — and
//! [`Decline::transient`] is what lets one unplaceable window be re-attempted
//! instead of costing the whole request its acceleration. The source order here
//! mirrors `super::fill`'s resolution order exactly, because delivery must not
//! change WHICH copy answers a read, only where the bytes end up.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use pacer_transport::TransportError;
use s3s::{s3_error, S3Result};
use tracing::{trace, warn};

use crate::delivery::{ChunkWindow, DeliveryDigest, DeliveryTarget};

use super::cluster::chunk_sources;
use super::fill::{collect_blob, FillCtx};
use super::target::ClientMemory;
#[cfg(feature = "efa")]
use super::target::ClientRegistration;

/// `source` label values of `pacer_delivery_chunks_total` — ADR-0026 point 4's
/// table, made observable. `peer_rdma` below 1.0 of the peer total is NOT a
/// fault: a local hit or a read-through is a copy by design, and only a remote
/// holder's serve is one-sided.
const SOURCE_LOCAL: &str = "local";
/// A remote holder's one-sided WRITE landed the chunk in client memory. Only an
/// `efa` build can reach it, so the series is simply absent on a gRPC-only node
/// rather than present-and-always-zero.
#[cfg(feature = "efa")]
const SOURCE_PEER_RDMA: &str = "peer_rdma";
/// A remote holder streamed the chunk and the daemon copied it in.
const SOURCE_PEER_STREAM: &str = "peer_stream";
/// The chunk was read through from the backend and copied in.
const SOURCE_BACKEND: &str = "backend";

/// How many times one delivery window is re-attempted after a *transient* decline
/// before the request degrades to a body ([`PacerProxy::place_with_retry`]).
///
/// Three, and the reason it is small is that the transport already owns the long ladder.
/// `TokenDecline::WriterNotInstalled` arrives only after `UNKNOWN_PEER_ATTEMPTS` (10 posts
/// spanning ~2 s) *and* a re-announce, and `NotAnnounceable` after a 250 ms announce
/// deadline — so by the time either reaches here, milliseconds of patience have already been
/// spent and what is left to buy is a couple of fresh announce cycles, not another ladder.
/// Three of those with the backoff below spans ~350 ms of additional wall clock in the worst
/// case, against a request that would otherwise be re-read in its entirety over the body
/// path.
const DELIVERY_DECLINE_ATTEMPTS: usize = 3;

/// First wait between those re-attempts, doubling: 50, 100, 200 ms.
///
/// Deliberately an order of magnitude above the transport's own 2 ms first backoff. That one
/// races a client building one address handle; this one waits for a client whose pump has
/// already missed a 250 ms announce deadline or a ~2 s WRITE ladder, which means it is busy
/// rather than merely late — measured at 8 GPUs, where a loader has up to 1024 handles to
/// build while verification pins every core (`bench/ladder/results/c5-multirail.md`). Retrying
/// such a client every 2 ms adds load to the thing being waited for.
const DELIVERY_DECLINE_BACKOFF: Duration = Duration::from_millis(50);

/// One window's placement: how many bytes reached the client, and a digest when the
/// placement itself produced one.
///
/// `digest` is `Some` only for a client-registered target, where it is computed over the
/// bytes sent because the window cannot be read back. For a mapped target it stays `None`
/// and [`Proxy::run_delivery`] digests the destination, which is the stronger check.
pub(super) struct Placed {
    pub(super) bytes: u64,
    pub(super) digest: Option<DeliveryDigest>,
    /// Where the bytes came from, as the `source` label of
    /// `pacer_delivery_chunks_total`. Carried back out rather than counted where it
    /// is known, because [`Proxy::run_delivery`] is the only place that also knows
    /// how long the resolution took — and a per-chunk latency split by anything
    /// other than its source would average a RAM hit against an NVMe read.
    pub(super) source: &'static str,
}

/// What became of one window's bytes.
///
/// Replaced an `Option<Placed>` so that a decline carries **why**, which is what lets
/// [`PacerProxy::run_delivery`] tell "this client has not installed us yet, re-attempt this
/// window" from "no rail on this node works, stop" — a distinction an `Option` could not
/// express, so the code took the pessimistic branch for both and one declined chunk cost the
/// whole request its acceleration (planning/19 § Track C).
pub(super) enum Placement {
    /// The bytes are in the client's memory.
    Placed(Placed),
    /// They are not, benignly. Only a client-registered (token) target can produce this:
    /// a mapped target is `memcpy`-able, so it succeeds or errors.
    Declined(Decline),
}

/// Why a window's bytes could not be placed, in the daemon's own terms.
///
/// Mirrors `pacer_transport::efa::TokenDecline` rather than re-exporting it, because
/// [`Placement`] has to exist in a build with no RDMA plane at all — where the token arm is
/// compiled out and nothing can decline, but [`PacerProxy::run_delivery`] is still the same
/// code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Decline {
    /// Whether re-attempting this one window can plausibly succeed while the request is
    /// still in flight. See `TokenDecline::is_transient`.
    transient: bool,
    /// The metric dimension and log field, from `TokenDecline::label`.
    reason: &'static str,
}

impl Placement {
    /// The placement if there was one — for the callers that only need to know whether to
    /// try another source, which is every peer-path helper.
    fn placed(self) -> Option<Placed> {
        match self {
            Self::Placed(p) => Some(p),
            Self::Declined(_) => None,
        }
    }
}

/// CRC32 one delivered window, on the blocking pool.
///
/// Same reasoning as the copy in `copy_window`: a checksum over a chunk-sized window
/// is CPU-bound work that never yields, and delivery runs `delivery.parallelism` of
/// them at once — leaving it on an async worker starves the runtime exactly as the
/// memcpy did, which on 2026-08-21 got the daemon SIGKILLed by its own liveness probe.
///
/// # Errors
///
/// The blocking task failing to join — a panic inside it, which for
/// `digest_window` means a bounds bug in the caller's window arithmetic.
pub(super) async fn digest_delivered(
    target: Arc<DeliveryTarget>,
    at: usize,
    len: usize,
) -> S3Result<DeliveryDigest> {
    tokio::task::spawn_blocking(move || target.digest_window(at, len))
        .await
        .map_err(|e| {
            warn!(error = %e, "delivery digest task failed");
            s3_error!(InternalError, "delivery checksum failed")
        })?
        .map_err(|e| {
            // A GPU read-back the driver refused. The bytes ARE delivered; only the
            // verification failed, so this is an error rather than a silent 200
            // without the checksum the client asked for.
            warn!(error = %e, "delivery digest failed");
            s3_error!(InternalError, "delivery checksum failed")
        })
}

impl FillCtx {
    /// Resolve one window, re-attempting a decline that says the client has not caught up
    /// yet.
    ///
    /// Bounded and *narrow*: only [`Decline::transient`] is retried, and a terminal decline
    /// returns on the first attempt so the caller can stop the whole pipeline immediately. A
    /// window that exhausts its attempts returns the last decline, which degrades the request
    /// exactly as it did before this existed — so the worst case is unchanged and the common
    /// case keeps its acceleration.
    ///
    /// # Errors
    ///
    /// Whatever [`Self::deliver_window`] errors with; a decline is not an error.
    pub(super) async fn place_with_retry(
        &self,
        window: ChunkWindow,
        client_memory: &ClientMemory,
    ) -> S3Result<Placement> {
        let mut backoff = DELIVERY_DECLINE_BACKOFF;
        for attempt in 1..=DELIVERY_DECLINE_ATTEMPTS {
            let placement = self.deliver_window(window, client_memory).await?;
            let Placement::Declined(declined) = placement else {
                return Ok(placement);
            };
            if !declined.transient || attempt == DELIVERY_DECLINE_ATTEMPTS {
                if declined.transient {
                    warn!(
                        chunk = window.idx,
                        reason = declined.reason,
                        attempts = DELIVERY_DECLINE_ATTEMPTS,
                        "window still unplaceable after every re-attempt; the request degrades to a body"
                    );
                }
                return Ok(placement);
            }
            trace!(
                chunk = window.idx,
                reason = declined.reason,
                attempt,
                "window declined for a transient reason; re-attempting this window only"
            );
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
        // Unreachable: the loop returns on every branch of its last iteration. Kept as a
        // decline rather than a panic, because the failure mode of being wrong here is a
        // degraded request, not a corrupt one.
        Ok(Placement::Declined(Decline {
            transient: false,
            reason: "retries_exhausted",
        }))
    }

    /// Deliver one covering chunk's window into the client's memory (ADR-0026
    /// point 4's tier table).
    ///
    /// Source order mirrors [`Self::resolve_chunk`] exactly — local hit, then a
    /// read-through if this node homes the chunk, then a peer, then the backend
    /// as the never-fail fallback — because delivery must not change *which* copy
    /// answers a read, only where the bytes end up.
    ///
    /// **No layer-1 admission here** (ADR-0016 layer 1), deliberately: on the
    /// RDMA path the bytes never pass through this process, so there is nothing
    /// to admit without reading them back out of the client's memory — and doing
    /// that on some paths but not others would make `pacer_local_admits_total`
    /// mean two different things. A chunk that is hot enough to keep is still
    /// admitted by ordinary body reads of the same key.
    ///
    /// ADR-0030 point 6 makes that a design decision rather than a consequence of
    /// where the bytes happen to be, and the reason is that the copy would buy no
    /// bandwidth: H2b measured the delivery ceiling per *leg* — 57 GiB/s
    /// host→host, 107 host→HBM, 361 HBM→HBM — so a host-memory leg costs about the
    /// same whether that DRAM is this node's or one SRD hop away, and reading a
    /// locally cached chunk into a client's HBM is bound by the same
    /// DRAM↔root-complex traversal as pulling it from a peer. A local copy is the
    /// same source at lower latency, not a cheaper one, and the checkpoint
    /// workload is not latency-bound. Note what this does NOT cover: the
    /// `owns_chunk` read-through below still fills, because a cluster miss that
    /// populated nothing would put every node's first read back on the backend.
    ///
    /// # Errors
    ///
    /// Only a backend fetch failure (no bytes to deliver); peer failures fall
    /// back and are never client-visible.
    async fn deliver_window(
        &self,
        window: ChunkWindow,
        client_memory: &ClientMemory,
    ) -> S3Result<Placement> {
        let chunk_key = self.chunk.chunk_key(&self.object_key, window.idx);
        // Timed even when it misses: a miss is what sends this chunk to a peer or the
        // backend, so charging only the hits would make the stage look cheap exactly
        // when it was not. `stage_seconds{stage="cache_read"}` plus
        // `stage_seconds{stage="copy"}` is the whole of `chunk_seconds` on a local hit.
        let read_started = std::time::Instant::now();
        let entry = self.tier.get_chunk(&chunk_key).await;
        self.metrics
            .delivery
            .stage_seconds
            .with_label_values(&[crate::metrics::DELIVERY_STAGE_CACHE_READ])
            .observe(read_started.elapsed().as_secs_f64());
        // Local hit: a `memcpy`, and NO registration — the fast path never touches
        // `ibv_reg_mr`, which is the entire point of `ClientMemory`'s laziness.
        if let Ok(Some(chunk)) = entry {
            self.metrics.cache_hits.inc();
            self.metrics.bytes_from_cache.inc_by(window.len as u64);
            return self
                .place_window(client_memory, window, &chunk.body, SOURCE_LOCAL)
                .await;
        }
        if self.owns_chunk(&chunk_key) {
            let bytes = self
                .fetch_from_backend(window.idx, &chunk_key, self.admit)
                .await?;
            return self
                .place_window(client_memory, window, &bytes, SOURCE_BACKEND)
                .await;
        }
        if let Some(delivered) = self
            .deliver_from_peer(&chunk_key, window, client_memory)
            .await
        {
            return Ok(Placement::Placed(delivered));
        }
        let bytes = self
            .fetch_from_backend(window.idx, &chunk_key, false)
            .await?;
        self.place_window(client_memory, window, &bytes, SOURCE_BACKEND)
            .await
    }

    /// Put this window's slice of `body` into the client's memory, however that memory has
    /// to be written — **the one place the two registration models differ**.
    ///
    /// [`Placement::Declined`] means the bytes could not be placed *at all*. That is
    /// reachable only for a token target: the daemon has no mapping of client-registered
    /// memory, so if the WRITE is unavailable (no healthy rail, the client is not
    /// announceable, the body does not fit a staging range) there is no `memcpy` to fall back
    /// to. A mapped target always succeeds or errors.
    ///
    /// The decline carries whether it is worth re-attempting — see [`Decline`] — because the
    /// caller's choice between retrying this window and degrading the whole request depends
    /// entirely on that.
    ///
    /// Note what is NOT a reason to degrade: a chunk being remote. With
    /// [`crate::delivery::DeliveryConfig::remote_write`] on, its holder writes it into the
    /// client directly and this function never sees it (ADR-0030 point 4, planning/19 C3);
    /// off, or when that holder declines, its bytes arrive in this node's memory and are
    /// written on here like any other — two hops, never a dead end.
    ///
    /// `source` is `&'static str` rather than `&str` here and in the two functions below
    /// because it is always one of the `SOURCE_*` consts and it now rides back out on
    /// [`Placed`], where the caller needs it to label the chunk's latency.
    ///
    /// # Errors
    ///
    /// A copy or a WRITE failing for a reason that is not a fallback: the blocking task
    /// panicking, `cuMemcpyHtoD` refusing, or the transport reporting a bad descriptor.
    async fn place_window(
        &self,
        client_memory: &ClientMemory,
        window: ChunkWindow,
        body: &Bytes,
        source: &'static str,
    ) -> S3Result<Placement> {
        match client_memory {
            ClientMemory::Mapped { target, .. } => {
                let bytes = self
                    .copy_window(Arc::clone(target), window, body, source)
                    .await?;
                // The mapped arm's digest is read back from the window by `run_delivery`,
                // as it always was: that also proves what is *in* the client's memory,
                // which is worth more than digesting what we sent.
                Ok(Placement::Placed(Placed {
                    bytes,
                    digest: None,
                    source,
                }))
            }
            #[cfg(feature = "efa")]
            ClientMemory::Token { window: token, .. } => {
                // The same slice `copy_window` takes, with the same defensive `min`s: a body
                // shorter than the window means the object shrank under us, which must
                // truncate the delivery rather than read past the chunk. Sliced here rather
                // than for both arms because the mapped arm slices for itself.
                let from = window.src_at.min(body.len());
                let to = (window.src_at + window.len).min(body.len());
                self.write_window(token, window, body.slice(from..to), source)
                    .await
            }
        }
    }

    /// Copy this window's slice of a chunk body into the client's memory and
    /// count it under `source`. The `min`s are defensive: a body shorter than the
    /// window means the object shrank under us, which must truncate the delivery
    /// rather than read past the chunk.
    async fn copy_window(
        &self,
        target: Arc<DeliveryTarget>,
        window: ChunkWindow,
        body: &Bytes,
        source: &'static str,
    ) -> S3Result<u64> {
        let from = window.src_at.min(body.len());
        let to = (window.src_at + window.len).min(body.len());
        // `slice` is a refcount bump, not a copy — the bytes move exactly once, in
        // the blocking task below.
        let src = body.slice(from..to);
        let len = src.len() as u64;
        let at = window.dst_at;
        // Timed from HERE, not from inside the closure: the wait for a blocking-pool
        // thread is part of what a chunk pays, and it is the difference between "the
        // copy is slow" and "the copies are queueing". Measuring only `copy_in` would
        // hide the second, which is the likelier of the two at this fan-out.
        let _copy_timer = crate::metrics::ScopedStage::new(
            &self.metrics.delivery.stage_seconds,
            crate::metrics::DELIVERY_STAGE_COPY,
        );
        // OFF THE ASYNC RUNTIME. A chunk-sized `memcpy` is CPU-bound work that never
        // yields, and delivery runs `delivery.parallelism` of them at once: at 256 ×
        // 16 MiB the tokio workers were fully occupied copying, the admin listener
        // never got scheduled, and the daemon was SIGKILLed by its own liveness probe
        // (2026-08-21, exit 137). The blocking pool is what this work belongs on.
        tokio::task::spawn_blocking(move || target.copy_in(at, &src))
            .await
            .map_err(|e| {
                warn!(error = %e, "delivery copy task failed");
                s3_error!(InternalError, "delivery copy failed")
            })?
            .map_err(|e| {
                // Only reachable on a GPU target: `cuMemcpyHtoD` refused. The host
                // arm cannot fail, so this is not a cost the shm path pays.
                warn!(error = %e, "copy into client memory failed");
                s3_error!(InternalError, "delivery copy failed")
            })?;
        self.metrics
            .delivery
            .chunks
            .with_label_values(&[source])
            .inc();
        // Per-chunk bytes, which is what a bandwidth graph needs: `delivery.bytes` moves once
        // per request (see its doc comment), so it cannot be rate()d.
        self.metrics
            .delivery
            .chunk_bytes
            .with_label_values(&[source])
            .inc_by(len);
        Ok(len)
    }

    /// WRITE this window into a client-registered target, and digest the bytes we sent.
    ///
    /// The digest is computed here rather than by reading the window back, because the
    /// daemon has no mapping of it (ADR-0030 point 7) — and on the blocking pool, for the
    /// same reason the mapped arm's copy and digest are: a chunk-sized CRC never yields, and
    /// `delivery.parallelism` of them starved the runtime badly enough to trip the liveness
    /// probe once already.
    ///
    /// # Errors
    ///
    /// The transport reporting a real failure (a rejected post, or a WRITE that completed
    /// with a failure status), or the digest task panicking. A *benign* refusal is
    /// [`Placement::Declined`] carrying its reason.
    #[cfg(feature = "efa")]
    async fn write_window(
        &self,
        token: &pacer_transport::token::TokenWindow,
        window: ChunkWindow,
        src: Bytes,
        source: &'static str,
    ) -> S3Result<Placement> {
        use pacer_transport::efa::TokenWrite;

        let Some(efa) = self.cluster.as_ref().and_then(|c| c.efa.as_ref()) else {
            // No RDMA plane: a token target cannot be written at all here, and no amount of
            // re-attempting builds one.
            return Ok(Placement::Declined(Decline {
                transient: false,
                reason: "no_rdma_plane",
            }));
        };
        let outcome = efa
            .write_into_token(token, window.dst_at, &src)
            .await
            .map_err(|e| {
                // Counted here and not deeper, for the same reason a decline is counted here
                // (just below): this is where "the WRITE did not happen" becomes the client's
                // answer, so it is where the count and the HTTP status cannot disagree. The
                // reason is the transport's own classification of the typed completion, never
                // a parse of the message — see `write_failure_reason`.
                let reason = pacer_transport::efa::write_failure_reason(&e);
                self.metrics
                    .delivery
                    .write_failures
                    .with_label_values(&[reason])
                    .inc();
                // `chunk` and `reason` are new on this line. The chunk index is what makes
                // one incident's work requests groupable after the fact — deliberately a log
                // field and not a label, since it is unbounded in the object's size.
                warn!(
                    error = %format!("{e:#}"),
                    reason,
                    chunk = window.idx,
                    "WRITE into client-registered memory failed"
                );
                s3_error!(InternalError, "delivery write failed")
            })?;
        if let TokenWrite::Declined(declined) = outcome {
            self.metrics
                .delivery
                .declines
                .with_label_values(&[declined.label()])
                .inc();
            return Ok(Placement::Declined(Decline {
                transient: declined.is_transient(),
                reason: declined.label(),
            }));
        }
        let len = src.len() as u64;
        let digest = tokio::task::spawn_blocking(move || DeliveryDigest::of(&src))
            .await
            .map_err(|e| {
                warn!(error = %e, "delivery digest task failed");
                s3_error!(InternalError, "delivery checksum failed")
            })?;
        self.metrics
            .delivery
            .chunks
            .with_label_values(&[source])
            .inc();
        // The RDMA arm's half of the bandwidth series (see `copy_window`): on this path the
        // bytes cross the NIC and nothing else on the node counts them, so without this the
        // only per-chunk signal a token delivery leaves is a chunk COUNT.
        self.metrics
            .delivery
            .chunk_bytes
            .with_label_values(&[source])
            .inc_by(len);
        Ok(Placement::Placed(Placed {
            bytes: len,
            digest: Some(digest),
            source,
        }))
    }

    /// Deliver one window from a peer that holds the chunk, trying sources in the
    /// same preference order as [`Self::fetch_from_peer`]. `None` means "fall
    /// back to the backend".
    ///
    /// A whole-chunk window takes the one-sided path first (the holder WRITEs
    /// into the client's memory and this node touches no bytes); a partial edge
    /// window cannot, because a holder-driven WRITE moves the entire cached body
    /// (ADR-0018) and would overwrite its neighbours — so it is fetched and
    /// copied like any other slice.
    async fn deliver_from_peer(
        &self,
        chunk_key: &str,
        window: ChunkWindow,
        client_memory: &ClientMemory,
    ) -> Option<Placed> {
        let cluster = self.cluster.as_ref()?;
        // The free function rather than the method: the pre-flight answers with the SAME
        // holder set this loop reads, and having one derivation is the point — a pre-flight
        // that disagrees with the read is worse than no pre-flight at all.
        for source in &chunk_sources(cluster, chunk_key) {
            if window.whole {
                if let Some(placed) = self
                    .holder_writes_client(source, chunk_key, window, client_memory)
                    .await
                {
                    return Some(placed);
                }
            }
            match cluster
                .transport
                .fetch_blob(source, chunk_key, None, self.no_fill)
                .await
            {
                Ok(blob) => match collect_blob(blob).await {
                    Ok(bytes) => {
                        self.metrics.peer_fetches.inc();
                        self.metrics.bytes_from_peers.inc_by(bytes.len() as u64);
                        return self
                            .place_window(client_memory, window, &bytes, SOURCE_PEER_STREAM)
                            .await
                            .ok()
                            .and_then(Placement::placed);
                    }
                    Err(e) => {
                        self.metrics.peer_fallbacks.inc();
                        warn!(key = %chunk_key, source = %source.name(), error = %e,
                            "peer chunk stream failed mid-delivery; trying next source / backend");
                    }
                },
                Err(TransportError::NotCached) => {}
                Err(e) => {
                    self.metrics.peer_fallbacks.inc();
                    warn!(key = %chunk_key, source = %source.name(), error = %e,
                        "peer chunk fetch failed mid-delivery; trying next source / backend");
                }
            }
        }
        None
    }

    /// Ask `source` to WRITE this whole chunk straight into the client's window, by whichever
    /// of the two registration models this request's memory uses. `None` = this source did
    /// not deliver; the caller retries it over gRPC and then moves on.
    ///
    /// The dispatch is the whole function, and the two arms differ in *who registered the
    /// destination*:
    ///
    /// * **Mapped** (`shm:`, ADR-0026) — this node registered the client's
    ///   window on one of its own rails, so the holder is offered an `RdmaBuffer` and the
    ///   registration is created here, lazily. This is the FIRST place one can possibly be
    ///   needed: a whole-chunk window a PEER will write. `registration_for` creates it once
    ///   per request and hands the same handle to every later window.
    /// * **Token** (`nic:`, ADR-0030) — the CLIENT registered its own window on its own
    ///   rails, so there is nothing to register here and the holder is offered the token
    ///   itself. That is planning/19's C3, and it is gated on
    ///   [`crate::delivery::DeliveryConfig::remote_write`] so both arms can be measured on
    ///   one image. Gated off, a remote chunk still reaches the client — it arrives here and
    ///   is written on from here, two hops, which is what `place_window` has always done.
    #[cfg(feature = "efa")]
    async fn holder_writes_client(
        &self,
        source: &pacer_ring::NodeId,
        chunk_key: &str,
        window: ChunkWindow,
        client_memory: &ClientMemory,
    ) -> Option<Placed> {
        let cluster = self.cluster.as_ref()?;
        match client_memory {
            ClientMemory::Mapped { .. } => {
                let registered = client_memory.registration_for(cluster, &self.metrics).await;
                self.rdma_into_client(source, chunk_key, window, client_memory, registered)
                    .await
            }
            ClientMemory::Token { .. } if self.remote_write => {
                self.rdma_into_client_token(source, chunk_key, window, client_memory)
                    .await
            }
            // The control arm: fall through to the streaming path, whose bytes `place_window`
            // then WRITEs into the client from this node.
            ClientMemory::Token { .. } => None,
        }
    }

    /// gRPC-only counterpart: there is no RDMA plane to write client memory with, so every
    /// peer chunk lands through [`Self::deliver_from_peer`]'s streaming path.
    #[cfg(not(feature = "efa"))]
    async fn holder_writes_client(
        &self,
        _source: &pacer_ring::NodeId,
        _chunk_key: &str,
        _window: ChunkWindow,
        _client_memory: &ClientMemory,
    ) -> Option<Placed> {
        None
    }

    /// Ask `source` to WRITE this whole chunk into the **client-registered** window this
    /// request delivers into — ADR-0030's remote half, planning/19's C3.
    ///
    /// What this removes is a hop, not a copy: before it, a remote chunk bound for a client
    /// token arrived in this node's memory and was WRITTEN into the client from here, so every
    /// byte of a checkpoint crossed one node's rails however many holders were serving it.
    /// Here N holders write one client buffer at once, which is the only shape in which a
    /// single read can approach the fabric's own ceiling (H2 measured 360.709 GiB/s, and one
    /// node's share of it is a fraction).
    ///
    /// Two things ride along that the mapped arm does not need:
    ///
    /// * **The digest comes back on the wire.** This node never sees these bytes and cannot
    ///   read the window back, so the holder reports a CRC32 of what it wrote and that is
    ///   folded like any other window's (ADR-0030 point 7). The transport refuses a landed
    ///   WRITE with no CRC32 when one was asked for, so a hole can never be folded silently.
    /// * **A decline is not a decline of the request.** A holder that cannot reach the client
    ///   streams the body instead, and it is written from here — the two-hop path, i.e. the
    ///   control arm, reached per chunk. That is why the worst case of this feature is the
    ///   behaviour it replaces.
    #[cfg(feature = "efa")]
    async fn rdma_into_client_token(
        &self,
        source: &pacer_ring::NodeId,
        chunk_key: &str,
        window: ChunkWindow,
        client_memory: &ClientMemory,
    ) -> Option<Placed> {
        use pacer_transport::efa::{ChunkDelivery, TokenDestination};

        // Destructured here rather than taken as two more arguments: the window and the
        // client's verification choice are one decision the request made, and the streaming
        // fallback below needs the whole `ClientMemory` anyway. Unreachable for any other arm
        // — `holder_writes_client` is the only caller and it has already matched.
        let ClientMemory::Token {
            window: token,
            checksum,
        } = client_memory
        else {
            return None;
        };
        let efa = self.cluster.as_ref()?.efa.as_ref()?;
        let delivered = efa
            .fetch_chunk_into_token(
                source,
                chunk_key,
                TokenDestination {
                    window: token,
                    at: window.dst_at,
                    len: window.len,
                    // The client's own choice, carried through: verification is O(bytes) on
                    // the HOLDER's CPU here, so `checksum=none` has to reach it.
                    checksum: *checksum,
                },
                self.no_fill,
            )
            .await;
        match delivered {
            Ok(ChunkDelivery::Landed { bytes, crc32 }) => {
                self.metrics.peer_fetches.inc();
                self.metrics.bytes_from_peers.inc_by(bytes);
                self.metrics
                    .delivery
                    .chunks
                    .with_label_values(&[SOURCE_PEER_RDMA])
                    .inc();
                // Counted here even though this node never touched the bytes: what the series
                // plots is the delivery plane's rate, not this node's memory traffic.
                self.metrics
                    .delivery
                    .chunk_bytes
                    .with_label_values(&[SOURCE_PEER_RDMA])
                    .inc_by(bytes);
                Some(Placed {
                    bytes,
                    // `None` when the client opted out of verification, in which case
                    // `run_delivery` folds nothing for this window either way.
                    digest: crc32.map(|crc| DeliveryDigest::reported(crc, bytes)),
                    source: SOURCE_PEER_RDMA,
                })
            }
            // The holder declined and streamed instead — the bytes are here, so write them
            // into the client from this node rather than re-fetching over the same wire.
            Ok(ChunkDelivery::Streamed(blob)) => match collect_blob(blob).await {
                Ok(bytes) => {
                    self.metrics.peer_fetches.inc();
                    self.metrics.bytes_from_peers.inc_by(bytes.len() as u64);
                    self.place_window(client_memory, window, &bytes, SOURCE_PEER_STREAM)
                        .await
                        .ok()
                        .and_then(Placement::placed)
                }
                Err(e) => {
                    self.metrics.peer_fallbacks.inc();
                    warn!(key = %chunk_key, source = %source.name(), error = %e,
                        "holder streamed a chunk that failed mid-delivery; falling back");
                    None
                }
            },
            Err(TransportError::NotCached) => None,
            Err(e) => {
                self.metrics.peer_fallbacks.inc();
                warn!(key = %chunk_key, source = %source.name(), error = %e,
                    "holder-into-client RDMA delivery failed; falling back");
                None
            }
        }
    }

    /// Ask `source` to WRITE this whole chunk straight into the client's window
    /// (ADR-0026 point 3 + ADR-0018's data plane). `None` = this source did not
    /// deliver; the caller retries it over gRPC and then moves on.
    ///
    /// Returns the placement rather than a byte count so the holder-declined case
    /// keeps its own `source`: those bytes were streamed and copied here, and
    /// reporting them as `peer_rdma` because this is the RDMA function would put a
    /// fallback's latency in the wrong series.
    #[cfg(feature = "efa")]
    async fn rdma_into_client(
        &self,
        source: &pacer_ring::NodeId,
        chunk_key: &str,
        window: ChunkWindow,
        client_memory: &ClientMemory,
        registered: Option<&Arc<ClientRegistration>>,
    ) -> Option<Placed> {
        let cluster = self.cluster.as_ref()?;
        let (client_target, efa) = (registered?, cluster.efa.as_ref()?);
        let delivered = efa
            .fetch_chunk_into(
                source,
                chunk_key,
                client_target,
                window.dst_at,
                window.len,
                self.no_fill,
            )
            .await;
        match delivered {
            // `crc32` is `None` by construction on this arm — the daemon registered this
            // window, so `run_delivery` digests it by reading it back, which proves what is
            // IN the client's memory rather than what a holder says it sent.
            Ok(pacer_transport::efa::ChunkDelivery::Landed { bytes, crc32: _ }) => {
                self.metrics.peer_fetches.inc();
                self.metrics.bytes_from_peers.inc_by(bytes);
                self.metrics
                    .delivery
                    .chunks
                    .with_label_values(&[SOURCE_PEER_RDMA])
                    .inc();
                // Holder-written bytes count on the bandwidth series too, even though this
                // node never touched them: it is the delivery plane's rate that is being
                // plotted, not this node's memory traffic.
                self.metrics
                    .delivery
                    .chunk_bytes
                    .with_label_values(&[SOURCE_PEER_RDMA])
                    .inc_by(bytes);
                // Reachable for a mapped target only (a token has no registration to
                // offer), so its digest is read back from the window like every other
                // mapped window's.
                Some(Placed {
                    bytes,
                    digest: None,
                    source: SOURCE_PEER_RDMA,
                })
            }
            // The holder declined to WRITE (no cached AH for us, capacity, its
            // own WRITE error) and streamed instead — the bytes are here, so
            // land them rather than re-fetching over the same wire.
            Ok(pacer_transport::efa::ChunkDelivery::Streamed(blob)) => {
                match collect_blob(blob).await {
                    Ok(bytes) => {
                        self.metrics.peer_fetches.inc();
                        self.metrics.bytes_from_peers.inc_by(bytes.len() as u64);
                        self.place_window(client_memory, window, &bytes, SOURCE_PEER_STREAM)
                            .await
                            .ok()
                            .and_then(Placement::placed)
                    }
                    Err(e) => {
                        self.metrics.peer_fallbacks.inc();
                        warn!(key = %chunk_key, source = %source.name(), error = %e,
                        "holder streamed a chunk that failed mid-delivery; falling back");
                        None
                    }
                }
            }
            Err(TransportError::NotCached) => None,
            Err(e) => {
                self.metrics.peer_fallbacks.inc();
                warn!(key = %chunk_key, source = %source.name(), error = %e,
                    "RDMA delivery into client memory failed; falling back");
                None
            }
        }
    }

    // No gRPC-only counterpart to `rdma_into_client` either: `holder_writes_client`'s stub
    // is the one place a featureless build answers "no holder writes the client here", and it
    // returns before reaching either registration model.
}
