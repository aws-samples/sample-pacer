//! Client-owned WRITE destinations (ADR-0026, planning/19 Track C C1): register
//! memory the *client* owns on one rail's protection domain, and let holders
//! WRITE cached chunks straight into it.
//!
//! This is ADR-0018's data plane with one hop removed. There, the requester
//! leased a range of its own arena, offered `(addr, rkey, len)` on the
//! `FetchBlob` call, and the holder WROTE into it — and nothing about that shape
//! requires the range to belong to a *daemon*. Here the range is a window of a
//! client's shared-memory segment (mapped by `pacer_daemon::delivery`), so the
//! bytes land where the consumer already wanted them and the requester's arena,
//! the HTTP body and the whole TCP leg leave the data path.
//!
//! Two things stay exactly as they were, deliberately:
//!
//! - **The peer wire protocol.** A holder cannot tell whose memory it is
//!   writing into: same `RdmaBuffer` fields, same done-as-response contract
//!   (ADR-0018 point 3), same `served_via_rdma` fallback. `peer.rs` is
//!   untouched.
//! - **The client never speaks RDMA** (ADR-0026 point 3). An rkey is scoped to
//!   the protection domain that issued it, so making every loader an EFA
//!   participant is a non-starter; the daemon registers on the client's behalf
//!   and the client's only obligations are to allocate, name, and not free early.
//!
//! What is genuinely different from the arena is the **lifetime**: an arena is
//! registered once at startup (ADR-0024 point 1, the invariant `buffers.rs`
//! spells out), while a client target is registered per request and
//! deregistered when [`ClientTarget`] drops — ADR-0026 point 7 requires exactly
//! that, since the daemon must not hold a registration across requests without
//! an explicit release step. Registration is therefore ON the request path, and
//! its cost is measured rather than assumed (the daemon's
//! `pacer_delivery_register_seconds_total`): one registration per GET amortized
//! over every chunk of the object, not one per chunk.

use std::sync::atomic::Ordering;

use anyhow::Context;
use ibverbs::{MemoryRegion, RemoteMemorySlice};
use pacer_proto::v1::{FetchBlobRequest, RdmaBuffer};
use pacer_ring::NodeId;

use crate::grpc::{call_fetch_blob, read_first_chunk};
use crate::token::TokenWindow;
use crate::{BlobStream, TransportError};

use super::buffers::efa_access_flags;
use super::{decode_stream_fallback, EfaRdmaTransport};

/// A client-owned region registered for one request's deliveries.
///
/// Pinned to ONE rail, because an rkey is only valid at the protection domain
/// that issued it and the by-index pairing rule
/// ([`EfaRdmaTransport`](super::EfaRdmaTransport)'s `rails` field) makes a
/// descriptor meaningful only alongside the rail it came from. Registering the
/// same window on all 32 rails would pay 32 `ibv_reg_mr` calls per request to
/// buy intra-request rail spread — which is planning/19's **C3** (parallel
/// fill), not C1: today concurrency ACROSS requests is what aggregates rails,
/// exactly as it is for arena-backed fetches.
///
/// Dropping this deregisters the MR. The mapping it covers must outlive it —
/// see [`EfaRdmaTransport::register_client_target`]'s safety contract.
pub struct ClientTarget {
    /// Registration over the whole client window, dropped (deregistered) with
    /// this value.
    mr: MemoryRegion<()>,
    /// Rail whose PD issued `mr`, and therefore the rail every WRITE into this
    /// target must be posted from.
    rail: usize,
    /// Address of the window's first byte, as the client's mapping sees it.
    base_addr: u64,
    /// Window length in bytes.
    len: usize,
}

impl ClientTarget {
    /// Rail this target is pinned to.
    #[must_use]
    pub fn rail(&self) -> usize {
        self.rail
    }

    /// Window length in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the window is empty (never, in practice — the descriptor parser
    /// rejects a zero length; present because clippy asks for it beside
    /// [`ClientTarget::len`]).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The `(addr, rkey, len)` naming `[at, at + len)` of this window — what a
    /// holder is offered, so its WRITE cannot reach past the sub-range it was
    /// given even though one registration covers the whole window (the same
    /// slicing the arena does per range).
    ///
    /// # Panics
    ///
    /// If `at + len` exceeds the window: the caller derives both from
    /// `pacer_daemon::delivery::chunk_windows`, which never exceeds the
    /// requested range, so this is a caller bug rather than a runtime
    /// condition.
    fn remote_at(&self, at: usize, len: usize) -> RemoteMemorySlice {
        assert!(
            at.saturating_add(len) <= self.len,
            "client target window overflow: {at} + {len} > {}",
            self.len
        );
        RemoteMemorySlice {
            addr: self.base_addr + at as u64,
            len,
            rkey: self.mr.remote().rkey,
        }
    }
}

/// How one chunk reached (or failed to reach) a client's memory.
///
/// The two arms are ADR-0026 point 4's table for the remote tier: a holder
/// either WRITEs the chunk into the client's window, or it declines and streams
/// the body the way it always has. Note what is NOT here: an error. A holder
/// that cannot WRITE is a *fallback*, not a fault (ADR-0003), and the caller
/// still delivers the bytes — just with one copy on this node.
pub enum ChunkDelivery {
    /// The holder's one-sided WRITE landed the chunk directly in the client's
    /// window. Nothing was copied on this node.
    Landed {
        /// How many bytes the holder reported writing.
        bytes: u64,
        /// CRC32 of those bytes as the holder computed them, present only on the
        /// client-token path and only when the request asked for one.
        ///
        /// `None` everywhere else, and that asymmetry is the point: when the
        /// daemon registered the window it can read the delivered bytes back and
        /// digest what is actually *in* the client's memory, which is the
        /// stronger check. When the CLIENT registered it there is nothing to read
        /// back, so the digest has to come from the side that held the bytes
        /// (ADR-0030 point 7).
        crc32: Option<u32>,
    },
    /// The holder streamed the body instead (it had no cached AH for this
    /// requester, the body did not fit the offered window, or its own WRITE
    /// errored). The caller copies these bytes into the window — still no TCP
    /// leg to the client, just one memcpy more than the RDMA path.
    Streamed(BlobStream),
}

impl EfaRdmaTransport {
    /// Register a client-owned region as a WRITE destination for the life of one
    /// request (ADR-0026 point 3).
    ///
    /// Picks a rail round-robin among those whose completion plane is alive, and
    /// pins the target to it (see [`ClientTarget`] for why one rail).
    ///
    /// # Safety
    ///
    /// `ptr` must point to `len` bytes of memory that stays mapped, and stays
    /// exclusively the caller's to hand out, for as long as the returned
    /// [`ClientTarget`] lives. A peer will DMA-write into it with no further
    /// coordination, so an early unmap is a use-after-free and an overlapping
    /// second target is silent corruption — the same contract ADR-0026 point 7
    /// places on the client ("the buffer must stay mapped and untouched until
    /// the 200 arrives") propagated one level down.
    ///
    /// # Errors
    ///
    /// Every rail's completion pump being dead (nothing to post from), or
    /// `ibv_reg_mr` failing — out of `RLIMIT_MEMLOCK`, or a segment the kernel
    /// will not pin. Both mean "deliver without RDMA": the caller falls back to
    /// fetching normally and copying into the client's window, which is a slower
    /// delivery, not a failed read.
    pub unsafe fn register_client_target(
        &self,
        ptr: *mut u8,
        len: usize,
    ) -> anyhow::Result<ClientTarget> {
        let rail = self.pick_healthy_rail()?;
        // SAFETY: forwarded from this function's own contract — the caller
        // guarantees `ptr`/`len` name a live mapping that outlives the returned
        // target, which is `register_from_raw`'s requirement.
        let mr = self.rails[rail]
            .ctx
            .pd()
            .register_from_raw(ptr, len, efa_access_flags())
            .with_context(|| format!("registering a {len}-byte client target (ADR-0026)"))?;
        Ok(ClientTarget {
            base_addr: ptr as u64,
            mr,
            rail,
            len,
        })
    }

    /// Round-robin among rails whose completion plane is alive.
    ///
    /// A `register_client_dmabuf` counterpart lived beside
    /// [`Self::register_client_target`] until ADR-0027's mechanism was removed
    /// (2026-09-08). Nothing here registers device memory any more: the daemon can only
    /// export a dma-buf for an allocation it *owns*, and ADR-0030 moved that to the
    /// client — which registers on its own NIC and sends a token. Device memory
    /// therefore reaches a holder as a `TokenWindow`, never as a [`ClientTarget`].
    ///
    /// # Errors
    ///
    /// Every rail's pump being dead: there is nothing to post a WRITE from, so the
    /// caller must deliver without RDMA.
    pub(super) fn pick_healthy_rail(&self) -> anyhow::Result<usize> {
        let healthy: Vec<usize> = self
            .rails
            .iter()
            .enumerate()
            .filter(|(_, rail)| rail.ctx.rdma_healthy.load(Ordering::Relaxed))
            .map(|(i, _)| i)
            .collect();
        if healthy.is_empty() {
            anyhow::bail!("no rail with a live completion plane to register a client target on");
        }
        let pick = self.next_rail.fetch_add(1, Ordering::Relaxed) as usize % healthy.len();
        Ok(healthy[pick])
    }

    /// Fetch `cache_key` from `peer` into `target[at .. at + len]`.
    ///
    /// The whole chunk, never a slice: a holder-driven WRITE moves the entire
    /// cached body (ADR-0018), so the caller must only offer windows that are a
    /// chunk's full extent (`ChunkWindow::whole`). A partial edge chunk is
    /// fetched the ordinary way and copied.
    ///
    /// # Errors
    ///
    /// [`TransportError`] exactly as [`crate::PeerTransport::fetch_blob`] does:
    /// [`TransportError::NotCached`] is the peer's protocol answer (try another
    /// source), and any transport failure evicts the peer's now-suspect AH,
    /// kicks off a proactive re-handshake, and returns — for the caller to
    /// retry over gRPC or the backend. A holder that merely declined to WRITE is
    /// NOT an error: it comes back as [`ChunkDelivery::Streamed`].
    pub async fn fetch_chunk_into(
        &self,
        peer: &NodeId,
        cache_key: &str,
        target: &ClientTarget,
        at: usize,
        len: usize,
        no_fill: bool,
    ) -> Result<ChunkDelivery, TransportError> {
        let remote = target.remote_at(at, len);
        let request = FetchBlobRequest {
            cache_key: cache_key.to_owned(),
            range_start: None,
            range_end: None,
            suffix_len: None,
            no_fill,
            rdma_buffer: Some(RdmaBuffer {
                addr: remote.addr,
                rkey: remote.rkey,
                len: remote.len as u64,
                rail: target.rail as u32,
            }),
            requester_node_id: Some(self.local_node.clone()),
            // This path offers memory THIS node registered; the client-token path offers
            // memory the client registered. Never both (see the proto's own contract).
            client_token: None,
        };
        let mut stream = match call_fetch_blob(&self.grpc, peer, request).await {
            Ok(stream) => stream,
            Err(e) => {
                return Err(self
                    .on_rdma_transport_error(peer, Some(target.rail), e)
                    .await)
            }
        };
        let (meta, first_data) = match read_first_chunk(&mut stream).await {
            Ok(chunk) => chunk,
            Err(e) => {
                return Err(self
                    .on_rdma_transport_error(peer, Some(target.rail), e)
                    .await)
            }
        };
        if !meta.served_via_rdma {
            return decode_stream_fallback(meta, first_data, stream)
                .await
                .map(ChunkDelivery::Streamed);
        }
        // The holder already refuses a body larger than the offered window, so
        // this can only fire if the two ends' geometry disagrees — treat it as a
        // transport error (fall back and re-fetch) rather than trusting a length
        // that would mean bytes landed outside the client's window.
        if meta.total_len > len as u64 {
            return Err(TransportError::Other(anyhow::anyhow!(
                "holder reported {} B written into a {len} B client window",
                meta.total_len
            )));
        }
        Ok(ChunkDelivery::Landed {
            bytes: meta.total_len,
            // The daemon registered this window, so `run_delivery` digests it by reading it
            // back — a stronger check than anything the holder could report.
            crc32: None,
        })
    }

    /// Fetch `cache_key` from `peer` **straight into the client's own registered window**
    /// (ADR-0030 point 4, planning/19 C3).
    ///
    /// The difference from [`Self::fetch_chunk_into`] is which memory the holder is offered.
    /// There, this node registered the client's window on one of its own rails and handed the
    /// holder an `RdmaBuffer` — so the holder's WRITE is bound to that rail's PD and the
    /// registration is this node's to pay for. Here the **client** registered its window, on
    /// every rail it wants written to, and what crosses is the token it published: the holder
    /// picks its own source rail, spreads over the client's destination rails, and this node
    /// neither registers nor touches a byte.
    ///
    /// That is the hop C3 exists to remove. Before this, a remote chunk destined for a
    /// client-registered window landed in *this* node's memory first and was then written on
    /// (ADR-0030 point 4's "a remote chunk is not a dead end") — correct, but two hops and
    /// bounded by this one node's rails. Now N holders write one client buffer at once, which
    /// is the only shape in which a single read can exceed one node's fabric.
    ///
    /// A holder that cannot write — no announce, no address handle, geometry disagreement —
    /// streams the body instead ([`ChunkDelivery::Streamed`]) and the caller writes it from
    /// here, i.e. the old two-hop path is exactly the fallback. Nothing about this can fail a
    /// read that would otherwise have succeeded.
    ///
    /// # Errors
    ///
    /// [`TransportError`] exactly as [`Self::fetch_chunk_into`] does:
    /// [`TransportError::NotCached`] is the peer's protocol answer (try another source), a
    /// transport failure evicts the peer's now-suspect AH and returns for the caller to retry
    /// over gRPC or the backend, and a holder that merely declined to WRITE is not an error.
    /// Also errors when the sub-window does not fit what the client registered, which is a
    /// caller bug rather than a runtime condition — refused rather than clamped, because a
    /// clamped delivery is a short read the client has no way to notice.
    pub async fn fetch_chunk_into_token(
        &self,
        peer: &NodeId,
        cache_key: &str,
        dest: TokenDestination<'_>,
        no_fill: bool,
    ) -> Result<ChunkDelivery, TransportError> {
        let token = dest
            .window
            .proto_at(dest.at, dest.len, dest.checksum)
            .ok_or_else(|| {
                TransportError::Other(anyhow::anyhow!(
                    "chunk window [{}, {}) is not inside the {}-byte client-registered window",
                    dest.at,
                    dest.at.saturating_add(dest.len),
                    dest.window.len(),
                ))
            })?;
        let request = FetchBlobRequest {
            cache_key: cache_key.to_owned(),
            range_start: None,
            range_end: None,
            suffix_len: None,
            no_fill,
            // Mutually exclusive with `client_token` by the proto's own contract: the two
            // name different destinations for the same bytes.
            rdma_buffer: None,
            requester_node_id: Some(self.local_node.clone()),
            client_token: Some(token),
        };
        // `None`, not a rail index: this node leased nothing and posted nothing, so no rail of
        // ours is implicated by a failure here (see `on_rdma_transport_error`).
        let mut stream = match call_fetch_blob(&self.grpc, peer, request).await {
            Ok(stream) => stream,
            Err(e) => return Err(self.on_rdma_transport_error(peer, None, e).await),
        };
        let (meta, first_data) = match read_first_chunk(&mut stream).await {
            Ok(chunk) => chunk,
            Err(e) => return Err(self.on_rdma_transport_error(peer, None, e).await),
        };
        if !meta.served_via_rdma {
            return decode_stream_fallback(meta, first_data, stream)
                .await
                .map(ChunkDelivery::Streamed);
        }
        // The holder refuses a body larger than the offered window (`TokenWindow::slice_on`),
        // so this can only fire if the two ends' geometry disagrees — a transport error
        // rather than trusting a length that would mean bytes landed outside the client's
        // window, or that the delivery digest will be folded over the wrong extent.
        if meta.total_len > dest.len as u64 {
            return Err(TransportError::Other(anyhow::anyhow!(
                "holder reported {} B written into a {} B client-registered window",
                meta.total_len,
                dest.len,
            )));
        }
        // A requested checksum that did not come back is a protocol violation, not a
        // missing optimisation: the delivery digest folds per window, so a hole in it would
        // hand the client a checksum computed over fewer bytes than were delivered — which
        // verifies, and is exactly the silent corruption ADR-0030 point 3 refuses to make
        // expressible. Fall back rather than deliver an unverifiable window.
        if dest.checksum && meta.written_crc32.is_none() {
            return Err(TransportError::Other(anyhow::anyhow!(
                "holder wrote {} B into client memory but reported no CRC32, and one was asked for",
                meta.total_len,
            )));
        }
        Ok(ChunkDelivery::Landed {
            bytes: meta.total_len,
            crc32: meta.written_crc32,
        })
    }
}

/// One chunk's destination inside a client-registered window.
///
/// A struct rather than four more arguments to [`EfaRdmaTransport::fetch_chunk_into_token`]:
/// the three range fields are only meaningful together, and the wire encoding stays in this
/// crate rather than leaking `pacer_proto` into the daemon's read path.
pub struct TokenDestination<'a> {
    /// The client's window, as its `x-pacer-target` header named it.
    pub window: &'a TokenWindow,
    /// Offset into that window this chunk lands at.
    pub at: usize,
    /// How many bytes this chunk may write. A holder whose cached body is larger declines
    /// and streams instead.
    pub len: usize,
    /// Whether the holder must report a CRC32 of what it wrote. Off when the client opted
    /// out of verification, because the cost is O(bytes) on the holder's CPU.
    pub checksum: bool,
}
