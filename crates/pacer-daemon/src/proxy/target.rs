//! The client memory one read delivers into, and how this node comes to hold it.
//!
//! ADR-0026 point 1 (a target descriptor names memory the client owns), ADR-0027
//! (that memory may be a GPU allocation, imported over CUDA IPC) and ADR-0030
//! (the client may instead register the memory itself and name it with a NIC
//! token). Opening it is where those three differ; everything above
//! [`ClientMemory`] is written against the funnel in `super::place`, so the
//! difference lives in exactly one place.
//!
//! **The invariant this module owns: whose fault a refusal is.** A descriptor this
//! node cannot honour degrades to a body-delivered read
//! ([`TargetRejection::Unsupported`] — our limitation, ADR-0026 point 8), while a
//! descriptor that is malformed or names memory that cannot back it is the
//! client's bug and answers `InvalidRequest`. Both go through
//! [`PacerProxy::target_rejected`] so the metric label and the HTTP answer cannot
//! disagree.

use std::sync::Arc;

use s3s::{dto, s3_error, S3Response, S3Result};
use tracing::warn;

use crate::delivery::{DeliveryTarget, MappedTarget, TargetMemory, TargetRejection, TargetSpec};
#[cfg(feature = "efa")]
use crate::metrics::Metrics;

#[cfg(feature = "efa")]
use super::cluster::Cluster;
use super::PacerProxy;

/// What a client's registered window is, when the RDMA plane is up: holders
/// WRITE cached chunks straight into it (ADR-0026 point 3).
///
/// `efa`-only, and so is everything that names it. A gRPC-only build has no plane to write
/// client memory with, so nothing registers it and nothing offers it: the whole
/// holder-writes-into-the-client family is stubbed out at ONE seam
/// ([`FillCtx::holder_writes_client`]) and delivery still happens, just with one copy on this
/// node (ADR-0026 point 4: only the remote tier is RDMA). It used to be an uninhabited alias
/// so each function could keep a featureless twin; consolidating the stubs made those twins
/// dead code, which the featureless clippy pass rightly refuses.
#[cfg(feature = "efa")]
pub(super) type ClientRegistration = pacer_transport::efa::ClientTarget;

/// The client memory one request delivers into, in whichever of the two registration
/// models the descriptor named.
///
/// The arms are not two flavours of the same thing: under [`Self::Mapped`] the daemon holds
/// the memory (it mapped a segment, or imported a handle) and can `memcpy` and digest it,
/// while under [`Self::Token`] the **client** registered it and the daemon holds an rkey and
/// nothing else — so placement is a WRITE, and the digest has to come from the source bytes
/// (ADR-0030 point 7). Everything above this type is written against the funnel
/// [`Proxy::place_window`], so that difference lives in exactly one place.
pub(super) enum ClientMemory {
    /// A client's mapped window plus its RDMA registration, **created on the first
    /// chunk that actually needs an rkey** rather than once per request.
    ///
    /// Why lazily: ADR-0026 point 4 makes only the REMOTE tier one-sided — a local cache
    /// hit is a `memcpy` and a backend read-through is a copy, and neither needs a
    /// registration at all. Registering up front therefore pins the whole window
    /// unconditionally, and `ibv_reg_mr` runs at ~3.6 GB/s, so a 4 GiB window costs
    /// ~1.19 s. In the 2026-08-21 arms (single node, every chunk local) that was **~47 %
    /// of every client thread's time spent pinning memory no peer ever wrote into** —
    /// pure waste, and invisible until the daemon's own registration timer was read.
    ///
    /// A warm checkpoint restore is exactly that case: the node serves its own cached
    /// chunks, so the fast path now pays nothing. A read that does reach a peer pays the
    /// registration once, on the first such chunk, and every later window in the same
    /// request reuses it — which is also why this is a `OnceCell` rather than a flag: the
    /// windows resolve concurrently, and two of them must not race into two
    /// registrations of the same memory.
    /// ADR-0026/0027: the daemon mapped or imported the client's memory.
    Mapped {
        /// The mapped window (owns the segment for the request's lifetime).
        target: Arc<DeliveryTarget>,
        /// `None` inside means "we tried and could not register" — a real possibility
        /// (no RDMA plane, `RLIMIT_MEMLOCK`), and one that must be remembered rather
        /// than retried per window.
        ///
        /// Absent entirely on a gRPC-only build: there is nothing that could register,
        /// so carrying an always-empty cell would be dead weight the compiler is right
        /// to complain about.
        #[cfg(feature = "efa")]
        registration: tokio::sync::OnceCell<Option<Arc<ClientRegistration>>>,
    },
    /// ADR-0030: the client registered its own memory and named it with a NIC token.
    ///
    /// Nothing to map, nothing to register, nothing to read back. Only reachable on an
    /// `efa` build, because without the RDMA plane there is no way to write into it at all
    /// — `open_client_memory` degrades instead (ADR-0026 point 8).
    #[cfg(feature = "efa")]
    Token {
        /// The window the client registered, narrowed to this request's range.
        window: pacer_transport::token::TokenWindow,
        /// Whether the client wants its delivery verified.
        ///
        /// The flag lives here, and only on this arm, because this is the only destination
        /// whose digest must be *requested from another node*: under the remote half a
        /// holder writes the bytes and reports their CRC32 (ADR-0030 point 7), and
        /// verification is O(bytes) on that holder's CPU — so `checksum=none` has to reach
        /// it. Every other arm digests where the flag is already in scope.
        checksum: bool,
    },
}

impl ClientMemory {
    /// Wrap a mapped window with no registration yet.
    fn mapped(target: Arc<DeliveryTarget>) -> Self {
        Self::Mapped {
            target,
            #[cfg(feature = "efa")]
            registration: tokio::sync::OnceCell::new(),
        }
    }

    /// Bytes the client offered.
    pub(super) fn window_len(&self) -> usize {
        match self {
            Self::Mapped { target, .. } => target.window_len(),
            #[cfg(feature = "efa")]
            Self::Token { window, .. } => window.len(),
        }
    }

    /// An owned handle to the window. Owned rather than borrowed because every use —
    /// the copy and the digest — now runs on the blocking pool, and
    /// `spawn_blocking` needs `'static + Send` (which is exactly why `MappedTarget`
    /// carries its unsafe `Send + Sync` impls).
    pub(super) fn mapped_target(&self) -> Option<Arc<DeliveryTarget>> {
        match self {
            Self::Mapped { target, .. } => Some(Arc::clone(target)),
            #[cfg(feature = "efa")]
            Self::Token { .. } => None,
        }
    }

    /// The window's RDMA registration, created on first call and reused after — the
    /// whole point of this type. Timed into `pacer_delivery_register_seconds_total`
    /// and counted into `pacer_delivery_registrations_total`, so "registrations per
    /// delivered request" is readable at scrape: a warm, locally-served restore should
    /// now show **zero**.
    ///
    /// `None` when this node has no RDMA plane or when registration failed — the
    /// caller then streams and copies, which is slower but correct. The failure is
    /// remembered (the `OnceCell` stores that `None`), so a 256-window request cannot
    /// retry a doomed `ibv_reg_mr` 256 times.
    #[cfg(feature = "efa")]
    pub(super) async fn registration_for(
        &self,
        cluster: &Cluster,
        metrics: &Metrics,
    ) -> Option<&Arc<ClientRegistration>> {
        // A token target has nothing to register — that is the point of ADR-0030 — so it
        // never takes the holder-writes-into-a-registration path. It takes the OTHER one:
        // `rdma_into_client_token` hands the holder the client's own token
        // (`FetchBlobRequest.client_token`) and the holder writes there directly. With that
        // off (`delivery.remote_write`, the control arm) its remote chunks arrive in this
        // node's memory and are then WRITTEN on — see `place_window`.
        let (target, registration) = match self {
            Self::Mapped {
                target,
                registration,
            } => (target, registration),
            #[cfg(feature = "efa")]
            Self::Token { .. } => return None,
        };
        registration
            .get_or_init(|| async {
                let efa = cluster.efa.as_ref()?;
                let started = std::time::Instant::now();
                let len = target.window_len();
                // An address range, because the only memory the daemon maps is host
                // memory. ADR-0027's GPU arm registered a dma-buf here instead; it was
                // removed with the rest of that mechanism, since the daemon can never
                // export one for a pointer it imported. Device memory now arrives as a
                // `nic:` token the CLIENT registered, which reaches none of this.
                //
                // SAFETY: the target is alive for the whole request — this `Arc`
                // outlives the registration, which drops with the response — the window
                // is `len` bytes, and no second target names it, since one
                // `ClientMemory` exists per request over memory that request opened.
                let registered = unsafe { efa.register_client_target(target.host_window_ptr(), len) };
                metrics
                    .delivery
                    .register_seconds
                    .inc_by(started.elapsed().as_secs_f64());
                metrics.delivery.registrations.inc();
                match registered {
                    Ok(client_target) => Some(Arc::new(client_target)),
                    Err(e) => {
                        warn!(error = %e, bytes = target.window_len(),
                            "registering client target failed; delivering through the daemon instead");
                        None
                    }
                }
            })
            .await
            .as_ref()
    }

    // No gRPC-only counterpart to `registration_for`: without the RDMA plane nothing can
    // offer a registration to anyone, so `FillCtx::holder_writes_client` is stubbed out
    // instead and this is never reached. One featureless stub at the seam, rather than one
    // per function on the way to it.
}

impl PacerProxy {
    /// Open whichever client memory the descriptor names.
    ///
    /// The two arms differ in *whose fault* a failure is, which is why they return
    /// different rejections. A host segment that cannot be mapped is the client's
    /// problem (`Unusable`, a 4xx). A `nic:` token on a daemon built without the RDMA
    /// plane is OUR limitation — so it is `Unsupported`, which degrades to a
    /// body-delivered read rather than telling a loader that its correct header is
    /// wrong (ADR-0026 point 8).
    pub(super) fn open_client_memory(
        &self,
        spec: &TargetSpec,
    ) -> Result<ClientMemory, TargetRejection> {
        match &spec.memory {
            TargetMemory::Shm { .. } => Ok(ClientMemory::mapped(Arc::new(DeliveryTarget::Host(
                MappedTarget::open(&self.delivery, spec, &self.delivery_quota)?,
            )))),
            // ADR-0030: nothing to open. The client registered this memory itself, so there
            // is no segment to map, no handle to import, and no pinned bytes to charge to
            // the quota — the window is named, not held. All this does is fold the request's
            // offset into it and check that something can address it.
            #[cfg(feature = "efa")]
            TargetMemory::Nic { base_addr, rails } => pacer_transport::token::TokenWindow::new(
                *base_addr,
                spec.offset,
                usize::try_from(spec.len).map_err(|_| {
                    TargetRejection::Malformed(format!("len {} exceeds this platform", spec.len))
                })?,
                rails.clone(),
            )
            .map(|window| ClientMemory::Token {
                window,
                checksum: spec.checksum,
            })
            .map_err(|e| TargetRejection::Malformed(e.to_string())),
            #[cfg(not(feature = "efa"))]
            TargetMemory::Nic { .. } => Err(TargetRejection::Unsupported(
                "client-registered memory (this daemon was built without the RDMA plane)"
                    .to_owned(),
            )),
        }
    }

    /// Turn a rejected target into either "serve the body" or a client error —
    /// one place, so the metric label and the HTTP answer can never disagree
    /// (ADR-0026 point 8: over-quota degrades, it never fails a read).
    ///
    /// # Errors
    ///
    /// `InvalidRequest` for a non-degradable rejection (malformed or unusable
    /// descriptor).
    pub(super) fn target_rejected(
        &self,
        rejection: TargetRejection,
    ) -> S3Result<Option<S3Response<dto::GetObjectOutput>>> {
        self.metrics
            .delivery
            .rejects
            .with_label_values(&[rejection.reason()])
            .inc();
        if rejection.is_degradable() {
            warn!(error = %rejection, "client-memory delivery declined; serving the body instead");
            return Ok(None);
        }
        Err(s3_error!(InvalidRequest, "{}", rejection))
    }
}
