//! Client-supplied target memory (ADR-0026, planning/19 Track C C1): a
//! cooperating S3 client names a POSIX shared-memory segment it owns, and the
//! daemon delivers the object's bytes into that segment instead of into the
//! response body.
//!
//! Why this exists: D5.3 measured the client-delivery leg — hyper's `sendmsg`,
//! TCP/IP segmentation, a veth pair, the client's `recvmsg` — at 31.589 GiB/s
//! while the full paired arm reached 27.502, i.e. **87 % of the daemon's
//! throughput goes into handing bytes to a local S3 client**. Track D, H and N
//! all make bytes arrive faster at a place the consumer must then copy out of;
//! this module removes that copy rather than shaving it.
//!
//! This file is the host-memory half: the descriptor a client sends, the
//! mapping of the segment it named, the quota that bounds how much client
//! memory a node will pin, and the chunk→window arithmetic that says which
//! bytes of the object land where. What it deliberately does NOT contain is any
//! RDMA: a remote chunk's WRITE destination is registered by the transport
//! (`pacer_transport::efa::EfaRdmaTransport::register_client_target`) from the
//! address this module exposes, and the proxy's read path is what drives both.
//!
//! Three invariants from the ADR live here:
//!
//! - **Opt-in by request header.** A client that does not send
//!   [`TARGET_HEADER`] gets today's behaviour byte for byte; the header-only
//!   200 is reachable no other way. That gate is what makes the compatibility
//!   claim structural rather than aspirational, and it is asserted by a test
//!   that drives a stock GET (`proxy::tests`).
//! - **Integrity moves into a header.** With no body there is no SDK checksum,
//!   so the response carries [`CHECKSUM_HEADER`] over the delivered bytes and
//!   the shim verifies it (ADR-0026 point 5).
//! - **Pinned client memory is quota'd, and over-quota degrades.** Registration
//!   pins pages, so an unbounded client could pin the node; an over-quota
//!   target falls back to a body-delivered read ([`TargetRejection::OverQuota`]),
//!   never a failed one (ADR-0026 point 8).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pacer_cache::chunk::ChunkConfig;
// The rail type lives in the transport crate: it is RDMA-shaped, and both this parser and
// the WRITE path that consumes it have to name it (ADR-0030 point 1).
use pacer_transport::token::TokenRail;

/// Request header a client sends to name its target memory. `x-pacer-*` rather
/// than `x-amz-*` on purpose: this is our extension, not S3 semantics, and it
/// must be visibly ours (ADR-0026 "it is a proprietary extension").
pub const TARGET_HEADER: &str = "x-pacer-target";
/// Response header carrying how many bytes were delivered into the client's
/// memory. Its PRESENCE is the protocol's completion signal: a client that
/// asked for delivery and does not see it must read the body instead, which is
/// exactly what makes the quota fallback safe.
pub const DELIVERED_HEADER: &str = "x-pacer-delivered";
/// Response header carrying `"{algorithm}={hex}"` over the delivered bytes.
pub const CHECKSUM_HEADER: &str = "x-pacer-checksum";

/// Checksum algorithm named in [`CHECKSUM_HEADER`].
///
/// CRC32 (IEEE, the `zlib`/S3 flavour) rather than the xxh3-128 this repo's
/// seed/verify tools use, for one reason that only applies to this leg: the
/// checksum the empty body removes IS the SDK's CRC32, and a client shim must
/// be able to re-check it with no new dependency — `zlib.crc32` is in Python's
/// standard library, `xxh3` is a pip install. Same guarantee, reachable from
/// any loader.
pub const CHECKSUM_ALGORITHM: &str = "crc32";

/// Scheme prefix of a host-memory target descriptor. Parsing rejects any scheme
/// it does not know, so an unsupported one can never be silently treated as shm.
const SHM_SCHEME: &str = "shm:";
/// Scheme prefix of a **NIC-token** target descriptor (ADR-0030): memory the client
/// registered itself, named by the rkeys and endpoints its own NIC issued.
///
/// **This is the only scheme that reaches GPU memory**, and the reason ADR-0027's
/// `cuda-ipc:` one was removed rather than kept beside it: only the owning process can
/// dma-buf export a `cudaMalloc`-backed tensor, and dma-buf is the only way to register
/// device memory on EFA, so a daemon holding a client's IPC handle could never register
/// it (`bench/ladder/results/c2-vmm-dmabuf-provenance.md`). Here the client registers and
/// the daemon only WRITEs, which is also why the daemon needs no CUDA at all.
const NIC_SCHEME: &str = "nic:";
/// Parameter carrying the client's per-rail endpoints and keys (ADR-0030 point 1).
///
/// A **list**, not one tuple: an rkey is scoped to the protection domain that issued it
/// and a PD belongs to one device, so a window the client wants reachable from several of
/// its own rails is registered once per rail and named once per rail here. The writer
/// picks the entry whose rail is PCIe-local to the GPU holding the tensor.
const RAILS_PARAM: &str = "rails";
/// Separator between rail entries in [`RAILS_PARAM`].
const RAIL_SEPARATOR: char = ',';
/// Separator between the fields of one rail entry: `<gid>/<qpn>/<rkey>`.
const RAIL_FIELD_SEPARATOR: char = '/';
/// Fields per rail entry — GID, QPN, rkey.
const RAIL_FIELDS: usize = 3;
/// Hex characters in a GID: 16 bytes, two characters each, no separators, so a
/// truncated or padded GID is a parse error rather than a silently different address.
const GID_HEX_CHARS: usize = 32;
/// Most rails one token may name.
///
/// Tied to the announce's limit rather than chosen separately: the two are the same
/// question asked in opposite directions (how many rails may one side name at once), and
/// a token a writer cannot answer with a single announce would be a protocol trap.
const MAX_TOKEN_RAILS: usize = pacer_transport::announce::MAX_ANNOUNCED_RAILS;
/// Separator between the descriptor's segment name and its parameters.
const PARAM_SEPARATOR: char = ';';
/// Parameter naming the byte offset into the segment the object starts at.
const OFFSET_PARAM: &str = "offset";
/// Parameter naming how many bytes of the segment the daemon may write.
const LEN_PARAM: &str = "len";
/// Parameter selecting the integrity check (`crc32`, the default, or `none`).
///
/// Opt-out exists because verification is **O(delivered bytes)**: a checkpoint
/// loader naming a 100 GB window would otherwise pay a full extra pass over
/// 100 GB — read back out of client memory, which on the RDMA path the daemon
/// never touched — before the 200 is sent. A client that verifies its own bytes
/// (or trusts the fabric's own CRCs for a restore it will immediately
/// deserialize) can say so; the default stays on, because a delivery that cannot
/// be checked is a delivery that cannot be trusted (ADR-0026 point 5).
const CHECKSUM_PARAM: &str = "checksum";
/// Value of [`CHECKSUM_PARAM`] that turns verification off.
const CHECKSUM_NONE: &str = "none";
/// Prefix marking a hexadecimal parameter value (`offset=0x40000`); decimal is
/// accepted too, since a client computing offsets in Python gets decimal for
/// free.
const HEX_PREFIX: &str = "0x";
/// Radix of a [`HEX_PREFIX`]-prefixed value.
const HEX_RADIX: u32 = 16;

/// Where POSIX shared-memory segments appear as files. On Linux
/// `shm_open("/name")` IS `open("/dev/shm/name")`, so mapping the path is the
/// same operation without linking `shm_open` — and it makes the directory a
/// knob, which is what lets a pod share a tmpfs with a loader through a plain
/// `emptyDir{medium: Memory}` mount and lets tests point at a temp dir.
pub const DEFAULT_SHM_DIR: &str = "/dev/shm";

/// Default per-request ceiling on client memory a single GET may pin (1 GiB).
/// Sized for a checkpoint shard read in one call; larger reads are still served
/// (body-delivered), so this bounds a client's mistake rather than its
/// workload.
pub const DEFAULT_MAX_TARGET_BYTES: u64 = 1 << 30;
/// Default delivery fan-out: windows filled concurrently for ONE request.
///
/// 64, not the body path's `fill_parallelism` (8), and the difference is
/// structural rather than a tuning preference. The body path emits bytes to the
/// client *in order*, so a deeper look-ahead than its reorder window buys
/// nothing. A delivery's windows are disjoint destinations in the client's own
/// buffer with no ordering constraint at all — and the shape that matters is ONE
/// GET for a multi-GiB checkpoint, i.e. thousands of windows, where a bound of 8
/// turns a fabric-limited transfer into ~N/8 serial rounds.
///
/// The real bounds are elsewhere and unchanged: the holder's serve admission, the
/// per-rail write window, and the arena. This only stops one request from
/// monopolising them.
pub const DEFAULT_DELIVERY_PARALLELISM: usize = 64;
/// Default node-wide ceiling on concurrently pinned client memory (8 GiB).
/// Independent of the RDMA arena's own budget (`PACER_RDMA_ARENA_BYTES`)
/// because these pages belong to clients, not to the daemon: both must fit the
/// node, and only this one grows with how many loaders are running.
pub const DEFAULT_PINNED_BYTES_MAX: u64 = 8 << 30;

/// Operator settings for client-memory delivery (ADR-0026).
#[derive(Debug, Clone)]
pub struct DeliveryConfig {
    /// Whether the daemon honours [`TARGET_HEADER`] at all. **Off by default**:
    /// delivery maps and pins memory a client named, which is a new privilege
    /// surface, so it is enabled deliberately (chart `delivery.enabled`) rather
    /// than by anyone who happens to send a header. Off, a target header is
    /// ignored and the read is served normally.
    pub enabled: bool,
    /// Directory the segment name in a `shm:` descriptor resolves under (see
    /// [`DEFAULT_SHM_DIR`]).
    pub shm_dir: PathBuf,
    /// Per-request ceiling on pinned client bytes (see
    /// [`DEFAULT_MAX_TARGET_BYTES`]).
    pub max_target_bytes: u64,
    /// Node-wide ceiling on concurrently pinned client bytes (see
    /// [`DEFAULT_PINNED_BYTES_MAX`]).
    pub pinned_bytes_max: u64,
    /// Windows delivered concurrently per request (see
    /// [`DEFAULT_DELIVERY_PARALLELISM`]).
    pub parallelism: usize,
    /// Whether a remote chunk destined for a **client-registered** window
    /// (`nic:`, ADR-0030) is written by its HOLDER directly, instead of arriving
    /// in this node's memory and being written on from here (planning/19 C3,
    /// "parallel fill").
    ///
    /// **Off by default, and the reason is measurement rather than doubt.** The
    /// two paths deliver the same bytes with the same integrity guarantee — a
    /// holder that cannot write streams instead, which is exactly the old path —
    /// so this is not a safety valve. It is the control arm: the hop it removes is
    /// the one C3 exists to remove, and "how much is that hop worth" has no answer
    /// unless both arms can be run on one image. It also bounds the one real cost,
    /// which is that **every holder** then pays first contact per client endpoint
    /// (its own announce, its own address handles) where before only the reading
    /// node did.
    ///
    /// Ignored on the mapped scheme (`shm:`): there the daemon registered the window
    /// itself and the holder has always written into it directly (ADR-0026 point 3), so
    /// there is no hop left to remove.
    pub remote_write: bool,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            shm_dir: PathBuf::from(DEFAULT_SHM_DIR),
            max_target_bytes: DEFAULT_MAX_TARGET_BYTES,
            pinned_bytes_max: DEFAULT_PINNED_BYTES_MAX,
            parallelism: DEFAULT_DELIVERY_PARALLELISM,
            remote_write: false,
        }
    }
}

/// Which memory a target names, and whatever it takes to find it.
///
/// The window (`offset`/`len`), the integrity rule and the lifetime contract are identical
/// across the arms — a scheme changes *how the memory is named and who registered it*,
/// nothing else — so they share [`TargetSpec`] and differ only here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetMemory {
    /// POSIX shared memory the client owns, named by segment (ADR-0026).
    Shm {
        /// Segment name, a single path component (validated — see
        /// [`TargetSpec::parse`]).
        name: String,
    },
    /// Memory the client registered itself, named by a NIC token (ADR-0030).
    ///
    /// Unlike [`TargetMemory::Shm`], nothing here is mappable by the daemon: there is no
    /// segment to open, so a local hit is a **loopback WRITE** (proven in
    /// `bench/ladder/results/c2-loopback-gate.md`) rather than a memcpy, and the
    /// delivery's integrity check cannot read the window back — it has to be computed on
    /// the side that holds the bytes. That is why this arm does not reach
    /// [`DeliveryTarget`], which is written around `copy_in`/`digest_window`.
    Nic {
        /// First byte of the window the client registered, as its own process sees it.
        /// `offset`/`len` on the [`TargetSpec`] then name a sub-window of it, exactly as
        /// they do for a segment.
        base_addr: u64,
        /// One entry per client rail; never empty (the parser rejects that).
        rails: Vec<TokenRail>,
    },
}

impl TargetMemory {
    /// Short, stable label for logs and the delivery metrics' `target` dimension
    /// — so a run can be read as "how much went to HBM" without inferring it.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            TargetMemory::Shm { .. } => "shm",
            TargetMemory::Nic { .. } => "nic",
        }
    }
}

/// The client memory one request delivers into: a mapped host window (ADR-0026).
///
/// A one-arm enum rather than [`MappedTarget`] directly, and deliberately so: it is the
/// seam a second *mappable* scheme would arrive at, and it is what keeps `run_delivery`
/// from growing a branch per target kind. Memory the daemon does NOT map — a `nic:`
/// token — never reaches here at all; see [`TargetMemory::Nic`].
///
/// ADR-0027's `cuda-ipc:` arm lived here until 2026-09-08. It was removed rather than
/// left in place because the daemon can never register an IPC-imported pointer with the
/// NIC (`bench/ladder/results/c2-vmm-dmabuf-provenance.md`), so its every method was
/// fallible for one arm that could only ever degrade to a two-hop copy.
pub enum DeliveryTarget {
    /// A mapped window of the client's POSIX shared memory (ADR-0026).
    Host(MappedTarget),
}

impl DeliveryTarget {
    /// Bytes of the client's memory this request may write.
    #[must_use]
    pub fn window_len(&self) -> usize {
        match self {
            DeliveryTarget::Host(t) => t.window_len(),
        }
    }

    /// Short, stable label for logs and metrics.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            DeliveryTarget::Host(_) => "shm",
        }
    }

    /// Copy `src` into the window at `at`.
    ///
    /// This is the *local* path — a cache hit, a read-through, or a peer that
    /// declined to WRITE. A remote chunk delivered by RDMA never comes through
    /// here: the holder's NIC writes the client's memory directly, which is the
    /// entire point of both ADRs.
    ///
    /// # Errors
    ///
    /// Nothing can fail today — the host arm is a memcpy into a mapping already
    /// bounds-checked at open time. The `Result` is the seam a mappable target that
    /// *can* refuse a copy would return through, and dropping it would make adding one
    /// a change to every caller.
    pub fn copy_in(&self, at: usize, src: &[u8]) -> Result<(), TargetRejection> {
        match self {
            DeliveryTarget::Host(t) => {
                t.copy_in(at, src);
                Ok(())
            }
        }
    }

    /// CRC32 of `[at, at + len)` of the window, as delivered.
    ///
    /// # Errors
    ///
    /// Nothing can fail today, for [`Self::copy_in`]'s reason.
    pub fn digest_window(&self, at: usize, len: usize) -> Result<DeliveryDigest, TargetRejection> {
        match self {
            DeliveryTarget::Host(t) => Ok(t.digest_window(at, len)),
        }
    }

    /// The host window's first byte, for the RDMA registration.
    ///
    /// Infallible, unlike the `Option` this returned while ADR-0027's GPU arm existed:
    /// every memory the daemon *maps* is host memory, and memory it does not map never
    /// becomes a [`DeliveryTarget`] at all.
    ///
    /// # Safety
    ///
    /// Forwards [`MappedTarget::window_ptr`]'s contract: nothing may read or write
    /// past [`Self::window_len`] bytes through it, and nothing derived from it may
    /// outlive this target — the mapping goes away on drop, and a peer DMA-writing
    /// into a stale pointer is a use-after-free with a NIC on the far end.
    #[must_use]
    pub unsafe fn host_window_ptr(&self) -> *mut u8 {
        match self {
            // SAFETY: forwarded to this function's own caller, unchanged.
            DeliveryTarget::Host(t) => unsafe { t.window_ptr() },
        }
    }
}

/// A parsed target descriptor: the memory a client named plus the window of it
/// the daemon may write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetSpec {
    /// Which memory this names, and how to reach it.
    pub memory: TargetMemory,
    /// Byte offset into the segment where the delivered bytes start.
    pub offset: u64,
    /// How many bytes of the segment the daemon may write.
    pub len: u64,
    /// Whether the response should carry a checksum of the delivered bytes.
    pub checksum: bool,
}

impl TargetSpec {
    /// The shm segment name, or `None` for memory the daemon does not map.
    /// Convenience for the host path, which is written against a name.
    #[must_use]
    pub fn shm_name(&self) -> Option<&str> {
        match &self.memory {
            TargetMemory::Shm { name } => Some(name),
            TargetMemory::Nic { .. } => None,
        }
    }
}

/// Why a target was not used. Split by *who* must act on it, because the two
/// halves get different HTTP answers: a malformed or unusable descriptor is the
/// client's bug (4xx), while over-quota is node pressure and must degrade to a
/// body-delivered read (ADR-0026 point 8).
#[derive(Debug, thiserror::Error)]
pub enum TargetRejection {
    /// The descriptor did not parse (unknown scheme, missing/undecodable
    /// parameter, unsafe segment name, zero length).
    #[error("malformed {TARGET_HEADER}: {0}")]
    Malformed(String),
    /// The descriptor parsed but the segment cannot back it (absent, not
    /// mappable, or smaller than `offset + len`).
    #[error("unusable target segment: {0}")]
    Unusable(String),
    /// A well-formed target this daemon cannot honour — today, a `nic:` token on a
    /// build without the RDMA plane, or one with no usable path to the client's
    /// window right now (ADR-0030).
    ///
    /// Degradable, and deliberately distinct from `Malformed`: the client did
    /// nothing wrong, so answering with a 4xx would tell a loader to fix a header
    /// that is correct. It gets the bytes in the body instead, and the metric's
    /// `reason` label says which capability was missing.
    #[error("this daemon cannot deliver into {0} memory")]
    Unsupported(String),
    /// Honouring the target would exceed a pinned-memory ceiling. NOT an error
    /// for the client: the caller serves the body instead.
    #[error("target would exceed the pinned-memory quota: {requested} B requested, {in_use} B of {max} B in use")]
    OverQuota {
        /// Bytes the descriptor asked to pin.
        requested: u64,
        /// Bytes already pinned node-wide.
        in_use: u64,
        /// The ceiling that was hit (per-request or node-wide).
        max: u64,
    },
}

impl TargetRejection {
    /// Whether this rejection means "serve the body instead" rather than "tell
    /// the client it got the request wrong". One predicate so the proxy's
    /// decision and the metric label can never disagree.
    #[must_use]
    pub fn is_degradable(&self) -> bool {
        matches!(
            self,
            TargetRejection::OverQuota { .. } | TargetRejection::Unsupported(_)
        )
    }

    /// Short, stable label for the `reason` dimension of the delivery
    /// rejection metric.
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            TargetRejection::Malformed(_) => "malformed",
            TargetRejection::Unusable(_) => "unusable",
            TargetRejection::Unsupported(_) => "unsupported",
            TargetRejection::OverQuota { .. } => "quota",
        }
    }
}

impl TargetSpec {
    /// Parse a descriptor of the form
    /// `shm:/<name>;offset=<n>;len=<n>` (offset may be `0x`-prefixed; both
    /// parameters are required, order-insensitive).
    ///
    /// The segment name must be a single path component: no separator, no `..`,
    /// no NUL, non-empty. That is the ADR's "the daemon must not accept a name
    /// it cannot attribute" reduced to what a filesystem-backed segment makes
    /// checkable — it keeps a descriptor from naming anything outside the
    /// configured shm directory.
    ///
    /// # Errors
    ///
    /// [`TargetRejection::Malformed`] for an unknown scheme, a missing or
    /// unparseable `offset`/`len`, an unsafe name, or `len == 0` (a zero-length
    /// window can deliver nothing, so it is a client bug rather than a no-op).
    pub fn parse(raw: &str) -> Result<Self, TargetRejection> {
        let raw = raw.trim();
        // Scheme first, then ONE parameter loop for both: the window, the integrity rule
        // and the lifetime are scheme-independent by design — a scheme changes how the
        // memory is named and who registered it, nothing else — so parsing them twice
        // would be two places for them to drift.
        let (body, scheme) = match (raw.strip_prefix(SHM_SCHEME), raw.strip_prefix(NIC_SCHEME)) {
            (Some(body), _) => (body, Scheme::Shm),
            (_, Some(body)) => (body, Scheme::Nic),
            _ => {
                return Err(malformed(format!(
                    "unsupported scheme in {raw:?} (expected {SHM_SCHEME}… or {NIC_SCHEME}…)"
                )))
            }
        };
        let mut parts = body.split(PARAM_SEPARATOR);
        let addr_or_name = parts.next().unwrap_or_default().trim();
        let (mut offset, mut len, mut rails) = (None, None, None);
        let mut checksum = true;
        for param in parts {
            let (key, value) = param
                .split_once('=')
                .ok_or_else(|| malformed(format!("parameter {param:?} is not key=value")))?;
            match key.trim() {
                OFFSET_PARAM => offset = Some(parse_u64(value)?),
                LEN_PARAM => len = Some(parse_u64(value)?),
                CHECKSUM_PARAM => checksum = parse_checksum(value)?,
                RAILS_PARAM => rails = Some(parse_token_rails(value)?),
                other => return Err(malformed(format!("unknown parameter {other:?}"))),
            }
        }
        let len = len.ok_or_else(|| malformed(format!("missing {LEN_PARAM}")))?;
        if len == 0 {
            return Err(malformed(format!("{LEN_PARAM} must be positive")));
        }
        // One check per cross-scheme parameter, stated once here rather than per arm: a
        // parameter that means nothing for the scheme it was sent with is a client bug
        // worth naming, not something to ignore.
        if !matches!(scheme, Scheme::Nic) && rails.is_some() {
            return Err(malformed(format!(
                "{RAILS_PARAM} is only meaningful for a {NIC_SCHEME} target"
            )));
        }
        let memory = if matches!(scheme, Scheme::Nic) {
            TargetMemory::Nic {
                base_addr: parse_u64(addr_or_name)?,
                rails: rails.ok_or_else(|| {
                    malformed(format!(
                        "a {NIC_SCHEME} target must name the rails it registered: \
                         {RAILS_PARAM}=<gid>/<qpn>/<rkey>[,…]"
                    ))
                })?,
            }
        } else {
            // POSIX spells segment names with a leading slash; the file under
            // the shm directory does not have one.
            let name = addr_or_name.trim_start_matches('/');
            validate_segment_name(name)?;
            TargetMemory::Shm {
                name: name.to_owned(),
            }
        };
        Ok(Self {
            memory,
            // An absent offset means "the start of the segment", the shape a
            // single-object-per-segment client uses.
            offset: offset.unwrap_or(0),
            len,
            checksum,
        })
    }

    /// The end of the window within the segment, or `None` on overflow (a
    /// descriptor whose `offset + len` wraps is malformed, not a huge segment).
    fn window_end(&self) -> Option<u64> {
        self.offset.checked_add(self.len)
    }
}

/// Which scheme a descriptor announced. An enum rather than a boolean because the
/// compiler then checks every arm is handled — which is what caught the cross-scheme
/// parameter rules when ADR-0027's third scheme was removed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Scheme {
    /// `shm:` — the daemon maps and registers the client's segment (ADR-0026).
    Shm,
    /// `nic:` — the client registered its own memory and named it (ADR-0030).
    Nic,
}

/// Parse [`RAILS_PARAM`]: `<gid>/<qpn>/<rkey>[,<gid>/<qpn>/<rkey>…]`.
///
/// # Errors
///
/// [`TargetRejection::Malformed`] for an empty list, more than [`MAX_TOKEN_RAILS`]
/// entries, an entry without exactly [`RAIL_FIELDS`] fields, a GID that is not
/// [`GID_HEX_CHARS`] hex characters, an unparseable QPN or rkey, or the same `(gid, qpn)`
/// twice — a repeat means the client published one rail under two keys, and picking either
/// would be a guess.
fn parse_token_rails(value: &str) -> Result<Vec<TokenRail>, TargetRejection> {
    let mut rails: Vec<TokenRail> = Vec::new();
    for entry in value.split(RAIL_SEPARATOR) {
        let entry = entry.trim();
        if entry.is_empty() {
            return Err(malformed(format!(
                "empty rail entry in {RAILS_PARAM}={value:?} (a trailing {RAIL_SEPARATOR:?}?)"
            )));
        }
        let fields: Vec<&str> = entry.split(RAIL_FIELD_SEPARATOR).collect();
        if fields.len() != RAIL_FIELDS {
            return Err(malformed(format!(
                "rail entry {entry:?} has {} field(s), expected {RAIL_FIELDS} \
                 (<gid>{RAIL_FIELD_SEPARATOR}<qpn>{RAIL_FIELD_SEPARATOR}<rkey>)",
                fields.len(),
            )));
        }
        let rail = TokenRail {
            gid: parse_gid(fields[0])?,
            qpn: parse_u32_field(fields[1], "qpn")?,
            rkey: parse_u32_field(fields[2], "rkey")?,
        };
        if rails.iter().any(|r| r.gid == rail.gid && r.qpn == rail.qpn) {
            return Err(malformed(format!(
                "rail {}/{} appears twice in {RAILS_PARAM}",
                fields[0], rail.qpn
            )));
        }
        rails.push(rail);
        if rails.len() > MAX_TOKEN_RAILS {
            return Err(malformed(format!(
                "{RAILS_PARAM} names more than the {MAX_TOKEN_RAILS} rails a token may carry"
            )));
        }
    }
    Ok(rails)
}

/// Parse a GID: exactly [`GID_HEX_CHARS`] hex characters, no `0x`, no separators.
///
/// Strict on length because a GID is an address: a short one padded on the wrong end
/// addresses a different device, and nothing downstream can tell.
///
/// # Errors
///
/// [`TargetRejection::Malformed`] on the wrong length or a non-hex character.
fn parse_gid(value: &str) -> Result<[u8; 16], TargetRejection> {
    if value.len() != GID_HEX_CHARS {
        return Err(malformed(format!(
            "gid {value:?} is {} characters, expected exactly {GID_HEX_CHARS} hex digits",
            value.len(),
        )));
    }
    let mut gid = [0u8; 16];
    // `as_chunks`, not `chunks_exact`: a hex pair is two characters by definition, so the
    // width is a constant and the pairs are sized at compile time. The length check above
    // already refused anything but exactly `GID_HEX_CHARS` digits, so the remainder this
    // discards is empty by construction.
    let (pairs, _) = value.as_bytes().as_chunks::<2>();
    for (byte, pair) in gid.iter_mut().zip(pairs) {
        let hex = std::str::from_utf8(pair)
            .map_err(|_| malformed(format!("gid {value:?} is not ASCII")))?;
        *byte = u8::from_str_radix(hex, 16)
            .map_err(|e| malformed(format!("gid {value:?} is not hex: {e}")))?;
    }
    Ok(gid)
}

/// Parse a `u32` rail field, decimal or `0x`-prefixed.
///
/// # Errors
///
/// [`TargetRejection::Malformed`] if it does not parse or does not fit.
fn parse_u32_field(value: &str, what: &str) -> Result<u32, TargetRejection> {
    let parsed = parse_u64(value)?;
    u32::try_from(parsed).map_err(|_| malformed(format!("{what} {parsed} does not fit in 32 bits")))
}

/// Build a [`TargetRejection::Malformed`] — one helper so every parse failure
/// reads the same way.
fn malformed(msg: impl Into<String>) -> TargetRejection {
    TargetRejection::Malformed(msg.into())
}

/// Reject a segment name that is empty or could escape the shm directory.
///
/// # Errors
///
/// [`TargetRejection::Malformed`] if the name is empty, contains a path
/// separator or NUL, or is a relative-path element.
fn validate_segment_name(name: &str) -> Result<(), TargetRejection> {
    if name.is_empty() {
        return Err(malformed("empty segment name"));
    }
    if name.contains('/') || name.contains('\0') || name == "." || name == ".." {
        return Err(malformed(format!("unsafe segment name {name:?}")));
    }
    Ok(())
}

/// Parse the `checksum=` parameter: `crc32` (on) or `none` (off).
///
/// # Errors
///
/// [`TargetRejection::Malformed`] for any other value — silently treating an
/// unrecognized algorithm as "off" would hand a client unverified bytes it
/// believes were checked.
fn parse_checksum(value: &str) -> Result<bool, TargetRejection> {
    match value.trim() {
        CHECKSUM_ALGORITHM => Ok(true),
        CHECKSUM_NONE => Ok(false),
        other => Err(malformed(format!(
            "unknown {CHECKSUM_PARAM} {other:?} (want {CHECKSUM_ALGORITHM} or {CHECKSUM_NONE})"
        ))),
    }
}

/// Parse a decimal or `0x`-prefixed descriptor parameter.
///
/// # Errors
///
/// [`TargetRejection::Malformed`] if the value does not parse in its radix.
fn parse_u64(value: &str) -> Result<u64, TargetRejection> {
    let value = value.trim();
    let parsed = match value.strip_prefix(HEX_PREFIX) {
        Some(hex) => u64::from_str_radix(hex, HEX_RADIX),
        None => value.parse::<u64>(),
    };
    parsed.map_err(|e| malformed(format!("value {value:?}: {e}")))
}

/// Node-wide accounting for pinned client memory (ADR-0026 point 8). Cheap
/// enough to consult per request: one atomic compare-and-swap loop, no lock.
#[derive(Debug)]
pub struct DeliveryQuota {
    /// Bytes currently reserved by live targets.
    in_use: AtomicU64,
    /// Node-wide ceiling.
    max: u64,
    /// Per-request ceiling.
    max_per_target: u64,
}

impl DeliveryQuota {
    /// A quota from the operator's two ceilings.
    #[must_use]
    pub fn new(pinned_bytes_max: u64, max_target_bytes: u64) -> Self {
        Self {
            in_use: AtomicU64::new(0),
            max: pinned_bytes_max,
            max_per_target: max_target_bytes,
        }
    }

    /// Bytes currently reserved, for the `pacer_delivery_pinned_bytes` gauge.
    #[must_use]
    pub fn in_use(&self) -> u64 {
        self.in_use.load(Ordering::Relaxed)
    }

    /// Reserve `bytes` until the returned permit drops.
    ///
    /// # Errors
    ///
    /// [`TargetRejection::OverQuota`] when `bytes` exceeds the per-request
    /// ceiling or would push the node-wide total past its own. Both are
    /// degradable: the caller serves the body instead of failing the read.
    pub fn reserve(self: &Arc<Self>, bytes: u64) -> Result<QuotaPermit, TargetRejection> {
        if bytes > self.max_per_target {
            return Err(TargetRejection::OverQuota {
                requested: bytes,
                in_use: self.in_use(),
                max: self.max_per_target,
            });
        }
        let mut current = self.in_use.load(Ordering::Relaxed);
        loop {
            let Some(next) = current.checked_add(bytes).filter(|n| *n <= self.max) else {
                return Err(TargetRejection::OverQuota {
                    requested: bytes,
                    in_use: current,
                    max: self.max,
                });
            };
            match self.in_use.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(QuotaPermit {
                        quota: Arc::clone(self),
                        bytes,
                    })
                }
                Err(observed) => current = observed,
            }
        }
    }
}

/// A live reservation against a [`DeliveryQuota`], released on drop — so the
/// accounting is tied to the mapping's lifetime rather than to a code path
/// remembering to give it back.
#[derive(Debug)]
pub struct QuotaPermit {
    quota: Arc<DeliveryQuota>,
    bytes: u64,
}

impl Drop for QuotaPermit {
    fn drop(&mut self) {
        self.quota.in_use.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// A `MAP_SHARED` mapping of a client's segment, unmapped on drop. Separate
/// from [`MappedTarget`] so the RDMA registration a caller layers on top
/// (`register_client_target`) can be dropped first: deregistering after the
/// pages are gone is undefined, and field order is what guarantees the order.
#[derive(Debug)]
struct Mapping {
    ptr: *mut u8,
    len: usize,
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`len` are exactly what `map_shared` returned and this is
        // their sole owner.
        unsafe {
            libc::munmap(self.ptr.cast(), self.len);
        }
    }
}

/// A client's segment, mapped, with the window the descriptor named and the
/// quota reservation that bounds it (ADR-0026 points 1 and 8).
///
/// **Lifetime is the request** (ADR-0026 point 7): dropping this unmaps the
/// segment and releases the reservation, and the daemon holds no registration
/// across requests. The client's own contract is the mirror image — the buffer
/// must stay allocated and untouched until the 200 arrives.
///
/// # Exclusivity
///
/// Writes go through [`MappedTarget::copy_in`] on a `&self`, because the
/// covering-chunk pipeline resolves several chunks concurrently and each writes
/// its own window. That is sound *because the windows are disjoint by
/// construction*: [`chunk_windows`] emits one window per covering chunk index
/// and consecutive chunks tile the requested range with no overlap
/// (`ChunkConfig::chunk_bounds`). A caller that invents its own offsets breaks
/// the invariant the same way two overlapping arena leases would (see
/// `pacer_transport::efa::arena`), so windows must come from that function.
#[derive(Debug)]
pub struct MappedTarget {
    /// The whole segment. Declared first so it drops LAST (Rust drops fields in
    /// declaration order), i.e. after anything the caller derived from it.
    mapping: Mapping,
    /// Byte offset of the window within the mapping.
    window_offset: usize,
    /// Window length in bytes — the descriptor's `len`.
    window_len: usize,
    /// The reservation this target holds; released when it drops.
    _permit: QuotaPermit,
}

// SAFETY: the only non-`Send`/`Sync` member is `Mapping`'s raw pointer, and the
// mapping is owned exclusively by this value from construction to drop. Every
// write names a window from `chunk_windows`, which are disjoint (see the type's
// Exclusivity note), so no two threads ever alias the same bytes. The proxy
// shares one target across the concurrent chunk resolutions of ONE request,
// which is what these impls are for.
unsafe impl Send for MappedTarget {}
unsafe impl Sync for MappedTarget {}

impl MappedTarget {
    /// Reserve quota for `spec`, map the segment it names under `cfg.shm_dir`,
    /// and hand back the window.
    ///
    /// # Errors
    ///
    /// [`TargetRejection::OverQuota`] when the reservation does not fit (the
    /// caller serves the body instead), or [`TargetRejection::Unusable`] when
    /// the segment is absent, cannot be mapped, or is smaller than
    /// `offset + len`.
    pub fn open(
        cfg: &DeliveryConfig,
        spec: &TargetSpec,
        quota: &Arc<DeliveryQuota>,
    ) -> Result<Self, TargetRejection> {
        let end = spec
            .window_end()
            .ok_or_else(|| malformed("offset + len overflows"))?;
        // Reserve BEFORE mapping: the reservation is what bounds pinned pages,
        // so a target that cannot be admitted must not first map anything.
        let permit = quota.reserve(spec.len)?;
        // Host path only. A `nic:` token names memory the daemon must not map at all,
        // so the caller dispatches on the scheme first and this never sees one — the
        // check stays because reaching a `MappedTarget` without a segment name would
        // otherwise mean inventing one.
        let name = spec.shm_name().ok_or_else(|| {
            TargetRejection::Unusable(format!(
                "{} target cannot be mapped as host shared memory",
                spec.memory.label()
            ))
        })?;
        let path = cfg.shm_dir.join(name);
        let (mapping, segment_len) = map_shared(&path)?;
        if end > segment_len {
            return Err(TargetRejection::Unusable(format!(
                "{} is {segment_len} B, too small for offset {} + len {}",
                path.display(),
                spec.offset,
                spec.len
            )));
        }
        Ok(Self {
            mapping,
            window_offset: spec.offset as usize,
            window_len: spec.len as usize,
            _permit: permit,
        })
    }

    /// Address of the window's first byte — what the transport registers as the
    /// holders' WRITE destination (ADR-0026 point 3: the client never speaks
    /// RDMA; the daemon registers on its behalf).
    #[must_use]
    pub fn window_addr(&self) -> u64 {
        // SAFETY-adjacent: pointer arithmetic only, never dereferenced here.
        // `window_offset + window_len <= mapping.len` was checked in `open`.
        (self.mapping.ptr as u64) + self.window_offset as u64
    }

    /// Raw pointer to the window's first byte, for the transport's
    /// registration.
    ///
    /// # Safety
    ///
    /// The caller must not read or write through this pointer beyond
    /// [`MappedTarget::window_len`] bytes, and must not let anything derived
    /// from it outlive this `MappedTarget` (the mapping goes away on drop).
    #[must_use]
    pub unsafe fn window_ptr(&self) -> *mut u8 {
        self.mapping.ptr.add(self.window_offset)
    }

    /// Bytes the client made available.
    #[must_use]
    pub fn window_len(&self) -> usize {
        self.window_len
    }

    /// Copy `src` into the window at `at`.
    ///
    /// This is the local-tier delivery mechanism (ADR-0026 point 4: a chunk in
    /// this node's own cache is a `memcpy`, not an RDMA WRITE) and the landing
    /// path for a peer that streamed instead of writing.
    ///
    /// # Panics
    ///
    /// If `at + src.len()` exceeds the window — the window comes from
    /// [`chunk_windows`], which never exceeds the requested range, so reaching
    /// here means a caller computed its own offsets (see the type's Exclusivity
    /// note).
    pub fn copy_in(&self, at: usize, src: &[u8]) {
        assert!(
            at.saturating_add(src.len()) <= self.window_len,
            "delivery window overflow: {at} + {} > {}",
            src.len(),
            self.window_len
        );
        // SAFETY: the destination is inside the window (asserted), the mapping
        // outlives this call, and `src` cannot alias it — `src` is either a
        // cached `Bytes` or a peer's streamed frame, never this mapping.
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), self.window_ptr().add(at), src.len());
        }
    }

    /// `"{algorithm}={hex}"` over the first `len` delivered bytes in ONE pass.
    ///
    /// Retained for the small-object case and for the test that proves
    /// [`DeliveryDigest`]'s combined result is identical — the delivery path
    /// itself uses per-window digests, because one serial pass over a 100 GB
    /// window is tens of seconds of latency added after the last byte lands.
    ///
    /// # Panics
    ///
    /// If `len` exceeds the window, for the same reason as
    /// [`MappedTarget::copy_in`].
    #[must_use]
    pub fn checksum(&self, len: usize) -> String {
        assert!(len <= self.window_len, "checksum past the delivery window");
        self.digest_window(0, len).header_value()
    }

    /// The digest of ONE window, computed where that window is delivered so the
    /// cost rides along with the copy/WRITE instead of following it.
    ///
    /// Combined in offset order by [`DeliveryDigest::absorb`], the result is
    /// byte-for-byte what a single pass produces (asserted by a test), so the
    /// client's `zlib.crc32` check is unaffected by how the work was split.
    ///
    /// # Panics
    ///
    /// If `at + len` exceeds the window — same caller-bug reasoning as
    /// [`MappedTarget::copy_in`].
    #[must_use]
    pub fn digest_window(&self, at: usize, len: usize) -> DeliveryDigest {
        assert!(
            at.saturating_add(len) <= self.window_len,
            "digest window overflow: {at} + {len} > {}",
            self.window_len
        );
        // SAFETY: the range is inside the window (asserted); the caller computes
        // it from `chunk_windows`, and calls this only after ITS window's bytes
        // have landed, so no writer aliases them.
        let bytes = unsafe { std::slice::from_raw_parts(self.window_ptr().add(at), len) };
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(bytes);
        DeliveryDigest(hasher)
    }
}

/// A CRC32 over part of a delivery, combinable with the windows around it.
///
/// Exists so integrity does not cost a second, serial pass over the delivered
/// bytes: each window's digest is computed by the task that delivered it, and
/// [`DeliveryDigest::absorb`] folds them in offset order into exactly the value
/// one pass would have produced (`crc32fast::Hasher::combine` tracks each side's
/// length, which is what makes the fold exact rather than approximate).
#[derive(Clone)]
pub struct DeliveryDigest(crc32fast::Hasher);

impl DeliveryDigest {
    /// The digest of zero bytes — the identity for [`DeliveryDigest::absorb`].
    #[must_use]
    pub fn empty() -> Self {
        Self(crc32fast::Hasher::new())
    }

    /// The digest of `bytes` themselves — the **source-side** variant.
    ///
    /// The destination-side [`DeliveryTarget::digest_window`] cannot be used for a
    /// client-registered target (ADR-0030 point 7): the daemon holds an rkey for that
    /// window and no mapping of it, so the only readable copy of the delivered bytes is
    /// the one it wrote *from*. Same value either way — the bytes are identical, and the
    /// WRITE's completion is what proves they arrived — but this side is readable.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(bytes);
        Self(hasher)
    }

    /// A digest another node computed over `len` bytes it wrote itself — the **remote-half**
    /// variant (planning/19 C3).
    ///
    /// Under ADR-0030's remote half a holder's NIC writes the chunk straight into the client,
    /// so neither [`Self::of`] nor [`DeliveryTarget::digest_window`] is available here: this
    /// node never saw the bytes and cannot read the window back. The holder reports its CRC32
    /// (`BlobMeta.written_crc32`) and this rehydrates it into something [`Self::absorb`] can
    /// fold.
    ///
    /// **The length is not decoration.** CRC32 combination is only exact if each side's
    /// length is known — that is what `crc32fast::Hasher::combine` tracks — so a digest
    /// rebuilt with the wrong length folds to a value no client can reproduce. Asserted
    /// against the same bytes hashed locally, by
    /// `a_holder_reported_digest_folds_like_a_local_one` in this module's tests.
    #[must_use]
    pub fn reported(crc32: u32, len: u64) -> Self {
        Self(crc32fast::Hasher::new_with_initial_len(crc32, len))
    }

    /// This digest's raw CRC32 — what a holder puts on the wire for [`Self::reported`] to
    /// rebuild.
    ///
    /// Exposed so the holder computes its answer with the same type the requester folds,
    /// rather than a second call into `crc32fast` that could drift onto a different flavour.
    #[must_use]
    pub fn value(&self) -> u32 {
        self.0.clone().finalize()
    }

    /// Append `next`'s window to this one. **Order matters**: CRC32 is
    /// position-dependent, so windows must be absorbed in ascending offset.
    pub fn absorb(&mut self, next: &Self) {
        self.0.combine(&next.0);
    }

    /// The `"{algorithm}={hex}"` value of [`CHECKSUM_HEADER`].
    #[must_use]
    pub fn header_value(&self) -> String {
        format!("{CHECKSUM_ALGORITHM}={:08x}", self.0.clone().finalize())
    }
}

/// `mmap` a client's segment `MAP_SHARED` and return it with its length.
///
/// `MAP_SHARED` is the whole point: the client's writes and ours must be the
/// same pages. The file is closed immediately — a mapping keeps its own
/// reference, so holding the fd would only add a leak path.
///
/// # Errors
///
/// [`TargetRejection::Unusable`] if the path cannot be opened, stat'd, is
/// empty, or `mmap` fails.
fn map_shared(path: &Path) -> Result<(Mapping, u64), TargetRejection> {
    let unusable =
        |e: std::io::Error| TargetRejection::Unusable(format!("{}: {e}", path.display()));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(unusable)?;
    let len = file.metadata().map_err(unusable)?.len();
    if len == 0 {
        return Err(TargetRejection::Unusable(format!(
            "{} is empty (the client must size its segment before the GET)",
            path.display()
        )));
    }
    // SAFETY: `len > 0`, the fd is open and writable for the duration of the
    // call, and the result is checked against MAP_FAILED before use.
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len as usize,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            std::os::fd::AsRawFd::as_raw_fd(&file),
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(unusable(std::io::Error::last_os_error()));
    }
    Ok((
        Mapping {
            ptr: ptr.cast(),
            len: len as usize,
        },
        len,
    ))
}

/// One covering chunk's contribution to a delivery: which bytes of the chunk go
/// where in the client's window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkWindow {
    /// Chunk index within the object (ADR-0015 addressing).
    pub idx: u64,
    /// Offset into the client's window these bytes land at.
    pub dst_at: usize,
    /// Offset within the chunk the bytes start at — non-zero only for the first
    /// covering chunk of a range that starts mid-chunk.
    pub src_at: usize,
    /// How many bytes this chunk contributes.
    pub len: usize,
    /// Whether this window is the chunk's ENTIRE body.
    ///
    /// This is the RDMA eligibility test, and it is why the field exists: a
    /// holder-driven WRITE moves the whole cached chunk into the offered range
    /// (ADR-0018), so a partial edge chunk cannot be RDMA-delivered into an
    /// exact window without overwriting its neighbours. Partial windows are
    /// fetched normally and copied; whole ones can land straight in client
    /// memory. Chunk-aligned reads — every whole-object read, and what a
    /// checkpoint loader issues — are all-whole.
    pub whole: bool,
}

/// The per-chunk windows that deliver object bytes `[start, end)` into a client
/// window whose byte 0 corresponds to object byte `start`.
///
/// Pure arithmetic over [`ChunkConfig`] — no cache, no I/O — so the mapping
/// from a ranged GET to a set of disjoint destination windows is unit-testable
/// on its own, which is exactly what [`MappedTarget`]'s exclusivity argument
/// rests on.
#[must_use]
pub fn chunk_windows(
    chunk: &ChunkConfig,
    object_len: u64,
    start: u64,
    end: u64,
) -> Vec<ChunkWindow> {
    let covering = chunk.covering(start, end);
    let mut windows = Vec::with_capacity((covering.end - covering.start) as usize);
    for idx in covering {
        let Some(bounds) = chunk.chunk_bounds(idx, object_len) else {
            break;
        };
        let lo = start.max(bounds.start);
        let hi = end.min(bounds.end);
        if lo >= hi {
            continue;
        }
        windows.push(ChunkWindow {
            idx,
            dst_at: (lo - start) as usize,
            src_at: (lo - bounds.start) as usize,
            len: (hi - lo) as usize,
            whole: lo == bounds.start && hi == bounds.end,
        });
    }
    windows
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The descriptor form ADR-0026 documents, plus the decimal spelling a
    /// Python client produces without formatting hex.
    #[test]
    fn parses_the_documented_descriptor() {
        let spec = TargetSpec::parse("shm:/pacer-loader-7;offset=0x40000;len=16777216").unwrap();
        assert_eq!(
            spec,
            TargetSpec {
                memory: TargetMemory::Shm {
                    name: "pacer-loader-7".into(),
                },
                offset: 0x40000,
                len: 16_777_216,
                // Verification is on unless the client opts out.
                checksum: true,
            }
        );
        // Decimal offset, and an absent offset meaning "start of the segment".
        assert_eq!(
            TargetSpec::parse("shm:/s;offset=4096;len=8")
                .unwrap()
                .offset,
            4096
        );
        assert_eq!(TargetSpec::parse("shm:/s;len=8").unwrap().offset, 0);
        // The POSIX leading slash is optional, since the file under the shm
        // directory does not carry one.
        assert_eq!(
            TargetSpec::parse("shm:s;len=8").unwrap().shm_name(),
            Some("s")
        );
    }

    /// `checksum=` selects the integrity check, and only the two known values are
    /// accepted: silently treating an unknown algorithm as "off" would hand a
    /// client unverified bytes it believes were checked.
    #[test]
    fn checksum_parameter_opts_out_explicitly() {
        assert!(TargetSpec::parse("shm:/s;len=8").unwrap().checksum);
        assert!(
            TargetSpec::parse("shm:/s;len=8;checksum=crc32")
                .unwrap()
                .checksum
        );
        assert!(
            !TargetSpec::parse("shm:/s;len=8;checksum=none")
                .unwrap()
                .checksum
        );
        let err = TargetSpec::parse("shm:/s;len=8;checksum=sha256").unwrap_err();
        assert!(matches!(err, TargetRejection::Malformed(_)), "{err}");
    }

    /// The property the whole per-window digest scheme rests on: folding each
    /// window's CRC32 in offset order is byte-for-byte what one pass produces. If
    /// this ever diverges, every client's `zlib.crc32` check starts failing on
    /// good data.
    #[test]
    fn per_window_digests_fold_to_the_single_pass_digest() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = DeliveryConfig {
            enabled: true,
            shm_dir: dir.path().to_path_buf(),
            ..DeliveryConfig::default()
        };
        std::fs::write(dir.path().join("seg"), vec![0u8; 8192]).unwrap();
        let quota = Arc::new(DeliveryQuota::new(1 << 20, 1 << 20));
        let spec = TargetSpec::parse("shm:/seg;len=6000").unwrap();
        let target = MappedTarget::open(&cfg, &spec, &quota).unwrap();
        // Three uneven windows, delivered out of order (as `buffered` may).
        let windows = [(0usize, 1500usize), (1500, 2500), (4000, 2000)];
        for (at, len) in windows {
            let payload: Vec<u8> = (0..len).map(|i| (i + at) as u8).collect();
            target.copy_in(at, &payload);
        }
        let mut folded = DeliveryDigest::empty();
        for (at, len) in windows {
            folded.absorb(&target.digest_window(at, len));
        }
        assert_eq!(folded.header_value(), target.checksum(6000));
        // And an empty fold is the identity, so a `checksum=none` request folding
        // zero windows cannot produce a bogus non-empty digest.
        assert_eq!(
            DeliveryDigest::empty().header_value(),
            format!("{CHECKSUM_ALGORITHM}=00000000")
        );
    }

    /// The property ADR-0030's remote half rests on: a digest rebuilt from the number a
    /// HOLDER reported folds to exactly what hashing the same bytes here would produce.
    ///
    /// This is the one thing that cannot be caught by inspection. If [`DeliveryDigest::reported`]
    /// were built without the length, `combine` would fold it at the wrong offset and every
    /// checksum of a partly-remote delivery would fail on good data — while a delivery served
    /// entirely from the local cache stayed green, so the failure would look like a fabric
    /// problem rather than an arithmetic one.
    #[test]
    fn a_holder_reported_digest_folds_like_a_local_one() {
        // Three uneven windows, as a chunk grid's edges produce.
        let windows: Vec<Vec<u8>> = [1500usize, 2500, 2000]
            .iter()
            .enumerate()
            .map(|(w, len)| (0..*len).map(|i| (i + w * 97) as u8).collect())
            .collect();
        let mut local = DeliveryDigest::empty();
        let mut remote = DeliveryDigest::empty();
        for bytes in &windows {
            let here = DeliveryDigest::of(bytes);
            // What the wire carries: one u32 and the byte count.
            let rebuilt = DeliveryDigest::reported(here.value(), bytes.len() as u64);
            assert_eq!(
                rebuilt.header_value(),
                here.header_value(),
                "a single reported window must equal the local hash of the same bytes"
            );
            local.absorb(&here);
            remote.absorb(&rebuilt);
        }
        // And the FOLD agrees, which is the part the length makes true.
        let whole: Vec<u8> = windows.concat();
        assert_eq!(
            local.header_value(),
            DeliveryDigest::of(&whole).header_value()
        );
        assert_eq!(remote.header_value(), local.header_value());
        // A mixed delivery — some windows local, some reported — is the real shape, since
        // rendezvous hashing puts roughly 1/N of an object on the reading node itself.
        let mut mixed = DeliveryDigest::empty();
        for (i, bytes) in windows.iter().enumerate() {
            let d = DeliveryDigest::of(bytes);
            if i == 1 {
                mixed.absorb(&DeliveryDigest::reported(d.value(), bytes.len() as u64));
            } else {
                mixed.absorb(&d);
            }
        }
        assert_eq!(mixed.header_value(), local.header_value());
        // The length really is load-bearing: rebuilt with a wrong one, the fold diverges.
        let mut wrong = DeliveryDigest::empty();
        wrong.absorb(&DeliveryDigest::reported(
            DeliveryDigest::of(&windows[0]).value(),
            windows[0].len() as u64,
        ));
        wrong.absorb(&DeliveryDigest::reported(
            DeliveryDigest::of(&windows[1]).value(),
            // One byte short.
            windows[1].len() as u64 - 1,
        ));
        let mut right = DeliveryDigest::empty();
        right.absorb(&DeliveryDigest::of(&windows[0]));
        right.absorb(&DeliveryDigest::of(&windows[1]));
        assert_ne!(
            wrong.header_value(),
            right.header_value(),
            "if this ever passes, `reported` has stopped tracking length and every \
             partly-remote delivery's checksum is unverifiable"
        );
    }

    /// A GID as a client would spell it: `fe80::` link-local, 16 bytes, 32 hex digits.
    const GID_A: &str = "fe800000000000000abcdefffe123456";
    const GID_B: &str = "fe800000000000000abcdefffe123457";

    #[test]
    fn parses_a_nic_token_descriptor() {
        let raw = format!(
            "nic:0x7f45f4000000;offset=0x1000000;len=67108864;\
             rails={GID_A}/16385/1048576,{GID_B}/16386/1048577"
        );
        let spec = TargetSpec::parse(&raw).unwrap();
        assert_eq!(spec.offset, 0x0100_0000);
        assert_eq!(spec.len, 67_108_864);
        assert_eq!(spec.memory.label(), "nic");
        assert_eq!(
            spec.shm_name(),
            None,
            "a client-registered target has no segment"
        );
        match spec.memory {
            TargetMemory::Nic { base_addr, rails } => {
                assert_eq!(base_addr, 0x7f45_f400_0000);
                assert_eq!(rails.len(), 2, "one entry per rail the client registered");
                // The rkey differs per rail because each rail's PD issued its own — the
                // reason the token is a set at all.
                assert_eq!(rails[0].qpn, 16_385);
                assert_eq!(rails[0].rkey, 1_048_576);
                assert_eq!(rails[1].rkey, 1_048_577);
                assert_eq!(
                    rails[0].gid[0], 0xfe,
                    "GID parses big-endian, first byte first"
                );
                assert_eq!(rails[0].gid[15], 0x56);
                assert_ne!(
                    rails[0].gid, rails[1].gid,
                    "the two rails are distinct devices"
                );
            }
            other => panic!("expected a NIC token, got {other:?}"),
        }
    }

    #[test]
    fn parses_a_single_rail_token_and_a_zero_qpn() {
        // The ordinary case (one rail), and a QPN of 0 — legitimate on EFA, so it must not
        // be read as "unset".
        let raw = format!("nic:0x1000;len=4096;rails={GID_A}/0/7");
        match TargetSpec::parse(&raw).unwrap().memory {
            TargetMemory::Nic { base_addr, rails } => {
                assert_eq!(base_addr, 0x1000);
                assert_eq!(rails.len(), 1);
                assert_eq!(rails[0].qpn, 0);
                assert_eq!(rails[0].rkey, 7);
            }
            other => panic!("expected a NIC token, got {other:?}"),
        }
    }

    #[test]
    fn parses_a_token_naming_every_rail_a_p5_has() {
        let rails: Vec<String> = (0..32)
            .map(|i| {
                format!(
                    "fe8000000000000000000000000000{i:02x}/{}/{}",
                    16_384 + i,
                    100 + i
                )
            })
            .collect();
        let raw = format!("nic:0x2000;len=65536;rails={}", rails.join(","));
        match TargetSpec::parse(&raw).unwrap().memory {
            TargetMemory::Nic { rails, .. } => assert_eq!(rails.len(), 32),
            other => panic!("expected a NIC token, got {other:?}"),
        }
    }

    #[test]
    fn rejects_malformed_nic_tokens() {
        // Each case is a way a real client gets this wrong, and each must be a REJECTION
        // rather than a silently different address: every field here is an address or a
        // capability, so a lenient parse writes someone else's bytes somewhere else.
        let cases: Vec<(String, &str)> = vec![
            (
                "nic:0x1000;len=4096".to_owned(),
                "a token with no rails names nothing to write to",
            ),
            (
                format!("nic:0x1000;rails={GID_A}/1/1"),
                "len is required, as for every other scheme",
            ),
            (
                format!("nic:0x1000;len=0;rails={GID_A}/1/1"),
                "a zero-length window can deliver nothing",
            ),
            (
                format!("nic:notahex;len=4096;rails={GID_A}/1/1"),
                "the base address must parse",
            ),
            (
                "nic:0x1000;len=4096;rails=fe80/1/1".to_owned(),
                "a short GID would address a different device",
            ),
            (
                format!("nic:0x1000;len=4096;rails={GID_A}0/1/1"),
                "a long GID is equally wrong",
            ),
            (
                "nic:0x1000;len=4096;rails=zzzz0000000000000abcdefffe123456/1/1".to_owned(),
                "a non-hex GID must not silently become zeroes",
            ),
            (
                format!("nic:0x1000;len=4096;rails={GID_A}/1"),
                "an entry missing its rkey",
            ),
            (
                format!("nic:0x1000;len=4096;rails={GID_A}/1/1/1"),
                "an entry with a field too many",
            ),
            (
                format!("nic:0x1000;len=4096;rails={GID_A}/4294967296/1"),
                "a qpn that does not fit in 32 bits",
            ),
            (
                format!("nic:0x1000;len=4096;rails={GID_A}/1/4294967296"),
                "an rkey that does not fit in 32 bits",
            ),
            (
                format!("nic:0x1000;len=4096;rails={GID_A}/1/1,"),
                "a trailing separator leaves an empty entry",
            ),
            (
                format!("nic:0x1000;len=4096;rails={GID_A}/1/1,{GID_A}/1/2"),
                "the same rail twice under two rkeys is ambiguous",
            ),
            (
                format!(
                    "nic:0x1000;len=4096;rails={GID_A}/1/1;\
                     device=GPU-8b9a0d44-e3b8-4ba1-9880-00f10c761608"
                ),
                "`device` went with ADR-0027's scheme and must not be silently ignored",
            ),
            (
                format!("shm:/seg;len=4096;rails={GID_A}/1/1"),
                "rails belong to a nic target",
            ),
        ];
        for (raw, why) in cases {
            let err = TargetSpec::parse(&raw).expect_err(why);
            assert!(
                matches!(err, TargetRejection::Malformed(_)),
                "{why}: expected Malformed, got {err:?} for {raw:?}",
            );
        }
    }

    #[test]
    fn rejects_more_rails_than_a_token_may_carry() {
        let rails: Vec<String> = (0..=MAX_TOKEN_RAILS)
            .map(|i| {
                format!(
                    "fe80000000000000000000000000{i:04x}/{}/{}",
                    16_384 + i,
                    100 + i
                )
            })
            .collect();
        let raw = format!("nic:0x2000;len=65536;rails={}", rails.join(","));
        assert!(
            matches!(TargetSpec::parse(&raw), Err(TargetRejection::Malformed(_))),
            "a token above the announce's rail limit would be unanswerable",
        );
    }

    /// Every malformed descriptor is a client error, not a silent default — a **retired**
    /// scheme especially. `cuda-ipc:` was removed with ADR-0027's mechanism, and the one
    /// answer it must never get is a silent reinterpretation as some other scheme: an old
    /// client is owed the parse error that names what it sent.
    #[test]
    fn rejects_malformed_descriptors() {
        for raw in [
            "cuda-ipc:QUJD;len=8",   // ADR-0027's retired scheme, no longer parsed
            "shm:/s;len=8;device=2", // `device` went with it
            "gpu-direct:/s;len=8",   // a scheme we do not know
            "shm:/s",                // no len
            "shm:/s;len=0",          // zero-length window
            "shm:/s;len=abc",        // undecodable
            "shm:/s;bogus=1;len=8",  // unknown parameter
            "shm:/s;len",            // not key=value
            "shm:/;len=8",           // empty name
            "shm:/../escape;len=8",  // path traversal
            "shm:/sub/dir;len=8",    // not a single component
            "",                      // no scheme at all
        ] {
            let err = TargetSpec::parse(raw).unwrap_err();
            assert!(
                matches!(err, TargetRejection::Malformed(_)),
                "{raw:?} should be malformed, got {err}"
            );
            assert!(
                !err.is_degradable(),
                "{raw:?} must not degrade to a body read"
            );
        }
    }

    /// Both ceilings reject, both are degradable (a body-delivered read, never a
    /// failed one — ADR-0026 point 8), and a released permit frees its bytes.
    #[test]
    fn quota_bounds_per_target_and_node_wide() {
        let quota = Arc::new(DeliveryQuota::new(1024, 512));
        // Per-request ceiling.
        let err = quota.reserve(513).unwrap_err();
        assert!(err.is_degradable() && err.reason() == "quota");
        // Node-wide ceiling: two 512 B targets fit, a third does not.
        let a = quota.reserve(512).unwrap();
        let b = quota.reserve(512).unwrap();
        assert_eq!(quota.in_use(), 1024);
        assert!(quota.reserve(512).unwrap_err().is_degradable());
        // Releasing frees exactly its own reservation.
        drop(a);
        assert_eq!(quota.in_use(), 512);
        let c = quota.reserve(512).unwrap();
        assert_eq!(quota.in_use(), 1024);
        drop((b, c));
        assert_eq!(quota.in_use(), 0);
    }

    /// A whole-object read is all-whole windows tiling the client buffer with no
    /// gap and no overlap — the disjointness `MappedTarget` relies on.
    #[test]
    fn whole_object_windows_tile_the_buffer() {
        let chunk = ChunkConfig::new(100);
        let windows = chunk_windows(&chunk, 250, 0, 250);
        assert_eq!(windows.len(), 3);
        let mut next = 0;
        for w in &windows {
            assert_eq!(w.dst_at, next, "windows must be contiguous");
            assert_eq!(w.src_at, 0);
            assert!(w.whole, "a whole-object read is RDMA-eligible throughout");
            next += w.len;
        }
        assert_eq!(next, 250);
        // The short last chunk is still "whole" — it is the entire cached body.
        assert_eq!(windows[2].len, 50);
    }

    /// A range starting and ending mid-chunk trims both edges, and those edges
    /// are NOT RDMA-eligible (a holder WRITEs the whole chunk, ADR-0018).
    #[test]
    fn partial_edges_are_trimmed_and_not_whole() {
        let chunk = ChunkConfig::new(100);
        let windows = chunk_windows(&chunk, 250, 50, 220);
        assert_eq!(windows.len(), 3);
        assert_eq!(
            windows[0],
            ChunkWindow {
                idx: 0,
                dst_at: 0,
                src_at: 50,
                len: 50,
                whole: false
            }
        );
        assert_eq!(
            windows[1],
            ChunkWindow {
                idx: 1,
                dst_at: 50,
                src_at: 0,
                len: 100,
                whole: true
            }
        );
        assert_eq!(
            windows[2],
            ChunkWindow {
                idx: 2,
                dst_at: 150,
                src_at: 0,
                len: 20,
                whole: false
            }
        );
        // Delivered bytes == the requested range.
        assert_eq!(windows.iter().map(|w| w.len).sum::<usize>(), 170);
    }

    /// A chunk-aligned range is whole throughout even though it is not the whole
    /// object: that is the shape a loader reading shard-by-shard produces.
    #[test]
    fn chunk_aligned_range_is_all_whole() {
        let chunk = ChunkConfig::new(100);
        let windows = chunk_windows(&chunk, 400, 100, 300);
        assert_eq!(windows.len(), 2);
        assert!(windows.iter().all(|w| w.whole));
        assert_eq!(windows[0].idx, 1);
        assert_eq!(windows[1].dst_at, 100);
    }

    /// Map a real segment, deliver into it by both mechanisms, and read the
    /// bytes back through an independent mapping — the end-to-end host-memory
    /// contract, minus the HTTP layer (which `proxy::tests` covers).
    #[test]
    fn maps_a_segment_delivers_and_checksums() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = DeliveryConfig {
            enabled: true,
            shm_dir: dir.path().to_path_buf(),
            ..DeliveryConfig::default()
        };
        std::fs::write(dir.path().join("seg"), vec![0u8; 4096]).unwrap();
        let quota = Arc::new(DeliveryQuota::new(1 << 20, 1 << 20));
        let spec = TargetSpec::parse("shm:/seg;offset=1024;len=64").unwrap();
        let target = MappedTarget::open(&cfg, &spec, &quota).unwrap();
        assert_eq!(target.window_len(), 64);
        assert_eq!(quota.in_use(), 64, "a live target holds its reservation");

        target.copy_in(0, &[7u8; 32]);
        target.copy_in(32, &[9u8; 32]);
        let checksum = target.checksum(64);
        assert!(checksum.starts_with("crc32="), "got {checksum}");

        // The client's own view: the same pages, at its own offset.
        let seen = std::fs::read(dir.path().join("seg")).unwrap();
        assert_eq!(&seen[1024..1056], &[7u8; 32]);
        assert_eq!(&seen[1056..1088], &[9u8; 32]);
        assert!(
            seen[..1024].iter().all(|b| *b == 0),
            "delivery stayed in its window"
        );
        assert!(
            seen[1088..].iter().all(|b| *b == 0),
            "delivery stayed in its window"
        );

        // The checksum is over the delivered bytes only, and reproducible from
        // the client side — which is what the shim verifies.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&seen[1024..1088]);
        assert_eq!(checksum, format!("crc32={:08x}", hasher.finalize()));

        drop(target);
        assert_eq!(quota.in_use(), 0, "dropping a target releases its quota");
    }

    /// The checksum is the STANDARD CRC32 (IEEE, reflected) — pinned by the
    /// canonical `"123456789"` → `0xcbf43926` vector, because the whole point of
    /// choosing it (ADR-0026 point 5) is that a client shim can re-check with
    /// `zlib.crc32` in any language's standard library. If this ever disagrees,
    /// every shim silently starts rejecting good deliveries.
    #[test]
    fn checksum_is_standard_crc32() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = DeliveryConfig {
            enabled: true,
            shm_dir: dir.path().to_path_buf(),
            ..DeliveryConfig::default()
        };
        std::fs::write(dir.path().join("vec"), vec![0u8; 4096]).unwrap();
        let quota = Arc::new(DeliveryQuota::new(1 << 20, 1 << 20));
        let spec = TargetSpec::parse("shm:/vec;len=9").unwrap();
        let target = MappedTarget::open(&cfg, &spec, &quota).unwrap();
        target.copy_in(0, b"123456789");
        assert_eq!(target.checksum(9), "crc32=cbf43926");
    }

    /// A window past the end of the segment is the client's bug, and a missing
    /// segment likewise — both non-degradable, and neither leaks a reservation.
    #[test]
    fn rejects_a_segment_that_cannot_back_the_window() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = DeliveryConfig {
            enabled: true,
            shm_dir: dir.path().to_path_buf(),
            ..DeliveryConfig::default()
        };
        std::fs::write(dir.path().join("small"), vec![0u8; 128]).unwrap();
        let quota = Arc::new(DeliveryQuota::new(1 << 20, 1 << 20));
        for raw in ["shm:/small;offset=64;len=128", "shm:/absent;len=8"] {
            let spec = TargetSpec::parse(raw).unwrap();
            let err = MappedTarget::open(&cfg, &spec, &quota).unwrap_err();
            assert!(matches!(err, TargetRejection::Unusable(_)), "{raw}: {err}");
            assert!(!err.is_degradable());
            assert_eq!(quota.in_use(), 0, "{raw} must not leak its reservation");
        }
    }
}
