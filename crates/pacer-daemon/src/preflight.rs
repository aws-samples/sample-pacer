//! The **pre-flight endpoint exchange**: "for this object, whose NICs may write into my
//! window?" (ADR-0030 point 2).
//!
//! ## The race this removes
//!
//! On EFA the receiver must hold an address handle for the sender before the sender can write
//! to it — SRD is reliable, so the receiving NIC has to ACK, and to ACK it needs a routing
//! entry for the writer. No entry, and the WRITE completes `UNKNOWN_PEER` with **nothing
//! landing** (`bench/ladder/results/c2-loopback-gate.md`). A handle is bound to a protection
//! domain, hence to one process, so only the client can build one; the daemon can only supply
//! the *address*.
//!
//! Today the daemon supplies it by **announcing**: `efa::client_write::announce_to` SENDs its
//! endpoints and awaits the SEND's completion, which proves the message reached the client's
//! queue pair — **not** that the client decoded it and ran `ibv_create_ah`. That happens later,
//! on the client's pump thread, so a WRITE can beat it. Every mitigation so far has been a
//! *wait*: ten posts over ~2 s, a re-announce, three per-window re-attempts. Each is correct
//! and none of them makes the race impossible.
//!
//! This makes the ordering *structural* instead:
//!
//! ```text
//! 1. GET /bucket/key   x-pacer-get-endpoints: offset=…  ──►  ring lookup for the windows
//!       ◄──── 200, x-pacer-endpoints: 1, {"nodes":[…]} ─────  this read will touch (NO I/O)
//! 2. for each holder: ibv_create_ah   ← purely LOCAL to the client
//!    ══ nothing can write yet: no delivery request has been issued ══
//! 3. GET /bucket/key   x-pacer-target: nic:…  ─────────────►  delivery proceeds
//! 4.                                                          writes land, acknowledged ✔
//! ```
//!
//! Because step 3 is what sets writes in motion and step 2 finished first, no writer can be
//! early. No acknowledgement message, no nonce, and no new receive path on any daemon.
//!
//! ## Why a request header on an ordinary GET
//!
//! This is the shape the delivery protocol already has twice: a request header in
//! ([`TARGET_HEADER`](crate::delivery::TARGET_HEADER)), a small-or-empty body plus response
//! headers out ([`DELIVERED_HEADER`](crate::delivery::DELIVERED_HEADER),
//! [`CHECKSUM_HEADER`](crate::delivery::CHECKSUM_HEADER)). Three things follow from reusing it
//! rather than inventing a route:
//!
//! * **The client signs nothing by hand.** The request *is* a `GetObject`, so boto3 signs it
//!   natively and the header rides in on the same `before-send` hook `x-pacer-target` already
//!   uses. SigV4 covers `host` and `x-amz-*` only, so an `x-pacer-*` header added after signing
//!   is safe — the property `pacer_delivery.py` documents and production already depends on.
//! * **There is no query string to canonicalise.** A dedicated route would have needed one, and
//!   the two ends disagree about it: `s3s` verifies the signature against the query it *decoded
//!   and re-encoded* with strict RFC3986 rules, while botocore canonicalises the **raw** query.
//!   They agree only when the raw form is already canonical, which is a trap with no upside
//!   here.
//! * **No custom route, and no reserved path.** A reserved path was tried and rejected: `s3s`
//!   parses the S3 path *before* it consults an `S3Route`, and rejects an invalid bucket name
//!   outright — so a namespace chosen to be un-shadowable (`/_pacer/…`, since `_` is illegal in
//!   a bucket name) is a `400 InvalidBucketName` that never reaches the handler, while any
//!   prefix that *is* a valid bucket name would shadow a real bucket of that name.
//!
//! ## Why the endpoint list is the BODY
//!
//! A GID is 32 hex characters, so one 32-rail p5 node is ~1 KB of hex. Today's real maximum —
//! 8 nodes — is ~8.4 KB, which would fit a header; the concurrency-bounded worst case
//! ([`MAX_ENDPOINT_NODES`] nodes × 32 rails) is ~67 KB, which would not, and many HTTP stacks
//! cap one header line at 8 KB. The body has no such limit and the delivery protocol is already
//! sending an empty one.
//!
//! ## Why it moves no bytes
//!
//! **Nothing here reads a cache entry or touches the backend** — that is a property of the
//! code, not a promise, and `an_endpoint_query_moves_no_bytes` is the test that keeps it: it
//! asks for a key that does not exist in the backend at all and still gets an answer, which a
//! `HeadObject` would have turned into a 404.
//!
//! It is possible because the answer needs no object *length*. The chunk indices come from the
//! read's own offset ([`EndpointsAsk`]) and the bound below, and a chunk key needs only an index
//! — so the whole query is `offset ÷ chunk_size`, a ring lookup per index, and a read of the
//! per-rail address-handle cache the ADR-0019 handshake already populated. The cost is
//! microseconds of arithmetic against a race worth milliseconds.
//!
//! ## Announce is not removed — it is demoted to the repair path
//!
//! Three cases a prediction cannot cover, and all three are ordinary: a holder past the bounded
//! set below, a holder the prediction did not name (the source set can shift between the two
//! calls — eviction, rebalance, a node joining), and a client that skipped the pre-flight
//! entirely (an older shim, or one whose GET carried no marker). `announce_to`, its retry ladder
//! and `Announcer::forget` are therefore unchanged. What changes is that on the healthy path an
//! arriving announce finds the handle **already built**, which the client counts as `prearmed`
//! (`pacer_client::handles`) — that counter, and [`crate::metrics::DeliveryMetrics::preflight`]
//! here, are how an operator sees the pre-flight working rather than silently doing nothing.

use std::fmt::Write as _;

use pacer_cache::chunk::ChunkConfig;
use s3s::dto;
use s3s::{S3Response, S3Result};

use crate::proxy::{chunk_sources, Cluster};

/// Request header asking for the endpoint list instead of the object.
///
/// `x-pacer-*` rather than `x-amz-*` and that is load-bearing: SigV4 signs `host` and `x-amz-*`,
/// so this can be added **after** signing by the same `before-send` hook
/// [`crate::delivery::TARGET_HEADER`] uses, and a stock SDK needs no signer of its own.
///
/// Its **value** carries the range the client is about to read ([`EndpointsAsk`]), because the
/// HTTP `Range` header of a pre-flight GET is not that range — see [`ENDPOINTS_HEADER`] for what
/// a client sends there and why.
pub const GET_ENDPOINTS_HEADER: &str = "x-pacer-get-endpoints";

/// Response header whose **presence** says "this body is an endpoint document, not your object".
///
/// The whole of forward compatibility. A daemon that predates this — or one with delivery
/// disabled, which ignores the marker exactly as it ignores a target descriptor — answers the
/// GET normally, and a client must not feed object bytes to a decoder. So a client
/// **1.** sends `Range: bytes=0-0` on a pre-flight GET, bounding that fallback to one byte
/// instead of a checkpoint, and **2.** treats the absence of this header as "no pre-flight
/// here, proceed on the announce path". The status code cannot serve: an ordinary ranged read
/// answers 206 and this answers 200, but so would several other things.
pub const ENDPOINTS_HEADER: &str = "x-pacer-endpoints";

/// Wire version of the JSON document, and this header's value. Bump on any shape change; a
/// client that does not know a version must skip priming rather than guess, because a misread
/// GID produces an address handle that silently addresses nothing.
pub const ENDPOINTS_VERSION: u32 = 1;

/// Field of [`GET_ENDPOINTS_HEADER`]'s value: the first object byte the client is about to read.
const FIELD_OFFSET: &str = "offset";
/// Field of [`GET_ENDPOINTS_HEADER`]'s value: how many bytes it is about to read. Optional —
/// absent means "from `offset` onward", which the window bound below truncates anyway.
const FIELD_LENGTH: &str = "length";
/// Separator between fields, matching the target descriptor's grammar so a reader of one can
/// read the other.
const FIELD_SEPARATOR: char = ';';

/// `plane` value when this node has an RDMA plane and can name endpoints.
const PLANE_EFA: &str = "efa";
/// `plane` value when it has none — a gRPC-only node, a single-node daemon, or a build without
/// the `efa` feature. A *defined* answer meaning "there is nothing to prime", not an error.
const PLANE_NONE: &str = "none";

/// Content type of the answer. JSON rather than the binary announce format it embeds, because
/// the envelope is read by a Python shim with no parser of its own.
const CONTENT_TYPE_JSON: &str = "application/json";

/// `outcome` label of `pacer_delivery_preflight_total`: endpoints were named.
pub const OUTCOME_SERVED: &str = "served";
/// The query was answerable but this node has no RDMA plane, so the list is empty.
pub const OUTCOME_NO_PLANE: &str = "no_plane";
/// The marker's value did not parse. A client-side bug, and the one outcome that is an error
/// rather than an answer.
pub const OUTCOME_MALFORMED: &str = "malformed";

/// Most windows the answer looks at, and most holder nodes it names — **one number, used
/// twice, because it is one fact.**
///
/// The daemon resolves a request's windows through `stream::buffered(delivery.parallelism)`, so
/// at any instant at most that many windows are in flight, and each is written by at most one
/// node. That makes `delivery.parallelism` simultaneously the count of windows whose holders can
/// be writing *now* and the count of distinct writers that can be doing it — which is exactly
/// the set whose handles have to exist before the request, and exactly the bound the client's own
/// handle cache is sized to (`pacer_client::handles::DEFAULT_MAX_HANDLES` is this times a p5's
/// rail count).
///
/// **Why the tail is safe to leave out.** An object on a 1000-node fleet has holders approaching
/// the whole fleet, and naming them would be ~32 000 `ibv_create_ah` calls at roughly a
/// millisecond each — slower than the race it removes, and past the client's bound, so it would
/// *evict* the handles it just built. What the pre-flight has to cover is the **first-contact
/// burst**: the windows already in flight while the client's pump is still catching up. Windows
/// past that resolve later, when the pump has caught up, and a holder that was not named simply
/// announces itself — one chunk pays the ladder once per endpoint (the daemon's `ClientReady`
/// gate serialises it), and every later chunk to that holder is free. That is what announce is
/// *for*.
///
/// Resolved from the live config rather than a constant here, so an operator who widens the
/// fan-out widens the prediction with it.
#[must_use]
pub fn max_endpoint_nodes(parallelism: usize) -> usize {
    parallelism.max(1)
}

/// Documentation anchor for the bound above, so the module header can name it. Equal to the
/// shipped `delivery.parallelism`.
pub const MAX_ENDPOINT_NODES: usize = crate::delivery::DEFAULT_DELIVERY_PARALLELISM;

/// What a client is about to read, parsed from [`GET_ENDPOINTS_HEADER`]'s value.
///
/// The read's own range, **not** the pre-flight GET's `Range` header — those differ on purpose:
/// the pre-flight sends `Range: bytes=0-0` so that a daemon which ignores the marker answers one
/// byte rather than a checkpoint, which leaves the real range with nowhere else to travel.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EndpointsAsk {
    /// First object byte the read will touch.
    pub offset: u64,
    /// How many bytes it will touch, or `None` for "from `offset` onward".
    pub length: Option<u64>,
}

impl EndpointsAsk {
    /// Parse the header's value. An empty value is the whole object from byte 0.
    ///
    /// # Errors
    ///
    /// A field that is not `k=v`, an unknown field name, or a value that is not a number — each
    /// named, because the only thing that can fix it is the shim that built the header.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let mut ask = Self::default();
        for field in raw.split(FIELD_SEPARATOR).map(str::trim) {
            if field.is_empty() {
                continue;
            }
            let (name, value) = field
                .split_once('=')
                .ok_or_else(|| format!("{field:?} is not name=value"))?;
            match name {
                FIELD_OFFSET => {
                    ask.offset = value
                        .parse()
                        .map_err(|_| format!("{FIELD_OFFSET}={value:?} is not a byte offset"))?;
                }
                FIELD_LENGTH => {
                    ask.length =
                        Some(value.parse().map_err(|_| {
                            format!("{FIELD_LENGTH}={value:?} is not a byte count")
                        })?);
                }
                // Refused rather than ignored, unlike an unknown *query* parameter would be: the
                // whole value is this shim's own contract with this daemon, so an unknown field
                // means the two disagree about the grammar and the offset may not mean what it
                // says.
                other => return Err(format!("unknown field {other:?} in {GET_ENDPOINTS_HEADER}")),
            }
        }
        Ok(ask)
    }
}

/// The chunk indices whose holders the answer names: the first [`max_endpoint_nodes`] windows
/// from the read's start, truncated by the read's own length when it has one.
///
/// A pure function of the ask, the geometry and the bound — no object length, which is what
/// keeps the whole query off the cache and the backend (see the module header). A short read
/// therefore names only its own windows, and a long one names the burst that can be in flight.
///
/// Indices past the object's end are possible (nothing here knows where that is) and harmless:
/// they hash to nodes that hold nothing, so at worst the client builds a handle for a writer
/// that never writes. Bounded by the same `cap`, and cheaper than the `HeadObject` it would take
/// to avoid.
#[must_use]
pub fn endpoint_windows(
    ask: EndpointsAsk,
    chunk: &ChunkConfig,
    cap: usize,
) -> std::ops::Range<u64> {
    let size = chunk.chunk_size();
    let first = ask.offset / size;
    let ceiling = first.saturating_add(cap as u64);
    let last = match ask.length {
        // `div_ceil` on the exclusive end: a read ending mid-chunk still touches that chunk.
        Some(len) => ask
            .offset
            .saturating_add(len)
            .div_ceil(size)
            .max(first + 1)
            .min(ceiling),
        None => ceiling,
    };
    first..last
}

/// One holder and the endpoints a client must hold handles for before it writes.
struct HolderEndpoints {
    node: String,
    /// Whether this is the daemon the client is talking to. It writes every local hit, every
    /// backend read-through and every partial edge window whatever the ring says, so its handle
    /// is the one that must exist; reported so a client can see which is which rather than
    /// inferring it from position.
    local: bool,
    /// An encoded [`pacer_transport::announce`] message naming every rail of this holder —
    /// byte-identical to what that writer would SEND on first contact.
    announce: Vec<u8>,
}

/// Every node that could write `ask`'s early windows, in first-seen order, bounded by `cap`.
///
/// This node comes first and unconditionally: it writes every local hit, every backend
/// read-through and every partial edge window whatever the ring says. The rest are the chunks'
/// holders — which, since ADR-0030's remote half landed, really do write into the client's window
/// themselves (`proxy::PacerProxy::rdma_into_client_token`), so this is live work rather than
/// pre-warming.
///
/// The per-chunk source list is [`chunk_sources`] — the same function the read path selects with
/// — so this predicts the set the read will really use rather than a second guess at it.
fn holder_names(
    cluster: &Cluster,
    chunk: &ChunkConfig,
    object_key: &str,
    windows: std::ops::Range<u64>,
    cap: usize,
) -> Vec<String> {
    let mut names = vec![cluster.local_node.clone()];
    for idx in windows {
        if names.len() >= cap {
            break;
        }
        let chunk_key = chunk.chunk_key(object_key, idx);
        for source in chunk_sources(cluster, &chunk_key) {
            push_unique(&mut names, source.name(), cap);
        }
    }
    names
}

/// Append `name` unless it is already listed or the cap is reached.
///
/// A linear scan because the list is at most `cap` — the concurrency bound, tens of entries —
/// so a set would cost more in allocation than the scan saves, and would lose the first-seen
/// order this node depends on being first in.
fn push_unique(names: &mut Vec<String>, name: &str, cap: usize) {
    if names.len() < cap && !names.iter().any(|n| n == name) {
        names.push(name.to_owned());
    }
}

/// Turn holder node names into encoded announce messages, dropping any node this daemon cannot
/// name endpoints for.
///
/// A node is dropped rather than reported empty when the handshake has not negotiated it yet — a
/// legitimate state (the sweep is periodic, and a peer may be gRPC-only) whose correct client
/// behaviour is identical to not being told about it: announce covers it.
#[cfg(feature = "efa")]
async fn endpoints_of(cluster: &Cluster, names: &[String]) -> Vec<HolderEndpoints> {
    let Some(efa) = cluster.efa.as_ref() else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let is_local = name == &cluster.local_node;
        let rails = if is_local {
            match efa.local_announced_rails() {
                Ok(rails) => rails,
                // A rail with no GID is a bring-up failure, not a per-peer state: with no local
                // endpoints there is nothing to prime for the writer that does most of the
                // writing, so the honest answer is an empty document.
                Err(e) => {
                    tracing::debug!(
                        error = %format!("{e:#}"),
                        "this node cannot name its own rails; answering with no endpoints"
                    );
                    return Vec::new();
                }
            }
        } else {
            efa.peer_announced_rails(name).await
        };
        // `encode` refuses an empty list — a message naming zero rails would decode to one that
        // installs nobody — so an un-negotiated peer is simply omitted.
        if let Ok(announce) = pacer_transport::announce::encode(&rails) {
            out.push(HolderEndpoints {
                node: name.clone(),
                local: is_local,
                announce,
            });
        }
    }
    out
}

/// gRPC-only counterpart: there is no RDMA plane, so no endpoint exists to name and the answer
/// is an empty list with `"plane":"none"` — which tells a client there is nothing to prime, as
/// opposed to telling it this daemon does not know how.
#[cfg(not(feature = "efa"))]
async fn endpoints_of(_cluster: &Cluster, _names: &[String]) -> Vec<HolderEndpoints> {
    Vec::new()
}

/// Answer one endpoint query: `(response, outcome label, nodes named)`.
///
/// `object_key` must already be derived from the **real** bucket (an alias resolved), because
/// chunk keys are, and a prediction over the alias would name a different key space entirely.
///
/// # Errors
///
/// Only a marker value that does not parse, which is `InvalidRequest` — a client-side bug that a
/// body fallback would not help, and the same treatment a malformed target descriptor gets.
pub async fn answer(
    cluster: Option<&Cluster>,
    chunk: &ChunkConfig,
    object_key: &str,
    raw: &str,
    parallelism: usize,
) -> S3Result<(S3Response<dto::GetObjectOutput>, &'static str, usize)> {
    let ask = EndpointsAsk::parse(raw)
        .map_err(|why| s3s::s3_error!(InvalidRequest, "{GET_ENDPOINTS_HEADER}: {why}"))?;
    let cap = max_endpoint_nodes(parallelism);
    let holders = match cluster {
        Some(cluster) => {
            let windows = endpoint_windows(ask, chunk, cap);
            endpoints_of(
                cluster,
                &holder_names(cluster, chunk, object_key, windows, cap),
            )
            .await
        }
        // Single-node: no ring, no peers, and no EFA plane is built at all (see `main.rs`), so
        // there is nothing to name.
        None => Vec::new(),
    };
    let outcome = if holders.is_empty() {
        OUTCOME_NO_PLANE
    } else {
        OUTCOME_SERVED
    };
    Ok((response(&document(&holders)), outcome, holders.len()))
}

/// The JSON document a client parses.
fn document(holders: &[HolderEndpoints]) -> String {
    let plane = if holders.is_empty() {
        PLANE_NONE
    } else {
        PLANE_EFA
    };
    let mut json = String::with_capacity(holders.len() * 128);
    let _ = write!(
        json,
        "{{\"version\":{ENDPOINTS_VERSION},\"plane\":\"{plane}\",\"nodes\":["
    );
    for (i, holder) in holders.iter().enumerate() {
        if i > 0 {
            json.push(',');
        }
        // The announce bytes travel as lowercase hex rather than base64: `bytes.fromhex` is in
        // Python's standard library and a C++ port is a lookup table, whereas base64 would add a
        // dependency to the daemon for a payload measured in kilobytes.
        let _ = write!(
            json,
            "{{\"node\":\"{}\",\"local\":{},\"announce\":\"{}\"}}",
            escape(&holder.node),
            holder.local,
            hex(&holder.announce),
        );
    }
    json.push_str("]}");
    json
}

/// Wrap the document as the GET's response: a 200 whose body is JSON, marked so a client can
/// prove it is not looking at object bytes.
///
/// Deliberately carries **no** `ETag`, `Last-Modified` or `Accept-Ranges`: this node did not
/// resolve the object's header (that is the point — see the module doc), so it knows none of
/// them, and a stale or invented one is worse than an absent one. `Content-Range` is absent too,
/// which is what distinguishes this 200 from the 206 an ordinary ranged read answers.
fn response(json: &str) -> S3Response<dto::GetObjectOutput> {
    let mut resp = S3Response::new(dto::GetObjectOutput {
        content_length: Some(json.len() as i64),
        content_type: Some(CONTENT_TYPE_JSON.to_owned()),
        body: Some(dto::StreamingBlob::from(s3s::Body::from(json.to_owned()))),
        ..Default::default()
    });
    resp.headers.insert(
        hyper::header::HeaderName::from_static(ENDPOINTS_HEADER),
        hyper::header::HeaderValue::from(ENDPOINTS_VERSION),
    );
    resp
}

/// Lowercase hex, so the payload needs no decoder on either side beyond a lookup.
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Escape a string for a JSON string literal.
///
/// Hand-written because the daemon has no JSON serializer and this document is a fixed shape of
/// integers, booleans and two kinds of string; pulling in `serde_json` for it would add a
/// dependency to answer a query whose whole payload is hex. Everything below `0x20` is escaped —
/// a control character in a K8s node name is not expected, but an unescaped one would produce a
/// document no client can parse, which reads as "the daemon is broken" rather than "that name is
/// odd".
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        document, endpoint_windows, escape, hex, max_endpoint_nodes, response, EndpointsAsk,
        HolderEndpoints, MAX_ENDPOINT_NODES,
    };
    use pacer_cache::chunk::ChunkConfig;

    /// 16 MiB, the shipped chunk size, so the window arithmetic below is the one production runs.
    const CHUNK: u64 = 16 << 20;

    fn chunk() -> ChunkConfig {
        ChunkConfig::new(CHUNK)
    }

    /// The grammar, including the shape a whole-object read sends (nothing at all).
    #[test]
    fn the_marker_value_parses() {
        assert_eq!(EndpointsAsk::parse("").unwrap(), EndpointsAsk::default());
        assert_eq!(
            EndpointsAsk::parse("offset=1048576").unwrap(),
            EndpointsAsk {
                offset: 1_048_576,
                length: None
            }
        );
        assert_eq!(
            EndpointsAsk::parse("offset=1048576;length=8388608").unwrap(),
            EndpointsAsk {
                offset: 1_048_576,
                length: Some(8_388_608)
            }
        );
        // Order-independent, and whitespace-tolerant: a header value picks both up.
        assert_eq!(
            EndpointsAsk::parse("length=16 ; offset=32").unwrap(),
            EndpointsAsk {
                offset: 32,
                length: Some(16)
            }
        );
    }

    /// A value the shim built wrong is named, not silently answered for the wrong windows — and
    /// an unknown FIELD is refused rather than ignored, because the whole value is one contract
    /// and a disagreement about the grammar means the offset may not mean what it says.
    #[test]
    fn an_unusable_marker_value_says_why() {
        for raw in [
            "offset",
            "offset=",
            "offset=wat",
            "offset=-1",
            "length=1.5",
            "first=0",
            "offset=0;stride=4",
        ] {
            let err = EndpointsAsk::parse(raw).expect_err(&format!("{raw:?} should be refused"));
            assert!(!err.is_empty(), "{raw:?} must say why");
        }
    }

    /// The bound is the concurrency bound, and it is what stops a 1000-node fleet's worth of
    /// handles: a whole-object read of any size looks at exactly `cap` windows, never more.
    #[test]
    fn a_whole_object_read_looks_at_the_bound_and_no_further() {
        let windows = endpoint_windows(EndpointsAsk::default(), &chunk(), 64);
        assert_eq!(windows, 0..64);
        // And from the middle of an object, the same width from where the read starts.
        let from_middle = endpoint_windows(
            EndpointsAsk {
                offset: 100 * CHUNK,
                length: None,
            },
            &chunk(),
            64,
        );
        assert_eq!(from_middle, 100..164);
    }

    /// A short read names only its own windows. The alternative — always answering for the whole
    /// object — names holders the read will never be written by, and each one costs an
    /// `ibv_create_ah` and a slot in the client's bounded cache.
    #[test]
    fn a_short_read_names_only_its_own_windows() {
        // Exactly two chunks.
        assert_eq!(
            endpoint_windows(
                EndpointsAsk {
                    offset: 0,
                    length: Some(2 * CHUNK)
                },
                &chunk(),
                64
            ),
            0..2
        );
        // One byte still touches one chunk, never zero.
        assert_eq!(
            endpoint_windows(
                EndpointsAsk {
                    offset: 0,
                    length: Some(1)
                },
                &chunk(),
                64
            ),
            0..1
        );
        assert_eq!(
            endpoint_windows(
                EndpointsAsk {
                    offset: 0,
                    length: Some(0)
                },
                &chunk(),
                64
            ),
            0..1,
            "a zero-length read must not produce an EMPTY window range, which would name only \
             this node"
        );
        // A range starting and ending mid-chunk touches both.
        assert_eq!(
            endpoint_windows(
                EndpointsAsk {
                    offset: CHUNK / 2,
                    length: Some(CHUNK)
                },
                &chunk(),
                64
            ),
            0..2
        );
        // And a read LONGER than the bound is still truncated to it.
        assert_eq!(
            endpoint_windows(
                EndpointsAsk {
                    offset: 0,
                    length: Some(1000 * CHUNK)
                },
                &chunk(),
                8
            ),
            0..8
        );
    }

    /// Arithmetic near the ends must not panic or wrap: an offset a client got wrong is a
    /// degraded prediction, never a crashed daemon on the read path.
    #[test]
    fn extreme_asks_saturate_rather_than_wrap() {
        let windows = endpoint_windows(
            EndpointsAsk {
                offset: u64::MAX,
                length: Some(u64::MAX),
            },
            &chunk(),
            64,
        );
        assert!(windows.start <= windows.end, "{windows:?}");
        let capped = endpoint_windows(EndpointsAsk::default(), &chunk(), usize::MAX);
        assert!(capped.start <= capped.end, "{capped:?}");
    }

    /// The cap is never zero, or the answer would name nothing at all — including this node,
    /// which is the writer that must be primed.
    #[test]
    fn the_node_cap_is_never_zero() {
        assert_eq!(max_endpoint_nodes(0), 1);
        assert_eq!(max_endpoint_nodes(64), 64);
        assert_eq!(
            MAX_ENDPOINT_NODES, 64,
            "the documented bound must equal the shipped delivery.parallelism"
        );
    }

    /// The exact document a client parses, for a two-node answer — the one shape a Rust test can
    /// pin that an integration test cannot, because naming a *non-empty* endpoint set needs an
    /// RDMA plane.
    ///
    /// Asserted literally rather than by `contains`, and that is the point: the shim decodes
    /// `nodes[].announce` with `bytes.fromhex` and gates on `version`, so a renamed field or a
    /// changed encoding is a silent loss of priming — the shim would find nothing to prime and
    /// fall back exactly as it does against an old daemon. The counterpart fixture is
    /// `clients/python/test_pacer_nic_preflight.py::document`, and these two are what keep the
    /// envelope's two ends in step.
    #[test]
    fn the_document_a_client_parses_is_exactly_this() {
        let rails = |gid: u8, qpn: u32| {
            pacer_transport::announce::encode(&[pacer_transport::announce::AnnouncedRail {
                gid: [gid; 16],
                qpn,
                rail: 0,
            }])
            .expect("one rail encodes")
        };
        let json = document(&[
            HolderEndpoints {
                node: "ip-10-0-1-5".to_owned(),
                local: true,
                announce: rails(0xab, 16_384),
            },
            HolderEndpoints {
                node: "ip-10-0-2-9".to_owned(),
                local: false,
                announce: rails(0xcd, 49_153),
            },
        ]);
        assert_eq!(
            json,
            concat!(
                r#"{"version":1,"plane":"efa","nodes":["#,
                r#"{"node":"ip-10-0-1-5","local":true,"announce":"0101"#,
                "abababababababababababababababab", // the GID, 16 bytes
                "00004000",                         // QPN 16384
                r#"0000"},"#,                       // rail 0
                r#"{"node":"ip-10-0-2-9","local":false,"announce":"0101"#,
                "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
                "0000c001", // QPN 49153
                r#"0000"}]}"#,
            )
        );
    }

    /// An empty set is `plane: "none"` and an empty array — a *defined* answer meaning "nothing
    /// to prime here", which the shim must not confuse with a daemon that cannot answer.
    #[test]
    fn no_endpoints_is_a_defined_answer_not_an_error() {
        assert_eq!(document(&[]), r#"{"version":1,"plane":"none","nodes":[]}"#);
    }

    /// The answer must be legible as "not your object": marked, JSON, and carrying none of the
    /// object metadata this node deliberately never looked up.
    #[test]
    fn the_answer_is_marked_and_carries_no_object_metadata() {
        let json = document(&[]);
        let resp = response(&json);
        assert_eq!(
            resp.headers
                .get(super::ENDPOINTS_HEADER)
                .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned()),
            Some(super::ENDPOINTS_VERSION.to_string())
        );
        assert_eq!(resp.output.content_length, Some(json.len() as i64));
        assert_eq!(
            resp.output.content_type.as_deref(),
            Some("application/json")
        );
        assert!(
            resp.output.e_tag.is_none()
                && resp.output.last_modified.is_none()
                && resp.output.accept_ranges.is_none()
                && resp.output.content_range.is_none(),
            "no object header was resolved, so none of these are known — and a 206's \
             Content-Range in particular must be absent"
        );
    }

    /// Hex is what the client decodes with `bytes.fromhex`, so it must be lowercase, unpadded
    /// and two characters per byte.
    #[test]
    fn hex_is_lowercase_and_two_per_byte() {
        assert_eq!(hex(&[0x00, 0x01, 0x0f, 0xa0, 0xff]), "00010fa0ff");
        assert_eq!(hex(&[]), "");
    }

    /// A node name is escaped, so an odd one produces a document a client can still parse rather
    /// than one that reads as a broken daemon.
    #[test]
    fn strings_are_escaped_for_json() {
        assert_eq!(escape("ip-10-0-1-5"), "ip-10-0-1-5");
        assert_eq!(escape("a\"b\\c"), "a\\\"b\\\\c");
        assert_eq!(escape("a\nb"), "a\\nb");
        assert_eq!(escape("a\u{1}b"), "a\\u0001b");
    }
}
