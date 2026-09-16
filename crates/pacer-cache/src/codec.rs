//! The disk tier's wire format for a [`CacheValue`], hand-written so a promoted
//! chunk's bytes land in an ADR-0028 slab frame instead of on the heap.
//!
//! # Why not the serde derive
//!
//! foyer's `Code` trait is what the disk tier actually serializes through, and
//! with foyer's `serde` feature there is a blanket
//! `impl<T: Serialize + DeserializeOwned> Code for T` that routes it via bincode.
//! That is what [`CacheValue`] used, and it decodes a chunk body by allocating a
//! `Vec<u8>` and copying foyer's region buffer into it — so a chunk promoted from
//! NVMe comes back **on the heap**, and the holder serving it stages a copy, with
//! the slab sitting fully registered and unused (ADR-0028 § "a promoted chunk is
//! NOT frame-backed").
//!
//! `Code::decode` takes a `&mut impl std::io::Read` — for an uncompressed entry,
//! a cursor over foyer's region buffer. So the fix is to read the body's bytes
//! *directly into a claimed frame*: `read_exact` replaces `Vec` + copy, and the
//! copy count is unchanged. There is no serde equivalent, because `Deserialize`
//! has nowhere to carry the slab; hence a hand-written impl, and hence
//! [`CacheValue`] must not derive `Serialize`/`Deserialize` (the blanket impl
//! would overlap and coherence would reject this one).
//!
//! # The format is bincode's, deliberately
//!
//! Byte-for-byte what the derive produced, because foyer **reuses the cache
//! directory across restarts** (`pacer_daemon::main`): a format change would make
//! every entry written by a previous build decode as garbage — bytes that still
//! pass foyer's own integrity check, since they are intact, merely reinterpreted.
//! Silent corruption on rollout is a far worse failure than the copy this saves,
//! so the encoding is pinned and [`tests::the_format_is_byte_identical_to_bincode`]
//! holds it there against a mirror type that still derives serde.
//!
//! Concretely, bincode's legacy default (fixed-width ints, little-endian):
//! a `u32` variant index, then the variant's fields; a `Bytes`/`String` as a
//! `u64` length then raw bytes; an `Option` as a `u8` tag then the payload.
//!
//! # Extending it, without redefining it
//!
//! ADR-0032 § 7 adds an ETag witness to a chunk. The pinning above is exactly why
//! that arrives as a **new variant tag** ([`TAG_CHUNK_VERSIONED`]) rather than a
//! field appended to [`TAG_CHUNK`]: appending would re-point tag 1 at a different
//! layout, and every chunk a previous build wrote would then decode its body
//! length out of the witness's bytes — intact, integrity-checked, and wrong. With
//! a separate tag the two directions both degrade safely: an old entry decodes as
//! a chunk with no witness, and a new entry met by a rolled-back build takes the
//! unknown-tag arm, which every call site swallows as a cache miss. A chunk with
//! no witness still writes tag 1, so only write-path entries are even at risk.

use bytes::Bytes;
use foyer::{Code, Error, ErrorKind, Result};

use crate::chunk::{CachedChunk, ObjectHeader};
use crate::frames::decode_into_frame;
use crate::CacheValue;

/// bincode's variant index for [`CacheValue::Header`].
const TAG_HEADER: u32 = 0;
/// bincode's variant index for [`CacheValue::Chunk`] with no version witness —
/// the encoding the serde derive produced, and still what a read-fill chunk
/// writes.
const TAG_CHUNK: u32 = 1;
/// Tag for a [`CacheValue::Chunk`] carrying its object version's ETag
/// (ADR-0032 § 7).
///
/// A **new** tag, not an extra field appended to [`TAG_CHUNK`]: redefining tag 1
/// would make every entry a previous build wrote decode as garbage that still
/// passes foyer's integrity check — the silent corruption this module exists to
/// prevent. Compatible in both directions instead. An old entry decodes as a
/// chunk with no witness, and an entry written here, met by a rolled-back build,
/// falls into the unknown-tag arm below, which every call site swallows as a
/// cache miss (`if let Ok(Some(entry)) = self.cache.get(…)`).
const TAG_CHUNK_VERSIONED: u32 = 2;

/// Longest ETag this decoder will allocate for, guarding a corrupt length prefix
/// the same way [`MAX_DECODABLE_CHUNK`] guards a body. An S3 ETag is a quoted MD5
/// hex digest (34 bytes) or its composite multipart form `"<hex>-<parts>"`, so it
/// never approaches this; 256 separates "an unusual but real ETag" from "these
/// bytes are not an ETag" without becoming a limit anyone can reach.
const MAX_DECODABLE_ETAG: u64 = 256;

/// Width of bincode's enum variant index. Fixed at 4 bytes by its legacy default
/// config (the one `bincode::serialize_into` uses), not by the number of variants.
const TAG_BYTES: usize = 4;
/// Width of a bincode length prefix (`Bytes`, `String`, sequences).
const LEN_BYTES: usize = 8;
/// Width of a bincode `Option` discriminant.
const OPTION_TAG_BYTES: usize = 1;

/// Largest chunk body this decoder will allocate for, as a guard against a
/// corrupt or mis-framed length prefix turning into a multi-exabyte allocation.
/// 1 GiB is far above any workable `chunk_size` (the default is 16 MiB and the
/// largest ever benched is 64 MiB) and far below anything that could be a real
/// length, so it separates "an operator picked a big chunk size" from "these
/// bytes are not a chunk" without becoming a limit anyone can reach.
const MAX_DECODABLE_CHUNK: u64 = 1 << 30;

impl Code for CacheValue {
    /// Encode into foyer's write buffer, in the format the module doc pins.
    ///
    /// # Errors
    ///
    /// The writer failing (foyer's buffer being too small for the entry), or
    /// bincode failing to encode a header's small string fields.
    fn encode(&self, writer: &mut impl std::io::Write) -> Result<()> {
        match self {
            CacheValue::Header(header) => {
                write_all(writer, &TAG_HEADER.to_le_bytes())?;
                // Delegated: a header is metadata with `Option<String>` fields and
                // is never RDMA-sourced, so there is nothing to gain by
                // hand-rolling it — and delegating is what keeps the format
                // identical to the derive's by construction rather than by review.
                bincode::serialize_into(writer, header).map_err(Error::bincode_error)
            }
            // An unversioned chunk keeps tag 1 and its exact pre-ADR-0032 bytes,
            // so only a write-path chunk ever writes the new tag — which bounds
            // what a rollback could fail to read to write-path entries alone.
            CacheValue::Chunk(chunk) => match &chunk.e_tag {
                None => {
                    write_all(writer, &TAG_CHUNK.to_le_bytes())?;
                    write_all(writer, &(chunk.body.len() as u64).to_le_bytes())?;
                    write_all(writer, &chunk.body)
                }
                Some(e_tag) => {
                    write_all(writer, &TAG_CHUNK_VERSIONED.to_le_bytes())?;
                    // Witness BEFORE the body, so the body stays the last read —
                    // that is what lets `decode` land it straight in a frame
                    // without buffering anything after it.
                    write_all(writer, &(e_tag.len() as u64).to_le_bytes())?;
                    write_all(writer, e_tag.as_bytes())?;
                    write_all(writer, &(chunk.body.len() as u64).to_le_bytes())?;
                    write_all(writer, &chunk.body)
                }
            },
        }
    }

    /// Decode from foyer's region buffer. A chunk body is read straight into a
    /// registered slab frame when this node has one ([`decode_into_frame`]).
    ///
    /// # Errors
    ///
    /// A truncated entry (the reader ending mid-field), an unknown variant tag or
    /// an implausible body length — both of which mean these bytes are not a
    /// `CacheValue` — or bincode failing on a header.
    fn decode(reader: &mut impl std::io::Read) -> Result<Self> {
        let mut tag = [0u8; TAG_BYTES];
        read_exact(reader, &mut tag)?;
        match u32::from_le_bytes(tag) {
            TAG_HEADER => Ok(CacheValue::Header(
                bincode::deserialize_from::<_, ObjectHeader>(reader)
                    .map_err(Error::bincode_error)?,
            )),
            TAG_CHUNK => Ok(CacheValue::Chunk(CachedChunk::new(read_body(reader)?))),
            TAG_CHUNK_VERSIONED => {
                // Witness first, body last — the order `encode` writes them, and
                // the order that keeps the frame read at the end.
                let e_tag = read_e_tag(reader)?;
                Ok(CacheValue::Chunk(CachedChunk::versioned(
                    read_body(reader)?,
                    e_tag,
                )))
            }
            other => Err(Error::new(
                ErrorKind::Parse,
                format!(
                    "unknown CacheValue variant tag {other} (expected {TAG_HEADER} header, \
                     {TAG_CHUNK} chunk or {TAG_CHUNK_VERSIONED} versioned chunk) — a cache \
                     directory written by an incompatible build?"
                ),
            )),
        }
    }

    /// Serialized size, which foyer's engine selector uses to route an entry.
    /// Exact for a chunk (framing plus the body) and exact for a header too,
    /// since bincode can size it without allocating.
    fn estimated_size(&self) -> usize {
        TAG_BYTES
            + match self {
                CacheValue::Chunk(c) => {
                    // A versioned chunk pays a second length prefix plus the
                    // witness; `round_trips_both_variants` holds this exact.
                    let witness = c.e_tag.as_ref().map_or(0, |e_tag| LEN_BYTES + e_tag.len());
                    witness + LEN_BYTES + c.body.len()
                }
                // Falls back to the framing-only lower bound if sizing fails,
                // which only misroutes an entry between engines — never wrong
                // bytes, and never a panic (the derive's `unwrap()` here is a
                // hazard this impl deliberately does not copy).
                CacheValue::Header(h) => {
                    bincode::serialized_size(h).map_or(header_size_floor(), |n| n as usize)
                }
            }
    }
}

/// Lower bound on a header's encoded size: its one `u64` plus the discriminants
/// of its three `Option` fields. Only used if bincode declines to size a header,
/// which it does not for these field types.
fn header_size_floor() -> usize {
    LEN_BYTES + 3 * OPTION_TAG_BYTES
}

/// Read a chunk body: a bounds-checked `u64` length, then the bytes **straight
/// into a registered slab frame** when this node has one ([`decode_into_frame`]).
///
/// Shared by both chunk tags so the frame path cannot drift between them — the
/// whole point of this codec is that a promoted chunk is frame-backed, and a
/// second hand-rolled copy of this would be exactly where that regresses.
///
/// # Errors
///
/// A truncated body, or a length above [`MAX_DECODABLE_CHUNK`] — which means
/// these bytes are not a chunk.
fn read_body(reader: &mut impl std::io::Read) -> Result<Bytes> {
    let len = read_len(reader)?;
    if len > MAX_DECODABLE_CHUNK {
        return Err(Error::new(
            ErrorKind::Parse,
            format!(
                "cached chunk claims {len} bytes, above the {MAX_DECODABLE_CHUNK}-byte \
                 decodable maximum — these bytes are not a chunk"
            ),
        ));
    }
    // `len` fits `usize` on any 64-bit target after the bound above; the cast is
    // the one place this decoder trusts the check.
    decode_into_frame(len as usize, |dst| read_exact(reader, dst))
}

/// Read a versioned chunk's ETag witness: a bounds-checked `u64` length, then
/// UTF-8 bytes. Heap-allocated rather than framed — it is tens of bytes, and the
/// slab is for chunk payloads.
///
/// # Errors
///
/// A truncated witness, a length above [`MAX_DECODABLE_ETAG`], or bytes that are
/// not UTF-8 — each meaning these bytes are not a versioned chunk.
fn read_e_tag(reader: &mut impl std::io::Read) -> Result<String> {
    let len = read_len(reader)?;
    if len > MAX_DECODABLE_ETAG {
        return Err(Error::new(
            ErrorKind::Parse,
            format!(
                "versioned chunk claims a {len}-byte ETag, above the \
                 {MAX_DECODABLE_ETAG}-byte maximum — these bytes are not a versioned chunk"
            ),
        ));
    }
    let mut buf = vec![0u8; len as usize];
    read_exact(reader, &mut buf)?;
    String::from_utf8(buf).map_err(|e| {
        Error::new(
            ErrorKind::Parse,
            format!("versioned chunk's ETag is not UTF-8: {e}"),
        )
    })
}

/// Read one little-endian `u64` length prefix.
fn read_len(reader: &mut impl std::io::Read) -> Result<u64> {
    let mut len = [0u8; LEN_BYTES];
    read_exact(reader, &mut len)?;
    Ok(u64::from_le_bytes(len))
}

/// `write_all`, with foyer's error mapping. A one-line helper because the mapping
/// is what foyer's `Code` doc requires and repeating it four times invites one
/// site to drift.
fn write_all(writer: &mut impl std::io::Write, buf: &[u8]) -> Result<()> {
    writer.write_all(buf).map_err(Error::io_error)
}

/// `read_exact`, with foyer's error mapping (see [`write_all`]).
fn read_exact(reader: &mut impl std::io::Read, buf: &mut [u8]) -> Result<()> {
    reader.read_exact(buf).map_err(Error::io_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde::{Deserialize, Serialize};

    /// What [`CacheValue`] looked like while it derived serde, kept as the
    /// compatibility oracle. If this and `CacheValue` ever disagree, entries
    /// written by a pre-ADR-0028 build stop decoding — silently, since intact
    /// bytes pass foyer's integrity check either way.
    #[derive(Serialize, Deserialize)]
    enum MirrorValue {
        Header(ObjectHeader),
        Chunk(MirrorChunk),
    }

    /// `CachedChunk` as the derive saw it (one `Bytes` field).
    #[derive(Serialize, Deserialize)]
    struct MirrorChunk {
        body: Bytes,
    }

    fn header() -> ObjectHeader {
        ObjectHeader::new(
            1 << 30,
            Some("\"deadbeef\"".into()),
            Some("application/octet-stream".into()),
            Some(1_700_000_000),
        )
    }

    /// Bodies with a length that is not a round number and bytes that would
    /// expose an off-by-one in the framing.
    fn body() -> Bytes {
        Bytes::from((0u32..1000).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
    }

    /// The load-bearing compatibility test: our encoding of each variant is
    /// byte-identical to what bincode produced for the derived enum, so a cache
    /// directory survives the upgrade that introduced this codec.
    #[test]
    fn the_format_is_byte_identical_to_bincode() {
        let mine = |v: &CacheValue| {
            let mut buf = Vec::new();
            v.encode(&mut buf).unwrap();
            buf
        };
        assert_eq!(
            mine(&CacheValue::Header(header())),
            bincode::serialize(&MirrorValue::Header(header())).unwrap(),
            "header framing drifted from bincode's"
        );
        assert_eq!(
            mine(&CacheValue::Chunk(CachedChunk::new(body()))),
            bincode::serialize(&MirrorValue::Chunk(MirrorChunk { body: body() })).unwrap(),
            "chunk framing drifted from bincode's"
        );
    }

    /// The other direction: bytes a previous build wrote still decode, which is
    /// what "reuses the cache directory across restarts" requires. The witness
    /// assertion is the ADR-0032 half — a pre-witness entry must come back as a
    /// chunk with *no* witness, never as one with a garbage witness read out of
    /// its body.
    #[test]
    fn bytes_written_by_the_derive_still_decode() {
        let encoded =
            bincode::serialize(&MirrorValue::Chunk(MirrorChunk { body: body() })).unwrap();
        let decoded = CacheValue::decode(&mut &encoded[..]).unwrap();
        assert_eq!(decoded.as_chunk().unwrap().body, body());
        assert!(
            decoded.as_chunk().unwrap().e_tag.is_none(),
            "a chunk written before the witness existed must decode without one"
        );

        let encoded = bincode::serialize(&MirrorValue::Header(header())).unwrap();
        let decoded = CacheValue::decode(&mut &encoded[..]).unwrap();
        assert_eq!(*decoded.as_header().unwrap(), header());
    }

    /// An unversioned chunk must still write tag 1, not merely decode as one:
    /// that is what confines a rollback's blast radius to write-path entries.
    #[test]
    fn an_unversioned_chunk_still_writes_the_original_tag() {
        let mut buf = Vec::new();
        CacheValue::Chunk(CachedChunk::new(body()))
            .encode(&mut buf)
            .unwrap();
        assert_eq!(
            u32::from_le_bytes(buf[..TAG_BYTES].try_into().unwrap()),
            TAG_CHUNK,
            "a chunk with no witness must keep the pre-ADR-0032 tag"
        );
    }

    /// A versioned chunk carries its witness across a round trip, under the new
    /// tag. Composite form (`-4`), because a scattered write always completes as
    /// a multipart upload and that is the ETag shape it will actually carry.
    #[test]
    fn a_versioned_chunk_round_trips_its_witness() {
        let e_tag = "\"66978554afd386101b895ddc4d606746-4\"".to_owned();
        let value = CacheValue::Chunk(CachedChunk::versioned(body(), e_tag.clone()));
        let mut buf = Vec::new();
        value.encode(&mut buf).unwrap();
        assert_eq!(
            u32::from_le_bytes(buf[..TAG_BYTES].try_into().unwrap()),
            TAG_CHUNK_VERSIONED
        );
        let back = CacheValue::decode(&mut &buf[..]).unwrap();
        let chunk = back.as_chunk().unwrap();
        assert_eq!(chunk.body, body());
        assert_eq!(chunk.e_tag.as_deref(), Some(e_tag.as_str()));
    }

    /// The rollback direction of ADR-0032 § 7's compatibility claim, and the half the
    /// tests above could not state: an entry *this* build writes for a versioned chunk
    /// must be **unreadable** to a build that predates the tag, never misread. Decoded
    /// by the pre-witness rules — the mirror enum, which has two variants — tag 2 is
    /// out of range and fails, which is the unknown-tag path every call site swallows
    /// as a cache miss. A miss costs warmth; a misread would serve an ETag's bytes as
    /// a body length.
    #[test]
    fn a_versioned_chunk_is_a_miss_on_a_build_that_predates_the_tag() {
        let mut buf = Vec::new();
        CacheValue::Chunk(CachedChunk::versioned(body(), "\"abc-2\"".into()))
            .encode(&mut buf)
            .unwrap();
        assert!(
            bincode::deserialize::<MirrorValue>(&buf).is_err(),
            "a pre-ADR-0032 build must reject the new tag, not interpret it"
        );
    }

    /// Round-trip through our own codec, including a zero-length body (the last
    /// chunk of an object whose length is a whole multiple of `chunk_size` never
    /// occurs, but a decoder that mishandles 0 would fail obscurely if it did)
    /// and both witness states, since `estimated_size` now branches on it.
    #[test]
    fn round_trips_both_variants() {
        for value in [
            CacheValue::Chunk(CachedChunk::new(body())),
            CacheValue::Chunk(CachedChunk::new(Bytes::new())),
            CacheValue::Chunk(CachedChunk::versioned(body(), "\"abc-2\"".into())),
            CacheValue::Chunk(CachedChunk::versioned(Bytes::new(), "\"abc-2\"".into())),
            CacheValue::Header(header()),
        ] {
            let mut buf = Vec::new();
            value.encode(&mut buf).unwrap();
            assert_eq!(
                buf.len(),
                value.estimated_size(),
                "estimated_size must match what encode writes"
            );
            let back = CacheValue::decode(&mut &buf[..]).unwrap();
            match (&value, &back) {
                (CacheValue::Chunk(a), CacheValue::Chunk(b)) => {
                    assert_eq!(a.body, b.body);
                    assert_eq!(a.e_tag, b.e_tag);
                }
                (CacheValue::Header(a), CacheValue::Header(b)) => assert_eq!(a, b),
                _ => panic!("decode returned the wrong variant"),
            }
        }
    }

    /// An implausible witness length is refused before it becomes an allocation,
    /// the same guard [`MAX_DECODABLE_CHUNK`] gives a body.
    #[test]
    fn an_implausible_etag_length_is_refused() {
        let mut buf = TAG_CHUNK_VERSIONED.to_le_bytes().to_vec();
        buf.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(CacheValue::decode(&mut &buf[..]).is_err());
    }

    /// A truncated witness is an error, not a short ETag — a versioned chunk that
    /// decoded to a partial witness would compare unequal to every real version
    /// and turn into a permanent miss once the check is on.
    #[test]
    fn a_truncated_etag_is_an_error() {
        let mut buf = TAG_CHUNK_VERSIONED.to_le_bytes().to_vec();
        buf.extend_from_slice(&32u64.to_le_bytes());
        buf.extend_from_slice(b"\"abc-2\"");
        assert!(CacheValue::decode(&mut &buf[..]).is_err());
    }

    /// A witness that is not UTF-8 is refused rather than lossily converted.
    #[test]
    fn a_non_utf8_etag_is_refused() {
        let mut buf = TAG_CHUNK_VERSIONED.to_le_bytes().to_vec();
        buf.extend_from_slice(&2u64.to_le_bytes());
        buf.extend_from_slice(&[0xff, 0xfe]);
        buf.extend_from_slice(&0u64.to_le_bytes());
        assert!(CacheValue::decode(&mut &buf[..]).is_err());
    }

    /// A tag from an incompatible format is rejected rather than interpreted.
    /// Without this, a foreign byte stream whose first four bytes happen to read
    /// as 1 would be decoded as a chunk of whatever the next eight bytes say.
    #[test]
    fn an_unknown_variant_tag_is_refused() {
        let mut buf = 7u32.to_le_bytes().to_vec();
        buf.extend_from_slice(&0u64.to_le_bytes());
        assert!(CacheValue::decode(&mut &buf[..]).is_err());
    }

    /// An implausible length is refused before it becomes an allocation.
    #[test]
    fn an_implausible_body_length_is_refused() {
        let mut buf = TAG_CHUNK.to_le_bytes().to_vec();
        buf.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(CacheValue::decode(&mut &buf[..]).is_err());
    }

    /// A truncated entry is an error, not a short body: a chunk that decoded to
    /// fewer bytes than its length claims would be served as a valid short read.
    #[test]
    fn a_truncated_body_is_an_error() {
        let mut buf = TAG_CHUNK.to_le_bytes().to_vec();
        buf.extend_from_slice(&64u64.to_le_bytes());
        buf.extend_from_slice(&[1u8; 32]);
        assert!(CacheValue::decode(&mut &buf[..]).is_err());
    }
}

/// Property tests for this module's `encode`/`decode` (T5): the hand-rolled
/// framing is what has to survive an arbitrary header/body/witness and an
/// arbitrarily truncated or corrupted buffer, since it is what a cache
/// directory written by *some* previous build hands back to `decode` — see the
/// module doc's compatibility argument, which the example-based tests above
/// already hold for the specific bytes they construct. These generalize the
/// same claims over generated input.
#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Default proptest case count for this module — see the identical const
    /// in `pacer-cache::proptests` (`src/lib.rs`) for why it is documentation,
    /// not an override: leaving `ProptestConfig` untouched keeps the
    /// `PROPTEST_CASES` env var in effect.
    #[allow(dead_code)]
    const PROPTEST_CASES: u32 = 256;

    /// Upper bound on generated chunk bodies. Bounded (rather than sized up to
    /// [`MAX_DECODABLE_CHUNK`]) purely so the suite stays fast — the framing
    /// this test exercises is identical at 4 KiB and at 1 GiB, since the codec
    /// never branches on body length except against that ceiling, which
    /// [`an_implausible_body_length_is_refused`] already pins directly.
    const MAX_PROPTEST_BODY_BYTES: usize = 4096;

    /// Upper bound on generated ETag witnesses, in **characters**, using only
    /// the printable-ASCII range below — so this is also a byte-length bound,
    /// comfortably under [`MAX_DECODABLE_ETAG`] (256).
    const MAX_PROPTEST_ETAG_CHARS: usize = 32;

    /// Trailing bytes generated after a real tag in
    /// [`corrupt_bytes_under_a_known_tag_never_panic`]. Small on purpose: that
    /// test is about the decoder's *reaction* to a bad length/body, not about
    /// exercising large allocations (already covered by the bounded-body
    /// property above and by [`MAX_DECODABLE_CHUNK`]/[`MAX_DECODABLE_ETAG`]).
    const MAX_FUZZ_TAIL_BYTES: usize = 64;

    /// A chunk body: arbitrary bytes, bounded by [`MAX_PROPTEST_BODY_BYTES`].
    fn body_strategy() -> impl Strategy<Value = Bytes> {
        prop::collection::vec(any::<u8>(), 0..=MAX_PROPTEST_BODY_BYTES).prop_map(Bytes::from)
    }

    /// A short, printable-ASCII string standing in for an ETag or a
    /// Content-Type — real values are already narrow (a quoted hex digest, a
    /// MIME type), and printable ASCII keeps 1 char == 1 byte so
    /// [`MAX_PROPTEST_ETAG_CHARS`] is also the byte-length bound this decoder
    /// actually enforces ([`MAX_DECODABLE_ETAG`]).
    fn short_string_strategy() -> impl Strategy<Value = String> {
        prop::collection::vec(proptest::char::range('!', '~'), 0..=MAX_PROPTEST_ETAG_CHARS)
            .prop_map(|chars| chars.into_iter().collect())
    }

    /// An arbitrary [`ObjectHeader`].
    fn object_header_strategy() -> impl Strategy<Value = ObjectHeader> {
        (
            any::<u64>(),
            prop::option::of(short_string_strategy()),
            prop::option::of(short_string_strategy()),
            prop::option::of(any::<i64>()),
        )
            .prop_map(
                |(object_len, e_tag, content_type, last_modified_epoch_secs)| {
                    ObjectHeader::new(object_len, e_tag, content_type, last_modified_epoch_secs)
                },
            )
    }

    /// An arbitrary [`CachedChunk`], unversioned or carrying an ETag witness
    /// (exercises both [`TAG_CHUNK`] and [`TAG_CHUNK_VERSIONED`]).
    fn cached_chunk_strategy() -> impl Strategy<Value = CachedChunk> {
        (body_strategy(), prop::option::of(short_string_strategy())).prop_map(|(body, e_tag)| {
            match e_tag {
                Some(e_tag) => CachedChunk::versioned(body, e_tag),
                None => CachedChunk::new(body),
            }
        })
    }

    /// An arbitrary [`CacheValue`], either variant.
    fn cache_value_strategy() -> impl Strategy<Value = CacheValue> {
        prop_oneof![
            object_header_strategy().prop_map(CacheValue::Header),
            cached_chunk_strategy().prop_map(CacheValue::Chunk),
        ]
    }

    proptest! {
        /// `encode` then `decode` reproduces the original value, and the bytes
        /// `encode` wrote are exactly as long as [`CacheValue::estimated_size`]
        /// declared — the property form of [`round_trips_both_variants`] above,
        /// over generated headers, bodies and witnesses instead of a fixed list.
        #[test]
        fn round_trips_arbitrary_values(value in cache_value_strategy()) {
            let mut buf = Vec::new();
            value.encode(&mut buf).unwrap();
            prop_assert_eq!(buf.len(), value.estimated_size(), "encoded length must match the declared framing");
            let back = CacheValue::decode(&mut &buf[..]).unwrap();
            match (&value, &back) {
                (CacheValue::Header(a), CacheValue::Header(b)) => prop_assert_eq!(a, b),
                (CacheValue::Chunk(a), CacheValue::Chunk(b)) => {
                    prop_assert_eq!(&a.body, &b.body);
                    prop_assert_eq!(&a.e_tag, &b.e_tag);
                }
                _ => prop_assert!(false, "decode returned the wrong variant"),
            }
        }

        /// Any strict prefix of a validly-encoded entry is an error, never a
        /// panic and never a (wrong) success — the property form of
        /// [`a_truncated_body_is_an_error`] and [`a_truncated_etag_is_an_error`]
        /// above, over generated values and every possible cut point rather than
        /// one hand-picked length.
        #[test]
        fn truncated_valid_entry_is_always_an_error(
            value in cache_value_strategy(),
            cut_fraction in 0.0f64..1.0,
        ) {
            let mut buf = Vec::new();
            value.encode(&mut buf).unwrap();
            // A cut at `buf.len()` itself is the whole valid buffer, not a
            // truncation — clamped below `buf.len()` so the slice handed to
            // `decode` is always genuinely incomplete (every encoding here is
            // at least TAG_BYTES + LEN_BYTES long, so this never underflows).
            let cut = ((buf.len() as f64) * cut_fraction) as usize;
            let cut = cut.min(buf.len() - 1);
            prop_assert!(CacheValue::decode(&mut &buf[..cut]).is_err());
        }

        /// Arbitrary bytes after a real (or plausibly-real) tag never panic,
        /// whatever length prefix or body/witness bytes follow — the property
        /// form of [`an_implausible_body_length_is_refused`],
        /// [`an_implausible_etag_length_is_refused`],
        /// [`a_non_utf8_etag_is_refused`] and [`an_unknown_variant_tag_is_refused`]
        /// combined, over random tails instead of one crafted one each.
        ///
        /// [`TAG_HEADER`] is deliberately excluded: that path delegates to
        /// `bincode::deserialize_from` (see the module doc's "why not the serde
        /// derive" — everything else here is hand-rolled specifically so it
        /// *can* bound an attacker-controlled length before allocating, which is
        /// not this suite's claim to make about a third-party decoder).
        #[test]
        fn corrupt_bytes_under_a_known_tag_never_panic(
            tag in prop_oneof![Just(TAG_CHUNK), Just(TAG_CHUNK_VERSIONED), Just(TAG_CHUNK_VERSIONED + 1)],
            tail in prop::collection::vec(any::<u8>(), 0..=MAX_FUZZ_TAIL_BYTES),
        ) {
            let mut buf = tag.to_le_bytes().to_vec();
            buf.extend_from_slice(&tail);
            // Ok or Err are both acceptable outcomes here; a panic is the only
            // one this test exists to catch.
            let _ = CacheValue::decode(&mut &buf[..]);
        }
    }
}
