//! The on-disk shape of one chunk slot's header (ADR-0033).
//!
//! A slot is a 4 KiB header **entry** plus a body, and the two are not adjacent: every
//! extent's header entries are collected into a region at its front, with the bodies after
//! them (`store::HEADER_REGION_BYTES` has the layout and the reason). The header being exactly
//! one page is what makes that work — both its own offset and every body offset stay
//! page-aligned, so `O_DIRECT` (and later a GDS read, planning/20) is reachable from this
//! layout without a format change.
//!
//! # What the header is for
//!
//! Not compression, not versioning of the *value* — [`SlotHeader::key`] is the point.
//! ADR-0033 removes foyer's unconditional XxHash64 from the read path, and the failure
//! mode that removal must not open is the one **this** code can create: an index that
//! names a slot holding some other key's bytes, after a restart, an eviction race, or a
//! scan that misread. So every read compares the key it asked for against the key the
//! slot claims, and a mismatch is a miss, never a serve. That check costs one 4 KiB
//! compare against a 1.335 ms device read.
//!
//! The body CRC is written unconditionally and *verified* only on request
//! (`verifyChunkBody`), which is what lets it be turned on without a migration. ADR-0033
//! § Integrity states the trade in full.
//!
//! [`SlotHeader::e_tag`] is a second, narrower kind of mismatch check: not "is this slot's
//! key what I asked for" but "is this slot's bytes the object version I still believe is
//! current." A restart trusts whatever the scan finds, and nothing before this format
//! version could tell a live chunk from one whose object was overwritten while this node
//! was unreachable (gh64). The header carries the witness so that question survives a
//! restart along with the bytes; whether anything reads and checks it is a caller
//! decision, not this module's.
//!
//! # Why an all-zero slot is meaningful
//!
//! A freshly created extent file reads zeros where nothing has been written — from a hole, or
//! from a preallocated-but-unwritten extent, which read the same — and zeros cannot match the
//! magic. That is how [`SlotHeader::parse`] distinguishes "free" from "occupied" during the
//! startup scan, with no separate free-list on disk to fall out of sync with the slots it
//! describes.

/// Bytes one header entry occupies. One page, so both it and every body offset stay
/// page-aligned — see the module docs for why that is a format decision and not an accident.
pub const SLOT_HEADER_BYTES: usize = 4 << 10;

/// Identifies a slot header written by this code. Impossible in a hole (a zeroed slot
/// reads 0, which is not this) and recognisable in a dump: stored little-endian, so the
/// first eight bytes of an occupied slot read `LS_RECAP`.
const SLOT_MAGIC: u64 = 0x5041_4345_525f_534c;

/// On-disk format version. Bumped only for a change the current reader cannot
/// understand; a mismatch makes the slot free rather than an error, so an older tier is
/// re-filled instead of refused.
///
/// **2**: header entries moved out from in front of each body into a per-extent region
/// (`store::HEADER_REGION_BYTES`). The header bytes themselves are unchanged, but a v1 tier's
/// headers are at v2 *body* offsets, so reading one as a header would be reading chunk bytes.
/// It cannot mis-serve — the key check still runs, and a chunk body will not carry the magic —
/// but the version makes "this tier is not for this reader" the stated reason rather than a
/// coincidence, and a v1 tier is therefore re-filled rather than mined for accidental hits.
///
/// **3**: the two bytes immediately after the key length — previously reserved and always
/// zero — are now the ETag witness's length, and the witness's bytes (when present) sit
/// right after the key (see [`OFF_ETAG_LEN`], [`MAX_ETAG_BYTES`]). A v2 header has no such
/// field at all, not merely an empty one, so this is a version bump rather than "zero
/// reserved bytes means no witness": a v2 tier's chunks carried no witness, but that was
/// true because nothing set one, not because this format said so, and a future format
/// must not inherit that as a reading rule by accident.
const SLOT_FORMAT_VERSION: u32 = 3;

/// Field offsets within the header. Written out rather than derived from a struct
/// layout: this is a byte format that outlives a rollout (a cache directory survives a
/// restart), so it must not move because a field was reordered in Rust.
const OFF_MAGIC: usize = 0;
/// Format version, immediately after the magic.
const OFF_VERSION: usize = 8;
/// Body length in bytes.
const OFF_BODY_LEN: usize = 12;
/// CRC32 of the body.
const OFF_BODY_CRC: usize = 16;
/// Key length in bytes.
const OFF_KEY_LEN: usize = 20;
/// ETag witness length in bytes, `0` for no witness. Before v3 these two bytes were
/// reserved and always zero, which is why an absent witness still round-trips through a
/// header written by that reading rule (see [`SLOT_FORMAT_VERSION`]).
const OFF_ETAG_LEN: usize = 22;
/// First byte of the key, immediately after the ETag length. Still 8-byte aligned, as
/// it was when these two bytes were merely reserved.
const OFF_KEY: usize = 24;

/// Longest ETag witness a slot can hold, stored immediately after the key (see
/// [`SlotHeader::e_tag`]). S3's own ETags are far shorter — a quoted 32-hex MD5 is 34
/// bytes, and a multipart or checksum-based ETag is not much longer — so this is
/// headroom, not a real limit, and exists to make a witness that could not possibly be
/// one of ours refuse loudly rather than be mined for an accidental key/witness split.
pub const MAX_ETAG_BYTES: usize = 256;

/// Longest key a slot can hold, from what is left of the page after the fixed fields and
/// the reserve for [`MAX_ETAG_BYTES`] (the witness sits right after the key, so the two
/// budgets must fit together). An S3 key is at most 1024 bytes and a bucket 63, plus the
/// `#{size}:{index}` suffix ADR-0015 appends — so this is still roughly 3x the largest
/// key that can occur, and the check exists to make a violation loud rather than to be
/// tight.
pub const MAX_KEY_BYTES: usize = SLOT_HEADER_BYTES - OFF_KEY - MAX_ETAG_BYTES;

/// What one occupied slot's header says about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotHeader {
    /// Chunk key these bytes belong to — the check that makes an index/slot skew a miss
    /// instead of a wrong serve.
    pub key: String,
    /// Body length. May be less than the tier's chunk size: an object's last chunk is
    /// short (ADR-0015 `chunk_bounds`).
    pub body_len: u32,
    /// CRC32 of exactly `body_len` body bytes.
    pub body_crc: u32,
    /// ETag of the object version [`Self::key`]'s bytes were witnessed against, or
    /// `None` for a chunk cached with no witness — the on-disk half of
    /// `pacer_cache::chunk::CachedChunk::e_tag` (ADR-0032 § 7, ADR-0044). Carrying this
    /// through the slot header is what lets a witness set before a restart still be
    /// there to check after one; nothing in this module reads or compares it.
    pub e_tag: Option<String>,
}

/// Why a slot's header did not yield a [`SlotHeader`].
///
/// Distinguished because they mean different things operationally: [`Self::Free`] is the
/// normal state of an unused slot and must not be counted as damage, while the other two
/// are damage and have their own counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    /// Never written, or written by an incompatible format version. Reusable as-is.
    Free,
    /// Magic matched but a field is impossible (key length past the page, body longer
    /// than the slot, an ETag length past the page). A bug or a torn write, not an
    /// empty slot.
    Corrupt,
    /// The key bytes are not UTF-8, so the slot cannot name a chunk key. An ETag witness
    /// that fails UTF-8 is reported as [`Self::Corrupt`] instead — it names no key, so
    /// there is nothing to be specific about the way this variant is for the key.
    KeyNotUtf8,
}

impl SlotHeader {
    /// Serialize into `dst`, which must be exactly [`SLOT_HEADER_BYTES`] and is fully
    /// overwritten (trailing bytes zeroed, so a reused slot cannot leak the tail of the
    /// key or ETag it held before).
    ///
    /// # Errors
    ///
    /// A key longer than [`MAX_KEY_BYTES`], or an ETag witness longer than
    /// [`MAX_ETAG_BYTES`].
    ///
    /// # Panics
    ///
    /// If `dst` is not exactly [`SLOT_HEADER_BYTES`] long — a caller-side invariant, not
    /// a runtime condition.
    pub fn write_to(&self, dst: &mut [u8]) -> anyhow::Result<()> {
        assert_eq!(
            dst.len(),
            SLOT_HEADER_BYTES,
            "a slot header buffer is exactly one page"
        );
        let key = self.key.as_bytes();
        if key.len() > MAX_KEY_BYTES {
            anyhow::bail!(
                "chunk key of {} bytes exceeds the {MAX_KEY_BYTES}-byte slot header budget",
                key.len()
            );
        }
        let etag = self.e_tag.as_deref().unwrap_or("").as_bytes();
        if etag.len() > MAX_ETAG_BYTES {
            anyhow::bail!(
                "chunk etag of {} bytes exceeds the {MAX_ETAG_BYTES}-byte slot header budget",
                etag.len()
            );
        }
        dst.fill(0);
        dst[OFF_MAGIC..OFF_VERSION].copy_from_slice(&SLOT_MAGIC.to_le_bytes());
        dst[OFF_VERSION..OFF_BODY_LEN].copy_from_slice(&SLOT_FORMAT_VERSION.to_le_bytes());
        dst[OFF_BODY_LEN..OFF_BODY_CRC].copy_from_slice(&self.body_len.to_le_bytes());
        dst[OFF_BODY_CRC..OFF_KEY_LEN].copy_from_slice(&self.body_crc.to_le_bytes());
        // Casts are bounded by the MAX_KEY_BYTES/MAX_ETAG_BYTES checks above, both well
        // under u16::MAX.
        #[allow(clippy::cast_possible_truncation)]
        dst[OFF_KEY_LEN..OFF_ETAG_LEN].copy_from_slice(&(key.len() as u16).to_le_bytes());
        #[allow(clippy::cast_possible_truncation)]
        dst[OFF_ETAG_LEN..OFF_KEY].copy_from_slice(&(etag.len() as u16).to_le_bytes());
        dst[OFF_KEY..OFF_KEY + key.len()].copy_from_slice(key);
        let etag_at = OFF_KEY + key.len();
        dst[etag_at..etag_at + etag.len()].copy_from_slice(etag);
        Ok(())
    }

    /// Parse a header out of `src`, or say why it is not one.
    ///
    /// `slot_body_capacity` is the tier's chunk size: a body longer than the slot can
    /// hold is [`SlotState::Corrupt`], because trusting it would make the reader issue a
    /// read past the slot into its neighbour.
    ///
    /// # Errors
    ///
    /// A [`SlotState`] saying why these bytes are not a header — [`SlotState::Free`] for
    /// an unwritten or older-format slot (the normal case for most of a tier, and not
    /// damage), or [`SlotState::Corrupt`]/[`SlotState::KeyNotUtf8`] for one that claims
    /// to be a header and is not.
    ///
    /// # Panics
    ///
    /// If `src` is shorter than [`SLOT_HEADER_BYTES`].
    pub fn parse(src: &[u8], slot_body_capacity: usize) -> Result<Self, SlotState> {
        assert!(
            src.len() >= SLOT_HEADER_BYTES,
            "a slot header buffer is at least one page"
        );
        let magic = u64::from_le_bytes(read_array(src, OFF_MAGIC));
        let version = u32::from_le_bytes(read_array(src, OFF_VERSION));
        // A hole reads zero, and an older format is re-fillable rather than broken.
        if magic != SLOT_MAGIC || version != SLOT_FORMAT_VERSION {
            return Err(SlotState::Free);
        }
        let body_len = u32::from_le_bytes(read_array(src, OFF_BODY_LEN));
        let body_crc = u32::from_le_bytes(read_array(src, OFF_BODY_CRC));
        let key_len = usize::from(u16::from_le_bytes(read_array(src, OFF_KEY_LEN)));
        if key_len == 0 || key_len > MAX_KEY_BYTES || body_len as usize > slot_body_capacity {
            return Err(SlotState::Corrupt);
        }
        let key = std::str::from_utf8(&src[OFF_KEY..OFF_KEY + key_len])
            .map_err(|_| SlotState::KeyNotUtf8)?;
        let etag_len = usize::from(u16::from_le_bytes(read_array(src, OFF_ETAG_LEN)));
        if etag_len > MAX_ETAG_BYTES {
            return Err(SlotState::Corrupt);
        }
        // In bounds unconditionally: key_len <= MAX_KEY_BYTES and etag_len <=
        // MAX_ETAG_BYTES, and MAX_KEY_BYTES was sized so the two budgets sum to exactly
        // what is left of the page after OFF_KEY — see MAX_KEY_BYTES.
        let etag_at = OFF_KEY + key_len;
        let e_tag = if etag_len == 0 {
            None
        } else {
            let etag = std::str::from_utf8(&src[etag_at..etag_at + etag_len])
                .map_err(|_| SlotState::Corrupt)?;
            Some(etag.to_owned())
        };
        Ok(Self {
            key: key.to_owned(),
            body_len,
            body_crc,
            e_tag,
        })
    }
}

/// Read a fixed-size little-endian field at `offset`.
///
/// A helper rather than inline `try_into().unwrap()` at five call sites: the slice is
/// bounds-checked by [`SlotHeader::parse`]'s length assertion, and every field here is
/// within one page of the start.
fn read_array<const N: usize>(src: &[u8], offset: usize) -> [u8; N] {
    src[offset..offset + N]
        .try_into()
        .expect("field lies within the asserted header page")
}

/// CRC32 of a chunk body, as stored in [`SlotHeader::body_crc`].
///
/// `crc32fast` and not XxHash64: it is already a workspace dependency (the write path
/// computes S3's CRC32 with it), it is SIMD-accelerated, and ADR-0033's whole point is
/// that the per-byte cost of the old codec was the wall.
#[must_use]
pub fn body_crc(body: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(body);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(key: &str, body_len: u32) -> SlotHeader {
        SlotHeader {
            key: key.to_owned(),
            body_len,
            body_crc: 0xdead_beef,
            e_tag: None,
        }
    }

    fn versioned_header(key: &str, body_len: u32, e_tag: &str) -> SlotHeader {
        SlotHeader {
            e_tag: Some(e_tag.to_owned()),
            ..header(key, body_len)
        }
    }

    /// A round trip must preserve every field. The base case, and the one that would
    /// break if an offset moved.
    #[test]
    fn round_trip_preserves_every_field() {
        let mut page = vec![0u8; SLOT_HEADER_BYTES];
        let want = header("bucket/key#16777216:42", 1 << 20);
        want.write_to(&mut page).unwrap();
        assert_eq!(SlotHeader::parse(&page, 16 << 20).unwrap(), want);
    }

    /// The witness is the new field v3 adds — its own round trip, both present and
    /// absent, rather than folding it into the base case above.
    #[test]
    fn round_trip_preserves_the_etag_witness() {
        let mut page = vec![0u8; SLOT_HEADER_BYTES];
        let want = versioned_header("bucket/key#16777216:42", 1 << 20, "\"66978554afd-4\"");
        want.write_to(&mut page).unwrap();
        let got = SlotHeader::parse(&page, 16 << 20).unwrap();
        assert_eq!(got, want);
        assert_eq!(got.e_tag.as_deref(), Some("\"66978554afd-4\""));
    }

    /// **Pins the byte format.** A cache directory outlives a rollout, so a reordered
    /// field or a changed endianness must fail here rather than silently orphan (or
    /// worse, misread) a warm tier. Mirrors the existing codec's format test.
    #[test]
    fn byte_format_is_pinned() {
        let mut page = vec![0u8; SLOT_HEADER_BYTES];
        header("b/k#100:7", 300).write_to(&mut page).unwrap();
        assert_eq!(&page[0..8], b"LS_RECAP", "magic, little-endian");
        assert_eq!(&page[8..12], &3u32.to_le_bytes(), "format version");
        assert_eq!(&page[12..16], &300u32.to_le_bytes(), "body length");
        assert_eq!(&page[16..20], &0xdead_beefu32.to_le_bytes(), "body crc");
        assert_eq!(&page[20..22], &9u16.to_le_bytes(), "key length");
        assert_eq!(&page[22..24], &0u16.to_le_bytes(), "etag length: none set");
        assert_eq!(&page[24..33], b"b/k#100:7", "key bytes at offset 24");
        assert!(page[33..].iter().all(|&b| b == 0), "tail is zeroed");
    }

    /// Pins where the witness lands: right after the key, length-prefixed in the field
    /// v2 left reserved. A byte moving here would silently start reading key or witness
    /// bytes from the wrong place on every tier that predates the move.
    #[test]
    fn byte_format_is_pinned_with_etag() {
        let mut page = vec![0u8; SLOT_HEADER_BYTES];
        versioned_header("b/k#100:7", 300, "\"abc-2\"")
            .write_to(&mut page)
            .unwrap();
        assert_eq!(&page[20..22], &9u16.to_le_bytes(), "key length");
        assert_eq!(&page[22..24], &7u16.to_le_bytes(), "etag length");
        assert_eq!(&page[24..33], b"b/k#100:7", "key bytes at offset 24");
        assert_eq!(
            &page[33..40],
            b"\"abc-2\"",
            "etag bytes immediately after the key"
        );
        assert!(page[40..].iter().all(|&b| b == 0), "tail is zeroed");
    }

    /// An untouched slot — the normal state of most of a fresh tier — must read as
    /// Free, which is what lets the startup scan build the free list with no on-disk
    /// bookkeeping.
    #[test]
    fn a_hole_reads_as_free() {
        let page = vec![0u8; SLOT_HEADER_BYTES];
        assert_eq!(SlotHeader::parse(&page, 16 << 20), Err(SlotState::Free));
    }

    /// A future format must be Free (re-fillable), not Corrupt: an operator rolling
    /// back should get a cold tier, not a tier full of counted damage.
    #[test]
    fn an_unknown_version_is_free_not_corrupt() {
        let mut page = vec![0u8; SLOT_HEADER_BYTES];
        header("b/k#100:0", 8).write_to(&mut page).unwrap();
        page[OFF_VERSION..OFF_VERSION + 4].copy_from_slice(&99u32.to_le_bytes());
        assert_eq!(SlotHeader::parse(&page, 16 << 20), Err(SlotState::Free));
    }

    /// A v2 header (no witness field at all) must read as Free, not as a v3 header with
    /// an all-zero — and therefore absent — witness. The version check has to fire
    /// before anything looks at the bytes v3 repurposed.
    #[test]
    fn a_v2_header_is_free_not_a_witness_less_v3_header() {
        let mut page = vec![0u8; SLOT_HEADER_BYTES];
        header("b/k#100:0", 8).write_to(&mut page).unwrap();
        page[OFF_VERSION..OFF_VERSION + 4].copy_from_slice(&2u32.to_le_bytes());
        assert_eq!(SlotHeader::parse(&page, 16 << 20), Err(SlotState::Free));
    }

    /// A body length past the slot must be refused. Trusting it would make the reader
    /// read into the NEXT slot and serve a neighbour's bytes — the exact class of bug
    /// the header check exists to prevent.
    #[test]
    fn a_body_longer_than_the_slot_is_corrupt() {
        let mut page = vec![0u8; SLOT_HEADER_BYTES];
        header("b/k#100:0", 32 << 20).write_to(&mut page).unwrap();
        assert_eq!(
            SlotHeader::parse(&page, 16 << 20),
            Err(SlotState::Corrupt),
            "16 MiB slot must refuse a 32 MiB body"
        );
    }

    #[test]
    fn a_zero_or_oversized_key_length_is_corrupt() {
        let mut page = vec![0u8; SLOT_HEADER_BYTES];
        header("b/k#100:0", 8).write_to(&mut page).unwrap();
        page[OFF_KEY_LEN..OFF_KEY_LEN + 2].copy_from_slice(&0u16.to_le_bytes());
        assert_eq!(SlotHeader::parse(&page, 16 << 20), Err(SlotState::Corrupt));
        // Cast is the point of the test: a length past the page must be refused.
        #[allow(clippy::cast_possible_truncation)]
        let too_long = (MAX_KEY_BYTES + 1) as u16;
        page[OFF_KEY_LEN..OFF_KEY_LEN + 2].copy_from_slice(&too_long.to_le_bytes());
        assert_eq!(SlotHeader::parse(&page, 16 << 20), Err(SlotState::Corrupt));
    }

    /// The same refusal as the key length, for the length field v3 adds.
    #[test]
    fn an_oversized_etag_length_is_corrupt() {
        let mut page = vec![0u8; SLOT_HEADER_BYTES];
        header("b/k#100:0", 8).write_to(&mut page).unwrap();
        #[allow(clippy::cast_possible_truncation)]
        let too_long = (MAX_ETAG_BYTES + 1) as u16;
        page[OFF_ETAG_LEN..OFF_ETAG_LEN + 2].copy_from_slice(&too_long.to_le_bytes());
        assert_eq!(SlotHeader::parse(&page, 16 << 20), Err(SlotState::Corrupt));
    }

    #[test]
    fn a_non_utf8_key_is_reported_as_such() {
        let mut page = vec![0u8; SLOT_HEADER_BYTES];
        header("b/k#100:0", 8).write_to(&mut page).unwrap();
        page[OFF_KEY] = 0xff;
        assert_eq!(
            SlotHeader::parse(&page, 16 << 20),
            Err(SlotState::KeyNotUtf8)
        );
    }

    /// An etag that fails UTF-8 is `Corrupt`, not `KeyNotUtf8` — it is not the key that
    /// is unreadable here, and the variant name says which one is.
    #[test]
    fn a_non_utf8_etag_is_corrupt() {
        let mut page = vec![0u8; SLOT_HEADER_BYTES];
        versioned_header("b/k#100:0", 8, "\"abc-2\"")
            .write_to(&mut page)
            .unwrap();
        let etag_at = OFF_KEY + "b/k#100:0".len();
        page[etag_at] = 0xff;
        assert_eq!(SlotHeader::parse(&page, 16 << 20), Err(SlotState::Corrupt));
    }

    /// Writing must reject a key it cannot store, rather than truncate it — a truncated
    /// key would compare unequal on every later read and make the slot permanently
    /// unservable while still occupying capacity.
    #[test]
    fn an_oversized_key_is_refused_at_write_time() {
        let mut page = vec![0u8; SLOT_HEADER_BYTES];
        let key = "k".repeat(MAX_KEY_BYTES + 1);
        assert!(header(&key, 8).write_to(&mut page).is_err());
        // The largest key that fits must still work, so the bound is exact.
        let key = "k".repeat(MAX_KEY_BYTES);
        assert!(header(&key, 8).write_to(&mut page).is_ok());
        assert_eq!(SlotHeader::parse(&page, 16 << 20).unwrap().key, key);
    }

    /// The same exactness as the key bound, for the witness v3 adds. Truncating a
    /// witness would make it compare unequal to the version it was meant to name —
    /// worse than dropping it, since it looks like a witness for the wrong version.
    #[test]
    fn an_oversized_etag_is_refused_at_write_time() {
        let mut page = vec![0u8; SLOT_HEADER_BYTES];
        let etag = "e".repeat(MAX_ETAG_BYTES + 1);
        assert!(versioned_header("b/k#100:0", 8, &etag)
            .write_to(&mut page)
            .is_err());
        let etag = "e".repeat(MAX_ETAG_BYTES);
        let want = versioned_header("b/k#100:0", 8, &etag);
        assert!(want.write_to(&mut page).is_ok());
        assert_eq!(SlotHeader::parse(&page, 16 << 20).unwrap(), want);
    }

    /// A reused slot must not leak the tail of the key it held before: a longer key
    /// followed by a shorter one has to leave no trailing bytes of the first.
    #[test]
    fn rewriting_a_slot_zeroes_the_previous_key() {
        let mut page = vec![0u8; SLOT_HEADER_BYTES];
        header("bucket/a-very-long-previous-key#100:9", 8)
            .write_to(&mut page)
            .unwrap();
        header("b/k#100:0", 8).write_to(&mut page).unwrap();
        let parsed = SlotHeader::parse(&page, 16 << 20).unwrap();
        assert_eq!(parsed.key, "b/k#100:0");
        assert!(
            page[OFF_KEY + parsed.key.len()..].iter().all(|&b| b == 0),
            "no bytes of the previous key may survive"
        );
    }

    /// The same leak this module already guards against for the key, for the witness: a
    /// slot that drops its witness on rewrite must not leave the old one's bytes sitting
    /// (harmlessly unread, but not something this format should ever produce) past the
    /// new, shorter header.
    #[test]
    fn rewriting_a_slot_zeroes_the_previous_etag() {
        let mut page = vec![0u8; SLOT_HEADER_BYTES];
        versioned_header("b/k#100:0", 8, "\"a-very-long-previous-etag-4\"")
            .write_to(&mut page)
            .unwrap();
        header("b/k#100:0", 8).write_to(&mut page).unwrap();
        let parsed = SlotHeader::parse(&page, 16 << 20).unwrap();
        assert_eq!(parsed.e_tag, None);
        assert!(
            page[OFF_KEY + parsed.key.len()..].iter().all(|&b| b == 0),
            "no bytes of the previous etag may survive"
        );
    }

    #[test]
    fn body_crc_is_content_dependent() {
        assert_eq!(body_crc(b""), body_crc(b""));
        assert_ne!(body_crc(b"abc"), body_crc(b"abd"));
    }
}
