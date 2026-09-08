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
const SLOT_FORMAT_VERSION: u32 = 2;

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
/// First byte of the key. Leaves two reserved bytes after the key length, so the key
/// starts 8-byte aligned.
const OFF_KEY: usize = 24;

/// Longest key a slot can hold, from what is left of the page after the fixed fields.
/// An S3 key is at most 1024 bytes and a bucket 63, plus the `#{size}:{index}` suffix
/// ADR-0015 appends — so this is roughly 3.5x the largest key that can occur, and the
/// check exists to make a violation loud rather than to be tight.
pub const MAX_KEY_BYTES: usize = SLOT_HEADER_BYTES - OFF_KEY;

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
    /// than the slot). A bug or a torn write, not an empty slot.
    Corrupt,
    /// The key bytes are not UTF-8, so the slot cannot name a chunk key.
    KeyNotUtf8,
}

impl SlotHeader {
    /// Serialize into `dst`, which must be exactly [`SLOT_HEADER_BYTES`] and is fully
    /// overwritten (trailing bytes zeroed, so a reused slot cannot leak the tail of the
    /// key it held before).
    ///
    /// # Errors
    ///
    /// A key longer than [`MAX_KEY_BYTES`].
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
        dst.fill(0);
        dst[OFF_MAGIC..OFF_VERSION].copy_from_slice(&SLOT_MAGIC.to_le_bytes());
        dst[OFF_VERSION..OFF_BODY_LEN].copy_from_slice(&SLOT_FORMAT_VERSION.to_le_bytes());
        dst[OFF_BODY_LEN..OFF_BODY_CRC].copy_from_slice(&self.body_len.to_le_bytes());
        dst[OFF_BODY_CRC..OFF_KEY_LEN].copy_from_slice(&self.body_crc.to_le_bytes());
        // Cast is bounded by the MAX_KEY_BYTES check above, which is well under u16::MAX.
        #[allow(clippy::cast_possible_truncation)]
        dst[OFF_KEY_LEN..OFF_KEY_LEN + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
        dst[OFF_KEY..OFF_KEY + key.len()].copy_from_slice(key);
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
        Ok(Self {
            key: key.to_owned(),
            body_len,
            body_crc,
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

    /// **Pins the byte format.** A cache directory outlives a rollout, so a reordered
    /// field or a changed endianness must fail here rather than silently orphan (or
    /// worse, misread) a warm tier. Mirrors the existing codec's format test.
    #[test]
    fn byte_format_is_pinned() {
        let mut page = vec![0u8; SLOT_HEADER_BYTES];
        header("b/k#100:7", 300).write_to(&mut page).unwrap();
        assert_eq!(&page[0..8], b"LS_RECAP", "magic, little-endian");
        assert_eq!(&page[8..12], &2u32.to_le_bytes(), "format version");
        assert_eq!(&page[12..16], &300u32.to_le_bytes(), "body length");
        assert_eq!(&page[16..20], &0xdead_beefu32.to_le_bytes(), "body crc");
        assert_eq!(&page[20..22], &9u16.to_le_bytes(), "key length");
        assert_eq!(&page[22..24], &[0, 0], "reserved bytes stay zero");
        assert_eq!(&page[24..33], b"b/k#100:7", "key bytes at offset 24");
        assert!(page[33..].iter().all(|&b| b == 0), "tail is zeroed");
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

    #[test]
    fn body_crc_is_content_dependent() {
        assert_eq!(body_crc(b""), body_crc(b""));
        assert_ne!(body_crc(b"abc"), body_crc(b"abd"));
    }
}
