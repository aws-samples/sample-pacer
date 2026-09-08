//! The **announce** wire format: how a writer names itself to a client that has
//! never heard of it (ADR-0030 § Decision point 2).
//!
//! ## Why this exists
//!
//! EFA SRD is *reliable*, so the NIC receiving a one-sided WRITE has to send transport
//! ACKs back — which it can only do through an address-vector entry for the initiator.
//! A target holding no address handle for the writer therefore refuses the WRITE with
//! `EFA_IO_COMP_STATUS_REMOTE_ERROR_UNKNOWN_PEER`, and the bytes do not land. Measured
//! twice: cross-node (planning/09 finding 10) and, decisively, between two endpoints on
//! **one device**, where being the same device exempts nothing
//! (`bench/ladder/results/c2-loopback-gate.md`).
//!
//! Under ADR-0030 a *remote* hit is written by the **holder**, and a client cannot know
//! which holders those will be — the daemon owns placement. It does not need to: a
//! two-sided SEND *is* delivered to a target that has not inserted the sender, so a
//! writer can install itself in the client's address table with its own first packet
//! (`bench/ladder/results/c2-announce-gate.md`, L1.1-L1.3). This module is the message
//! that carries it.
//!
//! ## Why the endpoint travels in the payload
//!
//! The receive completion carries the sender's **`src_qp` but not its GID** — reaching
//! that needs `efadv_wc_read_sgid`, which rdma-core ships as a `static inline` over a
//! provider op and no Rust binding exposes. So the writer states its own address
//! explicitly, exactly as libfabric's RDM protocol does with its packet header.
//!
//! ## Why one message carries every rail
//!
//! A holder round-robins its rails to reach line rate, so a client would otherwise need
//! one announce per (holder, rail) pair. One message with every rail in it keeps that at
//! one SEND per (client, holder) pair however many rails the holder ends up using — the
//! shape the reference EFA implementation uses (`abcdabcd987/libfabric-efa-demo`
//! `src/15_lazy.cpp`, whose CONNECT carries `num_nets` addresses).
//!
//! ## The format is a contract with non-Rust code
//!
//! ADR-0031 makes the client half a NIXL backend plugin — C++ — so this layout is
//! mirrored by a decoder we do not compile. Hence: explicit offsets, big-endian
//! integers, a version byte, an exact-length check, and [`GOLDEN_ONE_RAIL`] as a fixed
//! byte vector any port can be checked against.
//!
//! ```text
//! offset  size  field
//! 0       1     version (ANNOUNCE_VERSION)
//! 1       1     rail count, 1..=MAX_ANNOUNCED_RAILS
//! then, per rail, in the writer's own rail order:
//! +0      16    GID, network byte order, as it appears on the wire
//! +16     4     QPN, big-endian
//! +20     2     rail index, big-endian (diagnostic: pairs a WRITE with a rail)
//! ```
//!
//! The message is deliberately not protobuf: it is posted from a registered buffer as
//! the payload of one SEND, so a fixed layout avoids a serializer in the data path and
//! keeps the C++ side to pointer arithmetic.
//!
//! That contract is also why this module sits **outside the `efa` feature gate**, as
//! [`crate::token`] does: the layout has to be nameable — and its tests have to run — in
//! every build, not only where libefa links.

use anyhow::{bail, ensure, Result};

/// Wire version. Bump on any layout change; a decoder must reject what it does not know
/// rather than guess, because a misread GID produces an address handle that silently
/// addresses nothing.
pub const ANNOUNCE_VERSION: u8 = 1;

/// Most rails one message may name.
///
/// A p5.48xlarge has 32 EFA devices, the largest shape this repo runs; 64 leaves room
/// for a bigger one without making the count field ambiguous. It is a *limit*, not an
/// expectation — a one-rail client is the ordinary case.
pub const MAX_ANNOUNCED_RAILS: usize = 64;

/// Bytes before the first rail entry: version + count.
const HEADER_BYTES: usize = 2;
/// Byte offset of the version.
const OFFSET_VERSION: usize = 0;
/// Byte offset of the rail count.
const OFFSET_COUNT: usize = 1;

/// Bytes per rail entry: GID + QPN + rail index.
const ENTRY_BYTES: usize = 22;
/// GID's offset and length within an entry.
const ENTRY_GID: std::ops::Range<usize> = 0..16;
/// QPN's offset and length within an entry (big-endian `u32`).
const ENTRY_QPN: std::ops::Range<usize> = 16..20;
/// Rail index's offset and length within an entry (big-endian `u16`).
const ENTRY_RAIL: std::ops::Range<usize> = 20..22;

/// Largest message this format can produce — the size a receiver's buffer must admit.
pub const ANNOUNCE_MAX_BYTES: usize = HEADER_BYTES + MAX_ANNOUNCED_RAILS * ENTRY_BYTES;

/// A one-rail message with GID `00 01 .. 0f`, QPN `0x4001` and rail 0.
///
/// Exists so the C++ decoder in the NIXL plugin can be checked against fixed bytes
/// rather than against a second implementation of the same arithmetic.
pub const GOLDEN_ONE_RAIL: [u8; HEADER_BYTES + ENTRY_BYTES] = [
    0x01, // version
    0x01, // one rail
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, // GID, bytes 0-7
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, // GID, bytes 8-15
    0x00, 0x00, 0x40, 0x01, // QPN 0x4001
    0x00, 0x00, // rail 0
];

/// One rail of a writer: everything a client needs to build an address handle for it.
///
/// A GID and a QPN, nothing else — the client never sends to this endpoint, it only has
/// to let its NIC ACK writes arriving from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AnnouncedRail {
    /// The rail's GID, in the byte order it appears on the wire (`ibverbs::Gid`'s raw
    /// form, so it round-trips through `Gid::from`/`Into<[u8; 16]>` unchanged).
    pub gid: [u8; 16],
    /// The queue pair number the writer posts from. **`0` is legitimate** — EFA firmware
    /// assigns from a small fixed set that includes it (A1 finding 11), so a decoder must
    /// not treat zero as unset.
    pub qpn: u32,
    /// The writer's own index for this rail. Diagnostic only: it lets a log line pair a
    /// WRITE with the rail that sent it, and a receiver never indexes anything by it.
    pub rail: u16,
}

/// Encode `rails` into one announce message.
///
/// # Errors
///
/// An empty list (a writer with no rails has nothing to announce, and a zero count would
/// decode to a message that installs nobody) or more than [`MAX_ANNOUNCED_RAILS`].
pub fn encode(rails: &[AnnouncedRail]) -> Result<Vec<u8>> {
    ensure!(!rails.is_empty(), "an announce must name at least one rail");
    ensure!(
        rails.len() <= MAX_ANNOUNCED_RAILS,
        "{} rails exceeds the {MAX_ANNOUNCED_RAILS}-rail announce limit",
        rails.len(),
    );
    let mut out = vec![0u8; HEADER_BYTES + rails.len() * ENTRY_BYTES];
    out[OFFSET_VERSION] = ANNOUNCE_VERSION;
    // The count fits by construction: MAX_ANNOUNCED_RAILS is far below u8::MAX.
    out[OFFSET_COUNT] = rails.len() as u8;
    for (i, rail) in rails.iter().enumerate() {
        let entry = &mut out[HEADER_BYTES + i * ENTRY_BYTES..][..ENTRY_BYTES];
        entry[ENTRY_GID].copy_from_slice(&rail.gid);
        entry[ENTRY_QPN].copy_from_slice(&rail.qpn.to_be_bytes());
        entry[ENTRY_RAIL].copy_from_slice(&rail.rail.to_be_bytes());
    }
    Ok(out)
}

/// Decode an announce message.
///
/// The length check is **exact**, not a lower bound: a message longer than its own count
/// implies the sender and this decoder disagree about the layout, and guessing which half
/// is right is how a wrong GID becomes an address handle that addresses nothing.
///
/// # Errors
///
/// A buffer too short for the header, a version this build does not know, a count of zero
/// or above [`MAX_ANNOUNCED_RAILS`], or a length that does not match the count exactly.
pub fn decode(bytes: &[u8]) -> Result<Vec<AnnouncedRail>> {
    ensure!(
        bytes.len() >= HEADER_BYTES,
        "announce is {} bytes, shorter than its {HEADER_BYTES}-byte header",
        bytes.len(),
    );
    let version = bytes[OFFSET_VERSION];
    if version != ANNOUNCE_VERSION {
        bail!("announce version {version} is not the {ANNOUNCE_VERSION} this build knows");
    }
    let count = bytes[OFFSET_COUNT] as usize;
    ensure!(count > 0, "announce names zero rails");
    ensure!(
        count <= MAX_ANNOUNCED_RAILS,
        "announce names {count} rails, above the {MAX_ANNOUNCED_RAILS} limit",
    );
    let want = HEADER_BYTES + count * ENTRY_BYTES;
    ensure!(
        bytes.len() == want,
        "announce names {count} rails so it must be {want} bytes, got {}",
        bytes.len(),
    );
    // `as_chunks`, not `chunks_exact`: the chunk width is a compile-time constant, so the
    // slices are `[u8; ENTRY_BYTES]` and the remainder is a separate return value rather
    // than a runtime check per step. The `ensure!` above already proved the remainder is
    // empty, so discarding `.1` drops nothing.
    bytes[HEADER_BYTES..]
        .as_chunks::<ENTRY_BYTES>()
        .0
        .iter()
        .map(|entry| decode_entry(entry.as_slice()))
        .collect()
}

/// Decode one rail entry.
///
/// Every field is read through a fallible conversion rather than an `expect`, even though
/// `as_chunks` makes the lengths certain: an infallible-by-construction `expect` in a
/// wire decoder is a panic waiting for the first layout change, and the alternative costs
/// one `?` per field.
///
/// # Errors
///
/// A chunk that is not [`ENTRY_BYTES`] long — impossible via [`decode`], and a caller bug
/// anywhere else.
fn decode_entry(entry: &[u8]) -> Result<AnnouncedRail> {
    let field = |range: std::ops::Range<usize>| -> Result<&[u8]> {
        entry
            .get(range.clone())
            .ok_or_else(|| anyhow::anyhow!("announce entry has no bytes at {range:?}"))
    };
    let gid: [u8; 16] = field(ENTRY_GID)?.try_into()?;
    let qpn: [u8; 4] = field(ENTRY_QPN)?.try_into()?;
    let rail: [u8; 2] = field(ENTRY_RAIL)?.try_into()?;
    Ok(AnnouncedRail {
        gid,
        qpn: u32::from_be_bytes(qpn),
        rail: u16::from_be_bytes(rail),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rail(i: u16) -> AnnouncedRail {
        let mut gid = [0u8; 16];
        // A GID that differs per rail in more than its last byte, so a decoder that
        // reads a fixed offset for every entry cannot pass by accident.
        for (j, b) in gid.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(17).wrapping_add(j as u8);
        }
        AnnouncedRail {
            gid,
            // 16384 is one of the values EFA firmware really assigns (A1 finding 11).
            qpn: 16_384 + u32::from(i),
            rail: i,
        }
    }

    #[test]
    fn round_trips_one_rail() {
        let rails = vec![rail(0)];
        assert_eq!(decode(&encode(&rails).unwrap()).unwrap(), rails);
    }

    #[test]
    fn round_trips_every_rail_a_p5_has() {
        let rails: Vec<_> = (0..32).map(rail).collect();
        assert_eq!(decode(&encode(&rails).unwrap()).unwrap(), rails);
    }

    #[test]
    fn round_trips_a_zero_qpn() {
        // qpn == 0 is legitimate on EFA; a decoder that treats it as "unset" would
        // silently drop a usable writer.
        let rails = vec![AnnouncedRail {
            gid: [7u8; 16],
            qpn: 0,
            rail: 3,
        }];
        assert_eq!(decode(&encode(&rails).unwrap()).unwrap(), rails);
    }

    #[test]
    fn matches_the_golden_bytes() {
        let mut gid = [0u8; 16];
        for (i, b) in gid.iter_mut().enumerate() {
            *b = i as u8;
        }
        let encoded = encode(&[AnnouncedRail {
            gid,
            qpn: 0x4001,
            rail: 0,
        }])
        .unwrap();
        assert_eq!(
            encoded, GOLDEN_ONE_RAIL,
            "the C++ decoder is checked against these bytes"
        );
        assert_eq!(decode(&GOLDEN_ONE_RAIL).unwrap().len(), 1);
    }

    #[test]
    fn rejects_an_empty_rail_list() {
        assert!(encode(&[]).is_err());
    }

    #[test]
    fn rejects_more_rails_than_the_limit() {
        let rails: Vec<_> = (0..=MAX_ANNOUNCED_RAILS as u16).map(rail).collect();
        assert!(encode(&rails).is_err());
    }

    #[test]
    fn rejects_a_truncated_message() {
        let encoded = encode(&[rail(0), rail(1)]).unwrap();
        for len in 0..encoded.len() {
            assert!(
                decode(&encoded[..len]).is_err(),
                "a {len}-byte prefix of a {}-byte message must not decode",
                encoded.len(),
            );
        }
    }

    #[test]
    fn rejects_trailing_bytes() {
        let mut encoded = encode(&[rail(0)]).unwrap();
        encoded.push(0);
        assert!(
            decode(&encoded).is_err(),
            "an over-long message means a layout disagreement"
        );
    }

    #[test]
    fn rejects_an_unknown_version() {
        let mut encoded = encode(&[rail(0)]).unwrap();
        encoded[OFFSET_VERSION] = ANNOUNCE_VERSION + 1;
        assert!(decode(&encoded).is_err());
    }

    #[test]
    fn rejects_a_zero_count() {
        let mut encoded = encode(&[rail(0)]).unwrap();
        encoded[OFFSET_COUNT] = 0;
        assert!(decode(&encoded).is_err());
    }

    #[test]
    fn the_max_message_fits_its_declared_bound() {
        let rails: Vec<_> = (0..MAX_ANNOUNCED_RAILS as u16).map(rail).collect();
        assert_eq!(encode(&rails).unwrap().len(), ANNOUNCE_MAX_BYTES);
    }
}
