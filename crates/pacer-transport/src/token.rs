//! The delivery **token**: how a client names memory it registered itself (ADR-0030
//! point 1).
//!
//! ADR-0026 had the daemon map and register the client's memory; ADR-0027 was to have it
//! import a CUDA handle. Hardware refused the second — only the owning process can dma-buf
//! export a `cudaMalloc` tensor, and dma-buf is the only way to register device memory on
//! EFA — so registration moved to the side that owns the memory and what crosses is a
//! NIC-scoped name instead of a handle.
//!
//! The types live here rather than in the daemon's header parser because both ends need
//! them: `pacer_daemon::delivery` parses `x-pacer-target` into a [`TokenRail`] list, and
//! the transport's WRITE path consumes it. They are outside the `efa` feature gate for the
//! same reason as [`crate::announce`] — the client half that *produces* a token is the C++
//! NIXL plugin (ADR-0031), so the shape is a cross-language contract and must be nameable
//! in every build.
//!
//! ## Why a token is a set
//!
//! An rkey is scoped to the protection domain that issued it, and a PD belongs to one
//! device. A window the client wants reachable from several of its own rails is therefore
//! registered once per rail and carries one `{gid, qpn, rkey}` per rail. **The order is the
//! client's preference**, highest first: only the client knows which of its rails is
//! PCIe-local to the GPU holding the tensor (ADR-0030 point 5 makes that affinity
//! mandatory, and Track H measured 6.95× for getting it wrong), and a daemon cannot infer
//! it from a GID. A writer takes the first entry it can address.

/// Q_Key every SRD queue pair in this protocol is activated with — the daemon's rails and
/// **every client's**.
///
/// SRD matches datagrams on this value instead of a connection handshake, so a client that
/// activates its QP with any other number is a client whose NIC silently drops the daemon's
/// WRITEs: nothing lands, no error is reported anywhere, and the delivery looks like a
/// declined one. It therefore belongs to the cross-language contract beside [`TokenRail`]
/// and [`crate::announce`], not to the transport's private bring-up — ADR-0031's C++ plugin
/// needs the same number. Any fixed non-zero value both ends agree on would do; this is the
/// one the A0 spike validated on hardware, so it is the one that is kept.
pub const SRD_QKEY: u32 = 0x1111_2222;

/// One rail of a client's registered window: where to write, and the endpoint whose NIC
/// will acknowledge the write.
///
/// The daemon registers none of this. All three fields are opaque to it: `rkey`/`addr` go
/// to whichever holder serves the chunk, and `gid`/`qpn` are what an address handle is
/// built from so the WRITE can be addressed and acknowledged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TokenRail {
    /// GID of the client rail that issued `rkey`, in wire byte order.
    pub gid: [u8; 16],
    /// Queue pair the client's window is reachable at. **`0` is legitimate** — EFA
    /// firmware assigns from a small set including it — so it is never a sentinel.
    pub qpn: u32,
    /// Remote key for the client's window, valid only at the protection domain of the rail
    /// named by `gid`.
    pub rkey: u32,
}

/// A client-registered window, ready to be written into.
///
/// Built from the parsed header: the window the client named, narrowed to the sub-range
/// the request asked for, plus the per-rail names it is reachable by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenWindow {
    /// First byte of the sub-window, in the client's own address space — the token's base
    /// address plus the request's offset, folded together here so no later arithmetic can
    /// forget one of them.
    start: u64,
    /// Length of the sub-window in bytes.
    len: usize,
    /// The client's rails, in its own preference order (see the module header).
    rails: Vec<TokenRail>,
}

impl TokenWindow {
    /// Fold `base_addr + offset` into a window of `len` bytes reachable by `rails`.
    ///
    /// # Errors
    ///
    /// An empty rail list (nothing could address it), a zero length (nothing to deliver),
    /// or `base_addr + offset` overflowing — all three are malformed descriptors rather
    /// than runtime conditions, and the header parser rejects them first; this is the
    /// second gate for a caller that built a window some other way.
    pub fn new(
        base_addr: u64,
        offset: u64,
        len: usize,
        rails: Vec<TokenRail>,
    ) -> Result<Self, TokenWindowError> {
        if rails.is_empty() {
            return Err(TokenWindowError::NoRails);
        }
        if len == 0 {
            return Err(TokenWindowError::Empty);
        }
        let start = base_addr
            .checked_add(offset)
            .ok_or(TokenWindowError::AddressOverflow { base_addr, offset })?;
        Ok(Self { start, len, rails })
    }

    /// The window's length in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the window is empty — never, by construction; present because clippy asks
    /// for it beside [`TokenWindow::len`].
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The client's rails, in preference order.
    #[must_use]
    pub fn rails(&self) -> &[TokenRail] {
        &self.rails
    }

    /// Rebuild a window from the [`ClientToken`](pacer_proto::v1::ClientToken) a requester
    /// put on a `FetchBlob` call — **the holder's entry point to ADR-0030's remote half**.
    ///
    /// The requester has already folded the token's base address, the request's offset and
    /// this chunk's offset within the delivery into `addr`, so the window this returns is
    /// exactly the chunk's destination and a holder writes it at offset 0. That is the whole
    /// reason the wire carries an absolute address rather than a base plus an index: window
    /// arithmetic stays on the node that owns the chunk→window map, and a holder cannot get
    /// it wrong.
    ///
    /// # Errors
    ///
    /// [`TokenWindowError::NoRails`] / [`TokenWindowError::Empty`] as
    /// [`TokenWindow::new`] does, plus [`TokenWindowError::MalformedRail`] for a GID that is
    /// not 16 bytes — length-checked rather than padded, because a short GID silently
    /// addresses a different device.
    pub fn from_proto(token: &pacer_proto::v1::ClientToken) -> Result<Self, TokenWindowError> {
        let mut rails = Vec::with_capacity(token.rails.len());
        for wire in &token.rails {
            let gid: [u8; 16] =
                wire.gid
                    .as_slice()
                    .try_into()
                    .map_err(|_| TokenWindowError::MalformedRail {
                        gid_bytes: wire.gid.len(),
                    })?;
            rails.push(TokenRail {
                gid,
                qpn: wire.qpn,
                rkey: wire.rkey,
            });
        }
        let len = usize::try_from(token.len).map_err(|_| TokenWindowError::Empty)?;
        // Offset 0: `addr` is already absolute (see this function's doc).
        Self::new(token.addr, 0, len, rails)
    }

    /// Render `[at, at + len)` of this window as the wire token a holder is offered.
    ///
    /// The inverse of [`TokenWindow::from_proto`], and the requester's only way to hand a
    /// window on: it resolves the absolute address here so the holder does no arithmetic.
    /// Every rail is carried, not just the one this node would have picked — spreading the
    /// WRITEs over the client's rails is the holder's to do, and pairing them down is what
    /// cost 30-60 % when it was tried (`bench/ladder/results/c5-multirail.md`).
    ///
    /// `None` when the sub-range does not fit what the client registered, for exactly
    /// [`TokenWindow::slice_on`]'s reason: a clamped delivery is a short read the client has
    /// no way to notice.
    #[must_use]
    pub fn proto_at(
        &self,
        at: usize,
        len: usize,
        checksum: bool,
    ) -> Option<pacer_proto::v1::ClientToken> {
        // Any rail will do to resolve the address — `slice_on` returns that rail's rkey,
        // which is discarded here because every rail's rkey travels below. What is being
        // borrowed is its bounds check.
        let (addr, _, len) = self.slice_on(self.rails.first()?, at, len)?;
        Some(pacer_proto::v1::ClientToken {
            addr,
            len: len as u64,
            rails: self
                .rails
                .iter()
                .map(|r| pacer_proto::v1::ClientTokenRail {
                    gid: r.gid.to_vec(),
                    qpn: r.qpn,
                    rkey: r.rkey,
                })
                .collect(),
            checksum,
        })
    }

    /// Absolute address and rkey for `[at, at + len)` of this window, on `rail`.
    ///
    /// Returns `None` if the sub-range does not fit, which is a caller bug rather than a
    /// runtime condition — but a *silent* one if it were an assert, since writing past a
    /// client's registered window is exactly what an rkey stops the NIC from doing only if
    /// the arithmetic was right in the first place. So it is checked and refusable.
    #[must_use]
    pub fn slice_on(&self, rail: &TokenRail, at: usize, len: usize) -> Option<(u64, u32, usize)> {
        let end = at.checked_add(len)?;
        if end > self.len {
            return None;
        }
        let addr = self.start.checked_add(at as u64)?;
        Some((addr, rail.rkey, len))
    }
}

/// Why a [`TokenWindow`] could not be built.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TokenWindowError {
    /// The token named no rails, so nothing can address the window.
    #[error("a delivery token must name at least one rail")]
    NoRails,
    /// The window is zero bytes long.
    #[error("a delivery token's window must be at least one byte")]
    Empty,
    /// `base_addr + offset` wrapped.
    #[error("token window address overflows: base {base_addr:#x} + offset {offset:#x}")]
    AddressOverflow {
        /// The window's base address, as the client named it.
        base_addr: u64,
        /// The request's offset within that window.
        offset: u64,
    },
    /// A wire rail carried a GID that is not 16 bytes ([`TokenWindow::from_proto`]).
    #[error("a token rail's GID is {gid_bytes} bytes, expected exactly 16")]
    MalformedRail {
        /// How many bytes the malformed GID carried.
        gid_bytes: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rail(rkey: u32) -> TokenRail {
        TokenRail {
            gid: [0xfe; 16],
            qpn: 16_385,
            rkey,
        }
    }

    #[test]
    fn folds_the_offset_into_the_window_start() {
        let w = TokenWindow::new(0x7f00_0000, 0x1_0000, 4096, vec![rail(7)]).unwrap();
        let (addr, rkey, len) = w.slice_on(&w.rails()[0], 0, 4096).unwrap();
        assert_eq!(
            addr, 0x7f01_0000,
            "the request's offset is applied exactly once"
        );
        assert_eq!((rkey, len), (7, 4096));
    }

    #[test]
    fn slices_within_the_window() {
        let w = TokenWindow::new(0x1000, 0, 4096, vec![rail(1)]).unwrap();
        let (addr, _, len) = w.slice_on(&w.rails()[0], 1024, 1024).unwrap();
        assert_eq!((addr, len), (0x1400, 1024));
    }

    #[test]
    fn refuses_a_slice_past_the_window() {
        let w = TokenWindow::new(0x1000, 0, 4096, vec![rail(1)]).unwrap();
        // Each of these would have the NIC write outside what the client registered.
        assert!(
            w.slice_on(&w.rails()[0], 4096, 1).is_none(),
            "starts at the end"
        );
        assert!(
            w.slice_on(&w.rails()[0], 0, 4097).is_none(),
            "one byte too long"
        );
        assert!(
            w.slice_on(&w.rails()[0], 2048, 2049).is_none(),
            "overruns from the middle"
        );
        assert!(
            w.slice_on(&w.rails()[0], usize::MAX, 1).is_none(),
            "offset overflows"
        );
    }

    #[test]
    fn keeps_the_client_preference_order() {
        // The order is the client's affinity decision; a writer must not reorder it.
        let rails = vec![rail(1), rail(2), rail(3)];
        let w = TokenWindow::new(0x1000, 0, 4096, rails.clone()).unwrap();
        assert_eq!(w.rails(), &rails[..]);
    }

    /// The remote half's contract: what a requester puts on the wire is the chunk's
    /// destination, and what the holder rebuilds writes exactly there at offset 0. Asserted
    /// on the ADDRESS rather than on equality of windows, because the two are deliberately
    /// not equal — the requester's window spans the whole delivery, the holder's spans one
    /// chunk.
    #[test]
    fn a_wire_token_names_this_chunks_destination_absolutely() {
        let window = TokenWindow::new(0x7f00_0000, 0x1_0000, 64 << 20, vec![rail(7), rail(9)])
            .expect("a two-rail window");
        // The third chunk of a 16 MiB grid.
        let (at, len) = (3 * (16 << 20), 16 << 20);
        let wire = window.proto_at(at, len, true).expect("inside the window");
        assert_eq!(
            wire.addr,
            0x7f00_0000 + 0x1_0000 + at as u64,
            "base + request offset + chunk offset, folded exactly once"
        );
        assert_eq!(wire.len, len as u64);
        assert_eq!(
            wire.rails.len(),
            2,
            "every client rail travels, not just one"
        );
        assert!(wire.checksum);

        let rebuilt = TokenWindow::from_proto(&wire).expect("the holder rebuilds it");
        let (addr, rkey, wrote) = rebuilt
            .slice_on(&rebuilt.rails()[0], 0, len)
            .expect("a holder writes the whole chunk at offset 0");
        assert_eq!(addr, wire.addr, "no second offset is applied");
        assert_eq!((rkey, wrote), (7, len));
        // And the client's preference order survives, since that is its affinity decision.
        assert_eq!(rebuilt.rails()[1].rkey, 9);
    }

    /// The bounds check has to hold on the wire too: a holder must never be handed a window
    /// that reaches past what the client registered, and it must refuse one if it somehow is.
    #[test]
    fn a_wire_token_cannot_escape_the_registered_window() {
        let window = TokenWindow::new(0x1000, 0, 4096, vec![rail(1)]).unwrap();
        assert!(
            window.proto_at(4096, 1, false).is_none(),
            "starts at the end"
        );
        assert!(
            window.proto_at(0, 4097, false).is_none(),
            "one byte too long"
        );
        // And a holder handed a body larger than the offered window refuses rather than
        // truncating — a torn WRITE is not a recoverable partial result.
        let wire = window.proto_at(0, 4096, false).unwrap();
        let rebuilt = TokenWindow::from_proto(&wire).unwrap();
        assert!(rebuilt.slice_on(&rebuilt.rails()[0], 0, 4097).is_none());
    }

    /// A GID is an address, so a wrong-length one is a rejection rather than something to
    /// pad: padded on the wrong end it addresses a different device, and no layer below can
    /// tell. Same reasoning as the header parser's `parse_gid`.
    #[test]
    fn rejects_wire_rails_that_cannot_be_addressed() {
        let base = |rails: Vec<pacer_proto::v1::ClientTokenRail>| pacer_proto::v1::ClientToken {
            addr: 0x1000,
            len: 4096,
            rails,
            checksum: false,
        };
        for bad_gid in [vec![0xfe; 4], vec![0xfe; 17], vec![]] {
            let bytes = bad_gid.len();
            let err = TokenWindow::from_proto(&base(vec![pacer_proto::v1::ClientTokenRail {
                gid: bad_gid,
                qpn: 1,
                rkey: 1,
            }]))
            .expect_err("a malformed GID must not become a silent address");
            assert_eq!(err, TokenWindowError::MalformedRail { gid_bytes: bytes });
        }
        // A token naming no rails at all: nothing can address the window.
        assert_eq!(
            TokenWindow::from_proto(&base(vec![])),
            Err(TokenWindowError::NoRails)
        );
        // And a zero-length one delivers nothing.
        let mut empty = base(vec![pacer_proto::v1::ClientTokenRail {
            gid: vec![0xfe; 16],
            qpn: 0,
            rkey: 1,
        }]);
        empty.len = 0;
        assert_eq!(
            TokenWindow::from_proto(&empty),
            Err(TokenWindowError::Empty)
        );
    }

    #[test]
    fn rejects_a_window_nothing_can_address_or_hold() {
        assert_eq!(
            TokenWindow::new(0x1000, 0, 4096, vec![]),
            Err(TokenWindowError::NoRails)
        );
        assert_eq!(
            TokenWindow::new(0x1000, 0, 0, vec![rail(1)]),
            Err(TokenWindowError::Empty)
        );
        assert_eq!(
            TokenWindow::new(u64::MAX, 1, 4096, vec![rail(1)]),
            Err(TokenWindowError::AddressOverflow {
                base_addr: u64::MAX,
                offset: 1
            })
        );
    }
}
