//! Rendering the `x-pacer-target` value for a window this process registered
//! (ADR-0030 § Decision point 1).
//!
//! The daemon's parser owns this grammar (`pacer_daemon::delivery::TargetSpec::parse`);
//! this module is the only other place that has to agree with it, and the agreement is
//! checked rather than asserted — the tests below build a token here and parse it *with
//! the daemon's own parser*, so the two cannot drift while the tests run.
//!
//! Deliberately outside the `efa` feature gate: a token is a string, the grammar is a
//! contract with code we do not compile (ADR-0031's NIXL plugin renders the same thing in
//! C++), and a wire format that is only compiled where libefa links is a wire format
//! nothing checks on CI.

use anyhow::{ensure, Result};
use pacer_transport::announce::MAX_ANNOUNCED_RAILS;
use pacer_transport::token::TokenRail;

/// Scheme naming client-registered memory. The daemon's other scheme (`shm:`) names
/// memory it must map itself; this one names memory it must not touch.
const SCHEME: &str = "nic:";
/// Separator between the handle and each `key=value` parameter.
const PARAM: char = ';';
/// Parameter naming the window offset the delivery starts at.
const OFFSET_PARAM: &str = "offset";
/// Parameter naming how many bytes the daemon may write.
const LEN_PARAM: &str = "len";
/// Parameter naming the integrity algorithm, or its absence.
const CHECKSUM_PARAM: &str = "checksum";
/// Value of [`CHECKSUM_PARAM`] that turns the daemon's digest pass off.
///
/// Worth an explicit knob rather than always-on: verification is O(delivered bytes) on
/// *both* sides, and for a device window the client's half is a full device→host copy of
/// everything just delivered — which would cost more than the transport it is checking.
/// A rate arm passes `false`; an integrity arm passes `true` and pays for it knowingly.
const CHECKSUM_NONE: &str = "none";
/// Parameter naming the rails the client registered on.
const RAILS_PARAM: &str = "rails";
/// Separator between rail entries.
const RAIL_SEPARATOR: char = ',';
/// Separator between a rail's `gid`/`qpn`/`rkey` fields.
const RAIL_FIELD: char = '/';

/// Render the header value naming `[offset, offset + len)` of a registered window.
///
/// `base_addr` is what the *registration* reports — `MemoryRegion::remote().addr` — and
/// never a pointer the caller happens to hold. For a GPU window those differ: the window
/// is registered from a dma-buf with `iova == offset`, so its base is the dma-buf offset
/// (`0` for a whole-buffer export) rather than the device virtual address, and a token
/// carrying the device pointer names memory the NIC cannot reach (planning/21 § the
/// dma-buf addressing trap).
///
/// # Errors
///
/// An empty rail list, more rails than one announce can carry ([`MAX_ANNOUNCED_RAILS`] —
/// the daemon's token limit is defined as that same number), or a zero `len`: all three
/// are rejected by the daemon, and rendering one only spends a round trip to find out.
pub fn render(
    base_addr: u64,
    offset: u64,
    len: u64,
    checksum: bool,
    rails: &[TokenRail],
) -> Result<String> {
    ensure!(
        !rails.is_empty(),
        "a token must name at least one rail: nothing could address the window"
    );
    ensure!(
        rails.len() <= MAX_ANNOUNCED_RAILS,
        "{} rails exceeds the {MAX_ANNOUNCED_RAILS}-rail token limit",
        rails.len(),
    );
    ensure!(len > 0, "a zero-length window can receive nothing");
    let rails: Vec<String> = rails
        .iter()
        .map(|r| {
            format!(
                "{}{RAIL_FIELD}{}{RAIL_FIELD}{}",
                hex_gid(r.gid),
                r.qpn,
                r.rkey
            )
        })
        .collect();
    let mut token =
        format!("{SCHEME}{base_addr:#x}{PARAM}{OFFSET_PARAM}={offset}{PARAM}{LEN_PARAM}={len}");
    if !checksum {
        token.push_str(&format!("{PARAM}{CHECKSUM_PARAM}={CHECKSUM_NONE}"));
    }
    token.push_str(&format!(
        "{PARAM}{RAILS_PARAM}={}",
        rails.join(&RAIL_SEPARATOR.to_string())
    ));
    Ok(token)
}

/// Render a GID as the 32 lower-case hex digits the grammar wants — no `0x`, no
/// separators, no padding. The daemon's parser is strict about the length because a GID is
/// an address: a short one padded at the wrong end names a different device and nothing
/// downstream can tell.
fn hex_gid(gid: [u8; 16]) -> String {
    let mut out = String::with_capacity(gid.len() * 2);
    for byte in gid {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use pacer_daemon::delivery::{TargetMemory, TargetSpec};

    /// A GID whose every byte differs, so a renderer that repeats one byte or reverses the
    /// buffer cannot pass by accident.
    fn gid() -> [u8; 16] {
        let mut gid = [0u8; 16];
        for (i, b) in gid.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(17).wrapping_add(3);
        }
        gid
    }

    fn rail(qpn: u32, rkey: u32) -> TokenRail {
        TokenRail {
            gid: gid(),
            qpn,
            rkey,
        }
    }

    /// The one test that matters: what this crate emits is what the daemon accepts, field
    /// for field. Everything else about the grammar is the parser's own test suite.
    #[test]
    fn the_daemon_parses_what_we_render() {
        let rails = vec![rail(16_384, 1_048_576), rail(16_385, 1_048_577)];
        let raw = render(0x7f45_f400_0000, 16 << 20, 64 << 20, true, &rails).unwrap();
        let spec = TargetSpec::parse(&raw).expect("the daemon must accept our token");
        assert_eq!(spec.offset, 16 << 20);
        assert_eq!(spec.len, 64 << 20);
        assert!(spec.checksum, "checksum=true must not emit the none flag");
        match spec.memory {
            TargetMemory::Nic {
                base_addr,
                rails: parsed,
            } => {
                assert_eq!(base_addr, 0x7f45_f400_0000);
                assert_eq!(
                    parsed, rails,
                    "every rail field must survive the round trip"
                );
            }
            other => panic!("parsed as the wrong scheme: {other:?}"),
        }
    }

    /// A GPU window's base is the dma-buf offset, which for a whole-buffer export is 0 —
    /// and `0x0` must survive a grammar that also accepts decimal.
    #[test]
    fn a_zero_base_address_round_trips() {
        let raw = render(0, 0, 32 << 20, false, &[rail(0, 7)]).unwrap();
        let spec = TargetSpec::parse(&raw).unwrap();
        assert!(
            !spec.checksum,
            "checksum=false must turn the daemon's pass off"
        );
        assert!(matches!(
            spec.memory,
            TargetMemory::Nic { base_addr: 0, .. }
        ));
    }

    /// EFA firmware really does hand out QPN 0 (A1 finding 11), so a renderer that treats
    /// zero as unset would publish a rail the daemon cannot address.
    #[test]
    fn a_zero_qpn_round_trips() {
        let raw = render(0x1000, 0, 4096, true, &[rail(0, 1)]).unwrap();
        let spec = TargetSpec::parse(&raw).unwrap();
        match spec.memory {
            TargetMemory::Nic { rails, .. } => assert_eq!(rails[0].qpn, 0),
            other => panic!("parsed as the wrong scheme: {other:?}"),
        }
    }

    #[test]
    fn every_rail_a_p5_has_round_trips() {
        let rails: Vec<_> = (0..32).map(|i| rail(16_384 + i, 1 + i)).collect();
        let raw = render(0x2000, 0, 65_536, true, &rails).unwrap();
        match TargetSpec::parse(&raw).unwrap().memory {
            TargetMemory::Nic { rails: parsed, .. } => assert_eq!(parsed.len(), 32),
            other => panic!("parsed as the wrong scheme: {other:?}"),
        }
    }

    #[test]
    fn refuses_what_the_daemon_would_reject() {
        assert!(render(0x1000, 0, 4096, true, &[]).is_err(), "no rails");
        assert!(
            render(0x1000, 0, 0, true, &[rail(1, 1)]).is_err(),
            "zero len"
        );
        let too_many: Vec<_> = (0..=MAX_ANNOUNCED_RAILS as u32)
            .map(|i| rail(i, i))
            .collect();
        assert!(
            render(0x1000, 0, 4096, true, &too_many).is_err(),
            "too many rails"
        );
    }
}
