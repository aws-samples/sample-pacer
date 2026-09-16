//! Which rails a GPU should be written into, discovered from the host rather than tabulated
//! (ADR-0030 point 5: rail↔GPU affinity is mandatory, not advisory).
//!
//! ## Why this is not a convenience
//!
//! Getting the affinity wrong is expensive and, worse, it is *quiet*: Track H measured
//! **6.95×** on a p5 and the result landed BELOW host-memory rate, so it reads as a slow
//! fabric rather than as a misconfiguration; a p6-b200 measured **1.440×**
//! (`bench/ladder/results/c5-b200-nic-hbm.md`). Nothing fails, no metric fires, and the
//! delivery still verifies byte-for-byte. The only signal is a number nobody can explain.
//!
//! Until now the map was a comment in a values file and two environment variables set by
//! hand, which failed in all three available ways:
//!
//! * **It is per-instance-type.** A p5 puts 4 rails and 1 GPU under a switch; a p6-b200 puts
//!   2 rails and 2 GPUs under each of 4. A table written for one is wrong on the other, and
//!   wrong in the direction that still works.
//! * **It went stale.** When `pacer_transport::rdma_device::is_efa` started filtering
//!   non-EFA devices, every hand-written index shifted by two on p6-b200 — the two
//!   `mlx5_core` interfaces had been occupying 0 and 1.
//! * **It cannot be read from inside the pod anyway.** A container given one GPU sees
//!   ordinal 0 whatever it physically holds, so the "GPU ordinal" a table is keyed on is not
//!   available to the process that needs it. `Cuda::pci_bus_id` is, and reports the HOST
//!   address — which is what makes this module possible.
//!
//! ## How
//!
//! Rails are enumerated through **`ibverbs`, not sysfs**, and filtered with the same
//! predicate `endpoint::bring_up` applies. That is deliberate: the returned indices
//! have to mean the same thing to `bring_up`, and reproducing libibverbs' ordering from
//! sysfs would be a second ordering free to drift from the first. Sysfs is then asked only
//! for each device's PCI ancestry, and the GPU's, and the two are scored by shared bridges
//! ([`rdma_device::shared_bridges`]). No instance-type knowledge anywhere.
//!
//! `endpoint::bring_up` and `Cuda::pci_bus_id` are named in backticks rather than linked, for
//! the reason `crate::ffi`'s header records: both live in private modules while this one is
//! public, and rustdoc rejects a public item linking to a private one under `-D warnings`.
//!
//! ## What it refuses to do
//!
//! If no rail shares more than the root complex with the GPU
//! ([`rdma_device::SHARED_ROOT_ONLY`]), this reports **no distinction** instead of naming the
//! arbitrary first rail. On such a host pinning genuinely buys nothing, and a caller told
//! "rail 0 is affine" would spend the rest of the arm explaining a number that has no
//! topological cause.

use anyhow::{bail, Context as _, Result};
use pacer_transport::rdma_device;
use tracing::{info, warn};

use crate::cuda::Cuda;
use crate::endpoint::carries_rail;

/// One EFA rail, as both `bring_up` and the fabric see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rail {
    /// Index to hand `endpoint::bring_up` — position among EFA devices only.
    pub index: usize,
    /// libibverbs device name, e.g. `rdmap113s0`. Logged so a wrong-rail result is legible.
    pub name: String,
    /// PCI ancestry, root-first. Empty when sysfs would not say.
    pub ancestry: Vec<String>,
}

/// Every rail a client could register on, in `endpoint::bring_up` index order.
///
/// # Errors
///
/// Listing RDMA devices failing — no `/dev/infiniband`, or no efa kernel module.
pub fn rails() -> Result<Vec<Rail>> {
    let list = ibverbs::devices().context(
        "listing RDMA devices (is the efa kernel module loaded and /dev/infiniband mounted?)",
    )?;
    Ok(list
        .iter()
        .filter(|dev| carries_rail(dev))
        .enumerate()
        .map(|(index, dev)| {
            let name = dev.name().map_or_else(
                || format!("rail{index}"),
                |n| n.to_string_lossy().into_owned(),
            );
            Rail {
                ancestry: rdma_device::rdma_pci_ancestry(&name),
                index,
                name,
            }
        })
        .collect())
}

/// The outcome of asking which rails serve `gpu`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Affinity {
    /// Rail indices, nearest first, then by index so the choice is deterministic.
    pub rails: Vec<usize>,
    /// Device names of [`Self::rails`], positionally — for the log line and the record.
    pub names: Vec<String>,
    /// The GPU's PCI address, as CUDA reported it.
    pub gpu_pci: String,
    /// Bridges the best rail shares with the GPU. `<= SHARED_ROOT_ONLY` means no distinction.
    pub shared: usize,
    /// **How many rails are in this GPU's affine set** — every rail sharing [`Self::shared`]
    /// bridges with it, whatever the caller asked for.
    ///
    /// This is the number a client registering a window should use, and it is a property of
    /// the HARDWARE rather than a tuning knob: on a p5 four rails sit behind one PCIe switch
    /// with a GPU, on a p5en two, and on a p6-b200 the affinity unit is the GPU *pair* and it
    /// carries two. A caller passing a `want` of its own is guessing at a number this
    /// function already computed — which is how `C5_RAILS` came to default to **1** on every
    /// one of those shapes, leaving three quarters of a p5 GPU's affine fabric unused.
    ///
    /// ⚠ It is NOT a throughput promise. On a p5 one rail (~11.9 GiB/s) already saturates the
    /// per-GPU switch uplink (~12), which is why widening 1 → 4 there measured a null three
    /// times over; the point of registering the set is that it is the *correct* set on any
    /// shape, including the ones where one rail does not saturate (a single B200 absorbs
    /// 56.713 GiB/s measured, against ~46.6 for one 400 Gbps rail).
    ///
    /// When [`Self::distinct`] is `false` the host expresses no rail/GPU distinction, so
    /// there is no affine set to speak of and this is the total rail count.
    pub affine_count: usize,
    /// Whether the host expresses a usable rail↔GPU distinction at all.
    ///
    /// `false` is a real answer, not a failure: the rails are still returned (in index
    /// order), and a caller should record that pinning was not possible rather than claim an
    /// affinity it did not get.
    pub distinct: bool,
}

/// Resolve the `want` rails closest to CUDA device `ordinal` on the PCIe fabric.
///
/// Returns fewer than `want` only when the host has fewer rails. Callers registering a
/// window pass [`Affinity::rails`]`[0]` as the first rail and `rails.len()` as the count —
/// consecutive-from-first is what `WindowSpec` expresses, so this orders the result to make
/// the common case correct without further arithmetic.
///
/// # Errors
///
/// No CUDA driver or no such device, the PCI address query failing, or no EFA rail existing.
///
/// # Panics
///
/// Never.
pub fn affine_rails(ordinal: i32, want: usize) -> Result<Affinity> {
    let all = rails()?;
    if all.is_empty() {
        bail!("no EFA rail on this host — does this pod request `vpc.amazonaws.com/efa`?");
    }
    let gpu_pci = Cuda::open()?.device(ordinal)?.pci_bus_id()?;
    let gpu = rdma_device::pci_ancestry(&gpu_pci);
    if gpu.is_empty() {
        warn!(
            gpu_pci = %gpu_pci,
            "GPU not found under /sys/bus/pci/devices, so rail affinity cannot be resolved; \
             falling back to rails in index order — expect the unpinned rate (1.44x on \
             p6-b200, up to 6.95x on p5) rather than a failure"
        );
    }

    // Sort by shared bridges descending, then index ascending. The second key is what makes
    // two equally-affine rails resolve to the same pair on every run — without it the pair
    // could differ between processes and a comparison would not be one.
    let mut scored: Vec<(usize, &Rail)> = all
        .iter()
        .map(|rail| (rdma_device::shared_bridges(&rail.ancestry, &gpu), rail))
        .collect();
    scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.index.cmp(&b.1.index)));

    let shared = scored.first().map_or(0, |(score, _)| *score);
    let distinct = shared > rdma_device::SHARED_ROOT_ONLY;
    // The affine SET, before `want` truncates anything: the leading run at the best score.
    // Sorting has already grouped them, so this is the boundary and not a second search — the
    // information was being computed and discarded, which is why every caller had to guess.
    let affine_count = scored
        .iter()
        .take_while(|(score, _)| *score == shared)
        .count();
    let chosen: Vec<&Rail> = scored.into_iter().take(want).map(|(_, r)| r).collect();
    let affinity = Affinity {
        rails: chosen.iter().map(|r| r.index).collect(),
        names: chosen.iter().map(|r| r.name.clone()).collect(),
        gpu_pci,
        shared,
        affine_count,
        distinct,
    };

    if distinct {
        info!(
            gpu = ordinal, gpu_pci = %affinity.gpu_pci, shared_bridges = shared,
            rails = ?affinity.rails, devices = ?affinity.names,
            "resolved rail/GPU PCIe affinity from the host"
        );
    } else {
        warn!(
            gpu = ordinal, gpu_pci = %affinity.gpu_pci, shared_bridges = shared,
            rails = ?affinity.rails, devices = ?affinity.names, total_rails = all.len(),
            "this host expresses NO rail/GPU PCIe distinction (every rail shares only the \
             root complex), so pinning buys nothing here — rails returned in index order"
        );
    }
    Ok(affinity)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nearest-first, and ties broken by index — the property that makes two runs of one arm
    /// comparable. Exercised on the scoring directly, because the real ordering depends on
    /// hardware this test does not require.
    #[test]
    fn ties_resolve_by_index_so_the_choice_is_deterministic() {
        let gpu = vec!["0000:00:01.0".to_owned(), "0000:20:00.0".to_owned()];
        let rail = |index: usize, tail: &str| Rail {
            index,
            name: format!("rdmap{index}"),
            ancestry: vec!["0000:00:01.0".to_owned(), tail.to_owned()],
        };
        // Two rails share both bridges, one shares only the first.
        let all = [
            rail(2, "0000:20:00.0"),
            rail(0, "0000:99:00.0"),
            rail(1, "0000:20:00.0"),
        ];
        let mut scored: Vec<(usize, &Rail)> = all
            .iter()
            .map(|r| (rdma_device::shared_bridges(&r.ancestry, &gpu), r))
            .collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.index.cmp(&b.1.index)));
        assert_eq!(
            scored.iter().map(|(_, r)| r.index).collect::<Vec<_>>(),
            vec![1, 2, 0],
            "expected the two 2-bridge rails first in index order, then the 1-bridge one",
        );
    }

    /// The affine SET's size, which is the number a client registering a window should use.
    ///
    /// Exercised on the same scoring the real function does, then the boundary rule applied —
    /// `affine_count` is the leading run at the best score, so this fixture (two rails behind
    /// the GPU's switch, one elsewhere) must report **2** whatever `want` asks for. That
    /// independence is the whole point: `C5_RAILS` defaulted to 1 for months while this
    /// function already knew the answer was 4 on a p5.
    #[test]
    fn the_affine_set_is_the_leading_run_and_not_what_the_caller_asked_for() {
        let gpu = vec!["0000:00:01.0".to_owned(), "0000:20:00.0".to_owned()];
        let rail = |index: usize, tail: &str| Rail {
            index,
            name: format!("rdmap{index}"),
            ancestry: vec!["0000:00:01.0".to_owned(), tail.to_owned()],
        };
        let all = [
            rail(2, "0000:20:00.0"),
            rail(0, "0000:99:00.0"),
            rail(1, "0000:20:00.0"),
        ];
        let mut scored: Vec<(usize, &Rail)> = all
            .iter()
            .map(|r| (rdma_device::shared_bridges(&r.ancestry, &gpu), r))
            .collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.index.cmp(&b.1.index)));
        let shared = scored.first().map_or(0, |(score, _)| *score);
        let affine_count = scored
            .iter()
            .take_while(|(score, _)| *score == shared)
            .count();
        assert_eq!(
            affine_count, 2,
            "two rails share the GPU's switch, the third does not"
        );
        assert!(
            affine_count < all.len(),
            "a set that swallowed every rail would make the number meaningless"
        );
    }

    /// With no distinction, every rail ties at the root score — so the "set" is all of them.
    /// Recorded because a caller must then register in index order and SAY that pinning was
    /// not available, rather than reporting an affinity it did not get.
    #[test]
    fn no_distinction_makes_the_whole_rail_list_the_set() {
        let gpu: Vec<String> = vec!["0000:00:01.0".to_owned()];
        let rail = |index: usize| Rail {
            index,
            name: format!("rdmap{index}"),
            ancestry: vec!["0000:00:01.0".to_owned()],
        };
        let all = [rail(0), rail(1), rail(2)];
        let mut scored: Vec<(usize, &Rail)> = all
            .iter()
            .map(|r| (rdma_device::shared_bridges(&r.ancestry, &gpu), r))
            .collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.index.cmp(&b.1.index)));
        let shared = scored.first().map_or(0, |(score, _)| *score);
        let affine_count = scored
            .iter()
            .take_while(|(score, _)| *score == shared)
            .count();
        assert_eq!(affine_count, all.len());
        assert!(
            shared <= rdma_device::SHARED_ROOT_ONLY,
            "this fixture must be the no-distinction case, or it proves nothing"
        );
    }

    /// A host sharing only the root must report `distinct == false` rather than crowning the
    /// first rail. Asserted on the constant's meaning so the boundary cannot drift.
    #[test]
    fn sharing_only_the_root_is_not_a_distinction() {
        let gpu = vec!["0000:00:01.0".to_owned(), "0000:20:00.0".to_owned()];
        let far = vec!["0000:00:01.0".to_owned(), "0000:99:00.0".to_owned()];
        assert_eq!(rdma_device::shared_bridges(&far, &gpu), 1);
        assert!(rdma_device::shared_bridges(&far, &gpu) <= rdma_device::SHARED_ROOT_ONLY);
    }

    /// An empty GPU ancestry (sysfs unreadable) scores every rail 0 and must therefore fall
    /// back to index order, not to an arbitrary permutation.
    #[test]
    fn an_unresolvable_gpu_falls_back_to_index_order() {
        let rail = |index: usize| Rail {
            index,
            name: format!("rdmap{index}"),
            ancestry: vec!["0000:00:01.0".to_owned()],
        };
        let all = [rail(3), rail(1), rail(2)];
        let mut scored: Vec<(usize, &Rail)> = all
            .iter()
            .map(|r| (rdma_device::shared_bridges(&r.ancestry, &[]), r))
            .collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.index.cmp(&b.1.index)));
        assert_eq!(
            scored.iter().map(|(_, r)| r.index).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(scored[0].0, 0, "an unresolvable GPU shares nothing");
    }
}
