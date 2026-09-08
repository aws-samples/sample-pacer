//! Which of the host's RDMA devices can carry a rail (ADR-0021: efadv/ibverbs, not
//! libfabric — so SRD, so EFA only).
//!
//! Deliberately outside the `efa` feature gate and free of any `ibverbs` type: it reads
//! sysfs and takes a device *name*. Both ends of ADR-0030 have to agree on what "rail 3"
//! means — the daemon that raises rails (`efa::context`) and the client that registers
//! memory on one (`pacer-client::endpoint`) — and those two crates enable their `efa`
//! features independently, so a shared helper is only reachable from both if it needs
//! neither. That is the same reason `announce` and `token` sit outside the gate.
//!
//! **Why this exists at all.** A p6-b200 exposes ten devices under
//! `/sys/class/infiniband`: the eight EFA rails, plus `ibp115s0f0`/`ibp116s0f0` on
//! `mlx5_core`. The two Mellanox devices enumerate *first* — index 0 and 1 — and neither can
//! create an SRD QP. Selecting a rail by raw enumeration index therefore spends slots on
//! unusable hardware and shifts every subsequent index by two: measured on that node, a
//! privileged daemon got zero working rails at `max_rails=1` and six of eight at 8. A p5
//! never showed it (EFA devices only), which is why the raw index survived this long.
//!
//! **Do not predict an index from the enumeration order — read it back.** An earlier version
//! of this comment said libibverbs "sorts by name", which is *wrong* and wrong in the
//! direction that produces an off-by-N rail. The measured order on that node was ascending
//! PCI bus — `rdmap79s0, rdmap80s0, rdmap96s0, rdmap97s0, rdmap113s0, rdmap114s0,
//! rdmap132s0, rdmap133s0` — whereas a lexicographic sort of those names puts `rdmap113s0`
//! *first*, four positions off. So the "mlx5 first" observation is real but its stated
//! mechanism was not, and the safe rule is to take the device NAME the library logs for a
//! rail rather than deriving the rail from a name. [`crate::rdma_device`] exists to make the
//! filtered list agree on both ends; `pacer_client::topology` exists so nobody has to guess
//! the position within it.

/// Kernel driver a device must be bound to for the verbs this transport needs.
///
/// `efadv_create_qp_ex` and SRD exist only on EFA (ADR-0021), so a device on any other
/// driver cannot carry a rail — it fails bring-up after consuming a rail slot.
const EFA_DRIVER: &str = "efa";

/// sysfs directory with one entry per RDMA device the NODE exposes, whatever this pod may
/// open — which is what makes the driver readable even for devices not allocated here.
const INFINIBAND_CLASS_DIR: &str = "/sys/class/infiniband";

/// The kernel driver bound to the RDMA device named `name`, if sysfs will say.
///
/// `/sys/class/infiniband/<name>/device/driver` is a symlink whose target's file name is
/// the module — `efa`, or `mlx5_core` for the Mellanox interfaces a p6-b200 also carries.
fn driver_of(name: &str) -> Option<String> {
    let link = std::path::Path::new(INFINIBAND_CLASS_DIR)
        .join(name)
        .join("device/driver");
    let target = std::fs::read_link(link).ok()?;
    Some(target.file_name()?.to_string_lossy().into_owned())
}

/// Whether a rail can be raised on the device named `name` — i.e. whether it is EFA.
///
/// **Positive exclusion only.** A device whose driver sysfs will not name is KEPT, so a
/// host that mounts no `/sys/class/infiniband` (or a future naming change) behaves exactly
/// as it did before this filter existed: the device open and the QP creation remain the
/// real gates. Dropping unknowns instead would turn one unreadable sysfs into a silent
/// gRPC fallback on a node with healthy rails — a worse failure than the one being fixed,
/// because it is invisible.
#[must_use]
pub fn is_efa(name: &str) -> bool {
    match driver_of(name) {
        Some(driver) => driver == EFA_DRIVER,
        None => true,
    }
}

/// sysfs directory with one entry per PCI function on the host, used to resolve a GPU's
/// ancestry from the bus address CUDA reports.
const PCI_DEVICES_DIR: &str = "/sys/bus/pci/devices";

/// The PCI addresses on `path`, root-first — i.e. the device's ancestor chain.
///
/// A resolved sysfs PCI path is `/sys/devices/pci0000:00/0000:00:01.0/0000:01:00.0/…`, so
/// the components that *look* like a PCI address are exactly the bridges above the device,
/// already in order. Matching on shape rather than splitting at a fixed depth is what keeps
/// this working across the several sysfs layouts EFA and NVIDIA functions appear under.
fn ancestry_of(path: &std::path::Path) -> Vec<String> {
    let Ok(real) = std::fs::canonicalize(path) else {
        return Vec::new();
    };
    real.iter()
        .filter_map(|part| part.to_str())
        .filter(|part| part.matches(':').count() == 2 && part.contains('.'))
        .map(str::to_owned)
        .collect()
}

/// PCI ancestry of the RDMA device named `name`, root-first, or empty if sysfs will not say.
///
/// Pair with [`shared_bridges`] to score a rail against a GPU: the two are comparable
/// because both are read out of the same `/sys/devices` tree.
#[must_use]
pub fn rdma_pci_ancestry(name: &str) -> Vec<String> {
    ancestry_of(
        &std::path::Path::new(INFINIBAND_CLASS_DIR)
            .join(name)
            .join("device"),
    )
}

/// PCI ancestry of the function at PCI address `addr` (e.g. a GPU), root-first.
///
/// The `bus:device.function` part of a PCI address — everything after the domain.
///
/// The domain is the first colon-separated field and is the only field the two spellings
/// disagree about, so dropping it is what makes them comparable. **Not `rsplit`**: the
/// rightmost component of `0000:62:00.0` is `00.0`, which is the device and function alone
/// and is shared by nearly every GPU on a host — matching on it selects an arbitrary device.
///
/// Returns `None` unless `addr` carries a domain at all (two colons), so a domain-less
/// `62:00.0` cannot reduce to `00.0` and match everything — the same collision by a
/// different route.
fn without_domain(addr: &str) -> Option<&str> {
    (addr.matches(':').count() >= 2)
        .then(|| addr.split_once(':').map(|(_, rest)| rest))
        .flatten()
}

/// PCI ancestry of the function at PCI address `addr` (e.g. a GPU), root-first.
///
/// `addr` is matched case-insensitively and across domain widths, because **the two tools
/// that report this address disagree on format**: measured on a p6-b200,
/// `cuDeviceGetPCIBusId` returns the 4-digit `0000:62:00.0` while `nvidia-smi` prints the
/// 8-digit `00000000:62:00.0`, and sysfs names the same function `0000:62:00.0`. So a caller
/// may hand either spelling in. Scanning the directory rather than trusting one is the
/// difference between discovery working and silently finding no GPU — which would degrade to
/// "no affinity distinction" and read as a slow fabric, the exact failure this module exists
/// to prevent.
#[must_use]
pub fn pci_ancestry(addr: &str) -> Vec<String> {
    let want = addr.trim().to_ascii_lowercase();
    let direct = std::path::Path::new(PCI_DEVICES_DIR).join(&want);
    if direct.exists() {
        return ancestry_of(&direct);
    }
    let Ok(entries) = std::fs::read_dir(PCI_DEVICES_DIR) else {
        return Vec::new();
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_ascii_lowercase();
        let same_bdf = without_domain(&name)
            .zip(without_domain(&want))
            .is_some_and(|(a, b)| a == b);
        if name == want || same_bdf {
            return ancestry_of(&entry.path());
        }
    }
    Vec::new()
}

/// How many leading bridges two ancestries share — higher means closer on the PCIe fabric.
///
/// This is the whole affinity metric. It needs no per-instance table because it reads the
/// topology the host reports: a p5 puts four rails and one GPU under a switch, a p6-b200
/// puts two rails and two GPUs under each of four, and a host with no useful distinction
/// shares only the root with everything — which callers must treat as "pinning buys
/// nothing here" rather than as a winner (see [`SHARED_ROOT_ONLY`]).
#[must_use]
pub fn shared_bridges(a: &[String], b: &[String]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// Score at or below which two functions share only the host bridge, i.e. nothing useful.
///
/// Every function on a machine shares the root complex, so a score of 1 is the floor rather
/// than a weak signal. A caller seeing no rail above this must say so: reporting the
/// arbitrary first rail as "affine" is how a misconfiguration becomes an unexplained 1.44x
/// (p6-b200) or 6.95x (p5, Track H) that reads as a slow fabric.
pub const SHARED_ROOT_ONLY: usize = 1;

#[cfg(test)]
mod tests {
    use super::*;

    /// The two domain widths a real host reports must compare EQUAL, and two different buses
    /// must not.
    ///
    /// Both halves are load-bearing and the second is the bug this replaced: an `rsplit`
    /// took the rightmost component, so `0000:62:00.0` reduced to `00.0` — the device and
    /// function alone, which nearly every GPU on a host shares. The fallback would then have
    /// returned an arbitrary device's ancestry, scoring every rail against the wrong GPU and
    /// reporting either wrong rails or no distinction, with nothing failing.
    #[test]
    fn domain_width_is_ignored_but_bus_is_not() {
        // cuDeviceGetPCIBusId's 4-digit form vs nvidia-smi's 8-digit form, same function.
        assert_eq!(
            without_domain("0000:62:00.0"),
            without_domain("00000000:62:00.0"),
        );
        // Two GPUs on different buses, both device 0 function 0 — the collision case.
        assert_ne!(
            without_domain("0000:62:00.0"),
            without_domain("0000:4f:00.0"),
        );
        assert_eq!(without_domain("0000:62:00.0"), Some("62:00.0"));
        // An address with no domain yields NOTHING rather than reducing to `00.0` and
        // matching everything — the same collision reached by a different route.
        assert_eq!(without_domain("62:00.0"), None);
        assert_eq!(without_domain("nonsense"), None);
    }

    /// A name that cannot resolve in sysfs must be KEPT, not dropped — the fallback this
    /// module's contract turns on. Asserting it here is what stops a later "tidy-up" from
    /// inverting the default and converting an unreadable sysfs into a silent loss of
    /// every rail.
    #[test]
    fn unknown_device_is_kept() {
        assert!(is_efa("no-such-device-6f2b1a"));
    }

    /// A device sysfs lists must have a readable driver, so a typo in
    /// [`INFINIBAND_CLASS_DIR`] or a wrong `device/driver` suffix cannot masquerade as
    /// "sysfs said nothing" and silently keep every device. That masquerade is the one way
    /// this module fails open without anyone noticing, since [`is_efa`] deliberately keeps
    /// unknowns. On a machine with no RDMA at all the loop body never runs, which is
    /// correct: the claim is about hosts where devices DO exist.
    #[test]
    fn a_listed_device_has_a_readable_driver() {
        let Ok(entries) = std::fs::read_dir(INFINIBAND_CLASS_DIR) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            assert!(
                driver_of(&name).is_some(),
                "sysfs lists RDMA device {name} but its driver did not resolve — \
                 is_efa() would keep every device, filtering nothing",
            );
        }
    }
}
