//! Rail↔NUMA↔core placement (planning/19 D5 step 0), ported from the
//! transport-only bench's `spike/efa/src/affinity.rs`.
//!
//! **Why this is not an optimization.** planning/18's ~58 GiB/s host-memory
//! aggregate — the bar track D is measured against — was produced by a bench that
//! placed every rail deliberately, and its own module doc records what happens
//! without that placement. Two mechanisms, both specific to a dual-socket
//! p5.48xlarge with 16 rails per PCIe complex:
//!
//! 1. **Where a rail's registered memory lives.** A one-sided WRITE's local NIC
//!    DMA-*reads* its source from host memory, and the requester's NIC DMA-*writes*
//!    the landing range. If that memory sits on the far socket, every byte crosses
//!    the inter-socket link. Registering while pinned to a CPU on the NIC's own
//!    NUMA node first-touches (and `ibv_reg_mr`-pins) the pages locally.
//! 2. **Where a rail's completion reaper runs.** It blocks on the completion
//!    channel fd and is woken per batch; unpinned, several reapers pack onto shared
//!    cores and — measured, in the bench's words — "their wakeup latency starves
//!    the in-flight window ... per-rail rate *collapses* as rails are added ... the
//!    signature of contention, not memory bandwidth".
//!
//! The daemon had neither until now, and D4 measured exactly that signature:
//! 344 ms WRITE-completion waits against 1.6 µs posts, 0.76 of 192 cores busy,
//! all 32 rails uniformly at ~4 % of line rate, and per-rail throughput falling as
//! rails were added (planning/18's own daemon arms: 7.618 GiB/s on one rail →
//! 0.46 GiB/s per rail on 32). So this module exists to make the daemon's
//! comparison against the bench a fair one before any further tuning is attempted.
//!
//! **Best-effort by construction.** Every entry point degrades — unreadable
//! sysfs, an unknown NUMA node, a refused `sched_setaffinity` — to round-robin or
//! to a no-op, and only ever costs the placement win. A daemon must boot on a host
//! whose topology it cannot read, so nothing here is allowed to fail startup. The
//! operator can also switch the whole thing off
//! ([`super::RailPlacementPolicy::Unpinned`], `PACER_RDMA_AFFINITY=0`), which is
//! what makes an A/B on one image possible.
//!
//! Linux-only in effect: the sysfs paths simply do not exist elsewhere, and the
//! `efa` feature this module lives behind is Linux-only anyway.

use std::collections::HashMap;
use std::io;

use anyhow::{Context, Result};
use tracing::{debug, info, warn};

/// sysfs directory whose entries are the kernel RDMA device names
/// (`ibv_get_device_name`), each with a `device/numa_node` attribute.
const SYS_INFINIBAND: &str = "/sys/class/infiniband";
/// sysfs directory holding one `node<N>/cpulist` per NUMA node.
const SYS_NODE: &str = "/sys/devices/system/node";
/// sysfs file listing the online CPUs — the fallback pool when a rail's NUMA node
/// cannot be determined.
const SYS_CPU_ONLINE: &str = "/sys/devices/system/cpu/online";
/// Kernel sentinel in `device/numa_node` for "not attached to any NUMA node" —
/// treated as unknown, which falls back to the round-robin pool.
const NUMA_NODE_NONE: i32 = -1;

/// One rail's resolved placement: the CPU its completion reaper pins to and its
/// registered memory is first-touched on, plus the NUMA node that CPU belongs to.
///
/// `numa_node: None` means the topology was unreadable for this rail, so `cpu`
/// came from the online-CPU pool rather than the NIC's node — the distinction
/// matters when reading a measurement, because an unpinned-memory run is not
/// testing the same thing (see the module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RailPlacement {
    /// Logical CPU id this rail's reaper pins to, and the CPU whose node its
    /// arenas are registered from.
    pub cpu: usize,
    /// NUMA node the rail's NIC reports; `None` when unknown.
    pub numa_node: Option<u32>,
}

impl RailPlacement {
    /// The placement that means "do not place anything" — what
    /// [`super::RailPlacementPolicy::Unpinned`] resolves to, and the value every
    /// pin/registration site checks against. Kept as a named constructor rather
    /// than an `Option<RailPlacement>` at each site so the *policy* is expressed
    /// once and the call sites stay branch-free.
    #[must_use]
    pub fn unpinned() -> Self {
        Self {
            cpu: usize::MAX,
            numa_node: None,
        }
    }

    /// Whether this placement names a CPU worth pinning to.
    #[must_use]
    pub fn is_pinned(&self) -> bool {
        self.cpu != usize::MAX
    }
}

/// Assign every rail a distinct CPU on its own NIC's NUMA node, reading the
/// topology from sysfs.
///
/// `dev_names` is in `ibverbs` enumeration order — the same order rails are
/// brought up in — so index `i` of the result is rail `i`'s placement. A rail
/// whose node is unknown draws from the online-CPU pool instead.
///
/// See [`assign_cpus`] for the allocation rule; this function is only the sysfs
/// half, which is why the interesting logic is tested and this is not.
#[must_use]
pub fn plan_placements(dev_names: &[Option<String>]) -> Vec<RailPlacement> {
    let nodes: Vec<Option<u32>> = dev_names
        .iter()
        .map(|n| n.as_deref().and_then(device_numa_node))
        .collect();
    let mut pools: HashMap<Option<u32>, Vec<usize>> = HashMap::new();
    let fallback = online_cpus();
    for node in nodes.iter().copied() {
        pools.entry(node).or_insert_with(|| match node {
            Some(n) => {
                let cpus = node_cpus(n);
                if cpus.is_empty() {
                    fallback.clone()
                } else {
                    cpus
                }
            }
            None => fallback.clone(),
        });
    }
    assign_cpus(&nodes, &pools)
}

/// Hand each rail a CPU from its node's pool, rotating a per-pool cursor so
/// co-node rails never land on the same core — distinct cores are the whole point
/// of mechanism 2 in the module doc. The cursor wraps if a node has fewer CPUs
/// than rails (never true on p5, where 16 rails share 48+ cores per socket), which
/// degrades to sharing rather than to failing.
///
/// Split out from [`plan_placements`] so the allocation rule is unit-testable
/// without a real machine's sysfs.
#[must_use]
fn assign_cpus(
    nodes: &[Option<u32>],
    pools: &HashMap<Option<u32>, Vec<usize>>,
) -> Vec<RailPlacement> {
    let mut cursors: HashMap<Option<u32>, usize> = HashMap::new();
    nodes
        .iter()
        .map(|node| {
            let pool = pools.get(node);
            let cursor = cursors.entry(*node).or_insert(0);
            let placement = match pool {
                Some(cpus) if !cpus.is_empty() => RailPlacement {
                    cpu: cpus[*cursor % cpus.len()],
                    numa_node: *node,
                },
                // No CPU pool at all (sysfs gave us nothing): stay unpinned
                // rather than guess CPU 0 and pile every rail onto one core.
                _ => RailPlacement::unpinned(),
            };
            *cursor += 1;
            placement
        })
        .collect()
}

/// Read a RDMA device's NUMA node from
/// `/sys/class/infiniband/<dev>/device/numa_node`. `None` when the file is
/// absent/unparsable or holds [`NUMA_NODE_NONE`].
fn device_numa_node(dev: &str) -> Option<u32> {
    let raw = std::fs::read_to_string(format!("{SYS_INFINIBAND}/{dev}/device/numa_node")).ok()?;
    match raw.trim().parse::<i32>().ok()? {
        NUMA_NODE_NONE => None,
        n if n >= 0 => Some(n as u32),
        _ => None,
    }
}

/// Parse a Linux cpulist (`"0-47,96-143"`, `"3"`, `"0,2,4-6"`) into CPU ids.
/// Unparsable fragments are skipped rather than failing the whole list: a partial
/// pool still places rails better than no pool at all.
fn parse_cpu_list(spec: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    for part in spec.trim().split(',').filter(|p| !p.is_empty()) {
        match part.split_once('-') {
            Some((lo, hi)) => {
                if let (Ok(lo), Ok(hi)) = (lo.trim().parse::<usize>(), hi.trim().parse::<usize>()) {
                    cpus.extend(lo..=hi);
                }
            }
            None => {
                if let Ok(c) = part.trim().parse::<usize>() {
                    cpus.push(c);
                }
            }
        }
    }
    cpus
}

/// CPUs on one NUMA node (`/sys/devices/system/node/node<N>/cpulist`).
fn node_cpus(node: u32) -> Vec<usize> {
    std::fs::read_to_string(format!("{SYS_NODE}/node{node}/cpulist"))
        .map(|s| parse_cpu_list(&s))
        .unwrap_or_default()
}

/// All online CPUs — the pool for rails whose NUMA node is unknown. Falls back to
/// `0..available_parallelism` if even that read fails.
fn online_cpus() -> Vec<usize> {
    if let Ok(s) = std::fs::read_to_string(SYS_CPU_ONLINE) {
        let cpus = parse_cpu_list(&s);
        if !cpus.is_empty() {
            return cpus;
        }
    }
    let n = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    (0..n).collect()
}

/// Pin the CALLING thread to `placement`'s single CPU, or do nothing if the
/// placement is [`RailPlacement::unpinned`].
///
/// Best-effort on purpose: a refused `sched_setaffinity` (restrictive cgroup CPU
/// set, offline CPU, unusual container runtime) warns once and continues
/// unpinned. Losing the placement win is a throughput regression; refusing to
/// serve is an outage.
pub fn pin_current_thread(placement: RailPlacement, what: &str) {
    if !placement.is_pinned() {
        return;
    }
    if let Err(e) = set_affinity(&[placement.cpu]) {
        warn!(
            error = %e, cpu = placement.cpu, what,
            "pinning to the rail's NUMA-local CPU failed; continuing unpinned \
             (expect the cross-node completion-latency signature)"
        );
    }
}

// No `clear_affinity` counterpart on purpose: every thread this module pins is one
// the transport OWNS and runs to completion — a reaper for the process's life, or a
// scoped thread that registers one rail's arenas and dies. Nothing here narrows a
// borrowed tokio worker's mask, so there is nothing to hand back. (The bench needed
// a restore because it pinned its main thread per rail in a loop.)

/// Set the calling thread's CPU affinity mask to exactly `cpus`.
///
/// # Errors
///
/// The `sched_setaffinity` syscall failing.
fn set_affinity(cpus: &[usize]) -> Result<()> {
    // SAFETY: `set` is a stack-owned `cpu_set_t`, zeroed and then populated only
    // through the libc CPU_* macros; `sched_setaffinity(0, ...)` targets the
    // calling thread and reads exactly `size_of::<cpu_set_t>()` bytes of it, all
    // in bounds. The `unsafe` is inherent to the affinity syscall having no safe
    // wrapper in std.
    // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
    let rc = unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        for &c in cpus {
            libc::CPU_SET(c, &mut set);
        }
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set)
    };
    if rc != 0 {
        return Err(anyhow::Error::new(io::Error::last_os_error()))
            .context("sched_setaffinity for the calling thread");
    }
    Ok(())
}

/// Log where the rails landed: one `debug` line each plus an `info` summary of
/// rails-per-NUMA-node.
///
/// This is the first thing to read when an aggregate does not scale, and the
/// summary is deliberately at `info`: a run whose rails all report the same node
/// (or none) is not measuring NUMA-local placement at all, and a result recorded
/// without knowing that is uninterpretable — the same trap as reading a hugepage
/// arm without checking which pages it actually got.
pub fn log_placements(dev_names: &[Option<String>], placements: &[RailPlacement]) {
    let mut per_node: HashMap<Option<u32>, usize> = HashMap::new();
    for (i, p) in placements.iter().enumerate() {
        let dev = dev_names
            .get(i)
            .and_then(Option::as_deref)
            .unwrap_or("unknown");
        debug!(rail = i, dev, numa = ?p.numa_node, cpu = p.cpu, pinned = p.is_pinned(), "rail placement");
        *per_node.entry(p.numa_node).or_insert(0) += 1;
    }
    let pinned = placements.iter().filter(|p| p.is_pinned()).count();
    let nodes: Vec<String> = {
        let mut v: Vec<_> = per_node.into_iter().collect();
        v.sort_by_key(|(node, _)| *node);
        v.into_iter()
            .map(|(node, n)| match node {
                Some(node) => format!("node{node}={n}"),
                None => format!("unknown={n}"),
            })
            .collect()
    };
    info!(
        rails = placements.len(),
        pinned,
        distribution = %nodes.join(","),
        "rail placement resolved (NUMA-local arenas + pinned reapers)"
    );
    if pinned == 0 && !placements.is_empty() {
        warn!(
            "no rail could be placed (sysfs unreadable, or affinity disabled) — arenas are \
             not node-local and reapers are not pinned; measured at 15.7 GiB/s in this \
             configuration against ~58 GiB/s with rails placed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cpulist shapes Linux actually emits: a range, a single CPU, and the
    /// two-socket p5 form (`0-47,96-143`).
    #[test]
    fn cpu_lists_parse() {
        assert_eq!(parse_cpu_list("3"), vec![3]);
        assert_eq!(parse_cpu_list("0-3"), vec![0, 1, 2, 3]);
        assert_eq!(parse_cpu_list("0,2,4-6"), vec![0, 2, 4, 5, 6]);
        assert_eq!(parse_cpu_list("0-1,96-97\n"), vec![0, 1, 96, 97]);
        assert_eq!(parse_cpu_list(""), Vec::<usize>::new());
        // A malformed fragment is skipped, not fatal — a partial pool still
        // places rails better than none.
        assert_eq!(parse_cpu_list("0-1,bogus,4"), vec![0, 1, 4]);
    }

    /// The allocation rule: rails spread across DISTINCT cores of their own node,
    /// which is what removes reaper contention (module doc, mechanism 2).
    #[test]
    fn co_node_rails_get_distinct_cpus() {
        let nodes = vec![Some(0), Some(1), Some(0), Some(1)];
        let pools = HashMap::from([(Some(0), vec![0, 1, 2]), (Some(1), vec![48, 49, 50])]);
        let got = assign_cpus(&nodes, &pools);
        assert_eq!(got[0].cpu, 0);
        assert_eq!(got[1].cpu, 48);
        assert_eq!(got[2].cpu, 1, "second rail on node 0 must not reuse cpu 0");
        assert_eq!(got[3].cpu, 49);
        assert_eq!(got[0].numa_node, Some(0));
        assert_eq!(got[1].numa_node, Some(1));
    }

    /// More rails than cores wraps rather than failing — sharing a core is worse
    /// than distinct cores but far better than not booting.
    #[test]
    fn cpu_assignment_wraps_when_the_pool_is_smaller_than_the_rail_count() {
        let nodes = vec![Some(0), Some(0), Some(0)];
        let pools = HashMap::from([(Some(0), vec![7, 8])]);
        let got = assign_cpus(&nodes, &pools);
        assert_eq!(got.iter().map(|p| p.cpu).collect::<Vec<_>>(), vec![7, 8, 7]);
    }

    /// An empty pool must yield `unpinned`, never CPU 0 — piling every rail onto
    /// core 0 would be strictly worse than leaving the scheduler alone.
    #[test]
    fn an_empty_pool_yields_unpinned_not_cpu_zero() {
        let nodes = vec![None, None];
        let pools = HashMap::from([(None, Vec::new())]);
        let got = assign_cpus(&nodes, &pools);
        assert!(got.iter().all(|p| !p.is_pinned()));
        assert!(!RailPlacement::unpinned().is_pinned());
    }
}
