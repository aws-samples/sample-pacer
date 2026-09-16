//! The accounting the **kernel** kills on: this container's own cgroup memory
//! counters (planning/17 "OPEN DEFECT — the daemon OOMs on the requester side").
//!
//! [`crate::memstats`] answers *why the resident set grew*. This module answers a
//! prior question that neither it nor `process_resident_memory_bytes` can:
//! **how close is the container to the limit that ends it?**
//!
//! Those are not the same series, and the difference is the whole point:
//!
//! - `process_resident_memory_bytes` is `VmRSS` from `/proc/self/status`, which
//!   **excludes page cache**. Buffered `read()`/`pread()` populates page cache and
//!   `VmRSS` does not move.
//! - The cgroup limit acts on `memory.current`, which **includes** page cache
//!   (`memory.stat`'s `file`), because page cache is charged to the cgroup that
//!   instantiated it.
//!
//! foyer's disk tier does buffered I/O, so on a large-memory node the "NVMe tier"
//! largely *is* page cache — measured at **72.1 GiB resident in `Cached`** while
//! serving a 131 GiB checkpoint, against 0.52 GiB of device reads and `read_bytes:
//! 0` on the process (`bench/ladder/results/c4-fanout-depth.md`). A daemon can
//! therefore sit flat to the byte in every process-level series this repo had
//! while its cgroup marches to the limit, which is why `bench/ladder/memwatch.sh`'s
//! `RSS − foyer` gate is **structurally blind to the largest unaccounted term**
//! and why planning/17's two FLAT memgap arms cannot be read as clearing the
//! requester-side OOM.
//!
//! ## What each field is for
//!
//! - `current` against `max` is the pre-mortem series: the one plot on which an
//!   approaching kill is visible *before* it happens.
//! - `file` is the term the old instrument could not see. `file` growing with
//!   `anon` flat means the page cache is filling the cgroup — a budgeting or
//!   `config.diskTier` question, not a leak in our code.
//! - `oom` counts *reclaim failing under the limit*, which happens **before** any
//!   kill and can happen without one. A living daemon reporting a non-zero `oom`
//!   is the warning that the previously silent kill never gave.
//! - `oom_kill` is the kill itself. Its value after the fact is limited by where
//!   the counter lives — see [`CgroupMemory::oom_kill_events`].
//!
//! Sampled on the metrics scrape, never on a data path: three small `sysfs` reads
//! against a hierarchy located once per process.

use std::path::{Path, PathBuf};

/// One sample of this container's cgroup memory accounting, in bytes, plus the two
/// event counters.
///
/// Every field is `Option` on purpose: the two cgroup generations expose different
/// subsets, and a partial answer is worth far more than none — a v1 host still
/// yields `current`, `max` and `file`, which is the whole of the blindness this
/// module exists to remove.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CgroupMemory {
    /// `memory.current` (v2) / `memory.usage_in_bytes` (v1) — **the quantity the
    /// limit is compared against**, page cache included. This is the series
    /// `process_resident_memory_bytes` was wrongly documented as being.
    pub current_bytes: Option<u64>,
    /// `memory.max` (v2) / `memory.limit_in_bytes` (v1). `None` when the cgroup is
    /// unlimited — the literal `max` in v2, and in v1 a page-size-dependent sentinel
    /// (any value above 1 PiB) that is not a limit. An unlimited cgroup is not a
    /// limit of zero, and reporting it as 0 would make every headroom expression
    /// read as already exceeded.
    pub max_bytes: Option<u64>,
    /// `memory.stat`'s `file` (v2) / `total_cache` (v1): **page cache charged to
    /// this cgroup**. The term `VmRSS` cannot see and foyer's buffered disk tier
    /// fills.
    pub file_bytes: Option<u64>,
    /// `memory.stat`'s `file_dirty` (v2) / `total_dirty` (v1): page cache written
    /// but not yet flushed. It cannot be reclaimed until writeback finishes, so a
    /// large value is cache that will *not* yield under pressure — the difference
    /// between "reclaimable" and "about to be fatal".
    pub file_dirty_bytes: Option<u64>,
    /// `memory.stat`'s `file_writeback` (v2) / `total_writeback` (v1): pages
    /// currently being written back.
    pub file_writeback_bytes: Option<u64>,
    /// `memory.stat`'s `anon` (v2) / `total_rss` (v1): anonymous memory, which is
    /// the part of `current` that behaves like the process-level series.
    /// `current − anon − file` is what neither instrument attributes.
    pub anon_bytes: Option<u64>,
    /// `memory.events`' `oom` (v2 only): times an allocation could not be satisfied
    /// under the limit after reclaim. **Counted before any kill**, and it can
    /// advance without one — so unlike `oom_kill` this is readable by a daemon that
    /// is still alive, which makes it the alertable series of the pair.
    pub oom_events: Option<u64>,
    /// `memory.events`' `oom_kill` (v2) / `memory.oom_control`'s `oom_kill` (v1):
    /// tasks the OOM killer terminated in this cgroup.
    ///
    /// ⚠ **Read this with its scope in mind.** Under a cgroup namespace a container
    /// sees only its own cgroup, and Kubernetes gives each container *instance* a
    /// fresh one, so after a kill-and-restart this reads 0 again: it is evidence of
    /// a kill that did **not** take the whole container down (a child or a sibling
    /// task), not a durable epitaph. The durable evidence is [`Self::current_bytes`]
    /// against [`Self::max_bytes`] sampled over time — which is exactly what nothing
    /// in this repo published before.
    pub oom_kill_events: Option<u64>,
}

impl CgroupMemory {
    /// Whether nothing at all could be read. A sample whose every field is `None`
    /// is indistinguishable from having no cgroup, and publishing it would register
    /// zeros for `current` and `file` — which reads as a container using no memory
    /// and holding no page cache, the precise wrong conclusion this module exists
    /// to prevent.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// Where a cgroup-namespaced container's own hierarchy is mounted. Both
/// generations put the container's leaf here, which is why the common case needs no
/// `/proc/self/cgroup` parsing at all.
const CGROUP_MOUNT: &str = "/sys/fs/cgroup";

/// The v1 memory controller's subdirectory under [`CGROUP_MOUNT`] on a host-view
/// mount (no cgroup namespace), where each controller gets its own tree.
const V1_MEMORY_SUBDIR: &str = "memory";

/// The file whose presence proves a directory is a v2 memory cgroup.
const V2_MARKER: &str = "memory.current";

/// The v1 equivalent of [`V2_MARKER`]. v1 spells every quantity differently, which
/// is why the two generations get separate readers rather than one with fallbacks.
const V1_MARKER: &str = "memory.usage_in_bytes";

/// Above this, a v1 `memory.limit_in_bytes` is the kernel's "no limit" sentinel
/// rather than a limit.
///
/// v1 has no literal `max`: an unlimited cgroup reports `PAGE_COUNTER_MAX << 12`,
/// which is `0x7fff_ffff_ffff_f000` on a 64-bit kernel with 4 KiB pages — but the
/// exact value moves with the page size, so an equality test misreads a 64 KiB-page
/// kernel, and AL2023 on Graviton is one. 1 PiB is far above any real container
/// limit and far below every sentinel, so a threshold is both simpler and correct
/// on every page size.
const V1_UNLIMITED_FLOOR: u64 = 1 << 50;

/// Sample this container's cgroup memory accounting. `None` off Linux, and when no
/// cgroup memory controller can be found or nothing in it could be read — a
/// container without those files gets no series rather than a wrong one.
///
/// The hierarchy is located once and cached; every call after that is three small
/// `sysfs` reads. Called on the metrics scrape only.
#[cfg(target_os = "linux")]
pub fn sample() -> Option<CgroupMemory> {
    let sample = match hierarchy()? {
        Hierarchy::V2(root) => sample_v2(root),
        Hierarchy::V1(root) => sample_v1(root),
    };
    (!sample.is_empty()).then_some(sample)
}

/// Sample this container's cgroup memory accounting — the non-Linux build, which
/// has no cgroups at all. See the Linux variant for what the fields mean.
#[cfg(not(target_os = "linux"))]
pub fn sample() -> Option<CgroupMemory> {
    None
}

/// Which cgroup generation this container is under, and the directory holding its
/// memory files.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
enum Hierarchy {
    /// cgroup v2 (unified) — [`V2_MARKER`] lives in this directory.
    V2(PathBuf),
    /// cgroup v1 — [`V1_MARKER`] lives in this directory.
    V1(PathBuf),
}

/// Locate this container's memory cgroup, once per process.
///
/// A `OnceLock` rather than a lookup per scrape because the answer cannot change
/// while the process lives, and the miss path reads and parses
/// `/proc/self/cgroup`. Borrowing out of the cell is what keeps the located path
/// alive without leaking it.
#[cfg(target_os = "linux")]
fn hierarchy() -> Option<&'static Hierarchy> {
    static FOUND: std::sync::OnceLock<Option<Hierarchy>> = std::sync::OnceLock::new();
    FOUND.get_or_init(locate_hierarchy).as_ref()
}

/// The uncached half of [`hierarchy`].
///
/// Resolution order is cheapest and most likely first: a namespaced v2 leaf mounted
/// straight at [`CGROUP_MOUNT`] (what containerd gives a pod on AL2023), then a
/// namespaced v1 controller under it, then the host-view forms of each via
/// `/proc/self/cgroup`. `None` when none of them has the files, which is a
/// legitimate answer and not an error — a plain `cargo test` on a developer's
/// machine may well run in a cgroup with no memory controller delegated to it.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn locate_hierarchy() -> Option<Hierarchy> {
    let mount = Path::new(CGROUP_MOUNT);
    if mount.join(V2_MARKER).exists() {
        return Some(Hierarchy::V2(mount.to_path_buf()));
    }
    let v1_mount = mount.join(V1_MEMORY_SUBDIR);
    if v1_mount.join(V1_MARKER).exists() {
        return Some(Hierarchy::V1(v1_mount));
    }
    // Host-view mount: the container can see the whole tree, so its own leaf has to
    // be read out of `/proc/self/cgroup`.
    let proc = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    if let Some(rel) = parse_v2_path(&proc) {
        let dir = join_cgroup_path(mount, rel);
        if dir.join(V2_MARKER).exists() {
            return Some(Hierarchy::V2(dir));
        }
    }
    let rel = parse_v1_memory_path(&proc)?;
    let dir = join_cgroup_path(&v1_mount, rel);
    dir.join(V1_MARKER).exists().then_some(Hierarchy::V1(dir))
}

/// Join a `/proc/self/cgroup` path (always absolute, and `/` for the root) onto a
/// mount point.
///
/// `Path::join` alone would treat the leading `/` as a new root and discard the
/// mount, which is the classic way this lookup silently reads the **host's**
/// numbers instead of the container's — the worst available failure here, because
/// it produces plausible values rather than none.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn join_cgroup_path(mount: &Path, rel: &str) -> PathBuf {
    mount.join(rel.trim_start_matches('/'))
}

/// Read the v2 files.
///
/// Every field is independently optional: a delegated cgroup can legitimately lack
/// `memory.max` or `memory.events`, and losing the rest of the sample over that
/// would reintroduce exactly the blindness being fixed.
#[cfg(target_os = "linux")]
fn sample_v2(root: &Path) -> CgroupMemory {
    let stat = read(root, "memory.stat");
    let events = read(root, "memory.events");
    let stat_field = |key: &str| stat.as_deref().and_then(|s| parse_keyed(s, key));
    let event = |key: &str| events.as_deref().and_then(|s| parse_keyed(s, key));
    CgroupMemory {
        current_bytes: read(root, V2_MARKER).as_deref().and_then(parse_u64),
        max_bytes: read(root, "memory.max").as_deref().and_then(parse_v2_max),
        file_bytes: stat_field("file"),
        file_dirty_bytes: stat_field("file_dirty"),
        file_writeback_bytes: stat_field("file_writeback"),
        anon_bytes: stat_field("anon"),
        oom_events: event("oom"),
        oom_kill_events: event("oom_kill"),
    }
}

/// Read the v1 files, whose names and keys all differ from v2's.
///
/// `total_*` rather than the bare keys: the bare ones exclude descendant cgroups,
/// while the limit a v1 container is killed on applies to the whole subtree.
#[cfg(target_os = "linux")]
fn sample_v1(root: &Path) -> CgroupMemory {
    let stat = read(root, "memory.stat");
    let stat_field = |key: &str| stat.as_deref().and_then(|s| parse_keyed(s, key));
    CgroupMemory {
        current_bytes: read(root, V1_MARKER).as_deref().and_then(parse_u64),
        max_bytes: read(root, "memory.limit_in_bytes")
            .as_deref()
            .and_then(parse_v1_limit),
        file_bytes: stat_field("total_cache"),
        file_dirty_bytes: stat_field("total_dirty"),
        file_writeback_bytes: stat_field("total_writeback"),
        anon_bytes: stat_field("total_rss"),
        // v1 has no `oom` counter — only the kill count, and only on kernels that
        // added it to `memory.oom_control`. Absent is the honest answer.
        oom_events: None,
        oom_kill_events: read(root, "memory.oom_control")
            .as_deref()
            .and_then(|s| parse_keyed(s, "oom_kill")),
    }
}

/// Read one cgroup file, discarding every failure.
///
/// A file that cannot be read is an absent field, never a scrape error: losing the
/// memory series is the exact situation this module exists to prevent.
#[cfg(target_os = "linux")]
fn read(root: &Path, name: &str) -> Option<String> {
    std::fs::read_to_string(root.join(name)).ok()
}

/// Parse a single-integer cgroup file (`memory.current`, `memory.usage_in_bytes`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_u64(body: &str) -> Option<u64> {
    body.trim().parse().ok()
}

/// Parse `memory.max`. `None` for the literal `max`, which is v2's spelling of
/// unlimited — not a value to be parsed, and not a number to publish.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_v2_max(body: &str) -> Option<u64> {
    let body = body.trim();
    if body == "max" {
        return None;
    }
    body.parse().ok()
}

/// Parse `memory.limit_in_bytes`. `None` for v1's no-limit sentinel — see
/// [`V1_UNLIMITED_FLOOR`] for why this is a threshold and not an equality.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_v1_limit(body: &str) -> Option<u64> {
    parse_u64(body).filter(|limit| *limit < V1_UNLIMITED_FLOOR)
}

/// Look up `key` in a `key value` file (`memory.stat`, `memory.events`,
/// `memory.oom_control`).
///
/// Matches the whole first field, never a prefix: `file` and `file_dirty` are
/// distinct keys that a `starts_with` would conflate, and so are `oom` and
/// `oom_kill`. Those are the two pairs this is called with, which is not a
/// coincidence — a prefix match here would report dirty pages as the entire page
/// cache and one kill as three OOM events, both plausible-looking wrong numbers.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_keyed(body: &str, key: &str) -> Option<u64> {
    for line in body.lines() {
        let mut fields = line.split_whitespace();
        if fields.next() == Some(key) {
            return fields.next()?.parse().ok();
        }
    }
    None
}

/// The unified-hierarchy path from `/proc/self/cgroup` — the line whose controller
/// list is empty (`0::/some/path`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_v2_path(proc_cgroup: &str) -> Option<&str> {
    proc_cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .map(str::trim)
}

/// The v1 memory controller's path from `/proc/self/cgroup`.
///
/// The controller field is a comma-separated list, so `memory` can share a line
/// with co-mounted siblings — `memory,hugetlb` and the `cpu,cpuacct` style pairs
/// are both routine — which a substring match on `:memory:` misses entirely.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_v1_memory_path(proc_cgroup: &str) -> Option<&str> {
    proc_cgroup.lines().find_map(|line| {
        // `hierarchy-id:controllers:path`, and the path may itself contain a colon,
        // so this splits into exactly three parts from the left.
        let mut parts = line.splitn(3, ':');
        let (_id, controllers, path) = (parts.next()?, parts.next()?, parts.next()?);
        controllers
            .split(',')
            .any(|c| c == V1_MEMORY_SUBDIR)
            .then_some(path.trim())
    })
}

#[cfg(test)]
mod tests {
    use super::{
        join_cgroup_path, parse_keyed, parse_u64, parse_v1_limit, parse_v1_memory_path,
        parse_v2_max, parse_v2_path, CgroupMemory,
    };
    use std::path::Path;

    /// A verbatim `memory.stat` head from a v2 container, truncated at the keys this
    /// module reads plus enough neighbours to prove the lookup is not positional —
    /// the real file is ~50 lines and their order is not contractual.
    ///
    /// The numbers are the measured c4 shape: 72.1 GiB of page cache against
    /// 8.25 GiB of anon.
    const V2_STAT: &str = "anon 8858370048
file 77424939008
kernel 1173209088
kernel_stack 3768320
pagetables 24449024
percpu 1044480
sock 0
vmalloc 0
shmem 0
zswap 0
file_mapped 0
file_dirty 90112
file_writeback 4096
anon_thp 0
inactive_anon 8858333184
active_file 41306386432
";

    /// The whole of a v2 `memory.events`.
    const V2_EVENTS: &str = "low 0\nhigh 0\nmax 12\noom 3\noom_kill 1\noom_group_kill 0\n";

    /// **The defect in one assertion.** `VmRSS` — and therefore memwatch's
    /// `RSS − foyer` gate — sees the `anon` number and not the `file` one, while the
    /// cgroup limit that does the killing is compared against a `memory.current`
    /// containing both. On this measured shape the invisible term is nearly 9× the
    /// visible one.
    #[test]
    fn page_cache_is_the_term_rss_cannot_see() {
        const GIB: f64 = (1_u64 << 30) as f64;
        let file = parse_keyed(V2_STAT, "file").expect("file present");
        let anon = parse_keyed(V2_STAT, "anon").expect("anon present");
        assert!(
            (file as f64 / GIB - 72.1).abs() < 0.1,
            "page cache misparsed: {file}"
        );
        assert!(
            (anon as f64 / GIB - 8.25).abs() < 0.1,
            "anon misparsed: {anon}"
        );
        assert!(file > anon * 8, "the blind term must dominate this fixture");
    }

    /// `file` and `file_dirty` are different keys, and so are `oom` and `oom_kill`.
    #[test]
    fn keys_match_whole_fields_not_prefixes() {
        assert_eq!(parse_keyed(V2_STAT, "file"), Some(77_424_939_008));
        assert_eq!(parse_keyed(V2_STAT, "file_dirty"), Some(90_112));
        assert_eq!(parse_keyed(V2_STAT, "file_writeback"), Some(4_096));
        assert_eq!(parse_keyed(V2_EVENTS, "oom"), Some(3));
        assert_eq!(parse_keyed(V2_EVENTS, "oom_kill"), Some(1));
        // A key that is a prefix of a present one must be absent, not the value of
        // whatever line happened to start with the same letters.
        assert_eq!(parse_keyed(V2_STAT, "fil"), None);
        assert_eq!(parse_keyed(V2_EVENTS, "oom_kil"), None);
    }

    /// `oom` advancing while `oom_kill` stays put is the pre-mortem state this
    /// module exists to surface: reclaim failed under the limit and nothing has been
    /// killed yet. The parse has to keep them independent for that to be readable.
    #[test]
    fn reclaim_failures_are_counted_apart_from_kills() {
        let events = "low 0\nhigh 0\nmax 41\noom 7\noom_kill 0\n";
        assert_eq!(parse_keyed(events, "oom"), Some(7));
        assert_eq!(parse_keyed(events, "oom_kill"), Some(0));
    }

    /// v2 spells "unlimited" as the literal `max`, which must not become a number.
    /// Publishing 0 for it would make `max − current` read as a container already
    /// over its limit on every unlimited pod in the fleet.
    #[test]
    fn v2_unlimited_is_absent_not_zero() {
        assert_eq!(parse_v2_max("max\n"), None);
        assert_eq!(parse_v2_max("111669149696\n"), Some(111_669_149_696));
    }

    /// v1's sentinel is page-size dependent, so it is rejected by magnitude. The
    /// first two of these are real: 4 KiB pages, then 64 KiB pages (AL2023 on
    /// Graviton).
    #[test]
    fn v1_unlimited_sentinels_are_absent_on_every_page_size() {
        assert_eq!(parse_v1_limit("9223372036854771712\n"), None);
        assert_eq!(parse_v1_limit("9223372036854710272\n"), None);
        assert_eq!(parse_v1_limit("111669149696\n"), Some(111_669_149_696));
    }

    /// v1 keys are `total_*`, because the bare ones exclude descendants while the
    /// limit applies to the subtree — and the two must not be confusable.
    #[test]
    fn v1_stat_uses_subtree_totals() {
        let stat = "cache 100\nrss 200\ntotal_cache 77424939008\ntotal_rss 8858370048\n\
                    total_dirty 90112\ntotal_writeback 4096\n";
        assert_eq!(parse_keyed(stat, "total_cache"), Some(77_424_939_008));
        assert_eq!(parse_keyed(stat, "total_rss"), Some(8_858_370_048));
        assert_eq!(parse_keyed(stat, "total_dirty"), Some(90_112));
        assert_eq!(parse_keyed(stat, "total_writeback"), Some(4_096));
        assert_eq!(parse_keyed(stat, "cache"), Some(100));
    }

    /// The v2 line is the one with an EMPTY controller list, and a real container's
    /// `/proc/self/cgroup` under a namespace says `/`.
    #[test]
    fn v2_path_comes_from_the_empty_controller_line() {
        assert_eq!(parse_v2_path("0::/\n"), Some("/"));
        assert_eq!(
            parse_v2_path("0::/kubepods.slice/kubepods-pod123.slice/cri-containerd-abc.scope\n"),
            Some("/kubepods.slice/kubepods-pod123.slice/cri-containerd-abc.scope")
        );
        // A pure v1 file has no unified line at all.
        assert_eq!(parse_v2_path("11:memory:/docker/abc\n"), None);
        assert_eq!(
            parse_v1_memory_path("11:memory:/docker/abc\n"),
            Some("/docker/abc")
        );
    }

    /// `memory` routinely shares its line with a co-mounted controller, so the field
    /// is a comma-separated LIST. Matching `:memory:` would find neither of the
    /// first two of these.
    #[test]
    fn v1_memory_controller_may_be_co_mounted() {
        assert_eq!(
            parse_v1_memory_path("4:memory,hugetlb:/kubepods/pod1\n"),
            Some("/kubepods/pod1")
        );
        assert_eq!(
            parse_v1_memory_path("3:cpu,cpuacct:/a\n4:hugetlb,memory:/b\n"),
            Some("/b")
        );
        // `memory` must match as a whole controller name, never as a substring of
        // another: a `memory_foo` controller is not this one.
        assert_eq!(parse_v1_memory_path("5:memory_foo:/x\n"), None);
        // And a path containing a colon survives, because the split is bounded.
        assert_eq!(
            parse_v1_memory_path("4:memory:/odd:name\n"),
            Some("/odd:name")
        );
    }

    /// A `/proc/self/cgroup` path is absolute, and `Path::join` on an absolute path
    /// DISCARDS the mount point — which would silently read the host's numbers
    /// instead of this container's.
    #[test]
    fn an_absolute_cgroup_path_does_not_escape_the_mount() {
        assert_eq!(
            join_cgroup_path(Path::new("/sys/fs/cgroup"), "/kubepods/pod1"),
            Path::new("/sys/fs/cgroup/kubepods/pod1")
        );
        // The namespaced-root case must stay at the mount rather than becoming a
        // trailing-slash path that fails to `exists()`.
        assert_eq!(
            join_cgroup_path(Path::new("/sys/fs/cgroup"), "/"),
            Path::new("/sys/fs/cgroup")
        );
    }

    /// Malformed and absent input must both be an absent FIELD rather than a zero or
    /// a panic — a scrape has to survive a kernel that spells something differently,
    /// and a 0 would read as "this container uses no memory".
    #[test]
    fn malformed_input_is_absent_not_zero() {
        assert_eq!(parse_u64("not a number\n"), None);
        assert_eq!(parse_u64(""), None);
        assert_eq!(parse_keyed("file\n", "file"), None);
        assert_eq!(parse_keyed("file notanumber\n", "file"), None);
        assert_eq!(parse_v2_max("wat\n"), None);
        assert_eq!(parse_v1_limit(""), None);
        assert!(parse_v2_path("").is_none());
        assert!(parse_v1_memory_path("garbage\n").is_none());
    }

    /// An all-absent sample is reported as no sample, because publishing it would
    /// register zeros for `current` and `file`.
    #[test]
    fn an_empty_sample_is_recognised_as_no_sample() {
        assert!(CgroupMemory::default().is_empty());
        assert!(!CgroupMemory {
            current_bytes: Some(0),
            ..CgroupMemory::default()
        }
        .is_empty());
    }

    /// Wherever this runs — a v2 container, a v1 host, a developer's macOS laptop —
    /// sampling must not panic and must not fail. That is the whole portability
    /// contract, and it is asserted rather than assumed because a panic here would
    /// take down a metrics scrape.
    #[test]
    fn sampling_is_safe_everywhere() {
        // Nothing about the environment is asserted: only the promise that a sample,
        // if one is taken at all, is never the empty one.
        if let Some(s) = super::sample() {
            assert!(!s.is_empty(), "an empty sample must be reported as None");
        }
    }
}
