//! Block-device read/write counters for the cache directory's backing flash.
//!
//! # Why the daemon publishes this and not node_exporter
//!
//! Every disk-tier rate this repo has ever measured — foyer's (ADR-0025) and the chunk
//! store's (ADR-0033) alike — is a rate *the daemon computed about itself*. None of them
//! evidences that a byte left a device. That distinction is not academic here: the store's
//! writes are buffered and nothing drops those pages, so straight after a seed the whole
//! working set is page-cache resident and a read of it returns at memory speed. A tier
//! reporting hundreds of GiB while the flash moved ~0 is the exact shape of
//! `results/c4-foyer-readpath.md`, where foyer claimed 428.45 GB read against 0.58 GiB on
//! `md127`, and of the retraction in `results/laguna-store-remotewrite.md`, which could
//! only *infer* its reads were direct because nothing on the node had counted them.
//!
//! `bench/ladder/c5-safetensors.sh` and `bench/ladder/README.md` both already state the bar
//! in writing — "read the arm's `/proc/diskstats` delta before claiming otherwise" — and
//! neither the arm nor the harness could meet it. node_exporter cannot either: a restore
//! arm's timed region is ~30 s, well inside a scrape interval, and Prometheus' rate floor
//! would smear it across the pod's whole lifetime. So the counters are published here, where
//! [`crate::metrics`] already brackets everything else a paid arm reads, and
//! `clients/python/pacer_daemon_metrics.py` differences them over the timed region alone.
//!
//! # Why the daemon is the only process that can resolve the device set
//!
//! The bench pod cannot: `/proc/diskstats` is host-wide inside a container (it is not
//! namespaced, which is what makes this work at all), but naming which of a p6's devices
//! back the cache needs the cache directory, and only the daemon mounts it. Guessing is not
//! an option — `pacer_nvme_report` learned that `nvme0n1` is the EBS ROOT volume on this
//! AMI, so "sum every nvme device" lets a read of the wrong disk satisfy the check.
//!
//! # Why three procfs files and no `/sys`
//!
//! `mountinfo` states each mount's `maj:min` as a string, `diskstats` carries major and
//! minor in its first two fields, and `mdstat` names an array's members. That chain needs no
//! `st_dev` arithmetic (glibc's encoding is not the kernel's), no `libc`, and no sysfs mount
//! in the container — and every step is a pure function over a string, so the parsing rules
//! are tested from fixtures rather than discovered on a paid node.

use std::path::Path;

/// `/proc/diskstats` counts SECTORS, and a diskstats sector is 512 bytes on every device
/// regardless of the logical block size the queue reports — the kernel's unit here is fixed.
/// That fixed unit is the whole reason a member's count can be summed with its array's.
/// `clients/python/pacer_nvme_report.py` names the same constant for the same reason.
const SECTOR_BYTES: u64 = 512;

/// 0-indexed `/proc/diskstats` field holding the device name (field 3, 1-indexed).
const DISKSTATS_NAME: usize = 2;
/// 0-indexed `/proc/diskstats` field holding sectors READ (field 6, 1-indexed).
const DISKSTATS_SECTORS_READ: usize = 5;
/// 0-indexed `/proc/diskstats` field holding sectors WRITTEN (field 10, 1-indexed).
const DISKSTATS_SECTORS_WRITTEN: usize = 9;
/// A `/proc/diskstats` line shorter than this cannot carry the written-sectors field, so it
/// is a header or a truncated read rather than a device.
const DISKSTATS_MIN_FIELDS: usize = DISKSTATS_SECTORS_WRITTEN + 1;

/// 0-indexed `/proc/self/mountinfo` field holding `maj:min`  (field 3, 1-indexed).
const MOUNTINFO_DEVICE: usize = 2;
/// 0-indexed `/proc/self/mountinfo` field holding the mount point (field 5, 1-indexed).
const MOUNTINFO_MOUNT_POINT: usize = 4;
/// A `/proc/self/mountinfo` line shorter than this is truncated, not a mount.
const MOUNTINFO_MIN_FIELDS: usize = MOUNTINFO_MOUNT_POINT + 1;

/// One backing device's cumulative counters, in bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceCounters {
    /// Device name as `/proc/diskstats` spells it (`nvme1n1`, `md127`).
    pub device: String,
    /// Cumulative bytes read from the device since boot.
    pub read_bytes: u64,
    /// Cumulative bytes written to the device since boot.
    pub written_bytes: u64,
}

/// Resolve the devices actually backing `cache_dir`, reading the live procfs.
///
/// Returns the array's MEMBERS when the cache sits on an md array, and the device itself
/// otherwise — never both. Summing an array with its members would double every byte, since
/// the array counts exactly what its members do; returning members only makes the family
/// safe to sum with no rule for the reader to remember.
///
/// An empty result means the chain could not be resolved (no procfs, an overlay or tmpfs
/// cache dir, a device absent from `diskstats`). It is deliberately not an error: this is a
/// measurement aid, and a daemon must not fail to start because it cannot name its disk.
pub fn backing_devices(cache_dir: &Path) -> Vec<String> {
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    let diskstats = std::fs::read_to_string("/proc/diskstats").unwrap_or_default();
    let mdstat = std::fs::read_to_string("/proc/mdstat").unwrap_or_default();
    resolve(cache_dir, &mountinfo, &diskstats, &mdstat)
}

/// [`backing_devices`] over three procfs snapshots, so the whole chain is testable.
///
/// Kept separate for the reason `parse_vm_rss_bytes` in [`crate::metrics`] is: the rules
/// here (which field, which mount wins, how a member is spelled) are the ones that cost
/// something to get wrong, so they are the ones a fixture has to pin.
pub fn resolve(cache_dir: &Path, mountinfo: &str, diskstats: &str, mdstat: &str) -> Vec<String> {
    let Some(majmin) = mount_device(mountinfo, cache_dir) else {
        return Vec::new();
    };
    let Some(device) = device_name(diskstats, &majmin) else {
        return Vec::new();
    };
    let members = array_members(mdstat, &device);
    if members.is_empty() {
        vec![device]
    } else {
        members
    }
}

/// The `maj:min` of the mount that contains `path`.
///
/// LONGEST matching mount point wins, which is the only correct rule: `/` matches every
/// path, so a first or last match would report the root filesystem for a cache directory
/// that has its own mount — turning an NVMe check into an EBS one.
fn mount_device(mountinfo: &str, path: &Path) -> Option<String> {
    let path = path.to_str()?;
    let mut best: Option<(usize, &str)> = None;
    for line in mountinfo.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < MOUNTINFO_MIN_FIELDS {
            continue;
        }
        let point = fields[MOUNTINFO_MOUNT_POINT];
        if !contains_path(point, path) {
            continue;
        }
        if best.is_none_or(|(len, _)| point.len() > len) {
            best = Some((point.len(), fields[MOUNTINFO_DEVICE]));
        }
    }
    best.map(|(_, device)| device.to_owned())
}

/// Whether the mount at `point` contains `path`.
///
/// A plain `starts_with` would make `/var/cache/pacer-old` contain `/var/cache/pacer`, so
/// the boundary has to be a separator or an exact match. `/` is spelled without a trailing
/// separator yet contains everything, hence its own arm.
fn contains_path(point: &str, path: &str) -> bool {
    if point == "/" || point == path {
        return true;
    }
    path.strip_prefix(point)
        .is_some_and(|rest| rest.starts_with('/'))
}

/// The device name `diskstats` gives `maj:min`, whose first two fields are exactly that
/// pair — which is why this needs no `st_dev` decomposition.
fn device_name(diskstats: &str, majmin: &str) -> Option<String> {
    let (major, minor) = majmin.split_once(':')?;
    diskstats.lines().find_map(|line| {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < DISKSTATS_MIN_FIELDS {
            return None;
        }
        (fields[0] == major && fields[1] == minor).then(|| fields[DISKSTATS_NAME].to_owned())
    })
}

/// The member devices of md array `array`, or empty when it is not an array.
///
/// An `mdstat` member is spelled `nvme8n1[7]`, and a failed or spare one carries further
/// bracketed flags (`nvme8n1[7](F)`), so everything from the first `[` is dropped. The
/// tokens that carry no `[` at all are the state words (`active`, `raid0`) and are skipped
/// by that same rule rather than by a list of words to ignore, which would rot.
fn array_members(mdstat: &str, array: &str) -> Vec<String> {
    let Some(line) = mdstat
        .lines()
        .find(|line| line.split_whitespace().next() == Some(array))
    else {
        return Vec::new();
    };
    let mut members: Vec<String> = line
        .split_whitespace()
        .skip_while(|token| *token != ":")
        .filter_map(|token| token.split_once('[').map(|(name, _)| name.to_owned()))
        .filter(|name| !name.is_empty())
        .collect();
    members.sort_unstable();
    members
}

/// Cumulative counters for `devices`, read from the live `/proc/diskstats`.
///
/// Devices absent from the snapshot are omitted rather than reported as zero: a zero is
/// indistinguishable from "the device did nothing", which is the one answer this must never
/// invent.
pub fn sample(devices: &[String]) -> Vec<DeviceCounters> {
    let diskstats = std::fs::read_to_string("/proc/diskstats").unwrap_or_default();
    counters(&diskstats, devices)
}

/// [`sample`] over a `/proc/diskstats` snapshot, so the field offsets are testable.
pub fn counters(diskstats: &str, devices: &[String]) -> Vec<DeviceCounters> {
    let mut out = Vec::new();
    for line in diskstats.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < DISKSTATS_MIN_FIELDS {
            continue;
        }
        let name = fields[DISKSTATS_NAME];
        if !devices.iter().any(|device| device == name) {
            continue;
        }
        let sectors = |at: usize| fields[at].parse::<u64>().unwrap_or(0) * SECTOR_BYTES;
        out.push(DeviceCounters {
            device: name.to_owned(),
            read_bytes: sectors(DISKSTATS_SECTORS_READ),
            written_bytes: sectors(DISKSTATS_SECTORS_WRITTEN),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A p6-b200's real shape: eight instance-store drives in one RAID0 at
    /// `/mnt/k8s-disks/0`, bind-mounted into the daemon at `/var/cache/pacer`, plus the EBS
    /// root the AMI boots from. `nvme0n1` being the ROOT is the trap this fixture exists to
    /// hold: it must never appear in a resolved backing set.
    const MOUNTINFO: &str = "\
23 1 259:0 / / rw,relatime - ext4 /dev/nvme0n1p1 rw
31 23 0:24 / /sys rw,nosuid - sysfs sysfs rw
40 23 9:127 /pacer-cache /var/cache/pacer rw,noatime - xfs /dev/md127 rw
41 23 259:0 /var/log /var/log rw,relatime - ext4 /dev/nvme0n1p1 rw
";

    const MDSTAT: &str = "\
Personalities : [raid0]
md127 : active raid0 nvme2n1[1] nvme1n1[0]
      6251237376 blocks super 1.2 512k chunks

unused devices: <none>
";

    /// Sector counts, not bytes: 2048 sectors is 1 MiB. The header line and the short
    /// `loop0` line are both here because both appear on a real node and both must be
    /// skipped without panicking on an index.
    const DISKSTATS: &str = "\
 259       0 nvme0n1 100 0 4096 10 50 0 2048 5 0 0 0
   9     127 md127 200 0 20480 20 100 0 10240 10 0 0 0
 259       1 nvme1n1 150 0 8192 15 60 0 4096 6 0 0 0
 259       2 nvme2n1 150 0 12288 15 60 0 6144 6 0 0 0
   7       0 loop0 0 0
";

    #[test]
    fn the_cache_dir_resolves_to_the_array_members_and_never_the_ebs_root() {
        let devices = resolve(Path::new("/var/cache/pacer"), MOUNTINFO, DISKSTATS, MDSTAT);
        assert_eq!(devices, vec!["nvme1n1".to_owned(), "nvme2n1".to_owned()]);
        assert!(
            !devices.iter().any(|d| d == "nvme0n1"),
            "nvme0n1 is the EBS root on this AMI: including it would let a read of the \
             wrong disk satisfy the honesty check"
        );
        assert!(
            !devices.iter().any(|d| d == "md127"),
            "the array counts the same bytes as its members, so returning both would \
             double every total this family is summed for"
        );
    }

    /// The rule that makes the whole chain work or silently measure EBS instead.
    #[test]
    fn the_longest_mount_point_wins_rather_than_the_root_that_matches_everything() {
        assert_eq!(
            mount_device(MOUNTINFO, Path::new("/var/cache/pacer")).as_deref(),
            Some("9:127")
        );
        // A path under the mount, not the mount itself, still resolves to it.
        assert_eq!(
            mount_device(MOUNTINFO, Path::new("/var/cache/pacer/chunks/0")).as_deref(),
            Some("9:127")
        );
        // And a path on no special mount falls back to the root.
        assert_eq!(
            mount_device(MOUNTINFO, Path::new("/etc/pacer")).as_deref(),
            Some("259:0")
        );
    }

    #[test]
    fn a_sibling_mount_with_a_shared_prefix_does_not_capture_the_path() {
        let mountinfo = "40 23 9:127 / /var/cache/pacer-old rw - xfs /dev/md127 rw\n\
                         23 1 259:0 / / rw - ext4 /dev/nvme0n1p1 rw\n";
        assert_eq!(
            mount_device(mountinfo, Path::new("/var/cache/pacer")).as_deref(),
            Some("259:0"),
            "/var/cache/pacer-old must not contain /var/cache/pacer"
        );
    }

    #[test]
    fn a_cache_dir_on_a_bare_device_resolves_to_that_device() {
        let mountinfo = "40 23 259:1 / /var/cache/pacer rw - xfs /dev/nvme1n1 rw\n";
        let devices = resolve(Path::new("/var/cache/pacer"), mountinfo, DISKSTATS, MDSTAT);
        assert_eq!(devices, vec!["nvme1n1".to_owned()]);
    }

    /// An unresolvable chain reports nothing rather than guessing — the alternative is a
    /// device set nobody chose, which is worse than no device set at all.
    #[test]
    fn an_unresolvable_chain_yields_no_devices_rather_than_a_guess() {
        assert!(resolve(Path::new("/var/cache/pacer"), "", DISKSTATS, MDSTAT).is_empty());
        assert!(resolve(Path::new("/var/cache/pacer"), MOUNTINFO, "", MDSTAT).is_empty());
    }

    #[test]
    fn counters_convert_sectors_to_bytes_and_skip_short_lines() {
        let got = counters(DISKSTATS, &["nvme1n1".to_owned(), "loop0".to_owned()]);
        assert_eq!(
            got,
            vec![DeviceCounters {
                device: "nvme1n1".to_owned(),
                read_bytes: 8192 * SECTOR_BYTES,
                written_bytes: 4096 * SECTOR_BYTES,
            }],
            "loop0's line is too short to carry the written field, so it is skipped \
             rather than reported as zero"
        );
    }

    /// Reads and writes come from different fields, and swapping them would make a seed
    /// look like a restore. Pinned with values that cannot be confused.
    #[test]
    fn the_read_field_is_not_the_write_field() {
        let got = counters(DISKSTATS, &["md127".to_owned()]);
        assert_eq!(got[0].read_bytes, 20480 * SECTOR_BYTES);
        assert_eq!(got[0].written_bytes, 10240 * SECTOR_BYTES);
    }

    #[test]
    fn a_failed_member_flag_does_not_become_part_of_the_device_name() {
        let mdstat = "md127 : active raid0 nvme2n1[1](F) nvme1n1[0]\n";
        assert_eq!(
            array_members(mdstat, "md127"),
            vec!["nvme1n1".to_owned(), "nvme2n1".to_owned()]
        );
    }

    #[test]
    fn a_device_that_is_not_an_array_has_no_members() {
        assert!(array_members(MDSTAT, "nvme1n1").is_empty());
        assert!(array_members("", "md127").is_empty());
    }
}
