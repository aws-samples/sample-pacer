//! The window a client owns and registers itself — the memory ADR-0030 is about.
//!
//! Two backings, one protocol. Host memory is an `mmap` this process owns, registered with
//! `ibv_reg_mr`; device memory is a `cuMemAlloc` this process exported as a dma-buf and
//! registered with `ibv_reg_dmabuf_mr`. The daemon cannot tell them apart, and that is the
//! ADR's central claim: it addresses an rkey and an offset and never maps the memory, so it
//! does not care which kind it is. Demonstrated on hardware while ADR-0027's `gpuTargets`
//! key still existed, with it *off* (`bench/ladder/results/c2-token-gate.md`) — the daemon
//! has had no GPU-facing configuration at all since that key was removed.
//!
//! Three things this module exists to get right:
//!
//! * **The registration is on the CLIENT's protection domain.** That is the whole point;
//!   an rkey is PD-scoped, so a window registered by the daemon would be the ADR-0026 path
//!   wearing a token's clothes.
//! * **Drop order is deregister-then-free, and it is enforced by field order.** The reverse
//!   makes `ibv_dereg_mr` fail `EINVAL` over freed pages, and the ibverbs crate panics in
//!   that destructor — the abort at the end of the c2-token gate (exit 134). Declared here
//!   once so no caller has to know.
//! * **The token's base address comes from the REGISTRATION, never from a pointer.** For a
//!   dma-buf window registered with `iova == offset` the two differ: `remote().addr` is the
//!   dma-buf offset, and a device pointer in a token names memory the NIC cannot reach
//!   (planning/21 § the dma-buf addressing trap).

use std::io;
use std::os::fd::{AsRawFd, OwnedFd};

use anyhow::{bail, Context, Result};
use ibverbs::{AccessFlags, MemoryRegion, ProtectionDomain};
use tracing::warn;

use crate::cuda::{Cuda, Device, DeviceBuffer, GPU_PAGE_BYTES};

/// Which pages back a host window.
///
/// A knob rather than a constant because registration pins per page: `ibv_reg_mr` was
/// measured at 12.3-14.4 GB/s on 4 KiB pages against 239-433 GB/s on hugepages (ADR-0028's
/// `reg-timing` spike). Under a token the registration happens once for the window's whole
/// life rather than per request, so this matters far less than it did for ADR-0026 — but a
/// 100 GB window still pays ~28 s of pinning on base pages, and the ladder's pods already
/// know how to ask for `hugepages-2Mi`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pages {
    /// Ordinary 4 KiB pages.
    Base,
    /// 2 MiB explicit hugepages — 512× fewer translations for the NIC to cache.
    Huge2Mi,
    /// 1 GiB explicit hugepages.
    Huge1Gi,
}

impl Pages {
    /// Page size in bytes; a mapping's length must be a multiple of it.
    pub fn bytes(self) -> usize {
        match self {
            Pages::Base => 4 << 10,
            Pages::Huge2Mi => 2 << 20,
            Pages::Huge1Gi => 1 << 30,
        }
    }

    /// Short label for a log line, so a result is never read without its page size.
    pub fn label(self) -> &'static str {
        match self {
            Pages::Base => "4KiB",
            Pages::Huge2Mi => "2MiB",
            Pages::Huge1Gi => "1GiB",
        }
    }

    /// `log2(page size)`, which is how `mmap` encodes an explicit hugepage size
    /// (`MAP_HUGE_SHIFT`); `None` for base pages, which take no encoding.
    fn huge_shift(self) -> Option<i32> {
        match self {
            Pages::Base => None,
            Pages::Huge2Mi => Some(21),
            Pages::Huge1Gi => Some(30),
        }
    }

    /// Parse the size an operator asked for, in MiB: `2` = 2 MiB, `1024` = 1 GiB, anything
    /// else (including `0`) = base pages.
    pub fn from_mib(mib: usize) -> Self {
        match mib {
            2 => Pages::Huge2Mi,
            1024 => Pages::Huge1Gi,
            _ => Pages::Base,
        }
    }
}

/// An anonymous private mapping this process owns, unmapped on drop.
///
/// Separate from the registration so that field order in [`Window`] can enforce
/// deregister-before-unmap without a hand-written `Drop` that has to get both right.
struct HostMapping {
    ptr: *mut u8,
    len: usize,
    pages: Pages,
}

// SAFETY: a private mapping this value exclusively owns from construction to drop; no
// aliasing handle is handed out except through `&self`/`&mut self`, so moving the owner
// moves the whole mapping.
unsafe impl Send for HostMapping {}

impl Drop for HostMapping {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`len` are exactly what `map_pages` returned and this is their sole
        // owner; any MR over them is already dropped (field order in `Window`).
        unsafe { libc::munmap(self.ptr.cast(), self.len) };
    }
}

/// What backs a window, and what only that backing can do.
enum Backing {
    /// Host memory: an `mmap` plus nothing else.
    Host(HostMapping),
    /// Device memory: the allocation, the dma-buf fd it was exported as (held for the
    /// window's life, since the MR references it), and the device whose primary context the
    /// allocation lives in.
    Gpu {
        /// The dma-buf fd. Held rather than closed: the kernel keeps its own reference, but
        /// keeping ours makes the ordering obvious and costs one descriptor.
        _dmabuf: OwnedFd,
        buf: DeviceBuffer,
        /// Retained so the primary context outlives the allocation in it.
        _device: Device,
    },
}

/// A registered window: the memory, plus one MR **per rail** whose `remote()` names it.
///
/// **One backing, N registrations.** An rkey is protection-domain scoped and each rail has
/// its own PD, so a window a client wants written from several rails must be registered on
/// each of them — the memory is mapped (or allocated and exported) exactly once and every
/// registration covers the same bytes. That is what lets a token name several destinations
/// for the same window, which is what lifts a delivery off one rail's ~11 GiB/s ceiling
/// (measured: `bench/ladder/results/c5-safetensors-8b.md`, where 16 shards in flight to one
/// client rail reached ~10 GiB/s and stopped).
///
/// The base address is shared and only the rkeys differ — for host memory every MR reports
/// the same virtual address, and for a dma-buf every export here uses `iova == 0` — which is
/// exactly the shape [`crate::token`] encodes: one address, one rkey per rail.
///
/// **Field order is the drop order and is load-bearing** — `mrs` is declared first so every
/// `ibv_dereg_mr` runs before the pages go away.
pub struct Window {
    /// One registration per rail, in the caller's rail order. Declared FIRST so they drop
    /// FIRST.
    mrs: Vec<MemoryRegion<()>>,
    backing: Backing,
    len: usize,
}

impl Window {
    /// Map and register `bytes` of host memory on `pd`.
    ///
    /// `pages` is a REQUEST: an explicit-hugepage `mmap` that the pool cannot satisfy warns
    /// and falls back to base pages rather than failing, so a run yields the base-page arm
    /// instead of nothing. Read [`Window::pages`] — never the request — when interpreting a
    /// result.
    ///
    /// # Errors
    ///
    /// The base-page `mmap` failing, or `ibv_reg_mr` failing (`RLIMIT_MEMLOCK`, or more
    /// pinned bytes than the host allows).
    pub fn host(pds: &[&ProtectionDomain], bytes: usize, pages: Pages) -> Result<Self> {
        let mapping = map_with_fallback(bytes, pages)?;
        let mut mrs = Vec::with_capacity(pds.len());
        for (index, pd) in pds.iter().enumerate() {
            // SAFETY: `ptr` is a live, writable, exclusively-owned mapping of exactly `len`
            // bytes that outlives every MR over it (field order + `Drop`), which is
            // `register_from_raw`'s requirement. Registering the same range on several PDs is
            // ordinary: each is an independent translation the NIC of that rail can use.
            mrs.push(
                unsafe { pd.register_from_raw(mapping.ptr, mapping.len, mr_access()) }
                    .with_context(|| {
                        format!(
                            "registering a {}-byte host window ({} pages) on rail {index}",
                            mapping.len,
                            mapping.pages.label()
                        )
                    })?,
            );
        }
        let len = mapping.len;
        Ok(Self {
            mrs,
            backing: Backing::Host(mapping),
            len,
        })
    }

    /// Allocate `bytes` in the HBM of CUDA device `ordinal`, export it as a dma-buf, and
    /// register that on `pd`.
    ///
    /// Sentinel-filled before registration so an untouched window is legible as such.
    ///
    /// # Errors
    ///
    /// No CUDA driver, no such device, the allocation or the export failing, or
    /// `ibv_reg_dmabuf_mr` failing — `EOPNOTSUPP` there means this kernel/driver pair has
    /// no dma-buf registration path at all.
    pub fn gpu(
        pds: &[&ProtectionDomain],
        bytes: usize,
        ordinal: i32,
        sentinel: u8,
    ) -> Result<Self> {
        let cuda = Cuda::open()?;
        let device = cuda.device(ordinal)?;
        tracing::info!(ordinal, summary = %device.summary()?, "GPU window: device open");
        let buf = device.alloc(bytes)?;
        buf.fill(sentinel).context("sentinel-filling the window")?;
        let len = buf.len();
        let dmabuf = buf
            .export_dmabuf(0, len)
            .context("exporting the GPU window as a dma-buf")?;
        let mut mrs = Vec::with_capacity(pds.len());
        for (index, pd) in pds.iter().enumerate() {
            // ONE export, registered on every rail: a dma-buf fd may be registered by several
            // devices, each building its own translation, and the fd is held for the window's
            // life either way. `iova == offset == 0` on all of them, so `remote().addr` is 0
            // for every rail and the token's single base address stays truthful.
            mrs.push(
                pd.register_dmabuf(dmabuf.as_raw_fd(), 0, len, 0, mr_access())
                    .with_context(|| {
                        format!("ibv_reg_dmabuf_mr(len={len}, iova=0) on rail {index}")
                    })?,
            );
        }
        Ok(Self {
            mrs,
            backing: Backing::Gpu {
                _dmabuf: dmabuf,
                buf,
                _device: device,
            },
            len,
        })
    }

    /// Registered length in bytes (host windows are page-rounded up, GPU windows
    /// [`GPU_PAGE_BYTES`]-rounded).
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the window is empty — never in practice; present for clippy beside
    /// [`Window::len`].
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// `(base address, rkey)` of rail `index` **as its registration reports them**, which is
    /// what a token must carry. For a dma-buf window the address is the dma-buf offset, not
    /// the device pointer.
    ///
    /// # Panics
    ///
    /// If `index` is not a rail this window was registered on — a caller bug, since the rail
    /// list is fixed at construction and the token is built from that same list.
    pub fn descriptor(&self, index: usize) -> (u64, u32) {
        let remote = self.mrs[index].remote();
        (remote.addr, remote.rkey)
    }

    /// How many rails this window is registered on.
    pub fn rail_count(&self) -> usize {
        self.mrs.len()
    }

    /// The host pointer, or `None` for a device window.
    pub fn host_ptr(&self) -> Option<*mut u8> {
        match &self.backing {
            Backing::Host(m) => Some(m.ptr),
            Backing::Gpu { .. } => None,
        }
    }

    /// The GPU virtual address, or `None` for a host window. What a consumer in this
    /// process addresses — and never what goes in a token.
    pub fn device_ptr(&self) -> Option<u64> {
        match &self.backing {
            Backing::Host(_) => None,
            Backing::Gpu { buf, .. } => Some(buf.ptr()),
        }
    }

    /// Which pages back this window: the realized size for a host window,
    /// [`GPU_PAGE_BYTES`] for a device one.
    pub fn pages(&self) -> usize {
        match &self.backing {
            Backing::Host(m) => m.pages.bytes(),
            Backing::Gpu { .. } => GPU_PAGE_BYTES,
        }
    }

    /// Set every byte of the window to `byte`.
    ///
    /// # Errors
    ///
    /// The device memset failing (host windows cannot fail).
    pub fn fill(&mut self, byte: u8) -> Result<()> {
        match &mut self.backing {
            Backing::Host(m) => {
                // SAFETY: a live writable mapping of `len` bytes, exclusively owned, and
                // `&mut self` guarantees no other reference exists.
                unsafe { std::slice::from_raw_parts_mut(m.ptr, m.len) }.fill(byte);
                Ok(())
            }
            Backing::Gpu { buf, .. } => buf.fill(byte),
        }
    }

    /// Copy `[offset, offset + dst.len())` of the window into host memory.
    ///
    /// The only way to read a device window, and therefore how a client digests what a NIC
    /// delivered into memory nothing else can read.
    ///
    /// # Errors
    ///
    /// A range past the end of the window, or the device→host copy failing.
    pub fn copy_out(&self, offset: usize, dst: &mut [u8]) -> Result<()> {
        match &self.backing {
            Backing::Host(m) => {
                let end = offset
                    .checked_add(dst.len())
                    .filter(|end| *end <= m.len)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "range {offset}+{} exceeds the {}-byte window",
                            dst.len(),
                            m.len
                        )
                    })?;
                // SAFETY: a live mapping of `len` bytes, and the range is checked above.
                let src = unsafe { std::slice::from_raw_parts(m.ptr, m.len) };
                dst.copy_from_slice(&src[offset..end]);
                Ok(())
            }
            Backing::Gpu { buf, .. } => buf.copy_out(offset, dst),
        }
    }

    /// Copy `src` into the window at `offset` — the leg a *control* arm pays (body read,
    /// then host→device) and a delivered arm does not.
    ///
    /// # Errors
    ///
    /// A range past the end of the window, or the host→device copy failing.
    pub fn copy_in(&mut self, offset: usize, src: &[u8]) -> Result<()> {
        match &mut self.backing {
            Backing::Host(m) => {
                let end = offset
                    .checked_add(src.len())
                    .filter(|end| *end <= m.len)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "range {offset}+{} exceeds the {}-byte window",
                            src.len(),
                            m.len
                        )
                    })?;
                // SAFETY: a live writable mapping of `len` bytes, exclusively owned via
                // `&mut self`; the range is checked above.
                let dst = unsafe { std::slice::from_raw_parts_mut(m.ptr, m.len) };
                dst[offset..end].copy_from_slice(src);
                Ok(())
            }
            Backing::Gpu { buf, .. } => buf.copy_in(offset, src),
        }
    }
}

/// MR access flags — the EFA-valid reduced set.
///
/// Never `PERMISSIVE`: that bundle includes the atomic bit, which EFA rejects outright
/// (spike finding 7), so a window registered with it fails to register at all.
fn mr_access() -> AccessFlags {
    AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_READ | AccessFlags::REMOTE_WRITE
}

/// Map `bytes` with the requested page size, degrading loudly to base pages.
///
/// # Errors
///
/// The base-page `mmap` failing.
fn map_with_fallback(bytes: usize, want: Pages) -> Result<HostMapping> {
    let rounded = bytes.next_multiple_of(want.bytes());
    match map_pages(rounded, want) {
        Ok(ptr) => Ok(HostMapping {
            ptr,
            len: rounded,
            pages: want,
        }),
        Err(e) if want != Pages::Base => {
            warn!(
                error = %e, bytes = rounded, requested = want.label(),
                "explicit-hugepage mmap failed (pod missing the hugepages-* resource, or \
                 the node pool exhausted); falling back to 4 KiB pages"
            );
            let len = bytes.next_multiple_of(Pages::Base.bytes());
            let ptr = map_pages(len, Pages::Base).context("mmap on base pages")?;
            Ok(HostMapping {
                ptr,
                len,
                pages: Pages::Base,
            })
        }
        Err(e) => Err(anyhow::Error::new(e)).context("mmap on base pages"),
    }
}

/// `mmap` an anonymous private region with an explicit page size.
///
/// `MAP_POPULATE` on the hugepage path is deliberate: without it a mapping the pool cannot
/// back fails with **SIGBUS at first touch** — mid-run and uncatchable — whereas
/// pre-faulting turns the same shortfall into an `ENOMEM` the caller can degrade from.
fn map_pages(len: usize, pages: Pages) -> io::Result<*mut u8> {
    let mut flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;
    if let Some(shift) = pages.huge_shift() {
        // The size goes in the flags word; `MAP_HUGETLB` alone silently takes the kernel
        // default (2 MiB on x86-64), which would make the 1 GiB arm a duplicate.
        flags |= libc::MAP_HUGETLB | libc::MAP_POPULATE | (shift << libc::MAP_HUGE_SHIFT);
    }
    // SAFETY: a null hint lets the kernel choose the address; `len > 0` is guaranteed by
    // callers, and the return value is checked against MAP_FAILED before use.
    let p = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            flags,
            -1,
            0,
        )
    };
    if p == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    Ok(p.cast())
}

/// Reject a window smaller than one page of whatever will back it, before anything is
/// mapped or allocated.
///
/// # Errors
///
/// A zero or sub-page request — which the daemon would reject as a zero-length window after
/// a round trip, and which for a GPU window would fail the dma-buf alignment check.
pub fn check_size(bytes: usize) -> Result<()> {
    if bytes == 0 {
        bail!("a delivery window needs a positive size");
    }
    Ok(())
}
