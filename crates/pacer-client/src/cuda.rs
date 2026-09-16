//! The slice of the CUDA driver API a delivery client needs, `dlopen`ed.
//!
//! Mirrors `spike/efa/src/cuda.rs` (which validated every one of these calls on an H100)
//! and `pacer_daemon::cuda` (the ADR-0027 path this supersedes), trimmed to what the
//! *client* half of ADR-0030 does: allocate a window in HBM, mark it for third-party DMA,
//! export it as a dma-buf so the client's own NIC can register it, and copy in or out for
//! verification.
//!
//! Three deliberate choices, each of which has a wrong-looking alternative:
//!
//! * **`dlopen`, not a link-time dependency.** The loader images this ships into carry no
//!   CUDA at build time; the driver is injected at *run* time by the NVIDIA container
//!   runtime. Linking `libcuda` would make the shared object unloadable on a CPU node,
//!   which is where its host-window arm is supposed to work.
//! * **The device's PRIMARY context, retained — never `cuCtxCreate`.** This library is
//!   loaded into a process that already has CUDA up (a torch loader), and a pointer
//!   allocated in one context cannot be used from another. Retaining the primary context
//!   is what makes the delivered window addressable as a `torch` tensor in the same
//!   process; a fresh context would hand back a pointer torch cannot touch.
//! * **`Arc<Cuda>`, not a borrow.** The window outlives the call that made it and is owned
//!   by a struct that crosses a thread boundary, so the driver handle is refcounted rather
//!   than borrowed — the spike could use `&'a Cuda` only because it leaks and exits.
//!
//! **Why the `_v2` symbol names:** `cuda.h` `#define`s `cuMemAlloc` to `cuMemAlloc_v2` (and
//! likewise free/memset/copies/mem-info); resolving by name gets the frozen CUDA-1.x ABI
//! with 32-bit sizes unless the suffix is asked for explicitly.

use std::ffi::{c_char, c_int, c_uchar, c_uint, c_ulonglong, c_void, CStr};
use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use tracing::warn;

/// `CUresult` — 0 is success, everything else is rendered through `cuGetErrorString`.
type CUresult = c_int;
/// `CUdevice` — an opaque device handle, not the ordinal it was resolved from.
type CUdevice = c_int;
/// `CUdeviceptr` — a GPU virtual address.
type CUdeviceptr = u64;
/// `CUcontext` — opaque; only ever retained and made current, so its layout never matters.
type CUcontext = *mut c_void;

/// `CUDA_SUCCESS`.
const CUDA_SUCCESS: CUresult = 0;
/// `CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD` — the handle type `ibv_reg_dmabuf_mr` takes, and
/// the only NIC registration path device memory has on EFA (`nvidia_peermem` cannot load
/// there: upstream `ib_core` does not export `ib_register_peer_memory_client`).
const HANDLE_TYPE_DMA_BUF_FD: c_uint = 1;
/// `CU_POINTER_ATTRIBUTE_SYNC_MEMOPS`, set to 1 on every window this allocates.
///
/// Required, not cosmetic: it tells the driver that operations on this range must be
/// synchronous with respect to other agents, which is what makes a third party — here the
/// NIC, DMA-ing into HBM — see coherent data. rdma-core's own CUDA dma-buf test
/// (`tests/test_cuda_dmabuf.py`) sets it before exporting a handle.
const ATTR_SYNC_MEMOPS: c_int = 6;
/// Granularity every exported range is aligned and sized to: the GPU page size.
///
/// Both `cuMemGetHandleForAddressRange` and `ibv_reg_dmabuf_mr` describe their range in
/// dma-buf terms, so a sub-page-aligned base or length is where a registration silently
/// covers the wrong bytes. Kept in step with `pacer_daemon::cuda::GPU_PAGE_BYTES` and
/// `pacer_nic.py`'s `GPU_PAGE_BYTES`.
pub const GPU_PAGE_BYTES: usize = 64 << 10;
/// Bytes reserved for `cuDeviceGetName`; CUDA truncates rather than failing.
const DEVICE_NAME_CAP: usize = 256;
/// Bytes reserved for `cuDeviceGetPCIBusId`. The documented form is
/// `domain:bus:device.function` (13 characters); CUDA's own header suggests 13, and this
/// leaves room for a wider domain plus the NUL rather than silently truncating an address
/// that discovery then fails to match.
const PCI_BUS_ID_CAP: usize = 32;
/// Paths tried for the driver library, in order. The container runtime normally puts it on
/// the loader's search path, so the bare soname hits first; the rest are the layouts seen
/// when only `LD_LIBRARY_PATH` was wired up (`/usr/local/nvidia/lib64` is the classic
/// Kubernetes device-plugin mount point).
const LIBCUDA_CANDIDATES: [&str; 4] = [
    "libcuda.so.1",
    "/usr/lib/x86_64-linux-gnu/libcuda.so.1",
    "/usr/lib64/libcuda.so.1",
    "/usr/local/nvidia/lib64/libcuda.so.1",
];

/// The resolved driver entry points, one field per symbol typed as its C prototype — so a
/// signature mistake is a compile error at the call site rather than stack corruption.
struct Syms {
    init: unsafe extern "C" fn(c_uint) -> CUresult,
    driver_get_version: unsafe extern "C" fn(*mut c_int) -> CUresult,
    device_get_count: unsafe extern "C" fn(*mut c_int) -> CUresult,
    device_get: unsafe extern "C" fn(*mut CUdevice, c_int) -> CUresult,
    device_get_name: unsafe extern "C" fn(*mut c_char, c_int, CUdevice) -> CUresult,
    device_get_pci_bus_id: unsafe extern "C" fn(*mut c_char, c_int, CUdevice) -> CUresult,
    primary_ctx_retain: unsafe extern "C" fn(*mut CUcontext, CUdevice) -> CUresult,
    ctx_set_current: unsafe extern "C" fn(CUcontext) -> CUresult,
    mem_get_info: unsafe extern "C" fn(*mut usize, *mut usize) -> CUresult,
    mem_alloc: unsafe extern "C" fn(*mut CUdeviceptr, usize) -> CUresult,
    mem_free: unsafe extern "C" fn(CUdeviceptr) -> CUresult,
    memset_d8: unsafe extern "C" fn(CUdeviceptr, c_uchar, usize) -> CUresult,
    memcpy_h_to_d: unsafe extern "C" fn(CUdeviceptr, *const c_void, usize) -> CUresult,
    memcpy_d_to_h: unsafe extern "C" fn(*mut c_void, CUdeviceptr, usize) -> CUresult,
    pointer_set_attribute: unsafe extern "C" fn(*const c_void, c_int, CUdeviceptr) -> CUresult,
    get_handle_for_address_range:
        unsafe extern "C" fn(*mut c_void, CUdeviceptr, usize, c_uint, c_ulonglong) -> CUresult,
    get_error_string: unsafe extern "C" fn(CUresult, *mut *const c_char) -> CUresult,
}

/// The loaded CUDA driver library. Never unloaded — the device pointers and the primary
/// context handed out assume it stays mapped for the process.
pub struct Cuda {
    sym: Syms,
}

// SAFETY: `Syms` is a table of function pointers into a library that is never unloaded,
// and the driver API is thread-safe. Nothing here holds per-thread state: the one
// thread-affine call, `cuCtxSetCurrent`, is issued explicitly by whoever needs it.
unsafe impl Send for Cuda {}
unsafe impl Sync for Cuda {}

impl Cuda {
    /// `dlopen` the driver, resolve every entry point, and `cuInit`.
    ///
    /// Safe to call in a process that already initialized CUDA: `cuInit` is idempotent.
    ///
    /// # Errors
    ///
    /// No candidate path loading (usually a pod that requested no GPU, so the container
    /// runtime injected no driver), a symbol missing (`cuMemGetHandleForAddressRange`
    /// needs a driver ≥ CUDA 11.7), or `cuInit` failing.
    pub fn open() -> Result<Arc<Self>> {
        let mut attempts = Vec::with_capacity(LIBCUDA_CANDIDATES.len());
        for path in LIBCUDA_CANDIDATES {
            match dlopen(path) {
                Ok(lib) => {
                    // SAFETY: `lib` is a live `dlopen` handle and every field's type is
                    // the C prototype of the symbol named beside it.
                    let sym = unsafe { Syms::load(lib) }
                        .with_context(|| format!("resolving CUDA symbols in {path}"))?;
                    let cuda = Self { sym };
                    // SAFETY: resolved entry point; the flags word must be 0.
                    cuda.check(unsafe { (cuda.sym.init)(0) }, "cuInit")?;
                    return Ok(Arc::new(cuda));
                }
                Err(e) => attempts.push(format!("{path}: {e}")),
            }
        }
        bail!(
            "no CUDA driver library could be loaded — does this pod request \
             `nvidia.com/gpu` (the container runtime injects libcuda.so.1 only then)? \
             tried:\n  {}",
            attempts.join("\n  ")
        )
    }

    /// Turn a `CUresult` into a `Result`, rendering the driver's own message.
    ///
    /// # Errors
    ///
    /// Any non-success code, annotated with `what`.
    fn check(&self, code: CUresult, what: &str) -> Result<()> {
        if code == CUDA_SUCCESS {
            return Ok(());
        }
        let mut msg: *const c_char = std::ptr::null();
        // SAFETY: resolved entry point, valid out-pointer; on success the driver writes a
        // pointer to a static NUL-terminated string.
        let got = unsafe { (self.sym.get_error_string)(code, &mut msg) };
        let text = if got == CUDA_SUCCESS && !msg.is_null() {
            // SAFETY: driver-owned static string, valid for the process.
            unsafe { CStr::from_ptr(msg) }
                .to_string_lossy()
                .into_owned()
        } else {
            format!("unknown CUDA error {code}")
        };
        Err(anyhow!("{what} failed: {text} (CUresult {code})"))
    }

    /// Retain `ordinal`'s primary context, make it current on this thread, and describe the
    /// device in one line for the run log.
    ///
    /// The ordinal is this process's own — narrow the view with `CUDA_VISIBLE_DEVICES`, not
    /// with `NVIDIA_VISIBLE_DEVICES`, on a cluster that injects GPUs through CDI.
    ///
    /// # Errors
    ///
    /// No device at `ordinal`, or any of the retain/set-current/query calls failing.
    pub fn device(self: &Arc<Self>, ordinal: i32) -> Result<Device> {
        let mut count = 0;
        // SAFETY: resolved entry point, valid out-pointer.
        self.check(
            unsafe { (self.sym.device_get_count)(&mut count) },
            "cuDeviceGetCount",
        )?;
        if ordinal >= count {
            bail!("no CUDA device {ordinal}: this process sees {count} (CUDA_VISIBLE_DEVICES?)");
        }
        let mut dev: CUdevice = 0;
        // SAFETY: resolved entry point, valid out-pointer, bounds-checked ordinal.
        self.check(
            unsafe { (self.sym.device_get)(&mut dev, ordinal) },
            "cuDeviceGet",
        )?;
        let mut ctx: CUcontext = std::ptr::null_mut();
        // SAFETY: resolved entry point, valid out-pointer. Retained for the process — the
        // window allocated in it outlives every call here.
        self.check(
            unsafe { (self.sym.primary_ctx_retain)(&mut ctx, dev) },
            "cuDevicePrimaryCtxRetain",
        )?;
        let device = Device {
            cuda: Arc::clone(self),
            dev,
            ctx,
        };
        device.make_current()?;
        Ok(device)
    }

    /// Driver version as `cuDriverGetVersion` reports it (12040 = 12.4), or `None` if the
    /// call fails — a log line must never cost a run.
    pub fn driver_version(&self) -> Option<i32> {
        let mut version = 0;
        // SAFETY: resolved entry point, valid out-pointer.
        (unsafe { (self.sym.driver_get_version)(&mut version) } == CUDA_SUCCESS).then_some(version)
    }
}

/// One GPU, with its primary context retained.
pub struct Device {
    cuda: Arc<Cuda>,
    dev: CUdevice,
    ctx: CUcontext,
}

// SAFETY: `ctx` is a handle to the device's PRIMARY context, which is process-wide rather
// than thread-owned — a retained primary context is exactly the kind that may be made
// current on any thread, and the driver's own calls are thread-safe. What is per-thread is
// *which* context is current, and every operation that depends on that binds it first
// ([`DeviceBuffer::bind`]) instead of assuming the caller's thread.
unsafe impl Send for Device {}

impl Device {
    /// Make this device's primary context current on the calling thread.
    ///
    /// Allocation targets whichever context is current, and every thread has its own
    /// current context — so a thread that will allocate or copy calls this first. In a torch
    /// process this is the context torch itself uses, which is what makes the window
    /// addressable as a tensor.
    ///
    /// # Errors
    ///
    /// `cuCtxSetCurrent` failing.
    pub fn make_current(&self) -> Result<()> {
        // SAFETY: resolved entry point; `ctx` came from a successful retain and is held for
        // the life of this `Device`.
        self.cuda.check(
            unsafe { (self.cuda.sym.ctx_set_current)(self.ctx) },
            "cuCtxSetCurrent",
        )
    }

    /// One line describing the device: name and free/total HBM.
    ///
    /// Free HBM is the number that explains the most common failure — a window larger than
    /// what is left after the model — so it is logged before the allocation, not after it
    /// fails.
    ///
    /// This device's PCI bus address, as `domain:bus:device.function`.
    ///
    /// The key to rail discovery, and the reason it can work from inside a container at all:
    /// **CUDA reports the HOST address even when the container ordinal is 0**. A pod given
    /// one GPU cannot learn which of the node's eight it holds from its ordinal — that is
    /// always 0 — but this says so directly, which is what lets
    /// [`crate::topology`] resolve the rails on that GPU's own PCIe switch instead of
    /// trusting a per-instance table.
    ///
    /// # Errors
    ///
    /// The query failing, or the driver returning something with no NUL terminator.
    pub fn pci_bus_id(&self) -> Result<String> {
        let mut buf = vec![0i8; PCI_BUS_ID_CAP];
        // SAFETY: resolved entry point; the buffer is `PCI_BUS_ID_CAP` bytes and CUDA
        // truncates to the length it is given.
        self.cuda.check(
            unsafe {
                (self.cuda.sym.device_get_pci_bus_id)(
                    buf.as_mut_ptr().cast(),
                    PCI_BUS_ID_CAP as c_int,
                    self.dev,
                )
            },
            "cuDeviceGetPCIBusId",
        )?;
        // SAFETY: the driver wrote a NUL-terminated string into the buffer above.
        Ok(unsafe { CStr::from_ptr(buf.as_ptr().cast()) }
            .to_string_lossy()
            .into_owned())
    }

    /// # Errors
    ///
    /// The name or mem-info query failing.
    pub fn summary(&self) -> Result<String> {
        let mut name = vec![0i8; DEVICE_NAME_CAP];
        // SAFETY: resolved entry point; the buffer is `DEVICE_NAME_CAP` bytes and CUDA
        // truncates to the length it is given.
        self.cuda.check(
            unsafe {
                (self.cuda.sym.device_get_name)(
                    name.as_mut_ptr().cast(),
                    DEVICE_NAME_CAP as c_int,
                    self.dev,
                )
            },
            "cuDeviceGetName",
        )?;
        // SAFETY: the driver wrote a NUL-terminated string into the buffer above.
        let name = unsafe { CStr::from_ptr(name.as_ptr().cast()) }
            .to_string_lossy()
            .into_owned();
        let (mut free, mut total) = (0usize, 0usize);
        // SAFETY: resolved entry point, two valid out-pointers.
        self.cuda.check(
            unsafe { (self.cuda.sym.mem_get_info)(&mut free, &mut total) },
            "cuMemGetInfo",
        )?;
        /// Bytes per GiB, for the log line only.
        const BYTES_PER_GIB: f64 = 1_073_741_824.0;
        Ok(format!(
            "{name}, hbm {:.1}/{:.1} GiB free, driver {:?}",
            free as f64 / BYTES_PER_GIB,
            total as f64 / BYTES_PER_GIB,
            self.cuda.driver_version(),
        ))
    }

    /// Allocate `len` bytes of device memory (rounded up to [`GPU_PAGE_BYTES`]) and mark it
    /// `SYNC_MEMOPS`, which a third-party DMA agent needs to see coherent data.
    ///
    /// # Errors
    ///
    /// The allocation failing (out of HBM), or the pointer attribute being rejected.
    pub fn alloc(&self, len: usize) -> Result<DeviceBuffer> {
        let len = len.next_multiple_of(GPU_PAGE_BYTES);
        let mut ptr: CUdeviceptr = 0;
        // SAFETY: resolved entry point, valid out-pointer, non-zero length.
        self.cuda
            .check(
                unsafe { (self.cuda.sym.mem_alloc)(&mut ptr, len) },
                "cuMemAlloc",
            )
            .with_context(|| format!("allocating a {len}-byte window in HBM"))?;
        let buf = DeviceBuffer {
            cuda: Arc::clone(&self.cuda),
            ctx: self.ctx,
            ptr,
            len,
        };
        let flag: c_int = 1;
        // SAFETY: resolved entry point; `&flag` points to the `c_int` the attribute expects
        // and `ptr` is the allocation just returned.
        self.cuda.check(
            unsafe {
                (self.cuda.sym.pointer_set_attribute)(
                    (&flag as *const c_int).cast(),
                    ATTR_SYNC_MEMOPS,
                    ptr,
                )
            },
            "cuPointerSetAttribute(SYNC_MEMOPS)",
        )?;
        Ok(buf)
    }
}

/// An owned device allocation: the GPU virtual address, its length, and the operations a
/// delivery window needs. Freed on drop.
///
/// **Drop order matters and is the caller's problem.** Freeing this while a `MemoryRegion`
/// still covers it makes the deregistration fail (`ibv_dereg_mr: Invalid argument`) and the
/// ibverbs crate panics in a destructor — the abort observed at the end of the c2-token
/// gate. Whoever holds both must declare the MR *before* the buffer.
pub struct DeviceBuffer {
    cuda: Arc<Cuda>,
    /// The primary context the allocation lives in, so every operation can bind it — see
    /// [`DeviceBuffer::bind`].
    ctx: CUcontext,
    ptr: CUdeviceptr,
    len: usize,
}

// SAFETY: the allocation is owned exclusively by this value and reachable only through it;
// the driver's own calls are thread-safe, and the pointer is valid in the primary context
// regardless of which thread touches it (that context is process-wide). The one per-thread
// dependency — which context is current — is removed by binding it in every operation.
unsafe impl Send for DeviceBuffer {}

impl DeviceBuffer {
    /// Make this allocation's context current on the calling thread.
    ///
    /// Called by every operation below rather than once at construction, because the current
    /// context is **per thread** and this library is driven over a C ABI from whichever
    /// thread a loader happens to use — a Python thread pool, or the pump. Without it, a
    /// copy issued from a fresh thread fails `CUDA_ERROR_INVALID_CONTEXT` (or, worse,
    /// targets whatever context that thread had). It is a TLS write when the context is
    /// already current, which is the common case in a torch process: torch uses the same
    /// primary context.
    ///
    /// # Errors
    ///
    /// `cuCtxSetCurrent` failing.
    fn bind(&self) -> Result<()> {
        // SAFETY: resolved entry point; `ctx` is a primary context retained for the life of
        // the `Device` that produced this buffer.
        self.cuda.check(
            unsafe { (self.cuda.sym.ctx_set_current)(self.ctx) },
            "cuCtxSetCurrent",
        )
    }
    /// The GPU virtual address. What a consumer in this process (a torch tensor over
    /// `__cuda_array_interface__`) addresses — and **never** what goes in a token.
    pub fn ptr(&self) -> u64 {
        self.ptr
    }

    /// Allocated length, after page rounding.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Set every byte of the window to `byte`.
    ///
    /// The point of filling before a delivery is to make "nothing arrived" distinguishable
    /// from "the wrong bytes arrived": with a sentinel, an untouched window is legible.
    ///
    /// # Errors
    ///
    /// `cuMemsetD8` failing.
    pub fn fill(&self, byte: u8) -> Result<()> {
        self.bind()?;
        // SAFETY: resolved entry point over this value's own live allocation.
        self.cuda.check(
            unsafe { (self.cuda.sym.memset_d8)(self.ptr, byte, self.len) },
            "cuMemsetD8",
        )
    }

    /// Copy `src` into the window at `offset` — the host→device leg a control arm pays and
    /// a delivered arm does not.
    ///
    /// # Errors
    ///
    /// A range past the end of the window, or `cuMemcpyHtoD` failing.
    pub fn copy_in(&self, offset: usize, src: &[u8]) -> Result<()> {
        self.check_range(offset, src.len())?;
        self.bind()?;
        // SAFETY: resolved entry point; the destination range is bounds-checked above and
        // `src` is a live slice of the stated length.
        self.cuda.check(
            unsafe {
                (self.cuda.sym.memcpy_h_to_d)(
                    self.ptr + offset as u64,
                    src.as_ptr().cast(),
                    src.len(),
                )
            },
            "cuMemcpyHtoD",
        )
    }

    /// Copy the window's `[offset, offset + dst.len())` out to host memory — how a client
    /// digests what a NIC delivered into memory nothing else can read.
    ///
    /// # Errors
    ///
    /// A range past the end of the window, or `cuMemcpyDtoH` failing.
    pub fn copy_out(&self, offset: usize, dst: &mut [u8]) -> Result<()> {
        self.check_range(offset, dst.len())?;
        self.bind()?;
        // SAFETY: resolved entry point; the source range is bounds-checked above and `dst`
        // is a live writable slice of the stated length.
        self.cuda.check(
            unsafe {
                (self.cuda.sym.memcpy_d_to_h)(
                    dst.as_mut_ptr().cast(),
                    self.ptr + offset as u64,
                    dst.len(),
                )
            },
            "cuMemcpyDtoH",
        )
    }

    /// Export `[offset, offset + len)` as a **dma-buf fd** — the handle
    /// `ibv_reg_dmabuf_mr` registers, and on EFA the only one device memory has.
    ///
    /// # Errors
    ///
    /// A misaligned or out-of-bounds range, or the driver rejecting the export (a driver
    /// older than CUDA 11.7, or one built without dma-buf support).
    pub fn export_dmabuf(&self, offset: usize, len: usize) -> Result<OwnedFd> {
        self.check_range(offset, len)?;
        self.bind()?;
        let base = self.ptr + offset as u64;
        if !base.is_multiple_of(GPU_PAGE_BYTES as u64) || !len.is_multiple_of(GPU_PAGE_BYTES) {
            bail!(
                "a dma-buf export range must be {GPU_PAGE_BYTES}-byte aligned, got base \
                 {base:#x} len {len}"
            );
        }
        let mut fd: c_int = -1;
        // SAFETY: resolved entry point. For DMA_BUF_FD the handle out-parameter is an
        // `int*`, which is what is passed; the range is bounds- and alignment-checked
        // above; flags must be 0 per the driver API.
        self.cuda.check(
            unsafe {
                (self.cuda.sym.get_handle_for_address_range)(
                    (&mut fd as *mut c_int).cast(),
                    base,
                    len,
                    HANDLE_TYPE_DMA_BUF_FD,
                    0,
                )
            },
            "cuMemGetHandleForAddressRange(DMA_BUF_FD)",
        )?;
        if fd < 0 {
            bail!("cuMemGetHandleForAddressRange succeeded but returned fd {fd}");
        }
        // SAFETY: a fresh owned descriptor the driver just handed us and nothing else
        // holds, so transferring ownership to `OwnedFd` is sound.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    /// Bounds-check `[offset, offset + len)` against the allocation.
    ///
    /// # Errors
    ///
    /// The range overflowing or reaching past the end of the buffer.
    fn check_range(&self, offset: usize, len: usize) -> Result<()> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| anyhow!("range {offset}+{len} overflows"))?;
        if end > self.len {
            bail!("range {offset}+{len} exceeds the {}-byte window", self.len);
        }
        Ok(())
    }
}

impl Drop for DeviceBuffer {
    fn drop(&mut self) {
        // The free needs the allocation's context current on THIS thread, exactly as every
        // other operation does — and a `Client` may well be dropped on a thread that never
        // touched CUDA. A failure here is reported by the free below, which is where it is
        // legible.
        let _ = self.bind();
        // SAFETY: resolved entry point; `ptr` came from `cuMemAlloc` in a context retained
        // for the process, and this is the sole owner.
        let code = unsafe { (self.cuda.sym.mem_free)(self.ptr) };
        if code != CUDA_SUCCESS {
            // Warned, not panicked: a destructor that panics turns a teardown detail into
            // an abort, which is how the c2-token gate ended with exit 134.
            warn!(code, ptr = format!("{:#x}", self.ptr), "cuMemFree failed");
        }
    }
}

impl Syms {
    /// Resolve every entry point in `lib`.
    ///
    /// # Errors
    ///
    /// Any symbol being absent — for `cuMemGetHandleForAddressRange` that means the driver
    /// predates CUDA 11.7 and dma-buf export is simply unavailable.
    ///
    /// # Safety
    ///
    /// `lib` must be a live `dlopen` handle for the CUDA driver library; each symbol is
    /// transmuted to the prototype declared on its field, which must match the driver ABI.
    unsafe fn load(lib: *mut c_void) -> Result<Self> {
        Ok(Self {
            init: dlsym_as(lib, "cuInit")?,
            driver_get_version: dlsym_as(lib, "cuDriverGetVersion")?,
            device_get_count: dlsym_as(lib, "cuDeviceGetCount")?,
            device_get: dlsym_as(lib, "cuDeviceGet")?,
            device_get_name: dlsym_as(lib, "cuDeviceGetName")?,
            device_get_pci_bus_id: dlsym_as(lib, "cuDeviceGetPCIBusId")?,
            primary_ctx_retain: dlsym_as(lib, "cuDevicePrimaryCtxRetain")?,
            ctx_set_current: dlsym_as(lib, "cuCtxSetCurrent")?,
            mem_get_info: dlsym_as(lib, "cuMemGetInfo_v2")?,
            mem_alloc: dlsym_as(lib, "cuMemAlloc_v2")?,
            mem_free: dlsym_as(lib, "cuMemFree_v2")?,
            memset_d8: dlsym_as(lib, "cuMemsetD8_v2")?,
            memcpy_h_to_d: dlsym_as(lib, "cuMemcpyHtoD_v2")?,
            memcpy_d_to_h: dlsym_as(lib, "cuMemcpyDtoH_v2")?,
            pointer_set_attribute: dlsym_as(lib, "cuPointerSetAttribute")?,
            get_handle_for_address_range: dlsym_as(lib, "cuMemGetHandleForAddressRange")?,
            get_error_string: dlsym_as(lib, "cuGetErrorString")?,
        })
    }
}

/// `dlopen` one path, or the loader's own error message.
///
/// # Errors
///
/// The library not loading, with `dlerror`'s text.
fn dlopen(path: &str) -> Result<*mut c_void> {
    let c_path = std::ffi::CString::new(path).context("library path contains a NUL")?;
    // SAFETY: a valid NUL-terminated path; the flags are the ordinary lazy-local pair.
    let handle = unsafe { libc::dlopen(c_path.as_ptr(), libc::RTLD_LAZY | libc::RTLD_LOCAL) };
    if handle.is_null() {
        bail!("{}", dlerror());
    }
    Ok(handle)
}

/// Resolve `name` in `lib` and transmute it to the caller's function-pointer type.
///
/// # Errors
///
/// The symbol being absent.
///
/// # Safety
///
/// `T` must be the exact C prototype of `name`, and `lib` a live handle.
unsafe fn dlsym_as<T>(lib: *mut c_void, name: &str) -> Result<T> {
    let c_name = std::ffi::CString::new(name).context("symbol name contains a NUL")?;
    // SAFETY: live handle, valid NUL-terminated name.
    let sym = unsafe { libc::dlsym(lib, c_name.as_ptr()) };
    if sym.is_null() {
        bail!("symbol {name} not found: {}", dlerror());
    }
    // SAFETY: the caller guarantees `T` is `name`'s prototype. Sizes are checked because a
    // transmute to anything but a pointer-sized value here would be silently wrong.
    debug_assert_eq!(std::mem::size_of::<T>(), std::mem::size_of::<*mut c_void>());
    Ok(unsafe { std::mem::transmute_copy(&sym) })
}

/// The dynamic loader's last error, or a stand-in when it has none.
fn dlerror() -> String {
    // SAFETY: `dlerror` returns either NULL or a pointer to a static string owned by the
    // loader, valid until the next call on this thread.
    let msg = unsafe { libc::dlerror() };
    if msg.is_null() {
        return "dlopen/dlsym failed with no dlerror message".to_owned();
    }
    // SAFETY: non-NULL loader-owned NUL-terminated string.
    unsafe { CStr::from_ptr(msg) }
        .to_string_lossy()
        .into_owned()
}
