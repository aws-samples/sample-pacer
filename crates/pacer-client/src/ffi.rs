//! The C ABI, because the clients that need this are not written in Rust.
//!
//! A checkpoint loader is Python (`clients/python/pacer_nic.py` ctypes-loads this shared
//! object) and ADR-0031's NIXL backend is C++. Both need exactly what a Rust caller needs, so
//! this is a thin, allocation-free-at-the-boundary wrapper over the crate's `Client`: opaque
//! `u64` handles into a process-wide registry rather than pointers, out-parameters for every
//! value, and one status code convention.
//!
//! `Client` is named in backticks rather than linked on purpose: it exists only under
//! `--features efa` while this module is compiled unconditionally (see the third shape below),
//! so an intra-doc link to it fails the featureless `cargo doc` that CI runs with
//! `-D warnings` — which is exactly what it did.
//!
//! Three deliberate shapes:
//!
//! * **Handles, not pointers.** A `u64` key cannot be dangling-dereferenced from Python, and
//!   `close` on an unknown handle is a defined answer instead of a segfault.
//! * **Every function returns a status; values come back through out-parameters.** No
//!   sentinel-value ambiguity — `0` is a legitimate device pointer for a host window and a
//!   legitimate dma-buf base address for a GPU one.
//! * **The build without hardware still exports every symbol.** A loader that imports this on
//!   a CPU node gets [`STATUS_UNSUPPORTED`] and a legible message, not a missing-symbol
//!   error from `dlopen`.

use std::cell::RefCell;
use std::ffi::{c_char, CString};

/// Success.
pub const STATUS_OK: i32 = 0;
/// The call failed; [`pacer_client_last_error`] has the message.
pub const STATUS_ERROR: i32 = -1;
/// No client is registered under that handle (already closed, or never opened).
pub const STATUS_UNKNOWN_HANDLE: i32 = -2;
/// This build has no RDMA plane (compiled without the `efa` feature), so no window can be
/// registered and no token produced. A defined answer rather than a failure: a host-side test
/// suite links this same object.
pub const STATUS_UNSUPPORTED: i32 = -3;

/// What the caller passes as the GPU ordinal to ask for a **host** window.
///
/// Negative rather than a separate flag argument: it keeps `open` to one memory-kind
/// parameter, and an ordinal is never negative.
pub const HOST_WINDOW: i32 = -1;

thread_local! {
    /// The last error on THIS thread, kept alive so the pointer handed out stays valid until
    /// the next failing call on the same thread — which is the contract C callers expect from
    /// `strerror`-shaped APIs.
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
    /// The last token rendered on THIS thread, for the same reason.
    static LAST_TOKEN: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// Record `message` as this thread's last error.
fn set_error(message: String) {
    // A NUL inside an error message would be a message we cannot hand to C; say so rather
    // than dropping the error entirely.
    let text = CString::new(message)
        .unwrap_or_else(|_| CString::new("error message contained a NUL byte").expect("literal"));
    LAST_ERROR.with(|slot| *slot.borrow_mut() = Some(text));
}

/// The last error recorded on this thread, or `NULL` if there has not been one.
///
/// The pointer is owned by this library and valid until the next failing call on the same
/// thread. Callers copy it; they must not free it.
#[no_mangle]
pub extern "C" fn pacer_client_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| {
        slot.borrow()
            .as_ref()
            .map_or(std::ptr::null(), |text| text.as_ptr())
    })
}

/// Whether this build can register a window at all: `1` with the `efa` feature, `0` without.
///
/// Lets a caller distinguish "no hardware in this build" from "hardware present but the open
/// failed", which are different problems with different fixes.
#[no_mangle]
pub extern "C" fn pacer_client_available() -> i32 {
    i32::from(cfg!(feature = "efa"))
}

#[cfg(not(feature = "efa"))]
mod stub {
    //! Every entry point in a build with no RDMA plane: they exist, they explain themselves,
    //! and they refuse.

    use super::{set_error, STATUS_UNSUPPORTED};
    use std::ffi::{c_char, c_void};

    /// What every stub reports. Names the cause and the fix, because the most likely reader
    /// is someone who built the wheel on a laptop and is running it on a p5.
    const NO_PLANE: &str = "this pacer-client was built without the `efa` feature, so it can \
                            register no memory and produce no token — rebuild it with \
                            `--features efa` on a host that has libibverbs/libefa";

    /// See the `efa` build's documentation for each of these; the signatures are identical.
    macro_rules! unsupported {
        ($($name:ident($($arg:ty),*)),* $(,)?) => {
            $(
                /// Refuses with [`STATUS_UNSUPPORTED`] (see [`NO_PLANE`]).
                ///
                /// # Safety
                ///
                /// Trivially safe in this build: no argument is dereferenced.
                #[no_mangle]
                pub unsafe extern "C" fn $name($(_: $arg),*) -> i32 {
                    set_error(NO_PLANE.to_owned());
                    STATUS_UNSUPPORTED
                }
            )*
        };
    }

    unsupported!(
        pacer_client_affine_rails(i32, u32, *mut u32, u32, *mut u32, *mut i32, *mut u32),
        pacer_client_open(u64, i32, u32, u32, u32, u8, *mut u64),
        pacer_client_len(u64, *mut u64),
        pacer_client_pages(u64, *mut u64),
        pacer_client_device_ptr(u64, *mut u64),
        pacer_client_host_ptr(u64, *mut u64),
        pacer_client_stats(u64, *mut u64, *mut u64, *mut i32),
        pacer_client_prime(u64, *const c_void, u64, *mut u64, *mut u64),
        pacer_client_handle_stats(u64, *mut u64, *mut u64, *mut u64, *mut u64),
        pacer_client_copy_out(u64, u64, *mut c_void, u64),
        pacer_client_copy_in(u64, u64, *const c_void, u64),
        pacer_client_fill(u64, u8),
        pacer_client_close(u64),
    );

    /// Always `NULL` in this build; the reason is in `pacer_client_last_error`.
    #[no_mangle]
    pub extern "C" fn pacer_client_token(_: u64, _: u64, _: u64, _: i32) -> *const c_char {
        set_error(NO_PLANE.to_owned());
        std::ptr::null()
    }
}

#[cfg(feature = "efa")]
mod real {
    //! The entry points that do something.

    use std::collections::HashMap;
    use std::ffi::{c_char, c_void, CString};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, OnceLock};

    use super::{
        set_error, HOST_WINDOW, LAST_TOKEN, STATUS_ERROR, STATUS_OK, STATUS_UNKNOWN_HANDLE,
    };
    use crate::window::Pages;
    use crate::{Client, WindowSpec};

    /// Live clients, keyed by the handle their opener was given.
    ///
    /// A `Mutex` over a map rather than a pointer per client: the lock is taken for the
    /// duration of one accessor (never across a delivery — the daemon writes into the window
    /// without going through this library at all), and it makes double-close and
    /// use-after-close defined instead of undefined.
    fn registry() -> &'static Mutex<HashMap<u64, Client>> {
        static REGISTRY: OnceLock<Mutex<HashMap<u64, Client>>> = OnceLock::new();
        REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// Source of handles. Starts at 1 so `0` is never valid, which makes an uninitialized
    /// variable on the caller's side fail cleanly.
    static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);

    /// Filter applied when `RUST_LOG` is unset: nothing. A library loaded into someone
    /// else's process does not get to write to their stderr uninvited, so logging is
    /// opt-in — but when it is asked for, `info` is enough (`endpoint::bring_up`'s "client
    /// rail up" line, which names the device a rail opened).
    const QUIET_FILTER: &str = "off";

    /// Install a stderr subscriber once, honoring `RUST_LOG`.
    ///
    /// Called from [`pacer_client_open`] rather than from a constructor: it is the first
    /// call that can log anything, and a `dlopen`-time side effect on someone else's
    /// process is worse than a slightly late subscriber. A second call is a no-op, and an
    /// embedder that installed its own subscriber keeps it (`try_init` does not panic).
    fn install_logging() {
        static ONCE: OnceLock<()> = OnceLock::new();
        ONCE.get_or_init(|| {
            let filter = tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(QUIET_FILTER));
            // Ignored deliberately: failure means this process already has a subscriber,
            // which is the embedder's choice and not an error here.
            let _ = tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_writer(std::io::stderr)
                .try_init();
        });
    }

    /// Run `f` against the client behind `handle`.
    ///
    /// Poisoning is reported as an ordinary error: a panic in one accessor must not make
    /// every later call panic too, and the window itself is untouched by a poisoned lock.
    fn with_client<T>(
        handle: u64,
        f: impl FnOnce(&mut Client) -> anyhow::Result<T>,
    ) -> Result<T, i32> {
        let mut guard = registry().lock().map_err(|_| {
            set_error("the pacer-client registry lock is poisoned".to_owned());
            STATUS_ERROR
        })?;
        let client = guard.get_mut(&handle).ok_or_else(|| {
            set_error(format!("no client registered under handle {handle}"));
            STATUS_UNKNOWN_HANDLE
        })?;
        f(client).map_err(|e| {
            set_error(format!("{e:#}"));
            STATUS_ERROR
        })
    }

    /// Open a window and start answering announces.
    ///
    /// `gpu_ordinal` is [`HOST_WINDOW`] for host memory or a CUDA ordinal for HBM; `rail` is
    /// the first EFA rail and `rail_count` how many CONSECUTIVE rails to register on (each is
    /// a destination the daemon may WRITE to — see [`crate::WindowSpec::rail_count`]);
    /// `pages_mib` is `2` or `1024` for explicit hugepages and anything else (including `0`)
    /// for base pages, and is ignored for a device window. `sentinel` is the byte the window
    /// is filled with before it is published.
    ///
    /// Discover which rails are PCIe-local to CUDA device `gpu`, so a caller never has to
    /// hard-code an instance type's map (see [`crate::topology`]).
    ///
    /// Writes up to `out_cap` rail indices into `out_rails`, nearest first, and the number
    /// written into `out_count`. Those indices are exactly what
    /// [`pacer_client_open`]'s `rail` takes: both count EFA devices only.
    ///
    /// `out_distinct` receives `1` when the host expresses a real rail/GPU distinction and
    /// **`0` when it does not** — in which case the rails are still returned, in index order,
    /// and the caller should record that it ran unpinned rather than report an affinity it
    /// did not get. That is the difference between an unexplained 1.44x and a known one.
    ///
    /// # Safety
    ///
    /// `out_rails` must be a valid, writable `uint32_t*` with room for `out_cap` entries;
    /// `out_count` a valid, writable `uint32_t*`; `out_distinct` a valid, writable `int32_t*`
    /// or `NULL` to ignore it.
    #[no_mangle]
    pub unsafe extern "C" fn pacer_client_affine_rails(
        gpu: i32,
        want: u32,
        out_rails: *mut u32,
        out_cap: u32,
        out_count: *mut u32,
        out_distinct: *mut i32,
        out_affine_count: *mut u32,
    ) -> i32 {
        install_logging();
        if out_rails.is_null() || out_count.is_null() {
            set_error(
                "pacer_client_affine_rails needs non-NULL out_rails and out_count".to_owned(),
            );
            return STATUS_ERROR;
        }
        let want = usize::try_from(want).unwrap_or(usize::MAX).max(1);
        let affinity = match crate::topology::affine_rails(gpu, want) {
            Ok(affinity) => affinity,
            Err(error) => {
                set_error(format!("{error:#}"));
                return STATUS_ERROR;
            }
        };
        let cap = usize::try_from(out_cap).unwrap_or(0);
        if affinity.rails.len() > cap {
            set_error(format!(
                "found {} rail(s) for GPU {gpu} but out_cap is {cap}",
                affinity.rails.len(),
            ));
            return STATUS_ERROR;
        }
        for (slot, index) in affinity.rails.iter().enumerate() {
            // SAFETY: `slot` is below `affinity.rails.len()`, which the check above proved is
            // within `out_cap` — the caller's documented allocation.
            unsafe {
                out_rails
                    .add(slot)
                    .write(u32::try_from(*index).unwrap_or(u32::MAX))
            };
        }
        // SAFETY: the caller's documented obligation.
        unsafe { out_count.write(u32::try_from(affinity.rails.len()).unwrap_or(u32::MAX)) };
        if !out_distinct.is_null() {
            // SAFETY: checked non-NULL; the caller's documented obligation otherwise.
            unsafe { out_distinct.write(i32::from(affinity.distinct)) };
        }
        // The size of the affine SET, independent of `want`. Optional like `out_distinct`, so
        // a caller that does not care passes NULL — but a caller REGISTERING a window should
        // ask for exactly this many rails rather than choosing a number itself.
        if !out_affine_count.is_null() {
            // SAFETY: checked non-NULL; the caller's documented obligation otherwise.
            unsafe {
                out_affine_count.write(u32::try_from(affinity.affine_count).unwrap_or(u32::MAX))
            };
        }
        STATUS_OK
    }

    /// # Safety
    ///
    /// `out_handle` must be a valid, writable `uint64_t*`.
    #[no_mangle]
    pub unsafe extern "C" fn pacer_client_open(
        bytes: u64,
        gpu_ordinal: i32,
        rail: u32,
        rail_count: u32,
        pages_mib: u32,
        sentinel: u8,
        out_handle: *mut u64,
    ) -> i32 {
        install_logging();
        if out_handle.is_null() {
            set_error("pacer_client_open needs a non-NULL out_handle".to_owned());
            return STATUS_ERROR;
        }
        let spec = WindowSpec {
            bytes: match usize::try_from(bytes) {
                Ok(bytes) => bytes,
                Err(_) => {
                    set_error(format!("a {bytes}-byte window exceeds this platform"));
                    return STATUS_ERROR;
                }
            },
            gpu: (gpu_ordinal != HOST_WINDOW).then_some(gpu_ordinal),
            rail: rail as usize,
            rail_count: rail_count as usize,
            pages: Pages::from_mib(pages_mib as usize),
            sentinel,
        };
        let client = match Client::open(&spec) {
            Ok(client) => client,
            Err(e) => {
                set_error(format!("{e:#}"));
                return STATUS_ERROR;
            }
        };
        let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
        match registry().lock() {
            Ok(mut guard) => {
                guard.insert(handle, client);
            }
            Err(_) => {
                set_error("the pacer-client registry lock is poisoned".to_owned());
                return STATUS_ERROR;
            }
        }
        // SAFETY: the caller's documented obligation, checked non-NULL above.
        unsafe { *out_handle = handle };
        STATUS_OK
    }

    /// The `x-pacer-target` value naming `[offset, offset + len)` of this client's window, or
    /// `NULL` on failure (the reason is in `pacer_client_last_error`).
    ///
    /// `checksum` non-zero asks the daemon to digest what it sent — leave it at zero for a
    /// rate arm, where the client's half of the check is a full device→host copy.
    ///
    /// The returned pointer is owned by this library and valid until the next call to this
    /// function **on the same thread**.
    #[no_mangle]
    pub extern "C" fn pacer_client_token(
        handle: u64,
        offset: u64,
        len: u64,
        checksum: i32,
    ) -> *const c_char {
        let rendered = with_client(handle, |client| client.token(offset, len, checksum != 0));
        let Ok(token) = rendered else {
            return std::ptr::null();
        };
        let Ok(text) = CString::new(token) else {
            set_error("the rendered token contained a NUL byte".to_owned());
            return std::ptr::null();
        };
        LAST_TOKEN.with(|slot| {
            let mut slot = slot.borrow_mut();
            *slot = Some(text);
            slot.as_ref().map_or(std::ptr::null(), |t| t.as_ptr())
        })
    }

    /// The window's registered length in bytes — which may exceed what was asked for, since
    /// both backings round up to a page.
    ///
    /// # Safety
    ///
    /// `out` must be a valid, writable `uint64_t*`.
    #[no_mangle]
    pub unsafe extern "C" fn pacer_client_len(handle: u64, out: *mut u64) -> i32 {
        // SAFETY: the caller's documented obligation.
        unsafe { write_u64(handle, out, |client| Ok(client.len() as u64)) }
    }

    /// The realized backing page size in bytes — never the size that was requested.
    ///
    /// # Safety
    ///
    /// `out` must be a valid, writable `uint64_t*`.
    #[no_mangle]
    pub unsafe extern "C" fn pacer_client_pages(handle: u64, out: *mut u64) -> i32 {
        // SAFETY: the caller's documented obligation.
        unsafe { write_u64(handle, out, |client| Ok(client.pages() as u64)) }
    }

    /// The GPU virtual address of a device window — what a `torch` tensor is built over.
    /// Fails for a host window rather than reporting zero.
    ///
    /// # Safety
    ///
    /// `out` must be a valid, writable `uint64_t*`.
    #[no_mangle]
    pub unsafe extern "C" fn pacer_client_device_ptr(handle: u64, out: *mut u64) -> i32 {
        // SAFETY: the caller's documented obligation.
        unsafe {
            write_u64(handle, out, |client| {
                client.device_ptr().ok_or_else(|| {
                    anyhow::anyhow!("this is a host window: it has no device pointer")
                })
            })
        }
    }

    /// The host address of a host window. Fails for a device window.
    ///
    /// # Safety
    ///
    /// `out` must be a valid, writable `uint64_t*`.
    #[no_mangle]
    pub unsafe extern "C" fn pacer_client_host_ptr(handle: u64, out: *mut u64) -> i32 {
        // SAFETY: the caller's documented obligation.
        unsafe {
            write_u64(handle, out, |client| {
                client.host_ptr().map(|ptr| ptr as u64).ok_or_else(|| {
                    anyhow::anyhow!("this is a device window: it has no host pointer")
                })
            })
        }
    }

    /// Pump counters: announces decoded, address handles built, and whether the pump is still
    /// reaping (`1`/`0`).
    ///
    /// The numbers a delivery arm should gate on. `announces == 0` after a GET that reported
    /// `x-pacer-delivered` would mean the daemon wrote without announcing, which the loopback
    /// gate says cannot happen; `healthy == 0` means later deliveries will fail.
    ///
    /// # Safety
    ///
    /// Every non-NULL out-pointer must be valid and writable; NULL ones are skipped.
    #[no_mangle]
    pub unsafe extern "C" fn pacer_client_stats(
        handle: u64,
        announces: *mut u64,
        handles: *mut u64,
        healthy: *mut i32,
    ) -> i32 {
        let read = with_client(handle, |client| {
            let pump = client.pump();
            Ok((pump.announces, pump.handles, i32::from(pump.healthy)))
        });
        match read {
            Ok((a, h, ok)) => {
                // SAFETY: the caller's documented obligation; NULL is skipped.
                unsafe {
                    if !announces.is_null() {
                        *announces = a;
                    }
                    if !handles.is_null() {
                        *handles = h;
                    }
                    if !healthy.is_null() {
                        *healthy = ok;
                    }
                }
                STATUS_OK
            }
            Err(status) => status,
        }
    }

    /// Build address handles for the writers one pre-flight answer names — **before** the GET
    /// that authorises anyone to write (ADR-0030's pre-flight exchange).
    ///
    /// `announce` points at `len` bytes of ONE `pacer_transport::announce` message: exactly
    /// what that writer would SEND on first contact, which is also what the daemon's
    /// pre-flight route hands back per holder node. A caller with several holders calls this
    /// once per holder.
    ///
    /// `out_created` receives the handles actually built and `out_cached` the rails whose GID
    /// was already held; either may be `NULL`. Both are summed over this window's rails,
    /// because an address handle is bound to a protection domain and a window on N rails needs
    /// the writer installed N times.
    ///
    /// Fails only on bytes that are not an announce — which is the caller's bug, not a
    /// writer's. **A caller must treat any failure as "skip priming and proceed"**: the
    /// announce path still installs writers, so the delivery is slower, never wrong.
    ///
    /// # Safety
    ///
    /// `announce` must be valid and readable for `len` bytes; `out_created`/`out_cached` must
    /// be valid, writable `uint64_t*` or NULL.
    #[no_mangle]
    pub unsafe extern "C" fn pacer_client_prime(
        handle: u64,
        announce: *const c_void,
        len: u64,
        out_created: *mut u64,
        out_cached: *mut u64,
    ) -> i32 {
        let Some((_, len)) = checked_range(0, len, announce.is_null()) else {
            return STATUS_ERROR;
        };
        // SAFETY: the caller's documented obligation, non-NULL checked above.
        let bytes = unsafe { std::slice::from_raw_parts(announce.cast::<u8>(), len) };
        match with_client(handle, |client| client.prime(bytes)) {
            Ok(primed) => {
                // SAFETY: the caller's documented obligation; NULL is skipped.
                unsafe {
                    if !out_created.is_null() {
                        *out_created = primed.created as u64;
                    }
                    if !out_cached.is_null() {
                        *out_cached = primed.already_held as u64;
                    }
                }
                STATUS_OK
            }
            Err(status) => status,
        }
    }

    /// Address-handle counters: held now, ever built, pre-armed by the pre-flight, and evicted
    /// at the bound.
    ///
    /// `prearmed` against `created` is what says the pre-flight is working: an announce whose
    /// rails were already held found the handles waiting, which is the race being structurally
    /// absent rather than merely not observed. `evicted` is expected to be **zero** — a
    /// non-zero value says the handle set outgrew its bound, which can destroy a handle a
    /// WRITE in flight still needs (raise `PACER_CLIENT_MAX_ADDRESS_HANDLES`).
    ///
    /// # Safety
    ///
    /// Every non-NULL out-pointer must be valid and writable; NULL ones are skipped.
    #[no_mangle]
    pub unsafe extern "C" fn pacer_client_handle_stats(
        handle: u64,
        held: *mut u64,
        created: *mut u64,
        prearmed: *mut u64,
        evicted: *mut u64,
    ) -> i32 {
        let read = with_client(handle, |client| Ok(client.handle_totals()));
        match read {
            Ok(totals) => {
                // SAFETY: the caller's documented obligation; NULL is skipped.
                unsafe {
                    if !held.is_null() {
                        *held = totals.held;
                    }
                    if !created.is_null() {
                        *created = totals.created;
                    }
                    if !prearmed.is_null() {
                        *prearmed = totals.prearmed;
                    }
                    if !evicted.is_null() {
                        *evicted = totals.evicted;
                    }
                }
                STATUS_OK
            }
            Err(status) => status,
        }
    }

    /// Copy `len` bytes of the window from `offset` into `dst` — verification, and the only
    /// way to read a device window.
    ///
    /// # Safety
    ///
    /// `dst` must be valid and writable for `len` bytes.
    #[no_mangle]
    pub unsafe extern "C" fn pacer_client_copy_out(
        handle: u64,
        offset: u64,
        dst: *mut c_void,
        len: u64,
    ) -> i32 {
        let Some((offset, len)) = checked_range(offset, len, dst.is_null()) else {
            return STATUS_ERROR;
        };
        // SAFETY: the caller's documented obligation, non-NULL checked above.
        let bytes = unsafe { std::slice::from_raw_parts_mut(dst.cast::<u8>(), len) };
        match with_client(handle, |client| client.copy_out(offset, bytes)) {
            Ok(()) => STATUS_OK,
            Err(status) => status,
        }
    }

    /// Copy `len` bytes from `src` into the window at `offset` — the leg a control arm pays.
    ///
    /// # Safety
    ///
    /// `src` must be valid and readable for `len` bytes.
    #[no_mangle]
    pub unsafe extern "C" fn pacer_client_copy_in(
        handle: u64,
        offset: u64,
        src: *const c_void,
        len: u64,
    ) -> i32 {
        let Some((offset, len)) = checked_range(offset, len, src.is_null()) else {
            return STATUS_ERROR;
        };
        // SAFETY: the caller's documented obligation, non-NULL checked above.
        let bytes = unsafe { std::slice::from_raw_parts(src.cast::<u8>(), len) };
        match with_client(handle, |client| client.copy_in(offset, bytes)) {
            Ok(()) => STATUS_OK,
            Err(status) => status,
        }
    }

    /// Refill the whole window with `byte`, so a second arm starts from a known state without
    /// paying another registration.
    ///
    /// # Safety
    ///
    /// Trivially safe: no pointer is dereferenced. `unsafe` only for symmetry with the rest of
    /// the ABI.
    #[no_mangle]
    pub unsafe extern "C" fn pacer_client_fill(handle: u64, byte: u8) -> i32 {
        match with_client(handle, |client| client.fill(byte)) {
            Ok(()) => STATUS_OK,
            Err(status) => status,
        }
    }

    /// Stop the pump, deregister the window and release its memory. Idempotent: closing an
    /// unknown handle reports [`STATUS_UNKNOWN_HANDLE`] rather than crashing.
    ///
    /// **Do not call this while a GET carrying one of its tokens is in flight** — the daemon
    /// would be writing into pages this process has released.
    ///
    /// # Safety
    ///
    /// Trivially safe: no pointer is dereferenced.
    #[no_mangle]
    pub unsafe extern "C" fn pacer_client_close(handle: u64) -> i32 {
        let removed = match registry().lock() {
            Ok(mut guard) => guard.remove(&handle),
            Err(_) => {
                set_error("the pacer-client registry lock is poisoned".to_owned());
                return STATUS_ERROR;
            }
        };
        match removed {
            // Dropped outside the lock: the pump thread is joined in `Client::drop`, and
            // holding the registry while joining would block every other accessor.
            Some(client) => {
                drop(client);
                STATUS_OK
            }
            None => {
                set_error(format!("no client registered under handle {handle}"));
                STATUS_UNKNOWN_HANDLE
            }
        }
    }

    /// Shared body of the `u64` accessors.
    ///
    /// # Safety
    ///
    /// `out` must be a valid, writable `uint64_t*`.
    unsafe fn write_u64(
        handle: u64,
        out: *mut u64,
        f: impl FnOnce(&mut Client) -> anyhow::Result<u64>,
    ) -> i32 {
        if out.is_null() {
            set_error("this accessor needs a non-NULL out-pointer".to_owned());
            return STATUS_ERROR;
        }
        match with_client(handle, f) {
            Ok(value) => {
                // SAFETY: the caller's documented obligation, checked non-NULL above.
                unsafe { *out = value };
                STATUS_OK
            }
            Err(status) => status,
        }
    }

    /// Validate a `(offset, len)` pair from C, recording the reason if it is unusable.
    ///
    /// Returns `None` for a NULL buffer or a length this platform cannot address — both of
    /// which would otherwise become a slice over nothing.
    fn checked_range(offset: u64, len: u64, is_null: bool) -> Option<(usize, usize)> {
        if is_null {
            set_error("this call needs a non-NULL buffer".to_owned());
            return None;
        }
        match (usize::try_from(offset), usize::try_from(len)) {
            (Ok(offset), Ok(len)) => Some((offset, len)),
            _ => {
                set_error(format!("range {offset}+{len} exceeds this platform"));
                None
            }
        }
    }
}
