//! **The client half of ADR-0030**: register your own memory, name it in a token, answer
//! announces — so a daemon can WRITE an S3 object straight into it.
//!
//! Everything the daemon needs for token delivery has existed since
//! [ADR-0030](../../../docs/adr/0030-delivery-registration-belongs-to-the-memory-owner.md)
//! landed, and it is proven on hardware (`bench/ladder/results/c2-token-gate.md`: an object
//! written into a client's H100 window, byte-verified). What did not exist was a
//! *counterparty a real loader can be*. The only token producer was `spike/efa`'s
//! `token-client` role — a standalone binary that publishes a token, waits, and exits — and
//! `clients/python/pacer_delivery.py` says plainly that it "cannot construct such a token (it
//! has no ibverbs)". A checkpoint loader is Python. This crate is the missing piece:
//!
//! ```text
//!   loader (python, torch)                     this crate                    daemon
//!   ────────────────────────                   ──────────                    ──────
//!   pacer_nic.NicTarget(...)  ── ctypes ──►  ibv_reg_dmabuf_mr on the
//!                                            CLIENT's own PD; SRD rail up;
//!                                            receive ring posted
//!                             ◄── token ───  nic:0x0;offset=…;rails=gid/qpn/rkey
//!   GET /?x-pacer-endpoints  ───────────────────────────────────────────────►  ring lookup
//!                             ◄── endpoints ──────────────────────────────────  (no I/O)
//!                                            ibv_create_ah per holder GID  ── pre-flight,
//!                                                                             purely LOCAL
//!   GET + x-pacer-target  ──────────────────────────────────────────────────►
//!                                            announce ◄─────── SEND (a writer the
//!                                            ibv_create_ah              pre-flight missed)
//!                                            HBM ◄───────────── one-sided WRITE
//!   torch.as_tensor(ptr)  ◄── device ptr ──  (the tensor IS the delivered window)
//! ```
//!
//! ## Ordering is structural, not raced
//!
//! Every address handle this client will need is built **before** it issues the request that
//! authorises anyone to write — that is what the pre-flight step above buys, and it is the
//! whole reason it exists. An announce still runs, demoted from *the* mechanism to the
//! **repair path**: it covers a holder the prediction did not name (the source set can shift
//! between the two calls — eviction, rebalance, a node joining) and a client that skipped the
//! pre-flight entirely (an older shim). See [`handles`] for the cache those handles live in
//! and the bound it carries.
//!
//! ## What it is not
//!
//! Not a transport: it posts no send and no WRITE. Not a ring member: a delivery client is
//! deliberately not required to be one (ADR-0030 point 6), so nothing here links gRPC or
//! joins a cluster. Not the publication route either — [ADR-0031](../../../docs/adr/0031-nixl-publish-as-a-backend-plugin.md)
//! makes the shipping client half a NIXL backend plugin in C++; that plugin does what
//! [`ffi`] does, against the same [`token`] grammar and the same
//! [`pacer_transport::announce`] format, which is why both of those live outside the `efa`
//! feature gate.
//!
//! ## The `efa` feature
//!
//! Off by default. With it off, the [`token`] renderer and its round-trip tests against the
//! daemon's own parser still compile and run — that is the half with a wire format to keep —
//! and every C entry point exists and answers [`ffi::STATUS_UNSUPPORTED`], so a loader that
//! imports the shared object on a CPU node gets a sentence instead of a link error.
//!
//! ## Two invariants worth carrying out of here
//!
//! * **A token's base address comes from the registration, never from a pointer.** A GPU
//!   window is registered from a dma-buf with `iova == offset`, so its base is the dma-buf
//!   offset (`0`), while the *device pointer* is what a tensor in this process is built over.
//!   Confusing the two produces a token whose bytes land nowhere (planning/21).
//! * **The pump must be running for a delivery to work.** A writer installs itself with a
//!   SEND before its first WRITE, and a SEND to a client with no receive posted *hangs the
//!   sender*. That is why a client is a live object with a thread, not a function that
//!   returns a string.

#[cfg(feature = "efa")]
mod client;
#[cfg(feature = "efa")]
mod cuda;
#[cfg(feature = "efa")]
mod endpoint;
#[cfg(feature = "efa")]
mod pump;
#[cfg(feature = "efa")]
pub mod topology;
#[cfg(feature = "efa")]
mod window;

pub mod ffi;
// The bounded address-handle cache. Outside the `efa` gate for the same reason [`token`] is:
// the part with a decision in it — the key, the bound, the eviction order — is pure policy
// over an announce's rails, so it compiles and is TESTED in every build, and the `efa` build
// merely instantiates it at `ibverbs::AddressHandle`.
pub mod handles;
pub mod token;

#[cfg(feature = "efa")]
pub use client::{Client, HandleTotals, PrimedEndpoints, WindowSpec};
#[cfg(feature = "efa")]
pub use cuda::GPU_PAGE_BYTES;
#[cfg(feature = "efa")]
pub use pump::PumpState;
#[cfg(feature = "efa")]
pub use window::Pages;
