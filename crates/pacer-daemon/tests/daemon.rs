//! The daemon's integration suite: one binary, one fixture, one file per subject.
//!
//! # Why one binary
//!
//! There used to be nine, each a separate `tests/*.rs`, and Cargo links a fresh copy of
//! `pacer-daemon` and its whole dependency closure into every one — nine link steps for
//! nine copies of the same code, which was most of the wall clock of a warm
//! `cargo nextest run`. The reason to keep them apart was parallelism: the stock test
//! runner takes one *binary* at a time, so merging them would have serialised the suite
//! behind its slowest file. Under nextest each **test** is a process, so that reason is
//! gone and only the cost remains.
//!
//! What is deliberately *not* lost is the file boundary. Each former binary is a module
//! of its own here, in a file of its own under `tests/daemon/`, with its own header
//! saying what it proves and which ADRs it discharges. `#[path]` rather than the default
//! lookup so those files can sit in a directory named after the binary while
//! `tests/common/` stays a sibling of it — and so `git log --follow` still reaches the
//! history of each one.
//!
//! Because they are now modules of one crate, every test's name gains its module as a
//! prefix: `correctness::roundtrip_small_object_uncached` where it used to be
//! `roundtrip_small_object_uncached` in a binary called `correctness`. Nextest's
//! `binary_id::test` becomes `daemon::module::test`, and no filter that named a file
//! stops working.
//!
//! # The subjects, and what each one is the only place for
//!
//! * [`correctness`] — Phase 1 read/write correctness and the ADR-0023 backend parity
//!   matrix, in process over `s3s-fs`.
//! * [`cluster`] — N nodes over one backend with a real gRPC peer plane: ADR-0012
//!   ownership, ADR-0016 admission, ADR-0017 directory.
//! * [`scatter`] — the ADR-0032 write scatter across a five-node fleet, split by gate
//!   group.
//! * [`delivery`] — ADR-0026 delivery into client memory and ADR-0030's pre-flight,
//!   called straight at the proxy because the protocol is in headers the SDK drops.
//! * [`listener`] — ADR-0036's socket layer over real TCP loopback: caps, deadlines,
//!   drain.
//! * [`budgets`] — every one of those bounds *reached*, with several clients at once.
//! * [`backend_retry`] — a severed backend body, and the truncation the retry exists to
//!   prevent.
//! * [`fill_coalesce`] — ADR-0040's single flight: two clients on one cold chunk, counted
//!   at the backend, with the knob off as the control.
//! * [`write_framing`] — what the passthrough PUT puts on the wire, as a function of
//!   what the client sent.
//! * [`backend_matrix_s3`] — the arm that needs a real bucket. `#[ignore]`d, and it
//!   panics rather than passes when run without one.

mod common;

#[path = "daemon/backend_matrix_s3.rs"]
mod backend_matrix_s3;
#[path = "daemon/backend_retry.rs"]
mod backend_retry;
#[path = "daemon/budgets.rs"]
mod budgets;
#[path = "daemon/cluster.rs"]
mod cluster;
#[path = "daemon/correctness.rs"]
mod correctness;
#[path = "daemon/delivery.rs"]
mod delivery;
#[path = "daemon/fill_coalesce.rs"]
mod fill_coalesce;
#[path = "daemon/listener.rs"]
mod listener;
#[path = "daemon/scatter/mod.rs"]
mod scatter;
#[path = "daemon/write_framing.rs"]
mod write_framing;
