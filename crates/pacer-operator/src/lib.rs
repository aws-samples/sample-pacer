//! PACER's Kubernetes operator (ADR-0047).
//!
//! A [`CacheRing`](crd::CacheRing) declares one cache ring; the operator reconciles it into
//! the workloads the shipped `pacer` chart renders for it, owns them, prunes what stops
//! rendering, and reports the ring's rollout and membership in its status.
//!
//! - [`crd`] — the resource and its status.
//! - [`render`] — `helm template` as the renderer, so the chart stays the one definition.
//! - [`guard`] — what the operator will write on a ring's behalf, and the ownership stamp.
//! - [`reconcile`] — the controller.
//! - [`status`] — conditions, rollout counts and membership, as pure functions.
//! - [`config`] — the environment the binary reads.

pub mod config;
pub mod crd;
pub mod guard;
pub mod reconcile;
pub mod render;
pub mod status;
