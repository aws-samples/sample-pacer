//! pacer-daemon as a library: everything main.rs wires together, exposed so
//! integration tests can assemble the daemon in-process.

pub mod auth;
pub mod cachefill;
pub mod cgroup;
pub mod config;
pub mod coordinate;
pub mod delivery;
pub mod diskstats;
pub mod health;
pub mod listen;
pub mod memory_budget;
pub mod memstats;
pub mod metrics;
pub mod peer;
pub mod preflight;
pub mod proxy;
pub mod scatter;
pub mod shutdown;
pub mod staging;
