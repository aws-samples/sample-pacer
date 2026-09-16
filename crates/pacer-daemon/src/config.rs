//! Runtime configuration (ADR-0013). Three layers, lowest to highest:
//! built-in defaults < YAML config file < `PACER_*` environment variables.
//!
//! The file (path from `CONFIG_PATH`, default `/etc/pacer/config.yaml`) is a
//! ConfigMap in production; env vars carry per-pod values the downward API
//! injects (`PACER_NODE_NAME`, `PACER_NAMESPACE`) and let any deployment or test
//! override a single knob without editing the file. Env always wins.
//!
//! Every `PACER_*` name is an external contract (chart values, runbooks) and is
//! defined exactly once here as an `EnvVar`. Precedence lives in exactly one
//! place — `resolve` — which takes the parsed file and an env lookup so it is
//! unit-testable without touching process-global state.

use std::collections::HashMap;

use pacer_backend::{BackendConfig, BackendType};
use pacer_cache::{CacheConfig, UringConfig};
use serde::Deserialize;

/// Env var naming the config file. Absent → try [`DEFAULT_CONFIG_PATH`].
const CONFIG_PATH: EnvVar = EnvVar {
    name: "PACER_CONFIG",
    default: "",
};
/// Where the chart mounts the ConfigMap; used when `PACER_CONFIG` is unset. A
/// missing file here is not an error (env-only / dev runs are valid).
const DEFAULT_CONFIG_PATH: &str = "/etc/pacer/config.yaml";

/// Env lookup: returns the raw value of a variable, or `None` if unset. The
/// indirection lets [`resolve`] be tested with a fake environment.
type EnvFn<'a> = &'a dyn Fn(&str) -> Option<String>;

/// An environment variable name paired with its built-in default. The default
/// is the lowest layer; a config-file value sits above it; the env value (when
/// set) is highest.
struct EnvVar {
    name: &'static str,
    default: &'static str,
}

impl EnvVar {
    /// Resolve to a concrete string: env wins, then the file value, then the
    /// built-in default. A set-but-empty env var wins as `""` (matches the
    /// prior env-only behavior).
    fn resolve(&self, env: EnvFn, file: Option<String>) -> String {
        env(self.name)
            .or(file)
            .unwrap_or_else(|| self.default.to_string())
    }

    /// Resolve to an optional string, treating empty as absent at every layer
    /// (the default for these is `""`, i.e. `None`).
    fn resolve_opt(&self, env: EnvFn, file: Option<String>) -> Option<String> {
        env(self.name)
            .filter(|s| !s.is_empty())
            .or(file)
            .filter(|s| !s.is_empty())
    }
}

/// Address the S3 API listens on.
const LISTEN_ADDR: EnvVar = EnvVar {
    name: "PACER_LISTEN_ADDR",
    default: "0.0.0.0:9000",
};
/// Concurrent S3 connections the listener holds open (ADR-0036). `0` → the
/// built-in [`crate::listen::ListenLimits::default_max_connections`]. Reaching it
/// backpressures at the accept queue rather than failing a request, so the
/// symptom of a value that is too low is a slow connect — watch
/// `pacer_s3_connections_at_capacity_total`.
const S3_MAX_CONNECTIONS: EnvVar = EnvVar {
    name: "PACER_S3_MAX_CONNECTIONS",
    default: "0",
};
/// Whole seconds a connection may go without a complete request head
/// (ADR-0036). hyper re-arms this timer between keep-alive requests, so it is
/// also how long an idle connection is kept. `0` → the built-in
/// [`crate::listen::ListenLimits::default_header_timeout_secs`].
const S3_HEADER_TIMEOUT: EnvVar = EnvVar {
    name: "PACER_S3_HEADER_TIMEOUT",
    default: "0",
};
/// Whole seconds a connection may go with no byte moving in either direction
/// (ADR-0036) — a stalled body or an unread response, which the header deadline
/// cannot see. `0` → the built-in
/// [`crate::listen::ListenLimits::default_idle_timeout_secs`].
const S3_IDLE_TIMEOUT: EnvVar = EnvVar {
    name: "PACER_S3_IDLE_TIMEOUT",
    default: "0",
};
/// Whole seconds in-flight work gets to finish after SIGTERM (ADR-0036). `0` →
/// [`crate::shutdown::DEFAULT_DRAIN_TIMEOUT_SECS`]. Must stay below the pod's
/// `terminationGracePeriodSeconds` or the kubelet SIGKILLs the process mid-drain.
const SHUTDOWN_DRAIN_TIMEOUT: EnvVar = EnvVar {
    name: "PACER_SHUTDOWN_DRAIN_TIMEOUT",
    default: "0",
};
/// Log encoding: `json` (the default) or `text`. JSON because the daemon's logs
/// are read out of `kubectl logs`/CloudWatch by tooling far more often than by a
/// person, and a text line's `key=value` tail is not parseable without knowing
/// every field; `text` stays one flag away for a human tailing a dev pod.
const LOG_FORMAT: EnvVar = EnvVar {
    name: "PACER_LOG_FORMAT",
    default: LogFormat::DEFAULT_NAME,
};
/// Address for health/readiness/metrics (kubelet probes hit this).
const ADMIN_ADDR: EnvVar = EnvVar {
    name: "PACER_ADMIN_ADDR",
    default: "0.0.0.0:9090",
};
/// Disk-tier directory (local NVMe instance-store RAID0 in production).
const CACHE_DIR: EnvVar = EnvVar {
    name: "PACER_CACHE_DIR",
    default: "/var/cache/pacer",
};
/// In-memory tier capacity.
const MEM_CAPACITY: EnvVar = EnvVar {
    name: "PACER_MEM_CAPACITY",
    default: "1GiB",
};
/// Disk tier capacity.
const DISK_CAPACITY: EnvVar = EnvVar {
    name: "PACER_DISK_CAPACITY",
    default: "100GiB",
};
/// foyer block size: eviction unit, max on-disk entry size, and fd-count
/// divisor (see `CacheConfig::block_size` for the constraints).
const BLOCK_SIZE: EnvVar = EnvVar {
    name: "PACER_BLOCK_SIZE",
    default: "1GiB",
};
/// foyer flush (DRAM→NVMe) buffer pool size. `0` = auto-size to
/// `2 × block_size` — entries larger than this buffer are silently dropped on
/// demotion and never reach the disk tier (see
/// `CacheConfig::flush_buffer_size`), so the auto default tracks the largest
/// cacheable entry rather than foyer's fixed 16 MiB.
const FLUSH_BUFFER_SIZE: EnvVar = EnvVar {
    name: "PACER_FLUSH_BUFFER_SIZE",
    default: "0",
};
/// Disk I/O engine: `psync` (portable) or `uring` (Linux only).
const IO_ENGINE: EnvVar = EnvVar {
    name: "PACER_IO_ENGINE",
    default: "psync",
};
/// io_uring worker threads (only read when the engine is `uring`). `0` = keep
/// the built-in default.
const URING_THREADS: EnvVar = EnvVar {
    name: "PACER_URING_THREADS",
    default: "0",
};
/// io_uring submission-queue depth per thread. `0` = keep the built-in default.
const URING_IO_DEPTH: EnvVar = EnvVar {
    name: "PACER_URING_IO_DEPTH",
    default: "0",
};
/// Blocks the disk tier flushes into concurrently, which is also what spreads a
/// sequential read back across io_uring shards (see
/// `pacer_cache::StorageTuning::flushers`). `0` = foyer's default of 1.
const CACHE_FLUSHERS: EnvVar = EnvVar {
    name: "PACER_CACHE_FLUSHERS",
    default: "0",
};
/// Concurrent block reclaimers. `0` = foyer's default of 1.
const CACHE_RECLAIMERS: EnvVar = EnvVar {
    name: "PACER_CACHE_RECLAIMERS",
    default: "0",
};
/// In-flight DRAM→NVMe write budget; enqueues past it are silently dropped. `0` =
/// foyer's default of 16 MiB, which is one 16-MiB chunk entry.
const SUBMIT_QUEUE_THRESHOLD: EnvVar = EnvVar {
    name: "PACER_SUBMIT_QUEUE_THRESHOLD",
    default: "0",
};
/// Worker threads for a runtime dedicated to the disk tier. `0` = share the main
/// runtime (foyer's default), which puts every entry's checksum and decode on it.
const STORAGE_RUNTIME_THREADS: EnvVar = EnvVar {
    name: "PACER_STORAGE_RUNTIME_THREADS",
    default: "0",
};
/// Whether a chunk read promotes a disk hit into the RAM tier
/// (`on-disk-hit`, foyer's behaviour, or `never`).
const CHUNK_PROMOTION: EnvVar = EnvVar {
    name: "PACER_CHUNK_PROMOTION",
    default: "on-disk-hit",
};
/// Which implementation holds chunk bodies on disk: `foyer` (an entry per chunk,
/// paying a checksum and a decode copy per hit) or `store` (ADR-0033: one `pread`
/// into a registered frame). Defaults to `foyer` so ADR-0033 is opt-in and a
/// regression is one flag back.
const DISK_TIER: EnvVar = EnvVar {
    name: "PACER_DISK_TIER",
    default: "foyer",
};
/// Verify a chunk body's CRC32 on every read from the ADR-0033 store. Off by
/// default: a full pass over a 16 MiB chunk is ~1.1 ms against the 1.335 ms the
/// device takes to read it, so it is a ~45 % throughput tax to re-check what NVMe
/// end-to-end protection covers and what the client's delivery digest checks again.
/// The CRC is *written* regardless, so this is a flag flip, not a migration. Read
/// only when `PACER_DISK_TIER=store`; the slot's KEY is checked either way, because
/// that catches the failure this daemon's own bookkeeping can cause.
const VERIFY_CHUNK_BODY: EnvVar = EnvVar {
    name: "PACER_VERIFY_CHUNK_BODY",
    default: "false",
};
/// Whether a GET carrying `If-Match` may be served from the cache when the ETag the
/// client named equals the one this node resolved (ADR-0039). `false` restores the
/// unconditional passthrough every conditional GET took before.
///
/// **Default `true` because otherwise the cache is unreachable for a real client.**
/// Mountpoint-for-S3 puts `If-Match` on *every* GET it issues, so measured against it
/// PACER's hit rate was exactly **0 %** — 100 % of its reads bypassed
/// (`bench/ladder/results/mountpoint-vs-pacer.md`). That is our gate, not their bug.
///
/// It is a knob rather than a constant because it is the one place this proxy answers a
/// conditional differently from S3: on a hit the ETag compared against is the one in
/// **our cache**, so a client cannot use `If-Match` through PACER to *detect* that the
/// object was replaced in place. See ADR-0039 for why that is bounded (ADR-0015 requires
/// a new name per version, under which the condition can never legitimately fail) and
/// why a mismatch passes through rather than answering 412.
const CONDITIONAL_GET_FROM_CACHE: EnvVar = EnvVar {
    name: "PACER_CONDITIONAL_GET_FROM_CACHE",
    default: "true",
};
/// Whether a store hit issues its 4 KiB header read and its 16 MiB body read one
/// after the other (`two-read`, the default and every measurement through
/// 2026-09-10) or at the same time (`overlap`).
///
/// The one live hypothesis for the ~2.1× between this tier and the device that fio
/// has not excluded. At the store's exact shape — 48 concurrent 16 MiB `O_DIRECT`
/// reads — fio reaches 43.208 GiB/s at 17.3 ms per read where the store manages
/// 19.926 at 43.8 ms, and inode sharing accounts for 4.1 % of it. The arm that
/// looked like a refutation merged the two reads into one of 16 MiB + 4 KiB, which
/// is stripe-misaligned, so it measured two opposite effects as one number
/// (`results/c5-dcp-store-single-read.md`). `overlap` removes the dependency and
/// touches no offset or length.
const STORE_READ_SHAPE: EnvVar = EnvVar {
    name: "PACER_STORE_READ_SHAPE",
    default: "two-read",
};
/// Chunk reads in flight against the ADR-0033 store, node-wide. `0` = the built-in
/// [`pacer_cache::store::DEFAULT_READ_CONCURRENCY`]; a deliberate `unlimited` is the
/// pre-ceiling behaviour and is spelled as such rather than as a number.
///
/// **The only place the total is bounded.** `PACER_FILL_PARALLELISM` bounds one client
/// GET and `delivery.parallelism` bounds one delivery request; N concurrent requests
/// multiply, and a measured arm reached ~59 reads in flight. The array saturates at
/// ~4 — one 16 MiB `pread` is already ~128 device requests — so past the knee the extra
/// width is pure queueing, and it is where the 40-43.8 ms per-hit service times in
/// `results/c5-dcp-store-odirect.md` came from.
const STORE_READ_CONCURRENCY: EnvVar = EnvVar {
    name: "PACER_STORE_READ_CONCURRENCY",
    default: "0",
};
/// tokio worker threads. `0` = tokio default (one per core).
const WORKER_THREADS: EnvVar = EnvVar {
    name: "PACER_WORKER_THREADS",
    default: "0",
};
/// tokio worker threads for the dedicated RDMA serve-path runtime — the EFA
/// completion pump's drain loop and holder-side `serve_via_write` execution run
/// there, isolated from the main runtime that serves the S3 proxy, so
/// client-role GET handling cannot starve completion-pump wakeups or WRITE
/// posts (planning/15 all-hammer contention). `0` = tokio default (one per
/// core); a small explicit count is the intended production setting, since the
/// serve path is latency- not throughput-bound and should not over-subscribe
/// cores against the main runtime. Only consulted when the `efa` feature is
/// built in and cluster mode is on; otherwise there is no second runtime.
const RDMA_WORKER_THREADS: EnvVar = EnvVar {
    name: "PACER_RDMA_WORKER_THREADS",
    default: "0",
};
/// Objects at or below this size are proxied without caching.
const MIN_OBJECT_SIZE: EnvVar = EnvVar {
    name: "PACER_MIN_OBJECT_SIZE",
    default: "4MiB",
};
/// Optional upper bound on which objects are worth chunk-caching. **Unset =
/// unbounded** (the default). Chunk-granular caching (ADR-0015) streams and
/// stores an object of any size as `chunk_size` pieces distributed across the
/// ring, with per-read memory bounded to `fill_parallelism × chunk_size`
/// regardless of object length — so a multi-GiB checkpoint shard (or larger) is
/// cached, not bypassed. This is NOT a disk-persistence limit (entries are
/// chunks, so it is independent of `block_size`); set a value only as an
/// operator safety valve to proxy pathologically large objects through
/// uncached.
const MAX_OBJECT_SIZE: EnvVar = EnvVar {
    name: "PACER_MAX_OBJECT_SIZE",
    default: "",
};
/// Cluster cache chunk size (ADR-0015). Part of every chunk key, so it is a
/// cache-flush event to change and must be pinned per cluster; default is
/// [`pacer_cache::chunk::DEFAULT_CHUNK_SIZE`] (16 MiB). Empty → that default.
const CHUNK_SIZE: EnvVar = EnvVar {
    name: "PACER_CHUNK_SIZE",
    default: "",
};
/// Concurrent chunk resolutions per client GET (ADR-0015). `0` → the built-in
/// [`DEFAULT_FILL_PARALLELISM`]. Memory per multi-chunk read is bounded by
/// `fill_parallelism × chunk_size`; benchmark-tuned under the restore storm.
const FILL_PARALLELISM: EnvVar = EnvVar {
    name: "PACER_FILL_PARALLELISM",
    default: "0",
};
/// Backend shape (ADR-0023): `express` (S3 Express One Zone directory bucket,
/// same-AZ — the ADR-0002 default) or `standard` (S3 Standard regional,
/// cross-AZ, full functional parity). Chosen explicitly, not sniffed from the
/// bucket name; an unknown value is a startup error. Default keeps existing
/// deployments on Express.
const BACKEND_TYPE: EnvVar = EnvVar {
    name: "PACER_BACKEND_TYPE",
    default: "express",
};
/// Backend endpoint override (zonal S3 Express endpoint in production;
/// localstack/minio in tests). Unset = regular S3 resolution.
const S3_ENDPOINT: EnvVar = EnvVar {
    name: "PACER_S3_ENDPOINT",
    default: "",
};
/// Force path-style addressing on the backend client (test backends).
const FORCE_PATH_STYLE: EnvVar = EnvVar {
    name: "PACER_FORCE_PATH_STYLE",
    default: "false",
};
/// Access key clients sign with (ADR-0006; not a secret).
const PLACEHOLDER_ACCESS_KEY: EnvVar = EnvVar {
    name: "PACER_PLACEHOLDER_ACCESS_KEY",
    default: "pacer",
};
/// Secret key clients sign with (ADR-0006; not a secret).
const PLACEHOLDER_SECRET_KEY: EnvVar = EnvVar {
    name: "PACER_PLACEHOLDER_SECRET_KEY",
    default: "pacer",
};
/// Bucket aliases, `alias=real,…` (ADR-0002; see `Config::bucket_map`). The
/// file expresses this as a native map; this flat form is the env override.
const BUCKET_MAP: EnvVar = EnvVar {
    name: "PACER_BUCKET_MAP",
    default: "",
};
/// This node's Kubernetes node name (downward API). Set = cluster mode on.
/// Env-only: it is per-pod, so it can never live in the shared ConfigMap.
const NODE_NAME: EnvVar = EnvVar {
    name: "PACER_NODE_NAME",
    default: "",
};
/// Address the peer gRPC service listens on.
const PEER_LISTEN_ADDR: EnvVar = EnvVar {
    name: "PACER_PEER_LISTEN_ADDR",
    default: "0.0.0.0:9100",
};
/// Buffered chunks between a peer blob producer and its response stream. Small
/// on purpose: it smooths scheduling jitter, the stream is the backpressure.
const PEER_CHANNEL_CAPACITY: EnvVar = EnvVar {
    name: "PACER_PEER_CHANNEL_CAPACITY",
    default: "8",
};
/// Per-stream HTTP/2 receive window on the peer plane, both as server and as client.
/// Empty = leave hyper's default (1 MiB inbound to the server, 2 MiB inbound to the
/// client). See [`pacer_transport::H2Windows`] for the asymmetry that makes this worth
/// a knob and for what the two values cost in memory.
const PEER_H2_STREAM_WINDOW: EnvVar = EnvVar {
    name: "PACER_PEER_H2_STREAM_WINDOW",
    default: "",
};
/// Per-connection HTTP/2 receive window on the peer plane. Shared by every concurrent
/// RPC to one peer, because channels are cached per peer and multiplex — so this, not
/// the per-stream window, is what a `StoreChunk` fan-out contends on. Empty = leave
/// hyper's default (1 MiB inbound to the server, 5 MiB inbound to the client).
const PEER_H2_CONNECTION_WINDOW: EnvVar = EnvVar {
    name: "PACER_PEER_H2_CONNECTION_WINDOW",
    default: "",
};
/// HTTP/2 connections opened per peer. One channel multiplexes every concurrent RPC to a
/// peer onto one connection, hence one TCP flow and one framing task, which is what the
/// save path's `StoreChunk` leg appears to be capped by. `0` → the built-in
/// [`pacer_transport::DEFAULT_PEER_CONNECTIONS`] (1, i.e. what a single cached channel has
/// always been); refused above [`pacer_transport::MAX_PEER_CONNECTIONS`].
const PEER_CONNECTIONS: EnvVar = EnvVar {
    name: "PACER_PEER_CONNECTIONS",
    default: "0",
};
/// Chunk directory cap (ADR-0017): holders tracked per chunk key before an
/// entry flips to "widely held". `0` → the built-in
/// [`pacer_ring::directory::DEFAULT_MAX_SHARERS_TRACKED`]. Must stay ≥
/// `replication_r` (ADR-0016) once that knob exists.
const MAX_SHARERS_TRACKED: EnvVar = EnvVar {
    name: "PACER_MAX_SHARERS_TRACKED",
    default: "0",
};
/// Replication factor R (ADR-0016 layer 2): the top-R ranked nodes co-home
/// every chunk, so both serving bandwidth and fill-storm resilience multiply by
/// R at the cost of R× storage. `0` → the built-in [`DEFAULT_REPLICATION_R`].
/// `1` is exactly ADR-0012 single-copy semantics (layer 2 off). Clamped to
/// `max_sharers_tracked` (the directory cannot list more homes than it tracks).
const REPLICATION_R: EnvVar = EnvVar {
    name: "PACER_REPLICATION_R",
    default: "0",
};
/// Requester-local admission threshold (ADR-0016 layer 1): a peer-owned chunk
/// is admitted locally only after this many fetches within
/// [`LOCAL_ADMISSION_WINDOW_SECS`]. `1` ≈ cache-everything (kills aggregate
/// capacity); higher ≈ ADR-0012 never-store. `0` → [`DEFAULT_LOCAL_ADMISSION_THRESHOLD`].
const LOCAL_ADMISSION_THRESHOLD: EnvVar = EnvVar {
    name: "PACER_LOCAL_ADMISSION_THRESHOLD",
    default: "0",
};
/// Window over which the [`LOCAL_ADMISSION_THRESHOLD`] fetches must fall, in
/// seconds. Bounds the frequency sketch's memory of a chunk's heat. `0` →
/// [`DEFAULT_LOCAL_ADMISSION_WINDOW_SECS`].
const LOCAL_ADMISSION_WINDOW_SECS: EnvVar = EnvVar {
    name: "PACER_LOCAL_ADMISSION_WINDOW_SECS",
    default: "0",
};
/// Percentage of this node's cache capacity that requester-local (peer-owned)
/// hot copies may occupy (ADR-0016 layer 1) — the percent form of the ADR's
/// `local_copy_capacity_fraction`. Caps storm heat so peer copies cannot evict
/// the node's own homed chunks wholesale. `0` → [`DEFAULT_LOCAL_COPY_CAPACITY_PERCENT`].
const LOCAL_COPY_CAPACITY_PERCENT: EnvVar = EnvVar {
    name: "PACER_LOCAL_COPY_CAPACITY_PERCENT",
    default: "0",
};
/// Requester-side RDMA arena size (ADR-0024): the registered bytes this node
/// pins to receive peers' WRITEs, node-wide, split across rails and carved into
/// `chunk_size` ranges. It buys concurrency directly — `ranges = bytes ÷
/// chunk_size` — and on the zero-copy serve path a range stays leased until the
/// S3 client drains the served `Bytes` (client-drain-bound, ~70× the wire time on
/// the planning/15 rung-1 gate), so the size an operator wants is the hold-time
/// arithmetic `throughput × hold_time`: ~37 GiB to sustain the measured ~58 GiB/s
/// transport ceiling at a 645 ms drain. Undersized, fetches queue on the arena
/// while the NIC idles (watch `pacer_rdma_requester_slots_in_use` pinned at
/// capacity). Empty → `pacer_transport::efa::DEFAULT_REQUESTER_ARENA_BYTES`
/// (4 GiB). Raising it must be reflected in the chart's
/// `efa.pinnedPoolReservation` — the pod's memory limit has to cover pinned
/// bytes the cache's own accounting cannot see.
///
/// Replaces `PACER_RDMA_REQUESTER_SLOTS`, whose slot count no longer exists as a
/// configured quantity (see [`LEGACY_RDMA_REQUESTER_SLOTS`]).
const RDMA_ARENA_BYTES: EnvVar = EnvVar {
    name: "PACER_RDMA_ARENA_BYTES",
    default: "",
};
/// Bytes to map as ADR-0028's cache slab: the cache's RAM tier, mapped and
/// registered on every rail at startup so a holder posts a WRITE straight out of
/// the cache instead of staging a copy into an arena. `0` — the default — keeps
/// the RAM tier on the heap and holders staging, which is the pre-ADR-0028
/// behaviour.
///
/// **Size it ABOVE `PACER_MEM_CAPACITY`, not equal to it.** A frame is occupied
/// for as long as anything holds the chunk: foyer's resident set *plus* every
/// chunk currently in flight to a client or a peer. Sized exactly at
/// `mem_capacity`, a full cache leaves no free frame, every new fill silently
/// falls back to a heap allocation, and the slab's pinned memory is wasted —
/// `pacer_cache_slab_heap_fallbacks_total` is the metric that catches it, and
/// headroom for the node's concurrent in-flight chunks is what prevents it.
/// The chart derives this from `cache.memCapacity` and that headroom.
///
/// Like [`RDMA_ARENA_BYTES`] this is pinned memory the cache's own byte
/// accounting cannot see, so the pod's memory limit (and, for hugepages, the
/// node reservation) must cover it — ONCE, even though it is registered once per
/// rail, because all 32 registrations pin the same pages.
const CACHE_SLAB_BYTES: EnvVar = EnvVar {
    name: "PACER_CACHE_SLAB_BYTES",
    default: "",
};
/// Explicit page size for the RDMA arenas, in MiB: `2` = 2 MiB hugepages,
/// `1024` = 1 GiB, `0` (the default) = ordinary 4 KiB pages. NOT a throughput
/// knob — planning/18 measured page size as irrelevant to bandwidth; it keeps one
/// MR spanning tens of GiB cheap to register and small in the NIC's translation
/// footprint. The default is base pages because an explicit-hugepage `mmap`
/// fails `ENOMEM` unless the POD requests `hugepages-<size>`, which the chart
/// only does when `efa.hugepages` is set — and the chart sets this knob from that
/// same value, so the two cannot drift. A request that cannot be honored degrades
/// to base pages with a warning, never a boot failure.
const RDMA_ARENA_PAGE_MIB: EnvVar = EnvVar {
    name: "PACER_RDMA_ARENA_PAGE_MIB",
    default: "0",
};
/// Whether each EFA rail's completion reaper is pinned to a CPU on that rail's own
/// NIC NUMA node, and its RDMA arenas registered from the same node (planning/19
/// D5 step 0). `1` (the default) = place them; `0` = leave everything to the
/// scheduler and the startup thread.
///
/// NOT a tuning knob — it is the control arm. planning/18's ~58 GiB/s
/// transport-only bar was measured WITH this discipline (`spike/efa/src/affinity.rs`
/// documents both mechanisms: a WRITE's NIC DMA-reads its source from host memory,
/// so far-socket pages cross the inter-socket link; and unpinned reapers "pack onto
/// shared cores [where] their wakeup latency starves the in-flight window"), while
/// D4 measured 15.7 GiB/s WITHOUT it. Keeping `0` reachable is what lets one image
/// run both arms on the same nodes. Placement is best-effort regardless: a host
/// whose sysfs topology cannot be read simply runs unplaced.
const RDMA_AFFINITY: EnvVar = EnvVar {
    name: "PACER_RDMA_AFFINITY",
    default: "1",
};
/// WRITEs a single EFA rail may have on the wire at once. `0` (the default) =
/// unbounded, i.e. a rail's depth is whatever the serve-admission gate and the
/// requester's rail round-robin happen to produce.
///
/// The last premise the transport-only bench has that the daemon lacked
/// (planning/19 D5.1): that bench keeps a fixed in-flight window per rail and
/// swept it as a first-class variable, finding the *shape* matters — window 1 beat
/// window 64 at 32 rails into host memory (planning/18), while the H2 HBM run
/// reached line rate at 64. Unbounded is an emergent number nobody chose, which is
/// the class of undeclared cap track D exists to remove; a value here makes it
/// explicit and sweepable. Exhaustion is backpressure (the serve waits), never an
/// error, so this can never push a fetch onto the gRPC fallback. Read
/// `pacer_rdma_rail_writes_in_flight` while sweeping: pinned at the window means
/// the window binds, well below means something upstream does.
const RDMA_RAIL_WINDOW: EnvVar = EnvVar {
    name: "PACER_RDMA_RAIL_WINDOW",
    default: "0",
};
/// Retired knob, kept only to fail loudly. It configured the fixed 64 MiB-slot
/// pool ADR-0024 replaced with a registered arena, so a value set here would
/// otherwise be silently ignored — and silently running with a *quarter* of the
/// intended buffer supply is exactly the invisible cap that investigation was
/// about. Startup rejects it with the conversion to [`RDMA_ARENA_BYTES`]:
/// `slots × 64 MiB`.
const LEGACY_RDMA_REQUESTER_SLOTS: &str = "PACER_RDMA_REQUESTER_SLOTS";
/// What one retired slot pinned, for the migration arithmetic in the error
/// [`LEGACY_RDMA_REQUESTER_SLOTS`] raises (the old `DEFAULT_SLOT_BYTES`).
const LEGACY_SLOT_BYTES: usize = 64 << 20;
/// EFA rails (devices) the transport brings up, in device order (A5
/// multi-rail). `0` — the default — means ALL rails the node exposes: one on
/// r8gd, 32 on p5.48xlarge. An operator caps it to reproduce single-rail
/// behavior (`1`) or to leave rails free for other tenants. Pool memory does
/// NOT scale with rails (the configured slot totals are distributed across
/// them), so the only per-rail cost is a QP/CQ/pump set.
const EFA_RAILS: EnvVar = EnvVar {
    name: "PACER_EFA_RAILS",
    default: "0",
};
/// SRD queue pairs each EFA rail brings up (A5 multi-QP). The holder round-robins
/// its outbound WRITEs across this many QPs per rail. The extra QPs share the
/// rail's one PD/CQ/pump, so the only per-QP cost is a send queue.
///
/// **Leave this at `1` on p5.** The original rationale here — "a single SRD QP tops
/// out well below a 100 Gbps rail's line rate, the provider serializes one QP's
/// doorbell/DMA pipeline" — was **measured false** (planning/18 § RESULT 2): on
/// p5.48xlarge one SRD QP reaches 11.379 GiB/s = 97.7 Gbps, full line rate, and
/// `qps_per_rail` ∈ {1, 2, 4, 8} produce *byte-identical* throughput. Even one QP at
/// an in-flight depth of 1 reaches 92.7 Gbps. A5's apparent ~6 GiB/s per-QP plateau
/// was the multi-rail aggregate wall observed through one rail's share of it, not a
/// QP limit. Raising this adds send queues without adding bandwidth; it is retained
/// (not removed) because the knob is a published contract and a future NIC with a
/// genuinely serialized doorbell would want it.
const EFA_QPS_PER_RAIL: EnvVar = EnvVar {
    name: "PACER_EFA_QPS_PER_RAIL",
    default: "1",
};
/// How often this node re-announces the chunks it holds/admitted to their
/// directory homes (ADR-0017 soft-state healer), in seconds. A home that
/// restarted and lost its shard is repopulated within one interval by every
/// holder's sweep — the directory analogue of `HANDSHAKE_SWEEP_INTERVAL`
/// re-establishing EFA endpoints. `0` → [`DEFAULT_REANNOUNCE_INTERVAL_SECS`].
const REANNOUNCE_INTERVAL_SECS: EnvVar = EnvVar {
    name: "PACER_REANNOUNCE_INTERVAL_SECS",
    default: "0",
};
/// Whether the daemon honours `x-pacer-target` and delivers into client-supplied
/// memory (ADR-0026). `0`/unset — the default — ignores the header and serves
/// every read as a body.
///
/// Off by default on purpose: delivery maps and pins memory a *client* named and
/// hands a peer an rkey into it, which is a privilege surface, not a tuning knob.
/// Compatibility does not depend on this — a client that sends no header is
/// unaffected either way — so the only thing enabling it changes is whether the
/// accelerated path exists at all.
const DELIVERY_ENABLED: EnvVar = EnvVar {
    name: "PACER_DELIVERY_ENABLED",
    default: "0",
};
/// Whether a PUT is scattered across the chunks' homes (ADR-0032). **Three states,
/// and the empty default is the interesting one**: unset means *on where the design
/// applies* — a general-purpose (Standard) backend — and off on an Express directory
/// bucket, which ADR-0032 § 6 leaves on ADR-0007's plain proxy. An explicit
/// `1/true/on/yes` or `0/false/no` wins over that in either direction.
///
/// It was opt-in until 2026-09-01, when the last ADR-0032 gate closed (4.5, 4.73×
/// against a 2.00× bar). Two things about the flip are worth keeping written down:
///
/// * **it could not be a boolean default.** Express is [`BackendType`]'s own default,
///   and an enabled scatter on Express is a hard startup error below — so a blanket
///   `true` would have refused to start every Express daemon. Scoping it to the
///   backend is what makes a default possible at all.
/// * **it changes what a client sees.** A scattered PUT is a multipart upload, so its
///   ETag is the composite `-N` form rather than a plain MD5. The chart's `scatter`
///   block in values.yaml carries the operator-facing version of this, including the
///   `x-amz-checksum-crc32` (FULL_OBJECT, S3-enforced) that survives reassembly and is
///   what an integrity check should compare instead.
///
/// Truthiness is strict, and asymmetrically so: only an EMPTY value derives from the
/// backend, while any non-empty value that is not truthy — `false`, `no`, or a typo like
/// `ture` — is OFF. That is the safe direction for the one thing this knob can break: a
/// mistyped value can lose the scatter's speedup, but it can never turn the composite-ETag
/// form on for a client that did not ask for it.
const SCATTER_ENABLED: EnvVar = EnvVar {
    name: "PACER_SCATTER_ENABLED",
    default: "",
};
/// Node-wide ceiling on bytes held for peers between their `UploadPart` and the
/// coordinator's commit (ADR-0032 § 4). Empty →
/// [`crate::scatter::DEFAULT_STAGING_BYTES`].
///
/// This is the reject-fast threshold: past it an owner refuses offers instead of
/// queueing them, which is what keeps an all-to-all shuffle from deadlocking and
/// what makes the scatter self-limit to nodes with spare egress.
///
/// **Sizing it is not about load.** A staged window is held until its upload's
/// Complete, and Complete waits for the whole object, so what this bounds is
/// `(bytes of concurrently-uploading objects) ÷ N`. A save where every rank writes
/// at once needs the whole checkpoint divided by the fleet — see
/// [`crate::scatter::DEFAULT_STAGING_BYTES`] for the worked example and for why
/// the intended answer to a save that does not fit is the fallback, not a bigger
/// number here.
const SCATTER_STAGING_BYTES: EnvVar = EnvVar {
    name: "PACER_SCATTER_STAGING_BYTES",
    default: "",
};
/// Seconds a staged chunk may sit before the reaper drops it (ADR-0032 § 4).
/// `0` → [`crate::scatter::DEFAULT_STAGING_TTL_SECS`]. The backstop for a
/// coordinator that died between an owner's upload and its commit; without it one
/// dead writer would hold budget and refuse every later write on that node.
const SCATTER_STAGING_TTL_SECS: EnvVar = EnvVar {
    name: "PACER_SCATTER_STAGING_TTL_SECS",
    default: "0",
};
/// Windows a coordinator keeps in flight (ADR-0032 § 2). `0` →
/// [`crate::scatter::DEFAULT_WINDOWS_IN_FLIGHT`]. Bounds the coordinator's own
/// memory to `this × chunk_size`, and is the backpressure that stops it reading
/// the client's socket faster than owners can absorb windows.
const SCATTER_WINDOWS_IN_FLIGHT: EnvVar = EnvVar {
    name: "PACER_SCATTER_WINDOWS_IN_FLIGHT",
    default: "0",
};
/// Smallest object to scatter (ADR-0032). Empty →
/// [`crate::scatter::DEFAULT_MIN_SCATTER_BYTES`]. Below it a PUT keeps its
/// original shape, so the ETag change is confined to objects large enough for the
/// parallel upload to pay for it.
///
/// Distinct from [`MIN_OBJECT_SIZE`], which decides what is *cacheable*: an
/// object can be well worth caching and still too small to be worth scattering.
const SCATTER_MIN_OBJECT_BYTES: EnvVar = EnvVar {
    name: "PACER_SCATTER_MIN_OBJECT_BYTES",
    default: "",
};
/// Seconds a coordinator stops offering to an owner that refused for load
/// (ADR-0032 § 4). `0` → [`crate::scatter::DEFAULT_SATURATED_COOLDOWN_SECS`].
///
/// Needed because an owner can only refuse *after* gRPC delivered the window, so
/// re-offering every window to a saturated peer wastes one transfer per window
/// instead of one per peer.
const SCATTER_SATURATED_COOLDOWN_SECS: EnvVar = EnvVar {
    name: "PACER_SCATTER_SATURATED_COOLDOWN_SECS",
    default: "0",
};
/// Directory a `shm:/name` descriptor resolves under (ADR-0026). Default
/// [`pacer_daemon_delivery::DEFAULT_SHM_DIR`](crate::delivery::DEFAULT_SHM_DIR) —
/// `/dev/shm`, where `shm_open` puts POSIX segments. A knob because sharing a
/// segment with a loader pod means mounting a tmpfs both can see
/// (`emptyDir{medium: Memory}`), which need not be at the default path.
const DELIVERY_SHM_DIR: EnvVar = EnvVar {
    name: "PACER_DELIVERY_SHM_DIR",
    default: "",
};
/// Per-request ceiling on client memory one GET may pin (ADR-0026 point 8).
/// Empty → [`crate::delivery::DEFAULT_MAX_TARGET_BYTES`]. A target above it is
/// served as a body, so this bounds a client's mistake, not its workload.
const DELIVERY_MAX_TARGET_BYTES: EnvVar = EnvVar {
    name: "PACER_DELIVERY_MAX_TARGET_BYTES",
    default: "",
};
/// Node-wide ceiling on concurrently pinned client memory (ADR-0026 point 8).
/// Empty → [`crate::delivery::DEFAULT_PINNED_BYTES_MAX`]. Independent of
/// [`RDMA_ARENA_BYTES`]: those pages are the daemon's, these are clients' — both
/// have to fit the node, and only this one grows with how many loaders run.
const DELIVERY_PINNED_BYTES_MAX: EnvVar = EnvVar {
    name: "PACER_DELIVERY_PINNED_BYTES_MAX",
    default: "",
};
/// Windows delivered concurrently per client-memory GET (ADR-0026). `0` → the
/// built-in [`crate::delivery::DEFAULT_DELIVERY_PARALLELISM`] (64).
///
/// Deliberately NOT `PACER_FILL_PARALLELISM`: that one bounds an *ordered* body
/// stream, where look-ahead past the reorder window is wasted. Delivery windows
/// are disjoint destinations in the client's buffer, and the workload that matters
/// is one GET for a multi-GiB checkpoint — thousands of windows, where the body
/// path's 8 would serialize a fabric-limited transfer into ~N/8 rounds.
const DELIVERY_PARALLELISM: EnvVar = EnvVar {
    name: "PACER_DELIVERY_PARALLELISM",
    default: "0",
};
/// Whether a remote chunk bound for a **client-registered** window (`nic:`, ADR-0030)
/// is written by its HOLDER directly rather than routed through this node
/// (planning/19 C3, "parallel fill"). `0`/unset — the default — keeps the two-hop
/// path: the chunk arrives in this node's memory and is written on from here.
///
/// Off by default as a **control arm**, not a safety valve. Both paths deliver the
/// same bytes with the same integrity guarantee (a holder that cannot write streams
/// instead, which *is* the two-hop path), so nothing about enabling it can fail a read
/// that would otherwise have succeeded. What it changes is where the hop and the
/// per-endpoint announce cost land, and neither is answerable without running both
/// arms on one image. No effect on `shm:` targets, where the daemon registered the
/// window and holders have always written into it directly.
const DELIVERY_REMOTE_WRITE: EnvVar = EnvVar {
    name: "PACER_DELIVERY_REMOTE_WRITE",
    default: "0",
};
/// Fraction of this container's cgroup limit the startup memory check leaves
/// unclaimed by the terms it can compute (see
/// [`crate::memory_budget::DEFAULT_HEADROOM_FRACTION`] for why the value is 0.10 and
/// why it is an UPPER bound rather than a safety factor). Empty → that default.
const MEMORY_HEADROOM_FRACTION: EnvVar = EnvVar {
    name: "PACER_MEMORY_HEADROOM_FRACTION",
    default: "",
};

/// What the startup memory check does when the configured budget does not fit:
/// `enforce` (refuse to start, the default) or `warn`. A knob because a migration may
/// knowingly run a limit the arithmetic has not caught up with, and a fleet that
/// cannot start is worse than one that logs — but only when an operator chose it.
const MEMORY_CHECK: EnvVar = EnvVar {
    name: "PACER_MEMORY_CHECK",
    default: "enforce",
};

/// Static membership list, `name=addr,…` (tests/dev; wins over the watch).
/// Env-only: tests/dev inject it directly.
const PEERS: EnvVar = EnvVar {
    name: "PACER_PEERS",
    default: "",
};
/// Namespace of the peer Service for the EndpointSlice watch (downward API).
const NAMESPACE: EnvVar = EnvVar {
    name: "PACER_NAMESPACE",
    default: "default",
};
/// Name of the headless peer Service to watch (production membership).
const PEER_SERVICE: EnvVar = EnvVar {
    name: "PACER_PEER_SERVICE",
    default: "",
};

/// How the daemon encodes its log lines (`PACER_LOG_FORMAT`, ADR-0036).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogFormat {
    /// One JSON object per line, span fields flattened into the event so
    /// `kubectl logs | jq '.chunk_key'` works without walking a `spans` array.
    /// The default.
    #[default]
    Json,
    /// `tracing_subscriber`'s human-readable formatter — what the daemon emitted
    /// before ADR-0036, kept for tailing a dev pod.
    Text,
}

impl LogFormat {
    /// The default's spelling, so [`LOG_FORMAT`]'s built-in default and this enum
    /// cannot disagree.
    const DEFAULT_NAME: &'static str = "json";
}

impl std::str::FromStr for LogFormat {
    type Err = anyhow::Error;

    /// # Errors
    ///
    /// Any spelling other than `json`/`text`. Refused rather than defaulted: the
    /// value decides whether a fleet's logs are machine-readable at all, and a
    /// typo that silently produced the other encoding would be discovered by a
    /// broken log pipeline rather than by a startup error.
    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "text" | "plain" => Ok(Self::Text),
            other => anyhow::bail!(
                "{} must be `json` or `text`, got `{other}`",
                LOG_FORMAT.name
            ),
        }
    }
}

impl std::fmt::Display for LogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Json => "json",
            Self::Text => "text",
        })
    }
}

/// Resolve ONLY the log format, before the rest of the configuration is loaded.
///
/// The subscriber has to exist before [`Config::load`] runs, because `load` itself
/// warns (an unpersistable chunk size, an undersized slab) and those warnings are
/// dropped by a process with no subscriber. So this reads the same three layers
/// `resolve` does — and reads the config file a second time to do it, which is a
/// few microseconds once per process start.
///
/// Lenient by design, unlike `resolve`: an unreadable file or an unknown value
/// yields [`LogFormat::Json`] here rather than failing, because failing before the
/// subscriber exists produces a silent non-zero exit. The strict check still
/// happens — [`Config::load`] resolves the same value through `FromStr` moments
/// later and *that* error is logged, in the format this function chose.
#[must_use]
pub fn log_format() -> LogFormat {
    let env = |k: &str| std::env::var(k).ok();
    let file = read_file_config(&env).unwrap_or_default();
    LOG_FORMAT
        .resolve(&env, file.log.and_then(|l| l.format))
        .parse()
        .unwrap_or_default()
}

/// Runtime configuration for one daemon instance.
#[derive(Debug, Clone)]
pub struct Config {
    /// Address the S3 API listens on. Pods reach it via a Service with
    /// `internalTrafficPolicy: Local` (or a link-local IP à la node-local-dns).
    pub listen_addr: String,
    /// Bounds on the S3 listener — connection cap, header/keep-alive deadline,
    /// no-progress deadline (ADR-0036). The daemon's other queues are all
    /// bounded; this is where request volume enters.
    pub listen: crate::listen::ListenLimits,
    /// How long in-flight work may finish after SIGTERM before the remaining
    /// tasks are dropped (ADR-0036). Must stay below the pod's
    /// `terminationGracePeriodSeconds`.
    pub shutdown_drain_timeout: std::time::Duration,
    /// Log encoding (ADR-0036). Resolved twice — once by [`log_format`] before
    /// the subscriber is built, once here so the startup line reports it and a
    /// bad value is a startup error.
    pub log_format: LogFormat,
    /// Address for health/readiness/metrics (kubelet probes hit this).
    pub admin_addr: String,
    /// tokio worker threads; `0` = tokio default (one per core).
    pub worker_threads: usize,
    /// tokio worker threads for the dedicated RDMA serve-path runtime; `0` =
    /// tokio default (one per core). Only used when the `efa` feature is built
    /// in and cluster mode is on (see `RDMA_WORKER_THREADS`).
    pub rdma_worker_threads: usize,
    /// Hybrid cache tier sizing and placement.
    pub cache: CacheConfig,
    /// Objects smaller than this are proxied without caching
    /// (small objects are cheap to re-fetch and would churn the cache).
    pub min_object_size: u64,
    /// Optional cap on which objects are chunk-cached; `None` = unbounded (the
    /// default). Chunking (ADR-0015) makes this a policy valve, not a limit —
    /// an object of any size is stored as `chunk_size` pieces, so it is
    /// independent of `block_size`. Set via `PACER_MAX_OBJECT_SIZE`.
    pub max_object_size: Option<u64>,
    /// Cluster cache chunking (ADR-0015). The chunk size is embedded in every
    /// chunk key, so changing it orphans (never corrupts) existing entries —
    /// a cache-flush event, pinned per cluster.
    pub chunk: pacer_cache::chunk::ChunkConfig,
    /// Whether a chunk read promotes a disk hit into the RAM tier. foyer's own
    /// `get` always does; on a one-pass checkpoint sweep that promotion hit 5.3 %
    /// and evicts fresh fill data that then has to be written down, so
    /// `Promotion::Never` exists to take it out of the path
    /// (`PACER_CHUNK_PROMOTION`). Read only on the `foyer` disk tier — the ADR-0033
    /// store *is* the tier, so it has nothing to promote into.
    pub promotion: pacer_cache::Promotion,
    /// Which implementation holds chunk bodies on disk (ADR-0033). `PACER_DISK_TIER`.
    pub disk_tier: pacer_cache::DiskTier,
    /// Verify a chunk body's CRC32 on every store read (`PACER_VERIFY_CHUNK_BODY`).
    /// See ADR-0033 § Integrity for why this is off by default and what still *is*
    /// checked when it is.
    pub verify_chunk_body: bool,
    /// Whether an `If-Match` GET may be served from cache on an ETag match (ADR-0039,
    /// `PACER_CONDITIONAL_GET_FROM_CACHE`). Defaults **on**, because Mountpoint-for-S3 puts
    /// `If-Match` on every GET and so measured a 0 % hit rate against this cache; `false`
    /// restores the unconditional passthrough. ADR-0039 records what that trades — on a hit
    /// the ETag compared against is the cached one, so `If-Match` through PACER cannot
    /// *detect* an in-place replacement.
    pub conditional_get_from_cache: bool,
    /// Whether a store hit's two reads are issued together (`PACER_STORE_READ_SHAPE`).
    /// Read only when `PACER_DISK_TIER=store`.
    pub store_read_shape: pacer_cache::store::ReadShape,
    /// Chunk reads in flight against the store, node-wide
    /// (`PACER_STORE_READ_CONCURRENCY`). Resolved, so `0` here means unlimited and the
    /// built-in default has already been applied.
    pub store_read_concurrency: usize,
    /// Max chunk resolutions in flight per client GET (ADR-0015). Bounds a
    /// multi-chunk read's memory to `fill_parallelism × chunk_size` regardless of
    /// object size, while overlapping backend/peer fetches for throughput. The
    /// client body still emits chunks in order (see the proxy's ordered pipeline).
    pub fill_parallelism: usize,
    /// Backend S3 client settings (endpoint, addressing style).
    pub backend: BackendConfig,
    /// Placeholder credentials clients sign with (ADR-0006). Not secret —
    /// they only gate malformed requests; real auth is the daemon's identity.
    pub placeholder_access_key: String,
    /// Secret half of the placeholder credentials (ADR-0006; not a secret).
    pub placeholder_secret_key: String,
    /// Bucket aliases (`alias=real,…`). Clients MUST use plain bucket names:
    /// a name matching the directory-bucket pattern (`*--x-s3`) flips SDKs
    /// into Express-specific behavior (zonal DNS, CreateSession, `s3express`
    /// signing scope) aimed at the proxy, which cannot honor it. The alias
    /// also picks the same-AZ bucket for this node (ADR-0002).
    pub bucket_map: HashMap<String, String>,
    /// Cluster tier (Phase 2). None = single-node (Phase 1 behavior).
    pub cluster: Option<ClusterConfig>,
    /// Client-memory delivery (ADR-0026): whether `x-pacer-target` is honoured,
    /// where segments are looked up, and the two pinned-memory ceilings.
    /// Independent of `cluster` — a single-node daemon can deliver local hits
    /// into client memory, it just has no peers to WRITE from.
    pub delivery: crate::delivery::DeliveryConfig,
    /// Write scatter (ADR-0032): whether a PUT of a new key is decomposed onto
    /// the chunk grid and uploaded by the chunks' homes, plus the staging budget
    /// this node offers peers and the coordinator-side depth and thresholds.
    ///
    /// Only meaningful with `cluster` set — a single-node daemon has no homes to
    /// scatter to — but resolved independently so the two can be reasoned about
    /// separately, exactly like `delivery`.
    pub scatter: crate::scatter::ScatterConfig,
    /// The startup memory check (quality item R3): whether a configured footprint
    /// that does not fit this container's cgroup limit refuses the start, and how
    /// much of the limit is left for the terms no configuration can express. See
    /// [`crate::memory_budget`].
    pub memory: crate::memory_budget::MemoryCheckConfig,
}

/// Cluster-tier settings (Phase 2, ADR-0012).
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This node's stable identity — the Kubernetes node name (downward API
    /// `spec.nodeName`). Must match the EndpointSlice `nodeName` of this pod.
    pub node_name: String,
    /// Address the peer gRPC service listens on.
    pub peer_listen_addr: String,
    /// Port peers are dialed on (same on every node — DaemonSet symmetry).
    pub peer_port: u16,
    /// Buffered chunks per peer blob stream (see `PEER_CHANNEL_CAPACITY`).
    pub channel_capacity: usize,
    /// HTTP/2 receive windows for the peer plane, applied to both the peer server and
    /// the channels this node dials. Default (both `None`) is hyper's own behaviour.
    pub h2_windows: pacer_transport::H2Windows,
    /// Connections this node opens per peer, ≥ 1 — see
    /// [`pacer_transport::DEFAULT_PEER_CONNECTIONS`] for why one is a ceiling and what
    /// widening it costs.
    pub peer_connections: usize,
    /// Chunk directory cap (ADR-0017): holders tracked per chunk key before
    /// an entry flips to "widely held" and stops recording new ones.
    pub max_sharers_tracked: usize,
    /// Replication factor R (ADR-0016 layer 2): the top-R ranked nodes co-home
    /// every chunk. `1` is single-copy (ADR-0012). Clamped to
    /// `max_sharers_tracked` so the directory can list every home.
    pub replication_r: usize,
    /// Fetches within [`Self::local_admission_window`] before a requester keeps
    /// a peer-owned chunk locally (ADR-0016 layer 1).
    pub local_admission_threshold: u32,
    /// Window the admission fetches must fall within (ADR-0016 layer 1).
    pub local_admission_window: std::time::Duration,
    /// Fraction (0.0–1.0) of cache capacity requester-local hot copies may
    /// occupy (ADR-0016 layer 1), so peer-copy heat cannot evict this node's
    /// own homed chunks wholesale.
    pub local_copy_capacity_fraction: f64,
    /// Requester-side RDMA arena bytes, node-wide (see
    /// `PACER_RDMA_ARENA_BYTES`); the node's concurrent-peer-fetch capacity on
    /// the zero-copy path, since ranges = bytes ÷ `chunk_size`. `0` = the
    /// transport's default arena.
    pub rdma_arena_bytes: usize,
    /// Bytes to map as ADR-0028's cache slab — the cache's RAM tier made
    /// RDMA-postable in place (`PACER_CACHE_SLAB_BYTES`). `0` = no slab, cached
    /// chunks on the heap, holders stage their WRITEs.
    pub cache_slab_bytes: usize,
    /// Explicit page size for the RDMA arenas, in MiB (see
    /// `PACER_RDMA_ARENA_PAGE_MIB`). `0` = ordinary 4 KiB pages.
    pub rdma_arena_page_mib: usize,
    /// Whether to place each rail's reaper + arenas on its NIC's NUMA node (see
    /// `PACER_RDMA_AFFINITY`). `false` is the pre-D5 control arm.
    pub rdma_affinity: bool,
    /// WRITEs one rail may have on the wire at once (see
    /// `PACER_RDMA_RAIL_WINDOW`). `0` = unbounded.
    pub rdma_rail_window: usize,
    /// EFA rails to bring up (see `PACER_EFA_RAILS`). `0` = all devices.
    pub efa_rails: usize,
    /// SRD queue pairs per rail (see `PACER_EFA_QPS_PER_RAIL`). `1` = one QP
    /// per rail (pre-multi-QP behavior); the transport clamps `0` up to `1`.
    pub efa_qps_per_rail: usize,
    /// How often the background healer re-announces this node's held/admitted
    /// chunks to their directory homes (ADR-0017 soft state), so a restarted
    /// home's shard reconverges without waiting on organic re-reads.
    pub reannounce_interval: std::time::Duration,
    /// Where the member set comes from.
    pub membership: Membership,
}

/// Membership source for the ring.
#[derive(Debug, Clone)]
pub enum Membership {
    /// Watch the EndpointSlices of the peer Service (production).
    K8s {
        /// Namespace the peer Service lives in.
        namespace: String,
        /// Name of the headless peer Service.
        service: String,
    },
    /// Fixed `name=addr,…` list (tests, dev, non-K8s runs).
    Static {
        /// The complete member set.
        peers: Vec<pacer_ring::NodeId>,
    },
}

// ---- config-file schema (ADR-0013) ----
//
// Mirrors `Config` but every field is optional: a present value is the middle
// precedence layer, an absent one falls through to the built-in default.
// `deny_unknown_fields` turns a mistyped key into a startup error instead of a
// silent default. Sizes stay strings so the file and env share `parse_bytes`.

/// Root of the YAML config file.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct FileConfig {
    listen_addr: Option<String>,
    admin_addr: Option<String>,
    listen: Option<FileListen>,
    shutdown: Option<FileShutdown>,
    log: Option<FileLog>,
    cache: Option<FileCache>,
    runtime: Option<FileRuntime>,
    policy: Option<FilePolicy>,
    backend: Option<FileBackend>,
    auth: Option<FileAuth>,
    bucket_map: Option<HashMap<String, String>>,
    cluster: Option<FileCluster>,
    delivery: Option<FileDelivery>,
    scatter: Option<FileScatter>,
    memory: Option<FileMemory>,
}

/// `memory:` block — the startup budget check (quality item R3).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct FileMemory {
    headroom_fraction: Option<String>,
    check: Option<String>,
}

/// `listen:` block — the S3 listener's bounds (ADR-0036). `0`/absent means the
/// built-in default for each, so an arm can move one alone.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct FileListen {
    max_connections: Option<usize>,
    header_timeout_secs: Option<u64>,
    idle_timeout_secs: Option<u64>,
}

/// `shutdown:` block (ADR-0036).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct FileShutdown {
    drain_timeout_secs: Option<u64>,
}

/// `log:` block (ADR-0036). Only the encoding: the *filter* stays `RUST_LOG`,
/// which is `tracing_subscriber`'s own contract and not something this daemon
/// should re-spell.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct FileLog {
    format: Option<String>,
}

/// `delivery:` block (ADR-0026).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct FileDelivery {
    enabled: Option<bool>,
    shm_dir: Option<String>,
    max_target_bytes: Option<String>,
    pinned_bytes_max: Option<String>,
    parallelism: Option<usize>,
    remote_write: Option<bool>,
}

/// `scatter:` block (ADR-0032).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct FileScatter {
    enabled: Option<bool>,
    staging_bytes: Option<String>,
    staging_ttl_secs: Option<u64>,
    windows_in_flight: Option<usize>,
    min_object_bytes: Option<String>,
    saturated_cooldown_secs: Option<u64>,
}

/// `cache:` block of the config file.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct FileCache {
    dir: Option<String>,
    mem_capacity: Option<String>,
    disk_capacity: Option<String>,
    block_size: Option<String>,
    flush_buffer_size: Option<String>,
    io_engine: Option<String>,
    uring: Option<FileUring>,
    tuning: Option<FileStorageTuning>,
}

/// `cache.uring:` block.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct FileUring {
    threads: Option<usize>,
    io_depth: Option<usize>,
}

/// `cache.tuning:` block (see `pacer_cache::StorageTuning`).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct FileStorageTuning {
    flushers: Option<usize>,
    reclaimers: Option<usize>,
    submit_queue_threshold: Option<String>,
    storage_runtime_threads: Option<usize>,
}

/// `runtime:` block.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct FileRuntime {
    worker_threads: Option<usize>,
    rdma_worker_threads: Option<usize>,
}

/// `policy:` block.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct FilePolicy {
    min_object_size: Option<String>,
    max_object_size: Option<String>,
    chunk_size: Option<String>,
    fill_parallelism: Option<usize>,
    promotion: Option<String>,
    disk_tier: Option<String>,
    verify_chunk_body: Option<bool>,
    conditional_get_from_cache: Option<bool>,
    store_read_shape: Option<String>,
    store_read_concurrency: Option<String>,
}

/// `backend:` block.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct FileBackend {
    backend_type: Option<String>,
    endpoint: Option<String>,
    force_path_style: Option<bool>,
}

/// `auth:` block.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct FileAuth {
    placeholder_access_key: Option<String>,
    placeholder_secret_key: Option<String>,
}

/// `cluster:` block. Only the release-wide knobs live here; the per-pod
/// membership inputs (node name, namespace, peers, service) stay env-only.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct FileCluster {
    peer_listen_addr: Option<String>,
    channel_capacity: Option<usize>,
    h2_stream_window: Option<String>,
    h2_connection_window: Option<String>,
    peer_connections: Option<usize>,
    max_sharers_tracked: Option<usize>,
    replication_r: Option<usize>,
    local_admission_threshold: Option<usize>,
    local_admission_window_secs: Option<usize>,
    local_copy_capacity_percent: Option<usize>,
    rdma_arena_bytes: Option<String>,
    cache_slab_bytes: Option<String>,
    rdma_arena_page_mib: Option<usize>,
    rdma_affinity: Option<bool>,
    rdma_rail_window: Option<usize>,
    efa_rails: Option<usize>,
    efa_qps_per_rail: Option<usize>,
    reannounce_interval_secs: Option<usize>,
}

impl Config {
    /// Load configuration: built-in defaults < config file < `PACER_*` env.
    ///
    /// # Errors
    ///
    /// Fails when the config file exists but cannot be read or parsed
    /// (including unknown keys), when a size or numeric value does not parse,
    /// or when cluster mode is enabled without a membership source.
    pub fn load() -> anyhow::Result<Self> {
        let env = |k: &str| std::env::var(k).ok();
        let file = read_file_config(&env)?;
        resolve(&file, &env)
    }
}

/// Locate and parse the config file. Precedence for the path: `PACER_CONFIG`,
/// else [`DEFAULT_CONFIG_PATH`] if it exists, else no file (all defaults).
fn read_file_config(env: EnvFn) -> anyhow::Result<FileConfig> {
    let explicit = CONFIG_PATH.resolve_opt(env, None);
    let path = match explicit {
        Some(p) => p,
        None if std::path::Path::new(DEFAULT_CONFIG_PATH).is_file() => {
            DEFAULT_CONFIG_PATH.to_string()
        }
        None => return Ok(FileConfig::default()),
    };
    let text = std::fs::read_to_string(&path)
        .map_err(|e| anyhow::anyhow!("reading config file {path}: {e}"))?;
    serde_yaml_ng::from_str(&text).map_err(|e| anyhow::anyhow!("parsing config file {path}: {e}"))
}

/// Merge the three layers into a [`Config`]. The single home of precedence:
/// pure over `(file, env)` so it is testable without process-global state.
///
/// # Errors
///
/// Propagates size/number parse failures and the cluster-without-membership
/// error (see [`cluster_config`]).
fn resolve(file: &FileConfig, env: EnvFn) -> anyhow::Result<Config> {
    let cfg = Config {
        listen_addr: LISTEN_ADDR.resolve(env, file.listen_addr.clone()),
        admin_addr: ADMIN_ADDR.resolve(env, file.admin_addr.clone()),
        listen: listen_limits(file, env)?,
        shutdown_drain_timeout: drain_timeout(file, env)?,
        log_format: LOG_FORMAT
            .resolve(env, file.log.as_ref().and_then(|l| l.format.clone()))
            .parse()?,
        worker_threads: parse_num(
            &WORKER_THREADS.resolve(
                env,
                file.runtime
                    .as_ref()
                    .and_then(|r| r.worker_threads)
                    .map(num_to_string),
            ),
        )?,
        rdma_worker_threads: parse_num(
            &RDMA_WORKER_THREADS.resolve(
                env,
                file.runtime
                    .as_ref()
                    .and_then(|r| r.rdma_worker_threads)
                    .map(num_to_string),
            ),
        )?,
        cache: cache_config(file, env)?,
        promotion: CHUNK_PROMOTION
            .resolve(env, policy_field(file, |p| p.promotion.clone()))
            .parse()?,
        disk_tier: DISK_TIER
            .resolve(env, policy_field(file, |p| p.disk_tier.clone()))
            .parse()?,
        verify_chunk_body: verify_chunk_body(file, env),
        conditional_get_from_cache: conditional_get_from_cache(file, env),
        store_read_shape: STORE_READ_SHAPE
            .resolve(env, policy_field(file, |p| p.store_read_shape.clone()))
            .parse()?,
        store_read_concurrency: store_read_concurrency(file, env)?,
        min_object_size: parse_bytes(
            &MIN_OBJECT_SIZE.resolve(env, policy_field(file, |p| p.min_object_size.clone())),
        )?,
        max_object_size: MAX_OBJECT_SIZE
            .resolve_opt(env, policy_field(file, |p| p.max_object_size.clone()))
            .map(|s| parse_bytes(&s))
            .transpose()?,
        chunk: chunk_config(file, env)?,
        fill_parallelism: fill_parallelism(file, env)?,
        backend: BackendConfig {
            backend_type: backend_type(file, env)?,
            endpoint: S3_ENDPOINT
                .resolve_opt(env, file.backend.as_ref().and_then(|b| b.endpoint.clone())),
            force_path_style: force_path_style(file, env),
        },
        placeholder_access_key: PLACEHOLDER_ACCESS_KEY
            .resolve(env, auth_field(file, |a| a.placeholder_access_key.clone())),
        placeholder_secret_key: PLACEHOLDER_SECRET_KEY
            .resolve(env, auth_field(file, |a| a.placeholder_secret_key.clone())),
        bucket_map: bucket_map(file, env),
        cluster: cluster_config(file, env)?,
        delivery: delivery_config(file, env)?,
        scatter: scatter_config(file, env, backend_type(file, env)?)?,
        memory: memory_check_config(file, env)?,
    };
    Ok(cfg.warn_unpersistable_chunks())
}

/// Resolve a [`Config`] from a config-file body and a fixed environment.
///
/// `pub(crate)` and test-only so a sibling module can exercise the daemon's **own**
/// three-layer resolution instead of hand-building a `Config`. [`crate::memory_budget`]
/// is why it exists: its chart-parity tests assert the budget against the ConfigMap
/// `helm template` actually renders, and that only proves anything if the daemon parsed
/// that exact YAML — a hand-built struct would assert the test author's reading of the
/// chart rather than the daemon's.
///
/// # Errors
///
/// Whatever [`resolve`] and the YAML parse raise, so a fixture that has drifted from
/// the file schema fails loudly instead of silently defaulting.
#[cfg(test)]
pub(crate) fn resolve_for_test(yaml: &str, env: &[(&str, &str)]) -> anyhow::Result<Config> {
    let file: FileConfig = serde_yaml_ng::from_str(yaml)?;
    let map: HashMap<String, String> = env
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    let lookup = move |k: &str| map.get(k).cloned();
    resolve(&file, &lookup)
}

/// Resolve the startup memory check's two knobs (quality item R3).
///
/// The headroom is "empty → the built-in default", the same idiom as every byte
/// ceiling here, and deliberately NOT "0 → the default": a control arm has to be able
/// to ask for no margin at all, and `0.0` is exactly that request.
///
/// # Errors
///
/// A headroom that does not parse as a number or is outside `[0, 1)`, or a mode that
/// is neither `enforce` nor `warn` — both are configuration this daemon must not
/// guess at, since guessing either way changes whether it starts.
fn memory_check_config(
    file: &FileConfig,
    env: EnvFn,
) -> anyhow::Result<crate::memory_budget::MemoryCheckConfig> {
    let block = file.memory.as_ref();
    let headroom = match MEMORY_HEADROOM_FRACTION
        .resolve_opt(env, block.and_then(|m| m.headroom_fraction.clone()))
    {
        Some(raw) => crate::memory_budget::Headroom::new(raw.trim().parse().map_err(|e| {
            anyhow::anyhow!(
                "{} is not a number: {raw:?} ({e})",
                MEMORY_HEADROOM_FRACTION.name
            )
        })?)?,
        None => crate::memory_budget::Headroom::DEFAULT,
    };
    Ok(crate::memory_budget::MemoryCheckConfig {
        mode: MEMORY_CHECK
            .resolve(env, block.and_then(|m| m.check.clone()))
            .parse()?,
        headroom,
    })
}

impl Config {
    /// Warn if a cache entry can never reach the disk tier.
    ///
    /// A foyer block is the largest entry the disk tier can persist; an entry
    /// bigger than a block fills RAM with pieces that silently never reach NVMe
    /// (a Phase 1 finding, back when a whole object was a single cache entry).
    /// Under the chunked cache (ADR-0015) every entry is one chunk
    /// (`chunk_size`) — never a whole object — so the disk-persistability
    /// ceiling is `chunk_size`, not the object length. `max_object_size` bounds
    /// only which objects are worth chunk-caching (a checkpoint shard is
    /// multi-GiB yet stored as 16-MiB chunks) and is therefore independent of
    /// the block size; the old rule clamped it down to `block_size` and thereby
    /// bypassed every object larger than one block — exactly the checkpoints
    /// the chunked cache exists to hold. Warn rather than error: both values
    /// are independently tunable.
    fn warn_unpersistable_chunks(self) -> Self {
        let block = self.cache.block_size as u64;
        let chunk = self.chunk.chunk_size();
        if chunk > block {
            tracing::warn!(
                chunk_size = chunk,
                block_size = block,
                "{} exceeds {}; chunks larger than a block can never persist to disk",
                CHUNK_SIZE.name,
                BLOCK_SIZE.name,
            );
        }
        self
    }
}

/// Resolve the client-memory delivery knobs (ADR-0026). Every ceiling is
/// "empty → the built-in default", the same idiom as the arena's byte knob, so a
/// deployment that sets only `enabled` gets the documented defaults rather than
/// zeros (a zero ceiling would refuse every target and look like a bug).
///
/// # Errors
///
/// A ceiling that does not parse as a byte size.
/// Resolve the ADR-0032 scatter knobs from the `scatter:` file block and env.
///
/// # Errors
///
/// Enabling the scatter on an Express backend is refused rather than downgraded:
/// ADR-0032 § 6 scopes the design to general-purpose buckets, so a ConfigMap
/// asking for it on a directory bucket is asking for something that cannot
/// happen, and silently proxying instead would leave an operator believing the
/// path was live. Any unparseable size or duration also fails startup.
fn scatter_config(
    file: &FileConfig,
    env: EnvFn,
    backend_type: BackendType,
) -> anyhow::Result<crate::scatter::ScatterConfig> {
    use crate::scatter::{
        ScatterConfig, DEFAULT_MIN_SCATTER_BYTES, DEFAULT_SATURATED_COOLDOWN_SECS,
        DEFAULT_STAGING_BYTES, DEFAULT_STAGING_TTL_SECS, DEFAULT_WINDOWS_IN_FLIGHT,
    };
    let fs = file.scatter.as_ref();
    // Three states, not two (see `SCATTER_ENABLED`): an explicit value wins in either
    // direction, and *unset* means on where the design applies — a general-purpose
    // backend — and off on Express. An explicitly truthy value on Express still fails
    // below, because the derived default can never produce that combination, so reaching
    // it means someone asked for it and must be told rather than downgraded.
    //
    // Only an EMPTY value derives. A non-empty value that is not truthy is off, typos
    // included, which is the direction that cannot surprise a client: a mistyped knob may
    // cost the speedup, never turn the composite ETag on for someone who did not ask.
    let requested =
        SCATTER_ENABLED.resolve(env, fs.and_then(|s| s.enabled).map(|on| on.to_string()));
    let requested = requested.trim();
    let enabled = if requested.is_empty() {
        !backend_type.is_express()
    } else {
        matches!(requested, "1" | "true" | "on" | "yes")
    };
    if enabled && backend_type.is_express() {
        anyhow::bail!(
            "{} is set but the backend is an Express directory bucket; the write scatter \
             is scoped to general-purpose buckets (ADR-0032 § 6). Unset it, or set the \
             backend type to `standard`.",
            SCATTER_ENABLED.name,
        );
    }
    let bytes_or = |var: &EnvVar, from_file: Option<String>, default: u64| -> anyhow::Result<u64> {
        match var.resolve_opt(env, from_file) {
            Some(raw) => parse_bytes(&raw),
            None => Ok(default),
        }
    };
    // Seconds rather than a byte string, and `0` means "use the default" — the
    // same convention `resolve_num_or` gives the usize knobs, spelled out here
    // because a Duration cannot go through it.
    let secs_or = |var: &EnvVar,
                   from_file: Option<u64>,
                   default: u64|
     -> anyhow::Result<std::time::Duration> {
        let raw = var.resolve(env, from_file.map(|n| n.to_string()));
        let trimmed = raw.trim();
        let secs = if trimmed.is_empty() {
            default
        } else {
            trimmed.parse::<u64>().map_err(|e| {
                anyhow::anyhow!("{} must be a whole number of seconds: {e}", var.name)
            })?
        };
        Ok(std::time::Duration::from_secs(if secs == 0 {
            default
        } else {
            secs
        }))
    };
    Ok(ScatterConfig {
        enabled,
        staging_bytes: bytes_or(
            &SCATTER_STAGING_BYTES,
            fs.and_then(|s| s.staging_bytes.clone()),
            DEFAULT_STAGING_BYTES,
        )?,
        staging_ttl: secs_or(
            &SCATTER_STAGING_TTL_SECS,
            fs.and_then(|s| s.staging_ttl_secs),
            DEFAULT_STAGING_TTL_SECS,
        )?,
        windows_in_flight: resolve_num_or(
            &SCATTER_WINDOWS_IN_FLIGHT
                .resolve(env, fs.and_then(|s| s.windows_in_flight).map(num_to_string)),
            DEFAULT_WINDOWS_IN_FLIGHT,
        )?,
        min_object_bytes: bytes_or(
            &SCATTER_MIN_OBJECT_BYTES,
            fs.and_then(|s| s.min_object_bytes.clone()),
            DEFAULT_MIN_SCATTER_BYTES,
        )?,
        saturated_cooldown: secs_or(
            &SCATTER_SATURATED_COOLDOWN_SECS,
            fs.and_then(|s| s.saturated_cooldown_secs),
            DEFAULT_SATURATED_COOLDOWN_SECS,
        )?,
    })
}

fn delivery_config(
    file: &FileConfig,
    env: EnvFn,
) -> anyhow::Result<crate::delivery::DeliveryConfig> {
    use crate::delivery::{
        DeliveryConfig, DEFAULT_DELIVERY_PARALLELISM, DEFAULT_MAX_TARGET_BYTES,
        DEFAULT_PINNED_BYTES_MAX,
    };
    let fd = file.delivery.as_ref();
    let bytes_or = |var: &EnvVar, from_file: Option<String>, default: u64| -> anyhow::Result<u64> {
        match var.resolve_opt(env, from_file) {
            Some(raw) => parse_bytes(&raw),
            None => Ok(default),
        }
    };
    Ok(DeliveryConfig {
        // Anything other than an explicitly truthy value leaves delivery OFF:
        // the surface is opt-in, so a typo must not enable it (the mirror image
        // of `RDMA_AFFINITY`, where a typo must not *disable* the default).
        enabled: matches!(
            DELIVERY_ENABLED
                .resolve(env, fd.and_then(|d| d.enabled).map(|on| on.to_string()))
                .trim(),
            "1" | "true" | "on" | "yes"
        ),
        shm_dir: DELIVERY_SHM_DIR
            .resolve_opt(env, fd.and_then(|d| d.shm_dir.clone()))
            .map_or_else(
                || std::path::PathBuf::from(crate::delivery::DEFAULT_SHM_DIR),
                std::path::PathBuf::from,
            ),
        max_target_bytes: bytes_or(
            &DELIVERY_MAX_TARGET_BYTES,
            fd.and_then(|d| d.max_target_bytes.clone()),
            DEFAULT_MAX_TARGET_BYTES,
        )?,
        pinned_bytes_max: bytes_or(
            &DELIVERY_PINNED_BYTES_MAX,
            fd.and_then(|d| d.pinned_bytes_max.clone()),
            DEFAULT_PINNED_BYTES_MAX,
        )?,
        parallelism: resolve_num_or(
            &DELIVERY_PARALLELISM.resolve(env, fd.and_then(|d| d.parallelism).map(num_to_string)),
            DEFAULT_DELIVERY_PARALLELISM,
        )?,
        // Same truthiness rule as `enabled` above, for the same reason: this changes
        // which node's NIC writes a client's memory, so a typo must not turn it on.
        remote_write: matches!(
            DELIVERY_REMOTE_WRITE
                .resolve(
                    env,
                    fd.and_then(|d| d.remote_write).map(|on| on.to_string())
                )
                .trim(),
            "1" | "true" | "on" | "yes"
        ),
    })
}

/// Resolve the hybrid-cache tier sizing and placement.
///
/// Extracted from [`resolve`] purely for length — that function is at its budget
/// and this was a fifth of it — so it is a straight lift with no behaviour change.
///
/// # Errors
///
/// Any size that does not parse, or an unknown `io-engine`.
fn cache_config(file: &FileConfig, env: EnvFn) -> anyhow::Result<CacheConfig> {
    let cache = file.cache.as_ref();
    Ok(CacheConfig {
        dir: CACHE_DIR
            .resolve(env, cache.and_then(|c| c.dir.clone()))
            .into(),
        mem_capacity: parse_bytes(
            &MEM_CAPACITY.resolve(env, cache.and_then(|c| c.mem_capacity.clone())),
        )? as usize,
        disk_capacity: parse_bytes(
            &DISK_CAPACITY.resolve(env, cache.and_then(|c| c.disk_capacity.clone())),
        )? as usize,
        block_size: parse_bytes(&BLOCK_SIZE.resolve(env, cache.and_then(|c| c.block_size.clone())))?
            as usize,
        flush_buffer_size: parse_bytes(
            &FLUSH_BUFFER_SIZE.resolve(env, cache.and_then(|c| c.flush_buffer_size.clone())),
        )? as usize,
        io_engine: IO_ENGINE
            .resolve(env, cache.and_then(|c| c.io_engine.clone()))
            .parse()?,
        uring: uring_config(file, env)?,
        tuning: storage_tuning(file, env)?,
        // The legacy pre-chunking `ObjectCache` (`pacer_cache::CachedObject`)
        // that this bounds is not built by this daemon at all (ADR-0015's
        // chunked `ChunkCache`/`ChunkTier` is the only cache this binary
        // constructs) — `None` takes `pacer_cache::DEFAULT_MAX_OBJECT_BYTES`,
        // which is the crate-level fix for R7. No env var surfaces an override
        // here because there is no live caller to tune yet; add one
        // (`PACER_MAX_LEGACY_OBJECT_BYTES`, following this module's
        // env-var-is-a-contract rule) if that path is ever revived.
        max_object_bytes: None,
    })
}

/// Resolve the S3 listener's bounds (ADR-0036). `0`/absent at every layer means
/// the built-in default for that knob, the same convention the numeric knobs
/// elsewhere use, so a benchmark arm can pin one and leave the other two alone.
///
/// # Errors
///
/// A count or a second count that does not parse as a whole number.
fn listen_limits(file: &FileConfig, env: EnvFn) -> anyhow::Result<crate::listen::ListenLimits> {
    use crate::listen::ListenLimits;
    let fl = file.listen.as_ref();
    Ok(ListenLimits {
        max_connections: resolve_num_or(
            &S3_MAX_CONNECTIONS.resolve(env, fl.and_then(|l| l.max_connections).map(num_to_string)),
            ListenLimits::default_max_connections(),
        )?,
        header_timeout: resolve_secs_or(
            &S3_HEADER_TIMEOUT,
            env,
            fl.and_then(|l| l.header_timeout_secs),
            ListenLimits::default_header_timeout_secs(),
        )?,
        idle_timeout: resolve_secs_or(
            &S3_IDLE_TIMEOUT,
            env,
            fl.and_then(|l| l.idle_timeout_secs),
            ListenLimits::default_idle_timeout_secs(),
        )?,
    })
}

/// Resolve the post-SIGTERM drain deadline (ADR-0036).
///
/// # Errors
///
/// A value that is not a whole number of seconds.
fn drain_timeout(file: &FileConfig, env: EnvFn) -> anyhow::Result<std::time::Duration> {
    resolve_secs_or(
        &SHUTDOWN_DRAIN_TIMEOUT,
        env,
        file.shutdown.as_ref().and_then(|s| s.drain_timeout_secs),
        crate::shutdown::DEFAULT_DRAIN_TIMEOUT_SECS,
    )
}

/// Resolve a whole-seconds knob into a [`std::time::Duration`], where empty or
/// `0` at every layer means `default`.
///
/// The same convention [`resolve_num_or`] gives the count knobs; spelled
/// separately because a `Duration` cannot go through it. `scatter_config` has a
/// closure of this shape inlined for its own two knobs — that one stays where it
/// is rather than being replaced here, so this change touches no ADR-0032 path.
///
/// # Errors
///
/// A value that is not a whole number.
fn resolve_secs_or(
    var: &EnvVar,
    env: EnvFn,
    from_file: Option<u64>,
    default: u64,
) -> anyhow::Result<std::time::Duration> {
    let raw = var.resolve(env, from_file.map(|n| n.to_string()));
    let trimmed = raw.trim();
    let secs = if trimmed.is_empty() {
        default
    } else {
        trimmed
            .parse::<u64>()
            .map_err(|e| anyhow::anyhow!("{} must be a whole number of seconds: {e}", var.name))?
    };
    Ok(std::time::Duration::from_secs(if secs == 0 {
        default
    } else {
        secs
    }))
}

/// Resolve the io_uring tuning; `0` at any layer means "keep the built-in
/// default" (represented by [`UringConfig::default`]).
fn uring_config(file: &FileConfig, env: EnvFn) -> anyhow::Result<UringConfig> {
    let file_uring = file.cache.as_ref().and_then(|c| c.uring.as_ref());
    let threads = parse_num(
        &URING_THREADS.resolve(env, file_uring.and_then(|u| u.threads).map(num_to_string)),
    )?;
    let io_depth = parse_num(
        &URING_IO_DEPTH.resolve(env, file_uring.and_then(|u| u.io_depth).map(num_to_string)),
    )?;
    let d = UringConfig::default();
    Ok(UringConfig {
        threads: if threads == 0 { d.threads } else { threads },
        io_depth: if io_depth == 0 { d.io_depth } else { io_depth },
    })
}

/// Resolve the foyer storage-engine tuning. Unlike [`uring_config`], `0` is
/// carried through rather than replaced: [`pacer_cache::StorageTuning`] treats it
/// as "keep foyer's own default", so the zero value is the untuned behaviour and
/// each knob can be moved on its own in a benchmark arm.
///
/// # Errors
///
/// A `submit-queue-threshold` that does not parse as a byte size, or a thread
/// count that does not parse as a number.
fn storage_tuning(file: &FileConfig, env: EnvFn) -> anyhow::Result<pacer_cache::StorageTuning> {
    let t = file.cache.as_ref().and_then(|c| c.tuning.as_ref());
    Ok(pacer_cache::StorageTuning {
        flushers: parse_num(
            &CACHE_FLUSHERS.resolve(env, t.and_then(|t| t.flushers).map(num_to_string)),
        )?,
        reclaimers: parse_num(
            &CACHE_RECLAIMERS.resolve(env, t.and_then(|t| t.reclaimers).map(num_to_string)),
        )?,
        submit_queue_threshold: parse_bytes(
            &SUBMIT_QUEUE_THRESHOLD.resolve(env, t.and_then(|t| t.submit_queue_threshold.clone())),
        )? as usize,
        storage_runtime_threads: parse_num(&STORAGE_RUNTIME_THREADS.resolve(
            env,
            t.and_then(|t| t.storage_runtime_threads).map(num_to_string),
        ))?,
    })
}

/// Resolve the chunk size (ADR-0015): empty at every layer → the built-in
/// [`pacer_cache::chunk::DEFAULT_CHUNK_SIZE`]; otherwise the parsed byte size.
///
/// # Errors
///
/// A non-empty value that does not parse as a byte size, or a zero size (a
/// zero-sized chunk has no valid index math — [`ChunkConfig::new`] would panic,
/// so reject it here with a message instead).
fn chunk_config(file: &FileConfig, env: EnvFn) -> anyhow::Result<pacer_cache::chunk::ChunkConfig> {
    use pacer_cache::chunk::{ChunkConfig, DEFAULT_CHUNK_SIZE};
    let raw = CHUNK_SIZE.resolve(env, policy_field(file, |p| p.chunk_size.clone()));
    let size = if raw.trim().is_empty() {
        DEFAULT_CHUNK_SIZE
    } else {
        parse_bytes(&raw)?
    };
    if size == 0 {
        anyhow::bail!("{} must be a positive size", CHUNK_SIZE.name);
    }
    Ok(ChunkConfig::new(size))
}

/// Default concurrent chunk resolutions per client GET (ADR-0015). 8 keeps a
/// multi-chunk read's look-ahead memory at `8 × chunk_size` (128 MiB at the
/// 16 MiB default) while overlapping fetches; benchmark-tuned under the storm.
const DEFAULT_FILL_PARALLELISM: usize = 8;

/// Default replication factor (ADR-0016 knobs): 2 co-homes per chunk — the
/// floor that removes the single-point-of-departure hotspot without paying more
/// than 2× storage. Raise only with B4 storm-benchmark evidence.
const DEFAULT_REPLICATION_R: usize = 2;
/// Default requester-local admission threshold (ADR-0016 knobs): admit a
/// peer-owned chunk on the 2nd fetch in the window (the re-read workload
/// assumption; a pure storm reads each chunk once and leans on layer 2).
const DEFAULT_LOCAL_ADMISSION_THRESHOLD: u32 = 2;
/// Default admission window (ADR-0016 knobs): 60 s bounds how long a chunk's
/// prior fetches count toward its heat.
const DEFAULT_LOCAL_ADMISSION_WINDOW_SECS: u64 = 60;
/// Default requester-local copy capacity (ADR-0016 knobs, expressed as a
/// percent): 25 % of cache capacity, so storm heat cannot evict a node's own
/// homed chunks wholesale.
const DEFAULT_LOCAL_COPY_CAPACITY_PERCENT: usize = 25;
/// Denominator for the percent → fraction conversion of
/// `local_copy_capacity_percent`.
const PERCENT_DENOMINATOR: f64 = 100.0;
/// Default re-announce sweep interval (ADR-0017 healer): 300 s. A restarted
/// home's directory is soft state repopulated from holder re-announces; five
/// minutes bounds the stale-directory window while keeping the periodic
/// control-plane fan-out negligible against read traffic.
const DEFAULT_REANNOUNCE_INTERVAL_SECS: u64 = 300;

/// Resolve `fill_parallelism`; `0` at any layer means the built-in default.
///
/// # Errors
///
/// A value that does not parse as a non-negative integer.
fn fill_parallelism(file: &FileConfig, env: EnvFn) -> anyhow::Result<usize> {
    let n =
        parse_num(&FILL_PARALLELISM.resolve(env, policy_field_num(file, |p| p.fill_parallelism)))?;
    Ok(if n == 0 { DEFAULT_FILL_PARALLELISM } else { n })
}

/// Resolve the backend shape (ADR-0023): env wins, then the file's
/// `backend.backend-type`, then the `express` default. Parsed strictly.
///
/// # Errors
///
/// A value that is neither `express` nor `standard` — fail fast rather than
/// silently defaulting, since the two behave differently on the write path.
fn backend_type(file: &FileConfig, env: EnvFn) -> anyhow::Result<BackendType> {
    let raw = BACKEND_TYPE.resolve(
        env,
        file.backend.as_ref().and_then(|b| b.backend_type.clone()),
    );
    raw.parse::<BackendType>()
        .map_err(|e| anyhow::anyhow!("{}: {e}", BACKEND_TYPE.name))
}

/// `store-read-concurrency`: the resolved ceiling, with `0` meaning the built-in default and
/// the word `unlimited` meaning no ceiling at all.
///
/// Two spellings for "no limit" would be one too many, so the *number* `0` is NOT it: `0` is
/// how every other numeric knob here says "use the default", and a config that said `0` to
/// mean unlimited would silently uncap a node whose author meant the opposite. `unlimited` is
/// the pre-ceiling behaviour and has to be asked for by name.
///
/// # Errors
///
/// A value that is neither `unlimited` nor a non-negative integer.
fn store_read_concurrency(file: &FileConfig, env: EnvFn) -> anyhow::Result<usize> {
    let raw = STORE_READ_CONCURRENCY.resolve(
        env,
        policy_field(file, |p| p.store_read_concurrency.clone()),
    );
    if raw.trim() == "unlimited" {
        return Ok(0);
    }
    let n = parse_num(&raw)?;
    Ok(if n == 0 {
        pacer_cache::store::DEFAULT_READ_CONCURRENCY
    } else {
        n
    })
}

/// `verify-chunk-body`: env (`"true"`) wins, then the file bool, then `false`.
///
/// Same shape as [`force_path_style`] rather than a `parse::<bool>()`, so a typo is
/// "off" consistently with every other boolean here instead of a startup failure in one
/// of them and not the others.
fn verify_chunk_body(file: &FileConfig, env: EnvFn) -> bool {
    // Reached directly rather than through `policy_field`, which is String-shaped.
    let file_val = file
        .policy
        .as_ref()
        .and_then(|p| p.verify_chunk_body)
        .map(|b| b.to_string());
    VERIFY_CHUNK_BODY.resolve(env, file_val) == "true"
}

/// `conditional-get-from-cache`: env wins, then the file bool, then `true` (ADR-0039).
///
/// Compared against `"true"` like every other boolean here, which means a TYPO reads as
/// `false`. That is the right direction for this one specifically: `false` is the strict
/// passthrough every conditional GET took before ADR-0039, so a misspelling loses the
/// optimisation rather than silently keeping a semantic deviation the operator was trying
/// to turn off.
fn conditional_get_from_cache(file: &FileConfig, env: EnvFn) -> bool {
    let file_val = file
        .policy
        .as_ref()
        .and_then(|p| p.conditional_get_from_cache)
        .map(|b| b.to_string());
    CONDITIONAL_GET_FROM_CACHE.resolve(env, file_val) == "true"
}

/// `force-path-style`: env (`"true"`) wins, then the file bool, then `false`.
fn force_path_style(file: &FileConfig, env: EnvFn) -> bool {
    let file_val = file
        .backend
        .as_ref()
        .and_then(|b| b.force_path_style)
        .map(|b| b.to_string());
    FORCE_PATH_STYLE.resolve(env, file_val) == "true"
}

/// Bucket map: env flat form (`alias=real,…`) wins, then the file's native
/// map, then empty.
fn bucket_map(file: &FileConfig, env: EnvFn) -> HashMap<String, String> {
    if let Some(flat) = BUCKET_MAP.resolve_opt(env, None) {
        return parse_bucket_map(&flat);
    }
    file.bucket_map.clone().unwrap_or_default()
}

/// Read one `policy:` string field, or `None` when the block is absent.
fn policy_field(file: &FileConfig, f: impl Fn(&FilePolicy) -> Option<String>) -> Option<String> {
    file.policy.as_ref().and_then(f)
}

/// Read one `policy:` numeric field as a string (so it flows through
/// [`EnvVar::resolve`] alongside the env layer), or `None` when absent.
fn policy_field_num(file: &FileConfig, f: impl Fn(&FilePolicy) -> Option<usize>) -> Option<String> {
    file.policy.as_ref().and_then(f).map(num_to_string)
}

/// Read one `auth:` string field, or `None` when the block is absent.
fn auth_field(file: &FileConfig, f: impl Fn(&FileAuth) -> Option<String>) -> Option<String> {
    file.auth.as_ref().and_then(f)
}

/// Build the cluster config. Cluster mode is on iff `PACER_NODE_NAME` is set
/// (the chart sets it via the downward API); the file only supplies the
/// release-wide peer address and channel capacity.
///
/// # Errors
///
/// K8s membership without a peer Service name.
fn cluster_config(file: &FileConfig, env: EnvFn) -> anyhow::Result<Option<ClusterConfig>> {
    let Some(node_name) = NODE_NAME.resolve_opt(env, None) else {
        return Ok(None);
    };
    let fc = file.cluster.as_ref();
    let peer_listen_addr =
        PEER_LISTEN_ADDR.resolve(env, fc.and_then(|c| c.peer_listen_addr.clone()));
    let peer_port: u16 = peer_listen_addr
        .rsplit_once(':')
        .map(|(_, p)| p.parse())
        .transpose()?
        .ok_or_else(|| anyhow::anyhow!("{} missing port", PEER_LISTEN_ADDR.name))?;
    let channel_capacity = parse_num(
        &PEER_CHANNEL_CAPACITY.resolve(env, fc.and_then(|c| c.channel_capacity).map(num_to_string)),
    )?;
    let max_sharers_tracked = parse_num(&MAX_SHARERS_TRACKED.resolve(
        env,
        fc.and_then(|c| c.max_sharers_tracked).map(num_to_string),
    ))?;
    let max_sharers_tracked = if max_sharers_tracked == 0 {
        pacer_ring::directory::DEFAULT_MAX_SHARERS_TRACKED
    } else {
        max_sharers_tracked
    };
    let repl = replication_config(fc, env, max_sharers_tracked)?;
    let arena = rdma_arena_knobs(fc, env)?;
    let efa_rails =
        parse_num(&EFA_RAILS.resolve(env, fc.and_then(|c| c.efa_rails).map(num_to_string)))?;
    let efa_qps_per_rail = parse_num(
        &EFA_QPS_PER_RAIL.resolve(env, fc.and_then(|c| c.efa_qps_per_rail).map(num_to_string)),
    )?;
    let reannounce_interval = std::time::Duration::from_secs(resolve_num_or(
        &REANNOUNCE_INTERVAL_SECS.resolve(
            env,
            fc.and_then(|c| c.reannounce_interval_secs)
                .map(num_to_string),
        ),
        DEFAULT_REANNOUNCE_INTERVAL_SECS as usize,
    )? as u64);
    let membership = match PEERS.resolve_opt(env, None) {
        Some(peers) => Membership::Static {
            peers: pacer_ring::membership::parse_static_peers(&peers),
        },
        None => Membership::K8s {
            namespace: NAMESPACE.resolve(env, None),
            service: PEER_SERVICE.resolve_opt(env, None).ok_or_else(|| {
                anyhow::anyhow!(
                    "cluster mode ({} set) needs {} or {}",
                    NODE_NAME.name,
                    PEERS.name,
                    PEER_SERVICE.name,
                )
            })?,
        },
    };
    Ok(Some(ClusterConfig {
        node_name,
        peer_listen_addr,
        peer_port,
        channel_capacity,
        h2_windows: h2_windows(fc, env)?,
        peer_connections: peer_connections(fc, env)?,
        max_sharers_tracked,
        replication_r: repl.replication_r,
        local_admission_threshold: repl.local_admission_threshold,
        local_admission_window: repl.local_admission_window,
        local_copy_capacity_fraction: repl.local_copy_capacity_fraction,
        rdma_arena_bytes: arena.bytes,
        cache_slab_bytes: arena.slab_bytes,
        rdma_arena_page_mib: arena.page_mib,
        rdma_affinity: arena.affinity,
        rdma_rail_window: arena.rail_window,
        efa_rails,
        efa_qps_per_rail,
        reannounce_interval,
        membership,
    }))
}

/// The four ADR-0016 replication knobs, resolved together.
struct ReplicationConfig {
    replication_r: usize,
    local_admission_threshold: u32,
    local_admission_window: std::time::Duration,
    local_copy_capacity_fraction: f64,
}

/// Resolve the ADR-0016 replication knobs from the `cluster:` file block and
/// env. `max_sharers_tracked` bounds `replication_r` (ADR-0017: the directory
/// cap must stay ≥ R, so it can list every home).
///
/// # Errors
///
/// A knob whose value does not parse as a non-negative integer.
fn replication_config(
    fc: Option<&FileCluster>,
    env: EnvFn,
    max_sharers_tracked: usize,
) -> anyhow::Result<ReplicationConfig> {
    let replication_r = resolve_num_or(
        &REPLICATION_R.resolve(env, fc.and_then(|c| c.replication_r).map(num_to_string)),
        DEFAULT_REPLICATION_R,
    )?
    // The directory cannot list more homes than it tracks; clamp rather than
    // error (both are independently tunable) and floor at 1 (single-copy).
    .min(max_sharers_tracked)
    .max(1);
    let local_admission_threshold = resolve_num_or(
        &LOCAL_ADMISSION_THRESHOLD.resolve(
            env,
            fc.and_then(|c| c.local_admission_threshold)
                .map(num_to_string),
        ),
        DEFAULT_LOCAL_ADMISSION_THRESHOLD as usize,
    )? as u32;
    let local_admission_window = std::time::Duration::from_secs(resolve_num_or(
        &LOCAL_ADMISSION_WINDOW_SECS.resolve(
            env,
            fc.and_then(|c| c.local_admission_window_secs)
                .map(num_to_string),
        ),
        DEFAULT_LOCAL_ADMISSION_WINDOW_SECS as usize,
    )? as u64);
    let local_copy_capacity_percent = resolve_num_or(
        &LOCAL_COPY_CAPACITY_PERCENT.resolve(
            env,
            fc.and_then(|c| c.local_copy_capacity_percent)
                .map(num_to_string),
        ),
        DEFAULT_LOCAL_COPY_CAPACITY_PERCENT,
    )?;
    Ok(ReplicationConfig {
        replication_r,
        local_admission_threshold,
        local_admission_window,
        local_copy_capacity_fraction: local_copy_capacity_percent as f64 / PERCENT_DENOMINATOR,
    })
}

/// The two ADR-0024 RDMA-arena knobs, resolved together.
struct RdmaArenaKnobs {
    /// Requester arena bytes; `0` = the transport's default.
    bytes: usize,
    /// Requested page size in MiB; `0` = base pages.
    page_mib: usize,
    /// Place rails on their NIC's NUMA node (planning/19 D5); `false` = pre-D5.
    affinity: bool,
    /// Per-rail in-flight WRITE cap; `0` = unbounded.
    rail_window: usize,
    /// ADR-0028 cache-slab bytes; `0` = no slab.
    slab_bytes: usize,
}

/// Resolve the RDMA-arena knobs from the `cluster:` file block and env, and
/// reject the retired slot-count knob.
///
/// The rejection is the point of doing this in one place: `PACER_RDMA_ARENA_BYTES`
/// and `PACER_RDMA_REQUESTER_SLOTS` configure the same thing in different units,
/// so a deployment carrying the old name would boot with the *default* arena and
/// look, from the outside, exactly like the undeclared buffer cap ADR-0024 exists
/// to remove.
///
/// # Errors
///
/// The retired `PACER_RDMA_REQUESTER_SLOTS` being set, either byte size failing
/// to parse (`4GiB`, `37GiB`, a plain count — the arena's or the slab's), or a
/// page size that is not a number.
fn rdma_arena_knobs(fc: Option<&FileCluster>, env: EnvFn) -> anyhow::Result<RdmaArenaKnobs> {
    if let Some(slots) = env(LEGACY_RDMA_REQUESTER_SLOTS).filter(|s| !s.is_empty() && s != "0") {
        let hint = match parse_num(&slots) {
            Ok(n) => format!(
                " — the old {slots} slots pinned {} bytes",
                n * LEGACY_SLOT_BYTES
            ),
            Err(_) => String::new(),
        };
        return Err(anyhow::anyhow!(
            "{LEGACY_RDMA_REQUESTER_SLOTS} was retired with the fixed slot pool (ADR-0024); \
             set {} instead{hint}",
            RDMA_ARENA_BYTES.name,
        ));
    }
    let bytes = match RDMA_ARENA_BYTES.resolve_opt(env, fc.and_then(|c| c.rdma_arena_bytes.clone()))
    {
        Some(raw) => parse_bytes(&raw)? as usize,
        None => 0,
    };
    let slab_bytes =
        match CACHE_SLAB_BYTES.resolve_opt(env, fc.and_then(|c| c.cache_slab_bytes.clone())) {
            Some(raw) => parse_bytes(&raw)? as usize,
            None => 0,
        };
    Ok(RdmaArenaKnobs {
        bytes,
        slab_bytes,
        page_mib: parse_num(&RDMA_ARENA_PAGE_MIB.resolve(
            env,
            fc.and_then(|c| c.rdma_arena_page_mib).map(num_to_string),
        ))?,
        // Anything other than an explicit falsey value keeps placement ON: the
        // default is the discipline planning/18 measured with, and a typo should
        // not silently select the control arm.
        affinity: !matches!(
            RDMA_AFFINITY
                .resolve(
                    env,
                    fc.and_then(|c| c.rdma_affinity).map(|on| on.to_string()),
                )
                .trim(),
            "0" | "false" | "off" | "no"
        ),
        rail_window: parse_num(
            &RDMA_RAIL_WINDOW.resolve(env, fc.and_then(|c| c.rdma_rail_window).map(num_to_string)),
        )?,
    })
}

/// Parse a count knob, substituting `default` when the resolved value is `0`
/// (the "keep the built-in default" idiom shared by the numeric cluster knobs).
///
/// # Errors
///
/// A value that does not parse as a non-negative integer.
fn resolve_num_or(raw: &str, default: usize) -> anyhow::Result<usize> {
    let n = parse_num(raw)?;
    Ok(if n == 0 { default } else { n })
}

/// Parse "alias=real-bucket,alias2=real2" into a map.
fn parse_bucket_map(s: &str) -> HashMap<String, String> {
    s.split(',')
        .filter_map(|pair| {
            let (alias, real) = pair.split_once('=')?;
            let (alias, real) = (alias.trim(), real.trim());
            (!alias.is_empty() && !real.is_empty()).then(|| (alias.to_owned(), real.to_owned()))
        })
        .collect()
}

/// Parse `4MiB` / `100GiB` / `8Gi` / plain byte counts.
///
/// Both spellings are accepted on purpose. This crate's own knobs are documented
/// as `GiB`, but the Helm chart also carries **Kubernetes quantities** (`8Gi`) for
/// the resource fields beside them, and the two conventions sit in the same
/// `values.yaml` — so a value copied from one line to another produced a *startup
/// crash* (`invalid digit found in string`) rather than a warning. Accepting `Gi`
/// costs nothing and removes a class of deploy-time failure that only shows up on
/// the node.
fn parse_bytes(s: &str) -> anyhow::Result<u64> {
    let s = s.trim();
    // Longest suffix first: `GiB` must be tried before `Gi`, or `8GiB` would parse
    // as the number `8G`.
    const UNITS: [(&str, u64); 6] = [
        ("GiB", 1 << 30),
        ("MiB", 1 << 20),
        ("KiB", 1 << 10),
        ("Gi", 1 << 30),
        ("Mi", 1 << 20),
        ("Ki", 1 << 10),
    ];
    for (suffix, mult) in UNITS {
        if let Some(num) = s.strip_suffix(suffix) {
            return Ok(num.trim().parse::<u64>()? * mult);
        }
    }
    Ok(s.parse::<u64>()?)
}

/// Parse a plain count (thread/depth/capacity knobs).
fn parse_num(s: &str) -> anyhow::Result<usize> {
    Ok(s.trim().parse::<usize>()?)
}

/// The peer plane's HTTP/2 receive windows, from both configuration layers.
///
/// Its own function purely for length: [`cluster_config`] is at its budget, and the two knobs
/// belong to one another anyway — a stream window without a connection window to carry
/// several of them concurrently is half a setting.
///
/// # Errors
///
/// Either value being unparseable or past HTTP/2's maximum — see [`h2_window`].
fn h2_windows(fc: Option<&FileCluster>, env: EnvFn) -> anyhow::Result<pacer_transport::H2Windows> {
    Ok(pacer_transport::H2Windows {
        stream_bytes: h2_window(
            &PEER_H2_STREAM_WINDOW,
            env,
            fc.and_then(|c| c.h2_stream_window.clone()),
        )?,
        connection_bytes: h2_window(
            &PEER_H2_CONNECTION_WINDOW,
            env,
            fc.and_then(|c| c.h2_connection_window.clone()),
        )?,
    })
}

/// How many connections each peer gets, from both configuration layers.
///
/// `0` (the shipped default) means the transport's own
/// [`pacer_transport::DEFAULT_PEER_CONNECTIONS`], the same "`0` → the built-in" convention
/// `max_sharers_tracked` and `replication_r` use, so the number lives in one place.
///
/// # Errors
///
/// An unparseable count, or one past [`pacer_transport::MAX_PEER_CONNECTIONS`]. Refused here
/// rather than clamped silently: a pool width is what an arm is *named after*, and a value
/// quietly reduced would publish one width while measuring another.
fn peer_connections(fc: Option<&FileCluster>, env: EnvFn) -> anyhow::Result<usize> {
    let raw = PEER_CONNECTIONS.resolve(env, fc.and_then(|c| c.peer_connections).map(num_to_string));
    let requested = parse_num(&raw)
        .map_err(|e| anyhow::anyhow!("{} is not a count: {raw:?} ({e})", PEER_CONNECTIONS.name))?;
    if requested == 0 {
        return Ok(pacer_transport::DEFAULT_PEER_CONNECTIONS);
    }
    anyhow::ensure!(
        requested <= pacer_transport::MAX_PEER_CONNECTIONS,
        "{} is {requested}, past the {} this daemon will open per peer — every connection is a \
         socket and an HTTP/2 state machine on both ends, times the peer count",
        PEER_CONNECTIONS.name,
        pacer_transport::MAX_PEER_CONNECTIONS,
    );
    Ok(requested)
}

/// Resolve one HTTP/2 window knob to bytes, `None` when unset.
///
/// # Errors
///
/// An unparseable size, or one past what HTTP/2 can carry — refused here rather than at the
/// first connection, where it would surface as a peer that cannot be dialed.
fn h2_window(var: &EnvVar, env: EnvFn, file: Option<String>) -> anyhow::Result<Option<u32>> {
    /// RFC 9113 § 6.9.1 caps a flow-control window at 2^31 - 1; a `SETTINGS` value above it
    /// is a connection-level `FLOW_CONTROL_ERROR`, so a larger number here is not a big
    /// window but a peer plane that never comes up.
    const H2_MAX_WINDOW_BYTES: u64 = (1u64 << 31) - 1;

    let Some(raw) = var.resolve_opt(env, file) else {
        return Ok(None);
    };
    let bytes = parse_bytes(&raw)
        .map_err(|e| anyhow::anyhow!("{} is not a byte size: {raw:?} ({e})", var.name))?;
    anyhow::ensure!(
        bytes <= H2_MAX_WINDOW_BYTES,
        "{} is {bytes} bytes, past HTTP/2's {H2_MAX_WINDOW_BYTES}-byte maximum window",
        var.name,
    );
    Ok(Some(u32::try_from(bytes)?))
}

/// Render a numeric file value so it can flow through [`EnvVar::resolve`]
/// alongside the string env layer.
fn num_to_string(n: usize) -> String {
    n.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An env lookup backed by a fixed list — no process-global state, so the
    /// precedence tests run in parallel safely.
    fn fake_env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |k: &str| map.get(k).cloned()
    }

    fn parse_file(yaml: &str) -> anyhow::Result<FileConfig> {
        Ok(serde_yaml_ng::from_str(yaml)?)
    }

    #[test]
    fn parses_suffixed_sizes() {
        assert_eq!(parse_bytes("4MiB").unwrap(), 4 << 20);
        assert_eq!(parse_bytes("100GiB").unwrap(), 100 << 30);
        assert_eq!(parse_bytes("512").unwrap(), 512);
        // Kubernetes quantities too: the chart carries both spellings in one
        // values.yaml, and rejecting `8Gi` crashed the daemon at startup rather
        // than warning. `GiB` must still win over `Gi` (suffix order).
        assert_eq!(parse_bytes("8Gi").unwrap(), 8 << 30);
        assert_eq!(parse_bytes("96Gi").unwrap(), 96 << 30);
        assert_eq!(parse_bytes("512Mi").unwrap(), 512 << 20);
        assert_eq!(parse_bytes("64Ki").unwrap(), 64 << 10);
        assert_eq!(parse_bytes(" 2GiB ").unwrap(), 2 << 30);
        // Still an error, not a silent 0.
        assert!(parse_bytes("8GB").is_err());
        assert!(parse_bytes("lots").is_err());
    }

    #[test]
    fn defaults_when_no_file_no_env() {
        let cfg = resolve(&FileConfig::default(), &fake_env(&[])).unwrap();
        assert_eq!(cfg.listen_addr, "0.0.0.0:9000");
        assert_eq!(cfg.cache.mem_capacity, 1 << 30);
        assert_eq!(cfg.worker_threads, 0);
        assert_eq!(cfg.rdma_worker_threads, 0);
        assert!(cfg.cluster.is_none());
        // 0 in the file/default means "keep the built-in uring default".
        assert_eq!(cfg.cache.uring, UringConfig::default());
        // No chunk-size set → the ADR-0015 default (16 MiB).
        assert_eq!(
            cfg.chunk.chunk_size(),
            pacer_cache::chunk::DEFAULT_CHUNK_SIZE
        );
    }

    #[test]
    fn chunk_size_layers_and_rejects_zero() {
        // File value applies...
        let file = parse_file("policy:\n  chunk-size: 8MiB\n").unwrap();
        let cfg = resolve(&file, &fake_env(&[])).unwrap();
        assert_eq!(cfg.chunk.chunk_size(), 8 << 20);
        // ...and env overrides it.
        let cfg = resolve(&file, &fake_env(&[("PACER_CHUNK_SIZE", "32MiB")])).unwrap();
        assert_eq!(cfg.chunk.chunk_size(), 32 << 20);
        // Zero is a config error, not a panic.
        assert!(resolve(
            &FileConfig::default(),
            &fake_env(&[("PACER_CHUNK_SIZE", "0")])
        )
        .is_err());
    }

    #[test]
    fn max_object_size_defaults_to_unbounded_and_ignores_block_size() {
        // Regression: the chunked cache (ADR-0015) stores an object as many
        // `chunk_size` entries, so a checkpoint shard of any size is cacheable
        // even though it dwarfs a foyer block. The old whole-object rule capped
        // the ceiling at 8 GiB and then clamped it down to `block_size` (1 GiB),
        // silently bypassing every object larger than one block — exactly the
        // checkpoints the cache exists to hold. Default: no cap at all.
        let cfg = resolve(&FileConfig::default(), &fake_env(&[])).unwrap();
        assert_eq!(cfg.max_object_size, None);
        assert_eq!(cfg.cache.block_size, 1 << 30);
        // An explicit operator cap is honored verbatim and NOT dragged down to
        // the block size.
        let cfg = resolve(
            &FileConfig::default(),
            &fake_env(&[
                ("PACER_BLOCK_SIZE", "512MiB"),
                ("PACER_MAX_OBJECT_SIZE", "16GiB"),
            ]),
        )
        .unwrap();
        assert_eq!(cfg.max_object_size, Some(16 << 30));
        // The real disk-persistability unit — one chunk — still fits a block.
        assert!(cfg.chunk.chunk_size() <= cfg.cache.block_size as u64);
    }

    /// The read-path tuning must default to "leave foyer alone" and then layer
    /// file-under-env like everything else. Worth a test of its own because the
    /// failure mode is silent: a benchmark arm whose knob never arrived measures
    /// its own control and looks like a null result.
    #[test]
    fn storage_tuning_defaults_to_zero_and_layers_file_under_env() {
        // Unset everywhere → all zero, which `pacer_cache` reads as "keep foyer's
        // built-in default", i.e. the daemon's historical behaviour.
        let cfg = resolve(&FileConfig::default(), &fake_env(&[])).unwrap();
        assert_eq!(cfg.cache.tuning, pacer_cache::StorageTuning::default());
        assert_eq!(cfg.cache.tuning.flushers, 0);

        // The chart's `cache.tuning:` block applies...
        let file = parse_file(concat!(
            "cache:\n",
            "  tuning:\n",
            "    flushers: 32\n",
            "    reclaimers: 8\n",
            "    submit-queue-threshold: 2GiB\n",
            "    storage-runtime-threads: 16\n",
        ))
        .unwrap();
        let cfg = resolve(&file, &fake_env(&[])).unwrap();
        assert_eq!(cfg.cache.tuning.flushers, 32);
        assert_eq!(cfg.cache.tuning.reclaimers, 8);
        assert_eq!(cfg.cache.tuning.submit_queue_threshold, 2 << 30);
        assert_eq!(cfg.cache.tuning.storage_runtime_threads, 16);

        // ...and env wins over it, per knob.
        let cfg = resolve(
            &file,
            &fake_env(&[
                ("PACER_CACHE_FLUSHERS", "4"),
                ("PACER_SUBMIT_QUEUE_THRESHOLD", "512MiB"),
            ]),
        )
        .unwrap();
        assert_eq!(cfg.cache.tuning.flushers, 4);
        assert_eq!(cfg.cache.tuning.submit_queue_threshold, 512 << 20);
        // Untouched knobs keep the file's values.
        assert_eq!(cfg.cache.tuning.reclaimers, 8);
        assert_eq!(cfg.cache.tuning.storage_runtime_threads, 16);
    }

    /// ADR-0033 must be opt-in from **every** layer: an untouched config, an empty
    /// string, and a file that never mentions the block all have to select `foyer`, or a
    /// rollout would move the disk tier under a cluster that did not ask for it. And an
    /// unknown value must fail startup rather than silently fall back — the two tiers
    /// have different integrity properties, so guessing between them is not acceptable.
    #[test]
    fn disk_tier_is_opt_in_and_body_verification_defaults_off() {
        let cfg = resolve(&FileConfig::default(), &fake_env(&[])).unwrap();
        assert_eq!(cfg.disk_tier, pacer_cache::DiskTier::Foyer);
        assert!(!cfg.verify_chunk_body);

        let file = parse_file("policy:\n  disk-tier: store\n  verify-chunk-body: true\n").unwrap();
        let cfg = resolve(&file, &fake_env(&[])).unwrap();
        assert_eq!(cfg.disk_tier, pacer_cache::DiskTier::Store);
        assert!(cfg.verify_chunk_body);

        // Env beats the file, both ways.
        let cfg = resolve(&file, &fake_env(&[("PACER_DISK_TIER", "foyer")])).unwrap();
        assert_eq!(cfg.disk_tier, pacer_cache::DiskTier::Foyer);
        let cfg = resolve(
            &FileConfig::default(),
            &fake_env(&[
                ("PACER_DISK_TIER", "store"),
                ("PACER_VERIFY_CHUNK_BODY", "true"),
            ]),
        )
        .unwrap();
        assert_eq!(cfg.disk_tier, pacer_cache::DiskTier::Store);
        assert!(cfg.verify_chunk_body);

        assert!(resolve(
            &FileConfig::default(),
            &fake_env(&[("PACER_DISK_TIER", "chunkstore")])
        )
        .is_err());
    }

    /// The read shape defaults to the shape every prior measurement used, so a config that
    /// says nothing about it is not silently on a new code path — and `overlap` resolves from
    /// both layers, because an arm that asks for it and gets `two-read` would report the
    /// baseline twice and read as a null.
    #[test]
    fn the_store_read_shape_defaults_to_two_read_and_takes_both_spellings() {
        use pacer_cache::store::ReadShape;
        let cfg = resolve(&FileConfig::default(), &fake_env(&[])).unwrap();
        assert_eq!(cfg.store_read_shape, ReadShape::TwoRead);

        let file = parse_file("policy:\n  store-read-shape: overlap\n").unwrap();
        assert_eq!(
            resolve(&file, &fake_env(&[])).unwrap().store_read_shape,
            ReadShape::Overlap
        );
        // Env beats the file, both ways.
        let cfg = resolve(&file, &fake_env(&[("PACER_STORE_READ_SHAPE", "two-read")])).unwrap();
        assert_eq!(cfg.store_read_shape, ReadShape::TwoRead);
        let cfg = resolve(
            &FileConfig::default(),
            &fake_env(&[("PACER_STORE_READ_SHAPE", "overlap")]),
        )
        .unwrap();
        assert_eq!(cfg.store_read_shape, ReadShape::Overlap);

        // A typo is a startup failure, not a silent baseline — the failure mode that would
        // make a whole paid arm a duplicate of its own control.
        assert!(resolve(
            &FileConfig::default(),
            &fake_env(&[("PACER_STORE_READ_SHAPE", "overlapped")])
        )
        .is_err());
    }

    /// The read ceiling is ON by default, and "no ceiling" has to be asked for by NAME.
    ///
    /// The distinction is the whole point: `0` means "use the measured default" the way every
    /// other numeric knob here does, so a config that meant to uncap a node and wrote `0`
    /// would get the ceiling instead of silently getting the old unbounded behaviour back.
    #[test]
    fn the_store_read_ceiling_defaults_on_and_uncaps_only_by_name() {
        let cfg = resolve(&FileConfig::default(), &fake_env(&[])).unwrap();
        assert_eq!(
            cfg.store_read_concurrency,
            pacer_cache::store::DEFAULT_READ_CONCURRENCY
        );

        // An explicit number wins.
        let cfg = resolve(
            &FileConfig::default(),
            &fake_env(&[("PACER_STORE_READ_CONCURRENCY", "24")]),
        )
        .unwrap();
        assert_eq!(cfg.store_read_concurrency, 24);

        // `0` is "the default", NOT "unlimited".
        let cfg = resolve(
            &FileConfig::default(),
            &fake_env(&[("PACER_STORE_READ_CONCURRENCY", "0")]),
        )
        .unwrap();
        assert_eq!(
            cfg.store_read_concurrency,
            pacer_cache::store::DEFAULT_READ_CONCURRENCY
        );

        // Only the word uncaps, and it resolves to the 0 the store reads as unlimited.
        let cfg = resolve(
            &FileConfig::default(),
            &fake_env(&[("PACER_STORE_READ_CONCURRENCY", "unlimited")]),
        )
        .unwrap();
        assert_eq!(cfg.store_read_concurrency, 0);

        // The file layer resolves too, and a typo is a startup failure.
        let file = parse_file("policy:\n  store-read-concurrency: 8\n").unwrap();
        assert_eq!(
            resolve(&file, &fake_env(&[]))
                .unwrap()
                .store_read_concurrency,
            8
        );
        assert!(resolve(
            &FileConfig::default(),
            &fake_env(&[("PACER_STORE_READ_CONCURRENCY", "none")])
        )
        .is_err());
    }

    #[test]
    fn promotion_defaults_to_foyers_behaviour_and_rejects_unknown() {
        // Default must be foyer's own promoting `get`: taking the promotion out of
        // the path is a workload choice (it gives up single-flight coalescing), so
        // it can never be what an unconfigured daemon does.
        let cfg = resolve(&FileConfig::default(), &fake_env(&[])).unwrap();
        assert_eq!(cfg.promotion, pacer_cache::Promotion::OnDiskHit);

        let file = parse_file("policy:\n  promotion: never\n").unwrap();
        let cfg = resolve(&file, &fake_env(&[])).unwrap();
        assert_eq!(cfg.promotion, pacer_cache::Promotion::Never);

        let cfg = resolve(
            &file,
            &fake_env(&[("PACER_CHUNK_PROMOTION", "on-disk-hit")]),
        )
        .unwrap();
        assert_eq!(cfg.promotion, pacer_cache::Promotion::OnDiskHit);

        // A typo fails startup rather than silently promoting.
        assert!(resolve(
            &FileConfig::default(),
            &fake_env(&[("PACER_CHUNK_PROMOTION", "sometimes")])
        )
        .is_err());
    }

    #[test]
    fn backend_type_defaults_layers_and_rejects_unknown() {
        // Unset everywhere → the ADR-0002/0023 default (Express), so existing
        // deployments keep their behavior.
        let cfg = resolve(&FileConfig::default(), &fake_env(&[])).unwrap();
        assert_eq!(cfg.backend.backend_type, BackendType::Express);

        // File value applies...
        let file = parse_file("backend:\n  backend-type: standard\n").unwrap();
        let cfg = resolve(&file, &fake_env(&[])).unwrap();
        assert_eq!(cfg.backend.backend_type, BackendType::Standard);

        // ...and env overrides it (back to Express here).
        let cfg = resolve(&file, &fake_env(&[("PACER_BACKEND_TYPE", "express")])).unwrap();
        assert_eq!(cfg.backend.backend_type, BackendType::Express);

        // An unknown value is a startup error, not a silent default.
        let err = resolve(
            &FileConfig::default(),
            &fake_env(&[("PACER_BACKEND_TYPE", "glacier")]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("PACER_BACKEND_TYPE"), "got: {err}");
    }

    #[test]
    fn file_overrides_defaults() {
        let file = parse_file(
            "
listen-addr: 0.0.0.0:7000
cache:
  mem-capacity: 2GiB
  uring:
    threads: 8
runtime:
  worker-threads: 4
  rdma-worker-threads: 2
bucket-map:
  cache: real--use2-az1--x-s3
",
        )
        .unwrap();
        let cfg = resolve(&file, &fake_env(&[])).unwrap();
        assert_eq!(cfg.listen_addr, "0.0.0.0:7000");
        assert_eq!(cfg.cache.mem_capacity, 2 << 30);
        // flush_buffer_size unset everywhere → 0 → auto (2 × block_size).
        assert_eq!(cfg.cache.flush_buffer_size, 0);
        assert_eq!(
            cfg.cache.flush_buffer_size_or_default(),
            2 * cfg.cache.block_size
        );
        assert_eq!(cfg.cache.uring.threads, 8);
        assert_eq!(cfg.cache.uring.io_depth, UringConfig::default().io_depth);
        assert_eq!(cfg.worker_threads, 4);
        assert_eq!(cfg.rdma_worker_threads, 2);
        // env wins over the file for the RDMA runtime knob too.
        let cfg_env = resolve(&file, &fake_env(&[("PACER_RDMA_WORKER_THREADS", "3")])).unwrap();
        assert_eq!(cfg_env.rdma_worker_threads, 3);
        assert_eq!(
            cfg.bucket_map.get("cache").map(String::as_str),
            Some("real--use2-az1--x-s3")
        );
    }

    #[test]
    fn env_overrides_file() {
        let file = parse_file("listen-addr: 0.0.0.0:7000\ncache:\n  mem-capacity: 2GiB\n").unwrap();
        let env = fake_env(&[
            ("PACER_LISTEN_ADDR", "0.0.0.0:6000"),
            ("PACER_MEM_CAPACITY", "512MiB"),
        ]);
        let cfg = resolve(&file, &env).unwrap();
        assert_eq!(cfg.listen_addr, "0.0.0.0:6000");
        assert_eq!(cfg.cache.mem_capacity, 512 << 20);
    }

    #[test]
    fn env_bucket_map_overrides_file_map() {
        let file = parse_file("bucket-map:\n  cache: from-file\n").unwrap();
        let env = fake_env(&[("PACER_BUCKET_MAP", "cache=from-env,extra=e")]);
        let cfg = resolve(&file, &env).unwrap();
        assert_eq!(
            cfg.bucket_map.get("cache").map(String::as_str),
            Some("from-env")
        );
        assert_eq!(cfg.bucket_map.len(), 2);
    }

    #[test]
    fn unknown_key_is_rejected() {
        let err = parse_file("listen-addr: x\nnonsense-key: 1\n").unwrap_err();
        assert!(
            err.to_string().contains("nonsense-key") || err.to_string().contains("unknown field"),
            "expected unknown-field error, got: {err}"
        );
    }

    #[test]
    fn cluster_on_when_node_name_set() {
        let file =
            parse_file("cluster:\n  peer-listen-addr: 0.0.0.0:9200\n  channel-capacity: 32\n")
                .unwrap();
        let env = fake_env(&[
            ("PACER_NODE_NAME", "node-a"),
            ("PACER_PEER_SERVICE", "pacer-peer"),
        ]);
        let cluster = resolve(&file, &env).unwrap().cluster.unwrap();
        assert_eq!(cluster.node_name, "node-a");
        assert_eq!(cluster.peer_listen_addr, "0.0.0.0:9200");
        assert_eq!(cluster.peer_port, 9200);
        assert_eq!(cluster.channel_capacity, 32);
    }

    /// Unset must mean "leave hyper alone", not "zero".
    ///
    /// A zero window is a legal HTTP/2 setting and it means *nothing may be sent* — so
    /// defaulting these to 0 rather than absent would not be a conservative default, it would
    /// deadlock the whole peer plane on the first RPC. Worth a test because the two spellings
    /// look identical in a `values.yaml` diff.
    #[test]
    fn unset_h2_windows_leave_hypers_defaults_rather_than_becoming_zero() {
        let env = fake_env(&[
            ("PACER_NODE_NAME", "node-a"),
            ("PACER_PEER_SERVICE", "pacer-peer"),
        ]);
        let c = resolve(&FileConfig::default(), &env)
            .unwrap()
            .cluster
            .unwrap();
        assert_eq!(c.h2_windows, pacer_transport::H2Windows::default());
        assert!(!c.h2_windows.is_set());
    }

    #[test]
    fn h2_windows_accept_a_byte_size_and_refuse_one_http2_cannot_carry() {
        let env = fake_env(&[
            ("PACER_NODE_NAME", "node-a"),
            ("PACER_PEER_SERVICE", "pacer-peer"),
            ("PACER_PEER_H2_STREAM_WINDOW", "16MiB"),
            ("PACER_PEER_H2_CONNECTION_WINDOW", "67108864"),
        ]);
        let c = resolve(&FileConfig::default(), &env)
            .unwrap()
            .cluster
            .unwrap();
        assert_eq!(c.h2_windows.stream_bytes, Some(16 << 20));
        assert_eq!(c.h2_windows.connection_bytes, Some(64 << 20));
        assert!(c.h2_windows.is_set());

        // The chart's route: the ConfigMap, because `extraEnv` is documented as NOT a home
        // for PACER_* knobs (only `config` is checksummed into a pod roll, ADR-0013).
        let file =
            parse_file("cluster:\n  h2-stream-window: 16MiB\n  h2-connection-window: 64MiB\n")
                .unwrap();
        let env = fake_env(&[
            ("PACER_NODE_NAME", "node-a"),
            ("PACER_PEER_SERVICE", "pacer-peer"),
        ]);
        let c = resolve(&file, &env).unwrap().cluster.unwrap();
        assert_eq!(c.h2_windows.stream_bytes, Some(16 << 20));
        assert_eq!(c.h2_windows.connection_bytes, Some(64 << 20));

        // Past 2^31-1 is a FLOW_CONTROL_ERROR at handshake time, which would present as
        // "no peer can be dialed" — so it fails at startup, naming the variable.
        let env = fake_env(&[
            ("PACER_NODE_NAME", "node-a"),
            ("PACER_PEER_SERVICE", "pacer-peer"),
            ("PACER_PEER_H2_STREAM_WINDOW", "4GiB"),
        ]);
        let err = resolve(&FileConfig::default(), &env)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(PEER_H2_STREAM_WINDOW.name),
            "the refusal must name the variable, got: {err}"
        );
    }

    /// Both configuration layers reach the pool width, and the shipped default is the single
    /// cached channel this replaced — a default that widened silently would change the peer
    /// plane's socket count on every existing deployment.
    #[test]
    fn peer_connections_default_to_one_and_come_from_either_layer() {
        let env = fake_env(&[
            ("PACER_NODE_NAME", "node-a"),
            ("PACER_PEER_SERVICE", "pacer-peer"),
        ]);
        let c = resolve(&FileConfig::default(), &env)
            .unwrap()
            .cluster
            .unwrap();
        assert_eq!(
            c.peer_connections,
            pacer_transport::DEFAULT_PEER_CONNECTIONS
        );
        assert_eq!(c.peer_connections, 1);

        let file = parse_file("cluster:\n  peer-connections: 8\n").unwrap();
        assert_eq!(
            resolve(&file, &env)
                .unwrap()
                .cluster
                .unwrap()
                .peer_connections,
            8
        );

        let env = fake_env(&[
            ("PACER_NODE_NAME", "node-a"),
            ("PACER_PEER_SERVICE", "pacer-peer"),
            ("PACER_PEER_CONNECTIONS", "4"),
        ]);
        assert_eq!(
            resolve(&FileConfig::default(), &env)
                .unwrap()
                .cluster
                .unwrap()
                .peer_connections,
            4,
            "env must win over the file, like every other knob"
        );
    }

    /// A width past the cap is refused by name rather than clamped: an arm is named after its
    /// pool width, and a value quietly reduced would publish one width and measure another.
    #[test]
    fn a_peer_connection_count_past_the_cap_is_refused_by_name() {
        let env = fake_env(&[
            ("PACER_NODE_NAME", "node-a"),
            ("PACER_PEER_SERVICE", "pacer-peer"),
            ("PACER_PEER_CONNECTIONS", "4096"),
        ]);
        let err = resolve(&FileConfig::default(), &env)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(PEER_CONNECTIONS.name),
            "the refusal must name the variable, got: {err}"
        );
    }

    #[test]
    fn replication_knobs_default_layer_and_clamp() {
        // Defaults (ADR-0016) when nothing is set.
        let env = fake_env(&[
            ("PACER_NODE_NAME", "node-a"),
            ("PACER_PEER_SERVICE", "pacer-peer"),
        ]);
        let c = resolve(&FileConfig::default(), &env)
            .unwrap()
            .cluster
            .unwrap();
        assert_eq!(c.replication_r, DEFAULT_REPLICATION_R);
        assert_eq!(
            c.local_admission_threshold,
            DEFAULT_LOCAL_ADMISSION_THRESHOLD
        );
        assert_eq!(
            c.local_admission_window.as_secs(),
            DEFAULT_LOCAL_ADMISSION_WINDOW_SECS
        );
        assert!((c.local_copy_capacity_fraction - 0.25).abs() < f64::EPSILON);

        // File values layer in; env wins over file.
        let file = parse_file(
            "cluster:\n  replication-r: 3\n  local-admission-threshold: 4\n  \
             local-admission-window-secs: 30\n  local-copy-capacity-percent: 50\n",
        )
        .unwrap();
        let c = resolve(&file, &env).unwrap().cluster.unwrap();
        assert_eq!(c.replication_r, 3);
        assert_eq!(c.local_admission_threshold, 4);
        assert_eq!(c.local_admission_window.as_secs(), 30);
        assert!((c.local_copy_capacity_fraction - 0.50).abs() < f64::EPSILON);

        // R is clamped to max_sharers_tracked (directory can't list more homes
        // than it tracks) and floored at 1.
        let env2 = fake_env(&[
            ("PACER_NODE_NAME", "node-a"),
            ("PACER_PEER_SERVICE", "pacer-peer"),
            ("PACER_REPLICATION_R", "99"),
            ("PACER_MAX_SHARERS_TRACKED", "8"),
        ]);
        let c = resolve(&FileConfig::default(), &env2)
            .unwrap()
            .cluster
            .unwrap();
        assert_eq!(c.replication_r, 8);
    }

    /// Shared env for the RDMA knob tests: the two vars that switch cluster mode on.
    fn cluster_base() -> Vec<(&'static str, &'static str)> {
        vec![
            ("PACER_NODE_NAME", "node-a"),
            ("PACER_PEER_SERVICE", "pacer-peer"),
        ]
    }

    /// Resolve the cluster config from `pairs` (panicking on error), for the knob
    /// tests below — each of which asserts about one field.
    fn cluster_from(file: &FileConfig, pairs: &[(&str, &str)]) -> ClusterConfig {
        resolve(file, &fake_env(pairs)).unwrap().cluster.unwrap()
    }

    /// ADR-0024: the arena is sized in BYTES, so a size suffix must parse, and the
    /// page request layers like every other knob (file under env).
    #[test]
    fn rdma_arena_bytes_and_pages_layer_and_env_wins() {
        let base = cluster_base();
        // Unset everywhere → 0, i.e. "use the transport's default arena".
        let c = cluster_from(&FileConfig::default(), &base);
        assert_eq!(c.rdma_arena_bytes, 0);
        assert_eq!(c.rdma_arena_page_mib, 0);
        // File values layer in, with a size suffix on the arena...
        let file =
            parse_file("cluster:\n  rdma-arena-bytes: 37GiB\n  rdma-arena-page-mib: 2\n").unwrap();
        let c = cluster_from(&file, &base);
        assert_eq!(c.rdma_arena_bytes, 37 << 30);
        assert_eq!(c.rdma_arena_page_mib, 2);
        // ...and env wins over the file.
        let mut env_pairs = base.clone();
        env_pairs.push(("PACER_RDMA_ARENA_BYTES", "8GiB"));
        env_pairs.push(("PACER_RDMA_ARENA_PAGE_MIB", "1024"));
        let c = cluster_from(&file, &env_pairs);
        assert_eq!(c.rdma_arena_bytes, 8 << 30);
        assert_eq!(c.rdma_arena_page_mib, 1024);
    }

    /// ADR-0028's slab knob, through both layers. Worth its own test because the
    /// default must be `0`: a slab is pinned memory the pod's limit has to cover,
    /// so it can only ever appear because someone asked for it.
    #[test]
    fn cache_slab_bytes_defaults_off_and_layers_like_the_arena() {
        let base = cluster_base();
        assert_eq!(
            cluster_from(&FileConfig::default(), &base).cache_slab_bytes,
            0
        );
        let file = parse_file("cluster:\n  cache-slab-bytes: 40GiB\n").unwrap();
        assert_eq!(cluster_from(&file, &base).cache_slab_bytes, 40 << 30);
        let mut env_pairs = base.clone();
        env_pairs.push(("PACER_CACHE_SLAB_BYTES", "100Gi"));
        assert_eq!(cluster_from(&file, &env_pairs).cache_slab_bytes, 100 << 30);
    }

    /// ADR-0025: placement defaults ON, and ONLY an explicitly falsey value selects
    /// the pre-D5 control arm — a typo must never silently pick it. The per-rail
    /// window (D5.1) defaults to 0 = unbounded, what every arm through D5 step 0
    /// measured.
    #[test]
    fn rdma_placement_and_rail_window_layer() {
        let base = cluster_base();
        let c = cluster_from(&FileConfig::default(), &base);
        assert!(c.rdma_affinity);
        assert_eq!(c.rdma_rail_window, 0);
        for off in ["0", "false", "off", "no"] {
            let mut env_off = base.clone();
            env_off.push(("PACER_RDMA_AFFINITY", off));
            assert!(
                !cluster_from(&FileConfig::default(), &env_off).rdma_affinity,
                "{off} should disable placement"
            );
        }
        let file_off =
            parse_file("cluster:\n  rdma-affinity: false\n  rdma-rail-window: 4\n").unwrap();
        let c = cluster_from(&file_off, &base);
        assert!(!c.rdma_affinity);
        assert_eq!(c.rdma_rail_window, 4);
        // Env wins over the file, like every other knob.
        let mut env_on = base.clone();
        env_on.push(("PACER_RDMA_AFFINITY", "1"));
        env_on.push(("PACER_RDMA_RAIL_WINDOW", "16"));
        let c = cluster_from(&file_off, &env_on);
        assert!(c.rdma_affinity);
        assert_eq!(c.rdma_rail_window, 16);
    }

    /// The retired slot knob is a hard startup error naming its replacement, not a
    /// value silently ignored — being ignored would boot the DEFAULT arena, which is
    /// indistinguishable from the invisible cap ADR-0024 removed.
    #[test]
    fn retired_requester_slots_knob_fails_startup() {
        let base = cluster_base();
        let mut legacy = base.clone();
        legacy.push(("PACER_RDMA_REQUESTER_SLOTS", "256"));
        let err = resolve(&FileConfig::default(), &fake_env(&legacy))
            .unwrap_err()
            .to_string();
        assert!(err.contains("PACER_RDMA_ARENA_BYTES"), "{err}");
        // An explicit 0 was the old "use the default" spelling — not a request, so
        // it must not block a boot.
        let mut zeroed = base.clone();
        zeroed.push(("PACER_RDMA_REQUESTER_SLOTS", "0"));
        assert!(resolve(&FileConfig::default(), &fake_env(&zeroed)).is_ok());
    }

    #[test]
    fn reannounce_interval_defaults_layers_and_env_wins() {
        let base = &[
            ("PACER_NODE_NAME", "node-a"),
            ("PACER_PEER_SERVICE", "pacer-peer"),
        ];
        // Unset everywhere → the ADR-0017 healer default.
        let c = resolve(&FileConfig::default(), &fake_env(base))
            .unwrap()
            .cluster
            .unwrap();
        assert_eq!(
            c.reannounce_interval.as_secs(),
            DEFAULT_REANNOUNCE_INTERVAL_SECS
        );
        // File value layers in...
        let file = parse_file("cluster:\n  reannounce-interval-secs: 90\n").unwrap();
        let c = resolve(&file, &fake_env(base)).unwrap().cluster.unwrap();
        assert_eq!(c.reannounce_interval.as_secs(), 90);
        // ...and env wins over the file; 0 falls back to the default.
        let mut env_pairs = base.to_vec();
        env_pairs.push(("PACER_REANNOUNCE_INTERVAL_SECS", "0"));
        let c = resolve(&file, &fake_env(&env_pairs))
            .unwrap()
            .cluster
            .unwrap();
        assert_eq!(
            c.reannounce_interval.as_secs(),
            DEFAULT_REANNOUNCE_INTERVAL_SECS
        );
        env_pairs.pop();
        env_pairs.push(("PACER_REANNOUNCE_INTERVAL_SECS", "45"));
        let c = resolve(&file, &fake_env(&env_pairs))
            .unwrap()
            .cluster
            .unwrap();
        assert_eq!(c.reannounce_interval.as_secs(), 45);
    }

    #[test]
    fn efa_qps_per_rail_defaults_layers_and_env_wins() {
        let base = &[
            ("PACER_NODE_NAME", "node-a"),
            ("PACER_PEER_SERVICE", "pacer-peer"),
        ];
        // Unset everywhere → one QP per rail (pre-multi-QP behavior).
        let c = resolve(&FileConfig::default(), &fake_env(base))
            .unwrap()
            .cluster
            .unwrap();
        assert_eq!(c.efa_qps_per_rail, 1);
        // File value layers in...
        let file = parse_file("cluster:\n  efa-qps-per-rail: 2\n").unwrap();
        let c = resolve(&file, &fake_env(base)).unwrap().cluster.unwrap();
        assert_eq!(c.efa_qps_per_rail, 2);
        // ...and env wins over the file. The value is passed through verbatim
        // (the transport, not config, clamps 0 → 1 — see `bring_up_rails`).
        let mut env_pairs = base.to_vec();
        env_pairs.push(("PACER_EFA_QPS_PER_RAIL", "8"));
        let c = resolve(&file, &fake_env(&env_pairs))
            .unwrap()
            .cluster
            .unwrap();
        assert_eq!(c.efa_qps_per_rail, 8);
    }

    /// ADR-0026: delivery is OFF unless explicitly enabled (a typo must not turn
    /// on a path that pins client memory), the ceilings default to their built-in
    /// values rather than to zero — a zero ceiling would refuse every target and
    /// read as a bug — and both layer file-under-env like every other knob.
    #[test]
    fn delivery_knobs_default_off_and_layer() {
        use crate::delivery::{
            DEFAULT_MAX_TARGET_BYTES, DEFAULT_PINNED_BYTES_MAX, DEFAULT_SHM_DIR,
        };
        let cfg = resolve(&FileConfig::default(), &fake_env(&[])).unwrap();
        assert!(!cfg.delivery.enabled);
        assert_eq!(cfg.delivery.shm_dir, std::path::Path::new(DEFAULT_SHM_DIR));
        assert_eq!(cfg.delivery.max_target_bytes, DEFAULT_MAX_TARGET_BYTES);
        assert_eq!(cfg.delivery.pinned_bytes_max, DEFAULT_PINNED_BYTES_MAX);

        // Only an explicitly truthy value enables it; a typo leaves it off.
        for on in ["1", "true", "on", "yes"] {
            let cfg = resolve(
                &FileConfig::default(),
                &fake_env(&[("PACER_DELIVERY_ENABLED", on)]),
            )
            .unwrap();
            assert!(cfg.delivery.enabled, "{on} should enable delivery");
        }
        for off in ["0", "false", "off", "no", "ture", ""] {
            let cfg = resolve(
                &FileConfig::default(),
                &fake_env(&[("PACER_DELIVERY_ENABLED", off)]),
            )
            .unwrap();
            assert!(!cfg.delivery.enabled, "{off:?} must not enable delivery");
        }

        // File values layer in, with size suffixes on both ceilings...
        let file = parse_file(
            "delivery:\n  enabled: true\n  shm-dir: /mnt/pacer-shm\n  \
             max-target-bytes: 2GiB\n  pinned-bytes-max: 16GiB\n",
        )
        .unwrap();
        let cfg = resolve(&file, &fake_env(&[])).unwrap();
        assert!(cfg.delivery.enabled);
        assert_eq!(cfg.delivery.shm_dir, std::path::Path::new("/mnt/pacer-shm"));
        assert_eq!(cfg.delivery.max_target_bytes, 2 << 30);
        assert_eq!(cfg.delivery.pinned_bytes_max, 16 << 30);

        // ...and env wins over the file, including turning it back off.
        let cfg = resolve(
            &file,
            &fake_env(&[
                ("PACER_DELIVERY_ENABLED", "0"),
                ("PACER_DELIVERY_SHM_DIR", "/dev/shm"),
                ("PACER_DELIVERY_PINNED_BYTES_MAX", "4GiB"),
            ]),
        )
        .unwrap();
        assert!(!cfg.delivery.enabled);
        assert_eq!(cfg.delivery.shm_dir, std::path::Path::new("/dev/shm"));
        assert_eq!(cfg.delivery.pinned_bytes_max, 4 << 30);
        // Unset env leaves the file value standing.
        assert_eq!(cfg.delivery.max_target_bytes, 2 << 30);
    }

    /// C3's holder-writes-directly path (ADR-0030 point 4) is off unless explicitly
    /// asked for, and layers like every other knob. It is the arm's control, so a
    /// run that believes it enabled the treatment and did not would report the
    /// two-hop path's number as C3's — the same class of mistake as a chart default
    /// that left ADR-0028's slab off through every shipped profile.
    #[test]
    fn the_remote_write_half_defaults_off_and_layers() {
        assert!(
            !resolve(&FileConfig::default(), &fake_env(&[]))
                .unwrap()
                .delivery
                .remote_write
        );
        for on in ["1", "true", "on", "yes"] {
            let cfg = resolve(
                &FileConfig::default(),
                &fake_env(&[("PACER_DELIVERY_REMOTE_WRITE", on)]),
            )
            .unwrap();
            assert!(
                cfg.delivery.remote_write,
                "{on} should enable the remote half"
            );
        }
        for off in ["0", "false", "off", "no", "ture", ""] {
            let cfg = resolve(
                &FileConfig::default(),
                &fake_env(&[("PACER_DELIVERY_REMOTE_WRITE", off)]),
            )
            .unwrap();
            assert!(
                !cfg.delivery.remote_write,
                "{off:?} must not enable the remote half"
            );
        }
        // File on, env off — env wins, which is what makes an A/B arm settable per pod.
        let file = parse_file("delivery:\n  enabled: true\n  remote-write: true\n").unwrap();
        assert!(
            resolve(&file, &fake_env(&[]))
                .unwrap()
                .delivery
                .remote_write
        );
        assert!(
            !resolve(&file, &fake_env(&[("PACER_DELIVERY_REMOTE_WRITE", "0")]))
                .unwrap()
                .delivery
                .remote_write
        );
    }

    /// ADR-0032: every scatter ceiling defaults to its built-in rather than to zero,
    /// and all of them layer file-under-env. Whether the scatter itself is on is the
    /// next test's subject.
    #[test]
    fn scatter_knobs_default_and_layer() {
        use crate::scatter::{
            DEFAULT_MIN_SCATTER_BYTES, DEFAULT_SATURATED_COOLDOWN_SECS, DEFAULT_STAGING_BYTES,
            DEFAULT_STAGING_TTL_SECS, DEFAULT_WINDOWS_IN_FLIGHT,
        };
        let cfg = resolve(&FileConfig::default(), &fake_env(&[])).unwrap();
        assert_eq!(cfg.scatter.staging_bytes, DEFAULT_STAGING_BYTES);
        assert_eq!(
            cfg.scatter.staging_ttl.as_secs(),
            DEFAULT_STAGING_TTL_SECS,
            "a zero TTL would reap everything instantly"
        );
        assert_eq!(cfg.scatter.windows_in_flight, DEFAULT_WINDOWS_IN_FLIGHT);
        assert_eq!(cfg.scatter.min_object_bytes, DEFAULT_MIN_SCATTER_BYTES);
        assert_eq!(
            cfg.scatter.saturated_cooldown.as_secs(),
            DEFAULT_SATURATED_COOLDOWN_SECS
        );

        // The file block sets them...
        let file = parse_file(
            "backend:\n  backend-type: standard\nscatter:\n  enabled: true\n  \
             staging-bytes: 4GiB\n  staging-ttl-secs: 60\n  windows-in-flight: 32\n  \
             min-object-bytes: 256MiB\n  saturated-cooldown-secs: 2\n",
        )
        .unwrap();
        let cfg = resolve(&file, &fake_env(&[])).unwrap();
        assert!(cfg.scatter.enabled);
        assert_eq!(cfg.scatter.staging_bytes, 4 << 30);
        assert_eq!(cfg.scatter.staging_ttl.as_secs(), 60);
        assert_eq!(cfg.scatter.windows_in_flight, 32);
        assert_eq!(cfg.scatter.min_object_bytes, 256 << 20);
        assert_eq!(cfg.scatter.saturated_cooldown.as_secs(), 2);

        // ...and env wins over the file, including turning it back off.
        let cfg = resolve(
            &file,
            &fake_env(&[
                ("PACER_SCATTER_ENABLED", "0"),
                ("PACER_SCATTER_STAGING_BYTES", "1GiB"),
                ("PACER_SCATTER_WINDOWS_IN_FLIGHT", "8"),
            ]),
        )
        .unwrap();
        assert!(!cfg.scatter.enabled);
        assert_eq!(cfg.scatter.staging_bytes, 1 << 30);
        assert_eq!(cfg.scatter.windows_in_flight, 8);
        // Unset env leaves the file value standing.
        assert_eq!(cfg.scatter.staging_ttl.as_secs(), 60);
    }

    /// ADR-0032 § 6, amended 2026-09-01: `PACER_SCATTER_ENABLED` is three-state, and
    /// *unset* means on where the design applies. Both halves of that are asserted here
    /// because either alone would be satisfied by a plain boolean — off on the default
    /// (Express) backend, on for a general-purpose one — and the pair is what makes the
    /// default expressible at all: a blanket `true` would refuse to start every Express
    /// daemon, since the combination below is a hard error.
    #[test]
    fn the_scatter_default_is_scoped_to_the_backend() {
        assert!(
            !resolve(&FileConfig::default(), &fake_env(&[]))
                .unwrap()
                .scatter
                .enabled,
            "the default backend is Express, where ADR-0032 § 6 does not apply"
        );
        assert!(
            resolve(
                &FileConfig::default(),
                &fake_env(&[("PACER_BACKEND_TYPE", "standard")])
            )
            .unwrap()
            .scatter
            .enabled,
            "a Standard backend scatters unless told otherwise"
        );
        // An explicit value wins over the derivation in BOTH directions, and the `off` list
        // is the load-bearing one now: it runs on `standard`, where the default is on, so
        // each value has to actually override it. `ture` is there deliberately — a non-empty
        // unrecognised value is OFF, not "derive it", because a typo that costs the speedup
        // is recoverable and one that hands a client the composite ETag is not.
        for (raw, want) in [
            ("1", true),
            ("true", true),
            ("on", true),
            ("yes", true),
            ("0", false),
            ("false", false),
            ("off", false),
            ("no", false),
            ("ture", false),
            // Empty is the *unset* case however it arrives, so it derives — which on this
            // backend means on. This is the value the chart passes when an operator names
            // no preference, so it is not a hypothetical.
            ("", true),
            ("   ", true),
        ] {
            let cfg = resolve(
                &FileConfig::default(),
                &fake_env(&[
                    ("PACER_BACKEND_TYPE", "standard"),
                    ("PACER_SCATTER_ENABLED", raw),
                ]),
            )
            .unwrap();
            assert_eq!(
                cfg.scatter.enabled, want,
                "PACER_SCATTER_ENABLED={raw:?} on a Standard backend should resolve to {want}"
            );
        }
    }

    /// ADR-0032 § 6 scopes the scatter to general-purpose buckets. Asking for it
    /// on an Express directory bucket fails startup rather than being silently
    /// downgraded — an operator who set the flag should not be left believing the
    /// path is live when every PUT is still a plain proxy.
    #[test]
    fn enabling_the_scatter_on_express_fails_startup() {
        let err = resolve(
            &FileConfig::default(),
            &fake_env(&[
                ("PACER_BACKEND_TYPE", "express"),
                ("PACER_SCATTER_ENABLED", "1"),
            ]),
        )
        .expect_err("Express + scatter must not resolve");
        let msg = err.to_string();
        assert!(msg.contains("PACER_SCATTER_ENABLED"), "{msg}");
        assert!(msg.contains("general-purpose"), "{msg}");

        // The default backend IS Express, so the same must hold with it unset.
        assert!(resolve(
            &FileConfig::default(),
            &fake_env(&[("PACER_SCATTER_ENABLED", "1")]),
        )
        .is_err());
    }

    #[test]
    fn cluster_without_membership_errors() {
        // Node name set (cluster on) but neither peers nor a peer service.
        let env = fake_env(&[("PACER_NODE_NAME", "node-a")]);
        let err = resolve(&FileConfig::default(), &env).unwrap_err();
        assert!(err.to_string().contains("needs"), "got: {err}");
    }

    #[test]
    fn static_peers_env_beats_k8s_membership() {
        let env = fake_env(&[
            ("PACER_NODE_NAME", "node-a"),
            ("PACER_PEERS", "node-a=10.0.0.1,node-b=10.0.0.2"),
        ]);
        let cluster = resolve(&FileConfig::default(), &env)
            .unwrap()
            .cluster
            .unwrap();
        assert!(matches!(cluster.membership, Membership::Static { .. }));
    }

    /// ADR-0036: the listener bounds default, layer from the file, and are
    /// overridden by env — including `0` meaning "the built-in default", which is
    /// what lets a chart emit the block unconditionally.
    #[test]
    fn listen_limits_default_layer_and_env_wins() {
        use crate::listen::ListenLimits;
        let d = ListenLimits::default();

        let c = resolve(&FileConfig::default(), &fake_env(&[])).unwrap();
        assert_eq!(c.listen.max_connections, d.max_connections);
        assert_eq!(c.listen.header_timeout, d.header_timeout);
        assert_eq!(c.listen.idle_timeout, d.idle_timeout);

        let file = parse_file(
            "listen:\n  max-connections: 32\n  header-timeout-secs: 5\n  \
             idle-timeout-secs: 7\n",
        )
        .unwrap();
        let c = resolve(&file, &fake_env(&[])).unwrap();
        assert_eq!(c.listen.max_connections, 32);
        assert_eq!(c.listen.header_timeout.as_secs(), 5);
        assert_eq!(c.listen.idle_timeout.as_secs(), 7);

        let c = resolve(
            &file,
            &fake_env(&[
                ("PACER_S3_MAX_CONNECTIONS", "64"),
                ("PACER_S3_HEADER_TIMEOUT", "11"),
                // `0` is not "no timeout" — it is "use the default".
                ("PACER_S3_IDLE_TIMEOUT", "0"),
            ]),
        )
        .unwrap();
        assert_eq!(c.listen.max_connections, 64);
        assert_eq!(c.listen.header_timeout.as_secs(), 11);
        assert_eq!(c.listen.idle_timeout, d.idle_timeout);
    }

    /// ADR-0036: the drain deadline follows the same three layers, and its default
    /// is the one paired with the chart's grace period.
    #[test]
    fn drain_timeout_default_layer_and_env_wins() {
        let c = resolve(&FileConfig::default(), &fake_env(&[])).unwrap();
        assert_eq!(
            c.shutdown_drain_timeout.as_secs(),
            crate::shutdown::DEFAULT_DRAIN_TIMEOUT_SECS
        );

        let file = parse_file("shutdown:\n  drain-timeout-secs: 9\n").unwrap();
        assert_eq!(
            resolve(&file, &fake_env(&[]))
                .unwrap()
                .shutdown_drain_timeout
                .as_secs(),
            9
        );
        assert_eq!(
            resolve(&file, &fake_env(&[("PACER_SHUTDOWN_DRAIN_TIMEOUT", "3")]))
                .unwrap()
                .shutdown_drain_timeout
                .as_secs(),
            3
        );
    }

    /// ADR-0036: JSON is the default encoding, `text` is reachable from either
    /// layer, and a typo is a startup error rather than a silent switch.
    #[test]
    fn log_format_defaults_to_json_and_rejects_typos() {
        let c = resolve(&FileConfig::default(), &fake_env(&[])).unwrap();
        assert_eq!(c.log_format, LogFormat::Json);

        let file = parse_file("log:\n  format: text\n").unwrap();
        assert_eq!(
            resolve(&file, &fake_env(&[])).unwrap().log_format,
            LogFormat::Text
        );
        assert_eq!(
            resolve(&file, &fake_env(&[("PACER_LOG_FORMAT", "JSON")]))
                .unwrap()
                .log_format,
            LogFormat::Json,
            "the value is case-insensitive"
        );

        let bad = resolve(
            &FileConfig::default(),
            &fake_env(&[("PACER_LOG_FORMAT", "jsonl")]),
        );
        assert!(bad.is_err(), "an unknown encoding must fail startup");
    }

    /// A `0` for the connection cap means the default, not "admit nothing" — the
    /// inverse reading would render the daemon unreachable from a chart that
    /// emitted an empty value.
    #[test]
    fn zero_max_connections_is_the_default_not_zero() {
        let c = resolve(
            &FileConfig::default(),
            &fake_env(&[("PACER_S3_MAX_CONNECTIONS", "0")]),
        )
        .unwrap();
        assert_eq!(
            c.listen.max_connections,
            crate::listen::ListenLimits::default_max_connections()
        );
    }
}
