//! Gate 3 correctness suite for the write scatter (ADR-0032,
//! `planning/24-write-path.md` § Phase 3).
//!
//! Five full daemon nodes — proxy, peer gRPC server on loopback, shared ring, and
//! ONE staging area per node shared by both roles exactly as `main` wires it — over
//! a single [`ProbeFs`] backend, driven through unmodified `aws-sdk-s3` clients.
//! What `ProbeFs` adds to `s3s-fs`, and what that can and cannot prove, is in
//! [`probe`]'s header.
//!
//! # Gates discharged here, and where each one lives
//!
//! | file | gates |
//! |---|---|
//! | [`gate_integrity`] | 3.1 (in process), 3.2, 3.6, 3.7, 3.8, 3.9, 3.13 |
//! | [`gate_budget`] | 3.3, both halves |
//! | [`gate_concurrency`] | 3.4, 3.5 |
//! | [`gate_placement`] | 3.10 (size half), 3.11 |
//! | [`gate_pipeline`] | the `windows_in_flight` memory bound, and phase attribution |
//! | [`probe`] | the suite's own checksum oracle |
//!
//! Still owed: gate 3.1's hardware arm (a real checkpoint shard on a real cluster) and
//! every Phase-4 bench arm. Gate 3.10's Express half is a *startup* refusal, so it
//! lives in `config::tests::enabling_the_scatter_on_express_fails_startup`; 3.12 and
//! 3.14 are unit tests in `scatter`, `staging` and `pacer_cache::codec`.
//!
//! The split is by gate group rather than by size. Each file's arms share a budget
//! (`ROOMY_STAGING`, `ONE_WINDOW_STAGING`, `SUB_WINDOW_STAGING`) and a question, so a
//! reader who wants "what does the design promise when a budget is full" reads one
//! file, and a failure names the group it broke.

// Tests are linear scenarios; splitting them to satisfy a line count would hurt
// readability (CLAUDE.md: size limits target production code).
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use pacer_backend::BackendType;
use pacer_cache::chunk::ChunkConfig;
use pacer_daemon::coordinate::ScatterCoordinator;
use pacer_daemon::metrics::Metrics;
use pacer_daemon::proxy::PacerProxy;
use pacer_daemon::scatter::ScatterConfig;
use pacer_daemon::staging::StagingArea;
use pacer_ring::{NodeId, SharedRing};
use pacer_transport::grpc::GrpcTransport;
use pacer_transport::{PeerTransport, StoreOffer, StoreOutcome};

use crate::common::{
    self, backend_service, create_test_bucket, node_parts, sdk_client_for, seeded_body, serve_node,
    CacheSpec, NodeParts, NodeSpec, BUCKET,
};

mod gate_budget;
mod gate_concurrency;
mod gate_integrity;
mod gate_pipeline;
mod gate_placement;
mod probe;

use probe::{base64_crc32, Faults, ProbeFs};

/// Chunk size — and therefore window size and S3 part size — for the whole suite,
/// pinned to `pacer_daemon::scatter::MIN_S3_PART_SIZE`. The smallest grid a scatter
/// is allowed to use, which keeps the test objects as small as the design permits.
const CHUNK_SIZE: u64 = 5 << 20;
/// Windows in a scattered test object. Eight over five nodes is enough for the ring
/// to place windows on [`SPREAD_OWNERS`] homes other than the coordinator, which is
/// what gate 3.1's "≥ 4 owners" asks for.
const WINDOWS: u64 = 8;
/// Length of a scattered test object.
const OBJECT_LEN: u64 = CHUNK_SIZE * WINDOWS;
/// Owners *other than the coordinator* a gate-3.1 object must reach.
const SPREAD_OWNERS: usize = 4;
/// The fleet. Five nodes so a coordinator plus [`SPREAD_OWNERS`] peers fit.
const NODES: [&str; 5] = ["node-a", "node-b", "node-c", "node-d", "node-e"];
/// Single-copy placement (ADR-0012), so every chunk has exactly one home and
/// "distinct homes" and "distinct owners" are the same count.
const REPLICATION_R: usize = 1;
/// A staging budget with room for a whole test object on one node, so nothing
/// refuses unless a test sets out to make it.
const ROOMY_STAGING: u64 = OBJECT_LEN;
/// Room for exactly one window: the second offer to a node is `BudgetExhausted`,
/// which is the *transient* refusal reject-fast is built around (gate 3.3).
const ONE_WINDOW_STAGING: u64 = CHUNK_SIZE;
/// A budget below one window, so every offer is `OversizedForBudget` — the
/// non-transient refusal, which no cooldown clears (gate 3.3's second arm).
const SUB_WINDOW_STAGING: u64 = CHUNK_SIZE - 1;
/// Long enough that nothing is reaped unless a test advances the clock itself.
const STAGING_TTL: Duration = Duration::from_secs(900);
/// Windows a coordinator keeps in flight. Below the window count on purpose, so
/// every test object exercises the semaphore that bounds coordinator memory.
const WINDOWS_IN_FLIGHT: usize = 4;
/// Windows a coordinator may have taken in from the body once every upload is held:
/// one per slot in flight, plus the one it is holding while it waits for a slot.
///
/// The whole bound `windows_in_flight` claims, and what
/// [`gate_pipeline::a_full_pipeline_stops_the_body_read`] asserts. Anything above this
/// is the coordinator buffering the client's object.
const WINDOWS_AT_A_FULL_PIPELINE: usize = WINDOWS_IN_FLIGHT + 1;
/// Smallest object these tests scatter: two windows. An object of one window is
/// then below the threshold and takes ADR-0007's path (gate 3.10).
const MIN_SCATTER_BYTES: u64 = 2 * CHUNK_SIZE;
/// Cooldown after a transient refusal. Longer than any test runs, so a refusal's
/// effect on later offers is deterministic rather than a race with a timer.
const SATURATED_COOLDOWN: Duration = Duration::from_secs(300);
/// Smallest cacheable object on the read path (ADR-0002), well below one window.
const MIN_OBJECT_SIZE: u64 = 1 << 20;
/// No upper bound on what these tests cache.
const MAX_OBJECT_SIZE: Option<u64> = None;
/// Wall clock an all-to-all round of concurrent scatters gets before gate 3.4
/// calls it a deadlock. Two orders of magnitude above the ~1 s the round takes on
/// loopback, so this fires on a genuine cycle and not on a slow machine.
const DEADLOCK_BUDGET: Duration = Duration::from_secs(120);

/// Windows in the interleaving objects — four rather than [`WINDOWS`], because the
/// gate-3.2 sweep writes one object per interleaving and the *sweep*, not the object,
/// is what has to be exhaustive.
const INTERLEAVE_WINDOWS: u64 = 4;
/// Length of one interleaving's object.
const INTERLEAVE_LEN: u64 = CHUNK_SIZE * INTERLEAVE_WINDOWS;
/// Owners an interleaving needs before "some committed, some not" means anything.
const MIN_INTERLEAVE_OWNERS: usize = 2;

/// foyer's capacities for a fleet node. Tighter than the suite default because there
/// are five of them per test and none of these arms is about residency.
const NODE_CACHE: CacheSpec = CacheSpec {
    mem_capacity: 128 << 20,
    disk_capacity: 1 << 30,
    block_size: 16 << 20,
};

// ------------------------------------------------------------------- the fleet

/// One daemon node.
struct Node {
    /// Client pointing at this node's S3 front.
    client: aws_sdk_s3::Client,
    metrics: Metrics,
    name: String,
    /// The node's staging area — the same `Arc` its proxy's coordinator and its peer
    /// server hold, which is what lets a test read the budget and drive the reaper.
    staging: Arc<StagingArea>,
    /// The same coordinator this node's proxy drives. Held so a test can hand it a
    /// body stream of its own making: the S3 front end reframes whatever a client
    /// sends, and one test needs to control exactly when a frame is handed over.
    coordinator: Arc<ScatterCoordinator>,
    _cache_dir: tempfile::TempDir,
}

/// The fleet, plus everything a test needs to observe it.
struct Harness {
    nodes: Vec<Node>,
    /// Ground-truth client straight at the backend, and the one the hand-driven
    /// uploads use to play coordinator.
    backend: aws_sdk_s3::Client,
    ring: SharedRing,
    faults: Arc<Faults>,
    chunk: ChunkConfig,
    /// A transport belonging to no node, for driving owner-side RPCs directly.
    probe: GrpcTransport,
    _backend_dir: tempfile::TempDir,
}

/// One planned window, as the coordinator would compute it.
#[derive(Debug, Clone)]
struct Window {
    index: u64,
    part_number: i32,
    chunk_key: String,
    /// The chunk's single home at R = 1.
    home: NodeId,
}

/// The scatter configuration every node in the suite runs, at `staging_bytes`.
fn scatter_config(staging_bytes: u64) -> ScatterConfig {
    ScatterConfig {
        enabled: true,
        staging_bytes,
        staging_ttl: STAGING_TTL,
        windows_in_flight: WINDOWS_IN_FLIGHT,
        min_object_bytes: MIN_SCATTER_BYTES,
        saturated_cooldown: SATURATED_COOLDOWN,
    }
}

/// [`NODES`] daemon nodes with the scatter on, each with a `staging_bytes` budget,
/// over one probe-wrapped backend.
///
/// The loop is here and the per-node assembly is [`common::node_parts`] +
/// [`scatter_node`] + [`common::serve_node`], which is what keeps this under a
/// screenful: the 429-line original was that assembly inlined, and `cluster.rs` had
/// the same code again without the staging area.
async fn fleet(staging_bytes: u64) -> Harness {
    let backend_dir = tempfile::tempdir().unwrap();
    let faults = Arc::new(Faults::default());
    let (service, creds) = backend_service(ProbeFs::new(
        s3s_fs::FileSystem::new(backend_dir.path()).unwrap(),
        Arc::clone(&faults),
    ));
    let backend_client = sdk_client_for(service.clone(), creds.clone());
    create_test_bucket(&backend_client).await;

    let chunk = ChunkConfig::new(CHUNK_SIZE);
    let ring = SharedRing::default();
    let mut nodes = Vec::new();
    let mut members = Vec::new();
    for name in NODES {
        let spec = NodeSpec {
            name,
            replication_r: REPLICATION_R,
            min_object_size: MIN_OBJECT_SIZE,
            max_object_size: MAX_OBJECT_SIZE,
            chunk,
            cache: NODE_CACHE,
        };
        let parts = node_parts(&spec, &ring, &service, &creds).await;
        let (proxy, staging, coordinator) = scatter_node(&parts, chunk, staging_bytes);
        // The peer gets the SAME staging area the coordinator holds: a coordinator's
        // own windows wait for the same Complete an owner's do (ADR-0032 § 4).
        let served = serve_node(&spec, &parts, proxy, Some(Arc::clone(&staging))).await;

        members.push(NodeId::new(name, served.peer_addr.to_string()));
        nodes.push(Node {
            client: served.client,
            metrics: parts.metrics,
            name: name.to_owned(),
            staging,
            coordinator,
            _cache_dir: parts.cache_dir,
        });
    }
    ring.store(members);

    Harness {
        nodes,
        backend: backend_client,
        ring,
        faults,
        chunk,
        probe: GrpcTransport::new(0, "probe"),
        _backend_dir: backend_dir,
    }
}

/// One node's scatter half: a coordinator over its staging area, and a proxy that
/// drives it.
///
/// Wired the way `main` wires it, and the sharing matters — the returned
/// [`StagingArea`] is the one the coordinator consults, so a peer server given a
/// *different* one would publish a budget nothing reads.
fn scatter_node(
    parts: &NodeParts,
    chunk: ChunkConfig,
    staging_bytes: u64,
) -> (PacerProxy, Arc<StagingArea>, Arc<ScatterCoordinator>) {
    let staging = Arc::new(StagingArea::new(
        usize::try_from(staging_bytes).unwrap(),
        STAGING_TTL,
    ));
    let coordinator = Arc::new(ScatterCoordinator::new(
        parts.backend.clone(),
        parts.tier.clone(),
        chunk,
        parts.cluster.clone(),
        &scatter_config(staging_bytes),
        Arc::clone(&staging),
        parts.metrics.clone(),
    ));
    let proxy = PacerProxy::new(
        parts.backend.clone(),
        parts.tier.clone(),
        parts.metrics.clone(),
        MIN_OBJECT_SIZE,
        MAX_OBJECT_SIZE,
        chunk,
        common::FILL_PARALLELISM,
    )
    // ADR-0032 § 6: the scatter is a general-purpose-bucket feature, and enabling it
    // on Express is refused at startup.
    .with_backend_type(BackendType::Standard)
    .with_cluster(parts.cluster.clone())
    .with_scatter(Arc::clone(&coordinator), MIN_SCATTER_BYTES);
    (proxy, staging, coordinator)
}

impl Harness {
    /// Node index for a node name.
    fn index_of(&self, name: &str) -> usize {
        self.nodes.iter().position(|n| n.name == name).unwrap()
    }

    /// The dialable [`NodeId`] for a node name.
    fn node_id(&self, name: &str) -> NodeId {
        self.ring
            .load()
            .members()
            .iter()
            .find(|n| n.name() == name)
            .expect("node in the ring")
            .clone()
    }

    /// The windows an object of `object_len` bytes under `key` decomposes into, and
    /// where each belongs — the same computation `ScatterPlan` does, done here so a
    /// test can address one window's home directly.
    fn plan(&self, key: &str, object_len: u64) -> Vec<Window> {
        let object_key = format!("{BUCKET}/{key}");
        (0..self.chunk.chunk_count(object_len))
            .map(|index| {
                let chunk_key = self.chunk.chunk_key(&object_key, index);
                let home = self
                    .ring
                    .homes(&chunk_key, REPLICATION_R)
                    .first()
                    .expect("a non-empty ring homes every chunk")
                    .clone();
                Window {
                    index,
                    part_number: i32::try_from(index + 1).unwrap(),
                    chunk_key,
                    home,
                }
            })
            .collect()
    }

    /// A key under `prefix` whose `object_len` bytes reach at least `owners` homes
    /// other than `exclude` (pass `""` to exclude nobody). Deterministic: the ring is
    /// fixed, so this searches the same keys in the same order every run and cannot
    /// flake on a key that happened to concentrate.
    fn key_reaching(&self, prefix: &str, object_len: u64, exclude: &str, owners: usize) -> String {
        /// Candidate keys tried before giving up. Far more than needed — a five-node
        /// ring reaches four peers on most keys — so exhausting it means the ring or
        /// the hash changed, not that this key was unlucky.
        const CANDIDATES: usize = 64;
        for i in 0..CANDIDATES {
            let key = format!("{prefix}-{i}.bin");
            let mut peers: Vec<String> = self
                .plan(&key, object_len)
                .iter()
                .map(|w| w.home.name().to_owned())
                .filter(|name| name != exclude)
                .collect();
            peers.sort_unstable();
            peers.dedup();
            if peers.len() >= owners {
                return key;
            }
        }
        panic!("no key under {prefix} reaches {owners} peer owners in {CANDIDATES} tries");
    }

    /// Read a whole object through node `idx`.
    async fn read_whole(&self, idx: usize, key: &str) -> Bytes {
        self.nodes[idx]
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes()
    }

    /// Read exactly window `w`'s byte range through node `idx`.
    async fn read_window(&self, idx: usize, key: &str, w: &Window, object_len: u64) -> Bytes {
        let bounds = self.chunk.chunk_bounds(w.index, object_len).unwrap();
        self.nodes[idx]
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .range(format!("bytes={}-{}", bounds.start, bounds.end - 1))
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes()
    }

    /// Read a whole object straight from the backend — the durability oracle.
    async fn read_backend(&self, key: &str) -> Bytes {
        self.backend
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes()
    }

    /// Whether the backend has no object under `key` — the check every failed write
    /// owes, since "the PUT failed" and "the object does not exist" are different
    /// claims and only the second one is safety.
    async fn is_absent(&self, key: &str) -> bool {
        match self
            .backend
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
        {
            Ok(_) => false,
            Err(e) => e.into_service_error().is_no_such_key(),
        }
    }

    /// Whether `home`'s directory shard lists any holder for `chunk_key`.
    async fn is_announced(&self, home: &NodeId, chunk_key: &str) -> bool {
        self.probe
            .lookup_sharers(home, chunk_key)
            .await
            .unwrap()
            .is_some_and(|set| !set.holders.is_empty())
    }

    /// Poll until node `idx` has completed (or aborted) a fill past `before`. A fill
    /// is a spawned tee, so a test that asserts on it immediately is racing it.
    async fn wait_for_fill(&self, idx: usize, before: u64) {
        let m = &self.nodes[idx].metrics;
        common::poll_until(
            &format!("{} fills after a miss", self.nodes[idx].name),
            || async { m.fills_completed.get() > before || m.fills_aborted.get() > before },
        )
        .await;
    }

    /// Poll until `home` lists a holder for `chunk_key` — commit and its announce
    /// are fire-and-forget, so a test that asserts immediately is racing them.
    async fn wait_announced(&self, home: &NodeId, chunk_key: &str) {
        common::poll_until(
            &format!("{} announces {chunk_key}", home.name()),
            || async { self.is_announced(home, chunk_key).await },
        )
        .await;
    }
}

/// An upload driven window by window from the test, stopping at Complete.
struct HandDriven {
    upload_id: String,
    /// The composite ETag Complete minted — what a commit publishes chunks under.
    e_tag: String,
    windows: Vec<Window>,
}

impl HandDriven {
    /// The distinct owners that staged at least one window, in window order.
    fn owners(&self) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        for w in &self.windows {
            if !names.iter().any(|n| n == w.home.name()) {
                names.push(w.home.name().to_owned());
            }
        }
        names
    }
}

impl Harness {
    /// Play coordinator by hand: open a multipart upload, offer every window to its
    /// home over the real peer plane, and Complete it — committing nothing.
    ///
    /// This is how a test reaches the states a *successful* coordinator never leaves
    /// behind: an upload whose owners have staged but not published (gate 3.7's dead
    /// coordinator), or one where only some owners were told (gate 3.2). Only the
    /// coordinator is the test; the owners run their real handlers over real gRPC,
    /// and the backend is the same one the daemons use.
    async fn drive_by_hand(&self, key: &str, payload: &Bytes) -> HandDriven {
        let object_len = payload.len() as u64;
        let created = self
            .backend
            .create_multipart_upload()
            .bucket(BUCKET)
            .key(key)
            .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc32)
            .checksum_type(aws_sdk_s3::types::ChecksumType::FullObject)
            .send()
            .await
            .unwrap();
        let upload_id = created.upload_id().unwrap().to_owned();
        let windows = self.plan(key, object_len);
        let mut parts = Vec::new();
        for w in &windows {
            let bounds = self.chunk.chunk_bounds(w.index, object_len).unwrap();
            let window = payload.slice(bounds.start as usize..bounds.end as usize);
            let checksum = base64_crc32(&window);
            let outcome = self
                .probe
                .store_chunk(
                    &w.home,
                    StoreOffer {
                        chunk_key: &w.chunk_key,
                        upload_id: &upload_id,
                        bucket: BUCKET,
                        key,
                        part_number: w.part_number,
                        body: window,
                        checksum_crc32: &checksum,
                    },
                )
                .await
                .unwrap();
            let StoreOutcome::Uploaded { e_tag } = outcome else {
                panic!(
                    "owner {} refused window {}: {outcome:?}",
                    w.home.name(),
                    w.index
                );
            };
            parts.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(w.part_number)
                    .e_tag(e_tag)
                    .build(),
            );
        }
        let out = self
            .backend
            .complete_multipart_upload()
            .bucket(BUCKET)
            .key(key)
            .upload_id(&upload_id)
            .checksum_crc32(base64_crc32(payload))
            .checksum_type(aws_sdk_s3::types::ChecksumType::FullObject)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await
            .unwrap();
        HandDriven {
            upload_id,
            e_tag: out.e_tag().unwrap().to_owned(),
            windows,
        }
    }

    /// PUT `payload` under `key` through node `idx`, expecting success.
    ///
    /// # Panics
    ///
    /// If the PUT fails; `why` says what the arm was proving.
    async fn put_expecting_success(
        &self,
        idx: usize,
        key: &str,
        payload: &Bytes,
        why: &str,
    ) -> aws_sdk_s3::operation::put_object::PutObjectOutput {
        self.nodes[idx]
            .client
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from(payload.clone()))
            .send()
            .await
            .unwrap_or_else(|e| panic!("{why}: {e}"))
    }

    /// Assert every node reads `key` back as `payload`.
    async fn assert_readable_everywhere(&self, key: &str, payload: &Bytes, what: &str) {
        assert_eq!(
            &self.read_backend(key).await,
            payload,
            "backend bytes differ after {what}"
        );
        for idx in 0..self.nodes.len() {
            assert_eq!(
                &self.read_whole(idx, key).await,
                payload,
                "bytes differ read through {} after {what}",
                self.nodes[idx].name
            );
        }
    }
}

/// A body whose every window is distinguishable, so a misplaced or duplicated
/// window shows up as wrong bytes rather than as bytes that happen to match.
fn body(seed: u8, len: u64) -> Bytes {
    seeded_body(seed, usize::try_from(len).unwrap())
}

/// Windows this node uploaded in the role `role`, off its scatter metrics.
fn windows(node: &Node, role: &str) -> u64 {
    node.metrics
        .scatter
        .windows
        .with_label_values(&[role])
        .get()
}

/// PUTs this node declined to scatter for `reason`.
fn declined(node: &Node, reason: &str) -> u64 {
    node.metrics
        .scatter
        .declined
        .with_label_values(&[reason])
        .get()
}

/// Observations this node recorded for one `phase` of a window's slot-hold, and their
/// summed seconds (`pacer_scatter_phase_seconds`).
///
/// Read off the child directly rather than through the exposition, because the counts
/// are what the assertions are about; a test that never observed a phase asserts on the
/// *other* nodes' counts instead of on this one's absence.
fn phase(node: &Node, phase: &str) -> (u64, f64) {
    let h = node
        .metrics
        .scatter
        .phase_seconds
        .with_label_values(&[phase]);
    (h.get_sample_count(), h.get_sample_sum())
}

/// Windows this node refused as an owner, by reason.
fn refusals(node: &Node, reason: &str) -> u64 {
    node.metrics
        .scatter
        .refusals
        .with_label_values(&[reason])
        .get()
}

/// This node's scatter series, as text. Put in a failure message so a test that
/// finds the scatter did not engage also says *why* — the declines are labeled by
/// reason precisely so that question has an answer (gate 3.13).
fn scatter_report(node: &Node) -> String {
    common::nonzero_lines(&node.metrics.encode(), "pacer_scatter")
}
