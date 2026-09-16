# PACER Grafana dashboard

[`pacer-dashboard.json`](pacer-dashboard.json) is a Grafana dashboard model built **only**
from metrics the daemon actually exports at `/metrics` (see
[`crates/pacer-daemon/src/metrics.rs`](../../crates/pacer-daemon/src/metrics.rs)). Nothing on
it is aspirational — if a metric isn't emitted, there is no panel for it.

## Prerequisites

- **Grafana** 9.x or newer (the model uses `schemaVersion: 39`; older Grafana will import
  it but may drop a field or two).
- A **Prometheus datasource** scraping the daemon admin port (`ports.admin`, default
  `9090`) at `/metrics`. Scrape every daemon pod so the per-`instance` template variable
  works — e.g. a `PodMonitor`/`ServiceMonitor` selecting the DaemonSet, or a scrape config
  targeting the pods. foyer's own metrics land in the same registry, so they are available
  too (this dashboard sticks to the `pacer_*` daemon metrics).

  The chart ships no monitor object (one belongs to whoever owns the Prometheus, and a
  duplicate would double every scrape), so for the record: the Service this chart creates
  carries `app.kubernetes.io/name: pacer` and names the port `admin`, which is what a
  `ServiceMonitor` selects on — one per *release*, and every session's release shares that
  name label, so a single monitor covers them all:

  ```yaml
  spec:
    namespaceSelector: { matchNames: [pacer] }
    selector:
      matchLabels: { app.kubernetes.io/name: pacer }
    endpoints:
      - port: admin
        path: /metrics
        interval: 15s        # see the delivery-row caveat: too coarse for a single arm
  ```

## Importing

1. Grafana → **Dashboards → New → Import**.
2. Upload `pacer-dashboard.json` (or paste its contents).
3. When prompted, select your Prometheus datasource for the `datasource` variable.
4. Save. Use the **Instance** template variable at the top to focus on one node or view
   the fleet (`All`).

To provision it automatically instead, drop the JSON into a Grafana dashboard-provider
path or a `grafana_dashboard`-labeled ConfigMap (Grafana Operator / kube-prometheus-stack
sidecar convention).

## Panels and the metrics behind them

| Row | Panel | Metric(s) |
|---|---|---|
| Client-facing | Cache hit ratio | `pacer_cache_hits_total`, `pacer_cache_misses_total` |
| Client-facing | Read throughput (bytes/s) | `pacer_bytes_from_cache_total`, `pacer_bytes_from_peers_total` |
| Client-facing | S3 operations/s by op | `pacer_ops_total` (`op` label) |
| Client delivery | Delivered bandwidth (bytes/s) | `pacer_delivery_chunk_bytes_total` (`source` label) |
| Client delivery | Delivered chunks/s by source | `pacer_delivery_chunks_total` (`source` label) |
| Client delivery | Delivery requests & rejects/s | `pacer_delivery_requests_total`, `pacer_delivery_rejects_total` (`reason`) |
| Client delivery | Announce race: retries & declines/s | `pacer_delivery_unknown_peer_retries_total`, `..._declines_total` |
| Client delivery | Delivered share of GETs | `pacer_delivery_requests_total`, `pacer_ops_total{op="get_object"}` |
| Client delivery | Client pinning & registrations | `pacer_delivery_pinned_bytes`, `pacer_delivery_registrations_total` |
| EFA rails | Per-rail WRITE bandwidth (bytes/s) | `pacer_rdma_rail{metric="write_bytes_total"}` |
| EFA rails | WRITEs in flight per rail | `pacer_rdma_rail{metric="writes_in_flight"}` |
| EFA rails | Staging pool occupancy per rail | `pacer_rdma_rail{metric="staging_in_use"\|"staging_ranges"}` |
| EFA rails | Mean WRITE completion wait | `pacer_rdma_write_completion_wait_seconds_total`, `pacer_rdma_rail{metric="writes_total"}` |
| EFA rails | Rails up | `pacer_rdma_rails` |
| EFA rails | Rail health: CQ errors & WRITE sources | `pacer_rdma_cq_errors_total`, `pacer_rdma_write_sources_held`, `..._orphaned_total` |
| Cache tier | GET outcomes/s | `pacer_cache_hits_total`, `pacer_cache_misses_total`, `pacer_cache_bypass_total` |
| Cache tier | Cache fills/s | `pacer_fills_completed_total`, `pacer_fills_aborted_total` |
| Cache tier | Cache fill throughput | `pacer_bytes_filled_total` |
| Cache tier | Backend read retries & failures/s | `pacer_backend_read_retries_total`, `pacer_backend_read_failures_total` (`outcome`) |
| Peer plane | RDMA-served fraction | `pacer_peer_serves_rdma_total`, `pacer_peer_serves_total` |
| Peer plane | Peer fetches & fallbacks/s | `pacer_peer_fetches_total`, `pacer_peer_fallbacks_total` |
| Peer plane | Peer serves, misses & admits/s | `pacer_peer_serves_total`, `pacer_peer_misses_total`, `pacer_peer_readthroughs_total`, `pacer_local_admits_total` |
| Peer plane | Peer byte throughput | `pacer_bytes_from_peers_total`, `pacer_bytes_to_peers_total` |
| Peer plane | RDMA copy time | `pacer_rdma_holder_copy_seconds_total`, `pacer_rdma_requester_copy_seconds_total` |
| Directory | Directory RPC latency by op | `pacer_dir_rpc_seconds` (histogram, `op` label) |
| Directory | Directory RPC rate by op | `pacer_dir_rpc_seconds_count` (`op` label) |
| Resources | Ring members | `pacer_ring_members` |
| Resources | Daemon CPU (cores) | `process_cpu_seconds_total` |
| Resources | Daemon memory | `process_resident_memory_bytes`, `pacer_malloc_in_use_bytes`, `pacer_malloc_free_retained_bytes`, `pacer_cache_slab_bytes` |
| Resources | Cgroup memory vs its limit | `pacer_cgroup_memory_current_bytes`, `pacer_cgroup_memory_max_bytes`, `pacer_cgroup_memory_file_bytes`, `pacer_cgroup_memory_anon_bytes`, `pacer_cgroup_memory_oom_total` |
| Resources | Cache slab (ADR-0028) | `pacer_cache_slab_frames_in_use`, `pacer_cache_slab_stores_total`, `pacer_cache_slab_heap_fallbacks_total` |

## Reading the delivery rows (ADR-0026 / ADR-0030)

Four things about these series are counter-intuitive enough that a panel alone will mislead:

- **`pacer_delivery_bytes_total` is not a bandwidth series** and no panel uses it. It advances
  once per *request*, when a span's last window lands, so a `rate()` over it measures
  completions bunching: it reported **53.6 GiB/s on a single ~11 GiB/s rail** (2026-08-24),
  which is how the flaw was caught. `pacer_delivery_chunk_bytes_total` advances as each chunk
  lands and is what the bandwidth panel plots.
- **A scrape interval coarser than the arm cannot show the arm.** A 131 GiB checkpoint restore
  at ~18 GiB/s is over in ~7 s; at the conventional 15 s `ServiceMonitor` interval that is one
  or two samples, and every rate is an average across the idle time around it. For a
  measurement, either drop the interval (`interval: 1s` on a bench-only `ServiceMonitor`) or
  use `clients/python/pacer_metrics_sampler.py`, which is what the C5/C3 arms report from.
- **A 1 s scrape is necessary and not sufficient — Grafana averages independently.** Every rate
  here is `rate(...[$__rate_interval])`, and Grafana computes that as
  `max($__interval + min step, 4 × min step)`, where **`min step` is the datasource's
  configured "Scrape interval" field, not the interval Prometheus actually scrapes at**. That
  field defaults to `15s`, which puts a floor of **60 s** under every panel — so a 7-second
  restore reads as ~2 GiB/s no matter how granular the stored samples are. `$__interval` is the
  second half of it: `time range ÷ maxDataPoints`, so widening the range re-inflates the window
  even with the floor fixed. Every panel here therefore pins `interval: "1s"` and
  `maxDataPoints: 3000`, which holds the rate window at 4 s independently of datasource config,
  and the model defaults to a 1 h range refreshing every 10 s. **Copy those two fields into any
  panel you add** — the delivery and rail rows were added on 2026-08-24/25 without them and a
  line-rate 70B restore still read as almost nothing. (If a 32-rail panel is heavy on your
  Prometheus, lower its `maxDataPoints` rather than dropping `interval`; the floor is what
  matters.) The one exception is *Directory RPC latency by op*, whose percentiles come from
  histogram buckets that are legitimately sparse in a 4 s window.
- **Zeros can mean "not accounted", not "not happening".** Both delivery gauges had exactly
  that failure: `pacer_rdma_rail{metric="write_bytes_total"}` counted only peer serves until
  2026-08-24, and `writes_in_flight` / `write_completion_wait_seconds` until 2026-08-25 — so a
  checkpoint delivery moved 131 GiB with **176 consecutive samples of `writes_in_flight` flat
  at zero** while `pacer_delivery_chunks_total` advanced. If a panel here is flat while another
  says bytes are moving, suspect the accounting before the fabric.

The two panels worth pairing: **WRITEs in flight per rail** and **Mean WRITE completion wait**
are Little's law. Depth ÷ hold time is the rate a rail can sustain, so a low depth with a short
hold says the pipe was never filled — and **look at the client first, because
`delivery.parallelism` is usually not the answer**. A delivered GET fans out over the windows of
one request (`min(parallelism, windows)`), so a reader asking for 512 MiB of 16 MiB chunks gives
the daemon 32 chunk resolutions no matter how high the setting is; `pacer_delivery_inflight_chunks_peak`
against that arithmetic says which side is short, and `pacer_delivery_chunk_seconds{source}` says
what each chunk cost while it was. Meanwhile
**Staging pool occupancy** pinned at 1.0 says the source side is the constraint — that pool is
`HOLDER_ARENA_RANGES / rails`, i.e. 8 ranges per rail on a 32-rail node, and every WRITE whose
source is not an ADR-0028 slab frame has to lease one and memcpy into it.

## Reading the backend-read row

The two series on **Backend read retries & failures/s** are the same event — a chunk's backend
read that did not work first time — split by whether PACER absorbed it, and they are read in
opposite directions:

- **`retried` going up is the retry working, not a fault.** It counts retried *attempts*, so one
  read that succeeded on its third try adds 2. Do not alert on it; it is the backend's flakiness
  made visible, and every increment is a client GET that stayed intact because of it. Its cost is
  one re-read of one chunk (`chunk_size` bytes) plus a jittered backoff — under the default
  3-attempt policy, at most ~150 ms of added latency for that chunk.
- **`failed` going up is a client being served short, and is the one to page on.** With
  `outcome=exhausted` the backend stayed unhealthy across the whole backoff window; with
  `outcome=permanent` the fault is a 4xx that retrying cannot fix — this daemon's credentials or
  request shape, so look here rather than at S3. A `NoSuchKey` is deliberately *not* counted: a
  404 is an answer.

Why the pair exists at all: before the retry (`crates/pacer-backend/src/retry.rs`) a single
transient S3 streaming error truncated the whole client GET, and because the `200` and
`Content-Length` had already gone out with the headers, the only trace was one `warn!` line in
the daemon log. Nothing on this dashboard moved.

## Caveats (what is *not* here, and why)

- **No client latency / TTFB panel.** The daemon does not currently export a
  request-latency histogram, so the dashboard has no TTFB or p99-latency panel — adding one
  would mean inventing a metric that isn't emitted. If a latency histogram is added later,
  a "TTFB" panel belongs in the Client-facing row.
- **No fan-out panel, on purpose.** `pacer_delivery_inflight_chunks_peak` and
  `pacer_delivery_chunk_seconds` are exported and referenced above, but neither is panelled:
  the peak is a high-water mark (a time series of it is a staircase, and a 15 s scrape lands
  between the spans that made it), and the latency split matters against a specific window and
  chunk size rather than as a fleet trend. `bench/ladder/dcp.sh` prints both into every
  transcript, beside the arithmetic they have to be read against — which is where the C4 arms'
  evidence lives.
- **RDMA copy-time panels stay flat at 0** on nodes that were not built with the `efa`
  feature or where the EFA transport never came up (`pacer_rdma_*_copy_seconds_total` are
  registered and refreshed only under `#[cfg(feature = "efa")]` with a wired transport).
  This is expected on gRPC-only / non-EFA deployments.
- **Peer-plane, directory, and ring panels are empty on a single-node install** (cluster
  tier off): those counters only advance when `PACER_NODE_NAME` is set and the ring has
  peers.
- **Per-peer byte breakdown** is not available as a label — `pacer_bytes_to_peers_total` /
  `pacer_bytes_from_peers_total` are unlabeled aggregates, so the "per-peer bytes" view is
  the fleet total split by `instance` (the serving/requesting node), not by remote peer.
