# ADR-0036: The daemon bounds its S3 listener and drains on SIGTERM

Date: 2026-09-08 · Status: **Accepted, on by default, no flag.** Every bound here is a default the
daemon now always applies; the knobs exist to widen or narrow them, not to switch the behaviour on.

Closes three gaps that were all the same gap — the front door had no bounds:

1. the accept loop had no connection cap, no header deadline and no idle deadline, so the daemon's
   memory scaled with whatever a client opened (quality item **R1**);
2. there was no `SIGTERM`/`SIGINT` handler anywhere, so a DaemonSet rolling update severed
   in-flight requests at the kernel level (**R5**);
3. logs were the human text formatter, unparseable by the pipeline that actually reads them
   (**R9**).

Complements the bounds ADR-0032 put on the *write* path (staging budget, window slots) and the
per-read bound ADR-0015 put on the fill path (`fill_parallelism × chunk_size`). Those made every
internal queue finite; this makes the *entry* finite, which is what they were assuming.

## Context

Every other queue in the daemon is bounded. Peer chunk streams are a bounded channel
(`PACER_PEER_CHANNEL_CAPACITY`); a multi-chunk read holds at most `fill_parallelism × chunk_size`
(ADR-0015); the scatter has a node-wide staging budget and a window-slot semaphore (ADR-0032);
client-pinned delivery memory has both a per-request and a node-wide ceiling (ADR-0026). The one
place work entered without a bound was `serve_s3`:

```rust
loop {
    let (stream, _) = listener.accept().await?;   // no cap, no timeout
    tokio::spawn(serve_connection(TokioIo::new(stream), service.clone()));
}
```

Three separate defects in five lines.

**No cap.** One task per connection, each able to hold a chunk-sized buffer on the fill path, so
resident memory is a function of client behaviour rather than configuration. A node whose daemon is
OOMKilled takes its whole cache tier with it, and the kill leaves nothing in the log — that has
happened (2026-08-21, exit 137, last line a routine directory sweep), which is why
`process_resident_memory_bytes` and the cgroup gauges exist at all.

**No deadline.** A connection that opened and sent nothing, or stalled mid-body, was held forever.
That is the classic slow-loris shape, but the reason it matters here is duller: a stalled request
holds a fill open, and the fill holds its buffers.

**`accept().await?`.** The `?` ends the loop on *any* error. `EMFILE` when the process runs out of
descriptors, `ENOBUFS` under memory pressure, `ECONNABORTED` when a peer goes away between SYN and
accept — all transient, all fatal to the S3 endpoint. And the pod would stay `Ready`, because
liveness is probed on the *admin* listener, which is a different socket. A node in that state
serves nothing and reports nothing.

**And no drain.** A `helm upgrade` rolls the DaemonSet: the kubelet sends SIGTERM, waits
`terminationGracePeriodSeconds`, then SIGKILLs. With no handler, SIGTERM's default disposition
terminates the process immediately — every in-flight GET is cut, and on a restore storm that is a
pod's whole share of a checkpoint turning into client-side errors, at the moment an operator was
most confident they were doing something safe.

## Decision

### 1. Three bounds on the listener, in the order a connection meets them

| bound | knob | default | why that value |
|---|---|---:|---|
| connection cap | `PACER_S3_MAX_CONNECTIONS` | **1024** | The Python client keeps one HTTP/1.1 connection per in-flight stream and sizes its botocore pool to match — `POOL_CONNECTIONS = 64` per rank (`clients/python/pacer_vllm.py`) — and a p5/p6 runs one rank per GPU, so eight ranks reach **512** sockets against the node-local daemon. vLLM's `EngineCore` is a separate process per rank with its own pool, and a save adds the writer's upload pool. 1024 is one doubling above the measured 512: the smallest headroom that still admits a second concurrent job. |
| header / keep-alive deadline | `PACER_S3_HEADER_TIMEOUT` | **120 s** | hyper arms this timer every time a connection *starts* reading a request head, including between keep-alive requests, so it is the idle-connection reaper too. That is why it is minutes: a torch loader pauses between batches while it copies the last one into HBM, and closing its socket in that gap trades a slow-header defence for reconnect churn against the node's ephemeral-port range. |
| no-progress deadline | `PACER_S3_IDLE_TIMEOUT` | **300 s** | Covers what the header deadline cannot see: a request body that stalls mid-upload, a response nobody reads, an HTTP/2 connection with no open stream. Enforced at the socket, and **reset on every byte in either direction**, so a slow-but-moving multi-GiB GET never trips it however long it runs. Generous on purpose — this is a leak reaper, and the cost of firing early is a failed checkpoint read. |

The cap is taken **before** `accept`, not after. A node at its ceiling therefore stops draining the
kernel backlog, which is the only backpressure a client's TCP stack understands without being
taught a new error — the alternative, accepting and then closing, converts a queueing condition
into a connection error the SDK retries. The consequence is that the ceiling is **invisible to the
client**, which is why it is visible in `/metrics`:

* `pacer_s3_connections_active` — gauge, compare against the cap;
* `pacer_s3_connections_at_capacity_total` — one increment per moment at the ceiling. Deliberately
  not named `*_rejected_total`: nothing is rejected, so a rejection counter would sit at zero while
  the node throttled every connect;
* `pacer_s3_accept_errors_total` — transient `accept` failures, retried after a 100 ms backoff.

A transient `accept` failure now logs, counts and backs off instead of ending the loop. It is not
infinitely patient: **64 consecutive** failures (~6.4 s) returns an error and restarts the pod,
because retrying forever on a permanently broken listener reproduces the original defect with extra
steps — a Ready pod serving nothing.

### 2. What drains, and the 20 s / 30 s pairing

SIGTERM and SIGINT raise one `tokio::sync::watch` flag. Both listeners watch it:

* the **S3 listener** stops accepting and hands its live connections to `hyper_util`'s
  `GracefulShutdown`, which lets each finish the request it is serving and then closes it rather
  than reusing it;
* the **peer gRPC server** moves from `serve` to `serve_with_shutdown`, which stops accepting and
  lets started RPCs finish. This half matters as much as the S3 half: the peer plane carries other
  nodes' chunk fetches, so a pod that stops answering them mid-stream turns one rolling update into
  a fallback storm on every node reading from it.

Then, in order: the background sweeps (re-announce, EFA handshake, staged-chunk reap, EndpointSlice
watch) are **aborted rather than awaited** — each is an infinite `interval` loop holding nothing a
client waits on, so awaiting them would simply burn the deadline; and the cache is closed **last**,
once nothing can still be writing to it, so foyer's in-flight flush and reclaim tasks are the only
thing left to wait for. The process then exits **0**: a non-zero exit during a rolling update is
reported as a crash-looping pod, which is a worse signal than a logged warning about a slow flush.

**The deadline is 20 s and the chart's `terminationGracePeriodSeconds` is 30 s, and the ordering is
the whole point.** A drain deadline at or above the grace period means the kubelet SIGKILLs the
process *during* its own drain — the same severed requests, just later, and now with a drain that
looks like it worked. The 10 s of margin is for the tail: the cache close waits on device latency,
not on this value. Whoever changes either number must change both; `PACER_SHUTDOWN_DRAIN_TIMEOUT`
and `config.shutdownDrainTimeoutSecs` name the pair in their own docs so the coupling is not folk
knowledge.

What is *not* covered, stated so the omission is a decision: the EFA completion reapers. Since
planning/19 D5 each rail owns a pinned thread with its own current-thread runtime and no
cancellation handle, and giving it one means changing `pacer-transport`. They hold no client work —
a holder's WRITE either completes or its requester falls back — so process exit is their teardown.

**A DaemonSet rolling update relies on all of this.** That is the only routine event that stops a
healthy daemon, it happens on every chart change, and until now it was indistinguishable from a
crash from the client's side.

### 3. JSON logs by default

`PACER_LOG_FORMAT` = `json` (default) | `text`. JSON with `flatten_event(true)`, so an event's own
fields sit at the top level and `kubectl logs | jq '.chunk_key'` works instead of
`.fields.chunk_key`; the current span comes along as one object, the full ancestor list does not.
`target` and thread identity are off in both encodings — the module path is not what identifies a
line, and a thread id is noise in a process that spreads its work over a tokio pool by design.

The **filter** stays `RUST_LOG`. It is `tracing_subscriber`'s own contract, the chart already
renders it from `config.logLevel`, and a `PACER_*` alias would be a second way to say one thing.

One wrinkle worth recording: the subscriber has to exist *before* `Config::load`, because `load`
itself warns about misconfigurations (an unpersistable chunk size, an undersized slab) that a
process with no subscriber drops on the floor. So the encoding is resolved on its own first, over
the same three layers, and leniently — an unreadable file or an unknown value yields `json` rather
than a silent non-zero exit. The strict check still happens moments later in `Config::load`, and
*that* error is logged, in the format this resolution chose.

## Consequences

* **A client at the ceiling waits instead of failing.** Correct, and invisible without the metrics
  above. An operator diagnosing "connects got slow" must look at
  `pacer_s3_connections_at_capacity_total` before anything else.
* **A stalled connection is now closed.** A client that relied on holding an idle socket for more
  than `PACER_S3_HEADER_TIMEOUT` will reconnect. The default is set above every inter-batch gap
  measured on the ladder, but it is a behaviour change, not a pure addition.
* **The idle deadline is the riskiest piece here**, because a wrong implementation cuts a
  legitimate slow transfer. It is enforced by a socket wrapper that resets its clock on every byte,
  and the regression has its own test arm (`active_transfer_survives_a_short_idle_deadline`, which
  reads a 32 MiB object over several times the deadline and requires the run to have outlasted the
  deadline before the assertion counts).
* **Text logs are one flag away**: `--set config.logFormat=text`, or `PACER_LOG_FORMAT=text` on a
  dev pod. Anything parsing the daemon's stdout as text needs updating; nothing in-tree does.
* **Config surface**: three new file blocks (`listen:`, `shutdown:`, `log:`) and five env vars, all
  with `0`/absent meaning "the built-in default" so the chart can emit them unconditionally and a
  benchmark arm can move one alone.
* `tracing-subscriber` gains its `json` feature (and with it `tracing-serde`) — one crate, no new
  supply-chain surface beyond `serde_json`, which the AWS SDK already brings.

## Alternatives considered

**Accept, then close over the cap.** Gives the client a fast, explicit failure. Rejected: the SDK
retries a connection error, so the fleet's response to one node being busy would be more
connections to that node. Backpressure at the accept queue makes the client's own TCP stack do the
waiting.

**One deadline instead of two.** hyper's header timer already covers the idle keep-alive case, so a
single knob is tempting. It cannot see a stalled *body* or an unread response, which is the case
that holds a fill open — the expensive one. Two knobs, two documented roles.

**Resetting the idle `Sleep` on every read.** Simpler to reason about, and puts a timer-wheel update
on the per-byte path. The wrapper instead records the activity instant and re-arms only when the
timer actually fires and finds the connection was busy — one clock read per poll, no timer work.

**A cancellation token crate (`tokio-util`).** A `watch<bool>` does what is needed with a dependency
already in the tree, and the flag is deliberately *not* `Clone` on the sender side: two owners would
each believe they decide when the process stops, and a drain that can start twice logs two
conflicting begin/end pairs for one shutdown.

**Loading the whole config before initialising logging**, so the encoding is a normal field. That
drops `Config::load`'s own warnings, which are the ones an operator most needs — a slab sized at or
below `mem_capacity` silently disables ADR-0028. Reading the config file twice at startup is
cheaper than losing them.
