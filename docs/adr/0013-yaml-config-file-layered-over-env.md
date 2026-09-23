# ADR-0013: YAML config file (ConfigMap-mounted), layered under env-var overrides

Date: 2026-07-12 · Status: Accepted (implemented)

## Context

Configuration today is env-vars-only: ~18 `PACER_*` variables, defined once in
`crates/pacer-daemon/src/config.rs` and rendered into the DaemonSet by the Helm chart from
`values.yaml` (`config:` block). This worked for Phase 1–2 but is straining:

- **Flat namespace.** Env vars can't express structure. The cluster block
  (`PACER_NODE_NAME` / `PACER_PEERS` / `PACER_PEER_SERVICE` / `PACER_NAMESPACE`) already encodes a
  tagged union through the *presence* of variables — implicit coupling that grows worse as
  Phase 3 adds transport tuning (EFA buffer pools, capability knobs) and Phase 4 adds
  replication/warming policy.
- **Tuning knobs are trapped in consts.** The style sweep (CLAUDE.md "Coding style") named
  the magic numbers, which exposed that several are *tunables*, not constants: io_uring
  worker threads and submission depth (`URING_THREADS`/`URING_IO_DEPTH` in pacer-cache),
  peer-stream channel capacities, tokio worker thread count (currently runtime default).
  Promoting each to yet another env var scales linearly in chart template noise.
- **Chart double-bookkeeping.** Every new knob touches four places: const/struct,
  `from_env`, `daemonset.yaml` env block, `values.yaml`. A file collapses the middle two.
- **Diffability/auditability.** A ConfigMap is one kubectl-diffable object; 20 env vars
  interleaved with downward-API entries are not.

Options considered for format and mechanics:

1. **TOML** — Rust-native (Cargo idiom), serde support first-class. But the operator surface
   of this project is Kubernetes: values.yaml → ConfigMap → file is zero-friction when the
   file is YAML (`toYaml` in the chart template renders the values block verbatim). A TOML
   file would need translation templating and gives operators a second syntax to learn.
2. **YAML** — congruent with the entire deployment surface (Helm values, K8s manifests).
   Serde ecosystem note: `serde_yaml` is archived/unmaintained; use a maintained
   fork-equivalent (`serde_yaml_ng` or `serde-norway`) — API-identical, actively released.
3. **Env-only, more vars** — status quo; rejected per above.
4. **Config crate with built-in layering** (`figment`, `config-rs`) — does file+env merging
   for us, but drags a dependency with its own conventions into a daemon that needs exactly
   one merge rule; hand-rolled layering over serde `Option<T>` fields is ~50 lines and keeps
   `EnvVar` (ADR-scoped in config.rs) as the single env authority.

Hard constraint: some values **cannot** live in a file. `PACER_NODE_NAME` and
`PACER_NAMESPACE` come from the Kubernetes downward API (per-pod, injected as env);
a ConfigMap is per-release, identical on every node. Any design must keep env in the loop.

## Decision

Add an optional **YAML config file**, path given by **`PACER_CONFIG`** (default:
`/etc/pacer/config.yaml` when present, else no file). Precedence, lowest → highest:

```
built-in defaults  <  config file  <  PACER_* env vars
```

- **Env always wins.** Existing deployments, the dev loop, and downward-API injection keep
  working unchanged; the file is a new middle layer, not a replacement. No deprecation of
  any `PACER_*` variable.
- **Schema mirrors the existing `Config` struct** (serde, `deny_unknown_fields` so typos
  fail fast at startup, kebab-case keys):

  ```yaml
  listen-addr: 0.0.0.0:9000
  admin-addr: 0.0.0.0:9090
  cache:
    dir: /var/cache/pacer
    mem-capacity: 1GiB
    disk-capacity: 100GiB
    block-size: 1GiB
    io-engine: uring        # psync | uring
    uring:                  # NEW: previously const
      threads: 4
      io-depth: 256
  runtime:                  # NEW: previously tokio defaults
    worker-threads: 0       # 0 = tokio default (one per core)
  policy:
    min-object-size: 4MiB
    max-object-size: 8GiB
  backend:
    endpoint: ""
    force-path-style: false
  auth:
    placeholder-access-key: pacer
    placeholder-secret-key: pacer
  bucket-map:
    cache: my-bucket--use2-az1--x-s3
  cluster:                  # presence of the block = enabled (node-name still env-only)
    peer-listen-addr: 0.0.0.0:9100
    channel-capacity: 8     # NEW: previously const (peer/relay chunk channels)
  ```

- **Newly configurable knobs** (const → config, defaults unchanged): uring threads and
  io-depth, tokio worker threads, peer/relay channel capacity.
- **Flush-gated config (Phase 3):** `chunk_size` (ADR-0015) is configurable per
  *cluster* but is **not** freely mutable — it is embedded in chunk cache keys, so
  changing it orphans all entries and must never differ across a rolling update
  (ADR-0015); it must also stay under tonic's decode limit. It lives in the config
  file like any knob but carries a "flush on change, pin per cluster" contract,
  distinct from the hot-tunable knobs above.
- **Deliberately NOT configurable:** wire/ownership stability constants —
  `PROTOCOL_VERSION`, the ADR-0014 hash seed/separator (see the pinned-score test in
  pacer-ring) and the directory ABI (ADR-0020) — and the ring gauge sample interval
  (observability detail). The bar for promotion: *an operator could plausibly need a
  different value per cluster/workload* (and, for flush-gated values, accept the
  flush). Everything else stays a documented const per CLAUDE.md style.
- **Chart:** `values.yaml` `config:` block renders into a ConfigMap (`toYaml`, near-1:1),
  mounted at `/etc/pacer/config.yaml`; the DaemonSet env block shrinks to what is genuinely
  per-pod or per-release-secret: downward API (`PACER_NODE_NAME`, `PACER_NAMESPACE`),
  `AWS_REGION`, `RUST_LOG`, dev-mode extras. Config changes = ConfigMap update + rollout
  restart (no hot reload — see below).
- **No hot reload.** The daemon reads the file once at startup. Most knobs (cache sizing,
  io engine, ports) can't safely change live anyway; a rollout restart through the
  DaemonSet is the one consistent update path. Revisit only if a Phase 4 policy knob
  demands it.
- `config.rs` stays the single authority: `EnvVar` consts as today, plus one serde
  `FileConfig` struct of `Option<T>` fields and a `merge(defaults, file, env)` that is the
  only place precedence exists.

## Consequences

- Operators tune everything from `values.yaml` exactly as today — the chart contract
  doesn't change shape; only the transport (ConfigMap file vs env block) changes.
- Bare-metal / non-K8s runs (dev loop, integration tests) get a readable single-file
  config instead of a wall of exports; env overrides keep per-test tweaks cheap.
- Startup must log the *effective* merged config (already does via `info!(?cfg)`) and fail
  fast on unknown keys — a mistyped YAML key must not silently fall back to a default.
- Two sources of truth per value (file + env) is real complexity; contained by the single
  merge function and by CI: the correctness suite gains a config-precedence test
  (file-only, env-override, unknown-key rejection, cluster-block-without-node-name error).
- The `serde_yaml` situation (unmaintained upstream) is a supply-chain consideration:
  pin the chosen fork in workspace deps; cargo-deny already gates advisories (ADR-0010).
- Phase 3/4 knobs get a structured home instead of another env-var generation:
  `chunk` (ADR-0015), `replication` (ADR-0016), `directory` (ADR-0017), and `rdma`
  (ADR-0018/0019/0020) config blocks slot under the existing schema. Each such ADR
  owns its knobs' defaults and provenance; this ADR owns only the merge rule and the
  promotion bar.
