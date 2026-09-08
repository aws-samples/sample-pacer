# S3 PACER — common dev steps. `make help` lists targets.
#
# Local build/test needs only the Rust toolchain. Cross-compile targets
# (dev-push) additionally need zig + cargo-zigbuild + the Linux target —
# see scripts/dev/README.md (one-time setup).

.DEFAULT_GOAL := help

# Deliberately no PATH games here: build/test use the ambient cargo. Two
# toolchains coexist on dev Macs (Homebrew rust + rustup for the Linux
# cross-target) and mixing them in one target/ dir breaks builds (E0514);
# only scripts/dev/pacer-dev needs rustup, and it sets its own PATH.

## Build & test ---------------------------------------------------------------

.PHONY: build
build: ## Debug build of the whole workspace
	cargo build --workspace

.PHONY: release
release: ## Release build of pacer-daemon (native arch)
	cargo build --release -p pacer-daemon

.PHONY: test
test: ## Run all unit/integration tests
	cargo test --workspace

.PHONY: unit
unit: ## Inner-loop tests only: cargo test --lib --bins (skips crates' tests/*.rs integration binaries)
	cargo test --workspace --lib --bins

.PHONY: check
check: ## Fast typecheck of everything (no codegen)
	cargo check --workspace --all-targets

.PHONY: smoke
smoke: ## Boot the debug daemon and probe /healthz like kubelet does
	cargo build -p pacer-daemon
	ci/functional-smoke.sh target/debug/pacer-daemon

## CI gate (same checks the pipeline runs) ------------------------------------

.PHONY: fmt
fmt: ## Format all code in place
	cargo fmt --all

.PHONY: lint
lint: ## fmt --check + clippy -D warnings + helm lint
	cargo fmt --all --check
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	helm lint deploy/helm/pacer

.PHONY: doc
doc: ## Build rustdoc with warnings denied (CI parity)
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

.PHONY: deny
deny: ## License/advisory/ban checks (needs cargo-deny)
	cargo deny check

.PHONY: ci
ci: lint test doc deny smoke ## Everything CI gates on, locally, in order

## Docker (local one-off; CI builds the real multi-arch image) ----------------

.PHONY: image
image: ## Self-contained local image build (needs docker)
	docker build -t pacer .

## The one entry point (scripts/dev/dev — see scripts/dev/README.md) -----------

# Every `dev-*` target below still works and still calls its script directly — they are
# thin aliases, and nothing that already types one has to change. `dev` is the front
# door: one verb set over the same scripts, with `dev doctor` as the preflight and
# `dev help` as the map. Quality item G1b.

.PHONY: dev
dev: ## The dev-loop entry point: prints its verbs (dev doctor, dev build, dev fleet, …)
	scripts/dev/dev help

## Cluster dev loop (scripts/dev/pacer-dev — see scripts/dev/README.md) --------

# make dev-up N=2 → two nodes (default 1)
# make dev-up EFA=1 → a real cache node with its EFA device (~$1.5/hr) instead of the
# default general node (~$0.25/hr). EFA passes straight through as an env var, and so
# does SPOT: every dev node is spot unless you pass SPOT=0 (scripts/dev/README.md).
N ?= 1

.PHONY: dev-up
dev-up: ## Bring up + claim N node(s): general pool by default, EFA=1 for a cache node
	scripts/dev/pacer-dev up $(N)

.PHONY: dev-deploy
dev-deploy: ## Deploy the DaemonSet in dev mode (supervisor, no image)
	scripts/dev/pacer-dev deploy

.PHONY: push
push: dev-push ## Alias for dev-push

.PHONY: dev-push
dev-push: ## Cross-compile + hot-swap the binary into running pods
	scripts/dev/pacer-dev push

.PHONY: dev-status
dev-status: ## Nodes, pods, and which binary sha each pod runs
	scripts/dev/pacer-dev status

.PHONY: dev-logs
dev-logs: ## Follow daemon logs (first pod)
	scripts/dev/pacer-dev logs

.PHONY: dev-down
dev-down: ## Restore the image-based deploy and release the nodes
	scripts/dev/pacer-dev down

## EFA spike dev loop (scripts/dev/efa-dev — see scripts/dev/README-efa.md) --

# ECR, not GitLab (spike/efa/README.md finding #4 — cache nodes have no
# imagePullSecret). :dev is a fixed tag reused across `dev-efa-up` builds so
# the dev loop never needs a new tag per iteration (push swaps the binary,
# not the image); override for a scratch tag of your own.
EFA_DEV_IMAGE ?= 123456789012.dkr.ecr.us-east-2.amazonaws.com/pacer-efa-spike:dev
export EFA_DEV_IMAGE

# make dev-efa-logs PODS=server → server pod's logs (default: client, S4-S6)
PODS ?= client

.PHONY: dev-efa-up
dev-efa-up: ## Build (if needed) + deploy the EFA spike server/client dev pods
	scripts/dev/efa-dev up

.PHONY: dev-efa-push
dev-efa-push: ## Build efa-spike in-container + hot-swap into both dev pods
	scripts/dev/efa-dev push

.PHONY: dev-efa-status
dev-efa-status: ## Dev pods and which binary sha each is running
	scripts/dev/efa-dev status

.PHONY: dev-efa-logs
dev-efa-logs: ## Follow client logs (S4-S6 verdict; PODS=server for the other)
	scripts/dev/efa-dev logs efa-dev-$(PODS)

.PHONY: dev-efa-down
dev-efa-down: ## Delete the dev pods (Karpenter reclaims the EFA nodes)
	scripts/dev/efa-dev down

## Remote build pod (scripts/dev/build-pod — see scripts/dev/README-build-pod.md)

# The `--features efa` path cannot be compiled on macOS at all (ibverbs-sys
# vendors rdma-core and links libefa/libibverbs), so it has to build on Linux.
# These targets run that build on a throwaway cluster node with a persistent
# cargo cache instead of in a VM on the Mac — no local disk, and the same image
# CI uses. BUILD_POD_ARCH=arm64 to target Graviton instead of the x86 default.
#
# ONE POD PER WORKTREE: the pod is named after the branch, so these targets never
# collide between parallel sessions (CLAUDE.md's worktree protocol). That means a
# node per worktree, so `dev-build-list` prices the fleet and `dev-build-reap`
# collects the pods whose worktree is gone.

.PHONY: dev-build-up
dev-build-up: ## Create this worktree's build pod + persistent cargo cache volume
	scripts/dev/build-pod up

.PHONY: dev-build-check
dev-build-check: ## Sync + cargo check --all-targets --all-features, remotely
	scripts/dev/build-pod check

.PHONY: dev-build-test
dev-build-test: ## Sync + cargo test --workspace, remotely
	scripts/dev/build-pod test

.PHONY: dev-build-unit
dev-build-unit: ## Sync + cargo test --workspace --lib --bins, remotely (unit tests only, no tests/*.rs binaries)
	scripts/dev/build-pod unit

.PHONY: dev-build-test-efa
dev-build-test-efa: ## Sync + cargo test --workspace --all-features (what CI's rust-efa runs), remotely
	scripts/dev/build-pod test-efa

.PHONY: dev-build-lint
dev-build-lint: ## Sync + fmt --check and clippy -D warnings incl. --features efa
	scripts/dev/build-pod lint

.PHONY: dev-build-spike
dev-build-spike: ## Sync + the exact check the spike-efa-check CI job runs
	scripts/dev/build-pod spike

.PHONY: dev-build-swap
dev-build-swap: ## Release-build --features efa remotely + hot-swap into pacer pods
	scripts/dev/build-pod swap

.PHONY: dev-build-shell
dev-build-shell: ## Interactive shell in the remote build pod
	scripts/dev/build-pod shell

.PHONY: dev-build-status
dev-build-status: ## This worktree's pod, its node, and cache volume usage
	scripts/dev/build-pod status

.PHONY: dev-build-list
dev-build-list: ## Every build pod/cache volume in the cluster, and what it costs
	scripts/dev/build-pod list

.PHONY: dev-build-reap
dev-build-reap: ## Show pods whose worktree is gone or idle (APPLY=1 to delete)
	scripts/dev/build-pod reap $(if $(APPLY),--apply,)

.PHONY: dev-build-down
dev-build-down: ## Delete the build pod, KEEP the cache (PURGE=1 to drop it too)
	scripts/dev/build-pod down $(if $(PURGE),--purge,)

## Local daemon (scripts/dev/local-daemon — see scripts/dev/README-local.md) --------

# Runs pacer-daemon as two plain OS processes on THIS Mac, against the in-repo
# `fake_s3` example instead of a real S3 bucket — no cluster, no build pod, no EFA.
# Both binaries are single-crate, default-feature `cargo build -p pacer-daemon`,
# which scripts/dev/no-host-toolchain already allows natively.

.PHONY: dev-local-up
dev-local-up: ## Build + start fake_s3 and pacer-daemon locally
	scripts/dev/local-daemon up

.PHONY: dev-local-smoke
dev-local-smoke: ## PUT+GET round trip through the local daemon, plus /healthz + /metrics
	scripts/dev/local-daemon smoke

.PHONY: dev-local-logs
dev-local-logs: ## Tail both local logs (or one: scripts/dev/local-daemon logs fake|daemon)
	scripts/dev/local-daemon logs

.PHONY: dev-local-down
dev-local-down: ## Stop both local processes (PURGE=1 also wipes their scratch state)
	scripts/dev/local-daemon down $(if $(PURGE),--purge,)

## Python client suites (scripts/dev/py-devpod — see that script's header) ----------

# The client modules are a torch codebase and the dev Mac has no torch — and a Mac wheel
# would be the wrong torch anyway, since every suite asserts against behaviour the LADDER's
# baked loader image pins. So they run in a pod on that image. One pod per worktree, same
# rule as the build pod; it is 2 vCPU, so it lands on a general node that already exists.

.PHONY: dev-py-up
dev-py-up: ## Create this worktree's python devpod on the loader image
	scripts/dev/py-devpod up

.PHONY: dev-py-test
dev-py-test: ## Sync clients/python + run its suites remotely (FILES=... for a subset)
	scripts/dev/py-devpod test $(FILES)

.PHONY: dev-py-shell
dev-py-shell: ## Interactive shell in the python devpod, in /scripts
	scripts/dev/py-devpod shell

.PHONY: dev-py-status
dev-py-status: ## The python devpod, its node, its image and torch version
	scripts/dev/py-devpod status

.PHONY: dev-py-down
dev-py-down: ## Delete the python devpod (it holds no volume, so nothing survives)
	scripts/dev/py-devpod down

## Cluster sessions (scripts/dev/pacer-session — see scripts/dev/README-session.md) ---

# Several sessions run fleets on one cluster; every fleet and every node belongs to
# exactly one worktree's session token (ADR-0029). `ls` before you start, `reap` for
# what a dead session left holding a node.

.PHONY: dev-session-ls
dev-session-ls: ## Who else is live on the cluster, and what they own
	scripts/dev/pacer-session ls

.PHONY: dev-session-reap
dev-session-reap: ## Collect nodes/launchers/releases of GONE sessions (APPLY=1 deletes)
	scripts/dev/pacer-session reap $(if $(APPLY),--apply,)

## Housekeeping ---------------------------------------------------------------

.PHONY: clean
clean: ## Remove build artifacts
	cargo clean

.PHONY: help
help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

## Benchmark harness (in-cluster ladder Job — see bench/incluster/README.md) ---
#
# Thin front-end over workstream B2's submission path: it ONLY renders
# bench/incluster/k8s/job.yaml with envsubst and `kubectl apply`s it (plus the
# one-time RBAC and the operator-side node up/down that B2 deliberately keeps
# OUT of the Job). It re-implements NO ladder logic — the Job runs the same
# bench/ladder/run.sh, and its exit status IS the ladder's pass/fail verdict.
#
# Node count is NOT a Job knob: the Job discovers whatever EFA nodes are up
# (bench_nodes), and provisioning is operator-driven (`bench-up`/`down`), so N
# below drives `bench-up` and is threaded into LADDER_RUN_ID for traceability —
# it is not sent to the Job as a node count. Run order for a fresh rung:
#   make bench-rbac                 # once per cluster
#   make bench-up   N=3             # operator provisions the EFA nodes
#   make bench      RUNG=3 N=3      # submit the Job (needs a built ORCH_IMAGE)
#   make bench-logs                 # follow to the verdict
#   make down                       # operator reclaims the nodes ($$/hr!)

# The Job/RBAC/pods all live in this namespace; MUST match job.yaml's
# metadata.namespace and the daemon release namespace (bench/b4 BENCH_NAMESPACE).
BENCH_NAMESPACE ?= pacer
# The Job's fixed name; MUST match job.yaml metadata.name. It is fixed (not
# per-run), so a re-submit deletes the prior Job first (a Job's pod template is
# immutable — `apply` over a finished one would be rejected).
BENCH_JOB ?= pacer-ladder
# Label selector the Job stamps on its pod (job.yaml template labels) — used to
# list the orchestrator pod for status.
BENCH_SELECTOR ?= app.kubernetes.io/name=pacer-bench

# B2 interface files (paths are repo-root-relative; this Makefile runs there).
BENCH_JOB_MANIFEST  ?= bench/incluster/k8s/job.yaml
BENCH_RBAC_MANIFEST ?= bench/incluster/k8s/rbac.yaml
# Operator-side ladder orchestrator: its `up`/`down` (node provisioning) are the
# steps the in-cluster Job refuses by design, so they run from here.
BENCH_LADDER_RUN    ?= bench/ladder/run.sh

# General-nodepool arch the Job lands on. arm64 because that nodepool is Graviton
# (r8g) and the daemon images are -arm64 — ADR-0009 requires an explicit arch
# selector, never "whatever schedules". Override for an amd64 general nodepool.
ORCH_ARCH ?= arm64
# The pushed orchestration image (bench/incluster/Dockerfile). REQUIRED by B2 —
# it is also the seed pods' image (LADDER_SEEDER_IMAGE) — and has NO default: no
# account-ID/ECR path is baked in here (a separate secret-scrub workstream owns
# the one hardcoded ECR line in this file). Build+push per bench/incluster/
# README.md, then pass ORCH_IMAGE=<registry>/s3-pacer-bench:<tag>. Guarded
# below so `make bench` fails loudly (not on-cluster) when it is unset.
ORCH_IMAGE ?=

# Which rung to run (1|2|3). Default 1: the simplest, cheapest, always-valid
# rung (2 nodes, one edge). Rungs 2 and 3 need N>=3 (a real ring / fan-in).
RUNG ?= 1
# LADDER_STEPS per rung = shared `deploy` + that rung's seed + the rung itself,
# using run.sh's own step names (rung2 seeds with seed2, rung3 with seed3 — the
# README's per-rung recipe). `deploy` is an idempotent fast-path, so including
# it every time is a logged no-op when the live config already matches.
BENCH_STEPS_1 := deploy seed rung1
BENCH_STEPS_2 := deploy seed2 rung2
BENCH_STEPS_3 := deploy seed3 rung3
BENCH_STEPS   ?= $(BENCH_STEPS_$(RUNG))
# Seed-only variant of the above (no rung step) for `make seed`.
BENCH_SEED_STEPS_1 := deploy seed
BENCH_SEED_STEPS_2 := deploy seed2
BENCH_SEED_STEPS_3 := deploy seed3
BENCH_SEED_STEPS   ?= $(BENCH_SEED_STEPS_$(RUNG))

# Run label threaded to LADDER_RUN_ID (the rung/probe steps require a <label>;
# the entrypoint passes this). Encodes RUNG+N so a run's logs/result files are
# self-identifying. Deterministic (no timestamp) so `make -n` needs no shell;
# the fixed Job name already forces one run at a time anyway.
BENCH_RUN_ID ?= r$(RUNG)-n$(N)

# Optional ladder knobs — left EMPTY on purpose: an empty envsubst value makes
# each LADDER_* fall back to its run.sh default (NOBJ 512, CHUNK/OBJECT 16 MiB,
# MEM_CAPACITY 128 MiB). Override only to change the workload shape.
LADDER_NOBJ         ?=
LADDER_CHUNK_SIZE   ?=
LADDER_MEM_CAPACITY ?=
# Daemon image tag. Empty => run.sh default (1e543796-arm64, which carries the
# AhCache concurrent-fan-in fix rung 3 requires). Set 808a49a4-arm64 for a
# probe-cpu at fan-in >= ~4 (the serve-admission OOM fix). Bench rungs 1-3 are
# fine on the default.
LADDER_IMAGE_TAG    ?=
# D2 (ADR-0023): the backend the daemon fronts. Empty => run.sh default (express
# against the pinned directory bucket). For the Standard arm, set
# LADDER_BACKEND_TYPE=standard and BENCH_REAL_BUCKET=<regional bucket> (see
# bench/ladder/standard-backend.md). Both must be threaded through envsubst
# below, else job.yaml's ${...} placeholders leak as literal env values.
LADDER_BACKEND_TYPE ?=
BENCH_REAL_BUCKET   ?=

.PHONY: bench-rbac
bench-rbac: ## Apply the Job's RBAC (ServiceAccount/Role/binding) — once per cluster
	kubectl apply -f $(BENCH_RBAC_MANIFEST)

.PHONY: bench-up
bench-up: ## Operator-side: provision N EFA cache nodes for the bench (run.sh up N)
	$(BENCH_LADDER_RUN) up $(N)

.PHONY: bench
bench: ## Render + submit the in-cluster ladder Job for RUNG (1|2|3); needs ORCH_IMAGE + nodes up
	@[ -n "$(strip $(BENCH_STEPS))" ] || { echo "make bench: RUNG must be 1, 2 or 3 (got '$(RUNG)')"; exit 2; }
	@[ -n "$(strip $(ORCH_IMAGE))" ] || { echo "make bench: ORCH_IMAGE is required — build+push per bench/incluster/README.md, then pass ORCH_IMAGE=<registry>/s3-pacer-bench:<tag>"; exit 2; }
	kubectl -n $(BENCH_NAMESPACE) delete job $(BENCH_JOB) --ignore-not-found
	ORCH_IMAGE='$(ORCH_IMAGE)' ORCH_ARCH='$(ORCH_ARCH)' \
	  LADDER_STEPS='$(BENCH_STEPS)' LADDER_RUN_ID='$(BENCH_RUN_ID)' \
	  LADDER_NOBJ='$(LADDER_NOBJ)' LADDER_CHUNK_SIZE='$(LADDER_CHUNK_SIZE)' \
	  LADDER_MEM_CAPACITY='$(LADDER_MEM_CAPACITY)' LADDER_IMAGE_TAG='$(LADDER_IMAGE_TAG)' \
	  LADDER_BACKEND_TYPE='$(LADDER_BACKEND_TYPE)' BENCH_REAL_BUCKET='$(BENCH_REAL_BUCKET)' \
	  envsubst '$${ORCH_IMAGE} $${ORCH_ARCH} $${LADDER_STEPS} $${LADDER_NOBJ} $${LADDER_CHUNK_SIZE} $${LADDER_MEM_CAPACITY} $${LADDER_IMAGE_TAG} $${LADDER_RUN_ID} $${LADDER_BACKEND_TYPE} $${BENCH_REAL_BUCKET}' \
	  < $(BENCH_JOB_MANIFEST) | kubectl -n $(BENCH_NAMESPACE) apply -f -
	@echo "Submitted $(BENCH_JOB) [steps: $(BENCH_STEPS), run-id: $(BENCH_RUN_ID)]. Follow: make bench-logs"

.PHONY: seed
seed: ## Submit a seed-only Job for RUNG (deploy + that rung's seed step); needs ORCH_IMAGE
	@[ -n "$(strip $(BENCH_SEED_STEPS))" ] || { echo "make seed: RUNG must be 1, 2 or 3 (got '$(RUNG)')"; exit 2; }
	@[ -n "$(strip $(ORCH_IMAGE))" ] || { echo "make seed: ORCH_IMAGE is required — see 'make bench'"; exit 2; }
	kubectl -n $(BENCH_NAMESPACE) delete job $(BENCH_JOB) --ignore-not-found
	ORCH_IMAGE='$(ORCH_IMAGE)' ORCH_ARCH='$(ORCH_ARCH)' \
	  LADDER_STEPS='$(BENCH_SEED_STEPS)' LADDER_RUN_ID='$(BENCH_RUN_ID)' \
	  LADDER_NOBJ='$(LADDER_NOBJ)' LADDER_CHUNK_SIZE='$(LADDER_CHUNK_SIZE)' \
	  LADDER_MEM_CAPACITY='$(LADDER_MEM_CAPACITY)' LADDER_IMAGE_TAG='$(LADDER_IMAGE_TAG)' \
	  LADDER_BACKEND_TYPE='$(LADDER_BACKEND_TYPE)' BENCH_REAL_BUCKET='$(BENCH_REAL_BUCKET)' \
	  envsubst '$${ORCH_IMAGE} $${ORCH_ARCH} $${LADDER_STEPS} $${LADDER_NOBJ} $${LADDER_CHUNK_SIZE} $${LADDER_MEM_CAPACITY} $${LADDER_IMAGE_TAG} $${LADDER_RUN_ID} $${LADDER_BACKEND_TYPE} $${BENCH_REAL_BUCKET}' \
	  < $(BENCH_JOB_MANIFEST) | kubectl -n $(BENCH_NAMESPACE) apply -f -
	@echo "Submitted seed-only $(BENCH_JOB) [steps: $(BENCH_SEED_STEPS)]. Follow: make bench-logs"

.PHONY: bench-logs
bench-logs: ## Follow the running/finished ladder Job's logs (the verdict prints here)
	kubectl -n $(BENCH_NAMESPACE) logs -f job/$(BENCH_JOB)

.PHONY: bench-status
bench-status: ## Show the ladder Job + its orchestrator pod
	kubectl -n $(BENCH_NAMESPACE) get job/$(BENCH_JOB) -o wide
	kubectl -n $(BENCH_NAMESPACE) get pods -l $(BENCH_SELECTOR) -o wide

.PHONY: bench-clean
bench-clean: ## Delete the ladder Job (does NOT touch nodes — use `make down` for those)
	kubectl -n $(BENCH_NAMESPACE) delete job $(BENCH_JOB) --ignore-not-found

.PHONY: down
down: ## Operator-side: reclaim EFA nodes + uninstall the daemon (run.sh down; NOT a Job step)
	$(BENCH_LADDER_RUN) down
