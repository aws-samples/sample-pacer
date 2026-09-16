# PACER — common dev steps. `make help` lists targets.
#
# Everything here runs cargo, helm or docker against this checkout and nothing
# else: no cluster, no credentials, no images to pull. That is the dividing line
# this file is kept on — the targets that drive our own cluster, build pods and
# benchmark harness live in `mk/internal.mk`, which is not part of the public
# snapshot (see the header there, and scripts/publish/exclude.txt). Add a target
# here only if `make <it>` works on a fresh clone with a Rust toolchain.

.DEFAULT_GOAL := help

# Deliberately no PATH games here: build/test use the ambient cargo. Two
# toolchains coexist on dev Macs (Homebrew rust + rustup for the Linux
# cross-target) and mixing them in one target/ dir breaks builds (E0514).

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

## Housekeeping ---------------------------------------------------------------

.PHONY: clean
clean: ## Remove build artifacts
	cargo clean

.PHONY: help
help: ## List targets
	@# -h is load-bearing: $(MAKEFILE_LIST) is more than one file whenever
	@# mk/internal.mk is present, and grep prefixes `<file>:` on every line as
	@# soon as it has two inputs. The awk field separator starts at the first
	@# `:`, so without -h the printed name would be the FILENAME, not the target.
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

# The cluster/build-pod/benchmark targets. `-include`, not `include`: the file is
# absent from the public snapshot by design, and a hard `include` would make every
# `make` invocation there fail before running anything. It is listed after `help`
# on purpose — `$(MAKEFILE_LIST)` is expanded when the recipe runs, so `make help`
# lists these too wherever the file is present.
-include mk/internal.mk
