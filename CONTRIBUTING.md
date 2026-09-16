# Contributing Guidelines

Thank you for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional
documentation, we greatly value feedback and contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure we have all the necessary
information to effectively respond to your bug report or contribution.


## Reporting Bugs/Feature Requests

We welcome you to use the GitHub issue tracker to report bugs or suggest features.

When filing an issue, please check existing open, or recently closed, issues to make sure somebody else hasn't already
reported the issue. Please try to include as much information as you can. Details like these are incredibly useful:

* A reproducible test case or series of steps
* The version of our code being used
* Any modifications you've made relevant to the bug
* Anything unusual about your environment or deployment


## Development environment

Building PACER needs a Rust 1.96 toolchain; the RDMA transport additionally needs the
EFA userspace libraries, because `ibverbs-sys` vendors rdma-core, builds it with
cmake + bindgen, and links `libibverbs`/`libefa`. Rather than ask you to assemble that
by hand, the repo ships two devcontainers built on the **same images CI uses**, so
"green in my editor" and "green in CI" mean the same thing.

| Container | Reproduces | Cannot |
|---|---|---|
| `.devcontainer/` (default) | fmt, clippy, `cargo test`, rustdoc, `cargo deny`, `helm lint`, the functional smoke test | build `--features efa` |
| `.devcontainer/efa/` | all of the above, **plus** `--features efa` and `--all-features` clippy | exercise RDMA — there are no EFA devices in a laptop |

Open either with **Reopen in Container** (VS Code offers a picker when both are
present). Start with the default one: it covers everything except the transport's EFA
code path. Reach for the EFA variant when you touch `crates/pacer-transport` — it
builds rdma-core from source, so the first build is slow, and it builds for your
machine's own architecture only.

In the default container, the gates that must pass:

```bash
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
helm lint deploy/helm/pacer
```

`make ci` runs the whole set in one step — but note that `make lint` passes
`--all-features`, which enables `efa` and therefore needs the EFA container.

Two details the containers handle that are easy to get wrong by hand:

* **`CARGO_TARGET_DIR` points at a named volume**, not the bind-mounted `target/`. A
  host toolchain and a container toolchain writing one `target/` fail with E0514
  ("found crate compiled by a different version of rustc"). It also keeps builds
  incremental across container restarts.
* **`rustfmt` and `clippy` are installed explicitly** — the official Rust image ships
  a minimal toolchain with neither, and `make lint` needs both.

Prefer to work without a container? You need Rust 1.96 with `rustfmt` and `clippy`,
plus `helm` and `cargo-deny`; `.devcontainer/install-dev-tools.sh` installs those last
two at the versions CI pins. Everything except `--features efa` builds on macOS and
Linux — that one feature needs Linux with the EFA userspace present.

## Contributing via Pull Requests
Contributions via pull requests are much appreciated. Before sending us a pull request, please ensure that:

1. You are working against the latest source on the *main* branch.
2. You check existing open, and recently merged, pull requests to make sure someone else hasn't addressed the problem already.
3. You open an issue to discuss any significant work - we would hate for your time to be wasted.

To send us a pull request, please:

1. Fork the repository.
2. Modify the source; please focus on the specific change you are contributing. If you also reformat all the code, it will be hard for us to focus on your change.
3. Ensure local tests pass — see [Development environment](#development-environment) for the container that runs the same checks CI does.
4. Commit to your fork using clear commit messages.
5. Send us a pull request, answering any default questions in the pull request interface.
6. Pay attention to any automated CI failures reported in the pull request, and stay involved in the conversation.

GitHub provides additional document on [forking a repository](https://help.github.com/articles/fork-a-repo/) and
[creating a pull request](https://help.github.com/articles/creating-a-pull-request/).


## Coding style

Machine-enforced where possible: `[workspace.lints]` in `Cargo.toml` plus `clippy.toml`
thresholds. CI runs clippy with `-D warnings`, so every lint below is a merge blocker.
`make lint` (or `make ci`) reproduces the gate locally.

- **No magic values.** A literal whose meaning isn't obvious from the expression it
  appears in (buffer sizes, queue depths, timeouts, ports, protocol strings, env-var
  names, limits) becomes a `const NAME: T = ...;` — SCREAMING_SNAKE_CASE, at the top of
  the module (or function, if single-use), with a `///` comment explaining **why this
  value** (constraints, provenance), not just what it is. Obvious-in-context literals
  (`+ 1`, index `0`, `len - 1`, test fixtures) stay inline.
- **Env-var names are a contract.** Every `PACER_*` variable is defined once as a `const`
  in `config.rs` and referenced, never retyped.
- **Small units.** Functions ≤ 80 lines, nesting ≤ 5 levels, ≤ 7 args (clippy-enforced:
  `too_many_lines`, `excessive_nesting`, `too_many_arguments`). Prefer extracting a named
  helper over growing a body; prefer early returns over nesting.
- **Everything public is documented.** `missing_docs` is on workspace-wide: every public
  item gets a `///` explaining intent and the *why*, not a restatement of the name.
  Modules get `//!` headers. Functions returning `Result` document failure modes
  (`# Errors`); panicking paths document them (`# Panics`) — both clippy-enforced.
- **Readable literals.** Numbers ≥ 5 digits use `_` separators (`10_000`); sizes use
  shifted forms (`4 << 20`) or named consts (clippy `unreadable_literal`).
- **New crates** add `[lints] workspace = true` to their `Cargo.toml`.

Documentation style follows the
[kernel Rust guidelines](https://docs.kernel.org/rust/coding-guidelines.html).

### Design-record references (`ADR-nnnn`, `planning/…`, `spike/…`)

Comments throughout the code cite design records — `ADR-0016`, `planning/15`,
`spike/efa`, and similar. These are the project's internal architecture-decision records
and research notes; they are **not published in this repository** (yet), so the
references don't resolve here. They are kept in the code on purpose: they mark *which
decision* a constraint or constant came from, and they resolve for the maintainers when
tracing a design question. Treat them as provenance pointers, and feel free to ask about
any of them in an issue.


## Finding contributions to work on
Looking at the existing issues is a great way to find something to contribute on. As our projects, by default, use the default GitHub issue labels (enhancement/bug/duplicate/help wanted/invalid/question/wontfix), looking at any 'help wanted' issues is a great place to start.


## Code of Conduct
This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or contact
opensource-codeofconduct@amazon.com with any additional questions or comments.


## Security issue notifications
If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public github issue.


## Licensing

See the [LICENSE](LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.
