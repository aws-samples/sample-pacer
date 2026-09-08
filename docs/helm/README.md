# PACER Helm chart — design notes

[`deploy/helm/pacer/values.yaml`](../../deploy/helm/pacer/values.yaml) keeps **one short
comment per key**: what it is, its unit, its default, and a pointer into this directory. The
reasoning, the measurements and the failure history live here.

That split exists because the values file had become 1060 lines of which most was narrative,
and the narrative was unreviewable in that form: a paragraph correcting a measurement was a
diff inside a comment inside a YAML mapping. Nothing was deleted in the move — every claim,
number, date, commit SHA and source reference from the old comments is in one of the files
below, and each one names the values keys it covers.

## The second chart

There are two charts. This directory documents the **daemon** chart. The **bench** chart
([`deploy/helm/pacer-bench`](../../deploy/helm/pacer-bench), quality item H1) renders one
object per invocation — the ADR-0029 node launcher, each bench/probe/loader pod, the build
pod, the python dev pod, the dev-loop placeholder — and obeys the same one-short-comment-per-key
rule, verified: an 11-line header stating the chart's contract, then at most three comment
lines per key across 249 lines.

Its design notes are **not** here, deliberately. Every value in it was an `envsubst`
`${VAR}` whose meaning is the manifest it lands in, so the reasoning lives in the template
that reads it and every refusal in `templates/_validate.tpl` — one hop from the value, in
the file that would break. Nothing about it is narrative enough to need a page: it has no
tuning knobs, no measurements and no failure history of its own, because it has no
defaults at all. That is the chart's whole design (an unset value fails the render instead
of reaching the API server as the empty string, which is how a pod came to be pinned to
`kubernetes.io/hostname: ""` and sat Pending forever with nothing saying why).

## Why `docs/helm/` and not `deploy/helm/pacer/docs/`

`docs/` is already this repository's documentation root
([`docs/benchmarks/`](../benchmarks/)), and the OOM runbook these notes cross-link to is an
operational document rather than a chart artifact — it belongs beside them, not inside the
packaged chart. Keeping the prose out of `deploy/helm/pacer/` also keeps `helm package` lean:
everything under the chart directory ships in the `.tgz`.

## The files

| file | values keys |
|---|---|
| [memory-model.md](memory-model.md) | `resources`, `allocator`, and the rule every memory key obeys. **Read this before raising `config.memCapacity`.** |
| [cache-and-disk-tier.md](cache-and-disk-tier.md) | `config.*` (backend, tiers, chunking, foyer tuning, `diskTier`), `cache.hostPath` |
| [efa-and-rdma.md](efa-and-rdma.md) | `cluster.*`, `efa.*` |
| [delivery.md](delivery.md) | `delivery.*` (ADR-0026/0027/0030) |
| [write-scatter.md](write-scatter.md) | `scatter.*` (ADR-0032) |
| [network-policy.md](network-policy.md) | `networkPolicy.*`, `ports`, `service` |
| [scheduling.md](scheduling.md) | `session`, `nodeSelector`, `tolerations`, `priorityClassName`, `podDisruptionBudget`, `terminationGracePeriodSeconds`, `extraEnv`, `devMode`, `serviceAccount`, `image`, `karpenter` |
| [monitoring.md](monitoring.md) | `monitoring.prometheusRule.*` |
| [`../runbooks/daemon-oom.md`](../runbooks/daemon-oom.md) | not a values reference — the eight recorded OOM incidents and the decision tree |

## What validates what

Three layers, deliberately distinct:

| layer | catches | file |
|---|---|---|
| `values.schema.json` | wrong TYPE, unknown key (`config.chunkSiz`), a byte size in a dialect `pacer.toBytes` silently reads as 0 (`1Ti`), an out-of-range number, a bad enum | [`values.schema.json`](../../deploy/helm/pacer/values.schema.json) |
| `templates/_validate.tpl` | cross-field minimums draft-07 cannot express: `limits.memory` vs `memCapacity`, `pinnedPoolReservation` vs both arenas, the two foyer buffers vs `chunkSize`, the grace period vs the drain deadline | [`_validate.tpl`](../../deploy/helm/pacer/templates/_validate.tpl) |
| `templates/_helpers.tpl` | the older feature-scoped guards: hugepage budget, EFA device access, Express-vs-scatter, the two delivery quotas, the two deny-shaped NetworkPolicy lists | [`_helpers.tpl`](../../deploy/helm/pacer/templates/_helpers.tpl) |

Every one of them fails the **render**, where the operator is still looking at the
configuration, rather than becoming a CrashLoop or a silently degraded path five minutes
into `helm --wait`.

`deploy/helm/pacer/tests/` holds the [helm-unittest](https://github.com/helm-unittest/helm-unittest)
suites; the CI `helm` job runs `helm lint`, `helm unittest` and a set of `helm template …`
renders with grep assertions. See [`ci/README.md`](../../ci/README.md).

## Layered values files

The base chart is a complete, working install. The shipped overlays layer onto it with `-f`,
in this order:

```
deploy/helm/pacer/values.yaml                 the base
  + values-<env>.yaml                         values-gpu-ohio, values-gpu-mumbai, values-example
    + values-efa.yaml                         turns the RDMA transport on
    + values-dev.yaml                         the dev supervisor loop
      + values-dev-general.yaml | -dev-gpu.yaml
    + bench/ladder/values-*.yaml              one per measured arm
```

Two Helm behaviours decide how they compose, and both have cost this repo a broken deploy:
maps MERGE (so a default label must be nulled out explicitly, which is why
`pacer.nodeSelector` skips nulls), and **lists REPLACE** (which is how a GPU overlay drops a
cache-pool toleration wholesale).
