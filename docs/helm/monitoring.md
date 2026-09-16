> Design notes for the `monitoring` keys in
> [`deploy/helm/pacer/values.yaml`](../../deploy/helm/pacer/values.yaml) and for
> [`templates/prometheusrule.yaml`](../../deploy/helm/pacer/templates/prometheusrule.yaml).

# Alerting on the daemon's memory

## Why the cgroup series and not the process ones

`process_resident_memory_bytes` measures RSS, which **excludes page cache**, and foyer's disk
tier does buffered I/O — so on a large-memory node the daemon can look flat to the byte while
its cgroup marches to the limit. Measured on a p5 serving a 131 GiB checkpoint: 512 GiB
served from the disk tier, 0.52 GiB of actual md127 reads, `read_bytes: 0` on the process, and
**72.1 GiB resident in `Cached`**
(`bench/ladder/results/c4-fanout-depth.md`).

The kill leaves nothing in the log, because the kernel kills the container. Both client-side
symptoms name something else: `IncompleteRead(… more expected)` and
`EndpointConnectionError`. That is why the daemon publishes its own cgroup's accounting
([`crates/pacer-daemon/src/cgroup.rs`](../../crates/pacer-daemon/src/cgroup.rs)) under
`pacer_cgroup_*` rather than relying on the `container_memory_*` series cAdvisor already
exports for the same cgroup: those come from kubelet on its own schedule.

Full reasoning: [memory-model.md § rule 5](memory-model.md#rule-5--what-will-not-warn-you).
Operational response: [`docs/runbooks/daemon-oom.md`](../runbooks/daemon-oom.md).

## The five alerts

`monitoring.prometheusRule.enabled` is **false by default**, because
`monitoring.coreos.com/v1` is a CRD: on a cluster without the Prometheus Operator, rendering
this object fails the whole install.

| alert | expression | severity | default threshold |
|---|---|---|---|
| `PacerDaemonMemoryNearLimit` | `current / max > memoryRatio` | warning | 0.9 for 10m |
| `PacerDaemonAnonymousMemoryNearLimit` | `(current − file) / max > anonRatio` | warning | 0.85 for 15m |
| `PacerDaemonCgroupOom` | `increase(oom_total[oomWindow]) > 0` | critical | any event in 5m |
| `PacerDaemonOomKilled` | `increase(oom_kill_total[oomWindow]) > 0` | critical | any event in 5m |
| `PacerDaemonMemoryBudgetNearLimit` | `budget_total × (1 + headroom) / max > budgetRatio` | warning | 0.95 for 15m |

**`PacerDaemonMemoryNearLimit`** is `memory.current` over `memory.max` — exactly the
comparison the kernel makes, page cache included. This is *the* series to alert on.

**`PacerDaemonAnonymousMemoryNearLimit`** is the page-cache-crowding alert. `current − file`
is the part of the charge that **no reclaim can free**, so this firing means the page cache
has nowhere left to live and the next buffered write goes straight into the limit — the shape
[rule 3](memory-model.md#rule-3--the-term-no-template-can-compute-and-it-is-the-one-that-kills-a-big-load)
describes, caught before the page cache pushes it over. `pacer_cgroup_memory_anon_bytes`
reports the same quantity directly; the subtraction is used because it is derivable on any
hierarchy that publishes the first two series, and because `file` is the term the operator
then has to reason about.

**`PacerDaemonCgroupOom`** is the only alert that fires while there is still something to do
about it: `memory.events oom` counts an allocation that failed after reclaim, under the
cgroup's own limit, and it is counted **before** any kill and readable by a daemon still
running. No `for:` clause — a single event is the signal.

**`PacerDaemonOomKilled`** catches a kill that did **not** end the container. `oom_kill`
resets with the container's cgroup, so a surviving daemon reporting an increase is evidence
of a task killed inside a pod that stayed up — which no restart count and no client symptom
would show.

**`PacerDaemonMemoryBudgetNearLimit`** is the odd one out: it alerts on the budget the daemon
was **configured** with, not on anything measured. `pacer_memory_budget_total_bytes` is the
enforced sum of the counted terms — the same sum
[`pacer.memoryLimit`](../../deploy/helm/pacer/templates/_helpers.tpl) renders and the same one
the startup check in
[`memory_budget.rs`](../../crates/pacer-daemon/src/memory_budget.rs) compares against
`memory.max` — and each term is published separately as
`pacer_memory_budget_bytes{term,counted}`, where `counted="false"` marks a term reported for
an operator's benefit but **not** in the enforced total, because the chart does not add it
either. Plot the total against `pacer_cgroup_memory_max_bytes` and the two failure classes
separate: *the configuration never fitted* looks nothing like *the configuration fitted and
something grew*, and only the first has a values-file answer.

The alert applies the **same** headroom the startup check does — `config.memoryHeadroomFraction`,
or the daemon's own 0.10 when that is unset, resolved by the `pacer.memoryHeadroomFraction`
helper and held in lockstep by a unit test that reads the template out of the repo. **It can
only fire in three situations, because at a ratio of 1 the daemon refuses to start at all:**
the limit was lowered *under* a running pod (a quota or `LimitRange` edit, or a node still
draining an old pod after a `helm upgrade` that shrank `resources.limits.memory`);
`config.memoryCheck` is `warn`, so the daemon logged the same verdict and started anyway and
this is the only thing that says so afterwards; or the chart and the daemon have drifted about
which terms count. `budgetRatio` defaults to **0.95** rather than 1.0 for exactly that reason —
the interesting range is just below the value that would have refused the start. `budgetFor` is
15m, longer than `memoryFor`, because the series are published **once at startup** and never
move: a long `for:` costs nothing and rules out a scrape gap or a rolling restart.

## Two edge cases the expressions survive without a guard

Every ratio here, the configured-budget one included, is written as bare division on purpose:

* an **unlimited cgroup** publishes `+Inf` for the limit (`cgroup.rs` reports "unset" as
  infinity rather than 0), so `current / +Inf` is 0 and nothing fires;
* a container with **no cgroup memory controller** publishes 0 for every series, so the ratio
  is `0 / 0` = NaN, and a NaN comparison in PromQL is false.

Wrapping them in `and pacer_cgroup_memory_max_bytes > 0` changes no outcome and only gives a
reader something else to get wrong.

## Wiring it up

```yaml
monitoring:
  prometheusRule:
    enabled: true
    # A Prometheus Operator selects rules by label. Without a match this object is inert,
    # which looks exactly like a healthy install — check `prometheus.spec.ruleSelector`.
    labels:
      release: kube-prometheus-stack
    # Where the alert's runbook_url points. Each alert appends its own #anchor.
    runbookUrl: https://your-wiki/pacer/daemon-oom
```

Every alert also carries `pacer.io/session` when `session.id` is set (ADR-0029), so a
bench fleet's alerts can be routed away from a production one's.

Scraping is a prerequisite and is not part of this object: `/metrics` lives on
`ports.admin`, and `networkPolicy.metrics` is what admits the scraper —
[network-policy.md § `metrics`](network-policy.md#metrics). `/metrics` is unauthenticated
and discloses key and topology detail
(`threat-model.md` T-008).

## See also

* [`docs/runbooks/daemon-oom.md`](../runbooks/daemon-oom.md)
* [memory-model.md](memory-model.md)
