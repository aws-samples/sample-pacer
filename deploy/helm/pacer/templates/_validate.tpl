{{/*
Cross-field validation JSON Schema cannot express.

values.schema.json checks the SHAPE of every key — type, enum, byte-size dialect, range,
and `additionalProperties: false` so a typo fails the render. What it cannot check is a
RELATIONSHIP between two keys, because draft-07 has no way to compare one property to
another. Those live here, as `fail` calls in the same style as `pacer.validateScatter` /
`pacer.validateEfaAccess` / `pacer.validateDelivery` in _helpers.tpl: name the two keys,
say what the runtime symptom would have been, and give the operator the exact number.

Every check in this file guards a failure that is SILENT or MISATTRIBUTED at runtime — an
OOMKill that reads client-side as a truncated body, or a foyer enqueue that is dropped and
reads as a cache miss. A relationship whose failure announces itself (an unschedulable pod,
an API-invalid object) is deliberately NOT checked here.

`pacer.validate` aggregates them and is invoked once from templates/daemonset.yaml, which is
always rendered. It emits nothing unless something fails.
*/}}

{{/*
The daemon's DEFAULT SIGTERM drain deadline, in seconds.

Named rather than inlined because two places need the same number and they are in different
files: the `minimum` on `terminationGracePeriodSeconds` in values.schema.json (which a
`--skip-schema-validation` render bypasses) and the message below. It restates the deadline
the daemon's shutdown path enforces; raising one without the other is the drift this
constant exists to make visible.

Since ADR-0036 the deadline is configurable (`config.shutdownDrainTimeoutSecs`), so this is
the value that applies when the knob is left at 0 — and it must stay equal to
`shutdown::DEFAULT_DRAIN_TIMEOUT_SECS` in crates/pacer-daemon/src/shutdown.rs, which is where
the daemon's own default lives. `pacer.validateGracePeriod` prefers the configured value when
there is one.
*/}}
{{- define "pacer.drainDeadlineSeconds" -}}20{{- end -}}

{{/*
`resources.limits.memory` must at least hold `config.memCapacity`.

Rule 1 of the sizing model (docs/helm/memory-model.md): the limit is a BUDGET for
everything the cache's own accounting can see, and the RAM tier is the largest single
claim on it. Below the tier size the configuration is simply impossible — the daemon fills
the tier it was told to fill and the cgroup kills it mid-response, which the client sees as
`IncompleteRead(… more expected)` and reads as a protocol bug.

⚠ **Equality still renders, and that is a deliberate gap.** `memCapacity: 24GiB` against a
24Gi limit is exactly what cost a 256 GiB run, but a chart cannot tell a deliberately tiny
tier on a large limit from an under-sized one: a benchmark overlay may run a 128 MiB tier
on purpose, and a GPU dev overlay may sit at exactly 1x when layered onto the base alone.
So this fails only where the arithmetic is unambiguous, and the ~2x rule of thumb in
docs/helm/memory-model.md closes the rest.

Compared in BYTES, not as strings: the two keys are written in different dialects across
this repo's overlays (`4Gi` here, `24GiB` there), and a string comparison would rank
`8GiB` below `16Gi` lexically and pass a configuration that cannot work — the same trap
pacer.deliveryMaxTargetBytes carries a comment about.
*/}}
{{- define "pacer.validateMemoryBudget" -}}
{{- $limit := include "pacer.toBytes" .Values.resources.limits.memory | int64 -}}
{{- $tier := include "pacer.toBytes" .Values.config.memCapacity | int64 -}}
{{- if lt $limit $tier -}}
{{- fail (printf "resources.limits.memory (%d bytes) is below config.memCapacity (%d bytes): the limit is the budget the RAM tier is spent OUT of, so the tier cannot fit at all and the daemon is OOMKilled mid-response during the first fill — which the client sees as `IncompleteRead(... more expected)` and reads as a protocol bug, not as a cgroup limit. Raise resources.limits.memory to at least config.memCapacity, and read docs/helm/memory-model.md first: ~2x memCapacity is the rule of thumb this repo's overlays use, because the limit also has to hold the S3 SDK's buffers, in-flight chunk fills and hyper's buffers for a ranged GET. The pinned terms (arenas, ADR-0028 slab, delivery and scatter footprints) are added on top for you by pacer.memoryLimit and are NOT what this check is about." $limit $tier) -}}
{{- end -}}
{{- end -}}

{{/*
`efa.pinnedPoolReservation` must cover BOTH arenas the transport registers.

ADR-0024's two arenas are `cluster.rdmaArenaBytes` (requester) and
`HOLDER_ARENA_RANGES x config.chunkSize` (holder), and `pinnedPoolReservation` is what
adds them to the cgroup limit. It is a BUDGET LINE, not a cap on registration: the daemon
registers both arenas whatever this says, so a shortfall is a cgroup limit that does not
cover pinned pages `config.memCapacity` cannot see — planning/16 §4.5, where both daemons
OOMKilled under seed load.

values.yaml has said "keep it in lock-step with BOTH terms" since ADR-0024 landed. This
makes it checkable, and the term that actually drifts is `chunkSize`: the holder arena is
256x it, so moving a fleet from a 16 MiB to a 64 MiB chunk quadruples 4Gi to 16Gi while
`pinnedPoolReservation` sits at whatever the profile last wrote down.

Only checked when efa.enabled: with the transport off no arena is registered and the term
is not added.
*/}}
{{- define "pacer.validateArenaReservation" -}}
{{- if .Values.efa.enabled -}}
{{- $chunk := include "pacer.toBytes" (.Values.config.chunkSize | default "16MiB") | int64 -}}
{{- $requester := include "pacer.toBytes" (.Values.cluster.rdmaArenaBytes | default "4Gi") | int64 -}}
{{- $holder := mul 256 $chunk -}}
{{- $need := add $requester $holder -}}
{{- $have := include "pacer.toBytes" .Values.efa.pinnedPoolReservation | int64 -}}
{{- if lt $have $need -}}
{{- fail (printf "efa.pinnedPoolReservation (%d bytes) is below the two arenas the transport registers: requester = cluster.rdmaArenaBytes (%d) plus holder = 256 x config.chunkSize (%d), needing %d bytes (%dGi). It is a BUDGET LINE, not a cap — the daemon registers both arenas regardless, so the cgroup limit then does not cover pinned pages config.memCapacity cannot see and the pod OOMKills under seed load. Raise efa.pinnedPoolReservation to at least %dGi, or lower cluster.rdmaArenaBytes / config.chunkSize. Note the holder arena is 256x chunkSize, so a 16MiB -> 64MiB chunk move quadruples it." $have $requester $holder $need (divf $need 1073741824 | ceil | int64) (divf $need 1073741824 | ceil | int64)) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
An explicit `config.tuning.submitQueueThreshold` must hold at least one chunk entry.

The budget bounds in-flight DRAM->NVMe writes, and an enqueue arriving when that much is
already unwritten is **silently dropped**: foyer counts
`foyer_storage_inner_op_total{op="channel_overflow"}`, the chunk never reaches the disk
tier, and a later read misses to a peer or the backend. That is the exact defect
pacer.submitQueueThreshold exists to close — foyer's own 16 MiB default is ONE entry at the
shipped chunk size, and sizing it took the drop count from 4229/2244 to exactly 0, twice
(bench/ladder/results/c4-foyer-readpath.md).

So the floor is one entry: below `chunkSize` the budget cannot admit a single demotion and
the disk tier is write-only in name. The DERIVED value is `flushers x chunkSize x 4`, well
above this floor; this check catches the operator who names a number by hand and picks it
from the wrong dialect or the wrong scale.

`0` is exempt, and deliberately: it means "keep foyer's own 16 MiB", which a control arm has
to be able to ask for even though it is the historical drop.
*/}}
{{- define "pacer.validateSubmitQueue" -}}
{{- $t := .Values.config.tuning | default dict -}}
{{- $explicit := $t.submitQueueThreshold -}}
{{- if and (not (kindIs "invalid" $explicit)) (ne ($explicit | toString | trim) "") (ne ($explicit | toString | trim) "0") -}}
{{- $budget := include "pacer.toBytes" $explicit | int64 -}}
{{- $chunk := include "pacer.toBytes" (.Values.config.chunkSize | default "16MiB") | int64 -}}
{{- if lt $budget $chunk -}}
{{- fail (printf "config.tuning.submitQueueThreshold (%d bytes) is below config.chunkSize (%d bytes), so the in-flight DRAM->NVMe budget cannot admit ONE chunk entry: every demotion past the first is SILENTLY DROPPED (foyer counts foyer_storage_inner_op_total{op=\"channel_overflow\"}, the chunk never reaches disk, and a later read misses to a peer or the backend — measured at 4229 drops on foyer's own default). Leave it empty for the derived `flushers x chunkSize x 4`, set it to at least config.chunkSize, or set it to exactly 0 if you really want foyer's own 16MiB back for a control arm." $budget $chunk) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
An explicit `config.flushBufferSize` must hold at least one chunk entry, for the same
reason and with a worse symptom.

values.yaml states it: "Entries larger than this buffer are SILENTLY DROPPED on demotion
(never reach the disk tier)". Unset, the daemon auto-sizes to `2 x blockSize`, which is
always far above a chunk. An operator who sets it explicitly and under-sizes it gets a disk
tier that accepts nothing and reports nothing — a cache that looks warm and misses every
read.
*/}}
{{- define "pacer.validateFlushBuffer" -}}
{{- $explicit := .Values.config.flushBufferSize -}}
{{- if and (not (kindIs "invalid" $explicit)) (ne ($explicit | toString | trim) "") (ne ($explicit | toString | trim) "0") -}}
{{- $buffer := include "pacer.toBytes" $explicit | int64 -}}
{{- $chunk := include "pacer.toBytes" (.Values.config.chunkSize | default "16MiB") | int64 -}}
{{- if lt $buffer $chunk -}}
{{- fail (printf "config.flushBufferSize (%d bytes) is below config.chunkSize (%d bytes): an entry larger than the flush buffer is SILENTLY DROPPED on demotion and never reaches the disk tier, so every chunk misses on a later read while the cache reports itself healthy. Leave it empty (the daemon auto-sizes to 2 x config.blockSize) or set it to at least config.chunkSize." $buffer $chunk) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
`terminationGracePeriodSeconds` must exceed the daemon's SIGTERM drain deadline.

The drain finishes in-flight requests before the listeners close; a grace period at or below
its deadline means the kubelet SIGKILLs the container mid-drain, so the requests the drain
exists to finish are lost anyway and the drain has bought nothing but latency on every
rollout. Duplicated from the schema's `minimum` on purpose: a
`helm template --skip-schema-validation` (which helm-unittest can pass) bypasses the schema
but not this.

**Compared against the CONFIGURED drain, not only the built-in one.** ADR-0036 made the
deadline an operator knob (`config.shutdownDrainTimeoutSecs`, `PACER_SHUTDOWN_DRAIN_TIMEOUT`),
and the schema's `exclusiveMinimum: 20` on the grace period can only know the built-in
default — so `shutdownDrainTimeoutSecs: 30` against the shipped grace of 30 passes the schema
while producing exactly the mid-drain SIGKILL this check exists to prevent. Raising the drain
without raising the grace period is the likelier direction of that mistake, because the drain
is the number an operator has a reason to touch.

`0` means "the daemon's own default", the same convention every numeric knob in
`config` uses, so it falls back to `pacer.drainDeadlineSeconds` rather than comparing
against zero — which would pass anything.
*/}}
{{- define "pacer.validateGracePeriod" -}}
{{- $grace := .Values.terminationGracePeriodSeconds | default 0 | int64 -}}
{{- $builtin := include "pacer.drainDeadlineSeconds" . | int64 -}}
{{- $configured := .Values.config.shutdownDrainTimeoutSecs | default 0 | int64 -}}
{{- $drain := $builtin -}}
{{- $source := "the daemon's built-in default" -}}
{{- if gt $configured 0 -}}
{{- $drain = $configured -}}
{{- $source = "config.shutdownDrainTimeoutSecs" -}}
{{- end -}}
{{- if le $grace $drain -}}
{{- fail (printf "terminationGracePeriodSeconds (%d) is not above the daemon's SIGTERM drain deadline (%d s, from %s): the kubelet SIGKILLs the container when the grace period expires, so a drain that has not finished by then loses exactly the in-flight requests it exists to finish. Set terminationGracePeriodSeconds above %d, or lower config.shutdownDrainTimeoutSecs below it — the chart ships a 20 s drain against a grace of 30, which is the drain plus room for the listener shutdown and a final metrics scrape." $grace $drain $source $drain) -}}
{{- end -}}
{{- end -}}

{{- define "pacer.validate" -}}
{{- include "pacer.validateMemoryBudget" . -}}
{{- include "pacer.validateArenaReservation" . -}}
{{- include "pacer.validateSubmitQueue" . -}}
{{- include "pacer.validateFlushBuffer" . -}}
{{- include "pacer.validateGracePeriod" . -}}
{{- end -}}
