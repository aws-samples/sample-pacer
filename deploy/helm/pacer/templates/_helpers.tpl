{{- define "pacer.name" -}}
{{- .Chart.Name -}}
{{- end -}}

{{- define "pacer.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "pacer.labels" -}}
app.kubernetes.io/name: {{ include "pacer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- with include "pacer.sessionLabels" . }}{{ . | nindent 0 }}{{- end }}
{{- end -}}

{{/*
Owning-session label (ADR-0029), emitted only when values.session.id is set.

Deliberately NOT part of pacer.selectorLabels: a DaemonSet's spec.selector is
immutable, so adding a key there would make every existing release unupgradeable.
This goes on object metadata and on the pod TEMPLATE (a superset of the selector is
legal), which is what `kubectl get pods -l pacer.io/session=<id>` needs.
*/}}
{{- define "pacer.sessionLabels" -}}
{{- with .Values.session.id -}}
pacer.io/session: {{ . | quote }}
{{- end -}}
{{- end -}}

{{- define "pacer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pacer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
The DaemonSet's effective nodeSelector.

Two jobs. First, skip null-valued entries: Helm's "key: null" deletion only works
against chart DEFAULTS, not between -f overlay files, so a later overlay nulling an
earlier overlay's selector (values-p5.yaml nulling values-gpu-ohio.yaml's pool label)
would otherwise render an API-invalid null value.

Second, add `pacer.io/session: <id>` when session.requireClaimedNodes is on — the key
that confines this release to nodes its own session has claimed. Merged HERE rather
than asked of each caller, so no deploy path can forget it and quietly land on a
node another session is measuring on.
*/}}
{{- define "pacer.nodeSelector" -}}
{{- $sel := dict }}
{{- range $k, $v := .Values.nodeSelector }}{{- if $v }}{{- $_ := set $sel $k $v }}{{- end }}{{- end }}
{{- if and .Values.session.id .Values.session.requireClaimedNodes }}
{{- $_ := set $sel "pacer.io/session" .Values.session.id }}
{{- end }}
{{- with $sel }}{{- toYaml . }}{{- end }}
{{- end -}}

{{- define "pacer.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "pacer.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Parse a binary memory quantity into bytes, in EITHER dialect this chart carries,
because it has two and a value that spans them is how a derived size goes wrong.

  * k8s resource quantities — `Gi`/`Mi`/`Ki` — used by resources.limits.memory,
    efa.pinnedPoolReservation, efa.hugepages;
  * the daemon's own config format — `GiB`/`MiB`/`KiB` — used by
    config.memCapacity (`1GiB`), config.chunkSize (`16MiB`),
    cluster.rdmaArenaBytes, cluster.cacheSlabBytes.

**Longest suffix first**, matching `parse_bytes` in crates/pacer-daemon/src/config.rs:
tested the other way round, `8GiB` matches neither `Gi` (it ends in `B`) nor a bare
integer, and silently yields 0 — which is exactly the failure mode a derived slab size
must not have, since it feeds both the ConfigMap and the cgroup limit. A bare integer
passes through.

Used by pacer.memoryLimit and pacer.cacheSlabBytes to sum quantities, which sprig
cannot do on the suffixed strings directly.
*/}}
{{- define "pacer.toBytes" -}}
{{- $q := . | toString | trim -}}
{{- if hasSuffix "GiB" $q -}}
{{- mul (trimSuffix "GiB" $q | int64) 1073741824 -}}
{{- else if hasSuffix "MiB" $q -}}
{{- mul (trimSuffix "MiB" $q | int64) 1048576 -}}
{{- else if hasSuffix "KiB" $q -}}
{{- mul (trimSuffix "KiB" $q | int64) 1024 -}}
{{- else if hasSuffix "Gi" $q -}}
{{- mul (trimSuffix "Gi" $q | int64) 1073741824 -}}
{{- else if hasSuffix "Mi" $q -}}
{{- mul (trimSuffix "Mi" $q | int64) 1048576 -}}
{{- else if hasSuffix "Ki" $q -}}
{{- mul (trimSuffix "Ki" $q | int64) 1024 -}}
{{- else -}}
{{- $q | int64 -}}
{{- end -}}
{{- end -}}

{{/*
Effective ADR-0028 cache-slab size in bytes, or "" for no slab.

**This is where the slab is turned ON by default**, and it is deliberately gated on
hugepages rather than on efa.enabled, because ADR-0028 calls hugepages a *precondition*
and the arithmetic says why. Registration is per PAGE and repeated once per RAIL over
the same pages, so the cost is `bytes x rails / rate`: measured at 239-433 GB/s on
hugepages versus 12-14 GB/s on 4 KiB pages. A 132 GiB slab on 32 rails is ~19 s of
startup on 2 MiB pages and ~300 s on base pages — the second trips the startup probe and
CrashLoops the pod. So a slab is only defaulted on where `efa.hugepages` is set, which is
also the operator's signal that the node pre-reserves them.

Resolution order:
  1. cluster.cacheSlabBytes set explicitly -> that value, whatever it is. An operator who
     names a size gets it, including a deliberately small one for a control arm.
  2. efa.enabled AND efa.hugepages set -> DERIVED (see below).
  3. otherwise -> "" (no slab; cached chunks stay on the heap and holders stage their
     WRITEs, exactly as before ADR-0028).

The derivation is `config.memCapacity + HOLDER_SERVE_SLOTS x config.chunkSize`, which is
the ADR's sizing rule made concrete: a frame is held by foyer's resident set PLUS every
chunk in flight, and the serve-admission gate bounds in-flight bodies at
HOLDER_SERVE_SLOTS (256, crates/pacer-daemon/src/peer.rs). Sized at memCapacity exactly,
a full cache leaves no free frame and every later fill silently takes the heap.

Not a guess: the churn gate ran precisely this shape — memCapacity 2 GiB (128 frames)
plus 256 x 16 MiB = a 6 GiB slab of 384 frames — and recorded **0 heap fallbacks while
recycling frames 106x** (bench/ladder/results/adr28-churn-gate.md). The 256 term is
therefore the measured headroom, not a safety factor.

Cost to be aware of: the slab RELOCATES the RAM tier into pinned pages rather than adding
a tier, but it is invisible to memCapacity's accounting, so pacer.memoryLimit adds it to
the cgroup limit and efa.hugepages must cover it alongside the arenas. pacer.validate
fails the render when it does not.
*/}}
{{- define "pacer.cacheSlabBytes" -}}
{{/*
"Set" must mean SET, including set to zero. A bare `if .Values.cluster.cacheSlabBytes`
treats an explicit `--set cluster.cacheSlabBytes=0` as unset, because Helm coerces that to
the integer 0 and Go templates call 0 falsey — so the documented escape hatch would
silently re-derive a slab instead of disabling one. Test for absent (nil) or empty string
instead, which is what the values file actually ships.
*/}}
{{- $explicit := .Values.cluster.cacheSlabBytes -}}
{{- if and (not (kindIs "invalid" $explicit)) (ne ($explicit | toString | trim) "") -}}
{{- include "pacer.toBytes" $explicit -}}
{{- else if and .Values.efa.enabled .Values.efa.hugepages -}}
{{- $mem := include "pacer.toBytes" .Values.config.memCapacity | int64 -}}
{{- $chunk := include "pacer.toBytes" (.Values.config.chunkSize | default "16MiB") | int64 -}}
{{- add $mem (mul 256 $chunk) -}}
{{- end -}}
{{- end -}}

{{/*
Effective foyer submit-queue threshold in bytes — the in-flight DRAM→NVMe write budget.

Derived rather than left to foyer's own default because that default is 16 MiB, which
equals exactly ONE chunk entry at the shipped chunkSize: about two chunks fit, and every
further enqueue is **silently dropped** (foyer counts storage_queue_channel_overflow, the
entry never reaches the disk tier, and a later read misses to a peer or to S3). Not a
tuning preference — bench/ladder/results/c4-foyer-readpath.md recorded 4229 and 2244 drops
on foyer's default and exactly 0 once this was sized, twice, deterministically.

The rule is the write fan-out actually wanted, `flushers x chunkSize x depth`, so it tracks
both knobs it depends on: raising chunkSize (a 64 MiB chunk would otherwise be 4x past
foyer's whole default) or flushers cannot silently re-create the drops.

Resolution order:
  1. config.tuning.submitQueueThreshold set explicitly -> that value, whatever it is,
     including `0` for "keep foyer's own 16 MiB" (a control arm has to be able to ask
     for the historical behaviour).
  2. otherwise -> DERIVED, as above.
*/}}
{{- define "pacer.submitQueueThreshold" -}}
{{- $t := .Values.config.tuning | default dict -}}
{{/*
"Set" must mean SET, including set to zero — same trap as pacer.cacheSlabBytes: Helm
coerces `--set config.tuning.submitQueueThreshold=0` to the integer 0, which Go templates
call falsey, so a bare `if` would re-derive a budget for the operator who asked for
foyer's default. Test for absent (nil) or empty string instead.
*/}}
{{- $explicit := $t.submitQueueThreshold -}}
{{- if and (not (kindIs "invalid" $explicit)) (ne ($explicit | toString | trim) "") -}}
{{- $explicit -}}
{{- else -}}
{{- $chunk := include "pacer.toBytes" (.Values.config.chunkSize | default "16MiB") | int64 -}}
{{/*
foyer's own default when flushers is 0/unset is ONE flusher, so that is the floor to
derive against — never 0, which would emit a threshold of 0 and mean "foyer's default"
to the daemon, i.e. the bug this helper exists to close.
*/}}
{{- $flushers := $t.flushers | default 1 | int64 -}}
{{/*
Chunk-fill depth each flusher is sized for. 4 because that is the reader look-ahead the
C4 ladder found to be the knee (bench/ladder/results/c4-fanout-depth.md: depth 4 is where
delivery peaks, depth 8 buys +5.4 % for double the queue), so `flushers x chunk x 4` is
the demotion burst a fill at the knee can actually produce.
*/}}
{{- $depth := 4 -}}
{{- mul $flushers $chunk $depth -}}
{{- end -}}
{{- end -}}

{{/*
Fail the render when efa.hugepages cannot cover everything that maps from it.

Every hugepage-backed mapping draws on the ONE pod request, and the arenas map FIRST
(build_rails registers them before the slab), so a shortfall lands on the transport's
correctness floor rather than on ADR-0028's optimization. Worse, a hugepage request that
cannot be honored **degrades to 4 KiB pages with a warning rather than failing** — which
for the slab is a silent invalidation, and at production sizes a startup-probe timeout.

So the arithmetic is checked at install time, where it is actionable and cannot corrupt
anything, instead of being left to a log line nobody reads:

  requester arena  cluster.rdmaArenaBytes (transport default 4Gi)
  holder arena     HOLDER_ARENA_RANGES (256) x config.chunkSize
  ADR-0028 slab    pacer.cacheSlabBytes

Only checked when efa.hugepages is set: unset means no slab is derived and the arenas map
on base pages, which is the pre-hugepage behaviour and not an error.
*/}}
{{- /*
Refuse a configuration that can only produce a gRPC-only daemon.

`shareHostDevices` mounts /dev/infiniband but grants no device-cgroup rule, and only an
allocation or `privileged` does — so `enabled + shareHostDevices` without either mounts 32
visible interfaces the daemon cannot open. Measured on a p5 with 32 units free (2026-08-24):
the daemon logged `rail placement resolved rails=32`, failed the capability probe with EPERM,
and served every delivery as a body. Nothing about that reads as a misconfiguration at
runtime — it reads as the delivery path declining — so it is caught here instead.

When a plugin or DRA driver grants the rule some other way (ADR-0030 point 9 option 3), this
guard needs a third branch naming that knob.
*/ -}}
{{/*
Is the ADR-0032 write scatter ON for this release? `"true"` or `""`.

`scatter.enabled` is a THREE-state knob, and this helper is the one place that resolves it:
an explicit value wins, and `null` — the shipped default — means **on where the design
applies**, i.e. on a general-purpose (Standard) backend and off on an Express directory
bucket. Every gate in ADR-0032 § Phases passed on hardware (gate 4.5 on 2026-09-01 at
4.73×), so the mechanism no longer has to be asked for; what it still must not do is turn
itself on where it cannot run.

**A bare `enabled: true` default would have broken every Express deployment** — Express is
`config.backendType`'s own default, `pacer.validateScatter` FAILS the render on that
combination and the daemon `bail!`s at startup — so the default cannot be a boolean flip. It
has to be scoped, and scoped in a way that keeps an explicit ask failing loudly rather than
being silently downgraded.

Truthiness deliberately mirrors `crates/pacer-daemon/src/config.rs`: only `1/true/on/yes`
count, so a typo leaves the scatter in whatever state the backend implies rather than
flipping it. Anything else explicit (`false`, `0`, `no`) turns it OFF on any backend — and
the ConfigMap then emits that `false` explicitly, because the daemon derives the same
default from the same rule and would otherwise turn it back on. See
`pacer.scatterConfigured`.
*/}}
{{- define "pacer.scatterEnabled" -}}
{{- $raw := .Values.scatter.enabled -}}
{{- if and (not (kindIs "invalid" $raw)) (ne ($raw | toString | trim) "") -}}
{{- if has ($raw | toString | trim | lower) (list "1" "true" "on" "yes") -}}true{{- end -}}
{{- else if ne (.Values.config.backendType | default "express") "express" -}}
true
{{- end -}}
{{- end -}}

{{/*
Should the ConfigMap carry a `scatter:` block at all? `"true"` or `""`.

Two cases need one, and they are not the same case: the scatter being ON, and an operator
having explicitly turned it OFF. The second is not redundant — the daemon applies the SAME
backend-scoped default when the file says nothing, so an omitted block on a Standard
backend would turn the scatter back on and silently overrule the operator.

Left empty for the one remaining combination — Express with the knob unset — so that every
existing Express release renders a byte-identical ConfigMap and its pods do not roll for a
default that does not apply to them.
*/}}
{{- define "pacer.scatterConfigured" -}}
{{- $raw := .Values.scatter.enabled -}}
{{- if or (include "pacer.scatterEnabled" .) (and (not (kindIs "invalid" $raw)) (ne ($raw | toString | trim) "")) -}}
true
{{- end -}}
{{- end -}}

{{/*
Effective ADR-0032 staging budget in bytes when the write scatter is on, else "".

Derived rather than left to the daemon's own default because this number is charged
to the cgroup twice over: it is heap `Bytes` a node holds on behalf of OTHER nodes
(crates/pacer-daemon/src/staging.rs — `Staged { body: Bytes }`), and like the arenas
and the ADR-0028 slab it is invisible to config.memCapacity's accounting. So
pacer.memoryLimit has to add it, which means the chart must KNOW it — and the only
way for the budgeted number and the enforced number to be the same number is to emit
this one into the ConfigMap as well.

The 2GiB default restates crates/pacer-daemon/src/scatter.rs DEFAULT_STAGING_BYTES.
That duplication is the price of budgeting for it; the ConfigMap emission is what
keeps it from drifting silently — a daemon whose default changed would still run at
the size this helper budgeted, not at a size nobody accounted for.
*/}}
{{- define "pacer.scatterStagingBytes" -}}
{{- if include "pacer.scatterEnabled" . -}}
{{- $explicit := .Values.scatter.stagingBytes -}}
{{- if and (not (kindIs "invalid" $explicit)) (ne ($explicit | toString | trim) "") -}}
{{- include "pacer.toBytes" $explicit -}}
{{- else -}}
{{- 2147483648 -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The bytes a COORDINATOR may hold in buffered windows, or "" when the scatter is off.

**`scatter.windowsInFlight` IS a byte bound, as of commit ebe07c07 (2026-08-31).**
`ScatterCoordinator::dispatch` (crates/pacer-daemon/src/coordinate.rs) is `async` and takes
the permit BEFORE it hands the window's bytes to a task; the single task that reads the
client's body awaits `dispatch`, so a full pipeline stops `run_pipeline` reaching its next
`body.next()` and the client is backpressured through TCP. The daemon's docstring on that
field is accurate for the first time. It was NOT true when this helper was written: the
permit used to be awaited inside the spawned upload, after the window had been allocated
and moved into it, so the semaphore bounded concurrent UPLOADS and a coordinator could
buffer the whole object per in-flight PUT.

Two facts decide the arithmetic, and both are easy to state slightly wrong:

  * **The residual is NOT `(windowsInFlight + 1) × chunkSize`.** Held bytes are
    `windowsInFlight × chunkSize` PLUS the splitter's under-one-window remainder PLUS the
    one frame `body.next()` last yielded — and that last term is the SENDER's framing, not
    ours. Over a network it is tens of KiB; an in-process caller can hand over the whole
    object in one frame, so no ordering can bound it and NO HELM TEMPLATE CAN COMPUTE IT.
    The budget below does not cover that term and does not pretend to.
  * **The semaphore is NODE-WIDE, not per-PUT.** One ScatterCoordinator is constructed per
    daemon (main.rs `attach_scatter`, one call site) holding one `Arc<Semaphore>`, and
    `scatter(&self, ...)` takes `&self` — so every concurrent PUT shares it. Concurrency
    therefore does NOT multiply `windowsInFlight × chunkSize`; it multiplies only the
    per-PUT residual above, since each in-flight PUT has its own WindowSplitter.

Measured cost of the old ordering, kept because it is why this term exists at all
(bench/ladder/results/w1-write-scatter.md, 2026-08-27, five `r8gd.24xlarge`): `bal-c4` —
five writers at client concurrency 4 over 160 GiB — ran at the best aggregate that session
recorded, and `bal-c8` at concurrency **8 over the same 160 GiB, same fleet, same objects,
same limit** had **three of five daemons `OOMKilled`** against the 53 GiB this chart
computed. The discriminating variable was CONCURRENCY, not bytes moved: 64 × 16 MiB = 1 GiB
budgeted against up to 8 × 4 GiB = **32 GiB** actually held.

Resolution order — UNCHANGED by ebe07c07, deliberately, and the next block says why:
  1. scatter.coordinatorReservation set -> that value, including `0` for "add nothing"
     (the pre-2026-08-27 arithmetic, for a control arm).
  2. otherwise -> `coordinatorConcurrency × perPut`, where `perPut` is
     scatter.coordinatorObjectBytes when the operator has declared the largest object this
     node will coordinate, and `windowsInFlight × chunkSize` when they have not.

⚠ **WHY THIS STILL OVER-BUDGETS, AND WHY THAT IS NOT AN OVERSIGHT.** Since ebe07c07 the
fallback in (2) is the real node-wide bound, so `coordinatorObjectBytes` is no longer what
makes the limit safe: it is **voluntary headroom, retained pending a hardware arm.**
Deleting it now would take the ladder's w1 stack at the 2026-08-27 sweep's own `zero-c16`
shape (`-f bench/ladder/values-w1.yaml`, stagingBytes 8GiB, windowsInFlight 64, concurrency
16 over 4 GiB objects) from **84.000 GiB to 21.000 GiB** — the scatter terms themselves from
72 GiB to 9 GiB, exactly **8×** — on a derivation that has already been WRONG TWICE. And
the term ebe07c07 did NOT remove is page cache: the scatter caches every window it uploads
and stages, both disk tiers write buffered, so it is real, charged to this cgroup,
unbudgeted, present in **both** `bal-c4` and `bal-c8` — which is why it cannot be that
pair's discriminator and equally cannot be ruled out as a contributor — and **unmeasured**,
since the arm predates cgroup.rs.

So the number shrinks in a LATER commit, and that commit carries the hardware arm that
earns it: peak RSS and `pacer_cgroup_memory_file_bytes` on a re-run of `bal-c8`. Until
those two series exist this helper is deliberately generous, and every rendered limit is
byte-identical to what the measured fleets ran with.

What breaks if these are wrong: too small and the daemon OOMKills (exit 137) on the WRITE
path, which surfaces client-side as a truncated body or a vanished endpoint and never as a
memory problem. Too large and the DaemonSet is unschedulable, which says so immediately.
*/}}
{{- define "pacer.scatterCoordinatorBytes" -}}
{{- if include "pacer.scatterEnabled" . -}}
{{/*
"Set" must mean SET, including set to zero — the same trap pacer.cacheSlabBytes and
pacer.submitQueueThreshold each carry a comment about: Helm coerces
`--set scatter.coordinatorReservation=0` to the integer 0, which Go templates call falsey,
so a bare `if` would re-derive the term and take the documented control arm away.
*/}}
{{- $explicit := .Values.scatter.coordinatorReservation -}}
{{- if and (not (kindIs "invalid" $explicit)) (ne ($explicit | toString | trim) "") -}}
{{- include "pacer.toBytes" $explicit -}}
{{- else -}}
{{- $chunk := include "pacer.toBytes" (.Values.config.chunkSize | default "16MiB") | int64 -}}
{{/*
Restates crates/pacer-daemon/src/scatter.rs DEFAULT_WINDOWS_IN_FLIGHT (16), for the same
reason pacer.submitQueueThreshold floors `flushers` at foyer's own 1: the chart has to
budget the number the daemon will actually run, not the empty string in the values file.
*/}}
{{- $windows := .Values.scatter.windowsInFlight | toString | trim -}}
{{- if or (eq $windows "") (eq $windows "0") -}}{{- $windows = "16" -}}{{- end -}}
{{- $perPut := mul ($windows | int64) $chunk -}}
{{- $object := .Values.scatter.coordinatorObjectBytes -}}
{{- if and (not (kindIs "invalid" $object)) (ne ($object | toString | trim) "") -}}
{{- $perPut = include "pacer.toBytes" $object | int64 -}}
{{- end -}}
{{/*
Concurrency 1 is the shape a training save actually has (one PUT per rank at a time) and
the ladder's own default (LADDER_WRITE_CONCURRENCY, bench/ladder/scatter.sh), so it is the
default here too. Since ebe07c07 the semaphore fallback above is a real bound at EVERY
concurrency, because the semaphore is node-wide — this factor now buys headroom rather than
correctness, and the block above the helper is where that is argued.

`default 1` rather than the kindIs/trim dance the string knobs use, and deliberately: nil
(an overlay nulling the key), "" and 0 must ALL mean one PUT, because a concurrency of zero
is not a control arm — it is a term of zero multiplying the whole footprint away, which is
the arithmetic this helper exists to stop. `coordinatorReservation: 0` is the control arm.

⚠ Two ADJACENT template-comment blocks would emit the newline between them into this
helper's output, and `int64` reads a leading newline as 0 — i.e. the term would silently
vanish, which is this defect all over again. Keep prose in ONE block per gap, and never
write a literal comment-close sequence inside one: it ends the comment there and dumps the
remaining prose into the rendered value. Both mistakes were made writing this helper, and
neither is visible in a diff.
*/}}
{{- $conc := .Values.scatter.coordinatorConcurrency | default 1 | int64 -}}
{{- mul $conc $perPut -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Refuse the write scatter on an Express backend at RENDER time.

The daemon already refuses it at startup (ADR-0032 § 6 scopes the design to
general-purpose buckets, and config.rs bails rather than downgrading), so without
this the failure mode is a CrashLoopBackOff and a `helm --wait` that dies on
"context deadline exceeded" — five minutes to learn what a render can say in one
line. Same reasoning as validateEfaAccess: a configuration that cannot work should
fail where the operator is still looking at it.

There used to be a SECOND check here, added after the 2026-08-27 OOMKill: it failed the
render when `coordinatorConcurrency > 1` with neither `coordinatorObjectBytes` nor
`coordinatorReservation` set, on the grounds that the fallback in
pacer.scatterCoordinatorBytes was only the semaphore's nominal bound. **Deleted with
ebe07c07 (2026-08-31), which made that fallback real** — `dispatch` takes the permit before
the window's bytes and the body reader awaits it, so `windowsInFlight × chunkSize` bounds a
node's buffered windows whatever its concurrency. The check was refusing a footprint that
no longer exists, and it made the 2026-08-27 arm's own shape unrenderable for the wrong
reason. The three `coordinator*` knobs stay and their arithmetic is unchanged: read the
headroom argument above pacer.scatterCoordinatorBytes before touching any of them.
*/}}
{{- define "pacer.validateScatter" -}}
{{- if and .Values.scatter.enabled (eq (.Values.config.backendType | default "express") "express") -}}
{{- fail "scatter.enabled is set but config.backendType is \"express\": the write scatter is scoped to general-purpose (Standard) buckets — ADR-0032 § 6 supersedes ADR-0007 only there, and a directory bucket's write path stays the plain proxy. Set config.backendType=standard (and point config.bucketMap.cache at a regional bucket), or leave the scatter off. The daemon refuses this at startup too, so rendering it would only buy you a CrashLoop." -}}
{{- end -}}
{{- end -}}

{{/*
"true" when networkPolicy.clients admits every pod in the cluster — i.e. when the policy
renders but scopes :9000 no more tightly than having no policy at all. NOTES.txt says so
out loud, because that is the shipped default and the difference between "we installed the
policy" and "we scoped the node's IAM identity" is invisible otherwise.

Wide-open means an entry with no label selection AND no ipBlock. Each sub-test needs its
own emptiness check rather than plain truthiness: `namespaceSelector: {}` — the default,
and precisely the wide-open case — is an EMPTY map, which Go templates treat as false, so
`if .namespaceSelector` skips exactly the shape being looked for.
*/}}
{{- define "pacer.networkPolicyClientsWideOpen" -}}
{{- range .Values.networkPolicy.clients -}}
{{- if not .ipBlock -}}
{{- $ns := .namespaceSelector | default dict -}}
{{- $pod := .podSelector | default dict -}}
{{- if and (not (or (get $ns "matchLabels") (get $ns "matchExpressions"))) (not (or (get $pod "matchLabels") (get $pod "matchExpressions"))) -}}
true
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Catch the two `networkPolicy` shapes that deny rather than restrict.

An ingress rule is only emitted for a port when its peer list is non-empty, so an EMPTY
list is not "no restriction on this port" — it is "no rule admits this port", i.e. deny.
That inversion is invisible in the rendered object (the port simply isn't mentioned) and
surfaces as connection timeouts from every client, or as a CrashLooping DaemonSet when it
is the probe path. Fail at render time with the alternative spelled out instead.
*/}}
{{- define "pacer.validateNetworkPolicy" -}}
{{- if not .Values.networkPolicy.clients -}}
{{- fail "networkPolicy.clients is empty, which DENIES :9000 rather than leaving it open: a port with no ingress rule is unreachable, so no client can be served and the symptom is a connection timeout with a healthy-looking daemon. If you meant to deny it, set networkPolicy.enabled=false and write your own policy — the chart will not render a deny it cannot distinguish from a mistake. If you meant no restriction, the shipped default is `clients: [{namespaceSelector: {}}]`." -}}
{{- end -}}
{{- if not .Values.networkPolicy.probeCidrs -}}
{{- fail "networkPolicy.probeCidrs is empty, which DENIES :9090: the kubelet's startup/liveness probes arrive from the node's own address and match no podSelector, so the container fails its startupProbe and CrashLoops forever with nothing naming the policy as the cause. Narrow it to your node subnets (e.g. [\"10.0.0.0/16\"]) instead of emptying it; networkPolicy.metrics is the knob for scrapers, and it may be empty." -}}
{{- end -}}
{{- end -}}

{{- define "pacer.validateEfaAccess" -}}
{{- if and .Values.efa.enabled .Values.efa.shareHostDevices (not .Values.efa.privileged) -}}
{{- fail "efa.shareHostDevices mounts /dev/infiniband but cannot make it openable: the kubelet's device cgroup admits only devices a plugin ALLOCATED, so every ibv_open_device returns EPERM and the daemon runs gRPC-only while reporting its rails placed. Either set efa.privileged=true (the supported mechanism — it consumes no vpc.amazonaws.com/efa unit; see ADR-0030 point 9), or set efa.shareHostDevices=false so the pod requests efa.count units from the device plugin instead. Turning efa.enabled off is the third option if this daemon is not meant to do RDMA." -}}
{{- end -}}
{{- end -}}

{{- define "pacer.validateHugepages" -}}
{{- if and .Values.efa.enabled .Values.efa.hugepages -}}
{{- $chunk := include "pacer.toBytes" (.Values.config.chunkSize | default "16MiB") | int64 -}}
{{- $arena := include "pacer.toBytes" (.Values.cluster.rdmaArenaBytes | default "4Gi") | int64 -}}
{{- $holder := mul 256 $chunk -}}
{{- $slab := include "pacer.cacheSlabBytes" . -}}
{{- $need := add $arena $holder ($slab | default "0" | int64) -}}
{{- $have := include "pacer.toBytes" .Values.efa.hugepages | int64 -}}
{{- if lt $have $need -}}
{{- fail (printf "efa.hugepages=%s is too small: the requester arena (%d bytes), the holder arena (256 x chunkSize = %d) and ADR-0028's cache slab (%d) all map from it, needing %d bytes. Raise efa.hugepages to at least %dGi (and the node's boot-time reservation with it), or set cluster.cacheSlabBytes=0 to run without the slab. A request that cannot be honored degrades to 4 KiB pages, which for the slab means a silent invalidation and, at this size, a startup-probe timeout." .Values.efa.hugepages $arena $holder ($slab | default "0" | int64) $need (divf $need 1073741824 | ceil | int64)) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Effective per-request delivery ceiling in bytes — `delivery.maxTargetBytes` resolved.

Wanted as a NUMBER because pacer.validateDelivery compares it against the node-wide
ceiling, and the two are written in different dialects across this repo's overlays
(`4Gi` here, `8GiB` elsewhere) — a string comparison would rank `8GiB` below `16Gi`
lexically and pass a configuration that cannot work.

Resolution order:
  1. delivery.maxTargetBytes set explicitly -> that value.
  2. otherwise -> the daemon's own default, restated below.

The values file SHIPS 4Gi, so branch 2 fires only for an install that blanks it
deliberately. It still has to be right: the guard's job is to compare the number the
daemon will actually enforce, not the number the chart happens to have written down.
*/}}
{{- define "pacer.deliveryMaxTargetBytes" -}}
{{- $explicit := .Values.delivery.maxTargetBytes -}}
{{- if and (not (kindIs "invalid" $explicit)) (ne ($explicit | toString | trim) "") -}}
{{- include "pacer.toBytes" $explicit -}}
{{- else -}}
{{/*
Restates crates/pacer-daemon/src/delivery.rs DEFAULT_MAX_TARGET_BYTES (1 GiB). The
duplication is the price of checking the relationship at render time; see
pacer.scatterStagingBytes for the same trade on the staging budget.
*/}}
{{- 1073741824 -}}
{{- end -}}
{{- end -}}

{{/*
Effective node-wide pinned-client-bytes ceiling in bytes — `delivery.pinnedBytesMax`
resolved, restating crates/pacer-daemon/src/delivery.rs DEFAULT_PINNED_BYTES_MAX (8 GiB)
when it is left empty.

This is the number that actually bounds what a client can make this daemon pin, which is
why pacer.validateDelivery measures the per-request ceiling AND the cgroup reservation
against it rather than against each other.
*/}}
{{- define "pacer.deliveryPinnedBytesMax" -}}
{{- $explicit := .Values.delivery.pinnedBytesMax -}}
{{- if and (not (kindIs "invalid" $explicit)) (ne ($explicit | toString | trim) "") -}}
{{- include "pacer.toBytes" $explicit -}}
{{- else -}}
{{- 8589934592 -}}
{{- end -}}
{{- end -}}

{{/*
Refuse a delivery configuration whose quotas cannot mean what they say.

Two checks, both about numbers that must move together and historically have not:

  1. `maxTargetBytes > pinnedBytesMax`. DeliveryQuota::reserve tests the per-request
     ceiling FIRST and the node-wide total second, so a target between the two passes one
     gate and fails the other — the operator's per-request ceiling is dead configuration
     above the node-wide one, and it fails as an over-quota DECLINE (a body-delivered
     read), i.e. as the delivery path merely looking slow. This is the exact trap raising
     the quota for a 70B embedding walks into: `maxTargetBytes` is the knob the model
     forces you to touch, and it is not the one that bounds the node.
  2. `pinnedReservation < pinnedBytesMax`. Pinned client pages are invisible to
     config.memCapacity, so pacer.memoryLimit adds pinnedReservation to the cgroup limit;
     if that is smaller than what the daemon will let clients pin, the limit is not a
     limit and the pod OOMKills under load — the planning/16 §4.5 failure with a different
     class of pages. values.yaml has said "should match" since ADR-0026 landed; this makes
     it checkable.

Same reasoning as validateScatter and validateEfaAccess: a configuration that cannot work
should fail where the operator is still looking at it, not become a slow path nobody can
attribute.
*/}}
{{- define "pacer.validateDelivery" -}}
{{- if .Values.delivery.enabled -}}
{{- $target := include "pacer.deliveryMaxTargetBytes" . | int64 -}}
{{- $nodeWide := include "pacer.deliveryPinnedBytesMax" . | int64 -}}
{{- if gt $target $nodeWide -}}
{{- fail (printf "delivery.maxTargetBytes (%d bytes) is above delivery.pinnedBytesMax (%d bytes): the node-wide gate is checked second, so every target in between passes the per-request ceiling and is then DECLINED for the node-wide one — a body-delivered read that looks like the delivery path being slow rather than a misconfiguration. Raise delivery.pinnedBytesMax to at least the per-request ceiling (and delivery.pinnedReservation with it, since the cgroup limit has to cover pinned client pages), or lower delivery.maxTargetBytes." $target $nodeWide) -}}
{{- end -}}
{{- $reservation := include "pacer.toBytes" .Values.delivery.pinnedReservation | int64 -}}
{{- if lt $reservation $nodeWide -}}
{{- fail (printf "delivery.pinnedReservation (%d bytes) is below delivery.pinnedBytesMax (%d bytes): pinned client pages are invisible to config.memCapacity, so pacer.memoryLimit adds the RESERVATION to the container limit while the daemon admits up to the CEILING — the cgroup limit then stops being one and the pod OOMKills under load (planning/16 §4.5, with client pages in place of the arenas). Set delivery.pinnedReservation to at least delivery.pinnedBytesMax." $reservation $nodeWide) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The delivery path's OWN in-flight footprint in bytes, or "" when delivery is off.

**Read the caveat before trusting this number.** It is the fourth term
pacer.memoryLimit adds for something config.memCapacity cannot see, alongside the arenas,
the ADR-0028 slab and ADR-0032's staging budget — and unlike those three it is a FLOOR
rather than the working set. The measured working set is much larger and is NOT derivable
from values at all; see the sizing rule above `resources:` in values.yaml for what the
operator has to add by hand and why.

What IS derivable is the one bound in the code: `Proxy::run_delivery` buffers
`min(delivery.parallelism, windows)` chunk resolutions for ONE span, and each in-flight
window holds a chunk body. So `parallelism x chunkSize` is the heap the delivery fan-out
can hold at once, and it tracks both knobs that set it — an install that raises
`delivery.parallelism` to 256 at a 64 MiB chunk is asking for 16 GiB of in-flight bodies,
which is worth having in the limit even though it is not the whole story.

Resolution order, the same shape as pacer.cacheSlabBytes and pacer.scatterStagingBytes:
  1. delivery.workingSetReservation set explicitly -> that value, including `0` for "add
     nothing" (a control arm has to be able to ask for the pre-fix arithmetic).
  2. otherwise -> DERIVED as above.

Only computed when delivery.enabled: with ADR-0026 off no client names a target, no window
is ever buffered, and the body path's own look-ahead is `config.fillParallelism` — a
different bound that the base resources.limits.memory has always covered.
*/}}
{{- define "pacer.deliveryWorkingSetBytes" -}}
{{- if .Values.delivery.enabled -}}
{{- $explicit := .Values.delivery.workingSetReservation -}}
{{- if and (not (kindIs "invalid" $explicit)) (ne ($explicit | toString | trim) "") -}}
{{- include "pacer.toBytes" $explicit -}}
{{- else -}}
{{- $chunk := include "pacer.toBytes" (.Values.config.chunkSize | default "16MiB") | int64 -}}
{{/*
Restates crates/pacer-daemon/src/delivery.rs DEFAULT_DELIVERY_PARALLELISM (64) for the
same reason pacer.submitQueueThreshold floors `flushers` at foyer's own 1: the chart has
to budget the number the daemon will actually run, not the empty string in the values file.
*/}}
{{- $parallelism := .Values.delivery.parallelism | toString | trim -}}
{{- if or (eq $parallelism "") (eq $parallelism "0") -}}{{- $parallelism = "64" -}}{{- end -}}
{{- mul ($parallelism | int64) $chunk -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Effective container memory limit, in bytes.

⚠ **THIS SUM NOW HAS A SECOND IMPLEMENTATION, AND THEY MUST MOVE TOGETHER.**
`crates/pacer-daemon/src/memory_budget.rs` re-derives it at startup from the daemon's
own resolved configuration and refuses to start when the terms do not fit the cgroup
limit this helper produced (quality item R3) — because a chart can compute a limit but
cannot check its own arithmetic against the one the kubelet applied, and every recorded
daemon OOM kill left nothing in the log to say which term was wrong.

Each `Term` in that module names the line here it mirrors, and a unit test
(`helper_terms_are_all_present_in_the_chart`) reads this file out of the repo and fails
the build if a helper named there is renamed or deleted here. So: **adding a term below
means adding it there too**, and the reverse — a term added on one side alone either
fails that test or, worse, makes the daemon stricter than the limit this file renders,
which refuses a whole fleet's start. The module's header records which class of OOM the
runtime check catches (a configuration that provably never fitted) and which four it
cannot (page cache, a measured working set, allocator retention, code exceeding its own
declared bound), so read it before assuming a new term belongs in the enforced sum.

config.memCapacity is NOT the whole footprint: when the EFA RDMA transport is
enabled the daemon pins two buffer pools (requester + holder) at startup,
DEFAULT_POOL_SLOTS x DEFAULT_SLOT_BYTES each (crates/pacer-transport/src/efa/
mod.rs — 64 x 64 MiB x 2 = 8 GiB by default), and that pinned memory is
invisible to the cache's own memCapacity budget. Omitted from the cgroup limit
it OOMKills the pod under seed load (planning/16 §4.5). So when efa.enabled we
add the pinned-pool total (efa.pinnedPoolReservation) on top of the operator's
base resources.limits.memory. When EFA is off there are no pinned pools and the
base limit is emitted unchanged.

Client-memory delivery (ADR-0026) pins a second, independent class of pages: a
window of a segment a CLIENT allocated, mapped and registered by the daemon for
the life of a request. Those pages are equally invisible to memCapacity and to
the EFA arena accounting, so delivery.pinnedReservation is added on the same
terms. Both can be on at once, and then both are added.
*/}}
{{- define "pacer.memoryLimit" -}}
{{- $bytes := include "pacer.toBytes" .Values.resources.limits.memory | int64 -}}
{{- $pinned := false -}}
{{- if .Values.efa.enabled -}}
{{- $bytes = add $bytes (include "pacer.toBytes" .Values.efa.pinnedPoolReservation | int64) -}}
{{- $pinned = true -}}
{{- end -}}
{{- $slab := include "pacer.cacheSlabBytes" . -}}
{{- if $slab -}}
{{/*
ADR-0028's slab, added ONCE even though it is registered once per rail: all 32
registrations pin the same pages. It is added on top of pinnedPoolReservation rather
than being folded into it because the two are sized by different rules — the arenas by
rdmaArenaBytes + holder ranges, the slab by memCapacity + in-flight headroom — and an
operator raising memCapacity must not have to remember to raise a second knob. Omitting
it OOMKills the pod under seed load, the same way the arenas did (planning/16 §4.5):
memCapacity's own accounting cannot see pinned pages.
*/}}
{{- $bytes = add $bytes ($slab | int64) -}}
{{- $pinned = true -}}
{{- end -}}
{{- if .Values.delivery.enabled -}}
{{- $bytes = add $bytes (include "pacer.toBytes" .Values.delivery.pinnedReservation | int64) -}}
{{/*
The delivery path's own in-flight bodies, on top of the client pages it pins — a second,
independent footprint that this derivation was missing until a 70B arm OOMKilled the daemon
(exit 137) with the cache tier and the pinned pools all inside budget. Plain heap, not
pinned, and a FLOOR rather than the measured working set: see pacer.deliveryWorkingSetBytes
for what it does and does not cover, and the sizing rule above `resources:` in values.yaml
for the part no template can compute.
*/}}
{{- $bytes = add $bytes (include "pacer.deliveryWorkingSetBytes" . | int64) -}}
{{- $pinned = true -}}
{{- end -}}
{{- if include "pacer.scatterEnabled" . -}}
{{/*
ADR-0032's two write-path footprints, both heap and both invisible to memCapacity:

  staging      bytes this node holds for OTHER nodes between their UploadPart and the
               coordinator's commit (pacer.scatterStagingBytes). A HARD bound — the
               StagingArea refuses past the budget rather than queueing
               (crates/pacer-daemon/src/staging.rs), and an owner's refusal is the
               designed reject-fast, so this term is honest as written;
  coordinator  the buffered windows of this node's OWN in-flight PUTs
               (pacer.scatterCoordinatorBytes). A hard bound as of ebe07c07 — `dispatch`
               takes the windowsInFlight permit BEFORE the window's bytes and the body
               reader awaits it, so `windowsInFlight x chunkSize` really is the node-wide
               ceiling and the client stalls at it. Two residuals it does NOT cover: the
               splitter's sub-window remainder, and the last frame `body.next()` yielded,
               whose size is the SENDER's framing. This term is now deliberately larger
               than the bound; that headroom is argued in full above that helper, and it
               shrinks only with a hardware arm behind it.

Not pinned pages this time — plain allocations — but the cgroup does not care which, and
the failure mode is the one trap 23 in the harness notes already cost a paid arm: the
daemon OOMKills (exit 137) on the WRITE path while the read path's own arms fit
comfortably, and it surfaces client-side as a truncated body or a vanished endpoint rather
than as a memory problem.

**What is deliberately NOT added here, and why it is not an oversight: page cache.** The
scatter caches every window it uploads and every window it stages, and BOTH disk tiers
write buffered — `config.diskTier: store` makes only READS O_DIRECT
(crates/pacer-cache/src/store.rs: "Reads are O_DIRECT and writes are not"), and neither
tier drops the pages afterwards. So a write arm instantiates page cache under either
setting, it is charged to this cgroup, and this derivation must NOT branch on diskTier
hoping otherwise — that would under-budget exactly the arm that died. It is left out
because it is not a bound: the clean portion is reclaimable and scales with the bytes a
save moves, which no template can know. Rule 3 of the sizing block above `resources:` in
values.yaml is where the operator sizes it, and pacer_cgroup_memory_file_bytes is where
they watch it.
*/}}
{{- $bytes = add $bytes (include "pacer.scatterStagingBytes" . | int64) (include "pacer.scatterCoordinatorBytes" . | int64) -}}
{{- $pinned = true -}}
{{- end -}}
{{- if $pinned -}}
{{- $bytes -}}
{{- else -}}
{{- .Values.resources.limits.memory -}}
{{- end -}}
{{- end -}}

{{/*
The memory headroom fraction the DAEMON will enforce, resolved to a number.

config.memoryHeadroomFraction ships empty, and the daemon then applies its own
DEFAULT_HEADROOM_FRACTION (crates/pacer-daemon/src/memory_budget.rs). So a template that
wants to alert on the SAME inequality the startup check makes has to resolve the same
default, and the literal below is a mirror of that constant — held in lockstep by
`the_chart_mirrors_the_default_headroom_fraction` in that module, which reads this file out
of the repo and fails the build when the two drift. An alert computed from a different
headroom than the daemon enforces is worse than no alert: it either pages over
configurations that start perfectly well, or stays quiet over ones that will not start.

`default`, rather than a test for the empty string, and that is deliberate rather than
sloppy: the ConfigMap gates this key with a `with` block, which omits it for ANY falsy
value — so a literal 0 (a control arm asking for no margin) never reaches the daemon
either, and the daemon applies this same default to it. Mirroring the quirk is what keeps
the alert and the enforced check in agreement. Making a 0 reach the daemon is a separate
change, and it has to move both sides at once.
*/}}
{{- define "pacer.memoryHeadroomFraction" -}}
{{- .Values.config.memoryHeadroomFraction | default "0.10" -}}
{{- end -}}
