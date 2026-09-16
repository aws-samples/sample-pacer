# Benchmarks

Published measurements of PACER, with the method and the measurement scripts alongside
the numbers.

| benchmark | question | script |
|---|---|---|
| [http-path-vs-s3-standard.md](http-path-vs-s3-standard.md) | What is the cache worth on the plain HTTP/S3 path, versus reading the same objects directly from regional S3? | [`run-http-path-benchmark.sh`](run-http-path-benchmark.sh) |

## The contract these follow

Each benchmark here is expected to hold to the following. They are listed because a
number without them is not a measurement, and because they are the standard a reader
should hold the next one to.

1. **The baseline is the real alternative.** Not a degraded configuration of PACER, not
   a synthetic sink — what the workload would actually use instead.
2. **One variable.** Same client at the same version, same keys, same object size, same
   concurrency, same run length across every arm; only the path changes. A client
   version differing between arms is a defect, not a detail.
3. **The client's own number is the headline.** Server-side counters validate that the
   path was the path claimed; they never substitute for what the reader received.
4. **Warm-up is explicit and separate.** An arm that warms itself measures filling and
   serving together and reports the blend as serving.
5. **Repetitions, interleaved, with the range shown.** One sample is not a measurement
   on shared hardware. Arms are interleaved so drift over the session shows up as
   within-arm variance rather than as a between-arm difference. Ratios are quoted to the
   precision the observed spread supports and no further.
6. **Mechanical validity assertions abort the run.** Each arm proves from counters that
   it measured what it claims — in particular that a cache arm took **zero** bytes from
   the backend during measurement, so it cannot be flattered by the origin quietly
   serving part of the load.
7. **The raw client report ships with the result.** Parsers break silently when a tool's
   output format moves; the unedited log is the primary evidence.
8. **Limits are stated in the document, not discovered by the reader.** What was not
   measured, and what would move the result, is part of the result.

## Running these

The scripts provision no hardware and hard-code no bucket, account or cluster. Every
input that names your infrastructure is a required environment variable with no default,
so a script cannot silently run against the wrong thing.

They do assume a Kubernetes cluster already running PACER, and a bucket that already
holds a keyset. Fleet provisioning is deliberately out of scope: it is site-specific and
does not belong inside a measurement.
