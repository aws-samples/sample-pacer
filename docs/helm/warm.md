> How to run `pacer-daemon warm` as a Kubernetes Job. The design — the warm-only GET and why
> it is synchronous — is [ADR-0048](../adr/0048-warm-only-get.md).

# Warming the cache ahead of first read

A warm is a Job that runs the daemon's own image with `warm` as its first argument. You
submit it and move on; `kubectl get job` says when it is done, and its log has the account:
objects and bytes warmed, what was skipped and why, and every failure by name. It exits
non-zero if anything failed. Re-running it is safe and cheap, because an already-warm slice
is a cache hit.

```text
pacer-daemon warm (--endpoint URL | --proxy URL [--proxy-ca PATH]) [options]
                  [s3://bucket/prefix/ | s3://bucket/key]...
  --manifest PATH     also warm every s3:// URI listed in PATH, one per line
  --concurrency N     warm requests in flight at once (default 4)
  --slice SIZE        longest byte range one request covers (default 1Gi)
  --max-bytes SIZE    refuse to warm more than SIZE in total
  --dry-run           list what would be warmed, and warm nothing
```

A URI ending in `/`, or naming only a bucket, is every object under it. Anything else is one
object. Objects outside the cacheable size band (`config.minObjectSize`,
`config.maxObjectSize`) are skipped, not failed — a model directory's `config.json` is.

## Three things every warm Job needs

1. **A node the ring runs on.** The Service has `internalTrafficPolicy: Local`, so the Job
   must land on a node with a daemon. Give it the DaemonSet's own `nodeSelector` and
   tolerations.
2. **Admission through the NetworkPolicy.** `networkPolicy.clients` decides which pods may
   reach the daemon. If you have narrowed it, the Job's pods must match it.
3. **The client shape of your `auth.mode`**, below.

## `auth.mode: node`

The daemon is the S3 endpoint. Name buckets by their `config.bucketMap` alias and sign with
the placeholder credentials; the daemon reads S3 with its own identity.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: warm-llama-405b
  namespace: pacer
spec:
  backoffLimit: 2
  template:
    spec:
      restartPolicy: OnFailure
      nodeSelector:
        pacer.io/nodepool: cache            # the DaemonSet's nodeSelector
      containers:
        - name: warm
          image: ghcr.io/aws-samples/sample-pacer:<release>   # one that includes `warm` (ADR-0048)
          args:
            - warm
            - --endpoint
            - http://pacer.pacer.svc.cluster.local:9000
            - --max-bytes
            - 2Ti
            - s3://cache/models/llama-3-405b/
          env:
            - { name: AWS_ACCESS_KEY_ID, value: pacer }       # placeholders, not secrets
            - { name: AWS_SECRET_ACCESS_KEY, value: pacer }
            - { name: AWS_REGION, value: us-east-1 }
          resources:
            requests: { cpu: 250m, memory: 128Mi }
```

## `auth.mode: requester`

The daemon's TLS listener (`auth.requester.tls`) is a forwarding proxy. Name the real bucket
and sign with the Job's own credentials: S3 authorizes every object for that identity, so a
Job can warm only what it could read. Give the Job a ServiceAccount with that access (an EKS
Pod Identity association, for example), and the CA that signed the daemon's certificate.

The request goes to the daemon inside TLS, and the daemon opens its own TLS session to S3. No
hop is plaintext, and a `CONNECT` tunnel is never used — the daemon refuses one, because it
could not see into it. `--proxy` therefore accepts only an `https://` URL.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: warm-llama-405b
  namespace: training
spec:
  backoffLimit: 2
  template:
    spec:
      restartPolicy: OnFailure
      serviceAccountName: model-reader      # its identity must be able to read the source
      nodeSelector:
        pacer.io/nodepool: cache
      containers:
        - name: warm
          image: ghcr.io/aws-samples/sample-pacer:<release>
          args:
            - warm
            - --proxy
            - https://pacer.pacer.svc.cluster.local:9443
            - --proxy-ca
            - /etc/pacer-ca/ca.crt
            - s3://amzn-s3-demo-bucket--use2-az1--x-s3/models/llama-3-405b/
          env:
            - { name: AWS_REGION, value: us-east-2 }
          volumeMounts:
            - { name: pacer-ca, mountPath: /etc/pacer-ca, readOnly: true }
          resources:
            requests: { cpu: 250m, memory: 128Mi }
      volumes:
        - name: pacer-ca
          configMap:
            name: pacer-ca                  # holds ca.crt: the CA that signed the daemon's cert
```

## What a warm does not do yet

- **It fills one of each chunk's homes.** With `cluster.replicationR` above 1, a reader on
  another node asks the co-home the warm did not fill for about half of the chunks, and that
  first read comes from S3. Filling every home is
  [#38](https://github.com/aws-samples/sample-pacer/issues/38).
- **It does not check capacity.** A warm larger than the ring's cache evicts its own start.
  Set `--max-bytes` to what the ring holds.
