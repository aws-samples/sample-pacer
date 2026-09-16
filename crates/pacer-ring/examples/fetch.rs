//! Read a seeded keyset from an S3 endpoint and either TIME it (C1) or VERIFY
//! its bytes against a seed manifest (C2). A sibling of `seed.rs`/`classify_keys`
//! so a benchmark Job can bake and invoke it identically.
//!
//! ONE tool, two modes, so the cache path and the S3-Express bypass path are
//! measured by the SAME code and are directly comparable by construction:
//!
//!   * `--mode throughput` (workstream C1) — spin a bounded-concurrency GET loop
//!     over the keyset and report the **interior-window** throughput: bytes are
//!     accumulated continuously, sampled at `--ramp` and again at `--ramp +
//!     --window`, and the rate is `Δbytes / Δt` over that window. This mirrors
//!     the ladder's server-side interior-window method (`bench/ladder/run.sh`,
//!     two scrapes of `pacer_bytes_to_peers_total` at RAMP / RAMP+WINDOW,
//!     planning/16 §4.2) but client-side, so pointing this at the node-local
//!     daemon (cache/RDMA path) and at S3 Express directly (bypass, no daemon)
//!     yields two numbers whose ratio is the cache's benefit.
//!
//!   * `--mode verify` (workstream C2) — GET every key once, recompute the
//!     content digest the seeder recorded (`seed --checksum`, xxh3-128), and
//!     compare. A mismatch means the served bytes are NOT the source bytes — a
//!     corrupt cache/RDMA serve or a corrupt bypass read. Reusable beyond the
//!     ladder: F1's HF checkpoint arm points `--manifest` at its shard manifest
//!     (a corrupt shard is a broken model), no ladder coupling.
//!
//! AUTH picks how the two paths differ, nothing else:
//!   * `--auth static` (default) — ADR-0006 placeholder creds (`pacer`/`pacer`),
//!     `--endpoint host:port` REQUIRED, path-style, plain HTTP: the node-local
//!     **daemon** S3 proxy (bucket ALIAS, never the real `*--x-s3` name).
//!   * `--auth aws` — the default credential provider chain (EKS Pod Identity →
//!     S3 Express `CreateSession`), talking to **S3 Express directly** (the real
//!     directory bucket). `--endpoint` optional: omit to let the SDK resolve the
//!     zonal endpoint from the directory-bucket name + region.
//!
//! ```text
//! # cache path throughput (through the daemon):
//! fetch --mode throughput --auth static --endpoint 10.0.1.5:9000 \
//!       --bucket cache --list --prefix lad1/obj- --ramp 30 --window 90
//! # bypass throughput (direct to Express):
//! fetch --mode throughput --auth aws --bucket s3-...-x-s3 \
//!       --list --prefix lad1/obj- --ramp 30 --window 90
//! # integrity of served bytes vs the seed manifest:
//! fetch --mode verify --auth static --endpoint 10.0.1.5:9000 \
//!       --bucket cache --manifest /tmp/lad1.seed.json
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::Client;
use futures::stream::{self, StreamExt};

/// Default client-facing bucket alias (matches `seed.rs`/`BENCH_BUCKET`).
const DEFAULT_BUCKET: &str = "cache";
/// Default SigV4 signing region (matches the seed pod / `ckpt_seed.py`).
const DEFAULT_REGION: &str = "us-east-2";
/// ADR-0006 placeholder access key the daemon accepts (static-auth path).
const DEFAULT_ACCESS_KEY: &str = "pacer";
/// ADR-0006 placeholder secret key the daemon accepts (static-auth path).
const DEFAULT_SECRET_KEY: &str = "pacer";
/// Label attached to the static credentials provider (shows in SDK traces).
const CREDENTIALS_PROVIDER_NAME: &str = "pacer-fetch";
/// Scheme prepended to a bare `host:port` endpoint (plain HTTP daemon hop).
const ENDPOINT_SCHEME: &str = "http://";

/// Default GET fan-out. High enough to keep the link busy through the interior
/// window (a single serial GET chain under-utilises the NIC), bounded so peak
/// buffering stays ≈ `concurrency × object_size` — the same reasoning as
/// `seed.rs`'s `DEFAULT_CONCURRENCY`.
const DEFAULT_CONCURRENCY: usize = 64;
/// Default interior-window ramp, seconds — the ladder's fast-window `RAMP`
/// (`bench/ladder/run.sh`): excludes pod-scheduling / connection warm-up.
const DEFAULT_RAMP_SECS: u64 = 30;
/// Default interior-window width, seconds — the ladder's fast-window `WINDOW`.
const DEFAULT_WINDOW_SECS: u64 = 90;

/// Checksum algorithm this tool recomputes and the value `seed --checksum`
/// records. Kept identical to `seed.rs`'s `CHECKSUM_ALGO`; a manifest naming any
/// other algorithm is rejected rather than silently mis-verified.
const CHECKSUM_ALGO: &str = "xxh3-128";
/// Hex width of a 128-bit digest — must match `seed.rs`'s `CHECKSUM_HEX_WIDTH`
/// so a leading-zero digest string compares equal.
const CHECKSUM_HEX_WIDTH: usize = 32;

/// Bytes per GiB, for the human-readable rate.
const BYTES_PER_GIB: f64 = (1u64 << 30) as f64;
/// `list-objects-v2` page size when discovering the seeded keyset (`--list`).
const LIST_PAGE_SIZE: i32 = 1000;

/// What the tool does with the keyset.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Interior-window GET throughput (C1).
    Throughput,
    /// Byte-integrity of served bytes vs the manifest digests (C2).
    Verify,
}

/// How credentials + endpoint are resolved (daemon vs direct Express).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Auth {
    /// ADR-0006 placeholder creds against the node-local daemon proxy.
    Static,
    /// Default provider chain (Pod Identity) against S3 Express directly.
    Aws,
}

/// Parsed CLI options, all `--flag value` (matching `seed.rs`/`classify_keys`).
struct Opts {
    /// Throughput vs verify.
    mode: Mode,
    /// Credential/endpoint resolution mode.
    auth: Auth,
    /// Daemon endpoint `host:port` (required for `--auth static`; optional and,
    /// if set, forces this endpoint under `--auth aws`).
    endpoint: Option<String>,
    /// Bucket: the daemon ALIAS (static) or the real `*--x-s3` name (aws).
    bucket: String,
    /// SigV4 signing region.
    region: String,
    /// Static access key (static auth only).
    access_key: String,
    /// Static secret key (static auth only).
    secret_key: String,
    /// Key prefix for `--list`/generation (full prefix incl. any `obj-`).
    prefix: String,
    /// Generated key count when neither `--list` nor a key/manifest file is set.
    count: u64,
    /// Discover the keyset by `list-objects-v2` under `--prefix` (throughput).
    list: bool,
    /// Explicit key list, one per line (throughput).
    keys_file: Option<String>,
    /// Seed manifest JSON path — the key (and, for verify, digest) source.
    manifest: Option<String>,
    /// GET fan-out.
    concurrency: usize,
    /// Interior-window ramp (seconds).
    ramp: u64,
    /// Interior-window width (seconds).
    window: u64,
}

/// Parse a mode name.
///
/// # Errors
/// Returns a message for anything but `throughput`/`verify`.
fn parse_mode(s: &str) -> Result<Mode, String> {
    match s {
        "throughput" => Ok(Mode::Throughput),
        "verify" => Ok(Mode::Verify),
        other => Err(format!("bad --mode '{other}' (want throughput/verify)")),
    }
}

/// Parse an auth name.
///
/// # Errors
/// Returns a message for anything but `static`/`aws`.
fn parse_auth(s: &str) -> Result<Auth, String> {
    match s {
        "static" => Ok(Auth::Static),
        "aws" => Ok(Auth::Aws),
        other => Err(format!("bad --auth '{other}' (want static/aws)")),
    }
}

/// Parse a boolean flag value (same spellings as `seed.rs`).
///
/// # Errors
/// Returns a message if the value is not an accepted spelling.
fn parse_bool(s: &str) -> Result<bool, String> {
    match s.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(format!("bad boolean '{other}' (want true/false)")),
    }
}

/// Parse `--flag value` args into [`Opts`].
///
/// # Errors
/// Returns a message on an unknown flag, a missing value, an unparseable
/// number/bool, or a missing required `--mode`.
fn parse_opts() -> Result<Opts, String> {
    let mut mode = None;
    let mut auth = Auth::Static;
    let mut endpoint = None;
    let mut bucket = String::from(DEFAULT_BUCKET);
    let mut region = String::from(DEFAULT_REGION);
    let mut access_key = String::from(DEFAULT_ACCESS_KEY);
    let mut secret_key = String::from(DEFAULT_SECRET_KEY);
    let mut prefix = String::new();
    let mut count = 0;
    let mut list = false;
    let mut keys_file = None;
    let mut manifest = None;
    let mut concurrency = DEFAULT_CONCURRENCY;
    let mut ramp = DEFAULT_RAMP_SECS;
    let mut window = DEFAULT_WINDOW_SECS;

    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut value = || args.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--mode" => mode = Some(parse_mode(&value()?)?),
            "--auth" => auth = parse_auth(&value()?)?,
            "--endpoint" => endpoint = Some(value()?),
            "--bucket" => bucket = value()?,
            "--region" => region = value()?,
            "--access-key" => access_key = value()?,
            "--secret-key" => secret_key = value()?,
            "--prefix" => prefix = value()?,
            "--count" => count = value()?.parse().map_err(|e| format!("{e}"))?,
            "--list" => list = parse_bool(&value()?)?,
            "--keys-file" => keys_file = Some(value()?),
            "--manifest" => manifest = Some(value()?),
            "--concurrency" => concurrency = value()?.parse().map_err(|e| format!("{e}"))?,
            "--ramp" => ramp = value()?.parse().map_err(|e| format!("{e}"))?,
            "--window" => window = value()?.parse().map_err(|e| format!("{e}"))?,
            other => return Err(format!("unknown flag {other}")),
        }
    }
    let mode = mode.ok_or("--mode is required (throughput|verify)")?;
    if concurrency == 0 {
        return Err("--concurrency must be >= 1".into());
    }
    if auth == Auth::Static && endpoint.is_none() {
        return Err("--auth static requires --endpoint host:port (the daemon proxy)".into());
    }
    Ok(Opts {
        mode,
        auth,
        endpoint,
        bucket,
        region,
        access_key,
        secret_key,
        prefix,
        count,
        list,
        keys_file,
        manifest,
        concurrency,
        ramp,
        window,
    })
}

/// Normalize a bare `host:port` endpoint to a URL (prepend the plain-HTTP scheme
/// unless the caller already gave one), matching `seed.rs`.
fn endpoint_url(endpoint: &str) -> String {
    if endpoint.contains("://") {
        endpoint.to_owned()
    } else {
        format!("{ENDPOINT_SCHEME}{endpoint}")
    }
}

/// Build the S3 client for the chosen [`Auth`].
///
/// `static`: ADR-0006 creds, path-style, plain-HTTP daemon endpoint (identical
/// to `seed.rs`'s client). `aws`: the default provider chain (Pod Identity → S3
/// Express `CreateSession`), so a direct directory-bucket read authenticates;
/// an explicit `--endpoint` overrides the SDK's zonal resolution.
async fn build_client(opts: &Opts) -> Client {
    match opts.auth {
        Auth::Static => {
            let creds = Credentials::new(
                &opts.access_key,
                &opts.secret_key,
                None,
                None,
                CREDENTIALS_PROVIDER_NAME,
            );
            let conf = aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new(opts.region.clone()))
                .credentials_provider(creds)
                .endpoint_url(endpoint_url(opts.endpoint.as_deref().unwrap_or_default()))
                .force_path_style(true)
                .build();
            Client::from_conf(conf)
        }
        Auth::Aws => {
            let shared = aws_config::defaults(BehaviorVersion::latest())
                .region(Region::new(opts.region.clone()))
                .load()
                .await;
            let mut builder = aws_sdk_s3::config::Builder::from(&shared);
            if let Some(ep) = &opts.endpoint {
                builder = builder.endpoint_url(endpoint_url(ep));
            }
            Client::from_conf(builder.build())
        }
    }
}

/// Hex-encode a 128-bit digest at the fixed [`CHECKSUM_HEX_WIDTH`], so a
/// leading-zero digest is never truncated and compares equal to `seed.rs`'s.
fn hex128(digest: u128) -> String {
    format!("{digest:0width$x}", width = CHECKSUM_HEX_WIDTH)
}

/// GET `key` and digest it **as it arrives**, holding one response chunk at a time.
///
/// The whole point is the memory bound. Collecting the body first — which is what the
/// throughput path does, and what this function replaced — costs O(object) plus the
/// SDK's own buffering, so verifying ADR-0032's 6 GiB shard OOMKilled an 8Gi pod, and a
/// checkpoint-scale object could not be verified at any pod size. Streaming makes the
/// verifier's footprint independent of the object, which is what lets the same arm check
/// a 16 MiB rung key and a 131 GiB checkpoint.
///
/// The digest is bit-identical to the one-shot `xxh3_128` the seeder records:
/// `Xxh3::update` over the same bytes in the same order is the same hash. Asserted
/// rather than trusted — `streaming_digest_matches_the_one_shot_form` compares the two
/// across several chunk splits, since the one risk of streaming is a boundary changing
/// the result.
///
/// # Errors
/// Returns the SDK error text on a failed GET or a failed chunk read.
async fn streaming_digest(client: &Client, bucket: &str, key: &str) -> Result<String, String> {
    use xxhash_rust::xxh3::Xxh3;
    let mut resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| format!("GET {key}: {e}"))?;
    let mut hasher = Xxh3::new();
    while let Some(chunk) = resp
        .body
        .try_next()
        .await
        .map_err(|e| format!("GET-stream {key}: {e}"))?
    {
        hasher.update(&chunk);
    }
    Ok(hex128(hasher.digest128()))
}

/// List every object key under `prefix` via paginated `list-objects-v2` — the
/// warp `--list-existing` equivalent, so throughput hammers exactly the seeded
/// set regardless of how the keys were numbered.
///
/// # Errors
/// Returns the SDK error text on any page request failure.
async fn list_keys(client: &Client, bucket: &str, prefix: &str) -> Result<Vec<String>, String> {
    let mut keys = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let resp = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .max_keys(LIST_PAGE_SIZE)
            .set_continuation_token(token)
            .send()
            .await
            .map_err(|e| format!("LIST {prefix}: {e}"))?;
        keys.extend(
            resp.contents()
                .iter()
                .filter_map(|o| o.key().map(str::to_owned)),
        );
        if resp.is_truncated().unwrap_or(false) {
            token = resp.next_continuation_token().map(str::to_owned);
        } else {
            break;
        }
    }
    if keys.is_empty() {
        return Err(format!(
            "no objects under prefix '{prefix}' (was the keyset seeded?)"
        ));
    }
    Ok(keys)
}

/// Read `keys` (and, for verify, `digests`) from a seed manifest (`seed.rs`
/// stdout). Digests are `Some` only when the manifest carries the C2 block.
///
/// # Errors
/// Returns a message if the file is unreadable, not JSON, missing `keys`, or
/// names a checksum algorithm this tool does not implement.
fn keys_from_manifest(path: &str) -> Result<(Vec<String>, Option<Vec<String>>), String> {
    let body = std::fs::read_to_string(path).map_err(|e| format!("read {path}: {e}"))?;
    let json: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| format!("parse {path}: {e}"))?;
    let keys = json_string_array(&json, "keys")?
        .ok_or_else(|| format!("manifest {path} has no 'keys' array"))?;
    let digests = match json.get("checksum") {
        None => None,
        Some(algo) => {
            let algo = algo.as_str().unwrap_or_default();
            if algo != CHECKSUM_ALGO {
                return Err(format!("manifest checksum '{algo}' != '{CHECKSUM_ALGO}'"));
            }
            let d = json_string_array(&json, "digests")?
                .ok_or_else(|| format!("manifest {path} has 'checksum' but no 'digests'"))?;
            if d.len() != keys.len() {
                return Err(format!(
                    "manifest digests ({}) != keys ({})",
                    d.len(),
                    keys.len()
                ));
            }
            Some(d)
        }
    };
    Ok((keys, digests))
}

/// Extract a JSON string array field; `Ok(None)` if the field is absent.
///
/// # Errors
/// Returns a message if the field is present but not an array of strings.
fn json_string_array(json: &serde_json::Value, field: &str) -> Result<Option<Vec<String>>, String> {
    let Some(v) = json.get(field) else {
        return Ok(None);
    };
    let arr = v
        .as_array()
        .ok_or_else(|| format!("'{field}' is not an array"))?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        out.push(
            item.as_str()
                .ok_or_else(|| format!("'{field}' has a non-string"))?
                .to_owned(),
        );
    }
    Ok(Some(out))
}

/// Resolve the keyset (and optional digests) from whichever source is set:
/// `--manifest` > `--keys-file` > `--list` > generated `--prefix`+`--count`.
///
/// # Errors
/// Returns a message from the underlying source, or if no source yields keys.
async fn resolve_keys(
    opts: &Opts,
    client: &Client,
) -> Result<(Vec<String>, Option<Vec<String>>), String> {
    if let Some(path) = &opts.manifest {
        return keys_from_manifest(path);
    }
    if let Some(path) = &opts.keys_file {
        let body = std::fs::read_to_string(path).map_err(|e| format!("read {path}: {e}"))?;
        let keys: Vec<String> = body
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_owned)
            .collect();
        if keys.is_empty() {
            return Err(format!("--keys-file {path} contained no keys"));
        }
        return Ok((keys, None));
    }
    if opts.list {
        return Ok((list_keys(client, &opts.bucket, &opts.prefix).await?, None));
    }
    if opts.count == 0 {
        return Err(
            "no key source: pass --manifest, --keys-file, --list true, or --count N".into(),
        );
    }
    let keys = (0..opts.count)
        .map(|s| format!("{}{s:08}", opts.prefix))
        .collect();
    Ok((keys, None))
}

/// GET one object, drain the body, return its byte length. The daemon serves a
/// whole-object GET from the peer/cache path (or S3 read-through); direct Express
/// returns the object body. No retry — a transient blip is logged and skipped by
/// the throughput loop; verify treats any failure as a hard mismatch.
///
/// # Errors
/// Returns the SDK error text on a failed GET or body drain.
async fn get_bytes(client: &Client, bucket: &str, key: &str) -> Result<bytes::Bytes, String> {
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| format!("GET {key}: {e}"))?;
    let data = resp
        .body
        .collect()
        .await
        .map_err(|e| format!("GET-drain {key}: {e}"))?;
    Ok(data.into_bytes())
}

/// One throughput worker: GET keys round-robin, adding each object's byte count
/// to the shared total, until `stop` is set. A transient GET error is logged and
/// skipped so one blip never aborts the measurement.
async fn throughput_worker(
    client: Client,
    bucket: String,
    keys: Arc<Vec<String>>,
    idx: Arc<AtomicUsize>,
    total: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Relaxed) {
        let key = &keys[idx.fetch_add(1, Ordering::Relaxed) % keys.len()];
        match get_bytes(&client, &bucket, key).await {
            Ok(body) => {
                total.fetch_add(body.len() as u64, Ordering::Relaxed);
            }
            Err(e) => eprintln!("fetch: {e} (skipped)"),
        }
    }
}

/// Run the interior-window throughput measurement (C1): fan out
/// `concurrency` GET loops, sample the byte counter at `ramp` and `ramp+window`,
/// and report `Δbytes / Δt` — the same interior-window rate the cached ladder
/// arm reports, so the two are directly comparable.
///
/// # Errors
/// Returns a message if not a single byte moved in the window (dead endpoint /
/// empty keyset), so a zero rate fails loudly instead of printing `0.000`.
async fn run_throughput(
    opts: &Opts,
    client: Client,
    keys: Vec<String>,
) -> Result<serde_json::Value, String> {
    let n_keys = keys.len();
    let keys = Arc::new(keys);
    let total = Arc::new(AtomicU64::new(0));
    let idx = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    eprintln!(
        "throughput: {} key(s), c={}, ramp={}s window={}s, bucket {} (auth {})",
        n_keys,
        opts.concurrency,
        opts.ramp,
        opts.window,
        opts.bucket,
        if opts.auth == Auth::Aws {
            "aws"
        } else {
            "static"
        },
    );
    let mut handles = Vec::with_capacity(opts.concurrency);
    for _ in 0..opts.concurrency {
        handles.push(tokio::spawn(throughput_worker(
            client.clone(),
            opts.bucket.clone(),
            Arc::clone(&keys),
            Arc::clone(&idx),
            Arc::clone(&total),
            Arc::clone(&stop),
        )));
    }
    tokio::time::sleep(Duration::from_secs(opts.ramp)).await;
    let b0 = total.load(Ordering::Relaxed);
    let mark = Instant::now();
    tokio::time::sleep(Duration::from_secs(opts.window)).await;
    let window_bytes = total.load(Ordering::Relaxed) - b0;
    let elapsed = mark.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }
    if window_bytes == 0 {
        return Err("no bytes read in the interior window — dead endpoint or empty keyset".into());
    }
    let gibps = (window_bytes as f64 / BYTES_PER_GIB) / elapsed;
    Ok(serde_json::json!({
        "mode": "throughput",
        "auth": if opts.auth == Auth::Aws { "aws" } else { "static" },
        "bucket": opts.bucket,
        "keys": n_keys,
        "concurrency": opts.concurrency,
        "ramp_s": opts.ramp,
        "window_s": opts.window,
        "window_bytes": window_bytes,
        "elapsed_s": elapsed,
        "gibps": gibps,
    }))
}

/// Verify one key: GET it, recompute the digest **as it streams**, compare to
/// `expected`. See [`streaming_digest`] for why it does not collect the body first.
///
/// # Errors
/// Returns a message on a fetch failure OR a digest mismatch — both are verify
/// failures (the served bytes are unavailable or not the source bytes).
async fn verify_one(
    client: &Client,
    bucket: &str,
    key: &str,
    expected: &str,
) -> Result<(), String> {
    let got = streaming_digest(client, bucket, key).await?;
    if got == expected {
        Ok(())
    } else {
        Err(format!(
            "{key}: digest mismatch (expected {expected}, got {got})"
        ))
    }
}

/// Run byte-integrity verification (C2): recompute every key's digest over the
/// bytes actually served and compare to the manifest. Returns a JSON summary;
/// the caller exits non-zero when `failures` is non-empty.
///
/// # Errors
/// Returns a message if the manifest carried no digests (nothing to verify).
async fn run_verify(
    opts: &Opts,
    client: Client,
    keys: Vec<String>,
    digests: Option<Vec<String>>,
) -> Result<serde_json::Value, String> {
    let digests = digests.ok_or(
        "verify needs a manifest with digests — re-seed with `seed --checksum true` and pass --manifest",
    )?;
    eprintln!(
        "verify: {} key(s), bucket {} (algo {CHECKSUM_ALGO})",
        keys.len(),
        opts.bucket
    );
    let failures: Vec<String> = stream::iter(keys.iter().zip(digests.iter()))
        .map(|(key, expected)| {
            let client = client.clone();
            let bucket = opts.bucket.clone();
            async move { verify_one(&client, &bucket, key, expected).await }
        })
        .buffer_unordered(opts.concurrency)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .filter_map(Result::err)
        .collect();
    Ok(serde_json::json!({
        "mode": "verify",
        "auth": if opts.auth == Auth::Aws { "aws" } else { "static" },
        "bucket": opts.bucket,
        "checksum": CHECKSUM_ALGO,
        "checked": keys.len(),
        "ok": keys.len() - failures.len(),
        "failures": failures,
    }))
}

/// Build the client, resolve the keyset, and dispatch the mode. Returns
/// `(result_json, ok)`; `ok` is false when verify found a mismatch so `main`
/// can set the exit code.
///
/// # Errors
/// Propagates client/key-resolution/mode-run errors.
async fn run(opts: &Opts) -> Result<(serde_json::Value, bool), String> {
    let client = build_client(opts).await;
    let (keys, digests) = resolve_keys(opts, &client).await?;
    match opts.mode {
        Mode::Throughput => Ok((run_throughput(opts, client, keys).await?, true)),
        Mode::Verify => {
            let out = run_verify(opts, client, keys, digests).await?;
            let ok = out["failures"].as_array().is_none_or(|f| f.is_empty());
            Ok((out, ok))
        }
    }
}

#[tokio::main]
async fn main() {
    let opts = match parse_opts() {
        Ok(o) => o,
        Err(e) => {
            eprintln!("fetch: {e}");
            std::process::exit(2);
        }
    };
    match run(&opts).await {
        Ok((result, ok)) => {
            println!("{result}");
            if !ok {
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("fetch: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The one-shot digest `seed.rs` records, and the reference every streaming digest
    /// below is compared against. Test-only: production hashes as it streams, and a
    /// one-shot form in the binary would be an O(object) trap waiting to be called.
    fn digest_hex(body: &[u8]) -> String {
        hex128(xxhash_rust::xxh3::xxh3_128(body))
    }

    /// The streaming verifier must produce the SAME digest as the one-shot form, or it
    /// is not a verifier — it is a second, incompatible checksum that would fail every
    /// key a seeder hashed in one pass. Asserted over several chunk splits, including
    /// ones that land mid-word, since the whole risk of streaming is that a boundary
    /// changes the result.
    #[test]
    fn streaming_digest_matches_the_one_shot_form() {
        use xxhash_rust::xxh3::Xxh3;
        let body: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        let one_shot = digest_hex(&body);
        for chunk in [1usize, 7, 8, 999, 4096, 10_000] {
            let mut hasher = Xxh3::new();
            for part in body.chunks(chunk) {
                hasher.update(part);
            }
            assert_eq!(
                hex128(hasher.digest128()),
                one_shot,
                "chunked by {chunk} must hash the same"
            );
        }
    }

    #[test]
    fn mode_and_auth_parse() {
        assert!(matches!(parse_mode("throughput"), Ok(Mode::Throughput)));
        assert!(matches!(parse_mode("verify"), Ok(Mode::Verify)));
        assert!(parse_mode("nope").is_err());
        assert!(matches!(parse_auth("static"), Ok(Auth::Static)));
        assert!(matches!(parse_auth("aws"), Ok(Auth::Aws)));
        assert!(parse_auth("iam").is_err());
    }

    #[test]
    fn digest_matches_seed_formula() {
        // Identical algorithm + width to seed.rs's content_digest, so a manifest
        // digest and a re-fetched digest of the same bytes compare equal.
        let d = digest_hex(&[0u8; 4096]);
        assert_eq!(d.len(), CHECKSUM_HEX_WIDTH);
        assert!(d.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(d, digest_hex(&[0u8; 4096]));
        let mut other = vec![0u8; 4096];
        other[7] = 9;
        assert_ne!(d, digest_hex(&other));
    }

    #[test]
    fn endpoint_url_prepends_scheme_once() {
        assert_eq!(endpoint_url("10.0.1.5:9000"), "http://10.0.1.5:9000");
        assert_eq!(endpoint_url("https://x:1"), "https://x:1");
    }

    #[test]
    fn manifest_with_digests_round_trips() {
        let json = serde_json::json!({
            "keys": ["lad1/obj-00000000", "lad1/obj-00000001"],
            "checksum": CHECKSUM_ALGO,
            "digests": ["aa", "bb"],
        })
        .to_string();
        // Test-only fixture: writes, reads, and deletes its own PID-suffixed
        // scratch file in-process, never a predictable-tempfile attack surface.
        // The marker must stay DIRECTLY above the code and carry the full rule
        // id — semgrep ignores it otherwise.
        // nosemgrep: rust.lang.security.temp-dir.temp-dir
        let dir = std::env::temp_dir();
        let path = dir.join(format!("fetch-manifest-{}.json", std::process::id()));
        std::fs::write(&path, json).unwrap();
        let (keys, digests) = keys_from_manifest(path.to_str().unwrap()).unwrap();
        std::fs::remove_file(&path).ok();
        assert_eq!(keys.len(), 2);
        assert_eq!(digests.unwrap(), vec!["aa".to_string(), "bb".to_string()]);
    }

    #[test]
    fn manifest_without_checksum_has_no_digests() {
        let json = serde_json::json!({ "keys": ["k0"] }).to_string();
        // Test-only fixture, see manifest_with_digests_round_trips above.
        // nosemgrep: rust.lang.security.temp-dir.temp-dir
        let dir = std::env::temp_dir();
        let path = dir.join(format!("fetch-nomanifest-{}.json", std::process::id()));
        std::fs::write(&path, json).unwrap();
        let (keys, digests) = keys_from_manifest(path.to_str().unwrap()).unwrap();
        std::fs::remove_file(&path).ok();
        assert_eq!(keys, vec!["k0".to_string()]);
        assert!(digests.is_none());
    }

    #[test]
    fn manifest_wrong_algo_is_rejected() {
        let json = serde_json::json!({
            "keys": ["k0"], "checksum": "crc32", "digests": ["00"],
        })
        .to_string();
        // Test-only fixture, see manifest_with_digests_round_trips above.
        // nosemgrep: rust.lang.security.temp-dir.temp-dir
        let dir = std::env::temp_dir();
        let path = dir.join(format!("fetch-badalgo-{}.json", std::process::id()));
        std::fs::write(&path, json).unwrap();
        let res = keys_from_manifest(path.to_str().unwrap());
        std::fs::remove_file(&path).ok();
        assert!(res.is_err());
    }
}
