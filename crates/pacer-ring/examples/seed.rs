//! Fast parallel object seeder for the PACER benchmark harness (planning/15).
//!
//! Replaces the *sequential* PUT+GET seed loops the harness runs today — the
//! ladder's `bench/ladder/run.sh` `cmd_seed`/`seed2`/`seed3` (an awscli pod that
//! PUTs then GETs one object at a time, budgeted `NOBJ*3+120` s ≈ 25 min for 512
//! objects) and `bench/b4/restore/ckpt_seed.py` (boto3 uploading shards one after
//! another) — with a single Rust binary that drives the same PUT+warm-GET
//! semantics under **bounded parallelism**.
//!
//! Semantics preserved from those seeders (do NOT change without an ADR):
//!   * All S3 I/O goes through a **node-local daemon** S3 endpoint (`host:port`,
//!     plain HTTP), **path-style** addressing, ADR-0006 placeholder creds
//!     (`pacer`/`pacer`), against a client-facing bucket **alias** — never the real
//!     `*--x-s3` directory-bucket name (CLAUDE.md bucket-aliasing rule).
//!   * Each object is PUT (fills the same-AZ Express backend via the daemon) and
//!     then, when warming is enabled, whole-object GET back so the daemon
//!     read-through-fills every chunk into its ring home(s) (ADR-0016) — exactly
//!     the warm the existing seeders perform. Warming is optional (`--warm`):
//!     some callers only need bytes in the backend, not a warm ring.
//!   * Object payloads are generated **in memory** (default: deterministic zero
//!     filler — the ladder measures bytes moved, not content; mirrors the awscli
//!     pod's `head -c N /dev/zero`), so seeding never reads from disk. `--fill
//!     keyed` opts into a position- and key-dependent payload instead, which is
//!     what makes a digest comparison able to detect *displaced* bytes rather than
//!     only corrupted ones — see [`Fill`], and ADR-0032's gate 3.1, where the
//!     object is assembled from parts several nodes uploaded. The default is
//!     untouched, so every existing arm seeds exactly the bytes it always did.
//!   * Each key's PUT+GET is retried with backoff (S3 Express One Zone transiently
//!     503s — SlowDown — under a concurrent burst; see the seed-pod manifest and
//!     commit 808a49a), so one throttle costs seconds, not the whole keyset.
//!   * The set of keys seeded (+ counts + bytes) is emitted as the **final stdout
//!     line** in JSON, like `ckpt_seed.py`, so the harness can record what was
//!     seeded; all progress goes to stderr.
//!
//! What changes: the sequential loop becomes a `buffer_unordered` fan-out at a
//! caller-chosen concurrency (default [`DEFAULT_CONCURRENCY`]). Key SOURCE is
//! either generated (`{prefix}{seq:08}`, matching `classify_keys`/`seed3`) or an
//! explicit `--keys-file` (one key per line) so classified rung-1/rung-2 keysets
//! seed through this same binary.
//!
//! Invocation (a sibling of `classify_keys`, so a B2 Job can call it identically):
//!
//! ```text
//! cargo run -p pacer-ring --example seed -- \
//!     --endpoint 10.0.1.5:9000 --bucket cache --prefix lad3/obj- \
//!     --count 512 --object-size 16MiB --concurrency 32 --warm true
//! ```

use std::time::Duration;

use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bytes::Bytes;
use futures::stream::{self, StreamExt};
use xxhash_rust::xxh3::xxh3_128;

/// Default fan-out. Bounded on purpose: high enough to hide per-object PUT+GET
/// round-trip latency and saturate the same-AZ Express backend (the sequential
/// awscli loop left the link ~idle between objects), yet low enough to limit the
/// concurrent-burst pressure that makes S3 Express One Zone return 503 SlowDown
/// (planning/15 B4, commit 808a49a). Peak seeder memory during a warm pass is
/// ≈ `concurrency × object_size`, so this also caps warm-GET buffering. Override
/// with `--concurrency`.
const DEFAULT_CONCURRENCY: usize = 32;

/// Default object count when generating keys — the ladder's `LADDER_NOBJ` default
/// (512 × 16 MiB = 8 GiB keyset). Ignored when `--keys-file` supplies the keys.
const DEFAULT_COUNT: u64 = 512;

/// Default object size, the ladder's `OBJECT_SIZE`/chunk default (ADR-0015 cache
/// unit). Bytes moved is what the benchmark measures, so any size is valid.
const DEFAULT_OBJECT_SIZE_BYTES: u64 = 16 << 20;

/// Default key prefix. The caller supplies the FULL prefix including any `obj-`
/// component (the `classify_keys` convention); the tool appends a zero-padded
/// sequence number, yielding e.g. `seed/obj-00000000`.
const DEFAULT_PREFIX: &str = "seed/obj-";

/// Default client-facing bucket alias — the daemon's `BENCH_BUCKET` default. This
/// is an ALIAS the daemon maps to the real directory bucket; clients must never
/// see the `*--x-s3` name (CLAUDE.md bucket-aliasing rule).
const DEFAULT_BUCKET: &str = "cache";

/// Default signing region. Irrelevant to the daemon (it proxies), but SigV4 needs
/// one; matches the seed-pod manifest / `ckpt_seed.py` (`us-east-2`).
const DEFAULT_REGION: &str = "us-east-2";

/// ADR-0006 placeholder access key the daemon accepts (public dummy identity, not
/// a secret) — same value the awscli seed pod exports.
const DEFAULT_ACCESS_KEY: &str = "pacer";

/// ADR-0006 placeholder secret key the daemon accepts (see [`DEFAULT_ACCESS_KEY`]).
const DEFAULT_SECRET_KEY: &str = "pacer";

/// Checksum algorithm recorded in the manifest when `--checksum` is enabled
/// (workstream C2). xxh3-128 is ALREADY a `pacer-ring` dependency (it backs the
/// ring's ownership hash), so recording it pulls in no new crate; it is a fast,
/// strong 128-bit non-cryptographic digest — ample to catch a corrupted served
/// byte (the C2 goal is integrity, not tamper-resistance) while hashing at close
/// to memory bandwidth on the seed path. The `fetch --mode verify` reader
/// recomputes THIS algorithm over the bytes it fetches and compares.
const CHECKSUM_ALGO: &str = "xxh3-128";

/// Hex width of a 128-bit digest (`{:032x}` — 32 nibbles). A shorter width would
/// silently truncate the digest and defeat the integrity check.
const CHECKSUM_HEX_WIDTH: usize = 32;

/// Default for `--checksum`: OFF, so a plain seed's manifest stays byte-for-byte
/// identical to B1's — the checksum block is purely additive (C2 requirement).
const DEFAULT_CHECKSUM: bool = false;

/// Bytes per `u64` word of a [`Fill::Keyed`] payload — the granularity at which
/// that fill varies with position, and therefore the smallest displacement of
/// bytes it can detect.
const KEYED_FILL_WORD_BYTES: u64 = 8;

/// Zero-pad width of the generated sequence number. Matches `classify_keys`'s
/// `{seq:08}` and `seed3`'s `%08d` so generated keys are byte-identical to what
/// the existing harness produces (a diverging width would miss the seeded set).
const KEY_SEQ_WIDTH: usize = 8;

/// Max PUT+GET attempts per key before the whole seed fails. Mirrors the seed-pod
/// manifest's `attempts=6`: a transient Express SlowDown clears in a few seconds,
/// but a sustained one must fail loudly, not hang or silently short the keyset.
const PER_KEY_MAX_ATTEMPTS: u32 = 6;

/// Max SDK-internal attempts per request (adaptive retry). Mirrors the seed-pod's
/// `AWS_MAX_ATTEMPTS=10` — the SDK rate-limits itself under throttling before the
/// coarser per-key backoff ([`PER_KEY_MAX_ATTEMPTS`]) ever engages.
const SDK_MAX_ATTEMPTS: u32 = 10;

/// Per-key backoff unit: attempt `i` sleeps `i × BACKOFF_UNIT` (linear, like the
/// seed-pod's `sleep "$i"`), so a burst of throttles spreads out instead of
/// hot-looping the backend.
const BACKOFF_UNIT: Duration = Duration::from_secs(1);

/// Label attached to the static credentials provider (shows up in SDK traces).
const CREDENTIALS_PROVIDER_NAME: &str = "pacer-seeder";

/// Scheme for the node-local daemon endpoint: the daemon's S3 proxy listens on
/// plain HTTP on the node (no TLS on the pod-local hop), same as the awscli
/// `--endpoint-url http://...`. Prepended unless the caller already gave a scheme.
const ENDPOINT_SCHEME: &str = "http://";

/// Bytes per binary size unit, so `--object-size 16MiB` parses like the ladder's
/// `size_to_bytes` (bare bytes or `KiB`/`MiB`/`GiB`/`TiB`).
const BYTES_PER_KIB: u64 = 1 << 10;
/// See [`BYTES_PER_KIB`].
const BYTES_PER_MIB: u64 = 1 << 20;
/// See [`BYTES_PER_KIB`].
const BYTES_PER_GIB: u64 = 1 << 30;
/// See [`BYTES_PER_KIB`].
const BYTES_PER_TIB: u64 = 1 << 40;

/// What the payload bytes are — and therefore what a digest comparison can prove.
///
/// This is a correctness knob disguised as a payload knob, so it is worth stating
/// plainly. Under [`Fill::Zero`] every object of a run is byte-identical AND every
/// window inside an object is byte-identical, so a recomputed digest proves only
/// that the right NUMBER of bytes came back: an assembly that ordered its parts
/// wrongly, or a cache that staged chunk *i*'s bytes under chunk *j*'s key, or a
/// serve that returned another object's chunk entirely, all pass. For the ladder's
/// read path that is fine — its chunks are placed by a fill it did not write, and the
/// bytes' provenance is not what those rungs are testing.
///
/// It is NOT fine for ADR-0032's write scatter (gate 3.1): there the object is
/// assembled from parts uploaded by different nodes and cached by their homes, so
/// mis-ordering and mis-keying are the specific new failure modes, and a zero fill
/// makes them invisible. [`Fill::Keyed`] varies every
/// [`KEYED_FILL_WORD_BYTES`]-byte word with both its offset and the key, so any
/// displacement of bytes within an object, or between two objects, changes the
/// digest.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Fill {
    /// Deterministic zeros from ONE shared buffer, cloned per PUT (`O(1)`, one
    /// digest for the whole keyset). The historical behaviour and the default.
    Zero,
    /// Position- and key-dependent words, generated PER KEY. Costs
    /// `concurrency × object_size` of live buffers and one digest per key.
    Keyed,
}

/// Parse `--fill`.
///
/// # Errors
/// Returns a message for anything but `zero`/`keyed`.
fn parse_fill(s: &str) -> Result<Fill, String> {
    match s.to_ascii_lowercase().as_str() {
        "zero" => Ok(Fill::Zero),
        "keyed" => Ok(Fill::Keyed),
        other => Err(format!("bad --fill '{other}' (want zero/keyed)")),
    }
}

/// Build one key's payload under `fill`.
///
/// [`Fill::Keyed`] writes `offset_word ^ key_hash` into every
/// [`KEYED_FILL_WORD_BYTES`]-byte word, little-endian. Both terms matter: the offset
/// makes a within-object displacement visible, the key hash makes a between-object
/// one visible. A trailing partial word is filled from the same word's bytes, so any
/// object size works.
fn payload(key: &str, size: u64, fill: Fill) -> Bytes {
    let size = usize::try_from(size).unwrap_or(usize::MAX);
    match fill {
        Fill::Zero => Bytes::from(vec![0u8; size]),
        Fill::Keyed => {
            let key_hash = xxh3_128(key.as_bytes()) as u64;
            let mut buf = Vec::with_capacity(size);
            let mut word_index = 0u64;
            while buf.len() < size {
                let word = word_index
                    .wrapping_mul(KEYED_FILL_WORD_BYTES)
                    .wrapping_add(key_hash);
                let take = (size - buf.len()).min(KEYED_FILL_WORD_BYTES as usize);
                buf.extend_from_slice(&word.to_le_bytes()[..take]);
                word_index += 1;
            }
            Bytes::from(buf)
        }
    }
}

/// Parsed CLI options, all `--flag value` (matching `classify_keys`).
struct Opts {
    /// Node-local daemon S3 endpoint, `host:port` (scheme optional).
    endpoint: String,
    /// Client-facing bucket alias (never the real `*--x-s3` name).
    bucket: String,
    /// Full key prefix; the tool appends a [`KEY_SEQ_WIDTH`]-digit sequence.
    prefix: String,
    /// Objects to seed when generating keys (ignored if `keys_file` is set).
    count: u64,
    /// Object size in bytes (payload is that many deterministic zero bytes).
    object_size: u64,
    /// The `--object-size` string as given, echoed into the manifest.
    object_size_str: String,
    /// Bounded fan-out (concurrent in-flight PUT+GET pipelines).
    concurrency: usize,
    /// Whether to whole-object GET each key back to warm the ring (ADR-0016).
    warm: bool,
    /// Whether to record a per-object content digest in the manifest (C2). Off
    /// by default so a plain seed's manifest is unchanged.
    checksum: bool,
    /// What the payload bytes are, and hence what `--checksum` can prove. See
    /// [`Fill`]; `zero` is the default and the historical behaviour.
    fill: Fill,
    /// Optional explicit key list (one per line); overrides generation.
    keys_file: Option<String>,
    /// SigV4 signing region (proxied away by the daemon; any value works).
    region: String,
    /// ADR-0006 placeholder access key the daemon accepts.
    access_key: String,
    /// ADR-0006 placeholder secret key the daemon accepts.
    secret_key: String,
}

/// Parse a size string: bare bytes, or `KiB`/`MiB`/`GiB`/`TiB` (binary),
/// identical to the ladder's `size_to_bytes`.
///
/// # Errors
/// Returns a message if the number is non-numeric or the unit is unknown.
fn parse_size(s: &str) -> Result<u64, String> {
    let units = [
        ("KiB", BYTES_PER_KIB),
        ("MiB", BYTES_PER_MIB),
        ("GiB", BYTES_PER_GIB),
        ("TiB", BYTES_PER_TIB),
    ];
    for (suffix, mult) in units {
        if let Some(num) = s.strip_suffix(suffix) {
            let n: u64 = num.parse().map_err(|e| format!("bad size '{s}': {e}"))?;
            return Ok(n * mult);
        }
    }
    s.parse().map_err(|e| format!("bad size '{s}': {e}"))
}

/// Parse a boolean flag value (`true`/`false`/`1`/`0`/`yes`/`no`).
///
/// # Errors
/// Returns a message if the value is not one of the accepted spellings.
fn parse_bool(s: &str) -> Result<bool, String> {
    match s.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(format!("bad boolean '{other}' (want true/false)")),
    }
}

/// Parse `--flag value` args into [`Opts`], applying the documented defaults.
///
/// # Errors
/// Returns a message on an unknown flag, a missing value, an unparseable
/// number/size/bool, or a missing required `--endpoint`.
fn parse_opts() -> Result<Opts, String> {
    let mut endpoint = None;
    let mut bucket = String::from(DEFAULT_BUCKET);
    let mut prefix = String::from(DEFAULT_PREFIX);
    let mut count = DEFAULT_COUNT;
    let mut object_size_str = String::new();
    let mut concurrency = DEFAULT_CONCURRENCY;
    let mut warm = true;
    let mut checksum = DEFAULT_CHECKSUM;
    let mut fill = Fill::Zero;
    let mut keys_file = None;
    let mut region = String::from(DEFAULT_REGION);
    let mut access_key = String::from(DEFAULT_ACCESS_KEY);
    let mut secret_key = String::from(DEFAULT_SECRET_KEY);

    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut value = || args.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--endpoint" => endpoint = Some(value()?),
            "--bucket" => bucket = value()?,
            "--prefix" => prefix = value()?,
            "--count" => count = value()?.parse().map_err(|e| format!("{e}"))?,
            "--object-size" => object_size_str = value()?,
            "--concurrency" => concurrency = value()?.parse().map_err(|e| format!("{e}"))?,
            "--warm" => warm = parse_bool(&value()?)?,
            "--checksum" => checksum = parse_bool(&value()?)?,
            "--fill" => fill = parse_fill(&value()?)?,
            "--keys-file" => keys_file = Some(value()?),
            "--region" => region = value()?,
            "--access-key" => access_key = value()?,
            "--secret-key" => secret_key = value()?,
            other => return Err(format!("unknown flag {other}")),
        }
    }

    if object_size_str.is_empty() {
        object_size_str = DEFAULT_OBJECT_SIZE_BYTES.to_string();
    }
    let object_size = parse_size(&object_size_str)?;
    if concurrency == 0 {
        return Err("--concurrency must be >= 1".into());
    }
    Ok(Opts {
        endpoint: endpoint.ok_or("--endpoint is required (daemon S3 host:port)")?,
        bucket,
        prefix,
        count,
        object_size,
        object_size_str,
        concurrency,
        warm,
        checksum,
        fill,
        keys_file,
        region,
        access_key,
        secret_key,
    })
}

/// Generate `count` sequential keys `{prefix}{seq:0KEY_SEQ_WIDTH}` — byte-identical
/// to `classify_keys`/`seed3` so a generated keyset matches the harness's.
fn generate_keys(prefix: &str, count: u64) -> Vec<String> {
    (0..count)
        .map(|seq| format!("{prefix}{seq:0KEY_SEQ_WIDTH$}"))
        .collect()
}

/// Resolve the keyset: the explicit `--keys-file` (one key per line, blanks
/// skipped) when given, else [`generate_keys`].
///
/// # Errors
/// Returns a message if `--keys-file` is set but cannot be read, or is empty.
fn resolve_keys(opts: &Opts) -> Result<Vec<String>, String> {
    let Some(path) = &opts.keys_file else {
        return Ok(generate_keys(&opts.prefix, opts.count));
    };
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
    Ok(keys)
}

/// Build the S3 client aimed at the node-local daemon: static ADR-0006 creds,
/// path-style, plain-HTTP endpoint, adaptive retry ([`SDK_MAX_ATTEMPTS`]).
fn build_client(opts: &Opts) -> Client {
    let creds = Credentials::new(
        &opts.access_key,
        &opts.secret_key,
        None,
        None,
        CREDENTIALS_PROVIDER_NAME,
    );
    let endpoint = if opts.endpoint.contains("://") {
        opts.endpoint.clone()
    } else {
        format!("{ENDPOINT_SCHEME}{}", opts.endpoint)
    };
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(opts.region.clone()))
        .credentials_provider(creds)
        .endpoint_url(endpoint)
        .force_path_style(true)
        .retry_config(RetryConfig::adaptive().with_max_attempts(SDK_MAX_ATTEMPTS))
        .build();
    Client::from_conf(conf)
}

/// Render an SDK error with the S3 error code and message, not just its `Display`.
///
/// `SdkError`'s own `Display` for a failed request is the bare string `"service
/// error"` — no code, no message, no request id. That cost a whole hardware arm on
/// 2026-08-25: every scatter-off PUT failed and the only text the harness could
/// report was `service error`, so the S3 code that would have named the cause was
/// never seen (`bench/ladder/results/w1-write-scatter.md`). The code and message are
/// present in the error's metadata all along — this prints them.
///
/// `DisplayErrorContext` is appended because a *transport* failure (connect reset,
/// timeout) carries no metadata at all, and for those the source chain is the only
/// description there is.
fn sdk_err(e: &impl ProvideErrorMetadata) -> String {
    let code = e.code().unwrap_or("<no-code>");
    let message = e.message().unwrap_or("<no-message>");
    format!("{code}: {message}")
}

/// PUT one object, then (if `warm`) whole-object GET it back so the daemon
/// read-through-fills every chunk into its ring home(s). No retry — that is
/// [`seed_one_with_retry`]'s job. `body` is a cheap refcounted clone of the
/// shared filler buffer.
///
/// # Errors
/// Returns the S3 error code and message on a failed PUT or GET (see [`sdk_err`]),
/// or the SDK's own text on a failed body drain.
async fn seed_one(
    client: &Client,
    bucket: &str,
    key: &str,
    body: Bytes,
    warm: bool,
) -> Result<(), String> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body))
        .send()
        .await
        .map_err(|e| format!("PUT {key}: {} [{}]", sdk_err(&e), DisplayErrorContext(&e)))?;
    if warm {
        let resp = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| format!("GET {key}: {} [{}]", sdk_err(&e), DisplayErrorContext(&e)))?;
        // Drain the whole body: that read-through is what fills the chunk homes.
        resp.body
            .collect()
            .await
            .map_err(|e| format!("GET-drain {key}: {e}"))?;
    }
    Ok(())
}

/// [`seed_one`] wrapped in a bounded linear backoff (mirrors the seed-pod's
/// `until put_get; do sleep i` loop) to ride out transient Express SlowDown.
///
/// # Errors
/// Returns the last error text after [`PER_KEY_MAX_ATTEMPTS`] failed attempts.
async fn seed_one_with_retry(
    client: &Client,
    bucket: &str,
    key: &str,
    body: Bytes,
    warm: bool,
) -> Result<(), String> {
    let mut last = String::new();
    for attempt in 1..=PER_KEY_MAX_ATTEMPTS {
        match seed_one(client, bucket, key, body.clone(), warm).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                last = e;
                if attempt < PER_KEY_MAX_ATTEMPTS {
                    eprintln!(
                        "seed: key {key} attempt {attempt} failed ({last}); backing off {attempt}s"
                    );
                    tokio::time::sleep(BACKOFF_UNIT * attempt).await;
                }
            }
        }
    }
    Err(format!(
        "key {key} FAILED after {PER_KEY_MAX_ATTEMPTS} attempts: {last}"
    ))
}

/// Hex-encode the xxh3-128 content digest of `body` (C2). Fixed
/// [`CHECKSUM_HEX_WIDTH`]-nibble output so a leading-zero digest is never
/// truncated — the verifier compares the string verbatim.
fn content_digest(body: &[u8]) -> String {
    format!("{:0width$x}", xxh3_128(body), width = CHECKSUM_HEX_WIDTH)
}

/// The seeded-key manifest, printed as the final stdout line (JSON).
///
/// When `digest` is `Some` (`--checksum` on), an additive `checksum` (the
/// [`CHECKSUM_ALGO`] name) + `digests` (one hex digest per key, index-aligned
/// with `keys`) block is appended — the C2 integrity contract the
/// `fetch --mode verify` reader consumes. When `None` (the default) the object
/// is byte-for-byte the manifest B1 emits, so a plain seed is unchanged.
fn build_manifest(
    opts: &Opts,
    keys: &[String],
    total_bytes: u64,
    digests: Option<&[String]>,
    elapsed: Duration,
) -> serde_json::Value {
    let secs = elapsed.as_secs_f64();
    let mut manifest = serde_json::json!({
        "endpoint": opts.endpoint,
        "bucket": opts.bucket,
        "prefix": opts.prefix,
        "object_size": opts.object_size_str,
        "object_size_bytes": opts.object_size,
        "concurrency": opts.concurrency,
        "warm": opts.warm,
        "fill": format!("{:?}", opts.fill).to_ascii_lowercase(),
        "count": keys.len(),
        "bytes": total_bytes,
        // Wall clock of the whole fan-out, and the rate it implies. This is the
        // WRITE rate only when `warm` is false: with warming on, each key's future
        // is a PUT *and* a whole-object GET, so the seconds cover both and the
        // number is a seed rate, not an upload rate. ADR-0032's write arms seed
        // with `--warm false` for exactly this reason.
        "elapsed_seconds": (secs * 1000.0).round() / 1000.0,
        "gibps": if secs > 0.0 {
            (total_bytes as f64 / BYTES_PER_GIB as f64 / secs * 1000.0).round() / 1000.0
        } else {
            0.0
        },
        "keys": keys,
    });
    if let Some(d) = digests {
        // One digest per key, index-aligned with `keys` — the shape
        // `fetch --mode verify` consumes. Under Fill::Zero every entry is the same
        // string (one shared buffer, hashed once); under Fill::Keyed they differ,
        // which is the whole point of that fill.
        manifest["checksum"] = serde_json::json!(CHECKSUM_ALGO);
        manifest["digests"] = serde_json::json!(d);
    }
    manifest
}

/// Seed every key with bounded fan-out; return the manifest on full success.
///
/// # Errors
/// Returns a joined message listing every key that exhausted its retries.
async fn run(opts: &Opts) -> Result<serde_json::Value, String> {
    let keys = resolve_keys(opts)?;
    let client = build_client(opts);
    // Under Fill::Zero one shared buffer serves every PUT (each clone is O(1)) and
    // one hash covers the whole keyset. Under Fill::Keyed the payload depends on the
    // key, so it is built inside each future instead — bounding live buffers to
    // `concurrency × object_size` rather than to the keyset.
    let shared = (opts.fill == Fill::Zero).then(|| payload("", opts.object_size, Fill::Zero));
    let shared_digest = opts
        .checksum
        .then(|| shared.as_ref().map(|b| content_digest(b)))
        .flatten();
    eprintln!(
        "seeding {} x {} object(s) -> {} (bucket {}, warm={}, fill={:?}, concurrency {})",
        keys.len(),
        opts.object_size_str,
        opts.endpoint,
        opts.bucket,
        opts.warm,
        opts.fill,
        opts.concurrency,
    );
    if shared.is_none() {
        // Worth saying out loud: this fill trades memory for detectability, and the
        // peak is the product of two knobs a caller sets independently.
        eprintln!(
            "seed: fill=keyed generates a payload PER KEY — peak live buffers ≈ {:.2} GiB \
             (concurrency {} × {})",
            opts.concurrency as f64 * opts.object_size as f64 / BYTES_PER_GIB as f64,
            opts.concurrency,
            opts.object_size_str,
        );
    }

    let started = std::time::Instant::now();
    let results: Vec<Result<(String, Option<String>), String>> = stream::iter(keys.clone())
        .map(|key| {
            let client = client.clone();
            let bucket = opts.bucket.clone();
            let shared = shared.clone();
            let (warm, checksum, fill, size) =
                (opts.warm, opts.checksum, opts.fill, opts.object_size);
            async move {
                let body = shared.unwrap_or_else(|| payload(&key, size, fill));
                let digest = checksum.then(|| content_digest(&body));
                seed_one_with_retry(&client, &bucket, &key, body, warm).await?;
                Ok((key, digest))
            }
        })
        .buffer_unordered(opts.concurrency)
        .collect()
        .await;
    let elapsed = started.elapsed();

    let digests = collect_digests(&keys, results, shared_digest.as_deref())?;
    let total_bytes = opts.object_size * keys.len() as u64;
    eprintln!(
        "seed complete: {} objects, {:.2} GiB under {} in {:.2}s ({:.3} GiB/s{})",
        keys.len(),
        total_bytes as f64 / BYTES_PER_GIB as f64,
        opts.prefix,
        elapsed.as_secs_f64(),
        total_bytes as f64 / BYTES_PER_GIB as f64 / elapsed.as_secs_f64(),
        if opts.warm { ", PUT+warm-GET" } else { ", PUT" },
    );
    Ok(build_manifest(
        opts,
        &keys,
        total_bytes,
        digests.as_deref(),
        elapsed,
    ))
}

/// Fail on any key that exhausted its retries; otherwise put the digests back into
/// `keys` order, since `buffer_unordered` completes them in whatever order it likes
/// and `fetch --mode verify` reads the two arrays index-aligned.
///
/// `shared` is `Some` under [`Fill::Zero`] with `--checksum` — one digest for every
/// key — and the per-key digests are used when it is `None`.
///
/// # Errors
/// Returns a joined message listing every failed key, or a message if a key came
/// back without the digest `--checksum` asked for.
fn collect_digests(
    keys: &[String],
    results: Vec<Result<(String, Option<String>), String>>,
    shared: Option<&str>,
) -> Result<Option<Vec<String>>, String> {
    let mut by_key = std::collections::HashMap::new();
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok((key, digest)) => {
                by_key.insert(key, digest);
            }
            Err(e) => errors.push(e),
        }
    }
    if !errors.is_empty() {
        return Err(errors.join("\n"));
    }
    if let Some(d) = shared {
        return Ok(Some(vec![d.to_string(); keys.len()]));
    }
    let mut ordered = Vec::with_capacity(keys.len());
    for key in keys {
        match by_key.get(key) {
            Some(Some(digest)) => ordered.push(digest.clone()),
            // No digest anywhere means --checksum was off: nothing to record.
            Some(None) => return Ok(None),
            None => return Err(format!("key {key} reported neither success nor failure")),
        }
    }
    Ok(Some(ordered))
}

#[tokio::main]
async fn main() {
    let opts = match parse_opts() {
        Ok(o) => o,
        Err(e) => {
            eprintln!("seed: {e}");
            std::process::exit(2);
        }
    };
    match run(&opts).await {
        Ok(manifest) => println!("{manifest}"),
        Err(e) => {
            eprintln!("seed: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_size_units_and_bytes() {
        assert_eq!(parse_size("16MiB").unwrap(), 16 << 20);
        assert_eq!(parse_size("1GiB").unwrap(), 1 << 30);
        assert_eq!(parse_size("512KiB").unwrap(), 512 << 10);
        assert_eq!(parse_size("2TiB").unwrap(), 2u64 << 40);
        assert_eq!(parse_size("1048576").unwrap(), 1 << 20);
        assert!(parse_size("nope").is_err());
        assert!(parse_size("12ZiB").is_err());
    }

    #[test]
    fn parse_bool_spellings() {
        for t in ["true", "1", "YES", "on"] {
            assert!(parse_bool(t).unwrap());
        }
        for f in ["false", "0", "No", "off"] {
            assert!(!parse_bool(f).unwrap());
        }
        assert!(parse_bool("maybe").is_err());
    }

    #[test]
    fn generated_keys_match_classify_format() {
        let keys = generate_keys("lad3/obj-", 3);
        assert_eq!(
            keys,
            vec![
                "lad3/obj-00000000".to_string(),
                "lad3/obj-00000001".to_string(),
                "lad3/obj-00000002".to_string(),
            ]
        );
        assert_eq!(generate_keys("p", 0), Vec::<String>::new());
    }

    /// A fixed `Opts` for the manifest tests; `checksum` overridable per case.
    fn test_opts(checksum: bool) -> Opts {
        Opts {
            endpoint: "10.0.1.5:9000".into(),
            bucket: "cache".into(),
            prefix: "lad3/obj-".into(),
            count: 2,
            object_size: 16 << 20,
            object_size_str: "16MiB".into(),
            concurrency: 32,
            warm: true,
            checksum,
            fill: Fill::Zero,
            keys_file: None,
            region: DEFAULT_REGION.into(),
            access_key: DEFAULT_ACCESS_KEY.into(),
            secret_key: DEFAULT_SECRET_KEY.into(),
        }
    }

    /// A stand-in elapsed time for the manifest tests — any non-zero duration, so
    /// the derived rate is finite.
    const TEST_ELAPSED: Duration = Duration::from_secs(4);

    #[test]
    fn manifest_has_expected_fields() {
        let opts = test_opts(false);
        let keys = generate_keys(&opts.prefix, opts.count);
        let total = opts.object_size * keys.len() as u64;
        let m = build_manifest(&opts, &keys, total, None, TEST_ELAPSED);
        assert_eq!(m["count"], 2);
        assert_eq!(m["bytes"], total);
        assert_eq!(m["bucket"], "cache");
        assert_eq!(m["object_size"], "16MiB");
        assert_eq!(m["warm"], true);
        assert_eq!(m["fill"], "zero");
        assert_eq!(m["keys"][0], "lad3/obj-00000000");
        // 32 MiB in 4 s = 0.0078125 GiB/s, rounded to milli-GiB/s.
        assert_eq!(m["elapsed_seconds"], 4.0);
        assert_eq!(m["gibps"], 0.008);
        // Secret creds must NOT leak into the recorded manifest.
        assert!(m.get("secret_key").is_none());
    }

    #[test]
    fn manifest_default_omits_checksum_block() {
        // C2 requirement: with `--checksum` off (digest None) the manifest carries
        // no integrity block — those fields stay purely additive.
        let opts = test_opts(false);
        let keys = generate_keys(&opts.prefix, opts.count);
        let m = build_manifest(&opts, &keys, opts.object_size * 2, None, TEST_ELAPSED);
        assert!(
            m.get("checksum").is_none(),
            "checksum leaked into default manifest"
        );
        assert!(
            m.get("digests").is_none(),
            "digests leaked into default manifest"
        );
    }

    #[test]
    fn manifest_records_per_key_digests_when_enabled() {
        let opts = test_opts(true);
        let keys = generate_keys(&opts.prefix, opts.count);
        let digest = content_digest(&vec![0u8; opts.object_size as usize]);
        let repeated = vec![digest.clone(), digest.clone()];
        let m = build_manifest(
            &opts,
            &keys,
            opts.object_size * 2,
            Some(&repeated),
            TEST_ELAPSED,
        );
        assert_eq!(m["checksum"], CHECKSUM_ALGO);
        // One digest per key, index-aligned, all equal (identical objects).
        let digests = m["digests"].as_array().expect("digests is an array");
        assert_eq!(digests.len(), keys.len());
        assert_eq!(digests[0], digest.as_str());
        assert_eq!(digests[1], digest.as_str());
    }

    #[test]
    fn parse_fill_spellings() {
        assert_eq!(parse_fill("zero").unwrap(), Fill::Zero);
        assert_eq!(parse_fill("KEYED").unwrap(), Fill::Keyed);
        assert!(parse_fill("random").is_err());
    }

    /// The property ADR-0032's gate 3.1 rests on: a keyed payload changes when bytes
    /// MOVE, which a zero payload cannot express. Both halves are asserted, because
    /// the zero fill's blindness is exactly why the keyed one exists.
    #[test]
    fn keyed_fill_detects_displacement_that_zero_fill_cannot() {
        const SIZE: u64 = 4 << 10;
        let a = payload("obj-a", SIZE, Fill::Keyed);
        let b = payload("obj-b", SIZE, Fill::Keyed);
        assert_eq!(a.len(), SIZE as usize);
        // Two keys, same size: different bytes. Catches an assembly that used
        // another object's part, or a cache that staged one key's chunk under
        // another's.
        assert_ne!(a, b, "keyed payloads must differ per key");
        // Halves swapped inside ONE object — a mis-ordered assembly. The digest has
        // to move, or gate 3.1 proves nothing about part order.
        let half = a.len() / 2;
        let mut swapped = Vec::with_capacity(a.len());
        swapped.extend_from_slice(&a[half..]);
        swapped.extend_from_slice(&a[..half]);
        assert_ne!(
            content_digest(&a),
            content_digest(&swapped),
            "a keyed payload must detect reordered windows"
        );
        // The control: under the zero fill both mutations are undetectable.
        let z = payload("obj-a", SIZE, Fill::Zero);
        assert_eq!(z, payload("obj-b", SIZE, Fill::Zero));
        let mut z_swapped = Vec::with_capacity(z.len());
        z_swapped.extend_from_slice(&z[half..]);
        z_swapped.extend_from_slice(&z[..half]);
        assert_eq!(content_digest(&z), content_digest(&z_swapped));
    }

    /// Sizes that are not a whole number of fill words must still come out exactly
    /// `size` bytes long — an object size is a workload input, not a multiple of 8.
    #[test]
    fn keyed_fill_handles_a_partial_trailing_word() {
        for size in [1u64, 7, 8, 9, 4095] {
            assert_eq!(payload("k", size, Fill::Keyed).len(), size as usize);
        }
    }

    #[test]
    fn collect_digests_restores_key_order() {
        let keys = generate_keys("obj-", 3);
        // buffer_unordered completed them 2, 0, 1 — the manifest must not.
        let out_of_order = vec![
            Ok((keys[2].clone(), Some("cc".into()))),
            Ok((keys[0].clone(), Some("aa".into()))),
            Ok((keys[1].clone(), Some("bb".into()))),
        ];
        let digests = collect_digests(&keys, out_of_order, None).unwrap().unwrap();
        assert_eq!(digests, vec!["aa", "bb", "cc"]);
    }

    #[test]
    fn collect_digests_reports_every_failed_key() {
        let keys = generate_keys("obj-", 2);
        let results = vec![
            Err("key obj-00000000 FAILED".to_string()),
            Ok((keys[1].clone(), None)),
        ];
        let err = collect_digests(&keys, results, None).expect_err("a failure must fail the seed");
        assert!(err.contains("obj-00000000"), "{err}");
    }

    #[test]
    fn collect_digests_uses_the_shared_digest_for_every_key() {
        let keys = generate_keys("obj-", 3);
        let results = keys.iter().map(|k| Ok((k.clone(), None))).collect();
        let digests = collect_digests(&keys, results, Some("dd"))
            .unwrap()
            .unwrap();
        assert_eq!(digests, vec!["dd", "dd", "dd"]);
    }

    #[test]
    fn content_digest_is_fixed_width_hex_and_content_sensitive() {
        let d = content_digest(&[0u8; 4096]);
        assert_eq!(d.len(), CHECKSUM_HEX_WIDTH);
        assert!(d.chars().all(|c| c.is_ascii_hexdigit()));
        // Deterministic, and a single flipped byte changes the digest.
        assert_eq!(d, content_digest(&[0u8; 4096]));
        let mut other = vec![0u8; 4096];
        other[0] = 1;
        assert_ne!(d, content_digest(&other));
    }
}
