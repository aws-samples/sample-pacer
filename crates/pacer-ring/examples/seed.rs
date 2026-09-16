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
//! # Two knobs that exist because this binary was the write path's wall
//!
//! Every ADR-0032 write arm on record pinned at ~1.1 GiB/s per writer node, and the
//! record attributed it to S3 part concurrency, the peer hop, the staging budget, and
//! the SDK's connector in turn (`bench/ladder/results/w1-peer-flows-and-part-size.md`).
//! Reading this binary's own request path gave two per-byte costs that are the
//! LOAD GENERATOR's, not PACER's, and both default OFF here so the historical control
//! still renders byte-for-byte:
//!
//!   * `--payload-signing` picks how the body is bound to the SigV4 signature. `sdk`
//!     (the default, and every arm on record) is what the Rust SDK does for an
//!     in-memory body: it sets no payload override for `PutObject`, so the body is
//!     signed as `SignableBody::Bytes` — a SHA-256 over the WHOLE object on the
//!     calling task before the first byte moves (`aws-runtime` `auth/sigv4.rs`), and
//!     the daemon's `s3s` front end then hashes every byte AGAIN to verify it
//!     (`sig_v4/upload_stream.rs`). `unsigned` sends `x-amz-content-sha256:
//!     UNSIGNED-PAYLOAD`, which is what boto3 and the CRT send S3 over TLS and what
//!     `pacer_dcp_writer.py` already sends the daemon — neither hash runs, and on
//!     ADR-0006's public placeholder identity neither protected anything. `streaming`
//!     is the third shape SigV4 defines, `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`:
//!     the body goes as `aws-chunked` 64 KiB chunks, each chunk signed as it is sent
//!     and each verified as it arrives, so both SHA-256 passes still run but overlap
//!     the transfer instead of preceding it. That is the "keep the signature, lose the
//!     stall" arm, and the SDK only takes it for a body it cannot see into — see
//!     [`streaming_body`] for how one is made from a buffer it could.
//!   * `--spawn true` runs each key's PUT in its own `tokio::spawn`ed task under a
//!     semaphore, instead of polling every in-flight future inside ONE
//!     `buffer_unordered` combinator on ONE worker thread. With the default, every
//!     concurrent object's hashing, checksumming, framing and `sendmsg` calls share a
//!     single core — a per-WRITER cap that a one-writer-per-node arm cannot tell from
//!     a per-node one.
//!
//! The manifest also carries the process's own `utime`/`stime` so a writer that is
//! CPU-bound announces itself, instead of the daemon being blamed for it.
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
use aws_sdk_s3::primitives::{ByteStream, SdkBody};
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

/// Default for `--payload-signing`: the SDK's own behaviour, and therefore what every write
/// arm on record measured — a whole-object SHA-256 on this side and a second one on the
/// daemon's. The default so that the historical arms stay the control; a throughput arm
/// picks another shape and says so in its record (see the module docs).
const DEFAULT_PAYLOAD_SIGNING: PayloadSigning = PayloadSigning::Sdk;

/// Default for `--spawn`: OFF, the single-task `buffer_unordered` fan-out every arm on
/// record ran. Off for the same reason as [`DEFAULT_PAYLOAD_SIGNING`]: the control must
/// keep reproducing, and the treatment must be a flag a record can quote.
const DEFAULT_SPAWN: bool = false;

/// Default `--mpu-parallelism`: `UploadPart`s in flight per object under `--mpu-part-size`.
/// AWS's own S3 performance guidance sizes ~15 concurrent requests to a 10 Gb/s link at
/// 8–16 MB parts; 16 is that figure rounded to the power of two, and with the seeder's own
/// object concurrency on top it is a per-object depth, not the node's.
const DEFAULT_MPU_PARALLELISM: usize = 16;

/// S3's floor for every part but the last of a multipart upload (5 MiB). A smaller
/// `--mpu-part-size` would fail at Complete with `EntityTooSmall`, after every part had
/// been uploaded — so it is refused at parse time instead.
const S3_MIN_PART_BYTES: u64 = 5 << 20;

/// Frame size [`streaming_body`] hands the SDK. Not the wire chunk — the SDK re-chunks
/// whatever it is given into its own 64 KiB signed chunks (`aws-runtime`
/// `content_encoding/body.rs`) — only the granularity at which the body is described to
/// it, so this trades a 1024-entry frame table per 1 GiB object against nothing; 1 MiB
/// keeps it small without making any frame large enough to matter.
const STREAMING_FRAME_BYTES: usize = 1 << 20;

/// Clock ticks per second in `/proc/self/stat`'s `utime`/`stime` fields. Linux fixes the
/// procfs `USER_HZ` at 100 as a userspace ABI constant independent of the kernel's own
/// `HZ`, which is why this is a literal rather than a `sysconf(_SC_CLK_TCK)` call that
/// would need `libc` — a dependency this example does not otherwise have.
const PROC_STAT_TICKS_PER_SECOND: f64 = 100.0;

/// Position of `utime` among the whitespace-separated fields that FOLLOW the closing
/// parenthesis of `comm` in `/proc/self/stat`: state, ppid, pgrp, session, tty_nr, tpgid,
/// flags, minflt, cminflt, majflt, cmajflt come first (fields 3–13 of the whole line), so
/// `utime` (field 14) is the twelfth, index 11, and `stime` (field 15) follows it. Counted
/// after the parenthesis because `comm` may itself contain spaces.
const PROC_STAT_UTIME_INDEX_AFTER_COMM: usize = 11;

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

/// Scheme that means "real S3, no daemon in the path" — see [`build_client`], which reads it
/// to pick the credential source. The daemon's own S3 front is plain HTTP by design
/// (ADR-0006 re-signs there), so the two targets cannot be confused by this test.
const DIRECT_ENDPOINT_SCHEME: &str = "https://";

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

/// How a PUT's body is bound to its SigV4 signature — the `--payload-signing` knob. See
/// the module docs for what each shape costs and protects.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PayloadSigning {
    /// The SDK's default for an in-memory body: `x-amz-content-sha256` is the hex SHA-256
    /// of the whole object, computed before the request is sent. Every arm on record.
    Sdk,
    /// `UNSIGNED-PAYLOAD`: the signature covers the headers (including the CRC32 the SDK
    /// adds), not the body. What every other AWS client sends over TLS.
    Unsigned,
    /// `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`: `aws-chunked` 64 KiB chunks, each
    /// signed on the way out and verified on the way in, overlapping the transfer.
    Streaming,
}

impl PayloadSigning {
    /// The spelling the flag takes and the manifest records.
    fn name(self) -> &'static str {
        match self {
            PayloadSigning::Sdk => "sdk",
            PayloadSigning::Unsigned => "unsigned",
            PayloadSigning::Streaming => "streaming",
        }
    }
}

/// Parse `--payload-signing`.
///
/// # Errors
/// Returns a message for anything but `sdk`/`unsigned`/`streaming`.
fn parse_payload_signing(s: &str) -> Result<PayloadSigning, String> {
    match s.to_ascii_lowercase().as_str() {
        "sdk" => Ok(PayloadSigning::Sdk),
        "unsigned" => Ok(PayloadSigning::Unsigned),
        "streaming" => Ok(PayloadSigning::Streaming),
        other => Err(format!(
            "bad --payload-signing '{other}' (want sdk/unsigned/streaming)"
        )),
    }
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
    /// How the body is bound to the signature — whole-object hash first (`sdk`), not at
    /// all (`unsigned`), or per 64 KiB chunk as it streams (`streaming`). Module docs.
    payload_signing: PayloadSigning,
    /// One spawned task per in-flight key rather than one `buffer_unordered` on one
    /// worker thread. See the module docs.
    spawn: bool,
    /// Client-side multipart, see [`Mpu`]. `part_size == 0` (the default) is one
    /// `PutObject` per object — the only shape the daemon's scatter can populate from.
    mpu: Mpu,
    /// Optional explicit key list (one per line); overrides generation.
    keys_file: Option<String>,
    /// SigV4 signing region (proxied away by the daemon; any value works).
    region: String,
    /// ADR-0006 placeholder access key the daemon accepts.
    access_key: String,
    /// ADR-0006 placeholder secret key the daemon accepts.
    secret_key: String,
}

/// `--mpu-part-size` / `--mpu-parallelism`: write each object as a client-side multipart
/// upload — `CreateMultipartUpload`, `parallelism` concurrent `UploadPart`s of `part_size`,
/// `CompleteMultipartUpload` — the way a CRT-class client does.
///
/// **For the no-PACER control only.** The whole-object `PutObject` a plain client sends is
/// capped by S3 at a rate per connection (~105 MiB/s measured), so "direct at c=N" measures
/// N connections, not the node: `results/w1-writer-payload-signing.md` found the same
/// ~3.3 GiB/s from a 40 Gbps and a 300 Gbps node at c=32, 64 and 128. A multipart client is
/// what a high-performance SDK is, and so the bar a scattered write has to be measured
/// against. Through the daemon this shape is WRONG, not slow: ADR-0032 hooks `PutObject`
/// only, so a client MPU populates nothing and its Complete purges the key
/// (`results/laguna-disk4096-and-control-rot.md`); `scatter.sh` refuses it there.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Mpu {
    /// Bytes per part; `0` = off (one `PutObject` per object).
    part_size: u64,
    /// `UploadPart`s in flight per object.
    parallelism: usize,
}

impl Mpu {
    /// Whether `len` bytes go up as parts rather than one PUT: multipart is on and the
    /// object is longer than one part. A one-part object is a `PutObject` — S3 would take a
    /// single-part MPU, but it is the same bytes over the same one connection with two
    /// extra round trips, so nothing is measured by it.
    fn applies_to(self, len: usize) -> bool {
        self.part_size > 0 && len as u64 > self.part_size
    }
}

/// The part table for `len` bytes at `part_size`: 1-based part numbers with the byte range
/// each covers, the last one short. Pure, so the boundary cases have a test.
fn part_ranges(len: usize, part_size: usize) -> Vec<(i32, std::ops::Range<usize>)> {
    (0..len)
        .step_by(part_size)
        .enumerate()
        .map(|(i, start)| ((i + 1) as i32, start..(start + part_size).min(len)))
        .collect()
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
    let mut payload_signing = DEFAULT_PAYLOAD_SIGNING;
    let mut spawn = DEFAULT_SPAWN;
    let mut mpu_part_size_str = String::new();
    let mut mpu_parallelism = DEFAULT_MPU_PARALLELISM;
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
            "--payload-signing" => payload_signing = parse_payload_signing(&value()?)?,
            "--spawn" => spawn = parse_bool(&value()?)?,
            "--mpu-part-size" => mpu_part_size_str = value()?,
            "--mpu-parallelism" => {
                mpu_parallelism = value()?.parse().map_err(|e| format!("{e}"))?;
            }
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
    let mpu = parse_mpu(&mpu_part_size_str, mpu_parallelism)?;
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
        payload_signing,
        spawn,
        mpu,
        keys_file,
        region,
        access_key,
        secret_key,
    })
}

/// Resolve `--mpu-part-size` (empty = off) and `--mpu-parallelism` into an [`Mpu`].
///
/// # Errors
/// A part below S3's 5 MiB floor, or a parallelism of zero — both would otherwise fail
/// only after every part of the first object had been uploaded.
fn parse_mpu(part_size: &str, parallelism: usize) -> Result<Mpu, String> {
    let part_size = if part_size.is_empty() {
        0
    } else {
        parse_size(part_size)?
    };
    if part_size != 0 && part_size < S3_MIN_PART_BYTES {
        return Err(format!(
            "--mpu-part-size {part_size} is below S3's {S3_MIN_PART_BYTES}-byte part floor"
        ));
    }
    if parallelism == 0 {
        return Err("--mpu-parallelism must be >= 1".into());
    }
    Ok(Mpu {
        part_size,
        parallelism,
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

/// Build the S3 client for `opts.endpoint`, adaptive retry ([`SDK_MAX_ATTEMPTS`]).
///
/// # Two credential sources, chosen by the endpoint's SCHEME
///
/// * **`http://` — the node-local daemon.** Static ADR-0006 placeholders and path-style,
///   which is every arm on record: the daemon re-signs with the node's own identity, so the
///   keys here are a formality and `pacer`/`pacer` is what they are.
/// * **`https://` — real S3, no daemon in the path** (`LADDER_WRITE_DIRECT=1`, the no-PACER
///   control). The SDK's default provider chain, so EKS Pod Identity is what signs, and
///   virtual-hosted addressing because path-style is deprecated for new buckets.
///
/// Derived rather than given as a flag, and that is deliberate. The fact is already spelled
/// twice — the bench chart ties `seed.serviceAccount` to the endpoint scheme in
/// `_validate.tpl`, and here the scheme picks the credential source — so a third spelling
/// could only ever disagree with those two. It is the same argument
/// `assert_write_object_within_put_cap` makes for reading one tri-state instead of inventing
/// a second.
///
/// ⚠ **What this fixes, and how it presented:** with static creds the direct control sent
/// `AWS_ACCESS_KEY_ID: pacer` to real S3 and got `InvalidAccessKeyId` — six retries per
/// object, on a billing fleet, reading as a broken Pod Identity association rather than as a
/// client that never consults the environment. `Credentials::new` also cannot carry a
/// **session token**, which temporary Pod Identity credentials require, so no combination of
/// `--access-key`/`--secret-key` could have worked (the warp bypass pod resolves creds in an
/// initContainer for exactly this reason).
async fn build_client(opts: &Opts) -> Client {
    let endpoint = if opts.endpoint.contains("://") {
        opts.endpoint.clone()
    } else {
        format!("{ENDPOINT_SCHEME}{}", opts.endpoint)
    };
    let direct = endpoint.starts_with(DIRECT_ENDPOINT_SCHEME);
    let mut conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(opts.region.clone()))
        .endpoint_url(endpoint.clone())
        .retry_config(RetryConfig::adaptive().with_max_attempts(SDK_MAX_ATTEMPTS));
    if direct {
        // `load_from_env` is the whole chain: Pod Identity's container credentials in
        // cluster, the usual env/profile/IMDS fall-backs anywhere else.
        let base = aws_config::load_from_env().await;
        conf = conf.credentials_provider(
            base.credentials_provider()
                .expect("the default chain always yields a provider"),
        );
    } else {
        conf = conf
            .credentials_provider(Credentials::new(
                &opts.access_key,
                &opts.secret_key,
                None,
                None,
                CREDENTIALS_PROVIDER_NAME,
            ))
            .force_path_style(true);
    }
    // Loud, because which identity signed is the difference between measuring PACER and
    // measuring S3, and the pod log is where an arm's provenance is read afterwards.
    eprintln!(
        "seed: {endpoint} — credentials: {}",
        if direct {
            "default chain (Pod Identity); NO daemon in the path"
        } else {
            "static ADR-0006 placeholders; the daemon re-signs"
        }
    );
    Client::from_conf(conf.build())
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
/// `signing` is `--payload-signing`. `unsigned` uses `disable_payload_signing`, the SDK's
/// own per-request switch for `PayloadSigningOverride::UnsignedPayload`, so the request is
/// otherwise identical — same `x-amz-checksum-crc32`, same headers, same body framing;
/// only `x-amz-content-sha256` changes, from a 64-hex digest of the whole body to
/// `UNSIGNED-PAYLOAD`, which is the one header that decides whether both ends hash it.
/// `streaming` hands the SDK a body it cannot read up front ([`streaming_body`]) and the
/// object's length explicitly, which is what makes it choose the `aws-chunked` signed
/// shape on its own (`aws-sdk-s3` `aws_chunked.rs`: `sign_during_encoding` is exactly
/// "the endpoint is `http:`").
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
    signing: PayloadSigning,
    mpu: Mpu,
) -> Result<(), String> {
    if mpu.applies_to(body.len()) {
        upload_multipart(client, bucket, key, body, signing, mpu).await?;
        return warm_get(client, bucket, key, warm).await;
    }
    let put = client.put_object().bucket(bucket).key(key);
    let sent = match signing {
        PayloadSigning::Sdk => put.body(ByteStream::from(body)).send().await,
        PayloadSigning::Unsigned => {
            put.body(ByteStream::from(body))
                .customize()
                .disable_payload_signing()
                .send()
                .await
        }
        PayloadSigning::Streaming => {
            // The aws-chunked interceptor needs the decoded length from somewhere, and a
            // body it cannot see into carries no size hint — so it is stated.
            let len = i64::try_from(body.len())
                .map_err(|e| format!("PUT {key}: object length does not fit i64: {e}"))?;
            put.content_length(len)
                .body(streaming_body(body))
                .send()
                .await
        }
    };
    sent.map_err(|e| format!("PUT {key}: {} [{}]", sdk_err(&e), DisplayErrorContext(&e)))?;
    warm_get(client, bucket, key, warm).await
}

/// The warm half of a seed: whole-object GET the key back so the daemon read-through-fills
/// every chunk into its ring home(s). A no-op when `warm` is off.
///
/// # Errors
/// The S3 error code and message on a failed GET, or the SDK's text on a failed drain.
async fn warm_get(client: &Client, bucket: &str, key: &str, warm: bool) -> Result<(), String> {
    if !warm {
        return Ok(());
    }
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
    Ok(())
}

/// Write one object as a client-side multipart upload — see [`Mpu`] for when and why.
///
/// Create, then the parts under a per-object semaphore of `mpu.parallelism`, then Complete
/// in part order. A failure anywhere after Create aborts the upload so S3 does not keep
/// paying for orphaned parts; the abort is best-effort and the original error is what is
/// returned.
///
/// # Errors
/// The first S3 error of Create, any part, or Complete, with its code and message.
async fn upload_multipart(
    client: &Client,
    bucket: &str,
    key: &str,
    body: Bytes,
    signing: PayloadSigning,
    mpu: Mpu,
) -> Result<(), String> {
    let created = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| {
            format!(
                "Create {key}: {} [{}]",
                sdk_err(&e),
                DisplayErrorContext(&e)
            )
        })?;
    let upload_id = created
        .upload_id()
        .ok_or_else(|| format!("Create {key}: no upload id"))?
        .to_owned();
    let uploaded = upload_parts(client, bucket, key, &upload_id, body, signing, mpu).await;
    let completed = match uploaded {
        Ok(parts) => client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await
            .map(|_| ())
            .map_err(|e| {
                format!(
                    "Complete {key}: {} [{}]",
                    sdk_err(&e),
                    DisplayErrorContext(&e)
                )
            }),
        Err(e) => Err(e),
    };
    if completed.is_err() {
        // Best effort, and its own failure is not the news: the error above is.
        let _ = client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
    }
    completed
}

/// Upload every part of `body`, `mpu.parallelism` at a time, returning them in part order.
///
/// `unsigned` applies `disable_payload_signing` per part, as the daemon does for its own;
/// under `sdk` and `streaming` a part is an in-memory body and is whole-part hashed by the
/// SDK before it moves, which is the SDK's own behaviour and is left as it is — this shape
/// exists for the direct control, which runs unsigned.
///
/// # Errors
/// The first failed part's S3 error, or a part task that panicked.
async fn upload_parts(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    body: Bytes,
    signing: PayloadSigning,
    mpu: Mpu,
) -> Result<Vec<aws_sdk_s3::types::CompletedPart>, String> {
    let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(mpu.parallelism));
    let mut tasks = tokio::task::JoinSet::new();
    let part_size = usize::try_from(mpu.part_size).unwrap_or(usize::MAX);
    for (number, range) in part_ranges(body.len(), part_size) {
        let permit = std::sync::Arc::clone(&slots)
            .acquire_owned()
            .await
            .expect("the seeder never closes its own semaphore");
        let (client, data) = (client.clone(), body.slice(range));
        let (bucket, key, upload_id) = (bucket.to_owned(), key.to_owned(), upload_id.to_owned());
        tasks.spawn(async move {
            let _slot = permit;
            let req = client
                .upload_part()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .part_number(number)
                .body(ByteStream::from(data));
            let out = match signing {
                PayloadSigning::Unsigned => req.customize().disable_payload_signing().send().await,
                PayloadSigning::Sdk | PayloadSigning::Streaming => req.send().await,
            }
            .map_err(|e| {
                format!(
                    "UploadPart {number}: {} [{}]",
                    sdk_err(&e),
                    DisplayErrorContext(&e)
                )
            })?;
            let e_tag = out
                .e_tag()
                .ok_or_else(|| format!("UploadPart {number}: no ETag"))?
                .to_owned();
            Ok::<_, String>(
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(number)
                    .e_tag(e_tag)
                    .build(),
            )
        });
    }
    let mut parts = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        parts.push(joined.map_err(|e| format!("part task panicked: {e}"))??);
    }
    // Complete wants ascending part numbers; the JoinSet hands them back in finish order.
    parts.sort_by_key(|p| p.part_number());
    Ok(parts)
}

/// The same bytes as a body the SDK cannot read up front, so it takes its streaming path.
///
/// `SdkBody::bytes()` answers `Some` only for a body built from a buffer, and that answer
/// is what `must_not_use_chunked_encoding` keys on (`aws-sdk-s3` `aws_chunked.rs`): an
/// in-memory body is never aws-chunked and is always whole-object hashed. A body built
/// through `from_body_1_x` answers `None`, so the SDK chunk-encodes it, signs each chunk
/// as it goes over `http:`, and puts the CRC32 in a trailer. The bytes themselves are the
/// same shared buffer sliced into [`STREAMING_FRAME_BYTES`] frames — no copy — and the
/// body is `retryable`, rebuilt from that buffer for each attempt, because a streaming
/// body the SDK cannot replay would turn the seeder's retry into an error.
fn streaming_body(body: Bytes) -> ByteStream {
    let sdk_body = SdkBody::retryable(move || {
        let frames: Vec<Result<hyper::body::Frame<Bytes>, std::convert::Infallible>> = (0..body
            .len())
            .step_by(STREAMING_FRAME_BYTES)
            .map(|start| {
                let end = (start + STREAMING_FRAME_BYTES).min(body.len());
                Ok(hyper::body::Frame::data(body.slice(start..end)))
            })
            .collect();
        SdkBody::from_body_1_x(http_body_util::StreamBody::new(stream::iter(frames)))
    });
    ByteStream::new(sdk_body)
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
    signing: PayloadSigning,
    mpu: Mpu,
) -> Result<(), String> {
    let mut last = String::new();
    for attempt in 1..=PER_KEY_MAX_ATTEMPTS {
        match seed_one(client, bucket, key, body.clone(), warm, signing, mpu).await {
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
        // Both recorded because a rate is only comparable to another taken the same way:
        // every arm before these flags existed ran `sdk`/`false`.
        "payload_signing": opts.payload_signing.name(),
        "spawn": opts.spawn,
        // 0 = one PutObject per object. Non-zero only ever on the direct control.
        "mpu_part_size": opts.mpu.part_size,
        "mpu_parallelism": opts.mpu.parallelism,
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
    let client = build_client(opts).await;
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

    let job = KeyJob {
        client,
        bucket: opts.bucket.clone(),
        shared,
        warm: opts.warm,
        checksum: opts.checksum,
        fill: opts.fill,
        size: opts.object_size,
        signing: opts.payload_signing,
        mpu: opts.mpu,
    };
    let cpu_before = process_cpu_seconds();
    let started = std::time::Instant::now();
    let results = if opts.spawn {
        run_spawned(&job, &keys, opts.concurrency).await
    } else {
        run_single_task(&job, &keys, opts.concurrency).await
    };
    let elapsed = started.elapsed();
    let cpu = cpu_delta(cpu_before, process_cpu_seconds());

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
    if let Some((user, system)) = cpu {
        // The line a CPU-bound writer is identified by: ~1.0 cores with the default
        // `--spawn false` is one worker thread pinned, whatever the node has spare.
        eprintln!(
            "seed cpu: user {user:.2}s + system {system:.2}s = {:.2} cores over the run \
             (payload_signing={}, spawn={})",
            (user + system) / elapsed.as_secs_f64(),
            opts.payload_signing.name(),
            opts.spawn,
        );
    }
    let mut manifest = build_manifest(opts, &keys, total_bytes, digests.as_deref(), elapsed);
    attach_cpu(&mut manifest, cpu, elapsed);
    Ok(manifest)
}

/// Everything one key's PUT needs besides the key. Cloned per key: every field is a
/// handle or a `Copy`, so a clone is a few refcount bumps, and it is what lets a spawned
/// task own its inputs without a `'static` borrow.
#[derive(Clone)]
struct KeyJob {
    client: Client,
    bucket: String,
    /// The shared zero buffer under [`Fill::Zero`]; `None` under [`Fill::Keyed`], where
    /// the payload is built per key inside the task.
    shared: Option<Bytes>,
    warm: bool,
    checksum: bool,
    fill: Fill,
    size: u64,
    signing: PayloadSigning,
    mpu: Mpu,
}

/// One key's outcome for the manifest: the key and its digest if `--checksum` asked.
type KeyResult = Result<(String, Option<String>), String>;

/// PUT (+GET) one key. The body of both dispatch shapes, so they cannot drift apart.
async fn seed_key(job: KeyJob, key: String) -> KeyResult {
    let body = job
        .shared
        .unwrap_or_else(|| payload(&key, job.size, job.fill));
    let digest = job.checksum.then(|| content_digest(&body));
    seed_one_with_retry(
        &job.client,
        &job.bucket,
        &key,
        body,
        job.warm,
        job.signing,
        job.mpu,
    )
    .await?;
    Ok((key, digest))
}

/// The historical fan-out: `concurrency` futures polled by ONE `buffer_unordered`, which
/// means by one task on one worker thread. Every in-flight key's hashing, checksumming,
/// framing and `sendmsg` calls take turns on that thread — see the module docs for what
/// that cost every write arm on record.
async fn run_single_task(job: &KeyJob, keys: &[String], concurrency: usize) -> Vec<KeyResult> {
    stream::iter(keys.iter().cloned())
        .map(|key| seed_key(job.clone(), key))
        .buffer_unordered(concurrency)
        .collect()
        .await
}

/// `--spawn true`: one `tokio::spawn`ed task per in-flight key, so the runtime spreads
/// them over its worker threads.
///
/// The permit is taken BEFORE the spawn, so at most `concurrency` tasks exist at once and
/// the live-buffer bound the memory guard in `scatter.sh` computes
/// (`concurrency × object_size` under `keyed`) is the same one `buffer_unordered` gave.
/// Acquiring inside the task would spawn every key's task up front, each holding its
/// payload while parked — the exact ordering defect `coordinate.rs`'s `dispatch` documents
/// having paid for on hardware.
async fn run_spawned(job: &KeyJob, keys: &[String], concurrency: usize) -> Vec<KeyResult> {
    let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut tasks = tokio::task::JoinSet::new();
    for key in keys {
        let permit = std::sync::Arc::clone(&slots)
            .acquire_owned()
            .await
            .expect("the seeder never closes its own semaphore");
        let job = job.clone();
        let key = key.clone();
        tasks.spawn(async move {
            // Named so it lives until the PUT (and any warm GET) has finished.
            let _slot = permit;
            seed_key(job, key).await
        });
    }
    let mut results = Vec::with_capacity(keys.len());
    while let Some(joined) = tasks.join_next().await {
        results.push(joined.unwrap_or_else(|e| Err(format!("seed task panicked: {e}"))));
    }
    results
}

/// This process's `(user, system)` CPU seconds so far, from `/proc/self/stat`; `None`
/// where procfs is absent (a macOS unit test), never an error — the number is a
/// diagnostic beside the rate, not a gate on it.
fn process_cpu_seconds() -> Option<(f64, f64)> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    parse_proc_stat_cpu(&stat)
}

/// `(utime, stime)` in seconds out of one `/proc/<pid>/stat` line.
///
/// Splits after the LAST `)` rather than on whitespace from the start, because field 2
/// (`comm`) is parenthesised and may itself contain spaces — a thread named `seed (2)`
/// would otherwise shift every later field by one.
fn parse_proc_stat_cpu(stat: &str) -> Option<(f64, f64)> {
    let after_comm = &stat[stat.rfind(')')? + 1..];
    let mut fields = after_comm.split_whitespace();
    let utime: f64 = fields.nth(PROC_STAT_UTIME_INDEX_AFTER_COMM)?.parse().ok()?;
    let stime: f64 = fields.next()?.parse().ok()?;
    Some((
        utime / PROC_STAT_TICKS_PER_SECOND,
        stime / PROC_STAT_TICKS_PER_SECOND,
    ))
}

/// The CPU this run consumed: `after − before`, or `None` if either reading is missing.
/// Differenced so the client's own startup (argument parsing, the SDK's config load, the
/// shared buffer's allocation) is not charged to the write.
fn cpu_delta(before: Option<(f64, f64)>, after: Option<(f64, f64)>) -> Option<(f64, f64)> {
    let (bu, bs) = before?;
    let (au, as_) = after?;
    Some(((au - bu).max(0.0), (as_ - bs).max(0.0)))
}

/// Add the run's CPU to the manifest, when it could be read: `cpu_user_seconds`,
/// `cpu_system_seconds` and `cpu_cores` (their sum over the wall clock). Additive, like
/// the checksum block: a manifest with no procfs behind it is unchanged.
///
/// Split user from system on purpose. This tree once struck the daemon off a suspect
/// list on a USER-only figure that understated it ~50× (`results/http-path-ceiling.md`);
/// a single total would let the same mistake happen to the writer.
fn attach_cpu(manifest: &mut serde_json::Value, cpu: Option<(f64, f64)>, elapsed: Duration) {
    let Some((user, system)) = cpu else {
        return;
    };
    let secs = elapsed.as_secs_f64();
    let round = |v: f64| (v * 1000.0).round() / 1000.0;
    manifest["cpu_user_seconds"] = serde_json::json!(round(user));
    manifest["cpu_system_seconds"] = serde_json::json!(round(system));
    manifest["cpu_cores"] = serde_json::json!(if secs > 0.0 {
        round((user + system) / secs)
    } else {
        0.0
    });
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
            payload_signing: DEFAULT_PAYLOAD_SIGNING,
            spawn: DEFAULT_SPAWN,
            mpu: Mpu {
                part_size: 0,
                parallelism: DEFAULT_MPU_PARALLELISM,
            },
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
        // The two request-shape knobs are always recorded, so a rate can be compared
        // only against another taken the same way.
        assert_eq!(m["payload_signing"], "sdk");
        assert_eq!(m["spawn"], false);
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

    /// A real `/proc/self/stat` line, with a `comm` that contains a space and a
    /// parenthesis — the shape a whitespace split from the start gets wrong.
    #[test]
    fn proc_stat_cpu_is_read_after_the_comm_field() {
        let line = "4242 (seed (2)) S 1 4242 4242 0 -1 4194560 12345 0 3 0 \
                    150 25 0 0 20 0 9 0 98765 1073741824 4096 18446744073709551615 1 1 0 0 0 0 \
                    0 0 0 0 0 0 17 3 0 0 0 0 0 0 0 0 0 0 0 0 0\n";
        let (user, system) = parse_proc_stat_cpu(line).expect("a well-formed stat line");
        // 150 and 25 ticks at USER_HZ = 100.
        assert!((user - 1.5).abs() < f64::EPSILON, "utime: {user}");
        assert!((system - 0.25).abs() < f64::EPSILON, "stime: {system}");
        assert!(parse_proc_stat_cpu("garbage").is_none());
        assert!(parse_proc_stat_cpu("1 (x) S 1").is_none(), "too few fields");
    }

    /// The CPU block is additive and differenced: absent without procfs, and never
    /// charged with what ran before the timer started.
    #[test]
    fn cpu_block_is_differenced_and_optional() {
        assert_eq!(
            cpu_delta(Some((1.0, 0.5)), Some((3.5, 1.0))),
            Some((2.5, 0.5))
        );
        assert_eq!(cpu_delta(None, Some((3.5, 1.0))), None);
        assert_eq!(cpu_delta(Some((1.0, 0.5)), None), None);

        let opts = test_opts(false);
        let keys = generate_keys(&opts.prefix, opts.count);
        let mut m = build_manifest(&opts, &keys, opts.object_size * 2, None, TEST_ELAPSED);
        attach_cpu(&mut m, None, TEST_ELAPSED);
        assert!(m.get("cpu_cores").is_none(), "no procfs, no block");
        // 3 s user + 1 s system over a 4 s run is one core.
        attach_cpu(&mut m, Some((3.0, 1.0)), TEST_ELAPSED);
        assert_eq!(m["cpu_user_seconds"], 3.0);
        assert_eq!(m["cpu_system_seconds"], 1.0);
        assert_eq!(m["cpu_cores"], 1.0);
    }

    #[test]
    fn request_shape_flags_default_to_the_historical_control_and_parse() {
        // Both default to the historical behaviour, so an existing arm's invocation is
        // the control without knowing these flags exist.
        assert_eq!(DEFAULT_PAYLOAD_SIGNING, PayloadSigning::Sdk);
        assert!(!DEFAULT_SPAWN);
        assert_eq!(parse_payload_signing("SDK").unwrap(), PayloadSigning::Sdk);
        assert_eq!(
            parse_payload_signing("unsigned").unwrap(),
            PayloadSigning::Unsigned
        );
        assert_eq!(
            parse_payload_signing("streaming").unwrap(),
            PayloadSigning::Streaming
        );
        assert!(
            parse_payload_signing("true").is_err(),
            "the old boolean spelling is gone"
        );
        for mode in [
            PayloadSigning::Sdk,
            PayloadSigning::Unsigned,
            PayloadSigning::Streaming,
        ] {
            assert_eq!(parse_payload_signing(mode.name()).unwrap(), mode);
        }
    }

    /// The part table is what Complete is built from, so its boundaries are the whole risk:
    /// an off-by-one here is an `InvalidPart` after every byte has been uploaded.
    #[test]
    fn part_ranges_cover_the_body_exactly_once_with_a_short_tail() {
        // Two full parts and a short third.
        assert_eq!(
            part_ranges(25, 10),
            vec![(1, 0..10), (2, 10..20), (3, 20..25)]
        );
        // An exact multiple has no short part.
        assert_eq!(part_ranges(20, 10), vec![(1, 0..10), (2, 10..20)]);
        // A body shorter than one part is one (short) part — and `applies_to` says such an
        // object is a plain PutObject anyway.
        assert_eq!(part_ranges(7, 10), vec![(1, 0..7)]);
        assert!(part_ranges(0, 10).is_empty());
        let mpu = Mpu {
            part_size: 10,
            parallelism: 1,
        };
        assert!(mpu.applies_to(11));
        assert!(!mpu.applies_to(10), "one part is a PutObject, not an MPU");
        assert!(
            !Mpu {
                part_size: 0,
                parallelism: 1
            }
            .applies_to(1 << 30),
            "part_size 0 is off"
        );
    }

    /// The multipart knobs refuse at parse time what S3 would refuse after the upload.
    #[test]
    fn mpu_knobs_refuse_below_the_part_floor_and_zero_parallelism() {
        assert_eq!(
            parse_mpu("", DEFAULT_MPU_PARALLELISM).unwrap(),
            Mpu {
                part_size: 0,
                parallelism: DEFAULT_MPU_PARALLELISM
            }
        );
        assert_eq!(parse_mpu("16MiB", 8).unwrap().part_size, 16 << 20);
        assert!(parse_mpu("4MiB", 8).is_err(), "below S3's 5 MiB floor");
        assert!(parse_mpu("16MiB", 0).is_err());
    }

    /// The streaming body must be the SAME bytes, in order, in frames the SDK can replay —
    /// and it must NOT be readable up front, or the SDK would silently take the whole-object
    /// path and the "streaming" leg would measure the control.
    #[tokio::test]
    async fn streaming_body_is_the_same_bytes_and_opaque_to_the_sdk() {
        use http_body_util::BodyExt;
        // Three full frames and a partial fourth, so the tail is exercised.
        let len = STREAMING_FRAME_BYTES * 3 + 4321;
        let body: Bytes = (0..len).map(|i| (i % 251) as u8).collect();
        let stream = streaming_body(body.clone());
        let inner = stream.into_inner();
        assert!(
            inner.bytes().is_none(),
            "an in-memory body is whole-object hashed by the SDK, not chunk-signed"
        );
        assert!(
            inner.try_clone().is_some(),
            "the seeder retries a failed PUT, so the body must be replayable"
        );
        let collected = inner
            .collect()
            .await
            .expect("a streaming body drains")
            .to_bytes();
        assert_eq!(collected.len(), len);
        assert_eq!(
            collected, body,
            "the frames must reassemble to the original bytes"
        );
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
