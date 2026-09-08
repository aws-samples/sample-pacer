//! Classify object keys to their home node for the A4 EFA benchmark.
//!
//! The A4 benchmark (planning/14) needs keysets whose *home* is a known node so
//! it can force the peer plane deterministically: a requester GETting a key
//! homed on a different node always crosses the transport under test. This tool
//! links the real [`pacer_ring`] crate, so its ownership hash is byte-identical
//! to the daemon's (the `score_is_pinned` golden test guards that hash in CI);
//! there is no second implementation to drift.
//!
//! Ownership is scored on the *chunk key*, not the bare object key — the daemon
//! homes chunks, not objects (ADR-0015). For the A4 config (`PACER_BLOCK_SIZE`
//! large enough that a benchmark object is one chunk) every object is a single
//! chunk at index 0, so we score `"{bucket}/{key}#{chunk_size}:0"`.
//!
//! Output is one `owner_name<TAB>key` line per classified key, sorted by owner
//! then key, so the harness can `grep`/`awk` a per-home keyset:
//!
//! ```text
//! classify-keys --nodes ip-10-0-1-5,ip-10-0-1-6 --bucket bench \
//!     --chunk-size 1073741824 --prefix obj- --per-home 300
//! ```
//!
//! `--per-home N` emits exactly N keys for *every* node (scanning the generated
//! key space until each home is full), so both fan-out shapes are covered from
//! one run: many→one greps the holder's lines; one→many takes every line whose
//! owner is not the requester.

use pacer_ring::{NodeId, Ring};

/// Chunk index scored for ownership. The A4 object size is chosen to fit one
/// chunk, so every benchmark object is addressed at index 0.
const CHUNK_INDEX: u64 = 0;

/// Safety cap on how many candidate keys we generate per requested `per_home`
/// key before giving up — guards against a degenerate ring (e.g. one member
/// scoring nothing) spinning forever. Rendezvous hashing is near-uniform, so
/// ~40× headroom over `members × per_home` is comfortably enough in practice.
const SCAN_MULTIPLIER: u64 = 40;

/// CLI options, all `--flag value` with sensible benchmark defaults.
struct Opts {
    /// Ring member node names (the daemon's `PACER_NODE_NAME` values).
    nodes: Vec<String>,
    /// Bucket component of the object key (`"{bucket}/{key}"`).
    bucket: String,
    /// Chunk size embedded in the chunk key — must equal the daemon's
    /// `PACER_BLOCK_SIZE`, or classification will not match runtime ownership.
    chunk_size: u64,
    /// Key-name prefix; the tool appends a zero-padded sequence number.
    prefix: String,
    /// Number of keys to emit per home node.
    per_home: u64,
}

fn parse_opts() -> Result<Opts, String> {
    let mut nodes = None;
    let mut bucket = String::from("bench");
    let mut chunk_size: u64 = 1 << 30;
    let mut prefix = String::from("obj-");
    let mut per_home: u64 = 300;

    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut value = || args.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--nodes" => {
                nodes = Some(value()?.split(',').map(str::to_owned).collect::<Vec<_>>());
            }
            "--bucket" => bucket = value()?,
            "--chunk-size" => chunk_size = value()?.parse().map_err(|e| format!("{e}"))?,
            "--prefix" => prefix = value()?,
            "--per-home" => per_home = value()?.parse().map_err(|e| format!("{e}"))?,
            other => return Err(format!("unknown flag {other}")),
        }
    }

    let nodes = nodes.ok_or("--nodes is required (comma-separated node names)")?;
    if nodes.iter().any(String::is_empty) || nodes.is_empty() {
        return Err("--nodes must be a non-empty comma-separated list".into());
    }
    Ok(Opts {
        nodes,
        bucket,
        chunk_size,
        prefix,
        per_home,
    })
}

fn main() {
    let opts = match parse_opts() {
        Ok(o) => o,
        Err(e) => {
            eprintln!("classify-keys: {e}");
            std::process::exit(2);
        }
    };

    // Addresses are irrelevant to ownership (hashing is over the node name);
    // any placeholder keeps NodeId construction honest.
    let ring = Ring::new(
        opts.nodes
            .iter()
            .map(|name| NodeId::new(name.clone(), "0.0.0.0"))
            .collect(),
    );

    let mut counts: std::collections::HashMap<&str, u64> = std::collections::HashMap::new();
    let mut lines: Vec<(String, String)> = Vec::new();
    let target_total = opts.per_home * opts.nodes.len() as u64;
    let scan_limit = target_total.saturating_mul(SCAN_MULTIPLIER);

    let mut seq: u64 = 0;
    while (lines.len() as u64) < target_total && seq < scan_limit {
        let key = format!("{}{seq:08}", opts.prefix);
        seq += 1;
        let object_key = format!("{}/{key}", opts.bucket);
        let chunk_key = format!("{object_key}#{}:{CHUNK_INDEX}", opts.chunk_size);
        let owner = ring
            .owner(&chunk_key)
            .expect("non-empty ring always has an owner")
            .name();
        let seen = counts.entry(owner).or_insert(0);
        if *seen < opts.per_home {
            *seen += 1;
            lines.push((owner.to_owned(), key));
        }
    }

    if (lines.len() as u64) < target_total {
        eprintln!(
            "classify-keys: only classified {} of {target_total} keys after {seq} candidates \
             (some home under-filled) — raise --prefix cardinality or lower --per-home",
            lines.len()
        );
    }

    lines.sort();
    let mut out = String::with_capacity(lines.len() * 32);
    for (owner, key) in &lines {
        out.push_str(owner);
        out.push('\t');
        out.push_str(key);
        out.push('\n');
    }
    print!("{out}");
}
