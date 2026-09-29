//! `pacer-daemon warm`: warm every object under the prefixes, keys and manifests a caller
//! names, through a daemon, before the workload that needs them starts (ADR-0048).
//!
//! It is a subcommand of the daemon binary rather than a tool of its own so the image an
//! operator already runs is the image a warm Job runs: no second artifact to build, scan or
//! pin, and the wire contract in [`crate::warm`] has one definition used from both ends.
//!
//! What it does, in order: expand every source to objects (`ListObjectsV2` for a prefix,
//! `HeadObject` for a key, both through the daemon so `auth.mode: requester` authorizes
//! them), refuse a total above `--max-bytes`, then send warm-only GETs — one per slice of at
//! most `--slice` bytes, `--concurrency` at a time. A daemon that does not know the warm
//! header answers with a body; that is drained, and the object is warm anyway.
//!
//! Re-running is safe and cheap: a slice that is already warm is a cache hit. Every failure
//! is reported and the exit status is non-zero if there was one.

use std::collections::BTreeMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use aws_sdk_s3::config::{BehaviorVersion, ConfigBag, RuntimeComponents};
use aws_sdk_s3::error::DisplayErrorContext;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::context::{
    BeforeDeserializationInterceptorContextRef, BeforeTransmitInterceptorContextMut,
};
use aws_smithy_runtime_api::client::interceptors::Intercept;
use futures::StreamExt;
use tracing::{info, warn};

use super::plan::{self, Object, Source};
use super::{WARMED_HEADER, WARM_HEADER, WARM_REQUESTED, WARM_SKIPPED_HEADER};

/// The first argument that selects this command instead of the daemon.
pub const SUBCOMMAND: &str = "warm";

/// Warm-only GETs in flight at once. Each one resolves up to the daemon's own
/// `fill_parallelism` chunks, so 4 keeps a warm's footprint on the daemon it talks to at
/// a few times one client read's — enough to keep the backend busy, not enough to crowd
/// the workload the warm is for off a shared daemon. Raise it for a dedicated warm.
const DEFAULT_CONCURRENCY: usize = 4;

/// Longest byte range one warm GET covers. A request should finish in seconds whatever the
/// object's size, so no load balancer, SDK or proxy timeout in between ever sees a long
/// one; at the rates one node's daemon fills at, 1 GiB is seconds. A power of two, so it
/// is a whole number of chunks at every chunk size the daemon accepts.
const DEFAULT_SLICE_BYTES: u64 = 1 << 30;

/// How often progress is logged while a warm runs.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(10);

/// Byte count logged as the unit of a rate, so a rate line reads in GiB/s.
const GIB: f64 = (1u64 << 30) as f64;

/// `--help`.
const USAGE: &str = "\
usage: pacer-daemon warm --endpoint URL [options] [s3://bucket/prefix/ | s3://bucket/key]...

Warm every named object in the cache before it is first read. A URI ending in / (or naming
only a bucket) is every object under it; anything else is one object.

  --endpoint URL      the PACER daemon to warm through (required)
  --manifest PATH     also warm every s3:// URI listed in PATH, one per line
  --concurrency N     warm requests in flight at once (default 4)
  --slice SIZE        longest byte range one request covers (default 1Gi)
  --max-bytes SIZE    refuse to warm more than SIZE in total
  --dry-run           list what would be warmed, and warm nothing

SIZE is an integer, optionally with a Ki, Mi, Gi or Ti suffix.
Credentials and region come from the standard AWS environment.";

/// What the caller asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WarmArgs {
    /// The daemon every request goes through.
    pub endpoint: String,
    /// Sources named on the command line.
    pub sources: Vec<Source>,
    /// A manifest of more sources.
    pub manifest: Option<PathBuf>,
    /// Warm requests in flight at once.
    pub concurrency: usize,
    /// Longest byte range one request covers.
    pub slice_bytes: u64,
    /// Refuse a warm whose total is larger than this.
    pub max_bytes: Option<u64>,
    /// List and total, warm nothing.
    pub dry_run: bool,
}

/// Parse the arguments after [`SUBCOMMAND`]. `None` means `--help` was asked for and the
/// usage has been printed.
///
/// # Errors
///
/// An unknown flag, a flag without its value, a malformed value, or no source at all.
pub fn parse_args<I: IntoIterator<Item = String>>(args: I) -> anyhow::Result<Option<WarmArgs>> {
    let mut endpoint = None;
    let mut parsed = WarmArgs {
        endpoint: String::new(),
        sources: Vec::new(),
        manifest: None,
        concurrency: DEFAULT_CONCURRENCY,
        slice_bytes: DEFAULT_SLICE_BYTES,
        max_bytes: None,
        dry_run: false,
    };
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let (flag, inline) = match arg.split_once('=') {
            Some((f, v)) if f.starts_with("--") => (f.to_owned(), Some(v.to_owned())),
            _ => (arg.clone(), None),
        };
        let mut value = || {
            inline
                .clone()
                .or_else(|| args.next())
                .with_context(|| format!("{flag} needs a value"))
        };
        match flag.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(None);
            }
            "--endpoint" => endpoint = Some(value()?),
            "--manifest" => parsed.manifest = Some(PathBuf::from(value()?)),
            "--concurrency" => parsed.concurrency = value()?.parse().context("--concurrency")?,
            "--slice" => parsed.slice_bytes = plan::parse_size(&value()?).context("--slice")?,
            "--max-bytes" => {
                parsed.max_bytes = Some(plan::parse_size(&value()?).context("--max-bytes")?);
            }
            "--dry-run" => parsed.dry_run = true,
            f if f.starts_with('-') => bail!("unknown option {f}\n\n{USAGE}"),
            uri => parsed.sources.push(Source::parse(uri)?),
        }
    }
    parsed.endpoint = endpoint.with_context(|| format!("--endpoint is required\n\n{USAGE}"))?;
    if parsed.sources.is_empty() && parsed.manifest.is_none() {
        bail!("name at least one s3:// source or a --manifest\n\n{USAGE}");
    }
    if parsed.concurrency == 0 || parsed.slice_bytes == 0 {
        bail!("--concurrency and --slice must be at least 1");
    }
    Ok(Some(parsed))
}

/// Run the command: parse, warm, report. The daemon binary's `main` calls this when its
/// first argument is [`SUBCOMMAND`].
///
/// # Errors
///
/// Bad arguments, a source that could not be listed, a total over `--max-bytes`, or any
/// slice that failed to warm.
pub fn main<I: IntoIterator<Item = String>>(args: I) -> anyhow::Result<()> {
    let Some(args) = parse_args(args)? else {
        return Ok(());
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building the warm command's runtime")?;
    rt.block_on(async {
        let client = client_for(&args.endpoint).await;
        let summary = run(&client, &args).await?;
        summary.log();
        summary.into_result()
    })
}

/// An S3 client that sends every request to the daemon at `endpoint`, with credentials and
/// region from the standard AWS environment.
///
/// Path-style addressing, because the daemon is reached at one host name and serves every
/// bucket under it.
pub async fn client_for(endpoint: &str) -> aws_sdk_s3::Client {
    let shared = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let config = aws_sdk_s3::config::Builder::from(&shared)
        .endpoint_url(endpoint)
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

/// Expand, check and warm `args`' sources through `client`.
///
/// # Errors
///
/// A manifest that cannot be read, a source that cannot be listed, or a total over
/// `--max-bytes`. A slice that fails to warm is not an error here — it is counted in the
/// [`Summary`], so one bad object does not stop the rest.
pub async fn run(client: &aws_sdk_s3::Client, args: &WarmArgs) -> anyhow::Result<Summary> {
    let mut sources = args.sources.clone();
    if let Some(path) = &args.manifest {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading manifest {}", path.display()))?;
        sources.extend(plan::parse_manifest(&text)?);
    }
    let objects = plan::dedup(expand(client, &sources).await?);
    let mut summary = Summary {
        objects: objects.len() as u64,
        bytes: objects.iter().map(|o| o.size).sum(),
        ..Summary::default()
    };
    info!(
        objects = summary.objects,
        bytes = summary.bytes,
        dry_run = args.dry_run,
        "warm planned"
    );
    if let Some(max) = args.max_bytes.filter(|max| summary.bytes > *max) {
        bail!(
            "refusing to warm {} bytes: above --max-bytes {max}",
            summary.bytes
        );
    }
    if args.dry_run {
        for o in &objects {
            info!(bucket = %o.bucket, key = %o.key, size = o.size, "would warm");
        }
        return Ok(summary);
    }
    warm_all(client, &objects, args, &mut summary).await;
    Ok(summary)
}

/// Every object each source names, with its size.
async fn expand(client: &aws_sdk_s3::Client, sources: &[Source]) -> anyhow::Result<Vec<Object>> {
    let mut objects = Vec::new();
    for source in sources {
        if source.is_prefix() {
            list_prefix(client, source, &mut objects).await?;
        } else {
            let head = client
                .head_object()
                .bucket(&source.bucket)
                .key(&source.key)
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("{}", DisplayErrorContext(e)))
                .with_context(|| format!("s3://{}/{}", source.bucket, source.key))?;
            objects.push(Object {
                bucket: source.bucket.clone(),
                key: source.key.clone(),
                size: size_of(head.content_length()),
            });
        }
    }
    Ok(objects)
}

/// Append every non-empty object under `source`'s prefix to `objects`. Zero-byte objects —
/// the "folder" markers consoles create among them — have nothing to warm.
async fn list_prefix(
    client: &aws_sdk_s3::Client,
    source: &Source,
    objects: &mut Vec<Object>,
) -> anyhow::Result<()> {
    let mut pages = client
        .list_objects_v2()
        .bucket(&source.bucket)
        .prefix(&source.key)
        .into_paginator()
        .send();
    while let Some(page) = pages.next().await {
        let page = page
            .map_err(|e| anyhow::anyhow!("{}", DisplayErrorContext(e)))
            .with_context(|| format!("listing s3://{}/{}", source.bucket, source.key))?;
        for entry in page.contents() {
            let (Some(key), size) = (entry.key(), size_of(entry.size())) else {
                continue;
            };
            if size > 0 {
                objects.push(Object {
                    bucket: source.bucket.clone(),
                    key: key.to_owned(),
                    size,
                });
            }
        }
    }
    Ok(())
}

/// An S3 length as a byte count; S3 never reports a negative one.
fn size_of(len: Option<i64>) -> u64 {
    len.and_then(|n| u64::try_from(n).ok()).unwrap_or(0)
}

/// Warm every slice of every object, `args.concurrency` at a time, folding each outcome
/// into `summary` and logging progress as it goes.
async fn warm_all(
    client: &aws_sdk_s3::Client,
    objects: &[Object],
    args: &WarmArgs,
    summary: &mut Summary,
) {
    let started = Instant::now();
    let mut last_report = started;
    let work = objects
        .iter()
        .flat_map(|o| plan::slices(o.size, args.slice_bytes).map(move |range| (o, range)));
    let mut outcomes = futures::stream::iter(work)
        .map(|(object, range)| async move {
            let len = range.end - range.start;
            (object, len, warm_slice(client, object, range).await)
        })
        .buffer_unordered(args.concurrency);
    while let Some((object, len, outcome)) = outcomes.next().await {
        summary.record(object, len, outcome);
        if last_report.elapsed() >= PROGRESS_INTERVAL {
            last_report = Instant::now();
            summary.log_progress(started.elapsed());
        }
    }
}

/// What the daemon said about one slice.
#[derive(Debug)]
pub enum SliceOutcome {
    /// The daemon resolved it through the cache.
    Warmed,
    /// The daemon read nothing, and named why ([`crate::warm::SkipReason`]).
    Skipped(String),
    /// The daemon predates warm-only GETs and sent the slice as a body, now drained. The
    /// slice went through the cache all the same.
    Drained,
    /// The request failed.
    Failed(String),
}

/// One warm-only GET of `range` of `object`.
async fn warm_slice(
    client: &aws_sdk_s3::Client,
    object: &Object,
    range: Range<u64>,
) -> SliceOutcome {
    let answer = Arc::new(Mutex::new(None));
    let sent = client
        .get_object()
        .bucket(&object.bucket)
        .key(&object.key)
        .customize()
        .interceptor(WarmRequest {
            range: plan::range_header(&range),
            answer: Arc::clone(&answer),
        })
        .send()
        .await;
    let output = match sent {
        Ok(output) => output,
        Err(e) => return SliceOutcome::Failed(format!("{}", DisplayErrorContext(e))),
    };
    let answered = answer.lock().ok().and_then(|mut a| a.take());
    match answered {
        Some(WarmAnswer { skipped: Some(r) }) => SliceOutcome::Skipped(r),
        Some(WarmAnswer { skipped: None }) => SliceOutcome::Warmed,
        None => match drain(output.body).await {
            Ok(()) => SliceOutcome::Drained,
            Err(e) => SliceOutcome::Failed(format!("draining the body: {e}")),
        },
    }
}

/// Read a body to its end and keep none of it — piece by piece, so a slice-sized body from
/// a daemon that predates warm-only GETs never sits in memory whole.
async fn drain(mut body: aws_sdk_s3::primitives::ByteStream) -> anyhow::Result<()> {
    while body.try_next().await?.is_some() {}
    Ok(())
}

/// The warm half of the response headers, captured before the SDK's modelled output drops
/// them.
#[derive(Debug)]
struct WarmAnswer {
    /// [`WARM_SKIPPED_HEADER`]'s value, when the daemon read nothing.
    skipped: Option<String>,
}

/// Adds the warm header and the slice's `Range` to one request, and captures the answer.
///
/// Both headers go on in `modify_before_transmit`, **after** signing, so neither is under
/// the signature: `auth.mode: requester` re-emits the caller's request to S3 with a `Range`
/// of its own per chunk, which it can only do to a `Range` the caller left unsigned. The
/// same hook is how the delivery shims add their header.
#[derive(Debug)]
struct WarmRequest {
    /// The `Range` header value for this slice.
    range: String,
    /// Where [`Intercept::read_before_deserialization`] leaves the answer.
    answer: Arc<Mutex<Option<WarmAnswer>>>,
}

impl Intercept for WarmRequest {
    fn name(&self) -> &'static str {
        "PacerWarmRequest"
    }

    fn modify_before_transmit(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let headers = context.request_mut().headers_mut();
        headers.insert(WARM_HEADER, WARM_REQUESTED);
        headers.insert(http::header::RANGE.as_str(), self.range.clone());
        Ok(())
    }

    fn read_before_deserialization(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let headers = context.response().headers();
        if headers.get(WARMED_HEADER).is_none() {
            return Ok(());
        }
        let skipped = headers.get(WARM_SKIPPED_HEADER).map(str::to_owned);
        if let Ok(mut slot) = self.answer.lock() {
            *slot = Some(WarmAnswer { skipped });
        }
        Ok(())
    }
}

/// Upper bound on the failures [`Summary::failures`] lists by name.
const MAX_LISTED_FAILURES: usize = 20;

/// What a warm did.
#[derive(Debug, Default)]
pub struct Summary {
    /// Objects the sources expanded to.
    pub objects: u64,
    /// Their total size.
    pub bytes: u64,
    /// Bytes the daemon resolved through the cache.
    pub warmed_bytes: u64,
    /// Bytes drained from a daemon that predates warm-only GETs.
    pub drained_bytes: u64,
    /// Slices skipped, by the reason the daemon gave.
    pub skipped: BTreeMap<String, u64>,
    /// Slices that failed.
    pub failed: u64,
    /// The first 20 failures, as `bucket/key: error`. The count is always exact; past
    /// that many, the log would be the failures and nothing else.
    pub failures: Vec<String>,
}

impl Summary {
    /// Fold one slice's outcome in.
    fn record(&mut self, object: &Object, len: u64, outcome: SliceOutcome) {
        match outcome {
            SliceOutcome::Warmed => self.warmed_bytes += len,
            SliceOutcome::Drained => self.drained_bytes += len,
            SliceOutcome::Skipped(reason) => *self.skipped.entry(reason).or_default() += 1,
            SliceOutcome::Failed(e) => {
                self.failed += 1;
                if self.failures.len() < MAX_LISTED_FAILURES {
                    self.failures
                        .push(format!("s3://{}/{}: {e}", object.bucket, object.key));
                }
            }
        }
    }

    /// Log where a running warm has got to.
    fn log_progress(&self, elapsed: Duration) {
        let done = self.warmed_bytes + self.drained_bytes;
        info!(
            warmed_bytes = done,
            total_bytes = self.bytes,
            failed = self.failed,
            gib_per_s = done as f64 / GIB / elapsed.as_secs_f64().max(f64::EPSILON),
            "warm progress"
        );
    }

    /// Log the final account.
    pub fn log(&self) {
        info!(
            objects = self.objects,
            total_bytes = self.bytes,
            warmed_bytes = self.warmed_bytes,
            skipped = ?self.skipped,
            failed = self.failed,
            "warm finished"
        );
        if self.drained_bytes > 0 {
            warn!(
                drained_bytes = self.drained_bytes,
                "the daemon does not support warm-only GETs, so bodies were read and discarded; \
                 the objects are warm, but upgrading the daemon avoids moving the bytes"
            );
        }
        for failure in &self.failures {
            warn!(%failure, "warm failed");
        }
    }

    /// `Ok` when nothing failed.
    ///
    /// # Errors
    ///
    /// How many slices failed.
    pub fn into_result(self) -> anyhow::Result<()> {
        if self.failed > 0 {
            bail!("{} warm request(s) failed", self.failed);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> anyhow::Result<Option<WarmArgs>> {
        parse_args(list.iter().map(|s| (*s).to_owned()))
    }

    #[test]
    fn defaults_apply_and_both_flag_spellings_parse() {
        let a = args(&["--endpoint", "http://pacer:8080", "s3://m/llama/"])
            .unwrap()
            .unwrap();
        assert_eq!(a.endpoint, "http://pacer:8080");
        assert_eq!(a.concurrency, DEFAULT_CONCURRENCY);
        assert_eq!(a.slice_bytes, DEFAULT_SLICE_BYTES);
        assert!(!a.dry_run);
        let b = args(&[
            "--endpoint=http://p",
            "--slice=64Mi",
            "--max-bytes",
            "2Ti",
            "--dry-run",
            "--manifest",
            "keys.txt",
        ])
        .unwrap()
        .unwrap();
        assert_eq!(b.slice_bytes, 64 << 20);
        assert_eq!(b.max_bytes, Some(2 << 40));
        assert!(b.dry_run);
        assert_eq!(b.manifest, Some(PathBuf::from("keys.txt")));
    }

    #[test]
    fn an_endpoint_and_a_source_are_required() {
        assert!(args(&["s3://m/a"]).is_err());
        assert!(args(&["--endpoint", "http://p"]).is_err());
    }

    #[test]
    fn nonsense_is_refused_rather_than_ignored() {
        assert!(args(&["--endpoint", "http://p", "--bogus", "s3://m/a"]).is_err());
        assert!(args(&["--endpoint", "http://p", "--concurrency", "0", "s3://m/a"]).is_err());
        assert!(args(&["--endpoint", "http://p", "m/a"]).is_err());
        assert!(args(&["--endpoint"]).is_err());
    }

    #[test]
    fn help_prints_and_asks_for_nothing_else() {
        assert_eq!(args(&["--help"]).unwrap(), None);
    }

    #[test]
    fn a_summary_counts_every_outcome_and_fails_on_any_failure() {
        let o = Object {
            bucket: "m".into(),
            key: "k".into(),
            size: 30,
        };
        let mut s = Summary::default();
        s.record(&o, 10, SliceOutcome::Warmed);
        s.record(&o, 10, SliceOutcome::Skipped("object-size".into()));
        s.record(&o, 10, SliceOutcome::Failed("boom".into()));
        assert_eq!(s.warmed_bytes, 10);
        assert_eq!(s.skipped.get("object-size"), Some(&1));
        assert_eq!(s.failures, vec!["s3://m/k: boom".to_owned()]);
        assert!(s.into_result().is_err());
        assert!(Summary::default().into_result().is_ok());
    }
}
