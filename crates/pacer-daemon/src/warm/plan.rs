//! What a warm will read, decided before anything is read: the sources a caller named, the
//! objects they expand to, and the byte-range slices each object is warmed in.
//!
//! All of it is pure so it is tested on its own. The caller only ever names **objects** —
//! a key, a prefix, a manifest of either — and never a chunk: chunking is the daemon's
//! business, and a slice here is only a bound on how long one request runs.

use std::collections::BTreeMap;
use std::ops::Range;

use anyhow::{bail, Context};

/// URI scheme every source is written in.
const S3_SCHEME: &str = "s3://";

/// Line prefix that marks a manifest line as a comment.
const MANIFEST_COMMENT: char = '#';

/// One source a caller named: an exact object, or every object under a prefix.
///
/// The rule is the one `aws s3` users already read a URI by: a key ending in `/` — or no
/// key at all — is a prefix, anything else is exactly one object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Source {
    /// Bucket as the caller addresses it through the daemon (an alias is fine: the daemon
    /// maps it).
    pub bucket: String,
    /// The key, or the prefix when [`Self::is_prefix`].
    pub key: String,
}

impl Source {
    /// Parse `s3://bucket`, `s3://bucket/prefix/` or `s3://bucket/key`.
    ///
    /// # Errors
    ///
    /// A string that is not an `s3://` URI, or names no bucket.
    pub fn parse(uri: &str) -> anyhow::Result<Self> {
        let Some(rest) = uri.strip_prefix(S3_SCHEME) else {
            bail!("{uri:?} is not an s3:// URI");
        };
        let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            bail!("{uri:?} names no bucket");
        }
        Ok(Self {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
        })
    }

    /// Whether this names every object under [`Self::key`] rather than one object.
    #[must_use]
    pub fn is_prefix(&self) -> bool {
        self.key.is_empty() || self.key.ends_with('/')
    }
}

/// Parse a manifest: one `s3://` URI per line, blank lines and `#` comments ignored.
///
/// # Errors
///
/// The first line that is not a URI, with its line number.
pub fn parse_manifest(text: &str) -> anyhow::Result<Vec<Source>> {
    text.lines()
        .enumerate()
        .map(|(i, line)| (i + 1, line.trim()))
        .filter(|(_, line)| !line.is_empty() && !line.starts_with(MANIFEST_COMMENT))
        .map(|(n, line)| Source::parse(line).with_context(|| format!("manifest line {n}")))
        .collect()
}

/// One object to warm, and its size.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Object {
    /// Bucket, as addressed through the daemon.
    pub bucket: String,
    /// The object's key.
    pub key: String,
    /// Its length in bytes.
    pub size: u64,
}

/// De-duplicate `objects` by bucket and key, keeping the first size seen — a key named both
/// on its own and under a prefix is warmed once.
#[must_use]
pub fn dedup(objects: Vec<Object>) -> Vec<Object> {
    let mut seen = BTreeMap::new();
    for o in objects {
        seen.entry((o.bucket.clone(), o.key.clone())).or_insert(o);
    }
    seen.into_values().collect()
}

/// The byte ranges an object of `size` is warmed in, each at most `slice_bytes` long.
///
/// An empty object has none: there is nothing to warm, and a range over zero bytes is not
/// satisfiable.
///
/// # Panics
///
/// If `slice_bytes` is zero — [`crate::warm::command`] refuses that before planning.
pub fn slices(size: u64, slice_bytes: u64) -> impl Iterator<Item = Range<u64>> {
    assert!(slice_bytes > 0, "a slice must hold at least one byte");
    (0..size.div_ceil(slice_bytes)).map(move |i| {
        let start = i * slice_bytes;
        start..(start + slice_bytes).min(size)
    })
}

/// The `Range` header value for `range` (HTTP ranges are inclusive at the end).
#[must_use]
pub fn range_header(range: &Range<u64>) -> String {
    format!("bytes={}-{}", range.start, range.end - 1)
}

/// Binary size suffixes a size argument may carry, largest first so `Ti` is not read as `T`
/// followed by garbage.
const SIZE_SUFFIXES: [(&str, u32); 4] = [("Ti", 40), ("Gi", 30), ("Mi", 20), ("Ki", 10)];

/// Parse a byte count: a plain integer, or one with a binary suffix (`Ki`, `Mi`, `Gi`,
/// `Ti`) — the spelling Kubernetes quantities already use for the same thing.
///
/// # Errors
///
/// Anything else, or a count that does not fit in 64 bits.
pub fn parse_size(text: &str) -> anyhow::Result<u64> {
    let (digits, shift) = SIZE_SUFFIXES
        .iter()
        .find_map(|(suffix, shift)| text.strip_suffix(suffix).map(|d| (d, *shift)))
        .unwrap_or((text, 0));
    let n: u64 = digits
        .parse()
        .with_context(|| format!("{text:?} is not a size (an integer, optionally Ki/Mi/Gi/Ti)"))?;
    n.checked_mul(1 << shift)
        .with_context(|| format!("{text:?} does not fit in 64 bits"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_trailing_slash_or_no_key_is_a_prefix_and_anything_else_one_object() {
        let bucket_only = Source::parse("s3://models").unwrap();
        assert_eq!(bucket_only.key, "");
        assert!(bucket_only.is_prefix());
        assert!(Source::parse("s3://models/llama/").unwrap().is_prefix());
        let one = Source::parse("s3://models/llama/model.safetensors").unwrap();
        assert_eq!(one.bucket, "models");
        assert_eq!(one.key, "llama/model.safetensors");
        assert!(!one.is_prefix());
    }

    #[test]
    fn a_uri_without_the_scheme_or_a_bucket_is_refused() {
        assert!(Source::parse("models/llama/").is_err());
        assert!(Source::parse("s3:///llama/").is_err());
    }

    #[test]
    fn a_manifest_skips_blanks_and_comments_and_names_the_bad_line() {
        let parsed = parse_manifest("# weights\n\ns3://m/a\n  s3://m/b/  \n").unwrap();
        assert_eq!(parsed.len(), 2);
        assert!(parsed[1].is_prefix());
        let err = parse_manifest("s3://m/a\nnot-a-uri\n").unwrap_err();
        assert!(format!("{err:#}").contains("manifest line 2"), "{err:#}");
    }

    #[test]
    fn an_object_named_twice_is_warmed_once() {
        let o = |key: &str| Object {
            bucket: "m".into(),
            key: key.into(),
            size: 1,
        };
        assert_eq!(dedup(vec![o("a"), o("b"), o("a")]).len(), 2);
    }

    #[test]
    fn slices_tile_the_object_exactly_with_a_short_last_one() {
        let s: Vec<_> = slices(10, 4).collect();
        assert_eq!(s, vec![0..4, 4..8, 8..10]);
        assert_eq!(slices(8, 4).count(), 2);
        assert_eq!(slices(0, 4).count(), 0);
    }

    #[test]
    fn a_range_header_is_inclusive_at_the_end() {
        assert_eq!(range_header(&(0..1024)), "bytes=0-1023");
    }

    #[test]
    fn sizes_take_binary_suffixes() {
        assert_eq!(parse_size("4096").unwrap(), 4096);
        assert_eq!(parse_size("1Gi").unwrap(), 1 << 30);
        assert_eq!(parse_size("800Ti").unwrap(), 800 << 40);
        assert!(parse_size("1G").is_err());
        assert!(parse_size("99999999Ti").is_err());
    }
}
