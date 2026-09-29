//! Rendering a ring into manifests by running the shipped chart (ADR-0047 § 1).
//!
//! The operator does not re-implement the chart. `helm template` is a pure function of
//! (chart, release name, namespace, values) — the chart reads no cluster state (no
//! `lookup`, no `.Capabilities`) — so it can serve as the operator's renderer, and every
//! derivation and refusal the chart carries (memory limits, slab sizing, the disk-tier
//! default, its `fail` checks) holds for an operator-managed ring exactly as for a
//! `helm install`.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use kube::api::DynamicObject;
use serde::Deserialize;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

/// Longest a single `helm template` may run. It renders a dozen objects in well under a
/// second; a render that takes this long is hung, and holding the reconcile on it would
/// stall every other ring behind it.
const RENDER_TIMEOUT: Duration = Duration::from_secs(30);

/// Longest tail of helm's stderr kept in an error. A chart `fail` message is one line; a
/// schema failure lists each offending key. Bounded because the text lands in a status
/// condition, which the API server stores in etcd with every other status write.
const STDERR_TAIL_BYTES: usize = 2 << 10;

/// Why a ring could not be rendered.
#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    /// The chart refused the values: a schema violation or one of its `fail` checks. The
    /// ring's author has to change `spec.values`; retrying cannot help.
    #[error("chart refused the values: {0}")]
    Refused(String),
    /// The renderer itself could not run — a missing binary, a timeout, unparseable output.
    #[error("renderer failed: {0}")]
    Failed(String),
}

/// Something that turns a ring into manifests. A trait so the reconciler can be driven by
/// a fixed manifest set in tests.
pub trait Renderer: Send + Sync {
    /// Render release `name` in `namespace` with `values`.
    ///
    /// # Errors
    ///
    /// [`RenderError::Refused`] when the chart rejects the values, [`RenderError::Failed`]
    /// when rendering could not run at all.
    fn render(
        &self,
        name: &str,
        namespace: &str,
        values: &serde_json::Map<String, serde_json::Value>,
    ) -> impl std::future::Future<Output = Result<Vec<DynamicObject>, RenderError>> + Send;
}

/// Renders with a `helm` binary against a chart directory on disk.
#[derive(Debug, Clone)]
pub struct HelmRenderer {
    /// The `helm` executable.
    pub helm: PathBuf,
    /// The `pacer` chart directory.
    pub chart: PathBuf,
}

impl Renderer for HelmRenderer {
    async fn render(
        &self,
        name: &str,
        namespace: &str,
        values: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Vec<DynamicObject>, RenderError> {
        // JSON is YAML, so the values go in as-is on stdin: no temp file to clean up, and
        // nothing a value contains can be mistaken for a flag.
        let input = serde_json::to_vec(values).map_err(|e| RenderError::Failed(e.to_string()))?;
        let mut child = Command::new(&self.helm)
            .arg("template")
            .arg(name)
            .arg(&self.chart)
            .args(["--namespace", namespace, "--values", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| RenderError::Failed(format!("spawn {}: {e}", self.helm.display())))?;
        let mut stdin = child.stdin.take().expect("stdin is piped");
        stdin
            .write_all(&input)
            .await
            .map_err(|e| RenderError::Failed(e.to_string()))?;
        drop(stdin);

        let out = tokio::time::timeout(RENDER_TIMEOUT, child.wait_with_output())
            .await
            .map_err(|_| RenderError::Failed(format!("helm template exceeded {RENDER_TIMEOUT:?}")))?
            .map_err(|e| RenderError::Failed(e.to_string()))?;
        if !out.status.success() {
            // helm exits 1 for a refusal and for an unreadable chart alike; the chart is
            // baked into the operator's image, so a refusal is by far the likelier reading
            // and the stderr text says which it was either way.
            return Err(RenderError::Refused(stderr_tail(&out.stderr)));
        }
        let text = String::from_utf8(out.stdout).map_err(|e| RenderError::Failed(e.to_string()))?;
        parse_manifests(&text).map_err(RenderError::Failed)
    }
}

/// The last [`STDERR_TAIL_BYTES`] of `stderr`, trimmed, cut on a character boundary.
fn stderr_tail(stderr: &[u8]) -> String {
    let text = String::from_utf8_lossy(stderr);
    let text = text.trim();
    let mut start = text.len().saturating_sub(STDERR_TAIL_BYTES);
    while !text.is_char_boundary(start) {
        start += 1;
    }
    text[start..].to_owned()
}

/// Split a multi-document manifest stream into objects, skipping empty documents (a
/// template whose condition is off renders to nothing between two `---`).
///
/// # Errors
///
/// A document that is not valid YAML, or not an object with `apiVersion`/`kind`/`metadata`.
pub fn parse_manifests(text: &str) -> Result<Vec<DynamicObject>, String> {
    let mut objects = Vec::new();
    for doc in serde_yaml_ng::Deserializer::from_str(text) {
        let value = serde_json::Value::deserialize(doc).map_err(|e| e.to_string())?;
        if value.is_null() {
            continue;
        }
        let object: DynamicObject = serde_json::from_value(value).map_err(|e| e.to_string())?;
        if object.types.is_none() {
            return Err(format!(
                "document {:?} has no apiVersion/kind",
                object.metadata.name
            ));
        }
        objects.push(object);
    }
    Ok(objects)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_documents_are_skipped() {
        let text =
            "---\n# Source: a\n---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n---\n";
        let objects = parse_manifests(text).unwrap();
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].metadata.name.as_deref(), Some("a"));
    }

    #[test]
    fn a_document_without_a_kind_is_refused() {
        assert!(parse_manifests("metadata:\n  name: a\n").is_err());
    }

    #[test]
    fn stderr_tail_keeps_the_end_on_a_char_boundary() {
        let long = format!("{}é{}", "x".repeat(STDERR_TAIL_BYTES), "Error: the end");
        let tail = stderr_tail(long.as_bytes());
        assert!(tail.ends_with("Error: the end"));
        assert!(tail.len() <= STDERR_TAIL_BYTES);
    }
}
