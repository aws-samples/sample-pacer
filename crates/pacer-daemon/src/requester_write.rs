//! `auth.mode: requester`'s write path with the tee on (ADR-0041 point 2, planning/30 § 3):
//! every write is forwarded exactly as the client sent it, and the ones that carry object
//! bytes — a `PutObject`, each `UploadPart` — are teed into the cache on the way through
//! ([`crate::populate`]). Nothing is visible before S3 says the object exists: a 2xx on the
//! PUT, or a `CompleteMultipartUploadResult` from Complete.
//!
//! The forwarded request and response are never modified. Two responses and one request
//! body are *read* — `CreateMultipartUpload`'s answer for its upload id, Complete's request
//! for the part list and its answer for the ETag — and each is relayed byte for byte.

use bytes::Bytes;
use http::{header, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use s3s::host::MultiDomain;
use tracing::warn;

use crate::authz::{object_address, out_body, query_has, query_value, Forwarder};
use crate::populate::{BodyShape, PartTee, TeeBody, WriteTee};

/// Largest XML answer read from S3 (`InitiateMultipartUploadResult`,
/// `CompleteMultipartUploadResult`): a few hundred bytes in practice.
const MAX_XML_RESPONSE: usize = 64 << 10;

/// Largest `CompleteMultipartUpload` request body read for its part list. 10,000 parts at
/// roughly 150 bytes each, checksums included, is 1.5 MB; past this the Complete is
/// forwarded untouched and the upload populates nothing.
const MAX_COMPLETE_BODY: usize = 4 << 20;

/// The write operations the tee acts on (planning/30 § 3.1). Everything else is forwarded
/// by the ordinary raw path.
pub(crate) enum WriteOp {
    Put {
        bucket: String,
        key: String,
    },
    CreateUpload {
        bucket: String,
        key: String,
    },
    UploadPart {
        bucket: String,
        key: String,
        upload_id: String,
        part: i32,
    },
    Complete {
        upload_id: String,
    },
    Abort {
        upload_id: String,
    },
}

impl WriteOp {
    /// Classify `req`, by method and query alone.
    pub(crate) fn of(req: &Request<Incoming>, domains: &MultiDomain) -> Option<Self> {
        let (bucket, key) = object_address(req, domains)?;
        let query = req.uri().query().unwrap_or("");
        let upload_id = query_value(query, "uploadId");
        let copy = req.headers().contains_key("x-amz-copy-source");
        match (req.method().as_str(), upload_id) {
            ("PUT", None) if query.is_empty() && !copy => Some(Self::Put { bucket, key }),
            ("PUT", Some(upload_id)) if !copy => {
                let part = query_value(query, "partNumber")?.parse().ok()?;
                Some(Self::UploadPart {
                    bucket,
                    key,
                    upload_id,
                    part,
                })
            }
            ("POST", None) if query_has(query, "uploads") => {
                Some(Self::CreateUpload { bucket, key })
            }
            ("POST", Some(upload_id)) => Some(Self::Complete { upload_id }),
            ("DELETE", Some(upload_id)) => Some(Self::Abort { upload_id }),
            _ => None,
        }
    }
}

/// Forward `req` — classified as `op` — through `forwarder`, teeing it through `tee`.
pub(crate) async fn forward(
    forwarder: &Forwarder,
    tee: &WriteTee,
    req: Request<Incoming>,
    op: WriteOp,
) -> Response<s3s::Body> {
    let result = match op {
        WriteOp::Put { bucket, key } => put(forwarder, tee, req, &bucket, &key).await,
        WriteOp::CreateUpload { bucket, key } => create(forwarder, tee, req, &bucket, &key).await,
        WriteOp::UploadPart {
            bucket,
            key,
            upload_id,
            part,
        } => {
            let target = (bucket.as_str(), key.as_str(), upload_id.as_str());
            upload_part(forwarder, tee, req, target, part).await
        }
        WriteOp::Complete { upload_id } => complete(forwarder, tee, req, &upload_id).await,
        WriteOp::Abort { upload_id } => abort(forwarder, tee, req, &upload_id).await,
    };
    result.unwrap_or_else(|e| {
        warn!(error = %e, "requester-mode write forward failed");
        let mut resp = Response::new(s3s::Body::empty());
        *resp.status_mut() = StatusCode::BAD_GATEWAY;
        resp
    })
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// How a body announces its object bytes, or `None` when it does not say: no tee.
fn shape(headers: &http::HeaderMap) -> Option<BodyShape> {
    let get = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let aws_chunked = get("content-encoding").is_some_and(|v| v.contains("aws-chunked"))
        || get("x-amz-content-sha256").is_some_and(|v| v.starts_with("STREAMING-"));
    let len = if aws_chunked {
        get("x-amz-decoded-content-length")
    } else {
        get("content-length")
    };
    Some(BodyShape {
        len: len?.trim().parse().ok()?,
        aws_chunked,
    })
}

/// Send `req` with its body wrapped in `tee` (when there is one).
async fn send_teed(
    forwarder: &Forwarder,
    req: Request<Incoming>,
    tee: Option<PartTee>,
) -> Result<Response<Incoming>, BoxError> {
    let (parts, body) = req.into_parts();
    Ok(forwarder
        .send(parts, out_body(TeeBody::new(body, tee)))
        .await?)
}

fn relay(resp: Response<Incoming>) -> Response<s3s::Body> {
    let (parts, body) = resp.into_parts();
    Response::from_parts(parts, s3s::Body::http_body(body))
}

async fn put(
    forwarder: &Forwarder,
    tee: &WriteTee,
    req: Request<Incoming>,
    bucket: &str,
    key: &str,
) -> Result<Response<s3s::Body>, BoxError> {
    let begun = shape(req.headers()).and_then(|s| tee.begin_put(bucket, key, &s));
    let (id, part) = begun.map_or((None, None), |(id, part)| (Some(id), Some(part)));
    let sent = send_teed(forwarder, req, part).await;
    let Some(id) = id else {
        return sent.map(relay);
    };
    let e_tag = sent
        .as_ref()
        .ok()
        .filter(|r| r.status().is_success())
        .and_then(|r| {
            r.headers()
                .get(header::ETAG)?
                .to_str()
                .ok()
                .map(|e| e.trim_matches('"').to_owned())
        });
    let tee = tee.clone();
    tokio::spawn(async move {
        match e_tag {
            Some(e_tag) => tee.commit(&id, &e_tag).await,
            None => tee.discard(&id).await,
        }
    });
    sent.map(relay)
}

async fn upload_part(
    forwarder: &Forwarder,
    tee: &WriteTee,
    req: Request<Incoming>,
    target: (&str, &str, &str),
    part: i32,
) -> Result<Response<s3s::Body>, BoxError> {
    let part_tee = shape(req.headers()).and_then(|s| tee.begin_part(target, part, &s));
    let sent = send_teed(forwarder, req, part_tee).await;
    if !sent.as_ref().is_ok_and(|r| r.status().is_success()) {
        // S3 did not take this part, so whatever it staged is not what the object will hold.
        tee.taint_upload(target.2);
    }
    sent.map(relay)
}

async fn create(
    forwarder: &Forwarder,
    tee: &WriteTee,
    req: Request<Incoming>,
    bucket: &str,
    key: &str,
) -> Result<Response<s3s::Body>, BoxError> {
    let resp = send_teed(forwarder, req, None).await?;
    if !resp.status().is_success() {
        return Ok(relay(resp));
    }
    let (parts, body) = read_response(resp).await?;
    if let Some(upload_id) = xml_values(&String::from_utf8_lossy(&body), "UploadId").next() {
        tee.open_upload(bucket, key, &unescape(upload_id));
    }
    Ok(rebuilt(parts, body))
}

async fn complete(
    forwarder: &Forwarder,
    tee: &WriteTee,
    req: Request<Incoming>,
    upload_id: &str,
) -> Result<Response<s3s::Body>, BoxError> {
    let small = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok()?.parse::<usize>().ok())
        .is_some_and(|len| len <= MAX_COMPLETE_BODY);
    if !small {
        spawn_discard(tee, upload_id);
        return Ok(relay(send_teed(forwarder, req, None).await?));
    }
    let (parts, body) = req.into_parts();
    let request_body = Limited::new(body, MAX_COMPLETE_BODY)
        .collect()
        .await?
        .to_bytes();
    let part_list: Vec<i32> = xml_values(&String::from_utf8_lossy(&request_body), "PartNumber")
        .filter_map(|n| n.trim().parse().ok())
        .collect();
    let resp = forwarder
        .send(parts, out_body(Full::new(request_body)))
        .await?;
    let status = resp.status();
    let (parts, body) = read_response(resp).await?;
    let text = String::from_utf8_lossy(&body);
    let e_tag = (status.is_success() && text.contains("<CompleteMultipartUploadResult"))
        .then(|| xml_values(&text, "ETag").next().map(unescape))
        .flatten()
        .map(|e| e.trim_matches('"').to_owned());
    let tee = tee.clone();
    let upload_id = upload_id.to_owned();
    tokio::spawn(async move {
        match e_tag {
            Some(e_tag) => tee.complete(&upload_id, &part_list, &e_tag).await,
            None => tee.discard_upload(&upload_id).await,
        }
    });
    Ok(rebuilt(parts, body))
}

async fn abort(
    forwarder: &Forwarder,
    tee: &WriteTee,
    req: Request<Incoming>,
    upload_id: &str,
) -> Result<Response<s3s::Body>, BoxError> {
    let resp = send_teed(forwarder, req, None).await?;
    if resp.status().is_success() {
        spawn_discard(tee, upload_id);
    }
    Ok(relay(resp))
}

fn spawn_discard(tee: &WriteTee, upload_id: &str) {
    let (tee, upload_id) = (tee.clone(), upload_id.to_owned());
    tokio::spawn(async move { tee.discard_upload(&upload_id).await });
}

/// Read one of S3's small XML answers whole.
async fn read_response(
    resp: Response<Incoming>,
) -> Result<(http::response::Parts, Bytes), BoxError> {
    let (parts, body) = resp.into_parts();
    let body = Limited::new(body, MAX_XML_RESPONSE)
        .collect()
        .await?
        .to_bytes();
    Ok((parts, body))
}

/// `parts` with `body` as a whole, fixed-length body — the answer S3 sent, byte for byte.
fn rebuilt(mut parts: http::response::Parts, body: Bytes) -> Response<s3s::Body> {
    parts.headers.remove(header::TRANSFER_ENCODING);
    parts
        .headers
        .insert(header::CONTENT_LENGTH, body.len().into());
    Response::from_parts(parts, s3s::Body::from(body))
}

/// Every `<tag>…</tag>` text in `xml`, in document order, still escaped.
fn xml_values<'a>(xml: &'a str, tag: &str) -> impl Iterator<Item = &'a str> + 'a {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut rest = xml;
    std::iter::from_fn(move || {
        let (_, after) = rest.split_once(open.as_str())?;
        let (value, tail) = after.split_once(close.as_str())?;
        rest = tail;
        Some(value)
    })
}

/// The five predefined XML entities, decoded — S3 writes an ETag as `&quot;…&quot;`.
fn unescape(s: &str) -> String {
    s.replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xml_values_are_found_in_order_and_unescaped() {
        let xml = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part>\
                   <Part><PartNumber>2</PartNumber></Part></CompleteMultipartUpload>";
        assert_eq!(
            xml_values(xml, "PartNumber").collect::<Vec<_>>(),
            ["1", "2"]
        );
        let result = "<CompleteMultipartUploadResult><ETag>&quot;abc-2&quot;</ETag>\
                      </CompleteMultipartUploadResult>";
        let e_tag = xml_values(result, "ETag").next().map(unescape);
        assert_eq!(e_tag.as_deref(), Some("\"abc-2\""));
        assert_eq!(xml_values("<a>1</a>", "b").next(), None);
    }

    #[test]
    fn a_body_announces_its_object_length_or_is_not_teed() {
        let mut h = http::HeaderMap::new();
        assert!(shape(&h).is_none(), "no length, no tee");
        h.insert("content-length", "42".parse().unwrap());
        let s = shape(&h).unwrap();
        assert_eq!((s.len, s.aws_chunked), (42, false));
        h.insert("content-encoding", "aws-chunked".parse().unwrap());
        assert!(
            shape(&h).is_none(),
            "an aws-chunked body must say its decoded length"
        );
        h.insert("x-amz-decoded-content-length", "30".parse().unwrap());
        let s = shape(&h).unwrap();
        assert_eq!((s.len, s.aws_chunked), (30, true));
    }
}
