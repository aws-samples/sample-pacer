//! Generated control-plane types. See `proto/pacer/v1/peer.proto`.

/// Types generated from `pacer.v1` (tonic/prost — style lints off: the source
/// of truth is the .proto file, not the generated Rust).
///
/// `result_large_err` is here for the same reason it is on
/// `pacer_daemon::peer::PeerService::serve_cached`: `tonic::Status` is ~180 bytes
/// by design and tonic returns it **by value** from every RPC signature, so there
/// is nothing on our side to box. It fires on the generated *client* stubs
/// specifically because those are inherent methods — clippy exempts the trait
/// impls that make up the server. Needed from clippy 1.98 (the public mirror's
/// CI tracks stable; the internal pipeline pins 1.96, where it does not fire yet).
#[allow(
    missing_docs,
    clippy::excessive_nesting,
    clippy::missing_errors_doc,
    clippy::result_large_err,
    clippy::too_many_lines
)]
pub mod v1 {
    tonic::include_proto!("pacer.v1");
}

#[cfg(test)]
mod tests {
    //! Wire round-trip coverage for the ADR-0018/0019 EFA fields added in
    //! A1: no `ibverbs`/hardware dependency here, just prost encode/decode —
    //! CI's default `cargo test` (no `efa` feature, no libefa) runs these on
    //! every commit, unlike anything that touches `pacer_transport::efa`.
    use prost::Message;

    use super::v1::{
        BlobMeta, ClientToken, ClientTokenRail, EfaEndpoint, FetchBlobRequest, HandshakeRequest,
        HandshakeResponse, RdmaBuffer, RdmaCapabilities,
    };

    fn roundtrip<M: Message + Default + PartialEq>(msg: &M) -> M {
        let bytes = msg.encode_to_vec();
        M::decode(bytes.as_slice()).expect("decoding what we just encoded")
    }

    #[test]
    fn handshake_request_roundtrips_efa_endpoint() {
        let req = HandshakeRequest {
            node_id: "node-a".into(),
            capabilities: Some(RdmaCapabilities {
                rdma_read: false,
                rdma_write: true,
                send_recv: false,
            }),
            protocol_version: 1,
            efa_endpoint: Some(EfaEndpoint {
                queue_pair_endpoint: vec![0xAB; 23],
                rail_endpoints: vec![vec![0xAB; 23], vec![0xAC; 23]],
            }),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn handshake_request_without_efa_endpoint_roundtrips_as_none() {
        // Phase 2 / non-EFA daemons never set this field — confirm absence
        // survives encode/decode, not just presence (a oneof-shaped bug
        // could make `None` silently become `Some(default)`).
        let req = HandshakeRequest {
            node_id: "node-a".into(),
            capabilities: Some(RdmaCapabilities::default()),
            protocol_version: 1,
            efa_endpoint: None,
        };
        let got = roundtrip(&req);
        assert_eq!(got.efa_endpoint, None);
        assert_eq!(got, req);
    }

    #[test]
    fn handshake_response_roundtrips_efa_endpoint() {
        let resp = HandshakeResponse {
            node_id: "node-b".into(),
            capabilities: Some(RdmaCapabilities {
                rdma_read: false,
                rdma_write: true,
                send_recv: false,
            }),
            protocol_version: 1,
            efa_endpoint: Some(EfaEndpoint {
                queue_pair_endpoint: vec![0xCD; 23],
                rail_endpoints: vec![vec![0xCD; 23]],
            }),
        };
        assert_eq!(roundtrip(&resp), resp);
    }

    #[test]
    fn fetch_blob_request_roundtrips_rdma_buffer_and_requester() {
        let req = FetchBlobRequest {
            cache_key: "bucket/key".into(),
            range_start: None,
            range_end: None,
            suffix_len: None,
            no_fill: false,
            rdma_buffer: Some(RdmaBuffer {
                addr: 0xdead_beef,
                rkey: 42,
                len: 64 << 20,
                rail: 0,
            }),
            requester_node_id: Some("node-a".into()),
            // The two destinations are mutually exclusive: a request naming its own buffer
            // never names the reading client's.
            client_token: None,
        };
        assert_eq!(roundtrip(&req), req);
    }

    /// ADR-0030's remote half on the wire: a client token round-trips with every rail it
    /// names, and a 16-byte GID survives as bytes rather than as anything prost might
    /// normalise. The GID is an ADDRESS — a byte lost here writes someone else's memory —
    /// so it gets the same explicit round-trip the announce codec's golden vector gets.
    #[test]
    fn fetch_blob_request_roundtrips_a_client_token() {
        let req = FetchBlobRequest {
            cache_key: "bucket/key#16777216:3".into(),
            range_start: None,
            range_end: None,
            suffix_len: None,
            no_fill: false,
            rdma_buffer: None,
            requester_node_id: Some("node-a".into()),
            client_token: Some(ClientToken {
                addr: 0x7f45_f400_0000,
                len: 16 << 20,
                rails: vec![
                    ClientTokenRail {
                        gid: (0u8..16).collect(),
                        // 0 is a legitimate EFA queue-pair number, so it must survive as a
                        // value rather than read as "unset".
                        qpn: 0,
                        rkey: 1_048_576,
                    },
                    ClientTokenRail {
                        gid: vec![0xfe; 16],
                        qpn: 16_385,
                        rkey: 1_048_577,
                    },
                ],
                checksum: true,
            }),
        };
        let got = roundtrip(&req);
        assert_eq!(got, req);
        let token = got.client_token.expect("the token survives the wire");
        assert_eq!(
            token.rails.len(),
            2,
            "every rail travels, not just the first"
        );
        assert_eq!(token.rails[0].gid.len(), 16);
        assert_eq!(token.rails[0].gid[15], 15, "GID byte order is preserved");
        assert_eq!(token.rails[0].qpn, 0);
    }

    /// And the holder's answer: a CRC32 of what it wrote, which is the only integrity signal
    /// a client-registered delivery has (ADR-0030 point 7). Absent means "nothing to attest",
    /// and `0` is a legitimate CRC32 — so the two must not be conflated on the wire.
    #[test]
    fn blob_meta_roundtrips_a_written_crc32() {
        let meta = BlobMeta {
            total_len: 16 << 20,
            e_tag: None,
            object_len: 16 << 20,
            body_start: 0,
            content_type: None,
            last_modified_epoch_secs: None,
            served_via_rdma: true,
            written_crc32: Some(0),
        };
        let got = roundtrip(&meta);
        assert_eq!(got, meta);
        assert_eq!(
            got.written_crc32,
            Some(0),
            "a zero CRC32 must not decode as absent"
        );
        let none = BlobMeta {
            written_crc32: None,
            ..meta
        };
        assert_eq!(roundtrip(&none).written_crc32, None);
    }

    #[test]
    fn fetch_blob_request_without_rdma_fields_roundtrips_as_none() {
        // The gRPC-only path (GrpcTransport::fetch_blob) never sets these —
        // confirm the absence itself is what round-trips, matching the
        // handshake test's reasoning above.
        let req = FetchBlobRequest {
            cache_key: "bucket/key".into(),
            range_start: Some(10),
            range_end: Some(20),
            suffix_len: None,
            no_fill: true,
            rdma_buffer: None,
            requester_node_id: None,
            client_token: None,
        };
        let got = roundtrip(&req);
        assert_eq!(got.rdma_buffer, None);
        assert_eq!(got.requester_node_id, None);
        assert_eq!(got.client_token, None);
        assert_eq!(got, req);
    }
}

/// Guards the field-number evolution rule stated at the top of
/// `proto/pacer/v1/peer.proto`, by parsing that file's own text — no protoc
/// descriptor needed, so this runs with no extra build step or dependency and
/// fails at `cargo test` time rather than only when protoc happens to be run
/// with the right flags.
///
/// Deliberately a plain-text scanner rather than a real `.proto` parser: it
/// assumes no message nests another `message` or `enum` inside its body
/// (true of every message here today), because a nested type has its own,
/// separate field-number namespace that a flat brace-depth scan would
/// wrongly fold into the enclosing message's. If that ever changes, this
/// module needs to track nesting depth per type, not just per message.
#[cfg(test)]
mod field_number_hygiene {
    use std::collections::HashMap;

    /// One `message` block's field numbers (name, number) and any numbers its
    /// own `reserved` statements have retired.
    struct ParsedMessage {
        name: String,
        fields: Vec<(String, u64)>,
        reserved: Vec<u64>,
    }

    /// Drop everything from `//` to end of line, on every line. Good enough
    /// here because nothing in this file puts `//` inside a string literal —
    /// the only quoted strings are `reserved "name";` entries, none of which
    /// contain a comment marker.
    fn strip_line_comments(src: &str) -> String {
        src.lines()
            .map(|line| line.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Split `body` (the text between one `message`'s braces) into field
    /// `(name, number)` pairs. A field line is, after stripping the trailing
    /// `;` and any `[...]` field-options block, `[repeated|optional]? TYPE
    /// NAME = NUMBER` — whatever TYPE looks like, the name and number are
    /// always the two tokens straddling the final `=`.
    fn extract_fields(body: &str) -> Vec<(String, u64)> {
        let mut fields = Vec::new();
        for raw_line in body.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with("reserved") || !line.ends_with(';') {
                continue;
            }
            let line = &line[..line.len() - 1];
            let line = match line.rfind('[') {
                Some(idx) => line[..idx].trim_end(),
                None => line,
            };
            let tokens: Vec<&str> = line.split_whitespace().collect();
            let Some(number_tok) = tokens.last() else {
                continue;
            };
            if tokens.len() < 4 || tokens[tokens.len() - 2] != "=" {
                continue;
            }
            let Ok(number) = number_tok.parse::<u64>() else {
                continue;
            };
            fields.push((tokens[tokens.len() - 3].to_owned(), number));
        }
        fields
    }

    /// Collect the numeric entries of every `reserved ...;` statement in
    /// `body` (quoted name entries and `N to M` ranges both supported; a
    /// name-only reservation contributes nothing here, which is correct —
    /// this function answers "which numbers are retired", not "which names").
    fn extract_reserved_numbers(body: &str) -> Vec<u64> {
        let mut numbers = Vec::new();
        for raw_line in body.lines() {
            let line = raw_line.trim();
            let Some(rest) = line.strip_prefix("reserved ") else {
                continue;
            };
            let Some(rest) = rest.strip_suffix(';') else {
                continue;
            };
            for item in rest.split(',') {
                numbers.extend(parse_reserved_item(item.trim()));
            }
        }
        numbers
    }

    /// Parse one comma-separated entry of a `reserved ...;` statement: a bare
    /// number, a `"name"` (retires no number, so this returns empty — see
    /// [`extract_reserved_numbers`]'s doc), or an `N to M` inclusive range.
    fn parse_reserved_item(item: &str) -> Vec<u64> {
        if item.starts_with('"') {
            return Vec::new();
        }
        if let Some((lo, hi)) = item.split_once(" to ") {
            return match (lo.trim().parse::<u64>(), hi.trim().parse::<u64>()) {
                (Ok(lo), Ok(hi)) => (lo..=hi).collect(),
                _ => Vec::new(),
            };
        }
        item.parse::<u64>().map(|n| vec![n]).unwrap_or_default()
    }

    /// Byte offset, within `s`, of the `}` that closes the brace already
    /// opened at `s[..open_body]` (i.e. `open_body` is the position right
    /// after that opening `{`, with its depth of 1 not yet reflected in `s`).
    /// `s.len()` if `s` has no matching close — an unterminated block, which
    /// the caller treats as "stop scanning" rather than panicking.
    fn find_matching_brace(s: &str, open_body: usize) -> usize {
        let mut depth = 1i32;
        for (i, c) in s[open_body..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => depth -= 1,
                _ => continue,
            }
            if depth == 0 {
                return open_body + i;
            }
        }
        s.len()
    }

    /// Find every top-level `message NAME { ... }` block in `src` (after
    /// comment-stripping) and parse its fields and reservations.
    fn parse_messages(src: &str) -> Vec<ParsedMessage> {
        let clean = strip_line_comments(src);
        let mut messages = Vec::new();
        let mut rest = clean.as_str();
        while let Some(kw) = rest.find("message ") {
            let after_kw = &rest[kw + "message ".len()..];
            let Some(brace) = after_kw.find('{') else {
                break;
            };
            let name = after_kw[..brace].trim().to_owned();
            let body_start = kw + "message ".len() + brace + 1;
            let body_end = find_matching_brace(rest, body_start);
            let body = &rest[body_start..body_end];
            messages.push(ParsedMessage {
                name,
                fields: extract_fields(body),
                reserved: extract_reserved_numbers(body),
            });
            // `+1` skips past the closing brace itself; clamped to `rest.len()`
            // for the (here, never-hit-on-a-valid-.proto) unterminated case,
            // where `find_matching_brace` returned `rest.len()` rather than
            // panicking on an out-of-bounds slice start.
            rest = &rest[(body_end + 1).min(rest.len())..];
        }
        messages
    }

    /// The scanned proto text, fixed at compile time so a scanner bug and a
    /// proto edit cannot silently drift apart between runs.
    const PEER_PROTO: &str = include_str!("../proto/pacer/v1/peer.proto");

    #[test]
    fn scanner_finds_every_message_with_the_right_field_counts() {
        // Not a schema gate — a guard against the scanner itself silently
        // finding zero messages (which would make the collision test below
        // vacuously pass). Update the counts here if a message's own field
        // list changes; that is a real, visible diff, not a silent no-op.
        let messages = parse_messages(PEER_PROTO);
        assert_eq!(
            messages.len(),
            24,
            "message count changed — update this alongside the .proto edit, \
             or the scanner stopped finding messages it used to"
        );
        let sharer = messages
            .iter()
            .find(|m| m.name == "Sharer")
            .expect("Sharer must be found");
        assert_eq!(
            sharer.fields,
            vec![
                ("node".to_owned(), 1),
                ("tier".to_owned(), 2),
                ("generation".to_owned(), 3),
            ]
        );
        let blob_meta = messages
            .iter()
            .find(|m| m.name == "BlobMeta")
            .expect("BlobMeta must be found");
        assert_eq!(blob_meta.fields.len(), 8);
    }

    #[test]
    fn no_message_has_two_fields_sharing_a_number() {
        for message in parse_messages(PEER_PROTO) {
            let mut by_number: HashMap<u64, &str> = HashMap::new();
            for (name, number) in &message.fields {
                if let Some(prev) = by_number.insert(*number, name) {
                    panic!(
                        "{}: field number {number} is used by both `{prev}` and `{name}` — \
                         a field number must be assigned exactly once, forever",
                        message.name
                    );
                }
            }
        }
    }

    #[test]
    fn no_reserved_number_is_also_a_live_field() {
        for message in parse_messages(PEER_PROTO) {
            let live: HashMap<u64, &str> = message
                .fields
                .iter()
                .map(|(name, number)| (*number, name.as_str()))
                .collect();
            for reserved in &message.reserved {
                if let Some(name) = live.get(reserved) {
                    panic!(
                        "{}: field number {reserved} is both `reserved` and still live as `{name}` \
                         — a removed field's number must never be reused",
                        message.name
                    );
                }
            }
        }
    }
}
