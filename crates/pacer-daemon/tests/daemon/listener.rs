//! The S3 listener's bounds and its drain, over real TCP loopback (ADR-0036).
//!
//! Most of the suite plugs the `S3Service` straight into the SDK's HTTP client
//! (`s3s_aws::Client::from(service)`), which never opens a socket — so it cannot
//! see a connection cap, a socket deadline or a graceful shutdown. This file
//! takes the *same* [`common::DaemonCore`] every other arm builds and chooses the
//! other shape, [`common::DaemonCore::served`], which hands a bound loopback port
//! to [`pacer_daemon::listen`]. Every assertion here is about the socket layer the
//! in-process shape deliberately skips.
//!
//! client ⇢ TCP 127.0.0.1:0 ⇢ listen::serve_s3_on
//!            → S3Service[auth = PlaceholderAuth, s3 = PacerProxy]
//!              → aws-sdk-s3 (in-process) → s3s-fs

use std::time::Duration;

use bytes::Bytes;
use pacer_daemon::listen::ListenLimits;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::common::{self, daemon_core, CacheSpec, Daemon, DaemonSpec, BUCKET, PATIENCE, SETTLE};

/// Below every object these tests write, so nothing is bypassed as "too small to
/// cache" — the point is to exercise the socket, not the admission policy.
const MIN_OBJECT_SIZE: u64 = 4 << 10;
/// Small enough that the slow-GET test's object spans many chunks, so the
/// response body is genuinely streamed rather than produced in one piece.
const CHUNK_SIZE: u64 = 1 << 20;

/// Short deadlines for the timeout tests. Real durations, not a paused clock:
/// these assertions run against a real `TcpStream`, and pausing time while the OS
/// keeps running is how a socket test becomes non-deterministic.
const SHORT_TIMEOUT: Duration = Duration::from_millis(300);
/// A deadline long enough to be provably NOT the one under test.
const LONG_TIMEOUT: Duration = Duration::from_secs(300);

/// Idle deadline for the slow-consumer arm. Larger than [`SHORT_TIMEOUT`] because
/// the assertion needs a *ratio* between it and one consumer pause wide enough to
/// absorb the SDK's client-side read-ahead — see that test.
///
/// **Why seconds and not one second.** This is a test-only deadline, not the daemon's
/// shipped default, and its only job is to be a deadline an active transfer must not
/// reach. It was 1 s, and at that value the arm did not measure the daemon — it
/// measured the machine: the assertion is about REAL time by necessity (a real
/// `TcpStream`; see the note on [`SHORT_TIMEOUT`]), so on a 16-vCPU build pod sharing
/// itself with ~400 sibling test processes any >1 s scheduling or I/O stall inside the
/// paced run is indistinguishable from the regression this exists to catch. Measured
/// 2026-09-08: 6 failures in 8 runs at full nextest parallelism. 5 s does not weaken
/// the arm — the regression it catches (a watchdog that bounds the *request* instead of
/// the gaps within it) cuts at this deadline while [`PACED_RUN`] keeps the run twice as
/// long, which is the same proof it always was — and a >5 s stall on a machine that is
/// merely oversubscribed does not happen. The alternative, reserving the whole
/// test-thread pool via `threads-required` in `.config/nextest.toml`, serialised the
/// run for ~14 s of a ~31 s pass and made this one test's timing a property of the
/// scheduler; it was removed when these two constants were raised.
const IDLE_UNDER_LOAD: Duration = Duration::from_secs(5);
/// Total pause the slow consumer spreads over the whole body, in proportion to
/// each piece's size. Because the pieces sum to the body, the run lasts at least
/// this long **by construction**, whatever the hardware and however the SDK slices
/// the stream — the first version paced per piece with a fixed 5 ms and relied on
/// the transfer itself being slow, and a Graviton release build finished 32 MiB in
/// 934 ms, under the then-1 s deadline (pipeline 2828836881). Twice the deadline, so
/// the assertion has margin; the relation is checked below.
const PACED_RUN: Duration = Duration::from_secs(10);
const _: () = assert!(
    PACED_RUN.as_millis() >= 2 * IDLE_UNDER_LOAD.as_millis(),
    "the paced run must outlast the idle deadline with margin, or the arm proves nothing"
);
/// Chunks the slow-consumer object spans. Also bounds one pause: a piece is never
/// larger than one chunk (hyper's read buffer is far smaller), so a single pause is
/// at most `PACED_RUN / STREAMED_CHUNKS` = 312.5 ms, a sixteenth of the deadline —
/// the SDK would have to serve 16 MiB out of read-ahead without touching the
/// socket to accumulate a false timeout. That sixteenth is fixed by the 2× relation
/// asserted above and this count, so raising both durations together (which is what
/// made this arm robust under load) moved the absolute pause and not the ratio.
const STREAMED_CHUNKS: u64 = 32;
/// Chunks the drain arm's object spans — enough that its body is genuinely still
/// streaming when the shutdown flag goes up.
const IN_FLIGHT_CHUNKS: u64 = 8;

/// Bring up the daemon stack and serve it on a loopback port under `limits`.
///
/// A cache smaller than the suite default: this file's objects are up to
/// [`STREAMED_CHUNKS`] chunks and none of its assertions is about residency, so a
/// tighter tier keeps the arm cheap without changing what it proves.
async fn server(limits: ListenLimits) -> Daemon {
    daemon_core(DaemonSpec {
        min_object_size: MIN_OBJECT_SIZE,
        max_object_size: None,
        chunk_size: CHUNK_SIZE,
        cache: CacheSpec {
            mem_capacity: 64 << 20,
            disk_capacity: 256 << 20,
            ..CacheSpec::default()
        },
        ..DaemonSpec::default()
    })
    .await
    .served(limits)
    .await
}

/// A minimal HTTP/1.1 request. Deliberately unsigned: the reply (a 403 from
/// [`PlaceholderAuth`]) is as good as a 200 for proving the connection was
/// *accepted and served*, which is all the cap tests ask.
const PROBE_REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: pacer.local\r\n\r\n";

/// A request head that is never terminated — the slow-header client the header
/// deadline exists for.
const PARTIAL_HEAD: &[u8] = b"GET / HTTP/1.1\r\nHost: pacer.local\r\n";

/// Send [`PROBE_REQUEST`] and wait for the first byte of a reply, up to
/// [`PATIENCE`]. `false` means the daemon never served this connection.
async fn probe(stream: &mut TcpStream) -> bool {
    stream.write_all(PROBE_REQUEST).await.unwrap();
    let mut byte = [0u8; 1];
    matches!(
        tokio::time::timeout(PATIENCE, stream.read(&mut byte)).await,
        Ok(Ok(1))
    )
}

/// Whether a reply arrives within [`SETTLE`] — the negative form of [`probe`].
async fn probe_stays_silent(stream: &mut TcpStream) -> bool {
    stream.write_all(PROBE_REQUEST).await.unwrap();
    let mut byte = [0u8; 1];
    tokio::time::timeout(SETTLE, stream.read(&mut byte))
        .await
        .is_err()
}

/// Wait for the server to close `stream`, up to [`PATIENCE`].
///
/// A clean close reads as EOF (`Ok(0)`); an RST reads as an error. Both mean the
/// daemon dropped the connection, which is what a timeout is supposed to do.
async fn closed_by_server(stream: &mut TcpStream) -> bool {
    let mut buf = [0u8; 64];
    loop {
        match tokio::time::timeout(PATIENCE, stream.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) => return true,
            // A response the server sent before closing; keep reading.
            Ok(Ok(_)) => {}
            Err(_) => return false,
        }
    }
}

/// R1: the connection cap is a real ceiling, and it lifts when a slot frees.
///
/// Asserted through the socket rather than the gauge, because the gauge would
/// also read 2 if the third connection had been accepted and then stalled
/// somewhere else.
#[tokio::test]
async fn connection_cap_admits_exactly_max_connections() {
    let mut server = server(ListenLimits {
        max_connections: 2,
        header_timeout: LONG_TIMEOUT,
        idle_timeout: LONG_TIMEOUT,
    })
    .await;

    let mut first = TcpStream::connect(server.addr()).await.unwrap();
    let mut second = TcpStream::connect(server.addr()).await.unwrap();
    assert!(probe(&mut first).await, "first connection must be served");
    assert!(probe(&mut second).await, "second connection must be served");
    // Keep-alive: both replies are complete, but neither socket is closed, so
    // both permits are still held.
    assert_eq!(server.metrics.listener.connections_active.get(), 2);

    // The kernel accepts the third socket into the backlog — the cap is in the
    // daemon, so `connect` succeeding proves nothing on its own.
    let mut third = TcpStream::connect(server.addr()).await.unwrap();
    assert!(
        probe_stays_silent(&mut third).await,
        "third connection must not be served while the cap is held"
    );
    assert!(
        server.metrics.listener.connections_at_capacity.get() >= 1,
        "reaching the cap must be observable in /metrics"
    );

    // Free one permit; the third connection becomes servable.
    drop(first);
    let mut byte = [0u8; 1];
    let served = tokio::time::timeout(PATIENCE, third.read(&mut byte)).await;
    assert!(
        matches!(served, Ok(Ok(1))),
        "third connection must be served once a slot frees, got {served:?}"
    );

    drop(second);
    drop(third);
    server.shutdown_quietly().await;
}

/// R1: a connection that never makes progress is closed by the idle deadline.
///
/// The header deadline is pinned long, so a close here can only come from the
/// socket-level watchdog.
#[tokio::test]
async fn idle_connection_is_closed() {
    let mut server = server(ListenLimits {
        max_connections: 8,
        header_timeout: LONG_TIMEOUT,
        idle_timeout: SHORT_TIMEOUT,
    })
    .await;

    let mut idle = TcpStream::connect(server.addr()).await.unwrap();
    assert!(
        closed_by_server(&mut idle).await,
        "an idle connection must be closed by PACER_S3_IDLE_TIMEOUT"
    );

    server.shutdown_quietly().await;
}

/// R1: a request head that never terminates is closed by the header deadline.
///
/// The mirror of the test above — idle pinned long — so the two knobs are proven
/// to be wired separately rather than one of them covering for the other.
#[tokio::test]
async fn incomplete_request_head_is_closed() {
    let mut server = server(ListenLimits {
        max_connections: 8,
        header_timeout: SHORT_TIMEOUT,
        idle_timeout: LONG_TIMEOUT,
    })
    .await;

    let mut slow = TcpStream::connect(server.addr()).await.unwrap();
    slow.write_all(PARTIAL_HEAD).await.unwrap();
    assert!(
        closed_by_server(&mut slow).await,
        "a partial request head must be closed by PACER_S3_HEADER_TIMEOUT"
    );

    server.shutdown_quietly().await;
}

/// R1: an active transfer must never be cut by the idle deadline, even when it
/// runs for several times the deadline's length.
///
/// The regression the socket watchdog is most likely to introduce — a naive
/// implementation bounds the *request*, not the gaps within it — so it gets its
/// own arm. The client is deliberately slow: after every piece it pauses for that
/// piece's share of [`PACED_RUN`], so the whole run lasts at least [`PACED_RUN`]
/// (twice [`IDLE_UNDER_LOAD`]) regardless of how fast the bytes themselves move
/// or how the SDK slices them. The precondition is still asserted rather than
/// assumed, because a test that cannot fail its own premise proves nothing.
///
/// The two durations are a *ratio*, not two independent knobs. One pause has to
/// stay well under the deadline even after the SDK serves a run of pieces out of
/// its own read-ahead buffer without touching the socket — which is what makes a
/// coarse pause (half the deadline) fail here for a reason that has nothing to do
/// with the daemon. Sizing the pause per byte rather than per piece keeps that
/// bound (see [`STREAMED_CHUNKS`]) while making the total independent of the
/// piece count, which is what the fixed per-piece gap got wrong.
///
/// The paced GET reads a WARM object. The watchdog counts socket bytes only, so
/// the daemon's time to first byte — a cold 32 MiB object is 32 backend fetches
/// before the first chunk is framed — is "idle" from its point of view, and on a
/// debug build that alone exceeded the then-1 s deadline and cut the connection
/// before a single piece arrived, which is a property of the fill path and not the
/// thing this arm tests. One unpaced GET first puts every chunk in the memory tier
/// (sized to hold the whole object), so the paced run measures the socket layer.
#[tokio::test]
async fn active_transfer_survives_a_short_idle_deadline() {
    let mut server = server(ListenLimits {
        max_connections: 8,
        header_timeout: LONG_TIMEOUT,
        idle_timeout: IDLE_UNDER_LOAD,
    })
    .await;
    let body = server.seed_chunks("streamed", STREAMED_CHUNKS).await;
    let client = server.client.clone();

    let warm = client
        .get_object()
        .bucket(BUCKET)
        .key("streamed")
        .send()
        .await
        .expect("warm-up GET must succeed")
        .body
        .collect()
        .await
        .expect("warm-up body must arrive")
        .into_bytes();
    assert_eq!(warm, body, "warm-up must return the object unchanged");

    let response = client
        .get_object()
        .bucket(BUCKET)
        .key("streamed")
        .send()
        .await
        .expect("GET must succeed");
    let mut stream = response.body;
    let mut received = Vec::with_capacity(body.len());
    let started = tokio::time::Instant::now();
    #[allow(clippy::cast_precision_loss)] // body lengths here are far below 2^52
    let body_len = body.len() as f64;
    while let Some(piece) = stream.try_next().await.expect("stream must not break") {
        received.extend_from_slice(&piece);
        // This piece's share of the paced run; the shares sum to PACED_RUN.
        #[allow(clippy::cast_precision_loss)]
        let share = PACED_RUN.mul_f64(piece.len() as f64 / body_len);
        tokio::time::sleep(share).await;
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed > IDLE_UNDER_LOAD,
        "the transfer must outlast the idle deadline for this to prove anything \
         (took {elapsed:?}, deadline {IDLE_UNDER_LOAD:?}, paced run {PACED_RUN:?})"
    );
    assert_eq!(received.len(), body.len(), "whole object must arrive");
    assert_eq!(Bytes::from(received), body, "bytes must be unchanged");

    server.shutdown_quietly().await;
}

/// R5: shutdown lets an in-flight GET finish and stops accepting new work.
///
/// Triggered through [`Shutdown::trigger`], which is the same code path
/// `Shutdown::on_signal` takes after SIGTERM — the signal itself is
/// process-global and cannot be raised inside one test of a shared binary.
#[tokio::test]
async fn shutdown_drains_in_flight_and_refuses_new_connections() {
    let mut server = server(ListenLimits::default()).await;
    let body = server.seed_chunks("in-flight", IN_FLIGHT_CHUNKS).await;
    let client = server.client.clone();

    let response = client
        .get_object()
        .bucket(BUCKET)
        .key("in-flight")
        .send()
        .await
        .expect("GET must succeed");
    let mut stream = response.body;
    let first = stream
        .try_next()
        .await
        .expect("first piece must arrive")
        .expect("body must not be empty");
    assert!(!first.is_empty());

    // The request is now genuinely mid-body.
    server.trigger_shutdown();

    let mut received = first.to_vec();
    while let Some(piece) = stream
        .try_next()
        .await
        .expect("a draining server must not break an in-flight body")
    {
        received.extend_from_slice(&piece);
    }
    assert_eq!(
        Bytes::from(received),
        body,
        "the in-flight GET must complete with every byte"
    );

    // And the door is shut. The listener is dropped when the accept loop breaks,
    // which the kernel then answers with ECONNREFUSED; the loop observes the flag
    // asynchronously, so this is polled rather than asserted once.
    common::wait_until_connection_refused(server.addr()).await;

    server.shutdown_and_drain(PATIENCE).await;
}
