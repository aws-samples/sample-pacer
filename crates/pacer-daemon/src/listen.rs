//! The S3 front end's accept loop, and the three bounds it puts on request
//! volume (ADR-0036).
//!
//! The daemon's internal channels are all bounded — peer chunk streams, the fill
//! pipeline, the scatter's staging budget and window slots — so the only place
//! an unbounded amount of work could enter was here: an accept loop that spawned
//! one task per connection with no cap, no timeout and no drain. A node's memory
//! then scales with whatever a client opens, and a rolling update kills in-flight
//! requests at the kernel level.
//!
//! Three bounds, in the order a connection meets them:
//!
//! 1. **A connection permit** ([`ListenLimits::max_connections`]). The permit is
//!    taken *before* `accept`, so a node at its cap stops draining the kernel
//!    backlog instead of accepting a socket it will not serve. Backpressure at
//!    the accept queue is the only kind a client's TCP stack understands without
//!    being taught a new error.
//! 2. **A header deadline** ([`ListenLimits::header_timeout`]), which hyper arms
//!    every time the connection starts reading a request head — including while a
//!    keep-alive connection sits between requests, so it doubles as the
//!    keep-alive reaper.
//! 3. **An idle deadline** ([`ListenLimits::idle_timeout`]) at the socket, for
//!    the states the header deadline cannot see: a request *body* that stalls
//!    mid-upload, a response nobody reads, and an HTTP/2 connection with no open
//!    stream.
//!
//! Then [`serve_s3`] stops accepting on the shutdown signal and lets watched
//! connections finish (`hyper_util`'s [`GracefulShutdown`]); the caller bounds
//! that wait — see `main`'s drain.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, Sleep};
use tracing::{info, warn};

use crate::metrics::Metrics;
use crate::shutdown::ShutdownSignal;

/// Concurrent S3 connections one daemon accepts, default for
/// [`ListenLimits::max_connections`].
///
/// Derived from what a full training node actually opens, not from a round
/// number. The Python client keeps one HTTP/1.1 connection per in-flight stream
/// and sizes its botocore pool accordingly: `POOL_CONNECTIONS = 64` per rank
/// (`clients/python/pacer_vllm.py`), and a p5/p6 node runs one rank per GPU, so
/// eight ranks reach **512** sockets against the node-local daemon at full tilt.
/// vLLM's `EngineCore` is a separate process per rank with its own pool, and a
/// checkpoint save adds the writer's own upload pool on top, so the ceiling has
/// to sit above that measured 512 rather than at it — 1024 is one doubling, which
/// is the smallest headroom that still admits a second concurrent job.
///
/// The cost of the cap being *too high* is the one being bounded: every accepted
/// connection may hold a chunk-sized buffer on the fill path, so the memory this
/// number multiplies is megabytes, not kilobytes. The cost of it being too low is
/// added latency on a client's connect, never an error — see [`serve_s3`].
const DEFAULT_MAX_CONNECTIONS: usize = 1024;

/// Seconds a connection may sit without a complete request head, default for
/// [`ListenLimits::header_timeout`].
///
/// hyper arms this timer whenever the connection begins reading a head, so on a
/// keep-alive connection it is the *idle* bound too — which is why it is minutes
/// rather than seconds. A torch loader pauses between batches while it copies the
/// last one into HBM, and closing its socket in that gap trades a slow-header
/// defence for reconnect churn against the node's ephemeral-port range (each
/// reconnect burns one for `tcp_fin_timeout`). 120 s is comfortably longer than
/// any inter-batch gap measured on the ladder and still bounds a client that
/// opened a socket and sent nothing.
const DEFAULT_HEADER_TIMEOUT_SECS: u64 = 120;

/// Seconds a connection may make no progress in either direction, default for
/// [`ListenLimits::idle_timeout`].
///
/// This one is about a *stalled transfer*, not an idle client: the deadline
/// resets on every byte read or written, so a slow-but-moving GET of a 16 GiB
/// shard never trips it however long it takes. 300 s of a socket that is neither
/// readable nor writable, on loopback or a same-AZ VPC path, means the peer is
/// gone — and the request it is holding open is holding a fill and its buffers
/// with it. Generous on purpose: this is a leak reaper, and the cost of firing it
/// early is a failed checkpoint read.
const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 300;

/// How long the accept loop pauses after a transient `accept` failure.
///
/// `accept` fails without the listener being broken — `EMFILE`/`ENFILE` when the
/// process is out of descriptors, `ENOBUFS`/`ENOMEM` under memory pressure,
/// `ECONNABORTED` when the peer went away between SYN and accept. Retrying
/// immediately spins a core in a tight loop for as long as the condition lasts;
/// 100 ms is long enough that the loop costs nothing and short enough that a
/// descriptor freed by a closing connection is picked up within one client's
/// connect timeout.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Consecutive `accept` failures tolerated before the loop gives up and returns
/// an error (which restarts the pod).
///
/// The backoff above is only correct for a *transient* condition. A listener that
/// has become permanently unusable fails every time, and retrying forever would
/// leave a pod that passes its liveness probe — the admin listener is a separate
/// socket — while serving nothing. 64 failures at [`ACCEPT_BACKOFF`] is ~6.4 s of
/// patience, far more than any descriptor-exhaustion blip observed, before the
/// daemon reports the truth by dying.
const MAX_CONSECUTIVE_ACCEPT_ERRORS: u32 = 64;

/// The bounds [`serve_s3`] applies to the S3 listener (ADR-0036).
///
/// Resolved from config (`PACER_S3_MAX_CONNECTIONS`, `PACER_S3_HEADER_TIMEOUT`,
/// `PACER_S3_IDLE_TIMEOUT`) so a benchmark arm can move one without touching the
/// others; [`ListenLimits::default`] is the production setting.
#[derive(Debug, Clone, Copy)]
pub struct ListenLimits {
    /// Concurrent connections the S3 listener will hold open. Reaching it stops
    /// the accept loop, not the served connections.
    pub max_connections: usize,
    /// How long a connection may go without delivering a complete request head.
    /// Also the keep-alive idle bound (see the module header).
    pub header_timeout: Duration,
    /// How long a connection may go with no byte moving in either direction.
    pub idle_timeout: Duration,
}

impl Default for ListenLimits {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            header_timeout: Duration::from_secs(DEFAULT_HEADER_TIMEOUT_SECS),
            idle_timeout: Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS),
        }
    }
}

impl ListenLimits {
    /// The default connection cap, for config resolution's "0 means default".
    #[must_use]
    pub const fn default_max_connections() -> usize {
        DEFAULT_MAX_CONNECTIONS
    }

    /// The default header/keep-alive deadline in seconds, for config resolution.
    #[must_use]
    pub const fn default_header_timeout_secs() -> u64 {
        DEFAULT_HEADER_TIMEOUT_SECS
    }

    /// The default no-progress deadline in seconds, for config resolution.
    #[must_use]
    pub const fn default_idle_timeout_secs() -> u64 {
        DEFAULT_IDLE_TIMEOUT_SECS
    }
}

/// Serve the S3 API on `addr` until `shutdown` is raised, then stop accepting
/// and let watched connections finish.
///
/// Returns once every connection this listener accepted has completed. The
/// caller decides how long that may take — see `main`'s drain deadline, which is
/// paired with the chart's `terminationGracePeriodSeconds`.
///
/// # Errors
///
/// Binding `addr`, or `MAX_CONSECUTIVE_ACCEPT_ERRORS` consecutive `accept`
/// failures (a listener that is not merely under pressure but unusable — the pod
/// should restart rather than pass its liveness probe while serving nothing).
/// Per-connection errors are logged and absorbed.
pub async fn serve_s3(
    addr: String,
    service: s3s::service::S3Service,
    limits: ListenLimits,
    metrics: Metrics,
    shutdown: ShutdownSignal,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&addr).await?;
    serve_s3_on(listener, service, limits, metrics, shutdown).await
}

/// [`serve_s3`] on a listener the caller already bound.
///
/// Exists for the integration suite (`tests/daemon/listener.rs`), which needs the port
/// *before* the server starts: binding `127.0.0.1:0` here and reporting the port
/// afterwards would leave the test guessing, and guessing a free port is how a
/// suite becomes flaky on a busy CI runner.
///
/// # Errors
///
/// As [`serve_s3`], minus the bind.
pub async fn serve_s3_on(
    listener: TcpListener,
    service: s3s::service::S3Service,
    limits: ListenLimits,
    metrics: Metrics,
    shutdown: ShutdownSignal,
) -> anyhow::Result<()> {
    let addr = listener.local_addr()?.to_string();
    let permits = Arc::new(Semaphore::new(limits.max_connections));
    let graceful = GracefulShutdown::new();
    let builder = connection_builder(&limits);
    info!(
        %addr,
        max_connections = limits.max_connections,
        header_timeout_secs = limits.header_timeout.as_secs(),
        idle_timeout_secs = limits.idle_timeout.as_secs(),
        "s3 endpoint listening"
    );
    let mut consecutive_errors = 0u32;
    loop {
        let Some(permit) = acquire_permit(&permits, &metrics, &shutdown).await else {
            break;
        };
        match accept(&listener, &shutdown).await {
            Accepted::Stream(stream) => {
                consecutive_errors = 0;
                let watcher = graceful.watcher();
                let conn = builder
                    .serve_connection(
                        TokioIo::new(IdleTimeout::new(stream, limits.idle_timeout)),
                        service.clone(),
                    )
                    .into_owned();
                metrics.listener.connections_active.inc();
                let metrics = metrics.clone();
                tokio::spawn(async move {
                    if let Err(e) = watcher.watch(conn).await {
                        warn!(error = %e, "s3 connection error");
                    }
                    metrics.listener.connections_active.dec();
                    // Explicit, and last: the permit is what admits the NEXT
                    // connection, so it must outlive the served one.
                    drop(permit);
                });
            }
            Accepted::Transient(e) => {
                metrics.listener.accept_errors.inc();
                consecutive_errors += 1;
                if consecutive_errors >= MAX_CONSECUTIVE_ACCEPT_ERRORS {
                    return Err(anyhow::Error::new(e).context(format!(
                        "{MAX_CONSECUTIVE_ACCEPT_ERRORS} consecutive accept failures on {addr}"
                    )));
                }
                warn!(error = %e, consecutive_errors, "s3 accept failed; backing off");
                tokio::time::sleep(ACCEPT_BACKOFF).await;
            }
            Accepted::ShuttingDown => break,
        }
    }
    let in_flight = graceful.count();
    info!(
        %addr,
        in_flight, "s3 listener stopped accepting; draining connections"
    );
    graceful.shutdown().await;
    info!(%addr, "s3 connections drained");
    Ok(())
}

/// The three things one turn of the accept loop can produce.
enum Accepted {
    /// A connection to serve.
    Stream(TcpStream),
    /// `accept` failed for a reason that does not implicate the listener.
    Transient(io::Error),
    /// The shutdown flag went up while waiting.
    ShuttingDown,
}

/// Take a connection permit, or `None` if shutdown is raised while waiting.
///
/// Split out so the "at capacity" case has a name and a metric: the first
/// `try_acquire` failing is the *only* observable moment a node is at its
/// connection ceiling, because everything after it is indistinguishable from a
/// client that is simply slow to connect.
async fn acquire_permit(
    permits: &Arc<Semaphore>,
    metrics: &Metrics,
    shutdown: &ShutdownSignal,
) -> Option<OwnedSemaphorePermit> {
    if let Ok(permit) = Arc::clone(permits).try_acquire_owned() {
        return Some(permit);
    }
    metrics.listener.connections_at_capacity.inc();
    warn!(
        available = permits.available_permits(),
        "s3 connection limit reached; not accepting until one closes \
         (PACER_S3_MAX_CONNECTIONS)"
    );
    tokio::select! {
        biased;
        () = shutdown.wait() => None,
        // Only errors if the semaphore is closed, which nothing here does.
        permit = Arc::clone(permits).acquire_owned() => permit.ok(),
    }
}

/// One `accept`, cancelled by the shutdown flag.
///
/// `biased` so a pod that is already draining does not accept one more
/// connection because the scheduler happened to poll the listener first.
async fn accept(listener: &TcpListener, shutdown: &ShutdownSignal) -> Accepted {
    tokio::select! {
        biased;
        () = shutdown.wait() => Accepted::ShuttingDown,
        result = listener.accept() => match result {
            Ok((stream, _peer)) => Accepted::Stream(stream),
            Err(e) => Accepted::Transient(e),
        },
    }
}

/// The per-connection hyper builder, configured once and cloned per connection.
///
/// [`TokioTimer`] is not optional here: hyper's header deadline is driven by the
/// timer the builder is given, and with none installed `header_read_timeout` is
/// silently inert.
fn connection_builder(
    limits: &ListenLimits,
) -> hyper_util::server::conn::auto::Builder<TokioExecutor> {
    let mut builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(limits.header_timeout);
    builder.http2().timer(TokioTimer::new());
    builder
}

/// A stream that fails once no byte has moved in either direction for
/// `idle` (see [`ListenLimits::idle_timeout`]).
///
/// Wraps the socket rather than the connection future, because the question is
/// about *bytes*, not about requests: a wrapper around the future would have to
/// choose between killing a legitimately long transfer and not noticing a stalled
/// one. Here a 30-minute GET that is still moving keeps resetting its own
/// deadline, and a request whose peer vanished mid-body trips it.
///
/// The hot path costs one [`Instant::now`] per successful poll and *no* timer
/// work: the [`Sleep`] is only re-armed when it actually fires and finds the
/// connection was active in the meantime.
struct IdleTimeout<S> {
    /// The socket.
    inner: S,
    /// The no-progress budget.
    idle: Duration,
    /// When a byte last moved.
    last_activity: Instant,
    /// Fires at (an underestimate of) `last_activity + idle`.
    deadline: Pin<Box<Sleep>>,
}

impl<S> IdleTimeout<S> {
    /// Wrap `inner`, starting the clock now.
    fn new(inner: S, idle: Duration) -> Self {
        let now = Instant::now();
        Self {
            inner,
            idle,
            last_activity: now,
            deadline: Box::pin(tokio::time::sleep_until(now + idle)),
        }
    }

    /// Record that a byte moved.
    fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    /// Decide what a `Pending` inner poll means: still waiting, or idle too long.
    ///
    /// The deadline is deliberately allowed to fire early — it is armed once from
    /// the *previous* activity, so a busy connection's timer expires while it is
    /// working. That firing is where the real check happens, and where the timer
    /// is pushed out again; the alternative (resetting the `Sleep` on every read)
    /// puts a timer-wheel update on the per-byte path for no added safety.
    fn poll_idle<T>(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<T>> {
        if self.deadline.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        let idle_for = self.last_activity.elapsed();
        if idle_for >= self.idle {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection idle past PACER_S3_IDLE_TIMEOUT",
            )));
        }
        let next = self.last_activity + self.idle;
        self.deadline.as_mut().reset(next);
        // Re-poll so the new deadline registers this waker; a reset alone leaves
        // the task with no wakeup and the connection hangs until the peer moves.
        let _ = self.deadline.as_mut().poll(cx);
        Poll::Pending
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for IdleTimeout<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(result) => {
                self.touch();
                Poll::Ready(result)
            }
            Poll::Pending => self.poll_idle(cx),
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for IdleTimeout<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(result) => {
                self.touch();
                Poll::Ready(result)
            }
            Poll::Pending => self.poll_idle(cx),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.inner).poll_flush(cx) {
            Poll::Ready(result) => {
                self.touch();
                Poll::Ready(result)
            }
            Poll::Pending => self.poll_idle(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.inner).poll_shutdown(cx) {
            Poll::Ready(result) => {
                self.touch();
                Poll::Ready(result)
            }
            Poll::Pending => self.poll_idle(cx),
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write_vectored(cx, bufs) {
            Poll::Ready(result) => {
                self.touch();
                Poll::Ready(result)
            }
            Poll::Pending => self.poll_idle(cx),
        }
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A stream that never becomes readable and always accepts writes — the two
    /// halves of "the peer stopped participating".
    struct Stalled;

    impl AsyncRead for Stalled {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for Stalled {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// The knob is documented in seconds; the tests use milliseconds so they run
    /// on the paused clock without waiting on wall time.
    const TEST_IDLE: Duration = Duration::from_millis(50);

    #[tokio::test(start_paused = true)]
    async fn read_times_out_after_idle() {
        let mut io = IdleTimeout::new(Stalled, TEST_IDLE);
        let mut buf = [0u8; 8];
        let err = io.read(&mut buf).await.expect_err("must time out");
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test(start_paused = true)]
    async fn write_times_out_after_idle() {
        let mut io = IdleTimeout::new(Stalled, TEST_IDLE);
        let err = io.write(b"hello").await.expect_err("must time out");
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    /// A connection that keeps moving bytes must never trip the deadline, however
    /// far past `idle` the transfer runs — the regression this wrapper is most
    /// likely to cause.
    #[tokio::test(start_paused = true)]
    async fn activity_defers_the_deadline_indefinitely() {
        let (client, server) = tokio::io::duplex(64);
        let mut io = IdleTimeout::new(server, TEST_IDLE);
        let writer = tokio::spawn(async move {
            let mut client = client;
            for _ in 0..10 {
                tokio::time::sleep(TEST_IDLE / 2).await;
                client.write_all(b"x").await.unwrap();
            }
        });
        let mut buf = [0u8; 1];
        for _ in 0..10 {
            io.read_exact(&mut buf).await.expect("still active");
        }
        writer.await.unwrap();
    }

    #[test]
    fn defaults_are_the_documented_ones() {
        let limits = ListenLimits::default();
        assert_eq!(limits.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(
            limits.header_timeout,
            Duration::from_secs(DEFAULT_HEADER_TIMEOUT_SECS)
        );
        assert_eq!(
            limits.idle_timeout,
            Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS)
        );
    }
}
