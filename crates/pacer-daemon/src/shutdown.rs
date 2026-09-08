//! Cooperative process shutdown (ADR-0036).
//!
//! One broadcast flag every long-lived task watches, plus the signal handler
//! that raises it. The daemon runs as a DaemonSet pod, so the only orderly way
//! out is the SIGTERM the kubelet sends before the grace period expires — and
//! before this module existed there was no handler at all, which meant a
//! rolling update killed in-flight GETs at the kernel level and left the disk
//! tier's flush and reclaim tasks mid-write.
//!
//! The flag is a [`tokio::sync::watch`] channel rather than a signal future
//! passed around, for one reason that matters to tests: [`Shutdown::trigger`]
//! is the *same* code path the signal handler takes, so an integration test can
//! drain the daemon without the process-global side effects of raising a real
//! signal (`tests/daemon/listener.rs`).

use tokio::sync::watch;
use tracing::info;

/// Seconds in-flight work gets to finish after the shutdown flag goes up, default
/// for `PACER_SHUTDOWN_DRAIN_TIMEOUT`.
///
/// **Paired with the chart's `terminationGracePeriodSeconds: 30`, and it must stay
/// strictly below it.** The kubelet sends SIGTERM, waits the grace period, then
/// SIGKILLs; a drain deadline at or above the grace period means the process is
/// killed *during* its own drain, which is the failure mode the drain exists to
/// remove — the pod would still cut in-flight GETs, just later. 20 s leaves 10 s of
/// margin for the tail of the drain itself: the cache's `close` waits on foyer's
/// ongoing flush and reclaim tasks, and that wait is bounded by device latency,
/// not by this value.
///
/// Why 20 s is enough for the *requests*: a chunk fill is bounded by
/// `fill_parallelism × chunk_size` in flight, and the slowest measured
/// per-16-MiB-chunk service time on the ladder is tens of milliseconds. What can
/// legitimately exceed 20 s is a client streaming a multi-GiB object at its own
/// pace; that one is cut, and cutting it is correct — the alternative is a pod that
/// never finishes a rolling update because one slow reader keeps it alive.
pub const DEFAULT_DRAIN_TIMEOUT_SECS: u64 = 20;

/// The sender half: one per process, held by whoever orchestrates the drain.
///
/// Not `Clone` on purpose — two owners would each believe they decide when the
/// process stops, and a drain that can be started twice logs two conflicting
/// begin/end pairs for one shutdown.
#[derive(Debug)]
pub struct Shutdown {
    /// Raised exactly once; `true` means "stop accepting and drain".
    tx: watch::Sender<bool>,
}

/// A watcher handle. Cheap to clone, handed to every task that has to stop
/// accepting new work when the process is going down.
#[derive(Clone, Debug)]
pub struct ShutdownSignal {
    /// Receiver on [`Shutdown::tx`]. Cloned per watcher so each has its own
    /// seen-version cursor.
    rx: watch::Receiver<bool>,
}

impl Shutdown {
    /// Create an un-triggered handle.
    #[must_use]
    pub fn new() -> Self {
        let (tx, _) = watch::channel(false);
        Self { tx }
    }

    /// A watcher for one task. Take as many as there are tasks to stop.
    #[must_use]
    pub fn signal(&self) -> ShutdownSignal {
        ShutdownSignal {
            rx: self.tx.subscribe(),
        }
    }

    /// Raise the flag. Idempotent: a second call changes nothing, because the
    /// value is already `true` and `watch` only notifies on change.
    ///
    /// `send_replace`, not `send`: [`tokio::sync::watch::Sender::send`] **fails
    /// and leaves the value untouched** when no receiver is currently alive, so a
    /// trigger raised before any task took a watcher — or after the last one
    /// finished — would silently not happen, and a watcher taken afterwards would
    /// see an un-triggered flag. The state has to be true whether or not anyone
    /// is listening yet.
    pub fn trigger(&self) {
        self.tx.send_replace(true);
    }

    /// Wait for SIGTERM or SIGINT and raise the flag, then return the signal's
    /// name for the caller's drain log line.
    ///
    /// SIGTERM is what the kubelet sends; SIGINT is what a developer's Ctrl-C
    /// sends to a foreground `pacer-daemon`. Both mean "stop", so both take the
    /// same path — an operator debugging a drain locally must exercise the code
    /// production exercises, not a second one.
    ///
    /// # Panics
    ///
    /// Registering a signal handler failing. That is a process-startup
    /// condition (the handler slot is taken, or the platform refuses), not a
    /// runtime one, and a daemon that silently cannot be drained is worse than
    /// one that refuses to start: a rolling update would then SIGKILL every pod
    /// mid-request and look like a data-plane fault.
    pub async fn on_signal(&self) -> &'static str {
        let name = await_signal().await;
        info!(signal = name, "shutdown signal received");
        self.trigger();
        name
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownSignal {
    /// Resolve as soon as the flag is (or already was) raised.
    ///
    /// Takes `&self` so it can be used inside a `select!` in a loop without
    /// moving the handle. The already-raised check is not an optimization: a
    /// receiver marks the current value as seen when it is cloned, so a watcher
    /// created *after* the trigger would wait forever on `changed()` alone.
    pub async fn wait(&self) {
        let mut rx = self.rx.clone();
        if *rx.borrow_and_update() {
            return;
        }
        // Only errors when every sender is dropped, which for this channel
        // means the process is already unwinding — treat it as "shut down".
        let _ = rx.changed().await;
    }

    /// Whether the flag is already raised, without waiting.
    #[must_use]
    pub fn is_triggered(&self) -> bool {
        *self.rx.borrow()
    }
}

/// Wait, up to `timeout`, for the listener tasks to finish serving whatever they
/// were mid-way through. `true` if they all did.
///
/// Step 1 of the daemon's drain, and the only step a client can observe: both
/// servers were handed a [`ShutdownSignal`], so by now they have stopped accepting,
/// and awaiting their tasks awaits their *watched connections* — the requests a
/// client is part-way through. What happens after this (aborting the background
/// sweeps, closing the cache) is `main`'s ordering and is not something a client is
/// waiting on.
///
/// Generic over what the tasks return, and indifferent to it: a `JoinError` means
/// the task panicked, which its own logging has already reported, and there is
/// nothing a drain can do about it either way. The two callers hand it
/// `anyhow::Result<()>` and `Result<(), tonic::transport::Error>`.
///
/// Lives here rather than inline in `main` so it is reachable from a test — the
/// integration suite's drain arms assert exactly this function's contract, and a
/// binary-private helper is not callable from `tests/`.
pub async fn drain_listeners<S, P>(
    timeout: std::time::Duration,
    s3: tokio::task::JoinHandle<S>,
    peer: Option<tokio::task::JoinHandle<P>>,
) -> bool {
    let listeners = async move {
        let _ = s3.await;
        if let Some(peer) = peer {
            let _ = peer.await;
        }
    };
    tokio::time::timeout(timeout, listeners).await.is_ok()
}

/// Block until the OS asks the process to stop, returning the signal's name.
#[cfg(unix)]
async fn await_signal() -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};

    let mut term = signal(SignalKind::terminate()).expect("registering a SIGTERM handler");
    let mut int = signal(SignalKind::interrupt()).expect("registering a SIGINT handler");
    tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = int.recv() => "SIGINT",
    }
}

/// Non-Unix fallback. The daemon only ever runs on Linux (it reads `/proc` and
/// opens `/dev/infiniband`), but the crate still has to compile elsewhere for a
/// developer's editor, so Ctrl-C stands in for both signals.
#[cfg(not(unix))]
async fn await_signal() -> &'static str {
    tokio::signal::ctrl_c()
        .await
        .expect("registering a Ctrl-C handler");
    "ctrl-c"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn wait_returns_after_trigger() {
        let shutdown = Shutdown::new();
        let signal = shutdown.signal();
        assert!(!signal.is_triggered());
        shutdown.trigger();
        assert!(signal.is_triggered());
        // Would hang if `wait` relied on `changed()` alone.
        signal.wait().await;
    }

    #[tokio::test]
    async fn watcher_taken_after_trigger_does_not_wait() {
        let shutdown = Shutdown::new();
        shutdown.trigger();
        let late = shutdown.signal();
        assert!(late.is_triggered());
        late.wait().await;
    }

    #[tokio::test]
    async fn trigger_is_idempotent() {
        let shutdown = Shutdown::new();
        let signal = shutdown.signal();
        shutdown.trigger();
        shutdown.trigger();
        signal.wait().await;
        assert!(signal.is_triggered());
    }

    #[tokio::test]
    async fn wait_wakes_a_pending_watcher() {
        let shutdown = Shutdown::new();
        let signal = shutdown.signal();
        let waiter = tokio::spawn(async move { signal.wait().await });
        // Yield so the spawned task is parked inside `changed()` first.
        tokio::task::yield_now().await;
        shutdown.trigger();
        waiter.await.unwrap();
    }
}
