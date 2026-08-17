//! A background worker that keeps many of the server's one-sided RMAs in flight at once.
//!
//! libfabric RMA is asynchronous: posting a `fi_writemsg`/`fi_read` returns immediately and the
//! completion arrives later on the completion queue. `FabricServer` exploits that with one
//! event-loop thread over one endpoint: it drains submitted [`TransferRequest`]s, posts up to an
//! in-flight cap without waiting, reaps completions in batches, and dispatches each by its
//! `op_context`, the per-op heap pointer that identifies which transfer completed.
//!
//! Completions go back to the caller in batches via the `complete_batch` hook, so a whole batch
//! finishes under one lock acquisition rather than one per op.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::thread::JoinHandle;

use dma_traits::DmaError;

use crate::configuration::Configuration;
use crate::pool::Pool;

/// Direction of the one-sided RMA the server initiates against the client's exposed buffer.
#[derive(Debug, Clone, Copy)]
pub enum Direction {
    /// `fi_writemsg` local to peer: the `dma.get` path.
    ToPeer,
    /// `fi_read` peer to local: the `dma.set` path.
    FromPeer,
}

/// A raw local buffer the worker transfers to or from. The submitter guarantees it stays valid and
/// unmodified until the transfer's completion is handed back.
#[derive(Clone, Copy)]
pub struct TransferBuffer {
    pub pointer: *mut u8,
    pub length: usize,
}

// SAFETY: the submitter keeps `length` bytes alive until the completion runs, and the pointer is
// dereferenced only on the worker thread, never concurrently with the submitter.
unsafe impl Send for TransferBuffer {}

/// Implemented by a transfer's caller context so the worker allocates the `FromPeer` landing buffer
/// itself, keeping the allocation off the submitter's thread and any lock it holds. Runs once per
/// `FromPeer` op in `prepare`, and the implementor retains the allocation until the completion is
/// handed back. `ToPeer` transfers supply their source buffer in the request and never call this.
pub trait DestinationAllocator {
    fn allocate(&mut self, length: usize) -> *mut u8;
}

/// The result of a completed transfer: bytes moved, plus the CRC32 when one was requested.
#[derive(Debug, Clone, Copy)]
pub struct TransferDone {
    pub bytes: usize,
    pub checksum: Option<u32>,
}

/// The outcome of a transfer the worker hands back.
pub type Outcome = Result<TransferDone, DmaError>;

/// A finished transfer's caller-supplied context paired with its outcome. `complete_batch` consumes
/// a whole `Vec` of these under one lock acquisition.
pub struct Completion<T> {
    pub caller_context: T,
    pub outcome: Outcome,
}

/// Hook the caller supplies to finish a batch of completions under one lock acquisition. The
/// context `T` is opaque here; it carries whatever the caller needs to reply and commit.
pub type BatchCompleter<T> = Box<dyn Fn(std::vec::Drain<Completion<T>>) + Send + Sync>;

/// A submitted transfer: where the peer's exposed buffer is, the local buffer to move, and an opaque
/// `caller_context` handed back when the RMA finishes.
pub struct TransferRequest<TContext> {
    pub client_id: u64,
    pub peer_address: Vec<u8>,
    pub remote_key: u64,
    pub remote_address: u64,
    pub buffer: TransferBuffer,
    pub direction: Direction,
    pub want_checksum: bool,
    pub caller_context: TContext,
    /// The caller's per-transfer span. A plain `Id`, not a `Span` clone, because `tracing-cache`
    /// closes a span on the first clone's drop — so the worker parents the on-wire span under it
    /// without touching its lifetime, while the one owning `Span` rides in `caller_context` and
    /// closes at completion. `None` when tracing is off.
    pub parent_id: Option<tracing::span::Id>,
}

/// What the worker consumes off its channel: a transfer to run, or a request to drop a disconnected
/// client's address-vector entry. One ordered channel carries both, so a removal is processed after
/// every transfer the client already submitted.
pub enum WorkerMessage<TContext> {
    Transfer(TransferRequest<TContext>),
    /// Remove this disconnected client's peer once its in-flight transfers have drained.
    RemovePeer(u64),
}

/// A sideband an rpc server uses to implement data-transfer commands over a faster transport. It
/// does no rpc of its own.
///
/// Owns the `fabric-nn` worker thread and the submission channel. Dropping the server closes the
/// channel, so the worker drains its in-flight ops, exits, and is joined.
pub struct FabricServer<TContext: Send + 'static> {
    sender: Option<Sender<WorkerMessage<TContext>>>,
    dma_worker_handle: Option<JoinHandle<()>>,
    /// The worker endpoint's fabric address, captured at startup. `efa-direct` requires the target
    /// hold the initiator's address, so a client inserts this into its own address vector before the
    /// server RMAs against it, learning it via [`Self::local_address`] in the `dma.hello` exchange.
    address: Vec<u8>,
    /// Submitted but not yet finished, counting queued ops as well as posted ones so the depth of
    /// this device's backlog is visible to a caller balancing across devices. Shared with the worker,
    /// which decrements as each op finishes.
    outstanding: Arc<AtomicUsize>,
}

impl<TContext: Send + 'static> std::fmt::Debug for FabricServer<TContext> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FabricServer")
            .field("running", &self.sender.is_some())
            .finish()
    }
}

impl<TContext: Send + 'static> FabricServer<TContext> {
    /// Open the server endpoint on a worker thread and start serving transfers, blocking until the
    /// endpoint opens or fails to. Completions run on the worker thread, except checksummed ones,
    /// whose CRC and reply run on the shared `pool`.
    pub fn start(
        configuration: &Configuration,
        complete_batch: BatchCompleter<TContext>,
        pool: Arc<Pool>,
    ) -> Result<Self, DmaError>
    where
        TContext: DestinationAllocator,
    {
        let (sender, receiver) = channel();
        let (ready_sender, ready_receiver) = channel();
        let configuration = configuration.clone();
        // Shared with the pool jobs finishing checksummed transfers off the worker thread.
        let complete_batch = Arc::new(complete_batch);
        let outstanding = Arc::new(AtomicUsize::new(0));
        let worker_outstanding = Arc::clone(&outstanding);

        static INDEX: AtomicUsize = AtomicUsize::new(0);
        let handle = std::thread::Builder::new()
            .name(format!(
                "fabric-{:02}",
                INDEX.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ))
            .spawn(move || {
                crate::server_dma_worker::worker_main(
                    configuration,
                    &ready_sender,
                    &receiver,
                    complete_batch,
                    pool,
                    &worker_outstanding,
                )
            })
            .map_err(|error| DmaError::Fabric(format!("failed to spawn fabric worker: {error}")))?;
        match ready_receiver.recv() {
            Ok(Ok(address)) => Ok(Self {
                sender: Some(sender),
                dma_worker_handle: Some(handle),
                address,
                outstanding,
            }),
            Ok(Err(error)) => {
                let _ = handle.join();
                Err(error)
            }
            Err(_) => Err(DmaError::Fabric(
                "fabric worker exited before signalling readiness".into(),
            )),
        }
    }

    /// The worker endpoint's fabric address, which a client discovers via `dma.hello` before the
    /// server RMAs against it: on efa-direct the target must hold the initiator's address in its
    /// address vector. This side initiates every dma itself.
    pub fn local_address(&self) -> &[u8] {
        &self.address
    }

    /// Transfers submitted here but not yet finished, queued and posted alike. The load signal a
    /// caller balances on: it rises the moment work is handed over, not when it reaches the wire.
    pub fn outstanding(&self) -> usize {
        self.outstanding.load(Ordering::Relaxed)
    }

    /// Submit a transfer, whose completion is handed back in a batch when it finishes. If the worker
    /// is gone the request comes back, so the caller can fail the client.
    pub fn submit(
        &self,
        request: TransferRequest<TContext>,
    ) -> Result<(), TransferRequest<TContext>> {
        let Some(sender) = self.sender.as_ref() else {
            return Err(request);
        };
        self.outstanding.fetch_add(1, Ordering::Relaxed);
        sender
            .send(WorkerMessage::Transfer(request))
            .map_err(|error| {
                self.outstanding.fetch_sub(1, Ordering::Relaxed);
                let WorkerMessage::Transfer(request) = error.0 else {
                    unreachable!("sent a Transfer");
                };
                request
            })
    }

    /// Ask the worker to drop a disconnected client's address-vector entry once its in-flight
    /// transfers drain. Best-effort: if the worker is gone the entry dies with the endpoint.
    pub fn remove_peer(&self, client_id: u64) {
        if let Some(sender) = self.sender.as_ref() {
            let _ = sender.send(WorkerMessage::RemovePeer(client_id));
        }
    }
}

impl<T: Send + 'static> Drop for FabricServer<T> {
    fn drop(&mut self) {
        // Closing the channel ends the worker's recv loop.
        self.sender = None;
        if let Some(handle) = self.dma_worker_handle.take() {
            let _ = handle.join();
        }
    }
}
