use crate::sys::FI_EAGAIN;
use dma_libfabric_protocol::DmaError;
use dma_libfabric_protocol::checksum;
use std::sync::Arc;
use std::time::Instant;

use super::op_header::OpHeader;
use crate::Completion;
use crate::TransferDone;
use crate::endpoint::LibfabricEndpoint;
use crate::error::fabric_error;
use crate::local_regions::LocalOperand;
use crate::peer_addresses::RegisteredAddress;
use crate::region_cache::Registration;

/// Per-op state. Its pointer is the `op_context` handed to libfabric and recovered on completion.
#[repr(C)]
pub(super) struct InFlight<T> {
    /// First; see [`OpHeader`].
    pub(super) header: OpHeader,
    /// For users of this crate: completion callbacks and whatever else the call carries.
    pub(super) caller_context: T,
    /// The submitting client, so reaping decrements its in-flight count and fires a deferred peer
    /// removal once its last op drains.
    pub(super) client_id: u64,
    pub(super) length: usize,
    /// Whether the caller wants a checksum, taken over the operand at completion, on the pool.
    pub(super) want_checksum: bool,
    /// The local operand: the source for `ToPeer`, the landing buffer for `FromPeer`.
    pub(super) buffer_pointer: *mut u8,
    /// The operand's lease on its registration, held from post until this op is reaped and its
    /// `InFlight` dropped. An uncached extent is deregistered right there, being its only holder; a
    /// cached one only if a reclaim already retired it from the registry. `None` until posted, and
    /// on providers needing no local registration.
    pub(super) local_lease: Option<Arc<Registration>>,
    /// Post to reap: the on-wire duration. `Span::none()` until posted, and when tracing is off.
    pub(super) wire_span: tracing::Span,
    /// Child of `wire_span` covering only the wait, from `fi_*` returning to the completion reaped.
    pub(super) await_span: tracing::Span,
    /// Posted once this `ToPeer` write completes. `None` when `quiesce_writes` is off, for `FromPeer`,
    /// and for an empty write.
    pub(super) quiesce: Option<QuiesceRead>,
    /// The entry this op was posted against, so an unreachable completion can mark it dead. `None`
    /// until posted.
    pub(super) peer: Option<Arc<RegisteredAddress>>,
}

// SAFETY: moved to a pool worker to finish a checksummed completion. Its raw pointers reference the
// operand, kept alive by `caller_context` until the completion runs, and the provider-owned
// `_context2` scratch, untouched after the op is reaped.
unsafe impl<T: Send> Send for InFlight<T> {}

impl<T> InFlight<T> {
    /// Turn a raw libfabric result into a [`Completion`], hashing the operand if asked. Checksummed
    /// ops run this on a pool worker, keeping the hash off the fabric worker.
    ///
    /// Takes the box rather than its contents so the hash runs before the caller context moves. The
    /// operand may live inside that context, and the box keeps the address stable since `prepare`.
    #[expect(
        clippy::boxed_local,
        reason = "the box pins the operand's address across the hash"
    )]
    pub(super) fn into_completion(self: Box<Self>, result: Result<(), DmaError>) -> Completion<T> {
        let outcome = result.map(|()| {
            let checksum_value = self.want_checksum.then(|| {
                let _span =
                    tracing::info_span!(parent: &self.wire_span, "checksum", length = self.length)
                        .entered();
                // SAFETY: the operand is the caller context's, still held by this `InFlight`.
                checksum(unsafe { std::slice::from_raw_parts(self.buffer_pointer, self.length) })
            });
            TransferDone {
                bytes: self.length,
                checksum: checksum_value,
            }
        });
        Completion {
            caller_context: self.caller_context,
            outcome,
        }
    }
}

/// Reclaim ownership of an `InFlight` from its `op_context` pointer. Called once per op: on its
/// completion, or inline on a post failure or shutdown.
pub(super) fn reclaim<T>(context: *mut InFlight<T>) -> Box<InFlight<T>> {
    // SAFETY: the pointer came from `Box::into_raw` in `prepare` and is reclaimed once.
    unsafe { Box::from_raw(context) }
}

/// Tiny read back from the end of a write. The target NIC can't return the read ahead of its posted
/// writes, so the read ensures the payload is in the peer's memory.
pub(super) struct QuiesceRead {
    /// Moved out of [`Prepared`](super::prepared::Prepared) after the write posts. The peer is
    /// re-resolved through the endpoint rather than held, so eviction still drops the last clone of
    /// a stale entry.
    pub(super) peer_address: Vec<u8>,
    pub(super) remote_key: u64,
    pub(super) remote_address: u64,
}

/// A completed write waiting for a quiesce read.
pub(super) struct Quiescing<T> {
    pub(super) inflight: Box<InFlight<T>>,
    pub(super) read: QuiesceRead,
    /// When the provider first answered `-FI_EAGAIN`; the progress deadline counts from here.
    pub(super) stalled_since: Option<Instant>,
}

/// How posting a quiesce read went.
pub(super) enum QuiescePost<T> {
    /// libfabric holds the op until the read is reaped.
    Posted,
    /// `-FI_EAGAIN`: the op comes back to retry.
    Again(Quiescing<T>),
    Failed(Box<InFlight<T>>, DmaError),
}

/// Post a write's quiesce read into the worker's quiesce buffer.
/// The op itself is the read's `op_context`, as was the write's.
pub(super) fn post_quiesce<T>(
    endpoint: &mut LibfabricEndpoint,
    landing: *mut u8,
    waiting: Quiescing<T>,
) -> QuiescePost<T> {
    let Quiescing {
        mut inflight,
        read,
        stalled_since,
    } = waiting;
    let resolved = endpoint
        .insert_peer(inflight.client_id, &read.peer_address)
        .and_then(|peer| Ok((peer, endpoint.local_operand(landing, 1, || None)?)));
    let (peer, LocalOperand { descriptor, lease }) = match resolved {
        Ok(resolved) => resolved,
        Err(error) => return QuiescePost::Failed(inflight, error),
    };
    // The write is done with its operand, so the read's landing takes the lease slot.
    inflight.local_lease = lease;
    let connection = endpoint.connection(&peer, read.remote_key, read.remote_address);
    let quiesce = tracing::info_span!(parent: &inflight.wire_span, "quiesce");
    let context = Box::into_raw(inflight);
    // SAFETY: the quiesce buffer outlives the worker's every read into it, and its bytes are never
    // read back.
    let bytes = unsafe { std::slice::from_raw_parts_mut(landing, 1) };
    let code = {
        let _posting = quiesce.enter();
        connection.post_in(bytes, descriptor, context.cast())
    };
    if 0 == code {
        // SAFETY: posted, so the op is libfabric's until this thread reaps it.
        unsafe { (*context).await_span = quiesce };
        return QuiescePost::Posted;
    }
    let inflight = reclaim(context);
    if code == -(FI_EAGAIN as isize) {
        return QuiescePost::Again(Quiescing {
            inflight,
            read,
            stalled_since,
        });
    }
    QuiescePost::Failed(inflight, fabric_error(code as i32, "quiesce read"))
}
