use dma_libfabric_protocol::DmaError;
use std::os::raw::c_void;
use std::time::Instant;

use super::in_flight::{InFlight, QuiesceRead};
use super::op_header::{OpHeader, OpKind};
use crate::Completion;
use crate::Direction;
use crate::Operands;
use crate::TransferRequest;
use crate::endpoint::LibfabricEndpoint;
use crate::local_regions::LocalOperand;

/// A request whose `InFlight` box is already built, so an `-FI_EAGAIN` deferral holds it cheaply,
/// plus the parameters needed to post.
pub(super) struct Prepared<T> {
    pub(super) context: *mut InFlight<T>,
    pub(super) client_id: u64,
    pub(super) peer_address: Vec<u8>,
    remote_key: u64,
    remote_address: u64,
    /// The resolved local operand pointer must stay valid for the post.
    pointer: *mut u8,
    length: usize,
    direction: Direction,
    /// The caller's transfer span, parent of the on-wire span.
    parent_id: Option<tracing::span::Id>,
    /// When the provider first answered this op `-FI_EAGAIN`; the progress deadline counts from
    /// here. After posting, the op is gone, so this never needs clearing.
    pub(super) stalled_since: Option<Instant>,
}

/// Build the per-op `InFlight` box and the post parameters, resolving the local operand out of the
/// caller's context.
/// Both run on the worker thread, keeping allocation off the submitting thread.
pub(super) fn prepare<T: Operands>(
    request: TransferRequest<T>,
    quiesce_writes: bool,
) -> Result<Prepared<T>, Completion<T>> {
    let mut inflight = Box::new(InFlight {
        header: OpHeader::new(OpKind::Transfer),
        caller_context: request.caller_context,
        client_id: request.client_id,
        // Both set just below, once the operand resolves.
        length: 0,
        buffer_pointer: std::ptr::null_mut(),
        want_checksum: request.want_checksum,
        // Set at post time, once the operand is registered.
        local_lease: None,
        wire_span: tracing::Span::none(),
        await_span: tracing::Span::none(),
        quiesce: None,
        peer: None,
    });
    let operand = match request.direction {
        Direction::ToPeer => match inflight.caller_context.source() {
            Some(bytes) => Ok((bytes.as_ptr().cast_mut(), bytes.len())),
            None => Err(DmaError::Fabric(
                "the caller context supplied no source bytes for a ToPeer transfer".into(),
            )),
        },
        Direction::FromPeer { length } => match inflight.caller_context.allocate(length) {
            // Overrunning a short landing buffer would corrupt whatever follows it, so refuse.
            Some(landing) if landing.len() < length => Err(DmaError::Fabric(format!(
                "the caller context allocated {} bytes for a {length} byte FromPeer transfer",
                landing.len()
            ))),
            Some(landing) => Ok((landing.as_mut_ptr(), length)),
            None => Err(DmaError::Fabric(
                "the caller context allocated no landing buffer for a FromPeer transfer".into(),
            )),
        },
    };
    let (pointer, length) = match operand {
        Ok(operand) => operand,
        Err(error) => {
            return Err(Completion {
                caller_context: inflight.caller_context,
                outcome: Err(error),
            });
        }
    };
    inflight.buffer_pointer = pointer;
    inflight.length = length;
    if quiesce_writes && matches!(request.direction, Direction::ToPeer) && 0 < length {
        // The write itself would fail on a range this overflows; refuse before posting either.
        let Some(last) = request.remote_address.checked_add(length as u64 - 1) else {
            return Err(Completion {
                caller_context: inflight.caller_context,
                outcome: Err(DmaError::Fabric("the peer's buffer range overflows".into())),
            });
        };
        inflight.quiesce = Some(QuiesceRead {
            peer_address: Vec::new(),
            remote_key: request.remote_key,
            remote_address: last,
        });
    }
    Ok(Prepared {
        context: Box::into_raw(inflight),
        client_id: request.client_id,
        peer_address: request.peer_address,
        remote_key: request.remote_key,
        remote_address: request.remote_address,
        pointer,
        length,
        direction: request.direction,
        parent_id: request.parent_id,
        stalled_since: None,
    })
}

/// Post one prepared transfer, returning the raw libfabric code (`0` posted, `-FI_EAGAIN` transmit
/// queue full, else error), or `Err` for a setup failure: peer insert or region registration.
pub(super) fn post<T: Operands>(
    endpoint: &mut LibfabricEndpoint,
    prepared: &mut Prepared<T>,
) -> Result<isize, DmaError> {
    let peer = endpoint.insert_peer(prepared.client_id, &prepared.peer_address)?;
    let context = prepared.context.cast::<c_void>();
    let length = prepared.length;
    // Asked only on a registration cache miss, so a caller's per-extent setup runs once per
    // registration rather than once per transfer.
    // SAFETY: `context` is this operation's live `InFlight`, and nothing else touches it until posted.
    let cacheable_span = || unsafe { (*prepared.context).caller_context.cacheable_span() };
    let LocalOperand { descriptor, lease } =
        endpoint.local_operand(prepared.pointer, length, cacheable_span)?;
    let connection = endpoint.connection(&peer, prepared.remote_key, prepared.remote_address);
    // On-wire time, as an explicit child of the caller's `dma.get`/`dma.set` span so the transfer
    // tree covers setup -> wire -> completion without entering the parent on this thread. Stored in
    // the operation, so it stays open until completion.
    let wire = tracing::info_span!(
        parent: prepared.parent_id.clone(),
        "wire",
        direction = ?prepared.direction,
        length
    );
    let code = {
        // so the post spans are children of the wire span
        let _posting = wire.enter();
        match prepared.direction {
            Direction::ToPeer => {
                // SAFETY: the operand lives in the caller context, which the `InFlight` holds until
                //         the completion runs.
                let bytes = unsafe { std::slice::from_raw_parts(prepared.pointer, length) };
                connection.post_out(bytes, descriptor, context)
            }
            Direction::FromPeer { .. } => {
                // SAFETY: as above; no other thread touches the buffer until completion.
                let bytes = unsafe { std::slice::from_raw_parts_mut(prepared.pointer, length) };
                connection.post_in(bytes, descriptor, context)
            }
        }
    };
    if 0 == code {
        // Hand both spans to the in-flight operation so they live until it is reaped.
        let await_completion = tracing::info_span!(parent: &wire, "await_completion");
        // SAFETY: `context` is this operation's live `InFlight`, valid until the completion runs, and no
        //         other thread touches it until then.
        unsafe {
            (*prepared.context).wire_span = wire;
            (*prepared.context).await_span = await_completion;
            // Held until the completion drops the operation: while this lease lives the region
            // cannot be deregistered, so a concurrent reclaim cannot pull it out from under the
            // descriptor this post just handed to libfabric.
            (*prepared.context).local_lease = lease;
            // Posted, so `prepared` is done with the address and the quiesce read takes it.
            if let Some(quiesce) = (*prepared.context).quiesce.as_mut() {
                quiesce.peer_address = std::mem::take(&mut prepared.peer_address);
            }
            (*prepared.context).peer = Some(peer);
        }
    }
    Ok(code)
}
