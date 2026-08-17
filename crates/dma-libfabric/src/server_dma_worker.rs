use dma_traits::DmaError;
use dma_traits::checksum;
use libfabric_sys::FI_EAGAIN;
use libfabric_sys::fi_context2;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::os::raw::c_void;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::sync::mpsc::{Receiver, TryRecvError};

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::BatchCompleter;
use crate::Completion;
use crate::Configuration;
use crate::DestinationAllocator;
use crate::Direction;
use crate::TransferBuffer;
use crate::TransferDone;
use crate::TransferRequest;
use crate::endpoint::{LibfabricEndpoint, PeerHandle};
use crate::error::fabric_error;
use crate::local_regions::{LocalOperand, OperandRegistration};
use crate::pool::Pool;
use crate::server::WorkerMessage;

/// Completions to reap in one pass, and the batch size that triggers a flush — bounding what one
/// batch, and so one downstream lock acquisition, accumulates.
const TARGET_COMPLETIONS: usize = 10;

/// A client's address-vector entry, kept so we don't re-insert per request.
pub struct CachedPeer {
    address: Vec<u8>,
    peer: PeerHandle,
}

pub fn worker_main<T: DestinationAllocator + Send + 'static>(
    configuration: Configuration,
    ready: &Sender<Result<Vec<u8>, DmaError>>,
    receiver: &Receiver<WorkerMessage<T>>,
    complete_batch: Arc<BatchCompleter<T>>,
    pool: Arc<Pool>,
    outstanding: &AtomicUsize,
) {
    // The endpoint's fabric address doubles as the readiness signal — clients insert it via
    // dma.hello so the server can RMA against them on efa-direct.
    let mut endpoint = match LibfabricEndpoint::open(&configuration, configuration.bind.as_deref())
        .and_then(|endpoint| {
            let address = endpoint.local_address()?;
            Ok((endpoint, address))
        }) {
        Ok((endpoint, address)) => {
            let _ = ready.send(Ok(address));
            endpoint
        }
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };

    let cap = in_flight_cap(&endpoint, configuration.max_in_flight);
    let again = -(FI_EAGAIN as isize);
    let mut peers: HashMap<u64, CachedPeer> = HashMap::new();
    // Outstanding (queued or in-flight) ops per client, and clients whose peer to remove once that
    // count hits 0. Counting from intake rather than posting stops a removal slipping between a
    // queued transfer and its post, where the post would re-insert the peer just removed. Deferring
    // `fi_av_remove` to 0 also keeps it from flushing a live queue pair — the failure this prevents.
    let mut outstanding_by_client: HashMap<u64, usize> = HashMap::new();
    let mut pending_removal: HashSet<u64> = HashSet::new();
    let mut pending: VecDeque<Prepared<T>> = VecDeque::new();
    let mut in_flight: usize = 0;
    let mut reaped: Vec<(*mut c_void, Result<(), DmaError>)> = Vec::new();
    let mut disconnected = false;

    let mut completions: Vec<Completion<T>> = Vec::new();
    // Reaped ops wanting a checksum: their CRC + reply runs on the pool so hashing doesn't stall
    // posting and reaping. The rest complete inline.
    let mut crc_pending: Vec<ReapedOp<T>> = Vec::new();
    let mut drain_spins = 0;

    loop {
        // Intake: drain the channel without blocking.
        loop {
            match receiver.try_recv() {
                Ok(WorkerMessage::Transfer(request)) => {
                    *outstanding_by_client.entry(request.client_id).or_insert(0) += 1;
                    pending.push_back(prepare(request));
                }
                Ok(WorkerMessage::RemovePeer(client_id)) => remove_or_defer(
                    &endpoint,
                    &mut peers,
                    &outstanding_by_client,
                    &mut pending_removal,
                    client_id,
                ),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }

        // Post up to the cap.
        let mut posted = 0usize;
        while in_flight < cap {
            let Some(prepared) = pending.pop_front() else {
                break;
            };
            match post(&mut endpoint, &mut peers, &prepared) {
                Ok(0) => {
                    in_flight += 1;
                    posted += 1;
                }
                Ok(code) if code == again => {
                    pending.push_front(prepared); // tx queue full: defer, reap to make room
                    break;
                }
                // Post failed outright: the op finishes here and is never reaped, so account it.
                Ok(code) => {
                    let error = fabric_error(code as i32, "post");
                    let client_id = prepared.client_id;
                    completions.push(reclaim(prepared.context).into_completion(Err(error)));
                    finish_op(
                        &endpoint,
                        &mut peers,
                        &mut outstanding_by_client,
                        &mut pending_removal,
                        outstanding,
                        client_id,
                    );
                }
                Err(error) => {
                    let client_id = prepared.client_id;
                    completions.push(reclaim(prepared.context).into_completion(Err(error)));
                    finish_op(
                        &endpoint,
                        &mut peers,
                        &mut outstanding_by_client,
                        &mut pending_removal,
                        outstanding,
                        client_id,
                    );
                }
            }
        }

        // Reap up to 10, then dispatch — bounding what one batch, and so one downstream lock
        // acquisition, accumulates.
        reaped.clear();
        if let Err(error) = endpoint.reap(&mut reaped, TARGET_COMPLETIONS) {
            // Names no op, so no client can be failed for it. Keep serving: the ops it did yield are
            // in `reaped`, and the rest stay in flight for a later pass.
            tracing::warn!("completion queue read failed: {error}");
        }
        // Saturating because a reap that outran `in_flight` would otherwise wrap and leave the
        // worker unable to post again — stalling every client on this device, silently.
        in_flight = in_flight.saturating_sub(reaped.len());
        let reaped_any = !reaped.is_empty();
        // `in_flight` is the live pipeline depth — at c=1 it pins at 1. Emitted only when
        // something moved this pass.
        if 0 < posted || reaped_any {
            let _pass = tracing::debug_span!(
                parent: None,
                "worker_pass",
                in_flight,
                posted,
                reaped = reaped.len(),
                pending = pending.len(),
            );
        }
        for (context, result) in reaped.drain(..) {
            let inflight = reclaim(context.cast::<InFlight<T>>());
            finish_op(
                &endpoint,
                &mut peers,
                &mut outstanding_by_client,
                &mut pending_removal,
                outstanding,
                inflight.client_id,
            );
            // Checksummed ops carry their CRC into the completion, so defer the whole completion to
            // the pool rather than hash here. The rest are cheap enough to finish inline.
            if inflight.want_checksum {
                crc_pending.push((inflight, result));
            } else {
                completions.push(inflight.into_completion(result));
            }
        }

        if should_flush_crc(
            crc_pending.len(),
            posted,
            reaped_any,
            pending.is_empty() && 0 == in_flight,
        ) {
            submit_crc_batch(&pool, &complete_batch, std::mem::take(&mut crc_pending));
        }

        if !completions.is_empty() {
            if completions.len() < TARGET_COMPLETIONS && drain_spins < 10 {
                // let's not run `complete_batch` again juuuust yet.
                std::hint::spin_loop();
                drain_spins += 1;
                continue;
            }
            drain_spins = 0;
            (*complete_batch)(completions.drain(..));
        }

        if disconnected && pending.is_empty() && 0 == in_flight {
            break;
        }
        if 0 == in_flight && pending.is_empty() {
            // Nothing outstanding: block on the channel to release the core.
            match receiver.recv() {
                Ok(WorkerMessage::Transfer(request)) => {
                    *outstanding_by_client.entry(request.client_id).or_insert(0) += 1;
                    pending.push_back(prepare(request));
                }
                // Nothing is outstanding here, so this client has no ops to drain: remove now.
                Ok(WorkerMessage::RemovePeer(client_id)) => remove_or_defer(
                    &endpoint,
                    &mut peers,
                    &outstanding_by_client,
                    &mut pending_removal,
                    client_id,
                ),
                Err(_) => break, // channel closed and nothing left → shutdown
            }
        } else if 0 == posted && !reaped_any {
            // Work outstanding but nothing moved: don't spin hot.
            std::hint::spin_loop();
        }
    }

    // Shutdown: fail never-posted requests and finish deferred checksummed completions inline, since
    // the pool may be tearing down alongside us.
    let mut leftover: Vec<Completion<T>> = pending
        .drain(..)
        .map(|prepared| {
            let error = DmaError::Fabric("fabric worker is gone".into());
            reclaim(prepared.context).into_completion(Err(error))
        })
        .chain(
            crc_pending
                .into_iter()
                .map(|(op, result)| op.into_completion(result)),
        )
        .collect();
    if !leftover.is_empty() {
        (*complete_batch)(leftover.drain(..));
    }
}

/// Whether to hand the accumulated checksummed completions to the pool this pass: when the batch
/// fills, when nothing moved, or when the worker is about to park.
///
/// `about_to_park` is the one that must never be dropped. The worker blocks on the channel with
/// nothing left to post or reap, so a batch held back here waits for an unrelated message to wake
/// the worker — stranding every client in it until then.
fn should_flush_crc(batched: usize, posted: usize, reaped_any: bool, about_to_park: bool) -> bool {
    0 < batched && (batched >= TARGET_COMPLETIONS || (0 == posted && !reaped_any) || about_to_park)
}

/// Finish checksummed completions on the pool: hash each payload, then run the batch's
/// `complete_batch`, all off the fabric worker.
fn submit_crc_batch<T: DestinationAllocator + Send + 'static>(
    pool: &Arc<Pool>,
    complete_batch: &Arc<BatchCompleter<T>>,
    batch: Vec<ReapedOp<T>>,
) {
    let complete = Arc::clone(complete_batch);
    pool.submit(Box::new(move || {
        let mut done: Vec<Completion<T>> = batch
            .into_iter()
            .map(|(op, result)| op.into_completion(result))
            .collect();
        (*complete)(done.drain(..));
    }));
}

/// Remove a disconnected client's peer, or defer until its last op finishes. Removing with ops
/// outstanding would flush the queue pair, and a queued transfer would re-insert the peer.
fn remove_or_defer(
    endpoint: &LibfabricEndpoint,
    peers: &mut HashMap<u64, CachedPeer>,
    outstanding_by_client: &HashMap<u64, usize>,
    pending_removal: &mut HashSet<u64>,
    client_id: u64,
) {
    if 0 == outstanding_by_client.get(&client_id).copied().unwrap_or(0) {
        drop_peer(endpoint, peers, client_id);
    } else {
        pending_removal.insert(client_id);
    }
}

/// Account one finished op, reaped or failed to post, against its client, performing a deferred peer
/// removal when its last op drains.
fn finish_op(
    endpoint: &LibfabricEndpoint,
    peers: &mut HashMap<u64, CachedPeer>,
    outstanding_by_client: &mut HashMap<u64, usize>,
    pending_removal: &mut HashSet<u64>,
    outstanding: &AtomicUsize,
    client_id: u64,
) {
    // The device-wide counter `submit` raised. Saturating for the same reason the in-flight tally is:
    // a stray extra finish must not wrap it and make this device look infinitely loaded forever.
    let _ = outstanding.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(1))
    });
    let remaining = match outstanding_by_client.get_mut(&client_id) {
        Some(count) => {
            *count -= 1;
            *count
        }
        None => 0,
    };
    if 0 == remaining {
        outstanding_by_client.remove(&client_id);
        if pending_removal.remove(&client_id) {
            drop_peer(endpoint, peers, client_id);
        }
    }
}

/// Remove a client's address-vector entry and forget its cached handle. No-op if it never had one.
fn drop_peer(endpoint: &LibfabricEndpoint, peers: &mut HashMap<u64, CachedPeer>, client_id: u64) {
    if let Some(cached) = peers.remove(&client_id)
        && let Err(error) = endpoint.remove_peer(cached.peer)
    {
        tracing::warn!("fi_av_remove failed for disconnected client {client_id}: {error}");
    }
}

/// A reaped op awaiting its checksummed completion on the pool.
type ReapedOp<T> = (Box<InFlight<T>>, Result<(), DmaError>);

/// Reclaim ownership of an `InFlight` from its `op_context` pointer. Called once per op: on its
/// completion, or inline on a post failure or shutdown.
fn reclaim<T>(context: *mut InFlight<T>) -> Box<InFlight<T>> {
    // SAFETY: the pointer came from `Box::into_raw` in `prepare` and is reclaimed once.
    unsafe { Box::from_raw(context) }
}

/// Post one prepared transfer, returning the raw libfabric code (`0` posted, `-FI_EAGAIN` transmit
/// queue full, else error), or `Err` for a setup failure: peer insert or region registration.
fn post<T>(
    endpoint: &mut LibfabricEndpoint,
    peers: &mut HashMap<u64, CachedPeer>,
    prepared: &Prepared<T>,
) -> Result<isize, DmaError> {
    let peer = peer_handle(endpoint, peers, prepared.client_id, &prepared.peer_address)?;
    let context = prepared.context.cast::<c_void>();
    let length = prepared.buffer.length;
    let LocalOperand {
        descriptor,
        registration,
    } = endpoint.local_operand(prepared.buffer.pointer, length)?;
    let connection = endpoint.connection(peer, prepared.remote_key, prepared.remote_address);
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
                // SAFETY: the submitter keeps this buffer valid until the completion runs.
                let bytes = unsafe { std::slice::from_raw_parts(prepared.buffer.pointer, length) };
                connection.post_out(bytes, descriptor, context)
            }
            Direction::FromPeer => {
                // SAFETY: as above; no other thread touches the buffer until completion.
                let bytes =
                    unsafe { std::slice::from_raw_parts_mut(prepared.buffer.pointer, length) };
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
            // Held until the completion drops the operation.
            (*prepared.context).local_registration = registration;
        }
    }
    Ok(code)
}

/// The in-flight cap, clamped to the provider's transmit-queue depth so we self-limit below
/// `-FI_EAGAIN`.
fn in_flight_cap(endpoint: &LibfabricEndpoint, configured: Option<usize>) -> usize {
    // A few tens saturate the network card at large payloads while bounding pinned memory, since
    // each in-flight op pins its buffer.
    let requested = configured.unwrap_or(64).max(1);
    // `tx_attr.size` is 0 when the provider reports no fixed depth; clamping to that would cap at 1
    // and serialize everything.
    match endpoint.max_tx() {
        0 => requested,
        hardware => requested.min(hardware),
    }
}

/// The cached `fi_addr_t` for this client, inserting or refreshing it as needed.
fn peer_handle(
    endpoint: &LibfabricEndpoint,
    peers: &mut HashMap<u64, CachedPeer>,
    client_id: u64,
    address: &[u8],
) -> Result<PeerHandle, DmaError> {
    if let Some(cached) = peers.get(&client_id) {
        if cached.address == address {
            return Ok(cached.peer);
        }
        // Drop the stale entry first, so a re-advertised address doesn't leak the old handle.
        let _ = endpoint.remove_peer(cached.peer);
    }
    let peer = endpoint.insert_peer(address)?;
    peers.insert(
        client_id,
        CachedPeer {
            address: address.to_vec(),
            peer,
        },
    );
    Ok(peer)
}

/// Per-op state. Its pointer is the `op_context` handed to libfabric and recovered on completion.
#[repr(C)]
struct InFlight<T> {
    /// `#[repr(C)]` with this field first gives `&inflight == &inflight._context2`, so passing the
    /// `InFlight` pointer as `op_context` satisfies `FI_CONTEXT2` and the completion hands the whole
    /// struct back — no separate in-flight table needed. The efa-direct provider owns these 64 bits
    /// for the op's duration; never read them. Providers without `FI_CONTEXT2` (tcp, rxr efa) ignore
    /// the contents.
    _context2: fi_context2,
    /// For users of this crate: completion callbacks and whatever else the call carries.
    caller_context: T,
    /// The submitting client, so reaping decrements its in-flight count and fires a deferred peer
    /// removal once its last op drains.
    client_id: u64,
    length: usize,
    /// Whether the caller wants a checksum, taken over the operand at completion, on the pool.
    want_checksum: bool,
    /// The local operand: the source for `ToPeer`, the landing buffer for `FromPeer`.
    buffer_pointer: *mut u8,
    /// The operand's registration under `RegionMode::PerOperation`, closed when this op is reaped
    /// and its `InFlight` dropped. `None` when the registry owns it.
    local_registration: Option<OperandRegistration>,
    /// Post to reap: the on-wire duration. `Span::none()` until posted, and when tracing is off.
    wire_span: tracing::Span,
    /// Child of `wire_span` covering only the wait, from `fi_*` returning to the completion reaped.
    await_span: tracing::Span,
}

// SAFETY: moved to a pool worker to finish a checksummed completion. Its raw pointers reference the
// operand, kept alive by `caller_context` until the completion runs, and the provider-owned
// `_context2` scratch, untouched after the op is reaped.
unsafe impl<T: Send> Send for InFlight<T> {}

impl<T> InFlight<T> {
    /// Turn a raw libfabric result into a [`Completion`], hashing the operand if asked. Checksummed
    /// ops run this on a pool worker, keeping the hash off the fabric worker.
    fn into_completion(self, result: Result<(), DmaError>) -> Completion<T> {
        let outcome = result.map(|()| {
            let checksum_value = self.want_checksum.then(|| {
                let _span =
                    tracing::info_span!(parent: &self.wire_span, "checksum", length = self.length)
                        .entered();
                // SAFETY: the caller retains the operand until this completion hands it back.
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

/// A request whose `InFlight` box is already built, so an `-FI_EAGAIN` deferral holds it cheaply,
/// plus the parameters needed to post.
struct Prepared<T> {
    context: *mut InFlight<T>,
    client_id: u64,
    peer_address: Vec<u8>,
    remote_key: u64,
    remote_address: u64,
    buffer: TransferBuffer,
    direction: Direction,
    /// The caller's transfer span, parent of the on-wire span.
    parent_id: Option<tracing::span::Id>,
}

/// Build the per-op `InFlight` box and the post parameters.
fn prepare<T: DestinationAllocator>(mut request: TransferRequest<T>) -> Prepared<T> {
    let length = request.buffer.length;
    // `ToPeer` writes from the caller's source buffer; `FromPeer` allocates its landing buffer here,
    // on the worker thread, keeping that allocation off the submitting thread.
    let pointer = match request.direction {
        Direction::ToPeer => request.buffer.pointer,
        Direction::FromPeer => request.caller_context.allocate(length),
    };
    let inflight = Box::new(InFlight {
        // Provider-owned scratch; it initializes this when the op is posted.
        _context2: unsafe { std::mem::zeroed() },
        caller_context: request.caller_context,
        client_id: request.client_id,
        length,
        want_checksum: request.want_checksum,
        buffer_pointer: pointer,
        // Set at post time, once the operand is registered.
        local_registration: None,
        wire_span: tracing::Span::none(),
        await_span: tracing::Span::none(),
    });
    Prepared {
        context: Box::into_raw(inflight),
        client_id: request.client_id,
        peer_address: request.peer_address,
        remote_key: request.remote_key,
        remote_address: request.remote_address,
        buffer: TransferBuffer { pointer, length },
        direction: request.direction,
        parent_id: request.parent_id,
    }
}

#[cfg(test)]
mod tests {
    use super::{TARGET_COMPLETIONS, should_flush_crc};

    /// The worker is about to block on the channel, so a held-back batch would wait for an unrelated
    /// message to wake it — the clients in it hang until then. This is what a reaped checksummed
    /// transfer hit: batch of 1, nothing posted, but `reaped_any` true, so neither of the other two
    /// conditions fired.
    #[test]
    fn flushes_a_partial_batch_before_parking() {
        assert!(should_flush_crc(1, 0, true, true));
    }

    #[test]
    fn flushes_a_full_batch_even_with_work_left() {
        assert!(should_flush_crc(TARGET_COMPLETIONS, 4, true, false));
    }

    /// Nothing moved this pass, so a partial batch shouldn't wait on future work.
    #[test]
    fn flushes_a_partial_batch_when_nothing_moved() {
        assert!(should_flush_crc(1, 0, false, false));
    }

    /// Work is still in flight and the batch is partial: let it fill.
    #[test]
    fn holds_a_partial_batch_while_work_is_moving() {
        assert!(!should_flush_crc(1, 1, true, false));
    }

    #[test]
    fn never_flushes_an_empty_batch() {
        assert!(!should_flush_crc(0, 0, false, true));
    }
}
