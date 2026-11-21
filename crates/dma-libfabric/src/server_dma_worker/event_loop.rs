use crate::sys::FI_EAGAIN;
use dma_libfabric_protocol::DmaError;
use std::collections::VecDeque;
use std::os::raw::c_void;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::Instant;

use std::sync::atomic::AtomicUsize;

use super::in_flight::{InFlight, QuiescePost, Quiescing, post_quiesce, reclaim};
use super::ledger::Ledger;
use super::op_header::{OpHeader, OpKind};
use super::prepared::post;
use super::worker::{Verdict, Worker};
use crate::BatchCompleter;
use crate::Completion;
use crate::Configuration;
use crate::Operands;
use crate::endpoint::LibfabricEndpoint;
use crate::error::{CompletionFailure, fabric_error};
use crate::local_regions::LocalRegions;
use crate::pool::Pool;
use crate::probe::{Probe, Probes};
use crate::reply::Answer;
use crate::server::WorkerMessage;

/// Completions to reap in one pass, and the batch size that triggers a flush — bounding what one
/// batch, and so one downstream lock acquisition, accumulates.
const TARGET_COMPLETIONS: usize = 10;

/// The worker's readiness signal: its endpoint's fabric address, and the handle callers pin with
/// when the provider needs local registrations.
pub(crate) type WorkerReady = (Vec<u8>, Option<LocalRegions>);

pub fn worker_main<T: Operands + Send + 'static>(
    configuration: Configuration,
    ready: Answer<Result<WorkerReady, DmaError>>,
    receiver: &Receiver<WorkerMessage<T>>,
    complete_batch: Arc<BatchCompleter<T>>,
    pool: Arc<Pool>,
    outstanding: &AtomicUsize,
) {
    let mut quiesce = Box::new([0u8; 8]);
    let quiesce_pointer = quiesce.as_mut_ptr();
    // The endpoint's fabric address doubles as the readiness signal — clients insert it via
    // dma.hello so the server can RMA against them on efa-direct.
    let endpoint = match LibfabricEndpoint::open(&configuration, configuration.bind.as_deref())
        .and_then(|endpoint| {
            // Liveness probes land on the quiesce byte too.
            if (configuration.quiesce_writes || configuration.liveness_probe.is_some())
                && let Some(regions) = endpoint.caller_regions()
            {
                regions.pin(quiesce_pointer as usize, quiesce.len())?;
            }
            let address = endpoint.local_address()?;
            Ok((endpoint, address))
        }) {
        Ok((endpoint, address)) => {
            ready.send(Ok((address, endpoint.caller_regions())));
            endpoint
        }
        Err(error) => {
            ready.send(Err(error));
            return;
        }
    };

    let cap = in_flight_cap(&endpoint, configuration.max_in_flight);
    let again = -(FI_EAGAIN as isize);
    let progress_deadline = configuration.progress_deadline;
    let mut worker = Worker {
        endpoint,
        ledger: Ledger::new(outstanding),
        pending: VecDeque::new(),
        quiescing: VecDeque::new(),
        completions: Vec::new(),
        probes: configuration.liveness_probe.map(Probes::new),
        quiesce_writes: configuration.quiesce_writes,
    };
    let mut in_flight: usize = 0;
    let mut reaped: Vec<(*mut c_void, Result<(), CompletionFailure>)> = Vec::new();
    let mut disconnected = false;

    // Reaped ops wanting a checksum: their CRC + reply runs on the pool so hashing doesn't stall
    // posting and reaping. The rest complete inline.
    let mut crc_pending: Vec<ReapedOp<T>> = Vec::new();
    let mut drain_spins = 0;

    loop {
        // Intake: drain the channel without blocking.
        loop {
            match receiver.try_recv() {
                Ok(message) => worker.intake(message),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }

        // Probes first: they hold no bytes, and every transfer to a new peer waits on one.
        let mut posted = 0usize;
        if let Some(probes) = worker.probes.as_mut() {
            let (sent, failed) = probes.post(&mut worker.endpoint, quiesce_pointer);
            in_flight += sent;
            posted += sent;
            let deadline = probes.deadline();
            let expired = probes.expired(Instant::now());
            for (waiting, error) in failed {
                worker.settle(waiting, Verdict::Dead(error.to_string()));
            }
            for waiting in expired {
                worker.settle(
                    waiting,
                    Verdict::Dead(format!("no answer to a liveness probe within {deadline:?}")),
                );
            }
        }

        // Post up to the cap, one pass over the queue.
        let mut attempts = worker.pending.len();
        while in_flight < cap && 0 < attempts {
            attempts -= 1;
            let Some(mut prepared) = worker.pending.pop_front() else {
                break;
            };
            // SAFETY: queued, so its `InFlight` is still the worker's alone.
            if unsafe { (*prepared.context).caller_context.abandoned() } {
                worker.fail(
                    prepared,
                    DmaError::Transfer("abandoned before it was posted".into()),
                );
                continue;
            }
            let error = match post(&mut worker.endpoint, &mut prepared) {
                Ok(0) => {
                    in_flight += 1;
                    posted += 1;
                    continue;
                }
                Ok(code) if code == again => {
                    let stalled_since = *prepared.stalled_since.get_or_insert_with(Instant::now);
                    if stalled_since.elapsed() < progress_deadline {
                        worker.pending.push_back(prepared);
                        continue;
                    }
                    DmaError::Transfer(format!(
                        "no progress posting for {progress_deadline:?}: peer unreachable or transmit queue stuck"
                    ))
                }
                Ok(code) => fabric_error(code as i32, "post"),
                Err(error) => error,
            };
            worker.fail(prepared, error);
        }

        // Reap up to 10, then dispatch — bounding what one batch, and so one downstream lock
        // acquisition, accumulates.
        reaped.clear();
        if let Err(error) = worker.endpoint.reap(&mut reaped, TARGET_COMPLETIONS) {
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
                pending = worker.pending.len(),
            );
        }
        for (context, result) in reaped.drain(..) {
            // SAFETY: every `op_context` this worker posts starts with an `OpHeader`.
            if let OpKind::Probe = unsafe { OpHeader::kind_of(context) } {
                // SAFETY: a probe's, reaped once, here.
                let peer = unsafe { Probe::reclaim(context) };
                worker.probe_answered(&peer, result);
                continue;
            }
            let mut inflight = reclaim(context.cast::<InFlight<T>>());
            let result = result.map_err(|failure| worker.note_failure(&inflight, failure));
            // A write is done after the quiesce read succeeds.
            if result.is_ok()
                && let Some(read) = inflight.quiesce.take()
            {
                worker.quiescing.push_back(Quiescing {
                    inflight,
                    read,
                    stalled_since: None,
                });
                continue;
            }
            worker
                .ledger
                .finish(&mut worker.endpoint, inflight.client_id);
            // Checksummed ops carry their CRC into the completion, so defer the whole completion to
            // the pool rather than hash here. The rest are cheap enough to finish inline.
            if inflight.want_checksum {
                crc_pending.push((inflight, result));
            } else {
                worker.completions.push(inflight.into_completion(result));
            }
        }

        // Post the quiesce reads
        let mut attempts = worker.quiescing.len();
        while 0 < attempts {
            attempts -= 1;
            let Some(waiting) = worker.quiescing.pop_front() else {
                break;
            };
            let (inflight, error) = match post_quiesce(
                &mut worker.endpoint,
                quiesce_pointer,
                waiting,
            ) {
                QuiescePost::Posted => {
                    in_flight += 1;
                    posted += 1;
                    continue;
                }
                QuiescePost::Again(mut waiting) => {
                    let stalled_since = *waiting.stalled_since.get_or_insert_with(Instant::now);
                    if stalled_since.elapsed() < progress_deadline {
                        worker.quiescing.push_back(waiting);
                        continue;
                    }
                    let error = DmaError::Transfer(format!(
                        "no progress posting the quiesce read for {progress_deadline:?}: peer unreachable or transmit queue stuck"
                    ));
                    (waiting.inflight, error)
                }
                QuiescePost::Failed(inflight, error) => (inflight, error),
            };
            // The write completed but can't be quiesced, so the transfer fails.
            worker.finish_unreaped(inflight, error);
        }

        let idle = worker.pending.is_empty()
            && worker.quiescing.is_empty()
            && worker.probes.as_ref().is_none_or(Probes::is_empty)
            && 0 == in_flight;
        if should_flush_crc(crc_pending.len(), posted, reaped_any, idle) {
            submit_crc_batch(&pool, &complete_batch, std::mem::take(&mut crc_pending));
        }

        if !worker.completions.is_empty() {
            if worker.completions.len() < TARGET_COMPLETIONS && drain_spins < 10 {
                // let's not run `complete_batch` again juuuust yet.
                std::hint::spin_loop();
                drain_spins += 1;
                continue;
            }
            drain_spins = 0;
            (*complete_batch)(worker.completions.drain(..));
        }

        if disconnected && idle {
            break;
        }
        if idle {
            // Nothing outstanding: block on the channel to release the core.
            match receiver.recv() {
                Ok(message) => worker.intake(message),
                Err(_) => break, // channel closed and nothing left → shutdown
            }
        } else if 0 == posted && !reaped_any {
            if 0 == in_flight {
                // Everything pending is stalled on -FI_EAGAIN, peers connecting or gone. There is
                // nothing to reap, so busy-polling probably won't help much. Make sure the core is
                // available to other work since it probably isn't useful here for at least a few
                // hundred microseconds.
                std::thread::yield_now();
            } else {
                // Work in flight but nothing moved this pass: stay hot for the completion.
                std::hint::spin_loop();
            }
        }
    }

    // Shutdown: fail never-posted requests and finish deferred checksummed completions inline, since
    // the pool may be tearing down alongside us.
    let mut leftover: Vec<Completion<T>> = worker
        .pending
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
fn submit_crc_batch<T: Operands + Send + 'static>(
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

/// A reaped op awaiting its checksummed completion on the pool.
type ReapedOp<T> = (Box<InFlight<T>>, Result<(), DmaError>);

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
