use dma_libfabric_protocol::DmaError;
use std::collections::VecDeque;
use std::sync::Arc;

use super::in_flight::{InFlight, Quiescing, reclaim};
use super::ledger::Ledger;
use super::prepared::{Prepared, prepare};
use crate::Completion;
use crate::Operands;
use crate::endpoint::LibfabricEndpoint;
use crate::error::CompletionFailure;
use crate::peer_addresses::{Liveness, RegisteredAddress};
use crate::probe::{Parked, Probes, Waiting};
use crate::server::WorkerMessage;

/// How a liveness probe came out.
pub(super) enum Verdict {
    Alive,
    /// Why, for the transfers and hellos failed because of it.
    Dead(String),
}

/// Everything the fabric worker owns between passes, besides what the post-and-reap loop keeps
/// locally.
pub(super) struct Worker<'counter, T> {
    pub(super) endpoint: LibfabricEndpoint,
    pub(super) ledger: Ledger<'counter>,
    /// Prepared transfers to post, in order. With probes on, only ones whose peer is alive.
    pub(super) pending: VecDeque<Prepared<T>>,
    /// Completed writes waiting to post their quiesce read, still counted as outstanding.
    pub(super) quiescing: VecDeque<Quiescing<T>>,
    pub(super) completions: Vec<Completion<T>>,
    /// `None` when `liveness_probe` is off.
    pub(super) probes: Option<Probes<Prepared<T>>>,
    pub(super) quiesce_writes: bool,
}

impl<T: Operands> Worker<'_, T> {
    pub(super) fn intake(&mut self, message: WorkerMessage<T>) {
        match message {
            WorkerMessage::Transfer(request) => match prepare(request, self.quiesce_writes) {
                Ok(prepared) => {
                    self.ledger.admit(prepared.client_id);
                    self.admit(prepared);
                }
                Err(completion) => {
                    self.ledger.release_uncounted();
                    self.completions.push(completion);
                }
            },
            WorkerMessage::AddPeer {
                client_id,
                address,
                reply,
            } => {
                let peer = match self.endpoint.insert_peer(client_id, &address) {
                    Ok(peer) => peer,
                    Err(error) => return reply.send(Err(error)),
                };
                let Some(probes) = self.probes.as_mut() else {
                    return reply.send(Ok(()));
                };
                match peer.liveness() {
                    Liveness::Alive => reply.send(Ok(())),
                    Liveness::Dead => reply.send(Err(peer_unreachable())),
                    Liveness::Unprobed | Liveness::Probing => {
                        probes.park(&peer, Parked::Reply(reply));
                    }
                }
            }
            WorkerMessage::RemovePeer(client_id) => {
                // Nobody is left to read these, and they hold no device resources yet.
                self.drop_queued(
                    |_, prepared| client_id == prepared.client_id,
                    "the client disconnected before this transfer was posted",
                );
                self.ledger.remove_or_defer(&mut self.endpoint, client_id);
            }
        }
    }

    /// Queue a prepared transfer: to post now, to wait on its peer's probe, or to fail when its
    /// peer is already known dead.
    fn admit(&mut self, prepared: Prepared<T>) {
        if self.probes.is_none() {
            return self.pending.push_back(prepared);
        }
        let peer = match self
            .endpoint
            .insert_peer(prepared.client_id, &prepared.peer_address)
        {
            Ok(peer) => peer,
            Err(error) => return self.fail(prepared, error),
        };
        match peer.liveness() {
            Liveness::Alive => self.pending.push_back(prepared),
            Liveness::Dead => self.fail(prepared, peer_unreachable()),
            Liveness::Unprobed | Liveness::Probing => match self.probes.as_mut() {
                Some(probes) => probes.park(&peer, Parked::Transfer(prepared)),
                None => self.pending.push_back(prepared),
            },
        }
    }

    /// Release everything waiting on a probe that has come out one way or the other.
    pub(super) fn settle(&mut self, waiting: Waiting<Prepared<T>>, verdict: Verdict) {
        let Waiting { peer, parked, .. } = waiting;
        match &verdict {
            Verdict::Alive => peer.set_liveness(Liveness::Alive),
            Verdict::Dead(reason) => {
                tracing::warn!("peer {} is unreachable: {reason}", peer.handle());
                peer.set_liveness(Liveness::Dead);
            }
        }
        for parked in parked {
            match (parked, &verdict) {
                (Parked::Transfer(prepared), Verdict::Alive) => self.pending.push_back(prepared),
                (Parked::Transfer(prepared), Verdict::Dead(reason)) => {
                    self.fail(
                        prepared,
                        DmaError::Transfer(format!("peer unreachable: {reason}")),
                    );
                }
                (Parked::Reply(reply), Verdict::Alive) => reply.send(Ok(())),
                (Parked::Reply(reply), Verdict::Dead(reason)) => reply.send(Err(
                    DmaError::Transfer(format!("peer unreachable: {reason}")),
                )),
            }
        }
    }

    /// A probe was reaped. Any completion, error or not, means the peer's device answered, except
    /// one saying it never did.
    pub(super) fn probe_answered(
        &mut self,
        peer: &Arc<RegisteredAddress>,
        result: Result<(), CompletionFailure>,
    ) {
        let verdict = match result {
            Ok(()) | Err(CompletionFailure::Other(_)) => Verdict::Alive,
            Err(CompletionFailure::PeerUnreachable(error)) => Verdict::Dead(error.to_string()),
        };
        match self
            .probes
            .as_mut()
            .and_then(|probes| probes.answered(peer))
        {
            Some(waiting) => self.settle(waiting, verdict),
            // Its deadline already failed what waited on it. A late answer still decides whether
            // anything more is posted to it.
            None => peer.set_liveness(match verdict {
                Verdict::Alive => Liveness::Alive,
                Verdict::Dead(_) => Liveness::Dead,
            }),
        }
    }

    /// A transfer completed with an error. If the error says its peer is gone, mark the peer dead
    /// and fail what's queued for it, since each of those would be held ~190 s by the device too.
    pub(super) fn note_failure(
        &mut self,
        inflight: &InFlight<T>,
        failure: CompletionFailure,
    ) -> DmaError {
        match (failure, self.probes.is_some(), &inflight.peer) {
            (CompletionFailure::PeerUnreachable(error), true, Some(peer)) => {
                if Liveness::Dead != peer.liveness() {
                    tracing::warn!("peer {} is unreachable: {error}", peer.handle());
                    peer.set_liveness(Liveness::Dead);
                    self.drop_dead();
                }
                error
            }
            (failure, _, _) => failure.into_error(),
        }
    }

    /// Fail every queued transfer and quiesce read whose peer is dead.
    fn drop_dead(&mut self) {
        self.drop_queued(
            |endpoint, prepared| {
                endpoint
                    .find_peer(&prepared.peer_address)
                    .is_some_and(|peer| Liveness::Dead == peer.liveness())
            },
            "the peer stopped answering before this transfer was posted",
        );
        for waiting in std::mem::take(&mut self.quiescing) {
            let dead = waiting
                .inflight
                .peer
                .as_ref()
                .is_some_and(|peer| Liveness::Dead == peer.liveness());
            if dead {
                self.finish_unreaped(waiting.inflight, peer_unreachable());
            } else {
                self.quiescing.push_back(waiting);
            }
        }
    }

    /// Fail every queued or parked transfer `unwanted` picks out. None of them is posted, so their
    /// memory is still the worker's to hand back.
    fn drop_queued(
        &mut self,
        unwanted: impl Fn(&LibfabricEndpoint, &Prepared<T>) -> bool,
        reason: &'static str,
    ) {
        let mut dropped = Vec::new();
        for prepared in std::mem::take(&mut self.pending) {
            if unwanted(&self.endpoint, &prepared) {
                dropped.push(prepared);
            } else {
                self.pending.push_back(prepared);
            }
        }
        if let Some(probes) = self.probes.as_mut() {
            dropped.extend(probes.take_parked(|prepared| unwanted(&self.endpoint, prepared)));
        }
        for prepared in dropped {
            self.fail(prepared, DmaError::Transfer(reason.into()));
        }
    }

    /// Fail a transfer that was never posted.
    pub(super) fn fail(&mut self, prepared: Prepared<T>, error: DmaError) {
        self.finish_unreaped(reclaim(prepared.context), error);
    }

    /// Fail orphaned ops
    pub(super) fn finish_unreaped(&mut self, inflight: Box<InFlight<T>>, error: DmaError) {
        let client_id = inflight.client_id;
        self.completions.push(inflight.into_completion(Err(error)));
        self.ledger.finish(&mut self.endpoint, client_id);
    }
}

fn peer_unreachable() -> DmaError {
    DmaError::Transfer("peer unreachable: it didn't answer a liveness probe or a transfer".into())
}
