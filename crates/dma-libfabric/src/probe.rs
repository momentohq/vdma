//! Liveness probes: a 0-byte read from a new peer before anything else is posted to it.
//!
//! An EFA device holds a read to a peer that never answers for about 190 s without sending it, and
//! while enough read bytes are held that way it stops serving every other peer on the device. So a
//! new peer's transfers wait here while a 0-byte read, which holds nothing, finds out whether it
//! answers. Any answer counts, errors included: a bad-key or unknown-peer error comes from the
//! peer's own device.

use std::os::raw::c_void;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dma_libfabric_protocol::DmaError;

use crate::endpoint::LibfabricEndpoint;
use crate::local_regions::LocalOperand;
use crate::peer_addresses::{Liveness, RegisteredAddress};
use crate::region_cache::Registration;
use crate::reply::Answer;
use crate::server_dma_worker::{OpHeader, OpKind};
use crate::sys::FI_EAGAIN;

/// A posted probe. Its pointer is the `op_context`, like a transfer's `InFlight`.
#[repr(C)]
pub(crate) struct Probe {
    /// First, so the reaper can read the kind before knowing which type it holds.
    header: OpHeader,
    /// Held until reaped, so the entry stays in the address vector while the device holds the read.
    peer: Arc<RegisteredAddress>,
    /// The landing byte's registration, likewise held until reaped.
    _lease: Option<Arc<Registration>>,
}

impl Probe {
    /// Reclaim a reaped probe from its `op_context`, handing back its peer.
    ///
    /// # Safety
    /// `context` came from a posted probe's `Box::into_raw` and is reclaimed once.
    pub(crate) unsafe fn reclaim(context: *mut c_void) -> Arc<RegisteredAddress> {
        // SAFETY: per the contract above.
        let probe = unsafe { Box::from_raw(context.cast::<Probe>()) };
        probe.peer
    }
}

/// Something waiting on a peer's probe.
pub(crate) enum Parked<P> {
    /// A transfer, posted once the peer answers.
    Transfer(P),
    /// An `AddPeer` hello, answered with the verdict.
    Reply(Answer<Result<(), DmaError>>),
}

/// A peer whose probe hasn't answered yet, and what waits on it.
pub(crate) struct Waiting<P> {
    pub(crate) peer: Arc<RegisteredAddress>,
    deadline: Instant,
    posted: bool,
    pub(crate) parked: Vec<Parked<P>>,
}

/// Every peer with a probe out.
pub(crate) struct Probes<P> {
    deadline: Duration,
    waiting: Vec<Waiting<P>>,
}

impl<P> Probes<P> {
    pub(crate) fn new(deadline: Duration) -> Self {
        Self {
            deadline,
            waiting: Vec::new(),
        }
    }

    pub(crate) fn deadline(&self) -> Duration {
        self.deadline
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.waiting.is_empty()
    }

    /// Hold `parked` until `peer` answers, starting its probe if this is the first thing to wait.
    pub(crate) fn park(&mut self, peer: &Arc<RegisteredAddress>, parked: Parked<P>) {
        if let Some(waiting) = self
            .waiting
            .iter_mut()
            .find(|waiting| Arc::ptr_eq(&waiting.peer, peer))
        {
            waiting.parked.push(parked);
            return;
        }
        peer.set_liveness(Liveness::Probing);
        self.waiting.push(Waiting {
            peer: Arc::clone(peer),
            deadline: Instant::now() + self.deadline,
            posted: false,
            parked: vec![parked],
        });
    }

    /// Post every probe not yet posted. Returns how many went out, for the in-flight count, and the
    /// peers whose probe couldn't be posted at all.
    pub(crate) fn post(
        &mut self,
        endpoint: &mut LibfabricEndpoint,
        landing: *mut u8,
    ) -> (usize, Vec<(Waiting<P>, DmaError)>) {
        let mut posted = 0;
        let mut failed = Vec::new();
        let mut index = 0;
        while index < self.waiting.len() {
            let waiting = &mut self.waiting[index];
            if waiting.posted {
                index += 1;
                continue;
            }
            match post_probe(endpoint, landing, &waiting.peer) {
                Ok(true) => {
                    waiting.posted = true;
                    posted += 1;
                    index += 1;
                }
                // The transmit queue is full; the deadline still runs.
                Ok(false) => index += 1,
                Err(error) => failed.push((self.waiting.swap_remove(index), error)),
            }
        }
        (posted, failed)
    }

    /// Take every peer whose probe is past its deadline.
    pub(crate) fn expired(&mut self, now: Instant) -> Vec<Waiting<P>> {
        let mut expired = Vec::new();
        let mut index = 0;
        while index < self.waiting.len() {
            if self.waiting[index].deadline <= now {
                expired.push(self.waiting.swap_remove(index));
            } else {
                index += 1;
            }
        }
        expired
    }

    /// Take `peer`'s wait list, if its probe is still awaited.
    pub(crate) fn answered(&mut self, peer: &Arc<RegisteredAddress>) -> Option<Waiting<P>> {
        let index = self
            .waiting
            .iter()
            .position(|waiting| Arc::ptr_eq(&waiting.peer, peer))?;
        Some(self.waiting.swap_remove(index))
    }

    /// Take every parked transfer `unwanted` picks out, from every peer.
    pub(crate) fn take_parked(&mut self, mut unwanted: impl FnMut(&P) -> bool) -> Vec<P> {
        let mut taken = Vec::new();
        for waiting in &mut self.waiting {
            let mut kept = Vec::with_capacity(waiting.parked.len());
            for parked in waiting.parked.drain(..) {
                match parked {
                    Parked::Transfer(transfer) if unwanted(&transfer) => taken.push(transfer),
                    parked => kept.push(parked),
                }
            }
            waiting.parked = kept;
        }
        taken
    }
}

/// Post one 0-byte read from `peer`'s address 0 with key 0. `Ok(false)` when the transmit queue is
/// full. The read lands nothing, but efa still wants a registered local descriptor, so it points at
/// the worker's quiesce byte.
fn post_probe(
    endpoint: &mut LibfabricEndpoint,
    landing: *mut u8,
    peer: &Arc<RegisteredAddress>,
) -> Result<bool, DmaError> {
    let LocalOperand { descriptor, lease } = endpoint.local_operand(landing, 1, || None)?;
    let probe = Box::into_raw(Box::new(Probe {
        header: OpHeader::new(OpKind::Probe),
        peer: Arc::clone(peer),
        _lease: lease,
    }));
    // SAFETY: the quiesce byte outlives the worker, and a 0-byte read writes none of it.
    let bytes = unsafe { std::slice::from_raw_parts_mut(landing, 0) };
    let code = endpoint
        .connection(peer, 0, 0)
        .post_in(bytes, descriptor, probe.cast());
    if 0 == code {
        return Ok(true);
    }
    // SAFETY: not posted, so the box is still ours.
    drop(unsafe { Box::from_raw(probe) });
    if code == -(FI_EAGAIN as isize) {
        return Ok(false);
    }
    Err(crate::error::fabric_error(code as i32, "liveness probe"))
}
