use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::endpoint::LibfabricEndpoint;

/// Outstanding (queued, parked or in-flight) ops per client, and clients whose peer to remove once
/// that count hits 0. Counting from intake rather than posting stops a removal slipping between a
/// queued transfer and its post, where the post would re-insert the peer just removed. Deferring
/// `fi_av_remove` to 0 also keeps it from flushing a live queue pair — the failure this prevents.
pub(super) struct Ledger<'counter> {
    by_client: HashMap<u64, usize>,
    pending_removal: HashSet<u64>,
    /// The device-wide tally `submit` raised.
    outstanding: &'counter AtomicUsize,
}

impl<'counter> Ledger<'counter> {
    pub(super) fn new(outstanding: &'counter AtomicUsize) -> Self {
        Self {
            by_client: HashMap::new(),
            pending_removal: HashSet::new(),
            outstanding,
        }
    }

    /// Count an admitted op against its client.
    pub(super) fn admit(&mut self, client_id: u64) {
        *self.by_client.entry(client_id).or_insert(0) += 1;
    }

    /// Account an op that finished before it was ever counted against a client.
    pub(super) fn release_uncounted(&self) {
        release_outstanding(self.outstanding);
    }

    /// Remove a disconnected client's peer, or defer until its last op finishes. Removing with ops
    /// outstanding would flush the queue pair, and a queued transfer would re-insert the peer.
    pub(super) fn remove_or_defer(&mut self, endpoint: &mut LibfabricEndpoint, client_id: u64) {
        if 0 == self.by_client.get(&client_id).copied().unwrap_or(0) {
            endpoint.release_peer(client_id);
        } else {
            self.pending_removal.insert(client_id);
        }
    }

    /// Account one finished op, reaped or failed unposted, against its client, performing a deferred
    /// peer removal when its last op drains.
    pub(super) fn finish(&mut self, endpoint: &mut LibfabricEndpoint, client_id: u64) {
        release_outstanding(self.outstanding);
        let remaining = match self.by_client.get_mut(&client_id) {
            Some(count) => {
                *count -= 1;
                *count
            }
            None => 0,
        };
        if 0 == remaining {
            self.by_client.remove(&client_id);
            if self.pending_removal.remove(&client_id) {
                endpoint.release_peer(client_id);
            }
        }
    }
}

/// Drop one op from the device-wide tally `submit` raised.
fn release_outstanding(outstanding: &AtomicUsize) {
    // gotta saturate so it never sees a spurious wrap state
    outstanding.update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        current.saturating_sub(1)
    });
}
