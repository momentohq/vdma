//! The address vector entries for an endpoint.
//!
//! `fi_av_insert` is keyed by address, but clients can have more than 1 connection per fabric.
//! `fi_av_remove` when all the client_ids referencing it drop.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use dma_libfabric_protocol::DmaError;

use crate::sys::{fi_addr_t, fi_av_insert, fi_av_remove, fid_av};

const MINIMUM_SWEEP: usize = 64;

/// An address vector entry.
///
/// One of these exists per address while any client holds it. It removes the peer address when
/// it is dropped, with the last client to hold a reference.
pub struct RegisteredAddress {
    /// The vector this entry lives in. [`PeerAddresses::clear`] releases every entry before the
    /// endpoint closes the vector, so this lives for as long as an entry exists.
    address_vector: *mut fid_av,
    peer: fi_addr_t,
}

// SAFETY: Only read while live, and the fi_av_remove is safe because of Drop. libfabric
// permits its objects to move between threads when they are not used concurrently.
unsafe impl Send for RegisteredAddress {}
unsafe impl Sync for RegisteredAddress {}

impl RegisteredAddress {
    /// The `fi_addr_t` to post against.
    pub fn handle(&self) -> fi_addr_t {
        self.peer
    }
}

impl std::fmt::Debug for RegisteredAddress {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RegisteredAddress")
            .field("peer", &self.peer)
            .finish()
    }
}

impl Drop for RegisteredAddress {
    fn drop(&mut self) {
        let mut address = self.peer;
        // SAFETY: the last holder is gone, so nothing will post against this entry again, and the
        // endpoint releases every entry before closing the vector.
        let code = unsafe { fi_av_remove(self.address_vector, &raw mut address, 1, 0) };
        if 0 != code {
            tracing::warn!("fi_av_remove failed releasing a peer address: {code}");
        }
    }
}

/// Every address-vector entry this endpoint holds.
#[derive(Debug)]
pub(crate) struct PeerAddresses {
    address_vector: *mut fid_av,
    /// The dedup index, deliberately non-owning: holding `Arc` here would keep every entry alive
    /// forever and nothing would ever be removed.
    by_address: HashMap<Vec<u8>, Weak<RegisteredAddress>>,
    /// The ownership, and so the refcount. A client's entry lives exactly as long as it is here.
    by_client: HashMap<u64, Arc<RegisteredAddress>>,
    /// Index size at which dead weak entries are swept.
    sweep_at: usize,
}

impl PeerAddresses {
    pub(crate) fn new(address_vector: *mut fid_av) -> Self {
        Self {
            address_vector,
            by_address: HashMap::new(),
            by_client: HashMap::new(),
            sweep_at: MINIMUM_SWEEP,
        }
    }

    /// The entry `client_id` posts against, inserting the address if this is its first user.
    ///
    /// A client re-advertising a different address lets go of its old entry here, so the old one is
    /// removed if this client was the last on it, and kept if it was not.
    pub(crate) fn handle(
        &mut self,
        client_id: u64,
        address: &[u8],
    ) -> Result<Arc<RegisteredAddress>, DmaError> {
        if let Some(held) = self.by_client.get(&client_id)
            && self
                .by_address
                .get(address)
                .and_then(Weak::upgrade)
                .is_some_and(|entry| Arc::ptr_eq(&entry, held))
        {
            return Ok(Arc::clone(held));
        }

        let entry = match self.by_address.get(address).and_then(Weak::upgrade) {
            Some(entry) => entry,
            None => {
                let entry = Arc::new(RegisteredAddress {
                    address_vector: self.address_vector,
                    peer: self.insert(address)?,
                });
                self.sweep();
                self.by_address
                    .insert(address.to_vec(), Arc::downgrade(&entry));
                entry
            }
        };
        // Replaces whatever this client held. If that was the last clone of its previous entry,
        // dropping it here removes that entry from the vector.
        self.by_client.insert(client_id, Arc::clone(&entry));
        Ok(entry)
    }

    /// Let go of `client_id`'s entry. The entry leaves the address vector only if this was the last
    /// client on it — which is the whole of the accounting.
    pub(crate) fn release(&mut self, client_id: u64) {
        self.by_client.remove(&client_id);
    }

    /// Release every entry. The endpoint calls this before closing the address vector, since an
    /// entry's `Drop` removes itself from that vector.
    pub(crate) fn clear(&mut self) {
        self.by_client.clear();
        self.by_address.clear();
    }

    fn insert(&self, address: &[u8]) -> Result<fi_addr_t, DmaError> {
        let mut peer: fi_addr_t = 0;
        let inserted = unsafe {
            fi_av_insert(
                self.address_vector,
                address.as_ptr().cast(),
                1,
                &raw mut peer,
                0,
                std::ptr::null_mut(),
            )
        };
        if 1 != inserted {
            return Err(DmaError::Fabric(format!(
                "fi_av_insert inserted {inserted} of 1 addresses"
            )));
        }
        Ok(peer)
    }

    /// Drop index entries whose last client has gone. Without this, an address never seen again
    /// leaves its key behind forever, so a server meeting many hosts grows the index without bound.
    fn sweep(&mut self) {
        if self.by_address.len() < self.sweep_at {
            return;
        }
        self.by_address.retain(|_, entry| 0 < entry.strong_count());
        self.sweep_at = MINIMUM_SWEEP.max(self.by_address.len() * 2);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::{MINIMUM_SWEEP, PeerAddresses};
    use crate::configuration::{Configuration, Provider};
    use crate::endpoint::LibfabricEndpoint;
    use std::sync::Arc;

    /// A real tcp endpoint, since these assertions are about what libfabric's address vector does.
    fn endpoint() -> LibfabricEndpoint {
        let configuration = Configuration {
            providers: vec![Provider::Tcp],
            bind: Some("127.0.0.1".to_string()),
            ..Configuration::default()
        };
        LibfabricEndpoint::open(&configuration, Some("127.0.0.1")).expect("tcp endpoint")
    }

    /// The premise, checked against libfabric rather than assumed: one address is one entry, however
    /// many clients advertise it.
    #[test]
    fn clients_sharing_an_address_share_one_entry() {
        let endpoint = endpoint();
        let address = endpoint.local_address().expect("local address");
        let mut peers = PeerAddresses::new(endpoint.address_vector());

        let first = peers.handle(1, &address).expect("client 1");
        let second = peers.handle(2, &address).expect("client 2");
        assert!(Arc::ptr_eq(&first, &second), "one address, one entry");
        assert_eq!(first.handle(), second.handle());
    }

    /// The crash this exists to prevent: a disconnect must not remove an entry another client is
    /// still posting against.
    #[test]
    fn one_client_leaving_keeps_an_entry_another_still_uses() {
        let endpoint = endpoint();
        let address = endpoint.local_address().expect("local address");
        let mut peers = PeerAddresses::new(endpoint.address_vector());
        let shared = peers.handle(1, &address).expect("client 1");
        peers.handle(2, &address).expect("client 2");

        peers.release(1);

        assert_eq!(
            2,
            Arc::strong_count(&shared),
            "client 2 and this test hold it; the entry must still be in the vector"
        );
    }

    /// And the last one out does remove it, or every client ever seen would hold an entry for the
    /// endpoint's life.
    #[test]
    fn the_last_client_out_removes_the_entry() {
        let endpoint = endpoint();
        let address = endpoint.local_address().expect("local address");
        let mut peers = PeerAddresses::new(endpoint.address_vector());
        peers.handle(1, &address).expect("client 1");
        peers.handle(2, &address).expect("client 2");

        peers.release(1);
        peers.release(2);

        assert!(peers.by_client.is_empty(), "no client holds it");
        assert_eq!(
            0,
            peers
                .by_address
                .get(&address)
                .map_or(0, std::sync::Weak::strong_count),
            "the last release must have dropped the entry, removing it from the vector"
        );
    }

    /// Releasing twice is a no-op rather than a second removal.
    #[test]
    fn releasing_a_client_twice_is_harmless() {
        let endpoint = endpoint();
        let address = endpoint.local_address().expect("local address");
        let mut peers = PeerAddresses::new(endpoint.address_vector());
        let shared = peers.handle(1, &address).expect("client 1");
        peers.handle(2, &address).expect("client 2");

        peers.release(1);
        peers.release(1);

        assert_eq!(
            2,
            Arc::strong_count(&shared),
            "the repeat release must not have taken client 2's entry"
        );
    }

    /// Asking twice for the same address does not stack references, or a client would pin its entry
    /// once per transfer it ever ran.
    #[test]
    fn re_asking_for_the_same_address_holds_one_reference() {
        let endpoint = endpoint();
        let address = endpoint.local_address().expect("local address");
        let mut peers = PeerAddresses::new(endpoint.address_vector());
        let first = peers.handle(1, &address).expect("first ask");
        let again = peers.handle(1, &address).expect("second ask");

        assert!(Arc::ptr_eq(&first, &again));
        // The map's clone plus this test's two.
        assert_eq!(3, Arc::strong_count(&first));
        peers.release(1);
        assert_eq!(2, Arc::strong_count(&first), "one release is enough");
    }

    /// The index does not grow without bound as addresses come and go.
    ///
    /// Driven on the map directly rather than through libfabric: this is a policy about when dead
    /// index keys are dropped, and tcp will not accept the couple of hundred synthetic addresses it
    /// would otherwise take to reach the threshold.
    #[test]
    fn dead_index_entries_are_swept() {
        let mut peers = PeerAddresses::new(std::ptr::null_mut());
        for index in 0..(MINIMUM_SWEEP * 3) {
            // A fresh `Weak` never upgrades, which is exactly the state a departed client leaves.
            peers
                .by_address
                .insert(index.to_ne_bytes().to_vec(), std::sync::Weak::new());
        }

        peers.sweep();

        assert!(
            peers.by_address.is_empty(),
            "dead index keys accumulated: {}",
            peers.by_address.len()
        );
        assert_eq!(
            MINIMUM_SWEEP, peers.sweep_at,
            "the threshold must fall back to its floor once the index empties"
        );
    }

    /// A live index is left alone, or every insert past the threshold would rebuild the map.
    #[test]
    fn sweeping_keeps_live_entries() {
        let endpoint = endpoint();
        let address = endpoint.local_address().expect("local address");
        let mut peers = PeerAddresses::new(endpoint.address_vector());
        peers.handle(1, &address).expect("client 1");

        peers.sweep_at = 0; // force it
        peers.sweep();

        assert_eq!(1, peers.by_address.len(), "a held entry must survive");
    }
}
