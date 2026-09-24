//! The address vector entries for an endpoint.
//!
//! `fi_av_insert` is keyed by address, but clients can have more than 1 connection per fabric.
//! `fi_av_remove` when all the client_ids referencing it drop.
//!
//! On efa an address is not an identifier - it's a _partially-significant tuple_, with data
//! wedged into some of its bytes. There is an endpoint slot the device reuses that bleeds into
//! the "address," so entries on efa must be keyed by what identifies the slot rather than by
//! the "address." See [`PeerAddress`].

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Weak};

use dma_libfabric_protocol::DmaError;

use crate::sys::{FI_ADDR_EFA, fi_addr_t, fi_av_insert, fi_av_remove, fid_av};

const MINIMUM_SWEEP: usize = 64;

/// `sizeof(struct efa_ep_addr)`: `raw[16]`, `qpn: u16`, `pad: u16`, `qkey: u32`, `next: ptr`.
const EFA_ADDRESS_LEN: usize = 32;
/// The GID and the QPN, which is how the efa provider chose to key its reverse address vector.
const EFA_IDENTITY_LEN: usize = 18;

/// How the provider lays out an address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AddressFormat {
    /// The device cares about its GID and QPN, and it hands a closed endpoint's QPN to the next
    /// endpoint on the card. So GID + QPN is the actual slot, and the QKEY only tells its epochs
    /// apart.
    Efa,
    /// The address is the address.
    Opaque,
}

impl AddressFormat {
    /// From `fi_info.addr_format`.
    pub(crate) fn from_fi(addr_format: u32) -> Self {
        if FI_ADDR_EFA == addr_format {
            Self::Efa
        } else {
            Self::Opaque
        }
    }

    /// How many leading bytes of an address name its endpoint slot.
    fn identity_len(self, bytes: &[u8]) -> Result<usize, DmaError> {
        match self {
            // guards against a `fi_av_insert` reading something arbitrary in case of a naughty short address
            Self::Efa if EFA_ADDRESS_LEN != bytes.len() => Err(DmaError::Fabric(format!(
                "efa address is {} bytes, not {EFA_ADDRESS_LEN}",
                bytes.len()
            ))),
            Self::Efa => Ok(EFA_IDENTITY_LEN),
            Self::Opaque => Ok(bytes.len()),
        }
    }
}

/// A peer endpoint's advertised address. Two are equal when they refer to the same endpoint slot, which on efa
/// ignores the QKEY. The provider cannot hold both in one address vector, and the one advertised earlier is dead,
/// or the device would not have handed its QPN out again.
///
/// A borrow is over a slice of `::identity()` rather than the whole thing. If you want to use the bytes with
/// `fi_av_insert` you need to use `::bytes()` explicitly.
#[derive(Debug)]
struct PeerAddress {
    bytes: Vec<u8>,
    identity_len: usize,
}

impl PeerAddress {
    fn new(format: AddressFormat, bytes: &[u8]) -> Result<Self, DmaError> {
        Ok(Self {
            bytes: bytes.to_vec(),
            identity_len: format.identity_len(bytes)?,
        })
    }

    /// The bytes `fi_av_insert` takes.
    fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn identity(&self) -> &[u8] {
        &self.bytes[..self.identity_len]
    }
}

// Lookups borrow the identity slice of an advertised address rather than building a key, since one
// happens per transfer. `Eq` and `Hash` below are over that same slice, as `Borrow` requires.
impl Borrow<[u8]> for PeerAddress {
    fn borrow(&self) -> &[u8] {
        self.identity()
    }
}

impl PartialEq for PeerAddress {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}

impl Eq for PeerAddress {}

impl Hash for PeerAddress {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.identity().hash(state);
    }
}

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
    format: AddressFormat,
    /// The dedup index, keyed by endpoint slot, and deliberately non-owning.
    by_address: HashMap<PeerAddress, Weak<RegisteredAddress>>,
    /// The ownership, and so the refcount. A client's entry lives exactly as long as it is here.
    by_client: HashMap<u64, Arc<RegisteredAddress>>,
    /// Index size at which dead weak entries are swept.
    sweep_at: usize,
}

impl PeerAddresses {
    pub(crate) fn new(address_vector: *mut fid_av, format: AddressFormat) -> Self {
        Self {
            address_vector,
            format,
            by_address: HashMap::new(),
            by_client: HashMap::new(),
            sweep_at: MINIMUM_SWEEP,
        }
    }

    /// The entry `client_id` posts against, inserting the address if this is its first user.
    ///
    /// A client re-advertising a different address lets go of its old entry here, so the old one is
    /// removed if this client was the last on it, and kept if it was not. An address that supersedes
    /// an entry evicts it.
    pub(crate) fn handle(
        &mut self,
        client_id: u64,
        address: &[u8],
    ) -> Result<Arc<RegisteredAddress>, DmaError> {
        let identity = &address[..self.format.identity_len(address)?];

        // What is held for this slot, and is it the actual address or not?
        let held = self
            .by_address
            .get_key_value(identity)
            .and_then(|(known, entry)| {
                entry
                    .upgrade()
                    .map(|entry| (known.bytes() == address, entry))
            });
        match held {
            Some((true, entry)) => {
                // Replaces whatever this client held. If that was the last clone of its previous
                // entry, dropping it here removes that entry from the vector.
                self.by_client.insert(client_id, Arc::clone(&entry));
                return Ok(entry);
            }
            // Same "slot," different bytes, for providers that mix "slots" with identification. Like efa.
            Some((false, stale)) => {
                let holders = self.by_client.len();
                self.by_client.retain(|_, held| !Arc::ptr_eq(held, &stale));
                let evicted = holders - self.by_client.len();
                tracing::warn!(
                    "client {client_id} advertised a recycled id; released the stale endpoint and {evicted} clients"
                );
                // `stale` is the last clone, and dropping it here is the removal.
            }
            None => {}
        }
        // This client's previous entry drops before its new one comes in.
        self.by_client.remove(&client_id);
        // `insert` on an equal key keeps the old value, so remove explicitly.
        self.by_address.remove(identity);

        let key = PeerAddress::new(self.format, address)?;
        let entry = Arc::new(RegisteredAddress {
            address_vector: self.address_vector,
            peer: self.insert(key.bytes())?,
        });
        self.sweep();
        self.by_address.insert(key, Arc::downgrade(&entry));
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
    use super::{AddressFormat, EFA_ADDRESS_LEN, MINIMUM_SWEEP, PeerAddress, PeerAddresses};
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
        let mut peers = PeerAddresses::new(endpoint.address_vector(), AddressFormat::Opaque);

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
        let mut peers = PeerAddresses::new(endpoint.address_vector(), AddressFormat::Opaque);
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
        let mut peers = PeerAddresses::new(endpoint.address_vector(), AddressFormat::Opaque);
        peers.handle(1, &address).expect("client 1");
        peers.handle(2, &address).expect("client 2");

        peers.release(1);
        peers.release(2);

        assert!(peers.by_client.is_empty(), "no client holds it");
        let key = PeerAddress::new(AddressFormat::Opaque, &address).expect("opaque takes anything");
        assert_eq!(
            0,
            peers
                .by_address
                .get(&key)
                .map_or(0, std::sync::Weak::strong_count),
            "the last release must have dropped the entry, removing it from the vector"
        );
    }

    /// Releasing twice is a no-op rather than a second removal.
    #[test]
    fn releasing_a_client_twice_is_harmless() {
        let endpoint = endpoint();
        let address = endpoint.local_address().expect("local address");
        let mut peers = PeerAddresses::new(endpoint.address_vector(), AddressFormat::Opaque);
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
        let mut peers = PeerAddresses::new(endpoint.address_vector(), AddressFormat::Opaque);
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
        let mut peers = PeerAddresses::new(std::ptr::null_mut(), AddressFormat::Opaque);
        for index in 0..(MINIMUM_SWEEP * 3) {
            let key = PeerAddress::new(AddressFormat::Opaque, &index.to_ne_bytes())
                .expect("opaque takes anything");
            // A fresh `Weak` never upgrades, which is exactly the state a departed client leaves.
            peers.by_address.insert(key, std::sync::Weak::new());
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
        let mut peers = PeerAddresses::new(endpoint.address_vector(), AddressFormat::Opaque);
        peers.handle(1, &address).expect("client 1");

        peers.sweep_at = 0; // force it
        peers.sweep();

        assert_eq!(1, peers.by_address.len(), "a held entry must survive");
    }

    /// An `efa_ep_addr` with a fixed GID.
    fn efa_address(qpn: u16, qkey: u32) -> Vec<u8> {
        let mut bytes = vec![0; EFA_ADDRESS_LEN];
        bytes[..16].fill(0xfe);
        bytes[16..18].copy_from_slice(&qpn.to_ne_bytes());
        bytes[20..24].copy_from_slice(&qkey.to_ne_bytes());
        bytes
    }

    /// The two addresses from the open issue: one card, one QPN, a new QKEY. One key, and a lookup
    /// by either one's identity finds it.
    #[test]
    fn efa_addresses_differing_only_in_qkey_are_one_slot() {
        let old =
            PeerAddress::new(AddressFormat::Efa, &efa_address(0, 0x9bce1061)).expect("32 bytes");
        let new =
            PeerAddress::new(AddressFormat::Efa, &efa_address(0, 0x961f0203)).expect("32 bytes");
        assert_eq!(old, new);
        assert_ne!(old.bytes(), new.bytes());

        let mut index = std::collections::HashMap::new();
        index.insert(old, ());
        assert!(index.contains_key(new.identity()));
    }

    #[test]
    fn efa_addresses_with_different_qpns_are_different_slots() {
        let one =
            PeerAddress::new(AddressFormat::Efa, &efa_address(0, 0x9bce1061)).expect("32 bytes");
        let other =
            PeerAddress::new(AddressFormat::Efa, &efa_address(1, 0x9bce1061)).expect("32 bytes");

        assert_ne!(one, other);
    }

    /// `fi_av_insert` reads 32 bytes whatever was advertised, so anything else must stop here.
    #[test]
    fn efa_rejects_any_length_but_the_providers() {
        assert!(PeerAddress::new(AddressFormat::Efa, &[0; EFA_ADDRESS_LEN - 1]).is_err());
        assert!(PeerAddress::new(AddressFormat::Efa, &[0; EFA_ADDRESS_LEN + 1]).is_err());
        assert!(PeerAddress::new(AddressFormat::Efa, &[0; EFA_ADDRESS_LEN]).is_ok());
    }

    /// No slot to share on other providers: every byte is the identity.
    #[test]
    fn opaque_addresses_compare_every_byte() {
        let one = PeerAddress::new(AddressFormat::Opaque, &[1, 2, 3]).expect("any length");
        let other = PeerAddress::new(AddressFormat::Opaque, &[1, 2, 4]).expect("any length");

        assert_ne!(one, other);
    }

    /// The open issue itself: efa-direct's reverse address vector cannot hold two entries for one
    /// (GID, QPN). Unfixed, the second insert lands beside the first: a debug libfabric aborts the
    /// binary on its own assertion in `efa_av_reverse_av_add`, and a release one carries on with
    /// the table wrong, which only the eviction assertion below catches. The second incarnation is
    /// forged by flipping a QKEY byte, since `fi_av_insert` never contacts the peer.
    #[test]
    #[ignore = "needs an efa device"]
    fn a_recycled_queue_pair_evicts_its_dead_incarnation() {
        let configuration = Configuration {
            providers: vec![Provider::EfaDirect],
            ..Configuration::default()
        };
        let endpoint = LibfabricEndpoint::open(&configuration, None).expect("efa-direct endpoint");
        let format = AddressFormat::from_fi(endpoint.describe().addr_format);
        assert_eq!(AddressFormat::Efa, format);
        let mut peers = PeerAddresses::new(endpoint.address_vector(), format);

        let old = endpoint.local_address().expect("local address");
        let mut new = old.clone();
        new[20] ^= 1;

        peers.handle(1, &old).expect("first incarnation");
        peers.handle(2, &old).expect("client 2 shares it");
        // Held only by the map, as in production: `post` drops its clone before the op completes.
        let second = peers
            .handle(1, &new)
            .expect("second incarnation, with the first removed first");

        assert_eq!(
            vec![&1],
            peers.by_client.keys().collect::<Vec<_>>(),
            "client 2 was posting at a dead endpoint and must have been evicted"
        );
        assert_eq!(2, Arc::strong_count(&second), "the map and this test");
        peers.release(1);
    }
}
