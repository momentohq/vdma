//! Preregistered memory.
//!
//! Registering memory allows operands inside the span resolve as a plain `covering()` hits,
//! so no dynamic memory region registration is used.

use std::sync::Arc;

use crate::region_cache;

/// Your registered memory.
///
/// Cheap to clone. Hold a clone in the caller context of every transfer whose operand lives here —
/// that is what keeps the storage mapped for the length of the transfer.
///
/// Operands are read through [`Self::storage`], so the [`crate::Operands::source`] direction needs
/// nothing extra.
/// [`crate::Operands::allocate`] is special though. Inferring a `&mut [u8]` from a clone is not
/// sound, because this crate does not know which ranges you consider disjoint and deliberately does
/// not track them. That requirement lives inside `S`, with the allocation policy that created it.
pub struct MemoryRegion<S> {
    inner: Arc<Region<S>>,
}

/// The owned storage and the span registered for it. Dropping the last clone retires the
/// registration and then frees the storage.
struct Region<S> {
    /// Boxed so the span stays put: a `Vec<u8>`'s bytes survive a move, but an `[u8; N]` by value
    /// does not, and the registration names an address.
    storage: Box<S>,
    base: usize,
    end: usize,
}

impl<S> MemoryRegion<S> {
    pub(crate) fn new(storage: Box<S>, base: usize, end: usize) -> Self {
        Self {
            inner: Arc::new(Region { storage, base, end }),
        }
    }

    /// Your memory container
    pub fn storage(&self) -> &S {
        &self.inner.storage
    }

    /// Registered length in bytes
    pub fn length(&self) -> usize {
        self.inner.end - self.inner.base
    }
}

/// Hand-written so `S` need not be `Clone`: the storage is shared, not copied.
impl<S> Clone for MemoryRegion<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// `S` is deliberately not shown: it is the caller's container and may be enormous.
impl<S> std::fmt::Debug for MemoryRegion<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemoryRegion")
            .field("base", &self.inner.base)
            .field("length", &self.length())
            .field("holders", &Arc::strong_count(&self.inner))
            .finish()
    }
}

impl<S> Drop for Region<S> {
    fn drop(&mut self) {
        // Retire the registration before the storage frees. `invalidate` removes the entry and drops
        // the registry's lease; no operand can still hold one, because a transfer's caller context
        // holds a `MemoryRegion` clone and so this cannot run mid-flight. Fields drop after this
        // body, so deregistration always precedes the unmap — which matters because EFA has no
        // on-demand paging and `ibv_dereg_mr` over freed pages is illegal.
        //
        // If the server went first, its endpoint already closed this domain's regions via
        // `clear_domain`, and this finds nothing.
        region_cache::invalidate(self.base, self.end);
    }
}
