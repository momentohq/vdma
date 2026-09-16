//! The local bytes for a transfer, from the per-op context.

/// A span of your memory whose `fi_mr_reg` may outlive one transfer and serve later operands.
///
/// Registering pins physical pages. You must [`crate::invalidate`] when any of it is about
/// to be reclaimed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheableSpan {
    /// First address of the span.
    pub base: usize,
    /// One past the last address.
    pub end: usize,
}

impl CacheableSpan {
    /// The span of `length` bytes at `base`, or `None` if that runs past the address space.
    pub fn new(base: usize, length: usize) -> Option<Self> {
        Some(Self {
            base,
            end: base.checked_add(length)?,
        })
    }

    pub(crate) fn contains(&self, start: usize, end: usize) -> bool {
        self.base <= start && end <= self.end
    }
}

/// The local end of a transfer. Your context owns the bytes but the worker owns your context until
/// the completion.
///
/// Both directions default to unsupported, so a context only needs to implement the half it serves.
/// A transfer whose direction has no operand fails.
pub trait Operands {
    /// The bytes for a [`ToPeer`](crate::Direction::ToPeer) transfer.
    fn source(&self) -> Option<&[u8]> {
        None
    }

    /// The buffer for a [`FromPeer`](crate::Direction::FromPeer) transfer, at least as long as
    /// the requested length. A shorter buffer fails the transfer, and the tail of a longer buffer
    /// is untouched.
    fn allocate(&mut self, _length: usize) -> Option<&mut [u8]> {
        None
    }

    /// The span around this transfer's operand whose registration may be cached and reused.
    ///
    /// `fi_mr_reg` is expensive and a registration is frozen to the physical pages present when it
    /// was taken, so caching one is only sound if someone reports the pages going away.
    ///
    /// If you return `Some`, you promise to call [`crate::invalidate`] over the span before any of
    /// its pages are unmapped, decommitted, or madvised away. Widening it past the operand lets
    /// neighboring operands share the one registration, at the cost of pinning the whole span.
    ///
    /// Called on the worker, after the operand resolves, and only when no cached registration
    /// already covers the operand, so per-extent setup here costs once per registration. A span not
    /// containing the operand is ignored — registering it would pin the wrong memory and leave the
    /// operand unregistered.
    ///
    /// `None`, the default, registers the operand for this transfer and closes it at completion.
    /// Memory registered through [`crate::FabricService::register`] doesn't need this treatment.
    /// Its span is already cached, and the operand resolves out of it either way.
    fn cacheable_span(&self) -> Option<CacheableSpan> {
        None
    }
}

/// A `Vec<u8>` can be a whole context on its own. It provides its own bytes, and takes a `FromPeer`
/// transfer into itself, so the completion hands it back filled.
impl Operands for Vec<u8> {
    fn source(&self) -> Option<&[u8]> {
        Some(self.as_slice())
    }

    fn allocate(&mut self, length: usize) -> Option<&mut [u8]> {
        self.resize(length, 0);
        Some(self.as_mut_slice())
    }
}
