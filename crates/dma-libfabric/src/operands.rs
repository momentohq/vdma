//! The local bytes for a transfer, from the per-op context.

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
