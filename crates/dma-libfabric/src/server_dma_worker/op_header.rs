use crate::sys::fi_context2;
use std::os::raw::c_void;

/// The start of every `op_context` the worker posts: a transfer's
/// [`InFlight`](super::in_flight::InFlight) or a liveness [`Probe`](crate::probe::Probe). Both are
/// `#[repr(C)]` with this first, so the reaper can read the kind through the pointer before it
/// knows which one it holds.
#[repr(C)]
pub(crate) struct OpHeader {
    /// `#[repr(C)]` with this field first gives `&op == &op._context2`, so passing the op's
    /// pointer as `op_context` satisfies `FI_CONTEXT2` and the completion hands the whole struct
    /// back — no separate in-flight table needed. The efa-direct provider owns these 64 bytes for
    /// the op's duration; never read them. Providers without `FI_CONTEXT2` (tcp, rxr efa) ignore
    /// the contents.
    _context2: fi_context2,
    kind: OpKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum OpKind {
    Transfer,
    Probe,
}

impl OpHeader {
    pub(crate) fn new(kind: OpKind) -> Self {
        Self {
            // Provider-owned scratch; it initializes this when the op is posted.
            _context2: unsafe { std::mem::zeroed() },
            kind,
        }
    }

    /// Which kind of op a reaped `op_context` is.
    ///
    /// # Safety
    /// `context` is a live `op_context` this worker posted, not yet reclaimed.
    pub(super) unsafe fn kind_of(context: *mut c_void) -> OpKind {
        // SAFETY: per the contract, it points at an `OpHeader`, and the provider only writes the
        // `_context2` ahead of `kind`.
        unsafe { (*context.cast::<OpHeader>()).kind }
    }
}
