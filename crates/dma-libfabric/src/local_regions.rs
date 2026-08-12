//! Per-operand local registrations, cached in a process-global registry so the RMA hot path rarely
//! calls `fi_mr_reg` yet page decay can stay ON.
//!
//! EFA requires `FI_MR_LOCAL`: every local RMA operand needs a registered descriptor, and
//! registering per op costs ~250µs and serializes the single worker. So each operand's extent is
//! registered once and its descriptor reused for as long as that memory lives. Freed memory must not
//! keep a registration: an EFA registration is frozen to the physical pages present when it was made
//! — `FI_MR_ALLOCATED`, empty `*_odp_caps`, no on-demand paging — so once jemalloc `madvise`s or
//! reuses those pages the network card would read or write the wrong bytes.
//!
//! [`crate::extent_hooks`] deregisters a covering registration before any
//! page-dropping event. That lets this register the exact operand extent rather than a wide window
//! and leave reclaim to the hooks, so decay stays on, RSS stays bounded, and no registration
//! outlives its pages. The registrations live in the global registry keyed by base address; this
//! type carries only the domain to register against.
//!
//! Registered `FI_READ | FI_WRITE`, never remote-accessible, and the key is never exported, so
//! nothing here is reachable by a peer.

use std::os::raw::c_void;
use std::ptr;

use dma_traits::DmaError;
use libfabric_sys::{FI_READ, FI_WRITE, fi_close, fi_mr_desc, fi_mr_reg, fid_domain, fid_mr};

use crate::error::check;
use crate::extent_hooks::{self, RegionMode};

/// A local RMA operand's descriptor, plus the registration when this operation owns it.
#[derive(Debug)]
pub(crate) struct LocalOperand {
    pub(crate) descriptor: *mut c_void,
    /// `Some` under [`RegionMode::PerOperation`]. Held for the operation and closed on completion;
    /// `None` when the registry owns the registration and the extent hooks reclaim it.
    pub(crate) registration: Option<OperandRegistration>,
}

/// A registration belonging to one operation.
#[derive(Debug)]
pub(crate) struct OperandRegistration(*mut fid_mr);

// SAFETY: travels to the fabric worker with its op and is closed there, never used concurrently —
// which is all libfabric requires of an object crossing threads.
unsafe impl Send for OperandRegistration {}

impl Drop for OperandRegistration {
    fn drop(&mut self) {
        // SAFETY: the op has completed, so nothing holds this descriptor; closed once.
        unsafe { fi_close(&mut (*self.0).fid) };
    }
}

/// Per-endpoint handle for the operand-registration path, deferring registration lifetime to the
/// global registry and its extent hooks.
#[derive(Debug)]
pub(crate) struct LocalRegions {
    domain: *mut fid_domain,
}

impl LocalRegions {
    pub(crate) fn new(domain: *mut fid_domain) -> Self {
        Self { domain }
    }

    /// The local descriptor covering `[pointer, pointer + length)`, reusing a cached registration
    /// that already covers it, else registering the exact operand extent. The extent hooks
    /// deregister it when jemalloc reclaims the memory, so it stays valid as long as the operand
    /// does — which spans the transfer, since the transfer retains its operand.
    pub(crate) fn operand(
        &mut self,
        pointer: *mut u8,
        length: usize,
    ) -> Result<LocalOperand, DmaError> {
        let start = pointer as usize;
        let end = start
            .checked_add(length)
            .ok_or_else(|| DmaError::Fabric("local operand range overflows".into()))?;

        if RegionMode::PerOperation == extent_hooks::mode() {
            let memory_region = self.register(pointer, length)?;
            let registration = OperandRegistration(memory_region);
            // SAFETY: freshly registered, and held by the returned `OperandRegistration`.
            let descriptor = unsafe { fi_mr_desc(memory_region) };
            return Ok(LocalOperand {
                descriptor,
                registration: Some(registration),
            });
        }

        let domain = self.domain as usize;
        if let Some(memory_region) = extent_hooks::covering(start, end, domain) {
            // SAFETY: still tracked, so not yet deregistered, and the operand is live.
            return Ok(LocalOperand {
                descriptor: unsafe { fi_mr_desc(memory_region) },
                registration: None,
            });
        }

        // Register the operand's extent, first making sure its arena carries the deregister-on-free
        // hooks. Without that, frees there would skip deregistration and the registration would go
        // stale as the pages are reused.
        extent_hooks::ensure_arena_hooked(pointer);
        let memory_region = self.register(pointer, length)?;
        extent_hooks::track(start, end, domain, memory_region);
        // SAFETY: freshly registered above.
        Ok(LocalOperand {
            descriptor: unsafe { fi_mr_desc(memory_region) },
            registration: None,
        })
    }

    fn register(&self, pointer: *mut u8, length: usize) -> Result<*mut fid_mr, DmaError> {
        let mut memory_region: *mut fid_mr = ptr::null_mut();
        // A root span, so first-touch registration cost is visible.
        let _span = tracing::info_span!(parent: None, "region_register", bytes = length).entered();
        check(
            unsafe {
                fi_mr_reg(
                    self.domain,
                    pointer as *const c_void,
                    length,
                    u64::from(FI_READ | FI_WRITE),
                    0,
                    0,
                    0,
                    &mut memory_region,
                    ptr::null_mut(),
                )
            },
            "fi_mr_reg(operand)",
        )?;
        Ok(memory_region)
    }

    /// Close this domain's registrations before the endpoint closes it, since memory regions are
    /// domain objects. Also stops the hooks touching a region whose domain is gone.
    pub(crate) fn clear(&mut self) {
        extent_hooks::clear_domain(self.domain as usize);
    }
}
