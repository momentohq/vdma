//! Per-operand local registrations, cached in [`crate::region_cache`].
//!
//! EFA requires `FI_MR_LOCAL`: every local RMA operand needs a registered descriptor.
//! Freed memory must not keep a registration. An EFA registration is frozen to the
//! physical pages present when it was made.
//!
//! The installed [`crate::region_cache::ReclaimNotifier`] deregisters a covering registration before
//! any page-dropping event. That enables registration caching and leaves reclaiming to the notifier.
//!
//! Registered `FI_READ | FI_WRITE`, never remote-accessible, and the key is never exported, so
//! nothing here is reachable by a peer.

use std::os::raw::c_void;
use std::ptr;

use dma_traits::DmaError;
use libfabric_sys::{FI_READ, FI_WRITE, fi_close, fi_mr_desc, fi_mr_reg, fid_domain, fid_mr};

use crate::error::check;
use crate::region_cache;

/// A local RMA operand's descriptor, plus the registration when this operation owns it.
#[derive(Debug)]
pub(crate) struct LocalOperand {
    pub(crate) descriptor: *mut c_void,
    /// `Some` when this extent isn't cacheable. Held for the operation and closed on completion.
    /// `None` when [`crate::region_cache`] owns the registration.
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

/// Per-endpoint handle for the operand-registration path, deferring registration lifetime to
/// [`crate::region_cache`].
#[derive(Debug)]
pub(crate) struct LocalRegions {
    domain: *mut fid_domain,
}

impl LocalRegions {
    pub(crate) fn new(domain: *mut fid_domain) -> Self {
        Self { domain }
    }

    /// The local descriptor covering `[pointer, pointer + length)`, reusing a cached registration
    /// that already covers it, else registering the exact operand extent. A cached registration is
    /// deregistered as its memory is reclaimed, so it stays valid as long as the operand does —
    /// which spans the transfer, since the transfer retains its operand.
    pub(crate) fn operand(
        &mut self,
        pointer: *mut u8,
        length: usize,
    ) -> Result<LocalOperand, DmaError> {
        let start = pointer as usize;
        let end = start
            .checked_add(length)
            .ok_or_else(|| DmaError::Fabric("local operand range overflows".into()))?;

        let domain = self.domain as usize;
        if let Some(memory_region) = region_cache::covering(start, end, domain) {
            // SAFETY: still tracked, so not yet deregistered, and the operand is live.
            return Ok(LocalOperand {
                descriptor: unsafe { fi_mr_desc(memory_region) },
                registration: None,
            });
        }

        // done before registering so notifier coverage doesn't lag the registration it reclaims
        let cacheable = region_cache::will_track(pointer, length);
        let memory_region = self.register(pointer, length)?;
        // SAFETY: freshly registered above.
        let descriptor = unsafe { fi_mr_desc(memory_region) };
        if !cacheable {
            return Ok(LocalOperand {
                descriptor,
                registration: Some(OperandRegistration(memory_region)),
            });
        }
        region_cache::track(start, end, domain, memory_region);
        Ok(LocalOperand {
            descriptor,
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
    /// domain objects. Also stops a reclaim touching a region whose domain is gone.
    pub(crate) fn clear(&mut self) {
        region_cache::clear_domain(self.domain as usize);
    }
}
