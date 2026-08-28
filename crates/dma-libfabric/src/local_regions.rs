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
use std::sync::Arc;

use crate::sys::{FI_READ, FI_WRITE, fi_mr_desc, fi_mr_reg, fid_domain, fid_mr};
use dma_libfabric_protocol::DmaError;

use crate::error::check;
use crate::region_cache::{self, Registration};

/// A local RMA operand's descriptor and the lease keeping its registration alive.
#[derive(Debug)]
pub(crate) struct LocalOperand {
    pub(crate) descriptor: *mut c_void,
    /// `None` only on providers that need no local registration at all, where the descriptor is
    /// null and there is nothing to hold open.
    pub(crate) lease: Option<Arc<Registration>>,
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
        if let Some(lease) = region_cache::covering(start, end, domain) {
            // SAFETY: the lease holds the region open for as long as this operand lives, so the
            //         descriptor cannot be deregistered underneath the transfer that posts it.
            let descriptor = unsafe { fi_mr_desc(lease.memory_region()) };
            return Ok(LocalOperand {
                descriptor,
                lease: Some(lease),
            });
        }

        // done before registering so notifier coverage doesn't lag the registration it reclaims
        let cacheable = region_cache::will_track(pointer, length);
        // Register the containing extent when the notifier supports it, so the next operand landing
        // nearby reuses this registration instead of taking its own.
        let (base, limit) = region_cache::extent_for(pointer, length);
        // If a widened registration fails, take the operand's extent instead
        let region = match self.register(base as *mut u8, limit - base) {
            Ok(region) => Ok((base, limit, region)),
            Err(_) if (base, limit) != (start, end) => self
                .register(pointer, length)
                .map(|region| (start, end, region)),
            Err(error) => Err(error),
        };
        let (base, limit, region) = region?;
        let lease = Registration::new(region);
        // SAFETY: freshly registered above, and held by `lease`.
        let descriptor = unsafe { fi_mr_desc(lease.memory_region()) };
        if cacheable {
            // The registry takes its own lease; this operand keeps the one it already holds.
            region_cache::track(base, limit, domain, &lease);
        }
        Ok(LocalOperand {
            descriptor,
            lease: Some(lease),
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
