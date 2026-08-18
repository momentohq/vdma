//! A process-global cache of fi_mr_reg operand registrations
//!
//! A registration pins physical pages when it was taken. It goes stale when those pages are
//! `madvise`d away, decommitted, or unmapped. The [`ReclaimNotifier`]  drives [`invalidate`]
//! of pages before they go stale.
//!
//! Entries are grouped by domain. Reclaim, by contrast, deregisters across  every domain.
//!
//! Registrations are local `FI_READ | FI_WRITE`.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use crate::sys::{fi_close, fid_mr};

/// Your promise that you will call [`invalidate`] over any range whose pages you are about to
/// be reclaim. Required for `fi_mr_reg` caching.
pub trait ReclaimNotifier: Send + Sync {
    /// Can this notifier guarantee it will report a reclaim of `[pointer, pointer + length)`?
    /// `false` uses an explicit registration rather than leaking one.
    fn covers(&self, pointer: *mut u8, length: usize) -> bool;
}

static NOTIFIER: OnceLock<&'static dyn ReclaimNotifier> = OnceLock::new();

/// Install the reclaim notifier, enabling the `fi_mr_reg` cache.
///
/// Optionally, call once at startup.
pub fn install_reclaim_notifier(notifier: &'static dyn ReclaimNotifier) {
    assert!(
        NOTIFIER.set(notifier).is_ok(),
        "reclaim notifier is installed at most once per process"
    );
}

/// Whether a registration of this extent may be cached. `false` means register per operation.
pub(crate) fn will_track(pointer: *mut u8, length: usize) -> bool {
    NOTIFIER
        .get()
        .is_some_and(|notifier| notifier.covers(pointer, length))
}

static REGISTRY: Mutex<Vec<DomainRegions>> = Mutex::new(Vec::new());

struct DomainRegions {
    domain: usize,
    regions: BTreeMap<usize, RegionEntry>,
}

struct RegionEntry {
    end: usize,
    memory_region: *mut fid_mr,
}

// SAFETY: the `fid_mr` is `fi_close`d once (here) or read as a descriptor by its owning endpoint;
// the registry lock serializes structural access.
unsafe impl Send for RegionEntry {}

/// Record a registered operand extent so [`invalidate`] deregisters it on reclaim and the RMA path
/// can reuse it.
pub(crate) fn track(base: usize, end: usize, domain: usize, memory_region: *mut fid_mr) {
    let replaced = {
        let mut registry = lock();
        domain_regions(&mut registry, domain).insert(base, RegionEntry { end, memory_region })
    };
    // A prior registration at this base+domain should already be gone; close it if not, so
    // reregistration doesn't leak it.
    if let Some(old) = replaced {
        // SAFETY: closed once, and this old region is superseded by this region
        unsafe { fi_close(&mut (*old.memory_region).fid) };
    }
}

/// The memory region of a live registration on `domain` containing `[start, end)`
pub(crate) fn covering(start: usize, end: usize, domain: usize) -> Option<*mut fid_mr> {
    let registry = lock();
    let regions = &registry
        .iter()
        .find(|entry| entry.domain == domain)?
        .regions;
    regions
        .range(..=start)
        .next_back()
        .filter(|(base, entry)| **base <= start && end <= entry.end)
        .map(|(_, entry)| entry.memory_region)
}

/// Deregister every tracked extent, on any domain, overlapping `[start, end)`. The [`ReclaimNotifier`]
/// calls this before the pages are reclaimed. Removes under the lock, releases it, then `fi_close`s.
/// `ibv_dereg_mr` must not run while the registry is locked, or a re-entrant free would deadlock.
pub fn invalidate(start: usize, end: usize) {
    let stale: Vec<*mut fid_mr> = {
        let mut registry = lock();
        let mut stale = Vec::new();
        for domain in registry.iter_mut() {
            let bases: Vec<usize> = domain
                .regions
                .range(..end)
                .filter(|(_, entry)| start < entry.end)
                .map(|(&base, _)| base)
                .collect();
            for base in bases {
                if let Some(entry) = domain.regions.remove(&base) {
                    stale.push(entry.memory_region);
                }
            }
        }
        stale
    };
    for memory_region in stale {
        // SAFETY: closed once; the pages are being freed, and a transfer retains its operand, so no
        // in-flight RMA is using them.
        unsafe { fi_close(&mut (*memory_region).fid) };
    }
}

/// Close every region on `domain`, called by an endpoint before closing that domain (regions are
/// domain objects). Dropping them from the registry stops a later reclaim from driving the cache
/// into a region whose domain is gone. Scoped to one domain so tearing down a worker leaves the
/// others' regions alone.
pub(crate) fn clear_domain(domain: usize) {
    let regions: Vec<*mut fid_mr> = {
        let mut registry = lock();
        match registry.iter().position(|entry| entry.domain == domain) {
            Some(index) => registry
                .remove(index)
                .regions
                .into_values()
                .map(|entry| entry.memory_region)
                .collect(),
            None => Vec::new(),
        }
    };
    for memory_region in regions {
        // SAFETY: closed once; the endpoint has stopped serving, so nothing is using it.
        unsafe { fi_close(&mut (*memory_region).fid) };
    }
}

/// The registrations for `domain`, empty on first use.
fn domain_regions(
    registry: &mut Vec<DomainRegions>,
    domain: usize,
) -> &mut BTreeMap<usize, RegionEntry> {
    let index = match registry.iter().position(|entry| entry.domain == domain) {
        Some(index) => index,
        None => {
            registry.push(DomainRegions {
                domain,
                regions: BTreeMap::new(),
            });
            registry.len() - 1
        }
    };
    &mut registry[index].regions
}

fn lock() -> std::sync::MutexGuard<'static, Vec<DomainRegions>> {
    REGISTRY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
