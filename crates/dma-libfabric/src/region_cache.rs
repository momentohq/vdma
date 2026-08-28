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
use std::sync::{Arc, Mutex, OnceLock};

use crate::sys::{fi_close, fid_mr};

/// A live `fi_mr_reg`, deregistered when the last holder drops it.
///
/// Every operand posted against a cached region holds a clone for the length of its transfer.
/// That lets [`invalidate`] run the moment pages are reclaimed. By dropping the
/// registry's clone, the entry becomes unreachable immediately and nothing new can bind to it,
/// while a transfer already on the wire keeps the region open until it completes. Deregistration
/// happens once, when the last lease goes.
///
/// The reclaimed pages themselves are never a live operand — a transfer retains its own operand for
/// its whole duration — so a lease outliving an `invalidate` keeps mapping memory that is still
/// there. What it must not do is die early underneath a posted descriptor.
pub(crate) struct Registration {
    memory_region: *mut fid_mr,
}

// SAFETY: the `fid_mr` is only read as a descriptor, which libfabric permits from any thread, and
// closed once the last `Arc` drops. Nothing mutates it after registration.
unsafe impl Send for Registration {}
unsafe impl Sync for Registration {}

impl Registration {
    /// Take ownership of a freshly registered region.
    pub(crate) fn new(memory_region: *mut fid_mr) -> Arc<Self> {
        Arc::new(Self { memory_region })
    }

    pub(crate) fn memory_region(&self) -> *mut fid_mr {
        self.memory_region
    }
}

impl std::fmt::Debug for Registration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Registration")
            .field("memory_region", &self.memory_region)
            .finish()
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        // SAFETY: this runs only when the last lease is gone, so no descriptor is in use.
        unsafe { fi_close(&mut (*self.memory_region).fid) };
    }
}

/// Your promise that you will call [`invalidate`] over any range whose pages you are about to
/// be reclaim. Required for `fi_mr_reg` caching.
pub trait ReclaimNotifier: Send + Sync {
    /// Can this notifier guarantee it will report a reclaim of `[pointer, pointer + length)`?
    /// `false` uses an explicit registration rather than leaking one.
    fn covers(&self, pointer: *mut u8, length: usize) -> bool;

    /// The allocator extent containing `[pointer, pointer + length)`.
    ///
    /// Answering this widens a registration from an exact operand to a whole extent. This lets
    /// many operands share one `fi_mr_reg` instead of taking one each. The returned range must
    /// be mapped in its entirety and must be reported to [`invalidate`] as a whole when
    /// reclaimed, since registering it pins every page in it.
    ///
    /// `None` registers the operand's extent only, which is always correct, but not always fast.
    fn extent_of(&self, pointer: *mut u8, length: usize) -> Option<(usize, usize)> {
        let _ = (pointer, length);
        None
    }
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

/// The extent to register for this operand.
pub(crate) fn extent_for(pointer: *mut u8, length: usize) -> (usize, usize) {
    let start = pointer as usize;
    let operand = (start, start.saturating_add(length));
    NOTIFIER
        .get()
        .and_then(|notifier| notifier.extent_of(pointer, length))
        // An extent not containing the operand would not work
        .filter(|(base, end)| *base <= operand.0 && operand.1 <= *end)
        .unwrap_or(operand)
}

static REGISTRY: Mutex<Vec<DomainRegions>> = Mutex::new(Vec::new());

struct DomainRegions {
    domain: usize,
    regions: BTreeMap<usize, RegionEntry>,
}

struct RegionEntry {
    end: usize,
    registration: Arc<Registration>,
}

/// Record a registered operand extent so [`invalidate`] deregisters it on reclaim and the RMA path
/// can reuse it. The registry holds a lease, and the caller keeps its own.
pub(crate) fn track(base: usize, end: usize, domain: usize, registration: &Arc<Registration>) {
    let replaced = {
        let mut registry = lock();
        domain_regions(&mut registry, domain).insert(
            base,
            RegionEntry {
                end,
                registration: Arc::clone(registration),
            },
        )
    };
    // Any prior registration at this base+domain is superseded. Dropped outside the lock, and only
    // deregistered if no operand still leases it.
    drop(replaced);
}

/// A lease on the live registration covering `[start, end)` on `domain`, if one is cached.
///
/// The lease holds the region open for as long as the caller keeps it, so a
/// concurrent [`invalidate`] can retire the entry without pulling the region out from under a
/// descriptor that is already posted.
pub(crate) fn covering(start: usize, end: usize, domain: usize) -> Option<Arc<Registration>> {
    let registry = lock();
    let regions = &registry
        .iter()
        .find(|entry| entry.domain == domain)?
        .regions;
    regions
        .range(..=start)
        .next_back()
        .filter(|(base, entry)| **base <= start && end <= entry.end)
        .map(|(_, entry)| Arc::clone(&entry.registration))
}

/// Retire every tracked extent, on any domain, overlapping `[start, end)`. The [`ReclaimNotifier`]
/// calls this before the pages are reclaimed.
///
/// Retiring is immediate: The entry leaves the registry under the lock, so no later operand can
/// bind to it. Deregistration is not immediate: It happens when the last lease drops, which may
/// be after an in-flight transfer completes. Leases are dropped after the lock is released, because
/// `ibv_dereg_mr` must not run while the registry is locked or a re-entrant free would deadlock.
pub fn invalidate(start: usize, end: usize) {
    let retired: Vec<Arc<Registration>> = {
        let mut registry = lock();
        let mut retired = Vec::new();
        for domain in registry.iter_mut() {
            let bases: Vec<usize> = domain
                .regions
                .range(..end)
                .filter(|(_, entry)| start < entry.end)
                .map(|(&base, _)| base)
                .collect();
            for base in bases {
                if let Some(entry) = domain.regions.remove(&base) {
                    retired.push(entry.registration);
                }
            }
        }
        retired
    };
    drop(retired);
}

/// Retire every region on `domain`, called by an endpoint before closing that domain (regions are
/// domain objects). Dropping them from the registry stops a later reclaim from driving the cache
/// into a region whose domain is gone. Scoped to one domain so tearing down a worker leaves the
/// others' regions alone.
pub(crate) fn clear_domain(domain: usize) {
    let retired: Vec<Arc<Registration>> = {
        let mut registry = lock();
        match registry.iter().position(|entry| entry.domain == domain) {
            Some(index) => registry
                .remove(index)
                .regions
                .into_values()
                .map(|entry| entry.registration)
                .collect(),
            None => Vec::new(),
        }
    };
    drop(retired);
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::{
        ReclaimNotifier, Registration, clear_domain, covering, extent_for,
        install_reclaim_notifier, invalidate, track,
    };
    use crate::sys::{
        FI_READ, FI_REMOTE_READ, FI_REMOTE_WRITE, FI_WRITE, fi_allocinfo, fi_close, fi_domain,
        fi_ep_type_FI_EP_RDM, fi_fabric, fi_freeinfo, fi_getinfo, fi_info, fi_mr_reg, fi_version,
        fid_domain, fid_fabric, fid_mr,
    };
    use std::ffi::CString;
    use std::os::raw::c_void;
    use std::ptr;
    use std::sync::Arc;

    /// A real tcp domain, because [`invalidate`] closes whatever it removes: a fake `fid_mr` would
    /// fault inside libfabric before any assertion could run.
    struct Domain {
        domain: *mut fid_domain,
        fabric: *mut fid_fabric,
        info: *mut fi_info,
        /// tcp selects keys application-side, and requesting one twice on a domain fails the second
        /// registration with `-FI_ENOKEY`, so every registration here takes a fresh one.
        next_key: std::cell::Cell<u64>,
    }

    impl Domain {
        fn open() -> Self {
            let provider = CString::new("tcp").expect("provider name");
            let mut info: *mut fi_info = ptr::null_mut();
            let mut fabric: *mut fid_fabric = ptr::null_mut();
            let mut domain: *mut fid_domain = ptr::null_mut();
            unsafe {
                let hints = fi_allocinfo();
                assert!(!hints.is_null(), "fi_allocinfo");
                (*(*hints).ep_attr).type_ = fi_ep_type_FI_EP_RDM;
                (*(*hints).fabric_attr).prov_name = provider.as_ptr().cast_mut();
                let code = fi_getinfo(
                    fi_version(),
                    ptr::null(),
                    ptr::null(),
                    0,
                    hints,
                    &raw mut info,
                );
                (*(*hints).fabric_attr).prov_name = ptr::null_mut();
                fi_freeinfo(hints);
                assert_eq!(0, code, "fi_getinfo(tcp)");
                assert_eq!(
                    0,
                    fi_fabric((*info).fabric_attr, &raw mut fabric, ptr::null_mut()),
                    "fi_fabric"
                );
                assert_eq!(
                    0,
                    fi_domain(fabric, info, &raw mut domain, ptr::null_mut()),
                    "fi_domain"
                );
            }
            Self {
                domain,
                fabric,
                info,
                next_key: std::cell::Cell::new(1),
            }
        }

        /// The registry partitions by domain, so this doubles as this test's private namespace —
        /// the registry is a process-global that every test in this binary shares.
        fn key(&self) -> usize {
            self.domain as usize
        }

        fn register(&self, memory: &mut [u8]) -> *mut fid_mr {
            let key = self.next_key.get();
            self.next_key.set(key + 1);
            let mut region: *mut fid_mr = ptr::null_mut();
            let code = unsafe {
                fi_mr_reg(
                    self.domain,
                    memory.as_ptr().cast::<c_void>(),
                    memory.len(),
                    u64::from(FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE),
                    0,
                    key,
                    0,
                    &raw mut region,
                    ptr::null_mut(),
                )
            };
            assert_eq!(0, code, "fi_mr_reg");
            region
        }
    }

    impl Drop for Domain {
        fn drop(&mut self) {
            clear_domain(self.key());
            unsafe {
                fi_close(&mut (*self.domain).fid);
                fi_close(&mut (*self.fabric).fid);
                fi_freeinfo(self.info);
            }
        }
    }

    /// The production crash, in miniature.
    ///
    /// One cached registration covers a whole jemalloc extent, so it serves operands for several
    /// values at once. When one of those values is freed the notifier reports only that value's
    /// pages — but `invalidate` removes and closes every registration *overlapping* the range, which
    /// is the one registration a neighbouring value's in-flight transfer is using. Its descriptor is
    /// already on the wire, so the next `fi_writemsg` dereferences a closed `fid_mr`.
    ///
    /// This is the value-size transition seen on hardware: repopulating keys frees the old values
    /// while GETs against their neighbours are still in flight.
    #[test]
    fn freeing_one_value_deregisters_a_neighbour_that_is_still_in_flight() {
        let domain = Domain::open();
        let mut extent = vec![0u8; 64 * 1024];
        let base = extent.as_mut_ptr() as usize;
        let registration = Registration::new(domain.register(&mut extent));
        track(base, base + extent.len(), domain.key(), &registration);
        drop(registration); // the registry is the only holder now, as it is in production

        // A transfer resolves its operand out of the cache and goes in flight holding this lease.
        let operand = (base + 4096, base + 8192);
        let lease = covering(operand.0, operand.1, domain.key())
            .expect("the cached registration covers this operand");

        // A different value in the same extent is freed; its pages are reported before reclaim.
        invalidate(base + 32768, base + 36864);

        // Retired from the registry, so nothing new can bind to it...
        assert!(
            covering(operand.0, operand.1, domain.key()).is_none(),
            "a reclaimed extent must leave the registry immediately"
        );
        // ...but still registered, because this operand's transfer is on the wire. Sole ownership
        // is exactly that: the registry let go, the lease did not, and `Drop` has not run.
        assert_eq!(
            1,
            Arc::strong_count(&lease),
            "the in-flight operand must be the last holder, not a holder of a closed region"
        );
    }

    /// Whole-extent registration, and its guard rail.
    ///
    /// A notifier that can name the allocator extent containing an operand widens the registration
    /// to it, so neighbouring operands share one `fi_mr_reg`. A notifier that names a range *not*
    /// containing the operand is ignored — registering that would pin the wrong memory and leave the
    /// operand itself unregistered.
    ///
    /// The notifier is process-global and installed at most once, so this is the only test that
    /// touches it.
    #[test]
    fn an_extent_aware_notifier_widens_the_registration() {
        struct Extents;
        // A stand-in allocator whose extents are 64 KiB aligned, plus one deliberately bogus answer.
        const EXTENT: usize = 64 * 1024;
        impl ReclaimNotifier for Extents {
            fn covers(&self, _pointer: *mut u8, _length: usize) -> bool {
                true
            }
            fn extent_of(&self, pointer: *mut u8, length: usize) -> Option<(usize, usize)> {
                let start = pointer as usize;
                if 1 == length {
                    // Not containing the operand: the caller must refuse this.
                    return Some((start + 4096, start + 8192));
                }
                // Rounded out at both ends: a stand-in extent that failed to contain its own
                // operand would make this test depend on where the allocator happened to land.
                let base = start & !(EXTENT - 1);
                let end = start
                    .saturating_add(length)
                    .next_multiple_of(EXTENT)
                    .max(base + EXTENT);
                Some((base, end))
            }
        }
        static EXTENTS: Extents = Extents;
        install_reclaim_notifier(&EXTENTS);

        let mut memory = vec![0u8; 4096];
        let pointer = memory.as_mut_ptr();
        let start = pointer as usize;

        let (base, end) = extent_for(pointer, memory.len());
        assert_eq!(start & !(EXTENT - 1), base, "widened to the extent base");
        assert_eq!(base + EXTENT, end, "widened to the extent end");
        assert!(
            base <= start && start + memory.len() <= end,
            "still contains the operand"
        );

        // The bogus answer falls back to the operand rather than registering elsewhere.
        assert_eq!((start, start + 1), extent_for(pointer, 1));
    }

    /// The same defect reached the other way: `covering` hands out a raw `fid_mr` and drops the
    /// registry lock, so the pointer carries no lease. Nothing stops `invalidate` from closing it
    /// between the lookup and the post that uses it — the caller has no way to hold it open.
    #[test]
    fn a_registration_handed_to_a_caller_has_no_lease() {
        let domain = Domain::open();
        let mut extent = vec![0u8; 8192];
        let base = extent.as_mut_ptr() as usize;
        let registration = Registration::new(domain.register(&mut extent));
        track(base, base + extent.len(), domain.key(), &registration);
        drop(registration);

        let lease = covering(base, base + extent.len(), domain.key()).expect("just tracked");
        invalidate(base, base + extent.len());

        assert!(covering(base, base + extent.len(), domain.key()).is_none());
        assert_eq!(
            1,
            Arc::strong_count(&lease),
            "the caller's lease must outlive the registry's, not be closed underneath it"
        );
    }

    /// The race the hardware hit, driven directly: many threads resolve operands out of the cache
    /// and read their descriptors while another thread reclaims the same extents underneath them.
    ///
    /// Under the old registry this dereferenced a closed `fid_mr` and faulted inside libfabric.
    /// A lease makes it merely a retire: the descriptor stays valid while any holder lives.
    #[test]
    fn descriptors_stay_valid_while_reclaims_run_underneath() {
        let domain = Domain::open();
        let key = domain.key();
        let mut extents: Vec<Vec<u8>> = (0..8).map(|_| vec![0u8; 32 * 1024]).collect();
        let bounds: Vec<(usize, usize)> = extents
            .iter_mut()
            .map(|extent| {
                let base = extent.as_mut_ptr() as usize;
                (base, base + extent.len())
            })
            .collect();
        for (index, extent) in extents.iter_mut().enumerate() {
            let registration = Registration::new(domain.register(extent));
            track(bounds[index].0, bounds[index].1, key, &registration);
        }

        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let bounds = bounds.clone();
                let running = Arc::clone(&running);
                std::thread::spawn(move || {
                    let mut seen = 0usize;
                    while running.load(std::sync::atomic::Ordering::Relaxed) {
                        for (base, end) in &bounds {
                            if let Some(lease) = covering(*base, *base + 4096, key) {
                                // What `post` does with it: read the descriptor off a leased region
                                // while a reclaim may be retiring it on another thread.
                                let descriptor =
                                    unsafe { crate::sys::fi_mr_desc(lease.memory_region()) };
                                assert!(!lease.memory_region().is_null());
                                let _ = descriptor;
                                seen += 1;
                            }
                            let _ = end;
                        }
                    }
                    seen
                })
            })
            .collect();

        for _ in 0..60 {
            for (base, end) in &bounds {
                invalidate(*base, *end);
            }
            // Re-register so the readers keep finding entries to lease.
            for (index, extent) in extents.iter_mut().enumerate() {
                let registration = Registration::new(domain.register(extent));
                track(bounds[index].0, bounds[index].1, key, &registration);
            }
        }
        running.store(false, std::sync::atomic::Ordering::Relaxed);
        let total: usize = readers
            .into_iter()
            .map(|reader| reader.join().unwrap_or(0))
            .sum();
        assert!(0 < total, "the readers should have leased something");
    }
}
