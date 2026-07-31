//! jemalloc extent hooks that deregister a memory region the instant jemalloc reclaims the pages it
//! covers, so page decay can stay on without a registration going stale.
//!
//! A registration pins the physical pages present when it was taken; it goes stale when those
//! pages are `madvise`d away or `munmap`d. Under `opt.retain` (the 64-bit Linux default) decay does
//! not `munmap` — it purges (`MADV_FREE`/`MADV_DONTNEED`) and decommits. So the five wrapped hooks
//! are `purge_lazy`, `purge_forced`, `decommit`, `dalloc`, `destroy`; each deregisters what it covers
//! then chains to jemalloc's default. `alloc`/`commit`/`split`/`merge` never drop a covered page and
//! pass straight through.
//!
//! Each hook removes overlapping entries under the registry lock, releases it, then `fi_close`s, so
//! `ibv_dereg_mr` never runs under the lock and cannot deadlock a re-entrant free. A page being
//! reclaimed is never a live operand (the transfer retains its operand), so deregistration cannot
//! race an in-flight descriptor.

use std::collections::BTreeMap;
use std::os::raw::{c_char, c_int, c_void};
use std::sync::Mutex;

use libfabric_sys::{fi_close, fid_mr};

// valkey's jemalloc, prefixed `je_`, resolved from the host process at module load.
unsafe extern "C" {
    fn je_mallctl(
        name: *const c_char,
        oldp: *mut c_void,
        oldlenp: *mut usize,
        newp: *mut c_void,
        newlen: usize,
    ) -> c_int;
    fn je_malloc(size: usize) -> *mut c_void;
    fn je_free(pointer: *mut c_void);
}

/// jemalloc 5.3's `extent_hooks_t`: nine function pointers. Layout must match.
#[repr(C)]
#[derive(Clone, Copy)]
struct ExtentHooks {
    alloc: *const c_void,
    dalloc: ExtentDalloc,
    destroy: ExtentDestroy,
    commit: *const c_void,
    decommit: ExtentRange,
    purge_lazy: ExtentRange,
    purge_forced: ExtentRange,
    split: *const c_void,
    merge: *const c_void,
}

/// `dalloc`: reclaim a whole extent. Returns true on failure (jemalloc keeps the extent).
type ExtentDalloc =
    Option<unsafe extern "C" fn(*mut ExtentHooks, *mut c_void, usize, bool, u32) -> bool>;
/// `destroy`: unconditionally unmap a whole extent.
type ExtentDestroy = Option<unsafe extern "C" fn(*mut ExtentHooks, *mut c_void, usize, bool, u32)>;
/// `decommit` / `purge_lazy` / `purge_forced`: act on `[addr + offset, addr + offset + length)` of the
/// extent `[addr, addr + size)`. Returns true on failure.
type ExtentRange =
    Option<unsafe extern "C" fn(*mut ExtentHooks, *mut c_void, usize, usize, usize, u32) -> bool>;

// SAFETY: a bag of `'static` C function pointers with their own internal threading semantics.
unsafe impl Send for ExtentHooks {}
unsafe impl Sync for ExtentHooks {}

/// jemalloc's defaults, captured at [`install`] before any wrapped hook can fire. Then read-only.
static mut DEFAULT: ExtentHooks = ExtentHooks {
    alloc: std::ptr::null(),
    dalloc: None,
    destroy: None,
    commit: std::ptr::null(),
    decommit: None,
    purge_lazy: None,
    purge_forced: None,
    split: std::ptr::null(),
    merge: std::ptr::null(),
};

/// The wrapper table jemalloc calls through; static so its address stays valid. Non-overridden
/// fields are filled from [`DEFAULT`] in [`install`].
static mut WRAPPED: ExtentHooks = ExtentHooks {
    alloc: std::ptr::null(),
    dalloc: Some(wrapped_dalloc),
    destroy: Some(wrapped_destroy),
    commit: std::ptr::null(),
    decommit: Some(wrapped_decommit),
    purge_lazy: Some(wrapped_purge_lazy),
    purge_forced: Some(wrapped_purge_forced),
    split: std::ptr::null(),
    merge: std::ptr::null(),
};

/// Registered operand extents, grouped by domain. A descriptor is valid only on its own domain's
/// endpoint (one fabric worker per EFA device, so several domains coexist); handing one domain's
/// descriptor to another's endpoint fails with "does not match IOVA". Reclaim, by contrast,
/// deregisters across every domain — the memory is gone for all of them. Domains are few, so the
/// outer scan is linear; the `BTreeMap` finds overlaps by range within one.
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

/// Record a registered operand extent so the hooks deregister it on reclaim and the RMA path can
/// reuse it.
pub(crate) fn track(base: usize, end: usize, domain: usize, memory_region: *mut fid_mr) {
    let replaced = {
        let mut registry = lock();
        domain_regions(&mut registry, domain).insert(base, RegionEntry { end, memory_region })
    };
    // A prior registration at this base+domain should already be gone; close it if not, so
    // re-registration doesn't leak it.
    if let Some(old) = replaced {
        // SAFETY: superseded; closed once.
        unsafe { fi_close(&mut (*old.memory_region).fid) };
    }
}

/// The memory region of a live registration on `domain` containing `[start, end)`, letting the RMA
/// hot path reuse it instead of calling `fi_mr_reg`.
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

/// Deregister every tracked extent, on any domain, overlapping `[start, end)`. Removes under the
/// lock, releases, then `fi_close`s — `ibv_dereg_mr` must not run while the registry is locked.
fn deregister_overlapping(start: usize, end: usize) {
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

unsafe extern "C" fn wrapped_dalloc(
    _hooks: *mut ExtentHooks,
    addr: *mut c_void,
    size: usize,
    committed: bool,
    arena_ind: u32,
) -> bool {
    deregister_overlapping(addr as usize, addr as usize + size);
    // SAFETY: DEFAULT is captured before any hook fires, then never mutated.
    match unsafe { DEFAULT.dalloc } {
        Some(dalloc) => unsafe {
            dalloc(
                std::ptr::addr_of_mut!(DEFAULT),
                addr,
                size,
                committed,
                arena_ind,
            )
        },
        // No default dalloc: report "not deallocated" so jemalloc retains rather than leaks it.
        None => true,
    }
}

unsafe extern "C" fn wrapped_destroy(
    _hooks: *mut ExtentHooks,
    addr: *mut c_void,
    size: usize,
    committed: bool,
    arena_ind: u32,
) {
    deregister_overlapping(addr as usize, addr as usize + size);
    if let Some(destroy) = unsafe { DEFAULT.destroy } {
        unsafe {
            destroy(
                std::ptr::addr_of_mut!(DEFAULT),
                addr,
                size,
                committed,
                arena_ind,
            )
        };
    }
}

unsafe extern "C" fn wrapped_decommit(
    hooks: *mut ExtentHooks,
    addr: *mut c_void,
    size: usize,
    offset: usize,
    length: usize,
    arena_ind: u32,
) -> bool {
    reclaim_range(
        unsafe { DEFAULT.decommit },
        hooks,
        addr,
        size,
        offset,
        length,
        arena_ind,
    )
}

unsafe extern "C" fn wrapped_purge_lazy(
    hooks: *mut ExtentHooks,
    addr: *mut c_void,
    size: usize,
    offset: usize,
    length: usize,
    arena_ind: u32,
) -> bool {
    reclaim_range(
        unsafe { DEFAULT.purge_lazy },
        hooks,
        addr,
        size,
        offset,
        length,
        arena_ind,
    )
}

unsafe extern "C" fn wrapped_purge_forced(
    hooks: *mut ExtentHooks,
    addr: *mut c_void,
    size: usize,
    offset: usize,
    length: usize,
    arena_ind: u32,
) -> bool {
    reclaim_range(
        unsafe { DEFAULT.purge_forced },
        hooks,
        addr,
        size,
        offset,
        length,
        arena_ind,
    )
}

/// Shared body for the range hooks (decommit / purge): deregister what covers the affected
/// sub-range, then chain to the default. No default (platform lacks this reclaim mode) reports
/// failure, so jemalloc treats the range as un-purged.
fn reclaim_range(
    default: ExtentRange,
    _hooks: *mut ExtentHooks,
    addr: *mut c_void,
    size: usize,
    offset: usize,
    length: usize,
    arena_ind: u32,
) -> bool {
    let sub_start = addr as usize + offset;
    deregister_overlapping(sub_start, sub_start + length);
    match default {
        Some(reclaim) => unsafe {
            reclaim(
                std::ptr::addr_of_mut!(DEFAULT),
                addr,
                size,
                offset,
                length,
                arena_ind,
            )
        },
        None => true,
    }
}

/// What [`install`] tuned, for operator logging.
#[derive(Debug)]
pub struct RegionHooksReport {
    /// The oversize/"huge" arena index, if oversize routing is on and it was found.
    pub huge_arena: Option<u32>,
    /// Decay window (ms) applied to the huge arena, overriding jemalloc's eager `0`.
    pub huge_arena_decay_ms: i64,
    /// Eager-purge threshold (bytes) for the huge arena, overriding jemalloc's 8 MiB so freed values
    /// below it recycle in place instead of being purged on free.
    pub huge_arena_oversize_threshold: usize,
}

/// Prepare the wrapped hooks and tune the huge arena. Call once at startup.
///
/// Arenas are hooked lazily on the registration path ([`ensure_arena_hooked`]), so an operand cannot
/// be registered in an unhooked arena and no startup scan is needed. Tuning must happen here: the
/// huge arena's dirty/muzzy decay window (jemalloc forces it to 0, purging every freed oversize value
/// at once) and its eager-purge-on-free threshold (jemalloc's 8 MiB purges freed oversize extents
/// instead of caching them dirty, re-faulting and re-registering the next value).
pub fn install(
    huge_arena_decay_ms: i64,
    huge_arena_oversize_threshold: usize,
) -> Result<RegionHooksReport, String> {
    // Capture arena 0's defaults so the wrappers can chain to real reclaim, and copy the
    // non-overridden fields into WRAPPED so jemalloc's own alloc/commit/split/merge still run.
    // `ensure_arena_hooked` installs WRAPPED, so it must be complete before any registration.
    let default = read_extent_hooks(0).ok_or_else(|| {
        "arena.0.extent_hooks unavailable (not the jemalloc allocator?)".to_string()
    })?;
    // SAFETY: single-threaded startup, before any wrapped hook is installed.
    unsafe {
        DEFAULT = default;
        WRAPPED.alloc = default.alloc;
        WRAPPED.commit = default.commit;
        WRAPPED.split = default.split;
        WRAPPED.merge = default.merge;
    }

    // Force the huge arena into existence and override its eager decay and eager-purge.
    let huge_arena = tune_huge_arena(huge_arena_decay_ms, huge_arena_oversize_threshold);

    Ok(RegionHooksReport {
        huge_arena,
        huge_arena_decay_ms,
        huge_arena_oversize_threshold,
    })
}

/// Close every region on `domain`, called by an endpoint before closing that domain (regions are
/// domain objects). Dropping them from the registry stops a later free from driving the hooks into a
/// region whose domain is gone. Scoped to one domain so tearing down a worker leaves the others'
/// regions alone.
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

/// Create the huge arena via one oversize allocation, set its dirty/muzzy decay and
/// eager-purge-on-free threshold, and return its index. `None` if the allocation or lookup fails, or
/// oversize routing is off — then the nudge lands in a normal arena, which we cover regardless.
fn tune_huge_arena(decay_ms: i64, oversize_threshold: usize) -> Option<u32> {
    // Above the 8 MiB default `oversize_threshold`, so this routes to the huge arena, creating it
    // and letting `arenas.lookup` name it.
    const OVERSIZE_NUDGE: usize = 16 * 1024 * 1024;
    let pointer = unsafe { je_malloc(OVERSIZE_NUDGE) };
    if pointer.is_null() {
        return None;
    }
    let arena = arena_of(pointer);
    if let Some(index) = arena {
        write_isize(&arena_key(index, "dirty_decay_ms"), decay_ms as isize);
        write_isize(&arena_key(index, "muzzy_decay_ms"), decay_ms as isize);
        write_size(&arena_key(index, "oversize_threshold"), oversize_threshold);
    }
    unsafe { je_free(pointer) };
    arena
}

/// Hook the arena backing `operand` if it isn't already. jemalloc creates arenas lazily as threads
/// bind to them, and an unhooked one holding a registered operand would skip deregistration on free
/// and leave the registration stale. Called on the registration (cache-miss) path before the operand
/// is registered, so coverage cannot lag the registration. The `arena.<i>.extent_hooks` write is
/// idempotent.
pub(crate) fn ensure_arena_hooked(operand: *mut u8) {
    if let Some(arena) = arena_of(operand.cast::<c_void>()) {
        write_extent_hooks(arena);
    }
}

/// The arena index owning `pointer`, via `arenas.lookup`: write the pointer, read the index.
fn arena_of(pointer: *mut c_void) -> Option<u32> {
    let mut arena_ind: u32 = 0;
    let mut out_size = std::mem::size_of::<u32>();
    let mut lookup = pointer;
    let code = unsafe {
        je_mallctl(
            c"arenas.lookup".as_ptr(),
            (&mut arena_ind as *mut u32).cast::<c_void>(),
            &mut out_size,
            (&mut lookup as *mut *mut c_void).cast::<c_void>(),
            std::mem::size_of::<*mut c_void>(),
        )
    };
    (0 == code).then_some(arena_ind)
}

fn write_isize(name: &std::ffi::CStr, mut value: isize) -> bool {
    let code = unsafe {
        je_mallctl(
            name.as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            (&mut value as *mut isize).cast::<c_void>(),
            std::mem::size_of::<isize>(),
        )
    };
    0 == code
}

fn write_size(name: &std::ffi::CStr, mut value: usize) -> bool {
    let code = unsafe {
        je_mallctl(
            name.as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            (&mut value as *mut usize).cast::<c_void>(),
            std::mem::size_of::<usize>(),
        )
    };
    0 == code
}

fn read_extent_hooks(arena: u32) -> Option<ExtentHooks> {
    let name = arena_key(arena, "extent_hooks");
    let mut value: *mut ExtentHooks = std::ptr::null_mut();
    let mut size = std::mem::size_of::<*mut ExtentHooks>();
    let code = unsafe {
        je_mallctl(
            name.as_ptr(),
            (&mut value as *mut *mut ExtentHooks).cast::<c_void>(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    (0 == code && !value.is_null()).then(|| unsafe { *value })
}

fn write_extent_hooks(arena: u32) -> bool {
    let name = arena_key(arena, "extent_hooks");
    // SAFETY: WRAPPED is a stable static; jemalloc retains the pointer and calls through it.
    let mut hooks_pointer: *mut ExtentHooks = std::ptr::addr_of_mut!(WRAPPED);
    let code = unsafe {
        je_mallctl(
            name.as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            (&mut hooks_pointer as *mut *mut ExtentHooks).cast::<c_void>(),
            std::mem::size_of::<*mut ExtentHooks>(),
        )
    };
    0 == code
}

fn arena_key(index: u32, field: &str) -> std::ffi::CString {
    std::ffi::CString::new(format!("arena.{index}.{field}")).unwrap_or_default()
}
