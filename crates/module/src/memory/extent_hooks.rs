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
//! Deregistration is [`dma_libfabric::invalidate`]. A page being reclaimed is never a live operand,
//! so deregistration cannot race an in-flight descriptor.

use std::os::raw::c_void;

use dma_libfabric::ReclaimNotifier;

use crate::memory::{huge_arena, jemalloc};

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

/// How a local operand registration is reclaimed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionMode {
    /// Registrations are cached and the wrapped hooks close them as the pages are reclaimed.
    Cached,
    /// No jemalloc. Register per operation and close at completion, which costs `fi_mr_reg` for
    /// every transfer.
    PerOperation,
}

/// What [`install`] tuned, for operator logging.
#[derive(Debug)]
pub struct RegionHooksReport {
    /// How operand registrations will be reclaimed.
    pub mode: RegionMode,
    /// The oversize/"huge" arena index, if oversize routing is on and it was found.
    pub huge_arena: Option<u32>,
    /// Decay window (ms) applied to the huge arena, overriding jemalloc's eager `0`.
    pub huge_arena_decay_ms: i64,
    /// Eager-purge threshold (bytes) for the huge arena, overriding jemalloc's 8 MiB so freed values
    /// below it recycle in place instead of being purged on free.
    pub huge_arena_oversize_threshold: usize,
}

/// Registers the wrapped hooks with `dma-libfabric` as its reclaim notifier and tunes the huge
/// arena. Call once at startup.
///
/// Arenas are hooked lazily, on the registration path ([`JemallocRegions::covers`]), so an operand
/// cannot be registered in an unhooked arena and no startup scan is needed. Tuning must happen here
/// — see [`huge_arena`] for what jemalloc's defaults would otherwise cost.
pub fn install(
    huge_arena_decay_ms: i64,
    huge_arena_oversize_threshold: usize,
) -> RegionHooksReport {
    // Capture arena 0's defaults so the wrappers can chain to real reclaim, and copy the
    // non-overridden fields into WRAPPED so jemalloc's own alloc/commit/split/merge still run.
    // `covers` installs WRAPPED, so it must be complete before any registration.
    let Some(default) = read_extent_hooks(0) else {
        return RegionHooksReport {
            mode: RegionMode::PerOperation,
            huge_arena: None,
            huge_arena_decay_ms,
            huge_arena_oversize_threshold,
        };
    };
    // SAFETY: single-threaded startup, before any wrapped hook is installed.
    unsafe {
        DEFAULT = default;
        WRAPPED.alloc = default.alloc;
        WRAPPED.commit = default.commit;
        WRAPPED.split = default.split;
        WRAPPED.merge = default.merge;
    }

    let huge_arena = huge_arena::tune(huge_arena_decay_ms, huge_arena_oversize_threshold);

    dma_libfabric::install_reclaim_notifier(&JemallocRegions);
    RegionHooksReport {
        mode: RegionMode::Cached,
        huge_arena,
        huge_arena_decay_ms,
        huge_arena_oversize_threshold,
    }
}

/// The promise `dma-libfabric` caches registrations against: any page of a hooked arena is reported
/// before it is reclaimed.
struct JemallocRegions;

impl ReclaimNotifier for JemallocRegions {
    /// Hook the arena backing this operand if it isn't already, and say whether it is now covered.
    /// jemalloc creates arenas lazily as threads bind to them, and an unhooked one holding a
    /// registered operand would skip deregistration on free and leave the registration stale. Called
    /// before the operand is registered, so coverage cannot lag the registration. The
    /// `arena.<i>.extent_hooks` write is idempotent.
    fn covers(&self, pointer: *mut u8, _length: usize) -> bool {
        jemalloc::arena_of(pointer.cast::<c_void>()).is_some_and(write_extent_hooks)
    }
}

unsafe extern "C" fn wrapped_dalloc(
    _hooks: *mut ExtentHooks,
    addr: *mut c_void,
    size: usize,
    committed: bool,
    arena_ind: u32,
) -> bool {
    dma_libfabric::invalidate(addr as usize, addr as usize + size);
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
    dma_libfabric::invalidate(addr as usize, addr as usize + size);
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
    dma_libfabric::invalidate(sub_start, sub_start + length);
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

fn read_extent_hooks(arena: u32) -> Option<ExtentHooks> {
    let table = jemalloc::read::<*mut ExtentHooks>(&jemalloc::arena_key(arena, "extent_hooks"))?;
    // SAFETY: jemalloc owns the table it named and keeps it alive for the arena's life.
    (!table.is_null()).then(|| unsafe { *table })
}

fn write_extent_hooks(arena: u32) -> bool {
    // SAFETY: WRAPPED is a stable static; jemalloc retains the pointer and calls through it.
    let table: *mut ExtentHooks = std::ptr::addr_of_mut!(WRAPPED);
    jemalloc::write(&jemalloc::arena_key(arena, "extent_hooks"), table)
}
