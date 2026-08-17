//! Tuning for jemalloc's oversize "huge" arena, where registered RMA value memory lives.
//!
//! jemalloc forces this arena's dirty/muzzy decay to 0, purging every freed oversize value at once,
//! which deregisters it and forces a fresh `fi_mr_reg` on the next touch. Its 8 MiB
//! `oversize_threshold` compounds that by purging a freed extent on free instead of keeping it
//! dirty for reuse. Both are overridden here so freed values recycle in place, with RSS bounded by
//! the decay window.

use crate::memory::jemalloc::{self, Allocation};

/// Above jemalloc's default 8 MiB `oversize_threshold`, so this allocation routes to the huge arena,
/// creating it and letting `arenas.lookup` name it.
const OVERSIZE_NUDGE: usize = 16 * 1024 * 1024;

/// Force the huge arena into existence, set its decay window and eager-purge-on-free threshold, and
/// return its index. `None` if the allocation or lookup fails, or oversize routing is off — then the
/// nudge lands in a normal arena, which the extent hooks cover regardless.
pub(crate) fn tune(decay_ms: i64, oversize_threshold: usize) -> Option<u32> {
    let allocation = Allocation::new(OVERSIZE_NUDGE)?;
    let arena = jemalloc::arena_of(allocation.pointer())?;
    jemalloc::write(
        &jemalloc::arena_key(arena, "dirty_decay_ms"),
        decay_ms as isize,
    );
    jemalloc::write(
        &jemalloc::arena_key(arena, "muzzy_decay_ms"),
        decay_ms as isize,
    );
    jemalloc::write(
        &jemalloc::arena_key(arena, "oversize_threshold"),
        oversize_threshold,
    );
    Some(arena)
}
