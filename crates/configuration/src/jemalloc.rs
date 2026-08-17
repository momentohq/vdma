//! jemalloc configuration for the oversize arena

/// `[jemalloc]`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct JemallocConfiguration {
    /// Dirty/muzzy page-decay window in milliseconds for the huge arena. jemalloc otherwise forces
    /// this arena to 0, purging freed oversize extents at once, which deregisters every value as it
    /// is freed and forces a re-`fi_mr_reg` on the next touch. A positive window keeps freed extents
    /// dirty for reuse, trading bounded dirty memory for far fewer registrations. `-1` never purges,
    /// so registrations are never reclaimed and RSS grows.
    #[serde(default = "default_huge_arena_decay_ms")]
    pub huge_arena_decay_ms: i64,
    /// jemalloc's `arena.<i>.oversize_threshold`: the byte size above which the huge arena purges a
    /// freed extent on free instead of keeping it dirty for reuse. Its 8 MiB default means oversize
    /// values never reuse memory; raising it lets them recycle in place, with RSS bounded by
    /// [`Self::huge_arena_decay_ms`]. Anything larger still gets the eager purge.
    #[serde(default = "default_huge_arena_oversize_threshold")]
    pub huge_arena_oversize_threshold: usize,
}

impl Default for JemallocConfiguration {
    fn default() -> Self {
        Self {
            huge_arena_decay_ms: default_huge_arena_decay_ms(),
            huge_arena_oversize_threshold: default_huge_arena_oversize_threshold(),
        }
    }
}

/// Long enough that a churning oversize workload reuses each registration many times before its
/// pages are purged, short enough to keep dirty memory bounded.
fn default_huge_arena_decay_ms() -> i64 {
    1000
}

/// Freed values below this recycle in place; giant one-shot allocations keep jemalloc's eager purge,
/// so their dirty pages don't linger.
fn default_huge_arena_oversize_threshold() -> usize {
    512 * 1024 * 1024
}
