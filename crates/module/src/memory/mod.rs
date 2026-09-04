mod extent_hooks;
mod huge_arena;
mod jemalloc;

pub use extent_hooks::{RegionMode, ensure_covered, install};
