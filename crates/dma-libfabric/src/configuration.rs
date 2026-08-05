//! Configuration for the libfabric DMA provider. Aggregated by the `configuration` crate.

/// Supported libfabric providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    Tcp,
    /// The EFA hardware-RDMA path: the `efa` provider's `efa-direct` fabric, which uses device RMA
    /// with no software wire protocol or host progress engine, unlike EFA's rxr path. Requires
    /// `FI_CONTEXT2` — see `server_dma_worker::InFlight` for how that is satisfied.
    #[serde(rename = "efa-direct")]
    EfaDirect,
}

impl Provider {
    /// The `fi_fabric_attr.prov_name`. `EfaDirect` shares the `efa` provider and is distinguished by
    /// [`Self::fabric_name`].
    pub fn as_str(self) -> &'static str {
        match self {
            Provider::Tcp => "tcp",
            Provider::EfaDirect => "efa",
        }
    }

    /// The `fi_fabric_attr.name` to pin when the provider alone doesn't disambiguate. One `efa`
    /// provider exposes both the rxr software `efa` fabric and the device-RMA `efa-direct` one, so
    /// selecting hardware RDMA means pinning the fabric name.
    pub fn fabric_name(self) -> Option<&'static str> {
        match self {
            Provider::EfaDirect => Some("efa-direct"),
            _ => None,
        }
    }

    /// Whether the endpoint requires `FI_CONTEXT2` mode: a provider-owned `fi_context2` as each op's
    /// `op_context`.
    pub fn requires_context2(self) -> bool {
        matches!(self, Provider::EfaDirect)
    }
}

/// Local configuration section for the libfabric DMA layer.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    /// Providers to attempt, in preference order.
    #[serde(default = "default_providers")]
    pub providers: Vec<Provider>,
    /// Interface names to use; empty means every interface discovered on the configured hardware.
    #[serde(default)]
    pub interfaces: Vec<String>,
    /// Source address to bind the local endpoint to. `None` lets the provider choose.
    #[serde(default)]
    pub bind: Option<String>,
    /// Transfers the server keeps in flight at once. `None` derives a default from the provider's
    /// transmit-queue depth, capped to bound pinned memory.
    #[serde(default)]
    pub max_in_flight: Option<usize>,
    /// Dirty/muzzy page-decay window in milliseconds for jemalloc's oversize "huge" arena, where
    /// registered RMA value memory lives. jemalloc otherwise forces this arena to 0, purging freed
    /// oversize extents at once, which deregisters every value as it is freed and forces a
    /// re-`fi_mr_reg` on the next touch. A positive window keeps freed extents dirty for reuse,
    /// trading bounded dirty memory for far fewer registrations. `-1` never purges, so registrations
    /// are never reclaimed and RSS grows to the high-water mark.
    #[serde(default = "default_huge_arena_decay_ms")]
    pub huge_arena_decay_ms: i64,
    /// jemalloc's `arena.<i>.oversize_threshold`: the byte size above which the huge arena purges a
    /// freed extent on free instead of keeping it dirty for reuse. Its 8 MiB default means oversize
    /// values never reuse memory; raising it lets them recycle in place, with RSS bounded by
    /// `huge_arena_decay_ms`. Anything larger still gets the eager purge.
    #[serde(default = "default_huge_arena_oversize_threshold")]
    pub huge_arena_oversize_threshold: usize,
    /// `f-pool-nn` threads running checksummed transfers' CRC and reply off the fabric worker, so
    /// hashing a payload doesn't stall the post-and-reap loop. More threads hash more in parallel,
    /// though the GIL commit still serializes. `None` derives 2 per EFA device.
    #[serde(default)]
    pub crc_pool_threads: Option<usize>,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            providers: default_providers(),
            interfaces: Vec::new(),
            bind: None,
            max_in_flight: None,
            huge_arena_decay_ms: default_huge_arena_decay_ms(),
            huge_arena_oversize_threshold: default_huge_arena_oversize_threshold(),
            crc_pool_threads: None,
        }
    }
}

fn default_providers() -> Vec<Provider> {
    vec![Provider::Tcp]
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
