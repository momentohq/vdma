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
    /// `f-pool-nn` threads running checksummed transfers' CRC and reply off the fabric worker, so
    /// hashing a payload doesn't stall the post-and-reap loop. More threads hash more in parallel,
    /// though the caller's completion hook still serializes. `None` derives 2 per EFA device.
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
            crc_pool_threads: None,
        }
    }
}

fn default_providers() -> Vec<Provider> {
    vec![Provider::Tcp]
}
