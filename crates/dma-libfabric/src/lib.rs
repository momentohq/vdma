//! The server half of vdma's data path, over the OFI (libfabric) C API.
//!
//! `LibfabricEndpoint` opens a local `FI_EP_RDM` endpoint; `FabricServer` runs the worker that
//! posts one-sided RMA against a client's exposed buffer and reaps completions. The peer address and
//! remote key arrive out-of-band, on the RESP control channel, per request. Clients are passive
//! targets and live outside this crate.

mod configuration;
mod connection;
mod devices;
mod endpoint;
mod error;
mod extent_hooks;
mod local_regions;
mod pool;
mod server;
mod server_dma_worker;

pub use configuration::{Configuration, Provider};
pub use devices::discover_domains;
pub use endpoint::{EndpointInfo, LibfabricEndpoint, PeerHandle};
pub use extent_hooks::{RegionHooksReport, RegionMode, install_region_hooks};
pub use pool::Pool;
pub use server::{
    BatchCompleter, Completion, DestinationAllocator, Direction, FabricServer, Outcome,
    TransferBuffer, TransferDone, TransferRequest,
};
