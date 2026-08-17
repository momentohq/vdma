//! The server half of vdma's data path, over the OFI (libfabric) C API.
//!
//! `LibfabricEndpoint` opens a local `FI_EP_RDM` endpoint; `FabricServer` runs the worker that
//! posts one-sided RMA against a client's exposed buffer and reaps completions. The peer address and
//! remote key arrive out-of-band, on the caller's own control channel, per request. Clients are
//! passive targets and live outside this crate.

mod configuration;
mod connection;
mod devices;
mod endpoint;
mod error;
mod local_regions;
mod pool;
mod region_cache;
mod server;
mod server_dma_worker;

pub use configuration::{Configuration, Provider};
pub use devices::discover_domains;
pub use endpoint::{EndpointInfo, LibfabricEndpoint, PeerHandle};
pub use pool::Pool;
pub use region_cache::{ReclaimNotifier, install_reclaim_notifier, invalidate};
pub use server::{
    BatchCompleter, Completion, DestinationAllocator, Direction, FabricServer, Outcome,
    TransferBuffer, TransferDone, TransferRequest,
};
