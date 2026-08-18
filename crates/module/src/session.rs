//! The shared server endpoint, opened lazily for `DMA.INFO`.
//!
//! Data transfers run on the fabric worker's own endpoint, in [`crate::transfer`]. This one exists
//! only so `DMA.INFO` can interrogate the selected provider's attributes without standing up a
//! worker. It runs on valkey's single command thread, so a thread-local avoids `Send` and `Sync`.

use std::cell::RefCell;

use configuration::Configuration;
use dma_libfabric::{EndpointInfo, LibfabricEndpoint};
use dma_libfabric_protocol::DmaError;

thread_local! {
    static ENDPOINT: RefCell<Option<LibfabricEndpoint>> = const { RefCell::new(None) };
}

/// The selected provider's attributes, opening the server endpoint if needed.
pub fn endpoint_info(configuration: &Configuration) -> Result<EndpointInfo, DmaError> {
    with_endpoint(configuration, |endpoint| Ok(endpoint.describe()))
}

/// Ensure the shared server endpoint is open, then run `body` with it.
fn with_endpoint<R>(
    configuration: &Configuration,
    body: impl FnOnce(&LibfabricEndpoint) -> Result<R, DmaError>,
) -> Result<R, DmaError> {
    ENDPOINT.with(|cell| {
        let mut endpoint = cell.borrow_mut();
        if endpoint.is_none() {
            *endpoint = Some(LibfabricEndpoint::open(
                &configuration.dma_libfabric,
                configuration.dma_libfabric.bind.as_deref(),
            )?);
        }
        let Some(endpoint) = endpoint.as_ref() else {
            return Err(DmaError::Fabric("endpoint unavailable".into()));
        };
        body(endpoint)
    })
}
