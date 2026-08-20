//! like `one_transfer`, but async.
//!
//! Run `one_target` first to print the three advertisement fields
//!
//! ```text
//! cargo run -p dma-libfabric --example one_target     -- 127.0.0.1
//! cargo run -p dma-libfabric --example async_transfer -- 127.0.0.1 <address-hex> <rkey> <remote-addr>
//! ```

use std::sync::Arc;

use dma_libfabric::asynchronous::FabricServer;
use dma_libfabric::{Direction, Operands, Pool, TransferRequest};
use dma_libfabric_protocol::{DmaError, encode_hex};

mod common;

/// The per-op context. It owns the payload, which keeps the buffer alive for the whole
/// transfer. Since the vec is inline it also trivially handles cancellation-safety.
struct Operation {
    payload: Vec<u8>,
}

impl Operands for Operation {
    /// The bytes this transfer writes into the peer. `FromPeer` would implement `allocate` instead.
    fn source(&self) -> Option<&[u8]> {
        Some(&self.payload)
    }
}

/// tokio here for brevity; a [`Transfer`](dma_libfabric::asynchronous::Transfer) is a plain future
/// and this crate depends on no particular runtime.
#[tokio::main]
async fn main() -> Result<(), DmaError> {
    let common::Arguments {
        configuration,
        peer,
    } = common::parse()?;

    let pool = Arc::new(Pool::new(2).map_err(|error| DmaError::Fabric(format!("pool: {error}")))?);
    let server: FabricServer<Operation> = FabricServer::start(&configuration, pool)?;

    // What a peer must hold to be RMA'd against. Carry it on your own control channel.
    println!("local address: {}", encode_hex(server.local_address()));

    let peer = match peer {
        Some(peer) => peer,
        None => {
            println!("paste the target's <address-hex> <rkey> <remote-addr>:");
            common::peer_from_stdin()?
        }
    };

    // `one_target` waits for a full buffer of this byte.
    let payload = vec![0xab_u8; 4096];
    let request = TransferRequest {
        client_id: 1,
        peer_address: peer.address,
        remote_key: peer.remote_key,
        remote_address: peer.remote_address,
        direction: Direction::ToPeer,
        want_checksum: true,
        caller_context: Operation { payload },
        parent_id: None,
    };
    let Ok(transfer) = server.transfer(request) else {
        return Err(DmaError::Fabric("fabric worker is gone".into()));
    };

    // The context comes back with the outcome, so the payload is yours again here.
    let (outcome, operation) = transfer.await;
    match outcome {
        Ok(transferred) => println!(
            "wrote {} of {} bytes, crc {:?}",
            transferred.bytes,
            operation.payload.len(),
            transferred.checksum
        ),
        Err(error) => println!("transfer failed: {error}"),
    }
    Ok(())
}
