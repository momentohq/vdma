//! The `FromPeer` direction.
//!
//! Run `one_target --read` first; it prefills its buffer and prints the three advertisement fields.
//!
//! ```text
//! cargo run -p dma-libfabric --example one_target     -- --read 127.0.0.1
//! cargo run -p dma-libfabric --example read_from_peer -- 127.0.0.1 <address-hex> <rkey> <remote-addr>
//! ```

use std::sync::Arc;

use dma_libfabric::asynchronous::FabricServer;
use dma_libfabric::{Configuration, Direction, Pool, Provider, TransferRequest};
use dma_libfabric_protocol::{Advertisement, DmaError, encode_hex};

/// What `one_target --read` fills its buffer with, and how much of it there is.
const PATTERN: u8 = 0xab;
const LENGTH: usize = 4096;

#[tokio::main]
async fn main() -> Result<(), DmaError> {
    let mut arguments = std::env::args().skip(1);
    let configuration = Configuration {
        providers: vec![Provider::Tcp],
        bind: arguments.next(),
        ..Configuration::default()
    };

    let pool = Arc::new(Pool::new(2).map_err(|error| DmaError::Fabric(format!("pool: {error}")))?);
    let server: FabricServer<Vec<u8>> = FabricServer::start(&configuration, pool)?;

    // What a peer must hold to be RMA'd against. Send this to peers via your control channel.
    println!("local address: {}", encode_hex(server.local_address()));

    let fields: Vec<String> = arguments.collect();
    let [address, remote_key, remote_address] = fields.as_slice() else {
        println!("pass <address-hex> <rkey> <remote-addr> to run a transfer");
        return Ok(());
    };
    let peer = Advertisement::from_fields(&[
        address.as_bytes(),
        remote_key.as_bytes(),
        remote_address.as_bytes(),
    ])
    .map_err(|error| DmaError::Fabric(error.to_string()))?;

    let request = TransferRequest {
        client_id: 1,
        peer_address: peer.address,
        remote_key: peer.remote_key,
        remote_address: peer.remote_address,
        direction: Direction::FromPeer { length: LENGTH },
        want_checksum: true,
        // Vec trivially implements Operands
        caller_context: Vec::new(),
        parent_id: None,
    };
    let Ok(transfer) = server.transfer(request) else {
        return Err(DmaError::Fabric("fabric worker is gone".into()));
    };

    let (outcome, payload) = transfer.await;
    match outcome {
        Ok(transferred) => println!(
            "read {} bytes, crc {:?}, payload {}",
            transferred.bytes,
            transferred.checksum,
            if payload.iter().all(|byte| PATTERN == *byte) {
                "verified"
            } else {
                "IS NOT WHAT THE TARGET SERVED"
            }
        ),
        Err(error) => println!("transfer failed: {error}"),
    }
    Ok(())
}
