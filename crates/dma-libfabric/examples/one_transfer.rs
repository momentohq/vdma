//! A minimal consumer of this crate: start a server, print the address a peer needs, and write one
//! buffer into a peer's exposed memory.
//!
//! Run `one_target` first; it prints the three advertisement fields to paste here.
//!
//! ```text
//! cargo run -p dma-libfabric --example one_target   -- 127.0.0.1
//! cargo run -p dma-libfabric --example one_transfer -- 127.0.0.1 <address-hex> <rkey> <remote-addr>
//! ```
//!
//! The peer is passive: it registers the buffer and hands you those fields over a control channel
//! this crate knows nothing about. Registration caching is off here — no `ReclaimNotifier` is
//! installed, so each operand is registered and closed per transfer.

use std::sync::Arc;
use std::sync::mpsc::{Sender, channel};

use dma_libfabric::{
    Completion, Configuration, DestinationAllocator, Direction, FabricServer, Outcome, Pool,
    Provider, TransferBuffer, TransferRequest,
};
use dma_libfabric_protocol::{Advertisement, DmaError, encode_hex};

/// The per-op context, carried to the worker and handed back at completion.
struct Operation {
    done: Sender<Outcome>,
}

impl DestinationAllocator for Operation {
    /// Runs on the worker, so a `FromPeer` landing buffer is allocated off the submitting thread.
    /// `ToPeer` supplies its buffer in the request, so this never runs here.
    fn allocate(&mut self, _length: usize) -> *mut u8 {
        std::ptr::null_mut()
    }
}

/// Batched, so a reply path takes its lock once per batch rather than per transfer.
fn complete(completions: std::vec::Drain<'_, Completion<Operation>>) {
    for completion in completions {
        let _ = completion.caller_context.done.send(completion.outcome);
    }
}

fn main() -> Result<(), DmaError> {
    let mut arguments = std::env::args().skip(1);
    let configuration = Configuration {
        providers: vec![Provider::Tcp],
        bind: arguments.next(),
        ..Configuration::default()
    };

    let pool = Arc::new(Pool::new(2).map_err(|error| DmaError::Fabric(format!("pool: {error}")))?);
    let server = FabricServer::start(&configuration, Box::new(complete), pool)?;

    // What a peer must hold to be RMA'd against. Carry it on your own control channel.
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

    // `one_target` waits for a full buffer of this byte.
    let payload = vec![0xab_u8; 4096];
    let (done, outcome) = channel();
    let request = TransferRequest {
        client_id: 1,
        peer_address: peer.address,
        remote_key: peer.remote_key,
        remote_address: peer.remote_address,
        buffer: TransferBuffer {
            pointer: payload.as_ptr().cast_mut(),
            length: payload.len(),
        },
        direction: Direction::ToPeer,
        want_checksum: true,
        caller_context: Operation { done },
        parent_id: None,
    };
    if server.submit(request).is_err() {
        return Err(DmaError::Fabric("fabric worker is gone".into()));
    }

    match outcome.recv() {
        Ok(Ok(transferred)) => println!(
            "wrote {} bytes, crc {:?}",
            transferred.bytes, transferred.checksum
        ),
        Ok(Err(error)) => println!("transfer failed: {error}"),
        Err(_) => println!("worker exited without completing the transfer"),
    }
    // Alive and unmodified until the completion: what `TransferBuffer`'s `unsafe impl Send` rests on.
    drop(payload);
    Ok(())
}
