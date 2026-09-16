//! Like `one_transfer`, but the payload is a slice of an arena you registered up front, so the
//! transfer costs no `fi_mr_reg` at all.
//!
//! Run `one_target` first; it prints the three advertisement fields to paste here.
//!
//! ```text
//! cargo run -p dma-libfabric --example one_target        -- 127.0.0.1
//! cargo run -p dma-libfabric --example registered_arena  -- 127.0.0.1 <address-hex> <rkey> <remote-addr>
//! ```
//!
//! `one_transfer` registers and closes a region per transfer, because nothing tells this crate when
//! the payload's pages go away. Here you own the memory, so you say so once with
//! [`FabricService::register`] and every operand inside the span is a cache hit afterwards. The
//! arena is yours end to end: this crate reads its span once and never touches the bytes.

use std::sync::Arc;
use std::sync::mpsc::{Sender, channel};

use dma_libfabric::{
    Completion, Direction, FabricService, MemoryRegion, Operands, Outcome, Pool, TransferRequest,
};
use dma_libfabric_protocol::{DmaError, encode_hex};

mod common;

/// One slot of the arena, plus the handle that keeps the arena registered and mapped.
///
/// Holding the `MemoryRegion` here is what makes this sound: the transfer's operand points into the
/// arena, and this clone is what stops the arena being freed while the RMA is on the wire. A
/// `TContext` is `Send + 'static`, so it could not have held a borrow instead.
struct Slot {
    region: MemoryRegion<Vec<u8>>,
    at: usize,
    length: usize,
    done: Sender<Outcome>,
}

impl Operands for Slot {
    /// The bytes this transfer writes into the peer, read straight out of the arena. No copy, and no
    /// registration — `register` already covered this span.
    fn source(&self) -> Option<&[u8]> {
        self.region.storage().get(self.at..self.at + self.length)
    }
}

/// Batched, so a reply path takes its lock once per batch rather than per transfer.
fn complete(completions: std::vec::Drain<'_, Completion<Slot>>) {
    for completion in completions {
        let _ = completion.caller_context.done.send(completion.outcome);
    }
}

fn main() -> Result<(), DmaError> {
    let common::Arguments {
        configuration,
        peer,
    } = common::parse()?;

    let pool = Arc::new(Pool::new(2).map_err(|error| DmaError::Fabric(format!("pool: {error}")))?);
    let server = FabricService::start(&configuration, Box::new(complete), pool)?;

    println!("local address: {}", encode_hex(server.local_address()));

    // Your allocation, your layout. A `Vec` here for brevity; an mmap with hugepages, a NUMA-bound
    // slab, or a bump allocator over an anonymous mapping all satisfy the same bound.
    const SLOT: usize = 4096;
    let mut arena = vec![0u8; 64 * SLOT];
    // `one_target` waits for a full buffer of this byte.
    arena[SLOT..2 * SLOT].fill(0xab);

    // Registered once, on this server's device, before any transfer runs. A failure here is the
    // honest place for it: `max_mr_size`, `RLIMIT_MEMLOCK` and ENOMEM all surface now rather than
    // stalling a worker thread mid-run.
    let region = server.register(arena)?;
    println!("registered {} bytes", region.length());

    let peer = match peer {
        Some(peer) => peer,
        None => {
            println!("paste the target's <address-hex> <rkey> <remote-addr>:");
            common::peer_from_stdin()?
        }
    };

    let (done, outcome) = channel();
    let request = TransferRequest {
        client_id: 1,
        peer_address: peer.address,
        remote_key: peer.remote_key,
        remote_address: peer.remote_address,
        direction: Direction::ToPeer,
        want_checksum: true,
        // The clone is the point: the arena outlives the transfer because this context holds it.
        caller_context: Slot {
            region: region.clone(),
            at: SLOT,
            length: SLOT,
            done,
        },
        parent_id: None,
    };
    if server.submit(request).is_err() {
        return Err(DmaError::Fabric("fabric worker is gone".into()));
    }

    match outcome.recv() {
        Ok(Ok(transferred)) => println!(
            "wrote {} bytes from the arena, crc {:?}",
            transferred.bytes, transferred.checksum
        ),
        Ok(Err(error)) => println!("transfer failed: {error}"),
        Err(_) => println!("the worker exited before the transfer completed"),
    }

    // Dropping `region` retires the registration and then frees the arena, in that order. It could
    // not have run any earlier: the transfer's context held a clone until completion.
    Ok(())
}
