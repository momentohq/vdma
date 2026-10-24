# dma-libfabric

Server-initiated one-sided RMA over [libfabric](https://ofiwg.github.io/libfabric/). The server
writes (`fi_writemsg`) or reads (`fi_read`) a buffer that a peer has registered and advertised.

## Requirements

- libfabric, discoverable via `pkg-config`
- Rust 1.95+

## Providers

| `Provider`   | libfabric provider | Notes                                    |
|--------------|--------------------|------------------------------------------|
| `Tcp`        | `tcp`              | No RDMA hardware needed.                 |
| `EfaDirect`  | `efa`              | Pinned to the `efa-direct` fabric name.  |

## Usage

1. Build a `Configuration` and a `Pool`, then `FabricService::start` with a `BatchCompleter`.
   `asynchronous::FabricService` returns futures instead of taking a completer.
2. Implement `Operands` on your per-transfer context: `source()` for `Direction::ToPeer`,
   `allocate()` for `Direction::FromPeer`.
3. Send `FabricService::local_address()` to the peer over your own control channel (e.g., RESP).
4. Receive the peer's `Advertisement` (address, rkey, remote address) over that channel.
5. `submit` a `TransferRequest`. The `Completion` comes back with your context and the `Outcome`.
6. `remove_peer` when the peer disconnects.

`register` pins a caller-owned `MemoryRegion`, and operands within it skip `fi_mr_reg`. Without
doing that, providers that need `FI_MR_LOCAL` (e.g., efa) register each operand per transfer.
Alternatively your `Operands` can return a `CacheableSpan` and you call `invalidate` before
that memory span is reclaimed.

## Examples

```text
# terminal 1: passive peer; prints `advertisement: <address-hex> <rkey> <remote-addr>`
cargo run -p dma-libfabric --example one_target -- 127.0.0.1
# terminal 2
cargo run -p dma-libfabric --example one_transfer -- 127.0.0.1 <address-hex> <rkey> <remote-addr>
```

efa-direct reverses the order. On efa, the peer must hold the initiator's address before it can be
referenced.

```text
# instance 1: initiator; prints `local address: <initiator-hex>`, then waits on stdin
cargo run -p dma-libfabric --example one_transfer -- --efa
# instance 2: passive peer; prints `advertisement: <address-hex> <rkey> <remote-addr>`
cargo run -p dma-libfabric --example one_target -- --efa <initiator-hex>
# paste <address-hex> <rkey> <remote-addr> into instance 1's stdin
```

| Example            | Shows                                              |
|--------------------|----------------------------------------------------|
| `one_target`       | The passive peer the other examples RMA against.   |
| `one_transfer`     | One `ToPeer` write with a `BatchCompleter`.        |
| `async_transfer`   | The same with `asynchronous::FabricService`.       |
| `registered_arena` | A payload inside a pre-registered `MemoryRegion`.  |
| `read_from_peer`   | One `FromPeer` read; run `one_target --read`.      |

## License

Apache-2.0
