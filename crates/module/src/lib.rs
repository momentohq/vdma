//! The valkey module: the `dma.*` commands, wrapping valkey's imperative C API behind RAII.

mod entry;
mod memory;
mod observability;
mod session;
mod static_state;
mod transfer;
mod valkey_error;
mod valkey_logger;
