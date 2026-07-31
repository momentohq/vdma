//! The one place a module error becomes a RESP error reply.

use valkey_module::ValkeyError;

/// Wrap an error as a RESP `-ERR` reply. Every `dma.*` command failure goes through here, so the
/// prefix is written once rather than at each call site.
pub fn command_error(error: impl std::fmt::Display) -> ValkeyError {
    ValkeyError::String(format!("ERR {error}"))
}
