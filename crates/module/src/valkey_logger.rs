//! Lifecycle logging to valkey's native log channel.
//!
//! Only for lifecycle events — startup, configuration load, fatal errors — so operators see them in
//! the valkey log regardless of the `tracing-cache` setup handling general API observability.

use valkey_module::Context;

/// Log a normal lifecycle event at notice level.
pub fn lifecycle(context: &Context, message: &str) {
    context.log_notice(message);
}

/// Log a fatal lifecycle failure at warning level.
pub fn fatal(context: &Context, message: &str) {
    context.log_warning(message);
}

/// Log a non-fatal lifecycle warning needing operator attention.
pub fn warning(context: &Context, message: &str) {
    context.log_warning(message);
}
