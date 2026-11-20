//! The fabric worker: one event-loop thread per endpoint that admits, posts, and reaps transfers.

mod event_loop;
mod in_flight;
mod ledger;
mod op_header;
mod prepared;
mod worker;

pub use event_loop::worker_main;
pub(crate) use op_header::{OpHeader, OpKind};
