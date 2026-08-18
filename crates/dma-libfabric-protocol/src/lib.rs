//! The wire vocabulary shared by the module and the fabric layer: what a client advertises, how a
//! payload is checksummed, and how a DMA failure is reported. Names the protocol, not the transport
//! — no valkey or libfabric type appears here.

mod advertisement;
mod checksum;
mod error;

pub use advertisement::{Advertisement, AdvertisementError, decode_hex, encode_hex, parse_ascii};
pub use checksum::checksum;
pub use error::DmaError;
