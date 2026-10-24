//! The wire vocabulary shareable by the fabric layer and applications. Client advertisements, how a
//! payload is checksummed, and how a DMA failure is reported.
#![warn(missing_docs)]

mod advertisement;
mod checksum;
mod error;

pub use advertisement::{Advertisement, AdvertisementError, decode_hex, encode_hex, parse_ascii};
pub use checksum::checksum;
pub use error::DmaError;
