//! The parent `Configuration` type and TOML parsing for the whole module, aggregating each concrete
//! implementation's local section.

mod config;
mod jemalloc;
mod observability;

pub use config::{Configuration, ConfigurationError, DmaLibfabricConfiguration};
pub use jemalloc::JemallocConfiguration;
pub use observability::{ConsoleHostConfiguration, ObservabilityConfiguration};
