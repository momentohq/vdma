//! Errors surfaced by the DMA layer.

/// Errors surfaced by the DMA layer.
#[derive(Debug, thiserror::Error)]
pub enum DmaError {
    /// A transfer failed at the transport level.
    #[error("transfer failed: {0}")]
    Transfer(String),
    /// A libfabric setup or control operation failed.
    #[error("fabric error: {0}")]
    Fabric(String),
}
