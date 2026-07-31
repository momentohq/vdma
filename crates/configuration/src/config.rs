//! The root `Configuration` and TOML parsing, depending on the leaves for their local sections.

use crate::observability::ObservabilityConfiguration;

/// Re-export of the libfabric DMA section so callers configure it through one crate.
pub use dma_libfabric::Configuration as DmaLibfabricConfiguration;

/// Module configuration root.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Configuration {
    /// `[dma-libfabric]`.
    #[serde(default, rename = "dma-libfabric")]
    pub dma_libfabric: DmaLibfabricConfiguration,
    /// `[observability]`.
    #[serde(default)]
    pub observability: ObservabilityConfiguration,
}

impl Configuration {
    /// Parse a configuration from a TOML document.
    pub fn from_toml(document: &str) -> Result<Self, ConfigurationError> {
        toml::from_str(document).map_err(|error| ConfigurationError::Parse(error.to_string()))
    }
}

/// Errors loading configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigurationError {
    #[error("failed to parse configuration: {0}")]
    Parse(String),
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use dma_libfabric::Provider;

    use super::Configuration;

    #[test]
    fn empty_document_uses_defaults() {
        let configuration = Configuration::from_toml("").expect("defaults parse");
        assert_eq!(configuration, Configuration::default());
        assert_eq!(configuration.dma_libfabric.providers, vec![Provider::Tcp]);
    }

    #[test]
    fn example_configs_parse_with_console_host_enabled() {
        let examples = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples/");
        for name in ["tcp", "efa"] {
            let document =
                std::fs::read_to_string(format!("{examples}{name}.toml")).expect("read example");
            let configuration = Configuration::from_toml(&document).expect("example parses");
            assert!(
                configuration.observability.console_host.is_some(),
                "{name}.toml should enable the console host"
            );
        }
    }

    #[test]
    fn console_host_is_disabled_by_default() {
        let configuration = Configuration::from_toml("").expect("defaults parse");
        assert!(configuration.observability.console_host.is_none());
    }

    #[test]
    fn parses_console_host_section() {
        let document = r#"
            [observability.console-host]
            listen = "0.0.0.0:9999"
        "#;
        let configuration = Configuration::from_toml(document).expect("section parses");
        let console_host = configuration
            .observability
            .console_host
            .expect("console host enabled");
        assert_eq!(console_host.listen, "0.0.0.0:9999".parse().expect("addr"));
    }

    #[test]
    fn parses_libfabric_section() {
        let document = r#"
            [dma-libfabric]
            providers = ["efa-direct", "tcp"]
            interfaces = ["rdmap83s0-rdm"]
            bind = "10.0.0.1"
        "#;
        let configuration = Configuration::from_toml(document).expect("section parses");
        assert_eq!(
            configuration.dma_libfabric.providers,
            vec![Provider::EfaDirect, Provider::Tcp]
        );
        assert_eq!(
            configuration.dma_libfabric.interfaces,
            vec!["rdmap83s0-rdm"]
        );
        assert_eq!(
            configuration.dma_libfabric.bind.as_deref(),
            Some("10.0.0.1")
        );
    }
}
