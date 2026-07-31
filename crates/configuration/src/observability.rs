//! Observability configuration: the optional tracing console host.

use std::net::SocketAddr;

/// `[observability]`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ObservabilityConfiguration {
    /// When present, run the tracing console host so remote consoles can stream spans.
    #[serde(default, rename = "console-host")]
    pub console_host: Option<ConsoleHostConfiguration>,
}

/// `[observability.console-host]`. The section's presence enables the host, and `listen` defaults
/// when omitted.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ConsoleHostConfiguration {
    /// Address the console host binds for remote console clients.
    #[serde(default = "default_listen")]
    pub listen: SocketAddr,
}

impl Default for ConsoleHostConfiguration {
    fn default() -> Self {
        Self {
            listen: default_listen(),
        }
    }
}

fn default_listen() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 7777))
}
