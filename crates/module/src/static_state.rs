//! Global module state, set once during initialization.

use std::sync::{Mutex, OnceLock};

use configuration::Configuration;

/// Path to the TOML config file, wired to a valkey string configuration parameter.
static CONFIG_FILE: Mutex<String> = Mutex::new(String::new());

/// The loaded configuration, set once during `init`.
static CONFIGURATION: OnceLock<Configuration> = OnceLock::new();

/// Only for wiring valkey's string configuration to this static. Valkey serializes init and the
/// parameter is registered immutable, so nothing mutates it concurrently.
pub unsafe fn module_config_file() -> &'static Mutex<String> {
    &CONFIG_FILE
}

/// Read the configured config-file path.
pub fn config_file_path() -> Option<String> {
    let path = CONFIG_FILE.lock().ok()?.clone();
    if path.is_empty() { None } else { Some(path) }
}

/// Set the global configuration. Called once during init.
pub fn set_configuration(configuration: Configuration) {
    let _ = CONFIGURATION.set(configuration);
}

/// The loaded configuration, or the defaults if init has not set it.
pub fn configuration() -> Configuration {
    CONFIGURATION.get().cloned().unwrap_or_default()
}
