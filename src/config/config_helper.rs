//! Configuration helper for interpreting config values.
//!
//! The `ConfigHelper` wraps a `Config` and provides methods for interpreting
//! configuration values, such as resolving inherited limits.

use super::Config;

/// Helper for interpreting configuration values.
///
/// Wraps a `Config` and provides methods for resolving inherited values,
/// computing effective limits, and other config interpretation tasks.
#[derive(Debug, Clone)]
pub struct ConfigHelper {
    config: Config,
}

impl ConfigHelper {
    /// Create a new ConfigHelper wrapping the given config.
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Get a reference to the underlying config.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Consume the helper and return the underlying config.
    pub fn into_config(self) -> Config {
        self.config
    }
}

impl From<Config> for ConfigHelper {
    fn from(config: Config) -> Self {
        Self::new(config)
    }
}
