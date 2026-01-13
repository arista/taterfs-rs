//! Configuration helper for interpreting config values.
//!
//! The `ConfigHelper` wraps a `Config` and provides methods for interpreting
//! configuration values, such as resolving inherited limits.

use super::{ByteSize, CapacityLimits, Config, FilestoreConfig, Limit, RepositoryConfig};

// =============================================================================
// Resolved Types
// =============================================================================

/// Fully resolved capacity limits with inheritance applied.
///
/// `None` means the limit is disabled; `Some(value)` means use that value.
#[derive(Debug, Clone, Default)]
pub struct ResolvedCapacityLimits {
    pub max_concurrent_requests: Option<u32>,
    pub max_requests_per_second: Option<u32>,
    pub max_read_bytes_per_second: Option<u64>,
    pub max_write_bytes_per_second: Option<u64>,
    pub max_total_bytes_per_second: Option<u64>,
}

/// Fully resolved S3 settings with inheritance applied.
#[derive(Debug, Clone, Default)]
pub struct ResolvedS3Settings {
    pub endpoint_url: Option<String>,
    pub region: Option<String>,
}

// =============================================================================
// ConfigHelper
// =============================================================================

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

    // =========================================================================
    // Capacity Limits Resolution
    // =========================================================================

    /// Resolve capacity limits for the [network] section.
    ///
    /// Network is the base level, so `Inherit` values should not appear
    /// (they would indicate a bug in config loading).
    pub fn resolve_network_limits(&self) -> ResolvedCapacityLimits {
        resolve_limits_base(&self.config.network.limits)
    }

    /// Resolve capacity limits for the [s3] section.
    ///
    /// S3 limits inherit from network limits.
    pub fn resolve_s3_limits(&self) -> ResolvedCapacityLimits {
        let network = self.resolve_network_limits();
        resolve_limits_with_parent(&self.config.s3.limits, &network)
    }

    /// Resolve capacity limits for a named repository.
    ///
    /// Repository limits inherit from s3 limits, which inherit from network limits.
    /// Returns `None` if the repository is not found.
    pub fn resolve_repository_limits(&self, name: &str) -> Option<ResolvedCapacityLimits> {
        let repo = self.config.repositories.get(name)?;
        Some(self.resolve_repository_config_limits(repo))
    }

    /// Resolve capacity limits for a repository config.
    ///
    /// Useful when you have the config from a URL parse rather than by name.
    pub fn resolve_repository_config_limits(&self, repo: &RepositoryConfig) -> ResolvedCapacityLimits {
        let s3 = self.resolve_s3_limits();
        resolve_limits_with_parent(&repo.limits, &s3)
    }

    /// Resolve capacity limits for a named filestore.
    ///
    /// Filestore limits inherit from s3 limits, which inherit from network limits.
    /// Returns `None` if the filestore is not found.
    pub fn resolve_filestore_limits(&self, name: &str) -> Option<ResolvedCapacityLimits> {
        let store = self.config.filestores.get(name)?;
        Some(self.resolve_filestore_config_limits(store))
    }

    /// Resolve capacity limits for a filestore config.
    ///
    /// Useful when you have the config from a URL parse rather than by name.
    pub fn resolve_filestore_config_limits(&self, store: &FilestoreConfig) -> ResolvedCapacityLimits {
        let s3 = self.resolve_s3_limits();
        resolve_limits_with_parent(&store.limits, &s3)
    }

    // =========================================================================
    // S3 Settings Resolution
    // =========================================================================

    /// Resolve S3 settings for the [s3] section.
    pub fn resolve_s3_settings(&self) -> ResolvedS3Settings {
        ResolvedS3Settings {
            endpoint_url: self.config.s3.settings.endpoint_url.clone(),
            region: self.config.s3.settings.region.clone(),
        }
    }

    /// Resolve S3 settings for a named repository.
    ///
    /// Repository settings inherit from s3 settings.
    /// Returns `None` if the repository is not found.
    pub fn resolve_repository_settings(&self, name: &str) -> Option<ResolvedS3Settings> {
        let repo = self.config.repositories.get(name)?;
        Some(self.resolve_repository_config_settings(repo))
    }

    /// Resolve S3 settings for a repository config.
    pub fn resolve_repository_config_settings(&self, repo: &RepositoryConfig) -> ResolvedS3Settings {
        let s3 = self.resolve_s3_settings();
        ResolvedS3Settings {
            endpoint_url: repo.settings.endpoint_url.clone().or(s3.endpoint_url),
            region: repo.settings.region.clone().or(s3.region),
        }
    }

    /// Resolve S3 settings for a named filestore.
    ///
    /// Filestore settings inherit from s3 settings.
    /// Returns `None` if the filestore is not found.
    pub fn resolve_filestore_settings(&self, name: &str) -> Option<ResolvedS3Settings> {
        let store = self.config.filestores.get(name)?;
        Some(self.resolve_filestore_config_settings(store))
    }

    /// Resolve S3 settings for a filestore config.
    pub fn resolve_filestore_config_settings(&self, store: &FilestoreConfig) -> ResolvedS3Settings {
        let s3 = self.resolve_s3_settings();
        ResolvedS3Settings {
            endpoint_url: store.settings.endpoint_url.clone().or(s3.endpoint_url),
            region: store.settings.region.clone().or(s3.region),
        }
    }

    // =========================================================================
    // Repository/Filestore Lookup
    // =========================================================================

    /// Get a repository config by name.
    pub fn get_repository(&self, name: &str) -> Option<&RepositoryConfig> {
        self.config.repositories.get(name)
    }

    /// Get a filestore config by name.
    pub fn get_filestore(&self, name: &str) -> Option<&FilestoreConfig> {
        self.config.filestores.get(name)
    }
}

impl From<Config> for ConfigHelper {
    fn from(config: Config) -> Self {
        Self::new(config)
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Resolve a single limit value against an optional parent value.
fn resolve_limit<T: Clone>(limit: &Limit<T>, parent: Option<T>) -> Option<T> {
    match limit {
        Limit::Value(v) => Some(v.clone()),
        Limit::Disabled => None,
        Limit::Inherit => parent,
    }
}

/// Resolve limits at the base level (network), where Inherit falls back to None.
fn resolve_limits_base(limits: &CapacityLimits) -> ResolvedCapacityLimits {
    ResolvedCapacityLimits {
        max_concurrent_requests: resolve_limit(&limits.max_concurrent_requests, None),
        max_requests_per_second: resolve_limit(&limits.max_requests_per_second, None),
        max_read_bytes_per_second: resolve_limit(&limits.max_read_bytes_per_second, None)
            .map(|b| b.0),
        max_write_bytes_per_second: resolve_limit(&limits.max_write_bytes_per_second, None)
            .map(|b| b.0),
        max_total_bytes_per_second: resolve_limit(&limits.max_total_bytes_per_second, None)
            .map(|b| b.0),
    }
}

/// Resolve limits with a parent level for inheritance.
fn resolve_limits_with_parent(
    limits: &CapacityLimits,
    parent: &ResolvedCapacityLimits,
) -> ResolvedCapacityLimits {
    ResolvedCapacityLimits {
        max_concurrent_requests: resolve_limit(
            &limits.max_concurrent_requests,
            parent.max_concurrent_requests,
        ),
        max_requests_per_second: resolve_limit(
            &limits.max_requests_per_second,
            parent.max_requests_per_second,
        ),
        max_read_bytes_per_second: resolve_limit(
            &limits.max_read_bytes_per_second,
            parent.max_read_bytes_per_second.map(ByteSize),
        )
        .map(|b| b.0),
        max_write_bytes_per_second: resolve_limit(
            &limits.max_write_bytes_per_second,
            parent.max_write_bytes_per_second.map(ByteSize),
        )
        .map(|b| b.0),
        max_total_bytes_per_second: resolve_limit(
            &limits.max_total_bytes_per_second,
            parent.max_total_bytes_per_second.map(ByteSize),
        )
        .map(|b| b.0),
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CacheConfig, MemoryConfig, NetworkConfig, S3Config, S3Settings,
    };
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn test_config() -> Config {
        Config {
            cache: CacheConfig {
                path: PathBuf::from("/tmp/cache"),
                no_cache: false,
            },
            memory: MemoryConfig {
                max: Limit::Value(ByteSize(100 * 1024 * 1024)),
            },
            network: NetworkConfig {
                limits: CapacityLimits {
                    max_concurrent_requests: Limit::Value(40),
                    max_requests_per_second: Limit::Value(100),
                    max_read_bytes_per_second: Limit::Value(ByteSize(100 * 1024 * 1024)),
                    max_write_bytes_per_second: Limit::Value(ByteSize(100 * 1024 * 1024)),
                    max_total_bytes_per_second: Limit::Disabled,
                },
            },
            s3: S3Config {
                settings: S3Settings {
                    endpoint_url: Some("http://localhost:9000".to_string()),
                    region: None,
                },
                limits: CapacityLimits {
                    max_concurrent_requests: Limit::Value(20), // Override network
                    max_requests_per_second: Limit::Inherit,   // Inherit from network
                    max_read_bytes_per_second: Limit::Inherit,
                    max_write_bytes_per_second: Limit::Inherit,
                    max_total_bytes_per_second: Limit::Inherit,
                },
            },
            repositories: {
                let mut map = HashMap::new();
                map.insert(
                    "main".to_string(),
                    RepositoryConfig {
                        url: "s3://bucket/prefix".to_string(),
                        settings: S3Settings {
                            endpoint_url: None,
                            region: Some("us-west-2".to_string()),
                        },
                        limits: CapacityLimits {
                            max_concurrent_requests: Limit::Value(10), // Override s3
                            max_requests_per_second: Limit::Inherit,   // Inherit from s3
                            max_read_bytes_per_second: Limit::Disabled, // Disable
                            max_write_bytes_per_second: Limit::Inherit,
                            max_total_bytes_per_second: Limit::Inherit,
                        },
                    },
                );
                map
            },
            filestores: HashMap::new(),
        }
    }

    #[test]
    fn test_resolve_network_limits() {
        let helper = ConfigHelper::new(test_config());
        let limits = helper.resolve_network_limits();

        assert_eq!(limits.max_concurrent_requests, Some(40));
        assert_eq!(limits.max_requests_per_second, Some(100));
        assert_eq!(limits.max_read_bytes_per_second, Some(100 * 1024 * 1024));
        assert_eq!(limits.max_total_bytes_per_second, None); // Disabled
    }

    #[test]
    fn test_resolve_s3_limits() {
        let helper = ConfigHelper::new(test_config());
        let limits = helper.resolve_s3_limits();

        assert_eq!(limits.max_concurrent_requests, Some(20)); // Overridden
        assert_eq!(limits.max_requests_per_second, Some(100)); // Inherited from network
        assert_eq!(limits.max_total_bytes_per_second, None); // Inherited disabled
    }

    #[test]
    fn test_resolve_repository_limits() {
        let helper = ConfigHelper::new(test_config());
        let limits = helper.resolve_repository_limits("main").unwrap();

        assert_eq!(limits.max_concurrent_requests, Some(10)); // Overridden
        assert_eq!(limits.max_requests_per_second, Some(100)); // Inherited from s3 -> network
        assert_eq!(limits.max_read_bytes_per_second, None); // Disabled at repo level
        assert_eq!(limits.max_write_bytes_per_second, Some(100 * 1024 * 1024)); // Inherited
    }

    #[test]
    fn test_resolve_repository_settings() {
        let helper = ConfigHelper::new(test_config());
        let settings = helper.resolve_repository_settings("main").unwrap();

        // endpoint_url inherited from s3
        assert_eq!(settings.endpoint_url, Some("http://localhost:9000".to_string()));
        // region overridden at repo level
        assert_eq!(settings.region, Some("us-west-2".to_string()));
    }

    #[test]
    fn test_resolve_nonexistent_repository() {
        let helper = ConfigHelper::new(test_config());
        assert!(helper.resolve_repository_limits("nonexistent").is_none());
        assert!(helper.resolve_repository_settings("nonexistent").is_none());
    }
}
