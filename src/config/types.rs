//! Configuration types for taterfs-rs.
//!
//! This module defines the structures used to represent application configuration
//! as parsed from an INI-format config file.

use std::collections::HashMap;
use std::path::PathBuf;

// =============================================================================
// Primitive Types
// =============================================================================

/// A byte size that can be parsed from strings like "100MB", "1GB", etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(pub u64);

/// Represents a capacity limit that can be inherited, disabled, or set.
///
/// - `Inherit`: Not specified in config; inherit from parent section
/// - `Disabled`: Explicitly set to "none"; no capacity manager will be used
/// - `Value(T)`: Specific limit value
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Limit<T> {
    Inherit,
    Disabled,
    Value(T),
}

// =============================================================================
// Capacity Limits (shared across network, s3, repository, filestore)
// =============================================================================

/// Capacity limits for resource throttling.
#[derive(Debug, Clone)]
pub struct CapacityLimits {
    pub max_concurrent_requests: Limit<u32>,
    pub max_requests_per_second: Limit<u32>,
    pub max_read_bytes_per_second: Limit<ByteSize>,
    pub max_write_bytes_per_second: Limit<ByteSize>,
    pub max_total_bytes_per_second: Limit<ByteSize>,
}

// =============================================================================
// S3 Settings (shared across s3, repository, filestore)
// =============================================================================

/// S3-specific connection settings.
#[derive(Debug, Clone)]
pub struct S3Settings {
    pub endpoint_url: Option<String>,
    pub region: Option<String>,
}

// =============================================================================
// Config Sections
// =============================================================================

/// [cache] section - local durable cache configuration.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Path to the cache database directory.
    pub path: PathBuf,
    /// If true, caching is disabled entirely.
    pub no_cache: bool,
    /// How often to flush pending writes to disk (milliseconds).
    pub pending_writes_flush_period_ms: u64,
    /// Maximum number of pending writes before forcing a flush.
    pub pending_writes_max_count: usize,
    /// Maximum total size of pending writes before forcing a flush.
    pub pending_writes_max_size: ByteSize,
    /// Maximum memory for the key-value read cache.
    pub max_memory_size: ByteSize,
    /// Maximum memory for the repository object cache.
    pub max_object_memory_size: ByteSize,
}

/// [memory] section - memory usage limits.
#[derive(Debug, Clone)]
pub struct MemoryConfig {
    pub max: Limit<ByteSize>,
}

/// [filestores] section - global filestore settings.
#[derive(Debug, Clone)]
pub struct FilestoresConfig {
    /// Global ignore patterns applied to all file store scans.
    /// These are parsed as gitignore patterns.
    pub global_ignores: Vec<String>,
}

/// [network] section - defaults for all network-based resources.
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    pub limits: CapacityLimits,
}

/// [s3] section - defaults for all S3-based resources.
#[derive(Debug, Clone)]
pub struct S3Config {
    pub settings: S3Settings,
    pub limits: CapacityLimits,
}

/// [repository.{name}] section - named repository configuration.
#[derive(Debug, Clone)]
pub struct RepositoryConfig {
    pub url: String,
    pub settings: S3Settings,
    pub limits: CapacityLimits,
    /// If true, disable caching for this repository (overrides global setting).
    pub no_cache: bool,
}

/// [filestore.{name}] section - named file store configuration.
#[derive(Debug, Clone)]
pub struct FilestoreConfig {
    pub url: String,
    pub settings: S3Settings,
    pub limits: CapacityLimits,
}

// =============================================================================
// Top-Level Config
// =============================================================================

/// Complete application configuration as parsed from config file.
#[derive(Debug, Clone)]
pub struct Config {
    pub cache: CacheConfig,
    pub memory: MemoryConfig,
    pub filestores_config: FilestoresConfig,
    pub network: NetworkConfig,
    pub s3: S3Config,
    pub repositories: HashMap<String, RepositoryConfig>,
    pub filestores: HashMap<String, FilestoreConfig>,
}
