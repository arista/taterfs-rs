//! Configuration file reading and parsing.
//!
//! This module handles locating, reading, and parsing INI-format configuration files,
//! with support for layered overrides.

use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};

use configparser::ini::Ini;
use thiserror::Error;

use super::{
    ByteSize, CacheConfig, CapacityLimits, Config, FilestoreConfig, FilestoresConfig, Limit,
    MemoryConfig, NetworkConfig, RepositoryConfig, S3Config, S3Settings,
};

// =============================================================================
// Constants - Default Values
// =============================================================================

const DEFAULT_CACHE_PATH: &str = "/tmp/tfsconfig-cache";
const DEFAULT_CACHE_NO_CACHE: bool = false;
const DEFAULT_MEMORY_MAX: u64 = 100 * 1024 * 1024; // 100MB
const DEFAULT_NETWORK_MAX_CONCURRENT_REQUESTS: u32 = 40;
const DEFAULT_NETWORK_MAX_REQUESTS_PER_SECOND: u32 = 100;
const DEFAULT_NETWORK_MAX_READ_BYTES_PER_SECOND: u64 = 100 * 1024 * 1024; // 100MB
const DEFAULT_NETWORK_MAX_WRITE_BYTES_PER_SECOND: u64 = 100 * 1024 * 1024; // 100MB
const DEFAULT_FILESTORES_GLOBAL_IGNORES: &str = ".git/,.tfs/";

const ENV_CONFIG_FILE: &str = "TFS_CONFIG_FILE";
const DEFAULT_CONFIG_FILENAME: &str = ".tfsconfig";

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur when reading configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("config file not found: {0}")]
    FileNotFound(PathBuf),

    #[error("failed to read config file {path}: {source}")]
    ReadError {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to parse config file {path}: {message}")]
    ParseError { path: PathBuf, message: String },

    #[error("invalid byte size '{value}': {message}")]
    InvalidByteSize { value: String, message: String },

    #[error("invalid integer '{value}': {source}")]
    InvalidInteger {
        value: String,
        source: std::num::ParseIntError,
    },

    #[error("invalid boolean '{value}' for key '{key}'")]
    InvalidBoolean { key: String, value: String },

    #[error("invalid override key '{key}': {message}")]
    InvalidOverrideKey { key: String, message: String },

    #[error("missing required field '{field}' in section '{section}'")]
    MissingRequiredField { section: String, field: String },
}

/// Result type for config operations.
pub type Result<T> = std::result::Result<T, ConfigError>;

// =============================================================================
// ConfigSource
// =============================================================================

/// Specifies how to locate and layer configuration.
#[derive(Debug, Clone, Default)]
pub struct ConfigSource {
    /// Explicit config file path from CLI. If specified and doesn't exist, error.
    /// If None, fall back to TFS_CONFIG_FILE env var, then ~/.tfsconfig.
    pub config_file: Option<PathBuf>,

    /// Additional override config file (layered on top of base config).
    pub override_file: Option<PathBuf>,

    /// Individual key=value overrides (applied last).
    /// Keys use dot-notation: "cache.path", "repository.myrepo.url"
    pub overrides: Vec<(String, String)>,
}

// =============================================================================
// ByteSize Parsing
// =============================================================================

impl ByteSize {
    /// Parse a byte size from a string like "100MB", "1GB", "500KB", or plain "1024".
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.is_empty() {
            return Err(ConfigError::InvalidByteSize {
                value: s.to_string(),
                message: "empty string".to_string(),
            });
        }

        // Find where the numeric part ends
        let num_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());

        if num_end == 0 {
            return Err(ConfigError::InvalidByteSize {
                value: s.to_string(),
                message: "no numeric value".to_string(),
            });
        }

        let num_str = &s[..num_end];
        let suffix = s[num_end..].trim().to_uppercase();

        let base: u64 = num_str.parse().map_err(|e| ConfigError::InvalidByteSize {
            value: s.to_string(),
            message: format!("invalid number: {}", e),
        })?;

        let multiplier: u64 = match suffix.as_str() {
            "" | "B" => 1,
            "K" | "KB" => 1024,
            "M" | "MB" => 1024 * 1024,
            "G" | "GB" => 1024 * 1024 * 1024,
            "T" | "TB" => 1024 * 1024 * 1024 * 1024,
            _ => {
                return Err(ConfigError::InvalidByteSize {
                    value: s.to_string(),
                    message: format!("unknown suffix '{}'", suffix),
                });
            }
        };

        Ok(ByteSize(base.saturating_mul(multiplier)))
    }
}

// =============================================================================
// Limit Parsing
// =============================================================================

/// Parse an optional limit value. Returns Inherit if the key is not present.
fn parse_limit_u32(ini: &Ini, section: &str, key: &str) -> Result<Limit<u32>> {
    match ini.get(section, key) {
        None => Ok(Limit::Inherit),
        Some(v) if v.eq_ignore_ascii_case("none") => Ok(Limit::Disabled),
        Some(v) => {
            let val: u32 = v.parse().map_err(|e| ConfigError::InvalidInteger {
                value: v.to_string(),
                source: e,
            })?;
            Ok(Limit::Value(val))
        }
    }
}

/// Parse an optional byte size limit. Returns Inherit if the key is not present.
fn parse_limit_bytesize(ini: &Ini, section: &str, key: &str) -> Result<Limit<ByteSize>> {
    match ini.get(section, key) {
        None => Ok(Limit::Inherit),
        Some(v) if v.eq_ignore_ascii_case("none") => Ok(Limit::Disabled),
        Some(v) => Ok(Limit::Value(ByteSize::parse(&v)?)),
    }
}

/// Parse a boolean value.
fn parse_bool(ini: &Ini, section: &str, key: &str, default: bool) -> Result<bool> {
    match ini.get(section, key) {
        None => Ok(default),
        Some(v) => match v.to_lowercase().as_str() {
            "true" | "yes" | "1" => Ok(true),
            "false" | "no" | "0" => Ok(false),
            _ => Err(ConfigError::InvalidBoolean {
                key: key.to_string(),
                value: v.to_string(),
            }),
        },
    }
}

// =============================================================================
// Config File Resolution
// =============================================================================

/// Information about how the config file was resolved.
#[derive(Debug)]
pub struct ResolvedConfigFile {
    /// The path to the config file, if one was found.
    pub path: Option<PathBuf>,
    /// Warning message if env var pointed to nonexistent file.
    pub warning: Option<String>,
}

/// Resolve which config file to use based on the ConfigSource and environment.
fn resolve_config_file(source: &ConfigSource) -> Result<ResolvedConfigFile> {
    // If explicit path provided, it must exist
    if let Some(ref path) = source.config_file {
        if path.exists() {
            return Ok(ResolvedConfigFile {
                path: Some(path.clone()),
                warning: None,
            });
        } else {
            return Err(ConfigError::FileNotFound(path.clone()));
        }
    }

    // Check environment variable
    if let Ok(env_path) = env::var(ENV_CONFIG_FILE) {
        let path = PathBuf::from(&env_path);
        if path.exists() {
            return Ok(ResolvedConfigFile {
                path: Some(path),
                warning: None,
            });
        } else {
            // Warn but continue with defaults
            return Ok(ResolvedConfigFile {
                path: None,
                warning: Some(format!(
                    "config file specified by {} does not exist: {}",
                    ENV_CONFIG_FILE, env_path
                )),
            });
        }
    }

    // Check ~/.tfsconfig
    if let Some(home) = home_dir() {
        let default_path = home.join(DEFAULT_CONFIG_FILENAME);
        if default_path.exists() {
            return Ok(ResolvedConfigFile {
                path: Some(default_path),
                warning: None,
            });
        }
    }

    // No config file found
    Ok(ResolvedConfigFile {
        path: None,
        warning: None,
    })
}

/// Get the user's home directory.
fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME").map(PathBuf::from)
}

// =============================================================================
// Default Config
// =============================================================================

/// Parse a comma-separated string into a Vec of trimmed strings.
fn parse_comma_separated(s: &str) -> Vec<String> {
    s.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Create a Config with all default values.
fn default_config() -> Config {
    Config {
        cache: CacheConfig {
            path: PathBuf::from(DEFAULT_CACHE_PATH),
            no_cache: DEFAULT_CACHE_NO_CACHE,
        },
        memory: MemoryConfig {
            max: Limit::Value(ByteSize(DEFAULT_MEMORY_MAX)),
        },
        filestores_config: FilestoresConfig {
            global_ignores: parse_comma_separated(DEFAULT_FILESTORES_GLOBAL_IGNORES),
        },
        network: NetworkConfig {
            limits: CapacityLimits {
                max_concurrent_requests: Limit::Value(DEFAULT_NETWORK_MAX_CONCURRENT_REQUESTS),
                max_requests_per_second: Limit::Value(DEFAULT_NETWORK_MAX_REQUESTS_PER_SECOND),
                max_read_bytes_per_second: Limit::Value(ByteSize(
                    DEFAULT_NETWORK_MAX_READ_BYTES_PER_SECOND,
                )),
                max_write_bytes_per_second: Limit::Value(ByteSize(
                    DEFAULT_NETWORK_MAX_WRITE_BYTES_PER_SECOND,
                )),
                max_total_bytes_per_second: Limit::Disabled,
            },
        },
        s3: S3Config {
            settings: S3Settings {
                endpoint_url: None,
                region: None,
            },
            limits: CapacityLimits {
                max_concurrent_requests: Limit::Inherit,
                max_requests_per_second: Limit::Inherit,
                max_read_bytes_per_second: Limit::Inherit,
                max_write_bytes_per_second: Limit::Inherit,
                max_total_bytes_per_second: Limit::Inherit,
            },
        },
        repositories: HashMap::new(),
        filestores: HashMap::new(),
    }
}

// =============================================================================
// INI Parsing
// =============================================================================

/// Parse capacity limits from an INI section.
fn parse_capacity_limits(ini: &Ini, section: &str) -> Result<CapacityLimits> {
    Ok(CapacityLimits {
        max_concurrent_requests: parse_limit_u32(ini, section, "max_concurrent_requests")?,
        max_requests_per_second: parse_limit_u32(ini, section, "max_requests_per_second")?,
        max_read_bytes_per_second: parse_limit_bytesize(ini, section, "max_read_bytes_per_second")?,
        max_write_bytes_per_second: parse_limit_bytesize(
            ini,
            section,
            "max_write_bytes_per_second",
        )?,
        max_total_bytes_per_second: parse_limit_bytesize(
            ini,
            section,
            "max_total_bytes_per_second",
        )?,
    })
}

/// Parse S3 settings from an INI section.
fn parse_s3_settings(ini: &Ini, section: &str) -> S3Settings {
    S3Settings {
        endpoint_url: ini.get(section, "endpoint_url"),
        region: ini.get(section, "region"),
    }
}

/// Apply an INI file's contents to a Config, layering on top of existing values.
fn apply_ini_to_config(config: &mut Config, ini: &Ini) -> Result<()> {
    // [cache] section
    if ini.get("cache", "path").is_some() || ini.get("cache", "no-cache").is_some() {
        if let Some(path) = ini.get("cache", "path") {
            config.cache.path = PathBuf::from(path);
        }
        config.cache.no_cache = parse_bool(ini, "cache", "no-cache", config.cache.no_cache)?;
    }

    // [memory] section
    if ini.get("memory", "max").is_some() {
        let max = parse_limit_bytesize(ini, "memory", "max")?;
        // If not specified in this file, keep existing value
        if !matches!(max, Limit::Inherit) {
            config.memory.max = max;
        }
    }

    // [filestores] section
    if let Some(global_ignores) = ini.get("filestores", "global_ignores") {
        config.filestores_config.global_ignores = parse_comma_separated(&global_ignores);
    }

    // [network] section
    let network_limits = parse_capacity_limits(ini, "network")?;
    apply_limits_if_set(&mut config.network.limits, &network_limits);

    // [s3] section
    let s3_settings = parse_s3_settings(ini, "s3");
    if s3_settings.endpoint_url.is_some() {
        config.s3.settings.endpoint_url = s3_settings.endpoint_url;
    }
    if s3_settings.region.is_some() {
        config.s3.settings.region = s3_settings.region;
    }
    let s3_limits = parse_capacity_limits(ini, "s3")?;
    apply_limits_if_set(&mut config.s3.limits, &s3_limits);

    // [repository.*] and [filestore.*] sections
    let sections: Vec<String> = ini.sections();
    for section_name in sections {
        if let Some(repo_name) = section_name.strip_prefix("repository.") {
            let url =
                ini.get(&section_name, "url")
                    .ok_or_else(|| ConfigError::MissingRequiredField {
                        section: section_name.clone(),
                        field: "url".to_string(),
                    })?;

            let no_cache = parse_bool(ini, &section_name, "no_cache", false)?;

            let repo_config = RepositoryConfig {
                url,
                settings: parse_s3_settings(ini, &section_name),
                limits: parse_capacity_limits(ini, &section_name)?,
                no_cache,
            };

            config
                .repositories
                .insert(repo_name.to_string(), repo_config);
        }

        if let Some(store_name) = section_name.strip_prefix("filestore.") {
            let url =
                ini.get(&section_name, "url")
                    .ok_or_else(|| ConfigError::MissingRequiredField {
                        section: section_name.clone(),
                        field: "url".to_string(),
                    })?;

            let store_config = FilestoreConfig {
                url,
                settings: parse_s3_settings(ini, &section_name),
                limits: parse_capacity_limits(ini, &section_name)?,
            };

            config
                .filestores
                .insert(store_name.to_string(), store_config);
        }
    }

    Ok(())
}

/// Apply limits from `from` to `to`, but only for non-Inherit values.
fn apply_limits_if_set(to: &mut CapacityLimits, from: &CapacityLimits) {
    if !matches!(from.max_concurrent_requests, Limit::Inherit) {
        to.max_concurrent_requests = from.max_concurrent_requests.clone();
    }
    if !matches!(from.max_requests_per_second, Limit::Inherit) {
        to.max_requests_per_second = from.max_requests_per_second.clone();
    }
    if !matches!(from.max_read_bytes_per_second, Limit::Inherit) {
        to.max_read_bytes_per_second = from.max_read_bytes_per_second.clone();
    }
    if !matches!(from.max_write_bytes_per_second, Limit::Inherit) {
        to.max_write_bytes_per_second = from.max_write_bytes_per_second.clone();
    }
    if !matches!(from.max_total_bytes_per_second, Limit::Inherit) {
        to.max_total_bytes_per_second = from.max_total_bytes_per_second.clone();
    }
}

/// Load and parse an INI file.
fn load_ini(path: &Path) -> Result<Ini> {
    let mut ini = Ini::new();
    ini.load(path).map_err(|e| ConfigError::ParseError {
        path: path.to_path_buf(),
        message: e,
    })?;
    Ok(ini)
}

// =============================================================================
// Override Application
// =============================================================================

/// Apply a single key=value override to the config.
fn apply_override(config: &mut Config, key: &str, value: &str) -> Result<()> {
    let parts: Vec<&str> = key.splitn(3, '.').collect();

    match parts.as_slice() {
        // cache.path, cache.no_cache
        ["cache", param] => apply_cache_override(config, param, value),

        // memory.max
        ["memory", param] => apply_memory_override(config, param, value),

        // filestores.global_ignores
        ["filestores", param] => apply_filestores_override(config, param, value),

        // network.max_concurrent_requests, etc.
        ["network", param] => apply_network_override(config, param, value),

        // s3.endpoint_url, s3.region, s3.max_concurrent_requests, etc.
        ["s3", param] => apply_s3_override(config, param, value),

        // repository.name.param
        ["repository", name, param] => apply_repository_override(config, name, param, value),

        // filestore.name.param
        ["filestore", name, param] => apply_filestore_override(config, name, param, value),

        _ => Err(ConfigError::InvalidOverrideKey {
            key: key.to_string(),
            message: "unrecognized key format".to_string(),
        }),
    }
}

fn apply_cache_override(config: &mut Config, param: &str, value: &str) -> Result<()> {
    match param {
        "path" => {
            config.cache.path = PathBuf::from(value);
            Ok(())
        }
        "no_cache" | "no-cache" => {
            config.cache.no_cache = parse_bool_value(param, value)?;
            Ok(())
        }
        _ => Err(ConfigError::InvalidOverrideKey {
            key: format!("cache.{}", param),
            message: "unknown parameter".to_string(),
        }),
    }
}

fn apply_memory_override(config: &mut Config, param: &str, value: &str) -> Result<()> {
    match param {
        "max" => {
            config.memory.max = parse_limit_value_bytesize(value)?;
            Ok(())
        }
        _ => Err(ConfigError::InvalidOverrideKey {
            key: format!("memory.{}", param),
            message: "unknown parameter".to_string(),
        }),
    }
}

fn apply_filestores_override(config: &mut Config, param: &str, value: &str) -> Result<()> {
    match param {
        "global_ignores" => {
            config.filestores_config.global_ignores = parse_comma_separated(value);
            Ok(())
        }
        _ => Err(ConfigError::InvalidOverrideKey {
            key: format!("filestores.{}", param),
            message: "unknown parameter".to_string(),
        }),
    }
}

fn apply_network_override(config: &mut Config, param: &str, value: &str) -> Result<()> {
    apply_capacity_limit_override(&mut config.network.limits, "network", param, value)
}

fn apply_s3_override(config: &mut Config, param: &str, value: &str) -> Result<()> {
    match param {
        "endpoint_url" => {
            config.s3.settings.endpoint_url = Some(value.to_string());
            Ok(())
        }
        "region" => {
            config.s3.settings.region = Some(value.to_string());
            Ok(())
        }
        _ => apply_capacity_limit_override(&mut config.s3.limits, "s3", param, value),
    }
}

fn apply_repository_override(
    config: &mut Config,
    name: &str,
    param: &str,
    value: &str,
) -> Result<()> {
    let repo = config
        .repositories
        .entry(name.to_string())
        .or_insert_with(|| RepositoryConfig {
            url: String::new(),
            settings: S3Settings {
                endpoint_url: None,
                region: None,
            },
            limits: CapacityLimits {
                max_concurrent_requests: Limit::Inherit,
                max_requests_per_second: Limit::Inherit,
                max_read_bytes_per_second: Limit::Inherit,
                max_write_bytes_per_second: Limit::Inherit,
                max_total_bytes_per_second: Limit::Inherit,
            },
            no_cache: false,
        });

    match param {
        "url" => {
            repo.url = value.to_string();
            Ok(())
        }
        "endpoint_url" => {
            repo.settings.endpoint_url = Some(value.to_string());
            Ok(())
        }
        "region" => {
            repo.settings.region = Some(value.to_string());
            Ok(())
        }
        "no_cache" => {
            repo.no_cache = parse_bool_value(param, value)?;
            Ok(())
        }
        _ => apply_capacity_limit_override(
            &mut repo.limits,
            &format!("repository.{}", name),
            param,
            value,
        ),
    }
}

fn apply_filestore_override(
    config: &mut Config,
    name: &str,
    param: &str,
    value: &str,
) -> Result<()> {
    let store = config
        .filestores
        .entry(name.to_string())
        .or_insert_with(|| FilestoreConfig {
            url: String::new(),
            settings: S3Settings {
                endpoint_url: None,
                region: None,
            },
            limits: CapacityLimits {
                max_concurrent_requests: Limit::Inherit,
                max_requests_per_second: Limit::Inherit,
                max_read_bytes_per_second: Limit::Inherit,
                max_write_bytes_per_second: Limit::Inherit,
                max_total_bytes_per_second: Limit::Inherit,
            },
        });

    match param {
        "url" => {
            store.url = value.to_string();
            Ok(())
        }
        "endpoint_url" => {
            store.settings.endpoint_url = Some(value.to_string());
            Ok(())
        }
        "region" => {
            store.settings.region = Some(value.to_string());
            Ok(())
        }
        _ => apply_capacity_limit_override(
            &mut store.limits,
            &format!("filestore.{}", name),
            param,
            value,
        ),
    }
}

fn apply_capacity_limit_override(
    limits: &mut CapacityLimits,
    section: &str,
    param: &str,
    value: &str,
) -> Result<()> {
    match param {
        "max_concurrent_requests" => {
            limits.max_concurrent_requests = parse_limit_value_u32(value)?;
            Ok(())
        }
        "max_requests_per_second" => {
            limits.max_requests_per_second = parse_limit_value_u32(value)?;
            Ok(())
        }
        "max_read_bytes_per_second" => {
            limits.max_read_bytes_per_second = parse_limit_value_bytesize(value)?;
            Ok(())
        }
        "max_write_bytes_per_second" => {
            limits.max_write_bytes_per_second = parse_limit_value_bytesize(value)?;
            Ok(())
        }
        "max_total_bytes_per_second" => {
            limits.max_total_bytes_per_second = parse_limit_value_bytesize(value)?;
            Ok(())
        }
        _ => Err(ConfigError::InvalidOverrideKey {
            key: format!("{}.{}", section, param),
            message: "unknown parameter".to_string(),
        }),
    }
}

fn parse_bool_value(key: &str, value: &str) -> Result<bool> {
    match value.to_lowercase().as_str() {
        "true" | "yes" | "1" => Ok(true),
        "false" | "no" | "0" => Ok(false),
        _ => Err(ConfigError::InvalidBoolean {
            key: key.to_string(),
            value: value.to_string(),
        }),
    }
}

fn parse_limit_value_u32(value: &str) -> Result<Limit<u32>> {
    if value.eq_ignore_ascii_case("none") {
        Ok(Limit::Disabled)
    } else {
        let v: u32 = value.parse().map_err(|e| ConfigError::InvalidInteger {
            value: value.to_string(),
            source: e,
        })?;
        Ok(Limit::Value(v))
    }
}

fn parse_limit_value_bytesize(value: &str) -> Result<Limit<ByteSize>> {
    if value.eq_ignore_ascii_case("none") {
        Ok(Limit::Disabled)
    } else {
        Ok(Limit::Value(ByteSize::parse(value)?))
    }
}

// =============================================================================
// Main Entry Point
// =============================================================================

/// Result of reading configuration, including any warnings.
#[derive(Debug)]
pub struct ConfigResult {
    /// The parsed configuration.
    pub config: Config,
    /// Any warnings generated during config loading.
    pub warnings: Vec<String>,
}

/// Read and parse configuration from the specified sources.
///
/// Configuration is layered in this order:
/// 1. Built-in defaults
/// 2. Base config file (from CLI, env var, or ~/.tfsconfig)
/// 3. Override config file (if specified)
/// 4. Individual overrides (applied last)
pub fn read_config(source: &ConfigSource) -> Result<ConfigResult> {
    let mut warnings = Vec::new();

    // Start with defaults
    let mut config = default_config();

    // Resolve and apply base config file
    let resolved = resolve_config_file(source)?;
    if let Some(warning) = resolved.warning {
        warnings.push(warning);
    }
    if let Some(ref path) = resolved.path {
        let ini = load_ini(path)?;
        apply_ini_to_config(&mut config, &ini)?;
    }

    // Apply override config file if specified
    if let Some(ref override_path) = source.override_file {
        if !override_path.exists() {
            return Err(ConfigError::FileNotFound(override_path.clone()));
        }
        let ini = load_ini(override_path)?;
        apply_ini_to_config(&mut config, &ini)?;
    }

    // Apply individual overrides
    for (key, value) in &source.overrides {
        apply_override(&mut config, key, value)?;
    }

    Ok(ConfigResult { config, warnings })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytesize_parse() {
        assert_eq!(ByteSize::parse("100").unwrap().0, 100);
        assert_eq!(ByteSize::parse("100B").unwrap().0, 100);
        assert_eq!(ByteSize::parse("100KB").unwrap().0, 100 * 1024);
        assert_eq!(ByteSize::parse("100K").unwrap().0, 100 * 1024);
        assert_eq!(ByteSize::parse("100MB").unwrap().0, 100 * 1024 * 1024);
        assert_eq!(ByteSize::parse("100M").unwrap().0, 100 * 1024 * 1024);
        assert_eq!(ByteSize::parse("1GB").unwrap().0, 1024 * 1024 * 1024);
        assert_eq!(ByteSize::parse("1G").unwrap().0, 1024 * 1024 * 1024);
        assert_eq!(ByteSize::parse("  50MB  ").unwrap().0, 50 * 1024 * 1024);
        assert_eq!(ByteSize::parse("50mb").unwrap().0, 50 * 1024 * 1024);
    }

    #[test]
    fn test_bytesize_parse_errors() {
        assert!(ByteSize::parse("").is_err());
        assert!(ByteSize::parse("MB").is_err());
        assert!(ByteSize::parse("100XB").is_err());
    }

    #[test]
    fn test_default_config() {
        let config = default_config();
        assert_eq!(config.cache.path, PathBuf::from("/tmp/tfsconfig-cache"));
        assert!(!config.cache.no_cache);
        assert!(matches!(
            config.network.limits.max_concurrent_requests,
            Limit::Value(40)
        ));
        assert!(matches!(
            config.network.limits.max_total_bytes_per_second,
            Limit::Disabled
        ));
        assert!(matches!(
            config.s3.limits.max_concurrent_requests,
            Limit::Inherit
        ));
    }

    #[test]
    fn test_apply_override_cache() {
        let mut config = default_config();
        apply_override(&mut config, "cache.path", "/custom/path").unwrap();
        assert_eq!(config.cache.path, PathBuf::from("/custom/path"));

        apply_override(&mut config, "cache.no_cache", "true").unwrap();
        assert!(config.cache.no_cache);
    }

    #[test]
    fn test_apply_override_network() {
        let mut config = default_config();
        apply_override(&mut config, "network.max_concurrent_requests", "100").unwrap();
        assert!(matches!(
            config.network.limits.max_concurrent_requests,
            Limit::Value(100)
        ));

        apply_override(&mut config, "network.max_read_bytes_per_second", "none").unwrap();
        assert!(matches!(
            config.network.limits.max_read_bytes_per_second,
            Limit::Disabled
        ));
    }

    #[test]
    fn test_apply_override_repository() {
        let mut config = default_config();
        apply_override(&mut config, "repository.myrepo.url", "s3://bucket/prefix").unwrap();
        apply_override(&mut config, "repository.myrepo.region", "us-west-2").unwrap();
        apply_override(
            &mut config,
            "repository.myrepo.max_concurrent_requests",
            "20",
        )
        .unwrap();

        let repo = config.repositories.get("myrepo").unwrap();
        assert_eq!(repo.url, "s3://bucket/prefix");
        assert_eq!(repo.settings.region, Some("us-west-2".to_string()));
        assert!(matches!(
            repo.limits.max_concurrent_requests,
            Limit::Value(20)
        ));
    }

    #[test]
    fn test_read_config_defaults_only() {
        let source = ConfigSource::default();
        // This will use defaults since no file exists
        let result = read_config(&source).unwrap();
        assert_eq!(
            result.config.cache.path,
            PathBuf::from("/tmp/tfsconfig-cache")
        );
    }

    #[test]
    fn test_parse_ini_config() {
        let mut ini = Ini::new();
        ini.read(
            r#"
[cache]
path = /custom/cache
no-cache = true

[network]
max_concurrent_requests = 50
max_requests_per_second = 200

[repository.main]
url = s3://my-bucket/repo
region = us-east-1
max_concurrent_requests = 10
"#
            .to_string(),
        )
        .unwrap();

        let mut config = default_config();
        apply_ini_to_config(&mut config, &ini).unwrap();

        assert_eq!(config.cache.path, PathBuf::from("/custom/cache"));
        assert!(config.cache.no_cache);
        assert!(matches!(
            config.network.limits.max_concurrent_requests,
            Limit::Value(50)
        ));
        assert!(matches!(
            config.network.limits.max_requests_per_second,
            Limit::Value(200)
        ));

        let repo = config.repositories.get("main").unwrap();
        assert_eq!(repo.url, "s3://my-bucket/repo");
        assert_eq!(repo.settings.region, Some("us-east-1".to_string()));
        assert!(matches!(
            repo.limits.max_concurrent_requests,
            Limit::Value(10)
        ));
    }
}
