//! File store creation utilities.
//!
//! This module provides utilities for parsing file store specifications and
//! creating the components needed to construct a file store.

use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;

use crate::app::CapacityManagers;
use crate::caches::{FileStoreCache, FileStoreCaches, LocalChunksCache};
use crate::config::ConfigHelper;
use crate::file_store::{FileStore, FsFileStore, S3FileSource, S3FileSourceConfig, StoreSyncState};
use crate::util::ManagedBuffers;

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during file store creation.
#[derive(Debug, Error)]
pub enum CreateFileStoreError {
    /// The file store specification is invalid.
    #[error("invalid file store spec: {0}")]
    InvalidSpec(String),

    /// The URL scheme is not supported.
    #[error("unsupported URL scheme: {0}")]
    UnsupportedScheme(String),

    /// A named file store was not found in the configuration.
    #[error("file store not found: {0}")]
    FileStoreNotFound(String),

    /// File store creation failed.
    #[error("failed to create file store: {0}")]
    CreationError(String),
}

/// Result type for file store creation.
pub type Result<T> = std::result::Result<T, CreateFileStoreError>;

// =============================================================================
// Parsed File Store Specification
// =============================================================================

/// The type of file store indicated by a specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileStoreType {
    /// S3-compatible storage (s3:// URL).
    S3,
    /// Local filesystem (file:// URL).
    FileSystem,
    /// HTTP source (http:// or https:// URL).
    Http,
}

/// A parsed file store specification.
#[derive(Debug, Clone)]
pub struct ParsedFileStoreSpec {
    /// The type of file store.
    pub store_type: FileStoreType,

    /// For S3: the bucket name. For filesystem: the base path. For HTTP: the base URL.
    pub location: String,

    /// For S3: optional prefix within the bucket.
    pub prefix: Option<String>,

    /// Optional endpoint URL (primarily for S3).
    pub endpoint_url: Option<String>,

    /// Optional region (primarily for S3).
    pub region: Option<String>,
}

impl ParsedFileStoreSpec {
    /// Parse a file store specification string.
    ///
    /// Accepts:
    /// - `s3://bucket/prefix?endpoint_url=...&region=...`
    /// - `file:///path/to/directory`
    /// - `http://host/path` or `https://host/path`
    /// - A bare name (looked up in config)
    pub fn parse(spec: &str, config: Option<&ConfigHelper>) -> Result<Self> {
        // Check for URL schemes
        if spec.starts_with("s3://") {
            return Self::parse_s3_url(spec);
        }
        if spec.starts_with("file://") {
            return Self::parse_file_url(spec);
        }
        if spec.starts_with("http://") || spec.starts_with("https://") {
            return Self::parse_http_url(spec);
        }

        // Treat as a named file store
        Self::parse_named(spec, config)
    }

    fn parse_s3_url(url: &str) -> Result<Self> {
        // Format: s3://bucket/prefix?endpoint_url=...&region=...
        let without_scheme = url.strip_prefix("s3://").unwrap();

        // Split on '?' to separate path from query string
        let (path_part, query_part) = match without_scheme.find('?') {
            Some(idx) => (&without_scheme[..idx], Some(&without_scheme[idx + 1..])),
            None => (without_scheme, None),
        };

        // Split path into bucket and prefix
        let (bucket, prefix) = match path_part.find('/') {
            Some(idx) => {
                let bucket = &path_part[..idx];
                let prefix = &path_part[idx + 1..];
                (
                    bucket.to_string(),
                    if prefix.is_empty() {
                        None
                    } else {
                        Some(prefix.to_string())
                    },
                )
            }
            None => (path_part.to_string(), None),
        };

        if bucket.is_empty() {
            return Err(CreateFileStoreError::InvalidSpec(
                "S3 URL must include bucket name".to_string(),
            ));
        }

        // Parse query parameters
        let params = parse_query_string(query_part.unwrap_or(""));

        Ok(Self {
            store_type: FileStoreType::S3,
            location: bucket,
            prefix,
            endpoint_url: params.get("endpoint_url").cloned(),
            region: params.get("region").cloned(),
        })
    }

    fn parse_file_url(url: &str) -> Result<Self> {
        // Format: file:///path/to/directory or file://path/to/directory
        let path = url.strip_prefix("file://").unwrap().to_string();

        if path.is_empty() {
            return Err(CreateFileStoreError::InvalidSpec(
                "file:// URL must include a path".to_string(),
            ));
        }

        Ok(Self {
            store_type: FileStoreType::FileSystem,
            location: path,
            prefix: None,
            endpoint_url: None,
            region: None,
        })
    }

    fn parse_http_url(url: &str) -> Result<Self> {
        // Just use the full URL as the location
        Ok(Self {
            store_type: FileStoreType::Http,
            location: url.to_string(),
            prefix: None,
            endpoint_url: None,
            region: None,
        })
    }

    fn parse_named(name: &str, config: Option<&ConfigHelper>) -> Result<Self> {
        let config = config.ok_or_else(|| {
            CreateFileStoreError::InvalidSpec(format!(
                "'{}' looks like a file store name but no config provided",
                name
            ))
        })?;

        let store_config = config
            .get_filestore(name)
            .ok_or_else(|| CreateFileStoreError::FileStoreNotFound(name.to_string()))?;

        // Parse the URL from the config
        let mut parsed = Self::parse(&store_config.url, None)?;

        // Apply settings from config (they override URL query params)
        let settings = config.resolve_filestore_config_settings(store_config);
        if parsed.endpoint_url.is_none() {
            parsed.endpoint_url = settings.endpoint_url;
        }
        if parsed.region.is_none() {
            parsed.region = settings.region;
        }

        Ok(parsed)
    }

    /// Generate the cache URL for this file store specification.
    ///
    /// This URL is used as a key for looking up the file store's cache.
    pub fn cache_url(&self) -> String {
        match self.store_type {
            FileStoreType::S3 => match &self.prefix {
                Some(prefix) => format!("s3://{}/{}", self.location, prefix),
                None => format!("s3://{}", self.location),
            },
            FileStoreType::FileSystem => format!("file://{}", self.location),
            FileStoreType::Http => self.location.clone(),
        }
    }
}

/// Parse a query string into key-value pairs.
fn parse_query_string(query: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();

    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        if let Some(idx) = pair.find('=') {
            let key = &pair[..idx];
            let value = &pair[idx + 1..];
            // Basic URL decoding (just handle %20 for spaces)
            let value = value.replace("%20", " ");
            params.insert(key.to_string(), value);
        }
    }

    params
}

// =============================================================================
// CreateFileStoreContext
// =============================================================================

/// Context for creating file stores.
///
/// Provides access to configuration, shared capacity managers, and caches.
pub struct CreateFileStoreContext {
    config: Arc<ConfigHelper>,
    network_managers: CapacityManagers,
    s3_managers: CapacityManagers,
    managed_buffers: ManagedBuffers,
    file_store_caches: Arc<dyn FileStoreCaches>,
    local_chunks_cache: Arc<dyn LocalChunksCache>,
}

impl CreateFileStoreContext {
    /// Create a new context.
    pub fn new(
        config: ConfigHelper,
        managed_buffers: ManagedBuffers,
        file_store_caches: Arc<dyn FileStoreCaches>,
        local_chunks_cache: Arc<dyn LocalChunksCache>,
    ) -> Self {
        let config = Arc::new(config);
        let network_managers =
            CapacityManagers::from_resolved_limits(&config.resolve_network_limits());
        let s3_managers = CapacityManagers::from_resolved_limits(&config.resolve_s3_limits());

        Self {
            config,
            network_managers,
            s3_managers,
            managed_buffers,
            file_store_caches,
            local_chunks_cache,
        }
    }

    /// Get the configuration helper.
    pub fn config(&self) -> &ConfigHelper {
        &self.config
    }

    /// Get the network-level capacity managers.
    pub fn network_capacity_managers(&self) -> &CapacityManagers {
        &self.network_managers
    }

    /// Get the S3-level capacity managers.
    pub fn s3_capacity_managers(&self) -> &CapacityManagers {
        &self.s3_managers
    }

    /// Get the managed buffers for memory allocation.
    pub fn managed_buffers(&self) -> &ManagedBuffers {
        &self.managed_buffers
    }

    /// Get the local chunks cache.
    pub fn local_chunks_cache(&self) -> Arc<dyn LocalChunksCache> {
        self.local_chunks_cache.clone()
    }

    /// Create capacity managers for a specific file store.
    ///
    /// Resolves the file store's limits (inheriting from s3 -> network as needed)
    /// and creates new managers for any store-specific overrides.
    pub fn create_filestore_managers(&self, name: &str) -> Option<CapacityManagers> {
        let limits = self.config.resolve_filestore_limits(name)?;
        Some(CapacityManagers::from_resolved_limits(&limits))
    }

    /// Create capacity managers from a parsed file store spec.
    ///
    /// For S3 stores, uses S3 defaults.
    /// For HTTP stores, uses network defaults.
    /// For file system stores, no capacity managers are used.
    pub fn create_managers_for_spec(&self, spec: &ParsedFileStoreSpec) -> CapacityManagers {
        match spec.store_type {
            FileStoreType::S3 => self.s3_managers.clone(),
            FileStoreType::Http => self.network_managers.clone(),
            // File system stores don't use flow control - local disk access
            // doesn't benefit from throughput or request rate limiting
            FileStoreType::FileSystem => CapacityManagers::default(),
        }
    }

    /// Parse a file store specification.
    pub fn parse_spec(&self, spec: &str) -> Result<ParsedFileStoreSpec> {
        ParsedFileStoreSpec::parse(spec, Some(&self.config))
    }

    /// Create a file store from a specification string.
    ///
    /// Parses the specification, resolves configuration, and creates the
    /// appropriate file store implementation.
    pub async fn create_file_store(&self, spec: &str) -> Result<Arc<dyn FileStore>> {
        let parsed = self.parse_spec(spec)?;
        self.create_file_store_from_spec(&parsed).await
    }

    /// Create a file store from a parsed specification.
    pub async fn create_file_store_from_spec(
        &self,
        spec: &ParsedFileStoreSpec,
    ) -> Result<Arc<dyn FileStore>> {
        let managers = self.create_managers_for_spec(spec);
        let flow_control = managers.to_flow_control_with_buffers(self.managed_buffers.clone());

        // Get the cache for this file store
        let cache_url = spec.cache_url();
        let cache = self
            .file_store_caches
            .get_cache(&cache_url)
            .await
            .map_err(CreateFileStoreError::CreationError)?;

        match spec.store_type {
            FileStoreType::S3 => {
                let mut s3_config = S3FileSourceConfig::new(&spec.location);
                if let Some(ref prefix) = spec.prefix {
                    s3_config = s3_config.with_prefix(prefix);
                }
                if let Some(ref endpoint_url) = spec.endpoint_url {
                    s3_config = s3_config.with_endpoint_url(endpoint_url);
                }
                if let Some(ref region) = spec.region {
                    s3_config = s3_config.with_region(region);
                }

                let source =
                    S3FileSource::new(s3_config, self.managed_buffers.clone(), flow_control).await;

                Ok(Arc::new(S3FileStoreWrapper::new(source, cache)))
            }

            FileStoreType::FileSystem => {
                let store = FsFileStore::new(
                    &spec.location,
                    self.managed_buffers.clone(),
                    cache,
                    Some(self.local_chunks_cache.clone()),
                );
                Ok(Arc::new(store))
            }

            FileStoreType::Http => {
                // TODO: HttpFileSource is not yet implemented
                Err(CreateFileStoreError::UnsupportedScheme(
                    "http/https file stores are not yet implemented".to_string(),
                ))
            }
        }
    }
}

// =============================================================================
// S3FileStoreWrapper
// =============================================================================

/// Wrapper to make S3FileSource implement FileStore.
///
/// S3FileSource only implements FileSource, not FileDest.
struct S3FileStoreWrapper {
    source: S3FileSource,
    cache: Arc<dyn FileStoreCache>,
}

impl S3FileStoreWrapper {
    fn new(source: S3FileSource, cache: Arc<dyn FileStoreCache>) -> Self {
        Self { source, cache }
    }
}

impl FileStore for S3FileStoreWrapper {
    fn get_source(&self) -> Option<&dyn crate::file_store::FileSource> {
        Some(&self.source)
    }

    fn get_dest(&self) -> Option<&dyn crate::file_store::FileDest> {
        None // S3FileStore doesn't support FileDest yet
    }

    fn get_cache(&self) -> Arc<dyn FileStoreCache> {
        self.cache.clone()
    }

    fn get_sync_state_manager(&self) -> Option<&dyn StoreSyncState> {
        None // S3FileStore doesn't support sync state yet
    }
}

// =============================================================================
// Convenience Function
// =============================================================================

/// Create a file store from a specification string.
///
/// This is a convenience function that delegates to [`CreateFileStoreContext::create_file_store`].
pub async fn create_file_store(
    spec: &str,
    ctx: &CreateFileStoreContext,
) -> Result<Arc<dyn FileStore>> {
    ctx.create_file_store(spec).await
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_url_simple() {
        let spec = ParsedFileStoreSpec::parse("s3://mybucket", None).unwrap();
        assert_eq!(spec.store_type, FileStoreType::S3);
        assert_eq!(spec.location, "mybucket");
        assert_eq!(spec.prefix, None);
        assert_eq!(spec.endpoint_url, None);
        assert_eq!(spec.region, None);
    }

    #[test]
    fn test_parse_s3_url_with_prefix() {
        let spec = ParsedFileStoreSpec::parse("s3://mybucket/path/to/files", None).unwrap();
        assert_eq!(spec.store_type, FileStoreType::S3);
        assert_eq!(spec.location, "mybucket");
        assert_eq!(spec.prefix, Some("path/to/files".to_string()));
    }

    #[test]
    fn test_parse_s3_url_with_params() {
        let spec = ParsedFileStoreSpec::parse(
            "s3://mybucket/prefix?endpoint_url=http://localhost:9000&region=us-west-2",
            None,
        )
        .unwrap();
        assert_eq!(spec.store_type, FileStoreType::S3);
        assert_eq!(spec.location, "mybucket");
        assert_eq!(spec.prefix, Some("prefix".to_string()));
        assert_eq!(spec.endpoint_url, Some("http://localhost:9000".to_string()));
        assert_eq!(spec.region, Some("us-west-2".to_string()));
    }

    #[test]
    fn test_parse_file_url() {
        let spec = ParsedFileStoreSpec::parse("file:///home/user/files", None).unwrap();
        assert_eq!(spec.store_type, FileStoreType::FileSystem);
        assert_eq!(spec.location, "/home/user/files");
    }

    #[test]
    fn test_parse_http_url() {
        let spec = ParsedFileStoreSpec::parse("http://example.com/files", None).unwrap();
        assert_eq!(spec.store_type, FileStoreType::Http);
        assert_eq!(spec.location, "http://example.com/files");

        let spec = ParsedFileStoreSpec::parse("https://example.com/files", None).unwrap();
        assert_eq!(spec.store_type, FileStoreType::Http);
        assert_eq!(spec.location, "https://example.com/files");
    }

    #[test]
    fn test_parse_named_without_config() {
        let result = ParsedFileStoreSpec::parse("myfilestore", None);
        assert!(matches!(result, Err(CreateFileStoreError::InvalidSpec(_))));
    }

    #[test]
    fn test_parse_empty_bucket() {
        let result = ParsedFileStoreSpec::parse("s3://", None);
        assert!(matches!(result, Err(CreateFileStoreError::InvalidSpec(_))));
    }

    #[test]
    fn test_parse_empty_file_path() {
        let result = ParsedFileStoreSpec::parse("file://", None);
        assert!(matches!(result, Err(CreateFileStoreError::InvalidSpec(_))));
    }

    #[tokio::test]
    async fn test_create_fs_file_store() {
        use crate::caches::{NoopFileStoreCaches, NoopLocalChunksCache};
        use crate::config::{ConfigHelper, ConfigSource, read_config};

        let source = ConfigSource::default();
        let result = read_config(&source).unwrap();
        let config = ConfigHelper::new(result.config);
        let file_store_caches = Arc::new(NoopFileStoreCaches);
        let local_chunks_cache = Arc::new(NoopLocalChunksCache);
        let ctx = CreateFileStoreContext::new(
            config,
            ManagedBuffers::new(),
            file_store_caches,
            local_chunks_cache,
        );

        let store = ctx.create_file_store("file:///tmp").await.unwrap();
        assert!(store.get_source().is_some());
    }
}
