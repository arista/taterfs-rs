//! Repository creation utilities.
//!
//! This module provides utilities for parsing repository specifications and
//! creating the components needed to construct a repository.

use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;

use crate::app::CapacityManagers;
use crate::backend::{
    FsBackend, FsLikeRepoBackendAdapter, HttpBackend, RepoBackend, S3Backend, S3BackendConfig,
};
use crate::caches::{NoopCache, RepoCache, RepoCaches};
use crate::config::ConfigHelper;

use super::Repo;

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during repository creation.
#[derive(Debug, Error)]
pub enum CreateRepoError {
    /// The repository specification is invalid.
    #[error("invalid repo spec: {0}")]
    InvalidRepoSpec(String),

    /// The URL scheme is not supported.
    #[error("unsupported URL scheme: {0}")]
    UnsupportedScheme(String),

    /// A named repository was not found in the configuration.
    #[error("repository not found: {0}")]
    RepositoryNotFound(String),

    /// Backend creation failed.
    #[error("failed to create backend: {0}")]
    BackendError(String),

    /// Cache lookup failed.
    #[error("failed to get cache: {0}")]
    CacheError(String),
}

/// Result type for repository creation.
pub type Result<T> = std::result::Result<T, CreateRepoError>;

// =============================================================================
// Parsed Repository Specification
// =============================================================================

/// The type of backend indicated by a repository specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendType {
    /// S3-compatible storage (s3:// URL).
    S3,
    /// Local filesystem (file:// URL).
    FileSystem,
    /// HTTP backend (http:// or https:// URL).
    Http,
}

/// A parsed repository specification.
#[derive(Debug, Clone)]
pub struct ParsedRepoSpec {
    /// The type of backend.
    pub backend_type: BackendType,

    /// For S3: the bucket name. For filesystem: the base path. For HTTP: the base URL.
    pub location: String,

    /// For S3: optional prefix within the bucket.
    pub prefix: Option<String>,

    /// Optional endpoint URL (primarily for S3).
    pub endpoint_url: Option<String>,

    /// Optional region (primarily for S3).
    pub region: Option<String>,
}

impl ParsedRepoSpec {
    /// Parse a repository specification string.
    ///
    /// Accepts:
    /// - `s3://bucket/prefix?endpoint_url=...&region=...`
    /// - `file:///path/to/repo`
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

        // Treat as a named repository
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
            return Err(CreateRepoError::InvalidRepoSpec(
                "S3 URL must include bucket name".to_string(),
            ));
        }

        // Parse query parameters
        let params = parse_query_string(query_part.unwrap_or(""));

        Ok(Self {
            backend_type: BackendType::S3,
            location: bucket,
            prefix,
            endpoint_url: params.get("endpoint_url").cloned(),
            region: params.get("region").cloned(),
        })
    }

    fn parse_file_url(url: &str) -> Result<Self> {
        // Format: file:///path/to/repo or file://path/to/repo
        let path = url.strip_prefix("file://").unwrap().to_string();

        if path.is_empty() {
            return Err(CreateRepoError::InvalidRepoSpec(
                "file:// URL must include a path".to_string(),
            ));
        }

        Ok(Self {
            backend_type: BackendType::FileSystem,
            location: path,
            prefix: None,
            endpoint_url: None,
            region: None,
        })
    }

    fn parse_http_url(url: &str) -> Result<Self> {
        // Just use the full URL as the location
        Ok(Self {
            backend_type: BackendType::Http,
            location: url.to_string(),
            prefix: None,
            endpoint_url: None,
            region: None,
        })
    }

    fn parse_named(name: &str, config: Option<&ConfigHelper>) -> Result<Self> {
        let config = config.ok_or_else(|| {
            CreateRepoError::InvalidRepoSpec(format!(
                "'{}' looks like a repository name but no config provided",
                name
            ))
        })?;

        let repo_config = config
            .get_repository(name)
            .ok_or_else(|| CreateRepoError::RepositoryNotFound(name.to_string()))?;

        // Parse the URL from the config
        let mut parsed = Self::parse(&repo_config.url, None)?;

        // Apply settings from config (they override URL query params)
        let settings = config.resolve_repository_config_settings(repo_config);
        if parsed.endpoint_url.is_none() {
            parsed.endpoint_url = settings.endpoint_url;
        }
        if parsed.region.is_none() {
            parsed.region = settings.region;
        }

        Ok(parsed)
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
// CreateRepoContext
// =============================================================================

/// Context for creating repositories.
///
/// Provides access to configuration, caches, and shared capacity managers.
pub struct CreateRepoContext {
    config: Arc<ConfigHelper>,
    repository_caches: Arc<dyn RepoCaches>,
    network_managers: CapacityManagers,
    s3_managers: CapacityManagers,
    allow_uninitialized: bool,
}

impl CreateRepoContext {
    /// Create a new context.
    ///
    /// The capacity managers should be created by the App and passed here.
    /// These are the global network and S3 capacity managers that will be
    /// shared across all repositories (cloned for each repo).
    pub fn new<R: RepoCaches + 'static>(
        config: ConfigHelper,
        repository_caches: R,
        network_managers: CapacityManagers,
        s3_managers: CapacityManagers,
    ) -> Self {
        Self {
            config: Arc::new(config),
            repository_caches: Arc::new(repository_caches),
            network_managers,
            s3_managers,
            allow_uninitialized: false,
        }
    }

    /// Set whether to allow creating repositories that are not yet initialized.
    ///
    /// When `true`, `create_repo` will skip the repository info check and use
    /// a no-op cache. This is useful when you need to create a `Repo` in order
    /// to call `initialize()` on it.
    pub fn with_allow_uninitialized(mut self, allow: bool) -> Self {
        self.allow_uninitialized = allow;
        self
    }

    /// Get the configuration helper.
    pub fn config(&self) -> &ConfigHelper {
        &self.config
    }

    /// Get the repository caches.
    pub fn repository_caches(&self) -> &dyn RepoCaches {
        &*self.repository_caches
    }

    /// Get the network-level capacity managers.
    pub fn network_capacity_managers(&self) -> &CapacityManagers {
        &self.network_managers
    }

    /// Get the S3-level capacity managers.
    pub fn s3_capacity_managers(&self) -> &CapacityManagers {
        &self.s3_managers
    }

    /// Create capacity managers for a specific repository.
    ///
    /// Resolves the repository's limits (inheriting from s3 -> network as needed)
    /// and creates new managers for any repo-specific overrides.
    pub fn create_repository_managers(&self, name: &str) -> Option<CapacityManagers> {
        let limits = self.config.resolve_repository_limits(name)?;
        Some(CapacityManagers::from_resolved_limits(&limits))
    }

    /// Create capacity managers from a parsed repo spec.
    ///
    /// If the spec came from a named repository, uses the config limits.
    /// Otherwise, uses S3 or network defaults based on backend type.
    ///
    /// For file repositories, no capacity managers are used since local disk
    /// access doesn't benefit from throughput or request rate limiting.
    pub fn create_managers_for_spec(&self, spec: &ParsedRepoSpec) -> CapacityManagers {
        match spec.backend_type {
            BackendType::S3 => self.s3_managers.clone(),
            BackendType::Http => self.network_managers.clone(),
            // File repositories don't use flow control - local disk access
            // doesn't benefit from throughput or request rate limiting
            BackendType::FileSystem => CapacityManagers::default(),
        }
    }

    /// Parse a repository specification.
    pub fn parse_repo_spec(&self, spec: &str) -> Result<ParsedRepoSpec> {
        ParsedRepoSpec::parse(spec, Some(&self.config))
    }

    /// Check if caching is disabled globally.
    pub fn is_cache_disabled(&self) -> bool {
        self.config.config().cache.no_cache
    }

    /// Check if caching is disabled for a specific repository.
    ///
    /// Returns true if either the global `[cache] no_cache` setting is true,
    /// or the repository-specific `[repository.{name}] no_cache` setting is true.
    pub fn is_cache_disabled_for_repo(&self, repo_name: &str) -> bool {
        if self.config.config().cache.no_cache {
            return true;
        }
        self.config
            .get_repository(repo_name)
            .map(|r| r.no_cache)
            .unwrap_or(false)
    }

    /// Get a cache for the repository with the given UUID.
    ///
    /// If caching is disabled in the configuration, returns a [`NoopCache`].
    /// Otherwise, delegates to the configured [`RepoCaches`] provider.
    pub async fn get_cache_for_uuid(&self, uuid: &str) -> Result<Arc<dyn RepoCache>> {
        if self.config.config().cache.no_cache {
            Ok(Arc::new(NoopCache))
        } else {
            self.repository_caches
                .get_cache(uuid)
                .await
                .map_err(CreateRepoError::CacheError)
        }
    }

    /// Get a cache for a named repository by its UUID.
    ///
    /// Checks both global and repository-specific `no_cache` settings.
    /// If either is true, returns a [`NoopCache`].
    /// Otherwise, delegates to the configured [`RepoCaches`] provider.
    pub async fn get_cache_for_repo(
        &self,
        repo_name: &str,
        uuid: &str,
    ) -> Result<Arc<dyn RepoCache>> {
        if self.is_cache_disabled_for_repo(repo_name) {
            Ok(Arc::new(NoopCache))
        } else {
            self.repository_caches
                .get_cache(uuid)
                .await
                .map_err(CreateRepoError::CacheError)
        }
    }

    /// Create a repository from a specification string.
    ///
    /// Parses the specification, creates the appropriate backend, retrieves
    /// the repository's cache based on its UUID, and constructs a [`Repo`].
    ///
    /// The repository must already be initialized (have repository info set),
    /// unless `allow_uninitialized` is set to `true`.
    pub async fn create_repo(&self, spec: &str) -> Result<Repo> {
        let parsed = self.parse_repo_spec(spec)?;
        self.create_repo_from_spec(&parsed).await
    }

    /// Create a repository from a parsed specification.
    pub async fn create_repo_from_spec(&self, spec: &ParsedRepoSpec) -> Result<Repo> {
        // Create the backend
        let backend: Arc<dyn RepoBackend> = match spec.backend_type {
            BackendType::S3 => {
                let mut s3_config = S3BackendConfig::new(&spec.location);
                if let Some(ref prefix) = spec.prefix {
                    s3_config = s3_config.with_prefix(prefix);
                }
                if let Some(ref endpoint_url) = spec.endpoint_url {
                    s3_config = s3_config.with_endpoint_url(endpoint_url);
                }
                if let Some(ref region) = spec.region {
                    s3_config = s3_config.with_region(region);
                }
                let s3_backend = S3Backend::new(s3_config).await;
                Arc::new(FsLikeRepoBackendAdapter::new(Arc::new(s3_backend)))
            }

            BackendType::FileSystem => {
                let fs_backend = FsBackend::new(&spec.location);
                Arc::new(FsLikeRepoBackendAdapter::new(Arc::new(fs_backend)))
            }

            BackendType::Http => Arc::new(HttpBackend::new(&spec.location)),
        };

        // Get cache - either from repository info or use NoopCache for uninitialized repos
        let cache: Arc<dyn RepoCache> = if self.allow_uninitialized {
            // Skip repository info check and use no-op cache
            Arc::new(NoopCache)
        } else {
            // Get repository info to find the UUID for caching
            let repo_info = backend
                .get_repository_info()
                .await
                .map_err(|e| CreateRepoError::BackendError(e.to_string()))?;

            // Get the cache for this repository
            self.get_cache_for_uuid(&repo_info.uuid).await?
        };

        // Create flow control configuration
        let managers = self.create_managers_for_spec(spec);
        let flow_control = managers.to_flow_control();

        // Create and return the Repo
        Ok(Repo::from_dyn(backend, cache, flow_control))
    }
}

// =============================================================================
// Convenience Function
// =============================================================================

/// Create a repository from a specification string.
///
/// This is a convenience function that delegates to [`CreateRepoContext::create_repo`].
pub async fn create_repo(spec: &str, ctx: &CreateRepoContext) -> Result<Repo> {
    ctx.create_repo(spec).await
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_url_simple() {
        let spec = ParsedRepoSpec::parse("s3://mybucket", None).unwrap();
        assert_eq!(spec.backend_type, BackendType::S3);
        assert_eq!(spec.location, "mybucket");
        assert_eq!(spec.prefix, None);
        assert_eq!(spec.endpoint_url, None);
        assert_eq!(spec.region, None);
    }

    #[test]
    fn test_parse_s3_url_with_prefix() {
        let spec = ParsedRepoSpec::parse("s3://mybucket/path/to/repo", None).unwrap();
        assert_eq!(spec.backend_type, BackendType::S3);
        assert_eq!(spec.location, "mybucket");
        assert_eq!(spec.prefix, Some("path/to/repo".to_string()));
    }

    #[test]
    fn test_parse_s3_url_with_params() {
        let spec = ParsedRepoSpec::parse(
            "s3://mybucket/prefix?endpoint_url=http://localhost:9000&region=us-west-2",
            None,
        )
        .unwrap();
        assert_eq!(spec.backend_type, BackendType::S3);
        assert_eq!(spec.location, "mybucket");
        assert_eq!(spec.prefix, Some("prefix".to_string()));
        assert_eq!(spec.endpoint_url, Some("http://localhost:9000".to_string()));
        assert_eq!(spec.region, Some("us-west-2".to_string()));
    }

    #[test]
    fn test_parse_file_url() {
        let spec = ParsedRepoSpec::parse("file:///home/user/repo", None).unwrap();
        assert_eq!(spec.backend_type, BackendType::FileSystem);
        assert_eq!(spec.location, "/home/user/repo");
    }

    #[test]
    fn test_parse_http_url() {
        let spec = ParsedRepoSpec::parse("http://example.com/repo", None).unwrap();
        assert_eq!(spec.backend_type, BackendType::Http);
        assert_eq!(spec.location, "http://example.com/repo");

        let spec = ParsedRepoSpec::parse("https://example.com/repo", None).unwrap();
        assert_eq!(spec.backend_type, BackendType::Http);
        assert_eq!(spec.location, "https://example.com/repo");
    }

    #[test]
    fn test_parse_named_without_config() {
        let result = ParsedRepoSpec::parse("myrepo", None);
        assert!(matches!(result, Err(CreateRepoError::InvalidRepoSpec(_))));
    }
}
