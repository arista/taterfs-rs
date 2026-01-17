//! Top-level application component.
//!
//! The [`App`] owns all global services and is the root for the application's functionality.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use thiserror::Error;

use crate::app::CapacityManagers;
use crate::caches::{
    CacheDb, CachingConfig, CachingKeyValueDb, DbFileStoreCaches, DbRepoCaches, FileStoreCaches,
    KeyValueDb, LmdbKeyValueDb, NoopCaches, NoopFileStoreCaches, RepoCaches,
};
use crate::config::{read_config, ConfigHelper, ConfigSource};
use crate::file_store::{
    create_file_store, CreateFileStoreContext, CreateFileStoreError, FileStore,
};
use crate::repo::{create_repo, CreateRepoContext, CreateRepoError, Repo};
use crate::util::ManagedBuffers;

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during App operations.
#[derive(Debug, Error)]
pub enum AppError {
    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// Cache initialization error.
    #[error("failed to initialize cache: {0}")]
    CacheInit(String),

    /// Repository creation error.
    #[error("failed to create repository: {0}")]
    CreateRepo(#[from] CreateRepoError),

    /// File store creation error.
    #[error("failed to create file store: {0}")]
    CreateFileStore(#[from] CreateFileStoreError),
}

/// Result type for App operations.
pub type Result<T> = std::result::Result<T, AppError>;

// =============================================================================
// Context Types
// =============================================================================

/// Context for creating an App.
#[derive(Default)]
pub struct AppContext {
    /// Source for configuration files.
    pub config_source: ConfigSource,
}

/// Context for creating a repository via the App.
pub struct AppCreateRepoContext {
    /// Repository specification (URL or named repository).
    pub spec: String,
    /// Whether to allow creating a Repo for an uninitialized repository.
    pub allow_uninitialized: bool,
}

impl AppCreateRepoContext {
    /// Create a new context for the given spec.
    pub fn new(spec: impl Into<String>) -> Self {
        Self {
            spec: spec.into(),
            allow_uninitialized: false,
        }
    }

    /// Set whether to allow uninitialized repositories.
    pub fn with_allow_uninitialized(mut self, allow: bool) -> Self {
        self.allow_uninitialized = allow;
        self
    }
}

/// Context for creating a file store via the App.
pub struct AppCreateFileStoreContext {
    /// File store specification (URL or named file store).
    pub spec: String,
}

impl AppCreateFileStoreContext {
    /// Create a new context for the given spec.
    pub fn new(spec: impl Into<String>) -> Self {
        Self { spec: spec.into() }
    }
}

// =============================================================================
// App
// =============================================================================

/// The top-level application component.
///
/// Owns all global services including configuration, caches, and capacity managers.
/// Provides methods for creating repositories and file stores.
///
/// **Important**: Call [`App::shutdown`] before dropping to ensure pending cache
/// writes are flushed to disk.
pub struct App {
    config: ConfigHelper,
    managed_buffers: ManagedBuffers,
    network_managers: CapacityManagers,
    s3_managers: CapacityManagers,
    repo_caches: Arc<dyn RepoCaches>,
    file_store_caches: Arc<dyn FileStoreCaches>,
    /// Reference to the caching layer for shutdown flushing.
    /// None if caching is disabled.
    caching_db: Option<Arc<CachingKeyValueDb>>,
}

impl App {
    /// Run a function with a new App, ensuring proper shutdown.
    ///
    /// This is the preferred way to use App. It creates the App, runs the
    /// provided async function, then shuts down the App (flushing caches)
    /// before returning. This ensures cleanup happens even if the function
    /// returns an error.
    ///
    /// The error type `E` must implement `From<AppError>` so that App creation
    /// and shutdown errors can be propagated.
    ///
    /// # Example
    ///
    /// ```ignore
    /// App::with_app(AppContext::default(), |app| Box::pin(async move {
    ///     let repo = app.create_repo(ctx).await?;
    ///     // ... use repo ...
    ///     Ok(())
    /// })).await?;
    /// ```
    pub async fn with_app<F, T, E>(ctx: AppContext, f: F) -> std::result::Result<T, E>
    where
        F: for<'a> FnOnce(&'a App) -> Pin<Box<dyn Future<Output = std::result::Result<T, E>> + Send + 'a>>,
        E: From<AppError>,
    {
        let app = Self::new(ctx)?;
        let result = f(&app).await;
        // Always flush cache, even if the function failed
        app.shutdown().await?;
        result
    }

    /// Create a new App with the given context.
    ///
    /// Prefer using [`App::with_app`] instead, which ensures proper shutdown.
    fn new(ctx: AppContext) -> Result<Self> {
        let config_result =
            read_config(&ctx.config_source).map_err(|e| AppError::Config(e.to_string()))?;

        let config = ConfigHelper::new(config_result.config);
        let managed_buffers = ManagedBuffers::new();

        // Create global capacity managers
        let network_managers =
            CapacityManagers::from_resolved_limits(&config.resolve_network_limits());
        let s3_managers = CapacityManagers::from_resolved_limits(&config.resolve_s3_limits());

        // Create cache infrastructure
        let cache_config = config.config().cache.clone();
        let (repo_caches, file_store_caches, caching_db) = if cache_config.no_cache {
            // Caching disabled - use no-op implementations
            let repo_caches: Arc<dyn RepoCaches> = Arc::new(NoopCaches);
            let file_store_caches: Arc<dyn FileStoreCaches> = Arc::new(NoopFileStoreCaches);
            (repo_caches, file_store_caches, None)
        } else {
            // Create LMDB-backed cache
            let lmdb = LmdbKeyValueDb::new(&cache_config.path)
                .map_err(|e| AppError::CacheInit(e.to_string()))?;

            let caching_config = CachingConfig {
                flush_period_ms: cache_config.pending_writes_flush_period_ms,
                max_pending_count: cache_config.pending_writes_max_count,
                max_pending_size: cache_config.pending_writes_max_size.0 as usize,
                max_cache_size: cache_config.max_memory_size.0 as usize,
            };

            let caching_db = Arc::new(CachingKeyValueDb::new(Arc::new(lmdb), caching_config));
            let cache_db = Arc::new(CacheDb::new(caching_db.clone() as Arc<dyn KeyValueDb>));

            let repo_caches: Arc<dyn RepoCaches> = Arc::new(DbRepoCaches::new(cache_db.clone()));
            let file_store_caches: Arc<dyn FileStoreCaches> =
                Arc::new(DbFileStoreCaches::new(cache_db));
            (repo_caches, file_store_caches, Some(caching_db))
        };

        Ok(Self {
            config,
            managed_buffers,
            network_managers,
            s3_managers,
            repo_caches,
            file_store_caches,
            caching_db,
        })
    }

    /// Get the configuration helper.
    pub fn config(&self) -> &ConfigHelper {
        &self.config
    }

    /// Get the file store caches.
    pub fn file_store_caches(&self) -> &Arc<dyn FileStoreCaches> {
        &self.file_store_caches
    }

    /// Get the underlying key-value database.
    ///
    /// Returns `None` if caching is disabled (--no-cache).
    pub fn key_value_db(&self) -> Option<Arc<dyn KeyValueDb>> {
        self.caching_db
            .as_ref()
            .map(|db| db.clone() as Arc<dyn KeyValueDb>)
    }

    /// Flush pending cache writes.
    ///
    /// Called automatically by [`App::with_app`].
    async fn shutdown(&self) -> Result<()> {
        if let Some(caching_db) = &self.caching_db {
            caching_db
                .flush()
                .await
                .map_err(|e| AppError::CacheInit(format!("failed to flush cache: {}", e)))?;
        }
        Ok(())
    }

    /// Create a repository from a specification.
    pub async fn create_repo(&self, ctx: AppCreateRepoContext) -> Result<Arc<Repo>> {
        let repo_ctx = CreateRepoContext::new(
            self.config.clone(),
            self.repo_caches.clone(),
            self.network_managers.clone(),
            self.s3_managers.clone(),
        )
        .with_allow_uninitialized(ctx.allow_uninitialized);

        let repo = create_repo(&ctx.spec, &repo_ctx).await?;
        Ok(Arc::new(repo))
    }

    /// Create a file store from a specification.
    pub async fn create_file_store(
        &self,
        ctx: AppCreateFileStoreContext,
    ) -> Result<Arc<dyn FileStore>> {
        let file_store_ctx = CreateFileStoreContext::new(
            self.config.clone(),
            self.managed_buffers.clone(),
            self.file_store_caches.clone(),
        );

        let store = create_file_store(&ctx.spec, &file_store_ctx).await?;
        Ok(store)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_app_creation() {
        let ctx = AppContext::default();
        let app = App::new(ctx).unwrap();
        assert!(app.config().config().cache.no_cache == false);
    }

    #[test]
    fn test_app_create_repo_context() {
        let ctx = AppCreateRepoContext::new("file:///tmp/repo");
        assert_eq!(ctx.spec, "file:///tmp/repo");
        assert!(!ctx.allow_uninitialized);

        let ctx = ctx.with_allow_uninitialized(true);
        assert!(ctx.allow_uninitialized);
    }

    #[test]
    fn test_app_create_file_store_context() {
        let ctx = AppCreateFileStoreContext::new("file:///tmp/files");
        assert_eq!(ctx.spec, "file:///tmp/files");
    }
}
