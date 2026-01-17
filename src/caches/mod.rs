//! Caches for repository and file store operations.
//!
//! This module provides caching interfaces and implementations for optimizing
//! repository operations by avoiding redundant lookups and computations.
//!
//! ## Architecture
//!
//! The cache system is layered:
//!
//! 1. **KeyValueDb** - Low-level key-value storage interface
//!    - [`LmdbKeyValueDb`] - LMDB-backed implementation
//!    - [`CachingKeyValueDb`] - Write-back caching wrapper
//!
//! 2. **CacheDb** - Higher-level cache operations
//!    - Repository ID and object caching
//!    - File store path and fingerprint caching
//!
//! 3. **Application-level caches**
//!    - [`RepoCache`] / [`RepoCaches`] - Repository object caching
//!    - [`FileStoreCache`] / [`FileStoreCaches`] - File fingerprint caching

mod cache_db;
mod caching_key_value_db;
mod file_store_cache;
mod key_value_db;
mod lmdb_key_value_db;
mod repo_cache;

// Key-value database layer
pub use key_value_db::{
    KeyValueDb, KeyValueDbError, KeyValueDbTransaction, KeyValueDbWrites, KeyValueEntries,
    KeyValueEntry, WriteOp,
};
pub use lmdb_key_value_db::LmdbKeyValueDb;
pub use caching_key_value_db::{CachingConfig, CachingKeyValueDb};

// Cache database layer
pub use cache_db::{CacheDb, DbId};

// Repository cache layer
pub use repo_cache::{
    CacheError, DbRepoCache, DbRepoCaches, NoopCache, NoopCaches, RepoCache, RepoCaches,
    Result as RepoCacheResult,
};

// File store cache layer
pub use file_store_cache::{
    DbFileStoreCache, DbFileStoreCaches, FileStoreCache, FileStoreCacheError, FileStoreCaches,
    FingerprintedFileInfo, NoopFileStoreCache, NoopFileStoreCaches,
    Result as FileStoreCacheResult,
};
