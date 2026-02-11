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
//! 2. **ObjectCacheDb** - Repository object caching (separate from KeyValueDb)
//!    - [`FsObjectCacheDb`] - Filesystem-backed implementation
//!    - [`CachingObjectCacheDb`] - In-memory LRU caching wrapper
//!
//! 3. **CacheDb** - Higher-level cache operations
//!    - Repository ID and existence tracking (uses KeyValueDb)
//!    - Repository object caching (uses ObjectCacheDb)
//!    - File store path and fingerprint caching (uses KeyValueDb)
//!    - Local chunks caching (uses KeyValueDb)
//!
//! 4. **Application-level caches**
//!    - [`RepoCache`] / [`RepoCaches`] - Repository object caching
//!    - [`FileStoreCache`] / [`FileStoreCaches`] - File fingerprint caching
//!    - [`LocalChunksCache`] - Local chunk location caching

mod cache_db;
mod caching_key_value_db;
mod file_store_cache;
mod key_value_db;
mod lmdb_key_value_db;
mod local_chunks_cache;
mod object_cache_db;
mod repo_cache;

// Key-value database layer
pub use caching_key_value_db::{CachingConfig, CachingKeyValueDb};
pub use key_value_db::{
    KeyEntries, KeyEntry, KeyValueDb, KeyValueDbError, KeyValueDbTransaction, KeyValueDbWrites,
    KeyValueEntries, KeyValueEntry, WriteOp,
};
pub use lmdb_key_value_db::LmdbKeyValueDb;

// Object cache database layer
pub use object_cache_db::{
    CachingObjectCacheDb, FsObjectCacheDb, NoopObjectCacheDb, ObjectCacheDb, ObjectCacheError,
};

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
    FingerprintedFileInfo, NoopFileStoreCache, NoopFileStoreCaches, Result as FileStoreCacheResult,
};

// Local chunks cache layer
pub use local_chunks_cache::{
    DbLocalChunksCache, LocalChunksCache, LocalChunksCacheError, NoopLocalChunksCache,
    Result as LocalChunksCacheResult,
};

// Re-export local chunk types from cache_db
pub use cache_db::{LocalChunk, PathEntry, PossibleLocalChunk};
