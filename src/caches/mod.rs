//! Caches for repository operations.
//!
//! This module provides caching interfaces and implementations for optimizing
//! repository operations by avoiding redundant lookups and computations.

mod repository_cache;

pub use repository_cache::{CacheError, RepositoryCache, Result};
