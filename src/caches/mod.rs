//! Caches for repository operations.
//!
//! This module provides caching interfaces and implementations for optimizing
//! repository operations by avoiding redundant lookups and computations.

mod repo_cache;

pub use repo_cache::{CacheError, RepoCache, RepoCaches, Result};
