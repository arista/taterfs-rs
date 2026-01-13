//! Repository interface with caching, deduplication, and flow control.
//!
//! This module provides the [`Repo`] struct which wraps a backend and cache
//! to provide a higher-level interface for repository operations.

#[allow(clippy::module_inception)]
mod repo;

pub use repo::{FlowControl, Repo, RepoError, Result};
