//! Repository interface with caching, deduplication, and flow control.
//!
//! This module provides the [`Repo`] struct which wraps a backend and cache
//! to provide a higher-level interface for repository operations.

mod create_repo;
#[allow(clippy::module_inception)]
mod repo;

pub use create_repo::{BackendType, CreateRepoContext, CreateRepoError, ParsedRepoSpec};
pub use repo::{FlowControl, Repo, RepoError, RepoInitialize, Result};
