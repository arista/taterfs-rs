#![allow(dead_code, unused_imports)]

mod fs_backend;
mod fs_like_repo_backend;
mod fs_like_repo_backend_adapter;
mod http_backend;
mod memory_backend;
mod repo_backend;
mod s3_backend;

pub use fs_backend::FsBackend;
pub use fs_like_repo_backend::FsLikeRepoBackend;
pub use fs_like_repo_backend_adapter::FsLikeRepoBackendAdapter;
pub use http_backend::HttpBackend;
pub use memory_backend::MemoryBackend;
pub use repo_backend::{BackendError, ObjectId, RepoBackend, RepositoryInfo, Result, SwapResult};
pub use s3_backend::{S3Backend, S3BackendConfig};
