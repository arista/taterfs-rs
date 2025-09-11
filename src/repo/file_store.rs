// Represents a simple mechanism for interacting with a filesystem-like storage mechanism.  RepoBackends may choose to build on this trait.

use async_trait::async_trait;
use anyhow::{Result};
use bytes::Bytes;

#[async_trait(?Send)]
pub trait FileStore {
    // Return true if a file exists at the given path
    async fn exists(&self, path: &str) -> Result<bool>;

    // Return the contents of the given file
    async fn read(&self, path: &str) -> Result<Bytes>;

    // Write the contents of the given file
    async fn write(&self, path: &str, buf: Bytes) -> Result<()>;

    // Assuming path is a directory name, return the name of the first file found under that directory (including files in subdirectories), if any
    async fn first_file(&self, path: &str) -> Result<Option<String>>;

    // If this FileStore does any in-memory buffering, flush all those buffers to permanent storage
    async fn flush(&self) -> Result<()>;
}
