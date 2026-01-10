use std::future::Future;

use super::repo_backend::Result;

/// A backend interface for filesystem-like storage mechanisms.
///
/// This trait is implemented by backends that use filesystem-like storage
/// (e.g., local filesystem, S3). An adapter can bridge from the `RepoBackend`
/// interface to this interface, handling the mapping of object IDs to file paths.
///
/// All operations are asynchronous.
pub trait FsLikeRepoBackend: Send + Sync {
    /// Check if a file exists at the given path.
    fn file_exists(&self, path: &str) -> impl Future<Output = Result<bool>> + Send;

    /// Read the contents of a file at the given path.
    ///
    /// Returns an error if the file does not exist.
    fn read_file(&self, path: &str) -> impl Future<Output = Result<Vec<u8>>> + Send;

    /// Write contents to a file at the given path.
    ///
    /// Creates the file if it doesn't exist, or overwrites if it does.
    /// Implementations should create any necessary parent directories.
    fn write_file(&self, path: &str, data: &[u8]) -> impl Future<Output = Result<()>> + Send;

    /// Get the lexically first file in a directory.
    ///
    /// Returns `None` if the directory is empty or does not exist.
    /// Returns the filename (not the full path) of the first file.
    fn first_file(&self, directory: &str) -> impl Future<Output = Result<Option<String>>> + Send;
}
