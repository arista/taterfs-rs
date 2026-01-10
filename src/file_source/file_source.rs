use std::future::Future;
use std::path::Path;

use crate::file_source::directory_list::DirectoryList;
use crate::file_source::error::Result;
use crate::file_source::file_chunks::FileChunks;
use crate::file_source::types::DirectoryListEntry;

/// A source of directory and file data.
///
/// FileSource abstracts reading from various storage backends (local filesystem,
/// S3, HTTP, etc.) providing a uniform interface for listing directories and
/// reading file contents as chunks.
pub trait FileSource: Send + Sync {
    /// List entries in a directory.
    ///
    /// Returns a DirectoryList that yields entries in lexical order.
    /// Returns `FileSourceError::NotFound` if the path does not exist or is not a directory.
    fn list_directory(&self, path: &Path) -> impl Future<Output = Result<DirectoryList>> + Send;

    /// Get the chunks of a file.
    ///
    /// Returns a FileChunks iterator that yields chunks according to the chunking algorithm.
    /// Returns `FileSourceError::NotFound` if the path does not exist or is not a file.
    fn get_file_chunks(&self, path: &Path) -> impl Future<Output = Result<FileChunks>> + Send;

    /// Get a single directory entry.
    ///
    /// Returns information about the file or directory at the given path,
    /// or None if the path does not exist.
    fn get_entry(
        &self,
        path: &Path,
    ) -> impl Future<Output = Result<Option<DirectoryListEntry>>> + Send;
}
