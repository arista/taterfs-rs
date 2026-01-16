//! File Store traits and types for reading and writing file hierarchies.
//!
//! A FileStore comprises two optional interfaces:
//! - [`FileSource`] - allows reading/scanning directory and file data
//! - [`FileDest`] - allows writing directory and file data

mod chunk_sizes;
mod create_file_store;
mod fs_file_store;
mod memory_file_store;
mod s3_file_store;
mod scan_ignore_helper;

pub use chunk_sizes::{CHUNK_SIZES, next_chunk_size};
pub use create_file_store::{
    CreateFileStoreContext, CreateFileStoreError, FileStoreType, ParsedFileStoreSpec,
    create_file_store,
};
pub use fs_file_store::FsFileStore;
pub use memory_file_store::{MemoryFileStore, MemoryFileStoreBuilder, MemoryFsEntry};
pub use s3_file_store::{S3FileSource, S3FileSourceConfig};
pub use scan_ignore_helper::ScanIgnoreHelper;

use async_trait::async_trait;
use bytes::Bytes;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use crate::caches::FileStoreCache;
use crate::util::ManagedBuffer;

/// Result type for file store operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in file store operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Path not found: {0}")]
    NotFound(String),

    #[error("Path is not a file: {0}")]
    NotAFile(String),

    #[error("Path is not a directory: {0}")]
    NotADirectory(String),

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("{0}")]
    Other(String),
}

// =============================================================================
// Entry Types
// =============================================================================

/// A directory entry returned during scanning or listing.
#[derive(Debug, Clone)]
pub struct DirEntry {
    /// Base name of the directory.
    pub name: String,
    /// Path relative to the FileStore's root.
    pub path: String,
}

/// A file entry returned during scanning or listing.
#[derive(Debug, Clone)]
pub struct FileEntry {
    /// Base name of the file.
    pub name: String,
    /// Path relative to the FileStore's root.
    pub path: String,
    /// Size of the file in bytes.
    pub size: u64,
    /// Whether the file is executable.
    pub executable: bool,
    /// A string that should change when the file changes.
    /// Used for quick change detection without reading file contents.
    /// Must be less than 128 characters if present.
    pub fingerprint: Option<String>,
}

/// A directory entry that can be either a directory or a file.
#[derive(Debug, Clone)]
pub enum DirectoryEntry {
    Dir(DirEntry),
    File(FileEntry),
}

// =============================================================================
// Scan Types
// =============================================================================

/// Events yielded during a depth-first directory scan.
#[derive(Debug, Clone)]
pub enum ScanEvent {
    /// Entering a directory.
    EnterDirectory(DirEntry),
    /// Exiting a directory (returning to parent).
    ExitDirectory,
    /// A file was encountered.
    File(FileEntry),
}

/// Events for directory enter/exit only (used by ScanIgnoreHelper).
#[derive(Debug, Clone)]
pub enum DirectoryScanEvent {
    /// Entering a directory.
    EnterDirectory(DirEntry),
    /// Exiting a directory.
    ExitDirectory,
}

/// Async iterator over scan events.
pub type ScanEvents = Pin<Box<dyn futures::Stream<Item = Result<ScanEvent>> + Send>>;

// =============================================================================
// Chunk Types
// =============================================================================

/// Content retrieved from a source chunk.
pub struct SourceChunkContent {
    /// Offset of this chunk within the file.
    pub offset: u64,
    /// Size of this chunk in bytes.
    pub size: u64,
    /// The chunk data.
    pub bytes: Arc<ManagedBuffer>,
    /// SHA-256 hash of the content in lower-case hexadecimal.
    pub hash: String,
}

/// A chunk of a file that can be retrieved on demand.
///
/// The chunk metadata (offset, size) is available immediately,
/// but the actual content is fetched lazily via `get()`.
#[async_trait]
pub trait SourceChunk: Send + Sync {
    /// Offset of this chunk within the file.
    fn offset(&self) -> u64;

    /// Size of this chunk in bytes.
    fn size(&self) -> u64;

    /// Retrieve the chunk content.
    /// This may be called in any order, and multiple chunks may be
    /// retrieved simultaneously.
    async fn get(&self) -> Result<SourceChunkContent>;
}

/// Async iterator over source chunks.
pub type SourceChunks = Pin<Box<dyn futures::Stream<Item = Result<Box<dyn SourceChunk>>> + Send>>;

/// Async iterator over source chunk contents.
pub type SourceChunkContents =
    Pin<Box<dyn futures::Stream<Item = Result<SourceChunkContent>> + Send>>;

// =============================================================================
// Directory Listing
// =============================================================================

/// Async iterator over directory entries.
pub type DirectoryList = Pin<Box<dyn futures::Stream<Item = Result<DirectoryEntry>> + Send>>;

// =============================================================================
// FileSource Trait
// =============================================================================

/// A source of directory and file data that can be scanned and read.
#[async_trait]
pub trait FileSource: Send + Sync {
    /// Walks depth-first through the directory structure yielding events
    /// in lexicographic order.
    ///
    /// The scan starts at the given path. Returns an error if the path does
    /// not exist or is not a directory. The resulting ScanEvents are yielded
    /// relative to the path. If path is None, then scan through the entire
    /// FileStore.
    ///
    /// Events are: EnterDirectory, File, ExitDirectory
    async fn scan(&self, path: Option<&Path>) -> Result<ScanEvents>;

    /// Get chunks for a file at the given path.
    ///
    /// Returns None if the path does not exist.
    /// Returns an error if the path exists but is not a file.
    ///
    /// Chunks are yielded lazily - content is not fetched until
    /// `SourceChunk::get()` is called.
    async fn get_source_chunks(&self, path: &Path) -> Result<Option<SourceChunks>>;

    /// Get chunk contents for a file, given a stream of chunks.
    ///
    /// Similar to iterating over `get_source_chunks` and calling `get()` on each,
    /// but allows implementations to fetch multiple chunks concurrently while
    /// still returning them in order.
    ///
    /// The chunks are returned in the same order as the input stream.
    async fn get_source_chunk_contents(&self, chunks: SourceChunks) -> Result<SourceChunkContents>;

    /// Get information about a file or directory at the given path.
    ///
    /// Returns None if the path does not exist.
    async fn get_entry(&self, path: &Path) -> Result<Option<DirectoryEntry>>;

    /// Retrieve an entire file's contents.
    ///
    /// Returns an error if the path does not exist or is not a file.
    /// This should only be used when the file is expected to be relatively small.
    async fn get_file(&self, path: &Path) -> Result<Bytes>;
}

// =============================================================================
// FileDest Trait
// =============================================================================

/// A destination for writing directory and file data.
#[async_trait]
pub trait FileDest: Send + Sync {
    /// Get information about a file or directory at the given path.
    ///
    /// Returns None if the path does not exist.
    async fn get_entry(&self, path: &Path) -> Result<Option<DirectoryEntry>>;

    /// List the contents of a directory.
    ///
    /// Returns None if the path does not exist.
    /// Returns an error if the path exists but is not a directory.
    async fn list_directory(&self, path: &Path) -> Result<Option<DirectoryList>>;

    /// Write a file whose contents are supplied by the given chunks.
    ///
    /// The implementation should avoid leaving a partially-written file
    /// even if interrupted (e.g., write to temp location then move atomically).
    async fn write_file_from_chunks(&self, path: &Path, chunks: SourceChunks) -> Result<()>;

    /// Remove the file or directory at the given path, if it exists.
    ///
    /// If the path is a directory, it and all its contents are removed.
    async fn rm(&self, path: &Path) -> Result<()>;

    /// Create a directory at the given path if one doesn't exist.
    ///
    /// Returns an error if a file already exists at the path.
    /// Parent directories are created as needed.
    async fn mkdir(&self, path: &Path) -> Result<()>;

    /// Change the executable bit of a file.
    ///
    /// Returns an error if the path does not point to a file.
    async fn set_executable(&self, path: &Path, executable: bool) -> Result<()>;
}

// =============================================================================
// FileStore Trait
// =============================================================================

/// A file store that can provide FileSource and/or FileDest interfaces.
pub trait FileStore: Send + Sync {
    /// Get the FileSource interface, if supported.
    fn get_source(&self) -> Option<&dyn FileSource>;

    /// Get the FileDest interface, if supported.
    fn get_dest(&self) -> Option<&dyn FileDest>;

    /// Get the cache for this file store.
    fn get_cache(&self) -> Arc<dyn FileStoreCache>;
}
