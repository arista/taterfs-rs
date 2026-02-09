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
pub use scan_ignore_helper::{ScanDirEntry, ScanDirectoryEvent, ScanFileSource, ScanIgnoreHelper};

use async_trait::async_trait;
use bytes::Bytes;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::OnceCell;

use crate::caches::FileStoreCache;
use crate::repo::BoxedFileChunksWithContent;
use crate::repository::ObjectId;
use crate::util::{ManagedBuffer, WithComplete};

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

    #[error("Repo error: {0}")]
    Repo(#[from] crate::repo::RepoError),

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

/// Async iterator over scan events.
///
/// Call `next()` to get the next scan event. Returns `None` when the scan is complete.
#[async_trait]
pub trait ScanEventList: Send {
    /// Get the next scan event.
    async fn next(&mut self) -> Option<Result<ScanEvent>>;
}

/// Boxed ScanEventList for dynamic dispatch.
pub type ScanEvents = Box<dyn ScanEventList>;

/// A simple ScanEventList implementation backed by a Vec.
pub struct VecScanEventList {
    events: Vec<ScanEvent>,
    index: usize,
}

impl VecScanEventList {
    /// Create a new VecScanEventList from a Vec of events.
    pub fn new(events: Vec<ScanEvent>) -> Self {
        Self { events, index: 0 }
    }
}

#[async_trait]
impl ScanEventList for VecScanEventList {
    async fn next(&mut self) -> Option<Result<ScanEvent>> {
        if self.index < self.events.len() {
            let event = self.events[self.index].clone();
            self.index += 1;
            Some(Ok(event))
        } else {
            None
        }
    }
}

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

/// Metadata about a source chunk.
#[derive(Debug, Clone)]
pub struct SourceChunk {
    /// Offset of this chunk within the file.
    pub offset: u64,
    /// Size of this chunk in bytes.
    pub size: u64,
}

/// Async iterator over source chunks.
#[async_trait]
pub trait SourceChunkList: Send {
    /// Get the next source chunk.
    async fn next(&mut self) -> Option<Result<SourceChunk>>;
}

/// Boxed SourceChunkList for dynamic dispatch.
pub type SourceChunks = Box<dyn SourceChunkList>;

/// A simple SourceChunkList implementation backed by a Vec.
pub struct VecSourceChunkList {
    chunks: Vec<SourceChunk>,
    index: usize,
}

impl VecSourceChunkList {
    /// Create a new VecSourceChunkList from a Vec of chunks.
    pub fn new(chunks: Vec<SourceChunk>) -> Self {
        Self { chunks, index: 0 }
    }
}

#[async_trait]
impl SourceChunkList for VecSourceChunkList {
    async fn next(&mut self) -> Option<Result<SourceChunk>> {
        if self.index < self.chunks.len() {
            let chunk = self.chunks[self.index].clone();
            self.index += 1;
            Some(Ok(chunk))
        } else {
            None
        }
    }
}

/// A chunk with content that can be retrieved on demand.
///
/// The chunk metadata (offset, size) is available immediately.
/// Call `content()` to retrieve the content.
pub struct SourceChunkWithContent {
    /// Offset of this chunk within the file.
    pub offset: u64,
    /// Size of this chunk in bytes.
    pub size: u64,
    /// The content, fetched lazily and cached.
    content_cell: Arc<OnceCell<Result<SourceChunkContent>>>,
    /// Optional JoinHandle for background download (used by S3).
    download_handle:
        tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<SourceChunkContent>>>>,
}

impl SourceChunkWithContent {
    /// Create a new SourceChunkWithContent with pre-computed content.
    pub fn new_immediate(offset: u64, size: u64, content: SourceChunkContent) -> Self {
        let cell = Arc::new(OnceCell::new());
        // We can't fail here since it's a new cell
        let _ = cell.set(Ok(content));
        Self {
            offset,
            size,
            content_cell: cell,
            download_handle: tokio::sync::Mutex::new(None),
        }
    }

    /// Create a new SourceChunkWithContent with a background download task.
    ///
    /// The download will be awaited when `content()` is called.
    pub fn new_with_download(
        offset: u64,
        size: u64,
        handle: tokio::task::JoinHandle<Result<SourceChunkContent>>,
    ) -> Self {
        Self {
            offset,
            size,
            content_cell: Arc::new(OnceCell::new()),
            download_handle: tokio::sync::Mutex::new(Some(handle)),
        }
    }

    /// Get the content of this chunk.
    ///
    /// If the content was created with a background download, this awaits the download.
    /// The result is cached, so subsequent calls return immediately.
    pub async fn content(&self) -> Result<&SourceChunkContent> {
        // Fast path: content already available
        if let Some(result) = self.content_cell.get() {
            return result.as_ref().map_err(|e| Error::Other(e.to_string()));
        }

        // Slow path: await the download handle if present
        let download_result = {
            let mut guard = self.download_handle.lock().await;
            if let Some(handle) = guard.take() {
                match handle.await {
                    Ok(result) => Some(result),
                    Err(e) => Some(Err(Error::Other(format!("Download task failed: {}", e)))),
                }
            } else {
                None
            }
        };

        // Set the content if we got a result from the download
        if let Some(result) = download_result {
            // Ignore error if another task set it first (race condition)
            let _ = self.content_cell.set(result);
        }

        // Now try to get the content
        if let Some(result) = self.content_cell.get() {
            return result.as_ref().map_err(|e| Error::Other(e.to_string()));
        }

        Err(Error::Other("Content not available".to_string()))
    }
}

/// Async iterator over source chunks with content.
///
/// Each call to `next()` acquires a ManagedBuffer and initiates a background download.
/// Call `content()` on the returned `SourceChunkWithContent` to wait for the download.
#[async_trait]
pub trait SourceChunkWithContentList: Send {
    /// Get the next chunk with content.
    ///
    /// This acquires a ManagedBuffer for the chunk and initiates a background download.
    /// Returns the chunk handle immediately; call `content()` to wait for the download.
    async fn next(&mut self) -> Option<Result<SourceChunkWithContent>>;
}

/// Boxed SourceChunkWithContentList for dynamic dispatch.
pub type SourceChunksWithContent = Box<dyn SourceChunkWithContentList>;

// =============================================================================
// Directory Listing
// =============================================================================

/// Trait for providing raw directory listings and file access to DirectoryList.
///
/// This combines the ability to list directory contents with the ability to
/// read files (needed by ScanIgnoreHelper to load ignore files).
/// Extends ScanFileSource so the helper can load ignore files from child directories.
#[async_trait]
pub trait DirectoryListSource: ScanFileSource + Send + Sync {
    /// List the raw (unfiltered) entries in a directory.
    ///
    /// Returns None if the path does not exist.
    /// Returns an error if the path exists but is not a directory.
    async fn list_raw_directory(&self, path: &str) -> Result<Option<Vec<DirectoryEntry>>>;
}

/// An ignore-aware directory listing.
///
/// DirectoryList holds pre-loaded, filtered directory entries and a
/// ScanIgnoreHelper for drilling into child directories via `list_directory`.
pub struct DirectoryList {
    /// Pre-loaded, already filtered entries.
    entries: Vec<DirectoryEntry>,
    /// Current position in entries.
    index: usize,
    /// Ignore helper with state for the current directory.
    helper: ScanIgnoreHelper,
    /// Source for listing child directories and loading ignore files.
    lister: Arc<dyn DirectoryListSource>,
    /// Path of the directory this listing represents (relative to store root).
    path: String,
}

impl DirectoryList {
    /// Create a new DirectoryList by filtering raw entries through the ignore helper.
    pub fn new(
        raw_entries: Vec<DirectoryEntry>,
        helper: ScanIgnoreHelper,
        lister: Arc<dyn DirectoryListSource>,
        path: String,
    ) -> Self {
        let entries: Vec<DirectoryEntry> = raw_entries
            .into_iter()
            .filter(|entry| {
                let (name, is_dir) = match entry {
                    DirectoryEntry::Dir(d) => (&d.name, true),
                    DirectoryEntry::File(f) => (&f.name, false),
                };
                !helper.should_ignore(name, is_dir)
            })
            .collect();
        Self {
            entries,
            index: 0,
            helper,
            lister,
            path,
        }
    }

    /// Yield the next non-ignored entry.
    pub async fn next(&mut self) -> Option<Result<DirectoryEntry>> {
        if self.index < self.entries.len() {
            let entry = self.entries[self.index].clone();
            self.index += 1;
            Some(Ok(entry))
        } else {
            None
        }
    }

    /// Create a child DirectoryList for a subdirectory by name.
    ///
    /// The child inherits the parent's ignore state and loads any additional
    /// ignore files from the child directory.
    pub async fn list_directory(&self, name: &str) -> Result<Option<DirectoryList>> {
        let child_path = if self.path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", self.path, name)
        };

        // List raw entries from the child directory
        let raw_entries = match self.lister.list_raw_directory(&child_path).await? {
            Some(entries) => entries,
            None => return Ok(None),
        };

        // Clone the parent's helper and enter the child directory
        let mut child_helper = self.helper.clone();
        let dir_entry = ScanDirEntry {
            name: name.to_string(),
            path: child_path.clone(),
        };
        child_helper
            .on_scan_event(
                &ScanDirectoryEvent::EnterDirectory(dir_entry),
                self.lister.as_ref(),
            )
            .await;

        Ok(Some(DirectoryList::new(
            raw_entries,
            child_helper,
            Arc::clone(&self.lister),
            child_path,
        )))
    }
}

// =============================================================================
// FileSource Trait
// =============================================================================

// =============================================================================
// Bridge: FileSource -> ScanFileSource
// =============================================================================

/// Blanket implementation so any FileSource can be used as a ScanFileSource.
#[async_trait]
impl<T: FileSource + ?Sized> ScanFileSource for T {
    async fn get_file(
        &self,
        path: &Path,
    ) -> std::result::Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
        FileSource::get_file(self, path)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Convert a file_store DirEntry into a ScanDirEntry.
impl From<DirEntry> for ScanDirEntry {
    fn from(entry: DirEntry) -> Self {
        ScanDirEntry {
            name: entry.name,
            path: entry.path,
        }
    }
}

/// Convert a file_store DirEntry reference into a ScanDirEntry.
impl From<&DirEntry> for ScanDirEntry {
    fn from(entry: &DirEntry) -> Self {
        ScanDirEntry {
            name: entry.name.clone(),
            path: entry.path.clone(),
        }
    }
}

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
    /// Chunks are yielded lazily - this just returns chunk metadata.
    async fn get_source_chunks(&self, path: &Path) -> Result<Option<SourceChunks>>;

    /// Get chunk contents for a file at the given path.
    ///
    /// Returns None if the path does not exist.
    /// Returns an error if the path exists but is not a file.
    ///
    /// Returns an async iterator that yields chunks with content. Each call to
    /// `next()` on the returned list:
    /// 1. Acquires buffer capacity (blocking until available)
    /// 2. Reads the chunk content
    /// 3. Creates a ManagedBuffer with the content
    /// 4. Returns a `SourceChunkWithContent` handle
    ///
    /// Call `content()` on the returned handle to get the content.
    ///
    /// Chunks are computed iteratively and returned in order. Because
    /// buffer capacity is acquired before reading, this prevents unbounded
    /// memory usage and ensures proper backpressure.
    async fn get_source_chunks_with_content(
        &self,
        path: &Path,
    ) -> Result<Option<SourceChunksWithContent>>;

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
    /// Downloads multiple chunks in parallel, then writes them to the file
    /// in sequential order. The file is written to a temporary location,
    /// then moved into its final location atomically.
    ///
    /// Returns once all chunks have started downloading (after `chunks.next()`
    /// has been called for each chunk, which subjects it to ManagedBuffers
    /// flow control). The `WithComplete` signals when all downloads have
    /// completed and the file has been moved to its final location.
    async fn write_file_from_chunks(
        &self,
        path: &Path,
        chunks: BoxedFileChunksWithContent,
        executable: bool,
    ) -> Result<WithComplete<()>>;

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

    /// Create a staging area for downloading file chunks.
    ///
    /// Returns None if staging is not supported by this FileDest.
    /// The stage allows temporarily caching downloaded file chunks before
    /// assembling them into final files.
    async fn create_stage(&self) -> Result<Option<Box<dyn FileDestStage>>>;
}

// =============================================================================
// FileDestStage Trait
// =============================================================================

/// A staging area for temporarily caching downloaded file chunks.
///
/// This allows all required content to be pulled from a repo before modifying
/// any existing files in the FileStore. Once all chunks have been downloaded
/// to the stage, files can be assembled and moved to their final locations
/// quickly, minimizing disruption.
#[async_trait]
pub trait FileDestStage: Send + Sync {
    /// Write a chunk to the stage.
    async fn write_chunk(&self, id: &ObjectId, chunk: Arc<ManagedBuffer>) -> Result<()>;

    /// Read a chunk from the stage.
    ///
    /// Returns None if the chunk is not in the stage.
    async fn read_chunk(&self, id: &ObjectId) -> Result<Option<Arc<ManagedBuffer>>>;

    /// Check if a chunk exists in the stage.
    async fn has_chunk(&self, id: &ObjectId) -> Result<bool>;

    /// Remove the stage and all of its content.
    async fn cleanup(&self) -> Result<()>;
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
