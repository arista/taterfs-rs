use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use crate::file_source::directory_list::DirectoryList;
use crate::file_source::error::Result;
use crate::file_source::file_chunks::FileChunks;

/// Type alias for boxed async function that lists a directory.
pub type ListDirectoryFn = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<DirectoryList>> + Send>> + Send + Sync,
>;

/// Type alias for boxed async function that gets a child entry by name.
pub type GetChildEntryFn = Arc<
    dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<Option<DirectoryListEntry>>> + Send>>
        + Send
        + Sync,
>;

/// Type alias for boxed async function that gets file chunks.
pub type GetChunksFn =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<FileChunks>> + Send>> + Send + Sync>;

/// A directory entry in a directory listing.
#[derive(Clone)]
pub struct DirEntry {
    /// The base name of the directory.
    pub name: String,
    /// The path to the directory from the FileSource root (uses OS separators).
    pub path: PathBuf,
    /// Function to list this directory's contents.
    list_fn: ListDirectoryFn,
    /// Function to get a child entry by name.
    get_child_fn: GetChildEntryFn,
}

impl DirEntry {
    /// Create a new DirEntry with the provided callback functions.
    pub fn new(
        name: String,
        path: PathBuf,
        list_fn: ListDirectoryFn,
        get_child_fn: GetChildEntryFn,
    ) -> Self {
        Self {
            name,
            path,
            list_fn,
            get_child_fn,
        }
    }

    /// List the contents of this directory.
    pub async fn list_directory(&self) -> Result<DirectoryList> {
        (self.list_fn)().await
    }

    /// Get an immediate child entry by name.
    pub async fn get_entry(&self, name: &str) -> Result<Option<DirectoryListEntry>> {
        (self.get_child_fn)(name.to_string()).await
    }
}

impl fmt::Debug for DirEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirEntry")
            .field("name", &self.name)
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

/// A file entry in a directory listing.
#[derive(Clone)]
pub struct FileEntry {
    /// The base name of the file.
    pub name: String,
    /// The path to the file from the FileSource root (uses OS separators).
    pub path: PathBuf,
    /// Size of the file in bytes.
    pub size: u64,
    /// Whether the file is executable.
    pub executable: bool,
    /// Function to get this file's chunks.
    get_chunks_fn: GetChunksFn,
}

impl FileEntry {
    /// Create a new FileEntry with the provided callback function.
    pub fn new(
        name: String,
        path: PathBuf,
        size: u64,
        executable: bool,
        get_chunks_fn: GetChunksFn,
    ) -> Self {
        Self {
            name,
            path,
            size,
            executable,
            get_chunks_fn,
        }
    }

    /// Get the chunks of this file.
    pub async fn get_chunks(&self) -> Result<FileChunks> {
        (self.get_chunks_fn)().await
    }
}

impl fmt::Debug for FileEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileEntry")
            .field("name", &self.name)
            .field("path", &self.path)
            .field("size", &self.size)
            .field("executable", &self.executable)
            .finish_non_exhaustive()
    }
}

/// An entry in a directory listing.
#[derive(Debug, Clone)]
pub enum DirectoryListEntry {
    /// A directory.
    Directory(DirEntry),
    /// A file.
    File(FileEntry),
}

impl DirectoryListEntry {
    /// Get the base name of this entry.
    pub fn name(&self) -> &str {
        match self {
            DirectoryListEntry::Directory(d) => &d.name,
            DirectoryListEntry::File(f) => &f.name,
        }
    }

    /// Get the path of this entry from the FileSource root.
    pub fn path(&self) -> &PathBuf {
        match self {
            DirectoryListEntry::Directory(d) => &d.path,
            DirectoryListEntry::File(f) => &f.path,
        }
    }
}

/// A chunk of file data.
#[derive(Debug, Clone)]
pub struct FileChunk {
    offset: u64,
    data: Vec<u8>,
}

impl FileChunk {
    /// Create a new file chunk.
    pub fn new(offset: u64, data: Vec<u8>) -> Self {
        Self { offset, data }
    }

    /// Get the offset of this chunk within the file.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Get the size of this chunk in bytes.
    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    /// Get the data of this chunk.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Consume this chunk and return its data.
    pub fn into_data(self) -> Vec<u8> {
        self.data
    }
}

/// Chunk sizes used for breaking files into chunks.
/// Sizes are in descending order: 4MB, 1MB, 256KB, 64KB, 16KB.
pub const CHUNK_SIZES: &[u64] = &[
    4 * 1024 * 1024, // 4MB
    1024 * 1024,     // 1MB
    256 * 1024,      // 256KB
    64 * 1024,       // 64KB
    16 * 1024,       // 16KB
];

/// Calculate the next chunk size for a file with the given remaining bytes.
/// Returns the largest chunk size from CHUNK_SIZES that fits, or the remaining bytes
/// if none fit.
pub fn next_chunk_size(remaining: u64) -> u64 {
    for &size in CHUNK_SIZES {
        if size <= remaining {
            return size;
        }
    }
    remaining
}
