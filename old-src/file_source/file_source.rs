// Interfaces for a FileSource - a representation of a filesystem that provides files, typically for uploading

// Thanks ChatGPT

use async_trait::async_trait;
use bytes::Bytes;
use std::fmt;
use std::path::{Path, PathBuf};

//----------------------------------------
// FileSourceService - interface to a FileSource

#[async_trait(?Send)]
pub trait FileSourceService {
    async fn list_directory(&self, root: &Path) -> anyhow::Result<Box<dyn DirectoryLister>>;
    async fn get_file_chunks(
        &self,
        path: std::path::PathBuf,
    ) -> anyhow::Result<Box<dyn FileChunksIterator>>;
}

//----------------------------------------
// DirectoryLister - lists the contents of a directory, sorted by name.  Directory entries are yielded one at a time by calling "next()", until Ok(None) is reached.

#[async_trait(?Send)]
pub trait DirectoryLister {
    // Pull the next entry, or `Ok(None)` at end-of-directory.
    async fn next(&mut self) -> anyhow::Result<Option<DirEntry>>;
}

#[derive(Debug)]
pub enum DirEntry {
    File(FileEntry),
    Directory(DirectoryEntry),
}

#[derive(Debug, Clone)]
pub struct FileEntry {
    // Basename
    pub name: String,
    pub abs_path: PathBuf,
    // Path relative to the listing root (uses OS separators)
    pub rel_path: PathBuf,
    pub size: u64,
    pub executable: bool,
}

pub struct DirectoryEntry {
    // Basename
    pub name: String,
    pub abs_path: PathBuf,
    // Path relative to the listing root (uses OS separators)
    pub rel_path: PathBuf,
    // DirectoryLister that can be used to recursively list this directory
    pub lister: Box<dyn DirectoryLister>,
}

// Implement Debug without including lister
impl fmt::Debug for DirectoryEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectoryEntry")
            .field("name", &self.name)
            .field("rel_path", &self.rel_path)
            .finish_non_exhaustive() // makes it obvious there are more fields
    }
}

//----------------------------------------
// FileChunksIterator - iterates through a file, yielding chunks in CHUNK_SIZES order

/// A handle for one file chunk.
#[async_trait(?Send)]
pub trait FileChunkHandle {
    /// Size of this chunk in bytes.
    fn size(&self) -> usize;

    /// Offset of this chunk in bytes.
    fn offset(&self) -> usize;

    /// Fetch the chunk's bytes.
    async fn get_chunk(&self) -> anyhow::Result<Bytes>;
}

/// An async iterator over file chunks. Call `next().await` until it returns `Ok(None)`.
#[async_trait(?Send)]
pub trait FileChunksIterator {
    async fn next(&mut self) -> anyhow::Result<Option<Box<dyn FileChunkHandle>>>;
}
