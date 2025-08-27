// Interfaces for a FileStore - a representation of a filesystem that will likely serve as 

// Thanks ChatGPT

use async_trait::async_trait;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

//----------------------------------------
// FileStoreService - interface to a FileStore

#[async_trait]
pub trait FileStoreService: Send + Sync {
    async fn list_directory(&self, root: &Path) -> io::Result<Box<dyn DirectoryLister>>;
}

//----------------------------------------
// DirectoryLister - lists the contents of a directory, sorted by name.  Directory entries are yielded one at a time by calling "next()", until Ok(None) is reached.

#[async_trait]
pub trait DirectoryLister: Send {
    // Pull the next entry, or `Ok(None)` at end-of-directory.
    async fn next(&mut self) -> io::Result<Option<DirEntry>>;
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
    // Path relative to the listing root (uses OS separators)
    pub rel_path: PathBuf,
    pub size: u64,
    pub executable: bool,
}

pub struct DirectoryEntry {
    // Basename
    pub name: String,
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
