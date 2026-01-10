use std::path::PathBuf;

/// Common name information for directory list entries.
#[derive(Debug, Clone)]
pub struct DirectoryListEntryName {
    /// The base name of the file or directory.
    pub name: String,
    /// The absolute path to the file or directory.
    pub abs_path: PathBuf,
    /// The path relative to the listing root (uses OS separators).
    pub rel_path: PathBuf,
}

/// A directory entry in a directory listing.
#[derive(Debug, Clone)]
pub struct DirEntry {
    /// Name information for this directory.
    pub name: DirectoryListEntryName,
}

/// A file entry in a directory listing.
#[derive(Debug, Clone)]
pub struct FileEntry {
    /// Name information for this file.
    pub name: DirectoryListEntryName,
    /// Size of the file in bytes.
    pub size: u64,
    /// Whether the file is executable.
    pub executable: bool,
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
    /// Get the name information for this entry.
    pub fn name(&self) -> &DirectoryListEntryName {
        match self {
            DirectoryListEntry::Directory(d) => &d.name,
            DirectoryListEntry::File(f) => &f.name,
        }
    }

    /// Get the base name of this entry.
    pub fn base_name(&self) -> &str {
        &self.name().name
    }

    /// Get the absolute path of this entry.
    pub fn abs_path(&self) -> &PathBuf {
        &self.name().abs_path
    }

    /// Get the relative path of this entry.
    pub fn rel_path(&self) -> &PathBuf {
        &self.name().rel_path
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
    4 * 1024 * 1024,  // 4MB
    1024 * 1024,      // 1MB
    256 * 1024,       // 256KB
    64 * 1024,        // 64KB
    16 * 1024,        // 16KB
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
