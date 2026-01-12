//! Filesystem-based FileStore implementation.
//!
//! FsFileStore provides access to a local filesystem directory as a FileStore.

use super::chunk_sizes::next_chunk_size;
use super::scan_ignore_helper::ScanIgnoreHelper;
use crate::file_store::{
    DirEntry, DirectoryEntry, DirectoryScanEvent, Error, FileEntry, FileSource, FileStore, Result,
    ScanEvent, ScanEvents, SourceChunk, SourceChunkContent, SourceChunks,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// A FileStore backed by the local filesystem.
pub struct FsFileStore {
    /// Root path on the filesystem.
    root: PathBuf,
}

impl FsFileStore {
    /// Create a new FsFileStore rooted at the given path.
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    /// Convert a relative path to an absolute filesystem path.
    fn to_absolute(&self, relative: &Path) -> PathBuf {
        self.root.join(relative)
    }

    /// Check if a file is executable (Unix only).
    #[cfg(unix)]
    fn is_executable(metadata: &std::fs::Metadata) -> bool {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode();
        // Check if any execute bit is set
        mode & 0o111 != 0
    }

    /// Check if a file is executable (non-Unix always returns false).
    #[cfg(not(unix))]
    fn is_executable(_metadata: &std::fs::Metadata) -> bool {
        false
    }

    /// Create a fingerprint from file metadata.
    ///
    /// Format: `{mtime_millis}:{size}:{x or -}`
    fn fingerprint(metadata: &std::fs::Metadata) -> Option<String> {
        let modified = metadata.modified().ok()?;
        let duration = modified.duration_since(std::time::UNIX_EPOCH).ok()?;
        let millis = duration.as_millis();
        let exec_bit = if Self::is_executable(metadata) {
            "x"
        } else {
            "-"
        };
        Some(format!("{}:{}:{}", millis, metadata.len(), exec_bit))
    }
}

// =============================================================================
// SourceChunk Implementation
// =============================================================================

/// A chunk from a filesystem file.
struct FsSourceChunk {
    /// Path to the file.
    path: PathBuf,
    /// Offset within the file.
    offset: u64,
    /// Size of this chunk.
    size: u64,
}

/// State for lazy chunk iteration.
struct ChunkIterState {
    path: PathBuf,
    file_size: u64,
    offset: u64,
}

#[async_trait]
impl SourceChunk for FsSourceChunk {
    fn offset(&self) -> u64 {
        self.offset
    }

    fn size(&self) -> u64 {
        self.size
    }

    async fn get(&self) -> Result<SourceChunkContent> {
        let mut file = fs::File::open(&self.path).await?;
        file.seek(std::io::SeekFrom::Start(self.offset)).await?;

        let mut buffer = vec![0u8; self.size as usize];
        file.read_exact(&mut buffer).await?;

        let hash = {
            let mut hasher = Sha256::new();
            hasher.update(&buffer);
            format!("{:x}", hasher.finalize())
        };

        Ok(SourceChunkContent {
            bytes: Bytes::from(buffer),
            hash,
        })
    }
}

// =============================================================================
// FileSource Implementation
// =============================================================================

#[async_trait]
impl FileSource for FsFileStore {
    async fn scan(&self) -> Result<ScanEvents> {
        let mut events = Vec::new();
        let mut helper = ScanIgnoreHelper::new();

        // Process root directory first to load top-level ignore files
        let root_entry = DirEntry {
            name: String::new(),
            path: String::new(),
        };
        helper
            .on_scan_event(&DirectoryScanEvent::EnterDirectory(root_entry), self)
            .await;

        // Scan the tree with ignore filtering
        scan_directory(&self.root, &self.root, &mut events, &mut helper, self).await?;

        Ok(Box::pin(stream::iter(events.into_iter().map(Ok))))
    }

    async fn get_source_chunks(&self, path: &Path) -> Result<Option<SourceChunks>> {
        let absolute = self.to_absolute(path);

        let metadata = match fs::metadata(&absolute).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        if !metadata.is_file() {
            return Err(Error::NotAFile(path.to_string_lossy().into_owned()));
        }

        let file_size = metadata.len();

        // Create a lazy stream that computes chunks on demand
        let chunks_stream = stream::unfold(
            ChunkIterState {
                path: absolute,
                file_size,
                offset: 0,
            },
            |state| async move {
                if state.offset >= state.file_size {
                    return None;
                }

                let remaining = state.file_size - state.offset;
                let chunk_size = next_chunk_size(remaining);

                let chunk = FsSourceChunk {
                    path: state.path.clone(),
                    offset: state.offset,
                    size: chunk_size,
                };

                let next_state = ChunkIterState {
                    path: state.path,
                    file_size: state.file_size,
                    offset: state.offset + chunk_size,
                };

                Some((Ok(Box::new(chunk) as Box<dyn SourceChunk>), next_state))
            },
        );

        Ok(Some(Box::pin(chunks_stream)))
    }

    async fn get_entry(&self, path: &Path) -> Result<Option<DirectoryEntry>> {
        let absolute = self.to_absolute(path);

        let metadata = match fs::metadata(&absolute).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let name = path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let path_str = path.to_string_lossy().into_owned();

        if metadata.is_dir() {
            Ok(Some(DirectoryEntry::Dir(DirEntry {
                name,
                path: path_str,
            })))
        } else {
            Ok(Some(DirectoryEntry::File(FileEntry {
                name,
                path: path_str,
                size: metadata.len(),
                executable: Self::is_executable(&metadata),
                fingerprint: Self::fingerprint(&metadata),
            })))
        }
    }

    async fn get_file(&self, path: &Path) -> Result<Bytes> {
        let absolute = self.to_absolute(path);

        let metadata = fs::metadata(&absolute).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound(path.to_string_lossy().into_owned())
            } else {
                Error::Io(e)
            }
        })?;

        if !metadata.is_file() {
            return Err(Error::NotAFile(path.to_string_lossy().into_owned()));
        }

        let contents = fs::read(&absolute).await?;
        Ok(Bytes::from(contents))
    }
}

impl FileStore for FsFileStore {
    fn get_source(&self) -> Option<&dyn FileSource> {
        Some(self)
    }

    fn get_dest(&self) -> Option<&dyn crate::file_store::FileDest> {
        None // Not implemented yet
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Recursively scan a directory, generating scan events with ignore filtering.
async fn scan_directory(
    dir_path: &Path,
    root: &Path,
    events: &mut Vec<ScanEvent>,
    helper: &mut ScanIgnoreHelper,
    source: &FsFileStore,
) -> Result<()> {
    let mut entries = fs::read_dir(dir_path).await?;
    let mut sorted_entries = Vec::new();

    // Collect and sort entries by name
    while let Some(entry) = entries.next_entry().await? {
        sorted_entries.push(entry);
    }
    sorted_entries.sort_by_key(|a| a.file_name());

    for entry in sorted_entries {
        let entry_path = entry.path();
        let file_name = entry.file_name().to_string_lossy().into_owned();
        let metadata = entry.metadata().await?;
        let is_dir = metadata.is_dir();

        // Check if this entry should be ignored
        if helper.should_ignore(&file_name, is_dir) {
            continue;
        }

        let relative_path = entry_path
            .strip_prefix(root)
            .unwrap_or(&entry_path)
            .to_string_lossy()
            .into_owned();

        if is_dir {
            let dir_entry = DirEntry {
                name: file_name,
                path: relative_path,
            };
            events.push(ScanEvent::EnterDirectory(dir_entry.clone()));

            // Notify helper of directory entry to load ignore files
            helper
                .on_scan_event(&DirectoryScanEvent::EnterDirectory(dir_entry), source)
                .await;

            Box::pin(scan_directory(&entry_path, root, events, helper, source)).await?;

            // Notify helper of directory exit
            helper
                .on_scan_event(&DirectoryScanEvent::ExitDirectory, source)
                .await;

            events.push(ScanEvent::ExitDirectory);
        } else {
            events.push(ScanEvent::File(FileEntry {
                name: file_name,
                path: relative_path,
                size: metadata.len(),
                executable: FsFileStore::is_executable(&metadata),
                fingerprint: FsFileStore::fingerprint(&metadata),
            }));
        }
    }

    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    #[tokio::test]
    async fn test_empty_directory() {
        let temp = create_test_dir();
        let store = FsFileStore::new(temp.path());

        let events: Vec<_> = store
            .scan()
            .await
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
            .await;

        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn test_single_file() {
        let temp = create_test_dir();
        File::create(temp.path().join("hello.txt"))
            .unwrap()
            .write_all(b"Hello, World!")
            .unwrap();

        let store = FsFileStore::new(temp.path());

        // Test get_entry
        let entry = store.get_entry(Path::new("hello.txt")).await.unwrap();
        match entry {
            Some(DirectoryEntry::File(f)) => {
                assert_eq!(f.name, "hello.txt");
                assert_eq!(f.size, 13);
            }
            _ => panic!("Expected file entry"),
        }

        // Test get_file
        let contents = store.get_file(Path::new("hello.txt")).await.unwrap();
        assert_eq!(&contents[..], b"Hello, World!");

        // Test scan
        let events: Vec<_> = store
            .scan()
            .await
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
            .await;
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], ScanEvent::File(f) if f.name == "hello.txt"));
    }

    #[tokio::test]
    async fn test_nested_directories() {
        let temp = create_test_dir();
        std::fs::create_dir_all(temp.path().join("a/b")).unwrap();
        File::create(temp.path().join("a/b/c.txt"))
            .unwrap()
            .write_all(b"nested")
            .unwrap();
        File::create(temp.path().join("a/d.txt"))
            .unwrap()
            .write_all(b"sibling")
            .unwrap();

        let store = FsFileStore::new(temp.path());

        let events: Vec<_> = store
            .scan()
            .await
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
            .await;

        // Expected: EnterDir(a), EnterDir(b), File(c.txt), ExitDir, File(d.txt), ExitDir
        assert_eq!(events.len(), 6);
        assert!(matches!(&events[0], ScanEvent::EnterDirectory(d) if d.name == "a"));
        assert!(matches!(&events[1], ScanEvent::EnterDirectory(d) if d.name == "b"));
        assert!(matches!(&events[2], ScanEvent::File(f) if f.name == "c.txt"));
        assert!(matches!(&events[3], ScanEvent::ExitDirectory));
        assert!(matches!(&events[4], ScanEvent::File(f) if f.name == "d.txt"));
        assert!(matches!(&events[5], ScanEvent::ExitDirectory));
    }

    #[tokio::test]
    async fn test_get_source_chunks() {
        let temp = create_test_dir();
        File::create(temp.path().join("small.txt"))
            .unwrap()
            .write_all(b"tiny")
            .unwrap();

        let store = FsFileStore::new(temp.path());

        let mut chunks = store
            .get_source_chunks(Path::new("small.txt"))
            .await
            .unwrap()
            .unwrap();

        let chunk = chunks.next().await.unwrap().unwrap();
        assert_eq!(chunk.offset(), 0);
        assert_eq!(chunk.size(), 4);

        let content = chunk.get().await.unwrap();
        assert_eq!(&content.bytes[..], b"tiny");
        assert_eq!(content.hash.len(), 64); // SHA-256 hex
    }

    #[tokio::test]
    async fn test_get_source_chunks_not_found() {
        let temp = create_test_dir();
        let store = FsFileStore::new(temp.path());

        let result = store
            .get_source_chunks(Path::new("missing.txt"))
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_source_chunks_on_directory() {
        let temp = create_test_dir();
        std::fs::create_dir(temp.path().join("subdir")).unwrap();

        let store = FsFileStore::new(temp.path());

        let result = store.get_source_chunks(Path::new("subdir")).await;
        assert!(matches!(result, Err(Error::NotAFile(_))));
    }

    #[tokio::test]
    async fn test_scan_ignores_git_directory() {
        let temp = create_test_dir();
        std::fs::create_dir(temp.path().join(".git")).unwrap();
        File::create(temp.path().join(".git/config"))
            .unwrap()
            .write_all(b"git config")
            .unwrap();
        File::create(temp.path().join("file.txt"))
            .unwrap()
            .write_all(b"content")
            .unwrap();

        let store = FsFileStore::new(temp.path());

        let events: Vec<_> = store
            .scan()
            .await
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
            .await;

        let names: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                ScanEvent::EnterDirectory(d) => Some(d.name.as_str()),
                ScanEvent::File(f) => Some(f.name.as_str()),
                _ => None,
            })
            .collect();

        assert!(!names.contains(&".git"));
        assert!(names.contains(&"file.txt"));
    }

    #[tokio::test]
    async fn test_scan_respects_gitignore() {
        let temp = create_test_dir();
        File::create(temp.path().join(".gitignore"))
            .unwrap()
            .write_all(b"*.log")
            .unwrap();
        File::create(temp.path().join("app.log"))
            .unwrap()
            .write_all(b"log")
            .unwrap();
        File::create(temp.path().join("main.rs"))
            .unwrap()
            .write_all(b"fn main() {}")
            .unwrap();

        let store = FsFileStore::new(temp.path());

        let events: Vec<_> = store
            .scan()
            .await
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
            .await;

        let names: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                ScanEvent::File(f) => Some(f.name.as_str()),
                _ => None,
            })
            .collect();

        assert!(!names.contains(&"app.log"));
        assert!(names.contains(&"main.rs"));
        assert!(names.contains(&".gitignore"));
    }

    #[tokio::test]
    async fn test_lexicographic_order() {
        let temp = create_test_dir();
        File::create(temp.path().join("z.txt")).unwrap();
        File::create(temp.path().join("a.txt")).unwrap();
        File::create(temp.path().join("m.txt")).unwrap();

        let store = FsFileStore::new(temp.path());

        let events: Vec<_> = store
            .scan()
            .await
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
            .await;

        let names: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                ScanEvent::File(f) => Some(f.name.as_str()),
                _ => None,
            })
            .collect();

        assert_eq!(names, vec!["a.txt", "m.txt", "z.txt"]);
    }

    #[tokio::test]
    async fn test_get_file_not_found() {
        let temp = create_test_dir();
        let store = FsFileStore::new(temp.path());

        let result = store.get_file(Path::new("missing.txt")).await;
        assert!(matches!(result, Err(Error::NotFound(_))));
    }

    #[tokio::test]
    async fn test_get_entry_root() {
        let temp = create_test_dir();
        let store = FsFileStore::new(temp.path());

        let entry = store.get_entry(Path::new("")).await.unwrap();
        assert!(matches!(entry, Some(DirectoryEntry::Dir(_))));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_executable_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp = create_test_dir();
        let script_path = temp.path().join("script.sh");
        File::create(&script_path)
            .unwrap()
            .write_all(b"#!/bin/bash")
            .unwrap();

        // Set executable bit
        let mut perms = std::fs::metadata(&script_path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script_path, perms).unwrap();

        let store = FsFileStore::new(temp.path());

        let entry = store.get_entry(Path::new("script.sh")).await.unwrap();
        match entry {
            Some(DirectoryEntry::File(f)) => {
                assert!(f.executable);
            }
            _ => panic!("Expected file entry"),
        }
    }
}
