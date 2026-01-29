//! Filesystem-based FileStore implementation.
//!
//! FsFileStore provides access to a local filesystem directory as a FileStore.

use super::chunk_sizes::next_chunk_size;
use super::scan_ignore_helper::{ScanDirEntry, ScanDirectoryEvent, ScanIgnoreHelper};
use crate::caches::FileStoreCache;
use crate::file_store::{
    DirEntry, DirectoryEntry, DirectoryList, DirectoryListSource, Error, FileEntry, FileSource,
    FileStore, Result, ScanEvent, ScanEvents, SourceChunk, SourceChunkContent, SourceChunkContents,
    SourceChunks,
};
use crate::util::ManagedBuffers;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// A FileStore backed by the local filesystem.
pub struct FsFileStore {
    /// Root path on the filesystem.
    root: PathBuf,
    /// Buffer manager for chunk allocation.
    managed_buffers: ManagedBuffers,
    /// Cache for this file store.
    cache: Arc<dyn FileStoreCache>,
}

impl FsFileStore {
    /// Create a new FsFileStore rooted at the given path.
    pub fn new(
        root: impl AsRef<Path>,
        managed_buffers: ManagedBuffers,
        cache: Arc<dyn FileStoreCache>,
    ) -> Self {
        let root_path = root.as_ref().to_path_buf();
        Self {
            root: root_path,
            managed_buffers,
            cache,
        }
    }

    /// Convert a path to an absolute filesystem path.
    ///
    /// The path is always interpreted relative to the file store's root,
    /// even if it appears to be absolute (starts with `/`).
    fn to_absolute(&self, path: &Path) -> PathBuf {
        // Strip leading "/" or root component to ensure path is treated as relative
        let relative = path
            .strip_prefix("/")
            .unwrap_or(path);
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
    /// Buffer manager for chunk allocation.
    managed_buffers: ManagedBuffers,
}

/// State for lazy chunk iteration.
struct ChunkIterState {
    path: PathBuf,
    file_size: u64,
    offset: u64,
    managed_buffers: ManagedBuffers,
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

        // Wrap the buffer in a ManagedBuffer
        let managed_buffer = self.managed_buffers.get_buffer_with_data(buffer).await;

        Ok(SourceChunkContent {
            offset: self.offset,
            size: self.size,
            bytes: Arc::new(managed_buffer),
            hash,
        })
    }
}

// =============================================================================
// FileSource Implementation
// =============================================================================

#[async_trait]
impl FileSource for FsFileStore {
    async fn scan(&self, path: Option<&Path>) -> Result<ScanEvents> {
        // Determine the starting directory
        let start_path = match path {
            Some(p) => self.to_absolute(p),
            None => self.root.clone(),
        };

        // Check if start_path is a directory, return error if not
        match tokio::fs::metadata(&start_path).await {
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => {
                let path_str = path.map(|p| p.to_string_lossy().into_owned()).unwrap_or_default();
                return Err(Error::NotADirectory(path_str));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let path_str = path.map(|p| p.to_string_lossy().into_owned()).unwrap_or_default();
                return Err(Error::NotFound(path_str));
            }
            Err(e) => return Err(e.into()),
        }

        let mut events = Vec::new();
        let mut helper = ScanIgnoreHelper::new();

        // Initialize helper by walking from root to the target path,
        // loading ignore files along the way
        helper.initialize_to_path(path, self).await;

        // Scan the tree with ignore filtering
        scan_directory(&start_path, &start_path, &mut events, &mut helper, self).await?;

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
        let managed_buffers = self.managed_buffers.clone();
        let chunks_stream = stream::unfold(
            ChunkIterState {
                path: absolute,
                file_size,
                offset: 0,
                managed_buffers,
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
                    managed_buffers: state.managed_buffers.clone(),
                };

                let next_state = ChunkIterState {
                    path: state.path,
                    file_size: state.file_size,
                    offset: state.offset + chunk_size,
                    managed_buffers: state.managed_buffers,
                };

                Some((Ok(Box::new(chunk) as Box<dyn SourceChunk>), next_state))
            },
        );

        Ok(Some(Box::pin(chunks_stream)))
    }

    async fn get_source_chunk_contents(&self, chunks: SourceChunks) -> Result<SourceChunkContents> {
        // Sequential implementation - no concurrency for FsFileStore
        let contents_stream = chunks.then(|chunk_result| async move {
            let chunk = chunk_result?;
            chunk.get().await
        });
        Ok(Box::pin(contents_stream))
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
        Some(self)
    }

    fn get_cache(&self) -> Arc<dyn FileStoreCache> {
        self.cache.clone()
    }
}

// =============================================================================
// DirectoryListSource Implementation
// =============================================================================

/// A DirectoryListSource backed by the local filesystem.
struct FsDirectoryListSource {
    root: PathBuf,
}

#[async_trait]
impl super::scan_ignore_helper::ScanFileSource for FsDirectoryListSource {
    async fn get_file(
        &self,
        path: &Path,
    ) -> std::result::Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
        let relative = path.strip_prefix("/").unwrap_or(path);
        let absolute = self.root.join(relative);
        let contents = fs::read(&absolute).await?;
        Ok(Bytes::from(contents))
    }
}

#[async_trait]
impl DirectoryListSource for FsDirectoryListSource {
    async fn list_raw_directory(&self, path: &str) -> Result<Option<Vec<DirectoryEntry>>> {
        let absolute = if path.is_empty() {
            self.root.clone()
        } else {
            self.root.join(path)
        };

        let metadata = match fs::metadata(&absolute).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        if !metadata.is_dir() {
            return Err(Error::NotADirectory(path.to_string()));
        }

        let mut read_dir = fs::read_dir(&absolute).await?;
        let mut entries = Vec::new();

        while let Some(entry) = read_dir.next_entry().await? {
            let file_type = entry.file_type().await?;
            // Only process regular files and directories, skip symlinks etc.
            if !file_type.is_dir() && !file_type.is_file() {
                continue;
            }

            let file_name = entry.file_name().to_string_lossy().into_owned();
            let entry_path = if path.is_empty() {
                file_name.clone()
            } else {
                format!("{}/{}", path, file_name)
            };

            if file_type.is_dir() {
                entries.push(DirectoryEntry::Dir(DirEntry {
                    name: file_name,
                    path: entry_path,
                }));
            } else {
                let metadata = entry.metadata().await?;
                entries.push(DirectoryEntry::File(FileEntry {
                    name: file_name,
                    path: entry_path,
                    size: metadata.len(),
                    executable: FsFileStore::is_executable(&metadata),
                    fingerprint: FsFileStore::fingerprint(&metadata),
                }));
            }
        }

        // Sort by name for consistent ordering
        entries.sort_by(|a, b| {
            let name_a = match a {
                DirectoryEntry::Dir(d) => &d.name,
                DirectoryEntry::File(f) => &f.name,
            };
            let name_b = match b {
                DirectoryEntry::Dir(d) => &d.name,
                DirectoryEntry::File(f) => &f.name,
            };
            name_a.cmp(name_b)
        });

        Ok(Some(entries))
    }
}

// =============================================================================
// FileDest Implementation
// =============================================================================

#[async_trait]
impl crate::file_store::FileDest for FsFileStore {
    async fn get_entry(&self, path: &Path) -> Result<Option<DirectoryEntry>> {
        FileSource::get_entry(self, path).await
    }

    async fn list_directory(&self, path: &Path) -> Result<Option<DirectoryList>> {
        let relative = path.strip_prefix("/").unwrap_or(path);
        let path_str = relative.to_string_lossy().into_owned();

        let absolute = self.to_absolute(path);
        let metadata = match fs::metadata(&absolute).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        if !metadata.is_dir() {
            return Err(Error::NotADirectory(path_str.clone()));
        }

        let lister: Arc<dyn DirectoryListSource> = Arc::new(FsDirectoryListSource {
            root: self.root.clone(),
        });

        // Create and initialize ignore helper
        let mut helper = ScanIgnoreHelper::new();
        helper
            .initialize_to_path(Some(relative), lister.as_ref())
            .await;

        // List raw entries
        let raw_entries = match lister.list_raw_directory(&path_str).await? {
            Some(entries) => entries,
            None => return Ok(None),
        };

        Ok(Some(DirectoryList::new(
            raw_entries,
            helper,
            lister,
            path_str,
        )))
    }

    async fn write_file_from_chunks(
        &self,
        path: &Path,
        chunks: SourceChunks,
        executable: bool,
    ) -> Result<()> {
        let absolute = self.to_absolute(path);

        // Create parent directories
        if let Some(parent) = absolute.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Write to a temp file in the same directory for atomic rename.
        // Use a unique name based on process id and a counter to avoid collisions.
        let file_name = absolute
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let temp_name = format!(
            ".{}.{}.tmp",
            file_name,
            std::process::id(),
        );
        let temp_path = absolute
            .parent()
            .unwrap_or(&self.root)
            .join(&temp_name);

        let mut file: fs::File = fs::File::create(&temp_path).await?;

        // Stream chunks into the temp file
        let mut chunks = chunks;
        while let Some(chunk_result) = chunks.next().await {
            let chunk = chunk_result?;
            let content = chunk.get().await?;
            file.write_all(&content.bytes[..]).await?;
        }
        file.flush().await?;
        drop(file);

        // Set executable bit if needed
        #[cfg(unix)]
        if executable {
            use std::os::unix::fs::PermissionsExt;
            let metadata = fs::metadata(&temp_path).await?;
            let mut perms = metadata.permissions();
            let mode = perms.mode();
            // Add execute bits where read bits are set
            perms.set_mode(mode | ((mode & 0o444) >> 2));
            fs::set_permissions(&temp_path, perms).await?;
        }

        // Atomic rename to final path
        fs::rename(&temp_path, &absolute).await?;

        // If not executable on unix, ensure execute bits are cleared
        // (in case the destination previously had them)
        #[cfg(unix)]
        if !executable {
            use std::os::unix::fs::PermissionsExt;
            let metadata = fs::metadata(&absolute).await?;
            let mut perms = metadata.permissions();
            let mode = perms.mode();
            if mode & 0o111 != 0 {
                perms.set_mode(mode & !0o111);
                fs::set_permissions(&absolute, perms).await?;
            }
        }

        Ok(())
    }

    async fn rm(&self, path: &Path) -> Result<()> {
        let absolute = self.to_absolute(path);

        let metadata = match fs::metadata(&absolute).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };

        if metadata.is_dir() {
            fs::remove_dir_all(&absolute).await?;
        } else {
            fs::remove_file(&absolute).await?;
        }

        Ok(())
    }

    async fn mkdir(&self, path: &Path) -> Result<()> {
        let absolute = self.to_absolute(path);

        // Check if a file exists at the path
        match fs::metadata(&absolute).await {
            Ok(m) if m.is_file() => {
                return Err(Error::NotADirectory(
                    path.to_string_lossy().into_owned(),
                ));
            }
            Ok(_) => return Ok(()), // Already a directory
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }

        fs::create_dir_all(&absolute).await?;
        Ok(())
    }

    async fn set_executable(&self, path: &Path, executable: bool) -> Result<()> {
        let absolute = self.to_absolute(path);

        let metadata = match fs::metadata(&absolute).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::NotFound(path.to_string_lossy().into_owned()));
            }
            Err(e) => return Err(e.into()),
        };

        if !metadata.is_file() {
            return Err(Error::NotAFile(path.to_string_lossy().into_owned()));
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = metadata.permissions();
            let mode = perms.mode();
            if executable {
                // Add execute bits where read bits are set
                perms.set_mode(mode | ((mode & 0o444) >> 2));
            } else {
                // Remove all execute bits
                perms.set_mode(mode & !0o111);
            }
            fs::set_permissions(&absolute, perms).await?;
        }

        Ok(())
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Recursively scan a directory, generating scan events with ignore filtering.
///
/// Only processes regular files and directories. Symlinks, block/char devices,
/// sockets, FIFOs, and other special file types are skipped.
///
/// TODO: Add support for symlinks. Currently symlinks are ignored to avoid
/// infinite loops and to keep the initial implementation simple. Future work
/// could support symlinks with cycle detection and configurable behavior.
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
        let file_type = entry.file_type().await?;

        // Only process regular files and directories.
        // Skip symlinks, block/char devices, sockets, FIFOs, etc.
        let is_dir = file_type.is_dir();
        let is_file = file_type.is_file();
        if !is_dir && !is_file {
            continue;
        }

        let entry_path = entry.path();
        let file_name = entry.file_name().to_string_lossy().into_owned();
        let metadata = entry.metadata().await?;

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
                .on_scan_event(
                    &ScanDirectoryEvent::EnterDirectory(ScanDirEntry::from(&dir_entry)),
                    source,
                )
                .await;

            Box::pin(scan_directory(&entry_path, root, events, helper, source)).await?;

            // Notify helper of directory exit
            helper
                .on_scan_event(&ScanDirectoryEvent::ExitDirectory, source)
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
    use crate::caches::NoopFileStoreCache;
    use futures::StreamExt;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    fn noop_cache() -> Arc<dyn FileStoreCache> {
        Arc::new(NoopFileStoreCache)
    }

    #[tokio::test]
    async fn test_empty_directory() {
        let temp = create_test_dir();
        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());

        let events: Vec<_> = store
            .scan(None)
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

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());

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
            .scan(None)
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

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());

        let events: Vec<_> = store
            .scan(None)
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

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());

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
        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());

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

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());

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

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());

        let events: Vec<_> = store
            .scan(None)
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

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());

        let events: Vec<_> = store
            .scan(None)
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

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());

        let events: Vec<_> = store
            .scan(None)
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
        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());

        let result = store.get_file(Path::new("missing.txt")).await;
        assert!(matches!(result, Err(Error::NotFound(_))));
    }

    #[tokio::test]
    async fn test_get_entry_root() {
        let temp = create_test_dir();
        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());

        let entry = store.get_entry(Path::new("")).await.unwrap();
        assert!(matches!(entry, Some(DirectoryEntry::Dir(_))));
    }

    #[tokio::test]
    async fn test_absolute_path_treated_as_relative() {
        let temp = create_test_dir();

        // Create a file
        File::create(temp.path().join("hello.txt"))
            .unwrap()
            .write_all(b"Hello")
            .unwrap();

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());

        // Even with an absolute path, it should be relative to the store root
        let entry = store.get_entry(Path::new("/hello.txt")).await.unwrap();
        assert!(matches!(entry, Some(DirectoryEntry::File(_))));

        // Also test with get_file
        let contents = store.get_file(Path::new("/hello.txt")).await.unwrap();
        assert_eq!(&contents[..], b"Hello");
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

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());

        let entry = store.get_entry(Path::new("script.sh")).await.unwrap();
        match entry {
            Some(DirectoryEntry::File(f)) => {
                assert!(f.executable);
            }
            _ => panic!("Expected file entry"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_scan_ignores_symlinks() {
        let temp = create_test_dir();

        // Create a regular file
        File::create(temp.path().join("real.txt"))
            .unwrap()
            .write_all(b"real content")
            .unwrap();

        // Create a symlink to the file
        std::os::unix::fs::symlink(
            temp.path().join("real.txt"),
            temp.path().join("link.txt"),
        )
        .unwrap();

        // Create a directory and a symlink to it
        std::fs::create_dir(temp.path().join("realdir")).unwrap();
        std::os::unix::fs::symlink(
            temp.path().join("realdir"),
            temp.path().join("linkdir"),
        )
        .unwrap();

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());

        let events: Vec<_> = store
            .scan(None)
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

        // Should include real file and directory, but not symlinks
        assert!(names.contains(&"real.txt"));
        assert!(names.contains(&"realdir"));
        assert!(!names.contains(&"link.txt"));
        assert!(!names.contains(&"linkdir"));
    }

    // =========================================================================
    // FileDest Tests
    // =========================================================================

    /// Helper to create a SourceChunks stream from bytes using MemoryFileStore.
    async fn chunks_from_bytes(data: &[u8]) -> SourceChunks {
        use crate::file_store::{MemoryFileStore, MemoryFsEntry};

        let store = MemoryFileStore::builder()
            .add("_data", MemoryFsEntry::file(data))
            .build();

        store
            .get_source_chunks(Path::new("_data"))
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn test_dest_write_file_from_chunks() {
        let temp = create_test_dir();
        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        let data = b"Hello, World!";
        let chunks = chunks_from_bytes(data).await;
        dest.write_file_from_chunks(Path::new("hello.txt"), chunks, false)
            .await
            .unwrap();

        // Verify the file was written
        let contents = std::fs::read(temp.path().join("hello.txt")).unwrap();
        assert_eq!(&contents, data);
    }

    #[tokio::test]
    async fn test_dest_write_creates_parent_dirs() {
        let temp = create_test_dir();
        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        let chunks = chunks_from_bytes(b"nested").await;
        dest.write_file_from_chunks(Path::new("a/b/c.txt"), chunks, false)
            .await
            .unwrap();

        let contents = std::fs::read(temp.path().join("a/b/c.txt")).unwrap();
        assert_eq!(&contents, b"nested");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_dest_write_executable() {
        use std::os::unix::fs::PermissionsExt;

        let temp = create_test_dir();
        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        let chunks = chunks_from_bytes(b"#!/bin/bash").await;
        dest.write_file_from_chunks(Path::new("script.sh"), chunks, true)
            .await
            .unwrap();

        let metadata = std::fs::metadata(temp.path().join("script.sh")).unwrap();
        let mode = metadata.permissions().mode();
        assert!(mode & 0o111 != 0, "Expected executable bit set, got mode {:o}", mode);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_dest_write_not_executable_clears_bits() {
        use std::os::unix::fs::PermissionsExt;

        let temp = create_test_dir();

        // Create an executable file first
        let script_path = temp.path().join("script.sh");
        File::create(&script_path)
            .unwrap()
            .write_all(b"old")
            .unwrap();
        let mut perms = std::fs::metadata(&script_path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script_path, perms).unwrap();

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        // Write non-executable over it
        let chunks = chunks_from_bytes(b"new content").await;
        dest.write_file_from_chunks(Path::new("script.sh"), chunks, false)
            .await
            .unwrap();

        let metadata = std::fs::metadata(&script_path).unwrap();
        let mode = metadata.permissions().mode();
        assert!(mode & 0o111 == 0, "Expected no execute bits, got mode {:o}", mode);
    }

    #[tokio::test]
    async fn test_dest_rm_file() {
        let temp = create_test_dir();
        File::create(temp.path().join("file.txt"))
            .unwrap()
            .write_all(b"content")
            .unwrap();

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        dest.rm(Path::new("file.txt")).await.unwrap();
        assert!(!temp.path().join("file.txt").exists());
    }

    #[tokio::test]
    async fn test_dest_rm_directory() {
        let temp = create_test_dir();
        std::fs::create_dir_all(temp.path().join("dir/sub")).unwrap();
        File::create(temp.path().join("dir/sub/file.txt"))
            .unwrap()
            .write_all(b"content")
            .unwrap();

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        dest.rm(Path::new("dir")).await.unwrap();
        assert!(!temp.path().join("dir").exists());
    }

    #[tokio::test]
    async fn test_dest_rm_nonexistent_is_ok() {
        let temp = create_test_dir();
        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        dest.rm(Path::new("nonexistent")).await.unwrap();
    }

    #[tokio::test]
    async fn test_dest_mkdir() {
        let temp = create_test_dir();
        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        dest.mkdir(Path::new("a/b/c")).await.unwrap();
        assert!(temp.path().join("a/b/c").is_dir());
    }

    #[tokio::test]
    async fn test_dest_mkdir_existing_dir_is_ok() {
        let temp = create_test_dir();
        std::fs::create_dir(temp.path().join("existing")).unwrap();

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        dest.mkdir(Path::new("existing")).await.unwrap();
        assert!(temp.path().join("existing").is_dir());
    }

    #[tokio::test]
    async fn test_dest_mkdir_error_if_file_exists() {
        let temp = create_test_dir();
        File::create(temp.path().join("file.txt")).unwrap();

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        let result = dest.mkdir(Path::new("file.txt")).await;
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_dest_set_executable() {
        use std::os::unix::fs::PermissionsExt;

        let temp = create_test_dir();
        File::create(temp.path().join("script.sh"))
            .unwrap()
            .write_all(b"#!/bin/bash")
            .unwrap();

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        // Set executable
        dest.set_executable(Path::new("script.sh"), true)
            .await
            .unwrap();
        let mode = std::fs::metadata(temp.path().join("script.sh"))
            .unwrap()
            .permissions()
            .mode();
        assert!(mode & 0o111 != 0, "Expected executable, got mode {:o}", mode);

        // Clear executable
        dest.set_executable(Path::new("script.sh"), false)
            .await
            .unwrap();
        let mode = std::fs::metadata(temp.path().join("script.sh"))
            .unwrap()
            .permissions()
            .mode();
        assert!(mode & 0o111 == 0, "Expected not executable, got mode {:o}", mode);
    }

    #[tokio::test]
    async fn test_dest_set_executable_not_found() {
        let temp = create_test_dir();
        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        let result = dest.set_executable(Path::new("missing"), true).await;
        assert!(matches!(result, Err(Error::NotFound(_))));
    }

    #[tokio::test]
    async fn test_dest_set_executable_on_directory() {
        let temp = create_test_dir();
        std::fs::create_dir(temp.path().join("dir")).unwrap();

        let store = FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache());
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        let result = dest.set_executable(Path::new("dir"), true).await;
        assert!(matches!(result, Err(Error::NotAFile(_))));
    }
}
