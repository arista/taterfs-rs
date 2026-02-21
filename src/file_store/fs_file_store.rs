//! Filesystem-based FileStore implementation.
//!
//! FsFileStore provides access to a local filesystem directory as a FileStore.

use super::chunk_sizes::next_chunk_size;
use super::scan_ignore_helper::{ScanDirEntry, ScanDirectoryEvent, ScanIgnoreHelper};
use crate::caches::{FileStoreCache, FingerprintedFileInfo, LocalChunk, LocalChunksCache};
use crate::file_store::{
    DirEntry, DirectoryEntry, DirectoryList, DirectoryListSource, Error, FileDestStage, FileEntry,
    FileSource, FileStore, Result, ScanEvent, ScanEvents, SourceChunk, SourceChunkContent,
    SourceChunkList, SourceChunkWithContent, SourceChunkWithContentList, SourceChunks,
    SourceChunksWithContent, StoreSyncState, SyncState, VecScanEventList,
};
use crate::repo::{BoxedFileChunksWithContent, FileChunkWithContent};
use crate::repository::ObjectId;
use crate::util::{Complete, ManagedBuffer, ManagedBuffers, NotifyComplete, WithComplete};
use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;

/// A FileStore backed by the local filesystem.
#[derive(Clone)]
pub struct FsFileStore {
    /// Root path on the filesystem.
    root: PathBuf,
    /// Buffer manager for chunk allocation.
    managed_buffers: ManagedBuffers,
    /// Cache for this file store.
    cache: Arc<dyn FileStoreCache>,
    /// Local chunks cache for tracking chunk locations on disk.
    local_chunks_cache: Option<Arc<dyn LocalChunksCache>>,
}

impl FsFileStore {
    /// Create a new FsFileStore rooted at the given path.
    pub fn new(
        root: impl AsRef<Path>,
        managed_buffers: ManagedBuffers,
        cache: Arc<dyn FileStoreCache>,
        local_chunks_cache: Option<Arc<dyn LocalChunksCache>>,
    ) -> Self {
        let root_path = root.as_ref().to_path_buf();
        Self {
            root: root_path,
            managed_buffers,
            cache,
            local_chunks_cache,
        }
    }

    /// Convert a path to an absolute filesystem path.
    ///
    /// The path is always interpreted relative to the file store's root,
    /// even if it appears to be absolute (starts with `/`).
    fn to_absolute(&self, path: &Path) -> PathBuf {
        // Strip leading "/" or root component to ensure path is treated as relative
        let relative = path.strip_prefix("/").unwrap_or(path);
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

    /// Update the cache after writing a file or changing its executable bit.
    ///
    /// Uses `get_entry` to obtain the file's fingerprint, ensuring consistency
    /// with how fingerprints are computed elsewhere.
    async fn update_cache_after_write(
        &self,
        path: &Path,
        object_id: &ObjectId,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get the file entry to obtain its fingerprint
        let entry = FileSource::get_entry(self, path).await?;

        if let Some(DirectoryEntry::File(file_entry)) = entry
            && let Some(fingerprint) = file_entry.fingerprint
        {
            // Get the path ID from the cache
            let path_str = path.to_string_lossy();
            if let Some(path_id) = self.cache.get_path_id(&path_str).await? {
                let info = FingerprintedFileInfo {
                    fingerprint,
                    object_id: object_id.clone(),
                };
                self.cache
                    .set_fingerprinted_file_info(path_id, &info)
                    .await?;
            }
        }

        Ok(())
    }
}

// =============================================================================
// Chunk List Implementation
// =============================================================================

/// SourceChunkList implementation for FsFileStore.
///
/// Computes chunks iteratively as next() is called.
pub struct FsSourceChunkList {
    file_size: u64,
    current_offset: u64,
}

impl FsSourceChunkList {
    fn new(file_size: u64) -> Self {
        Self {
            file_size,
            current_offset: 0,
        }
    }
}

#[async_trait]
impl SourceChunkList for FsSourceChunkList {
    async fn next(&mut self) -> Option<Result<SourceChunk>> {
        if self.current_offset >= self.file_size {
            return None;
        }
        let remaining = self.file_size - self.current_offset;
        let size = next_chunk_size(remaining);
        let chunk = SourceChunk {
            offset: self.current_offset,
            size,
        };
        self.current_offset += size;
        Some(Ok(chunk))
    }
}

// =============================================================================
// Chunk Content Implementation
// =============================================================================

/// Sequential implementation of SourceChunkWithContentList for FsFileStore.
///
/// Opens the file once on first call to next() and reuses the handle for
/// subsequent chunks. Computes chunks iteratively as next() is called.
struct FsSourceChunkWithContentList {
    path: PathBuf,
    file_size: u64,
    current_offset: u64,
    managed_buffers: ManagedBuffers,
    /// Cached file handle, opened lazily on first next() call.
    file: Option<fs::File>,
    /// Local chunks cache for registering chunk locations.
    local_chunks_cache: Option<Arc<dyn LocalChunksCache>>,
    /// Cached path ID for the local chunks cache.
    path_id: Option<u64>,
}

impl FsSourceChunkWithContentList {
    fn new(
        path: PathBuf,
        file_size: u64,
        managed_buffers: ManagedBuffers,
        local_chunks_cache: Option<Arc<dyn LocalChunksCache>>,
    ) -> Self {
        Self {
            path,
            file_size,
            current_offset: 0,
            managed_buffers,
            file: None,
            local_chunks_cache,
            path_id: None,
        }
    }
}

#[async_trait]
impl SourceChunkWithContentList for FsSourceChunkWithContentList {
    async fn next(&mut self) -> Option<Result<SourceChunkWithContent>> {
        if self.current_offset >= self.file_size {
            return None;
        }

        let remaining = self.file_size - self.current_offset;
        let size = next_chunk_size(remaining);
        let offset = self.current_offset;
        self.current_offset += size;

        // 1. Acquire capacity before reading (waits if capacity limit is reached)
        let acquired = self.managed_buffers.acquire(size).await;

        // 2. Open file if not already open
        if self.file.is_none() {
            match fs::File::open(&self.path).await {
                Ok(f) => self.file = Some(f),
                Err(e) => return Some(Err(e.into())),
            }
        }
        let file = self.file.as_mut().unwrap();

        // 3. Seek and read
        if let Err(e) = file.seek(std::io::SeekFrom::Start(offset)).await {
            return Some(Err(e.into()));
        }

        let mut buffer = vec![0u8; size as usize];
        if let Err(e) = file.read_exact(&mut buffer).await {
            return Some(Err(e.into()));
        }

        // 4. Compute hash
        let hash = {
            let mut hasher = Sha256::new();
            hasher.update(&buffer);
            format!("{:x}", hasher.finalize())
        };

        // 5. Register chunk in local chunks cache (if available)
        if let Some(ref cache) = self.local_chunks_cache {
            // Get path_id lazily on first chunk
            if self.path_id.is_none()
                && let Some(path_str) = self.path.to_str()
                && let Ok(Some(id)) = cache.get_path_id(path_str).await
            {
                self.path_id = Some(id);
            }
            // Register the chunk
            if let Some(path_id) = self.path_id {
                let local_chunk = LocalChunk {
                    chunk_id: hash.clone(),
                    offset,
                    length: size,
                };
                // Silently ignore errors during cache registration
                let _ = cache.set_local_chunk(path_id, &local_chunk).await;
            }
        }

        // 6. Create buffer using the acquired capacity (doesn't wait)
        let managed_buffer = self
            .managed_buffers
            .create_buffer_with_acquired(buffer, acquired);

        let content = SourceChunkContent {
            offset,
            size,
            bytes: Arc::new(managed_buffer),
            hash,
        };

        Some(Ok(SourceChunkWithContent::new_immediate(
            offset, size, content,
        )))
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
                let path_str = path
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default();
                return Err(Error::NotADirectory(path_str));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let path_str = path
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default();
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

        Ok(Box::new(VecScanEventList::new(events)))
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

        Ok(Some(Box::new(FsSourceChunkList::new(file_size))))
    }

    async fn get_source_chunks_with_content(
        &self,
        path: &Path,
    ) -> Result<Option<SourceChunksWithContent>> {
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

        Ok(Some(Box::new(FsSourceChunkWithContentList::new(
            absolute,
            file_size,
            self.managed_buffers.clone(),
            self.local_chunks_cache.clone(),
        ))))
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

    fn get_sync_state_manager(&self) -> Option<&dyn StoreSyncState> {
        Some(self)
    }

    fn get_root_path(&self) -> Option<&std::path::Path> {
        Some(&self.root)
    }
}

// =============================================================================
// StoreSyncState Implementation
// =============================================================================

#[async_trait]
impl StoreSyncState for FsFileStore {
    async fn get_sync_state(&self) -> Result<Option<SyncState>> {
        let path = self.root.join(".tfs/sync_state.json");
        read_sync_state_file(&path).await
    }

    async fn get_next_sync_state(&self) -> Result<Option<SyncState>> {
        let path = self.root.join(".tfs/next_sync_state.json");
        read_sync_state_file(&path).await
    }

    async fn set_next_sync_state(&self, state: Option<&SyncState>) -> Result<()> {
        let path = self.root.join(".tfs/next_sync_state.json");

        match state {
            Some(s) => {
                // Create the .tfs directory if it doesn't exist
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).await?;
                }
                // Serialize with pretty printing
                let json = serde_json::to_string_pretty(s)
                    .map_err(|e| Error::Other(format!("Failed to serialize sync state: {}", e)))?;
                fs::write(&path, json).await?;
            }
            None => {
                // Remove the file if it exists
                match fs::remove_file(&path).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }

        Ok(())
    }

    async fn commit_next_sync_state(&self) -> Result<()> {
        let next_path = self.root.join(".tfs/next_sync_state.json");
        let current_path = self.root.join(".tfs/sync_state.json");

        // Check if next state exists
        match fs::metadata(&next_path).await {
            Ok(_) => {
                // Atomically move next to current
                fs::rename(&next_path, &current_path).await?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // No next state to commit - this is not an error
            }
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }
}

/// Helper function to read a sync state file.
async fn read_sync_state_file(path: &Path) -> Result<Option<SyncState>> {
    match fs::read_to_string(path).await {
        Ok(contents) => {
            let state: SyncState = serde_json::from_str(&contents)
                .map_err(|e| Error::Other(format!("Failed to parse sync state: {}", e)))?;
            Ok(Some(state))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
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
        mut chunks: BoxedFileChunksWithContent,
        executable: bool,
        object_id: &ObjectId,
    ) -> Result<WithComplete<()>> {
        let absolute = self.to_absolute(path);

        // Create parent directories
        if let Some(parent) = absolute.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Write to a temp file in the same directory for atomic rename.
        // Format: .download.{filename}-{temp suffix}
        let file_name = absolute
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let temp_name = format!(".download.{}-{}", file_name, std::process::id());
        let temp_path = absolute.parent().unwrap_or(&self.root).join(&temp_name);

        // Create the temp file
        let file = fs::File::create(&temp_path).await?;

        // Set executable bit on temp file if needed
        #[cfg(unix)]
        if executable {
            use std::os::unix::fs::PermissionsExt;
            let metadata = file.metadata().await?;
            let mut perms = metadata.permissions();
            let mode = perms.mode();
            // Add execute bits where read bits are set
            perms.set_mode(mode | ((mode & 0o444) >> 2));
            fs::set_permissions(&temp_path, perms).await?;
        }

        // Create completion notifier
        let complete = Arc::new(NotifyComplete::new());
        let complete_for_task = Arc::clone(&complete);

        // Create a channel for passing chunks to the writer task
        // Use unbounded since we want to queue all chunks as they become available
        let (tx, mut rx) = mpsc::unbounded_channel::<Option<FileChunkWithContent>>();

        let temp_path_for_task = temp_path.clone();
        let absolute_for_task = absolute.clone();

        // Clone self and data needed for cache update in background task
        let store = self.clone();
        let path_for_task = path.to_path_buf();
        let object_id_for_task = object_id.clone();
        let local_chunks_cache_for_task = self.local_chunks_cache.clone();

        // Spawn the background writer task
        tokio::spawn(async move {
            // Build cache context if local chunks cache is available
            let cache_ctx = if let Some(cache) = local_chunks_cache_for_task
                && let Some(abs_str) = absolute_for_task.to_str()
                && let Ok(Some(path_id)) = cache.get_path_id(abs_str).await
            {
                Some(ChunkCacheContext { cache, path_id })
            } else {
                None
            };

            let result = write_chunks_to_file(
                file,
                &mut rx,
                &temp_path_for_task,
                &absolute_for_task,
                executable,
                cache_ctx,
            )
            .await;

            match result {
                Ok(()) => {
                    // Update the cache with the new file's fingerprint using get_entry
                    let _ = store
                        .update_cache_after_write(&path_for_task, &object_id_for_task)
                        .await;

                    complete_for_task.notify_complete();
                }
                Err(e) => complete_for_task.notify_error(e.to_string()),
            }
        });

        // Loop through chunks and send them to the writer task
        loop {
            match chunks.next().await {
                Ok(Some(chunk)) => {
                    // Send the chunk to the writer task
                    if tx.send(Some(chunk)).is_err() {
                        // Writer task has exited (likely due to error)
                        break;
                    }
                }
                Ok(None) => {
                    // No more chunks - signal end to writer task
                    let _ = tx.send(None);
                    break;
                }
                Err(e) => {
                    // Error getting next chunk - clean up temp file
                    let _ = fs::remove_file(&temp_path).await;
                    return Err(e.into());
                }
            }
        }

        Ok(WithComplete::new((), complete as Arc<dyn Complete>))
    }

    async fn create_stage(&self) -> Result<Option<Box<dyn FileDestStage>>> {
        let stage = FsFileDestStage::new(&self.root).await?;
        Ok(Some(Box::new(stage)))
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

        // Invalidate local chunks cache entries for the removed path
        if let Some(ref cache) = self.local_chunks_cache
            && let Some(abs_str) = absolute.to_str()
            && let Ok(Some(path_id)) = cache.get_path_id(abs_str).await
        {
            // Silently ignore errors during cache invalidation
            let _ = cache.invalidate_local_chunks(path_id).await;
        }

        Ok(())
    }

    async fn mkdir(&self, path: &Path) -> Result<()> {
        let absolute = self.to_absolute(path);

        // Check if a file exists at the path
        match fs::metadata(&absolute).await {
            Ok(m) if m.is_file() => {
                return Err(Error::NotADirectory(path.to_string_lossy().into_owned()));
            }
            Ok(_) => return Ok(()), // Already a directory
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }

        fs::create_dir_all(&absolute).await?;
        Ok(())
    }

    async fn set_executable(
        &self,
        path: &Path,
        executable: bool,
        object_id: &ObjectId,
    ) -> Result<()> {
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

        // Update the cache with the new fingerprint (executable bit changed)
        let _ = self.update_cache_after_write(path, object_id).await;

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

/// Context for registering chunks in the local cache during writes.
struct ChunkCacheContext {
    cache: Arc<dyn LocalChunksCache>,
    path_id: u64,
}

/// Helper function for writing chunks to a file from a channel.
///
/// Receives chunks from the channel, writes them to the file in order,
/// then moves the file to its final location.
///
/// If `cache_ctx` is provided, registers each chunk in the local cache as it's written.
async fn write_chunks_to_file(
    mut file: fs::File,
    rx: &mut mpsc::UnboundedReceiver<Option<FileChunkWithContent>>,
    temp_path: &Path,
    final_path: &Path,
    executable: bool,
    cache_ctx: Option<ChunkCacheContext>,
) -> Result<()> {
    let mut offset: u64 = 0;

    // Process chunks from the channel
    while let Some(chunk_option) = rx.recv().await {
        match chunk_option {
            Some(chunk) => {
                // Get chunk metadata before awaiting content
                let chunk_id = chunk.chunk.content.clone();
                let chunk_size = chunk.chunk.size;

                // Wait for the chunk content and write it
                let content = chunk
                    .content()
                    .await
                    .map_err(|e| Error::Other(e.to_string()))?;
                file.write_all(&content[..]).await.map_err(Error::Io)?;

                // Register chunk in cache immediately (if cache context provided)
                if let Some(ref ctx) = cache_ctx {
                    let local_chunk = LocalChunk {
                        chunk_id,
                        offset,
                        length: chunk_size,
                    };
                    // Silently ignore errors during cache registration
                    let _ = ctx.cache.set_local_chunk(ctx.path_id, &local_chunk).await;
                }

                offset += chunk_size;
            }
            None => {
                // End of chunks - finalize the file
                break;
            }
        }
    }

    // Flush and close the file
    file.flush().await?;
    drop(file);

    // Atomic rename to final path
    fs::rename(temp_path, final_path).await?;

    // If not executable on unix, ensure execute bits are cleared
    // (in case the destination previously had them)
    #[cfg(unix)]
    if !executable {
        use std::os::unix::fs::PermissionsExt;
        let metadata = fs::metadata(final_path).await?;
        let mut perms = metadata.permissions();
        let mode = perms.mode();
        if mode & 0o111 != 0 {
            perms.set_mode(mode & !0o111);
            fs::set_permissions(final_path, perms).await?;
        }
    }

    Ok(())
}

// =============================================================================
// FsFileDestStage Implementation
// =============================================================================

/// A staging area for temporarily caching downloaded file chunks on the filesystem.
///
/// Chunks are stored in a directory structure: `chunks/{id[0..2]}/{id[2..4]}/{id[4..6]}/{id}`
pub struct FsFileDestStage {
    /// The root directory of the stage (e.g., `{file store root}/.tfs/tmp/stage-{temp}`)
    stage_root: PathBuf,
}

impl FsFileDestStage {
    /// Create a new staging area.
    ///
    /// Creates the stage directory at `{root}/.tfs/tmp/stage-{temp}/`
    pub async fn new(file_store_root: &Path) -> Result<Self> {
        let stage_name = format!("stage-{}", std::process::id());
        let stage_root = file_store_root.join(".tfs/tmp").join(stage_name);
        fs::create_dir_all(&stage_root).await?;
        Ok(Self { stage_root })
    }

    /// Get the path for a chunk in the stage.
    ///
    /// Format: `chunks/{id[0..2]}/{id[2..4]}/{id[4..6]}/{id}`
    fn chunk_path(&self, id: &ObjectId) -> PathBuf {
        let id_str = id.to_string();
        self.stage_root
            .join("chunks")
            .join(&id_str[0..2])
            .join(&id_str[2..4])
            .join(&id_str[4..6])
            .join(&id_str)
    }

    /// Get the temporary download path for a chunk.
    ///
    /// Format: `chunks/{id[0..2]}/{id[2..4]}/{id[4..6]}/.download.{id}-{temp}`
    fn chunk_temp_path(&self, id: &ObjectId) -> PathBuf {
        let id_str = id.to_string();
        let temp_name = format!(".download.{}-{}", id_str, std::process::id());
        self.stage_root
            .join("chunks")
            .join(&id_str[0..2])
            .join(&id_str[2..4])
            .join(&id_str[4..6])
            .join(temp_name)
    }
}

#[async_trait]
impl FileDestStage for FsFileDestStage {
    async fn write_chunk(&self, id: &ObjectId, chunk: Arc<ManagedBuffer>) -> Result<()> {
        let final_path = self.chunk_path(id);
        let temp_path = self.chunk_temp_path(id);

        // Create parent directories
        if let Some(parent) = temp_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Write to temp file
        fs::write(&temp_path, &chunk[..]).await?;

        // Atomic rename to final path
        fs::rename(&temp_path, &final_path).await?;

        Ok(())
    }

    async fn read_chunk(&self, id: &ObjectId) -> Result<Option<Arc<ManagedBuffer>>> {
        let path = self.chunk_path(id);

        match fs::read(&path).await {
            Ok(data) => {
                let buffer = ManagedBuffers::new().get_buffer_with_data(data).await;
                Ok(Some(Arc::new(buffer)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn has_chunk(&self, id: &ObjectId) -> Result<bool> {
        let path = self.chunk_path(id);
        match fs::metadata(&path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn cleanup(&self) -> Result<()> {
        // Remove the entire stage directory
        match fs::remove_dir_all(&self.stage_root).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::caches::NoopFileStoreCache;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    fn noop_cache() -> Arc<dyn FileStoreCache> {
        Arc::new(NoopFileStoreCache)
    }

    fn create_store(temp: &TempDir) -> FsFileStore {
        FsFileStore::new(temp.path(), ManagedBuffers::new(), noop_cache(), None)
    }

    /// Helper to collect all events from a ScanEventList.
    async fn collect_scan_events(mut events: ScanEvents) -> Vec<ScanEvent> {
        let mut result = Vec::new();
        while let Some(event) = events.next().await {
            result.push(event.unwrap());
        }
        result
    }

    #[tokio::test]
    async fn test_empty_directory() {
        let temp = create_test_dir();
        let store = create_store(&temp);

        let events = collect_scan_events(store.scan(None).await.unwrap()).await;

        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn test_single_file() {
        let temp = create_test_dir();
        File::create(temp.path().join("hello.txt"))
            .unwrap()
            .write_all(b"Hello, World!")
            .unwrap();

        let store = create_store(&temp);

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
        let events = collect_scan_events(store.scan(None).await.unwrap()).await;
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

        let store = create_store(&temp);

        let events = collect_scan_events(store.scan(None).await.unwrap()).await;

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

        let store = create_store(&temp);

        // Get chunks with content directly from path
        let mut chunks_with_content = store
            .get_source_chunks_with_content(Path::new("small.txt"))
            .await
            .unwrap()
            .unwrap();

        let chunk = chunks_with_content.next().await.unwrap().unwrap();
        assert_eq!(chunk.offset, 0);
        assert_eq!(chunk.size, 4);

        let content = chunk.content().await.unwrap();
        assert_eq!(&content.bytes[..], b"tiny");
        assert_eq!(content.hash.len(), 64); // SHA-256 hex
    }

    #[tokio::test]
    async fn test_get_source_chunks_not_found() {
        let temp = create_test_dir();
        let store = create_store(&temp);

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

        let store = create_store(&temp);

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

        let store = create_store(&temp);

        let events = collect_scan_events(store.scan(None).await.unwrap()).await;

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

        let store = create_store(&temp);

        let events = collect_scan_events(store.scan(None).await.unwrap()).await;

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

        let store = create_store(&temp);

        let events = collect_scan_events(store.scan(None).await.unwrap()).await;

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
        let store = create_store(&temp);

        let result = store.get_file(Path::new("missing.txt")).await;
        assert!(matches!(result, Err(Error::NotFound(_))));
    }

    #[tokio::test]
    async fn test_get_entry_root() {
        let temp = create_test_dir();
        let store = create_store(&temp);

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

        let store = create_store(&temp);

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

        let store = create_store(&temp);

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
        std::os::unix::fs::symlink(temp.path().join("real.txt"), temp.path().join("link.txt"))
            .unwrap();

        // Create a directory and a symlink to it
        std::fs::create_dir(temp.path().join("realdir")).unwrap();
        std::os::unix::fs::symlink(temp.path().join("realdir"), temp.path().join("linkdir"))
            .unwrap();

        let store = create_store(&temp);

        let events = collect_scan_events(store.scan(None).await.unwrap()).await;

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

    // Note: write_file_from_chunks tests require a Repo to create FileChunkWithContentList
    // and should be implemented as integration tests.

    #[tokio::test]
    async fn test_dest_rm_file() {
        let temp = create_test_dir();
        File::create(temp.path().join("file.txt"))
            .unwrap()
            .write_all(b"content")
            .unwrap();

        let store = create_store(&temp);
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

        let store = create_store(&temp);
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        dest.rm(Path::new("dir")).await.unwrap();
        assert!(!temp.path().join("dir").exists());
    }

    #[tokio::test]
    async fn test_dest_rm_nonexistent_is_ok() {
        let temp = create_test_dir();
        let store = create_store(&temp);
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        dest.rm(Path::new("nonexistent")).await.unwrap();
    }

    #[tokio::test]
    async fn test_dest_mkdir() {
        let temp = create_test_dir();
        let store = create_store(&temp);
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        dest.mkdir(Path::new("a/b/c")).await.unwrap();
        assert!(temp.path().join("a/b/c").is_dir());
    }

    #[tokio::test]
    async fn test_dest_mkdir_existing_dir_is_ok() {
        let temp = create_test_dir();
        std::fs::create_dir(temp.path().join("existing")).unwrap();

        let store = create_store(&temp);
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        dest.mkdir(Path::new("existing")).await.unwrap();
        assert!(temp.path().join("existing").is_dir());
    }

    #[tokio::test]
    async fn test_dest_mkdir_error_if_file_exists() {
        let temp = create_test_dir();
        File::create(temp.path().join("file.txt")).unwrap();

        let store = create_store(&temp);
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

        let store = create_store(&temp);
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        // Set executable
        let test_object_id = "test_object_id".to_string();
        dest.set_executable(Path::new("script.sh"), true, &test_object_id)
            .await
            .unwrap();
        let mode = std::fs::metadata(temp.path().join("script.sh"))
            .unwrap()
            .permissions()
            .mode();
        assert!(
            mode & 0o111 != 0,
            "Expected executable, got mode {:o}",
            mode
        );

        // Clear executable
        dest.set_executable(Path::new("script.sh"), false, &test_object_id)
            .await
            .unwrap();
        let mode = std::fs::metadata(temp.path().join("script.sh"))
            .unwrap()
            .permissions()
            .mode();
        assert!(
            mode & 0o111 == 0,
            "Expected not executable, got mode {:o}",
            mode
        );
    }

    #[tokio::test]
    async fn test_dest_set_executable_not_found() {
        let temp = create_test_dir();
        let store = create_store(&temp);
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        let test_object_id = "test_object_id".to_string();
        let result = dest
            .set_executable(Path::new("missing"), true, &test_object_id)
            .await;
        assert!(matches!(result, Err(Error::NotFound(_))));
    }

    #[tokio::test]
    async fn test_dest_set_executable_on_directory() {
        let temp = create_test_dir();
        std::fs::create_dir(temp.path().join("dir")).unwrap();

        let store = create_store(&temp);
        let dest: &dyn crate::file_store::FileDest = store.get_dest().unwrap();

        let test_object_id = "test_object_id".to_string();
        let result = dest
            .set_executable(Path::new("dir"), true, &test_object_id)
            .await;
        assert!(matches!(result, Err(Error::NotAFile(_))));
    }

    // =========================================================================
    // StoreSyncState Tests
    // =========================================================================

    fn create_test_sync_state() -> SyncState {
        SyncState {
            created_at: "2025-01-15T10:30:00Z".to_string(),
            repository_url: "https://example.com/repo".to_string(),
            repository_directory: "data".to_string(),
            branch_name: "main".to_string(),
            base_commit: "abc123def456".to_string(),
        }
    }

    #[tokio::test]
    async fn test_sync_state_initially_none() {
        let temp = create_test_dir();
        let store = create_store(&temp);
        let sync_manager: &dyn StoreSyncState = store.get_sync_state_manager().unwrap();

        let state = sync_manager.get_sync_state().await.unwrap();
        assert!(state.is_none());

        let next_state = sync_manager.get_next_sync_state().await.unwrap();
        assert!(next_state.is_none());
    }

    #[tokio::test]
    async fn test_set_and_get_next_sync_state() {
        let temp = create_test_dir();
        let store = create_store(&temp);
        let sync_manager: &dyn StoreSyncState = store.get_sync_state_manager().unwrap();

        let state = create_test_sync_state();

        // Set next state
        sync_manager
            .set_next_sync_state(Some(&state))
            .await
            .unwrap();

        // Verify it's persisted
        let retrieved = sync_manager.get_next_sync_state().await.unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.repository_url, state.repository_url);
        assert_eq!(retrieved.branch_name, state.branch_name);
        assert_eq!(retrieved.base_commit, state.base_commit);

        // Current state should still be none
        assert!(sync_manager.get_sync_state().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_commit_next_sync_state() {
        let temp = create_test_dir();
        let store = create_store(&temp);
        let sync_manager: &dyn StoreSyncState = store.get_sync_state_manager().unwrap();

        let state = create_test_sync_state();

        // Set and commit next state
        sync_manager
            .set_next_sync_state(Some(&state))
            .await
            .unwrap();
        sync_manager.commit_next_sync_state().await.unwrap();

        // Current state should now be set
        let current = sync_manager.get_sync_state().await.unwrap();
        assert!(current.is_some());
        let current = current.unwrap();
        assert_eq!(current.repository_url, state.repository_url);
        assert_eq!(current.base_commit, state.base_commit);

        // Next state should be cleared
        assert!(sync_manager.get_next_sync_state().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_clear_next_sync_state() {
        let temp = create_test_dir();
        let store = create_store(&temp);
        let sync_manager: &dyn StoreSyncState = store.get_sync_state_manager().unwrap();

        let state = create_test_sync_state();

        // Set then clear next state
        sync_manager
            .set_next_sync_state(Some(&state))
            .await
            .unwrap();
        sync_manager.set_next_sync_state(None).await.unwrap();

        // Next state should be none
        assert!(sync_manager.get_next_sync_state().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_commit_when_no_next_state_is_ok() {
        let temp = create_test_dir();
        let store = create_store(&temp);
        let sync_manager: &dyn StoreSyncState = store.get_sync_state_manager().unwrap();

        // Should not error when there's no next state to commit
        sync_manager.commit_next_sync_state().await.unwrap();
    }

    #[tokio::test]
    async fn test_sync_state_files_are_pretty_printed() {
        let temp = create_test_dir();
        let store = create_store(&temp);
        let sync_manager: &dyn StoreSyncState = store.get_sync_state_manager().unwrap();

        let state = create_test_sync_state();
        sync_manager
            .set_next_sync_state(Some(&state))
            .await
            .unwrap();

        // Read the file directly and verify it's pretty-printed
        let path = temp.path().join(".tfs/next_sync_state.json");
        let contents = std::fs::read_to_string(&path).unwrap();

        // Pretty-printed JSON should have newlines
        assert!(contents.contains('\n'), "JSON should be pretty-printed");
        // And the content should be valid
        assert!(contents.contains("repository_url"));
        assert!(contents.contains("base_commit"));
    }

    #[tokio::test]
    async fn test_sync_state_persists_across_store_instances() {
        let temp = create_test_dir();
        let state = create_test_sync_state();

        // First store instance - set next state and commit
        {
            let store = create_store(&temp);
            let sync_manager: &dyn StoreSyncState = store.get_sync_state_manager().unwrap();
            sync_manager
                .set_next_sync_state(Some(&state))
                .await
                .unwrap();
            sync_manager.commit_next_sync_state().await.unwrap();
        }

        // Second store instance - should see the committed state
        {
            let store = create_store(&temp);
            let sync_manager: &dyn StoreSyncState = store.get_sync_state_manager().unwrap();
            let current = sync_manager.get_sync_state().await.unwrap();
            assert!(current.is_some());
            assert_eq!(current.unwrap().base_commit, state.base_commit);
        }
    }
}
