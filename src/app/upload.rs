//! Upload functions for transferring files from a FileStore to a Repository.
//!
//! This module provides functions for uploading files and directories from a
//! FileStore to a Repository, handling chunking, caching, and completion tracking.
//! It also provides [`StreamingFileUploader`] for uploading file content that is
//! produced incrementally (e.g., from a 3-way text merge).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::app::{DirectoryLeaf, DirectoryListBuilder, FileListBuilder};
use crate::caches::{DbId, FileStoreCache, FingerprintedFileInfo};
use crate::file_store::{FileStore, ScanEvent, ScanEvents, CHUNK_SIZES, next_chunk_size};
use crate::repo::{Repo, RepoError};
use crate::repository::{ChunkFilePart, DirEntry, Directory, File, FileEntry, ObjectId};
use crate::util::{Complete, ManagedBuffer, ManagedBuffers, NoopComplete, WithComplete};

// =============================================================================
// Upload Result Types
// =============================================================================

/// Result of uploading a file.
pub struct UploadFileResult {
    /// The uploaded File object.
    pub file: File,
    /// The hash/object ID of the file.
    pub hash: ObjectId,
}

/// Result of uploading a directory.
pub struct UploadDirectoryResult {
    /// The uploaded Directory object.
    pub directory: Directory,
    /// The hash/object ID of the directory.
    pub hash: ObjectId,
}

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during upload operations.
#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    /// File store error.
    #[error("file store error: {0}")]
    FileStore(#[from] crate::file_store::Error),

    /// Repository error.
    #[error("repository error: {0}")]
    Repo(#[from] RepoError),

    /// Cache error.
    #[error("cache error: {0}")]
    Cache(String),

    /// The file store does not support reading.
    #[error("file store does not support reading")]
    NoFileSource,

    /// Path not found.
    #[error("path not found: {0}")]
    NotFound(String),
}

/// Result type for upload operations.
pub type Result<T> = std::result::Result<T, UploadError>;

// =============================================================================
// Upload File
// =============================================================================

/// Upload a single file from a FileStore to a Repository.
///
/// Returns a `WithComplete` containing the uploaded File result (including hash)
/// and a completion handle that resolves when all background writes are finished.
pub async fn upload_file(
    store: &dyn FileStore,
    repo: Arc<Repo>,
    path: &Path,
) -> Result<WithComplete<UploadFileResult>> {
    let source = store.get_source().ok_or(UploadError::NoFileSource)?;

    // Get the chunk contents list directly from path
    let mut chunk_contents = source
        .get_source_chunks_with_content(path)
        .await?
        .ok_or_else(|| UploadError::NotFound(path.display().to_string()))?;

    // Create the file list builder
    let mut builder = FileListBuilder::new(repo.clone());

    // Process each chunk
    while let Some(chunk_result) = chunk_contents.next().await {
        let chunk = chunk_result?;
        let content = chunk.content().await?;

        // Write the chunk to the repository
        let write_result = repo.write(&content.hash, content.bytes.clone()).await?;

        // Create a ChunkFilePart
        let chunk_part = ChunkFilePart {
            size: content.size,
            content: content.hash.clone(),
        };

        // Add to the builder
        builder.add(chunk_part, write_result.complete).await?;
    }

    // Finish the builder
    let result = builder.finish().await?;

    let upload_result = UploadFileResult {
        file: result.object,
        hash: result.hash,
    };

    Ok(WithComplete::new(upload_result, result.complete))
}

// =============================================================================
// Upload Directory
// =============================================================================

/// Upload a directory from a FileStore to a Repository.
///
/// If `path` is `None`, uploads the entire FileStore.
///
/// Returns a `WithComplete` containing the uploaded Directory result (including hash)
/// and a completion handle that resolves when all background writes are finished.
pub async fn upload_directory(
    store: &dyn FileStore,
    repo: Arc<Repo>,
    cache: Arc<dyn FileStoreCache>,
    path: Option<&Path>,
) -> Result<WithComplete<UploadDirectoryResult>> {
    let source = store.get_source().ok_or(UploadError::NoFileSource)?;

    // Get the scan events
    let scan_events = source.scan(path).await?;

    // Determine the starting cache path ID
    // get_path_id returns None for empty/root paths
    let cache_path_id = match path {
        Some(p) => {
            let path_str = p.to_string_lossy();
            cache
                .get_path_id(&path_str)
                .await
                .map_err(|e| UploadError::Cache(e.to_string()))?
        }
        None => None,
    };

    // Start the recursive upload
    let path_buf = path.map(PathBuf::from).unwrap_or_default();
    let (result, _remaining) =
        upload_directory_from_scan_events(store, repo, cache, path_buf, cache_path_id, scan_events)
            .await?;

    Ok(result)
}

/// Result of uploading a directory from scan events.
///
/// Returns both the upload result and the remaining scan events stream
/// so that the caller can continue processing.
type UploadDirResult = (WithComplete<UploadDirectoryResult>, ScanEvents);

/// Upload a directory from scan events.
///
/// This is the recursive helper function that processes scan events and builds
/// the directory structure. Returns both the result and the remaining scan
/// events so that the parent can continue processing.
async fn upload_directory_from_scan_events(
    store: &dyn FileStore,
    repo: Arc<Repo>,
    cache: Arc<dyn FileStoreCache>,
    path: PathBuf,
    cache_path_id: Option<DbId>,
    mut scan_events: ScanEvents,
) -> Result<UploadDirResult> {
    // Create the directory list builder
    let mut builder = DirectoryListBuilder::new(repo.clone());

    loop {
        let event = match scan_events.next().await {
            Some(Ok(event)) => event,
            Some(Err(e)) => return Err(e.into()),
            None => {
                // End of list - finish this directory (happens for root directory)
                let result = builder.finish().await?;
                let upload_result = UploadDirectoryResult {
                    directory: result.object,
                    hash: result.hash,
                };
                let with_complete = WithComplete::new(upload_result, result.complete);
                return Ok((with_complete, scan_events));
            }
        };

        match event {
            ScanEvent::ExitDirectory => {
                // Finish this directory and return with remaining events
                let result = builder.finish().await?;
                let upload_result = UploadDirectoryResult {
                    directory: result.object,
                    hash: result.hash,
                };
                let with_complete = WithComplete::new(upload_result, result.complete);
                return Ok((with_complete, scan_events));
            }

            ScanEvent::EnterDirectory(dir_entry) => {
                // Calculate the new path
                let new_path = if path.as_os_str().is_empty() {
                    PathBuf::from(&dir_entry.name)
                } else {
                    path.join(&dir_entry.name)
                };

                // Get the cache path ID for the subdirectory
                let new_cache_path_id = cache
                    .get_path_entry_id(cache_path_id, &dir_entry.name)
                    .await
                    .map_err(|e| UploadError::Cache(e.to_string()))?;

                // Recursively upload the subdirectory
                let (sub_result, remaining_events) = Box::pin(upload_directory_from_scan_events(
                    store,
                    repo.clone(),
                    cache.clone(),
                    new_path,
                    Some(new_cache_path_id),
                    scan_events,
                ))
                .await?;

                // Continue with the remaining events
                scan_events = remaining_events;

                // Add the subdirectory to the builder
                let dir_entry_obj = DirEntry {
                    name: dir_entry.name,
                    directory: sub_result.result.hash.clone(),
                };
                builder
                    .add(DirectoryLeaf::Dir(dir_entry_obj), sub_result.complete)
                    .await?;
            }

            ScanEvent::File(file_entry) => {
                // Get the cache path ID for this file
                let file_cache_path_id = cache
                    .get_path_entry_id(cache_path_id, &file_entry.name)
                    .await
                    .map_err(|e| UploadError::Cache(e.to_string()))?;

                // Check if we have cached fingerprint info
                let cached_info = cache
                    .get_fingerprinted_file_info(file_cache_path_id)
                    .await
                    .map_err(|e| UploadError::Cache(e.to_string()))?;

                // Check if we can skip uploading:
                // 1. Fingerprint must match the cached fingerprint
                // 2. Repo cache must confirm the object is fully stored
                let can_skip = match (&cached_info, &file_entry.fingerprint) {
                    (Some(info), Some(fingerprint)) if &info.fingerprint == fingerprint => {
                        // Fingerprint matches - check if repo has it fully stored
                        repo.cache()
                            .object_fully_stored(&info.object_id)
                            .await
                            .unwrap_or(false)
                    }
                    _ => false,
                };

                let (file_hash, file_complete): (String, Arc<dyn Complete>) = if can_skip {
                    // Cache hit and fully stored - use the cached object ID
                    let info = cached_info.as_ref().unwrap();
                    (info.object_id.clone(), Arc::new(NoopComplete::new()))
                } else {
                    // Cache miss or not fully stored - upload the file
                    let file_path = if path.as_os_str().is_empty() {
                        PathBuf::from(&file_entry.name)
                    } else {
                        path.join(&file_entry.name)
                    };

                    let upload_result = upload_file(store, repo.clone(), &file_path).await?;

                    let file_hash = upload_result.result.hash.clone();
                    let complete = upload_result.complete;

                    // Update the cache if we have a fingerprint
                    if let Some(fingerprint) = &file_entry.fingerprint {
                        let info = FingerprintedFileInfo {
                            fingerprint: fingerprint.clone(),
                            object_id: file_hash.clone(),
                        };
                        let _ = cache
                            .set_fingerprinted_file_info(file_cache_path_id, &info)
                            .await;
                    }

                    (file_hash, complete)
                };

                // Create the FileEntry
                let entry = FileEntry {
                    name: file_entry.name,
                    size: file_entry.size,
                    executable: file_entry.executable,
                    file: file_hash,
                };

                builder
                    .add(DirectoryLeaf::File(entry), file_complete)
                    .await?;
            }
        }
    }
}

// =============================================================================
// StreamingFileUploader
// =============================================================================

/// Uploads file content that is produced incrementally as `ManagedBuffer`s.
///
/// This is used when file content is computed on the fly (e.g., output of a
/// 3-way text merge) rather than read from a `FileStore`. Buffers are added
/// via [`add`](Self::add), segmented into chunks using the same algorithm as
/// `FileStore::get_source_chunks_with_content`, written to the repo, and
/// tracked via a `FileListBuilder`. Call [`finish`](Self::finish) to flush
/// remaining data and get the final `UploadFileResult`.
pub struct StreamingFileUploader {
    repo: Arc<Repo>,
    builder: FileListBuilder,
    pending: Vec<ManagedBuffer>,
    pending_size: u64,
}

impl StreamingFileUploader {
    /// Create a new `StreamingFileUploader` that writes chunks to the given repo.
    pub fn new(repo: Arc<Repo>) -> Self {
        Self {
            builder: FileListBuilder::new(repo.clone()),
            repo,
            pending: Vec::new(),
            pending_size: 0,
        }
    }

    /// Add a buffer of content to the upload.
    ///
    /// The buffer is accumulated internally. When enough data has been
    /// accumulated to fill a full chunk (`CHUNK_SIZES[0]` = 4 MB), the chunk
    /// is hashed, written to the repo, and added to the internal file builder.
    pub async fn add(&mut self, buf: ManagedBuffer) -> Result<()> {
        self.pending_size += buf.size();
        self.pending.push(buf);

        // Flush full chunks while we have enough data
        while self.pending_size >= CHUNK_SIZES[0] {
            self.flush_chunk(CHUNK_SIZES[0]).await?;
        }

        Ok(())
    }

    /// Finish the upload, flushing any remaining data.
    ///
    /// Consumes `self` to prevent further `add` calls. Returns the uploaded
    /// file result with a completion handle.
    pub async fn finish(mut self) -> Result<WithComplete<UploadFileResult>> {
        // Drain remaining data using descending chunk sizes
        while self.pending_size > 0 {
            let chunk_size = next_chunk_size(self.pending_size);
            self.flush_chunk(chunk_size).await?;
        }

        let result = self.builder.finish().await?;

        let upload_result = UploadFileResult {
            file: result.object,
            hash: result.hash,
        };

        Ok(WithComplete::new(upload_result, result.complete))
    }

    /// Extract exactly `size` bytes from the front of the pending buffers,
    /// hash them, write to the repo, and add the chunk to the builder.
    async fn flush_chunk(&mut self, size: u64) -> Result<()> {
        let size_usize = size as usize;
        let mut assembled = Vec::with_capacity(size_usize);
        let mut remaining = size_usize;

        while remaining > 0 {
            let front = &self.pending[0];
            let front_len = front.buf().len();

            if front_len <= remaining {
                // Consume this entire buffer
                let buf = self.pending.remove(0);
                assembled.extend_from_slice(buf.as_ref());
                remaining -= front_len;
            } else {
                // Partially consume the front buffer
                assembled.extend_from_slice(&self.pending[0].as_ref()[..remaining]);
                // Split off the consumed bytes
                let _ = self.pending[0].buf_mut().split_to(remaining);
                remaining = 0;
            }
        }

        self.pending_size -= size;

        // Hash the assembled chunk
        let hash = {
            let mut hasher = Sha256::new();
            hasher.update(&assembled);
            format!("{:x}", hasher.finalize())
        };

        // Wrap in a ManagedBuffer (unmanaged — original buffers' capacity is
        // released as they are consumed above)
        let managed = ManagedBuffers::new()
            .get_buffer_with_data(assembled)
            .await;

        // Write to repo
        let write_result = self.repo.write(&hash, Arc::new(managed)).await?;

        // Add chunk to the file builder
        let chunk_part = ChunkFilePart {
            size,
            content: hash,
        };
        self.builder.add(chunk_part, write_result.complete).await?;

        Ok(())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;
    use crate::caches::NoopCache;
    use crate::repository::FilePart;
    use crate::util::ManagedBuffers;

    fn create_test_repo() -> Arc<Repo> {
        let backend = MemoryBackend::new();
        let cache = NoopCache;
        Arc::new(Repo::new(backend, cache))
    }

    async fn make_buffer(data: &[u8]) -> ManagedBuffer {
        ManagedBuffers::new()
            .get_buffer_with_data(data.to_vec())
            .await
    }

    /// Helper: collect all ChunkFilePart entries from a File object.
    fn collect_chunks(file: &File) -> Vec<&ChunkFilePart> {
        file.parts
            .iter()
            .filter_map(|p| match p {
                FilePart::Chunk(c) => Some(c),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn test_empty_file() {
        let repo = create_test_repo();
        let uploader = StreamingFileUploader::new(repo);

        let result = uploader.finish().await.unwrap();
        result.complete.complete().await.unwrap();

        // Empty file should have no chunks
        assert!(result.result.file.parts.is_empty());
    }

    #[tokio::test]
    async fn test_small_file() {
        let repo = create_test_repo();
        let mut uploader = StreamingFileUploader::new(repo);

        let data = b"hello world";
        uploader.add(make_buffer(data).await).await.unwrap();

        let result = uploader.finish().await.unwrap();
        result.complete.complete().await.unwrap();

        let chunks = collect_chunks(&result.result.file);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].size, data.len() as u64);
    }

    #[tokio::test]
    async fn test_exact_chunk_boundary() {
        let repo = create_test_repo();
        let mut uploader = StreamingFileUploader::new(repo);

        // Add exactly CHUNK_SIZES[0] (4MB) bytes
        let chunk_size = CHUNK_SIZES[0] as usize;
        let data = vec![0xABu8; chunk_size];
        uploader.add(make_buffer(&data).await).await.unwrap();

        let result = uploader.finish().await.unwrap();
        result.complete.complete().await.unwrap();

        let chunks = collect_chunks(&result.result.file);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].size, CHUNK_SIZES[0]);
    }

    #[tokio::test]
    async fn test_large_file_multiple_chunks() {
        let repo = create_test_repo();
        let mut uploader = StreamingFileUploader::new(repo);

        // Add 5MB + 100KB of data to get multiple chunks
        let big_chunk = CHUNK_SIZES[0] as usize; // 4MB
        let extra = 100_000usize;
        let total = big_chunk + extra;

        let data = vec![0x42u8; total];
        uploader.add(make_buffer(&data).await).await.unwrap();

        let result = uploader.finish().await.unwrap();
        result.complete.complete().await.unwrap();

        let chunks = collect_chunks(&result.result.file);

        // First chunk should be 4MB, remainder (100_000) gets chunked by next_chunk_size
        assert_eq!(chunks[0].size, CHUNK_SIZES[0]);

        // Remaining 100_000 should be a single chunk (< 16KB threshold doesn't apply,
        // next_chunk_size(100_000) = 65_536)
        let remaining = total as u64 - CHUNK_SIZES[0];
        let expected_second = next_chunk_size(remaining);
        assert_eq!(chunks[1].size, expected_second);

        // Total size across all chunks should equal total
        let total_chunked: u64 = chunks.iter().map(|c| c.size).sum();
        assert_eq!(total_chunked, total as u64);
    }

    #[tokio::test]
    async fn test_multiple_small_adds() {
        let repo = create_test_repo();
        let mut uploader = StreamingFileUploader::new(repo);

        // Add many small buffers that accumulate past the 4MB threshold
        let buf_size = 100_000usize;
        let num_bufs = 50; // 5MB total
        for _ in 0..num_bufs {
            let data = vec![0x01u8; buf_size];
            uploader.add(make_buffer(&data).await).await.unwrap();
        }

        let result = uploader.finish().await.unwrap();
        result.complete.complete().await.unwrap();

        let chunks = collect_chunks(&result.result.file);

        // Should have at least one 4MB chunk
        assert!(chunks[0].size == CHUNK_SIZES[0]);

        // Total size should match
        let total_chunked: u64 = chunks.iter().map(|c| c.size).sum();
        assert_eq!(total_chunked, (buf_size * num_bufs) as u64);
    }

    #[tokio::test]
    async fn test_chunk_hashes_are_valid_hex() {
        let repo = create_test_repo();
        let mut uploader = StreamingFileUploader::new(repo);

        uploader.add(make_buffer(b"test data").await).await.unwrap();

        let result = uploader.finish().await.unwrap();
        result.complete.complete().await.unwrap();

        let chunks = collect_chunks(&result.result.file);
        for chunk in &chunks {
            // SHA-256 hex is 64 characters
            assert_eq!(chunk.content.len(), 64);
            assert!(chunk.content.chars().all(|c| c.is_ascii_hexdigit()));
        }
    }

    #[tokio::test]
    async fn test_deterministic_hash() {
        // Uploading the same content twice should produce the same file hash
        let data = b"deterministic content";

        let repo1 = create_test_repo();
        let mut uploader1 = StreamingFileUploader::new(repo1);
        uploader1.add(make_buffer(data).await).await.unwrap();
        let result1 = uploader1.finish().await.unwrap();
        result1.complete.complete().await.unwrap();

        let repo2 = create_test_repo();
        let mut uploader2 = StreamingFileUploader::new(repo2);
        uploader2.add(make_buffer(data).await).await.unwrap();
        let result2 = uploader2.finish().await.unwrap();
        result2.complete.complete().await.unwrap();

        assert_eq!(result1.result.hash, result2.result.hash);
    }
}
