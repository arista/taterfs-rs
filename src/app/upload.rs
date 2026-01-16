//! Upload functions for transferring files from a FileStore to a Repository.
//!
//! This module provides functions for uploading files and directories from a
//! FileStore to a Repository, handling chunking, caching, and completion tracking.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::StreamExt;

use crate::app::{DirectoryLeaf, DirectoryListBuilder, FileListBuilder};
use crate::caches::{DbId, FileStoreCache, FingerprintedFileInfo};
use crate::file_store::{FileStore, ScanEvent, ScanEvents};
use crate::repo::{Repo, RepoError};
use crate::repository::{ChunkFilePart, DirEntry, Directory, File, FileEntry, ObjectId};
use crate::util::{Complete, NoopComplete, WithComplete};

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

    // Get the source chunks for the file
    let chunks = source
        .get_source_chunks(path)
        .await?
        .ok_or_else(|| UploadError::NotFound(path.display().to_string()))?;

    // Get the chunk contents stream
    let mut chunk_contents = source.get_source_chunk_contents(chunks).await?;

    // Create the file list builder
    let mut builder = FileListBuilder::new(repo.clone());

    // Process each chunk
    while let Some(chunk_result) = chunk_contents.next().await {
        let chunk = chunk_result?;

        // Write the chunk to the repository
        let write_result = repo.write(&chunk.hash, chunk.bytes.clone()).await?;

        // Create a ChunkFilePart
        let chunk_part = ChunkFilePart {
            size: chunk.size,
            content: chunk.hash,
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
    let (result, _remaining) = upload_directory_from_scan_events(
        store,
        repo,
        cache,
        path_buf,
        cache_path_id,
        scan_events,
    )
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
        let event = match scan_events.next().await.transpose()? {
            Some(event) => event,
            None => {
                // End of stream - finish this directory (happens for root directory)
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

                let (file_hash, file_complete): (String, Arc<dyn Complete>) =
                    match (&cached_info, &file_entry.fingerprint) {
                        (Some(info), Some(fingerprint)) if &info.fingerprint == fingerprint => {
                            // Cache hit - use the cached object ID
                            (info.object_id.clone(), Arc::new(NoopComplete::new()))
                        }
                        _ => {
                            // Cache miss - upload the file
                            let file_path = if path.as_os_str().is_empty() {
                                PathBuf::from(&file_entry.name)
                            } else {
                                path.join(&file_entry.name)
                            };

                            let upload_result =
                                upload_file(store, repo.clone(), &file_path).await?;

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
                        }
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
