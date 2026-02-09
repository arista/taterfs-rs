//! Core download implementation for synchronizing a repository directory to a file store.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use crate::caches::{DbId, FileStoreCache};
use crate::file_store::{self, DirectoryList, FileDestStage, FileStore};
use crate::repo::{self, FileChunkWithContent, FileChunksWithContent, Repo, RepoError};
use crate::repository::{File, FilePart, ObjectId};
use crate::util::{Complete, Completes, ManagedBuffers, WithComplete};

// Type aliases to disambiguate between repo and file_store DirectoryEntry types.
type RepoDirectoryEntry = repo::DirectoryEntry;
type StoreDirectoryEntry = file_store::DirectoryEntry;

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during download operations.
#[derive(Debug, thiserror::Error)]
pub enum DownloadError {
    /// File store error.
    #[error("file store error: {0}")]
    FileStore(#[from] file_store::Error),

    /// Repository error.
    #[error("repository error: {0}")]
    Repo(#[from] RepoError),

    /// Cache error.
    #[error("cache error: {0}")]
    Cache(String),

    /// The file store does not support writing.
    #[error("file store does not support writing")]
    NoFileDest,
}

/// Result type for download operations.
pub type Result<T> = std::result::Result<T, DownloadError>;

// =============================================================================
// DownloadActions Trait
// =============================================================================

/// Interface for performing mutations during a download.
///
/// This abstracts away how files are actually written, making the core
/// download algorithm testable independently of the file store implementation.
///
/// Each method returns a `WithComplete<()>` that signals when the operation
/// has finished. The caller should add the `Complete` to a `Completes` to
/// track when all operations have finished.
#[async_trait]
pub trait DownloadActions: Send + Sync {
    /// Remove a file or directory at the given path.
    async fn rm(&self, path: &Path) -> Result<WithComplete<()>>;

    /// Create a directory at the given path.
    async fn mkdir(&self, path: &Path) -> Result<WithComplete<()>>;

    /// Download a file from the repository to the given path.
    async fn download_file(
        &self,
        path: &Path,
        file_id: &ObjectId,
        executable: bool,
    ) -> Result<WithComplete<()>>;

    /// Change the executable bit of a file.
    async fn set_executable(&self, path: &Path, executable: bool) -> Result<WithComplete<()>>;
}

// =============================================================================
// DownloadRepoToStore
// =============================================================================

/// State for downloading a single directory level from a repository to a file store.
///
/// As the download descends recursively through the directory structure, new
/// instances of this struct are created for each subdirectory.
pub struct DownloadRepoToStore<'a> {
    repo: Arc<Repo>,
    actions: &'a dyn DownloadActions,
    file_store_cache: Arc<dyn FileStoreCache>,
    store_path: PathBuf,
    cache_path_id: Option<DbId>,
    repo_directory_id: ObjectId,
    store_entries: Option<DirectoryList>,
    /// Tracks completion of all actions performed during this download.
    completes: Completes,
}

impl<'a> DownloadRepoToStore<'a> {
    /// Create a new `DownloadRepoToStore` for the root download.
    ///
    /// Examines the current state of the store at `store_path` and prepares
    /// for the download:
    /// - If the path points to a directory, its entries are loaded for merging.
    /// - If the path points to nothing, a directory is created.
    /// - If the path points to a file, it is removed and a directory is created.
    pub async fn new(
        repo: Arc<Repo>,
        repo_directory_id: ObjectId,
        file_store: &dyn FileStore,
        store_path: PathBuf,
        actions: &'a dyn DownloadActions,
    ) -> Result<Self> {
        let file_store_cache = file_store.get_cache();
        let dest = file_store.get_dest().ok_or(DownloadError::NoFileDest)?;

        // Get cache path ID for the store path
        let path_str = store_path.to_string_lossy();
        let cache_path_id = file_store_cache
            .get_path_id(&path_str)
            .await
            .map_err(|e| DownloadError::Cache(e.to_string()))?;

        let completes = Completes::new();

        // Check what exists at the store path
        let entry = dest.get_entry(&store_path).await?;
        let store_entries = match entry {
            Some(StoreDirectoryEntry::Dir(_)) => {
                // Directory exists - list its contents for merging
                dest.list_directory(&store_path).await?
            }
            None => {
                // Nothing exists - create the directory
                let with_complete = actions.mkdir(&store_path).await?;
                completes
                    .add(with_complete.complete)
                    .expect("done() not yet called");
                None
            }
            Some(StoreDirectoryEntry::File(_)) => {
                // File exists where we need a directory - remove and create
                let with_complete = actions.rm(&store_path).await?;
                completes
                    .add(with_complete.complete)
                    .expect("done() not yet called");
                let with_complete = actions.mkdir(&store_path).await?;
                completes
                    .add(with_complete.complete)
                    .expect("done() not yet called");
                None
            }
        };

        Ok(Self {
            repo,
            actions,
            file_store_cache,
            store_path,
            cache_path_id,
            repo_directory_id,
            store_entries,
            completes,
        })
    }

    /// Create a child `DownloadRepoToStore` for a subdirectory.
    ///
    /// Inherits values from the parent but represents a child directory with
    /// the given name. `parent_store_entries` is passed separately to avoid
    /// borrow conflicts when the caller also borrows other fields of `self`.
    async fn for_child_with_store_entries(
        &self,
        name: &str,
        repo_directory_id: ObjectId,
        parent_store_entries: &Option<DirectoryList>,
    ) -> Result<DownloadRepoToStore<'a>> {
        let child_path = self.store_path.join(name);

        // Get child cache path ID
        let child_cache_path_id = self
            .file_store_cache
            .get_path_entry_id(self.cache_path_id, name)
            .await
            .map_err(|e| DownloadError::Cache(e.to_string()))?;

        // Get child store entries from parent's directory list
        let child_store_entries = match parent_store_entries {
            Some(list) => list.list_directory(name).await?,
            None => None,
        };

        Ok(DownloadRepoToStore {
            repo: self.repo.clone(),
            actions: self.actions,
            file_store_cache: self.file_store_cache.clone(),
            store_path: child_path,
            cache_path_id: Some(child_cache_path_id),
            repo_directory_id,
            store_entries: child_store_entries,
            completes: Completes::new(),
        })
    }

    /// Execute the download, synchronizing the repo directory to the store.
    ///
    /// This is the core merge algorithm that walks through both the repo entries
    /// and store entries in sorted order, determining what changes need to be made.
    ///
    /// Returns a `WithComplete<()>` that signals when all actions have finished.
    pub async fn download_repo_to_store(mut self) -> Result<WithComplete<()>> {
        let mut repo_entries = self
            .repo
            .list_directory_entries(&self.repo_directory_id)
            .await?;

        let mut repo_entry = repo_entries.next().await?;
        let mut store_entry = match self.store_entries {
            Some(ref mut entries) => entries.next().await.transpose()?,
            None => None,
        };

        loop {
            match (&repo_entry, &store_entry) {
                (Some(re), Some(se)) => {
                    let repo_name = entry_name_repo(re);
                    let store_name = entry_name_store(se);

                    if repo_name < store_name {
                        // Add repo entry (not in store)
                        self.download_repo_entry(re).await?;
                        repo_entry = repo_entries.next().await?;
                    } else if repo_name > store_name {
                        // Remove store entry (not in repo)
                        self.remove_store_entry(se).await?;
                        store_entry = match self.store_entries {
                            Some(ref mut entries) => entries.next().await.transpose()?,
                            None => None,
                        };
                    } else {
                        // Both exist - merge
                        self.merge_entries(re, se).await?;
                        repo_entry = repo_entries.next().await?;
                        store_entry = match self.store_entries {
                            Some(ref mut entries) => entries.next().await.transpose()?,
                            None => None,
                        };
                    }
                }
                (Some(re), None) => {
                    // Only repo entries remaining - download them
                    self.download_repo_entry(re).await?;
                    repo_entry = repo_entries.next().await?;
                }
                (None, Some(se)) => {
                    // Only store entries remaining - remove them
                    self.remove_store_entry(se).await?;
                    store_entry = match self.store_entries {
                        Some(ref mut entries) => entries.next().await.transpose()?,
                        None => None,
                    };
                }
                (None, None) => {
                    // Both exhausted
                    break;
                }
            }
        }

        self.completes.done();
        Ok(WithComplete::new(
            (),
            Arc::new(self.completes) as Arc<dyn Complete>,
        ))
    }

    /// Download a repo entry that doesn't exist in the store.
    async fn download_repo_entry(&mut self, entry: &RepoDirectoryEntry) -> Result<()> {
        match entry {
            RepoDirectoryEntry::Directory(dir_entry) => {
                let child_path = self.store_path.join(&dir_entry.name);
                let with_complete = self.actions.mkdir(&child_path).await?;
                self.completes
                    .add(with_complete.complete)
                    .expect("done() not yet called");
                let child = self
                    .for_child_with_store_entries(
                        &dir_entry.name,
                        dir_entry.directory.clone(),
                        &None,
                    )
                    .await?;
                let with_complete = Box::pin(child.download_repo_to_store()).await?;
                self.completes
                    .add(with_complete.complete)
                    .expect("done() not yet called");
            }
            RepoDirectoryEntry::File(file_entry) => {
                let file_path = self.store_path.join(&file_entry.name);
                let with_complete = self
                    .actions
                    .download_file(&file_path, &file_entry.file, file_entry.executable)
                    .await?;
                self.completes
                    .add(with_complete.complete)
                    .expect("done() not yet called");
            }
        }
        Ok(())
    }

    /// Remove a store entry that doesn't exist in the repo.
    async fn remove_store_entry(&mut self, entry: &StoreDirectoryEntry) -> Result<()> {
        let entry_path = self.store_path.join(entry_name_store(entry));
        let with_complete = self.actions.rm(&entry_path).await?;
        self.completes
            .add(with_complete.complete)
            .expect("done() not yet called");
        Ok(())
    }

    /// Merge a repo entry with a matching store entry.
    async fn merge_entries(
        &mut self,
        repo_entry: &RepoDirectoryEntry,
        store_entry: &StoreDirectoryEntry,
    ) -> Result<()> {
        match (repo_entry, store_entry) {
            // Dir -> Dir: descend recursively through both
            (RepoDirectoryEntry::Directory(dir_entry), StoreDirectoryEntry::Dir(_)) => {
                let child = self
                    .for_child_with_store_entries(
                        &dir_entry.name,
                        dir_entry.directory.clone(),
                        &self.store_entries,
                    )
                    .await?;
                let with_complete = Box::pin(child.download_repo_to_store()).await?;
                self.completes
                    .add(with_complete.complete)
                    .expect("done() not yet called");
            }
            // Dir -> File: replace store file with repo directory
            (RepoDirectoryEntry::Directory(dir_entry), StoreDirectoryEntry::File(_)) => {
                let child_path = self.store_path.join(&dir_entry.name);
                let with_complete = self.actions.rm(&child_path).await?;
                self.completes
                    .add(with_complete.complete)
                    .expect("done() not yet called");
                let with_complete = self.actions.mkdir(&child_path).await?;
                self.completes
                    .add(with_complete.complete)
                    .expect("done() not yet called");
                let child = self
                    .for_child_with_store_entries(
                        &dir_entry.name,
                        dir_entry.directory.clone(),
                        &None,
                    )
                    .await?;
                let with_complete = Box::pin(child.download_repo_to_store()).await?;
                self.completes
                    .add(with_complete.complete)
                    .expect("done() not yet called");
            }
            // File -> Dir: replace store directory with repo file
            (RepoDirectoryEntry::File(file_entry), StoreDirectoryEntry::Dir(_)) => {
                let file_path = self.store_path.join(&file_entry.name);
                let with_complete = self.actions.rm(&file_path).await?;
                self.completes
                    .add(with_complete.complete)
                    .expect("done() not yet called");
                let with_complete = self
                    .actions
                    .download_file(&file_path, &file_entry.file, file_entry.executable)
                    .await?;
                self.completes
                    .add(with_complete.complete)
                    .expect("done() not yet called");
            }
            // File -> File: check if we can skip the download
            (RepoDirectoryEntry::File(file_entry), StoreDirectoryEntry::File(store_file)) => {
                let file_path = self.store_path.join(&file_entry.name);

                // Get cache path ID for this file
                let file_cache_path_id = self
                    .file_store_cache
                    .get_path_entry_id(self.cache_path_id, &file_entry.name)
                    .await
                    .map_err(|e| DownloadError::Cache(e.to_string()))?;

                // Check if the cached fingerprint matches
                let cached_info = self
                    .file_store_cache
                    .get_fingerprinted_file_info(file_cache_path_id)
                    .await
                    .map_err(|e| DownloadError::Cache(e.to_string()))?;

                let file_matches = matches!(
                    (&cached_info, &store_file.fingerprint),
                    (Some(info), Some(fingerprint))
                        if info.fingerprint == *fingerprint
                            && info.object_id == file_entry.file
                );

                if file_matches {
                    // File content matches - just check executable bit
                    if store_file.executable != file_entry.executable {
                        let with_complete = self
                            .actions
                            .set_executable(&file_path, file_entry.executable)
                            .await?;
                        self.completes
                            .add(with_complete.complete)
                            .expect("done() not yet called");
                    }
                } else {
                    // File doesn't match - replace it
                    let with_complete = self.actions.rm(&file_path).await?;
                    self.completes
                        .add(with_complete.complete)
                        .expect("done() not yet called");
                    let with_complete = self
                        .actions
                        .download_file(&file_path, &file_entry.file, file_entry.executable)
                        .await?;
                    self.completes
                        .add(with_complete.complete)
                        .expect("done() not yet called");
                }
            }
        }
        Ok(())
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Get the name of a repo directory entry.
fn entry_name_repo(entry: &RepoDirectoryEntry) -> &str {
    match entry {
        RepoDirectoryEntry::File(f) => &f.name,
        RepoDirectoryEntry::Directory(d) => &d.name,
    }
}

/// Get the name of a store directory entry.
fn entry_name_store(entry: &StoreDirectoryEntry) -> &str {
    match entry {
        StoreDirectoryEntry::File(f) => &f.name,
        StoreDirectoryEntry::Dir(d) => &d.name,
    }
}

// =============================================================================
// StagedFileChunkWithContentList
// =============================================================================

/// An iterator over file chunks that checks a stage before downloading.
///
/// This implements [`FileChunksWithContent`] but looks in a [`FileDestStage`]
/// for each chunk before falling back to downloading from the repository.
/// This is used in the second phase of staged downloads.
pub struct StagedFileChunkWithContentList {
    repo: Arc<Repo>,
    stage: Arc<dyn FileDestStage>,
    /// Stack of pending file parts to process.
    /// Each element is (file, index into parts).
    pending: Vec<(File, usize)>,
    /// ManagedBuffers for capacity management when downloading.
    managed_buffers: ManagedBuffers,
}

impl StagedFileChunkWithContentList {
    /// Create a new staged file chunk list.
    ///
    /// # Arguments
    /// * `repo` - The repository to download from if chunks are not in stage
    /// * `stage` - The stage to check for cached chunks
    /// * `file` - The file to iterate through
    /// * `managed_buffers` - Buffer manager for backpressure control
    pub fn new(
        repo: Arc<Repo>,
        stage: Arc<dyn FileDestStage>,
        file: File,
        managed_buffers: ManagedBuffers,
    ) -> Self {
        Self {
            repo,
            stage,
            pending: vec![(file, 0)],
            managed_buffers,
        }
    }
}

#[async_trait]
impl FileChunksWithContent for StagedFileChunkWithContentList {
    async fn next(&mut self) -> repo::Result<Option<FileChunkWithContent>> {
        // Get the next chunk (handling FileFilePart recursion)
        let chunk = loop {
            let Some((file, idx)) = self.pending.last_mut() else {
                return Ok(None);
            };

            if *idx >= file.parts.len() {
                self.pending.pop();
                continue;
            }

            let part = file.parts[*idx].clone();
            *idx += 1;

            match part {
                FilePart::Chunk(chunk) => {
                    break chunk;
                }
                FilePart::File(file_part) => {
                    let nested_file = self.repo.read_file(&file_part.file).await?;
                    self.pending.push((nested_file, 0));
                }
            }
        };

        // Check if the chunk is in the stage
        let chunk_id = &chunk.content;
        if let Ok(Some(content)) = self.stage.read_chunk(chunk_id).await {
            // Chunk is in stage - return it directly
            return Ok(Some(FileChunkWithContent::new_with_content(chunk, content)));
        }

        // Chunk not in stage - download from repo
        // Acquire buffer capacity for backpressure
        let acquired = self.managed_buffers.acquire(chunk.size).await;

        let repo = Arc::clone(&self.repo);
        let chunk_id = chunk.content.clone();

        let handle = tokio::spawn(async move { repo.read(&chunk_id, Some(acquired)).await });

        Ok(Some(FileChunkWithContent::new(chunk, handle)))
    }
}

// =============================================================================
// Public Download Functions
// =============================================================================

/// Download a single file from a repository to a file store.
///
/// Downloads the file identified by `file_id` from the repository and writes it
/// to `path` in the file store. If a file already exists at that path, it will
/// be overwritten.
///
/// The function blocks until it has acquired all resources needed to complete
/// the operation (usually ManagedBuffers for backpressure), then returns a
/// `WithComplete` that signals when the download has finished.
///
/// # Arguments
///
/// * `repo` - The repository to download from
/// * `file_id` - The ObjectId of the file to download
/// * `store` - The file store to download to (must support FileDest)
/// * `path` - The path within the store where the file should be written
/// * `executable` - Whether the file should be marked as executable
///
/// # Errors
///
/// Returns an error if:
/// * The file store does not support writing (`NoFileDest`)
/// * The file cannot be read from the repository
/// * The file cannot be written to the store
pub async fn download_file(
    repo: Arc<Repo>,
    file_id: &ObjectId,
    store: &dyn FileStore,
    path: &Path,
    executable: bool,
) -> Result<WithComplete<()>> {
    let dest = store.get_dest().ok_or(DownloadError::NoFileDest)?;

    // Get chunks with content (this applies backpressure via ManagedBuffers)
    let chunks = repo.read_file_chunks_with_content(file_id).await?;

    // Write the file using the boxed chunks iterator
    let with_complete = dest
        .write_file_from_chunks(path, Box::new(chunks), executable)
        .await?;

    Ok(with_complete)
}

// =============================================================================
// DownloadToFileStoreActions
// =============================================================================

/// Implementation of [`DownloadActions`] that writes directly to a file store.
///
/// This is used for non-staged downloads where files are written directly to
/// their final locations as they are downloaded.
pub struct DownloadToFileStoreActions<'a> {
    repo: Arc<Repo>,
    dest: &'a dyn file_store::FileDest,
}

impl<'a> DownloadToFileStoreActions<'a> {
    /// Create a new `DownloadToFileStoreActions`.
    pub fn new(repo: Arc<Repo>, dest: &'a dyn file_store::FileDest) -> Self {
        Self { repo, dest }
    }
}

#[async_trait]
impl<'a> DownloadActions for DownloadToFileStoreActions<'a> {
    async fn rm(&self, path: &Path) -> Result<WithComplete<()>> {
        self.dest.rm(path).await?;
        Ok(WithComplete::new(
            (),
            Arc::new(crate::util::NoopComplete) as Arc<dyn Complete>,
        ))
    }

    async fn mkdir(&self, path: &Path) -> Result<WithComplete<()>> {
        self.dest.mkdir(path).await?;
        Ok(WithComplete::new(
            (),
            Arc::new(crate::util::NoopComplete) as Arc<dyn Complete>,
        ))
    }

    async fn download_file(
        &self,
        path: &Path,
        file_id: &ObjectId,
        executable: bool,
    ) -> Result<WithComplete<()>> {
        let chunks = self.repo.read_file_chunks_with_content(file_id).await?;
        let with_complete = self
            .dest
            .write_file_from_chunks(path, Box::new(chunks), executable)
            .await?;
        Ok(with_complete)
    }

    async fn set_executable(&self, path: &Path, executable: bool) -> Result<WithComplete<()>> {
        self.dest.set_executable(path, executable).await?;
        Ok(WithComplete::new(
            (),
            Arc::new(crate::util::NoopComplete) as Arc<dyn Complete>,
        ))
    }
}

// =============================================================================
// DownloadToStageActions
// =============================================================================

/// Implementation of [`DownloadActions`] for the first phase of a staged download.
///
/// In the first phase, we download all required chunks to the stage. The `rm`,
/// `mkdir`, and `set_executable` methods are no-ops since we're just collecting
/// chunks, not modifying the file store yet.
pub struct DownloadToStageActions {
    repo: Arc<Repo>,
    stage: Arc<dyn FileDestStage>,
    managed_buffers: ManagedBuffers,
    /// Set of chunk IDs currently being downloaded, for deduplication.
    downloading: std::sync::Mutex<std::collections::HashSet<ObjectId>>,
}

impl DownloadToStageActions {
    /// Create a new `DownloadToStageActions`.
    pub fn new(
        repo: Arc<Repo>,
        stage: Arc<dyn FileDestStage>,
        managed_buffers: ManagedBuffers,
    ) -> Self {
        Self {
            repo,
            stage,
            managed_buffers,
            downloading: std::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }
}

#[async_trait]
impl DownloadActions for DownloadToStageActions {
    async fn rm(&self, _path: &Path) -> Result<WithComplete<()>> {
        // No-op in phase 1
        Ok(WithComplete::new(
            (),
            Arc::new(crate::util::NoopComplete) as Arc<dyn Complete>,
        ))
    }

    async fn mkdir(&self, _path: &Path) -> Result<WithComplete<()>> {
        // No-op in phase 1
        Ok(WithComplete::new(
            (),
            Arc::new(crate::util::NoopComplete) as Arc<dyn Complete>,
        ))
    }

    async fn download_file(
        &self,
        _path: &Path,
        file_id: &ObjectId,
        _executable: bool,
    ) -> Result<WithComplete<()>> {
        let completes = Completes::new();
        let mut file_chunks = self.repo.list_file_chunks(file_id).await?;

        while let Some(chunk) = file_chunks.next().await? {
            let chunk_id = chunk.content.clone();

            // Check if we're already downloading this chunk
            {
                let downloading = self.downloading.lock().unwrap();
                if downloading.contains(&chunk_id) {
                    continue;
                }
            }

            // Check if chunk is in stage (outside the lock)
            if self.stage.has_chunk(&chunk_id).await? {
                continue;
            }

            // Mark as downloading - check again in case another task started
            {
                let mut downloading = self.downloading.lock().unwrap();
                if !downloading.insert(chunk_id.clone()) {
                    // Another task started downloading while we checked the stage
                    continue;
                }
            }

            // Acquire buffer capacity for backpressure
            let acquired = self.managed_buffers.acquire(chunk.size).await;

            // Create a NotifyComplete to track this download
            let notify = Arc::new(crate::util::NotifyComplete::new());
            completes
                .add(notify.clone() as Arc<dyn Complete>)
                .expect("done() not yet called");

            // Spawn task to download and stage the chunk
            let repo = Arc::clone(&self.repo);
            let stage = Arc::clone(&self.stage);

            tokio::spawn(async move {
                let result = async {
                    let buffer = repo.read(&chunk_id, Some(acquired)).await?;
                    stage.write_chunk(&chunk_id, buffer).await?;
                    Ok::<_, DownloadError>(())
                }
                .await;

                match result {
                    Ok(()) => notify.notify_complete(),
                    Err(e) => notify.notify_error(e.to_string()),
                }
            });
        }

        completes.done();
        Ok(WithComplete::new(
            (),
            Arc::new(completes) as Arc<dyn Complete>,
        ))
    }

    async fn set_executable(&self, _path: &Path, _executable: bool) -> Result<WithComplete<()>> {
        // No-op in phase 1
        Ok(WithComplete::new(
            (),
            Arc::new(crate::util::NoopComplete) as Arc<dyn Complete>,
        ))
    }
}

// =============================================================================
// DownloadWithStageActions
// =============================================================================

/// Implementation of [`DownloadActions`] for the second phase of a staged download.
///
/// In the second phase, we assemble files from the staged chunks. We use
/// [`StagedFileChunkWithContentList`] to read from the stage when possible,
/// falling back to downloading from the repo if needed (in case the store changed
/// between phases and new chunks are required).
pub struct DownloadWithStageActions<'a> {
    repo: Arc<Repo>,
    dest: &'a dyn file_store::FileDest,
    stage: Arc<dyn FileDestStage>,
    managed_buffers: ManagedBuffers,
}

impl<'a> DownloadWithStageActions<'a> {
    /// Create a new `DownloadWithStageActions`.
    pub fn new(
        repo: Arc<Repo>,
        dest: &'a dyn file_store::FileDest,
        stage: Arc<dyn FileDestStage>,
        managed_buffers: ManagedBuffers,
    ) -> Self {
        Self {
            repo,
            dest,
            stage,
            managed_buffers,
        }
    }
}

#[async_trait]
impl<'a> DownloadActions for DownloadWithStageActions<'a> {
    async fn rm(&self, path: &Path) -> Result<WithComplete<()>> {
        self.dest.rm(path).await?;
        Ok(WithComplete::new(
            (),
            Arc::new(crate::util::NoopComplete) as Arc<dyn Complete>,
        ))
    }

    async fn mkdir(&self, path: &Path) -> Result<WithComplete<()>> {
        self.dest.mkdir(path).await?;
        Ok(WithComplete::new(
            (),
            Arc::new(crate::util::NoopComplete) as Arc<dyn Complete>,
        ))
    }

    async fn download_file(
        &self,
        path: &Path,
        file_id: &ObjectId,
        executable: bool,
    ) -> Result<WithComplete<()>> {
        // Read the file to get its structure
        let file = self.repo.read_file(file_id).await?;

        // Create a StagedFileChunkWithContentList that reads from the stage when possible
        let chunks = StagedFileChunkWithContentList::new(
            Arc::clone(&self.repo),
            self.stage.clone(),
            file,
            self.managed_buffers.clone(),
        );

        let with_complete = self
            .dest
            .write_file_from_chunks(path, Box::new(chunks), executable)
            .await?;
        Ok(with_complete)
    }

    async fn set_executable(&self, path: &Path, executable: bool) -> Result<WithComplete<()>> {
        self.dest.set_executable(path, executable).await?;
        Ok(WithComplete::new(
            (),
            Arc::new(crate::util::NoopComplete) as Arc<dyn Complete>,
        ))
    }
}

// =============================================================================
// download_directory function
// =============================================================================

/// Download a directory from a repository to a file store.
///
/// Recursively downloads the directory identified by `directory_id` from the
/// repository and synchronizes it to `path` in the file store. The store must
/// support [`FileDest`](file_store::FileDest).
///
/// If `with_stage` is false, files are downloaded directly to their final
/// locations. This minimizes temporary storage requirements but may leave the
/// file store in a disrupted state for longer.
///
/// If `with_stage` is true and the file store supports staging, the download
/// proceeds in two phases:
/// 1. All required chunks are downloaded to a temporary stage
/// 2. Files are assembled from the staged chunks and moved to their final locations
///
/// This minimizes the time the file store is in a disrupted state.
///
/// # Arguments
///
/// * `repo` - The repository to download from
/// * `directory_id` - The ObjectId of the directory to download
/// * `store` - The file store to download to (must support FileDest)
/// * `path` - The path within the store where the directory should be written
/// * `with_stage` - Whether to use staged downloading
///
/// # Errors
///
/// Returns an error if:
/// * The file store does not support writing (`NoFileDest`)
/// * The directory cannot be read from the repository
/// * Files cannot be written to the store
pub async fn download_directory(
    repo: Arc<Repo>,
    directory_id: &ObjectId,
    store: &dyn FileStore,
    path: PathBuf,
    with_stage: bool,
) -> Result<WithComplete<()>> {
    let dest = store.get_dest().ok_or(DownloadError::NoFileDest)?;

    if with_stage {
        // Try to create a stage
        if let Some(stage) = dest.create_stage().await? {
            // Stage is available - use two-phase download
            let stage: Arc<dyn FileDestStage> = Arc::from(stage);
            let managed_buffers = ManagedBuffers::new();

            // Phase 1: Download all chunks to the stage
            let phase1_actions = DownloadToStageActions::new(
                Arc::clone(&repo),
                Arc::clone(&stage),
                managed_buffers.clone(),
            );
            let download = DownloadRepoToStore::new(
                Arc::clone(&repo),
                directory_id.clone(),
                store,
                path.clone(),
                &phase1_actions,
            )
            .await?;
            let phase1_result = download.download_repo_to_store().await?;
            // Wait for phase 1 to complete
            phase1_result
                .complete
                .complete()
                .await
                .map_err(|e| DownloadError::Cache(format!("Stage download failed: {}", e)))?;

            // Phase 2: Assemble files from staged chunks
            let phase2_actions = DownloadWithStageActions::new(
                Arc::clone(&repo),
                dest,
                Arc::clone(&stage),
                managed_buffers,
            );
            let download =
                DownloadRepoToStore::new(repo, directory_id.clone(), store, path, &phase2_actions)
                    .await?;
            return download.download_repo_to_store().await;
        }
    }

    // Non-staged download or stage not available
    let actions = DownloadToFileStoreActions::new(Arc::clone(&repo), dest);
    let download =
        DownloadRepoToStore::new(repo, directory_id.clone(), store, path, &actions).await?;
    download.download_repo_to_store().await
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;
    use crate::caches::{CacheError, NoopFileStoreCache};
    use crate::repository::{
        ChunkFilePart, DirEntry as RepoDirEntry, Directory, DirectoryType, File,
        FileEntry as RepoFileEntry, FilePart, FileType, RepoObject,
    };
    use crate::util::ManagedBuffers;

    use async_trait::async_trait;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use crate::caches::RepoCache;

    /// Simple test cache for Repo.
    struct TestRepoCache;

    #[async_trait]
    impl RepoCache for TestRepoCache {
        async fn object_exists(&self, _id: &ObjectId) -> std::result::Result<bool, CacheError> {
            Ok(false)
        }
        async fn set_object_exists(&self, _id: &ObjectId) -> std::result::Result<(), CacheError> {
            Ok(())
        }
        async fn object_fully_stored(
            &self,
            _id: &ObjectId,
        ) -> std::result::Result<bool, CacheError> {
            Ok(false)
        }
        async fn set_object_fully_stored(
            &self,
            _id: &ObjectId,
        ) -> std::result::Result<(), CacheError> {
            Ok(())
        }
        async fn get_object(
            &self,
            _id: &ObjectId,
        ) -> std::result::Result<Option<RepoObject>, CacheError> {
            Ok(None)
        }
        async fn set_object(
            &self,
            _id: &ObjectId,
            _obj: &RepoObject,
        ) -> std::result::Result<(), CacheError> {
            Ok(())
        }
    }

    /// A recording DownloadActions implementation for testing.
    ///
    /// Records all actions taken and provides a simple in-memory filesystem
    /// for verification.
    struct RecordingActions {
        actions: Mutex<Vec<String>>,
    }

    impl RecordingActions {
        fn new() -> Self {
            Self {
                actions: Mutex::new(Vec::new()),
            }
        }

        fn get_actions(&self) -> Vec<String> {
            self.actions.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DownloadActions for RecordingActions {
        async fn rm(&self, path: &Path) -> Result<WithComplete<()>> {
            self.actions
                .lock()
                .unwrap()
                .push(format!("rm:{}", path.display()));
            Ok(WithComplete::new(
                (),
                Arc::new(crate::util::NoopComplete) as Arc<dyn Complete>,
            ))
        }

        async fn mkdir(&self, path: &Path) -> Result<WithComplete<()>> {
            self.actions
                .lock()
                .unwrap()
                .push(format!("mkdir:{}", path.display()));
            Ok(WithComplete::new(
                (),
                Arc::new(crate::util::NoopComplete) as Arc<dyn Complete>,
            ))
        }

        async fn download_file(
            &self,
            path: &Path,
            file_id: &ObjectId,
            executable: bool,
        ) -> Result<WithComplete<()>> {
            self.actions.lock().unwrap().push(format!(
                "download:{}:{}:{}",
                path.display(),
                file_id,
                executable
            ));
            Ok(WithComplete::new(
                (),
                Arc::new(crate::util::NoopComplete) as Arc<dyn Complete>,
            ))
        }

        async fn set_executable(&self, path: &Path, executable: bool) -> Result<WithComplete<()>> {
            self.actions.lock().unwrap().push(format!(
                "set_executable:{}:{}",
                path.display(),
                executable
            ));
            Ok(WithComplete::new(
                (),
                Arc::new(crate::util::NoopComplete) as Arc<dyn Complete>,
            ))
        }
    }

    /// A simple in-memory FileStore for testing downloads.
    ///
    /// Supports get_dest() with get_entry() and list_directory() but
    /// does not implement full FileDest write operations.
    struct TestFileStore {
        /// Map of path -> directory entries. Empty string = root.
        directories: BTreeMap<String, Vec<StoreDirectoryEntry>>,
        cache: Arc<dyn FileStoreCache>,
    }

    impl TestFileStore {
        fn empty() -> Self {
            Self {
                directories: BTreeMap::new(),
                cache: Arc::new(NoopFileStoreCache),
            }
        }

        fn with_entries(entries: BTreeMap<String, Vec<StoreDirectoryEntry>>) -> Self {
            Self {
                directories: entries,
                cache: Arc::new(NoopFileStoreCache),
            }
        }
    }

    impl FileStore for TestFileStore {
        fn get_source(&self) -> Option<&dyn file_store::FileSource> {
            None
        }

        fn get_dest(&self) -> Option<&dyn file_store::FileDest> {
            Some(self)
        }

        fn get_cache(&self) -> Arc<dyn FileStoreCache> {
            self.cache.clone()
        }
    }

    #[async_trait]
    impl file_store::FileDest for TestFileStore {
        async fn get_entry(&self, path: &Path) -> file_store::Result<Option<StoreDirectoryEntry>> {
            let path_str = path.to_string_lossy().to_string();
            if self.directories.contains_key(&path_str) {
                Ok(Some(StoreDirectoryEntry::Dir(file_store::DirEntry {
                    name: path
                        .file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_default(),
                    path: path_str,
                })))
            } else {
                Ok(None)
            }
        }

        async fn list_directory(&self, path: &Path) -> file_store::Result<Option<DirectoryList>> {
            let path_str = path.to_string_lossy().to_string();
            match self.directories.get(&path_str) {
                Some(entries) => {
                    let helper = file_store::ScanIgnoreHelper::new();
                    Ok(Some(DirectoryList::new(
                        entries.clone(),
                        helper,
                        Arc::new(NullDirectoryListSource),
                        path_str,
                    )))
                }
                None => Ok(None),
            }
        }

        async fn write_file_from_chunks(
            &self,
            _path: &Path,
            _chunks: crate::repo::BoxedFileChunksWithContent,
            _executable: bool,
        ) -> file_store::Result<crate::util::WithComplete<()>> {
            Ok(crate::util::WithComplete::new(
                (),
                std::sync::Arc::new(crate::util::NoopComplete)
                    as std::sync::Arc<dyn crate::util::Complete>,
            ))
        }

        async fn rm(&self, _path: &Path) -> file_store::Result<()> {
            Ok(())
        }

        async fn mkdir(&self, _path: &Path) -> file_store::Result<()> {
            Ok(())
        }

        async fn set_executable(&self, _path: &Path, _executable: bool) -> file_store::Result<()> {
            Ok(())
        }

        async fn create_stage(
            &self,
        ) -> file_store::Result<Option<Box<dyn file_store::FileDestStage>>> {
            Ok(None)
        }
    }

    /// Null implementation for DirectoryListSource (no child directory listing support).
    struct NullDirectoryListSource;

    #[async_trait]
    impl file_store::ScanFileSource for NullDirectoryListSource {
        async fn get_file(
            &self,
            _path: &Path,
        ) -> std::result::Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>> {
            Err("not implemented".into())
        }
    }

    #[async_trait]
    impl file_store::DirectoryListSource for NullDirectoryListSource {
        async fn list_raw_directory(
            &self,
            _path: &str,
        ) -> file_store::Result<Option<Vec<StoreDirectoryEntry>>> {
            Ok(None)
        }
    }

    /// Helper to write raw chunk data to a repo and return its object id.
    async fn write_chunk(repo: &Arc<Repo>, data: &[u8]) -> ObjectId {
        let buffers = ManagedBuffers::new();
        let buffer = buffers.get_buffer_with_data(data.to_vec()).await;
        let id = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(data);
            hex::encode(hasher.finalize())
        };
        let result = repo.write(&id, Arc::new(buffer)).await.unwrap();
        result.complete.complete().await.unwrap();
        id
    }

    #[tokio::test]
    async fn test_download_into_empty_store() {
        // Create a repo with: file_a.txt, dir_b/file_c.txt
        let backend = MemoryBackend::new();
        let repo = Arc::new(Repo::new(backend, TestRepoCache));

        let chunk_id = write_chunk(&repo, b"hello").await;

        let file_obj = File {
            type_tag: FileType::File,
            parts: vec![FilePart::Chunk(ChunkFilePart {
                size: 5,
                content: chunk_id.clone(),
            })],
        };
        let file_result = repo
            .write_object(&RepoObject::File(file_obj))
            .await
            .unwrap();
        let file_id = file_result.result.clone();
        file_result.complete.complete().await.unwrap();

        // Inner directory with file_c.txt
        let inner_dir = Directory {
            type_tag: DirectoryType::Directory,
            entries: vec![crate::repository::DirectoryPart::File(RepoFileEntry {
                name: "file_c.txt".to_string(),
                size: 5,
                executable: false,
                file: file_id.clone(),
            })],
        };
        let inner_dir_result = repo
            .write_object(&RepoObject::Directory(inner_dir))
            .await
            .unwrap();
        let inner_dir_id = inner_dir_result.result.clone();
        inner_dir_result.complete.complete().await.unwrap();

        // Root directory with file_a.txt and dir_b
        let root_dir = Directory {
            type_tag: DirectoryType::Directory,
            entries: vec![
                crate::repository::DirectoryPart::Directory(RepoDirEntry {
                    name: "dir_b".to_string(),
                    directory: inner_dir_id.clone(),
                }),
                crate::repository::DirectoryPart::File(RepoFileEntry {
                    name: "file_a.txt".to_string(),
                    size: 5,
                    executable: false,
                    file: file_id.clone(),
                }),
            ],
        };
        let root_result = repo
            .write_object(&RepoObject::Directory(root_dir))
            .await
            .unwrap();
        let root_dir_id = root_result.result.clone();
        root_result.complete.complete().await.unwrap();

        let store = TestFileStore::empty();
        let actions = RecordingActions::new();

        let download =
            DownloadRepoToStore::new(repo, root_dir_id, &store, PathBuf::from("dest"), &actions)
                .await
                .unwrap();

        download.download_repo_to_store().await.unwrap();

        let recorded = actions.get_actions();
        assert!(recorded.contains(&"mkdir:dest".to_string()));
        assert!(recorded.contains(&"mkdir:dest/dir_b".to_string()));
        assert!(
            recorded
                .iter()
                .any(|a| a.starts_with("download:dest/dir_b/file_c.txt:"))
        );
        assert!(
            recorded
                .iter()
                .any(|a| a.starts_with("download:dest/file_a.txt:"))
        );
    }

    #[tokio::test]
    async fn test_download_removes_extra_store_entries() {
        // Repo has: file_a.txt
        // Store has: file_a.txt, file_b.txt (extra)
        let backend = MemoryBackend::new();
        let repo = Arc::new(Repo::new(backend, TestRepoCache));

        let chunk_id = write_chunk(&repo, b"hello").await;
        let file_obj = File {
            type_tag: FileType::File,
            parts: vec![FilePart::Chunk(ChunkFilePart {
                size: 5,
                content: chunk_id.clone(),
            })],
        };
        let file_result = repo
            .write_object(&RepoObject::File(file_obj))
            .await
            .unwrap();
        let file_id = file_result.result.clone();
        file_result.complete.complete().await.unwrap();

        let root_dir = Directory {
            type_tag: DirectoryType::Directory,
            entries: vec![crate::repository::DirectoryPart::File(RepoFileEntry {
                name: "file_a.txt".to_string(),
                size: 5,
                executable: false,
                file: file_id.clone(),
            })],
        };
        let root_result = repo
            .write_object(&RepoObject::Directory(root_dir))
            .await
            .unwrap();
        let root_dir_id = root_result.result.clone();
        root_result.complete.complete().await.unwrap();

        let mut dirs = BTreeMap::new();
        dirs.insert(
            "dest".to_string(),
            vec![
                StoreDirectoryEntry::File(file_store::FileEntry {
                    name: "file_a.txt".to_string(),
                    path: "dest/file_a.txt".to_string(),
                    size: 5,
                    executable: false,
                    fingerprint: None,
                }),
                StoreDirectoryEntry::File(file_store::FileEntry {
                    name: "file_b.txt".to_string(),
                    path: "dest/file_b.txt".to_string(),
                    size: 10,
                    executable: false,
                    fingerprint: None,
                }),
            ],
        );
        let store = TestFileStore::with_entries(dirs);
        let actions = RecordingActions::new();

        let download =
            DownloadRepoToStore::new(repo, root_dir_id, &store, PathBuf::from("dest"), &actions)
                .await
                .unwrap();

        download.download_repo_to_store().await.unwrap();

        let recorded = actions.get_actions();
        // file_b.txt should be removed since it's not in the repo
        assert!(recorded.contains(&"rm:dest/file_b.txt".to_string()));
        // file_a.txt has no fingerprint match so it gets replaced
        assert!(recorded.contains(&"rm:dest/file_a.txt".to_string()));
        assert!(
            recorded
                .iter()
                .any(|a| a.starts_with("download:dest/file_a.txt:"))
        );
    }

    #[tokio::test]
    async fn test_download_replaces_file_with_directory() {
        // Repo has: dir_a/ (directory)
        // Store has: dir_a (file)
        let backend = MemoryBackend::new();
        let repo = Arc::new(Repo::new(backend, TestRepoCache));

        let empty_dir = Directory {
            type_tag: DirectoryType::Directory,
            entries: vec![],
        };
        let empty_dir_result = repo
            .write_object(&RepoObject::Directory(empty_dir))
            .await
            .unwrap();
        let empty_dir_id = empty_dir_result.result.clone();
        empty_dir_result.complete.complete().await.unwrap();

        let root_dir = Directory {
            type_tag: DirectoryType::Directory,
            entries: vec![crate::repository::DirectoryPart::Directory(RepoDirEntry {
                name: "dir_a".to_string(),
                directory: empty_dir_id.clone(),
            })],
        };
        let root_result = repo
            .write_object(&RepoObject::Directory(root_dir))
            .await
            .unwrap();
        let root_dir_id = root_result.result.clone();
        root_result.complete.complete().await.unwrap();

        let mut dirs = BTreeMap::new();
        dirs.insert(
            "dest".to_string(),
            vec![StoreDirectoryEntry::File(file_store::FileEntry {
                name: "dir_a".to_string(),
                path: "dest/dir_a".to_string(),
                size: 100,
                executable: false,
                fingerprint: None,
            })],
        );
        let store = TestFileStore::with_entries(dirs);
        let actions = RecordingActions::new();

        let download =
            DownloadRepoToStore::new(repo, root_dir_id, &store, PathBuf::from("dest"), &actions)
                .await
                .unwrap();

        download.download_repo_to_store().await.unwrap();

        let recorded = actions.get_actions();
        // Should remove the file and create a directory
        assert!(recorded.contains(&"rm:dest/dir_a".to_string()));
        assert!(recorded.contains(&"mkdir:dest/dir_a".to_string()));
    }

    #[tokio::test]
    async fn test_download_replaces_directory_with_file() {
        // Repo has: item_a (file)
        // Store has: item_a/ (directory)
        let backend = MemoryBackend::new();
        let repo = Arc::new(Repo::new(backend, TestRepoCache));

        let chunk_id = write_chunk(&repo, b"data").await;
        let file_obj = File {
            type_tag: FileType::File,
            parts: vec![FilePart::Chunk(ChunkFilePart {
                size: 4,
                content: chunk_id.clone(),
            })],
        };
        let file_result = repo
            .write_object(&RepoObject::File(file_obj))
            .await
            .unwrap();
        let file_id = file_result.result.clone();
        file_result.complete.complete().await.unwrap();

        let root_dir = Directory {
            type_tag: DirectoryType::Directory,
            entries: vec![crate::repository::DirectoryPart::File(RepoFileEntry {
                name: "item_a".to_string(),
                size: 4,
                executable: true,
                file: file_id.clone(),
            })],
        };
        let root_result = repo
            .write_object(&RepoObject::Directory(root_dir))
            .await
            .unwrap();
        let root_dir_id = root_result.result.clone();
        root_result.complete.complete().await.unwrap();

        let mut dirs = BTreeMap::new();
        dirs.insert(
            "dest".to_string(),
            vec![StoreDirectoryEntry::Dir(file_store::DirEntry {
                name: "item_a".to_string(),
                path: "dest/item_a".to_string(),
            })],
        );
        let store = TestFileStore::with_entries(dirs);
        let actions = RecordingActions::new();

        let download =
            DownloadRepoToStore::new(repo, root_dir_id, &store, PathBuf::from("dest"), &actions)
                .await
                .unwrap();

        download.download_repo_to_store().await.unwrap();

        let recorded = actions.get_actions();
        assert!(recorded.contains(&"rm:dest/item_a".to_string()));
        assert!(
            recorded
                .iter()
                .any(|a| a.starts_with("download:dest/item_a:") && a.ends_with(":true"))
        );
    }
}
