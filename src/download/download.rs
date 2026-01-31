//! Core download implementation for synchronizing a repository directory to a file store.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use crate::caches::{DbId, FileStoreCache};
use crate::file_store::{self, DirectoryList, FileStore};
use crate::repo::{self, Repo, RepoError};
use crate::repository::ObjectId;

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
#[async_trait]
pub trait DownloadActions: Send + Sync {
    /// Remove a file or directory at the given path.
    async fn rm(&self, path: &Path) -> Result<()>;

    /// Create a directory at the given path.
    async fn mkdir(&self, path: &Path) -> Result<()>;

    /// Download a file from the repository to the given path.
    async fn download_file(
        &self,
        path: &Path,
        file_id: &ObjectId,
        executable: bool,
    ) -> Result<()>;

    /// Change the executable bit of a file.
    async fn set_executable(&self, path: &Path, executable: bool) -> Result<()>;
}

// =============================================================================
// FileDestDownloadActions
// =============================================================================

/// Default implementation of [`DownloadActions`] that wraps a [`FileDest`] and [`Repo`].
///
/// The `download_file` method reads file chunks from the repository and writes
/// them to the file store via `FileDest::write_file_from_chunks`.
pub struct FileDestDownloadActions {
    file_store: Arc<dyn FileStore>,
    repo: Arc<Repo>,
}

impl FileDestDownloadActions {
    /// Create a new `FileDestDownloadActions`.
    pub fn new(file_store: Arc<dyn FileStore>, repo: Arc<Repo>) -> Result<Self> {
        // Verify the file store supports writing
        if file_store.get_dest().is_none() {
            return Err(DownloadError::NoFileDest);
        }
        Ok(Self { file_store, repo })
    }

    fn dest(&self) -> &dyn file_store::FileDest {
        self.file_store.get_dest().unwrap()
    }
}

#[async_trait]
impl DownloadActions for FileDestDownloadActions {
    async fn rm(&self, path: &Path) -> Result<()> {
        self.dest().rm(path).await?;
        Ok(())
    }

    async fn mkdir(&self, path: &Path) -> Result<()> {
        self.dest().mkdir(path).await?;
        Ok(())
    }

    async fn download_file(
        &self,
        path: &Path,
        file_id: &ObjectId,
        executable: bool,
    ) -> Result<()> {
        // Read file chunks from the repo and collect them into SourceChunk items.
        let mut chunk_list = self.repo.list_file_chunks(file_id).await?;
        let mut chunks: Vec<std::result::Result<Box<dyn file_store::SourceChunk>, file_store::Error>> = Vec::new();
        let mut offset: u64 = 0;

        loop {
            match chunk_list.next().await {
                Ok(Some(chunk_part)) => {
                    let chunk: Box<dyn file_store::SourceChunk> = Box::new(RepoSourceChunk {
                        repo: self.repo.clone(),
                        offset,
                        size: chunk_part.size,
                        content_id: chunk_part.content.clone(),
                    });
                    offset += chunk_part.size;
                    chunks.push(Ok(chunk));
                }
                Ok(None) => break,
                Err(e) => {
                    chunks.push(Err(file_store::Error::Other(e.to_string())));
                    break;
                }
            }
        }

        let source_chunks: file_store::SourceChunks =
            Box::pin(futures::stream::iter(chunks));
        self.dest()
            .write_file_from_chunks(path, source_chunks, executable)
            .await?;
        Ok(())
    }

    async fn set_executable(&self, path: &Path, executable: bool) -> Result<()> {
        self.dest().set_executable(path, executable).await?;
        Ok(())
    }
}

/// A [`SourceChunk`] that reads its content from a repository object.
struct RepoSourceChunk {
    repo: Arc<Repo>,
    offset: u64,
    size: u64,
    content_id: ObjectId,
}

#[async_trait]
impl file_store::SourceChunk for RepoSourceChunk {
    fn offset(&self) -> u64 {
        self.offset
    }

    fn size(&self) -> u64 {
        self.size
    }

    async fn get(&self) -> file_store::Result<file_store::SourceChunkContent> {
        let buffer = self
            .repo
            .read(&self.content_id, Some(self.size))
            .await
            .map_err(|e| file_store::Error::Other(e.to_string()))?;

        Ok(file_store::SourceChunkContent {
            offset: self.offset,
            size: self.size,
            hash: self.content_id.clone(),
            bytes: buffer,
        })
    }
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

        // Check what exists at the store path
        let entry = dest.get_entry(&store_path).await?;
        let store_entries = match entry {
            Some(StoreDirectoryEntry::Dir(_)) => {
                // Directory exists - list its contents for merging
                dest.list_directory(&store_path).await?
            }
            None => {
                // Nothing exists - create the directory
                actions.mkdir(&store_path).await?;
                None
            }
            Some(StoreDirectoryEntry::File(_)) => {
                // File exists where we need a directory - remove and create
                actions.rm(&store_path).await?;
                actions.mkdir(&store_path).await?;
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
        })
    }

    /// Execute the download, synchronizing the repo directory to the store.
    ///
    /// This is the core merge algorithm that walks through both the repo entries
    /// and store entries in sorted order, determining what changes need to be made.
    pub async fn download_repo_to_store(mut self) -> Result<()> {
        let mut repo_entries = self.repo.list_directory_entries(&self.repo_directory_id).await?;

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

        Ok(())
    }

    /// Download a repo entry that doesn't exist in the store.
    async fn download_repo_entry(&self, entry: &RepoDirectoryEntry) -> Result<()> {
        match entry {
            RepoDirectoryEntry::Directory(dir_entry) => {
                let child_path = self.store_path.join(&dir_entry.name);
                self.actions.mkdir(&child_path).await?;
                let child = self
                    .for_child_with_store_entries(&dir_entry.name, dir_entry.directory.clone(), &None)
                    .await?;
                Box::pin(child.download_repo_to_store()).await?;
            }
            RepoDirectoryEntry::File(file_entry) => {
                let file_path = self.store_path.join(&file_entry.name);
                self.actions
                    .download_file(&file_path, &file_entry.file, file_entry.executable)
                    .await?;
            }
        }
        Ok(())
    }

    /// Remove a store entry that doesn't exist in the repo.
    async fn remove_store_entry(&self, entry: &StoreDirectoryEntry) -> Result<()> {
        let entry_path = self.store_path.join(entry_name_store(entry));
        self.actions.rm(&entry_path).await?;
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
                Box::pin(child.download_repo_to_store()).await?;
            }
            // Dir -> File: replace store file with repo directory
            (RepoDirectoryEntry::Directory(dir_entry), StoreDirectoryEntry::File(_)) => {
                let child_path = self.store_path.join(&dir_entry.name);
                self.actions.rm(&child_path).await?;
                self.actions.mkdir(&child_path).await?;
                let child = self
                    .for_child_with_store_entries(&dir_entry.name, dir_entry.directory.clone(), &None)
                    .await?;
                Box::pin(child.download_repo_to_store()).await?;
            }
            // File -> Dir: replace store directory with repo file
            (RepoDirectoryEntry::File(file_entry), StoreDirectoryEntry::Dir(_)) => {
                let file_path = self.store_path.join(&file_entry.name);
                self.actions.rm(&file_path).await?;
                self.actions
                    .download_file(&file_path, &file_entry.file, file_entry.executable)
                    .await?;
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
                        self.actions
                            .set_executable(&file_path, file_entry.executable)
                            .await?;
                    }
                } else {
                    // File doesn't match - replace it
                    self.actions.rm(&file_path).await?;
                    self.actions
                        .download_file(&file_path, &file_entry.file, file_entry.executable)
                        .await?;
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
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;
    use crate::caches::{CacheError, NoopFileStoreCache};
    use crate::repository::{
        ChunkFilePart, DirEntry as RepoDirEntry, Directory, DirectoryType,
        File, FileEntry as RepoFileEntry, FileType, FilePart, RepoObject,
    };
    use crate::util::ManagedBuffers;

    use async_trait::async_trait;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use crate::caches::RepoCache;

    /// Simple test cache for Repo.
    struct TestRepoCache;

    #[async_trait]
    impl RepoCache for TestRepoCache {
        async fn object_exists(
            &self,
            _id: &ObjectId,
        ) -> std::result::Result<bool, CacheError> {
            Ok(false)
        }
        async fn set_object_exists(
            &self,
            _id: &ObjectId,
        ) -> std::result::Result<(), CacheError> {
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
        async fn rm(&self, path: &Path) -> Result<()> {
            self.actions
                .lock()
                .unwrap()
                .push(format!("rm:{}", path.display()));
            Ok(())
        }

        async fn mkdir(&self, path: &Path) -> Result<()> {
            self.actions
                .lock()
                .unwrap()
                .push(format!("mkdir:{}", path.display()));
            Ok(())
        }

        async fn download_file(
            &self,
            path: &Path,
            file_id: &ObjectId,
            executable: bool,
        ) -> Result<()> {
            self.actions.lock().unwrap().push(format!(
                "download:{}:{}:{}",
                path.display(),
                file_id,
                executable
            ));
            Ok(())
        }

        async fn set_executable(&self, path: &Path, executable: bool) -> Result<()> {
            self.actions
                .lock()
                .unwrap()
                .push(format!("set_executable:{}:{}", path.display(), executable));
            Ok(())
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

        async fn list_directory(
            &self,
            path: &Path,
        ) -> file_store::Result<Option<DirectoryList>> {
            let path_str = path.to_string_lossy().to_string();
            match self.directories.get(&path_str) {
                Some(entries) => {
                    let helper =
                        file_store::ScanIgnoreHelper::new();
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
            _chunks: file_store::SourceChunks,
            _executable: bool,
        ) -> file_store::Result<()> {
            Ok(())
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

        let download = DownloadRepoToStore::new(
            repo,
            root_dir_id,
            &store,
            PathBuf::from("dest"),
            &actions,
        )
        .await
        .unwrap();

        download.download_repo_to_store().await.unwrap();

        let recorded = actions.get_actions();
        assert!(recorded.contains(&"mkdir:dest".to_string()));
        assert!(recorded.contains(&"mkdir:dest/dir_b".to_string()));
        assert!(recorded
            .iter()
            .any(|a| a.starts_with("download:dest/dir_b/file_c.txt:")));
        assert!(recorded
            .iter()
            .any(|a| a.starts_with("download:dest/file_a.txt:")));
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

        let download = DownloadRepoToStore::new(
            repo,
            root_dir_id,
            &store,
            PathBuf::from("dest"),
            &actions,
        )
        .await
        .unwrap();

        download.download_repo_to_store().await.unwrap();

        let recorded = actions.get_actions();
        // file_b.txt should be removed since it's not in the repo
        assert!(recorded.contains(&"rm:dest/file_b.txt".to_string()));
        // file_a.txt has no fingerprint match so it gets replaced
        assert!(recorded.contains(&"rm:dest/file_a.txt".to_string()));
        assert!(recorded
            .iter()
            .any(|a| a.starts_with("download:dest/file_a.txt:")));
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

        let download = DownloadRepoToStore::new(
            repo,
            root_dir_id,
            &store,
            PathBuf::from("dest"),
            &actions,
        )
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

        let download = DownloadRepoToStore::new(
            repo,
            root_dir_id,
            &store,
            PathBuf::from("dest"),
            &actions,
        )
        .await
        .unwrap();

        download.download_repo_to_store().await.unwrap();

        let recorded = actions.get_actions();
        assert!(recorded.contains(&"rm:dest/item_a".to_string()));
        assert!(recorded
            .iter()
            .any(|a| a.starts_with("download:dest/item_a:") && a.ends_with(":true")));
    }
}
