//! Cache database built on top of KeyValueDb and ObjectCacheDb.
//!
//! Provides higher-level cache operations for repositories and file stores.

use std::sync::Arc;

use percent_encoding::{NON_ALPHANUMERIC, percent_encode};
use tokio::sync::Mutex;

use crate::repository::RepoObject;

use super::key_value_db::{KeyValueDb, KeyValueDbError, Result};
use super::object_cache_db::ObjectCacheDb;

// =============================================================================
// FingerprintedFileInfo
// =============================================================================

/// Information about a file identified by its fingerprint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FingerprintedFileInfo {
    /// The fingerprint that identifies this version of the file.
    pub fingerprint: String,
    /// The object ID of the file in the repository.
    pub object_id: String,
}

/// Database ID type (u64).
pub type DbId = u64;

/// A path entry storing the parent and name IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathEntry {
    /// The parent path ID, or None for root-level entries.
    pub parent_id: Option<DbId>,
    /// The name ID for this path component.
    pub name_id: DbId,
}

/// Information about a local chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalChunk {
    /// The chunk's object ID (hash).
    pub chunk_id: String,
    /// Offset within the file where this chunk starts.
    pub offset: u64,
    /// Length of the chunk in bytes.
    pub length: u64,
}

/// A possible location where a chunk might be found locally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PossibleLocalChunk {
    /// The path ID of the file containing the chunk.
    pub path_id: DbId,
    /// Offset within the file where this chunk starts.
    pub offset: u64,
    /// Length of the chunk in bytes.
    pub length: u64,
}

/// Block size for ID allocation.
const ID_BLOCK_SIZE: u64 = 100;

// =============================================================================
// Key Prefixes
// =============================================================================

const KEY_NEXT_ID: &[u8] = b"next-id";
const KEY_REPO_ID_PREFIX: &[u8] = b"repository-id-by-uuid/";
const KEY_EXISTS_PREFIX: &[u8] = b"ex/";
const KEY_FULLY_STORED_PREFIX: &[u8] = b"fs/";
const KEY_FILESTORE_ID_PREFIX: &[u8] = b"filestore-id-by-url/";
const KEY_NAME_ID_PREFIX: &[u8] = b"na/";
const KEY_NAME_REVERSE_PREFIX: &[u8] = b"nn/";
const KEY_PATH_ENTRY_PREFIX: &[u8] = b"pa/";
const KEY_PATH_PARENT_PREFIX: &[u8] = b"pp/";
const KEY_FILE_INFO_PREFIX: &[u8] = b"fi/";
const KEY_LOCAL_CHUNK_PREFIX: &[u8] = b"lc/";
const KEY_LOCAL_FILE_PREFIX: &[u8] = b"lf/";

// =============================================================================
// CacheDb
// =============================================================================

/// A cache database providing repository and file store caching.
///
/// Built on top of a KeyValueDb and ObjectCacheDb, provides:
/// - Repository existence and fully-stored tracking
/// - Repository object caching (via ObjectCacheDb)
/// - File store fingerprint caching
/// - ID allocation with block caching
pub struct CacheDb {
    db: Arc<dyn KeyValueDb>,
    object_cache: Arc<dyn ObjectCacheDb>,
    id_allocator: Mutex<IdAllocator>,
}

struct IdAllocator {
    /// Next ID to return from current block.
    next: u64,
    /// Upper bound of current block (exclusive).
    block_end: u64,
}

impl CacheDb {
    /// Create a new cache database.
    pub fn new(db: Arc<dyn KeyValueDb>, object_cache: Arc<dyn ObjectCacheDb>) -> Self {
        Self {
            db,
            object_cache,
            id_allocator: Mutex::new(IdAllocator {
                next: 0,
                block_end: 0,
            }),
        }
    }

    // =========================================================================
    // ID Generation
    // =========================================================================

    /// Generate a new unique database ID.
    ///
    /// Uses block allocation to minimize transactions.
    pub async fn generate_next_id(&self) -> Result<DbId> {
        let mut allocator = self.id_allocator.lock().await;

        // If we have IDs in the current block, return one
        if allocator.next < allocator.block_end {
            let id = allocator.next;
            allocator.next += 1;
            return Ok(id);
        }

        // Need to allocate a new block
        let mut txn = self.db.transaction().await?;

        // Get current next_id
        let current = match txn.get(KEY_NEXT_ID).await? {
            Some(bytes) => parse_u64(&bytes)?,
            None => 1, // Start at 1 if not set
        };

        // Reserve a block
        let new_next = current + ID_BLOCK_SIZE;
        txn.set(KEY_NEXT_ID.to_vec(), new_next.to_string().into_bytes())
            .await;
        txn.commit().await?;

        // Update local allocator
        allocator.next = current + 1;
        allocator.block_end = new_next;

        Ok(current)
    }

    // =========================================================================
    // Repository ID
    // =========================================================================

    /// Get the database ID for a repository UUID.
    pub async fn get_repository_id(&self, uuid: &str) -> Result<Option<DbId>> {
        let key = repo_id_key(uuid);
        match self.db.get(&key).await? {
            Some(bytes) => Ok(Some(parse_u64(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Set the database ID for a repository UUID.
    pub async fn set_repository_id(&self, uuid: &str, id: DbId) -> Result<()> {
        let key = repo_id_key(uuid);
        let mut writes = self.db.write().await?;
        writes.set(key, id.to_string().into_bytes()).await;
        writes.flush().await
    }

    /// Get or create a database ID for a repository UUID.
    pub async fn get_or_create_repository_id(&self, uuid: &str) -> Result<DbId> {
        // Try to get existing
        if let Some(id) = self.get_repository_id(uuid).await? {
            return Ok(id);
        }

        // Create new - use transaction for atomicity
        let key = repo_id_key(uuid);
        let mut txn = self.db.transaction().await?;

        // Double-check it wasn't created while we were waiting
        if let Some(bytes) = txn.get(&key).await? {
            return parse_u64(&bytes);
        }

        let id = self.generate_next_id().await?;
        txn.set(key, id.to_string().into_bytes()).await;
        txn.commit().await?;

        Ok(id)
    }

    // =========================================================================
    // Object Existence
    // =========================================================================

    /// Check if an object exists in a repository.
    pub async fn get_exists(&self, repo_id: DbId, object_id: &str) -> Result<bool> {
        let key = exists_key(repo_id, object_id);
        self.db.exists(&key).await
    }

    /// Mark an object as existing in a repository.
    pub async fn set_exists(&self, repo_id: DbId, object_id: &str) -> Result<()> {
        let key = exists_key(repo_id, object_id);
        let mut writes = self.db.write().await?;
        writes.set(key, Vec::new()).await;
        writes.flush().await
    }

    // =========================================================================
    // Fully Stored
    // =========================================================================

    /// Check if an object is fully stored (it and all reachable objects exist).
    pub async fn get_fully_stored(&self, repo_id: DbId, object_id: &str) -> Result<bool> {
        let key = fully_stored_key(repo_id, object_id);
        self.db.exists(&key).await
    }

    /// Mark an object as fully stored.
    pub async fn set_fully_stored(&self, repo_id: DbId, object_id: &str) -> Result<()> {
        let key = fully_stored_key(repo_id, object_id);
        let mut writes = self.db.write().await?;
        writes.set(key, Vec::new()).await;
        writes.flush().await
    }

    // =========================================================================
    // Repository Objects
    // =========================================================================

    /// Get a cached repository object.
    pub async fn get_object(&self, object_id: &str) -> Result<Option<RepoObject>> {
        self.object_cache
            .get_object(object_id)
            .await
            .map_err(|e| KeyValueDbError::Encoding(e.to_string()))
    }

    /// Cache a repository object.
    pub async fn set_object(&self, object_id: &str, obj: &RepoObject) -> Result<()> {
        self.object_cache
            .set_object(object_id, obj)
            .await
            .map_err(|e| KeyValueDbError::Encoding(e.to_string()))
    }

    // =========================================================================
    // Filestore ID
    // =========================================================================

    /// Get the database ID for a filestore URL.
    pub async fn get_filestore_id(&self, url: &str) -> Result<Option<DbId>> {
        let key = filestore_id_key(url);
        match self.db.get(&key).await? {
            Some(bytes) => Ok(Some(parse_u64(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Set the database ID for a filestore URL.
    pub async fn set_filestore_id(&self, url: &str, id: DbId) -> Result<()> {
        let key = filestore_id_key(url);
        let mut writes = self.db.write().await?;
        writes.set(key, id.to_string().into_bytes()).await;
        writes.flush().await
    }

    /// Get or create a database ID for a filestore URL.
    pub async fn get_or_create_filestore_id(&self, url: &str) -> Result<DbId> {
        // Try to get existing
        if let Some(id) = self.get_filestore_id(url).await? {
            return Ok(id);
        }

        // Create new
        let key = filestore_id_key(url);
        let mut txn = self.db.transaction().await?;

        if let Some(bytes) = txn.get(&key).await? {
            return parse_u64(&bytes);
        }

        let id = self.generate_next_id().await?;
        txn.set(key, id.to_string().into_bytes()).await;
        txn.commit().await?;

        Ok(id)
    }

    // =========================================================================
    // Name ID (for path component interning) - Global, not filestore-scoped
    // =========================================================================

    /// Get the ID for a name (global).
    pub async fn get_name_id(&self, name: &str) -> Result<Option<DbId>> {
        let key = name_id_key(name);
        match self.db.get(&key).await? {
            Some(bytes) => Ok(Some(parse_u64(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Get the name for a name ID (reverse lookup).
    pub async fn get_name(&self, name_id: DbId) -> Result<Option<String>> {
        let key = name_reverse_key(name_id);
        match self.db.get(&key).await? {
            Some(bytes) => {
                let s = String::from_utf8(bytes)
                    .map_err(|e| KeyValueDbError::Encoding(e.to_string()))?;
                Ok(Some(s))
            }
            None => Ok(None),
        }
    }

    /// Set the ID for a name (global).
    ///
    /// Also writes reverse lookup (name_id -> name).
    pub async fn set_name_id(&self, name: &str, id: DbId) -> Result<()> {
        let na_key = name_id_key(name);
        let nn_key = name_reverse_key(id);

        let mut writes = self.db.write().await?;
        writes.set(na_key, id.to_string().into_bytes()).await;
        writes.set(nn_key, name.as_bytes().to_vec()).await;
        writes.flush().await
    }

    /// Get or create an ID for a name (global).
    pub async fn get_or_create_name_id(&self, name: &str) -> Result<DbId> {
        if let Some(id) = self.get_name_id(name).await? {
            return Ok(id);
        }

        let na_key = name_id_key(name);
        let mut txn = self.db.transaction().await?;

        if let Some(bytes) = txn.get(&na_key).await? {
            return parse_u64(&bytes);
        }

        let id = self.generate_next_id().await?;

        // Write both forward and reverse lookups
        let nn_key = name_reverse_key(id);
        txn.set(na_key, id.to_string().into_bytes()).await;
        txn.set(nn_key, name.as_bytes().to_vec()).await;
        txn.commit().await?;

        Ok(id)
    }

    // =========================================================================
    // Path Entry ID - Global, not filestore-scoped
    // =========================================================================

    /// Get the path ID for a path entry (global).
    pub async fn get_path_entry_id(
        &self,
        parent: Option<DbId>,
        name_id: DbId,
    ) -> Result<Option<DbId>> {
        let key = path_entry_key(parent, name_id);
        match self.db.get(&key).await? {
            Some(bytes) => Ok(Some(parse_u64(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Set the path ID for a path entry (global).
    ///
    /// Also writes to pp/ prefix for path reconstruction.
    pub async fn set_path_entry_id(
        &self,
        parent: Option<DbId>,
        name_id: DbId,
        path_id: DbId,
    ) -> Result<()> {
        let pa_key = path_entry_key(parent, name_id);
        let pp_key = path_parent_key(path_id);
        let pp_value = path_parent_value(parent, name_id);

        let mut writes = self.db.write().await?;
        writes.set(pa_key, path_id.to_string().into_bytes()).await;
        writes.set(pp_key, pp_value.into_bytes()).await;
        writes.flush().await
    }

    /// Get or create a path ID for a path entry (global).
    pub async fn get_or_create_path_entry_id(
        &self,
        parent: Option<DbId>,
        name_id: DbId,
    ) -> Result<DbId> {
        if let Some(id) = self.get_path_entry_id(parent, name_id).await? {
            return Ok(id);
        }

        let pa_key = path_entry_key(parent, name_id);
        let mut txn = self.db.transaction().await?;

        if let Some(bytes) = txn.get(&pa_key).await? {
            return parse_u64(&bytes);
        }

        let id = self.generate_next_id().await?;

        // Write both pa/ and pp/ entries
        let pp_key = path_parent_key(id);
        let pp_value = path_parent_value(parent, name_id);

        txn.set(pa_key, id.to_string().into_bytes()).await;
        txn.set(pp_key, pp_value.into_bytes()).await;
        txn.commit().await?;

        Ok(id)
    }

    /// Get or create a path ID for a path entry by name string (global).
    ///
    /// Convenience method that first gets/creates the name_id.
    pub async fn get_or_create_path_entry_id_by_name(
        &self,
        parent: Option<DbId>,
        name: &str,
    ) -> Result<DbId> {
        let name_id = self.get_or_create_name_id(name).await?;
        self.get_or_create_path_entry_id(parent, name_id).await
    }

    /// Get the path entry (parent_id, name_id) for a path ID.
    pub async fn get_path_entry(&self, path_id: DbId) -> Result<Option<PathEntry>> {
        let key = path_parent_key(path_id);
        match self.db.get(&key).await? {
            Some(bytes) => {
                let s = String::from_utf8(bytes)
                    .map_err(|e| KeyValueDbError::Encoding(e.to_string()))?;
                let (parent_id, name_id) = parse_path_parent_value(&s)?;
                Ok(Some(PathEntry { parent_id, name_id }))
            }
            None => Ok(None),
        }
    }

    /// Get a path ID for a full path string (global), creating intermediate entries as needed.
    ///
    /// Returns `None` for empty/root paths (e.g., "" or "/").
    pub async fn get_path_id(&self, path: &str) -> Result<Option<DbId>> {
        let components: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        let mut parent: Option<DbId> = None;

        for component in components {
            parent = Some(
                self.get_or_create_path_entry_id_by_name(parent, component)
                    .await?,
            );
        }

        // For empty/root path, return None
        Ok(parent)
    }

    /// Reconstruct the full path string from a path ID.
    pub async fn get_path(&self, path_id: DbId) -> Result<Option<std::path::PathBuf>> {
        let mut components: Vec<String> = Vec::new();
        let mut current_id = Some(path_id);

        while let Some(id) = current_id {
            match self.get_path_entry(id).await? {
                Some(entry) => {
                    // Get the name for this name_id via reverse lookup
                    match self.get_name(entry.name_id).await? {
                        Some(name) => components.push(name),
                        None => return Ok(None), // Missing name, path is invalid
                    }
                    current_id = entry.parent_id;
                }
                None => return Ok(None), // Missing path entry
            }
        }

        components.reverse();
        Ok(Some(std::path::PathBuf::from(components.join("/"))))
    }

    // =========================================================================
    // Fingerprinted File Info
    // =========================================================================

    /// Get fingerprinted file info.
    pub async fn get_fingerprinted_file_info(
        &self,
        filestore_id: DbId,
        path_id: DbId,
    ) -> Result<Option<FingerprintedFileInfo>> {
        let key = file_info_key(filestore_id, path_id);
        match self.db.get(&key).await? {
            Some(bytes) => {
                let s = String::from_utf8(bytes)
                    .map_err(|e| KeyValueDbError::Encoding(e.to_string()))?;
                let parts: Vec<&str> = s.splitn(2, '|').collect();
                if parts.len() == 2 {
                    Ok(Some(FingerprintedFileInfo {
                        fingerprint: parts[0].to_string(),
                        object_id: parts[1].to_string(),
                    }))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// Set fingerprinted file info.
    pub async fn set_fingerprinted_file_info(
        &self,
        filestore_id: DbId,
        path_id: DbId,
        info: &FingerprintedFileInfo,
    ) -> Result<()> {
        let key = file_info_key(filestore_id, path_id);
        let value = format!("{}|{}", info.fingerprint, info.object_id);
        let mut writes = self.db.write().await?;
        writes.set(key, value.into_bytes()).await;
        writes.flush().await
    }

    // =========================================================================
    // Local Chunks
    // =========================================================================

    /// Record that a chunk can be found at a specific location in a file.
    ///
    /// Writes to both lc/ (for chunk lookup) and lf/ (for file-based invalidation).
    pub async fn set_local_chunk(&self, path_id: DbId, chunk: &LocalChunk) -> Result<()> {
        let lc_key = local_chunk_key(&chunk.chunk_id, path_id, chunk.offset, chunk.length);
        let lf_key = local_file_key(path_id, chunk.offset, &chunk.chunk_id);

        let mut writes = self.db.write().await?;
        writes.set(lc_key, Vec::new()).await;
        writes.set(lf_key, Vec::new()).await;
        writes.flush().await
    }

    /// List possible locations where a chunk might be found locally.
    ///
    /// Returns up to 256 entries. Does not page through results since the
    /// underlying data may change during iteration.
    pub async fn list_possible_local_chunks(
        &self,
        chunk_id: &str,
    ) -> Result<Vec<PossibleLocalChunk>> {
        let prefix = local_chunk_prefix(chunk_id);
        let mut keys = self.db.list_keys(&prefix).await?;

        let mut results = Vec::with_capacity(256);
        while results.len() < 256 {
            match keys.next().await? {
                Some(entry) => {
                    // Parse key: lc/{chunk_id}/{path_id}/{offset}/{length}
                    if let Some(chunk) = parse_local_chunk_key(&entry.key, &prefix) {
                        results.push(chunk);
                    }
                }
                None => break,
            }
        }

        Ok(results)
    }

    /// Invalidate all local chunk entries for a path.
    ///
    /// Calls both file and directory invalidation since we don't know which type the path is.
    /// Uses `Box::pin` internally to handle recursive async calls.
    pub fn invalidate_local_chunks(
        &self,
        path_id: DbId,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.invalidate_local_chunk_file(path_id).await?;
            self.invalidate_local_chunk_directory(path_id).await?;
            Ok(())
        })
    }

    /// Invalidate local chunk entries for a file.
    ///
    /// Removes all lf/ and corresponding lc/ entries for the given path.
    pub async fn invalidate_local_chunk_file(&self, path_id: DbId) -> Result<()> {
        let prefix = local_file_prefix(path_id);

        loop {
            // Get a batch of up to 256 entries
            let mut keys = self.db.list_keys(&prefix).await?;
            let mut batch: Vec<(Vec<u8>, String, u64)> = Vec::with_capacity(256);

            while batch.len() < 256 {
                match keys.next().await? {
                    Some(entry) => {
                        // Parse key: lf/{path_id}/{offset}/{chunk_id}
                        if let Some((offset, chunk_id)) =
                            parse_local_file_key(&entry.key, &prefix)
                        {
                            batch.push((entry.key, chunk_id, offset));
                        }
                    }
                    None => break,
                }
            }

            if batch.is_empty() {
                break;
            }

            // Delete the entries
            let mut writes = self.db.write().await?;
            for (lf_key, chunk_id, offset) in batch {
                // Also need length to construct lc key - get it from somewhere
                // Actually, we can list lc/ entries for this chunk and find matching path_id/offset
                // Or we could store length in lf/ value... let's just delete by prefix scan for now

                // Delete lf/ entry
                writes.del(lf_key).await;

                // For lc/, we need to scan for entries matching this path_id and offset
                // This is inefficient but correct. A better approach would store length in lf/ value.
                let lc_prefix = local_chunk_prefix(&chunk_id);
                let mut lc_keys = self.db.list_keys(&lc_prefix).await?;
                while let Some(lc_entry) = lc_keys.next().await? {
                    if let Some(chunk) = parse_local_chunk_key(&lc_entry.key, &lc_prefix)
                        && chunk.path_id == path_id
                        && chunk.offset == offset
                    {
                        writes.del(lc_entry.key).await;
                        break;
                    }
                }
            }
            writes.flush().await?;
        }

        Ok(())
    }

    /// Invalidate local chunk entries for a directory and all its descendants.
    ///
    /// Recursively invalidates all children found via pa/{parent_path_id}/.
    pub async fn invalidate_local_chunk_directory(&self, parent_path_id: DbId) -> Result<()> {
        let prefix = path_entry_prefix(Some(parent_path_id));

        loop {
            // Get a batch of up to 16 child paths
            let mut entries = self.db.list_entries(&prefix).await?;
            let mut batch: Vec<DbId> = Vec::with_capacity(16);

            while batch.len() < 16 {
                match entries.next().await? {
                    Some(entry) => {
                        // Value is the child path_id
                        if let Ok(child_path_id) = parse_u64(&entry.value) {
                            batch.push(child_path_id);
                        }
                    }
                    None => break,
                }
            }

            if batch.is_empty() {
                break;
            }

            // Recursively invalidate each child
            for child_path_id in batch {
                self.invalidate_local_chunks(child_path_id).await?;
            }
        }

        Ok(())
    }

    /// Flush any pending writes.
    pub async fn flush(&self) -> Result<()> {
        // The underlying CachingKeyValueDb handles periodic flushing
        // This is for explicit flush requests
        Ok(())
    }
}

// =============================================================================
// Key Construction Helpers
// =============================================================================

fn encode_uri_component(s: &str) -> String {
    percent_encode(s.as_bytes(), NON_ALPHANUMERIC).to_string()
}

fn repo_id_key(uuid: &str) -> Vec<u8> {
    let mut key = KEY_REPO_ID_PREFIX.to_vec();
    key.extend(encode_uri_component(uuid).as_bytes());
    key
}

fn exists_key(repo_id: DbId, object_id: &str) -> Vec<u8> {
    let mut key = KEY_EXISTS_PREFIX.to_vec();
    key.extend(repo_id.to_string().as_bytes());
    key.push(b'/');
    key.extend(object_id.as_bytes());
    key
}

fn fully_stored_key(repo_id: DbId, object_id: &str) -> Vec<u8> {
    let mut key = KEY_FULLY_STORED_PREFIX.to_vec();
    key.extend(repo_id.to_string().as_bytes());
    key.push(b'/');
    key.extend(object_id.as_bytes());
    key
}

fn filestore_id_key(url: &str) -> Vec<u8> {
    let mut key = KEY_FILESTORE_ID_PREFIX.to_vec();
    key.extend(encode_uri_component(url).as_bytes());
    key
}

fn name_id_key(name: &str) -> Vec<u8> {
    let mut key = KEY_NAME_ID_PREFIX.to_vec();
    key.extend(encode_uri_component(name).as_bytes());
    key
}

fn name_reverse_key(name_id: DbId) -> Vec<u8> {
    let mut key = KEY_NAME_REVERSE_PREFIX.to_vec();
    key.extend(name_id.to_string().as_bytes());
    key
}

fn path_entry_key(parent: Option<DbId>, name_id: DbId) -> Vec<u8> {
    let mut key = KEY_PATH_ENTRY_PREFIX.to_vec();
    match parent {
        Some(id) => key.extend(id.to_string().as_bytes()),
        None => key.extend(b"root"),
    }
    key.push(b'/');
    key.extend(name_id.to_string().as_bytes());
    key
}

fn path_entry_prefix(parent: Option<DbId>) -> Vec<u8> {
    let mut key = KEY_PATH_ENTRY_PREFIX.to_vec();
    match parent {
        Some(id) => key.extend(id.to_string().as_bytes()),
        None => key.extend(b"root"),
    }
    key.push(b'/');
    key
}

fn path_parent_key(path_id: DbId) -> Vec<u8> {
    let mut key = KEY_PATH_PARENT_PREFIX.to_vec();
    key.extend(path_id.to_string().as_bytes());
    key
}

fn path_parent_value(parent: Option<DbId>, name_id: DbId) -> String {
    match parent {
        Some(id) => format!("{}|{}", id, name_id),
        None => format!("root|{}", name_id),
    }
}

fn parse_path_parent_value(s: &str) -> Result<(Option<DbId>, DbId)> {
    let parts: Vec<&str> = s.splitn(2, '|').collect();
    if parts.len() != 2 {
        return Err(KeyValueDbError::Encoding(format!(
            "invalid path parent value: {}",
            s
        )));
    }

    let parent = if parts[0] == "root" {
        None
    } else {
        Some(
            parts[0]
                .parse()
                .map_err(|e: std::num::ParseIntError| KeyValueDbError::Encoding(e.to_string()))?,
        )
    };

    let name_id = parts[1]
        .parse()
        .map_err(|e: std::num::ParseIntError| KeyValueDbError::Encoding(e.to_string()))?;

    Ok((parent, name_id))
}

fn local_chunk_key(chunk_id: &str, path_id: DbId, offset: u64, length: u64) -> Vec<u8> {
    let mut key = KEY_LOCAL_CHUNK_PREFIX.to_vec();
    key.extend(chunk_id.as_bytes());
    key.push(b'/');
    key.extend(path_id.to_string().as_bytes());
    key.push(b'/');
    key.extend(offset.to_string().as_bytes());
    key.push(b'/');
    key.extend(length.to_string().as_bytes());
    key
}

fn local_chunk_prefix(chunk_id: &str) -> Vec<u8> {
    let mut key = KEY_LOCAL_CHUNK_PREFIX.to_vec();
    key.extend(chunk_id.as_bytes());
    key.push(b'/');
    key
}

fn local_file_key(path_id: DbId, offset: u64, chunk_id: &str) -> Vec<u8> {
    let mut key = KEY_LOCAL_FILE_PREFIX.to_vec();
    key.extend(path_id.to_string().as_bytes());
    key.push(b'/');
    key.extend(offset.to_string().as_bytes());
    key.push(b'/');
    key.extend(chunk_id.as_bytes());
    key
}

fn local_file_prefix(path_id: DbId) -> Vec<u8> {
    let mut key = KEY_LOCAL_FILE_PREFIX.to_vec();
    key.extend(path_id.to_string().as_bytes());
    key.push(b'/');
    key
}

/// Parse a local chunk key: lc/{chunk_id}/{path_id}/{offset}/{length}
/// The prefix (lc/{chunk_id}/) is already stripped.
fn parse_local_chunk_key(key: &[u8], prefix: &[u8]) -> Option<PossibleLocalChunk> {
    if !key.starts_with(prefix) {
        return None;
    }

    let suffix = &key[prefix.len()..];
    let s = std::str::from_utf8(suffix).ok()?;
    let parts: Vec<&str> = s.split('/').collect();

    if parts.len() != 3 {
        return None;
    }

    let path_id: DbId = parts[0].parse().ok()?;
    let offset: u64 = parts[1].parse().ok()?;
    let length: u64 = parts[2].parse().ok()?;

    Some(PossibleLocalChunk {
        path_id,
        offset,
        length,
    })
}

/// Parse a local file key: lf/{path_id}/{offset}/{chunk_id}
/// The prefix (lf/{path_id}/) is already stripped.
/// Returns (offset, chunk_id).
fn parse_local_file_key(key: &[u8], prefix: &[u8]) -> Option<(u64, String)> {
    if !key.starts_with(prefix) {
        return None;
    }

    let suffix = &key[prefix.len()..];
    let s = std::str::from_utf8(suffix).ok()?;
    let parts: Vec<&str> = s.splitn(2, '/').collect();

    if parts.len() != 2 {
        return None;
    }

    let offset: u64 = parts[0].parse().ok()?;
    let chunk_id = parts[1].to_string();

    Some((offset, chunk_id))
}

fn file_info_key(filestore_id: DbId, path_id: DbId) -> Vec<u8> {
    let mut key = KEY_FILE_INFO_PREFIX.to_vec();
    key.extend(filestore_id.to_string().as_bytes());
    key.push(b'/');
    key.extend(path_id.to_string().as_bytes());
    key
}

fn parse_u64(bytes: &[u8]) -> Result<u64> {
    let s = std::str::from_utf8(bytes).map_err(|e| KeyValueDbError::Encoding(e.to_string()))?;
    s.parse()
        .map_err(|e: std::num::ParseIntError| KeyValueDbError::Encoding(e.to_string()))
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::super::lmdb_key_value_db::LmdbKeyValueDb;
    use super::super::object_cache_db::NoopObjectCacheDb;
    use super::*;
    use tempfile::TempDir;

    fn create_test_db() -> (TempDir, CacheDb) {
        let temp_dir = TempDir::new().unwrap();
        let lmdb = Arc::new(LmdbKeyValueDb::new(temp_dir.path()).unwrap());
        let object_cache = Arc::new(NoopObjectCacheDb);
        let cache_db = CacheDb::new(lmdb, object_cache);
        (temp_dir, cache_db)
    }

    #[tokio::test]
    async fn test_id_generation() {
        let (_temp, db) = create_test_db();

        let id1 = db.generate_next_id().await.unwrap();
        let id2 = db.generate_next_id().await.unwrap();
        let id3 = db.generate_next_id().await.unwrap();

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    #[tokio::test]
    async fn test_repository_id() {
        let (_temp, db) = create_test_db();

        let uuid = "test-uuid-12345";

        // Initially not set
        assert!(db.get_repository_id(uuid).await.unwrap().is_none());

        // Get or create
        let id1 = db.get_or_create_repository_id(uuid).await.unwrap();
        assert!(id1 > 0);

        // Should return same ID
        let id2 = db.get_or_create_repository_id(uuid).await.unwrap();
        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn test_exists_and_fully_stored() {
        let (_temp, db) = create_test_db();

        let repo_id = db.get_or_create_repository_id("test-repo").await.unwrap();
        let object_id = "abc123def456";

        // Initially not set
        assert!(!db.get_exists(repo_id, object_id).await.unwrap());
        assert!(!db.get_fully_stored(repo_id, object_id).await.unwrap());

        // Set exists
        db.set_exists(repo_id, object_id).await.unwrap();
        assert!(db.get_exists(repo_id, object_id).await.unwrap());
        assert!(!db.get_fully_stored(repo_id, object_id).await.unwrap());

        // Set fully stored
        db.set_fully_stored(repo_id, object_id).await.unwrap();
        assert!(db.get_fully_stored(repo_id, object_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_path_id() {
        let (_temp, db) = create_test_db();

        // Paths are now global (not filestore-scoped)
        let path1 = db.get_path_id("foo/bar/baz.txt").await.unwrap().unwrap();
        let path2 = db.get_path_id("foo/bar/baz.txt").await.unwrap().unwrap();

        // Same path should get same ID
        assert_eq!(path1, path2);

        // Different path should get different ID
        let path3 = db.get_path_id("foo/bar/other.txt").await.unwrap().unwrap();
        assert_ne!(path1, path3);

        // Empty/root paths should return None
        assert!(db.get_path_id("").await.unwrap().is_none());
        assert!(db.get_path_id("/").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_path_roundtrip() {
        let (_temp, db) = create_test_db();

        // Create a path
        let path_id = db.get_path_id("foo/bar/baz.txt").await.unwrap().unwrap();

        // Reconstruct the path
        let reconstructed = db.get_path(path_id).await.unwrap().unwrap();
        assert_eq!(reconstructed, std::path::PathBuf::from("foo/bar/baz.txt"));
    }

    #[tokio::test]
    async fn test_get_path_entry() {
        let (_temp, db) = create_test_db();

        // Create a path
        let path_id = db.get_path_id("foo/bar").await.unwrap().unwrap();

        // Get the path entry
        let entry = db.get_path_entry(path_id).await.unwrap().unwrap();

        // parent_id should point to "foo"
        assert!(entry.parent_id.is_some());

        // name_id should correspond to "bar"
        let name = db.get_name(entry.name_id).await.unwrap().unwrap();
        assert_eq!(name, "bar");
    }

    #[tokio::test]
    async fn test_fingerprinted_file_info() {
        let (_temp, db) = create_test_db();

        let fs_id = db.get_or_create_filestore_id("file:///test").await.unwrap();
        let path_id = db.get_path_id("test.txt").await.unwrap().unwrap();

        // Initially not set
        assert!(
            db.get_fingerprinted_file_info(fs_id, path_id)
                .await
                .unwrap()
                .is_none()
        );

        // Set it
        let info = FingerprintedFileInfo {
            fingerprint: "12345:100:-".to_string(),
            object_id: "objectid123".to_string(),
        };
        db.set_fingerprinted_file_info(fs_id, path_id, &info)
            .await
            .unwrap();

        // Get it back
        let retrieved = db
            .get_fingerprinted_file_info(fs_id, path_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved, info);
    }

    #[tokio::test]
    async fn test_local_chunks() {
        let (_temp, db) = create_test_db();

        let path_id = db.get_path_id("data/file.bin").await.unwrap().unwrap();

        // Initially no chunks
        let chunks = db.list_possible_local_chunks("chunk123").await.unwrap();
        assert!(chunks.is_empty());

        // Add a chunk
        let chunk = LocalChunk {
            chunk_id: "chunk123".to_string(),
            offset: 0,
            length: 1024,
        };
        db.set_local_chunk(path_id, &chunk).await.unwrap();

        // Now we should find it
        let chunks = db.list_possible_local_chunks("chunk123").await.unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].path_id, path_id);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[0].length, 1024);

        // Add another chunk location for the same chunk
        let path_id2 = db.get_path_id("data/other.bin").await.unwrap().unwrap();
        let chunk2 = LocalChunk {
            chunk_id: "chunk123".to_string(),
            offset: 512,
            length: 1024,
        };
        db.set_local_chunk(path_id2, &chunk2).await.unwrap();

        // Should find both
        let chunks = db.list_possible_local_chunks("chunk123").await.unwrap();
        assert_eq!(chunks.len(), 2);
    }

    #[tokio::test]
    async fn test_invalidate_local_chunk_file() {
        let (_temp, db) = create_test_db();

        let path_id = db.get_path_id("data/file.bin").await.unwrap().unwrap();

        // Add chunks
        for i in 0..3 {
            let chunk = LocalChunk {
                chunk_id: format!("chunk{}", i),
                offset: i * 1024,
                length: 1024,
            };
            db.set_local_chunk(path_id, &chunk).await.unwrap();
        }

        // Verify chunks exist
        assert_eq!(
            db.list_possible_local_chunks("chunk0").await.unwrap().len(),
            1
        );
        assert_eq!(
            db.list_possible_local_chunks("chunk1").await.unwrap().len(),
            1
        );
        assert_eq!(
            db.list_possible_local_chunks("chunk2").await.unwrap().len(),
            1
        );

        // Invalidate the file
        db.invalidate_local_chunk_file(path_id).await.unwrap();

        // Chunks should be gone
        assert!(
            db.list_possible_local_chunks("chunk0")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            db.list_possible_local_chunks("chunk1")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            db.list_possible_local_chunks("chunk2")
                .await
                .unwrap()
                .is_empty()
        );
    }
}
