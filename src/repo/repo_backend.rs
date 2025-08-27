use crate::repo::repo_model::{ObjectId}; // your newtype module (where ObjectId lives)
use async_trait::async_trait;
use bytes::Bytes;

#[derive(thiserror::Error, Debug)]
pub enum BackendError {
    #[error("object not found: {0}")]
    NotFound(ObjectId),

    #[error("root pointer not set")]
    RootMissing,

    #[error("conflict writing root (CAS failed)")]
    Conflict,

    #[error("hash mismatch on write for {id}: expected {expected}, computed {computed}")]
    HashMismatch {
        id: ObjectId,
        expected: String,
        computed: String,
    },

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

/// Storage backends are dumb object stores + a single "current root" pointer.
/// Objects are immutable; their key is the SHA-256 of their bytes (hex string).
///
/// Size expectations:
/// - Structural JSON objects ≤ ~10 MiB
/// - Binary chunk objects follow your CHUNK_SIZES
///
/// Semantics:
/// - `write(id, bytes)` SHOULD verify that `sha256(bytes) == id` and error otherwise
/// - `read(id)` MUST return exactly the bytes previously written for that id
/// - `write_current_root(new)` replaces the current pointer (see CAS method below)
#[async_trait]
pub trait RepoBackend: Send + Sync {
    /// Does the root pointer exist at all?
    async fn current_root_exists(&self) -> Result<bool, BackendError>;

    /// Read the current root pointer (ObjectId of a RepoObject::Root).
    async fn read_current_root(&self) -> Result<ObjectId, BackendError>;

    /// Overwrite the current root pointer to `root`.
    /// Backends that can provide atomicity SHOULD override the CAS method below
    /// and have this call delegate to it with a "blind set" (expected=None).
    async fn write_current_root(&self, root: &ObjectId) -> Result<(), BackendError>;

    /// Optional but recommended: Compare-and-swap the current root pointer.
    /// Default impl just calls `write_current_root` and is NOT atomic.
    ///
    /// - If `expected` is `Some(x)`, succeed only if the stored root equals `x`.
    /// - If `expected` is `None`, perform an unconditional set.
    async fn compare_and_swap_current_root(
        &self,
        expected: Option<&ObjectId>,
        new: &ObjectId,
    ) -> Result<(), BackendError> {
        // Non-atomic default: emulate expected=None as blind set,
        // and expected=Some(x) with a racy check+set.
        match expected {
            None => self.write_current_root(new).await,
            Some(exp) => {
                let cur = self.read_current_root().await?;
                if &cur == exp {
                    self.write_current_root(new).await
                } else {
                    Err(BackendError::Conflict)
                }
            }
        }
    }

    /// True if an object with this id exists.
    async fn exists(&self, id: &ObjectId) -> Result<bool, BackendError>;

    /// Read an object by id. Returns its exact bytes.
    async fn read(&self, id: &ObjectId) -> Result<Bytes, BackendError>;

    /// Write an object by id. Implementations SHOULD verify id == sha256(bytes).
    async fn write(&self, id: &ObjectId, bytes: Bytes) -> Result<(), BackendError>;
}
