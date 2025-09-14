// Thanks ChatGPT

use super::repo_model as RM;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

/// Storage backends are dumb object stores + a single "current root" pointer.  Objects are intended to be immutable; their key is the SHA-256 of their bytes (hex string).
#[async_trait(?Send)]
pub trait RepoBackend {
    /// Does the root pointer exist at all?
    async fn current_root_exists(&self) -> Result<bool>;

    /// Read the current root pointer (ObjectId of a RepoObject::Root).
    async fn read_current_root(&self) -> Result<RM::ObjectId>;

    /// Overwrite the current root pointer to `root`.
    /// Backends that can provide atomicity SHOULD override the CAS method below
    /// and have this call delegate to it with a "blind set" (expected=None).
    async fn write_current_root(&self, root: &RM::ObjectId) -> Result<()>;

    /// True if an object with this id exists.
    async fn exists(&self, id: &RM::ObjectId) -> Result<bool>;

    /// Read an object by id. Returns the exact bytes that were written.
    async fn read(&self, id: &RM::ObjectId) -> Result<Bytes>;

    /// Write an object by id.  The id should be the sha256 of the specified bytes
    async fn write(&self, id: &RM::ObjectId, bytes: Bytes) -> Result<()>;
}
