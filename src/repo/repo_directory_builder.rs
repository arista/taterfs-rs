use async_trait::async_trait;

use crate::repo::repo_model::{DirectoryEntry, ObjectId};

// ---------- Trait ----------

#[async_trait]
pub trait RepoDirectoryBuilder: Send {
    async fn add_entry(&mut self, entry: DirectoryEntry) -> anyhow::Result<()>;
    async fn complete(&mut self) -> anyhow::Result<ObjectId>;
}
