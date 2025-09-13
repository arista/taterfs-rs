use async_trait::async_trait;
use bytes::Bytes;

use crate::repo::repo_model::ObjectId;

// ---------- Trait ----------

#[async_trait(?Send)]
pub trait RepoFileBuilder {
    async fn add_chunk(&mut self, buf: Bytes) -> anyhow::Result<()>;
    async fn complete(&mut self) -> anyhow::Result<RepoFileBuilderResult>;
}

pub struct RepoFileBuilderResult {
    pub file: ObjectId,
    pub size: u64,
}
