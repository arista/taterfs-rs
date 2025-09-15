use std::rc::Rc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::FutureExt;

use super::{
    Repo, RepoBackend,
    repo_model::{self, ObjectId},
};
use crate::{prelude::*, util::ReleasedFuture};

pub struct ManagedRepo {
    backend: Rc<dyn RepoBackend>,
}

#[async_trait(?Send)]
impl Repo for ManagedRepo {
    async fn read_current_root(&self) -> ReleasedFuture<Option<repo_model::ObjectId>> {
        let backend = self.backend.clone();
        Ok(async move { Ok(backend.read_current_root().await?) }.boxed_local())
    }

    async fn write_current_root(&self, current_root: ObjectId) -> ReleasedFuture<()> {
        let backend = self.backend.clone();
        Ok(async move { Ok(backend.write_current_root(&current_root).await?) }.boxed_local())
    }

    async fn read_object(&self, id: ObjectId) -> ReleasedFuture<repo_model::RepoObject> {
        let backend = self.backend.clone();
        Ok(async move {
            let bytes = backend.read(&id).await?;
            Ok(repo_model::RepoObject::from_json_bytes(bytes)?)
        }
        .boxed_local())
    }

    async fn write_object(&self, obj: repo_model::RepoObject) -> ReleasedFuture<()> {
        let backend = self.backend.clone();
        Ok(async move {
            let bytes = repo_model::to_canonical_json_bytes(&obj);
            let id = repo_model::bytes_hash(&bytes);
            Ok(backend.write(&id, bytes).await?)
        }
        .boxed_local())
    }

    async fn read_chunk(&self, id: ObjectId) -> ReleasedFuture<Bytes> {
        let backend = self.backend.clone();
        Ok(async move { Ok(backend.read(&id).await?) }.boxed_local())
    }

    async fn write_chunk(&self, buf: Bytes) -> ReleasedFuture<()> {
        let backend = self.backend.clone();
        Ok(async move {
            let id = repo_model::bytes_hash(&buf);
            Ok(backend.write(&id, buf).await?)
        }
        .boxed_local())
    }
}

impl ManagedRepo {}
