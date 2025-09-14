use super::repo_model::{self, ObjectId};
use crate::util::ReleasedFuture;
use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::FutureExt;

#[async_trait(?Send)]
pub trait Repo {
    async fn read_object(&self, id: ObjectId) -> ReleasedFuture<repo_model::RepoObject>;
    async fn write_object(&self, obj: repo_model::RepoObject) -> ReleasedFuture<()>;
    async fn read_chunk(&self, id: ObjectId) -> ReleasedFuture<Bytes>;
    async fn write_chunk(&self, buf: Bytes) -> ReleasedFuture<()>;

    async fn read_root(&self, id: ObjectId) -> ReleasedFuture<repo_model::Root> {
        let released_read = self.read_object(id.clone()).await?;
        let fut = async move {
            let obj = released_read.await?;
            match obj {
                repo_model::RepoObject::Root(o) => Ok(o),
                _ => Err(anyhow!("obj {:?} is not a Root", id)),
            }
        }
        .boxed_local();

        Ok(fut)
    }

    async fn read_branches(&self, id: ObjectId) -> ReleasedFuture<repo_model::Branches> {
        let released_read = self.read_object(id.clone()).await?;
        let fut = async move {
            let obj = released_read.await?;
            match obj {
                repo_model::RepoObject::Branches(o) => Ok(o),
                _ => Err(anyhow!("obj {:?} is not a Branches", id)),
            }
        }
        .boxed_local();

        Ok(fut)
    }

    async fn read_commit(&self, id: ObjectId) -> ReleasedFuture<repo_model::Commit> {
        let released_read = self.read_object(id.clone()).await?;
        let fut = async move {
            let obj = released_read.await?;
            match obj {
                repo_model::RepoObject::Commit(o) => Ok(o),
                _ => Err(anyhow!("obj {:?} is not a Commit", id)),
            }
        }
        .boxed_local();

        Ok(fut)
    }

    async fn read_directory(&self, id: ObjectId) -> ReleasedFuture<repo_model::Directory> {
        let released_read = self.read_object(id.clone()).await?;
        let fut = async move {
            let obj = released_read.await?;
            match obj {
                repo_model::RepoObject::Directory(o) => Ok(o),
                _ => Err(anyhow!("obj {:?} is not a Directory", id)),
            }
        }
        .boxed_local();

        Ok(fut)
    }

    async fn read_file(&self, id: ObjectId) -> ReleasedFuture<repo_model::File> {
        let released_read = self.read_object(id.clone()).await?;
        let fut = async move {
            let obj = released_read.await?;
            match obj {
                repo_model::RepoObject::File(o) => Ok(o),
                _ => Err(anyhow!("obj {:?} is not a File", id)),
            }
        }
        .boxed_local();

        Ok(fut)
    }

    async fn write_root(&self, obj: repo_model::Root) -> ReleasedFuture<()> {
        Ok(self.write_object(repo_model::RepoObject::Root(obj)).await?)
    }

    async fn write_branches(&self, obj: repo_model::Branches) -> ReleasedFuture<()> {
        Ok(self
            .write_object(repo_model::RepoObject::Branches(obj))
            .await?)
    }

    async fn write_commit(&self, obj: repo_model::Commit) -> ReleasedFuture<()> {
        Ok(self
            .write_object(repo_model::RepoObject::Commit(obj))
            .await?)
    }

    async fn write_directory(&self, obj: repo_model::Directory) -> ReleasedFuture<()> {
        Ok(self
            .write_object(repo_model::RepoObject::Directory(obj))
            .await?)
    }

    async fn write_file(&self, obj: repo_model::File) -> ReleasedFuture<()> {
        Ok(self.write_object(repo_model::RepoObject::File(obj)).await?)
    }
}
