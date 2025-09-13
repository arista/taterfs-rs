use async_trait::async_trait;
use futures_util::FutureExt;
use crate::util::ReleasedFuture;
use super::repo_model::{self, ObjectId};
use anyhow::{anyhow};

#[async_trait(?Send)]
pub trait Repo {
    async fn read_object(&self, id: ObjectId) -> ReleasedFuture<repo_model::RepoObject>;
    async fn write_object(&self, id: ObjectId, obj: repo_model::RepoObject) -> ReleasedFuture<()>;

    async fn read_root(&self, id: ObjectId) -> ReleasedFuture<repo_model::Root> {
        let released_read = self.read_object(id).await?;
        let fut = async move {
            let obj = released_read.await?;
            match obj {
                repo_model::RepoObject::Root(o) => Ok(o),
                _ => Err(anyhow!("obj {:?} is not a Root", id)),
            }
        }.boxed_local();

        Ok(fut)
    }
}
