// A ManagedRepo is a Repo implementation that forwards requests to a RepoBackend, while injecting several features:
//
// * Backpressure - the methods return "ReleasedFuture" which is intended to control the flow of requests into the ManagedRepo.  Clients must immediately await the ReleasedFuture when making requests, which blocks until the request is permitted to begin.  Clients can then perform a second await at their leisure to retrieve the final actual result.
// * Simultaneous request limiting - can be configured to limit the number of simultaneous requests
// * Request rate limiting - can be configured to limit the number of requests per second
// * Throughput limiting - can be configured to limit the number of bytes read or written per second
// * Request deduplication - combines multiple identical requests into a single request
// * Caching - can be configured to cache which objects exist in the repo, as well as the contents of repo objects

use std::{cell::RefCell, rc::Rc};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::FutureExt;

use super::{
    Repo, RepoBackend,
    repo_model::{self, ObjectId},
};
use crate::{
    prelude::*,
    util::{InUse, Pool, ReleasedFuture},
};

pub struct ManagedRepo {
    ctx: Rc<ManagedRepoContext>,
    request_limiter: Option<Pool<()>>,
}

pub struct ManagedRepoContext {
    backend: Rc<dyn RepoBackend>,
}

#[async_trait(?Send)]
impl Repo for ManagedRepo {
    async fn read_current_root(&self) -> ReleasedFuture<Option<repo_model::ObjectId>> {
        // FIXME - implement this
        let ctx = self.ctx.clone();
        Ok(async move { Ok(ctx.backend.read_current_root().await?) }.boxed_local())
    }

    async fn write_current_root(&self, current_root: ObjectId) -> ReleasedFuture<()> {
        // FIXME - implement this
        let ctx = self.ctx.clone();
        Ok(async move { Ok(ctx.backend.write_current_root(&current_root).await?) }.boxed_local())
    }

    async fn read_object(&self, id: ObjectId) -> ReleasedFuture<repo_model::RepoObject> {
        // FIXME - check if the object is in cache
        // FIXME - check for request deduplication

        // Apply simultaneous request limiting
        let request_limiter_in_use = self.use_request_limiter().await;

        // FIXME - apply request rate limiting
        // FIXME - apply throughput limiting

        let ctx = self.ctx.clone();
        Ok(async move {
            let _request_limiter_in_use = request_limiter_in_use;

            // FIXME - set up request deduplication
            let bytes = ctx.backend.read(&id).await?;
            // FIXME - report to throughput manager
            // FIXME - end request deduplication
            // FIXME - cache the object
            Ok(repo_model::RepoObject::from_json_bytes(bytes)?)
        }
        .boxed_local())
    }

    async fn write_object(&self, obj: repo_model::RepoObject) -> ReleasedFuture<()> {
        // FIXME - implement this

        // FIXME - check the cache to see if the object has already been written
        // FIXME - check the repo to see if the object already exists (record in cache if so)
        // FIXME - check for request deduplication
        // FIXME - apply simultaneous request limiting
        // FIXME - apply request rate limiting
        // FIXME - apply throughput limiting

        let ctx = self.ctx.clone();
        Ok(async move {
            let bytes = repo_model::to_canonical_json_bytes(&obj);
            // FIXME - set up request deduplication
            let id = repo_model::bytes_hash(&bytes);
            // FIXME - report to throughput manager
            // FIXME - end request deduplication
            // FIXME - mark the cache as the object having been written
            // FIXME - cache the object?
            Ok(ctx.backend.write(&id, bytes).await?)
        }
        .boxed_local())
    }

    async fn read_chunk(&self, id: ObjectId) -> ReleasedFuture<Bytes> {
        // FIXME - implement this
        let ctx = self.ctx.clone();
        Ok(async move { Ok(ctx.backend.read(&id).await?) }.boxed_local())
    }

    async fn write_chunk(&self, buf: Bytes) -> ReleasedFuture<()> {
        // FIXME - implement this
        let ctx = self.ctx.clone();
        Ok(async move {
            let id = repo_model::bytes_hash(&buf);
            Ok(ctx.backend.write(&id, buf).await?)
        }
        .boxed_local())
    }
}

impl ManagedRepo {
    async fn use_request_limiter(&self) -> Option<InUse<()>> {
        match self.request_limiter.as_ref() {
            Some(l) => Some(l.checkout().await),
            None => None,
        }
    }
}
