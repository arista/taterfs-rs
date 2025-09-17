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
    util::{InUse, Pool, RefillingPool, ReleasedFuture, Removed},
};

pub struct ManagedRepo {
    ctx: Rc<ManagedRepoContext>,

    // For limiting number of simultaneous requests
    request_limiter: Option<Pool<()>>,
    // For limiting overall requests/s
    request_rate_limiter: Option<RefillingPool>,
    // For limiting throughput
    throughput_limiter: Option<RefillingPool>,
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
        let request_limiter_handle = self.use_request_limiter().await;
        // Apply request rate limiting
        let request_rate_handle = self.use_request_rate_limiter().await;
        // Apply throughput limiting
        let throughput_handle = self.use_throughput_limiter(1000).await;

        let ctx = self.ctx.clone();
        Ok(async move {
            let _request_limiter_handle = request_limiter_handle;
            let _request_rate_handle = request_rate_handle;
            let _throughput_handle = throughput_handle;

            // FIXME - set up request deduplication

            let bytes = ctx.backend.read(&id).await?;

            // Report how many bytes were actually used
            _throughput_handle.map(|h| h.actually_removed(u64::from(bytes.len() as u64)));

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

    async fn read_chunk(&self, id: ObjectId, expected_size: u64) -> ReleasedFuture<Bytes> {
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

    async fn use_request_rate_limiter(&self) -> Option<Removed> {
        match self.request_rate_limiter.as_ref() {
            Some(l) => Some(l.remove(1).await),
            None => None,
        }
    }

    async fn use_throughput_limiter(&self, estimated_amount: u32) -> Option<Removed> {
        match self.throughput_limiter.as_ref() {
            Some(l) => Some(l.remove(estimated_amount).await),
            None => None,
        }
    }
}
