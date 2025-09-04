// S3 implementation of FileStore

use anyhow::{Result, Context};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_smithy_runtime_api::client::result::SdkError as SmithySdkError;
use aws_smithy_types::error::metadata::ProvideErrorMetadata; // trait for .code()/.meta()
use bytes::Bytes;
use std::path::{Path, PathBuf};

use super::FileStore;

#[derive(Clone)]
pub struct S3FileStoreContext {
    pub client: Client,
    pub bucket: String,
    /// Prefix within the bucket that represents the root of this store (no trailing slash required).
    pub prefix: String,
}

#[derive(Clone)]
pub struct S3FileStore {
    ctx: S3FileStoreContext,
}

impl S3FileStore {
    pub fn new(ctx: S3FileStoreContext) -> Self {
        let mut prefix = ctx.prefix.trim_matches('/').to_string();
        // Normalize: empty or single component OK; store without trailing slash
        if prefix == "." {
            prefix.clear();
        }
        Self { ctx: S3FileStoreContext { prefix, ..ctx }}
    }

    fn key_for(&self, rel: &Path) -> String {
        let rel = rel.strip_prefix("/").unwrap_or(rel);
        match self.ctx.prefix.is_empty() {
            true => rel.to_string_lossy().to_string(),
            false if rel.as_os_str().is_empty() => self.ctx.prefix.clone(),
            false => format!("{}/{}", self.ctx.prefix, rel.to_string_lossy()),
        }
    }

    fn dir_prefix_for(&self, rel_dir: &Path) -> String {
        let mut p = self.key_for(rel_dir);
        if !p.is_empty() && !p.ends_with('/') {
            p.push('/');
        }
        p
    }
}

#[async_trait(?Send)]
impl FileStore for S3FileStore {
    async fn exists(&self, path: &Path) -> Result<bool> {
        let key = self.key_for(path);
        let res = self
            .ctx
            .client
            .head_object()
            .bucket(&self.ctx.bucket)
            .key(&key)
            .send()
            .await;

        match res {
            Ok(_) => Ok(true),
            Err(e) => {
                if is_not_found(&e) {
                    Ok(false)
                }
                else {
                    Err(anyhow::anyhow!(e).context(format!("head_object key={key}")))
                }
            }
        }
    }

    async fn read(&self, path: &Path) -> Result<Bytes> {
        let key = self.key_for(path);
        let out = self
            .ctx
            .client
            .get_object()
            .bucket(&self.ctx.bucket)
            .key(&key)
            .send()
            .await
            .with_context(|| format!("get_object key={key}"))?;

        let b = out
            .body
            .collect()
            .await
            .with_context(|| format!("collect body for key={key}"))?;
        Ok(Bytes::from(b.to_vec()))
    }

    async fn write(&self, path: &Path, buf: Bytes) -> Result<()> {
        let key = self.key_for(path);
        self.ctx
            .client
            .put_object()
            .bucket(&self.ctx.bucket)
            .key(&key)
            .body(aws_sdk_s3::primitives::ByteStream::from(buf.to_vec()))
            .send()
            .await
            .with_context(|| format!("put_object key={key}"))?;
        Ok(())
    }

    // Recursively search under the given "directory" (key prefix) for the first file.
    // Returns the path RELATIVE TO the given directory (e.g. "dir1/file.txt" or "a/b/c.txt").
    async fn first_file(&self, path: &Path) -> Result<Option<PathBuf>> {
        let dir_prefix = self.dir_prefix_for(path);

        let out = self
            .ctx
            .client
            .list_objects_v2()
            .bucket(&self.ctx.bucket)
            .prefix(&dir_prefix)
            .max_keys(1000)
            .send()
            .await
            .with_context(|| format!("list_objects_v2 prefix={dir_prefix}"))?;

        let objs = out.contents(); // &[Object]

        // Keys are lexicographically sorted by S3
        if let Some(first_key) = objs
            .iter()
            .filter_map(|o| o.key())
            .map(|s| s.to_string())
            .find(|k| k != &dir_prefix && !k.ends_with('/'))
        {
            if let Some(rel) = first_key.strip_prefix(&dir_prefix) {
                if !rel.is_empty() {
                    return Ok(Some(PathBuf::from(rel)));
                }
            }
        }

        Ok(None)
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

/// Returns true if the SDK error represents "object not found".
/// By default, we treat 404-ish codes and 403s caused by lack of ListBucket as "not found".
fn is_not_found<E, R>(err: &SmithySdkError<E, R>) -> bool
where
    E: ProvideErrorMetadata,
{
    match err {
        SmithySdkError::ServiceError(se) => match se.err().code() {
            Some("NoSuchKey") | Some("NotFound") | Some("AccessDenied") | Some("Forbidden") => true,
            _ => false,
        },
        _ => false,
    }
}
