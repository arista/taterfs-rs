// Thanks ChatGPT

use std::{
    fmt,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{Client, primitives::ByteStream};
use aws_smithy_runtime_api::client::result::SdkError as SmithySdkError;
use aws_smithy_types::error::metadata::ProvideErrorMetadata; // trait for .code()/.meta()

use bytes::Bytes;

// Project paths you requested
use crate::repo::repo_backend::{BackendError, RepoBackend};
use crate::repo::repo_model::ObjectId;

/// Configuration used to construct an S3-backed repository.
#[derive(Clone, Debug)]
pub struct Context {
    pub bucket: String,
    /// Optional key prefix inside the bucket (e.g. "taterfs/my-repo").
    /// A trailing "/" will be trimmed.
    pub prefix: String,
}

/// S3 implementation of `RepoBackend`.
#[derive(Clone)]
pub struct S3RepoBackend {
    client: Client,
    bucket: String,
    /// No trailing "/" (empty means bucket root)
    prefix: String,
}

impl fmt::Debug for S3RepoBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3RepoBackend")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl S3RepoBackend {
    pub async fn new(ctx: Context) -> Result<Self, BackendError> {
        let cfg = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = Client::new(&cfg);
        let prefix = ctx.prefix.trim_end_matches('/').to_string();
        Ok(Self {
            client,
            bucket: ctx.bucket,
            prefix,
        })
    }

    #[inline]
    fn join_key(&self, tail: &str) -> String {
        if self.prefix.is_empty() {
            tail.to_string()
        } else {
            format!("{}/{}", self.prefix, tail)
        }
    }

    /// objects/xx/yy/zz/{fullhex}
    fn object_key_for(&self, id: &ObjectId) -> String {
        // Use to_string() to avoid assuming an accessor; adjust if you expose &str.
        let h = id.to_string();
        let (xx, yy, zz) = (&h[0..2], &h[2..4], &h[4..6]);
        self.join_key(&format!("objects/{}/{}/{}/{}", xx, yy, zz, h))
    }

    #[inline]
    fn current_root_prefix(&self) -> String {
        self.join_key("current-root")
    }

    /// current-root/{inverted_ts}/{root_hash}
    fn current_root_entry_key(&self, id: &ObjectId) -> String {
        let inv = inverted_timestamp_ms();
        self.join_key(&format!("current-root/{:020}/{}", inv, id.to_string()))
    }
}

/// Inverted timestamp so lexicographically *smallest* is the newest.
/// (Zero-padded to 20 digits for stable lexical ordering.)
fn inverted_timestamp_ms() -> u64 {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    u64::MAX - now_ms
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

#[async_trait]
impl RepoBackend for S3RepoBackend {
    async fn current_root_exists(&self) -> Result<bool, BackendError> {
        let resp = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(format!("{}/", self.current_root_prefix()))
            .max_keys(1)
            .send()
            .await
            .map_err(|e| BackendError::S3(format!("list current-root: {e}")))?;

        let any = !resp.contents().is_empty();
        Ok(any)
    }

    async fn read_current_root(&self) -> Result<ObjectId, BackendError> {
        let resp = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(format!("{}/", self.current_root_prefix()))
            .max_keys(1)
            .send()
            .await
            .map_err(|e| BackendError::S3(format!("list current-root: {e}")))?;

        let objs = resp.contents();
        if objs.is_empty() {
            return Err(BackendError::RootMissing);
        }

        let key = objs[0]
            .key()
            .ok_or_else(|| BackendError::Other("missing S3 key".into()))?;

        let hash = key
            .rsplit('/')
            .next()
            .ok_or_else(|| BackendError::Other("bad current-root key".into()))?;

        let id = ObjectId::from_str(hash)
            .map_err(|_| BackendError::Other(format!("invalid object id in key: {hash}")))?;
        Ok(id)
    }

    async fn write_current_root(&self, root: &ObjectId) -> Result<(), BackendError> {
        let key = self.current_root_entry_key(root);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from_static(&[]))
            .send()
            .await
            .map_err(|e| BackendError::S3(format!("put current-root: {e}")))?;
        Ok(())
    }

    async fn exists(&self, id: &ObjectId) -> Result<bool, BackendError> {
        let key = self.object_key_for(id);
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) if is_not_found(&e) => Ok(false),
            Err(e) => Err(BackendError::S3(format!("head object: {e}"))),
        }
    }

    async fn read(&self, id: &ObjectId) -> Result<Bytes, BackendError> {
        let key = self.object_key_for(id);
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(out) => {
                let data = out
                    .body
                    .collect()
                    .await
                    .map_err(|e| BackendError::S3(format!("read collect: {e}")))?;
                Ok(data.into_bytes())
            }
            Err(e) if is_not_found(&e) => Err(BackendError::NotFound(id.clone())),
            Err(e) => Err(BackendError::S3(format!("get object: {e}"))),
        }
    }

    async fn write(&self, id: &ObjectId, bytes: Bytes) -> Result<(), BackendError> {
        // If you want to enforce hash->content integrity, compute the content hash
        // here and compare to `id`, returning BackendError::HashMismatch on mismatch.
        // (Left out since hashing API is project-specific.)
        let key = self.object_key_for(id);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map_err(|e| BackendError::S3(format!("put object: {e}")))?;
        Ok(())
    }
}
