use async_trait::async_trait;
use std::time::{SystemTime, UNIX_EPOCH};
use std::rc::Rc;
use bytes::Bytes;
use anyhow::anyhow;

use super::{FileStore};
use super::repo_backend2::RepoBackend;
use super::repo_model as RM;

pub struct FileStoreRepoBackendContext {
    file_store: Rc<dyn FileStore>
}

pub struct FileStoreRepoBackend {
    ctx: FileStoreRepoBackendContext
}

impl FileStoreRepoBackend {
    pub fn new(ctx: FileStoreRepoBackendContext) -> Self {
        Self { ctx }
    }

    /// objects/xx/yy/zz/{fullhex}
    fn object_key_for(&self, id: &RM::ObjectId) -> String {
        // Use to_string() to avoid assuming an accessor; adjust if you expose &str.
        let h = id.to_string();
        let (xx, yy, zz) = (&h[0..2], &h[2..4], &h[4..6]);
        format!("objects/{}/{}/{}/{}", xx, yy, zz, h)
    }

    /// current-root/{inverted_ts 0-7}/{ts 8-9}/{ts 10-11}/{ts 12-19}/{root_hash}
    //
    // The timestamp is split up to avoid any one directory getting huge:
    //
    // {changes ~every 30 years}/{changes ~3x per year}/{changes ~every day}/{changes every ms}
    fn current_root_entry_key(&self, id: &RM::ObjectId) -> String {
        let inv = inverted_timestamp_ms();
        format!("current-root/{:020}/{}", inv, id.to_string())
    }
}

#[async_trait(?Send)]
impl RepoBackend for FileStoreRepoBackend {
    async fn current_root_exists(&self) -> anyhow::Result<bool> {
        Ok(self.ctx.file_store.first_file("current-root").await?.is_some())
    }
    
    async fn read_current_root(&self) -> anyhow::Result<RM::ObjectId> {
        match self.ctx.file_store.first_file("current-root").await? {
            Some(path) => {
                // Take everything after the last "/", or the whole string if there is no "/"
                match path.rsplit_once('/') {
                    Some((_, after)) => Ok(RM::ObjectId::from(after)),
                    None => Ok(RM::ObjectId::from(path)),
                }                
            }
            None => Err(anyhow!("No current root found"))
        }
    }
    
    async fn write_current_root(&self, root: &RM::ObjectId) -> anyhow::Result<()> {
        let key = self.current_root_entry_key(root);
        Ok(self.ctx.file_store.write(&key, Bytes::new()).await?)
    }
    
    async fn exists(&self, id: &RM::ObjectId) -> anyhow::Result<bool> {
        Ok(self.ctx.file_store.exists(&self.object_key_for(id)).await?)
    }
    
    async fn read(&self, id: &RM::ObjectId) -> anyhow::Result<Bytes> {
        Ok(self.ctx.file_store.read(&self.object_key_for(id)).await?)
    }
    
    async fn write(&self, id: &RM::ObjectId, bytes: Bytes) -> anyhow::Result<()> {
        Ok(self.ctx.file_store.write(&self.object_key_for(id), bytes).await?)
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
