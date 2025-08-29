use std::sync::Arc;
use crate::file_store::file_store as FS;
use crate::repo::repo_model as RM;
use crate::util::futures_queue::FuturesQueue;
use crate::repo::repo_directory_builder::RepoDirectoryBuilder;
use crate::repo::repo_file_builder::{RepoFileBuilder};
use futures::{FutureExt, future::LocalBoxFuture};
use bytes::Bytes;

pub struct DirectoryUploader<'a> {
    ctx: Context,
    directory_stack: Vec<UploadingDirectory<'a>>
}

type RepoDirectoryBuilderFactory = Arc<dyn Fn() -> Box<dyn RepoDirectoryBuilder> + Send + Sync + 'static>;
type RepoFileBuilderFactory = Arc<dyn Fn() -> Box<dyn RepoFileBuilder> + Send + Sync + 'static>;

pub struct Context {
    repo_directory_builder_factory: RepoDirectoryBuilderFactory,
    repo_file_builder_factory: RepoFileBuilderFactory,
}


struct UploadingDirectory<'a> {
    name: String,
    iter: Box<dyn FS::DirectoryLister>,
    queue: FuturesQueue<RM::DirectoryEntry>,
    result: LocalBoxFuture<'a, anyhow::Result<RM::DirectoryEntry>>,
}

impl<'a> DirectoryUploader<'a> {
    pub fn new(ctx: Context) -> DirectoryUploader<'a> {
        DirectoryUploader {
            ctx,
            directory_stack: Vec::default(),
        }
    }

    pub async fn upload_directory(&mut self, name: String, directory_lister: Box<dyn FS::DirectoryLister>) -> anyhow::Result<()> {
        {
            // Push the first directory
            let queue = FuturesQueue::<RM::DirectoryEntry>::new();
            let builder_factory = self.ctx.repo_directory_builder_factory.clone();
            self.directory_stack.push(UploadingDirectory {
                name: "".to_string(),
                iter: directory_lister,
                queue: queue.clone(),
                result: build_directory(builder_factory, name, queue).boxed_local(),
            });
        }

        loop {
            // Iterate over the entries in the directory at the top of the stack
            let top = self.directory_stack.last_mut().unwrap();
            match top.iter.next().await? {
                Some(dir_entry) => match dir_entry {
                    FS::DirEntry::File(f) => {
                        let mut queue = FuturesQueue::<Bytes>::new();
                        let builder_factory = self.ctx.repo_file_builder_factory.clone();
                        let result = build_file(builder_factory, f.name(), f.executable(), &mut queue);
                    }
                    FS::DirEntry::Directory(d) => {
                        // FIXME - implement this
                    }
                }
                None => {
                    // FIXME - implement this
                    break;
                }
            }
        }
        
        
        Ok(())
    }
}

pub async fn build_directory(builder_factory: RepoDirectoryBuilderFactory, name: &str, queue: FuturesQueue<RM::DirectoryEntry>) -> anyhow::Result<RM::DirectoryEntry> {
    let mut builder = (builder_factory)();
    while let Some(entry) = queue.next().await? {
        builder.add_entry(entry).await?
    }
    Ok(RM::DirectoryEntry::Directory(RM::DirEntry {
        name,
        directory: builder.complete().await?,
    }))
}

pub async fn build_file(builder_factory: RepoFileBuilderFactory, name: &str, executable: bool, queue: &mut FuturesQueue<Bytes>) -> anyhow::Result<RM::DirectoryEntry> {
    let mut builder = (builder_factory)();
    while let Some(chunk_bytes) = queue.next().await? {
        builder.add_chunk(chunk_bytes).await?
    }
    let result = builder.complete().await?;
    Ok(RM::DirectoryEntry::File(RM::FileEntry {
        name: name.to_string(),
        executable,
        size: result.size,
        file: result.file,
    }))
}
