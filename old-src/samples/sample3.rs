use std::path::PathBuf;
use std::sync::Arc;

use crate::file_source::{FileSourceService, FsFileSourceService};
use crate::repo::fs_repo_backend;
use crate::repo::repo_file_builder::RepoFileBuilder;
use crate::repo::sync_repo_file_builder;

pub async fn sample3() -> anyhow::Result<()> {
    let repo_backend = Arc::new(fs_repo_backend::FsRepoBackend::new(
        fs_repo_backend::Context {
            root: PathBuf::from("/tmp/taterfs-rs-1"),
            validate_hashes_on_write: true,
        },
    ));
    let fs = FsFileSourceService {};
    let mut iter = fs
        .get_file_chunks(PathBuf::from("/data/taterfs/tatercore/docs/log"))
        .await?;
    let mut file_builder =
        sync_repo_file_builder::SyncRepoFileBuilder::new(sync_repo_file_builder::Context {
            backend: repo_backend,
        });

    while let Some(handle) = iter.next().await? {
        let bytes = handle.get_chunk().await?;
        file_builder.add_chunk(bytes).await?;
    }
    let result = file_builder.complete().await?;
    println!("file_id: {}, size: {}", result.file, result.size);

    Ok(())
}
