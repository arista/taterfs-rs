use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::file_source::file_source;
use crate::file_source::file_source::{DirectoryLister, FileSourceService};
use crate::file_source::fs_file_source::FsFileSourceService;
use crate::repo::repo_backend::RepoBackend;
use crate::repo::repo_directory_builder::RepoDirectoryBuilder;
use crate::repo::repo_file_builder::{RepoFileBuilder, RepoFileBuilderResult};
use crate::repo::repo_model;
use crate::repo::repo_model::ObjectId;
use crate::repo::s3_repo_backend;
use crate::repo::sync_repo_directory_builder;
use crate::repo::sync_repo_file_builder;

pub async fn sample5() -> anyhow::Result<()> {
    let repo_backend_obj = s3_repo_backend::S3RepoBackend::new(s3_repo_backend::Context {
        bucket: "taterfs-test".to_string(),
        prefix: "taterfs-rs-test/2025-08-28-01".to_string(),
    })
    .await?;
    let repo_backend = Arc::new(repo_backend_obj);
    let fs = Arc::new(FsFileSourceService {});

    let mut uploader = Uploader {
        repo_backend: repo_backend.clone(),
        file_source: fs.clone(),
        frames: Vec::new(),
    };

    //    let directory_id = uploader.upload_directory(Path::new("/data/taterfs/tatercore/docs/noteflight")).await?;
    let directory_id = uploader
        .upload_directory(Path::new("/data/taterfs/tatercore/docs"))
        .await?;
    println!("directory_id: {}", directory_id);

    Ok(())
}

struct Uploader {
    repo_backend: Arc<dyn RepoBackend>,
    file_source: Arc<dyn FileSourceService>,
    frames: Vec<UploaderFrame>,
}

struct UploaderFrame {
    name: String,
    builder: Box<dyn RepoDirectoryBuilder>,
    iter: Box<dyn DirectoryLister>,
}

impl Uploader {
    pub async fn upload_directory(&mut self, path: &Path) -> anyhow::Result<ObjectId> {
        self.frames.push(UploaderFrame {
            name: "".to_string(),
            iter: self.file_source.list_directory(path).await?,
            builder: Box::new(sync_repo_directory_builder::SyncRepoDirectoryBuilder::new(
                sync_repo_directory_builder::Context {
                    backend: self.repo_backend.clone(),
                },
            )),
        });

        loop {
            let top = self.frames.last_mut().unwrap();
            match top.iter.next().await? {
                Some(dir_entry) => match dir_entry {
                    file_source::DirEntry::File(f) => {
                        let result = self.upload_file(f.abs_path.as_path()).await?;
                        self.frames
                            .last_mut()
                            .unwrap()
                            .builder
                            .add_entry(repo_model::DirectoryEntry::File(repo_model::FileEntry {
                                name: f.name,
                                executable: f.executable,
                                file: result.file,
                                size: result.size,
                            }))
                            .await?;
                    }
                    file_source::DirEntry::Directory(d) => {
                        self.frames.push(UploaderFrame {
                            name: d.name,
                            iter: d.lister,
                            builder: Box::new(
                                sync_repo_directory_builder::SyncRepoDirectoryBuilder::new(
                                    sync_repo_directory_builder::Context {
                                        backend: self.repo_backend.clone(),
                                    },
                                ),
                            ),
                        });
                    }
                },
                None => {
                    let mut completed = self.frames.pop().unwrap();
                    let object_id = completed.builder.complete().await?;
                    if self.frames.len() == 0 {
                        return Ok(object_id);
                    } else {
                        self.frames
                            .last_mut()
                            .unwrap()
                            .builder
                            .add_entry(repo_model::DirectoryEntry::Directory(
                                repo_model::DirEntry {
                                    name: completed.name,
                                    directory: object_id,
                                },
                            ))
                            .await?;
                    }
                }
            }
        }
    }

    pub async fn upload_file(&self, path: &Path) -> anyhow::Result<RepoFileBuilderResult> {
        let mut iter = self.file_source.get_file_chunks(PathBuf::from(path)).await?;
        let mut file_builder =
            sync_repo_file_builder::SyncRepoFileBuilder::new(sync_repo_file_builder::Context {
                backend: self.repo_backend.clone(),
            });

        while let Some(handle) = iter.next().await? {
            let bytes = handle.get_chunk().await?;
            file_builder.add_chunk(bytes).await?;
        }
        let result = file_builder.complete().await?;
        Ok(result)
    }
}
