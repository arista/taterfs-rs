//! Download functions for transferring directories from a Repository to a FileStore.
//!
//! This module provides the [`DownloadRepoToStore`] struct which handles the logic
//! of synchronizing a repository directory to a file store location, determining
//! what modifications need to be made and executing them via [`DownloadActions`].

#[allow(clippy::module_inception)]
mod download;

pub use download::{
    DownloadActions, DownloadError, DownloadRepoToStore, DownloadToFileStoreActions,
    DownloadToStageActions, DownloadWithStageActions, Result, StagedFileChunkWithContentList,
    download_directory, download_file,
};
