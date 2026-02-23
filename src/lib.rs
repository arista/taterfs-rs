//! taterfs-rs - A Rust command-line utility for file storage.

pub mod app;
pub mod backend;
pub mod caches;
pub mod cli;
pub mod commands;
pub mod config;
pub mod download;
pub mod file_store;
pub mod merge;
pub mod repo;
pub mod repo_model;
pub mod repository;
pub mod sync;
pub mod util;

pub use util::{CapacityManager, ReplenishmentRate, UsedCapacity};

pub use file_store::{
    DirEntry, DirectoryEntry, DirectoryList, DirectoryListSource, Error, FileDest, FileEntry,
    FileSource, FileStore, FsFileStore, MemoryFileStore, MemoryFileStoreBuilder, MemoryFsEntry,
    Result, S3FileSource, S3FileSourceConfig, ScanEvent, ScanEvents, SourceChunk,
    SourceChunkContent, SourceChunks,
};
