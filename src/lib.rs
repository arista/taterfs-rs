//! taterfs-rs - A Rust command-line utility for file storage.

pub mod file_store;

pub use file_store::{
    DirEntry, DirectoryEntry, DirectoryList, Error, FileDest, FileEntry, FileSource, FileStore,
    Result, ScanEvent, ScanEvents, SourceChunk, SourceChunkContent, SourceChunks,
};
