use std::future::Future;
use std::pin::Pin;

use crate::file_source::error::Result;
use crate::file_source::types::FileChunk;

/// Trait for async iteration over file chunks.
pub trait FileChunking: Send {
    /// Get the next file chunk, or None if all chunks have been read.
    fn next(&mut self) -> impl Future<Output = Result<Option<FileChunk>>> + Send;
}

/// Object-safe version of FileChunking for boxing.
pub trait FileChunkingBoxed: Send {
    /// Get the next file chunk, or None if all chunks have been read.
    fn next_boxed(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<FileChunk>>> + Send + '_>>;
}

impl<T: FileChunking + Send> FileChunkingBoxed for T {
    fn next_boxed(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<FileChunk>>> + Send + '_>> {
        Box::pin(self.next())
    }
}

/// A boxed file chunks iterator for use as a return type.
///
/// This is a wrapper around a boxed `FileChunkingBoxed` trait object
/// that provides an ergonomic `next()` method.
pub struct FileChunks {
    inner: Box<dyn FileChunkingBoxed + Send>,
}

impl FileChunks {
    /// Create a new FileChunks from a FileChunking implementation.
    pub fn new<T: FileChunking + Send + 'static>(chunking: T) -> Self {
        Self {
            inner: Box::new(chunking),
        }
    }

    /// Get the next file chunk, or None if all chunks have been read.
    pub async fn next(&mut self) -> Result<Option<FileChunk>> {
        self.inner.next_boxed().await
    }
}
