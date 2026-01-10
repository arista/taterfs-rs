use std::future::Future;
use std::pin::Pin;

use crate::file_source::error::Result;
use crate::file_source::types::DirectoryListEntry;

/// Trait for async iteration over directory entries.
pub trait DirectoryListing: Send {
    /// Get the next directory entry, or None if the listing is exhausted.
    fn next(&mut self) -> impl Future<Output = Result<Option<DirectoryListEntry>>> + Send;
}

/// Object-safe version of DirectoryListing for boxing.
pub trait DirectoryListingBoxed: Send {
    /// Get the next directory entry, or None if the listing is exhausted.
    fn next_boxed(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<DirectoryListEntry>>> + Send + '_>>;
}

impl<T: DirectoryListing + Send> DirectoryListingBoxed for T {
    fn next_boxed(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<DirectoryListEntry>>> + Send + '_>> {
        Box::pin(self.next())
    }
}

/// A boxed directory listing for use as a return type.
///
/// This is a wrapper around a boxed `DirectoryListingBoxed` trait object
/// that provides an ergonomic `next()` method.
pub struct DirectoryList {
    inner: Box<dyn DirectoryListingBoxed + Send>,
}

impl DirectoryList {
    /// Create a new DirectoryList from a DirectoryListing implementation.
    pub fn new<T: DirectoryListing + Send + 'static>(listing: T) -> Self {
        Self {
            inner: Box::new(listing),
        }
    }

    /// Get the next directory entry, or None if the listing is exhausted.
    pub async fn next(&mut self) -> Result<Option<DirectoryListEntry>> {
        self.inner.next_boxed().await
    }
}
