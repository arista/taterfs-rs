//! Managed buffer allocation with optional capacity limiting.
//!
//! This module provides [`ManagedBuffers`], a service that hands out [`ManagedBuffer`]
//! objects with RAII-based capacity tracking. When a `ManagedBuffer` is dropped,
//! its capacity is automatically returned.

use bytes::BytesMut;

use super::capacity_manager::{CapacityManager, UsedCapacity};

// =============================================================================
// ManagedBuffer
// =============================================================================

/// A buffer with RAII-based capacity management.
///
/// When dropped, the capacity is automatically returned to the [`ManagedBuffers`]
/// from which it was allocated.
pub struct ManagedBuffer {
    buf: BytesMut,
    // UsedCapacity handles returning capacity on drop.
    // None if no CapacityManager was configured.
    _used_capacity: Option<UsedCapacity>,
}

impl ManagedBuffer {
    /// Returns the size of the buffer in bytes.
    pub fn size(&self) -> u64 {
        self.buf.len() as u64
    }

    /// Returns a reference to the underlying buffer.
    pub fn buf(&self) -> &BytesMut {
        &self.buf
    }

    /// Returns a mutable reference to the underlying buffer.
    pub fn buf_mut(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    /// Consumes the ManagedBuffer and returns the underlying BytesMut.
    ///
    /// Note: This will release the capacity back to the ManagedBuffers.
    pub fn into_buf(self) -> BytesMut {
        self.buf
    }
}

impl AsRef<[u8]> for ManagedBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.buf
    }
}

impl std::ops::Deref for ManagedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

// =============================================================================
// ManagedBuffers
// =============================================================================

/// A service for allocating managed buffers with optional capacity limiting.
///
/// If configured with a [`CapacityManager`], buffer allocations will wait
/// until sufficient capacity is available. If no `CapacityManager` is provided,
/// buffers are allocated without any limiting.
///
/// # Examples
///
/// ```
/// use taterfs_rs::util::managed_buffers::ManagedBuffers;
/// use taterfs_rs::util::capacity_manager::CapacityManager;
///
/// # async fn example() {
/// // With capacity limiting
/// let capacity_manager = CapacityManager::new(1024 * 1024); // 1MB limit
/// let managed = ManagedBuffers::with_capacity_manager(capacity_manager);
///
/// let buf = managed.get_buffer(4096).await;
/// assert_eq!(buf.size(), 4096);
/// // Capacity is returned when buf is dropped
///
/// // Without capacity limiting
/// let unlimited = ManagedBuffers::new();
/// let buf = unlimited.get_buffer(4096).await;
/// # }
/// ```
#[derive(Clone)]
pub struct ManagedBuffers {
    capacity_manager: Option<CapacityManager>,
}

impl ManagedBuffers {
    /// Create a new `ManagedBuffers` without capacity limiting.
    ///
    /// Buffers can be allocated without any restrictions.
    pub fn new() -> Self {
        Self {
            capacity_manager: None,
        }
    }

    /// Create a new `ManagedBuffers` with the given capacity manager.
    ///
    /// Buffer allocations will wait until sufficient capacity is available.
    pub fn with_capacity_manager(capacity_manager: CapacityManager) -> Self {
        Self {
            capacity_manager: Some(capacity_manager),
        }
    }

    /// Allocate a buffer of the given size.
    ///
    /// If a capacity manager is configured and capacity is not available,
    /// this will wait until capacity becomes available.
    ///
    /// The returned [`ManagedBuffer`] will automatically return its capacity
    /// when dropped.
    pub async fn get_buffer(&self, size: u64) -> ManagedBuffer {
        let used_capacity = match &self.capacity_manager {
            Some(cm) => Some(cm.use_capacity(size).await),
            None => None,
        };

        let buf = BytesMut::zeroed(size as usize);

        ManagedBuffer {
            buf,
            _used_capacity: used_capacity,
        }
    }

    /// Returns whether this `ManagedBuffers` has capacity limiting enabled.
    pub fn has_capacity_limit(&self) -> bool {
        self.capacity_manager.is_some()
    }

    /// Returns the current capacity usage, if a capacity manager is configured.
    pub fn used(&self) -> Option<u64> {
        self.capacity_manager.as_ref().map(|cm| cm.used())
    }

    /// Returns the total capacity, if a capacity manager is configured.
    pub fn capacity(&self) -> Option<u64> {
        self.capacity_manager.as_ref().map(|cm| cm.capacity())
    }
}

impl Default for ManagedBuffers {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn test_unlimited_allocation() {
        let managed = ManagedBuffers::new();
        assert!(!managed.has_capacity_limit());

        let buf = managed.get_buffer(1024).await;
        assert_eq!(buf.size(), 1024);
        assert_eq!(buf.len(), 1024);
    }

    #[tokio::test]
    async fn test_limited_allocation() {
        let cm = CapacityManager::new(4096);
        let managed = ManagedBuffers::with_capacity_manager(cm);
        assert!(managed.has_capacity_limit());

        let buf = managed.get_buffer(1024).await;
        assert_eq!(buf.size(), 1024);
        assert_eq!(managed.used(), Some(1024));

        drop(buf);
        assert_eq!(managed.used(), Some(0));
    }

    #[tokio::test]
    async fn test_waits_for_capacity() {
        let cm = CapacityManager::new(1024);
        let managed = ManagedBuffers::with_capacity_manager(cm);

        // Use all capacity
        let buf1 = managed.get_buffer(1024).await;
        assert_eq!(managed.used(), Some(1024));

        // This should wait
        let managed_clone = managed.clone();
        let handle = tokio::spawn(async move { managed_clone.get_buffer(512).await });

        sleep(Duration::from_millis(10)).await;

        // Release capacity
        drop(buf1);

        // Now allocation should complete
        let buf2 = timeout(Duration::from_millis(100), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(buf2.size(), 512);
        assert_eq!(managed.used(), Some(512));
    }

    #[tokio::test]
    async fn test_buffer_deref() {
        let managed = ManagedBuffers::new();
        let buf = managed.get_buffer(4).await;

        // Test Deref
        assert_eq!(buf.len(), 4);

        // Test AsRef
        let slice: &[u8] = buf.as_ref();
        assert_eq!(slice.len(), 4);
    }

    #[tokio::test]
    async fn test_into_buf() {
        let cm = CapacityManager::new(4096);
        let managed = ManagedBuffers::with_capacity_manager(cm);

        let buf = managed.get_buffer(1024).await;
        assert_eq!(managed.used(), Some(1024));

        let bytes_mut = buf.into_buf();
        assert_eq!(bytes_mut.len(), 1024);

        // Capacity should be returned after into_buf consumes the ManagedBuffer
        assert_eq!(managed.used(), Some(0));
    }

    #[tokio::test]
    async fn test_multiple_buffers() {
        let cm = CapacityManager::new(4096);
        let managed = ManagedBuffers::with_capacity_manager(cm);

        let buf1 = managed.get_buffer(1000).await;
        let buf2 = managed.get_buffer(1000).await;
        let buf3 = managed.get_buffer(1000).await;

        assert_eq!(managed.used(), Some(3000));

        drop(buf2);
        assert_eq!(managed.used(), Some(2000));

        drop(buf1);
        drop(buf3);
        assert_eq!(managed.used(), Some(0));
    }
}
