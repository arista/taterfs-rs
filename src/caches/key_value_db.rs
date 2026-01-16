//! Key-value database trait and types.
//!
//! This module defines the fundamental key-value storage interface used by the cache system.

use std::fmt;

use async_trait::async_trait;

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during key-value database operations.
#[derive(Debug)]
pub enum KeyValueDbError {
    /// An I/O error occurred.
    Io(std::io::Error),
    /// Database error (e.g., from LMDB).
    Database(String),
    /// Encoding/decoding error.
    Encoding(String),
}

impl fmt::Display for KeyValueDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyValueDbError::Io(e) => write!(f, "I/O error: {}", e),
            KeyValueDbError::Database(msg) => write!(f, "database error: {}", msg),
            KeyValueDbError::Encoding(msg) => write!(f, "encoding error: {}", msg),
        }
    }
}

impl std::error::Error for KeyValueDbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            KeyValueDbError::Io(e) => Some(e),
            KeyValueDbError::Database(_) | KeyValueDbError::Encoding(_) => None,
        }
    }
}

impl From<std::io::Error> for KeyValueDbError {
    fn from(e: std::io::Error) -> Self {
        KeyValueDbError::Io(e)
    }
}

/// Result type for key-value database operations.
pub type Result<T> = std::result::Result<T, KeyValueDbError>;

// =============================================================================
// KeyValueDb Trait
// =============================================================================

/// A key-value database interface.
///
/// Provides basic key-value operations plus transaction support for atomic updates.
#[async_trait]
pub trait KeyValueDb: Send + Sync {
    /// Check if a key exists in the database.
    async fn exists(&self, key: &[u8]) -> Result<bool>;

    /// Get the value for a key, returning `None` if not found.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Start a transaction for atomic read-modify-write operations.
    ///
    /// Transactions support reads within the transaction and buffer writes
    /// until commit. This is needed for operations like ID generation where
    /// atomicity is required.
    async fn transaction(&self) -> Result<Box<dyn KeyValueDbTransaction + Send>>;

    /// Get a write handle for buffered writes.
    ///
    /// Unlike transactions, writes through this handle may be buffered and
    /// flushed asynchronously. Use this for writes that don't require
    /// immediate consistency.
    async fn write(&self) -> Result<Box<dyn KeyValueDbWrites + Send>>;
}

// =============================================================================
// KeyValueDbTransaction Trait
// =============================================================================

/// A transaction on a key-value database.
///
/// Supports reads and buffered writes that are atomically committed.
#[async_trait]
pub trait KeyValueDbTransaction: Send {
    /// Check if a key exists within this transaction's view.
    async fn exists(&self, key: &[u8]) -> Result<bool>;

    /// Get the value for a key within this transaction's view.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Set a key-value pair (buffered until commit).
    async fn set(&mut self, key: Vec<u8>, val: Vec<u8>);

    /// Delete a key (buffered until commit).
    async fn del(&mut self, key: Vec<u8>);

    /// Commit all buffered writes atomically.
    async fn commit(self: Box<Self>) -> Result<()>;
}

// =============================================================================
// KeyValueDbWrites Trait
// =============================================================================

/// A write handle for buffered writes to a key-value database.
///
/// Writes are buffered and may be flushed asynchronously or on drop.
#[async_trait]
pub trait KeyValueDbWrites: Send {
    /// Set a key-value pair (buffered).
    async fn set(&mut self, key: Vec<u8>, val: Vec<u8>);

    /// Delete a key (buffered).
    async fn del(&mut self, key: Vec<u8>);

    /// Flush all buffered writes to the database.
    async fn flush(self: Box<Self>) -> Result<()>;
}

// =============================================================================
// Write Operation Enum
// =============================================================================

/// A pending write operation.
#[derive(Debug, Clone)]
pub enum WriteOp {
    /// Set a key to a value.
    Set { key: Vec<u8>, value: Vec<u8> },
    /// Delete a key.
    Del { key: Vec<u8> },
}

impl WriteOp {
    /// Get the key for this operation.
    pub fn key(&self) -> &[u8] {
        match self {
            WriteOp::Set { key, .. } | WriteOp::Del { key } => key,
        }
    }

    /// Estimate the memory size of this operation.
    pub fn size(&self) -> usize {
        match self {
            WriteOp::Set { key, value } => key.len() + value.len(),
            WriteOp::Del { key } => key.len(),
        }
    }
}
