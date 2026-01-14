//! Completion tracking for concurrent operations.
//!
//! This module provides primitives for tracking when multiple concurrent operations
//! have completed, allowing callers to either wait for completion or roll up
//! completion tracking to their own callers.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Notify;

/// A trait for awaiting completion of an asynchronous operation.
#[async_trait]
pub trait Complete: Send + Sync {
    /// Wait for the operation to complete.
    async fn complete(&self);
}

// =============================================================================
// NotifyComplete
// =============================================================================

/// A completion flag that can be signaled manually.
///
/// Call `notify_complete()` to signal completion, and `complete()` to wait for it.
pub struct NotifyComplete {
    completed: AtomicBool,
    notify: Notify,
}

impl NotifyComplete {
    /// Create a new `NotifyComplete` in the incomplete state.
    pub fn new() -> Self {
        Self {
            completed: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    /// Signal that the operation has completed.
    ///
    /// Any current or future calls to `complete()` will return.
    pub fn notify_complete(&self) {
        self.completed.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

impl Default for NotifyComplete {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Complete for NotifyComplete {
    async fn complete(&self) {
        loop {
            let notified = self.notify.notified();
            if self.completed.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}

// =============================================================================
// Completes
// =============================================================================

/// Error returned when `add()` is called after `done()`.
#[derive(Debug, Clone)]
pub struct AddAfterDoneError;

impl std::fmt::Display for AddAfterDoneError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "add() called after done()")
    }
}

impl std::error::Error for AddAfterDoneError {}

/// Internal shared state for `Completes`.
struct CompletesInner {
    counter: AtomicUsize,
    done: AtomicBool,
    notify: Notify,
}

/// An aggregator that tracks multiple `Complete` instances.
///
/// Add `Complete` instances with `add()`, then call `done()` when finished adding.
/// The `complete()` method will return when all added instances have completed
/// AND `done()` has been called.
pub struct Completes {
    inner: Arc<CompletesInner>,
}

impl Completes {
    /// Create a new empty `Completes`.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CompletesInner {
                counter: AtomicUsize::new(0),
                done: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    /// Add a `Complete` to track.
    ///
    /// The provided `Complete` will be spawned immediately and run concurrently.
    ///
    /// # Errors
    ///
    /// Returns `AddAfterDoneError` if `done()` has already been called.
    pub fn add(&self, complete: Arc<dyn Complete>) -> Result<(), AddAfterDoneError> {
        if self.inner.done.load(Ordering::SeqCst) {
            return Err(AddAfterDoneError);
        }
        self.inner.counter.fetch_add(1, Ordering::SeqCst);

        let inner = self.inner.clone();
        tokio::spawn(async move {
            complete.complete().await;
            let prev = inner.counter.fetch_sub(1, Ordering::SeqCst);
            if prev == 1 && inner.done.load(Ordering::SeqCst) {
                inner.notify.notify_waiters();
            }
        });

        Ok(())
    }

    /// Mark that no more `Complete` instances will be added.
    ///
    /// After calling this, `add()` will return an error.
    pub fn done(&self) {
        self.inner.done.store(true, Ordering::SeqCst);
        if self.inner.counter.load(Ordering::SeqCst) == 0 {
            self.inner.notify.notify_waiters();
        }
    }
}

impl Default for Completes {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Complete for Completes {
    async fn complete(&self) {
        loop {
            let notified = self.inner.notify.notified();
            if self.inner.done.load(Ordering::SeqCst)
                && self.inner.counter.load(Ordering::SeqCst) == 0
            {
                return;
            }
            notified.await;
        }
    }
}

// =============================================================================
// WithComplete
// =============================================================================

/// A result paired with a completion handle.
///
/// This struct is returned by operations that complete in two phases:
/// 1. The result is available immediately
/// 2. Background work (like flow control, I/O) completes later
///
/// Callers can use the result immediately and optionally wait for full
/// completion via the `complete` handle.
pub struct WithComplete<T> {
    /// The result of the operation.
    pub result: T,
    /// Handle to wait for the operation to fully complete.
    pub complete: Arc<dyn Complete>,
}

impl<T> WithComplete<T> {
    /// Create a new `WithComplete` with the given result and completion handle.
    pub fn new(result: T, complete: Arc<dyn Complete>) -> Self {
        Self { result, complete }
    }

    /// Wait for the operation to fully complete and return the result.
    pub async fn wait(self) -> T {
        self.complete.complete().await;
        self.result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn notify_complete_before_wait() {
        let nc = NotifyComplete::new();
        nc.notify_complete();
        nc.complete().await; // Should return immediately
    }

    #[tokio::test]
    async fn notify_complete_after_wait() {
        let nc = Arc::new(NotifyComplete::new());
        let nc2 = nc.clone();

        let handle = tokio::spawn(async move {
            nc2.complete().await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        nc.notify_complete();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn completes_empty_done() {
        let c = Completes::new();
        c.done();
        c.complete().await; // Should return immediately
    }

    #[tokio::test]
    async fn completes_add_then_done() {
        let nc = Arc::new(NotifyComplete::new());
        let c = Completes::new();

        c.add(nc.clone()).unwrap();
        c.done();

        let c = Arc::new(c);
        let c2 = c.clone();

        let handle = tokio::spawn(async move {
            c2.complete().await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        nc.notify_complete();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn completes_add_after_done_errors() {
        let c = Completes::new();
        c.done();

        let nc = Arc::new(NotifyComplete::new());
        assert!(c.add(nc).is_err());
    }
}
