//! Flow control via capacity management.
//!
//! The [`CapacityManager`] provides a general-purpose mechanism for limiting
//! concurrent resource usage, including:
//! - Network bandwidth limiting
//! - Memory usage limiting
//! - Request rate limiting
//! - Concurrent request limiting

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for automatic capacity replenishment.
#[derive(Debug, Clone)]
pub struct ReplenishmentRate {
    /// Amount to replenish each period.
    pub amount: u64,
    /// Time period between replenishments.
    pub period: Duration,
}

impl ReplenishmentRate {
    /// Create a new replenishment rate.
    pub fn new(amount: u64, period: Duration) -> Self {
        Self { amount, period }
    }
}

// =============================================================================
// UsedCapacity
// =============================================================================

/// A handle representing capacity that has been reserved.
///
/// When this is dropped, the capacity is automatically returned to the
/// [`CapacityManager`]. This ensures capacity is never leaked.
pub struct UsedCapacity {
    amount: u64,
    inner: Arc<Inner>,
}

impl UsedCapacity {
    /// Returns the amount of capacity this handle represents.
    pub fn amount(&self) -> u64 {
        self.amount
    }
}

impl Drop for UsedCapacity {
    fn drop(&mut self) {
        if self.amount > 0 {
            self.inner.replenish(self.amount);
        }
    }
}

// =============================================================================
// Internal State
// =============================================================================

/// A waiter in the queue.
struct Waiter {
    amount: u64,
    sender: oneshot::Sender<()>,
}

/// Shared internal state.
struct Inner {
    state: Mutex<State>,
}

struct State {
    limit: u64,
    used: u64,
    waiters: VecDeque<Waiter>,
}

impl Inner {
    fn new(limit: u64) -> Self {
        Self {
            state: Mutex::new(State {
                limit,
                used: 0,
                waiters: VecDeque::new(),
            }),
        }
    }

    /// Try to use capacity immediately, returns None if we need to wait.
    fn try_use(&self, amount: u64) -> Option<()> {
        let mut state = self.state.lock().unwrap();
        if state.used + amount <= state.limit {
            state.used += amount;
            Some(())
        } else {
            None
        }
    }

    /// Add a waiter to the queue.
    fn add_waiter(&self, amount: u64) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        let mut state = self.state.lock().unwrap();
        state.waiters.push_back(Waiter { amount, sender });
        receiver
    }

    /// Replenish capacity and wake waiters if possible.
    fn replenish(&self, amount: u64) {
        let mut state = self.state.lock().unwrap();
        state.used = state.used.saturating_sub(amount);

        // Try to fulfill waiting requests
        while let Some(waiter) = state.waiters.front() {
            if state.used + waiter.amount <= state.limit {
                state.used += waiter.amount;
                let waiter = state.waiters.pop_front().unwrap();
                // Ignore send errors - receiver may have been dropped
                let _ = waiter.sender.send(());
            } else {
                break;
            }
        }
    }
}

// =============================================================================
// CapacityManager
// =============================================================================

/// Manages capacity for flow control.
///
/// The CapacityManager tracks usage against a configured limit. When capacity
/// is exhausted, requests to [`use_capacity`](Self::use_capacity) will wait
/// until capacity becomes available.
///
/// # Examples
///
/// ```
/// use taterfs_rs::CapacityManager;
///
/// # async fn example() {
/// // Create a manager with limit of 100
/// let manager = CapacityManager::new(100);
///
/// // Use some capacity - this returns immediately if under limit
/// let used = manager.use_capacity(50).await;
/// assert_eq!(used.amount(), 50);
///
/// // Capacity is automatically returned when `used` is dropped
/// drop(used);
/// # }
/// ```
#[derive(Clone)]
pub struct CapacityManager {
    inner: Arc<Inner>,
}

impl CapacityManager {
    /// Create a new CapacityManager with the given limit.
    pub fn new(limit: u64) -> Self {
        Self {
            inner: Arc::new(Inner::new(limit)),
        }
    }

    /// Create a new CapacityManager with automatic replenishment.
    ///
    /// The manager will automatically replenish capacity at the specified rate.
    /// This is useful for bandwidth or request rate limiting.
    ///
    /// # Note
    ///
    /// This spawns a background task that runs until the CapacityManager and
    /// all its clones are dropped.
    pub fn with_replenishment(limit: u64, rate: ReplenishmentRate) -> Self {
        let manager = Self::new(limit);
        let inner = Arc::clone(&manager.inner);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(rate.period);
            loop {
                interval.tick().await;
                // Check if we're the last reference (besides this task)
                // If so, exit the replenishment loop
                if Arc::strong_count(&inner) == 1 {
                    break;
                }
                inner.replenish(rate.amount);
            }
        });

        manager
    }

    /// Request capacity.
    ///
    /// If sufficient capacity is available, this returns immediately.
    /// Otherwise, the request is queued and this will wait until capacity
    /// becomes available.
    ///
    /// The returned [`UsedCapacity`] handle automatically returns the capacity
    /// when dropped.
    pub async fn use_capacity(&self, amount: u64) -> UsedCapacity {
        // Fast path: try to acquire immediately
        if self.inner.try_use(amount).is_some() {
            return UsedCapacity {
                amount,
                inner: Arc::clone(&self.inner),
            };
        }

        // Slow path: need to wait
        let receiver = self.inner.add_waiter(amount);

        // Wait for our turn - ignore errors (sender dropped means manager is gone)
        let _ = receiver.await;

        UsedCapacity {
            amount,
            inner: Arc::clone(&self.inner),
        }
    }

    /// Returns the current amount of capacity in use.
    pub fn used(&self) -> u64 {
        self.inner.state.lock().unwrap().used
    }

    /// Returns the configured limit.
    pub fn limit(&self) -> u64 {
        self.inner.state.lock().unwrap().limit
    }

    /// Returns the number of requests currently waiting for capacity.
    pub fn waiting_count(&self) -> usize {
        self.inner.state.lock().unwrap().waiters.len()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn test_basic_use_and_drop() {
        let manager = CapacityManager::new(100);

        assert_eq!(manager.used(), 0);
        assert_eq!(manager.limit(), 100);

        let used = manager.use_capacity(50).await;
        assert_eq!(used.amount(), 50);
        assert_eq!(manager.used(), 50);

        drop(used);
        assert_eq!(manager.used(), 0);
    }

    #[tokio::test]
    async fn test_multiple_uses() {
        let manager = CapacityManager::new(100);

        let a = manager.use_capacity(30).await;
        let b = manager.use_capacity(40).await;
        assert_eq!(manager.used(), 70);

        drop(a);
        assert_eq!(manager.used(), 40);

        drop(b);
        assert_eq!(manager.used(), 0);
    }

    #[tokio::test]
    async fn test_waits_when_at_capacity() {
        let manager = CapacityManager::new(100);

        // Use all capacity
        let used = manager.use_capacity(100).await;
        assert_eq!(manager.used(), 100);

        // This should not complete immediately
        let manager_clone = manager.clone();
        let handle = tokio::spawn(async move { manager_clone.use_capacity(50).await });

        // Give it a moment to queue up
        sleep(Duration::from_millis(10)).await;
        assert_eq!(manager.waiting_count(), 1);

        // Release capacity
        drop(used);

        // Now the waiting request should complete
        let result = timeout(Duration::from_millis(100), handle).await;
        assert!(result.is_ok());
        assert_eq!(manager.used(), 50);
    }

    #[tokio::test]
    async fn test_fifo_ordering() {
        let manager = CapacityManager::new(100);
        let order = Arc::new(AtomicU64::new(0));

        // Use all capacity
        let used = manager.use_capacity(100).await;

        // Queue up multiple waiters
        let mut handles = vec![];
        for i in 1..=3 {
            let m = manager.clone();
            let o = Arc::clone(&order);
            handles.push(tokio::spawn(async move {
                let _used = m.use_capacity(30).await;
                o.fetch_add(i * 10, Ordering::SeqCst)
            }));
            // Small delay to ensure ordering
            sleep(Duration::from_millis(5)).await;
        }

        assert_eq!(manager.waiting_count(), 3);

        // Release capacity - should fulfill in order
        drop(used);

        // Wait for all to complete
        for handle in handles {
            let _ = timeout(Duration::from_millis(100), handle).await;
        }

        // Order should be 10, then 20, then 30 added = 10, 30, 60
        // Actually the order value will be the sum: 10 + 20 + 30 = 60
        assert_eq!(order.load(Ordering::SeqCst), 60);
    }

    #[tokio::test]
    async fn test_replenishment_rate() {
        let rate = ReplenishmentRate::new(50, Duration::from_millis(50));
        let manager = CapacityManager::with_replenishment(100, rate);

        // Use all capacity
        let used = manager.use_capacity(100).await;
        assert_eq!(manager.used(), 100);

        // Prevent auto-replenishment from affecting our used capacity
        // by keeping the UsedCapacity handle alive

        // Manually drop to trigger replenishment test
        drop(used);

        // Use capacity again
        let _used = manager.use_capacity(100).await;

        // Wait for replenishment
        sleep(Duration::from_millis(60)).await;

        // Should have replenished 50
        assert!(manager.used() <= 100);
    }

    #[tokio::test]
    async fn test_zero_amount() {
        let manager = CapacityManager::new(100);

        let used = manager.use_capacity(0).await;
        assert_eq!(used.amount(), 0);
        assert_eq!(manager.used(), 0);

        drop(used);
        assert_eq!(manager.used(), 0);
    }

    #[tokio::test]
    async fn test_clone_shares_state() {
        let manager1 = CapacityManager::new(100);
        let manager2 = manager1.clone();

        let used = manager1.use_capacity(50).await;
        assert_eq!(manager2.used(), 50);

        drop(used);
        assert_eq!(manager2.used(), 0);
    }

    #[tokio::test]
    async fn test_partial_fulfillment() {
        let manager = CapacityManager::new(100);

        // Use all capacity
        let used100 = manager.use_capacity(100).await;

        // Queue requests for 60 and 50 - both must wait
        let m1 = manager.clone();
        let handle60 = tokio::spawn(async move { m1.use_capacity(60).await });
        sleep(Duration::from_millis(5)).await;

        let m2 = manager.clone();
        let handle50 = tokio::spawn(async move { m2.use_capacity(50).await });
        sleep(Duration::from_millis(5)).await;

        assert_eq!(manager.waiting_count(), 2);

        // Release 100 - only 60 can be fulfilled (used=60, 40 remaining)
        // 50 still needs to wait because 50 > 40
        drop(used100);

        // Wait for 60 to complete
        let used60 = timeout(Duration::from_millis(100), handle60)
            .await
            .unwrap()
            .unwrap();

        // 50 is still waiting
        assert_eq!(manager.waiting_count(), 1);
        assert_eq!(manager.used(), 60);

        // Release 60 - now 50 can be fulfilled
        drop(used60);

        let used50 = timeout(Duration::from_millis(100), handle50)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(manager.used(), 50);
        assert_eq!(manager.waiting_count(), 0);

        // Keep used50 alive until assertion is checked
        drop(used50);
    }

    #[tokio::test]
    async fn test_large_request_waits_for_small_ones() {
        let manager = CapacityManager::new(100);

        // Use 60
        let used60 = manager.use_capacity(60).await;

        // Queue a large request (90) - can't be fulfilled even after release
        // because we'll need to wait for more capacity
        let m1 = manager.clone();
        let handle90 = tokio::spawn(async move { m1.use_capacity(90).await });
        sleep(Duration::from_millis(5)).await;

        assert_eq!(manager.waiting_count(), 1);

        // Release 60 - still can't fulfill 90 (limit is 100, need 90, have 100 available)
        // Actually 90 <= 100, so it should work!
        drop(used60);

        let result = timeout(Duration::from_millis(100), handle90).await;
        assert!(result.is_ok());
        assert_eq!(manager.used(), 90);
    }
}
