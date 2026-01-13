//! Request deduplication for concurrent async operations.
//!
//! The [`Dedup`] utility combines concurrent requests with the same key into a single
//! operation. When multiple tasks request the same key simultaneously, only one
//! executes the underlying operation while others wait for its result.
//!
//! This is useful for avoiding redundant backend calls when multiple parts of an
//! application request the same resource concurrently.
//!
//! # Cancellation
//!
//! **Note:** If the "leader" task (the one executing the operation) is cancelled,
//! any waiting "follower" tasks will wait indefinitely. Callers should ensure that
//! leader tasks are not cancelled, or use timeouts on the caller side.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, OnceCell};

/// Internal state for a single in-flight request.
struct Waiter<V, E> {
    /// Holds the result once the leader completes.
    result: OnceCell<Result<V, E>>,
    /// Notifies waiting followers when the result is ready.
    notify: Notify,
}

/// Request deduplication for concurrent async operations.
///
/// `Dedup` ensures that concurrent calls with the same key result in only one
/// actual execution of the underlying operation. Additional callers wait for
/// and receive a clone of the result.
///
/// # Example
///
/// ```ignore
/// use taterfs_rs::util::Dedup;
///
/// let dedup: Dedup<String, Vec<u8>, std::io::Error> = Dedup::new();
///
/// // Multiple concurrent calls with the same key will only execute once
/// let result = dedup.call("key".to_string(), || async {
///     // expensive operation
///     Ok(vec![1, 2, 3])
/// }).await;
/// ```
pub struct Dedup<K, V, E> {
    in_flight: Mutex<HashMap<K, Arc<Waiter<V, E>>>>,
}

impl<K, V, E> Dedup<K, V, E>
where
    K: Hash + Eq + Clone + Send,
    V: Clone + Send,
    E: Clone + Send,
{
    /// Create a new `Dedup` instance.
    pub fn new() -> Self {
        Self {
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Execute an async operation with request deduplication.
    ///
    /// If no request with this key is currently in flight, the provided function
    /// is executed and its result is returned. If a request with this key is
    /// already in flight, this call waits for that request to complete and
    /// returns a clone of its result.
    ///
    /// # Arguments
    ///
    /// * `key` - The key identifying this request. Requests with equal keys are deduplicated.
    /// * `f` - A function that produces the future to execute if this is the first request.
    ///
    /// # Returns
    ///
    /// The result of the operation, either from executing `f` or from the in-flight request.
    pub async fn call<F, Fut>(&self, key: K, f: F) -> Result<V, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
    {
        let (waiter, is_leader) = {
            let mut map = self.in_flight.lock().unwrap();
            if let Some(waiter) = map.get(&key) {
                (Arc::clone(waiter), false)
            } else {
                let waiter = Arc::new(Waiter {
                    result: OnceCell::new(),
                    notify: Notify::new(),
                });
                map.insert(key.clone(), Arc::clone(&waiter));
                (waiter, true)
            }
        };

        if is_leader {
            let result = f().await;

            // Store the result and notify all waiters
            // Ignore error from set() - shouldn't happen since we're the only writer
            let _ = waiter.result.set(result.clone());
            waiter.notify.notify_waiters();

            // Clean up the in-flight entry
            self.in_flight.lock().unwrap().remove(&key);

            result
        } else {
            // Wait for the leader to complete
            // Use a loop to handle the race between checking result and waiting
            loop {
                // Register for notification before checking result
                let notified = waiter.notify.notified();

                // Check if result is already available
                if let Some(result) = waiter.result.get() {
                    return result.clone();
                }

                // Wait for notification
                notified.await;
            }
        }
    }

    /// Returns the number of requests currently in flight.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.lock().unwrap().len()
    }
}

impl<K, V, E> Default for Dedup<K, V, E>
where
    K: Hash + Eq + Clone + Send,
    V: Clone + Send,
    E: Clone + Send,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_single_call() {
        let dedup: Dedup<String, i32, ()> = Dedup::new();

        let result = dedup.call("key".to_string(), || async { Ok(42) }).await;

        assert_eq!(result, Ok(42));
        assert_eq!(dedup.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn test_single_call_error() {
        let dedup: Dedup<String, i32, String> = Dedup::new();

        let result = dedup
            .call("key".to_string(), || async { Err("error".to_string()) })
            .await;

        assert_eq!(result, Err("error".to_string()));
        assert_eq!(dedup.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn test_concurrent_same_key_deduplicates() {
        let dedup: Arc<Dedup<String, i32, ()>> = Arc::new(Dedup::new());
        let call_count = Arc::new(AtomicU32::new(0));

        let mut handles = vec![];

        // Spawn multiple concurrent requests with the same key
        for _ in 0..5 {
            let dedup = Arc::clone(&dedup);
            let call_count = Arc::clone(&call_count);

            handles.push(tokio::spawn(async move {
                dedup
                    .call("same-key".to_string(), || {
                        let call_count = Arc::clone(&call_count);
                        async move {
                            call_count.fetch_add(1, Ordering::SeqCst);
                            sleep(Duration::from_millis(50)).await;
                            Ok(42)
                        }
                    })
                    .await
            }));
        }

        // Wait for all to complete
        for handle in handles {
            let result = handle.await.unwrap();
            assert_eq!(result, Ok(42));
        }

        // The underlying function should only have been called once
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        assert_eq!(dedup.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn test_different_keys_not_deduplicated() {
        let dedup: Arc<Dedup<String, i32, ()>> = Arc::new(Dedup::new());
        let call_count = Arc::new(AtomicU32::new(0));

        let mut handles = vec![];

        // Spawn requests with different keys
        for i in 0..5 {
            let dedup = Arc::clone(&dedup);
            let call_count = Arc::clone(&call_count);
            let key = format!("key-{}", i);

            handles.push(tokio::spawn(async move {
                dedup
                    .call(key, || {
                        let call_count = Arc::clone(&call_count);
                        async move {
                            call_count.fetch_add(1, Ordering::SeqCst);
                            sleep(Duration::from_millis(50)).await;
                            Ok(42)
                        }
                    })
                    .await
            }));
        }

        // Wait for all to complete
        for handle in handles {
            let result = handle.await.unwrap();
            assert_eq!(result, Ok(42));
        }

        // Each key should have its own call
        assert_eq!(call_count.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn test_sequential_same_key_not_deduplicated() {
        let dedup: Dedup<String, i32, ()> = Dedup::new();
        let call_count = Arc::new(AtomicU32::new(0));

        // First call
        {
            let call_count = Arc::clone(&call_count);
            let result = dedup
                .call("key".to_string(), || {
                    let call_count = Arc::clone(&call_count);
                    async move {
                        call_count.fetch_add(1, Ordering::SeqCst);
                        Ok(1)
                    }
                })
                .await;
            assert_eq!(result, Ok(1));
        }

        // Second call after first completes
        {
            let call_count = Arc::clone(&call_count);
            let result = dedup
                .call("key".to_string(), || {
                    let call_count = Arc::clone(&call_count);
                    async move {
                        call_count.fetch_add(1, Ordering::SeqCst);
                        Ok(2)
                    }
                })
                .await;
            assert_eq!(result, Ok(2));
        }

        // Both calls should have executed since they were sequential
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_error_propagates_to_all_waiters() {
        let dedup: Arc<Dedup<String, i32, String>> = Arc::new(Dedup::new());

        let mut handles = vec![];

        // Spawn multiple concurrent requests
        for _ in 0..3 {
            let dedup = Arc::clone(&dedup);

            handles.push(tokio::spawn(async move {
                dedup
                    .call("key".to_string(), || async {
                        sleep(Duration::from_millis(50)).await;
                        Err("shared error".to_string())
                    })
                    .await
            }));
        }

        // All should receive the same error
        for handle in handles {
            let result = handle.await.unwrap();
            assert_eq!(result, Err("shared error".to_string()));
        }
    }

    #[tokio::test]
    async fn test_in_flight_count() {
        let dedup: Arc<Dedup<String, i32, ()>> = Arc::new(Dedup::new());

        assert_eq!(dedup.in_flight_count(), 0);

        let dedup_clone = Arc::clone(&dedup);
        let handle = tokio::spawn(async move {
            dedup_clone
                .call("key".to_string(), || async {
                    sleep(Duration::from_millis(100)).await;
                    Ok(42)
                })
                .await
        });

        // Give the task time to start
        sleep(Duration::from_millis(10)).await;
        assert_eq!(dedup.in_flight_count(), 1);

        handle.await.unwrap().unwrap();
        assert_eq!(dedup.in_flight_count(), 0);
    }
}
