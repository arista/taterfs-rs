// Thanks ChatGPT

// This queue allows a producer to add futures, then call complete whan done.  The consumer can call next() repeatedly, and will receive the resolved futures in fifo order, or None once complete has been called

use anyhow::{Result, anyhow};
use futures_util::FutureExt; // for .catch_unwind()
use std::panic::AssertUnwindSafe;
use std::{collections::VecDeque, future::Future, sync::Arc};
use tokio::sync::{Mutex, Notify, oneshot};

pub struct FuturesQueue<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

struct Inner<T> {
    queue: VecDeque<oneshot::Receiver<anyhow::Result<T>>>,
    completed: bool,
    notify: Arc<Notify>,
}

impl<T: Send + 'static> FuturesQueue<T> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                queue: VecDeque::new(),
                completed: false,
                notify: Arc::new(Notify::new()),
            })),
        }
    }

    /// Add a future producing `anyhow::Result<T>`. It runs concurrently.
    /// Non-async API; we push the receiver under lock in a tiny spawned task.
    pub fn add<F>(&self, fut: F)
    where
        F: Future<Output = Result<T>> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<Result<T>>();

        // Spawn the user future; convert panics to Err(anyhow!)
        tokio::spawn(async move {
            let sent: Result<T> = AssertUnwindSafe(fut)
                .catch_unwind()
                .await
                .map_err(|_| anyhow!("task panicked"))
                .and_then(|r| r);
            let _ = tx.send(sent);
        });

        // Push the receiver and notify any waiter.
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            let mut g = inner.lock().await;
            g.queue.push_back(rx);
            g.notify.notify_one();
        });
    }

    /// Signal no more items will be added.
    pub fn complete(&self) {
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            let mut g = inner.lock().await;
            g.completed = true;
            g.notify.notify_waiters();
        });
    }

    /// Await the next result in FIFO order.
    /// - Some(Ok(T))  : next item finished successfully
    /// - Some(Err(_)) : next item errored/panicked/cancelled
    /// - None         : completed and drained
    pub async fn next(&self) -> Option<anyhow::Result<T>> {
        loop {
            let wait_on: Option<Arc<Notify>> = {
                let mut g = self.inner.lock().await;

                if let Some(rx) = g.queue.pop_front() {
                    drop(g);
                    return Some(match rx.await {
                        Ok(r) => r,
                        Err(_) => Err(anyhow::anyhow!("task cancelled before sending result")),
                    });
                } else if g.completed {
                    return None;
                } else {
                    // Clone the Arc so it outlives the lock guard
                    Some(Arc::clone(&g.notify))
                }
            };

            if let Some(n) = wait_on {
                n.notified().await;
            }
        }
    }
}

impl<T> Clone for FuturesQueue<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
