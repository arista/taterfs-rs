// Thanks ChatGPT

use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};
use tokio::time::{self, Duration, Instant};

#[derive(Clone, Debug)]
pub struct ThroughputManager {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    bytes_per_sec: u64,
    capacity: u64, // burst bytes
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    // token bucket
    available: u64,
    // precise integer refill (carry remainder of bytes*ns / 1e9)
    accum_ns_bytes: u128,
    last: Instant,
    // FIFO of waiters
    queue: VecDeque<Waiter>,
    running: bool,
}

#[derive(Debug)]
struct Waiter {
    amount: u64,
    tx: oneshot::Sender<()>,
}

impl ThroughputManager {
    /// `rate_bps`: sustained rate in bytes/sec.
    /// `burst_seconds`: capacity = rate * burst_seconds. Choose >= your max chunk (e.g., ≥ 10 MiB).
    pub fn new(rate_bps: u64, burst_seconds: u64) -> Self {
        assert!(rate_bps > 0);
        let capacity = rate_bps.saturating_mul(burst_seconds);
        let inner = Arc::new(Inner {
            bytes_per_sec: rate_bps,
            capacity,
            state: Mutex::new(State {
                available: capacity, // start full (allow initial burst)
                accum_ns_bytes: 0,
                last: Instant::now(),
                queue: VecDeque::new(),
                running: true,
            }),
        });

        // Refill loop
        let weak = Arc::downgrade(&inner);
        tokio::spawn(async move {
            let mut intv = time::interval(Duration::from_millis(50)); // ~20 Hz
            intv.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            loop {
                intv.tick().await;
                let Some(inner) = weak.upgrade() else { break };
                let mut st = inner.state.lock().await;
                if !st.running {
                    break;
                }

                // Refill based on elapsed time
                let now = Instant::now();
                let elapsed = now.saturating_duration_since(st.last);
                if elapsed.is_zero() {
                    continue;
                }
                st.last = now;

                let add_ns_bytes = (inner.bytes_per_sec as u128) * elapsed.as_nanos();
                st.accum_ns_bytes = st.accum_ns_bytes.saturating_add(add_ns_bytes);
                let add_bytes = (st.accum_ns_bytes / 1_000_000_000u128) as u64;
                st.accum_ns_bytes %= 1_000_000_000u128;

                if add_bytes > 0 {
                    st.available = (st.available + add_bytes).min(inner.capacity);
                }

                // Serve as many FIFO waiters as fit
                while let Some(front) = st.queue.front() {
                    if st.available < front.amount {
                        break; // HoL blocking by design (strict FIFO)
                    }
                    let Waiter { amount, tx } = st.queue.pop_front().unwrap();
                    st.available -= amount;
                    // If receiver is gone (cancelled/timeout), refund tokens
                    if tx.send(()).is_err() {
                        st.available = st.available.saturating_add(amount);
                    }
                }
            }
        });

        Self { inner }
    }

    pub fn bytes_per_sec(&self) -> u64 {
        self.inner.bytes_per_sec
    }
    pub fn capacity(&self) -> u64 {
        self.inner.capacity
    }

    pub async fn available(&self) -> u64 {
        let st = self.inner.state.lock().await;
        st.available
    }

    /// Wait until `amount` tokens are available, then deduct and return.
    /// IMPORTANT: If `amount > capacity`, this will never complete.
    pub async fn reserve(&self, amount: u64) {
        assert!(amount > 0, "amount must be > 0");
        // Fast path: try to take immediately
        {
            let mut st = self.inner.state.lock().await;
            if st.available >= amount {
                st.available -= amount;
                return;
            }
            // Enqueue and await
            let (tx, rx) = oneshot::channel();
            st.queue.push_back(Waiter { amount, tx });
            drop(st);
            // Await completion (or cancellation/timeout by the caller if they wrap this)
            let _ = rx.await;
        }
    }

    /// Try to reserve immediately; returns false if not enough tokens.
    pub async fn try_reserve(&self, amount: u64) -> bool {
        assert!(amount > 0);
        let mut st = self.inner.state.lock().await;
        if st.available >= amount {
            st.available -= amount;
            true
        } else {
            false
        }
    }

    /// Graceful shutdown (mostly for tests)
    pub async fn shutdown(&self) {
        let mut st = self.inner.state.lock().await;
        st.running = false;
    }
}
