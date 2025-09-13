// Thanks ChatGPT
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};

#[derive(Clone, Debug)]
pub struct ReservationManager {
    capacity: u32,
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    reserved: u32,
    // FIFO of pending requests
    queue: VecDeque<Waiter>,
}

#[derive(Debug)]
struct Waiter {
    amount: u32,
    tx: oneshot::Sender<Reservation>,
}

impl ReservationManager {
    pub fn new(capacity: u32) -> Self {
        Self {
            capacity,
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    reserved: 0,
                    queue: VecDeque::new(),
                }),
            }),
        }
    }

    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    pub async fn reserved(&self) -> u32 {
        self.inner.state.lock().await.reserved
    }

    pub async fn available(&self) -> u32 {
        let st = self.inner.state.lock().await;
        self.capacity - st.reserved
    }

    /// Wait until `available >= amount`, then returns a guard that holds the reservation.
    /// NOTE: If `amount > capacity`, this will never complete (guard with your own check if desired).
    pub async fn reserve(&self, amount: u32) -> Reservation {
        debug_assert!(amount <= self.capacity, "request exceeds capacity");
        // Fast path: take immediately if possible.
        {
            let mut st = self.inner.state.lock().await;
            if self.capacity - st.reserved >= amount {
                st.reserved += amount;
                return Reservation {
                    amount,
                    mgr: self.clone(),
                };
            }
            // Otherwise, enqueue and await.
            let (tx, rx) = oneshot::channel();
            st.queue.push_back(Waiter { amount, tx });
            // lock dropped here
            drop(st);
            // Await the granted Reservation
            // (If caller cancels, this await is aborted; the queued waiter will be cleaned when it reaches the head.)
            return rx.await.expect("manager dropped while waiting");
        }
    }

    /// Try to reserve immediately; returns `Some(Reservation)` on success, `None` otherwise.
    pub async fn try_reserve(&self, amount: u32) -> Option<Reservation> {
        if amount == 0 || amount > self.capacity {
            return None;
        }
        let mut st = self.inner.state.lock().await;
        if self.capacity - st.reserved >= amount {
            st.reserved += amount;
            Some(Reservation {
                amount,
                mgr: self.clone(),
            })
        } else {
            None
        }
    }

    // Called when capacity may have changed (e.g., on drop of a Reservation).
    async fn serve_waiters(&self) {
        let mut st = self.inner.state.lock().await;

        while let Some(front) = st.queue.front() {
            if self.capacity - st.reserved < front.amount {
                break; // head-of-line blocking by design (strict FIFO)
            }
            let Waiter { amount, tx } = st.queue.pop_front().unwrap();
            st.reserved += amount;

            // Build the guard *now* so the amount is accounted for before waking the waiter.
            let guard = Reservation {
                amount,
                mgr: self.clone(),
            };

            // If the receiver is gone (task cancelled), immediately refund and continue.
            if tx.send(guard).is_err() {
                st.reserved = st.reserved.saturating_sub(amount);
                continue;
            }
        }
    }
}

#[derive(Debug)]
pub struct Reservation {
    amount: u32,
    mgr: ReservationManager,
}

impl Reservation {
    pub fn amount(&self) -> u32 {
        self.amount
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        // Release capacity and try to serve queued waiters.
        let mgr = self.mgr.clone();
        let amt = self.amount;
        tokio::spawn(async move {
            let mut st = mgr.inner.state.lock().await;
            st.reserved = st.reserved.saturating_sub(amt);
            drop(st); // avoid holding the lock across the follow-up call
            mgr.serve_waiters().await;
        });
    }
}
