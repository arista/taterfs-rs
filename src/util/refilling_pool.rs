// Represents a pool that refills over time.  The pool is created with a maximum capacity and a refill rate (amount, period).  Clients call remove() to wait for the pool to contain a specified amount, and receive a Removed.  Later, the client can call actually_removed on the Removed to report how much was actually removed.
//
// A typical application is to limit throughput, in which the refill rate represents the throughput, and the maximum capacity represents a "burst" capacity.  A client would wait for the pool to contain an amount based on the estimated number of bytes to be read or written.  Then when the operation completes, the client reports how many bytes were actually read/written so that the pool can update its amounts correctly.

use std::time::Instant;
use std::{cell::RefCell, collections::VecDeque, rc::Rc};

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

pub struct RefillingPool {
    state: Rc<RefCell<State>>,
}

struct State {
    ctx: RefillingPoolContext,
    // Note that available can go negative due to corrections sent through Removed.actually_removed()
    available: RefillingAmount,
    // The queue of those waiting for an item to become available
    waiters: VecDeque<Waiter>,

    refilling_process: Option<JoinHandle<()>>,
}

// Abstraction of a value that "refills" at a particular rate up to a given capacity.  The value doesn't actually refill in real time - instead, whenever has_at_least is called, it recomputes how much should be refilled based on the last time has_at_least was called
struct RefillingAmount {
    amount: i64,
    capacity: u64,
    refill_amount_per_sec: u32,
    last_refill: Instant,
}

impl RefillingAmount {
    pub fn new(amount: i64, capacity: u64, refill_amount_per_sec: u32) -> RefillingAmount {
        Self {
            amount,
            capacity,
            refill_amount_per_sec,
            last_refill: Instant::now(),
        }
    }

    pub fn adjust(&mut self, amount: i64) {
        self.clamp_to_capacity(self.amount + amount);
    }

    pub fn has_at_least(&mut self, amount: i64) -> bool {
        self.update_amount();
        self.amount >= amount
    }

    fn clamp_to_capacity(&self, amount: i64) -> i64 {
        let capacity = i64::try_from(self.capacity).unwrap();
        if amount <= capacity { amount } else { capacity }
    }

    fn update_amount(&mut self) {
        let now = Instant::now();
        let (refill_amount, last_refill) =
            Self::refill_at_rate_per_sec(self.last_refill, now, self.refill_amount_per_sec);
        self.amount += i64::try_from(refill_amount).unwrap();
        self.last_refill = last_refill;
    }

    fn refill_at_rate_per_sec(
        last_refill: Instant,
        now: Instant,
        refill_per_sec: u32,
    ) -> (u64, Instant) {
        if refill_per_sec == 0 || now <= last_refill {
            return (0, last_refill);
        }

        // All math in integers (u128 to avoid overflow).
        const NS_PER_SEC: u128 = 1_000_000_000;
        let elapsed_ns: u128 = now.duration_since(last_refill).as_nanos();

        let rate: u128 = refill_per_sec as u128;

        // A) how much to refill = floor(elapsed_ns * rate / 1e9)
        let tokens: u128 = (elapsed_ns.saturating_mul(rate)) / NS_PER_SEC;

        if tokens == 0 {
            return (0, last_refill);
        }

        // B) advance last_refill by the exact time that produced `tokens`:
        // consumed_ns = floor(tokens * 1e9 / rate)
        let consumed_ns: u128 = tokens.saturating_mul(NS_PER_SEC) / rate;

        // Safe to cast: consumed_ns <= elapsed_ns, and Duration::from_nanos takes u64.
        let consumed = Duration::from_nanos(consumed_ns as u64);
        let new_last = last_refill + consumed;

        (tokens as u64, new_last)
    }
}

impl State {
    fn adjust_pool(&mut self, amount: i64) {
        self.available.adjust(amount);
        self.check_waiters();
    }

    fn check_waiters(&mut self) {
        match self.waiters.front() {
            Some(waiter) => {
                if self.available.has_at_least(i64::from(waiter.amount)) {
                    self.waiters.pop_front().unwrap().tx.send(()).unwrap();
                }
            }
            None => (),
        }
    }
}

struct Waiter {
    amount: u32,
    tx: oneshot::Sender<()>,
}

pub struct Removed {
    state: Rc<RefCell<State>>,
    amount: u64,
}

impl Removed {
    pub fn actually_removed(&self, amount: u64) {
        let original_amount = i64::try_from(self.amount).unwrap();
        let actual_amount = i64::try_from(amount).unwrap();
        // if we originally requested more than we actually used, then add the difference back to the capacity (and vice versa)
        let adjustment: i64 = original_amount - actual_amount;
        if adjustment != 0 {
            self.state.borrow_mut().adjust_pool(adjustment);
        }
    }
}

#[derive(Copy, Clone)]
pub struct RefillingPoolContext {
    capacity: u64,
    refill_amount_per_sec: u32,
    refill_interval_ms: u32,
}

impl RefillingPool {
    pub fn new(ctx: RefillingPoolContext) -> Self {
        let ret = Self {
            state: Rc::new(RefCell::new(State {
                ctx,
                available: RefillingAmount::new(
                    i64::try_from(ctx.capacity).unwrap(),
                    ctx.capacity,
                    ctx.refill_amount_per_sec,
                ),
                waiters: VecDeque::new(),
                refilling_process: None,
            })),
        };
        ret.state.borrow_mut().refilling_process = Some(ret.start_refilling());
        ret
    }

    pub fn start_refilling(&self) -> JoinHandle<()> {
        let state = self.state.clone();
        tokio::task::spawn_local(async move {
            let _state = state;
            let mut interval = time::interval(Duration::from_millis(u64::from(_state.borrow().ctx.refill_interval_ms)));
            loop {
                interval.tick().await;
                _state.borrow_mut().check_waiters();
            }
        })
    }
    
    // Wait until the given amount is available in the pool, then removes it
    pub async fn remove(&self, amount: u32) -> Removed {
        if self.can_remove_immediately(amount) {
            self.do_remove(amount)
        } else {
            let (tx, rx) = oneshot::channel();
            self.state
                .borrow_mut()
                .waiters
                .push_back(Waiter { amount, tx });
            rx.await.unwrap();
            self.do_remove(amount)
        }
    }

    // true if the given amount is available
    fn can_remove(&self, amount: u32) -> bool {
        self.state
            .borrow_mut()
            .available
            .has_at_least(i64::from(amount))
    }

    // true if the given amount is available, and there are no waiters
    fn can_remove_immediately(&self, amount: u32) -> bool {
        self.can_remove(amount) && self.state.borrow().waiters.is_empty()
    }

    fn do_remove(&self, amount: u32) -> Removed {
        self.state.borrow_mut().available.adjust(-i64::from(amount));
        Removed {
            state: self.state.clone(),
            amount: amount.into(),
        }
    }
}

impl Drop for RefillingPool {
    fn drop(&mut self) {
        match &self.state.borrow_mut().refilling_process {
            Some(h) => {
                h.abort();
            }
            None => ()
        }
    }
}
