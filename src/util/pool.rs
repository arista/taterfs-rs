// Represents a pool of items that can be checked out one at a time.  checkout() will wait until an item becomes available, and will return an "InUse" that contains that item.  When the InUse goes out of scope, the item is returned.
//
// When a pool is constructed, it must also be initialized with a set of items by calling add()
//
// This is only intended to be used in a single-threaded context

use anyhow::Result;
use std::{
    cell::RefCell,
    collections::VecDeque,
    ops::{Deref, DerefMut},
    rc::Rc,
};
use tokio::sync::oneshot;

pub struct Pool<T> {
    state: Rc<RefCell<State<T>>>,
}

pub struct InUse<T> {
    state: Rc<RefCell<State<T>>>,
    item: Option<T>,
}

impl<T> Deref for InUse<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.item.as_ref().expect("InUse missing item")
    }
}

impl<T> DerefMut for InUse<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.item.as_mut().expect("InUse missing item")
    }
}

impl<T> Drop for InUse<T> {
    fn drop(&mut self) {
        // Move the item back into the list of items
        match self.item.take() {
            Some(item) => {
                self.state.borrow_mut().add(item);
            }
            None => {
                println!("Assertion failed: InUse does not contain an item");
            }
        }
    }
}

struct State<T> {
    // The list of items available for use
    items: Vec<T>,
    // The queue of those waiting for an item to become available
    waiters: VecDeque<Waiter>,
}

struct Waiter {
    tx: oneshot::Sender<()>,
}

impl<T> State<T> {
    pub fn add(&mut self, item: T) {
        self.items.push(item);
        self.check_waiters();
    }

    // Notify the first waiter that an item is available
    fn check_waiters(&mut self) {
        match self.waiters.pop_front() {
            Some(waiter) => {
                waiter
                    .tx
                    .send(())
                    .expect("Assertion failed: Pool.check_waiters failed to send");
            }
            None => (),
        }
    }
}

impl<T> Pool<T> {
    pub fn new() -> Pool<T> {
        Pool {
            state: Rc::new(RefCell::new(State {
                items: Vec::new(),
                waiters: VecDeque::new(),
            })),
        }
    }

    pub fn add(&self, item: T) {
        self.state.borrow_mut().add(item);
    }

    pub async fn checkout(&self) -> InUse<T> {
        // Return an item if one is available
        if !self.state.borrow_mut().items.is_empty() {
            self.put_in_use()
        }
        // Otherwise queue a waiter, to be notified when an item
        // becomes available
        else {
            let (tx, rx) = oneshot::channel();
            self.state.borrow_mut().waiters.push_back(Waiter { tx });
            rx.await
                .expect("Assertion failed: error while waiting for item");
            self.put_in_use()
        }
    }

    pub fn put_in_use(&self) -> InUse<T> {
        let item = self
            .state
            .borrow_mut()
            .items
            .pop()
            .expect("Assertion failed: items not expected to be empty");
        InUse {
            state: self.state.clone(),
            item: Some(item),
        }
    }
}

impl<T> Clone for Pool<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}
