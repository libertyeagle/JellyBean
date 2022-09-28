// READ
//! A helper struct to report when something has been dropped.

use std::rc::Rc;
use std::cell::RefCell;

/// An opaque type that reports when it is dropped.
pub struct Canary {
    index: usize,
    // it pushes an index (let's say, channel idx) into a thread-local shared queue
    queue: Rc<RefCell<Vec<usize>>>,
}

impl Canary {
    /// Allocates a new drop canary.
    pub fn new(index: usize, queue: Rc<RefCell<Vec<usize>>>) -> Self {
        Canary { index, queue }
    }
}

impl Drop for Canary {
    fn drop(&mut self) {
        self.queue.borrow_mut().push(self.index);
    }
}