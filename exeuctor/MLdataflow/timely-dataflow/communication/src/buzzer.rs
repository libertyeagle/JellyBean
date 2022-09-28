// READ
//! A type that can unpark specific threads.

use std::thread::Thread;

/// Can unpark a specific thread.
#[derive(Clone)]
pub struct Buzzer {
    thread: Thread,
}

impl Buzzer {
    /// Creates a new buzzer for the current thread.
    pub fn new() -> Self {
        Self {
            // get thread handler of current thread
            thread: std::thread::current()
        }
    }
    /// Unparks the target thread.
    pub fn buzz(&self) {
        // Every thread is equipped with some basic low-level blocking support,
        // via the thread::park function and thread::Thread::unpark method. 
        // park blocks the current thread, which can then be resumed from another thread
        // by calling the unpark method on the blocked thread’s handle.
        self.thread.unpark()
    }
}