// READ
//! Types and traits for sharing `Bytes`.

use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

use bytes::arc::Bytes;
use super::bytes_slab::BytesSlab;

/// A target for `Bytes`.
pub trait BytesPush {
    // /// Pushes bytes at the instance.
    // fn push(&mut self, bytes: Bytes);
    /// Pushes many bytes at the instance.
    // I needs to be a type that implements IntoIterator that can yield Bytes
    fn extend<I: IntoIterator<Item=Bytes>>(&mut self, iter: I);
}
/// A source for `Bytes`.
pub trait BytesPull {
    // /// Pulls bytes from the instance.
    // fn pull(&mut self) -> Option<Bytes>;
    /// Drains many bytes from the instance.
    fn drain_into(&mut self, vec: &mut Vec<Bytes>);
}

use std::sync::atomic::{AtomicBool, Ordering};
/// An unbounded queue of bytes intended for point-to-point communication
/// between threads. Cloning returns another handle to the same queue.
///
/// TODO: explain "extend"
#[derive(Clone)]
pub struct MergeQueue {
    queue: Arc<Mutex<VecDeque<Bytes>>>, // queue of bytes.
    buzzer: crate::buzzer::Buzzer,  // awakens receiver thread.
    panic: Arc<AtomicBool>,
}

impl MergeQueue {
    /// Allocates a new queue with an associated signal.
    pub fn new(buzzer: crate::buzzer::Buzzer) -> Self {
        MergeQueue {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            buzzer,
            panic: Arc::new(AtomicBool::new(false)),
        }
    }
    /// Indicates that all input handles to the queue have dropped.
    pub fn is_complete(&self) -> bool {
        // atomic value's load method takes an Ordering
        // SeqCst means Sequentially Consistent
        // which basically guarantees everyone will see that instruction as having occurred wherever you put it relative to other instructions
        // sequential execution within each thread
        // http://gcc.gnu.org/wiki/Atomic/GCCMM/AtomicSync
        if self.panic.load(Ordering::SeqCst) { panic!("MergeQueue poisoned."); }
        // the only one left is the receiver handle
        Arc::strong_count(&self.queue) == 1 && self.queue.lock().expect("Failed to acquire lock").is_empty()
    }
}

impl BytesPush for MergeQueue {
    fn extend<I: IntoIterator<Item=Bytes>>(&mut self, iterator: I) {

        if self.panic.load(Ordering::SeqCst) { panic!("MergeQueue poisoned."); }

        // try to acquire lock without going to sleep (Rust's lock() might yield)
        let mut lock_ok = self.queue.try_lock();
        while let Result::Err(::std::sync::TryLockError::WouldBlock) = lock_ok {
            lock_ok = self.queue.try_lock();
        }
        let mut queue = lock_ok.expect("MergeQueue mutex poisoned.");

        let mut iterator = iterator.into_iter();
        let mut should_ping = false;
        // consumes Bytes from into_iter
        if let Some(bytes) = iterator.next() {
            // extract the tail element
            let mut tail = if let Some(mut tail) = queue.pop_back() {
                // there is a chance that, if the tail Bytes and the current Bytes share the underlying memory space
                // we might be able to merge them
                if let Err(bytes) = tail.try_merge(bytes) {
                    // merge fail, then we need to replace tail with bytes, and then push back the original tail
                    // queue is MutexGuard<VecDeque<Bytes>>
                    // it is first got dereferenced to *queue (VecDeque<Bytes>)
                    // then mutable reference is added to obtain &mut *queue
                    // push_back is then called with &mut (*queue)
                    queue.push_back(::std::mem::replace(&mut tail, bytes));
                }
                // note that the queue stays at the same length
                // tail is now bytes (the current element from the iterator)
                tail
            }
            else {
                // empty queue, we should signal
                should_ping = true;
                bytes
            };

            // for the remaining elements (bytes) in the iterator
            for bytes in iterator {
                // try to merge it with the tail
                if let Err(bytes) = tail.try_merge(bytes) {
                    // if not successful, push back the original tail, and make replace tail with bytes
                    queue.push_back(::std::mem::replace(&mut tail, bytes));
                }
            }
            // tail is not pushed to the queue until this point
            queue.push_back(tail);
        }

        // Wakeup corresponding thread *after* releasing the lock
        // unlock the Mutex by dropping the MutexGuard
        ::std::mem::drop(queue);
        if should_ping {
            self.buzzer.buzz();  // only signal from empty to non-empty.
        }
    }
}

impl BytesPull for MergeQueue {
    fn drain_into(&mut self, vec: &mut Vec<Bytes>) {
        if self.panic.load(Ordering::SeqCst) { panic!("MergeQueue poisoned."); }

        // try to acquire lock without going to sleep (Rust's lock() might yield)
        let mut lock_ok = self.queue.try_lock();
        while let Result::Err(::std::sync::TryLockError::WouldBlock) = lock_ok {
            lock_ok = self.queue.try_lock();
        }
        let mut queue = lock_ok.expect("MergeQueue mutex poisoned.");

        // After using drain, the Vec is empty but the storage previously allocated for its elements remains allocated.
        // This means that you can insert new elements in the Vec without having to allocate storage for them until you reach the Vec's capacity.
        vec.extend(queue.drain(..));
    }
}

// We want to ping in the drop because a channel closing can unblock a thread waiting on
// the next bit of data to show up.
// but as we drop MergeQueue, the next bit will never show up
// we can just wake the thread
impl Drop for MergeQueue {
    fn drop(&mut self) {
        // Propagate panic information, to distinguish between clean and unclean shutdown.
        if ::std::thread::panicking() {
            self.panic.store(true, Ordering::SeqCst);
        }
        else {
            // TODO: Perhaps this aggressive ordering can relax orderings elsewhere.
            if self.panic.load(Ordering::SeqCst) { panic!("MergeQueue poisoned."); }
        }
        // Drop the queue before pinging.
        self.queue = Arc::new(Mutex::new(VecDeque::new()));
        self.buzzer.buzz();
    }
}


/// A `BytesPush` wrapper which stages writes.
pub struct SendEndpoint<P: BytesPush> {
    send: P,
    buffer: BytesSlab,
}

impl<P: BytesPush> SendEndpoint<P> {

    /// Moves `self.buffer` into `self.send`, replaces with empty buffer.
    fn send_buffer(&mut self) {
        let valid_len = self.buffer.valid().len();
        if valid_len > 0 {
            // self.buffer.extract returns a Bytes
            // Option<T> also implments IntoIter
            // the iterator yields one value if the Option is a Some, otherwise none.
            // after extract, self.buffer's `valid` field should be 0
            self.send.extend(Some(self.buffer.extract(valid_len)));
        }
    }

    /// Allocates a new `BytesSendEndpoint` from a shared queue.
    pub fn new(queue: P) -> Self {
        SendEndpoint {
            // the queue can be cloned and shared across threads
            send: queue,
            buffer: BytesSlab::new(20),
        }
    }
    /// Makes the next `bytes` bytes valid.
    ///
    /// The current implementation also sends the bytes, to ensure early visibility.
    // after we wrote to the slice returned from reserve, we need to call make_valid to inform that new data is now available.
    pub fn make_valid(&mut self, bytes: usize) {
        self.buffer.make_valid(bytes);
        self.send_buffer();
    }
    /// Acquires a prefix of `self.empty()` of length at least `capacity`.
    // we can write through here. It returns a slice of writable u8 bytes of length at least `capacity`.
    pub fn reserve(&mut self, capacity: usize) -> &mut [u8] {

        if self.buffer.empty().len() < capacity {
            // in this case, new buffer will be allocated
            self.send_buffer();
            self.buffer.ensure_capacity(capacity);
        }

        assert!(self.buffer.empty().len() >= capacity);
        self.buffer.empty()
    }
    /// Marks all written data as valid, makes visible.
    pub fn publish(&mut self) {
        self.send_buffer();
    }
}

impl<P: BytesPush> Drop for SendEndpoint<P> {
    fn drop(&mut self) {
        self.send_buffer();
    }
}

