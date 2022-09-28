// READ, Sep 12 2021
//! The exchange pattern distributes pushed data between many target pushees.

use timely_communication::MessageLatency;
use crate::Data;
use crate::communication::Push;
use crate::dataflow::channels::{Bundle, Message};

// TODO : Software write combining
/// Distributes records among target pushees according to a distribution function.
pub struct Exchange<T, D, P: Push<Bundle<T, D>>, H: FnMut(&T, &D) -> u64> {
    // distribute records among multiple pushers according to the hash function
    // the hash function takes timestamp and record
    // T: timestamp's type
    // D: data's type
    // P is pusher to type crate::communication::Message<Message<T, D>>;
    // i.e., timely dataflow's message embedded in P
    // FnMut: closures that cpature variables in the context by mutable reference
    pushers: Vec<P>,
    buffers: Vec<Vec<D>>,
    // current may be None
    current: Option<T>,
    hash_func: H,
}

impl<T: Clone, D, P: Push<Bundle<T, D>>, H: FnMut(&T, &D)->u64>  Exchange<T, D, P, H> {
    /// Allocates a new `Exchange` from a supplied set of pushers and a distribution function.
    pub fn new(pushers: Vec<P>, key: H) -> Exchange<T, D, P, H> {
        let mut buffers = vec![];
        // establish a buffer for every pusher
        for _ in 0..pushers.len() {
            // with_capacity constructs a new empty Vec<T> with the specified capcity
            buffers.push(Vec::with_capacity(Message::<T, D>::default_length()));
        }
        Exchange {
            pushers,
            hash_func: key,
            buffers,
            current: None,
        }
    }
    #[inline]
    fn flush(&mut self, index: usize) {
        if !self.buffers[index].is_empty() {
            // ref keyword binds by reference during pattern matching, it will NOT move the content out from self.current
            // ref annotates pattern bindings to make them borrow rather than move.
            // if let Some(ref time) is different from if let Some(&time)
            // the latter matches a different object (&time), while the former mathces the same object as Some(time).
            // let ref x = 1; is equivalent to let x = &1;
            // let &y = x; is equivalent to let y = *1;

            // only flush if the channel is open, we have advanced to a time
            // i.e., we should have something to flush
            if let Some(ref time) = self.current {
                // by using Message::push_at(), we will set the buffer's capacity to default
                Message::push_at(&mut self.buffers[index], time.clone(), &mut self.pushers[index]);
            }
        }
    }
}

// implement Push trait
impl<T: Eq+Data, D: Data, P: Push<Bundle<T, D>>, H: FnMut(&T, &D)->u64> Push<Bundle<T, D>> for Exchange<T, D, P, H> {
    #[inline(never)]
    fn push(&mut self, message: &mut Option<Bundle<T, D>>) {
        // if only one pusher, no exchange
        if self.pushers.len() == 1 {
            self.pushers[0].push(message);
        }
        else if let Some(message) = message {
            let message = message.as_mut();
            let time = &message.time;
            let data = &mut message.data;

            // if the time isn't right, flush everything.
            // we have advanced to a new timestamp
            if self.current.as_ref().map_or(false, |x| x != time) {
                for index in 0..self.pushers.len() {
                    self.flush(index);
                }
            }
            self.current = Some(time.clone());

            // if the number of pushers is a power of two, use a mask
            if (self.pushers.len() & (self.pushers.len() - 1)) == 0 {
                // mask: 011111111...
                let mask = (self.pushers.len() - 1) as u64;
                for datum in data.drain(..) {
                    // equivalent to mod
                    let index = (((self.hash_func)(time, &datum)) & mask) as usize;

                    self.buffers[index].push(datum);
                    // flush when the buffer is full
                    if self.buffers[index].len() == self.buffers[index].capacity() {
                        self.flush(index);
                    }

                    // unsafe {
                    //     self.buffers.get_unchecked_mut(index).push(datum);
                    //     if self.buffers.get_unchecked(index).len() == self.buffers.get_unchecked(index).capacity() {
                    //         self.flush(index);
                    //     }
                    // }

                }
            }
            // as a last resort, use mod (%)
            else {
                for datum in data.drain(..) {
                    let index = (((self.hash_func)(time, &datum)) % self.pushers.len() as u64) as usize;
                    self.buffers[index].push(datum);
                    if self.buffers[index].len() == self.buffers[index].capacity() {
                        self.flush(index);
                    }
                }
            }

        }
        else {
            // flush
            for index in 0..self.pushers.len() {
                self.flush(index);
                // also flush the pushers
                self.pushers[index].push(&mut None);
            }
        }
    }

    fn push_with_latency_passthrough(&mut self, element: &mut Option<Bundle<T, D>>, _latency: Option<MessageLatency>) {
        self.push(element);
    }
}
