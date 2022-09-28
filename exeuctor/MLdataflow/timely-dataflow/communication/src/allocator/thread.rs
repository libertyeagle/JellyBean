// READ
//! Intra-thread communication.

use std::rc::Rc;
use std::cell::RefCell;
use std::time::Duration;
use std::collections::VecDeque;

use crate::allocator::{Allocate, AllocateBuilder, Event};
use crate::allocator::counters::Pusher as CountPusher;
use crate::allocator::counters::Puller as CountPuller;
use crate::{Push, Pull, Message, MessageLatency};

/// Builder for single-threaded allocator.
pub struct ThreadBuilder;

impl AllocateBuilder for ThreadBuilder {
    type Allocator = Thread;
    fn build(self) -> Self::Allocator { Thread::new() }
}


/// An allocator for intra-thread communication.
// the allocator is bounded by the worker closure.
// it is the only handle the worker has to the outside world.
pub struct Thread {
    /// Shared counts of messages in channels.
    // events is local to the thread, shared between the pusher and the puller.
    // NOTE that this is a intra-thread (thread-local) allocator. 
    // events should have three references (events / puller / pusher)
    events: Rc<RefCell<VecDeque<(usize, Event)>>>,
}

impl Allocate for Thread {
    fn index(&self) -> usize { 0 }
    fn peers(&self) -> usize { 1 }
    fn allocate<T: 'static>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<Message<T>>>>, Box<dyn Pull<Message<T>>>) {
        // allocate a single pusher and a puller
        let (pusher, puller) = Thread::new_from(identifier, self.events.clone());
        (vec![Box::new(pusher)], Box::new(puller))
    }
    fn events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>> {
        &self.events
    }
    fn await_events(&self, duration: Option<Duration>) {
        if self.events.borrow().is_empty() {
            if let Some(duration) = duration {
                // only has a single thread
                std::thread::park_timeout(duration);
            }
            else {
                std::thread::park();
            }
        }
    }
}

/// Thread-local counting channel push endpoint.
pub type ThreadPusher<T> = CountPusher<T, Pusher<T>>;
/// Thread-local counting channel pull endpoint.
pub type ThreadPuller<T> = CountPuller<T, Puller<T>>;

impl Thread {
    /// Allocates a new thread-local channel allocator.
    pub fn new() -> Self {
        Thread {
            // when creating this allocator, we only allocate the events queue.
            events: Rc::new(RefCell::new(VecDeque::new())),
        }
    }

    /// Creates a new thread-local channel from an identifier and shared counts.
    pub fn new_from<T: 'static>(identifier: usize, events: Rc<RefCell<VecDeque<(usize, Event)>>>)
        -> (ThreadPusher<Message<T>>, ThreadPuller<Message<T>>)
    {
        // allocate pusher and puller
        // also the shared message channel
        // shared should have two references (pusher / puller)
        // identifier is used to differentiate different sets of channels
        let shared = Rc::new(RefCell::new((VecDeque::<Message<T>>::new(), VecDeque::<Message<T>>::new())));
        let pusher = Pusher { target: shared.clone() };
        let pusher = CountPusher::new(pusher, identifier, events.clone());
        let puller = Puller { source: shared, current: None, current_with_dummy_latency: None };
        let puller = CountPuller::new(puller, identifier, events);
        (pusher, puller)
    }
}


/// The push half of an intra-thread channel.
pub struct Pusher<T> {
    target: Rc<RefCell<(VecDeque<T>, VecDeque<T>)>>,
}

impl<T> Push<T> for Pusher<T> {
    #[inline]
    fn push(&mut self, element: &mut Option<T>) {
        let mut borrow = self.target.borrow_mut();
        // takes ownership of the element
        if let Some(element) = element.take() {
            borrow.0.push_back(element);
        }
        // put back something as potential return value
        *element = borrow.1.pop_front();
    }

    fn push_with_latency_passthrough(&mut self, element: &mut Option<T>, _latency: Option<MessageLatency>) {
        self.push(element);
    }
}

/// The pull half of an intra-thread channel.
// concrete Pusher structure
pub struct Puller<T> {
    current: Option<T>,
    current_with_dummy_latency: Option<(T, MessageLatency)>,
    source: Rc<RefCell<(VecDeque<T>, VecDeque<T>)>>,
}

impl<T> Pull<T> for Puller<T> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> {
        let mut borrow = self.source.borrow_mut();
        // if let Some(element) = self.current.take() {
        //     // TODO : Arbitrary constant.
        //     if borrow.1.len() < 16 {
        //         borrow.1.push_back(element);
        //     }
        // }
        self.current = borrow.0.pop_front();
        &mut self.current
    }

    fn pull_with_transmission_latency(&mut self) -> &mut Option<(T, MessageLatency)> {
        let mut borrow = self.source.borrow_mut();
        self.current = borrow.0.pop_front();
        self.current_with_dummy_latency = self.current.take().map(|x| (x, 0));
        &mut self.current_with_dummy_latency
    }
}
