// READ
//! Push and Pull wrappers to maintain counts of messages in channels.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;

use crate::{Push, Pull, MessageLatency};
use crate::allocator::Event;

/// The push half of an intra-thread channel.
pub struct Pusher<T, P: Push<T>> {
    index: usize,
    // count: usize,
    // events are shared, but not across threads.
    // it is a counted wrapper for intra-thread pusher
    events: Rc<RefCell<VecDeque<(usize, Event)>>>,
    pusher: P,
    phantom: ::std::marker::PhantomData<T>,
}

impl<T, P: Push<T>>  Pusher<T, P> {
    /// Wraps a pusher with a message counter.
    pub fn new(pusher: P, index: usize, events: Rc<RefCell<VecDeque<(usize, Event)>>>) -> Self {
        Pusher {
            index,
            // count: 0,
            events,
            pusher,
            phantom: ::std::marker::PhantomData,
        }
    }
}

impl<T, P: Push<T>> Push<T> for Pusher<T, P> {
    #[inline]
    fn push(&mut self, element: &mut Option<T>) {
        // if element.is_none() {
        //     if self.count != 0 {
        //         self.events
        //             .borrow_mut()
        //             .push_back((self.index, Event::Pushed(self.count)));
        //         self.count = 0;
        //     }
        // }
        // else {
        //     self.count += 1;
        // }
        // TODO: Version above is less chatty, but can be a bit late in
        //       moving information along. Better, but needs cooperation.
        
        // equivalent to self.events.
        self.events
            .borrow_mut()
            .push_back((self.index, Event::Pushed(1)));

        self.pusher.push(element)
    }

    fn push_with_latency_passthrough(&mut self, element: &mut Option<T>, latency: Option<MessageLatency>) {
        self.events
            .borrow_mut()
            .push_back((self.index, Event::Pushed(1)));
        self.pusher.push_with_latency_passthrough(element, latency);
    }
}

use crossbeam_channel::Sender;

/// The push half of an intra-thread channel.
// events can be sent across threads.
pub struct ArcPusher<T, P: Push<T>> {
    index: usize,
    // count: usize,
    events: Sender<(usize, Event)>,
    pusher: P,
    phantom: ::std::marker::PhantomData<T>,
    buzzer: crate::buzzer::Buzzer,
}

impl<T, P: Push<T>>  ArcPusher<T, P> {
    /// Wraps a pusher with a message counter.
    pub fn new(pusher: P, index: usize, events: Sender<(usize, Event)>, buzzer: crate::buzzer::Buzzer) -> Self {
        ArcPusher {
            index,
            // count: 0,
            events,
            pusher,
            phantom: ::std::marker::PhantomData,
            buzzer,
        }
    }
}

impl<T, P: Push<T>> Push<T> for ArcPusher<T, P> {
    #[inline]
    fn push(&mut self, element: &mut Option<T>) {
        // if element.is_none() {
        //     if self.count != 0 {
        //         self.events
        //             .send((self.index, Event::Pushed(self.count)))
        //             .expect("Failed to send message count");
        //         self.count = 0;
        //     }
        // }
        // else {
        //     self.count += 1;
        // }

        // These three calls should happen in this order, to ensure that
        // we first enqueue data, second enqueue interest in the channel,
        // and finally awaken the thread. Other orders are defective when
        // multiple threads are involved.
        self.pusher.push(element);
        let _ = self.events.send((self.index, Event::Pushed(1)));
        // TODO : Perhaps this shouldn't be a fatal error (e.g. in shutdown).
        // .expect("Failed to send message count");

        // we can unpark (resume) the execution of the receiver thread
        // it can obtain events through crossbeam channel.
        self.buzzer.buzz();
    }

    fn push_with_latency_passthrough(&mut self, element: &mut Option<T>, latency: Option<MessageLatency>) {
        self.pusher.push_with_latency_passthrough(element, latency);
        let _ = self.events.send((self.index, Event::Pushed(1)));

        self.buzzer.buzz();
    }
}

/// The pull half of an intra-thread channel.
pub struct Puller<T, P: Pull<T>> {
    index: usize,
    count: usize,
    events: Rc<RefCell<VecDeque<(usize, Event)>>>,
    puller: P,
    phantom: ::std::marker::PhantomData<T>,
}

impl<T, P: Pull<T>>  Puller<T, P> {
    /// Wraps a puller with a message counter.
    pub fn new(puller: P, index: usize, events: Rc<RefCell<VecDeque<(usize, Event)>>>) -> Self {
        Puller {
            index,
            count: 0,
            events,
            puller,
            phantom: ::std::marker::PhantomData,
        }
    }
}
impl<T, P: Pull<T>> Pull<T> for Puller<T, P> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> {
        let result = self.puller.pull();
        // receive multiple items and then add an event
        if result.is_none() {
            if self.count != 0 {
                self.events
                    .borrow_mut()
                    .push_back((self.index, Event::Pulled(self.count)));
                self.count = 0;
            }
        }
        else {
            self.count += 1;
        }

        result
    }

    fn pull_with_transmission_latency(&mut self) -> &mut Option<(T, MessageLatency)> {
        let result = self.puller.pull_with_transmission_latency();
        if result.is_none() {
            if self.count != 0 {
                self.events
                    .borrow_mut()
                    .push_back((self.index, Event::Pulled(self.count)));
                self.count = 0;
            }
        }
        else {
            self.count += 1;
        }

        result
    }
}
