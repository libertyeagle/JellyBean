// READ, Sep 12 2021
//! A `Push` implementor with a list of `Box<Push>` to forward pushes to.

use std::rc::Rc;
use std::cell::RefCell;
use timely_communication::MessageLatency;

use crate::Data;
use crate::dataflow::channels::{Bundle, Message};

use crate::communication::Push;

/// Wraps a shared list of `Box<Push>` to forward pushes to. Owned by `Stream`.
pub struct Tee<T: 'static, D: 'static> {
    buffer: Vec<D>,
    // we use dyn trait so that we can push to pushers of different types
    // e.g., thread-local pushers & inter-process pushers
    shared: Rc<RefCell<Vec<Box<dyn Push<Bundle<T, D>>>>>>,
}

impl<T: Data, D: Data> Push<Bundle<T, D>> for Tee<T, D> {
    #[inline]
    fn push(&mut self, message: &mut Option<Bundle<T, D>>) {
        let mut pushers = self.shared.borrow_mut();
        if let Some(message) = message {
            for index in 1..pushers.len() {
                self.buffer.extend_from_slice(&message.data);
                // buffer is emptied after every push
                Message::push_at(&mut self.buffer, message.time.clone(), &mut pushers[index-1]);
            }
        }
        else {
            for index in 1..pushers.len() {
                pushers[index-1].push(&mut None);
            }
        }
        if pushers.len() > 0 {
            let last = pushers.len() - 1;
            pushers[last].push(message);
        }
    }

    fn push_with_latency_passthrough(&mut self, element: &mut Option<Bundle<T, D>>, _latency: Option<MessageLatency>) {
        // there is no latency passthrough in timely worker to relay communication
        // there is no concept of latency in timely->relay communication
        // only in relay->relay and relay->timely communication.
        // relay->timely is handled by RelayLogPusher
        self.push(element);
    }
}

impl<T, D> Tee<T, D> {
    /// Allocates a new pair of `Tee` and `TeeHelper`.
    pub fn new() -> (Tee<T, D>, TeeHelper<T, D>) {
        let shared = Rc::new(RefCell::new(Vec::new()));
        let port = Tee {
            buffer: Vec::with_capacity(Message::<T, D>::default_length()),
            shared: shared.clone(),
        };

        // tee helper is used to add pushers
        (port, TeeHelper { shared })
    }
}

impl<T, D> Clone for Tee<T, D> {
    fn clone(&self) -> Tee<T, D> {
        Tee {
            buffer: Vec::with_capacity(self.buffer.capacity()),
            shared: self.shared.clone(),
        }
    }
}

/// A shared list of `Box<Push>` used to add `Push` implementors.
pub struct TeeHelper<T, D> {
    shared: Rc<RefCell<Vec<Box<dyn Push<Bundle<T, D>>>>>>
}

impl<T, D> TeeHelper<T, D> {
    /// Adds a new `Push` implementor to the list of recipients shared with a `Stream`.
    pub fn add_pusher<P: Push<Bundle<T, D>>+'static>(&self, pusher: P) {
        self.shared.borrow_mut().push(Box::new(pusher));
    }
}

impl<T, D> Clone for TeeHelper<T, D> {
    fn clone(&self) -> Self {
        TeeHelper {
            shared: self.shared.clone()
        }
    }
}



pub struct PipelineInputTee<T: 'static, D: 'static> {
    buffer: Vec<D>,
    // we use dyn trait so that we can push to pushers of different types
    // e.g., thread-local pushers & inter-process pushers
    shared: Rc<RefCell<Vec<Box<dyn Push<Bundle<T, D>>>>>>,
}

impl<T: Data, D: Data> Push<Bundle<T, D>> for PipelineInputTee<T, D> {
    #[inline]
    fn push(&mut self, message: &mut Option<Bundle<T, D>>) {
        let mut pushers = self.shared.borrow_mut();
        if let Some(message) = message {
            let pipeline_latency = message.pipeline_latency.unwrap();
            for index in 1..pushers.len() {
                self.buffer.extend_from_slice(&message.data);
                // buffer is emptied after every push
                Message::push_at_with_latency(&mut self.buffer, message.time.clone(), &mut pushers[index-1], pipeline_latency);
            }
        }
        else {
            for index in 1..pushers.len() {
                pushers[index-1].push(&mut None);
            }
        }
        if pushers.len() > 0 {
            let last = pushers.len() - 1;
            pushers[last].push(message);
        }
    }

    fn push_with_latency_passthrough(&mut self, element: &mut Option<Bundle<T, D>>, _latency: Option<MessageLatency>) {
        // there is no latency passthrough in timely worker to relay communication
        // there is no concept of latency in timely->relay communication
        // only in relay->relay and relay->timely communication.
        // relay->timely is handled by RelayLogPusher
        self.push(element);
    }
}

impl<T, D> PipelineInputTee<T, D> {
    /// Allocates a new pair of `Tee` and `TeeHelper`.
    pub fn new() -> (PipelineInputTee<T, D>, TeeHelper<T, D>) {
        let shared = Rc::new(RefCell::new(Vec::new()));
        let port = PipelineInputTee {
            buffer: Vec::with_capacity(Message::<T, D>::default_length()),
            shared: shared.clone(),
        };

        // tee helper is used to add pushers
        (port, TeeHelper { shared })
    }
}

impl<T, D> Clone for PipelineInputTee<T, D> {
    fn clone(&self) -> PipelineInputTee<T, D> {
        PipelineInputTee {
            buffer: Vec::with_capacity(self.buffer.capacity()),
            shared: self.shared.clone(),
        }
    }
}