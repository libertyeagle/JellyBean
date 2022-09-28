//! Generic allocator for direct relay communication

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

use crate::{Data, Message, Pull, Push};
use crate::allocator::Allocate;
use crate::allocator::AllocateBuilder;
use crate::allocator::direct_relay::process::{Process, ProcessBuilder};
use crate::allocator::direct_relay::tcp_allocator::{TcpAllocator, TcpBuilder};
use crate::allocator::Event;

use super::DirectRelayAllocate;

/// Generic timely worker allocators with direct relay connection
pub enum GenericDirectRelay {
    /// Single process, multi-threads allocator
    Process(Process),
    /// Cluster allocator
    ZeroCopy(TcpAllocator)
}

/// Generic timely worker allocator builders with direct relay connection
pub enum GenericDirectRelayBuilder {
    /// Builder for `Process` allocator.
    Process(ProcessBuilder),
    /// Builder for `ZeroCopy` allocator.
    ZeroCopy(TcpBuilder),
}

impl AllocateBuilder for GenericDirectRelayBuilder {
    type Allocator = GenericDirectRelay;
    fn build(self) -> GenericDirectRelay {
        match self {
            GenericDirectRelayBuilder::Process(p) => GenericDirectRelay::Process(p.build()),
            GenericDirectRelayBuilder::ZeroCopy(z) => GenericDirectRelay::ZeroCopy(z.build()),
        }
    }
}

impl GenericDirectRelay {
    /// The index of the worker out of `(0..self.peers())`.
    pub fn index(&self) -> usize {
        match self {
            GenericDirectRelay::Process(p) => p.index(),
            GenericDirectRelay::ZeroCopy(z) => z.index(),
        }
    }
    /// The number of workers.
    pub fn peers(&self) -> usize {
        match self {
            GenericDirectRelay::Process(p) => p.peers(),
            GenericDirectRelay::ZeroCopy(z) => z.peers(),
        }
    }
    /// Constructs several send endpoints and one receive endpoint.
    fn allocate<T: Data>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<Message<T>>>>, Box<dyn Pull<Message<T>>>) {
        match self {
            GenericDirectRelay::Process(p) => p.allocate(identifier),
            GenericDirectRelay::ZeroCopy(z) => z.allocate(identifier),
        }
    }
    /// Perform work before scheduling operators.
    fn receive(&mut self) {
        match self {
            GenericDirectRelay::Process(p) => p.receive(),
            GenericDirectRelay::ZeroCopy(z) => z.receive(),
        }
    }
    /// Perform work after scheduling operators.
    pub fn release(&mut self) {
        match self {
            GenericDirectRelay::Process(p) => p.release(),
            GenericDirectRelay::ZeroCopy(pb) => pb.release(),
        }
    }
    fn events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>> {
        match self {
            GenericDirectRelay::Process(ref p) => p.events(),
            GenericDirectRelay::ZeroCopy(ref z) => z.events(),
        }
    }
}


impl Allocate for GenericDirectRelay {
    fn index(&self) -> usize { self.index() }
    fn peers(&self) -> usize { self.peers() }
    fn allocate<T: Data>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<Message<T>>>>, Box<dyn Pull<Message<T>>>) {
        self.allocate(identifier)
    }

    fn events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>> { self.events() }
    fn await_events(&self, _duration: Option<std::time::Duration>) {
        match self {
            GenericDirectRelay::Process(p) => p.await_events(_duration),
            GenericDirectRelay::ZeroCopy(z) => z.await_events(_duration),
        }
    }
    fn receive(&mut self) { self.receive(); }
    fn release(&mut self) { self.release(); }
}

impl DirectRelayAllocate for GenericDirectRelay {
    fn get_num_input_pipelines(&self) -> usize {
        match self {
            GenericDirectRelay::Process(p) => p.get_num_input_pipelines(),
            GenericDirectRelay::ZeroCopy(z) => z.get_num_input_pipelines()
        }
    }

    fn get_num_output_pipelines(&self) -> usize {
        match self {
            GenericDirectRelay::Process(p) => p.get_num_output_pipelines(),
            GenericDirectRelay::ZeroCopy(z) => z.get_num_output_pipelines()
        }
    }

    fn get_num_input_pipeline_workers(&self, pipeline_index: usize) -> usize {
        match self {
            GenericDirectRelay::Process(p) => p.get_num_input_pipeline_workers(pipeline_index),
            GenericDirectRelay::ZeroCopy(z) => z.get_num_input_pipeline_workers(pipeline_index)
        }
    }

    fn get_num_output_pipeline_workers(&self, pipeline_index: usize) -> usize {
        match self {
            GenericDirectRelay::Process(p) => p.get_num_output_pipeline_workers(pipeline_index),
            GenericDirectRelay::ZeroCopy(z) => z.get_num_output_pipeline_workers(pipeline_index)
        }
    }

    fn allocate_input_pipeline_puller<T: Data>(&mut self, pipeline_index: usize, channel_identifier: usize) -> Box<dyn Pull<Message<T>>> {
        match self {
            GenericDirectRelay::Process(p) => p.allocate_input_pipeline_puller(pipeline_index, channel_identifier),
            GenericDirectRelay::ZeroCopy(z) => z.allocate_input_pipeline_puller(pipeline_index, channel_identifier)
        }
    }

    fn allocate_output_pipeline_pushers<T: Data>(&mut self, pipeline_index: usize, channel_identifier: usize) -> Vec<Box<dyn Push<Message<T>>>> {
        match self {
            GenericDirectRelay::Process(p) => p.allocate_output_pipeline_pushers(pipeline_index, channel_identifier),
            GenericDirectRelay::ZeroCopy(z) => z.allocate_output_pipeline_pushers(pipeline_index, channel_identifier)
        }
    }

    fn receive_from_input_pipeline(&mut self, pipeline_index: usize) {
        match self {
            GenericDirectRelay::Process(p) => p.receive_from_input_pipeline(pipeline_index),
            GenericDirectRelay::ZeroCopy(z) => z.receive_from_input_pipeline(pipeline_index)
        }
    }

    fn finish_input_pipeline_channel_allocation(&mut self, pipeline_index: usize) {
        match self {
            GenericDirectRelay::Process(p) => p.finish_input_pipeline_channel_allocation(pipeline_index),
            GenericDirectRelay::ZeroCopy(z) => z.finish_input_pipeline_channel_allocation(pipeline_index)
        }
    }

    fn release_output_pipeline_send(&mut self, pipeline_index: usize) {
        match self {
            GenericDirectRelay::Process(p) => p.release_output_pipeline_send(pipeline_index),
            GenericDirectRelay::ZeroCopy(z) => z.release_output_pipeline_send(pipeline_index)
        }
    }

    fn input_pipeline_events(&self, pipeline_index: usize) -> &Rc<RefCell<VecDeque<(usize, Event)>>> {
        match self {
            GenericDirectRelay::Process(p) => p.input_pipeline_events(pipeline_index),
            GenericDirectRelay::ZeroCopy(z) => z.input_pipeline_events(pipeline_index)
        }
    }

    fn await_input_pipeline_events(&self, pipeline_index: usize, _duration: Option<Duration>) {
        match self {
            GenericDirectRelay::Process(p) => p.await_input_pipeline_events(pipeline_index, _duration),
            GenericDirectRelay::ZeroCopy(z) => z.await_input_pipeline_events(pipeline_index, _duration)
        }
    }

    fn await_any_input_pipelines_events(&self, _duration: Option<Duration>) {
        match self {
            GenericDirectRelay::Process(p) => p.await_any_input_pipelines_events(_duration),
            GenericDirectRelay::ZeroCopy(z) => z.await_any_input_pipelines_events(_duration)
        }
    }

    fn await_any_events(&self, _duration: Option<Duration>) {
        match self {
            GenericDirectRelay::Process(p) => p.await_any_events(_duration),
            GenericDirectRelay::ZeroCopy(z) => z.await_any_events(_duration)
        }
    }

    fn input_pipeline_completed(&self, pipeline_index: usize) -> bool {
        match self {
            GenericDirectRelay::Process(p) => p.input_pipeline_completed(pipeline_index),
            GenericDirectRelay::ZeroCopy(z) => z.input_pipeline_completed(pipeline_index)
        }
    }
}