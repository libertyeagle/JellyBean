//! Allocators for relay<->relay, relay<->worker communication
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;
use crate::{Data, Message, Pull, Push};
use crate::allocator::Event;
use bytes::arc::Bytes;

pub mod relay_allocator;
pub mod timely_allocator;
pub mod logging;
pub mod relay_initialize;
pub mod timely_initialize;
mod relay_tcp;
mod relay_network_utils;
mod timely_network_utlis;
mod timely_tcp;
mod header;
mod push_pull;

pub use relay_initialize::initialize_relay_node_networking as relay_initialize_networking;
pub use timely_initialize::initialize_networking_to_relay as timely_initialize_networking_cluster;
pub use timely_initialize::initialize_networking_to_relay_single_worker_process as timely_initialize_networking_process;
pub use logging::{RelayCommunicationEvent, RelayCommunicationSetup, RelayTimelyCommunicationEvent, RelayTimelyCommunicationSetup};

// TODO: implement pusher and puller for raw Bytes
/// Trait for input pipeline relay worker allocator
pub trait InputRelayAllocate {
    /// Index of the input pipeline connects to
    fn pipeline_index(&self) -> usize;
    /// Index of the current relay node
    fn relay_index(&self) -> usize;
    /// Get total number of timely workers in this pipeline
    fn get_num_timely_workers(&self) -> usize;
    /// Get the number of peer relay nodes in this pipeline
    fn get_num_relay_nodes_peers(&self) -> usize;
    /// Allocate a channel to pull data from input pipeline relay nodes
    fn allocate_input_pipeline_puller<T: Data>(&mut self, channel_identifier: usize) -> Box<dyn Pull<Message<T>>>;
    /// Allocate a channel to push data to timely workers
    fn allocate_timely_workers_pushers<T: Data>(&mut self, channel_identifier: usize) -> Vec<Box<dyn Push<Message<T>>>>;
    /// Receive messages from input pipeline relay nodes, directly pulls Bytes
    fn allocate_input_pipeline_bytes_puller(&mut self, channel_identifier: usize) -> Box<dyn Pull<Bytes>>;
    /// Allocate a channel to push data to timely workers that directly pushes Bytes
    fn allocate_timely_workers_bytes_pushers(&mut self, channel_identifier: usize) -> Vec<Box<dyn Push<Bytes>>>;
    /// Indicate to the allocator that we have finished allocating channels
    /// we may potentially drop recv channels that we will never pull
    fn finish_channel_allocation(&mut self);
    /// Receive messages from input pipeline relay nodes
    fn receive_pipeline_input(&mut self) {}
    /// Message received events from input pipelines
    fn events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>>;
    /// Awaits events (messages) from input pipeline relay nodes
    fn await_events(&self, _duration: Option<Duration>);
    /// Whether all the relay nodes in the input pipeline has completed?
    /// i.e., TcpStream is closed.
    fn input_relay_completed(&self) -> bool;
    /// Perform postparatory work, most likely sending un-full binary buffers.
    fn release(&mut self);
}

/// Trait for output pipeline relay worker allocator
pub trait OutputRelayAllocate {
    /// Index of the output pipeline connects to
    fn pipeline_index(&self) -> usize;
    /// Index of the current relay node
    fn relay_index(&self) -> usize;
    /// Obtain the number of relay nodes in the output pipeline bind to
    fn get_num_output_relay_nodes(&self) -> usize;
    /// Get the number of peer relay nodes in this pipeline
    fn get_num_relay_nodes_peers(&self) -> usize;
    /// Allocate a channel to push data to output pipelines
    fn allocate_output_pipeline_pushers<T: Data>(&mut self, channel_identifier: usize) -> Vec<Box<dyn Push<Message<T>>>>;
    /// Allocate a channel to pull data from timely workers
    fn allocate_timely_workers_puller<T: Data>(&mut self, channel_identifier: usize) -> Box<dyn Pull<Message<T>>>;
    /// Allocate a channel to push data to output pipelines
    fn allocate_output_pipeline_bytes_pushers(&mut self, channel_identifier: usize) -> Vec<Box<dyn Push<Bytes>>>;
    /// Allocate a channel to directly pull Bytes data from timely workers
    fn allocate_timely_workers_bytes_puller(&mut self, channel_identifier: usize) -> Box<dyn Pull<Bytes>>;
    /// Indicate to the allocator that we have finished allocating channels
    fn finish_channel_allocation(&mut self);
    /// Receive messages from timely workers
    fn receive_from_timely_workers (&mut self) {}
    /// Message received events from timely workers
    fn events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>>;
    /// Awaits events (messages) from timely workers
    fn await_events(&self, _duration: Option<Duration>);
    /// Whether all the timely workers in current pipeline has completed?
    fn timely_workers_completed(&self) -> bool;
    /// Perform postparatory work, most likely sending un-full binary buffers.
    fn release(&mut self);
}