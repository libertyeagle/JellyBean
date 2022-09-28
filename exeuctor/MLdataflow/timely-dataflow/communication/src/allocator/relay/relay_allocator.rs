//! Allocators and allocator builders for relay nodes

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::collections::hash_map::Entry;
use std::time::Duration;
use crossbeam_channel::{Receiver, Sender};
use bytes::arc::Bytes;
use crate::{Data, Message, MessageLatency, Pull, Push};
use crate::allocator::canary::Canary;
use crate::allocator::Event;
use crate::allocator::relay::{InputRelayAllocate, OutputRelayAllocate};
use crate::allocator::zero_copy::bytes_exchange::{BytesPull, MergeQueue, SendEndpoint};
use crate::allocator::counters::Puller as CountPuller;
use crate::allocator::relay::header::{RelayToRelayMessageHeader, RelayToTimelyMessageHeader};
use crate::allocator::relay::push_pull::{RelayToRelayTimestampedPusher, RelayToRelayTimestampedPusherBytes, RelayToTimelyTimestampedPusher, RelayToTimelyTimestampedPusherBytes, TimestampedPuller, TimestampedPullerBytes};
use crate::allocator::zero_copy::push_pull::{Puller, PullerBytes};
use crate::networking::MessageHeader;

/// Builder for a relay node, contains everything required to build a relay node
pub struct RelayBuilder {
    /// There are #num_input_pipelines threads to handle data received from input pipelines
    /// and send the data to the workers, each thread correspond to an input pipeline (with
    /// multiple relay nodes in a pipeline)
    pub input_relay_worker_builders: Vec<InputRelayWorkerBuilder>,

    /// There are #num_output_pipelines threads to handle data received from workers
    /// and send the data to the (relay nodes) of the output pipelines,
    /// each thread correspond to an output pipeline (with multiple relay nodes in a pipeline)
    pub output_relay_worker_builders: Vec<OutputRelayWorkerBuilder>,

    /// Network threads that establish connections to input pipeline relay nodes,
    /// these threads shall execute recv_loop, receive (and push results to) a MergeQueue
    /// to the threads that handle receiving data from input pipelines (#num_input_pipelines threads)
    /// shape: (num_input_pipelines, num_relay_nodes in the pipeline (may be different for each pipeline))
    pub network_input_relay_worker_futures: Vec<Vec<Receiver<MergeQueue>>>,

    /// Network threads to the timely-dataflow workers should execute send_loop and recv_loop,
    /// one thread for send_loop and one for recv_loop
    /// send_loop sends a MergeQueue from the input pipeline threads
    /// the input pipeline threads push data to be sent to the worker through the MergeQueues.
    /// shape: (num_timely_worker_processes, num_input_pipelines)
    pub network_timely_workers_promises: Vec<Vec<Sender<MergeQueue>>>,

    /// Network threads that establish connections to output pipeline relay nodes,
    /// these threads shall execute send_loop, which sends a MergeQueue
    /// to the threads that handle sending data to output pipelines (#num_output_pipelines threads)
    /// shape: (num_output_pipelines, num_relay_nodes in the pipeline (may be different for each pipeline))
    pub network_output_relay_worker_promises: Vec<Vec<Sender<MergeQueue>>>,

    /// Network threads to the timely-dataflow workers should execute send_loop and recv_loop
    /// recv_loop receives a MergeQueue to the output pipeline threads
    /// shape: (num_timely_worker_processes, num_output_pipelines)
    pub network_timely_workers_futures: Vec<Vec<Receiver<MergeQueue>>>,
}

/// Creates two vectors of builders, one for input relay workers, one for output relay builders
/// and create crossbeam channels
/// return a RelayBuilder
pub fn new_vector(
    my_index: usize,
    num_peer_relay_nodes: usize,
    num_relay_nodes_input_pipelines: Vec<usize>,
    num_relay_nodes_output_pipelines: Vec<usize>,
    num_timely_worker_processes: usize,
    num_threads_per_timely_worker_process: usize,
) -> RelayBuilder {
    let num_input_pipelines = num_relay_nodes_input_pipelines.len();
    let num_output_pipelines = num_relay_nodes_output_pipelines.len();

    let mut input_relay_worker_builders = Vec::with_capacity(num_input_pipelines);
    let mut output_relay_worker_builders = Vec::with_capacity(num_output_pipelines);

    let mut network_input_relay_worker_futures = Vec::new();
    let mut network_output_relay_worker_promises= Vec::new();

    let (network_timely_workers_promises, input_relay_worker_futures) = crate::promise_futures(num_timely_worker_processes, num_input_pipelines);
    for (index, (num_relay_nodes, relay_futures)) in num_relay_nodes_input_pipelines.into_iter().zip(input_relay_worker_futures).enumerate() {
        let (mut relay_thread_promises, network_futures) = crate::promise_futures(1, num_relay_nodes);
        network_input_relay_worker_futures.push(
            network_futures.into_iter()
                .map(|mut future| future.pop().unwrap())
                .collect()
        );
        let builder = InputRelayWorkerBuilder {
            relay_node_index: my_index,
            num_relay_nodes: num_peer_relay_nodes,
            pipeline_index: index,
            input_relay_promises: relay_thread_promises.pop().unwrap(),
            timely_workers_futures: relay_futures,
            num_threads_per_timely_worker_process
        };
        input_relay_worker_builders.push(builder);
    }

    let (output_relay_worker_promises, network_timely_workers_futures) = crate::promise_futures(num_output_pipelines, num_timely_worker_processes);
    for (index, (num_relay_nodes, relay_promise)) in num_relay_nodes_output_pipelines.into_iter().zip(output_relay_worker_promises).enumerate() {
        let (network_promises, mut relay_thread_futures, ) = crate::promise_futures(num_relay_nodes, 1);
        network_output_relay_worker_promises.push(
            network_promises.into_iter()
                .map(|mut promise| promise.pop().unwrap())
                .collect()
        );
        let builder = OutputRelayWorkerBuilder {
            relay_node_index: my_index,
            num_relay_nodes: num_peer_relay_nodes,
            pipeline_index: index,
            output_relay_futures: relay_thread_futures.pop().unwrap(),
            timely_workers_promises: relay_promise
        };
        output_relay_worker_builders.push(builder);
    }

    RelayBuilder {
        input_relay_worker_builders,
        output_relay_worker_builders,
        network_input_relay_worker_futures,
        network_timely_workers_promises,
        network_output_relay_worker_promises,
        network_timely_workers_futures
    }
}


/// Builder for input pipeline relay worker
pub struct InputRelayWorkerBuilder {
    // Process index of current relay node (of current pipeline)
    relay_node_index: usize,
    // number of relay nodes
    num_relay_nodes: usize,
    // the index of the input pipeline
    pipeline_index: usize,
    // send MergeQueue from network threads
    // that are connected to the relay nodes in this input pipeline
    // length: #num_relay_nodes (relay processes) in this input pipeline
    input_relay_promises: Vec<Sender<MergeQueue>>,
    // receive MergeQueue to the network threads
    // that are connected to the workers
    // length: #num_timely_workers_processes
    timely_workers_futures: Vec<Receiver<MergeQueue>>,
    // number of threads in a timely worker process
    num_threads_per_timely_worker_process: usize
}

impl InputRelayWorkerBuilder {
    /// Build input relay worker allocator
    pub fn build(self) -> InputRelayWorkerAllocator {
        // len: #num_relay_nodes in the input pipeline
        let mut input_pipeline_recvs = Vec::with_capacity(self.input_relay_promises.len());
        for promise in self.input_relay_promises.into_iter() {
            let buzzer = crate::buzzer::Buzzer::new();
            // network thread will notify the input relay worker thread
            // when it receives data from a input relay node
            let queue = MergeQueue::new(buzzer);
            promise.send(queue.clone()).expect("Failed to send MergeQueue");
            // MergeQueue can be shared across threads
            input_pipeline_recvs.push(queue);
        }

        let mut worker_sends = Vec::with_capacity(self.timely_workers_futures.len());
        for future in self.timely_workers_futures.into_iter() {
            let queue = future.recv().expect("Failed to receive push queue to timely workers");
            let send_point = SendEndpoint::new(queue);
            worker_sends.push(Rc::new(RefCell::new(send_point)));
        }

        InputRelayWorkerAllocator {
            relay_node_index: self.relay_node_index,
            num_relay_nodes: self.num_relay_nodes,
            pipeline_index: self.pipeline_index,
            num_threads_per_timely_worker_process: self.num_threads_per_timely_worker_process,
            input_pipeline_events: Rc::new(RefCell::new(VecDeque::new())),
            recv_canaries: Rc::new(RefCell::new(Vec::new())),
            dropped_input_recv_channels: HashSet::new(),
            allocated_input_recv_channels: HashSet::new(),
            staged: Vec::new(),
            input_pipeline_recvs,
            input_recv_to_channels: HashMap::new(),
            timely_worker_sends: worker_sends
        }
    }
}

/// Builder for output pipeline relay worker
pub struct OutputRelayWorkerBuilder {
    // Process index of current relay node (of current pipeline)
    relay_node_index: usize,
    // number of relay nodes
    num_relay_nodes: usize,
    // the index of the input pipeline
    pipeline_index: usize,
    // receive MergeQueue from network threads
    // that are connected to the relay nodes in this input pipeline
    // length: #num_relay_nodes (relay processes) in this input pipeline
    output_relay_futures: Vec<Receiver<MergeQueue>>,
    // send MergeQueue to the network threads
    // that are connected to the workers
    // length: #num_timely_workers
    timely_workers_promises: Vec<Sender<MergeQueue>>,
}

impl OutputRelayWorkerBuilder {
    /// Build output relay worker allocator
    pub fn build(self) -> OutputRelayWorkerAllocator {
        // len: #num_timely_worker_processes
        let mut timely_recvs = Vec::with_capacity(self.timely_workers_promises.len());
        for promise in self.timely_workers_promises.into_iter() {
            let buzzer = crate::buzzer::Buzzer::new();
            let queue = MergeQueue::new(buzzer);
            promise.send(queue.clone()).expect("Failed to send MergeQueue");
            timely_recvs.push(queue);
        }

        let mut output_pipeline_sends = Vec::with_capacity(self.output_relay_futures.len());
        for future in self.output_relay_futures.into_iter() {
            let queue = future.recv().expect("Failed to receive push queue to timely workers");
            let send_point = SendEndpoint::new(queue);
            output_pipeline_sends.push(Rc::new(RefCell::new(send_point)));
        }

        OutputRelayWorkerAllocator {
            relay_node_index: self.relay_node_index,
            num_relay_nodes: self.num_relay_nodes,
            pipeline_index: self.pipeline_index,
            timely_events: Rc::new(RefCell::new(VecDeque::new())),
            recv_canaries: Rc::new(RefCell::new(Vec::new())),
            dropped_timely_worker_channels: HashSet::new(),
            allocated_timely_worker_channels: HashSet::new(),
            staged: Vec::new(),
            timely_recvs,
            timely_recv_to_channels: HashMap::new(),
            output_pipeline_sends
        }
    }
}

/// Allocator for input pipeline relay worker to allocate channels
pub struct InputRelayWorkerAllocator {
    relay_node_index: usize,
    num_relay_nodes: usize,
    pipeline_index: usize,
    num_threads_per_timely_worker_process: usize,

    input_pipeline_events: Rc<RefCell<VecDeque<(usize, Event)>>>,

    // use to drop channels of the input pipeline
    // if the puller to the channel is dropped
    recv_canaries: Rc<RefCell<Vec<usize>>>,
    dropped_input_recv_channels: HashSet<usize>,
    // allocated channels' identifiers
    allocated_input_recv_channels: HashSet<usize>,

    // stage area for received data from the input pipeline
    staged: Vec<Bytes>,
    input_pipeline_recvs: Vec<MergeQueue>,
    // put the staged received data to the corresponding channel
    input_recv_to_channels: HashMap<usize, Rc<RefCell<VecDeque<(Bytes, MessageLatency)>>>>,
    timely_worker_sends: Vec<Rc<RefCell<SendEndpoint<MergeQueue>>>>
}

impl InputRelayAllocate for InputRelayWorkerAllocator {
    fn pipeline_index(&self) -> usize {
        self.pipeline_index
    }

    fn relay_index(&self) -> usize {
        self.relay_node_index
    }

    fn get_num_timely_workers(&self) -> usize {
        self.timely_worker_sends.len() * self.num_threads_per_timely_worker_process
    }

    fn get_num_relay_nodes_peers(&self) -> usize {
        self.num_relay_nodes
    }

    fn allocate_input_pipeline_puller<T: Data>(&mut self, channel_identifier: usize) -> Box<dyn Pull<Message<T>>> {
        let channel =
        self.input_recv_to_channels
            .entry(channel_identifier)
            .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
            .clone();

        let canary = Canary::new(channel_identifier, self.recv_canaries.clone());
        let puller = Box::new(CountPuller::new(TimestampedPuller::new(channel, canary), channel_identifier, self.input_pipeline_events.clone()));
        self.allocated_input_recv_channels.insert(channel_identifier);

        puller
    }

    fn allocate_timely_workers_pushers<T: Data>(&mut self, channel_identifier: usize) -> Vec<Box<dyn Push<Message<T>>>> {
        let num_workers = self.get_num_timely_workers();

        let mut pushers = Vec::<Box<dyn Push<Message<T>>>>::with_capacity(num_workers);
        for target_index in 0..num_workers {
            let process_id = target_index / self.num_threads_per_timely_worker_process;

            // pass through the network transmission latency (between the pipeline relay nodes)
            let header = RelayToTimelyMessageHeader {
                channel: channel_identifier,
                source: self.relay_node_index,
                target: target_index,
                length: 0,
                seqno: 0,
                relay_transmission_latency: None
            };

            pushers.push(Box::new(RelayToTimelyTimestampedPusher::new(header, self.timely_worker_sends[process_id].clone())));
        }
        pushers
    }

    fn allocate_input_pipeline_bytes_puller(&mut self, channel_identifier: usize) -> Box<dyn Pull<Bytes>> {
        let channel =
            self.input_recv_to_channels
                .entry(channel_identifier)
                .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
                .clone();

        let canary = Canary::new(channel_identifier, self.recv_canaries.clone());
        let puller = Box::new(CountPuller::new(TimestampedPullerBytes::new(channel, canary), channel_identifier, self.input_pipeline_events.clone()));

        self.allocated_input_recv_channels.insert(channel_identifier);
        puller
    }

    fn allocate_timely_workers_bytes_pushers(&mut self, channel_identifier: usize) -> Vec<Box<dyn Push<Bytes>>> {
        let num_workers = self.get_num_timely_workers();

        let mut pushers = Vec::<Box<dyn Push<Bytes>>>::with_capacity(num_workers);
        for target_index in 0..num_workers {
            let process_id = target_index / self.num_threads_per_timely_worker_process;

            // pass through the network transmission latency (between the pipeline relay nodes)
            let header = RelayToTimelyMessageHeader {
                channel: channel_identifier,
                source: self.relay_node_index,
                target: target_index,
                length: 0,
                seqno: 0,
                relay_transmission_latency: None
            };

            pushers.push(Box::new(RelayToTimelyTimestampedPusherBytes::new(header, self.timely_worker_sends[process_id].clone())));
        }
        pushers
    }

    fn finish_channel_allocation(&mut self) {
        let current_channels = self.input_recv_to_channels.keys().cloned().collect::<Vec<_>>();
        for channel in current_channels {
            if !self.allocated_input_recv_channels.contains(&channel) {
                let _dropped =
                    self.input_recv_to_channels
                        .remove(&channel)
                        .expect("non-existent channel dropped");
                self.dropped_input_recv_channels.insert(channel);
            }
        }
    }

    fn receive_pipeline_input(&mut self) {
        // Check for channels whose `Puller` has been dropped.
        let mut canaries = (*self.recv_canaries).borrow_mut();
        for dropped_channel in canaries.drain(..) {
            let _dropped =
                self.input_recv_to_channels
                    .remove(&dropped_channel)
                    .expect("non-existent channel dropped");
            self.dropped_input_recv_channels.insert(dropped_channel);
            // Borrowed channels may be non-empty, if the dataflow was forcibly
            // dropped. The contract is that if a dataflow is dropped, all other
            // workers will drop the dataflow too, without blocking indefinitely
            // on events from it.
            // assert!(dropped.borrow().is_empty());
        }
        std::mem::drop(canaries);

        let mut events = (*self.input_pipeline_events).borrow_mut();

        for recv in self.input_pipeline_recvs.iter_mut() {
            recv.drain_into(&mut self.staged);
        }

        for mut bytes in self.staged.drain(..) {
            while bytes.len() > 0 {
                if let Some(header) = RelayToRelayMessageHeader::try_read(&mut bytes[..]) {

                    // Get the header and payload, ditch the header.
                    // Also extract the network transmission latency from send relay node -> recv relay node
                    let pipeline_network_latency = header.recv_timestamp.unwrap() - header.send_timestamp.unwrap();
                    let mut peel = bytes.extract_to(header.required_bytes());
                    let _ = peel.extract_to(std::mem::size_of::<RelayToRelayMessageHeader>());

                    // Increment message count for channel.
                    // Safe to do this even if the channel has been dropped.
                    events.push_back((header.channel, Event::Pushed(1)));

                    // Ensure that a queue exists.
                    match self.input_recv_to_channels.entry(header.channel) {
                        Entry::Vacant(entry) => {
                            // We may receive data before allocating, and shouldn't block.
                            if !self.dropped_input_recv_channels.contains(&header.channel) {
                                entry.insert(Rc::new(RefCell::new(VecDeque::new())))
                                    .borrow_mut()
                                    .push_back((peel, pipeline_network_latency));
                            }
                        }
                        Entry::Occupied(mut entry) => {
                            entry.get_mut().borrow_mut().push_back((peel, pipeline_network_latency));
                        }
                    }
                }
                else {
                    println!("failed to read full header!");
                }
            }
        }
    }

    fn events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>> {
        &self.input_pipeline_events
    }

    fn await_events(&self, _duration: Option<Duration>) {
        if self.input_pipeline_events.borrow().is_empty() {
            if let Some(duration) = _duration {
                std::thread::park_timeout(duration);
            }
            else {
                std::thread::park();
            }
        }
    }

    fn input_relay_completed(&self) -> bool {
        self.input_pipeline_recvs.iter().all(|x| x.is_complete())
    }

    fn release(&mut self) {
        // send everything in buffer
        for send in self.timely_worker_sends.iter_mut() {
            (**send).borrow_mut().publish();
        }
    }
}

/// Allocator for output pipeline relay worker to allocate channels
pub struct OutputRelayWorkerAllocator {
    relay_node_index: usize,
    num_relay_nodes: usize,
    pipeline_index: usize,
    // events of receiving from timely workers
    timely_events: Rc<RefCell<VecDeque<(usize, Event)>>>,

    // use to drop channels of the timely workers
    // if the puller to the channel is dropped
    recv_canaries: Rc<RefCell<Vec<usize>>>,
    dropped_timely_worker_channels: HashSet<usize>,
    // currently allocated channels to timely workers
    allocated_timely_worker_channels: HashSet<usize>,

    // stage area for received data from the timely workers
    staged: Vec<Bytes>,
    timely_recvs: Vec<MergeQueue>,
    // put the staged received data to the corresponding channel
    timely_recv_to_channels: HashMap<usize, Rc<RefCell<VecDeque<Bytes>>>>,
    output_pipeline_sends: Vec<Rc<RefCell<SendEndpoint<MergeQueue>>>>
}

impl OutputRelayAllocate for OutputRelayWorkerAllocator {
    fn pipeline_index(&self) -> usize {
        self.pipeline_index
    }

    fn relay_index(&self) -> usize {
        self.relay_node_index
    }

    fn get_num_output_relay_nodes(&self) -> usize {
        self.output_pipeline_sends.len()
    }

    fn get_num_relay_nodes_peers(&self) -> usize {
        self.num_relay_nodes
    }

    fn allocate_output_pipeline_pushers<T: Data>(&mut self, channel_identifier: usize) -> Vec<Box<dyn Push<Message<T>>>> {
        // number of relay nodes in the output pipeline (the worker thread is in charge of handling)
        let num_output_relay_nodes = self.get_num_output_relay_nodes();

        let mut pushers = Vec::<Box<dyn Push<Message<T>>>>::with_capacity(num_output_relay_nodes);
        for relay_idx in 0..num_output_relay_nodes {
            let header = RelayToRelayMessageHeader {
                channel: channel_identifier,
                source: self.relay_node_index,
                target: relay_idx,
                length: 0,
                seqno: 0,
                send_timestamp: None,
                recv_timestamp: None
            };

            pushers.push(Box::new(RelayToRelayTimestampedPusher::new(header, self.output_pipeline_sends[relay_idx].clone())));
        }
        pushers
    }

    fn allocate_timely_workers_puller<T: Data>(&mut self, channel_identifier: usize) -> Box<dyn Pull<Message<T>>> {
        let channel =
            self.timely_recv_to_channels
                .entry(channel_identifier)
                .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
                .clone();

        let canary = Canary::new(channel_identifier, self.recv_canaries.clone());
        let puller = Box::new(CountPuller::new(Puller::new(channel, canary), channel_identifier, self.timely_events.clone()));

        self.allocated_timely_worker_channels.insert(channel_identifier);
        puller
    }

    fn allocate_output_pipeline_bytes_pushers(&mut self, channel_identifier: usize) -> Vec<Box<dyn Push<Bytes>>> {
        let num_output_relay_nodes = self.get_num_output_relay_nodes();

        let mut pushers = Vec::<Box<dyn Push<Bytes>>>::with_capacity(num_output_relay_nodes);
        for relay_idx in 0..num_output_relay_nodes {
            let header = RelayToRelayMessageHeader {
                channel: channel_identifier,
                source: self.relay_node_index,
                target: relay_idx,
                length: 0,
                seqno: 0,
                send_timestamp: None,
                recv_timestamp: None
            };

            pushers.push(Box::new(RelayToRelayTimestampedPusherBytes::new(header, self.output_pipeline_sends[relay_idx].clone())));
        }

        pushers
    }

    fn allocate_timely_workers_bytes_puller(&mut self, channel_identifier: usize) -> Box<dyn Pull<Bytes>> {
        let channel =
            self.timely_recv_to_channels
                .entry(channel_identifier)
                .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
                .clone();

        let canary = Canary::new(channel_identifier, self.recv_canaries.clone());
        let puller = Box::new(CountPuller::new(PullerBytes::new(channel, canary), channel_identifier, self.timely_events.clone()));

        self.allocated_timely_worker_channels.insert(channel_identifier);
        puller
    }

    fn finish_channel_allocation(&mut self) {
        let current_channels = self.timely_recv_to_channels.keys().cloned().collect::<Vec<_>>();
        for channel in current_channels {
            if !self.allocated_timely_worker_channels.contains(&channel) {
                let _dropped =
                    self.timely_recv_to_channels
                        .remove(&channel)
                        .expect("non-existent channel dropped");
                self.dropped_timely_worker_channels.insert(channel);
            }
        }
    }

    fn receive_from_timely_workers(&mut self) {
        let mut canaries = (*self.recv_canaries).borrow_mut();
        for dropped_channel in canaries.drain(..) {
            let _dropped =
                self.timely_recv_to_channels
                    .remove(&dropped_channel)
                    .expect("non-existent channel dropped");
            self.dropped_timely_worker_channels.insert(dropped_channel);
        }
        std::mem::drop(canaries);

        let mut events = (*self.timely_events).borrow_mut();

        for recv in self.timely_recvs.iter_mut() {
            recv.drain_into(&mut self.staged);
        }

        for mut bytes in self.staged.drain(..) {
            while bytes.len() > 0 {
                if let Some(header) = MessageHeader::try_read(&mut bytes[..]) {

                    // Get the header and payload, ditch the header.
                    let mut peel = bytes.extract_to(header.required_bytes());
                    // drop MessageHeader
                    let _ = peel.extract_to(std::mem::size_of::<MessageHeader>());

                    // Increment message count for channel.
                    // Safe to do this even if the channel has been dropped.
                    events.push_back((header.channel, Event::Pushed(1)));

                    // Ensure that a queue exists.
                    match self.timely_recv_to_channels.entry(header.channel) {
                        Entry::Vacant(entry) => {
                            // We may receive data before allocating, and shouldn't block.
                            if !self.dropped_timely_worker_channels.contains(&header.channel) {
                                entry.insert(Rc::new(RefCell::new(VecDeque::new())))
                                    .borrow_mut()
                                    .push_back(peel);
                            }
                        }
                        Entry::Occupied(mut entry) => {
                            entry.get_mut().borrow_mut().push_back(peel);
                        }
                    }
                }
                else {
                    println!("failed to read full header!");
                }
            }
        }
    }

    fn events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>> {
        &self.timely_events
    }

    fn await_events(&self, _duration: Option<Duration>) {
        if self.timely_events.borrow().is_empty() {
            if let Some(duration) = _duration {
                std::thread::park_timeout(duration);
            }
            else {
                std::thread::park();
            }
        }
    }

    fn timely_workers_completed(&self) -> bool {
        self.timely_recvs.iter().all(|x| x.is_complete())
    }

    fn release(&mut self) {
        for send in self.output_pipeline_sends.iter_mut() {
            (**send).borrow_mut().publish();
        }
    }
}
