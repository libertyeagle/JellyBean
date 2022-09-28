//! Zero-copy allocator based on TCP.
//!
use std::cell::RefCell;
use std::collections::{hash_map::Entry, HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender};

use bytes::arc::Bytes;

use crate::{Allocate, Data, Message, Pull, Push};
use crate::allocator::AllocateBuilder;
use crate::allocator::canary::Canary;
use crate::allocator::counters::Puller as CountPuller;
use crate::allocator::direct_relay::DirectRelayAllocate;
use crate::allocator::Event;
use crate::allocator::Process;
use crate::allocator::process::ProcessBuilder;
use crate::allocator::zero_copy::bytes_exchange::{BytesPull, MergeQueue, SendEndpoint};
use crate::allocator::zero_copy::push_pull::{Puller as TcpPuller, PullerInner as TcpPullerInner, Pusher as TcpPusher};
use crate::networking::MessageHeader;

/// Builder for direct relay - cluster
pub struct DirectRelayWorkerTcpBuilder {
    /// Builders for the worker threads in the current process
    pub worker_thread_builders: Vec<TcpBuilder>,
    /// For sending data to peer workers in current pipeline
    pub network_peer_promises: Vec<Vec<Sender<MergeQueue>>>,
    /// For receiving data from peer workers in current pipeline
    pub network_peer_futures: Vec<Vec<Receiver<MergeQueue>>>,
    /// For recv_loop that receive data from input pipelines timely workers
    /// to receive a MergeQueue from all the worker threads in this worker process (in this pipeline)
    /// and write the received data to the MergeQueues
    /// (num_input_pipelines, num_worker_processes_in_i_th_pipeline, num_threads_current_process)
    /// Note that num_worker_processes is different for each input pipeline.
    pub network_input_pipelines_futures: Vec<Vec<Vec<Receiver<MergeQueue>>>>,
    /// For send_loop to send data to output pipelines
    /// (num_output_pipelines, num_worker_processes_in_i_th_pipeline, num_threads_current_process)
    pub network_output_pipelines_promises: Vec<Vec<Vec<Sender<MergeQueue>>>>,
}

/// Builds an instance of a TcpAllocator.
///
/// Builders are required because some of the state in a `TcpAllocator` cannot be sent between
/// threads (specifically, the `Rc<RefCell<_>>` local channels). So, we must package up the state
/// shared between threads here, and then provide a method that will instantiate the non-movable
/// members once in the destination thread.
pub struct TcpBuilder {
    // inner is allocator builder
    inner:  ProcessBuilder,
    index:  usize,                      // number out of peers
    peers:  usize,                      // number of peer allocators.
    // to receive the MergeQueue from the network send_loop thread
    // so that the worker can push messages to the MergeQueue
    // futures MergeQueues are used to send messages
    futures:   Vec<Receiver<MergeQueue>>,  // to receive queues to each network thread.
    // use sender to send MergeQueue to the network recv_loop thread
    // so that the recv_loop can push messages into MergeQueue
    // promises' MergeQueues are used to receive messages
    promises:   Vec<Sender<MergeQueue>>,    // to send queues from each network thread.

    // Number of timely workers in the input pipelines (num_processes * num_threads_per_process)
    num_input_pipelines_workers: Vec<usize>,
    // Send MergeQueue to network threads that are executing recv_loop
    // to receive data from input pipelines timely workers
    // shape: (num_input_pipelines, num_worker_processes_in_i_th_pipeline)
    input_pipelines_promises: Vec<Vec<Sender<MergeQueue>>>,

    // Number of timely workers in the output pipelines
    num_output_pipelines_workers: Vec<usize>,
    // Send messages to output pipelines timely workers
    // shape: (num_output_pipelines, num_worker_processes_in_i_th_pipeline)
    output_pipelines_futures: Vec<Vec<Receiver<MergeQueue>>>,
}

/// Creates a vector of builders, sharing appropriate state.
pub fn new_vector(
    allocators: Vec<ProcessBuilder>,
    my_process: usize,
    processes: usize,
    num_input_pipelines_timely_workers_processes: &[usize],
    num_output_pipelines_timely_workers_processes: &[usize],
    num_input_pipelines_threads_per_process: &[usize],
    num_output_pipelines_threads_per_process: &[usize]
) -> DirectRelayWorkerTcpBuilder
{
    // allocators is a vector of inner allocators
    // its length is #threads/process
    let threads = allocators.len();

    // For queues from worker threads to network threads, and vice versa.
    // for each process, we need to establish TCP connections to other progress
    // for each other process, we establish a network thread for receiving, and a network thread for sending
    let (network_promises, worker_futures) = crate::promise_futures(processes-1, threads);
    let (worker_promises, network_futures) = crate::promise_futures(threads, processes-1);


    let num_input_pipelines_workers = num_input_pipelines_timely_workers_processes.iter().zip(num_input_pipelines_threads_per_process.iter()).map(|(x, y)| *x * *y).collect::<Vec<_>>();
    let num_output_pipelines_workers = num_output_pipelines_timely_workers_processes.iter().zip(num_output_pipelines_threads_per_process.iter()).map(|(x, y)| *x * *y).collect::<Vec<_>>();

    let mut input_pipelines_workers_promises = Vec::with_capacity(threads);
    for _ in 0 .. threads {
        input_pipelines_workers_promises.push(Vec::new());
    }
    let mut input_pipelines_network_futures = Vec::with_capacity(num_input_pipelines_timely_workers_processes.len());
    for num_processes in num_input_pipelines_timely_workers_processes {
        let (worker_pipeline_promises, network_pipeline_futures) = crate::promise_futures(threads, *num_processes);
        for (index, thread_pipeline_promises) in worker_pipeline_promises.into_iter().enumerate() {
            input_pipelines_workers_promises[index].push(thread_pipeline_promises);
        }
        input_pipelines_network_futures.push(network_pipeline_futures);
    }
    let mut output_pipelines_workers_futures = Vec::with_capacity(threads);
    for _ in 0 .. threads {
        output_pipelines_workers_futures.push(Vec::new());
    }
    let mut output_pipelines_network_promises = Vec::with_capacity(num_output_pipelines_timely_workers_processes.len());
    for num_processes in num_output_pipelines_timely_workers_processes {
        let (network_pipeline_promises, worker_pipeline_futures) = crate::promise_futures(*num_processes, threads);
        for (index, thread_pipeline_futures) in worker_pipeline_futures.into_iter().enumerate() {
            output_pipelines_workers_futures[index].push(thread_pipeline_futures);
        }
        output_pipelines_network_promises.push(network_pipeline_promises);
    }

    let builders =
        allocators
            .into_iter()
            .zip(worker_promises)
            .zip(worker_futures)
            .zip(input_pipelines_workers_promises)
            .zip(output_pipelines_workers_futures)
            .enumerate()
            .map(|(index, ((((inner, promises), futures), pipelines_promises), pipelines_futures))| {
                // promises and futures's length is #process-1
                TcpBuilder {
                    inner,
                    index: my_process * threads + index,
                    peers: threads * processes,
                    promises,
                    futures,

                    num_input_pipelines_workers: num_input_pipelines_workers.clone(),
                    input_pipelines_promises: pipelines_promises,
                    num_output_pipelines_workers: num_output_pipelines_workers.clone(),
                    output_pipelines_futures: pipelines_futures,
                }})
            .collect();

    DirectRelayWorkerTcpBuilder {
        worker_thread_builders: builders,
        network_peer_promises: network_promises,
        network_peer_futures: network_futures,
        network_input_pipelines_futures: input_pipelines_network_futures,
        network_output_pipelines_promises: output_pipelines_network_promises
    }
}


impl TcpBuilder {
    // TcpBuilder is running on the current worker thread

    /// Builds a `TcpAllocator`, instantiating `Rc<RefCell<_>>` elements.
    pub fn build(self) -> TcpAllocator {
        // Fulfill puller obligations
        // to receive messages
        // recvs from contains the MergeQueue to pull data from every other process
        // recvs has (#processes - 1) elements
        let mut recvs = Vec::with_capacity(self.peers);
        // into_iter moves the elements
        for promise in self.promises.into_iter() {
            // create buzzer to the worker thread
            let buzzer = crate::buzzer::Buzzer::new();
            let queue = MergeQueue::new(buzzer);
            promise.send(queue.clone()).expect("Failed to send MergeQueue");
            // MergeQueue can be shared across threads
            recvs.push(queue.clone());
        }

        // Extract pusher commitments.
        // used to send messages
        // sends contains the SendEndPoint (which wraps the MergeQueue) to every other process
        // sends has (#processes - 1) elements
        let mut sends = Vec::with_capacity(self.peers);
        for pusher in self.futures.into_iter() {
            let queue = pusher.recv().expect("Failed to receive push queue");
            // use SendEndpoint to wrap up the MergeQueue to stage writes.
            let sendpoint = SendEndpoint::new(queue);
            sends.push(Rc::new(RefCell::new(sendpoint)));
        }

        let mut input_pipelines_recvs = Vec::with_capacity(self.num_input_pipelines_workers.len());
        for pipeline_promises in self.input_pipelines_promises.into_iter() {
            let mut pipeline_recvs = Vec::with_capacity(pipeline_promises.len());
            for promise in pipeline_promises {
                let buzzer = crate::buzzer::Buzzer::new();
                let queue = MergeQueue::new(buzzer);
                promise.send(queue.clone()).expect("Failed to send MergeQueue");
                pipeline_recvs.push(queue.clone());
            }
            input_pipelines_recvs.push(pipeline_recvs)
        }

        let mut output_pipelines_sends = Vec::with_capacity(self.num_output_pipelines_workers.len());
        for pipeline_futures in self.output_pipelines_futures.into_iter() {
            let mut pipeline_sends = Vec::with_capacity(pipeline_futures.len());
            for future in pipeline_futures {
                let queue = future.recv().expect("Failed to receive push queue");
                let sendpoint = SendEndpoint::new(queue);
                pipeline_sends.push(Rc::new(RefCell::new(sendpoint)));
            }
            output_pipelines_sends.push(pipeline_sends);
        }

        let mut input_pipelines_events = Vec::with_capacity(self.num_input_pipelines_workers.len());
        let mut input_pipelines_stages = Vec::with_capacity(self.num_input_pipelines_workers.len());
        let mut input_pipelines_canaries = Vec::with_capacity(self.num_input_pipelines_workers.len());
        let mut input_pipelines_recvs_to_local = Vec::with_capacity(self.num_input_pipelines_workers.len());
        let mut allocated_input_pipelines_channels = Vec::with_capacity(self.num_input_pipelines_workers.len());
        let mut dropped_input_pipelines_channels = Vec::with_capacity(self.num_input_pipelines_workers.len());

        for _index in 0..self.num_input_pipelines_workers.len() {
            input_pipelines_events.push(Rc::new(RefCell::new(VecDeque::new())));
            input_pipelines_stages.push(Vec::new());
            input_pipelines_canaries.push(Rc::new(RefCell::new(Vec::new())));
            input_pipelines_recvs_to_local.push(HashMap::new());
            allocated_input_pipelines_channels.push(HashSet::new());
            dropped_input_pipelines_channels.push(HashSet::new());
        }

        TcpAllocator {
            inner: self.inner.build(),
            index: self.index,
            peers: self.peers,
            canaries: Rc::new(RefCell::new(Vec::new())),
            channel_id_bound: None,
            staged: Vec::new(),
            sends,
            recvs,
            to_local: HashMap::new(),

            num_input_pipelines_workers: self.num_input_pipelines_workers,
            num_output_pipeline_workers: self.num_output_pipelines_workers,
            sends_output_pipelines: output_pipelines_sends,
            recvs_input_pipelines: input_pipelines_recvs,
            input_pipelines_recvs_to_local,
            input_pipelines_events,
            input_pipelines_stages,
            input_pipelines_canaries,
            allocated_input_pipelines_channels,
            dropped_input_pipelines_channels,
        }
    }
}

/// A TCP-based allocator for inter-process communication.
pub struct TcpAllocator {
    // inner is allocator
    inner:      Process,                                  // A non-serialized inner allocator for process-local peers.

    index:      usize,                              // number out of peers
    peers:      usize,                              // number of peer allocators (for typed channel allocation).

    staged:     Vec<Bytes>,                         // staging area for incoming Bytes
    canaries:   Rc<RefCell<Vec<usize>>>,

    channel_id_bound: Option<usize>,

    // sending, receiving, and responding to binary buffers.
    sends:      Vec<Rc<RefCell<SendEndpoint<MergeQueue>>>>,     // sends[x] -> goes to process x.
    recvs:      Vec<MergeQueue>,                                // recvs[x] <- from process x.
    to_local:   HashMap<usize, Rc<RefCell<VecDeque<Bytes>>>>,   // to worker-local typed pullers.

    // number of worker threads for each input pipeline
    num_input_pipelines_workers: Vec<usize>,
    // number of worker threads for each output pipeline
    num_output_pipeline_workers: Vec<usize>,
    // (num_output_pipelines, num_worker_process_in_i_th_pipeline)
    sends_output_pipelines: Vec<Vec<Rc<RefCell<SendEndpoint<MergeQueue>>>>>,
    // (num_input_pipelines, num_worker_process_in_i_th_pipeline)
    recvs_input_pipelines: Vec<Vec<MergeQueue>>,
    // data receive from each input pipeline, push them to the corresponding channel
    // of this input pipeline
    // note: channels are with respect to each pipeline
    input_pipelines_recvs_to_local: Vec<HashMap<usize, Rc<RefCell<VecDeque<Bytes>>>>>,
    // events of input pipelines, one VecDeque for each input pipeline
    input_pipelines_events: Vec<Rc<RefCell<VecDeque<(usize, Event)>>>>,
    // staged area for input pipelines
    input_pipelines_stages: Vec<Vec<Bytes>>,
    // used by the input pipeline pullers to signal channel drop
    input_pipelines_canaries: Vec<Rc<RefCell<Vec<usize>>>>,
    // allocated input pipeline channels
    allocated_input_pipelines_channels: Vec<HashSet<usize>>,
    // dropped input pipeline channels
    dropped_input_pipelines_channels: Vec<HashSet<usize>>,
}

impl Allocate for TcpAllocator {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn allocate<T: Data>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<Message<T>>>>, Box<dyn Pull<Message<T>>>) {

        // Assume and enforce in-order identifier allocation.
        if let Some(bound) = self.channel_id_bound {
            assert!(bound < identifier);
        }
        self.channel_id_bound = Some(identifier);

        // Result list of boxed pushers.
        let mut pushes = Vec::<Box<dyn Push<Message<T>>>>::new();

        // Inner exchange allocations.
        let inner_peers = self.inner.peers();
        // pushers to the threads in this (same) process and receiver to this thread
        let (mut inner_sends, inner_recv) = self.inner.allocate(identifier);

        for target_index in 0 .. self.peers() {

            // TODO: crappy place to hardcode this rule.
            let mut process_id = target_index / inner_peers;

            // if the target thread and current thread is on the same progress
            if process_id == self.index / inner_peers {
                // inter-thread, intra-process
                // use ProcessAllocator's allocated pusher to push
                pushes.push(inner_sends.remove(0));
            }
            else {
                // message header template.
                // construct message header, which is sent each time when we send messages through the nextwork using pusher from push_pull.rs
                let header = MessageHeader {
                    channel:    identifier,
                    source:     self.index,
                    target:     target_index,
                    length:     0,
                    seqno:      0,
                };

                // create, box, and stash new process_binary pusher.
                // we need to decrease the index by 1
                // since we don't establish a connection to the current running process
                if process_id > self.index / inner_peers { process_id -= 1; }
                pushes.push(Box::new(TcpPusher::new(header, self.sends[process_id].clone())));
            }
        }

        // establish a VecQueue for this thread to store data pulled from other processes
        // not for data obtained through inter-thread, intra-progress communication
        // again, the identifier is just used to indicate the #id of the group of chnanels
        // we can communicate to other workers (threads) using different channels to transmit different types of information
        let channel =
            self.to_local
                .entry(identifier)
                .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
                .clone();

        // we will know when another
        let canary = Canary::new(identifier, self.canaries.clone());
        let puller = Box::new(CountPuller::new(TcpPullerInner::new(inner_recv, channel, canary), identifier, self.events().clone()));

        (pushes, puller, )
    }

    // Perform preparatory work, most likely reading binary buffers from self.recv.
    #[inline(never)]
    fn receive(&mut self) {

        // Check for channels whose `Puller` has been dropped.
        // drop resources
        let mut canaries = self.canaries.borrow_mut();
        for dropped_channel in canaries.drain(..) {
            let _dropped =
                self.to_local
                    .remove(&dropped_channel)
                    .expect("non-existent channel dropped");
            // Borrowed channels may be non-empty, if the dataflow was forcibly
            // dropped. The contract is that if a dataflow is dropped, all other
            // workers will drop the dataflow too, without blocking indefinitely
            // on events from it.
            // assert!(dropped.borrow().is_empty());
        }
        ::std::mem::drop(canaries);

        self.inner.receive();

        for recv in self.recvs.iter_mut() {
            recv.drain_into(&mut self.staged);
        }

        let mut events = self.inner.events().borrow_mut();

        for mut bytes in self.staged.drain(..) {

            // We expect that `bytes` contains an integral number of messages.
            // No splitting occurs across allocations.
            while bytes.len() > 0 {

                if let Some(header) = MessageHeader::try_read(&mut bytes[..]) {

                    // Get the header and payload, ditch the header.
                    let mut peel = bytes.extract_to(header.required_bytes());
                    // ditch the header, header is 40 bytes
                    let _ = peel.extract_to(40);

                    // Increment message count for channel.
                    // Safe to do this even if the channel has been dropped.
                    events.push_back((header.channel, Event::Pushed(1)));

                    // Ensure that a queue exists.
                    // push to thread-local store
                    match self.to_local.entry(header.channel) {
                        Entry::Vacant(entry) => {
                            // We may receive data before allocating, and shouldn't block.
                            // before we call allocate method on this thread
                            // the pushers and pullers are allocated seperately for each process
                            // while in Process allocator, all the channels for all threads are allocated at once.
                            if self.channel_id_bound.map(|b| b < header.channel).unwrap_or(true) {
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

    // Perform postparatory work, most likely sending un-full binary buffers.
    fn release(&mut self) {
        // Publish outgoing byte ledgers.
        // send data to the send_loop network threads
        // emtpy buffer
        for send in self.sends.iter_mut() {
            send.borrow_mut().publish();
        }

        // OPTIONAL: Tattle on channels sitting on borrowed data.
        // OPTIONAL: Perhaps copy borrowed data into owned allocation.
        // for (index, list) in self.to_local.iter() {
        //     let len = list.borrow_mut().len();
        //     if len > 0 {
        //         eprintln!("Warning: worker {}, undrained channel[{}].len() = {}", self.index, index, len);
        //     }
        // }
    }
    fn events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>> {
        self.inner.events()
    }
    fn await_events(&self, duration: Option<std::time::Duration>) {
        self.inner.await_events(duration);
    }
}


impl DirectRelayAllocate for TcpAllocator
{
    fn get_num_input_pipelines(&self) -> usize {
        self.num_input_pipelines_workers.len()
    }

    fn get_num_output_pipelines(&self) -> usize {
        self.num_output_pipeline_workers.len()
    }

    fn get_num_input_pipeline_workers(&self, pipeline_index: usize) -> usize {
        self.num_input_pipelines_workers[pipeline_index]
    }

    fn get_num_output_pipeline_workers(&self, pipeline_index: usize) -> usize {
        self.num_output_pipeline_workers[pipeline_index]
    }

    fn allocate_input_pipeline_puller<T: Data>(&mut self, pipeline_index: usize, channel_identifier: usize) -> Box<dyn Pull<Message<T>>> {
        let channel =
            self.input_pipelines_recvs_to_local[pipeline_index]
                .entry(channel_identifier)
                .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
                .clone();

        let canary = Canary::new(channel_identifier, self.input_pipelines_canaries[pipeline_index].clone());
        let puller = Box::new(CountPuller::new(
            TcpPuller::new(channel, canary),
            channel_identifier,
            self.input_pipelines_events[pipeline_index].clone()));

        self.allocated_input_pipelines_channels[pipeline_index].insert(channel_identifier);
        puller
    }

    fn allocate_output_pipeline_pushers<T: Data>(&mut self, pipeline_index: usize, channel_identifier: usize) -> Vec<Box<dyn Push<Message<T>>>> {
        let num_output_pipeline_workers = self.num_output_pipeline_workers[pipeline_index];
        let num_output_pipeline_processes = self.sends_output_pipelines[pipeline_index].len();
        let mut pushers = Vec::<Box<dyn Push<Message<T>>>>::with_capacity(num_output_pipeline_workers);

        for remote_worker_idx in 0..num_output_pipeline_workers {
            let process_id = remote_worker_idx / num_output_pipeline_processes;

            let header = MessageHeader {
                channel: channel_identifier,
                source: self.index,
                target: remote_worker_idx,
                length: 0,
                seqno: 0,
            };

            pushers.push(Box::new(TcpPusher::new(header, self.sends_output_pipelines[pipeline_index][process_id].clone())));
        }
        pushers
    }

    fn receive_from_input_pipeline(&mut self, pipeline_index: usize) {
        // Check for channels whose `Puller` has been dropped.
        let mut canaries = self.input_pipelines_canaries[pipeline_index].borrow_mut();
        for dropped_channel in canaries.drain(..) {
            let _dropped =
                self.input_pipelines_recvs_to_local[pipeline_index]
                    .remove(&dropped_channel)
                    .expect("non-existent channel dropped");
            self.dropped_input_pipelines_channels[pipeline_index].insert(dropped_channel);
            // Borrowed channels may be non-empty, if the dataflow was forcibly
            // dropped. The contract is that if a dataflow is dropped, all other
            // workers will drop the dataflow too, without blocking indefinitely
            // on events from it.
            // assert!(dropped.borrow().is_empty());
        }
        std::mem::drop(canaries);

        let mut events = (*self.input_pipelines_events[pipeline_index]).borrow_mut();

        for recv in self.recvs_input_pipelines[pipeline_index].iter_mut() {
            recv.drain_into(&mut self.input_pipelines_stages[pipeline_index]);
        }

        for mut bytes in self.input_pipelines_stages[pipeline_index].drain(..) {
            while bytes.len() > 0 {
                if let Some(header) = MessageHeader::try_read(&mut bytes[..]) {

                    // Get the header and payload, ditch the header.
                    let mut peel = bytes.extract_to(header.required_bytes());
                    // drop MessageHeader
                    let _ = peel.extract_to(40);

                    // Increment message count for channel.
                    // Safe to do this even if the channel has been dropped.
                    events.push_back((header.channel, Event::Pushed(1)));

                    // Ensure that a queue exists.
                    match self.input_pipelines_recvs_to_local[pipeline_index].entry(header.channel) {
                        Entry::Vacant(entry) => {
                            // We may receive data before allocating, and shouldn't block.
                            if !self.dropped_input_pipelines_channels[pipeline_index].contains(&header.channel) {
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

    fn finish_input_pipeline_channel_allocation(&mut self, pipeline_index: usize) {
        let current_channels = self.input_pipelines_recvs_to_local[pipeline_index].keys().cloned().collect::<Vec<_>>();
        for channel in current_channels {
            if !self.allocated_input_pipelines_channels[pipeline_index].contains(&channel) {
                let _dropped =
                    self.input_pipelines_recvs_to_local[pipeline_index]
                        .remove(&channel)
                        .expect("non-existent channel dropped");
                self.dropped_input_pipelines_channels[pipeline_index].insert(channel);
            }
        }
    }

    fn release_output_pipeline_send(&mut self, pipeline_index: usize) {
        // send everything in buffer
        for send in self.sends_output_pipelines[pipeline_index].iter_mut() {
            (**send).borrow_mut().publish();
        }
    }

    fn input_pipeline_events(&self, pipeline_index: usize) -> &Rc<RefCell<VecDeque<(usize, Event)>>> {
        &self.input_pipelines_events[pipeline_index]
    }

    fn await_input_pipeline_events(&self, pipeline_index: usize, _duration: Option<Duration>) {
        if self.input_pipelines_events[pipeline_index].borrow().is_empty() {
            if let Some(duration) = _duration {
                std::thread::park_timeout(duration);
            }
            else {
                std::thread::park();
            }
        }
    }

    fn await_any_input_pipelines_events(&self, _duration: Option<Duration>) {
        let flag = self.input_pipelines_events.iter().all(|x| x.borrow().is_empty());
        if flag {
            if let Some(duration) = _duration {
                std::thread::park_timeout(duration);
            }
            else {
                std::thread::park();
            }
        }
    }

    fn await_any_events(&self, _duration: Option<Duration>) {
        let flag = self.input_pipelines_events.iter().all(|x| x.borrow().is_empty())
            && self.inner.events().borrow().is_empty();
        if flag {
            if let Some(duration) = _duration {
                std::thread::park_timeout(duration);
            }
            else {
                std::thread::park();
            }
        }
    }

    fn input_pipeline_completed(&self, pipeline_index: usize) -> bool {
        self.recvs_input_pipelines[pipeline_index].iter().all(|x| x.is_complete())
    }
}

