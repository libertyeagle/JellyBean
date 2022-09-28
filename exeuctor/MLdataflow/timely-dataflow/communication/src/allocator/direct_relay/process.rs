// READ
//! Typed inter-thread, intra-process channels.

use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::collections::hash_map::Entry;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender};

use bytes::arc::Bytes;

use crate::{Data, Message, MessageLatency, Pull, Push};
use crate::allocator::{Allocate, AllocateBuilder, Event, Thread};
use crate::allocator::canary::Canary;
use crate::allocator::counters::Puller as CountPuller;
use crate::allocator::direct_relay::DirectRelayAllocate;
use crate::allocator::thread::ThreadBuilder;
use crate::allocator::zero_copy::bytes_exchange::{BytesPull, MergeQueue, SendEndpoint};
use crate::allocator::zero_copy::push_pull::{Puller as TcpPuller, Pusher as TcpPusher};
use crate::buzzer::Buzzer;
use crate::networking::MessageHeader;

/// Builder for direct relay - single process
pub struct DirectRelayWorkerProcessBuilder {
    /// Builders for the worker threads in the current process
    pub worker_thread_builders: Vec<ProcessBuilder>,
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

/// An allocator for inter-thread, intra-process communication
pub struct ProcessBuilder {
    inner: ThreadBuilder,
    index: usize,
    peers: usize,
    // below: `Box<Any+Send>` is a `Box<Vec<Option<(Vec<Sender<T>>, Receiver<T>)>>>`
    channels: Arc<Mutex<HashMap<usize, Box<dyn Any+Send>>>>,

    // Buzzers for waking other local workers.
    buzzers_send: Vec<Sender<Buzzer>>,
    buzzers_recv: Vec<Receiver<Buzzer>>,

    counters_send: Vec<Sender<(usize, Event)>>,
    counters_recv: Receiver<(usize, Event)>,

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

impl AllocateBuilder for ProcessBuilder {
    type Allocator = Process;
    fn build(self) -> Self::Allocator {
        // Initialize buzzers; send first, then recv.
        // iter(), which iterates over &T.
        // iter_mut(), which iterates over &mut T.
        // into_iter(), which iterates over T.
        // crossbeam channels have already been established at this point

        // note that AllocateBuilder is moved and runs in the worker thread
        for worker in self.buzzers_send.iter() {
            // we create buzzer of current thread, send them to the peers
            let buzzer = Buzzer::new();
            worker.send(buzzer).expect("Failed to send buzzer");
        }
        let mut buzzers = Vec::with_capacity(self.buzzers_recv.len());
        for worker in self.buzzers_recv.iter() {
            buzzers.push(worker.recv().expect("Failed to recv buzzer"));
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

        Process {
            inner: self.inner.build(),
            index: self.index,
            peers: self.peers,
            channels: self.channels,
            buzzers,
            counters_send: self.counters_send,
            counters_recv: self.counters_recv,
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

/// An allocator for inter-thread, intra-process communication
// each allocator is only bounded to a single worker thread
pub struct Process {
    inner: Thread,
    index: usize,
    peers: usize,
    // below: `Box<Any+Send>` is a `Box<Vec<Option<(Vec<Sender<T>>, Receiver<T>)>>>`
    channels: Arc<Mutex<HashMap</* channel id */ usize, Box<dyn Any+Send>>>>,
    buzzers: Vec<Buzzer>,
    counters_send: Vec<Sender<(usize, Event)>>,
    counters_recv: Receiver<(usize, Event)>,

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

impl Process {
    /// Access the wrapped inner allocator.
    pub fn inner(&mut self) -> &mut Thread { &mut self.inner }
    /// Allocate a list of connected intra-process allocators.
    pub fn new_vector_with_direct_connect(
        peers: usize,
        num_input_pipelines_timely_workers_processes: &[usize],
        num_output_pipelines_timely_workers_processes: &[usize],
        num_input_pipelines_threads_per_process: &[usize],
        num_output_pipelines_threads_per_process: &[usize],
    ) -> DirectRelayWorkerProcessBuilder
    {
        let num_input_pipelines_workers = num_input_pipelines_timely_workers_processes.iter().zip(num_input_pipelines_threads_per_process.iter()).map(|(x, y)| *x * *y).collect::<Vec<_>>();
        let num_output_pipelines_workers = num_output_pipelines_timely_workers_processes.iter().zip(num_output_pipelines_threads_per_process.iter()).map(|(x, y)| *x * *y).collect::<Vec<_>>();

        let mut input_pipelines_workers_promises = Vec::with_capacity(peers);
        for _ in 0 .. peers {
            input_pipelines_workers_promises.push(Vec::new());
        }
        let mut input_pipelines_network_futures = Vec::with_capacity(num_input_pipelines_timely_workers_processes.len());
        for num_processes in num_input_pipelines_timely_workers_processes {
            let (worker_pipeline_promises, network_pipeline_futures) = crate::promise_futures(peers, *num_processes);
            for (index, thread_pipeline_promises) in worker_pipeline_promises.into_iter().enumerate() {
                input_pipelines_workers_promises[index].push(thread_pipeline_promises);
            }
            input_pipelines_network_futures.push(network_pipeline_futures);
        }
        let mut output_pipelines_workers_futures = Vec::with_capacity(peers);
        for _ in 0 .. peers {
            output_pipelines_workers_futures.push(Vec::new());
        }
        let mut output_pipelines_network_promises = Vec::with_capacity(num_output_pipelines_timely_workers_processes.len());
        for num_processes in num_output_pipelines_timely_workers_processes {
            let (network_pipeline_promises, worker_pipeline_futures) = crate::promise_futures(*num_processes, peers);
            for (index, thread_pipeline_futures) in worker_pipeline_futures.into_iter().enumerate() {
                output_pipelines_workers_futures[index].push(thread_pipeline_futures);
            }
            output_pipelines_network_promises.push(network_pipeline_promises);
        }

        let mut counters_send = Vec::with_capacity(peers);
        let mut counters_recv = Vec::with_capacity(peers);
        for _ in 0 .. peers {
            let (send, recv) = crossbeam_channel::unbounded();
            counters_send.push(send);
            counters_recv.push(recv);
        }
        let channels = Arc::new(Mutex::new(HashMap::with_capacity(peers)));
        let (buzzers_send, buzzers_recv) = crate::promise_futures(peers, peers);

        let builders = counters_recv
            .into_iter()
            .zip(buzzers_send.into_iter())
            .zip(buzzers_recv.into_iter())
            .zip(input_pipelines_workers_promises)
            .zip(output_pipelines_workers_futures)
            .enumerate()
            .map(|(index, ((((recv, bsend), brecv), pipeline_promises), pipeline_futures))| {
                ProcessBuilder {
                    inner: ThreadBuilder,
                    index,
                    peers,
                    buzzers_send: bsend,
                    buzzers_recv: brecv,
                    channels: channels.clone(),
                    // all workers (threads) share the list of event senders (to each thread)
                    counters_send: counters_send.clone(),
                    counters_recv: recv,

                    num_input_pipelines_workers: num_input_pipelines_workers.clone(),
                    input_pipelines_promises: pipeline_promises,
                    num_output_pipelines_workers: num_output_pipelines_workers.clone(),
                    output_pipelines_futures: pipeline_futures,
                }
            })
            .collect();

        DirectRelayWorkerProcessBuilder {
            worker_thread_builders: builders,
            network_input_pipelines_futures: input_pipelines_network_futures,
            network_output_pipelines_promises: output_pipelines_network_promises
        }
    }
}

impl Allocate for Process {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn allocate<T: Any+Send+Sync+'static>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<Message<T>>>>, Box<dyn Pull<Message<T>>>) {

        // this is race-y global initialisation of all channels for all workers, performed by the
        // first worker that enters this critical section

        // ensure exclusive access to shared list of channels
        let mut channels = self.channels.lock().expect("mutex error?");

        let (sends, recv, empty) = {

            // we may need to alloc a new channel ...
            // NOTE: the identifier is not the id of worker, but rather the id of the group of channels
            // each id corresponds to a group of all the channels for all workers
            // we only need to allocate it once
            // or_insert_with returns a mutable reference to the inserted element
            let entry = channels.entry(identifier).or_insert_with(|| {

                let mut pushers = Vec::with_capacity(self.peers);
                let mut pullers = Vec::with_capacity(self.peers);
                for buzzer in self.buzzers.iter() {
                    // use crossbeam to construct pushers and pullers for each thread
                    let (s, r): (Sender<Message<T>>, Receiver<Message<T>>) = crossbeam_channel::unbounded();
                    // TODO: the buzzer in the pusher may be redundant, because we need to buzz post-counter.
                    pushers.push((Pusher { target: s }, buzzer.clone()));
                    pullers.push(Puller { source: r, current: None, current_with_dummy_latency: None });
                
                }

                // channels[i] is the tuple of #peers pushers and a single puller for the i-th worker
                let mut to_box = Vec::with_capacity(pullers.len());
                for recv in pullers.into_iter() {
                    // each worker 
                    to_box.push(Some((pushers.clone(), recv)));
                }

                Box::new(to_box)
            });
            
            // vector: &mut Vec<Option<(Vec<(Pusher<Message<T>>, Buzzer)>, Puller<Message<T>>)>>
            let vector =
            entry
                // downcast_mut: returns some mutable reference to the boxed value if it is of type T, or None if it isn’t.
                .downcast_mut::<Vec<Option<(Vec<(Pusher<Message<T>>, Buzzer)>, Puller<Message<T>>)>>>()
                .expect("failed to correctly cast channel");
            
            // self.channels is just used as a sort of buffers

            let (sends, recv) =
            vector[self.index]
                .take()
                .expect("channel already consumed");

            // is channels[self.index] empty?
            let empty = vector.iter().all(|x| x.is_none());

            (sends, recv, empty)
        };

        // send is a vec of all senders, recv is this worker's receiver

        // if the current group of channels is exhausted, remove it in the hash map.
        if empty { channels.remove(&identifier); }

        use crate::allocator::counters::ArcPusher as CountPusher;

        let sends =
        sends.into_iter()
             .zip(self.counters_send.iter())
             // ((Pusher<Message<T>, Buzzer), &Sender<(usize, Event)>)
             // CountPusher wraps 
             .map(|((s,b), sender)| CountPusher::new(s, identifier, sender.clone(), b))
             .map(|s| Box::new(s) as Box<dyn Push<super::Message<T>>>)
             .collect::<Vec<_>>();

        let recv = Box::new(CountPuller::new(recv, identifier, self.inner.events().clone())) as Box<dyn Pull<super::Message<T>>>;

        (sends, recv)
    }

    fn events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>> {
        self.inner.events()
    }

    fn await_events(&self, duration: Option<Duration>) {
        self.inner.await_events(duration);
    }

    fn receive(&mut self) {
        let mut events = self.inner.events().borrow_mut();
        while let Ok((index, event)) = self.counters_recv.try_recv() {
            events.push_back((index, event));
        }
    }
}


impl DirectRelayAllocate for Process
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

/// The push half of an intra-process channel./
// pusher and puller each owns the respective Sender and Receiver parts of the crossbeam channel
struct Pusher<T> {
    target: Sender<T>,
}

impl<T> Clone for Pusher<T> {
    fn clone(&self) -> Self {
        Self {
            target: self.target.clone(),
        }
    }
}

impl<T> Push<T> for Pusher<T> {
    #[inline]
    fn push(&mut self, element: &mut Option<T>) {
        if let Some(element) = element.take() {
            // The remote endpoint could be shut down, and so
            // it is not fundamentally an error to fail to send.
            let _ = self.target.send(element);
        }
    }

    fn push_with_latency_passthrough(&mut self, element: &mut Option<T>, _latency: Option<MessageLatency>) {
        self.push(element);
    }
}

/// The pull half of an intra-process channel.
struct Puller<T> {
    current: Option<T>,
    current_with_dummy_latency: Option<(T, MessageLatency)>,
    source: Receiver<T>,
}

impl<T> Pull<T> for Puller<T> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> {
        self.current = self.source.try_recv().ok();
        &mut self.current
    }

    fn pull_with_transmission_latency(&mut self) -> &mut Option<(T, MessageLatency)> {
        self.current = self.source.try_recv().ok();
        self.current_with_dummy_latency = self.current.take().map(|x| (x, 0));
        &mut self.current_with_dummy_latency
    }
}
