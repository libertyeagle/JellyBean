// READ, Sep 8 2021
//! Zero-copy allocator based on TCP.
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{VecDeque, HashMap, hash_map::Entry, HashSet};
use crossbeam_channel::{Sender, Receiver};

use bytes::arc::Bytes;

use crate::networking::MessageHeader;

use crate::{Allocate, Message, Data, Push, Pull, MessageLatency};
use crate::allocator::{AllocateBuilder};
use crate::allocator::Event;
use crate::allocator::canary::Canary;

use super::bytes_exchange::{BytesPull, SendEndpoint, MergeQueue};
use super::push_pull::{Pusher, PullerInner};

/// Builds an instance of a TcpAllocator.
///
/// Builders are required because some of the state in a `TcpAllocator` cannot be sent between
/// threads (specifically, the `Rc<RefCell<_>>` local channels). So, we must package up the state
/// shared between threads here, and then provide a method that will instantiate the non-movable
/// members once in the destination thread.
pub struct TcpBuilder<A: AllocateBuilder> {
    // inner is allocator builder
    inner:  A,
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
    // number of relay nodes
    relay_peers: Option<usize>,
    // to receive MergeQueue from network thread of send_loop
    // to send messages to relay nodes
    relay_futures: Option<Vec<Receiver<MergeQueue>>>,
    // to receive messages from relay node
    relay_promises: Option<Vec<Sender<MergeQueue>>>,
}

/// Creates a vector of builders, sharing appropriate state.
///
/// `threads` is the number of workers in a single process, `processes` is the
/// total number of processes.
/// The returned tuple contains
/// ```ignore
/// (
///   AllocateBuilder for local threads,
///   info to spawn egress comm threads,
///   info to spawn ingress comm threads,
/// )
/// ```
pub fn new_vector<A: AllocateBuilder>(
    allocators: Vec<A>,
    my_process: usize,
    processes: usize)
-> (Vec<TcpBuilder<A>>,
    Vec<Vec<Sender<MergeQueue>>>,
    Vec<Vec<Receiver<MergeQueue>>>)
{
    // allocators is a vector of inner allocators
    // its length is #threads/process
    let threads = allocators.len();

    // For queues from worker threads to network threads, and vice versa.
    // for each process, we need to establish TCP connections to other progress
    // for each other process, we establish a network thread for receiving, and a network thread for sending
    let (network_promises, worker_futures) = crate::promise_futures(processes-1, threads);
    let (worker_promises, network_futures) = crate::promise_futures(threads, processes-1);

    let builders =
    allocators
        .into_iter()
        .zip(worker_promises)
        .zip(worker_futures)
        .enumerate()
        .map(|(index, ((inner, promises), futures))| {
            // promises and futures's length is #process-1
            TcpBuilder {
                inner,
                index: my_process * threads + index,
                peers: threads * processes,
                promises,
                futures,
                relay_peers: None,
                relay_promises: None,
                relay_futures: None
            }})
        .collect();

    (builders, network_promises, network_futures)
}

/// create builders for TcpAllocators
/// with the ability to communicate with relay nodes
pub fn new_vector_with_relay_connection<A: AllocateBuilder>(
    allocators: Vec<A>,
    num_relay_nodes: usize,
    my_process: usize,
    processes: usize)
-> (Vec<TcpBuilder<A>>,
    Vec<Vec<Sender<MergeQueue>>>,
    Vec<Vec<Receiver<MergeQueue>>>,
    Vec<Vec<Sender<MergeQueue>>>,
    Vec<Vec<Receiver<MergeQueue>>>)
{
    let threads = allocators.len();

    let (network_promises, worker_futures) = crate::promise_futures(processes-1, threads);
    let (worker_promises, network_futures) = crate::promise_futures(threads, processes-1);

    let (network_relay_promises, worker_relay_futures) = crate::promise_futures(num_relay_nodes, threads);
    let (worker_relay_promises, network_relay_futures) = crate::promise_futures(threads, num_relay_nodes);


    let builders =
        allocators
            .into_iter()
            .zip(worker_promises)
            .zip(worker_futures)
            .zip(worker_relay_promises)
            .zip(worker_relay_futures)
            .enumerate()
            .map(|(index, ((((inner, promises), futures), relay_promises), relay_futures))| {
                // promises and futures's length is #process-1
                TcpBuilder {
                    inner,
                    index: my_process * threads + index,
                    peers: threads * processes,
                    promises,
                    futures,
                    relay_peers: Some(num_relay_nodes),
                    relay_promises: Some(relay_promises),
                    relay_futures: Some(relay_futures)
                }})
            .collect();

    (builders, network_promises, network_futures, network_relay_promises, network_relay_futures)
}


impl<A: AllocateBuilder> TcpBuilder<A> {
    // TcpBuilder is running on the current worker thread

    /// Builds a `TcpAllocator`, instantiating `Rc<RefCell<_>>` elements.
    pub fn build(mut self) -> TcpAllocator<A::Allocator> {

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

        if self.relay_peers.is_some() {
            let relay_peers = self.relay_peers.take().unwrap();
            let mut relay_recvs = Vec::with_capacity(relay_peers);
            for promise in self.relay_promises.take().unwrap().into_iter() {
                let buzzer = crate::buzzer::Buzzer::new();
                let queue = MergeQueue::new(buzzer);
                promise.send(queue.clone()).expect("Failed to send MergeQueue");
                relay_recvs.push(queue.clone());
            }
            let mut relay_sends = Vec::with_capacity(relay_peers);
            for pusher in self.relay_futures.take().unwrap().into_iter() {
                let queue = pusher.recv().expect("Failed to receive push queue");
                let sendpoint = SendEndpoint::new(queue);
                relay_sends.push(Rc::new(RefCell::new(sendpoint)));
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
                relay_peers: Some(relay_peers),
                sends_relay: Some(relay_sends),
                recvs_relay: Some(relay_recvs),
                relay_to_local: Some(HashMap::new()),
                relay_events: Some(Rc::new(RefCell::new(VecDeque::new()))),
                relay_staged: Some(Vec::new()),
                relay_canaries: Some(Rc::new(RefCell::new(Vec::new()))),
                relay_dropped_channels: Some(HashSet::new()),
            }
        }
        else {
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
                relay_peers: None,
                sends_relay: None,
                recvs_relay: None,
                relay_to_local: None,
                relay_events: None,
                relay_staged: None,
                relay_canaries: None,
                relay_dropped_channels: None,
            }
        }

        // let sends: Vec<_> = self.sends.into_iter().map(
        //     |send| Rc::new(RefCell::new(SendEndpoint::new(send)))).collect();
    }
}

/// A TCP-based allocator for inter-process communication.
pub struct TcpAllocator<A: Allocate> {
    // inner is allocator
    inner:      A,                                  // A non-serialized inner allocator for process-local peers.

    index:      usize,                              // number out of peers
    peers:      usize,                              // number of peer allocators (for typed channel allocation).

    staged:     Vec<Bytes>,                         // staging area for incoming Bytes
    canaries:   Rc<RefCell<Vec<usize>>>,

    channel_id_bound: Option<usize>,

    // sending, receiving, and responding to binary buffers.
    sends:      Vec<Rc<RefCell<SendEndpoint<MergeQueue>>>>,     // sends[x] -> goes to process x.
    recvs:      Vec<MergeQueue>,                                // recvs[x] <- from process x.
    to_local:   HashMap<usize, Rc<RefCell<VecDeque<Bytes>>>>,   // to worker-local typed pullers.


    // num relay nodes
    pub(crate) relay_peers: Option<usize>,
    // sending, receiving, and responding to binary buffers for communication to the relay node
    pub(crate) sends_relay: Option<Vec<Rc<RefCell<SendEndpoint<MergeQueue>>>>>,
    pub(crate) recvs_relay: Option<Vec<MergeQueue>>,
    pub(crate) relay_to_local: Option<HashMap<usize, Rc<RefCell<VecDeque<(Bytes, MessageLatency)>>>>>,

    // events of the communication from relay node
    pub(crate) relay_events: Option<Rc<RefCell<VecDeque<(usize, Event)>>>>,
    // staged area for received messages from relay node
    pub(crate) relay_staged: Option<Vec<Bytes>>,
    pub(crate) relay_canaries: Option<Rc<RefCell<Vec<usize>>>>,
    pub(crate) relay_dropped_channels: Option<HashSet<usize>>,
}

impl<A: Allocate> Allocate for TcpAllocator<A> {
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
                pushes.push(Box::new(Pusher::new(header, self.sends[process_id].clone())));
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
        use crate::allocator::counters::Puller as CountPuller;
        let canary = Canary::new(identifier, self.canaries.clone());
        let puller = Box::new(CountPuller::new(PullerInner::new(inner_recv, channel, canary), identifier, self.events().clone()));

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