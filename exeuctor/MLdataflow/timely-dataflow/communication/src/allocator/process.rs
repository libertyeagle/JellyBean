// READ
//! Typed inter-thread, intra-process channels.

use std::rc::Rc;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::any::Any;
use std::time::Duration;
use std::collections::{HashMap, HashSet, VecDeque};
use crossbeam_channel::{Sender, Receiver};
use bytes::arc::Bytes;

use crate::allocator::thread::{ThreadBuilder};
use crate::allocator::{Allocate, AllocateBuilder, Event, Thread};
use crate::{Push, Pull, Message, MessageLatency};
use crate::allocator::zero_copy::bytes_exchange::{MergeQueue, SendEndpoint};
use crate::buzzer::Buzzer;

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

    // number of relay nodes
    relay_peers: Option<usize>,
    // to receive MergeQueue from network thread of send_loop
    // to send messages to relay nodes
    relay_futures: Option<Vec<Receiver<MergeQueue>>>,
    // to receive messages from relay node
    relay_promises: Option<Vec<Sender<MergeQueue>>>,
}

impl AllocateBuilder for ProcessBuilder {
    type Allocator = Process;
    fn build(mut self) -> Self::Allocator {

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

            Process {
                inner: self.inner.build(),
                index: self.index,
                peers: self.peers,
                channels: self.channels,
                buzzers,
                counters_send: self.counters_send,
                counters_recv: self.counters_recv,
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
            Process {
                inner: self.inner.build(),
                index: self.index,
                peers: self.peers,
                channels: self.channels,
                buzzers,
                counters_send: self.counters_send,
                counters_recv: self.counters_recv,
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

impl Process {
    /// Access the wrapped inner allocator.
    pub fn inner(&mut self) -> &mut Thread { &mut self.inner }
    /// Allocate a list of connected intra-process allocators.
    pub fn new_vector(peers: usize) -> Vec<ProcessBuilder> {

        let mut counters_send = Vec::with_capacity(peers);
        let mut counters_recv = Vec::with_capacity(peers);
        for _ in 0 .. peers {
            // each thread (worker) gets #peers senders to send Event to each other thread
            // each thread also has a single receiver to receive the events
            let (send, recv) = crossbeam_channel::unbounded();
            counters_send.push(send);
            counters_recv.push(recv);
        }

        // channels shared among all workers
        // use Arc and Mutex together, because Mutex does not implement Copy to move between threads
        // currently does not have elements, wating to be allocated
        let channels = Arc::new(Mutex::new(HashMap::with_capacity(peers)));

        // Allocate matrix of buzzer send and recv endpoints.
        // buzzers_send[i] is a vector of crossbeam Senders to send buzzers for the i-th workder
        let (buzzers_send, buzzers_recv) = crate::promise_futures(peers, peers);

        counters_recv
            // into_iter consumes the elements
            .into_iter()
            .zip(buzzers_send.into_iter())
            .zip(buzzers_recv.into_iter())
            .enumerate()
            .map(|(index, ((recv, bsend), brecv))| {
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
                    relay_peers: None,
                    relay_promises: None,
                    relay_futures: None
                }
            })
            .collect()
    }
    /// Allocate a list of connected intra-process allocators, with communication to relay nodes
    pub fn new_vector_with_relay_connection(
        peers: usize,
        num_relay_nodes: usize
    ) -> (
        Vec<ProcessBuilder>,
        Vec<Vec<Sender<MergeQueue>>>,
        Vec<Vec<Receiver<MergeQueue>>>
    )
    {
        let (network_relay_promises, worker_relay_futures) = crate::promise_futures(num_relay_nodes, peers);
        let (worker_relay_promises, network_relay_futures) = crate::promise_futures(peers, num_relay_nodes);

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
            .zip(worker_relay_promises)
            .zip(worker_relay_futures)
            .enumerate()
            .map(|(index, ((((recv, bsend), brecv), promises), futures))| {
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
                    relay_peers: Some(num_relay_nodes),
                    relay_promises: Some(promises),
                    relay_futures: Some(futures)
                }
            })
            .collect();
        (builders, network_relay_promises, network_relay_futures)
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
        use crate::allocator::counters::Puller as CountPuller;

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
    #[inline] fn push(&mut self, element: &mut Option<T>) {
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
