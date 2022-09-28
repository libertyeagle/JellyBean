// READ
//! A simple communication infrastructure providing typed exchange channels.
//!
//! This crate is part of the timely dataflow system, used primarily for its inter-worker communication.
//! It may be independently useful, but it is separated out mostly to make clear boundaries in the project.
//!
//! Threads are spawned with an [`allocator::Generic`](allocator::generic::Generic), whose
//! [`allocate`](allocator::generic::Generic::allocate) method returns a pair of several send endpoints and one
//! receive endpoint. Messages sent into a send endpoint will eventually be received by the corresponding worker,
//! if it receives often enough. The point-to-point channels are each FIFO, but with no fairness guarantees.
//!
//! To be communicated, a type must implement the [`Serialize`](serde::Serialize) trait when using the
//! `bincode` feature or the [`Abomonation`](abomonation::Abomonation) trait when not.
//!
//! Channel endpoints also implement a lower-level `push` and `pull` interface (through the [`Push`](Push) and [`Pull`](Pull)
//! traits), which is used for more precise control of resources.
//!
//! # Examples
//! ```
//! use timely_communication::Allocate;
//!
//! // configure for two threads, just one process.
//! let config = timely_communication::Config::Process(2);
//!
//! // initializes communication, spawns workers
//! let guards = timely_communication::initialize(config, |mut allocator| {
//!     println!("worker {} started", allocator.index());
//!
//!     // allocates a pair of senders list and one receiver.
//!     let (mut senders, mut receiver) = allocator.allocate(0);
//!
//!     // send typed data along each channel
//!     use timely_communication::Message;
//!     senders[0].send(Message::from_typed(format!("hello, {}", 0)));
//!     senders[1].send(Message::from_typed(format!("hello, {}", 1)));
//!
//!     // no support for termination notification,
//!     // we have to count down ourselves.
//!     let mut expecting = 2;
//!     while expecting > 0 {
//!
//!         allocator.receive();
//!         if let Some(message) = receiver.recv() {
//!             use std::ops::Deref;
//!             println!("worker {}: received: <{}>", allocator.index(), message.deref());
//!             expecting -= 1;
//!         }
//!         allocator.release();
//!     }
//!
//!     // optionally, return something
//!     allocator.index()
//! });
//!
//! // computation runs until guards are joined or dropped.
//! if let Ok(guards) = guards {
//!     for guard in guards.join() {
//!         println!("result: {:?}", guard);
//!     }
//! }
//! else { println!("error in computation"); }
//! ```
//!
//! The should produce output like:
//!
//! ```ignore
//! worker 0 started
//! worker 1 started
//! worker 0: received: <hello, 0>
//! worker 1: received: <hello, 1>
//! worker 0: received: <hello, 0>
//! worker 1: received: <hello, 1>
//! result: Ok(0)
//! result: Ok(1)
//! ```

#![forbid(missing_docs)]

extern crate abomonation;
#[macro_use] extern crate abomonation_derive;
#[cfg(feature = "bincode")]
extern crate bincode;
#[cfg(feature = "getopts")]
extern crate getopts;
#[cfg(feature = "bincode")]
extern crate serde;
extern crate timely_bytes as bytes;
extern crate timely_logging as logging_core;

use std::any::Any;

#[cfg(not(feature = "bincode"))]
use abomonation::Abomonation;
use crossbeam_channel::{Receiver, Sender};
#[cfg(feature = "bincode")]
use serde::{Deserialize, Serialize};

pub use allocator::Allocate;
pub use allocator::direct_relay::DirectRelayAllocate;
pub use allocator::direct_relay::generic::GenericDirectRelay as DirectRelayAllocator;
pub use allocator::Generic as Allocator;
pub use allocator::GenericToRelay as AllocatorWithRelay;
pub use allocator::RelayConnectAllocate;
pub use initialize::{Config as WorkerConfig, initialize, initialize_from, WorkerGuards};
pub use initialize_relay_node::Config as RelayNodeConfig;
pub use initialize_relay_node::initialize_with_input_only as relay_initialize_with_input_only;
pub use initialize_relay_node::initialize_with_input_output as relay_initialize;
pub use initialize_relay_node::initialize_with_output_only as relay_initialize_with_output_only;
pub use initialize_relay_node::WorkerGuards as RelayGuards;
pub use initialize_worker_direct_relay::Config as WorkerDirectRelayConfig;
pub use initialize_worker_direct_relay::initialize as worker_initialize_direct_relay;
pub use initialize_worker_direct_relay::initialize_from as worker_initialize_direct_relay_from;
pub use initialize_worker_with_relay::Config as WorkerWithRelayConfig;
pub use initialize_worker_with_relay::initialize as worker_initialize_with_relay;
pub use initialize_worker_with_relay::initialize_from as worker_initialize_with_relay_from;
pub use message::Message;

pub mod allocator;
pub mod networking;
pub mod initialize;
pub mod logging;
pub mod message;
pub mod buzzer;
pub mod initialize_relay_node;
pub mod initialize_worker_with_relay;
pub mod initialize_worker_direct_relay;

/// A composite trait for types that may be used with channels.
#[cfg(not(feature = "bincode"))]
// 'static bound on trait
// as a trait bound, it means the type does not contain any non-static references.
pub trait Data : Send+Sync+Any+Abomonation+'static { }
#[cfg(not(feature = "bincode"))]
impl<T: Send+Sync+Any+Abomonation+'static> Data for T { }

/// A composite trait for types that may be used with channels.
#[cfg(feature = "bincode")]
pub trait Data : Send+Sync+Any+Serialize+for<'a>Deserialize<'a>+'static { }
#[cfg(feature = "bincode")]
impl<T: Send+Sync+Any+Serialize+for<'a>Deserialize<'a>+'static> Data for T { }

/// Timestamp that the message is sent (in unix epoch, milliseconds)
pub type MessageTimestamp = i64;
/// Network latency that the message is transmitted over the network across pipelines.
pub type MessageLatency = i64;

/// Pushing elements of type `T`.
///
/// This trait moves data around using references rather than ownership,
/// which provides the opportunity for zero-copy operation. In the call
/// to `push(element)` the implementor can *swap* some other value to
/// replace `element`, effectively returning the value to the caller.
///
/// Conventionally, a sequence of calls to `push()` should conclude with
/// a call of `push(&mut None)` or `done()` to signal to implementors that
/// another call to `push()` may not be coming.
pub trait Push<T> {
    /// Pushes `element` with the opportunity to take ownership.
    fn push(&mut self, element: &mut Option<T>);
    /// Pushes `element` and drops any resulting resources.
    #[inline]
    // this anonymous reference &mut Some(element) lives at least as long as the push method
    fn send(&mut self, element: T) { self.push(&mut Some(element)); }
    /// Pushes `None`, conventionally signalling a flush.
    #[inline]
    fn done(&mut self) { self.push(&mut None); }
    /// Send a message, and pass through some latency metric
    fn push_with_latency_passthrough(&mut self, element: &mut Option<T>, latency: Option<MessageLatency>);
    #[inline]
    /// Send a message, and pass through some latency metric
    fn send_with_latency_passthrough(&mut self, element: T, latency: MessageLatency) {
        self.push_with_latency_passthrough(&mut Some(element), Some(latency));
    }
}

// we use P: ?Sized, because P may even be a dyn trait object.
impl<T, P: ?Sized + Push<T>> Push<T> for Box<P> {
    #[inline]
    fn push(&mut self, element: &mut Option<T>) { (**self).push(element) }

    #[inline]
    fn push_with_latency_passthrough(&mut self, element: &mut Option<T>, latency: Option<MessageLatency>) {
        (**self).push_with_latency_passthrough(element, latency);
    }
}

/// Pulling elements of type `T`.
pub trait Pull<T> {
    /// Pulls an element and provides the opportunity to take ownership.
    ///
    /// The puller may mutate the result, in particular take ownership of the data by
    /// replacing it with other data or even `None`. This allows the puller to return
    /// resources to the implementor.
    ///
    /// If `pull` returns `None` this conventionally signals that no more data is available
    /// at the moment, and the puller should find something better to do.
    fn pull(&mut self) -> &mut Option<T>;
    /// Takes an `Option<T>` and leaves `None` behind.
    #[inline]
    fn recv(&mut self) -> Option<T> { self.pull().take() }

    /// Pull an element along with the message transmission latency
    fn pull_with_transmission_latency(&mut self) -> &mut Option<(T, MessageLatency)>;

    #[inline]
    /// Pull an element along with the message transmission latency
    fn recv_with_transmission_latency(&mut self) -> Option<(T, MessageLatency)> {
        let data = self.pull_with_transmission_latency();
        data.take()
    }
}

impl<T, P: ?Sized + Pull<T>> Pull<T> for Box<P> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> { (**self).pull() }
    #[inline]
    fn pull_with_transmission_latency(&mut self) -> &mut Option<(T, MessageLatency)> { (**self).pull_with_transmission_latency() }
}


/// Allocate a matrix of send and receive changes to exchange items.
///
/// This method constructs channels for `sends` threads to create and send
/// items of type `T` to `recvs` receiver threads.
fn promise_futures<T>(sends: usize, recvs: usize) -> (Vec<Vec<Sender<T>>>, Vec<Vec<Receiver<T>>>) {

    // each pair of workers has a sender and a receiver.
    let mut senders: Vec<_> = (0 .. sends).map(|_| Vec::with_capacity(recvs)).collect();
    let mut recvers: Vec<_> = (0 .. recvs).map(|_| Vec::with_capacity(sends)).collect();

    for sender in 0 .. sends {
        for recver in 0 .. recvs {
            let (send, recv) = crossbeam_channel::unbounded();
            // move into the Vec
            senders[sender].push(send);
            recvers[recver].push(recv);
        }
    }

    (senders, recvers)
}