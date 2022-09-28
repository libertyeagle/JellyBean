// READ, Sep 12 2021
//! Parallelization contracts, describing requirements for data movement along dataflow edges.
//!
//! Pacts describe how data should be exchanged between workers, and implement a method which
//! creates a pair of `Push` and `Pull` implementors from an `A: AsWorker`. These two endpoints
//! respectively distribute and collect data among workers according to the pact.
//!
//! The only requirement of a pact is that it not alter the number of `D` records at each time `T`.
//! The progress tracking logic assumes that this number is independent of the pact used.

use std::marker::PhantomData;
use timely_communication::MessageLatency;

use crate::communication::{Push, Pull, Data};
use crate::communication::allocator::thread::{ThreadPusher, ThreadPuller};

use crate::worker::AsWorker;
use crate::dataflow::channels::pushers::Exchange as ExchangePusher;
use super::{Bundle, Message};

use crate::logging::TimelyLogger as Logger;

/// A `ParallelizationContract` allocates paired `Push` and `Pull` implementors.
pub trait ParallelizationContract<T: 'static, D: 'static> {
    /// Type implementing `Push` produced by this pact.
    // it just defines the pusher type for convenience
    type Pusher: Push<Bundle<T, D>>+'static;
    /// Type implementing `Pull` produced by this pact.
    type Puller: Pull<Bundle<T, D>>+'static;
    /// Allocates a matched pair of push and pull endpoints implementing the pact.
    fn connect<A: AsWorker>(self, allocator: &mut A, identifier: usize, address: &[usize], logging: Option<Logger>) -> (Self::Pusher, Self::Puller);
}

/// A direct connection
pub struct Pipeline;
impl<T: 'static, D: 'static> ParallelizationContract<T, D> for Pipeline {
    type Pusher = LogPusher<T, D, ThreadPusher<Bundle<T, D>>>;
    type Puller = LogPuller<T, D, ThreadPuller<Bundle<T, D>>>;
    fn connect<A: AsWorker>(self, allocator: &mut A, identifier: usize, address: &[usize], logging: Option<Logger>) -> (Self::Pusher, Self::Puller) {
        // allocate thread-local channel for the pipeline
        // the address specifies a path to an operator that should be
        // scheduled in response to the receipt of records on the channel.
        // identifier is the ID of the channel
        let (pusher, puller) = allocator.pipeline::<Message<T, D>>(identifier, address);
        // thread local channel, so the source and target are all allocator.index()
        (LogPusher::new(pusher, allocator.index(), allocator.index(), identifier, logging.clone()),
         LogPuller::new(puller, allocator.index(), identifier, logging.clone()))
    }
}

/// An exchange between multiple observers by data
// this structure stores the hash function to map data to workers
// the hash function takes the record and returns an integer
pub struct Exchange<D, F: FnMut(&D)->u64+'static> { hash_func: F, phantom: PhantomData<D>, }
impl<D, F: FnMut(&D)->u64> Exchange<D, F> {
    /// Allocates a new `Exchange` pact from a distribution function.
    pub fn new(func: F) -> Exchange<D, F> {
        Exchange {
            hash_func:  func,
            phantom:    PhantomData,
        }
    }
}

// Exchange uses a `Box<Pushable>` because it cannot know what type of pushable will return from the allocator.
impl<T: Eq+Data+Clone, D: Data+Clone, F: FnMut(&D)->u64+'static> ParallelizationContract<T, D> for Exchange<D, F> {
    // TODO: The closure in the type prevents us from naming it.
    //       Could specialize `ExchangePusher` to a time-free version.
    type Pusher = Box<dyn Push<Bundle<T, D>>>;
    type Puller = Box<dyn Pull<Bundle<T, D>>>;
    fn connect<A: AsWorker>(mut self, allocator: &mut A, identifier: usize, address: &[usize], logging: Option<Logger>) -> (Self::Pusher, Self::Puller) {
        // senders is a vector of length #workers, it contains a pusher to each worker
        let (senders, receiver) = allocator.allocate::<Message<T, D>>(identifier, address);
        // wraps each sender
        // into_iter() consumes the vector (container)
        let senders = senders.into_iter().enumerate().map(|(i,x)| LogPusher::new(x, allocator.index(), i, identifier, logging.clone())).collect::<Vec<_>>();
        // use ExchangePusher to wrap the senders with the hash function to dynamically distribute the data among the workers
        (Box::new(ExchangePusher::new(senders, move |_, d| (self.hash_func)(d))), Box::new(LogPuller::new(receiver, allocator.index(), identifier, logging.clone())))
    }
}

/// Wraps a `Message<T,D>` pusher to provide a `Push<(T, Content<D>)>`.
pub struct LogPusher<T, D, P: Push<Bundle<T, D>>> {
    pusher: P,
    channel: usize,
    // seqno
    counter: usize,
    // source worker
    source: usize,
    // target worker
    target: usize,
    phantom: ::std::marker::PhantomData<(T, D)>,
    logging: Option<Logger>,
}
impl<T, D, P: Push<Bundle<T, D>>> LogPusher<T, D, P> {
    /// Allocates a new pusher.
    pub fn new(pusher: P, source: usize, target: usize, channel: usize, logging: Option<Logger>) -> Self {
        LogPusher {
            pusher,
            channel,
            counter: 0,
            source,
            target,
            phantom: ::std::marker::PhantomData,
            logging,
        }
    }
}

impl<T, D, P: Push<Bundle<T, D>>> Push<Bundle<T, D>> for LogPusher<T, D, P> {
    #[inline]
    fn push(&mut self, pair: &mut Option<Bundle<T, D>>) {
        if let Some(bundle) = pair {
            self.counter += 1;
            // Stamp the sequence number and source.
            // FIXME: Awkward moment/logic.
            // correct seqno and source worker
            // because this information is not included when we push message via Message::push_at()
            if let Some(message) = bundle.if_mut() {
                message.seq = self.counter-1;
                message.from = self.source;
            }

            // log message
            self.logging.as_ref().map(|l| l.log(crate::logging::MessagesEvent {
                is_send: true,
                channel: self.channel,
                source: self.source,
                target: self.target,
                seq_no: self.counter-1,
                length: bundle.data.len(),
            }));
        }
        self.pusher.push(pair);
    }

    fn push_with_latency_passthrough(&mut self, element: &mut Option<Bundle<T, D>>, _latency: Option<MessageLatency>) {
        self.push(element);
    }
}

/// Wraps a `Message<T,D>` puller to provide a `Pull<(T, Content<D>)>`.
// which just generates a log each time it pulls data
pub struct LogPuller<T, D, P: Pull<Bundle<T, D>>> {
    puller: P,
    channel: usize,
    index: usize,
    // this type acts as though it holds (T, D)
    phantom: ::std::marker::PhantomData<(T, D)>,
    logging: Option<Logger>,
}
impl<T, D, P: Pull<Bundle<T, D>>> LogPuller<T, D, P> {
    /// Allocates a new `Puller`.
    pub fn new(puller: P, index: usize, channel: usize, logging: Option<Logger>) -> Self {
        LogPuller {
            puller,
            channel,
            index,
            phantom: ::std::marker::PhantomData,
            logging,
        }
    }
}

impl<T, D, P: Pull<Bundle<T, D>>> Pull<Bundle<T, D>> for LogPuller<T, D, P> {
    #[inline]
    fn pull(&mut self) -> &mut Option<Bundle<T,D>> {
        let result = self.puller.pull();
        if let Some(bundle) = result {
            // channel idx
            let channel = self.channel;
            // the current receiving worker id
            let target = self.index;
            self.logging.as_ref().map(|l| l.log(crate::logging::MessagesEvent {
                is_send: false,
                channel,
                source: bundle.from,
                target,
                seq_no: bundle.seq,
                length: bundle.data.len(),
            }));
        }
        result
    }

    fn pull_with_transmission_latency(&mut self) -> &mut Option<(Bundle<T, D>, MessageLatency)> {
        let result = self.puller.pull_with_transmission_latency();
        if let Some((bundle, _lat)) = result {
            // channel idx
            let channel = self.channel;
            // the current receiving worker id
            let target = self.index;
            self.logging.as_ref().map(|l| l.log(crate::logging::MessagesEvent {
                is_send: false,
                channel,
                source: bundle.from,
                target,
                seq_no: bundle.seq,
                length: bundle.data.len(),
            }));
        }
        result
    }
}
