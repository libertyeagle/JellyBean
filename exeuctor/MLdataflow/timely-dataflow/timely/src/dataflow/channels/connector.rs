//! Define a connector to connect a puller and a pusher

use std::marker::PhantomData;
use timely_communication::MessageLatency;
use crate::communication::{Pull, Push};
use crate::dataflow::channels::Bundle;
use crate::Data;
use crate::progress::Timestamp;

use crate::logging::TimelyLogger as Logger;

/// Trait for a connector of a pusher and puller
/// The connector pulls data from the puller
/// and pushes (forwards) the data to the pusher
pub trait Connector {
    /// Relay messages by forwarding the data pulled from the puller
    fn relay_messages(&mut self);
}

/// Connector for Bundle<T, D>
pub struct PushPullConnector<T, D, P, Q>
where
    P: Push<Bundle<T, D>>,
    Q: Pull<Bundle<T, D>>
{
    pusher: P,
    puller: Q,
    phantom: PhantomData<(T, D)>
}

impl<T, D, P, Q> Connector for PushPullConnector<T, D, P, Q>
where
    P: Push<Bundle<T, D>>,
    Q: Pull<Bundle<T, D>>,
    T: Timestamp,
    D: Data,
{
    fn relay_messages(&mut self) {
        while let Some((mut message, pipeline_latency)) = self.puller.recv_with_transmission_latency() {
            let content = message.as_mut();
            content.pipeline_latency = Some(pipeline_latency);
            self.pusher.send(message);
        }
        self.pusher.done();
    }
}

impl<T, D, P, Q> PushPullConnector<T, D, P, Q>
where
    P: Push<Bundle<T, D>>,
    Q: Pull<Bundle<T, D>>
{
    pub fn new(pusher: P, puller: Q) -> Self {
        PushPullConnector {
            pusher,
            puller,
            phantom: PhantomData
        }
    }
}

/// Scope output log pushers
pub struct RelayLogPusher<T, D, P: Push<Bundle<T, D>>> {
    pusher: P,
    // Registered scope output index (in the current pipeline)
    output_index: usize,
    // Source (current) timely worker index
    source_worker: usize,
    // Target relay node index
    target_relay: usize,
    phantom: PhantomData<(T, D)>,
    logging: Option<Logger>,
}

impl<T, D, P: Push<Bundle<T, D>>> RelayLogPusher<T, D, P> {
    /// Allocates a new pusher.
    pub fn new(pusher: P, output_index: usize, worker_index: usize, relay_index: usize,logging: Option<Logger>) -> Self {
        RelayLogPusher {
            pusher,
            output_index,
            source_worker: worker_index,
            target_relay: relay_index,
            phantom: PhantomData,
            logging,
        }
    }
}
impl<T, D, P: Push<Bundle<T, D>>> Push<Bundle<T, D>> for RelayLogPusher<T, D, P> {
    #[inline]
    fn push(&mut self, pair: &mut Option<Bundle<T, D>>) {
        if let Some(bundle) = pair {
            if let Some(message) = bundle.if_mut() {
                message.seq = 0;
                message.from = 0;
            }

            self.logging.as_ref().map(|l| l.log(crate::logging::RelayMessageEvent {
                is_send: true,
                index: self.output_index,
                source: self.source_worker,
                target: self.target_relay,
                length: bundle.data.len()
            }));
        }
        self.pusher.push(pair);
    }

    fn push_with_latency_passthrough(&mut self, element: &mut Option<Bundle<T, D>>, _latency: Option<MessageLatency>) {
        self.push(element);
    }
}


pub struct RelayLogPuller<T, D, P: Pull<Bundle<T, D>>> {
    puller: P,
    // Index of scope input (in the current pipeline)
    input_index: usize,
    // Target (current) timely worker index
    worker_index: usize,
    // this type acts as though it holds (T, D)
    phantom: PhantomData<(T, D)>,
    logging: Option<Logger>,
}
impl<T, D, P: Pull<Bundle<T, D>>> RelayLogPuller<T, D, P> {
    /// Allocates a new `Puller`.
    pub fn new(puller: P, input_index: usize, worker_index: usize, logging: Option<Logger>) -> Self {
        RelayLogPuller {
            puller,
            input_index,
            worker_index,
            phantom: PhantomData,
            logging,
        }
    }
}

impl<T, D, P: Pull<Bundle<T, D>>> Pull<Bundle<T, D>> for RelayLogPuller<T, D, P> {
    #[inline]
    fn pull(&mut self) -> &mut Option<Bundle<T,D>> {
        let result = self.puller.pull();
        let input_index = self.input_index;
        let worker_index = self.worker_index;
        if let Some(bundle) = result {
            self.logging.as_ref().map(|l| l.log(crate::logging::RelayMessageEvent {
                is_send: false,
                index: input_index,
                source: 0,
                target: worker_index,
                length: bundle.data.len()
            }));
        }
        result
    }

    fn pull_with_transmission_latency(&mut self) -> &mut Option<(Bundle<T, D>, MessageLatency)> {
        let result = self.puller.pull_with_transmission_latency();
        let input_index = self.input_index;
        let worker_index = self.worker_index;
        if let Some(bundle) = result {
            self.logging.as_ref().map(|l| l.log(crate::logging::RelayMessageEvent {
                is_send: false,
                index: input_index,
                source: 0,
                target: worker_index,
                length: bundle.0.data.len()
            }));
        }
        result
    }
}