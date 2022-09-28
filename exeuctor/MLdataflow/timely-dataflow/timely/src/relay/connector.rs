//! Data and frontier connector

use std::collections::{HashMap, HashSet};
use crate::communication::{Pull, Push, Message};
use crate::dataflow::channels::{Bundle};
use crate::progress::frontier_relay::SingleChannelFrontierUpdateMsg;
use crate::progress::Timestamp;

/// Trait to connect pull/push of data
pub(super) trait DataConnect {
    /// Relay received data
    fn relay_data(&mut self);
}

/// Trait to connect pull/push of frontier changes
pub(super) trait FrontierConnect {
    /// Relay received frontier
    fn relay_frontier(&mut self);
}

/// Data connector
pub(super) struct DataConnector<T: Timestamp+Send, D, P, H>
where
    P: Pull<Bundle<T, D>>,
    H: FnMut(&T, &Vec<D>) -> usize,
{
    pub(super) pushers: Vec<Box<dyn Push<Bundle<T, D>>>>,
    pub(super) puller: P,
    pub(super) hash_fn: H
}

impl<T: Timestamp+Send, D, P, H> DataConnect for DataConnector<T, D, P, H>
where
    P: Pull<Bundle<T, D>>,
    H: FnMut(&T, &Vec<D>) -> usize,
{
    /// Receive data from puller, use hash function to decide which pusher to push
    fn relay_data(&mut self) {
        // map each element in the Vec<D> to the worker according to the hash function
        let num_pushers = self.pushers.len();
        while let Some((element, latency)) = self.puller.recv_with_transmission_latency() {
            let pusher = &mut self.pushers[(self.hash_fn)(&element.time, &element.data) % num_pushers];
            pusher.send_with_latency_passthrough(element, latency);
        }
        for pusher in self.pushers.iter_mut() { pusher.done() }
    }
}

/// Input pipeline frontier connector
/// Receive input frontiers and broadcast to timely workers
pub(super) struct InputFrontierConnector<T: Timestamp+Send, P>
where
    P: Pull<SingleChannelFrontierUpdateMsg<T>>,
{
    pub(super) pushers: Vec<Box<dyn Push<SingleChannelFrontierUpdateMsg<T>>>>,
    pub(super) input_index_mapping: HashMap<usize, usize>,
    pub(super) puller: P,
}

impl<T: Timestamp+Send, P> FrontierConnect for InputFrontierConnector<T, P>
where
    P: Pull<SingleChannelFrontierUpdateMsg<T>>,
{
    fn relay_frontier(&mut self) {
        while let Some((frontier_changes, latency)) = self.puller.recv_with_transmission_latency() {
            let changes = frontier_changes.into_typed();
            let frontier_updates_vec = changes.1.into_iter()
                .filter(|x| self.input_index_mapping.contains_key(&x.0.0))
                .map(|x| {
                let input_index = self.input_index_mapping.get(&x.0.0).unwrap().to_owned();
                ((input_index, x.0.1), x.1)
            }).collect::<Vec<_>>();

            // push frontier updates to every timely worker
            let mut message = None;
            for pusher in self.pushers.iter_mut() {
                if message.is_none() {
                    message = Some(Message::from_typed((
                        changes.0,
                        frontier_updates_vec.clone()
                    )));
                }
                pusher.push_with_latency_passthrough(&mut message, Some(latency));
            }

        }
        for pusher in self.pushers.iter_mut() { pusher.done() }
    }
}

/// Connector that receives frontier updates from timely workers
/// and push the frontier to remote relay nodes
pub(super) struct OutputFrontierConnector<T: Timestamp+Send, P, H>
where
    P: Pull<SingleChannelFrontierUpdateMsg<T>>,
    H: Fn() -> usize,
{
    pub(super) pushers: Vec<Box<dyn Push<SingleChannelFrontierUpdateMsg<T>>>>,
    pub(super) required_indices: HashSet<usize>,
    pub(super) puller: P,
    pub(super) mapper_fn: H
}

impl<T: Timestamp+Send, P, H> FrontierConnect for OutputFrontierConnector<T, P, H>
where
    P: Pull<SingleChannelFrontierUpdateMsg<T>>,
    H: Fn() -> usize,
{
    fn relay_frontier(&mut self) {
        while let Some(frontier_changes) = self.puller.recv() {
            let changes = frontier_changes.into_typed();
            let frontier_updates_vec = changes.1.into_iter()
                .filter(|x| self.required_indices.contains(&x.0.0))
                .collect::<Vec<_>>();

            let message = Message::from_typed((
                changes.0,
                frontier_updates_vec
            ));

            let pusher_index = (self.mapper_fn)() % self.pushers.len();
            let pusher = &mut self.pushers[pusher_index];
            pusher.send(message);
        }
        for pusher in self.pushers.iter_mut() { pusher.done() }
    }
}


