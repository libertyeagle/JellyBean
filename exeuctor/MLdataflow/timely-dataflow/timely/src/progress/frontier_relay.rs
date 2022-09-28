//! Frontier relay

use std::collections::HashMap;

use rand::distributions::{Distribution, Uniform};
use rand::rngs::ThreadRng;

use crate::communication::{Message, Pull, Push};
use crate::logging::TimelyRelayProgressLogger as ProgressLogger;
use crate::progress::{ChangeBatch, Target, Timestamp};

pub type FrontierUpdateVec<T> = Vec<(T, i64)>;
pub type FrontierUpdateMsg<T> = Message<(usize, FrontierUpdateVec<T>)>;


pub trait FrontierRelay<T: Timestamp> {
    fn send(&mut self, changes: &mut ChangeBatch<(Target, T)>);
    fn recv(&mut self, changes: &mut ChangeBatch<(usize, T)>, started: &mut Vec<usize>) -> bool;
}

/// Frontier relay
/// We allow to acquire an input from multiple input pipelines
pub struct MultiChannelFrontierRelay<T: Timestamp, H>
where
    H: FnMut(usize, usize) -> usize
{
    // Message structs to stage communication message
    messages_to_push: Vec<Option<FrontierUpdateMsg<T>>>,
    pushers: Vec<Vec<Box<dyn Push<FrontierUpdateMsg<T>>>>>,
    pullers: Vec<Box<dyn Pull<FrontierUpdateMsg<T>>>>,
    init_pullers: Vec<Box<dyn Pull<Message<i32>>>>,
    // scope inputs index -> input port id in subgraph mapping
    input_index_id_map: HashMap<usize, usize>,
    // scope output id in subgraph -> output index mapping
    output_id_index_map: HashMap<usize, usize>,
    // counters for #frontier updates message, one counter for each scope output
    counters: Vec<usize>,
    // one u32 counter for each scope input
    // indicate whether we have received the first frontier message
    // from all input pipelines that contribute?
    // drop to 0, all init frontiers of this input received
    frontier_received: Vec<i32>,
    // remaining #scope inputs pending
    remaining_to_recv: usize,
    // mapping function
    mapper_fn: H,
    worker_index: usize,
    progress_logging: Option<ProgressLogger>,
}

impl<T: Timestamp+Send, H> MultiChannelFrontierRelay<T, H>
where
    H: FnMut(usize, usize) -> usize
{
    /// Build a new frontier relay node
    /// given the input index to port id mapping
    /// and output port id to index mapping
    /// mapper_fn takes the input index and the counter of #frontier updates message
    /// and return the relay node index to send to
    pub fn new<A: crate::worker::RelayConnector>(
        worker: &mut A,
        input_index_id_map: HashMap<usize, usize>,
        output_id_index_map: HashMap<usize, usize>,
        mapper_fn: H,
        progress_logging: Option<ProgressLogger>
    ) -> Self
    {
        let num_scope_inputs = input_index_id_map.len();
        let num_scope_outputs = output_id_index_map.len();
        let mut pullers = Vec::with_capacity(num_scope_inputs);
        let mut pushers = Vec::with_capacity(num_scope_outputs);
        let mut init_pullers = Vec::with_capacity(num_scope_inputs);
        for index in 0..num_scope_inputs {
            let (_channel_pushers, puller) = worker.allocate_relay_channel(2 * index);
            std::mem::drop(_channel_pushers);
            pullers.push(puller);
            let (_channel_pushers, puller) = worker.allocate_relay_channel::<i32>(2 * (num_scope_inputs + num_scope_outputs) + index);
            std::mem::drop(_channel_pushers);
            init_pullers.push(puller);
        }

        let mut frontier_received = Vec::with_capacity(num_scope_inputs);
        for init_puller in init_pullers.iter_mut() {
            loop {
                worker.receive_from_relay();
                if let Some(num_recv_input_pipelines) = init_puller.pull() {
                    frontier_received.push(*num_recv_input_pipelines.as_ref_or_mut());
                    break;
                }
            }
        }
        for index in 0..num_scope_outputs {
            let (channel_pushers, _puller) = worker.allocate_relay_channel(2 * (num_scope_inputs + index));
            std::mem::drop(_puller);
            pushers.push(channel_pushers);
        }

        let messages_to_push = (0..num_scope_outputs).map(|_| None).collect();
        let counters = vec![0; num_scope_outputs];

        MultiChannelFrontierRelay {
            messages_to_push,
            pushers,
            pullers,
            init_pullers,
            input_index_id_map,
            output_id_index_map,
            counters,
            frontier_received,
            remaining_to_recv: num_scope_inputs,
            mapper_fn,
            worker_index: worker.index(),
            progress_logging,
        }
    }
}
impl<T: Timestamp+Send, H> FrontierRelay<T> for MultiChannelFrontierRelay<T, H>
where
    H: FnMut(usize, usize) -> usize
{
    /// Send the frontier updates at the scope outputs to relay node
    fn send(&mut self, changes: &mut ChangeBatch<(Target, T)>) {
        changes.compact();
        if !changes.is_empty() {
            self.progress_logging.as_ref().map(|l| {
                let mut messages = Box::new(Vec::with_capacity(changes.len()));
                for ((location, time), diff) in changes.iter() {
                    messages.push((0, location.port, time.clone(), *diff));
                }
                l.log(crate::logging::TimelyRelayProgressEvent {
                    is_send: true,
                    source: self.worker_index,
                    messages
                });
            });
            let mut update_vecs = Vec::with_capacity(self.messages_to_push.len());
            for (idx, to_push) in self.messages_to_push.iter_mut().enumerate() {
                match to_push {
                    Some(msg) => {
                        let (counter, updates) = msg.as_mut();
                        *counter = self.counters[idx];
                        updates.clear();
                        update_vecs.push(updates);
                    },
                    None => {
                        let msg = to_push.insert(Message::from_typed(
                            (self.counters[idx], Vec::new())
                        ));
                        let (_, updates) = msg.as_mut();
                        update_vecs.push(updates);
                    }
                };
            }

            for ((location, time), delta) in changes.drain() {
                let port = self.output_id_index_map.get(&location.port).unwrap().to_owned();
                update_vecs[port].push((time, delta));
            }

            std::mem::drop(update_vecs);
            for (idx, msg) in self.messages_to_push.iter_mut().enumerate() {
                if !(*msg.as_mut().unwrap()).1.is_empty() {
                    let relay_to_send = (self.mapper_fn)(idx, self.counters[idx]);
                    let mask = self.pushers[idx].len() - 1;
                    let pusher = &mut self.pushers[idx][relay_to_send & mask];
                    pusher.push(msg);
                    pusher.done();
                    self.counters[idx] += 1;
                }
            }
        }
    }

    /// Receive the frontier changes of the scope inputs from the relay node
    fn recv(&mut self, changes: &mut ChangeBatch<(usize, T)>, started: &mut Vec<usize>) -> bool {
        let mut messages = Box::new(Vec::with_capacity(changes.len()));
        for (idx, puller) in self.pullers.iter_mut().enumerate() {
            let port = self.input_index_id_map.get(&idx).unwrap().to_owned();
            while let Some(message) = puller.pull() {
                let recv_changes = &message.1;
                for &(ref time, delta) in recv_changes.iter() {
                    changes.update((port, time.clone()), delta);
                    messages.push((0, port, time.clone(), delta));
                }
            }
        }

        for (idx, init_puller) in self.init_pullers.iter_mut().enumerate() {
            let port = self.input_index_id_map.get(&idx).unwrap().to_owned();
            while let Some(init_signal) = init_puller.pull() {
                if *init_signal.as_ref_or_mut() < 0 {
                    self.frontier_received[idx] -= 1;
                    if self.frontier_received[idx] == 0 {
                        self.remaining_to_recv -= 1;
                        started.push(port);
                    }
                }
            }
        }

        self.progress_logging.as_ref().map(|l| {
            l.log(crate::logging::TimelyRelayProgressEvent {
                is_send: false,
                // source 0, the messages come from relay nodes
                source: 0,
                messages,
            });
        });
        self.remaining_to_recv == 0
    }
}


pub type SingleChannelFrontierUpdateVec<T> = Vec<((usize, T), i64)>;
pub type SingleChannelFrontierUpdateMsg<T> = Message<(usize, SingleChannelFrontierUpdateVec<T>)>;

/// Single channel frontier relay
/// Use a single channel (channel ID 0)
/// to send and receive frontier updates
/// It does not allocate a separate channel for each input / output
/// The relay node must know the Timestamp's concrete type
/// and handle the mapping from the output index in the input pipeline
/// to the input index (not port id) in the current pipeline
/// We disallow to acquire an input from multiple input pipelines
pub struct SingleChannelFrontierRelay<T: Timestamp>
{
    to_push: Option<SingleChannelFrontierUpdateMsg<T>>,
    pushers: Vec<Box<dyn Push<SingleChannelFrontierUpdateMsg<T>>>>,
    puller:  Box<dyn Pull<SingleChannelFrontierUpdateMsg<T>>>,
    input_index_id_map: HashMap<usize, usize>,
    output_id_index_map: HashMap<usize, usize>,
    counter: usize,
    frontier_received: Vec<bool>,
    remaining_to_recv: usize,
    worker_index: usize,
    rng: ThreadRng,
    progress_logging: Option<ProgressLogger>
}


impl<T: Timestamp+Send> SingleChannelFrontierRelay<T>
{
    /// Build a new single channel frontier relay
    pub fn new<A: crate::worker::RelayConnector>(
        worker: &mut A,
        input_index_id_map: HashMap<usize, usize>,
        output_id_index_map: HashMap<usize, usize>,
        progress_logging: Option<ProgressLogger>
    ) -> Self
    {
        let num_scope_inputs = input_index_id_map.len();
        let (pushers, puller) = worker.allocate_relay_channel(0);

        let frontier_received = vec![false; num_scope_inputs];
        SingleChannelFrontierRelay {
            to_push: None,
            pushers,
            puller,
            input_index_id_map,
            output_id_index_map,
            counter: 0,
            frontier_received,
            remaining_to_recv: num_scope_inputs,
            rng: rand::thread_rng(),
            worker_index: worker.index(),
            progress_logging
        }
    }
}

impl<T: Timestamp+Send> FrontierRelay<T> for SingleChannelFrontierRelay<T> {
    /// Send the frontier updates at the scope outputs to relay node
    fn send(&mut self, changes: &mut ChangeBatch<(Target, T)>) {
        changes.compact();

        if !changes.is_empty() {
            self.progress_logging.as_ref().map(|l| {
                let mut messages = Box::new(Vec::with_capacity(changes.len()));
                for ((location, time), diff) in changes.iter() {
                    messages.push((0, location.port, time.clone(), *diff));
                }
                l.log(crate::logging::TimelyRelayProgressEvent {
                    is_send: true,
                    source: self.worker_index,
                    messages
                });
            });

            let uniform = Uniform::new(0, self.pushers.len());
            let relay_to_send = uniform.sample(&mut self.rng);
            let output_id_index_map = &self.output_id_index_map;
            if let Some(tuple) = &mut self.to_push {
                let tuple = tuple.as_mut();
                tuple.0 = self.counter;
                tuple.1.clear();
                tuple.1.extend(changes.drain().map(|x| {
                    let port = x.0.0.port;
                    let output_index = output_id_index_map.get(&port).unwrap().to_owned();
                    ((output_index, x.0.1), x.1)
                }));
            }

            if self.to_push.is_none() {
                self.to_push = Some(Message::from_typed((
                    self.counter,
                    changes.drain().map(|x| {
                        let port = x.0.0.port;
                        let output_index = output_id_index_map.get(&port).unwrap().to_owned();
                        ((output_index, x.0.1), x.1)
                    }).collect::<Vec<_>>()
                )));
            }

            let pusher = &mut self.pushers[relay_to_send];
            pusher.push(&mut self.to_push);
            pusher.done();
        }

        self.counter += 1;
    }

    /// Receive the frontier changes of the scope inputs from the relay node
    fn recv(&mut self, changes: &mut ChangeBatch<(usize, T)>, started: &mut Vec<usize>) -> bool {
        let mut messages = Box::new(Vec::with_capacity(changes.len()));
        while let Some(message) = self.puller.pull() {
            let recv_changes = &message.1;
            for &(ref update, delta) in recv_changes.iter() {
                let port_id = self.input_index_id_map.get(&update.0).unwrap().to_owned();
                changes.update((port_id, update.1.clone()), delta);
                messages.push((0, port_id, update.1.clone(), delta));
                if !self.frontier_received[port_id] {
                    self.frontier_received[port_id] = true;
                    self.remaining_to_recv -= 1;
                    started.push(port_id);
                }
            }
        }
        self.progress_logging.as_ref().map(|l| {
            l.log(crate::logging::TimelyRelayProgressEvent {
                is_send: false,
                source: 0,
                messages
            });
        });

        self.remaining_to_recv == 0

    }
}
