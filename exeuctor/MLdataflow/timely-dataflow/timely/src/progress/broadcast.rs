// READ, Sep 9 2021
//! Broadcasts progress information among workers.

use crate::progress::{ChangeBatch, Timestamp};
use crate::progress::{Location, Port};
use crate::communication::{Message, Push, Pull};
use crate::logging::TimelyLogger as Logger;
use crate::logging::TimelyProgressLogger as ProgressLogger;

/// A list of progress updates corresponding to `((child_scope, [in/out]_port, timestamp), delta)`
pub type ProgressVec<T> = Vec<((Location, T), i64)>;
/// A progress update message consisting of source worker id, sequence number and lists of
/// message and internal updates
// (source worker id, seqno, list of messages (updates))
// use a seqno to ensure order
pub type ProgressMsg<T> = Message<(usize, usize, ProgressVec<T>)>;

/// Manages broadcasting of progress updates to and receiving updates from workers.
pub struct Progcaster<T:Timestamp> {
    to_push: Option<ProgressMsg<T>>,
    // pushers to push progress update messages to other workers
    pushers: Vec<Box<dyn Push<ProgressMsg<T>>>>,
    // puller to receive message from
    puller: Box<dyn Pull<ProgressMsg<T>>>,
    /// Source worker index
    source: usize,
    /// Sequence number counter
    counter: usize,
    /// Sequence of nested scope identifiers indicating the path from the root to this subgraph
    addr: Vec<usize>,
    /// Communication channel identifier
    channel_identifier: usize,

    progress_logging: Option<ProgressLogger>,
}

impl<T:Timestamp+Send> Progcaster<T> {
    /// Creates a new `Progcaster` using a channel from the supplied worker.
    pub fn new<A: crate::worker::AsWorker>(worker: &mut A, path: &Vec<usize>, mut logging: Option<Logger>, progress_logging: Option<ProgressLogger>) -> Progcaster<T> {

        // allocate a new communication channel ID, ID below them this may be used for other channels to communicate other messages
        let channel_identifier = worker.new_identifier();
        //  scheduled in response to the receipt of records on the channel.
        // allocate takes a path that specify a path to an operator that should be
        // scheduled in response to the receipt of records on the channel.
        // Most commonly, this would be the address of the target of the channel
        let (pushers, puller) = worker.allocate(channel_identifier, &path[..]);
        logging.as_mut().map(|l| l.log(crate::logging::CommChannelsEvent {
            identifier: channel_identifier,
            kind: crate::logging::CommChannelKind::Progress,
        }));
        let worker_index = worker.index();
        // the path to the current (sub)-scope
        let addr = path.clone();
        Progcaster {
            to_push: None,
            pushers,
            puller,
            source: worker_index,
            counter: 0,
            addr,
            channel_identifier,
            progress_logging,
        }
    }

    /// Sends pointstamp changes to all workers.
    pub fn send(&mut self, changes: &mut ChangeBatch<(Location, T)>) {

        changes.compact();
        if !changes.is_empty() {

            self.progress_logging.as_ref().map(|l| {

                // Pre-allocate enough space; we transfer ownership, so there is not
                // an opportunity to re-use allocations (w/o changing the logging
                // interface to accept references).
                // messages & internal: Box<Vec<(usize, usize, T, i64)>>
                // messages and internal are just used to log events
                let mut messages = Box::new(Vec::with_capacity(changes.len()));
                let mut internal = Box::new(Vec::with_capacity(changes.len()));

                for ((location, time), diff) in changes.iter() {
                    match location.port {
                        Port::Target(port) => {
                            messages.push((location.node, port, time.clone(), *diff))
                        },
                        Port::Source(port) => {
                            internal.push((location.node, port, time.clone(), *diff))
                        }
                    }
                }

                l.log(crate::logging::TimelyProgressEvent {
                    is_send: true,
                    source: self.source,
                    channel: self.channel_identifier,
                    seq_no: self.counter,
                    addr: self.addr.clone(),
                    messages,
                    internal,
                });
            });

            for pusher in self.pushers.iter_mut() {

                // Attempt to reuse allocations, if possible.
                if let Some(tuple) = &mut self.to_push {
                    let tuple = tuple.as_mut();
                    tuple.0 = self.source;
                    tuple.1 = self.counter;
                    tuple.2.clear(); tuple.2.extend(changes.iter().cloned());
                }
                // If we don't have an allocation ...
                if self.to_push.is_none() {
                    // from_typed consumes the element
                    self.to_push = Some(Message::from_typed((
                        self.source,
                        self.counter,
                        // Vec<((Location, T), i64)>
                        // clone the changes
                        changes.clone().into_inner(),
                    )));
                }

                // TODO: This should probably use a broadcast channel.
                pusher.push(&mut self.to_push);
                pusher.done();
            }

            self.counter += 1;
            changes.clear();
        }
    }

    /// Receives pointstamp changes from all workers.
    // receive pointstamps OC broadcasts from all workers into a ChangeBatch
    pub fn recv(&mut self, changes: &mut ChangeBatch<(Location, T)>) {

        // we get a reference to the ProgressMsg from the puller
        while let Some(message) = self.puller.pull() {

            let source = message.0;
            let counter = message.1;
            let recv_changes = &message.2;

            let addr = &mut self.addr;
            let channel = self.channel_identifier;

            // See comments above about the relatively high cost of this logging, and our
            // options for improving it if performance limits users who want other logging.
            self.progress_logging.as_ref().map(|l| {

                // messages and internal are just used to log the events
                let mut messages = Box::new(Vec::with_capacity(changes.len()));
                let mut internal = Box::new(Vec::with_capacity(changes.len()));

                for ((location, time), diff) in recv_changes.iter() {

                    match location.port {
                        Port::Target(port) => {
                            messages.push((location.node, port, time.clone(), *diff))
                        },
                        Port::Source(port) => {
                            internal.push((location.node, port, time.clone(), *diff))
                        }
                    }
                }

                l.log(crate::logging::TimelyProgressEvent {
                    is_send: false,
                    source: source,
                    seq_no: counter,
                    channel,
                    addr: addr.clone(),
                    messages: messages,
                    internal: internal,
                });
            });

            // We clone rather than drain to avoid deserialization.
            for &(ref update, delta) in recv_changes.iter() {
                changes.update(update.clone(), delta);
            }
        }

    }
}
