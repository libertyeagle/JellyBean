//! Relay nodes communication logging

use crate::allocator::relay::header::{RelayToRelayMessageHeader, RelayToTimelyMessageHeader};
use crate::networking::MessageHeader;

/// Structures for relay-relay node communication logging
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct RelayCommunicationSetup {
    /// Is sender to relay nodes in output pipelines?
    /// or receiver of relay nodes in input pipelines?
    pub sender: bool,
    /// The current running relay node process index (serving current pipeline)
    pub local_relay_node_idx: usize,
    /// Remote (input/output) pipeline index
    pub remote_pipeline_index: usize,
    /// The relative index of remote relay node in the input/output pipeline
    pub remote_relay_node_idx: usize,
}

/// Various communication relay-relay events.
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum RelayCommunicationEvent {
    /// An observed message.
    Message(RelayMessageEvent),
    /// A state transition.
    State(RelayStateEvent),
}

/// An observed message from relay-relay communication.
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct RelayMessageEvent {
    /// Is the message received from input pipelines,
    /// or sent to output pipelines?
    pub is_send: bool,
    /// associated message header.
    pub header: RelayToRelayMessageHeader,
}

/// Starting or stopping relay-relay communication threads.
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct RelayStateEvent {
    /// Is the thread receiving from input pipelines
    /// or sending to the output pipelines?
    pub send: bool,
    /// input/output pipeline index
    pub pipeline_index: usize,
    /// current relay node (process) index
    pub local_relay_node_index: usize,
    /// Is the thread starting or stopping.
    pub start: bool,
}

/// Communication setup between relay nodes and timely workers,
/// the struct works for both timely workers and relay nodes
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct RelayTimelyCommunicationSetup {
    /// Send or receive?
    pub sender: bool,
    /// Relay node (process) id
    pub relay_node_index: usize,
    /// timely worker process id
    pub timely_worker_process_index: usize,
}


/// Various relay-timely communication events.
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum RelayTimelyCommunicationEvent {
    /// An observed message.
    Message(RelayTimelyMessageEvent),
    /// A state transition.
    State(RelayTimelyStateEvent),
}

/// Two types of relay <-> timely  worker communication message's headers
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum RelayTimelyCommMessageHeader {
    /// Timely -> relay communication
    TimelySend(MessageHeader),
    /// Relay -> timely communication
    TimelyRecv(RelayToTimelyMessageHeader)
}

/// An observed message of relay-timely communication.
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct RelayTimelyMessageEvent {
    /// Is the message received from input pipelines,
    /// or sent to output pipelines?
    pub is_send: bool,
    /// associated message header.
    pub header: RelayTimelyCommMessageHeader,
}

/// Starting or stopping relay-timely communication threads.
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct RelayTimelyStateEvent {
    /// Is the thread a send (vs a recv) thread.
    pub send: bool,
    /// relay node index
    pub relay_node_index: usize,
    /// timely worker process index
    pub timely_worker_process_index: usize,
    /// Is the thread starting or stopping.
    pub start: bool,
}



impl From<RelayMessageEvent> for RelayCommunicationEvent {
    fn from(v: RelayMessageEvent) -> RelayCommunicationEvent { RelayCommunicationEvent::Message(v) }
}
impl From<RelayStateEvent> for RelayCommunicationEvent {
    fn from(v: RelayStateEvent) -> RelayCommunicationEvent { RelayCommunicationEvent::State(v) }
}

impl From<RelayTimelyMessageEvent> for RelayTimelyCommunicationEvent {
    fn from(v: RelayTimelyMessageEvent) -> RelayTimelyCommunicationEvent { RelayTimelyCommunicationEvent::Message(v) }
}
impl From<RelayTimelyStateEvent> for RelayTimelyCommunicationEvent {
    fn from(v: RelayTimelyStateEvent) -> RelayTimelyCommunicationEvent { RelayTimelyCommunicationEvent::State(v) }
}
