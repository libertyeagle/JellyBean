//! Relay nodes communication logging

/// Structures for cross-pipeline communication logging
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct DirectRelayCommunicationSetup {
    /// Is sender to workers in output pipelines?
    /// or receiver of workers in input pipelines?
    pub sender: bool,
    /// The current running worker process index (serving current pipeline)
    pub local_worker_process: usize,
    /// Remote worker process index
    pub remote_worker_process: usize,
    /// Remote pipeline index
    pub remote_pipeline_index: usize,
}

/// Various communication events.
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum DirectRelayCommunicationEvent {
    /// An observed message.
    Message(DirectRelayMessageEvent),
    /// A state transition.
    State(DirectRelayStateEvent),
}

/// An observed message from relay-relay communication.
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct DirectRelayMessageEvent {
    /// Is the message received from input pipelines,
    /// or sent to output pipelines?
    pub is_send: bool,
    /// associated message header.
    pub header: crate::networking::MessageHeader,
}

/// Starting or stopping relay-relay communication threads.
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct DirectRelayStateEvent {
    /// Is the thread receiving from input pipelines
    /// or sending to the output pipelines?
    pub send: bool,
    /// Input/output pipeline index
    pub pipeline_index: usize,
    /// Current worker process index
    pub local: usize,
    /// Remote worker process index in the input/output pipeline
    pub remote: usize,
    /// Is the thread starting or stopping.
    pub start: bool,
}

impl From<DirectRelayMessageEvent> for DirectRelayCommunicationEvent {
    fn from(v: DirectRelayMessageEvent) -> DirectRelayCommunicationEvent { DirectRelayCommunicationEvent::Message(v) }
}
impl From<DirectRelayStateEvent> for DirectRelayCommunicationEvent {
    fn from(v: DirectRelayStateEvent) -> DirectRelayCommunicationEvent { DirectRelayCommunicationEvent::State(v) }
}
