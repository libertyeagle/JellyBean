// READ, Sep 12 2021
//! Structured communication between timely dataflow operators.

use once_cell::sync::OnceCell;

use crate::communication::MessageLatency;
use crate::communication::Push;

/// A collection of types that may be pushed at.
pub mod pushers;
/// A collection of types that may be pulled from.
pub mod pullers;
/// Parallelization contracts, describing how data must be exchanged between operators.
pub mod pact;
pub mod connector;

/// The input to and output from timely dataflow communication channels.
pub type Bundle<T, D> = crate::communication::Message<Message<T, D>>;

pub static MESSAGE_BUFFER_SIZE: OnceCell<usize> = OnceCell::new();

/// A serializable representation of timestamped data.
#[derive(Clone, Abomonation, Serialize, Deserialize)]
// for Data type that can be used in the timely_communication crate
// pub trait Data : Send+Sync+Any+Abomonation+'static { }
// Send, Sync: these traits are automatically implemented when the compiler determines it's appropriate.
// T: type of timestamp
// D: type of data
pub struct Message<T, D> {
    /// The timestamp associated with the message.
    pub time: T,
    /// The data in the message.
    pub data: Vec<D>,
    /// The source worker.
    pub from: usize,
    /// A sequence number for this worker-to-worker stream.
    pub seq: usize,
    /// Network latency that this message receiving this message from the input pipeline
    pub pipeline_latency: Option<MessageLatency>
}

impl<T, D> Message<T, D> {
    /// Default buffer size.
    pub fn default_length() -> usize {
        let buffer_size = *MESSAGE_BUFFER_SIZE.get_or_init(|| {
            println!("message buffer size set to 1024");
            1024
        });
        buffer_size
    }

    /// Creates a new message instance from arguments.
    pub fn new(time: T, data: Vec<D>, from: usize, seq: usize) -> Self {
        Message { time, data, from, seq, pipeline_latency: None}
    }

    /// Forms a message, and pushes contents at `pusher`.
    // takes a buffer  (it consumes the buffer)
    #[inline]
    pub fn push_at<P: Push<Bundle<T, D>>>(buffer: &mut Vec<D>, time: T, pusher: &mut P) {

        // buffer is now replaced with an empty vector
        let data = ::std::mem::replace(buffer, Vec::new());
        // source: worker 0, seqno: 0
        let message = Message::new(time, data, 0, 0);
        let mut bundle = Some(Bundle::from_typed(message));

        // push interface requires a mutable reference
        // so the pusher can take ownership to move the data in an zero-copy way
        pusher.push(&mut bundle);

        if let Some(message) = bundle {
            if let Some(message) = message.if_typed() {
                *buffer = message.data;
                buffer.clear();
            }
        }

        // TODO: Unclear we always want this here.
        if buffer.capacity() != Self::default_length() {
            *buffer = Vec::with_capacity(Self::default_length());
        }
    }

    pub fn push_at_with_latency<P: Push<Bundle<T, D>>>(buffer: &mut Vec<D>, time: T, pusher: &mut P, latency: MessageLatency) {

        // buffer is now replaced with an empty vector
        let data = ::std::mem::replace(buffer, Vec::new());
        // source: worker 0, seqno: 0
        let mut message = Message::new(time, data, 0, 0);
        message.pipeline_latency = Some(latency);
        let mut bundle = Some(Bundle::from_typed(message));

        // push interface requires a mutable reference
        // so the pusher can take ownership to move the data in an zero-copy way
        pusher.push(&mut bundle);

        if let Some(message) = bundle {
            if let Some(message) = message.if_typed() {
                *buffer = message.data;
                buffer.clear();
            }
        }

        // TODO: Unclear we always want this here.
        if buffer.capacity() != Self::default_length() {
            *buffer = Vec::with_capacity(Self::default_length());
        }
    }
}
