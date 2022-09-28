//! Push and Pull implementations wrapping serialized data with timestamp info.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::Write;

use bytes::arc::Bytes;

use crate::allocator::canary::Canary;

use crate::{Data, Push, Pull, MessageLatency};
use crate::allocator::Message;
use crate::allocator::relay::header::{RelayToRelayMessageHeader, RelayToTimelyMessageHeader};

use crate::allocator::zero_copy::bytes_exchange::{BytesPush, SendEndpoint};

/// An adapter into which one may push elements of type `T`.
///
/// This pusher has a fixed MessageHeader, and access to a SharedByteBuffer which it uses to
/// acquire buffers for serialization.
// messages are pushed to the MergeQueue
pub struct RelayToRelayTimestampedPusher<T, P: BytesPush> {
    header:     RelayToRelayMessageHeader,
    sender:     Rc<RefCell<SendEndpoint<P>>>,
    phantom:    ::std::marker::PhantomData<T>,
}

impl<T, P: BytesPush> RelayToRelayTimestampedPusher<T, P> {
    /// Creates a new `Pusher` from a header and shared byte buffer.
    pub fn new(header: RelayToRelayMessageHeader, sender: Rc<RefCell<SendEndpoint<P>>>) -> RelayToRelayTimestampedPusher<T, P> {
        RelayToRelayTimestampedPusher {
            header,
            sender,
            phantom:    ::std::marker::PhantomData,
        }
    }
}

impl<T:Data, P: BytesPush> Push<Message<T>> for RelayToRelayTimestampedPusher<T, P> {
    #[inline]
    fn push(&mut self, element: &mut Option<Message<T>>) {
        if let Some(ref mut element) = *element {
            // determine byte lengths and build header.
            // Note that MessageHeader implements Copy
            // we copy a header here
            let mut header = self.header;
            self.header.seqno += 1;
            // we should update send timestamp later
            // at the network thread that executes send_loop
            header.send_timestamp = None;
            header.length = element.length_in_bytes();
            assert!(header.length > 0);

            // acquire byte buffer and write header, element.
            let mut borrow = self.sender.borrow_mut();
            {
                let mut bytes = borrow.reserve(header.required_bytes());
                assert!(bytes.len() >= header.required_bytes());
                // &mut [u8] is a writer, while [u8] is not
                // &mut &mut [u8] is a mutable reference to the writer
                // we have `impl Write for &mut [u8]`
                let writer = &mut bytes;
                header.write_to(writer).expect("failed to write header!");
                element.into_bytes(writer);
            }
            // mark as valid
            borrow.make_valid(header.required_bytes());
        }
    }

    /// We don't need to pass latency in relay-relay communication,
    /// the latency of passing message across pipeline is passed through to the timely workers
    fn push_with_latency_passthrough(&mut self, element: &mut Option<Message<T>>, _latency: Option<MessageLatency>) {
        self.push(element);
    }
}

/// Pusher that directly push raw bytes (u8 wrapped in Bytes)
pub struct RelayToRelayTimestampedPusherBytes<P: BytesPush> {
    header: RelayToRelayMessageHeader,
    sender: Rc<RefCell<SendEndpoint<P>>>
}

impl<P: BytesPush> RelayToRelayTimestampedPusherBytes<P> {
    /// Create a new raw bytes pusher
    pub fn new(header: RelayToRelayMessageHeader, sender:Rc<RefCell<SendEndpoint<P>>>) -> RelayToRelayTimestampedPusherBytes<P> {
        RelayToRelayTimestampedPusherBytes {
            header,
            sender
        }
    }
}

impl<P: BytesPush> Push<Bytes> for RelayToRelayTimestampedPusherBytes<P> {
    fn push(&mut self, element: &mut Option<Bytes>) {
        if let Some(ref raw_message) = element {
            let mut header = self.header;
            self.header.seqno += 1;

            header.send_timestamp = None;
            header.length = raw_message.len();

            assert!(header.length > 0);

            let mut borrow = self.sender.borrow_mut();
            {
                let mut bytes = borrow.reserve(header.required_bytes());
                assert!(bytes.len() >= header.required_bytes());
                let writer = &mut bytes;
                header.write_to(writer).expect("failed to write header");
                writer.write_all(raw_message).expect("failed to write message");
            }
            borrow.make_valid(header.required_bytes());
        }
    }

    fn push_with_latency_passthrough(&mut self, element: &mut Option<Bytes>, _latency: Option<MessageLatency>) {
        self.push(element);
    }
}


/// Relay nodes to timely workers timestamped typed message pusher
pub struct RelayToTimelyTimestampedPusher<T, P: BytesPush> {
    header:     RelayToTimelyMessageHeader,
    sender:     Rc<RefCell<SendEndpoint<P>>>,
    phantom:    ::std::marker::PhantomData<T>,
}

impl<T, P: BytesPush> RelayToTimelyTimestampedPusher<T, P> {
    /// Creates a new `Pusher` from a header and shared byte buffer.
    pub fn new(header: RelayToTimelyMessageHeader, sender: Rc<RefCell<SendEndpoint<P>>>) -> RelayToTimelyTimestampedPusher<T, P> {
        RelayToTimelyTimestampedPusher {
            header,
            sender,
            phantom:    ::std::marker::PhantomData,
        }
    }
}

impl<T:Data, P: BytesPush> Push<Message<T>> for RelayToTimelyTimestampedPusher<T, P> {
    #[inline]
    fn push(&mut self, element: &mut Option<Message<T>>) {
        if let Some(ref mut element) = *element {
            // determine byte lengths and build header.
            // Note that MessageHeader implements Copy
            // we copy a header here
            let mut header = self.header;
            self.header.seqno += 1;
            // we should update send timestamp later
            // when we pull
            header.relay_transmission_latency = None;
            header.length = element.length_in_bytes();
            assert!(header.length > 0);

            // acquire byte buffer and write header, element.
            let mut borrow = self.sender.borrow_mut();
            {
                let mut bytes = borrow.reserve(header.required_bytes());
                assert!(bytes.len() >= header.required_bytes());
                // &mut [u8] is a writer, while [u8] is not
                // &mut &mut [u8] is a mutable reference to the writer
                // we have `impl Write for &mut [u8]`
                let writer = &mut bytes;
                header.write_to(writer).expect("failed to write header!");
                element.into_bytes(writer);
            }
            // mark as valid
            borrow.make_valid(header.required_bytes());
        }
    }

    fn push_with_latency_passthrough(&mut self, element: &mut Option<Message<T>>, latency: Option<MessageLatency>) {
        if let Some(ref mut element) = *element {
            // determine byte lengths and build header.
            // Note that MessageHeader implements Copy
            // we copy a header here
            let mut header = self.header;
            self.header.seqno += 1;
            // we should update send timestamp later
            // when we pull
            header.relay_transmission_latency = Some(latency.unwrap());
            header.length = element.length_in_bytes();
            assert!(header.length > 0);

            // acquire byte buffer and write header, element.
            let mut borrow = self.sender.borrow_mut();
            {
                let mut bytes = borrow.reserve(header.required_bytes());
                assert!(bytes.len() >= header.required_bytes());
                // &mut [u8] is a writer, while [u8] is not
                // &mut &mut [u8] is a mutable reference to the writer
                // we have `impl Write for &mut [u8]`
                let writer = &mut bytes;
                header.write_to(writer).expect("failed to write header!");
                element.into_bytes(writer);
            }
            // mark as valid
            borrow.make_valid(header.required_bytes());
        }
    }
}


/// Relay nodes to timely worker pusher that directly push raw bytes (u8 wrapped in Bytes)
pub struct RelayToTimelyTimestampedPusherBytes<P: BytesPush> {
    header: RelayToTimelyMessageHeader,
    sender: Rc<RefCell<SendEndpoint<P>>>
}

impl<P: BytesPush> RelayToTimelyTimestampedPusherBytes<P> {
    /// Create a new raw bytes pusher
    pub fn new(header: RelayToTimelyMessageHeader, sender:Rc<RefCell<SendEndpoint<P>>>) -> RelayToTimelyTimestampedPusherBytes<P> {
        RelayToTimelyTimestampedPusherBytes {
            header,
            sender
        }
    }
}

impl<P: BytesPush> Push<Bytes> for RelayToTimelyTimestampedPusherBytes<P> {
    fn push(&mut self, element: &mut Option<Bytes>) {
        if let Some(ref raw_message) = element {
            let mut header = self.header;
            self.header.seqno += 1;
            header.relay_transmission_latency = None;
            header.length = raw_message.len();
            assert!(header.length > 0);

            let mut borrow = self.sender.borrow_mut();
            {
                let mut bytes = borrow.reserve(header.required_bytes());
                assert!(bytes.len() >= header.required_bytes());
                let writer = &mut bytes;
                header.write_to(writer).expect("failed to write header");
                writer.write_all(raw_message).expect("failed to write message");
            }
            borrow.make_valid(header.required_bytes());
        }
    }

    fn push_with_latency_passthrough(&mut self, element: &mut Option<Bytes>, latency: Option<MessageLatency>) {
        if let Some(ref raw_message) = element {
            let mut header = self.header;
            self.header.seqno += 1;
            header.relay_transmission_latency = Some(latency.unwrap());
            header.length = raw_message.len();
            assert!(header.length > 0);

            let mut borrow = self.sender.borrow_mut();
            {
                let mut bytes = borrow.reserve(header.required_bytes());
                assert!(bytes.len() >= header.required_bytes());
                let writer = &mut bytes;
                header.write_to(writer).expect("failed to write header");
                writer.write_all(raw_message).expect("failed to write message");
            }
            borrow.make_valid(header.required_bytes());
        }
    }
}

/// An adapter from which one can pull elements of type `T`.
///
/// This type is very simple, and just consumes owned `Vec<u8>` allocations. It is
/// not the most efficient thing possible, which would probably instead be something
/// like the `bytes` crate (../bytes/) which provides an exclusive view of a shared
/// allocation.
pub struct TimestampedPuller<T> {
    _canary: Canary,
    current: Option<Message<T>>,
    current_with_latency: Option<(Message<T>, MessageLatency)>,
    receiver: Rc<RefCell<VecDeque<(Bytes, MessageLatency)>>>,    // source of serialized buffers
}

impl<T:Data> TimestampedPuller<T> {
    /// Creates a new `Puller` instance from a shared queue.
    pub fn new(receiver: Rc<RefCell<VecDeque<(Bytes, MessageLatency)>>>, _canary: Canary) -> TimestampedPuller<T> {
        TimestampedPuller {
            _canary,
            current: None,
            current_with_latency: None,
            receiver,
        }
    }
}

impl<T:Data> Pull<Message<T>> for TimestampedPuller<T> {
    #[allow(unused_unsafe)]
    #[inline]
    fn pull(&mut self) -> &mut Option<Message<T>> {
        self.current_with_latency =
            self.receiver
                .borrow_mut()
                .pop_front()
                .map(|(bytes, latency)| (unsafe { Message::from_bytes(bytes) }, latency));

        self.current = self.current_with_latency.take().map(|(x, _lat)| x);
        &mut self.current
    }

    #[allow(unused_unsafe)]
    #[inline]
    fn pull_with_transmission_latency(&mut self) -> &mut Option<(Message<T>, MessageLatency)> {
        self.current_with_latency =
            self.receiver
                .borrow_mut()
                .pop_front()
                .map(|(bytes, latency)| (unsafe { Message::from_bytes(bytes) }, latency));
        &mut self.current_with_latency
    }
}

/// Puller that directly pulls raw bytes (does not try to recover data type)
pub struct TimestampedPullerBytes {
    _canary: Canary,
    current: Option<Bytes>,
    current_with_latency: Option<(Bytes, MessageLatency)>,
    receiver: Rc<RefCell<VecDeque<(Bytes, MessageLatency)>>>
}

impl TimestampedPullerBytes {
    /// Create a new raw bytes puller
    pub fn new(receiver: Rc<RefCell<VecDeque<(Bytes, MessageLatency)>>>, _canary: Canary) -> TimestampedPullerBytes {
        TimestampedPullerBytes {
            _canary,
            current: None,
            current_with_latency: None,
            receiver
        }
    }
}

impl Pull<Bytes> for TimestampedPullerBytes {
    fn pull(&mut self) -> &mut Option<Bytes> {
        self.current_with_latency = self.receiver.borrow_mut().pop_front();
        self.current = self.current_with_latency.take().map(|(x, _lat)| x);
        &mut self.current
    }

    fn pull_with_transmission_latency(&mut self) -> &mut Option<(Bytes, MessageLatency)> {
        self.current_with_latency = self.receiver.borrow_mut().pop_front();
        &mut self.current_with_latency
    }
}


