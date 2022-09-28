// READ, Sep 7 2021
//! Push and Pull implementations wrapping serialized data.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::Write;

use bytes::arc::Bytes;

use crate::allocator::canary::Canary;
use crate::networking::MessageHeader;

use crate::{Data, Push, Pull, MessageLatency};
use crate::allocator::Message;

use super::bytes_exchange::{BytesPush, SendEndpoint};

/// An adapter into which one may push elements of type `T`.
///
/// This pusher has a fixed MessageHeader, and access to a SharedByteBuffer which it uses to
/// acquire buffers for serialization.
// messages are pushed to the MergeQueue
pub struct Pusher<T, P: BytesPush> {
    header:     MessageHeader,
    sender:     Rc<RefCell<SendEndpoint<P>>>,
    phantom:    ::std::marker::PhantomData<T>,
}

impl<T, P: BytesPush> Pusher<T, P> {
    /// Creates a new `Pusher` from a header and shared byte buffer.
    pub fn new(header: MessageHeader, sender: Rc<RefCell<SendEndpoint<P>>>) -> Pusher<T, P> {
        Pusher {
            header,
            sender,
            phantom:    ::std::marker::PhantomData,
        }
    }
}

impl<T:Data, P: BytesPush> Push<Message<T>> for Pusher<T, P> {
    #[inline]
    fn push(&mut self, element: &mut Option<Message<T>>) {
        if let Some(ref mut element) = *element {
            // determine byte lengths and build header.
            // Note that MessageHeader implements Copy
            // we copy a header here
            let mut header = self.header;
            self.header.seqno += 1;
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

    fn push_with_latency_passthrough(&mut self, element: &mut Option<Message<T>>, _latency: Option<MessageLatency>) {
        self.push(element);
    }
}

/// Pusher that directly push raw bytes (u8 wrapped in Bytes)
pub struct PusherBytes<P: BytesPush> {
    header: MessageHeader,
    sender: Rc<RefCell<SendEndpoint<P>>>
}

impl<P: BytesPush> PusherBytes<P> {
    /// Create a new raw bytes pusher
    pub fn new(header: MessageHeader, sender:Rc<RefCell<SendEndpoint<P>>>) -> PusherBytes<P> {
        PusherBytes {
            header,
            sender
        }
    }
}

impl<P: BytesPush> Push<Bytes> for PusherBytes<P> {
    fn push(&mut self, element: &mut Option<Bytes>) {
        if let Some(ref raw_message) = element {
            let mut header = self.header;
            self.header.seqno += 1;
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

/// An adapter from which one can pull elements of type `T`.
///
/// This type is very simple, and just consumes owned `Vec<u8>` allocations. It is
/// not the most efficient thing possible, which would probably instead be something
/// like the `bytes` crate (../bytes/) which provides an exclusive view of a shared
/// allocation.
pub struct Puller<T> {
    _canary: Canary,
    current: Option<Message<T>>,
    current_with_dummy_latency: Option<(Message<T>, MessageLatency)>,
    receiver: Rc<RefCell<VecDeque<Bytes>>>,    // source of serialized buffers
}

impl<T:Data> Puller<T> {
    /// Creates a new `Puller` instance from a shared queue.
    pub fn new(receiver: Rc<RefCell<VecDeque<Bytes>>>, _canary: Canary) -> Puller<T> {
        Puller {
            _canary,
            current: None,
            current_with_dummy_latency: None,
            receiver,
        }
    }
}

impl<T:Data> Pull<Message<T>> for Puller<T> {
    #[allow(unused_unsafe)]
    #[inline]
    fn pull(&mut self) -> &mut Option<Message<T>> {
        self.current =
        self.receiver
            .borrow_mut()
            .pop_front()
            .map(|bytes| unsafe { Message::from_bytes(bytes) });

        &mut self.current
    }

    #[allow(unused_unsafe)]
    #[inline]
    fn pull_with_transmission_latency(&mut self) -> &mut Option<(Message<T>, MessageLatency)> {
        self.current =
            self.receiver
                .borrow_mut()
                .pop_front()
                .map(|bytes| unsafe { Message::from_bytes(bytes) });
        self.current_with_dummy_latency = self.current.take().map(|x| (x, 0));
        &mut self.current_with_dummy_latency
    }
}

/// Puller that directly pulls raw bytes (does not try to recover data type)
pub struct PullerBytes {
    _canary: Canary,
    current: Option<Bytes>,
    current_with_dummy_latency: Option<(Bytes, MessageLatency)>,
    receiver: Rc<RefCell<VecDeque<Bytes>>>
}

impl PullerBytes {
    /// Create a new raw bytes puller
    pub fn new(receiver: Rc<RefCell<VecDeque<Bytes>>>, _canary: Canary) -> PullerBytes {
        PullerBytes {
            _canary,
            current: None,
            current_with_dummy_latency: None,
            receiver
        }
    }
}

impl Pull<Bytes> for PullerBytes {
    fn pull(&mut self) -> &mut Option<Bytes> {
        self.current = self.receiver.borrow_mut().pop_front();
        &mut self.current
    }

    fn pull_with_transmission_latency(&mut self) -> &mut Option<(Bytes, MessageLatency)> {
        self.current = self.receiver.borrow_mut().pop_front();
        self.current_with_dummy_latency = self.current.take().map(|x| (x, 0));
        &mut self.current_with_dummy_latency
    }
}

/// An adapter from which one can pull elements of type `T`.
///
/// This type is very simple, and just consumes owned `Vec<u8>` allocations. It is
/// not the most efficient thing possible, which would probably instead be something
/// like the `bytes` crate (../bytes/) which provides an exclusive view of a shared
/// allocation.
pub struct PullerInner<T> {
    inner: Box<dyn Pull<Message<T>>>,               // inner pullable (e.g. intra-process typed queue)
    _canary: Canary,
    current: Option<Message<T>>,
    current_with_dummy_latency: Option<(Message<T>, MessageLatency)>,
    receiver: Rc<RefCell<VecDeque<Bytes>>>,     // source of serialized buffers
}

impl<T:Data> PullerInner<T> {
    /// Creates a new `PullerInner` instance from a shared queue.
    pub fn new(inner: Box<dyn Pull<Message<T>>>, receiver: Rc<RefCell<VecDeque<Bytes>>>, _canary: Canary) -> Self {
        PullerInner {
            inner,
            _canary,
            current: None,
            current_with_dummy_latency: None,
            receiver,
        }
    }
}

impl<T:Data> Pull<Message<T>> for PullerInner<T> {
    #[allow(unused_unsafe)]
    #[inline]
    fn pull(&mut self) -> &mut Option<Message<T>> {
        // store the data in self.current in order to return a reference
        // instead of cloning / moving the data to return
        // first, try to pull from inner puller, i.e., ProcessAllocator's puller
        // for inter-thread, intra-process messages received
        // otherwise, pull from the inter-process messages received that stored in self.receiver
        let inner = self.inner.pull();
        if inner.is_some() {
            inner
        }
        else {
            self.current =
            self.receiver
                .borrow_mut()
                .pop_front()
                .map(|bytes| unsafe { Message::from_bytes(bytes) });
            &mut self.current
        }
    }

    #[allow(unused_unsafe)]
    #[inline]
    fn pull_with_transmission_latency(&mut self) -> &mut Option<(Message<T>, MessageLatency)> {
        let inner = self.inner.pull_with_transmission_latency();
        if inner.is_some() {
            inner
        }
        else {
            self.current =
                self.receiver
                    .borrow_mut()
                    .pop_front()
                    .map(|bytes| unsafe { Message::from_bytes(bytes) });
            self.current_with_dummy_latency = self.current.take().map(|x| (x, 0));
            &mut self.current_with_dummy_latency
        }
    }
    // return None when we have no data.
}