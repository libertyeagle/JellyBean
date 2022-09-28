//! Message headers with timestamp info

use abomonation::{encode, decode};
use crate::MessageLatency;


#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct RelayToRelayMessageHeader {
    /// index of channel.
    pub channel:    usize,
    /// index of worker sending message.
    pub source:     usize,
    /// index of worker receiving message.
    pub target:     usize,
    /// number of bytes in message.
    pub length:     usize,
    /// sequence number.
    pub seqno:      usize,
    /// timestamp that the message is sent at network thread that executes send_loop
    pub send_timestamp: Option<MessageLatency>,
    /// timestamp when this message is received at network thread that executes recv_loop
    pub recv_timestamp: Option<MessageLatency>
}

impl RelayToRelayMessageHeader {
    /// Returns a header when there is enough supporting data
    // construct MessageHeader from serialized data
    #[inline]
    pub fn try_read(bytes: &mut [u8]) -> Option<RelayToRelayMessageHeader> {
        // the first ::std::mem::size_of::<MessageHeader>() bytes contains MessageHeader
        // the remaining is the message
        // MessageHeader does not own data, we don't need to correct the pointers
        unsafe { decode::<RelayToRelayMessageHeader>(bytes) }
            .and_then(|(header, remaining)| {
                if remaining.len() >= header.length {
                    // Since MessageHeader does not own data and it implements Copy trait
                    // move it and clone it is actually the same
                    // we can also write Some(*header) instead
                    Some(header.clone())
                }
                else {
                    None
                }
            })
    }

    /// Writes the header as binary data.
    // serialize the MessageHeader
    #[inline]
    pub fn write_to<W: ::std::io::Write>(&self, writer: &mut W) -> ::std::io::Result<()> {
        unsafe { encode(self, writer) }
    }

    /// The number of bytes required for the header and data.
    #[inline]
    pub fn required_bytes(&self) -> usize {
        ::std::mem::size_of::<RelayToRelayMessageHeader>() + self.length
    }
}


#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct RelayToTimelyMessageHeader {
    /// index of channel.
    pub channel:    usize,
    /// index of worker sending message.
    pub source:     usize,
    /// index of worker receiving message.
    pub target:     usize,
    /// number of bytes in message.
    pub length:     usize,
    /// sequence number.
    pub seqno:      usize,
    /// transmission latency between the relays of the two pipelines
    pub relay_transmission_latency: Option<MessageLatency>
}

impl RelayToTimelyMessageHeader {
    /// Returns a header when there is enough supporting data
    // construct MessageHeader from serialized data
    #[inline]
    pub fn try_read(bytes: &mut [u8]) -> Option<RelayToTimelyMessageHeader> {
        // the first ::std::mem::size_of::<MessageHeader>() bytes contains MessageHeader
        // the remaining is the message
        // MessageHeader does not own data, we don't need to correct the pointers
        unsafe { decode::<RelayToTimelyMessageHeader>(bytes) }
            .and_then(|(header, remaining)| {
                if remaining.len() >= header.length {
                    // Since MessageHeader does not own data and it implements Copy trait
                    // move it and clone it is actually the same
                    // we can also write Some(*header) instead
                    Some(header.clone())
                }
                else {
                    None
                }
            })
    }

    /// Writes the header as binary data.
    // serialize the MessageHeader
    #[inline]
    pub fn write_to<W: ::std::io::Write>(&self, writer: &mut W) -> ::std::io::Result<()> {
        unsafe { encode(self, writer) }
    }

    /// The number of bytes required for the header and data.
    #[inline]
    pub fn required_bytes(&self) -> usize {
        ::std::mem::size_of::<RelayToTimelyMessageHeader>() + self.length
    }
}