//! Implement trait RelayConnectAllocate

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::VecDeque;
use crate::{Allocate, Data, Message, Pull, Push};
use crate::allocator::canary::Canary;
use crate::allocator::{Event, Process, RelayConnectAllocate};
use crate::networking::MessageHeader;
use crate::allocator::counters::Puller as CountPuller;
use crate::allocator::relay::header::RelayToTimelyMessageHeader;
use crate::allocator::relay::push_pull::TimestampedPuller;
use crate::allocator::zero_copy::allocator::TcpAllocator;
use crate::allocator::zero_copy::bytes_exchange::BytesPull;
use crate::allocator::zero_copy::push_pull::Pusher;

// TODO: use macros to avoid repeated implementations
impl<A> RelayConnectAllocate for TcpAllocator<A>
where
    A: Allocate
{
    fn relay_peers(&self) -> usize {
        self.relay_peers.as_ref().unwrap().to_owned()
    }

    fn allocate_channel_to_relay<T: Data>(&mut self, channel_identifier: usize) -> (Vec<Box<dyn Push<Message<T>>>>, Box<dyn Pull<Message<T>>>) {
        let mut pushers = Vec::<Box<dyn Push<Message<T>>>>::with_capacity(self.relay_peers());

        for target_relay_index in 0 .. self.relay_peers() {
            let header = MessageHeader {
                channel: channel_identifier,
                source: self.index(),
                target: target_relay_index,
                length: 0,
                seqno: 0
            };

            pushers.push(Box::new(Pusher::new(header, self.sends_relay.as_ref().unwrap()[target_relay_index].clone())));
        }

        let relay_channel = self.relay_to_local.as_mut().unwrap()
            .entry(channel_identifier)
            .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
            .clone();

        let canary = Canary::new(channel_identifier, self.relay_canaries.as_ref().unwrap().clone());
        let puller = Box::new(CountPuller::new(
            TimestampedPuller::new(relay_channel, canary),
            channel_identifier,
            self.relay_events.as_ref().unwrap().clone()
        ));

        (pushers, puller)
    }

    fn receive_from_relay(&mut self) {
        let mut canaries = self.relay_canaries.as_mut().unwrap().borrow_mut();
        for dropped_channel in canaries.drain(..) {
            let _dropped =
                self.relay_to_local.as_mut().unwrap()
                    .remove(&dropped_channel)
                    .expect("non-existent channel dropped");
            self.relay_dropped_channels.as_mut().unwrap().insert(dropped_channel);
        }
        ::std::mem::drop(canaries);

        let staged = self.relay_staged.as_mut().unwrap();

        for recv in self.recvs_relay.as_mut().unwrap().iter_mut() {
            recv.drain_into(staged);
        }

        let mut events = self.relay_events.as_mut().unwrap().borrow_mut();

        let to_local = self.relay_to_local.as_mut().unwrap();

        for mut bytes in staged.drain(..) {

            // We expect that `bytes` contains an integral number of messages.
            // No splitting occurs across allocations.
            while bytes.len() > 0 {

                if let Some(header) = RelayToTimelyMessageHeader::try_read(&mut bytes[..]) {

                    // get (passed-through) pipeline network transmission latency
                    let latency = header.relay_transmission_latency.unwrap();
                    // Get the header and payload, ditch the header.
                    let mut peel = bytes.extract_to(header.required_bytes());
                    let _ = peel.extract_to(std::mem::size_of::<RelayToTimelyMessageHeader>());

                    // Increment message count for channel.
                    // Safe to do this even if the channel has been dropped.
                    events.push_back((header.channel, Event::Pushed(1)));

                    // Ensure that a queue exists.
                    // push to thread-local store
                    match to_local.entry(header.channel) {
                        Entry::Vacant(entry) => {
                            if !self.relay_dropped_channels.as_ref().unwrap().contains(&header.channel) {
                                entry.insert(Rc::new(RefCell::new(VecDeque::new())))
                                    .borrow_mut()
                                    .push_back((peel, latency));
                            }
                        }
                        Entry::Occupied(mut entry) => {
                            entry.get_mut().borrow_mut().push_back((peel, latency));
                        }
                    }
                }
                else {
                    println!("failed to read full header!");
                }
            }
        }
    }

    fn release_relay(&mut self) {
        let relay_sends = self.sends_relay.as_mut().unwrap();
        for send in relay_sends.iter_mut() {
            send.borrow_mut().publish();
        }
    }

    fn relay_events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>> {
        self.relay_events.as_ref().unwrap()
    }

    fn relay_input_pipelines_complete(&self) -> bool {
        self.recvs_relay.as_ref().unwrap().iter().all(|x| x.is_complete())
    }

    fn await_relay_events(&self, _duration: Option<std::time::Duration>) {
        if self.relay_events.as_ref().unwrap().borrow().is_empty() && self.events().borrow().is_empty() {
            if let Some(duration) = _duration {
                std::thread::park_timeout(duration);
            }
            else {
                std::thread::park();
            }
        }
    }
}


impl RelayConnectAllocate for Process
{
    fn relay_peers(&self) -> usize {
        self.relay_peers.as_ref().unwrap().to_owned()
    }

    fn allocate_channel_to_relay<T: Data>(&mut self, channel_identifier: usize) -> (Vec<Box<dyn Push<Message<T>>>>, Box<dyn Pull<Message<T>>>) {
        let mut pushers = Vec::<Box<dyn Push<Message<T>>>>::with_capacity(self.relay_peers());

        for target_relay_index in 0 .. self.relay_peers() {
            let header = MessageHeader {
                channel: channel_identifier,
                source: self.index(),
                target: target_relay_index,
                length: 0,
                seqno: 0
            };

            pushers.push(Box::new(Pusher::new(header, self.sends_relay.as_ref().unwrap()[target_relay_index].clone())));
        }

        let relay_channel = self.relay_to_local.as_mut().unwrap()
            .entry(channel_identifier)
            .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
            .clone();

        let canary = Canary::new(channel_identifier, self.relay_canaries.as_ref().unwrap().clone());
        let puller = Box::new(CountPuller::new(
            TimestampedPuller::new(relay_channel, canary),
            channel_identifier,
            self.relay_events.as_ref().unwrap().clone()
        ));

        (pushers, puller)
    }

    fn receive_from_relay(&mut self) {
        let mut canaries = self.relay_canaries.as_mut().unwrap().borrow_mut();
        for dropped_channel in canaries.drain(..) {
            let _dropped =
                self.relay_to_local.as_mut().unwrap()
                    .remove(&dropped_channel)
                    .expect("non-existent channel dropped");
            self.relay_dropped_channels.as_mut().unwrap().insert(dropped_channel);
        }
        ::std::mem::drop(canaries);

        let staged = self.relay_staged.as_mut().unwrap();

        for recv in self.recvs_relay.as_mut().unwrap().iter_mut() {
            recv.drain_into(staged);
        }

        let mut events = self.relay_events.as_mut().unwrap().borrow_mut();

        let to_local = self.relay_to_local.as_mut().unwrap();

        for mut bytes in staged.drain(..) {

            // We expect that `bytes` contains an integral number of messages.
            // No splitting occurs across allocations.
            while bytes.len() > 0 {

                if let Some(header) = RelayToTimelyMessageHeader::try_read(&mut bytes[..]) {

                    // get (passed-through) pipeline network transmission latency
                    let latency = header.relay_transmission_latency.unwrap();
                    // Get the header and payload, ditch the header.
                    let mut peel = bytes.extract_to(header.required_bytes());
                    let _ = peel.extract_to(std::mem::size_of::<RelayToTimelyMessageHeader>());

                    // Increment message count for channel.
                    // Safe to do this even if the channel has been dropped.
                    events.push_back((header.channel, Event::Pushed(1)));

                    // Ensure that a queue exists.
                    // push to thread-local store
                    match to_local.entry(header.channel) {
                        Entry::Vacant(entry) => {
                            if !self.relay_dropped_channels.as_ref().unwrap().contains(&header.channel) {
                                entry.insert(Rc::new(RefCell::new(VecDeque::new())))
                                    .borrow_mut()
                                    .push_back((peel, latency));
                            }
                        }
                        Entry::Occupied(mut entry) => {
                            entry.get_mut().borrow_mut().push_back((peel, latency));
                        }
                    }
                }
                else {
                    println!("failed to read full header!");
                }
            }
        }
    }

    fn release_relay(&mut self) {
        let relay_sends = self.sends_relay.as_mut().unwrap();
        for send in relay_sends.iter_mut() {
            send.borrow_mut().publish();
        }
    }

    fn relay_events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>> {
        self.relay_events.as_ref().unwrap()
    }

    fn relay_input_pipelines_complete(&self) -> bool {
        self.recvs_relay.as_ref().unwrap().iter().all(|x| x.is_complete())
    }

    fn await_relay_events(&self, _duration: Option<std::time::Duration>) {
        if self.relay_events.as_ref().unwrap().borrow().is_empty() && self.events().borrow().is_empty() {
            if let Some(duration) = _duration {
                std::thread::park_timeout(duration);
            }
            else {
                std::thread::park();
            }
        }
    }
}